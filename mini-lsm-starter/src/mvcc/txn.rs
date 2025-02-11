#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::{
    collections::HashSet,
    ops::Bound,
    sync::{atomic::AtomicBool, atomic::Ordering, Arc},
};

use anyhow::{bail, Result};
use bytes::Bytes;
use crossbeam_skiplist::SkipMap;
use ouroboros::self_referencing;
use parking_lot::Mutex;

use crate::{
    iterators::{two_merge_iterator::TwoMergeIterator, StorageIterator},
    lsm_iterator::{FusedIterator, LsmIterator},
    lsm_storage::{LsmStorageInner, WriteBatchRecord},
    mem_table::map_bound,
};

use super::CommittedTxnData;

pub struct Transaction {
    pub(crate) read_ts: u64,
    pub(crate) inner: Arc<LsmStorageInner>,
    pub(crate) local_storage: Arc<SkipMap<Bytes, Bytes>>,
    pub(crate) committed: Arc<AtomicBool>,
    /// Write set and read set
    pub(crate) key_hashes: Option<Mutex<(HashSet<u32>, HashSet<u32>)>>,
}

impl Transaction {
    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        if self.committed.load(Ordering::SeqCst) {
            panic!("cannot operate on committed txn!");
        }

        if self.key_hashes.is_some() {
            let mut guard = self.key_hashes.as_ref().unwrap().lock();
            let (_, read_set) = &mut *guard;
            read_set.insert(crc32fast::hash(key));
        }

        // first probe the local_sotrage
        if let Some(entry) = self.local_storage.get(key) {
            if entry.value().is_empty() {
                return Ok(None);
            }
            return Ok(Some(entry.value().clone()));
        }
        self.inner.get_with_ts(key, self.read_ts)
    }

    pub fn scan(self: &Arc<Self>, lower: Bound<&[u8]>, upper: Bound<&[u8]>) -> Result<TxnIterator> {
        if self.committed.load(Ordering::SeqCst) {
            panic!("cannot operate on committed txn!");
        }

        let mut local_iter = TxnLocalIteratorBuilder {
            map: self.local_storage.clone(),
            iter_builder: |map| map.range((map_bound(lower), map_bound(upper))),
            item: (Bytes::new(), Bytes::new()),
        }
        .build();
        let _ = local_iter.next();

        let storage_iter = self.inner.scan_with_ts(lower, upper, self.read_ts)?;

        let iter = TwoMergeIterator::create(local_iter, storage_iter)?;
        TxnIterator::create(self.clone(), iter)
    }

    pub fn put(&self, key: &[u8], value: &[u8]) {
        if self.committed.load(Ordering::SeqCst) {
            panic!("cannot operate on committed txn!");
        }
        if self.key_hashes.is_some() {
            let mut guard = self.key_hashes.as_ref().unwrap().lock();
            let (write_set, _) = &mut *guard;
            write_set.insert(crc32fast::hash(key));
        }
        self.local_storage
            .insert(Bytes::copy_from_slice(key), Bytes::copy_from_slice(value));
    }

    pub fn delete(&self, key: &[u8]) {
        if self.committed.load(Ordering::SeqCst) {
            panic!("cannot operate on committed txn!");
        }
        if self.key_hashes.is_some() {
            let mut guard = self.key_hashes.as_ref().unwrap().lock();
            let (write_set, _) = &mut *guard;
            write_set.insert(crc32fast::hash(key));
        }
        self.local_storage
            .insert(Bytes::copy_from_slice(key), Bytes::new());
    }

    pub fn commit(&self) -> Result<()> {
        // serializable validation
        let mut serializable_check = false;
        let _commit_lock = self.inner.mvcc().commit_lock.lock();
        let expected_commit_ts = self.inner.mvcc().latest_commit_ts() + 1;
        if self.key_hashes.is_some() {
            let guard = self.key_hashes.as_ref().unwrap().lock();
            let (write_set, read_set) = &*guard;
            // println!("write_set: {:?}, read_set: {:?}", write_set, read_set);
            if !write_set.is_empty() {
                let committed_txns = self.inner.mvcc().committed_txns.lock();
                for (_ts, txn_data) in committed_txns.iter() {
                    let ts_overlap = self.read_ts < txn_data.commit_ts
                        && txn_data.commit_ts < expected_commit_ts;
                    // commit timestamp within range (read_ts, expected_commit_ts), and has jointset
                    if ts_overlap && !read_set.is_disjoint(&txn_data.key_hashes) {
                        bail!("Serializable Validation fail!");
                    }
                }
            }
            serializable_check = true;
        }

        // begin commit process
        self.committed
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .expect("cannot operate on committed txn!");

        // collect all key-value pairs from the local storage and submit a write batch to the storage engine
        let batch = self
            .local_storage
            .iter()
            .map(|entry| {
                if entry.value().is_empty() {
                    WriteBatchRecord::Del(entry.key().clone())
                } else {
                    WriteBatchRecord::Put(entry.key().clone(), entry.value().clone())
                }
            })
            .collect::<Vec<_>>();

        let ts = self.inner.write_batch_inner(&batch)?;
        if serializable_check {
            let mut guard = self.key_hashes.as_ref().unwrap().lock();
            let (write_set, read_set) = &mut *guard;
            let mut committed_txns = self.inner.mvcc().committed_txns.lock();
            let old_tnx_data = committed_txns.insert(
                ts,
                CommittedTxnData {
                    commit_ts: ts,
                    key_hashes: std::mem::take(write_set),
                    read_ts: self.read_ts,
                },
            );
            assert!(old_tnx_data.is_none());
            // Garbage Collection
            let min_ts = self.inner.mvcc().watermark();
            *committed_txns = committed_txns.split_off(&min_ts);
        }
        Ok(())
    }
}

impl Drop for Transaction {
    fn drop(&mut self) {
        // println!("remove read_ts: {:?}", self.read_ts);
        self.inner.mvcc().ts.lock().1.remove_reader(self.read_ts);
    }
}

type SkipMapRangeIter<'a> =
    crossbeam_skiplist::map::Range<'a, Bytes, (Bound<Bytes>, Bound<Bytes>), Bytes, Bytes>;

#[self_referencing]
pub struct TxnLocalIterator {
    /// Stores a reference to the skipmap.
    map: Arc<SkipMap<Bytes, Bytes>>,
    /// Stores a skipmap iterator that refers to the lifetime of `TxnLocalIterator` itself.
    #[borrows(map)]
    #[not_covariant]
    iter: SkipMapRangeIter<'this>,
    /// Stores the current key-value pair.
    item: (Bytes, Bytes),
}

impl StorageIterator for TxnLocalIterator {
    type KeyType<'a> = &'a [u8];

    fn value(&self) -> &[u8] {
        self.borrow_item().1.as_ref()
    }

    fn key(&self) -> &[u8] {
        self.borrow_item().0.as_ref()
    }

    fn is_valid(&self) -> bool {
        !self.borrow_item().0.is_empty()
    }

    fn next(&mut self) -> Result<()> {
        self.with_mut(|field| {
            let next_item = field
                .iter
                .next()
                .map(|entry| (entry.key().clone(), entry.value().clone()));
            *field.item = next_item.unwrap_or((Bytes::new(), Bytes::new()));
        });
        Ok(())
    }
}

pub struct TxnIterator {
    txn: Arc<Transaction>,
    iter: TwoMergeIterator<TxnLocalIterator, FusedIterator<LsmIterator>>,
}

impl TxnIterator {
    pub fn create(
        txn: Arc<Transaction>,
        iter: TwoMergeIterator<TxnLocalIterator, FusedIterator<LsmIterator>>,
    ) -> Result<Self> {
        let txn_iter = Self { txn, iter };
        if txn_iter.is_valid() {
            txn_iter.add_to_read_set(txn_iter.key());
        }
        Ok(txn_iter)
    }

    pub fn add_to_read_set(&self, key: &[u8]) {
        if self.txn.inner.options.serializable {
            let mut guard = self.txn.key_hashes.as_ref().unwrap().lock();
            let (_, read_set) = &mut *guard;
            read_set.insert(crc32fast::hash(key));
            println!("key: {:?} add to read_set", key);
        }
    }
}

impl StorageIterator for TxnIterator {
    type KeyType<'a>
        = &'a [u8]
    where
        Self: 'a;

    fn value(&self) -> &[u8] {
        self.iter.value()
    }

    fn key(&self) -> Self::KeyType<'_> {
        self.iter.key()
    }

    fn is_valid(&self) -> bool {
        self.iter.is_valid()
    }

    fn next(&mut self) -> Result<()> {
        self.iter.next()?;
        // skip deleted key
        while self.iter.is_valid() && self.iter.value().is_empty() {
            self.iter.next()?;
        }
        // add key to read_set
        if self.is_valid() {
            self.add_to_read_set(self.iter.key());
        }
        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        self.iter.num_active_iterators()
    }
}
