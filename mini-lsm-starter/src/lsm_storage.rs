#![allow(dead_code)] // REMOVE THIS LINE after fully implementing this functionality

use std::collections::{BTreeSet, HashMap};
use std::fs::File;
use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;

use anyhow::{Context, Result};
use bytes::Bytes;
use farmhash::fingerprint32;
use parking_lot::{Mutex, MutexGuard, RwLock};

use crate::block::Block;
use crate::compact::{
    CompactionController, CompactionOptions, LeveledCompactionController, LeveledCompactionOptions,
    SimpleLeveledCompactionController, SimpleLeveledCompactionOptions, TieredCompactionController,
};
use crate::iterators::concat_iterator::SstConcatIterator;
use crate::iterators::merge_iterator::MergeIterator;
use crate::iterators::two_merge_iterator::TwoMergeIterator;
use crate::iterators::StorageIterator;
use crate::key::{KeySlice, TS_RANGE_BEGIN, TS_RANGE_END};
use crate::lsm_iterator::{FusedIterator, LsmIterator};
use crate::manifest::{Manifest, ManifestRecord};
use crate::mem_table::{map_bound, map_bound_plus_ts, MemTable};
use crate::mvcc::LsmMvccInner;
use crate::table::{FileObject, SsTable, SsTableBuilder, SsTableIterator};

pub type BlockCache = moka::sync::Cache<(usize, usize), Arc<Block>>;

/// Represents the state of the storage engine.
#[derive(Clone)]
pub struct LsmStorageState {
    /// The current memtable.
    pub memtable: Arc<MemTable>,
    /// Immutable memtables, from latest to earliest.
    pub imm_memtables: Vec<Arc<MemTable>>,
    /// L0 SSTs, from latest to earliest.
    pub l0_sstables: Vec<usize>,
    /// SsTables sorted by key range; L1 - L_max for leveled compaction, or tiers for tiered
    /// compaction.
    pub levels: Vec<(usize, Vec<usize>)>,
    /// SST objects.
    pub sstables: HashMap<usize, Arc<SsTable>>,
}

pub enum WriteBatchRecord<T: AsRef<[u8]>> {
    Put(T, T),
    Del(T),
}

impl LsmStorageState {
    fn create(options: &LsmStorageOptions) -> Self {
        let levels = match &options.compaction_options {
            CompactionOptions::Leveled(LeveledCompactionOptions { max_levels, .. })
            | CompactionOptions::Simple(SimpleLeveledCompactionOptions { max_levels, .. }) => (1
                ..=*max_levels)
                .map(|level| (level, Vec::new()))
                .collect::<Vec<_>>(),
            CompactionOptions::Tiered(_) => Vec::new(),
            CompactionOptions::NoCompaction => vec![(1, Vec::new())],
        };
        Self {
            memtable: Arc::new(MemTable::create(0)),
            imm_memtables: Vec::new(),
            l0_sstables: Vec::new(),
            levels,
            sstables: Default::default(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct LsmStorageOptions {
    // Block size in bytes
    pub block_size: usize,
    // SST size in bytes, also the approximate memtable capacity limit
    pub target_sst_size: usize,
    // Maximum number of memtables in memory, flush to L0 when exceeding this limit
    pub num_memtable_limit: usize,
    pub compaction_options: CompactionOptions,
    pub enable_wal: bool,
    pub serializable: bool,
}

impl LsmStorageOptions {
    pub fn default_for_week1_test() -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 2 << 20,
            compaction_options: CompactionOptions::NoCompaction,
            enable_wal: false,
            num_memtable_limit: 50,
            serializable: false,
        }
    }

    pub fn default_for_week1_day6_test() -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 2 << 20,
            compaction_options: CompactionOptions::NoCompaction,
            enable_wal: false,
            num_memtable_limit: 2,
            serializable: false,
        }
    }

    pub fn default_for_week2_test(compaction_options: CompactionOptions) -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 1 << 20, // 1MB
            compaction_options,
            enable_wal: false,
            num_memtable_limit: 2,
            serializable: false,
        }
    }
}

#[derive(Clone, Debug)]
pub enum CompactionFilter {
    Prefix(Bytes),
}

/// The storage interface of the LSM tree.
pub(crate) struct LsmStorageInner {
    pub(crate) state: Arc<RwLock<Arc<LsmStorageState>>>,
    pub(crate) state_lock: Mutex<()>,
    path: PathBuf,
    pub(crate) block_cache: Arc<BlockCache>,
    next_sst_id: AtomicUsize,
    pub(crate) options: Arc<LsmStorageOptions>,
    pub(crate) compaction_controller: CompactionController,
    pub(crate) manifest: Option<Manifest>,
    pub(crate) mvcc: Option<LsmMvccInner>,
    pub(crate) compaction_filters: Arc<Mutex<Vec<CompactionFilter>>>,
}

/// A thin wrapper for `LsmStorageInner` and the user interface for MiniLSM.
pub struct MiniLsm {
    pub(crate) inner: Arc<LsmStorageInner>,
    /// Notifies the L0 flush thread to stop working. (In week 1 day 6)
    flush_notifier: crossbeam_channel::Sender<()>,
    /// The handle for the flush thread. (In week 1 day 6)
    flush_thread: Mutex<Option<std::thread::JoinHandle<()>>>,
    /// Notifies the compaction thread to stop working. (In week 2)
    compaction_notifier: crossbeam_channel::Sender<()>,
    /// The handle for the compaction thread. (In week 2)
    compaction_thread: Mutex<Option<std::thread::JoinHandle<()>>>,
}

impl Drop for MiniLsm {
    fn drop(&mut self) {
        self.compaction_notifier.send(()).ok();
        self.flush_notifier.send(()).ok();
    }
}

impl MiniLsm {
    /// wait until the flush thread (and the compaction thread in week 2) to finish
    pub fn close(&self) -> Result<()> {
        self.flush_notifier.send(()).ok();
        self.compaction_notifier.send(()).ok();

        let mut flush_thread = self.flush_thread.lock();
        if let Some(flush_handler) = flush_thread.take() {
            flush_handler
                .join()
                .map_err(|e| anyhow::anyhow!("{:?}", e))?;
        }

        let mut compaction_thread = self.compaction_thread.lock();
        if let Some(compaction_handler) = compaction_thread.take() {
            compaction_handler
                .join()
                .map_err(|e| anyhow::anyhow!("{:?}", e))?;
        }

        if self.inner.options.enable_wal {
            self.inner.sync()?;
            self.inner.sync_dir()?;
            return Ok(());
        }

        //check mem_table
        let res = {
            let snapshot = self.inner.state.read();
            !snapshot.memtable.is_empty()
        };
        if res {
            let new_memtable = Arc::new(MemTable::create(self.inner.next_sst_id()));
            self.inner
                .freeze_old_memtable_with_new_memtable(new_memtable)?;
        }

        //check imm_tables
        while {
            let snapshot = self.inner.state.read();
            !snapshot.imm_memtables.is_empty()
        } {
            self.inner.force_flush_next_imm_memtable()?;
        }
        self.inner.sync_dir()?;
        Ok(())
    }

    /// Start the storage engine by either loading an existing directory or creating a new one if the directory does
    /// not exist.
    pub fn open(path: impl AsRef<Path>, options: LsmStorageOptions) -> Result<Arc<Self>> {
        let inner = Arc::new(LsmStorageInner::open(path, options)?);
        let (tx1, rx) = crossbeam_channel::unbounded();
        let compaction_thread = inner.spawn_compaction_thread(rx)?;
        let (tx2, rx) = crossbeam_channel::unbounded();
        let flush_thread = inner.spawn_flush_thread(rx)?;
        Ok(Arc::new(Self {
            inner,
            flush_notifier: tx2,
            flush_thread: Mutex::new(flush_thread),
            compaction_notifier: tx1,
            compaction_thread: Mutex::new(compaction_thread),
        }))
    }

    pub fn new_txn(&self) -> Result<()> {
        self.inner.new_txn()
    }

    pub fn write_batch<T: AsRef<[u8]>>(&self, batch: &[WriteBatchRecord<T>]) -> Result<()> {
        self.inner.write_batch(batch)
    }

    pub fn add_compaction_filter(&self, compaction_filter: CompactionFilter) {
        self.inner.add_compaction_filter(compaction_filter)
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        self.inner.get(key)
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.inner.put(key, value)
    }

    pub fn delete(&self, key: &[u8]) -> Result<()> {
        self.inner.delete(key)
    }

    pub fn sync(&self) -> Result<()> {
        self.inner.sync()
    }

    pub fn scan(
        &self,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
    ) -> Result<FusedIterator<LsmIterator>> {
        self.inner.scan(lower, upper)
    }

    /// Only call this in test cases due to race conditions
    pub fn force_flush(&self) -> Result<()> {
        if !self.inner.state.read().memtable.is_empty() {
            self.inner
                .force_freeze_memtable(&self.inner.state_lock.lock())?;
        }
        if !self.inner.state.read().imm_memtables.is_empty() {
            self.inner.force_flush_next_imm_memtable()?;
        }
        Ok(())
    }

    pub fn force_full_compaction(&self) -> Result<()> {
        self.inner.force_full_compaction()
    }
}

pub fn key_within(table: &Arc<SsTable>, key: KeySlice) -> bool {
    println!("table.first_key: {:?}", table.first_key());
    println!("table.last_key : {:?}", table.last_key());
    println!("key : {:?}", key);
    println!(
        "key_within: {:?}",
        table.first_key().as_key_slice() <= key && key <= table.last_key().as_key_slice()
    );
    table.first_key().as_key_slice() <= key && key <= table.last_key().as_key_slice()
}

pub fn range_overlap(
    user_lower: Bound<KeySlice>,
    user_upper: Bound<KeySlice>,
    table: &Arc<SsTable>,
) -> bool {
    let first_key = table.first_key().key_ref();
    let last_key = table.last_key().key_ref();

    match user_lower {
        //                               [user_lower, user_upper )
        //     [first_key, last_key ]
        Bound::Included(key) if last_key < key.key_ref() => {
            return false;
        }
        //                          (user_lower, user_upper )
        //     [first_key, last_key ]
        Bound::Excluded(key) if last_key <= key.key_ref() => {
            return false;
        }
        _ => {}
    }

    match user_upper {
        //     (user_lower, user_upper]
        //                               [first_key, last_key ]
        Bound::Included(key) if key.key_ref() < first_key => {
            return false;
        }
        //     (user_lower, user_upper)
        //                            [first_key, last_key ]
        Bound::Excluded(key) if key.key_ref() <= first_key => {
            return false;
        }
        _ => {}
    }

    true
}

impl LsmStorageInner {
    pub(crate) fn next_sst_id(&self) -> usize {
        self.next_sst_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    }

    /// Start the storage engine by either loading an existing directory or creating a new one if the directory does
    /// not exist.
    pub(crate) fn open(path: impl AsRef<Path>, options: LsmStorageOptions) -> Result<Self> {
        let path = path.as_ref();
        let mut state = LsmStorageState::create(&options);
        let block_cache = Arc::new(BlockCache::new(1024));
        let mut next_sst_id = 1;

        if !path.exists() {
            std::fs::create_dir(path)?;
        }

        let compaction_controller = match &options.compaction_options {
            CompactionOptions::Leveled(options) => {
                CompactionController::Leveled(LeveledCompactionController::new(options.clone()))
            }
            CompactionOptions::Tiered(options) => {
                CompactionController::Tiered(TieredCompactionController::new(options.clone()))
            }
            CompactionOptions::Simple(options) => CompactionController::Simple(
                SimpleLeveledCompactionController::new(options.clone()),
            ),
            CompactionOptions::NoCompaction => CompactionController::NoCompaction,
        };

        // create manifest or recover
        let manifest;
        let manifest_path = path.join("MANIFEST");
        if !manifest_path.exists() {
            manifest = Manifest::create(manifest_path)?;
            if options.enable_wal {
                let wal_path = LsmStorageInner::path_of_wal_static(path, 0); // 0 is the id of the first memtable, which is created by LsmStorageState::create()
                state.memtable = Arc::new(MemTable::create_with_wal(0, wal_path)?);
            }
            manifest.add_record_when_init(ManifestRecord::NewMemtable(0))?;
        } else {
            let mut max_sst_id: usize = 0;
            let mut max_memtable_id: usize = 0;
            let mut memtables = BTreeSet::new();
            let (manifest_tmp, records) = Manifest::recover(manifest_path)?;
            manifest = manifest_tmp;
            for record in records {
                match record {
                    ManifestRecord::Flush(sst_id) => {
                        state.l0_sstables.insert(0, sst_id);
                        memtables.remove(&sst_id);
                        max_sst_id = max_sst_id.max(sst_id);
                    }
                    ManifestRecord::Compaction(task, output) => {
                        let (new_state, _del_ssts) = compaction_controller
                            .apply_compaction_result(&state, &task, &output, true);
                        state = new_state;
                        max_sst_id =
                            max_sst_id.max(output.iter().max().copied().unwrap_or_default());
                    }
                    ManifestRecord::NewMemtable(id) => {
                        // state.memtable = Arc::new(MemTable::create(id));
                        memtables.insert(id);
                        max_memtable_id = max_memtable_id.max(id);
                    }
                }
            }

            next_sst_id = max_sst_id.max(max_memtable_id) + 1;

            // create wal or recover
            if options.enable_wal {
                let wal_path = LsmStorageInner::path_of_wal_static(path, next_sst_id);
                state.memtable = Arc::new(MemTable::create_with_wal(next_sst_id, wal_path)?);
                // recover memtable by Wals and add to imm_memtables
                for id in memtables.iter() {
                    let id = *id;
                    let wal_path = Self::path_of_wal_static(path, id);
                    let memtable = MemTable::recover_from_wal(id, wal_path)?;
                    state.imm_memtables.insert(0, Arc::new(memtable));
                }
            } else {
                state.memtable = Arc::new(MemTable::create(next_sst_id));
            }
            manifest.add_record_when_init(ManifestRecord::NewMemtable(next_sst_id))?;
            next_sst_id += 1;

            // recover sstables
            for sst_id in state
                .l0_sstables
                .iter()
                .chain(state.levels.iter().flat_map(|(_, sst_ids)| sst_ids))
            {
                let sst_id = *sst_id;
                let sst = SsTable::open(
                    sst_id,
                    Some(block_cache.clone()),
                    FileObject::open(&LsmStorageInner::path_of_sst_static(path, sst_id))?,
                )
                .with_context(|| format!("fail to recover sstable {:?}", sst_id))?;
                state.sstables.insert(sst_id, Arc::new(sst));
            }

            // only for leveled compaction, sort the sstables of each level
            if let CompactionController::Leveled(_) = compaction_controller {
                for (_level, sst_ids) in &mut state.levels {
                    sst_ids.sort_by(|a, b| {
                        state.sstables[a]
                            .first_key()
                            .cmp(state.sstables[b].first_key())
                    });
                }
                println!("===== After recovery =====");
                println!("L0 = {:?}", state.l0_sstables);
                for i in state.levels.iter() {
                    println!("L{:?} = {:?}", i.0, i.1);
                }
                println!();
            }
        }

        let storage = Self {
            state: Arc::new(RwLock::new(Arc::new(state))),
            state_lock: Mutex::new(()),
            path: path.to_path_buf(),
            block_cache,
            next_sst_id: AtomicUsize::new(next_sst_id),
            compaction_controller,
            manifest: Some(manifest),
            options: options.into(),
            mvcc: Some(LsmMvccInner::new(0)),
            compaction_filters: Arc::new(Mutex::new(Vec::new())),
        };

        Ok(storage)
    }

    pub fn sync(&self) -> Result<()> {
        let snapshot = { self.state.read().clone() };
        snapshot.memtable.sync_wal()?;
        Ok(())
    }

    pub fn add_compaction_filter(&self, compaction_filter: CompactionFilter) {
        let mut compaction_filters = self.compaction_filters.lock();
        compaction_filters.push(compaction_filter);
    }

    pub fn mvcc(&self) -> &LsmMvccInner {
        self.mvcc.as_ref().unwrap()
    }

    /// Get a key from the storage. In day 7, this can be further optimized by using a bloom filter.
    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        let snapshot;
        {
            let state_guard = self.state.read();
            snapshot = Arc::clone(&state_guard);
        }

        let lower = Bound::Included(KeySlice::from_slice_with_ts(key, TS_RANGE_BEGIN));
        let upper = Bound::Included(KeySlice::from_slice_with_ts(key, TS_RANGE_END));
        //first search in mem_table
        let mut memtable_iters = Vec::with_capacity(snapshot.imm_memtables.len() + 1);
        memtable_iters.push(Box::new(snapshot.memtable.scan(lower, upper)));

        //then search in immutable mem_table
        // for imme_table in &storage_guard.imm_memtables {
        for imme_table in snapshot.imm_memtables.iter() {
            memtable_iters.push(Box::new(imme_table.scan(lower, upper)));
        }
        let merge_memtb_iter = MergeIterator::create(memtable_iters);
        while merge_memtb_iter.is_valid() {
            if !merge_memtb_iter.value().is_empty() {
                return Ok(Some(Bytes::copy_from_slice(merge_memtb_iter.value())));
            } else {
                return Ok(None);
            }
            // merge_memtb_iter.next()?;
        }

        //search in l0_Sstable
        let mut sst_iters = Vec::with_capacity(snapshot.l0_sstables.len());
        for sst_idx in snapshot.l0_sstables.iter() {
            let table = snapshot.sstables[sst_idx].clone();
            if !range_overlap(lower, upper, &table) {
                continue;
            }
            if let Some(ref bloom) = table.bloom {
                if !bloom.may_contain(fingerprint32(key)) {
                    continue;
                }
            }
            let iter = SsTableIterator::create_and_seek_to_key(
                table,
                KeySlice::from_slice_with_ts(key, TS_RANGE_BEGIN),
            )?;
            sst_iters.push(Box::new(iter));
        }
        let merge_l0_sst_iters = MergeIterator::create(sst_iters);
        while merge_l0_sst_iters.is_valid() {
            if !merge_l0_sst_iters.value().is_empty() {
                return Ok(Some(Bytes::copy_from_slice(merge_l0_sst_iters.value())));
            } else {
                return Ok(None);
            }
        }

        // create l1-lmax_sst_iter
        let mut l1_to_lmax_sst_concat_iters = Vec::with_capacity(snapshot.levels.len());
        for (_, li_ssts_ids) in snapshot.levels.iter() {
            // add sst_ids in each level for the concat_iter
            let mut li_sstables = Vec::with_capacity(li_ssts_ids.len());
            for sst_id in li_ssts_ids.iter() {
                li_sstables.push(snapshot.sstables[sst_id].clone());
            }
            if !li_sstables.is_empty() {
                let iter = SstConcatIterator::create_and_seek_to_key(
                    li_sstables,
                    KeySlice::from_slice_with_ts(key, TS_RANGE_BEGIN),
                )?;
                l1_to_lmax_sst_concat_iters.push(Box::new(iter));
            }
        }
        let merge_l1_to_lmax_sst_iters = MergeIterator::create(l1_to_lmax_sst_concat_iters);
        while merge_l1_to_lmax_sst_iters.is_valid() {
            if !merge_l0_sst_iters.value().is_empty() {
                return Ok(Some(Bytes::copy_from_slice(
                    merge_l1_to_lmax_sst_iters.value(),
                )));
            } else {
                return Ok(None);
            }
        }
        Ok(None)
    }

    /// Write a batch of data into the storage. Implement in week 2 day 7.
    pub fn write_batch<T: AsRef<[u8]>>(&self, batch: &[WriteBatchRecord<T>]) -> Result<()> {
        let _lk = self.mvcc().write_lock.lock();
        let ts = self.mvcc().latest_commit_ts() + 1;
        for record in batch {
            match record {
                WriteBatchRecord::Put(key, value) => {
                    let key = key.as_ref();
                    let value = value.as_ref();
                    assert!(!key.is_empty(), "key can not be empty");
                    assert!(!value.is_empty(), "value can not be empty");
                    let key_slice = KeySlice::from_slice_with_ts(key, ts);
                    let size;
                    {
                        let storage_guard = self.state.read();
                        storage_guard.memtable.put(key_slice, value)?;
                        size = storage_guard.memtable.approximate_size();
                    }

                    self.try_freeze(size)?;
                }
                WriteBatchRecord::Del(key) => {
                    let key = key.as_ref();
                    assert!(!key.is_empty(), "key can not be empty");
                    let key_slice = KeySlice::from_slice_with_ts(key, ts);
                    let size = {
                        let state_guard = self.state.read();
                        state_guard.memtable.put(key_slice, b"")?;
                        state_guard.memtable.approximate_size()
                    };
                    self.try_freeze(size)?;
                }
            }
        }
        self.mvcc().update_commit_ts(ts);
        Ok(())
    }

    /// Put a key-value pair into the storage by writing into the current memtable.
    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.write_batch(&[WriteBatchRecord::Put(key, value)])
    }

    /// Remove a key from the storage by writing an empty value.
    pub fn delete(&self, key: &[u8]) -> Result<()> {
        self.write_batch(&[WriteBatchRecord::Del(key)])
    }

    pub(crate) fn path_of_sst_static(path: impl AsRef<Path>, id: usize) -> PathBuf {
        path.as_ref().join(format!("{:05}.sst", id))
    }

    pub(crate) fn path_of_sst(&self, id: usize) -> PathBuf {
        Self::path_of_sst_static(&self.path, id)
    }

    pub(crate) fn path_of_wal_static(path: impl AsRef<Path>, id: usize) -> PathBuf {
        path.as_ref().join(format!("{:05}.wal", id))
    }

    pub(crate) fn path_of_wal(&self, id: usize) -> PathBuf {
        Self::path_of_wal_static(&self.path, id)
    }

    pub(super) fn sync_dir(&self) -> Result<()> {
        File::open(&self.path)?.sync_all()?;
        Ok(())
    }

    pub fn try_freeze(&self, estimated_size: usize) -> Result<()> {
        if estimated_size >= self.options.target_sst_size {
            // when 2 threads try_freeze at the same time, use state_lock to synchronize
            // won't block a thread when it check the freeze condition
            let state_lock = self.state_lock.lock();
            let state_guard = self.state.read();
            if state_guard.memtable.approximate_size() >= self.options.target_sst_size {
                drop(state_guard);
                self.force_freeze_memtable(&state_lock)?;
            }
        }
        Ok(())
    }

    pub fn freeze_old_memtable_with_new_memtable(&self, new_memtable: Arc<MemTable>) -> Result<()> {
        let mut state_guard = self.state.write();
        let mut snapshot = state_guard.as_ref().clone();
        let old_memtable = std::mem::replace(&mut snapshot.memtable, new_memtable);

        //snapshot.imm_memtables.insert(0, old_memtable.clone()); 多一个clone()是为什么
        snapshot.imm_memtables.insert(0, old_memtable.clone());

        *state_guard = Arc::new(snapshot);
        drop(state_guard);
        old_memtable.sync_wal()?;
        Ok(())
    }

    /// Force freeze the current memtable to an immutable memtable
    pub fn force_freeze_memtable(&self, state_lock_observer: &MutexGuard<'_, ()>) -> Result<()> {
        let id = self.next_sst_id();
        let new_memtable = if self.options.enable_wal {
            let path = self.path_of_wal(id);
            Arc::new(MemTable::create_with_wal(id, path)?)
        } else {
            Arc::new(MemTable::create(id))
        };
        self.freeze_old_memtable_with_new_memtable(new_memtable)?;
        self.manifest
            .as_ref()
            .unwrap()
            .add_record(state_lock_observer, ManifestRecord::NewMemtable(id))?;
        self.sync_dir()?;
        Ok(())
    }

    /// Force flush the earliest-created immutable memtable to disk
    pub fn force_flush_next_imm_memtable(&self) -> Result<()> {
        let _state_lock = self.state_lock.lock();
        let imm_table;
        {
            let state_guard = self.state.read();
            imm_table = state_guard
                .imm_memtables
                .last()
                .expect("no imm_table in the state!")
                .clone();
        }
        let mut sst_builder = SsTableBuilder::new(self.options.block_size);
        imm_table.flush(&mut sst_builder)?;
        let sst_id = imm_table.id();
        let sst = Arc::new(sst_builder.build(
            sst_id,
            Some(self.block_cache.clone()),
            self.path_of_sst(sst_id),
        )?);
        // println!("flushed {}.sst with size={}", sst_id, sst.table_size());

        {
            let mut state_guard = self.state.write();
            let mut snapshot = state_guard.as_ref().clone();
            snapshot.imm_memtables.pop();
            if self.compaction_controller.flush_to_l0() {
                snapshot.l0_sstables.insert(0, sst_id);
            } else {
                snapshot.levels.insert(0, (sst_id, Vec::from([sst_id])));
            }
            snapshot.sstables.insert(sst_id, sst);
            println!("===== After flush =====");
            println!("L0 : {:?}", snapshot.l0_sstables);
            for (tier, sstables) in snapshot.levels.iter() {
                println!("L{:?} : {:?}", tier, sstables);
            }
            println!();
            *state_guard = Arc::new(snapshot);
        }

        // sync
        self.manifest
            .as_ref()
            .unwrap()
            .add_record(&_state_lock, ManifestRecord::Flush(sst_id))?;
        self.sync_dir()?;
        Ok(())
    }

    pub fn new_txn(&self) -> Result<()> {
        // no-op
        Ok(())
    }

    /// Create an iterator over a range of keys.
    pub fn scan(
        &self,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
    ) -> Result<FusedIterator<LsmIterator>> {
        let snapshot;
        {
            let state_guard = self.state.read();
            snapshot = Arc::clone(&state_guard);
        }

        let end_bound = upper;
        //create merge_mem_iters
        let mut memtable_iters = Vec::with_capacity(snapshot.imm_memtables.len() + 1);
        memtable_iters.push(Box::new(snapshot.memtable.scan(
            map_bound_plus_ts(lower, TS_RANGE_BEGIN),
            map_bound_plus_ts(upper, TS_RANGE_END),
        )));
        for imm_memtable in snapshot.imm_memtables.iter() {
            memtable_iters.push(Box::new(imm_memtable.scan(
                map_bound_plus_ts(lower, TS_RANGE_BEGIN),
                map_bound_plus_ts(upper, TS_RANGE_END),
            )));
        }
        let merge_mem_iters = MergeIterator::create(memtable_iters);

        //create merge_l0_sst_iters
        let mut sst_iters = Vec::with_capacity(snapshot.l0_sstables.len());
        for sst_idx in snapshot.l0_sstables.iter() {
            let table = snapshot.sstables[sst_idx].clone();
            if !range_overlap(
                map_bound_plus_ts(lower, TS_RANGE_BEGIN),
                map_bound_plus_ts(upper, TS_RANGE_END),
                &table,
            ) {
                continue;
            }
            let iter = match lower {
                Bound::Included(x) => SsTableIterator::create_and_seek_to_key(
                    table,
                    KeySlice::from_slice_with_ts(x, TS_RANGE_BEGIN),
                )?,
                Bound::Excluded(x) => {
                    //TODO : note that excluded(x) should seek to the first keyslice of key_ref() and skip to next different key_ref(), if seek to (x, ts_range_end), we may seek to the last key of the table.
                    let mut sst_tmp_iter = SsTableIterator::create_and_seek_to_key(
                        table,
                        KeySlice::from_slice_with_ts(x, TS_RANGE_BEGIN),
                    )?;
                    while sst_tmp_iter.is_valid() && sst_tmp_iter.key().key_ref() == x {
                        sst_tmp_iter.next()?;
                    }
                    sst_tmp_iter
                }
                Bound::Unbounded => SsTableIterator::create_and_seek_to_first(table)?,
            };
            sst_iters.push(Box::new(iter));
        }
        let merge_l0_sst_iters = MergeIterator::create(sst_iters);

        let inner_two_merge_iter = TwoMergeIterator::create(merge_mem_iters, merge_l0_sst_iters)?;

        // create l1-lmax_sst_iter
        let mut l1_to_lmax_sst_concat_iters = Vec::with_capacity(snapshot.levels.len());
        for (_, li_ssts_ids) in snapshot.levels.iter() {
            // add sst_ids in each level for the concat_iter
            let mut li_sstables = Vec::with_capacity(li_ssts_ids.len());
            for sst_id in li_ssts_ids.iter() {
                li_sstables.push(snapshot.sstables[sst_id].clone());
            }
            if !li_sstables.is_empty() {
                let iter = match lower {
                    Bound::Included(x) => SstConcatIterator::create_and_seek_to_key(
                        li_sstables,
                        KeySlice::from_slice_with_ts(x, TS_RANGE_BEGIN),
                    )?,
                    Bound::Excluded(x) => {
                        let mut sst_tmp_iter = SstConcatIterator::create_and_seek_to_key(
                            li_sstables,
                            KeySlice::from_slice_with_ts(x, TS_RANGE_BEGIN),
                        )?;
                        while sst_tmp_iter.is_valid() && sst_tmp_iter.key().key_ref() == x {
                            sst_tmp_iter.next()?;
                        }
                        sst_tmp_iter
                    }
                    Bound::Unbounded => SstConcatIterator::create_and_seek_to_first(li_sstables)?,
                };
                l1_to_lmax_sst_concat_iters.push(Box::new(iter));
            }
        }
        let merge_l1_sst_iters = MergeIterator::create(l1_to_lmax_sst_concat_iters);

        let two_merge_iter = TwoMergeIterator::create(inner_two_merge_iter, merge_l1_sst_iters)?;
        Ok(FusedIterator::new(LsmIterator::new(
            two_merge_iter,
            map_bound(end_bound),
        )?))
        // unimplemented!()
    }
}
