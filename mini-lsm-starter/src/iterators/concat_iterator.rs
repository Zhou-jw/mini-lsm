#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::sync::Arc;

use anyhow::Result;

use super::StorageIterator;
use crate::{
    key::KeySlice,
    table::{SsTable, SsTableIterator},
};

/// Concat multiple iterators ordered in key order and their key ranges do not overlap. We do not want to create the
/// iterators when initializing this iterator to reduce the overhead of seeking.
pub struct SstConcatIterator {
    current: Option<SsTableIterator>,
    next_sst_idx: usize,
    sstables: Vec<Arc<SsTable>>,
}

impl SstConcatIterator {
    pub fn create_and_seek_to_first(sstables: Vec<Arc<SsTable>>) -> Result<Self> {
        let table = sstables.first().unwrap().clone();
        let sst_iter = SsTableIterator::create_and_seek_to_first(table)?;
        Ok(Self {
            current: Some(sst_iter),
            next_sst_idx: 1,
            sstables: sstables,
        })
    }

    pub fn create_and_seek_to_key(sstables: Vec<Arc<SsTable>>, key: KeySlice) -> Result<Self> {
        let sst_idx = 0;
        let mut sst_iter = None;
        for table in sstables.iter() {
            if table.last_key().as_key_slice() < key {
                continue;
            }
            sst_iter = Some(SsTableIterator::create_and_seek_to_key(table.clone(), key)?);
            break;
        }
        Ok(Self {
            current: sst_iter,
            next_sst_idx: 1,
            sstables: sstables,
        })
    }
}

impl StorageIterator for SstConcatIterator {
    type KeyType<'a> = KeySlice<'a>;

    fn key(&self) -> KeySlice {
        self.current.as_ref().unwrap().key()
    }

    fn value(&self) -> &[u8] {
        self.current.as_ref().unwrap().value()
    }

    fn is_valid(&self) -> bool {
        if let Some(sst_iter) = &self.current {
            return sst_iter.is_valid();
        }
        false
    }

    fn next(&mut self) -> Result<()> {
        self.current.as_mut().unwrap().next()
    }

    fn num_active_iterators(&self) -> usize {
        self.current.as_ref().unwrap().num_active_iterators()
    }
}
