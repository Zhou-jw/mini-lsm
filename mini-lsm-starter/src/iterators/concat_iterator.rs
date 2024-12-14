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
        if sstables.is_empty() {
            return Ok(Self {
                current: None,
                next_sst_idx: 0,
                sstables,
            });
        }

        // let mut sst_iter = SsTableIterator::create_and_seek_to_first(sstables[0].clone())?;
        let mut sst_iter = Self {
            current: Some(SsTableIterator::create_and_seek_to_first(
                sstables[0].clone(),
            )?),
            next_sst_idx: 1,
            sstables,
        };
        sst_iter.move_until_valid()?;
        Ok(sst_iter)
    }

    pub fn create_and_seek_to_key(sstables: Vec<Arc<SsTable>>, key: KeySlice) -> Result<Self> {
        let mut sst_idx = 0;
        let mut iter = None;
        for table in sstables.iter() {
            if table.last_key().as_key_slice() < key {
                sst_idx += 1;
                continue;
            }
            iter = Some(SsTableIterator::create_and_seek_to_key(table.clone(), key)?);
            break;
        }
        let mut sst_iter = Self {
            current: iter,
            next_sst_idx: sst_idx + 1,
            sstables,
        };
        sst_iter.move_until_valid()?;
        Ok(sst_iter)
    }

    fn move_until_valid(&mut self) -> Result<()> {
        loop {
            if self.is_valid() {
                return Ok(());
            }

            if self.next_sst_idx >= self.sstables.len() {
                self.current = None;
                break;
            }

            self.current = Some(SsTableIterator::create_and_seek_to_first(
                self.sstables[self.next_sst_idx].clone(),
            )?);
            self.next_sst_idx += 1;
        }
        Ok(())
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
        self.current.as_mut().unwrap().next()?;
        self.move_until_valid()
    }

    fn num_active_iterators(&self) -> usize {
        1
    }
}
