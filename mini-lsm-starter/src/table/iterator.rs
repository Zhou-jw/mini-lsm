use std::sync::Arc;

use anyhow::{Ok, Result};

use super::SsTable;
use crate::{block::BlockIterator, iterators::StorageIterator, key::KeySlice};

/// An iterator over the contents of an SSTable.
pub struct SsTableIterator {
    table: Arc<SsTable>,
    blk_iter: BlockIterator,
    blk_idx: usize,
}

impl SsTableIterator {
    /// Create a new iterator and seek to the first key-value pair in the first data block.
    /// self.data
    /// ---------------------------------------------------------------------------------------------------------------------------------------------
    /// |         Block Section         |                        Meta Section                     |                 Bloom filter Section            |
    /// ---------------------------------------------------------------------------------------------------------------------------------------------
    /// | data block | ... | data block | meta 1 | meta 2| ... |meta n  | meta block offset (u32) | bloom filter | k(u8) | bloom filter offset(u32) |
    /// ---------------------------------------------------------------------------------------------------------------------------------------------
    /// self.meta
    /// -------------------------------------------------------------------------------------------
    /// |                                         Meta#1                                    | ... |
    /// -------------------------------------------------------------------------------------------
    /// | block_offset(u32) | first_key_len(u16) | first_key | last_key_len(u16) | last_key | ... |
    /// -------------------------------------------------------------------------------------------
    pub fn create_and_seek_to_first_inner(table: &Arc<SsTable>) -> Result<(usize, BlockIterator)> {
        Ok((
            0,
            BlockIterator::create_and_seek_to_first(table.read_block_cached(0)?),
        ))
    }

    pub fn create_and_seek_to_first(table: Arc<SsTable>) -> Result<Self> {
        let (blk_idx, blk_iter) = Self::create_and_seek_to_first_inner(&table)?;

        Ok(Self {
            table,
            blk_iter,
            blk_idx,
        })
    }

    /// Seek to the first key-value pair in the first data block.
    pub fn seek_to_first(&mut self) -> Result<()> {
        let (blk_idx, blk_iter) = Self::create_and_seek_to_first_inner(&self.table)?;
        self.blk_idx = blk_idx;
        self.blk_iter = blk_iter;
        Ok(())
    }

    /// Create a new iterator and seek to the first key-value pair which >= `key`.
    pub fn create_and_seek_to_key(table: Arc<SsTable>, key: KeySlice) -> Result<Self> {
        let blk_idx = table.find_block_idx(key);
        let block = table.read_block_cached(blk_idx)?;
        let block_iter = BlockIterator::create_and_seek_to_key(block, key);
        Ok(Self {
            table,
            blk_iter: block_iter,
            blk_idx,
        })
    }

    /// Seek to the first key-value pair which >= `key`.
    /// Note: You probably want to review the handout for detailed explanation when implementing
    /// this function.
    pub fn seek_to_key(&mut self, key: KeySlice) -> Result<()> {
        let mut blk_idx = self.table.find_block_idx(key);
        if blk_idx == 25 {
            blk_idx = self.table.find_block_idx(key);
        }
        let mut block = self.table.read_block_cached(blk_idx)?;
        let mut blk_iter = BlockIterator::create_and_seek_to_key(block, key);

        if !blk_iter.is_valid() {
            blk_idx += 1;
            if blk_idx < self.table.num_of_blocks() {
                block = self.table.read_block_cached(blk_idx)?;
                blk_iter = BlockIterator::create_and_seek_to_first(block);
            }
        }
        self.blk_idx = blk_idx;
        self.blk_iter = blk_iter;
        Ok(())
    }
}

impl StorageIterator for SsTableIterator {
    type KeyType<'a> = KeySlice<'a>;

    /// Return the `key` that's held by the underlying block iterator.
    fn key(&self) -> KeySlice {
        // unimplemented!()
        self.blk_iter.key()
    }

    /// Return the `value` that's held by the underlying block iterator.
    fn value(&self) -> &[u8] {
        self.blk_iter.value()
    }

    /// Return whether the current block iterator is valid or not.
    fn is_valid(&self) -> bool {
        self.blk_iter.is_valid()
    }

    /// Move to the next `key` in the block.
    /// Note: You may want to check if the current block iterator is valid after the move.
    fn next(&mut self) -> Result<()> {
        let max_idx = self.table.block_meta.len() - 1;

        self.blk_iter.next();

        if self.blk_iter.is_valid() {
            return Ok(());
        }

        while self.blk_idx < max_idx {
            self.blk_idx += 1;
            let block = self.table.read_block_cached(self.blk_idx)?;
            let blk_iter = BlockIterator::create_and_seek_to_first(block);
            if blk_iter.is_valid() {
                self.blk_iter = blk_iter;
                return Ok(());
            }
        }

        Ok(())
    }
}
