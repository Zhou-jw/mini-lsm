#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::path::Path;
use std::sync::Arc;

use anyhow::Result;
use bytes::BufMut;

use super::{BlockMeta, FileObject, SsTable};
use crate::{
    block::BlockBuilder,
    key::{KeySlice, KeyVec},
    lsm_storage::BlockCache,
};

/// Builds an SSTable from key-value pairs.
pub struct SsTableBuilder {
    builder: BlockBuilder,
    first_key: Vec<u8>,
    last_key: Vec<u8>,
    data: Vec<u8>,
    pub(crate) meta: Vec<BlockMeta>,
    block_size: usize,
}

impl SsTableBuilder {
    /// Create a builder based on target block size.
    pub fn new(block_size: usize) -> Self {
        Self {
            builder: BlockBuilder::new(block_size),
            first_key: Vec::new(),
            last_key: Vec::new(),
            data: Vec::new(),
            meta: Vec::new(),
            block_size,
        }
    }

    /// Adds a key-value pair to SSTable.
    ///
    /// Note: You should split a new block when the current block is full.(`std::mem::replace` may
    /// be helpful here)
    pub fn add(&mut self, key: KeySlice, value: &[u8]) {
        // if first_key is empty, (k, v) is the first kv pair
        if self.first_key.is_empty() {
            self.first_key.clear();
            self.first_key.extend(key.into_inner());
        }

        // if successfully add(k, v), update lask_key
        if self.builder.add(key, value) {
            self.last_key.clear();
            self.last_key.extend(key.into_inner());
            return;
        }

        // create new blockbuilder and swap with self.builder
        self.finish_block();

        //update first_key and last_key
        self.first_key.clear();
        self.first_key.extend(key.into_inner());
        self.last_key.clear();
        self.last_key.extend(key.into_inner());
    }

    pub fn finish_block(&mut self) {
        // note that builder.build() will consume self.builder, which is referenced in add(&mut self)
        // so we can't just call builder.build() without assign a new builder to self.builder
        // std::mem::replace() is really helpful
        let block = std::mem::replace(&mut self.builder, BlockBuilder::new(self.block_size));
        let encoded_data = block.build().encode();
        // let x = std::mem::take(&mut self.first_key).as_slice();
        self.meta.push(BlockMeta {
            offset: self.data.len(),
            first_key: KeyVec::from_vec(std::mem::take(&mut self.first_key)).into_key_bytes(),
            last_key: KeyVec::from_vec(std::mem::take(&mut self.last_key)).into_key_bytes(),
        });
        self.data.extend(encoded_data); //TODO why return a Bytes and extend it to data instead of returning a Vec?
    }

    /// Get the estimated size of the SSTable.
    ///
    /// Since the data blocks contain much more data than meta blocks, just return the size of data
    /// blocks here.
    pub fn estimated_size(&self) -> usize {
        self.data.len()
    }

    /// Builds the SSTable and writes it to the given path. Use the `FileObject` structure to manipulate the disk objects.
    pub fn build(
        mut self,
        id: usize,
        block_cache: Option<Arc<BlockCache>>,
        path: impl AsRef<Path>,
    ) -> Result<SsTable> {
        // -------------------------------------------------------------------------------------------
        // |         Block Section         |          Meta Section         |          Extra          |
        // -------------------------------------------------------------------------------------------
        // | data block | ... | data block | meta 1 | meta 2| ... |meta n  | meta block offset (u32) |
        // -------------------------------------------------------------------------------------------
        self.finish_block();

        let block_meta_offset = self.data.len();

        let mut buf = self.data;
        BlockMeta::encode_block_meta(&self.meta, &mut buf);

        buf.put_u32(self.meta.len() as u32);

        let file = FileObject::create(path.as_ref(), buf)?;

        Ok(SsTable {
            file,
            id,
            first_key: self.meta.first().unwrap().first_key.clone(),
            last_key: self.meta.last().unwrap().last_key.clone(),
            bloom: None,
            block_cache,
            block_meta: self.meta,
            block_meta_offset,
            max_ts: 0,
        })
    }

    #[cfg(test)]
    pub(crate) fn build_for_test(self, path: impl AsRef<Path>) -> Result<SsTable> {
        self.build(0, None, path)
    }
}
