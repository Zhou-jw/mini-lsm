use std::path::Path;
use std::sync::Arc;

use anyhow::Result;
use bytes::BufMut;
use farmhash::fingerprint32;

use super::{bloom::Bloom, BlockMeta, FileObject, SsTable};
use crate::{
    block::BlockBuilder,
    key::{KeySlice, KeyVec},
    lsm_storage::BlockCache,
};

/// Builds an SSTable from key-value pairs.
pub struct SsTableBuilder {
    builder: BlockBuilder,
    first_key: KeyVec,
    last_key: KeyVec,
    data: Vec<u8>,
    pub(crate) meta: Vec<BlockMeta>,
    block_size: usize,
    key_hashes: Vec<u32>,
    max_ts: u64,
}

impl SsTableBuilder {
    /// Create a builder based on target block size.
    pub fn new(block_size: usize) -> Self {
        Self {
            builder: BlockBuilder::new(block_size),
            first_key: KeyVec::new(),
            last_key: KeyVec::new(),
            data: Vec::new(),
            meta: Vec::new(),
            block_size,
            key_hashes: Vec::new(),
            max_ts: 0,
        }
    }

    /// Adds a key-value pair to SSTable.
    ///
    /// Note: You should split a new block when the current block is full.(`std::mem::replace` may
    /// be helpful here)
    pub fn add(&mut self, key: KeySlice, value: &[u8]) {
        // if key.ts larger than sst_builder.max_ts,, update
        if key.ts() > self.max_ts {
            self.max_ts = key.ts();
        }

        // if first_key is empty, (k, v) is the first kv pair
        if self.first_key.is_empty() {
            self.first_key.clear();
            self.first_key.set_from_slice(key);
        }

        self.key_hashes.push(fingerprint32(key.key_ref()));

        // if successfully add(k, v), update lask_key
        if self.builder.add(key, value) {
            self.last_key.clear();
            self.last_key.set_from_slice(key);
            return;
        }

        // create new blockbuilder and swap with self.builder
        self.finish_block();

        //insert kv again and update first_key and last_key
        assert!(self.builder.add(key, value));
        self.first_key.clear();
        self.first_key.set_from_slice(key);
        self.last_key.clear();
        self.last_key.set_from_slice(key);
    }

    pub fn finish_block(&mut self) {
        // note that builder.build() will consume self.builder, which is referenced in add(&mut self)
        // so we can't just call builder.build() without assign a new builder to self.builder
        // std::mem::replace() is really helpful
        let block = std::mem::replace(&mut self.builder, BlockBuilder::new(self.block_size));
        let encoded_data = block.build().encode();
        let check_sum = crc32fast::hash(&encoded_data);
        self.meta.push(BlockMeta {
            offset: self.data.len(),
            first_key: std::mem::take(&mut self.first_key).into_key_bytes(),
            last_key: std::mem::take(&mut self.last_key).into_key_bytes(),
        });
        self.data.extend(encoded_data); //TODO why return a Bytes and extend it to data instead of returning a Vec?
        self.data.put_u32(check_sum);
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
        //                                 |---meta block offset                                                                            |---bloom filter offset
        //                                 V                                                                                                v
        // --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
        // |         Block Section         |                 Meta Section                          |                     Extra              |                         Bloom Filter Section                  |
        // --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
        // | data block | ... | data block | meta_num |meta 1 | meta 2| ... | meta n | meta_chksum | meta block offset (u32) |  max_ts(u64) | bloom filter | k(u8) | bloom chksum | bloom block offset(u32) |
        // --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
        if !self.builder.is_empty() {
            self.finish_block();
        }

        // encode meta section
        let block_meta_offset = self.data.len();
        let mut buf = self.data;
        BlockMeta::encode_block_meta(&self.meta, &mut buf);
        // encode meta block offset
        buf.put_u32(block_meta_offset as u32);

        // encode max_ts
        buf.put_u64(self.max_ts);

        // build bloom filter
        let entries = self.key_hashes.len();
        let bits_per_key = Bloom::bloom_bits_per_key(entries, 0.01);
        let bloom = Bloom::build_from_key_hashes(self.key_hashes.as_ref(), bits_per_key);

        // encode bloom filter section
        let meta_bloom_offset = buf.len();
        bloom.encode(&mut buf); // put_u8 in encode()
        buf.put_u32(meta_bloom_offset as u32);

        // create file
        let file = FileObject::create(path.as_ref(), buf)?;

        let first_key = self.meta.first().unwrap().first_key.clone();
        let last_key = self.meta.last().unwrap().last_key.clone();
        println!(
            "build {:?} , key range from {:?}, ts:{:?} to {:?}, ts: {:?}",
            id,
            &first_key.key_ref()[first_key.key_len().saturating_sub(6)..],
            first_key.ts(),
            &last_key.key_ref()[last_key.key_len().saturating_sub(6)..],
            last_key.ts()
        );

        assert!(
            first_key <= last_key,
            "wrong key order when building sstable! sst_id = {:?}, \nfirst_key: {:?}, \nlast_key: {:?}\n",
            id, &first_key.key_ref()[first_key.key_len().saturating_sub(6)..] , &last_key.key_ref()[last_key.key_len().saturating_sub(6)..]
        );

        Ok(SsTable {
            file,
            id,
            first_key,
            last_key,
            bloom: Some(bloom),
            block_cache,
            block_meta: self.meta,
            block_meta_offset,
            max_ts: self.max_ts,
        })
    }

    #[cfg(test)]
    pub(crate) fn build_for_test(self, path: impl AsRef<Path>) -> Result<SsTable> {
        self.build(0, None, path)
    }
}
