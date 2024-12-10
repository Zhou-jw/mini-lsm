use bytes::BufMut;

use crate::key::{KeySlice, KeyVec};

use super::{Block, SIZEOF_U16};

/// Builds a block.
pub struct BlockBuilder {
    /// Offsets of each key-value entries.
    offsets: Vec<u16>,
    /// All serialized key-value pairs in the block.
    data: Vec<u8>,
    /// The expected block size.
    block_size: usize,
    /// The first key in the block
    first_key: KeyVec,
}

pub fn compute_key_overlap(first_key: &[u8], key: &[u8]) -> usize {
    let mut i: usize = 0;
    loop {
        if i >= first_key.len() || i >= key.len() {
            break;
        }

        if first_key[i] != key[i] {
            break;
        }
        i += 1;
    }
    i
}

impl BlockBuilder {
    /// Creates a new block builder.
    pub fn new(block_size: usize) -> Self {
        Self {
            offsets: Vec::new(),
            data: Vec::new(),
            block_size, // since self.block_size has the same name , could rewrite just "block_size",
            first_key: KeyVec::new(),
        }
    }

    /// Adds a key-value pair to the block. Returns false when the block is full.
    #[must_use]
    pub fn add(&mut self, key: KeySlice, value: &[u8]) -> bool {
        if key.is_empty() {
            println!("block build try to add empty key");
            return false;
        }

        // key_len + value_len + offset_len
        let add_size = key.len() + value.len() + SIZEOF_U16 * 3;
        let cur_size = self.estimated_size();

        // if empty sapce is not enough //TODO: why allow insert large k-v pairs when self.is_empty()?
        if cur_size + add_size > self.block_size && !self.is_empty() {
            return false;
        }

        let key_overlap_len = compute_key_overlap(self.first_key.raw_ref(), key.into_inner());
        let res_len = key.len() - key_overlap_len;
        let res_key = &key.into_inner()[key_overlap_len..];

        self.offsets.push(self.data.len() as u16);
        self.data.put_u16(key_overlap_len as u16);
        self.data.put_u16(res_len as u16);
        self.data.put(res_key);
        self.data.put_u16(value.len() as u16);
        self.data.put(value);

        //update first key
        if self.first_key.is_empty() {
            self.first_key = key.to_key_vec();
        }
        true
    }

    /// Check if there is no key-value pair in the block.
    pub fn is_empty(&self) -> bool {
        // unimplemented!()
        self.offsets.is_empty()
        // self.data.len() == 0
    }

    /// Finalize the block.
    pub fn build(self) -> Block {
        if self.is_empty() {
            panic!("Block should not be empty");
        }
        Block {
            data: self.data,
            offsets: self.offsets,
        }
    }

    fn estimated_size(&self) -> usize {
        // Extra.size +  offset_section.size + data_section.size
        SIZEOF_U16 + self.offsets.len() * SIZEOF_U16 + self.data.len()
    }
}
