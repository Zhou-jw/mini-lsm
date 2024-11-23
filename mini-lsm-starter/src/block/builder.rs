// #![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
// #![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

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

impl BlockBuilder {
    /// Creates a new block builder.
    pub fn new(block_size: usize) -> Self {
        Self {
            offsets: Vec::new(),
            data: Vec::new(),
            block_size: block_size, // since self.block_size has the same name , could rewrite just "block_size",
            first_key: KeyVec::new(),
        }
    }

    /// Adds a key-value pair to the block. Returns false when the block is full.
    #[must_use]
    pub fn add(&mut self, key: KeySlice, value: &[u8]) -> bool {

        if key.is_empty() {
            println!("block build add key is empty");
            return false;
        }
        // key_len + value_len + offset_len
        let add_size = key.len() + value.len() + SIZEOF_U16*3;
        let cur_size = self.estimate_size();
        
        // if empty sapce is not enough
        if cur_size + add_size > self.block_size {
            return false;
        }

        self.offsets.push(self.data.len() as u16);
        self.data.put_u16(key.len() as u16); 
        self.data.put(key.into_inner());
        self.data.put_u16(value.len() as u16);
        self.data.put(value);

        true
    }

    /// Check if there is no key-value pair in the block.
    pub fn is_empty(&self) -> bool {
        // unimplemented!()
        // self.offsets.is_empty()
        self.data.len() == 0
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

    fn estimate_size(&self) -> usize {
        // Extra.size +  offset_section.size + data_section.size
        SIZEOF_U16 + self.offsets.len() * SIZEOF_U16 + self.data.len()
    }
}
