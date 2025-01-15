use std::sync::Arc;

use bytes::Buf;

use crate::{
    key::{KeySlice, KeyVec},
    table::SIZEOF_U64,
};

use super::{Block, SIZEOF_U16};

/// Iterates on a block.
pub struct BlockIterator {
    /// The internal `Block`, wrapped by an `Arc`
    block: Arc<Block>,
    /// The current key, empty represents the iterator is invalid
    key: KeyVec,
    /// the current value range in the block.data, corresponds to the current key
    value_range: (usize, usize),
    /// Current index of the key-value pair, should be in range of [0, num_of_elements)
    idx: usize,
    /// The first key in the block
    first_key: KeyVec,
}

impl Block {
    fn get_first_key(&self) -> KeyVec {
        let mut buf = &self.data[..];

        buf.get_u16(); // jump the key_overlap_len
        let key_len = buf.get_u16() as usize;
        let key = &buf[..key_len];
        let ts = buf.get_u64();
        KeyVec::from_vec_with_ts(key.to_vec(), ts)
    }
}

impl BlockIterator {
    fn new(block: Arc<Block>) -> Self {
        Self {
            first_key: block.get_first_key(),
            block,
            key: KeyVec::new(),
            value_range: (0, 0),
            idx: 0,
        }
    }

    /// Creates a block iterator and seek to the first entry.
    pub fn create_and_seek_to_first(block: Arc<Block>) -> Self {
        // unimplemented!()
        let mut iter = Self::new(block);
        iter.seek_to_first();
        iter
    }

    /// Creates a block iterator and seek to the first key that >= `key`.
    pub fn create_and_seek_to_key(block: Arc<Block>, key: KeySlice) -> Self {
        let mut iter = Self::new(block);
        iter.seek_to_key(key);
        iter
    }

    /// Returns the key of the current entry.
    pub fn key(&self) -> KeySlice {
        self.key.as_key_slice()
    }

    /// Returns the value of the current entry.
    pub fn value(&self) -> &[u8] {
        &self.block.data[self.value_range.0..self.value_range.1]
    }

    /// Returns true if the iterator is valid.
    /// Note: You may want to make use of `key`
    pub fn is_valid(&self) -> bool {
        // unimplemented!()
        !self.key.is_empty()
    }

    /// Seek to ith-key in the block
    pub fn seek_to(&mut self, i: usize) {
        if i >= self.block.offsets.len() {
            self.key.clear();
            self.value_range = (0, 0);
            return;
        }

        let offset = self.block.offsets[i] as usize;

        self.seek_to_offset(offset);
        self.idx = i;
    }

    /// Seek to the specific position and update current 'key' & 'value'
    pub fn seek_to_offset(&mut self, offset: usize) {
        let mut entry = &self.block.data[offset..];

        let key_overlap_len = entry.get_u16() as usize;
        let res_len = entry.get_u16() as usize;

        let key = &entry[..res_len];
        entry.advance(res_len);
        self.key.clear();
        self.key
            .append(&self.first_key.key_ref()[..key_overlap_len]);
        self.key.append(key);
        let ts = entry.get_u64();
        let value_len = entry.get_u16() as usize;
        let value_offset_begin = offset + SIZEOF_U16 * 2 + res_len + SIZEOF_U64 + SIZEOF_U16;
        let value_offset_end = value_offset_begin + value_len;

        self.value_range = (value_offset_begin, value_offset_end);
        entry.advance(value_len); //any effect?
    }

    /// Seeks to the first key in the block.
    pub fn seek_to_first(&mut self) {
        self.seek_to(0);
    }

    /// Move to the next key in the block.
    pub fn next(&mut self) {
        self.idx += 1;
        self.seek_to(self.idx);
    }

    /// Seek to the first key that >= `key`.
    /// Note: You should assume the key-value pairs in the block are sorted when being added by
    /// callers.
    pub fn seek_to_key(&mut self, key: KeySlice) {
        let mut low = 0;
        let mut high = self.block.offsets.len();
        while low < high {
            let mid = low + (high - low) / 2;
            self.seek_to(mid);
            assert!(self.is_valid());
            match self.key().cmp(&key) {
                std::cmp::Ordering::Less => low = mid + 1,
                std::cmp::Ordering::Greater => high = mid,
                std::cmp::Ordering::Equal => return,
            }
        }
        self.seek_to(low);

        // if key doesn't exist, it will seek to the last_key in this block
        // let mut offset: usize;
        // for i in 0..self.block.offsets.len() {
        //     offset = self.block.offsets[i] as usize;
        //     self.seek_to_offset(offset);
        //     if self.key() >= key {
        //         break;
        //     }
        // }
    }
}
