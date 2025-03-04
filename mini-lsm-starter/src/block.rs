#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

mod builder;
mod iterator;

pub use builder::BlockBuilder;
use bytes::{Buf, BufMut, Bytes};
pub use iterator::BlockIterator;

pub(crate) const SIZEOF_U16: usize = std::mem::size_of::<u16>();
const BLOCK_SIZE: usize = 4096;
/// A block is the smallest unit of read and caching in LSM tree. It is a collection of sorted key-value pairs.
pub struct Block {
    pub(crate) data: Vec<u8>,
    pub(crate) offsets: Vec<u16>,
}

impl Block {
    /// Encode the internal data to the data layout illustrated in the tutorial
    /// Note: You may want to recheck if any of the expected field is missing from your output
    pub fn encode(&self) -> Bytes {
        let mut buf = self.data.clone();
        let offsets_len = self.offsets.len();
        for offset in &self.offsets {
            buf.put_u16(*offset);
        }
        // ----------------------------------------------------------------------------------------------------
        // |             Data Section             |              Offset Section             |      Extra      |
        // ----------------------------------------------------------------------------------------------------
        // | Entry #1 | Entry #2 | ... | Entry #N | Offset #1 | Offset #2 | ... | Offset #N | num_of_elements |
        // ----------------------------------------------------------------------------------------------------

        buf.put_u16(offsets_len as u16);
        buf.into()
    }

    /// Decode from the data layout, transform the input `data` to a single `Block`
    pub fn decode(data: &[u8]) -> Self {
        let num_of_elements = (&data[data.len() - SIZEOF_U16..]).get_u16() as usize;
        let data_end = data.len() - SIZEOF_U16 - num_of_elements * SIZEOF_U16;
        let raw_offsets = &data[data_end..data.len() - SIZEOF_U16];
        let offsets = raw_offsets
            .chunks(SIZEOF_U16)
            .map(|mut x| x.get_u16())
            .collect();
        let data = &data[..data_end];
        Self {
            data: data.to_vec(),
            offsets,
        }
    }
}
