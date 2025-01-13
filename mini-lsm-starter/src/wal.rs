use std::fs::{File, OpenOptions};
// use std::hash::Hasher;
use std::io::{BufWriter, Read, Write};
use std::path::Path;
use std::sync::Arc;

use anyhow::{bail, Context, Result};
use bytes::{Buf, BufMut, Bytes};
use crossbeam_skiplist::SkipMap;
use parking_lot::Mutex;

use crate::block::SIZEOF_U16;

pub struct Wal {
    file: Arc<Mutex<BufWriter<File>>>,
}

impl Wal {
    pub fn create(path: impl AsRef<Path>) -> Result<Self> {
        // let path = &_path;
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(path)
            .context("fail to create WAL!")?;
        Ok(Self {
            file: Arc::new(Mutex::new(BufWriter::new(file))),
        })
    }

    pub fn recover(path: impl AsRef<Path>, skiplist: &SkipMap<Bytes, Bytes>) -> Result<Self> {
        let mut file = OpenOptions::new().read(true).append(true).open(path)?;
        let mut buf = Vec::new();
        file.read_to_end(&mut buf)?;
        let mut rbuf: &[u8] = buf.as_slice();
        let mut pos = 0;
        while rbuf.has_remaining() {
            let key_len = rbuf.get_u16() as usize;
            let key = Bytes::copy_from_slice(&rbuf[..key_len]);
            rbuf.advance(key_len);
            let value_len = rbuf.get_u16() as usize;
            let value = Bytes::copy_from_slice(&rbuf[..value_len]);
            rbuf.advance(value_len);
            // let mut hasher = crc32fast::Hasher::new();
            // hasher.write_u16(key_len as u16);
            // hasher.write(key.as_ref());
            // hasher.write_u16(value_len as u16);
            // hasher.write(value.as_ref());
            // let checksum = hasher.finalize();
            let end = pos + 2 * SIZEOF_U16 + key_len + value_len;
            let checksum = crc32fast::hash(&buf[pos..end]);
            if checksum != rbuf.get_u32() {
                bail!("mismatched wsl checksum!");
            }
            pos = end + 4;
            skiplist.insert(key, value);
        }

        Ok(Self {
            file: Arc::new(Mutex::new(BufWriter::new(file))),
        })
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        let mut file = self.file.lock();
        let mut buf = Vec::new();
        let estimated_size = 2 + key.len() + 2 + value.len();
        buf.reserve(estimated_size);
        buf.put_u16(key.len() as u16);
        buf.put_slice(key);
        buf.put_u16(value.len() as u16);
        buf.put_slice(value);
        // let mut hasher = crc32fast::Hasher::new();
        // hasher.write_u16(key.len() as u16);
        // hasher.write(key);
        // hasher.write_u16(value.len() as u16);
        // hasher.write(value);
        // buf.put_u32(hasher.finalize());

        let checksum = crc32fast::hash(buf.as_ref());
        buf.put_u32(checksum);

        file.write_all(buf.as_slice())
            .context("fail to write to WAL")?;
        Ok(())
    }

    /// Implement this in week 3, day 5.
    pub fn put_batch(&self, _data: &[(&[u8], &[u8])]) -> Result<()> {
        unimplemented!()
    }

    pub fn sync(&self) -> Result<()> {
        let mut file = self.file.lock();
        file.flush()?;
        file.get_mut().sync_all()?;
        Ok(())
    }
}
