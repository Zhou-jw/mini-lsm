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
use crate::key::{KeyBytes, KeySlice};
use crate::table::{SIZEOF_U32, SIZEOF_U64};

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

    pub fn recover(path: impl AsRef<Path>, skiplist: &SkipMap<KeyBytes, Bytes>) -> Result<Self> {
        let mut file = OpenOptions::new().read(true).append(true).open(path)?;
        let mut buf = Vec::new();
        file.read_to_end(&mut buf)?;
        let mut rbuf: &[u8] = buf.as_slice();
        let mut batch_begin = SIZEOF_U32;
        while rbuf.has_remaining() {
            let batch_size = rbuf.get_u32();
            let mut batch_end = batch_begin;
            while batch_size > (batch_end - batch_begin) as u32 {
                let key_len = rbuf.get_u16() as usize;
                let key = Bytes::copy_from_slice(&rbuf[..key_len]);
                rbuf.advance(key_len);
                let ts = rbuf.get_u64();
                let key_bytes = KeyBytes::from_bytes_with_ts(key, ts);
                let value_len = rbuf.get_u16() as usize;
                let value = Bytes::copy_from_slice(&rbuf[..value_len]);
                rbuf.advance(value_len);
                batch_end += 2 * SIZEOF_U16 + key_len + SIZEOF_U64 + value_len;

                skiplist.insert(key_bytes, value);
            }
            let checksum = crc32fast::hash(&buf[batch_begin..batch_end]);
            if checksum != rbuf.get_u32() {
                bail!("mismatched wal checksum!");
            }
            batch_begin = batch_end + SIZEOF_U32*2 /* batch_size + checksum */;
        }

        Ok(Self {
            file: Arc::new(Mutex::new(BufWriter::new(file))),
        })
    }

    /// | key_len (exclude ts len) (u16) | key | ts (u64) | value_len (u16) | value | checksum (u32) |
    pub fn put(&self, key: KeySlice, value: &[u8]) -> Result<()> {
        let mut file = self.file.lock();
        let mut buf = Vec::new();
        let estimated_size = SIZEOF_U16 * 2 + key.key_len() + SIZEOF_U64 + value.len() + SIZEOF_U32;
        buf.reserve(estimated_size);
        buf.put_u16(key.key_len() as u16);
        buf.put_slice(key.key_ref());
        buf.put_u64(key.ts());
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
        assert_eq!(
            estimated_size,
            buf.len(),
            "estimated_size != increased_size in wal put()"
        );

        file.write_all(buf.as_slice())
            .context("fail to write to WAL")?;
        Ok(())
    }

    /// Implement this in week 3, day 5.
    /// |   HEADER   |                          BODY                                      |  FOOTER  |
    /// |     u32    |   u16   | var | u64 |    u16    |  var  |           ...            |    u32   |
    /// | batch_size | key_len | key | ts  | value_len | value | more key-value pairs ... | checksum |
    pub fn put_batch(&self, data: &[(KeySlice, &[u8])]) -> Result<()> {
        let mut buf = Vec::new();
        for (key, value) in data {
            buf.put_u16(key.key_len() as u16);
            buf.put_slice(key.key_ref());
            buf.put_u64(key.ts());
            buf.put_u16(value.len() as u16);
            buf.put_slice(value);
        }
        let mut file = self.file.lock();
        // write Header
        file.write_all(&(buf.len() as u32).to_be_bytes())?;
        let chksum = crc32fast::hash(&buf);
        buf.put_u32(chksum);
        file.write_all(buf.as_slice())
            .context("fail to write to WAL")?;
        Ok(())
    }

    pub fn sync(&self) -> Result<()> {
        let mut file = self.file.lock();
        file.flush()?;
        file.get_mut().sync_all()?;
        Ok(())
    }
}
