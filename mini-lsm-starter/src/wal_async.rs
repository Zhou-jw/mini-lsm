use std::path::Path;

use anyhow::{bail, Context, Result};
use bytes::{Buf, BufMut, Bytes};
use crossbeam_skiplist::SkipMap;

use crate::block::SIZEOF_U16;
use crate::key::{KeyBytes, KeySlice};
use crate::table::{SIZEOF_U32, SIZEOF_U64};

pub struct AsyncWal {
    file: tokio_uring::fs::File,
    buffered: Vec<u8>,
    offset: u64,
}

impl AsyncWal {
    pub async fn create(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        
        tokio_uring::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(path)
            .await
            .context("fail to create WAL!")?;

        let file = tokio_uring::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .append(true)
            .open(path)
            .await
            .context("fail to open WAL!")?;

        Ok(Self {
            file,
            buffered: Vec::with_capacity(64 * 1024),
            offset: 0,
        })
    }

    pub async fn recover(path: impl AsRef<Path>, skiplist: &SkipMap<KeyBytes, Bytes>) -> Result<Self> {
        use std::fs::File;
        use std::io::Read;

        let path = path.as_ref();
        let mut file = File::open(path)?;
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
            batch_begin = batch_end + SIZEOF_U32 * 2;
        }

        let file = tokio_uring::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .append(true)
            .open(path)
            .await
            .context("fail to open WAL!")?;

        Ok(Self {
            file,
            buffered: Vec::with_capacity(64 * 1024),
            offset: buf.len() as u64,
        })
    }

    pub async fn put(&mut self, key: KeySlice<'_>, value: &[u8]) -> Result<()> {
        self.buffered.put_u16(key.key_len() as u16);
        self.buffered.put_slice(key.key_ref());
        self.buffered.put_u64(key.ts());
        self.buffered.put_u16(value.len() as u16);
        self.buffered.put_slice(value);
        Ok(())
    }

    pub async fn flush(&mut self) -> Result<()> {
        if self.buffered.is_empty() {
            return Ok(());
        }

        let batch_size = self.buffered.len() as u32;
        let chksum = crc32fast::hash(&self.buffered);

        let mut combined = Vec::with_capacity(4 + self.buffered.len() + 4);
        combined.extend_from_slice(&batch_size.to_be_bytes());
        combined.extend_from_slice(&self.buffered);
        combined.extend_from_slice(&chksum.to_be_bytes());

        let write_len = combined.len();
        let (res, _buf) = self.file.write_at(combined, self.offset).submit().await;
        res.map_err(|e| anyhow::anyhow!("write error: {}", e))?;
        
        self.offset += write_len as u64;
        self.buffered.clear();

        Ok(())
    }

    pub async fn sync(&mut self) -> Result<()> {
        self.file.sync_all().await.map_err(|e| anyhow::anyhow!("sync error: {}", e))?;
        Ok(())
    }
}