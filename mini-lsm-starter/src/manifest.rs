use std::fs::OpenOptions;
use std::io::Read;
use std::path::Path;
use std::sync::Arc;
use std::{fs::File, io::Write};

use anyhow::{bail, Context, Result};
use bytes::{Buf, BufMut};
use parking_lot::{Mutex, MutexGuard};
use serde::{Deserialize, Serialize};

use crate::compact::CompactionTask;

pub struct Manifest {
    file: Arc<Mutex<File>>,
}

#[derive(Serialize, Deserialize)]
pub enum ManifestRecord {
    Flush(usize),
    NewMemtable(usize),
    Compaction(CompactionTask, Vec<usize>),
}

impl Manifest {
    pub fn create(_path: impl AsRef<Path>) -> Result<Self> {
        Ok(Manifest {
            file: Arc::new(Mutex::new(
                OpenOptions::new()
                    .read(true)
                    .write(true)
                    .create_new(true)
                    .open(_path)
                    .context("fail to create manifest")?,
            )),
        })
    }

    pub fn recover(path: impl AsRef<Path>) -> Result<(Self, Vec<ManifestRecord>)> {
        // let manifest= Self::create(_path)?;
        let mut file = OpenOptions::new().read(true).append(true).open(path)?; //Note that setting .write(true).append(true) has the same effect as setting only .append(true).
        let mut buf = Vec::new();
        file.read_to_end(&mut buf)?;
        let mut buf_ptr = buf.as_slice();
        let mut records = Vec::new();

        while buf_ptr.has_remaining() {
            let len = buf_ptr.get_u64();
            let slice = &buf_ptr[0..len as usize];
            let chksum = crc32fast::hash(slice);
            buf_ptr.advance(len as usize);
            let read_chksum = buf_ptr.get_u32();
            if chksum != read_chksum {
                bail!("mismatched manifest checksum!");
            }
            let record = serde_json::from_slice::<ManifestRecord>(slice)?;
            records.push(record);
        }

        Ok((
            Self {
                file: Arc::new(Mutex::new(file)),
            },
            records,
        ))
    }

    pub fn add_record(
        &self,
        _state_lock_observer: &MutexGuard<()>,
        record: ManifestRecord,
    ) -> Result<()> {
        self.add_record_when_init(record)
    }

    pub fn add_record_when_init(&self, record: ManifestRecord) -> Result<()> {
        let mut file_guard = self.file.lock();
        let mut buf = serde_json::to_vec(&record)?;
        file_guard.write_all(&(buf.len() as u64).to_be_bytes())?;
        let hash = crc32fast::hash(&buf);
        buf.put_u32(hash);
        file_guard.write_all(&buf)?;
        file_guard.sync_all()?;
        Ok(())
    }
}
