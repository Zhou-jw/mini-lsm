use std::fs::OpenOptions;
use std::io::Read;
use std::path::Path;
use std::sync::Arc;
use std::{fs::File, io::Write};

use anyhow::{Context, Result};
use parking_lot::{Mutex, MutexGuard};
use serde::{Deserialize, Serialize};
use serde_json::Deserializer;

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
                    .create(true)
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
        let stream = Deserializer::from_slice(&buf).into_iter::<ManifestRecord>();
        let mut records = Vec::new();
        for x in stream {
            records.push(x?);
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
        let buf = serde_json::to_vec(&record)?;
        file_guard.write_all(&buf)?;
        file_guard.sync_all()?;
        Ok(())
    }
}
