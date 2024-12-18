#![allow(dead_code)] // REMOVE THIS LINE after fully implementing this functionality

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
        // unimplemented!()
    }

    pub fn recover(_path: impl AsRef<Path>) -> Result<(Self, Vec<ManifestRecord>)> {
        // let manifest= Self::create(_path)?;
        let mut file = OpenOptions::new().read(true).open(_path)?;
        let mut buf = Vec::new();
        file.read_to_end(&mut buf)?;
        let mut stream = Deserializer::from_slice(&buf).into_iter::<ManifestRecord>();
        let mut records = Vec::new();
        while let Some(x) = stream.next() {
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

    pub fn add_record_when_init(&self, _record: ManifestRecord) -> Result<()> {
        let mut file_guard = self.file.lock();
        let buf = serde_json::to_vec(&_record)?;
        file_guard.write_all(&buf)?;
        file_guard.sync_all()?;
        Ok(())
    }
}
