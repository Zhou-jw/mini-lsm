#![allow(dead_code)] // REMOVE THIS LINE after fully implementing this functionality

mod leveled;
mod simple_leveled;
mod tiered;

use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
pub use leveled::{LeveledCompactionController, LeveledCompactionOptions, LeveledCompactionTask};
use serde::{Deserialize, Serialize};
pub use simple_leveled::{
    SimpleLeveledCompactionController, SimpleLeveledCompactionOptions, SimpleLeveledCompactionTask,
};
pub use tiered::{TieredCompactionController, TieredCompactionOptions, TieredCompactionTask};

use crate::iterators::concat_iterator::SstConcatIterator;
use crate::iterators::merge_iterator::MergeIterator;
use crate::iterators::two_merge_iterator::TwoMergeIterator;
use crate::iterators::StorageIterator;
use crate::key::KeySlice;
use crate::lsm_storage::{LsmStorageInner, LsmStorageState};
use crate::manifest::ManifestRecord;
use crate::table::{SsTable, SsTableBuilder, SsTableIterator};

#[derive(Debug, Serialize, Deserialize)]
pub enum CompactionTask {
    Leveled(LeveledCompactionTask),
    Tiered(TieredCompactionTask),
    Simple(SimpleLeveledCompactionTask),
    ForceFullCompaction {
        l0_sstables: Vec<usize>,
        l1_sstables: Vec<usize>,
    },
}

impl CompactionTask {
    fn compact_to_bottom_level(&self) -> bool {
        match self {
            CompactionTask::ForceFullCompaction { .. } => true,
            CompactionTask::Leveled(task) => task.is_lower_level_bottom_level,
            CompactionTask::Simple(task) => task.is_lower_level_bottom_level,
            CompactionTask::Tiered(task) => task.bottom_tier_included,
        }
    }
}

pub(crate) enum CompactionController {
    Leveled(LeveledCompactionController),
    Tiered(TieredCompactionController),
    Simple(SimpleLeveledCompactionController),
    NoCompaction,
}

impl CompactionController {
    pub fn generate_compaction_task(&self, snapshot: &LsmStorageState) -> Option<CompactionTask> {
        match self {
            CompactionController::Leveled(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Leveled),
            CompactionController::Simple(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Simple),
            CompactionController::Tiered(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Tiered),
            CompactionController::NoCompaction => unreachable!(),
        }
    }

    pub fn apply_compaction_result(
        &self,
        snapshot: &LsmStorageState,
        task: &CompactionTask,
        output: &[usize],
        in_recovery: bool,
    ) -> (LsmStorageState, Vec<usize>) {
        match (self, task) {
            (CompactionController::Leveled(ctrl), CompactionTask::Leveled(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output, in_recovery)
            }
            (CompactionController::Simple(ctrl), CompactionTask::Simple(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output)
            }
            (CompactionController::Tiered(ctrl), CompactionTask::Tiered(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output)
            }
            _ => unreachable!(),
        }
    }
}

impl CompactionController {
    pub fn flush_to_l0(&self) -> bool {
        matches!(
            self,
            Self::Leveled(_) | Self::Simple(_) | Self::NoCompaction
        )
    }
}

#[derive(Debug, Clone)]
pub enum CompactionOptions {
    /// Leveled compaction with partial compaction + dynamic level support (= RocksDB's Leveled
    /// Compaction)
    Leveled(LeveledCompactionOptions),
    /// Tiered compaction (= RocksDB's universal compaction)
    Tiered(TieredCompactionOptions),
    /// Simple leveled compaction
    Simple(SimpleLeveledCompactionOptions),
    /// In no compaction mode (week 1), always flush to L0
    NoCompaction,
}

impl LsmStorageInner {
    fn compact_generate_ssts_from_iter(
        &self,
        mut iter: impl for<'a> StorageIterator<KeyType<'a> = KeySlice<'a>>,
    ) -> Result<Vec<Arc<SsTable>>> {
        // if lots of the same keys exist, simply retain the latest one.
        // if the latest version is a delete marker, we do not need to keep it in the produced SST files.
        let target_sst_size = self.options.target_sst_size;
        let block_size = self.options.block_size;
        let mut sstables = Vec::new();
        let mut sst_builder = None;
        let mut prev_key = Vec::<u8>::new();
        while iter.is_valid() {
            if sst_builder.is_none() {
                sst_builder = Some(SsTableBuilder::new(block_size));
            }

            let is_same_as_prevkey = prev_key == iter.key().key_ref();
            let builder_inner = sst_builder.as_mut().unwrap();
            builder_inner.add(iter.key(), iter.value());

            if builder_inner.estimated_size() >= target_sst_size && !is_same_as_prevkey {
                let next_sst_id = self.next_sst_id();
                let block_cache = self.block_cache.clone();
                let builder = sst_builder.take().unwrap();
                // println!("compact generate sst {:?}", next_sst_id);
                sstables.push(Arc::new(builder.build(
                    next_sst_id,
                    Some(block_cache),
                    self.path_of_sst(next_sst_id),
                )?));
            }

            iter.next()?;

            if !is_same_as_prevkey {
                prev_key.clear();
                prev_key.extend(iter.key().key_ref());
            }
        }

        if let Some(builder) = sst_builder {
            let next_sst_id = self.next_sst_id();
            // println!("compact generate sst {:?}", next_sst_id);
            let block_cache = self.block_cache.clone();
            sstables.push(Arc::new(builder.build(
                next_sst_id,
                Some(block_cache),
                self.path_of_sst(next_sst_id),
            )?));
        }

        Ok(sstables)
    }

    fn compact(&self, task: &CompactionTask) -> Result<Vec<Arc<SsTable>>> {
        let snapshot;
        {
            let state_guard = self.state.read();
            snapshot = state_guard.clone();
        }

        match task {
            CompactionTask::ForceFullCompaction {
                l0_sstables,
                l1_sstables,
            } => {
                //create merge_sst_iters
                let mut sst_iters = Vec::with_capacity(l0_sstables.len() + l1_sstables.len());
                for sst_idx in l0_sstables.iter() {
                    let table = snapshot.sstables[sst_idx].clone();
                    let iter = SsTableIterator::create_and_seek_to_first(table)?;
                    sst_iters.push(Box::new(iter));
                }

                //create sst_concate_iter
                let mut l1_ssts = Vec::with_capacity(l1_sstables.len());
                for sst_idx in l1_sstables.iter() {
                    l1_ssts.push(snapshot.sstables[sst_idx].clone());
                }
                let sst_concate_iter = SstConcatIterator::create_and_seek_to_first(l1_ssts)?;

                let iter =
                    TwoMergeIterator::create(MergeIterator::create(sst_iters), sst_concate_iter)?;
                self.compact_generate_ssts_from_iter(iter)
            }
            CompactionTask::Simple(simple_task) => {
                match simple_task.upper_level {
                    None => {
                        //note that snapshot.l0_sstables may be larger than the pre-snapshot when task was generated
                        let l0_sstables = simple_task.upper_level_sst_ids.clone();
                        let l1_sstables = simple_task.lower_level_sst_ids.clone();
                        // assert_eq!(l0_sstables, snapshot.l0_sstables);
                        // println!("compact L0={:?} to L1{:?}", l0_sstables, l1_sstables);
                        //create merge_sst_iters
                        let mut sst_iters = Vec::with_capacity(l0_sstables.len());
                        for sst_idx in l0_sstables.iter() {
                            let table = snapshot.sstables[sst_idx].clone();
                            let iter = SsTableIterator::create_and_seek_to_first(table)?;
                            sst_iters.push(Box::new(iter));
                        }
                        //create sst_concate_iter
                        let mut l1_ssts = Vec::with_capacity(l1_sstables.len());
                        for sst_idx in l1_sstables.iter() {
                            l1_ssts.push(snapshot.sstables[sst_idx].clone());
                        }
                        let sst_concate_iter =
                            SstConcatIterator::create_and_seek_to_first(l1_ssts)?;
                        let iter = TwoMergeIterator::create(
                            MergeIterator::create(sst_iters),
                            sst_concate_iter,
                        )?;
                        self.compact_generate_ssts_from_iter(iter)
                    }
                    Some(_) => {
                        let upper_sst_ids = simple_task.upper_level_sst_ids.clone();
                        let lower_sst_ids = simple_task.lower_level_sst_ids.clone();
                        // println!("== Before compact ==");
                        // println!("L0 = {:?}", snapshot.l0_sstables);
                        // for i in snapshot.levels.iter() {
                        //     println!("L{:?} = {:?}", i.0, i.1);
                        // }

                        // println!(
                        //     "compact L{:?}={:?} to L{:?}={:?} \n",
                        //     simple_task.lower_level - 1,
                        //     upper_sst_ids,
                        //     simple_task.lower_level,
                        //     lower_sst_ids
                        // );

                        let mut upper_ssts = Vec::with_capacity(upper_sst_ids.len());
                        for sst_idx in upper_sst_ids.iter() {
                            upper_ssts.push(snapshot.sstables[sst_idx].clone());
                        }
                        let upper_concat_iter =
                            SstConcatIterator::create_and_seek_to_first(upper_ssts)?;

                        let mut lower_ssts = Vec::with_capacity(lower_sst_ids.len());
                        for sst_idx in lower_sst_ids.iter() {
                            lower_ssts.push(snapshot.sstables[sst_idx].clone());
                        }
                        let lower_concat_iter =
                            SstConcatIterator::create_and_seek_to_first(lower_ssts)?;

                        let iter = TwoMergeIterator::create(upper_concat_iter, lower_concat_iter)?;
                        self.compact_generate_ssts_from_iter(iter)
                    }
                }
            }
            CompactionTask::Leveled(leveled_task) => {
                println!("\n===== Compaction task =====");
                println!(
                    "L{:?} : {:?}",
                    leveled_task.upper_level, leveled_task.upper_level_sst_ids
                );
                println!(
                    "L{:?} : {:?}",
                    leveled_task.lower_level, leveled_task.lower_level_sst_ids
                );
                println!("===========================\n");

                let mut lower_ssts = Vec::with_capacity(leveled_task.lower_level_sst_ids.len());
                for sst_id in leveled_task.lower_level_sst_ids.iter() {
                    lower_ssts.push(snapshot.sstables[sst_id].clone());
                }
                let lower_iter = SstConcatIterator::create_and_seek_to_first(lower_ssts)?;

                match leveled_task.upper_level {
                    None => {
                        let mut upper_iters = Vec::new();
                        for sst_id in leveled_task.upper_level_sst_ids.iter() {
                            let iter = SsTableIterator::create_and_seek_to_first(
                                snapshot.sstables[sst_id].clone(),
                            )?;
                            upper_iters.push(Box::new(iter));
                        }
                        let iter = TwoMergeIterator::create(
                            MergeIterator::create(upper_iters),
                            lower_iter,
                        )?;

                        self.compact_generate_ssts_from_iter(iter)
                    }
                    Some(_) => {
                        let mut upper_ssts =
                            Vec::with_capacity(leveled_task.upper_level_sst_ids.len());
                        for sst_id in leveled_task.upper_level_sst_ids.iter() {
                            upper_ssts.push(snapshot.sstables[sst_id].clone());
                        }
                        let upper_iter = SstConcatIterator::create_and_seek_to_first(upper_ssts)?;
                        let iter = TwoMergeIterator::create(upper_iter, lower_iter)?;
                        self.compact_generate_ssts_from_iter(iter)
                    }
                }
            }
            CompactionTask::Tiered(tiered_task) => {
                let mut sst_iters = Vec::with_capacity(tiered_task.tiers.len());
                // for id in sst_ids.iter() {
                //     let table = snapshot.sstables[id].clone();
                //     let iter = SsTableIterator::create_and_seek_to_first(table)?;
                //     sst_iters.push(Box::new(iter));
                // }
                for (_, sst_ids) in tiered_task.tiers.iter() {
                    let sstables = sst_ids
                        .iter()
                        .map(|x| snapshot.sstables[x].clone())
                        .collect::<Vec<_>>();
                    let concat_iter = SstConcatIterator::create_and_seek_to_first(sstables)?;
                    sst_iters.push(Box::new(concat_iter));
                }
                let iter = MergeIterator::create(sst_iters);
                self.compact_generate_ssts_from_iter(iter)
            }
        }
    }

    pub fn force_full_compaction(&self) -> Result<()> {
        let mut snapshot;
        {
            let state_guard = self.state.write();
            snapshot = state_guard.as_ref().clone();
        }

        let l0_ssts = snapshot.l0_sstables.clone();
        let (_, l1_ssts) = snapshot.levels[0].clone();
        let task = CompactionTask::ForceFullCompaction {
            l0_sstables: l0_ssts,
            l1_sstables: l1_ssts,
        };

        // try to compact l0 and l1 ssts to l1 ssts
        let compacted_ssts = self.compact(&task)?;

        // update snapshot
        snapshot.l0_sstables.clear();
        let mut level_1: Vec<usize> = Vec::with_capacity(compacted_ssts.len());
        for compacted_sst in compacted_ssts {
            let sst_id = compacted_sst.sst_id();
            level_1.push(sst_id);
            snapshot.sstables.insert(sst_id, compacted_sst);
        }
        snapshot.levels[0] = (1, level_1);

        // lock and update LsmStorageState
        {
            let _state_lock = self.state_lock.lock();
            let mut state_guard = self.state.write();
            *state_guard = Arc::new(snapshot);
        }
        Ok(())
    }

    fn trigger_compaction(&self) -> Result<()> {
        // self.force_full_compaction()?;
        let snapshot;
        {
            let state_guard = self.state.read();
            snapshot = state_guard.clone();
        }

        let task = self
            .compaction_controller
            .generate_compaction_task(snapshot.as_ref());
        let Some(task) = task else {
            return Ok(());
        };

        // update snapshot
        let new_sstables = self.compact(&task)?;
        let output = new_sstables.iter().map(|x| x.sst_id()).collect::<Vec<_>>();

        {
            // hold state_lock to prevent flush
            let state_lock = self.state_lock.lock();
            println!("compaction will add new sstables: {:?}", output);

            let mut snapshot = self.state.read().as_ref().clone();
            // 1. update snapshot.sstables
            // 1.1 first add new files to snapshot
            for file in new_sstables.iter() {
                let result = snapshot.sstables.insert(file.sst_id(), file.clone());
                assert!(result.is_none());
            }
            let files_to_be_removed;
            // 1.2 update snapshot.l0_sstables and snapshot.levels
            (snapshot, files_to_be_removed) = self
                .compaction_controller
                .apply_compaction_result(&snapshot, &task, &output, false);

            // 1.3 remove old files
            for file in files_to_be_removed.iter() {
                let result = snapshot.sstables.remove(file);
                assert!(result.is_some());
            }
            self.sync_dir()?;

            let mut state_guard = self.state.write();

            println!("===== After compaction =====");
            println!("L0 : {:?}", snapshot.l0_sstables);
            for (tier, sstables) in snapshot.levels.iter() {
                println!("L{:?} : {:?}", tier, sstables);
            }
            println!();

            *state_guard = Arc::new(snapshot);
            drop(state_guard);
            self.sync_dir()?;
            self.manifest
                .as_ref()
                .unwrap()
                .add_record(&state_lock, ManifestRecord::Compaction(task, output))?;
        }

        Ok(())
    }

    pub(crate) fn spawn_compaction_thread(
        self: &Arc<Self>,
        rx: crossbeam_channel::Receiver<()>,
    ) -> Result<Option<std::thread::JoinHandle<()>>> {
        if let CompactionOptions::Leveled(_)
        | CompactionOptions::Simple(_)
        | CompactionOptions::Tiered(_) = self.options.compaction_options
        {
            let this = self.clone();
            let handle = std::thread::spawn(move || {
                let ticker = crossbeam_channel::tick(Duration::from_millis(50));
                loop {
                    crossbeam_channel::select! {
                        recv(ticker) -> _ => if let Err(e) = this.trigger_compaction() {
                            eprintln!("compaction failed: {}", e);
                        },
                        recv(rx) -> _ => return
                    }
                }
            });
            return Ok(Some(handle));
        }
        Ok(None)
    }

    fn trigger_flush(&self) -> Result<()> {
        let snapshot;
        {
            let state_guard = self.state.read();
            snapshot = Arc::clone(&state_guard);
        }
        //TODO: what if 2 threads call force_flush?
        if snapshot.imm_memtables.len() >= self.options.num_memtable_limit {
            self.force_flush_next_imm_memtable()?;
        }
        Ok(())
    }

    pub(crate) fn spawn_flush_thread(
        self: &Arc<Self>,
        rx: crossbeam_channel::Receiver<()>,
    ) -> Result<Option<std::thread::JoinHandle<()>>> {
        let this = self.clone();
        let handle = std::thread::spawn(move || {
            let ticker = crossbeam_channel::tick(Duration::from_millis(50));
            loop {
                crossbeam_channel::select! {
                    recv(ticker) -> _ => if let Err(e) = this.trigger_flush() {
                        eprintln!("flush failed: {}", e);
                    },
                    recv(rx) -> _ => return
                }
            }
        });
        Ok(Some(handle))
    }
}
