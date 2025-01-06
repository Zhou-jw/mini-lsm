use crossbeam_skiplist::base;
use serde::{Deserialize, Serialize};

use crate::lsm_storage::LsmStorageState;

#[derive(Debug, Serialize, Deserialize)]
pub struct LeveledCompactionTask {
    // if upper_level is `None`, then it is L0 compaction
    pub upper_level: Option<usize>,
    pub upper_level_sst_ids: Vec<usize>,
    pub lower_level: usize,
    pub lower_level_sst_ids: Vec<usize>,
    pub is_lower_level_bottom_level: bool,
}

#[derive(Debug, Clone)]
pub struct LeveledCompactionOptions {
    pub level_size_multiplier: usize,
    pub level0_file_num_compaction_trigger: usize,
    pub max_levels: usize,
    pub base_level_size_mb: usize,
}

pub struct LeveledCompactionController {
    options: LeveledCompactionOptions,
}

impl LeveledCompactionController {
    pub fn new(options: LeveledCompactionOptions) -> Self {
        Self { options }
    }

    fn find_overlapping_ssts(
        &self,
        _snapshot: &LsmStorageState,
        _sst_ids: &[usize],
        _in_level: usize,
    ) -> Vec<usize> {
        unimplemented!()
    }

    pub fn generate_compaction_task(
        &self,
        snapshot: &LsmStorageState,
    ) -> Option<LeveledCompactionTask> {
        //Task 1.1: Compute Target Sizes
        let max_level = self.options.max_levels;
        let mut target_sizes = vec![0, max_level];
        let bottom_size = snapshot
            .levels
            .last()
            .unwrap()
            .1
            .iter()
            .map(|sst_id| snapshot.sstables.get(sst_id).unwrap().table_size())
            .sum::<u64>();

        //
        let mut real_sizes = Vec::with_capacity(max_level);
        for level in 0..max_level {
            real_sizes.push(
                snapshot.levels[level]
                    .1
                    .iter()
                    .map(|sst_id| snapshot.sstables[sst_id].table_size())
                    .sum::<u64>() as usize,
            );
        }
        let base_level_size_bytes = self.options.base_level_size_mb * 1024 * 1024;
        target_sizes[max_level - 1] = real_sizes[max_level - 1].max(base_level_size_bytes);

        let mut base_level = max_level;
        for level in (0..max_level - 1).rev() {
            let next_level_size = target_sizes[level + 1];
            let this_level_size = next_level_size / self.options.level_size_multiplier;
            if this_level_size > base_level_size_bytes {
                target_sizes[level] = this_level_size;
            }
            if target_sizes[level] > 0 {
                base_level = level + 1;
            }
        }
        println!("current sizes: {:?}", real_sizes);
        println!("target sizes: {:?}", target_sizes);

        //Task 1.2: Decide base level, compact l0_sstables to the first level whose target_size > 0 from top down
        //generate l0 compaction task
        if snapshot.l0_sstables.len() >= self.options.level0_file_num_compaction_trigger {
            return Some(LeveledCompactionTask {
                upper_level: None,
                upper_level_sst_ids: snapshot.l0_sstables.clone(),
                lower_level: base_level,
                lower_level_sst_ids: snapshot.levels[base_level - 1].1.clone(),
                is_lower_level_bottom_level: base_level == max_level,
            });
        }

        // Task 1.3: Decide Level Priorities
        let mut priorities = Vec::with_capacity(max_level);
        for lev in 0..max_level {
            // if target_sizes[lev] == 0 {
            //     continue;
            // }
            // TODO what if target_sizes[lev] is 0?
            let prio = real_sizes[lev] as f64 / target_sizes[lev] as f64;
            if prio > 1.0 {
                priorities.push((prio, lev + 1));
            }
        }

        // partial_cmp will compare the first element in the tuple then the second
        // we want to sort priorities from greater to less
        priorities.sort_by(|a, b| a.partial_cmp(b).unwrap().reverse());
        if let Some((_, level)) = priorities.first() {
            let level = *level;
            return Some(LeveledCompactionTask {
                upper_level: Some(level),
                upper_level_sst_ids: snapshot.levels[level - 1].1.clone(),
                lower_level: level + 1,
                lower_level_sst_ids: snapshot.levels[level].1.clone(),
                is_lower_level_bottom_level: level + 1 == max_level,
            });
        }
        None
    }

    pub fn apply_compaction_result(
        &self,
        _snapshot: &LsmStorageState,
        _task: &LeveledCompactionTask,
        _output: &[usize],
        _in_recovery: bool,
    ) -> (LsmStorageState, Vec<usize>) {
        unimplemented!()
    }
}
