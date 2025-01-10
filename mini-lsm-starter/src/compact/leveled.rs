use std::collections::HashSet;

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
        snapshot: &LsmStorageState,
        sst_ids: &[usize],
        in_level: usize,
    ) -> Vec<usize> {
        let key_from = sst_ids
            .iter()
            .map(|id| snapshot.sstables[id].first_key())
            .min()
            .unwrap();
        let key_to = sst_ids
            .iter()
            .map(|id| snapshot.sstables[id].last_key())
            .max()
            .unwrap();
        let mut lower_sst_ids = Vec::with_capacity(snapshot.levels[in_level - 1].1.len());
        for sst_id in snapshot.levels[in_level - 1].1.iter() {
            let sst = snapshot.sstables.get(sst_id).unwrap();
            let first_key = sst.first_key();
            let last_key = sst.last_key();
            if !(first_key > key_to || last_key < key_from) {
                lower_sst_ids.push(*sst_id);
            }
        }
        lower_sst_ids
    }

    pub fn generate_compaction_task(
        &self,
        snapshot: &LsmStorageState,
    ) -> Option<LeveledCompactionTask> {
        //Task 1.1: Compute Target Sizes
        let max_level = self.options.max_levels;
        let mut target_sizes = vec![0; max_level];
        // let bottom_size = snapshot
        //     .levels
        //     .last()
        //     .unwrap()
        //     .1
        //     .iter()
        //     .map(|sst_id| snapshot.sstables.get(sst_id).unwrap().table_size())
        //     .sum::<u64>();

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
            // level iterate from max_level-2 to 0
            let next_level_size = target_sizes[level + 1];
            let this_level_size = next_level_size / self.options.level_size_multiplier;
            if next_level_size > base_level_size_bytes {
                target_sizes[level] = this_level_size;
            }
            if target_sizes[level] > 0 {
                base_level = level + 1;
            }
        }
        println!(
            "real sizes: {:?}, target sizes: {:?}, base_level: {:?}",
            real_sizes
                .iter()
                .map(|x| format!("{:.3}MB", *x as f64 / 1024.0 / 1024.0))
                .collect::<Vec<_>>(),
            target_sizes
                .iter()
                .map(|x| format!("{:.3}MB", *x as f64 / 1024.0 / 1024.0))
                .collect::<Vec<_>>(),
            base_level
        );

        //Task 1.2: Decide base level, compact l0_sstables to the first level whose target_size > 0 from top down
        //generate l0 compaction task
        if snapshot.l0_sstables.len() >= self.options.level0_file_num_compaction_trigger {
            println!(
                "compaction trigger by l0 num {:?} >= {:?}, base level: {:?}",
                snapshot.l0_sstables.len(),
                self.options.level0_file_num_compaction_trigger,
                base_level
            );
            let lower_level_sst_ids =
                self.find_overlapping_ssts(snapshot, &snapshot.l0_sstables, base_level);
            return Some(LeveledCompactionTask {
                upper_level: None,
                upper_level_sst_ids: snapshot.l0_sstables.clone(),
                lower_level: base_level,
                lower_level_sst_ids,
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
            let lower_level_sst_ids = self.find_overlapping_ssts(
                snapshot,
                snapshot.levels[level - 1].1.as_slice(),
                level + 1,
            );
            return Some(LeveledCompactionTask {
                upper_level: Some(level),
                upper_level_sst_ids: snapshot.levels[level - 1].1.clone(),
                lower_level: level + 1,
                lower_level_sst_ids,
                is_lower_level_bottom_level: level + 1 == max_level,
            });
        }
        None
    }

    pub fn apply_compaction_result(
        &self,
        snapshot: &LsmStorageState,
        task: &LeveledCompactionTask,
        output: &[usize],
        in_recovery: bool,
    ) -> (LsmStorageState, Vec<usize>) {
        let mut new_state = snapshot.clone();
        let output = output.to_vec();
        println!("output = {:?}", output);

        let mut upper_level_sst_ids_set = task
            .upper_level_sst_ids
            .iter()
            .copied()
            .collect::<HashSet<_>>();
        let mut lower_level_sst_ids_set = task
            .lower_level_sst_ids
            .iter()
            .copied()
            .collect::<HashSet<_>>();

        // 1. dealing with upper_level_sst_ids
        match task.upper_level {
            None => {
                // == Attention! ==
                // remember that flush and compact are two unique threads
                // when compact, there may be new sstables flush to l0
                // so we need to reserve new l0_sstables and remove old l0_sstables

                let new_l0_sstables = new_state
                    .l0_sstables
                    .iter()
                    .copied()
                    .filter(|x| !upper_level_sst_ids_set.remove(x))
                    .collect::<Vec<_>>();
                assert!(upper_level_sst_ids_set.is_empty());
                new_state.l0_sstables = new_l0_sstables;
            }
            Some(upper_l) => {
                new_state.levels[upper_l - 1] = (upper_l, Vec::new());
            }
        }
        let mut del_sst_ids = task.upper_level_sst_ids.clone();
        del_sst_ids.extend(task.lower_level_sst_ids.iter());

        println!(
            "del_l{:?}_sst_ids = {:?}",
            task.upper_level, task.upper_level_sst_ids
        );
        println!(
            "del_l{:?}_sst_ids = {:?}",
            task.lower_level, task.lower_level_sst_ids
        );

        // 2.dealing with lower_level_sst_ids
        // 2.1 insert output to level 1 / lower_level
        let lower_l = task.lower_level;
        let mut new_lower_sstables = Vec::new();
        if !in_recovery {
            let lower_len = snapshot.levels[lower_l - 1].1.len();
            let mut concat_idx = lower_len;
            for idx in 0..lower_len {
                let id = snapshot.levels[lower_l - 1].1[idx];
                if lower_level_sst_ids_set.is_empty()
                    && snapshot.sstables[output.first().unwrap()].first_key()
                        <= snapshot.sstables[&id].first_key()
                {
                    concat_idx = idx;
                    break;
                }
                if !lower_level_sst_ids_set.remove(&id) {
                    new_lower_sstables.push(id);
                }
            }
            new_lower_sstables.extend(output.iter());
            // push remaining id to new_lower_sstables
            new_lower_sstables.extend_from_slice(&snapshot.levels[lower_l - 1].1[concat_idx..]);
            println!(
                "old l{:?}_sst_ids = {:?}",
                task.lower_level,
                snapshot.levels[lower_l - 1].1
            );
            println!(
                "new l{:?}_sst_ids = {:?}",
                task.lower_level, new_lower_sstables
            );
        } else {
            new_lower_sstables = new_state.levels[lower_l - 1]
                .1
                .iter()
                .copied()
                .filter(|x| !upper_level_sst_ids_set.remove(x))
                .collect::<Vec<_>>();
            new_lower_sstables.extend(output.iter());
        }
        new_state.levels[lower_l - 1] = (task.lower_level, new_lower_sstables);
        (new_state, del_sst_ids)
    }
}
