use std::collections::HashSet;

use serde::{Deserialize, Serialize};

use crate::lsm_storage::LsmStorageState;

#[derive(Debug, Serialize, Deserialize)]
pub struct TieredCompactionTask {
    pub tiers: Vec<(usize, Vec<usize>)>,
    pub bottom_tier_included: bool,
}

#[derive(Debug, Clone)]
pub struct TieredCompactionOptions {
    pub num_tiers: usize,
    pub max_size_amplification_percent: usize,
    pub size_ratio: usize,
    pub min_merge_width: usize,
    pub max_merge_width: Option<usize>,
}

pub struct TieredCompactionController {
    options: TieredCompactionOptions,
}

impl TieredCompactionController {
    pub fn new(options: TieredCompactionOptions) -> Self {
        Self { options }
    }

    pub fn generate_compaction_task(
        &self,
        snapshot: &LsmStorageState,
    ) -> Option<TieredCompactionTask> {
        assert!(
            snapshot.l0_sstables.is_empty(),
            "should not add l0 ssts in tiered compaction"
        );

        if snapshot.levels.len() < self.options.num_tiers {
            return None;
        }

        // trigger by space amplification ratio
        let mut all_size_except_last_level: usize = 0;
        let last_level_size: usize = snapshot.levels.last().unwrap().1.len();
        for idx in 0..snapshot.levels.len() - 1 {
            all_size_except_last_level += snapshot.levels[idx].1.len();
        }
        let space_amplification_ratio =
            (all_size_except_last_level as f64) / (last_level_size as f64) * 100.0;
        if space_amplification_ratio >= self.options.max_size_amplification_percent as f64 {
            let task = TieredCompactionTask {
                tiers: snapshot.levels.clone(),
                bottom_tier_included: true,
            };
            println!(
                "compaction triggered by space amplification ratio: {:?}",
                space_amplification_ratio
            );
            return Some(task);
        }

        //trigger by size ratio
        let size_ratio_trigger = (100.0 + self.options.size_ratio as f64) / 100.0;
        let mut size_previous_tiers: usize = snapshot.levels.first().unwrap().1.len();
        for idx in 1..snapshot.levels.len() {
            let size_current_iter = snapshot.levels[idx].1.len();
            let cur_size_ratio = size_current_iter as f64 / size_previous_tiers as f64;
            if cur_size_ratio >= size_ratio_trigger && idx + 1 >= self.options.min_merge_width {
                let task = TieredCompactionTask {
                    tiers: snapshot
                        .levels
                        .iter()
                        .take(idx)
                        .cloned()
                        .collect::<Vec<(usize, Vec<usize>)>>(),
                    bottom_tier_included: idx + 1 == snapshot.levels.len(), // TODO: idx+1 >= snapshot.levels.len()
                };
                println!(
                    "size_ratio_trigger: {:?}, compaction triggered by size ratio: {:?}",
                    size_ratio_trigger, cur_size_ratio
                );
                return Some(task);
            }
            size_previous_tiers += snapshot.levels[idx].1.len();
        }

        //none of triggers produce tasks, do a compaction to reduce the number of tiers, take all tiers into one
        let num_iters_to_take = snapshot
            .levels
            .len()
            .min(self.options.max_merge_width.unwrap_or(usize::MAX));
        Some(TieredCompactionTask {
            tiers: snapshot
                .levels
                .iter()
                .take(num_iters_to_take)
                .cloned()
                .collect::<Vec<_>>(),
            bottom_tier_included: true,
        })
    }

    pub fn apply_compaction_result(
        &self,
        snapshot: &LsmStorageState,
        task: &TieredCompactionTask,
        output: &[usize],
    ) -> (LsmStorageState, Vec<usize>) {
        let mut new_level = Vec::new();
        let del_sst_ids = task
            .tiers
            .iter()
            .flat_map(|x| x.1.clone())
            .collect::<Vec<_>>();
        let mut del_tier_ids = task.tiers.iter().map(|x| x.0).collect::<HashSet<_>>();
        let mut compacted_tier_added = false;
        for (tier_id, sstables) in snapshot.levels.iter() {
            // if the tier doesn't belong to the task, then retain
            if !del_tier_ids.remove(tier_id) {
                new_level.push((*tier_id, sstables.clone()));
            }

            // if all tiers in the task are deleted, then add the new tier to new_level
            if del_tier_ids.is_empty() && !compacted_tier_added {
                compacted_tier_added = true;
                new_level.push((output[0], output.to_vec()));
            }
        }
        let mut new_state = snapshot.clone();
        new_state.levels = new_level;
        (new_state, del_sst_ids)
    }
}
