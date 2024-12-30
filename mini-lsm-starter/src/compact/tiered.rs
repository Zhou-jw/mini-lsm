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
        assert!(snapshot.l0_sstables.is_empty(), "should not add l0 ssts in tiered compaction");

        if snapshot.levels.len() <= self.options.num_tiers {
            return None;
        }

        // trigger by space amplification ratio
        let mut all_size_except_last_level:usize = 0;
        let last_level_size:usize = snapshot.levels.last().unwrap().1.len();
        for idx in 0..snapshot.levels.len()-1 {
            all_size_except_last_level = snapshot.levels[idx].1.len();
        }
        let space_amplification_ratio = (all_size_except_last_level as f64) / (last_level_size as f64) * 100.0 ;
        if space_amplification_ratio >= self.options.max_size_amplification_percent as f64{
            let task = TieredCompactionTask{
                tiers: snapshot.levels.clone(),
                bottom_tier_included: true,
            };
            println!("compaction triggered by space amplification ratio: {:?}", space_amplification_ratio);
            return Some(task);
        }

        //trigger by size ratio
        
        unimplemented!()
    }

    pub fn apply_compaction_result(
        &self,
        _snapshot: &LsmStorageState,
        _task: &TieredCompactionTask,
        _output: &[usize],
    ) -> (LsmStorageState, Vec<usize>) {
        unimplemented!()
    }
}
