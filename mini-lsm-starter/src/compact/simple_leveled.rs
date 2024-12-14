use std::collections::HashSet;

use serde::{Deserialize, Serialize};

use crate::lsm_storage::LsmStorageState;

#[derive(Debug, Clone)]
pub struct SimpleLeveledCompactionOptions {
    pub size_ratio_percent: usize,
    pub level0_file_num_compaction_trigger: usize,
    pub max_levels: usize,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SimpleLeveledCompactionTask {
    // if upper_level is `None`, then it is L0 compaction
    pub upper_level: Option<usize>,
    pub upper_level_sst_ids: Vec<usize>,
    pub lower_level: usize,
    pub lower_level_sst_ids: Vec<usize>,
    pub is_lower_level_bottom_level: bool,
}

pub struct SimpleLeveledCompactionController {
    options: SimpleLeveledCompactionOptions,
}

impl SimpleLeveledCompactionController {
    pub fn new(options: SimpleLeveledCompactionOptions) -> Self {
        Self { options }
    }

    /// Generates a compaction task.
    ///
    /// Returns `None` if no compaction needs to be scheduled. The order of SSTs in the compaction task id vector matters.
    pub fn generate_compaction_task(
        &self,
        _snapshot: &LsmStorageState,
    ) -> Option<SimpleLeveledCompactionTask> {
        // l0 trigger
        let l0 = _snapshot.l0_sstables.len();
        let l1 = _snapshot.levels[0].1.len();
        if l0 >= self.options.level0_file_num_compaction_trigger
            && l0 != 0
            && (l1 as f64 / l0 as f64 * 100.0) < self.options.size_ratio_percent as f64
        {
            return Some(SimpleLeveledCompactionTask {
                upper_level: None,
                upper_level_sst_ids: _snapshot.l0_sstables.clone(),
                lower_level: 1,
                lower_level_sst_ids: _snapshot.levels[0].1.clone(),
                is_lower_level_bottom_level: false,
            });
        }

        // size ratio trigger
        if _snapshot.levels.len() <= 1 {
            return None;
        }
        for l_cur in 0..self.options.max_levels - 1 {
            let lower_level = l_cur + 2;
            let upper_level = l_cur + 1;
            if _snapshot.levels[l_cur].1.is_empty() {
                continue;
            }
            let size_ratio_percent = _snapshot.levels[l_cur + 1].1.len() as f64
                / _snapshot.levels[l_cur].1.len() as f64
                * 100.0;
            if size_ratio_percent < self.options.size_ratio_percent as f64 {
                return Some(SimpleLeveledCompactionTask {
                    lower_level,
                    lower_level_sst_ids: _snapshot.levels[l_cur + 1].1.clone(),
                    upper_level: Some(upper_level),
                    upper_level_sst_ids: _snapshot.levels[l_cur].1.clone(),
                    is_lower_level_bottom_level: lower_level == self.options.max_levels,
                });
            }
        }
        None
    }

    /// Apply the compaction result.
    ///
    /// The compactor will call this function with the compaction task and the list of SST ids generated. This function applies the
    /// result and generates a new LSM state. The functions should only change `l0_sstables` and `levels` without changing memtables
    /// and `sstables` hash map. Though there should only be one thread running compaction jobs, you should think about the case
    /// where an L0 SST gets flushed while the compactor generates new SSTs, and with that in mind, you should do some sanity checks
    /// in your implementation.
    pub fn apply_compaction_result(
        &self,
        _snapshot: &LsmStorageState,
        _task: &SimpleLeveledCompactionTask,
        _output: &[usize],
    ) -> (LsmStorageState, Vec<usize>) {
        let mut new_state = _snapshot.clone();
        let sst_ids = _output.to_vec();
        println!("output = {:?}", sst_ids);

        match _task.upper_level {
            None => {
                // == Attention! ==
                // remember that flush and compact are two unique threads
                // when compact, there may be new sstables flush to l0
                // so we need to reserve new l0_sstables and remove old l0_sstables
                let mut del_sst_ids = _task.upper_level_sst_ids.clone();
                println!("del_l0_sst_ids = {:?}", del_sst_ids);
                del_sst_ids.extend(_task.lower_level_sst_ids.iter());
                println!("del_l1_sst_ids = {:?}", _task.lower_level_sst_ids.iter());
                let mut l0_sstables_compacted = _task
                    .upper_level_sst_ids
                    .iter()
                    .copied()
                    .collect::<HashSet<_>>();
                let new_l0_sstables = new_state
                    .l0_sstables
                    .iter()
                    .copied()
                    .filter(|x| !l0_sstables_compacted.remove(x))
                    .collect::<Vec<_>>();
                assert!(l0_sstables_compacted.is_empty());
                new_state.l0_sstables = new_l0_sstables;
                new_state.levels[0] = (1, sst_ids.clone());
                (new_state, del_sst_ids)
            }
            Some(mut upper_l) => {
                upper_l -= 1;
                // println!("_output = {:?}", _output);
                let lower_l = _task.lower_level - 1;
                let mut del_sst_ids = _task.upper_level_sst_ids.clone();
                println!("del_l{:?}_sst_ids = {:?}", upper_l + 1, del_sst_ids);
                del_sst_ids.extend(_task.lower_level_sst_ids.iter());
                println!(
                    "del_l{:?}_sst_ids = {:?}",
                    upper_l + 2,
                    new_state.levels[lower_l].1
                );
                new_state.levels[upper_l] = (new_state.levels[upper_l].0, Vec::new());
                new_state.levels[lower_l] = (new_state.levels[lower_l].0, sst_ids.clone());
                (new_state, del_sst_ids)
            }
        }

        // unimplemented!()
    }
}
