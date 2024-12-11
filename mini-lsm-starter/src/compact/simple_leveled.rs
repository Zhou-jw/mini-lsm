use serde::{Deserialize, Serialize};

use crate::{iterators::{concat_iterator::SstConcatIterator, merge_iterator::MergeIterator, two_merge_iterator::TwoMergeIterator}, lsm_storage::LsmStorageState, table::SsTableIterator};

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
        if l0 >= self.options.level0_file_num_compaction_trigger && l0 !=0 && (l1 as f64 / l0 as f64 *100.0) < self.options.size_ratio_percent as f64 {
            return Some(SimpleLeveledCompactionTask {
                upper_level: None,
                upper_level_sst_ids: _snapshot.l0_sstables.clone(),
                lower_level: 1,
                lower_level_sst_ids: _snapshot.levels[0].1.clone(),
                is_lower_level_bottom_level: true,
            });
        }

        // size ratio trigger
        if _snapshot.levels.len() <= 1 {
            return None;
        }
        for l_cur in 0..self.options.max_levels-1 {
            let lower_level = l_cur+2;
            let upper_level = l_cur+1;
            if _snapshot.levels[l_cur].1.len() == 0 {
                continue;
            }
            let size_ratio_percent =
                _snapshot.levels[l_cur + 1].1.len() as f64 / _snapshot.levels[l_cur].1.len() as f64 * 100.0;
            if size_ratio_percent < self.options.size_ratio_percent as f64 {
                return Some(SimpleLeveledCompactionTask {
                    lower_level,
                    lower_level_sst_ids: _snapshot.levels[l_cur+1].1.clone(),
                    upper_level: Some(upper_level),
                    upper_level_sst_ids: _snapshot.levels[l_cur].1.clone(),
                    is_lower_level_bottom_level: false,
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
        
        match _task.upper_level {
            None => {
                new_state.l0_sstables.clear();
                let mut del_sst_ids = _snapshot.l0_sstables.clone();
                del_sst_ids.extend(_snapshot.levels[0].1.iter());
                println!("del_l0_sst_ids = {:?}", del_sst_ids);
                new_state.levels[0] = (1, sst_ids.clone());
                (new_state, del_sst_ids)
            }
            Some(mut upper_l) => {
                upper_l = upper_l-1;
                // println!("_output = {:?}", _output);
                let lower_l = _task.lower_level-1;
                let mut del_sst_ids = new_state.levels[upper_l].1.clone();
                del_sst_ids.extend(new_state.levels[lower_l].1.iter());
                println!("del_upper_sst_ids = {:?}", del_sst_ids);
                new_state.levels[upper_l] = (new_state.levels[upper_l].0, Vec::new());
                new_state.levels[lower_l] = (new_state.levels[lower_l].0, sst_ids.clone());
                (new_state, del_sst_ids)
            },
        }
        
        // unimplemented!()
    }

    fn trigger_l0_compaction(
        &self,
        _snapshot: & mut LsmStorageState,
        _task: &SimpleLeveledCompactionTask,
    ) -> TwoMergeIterator<MergeIterator<SsTableIterator>, SstConcatIterator> {
        let mut new_state = _snapshot.clone(); 
        let l0_sstables = &new_state.l0_sstables;
        let mut sst_iters = Vec::with_capacity(l0_sstables.len());
        for sst_idx in l0_sstables.iter() {
            let table = new_state.sstables[sst_idx].clone();
            let iter = SsTableIterator::create_and_seek_to_first(table).unwrap();
            sst_iters.push(Box::new(iter));
        }

        new_state.l0_sstables.clear();
        
        //create sst_concate_iter
        let l1_sstables = &new_state.levels[0].1;
        let mut l1_ssts = Vec::with_capacity(l1_sstables.len());
        for sst_idx in l1_sstables.iter() {
            l1_ssts.push(new_state.sstables[sst_idx].clone());
        }
        let sst_concate_iter = SstConcatIterator::create_and_seek_to_first(l1_ssts).unwrap();
        TwoMergeIterator::create(MergeIterator::create(sst_iters), sst_concate_iter).unwrap()
    }
}
