use clap::Parser;
use mini_lsm_starter::compact::{CompactionOptions, SimpleLeveledCompactionOptions};
use mini_lsm_starter::iterators::StorageIterator;
use mini_lsm_starter::lsm_storage::{LsmStorageOptions, MiniLsm};
use std::path::PathBuf;

#[derive(Parser, Debug)]
#[command(about = "Benchmark for mini-lsm")]
struct Args {
    #[arg(long, default_value = "bench.db")]
    path: PathBuf,

    #[arg(long, default_value_t = 100000)]
    num_ops: usize,

    #[arg(long)]
    enable_wal: bool,
}

fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let num_ops = args.num_ops;

    println!("=== mini-lsm Benchmark ===");
    println!("Ops: {}", num_ops);
    println!("WAL: {}", args.enable_wal);
    println!();

    let lsm = MiniLsm::open(
        args.path,
        LsmStorageOptions {
            block_size: 4096,
            target_sst_size: 2 << 20,
            num_memtable_limit: 3,
            compaction_options: CompactionOptions::Simple(SimpleLeveledCompactionOptions {
                size_ratio_percent: 200,
                level0_file_num_compaction_trigger: 2,
                max_levels: 4,
            }),
            enable_wal: args.enable_wal,
            serializable: false,
        },
    )?;

    // Write benchmark
    println!("Running {} write...", num_ops);
    let start = std::time::Instant::now();

    for i in 0..num_ops {
        let key = format!("k{}", i);
        let value = format!("v{}", i);
        lsm.put(key.as_bytes(), value.as_bytes())?;
    }

    let elapsed = start.elapsed();
    let ops_per_sec = num_ops as f64 / elapsed.as_secs_f64();

    println!("Time:     {:.2?}", elapsed);
    println!("Ops/sec:  {:.0}", ops_per_sec);
    println!();

    // Read benchmark (from disk)
    println!("Filling and flushing...");
    lsm.force_flush()?;
    lsm.force_full_compaction()?;

    println!("Running {} read...", num_ops);
    let start = std::time::Instant::now();

    for i in 0..num_ops {
        let key = format!("k{}", i);
        let _ = lsm.get(key.as_bytes())?;
    }

    let elapsed = start.elapsed();
    let ops_per_sec = num_ops as f64 / elapsed.as_secs_f64();

    println!("Time:     {:.2?}", elapsed);
    println!("Ops/sec:  {:.0}", ops_per_sec);
    println!();

    // Scan benchmark
    println!("Running scan...");
    let start = std::time::Instant::now();

    for _ in 0..5 {
        let mut iter = lsm.scan(std::ops::Bound::Unbounded, std::ops::Bound::Unbounded)?;
        let mut cnt = 0;
        while iter.is_valid() {
            iter.next()?;
            cnt += 1;
        }
    }

    let elapsed = start.elapsed();
    let keys_per_sec = (num_ops * 5) as f64 / elapsed.as_secs_f64();

    println!("Time:     {:.2?}", elapsed);
    println!("Keys/s:   {:.0}", keys_per_sec);

    lsm.close()?;
    Ok(())
}
