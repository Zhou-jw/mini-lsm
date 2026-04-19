#[cfg(test)]
mod wal_async_tests {
    use tempfile::tempdir;
    
    #[test]
    fn test_async_wal_basic() {
        tokio_uring::start(async {
            let dir = tempdir().unwrap();
            let wal_path = dir.path().join("test.wal");
            
            let mut wal = crate::wal_async::AsyncWal::create(&wal_path).await.unwrap();
            
            let key = crate::key::KeySlice::from_slice_with_ts(b"key1", 1);
            wal.put(key, b"value1").await.unwrap();
            wal.flush().await.unwrap();
            wal.sync().await.unwrap();
            
            let metadata = std::fs::metadata(&wal_path).unwrap();
            assert!(metadata.len() > 0);
        })
    }
    
    #[test]
    fn test_sync_wal_basic() {
        let dir = tempdir().unwrap();
        let wal_path = dir.path().join("test.wal");
        
        let wal = crate::wal::Wal::create(&wal_path).unwrap();
        
        let key = crate::key::KeySlice::from_slice_with_ts(b"key1", 1);
        wal.put(key, b"value1").unwrap();
        
        wal.sync().unwrap();
        
        let metadata = std::fs::metadata(&wal_path).unwrap();
        assert!(metadata.len() > 0);
    }
    
    #[test]
    fn test_wal_throughput_comparison() {
        const NUM_OPS: usize = 50000;
        
        println!("\n=== WAL Throughput Comparison ===\n");
        
        let dir = tempdir().unwrap();
        let sync_wal_path = dir.path().join("sync.wal");
        let async_wal_path = dir.path().join("async.wal");
        
        // Sync WAL benchmark - no fsync per write
        let sync_wal = crate::wal::Wal::create(&sync_wal_path).unwrap();
        let sync_start = std::time::Instant::now();
        
        for i in 0..NUM_OPS {
            let key = crate::key::KeySlice::from_slice_with_ts(b"key", i as u64);
            sync_wal.put(key, b"value").unwrap();
        }
        sync_wal.sync().unwrap();
        let sync_elapsed = sync_start.elapsed();
        let sync_ops_per_sec = NUM_OPS as f64 / sync_elapsed.as_secs_f64();
        
        println!("Sync WAL (no fsync): {} ops in {:?} = {:.2} ops/sec", 
            NUM_OPS, sync_elapsed, sync_ops_per_sec);
        
        // Async WAL benchmark - no fsync per write
        let async_start = std::time::Instant::now();
        
        tokio_uring::start(async {
            let mut wal = crate::wal_async::AsyncWal::create(&async_wal_path).await.unwrap();
            
            for i in 0..NUM_OPS {
                let key = crate::key::KeySlice::from_slice_with_ts(b"key", i as u64);
                wal.put(key, b"value").await.unwrap();
            }
            
            wal.flush().await.unwrap();
            wal.sync().await.unwrap();
        });
        
        let async_elapsed = async_start.elapsed();
        let async_ops_per_sec = NUM_OPS as f64 / async_elapsed.as_secs_f64();
        
        println!("Async WAL (no fsync): {} ops in {:?} = {:.2} ops/sec", 
            NUM_OPS, async_elapsed, async_ops_per_sec);
        
        let ratio = async_ops_per_sec / sync_ops_per_sec;
        println!("\nAsync/Sync ratio: {:.2}x", ratio);
    }
}
