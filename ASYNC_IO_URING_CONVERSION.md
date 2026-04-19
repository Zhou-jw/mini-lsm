# Mini-LSM 异步存储引擎改造方案 (io_uring)

## 一、项目现状分析

当前 `mini-lsm-starter` 是一个**同步阻塞**的 LSM Tree 存储引擎：

### 1.1 当前 I/O 模式

| 组件 | 当前实现方式 | 问题 |
|------|-------------|------|
| **WAL (预写日志)** | `BufWriter<File>` 同步写入 `wal.rs:16-17` | 阻塞线程，批量写入效率低 |
| **SSTable 读取** | `File::read_exact_at()` 同步读取 `table.rs:115` | 阻塞线程，无法并发 |
| **SSTable 写入** | `std::fs::write()` 同步写入 `table.rs:125` | 阻塞线程，大文件写入慢 |
| **Block 读取** | 同步 `FileObject::read()` `table.rs:109-116` | 每次读取都阻塞 |
| **后台 Flush** | `crossbeam-channel` 通知 + 同步 I/O `lsm_storage.rs:215-218` | 非真正的异步 |
| **后台 Compaction** | 独立线程但同步 I/O `lsm_storage.rs:216` | CPU 等待 I/O，资源浪费 |

### 1.2 当前关键代码路径

- **WAL 写入**: `wal.rs:69-96` - `Wal::put()` 使用同步 `write_all()`
- **SSTable 创建**: `table.rs:124-130` - `FileObject::create()` 使用 `std::fs::write()`
- **SSTable 读取**: `table.rs:109-116` - `FileObject::read()` 使用 `read_exact_at()`
- **Block 缓存读取**: `table.rs:246-253` - 同步读取 + moka 缓存

---

## 二、改造目标

### 2.1 预期收益

1. **I/O 并发** - 同一时刻发起多个 I/O 请求，充分利用内核 I/O 队列
2. **降低延迟** - 异步提交无需等待完成即可返回，批量提交减少系统调用
3. **资源利用率** - CPU 无需阻塞等待 I/O，可以处理其他任务
4. **吞吐量** - 特别是大量小 I/O 场景（如 WAL 批量写入、SSTable 读取）

### 2.2 改造约束

- 保持 API 兼容或提供异步版本的 API
- 支持 Linux 5.1+ (io_uring 基础支持)
- 尽量不破坏现有的测试用例

---

## 三、改造方案详细设计

### 3.1 技术选型

推荐使用 **tokio-uring** (https://github.com/tokio-uring/tokio-uring)：

```toml
# Cargo.toml 新增依赖
[dependencies]
tokio = { version = "1", features = ["full"] }
tokio-uring = "0.6"
```

**理由**：
- 提供与 `tokio` 完全兼容的 API
- 底层使用 io_uring，支持异步文件 I/O
- 有成熟的生态和社区支持
- 可以渐进式改造

### 3.2 架构改造总览

```
┌─────────────────────────────────────────────────────────────────┐
│                         Application                              │
└─────────────────────────────────────────────────────────────────┘
                                  │
                                  ▼
┌─────────────────────────────────────────────────────────────────┐
│              Async Storage API (tokio runtime)                  │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────────┐   │
│  │ async put  │  │ async get   │  │ async scan / write_batch │   │
│  └──────┬──────┘  └──────┬──────┘  └───────────┬─────────────┘   │
└─────────┼─────────────────┼─────���───────────┼────────────────┘
          │                 │                 │
          ▼                 ▼                 ▼
┌─────────────────────────────────────────────────────────────────┐
│                Async I/O Layer (io_uring)                    │
│  ┌─────────────────────────────────────────────────────────┐ │
│  │           AioFile (wrapper of tokio-uring::File)          │ │
│  │  - 异步 read/write                                       │ │
│  │  - I/O 提交队列                                          │ │
│  │  - 完成队列轮询                                          │ │
│  └─────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────┘
                                  │
                                  ▼
┌─────────────────────────────────────────────────────────────────┐
│                    Linux Kernel (io_uring)                     │
│           Submission Queue ← App                            │
│           Completion Queue  → App                           │
└─────────────────────────────────────────────────────────────────┘
```

### 3.3 核心组件改造

#### 3.3.1 文件抽象层 (`src/storage/async_file.rs`)

新增异步文件封装：

```rust
// src/storage/async_file.rs
use tokio_uring::fs::File;
use std::path::Path;

pub struct AioFile {
    inner: File,
}

impl AioFile {
    /// 异步打开文件
    pub async fn open(path: &Path, read: bool, write: bool) -> std::io::Result<Self> {
        let file = tokio_uring::fs::OpenOptions::new()
            .read(read)
            .write(write)
            .create(create)
            .open(path)
            .await?;
        Ok(Self { inner: file })
    }

    /// 异步批量读取 (支持 gather I/O)
    pub async fn read_at(&self, offset: u64, buf: &mut [u8]) -> std::io::Result<usize> {
        self.inner.read_at(offset, buf).await
    }

    /// 异步写入 (支持 scatter I/O)
    pub async fn write_at(&self, offset: u64, buf: &[u8]) -> std::io::Result<usize> {
        self.inner.write_at(offset, buf).await
    }

    /// 异步预分配文件空间
    pub async fn allocate(&self, size: u64) -> std::io::Result<()> {
        self.innerallocate(size).await
    }

    /// 异步 fsync
    pub async fn fsync(&self) -> std::io::Result<()> {
        self.inner.sync_all().await
    }
}
```

#### 3.3.2 WAL 改造 (`src/wal_async.rs`)

```rust
// src/wal_async.rs
use crate::storage::async_file::AioFile;
use bytes::Bytes;

/// 异步 WAL
pub struct AsyncWal {
    file: AioFile,
    write_buffer: Vec<u8>,
    offset: u64,
}

impl AsyncWal {
    pub async fn create(path: impl AsRef<Path>) -> std::io::Result<Self> {
        let file = AioFile::create(path, true, true).await?;
        Ok(Self {
            file,
            write_buffer: Vec::with_capacity(64 * 1024), // 64KB 缓冲
            offset: 0,
        })
    }

    /// 批量异步写入 (积累到缓冲区，后台异步刷盘)
    pub async fn put_batch(&mut self, entries: &[(KeySlice, &[u8])]) -> std::io::Result<()> {
        for (key, value) in entries {
            // 编码到缓冲区
            self.write_buffer.reserve(...);
            // 添加编码
        }
        
        // 异步刷盘 (使用 io_uring 的 buffered write)
        if self.write_buffer.len() >= 32 * 1024 {
            self.flush().await?;
        }
        Ok(())
    }

    /// 显式异步刷盘
    pub async fn flush(&mut self) -> std::io::Result<()> {
        if self.write_buffer.is_empty() {
            return Ok(());
        }
        
        // 使用 io_uring 异步写入，偏移量自动维护
        let written = self.file.write_at(self.offset, &self.write_buffer).await?;
        self.offset += written as u64;
        self.write_buffer.clear();
        
        // 定期 fsync
        if self.offset % (256 * 1024) < written {
            self.file.fsync().await?;
        }
        Ok(())
    }
}
```

#### 3.3.3 SSTable 读取改造 (`src/table/async_sstable.rs`)

```rust
// src/table/async_sstable.rs
use crate::storage::async_file::AioFile;
use crate::block::Block;

pub struct AsyncSsTable {
    file: AioFile,
    block_meta: Vec<BlockMeta>,
    block_cache: Option<Arc<BlockCache>>,
    // ... 其他字段
}

impl AsyncSsTable {
    /// 异步读取单个 block (io_uring 预取优化)
    pub async fn read_block(&self, block_idx: usize) -> std::io::Result<Arc<Block>> {
        // 先检查缓存
        if let Some(ref cache) = self.block_cache {
            if let Some(block) = cache.get(&(self.id, block_idx)) {
                return Ok(block);
            }
        }
        
        // 异步读取
        let offset = self.block_meta[block_idx].offset as u64;
        let len = self.calculate_block_size(block_idx);
        let mut buf = vec![0u8; len];
        
        self.file.read_at(offset, &mut buf).await?;
        
        let block = Block::decode(&buf);
        Ok(Arc::new(block))
    }

    /// 异步预读多个 block (利用 io_uring 的批量提交)
    pub async fn prefetch_blocks(&self, block_indices: &[usize]) -> std::io::Result<()> {
        // 构建多个 I/O 请求，一次提交
        for &idx in block_indices {
            let offset = self.block_meta[idx].offset as u64;
            let len = self.calculate_block_size(idx);
            // 这里使用 tokio-uring 的 link 机制实现预读
        }
        Ok(())
    }
}
```

#### 3.3.4 存储引擎主类改造 (`src/lsm_storage_async.rs`)

```rust
// src/lsm_storage_async.rs
use tokio::sync::RwLock;
use std::sync::Arc;

/// 异步存储引擎主类
pub struct AsyncMiniLsm {
    state: Arc<RwLock<LsmStorageState>>,
    // ... 其他字段
}

impl AsyncMiniLsm {
    /// 异步打开存储引擎
    pub async fn open(path: impl AsRef<Path>, options: LsmStorageOptions) -> std::io::Result<Arc<Self>> {
        // 初始化 tokio 运行时
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        
        // 在 tokio 运行时中执行初始化
        runtime.block_on(async {
            let inner = Arc::new(AsyncLsmStorageInner::open(path, options).await?);
            Ok(Arc::new(Self { inner, ... }))
        })
    }

    /// 异步写入
    pub async fn put(&self, key: &[u8], value: &[u8]) -> std::io::Result<()> {
        self.inner.write_batch(&[WriteBatchRecord::Put(key, value)]).await
    }

    /// 异步读取
    pub async fn get(&self, key: &[u8]) -> std::io::Result<Option<Bytes>> {
        self.inner.get(key).await
    }

    /// 异步扫描
    pub async fn scan(
        &self,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
    ) -> std::io::Result<AsyncTxnIterator> {
        self.inner.scan(lower, upper).await
    }

    /// 异步同步 (flush 到磁盘)
    pub async fn sync(&self) -> std::io::Result<()> {
        self.inner.sync().await
    }
}
```

---

## 四、渐进式改造路线图

### 阶段 1: 基础异步 I/O 层 (Week 1-2)

**目标**: 建立异步文件抽象，保持上层同步 API 兼容

| 任务 | 文件 | 改动 |
|------|------|------|
| 新增 async_file.rs | `src/storage/async_file.rs` | 实现 AioFile 封装 |
| 改造 WAL 写入 | `src/wal.rs` → `src/wal_async.rs` | 异步批量写入 |

```rust
// 阶段 1 完成后，可用以下方式操作：
let runtime = tokio::runtime::new()?;
runtime.block_on(async {
    let mut wal = AsyncWal::create("/path/to/wal").await?;
    wal.put_batch(&entries).await?;
    wal.sync().await?;
});
```

### 阶段 2: 异步 SSTable (Week 3)

**目标**: 将 SSTable 读取改造为异步

| 任务 | 文件 | 改动 |
|------|------|------|
| 改造 SsTable 读取 | `src/table.rs` | 添加 `AsyncSsTable` |
| 添加 Block 预读 | `src/table/async_sstable.rs` | 实现 prefetch |
| 改造 BlockCache | `src/lsm_storage.rs` | 异步缓存接口 |

### 阶段 3: 完整异步引擎 (Week 4)

**目标**: 完全异步化的存储引擎 API

| 任务 | 文件 | 改动 |
|------|------|------|
| 重构 LsmStorage | `src/lsm_storage_async.rs` | AsyncMiniLsm |
| 异步 MVCC | `src/mvcc_async.rs` | 事务异步化 |
| 异步 Compaction | `src/compact_async.rs` | 后台异步压缩 |
| 异步 Flush | - | 后台异步刷盘 |

---

## 五、关键代码示例

### 5.1 Cargo.toml 改动

```toml
[package]
name = "mini-lsm-starter"
version = "0.3.0"
edition = "2021"

[dependencies]
# 新增
tokio = { version = "1", features = ["full"] }
tokio-uring = "0.6"

# 保留原有
anyhow = "1"
bytes = "1"
crossbeam-skiplist = "0.1"
# ...
```

### 5.2 主入口示例 (提供双 API)

```rust
// src/lib.rs
pub mod lsm_storage;
pub mod lsm_storage_async;  // 新增

pub use lsm_storage::{LsmStorageOptions, MiniLsm};
pub use lsm_storage_async::{AsyncMiniLsm, AsyncLsmStorageOptions};

// 使用示例
#[cfg(test)]
mod tests {
    #[tokio::test]
    async fn test_async_put_get() {
        let db = AsyncMiniLsm::open("./test_data", Default::default()).await;
        
        db.put(b"key1", b"value1").await;
        let value = db.get(b"key1").await;
        
        assert_eq!(value, Some(Bytes::from("value1")));
    }
}
```

---

## 六、性能优化建议

### 6.1 I/O 调度优化

1. **批量提交**: 累积多个小 I/O 后一次提交，减少 syscalls
2. **预读策略**: 顺序扫描时自动预读下一个 block
3. **合并写入**: WAL 使用顺序追加，多路并发写入不同 SSTable

### 6.2 队列深度配置

```rust
// 配置 io_uring 队列深度 (tokio-uring 默认 128)
let ring = io_uring::IoUring::builder()
    .queue_depth(512)  // 增加队列深度提高并发
    .build()?;
```

### 6.3 Buffer Pool

```rust
// 使用 buffer pool 减少内存分配
use tokio_uring::buf::BufPool;

let pool = BufPool::new(256, 4096); // 256 buffers, 4KB each
let buf = pool.acquire(4096).await?;
```

---

## 七、注意事项

### 7.1 平台限制

- io_uring 需要 **Linux 5.1+**
- 部分特性需要 **Linux 5.13+** (如 fixed file descriptor)
- 暂不支持 Windows/macOS

### 7.2 兼容性策略

- 可以通过 feature flag 控制编译:
  ```toml
  [features]
  default = ["sync"]
  async-io = ["tokio-uring"]
  ```
- 提供两种构建产物给用户选择

### 7.3 测试策略

- 保留原有同步测试用例
- 新增异步测试用例 (使用 `#[tokio::test]`)

---

## 八、总结

本方案将 `mini-lsm-starter` 从同步存储引擎改造为基于 io_uring 的异步存储引擎，主要改动：

1. **��入 tokio-uring** 作为异步 I/O 底层
2. **新增异步文件层** (`AioFile`)
3. **改造 WAL** 为异步批量写入
4. **改造 SSTable** 为异步读取 + 预读
5. **提供异步 API** (`AsyncMiniLsm`)

改造是渐进式的，可以先改造 I/O 层，再逐步改造上层，最终提供完整的异步存储引擎。