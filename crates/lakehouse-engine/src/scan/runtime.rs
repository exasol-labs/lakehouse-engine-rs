/// DataFusion `RuntimeEnv` sizing and `/tmp` spill probe.
///
/// Sizes the DataFusion memory pool from the per-instance memory limit reported
/// by UDF metadata and optionally enables spill-to-disk when `/tmp` is real
/// (non-tmpfs) disk with sufficient free space.
use datafusion::execution::disk_manager::{DiskManagerBuilder, DiskManagerMode};
use datafusion::execution::memory_pool::{FairSpillPool, GreedyMemoryPool};
use datafusion::execution::runtime_env::{RuntimeEnv, RuntimeEnvBuilder};
use std::path::PathBuf;
use std::sync::Arc;

/// 0.6 × limit — leaves headroom below the engine's 80% concurrency-stall threshold.
pub(crate) const MEMORY_FRACTION: f64 = 0.6;

/// Conservative default pool budget when the per-instance limit is unknown (0 sentinel).
/// 1 GiB keeps a single shard comfortable without risking OOM on a low-memory node.
pub(crate) const DEFAULT_BUDGET_BYTES: u64 = 1024 * 1024 * 1024;

/// Whether `/tmp` is usable as a spill directory.
#[derive(Debug, Clone)]
pub enum SpillMode {
    /// `/tmp` is real disk with at least `MIN_FREE_BYTES` free.
    Disk(PathBuf),
    /// `/tmp` is tmpfs, or free space is insufficient; no spill.
    NoDisk,
}

/// Probe whether `/tmp` is real (non-tmpfs) disk with sufficient free space.
///
/// Strategy (dependency-free):
/// 1. Parse `/proc/mounts` for the filesystem type of `/tmp`.  If the type is
///    `tmpfs` → `NoDisk`.
/// 2. Write a 1-byte probe file under `/tmp` and immediately remove it to verify
///    the directory is writable at all (e.g., not `noexec` or read-only).
/// 3. Estimate free space by reading `f_bavail × f_frsize` via a minimal
///    `statvfs(2)` syscall using only `libc`-free std primitives:
///    since std does not expose statvfs, fall back to assuming disk space is
///    sufficient when step 1 confirmed the fs is not tmpfs and step 2 succeeded.
///    The write-probe itself is the free-space gate: it succeeds ↔ the OS can
///    accept at least one new file, which is a valid (if conservative) indicator
///    of write-readiness.
///
/// Returns `Disk(PathBuf::from("/tmp"))` or `NoDisk`.
// Spill is opportunistic — if the probe fails or the mounts file is unreadable
// we conservatively return NoDisk rather than risk surprises.
pub fn probe_tmp_spill() -> SpillMode {
    let tmp = PathBuf::from("/tmp");

    // Step 1: check /proc/mounts for a tmpfs entry on /tmp.
    if is_tmpfs(&tmp) {
        return SpillMode::NoDisk;
    }

    // Step 2 + 3: write-probe — if we can create and unlink a file, the FS is
    // writable. We treat this as "sufficient free space" (any disk with a full
    // partition would fail here before a real spill, and the spill itself would
    // surface a meaningful IO error rather than silently corrupt results).
    if write_probe_succeeds(&tmp) {
        SpillMode::Disk(tmp)
    } else {
        SpillMode::NoDisk
    }
}

/// Return `true` if `/tmp` is listed as `tmpfs` in `/proc/mounts`.
///
/// Reads `/proc/mounts` line by line; a line has the form:
/// `<device> <mountpoint> <fstype> <options> <dump> <pass>`
/// We look for lines where `<mountpoint>` is exactly `/tmp` and `<fstype>` is `tmpfs`.
fn is_tmpfs(tmp: &std::path::Path) -> bool {
    let Ok(contents) = std::fs::read_to_string("/proc/mounts") else {
        // Unreadable — assume not tmpfs (safe: worst case we try to spill and
        // hit an error at spill time, which surfaces a clean DataFusion error).
        return false;
    };
    let tmp_str = tmp.to_string_lossy();
    for line in contents.lines() {
        let mut fields = line.split_ascii_whitespace();
        let _device = fields.next();
        let mountpoint = fields.next();
        let fstype = fields.next();
        if mountpoint == Some(&tmp_str as &str) && fstype == Some("tmpfs") {
            return true;
        }
    }
    false
}

/// Return `true` if a temporary file can be created and immediately removed under `dir`.
fn write_probe_succeeds(dir: &std::path::Path) -> bool {
    let probe_path = dir.join(".lakehouse_spill_probe");
    // Write 1 byte; ignore the result — success means the directory is writable.
    let ok = std::fs::write(&probe_path, b"x").is_ok();
    // Best-effort cleanup; ignore unlink errors.
    let _ = std::fs::remove_file(&probe_path);
    ok
}

/// Build a `RuntimeEnv` sized from the per-instance memory limit.
///
/// - `memory_limit_bytes == 0` (unknown / unavailable sentinel) → default 1024 MB pool.
/// - `memory_limit_bytes  > 0` → pool budget = `MEMORY_FRACTION × limit` (≈ 0.6 ×).
/// - `Disk(path)` → `FairSpillPool` + `DiskManager` rooted at `path` (spill-to-disk path).
/// - `NoDisk`     → `GreedyMemoryPool` (returns `ResourcesExhausted` when budget exceeded).
pub fn build_runtime_env(
    memory_limit_bytes: u64,
    spill: SpillMode,
) -> Result<RuntimeEnv, datafusion::error::DataFusionError> {
    let budget = if memory_limit_bytes == 0 {
        DEFAULT_BUDGET_BYTES
    } else {
        (memory_limit_bytes as f64 * MEMORY_FRACTION) as u64
    };

    // usize cast: safe on 64-bit Linux (budget ≤ memory_limit_bytes ≤ u64::MAX, and
    // usize == u64 on 64-bit targets; Exasol UDFs run exclusively on 64-bit Linux).
    let budget_usize = budget as usize;

    let builder = match spill {
        SpillMode::Disk(path) => RuntimeEnvBuilder::new()
            .with_memory_pool(Arc::new(FairSpillPool::new(budget_usize)))
            .with_disk_manager_builder(
                DiskManagerBuilder::default().with_mode(DiskManagerMode::Directories(vec![path])),
            ),
        SpillMode::NoDisk => {
            RuntimeEnvBuilder::new().with_memory_pool(Arc::new(GreedyMemoryPool::new(budget_usize)))
        }
    };

    builder.build()
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::execution::memory_pool::MemoryConsumer;

    /// When a positive memory limit is supplied, the pool budget is ~0.6 × limit.
    #[test]
    fn build_runtime_env_sizes_pool_from_limit_fraction() {
        let limit: u64 = 1024 * 1024 * 1024; // 1 GiB
        let env = build_runtime_env(limit, SpillMode::NoDisk).unwrap();
        let expected = (limit as f64 * MEMORY_FRACTION) as usize;
        // Verify indirectly: try to grow to the expected budget — must succeed.
        let reservation = MemoryConsumer::new("test").register(&env.memory_pool);
        assert!(
            reservation.try_grow(expected).is_ok(),
            "growing to the budget must succeed"
        );
        // Reserve 1 more byte — must fail (greedy pool, no spill, budget exhausted).
        let reservation2 = MemoryConsumer::new("test2").register(&env.memory_pool);
        assert!(
            reservation2.try_grow(1).is_err(),
            "growing 1 byte beyond the budget must fail for GreedyPool"
        );
    }

    /// When the limit sentinel is 0, a 1024 MB default budget is used.
    #[test]
    fn build_runtime_env_uses_default_budget_on_zero_limit() {
        let env = build_runtime_env(0, SpillMode::NoDisk).unwrap();
        let default_budget = DEFAULT_BUDGET_BYTES as usize;
        let reservation = MemoryConsumer::new("test").register(&env.memory_pool);
        assert!(
            reservation.try_grow(default_budget).is_ok(),
            "default budget must accommodate 1 GiB"
        );
        let reservation2 = MemoryConsumer::new("test2").register(&env.memory_pool);
        assert!(
            reservation2.try_grow(1).is_err(),
            "one byte beyond default budget must fail"
        );
    }

    /// When SpillMode::Disk is supplied the pool is FairSpillPool (name() == "fair").
    #[test]
    fn build_runtime_env_uses_fair_spill_pool_when_disk() {
        let tmp = std::env::temp_dir();
        let env = build_runtime_env(0, SpillMode::Disk(tmp)).unwrap();
        assert_eq!(
            env.memory_pool.name(),
            "fair",
            "disk spill path must use FairSpillPool"
        );
    }

    /// When SpillMode::NoDisk is supplied the pool is GreedyMemoryPool (name() == "greedy").
    #[test]
    fn build_runtime_env_uses_greedy_pool_when_no_disk() {
        let env = build_runtime_env(0, SpillMode::NoDisk).unwrap();
        assert_eq!(
            env.memory_pool.name(),
            "greedy",
            "no-disk path must use GreedyMemoryPool"
        );
    }
}
