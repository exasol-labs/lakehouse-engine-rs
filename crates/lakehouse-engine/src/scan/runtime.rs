use datafusion::execution::disk_manager::{DiskManagerBuilder, DiskManagerMode};
use datafusion::execution::memory_pool::{FairSpillPool, GreedyMemoryPool};
use datafusion::execution::runtime_env::{RuntimeEnv, RuntimeEnvBuilder};
use std::path::PathBuf;
use std::sync::Arc;

/// Pool budget when the per-instance limit is unknown (0 sentinel).
pub(crate) const DEFAULT_BUDGET_BYTES: u64 = 1024 * 1024 * 1024;

/// Keeps the pool usable when `overhead_bytes ≥ memory_limit_bytes` collapses `net` toward zero.
pub(crate) const MIN_POOL_FLOOR_BYTES: u64 = 256 * 1024 * 1024;

#[derive(Debug, Clone)]
pub enum SpillMode {
    Disk(PathBuf),
    NoDisk,
}

/// Returns `NoDisk` when `/tmp` is tmpfs or not writable. std exposes no `statvfs`, so a
/// successful write probe stands in for the free-space check; a full disk later surfaces as a
/// clean spill IO error rather than wrong results.
pub fn probe_tmp_spill() -> SpillMode {
    let tmp = PathBuf::from("/tmp");

    if is_tmpfs(&tmp) {
        return SpillMode::NoDisk;
    }

    if write_probe_succeeds(&tmp) {
        SpillMode::Disk(tmp)
    } else {
        SpillMode::NoDisk
    }
}

/// Matches `/proc/mounts` lines `<device> <mountpoint> <fstype> ...` with mountpoint `/tmp`.
fn is_tmpfs(tmp: &std::path::Path) -> bool {
    let Ok(contents) = std::fs::read_to_string("/proc/mounts") else {
        // Worst case a spill attempt later fails with a clean DataFusion error.
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

fn write_probe_succeeds(dir: &std::path::Path) -> bool {
    let probe_path = dir.join(".lakehouse_spill_probe");
    let ok = std::fs::write(&probe_path, b"x").is_ok();
    let _ = std::fs::remove_file(&probe_path);
    ok
}

/// `memory_limit_bytes == 0` means unknown: use `DEFAULT_BUDGET_BYTES`, ignoring `fraction` and
/// `overhead_bytes`. Otherwise `budget = max((limit − overhead) × fraction, MIN_POOL_FLOOR_BYTES)`.
/// `NoDisk` uses `GreedyMemoryPool`, which returns `ResourcesExhausted` past the budget.
pub fn build_runtime_env(
    memory_limit_bytes: u64,
    fraction: f64,
    overhead_bytes: u64,
    spill: SpillMode,
) -> Result<RuntimeEnv, datafusion::error::DataFusionError> {
    let budget = if memory_limit_bytes == 0 {
        DEFAULT_BUDGET_BYTES
    } else {
        let net = memory_limit_bytes.saturating_sub(overhead_bytes);
        ((net as f64 * fraction) as u64).max(MIN_POOL_FLOOR_BYTES)
    };

    // Exasol UDFs run only on 64-bit Linux, where usize == u64.
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
#[path = "runtime_tests.rs"]
mod tests;
