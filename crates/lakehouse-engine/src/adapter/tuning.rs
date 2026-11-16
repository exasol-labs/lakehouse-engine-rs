use std::num::NonZeroUsize;

use super::properties::{ThreadingMode, TuningProperties, VsProperties};

pub(super) const DEFAULT_PARALLELISM_FACTOR: usize = 1;
/// DataFusion's default rows per RecordBatch.
pub(super) const DEFAULT_DF_BATCH_SIZE: usize = 8192;
/// Fraction of the net per-instance RSS budget allocated to the DataFusion memory pool.
pub(super) const DEFAULT_MEMORY_POOL_FRACTION: f64 = 0.6;
/// Fixed container/binary overhead (MB) subtracted from the per-instance RSS limit before
/// applying the pool fraction.
pub(super) const DEFAULT_INSTANCE_OVERHEAD_MB: u64 = 200;
pub(super) const DEFAULT_JOIN_BROADCAST_MAX_BYTES: u64 = 134_217_728;
/// The reads wait on the network, so the default does not follow the core count.
pub(super) const DEFAULT_METADATA_READ_CONCURRENCY: NonZeroUsize = NonZeroUsize::new(16).unwrap();
/// Connections per decode thread: S3 range GETs are latency-bound, so several in flight per
/// thread keep the NIC busy, and idle pooled connections are far cheaper than threads.
pub(super) const S3_CONNECTIONS_PER_THREAD: usize = 4;

/// The tuning a request derives from its properties and the adapter VM's core count.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct Tuning {
    pub(crate) parallelism_factor: usize,
    pub(crate) df_target_partitions: usize,
    pub(crate) df_threads_per_udf: usize,
    pub(crate) df_batch_size: usize,
    pub(crate) memory_pool_fraction: f64,
    pub(crate) instance_overhead_mb: u64,
    pub(crate) s3_max_connections: usize,
    pub(crate) join_broadcast_max_bytes: u64,
}

impl Tuning {
    /// Read at every request, so under AUTO the counts follow the cores the request runs on.
    pub(super) fn of(props: &VsProperties) -> Self {
        Self::resolve(&props.checked.tuning, resolve_nr_of_cores())
    }

    /// Applies each absent property's default and the AUTO/FIXED derivation. Absent FIXED counts
    /// take the AUTO values, and an absent `S3_MAX_CONNECTIONS` is `S3_CONNECTIONS_PER_THREAD` per
    /// thread.
    fn resolve(checked: &TuningProperties, nr_of_cores: u32) -> Self {
        let df_threading_mode = checked.threading_mode.unwrap_or(ThreadingMode::Auto);
        let (auto_partitions, auto_threads) = auto_df_threading(nr_of_cores);
        let (df_target_partitions, df_threads_per_udf) = match df_threading_mode {
            ThreadingMode::Fixed => (
                checked
                    .df_target_partitions
                    .map_or(auto_partitions, |n| usize::from(n.get())),
                checked
                    .df_threads_per_udf
                    .map_or(auto_threads, |n| usize::from(n.get())),
            ),
            ThreadingMode::Auto => (auto_partitions, auto_threads),
        };
        Self {
            parallelism_factor: checked
                .parallelism_factor
                .map_or(DEFAULT_PARALLELISM_FACTOR, |n| usize::from(n.get())),
            df_target_partitions,
            df_threads_per_udf,
            df_batch_size: checked
                .df_batch_size
                .map_or(DEFAULT_DF_BATCH_SIZE, |n| n.get() as usize),
            memory_pool_fraction: checked
                .memory_pool_fraction
                .unwrap_or(DEFAULT_MEMORY_POOL_FRACTION),
            instance_overhead_mb: checked
                .instance_overhead_mb
                .map_or(DEFAULT_INSTANCE_OVERHEAD_MB, u64::from),
            s3_max_connections: checked
                .s3_max_connections
                .map_or(df_threads_per_udf * S3_CONNECTIONS_PER_THREAD, |n| {
                    usize::from(n.get())
                }),
            join_broadcast_max_bytes: checked
                .join_broadcast_max_bytes
                .map_or(DEFAULT_JOIN_BROADCAST_MAX_BYTES, |n| n.get()),
        }
    }
}

/// The bound on concurrent metadata reads, which needs no core count.
pub(super) fn metadata_read_concurrency(checked: &TuningProperties) -> NonZeroUsize {
    checked
        .metadata_read_concurrency
        .map_or(DEFAULT_METADATA_READ_CONCURRENCY, NonZeroUsize::from)
}

/// Exasol runs one scalar scan VM per node, so that instance owns every core. Partitions
/// beyond threads overlap object-store I/O with decoding.
fn auto_df_threading(nr_of_cores: u32) -> (usize, usize) {
    let threads = (nr_of_cores as usize).max(1);
    (2 * threads, threads)
}

/// `available_parallelism()` honours the CPU quota of the adapter VM's container.
pub(super) fn resolve_nr_of_cores() -> u32 {
    core_count_or_default(std::thread::available_parallelism())
}

fn core_count_or_default(detected: std::io::Result<NonZeroUsize>) -> u32 {
    detected.map_or(1, |n| n.get() as u32)
}

#[cfg(test)]
#[path = "tuning_tests.rs"]
mod tests;
