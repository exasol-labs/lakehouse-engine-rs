//! A writer's partition buffer: rows wait in memory per partition of a partitioned table's current
//! spec, and the largest partitions go out as data files whenever the buffer passes its budget.

use arrow::record_batch::RecordBatch;
use iceberg::arrow::RecordBatchPartitionSplitter;
use iceberg::spec::{PartitionKey, PartitionSpec, SchemaRef, Struct};
use std::collections::HashMap;
use std::sync::Arc;

const MIB: u64 = 1024 * 1024;
/// The engine's own share of the per-instance limit, the same as the scan's.
const OVERHEAD_BYTES: u64 = crate::scan::spec::DEFAULT_INSTANCE_OVERHEAD_MB * MIB;
const MIN_BUDGET_BYTES: u64 = 64 * MIB;
/// A larger buffer would only make larger data files.
const MAX_BUDGET_BYTES: u64 = 512 * MIB;

/// The Arrow memory a writer may buffer: half the per-instance `memory_limit` after the overhead,
/// within 64 and 512 MiB, or 512 MiB when the limit is unknown (0).
pub(super) fn buffer_budget(memory_limit: u64) -> usize {
    let budget = match memory_limit {
        0 => MAX_BUDGET_BYTES,
        limit => {
            (limit.saturating_sub(OVERHEAD_BYTES) / 2).clamp(MIN_BUDGET_BYTES, MAX_BUDGET_BYTES)
        }
    };
    usize::try_from(budget).unwrap_or(usize::MAX)
}

/// The positions in `sizes` to write out, largest first, until the rest fit `budget`.
pub(super) fn write_out(sizes: &[usize], budget: usize) -> Vec<usize> {
    let mut order: Vec<usize> = (0..sizes.len()).collect();
    order.sort_by(|a, b| sizes[*b].cmp(&sizes[*a]));
    let mut total: usize = sizes.iter().sum();
    order
        .into_iter()
        .take_while(|&index| {
            let over = total > budget;
            total -= sizes[index];
            over
        })
        .collect()
}

/// The one partition of an unpartitioned `spec`: a null value per field, because iceberg-rust's
/// fast append refuses a data file whose partition arity differs from its spec's.
pub(super) fn null_key(spec: &PartitionSpec, schema: SchemaRef) -> PartitionKey {
    let nulls = spec.fields().iter().map(|_| None).collect();
    PartitionKey::new(spec.clone(), schema, nulls)
}

struct Buffered {
    key: PartitionKey,
    batches: Vec<RecordBatch>,
    bytes: usize,
}

/// One partition's buffered rows, ready to go into the partition's data files.
pub(super) type Partition = (PartitionKey, Vec<RecordBatch>);

pub(super) struct PartitionBuffer {
    splitter: RecordBatchPartitionSplitter,
    budget: usize,
    buffered: HashMap<Struct, Buffered>,
}

impl PartitionBuffer {
    pub(super) fn new(
        schema: SchemaRef,
        spec: &PartitionSpec,
        budget: usize,
    ) -> iceberg::Result<Self> {
        debug_assert!(
            !spec.is_unpartitioned(),
            "the sink streams an unpartitioned table's rows"
        );
        let splitter = RecordBatchPartitionSplitter::try_new_with_computed_values(
            schema,
            Arc::new(spec.clone()),
        )?;
        Ok(Self {
            splitter,
            budget,
            buffered: HashMap::new(),
        })
    }

    /// Buffers `batch`'s rows by partition and hands back the partitions that must go out so
    /// that the rest fit the budget.
    pub(super) fn push(&mut self, batch: &RecordBatch) -> iceberg::Result<Vec<Partition>> {
        for (key, part) in self.splitter.split(batch)? {
            let bytes = part.get_array_memory_size();
            let entry = self
                .buffered
                .entry(key.data().clone())
                .or_insert_with(|| Buffered {
                    key,
                    batches: Vec::new(),
                    bytes: 0,
                });
            entry.batches.push(part);
            entry.bytes += bytes;
        }
        let values: Vec<Struct> = self.buffered.keys().cloned().collect();
        let sizes: Vec<usize> = values
            .iter()
            .map(|value| self.buffered[value].bytes)
            .collect();
        Ok(write_out(&sizes, self.budget)
            .into_iter()
            .filter_map(|index| self.buffered.remove(&values[index]))
            .map(|buffered| (buffered.key, buffered.batches))
            .collect())
    }

    /// Every partition still buffered.
    pub(super) fn drain(&mut self) -> Vec<Partition> {
        self.buffered
            .drain()
            .map(|(_, buffered)| (buffered.key, buffered.batches))
            .collect()
    }
}

#[cfg(test)]
#[path = "fanout_tests.rs"]
mod tests;
