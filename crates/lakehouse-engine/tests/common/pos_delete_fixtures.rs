//! Ground truth for the Spark merge-on-read positional-delete fixtures and the v3
//! Puffin deletion-vector fixture. Authored at stack bring-up by the
//! `spark-iceberg-fixtures` job because iceberg-rust has no position-delete writer
//! (#344). Must stay in lockstep with the `scripts/spark-fixtures/*.sql` scripts.

pub const NAMESPACE: &str = "e2e_lakehouse";

pub const FILE_GRANULARITY_TABLE: &str = "mor_pos_file";

/// Two data files: ids 1..=10 and 11..=20.
pub const FILE_GRANULARITY_TOTAL_ROWS: usize = 20;

/// Two ids from each data file, so each file's own positional-delete file is exercised.
pub const FILE_GRANULARITY_DELETED_IDS: [i64; 4] = [3, 8, 13, 17];

pub const FILE_GRANULARITY_REMAINING_ROWS: usize = 16;

pub const PARTITION_GRANULARITY_TABLE: &str = "mor_pos_partition";

pub const PARTITION_COL: &str = "region";

pub const PARTITION_VALUES: [&str; 2] = ["east", "west"];

/// One data file per range, in commit order: east 1, east 2, west 1, west 2.
pub const PARTITION_DATA_FILE_ID_RANGES: [(i64, i64); 4] = [(1, 5), (6, 10), (11, 15), (16, 20)];

pub const PARTITION_GRANULARITY_TOTAL_ROWS: usize = 20;

/// Two ids per data file. Partition granularity commits one delete file per
/// partition, each referencing both of that partition's data files.
pub const PARTITION_GRANULARITY_DELETED_IDS: [i64; 8] = [2, 4, 7, 9, 13, 14, 17, 19];

pub const PARTITION_GRANULARITY_REMAINING_ROWS: usize = 12;

pub const PARTITION_EAST_DELETED_IDS: [i64; 4] = [2, 4, 7, 9];

pub const PARTITION_WEST_DELETED_IDS: [i64; 4] = [13, 14, 17, 19];

/// Its DELETE commits a Puffin deletion vector, which the engine rejects at plan
/// time; becomes readable once iceberg-rust supports v3 deletion vectors (#12).
pub const DELETION_VECTOR_TABLE: &str = "mor_dv_unsupported";
