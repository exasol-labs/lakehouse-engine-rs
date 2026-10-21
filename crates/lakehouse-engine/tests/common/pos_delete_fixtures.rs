//! Ground truth for the Spark merge-on-read positional-delete and deletion-vector
//! fixtures and the unsupported-format fixture. Authored at stack bring-up by the
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

pub const DELETION_VECTOR_TABLE: &str = "mor_dv";

/// One data file per range, in commit order. The first two carry a format-version-2
/// positional delete; the table then upgrades to version 3 and every later delete
/// writes a deletion vector.
pub const DELETION_VECTOR_FILE_ID_RANGES: [(i64, i64); 4] =
    [(1, 10), (41, 50), (11, 20), (1001, 200_000)];

/// Cardinalities of the planned vectors: the first file's merged {2, 5}, the third
/// file's replaced {15, 17}, and the large file's.
pub const DELETION_VECTOR_CARDINALITIES: [u64; 3] = [2, 2, 73_000];

pub fn deletion_vector_deleted(id: i64) -> bool {
    matches!(id, 2 | 5 | 15 | 17 | 42)
        || (id > 1000 && (id % 3 == 0 || (150_000..=160_000).contains(&id)))
}

pub fn deletion_vector_remaining_ids() -> impl Iterator<Item = i64> {
    DELETION_VECTOR_FILE_ID_RANGES
        .into_iter()
        .flat_map(|(first, last)| first..=last)
        .filter(|id| !deletion_vector_deleted(*id))
}

pub const UNSUPPORTED_FORMAT_TABLE: &str = "unsupported_format";

pub const PARQUET_FILE_IDS: (i64, i64) = (1, 10);

pub const ORC_FILE_IDS: (i64, i64) = (21, 30);
