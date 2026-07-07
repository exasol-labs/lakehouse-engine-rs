//! Ground truth for the Apache Spark Iceberg merge-on-read positional-delete
//! E2E fixtures (`packaging/positional-delete-fixtures`), plus the
//! format-version=3 Puffin deletion-vector fixture used to exercise the
//! unsupported-delete fail-loud path.
//!
//! Unlike `seed.rs`'s tables, these Iceberg tables are NOT seeded by this
//! Rust test harness: they are authored once, at Docker Compose stack
//! bring-up, by the `spark-iceberg-fixtures` one-shot job (see
//! `docker-compose.yml` and `scripts/spark-fixtures/`) running Apache
//! Spark's Iceberg Spark runtime against the SAME shared REST catalog +
//! MinIO every other E2E table uses (`NAMESPACE` below matches
//! `seed::E2E_NAMESPACE`).
//!
//! UPSTREAM TRACKING (apache/iceberg-rust#340): iceberg-rust 0.10 has no
//! position-delete writer, so the two positional-delete fixtures below cannot
//! be authored the same way as `seed.rs`'s tables — Apache Spark (an official
//! Apache Iceberg ecosystem engine) is used instead. DROP CONDITION: once
//! #340 lands and iceberg-rust exposes a position-delete writer, those two
//! fixtures' constants and Spark fixture scripts should be replaced by native
//! Rust fixture authoring here, matching `seed.rs`'s pattern. The
//! deletion-vector fixture (`DELETION_VECTOR_TABLE`) has its own, separate
//! upstream-tracking note — see its doc comment.
//!
//! The row/id/deletion facts below are NOT discovered at test time — they
//! are the fixed ground truth the Spark SQL scripts commit, and MUST stay in
//! lockstep with `scripts/spark-fixtures/create_file_granularity_fixture.sql`,
//! `create_partition_granularity_fixture.sql`, and
//! `create_deletion_vector_fixture.sql`.

/// Namespace shared with the other E2E seed tables (`seed::E2E_NAMESPACE`).
pub const NAMESPACE: &str = "e2e_lakehouse";

// ---------------------------------------------------------------------------
// write.delete.granularity=file fixture
// ---------------------------------------------------------------------------

/// Table name for the file-granularity merge-on-read positional-delete fixture.
pub const FILE_GRANULARITY_TABLE: &str = "mor_pos_file";

/// Total rows Spark inserted (id 1..=20), across TWO data files (ids 1..=10
/// and 11..=20 — one `INSERT` each).
pub const FILE_GRANULARITY_TOTAL_ROWS: usize = 20;

/// ids deleted by the fixture's `DELETE FROM ... WHERE id IN (...)` — two ids
/// from each of the two data files, so both files' own Parquet
/// positional-delete file is exercised (one delete file per data file, the
/// `file` granularity behavior).
pub const FILE_GRANULARITY_DELETED_IDS: [i64; 4] = [3, 8, 13, 17];

/// Row count after the delete: `FILE_GRANULARITY_TOTAL_ROWS -
/// FILE_GRANULARITY_DELETED_IDS.len()`.
pub const FILE_GRANULARITY_REMAINING_ROWS: usize = 16;

// ---------------------------------------------------------------------------
// write.delete.granularity=partition fixture
// ---------------------------------------------------------------------------

/// Table name for the partition-granularity merge-on-read positional-delete fixture.
pub const PARTITION_GRANULARITY_TABLE: &str = "mor_pos_partition";

/// Partition column (identity transform).
pub const PARTITION_COL: &str = "region";

/// Partition values — TWO partitions, each holding TWO data files.
pub const PARTITION_VALUES: [&str; 2] = ["east", "west"];

/// Inclusive id range per data file, in commit order: east file 1, east
/// file 2, west file 1, west file 2. Each range is its own `INSERT`, so each
/// is its own Iceberg data file.
pub const PARTITION_DATA_FILE_ID_RANGES: [(i64, i64); 4] = [(1, 5), (6, 10), (11, 15), (16, 20)];

/// Total rows Spark inserted across all four data files / both partitions.
pub const PARTITION_GRANULARITY_TOTAL_ROWS: usize = 20;

/// ids deleted by the fixture's single `DELETE` — two ids from each of the
/// four data files. With `write.delete.granularity=partition`, Iceberg
/// commits ONE positional-delete file per partition (not per data file): the
/// "east" delete file references BOTH east data files, and the "west" delete
/// file references BOTH west data files — so the committed delete files
/// collectively span multiple partitions, while each individually spans
/// multiple data files within its own partition.
pub const PARTITION_GRANULARITY_DELETED_IDS: [i64; 8] = [2, 4, 7, 9, 13, 14, 17, 19];

/// Row count after the delete: `PARTITION_GRANULARITY_TOTAL_ROWS -
/// PARTITION_GRANULARITY_DELETED_IDS.len()`.
pub const PARTITION_GRANULARITY_REMAINING_ROWS: usize = 12;

/// ids deleted from the "east" partition (subset of
/// `PARTITION_GRANULARITY_DELETED_IDS`, drawn from ids 1..=10) — the exact
/// rows the "east" partition-scoped delete file marks deleted.
pub const PARTITION_EAST_DELETED_IDS: [i64; 4] = [2, 4, 7, 9];

/// ids deleted from the "west" partition (subset of
/// `PARTITION_GRANULARITY_DELETED_IDS`, drawn from ids 11..=20) — the exact
/// rows the "west" partition-scoped delete file marks deleted.
pub const PARTITION_WEST_DELETED_IDS: [i64; 4] = [13, 14, 17, 19];

// ---------------------------------------------------------------------------
// format-version=3 Puffin deletion-vector fixture (unsupported delete)
// ---------------------------------------------------------------------------

/// Table name for the `format-version=3` merge-on-read fixture whose DELETE
/// commits a Puffin deletion vector instead of a Parquet positional-delete
/// file — the delete mechanism `e2e_unsupported_delete_fails_loud` exercises.
/// Authored by `scripts/spark-fixtures/create_deletion_vector_fixture.sql`
/// (kept in lockstep with it — do not change one without the other).
///
/// Unlike `FILE_GRANULARITY_TABLE` / `PARTITION_GRANULARITY_TABLE`, this
/// table's post-delete row/id ground truth is NOT tracked here: the engine
/// is expected to reject any query against it at plan time (Task 1.3's
/// fail-loud gate in `adapter/pushdown.rs`), so there is no successful read
/// to assert row-level ground truth against.
///
/// UPSTREAM TRACKING (apache/iceberg-rust#2681, #2580, #2411): once
/// iceberg-rust gains v3 deletion-vector READ support, this fixture becomes
/// READABLE (not just rejected) — see the fixture SQL's own header comment
/// for the follow-up this implies for `e2e_unsupported_delete_fails_loud`.
pub const DELETION_VECTOR_TABLE: &str = "mor_dv_unsupported";
