//! Ground truth for the Apache Spark Iceberg merge-on-read positional-delete,
//! deletion-vector, mixed-mechanism, and still-unsupported-mechanism E2E
//! fixtures (`packaging/positional-delete-fixtures`,
//! `packaging/deletion-vector-fixtures`).
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
//! Rust fixture authoring here, matching `seed.rs`'s pattern.
//!
//! The row/id/deletion facts below are NOT discovered at test time — they
//! are the fixed ground truth the Spark SQL scripts commit, and MUST stay in
//! lockstep with `scripts/spark-fixtures/create_file_granularity_fixture.sql`,
//! `create_partition_granularity_fixture.sql`, `create_deletion_vector_fixture.sql`,
//! `create_mixed_mechanism_fixture.sql`, and `create_orc_unsupported_fixture.sql`.

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
// format-version=3 Puffin deletion-vector fixture (positive fixture)
// ---------------------------------------------------------------------------

/// Table name for the `format-version=3` merge-on-read deletion-vector
/// fixture. Authored by
/// `scripts/spark-fixtures/create_deletion_vector_fixture.sql` (kept in
/// lockstep with it — do not change one without the other).
///
/// Originally authored (PR #72) as a fail-loud-only fixture (Iceberg v3
/// deletion vectors were then an unsupported mechanism). `add-deletion-
/// vector-application` repurposed it into a POSITIVE fixture: the engine now
/// decodes the Puffin `deletion-vector-v1` blob and applies it on read (see
/// `datafusion-scan/scan-execution-deletion-vectors`), so this table returns
/// its post-delete rows through the VS, exactly like `FILE_GRANULARITY_TABLE`
/// / `PARTITION_GRANULARITY_TABLE`.
pub const DELETION_VECTOR_TABLE: &str = "mor_dv";

/// Total rows Spark inserted (id 1..=10), written as ONE data file.
pub const DELETION_VECTOR_TOTAL_ROWS: usize = 10;

/// ids deleted by the fixture's `DELETE FROM ... WHERE id IN (...)` — a
/// strict subset of the single data file's rows, so Iceberg commits a Puffin
/// deletion vector referencing that data file rather than rewriting or
/// dropping it.
pub const DELETION_VECTOR_DELETED_IDS: [i64; 2] = [3, 7];

/// Row count after the delete: `DELETION_VECTOR_TOTAL_ROWS -
/// DELETION_VECTOR_DELETED_IDS.len()`.
pub const DELETION_VECTOR_REMAINING_ROWS: usize = 8;

// ---------------------------------------------------------------------------
// Mixed positional-delete + deletion-vector fixture (v2->v3 migration shape)
// ---------------------------------------------------------------------------

/// Table name for the mixed-mechanism fixture: one data file resolved via a
/// Parquet positional-delete file (committed while the table was
/// format-version=2), another resolved via a v3 Puffin deletion vector
/// (committed after an in-place upgrade to format-version=3). Authored by
/// `scripts/spark-fixtures/create_mixed_mechanism_fixture.sql` (kept in
/// lockstep with it — do not change one without the other).
pub const MIXED_MECHANISM_TABLE: &str = "mor_mixed";

/// Total rows Spark inserted across both data files (ids 1..=10, 11..=20).
pub const MIXED_MECHANISM_TOTAL_ROWS: usize = 20;

/// ids deleted from data file A (ids 1..=10) via a Parquet positional-delete
/// file, while the table was still format-version=2.
pub const MIXED_MECHANISM_POS_DELETED_IDS: [i64; 2] = [3, 7];

/// ids deleted from data file B (ids 11..=20) via a v3 Puffin deletion
/// vector, after the table was upgraded to format-version=3.
pub const MIXED_MECHANISM_DV_DELETED_IDS: [i64; 2] = [13, 17];

/// The combined deleted-id set across both mechanisms — the union of
/// `MIXED_MECHANISM_POS_DELETED_IDS` and `MIXED_MECHANISM_DV_DELETED_IDS`.
pub const MIXED_MECHANISM_DELETED_IDS: [i64; 4] = [3, 7, 13, 17];

/// Row count after both deletes: `MIXED_MECHANISM_TOTAL_ROWS -
/// MIXED_MECHANISM_DELETED_IDS.len()`.
pub const MIXED_MECHANISM_REMAINING_ROWS: usize = 16;

// ---------------------------------------------------------------------------
// Still-unsupported delete mechanism fixture (ORC data file)
// ---------------------------------------------------------------------------

/// Table name for the fixture whose data file is ORC (not Parquet) — a
/// mechanism `classify_manifest_file` in `adapter/pushdown.rs` still rejects
/// at plan time, exercised by `e2e_unsupported_delete_fails_loud`. Authored
/// by `scripts/spark-fixtures/create_orc_unsupported_fixture.sql` (kept in
/// lockstep with it — do not change one without the other).
///
/// This table's post-delete row/id ground truth is NOT tracked here: the
/// engine is expected to reject any query against it at plan time (no
/// delete is even involved — an ORC DATA file alone is unsupported), so
/// there is no successful read to assert row-level ground truth against.
///
/// Replaces `DELETION_VECTOR_TABLE` as the fail-loud target now that
/// deletion vectors are a supported mechanism (see its doc comment above).
/// Equality deletes remain untested at the E2E level — only Flink's
/// row-level upsert connectors write them, and Flink is not part of this
/// stack (`scripts/spark-fixtures/run_fixtures.sh`'s header) — but equality
/// deletes share the same plan-time gate (`classify_manifest_file`) as the
/// ORC arm exercised here, and are covered directly by that function's unit
/// tests.
pub const ORC_UNSUPPORTED_TABLE: &str = "mor_orc_unsupported";
