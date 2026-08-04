//! Ground truth for the Apache Spark far-future INT96 timestamp E2E fixture
//! (`packaging/int96-timestamp-fixture`), used to prove the `coerce_int96`
//! decode fix (issue #143 — `Cast error: Overflow converting 9999-12-31
//! 23:59:59 to Nanosecond`) against a genuinely INT96-encoded Iceberg table.
//!
//! Like the positional-delete fixtures in `pos_delete_fixtures.rs`, this
//! table is NOT seeded by this Rust test harness: it is authored once, at
//! Docker Compose stack bring-up, by the `spark-iceberg-fixtures` one-shot
//! job (see `docker-compose.yml` and `scripts/spark-fixtures/`) running
//! Apache Spark's Iceberg Spark runtime against the SAME shared REST catalog
//! and MinIO every other E2E table uses (`NAMESPACE` below matches
//! `seed::E2E_NAMESPACE`). Only a native Spark write with
//! `spark.sql.parquet.outputTimestampType=INT96` followed by an Iceberg
//! `add_files` import can land a genuinely INT96-encoded column — Iceberg's
//! own Spark writer (a plain `INSERT INTO`) always emits INT64 regardless of
//! that setting, so this fixture cannot be authored any other way.
//!
//! The table/column/value ground truth below is NOT discovered at test time
//! — it is the fixed ground truth
//! `scripts/spark-fixtures/create_int96_timestamp_fixture.sql` commits, and
//! MUST stay in lockstep with that script.
//!
//! WHY `timestamp` and not `timestamptz`: this fixture's column is
//! deliberately an Iceberg `timestamp` (WITHOUT time zone) column, not
//! `timestamptz`. The INT96 overflow decode fix (`coerce_int96`, `#143`) is
//! the behavior under test, and a naive `timestamp` column is the minimal
//! carrier for it — `timestamptz` would add an unrelated time-zone-mapping
//! variable to a fixture whose only job is proving the overflow fix.
//! `timestamptz` COLUMNS already reach the scan-emit path fine (#118 —
//! `types/mapping.rs` maps them to plain Exasol `TIMESTAMP`), so this
//! fixture's `timestamp` choice is scope narrowing, not a workaround for a
//! column-path constraint.

/// Namespace shared with the other E2E seed tables (`seed::E2E_NAMESPACE`).
pub const NAMESPACE: &str = "e2e_lakehouse";

/// Table name for the far-future INT96 timestamp fixture. Full ref:
/// `rest_catalog.e2e_lakehouse.int96_ts_far_future`.
pub const INT96_TS_FAR_FUTURE_TABLE: &str = "int96_ts_far_future";

/// Name of the fixture's Iceberg `timestamp` (WITHOUT time zone) column —
/// see the module doc comment for why this is `timestamp`, not
/// `timestamptz`.
pub const INT96_TS_FAR_FUTURE_COLUMN: &str = "ts";

/// Expected value of the fixture's single row, as Exasol/DataFusion render
/// a microsecond-resolution `TIMESTAMP` — the far-future value (issue #143)
/// that overflows arrow-rs's default INT96→Nanosecond decode.
pub const INT96_TS_FAR_FUTURE_EXPECTED_VALUE: &str = "9999-12-31 23:59:59";
