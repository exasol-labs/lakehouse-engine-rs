//! End-to-end positional-delete matrix for the lakehouse-engine Virtual Schema.
//!
//! Drives Iceberg merge-on-read positional-delete tables through the full
//! VS → adapter → scan UDF → DataFusion stack against the local Exasol
//! Docker + Apache Spark fixtures (`packaging/e2e-harness-positional-deletes`,
//! `packaging/positional-delete-fixtures`).
//!
//! The fixture tables (`mor_pos_file`, `mor_pos_partition`, and the
//! unsupported-delete `mor_dv_unsupported`) are authored ONCE by the
//! `spark-iceberg-fixtures` one-shot Compose job at stack bring-up (see
//! `scripts/spark-fixtures/`) — this file never seeds them itself, only the
//! delete-free `events` table used by the no-regression scenario. Ground
//! truth for the fixtures lives in `tests/common/pos_delete_fixtures.rs` and
//! MUST stay in lockstep with the Spark SQL scripts that produce them.
//!
//! All tests FAIL (never skip) when the stack is unavailable — per project
//! rules — because every test starts with `setup_e2e()`, which panics (via
//! `wait_for_exasol`/`wait_for_minio`/`wait_for_iceberg_catalog`) rather than
//! returning an `Err` when a dependency is down.
#![cfg(feature = "exasol-e2e")]

mod common;
use common::e2e_harness::*;
use common::exasol_ws::ExaConn;
use common::pos_delete_fixtures::{
    DELETION_VECTOR_TABLE, FILE_GRANULARITY_DELETED_IDS, FILE_GRANULARITY_REMAINING_ROWS,
    FILE_GRANULARITY_TABLE, FILE_GRANULARITY_TOTAL_ROWS, NAMESPACE, PARTITION_COL,
    PARTITION_EAST_DELETED_IDS, PARTITION_GRANULARITY_DELETED_IDS,
    PARTITION_GRANULARITY_REMAINING_ROWS, PARTITION_GRANULARITY_TABLE,
    PARTITION_GRANULARITY_TOTAL_ROWS, PARTITION_WEST_DELETED_IDS,
};
use common::seed::{E2E_TABLE, SEED_ROWS_SCORE_GT_15, SEED_TOTAL_ROWS, seed_events};
use common::stack::{
    iceberg_catalog_url, wait_for_exasol, wait_for_iceberg_catalog, wait_for_minio,
};

use lakehouse_engine::adapter::pushdown::shard_count;
use lakehouse_engine::adapter::sharding::partition_files_by_bytes;
use lakehouse_engine::scan::spec::DeleteMechanism;

use std::collections::{HashMap, HashSet};
use std::sync::OnceLock;

// ---------------------------------------------------------------------------
// Constants (schema/script names mirror e2e_scan_test.rs / e2e_capability_test.rs
// / e2e_count_distinct_test.rs — same .so, same idempotent CREATE OR REPLACE
// objects, shared across every E2E test binary).
// ---------------------------------------------------------------------------

/// Shared VS used by every other E2E test binary too (idempotent CREATE OR
/// REPLACE with an identical body, so concurrent recreation is harmless).
const VS_NAME: &str = "MY_LAKEHOUSE";
/// Dedicated VS forcing a single work-unit shard (`PARALLELISM_FACTOR = 1`)
/// over the shared namespace, used only by the fan-out-invariance test.
const SAMESHARD_VS_NAME: &str = "POSDEL_SAMESHARD_VS";
/// Dedicated VS forcing one shard per data file
/// (`PARALLELISM_FACTOR = SPLIT_PARALLELISM_FACTOR`, chosen to equal the
/// `mor_pos_partition` fixture's data-file count), used only by the
/// fan-out-invariance test.
const SPLITSHARD_VS_NAME: &str = "POSDEL_SPLITSHARD_VS";
/// `mor_pos_partition` has 4 data files (2 partitions × 2 files); setting
/// `PARALLELISM_FACTOR` to this value on a 1-node cluster makes
/// `shard_count(1, SPLIT_PARALLELISM_FACTOR, 4) == 4`, so every shard gets
/// exactly one file (see `partition_files_by_bytes`'s greedy-lightest-shard
/// assignment: with as many shards as files, each shard is filled exactly
/// once, regardless of file byte sizes) — a deterministic split-shard
/// placement, not a hash-partitioning gamble.
const SPLIT_PARALLELISM_FACTOR: usize = 4;

// ---------------------------------------------------------------------------
// One-time setup (idempotent; identical shape to the other E2E test binaries)
// ---------------------------------------------------------------------------

static SETUP_DONE: OnceLock<()> = OnceLock::new();

fn setup_e2e() {
    SETUP_DONE.get_or_init(|| {
        wait_for_exasol();
        wait_for_minio();
        wait_for_iceberg_catalog();

        // The mor_pos_file / mor_pos_partition fixtures are authored by the
        // spark-iceberg-fixtures Compose job, not by this harness. We only
        // need to seed the delete-free `events` table used by
        // e2e_delete_free_table_no_regression.
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("tokio runtime");
        rt.block_on(async {
            seed_events(&iceberg_catalog_url(), "s3://warehouse/")
                .await
                .expect("seed Iceberg events table")
        });

        install_slc();
        upload_so();

        let mut conn = exa_conn();
        create_schema_and_scripts(&mut conn);
        create_virtual_schema(&mut conn, &VsProps::new(VS_NAME, NAMESPACE));
        create_virtual_schema(
            &mut conn,
            &VsProps::new(SAMESHARD_VS_NAME, NAMESPACE).with_parallelism_factor(1),
        );
        create_virtual_schema(
            &mut conn,
            &VsProps::new(SPLITSHARD_VS_NAME, NAMESPACE)
                .with_parallelism_factor(SPLIT_PARALLELISM_FACTOR),
        );
    });
}

fn vs_table(vs_name: &str, table: &str) -> String {
    format!("{vs_name}.{}", table.to_uppercase())
}

fn ids_column(cols: &[Vec<serde_json::Value>]) -> Vec<i64> {
    cols[0].iter().map(parse_int).collect()
}

// ---------------------------------------------------------------------------
// Fixture-shape tests (packaging/positional-delete-fixtures) — inspect the
// Spark-committed Iceberg manifests directly via the Iceberg reader, bypassing
// Exasol, to verify the fixtures actually have the delete-file shape the
// e2e_* correctness tests below assume.
// ---------------------------------------------------------------------------

/// Spark's `write.delete.granularity=file` fixture commits exactly one
/// Parquet positional-delete file PER data file (two data files → two
/// distinct delete files), verified by directly reading the table's
/// `position_deletes` metadata table: delete file 1 contains ONLY entries
/// whose `file_path` is data file 1, delete file 2 contains ONLY entries
/// whose `file_path` is data file 2. No cross-references.
///
/// UPSTREAM TRACKING (apache/iceberg-rust#2532, pre-work for #340): what this
/// test can actually OBSERVE through the Iceberg reader is weaker than what
/// Spark committed. `iceberg-rust` 0.10.0's `DeleteFileIndex` has not
/// yet closed the TODO in `delete_file_index.rs` that gates position deletes
/// by their `referenced_data_file` field — it still applies every
/// partition-scoped position-delete file to every data file in the same
/// partition (correct for `write.delete.granularity=partition`, but for
/// `granularity=file` on this UNPARTITIONED table it means each of the two
/// data files resolves BOTH delete files, not just its own). PR #2532 closes
/// that TODO but is not yet merged/released. DROP CONDITION: once a release
/// containing #2532 is picked up, tighten this assertion back to "exactly 1
/// delete file per data file, referencing only that file" (the ORIGINAL,
/// intended assertion — see git history) and cross-check against
/// `position_deletes` as done here to confirm the read side, not just the
/// write side, is now correct.
#[test]
fn fixture_spark_file_granularity_delete_table() {
    setup_e2e();

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");
    let files = rt.block_on(resolve_fixture_files(NAMESPACE, FILE_GRANULARITY_TABLE));
    assert_eq!(
        files.len(),
        2,
        "mor_pos_file must resolve exactly 2 data files, got {}: {files:?}",
        files.len()
    );

    let total_size: usize = FILE_GRANULARITY_TOTAL_ROWS;
    assert!(
        total_size > 0,
        "sanity: fixture ground truth must be non-empty"
    );

    // Achievable invariant given the upstream gap documented above: each data
    // file resolves BOTH partition-scoped delete files (iceberg-rust cannot
    // yet narrow to the one it actually needs), and there are exactly 2
    // distinct delete files overall, each referenced by both data files.
    let mut refs_per_delete_path: HashMap<String, usize> = HashMap::new();
    for entry in &files {
        assert_eq!(
            entry.deletes.len(),
            2,
            "file granularity (pending apache/iceberg-rust#2532): data file {} \
             must resolve both partition-scoped delete files, got {}",
            entry.path,
            entry.deletes.len()
        );
        for delete in &entry.deletes {
            assert!(
                matches!(delete, DeleteMechanism::IcebergPositionalDelete { .. }),
                "file granularity: delete file for {} must be a Parquet positional delete",
                entry.path
            );
            *refs_per_delete_path
                .entry(
                    delete
                        .object_store_path()
                        .expect("positional delete path")
                        .to_string(),
                )
                .or_insert(0) += 1;
        }
    }
    assert_eq!(
        refs_per_delete_path.len(),
        2,
        "file granularity: Spark must commit exactly 2 distinct delete files \
         (one per data file), got {refs_per_delete_path:?}"
    );
    for (path, count) in &refs_per_delete_path {
        assert_eq!(
            *count, 2,
            "delete file {path} must be resolved by both data files under the \
             current iceberg-rust partition-scoped matching, got {count}"
        );
    }
}

/// Spark's `write.delete.granularity=partition` fixture commits exactly one
/// Parquet positional-delete file PER PARTITION (four data files, two
/// partitions → two delete files, each referenced by exactly the two data
/// files of its own partition).
#[test]
fn fixture_spark_partition_granularity_delete_table() {
    setup_e2e();

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");
    let files = rt.block_on(resolve_fixture_files(
        NAMESPACE,
        PARTITION_GRANULARITY_TABLE,
    ));
    assert_eq!(
        files.len(),
        4,
        "mor_pos_partition must resolve exactly 4 data files, got {}: {files:?}",
        files.len()
    );

    let mut refs_per_delete_path: HashMap<String, usize> = HashMap::new();
    for entry in &files {
        assert_eq!(
            entry.deletes.len(),
            1,
            "partition granularity: data file {} must have exactly 1 associated \
             delete file, got {}",
            entry.path,
            entry.deletes.len()
        );
        assert!(
            matches!(
                entry.deletes[0],
                DeleteMechanism::IcebergPositionalDelete { .. }
            ),
            "partition granularity: delete file for {} must be a Parquet positional delete",
            entry.path
        );
        *refs_per_delete_path
            .entry(
                entry.deletes[0]
                    .object_store_path()
                    .expect("positional delete path")
                    .to_string(),
            )
            .or_insert(0) += 1;
    }
    assert_eq!(
        refs_per_delete_path.len(),
        2,
        "partition granularity: exactly 2 partition-scoped delete files must be \
         committed (one per partition), got {refs_per_delete_path:?}"
    );
    for (path, count) in &refs_per_delete_path {
        assert_eq!(
            *count, 2,
            "partition-scoped delete file {path} must be referenced by exactly \
             2 data files (its partition's two files), got {count}"
        );
    }
}

// ---------------------------------------------------------------------------
// End-to-end correctness: file granularity
// ---------------------------------------------------------------------------

/// A `SELECT` over the file-granularity delete table returns exactly the
/// seeded rows minus the recorded deleted rows, with no deleted id present.
#[test]
fn e2e_file_granularity_returns_post_delete_rows() {
    setup_e2e();
    let mut conn = exa_conn();

    let sql = format!(
        "SELECT id FROM {} ORDER BY id",
        vs_table(VS_NAME, FILE_GRANULARITY_TABLE)
    );
    let cols = conn.query_columns(&sql);
    let ids = ids_column(&cols);

    assert_eq!(
        ids.len(),
        FILE_GRANULARITY_REMAINING_ROWS,
        "mor_pos_file must return {FILE_GRANULARITY_REMAINING_ROWS} rows post-delete, \
         got {}: {ids:?}",
        ids.len()
    );

    let deleted: HashSet<i64> = FILE_GRANULARITY_DELETED_IDS.iter().copied().collect();
    let expected: Vec<i64> = (1..=FILE_GRANULARITY_TOTAL_ROWS as i64)
        .filter(|id| !deleted.contains(id))
        .collect();
    assert_eq!(
        ids, expected,
        "mor_pos_file post-delete id set must be exactly {expected:?}, got {ids:?}"
    );

    for id in &ids {
        assert!(
            !deleted.contains(id),
            "deleted id {id} must NOT appear in the post-delete result"
        );
    }
}

// ---------------------------------------------------------------------------
// End-to-end correctness: partition granularity
// ---------------------------------------------------------------------------

/// A `SELECT` over the partition-granularity delete table returns exactly the
/// seeded rows minus the recorded deleted rows, each partition-scoped delete
/// file applied only to the data files it references.
#[test]
fn e2e_partition_granularity_returns_post_delete_rows() {
    setup_e2e();
    let mut conn = exa_conn();

    let sql = format!(
        "SELECT id FROM {} ORDER BY id",
        vs_table(VS_NAME, PARTITION_GRANULARITY_TABLE)
    );
    let cols = conn.query_columns(&sql);
    let ids = ids_column(&cols);

    assert_eq!(
        ids.len(),
        PARTITION_GRANULARITY_REMAINING_ROWS,
        "mor_pos_partition must return {PARTITION_GRANULARITY_REMAINING_ROWS} rows \
         post-delete, got {}: {ids:?}",
        ids.len()
    );

    let deleted: HashSet<i64> = PARTITION_GRANULARITY_DELETED_IDS.iter().copied().collect();
    let expected: Vec<i64> = (1..=PARTITION_GRANULARITY_TOTAL_ROWS as i64)
        .filter(|id| !deleted.contains(id))
        .collect();
    assert_eq!(
        ids, expected,
        "mor_pos_partition post-delete id set must be exactly {expected:?}, got {ids:?}"
    );
}

/// The multi-partition-spanning delete is applied correctly PER PARTITION:
/// querying each partition in isolation returns exactly that partition's
/// seeded rows minus its own recorded deleted rows — proving the "east"
/// delete file is not applied to "west" data files and vice versa.
#[test]
fn e2e_partition_delete_spans_multiple_partitions() {
    setup_e2e();
    let mut conn = exa_conn();

    let table = vs_table(VS_NAME, PARTITION_GRANULARITY_TABLE);

    let east_sql = format!("SELECT id FROM {table} WHERE {PARTITION_COL} = 'east' ORDER BY id");
    let east_ids = ids_column(&conn.query_columns(&east_sql));
    let east_deleted: HashSet<i64> = PARTITION_EAST_DELETED_IDS.iter().copied().collect();
    let expected_east: Vec<i64> = (1..=10i64)
        .filter(|id| !east_deleted.contains(id))
        .collect();
    assert_eq!(
        east_ids, expected_east,
        "east partition post-delete ids must be exactly {expected_east:?}, got {east_ids:?}"
    );
    for id in &east_ids {
        assert!(
            !east_deleted.contains(id),
            "deleted east id {id} must NOT appear in the east partition result"
        );
    }

    let west_sql = format!("SELECT id FROM {table} WHERE {PARTITION_COL} = 'west' ORDER BY id");
    let west_ids = ids_column(&conn.query_columns(&west_sql));
    let west_deleted: HashSet<i64> = PARTITION_WEST_DELETED_IDS.iter().copied().collect();
    let expected_west: Vec<i64> = (11..=20i64)
        .filter(|id| !west_deleted.contains(id))
        .collect();
    assert_eq!(
        west_ids, expected_west,
        "west partition post-delete ids must be exactly {expected_west:?}, got {west_ids:?}"
    );
    for id in &west_ids {
        assert!(
            !west_deleted.contains(id),
            "deleted west id {id} must NOT appear in the west partition result"
        );
    }

    assert_eq!(
        east_ids.len() + west_ids.len(),
        PARTITION_GRANULARITY_REMAINING_ROWS,
        "east + west partition results must together equal the total post-delete row count"
    );
}

// ---------------------------------------------------------------------------
// Fan-out invariance: post-delete result must not depend on shard placement
// ---------------------------------------------------------------------------

/// Deterministically forces both a same-shard and a different-shard
/// placement of `mor_pos_partition`'s affected data files (via
/// `PARALLELISM_FACTOR`, not hash-partitioning luck) and asserts the
/// post-delete result is identical either way.
///
/// The shard placement itself is proven directly against the production
/// `shard_count` + `partition_files_by_bytes` functions (the same ones the
/// running adapter uses) before the two VS queries even run:
/// - `PARALLELISM_FACTOR = 1` → `shard_count(1, 1, 4) == 1` → EVERY data file
///   (including any two files that share a partition-scoped delete file)
///   lands in the SAME single shard.
/// - `PARALLELISM_FACTOR = SPLIT_PARALLELISM_FACTOR (4)` →
///   `shard_count(1, 4, 4) == 4` → with as many shards as files,
///   `partition_files_by_bytes`'s greedy-lightest-shard assignment puts
///   EXACTLY one file per shard, so every data file lands in a DIFFERENT
///   shard from every other — including the two files that share a delete
///   file.
#[test]
fn e2e_partition_delete_invariant_across_fanout() {
    setup_e2e();

    // --- Prove the shard placement claim directly against production code ---
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");
    let files = rt.block_on(resolve_fixture_files(
        NAMESPACE,
        PARTITION_GRANULARITY_TABLE,
    ));
    assert_eq!(
        files.len(),
        4,
        "sanity: mor_pos_partition must have 4 data files"
    );

    let g_same = shard_count(1, 1, files.len());
    assert_eq!(
        g_same, 1,
        "PARALLELISM_FACTOR=1 on a 1-node cluster must yield G=1"
    );
    let same_shards = partition_files_by_bytes(files.clone(), g_same);
    assert_eq!(same_shards.len(), 1, "G=1 must yield exactly 1 shard");
    assert_eq!(
        same_shards[0].len(),
        files.len(),
        "with G=1 every data file (including any two sharing a delete file) \
         must land in the SAME single shard"
    );

    let g_split = shard_count(1, SPLIT_PARALLELISM_FACTOR, files.len());
    assert_eq!(
        g_split,
        files.len(),
        "PARALLELISM_FACTOR={SPLIT_PARALLELISM_FACTOR} on a 1-node cluster with \
         {} files must yield G == file_count",
        files.len()
    );
    let split_shards = partition_files_by_bytes(files.clone(), g_split);
    assert_eq!(
        split_shards.len(),
        files.len(),
        "G == file_count must yield exactly file_count shards"
    );
    for (i, shard) in split_shards.iter().enumerate() {
        assert_eq!(
            shard.len(),
            1,
            "with G == file_count every shard must get exactly 1 file \
             (shard {i} got {}), so every data file lands in a DIFFERENT shard",
            shard.len()
        );
    }

    // --- Run the actual query under both forced placements ---
    let deleted: HashSet<i64> = PARTITION_GRANULARITY_DELETED_IDS.iter().copied().collect();
    let expected: Vec<i64> = (1..=PARTITION_GRANULARITY_TOTAL_ROWS as i64)
        .filter(|id| !deleted.contains(id))
        .collect();

    let mut conn = exa_conn();

    let same_sql = format!(
        "SELECT id FROM {} ORDER BY id",
        vs_table(SAMESHARD_VS_NAME, PARTITION_GRANULARITY_TABLE)
    );
    let same_ids = ids_column(&conn.query_columns(&same_sql));
    assert_eq!(
        same_ids, expected,
        "same-shard placement (PARALLELISM_FACTOR=1) must return the exact \
         post-delete id set {expected:?}, got {same_ids:?}"
    );

    let split_sql = format!(
        "SELECT id FROM {} ORDER BY id",
        vs_table(SPLITSHARD_VS_NAME, PARTITION_GRANULARITY_TABLE)
    );
    let split_ids = ids_column(&conn.query_columns(&split_sql));
    assert_eq!(
        split_ids, expected,
        "split-shard placement (PARALLELISM_FACTOR={SPLIT_PARALLELISM_FACTOR}) must \
         return the exact post-delete id set {expected:?}, got {split_ids:?}"
    );

    assert_eq!(
        same_ids, split_ids,
        "post-delete result must be invariant to shard placement: \
         same-shard={same_ids:?} vs split-shard={split_ids:?}"
    );
}

// ---------------------------------------------------------------------------
// Composition with pushdown + aggregation
// ---------------------------------------------------------------------------

/// Deletes compose with projection (drops `region`), a WHERE filter
/// (`region = 'west'`), and a LIMIT: the returned rows equal the same
/// projection/filter/LIMIT evaluated over the post-delete data.
///
/// West partition post-delete ids: 11,12,15,16,18,20 (west deleted:
/// 13,14,17,19). `ORDER BY id LIMIT 3` → 11,12,15 → vals row-11,row-12,row-15.
#[test]
fn e2e_deletes_with_projection_filter_limit() {
    setup_e2e();
    let mut conn = exa_conn();

    let table = vs_table(VS_NAME, PARTITION_GRANULARITY_TABLE);
    let sql =
        format!("SELECT id, val FROM {table} WHERE {PARTITION_COL} = 'west' ORDER BY id LIMIT 3");
    let cols = conn.query_columns(&sql);
    assert_eq!(cols.len(), 2, "expected 2 columns (id, val): {cols:?}");

    let ids = ids_column(&cols);
    assert_eq!(
        ids,
        vec![11, 12, 15],
        "expected west ids [11,12,15], got {ids:?}"
    );

    let vals: Vec<String> = cols[1]
        .iter()
        .map(|v| {
            v.as_str()
                .unwrap_or_else(|| panic!("val not a string: {v:?}"))
                .to_string()
        })
        .collect();
    assert_eq!(
        vals,
        vec![
            "row-11".to_string(),
            "row-12".to_string(),
            "row-15".to_string()
        ],
        "expected vals [row-11,row-12,row-15], got {vals:?}"
    );

    let west_deleted: HashSet<i64> = PARTITION_WEST_DELETED_IDS.iter().copied().collect();
    for id in &ids {
        assert!(
            !west_deleted.contains(id),
            "deleted west id {id} must NOT appear in the projection/filter/limit result"
        );
    }
}

/// Deletes compose with a single-group aggregate: `COUNT(*)`/`SUM(id)` over
/// the file-granularity table equal the same aggregates over the post-delete
/// data (count=16, sum = Σ(1..20) - Σ{3,8,13,17} = 210 - 41 = 169).
///
/// Deletes also compose with a GROUP BY aggregate: grouping the
/// partition-granularity table by `region` yields 6 remaining rows in each
/// of "east" and "west" (12 total), never counting a deleted row.
#[test]
fn e2e_deletes_with_single_and_grouped_agg() {
    setup_e2e();
    let mut conn = exa_conn();

    // --- single-group aggregate over mor_pos_file ---
    let single_sql = format!(
        "SELECT COUNT(*), SUM(id) FROM {}",
        vs_table(VS_NAME, FILE_GRANULARITY_TABLE)
    );
    let single_cols = conn.query_columns(&single_sql);
    let count = parse_int(&single_cols[0][0]);
    let sum = parse_int(&single_cols[1][0]);
    assert_eq!(
        count, FILE_GRANULARITY_REMAINING_ROWS as i64,
        "COUNT(*) over mor_pos_file must be {FILE_GRANULARITY_REMAINING_ROWS}, got {count}"
    );
    let total: i64 = (1..=FILE_GRANULARITY_TOTAL_ROWS as i64).sum();
    let deleted_sum: i64 = FILE_GRANULARITY_DELETED_IDS.iter().sum();
    let expected_sum = total - deleted_sum;
    assert_eq!(
        sum, expected_sum,
        "SUM(id) over mor_pos_file must be {expected_sum} (={total}-{deleted_sum}), got {sum}"
    );

    // --- grouped aggregate over mor_pos_partition ---
    let grouped_sql = format!(
        "SELECT {PARTITION_COL}, COUNT(*) FROM {} GROUP BY {PARTITION_COL} ORDER BY {PARTITION_COL}",
        vs_table(VS_NAME, PARTITION_GRANULARITY_TABLE)
    );
    let grouped_cols = conn.query_columns(&grouped_sql);
    assert_eq!(
        grouped_cols[0].len(),
        2,
        "grouped aggregate must return 2 groups (east, west): {grouped_cols:?}"
    );

    let regions: Vec<String> = grouped_cols[0]
        .iter()
        .map(|v| {
            v.as_str()
                .unwrap_or_else(|| panic!("region not a string: {v:?}"))
                .to_string()
        })
        .collect();
    assert_eq!(
        regions,
        vec!["east".to_string(), "west".to_string()],
        "expected groups [east,west] in order, got {regions:?}"
    );

    let counts: Vec<i64> = grouped_cols[1].iter().map(parse_int).collect();
    assert_eq!(
        counts,
        vec![6, 6],
        "east and west must each have 6 post-delete rows, got {counts:?}"
    );
    let grouped_total: i64 = counts.iter().sum();
    assert_eq!(
        grouped_total, PARTITION_GRANULARITY_REMAINING_ROWS as i64,
        "grouped total must equal {PARTITION_GRANULARITY_REMAINING_ROWS}, got {grouped_total}"
    );
}

// ---------------------------------------------------------------------------
// Unsupported delete mechanism — fail loud at plan time
// ---------------------------------------------------------------------------

/// End-to-end: a query over a table whose snapshot carries an unsupported
/// delete mechanism (equality delete or Puffin/v3 deletion vector) MUST fail
/// at plan time with a clean error naming the mechanism, and MUST NOT return
/// any rows or leak credentials.
///
/// Targets `mor_dv_unsupported` (`DELETION_VECTOR_TABLE`), a
/// `format-version=3` merge-on-read table whose Spark `DELETE FROM` commits a
/// Puffin deletion vector instead of a Parquet positional-delete file (see
/// `scripts/spark-fixtures/create_deletion_vector_fixture.sql`). The
/// EqualityDelete arm of `UnsupportedDeleteMechanism` remains untested at the
/// E2E level — only Flink's row-level upsert connectors write equality
/// deletes, and Flink is not part of this stack
/// (`scripts/spark-fixtures/run_fixtures.sh`'s header) — but shares the same
/// plan-time gate (`classify_manifest_file` in `adapter/pushdown.rs`) as the
/// DeletionVector arm exercised here, and is covered directly by that
/// function's unit tests.
///
/// UPSTREAM TRACKING (apache/iceberg-rust#2681, #2580, #2411): once
/// iceberg-rust gains v3 deletion-vector READ support, `mor_dv_unsupported`
/// becomes readable rather than rejected, and this test will need a
/// genuinely unsupported fixture in its place (or retirement) plus a new
/// positive-path DV read test.
#[test]
fn e2e_unsupported_delete_fails_loud() {
    setup_e2e();
    let mut conn = exa_conn();

    let sql = format!(
        "SELECT * FROM {} LIMIT 1",
        vs_table(VS_NAME, DELETION_VECTOR_TABLE)
    );
    let resp = conn.try_execute(&sql);

    assert_eq!(
        resp["status"].as_str(),
        Some("error"),
        "query over an unsupported-delete table must fail, got: {resp}"
    );

    let message = resp["exception"]["text"]
        .as_str()
        .or_else(|| resp["message"].as_str())
        .unwrap_or_default()
        .to_ascii_lowercase();
    assert!(
        message.contains("equality")
            || message.contains("deletion vector")
            || message.contains("puffin"),
        "error must name the unsupported delete mechanism (equality delete or \
         deletion vector/Puffin), got: {resp}"
    );

    for secret_marker in [
        "access_key",
        "secret_key",
        "session_token",
        "secretaccesskey",
    ] {
        assert!(
            !message.contains(secret_marker),
            "error must not leak credentials (found '{secret_marker}'): {resp}"
        );
    }

    assert_eq!(
        resp["responseData"]["results"][0]["resultSet"]["numRows"].as_i64(),
        None,
        "a plan-time failure must not carry a result set: {resp}"
    );
}

// ---------------------------------------------------------------------------
// Delete-free non-regression
// ---------------------------------------------------------------------------

/// A delete-free table's existing projection/filter/LIMIT and aggregate
/// queries return the same results as before this feature, confirming the
/// unified `ParquetSource`-backed provider does not regress the
/// no-deletes path.
#[test]
fn e2e_delete_free_table_no_regression() {
    setup_e2e();
    let mut conn = exa_conn();

    let table = vs_table(VS_NAME, E2E_TABLE);

    let filter_count = conn.query_row_count(&format!("SELECT id FROM {table} WHERE score > 15.0"));
    assert_eq!(
        filter_count, SEED_ROWS_SCORE_GT_15 as i64,
        "delete-free filter regression: expected {SEED_ROWS_SCORE_GT_15} rows with \
         score > 15.0, got {filter_count}"
    );

    let limit_count = conn.query_row_count(&format!("SELECT id FROM {table} LIMIT 5"));
    assert_eq!(
        limit_count, 5,
        "delete-free LIMIT regression: expected 5 rows, got {limit_count}"
    );

    let agg_sql = format!("SELECT COUNT(*), SUM(score) FROM {table}");
    let agg_cols = conn.query_columns(&agg_sql);
    let total_count = parse_int(&agg_cols[0][0]);
    assert_eq!(
        total_count, SEED_TOTAL_ROWS as i64,
        "delete-free aggregate regression: expected COUNT(*)={SEED_TOTAL_ROWS}, got {total_count}"
    );
    let total_score = agg_cols[1][0]
        .as_f64()
        .or_else(|| agg_cols[1][0].as_str().and_then(|s| s.parse().ok()))
        .unwrap_or_else(|| panic!("SUM(score) not numeric: {:?}", agg_cols[1][0]));
    // scores are 5.0 * id for id=1..=SEED_TOTAL_ROWS → sum = 5.0 * Σ(1..=SEED_TOTAL_ROWS).
    let n = SEED_TOTAL_ROWS as f64;
    let expected_total_score = 5.0 * (n * (n + 1.0) / 2.0);
    assert!(
        (total_score - expected_total_score).abs() < 1e-6,
        "delete-free aggregate regression: expected SUM(score)≈{expected_total_score}, \
         got {total_score}"
    );
}

// ---------------------------------------------------------------------------
// Stack-unavailable contract
// ---------------------------------------------------------------------------

/// The positional-delete suite FAILS (never skips) when the stack is
/// unavailable — same contract as `e2e_fails_when_stack_unavailable` in
/// `e2e_scan_test.rs`: every test above starts with `setup_e2e()`, whose
/// `wait_for_exasol`/`wait_for_minio`/`wait_for_iceberg_catalog` calls panic
/// (never return an `Err` to swallow) on a dependency that never comes up.
/// This test documents that contract by verifying the underlying connect
/// helper panics on an unreachable host rather than returning `Ok`.
#[test]
fn positional_delete_suite_fails_when_stack_unavailable() {
    let result = std::panic::catch_unwind(|| ExaConn::connect("192.0.2.1", 8563, "sys", "exasol"));
    assert!(
        result.is_err(),
        "ExaConn::connect to an unreachable host must panic, not return Ok"
    );
}
