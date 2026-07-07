//! End-to-end deletion-vector matrix for the lakehouse-engine Virtual Schema.
//!
//! Drives Iceberg format-version=3 deletion-vector tables (and a mixed
//! positional-delete/deletion-vector table) through the full VS → adapter →
//! scan UDF → DataFusion stack against the local Exasol Docker + Apache
//! Spark fixtures (`packaging/e2e-harness-deletion-vectors`,
//! `packaging/deletion-vector-fixtures`). Mirrors
//! `e2e_positional_deletes_test.rs`'s structure and conventions exactly.
//!
//! The fixture tables (`mor_dv`, `mor_mixed`) are authored ONCE by the
//! `spark-iceberg-fixtures` one-shot Compose job at stack bring-up (see
//! `scripts/spark-fixtures/`) — this file never seeds them itself. Ground
//! truth lives in `tests/common/pos_delete_fixtures.rs` and MUST stay in
//! lockstep with the Spark SQL scripts that produce them.
//!
//! The still-unsupported-mechanism fail-loud path (equality deletes,
//! ORC/Avro) is covered by `e2e_positional_deletes_test.rs`, not here.
//!
//! All tests FAIL (never skip) when the stack is unavailable — per project
//! rules — because every test starts with `setup_e2e()`, which panics (via
//! `wait_for_exasol`/`wait_for_minio`/`wait_for_iceberg_catalog`) rather than
//! returning an `Err` when a dependency is down.
#![cfg(feature = "exasol-e2e")]

mod common;
use common::exasol_ws::ExaConn;
use common::pos_delete_fixtures::{
    DELETION_VECTOR_DELETED_IDS, DELETION_VECTOR_REMAINING_ROWS, DELETION_VECTOR_TABLE,
    DELETION_VECTOR_TOTAL_ROWS, MIXED_MECHANISM_DELETED_IDS, MIXED_MECHANISM_DV_DELETED_IDS,
    MIXED_MECHANISM_POS_DELETED_IDS, MIXED_MECHANISM_REMAINING_ROWS, MIXED_MECHANISM_TABLE,
    MIXED_MECHANISM_TOTAL_ROWS, NAMESPACE,
};
use common::stack::{
    bucketfs_port, bucketfs_write_password, build_create_connection_sql, exasol_host,
    exasol_sql_port, iceberg_catalog_url, iceberg_catalog_url_internal, lakehouse_engine_so_path,
    local_stack_connection_password, minio_url, upload_to_bucketfs, wait_for_exasol,
    wait_for_iceberg_catalog, wait_for_minio,
};

use lakehouse_engine::adapter::connection::ConnectionCreds;
use lakehouse_engine::adapter::pushdown::{resolve_file_list, shard_count};
use lakehouse_engine::adapter::sharding::partition_files_by_bytes;
use lakehouse_engine::scan::spec::{CatalogProps, DeleteFormat, DeleteType, StorageProps};

use std::collections::HashSet;
use std::sync::OnceLock;
use std::time::Duration;

// ---------------------------------------------------------------------------
// Constants (schema/script names mirror e2e_positional_deletes_test.rs —
// same .so, same idempotent CREATE OR REPLACE objects, shared across every
// E2E test binary).
// ---------------------------------------------------------------------------

const SYS_PASSWORD: &str = "exasol";
const SCHEMA_NAME: &str = "LHVS";
/// Shared VS used by every other E2E test binary too (idempotent CREATE OR
/// REPLACE with an identical body, so concurrent recreation is harmless).
const VS_NAME: &str = "MY_LAKEHOUSE";
/// Dedicated VS forcing a single work-unit shard (`PARALLELISM_FACTOR = 1`),
/// used only by the mixed-mechanism fan-out-invariance test. Named distinctly
/// from `e2e_positional_deletes_test.rs`'s own dedicated VS objects so the
/// two binaries never race over the same Exasol object.
const MIXED_SAMESHARD_VS_NAME: &str = "DVMIX_SAMESHARD_VS";
/// Dedicated VS forcing one shard per data file
/// (`PARALLELISM_FACTOR = MIXED_SPLIT_PARALLELISM_FACTOR`, chosen to equal
/// `mor_mixed`'s data-file count of 2), used only by the fan-out-invariance
/// test.
const MIXED_SPLITSHARD_VS_NAME: &str = "DVMIX_SPLITSHARD_VS";
/// `mor_mixed` has 2 data files (one positional-delete-backed, one
/// deletion-vector-backed); setting `PARALLELISM_FACTOR` to this value on a
/// 1-node cluster makes `shard_count(1, 2, 2) == 2`, so every shard gets
/// exactly one file (see `partition_files_by_bytes`'s greedy-lightest-shard
/// assignment: with as many shards as files, each shard is filled exactly
/// once) — a deterministic split-shard placement, not a hash-partitioning
/// gamble.
const MIXED_SPLIT_PARALLELISM_FACTOR: usize = 2;
const ADAPTER_SCRIPT_NAME: &str = "LAKEHOUSE_ADAPTER";
const SCAN_SCRIPT_NAME: &str = "LAKEHOUSE_SCAN";
const MERGE_SCRIPT_NAME: &str = "LAKEHOUSE_DISTINCT_MERGE_COUNT";
const SO_BUCKETFS_PUT_PATH: &str = "/default/udf/liblakehouse_engine.so";
const SO_UDF_OBJECT_PATH: &str = "buckets/bfsdefault/default/udf/liblakehouse_engine.so";
const SLC_BUCKETFS_PUT_PATH: &str = "/default/slc/lakehouse-rustslc.tar.gz";
const SLC_VERSION: &str = "0.20.2";
const LANG_ALIAS: &str = "RUST";
const CATALOG_CONN_NAME: &str = "LAKEHOUSE_CATALOG_CREDS";

// ---------------------------------------------------------------------------
// One-time setup (idempotent; identical shape to the other E2E test binaries)
// ---------------------------------------------------------------------------

static SETUP_DONE: OnceLock<()> = OnceLock::new();

fn setup_e2e() {
    SETUP_DONE.get_or_init(|| {
        wait_for_exasol();
        wait_for_minio();
        wait_for_iceberg_catalog();

        // mor_dv / mor_mixed are authored by the spark-iceberg-fixtures
        // Compose job, not by this harness — nothing to seed here.
        install_slc();

        let so_path = lakehouse_engine_so_path();
        upload_to_bucketfs(&so_path, SO_BUCKETFS_PUT_PATH);

        let mut conn = exa_conn();
        create_schema_and_scripts(&mut conn);
        create_virtual_schema(&mut conn, VS_NAME, None);
        create_virtual_schema(&mut conn, MIXED_SAMESHARD_VS_NAME, Some(1));
        create_virtual_schema(
            &mut conn,
            MIXED_SPLITSHARD_VS_NAME,
            Some(MIXED_SPLIT_PARALLELISM_FACTOR),
        );
    });
}

fn install_slc() {
    let slc_url = format!(
        "https://github.com/exasol-labs/language-container-rs/releases/download/v{SLC_VERSION}/lc-rust-{SLC_VERSION}.tar.gz"
    );
    let tarball_bytes = reqwest::blocking::get(&slc_url)
        .unwrap_or_else(|e| panic!("download SLC {SLC_VERSION} from {slc_url}: {e}"))
        .bytes()
        .unwrap_or_else(|e| panic!("read SLC tarball bytes: {e}"));
    assert!(
        !tarball_bytes.is_empty(),
        "SLC tarball is empty — download failed"
    );

    let password = bucketfs_write_password();
    let bfs_url = format!(
        "https://{}:{}{}",
        exasol_host(),
        bucketfs_port(),
        SLC_BUCKETFS_PUT_PATH
    );
    let client = reqwest::blocking::Client::builder()
        .danger_accept_invalid_certs(true)
        .timeout(Duration::from_secs(120))
        .build()
        .expect("BucketFS client");
    let resp = client
        .put(&bfs_url)
        .basic_auth("w", Some(&password))
        .body(tarball_bytes.to_vec())
        .send()
        .unwrap_or_else(|e| panic!("BucketFS PUT SLC to {bfs_url}: {e}"));
    assert!(
        resp.status().is_success(),
        "BucketFS PUT SLC returned {} — expected 2xx",
        resp.status()
    );

    let mut conn = exa_conn();
    let rust_def = format!(
        "{LANG_ALIAS}=localzmq+protobuf:///bfsdefault/default/slc/lakehouse-rustslc?lang=rust#buckets/bfsdefault/default/slc/lakehouse-rustslc/exaudf/exaudfclient"
    );
    let current = conn.query_columns(
        "SELECT SYSTEM_VALUE FROM EXA_PARAMETERS WHERE PARAMETER_NAME='SCRIPT_LANGUAGES'",
    );
    let current_val = current
        .first()
        .and_then(|col| col.first())
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    let preserved = current_val
        .split_whitespace()
        .filter(|s| !s.starts_with(&format!("{LANG_ALIAS}=")))
        .collect::<Vec<_>>()
        .join(" ");
    let new_val = format!("{preserved} {rust_def}");
    conn.execute(&format!(
        "ALTER SYSTEM SET SCRIPT_LANGUAGES = '{}'",
        new_val.trim()
    ));
}

fn exa_conn() -> ExaConn {
    ExaConn::connect(&exasol_host(), exasol_sql_port(), "sys", SYS_PASSWORD)
}

fn create_schema_and_scripts(conn: &mut ExaConn) {
    conn.execute(&format!("CREATE SCHEMA IF NOT EXISTS {SCHEMA_NAME}"));
    conn.execute(&format!(
        r#"CREATE OR REPLACE {LANG_ALIAS} ADAPTER SCRIPT {SCHEMA_NAME}.{ADAPTER_SCRIPT_NAME} AS
%udf_object {SO_UDF_OBJECT_PATH}
/"#
    ));
    conn.execute(&format!(
        r#"CREATE OR REPLACE {LANG_ALIAS} SET SCRIPT {SCHEMA_NAME}.{SCAN_SCRIPT_NAME}(common VARCHAR(2000000), files VARCHAR(2000000))
EMITS (...) AS
%udf_object {SO_UDF_OBJECT_PATH}
/"#
    ));
    conn.execute(&format!(
        r#"CREATE OR REPLACE {LANG_ALIAS} SCALAR SCRIPT {SCHEMA_NAME}.{MERGE_SCRIPT_NAME}(partials VARCHAR(2000000))
RETURNS DECIMAL(20,0) AS
%udf_object {SO_UDF_OBJECT_PATH}
/"#
    ));
}

/// Create (or replace) a Virtual Schema over the shared `e2e_lakehouse`
/// namespace. `parallelism_factor`, when set, is emitted as the
/// `PARALLELISM_FACTOR` VS property (see `adapter::mod::resolve_parallelism_factor`);
/// when `None` the adapter's hardware-derived default applies.
fn create_virtual_schema(conn: &mut ExaConn, vs_name: &str, parallelism_factor: Option<usize>) {
    let password = local_stack_connection_password();
    let catalog_uri = iceberg_catalog_url_internal();
    let create_conn_sql = build_create_connection_sql(CATALOG_CONN_NAME, &catalog_uri, &password);
    conn.execute(&create_conn_sql);

    let _ = conn.try_execute(&format!("DROP VIRTUAL SCHEMA IF EXISTS {vs_name} CASCADE"));

    let parallelism_clause = parallelism_factor
        .map(|f| format!("\n  PARALLELISM_FACTOR  = '{f}'"))
        .unwrap_or_default();
    conn.execute(&format!(
        r#"CREATE VIRTUAL SCHEMA {vs_name}
USING {SCHEMA_NAME}.{ADAPTER_SCRIPT_NAME} WITH
  CATALOG_CONNECTION  = '{CATALOG_CONN_NAME}'
  ICEBERG_NAMESPACE   = '{NAMESPACE}'
  SCAN_SCHEMA         = '{SCHEMA_NAME}'
  ALLOW_HTTP          = 'true'{parallelism_clause}"#
    ));
}

fn vs_table(vs_name: &str, table: &str) -> String {
    format!("{vs_name}.{}", table.to_uppercase())
}

// ---------------------------------------------------------------------------
// Adapter-level helpers for direct catalog inspection (fixture-shape tests +
// the fan-out-invariance shard-placement proof) — mirror
// e2e_positional_deletes_test.rs's identical helpers.
// ---------------------------------------------------------------------------

fn local_stack_creds() -> ConnectionCreds {
    ConnectionCreds {
        warehouse: "s3://warehouse/".to_string(),
        endpoint: minio_url(),
        region: "us-east-1".to_string(),
        access_key: "minioadmin".to_string(),
        secret_key: "minioadmin".to_string(),
        session_token: None,
        path_style: true,
        use_sigv4: false,
        use_vended_credentials: false,
        token: None,
        client_id: None,
        client_secret: None,
        oauth2_server_uri: None,
        scope: None,
    }
}

fn local_stack_storage() -> StorageProps {
    StorageProps {
        endpoint: minio_url(),
        region: "us-east-1".to_string(),
        access_key: "minioadmin".to_string(),
        secret_key: "minioadmin".to_string(),
        session_token: None,
        allow_http: true,
        path_style: true,
    }
}

fn local_stack_catalog(table: &str) -> CatalogProps {
    CatalogProps {
        uri: iceberg_catalog_url(),
        warehouse: "s3://warehouse/".to_string(),
        table: table.to_string(),
    }
}

fn resolve_fixture_files(table: &str) -> Vec<lakehouse_engine::scan::spec::FileEntry> {
    let catalog_uri = iceberg_catalog_url();
    let catalog_props = local_stack_catalog(&format!("{NAMESPACE}.{table}"));
    let storage = local_stack_storage();
    let creds = local_stack_creds();

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime for resolve_file_list");
    let (files, ..) = rt
        .block_on(async {
            resolve_file_list(&catalog_uri, &catalog_props, &storage, &creds, None).await
        })
        .unwrap_or_else(|e| panic!("resolve_file_list({table}) must succeed: {e}"));
    files
}

// ---------------------------------------------------------------------------
// Shared numeric parsers (small dup of the pattern in e2e_positional_deletes_test.rs)
// ---------------------------------------------------------------------------

fn parse_int(v: &serde_json::Value) -> i64 {
    v.as_i64()
        .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
        .unwrap_or_else(|| panic!("expected integer value, got: {v:?}"))
}

fn ids_column(cols: &[Vec<serde_json::Value>]) -> Vec<i64> {
    cols[0].iter().map(parse_int).collect()
}

// ---------------------------------------------------------------------------
// Fixture-shape tests (packaging/deletion-vector-fixtures) — inspect the
// Spark-committed Iceberg manifests directly via resolve_file_list, bypassing
// Exasol, to verify the fixtures actually have the delete-file shape the
// e2e_* correctness tests below assume.
// ---------------------------------------------------------------------------

/// Spark's `mor_dv` fixture commits exactly one Puffin `deletion-vector-v1`
/// blob referencing its single data file (not a Parquet positional-delete
/// file): one data file, one delete ref, type `DV` / format `PUFFIN`, with
/// blob `offset`/`length` present.
#[test]
fn fixture_spark_deletion_vector_table() {
    setup_e2e();

    let files = resolve_fixture_files(DELETION_VECTOR_TABLE);
    assert_eq!(
        files.len(),
        1,
        "mor_dv must resolve exactly 1 data file, got {}: {files:?}",
        files.len()
    );

    let entry = &files[0];
    assert_eq!(
        entry.deletes.len(),
        1,
        "mor_dv's data file must have exactly 1 delete ref, got {}",
        entry.deletes.len()
    );
    let delete = &entry.deletes[0];
    assert_eq!(
        delete.delete_type,
        DeleteType::Dv,
        "mor_dv's delete ref must be a deletion vector, got {:?}",
        delete.delete_type
    );
    assert_eq!(
        delete.format,
        DeleteFormat::Puffin,
        "mor_dv's delete ref must be Puffin-formatted, got {:?}",
        delete.format
    );
    assert!(
        delete.offset.is_some() && delete.length.is_some(),
        "a deletion-vector ref must carry blob offset/length, got {delete:?}"
    );
}

/// Spark's `mor_mixed` fixture commits TWO data files: one resolved via a
/// Parquet positional-delete file (written while the table was still
/// format-version=2) and one resolved via a v3 Puffin deletion vector
/// (written after the upgrade to format-version=3) — the v2→v3 migration
/// shape.
#[test]
fn fixture_spark_mixed_mechanism_table() {
    setup_e2e();

    let files = resolve_fixture_files(MIXED_MECHANISM_TABLE);
    assert_eq!(
        files.len(),
        2,
        "mor_mixed must resolve exactly 2 data files, got {}: {files:?}",
        files.len()
    );

    let mut pos_del_files = 0;
    let mut dv_files = 0;
    for entry in &files {
        assert_eq!(
            entry.deletes.len(),
            1,
            "mor_mixed data file {} must have exactly 1 delete ref, got {}",
            entry.path,
            entry.deletes.len()
        );
        let delete = &entry.deletes[0];
        match delete.delete_type {
            DeleteType::PosDel => {
                assert_eq!(
                    delete.format,
                    DeleteFormat::Parquet,
                    "positional-delete ref must be Parquet-formatted, got {:?}",
                    delete.format
                );
                assert!(
                    delete.offset.is_none() && delete.length.is_none(),
                    "a whole-file positional delete must not carry blob offset/length, got {delete:?}"
                );
                pos_del_files += 1;
            }
            DeleteType::Dv => {
                assert_eq!(
                    delete.format,
                    DeleteFormat::Puffin,
                    "deletion-vector ref must be Puffin-formatted, got {:?}",
                    delete.format
                );
                assert!(
                    delete.offset.is_some() && delete.length.is_some(),
                    "a deletion-vector ref must carry blob offset/length, got {delete:?}"
                );
                dv_files += 1;
            }
            other => panic!("mor_mixed must only use PosDel/Dv mechanisms, got {other:?}"),
        }
    }
    assert_eq!(
        pos_del_files, 1,
        "mor_mixed must have exactly 1 positional-delete-backed data file"
    );
    assert_eq!(
        dv_files, 1,
        "mor_mixed must have exactly 1 deletion-vector-backed data file"
    );
}

/// The fixture ground truth in `pos_delete_fixtures.rs` is internally
/// consistent with itself — a pure, no-stack check that catches a constants
/// edit that drifts out of lockstep (e.g. an updated total/deleted-id list
/// whose remaining-row count wasn't recomputed).
#[test]
fn fixture_ground_truth_lockstep() {
    assert_eq!(
        DELETION_VECTOR_REMAINING_ROWS,
        DELETION_VECTOR_TOTAL_ROWS - DELETION_VECTOR_DELETED_IDS.len(),
        "mor_dv remaining-row count must equal total minus deleted ids"
    );

    let pos: HashSet<i64> = MIXED_MECHANISM_POS_DELETED_IDS.iter().copied().collect();
    let dv: HashSet<i64> = MIXED_MECHANISM_DV_DELETED_IDS.iter().copied().collect();
    assert!(
        pos.is_disjoint(&dv),
        "mor_mixed's positional-delete and deletion-vector id sets must not overlap: \
         pos={pos:?} dv={dv:?}"
    );

    let mut combined: Vec<i64> = pos.union(&dv).copied().collect();
    combined.sort_unstable();
    let mut expected: Vec<i64> = MIXED_MECHANISM_DELETED_IDS.to_vec();
    expected.sort_unstable();
    assert_eq!(
        combined, expected,
        "MIXED_MECHANISM_DELETED_IDS must be exactly the union of the pos and DV id sets"
    );

    assert_eq!(
        MIXED_MECHANISM_REMAINING_ROWS,
        MIXED_MECHANISM_TOTAL_ROWS - MIXED_MECHANISM_DELETED_IDS.len(),
        "mor_mixed remaining-row count must equal total minus deleted ids"
    );

    // Sanity: the positional delete targets file A's id range and the DV
    // targets file B's, per create_mixed_mechanism_fixture.sql's split.
    let file_a_max_id = (MIXED_MECHANISM_TOTAL_ROWS / 2) as i64;
    for id in &pos {
        assert!(
            *id <= file_a_max_id,
            "positional-delete id {id} must fall in file A's range (<= {file_a_max_id})"
        );
    }
    for id in &dv {
        assert!(
            *id > file_a_max_id,
            "deletion-vector id {id} must fall in file B's range (> {file_a_max_id})"
        );
    }
}

// ---------------------------------------------------------------------------
// End-to-end correctness: deletion-vector-only table
// ---------------------------------------------------------------------------

/// A `SELECT` over the deletion-vector table returns exactly the seeded rows
/// minus the recorded deleted rows, with no deleted id present.
#[test]
fn e2e_dv_returns_post_delete_rows() {
    setup_e2e();
    let mut conn = exa_conn();

    let sql = format!(
        "SELECT id FROM {} ORDER BY id",
        vs_table(VS_NAME, DELETION_VECTOR_TABLE)
    );
    let cols = conn.query_columns(&sql);
    let ids = ids_column(&cols);

    assert_eq!(
        ids.len(),
        DELETION_VECTOR_REMAINING_ROWS,
        "mor_dv must return {DELETION_VECTOR_REMAINING_ROWS} rows post-delete, got {}: {ids:?}",
        ids.len()
    );

    let deleted: HashSet<i64> = DELETION_VECTOR_DELETED_IDS.iter().copied().collect();
    let expected: Vec<i64> = (1..=DELETION_VECTOR_TOTAL_ROWS as i64)
        .filter(|id| !deleted.contains(id))
        .collect();
    assert_eq!(
        ids, expected,
        "mor_dv post-delete id set must be exactly {expected:?}, got {ids:?}"
    );

    for id in &ids {
        assert!(
            !deleted.contains(id),
            "deleted id {id} must NOT appear in the post-delete result"
        );
    }
}

/// Deletion vectors compose with projection (drops `val`), a WHERE filter
/// (`id > 4`), and a LIMIT: the returned rows equal the same
/// projection/filter/LIMIT evaluated over the post-delete data.
///
/// mor_dv post-delete ids: 1,2,4,5,6,8,9,10. `WHERE id > 4` -> 5,6,8,9,10.
/// `ORDER BY id LIMIT 3` -> 5,6,8 -> vals row-05,row-06,row-08.
#[test]
fn e2e_dv_with_projection_filter_limit() {
    setup_e2e();
    let mut conn = exa_conn();

    let table = vs_table(VS_NAME, DELETION_VECTOR_TABLE);
    let sql = format!("SELECT id, val FROM {table} WHERE id > 4 ORDER BY id LIMIT 3");
    let cols = conn.query_columns(&sql);
    assert_eq!(cols.len(), 2, "expected 2 columns (id, val): {cols:?}");

    let ids = ids_column(&cols);
    assert_eq!(ids, vec![5, 6, 8], "expected ids [5,6,8], got {ids:?}");

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
            "row-05".to_string(),
            "row-06".to_string(),
            "row-08".to_string()
        ],
        "expected vals [row-05,row-06,row-08], got {vals:?}"
    );

    let deleted: HashSet<i64> = DELETION_VECTOR_DELETED_IDS.iter().copied().collect();
    for id in &ids {
        assert!(
            !deleted.contains(id),
            "deleted id {id} must NOT appear in the projection/filter/limit result"
        );
    }
}

/// Deletion vectors compose with a single-group aggregate: `COUNT(*)`/`SUM(id)`
/// over `mor_dv` equal the same aggregates over the post-delete data
/// (count=8, sum = Σ(1..10) - Σ{3,7} = 55 - 10 = 45).
///
/// They also compose with a GROUP BY aggregate: grouping by `val` yields one
/// group per surviving row (8 groups, each count=1), never counting a
/// deleted row's group.
#[test]
fn e2e_dv_with_single_and_grouped_agg() {
    setup_e2e();
    let mut conn = exa_conn();

    let table = vs_table(VS_NAME, DELETION_VECTOR_TABLE);

    // --- single-group aggregate ---
    let single_sql = format!("SELECT COUNT(*), SUM(id) FROM {table}");
    let single_cols = conn.query_columns(&single_sql);
    let count = parse_int(&single_cols[0][0]);
    let sum = parse_int(&single_cols[1][0]);
    assert_eq!(
        count, DELETION_VECTOR_REMAINING_ROWS as i64,
        "COUNT(*) over mor_dv must be {DELETION_VECTOR_REMAINING_ROWS}, got {count}"
    );
    let total: i64 = (1..=DELETION_VECTOR_TOTAL_ROWS as i64).sum();
    let deleted_sum: i64 = DELETION_VECTOR_DELETED_IDS.iter().sum();
    let expected_sum = total - deleted_sum;
    assert_eq!(
        sum, expected_sum,
        "SUM(id) over mor_dv must be {expected_sum} (={total}-{deleted_sum}), got {sum}"
    );

    // --- grouped aggregate ---
    let grouped_sql = format!("SELECT val, COUNT(*) FROM {table} GROUP BY val ORDER BY val");
    let grouped_cols = conn.query_columns(&grouped_sql);
    assert_eq!(
        grouped_cols[0].len(),
        DELETION_VECTOR_REMAINING_ROWS,
        "grouped aggregate must return {DELETION_VECTOR_REMAINING_ROWS} groups (one per \
         surviving row): {grouped_cols:?}"
    );

    let deleted_vals: HashSet<String> = DELETION_VECTOR_DELETED_IDS
        .iter()
        .map(|id| format!("row-{id:02}"))
        .collect();
    let vals: Vec<String> = grouped_cols[0]
        .iter()
        .map(|v| {
            v.as_str()
                .unwrap_or_else(|| panic!("val not a string: {v:?}"))
                .to_string()
        })
        .collect();
    for val in &vals {
        assert!(
            !deleted_vals.contains(val),
            "deleted val {val} must NOT appear as a group"
        );
    }

    let counts: Vec<i64> = grouped_cols[1].iter().map(parse_int).collect();
    assert!(
        counts.iter().all(|c| *c == 1),
        "each surviving row's group must have count=1, got {counts:?}"
    );
    let grouped_total: i64 = counts.iter().sum();
    assert_eq!(
        grouped_total, DELETION_VECTOR_REMAINING_ROWS as i64,
        "grouped total must equal {DELETION_VECTOR_REMAINING_ROWS}, got {grouped_total}"
    );
}

// ---------------------------------------------------------------------------
// End-to-end correctness: mixed positional-delete + deletion-vector table
// ---------------------------------------------------------------------------

/// A `SELECT` over the mixed-mechanism table returns exactly the seeded rows
/// minus the recorded deleted rows across BOTH mechanisms, with no row
/// deleted by either the positional-delete file or the deletion vector
/// present in the result.
#[test]
fn e2e_mixed_returns_combined_post_delete() {
    setup_e2e();
    let mut conn = exa_conn();

    let sql = format!(
        "SELECT id FROM {} ORDER BY id",
        vs_table(VS_NAME, MIXED_MECHANISM_TABLE)
    );
    let cols = conn.query_columns(&sql);
    let ids = ids_column(&cols);

    assert_eq!(
        ids.len(),
        MIXED_MECHANISM_REMAINING_ROWS,
        "mor_mixed must return {MIXED_MECHANISM_REMAINING_ROWS} rows post-delete, \
         got {}: {ids:?}",
        ids.len()
    );

    let deleted: HashSet<i64> = MIXED_MECHANISM_DELETED_IDS.iter().copied().collect();
    let expected: Vec<i64> = (1..=MIXED_MECHANISM_TOTAL_ROWS as i64)
        .filter(|id| !deleted.contains(id))
        .collect();
    assert_eq!(
        ids, expected,
        "mor_mixed post-delete id set must be exactly {expected:?}, got {ids:?}"
    );

    for id in &ids {
        assert!(
            !deleted.contains(id),
            "deleted id {id} (positional or DV) must NOT appear in the post-delete result"
        );
    }
}

// ---------------------------------------------------------------------------
// Fan-out invariance: post-delete result must not depend on shard placement
// ---------------------------------------------------------------------------

/// Deterministically forces both a same-shard and a different-shard
/// placement of `mor_mixed`'s two data files (one positional-delete-backed,
/// one deletion-vector-backed) via `PARALLELISM_FACTOR`, not
/// hash-partitioning luck, and asserts the combined post-delete result is
/// identical either way.
///
/// The shard placement itself is proven directly against the production
/// `shard_count` + `partition_files_by_bytes` functions before the two VS
/// queries even run — same reasoning as
/// `e2e_positional_deletes_test.rs::e2e_partition_delete_invariant_across_fanout`:
/// - `PARALLELISM_FACTOR = 1` -> `shard_count(1, 1, 2) == 1` -> BOTH data
///   files (the positional-delete-backed one and the DV-backed one) land in
///   the SAME single shard.
/// - `PARALLELISM_FACTOR = MIXED_SPLIT_PARALLELISM_FACTOR (2)` ->
///   `shard_count(1, 2, 2) == 2` -> with as many shards as files, each shard
///   gets EXACTLY one file, so the two files land in DIFFERENT shards.
#[test]
fn e2e_mixed_invariant_across_fanout() {
    setup_e2e();

    // --- Prove the shard placement claim directly against production code ---
    let files = resolve_fixture_files(MIXED_MECHANISM_TABLE);
    assert_eq!(files.len(), 2, "sanity: mor_mixed must have 2 data files");

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
        "with G=1 both data files (pos-delete-backed and DV-backed) must land \
         in the SAME single shard"
    );

    let g_split = shard_count(1, MIXED_SPLIT_PARALLELISM_FACTOR, files.len());
    assert_eq!(
        g_split,
        files.len(),
        "PARALLELISM_FACTOR={MIXED_SPLIT_PARALLELISM_FACTOR} on a 1-node cluster with \
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
             (shard {i} got {}), so the two data files land in DIFFERENT shards",
            shard.len()
        );
    }

    // --- Run the actual query under both forced placements ---
    let deleted: HashSet<i64> = MIXED_MECHANISM_DELETED_IDS.iter().copied().collect();
    let expected: Vec<i64> = (1..=MIXED_MECHANISM_TOTAL_ROWS as i64)
        .filter(|id| !deleted.contains(id))
        .collect();

    let mut conn = exa_conn();

    let same_sql = format!(
        "SELECT id FROM {} ORDER BY id",
        vs_table(MIXED_SAMESHARD_VS_NAME, MIXED_MECHANISM_TABLE)
    );
    let same_ids = ids_column(&conn.query_columns(&same_sql));
    assert_eq!(
        same_ids, expected,
        "same-shard placement (PARALLELISM_FACTOR=1) must return the exact \
         combined post-delete id set {expected:?}, got {same_ids:?}"
    );

    let split_sql = format!(
        "SELECT id FROM {} ORDER BY id",
        vs_table(MIXED_SPLITSHARD_VS_NAME, MIXED_MECHANISM_TABLE)
    );
    let split_ids = ids_column(&conn.query_columns(&split_sql));
    assert_eq!(
        split_ids, expected,
        "split-shard placement (PARALLELISM_FACTOR={MIXED_SPLIT_PARALLELISM_FACTOR}) must \
         return the exact combined post-delete id set {expected:?}, got {split_ids:?}"
    );

    assert_eq!(
        same_ids, split_ids,
        "combined post-delete result must be invariant to shard placement: \
         same-shard={same_ids:?} vs split-shard={split_ids:?}"
    );
}

// ---------------------------------------------------------------------------
// Stack-unavailable contract
// ---------------------------------------------------------------------------

/// The deletion-vector suite FAILS (never skips) when the stack is
/// unavailable — same contract as
/// `e2e_positional_deletes_test.rs::positional_delete_suite_fails_when_stack_unavailable`:
/// every test above starts with `setup_e2e()`, whose
/// `wait_for_exasol`/`wait_for_minio`/`wait_for_iceberg_catalog` calls panic
/// (never return an `Err` to swallow) on a dependency that never comes up.
/// This test documents that contract by verifying the underlying connect
/// helper panics on an unreachable host rather than returning `Ok`.
#[test]
fn deletion_vector_suite_fails_when_stack_unavailable() {
    let result = std::panic::catch_unwind(|| ExaConn::connect("192.0.2.1", 8563, "sys", "exasol"));
    assert!(
        result.is_err(),
        "ExaConn::connect to an unreachable host must panic, not return Ok"
    );
}
