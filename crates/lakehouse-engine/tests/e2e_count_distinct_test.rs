//! End-to-end correctness tests for single-group `COUNT(DISTINCT col)` pushdown
//! and its combination with expression-argument aggregates (Q9b's shape).
//!
//! Exercises the scenarios in
//! `specs/vs-adapter/pushdown-planning-count-distinct/spec.md` that unit tests
//! cannot reach: real DataFusion DISTINCT row-scans over real Iceberg/Parquet
//! data, whose per-shard distinct rows are counted by an outer Exasol-native
//! `COUNT(DISTINCT "V")` (the native-merge path). This includes a
//! high-cardinality single-shard set far larger than the former per-shard cap
//! that issue #146 tripped — now it completes and returns the exact count.
//!
//! Shares the same Exasol + MinIO + Iceberg REST catalog stack as
//! `e2e_scan_test.rs` / `e2e_capability_test.rs`. The setup (SLC install,
//! BucketFS upload, VS creation) is intentionally NOT deduplicated across
//! files — each E2E test binary runs in its own process, so each needs its
//! own `OnceLock`-guarded setup; this mirrors `e2e_capability_test.rs`.
//!
//! In addition to the shared `events`/`labels`/`regions` seed tables, this
//! file seeds two more tables used ONLY here (`common::seed::seed_distinct_probe`,
//! `seed_high_card_probe`) so the other E2E binaries do not pay for data they
//! do not need.
//!
//! All tests FAIL (never skip) when the stack is unavailable — per project rules.
#![cfg(feature = "exasol-e2e")]

mod common;
use common::exasol_ws::ExaConn;
use common::seed::{
    DISTINCT_CATEGORY_COL, DISTINCT_CATEGORY_COUNT, DISTINCT_COMMENT_COL,
    DISTINCT_COMMENT_LENGTH_SUM, DISTINCT_REGION_COL, DISTINCT_REGION_COUNT, E2E_DISTINCT_TABLE,
    E2E_HIGH_CARD_TABLE, E2E_NAMESPACE, HIGH_CARD_COL, HIGH_CARD_ROWS, seed_distinct_probe,
    seed_events, seed_high_card_probe,
};
use common::stack::{
    bucketfs_port, bucketfs_write_password, build_create_connection_sql, exasol_host,
    exasol_sql_port, iceberg_catalog_url, iceberg_catalog_url_internal, lakehouse_engine_so_path,
    local_stack_connection_password, upload_to_bucketfs, wait_for_exasol, wait_for_iceberg_catalog,
    wait_for_minio,
};

use std::sync::OnceLock;
use std::time::Duration;

// ---------------------------------------------------------------------------
// Constants (mirror e2e_scan_test.rs / e2e_capability_test.rs — same stack, same VS)
// ---------------------------------------------------------------------------

const SYS_PASSWORD: &str = "exasol";
const SCHEMA_NAME: &str = "LHVS";
const VS_NAME: &str = "MY_LAKEHOUSE";
const ADAPTER_SCRIPT_NAME: &str = "LAKEHOUSE_ADAPTER";
const SCAN_SCRIPT_NAME: &str = "LAKEHOUSE_SCAN";
/// LUA SET passthrough distributor doing the cross-node `GROUP BY shard_key`
/// fan-out. Not a Rust entry point — created by plain DDL, no .so involved.
const DISTRIBUTOR_SCRIPT_NAME: &str = "LAKEHOUSE_DISTRIBUTE_FILES";
const SO_BUCKETFS_PUT_PATH: &str = "/default/udf/liblakehouse_engine.so";
const SO_UDF_OBJECT_PATH: &str = "buckets/bfsdefault/default/udf/liblakehouse_engine.so";
const SLC_BUCKETFS_PUT_PATH: &str = "/default/slc/lakehouse-rustslc.tar.gz";
const SLC_VERSION: &str = "0.21.0";
const LANG_ALIAS: &str = "RUST";
/// Name of the Exasol CONNECTION carrying catalog + storage credentials.
const CATALOG_CONN_NAME: &str = "LAKEHOUSE_CATALOG_CREDS";

// ---------------------------------------------------------------------------
// One-time setup (idempotent; mirrors e2e_scan_test.rs / e2e_capability_test.rs)
// ponytail: duplicate setup — each E2E test binary runs independently, so
// each needs its own OnceLock guard.
// ---------------------------------------------------------------------------

static SETUP_DONE: OnceLock<()> = OnceLock::new();

fn setup_e2e() {
    SETUP_DONE.get_or_init(|| {
        wait_for_exasol();
        wait_for_minio();
        wait_for_iceberg_catalog();

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("tokio runtime");
        rt.block_on(async {
            seed_events(&iceberg_catalog_url(), "s3://warehouse/")
                .await
                .expect("seed Iceberg events table");
            seed_distinct_probe(&iceberg_catalog_url(), "s3://warehouse/")
                .await
                .expect("seed Iceberg distinct_probe table");
            seed_high_card_probe(&iceberg_catalog_url(), "s3://warehouse/")
                .await
                .expect("seed Iceberg high_card_probe table");
        });

        install_slc();

        let so_path = lakehouse_engine_so_path();
        upload_to_bucketfs(&so_path, SO_BUCKETFS_PUT_PATH);

        let mut conn = exa_conn();
        create_schema_and_scripts(&mut conn);
        create_virtual_schema(&mut conn);
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
        r#"CREATE OR REPLACE {LANG_ALIAS} SCALAR SCRIPT {SCHEMA_NAME}.{SCAN_SCRIPT_NAME}(common VARCHAR(2000000), files VARCHAR(2000000))
EMITS (...) AS
%udf_object {SO_UDF_OBJECT_PATH}
/"#
    ));
    // File distributor — LUA SET SCRIPT, pure passthrough.
    conn.execute(&format!(
        r#"CREATE OR REPLACE LUA SET SCRIPT {SCHEMA_NAME}.{DISTRIBUTOR_SCRIPT_NAME}(files VARCHAR(2000000))
EMITS (files VARCHAR(2000000)) AS
function run(ctx)
    repeat
        ctx.emit(ctx.files)
    until not ctx.next()
end
/"#
    ));
}

fn create_virtual_schema(conn: &mut ExaConn) {
    let password = local_stack_connection_password();
    let catalog_uri = iceberg_catalog_url_internal();
    let create_conn_sql = build_create_connection_sql(CATALOG_CONN_NAME, &catalog_uri, &password);
    conn.execute(&create_conn_sql);

    let _ = conn.try_execute(&format!("DROP VIRTUAL SCHEMA IF EXISTS {VS_NAME} CASCADE"));
    conn.execute(&format!(
        r#"CREATE VIRTUAL SCHEMA {VS_NAME}
USING {SCHEMA_NAME}.{ADAPTER_SCRIPT_NAME} WITH
  CATALOG_CONNECTION  = '{CATALOG_CONN_NAME}'
  ICEBERG_NAMESPACE   = '{E2E_NAMESPACE}'
  ALLOW_HTTP          = 'true'"#
    ));
}

fn distinct_table() -> String {
    format!("{VS_NAME}.{}", E2E_DISTINCT_TABLE.to_uppercase())
}

fn high_card_table() -> String {
    format!("{VS_NAME}.{}", E2E_HIGH_CARD_TABLE.to_uppercase())
}

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

fn parse_int(v: &serde_json::Value) -> i64 {
    v.as_i64()
        .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
        .unwrap_or_else(|| panic!("expected integer value, got: {v:?}"))
}

/// Runs `EXPLAIN VIRTUAL` for a Case 2/3 `COUNT(DISTINCT)` query (more than one
/// distinct, or a distinct mixed with an ordinary aggregate) and asserts the pushed
/// SQL is the DECLINED-fan-out qualified single-table wrapper: the exact select list
/// (every aggregate, each `COUNT(DISTINCT)` spliced VERBATIM) rendered over a
/// materialized raw scan aliased `LHS_T0`, so Exasol's own engine aggregates the
/// returned rows.
///
/// This is deliberately NOT the distinct fan-out and NOT a bare row scan: a Case 2/3
/// request cannot compose per-distinct SELECT-list scalar subqueries (Exasol rejects
/// an emitting UDF nested in a scalar subquery — `sqlCode 04000`, "emitting function
/// in expression"), and a bare row scan returns raw columns where Exasol expects one
/// per aggregate select item (`04000` column-count mismatch, since Exasol never
/// re-aggregates a declined pushdown).
fn assert_qualified_wrapper_pushed_down(conn: &mut ExaConn, query_sql: &str) {
    let explain_sql = format!("EXPLAIN VIRTUAL {query_sql}");
    let resp = conn.execute(&explain_sql);
    let result_set = &resp["responseData"]["results"][0]["resultSet"];
    let cols = conn.fetch_result_columns(result_set);

    let pushed_sql: String = cols
        .iter()
        .flat_map(|col| col.iter())
        .filter_map(|v| v.as_str())
        .collect::<Vec<_>>()
        .join(" ");

    assert!(
        pushed_sql.contains("LHS_T0"),
        "EXPLAIN VIRTUAL output must be the qualified single-table wrapper (one \
         aliased raw fan-out subquery, 'AS LHS_T0'), got:\n{pushed_sql}"
    );
    assert!(
        pushed_sql.to_uppercase().contains("COUNT(DISTINCT"),
        "the wrapper must render each COUNT(DISTINCT) verbatim over the materialized \
         scan (Exasol aggregates the returned rows), got:\n{pushed_sql}"
    );
    assert!(
        !pushed_sql.contains(r#"COUNT(DISTINCT "V")"#),
        "a Case 2/3 request must NOT emit the Case 1 distinct row-scan fan-out \
         (COUNT(DISTINCT \"V\")), got:\n{pushed_sql}"
    );
    assert!(
        !pushed_sql.contains(r#"(SELECT COUNT(DISTINCT "V")"#),
        "a Case 2/3 request must NOT compose per-distinct SELECT-list scalar \
         subqueries (the blocked 04000 design), got:\n{pushed_sql}"
    );
    assert!(
        !pushed_sql.contains("SELECT * FROM (SELECT"),
        "EXPLAIN VIRTUAL output must not be a raw row-scan fallback \
         ('SELECT * FROM (SELECT ...)'), got:\n{pushed_sql}"
    );
}

/// Runs `EXPLAIN VIRTUAL` for a `COUNT(DISTINCT ...)` query and asserts the
/// pushed SQL uses the native-merge fan-out — an outer `COUNT(DISTINCT "V")`
/// over a per-shard DISTINCT row-scan — rather than a raw row-scan fallback that
/// ships the whole column to Exasol to aggregate itself. This applies ONLY to a
/// lone single-group `COUNT(DISTINCT)` (Case 1); a Case 2/3 request declines the
/// fan-out and is asserted with `assert_qualified_wrapper_pushed_down` instead.
fn assert_count_distinct_fan_out_pushed_down(conn: &mut ExaConn, query_sql: &str) {
    let explain_sql = format!("EXPLAIN VIRTUAL {query_sql}");
    let resp = conn.execute(&explain_sql);
    let result_set = &resp["responseData"]["results"][0]["resultSet"];
    let cols = conn.fetch_result_columns(result_set);

    let pushed_sql: String = cols
        .iter()
        .flat_map(|col| col.iter())
        .filter_map(|v| v.as_str())
        .collect::<Vec<_>>()
        .join(" ");

    assert!(
        pushed_sql.to_uppercase().contains("COUNT(DISTINCT"),
        "EXPLAIN VIRTUAL output must contain a native COUNT(DISTINCT ...) merge \
         wrapper (the distinct count pushed down as a DISTINCT row-scan fan-out, \
         not a raw-scan fallback), got:\n{pushed_sql}"
    );
    assert!(
        !pushed_sql.contains("SELECT * FROM (SELECT"),
        "EXPLAIN VIRTUAL output must not be a raw row-scan fallback \
         ('SELECT * FROM (SELECT ...)'), got:\n{pushed_sql}"
    );
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// `COUNT(DISTINCT category)` over `distinct_probe` (seeded across TWO data
/// files / shards) proves the native-merge fan-out dedups AT RUNTIME, not
/// just in generated SQL text — the `support.rs` SQL-shape unit test can only
/// assert the wrapper text contains `COUNT(DISTINCT "V")`; only a real
/// Exasol run over real per-shard data can prove that outer aggregate
/// actually deduplicates across the shard boundary. Covers:
///   - dedup ACROSS shards: "A" appears as a non-NULL `category` value in both
///     shards (file 1: ids 3,6,9; file 2: ids 12,15,18) — a fan-out that just
///     summed per-shard distinct rows without deduplicating would overcount
///     (e.g. 2 + 2 = 4 instead of the correct 3).
///   - NULL exclusion: 7 of the 20 rows have `category IS NULL`, and none of
///     them may contribute to the distinct set.
///   - empty result: a WHERE filter matching zero rows returns a distinct
///     count of 0, not an error.
///   - an all-NULL local set edge case (`WHERE category IS NULL`) also
///     resolves to 0, exercising the "shard's local DISTINCT row-scan emits
///     nothing" path explicitly.
#[test]
fn count_distinct_dedups_across_shards_excludes_nulls_empty() {
    setup_e2e();
    let mut conn = exa_conn();

    // Dedup across shards + NULL exclusion.
    let sql = format!(
        "SELECT COUNT(DISTINCT {DISTINCT_CATEGORY_COL}) FROM {}",
        distinct_table()
    );
    assert_count_distinct_fan_out_pushed_down(&mut conn, &sql);
    let distinct_count = conn.query_scalar_i64(&sql);
    assert_eq!(
        distinct_count, DISTINCT_CATEGORY_COUNT,
        "COUNT(DISTINCT {DISTINCT_CATEGORY_COL}) must be {DISTINCT_CATEGORY_COUNT} \
         (values {{A,B,C}}, with A shared across both shards and NULLs excluded), \
         got {distinct_count}"
    );

    // Empty result (WHERE filter matches zero rows, but NOT zero files): 'AA'
    // is a category value that never appears in the seeded data, but it still
    // falls inside both files' min/max column-stats range ('A'..'B' for file 1,
    // 'A'..'C' for file 2 — "AA" lexicographically sorts between "A" and both
    // "B" and "C"), so Iceberg-level file pruning does NOT eliminate either
    // file. A predicate like `id > 1000` would instead prune BOTH files
    // entirely, which trips the unrelated, pre-existing empty-pushdown shape
    // bug tracked in issue #57 — this sub-case must exercise "the outer
    // COUNT(DISTINCT) counts zero rows from per-shard DISTINCT row-scans that
    // emitted nothing", not that bug, so it deliberately avoids
    // 100%-file-pruning predicates.
    let empty_sql = format!(
        "SELECT COUNT(DISTINCT {DISTINCT_CATEGORY_COL}) FROM {} WHERE {DISTINCT_CATEGORY_COL} = 'AA'",
        distinct_table()
    );
    let empty_count = conn.query_scalar_i64(&empty_sql);
    assert_eq!(
        empty_count, 0,
        "COUNT(DISTINCT {DISTINCT_CATEGORY_COL}) over an empty result set must be 0, \
         got {empty_count}"
    );

    // All-NULL local set edge case: every matching row has category IS NULL.
    let all_null_sql = format!(
        "SELECT COUNT(DISTINCT {DISTINCT_CATEGORY_COL}) FROM {} WHERE {DISTINCT_CATEGORY_COL} IS NULL",
        distinct_table()
    );
    let all_null_count = conn.query_scalar_i64(&all_null_sql);
    assert_eq!(
        all_null_count, 0,
        "COUNT(DISTINCT {DISTINCT_CATEGORY_COL}) WHERE {DISTINCT_CATEGORY_COL} IS NULL \
         must be 0 (an all-NULL local set), got {all_null_count}"
    );
}

/// The #146 regression proof: `COUNT(DISTINCT token)` over `high_card_probe` —
/// a single shard whose local distinct set is far larger than the deleted
/// per-shard byte cap — now COMPLETES (no `ResourcesExhausted`) and returns the
/// exact single-node distinct count.
///
/// `high_card_probe` is seeded as ONE data file (`HIGH_CARD_ROWS` unique 100-byte
/// `token` values), so the adapter's sharding yields exactly one shard and the
/// WHOLE distinct set (~3 MB) lands on it. Under the old JSON-serialized
/// distinct-set path this deterministically exceeded the 1,048,576-byte per-shard
/// budget and failed with `ResourcesExhausted`. Under the native-merge path each
/// shard-local distinct value streams as one row and Exasol's own
/// `COUNT(DISTINCT "V")` counts the union — no cap, exact result. Every token is
/// unique, so the distinct count equals `HIGH_CARD_ROWS`.
#[test]
fn high_cardinality_count_distinct_completes() {
    setup_e2e();
    let mut conn = exa_conn();

    let sql = format!(
        "SELECT COUNT(DISTINCT {HIGH_CARD_COL}) FROM {}",
        high_card_table()
    );

    // The count must go through the native-merge fan-out (outer COUNT(DISTINCT
    // "V") over a per-shard DISTINCT row-scan) — not a raw-scan fallback that
    // would leave the aggregation to Exasol and so bypass the #146-fixed path.
    assert_count_distinct_fan_out_pushed_down(&mut conn, &sql);

    // `query_scalar_i64` runs the query and asserts success internally, so a
    // ResourcesExhausted regression surfaces as a clear failure here rather than
    // a silent wrong answer.
    let distinct_count = conn.query_scalar_i64(&sql);
    assert_eq!(
        distinct_count, HIGH_CARD_ROWS as i64,
        "COUNT(DISTINCT {HIGH_CARD_COL}) over {HIGH_CARD_ROWS} unique tokens must \
         complete and equal {HIGH_CARD_ROWS} (the exact single-node distinct \
         count), got {distinct_count}"
    );
}

/// A single query combining multiple `COUNT(DISTINCT ...)` columns AND a
/// `SUM(LENGTH(...))`-shaped expression aggregate — the TPC-H Q9b shape
/// (see `bench/run.sh`'s "Q9b wide projection" query) — is a Case 3 request
/// (more than one distinct, mixed with an ordinary aggregate). It DECLINES the
/// distinct fan-out (which cannot compose in one SELECT list — `sqlCode 04000`,
/// "emitting function in expression") and routes to the qualified single-table
/// wrapper: every aggregate, each `COUNT(DISTINCT)` spliced VERBATIM, rendered over
/// a materialized sharded raw scan aliased `LHS_T0`, so Exasol's own engine
/// aggregates the returned rows. The wrapper's output is exactly N = 3 aggregate
/// columns (one per select item — not the raw row count, not the old
/// independent-scalar-subquery shape), and every value matches the single-node
/// (non-pushdown) result.
///
/// `distinct_probe`: `COUNT(DISTINCT category)` = 3, `COUNT(DISTINCT region)` = 4,
/// `SUM(LENGTH(comment))` = 210 (comment length == id, summed 1..=20) — the known
/// single-node values the wrapper must reproduce exactly.
#[test]
fn q9b_multi_count_distinct_matches_single_node() {
    setup_e2e();
    let mut conn = exa_conn();

    let sql = format!(
        "SELECT COUNT(DISTINCT {DISTINCT_CATEGORY_COL}), COUNT(DISTINCT {DISTINCT_REGION_COL}), \
         SUM(LENGTH({DISTINCT_COMMENT_COL})) FROM {}",
        distinct_table()
    );
    // Case 3 declines the fan-out and returns the qualified single-table wrapper.
    assert_qualified_wrapper_pushed_down(&mut conn, &sql);

    let cols = conn.query_columns(&sql);
    // N aggregate columns, one per select item — NOT the raw row count and NOT the
    // blocked per-distinct scalar-subquery shape.
    assert_eq!(cols.len(), 3, "expected 3 aggregate columns: {cols:?}");
    assert_eq!(cols[0].len(), 1, "expected 1 row: {cols:?}");

    let category_count = parse_int(&cols[0][0]);
    assert_eq!(
        category_count, DISTINCT_CATEGORY_COUNT,
        "COUNT(DISTINCT {DISTINCT_CATEGORY_COL}) must be {DISTINCT_CATEGORY_COUNT}, \
         got {category_count}"
    );

    let region_count = parse_int(&cols[1][0]);
    assert_eq!(
        region_count, DISTINCT_REGION_COUNT,
        "COUNT(DISTINCT {DISTINCT_REGION_COL}) must be {DISTINCT_REGION_COUNT}, \
         got {region_count}"
    );

    let comment_length_sum = cols[2][0]
        .as_i64()
        .or_else(|| cols[2][0].as_f64().map(|f| f.round() as i64))
        .or_else(|| {
            cols[2][0]
                .as_str()
                .and_then(|s| s.parse::<f64>().ok())
                .map(|f| f.round() as i64)
        })
        .unwrap_or_else(|| panic!("expected numeric SUM(LENGTH(...)), got: {:?}", cols[2][0]));
    assert_eq!(
        comment_length_sum, DISTINCT_COMMENT_LENGTH_SUM,
        "SUM(LENGTH({DISTINCT_COMMENT_COL})) must be {DISTINCT_COMMENT_LENGTH_SUM}, \
         got {comment_length_sum}"
    );
}

/// A single-group `COUNT(DISTINCT <string-expression>)` — the expression-argument
/// distinct case (issue #146 code-review follow-up, task 6.1). The fan-out's `"V"`
/// column carries the RAW VALUES of the counted EXPRESSION, not a count. If `"V"`
/// were declared with the COUNT's own (integer) result type — as it was before the
/// fix — the scan would coerce the expression's non-numeric string values to
/// DECIMAL at emit, turning every value into NULL and silently returning 0. With
/// `"V"` declared VARCHAR the string values survive, so Exasol's native
/// `COUNT(DISTINCT "V")` returns the exact single-node count.
///
/// `UPPER(category)` is idempotent on the already-uppercase {A,B,C} values (NULLs
/// excluded) → 3; `LOWER(region)` is idempotent on the lowercase
/// {north,central,south,east} values (no NULLs) → 4. Both exercise a string-valued
/// expression argument across the two-shard `distinct_probe` fixture, so a wrong
/// value type would surface as a wrong count.
#[test]
fn count_distinct_string_expression_argument_matches_single_node() {
    setup_e2e();
    let mut conn = exa_conn();

    // UPPER(category): string-valued expression, NULLs excluded → 3 distinct.
    let upper_sql = format!(
        "SELECT COUNT(DISTINCT UPPER({DISTINCT_CATEGORY_COL})) FROM {}",
        distinct_table()
    );
    assert_count_distinct_fan_out_pushed_down(&mut conn, &upper_sql);
    let upper_count = conn.query_scalar_i64(&upper_sql);
    assert_eq!(
        upper_count, DISTINCT_CATEGORY_COUNT,
        "COUNT(DISTINCT UPPER({DISTINCT_CATEGORY_COL})) must be \
         {DISTINCT_CATEGORY_COUNT} (string values {{A,B,C}}, NULLs excluded) — a 0 \
         here is the pre-fix defect (string values coerced to DECIMAL → NULL), got \
         {upper_count}"
    );

    // LOWER(region): string-valued expression, no NULLs → 4 distinct.
    let lower_sql = format!(
        "SELECT COUNT(DISTINCT LOWER({DISTINCT_REGION_COL})) FROM {}",
        distinct_table()
    );
    assert_count_distinct_fan_out_pushed_down(&mut conn, &lower_sql);
    let lower_count = conn.query_scalar_i64(&lower_sql);
    assert_eq!(
        lower_count, DISTINCT_REGION_COUNT,
        "COUNT(DISTINCT LOWER({DISTINCT_REGION_COL})) must be \
         {DISTINCT_REGION_COUNT} (string values {{north,central,south,east}}), got \
         {lower_count}"
    );
}

// ---------------------------------------------------------------------------
// All-files-pruned pushdown shape (issue #57)
// ---------------------------------------------------------------------------
//
// `distinct_probe` is seeded across two data files with disjoint id ranges
// (file 1: ids 1..=10, file 2: ids 11..=20 — see `seed_distinct_probe`), so
// `id > 1000` is beyond both files' max column stats and prunes 100% of the
// table's data files at the Iceberg level. Before the #57 fix, the zero-files
// short-circuit in `handle_pushdown` unconditionally returned the row-scan
// empty shape even for an aggregate/grouped request, so Exasol rejected the
// pushdown response with sqlCode 04000 ("Expected number of columns is 1 but
// pushdown query has N"). These three tests exercise the three plan shapes
// the fix must get right: single-group COUNT(DISTINCT), single-group SUM,
// and a grouped aggregate.

/// `COUNT(DISTINCT id)` with a predicate that prunes every data file returns
/// a single row with value `0` — not a pushdown-shape rejection.
#[test]
fn count_distinct_all_files_pruned_returns_zero() {
    setup_e2e();
    let mut conn = exa_conn();

    let sql = format!(
        "SELECT COUNT(DISTINCT id) FROM {} WHERE id > 1000",
        distinct_table()
    );
    let cols = conn.query_columns(&sql);
    assert_eq!(cols.len(), 1, "expected 1 aggregate column: {cols:?}");
    assert_eq!(cols[0].len(), 1, "expected exactly 1 row: {cols:?}");
    let count = parse_int(&cols[0][0]);
    assert_eq!(
        count, 0,
        "COUNT(DISTINCT id) over an all-files-pruned predicate must be 0, got {count}"
    );
}

/// Single-group `SUM(id)` with a predicate that prunes every data file
/// returns a single row with value `NULL` (single-node SQL semantics over
/// zero rows) — not a pushdown-shape rejection.
#[test]
fn sum_all_files_pruned_returns_null() {
    setup_e2e();
    let mut conn = exa_conn();

    let sql = format!("SELECT SUM(id) FROM {} WHERE id > 1000", distinct_table());
    let cols = conn.query_columns(&sql);
    assert_eq!(cols.len(), 1, "expected 1 aggregate column: {cols:?}");
    assert_eq!(cols[0].len(), 1, "expected exactly 1 row: {cols:?}");
    assert!(
        cols[0][0].is_null(),
        "SUM(id) over an all-files-pruned predicate must be NULL, got {:?}",
        cols[0][0]
    );
}

/// A grouped aggregate with a predicate that prunes every data file returns
/// zero rows in the grouped shape — not a pushdown-shape rejection.
#[test]
fn grouped_aggregate_all_files_pruned_returns_no_rows() {
    setup_e2e();
    let mut conn = exa_conn();

    let sql = format!(
        "SELECT id, COUNT(*) FROM {} WHERE id > 1000 GROUP BY id",
        distinct_table()
    );
    let cols = conn.query_columns(&sql);
    let total_rows: usize = cols.iter().map(|c| c.len()).sum();
    assert_eq!(
        total_rows, 0,
        "grouped aggregate over an all-files-pruned predicate must return zero rows, got {cols:?}"
    );
}
