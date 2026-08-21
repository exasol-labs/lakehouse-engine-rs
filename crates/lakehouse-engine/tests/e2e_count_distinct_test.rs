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
use common::e2e_harness::*;
use common::exasol_ws::ExaConn;
use common::seed::{
    DISTINCT_CATEGORY_COL, DISTINCT_CATEGORY_COUNT, DISTINCT_COMMENT_COL,
    DISTINCT_COMMENT_LENGTH_SUM, DISTINCT_REGION_COL, DISTINCT_REGION_COUNT, E2E_DISTINCT_TABLE,
    E2E_HIGH_CARD_TABLE, E2E_NAMESPACE, E2E_TYPED_TABLE, HIGH_CARD_COL, HIGH_CARD_ROWS,
    TYPED_COL_BOOL, TYPED_COL_DATE, TYPED_COL_DECIMAL_A, TYPED_COL_DECIMAL_B, TYPED_COL_DOUBLE,
    TYPED_COL_PRICE, TYPED_COL_QTY, TYPED_COL_TS, TYPED_COL_VARCHAR, seed_distinct_probe,
    seed_events, seed_high_card_probe, seed_typed_distinct_probe, typed_bool_distinct,
    typed_date_distinct, typed_decimal_a_distinct, typed_decimal_b_distinct, typed_double_distinct,
    typed_product_distinct, typed_ts_case_distinct, typed_ts_distinct, typed_varchar_char_distinct,
    typed_varchar_distinct, typed_varchar_upper_distinct,
};
use common::stack::{
    iceberg_catalog_url, wait_for_exasol, wait_for_iceberg_catalog, wait_for_minio,
};

use std::sync::OnceLock;

// ---------------------------------------------------------------------------
// Constants (mirror e2e_scan_test.rs / e2e_capability_test.rs — same stack, same VS)
// ---------------------------------------------------------------------------

const VS_NAME: &str = "MY_LAKEHOUSE";

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
            seed_typed_distinct_probe(&iceberg_catalog_url(), "s3://warehouse/")
                .await
                .expect("seed Iceberg typed_distinct_probe table");
        });

        install_slc();
        upload_so();

        let mut conn = exa_conn();
        create_schema_and_scripts(&mut conn);
        create_virtual_schema(&mut conn, &VsProps::new(VS_NAME, E2E_NAMESPACE));
    });
}

fn distinct_table() -> String {
    format!("{VS_NAME}.{}", E2E_DISTINCT_TABLE.to_uppercase())
}

fn high_card_table() -> String {
    format!("{VS_NAME}.{}", E2E_HIGH_CARD_TABLE.to_uppercase())
}

fn typed_table() -> String {
    format!("{VS_NAME}.{}", E2E_TYPED_TABLE.to_uppercase())
}

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

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

/// The harness reads a handle-backed result set to completion, not just its first
/// `fetch` response.
///
/// Task 1.6 measured the packing against the live stack rather than computing it:
/// an uncapped raw scan of `high_card_probe` (`HIGH_CARD_ROWS` rows of ~100-byte
/// `token` values, ~3 MB) returns ALL 30,000 rows in ONE response at the harness's
/// default 64 MiB `numBytes` budget, so no fixture that exists today makes the
/// harness issue a second `fetch` and a short read is unobservable at that budget
/// (`specs/_plans/fix-e2e-harness-undeclared-limit/injection-surface.md`). This
/// test therefore forces the chunking by reading the same scan at a 64 KiB budget,
/// where ~100-byte rows pack a few hundred to a response and 30,000 rows span tens
/// of them. How many rows the server packs into one response is the server's
/// business, so the invariant asserted is "more than one response", never an exact
/// count.
#[test]
fn harness_reads_high_cardinality_result_set_to_completion() {
    const CHUNKED_FETCH_NUM_BYTES: u64 = 65_536;

    setup_e2e();
    let mut conn = exa_conn();

    let sql = format!("SELECT {HIGH_CARD_COL} FROM {}", high_card_table());
    let resp = conn.execute(&sql);
    let result_set = &resp["responseData"]["results"][0]["resultSet"];

    let (cols, responses) =
        conn.fetch_result_columns_with_num_bytes(result_set, CHUNKED_FETCH_NUM_BYTES);

    let rows = cols.first().map_or(0, |col| col.len());
    assert_eq!(
        rows, HIGH_CARD_ROWS,
        "reading {HIGH_CARD_COL} at a {CHUNKED_FETCH_NUM_BYTES}-byte per-response \
         budget must yield every one of the {HIGH_CARD_ROWS} seeded rows, got \
         {rows} across {responses} fetch response(s) — a short read means the \
         harness stopped at the first response instead of reading to completion"
    );
    assert!(
        responses >= 2,
        "a {CHUNKED_FETCH_NUM_BYTES}-byte per-response budget must split \
         {HIGH_CARD_ROWS} ~100-byte rows across more than one fetch response, so \
         that the read loop is genuinely exercised, got {responses}"
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

/// A LONE single-group `COUNT(DISTINCT <string-expression>)` — the
/// expression-argument distinct case (PR #163 review: the injectivity concern that
/// motivated this plan). Before the dispatch fix a lone expression-argument distinct
/// fanned out per shard declaring its value column `"V"` as `VARCHAR(2000000)`,
/// relying on the expression's native→`Utf8` cast being injective across shards — an
/// unproven assumption that could silently undercount. After the fix a lone
/// `COUNT(DISTINCT <expression>)` NO LONGER fans out: `is_lone_count_distinct`
/// requires a bare-column argument, so an expression argument declines the fan-out
/// and routes to the qualified single-table wrapper (`has_distinct &&
/// !is_lone_count_distinct` in `mod.rs`) — exactly like a genuine Case 2/3 request.
/// Exasol evaluates the expression and the DISTINCT natively over the exact-typed
/// base column of a materialized raw scan aliased `LHS_T0`, with NO per-shard
/// fan-out and NO `VARCHAR`-typed intermediate value at all.
///
/// The pushed shape is therefore the qualified wrapper (asserted via
/// `assert_qualified_wrapper_pushed_down`), NOT the Case 1 `COUNT(DISTINCT "V")`
/// fan-out. The correctness assertion is unchanged: `UPPER(category)` over the
/// already-uppercase {A,B,C} values (NULLs excluded) → 3; `LOWER(region)` over the
/// lowercase {north,central,south,east} values (no NULLs) → 4, each across the
/// two-shard `distinct_probe` fixture so cross-shard dedup is genuinely exercised.
#[test]
fn count_distinct_string_expression_argument_matches_single_node() {
    setup_e2e();
    let mut conn = exa_conn();

    // UPPER(category): string-valued expression, NULLs excluded → 3 distinct.
    let upper_sql = format!(
        "SELECT COUNT(DISTINCT UPPER({DISTINCT_CATEGORY_COL})) FROM {}",
        distinct_table()
    );
    // A lone expression-argument distinct now declines the fan-out and routes to the
    // qualified single-table wrapper (no per-shard fan-out, no VARCHAR intermediate).
    assert_qualified_wrapper_pushed_down(&mut conn, &upper_sql);
    let upper_count = conn.query_scalar_i64(&upper_sql);
    assert_eq!(
        upper_count, DISTINCT_CATEGORY_COUNT,
        "COUNT(DISTINCT UPPER({DISTINCT_CATEGORY_COL})) must be \
         {DISTINCT_CATEGORY_COUNT} (string values {{A,B,C}}, NULLs excluded), got \
         {upper_count}"
    );

    // LOWER(region): string-valued expression, no NULLs → 4 distinct.
    let lower_sql = format!(
        "SELECT COUNT(DISTINCT LOWER({DISTINCT_REGION_COL})) FROM {}",
        distinct_table()
    );
    assert_qualified_wrapper_pushed_down(&mut conn, &lower_sql);
    let lower_count = conn.query_scalar_i64(&lower_sql);
    assert_eq!(
        lower_count, DISTINCT_REGION_COUNT,
        "COUNT(DISTINCT LOWER({DISTINCT_REGION_COL})) must be \
         {DISTINCT_REGION_COUNT} (string values {{north,central,south,east}}), got \
         {lower_count}"
    );
}

/// Task 2.2 — an expression-argument `COUNT(DISTINCT)` COMBINED with another
/// distinct and an ordinary aggregate, inside the Case 3 qualified single-table
/// wrapper. This is the reviewer's specifically-flagged coverage gap: no prior test
/// (unit or E2E) exercised an expression-argument distinct INSIDE the wrapper —
/// `q9b_multi_count_distinct_matches_single_node` uses only bare-column distincts
/// plus a non-distinct expression aggregate.
///
/// `COUNT(DISTINCT UPPER(category))` (expression arg) + `COUNT(DISTINCT region)`
/// (bare arg) + `SUM(LENGTH(comment))` (expression aggregate) over the two-shard
/// `distinct_probe` fixture. The whole select list declines the fan-out (it cannot
/// compose in one SELECT list — `sqlCode 04000`) and routes to the qualified wrapper
/// (`assert_qualified_wrapper_pushed_down`), where Exasol evaluates every item —
/// including the expression-argument distinct — natively over exact-typed base
/// columns of the materialized `LHS_T0` scan. Every value must match the single-node
/// result: `UPPER(category)` → 3 (idempotent on {A,B,C}, NULLs excluded), `region`
/// → 4, `SUM(LENGTH(comment))` → 210.
#[test]
fn count_distinct_expression_arg_combined_matches_single_node() {
    setup_e2e();
    let mut conn = exa_conn();

    let sql = format!(
        "SELECT COUNT(DISTINCT UPPER({DISTINCT_CATEGORY_COL})), \
         COUNT(DISTINCT {DISTINCT_REGION_COL}), \
         SUM(LENGTH({DISTINCT_COMMENT_COL})) FROM {}",
        distinct_table()
    );
    assert_qualified_wrapper_pushed_down(&mut conn, &sql);

    let cols = conn.query_columns(&sql);
    assert_eq!(cols.len(), 3, "expected 3 aggregate columns: {cols:?}");
    assert_eq!(cols[0].len(), 1, "expected 1 row: {cols:?}");

    let category_count = parse_int(&cols[0][0]);
    assert_eq!(
        category_count, DISTINCT_CATEGORY_COUNT,
        "COUNT(DISTINCT UPPER({DISTINCT_CATEGORY_COL})) must be \
         {DISTINCT_CATEGORY_COUNT}, got {category_count}"
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

/// Task 2.3 — the reviewer's requested bare-column `COUNT(DISTINCT <col>)` type
/// matrix (Case 1 fan-out path, unaffected by the dispatch fix but explicitly
/// requested). Each column of `typed_distinct_probe` is a distinct Exasol type,
/// seeded across TWO data files with NULLs mixed in and at least one non-NULL value
/// repeated in BOTH files — so the outer `COUNT(DISTINCT "V")` must dedup across the
/// shard boundary (a per-shard sum would overcount) and must exclude NULLs. Each
/// expected count is computed by the fixture from the SAME data it seeds
/// (`typed_*_distinct`), never a hand-written constant, so a data edit cannot
/// silently disagree.
///
/// Types covered: DECIMAL(9,2), DECIMAL(20,4) (varying precision/scale), DOUBLE,
/// VARCHAR, DATE, TIMESTAMP (millisecond-fraction distinctions within one second),
/// BOOLEAN. Every case must also push down as the native-merge fan-out (Case 1),
/// asserted via `assert_count_distinct_fan_out_pushed_down`.
///
/// CHAR is intentionally NOT in this matrix: no Iceberg/Arrow source type maps to
/// Exasol CHAR (Iceberg `string` → VARCHAR per the crate's type table), so a
/// bare-column CHAR virtual column is structurally unreachable through the scan
/// path. VARCHAR (above) is the closest bare-column string coverage; a CHAR-typed
/// intermediate is covered instead as a wrapper-routed `CAST(... AS CHAR(n))`
/// expression argument (see `count_distinct_expression_arg_via_wrapper_...`).
#[test]
fn count_distinct_bare_column_type_matrix_matches_single_node() {
    setup_e2e();
    let mut conn = exa_conn();

    let cases: [(&str, i64); 7] = [
        (TYPED_COL_DECIMAL_A, typed_decimal_a_distinct()),
        (TYPED_COL_DECIMAL_B, typed_decimal_b_distinct()),
        (TYPED_COL_DOUBLE, typed_double_distinct()),
        (TYPED_COL_VARCHAR, typed_varchar_distinct()),
        (TYPED_COL_DATE, typed_date_distinct()),
        (TYPED_COL_TS, typed_ts_distinct()),
        (TYPED_COL_BOOL, typed_bool_distinct()),
    ];

    for (col, expected) in cases {
        let sql = format!("SELECT COUNT(DISTINCT {col}) FROM {}", typed_table());
        // Bare-column lone distinct → native-merge fan-out (Case 1), not the wrapper.
        assert_count_distinct_fan_out_pushed_down(&mut conn, &sql);
        let got = conn.query_scalar_i64(&sql);
        assert_eq!(
            got, expected,
            "COUNT(DISTINCT {col}) must be {expected} (cross-shard dedup, NULLs \
             excluded), got {got}"
        );
    }
}

/// Task 2.4 — expression-argument `COUNT(DISTINCT <expr>)` now routed through the
/// qualified wrapper (numeric, string, temporal, and CHAR-cast), the regression
/// proof that the injectivity/precision-truncation class of bug the reviewer flagged
/// cannot recur via this path. The removed VARCHAR fan-out arm dedup'd on a
/// per-shard `arrow::compute::cast(.., Utf8)` string form, which could collapse two
/// natively-distinct values that print alike. The wrapper never casts to string at
/// all: Exasol evaluates the expression and the DISTINCT natively over exact-typed
/// base columns of the materialized `LHS_T0` scan. Each expected count is computed
/// by the fixture from the same seeded data.
///
/// - Numeric: `COUNT(DISTINCT c_price * c_qty)` — products dedup'd natively across
///   shards (a shared product value appears in both files); NULL operands excluded.
/// - String: `COUNT(DISTINCT UPPER(c_varchar))` — mixed-case values that fold
///   together under `UPPER` ACROSS the shard boundary ("aa"/"Aa", "bb"/"BB"), so the
///   folded distinct count (5) is strictly below the raw distinct count (8); native
///   `UPPER` dedup must collapse them exactly.
/// - Temporal (the required precision case): `COUNT(DISTINCT CASE WHEN c_bool THEN
///   c_ts ELSE NULL END)` — the selected timestamps differ ONLY in the millisecond
///   fraction within one whole second, split across two shards. The wrapper dedups
///   on the native `TIMESTAMP` column, so millisecond-distinct instants are counted
///   distinct; a naive string formatting that dropped fractional seconds would
///   collapse them and undercount. A `CASE` (not a bare column) forces the wrapper
///   route and is not optimizer-folded to the column.
/// - CHAR: `COUNT(DISTINCT CAST(c_varchar AS CHAR(20)))` — a CHAR-typed intermediate
///   (the type unreachable as a bare column). Correctness is asserted without a
///   pushed-shape assertion: whether it routes through the wrapper or Exasol's own
///   fallback, the count must be exact (fixed-width padding is injective over the
///   space-free seeded values).
#[test]
fn count_distinct_expression_arg_via_wrapper_matches_single_node() {
    setup_e2e();
    let mut conn = exa_conn();

    // Numeric product expression.
    let numeric_sql = format!(
        "SELECT COUNT(DISTINCT {TYPED_COL_PRICE} * {TYPED_COL_QTY}) FROM {}",
        typed_table()
    );
    assert_qualified_wrapper_pushed_down(&mut conn, &numeric_sql);
    let numeric_count = conn.query_scalar_i64(&numeric_sql);
    assert_eq!(
        numeric_count,
        typed_product_distinct(),
        "COUNT(DISTINCT {TYPED_COL_PRICE} * {TYPED_COL_QTY}) must be {} \
         (native product dedup across shards, NULL operands excluded), got {numeric_count}",
        typed_product_distinct()
    );

    // String expression with cross-shard case-folding.
    let string_sql = format!(
        "SELECT COUNT(DISTINCT UPPER({TYPED_COL_VARCHAR})) FROM {}",
        typed_table()
    );
    assert_qualified_wrapper_pushed_down(&mut conn, &string_sql);
    let string_count = conn.query_scalar_i64(&string_sql);
    assert_eq!(
        string_count,
        typed_varchar_upper_distinct(),
        "COUNT(DISTINCT UPPER({TYPED_COL_VARCHAR})) must be {} (mixed-case values \
         fold together across shards; strictly below the raw distinct count {}), got \
         {string_count}",
        typed_varchar_upper_distinct(),
        typed_varchar_distinct()
    );

    // Temporal expression — millisecond-precision distinctions preserved via native
    // dedup (the precision-truncation regression proof).
    let temporal_sql = format!(
        "SELECT COUNT(DISTINCT CASE WHEN {TYPED_COL_BOOL} THEN {TYPED_COL_TS} ELSE NULL END) \
         FROM {}",
        typed_table()
    );
    assert_qualified_wrapper_pushed_down(&mut conn, &temporal_sql);
    let temporal_count = conn.query_scalar_i64(&temporal_sql);
    assert_eq!(
        temporal_count,
        typed_ts_case_distinct(),
        "COUNT(DISTINCT CASE WHEN {TYPED_COL_BOOL} THEN {TYPED_COL_TS} END) must be {} \
         (millisecond-distinct timestamps within one second, deduped natively across \
         shards — a fractional-second-truncating string cast would undercount), got \
         {temporal_count}",
        typed_ts_case_distinct()
    );

    // CHAR-typed intermediate via CAST (bare CHAR is unreachable). Correctness only —
    // the count is exact whether pushed to the wrapper or handled by Exasol fallback.
    let char_sql = format!(
        "SELECT COUNT(DISTINCT CAST({TYPED_COL_VARCHAR} AS CHAR(20))) FROM {}",
        typed_table()
    );
    let char_count = conn.query_scalar_i64(&char_sql);
    assert_eq!(
        char_count,
        typed_varchar_char_distinct(),
        "COUNT(DISTINCT CAST({TYPED_COL_VARCHAR} AS CHAR(20))) must be {} \
         (fixed-width padding is injective over the space-free seeded values), got \
         {char_count}",
        typed_varchar_char_distinct()
    );
}

/// Task 2.5 — empty / all-NULL / all-files-pruned COUNT(DISTINCT) → 0 for an
/// expression-argument distinct now routed through the qualified wrapper. The
/// bare-column Case 1 equivalents are already covered
/// (`count_distinct_dedups_across_shards_excludes_nulls_empty` for empty + all-NULL,
/// `count_distinct_all_files_pruned_returns_zero` for all-pruned); this adds the
/// missing wrapper-path coverage, proving the declined-fan-out path returns a single
/// `0` row (never a pushdown-shape rejection) for each empty shape.
///
/// - Empty non-pruned: `WHERE category = 'AA'` matches zero rows but prunes NO files
///   ('AA' is inside both files' min/max category range), so the wrapper materializes
///   two empty per-shard scans and Exasol's `COUNT(DISTINCT UPPER(category))` over
///   them is 0.
/// - All-NULL: `WHERE category IS NULL` — `UPPER(NULL)` is NULL, excluded → 0.
/// - All-files-pruned: `WHERE id > 1000` prunes both data files at the Iceberg level;
///   the wrapper's zero-files short-circuit must still return the shape-correct
///   single `0`.
#[test]
fn count_distinct_expression_arg_empty_returns_zero() {
    setup_e2e();
    let mut conn = exa_conn();

    let scenarios: [(&str, String); 3] = [
        (
            "empty non-pruned",
            format!("{DISTINCT_CATEGORY_COL} = 'AA'"),
        ),
        ("all-NULL", format!("{DISTINCT_CATEGORY_COL} IS NULL")),
        ("all-files-pruned", "id > 1000".to_string()),
    ];

    for (label, predicate) in &scenarios {
        let sql = format!(
            "SELECT COUNT(DISTINCT UPPER({DISTINCT_CATEGORY_COL})) FROM {} WHERE {predicate}",
            distinct_table()
        );
        let cols = conn.query_columns(&sql);
        assert_eq!(
            cols.len(),
            1,
            "[{label}] expected 1 aggregate column: {cols:?}"
        );
        assert_eq!(
            cols[0].len(),
            1,
            "[{label}] expected exactly 1 row: {cols:?}"
        );
        let count = parse_int(&cols[0][0]);
        assert_eq!(
            count, 0,
            "[{label}] COUNT(DISTINCT UPPER({DISTINCT_CATEGORY_COL})) must be 0, got {count}"
        );
    }
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

/// Native (non-virtual) copy of the `distinct_probe` columns the scalar-wrapped
/// `COUNT(DISTINCT)` oracle reads.
const GROUND_TRUTH_DISTINCT_TABLE: &str = "GT_DISTINCT_PROBE";

/// The floor for a nested aggregate the single-group merge cannot decompose: a
/// scalar function wrapping a `COUNT(DISTINCT)` widens the projection instead of
/// being evaluated per shard, routing to the qualified single-table wrapper so
/// Exasol computes the DISTINCT itself over the materialized scan.
///
/// The result must be the single-node one — exactly ONE row equal to the native
/// oracle — not one partial row per shard, and no partial-aggregate column may
/// appear in the pushed SQL: `COUNT(DISTINCT)` has no partial/merge
/// decomposition, so pushing it as one would silently over-count.
#[test]
fn e2e_scalar_wrapped_count_distinct_routes_to_wrapper_and_matches_native_oracle() {
    setup_e2e();
    let mut conn = exa_conn();

    let select_list = format!("ROUND(COUNT(DISTINCT {DISTINCT_REGION_COL}), 2)");
    let sql = format!("SELECT {select_list} FROM {}", distinct_table());

    let pushed = explain_virtual_sql(&mut conn, &sql);
    assert!(
        pushed.contains(r#"AS "LHS_T0""#),
        "a scalar-wrapped COUNT(DISTINCT) must route to the qualified \
         single-table wrapper (one aliased raw fan-out subquery), got:\n{pushed}"
    );
    assert!(
        pushed.to_uppercase().contains("COUNT(DISTINCT"),
        "the wrapper must render the COUNT(DISTINCT) verbatim over the \
         materialized scan, got:\n{pushed}"
    );
    assert!(
        !pushed.contains("PARTIAL_"),
        "COUNT(DISTINCT) has no partial/merge decomposition, so no partial \
         aggregate column may be pushed for it, got:\n{pushed}"
    );

    conn.execute(&format!(
        "CREATE OR REPLACE TABLE {SCHEMA_NAME}.{GROUND_TRUTH_DISTINCT_TABLE} AS \
         SELECT {DISTINCT_CATEGORY_COL}, {DISTINCT_REGION_COL} FROM {}",
        distinct_table()
    ));

    let actual = conn.query_columns(&sql);
    assert_eq!(actual.len(), 1, "expected 1 column: {actual:?}");
    assert_eq!(
        actual[0].len(),
        1,
        "a scalar-wrapped COUNT(DISTINCT) must return the single-node result — \
         exactly ONE row, not one per shard: {actual:?}"
    );

    let expected = conn.query_columns(&format!(
        "SELECT {select_list} FROM {SCHEMA_NAME}.{GROUND_TRUTH_DISTINCT_TABLE}"
    ));
    let (got, want) = (parse_int(&actual[0][0]), parse_int(&expected[0][0]));
    assert_eq!(
        got, want,
        "scalar-wrapped COUNT(DISTINCT {DISTINCT_REGION_COL}) must equal the \
         native oracle {want}, got {got}"
    );
    assert_eq!(
        got, DISTINCT_REGION_COUNT,
        "the seeded fixture has {DISTINCT_REGION_COUNT} distinct \
         {DISTINCT_REGION_COL} values, got {got}"
    );
}
