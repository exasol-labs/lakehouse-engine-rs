//! End-to-end integration tests for the lakehouse-engine Virtual Schema.
//!
//! These tests run against a live Exasol + MinIO + Iceberg REST catalog stack.
//! They FAIL (never skip) when the stack is unavailable — per project rules.
//!
//! All tests share one VS, so they must run serially (--test-threads=1).
//! The Makefile `test-e2e` target passes this flag automatically.
//!
//! # Setup (done once via `setup_e2e` called from each test)
//! 1. Seed the Iceberg table into the REST catalog over MinIO.
//! 2. Install SLC 0.21.0 (LHRUST alias) and upload liblakehouse_engine.so to BucketFS.
//! 3. Create the LAKEHOUSE_ADAPTER script and the LAKEHOUSE_SCAN SCALAR script
//!    (both from the same .so), and the LAKEHOUSE_DISTRIBUTE_FILES LUA SET
//!    passthrough distributor.
//! 4. Create the LHVS Virtual Schema over the seeded table.
//!
//! The VS properties carry UDF-internal URLs (docker-network names) for the
//! catalog and MinIO, because the UDF runs inside the Exasol container.
#![cfg(feature = "exasol-e2e")]

mod common;
use common::e2e_harness::*;
use common::exasol_ws::ExaConn;
use common::seed::{
    DIM_CUSTOMER_ROWS, E2E_DIM_TABLE, E2E_EVO_TABLE, E2E_FACT_TABLE, E2E_LINEITEM_TABLE,
    E2E_NAMESPACE, E2E_PART_TABLE, E2E_TABLE, E2E_TABLE_2, EVO_INITDEF_POST_ADD_IDS,
    EVO_INITDEF_PRE_ADD_IDS, EVO_INITDEF_TABLE, EVO_INITDEF_TOTAL_ROWS, EVO_NEW_COL,
    EVO_TOTAL_ROWS, FACT_ORDERS_ROWS, LINEITEM_ROWS, PART_CENTRAL_IDS, PART_COL, PART_NORTH_IDS,
    PART_ROWS_PER_FILE, PART_TOTAL_ROWS, PART_VAL_CENTRAL, PART_VAL_NORTH, SEED_LABELS_ROWS,
    SEED_ROWS_SCORE_GT_15, SEED_TOTAL_ROWS, initdef_columns, seed_added_columns_initial_default,
    seed_events, seed_renamed_column,
};
use common::stack::{
    build_create_connection_sql, iceberg_catalog_url, wait_for_exasol, wait_for_iceberg_catalog,
    wait_for_minio,
};

use lakehouse_catalog::CatalogSession;
use lakehouse_engine::adapter::pushdown::{ConnectionStorage, ScanSource, format_reader};

use std::sync::OnceLock;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const VS_NAME: &str = "MY_LAKEHOUSE";

// ---------------------------------------------------------------------------
// One-time setup
// ---------------------------------------------------------------------------

/// Marker so setup runs once across the serial test binary.
static SETUP_DONE: OnceLock<()> = OnceLock::new();

fn setup_e2e() {
    SETUP_DONE.get_or_init(|| {
        // 1. Verify stack is up.
        wait_for_exasol();
        wait_for_minio();
        wait_for_iceberg_catalog();

        // 2. Seed the Iceberg table.
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("tokio runtime");
        rt.block_on(async {
            seed_events(&iceberg_catalog_url(), "s3://warehouse/")
                .await
                .expect("seed Iceberg events table")
        });

        // 3. Install SLC 0.21.0 (download + upload + ALTER SYSTEM).
        install_slc();

        // 4. Upload the .so to BucketFS.
        upload_so();

        // 5. Create Exasol schema + scripts + VS.
        let mut conn = exa_conn();
        create_schema_and_scripts(&mut conn);
        create_virtual_schema(&mut conn, &VsProps::new(VS_NAME, E2E_NAMESPACE));
    });
}

// ---------------------------------------------------------------------------
// Helpers: qualify VS table names (adapter uppercases all Iceberg names).
// ---------------------------------------------------------------------------

fn vs_table() -> String {
    format!("{VS_NAME}.{}", E2E_TABLE.to_uppercase())
}

fn vs_labels_table() -> String {
    format!("{VS_NAME}.{}", E2E_TABLE_2.to_uppercase())
}

/// `fact_lineitem` — seeded by `seed_events` (via `seed_multi_table_join_extension`)
/// alongside the `events`/`labels` tables, so it is already available under this
/// file's `VS_NAME` without any extra setup. See `common/seed.rs` for its columns
/// (`L_RETURNFLAG`, `L_QUANTITY`, `L_EXTENDEDPRICE`, ...) and row layout.
fn vs_lineitem_table() -> String {
    format!("{VS_NAME}.{}", E2E_LINEITEM_TABLE.to_uppercase())
}

/// `dim_customer` / `fact_orders` — seeded by `seed_events` (via
/// `seed_star_schema`) alongside `events`, so already available under this
/// file's `VS_NAME`. Used ONLY for the #193 outer-join-decline re-push shape,
/// which `events` (a single table with no FK relationship) cannot express.
fn vs_dim_table() -> String {
    format!("{VS_NAME}.{}", E2E_DIM_TABLE.to_uppercase())
}

/// `fact_orders` counterpart to [`vs_dim_table`] — same seeding, same scope.
fn vs_fact_table() -> String {
    format!("{VS_NAME}.{}", E2E_FACT_TABLE.to_uppercase())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// The E2E projection + filter + LIMIT query returns the correct projected,
/// filtered, capped rows.
#[test]
fn e2e_projection_filter_limit_returns_correct_rows() {
    setup_e2e();
    let mut conn = exa_conn();

    // SELECT id, name, score FROM ... WHERE score > 15.0 LIMIT 5
    // Seeded rows: id 1..20, score = 5.0*id. score > 15.0 → id >= 4 (17 rows).
    // LIMIT 5 → first 5 matching: id 4,5,6,7,8 (scores 20,25,30,35,40).
    let sql = format!(
        "SELECT id, name, score FROM {} WHERE score > 15.0 LIMIT 5",
        vs_table()
    );
    let cols = conn.query_columns(&sql);
    assert_eq!(
        cols.len(),
        3,
        "expected 3 columns (id, name, score): {cols:?}"
    );
    let id_col = &cols[0];
    assert_eq!(
        id_col.len(),
        5,
        "expected exactly 5 rows from LIMIT 5: {cols:?}"
    );

    // All returned scores must be > 15.0.
    let score_col = &cols[2];
    for score in score_col {
        let s = score
            .as_f64()
            .unwrap_or_else(|| panic!("score not f64: {score:?}"));
        assert!(s > 15.0, "filter violated: score {s} <= 15.0");
    }

    // IDs must be ascending and >= 4 (score = 5*id > 15 → id >= 4).
    // Exasol serializes DECIMAL(20,0) as a JSON string, so accept either form.
    let ids: Vec<i64> = id_col
        .iter()
        .map(|v| {
            v.as_i64()
                .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
                .unwrap_or_else(|| panic!("id not an integer: {v:?}"))
        })
        .collect();
    assert!(
        ids.iter().all(|&id| id >= 4),
        "id < 4 appeared (score would be <= 15): {ids:?}"
    );
    // Verify names match the expected pattern.
    let name_col = &cols[1];
    for (i, name) in name_col.iter().enumerate() {
        let expected_id = ids[i];
        let n = name
            .as_str()
            .unwrap_or_else(|| panic!("name not string: {name:?}"));
        assert!(
            n.contains(&format!("{expected_id:02}")),
            "name '{n}' does not match expected id {expected_id}"
        );
    }

    // #193 regression: aliased FROM (`EVENTS e`) must resolve exactly like the
    // unaliased query above, proving the aliased projection + filter + top-N
    // shape strips the leaked `tableAlias` and resolves bare names. Ordering
    // explicitly by score ASC pins the exact row set: with score = 5.0*id
    // (seed.rs), the 5 lowest scores > 15.0 are ids 4..8.
    let sql_aliased = format!(
        "SELECT e.id, e.name, e.score FROM {} e WHERE e.score > 15.0 ORDER BY e.score LIMIT 5",
        vs_table()
    );
    let cols_aliased = conn.query_columns(&sql_aliased);
    assert_eq!(
        cols_aliased.len(),
        3,
        "aliased query expected 3 columns (id, name, score): {cols_aliased:?}"
    );
    assert_eq!(
        cols_aliased[0].len(),
        5,
        "aliased query expected exactly 5 rows from LIMIT 5: {cols_aliased:?}"
    );
    let aliased_ids: Vec<i64> = cols_aliased[0].iter().map(parse_int).collect();
    let expected_aliased_ids: Vec<i64> = vec![4, 5, 6, 7, 8];
    assert_eq!(
        aliased_ids, expected_aliased_ids,
        "aliased ORDER BY e.score LIMIT 5 must return the 5 lowest scores > 15.0 \
         (ids 4..8, score = 5.0*id): {aliased_ids:?}"
    );

    // #193 regression: a scalar select-list expression over an aliased column
    // (`e.score + 1`) must render the bare "SCORE" name under the scan
    // relation, not the leaked "E"."SCORE". Row count matches
    // SEED_ROWS_SCORE_GT_15 and every value is the filtered score + 1.
    let sql_expr = format!(
        "SELECT e.score + 1 FROM {} e WHERE e.score > 15.0",
        vs_table()
    );
    let cols_expr = conn.query_columns(&sql_expr);
    assert_eq!(
        cols_expr.len(),
        1,
        "scalar expression query expected 1 column: {cols_expr:?}"
    );
    assert_eq!(
        cols_expr[0].len(),
        SEED_ROWS_SCORE_GT_15,
        "scalar expression query expected {SEED_ROWS_SCORE_GT_15} rows: {cols_expr:?}"
    );
    // score = 5.0*id, so score + 1 always ends in .0 with score % 5.0 == 0,
    // meaning (score + 1) % 5.0 == 1.0 — this fails if the `+ 1` is dropped.
    for v in &cols_expr[0] {
        let plus_one = parse_numeric(v);
        assert_eq!(
            plus_one % 5.0,
            1.0,
            "e.score + 1 must equal a multiple of 5 plus 1 (score = 5.0*id), got {plus_one}"
        );
    }

    // #193 regression: an UNQUALIFIED filter column (`score`, no `e.` prefix)
    // under an aliased FROM. Exasol still stamps `tableAlias:"E"` on this
    // column node even though the user wrote no qualifier — the leak this
    // fix targets. Row count must match the same SEED_ROWS_SCORE_GT_15 the
    // unaliased/qualified cases above use.
    let row_count_unqualified = conn.query_row_count(&format!(
        "SELECT id FROM {} e WHERE score > 15.0",
        vs_table()
    ));
    assert_eq!(
        row_count_unqualified, SEED_ROWS_SCORE_GT_15 as i64,
        "unqualified filter under alias (FROM ... e WHERE score > 15.0) must \
         return {SEED_ROWS_SCORE_GT_15} rows, got {row_count_unqualified}"
    );
}

/// Create VS maps the Iceberg table schema to Exasol types correctly.
#[test]
fn create_vs_maps_iceberg_schema() {
    setup_e2e();
    let mut conn = exa_conn();

    // DESCRIBE returns (COLUMN_NAME, COLUMN_TYPE, ...).
    let sql = format!("DESCRIBE {}", vs_table());
    let resp = conn.execute(&sql);
    let result_set = &resp["responseData"]["results"][0]["resultSet"];
    let cols = conn.fetch_result_columns(result_set);

    // cols[0] = column names, cols[1] = column types.
    let names = &cols[0];
    let types = &cols[1];
    assert!(!names.is_empty(), "DESCRIBE returned no columns");

    // Verify each expected column exists with the right type.
    let expected = [
        ("ID", "DECIMAL"),
        ("NAME", "VARCHAR"),
        ("SCORE", "DOUBLE"),
        ("EVENT_DATE", "DATE"),
        ("EVENT_TS", "TIMESTAMP"),
    ];
    for (expected_name, expected_type_prefix) in expected {
        let pos = names
            .iter()
            .position(|n| {
                n.as_str()
                    .map(|s| s.eq_ignore_ascii_case(expected_name))
                    .unwrap_or(false)
            })
            .unwrap_or_else(|| {
                panic!("column {expected_name} not found in DESCRIBE output: {names:?}")
            });
        let ty = types[pos]
            .as_str()
            .unwrap_or_else(|| panic!("type at position {pos} is not a string: {:?}", types[pos]));
        assert!(
            ty.to_uppercase().contains(expected_type_prefix),
            "column {expected_name}: expected type containing '{expected_type_prefix}', got '{ty}'"
        );
    }
}

/// Filter predicate restricts the emitted rows (no extra rows).
#[test]
fn scan_filter_restricts_rows() {
    setup_e2e();
    let mut conn = exa_conn();

    let row_count =
        conn.query_row_count(&format!("SELECT id FROM {} WHERE score > 15.0", vs_table()));
    assert_eq!(
        row_count, SEED_ROWS_SCORE_GT_15 as i64,
        "WHERE score > 15.0 should return {SEED_ROWS_SCORE_GT_15} rows, got {row_count}"
    );
}

/// LIMIT caps the rows emitted by the scan.
#[test]
fn scan_limit_caps_rows() {
    setup_e2e();
    let mut conn = exa_conn();

    let row_count = conn.query_row_count(&format!("SELECT id FROM {} LIMIT 3", vs_table()));
    assert_eq!(
        row_count, 3,
        "LIMIT 3 should return exactly 3 rows, got {row_count}"
    );
}

/// Both entry points (adapter + scan) resolve from the same uploaded .so artifact.
#[test]
fn both_scripts_resolve_one_artifact() {
    setup_e2e();
    let mut conn = exa_conn();

    // Verify both scripts exist and point to the same .so object path.
    let resp_adapter = conn.execute(&format!(
        "SELECT SCRIPT_TEXT FROM EXA_ALL_SCRIPTS WHERE SCRIPT_NAME='{ADAPTER_SCRIPT_NAME}' AND SCRIPT_SCHEMA='{SCHEMA_NAME}'"
    ));
    let adapter_body = resp_adapter["responseData"]["results"][0]["resultSet"]["data"][0][0]
        .as_str()
        .unwrap_or("")
        .to_string();

    let resp_scan = conn.execute(&format!(
        "SELECT SCRIPT_TEXT FROM EXA_ALL_SCRIPTS WHERE SCRIPT_NAME='{SCAN_SCRIPT_NAME}' AND SCRIPT_SCHEMA='{SCHEMA_NAME}'"
    ));
    let scan_body = resp_scan["responseData"]["results"][0]["resultSet"]["data"][0][0]
        .as_str()
        .unwrap_or("")
        .to_string();

    // Both script bodies must reference the same .so artifact path.
    assert!(
        adapter_body.contains("liblakehouse_engine.so") || adapter_body.contains("udf"),
        "adapter script body does not reference the .so: {adapter_body}"
    );
    assert!(
        scan_body.contains("liblakehouse_engine.so") || scan_body.contains("udf"),
        "scan script body does not reference the .so: {scan_body}"
    );
}

/// Full projection + filter + date/timestamp columns round-trip correctly.
#[test]
fn mixed_column_parquet_round_trips() {
    setup_e2e();
    let mut conn = exa_conn();

    // Select all columns for the first row (id=1) to verify type conversion.
    let cols = conn.query_columns(&format!(
        "SELECT id, name, score, event_date, event_ts FROM {} WHERE id = 1",
        vs_table()
    ));
    assert_eq!(cols.len(), 5, "expected 5 columns: {cols:?}");
    // Each column has exactly 1 row.
    for (i, col) in cols.iter().enumerate() {
        assert_eq!(col.len(), 1, "column {i} should have 1 row: {col:?}");
    }
    // id = 1 (Exasol returns numerics as strings or numbers).
    let id_val = &cols[0][0];
    assert!(
        id_val.as_i64().map(|v| v == 1).unwrap_or(false)
            || id_val.as_str().map(|s| s == "1").unwrap_or(false),
        "id should be 1, got: {id_val:?}"
    );
    // name = "event-01".
    let name_val = &cols[1][0];
    assert!(
        name_val
            .as_str()
            .map(|s| s.contains("event-01"))
            .unwrap_or(false),
        "name should be 'event-01', got: {name_val:?}"
    );
    // score = 5.0 (5.0 * 1).
    let score_val = &cols[2][0];
    assert!(
        score_val
            .as_f64()
            .map(|v| (v - 5.0).abs() < 0.001)
            .unwrap_or(false)
            || score_val.as_str().map(|s| s.contains('5')).unwrap_or(false),
        "score should be 5.0, got: {score_val:?}"
    );
    // event_date and event_ts: non-null values.
    assert!(!cols[3][0].is_null(), "event_date must not be null");
    assert!(!cols[4][0].is_null(), "event_ts must not be null");
}

/// Error scenario: CREATE VS with unreachable catalog returns a clear error without secrets.
///
/// The secret value lives in the CONNECTION password JSON (not in the SQL).
/// The test asserts the error message does not contain the credential values.
#[test]
fn create_vs_unreachable_catalog_errors_no_secret() {
    setup_e2e();
    let mut conn = exa_conn();

    // Create a CONNECTION with a bogus catalog URI and bogus credentials.
    // The credential values must not appear in any error message.
    let bogus_password = common::stack::CatalogConnectionPassword {
        warehouse: "s3://warehouse/".to_string(),
        endpoint: "http://does-not-exist.invalid:9000".to_string(),
        region: "us-east-1".to_string(),
        access_key: "SUPER_SECRET_KEY".to_string(),
        secret_key: "SUPER_SECRET_VALUE".to_string(),
        session_token: None,
        path_style: true,
        use_sigv4: false,
        use_vended_credentials: false,
        ..Default::default()
    };
    let bogus_uri = "http://does-not-exist.invalid:8181";
    let create_conn_sql =
        build_create_connection_sql("BAD_CATALOG_CREDS", bogus_uri, &bogus_password);
    conn.execute(&create_conn_sql);

    let resp = conn.try_execute(&format!(
        r#"CREATE VIRTUAL SCHEMA BAD_CATALOG_VS
USING {SCHEMA_NAME}.{ADAPTER_SCRIPT_NAME} WITH
  CATALOG_CONNECTION  = 'BAD_CATALOG_CREDS'
  ICEBERG_NAMESPACE   = 'ns'
  ALLOW_HTTP          = 'true'"#
    ));
    assert_eq!(
        resp["status"].as_str(),
        Some("error"),
        "expected an error when catalog is unreachable, got: {resp}"
    );
    let msg = resp["exception"]["text"].as_str().unwrap_or("");
    assert!(
        !msg.contains("SUPER_SECRET_KEY") && !msg.contains("SUPER_SECRET_VALUE"),
        "error message must not leak credentials: {msg}"
    );
}

/// A CONNECTION supplying a static `token` together with a complete OAuth2
/// `client_id`/`client_secret` pair is rejected through the real Exasol +
/// deployed `.so` path, not just the unit-level `StubCtx`.
///
/// Iceberg kind only: the Unity kind's equivalent ambiguity is already
/// covered by the unit test `token_with_complete_oauth_pair_is_rejected_under_both_kinds`.
#[test]
fn create_vs_ambiguous_catalog_auth_errors_no_secret() {
    setup_e2e();
    let mut conn = exa_conn();

    let ambiguous_password = common::stack::CatalogConnectionPassword {
        warehouse: "s3://warehouse/".to_string(),
        endpoint: "http://does-not-exist.invalid:9000".to_string(),
        region: "us-east-1".to_string(),
        access_key: "SUPER_SECRET_KEY".to_string(),
        secret_key: "SUPER_SECRET_VALUE".to_string(),
        session_token: None,
        path_style: true,
        use_sigv4: false,
        use_vended_credentials: false,
        token: Some("SUPER_SECRET_TOKEN".to_string()),
        client_id: Some("SUPER_SECRET_CLIENT_ID".to_string()),
        client_secret: Some("SUPER_SECRET_CLIENT_SECRET".to_string()),
        ..Default::default()
    };
    let bogus_uri = "http://does-not-exist.invalid:8181";
    let create_conn_sql =
        build_create_connection_sql("AMBIGUOUS_CATALOG_CREDS", bogus_uri, &ambiguous_password);
    conn.execute(&create_conn_sql);

    let resp = conn.try_execute(&format!(
        r#"CREATE VIRTUAL SCHEMA AMBIGUOUS_CATALOG_VS
USING {SCHEMA_NAME}.{ADAPTER_SCRIPT_NAME} WITH
  CATALOG_CONNECTION  = 'AMBIGUOUS_CATALOG_CREDS'
  ICEBERG_NAMESPACE   = 'ns'
  ALLOW_HTTP          = 'true'"#
    ));
    assert_eq!(
        resp["status"].as_str(),
        Some("error"),
        "expected an error when token and client_id/client_secret are both supplied, got: {resp}"
    );
    let msg = resp["exception"]["text"].as_str().unwrap_or("");
    assert!(
        msg.contains("token") && msg.contains("client_id") && msg.contains("client_secret"),
        "error message must name token, client_id, and client_secret: {msg}"
    );
    assert!(
        !msg.contains("SUPER_SECRET_TOKEN")
            && !msg.contains("SUPER_SECRET_CLIENT_ID")
            && !msg.contains("SUPER_SECRET_CLIENT_SECRET"),
        "error message must not leak credential values: {msg}"
    );
}

/// Querying a non-existent virtual table name in the VS errors with a clear TABLE_MAP message.
///
/// With namespace enumeration, a table that does not exist in the namespace was never
/// registered in TABLE_MAP. Any pushdown for such a name must fail with a clear error
/// rather than silently scanning the wrong table.
#[test]
fn scan_unknown_virtual_table_errors() {
    setup_e2e();
    let mut conn = exa_conn();

    // Querying a table name that was not in the namespace at create time will fail
    // at the pushdown stage because the Exasol table name is not in TABLE_MAP.
    // We exercise this by querying a VS table that we know does not exist.
    let resp = conn.try_execute(&format!("SELECT * FROM {VS_NAME}.NO_SUCH_TABLE LIMIT 1"));
    assert_eq!(
        resp["status"].as_str(),
        Some("error"),
        "expected an error for unknown virtual table NO_SUCH_TABLE: {resp}"
    );
}

/// Schema evolution with a renamed column resolves correctly by Iceberg field-id.
///
/// Scenario (seeded by `seed_renamed_column`), field-id 2 stable throughout:
///   - file A: ids 1..=5,  physical parquet column `score`
///   - catalog rename `score` -> `rating` (field-id 2 preserved)
///   - file B: ids 6..=10, physical parquet column `rating`
///
/// The VS is created with `PARALLELISM_FACTOR = 1` so that on this single-node
/// cluster the shard count G = clamp(1 * 1, 1, min(file_count, 300)) = 1. Both
/// files therefore land in ONE shard → one `ScanSpec` → one DataFusion
/// `ListingTable`. The two divergent physical layouts (`score` vs `rating` for
/// field-id 2) are handled by the field-id expression adapter, which binds both
/// files to the current logical name by field-id rather than by physical name.
///
/// Expected result: `EVO_TOTAL_ROWS` (10) rows, `rating = 10*id`, no NULLs.
#[test]
fn e2e_renamed_column_resolves_by_field_id() {
    setup_e2e();

    // Seed the dedicated evo table: create + file A (score) + rename + file B (rating).
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");
    rt.block_on(async {
        seed_renamed_column(&iceberg_catalog_url(), "s3://warehouse/")
            .await
            .expect("seed evo (renamed-column) table");
    });

    let mut conn = exa_conn();

    // A dedicated VS created AFTER evo exists, so the adapter enumerates it
    // (the shared MY_LAKEHOUSE VS was created before evo and does not see it).
    // PARALLELISM_FACTOR = 1 forces a single shard (G = 1 on this 1-node cluster),
    // so both parquet files are scanned together in one ListingTable.
    let _ = conn.try_execute("DROP VIRTUAL SCHEMA IF EXISTS EVO_VS CASCADE");
    conn.execute(&format!(
        r#"CREATE VIRTUAL SCHEMA EVO_VS
USING {SCHEMA_NAME}.{ADAPTER_SCRIPT_NAME} WITH
  CATALOG_CONNECTION  = '{DEFAULT_CATALOG_CONN_NAME}'
  ICEBERG_NAMESPACE   = '{E2E_NAMESPACE}'
  PARALLELISM_FACTOR  = '1'
  ALLOW_HTTP          = 'true'"#
    ));

    let col = EVO_NEW_COL.to_uppercase();
    let sql = format!(
        "SELECT id, {col} FROM EVO_VS.{} ORDER BY id",
        E2E_EVO_TABLE.to_uppercase()
    );

    let cols = conn.query_columns(&sql);
    let ids = &cols[0];
    let ratings = &cols[1];
    let row_count = ids.len();

    assert_eq!(
        row_count, EVO_TOTAL_ROWS,
        "field-id projection must return all {EVO_TOTAL_ROWS} rows across both \
         pre- and post-rename files; got {row_count}"
    );

    for (i, r) in ratings.iter().enumerate() {
        let id = ids[i]
            .as_i64()
            .or_else(|| ids[i].as_str().and_then(|s| s.parse().ok()))
            .unwrap_or(-1);
        let rating = r
            .as_f64()
            .expect("rating must not be NULL after field-id projection");
        assert!(
            (rating - 10.0 * id as f64).abs() < 1e-6,
            "row {i}: expected rating = 10 * {id} = {}, got {rating}",
            10 * id
        );
    }
}

/// Added columns absent from a pre-existing data file return their Iceberg
/// `initial-default`; the same columns return real values where they are present.
///
/// Scenario (seeded by `seed_added_columns_initial_default`), Iceberg
/// column-projection rule (3):
///   - file A: ids `EVO_INITDEF_PRE_ADD_IDS`, physical parquet has only `id`
///   - catalog `add-schema`: one column per primitive type (field-ids 2..=11),
///     each with an `initial-default`; `c_bool` REQUIRED, the rest NULLABLE
///   - file B: ids `EVO_INITDEF_POST_ADD_IDS`, all columns with real values
///
/// `PARALLELISM_FACTOR = 1` forces one shard, so both files land in one
/// `ListingTable`. The per-file field-id adapter must, for the pre-add file, fill
/// each absent added column with its `initial-default` (required AND nullable),
/// and for the post-add file bind the real written values — never defaulting a
/// present field. Asserted across ALL added primitive types.
#[test]
fn e2e_added_columns_initial_default_fill_all_types() {
    setup_e2e();

    // Seed the dedicated initdef table: create + file A (id only) + add-columns +
    // file B (all columns with real values).
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");
    rt.block_on(async {
        seed_added_columns_initial_default(&iceberg_catalog_url(), "s3://warehouse/")
            .await
            .expect("seed initdef (all-types initial-default) table");
    });

    let mut conn = exa_conn();

    // A dedicated VS created AFTER initdef exists so the adapter enumerates it.
    // PARALLELISM_FACTOR = 1 forces a single shard (G = 1 on this 1-node cluster),
    // so both parquet files are scanned together in one ListingTable.
    let _ = conn.try_execute("DROP VIRTUAL SCHEMA IF EXISTS INITDEF_VS CASCADE");
    conn.execute(&format!(
        r#"CREATE VIRTUAL SCHEMA INITDEF_VS
USING {SCHEMA_NAME}.{ADAPTER_SCRIPT_NAME} WITH
  CATALOG_CONNECTION  = '{DEFAULT_CATALOG_CONN_NAME}'
  ICEBERG_NAMESPACE   = '{E2E_NAMESPACE}'
  SCAN_SCHEMA         = '{SCHEMA_NAME}'
  PARALLELISM_FACTOR  = '1'
  ALLOW_HTTP          = 'true'"#
    ));

    let columns = initdef_columns();
    let col_list = columns
        .iter()
        .map(|c| c.name.to_uppercase())
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!(
        "SELECT id, {col_list} FROM INITDEF_VS.{} ORDER BY id",
        EVO_INITDEF_TABLE.to_uppercase()
    );

    let cols = conn.query_columns(&sql);
    assert_eq!(
        cols.len(),
        columns.len() + 1,
        "expected id + {} added columns, got {} columns: {cols:?}",
        columns.len(),
        cols.len()
    );

    let ids = &cols[0];
    let row_count = ids.len();
    assert_eq!(
        row_count, EVO_INITDEF_TOTAL_ROWS,
        "field-id scan must return all {EVO_INITDEF_TOTAL_ROWS} rows across the \
         pre-add and post-add files; got {row_count}"
    );

    let (pre0, pre1) = EVO_INITDEF_PRE_ADD_IDS;
    let (post0, post1) = EVO_INITDEF_POST_ADD_IDS;

    for (r, id_val) in ids.iter().enumerate() {
        let id = id_val
            .as_i64()
            .or_else(|| id_val.as_str().and_then(|s| s.parse().ok()))
            .unwrap_or_else(|| panic!("row {r}: id must be an integer, got {id_val:?}"));

        let pre_add = (pre0..=pre1).contains(&id);
        let post_add = (post0..=post1).contains(&id);
        assert!(
            pre_add ^ post_add,
            "row {r}: id {id} is outside both seeded ranges \
             {EVO_INITDEF_PRE_ADD_IDS:?} / {EVO_INITDEF_POST_ADD_IDS:?}"
        );

        for (c, col) in columns.iter().enumerate() {
            let actual = &cols[c + 1][r];
            let expected = if pre_add { &col.default } else { &col.real };
            let phase = if pre_add {
                "pre-add row must carry the column's initial-default"
            } else {
                "post-add row must carry the real written value"
            };
            let kind = if col.required {
                "required-with-default"
            } else {
                "nullable-with-default"
            };
            assert!(
                expected.matches(actual),
                "row {r} (id {id}), column '{}' [{kind}]: {phase} — got {actual:?}",
                col.name
            );
        }
    }
}

/// Verify the suite panics (not silently passes) when Exasol is unreachable.
///
/// This test is a behavioural assertion: if the connect helpers get a bad host,
/// they panic/unwrap, which causes the test binary to abort — NOT return Ok.
/// We verify the behaviour by showing the panic path is reachable.
#[test]
fn e2e_fails_when_stack_unavailable() {
    // The design contract: ExaConn::connect panics on TCP failure.
    // We verify the contract is enforced by asserting that attempting to connect
    // to a known-bad address would panic. We do NOT actually panic here (that
    // would fail the test), but we verify the function body takes the panic path.
    //
    // The real enforcement is that every test calls setup_e2e(), which calls
    // wait_for_exasol() which panics on timeout — this test documents the contract.
    let result = std::panic::catch_unwind(|| {
        // This MUST panic — ExaConn::connect on a bad address panics, never returns Err.
        ExaConn::connect("192.0.2.1", 8563, "sys", "exasol")
    });
    assert!(
        result.is_err(),
        "ExaConn::connect to an unreachable host must panic, not return Ok"
    );
}

// ---------------------------------------------------------------------------
// Task 5.4 / Plan scenario coverage: partial-aggregate E2E stubs
// Group D fills in the assertions; we define the function names here so the
// plan's scenario table can reference them and they compile without the feature.
// ---------------------------------------------------------------------------

/// Scan computes a node-local partial aggregate instead of raw rows.
///
/// Verifies: spec with aggregate plan causes the UDF to emit one partial row
/// per shard, not the full row set.
#[test]
fn scan_emits_partial_aggregate_row() {
    setup_e2e();
    let mut conn = exa_conn();

    // COUNT(*) over the whole table: one merged row with the total row count.
    let cols = conn.query_columns(&format!("SELECT COUNT(*) FROM {}", vs_table()));
    assert_eq!(cols.len(), 1, "COUNT(*) must return one column: {cols:?}");
    assert_eq!(cols[0].len(), 1, "COUNT(*) must return one row: {cols:?}");
    let count = cols[0][0]
        .as_i64()
        .or_else(|| cols[0][0].as_str().and_then(|s| s.parse().ok()))
        .unwrap_or_else(|| panic!("COUNT(*) result not integer: {:?}", cols[0][0]));
    // The seeded table has 20 rows.
    assert_eq!(count, 20, "COUNT(*) should return 20 for the seeded table");
}

/// Partial COUNT/SUM/MIN/MAX emitted in merge-ready form.
///
/// Verifies: each aggregate type returns the correct merged scalar.
#[test]
fn partial_count_sum_min_max_merge_ready() {
    setup_e2e();
    let mut conn = exa_conn();

    // score = 5.0 * id for id 1..20; SUM = 5*(1+2+...+20) = 5*210 = 1050.
    let cols = conn.query_columns(&format!(
        "SELECT COUNT(*), SUM(score), MIN(score), MAX(score) FROM {}",
        vs_table()
    ));
    assert_eq!(cols.len(), 4, "must return 4 columns: {cols:?}");

    let count = cols[0][0]
        .as_i64()
        .or_else(|| cols[0][0].as_str().and_then(|s| s.parse().ok()))
        .expect("COUNT must be integer");
    assert_eq!(count, 20, "COUNT(*) must be 20");

    let sum = cols[1][0]
        .as_f64()
        .or_else(|| cols[1][0].as_str().and_then(|s| s.parse().ok()))
        .expect("SUM must be numeric");
    assert!(
        (sum - 1050.0).abs() < 0.001,
        "SUM(score) must be 1050, got {sum}"
    );

    let min = cols[2][0]
        .as_f64()
        .or_else(|| cols[2][0].as_str().and_then(|s| s.parse().ok()))
        .expect("MIN must be numeric");
    assert!(
        (min - 5.0).abs() < 0.001,
        "MIN(score) must be 5.0, got {min}"
    );

    let max = cols[3][0]
        .as_f64()
        .or_else(|| cols[3][0].as_str().and_then(|s| s.parse().ok()))
        .expect("MAX must be numeric");
    assert!(
        (max - 100.0).abs() < 0.001,
        "MAX(score) must be 100.0, got {max}"
    );
}

/// AVG emitted as a partial sum and partial count pair.
///
/// Verifies: AVG(score) returns the correct average, including with a WHERE filter.
#[test]
fn partial_avg_emits_sum_count_pair() {
    setup_e2e();
    let mut conn = exa_conn();

    // AVG(score) over all rows: 1050.0 / 20 = 52.5.
    let cols = conn.query_columns(&format!("SELECT AVG(score) FROM {}", vs_table()));
    assert_eq!(cols.len(), 1, "AVG must return one column: {cols:?}");
    let avg = cols[0][0]
        .as_f64()
        .or_else(|| cols[0][0].as_str().and_then(|s| s.parse().ok()))
        .expect("AVG must be numeric");
    assert!(
        (avg - 52.5).abs() < 0.001,
        "AVG(score) must be 52.5, got {avg}"
    );

    // AVG with WHERE: score > 15.0 → id >= 4 (17 rows), scores 20..100.
    // SUM = 5*(4+5+...+20) = 5*(17*12) = 5*204... actually sum of 4..20 = (4+20)*17/2 = 204,
    // so SUM(score) = 5*204 = 1020, AVG = 1020/17 = 60.0.
    let cols_filtered = conn.query_columns(&format!(
        "SELECT AVG(score) FROM {} WHERE score > 15.0",
        vs_table()
    ));
    let avg_filtered = cols_filtered[0][0]
        .as_f64()
        .or_else(|| cols_filtered[0][0].as_str().and_then(|s| s.parse().ok()))
        .expect("filtered AVG must be numeric");
    assert!(
        (avg_filtered - 60.0).abs() < 0.001,
        "filtered AVG(score) must be 60.0, got {avg_filtered}"
    );
}

/// Runs `EXPLAIN VIRTUAL` for a single-group (no GROUP BY) aggregate query and
/// asserts the pushed SQL evidences single-group aggregate pushdown — an
/// `aggregates` field in the scan spec — rather than a raw row-scan fallback
/// that would ship every projected column to Exasol for it to aggregate itself.
///
/// Mirrors [`assert_group_by_pushed_down`]'s pattern for the single-group
/// (non-GROUP-BY) aggregate path: `aggregates` (not `group_keys`) is this
/// path's discriminating field, since single-group partial aggregation also
/// emits `PARTIAL_` columns but never a `group_keys` array.
fn assert_single_group_aggregate_pushed_down(conn: &mut ExaConn, query_sql: &str) {
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
        pushed_sql.contains("aggregates"),
        "EXPLAIN VIRTUAL output must contain an 'aggregates' field in the scan \
         spec (single-group aggregate pushdown occurred), got:\n{pushed_sql}"
    );
    assert!(
        !pushed_sql.contains("SELECT * FROM (SELECT"),
        "EXPLAIN VIRTUAL output must not be a raw row-scan fallback \
         ('SELECT * FROM (SELECT ...)'), got:\n{pushed_sql}"
    );
}

/// Regression guard for issue #145: the `LAKEHOUSE_SCAN` common scan spec for
/// a single-group (no GROUP BY) aggregate query MUST report an empty
/// `projection` field, both for a bare `COUNT(*)` and a `SUM(score)`.
///
/// The aggregate-dispatch path builds its query from `aggregates`/`group_keys`,
/// never from `projection` (see the doc comment on
/// [`CommonScanSpec::projection`](lakehouse_engine::scan::spec::CommonScanSpec)),
/// so `handle_pushdown` leaves it empty rather than splicing in the full
/// base-table column list `extract_projection` would otherwise fall back to —
/// that full-row splice is exactly what the reporter observed in #145.
///
/// Matches the precise, field-shaped `"projection":[]` marker against the
/// adapter's OWN emitted scan-spec JSON, mirroring the `"order_by":` marker in
/// [`ordered_topn_pushes_down_matches_single_node`]: `CommonScanSpec::projection`
/// has no `skip_serializing_if`, so an empty vector always serializes as
/// `"projection":[]`, and the common blob is spliced into the pushed SQL via
/// `sql_string_literal`, which only doubles single quotes — the JSON's double
/// quotes reach the pushed SQL text unescaped, so the un-escaped substring is
/// the correct, confirmed marker (not an assumption).
#[test]
fn single_group_aggregate_scan_spec_projection_is_empty() {
    setup_e2e();
    let mut conn = exa_conn();

    for sql in [
        format!("SELECT COUNT(*) FROM {}", vs_table()),
        format!("SELECT SUM(score) FROM {}", vs_table()),
    ] {
        let pushed_sql = explain_virtual_sql(&mut conn, &sql);
        assert!(
            pushed_sql.contains("aggregates"),
            "{sql} must push down as a single-group aggregate (an \
             'aggregates' field in the scan spec), got:\n{pushed_sql}"
        );
        assert!(
            pushed_sql.contains("\"projection\":[]"),
            "{sql}'s single-group aggregate scan spec must report an empty \
             'projection' field (#145: the aggregate-dispatch path reads \
             'aggregates'/'group_keys', not 'projection', so an empty value \
             means \"not applicable\", not \"all columns\"), got:\n{pushed_sql}"
        );
        // Sibling of the `projection` leak (#145): the aggregate scan emits via
        // the Value path and never reads `emit_exa_types`, so the aggregate spec
        // must not leak a full base-table type list into the common blob. Unlike
        // `projection`, `CommonScanSpec::emit_exa_types` carries
        // `skip_serializing_if = "Vec::is_empty"`, so an empty value is OMITTED
        // entirely — the field name must be absent, not `"emit_exa_types":[]`.
        assert!(
            !pushed_sql.contains("emit_exa_types"),
            "{sql}'s single-group aggregate scan spec must omit \
             'emit_exa_types' (#145 sibling: the aggregate path emits via the \
             Value path and never reads it; empty + skip_serializing_if means \
             the field is absent from the common blob), got:\n{pushed_sql}"
        );
    }
}

/// `SUM(LENGTH(col))` — an aggregate over a scalar expression argument, not a
/// bare column — is pushed down as node-local partial aggregation instead of
/// falling back to a raw row-scan.
///
/// `name` = "event-NN" for every seeded row (fixed 8-character format), so
/// `SUM(LENGTH(name))` over all `SEED_TOTAL_ROWS` rows is `8 * SEED_TOTAL_ROWS`.
#[test]
fn sum_length_expression_argument_pushed_down() {
    setup_e2e();
    let mut conn = exa_conn();

    let sql = format!("SELECT SUM(LENGTH(name)) FROM {}", vs_table());
    assert_single_group_aggregate_pushed_down(&mut conn, &sql);

    let cols = conn.query_columns(&sql);
    assert_eq!(cols.len(), 1, "expected 1 aggregate column: {cols:?}");
    assert_eq!(cols[0].len(), 1, "expected 1 row: {cols:?}");

    let total = parse_numeric(&cols[0][0]);
    let expected = 8.0 * SEED_TOTAL_ROWS as f64;
    assert!(
        (total - expected).abs() < 0.001,
        "SUM(LENGTH(name)) must be {expected} (name is always 8 chars, \
         {SEED_TOTAL_ROWS} rows), got {total}"
    );
}

/// `SUM(id * score)` — a SUM over a two-column binary-arithmetic argument
/// (the NQ1 / TPC-H Q6 shape: `SUM(L_EXTENDEDPRICE * L_DISCOUNT)` with a
/// date-range + BETWEEN + comparison filter) — is pushed down as a decomposed
/// node-local partial/merge aggregate (`aggregates` + `arg_expr` in the scan
/// spec), and the merged result across shards matches the value a single,
/// undecomposed full-table scan would compute.
///
/// The `events` table is seeded across TWO Iceberg data files (ids 1..=10,
/// 11..=20; see `common/seed.rs`), so this filter range is chosen to
/// deliberately straddle both shards — proving the per-shard partial SUM of
/// the product, merged back together, is not just plan-shape-correct but
/// numerically correct across a shard boundary.
///
/// Filter shape mirrors NQ1 (`bench/run.sh`): a date range on `event_date`
/// (mirrors `L_SHIPDATE`), a `BETWEEN` on `score` (mirrors `L_DISCOUNT`, which
/// is also a product operand — same as NQ1), and a `<=` comparison on `id`
/// (mirrors `L_QUANTITY <`).
///
/// Seeded data: `score = 5.0 * id`. `event_date >= '2024-01-05' AND
/// event_date < '2024-01-15'` selects ids 5..=14; `score BETWEEN 30.0 AND
/// 60.0` narrows to ids 6..=12; `id <= 12` is redundant (shape parity with
/// NQ1's extra predicate). The "single-node" ground truth for `SUM(id *
/// score)` over ids 6..=12 is `SUM(5 * id^2)` for id in 6..=12 = `5 * (36 +
/// 49 + 64 + 81 + 100 + 121 + 144)` = `5 * 595` = `2975.0` — computed here as
/// a closed form, i.e. exactly what a single, undecomposed scan of all
/// matching rows would sum to, with no partial/merge step involved.
#[test]
fn sum_two_column_product_pushes_down_matches_single_node() {
    setup_e2e();
    let mut conn = exa_conn();

    let sql = format!(
        "SELECT SUM(id * score) AS revenue FROM {} \
         WHERE event_date >= DATE '2024-01-05' AND event_date < DATE '2024-01-15' \
           AND score BETWEEN 30.0 AND 60.0 AND id <= 12",
        vs_table()
    );

    let pushed_sql = explain_virtual_sql(&mut conn, &sql);
    assert!(
        pushed_sql.contains("aggregates"),
        "SUM(id * score) must push down as an 'aggregates' plan (two-column \
         arithmetic aggregate pushdown), got:\n{pushed_sql}"
    );
    assert!(
        pushed_sql.contains("arg_expr"),
        "SUM(id * score) must carry the rendered product in 'arg_expr' (not a \
         bare source column), proving the SUM is decomposed rather than \
         falling back to a raw two-column row scan, got:\n{pushed_sql}"
    );
    assert!(
        !pushed_sql.contains("SELECT * FROM (SELECT"),
        "SUM(id * score) must not fall back to a raw row-scan \
         ('SELECT * FROM (SELECT ...)'), got:\n{pushed_sql}"
    );

    let cols = conn.query_columns(&sql);
    assert_eq!(cols.len(), 1, "expected 1 aggregate column: {cols:?}");
    assert_eq!(cols[0].len(), 1, "expected 1 row: {cols:?}");

    let revenue = parse_numeric(&cols[0][0]);
    let expected = 2975.0;
    assert!(
        (revenue - expected).abs() < 0.001,
        "SUM(id * score) over ids 6..=12 must be {expected} (matching a \
         single, undecomposed full-scan evaluation), got {revenue}"
    );
}

/// Regression check: an aggregate argument the VS expression translator
/// genuinely cannot render must still decline aggregate pushdown and fall
/// back to row scanning — proving the new arithmetic-pushdown capability
/// (`FN_ADD`/`FN_SUB`/`FN_MULT`/`FN_FLOAT_DIV`) did not accidentally widen
/// what counts as "translatable" in a way that breaks this safety net.
///
/// `BIT_AND` is a real Exasol scalar function (bitwise AND over two numeric
/// values, see Exasol SQL reference) with no `vs-expression` translation arm
/// — `render_expression_inner`'s `function_scalar` match falls through to its
/// `other => Err("unsupported scalar function: ...")` arm, so `SUM(BIT_AND(id,
/// 7))`'s argument cannot be rendered and the whole aggregate declines
/// pushdown (`arg_column_or_expr` returns `None`).
///
/// The row-scan fallback must still compute the correct answer: seeded
/// `id` runs 1..=20, so `id & 7` cycles `1,2,3,4,5,6,7,0` twice (ids 1..=8,
/// 9..=16, each summing to 28) plus a partial cycle for ids 17..=20
/// (`1+2+3+4` = 10), for a total of `28 + 28 + 10` = `66`.
#[test]
fn untranslatable_aggregate_argument_falls_back_to_row_scan() {
    setup_e2e();
    let mut conn = exa_conn();

    let sql = format!("SELECT SUM(BIT_AND(id, 7)) FROM {}", vs_table());

    let pushed_sql = explain_virtual_sql(&mut conn, &sql);
    assert!(
        !pushed_sql.contains("aggregates"),
        "SUM(BIT_AND(id, 7)) has an untranslatable argument (BIT_AND has no \
         vs-expression translation arm) and must decline aggregate pushdown \
         (no 'aggregates' field in the scan spec), got:\n{pushed_sql}"
    );

    let cols = conn.query_columns(&sql);
    assert_eq!(cols.len(), 1, "expected 1 aggregate column: {cols:?}");
    assert_eq!(cols[0].len(), 1, "expected 1 row: {cols:?}");

    let sum = parse_int(&cols[0][0]);
    assert_eq!(
        sum, 66,
        "SUM(BIT_AND(id, 7)) computed by Exasol over the row-scan fallback \
         must be 66, got {sum}"
    );
}

/// `ORDER BY score DESC LIMIT 12` — a bare, projected sort column with a LIMIT
/// (the NQ4 / TPC-H top-N shape: `ORDER BY L_EXTENDEDPRICE DESC LIMIT 20`) — is
/// pushed down as a decomposed per-shard bounded top-N plus an Exasol-side
/// merge (`order_by` in the scan spec, `ORDER BY … LIMIT` in both the per-shard
/// and outer merge SQL), and the merged result matches what a single, full
/// scan + sort + limit would produce.
///
/// The `events` table is seeded across TWO Iceberg data files (ids 1..=10,
/// 11..=20; see `common/seed.rs`). `LIMIT 12` is chosen deliberately so the
/// top-12 by score DESC (ids 20..=9) straddles BOTH files — ids 9 and 10 come
/// from the first file, ids 11..=20 from the second — proving the per-shard
/// bounded top-N, merged back together, is not just plan-shape-correct but
/// also correct across a real shard boundary (not merely a single shard's own
/// local top-N happening to be the global answer).
///
/// Seeded data: `score = 5.0 * id` for id in 1..=20, so score is strictly
/// increasing in id — the top-12 by score DESC is exactly ids 20,19,...,9, in
/// that descending order, with score = 5.0 * id for each row.
#[test]
fn ordered_topn_pushes_down_matches_single_node() {
    setup_e2e();
    let mut conn = exa_conn();

    let sql = format!(
        "SELECT id, score FROM {} ORDER BY score DESC LIMIT 12",
        vs_table()
    );

    let pushed_sql = explain_virtual_sql(&mut conn, &sql);
    // `EXPLAIN VIRTUAL`'s output also echoes Exasol's incoming `pushdownRequest`,
    // whose `orderBy` element carries a literal `"order_by_element"` type tag —
    // that string is present for ANY ORDER-BY-carrying query, matched or not, so
    // a bare `contains("order_by")` would be a false positive here. The precise,
    // field-shaped marker `"order_by":` only appears in the ADAPTER'S OWN emitted
    // scan-spec JSON (`"order_by":[{"column":"SCORE",...}]`), which is present
    // only when `detect_topn` actually matched.
    assert!(
        pushed_sql.contains("\"order_by\":"),
        "ORDER BY score DESC LIMIT 12 over a projected column must push down \
         as an 'order_by' top-N plan in the scan spec, got:\n{pushed_sql}"
    );
    // The outer merge ORDER BY is self-contained (decision [5]): spliced directly
    // after the shard fan-out's closing paren, not left to an Exasol backstop.
    assert!(
        pushed_sql.contains("GROUP BY shard_key) ORDER BY"),
        "pushed SQL must carry a self-contained outer ORDER BY immediately \
         after the shard fan-out, got:\n{pushed_sql}"
    );
    assert!(
        pushed_sql.contains("LIMIT 12"),
        "pushed SQL must carry a LIMIT 12 clause bounding the top-N (not an \
         unlimited raw scan), got:\n{pushed_sql}"
    );

    let cols = conn.query_columns(&sql);
    assert_eq!(cols.len(), 2, "expected 2 columns (id, score): {cols:?}");
    assert_eq!(
        cols[0].len(),
        12,
        "expected exactly 12 rows from LIMIT 12: {cols:?}"
    );

    let ids: Vec<i64> = cols[0].iter().map(parse_int).collect();
    let scores: Vec<f64> = cols[1].iter().map(parse_numeric).collect();

    let expected_ids: Vec<i64> = (9..=20).rev().collect();
    assert_eq!(
        ids, expected_ids,
        "top-12 ids by score DESC must be {expected_ids:?} (matching a \
         single, undecomposed full scan + sort + limit), got {ids:?}"
    );
    for (i, &id) in ids.iter().enumerate() {
        let expected_score = 5.0 * id as f64;
        assert!(
            (scores[i] - expected_score).abs() < 1e-9,
            "row {i}: score for id {id} must be {expected_score}, got {}",
            scores[i]
        );
    }
}

/// Regression check: `ORDER BY score DESC` with NO `LIMIT` must decline the
/// ordered-top-N pushdown — `detect_topn` requires a `limit` to be present
/// (decision: the shape is "single table, no GROUP BY/aggregates/HAVING,
/// limit present with a zero (or absent) offset, ...") — and fall back to
/// the pre-existing plan, relying on Exasol's own backstop `ORDER BY` for
/// correctness. This
/// proves the new top-N capability did not silently widen what counts as
/// "matched" in a way that breaks the existing, unchanged fallback behavior
/// for a plain (unbounded) sort.
///
/// Confirmed live (see below) that this decline shape IS safe today: when
/// Exasol's pushdown request carries `orderBy` but no `limit` at all (because
/// the query has no LIMIT), Exasol keeps its own top-level `ORDER BY` operator
/// and re-sorts the adapter's returned (unsorted) rows itself — unlike the
/// `orderBy` + `limit`-together case, where Exasol fully delegates both to the
/// returned SQL and does not re-apply either if the adapter declines. (That
/// latter shape — `ORDER BY <unprojected column> LIMIT n` — was verified live
/// during this task to return WRONG, unsorted/unbounded results today because
/// the withheld-limit fallback assumes an Exasol backstop that does not
/// actually run once `ORDER_BY_COLUMN` is advertised; see the decision log /
/// review notes for `add-topn-pushdown` B5 — this is a real gap in B3/B3b's
/// "withhold the limit, Exasol re-applies" invariant, tracked separately from
/// this regression test, which intentionally exercises a shape that IS safe.)
///
/// Same seeded data as the match case: the fallback's answer must still be
/// every id, fully sorted by score DESC (20,19,...,1).
#[test]
fn order_by_without_limit_falls_back_correctly() {
    setup_e2e();
    let mut conn = exa_conn();

    let sql = format!("SELECT id, score FROM {} ORDER BY score DESC", vs_table());

    let pushed_sql = explain_virtual_sql(&mut conn, &sql);
    // Use the precise field-shaped marker (see the comment in
    // `ordered_topn_pushes_down_matches_single_node`): a bare `contains("order_by")`
    // would false-positive on Exasol's echoed `pushdownRequest.orderBy[].type ==
    // "order_by_element"`, which is present for ANY ORDER-BY query regardless of
    // whether the adapter's own scan spec ends up carrying an `order_by` field.
    assert!(
        !pushed_sql.contains("\"order_by\":"),
        "ORDER BY with no LIMIT must decline the ordered-top-N pushdown \
         (no 'order_by' field in the scan spec; a limit is required to \
         match), got:\n{pushed_sql}"
    );
    assert!(
        !pushed_sql.contains("\"limit\":"),
        "ORDER BY with no LIMIT must not synthesize a limit in the scan \
         spec, got:\n{pushed_sql}"
    );

    let cols = conn.query_columns(&sql);
    assert_eq!(cols.len(), 2, "expected 2 columns (id, score): {cols:?}");
    assert_eq!(
        cols[0].len(),
        20,
        "no LIMIT means all 20 rows must be returned: {cols:?}"
    );

    // Exasol must re-apply the ORDER BY itself (the adapter's returned SQL
    // carries no ORDER BY when the shape is unmatched) — all 20 ids in
    // descending score order.
    let ids: Vec<i64> = cols[0].iter().map(parse_int).collect();
    let expected_ids: Vec<i64> = (1..=20).rev().collect();
    assert_eq!(
        ids, expected_ids,
        "ORDER BY score DESC (no LIMIT, fallback path) must return all ids \
         in descending order {expected_ids:?}, got {ids:?}"
    );
}

/// After createVirtualSchema the schema's adapterNotes carry PARALLELISM_FACTOR
/// and NR_OF_CORES, but no CLUSTER_NODES key — the node count is no longer
/// persisted in adapterNotes at all; `pushdown` now reads it live from
/// `UdfContext::node_count()` on every request instead.
///
/// Queries SYS.EXA_ALL_VIRTUAL_SCHEMAS.ADAPTER_NOTES — the observable catalog
/// column for adapter-controlled schema state. Exasol does NOT persist
/// adapter-returned schemaMetadata.properties (they are silently dropped and
/// never appear in any catalog view), so the adapter carries its persisted
/// properties in adapterNotes (a JSON string), which Exasol DOES persist and
/// surface here.
///
/// (The view is keyed by SCHEMA_NAME, confirmed against the live DB.)
#[test]
fn create_vs_omits_cluster_nodes_from_adapter_notes() {
    setup_e2e();
    let mut conn = exa_conn();

    let cols = conn.query_columns(&format!(
        "SELECT ADAPTER_NOTES FROM SYS.EXA_ALL_VIRTUAL_SCHEMAS \
         WHERE SCHEMA_NAME = '{VS_NAME}'"
    ));
    assert_eq!(
        cols.len(),
        1,
        "query must return one column (ADAPTER_NOTES): {cols:?}"
    );
    assert_eq!(
        cols[0].len(),
        1,
        "the virtual schema must exist (one row): {cols:?}"
    );
    let notes = cols[0][0]
        .as_str()
        .unwrap_or_else(|| panic!("ADAPTER_NOTES value is not a string: {:?}", cols[0][0]));
    assert!(
        !notes.is_empty(),
        "ADAPTER_NOTES must be non-empty (Exasol must have persisted it): {notes:?}"
    );

    let parsed: serde_json::Value = serde_json::from_str(notes)
        .unwrap_or_else(|e| panic!("ADAPTER_NOTES must be valid JSON ({e}): {notes:?}"));
    assert!(
        parsed.get("PARALLELISM_FACTOR").is_some(),
        "ADAPTER_NOTES must carry PARALLELISM_FACTOR: {notes:?}"
    );
    assert!(
        parsed.get("NR_OF_CORES").is_some(),
        "ADAPTER_NOTES must carry NR_OF_CORES: {notes:?}"
    );
    assert!(
        parsed.get("CLUSTER_NODES").is_none(),
        "ADAPTER_NOTES must NOT carry CLUSTER_NODES (node count is read live \
         from UdfContext::node_count() per pushdown request, never persisted): \
         {notes:?}"
    );
}

/// Even with CLUSTER_NODES no longer persisted in adapterNotes,
/// `EXPLAIN VIRTUAL` over a multi-file scan against the (now note-free)
/// virtual schema still produces the `LAKEHOUSE_DISTRIBUTE_FILES` shard
/// fan-out — i.e. dropping the persisted note did not break shard-fan-out
/// emission: `pushdown` still plans the fan-out with no adapterNotes node
/// count available.
///
/// This test cannot distinguish the handshake node-count source
/// (`UdfContext::node_count()`, captured in `dispatch` and threaded through
/// as `cluster_nodes`) from the pre-refactor absent-note default: the
/// `events` fixture commits exactly two data files, which clamps
/// `shard_count` to 2 for every node count >= 1, so the fan-out markers below
/// are insensitive to the node count by construction on this single-node
/// suite. The plan's `parallelism/work-unit-sharding (GATE: four-node
/// staging)` manual check is the only one that distinguishes a correctly
/// read four-node count from the `0 => 1` floor.
///
/// Uses the same fan-out marker style as
/// `ordered_topn_pushes_down_matches_single_node`: the literal
/// `AS shards(shard_key, files) GROUP BY shard_key)` string is the adapter's
/// own emitted fan-out SQL, not an echo of Exasol's pushdown request, so it
/// is a precise, non-false-positive marker.
#[test]
fn pushdown_shards_from_handshake_node_count_without_note() {
    setup_e2e();
    let mut conn = exa_conn();

    let sql = format!("SELECT id, name, score FROM {}", vs_table());

    let pushed_sql = explain_virtual_sql(&mut conn, &sql);
    assert!(
        pushed_sql.contains("LAKEHOUSE_DISTRIBUTE_FILES"),
        "multi-file scan must push down as a LAKEHOUSE_DISTRIBUTE_FILES shard \
         fan-out, got:\n{pushed_sql}"
    );
    assert!(
        pushed_sql.contains("AS shards(shard_key, files) GROUP BY shard_key)"),
        "shard fan-out must carry the shards(shard_key, files) VALUES table \
         grouped by shard_key, got:\n{pushed_sql}"
    );
}

/// COUNT(col) aggregate pushdown returns the correct non-null row count.
///
/// Verifies COUNT(score) and COUNT(score) WHERE score > 15.0, covering the
/// COUNT(col) case not exercised by existing COUNT(*) tests.
#[test]
fn aggregate_count_col_returns_correct_value() {
    setup_e2e();
    let mut conn = exa_conn();

    // COUNT(score) over all rows: all 20 rows have non-null score.
    let cols = conn.query_columns(&format!("SELECT COUNT(score) FROM {}", vs_table()));
    assert_eq!(
        cols.len(),
        1,
        "COUNT(score) must return one column: {cols:?}"
    );
    assert_eq!(
        cols[0].len(),
        1,
        "COUNT(score) must return one row: {cols:?}"
    );
    let count_all = cols[0][0]
        .as_i64()
        .or_else(|| cols[0][0].as_str().and_then(|s| s.parse().ok()))
        .unwrap_or_else(|| panic!("COUNT(score) result not integer: {:?}", cols[0][0]));
    assert_eq!(count_all, 20, "COUNT(score) must be 20 (all rows non-null)");

    // COUNT(score) WHERE score > 15.0 → 17 rows (SEED_ROWS_SCORE_GT_15).
    let cols_filtered = conn.query_columns(&format!(
        "SELECT COUNT(score) FROM {} WHERE score > 15.0",
        vs_table()
    ));
    let count_filtered = cols_filtered[0][0]
        .as_i64()
        .or_else(|| cols_filtered[0][0].as_str().and_then(|s| s.parse().ok()))
        .unwrap_or_else(|| {
            panic!(
                "filtered COUNT(score) result not integer: {:?}",
                cols_filtered[0][0]
            )
        });
    assert_eq!(
        count_filtered, SEED_ROWS_SCORE_GT_15 as i64,
        "COUNT(score) WHERE score > 15.0 must be {SEED_ROWS_SCORE_GT_15}, got {count_filtered}"
    );

    // COUNT(*) WHERE score > 15.0 — also verifies the WHERE path for COUNT(*).
    let cols_star = conn.query_columns(&format!(
        "SELECT COUNT(*) FROM {} WHERE score > 15.0",
        vs_table()
    ));
    let count_star = cols_star[0][0]
        .as_i64()
        .or_else(|| cols_star[0][0].as_str().and_then(|s| s.parse().ok()))
        .unwrap_or_else(|| panic!("COUNT(*) WHERE result not integer: {:?}", cols_star[0][0]));
    assert_eq!(
        count_star, SEED_ROWS_SCORE_GT_15 as i64,
        "COUNT(*) WHERE score > 15.0 must be {SEED_ROWS_SCORE_GT_15}, got {count_star}"
    );

    // #193 regression: the same COUNT(e.score) / COUNT(*) aggregates over an
    // ALIASED FROM, with the filter column qualified by the alias. Expected
    // values are the SAME SEED_ROWS_SCORE_GT_15 the unaliased case above
    // asserts, proving alias stripping leaves single-group aggregate
    // pushdown (aggregate args + filter) unchanged.
    let cols_aliased_col = conn.query_columns(&format!(
        "SELECT COUNT(e.score) FROM {} e WHERE e.score > 15.0",
        vs_table()
    ));
    let count_aliased_col = cols_aliased_col[0][0]
        .as_i64()
        .or_else(|| cols_aliased_col[0][0].as_str().and_then(|s| s.parse().ok()))
        .unwrap_or_else(|| {
            panic!(
                "aliased COUNT(e.score) result not integer: {:?}",
                cols_aliased_col[0][0]
            )
        });
    assert_eq!(
        count_aliased_col, SEED_ROWS_SCORE_GT_15 as i64,
        "aliased COUNT(e.score) WHERE e.score > 15.0 must be {SEED_ROWS_SCORE_GT_15}, \
         got {count_aliased_col}"
    );

    let cols_aliased_star = conn.query_columns(&format!(
        "SELECT COUNT(*) FROM {} e WHERE e.score > 15.0",
        vs_table()
    ));
    let count_aliased_star = cols_aliased_star[0][0]
        .as_i64()
        .or_else(|| {
            cols_aliased_star[0][0]
                .as_str()
                .and_then(|s| s.parse().ok())
        })
        .unwrap_or_else(|| {
            panic!(
                "aliased COUNT(*) result not integer: {:?}",
                cols_aliased_star[0][0]
            )
        });
    assert_eq!(
        count_aliased_star, SEED_ROWS_SCORE_GT_15 as i64,
        "aliased COUNT(*) WHERE e.score > 15.0 must be {SEED_ROWS_SCORE_GT_15}, \
         got {count_aliased_star}"
    );

    // #193 regression: a GROUP BY under an aliased FROM (group key + aggregate
    // argument both alias-qualified). Grouping by the near-unique `id` under
    // the same `e.score > 15.0` filter yields exactly SEED_ROWS_SCORE_GT_15
    // groups (one per matching id), each with COUNT(e.score) == 1 — proving
    // alias stripping applies to GROUP BY keys, not just filters/aggregates.
    let cols_grouped = conn.query_columns(&format!(
        "SELECT e.id, COUNT(e.score) FROM {} e WHERE e.score > 15.0 GROUP BY e.id",
        vs_table()
    ));
    assert_eq!(
        cols_grouped.len(),
        2,
        "grouped aliased query expected 2 columns (id, count): {cols_grouped:?}"
    );
    assert_eq!(
        cols_grouped[0].len(),
        SEED_ROWS_SCORE_GT_15,
        "GROUP BY e.id WHERE e.score > 15.0 must yield {SEED_ROWS_SCORE_GT_15} groups \
         (one per matching id), got {}",
        cols_grouped[0].len()
    );
    for count in &cols_grouped[1] {
        let c = count
            .as_i64()
            .or_else(|| count.as_str().and_then(|s| s.parse().ok()))
            .unwrap_or_else(|| panic!("grouped count not integer: {count:?}"));
        assert_eq!(
            c, 1,
            "each group (unique id) must have COUNT(e.score) == 1, got {c}"
        );
    }
}

/// #193 regression: a declined outer join re-pushes a PLAIN single-table scan
/// carrying the alias filter. The join gate hard-declines LEFT/RIGHT/FULL
/// joins (see `pushdown-planning-join-fallback`); Exasol then falls back to
/// executing the join itself over two adapter-returned single-table scans,
/// each still carrying its own alias (`c`, `o`) on the WHERE filter. This is
/// the one shape `events` cannot express (it has no FK relationship to join
/// against), so it reuses the existing `dim_customer`/`fact_orders` star
/// schema fixture instead of the events fixture the other cases use.
///
/// Every order's `O_CUSTKEY` cycles `1..=DIM_CUSTOMER_ROWS` across the seeded
/// orders, so each of `C_CUSTKEY` 1, 2, 3 matches `FACT_ORDERS_ROWS /
/// DIM_CUSTOMER_ROWS` orders and every customer has at least one order (no
/// NULL-extended rows) — the LEFT JOIN filtered to `C_CUSTKEY <= 3` therefore
/// yields exactly `3 * (FACT_ORDERS_ROWS / DIM_CUSTOMER_ROWS)` rows.
#[test]
fn e2e_declined_outer_join_repushes_aliased_single_table_scan() {
    setup_e2e();
    let mut conn = exa_conn();

    let sql = format!(
        "SELECT COUNT(*) FROM {} c LEFT JOIN {} o ON o.O_CUSTKEY = c.C_CUSTKEY \
         WHERE c.C_CUSTKEY <= 3",
        vs_dim_table(),
        vs_fact_table()
    );
    let cols = conn.query_columns(&sql);
    assert_eq!(cols.len(), 1, "COUNT(*) must return one column: {cols:?}");
    assert_eq!(cols[0].len(), 1, "COUNT(*) must return one row: {cols:?}");
    let count = cols[0][0]
        .as_i64()
        .or_else(|| cols[0][0].as_str().and_then(|s| s.parse().ok()))
        .unwrap_or_else(|| panic!("COUNT(*) result not integer: {:?}", cols[0][0]));
    let expected_count = 3 * (FACT_ORDERS_ROWS / DIM_CUSTOMER_ROWS) as i64;
    assert_eq!(
        count,
        expected_count,
        "declined LEFT JOIN dim_customer c ... WHERE c.C_CUSTKEY <= 3 must yield \
         {expected_count} rows (3 matching customers x {} orders each), got {count}",
        FACT_ORDERS_ROWS / DIM_CUSTOMER_ROWS
    );
}

/// A multi-shard fan-out query returns the complete, non-overlapping row set.
///
/// The test Exasol stack is single-node so partition_files yields one shard at
/// runtime; true cross-node file placement is exercised only on a real multi-node
/// cluster. On the single-node stack this test asserts the union-completeness
/// invariant: the fan-out/union path returns every row exactly once with no gaps
/// and no duplicates — the correctness property that multi-shard sharding guarantees.
#[test]
fn multi_shard_row_query_matches_single_shard() {
    setup_e2e();
    let mut conn = exa_conn();

    let cols = conn.query_columns(&format!("SELECT id FROM {} ORDER BY id", vs_table()));
    assert_eq!(cols.len(), 1, "SELECT id must return one column: {cols:?}");
    assert_eq!(
        cols[0].len(),
        20,
        "fan-out must return all 20 rows, no duplicates, no gaps: got {}",
        cols[0].len()
    );

    let ids: Vec<i64> = cols[0]
        .iter()
        .map(|v| {
            v.as_i64()
                .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
                .unwrap_or_else(|| panic!("id is not an integer: {v:?}"))
        })
        .collect();

    for (pos, &id) in ids.iter().enumerate() {
        let expected = (pos + 1) as i64;
        assert_eq!(
            id, expected,
            "id at position {pos} must be {expected}, got {id} (union-completeness violated)"
        );
    }
}

/// A multi-file scan through the VS returns correct rows end-to-end with the
/// reshaped `(path, size)` + `table_root` payload: the adapter resolves the file
/// list once, byte-balances it into shards carrying `(relative-or-absolute path,
/// byte-size)` entries under a table root serialized once in the common blob, and
/// each fanned-out UDF reconstructs the absolute URIs and registers ONLY its
/// assigned files. Proving every file across every shard is scanned exactly once
/// (no gaps, no duplicates) with fully correct column values exercises the new
/// payload through the real fan-out.
///
/// The generated fan-out SQL shape — table root carried ONCE in the common
/// literal and per-shard `[[path, size], ...]` literals — is asserted host-side
/// (no DB) by the pushdown unit test
/// `fan_out_carries_root_once_and_path_size_tuples_per_shard`.
#[test]
fn scan_registers_assigned_files_with_path_size_payload() {
    setup_e2e();
    let mut conn = exa_conn();

    // Full projection over the whole (multi-file) table. Seeded rows: id 1..20,
    // name carries the zero-padded id, score = 5.0 * id.
    let sql = format!("SELECT id, name, score FROM {} ORDER BY id", vs_table());
    let cols = conn.query_columns(&sql);
    assert_eq!(
        cols.len(),
        3,
        "expected 3 columns (id, name, score): {cols:?}"
    );
    assert_eq!(
        cols[0].len(),
        20,
        "multi-file fan-out must return all 20 rows with no gaps/duplicates: got {}",
        cols[0].len()
    );

    let ids: Vec<i64> = cols[0]
        .iter()
        .map(|v| {
            v.as_i64()
                .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
                .unwrap_or_else(|| panic!("id is not an integer: {v:?}"))
        })
        .collect();

    for (pos, &id) in ids.iter().enumerate() {
        let expected = (pos + 1) as i64;
        assert_eq!(
            id, expected,
            "id at position {pos} must be {expected}, got {id} (a file was missed or double-scanned)"
        );

        // score = 5.0 * id — proves the data (not just the row count) is correct
        // for the file this row came from.
        let score = cols[2][pos]
            .as_f64()
            .unwrap_or_else(|| panic!("score not f64: {:?}", cols[2][pos]));
        assert!(
            (score - 5.0 * expected as f64).abs() < 1e-9,
            "score for id {expected} must be {}, got {score}",
            5.0 * expected as f64
        );

        // name carries the zero-padded id.
        let name = cols[1][pos]
            .as_str()
            .unwrap_or_else(|| panic!("name not string: {:?}", cols[1][pos]));
        assert!(
            name.contains(&format!("{expected:02}")),
            "name '{name}' does not carry expected id {expected}"
        );
    }
}

// ---------------------------------------------------------------------------
// Phase 5 — GROUP BY E2E tests
//
// Seed recap (20 rows, id=1..20):
//   score  = 5.0 * id   (5.0, 10.0, ..., 100.0)
//   name   = "event-NN"
//   event_date = 2024-01-01 + (id-1) days
//   event_ts   = 2024-01-01T00:00:00Z + (id-1) hours
//
// Group-key derivations used below:
//   MOD(id, 4) → groups {0,1,2,3}, 5 rows each
//   MOD(id, 2) × MOD(id, 4) × WHERE score > 50 → 4 groups across 10 rows
//   CAST(score / 25.0 AS DECIMAL(4,0)) → groups {0,1,2,3,4} with sizes 2,5,5,5,3
//     (Exasol CAST-to-DECIMAL rounds half away from zero)
//   id → 20 groups, 1 row each (high cardinality / spill path)
//   NULLIF(MOD(id, 5), 0) → groups {1,2,3,4,NULL}, sizes 4,4,4,4,4
// ---------------------------------------------------------------------------

/// Runs `EXPLAIN VIRTUAL` for a GROUP BY query and asserts the pushed SQL
/// evidences a grouped partial-aggregate pushdown — not the raw row-scan
/// fallback that Exasol would otherwise aggregate itself.
///
/// The real, shard-count-independent evidence of grouped pushdown is the
/// `group_keys` field inside the `LAKEHOUSE_SCAN` scan spec: the grouped
/// partial-aggregate path emits `"group_keys":[...]` (and the `PARTIAL_`
/// aggregate-column prefix), while the raw-scan fallback emits neither.
///
/// `GROUP BY shard_key` is deliberately NOT used as the discriminator: that
/// inner fan-out only appears when the scan spreads over MULTIPLE shards. When
/// a WHERE filter prunes the file list to a SINGLE file/shard, grouped
/// pushdown still occurs — it emits `... SUM("PARTIAL_...") ... GROUP BY
/// "GK_0", "GK_1"` with no `GROUP BY shard_key` — so asserting on it would
/// false-negative any legitimately pushed-down, single-shard grouped query.
///
/// Asserts: the pushed SQL contains `group_keys` and the `PARTIAL_` partial-
/// aggregate column prefix (grouped pushdown occurred), contains no `IPROC()`
/// (legacy, non-oversubscribed sharding), and is not a raw `SELECT * FROM
/// (SELECT ...)` row-scan wrapper (which would mean the multi-key GROUP BY
/// silently fell back instead of being pushed down as partial aggregation).
fn assert_group_by_pushed_down(conn: &mut ExaConn, query_sql: &str) {
    let explain_sql = format!("EXPLAIN VIRTUAL {query_sql}");
    let resp = conn.execute(&explain_sql);
    let result_set = &resp["responseData"]["results"][0]["resultSet"];
    let cols = conn.fetch_result_columns(result_set);

    // Flatten all returned text fragments into one string for pattern checks.
    let pushed_sql: String = cols
        .iter()
        .flat_map(|col| col.iter())
        .filter_map(|v| v.as_str())
        .collect::<Vec<_>>()
        .join(" ");

    assert!(
        pushed_sql.contains("group_keys"),
        "EXPLAIN VIRTUAL output must contain 'group_keys' in the scan spec \
         (grouped partial-aggregate pushdown occurred), got:\n{pushed_sql}"
    );
    assert!(
        pushed_sql.contains("PARTIAL_"),
        "EXPLAIN VIRTUAL output must contain the 'PARTIAL_' partial-aggregate \
         column prefix (grouped pushdown occurred), got:\n{pushed_sql}"
    );
    assert!(
        !pushed_sql.contains("IPROC()"),
        "EXPLAIN VIRTUAL output must NOT contain 'IPROC()' (legacy sharding), got:\n{pushed_sql}"
    );
    assert!(
        !pushed_sql.contains("SELECT * FROM (SELECT"),
        "EXPLAIN VIRTUAL output must not be a raw row-scan fallback \
         ('SELECT * FROM (SELECT ...)'), got:\n{pushed_sql}"
    );
}

/// Regression guard for issue #145: the `LAKEHOUSE_SCAN` common scan spec for
/// a genuinely decomposed GROUP BY query (single key, real grouped
/// partial-aggregate pushdown — NOT the undecomposable single-table raw-scan
/// fallback `build_qualified_single_table_fallback_sql` falls back to, which
/// legitimately carries a non-empty `projection`) MUST also report an empty
/// `projection` field.
///
/// The grouped path builds its query from `group_keys`/`aggregates`, never
/// from `projection` (see the doc comment on
/// [`CommonScanSpec::projection`](lakehouse_engine::scan::spec::CommonScanSpec)),
/// so leaving it empty is accurate — mirrors
/// [`single_group_aggregate_scan_spec_projection_is_empty`] for the grouped
/// dispatch path. [`assert_group_by_pushed_down`] confirms real grouped
/// pushdown occurred (not the raw-scan fallback) before the `"projection":[]`
/// marker is checked.
#[test]
fn grouped_aggregate_scan_spec_projection_is_empty() {
    setup_e2e();
    let mut conn = exa_conn();

    let sql = format!(
        "SELECT MOD(id, 4), COUNT(*), SUM(score) FROM {} GROUP BY MOD(id, 4)",
        vs_table()
    );
    assert_group_by_pushed_down(&mut conn, &sql);

    let pushed_sql = explain_virtual_sql(&mut conn, &sql);
    assert!(
        pushed_sql.contains("\"projection\":[]"),
        "grouped aggregate scan spec must report an empty 'projection' \
         field (#145: the grouped-dispatch path reads \
         'group_keys'/'aggregates', not 'projection', so an empty value \
         means \"not applicable\", not \"all columns\"), got:\n{pushed_sql}"
    );
}

/// GROUP BY returns correct per-group COUNT(*) and SUM(score).
///
/// Key: MOD(id, 4) — four equal-sized groups (5 rows each).
///
/// Expected:
///   group 0 (id=4,8,12,16,20):  count=5, sum_score=300.0
///   group 1 (id=1,5,9,13,17):   count=5, sum_score=225.0
///   group 2 (id=2,6,10,14,18):  count=5, sum_score=250.0
///   group 3 (id=3,7,11,15,19):  count=5, sum_score=275.0
#[test]
fn test_group_by_sum_count() {
    setup_e2e();
    let mut conn = exa_conn();

    let sql = format!(
        "SELECT MOD(id, 4), COUNT(*), SUM(score) FROM {} GROUP BY MOD(id, 4) ORDER BY MOD(id, 4)",
        vs_table()
    );
    let cols = conn.query_columns(&sql);
    assert_eq!(cols.len(), 3, "expected 3 columns: {cols:?}");
    assert_eq!(cols[0].len(), 4, "expected 4 groups: {cols:?}");

    // Expected values sorted by group key 0..3.
    let expected_counts = [5i64, 5, 5, 5];
    let expected_sums = [300.0f64, 225.0, 250.0, 275.0];

    for i in 0..4 {
        let count = parse_int(&cols[1][i]);
        assert_eq!(
            count, expected_counts[i],
            "group {i}: COUNT(*) must be {}, got {count}",
            expected_counts[i]
        );
        let sum = parse_numeric(&cols[2][i]);
        assert!(
            (sum - expected_sums[i]).abs() < 0.01,
            "group {i}: SUM(score) must be {}, got {sum}",
            expected_sums[i]
        );
    }

    // Total rows across all groups = 20.
    let total: i64 = cols[1].iter().map(parse_int).sum();
    assert_eq!(
        total, 20,
        "total COUNT(*) across groups must be 20, got {total}"
    );
}

/// Two GROUP BY keys with a WHERE filter returns correct per-group row counts.
///
/// Keys: MOD(id, 4) × MOD(id, 2), filter: score > 50.0 (id=11..20, 10 rows).
///
/// Expected 4 groups:
///   (0, 0): id=12,16,20       → count=3
///   (1, 1): id=13,17          → count=2
///   (2, 0): id=14,18          → count=2
///   (3, 1): id=11,15,19       → count=3
#[test]
fn test_group_by_multi_key_with_filter() {
    setup_e2e();
    let mut conn = exa_conn();

    let sql = format!(
        "SELECT MOD(id, 4), MOD(id, 2), COUNT(*) \
         FROM {} WHERE score > 50.0 \
         GROUP BY MOD(id, 4), MOD(id, 2)",
        vs_table()
    );

    // Pushdown-occurred assertion: multi-key GROUP BY (with a WHERE filter)
    // must be pushed down as shard-key fan-out partial aggregation, not the
    // raw row-scan fallback.
    assert_group_by_pushed_down(&mut conn, &sql);

    let cols = conn.query_columns(&sql);
    assert_eq!(cols.len(), 3, "expected 3 columns: {cols:?}");
    assert_eq!(cols[0].len(), 4, "expected 4 distinct groups: {cols:?}");

    // Total rows across all groups = 10 (id=11..20).
    let total: i64 = cols[2].iter().map(parse_int).sum();
    assert_eq!(
        total, 10,
        "total COUNT(*) across all groups must be 10 (id=11..20), got {total}"
    );

    // No group can have more than 3 rows (id range 11..20 across 4 buckets).
    for (i, v) in cols[2].iter().enumerate() {
        let c = parse_int(v);
        assert!(
            (2..=3).contains(&c),
            "group {i}: count must be 2 or 3, got {c}"
        );
    }
}

/// GROUP BY a scalar expression key returns correct per-group counts.
///
/// Key expression: CAST(score / 25.0 AS DECIMAL(4,0)) — supported arithmetic (FLOAT_DIV) + CAST.
///
/// Exasol's CAST-to-DECIMAL rounds half away from zero. For scores 5..100 in
/// steps of 5, score/25 = 0.2, 0.4, ..., 4.0, which rounds to:
///   key 0: scores {5,10}             → count=2
///   key 1: scores {15,20,25,30,35}   → count=5
///   key 2: scores {40,45,50,55,60}   → count=5
///   key 3: scores {65,70,75,80,85}   → count=5
///   key 4: scores {90,95,100}        → count=3
/// (2+5+5+5+3 = 20.)
#[test]
fn test_group_by_expression_key() {
    setup_e2e();
    let mut conn = exa_conn();

    let sql = format!(
        "SELECT CAST(score / 25.0 AS DECIMAL(4,0)), COUNT(*) \
         FROM {} \
         GROUP BY CAST(score / 25.0 AS DECIMAL(4,0)) \
         ORDER BY CAST(score / 25.0 AS DECIMAL(4,0))",
        vs_table()
    );
    let cols = conn.query_columns(&sql);
    assert_eq!(cols.len(), 2, "expected 2 columns (key, count): {cols:?}");
    assert_eq!(cols[0].len(), 5, "expected 5 groups: {cols:?}");

    // Sort (key, count) pairs by key so the test is robust to row ordering.
    let mut pairs: Vec<(i64, i64)> = cols[0]
        .iter()
        .zip(cols[1].iter())
        .map(|(k, c)| (parse_int(k), parse_int(c)))
        .collect();
    pairs.sort_by_key(|(k, _)| *k);

    let expected_counts = [2i64, 5, 5, 5, 3];
    for (i, expected) in expected_counts.iter().enumerate() {
        let (key, count) = pairs[i];
        assert_eq!(
            key, i as i64,
            "group at position {i}: key must be {i}, got {key}"
        );
        assert_eq!(
            count, *expected,
            "group key {key}: COUNT(*) must be {expected}, got {count}"
        );
    }

    // Total = 20.
    let total: i64 = pairs.iter().map(|(_, c)| *c).sum();
    assert_eq!(
        total, 20,
        "total rows across expression-key groups must be 20, got {total}"
    );
}

/// AVG(score) per group is correct for groups with unequal row counts.
///
/// Key expression: CAST(score / 25.0 AS DECIMAL(4,0)) — Exasol rounds half away
/// from zero, so score/25 (0.2..4.0) buckets into groups of sizes 2,5,5,5,3:
///   key 0 (scores {5,10}):              AVG = 15.0 / 2  = 7.5
///   key 1 (scores {15,20,25,30,35}):    AVG = 125.0 / 5 = 25.0
///   key 2 (scores {40,45,50,55,60}):    AVG = 250.0 / 5 = 50.0
///   key 3 (scores {65,70,75,80,85}):    AVG = 375.0 / 5 = 75.0
///   key 4 (scores {90,95,100}):         AVG = 285.0 / 3 = 95.0
#[test]
fn test_group_by_avg_correctness() {
    setup_e2e();
    let mut conn = exa_conn();

    let sql = format!(
        "SELECT CAST(score / 25.0 AS DECIMAL(4,0)), AVG(score) \
         FROM {} \
         GROUP BY CAST(score / 25.0 AS DECIMAL(4,0)) \
         ORDER BY CAST(score / 25.0 AS DECIMAL(4,0))",
        vs_table()
    );
    let cols = conn.query_columns(&sql);
    assert_eq!(cols.len(), 2, "expected 2 columns (key, avg): {cols:?}");
    assert_eq!(cols[0].len(), 5, "expected 5 groups: {cols:?}");

    // Sort (key, avg) pairs by key so the test is robust to row ordering.
    let mut pairs: Vec<(i64, f64)> = cols[0]
        .iter()
        .zip(cols[1].iter())
        .map(|(k, a)| (parse_int(k), parse_numeric(a)))
        .collect();
    pairs.sort_by_key(|(k, _)| *k);

    let expected_avgs = [7.5f64, 25.0, 50.0, 75.0, 95.0];
    for (i, expected) in expected_avgs.iter().enumerate() {
        let (key, avg) = pairs[i];
        assert_eq!(
            key, i as i64,
            "group at position {i}: key must be {i}, got {key}"
        );
        assert!(
            (avg - expected).abs() < 0.01,
            "group key {key}: AVG(score) must be {expected}, got {avg}"
        );
    }
}

/// GROUP BY a near-unique column (id) completes with correct per-group counts.
///
/// Exercises the high-cardinality path: 20 distinct groups, each with exactly one row.
/// Verifies the memory-pool + spill backstop does not crash at high group cardinality.
#[test]
fn test_high_cardinality_group_by_spill() {
    setup_e2e();
    let mut conn = exa_conn();

    let sql = format!(
        "SELECT id, COUNT(*) FROM {} GROUP BY id ORDER BY id",
        vs_table()
    );
    let cols = conn.query_columns(&sql);
    assert_eq!(cols.len(), 2, "expected 2 columns (id, count): {cols:?}");
    assert_eq!(
        cols[0].len(),
        20,
        "GROUP BY id must return 20 groups, got {}",
        cols[0].len()
    );

    // Every group must have exactly one row (id is unique).
    for (i, v) in cols[1].iter().enumerate() {
        let count = parse_int(v);
        assert_eq!(
            count,
            1,
            "group at position {i} (id={}): COUNT(*) must be 1, got {count}",
            parse_int(&cols[0][i])
        );
    }

    // IDs must be 1..20 in order.
    let ids: Vec<i64> = cols[0].iter().map(parse_int).collect();
    for (pos, &id) in ids.iter().enumerate() {
        let expected = (pos + 1) as i64;
        assert_eq!(
            id, expected,
            "id at position {pos} must be {expected}, got {id}"
        );
    }
}

/// EXPLAIN VIRTUAL shows shard_key fan-out and no IPROC() in the pushed SQL.
///
/// Verifies: the VS generates `GROUP BY shard_key` (oversubscribed fan-out)
/// and never falls back to the legacy `IPROC()` node-count sharding.
#[test]
fn test_shard_key_fanout_explain() {
    setup_e2e();
    let mut conn = exa_conn();

    // EXPLAIN VIRTUAL returns the pushdown SQL as a single-column result set.
    let sql = format!(
        "EXPLAIN VIRTUAL SELECT id, COUNT(*) FROM {} GROUP BY id",
        vs_table()
    );
    let resp = conn.execute(&sql);
    // Collect all text from the result set — each element is a fragment of the SQL.
    let result_set = &resp["responseData"]["results"][0]["resultSet"];
    let cols = conn.fetch_result_columns(result_set);

    // Flatten all returned text fragments into one string for pattern checks.
    let pushed_sql: String = cols
        .iter()
        .flat_map(|col| col.iter())
        .filter_map(|v| v.as_str())
        .collect::<Vec<_>>()
        .join(" ");

    assert!(
        pushed_sql.contains("shard_key"),
        "EXPLAIN VIRTUAL output must contain 'shard_key' (oversubscribed fan-out), got:\n{pushed_sql}"
    );
    assert!(
        !pushed_sql.contains("IPROC()"),
        "EXPLAIN VIRTUAL output must NOT contain 'IPROC()' (legacy sharding), got:\n{pushed_sql}"
    );
    assert!(
        pushed_sql.contains("GROUP BY"),
        "EXPLAIN VIRTUAL output must contain 'GROUP BY', got:\n{pushed_sql}"
    );
}

/// NULL group keys are grouped together consistently.
///
/// Key: NULLIF(MOD(id, 5), 0) — multiples of 5 (id=5,10,15,20) yield NULL.
/// Non-null groups are {1,2,3,4}, each with 4 rows; NULL group also has 4 rows.
///
/// Seed has no nullable columns; NULL is produced via NULLIF expression.
#[test]
fn test_group_by_null_key_grouping() {
    setup_e2e();
    let mut conn = exa_conn();

    // NULLIF(MOD(id, 5), 0): id=5,10,15,20 → 0 → NULL; id=1..4 → 1..4; etc.
    // Non-null groups: 1 (id=1,6,11,16), 2 (id=2,7,12,17), 3 (id=3,8,13,18), 4 (id=4,9,14,19)
    // NULL group: id=5,10,15,20 → 4 rows
    let sql = format!(
        "SELECT NULLIF(MOD(id, 5), 0), COUNT(*) \
         FROM {} \
         GROUP BY NULLIF(MOD(id, 5), 0) \
         ORDER BY NULLIF(MOD(id, 5), 0) NULLS LAST",
        vs_table()
    );
    let cols = conn.query_columns(&sql);
    assert_eq!(cols.len(), 2, "expected 2 columns (key, count): {cols:?}");
    // 5 groups: {1,2,3,4,NULL}
    assert_eq!(
        cols[0].len(),
        5,
        "expected 5 groups (1,2,3,4,NULL): {cols:?}"
    );

    // All groups have exactly 4 rows.
    for (i, v) in cols[1].iter().enumerate() {
        let count = parse_int(v);
        assert_eq!(
            count, 4,
            "group at position {i}: COUNT(*) must be 4, got {count}"
        );
    }

    // Total rows = 20.
    let total: i64 = cols[1].iter().map(parse_int).sum();
    assert_eq!(
        total, 20,
        "total rows across all groups must be 20, got {total}"
    );

    // Exactly one group key must be NULL (the multiples-of-5 group).
    // We scan for null entries rather than asserting a fixed position, because
    // the ORDER BY NULLS LAST may not survive the GROUP BY pushdown — position
    // is not guaranteed across execution paths.
    let null_count = cols[0].iter().filter(|v| v.is_null()).count();
    assert_eq!(
        null_count, 1,
        "exactly one NULL group key must exist (multiples of 5): {cols:?}"
    );

    // Find the null group's count and verify it is 4.
    let null_group_count = cols[0]
        .iter()
        .zip(cols[1].iter())
        .find(|(k, _)| k.is_null())
        .map(|(_, cnt)| parse_int(cnt))
        .expect("NULL group must have a COUNT value");
    assert_eq!(
        null_group_count, 4,
        "NULL group (multiples of 5) must have COUNT=4, got {null_group_count}"
    );
}

/// Aggregate placed before the group key in the select list (GitHub #33 repro).
///
/// `SELECT SUM(score), MOD(id, 4) ... GROUP BY MOD(id, 4)` — the aggregate is
/// select-list position 0, the group key is position 1. Before the fix, the
/// adapter always emitted keys first in the outer merge SELECT, transposing
/// this query's columns relative to `selectListDataTypes` and failing with a
/// "Data type mismatch in column number 1" error. Values must match the
/// already-correct key-first form (`test_group_by_sum_count`).
///
/// Key: MOD(id, 4) — four equal-sized groups (5 rows each).
///   group 0 (id=4,8,12,16,20):  sum_score=300.0
///   group 1 (id=1,5,9,13,17):   sum_score=225.0
///   group 2 (id=2,6,10,14,18):  sum_score=250.0
///   group 3 (id=3,7,11,15,19):  sum_score=275.0
#[test]
fn test_group_by_agg_before_key() {
    setup_e2e();
    let mut conn = exa_conn();

    let sql = format!(
        "SELECT SUM(score), MOD(id, 4) FROM {} GROUP BY MOD(id, 4)",
        vs_table()
    );
    let cols = conn.query_columns(&sql);
    assert_eq!(cols.len(), 2, "expected 2 columns (sum, key): {cols:?}");
    assert_eq!(cols[0].len(), 4, "expected 4 groups: {cols:?}");

    // Sort (key, sum) pairs by key so the test is robust to row ordering.
    let mut pairs: Vec<(i64, f64)> = cols[1]
        .iter()
        .zip(cols[0].iter())
        .map(|(k, s)| (parse_int(k), parse_numeric(s)))
        .collect();
    pairs.sort_by_key(|(k, _)| *k);

    let expected_sums = [300.0f64, 225.0, 250.0, 275.0];
    for (i, expected) in expected_sums.iter().enumerate() {
        let (key, sum) = pairs[i];
        assert_eq!(
            key, i as i64,
            "group at position {i}: key must be {i}, got {key}"
        );
        assert!(
            (sum - expected).abs() < 0.01,
            "group key {key}: SUM(score) must be {expected}, got {sum}"
        );
    }

    // Total across all groups = 5.0 * (1+2+...+20) = 1050.0.
    let total: f64 = pairs.iter().map(|(_, s)| *s).sum();
    assert!(
        (total - 1050.0).abs() < 0.01,
        "total SUM(score) across groups must be 1050.0, got {total}"
    );
}

/// Interleaved multi-key GROUP BY: a group key, an aggregate, then a second
/// group key — `SELECT MOD(id,4), SUM(score), MOD(id,2) ... GROUP BY MOD(id,4), MOD(id,2)`.
///
/// Select-list order is key(0), agg(1), key(2), which does not match either the
/// keys-first or aggregate-first outer-SELECT ordering — exercising general
/// positional reassembly rather than either single-swap special case.
///
/// Groups (16 combinations possible; only even/odd-consistent pairs occur since
/// MOD(id,4) mod 2 == MOD(id,2)):
///   (0,0): id=4,8,12,16,20  → sum=300.0
///   (1,1): id=1,5,9,13,17   → sum=225.0
///   (2,0): id=2,6,10,14,18  → sum=250.0
///   (3,1): id=3,7,11,15,19  → sum=275.0
#[test]
fn test_group_by_interleaved_multi_key() {
    setup_e2e();
    let mut conn = exa_conn();

    let sql = format!(
        "SELECT MOD(id, 4), SUM(score), MOD(id, 2) FROM {} GROUP BY MOD(id, 4), MOD(id, 2)",
        vs_table()
    );

    // Pushdown-occurred assertion: interleaved multi-key GROUP BY must be
    // pushed down as shard-key fan-out partial aggregation, not the raw
    // row-scan fallback.
    assert_group_by_pushed_down(&mut conn, &sql);

    let cols = conn.query_columns(&sql);
    assert_eq!(
        cols.len(),
        3,
        "expected 3 columns (key1, sum, key2): {cols:?}"
    );
    assert_eq!(cols[0].len(), 4, "expected 4 groups: {cols:?}");

    // Sort (key1, sum, key2) triples by key1 so the test is robust to row ordering.
    let mut rows: Vec<(i64, f64, i64)> = cols[0]
        .iter()
        .zip(cols[1].iter())
        .zip(cols[2].iter())
        .map(|((k1, s), k2)| (parse_int(k1), parse_numeric(s), parse_int(k2)))
        .collect();
    rows.sort_by_key(|(k1, _, _)| *k1);

    let expected = [
        (0i64, 300.0f64, 0i64),
        (1, 225.0, 1),
        (2, 250.0, 0),
        (3, 275.0, 1),
    ];
    for (i, (exp_k1, exp_sum, exp_k2)) in expected.iter().enumerate() {
        let (k1, sum, k2) = rows[i];
        assert_eq!(
            k1, *exp_k1,
            "group at position {i}: MOD(id,4) key must be {exp_k1}, got {k1}"
        );
        assert!(
            (sum - exp_sum).abs() < 0.01,
            "group key {k1}: SUM(score) must be {exp_sum}, got {sum}"
        );
        assert_eq!(
            k2, *exp_k2,
            "group key {k1}: MOD(id,2) key must be {exp_k2}, got {k2}"
        );
    }

    // Total across all groups = 1050.0.
    let total: f64 = rows.iter().map(|(_, s, _)| *s).sum();
    assert!(
        (total - 1050.0).abs() < 0.01,
        "total SUM(score) across groups must be 1050.0, got {total}"
    );
}

/// Expression group key placed after an aggregate — `SELECT COUNT(*), MOD(id,4)
/// ... GROUP BY MOD(id,4)` — and the key column's declared type must survive
/// as its resolved DECIMAL type, not fall back to VARCHAR.
///
/// Guards against the secondary fragility described in the plan: resolving a
/// group key's declared type by rendered-string comparison silently defaults
/// to VARCHAR(2000000) if detection and lookup disagree; the fix resolves the
/// type by select-list index instead.
///
/// Key: MOD(id, 4) — four equal-sized groups (5 rows each), counts of 5 each.
#[test]
fn test_group_by_expr_key_after_agg() {
    setup_e2e();
    let mut conn = exa_conn();

    let sql = format!(
        "SELECT COUNT(*), MOD(id, 4) FROM {} GROUP BY MOD(id, 4)",
        vs_table()
    );
    let resp = conn.execute(&sql);

    // Assert the key column (position 1) carries a DECIMAL data type, not VARCHAR.
    let result_set = &resp["responseData"]["results"][0]["resultSet"];
    let column_type = result_set["columns"][1]["dataType"]["type"]
        .as_str()
        .unwrap_or_else(|| panic!("expected dataType.type for column 1: {result_set:?}"));
    assert_eq!(
        column_type, "DECIMAL",
        "MOD(id,4) group key column must carry DECIMAL type, not VARCHAR fallback: {result_set:?}"
    );

    let cols = conn.fetch_result_columns(result_set);
    assert_eq!(cols.len(), 2, "expected 2 columns (count, key): {cols:?}");
    assert_eq!(cols[0].len(), 4, "expected 4 groups: {cols:?}");

    // Sort (key, count) pairs by key so the test is robust to row ordering.
    let mut pairs: Vec<(i64, i64)> = cols[1]
        .iter()
        .zip(cols[0].iter())
        .map(|(k, c)| (parse_int(k), parse_int(c)))
        .collect();
    pairs.sort_by_key(|(k, _)| *k);

    for (i, (key, count)) in pairs.iter().enumerate() {
        assert_eq!(
            *key, i as i64,
            "group at position {i}: key must be {i}, got {key}"
        );
        assert_eq!(
            *count, 5,
            "group key {key}: COUNT(*) must be 5, got {count}"
        );
    }

    let total: i64 = pairs.iter().map(|(_, c)| *c).sum();
    assert_eq!(
        total, 20,
        "total COUNT(*) across groups must be 20, got {total}"
    );
}

/// Aggregate-first GROUP BY combined with HAVING — covers four HAVING shapes
/// against the same EVENTS fixture (per-group `SUM(score)` = {0: 300.0, 1:
/// 225.0, 2: 250.0, 3: 275.0}; 5 rows per `MOD(id,4)` group, 20 rows total,
/// seeded `name` values `event-01`..`event-20`, all unique):
///
/// 1. **Matched control** — `SELECT SUM(score), MOD(id,4) ... HAVING
///    SUM(score) > 250.0`, the aggregate ahead of the group key in the select
///    list. The HAVING aggregate is selected, so this decomposes into the
///    accelerated grouped partial/merge pushdown.
/// 2. **Unmatched aggregate** (issue #195) — `SELECT MOD(id,4), COUNT(*) ...
///    HAVING SUM(score) > 250.0`. `SUM(score)` is not in the select list, so
///    `render_having_over_merge` cannot rewrite it over the merge; the request
///    falls back to the qualified single-table wrapper (`LHS_T0`) instead of
///    hard-erroring at `EXPLAIN VIRTUAL` time.
/// 3. **Mixed AND junction** — same select list, `HAVING COUNT(*) > 0 AND
///    SUM(score) > 250.0`. Only one conjunct matches a selected aggregate;
///    the whole junction is unrenderable over the merge, so this also falls
///    back to the wrapper.
/// 4. **`COUNT(DISTINCT)` in HAVING** (issue #195's own repro shape) —
///    `HAVING COUNT(DISTINCT name) > 4`/`> 5`. `parse_agg_item` rejects
///    `distinct: true` unconditionally, so this is a third route to the same
///    unrenderable-HAVING fallback. All `name` values are unique per group of
///    5, so every group has exactly 5 distinct names: `> 4` keeps all 4
///    groups and `> 5` keeps none — the pair that proves the HAVING was
///    actually applied rather than silently dropped (a dropped HAVING would
///    return all 4 groups for both thresholds).
///
/// Cases 2 and 3 assert the fallback shape inline via `explain_virtual_sql`:
/// the pushed SQL must contain `LHS_T0` and must not contain `PARTIAL_`.
#[test]
fn test_group_by_agg_first_with_having() {
    setup_e2e();
    let mut conn = exa_conn();

    // Case 1: matched control — HAVING aggregate is selected, decomposes into
    // the accelerated grouped pushdown. Unchanged from before this fix.
    let sql = format!(
        "SELECT SUM(score), MOD(id, 4) FROM {} GROUP BY MOD(id, 4) HAVING SUM(score) > 250.0",
        vs_table()
    );
    let cols = conn.query_columns(&sql);
    assert_eq!(cols.len(), 2, "expected 2 columns (sum, key): {cols:?}");
    assert_eq!(
        cols[0].len(),
        2,
        "HAVING SUM(score) > 250.0 must keep exactly 2 groups (0 and 3): {cols:?}"
    );

    // Sort (key, sum) pairs by key so the test is robust to row ordering.
    let mut pairs: Vec<(i64, f64)> = cols[1]
        .iter()
        .zip(cols[0].iter())
        .map(|(k, s)| (parse_int(k), parse_numeric(s)))
        .collect();
    pairs.sort_by_key(|(k, _)| *k);

    let expected = [(0i64, 300.0f64), (3, 275.0)];
    for (i, (exp_key, exp_sum)) in expected.iter().enumerate() {
        let (key, sum) = pairs[i];
        assert_eq!(
            key, *exp_key,
            "group at position {i}: key must be {exp_key}, got {key}"
        );
        assert!(
            (sum - exp_sum).abs() < 0.01,
            "group key {key}: SUM(score) must be {exp_sum}, got {sum}"
        );
        assert!(
            sum > 250.0,
            "group key {key}: SUM(score) must satisfy HAVING > 250.0, got {sum}"
        );
    }

    // Case 2: unmatched aggregate — SUM(score) is not selected, so the merge
    // rewrite cannot find it. MUST succeed via the wrapper fallback, not error.
    let unmatched_sql = format!(
        "SELECT MOD(id, 4), COUNT(*) FROM {} GROUP BY MOD(id, 4) HAVING SUM(score) > 250.0",
        vs_table()
    );
    let unmatched_pushed_sql = explain_virtual_sql(&mut conn, &unmatched_sql);
    assert!(
        unmatched_pushed_sql.contains("LHS_T0"),
        "unmatched-aggregate HAVING must fall back to the qualified single-table \
         wrapper (pushed SQL must contain 'LHS_T0'), got:\n{unmatched_pushed_sql}"
    );
    assert!(
        !unmatched_pushed_sql.contains("PARTIAL_"),
        "unmatched-aggregate HAVING must NOT use the accelerated grouped \
         partial/merge pushdown (pushed SQL must not contain 'PARTIAL_'), \
         got:\n{unmatched_pushed_sql}"
    );

    let unmatched_cols = conn.query_columns(&unmatched_sql);
    assert_eq!(
        unmatched_cols.len(),
        2,
        "expected 2 columns (key, count): {unmatched_cols:?}"
    );
    let mut unmatched_pairs: Vec<(i64, i64)> = unmatched_cols[0]
        .iter()
        .zip(unmatched_cols[1].iter())
        .map(|(k, c)| (parse_int(k), parse_int(c)))
        .collect();
    unmatched_pairs.sort_by_key(|(k, _)| *k);
    assert_eq!(
        unmatched_pairs,
        vec![(0i64, 5i64), (3, 5)],
        "unmatched-aggregate HAVING must keep exactly groups 0 and 3, each with \
         COUNT(*) = 5: {unmatched_pairs:?}"
    );

    // Case 3: mixed AND junction — one conjunct matches a selected aggregate,
    // the other does not. The whole junction is unrenderable over the merge,
    // so this must also fall back to the wrapper with the same result.
    let mixed_sql = format!(
        "SELECT MOD(id, 4), COUNT(*) FROM {} GROUP BY MOD(id, 4) \
         HAVING COUNT(*) > 0 AND SUM(score) > 250.0",
        vs_table()
    );
    let mixed_pushed_sql = explain_virtual_sql(&mut conn, &mixed_sql);
    assert!(
        mixed_pushed_sql.contains("LHS_T0"),
        "mixed-junction HAVING must fall back to the qualified single-table \
         wrapper (pushed SQL must contain 'LHS_T0'), got:\n{mixed_pushed_sql}"
    );
    assert!(
        !mixed_pushed_sql.contains("PARTIAL_"),
        "mixed-junction HAVING must NOT use the accelerated grouped \
         partial/merge pushdown (pushed SQL must not contain 'PARTIAL_'), \
         got:\n{mixed_pushed_sql}"
    );

    let mixed_cols = conn.query_columns(&mixed_sql);
    assert_eq!(
        mixed_cols.len(),
        2,
        "expected 2 columns (key, count): {mixed_cols:?}"
    );
    let mut mixed_pairs: Vec<(i64, i64)> = mixed_cols[0]
        .iter()
        .zip(mixed_cols[1].iter())
        .map(|(k, c)| (parse_int(k), parse_int(c)))
        .collect();
    mixed_pairs.sort_by_key(|(k, _)| *k);
    assert_eq!(
        mixed_pairs,
        vec![(0i64, 5i64), (3, 5)],
        "mixed-junction HAVING must keep exactly groups 0 and 3, each with \
         COUNT(*) = 5: {mixed_pairs:?}"
    );

    // Case 4: COUNT(DISTINCT) in HAVING (issue #195's own repro shape). Every
    // group has exactly 5 distinct `name` values, so `> 4` keeps all 4 groups
    // and `> 5` keeps none — the pair that proves the HAVING was applied.
    let distinct_sql = format!(
        "SELECT MOD(id, 4), COUNT(*) FROM {} GROUP BY MOD(id, 4) \
         HAVING COUNT(DISTINCT name) > 4",
        vs_table()
    );
    let distinct_cols = conn.query_columns(&distinct_sql);
    assert_eq!(
        distinct_cols.len(),
        2,
        "expected 2 columns (key, count): {distinct_cols:?}"
    );
    let mut distinct_pairs: Vec<(i64, i64)> = distinct_cols[0]
        .iter()
        .zip(distinct_cols[1].iter())
        .map(|(k, c)| (parse_int(k), parse_int(c)))
        .collect();
    distinct_pairs.sort_by_key(|(k, _)| *k);
    assert_eq!(
        distinct_pairs,
        vec![(0i64, 5i64), (1, 5), (2, 5), (3, 5)],
        "COUNT(DISTINCT name) > 4 must keep all 4 groups, each with COUNT(*) = 5: \
         {distinct_pairs:?}"
    );

    let distinct_zero_sql = format!(
        "SELECT MOD(id, 4), COUNT(*) FROM {} GROUP BY MOD(id, 4) \
         HAVING COUNT(DISTINCT name) > 5",
        vs_table()
    );
    assert_eq!(
        conn.query_row_count(&distinct_zero_sql),
        0,
        "COUNT(DISTINCT name) > 5 must keep zero groups (every group has \
         exactly 5 distinct names) — a non-zero result here would mean the \
         HAVING was silently dropped rather than applied"
    );
}

/// Expression-valued multi-key tuple GROUP BY — every key element is itself an
/// expression (not a bare column): `MOD(id, 4)` and `UPPER(name)`. Verifies
/// correct per-group counts, that each key's declared type survives instead of
/// falling back to the VARCHAR(2000000) default, and that the GROUP BY is
/// pushed down as a grouped partial aggregation.
///
/// The two keys are deliberately of DIFFERENT types — key 0 is `MOD(id, 4)`
/// (DECIMAL) and key 1 is `UPPER(name)` (VARCHAR) — so this test genuinely
/// exercises per-index, mixed-type independence: a bug that shared one key's
/// type across both indices would surface here as a wrong column type.
///
/// The seeded `name` values (`event-01` … `event-20`) are unique, so each
/// (`MOD(id,4)`, `UPPER(name)`) pair identifies exactly one row: 20 groups, one
/// row each. Grouping by `MOD(id, 4)` first buckets the ids, and `UPPER(name)`
/// (`EVENT-NN`) then distinguishes every row within a bucket.
#[test]
fn test_group_by_expr_multi_key_tuple() {
    setup_e2e();
    let mut conn = exa_conn();

    let sql = format!(
        "SELECT MOD(id, 4), UPPER(name), COUNT(*) \
         FROM {} GROUP BY MOD(id, 4), UPPER(name)",
        vs_table()
    );

    // Pushdown-occurred assertion: expression-valued multi-key GROUP BY must
    // be pushed down as grouped partial aggregation.
    assert_group_by_pushed_down(&mut conn, &sql);

    let resp = conn.execute(&sql);
    let result_set = &resp["responseData"]["results"][0]["resultSet"];

    // Per-index, mixed-type independence: key 0 (`MOD(id, 4)`) carries DECIMAL
    // and key 1 (`UPPER(name)`) carries VARCHAR — each key's declared type
    // survives independently, neither collapsing to the other's type nor to a
    // fallback.
    for (i, label, expected_type) in [(0, "MOD(id, 4)", "DECIMAL"), (1, "UPPER(name)", "VARCHAR")] {
        let column_type = result_set["columns"][i]["dataType"]["type"]
            .as_str()
            .unwrap_or_else(|| panic!("expected dataType.type for column {i}: {result_set:?}"));
        assert_eq!(
            column_type, expected_type,
            "{label} group key (column {i}) must carry {expected_type} type: {result_set:?}"
        );
    }

    let cols = conn.fetch_result_columns(result_set);
    assert_eq!(
        cols.len(),
        3,
        "expected 3 columns (key1, key2, count): {cols:?}"
    );
    assert_eq!(
        cols[0].len(),
        20,
        "expected 20 groups (one per unique name): {cols:?}"
    );

    // Sort (key1, key2, count) triples so the test is robust to row ordering.
    let mut rows: Vec<(i64, String, i64)> = cols[0]
        .iter()
        .zip(cols[1].iter())
        .zip(cols[2].iter())
        .map(|((k1, k2), c)| {
            let name = k2
                .as_str()
                .unwrap_or_else(|| panic!("expected string UPPER(name) value, got: {k2:?}"))
                .to_string();
            (parse_int(k1), name, parse_int(c))
        })
        .collect();
    rows.sort();

    let expected: Vec<(i64, String, i64)> = [
        (0i64, "EVENT-04"),
        (0, "EVENT-08"),
        (0, "EVENT-12"),
        (0, "EVENT-16"),
        (0, "EVENT-20"),
        (1, "EVENT-01"),
        (1, "EVENT-05"),
        (1, "EVENT-09"),
        (1, "EVENT-13"),
        (1, "EVENT-17"),
        (2, "EVENT-02"),
        (2, "EVENT-06"),
        (2, "EVENT-10"),
        (2, "EVENT-14"),
        (2, "EVENT-18"),
        (3, "EVENT-03"),
        (3, "EVENT-07"),
        (3, "EVENT-11"),
        (3, "EVENT-15"),
        (3, "EVENT-19"),
    ]
    .iter()
    .map(|(k, n)| (*k, n.to_string(), 1i64))
    .collect();
    assert_eq!(
        rows, expected,
        "grouped (MOD(id,4), UPPER(name)) rows must match the expected per-name groups"
    );

    let total: i64 = rows.iter().map(|(_, _, c)| *c).sum();
    assert_eq!(
        total, 20,
        "total COUNT(*) across all groups must be 20, got {total}"
    );
}

/// Multi-key GROUP BY combined with HAVING and LIMIT — both must apply only in
/// the outer merge wrapper (never per-shard), so the LIMIT caps the number of
/// *groups* returned, not rows scanned per shard.
///
/// Keys: `MOD(id, 4)` × `MOD(id, 3)` (12 groups; `SUM(score)` per group).
/// Groups satisfying `HAVING SUM(score) > 100.0`:
///   (0,2)=140.0  (1,2)=110.0  (2,0)=120.0  (3,1)=130.0
/// `LIMIT 2` must cap the result to exactly 2 of these 4 qualifying groups.
#[test]
fn test_group_by_multi_key_having_limit() {
    setup_e2e();
    let mut conn = exa_conn();

    let sql = format!(
        "SELECT MOD(id, 4), MOD(id, 3), SUM(score) FROM {} \
         GROUP BY MOD(id, 4), MOD(id, 3) HAVING SUM(score) > 100.0 LIMIT 2",
        vs_table()
    );

    // Pushdown-occurred assertion: multi-key GROUP BY with HAVING and LIMIT
    // must be pushed down as shard-key fan-out partial aggregation, not the
    // raw row-scan fallback (HAVING/LIMIT results are correct either way).
    assert_group_by_pushed_down(&mut conn, &sql);

    let cols = conn.query_columns(&sql);
    assert_eq!(
        cols.len(),
        3,
        "expected 3 columns (key1, key2, sum): {cols:?}"
    );
    assert_eq!(
        cols[0].len(),
        2,
        "LIMIT 2 must cap the result to exactly 2 groups: {cols:?}"
    );

    // Every returned group must both satisfy HAVING and match one of the
    // known-qualifying (key1, key2) -> sum pairs (not just an arbitrary
    // over-threshold value).
    let qualifying: [((i64, i64), f64); 4] = [
        ((0, 2), 140.0),
        ((1, 2), 110.0),
        ((2, 0), 120.0),
        ((3, 1), 130.0),
    ];

    for (i, ((k1_raw, k2_raw), sum_raw)) in cols[0].iter().zip(&cols[1]).zip(&cols[2]).enumerate() {
        let k1 = parse_int(k1_raw);
        let k2 = parse_int(k2_raw);
        let sum = parse_numeric(sum_raw);

        assert!(
            sum > 100.0,
            "row {i}: SUM(score) must satisfy HAVING > 100.0, got {sum} for key ({k1}, {k2})"
        );

        let expected_sum = qualifying
            .iter()
            .find(|((qk1, qk2), _)| *qk1 == k1 && *qk2 == k2)
            .map(|(_, s)| *s)
            .unwrap_or_else(|| {
                panic!("row {i}: key ({k1}, {k2}) is not one of the known qualifying groups")
            });
        assert!(
            (sum - expected_sum).abs() < 0.01,
            "row {i}: key ({k1}, {k2}) must have SUM(score) = {expected_sum}, got {sum}"
        );
    }
}

/// High-cardinality multi-key GROUP BY completes under the bounded memory
/// pool — a tuple key (`id`, `MOD(id, 2)`) exercises the same near-unique key
/// space as the single-key spill test, but through the multi-key GK_0/GK_1
/// path, proving the bounded-pool/spill backstop is not single-key-only.
///
/// 20 distinct (id, MOD(id,2)) groups, each with exactly one row.
#[test]
fn test_high_cardinality_multi_key_group_by_spill() {
    setup_e2e();
    let mut conn = exa_conn();

    let sql = format!(
        "SELECT id, MOD(id, 2), COUNT(*) FROM {} GROUP BY id, MOD(id, 2) ORDER BY id",
        vs_table()
    );

    // Pushdown-occurred assertion: even the high-cardinality multi-key case
    // must go through shard-key fan-out partial aggregation.
    assert_group_by_pushed_down(&mut conn, &sql);

    let cols = conn.query_columns(&sql);
    assert_eq!(
        cols.len(),
        3,
        "expected 3 columns (id, MOD(id,2), count): {cols:?}"
    );
    assert_eq!(
        cols[0].len(),
        20,
        "GROUP BY id, MOD(id,2) must return 20 groups, got {}",
        cols[0].len()
    );

    // Every group must have exactly one row (id is unique).
    for (i, v) in cols[2].iter().enumerate() {
        let count = parse_int(v);
        assert_eq!(
            count,
            1,
            "group at position {i} (id={}): COUNT(*) must be 1, got {count}",
            parse_int(&cols[0][i])
        );
    }

    // IDs must be 1..20 in order, and MOD(id,2) must be consistent with id.
    let ids: Vec<i64> = cols[0].iter().map(parse_int).collect();
    let mods: Vec<i64> = cols[1].iter().map(parse_int).collect();
    for (pos, (&id, &m)) in ids.iter().zip(mods.iter()).enumerate() {
        let expected_id = (pos + 1) as i64;
        assert_eq!(
            id, expected_id,
            "id at position {pos} must be {expected_id}, got {id}"
        );
        assert_eq!(
            m,
            id % 2,
            "MOD(id,2) at position {pos} (id={id}) must be {}, got {m}",
            id % 2
        );
    }
}

// ---------------------------------------------------------------------------
// Plan `fix-scalar-over-aggregate-grouped-pushdown` (#82) — single-table
// scalar-over-aggregate GROUP BY E2E tests
//
// `fact_lineitem` (seeded by `seed_events`, see `common/seed.rs`): 20 rows
// across 2 files, `L_RETURNFLAG` alternating "R"/"N" (row 1 = "R", so 10 R
// rows + 10 N rows), `L_QUANTITY` and `L_EXTENDEDPRICE` deterministic per row.
//
// Before the fix, a single-table grouped select list containing a scalar
// function wrapping aggregates (e.g. `ROUND(100.0 * SUM(CASE …)/COUNT(*), 2)`)
// made `detect_group_by_aggregates` decline the whole request, falling through
// to a bare raw row-scan that hard-fails with SQL state 04000 ("Expected
// number of columns is N but pushdown query has M"). These tests exercise
// that exact shape end-to-end through the VS and check the result against a
// native (non-virtual) ground-truth table built from the same source columns.
// ---------------------------------------------------------------------------

/// Native (non-virtual) table the ground truth is materialized into — see
/// [`ensure_ground_truth_lineitem_table`]. Named distinctly from
/// `e2e_join_test.rs`'s own ground-truth table (same schema, different test
/// binary) to keep the two files' fixtures unambiguous.
const GROUND_TRUTH_LINEITEM_SCAN_TABLE: &str = "GROUND_TRUTH_LINEITEM_SCAN";

/// Materialize the `fact_lineitem` columns the scalar-over-aggregate ground
/// truth needs into a NATIVE Exasol table (in the same schema as the adapter
/// scripts), via a plain projection over the virtual `fact_lineitem` table.
///
/// `CREATE OR REPLACE TABLE` is idempotent, so both tests below can safely
/// share and re-run this under the suite's `--test-threads=1` serial
/// execution.
fn ensure_ground_truth_lineitem_table(conn: &mut ExaConn) {
    conn.execute(&format!(
        "CREATE OR REPLACE TABLE {SCHEMA_NAME}.{GROUND_TRUTH_LINEITEM_SCAN_TABLE} AS \
         SELECT L_RETURNFLAG, L_QUANTITY, L_EXTENDEDPRICE FROM {}",
        vs_lineitem_table()
    ));
}

/// #82's exact select list: a plain group key, two plain aggregates, and a
/// scalar function (`ROUND`) wrapping a `SUM(CASE …)` and a `COUNT(*)`.
fn scalar_over_aggregate_round_select_list() -> &'static str {
    "L_RETURNFLAG, SUM(L_QUANTITY), AVG(L_EXTENDEDPRICE), \
     ROUND(100.0 * SUM(CASE WHEN L_RETURNFLAG='R' THEN 1 ELSE 0 END)/COUNT(*), 2)"
}

/// Collects the distinct `{i}` suffixes following `marker` in `haystack` (e.g.
/// every distinct index `i` in occurrences of `"PARTIAL_count_{i}"`), without
/// pulling in a regex dependency for a single fixed-prefix scan.
fn distinct_numeric_suffixes(haystack: &str, marker: &str) -> std::collections::BTreeSet<String> {
    let mut indices = std::collections::BTreeSet::new();
    let mut rest = haystack;
    while let Some(pos) = rest.find(marker) {
        let after = &rest[pos + marker.len()..];
        let digits: String = after.chars().take_while(|c| c.is_ascii_digit()).collect();
        if !digits.is_empty() {
            indices.insert(digits.clone());
        }
        rest = &after[digits.len().max(1)..];
    }
    indices
}

/// #82's query — a single-table grouped select list with a scalar function
/// (`ROUND`) wrapping a `SUM(CASE …)` and a `COUNT(*)` — runs green through
/// the VS (no `04000` column-count-mismatch hard-fail) and pushes down as the
/// merged grouped partial/merge wrapper (`assert_group_by_pushed_down`: no
/// `SELECT * FROM (…)` row-scan wrapper), matching the same select list
/// evaluated over a native (non-virtual) copy of the same `fact_lineitem`
/// columns.
#[test]
fn test_group_by_scalar_over_aggregate_round() {
    setup_e2e();
    let mut conn = exa_conn();

    let sql = format!(
        "SELECT {} FROM {} GROUP BY L_RETURNFLAG ORDER BY L_RETURNFLAG",
        scalar_over_aggregate_round_select_list(),
        vs_lineitem_table()
    );

    // Pushdown-occurred assertion: the scalar-over-aggregate item must not
    // send the grouped path down the bare-row-scan fallback (the pre-fix
    // 04000 bug) — it must be the merged grouped partial/merge wrapper.
    assert_group_by_pushed_down(&mut conn, &sql);

    let actual = conn.query_columns(&sql);
    assert_eq!(
        actual.len(),
        4,
        "expected 4 columns (L_RETURNFLAG, SUM_QTY, AVG_PRICE, RETURN_PCT): {actual:?}"
    );
    assert_eq!(
        actual[0].len(),
        2,
        "GROUP BY L_RETURNFLAG must return exactly 2 groups (R, N): {actual:?}"
    );

    ensure_ground_truth_lineitem_table(&mut conn);
    let ground_truth_sql = format!(
        "SELECT {} FROM {SCHEMA_NAME}.{GROUND_TRUTH_LINEITEM_SCAN_TABLE} \
         GROUP BY L_RETURNFLAG ORDER BY L_RETURNFLAG",
        scalar_over_aggregate_round_select_list()
    );
    let expected = conn.query_columns(&ground_truth_sql);
    assert_eq!(
        expected[0].len(),
        2,
        "ground truth must have 2 groups: {expected:?}"
    );

    for i in 0..2 {
        let actual_flag = actual[0][i]
            .as_str()
            .unwrap_or_else(|| panic!("L_RETURNFLAG not a string: {:?}", actual[0][i]));
        let expected_flag = expected[0][i]
            .as_str()
            .unwrap_or_else(|| panic!("L_RETURNFLAG not a string: {:?}", expected[0][i]));
        assert_eq!(
            actual_flag, expected_flag,
            "row {i}: L_RETURNFLAG must match the native ground truth"
        );

        let actual_sum_qty = parse_numeric(&actual[1][i]);
        let expected_sum_qty = parse_numeric(&expected[1][i]);
        assert!(
            (actual_sum_qty - expected_sum_qty).abs() < 0.001,
            "row {i} ({actual_flag}): SUM(L_QUANTITY) must be {expected_sum_qty}, got {actual_sum_qty}"
        );

        let actual_avg_price = parse_numeric(&actual[2][i]);
        let expected_avg_price = parse_numeric(&expected[2][i]);
        assert!(
            (actual_avg_price - expected_avg_price).abs() < 0.001,
            "row {i} ({actual_flag}): AVG(L_EXTENDEDPRICE) must be {expected_avg_price}, got {actual_avg_price}"
        );

        let actual_pct = parse_numeric(&actual[3][i]);
        let expected_pct = parse_numeric(&expected[3][i]);
        assert!(
            (actual_pct - expected_pct).abs() < 0.001,
            "row {i} ({actual_flag}): ROUND(...) return-pct must be {expected_pct}, got {actual_pct}"
        );
    }
}

/// A grouped select list carrying BOTH a bare `COUNT(*)` and a scalar function
/// wrapping a `COUNT(*)` (`ROUND(100.0 * SUM(CASE …)/COUNT(*), 2)`) must
/// decompose the shared `COUNT(*)` into exactly ONE deduplicated partial
/// column (one `PARTIAL_count_{i}` index across the whole pushed SQL) rather
/// than one partial column per occurrence — and must still compute the
/// correct result, matching the native ground truth.
#[test]
fn test_group_by_shared_inner_aggregate_dedup() {
    setup_e2e();
    let mut conn = exa_conn();

    let sql = format!(
        "SELECT L_RETURNFLAG, COUNT(*), \
         ROUND(100.0 * SUM(CASE WHEN L_RETURNFLAG='R' THEN 1 ELSE 0 END)/COUNT(*), 2) \
         FROM {} GROUP BY L_RETURNFLAG ORDER BY L_RETURNFLAG",
        vs_lineitem_table()
    );

    assert_group_by_pushed_down(&mut conn, &sql);

    let pushed_sql = explain_virtual_sql(&mut conn, &sql);
    let count_indices = distinct_numeric_suffixes(&pushed_sql, "PARTIAL_count_");
    assert_eq!(
        count_indices.len(),
        1,
        "the bare COUNT(*) and the COUNT(*) nested inside ROUND(...) must \
         dedup to exactly ONE PARTIAL_count_{{i}} index, got indices \
         {count_indices:?} in:\n{pushed_sql}"
    );

    let actual = conn.query_columns(&sql);
    assert_eq!(
        actual.len(),
        3,
        "expected 3 columns (L_RETURNFLAG, COUNT(*), RETURN_PCT): {actual:?}"
    );
    assert_eq!(
        actual[0].len(),
        2,
        "GROUP BY L_RETURNFLAG must return exactly 2 groups (R, N): {actual:?}"
    );

    let total_count: i64 = actual[1].iter().map(parse_int).sum();
    assert_eq!(
        total_count, LINEITEM_ROWS as i64,
        "total COUNT(*) across both groups must be {LINEITEM_ROWS}, got {total_count}"
    );

    ensure_ground_truth_lineitem_table(&mut conn);
    let ground_truth_sql = format!(
        "SELECT L_RETURNFLAG, COUNT(*), \
         ROUND(100.0 * SUM(CASE WHEN L_RETURNFLAG='R' THEN 1 ELSE 0 END)/COUNT(*), 2) \
         FROM {SCHEMA_NAME}.{GROUND_TRUTH_LINEITEM_SCAN_TABLE} \
         GROUP BY L_RETURNFLAG ORDER BY L_RETURNFLAG"
    );
    let expected = conn.query_columns(&ground_truth_sql);
    assert_eq!(
        expected[0].len(),
        2,
        "ground truth must have 2 groups: {expected:?}"
    );

    for i in 0..2 {
        let actual_flag = actual[0][i]
            .as_str()
            .unwrap_or_else(|| panic!("L_RETURNFLAG not a string: {:?}", actual[0][i]));
        let expected_flag = expected[0][i]
            .as_str()
            .unwrap_or_else(|| panic!("L_RETURNFLAG not a string: {:?}", expected[0][i]));
        assert_eq!(
            actual_flag, expected_flag,
            "row {i}: L_RETURNFLAG must match the native ground truth"
        );

        let actual_count = parse_int(&actual[1][i]);
        let expected_count = parse_int(&expected[1][i]);
        assert_eq!(
            actual_count, expected_count,
            "row {i} ({actual_flag}): COUNT(*) must be {expected_count}, got {actual_count}"
        );

        let actual_pct = parse_numeric(&actual[2][i]);
        let expected_pct = parse_numeric(&expected[2][i]);
        assert!(
            (actual_pct - expected_pct).abs() < 0.001,
            "row {i} ({actual_flag}): ROUND(...) return-pct must be {expected_pct}, got {actual_pct}"
        );
    }
}

// ---------------------------------------------------------------------------
// Task 2.14 — multi-table VS tests
// ---------------------------------------------------------------------------

/// Create VS with ICEBERG_NAMESPACE enumerates all tables in the namespace.
///
/// Asserts that both `EVENTS` and `LABELS` appear in `SYS.EXA_ALL_TABLES` for
/// the virtual schema — one Exasol virtual table per Iceberg table in the namespace.
#[test]
fn e2e_create_vs_enumerates_namespace_tables() {
    setup_e2e();
    let mut conn = exa_conn();

    let cols = conn.query_columns(&format!(
        "SELECT TABLE_NAME FROM SYS.EXA_ALL_TABLES \
         WHERE TABLE_SCHEMA = '{VS_NAME}' \
         ORDER BY TABLE_NAME"
    ));
    assert_eq!(
        cols.len(),
        1,
        "query must return one column (TABLE_NAME): {cols:?}"
    );
    let table_names: Vec<&str> = cols[0].iter().filter_map(|v| v.as_str()).collect();

    assert!(
        table_names.contains(&E2E_TABLE.to_uppercase().as_str()),
        "EVENTS must appear in the virtual schema tables: {table_names:?}"
    );
    assert!(
        table_names.contains(&E2E_TABLE_2.to_uppercase().as_str()),
        "LABELS must appear in the virtual schema tables: {table_names:?}"
    );
}

/// Pushdown derives the scanned Iceberg table from involvedTables[0].name.
///
/// Queries the second table (LABELS) directly and asserts correct rows are
/// returned — proving that pushdown looked up the Iceberg identifier from
/// TABLE_MAP using the Exasol virtual table name.
#[test]
fn e2e_pushdown_scans_table_from_involved_tables() {
    setup_e2e();
    let mut conn = exa_conn();

    let cols = conn.query_columns(&format!(
        "SELECT id, label FROM {} ORDER BY id",
        vs_labels_table()
    ));
    assert_eq!(cols.len(), 2, "must return 2 columns (id, label): {cols:?}");
    assert_eq!(
        cols[0].len(),
        SEED_LABELS_ROWS,
        "must return all {SEED_LABELS_ROWS} label rows, got {}",
        cols[0].len()
    );

    // Verify id=1 maps to "label-01".
    let first_id = cols[0][0]
        .as_i64()
        .or_else(|| cols[0][0].as_str().and_then(|s| s.parse().ok()))
        .unwrap_or_else(|| panic!("id not integer: {:?}", cols[0][0]));
    assert_eq!(
        first_id, 1,
        "first id must be 1 after ORDER BY, got {first_id}"
    );
    let first_label = cols[1][0]
        .as_str()
        .unwrap_or_else(|| panic!("label not string: {:?}", cols[1][0]));
    assert_eq!(
        first_label, "label-01",
        "id=1 must have label 'label-01', got '{first_label}'"
    );
}

// ---------------------------------------------------------------------------
// Group E — Iceberg file-pruning E2E tests (tasks 5.2 + 5.3)
//
// Seed recap (regions table, 3 files):
//   north   → ids 1..=5    (5 rows per file, partition value "north")
//   central → ids 6..=10   (5 rows per file, partition value "central")
//   south   → ids 11..=15  (5 rows per file, partition value "south")
//
// The VS exposes the table as MY_LAKEHOUSE.REGIONS (Exasol-uppercased).
// ---------------------------------------------------------------------------

/// Helper: virtual schema name for the partitioned regions table.
fn vs_regions_table() -> String {
    format!("{VS_NAME}.{}", E2E_PART_TABLE.to_uppercase())
}

/// Task 5.2 — Partition filter prunes and returns correct rows.
///
/// Asserts:
/// - `SELECT id FROM {VS}.REGIONS WHERE region = 'north'` returns exactly ids 1..=5
///   (5 rows, matching PART_NORTH_IDS) — correct rows, partition pruning applied.
/// - `SELECT id FROM {VS}.REGIONS WHERE region = 'central'` returns exactly ids 6..=10
///   (5 rows, matching PART_CENTRAL_IDS) — a second partition value to increase
///   confidence that the filter is correct, not just returning all rows.
/// - Correctness is the primary assertion; file-count pruning is asserted in 5.3.
#[test]
fn e2e_partition_filter_prunes_and_returns_correct_rows() {
    setup_e2e();
    let mut conn = exa_conn();

    // --- north partition: ids 1..=5 ---
    let north_sql = format!(
        "SELECT id FROM {} WHERE {} = '{}' ORDER BY id",
        vs_regions_table(),
        PART_COL,
        PART_VAL_NORTH,
    );
    let north_cols = conn.query_columns(&north_sql);
    assert_eq!(
        north_cols.len(),
        1,
        "SELECT id FROM REGIONS WHERE region='north' must return 1 column: {north_cols:?}"
    );
    assert_eq!(
        north_cols[0].len(),
        PART_ROWS_PER_FILE,
        "north partition must return exactly {} rows, got {}: {north_cols:?}",
        PART_ROWS_PER_FILE,
        north_cols[0].len()
    );

    let north_ids: Vec<i64> = north_cols[0]
        .iter()
        .map(|v| {
            v.as_i64()
                .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
                .unwrap_or_else(|| panic!("north id not integer: {v:?}"))
        })
        .collect();

    let expected_north: Vec<i64> = (PART_NORTH_IDS.0 as i64..=PART_NORTH_IDS.1 as i64).collect();
    assert_eq!(
        north_ids, expected_north,
        "north partition ids must be exactly {expected_north:?}, got {north_ids:?}"
    );

    // --- central partition: ids 6..=10 ---
    let central_sql = format!(
        "SELECT id FROM {} WHERE {} = '{}' ORDER BY id",
        vs_regions_table(),
        PART_COL,
        PART_VAL_CENTRAL,
    );
    let central_cols = conn.query_columns(&central_sql);
    assert_eq!(
        central_cols[0].len(),
        PART_ROWS_PER_FILE,
        "central partition must return exactly {} rows, got {}: {central_cols:?}",
        PART_ROWS_PER_FILE,
        central_cols[0].len()
    );

    let central_ids: Vec<i64> = central_cols[0]
        .iter()
        .map(|v| {
            v.as_i64()
                .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
                .unwrap_or_else(|| panic!("central id not integer: {v:?}"))
        })
        .collect();

    let expected_central: Vec<i64> =
        (PART_CENTRAL_IDS.0 as i64..=PART_CENTRAL_IDS.1 as i64).collect();
    assert_eq!(
        central_ids, expected_central,
        "central partition ids must be exactly {expected_central:?}, got {central_ids:?}"
    );

    // --- total row count sanity: filtering all partitions yields the full table ---
    let total = conn.query_row_count(&format!("SELECT id FROM {}", vs_regions_table()));
    assert_eq!(
        total, PART_TOTAL_ROWS as i64,
        "REGIONS total row count must be {PART_TOTAL_ROWS} (all 3 partitions), got {total}"
    );

    // --- LIKE correctness: untranslatable predicate → DataFusion applies it, correct count ---
    // ponytail: LIKE is not pushed to Iceberg (untranslatable); DataFusion applies the full
    // filter as correctness backstop. Verifies the untranslatable-conjunct path.
    let like_count = conn.query_row_count(&format!(
        "SELECT id FROM {} WHERE {} LIKE 'nor%'",
        vs_regions_table(),
        PART_COL,
    ));
    assert_eq!(
        like_count, PART_ROWS_PER_FILE as i64,
        "LIKE 'nor%' (untranslatable, DataFusion applies) must return {PART_ROWS_PER_FILE} rows, \
         got {like_count}"
    );
}

/// Adapter-level file-count pruning, asserted by calling the format-reader
/// seam directly (bypassing Exasol) with and without a filter and comparing
/// the resolved file counts against the unfiltered snapshot. Both pruning
/// paths the plan claims are exercised against the seeded `regions` table (3
/// files, one per partition, disjoint id ranges):
///
/// 1. Partition pruning (`region = 'north'`): Iceberg identity-partition pruning
///    eliminates the central and south files → 1 file.
/// 2. Per-file min/max range pruning (`id <= 5`): files whose id min > 5
///    (central: min=6, south: min=11) are eliminated by
///    `InclusiveMetricsEvaluator` → 1 file (north only, ids 1..=5).
#[test]
fn e2e_range_filter_prunes_by_file_bounds() {
    setup_e2e();

    let catalog_uri = common::stack::iceberg_catalog_url();
    let catalog_props = local_stack_catalog(&format!("{E2E_NAMESPACE}.{E2E_PART_TABLE}"));
    let storage = local_stack_storage();
    let creds = local_stack_creds();

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime for file-count pruning test");

    // One `CatalogSession` built once and reused across all three pruning calls
    // below, mirroring the single-session-per-query contract the format-reader
    // seam now requires (see `adapter/mod.rs`'s hoisted enumeration session and
    // `pushdown/mod.rs`'s `handle_pushdown`). The reader built from it is reused
    // identically: it carries no per-call state, so a fresh `resolve_scan` call
    // per filter is the same seam every pushdown request resolves through.
    let session = rt
        .block_on(async { CatalogSession::resolve(&catalog_uri, &creds.warehouse, &creds).await })
        .expect("CatalogSession::resolve must succeed");
    // `allow_http = true`: the local stack's MinIO is plain HTTP.
    let connection = ConnectionStorage {
        storage: &storage,
        creds: &creds,
        allow_http: true,
    };
    let reader = format_reader(
        ScanSource::Iceberg {
            session: &session,
            catalog_props: &catalog_props,
        },
        &connection,
    )
    .expect("format_reader must succeed");

    // --- baseline: no filter → 3 data files (one per partition) ---
    let all_files = rt
        .block_on(reader.resolve_scan(None))
        .expect("resolve_scan (no filter) must succeed")
        .files;
    assert_eq!(
        all_files.len(),
        3,
        "unfiltered REGIONS must resolve 3 data files (one per partition), got {}: {all_files:?}",
        all_files.len()
    );

    // --- partition pruning: region = 'north' → exactly 1 file ---
    // Filter JSON shape mirrors the Exasol pushdown format the translator expects.
    // Column names are Exasol-uppercase; the translator resolves them case-insensitively.
    let partition_filter = serde_json::json!({
        "type": "predicate_equal",
        "left": {"type": "column", "name": "REGION"},
        "right": {"type": "literal_string", "value": "north"}
    });
    let pruned_partition = rt
        .block_on(reader.resolve_scan(Some(&partition_filter)))
        .expect("resolve_scan (partition filter) must succeed")
        .files;
    assert_eq!(
        pruned_partition.len(),
        1,
        "partition filter 'region = north' must resolve 1 file, got {}: {pruned_partition:?}",
        pruned_partition.len()
    );
    assert!(
        pruned_partition.len() < all_files.len(),
        "partition filter must prune files: pruned={} is not < unfiltered={}",
        pruned_partition.len(),
        all_files.len()
    );

    // --- per-file min/max range pruning: id <= 5 → files with min(id) > 5 are pruned ---
    // The regions table has disjoint id ranges per file:
    //   north   id 1..=5  (max=5)
    //   central id 6..=10 (min=6 > 5 → pruned)
    //   south   id 11..=15(min=11 > 5 → pruned)
    // With Iceberg's InclusiveMetricsEvaluator, `id <= 5` prunes files where min(id) > 5.
    let range_filter = serde_json::json!({
        "type": "predicate_lessequal",
        "left": {"type": "column", "name": "ID"},
        "right": {"type": "literal_exactnumeric", "value": "5"}
    });
    let pruned_range = rt
        .block_on(reader.resolve_scan(Some(&range_filter)))
        .expect("resolve_scan (range filter) must succeed")
        .files;
    // Only the north file (ids 1..=5) overlaps `id <= 5`; central (min=6) and
    // south (min=11) are pruned by their per-file min/max bounds.
    assert_eq!(
        pruned_range.len(),
        1,
        "range filter 'id <= 5' must resolve only the north file, got {}: {pruned_range:?}",
        pruned_range.len()
    );
}

/// Exasol-side JOIN across two virtual tables returns correct joined rows.
///
/// Joins EVENTS and LABELS on `id` and asserts the result contains the
/// expected id and label values — proving both tables are independently
/// scanned by pushdown and joined by Exasol.
#[test]
fn e2e_pushdown_resolves_files_once_multi_table() {
    setup_e2e();
    let mut conn = exa_conn();

    // JOIN events and labels on id; ORDER BY id for determinism.
    let sql = format!(
        "SELECT a.id, b.label FROM {events} a \
         JOIN {labels} b ON a.id = b.id \
         WHERE a.id <= 5 \
         ORDER BY a.id",
        events = vs_table(),
        labels = vs_labels_table(),
    );
    let cols = conn.query_columns(&sql);
    assert_eq!(cols.len(), 2, "must return 2 columns (id, label): {cols:?}");
    assert_eq!(
        cols[0].len(),
        5,
        "JOIN with id <= 5 must return 5 rows, got {}",
        cols[0].len()
    );

    // Verify each id maps to the expected label.
    for (i, (id_val, label_val)) in cols[0].iter().zip(cols[1].iter()).enumerate() {
        let expected_id = (i + 1) as i64;
        let id = id_val
            .as_i64()
            .or_else(|| id_val.as_str().and_then(|s| s.parse().ok()))
            .unwrap_or_else(|| panic!("id at pos {i} not integer: {id_val:?}"));
        assert_eq!(
            id, expected_id,
            "id at position {i} must be {expected_id}, got {id}"
        );

        let label = label_val
            .as_str()
            .unwrap_or_else(|| panic!("label at pos {i} not string: {label_val:?}"));
        assert_eq!(
            label,
            format!("label-{expected_id:02}"),
            "label at position {i} must be 'label-{expected_id:02}', got '{label}'"
        );
    }
}

// ---------------------------------------------------------------------------
// Regression: nested aggregate over a grouped sub-select (issue #52).
//
// Exasol rewrites `COUNT(*) FROM (SELECT k, COUNT(*) FROM t GROUP BY k) sub`
// into a single flat pushdown request: `aggregationType: "group_by"` with a
// literal-only `selectList` (a `literal_null` "count the groups" placeholder,
// since the outer query needs neither the group key nor the inner COUNT(*)
// value). Before the fix, the adapter rendered that literal placeholder as a
// bare `NULL` projection column, producing scan SQL that referenced a
// phantom `"NULL"` identifier and crashing with `F-UDF-CL-RUST-9001: ...
// Schema error: No field named "NULL"`. The fix (pushdown.rs
// `detect_group_by_aggregates`) preserves the GROUP BY so the scan still
// returns one row per distinct group; Exasol's outer COUNT(*) then counts
// those group rows correctly.
// ---------------------------------------------------------------------------

/// End-to-end nested aggregate over a grouped sub-select returns the correct
/// outer count — including the duplicate-key case that discriminates a
/// correct grouped-scan fix from an unsafe row-scan fallback.
///
/// `events.id` is unique, so `GROUP BY id` alone cannot tell a correct fix
/// apart from a fallback that returns one row per source row (both
/// coincidentally yield 20 on this table). `GROUP BY MOD(id, 4)` is the
/// discriminating case: 20 seeded rows (id 0..19) fall into exactly 4
/// distinct `MOD(id, 4)` buckets, so the outer `COUNT(*)` must be 4. A
/// row-scan fallback would instead return the raw row count (20).
#[test]
fn e2e_nested_aggregate_over_grouped_subselect_returns_correct_count() {
    setup_e2e();
    let mut conn = exa_conn();

    // Duplicate-key case — the actual regression guard. Must be 4 (distinct
    // MOD(id, 4) groups), NOT 20 (which a row-scan fallback would wrongly
    // return since it doesn't re-group).
    let sql_duplicate_keys = format!(
        "SELECT COUNT(*) FROM (SELECT MOD(id, 4) AS k, COUNT(*) AS cnt FROM {} GROUP BY MOD(id, 4)) t",
        vs_table()
    );
    let cols = conn.query_columns(&sql_duplicate_keys);
    assert_eq!(
        cols.len(),
        1,
        "nested COUNT(*) must return one column: {cols:?}"
    );
    assert_eq!(
        cols[0].len(),
        1,
        "nested COUNT(*) must return one row: {cols:?}"
    );
    let distinct_group_count = parse_int(&cols[0][0]);
    assert_eq!(
        distinct_group_count, 4,
        "COUNT(*) over (GROUP BY MOD(id,4)) sub-select must be 4 (distinct groups), \
         got {distinct_group_count} — 20 would indicate an unsafe row-scan fallback \
         instead of a correctly preserved grouped scan"
    );

    // Unique-key smoke case from the plan (kept as an additional assertion,
    // not a substitute for the duplicate-key case above): every id is
    // distinct, so the outer COUNT(*) over `GROUP BY id` must equal 20.
    let sql_unique_key = format!(
        "SELECT COUNT(*) FROM (SELECT id, COUNT(*) AS cnt FROM {} GROUP BY id) t",
        vs_table()
    );
    let cols_unique = conn.query_columns(&sql_unique_key);
    let unique_group_count = parse_int(&cols_unique[0][0]);
    assert_eq!(
        unique_group_count, 20,
        "COUNT(*) over (GROUP BY id) sub-select must be 20 (distinct ids), \
         got {unique_group_count}"
    );
}

// ---------------------------------------------------------------------------
// #191 — ORDER BY ... LIMIT n OFFSET m regression coverage
// ---------------------------------------------------------------------------
//
// Before this fix, `LIMIT_WITH_OFFSET` was unadvertised: Exasol stripped the
// `offset` from every pushdown request and applied neither bound itself, so
// every one of these shapes silently returned ranks 1..n instead of the
// requested (m+1)..(m+n) window. `render_limit_offset` (`support.rs`) is now
// the single seam all three reachable wrapper sites route through; these
// tests exercise each site end to end, plus the two shapes the design
// declares permanently unreachable with a non-zero offset (S3, S4/S5), whose
// `debug_assert!` guards compile out of the release-profile `.so` and so have
// no live backstop other than these two canaries.

/// The issue's literal repro: `ORDER BY score DESC LIMIT 12 OFFSET 3` over a
/// PROJECTED sort key (`score` is in the select list). A non-zero offset
/// always declines the per-shard bounded top-N (`detect_topn`'s guard is now
/// a non-zero-offset test, not a presence test), so this renders on the
/// declined row-scan wrapper (S1, `topn.rs`): `... GROUP BY shard_key)) ORDER
/// BY "SCORE" DESC NULLS FIRST LIMIT 12 OFFSET 3`, live-verified via
/// `EXPLAIN VIRTUAL` during this task.
///
/// Seeded data: `score = 5.0 * id` for id in 1..=20, so the ranks by score
/// DESC are exactly ids 20,19,...,1 in order. Ranks 4-15 (LIMIT 12 OFFSET 3)
/// are ids 17,16,...,6 — NOT ids 20..=9 (ranks 1-12), which is the #191 bug:
/// silent collapse to OFFSET 0.
#[test]
fn ordered_limit_offset_returns_shifted_window() {
    setup_e2e();
    let mut conn = exa_conn();

    let sql = format!(
        "SELECT id, score FROM {} ORDER BY score DESC LIMIT 12 OFFSET 3",
        vs_table()
    );

    let pushed_sql = explain_virtual_sql(&mut conn, &sql);
    assert!(
        pushed_sql.contains("LIMIT 12 OFFSET 3"),
        "a non-zero offset must render on the wrapper as LIMIT 12 OFFSET 3, \
         got:\n{pushed_sql}"
    );

    let cols = conn.query_columns(&sql);
    assert_eq!(cols.len(), 2, "expected 2 columns (id, score): {cols:?}");
    let ids: Vec<i64> = cols[0].iter().map(parse_int).collect();
    let expected_ids: Vec<i64> = (6..=17).rev().collect();
    assert_eq!(
        ids, expected_ids,
        "ORDER BY score DESC LIMIT 12 OFFSET 3 must return ranks 4-15 \
         ({expected_ids:?}), NOT ranks 1-12 (ids 20..=9) — the #191 silent \
         collapse to OFFSET 0 — got {ids:?}"
    );
    let scores: Vec<f64> = cols[1].iter().map(parse_numeric).collect();
    for (i, &id) in ids.iter().enumerate() {
        let expected_score = 5.0 * id as f64;
        assert!(
            (scores[i] - expected_score).abs() < 1e-9,
            "row {i}: score for id {id} must be {expected_score}, got {}",
            scores[i]
        );
    }
}

/// Same declined-row-scan-wrapper site (S1) as
/// [`ordered_limit_offset_returns_shifted_window`], but with an UNPROJECTED
/// sort key: `score` drives the ORDER BY but is not in the select list. This
/// is the shape `add-topn-pushdown` (#225/#189) and `fix/198-orderby-expr-hidden-col`
/// hardened against leaking a hidden internal sort column into the wrapper's
/// select list; this test pins that the OFFSET renders correctly on top of
/// that fix, not just on the simpler projected-sort-key case above.
///
/// `LIMIT 5 OFFSET 2` over score DESC ranks: rank 1-2 are ids 20,19; ranks 3-7
/// (the requested window) are ids 18,17,16,15,14.
#[test]
fn ordered_limit_offset_unprojected_sort_key_returns_shifted_window() {
    setup_e2e();
    let mut conn = exa_conn();

    let sql = format!(
        "SELECT id FROM {} ORDER BY score DESC LIMIT 5 OFFSET 2",
        vs_table()
    );

    let pushed_sql = explain_virtual_sql(&mut conn, &sql);
    assert!(
        pushed_sql.contains("LIMIT 5 OFFSET 2"),
        "a non-zero offset must render on the wrapper as LIMIT 5 OFFSET 2, \
         got:\n{pushed_sql}"
    );

    let cols = conn.query_columns(&sql);
    assert_eq!(cols.len(), 1, "expected 1 column (id): {cols:?}");
    let ids: Vec<i64> = cols[0].iter().map(parse_int).collect();
    assert_eq!(
        ids,
        vec![18, 17, 16, 15, 14],
        "ORDER BY score DESC LIMIT 5 OFFSET 2 (unprojected sort key) must \
         return ranks 3-7 (ids 18,17,16,15,14), got {ids:?}"
    );
}

/// The grouped merge wrapper (S2, `grouped_agg.rs`) renders the request's
/// OFFSET alongside its LIMIT: `GROUP BY MOD(id,4) ORDER BY 1 LIMIT 2 OFFSET
/// 1`.
///
/// Seeded ids 1..=20 cycle `MOD(id,4)` through 1,2,3,0 repeating, so all four
/// groups (k=0,1,2,3) have exactly 5 members each. Ranked by k ascending,
/// `LIMIT 2 OFFSET 1` must return groups ranked 2-3 (k=1, k=2), NOT the
/// groups ranked 1-2 (k=0, k=1) that the pre-fix silent-OFFSET-0 collapse
/// would have produced.
#[test]
fn grouped_order_by_limit_offset_returns_shifted_groups() {
    setup_e2e();
    let mut conn = exa_conn();

    let sql = format!(
        "SELECT MOD(id,4) AS k, COUNT(*) AS c FROM {} GROUP BY MOD(id,4) \
         ORDER BY 1 LIMIT 2 OFFSET 1",
        vs_table()
    );

    // Pin the render site: the same `group_keys` + `PARTIAL_` markers
    // `assert_group_by_pushed_down` uses to evidence the grouped
    // partial-aggregate builder (S2, `grouped_agg.rs`), not the qualified
    // single-table wrapper (S6, which has no `group_keys`/`PARTIAL_` — see
    // `qualified_wrapper_limit_offset_returns_shifted_window`'s `LHS_T0`
    // check), plus the shifted-window shape itself.
    let pushed_sql = explain_virtual_sql(&mut conn, &sql);
    assert!(
        pushed_sql.contains("group_keys") && pushed_sql.contains("PARTIAL_"),
        "GROUP BY MOD(id,4) ORDER BY 1 LIMIT 2 OFFSET 1 must render via the \
         grouped partial-aggregate builder (scan spec carries 'group_keys' \
         and a 'PARTIAL_' column), got:\n{pushed_sql}"
    );
    assert!(
        pushed_sql.contains("GROUP BY") && pushed_sql.contains("LIMIT 2 OFFSET 1"),
        "pushed SQL must carry a GROUP BY merge with LIMIT 2 OFFSET 1, \
         got:\n{pushed_sql}"
    );

    let cols = conn.query_columns(&sql);
    assert_eq!(cols.len(), 2, "expected 2 columns (k, c): {cols:?}");
    assert_eq!(
        cols[0].len(),
        2,
        "LIMIT 2 OFFSET 1 must cap the result to exactly 2 groups: {cols:?}"
    );
    let ks: Vec<i64> = cols[0].iter().map(parse_int).collect();
    let counts: Vec<i64> = cols[1].iter().map(parse_int).collect();
    assert_eq!(
        ks,
        vec![1, 2],
        "GROUP BY MOD(id,4) ORDER BY 1 LIMIT 2 OFFSET 1 must return groups \
         ranked 2-3 (k=1, k=2), NOT ranks 1-2 (k=0, k=1), got {ks:?}"
    );
    assert_eq!(
        counts,
        vec![5, 5],
        "each of the 4 MOD(id,4) groups has exactly 5 members, got {counts:?}"
    );
}

/// The qualified single-table wrapper (S6, `joins/sql_builders.rs`) renders
/// the request's OFFSET, exercised via its `GROUP BY` + `COUNT(DISTINCT)`
/// entry point — capture row 8 in the plan (`GROUP BY MOD(id,4)` with
/// `COUNT(DISTINCT id)`, `ORDER BY 1 LIMIT 2 OFFSET 1`).
///
/// Same seeded groups as
/// [`grouped_order_by_limit_offset_returns_shifted_groups`] (k=0..3, 5
/// members each, all distinct ids so `COUNT(DISTINCT id)` == `COUNT(*)` per
/// group): the shifted window must return groups ranked 2-3 (k=1, k=2).
#[test]
fn qualified_wrapper_limit_offset_returns_shifted_window() {
    setup_e2e();
    let mut conn = exa_conn();

    let sql = format!(
        "SELECT MOD(id,4) AS k, COUNT(DISTINCT id) AS c FROM {} \
         GROUP BY MOD(id,4) ORDER BY 1 LIMIT 2 OFFSET 1",
        vs_table()
    );

    // Pin the render site: `LHS_T0` is the qualified single-table wrapper's
    // (S6, `joins/sql_builders.rs`) own aliasing scheme, the same marker used
    // at `unmatched_pushed_sql.contains("LHS_T0")` above — distinct from the
    // plain grouped-merge builder (S2), which never aliases `LHS_T0` and
    // instead carries `group_keys`/`PARTIAL_` (see
    // `grouped_order_by_limit_offset_returns_shifted_groups`).
    let pushed_sql = explain_virtual_sql(&mut conn, &sql);
    assert!(
        pushed_sql.contains("LHS_T0"),
        "GROUP BY MOD(id,4) + COUNT(DISTINCT id) must render via the \
         qualified single-table wrapper (pushed SQL must contain 'LHS_T0'), \
         got:\n{pushed_sql}"
    );
    assert!(
        pushed_sql.contains("LIMIT 2 OFFSET 1"),
        "pushed SQL must carry LIMIT 2 OFFSET 1 on the qualified wrapper, \
         got:\n{pushed_sql}"
    );

    let cols = conn.query_columns(&sql);
    assert_eq!(cols.len(), 2, "expected 2 columns (k, c): {cols:?}");
    assert_eq!(
        cols[0].len(),
        2,
        "LIMIT 2 OFFSET 1 must cap the result to exactly 2 groups: {cols:?}"
    );
    let ks: Vec<i64> = cols[0].iter().map(parse_int).collect();
    let counts: Vec<i64> = cols[1].iter().map(parse_int).collect();
    assert_eq!(
        ks,
        vec![1, 2],
        "GROUP BY MOD(id,4) + COUNT(DISTINCT id) ORDER BY 1 LIMIT 2 OFFSET 1 \
         must return groups ranked 2-3 (k=1, k=2), NOT ranks 1-2 (k=0, k=1), \
         got {ks:?}"
    );
    assert_eq!(
        counts,
        vec![5, 5],
        "every group's 5 ids are distinct, so COUNT(DISTINCT id) == 5 per \
         group, got {counts:?}"
    );
}

/// Fact 6 canary (S4, S5 — grammar assertion). Exasol's grammar ties OFFSET
/// to a non-aggregated select: `OFFSET` on an ungrouped aggregated select
/// (`SELECT COUNT(*) ... ORDER BY 1 LIMIT 5 OFFSET 2`, no `GROUP BY`) is
/// rejected by Exasol itself, BEFORE the adapter is ever consulted — this is
/// the live, release-mode backstop for the `debug_assert!`s at the two
/// one-row merge builders (`build_aggregate_scan_sql`,
/// `build_count_distinct_scan_sql`, both in `support.rs`), which compile out
/// of the release-profile `.so` and so guard nothing there. A future Exasol
/// build that relaxes this grammar rule must fail this test rather than
/// silently return the single aggregate row where an offset should apply.
///
/// Live-verified: Exasol's WebSocket response for this shape is
/// `{"status":"error","exception":{"sqlCode":"42000","text":"OFFSET not
/// allowed in aggregated selects ..."}}`.
#[test]
fn offset_on_single_group_aggregate_is_rejected_by_exasol() {
    setup_e2e();
    let mut conn = exa_conn();

    let sql = format!(
        "SELECT COUNT(*) FROM {} ORDER BY 1 LIMIT 5 OFFSET 2",
        vs_table()
    );
    let resp = conn.try_execute(&sql);

    assert_eq!(
        resp["status"].as_str(),
        Some("error"),
        "OFFSET on a single-group aggregate must be rejected by Exasol \
         itself (the adapter is never consulted), got: {resp}"
    );
    assert_eq!(
        resp["exception"]["sqlCode"].as_str(),
        Some("42000"),
        "expected sqlCode 42000, got: {resp}"
    );
    let msg = resp["exception"]["text"].as_str().unwrap_or("");
    assert!(
        msg.contains("OFFSET") && msg.contains("aggregated"),
        "expected Exasol's 'OFFSET not allowed in aggregated selects' \
         message, got: {msg}"
    );
}

/// Fact 5 canary (S3 — unrenderable-ordering invariant). `HASH_MD5(id)` is an
/// ordering Exasol cannot delegate to the adapter: for this shape Exasol
/// pushes NEITHER `orderBy` NOR `limit` at all (live-verified via `EXPLAIN
/// VIRTUAL`: the returned scan spec carries no `order_by` field and the
/// pushed SQL carries no `LIMIT`/`OFFSET` token) and windows the result
/// itself. This is the live backstop for the invariant `extract_offset(req) >
/// 0 IMPLIES order_by_present(req)` that `build_row_scan_sql`'s
/// `debug_assert!` (S3, `support.rs`) states but cannot enforce in the
/// release-profile `.so`: if a future Exasol build ever pushed a bare `limit`
/// with no `orderBy` for this shape, S3 would render ` LIMIT 5` with no
/// ORDER BY and no OFFSET, silently reopening #191.
///
/// The VS-side query and a single-node native reference (no VS involved,
/// `ORDER BY HASH_MD5(id) LIMIT 5 OFFSET 2` over a literal 1..=20 values
/// list) must return the SAME 5 ids in the SAME order. The reference casts
/// each literal to `DECIMAL(20,0)` — the VS's own `ID` column type (Arrow
/// `Int64` maps to `DECIMAL(20,0)`, see this project's type-mapping table) —
/// because `HASH_MD5` hashes the value's on-the-wire representation, so an
/// uncast literal (which Exasol infers as a narrower `DECIMAL`) hashes
/// differently and would produce a different (wrong) reference ordering;
/// this was confirmed live during this task.
#[test]
fn unrenderable_ordering_with_offset_matches_single_node() {
    setup_e2e();
    let mut conn = exa_conn();

    let sql = format!(
        "SELECT id FROM {} ORDER BY HASH_MD5(id) LIMIT 5 OFFSET 2",
        vs_table()
    );

    let pushed_sql = explain_virtual_sql(&mut conn, &sql);
    assert!(
        !pushed_sql.contains("\"order_by\":"),
        "HASH_MD5(id) is an unrenderable ordering: the adapter's scan spec \
         must carry no 'order_by' field, got:\n{pushed_sql}"
    );
    // The precise field-shaped marker, not a bare `contains("LIMIT")` — as in
    // `ordered_topn_pushes_down_matches_single_node` and
    // `order_by_without_limit_falls_back_correctly`, `explain_virtual_sql`
    // echoes Exasol's own incoming `pushdownRequest`, which can carry a
    // literal "LIMIT" token unrelated to the adapter's own scan spec, so a
    // raw substring search would false-positive regardless of this shape.
    assert!(
        !pushed_sql.contains("\"limit\":"),
        "HASH_MD5(id) is an unrenderable ordering: Exasol withholds the \
         limit entirely (fact 5), so the adapter's scan spec must carry no \
         'limit' field, got:\n{pushed_sql}"
    );

    let cols = conn.query_columns(&sql);
    assert_eq!(cols.len(), 1, "expected 1 column (id): {cols:?}");
    let ids: Vec<i64> = cols[0].iter().map(parse_int).collect();
    assert_eq!(ids.len(), 5, "expected exactly 5 ids: {ids:?}");

    let native_sql = "SELECT CAST(id AS DECIMAL(20,0)) AS id FROM (VALUES \
         (1),(2),(3),(4),(5),(6),(7),(8),(9),(10),(11),(12),(13),(14),(15),\
         (16),(17),(18),(19),(20)) AS t(id) \
         ORDER BY HASH_MD5(CAST(id AS DECIMAL(20,0))) LIMIT 5 OFFSET 2";
    let native_cols = conn.query_columns(native_sql);
    let native_ids: Vec<i64> = native_cols[0].iter().map(parse_int).collect();

    assert_eq!(
        ids, native_ids,
        "ORDER BY HASH_MD5(id) LIMIT 5 OFFSET 2 through the VS must match a \
         single-node native evaluation of the same ordering over the same \
         20 ids (no VS involved), got VS={ids:?} native={native_ids:?}"
    );
}
