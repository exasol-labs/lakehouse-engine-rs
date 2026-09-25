//! E2E tests against a live Exasol + MinIO + Iceberg REST stack. They fail, never
//! skip, when the stack is unavailable, and share one VS, so run with
//! `--test-threads=1`.
#![cfg(feature = "exasol-e2e")]

mod common;
use common::e2e_harness::*;
use common::exasol_ws::ExaConn;
use common::seed::{
    DIM_CUSTOMER_ROWS, E2E_DIM_TABLE, E2E_EVO_TABLE, E2E_FACT_TABLE, E2E_LINEITEM_TABLE,
    E2E_NAMESPACE, E2E_PART_TABLE, E2E_TABLE, E2E_TABLE_2, E2E_TYPED_TABLE,
    EVO_INITDEF_POST_ADD_IDS, EVO_INITDEF_PRE_ADD_IDS, EVO_INITDEF_TABLE, EVO_INITDEF_TOTAL_ROWS,
    EVO_NEW_COL, EVO_TOTAL_ROWS, FACT_ORDERS_ROWS, LINEITEM_ROWS, LINES_PER_ORDER,
    PART_CENTRAL_IDS, PART_COL, PART_NORTH_IDS, PART_ROWS_PER_FILE, PART_TOTAL_ROWS,
    PART_VAL_CENTRAL, PART_VAL_NORTH, SEED_LABELS_ROWS, SEED_ROWS_SCORE_GT_15, SEED_TOTAL_ROWS,
    TYPED_COL_DECIMAL_A, TYPED_COL_DECIMAL_B, initdef_columns, seed_added_columns_initial_default,
    seed_events, seed_renamed_column, seed_typed_distinct_probe, typed_decimal_a_avg_stddev,
    typed_decimal_b_avg_stddev, typed_id_avg_stddev,
};
use common::stack::{
    build_create_connection_sql, exasol_container, iceberg_catalog_url, wait_for_exasol,
    wait_for_iceberg_catalog, wait_for_minio,
};

use lakehouse_catalog::CatalogSession;
use lakehouse_engine::adapter::pushdown::{ConnectionStorage, ScanSource, format_reader};

use std::sync::OnceLock;

const VS_NAME: &str = "MY_LAKEHOUSE";

/// `PARALLELISM_FACTOR = '1'` so its recorded `DF_THREADS_PER_UDF` reads back the core
/// count the adapter VM detected.
const CPU_PROBE_VS_NAME: &str = "LHVS_CPU_PROBE";

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

fn vs_table() -> String {
    format!("{VS_NAME}.{}", E2E_TABLE.to_uppercase())
}

fn typed_table() -> String {
    format!("{VS_NAME}.{}", E2E_TYPED_TABLE.to_uppercase())
}

fn vs_labels_table() -> String {
    format!("{VS_NAME}.{}", E2E_TABLE_2.to_uppercase())
}

fn vs_lineitem_table() -> String {
    format!("{VS_NAME}.{}", E2E_LINEITEM_TABLE.to_uppercase())
}

/// Used only for the #193 outer-join-decline shape, which `events` cannot express.
fn vs_dim_table() -> String {
    format!("{VS_NAME}.{}", E2E_DIM_TABLE.to_uppercase())
}

fn vs_fact_table() -> String {
    format!("{VS_NAME}.{}", E2E_FACT_TABLE.to_uppercase())
}

/// Scenario: projection + filter + LIMIT returns the correct projected, filtered, capped rows
#[test]
fn e2e_projection_filter_limit_returns_correct_rows() {
    setup_e2e();
    let mut conn = exa_conn();

    // score = 5.0*id, so score > 15.0 is ids 4..=20 and LIMIT 5 yields ids 4..=8.
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

    let score_col = &cols[2];
    for score in score_col {
        let s = score
            .as_f64()
            .unwrap_or_else(|| panic!("score not f64: {score:?}"));
        assert!(s > 15.0, "filter violated: score {s} <= 15.0");
    }

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

    // #193: an aliased FROM must resolve like the unaliased query, stripping the leaked
    // `tableAlias`.
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

    // #193: `e.score + 1` must render the bare "SCORE" under the scan relation, not "E"."SCORE".
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
    // (score + 1) % 5.0 == 1.0 fails if the `+ 1` is dropped.
    for v in &cols_expr[0] {
        let plus_one = parse_numeric(v);
        assert_eq!(
            plus_one % 5.0,
            1.0,
            "e.score + 1 must equal a multiple of 5 plus 1 (score = 5.0*id), got {plus_one}"
        );
    }

    // #193: Exasol stamps `tableAlias:"E"` even on an unqualified column under an aliased FROM.
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

/// Scenario: create VS maps the Iceberg table schema to Exasol types
#[test]
fn create_vs_maps_iceberg_schema() {
    setup_e2e();
    let mut conn = exa_conn();

    let sql = format!("DESCRIBE {}", vs_table());
    let resp = conn.execute(&sql);
    let result_set = &resp["responseData"]["results"][0]["resultSet"];
    let cols = conn.fetch_result_columns(result_set);

    let names = &cols[0];
    let types = &cols[1];
    assert!(!names.is_empty(), "DESCRIBE returned no columns");

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

/// Scenario: a filter predicate restricts the emitted rows
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

/// Scenario: LIMIT caps the rows emitted by the scan
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

/// Scenario: adapter and scan entry points resolve from the same uploaded .so
#[test]
fn both_scripts_resolve_one_artifact() {
    setup_e2e();
    let mut conn = exa_conn();

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

    assert!(
        adapter_body.contains("liblakehouse_engine.so") || adapter_body.contains("udf"),
        "adapter script body does not reference the .so: {adapter_body}"
    );
    assert!(
        scan_body.contains("liblakehouse_engine.so") || scan_body.contains("udf"),
        "scan script body does not reference the .so: {scan_body}"
    );
}

/// Scenario: full projection with date and timestamp columns round-trips correctly
#[test]
fn mixed_column_parquet_round_trips() {
    setup_e2e();
    let mut conn = exa_conn();

    let cols = conn.query_columns(&format!(
        "SELECT id, name, score, event_date, event_ts FROM {} WHERE id = 1",
        vs_table()
    ));
    assert_eq!(cols.len(), 5, "expected 5 columns: {cols:?}");
    for (i, col) in cols.iter().enumerate() {
        assert_eq!(col.len(), 1, "column {i} should have 1 row: {col:?}");
    }
    let id_val = &cols[0][0];
    assert!(
        id_val.as_i64().map(|v| v == 1).unwrap_or(false)
            || id_val.as_str().map(|s| s == "1").unwrap_or(false),
        "id should be 1, got: {id_val:?}"
    );
    let name_val = &cols[1][0];
    assert!(
        name_val
            .as_str()
            .map(|s| s.contains("event-01"))
            .unwrap_or(false),
        "name should be 'event-01', got: {name_val:?}"
    );
    let score_val = &cols[2][0];
    assert!(
        score_val
            .as_f64()
            .map(|v| (v - 5.0).abs() < 0.001)
            .unwrap_or(false)
            || score_val.as_str().map(|s| s.contains('5')).unwrap_or(false),
        "score should be 5.0, got: {score_val:?}"
    );
    assert!(!cols[3][0].is_null(), "event_date must not be null");
    assert!(!cols[4][0].is_null(), "event_ts must not be null");
}

/// Scenario: CREATE VS with an unreachable catalog errors clearly without leaking credentials
#[test]
fn create_vs_unreachable_catalog_errors_no_secret() {
    setup_e2e();
    let mut conn = exa_conn();

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
  NAMESPACE   = 'ns'
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

/// Scenario: a CONNECTION with both a static token and a complete OAuth2 pair is rejected through the deployed .so
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
  NAMESPACE   = 'ns'
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

/// Scenario: querying a table absent from TABLE_MAP errors clearly instead of scanning another table
#[test]
fn scan_unknown_virtual_table_errors() {
    setup_e2e();
    let mut conn = exa_conn();

    let resp = conn.try_execute(&format!("SELECT * FROM {VS_NAME}.NO_SUCH_TABLE LIMIT 1"));
    assert_eq!(
        resp["status"].as_str(),
        Some("error"),
        "expected an error for unknown virtual table NO_SUCH_TABLE: {resp}"
    );
}

/// Scenario: a renamed column resolves by Iceberg field-id across pre- and post-rename files in one shard
#[test]
fn e2e_renamed_column_resolves_by_field_id() {
    setup_e2e();

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

    // Created after `evo` exists so the adapter enumerates it. PARALLELISM_FACTOR = 1 puts
    // both files in one shard, so one ListingTable sees both physical layouts.
    let _ = conn.try_execute("DROP VIRTUAL SCHEMA IF EXISTS EVO_VS CASCADE");
    conn.execute(&format!(
        r#"CREATE VIRTUAL SCHEMA EVO_VS
USING {SCHEMA_NAME}.{ADAPTER_SCRIPT_NAME} WITH
  CATALOG_CONNECTION  = '{DEFAULT_CATALOG_CONN_NAME}'
  NAMESPACE   = '{E2E_NAMESPACE}'
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

/// Scenario: added columns return their initial-default for a pre-add file and real values for a post-add file
#[test]
fn e2e_added_columns_initial_default_fill_all_types() {
    setup_e2e();

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

    // Created after `initdef` exists so the adapter enumerates it. PARALLELISM_FACTOR = 1
    // puts both files in one shard.
    let _ = conn.try_execute("DROP VIRTUAL SCHEMA IF EXISTS INITDEF_VS CASCADE");
    conn.execute(&format!(
        r#"CREATE VIRTUAL SCHEMA INITDEF_VS
USING {SCHEMA_NAME}.{ADAPTER_SCRIPT_NAME} WITH
  CATALOG_CONNECTION  = '{DEFAULT_CATALOG_CONN_NAME}'
  NAMESPACE   = '{E2E_NAMESPACE}'
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

/// Scenario: connecting to an unreachable Exasol panics rather than returning Ok
#[test]
fn e2e_fails_when_stack_unavailable() {
    let result = std::panic::catch_unwind(|| ExaConn::connect("192.0.2.1", 8563, "sys", "exasol"));
    assert!(
        result.is_err(),
        "ExaConn::connect to an unreachable host must panic, not return Ok"
    );
}

/// Scenario: the scan emits node-local partial aggregates instead of raw rows
#[test]
fn scan_emits_partial_aggregate_row() {
    setup_e2e();
    let mut conn = exa_conn();

    let cols = conn.query_columns(&format!("SELECT COUNT(*) FROM {}", vs_table()));
    assert_eq!(cols.len(), 1, "COUNT(*) must return one column: {cols:?}");
    assert_eq!(cols[0].len(), 1, "COUNT(*) must return one row: {cols:?}");
    let count = cols[0][0]
        .as_i64()
        .or_else(|| cols[0][0].as_str().and_then(|s| s.parse().ok()))
        .unwrap_or_else(|| panic!("COUNT(*) result not integer: {:?}", cols[0][0]));
    assert_eq!(count, 20, "COUNT(*) should return 20 for the seeded table");
}

/// Scenario: partial COUNT/SUM/MIN/MAX merge to the correct scalars
#[test]
fn partial_count_sum_min_max_merge_ready() {
    setup_e2e();
    let mut conn = exa_conn();

    // SUM(score) = 5 * (1 + ... + 20) = 1050.
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

/// Scenario: AVG is emitted as a partial sum and count and merges correctly, with and without a filter
#[test]
fn partial_avg_emits_sum_count_pair() {
    setup_e2e();
    let mut conn = exa_conn();

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

    // score > 15.0 is ids 4..=20: SUM = 5 * 204 = 1020, AVG = 60.0.
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

/// Scenario: AVG/STDDEV over BIGINT and DECIMAL columns match the seed-derived oracle (#399)
#[test]
fn partial_avg_stddev_over_non_double_columns() {
    setup_e2e();
    let mut conn = exa_conn();

    let cases: [(&str, (f64, f64)); 3] = [
        ("id", typed_id_avg_stddev()),
        (TYPED_COL_DECIMAL_A, typed_decimal_a_avg_stddev()),
        (TYPED_COL_DECIMAL_B, typed_decimal_b_avg_stddev()),
    ];

    for (col, (expected_avg, expected_stddev)) in cases {
        let cols = conn.query_columns(&format!(
            "SELECT AVG({col}), STDDEV({col}) FROM {}",
            typed_table()
        ));
        let avg = parse_numeric(&cols[0][0]);
        let stddev = parse_numeric(&cols[1][0]);
        assert!(
            (avg - expected_avg).abs() < 0.01,
            "AVG({col}) must be {expected_avg}, got {avg}"
        );
        assert!(
            (stddev - expected_stddev).abs() < 0.01,
            "STDDEV({col}) must be {expected_stddev}, got {stddev}"
        );
    }
}

/// `aggregates`, not `group_keys`, is the discriminating field: single-group partial
/// aggregation also emits `PARTIAL_` columns but never `group_keys`.
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

/// Scenario: a single-group aggregate's common scan spec carries an empty projection (#145)
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
    }
}

/// Scenario: SUM(LENGTH(col)) is pushed down as node-local partial aggregation
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

/// Scenario: SUM(id * score) with an NQ1-shaped filter straddling both shards merges to the single-scan value
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

/// Scenario: an untranslatable aggregate argument (BIT_AND) declines aggregate pushdown and the row-scan fallback is correct
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

/// Scenario: ORDER BY score DESC LIMIT 12 straddling both files pushes a per-shard top-N and matches a single full scan
#[test]
fn ordered_topn_pushes_down_matches_single_node() {
    setup_e2e();
    let mut conn = exa_conn();

    let sql = format!(
        "SELECT id, score FROM {} ORDER BY score DESC LIMIT 12",
        vs_table()
    );

    let pushed_sql = explain_virtual_sql(&mut conn, &sql);
    // Exasol's echoed `pushdownRequest` carries an `"order_by_element"` tag for any
    // ORDER BY query, so only the field-shaped `"order_by":` marker proves the adapter's
    // own scan spec matched.
    assert!(
        pushed_sql.contains("\"order_by\":"),
        "ORDER BY score DESC LIMIT 12 over a projected column must push down \
         as an 'order_by' top-N plan in the scan spec, got:\n{pushed_sql}"
    );
    // The outer merge ORDER BY is spliced after the fan-out, not left to an Exasol backstop.
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

/// Scenario: ORDER BY without LIMIT declines top-N pushdown and Exasol re-sorts the rows itself
#[test]
fn order_by_without_limit_falls_back_correctly() {
    setup_e2e();
    let mut conn = exa_conn();

    let sql = format!("SELECT id, score FROM {} ORDER BY score DESC", vs_table());

    let pushed_sql = explain_virtual_sql(&mut conn, &sql);
    // Field-shaped marker: a bare `contains("order_by")` false-positives on Exasol's echo.
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

    // With `orderBy` but no `limit` in the request, Exasol keeps its own ORDER BY operator
    // and re-sorts the adapter's unsorted rows.
    let ids: Vec<i64> = cols[0].iter().map(parse_int).collect();
    let expected_ids: Vec<i64> = (1..=20).rev().collect();
    assert_eq!(
        ids, expected_ids,
        "ORDER BY score DESC (no LIMIT, fallback path) must return all ids \
         in descending order {expected_ids:?}, got {ids:?}"
    );
}

/// Scenario: adapterNotes carry PARALLELISM_FACTOR but no NR_OF_CORES or CLUSTER_NODES
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
        parsed.get("NR_OF_CORES").is_none(),
        "ADAPTER_NOTES must NOT carry NR_OF_CORES (the per-node core count is a \
         derivation input the adapter discards, never a persisted note): {notes:?}"
    );
    assert!(
        parsed.get("CLUSTER_NODES").is_none(),
        "ADAPTER_NOTES must NOT carry CLUSTER_NODES (node count is read live \
         from UdfContext::node_count() per pushdown request, never persisted): \
         {notes:?}"
    );
}

/// Scenario: the adapter VM's core count comes from the Exasol container's CPU set, not the host
#[test]
fn adapter_detects_container_cpuset() {
    setup_e2e();

    let cpuset_cores = exasol_container_cpuset_cores();
    let host_cores = std::thread::available_parallelism()
        .expect("the test host must report a core count")
        .get();
    assert!(
        cpuset_cores < host_cores,
        "PRECONDITION UNMET: the Exasol container's CPU set must name fewer CPUs \
         ({cpuset_cores}) than this host's core count ({host_cores}). At an equal \
         cardinality the assertion below holds whether the adapter reads the \
         container's CPU set or the unconstrained host, so it would record no \
         evidence. Narrow LH_EXASOL_CPUSET to a smaller set of CPUs and recreate \
         the container."
    );

    let mut conn = exa_conn();
    create_virtual_schema(
        &mut conn,
        &VsProps::new(CPU_PROBE_VS_NAME, E2E_NAMESPACE).with_parallelism_factor(1),
    );

    let cols = conn.query_columns(&format!(
        "SELECT ADAPTER_NOTES FROM SYS.EXA_ALL_VIRTUAL_SCHEMAS \
         WHERE SCHEMA_NAME = '{CPU_PROBE_VS_NAME}'"
    ));
    let notes = cols
        .first()
        .and_then(|col| col.first())
        .and_then(|v| v.as_str())
        .unwrap_or_else(|| panic!("{CPU_PROBE_VS_NAME} must have ADAPTER_NOTES: {cols:?}"));
    let parsed: serde_json::Value = serde_json::from_str(notes)
        .unwrap_or_else(|e| panic!("ADAPTER_NOTES must be valid JSON ({e}): {notes:?}"));
    let detected: usize = parsed["DF_THREADS_PER_UDF"]
        .as_str()
        .unwrap_or_else(|| panic!("ADAPTER_NOTES must carry DF_THREADS_PER_UDF: {notes:?}"))
        .parse()
        .unwrap_or_else(|e| panic!("DF_THREADS_PER_UDF must be an integer ({e}): {notes:?}"));

    assert_eq!(
        detected, cpuset_cores,
        "at PARALLELISM_FACTOR = '1' the recorded DF_THREADS_PER_UDF is the core \
         count the adapter VM detected; it must equal the number of CPUs in the \
         container's CPU set ({cpuset_cores}), and so be neither the \
         undetectable-platform fallback of 1 nor the host's {host_cores} cores"
    );
}

/// Read inside the container rather than from this process's environment.
fn exasol_container_cpuset_cores() -> usize {
    let container = exasol_container();
    let path = "/sys/fs/cgroup/cpuset.cpus.effective";
    let out = std::process::Command::new("docker")
        .args(["exec", &container, "cat", path])
        .output()
        .unwrap_or_else(|e| panic!("docker exec {container} to read {path}: {e}"));
    assert!(
        out.status.success(),
        "reading {path} in {container} failed ({status}): {stderr}",
        status = out.status,
        stderr = String::from_utf8_lossy(&out.stderr).trim()
    );
    let raw = String::from_utf8_lossy(&out.stdout).trim().to_string();
    assert!(
        !raw.is_empty(),
        "{path} in {container} is empty, so the container is pinned to no CPU at \
         all — set LH_EXASOL_CPUSET to a set of CPUs this host has and recreate \
         the container"
    );
    raw.split(',')
        .map(|entry| {
            let (first, last) = entry.split_once('-').unwrap_or((entry, entry));
            let cpu_id = |bound: &str| {
                bound.parse::<usize>().unwrap_or_else(|e| {
                    panic!(
                        "{path} in {container} reads {raw:?}, whose entry {entry:?} is \
                         neither a CPU id nor an inclusive range of them ({e})"
                    )
                })
            };
            let (first, last) = (cpu_id(first), cpu_id(last));
            assert!(
                first <= last,
                "{path} in {container} reads {raw:?}, whose entry {entry:?} names a \
                 descending range"
            );
            last - first + 1
        })
        .sum()
}

/// Scenario: with no node count in adapterNotes, a multi-file scan still emits the shard fan-out
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

/// Scenario: COUNT(col) pushdown returns the correct non-null row count, with and without a filter
#[test]
fn aggregate_count_col_returns_correct_value() {
    setup_e2e();
    let mut conn = exa_conn();

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

    // #193: alias stripping must leave single-group aggregate pushdown unchanged.
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

    // #193: alias stripping must also apply to GROUP BY keys; grouping by `id` yields one
    // group per matching row.
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

/// Scenario: a declined outer join re-pushes plain single-table scans carrying the alias filter (#193)
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

/// Scenario: the fan-out path returns every row exactly once, with no gaps or duplicates
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

/// Scenario: a multi-file scan with the `(path, size)` + `table_root` payload scans every file exactly once with correct values
#[test]
fn scan_registers_assigned_files_with_path_size_payload() {
    setup_e2e();
    let mut conn = exa_conn();

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

        // Proves the data, not just the row count, is correct for the file each row came from.
        let score = cols[2][pos]
            .as_f64()
            .unwrap_or_else(|| panic!("score not f64: {:?}", cols[2][pos]));
        assert!(
            (score - 5.0 * expected as f64).abs() < 1e-9,
            "score for id {expected} must be {}, got {score}",
            5.0 * expected as f64
        );

        let name = cols[1][pos]
            .as_str()
            .unwrap_or_else(|| panic!("name not string: {:?}", cols[1][pos]));
        assert!(
            name.contains(&format!("{expected:02}")),
            "name '{name}' does not carry expected id {expected}"
        );
    }
}

// GROUP BY E2E tests. Seed: id 1..=20, score = 5.0 * id. Exasol's CAST to DECIMAL
// rounds half away from zero, giving `CAST(score / 25.0 AS DECIMAL(4,0))` groups
// {0..4} of sizes 2,5,5,5,3.

/// Checks `group_keys` and the `PARTIAL_` prefix, not `GROUP BY shard_key`: a filter
/// pruning to a single shard still pushes grouped aggregation but has no shard fan-out.
fn assert_group_by_pushed_down(conn: &mut ExaConn, query_sql: &str) {
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

/// Scenario: a decomposed GROUP BY query's common scan spec carries an empty projection (#145)
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

/// Scenario: GROUP BY MOD(id, 4) returns correct per-group COUNT(*) and SUM(score)
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

    let total: i64 = cols[1].iter().map(parse_int).sum();
    assert_eq!(
        total, 20,
        "total COUNT(*) across groups must be 20, got {total}"
    );
}

/// Scenario: two GROUP BY keys with a WHERE filter return correct per-group row counts
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

    assert_group_by_pushed_down(&mut conn, &sql);

    let cols = conn.query_columns(&sql);
    assert_eq!(cols.len(), 3, "expected 3 columns: {cols:?}");
    assert_eq!(cols[0].len(), 4, "expected 4 distinct groups: {cols:?}");

    let total: i64 = cols[2].iter().map(parse_int).sum();
    assert_eq!(
        total, 10,
        "total COUNT(*) across all groups must be 10 (id=11..20), got {total}"
    );

    for (i, v) in cols[2].iter().enumerate() {
        let c = parse_int(v);
        assert!(
            (2..=3).contains(&c),
            "group {i}: count must be 2 or 3, got {c}"
        );
    }
}

/// Scenario: GROUP BY a CAST(score / 25.0 AS DECIMAL(4,0)) expression key returns correct per-group counts
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

    let total: i64 = pairs.iter().map(|(_, c)| *c).sum();
    assert_eq!(
        total, 20,
        "total rows across expression-key groups must be 20, got {total}"
    );
}

/// Scenario: AVG(score) per group is correct for groups with unequal row counts
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

/// Scenario: GROUP BY a near-unique column completes under the memory-pool and spill backstop
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

    for (i, v) in cols[1].iter().enumerate() {
        let count = parse_int(v);
        assert_eq!(
            count,
            1,
            "group at position {i} (id={}): COUNT(*) must be 1, got {count}",
            parse_int(&cols[0][i])
        );
    }

    let ids: Vec<i64> = cols[0].iter().map(parse_int).collect();
    for (pos, &id) in ids.iter().enumerate() {
        let expected = (pos + 1) as i64;
        assert_eq!(
            id, expected,
            "id at position {pos} must be {expected}, got {id}"
        );
    }
}

/// Scenario: EXPLAIN VIRTUAL shows the shard_key fan-out and no IPROC()
#[test]
fn test_shard_key_fanout_explain() {
    setup_e2e();
    let mut conn = exa_conn();

    let sql = format!(
        "EXPLAIN VIRTUAL SELECT id, COUNT(*) FROM {} GROUP BY id",
        vs_table()
    );
    let resp = conn.execute(&sql);
    let result_set = &resp["responseData"]["results"][0]["resultSet"];
    let cols = conn.fetch_result_columns(result_set);

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

/// Scenario: NULL group keys produced by NULLIF are grouped together consistently
#[test]
fn test_group_by_null_key_grouping() {
    setup_e2e();
    let mut conn = exa_conn();

    // id = 5, 10, 15, 20 yield the NULL group; every group has 4 rows.
    let sql = format!(
        "SELECT NULLIF(MOD(id, 5), 0), COUNT(*) \
         FROM {} \
         GROUP BY NULLIF(MOD(id, 5), 0) \
         ORDER BY NULLIF(MOD(id, 5), 0) NULLS LAST",
        vs_table()
    );
    let cols = conn.query_columns(&sql);
    assert_eq!(cols.len(), 2, "expected 2 columns (key, count): {cols:?}");
    assert_eq!(
        cols[0].len(),
        5,
        "expected 5 groups (1,2,3,4,NULL): {cols:?}"
    );

    for (i, v) in cols[1].iter().enumerate() {
        let count = parse_int(v);
        assert_eq!(
            count, 4,
            "group at position {i}: COUNT(*) must be 4, got {count}"
        );
    }

    let total: i64 = cols[1].iter().map(parse_int).sum();
    assert_eq!(
        total, 20,
        "total rows across all groups must be 20, got {total}"
    );

    // ORDER BY NULLS LAST may not survive the GROUP BY pushdown, so the NULL key's position
    // is not asserted.
    let null_count = cols[0].iter().filter(|v| v.is_null()).count();
    assert_eq!(
        null_count, 1,
        "exactly one NULL group key must exist (multiples of 5): {cols:?}"
    );

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

/// Scenario: an aggregate before the group key in the select list keeps its column position (#33)
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

    let total: f64 = pairs.iter().map(|(_, s)| *s).sum();
    assert!(
        (total - 1050.0).abs() < 0.01,
        "total SUM(score) across groups must be 1050.0, got {total}"
    );
}

/// Scenario: an interleaved key, aggregate, key select list is reassembled positionally
#[test]
fn test_group_by_interleaved_multi_key() {
    setup_e2e();
    let mut conn = exa_conn();

    let sql = format!(
        "SELECT MOD(id, 4), SUM(score), MOD(id, 2) FROM {} GROUP BY MOD(id, 4), MOD(id, 2)",
        vs_table()
    );

    assert_group_by_pushed_down(&mut conn, &sql);

    let cols = conn.query_columns(&sql);
    assert_eq!(
        cols.len(),
        3,
        "expected 3 columns (key1, sum, key2): {cols:?}"
    );
    assert_eq!(cols[0].len(), 4, "expected 4 groups: {cols:?}");

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

    let total: f64 = rows.iter().map(|(_, s, _)| *s).sum();
    assert!(
        (total - 1050.0).abs() < 0.01,
        "total SUM(score) across groups must be 1050.0, got {total}"
    );
}

/// Scenario: an expression group key after an aggregate keeps its resolved DECIMAL type, not VARCHAR
#[test]
fn test_group_by_expr_key_after_agg() {
    setup_e2e();
    let mut conn = exa_conn();

    let sql = format!(
        "SELECT COUNT(*), MOD(id, 4) FROM {} GROUP BY MOD(id, 4)",
        vs_table()
    );
    let resp = conn.execute(&sql);

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

/// Scenario: aggregate-first GROUP BY with matched, unmatched, mixed-junction, and COUNT(DISTINCT) HAVING shapes (#195)
#[test]
fn test_group_by_agg_first_with_having() {
    setup_e2e();
    let mut conn = exa_conn();

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

    // Case 2: SUM(score) is not selected, so the merge rewrite cannot find it and the
    // query must succeed via the wrapper fallback.
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

    // Case 3: only one conjunct matches a selected aggregate, so the whole junction falls
    // back to the wrapper.
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

    // Case 4: every group has 5 distinct names, so `> 4` keeps all groups and `> 5` keeps
    // none, proving the HAVING was applied rather than dropped.
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

/// Scenario: an expression-valued multi-key GROUP BY with mixed key types keeps each key's declared type
#[test]
fn test_group_by_expr_multi_key_tuple() {
    setup_e2e();
    let mut conn = exa_conn();

    let sql = format!(
        "SELECT MOD(id, 4), UPPER(name), COUNT(*) \
         FROM {} GROUP BY MOD(id, 4), UPPER(name)",
        vs_table()
    );

    assert_group_by_pushed_down(&mut conn, &sql);

    let resp = conn.execute(&sql);
    let result_set = &resp["responseData"]["results"][0]["resultSet"];

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

/// Scenario: multi-key GROUP BY with HAVING and LIMIT caps the number of groups in the outer merge
#[test]
fn test_group_by_multi_key_having_limit() {
    setup_e2e();
    let mut conn = exa_conn();

    let sql = format!(
        "SELECT MOD(id, 4), MOD(id, 3), SUM(score) FROM {} \
         GROUP BY MOD(id, 4), MOD(id, 3) HAVING SUM(score) > 100.0 LIMIT 2",
        vs_table()
    );

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

/// Scenario: a high-cardinality multi-key GROUP BY completes under the bounded memory pool
#[test]
fn test_high_cardinality_multi_key_group_by_spill() {
    setup_e2e();
    let mut conn = exa_conn();

    let sql = format!(
        "SELECT id, MOD(id, 2), COUNT(*) FROM {} GROUP BY id, MOD(id, 2) ORDER BY id",
        vs_table()
    );

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

    for (i, v) in cols[2].iter().enumerate() {
        let count = parse_int(v);
        assert_eq!(
            count,
            1,
            "group at position {i} (id={}): COUNT(*) must be 1, got {count}",
            parse_int(&cols[0][i])
        );
    }

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

// Single-table scalar-over-aggregate GROUP BY (#82), checked against a native
// ground-truth table built from the same `fact_lineitem` columns.

/// Named distinctly from `e2e_join_test.rs`'s ground-truth table to keep fixtures unambiguous.
const GROUND_TRUTH_LINEITEM_SCAN_TABLE: &str = "GROUND_TRUTH_LINEITEM_SCAN";

fn ensure_ground_truth_lineitem_table(conn: &mut ExaConn) {
    conn.execute(&format!(
        "CREATE OR REPLACE TABLE {SCHEMA_NAME}.{GROUND_TRUTH_LINEITEM_SCAN_TABLE} AS \
         SELECT L_RETURNFLAG, L_QUANTITY, L_EXTENDEDPRICE FROM {}",
        vs_lineitem_table()
    ));
}

fn scalar_over_aggregate_round_select_list() -> &'static str {
    "L_RETURNFLAG, SUM(L_QUANTITY), AVG(L_EXTENDEDPRICE), \
     ROUND(100.0 * SUM(CASE WHEN L_RETURNFLAG='R' THEN 1 ELSE 0 END)/COUNT(*), 2)"
}

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

/// Scenario: a single-table grouped select list with ROUND over aggregates pushes down as the grouped merge wrapper and is correct (#82)
#[test]
fn test_group_by_scalar_over_aggregate_round() {
    setup_e2e();
    let mut conn = exa_conn();

    let sql = format!(
        "SELECT {} FROM {} GROUP BY L_RETURNFLAG ORDER BY L_RETURNFLAG",
        scalar_over_aggregate_round_select_list(),
        vs_lineitem_table()
    );

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

/// Scenario: a bare COUNT(*) and a scalar-wrapped COUNT(*) share one deduplicated partial column
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

/// Scenario: create VS with NAMESPACE enumerates every table in the namespace
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

/// Scenario: pushdown resolves the scanned Iceberg table from TABLE_MAP by the virtual table name
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

// Iceberg file-pruning E2E tests over the partitioned `regions` table.

fn vs_regions_table() -> String {
    format!("{VS_NAME}.{}", E2E_PART_TABLE.to_uppercase())
}

/// Scenario: a partition filter prunes and returns the correct rows
#[test]
fn e2e_partition_filter_prunes_and_returns_correct_rows() {
    setup_e2e();
    let mut conn = exa_conn();

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

    let total = conn.query_row_count(&format!("SELECT id FROM {}", vs_regions_table()));
    assert_eq!(
        total, PART_TOTAL_ROWS as i64,
        "REGIONS total row count must be {PART_TOTAL_ROWS} (all 3 partitions), got {total}"
    );

    // LIKE is not pushed to Iceberg; DataFusion applies the full filter as a backstop.
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

/// Scenario: partition pruning and per-file min/max range pruning each resolve the regions table to one file
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

    // One session reused across the pruning calls, mirroring the single-session-per-query
    // contract of the format-reader seam.
    let session = rt
        .block_on(async { CatalogSession::resolve(&catalog_uri, &creds.warehouse, &creds).await })
        .expect("CatalogSession::resolve must succeed");
    // The local stack's MinIO is plain HTTP.
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

    // Iceberg's InclusiveMetricsEvaluator prunes files whose min(id) > 5.
    let range_filter = serde_json::json!({
        "type": "predicate_lessequal",
        "left": {"type": "column", "name": "ID"},
        "right": {"type": "literal_exactnumeric", "value": "5"}
    });
    let pruned_range = rt
        .block_on(reader.resolve_scan(Some(&range_filter)))
        .expect("resolve_scan (range filter) must succeed")
        .files;
    assert_eq!(
        pruned_range.len(),
        1,
        "range filter 'id <= 5' must resolve only the north file, got {}: {pruned_range:?}",
        pruned_range.len()
    );
}

/// Scenario: an Exasol-side JOIN across two virtual tables returns the correct joined rows
#[test]
fn e2e_pushdown_resolves_files_once_multi_table() {
    setup_e2e();
    let mut conn = exa_conn();

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

// #52: Exasol flattens `COUNT(*) FROM (SELECT k, COUNT(*) ... GROUP BY k)` into one
// group_by request with a literal-only select list; the scan must keep the GROUP BY
// so Exasol's outer COUNT(*) counts group rows.

/// Scenario: COUNT(*) over a grouped sub-select counts groups, not source rows (#52)
#[test]
fn e2e_nested_aggregate_over_grouped_subselect_returns_correct_count() {
    setup_e2e();
    let mut conn = exa_conn();

    // Must be 4, not the 20 a row-scan fallback would return; `GROUP BY id` alone cannot
    // tell the two apart.
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

// ORDER BY ... LIMIT n OFFSET m coverage (#191) for each wrapper site routed through
// `render_limit_offset`, plus canaries for the two shapes whose `debug_assert!` guards
// compile out of the release `.so`.

/// Scenario: ORDER BY a projected key with LIMIT 12 OFFSET 3 returns the shifted window on the row-scan wrapper (#191)
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

/// Scenario: ORDER BY an unprojected key with LIMIT 5 OFFSET 2 returns the shifted window without leaking the sort column
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

/// Scenario: the grouped merge wrapper applies LIMIT 2 OFFSET 1 to the ranked groups
#[test]
fn grouped_order_by_limit_offset_returns_shifted_groups() {
    setup_e2e();
    let mut conn = exa_conn();

    let sql = format!(
        "SELECT MOD(id,4) AS k, COUNT(*) AS c FROM {} GROUP BY MOD(id,4) \
         ORDER BY 1 LIMIT 2 OFFSET 1",
        vs_table()
    );

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

/// Scenario: the qualified single-table wrapper applies LIMIT 2 OFFSET 1 with COUNT(DISTINCT)
#[test]
fn qualified_wrapper_limit_offset_returns_shifted_window() {
    setup_e2e();
    let mut conn = exa_conn();

    let sql = format!(
        "SELECT MOD(id,4) AS k, COUNT(DISTINCT id) AS c FROM {} \
         GROUP BY MOD(id,4) ORDER BY 1 LIMIT 2 OFFSET 1",
        vs_table()
    );

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

/// Scenario: Exasol rejects OFFSET on an ungrouped aggregated select before the adapter is consulted (42000)
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

/// Scenario: an unrenderable ordering (HASH_MD5) with OFFSET is windowed by Exasol and matches a native reference
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
    // Field-shaped marker: Exasol's echoed request can carry an unrelated "LIMIT" token.
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

    // HASH_MD5 hashes the value's representation, so literals must be cast to the VS's
    // `DECIMAL(20,0)` id type or the reference ordering differs.
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

// Single-group scalar-over-aggregate decomposition (#194, #188).

/// Carries `L_ORDERKEY` so the all-files-pruned predicate is expressible on both surfaces.
const GROUND_TRUTH_SINGLE_GROUP_TABLE: &str = "GT_SINGLE_GROUP_LINEITEM";

fn single_group_ground_truth_table() -> String {
    format!("{SCHEMA_NAME}.{GROUND_TRUTH_SINGLE_GROUP_TABLE}")
}

fn ensure_single_group_ground_truth_table(conn: &mut ExaConn) {
    conn.execute(&format!(
        "CREATE OR REPLACE TABLE {} AS \
         SELECT L_ORDERKEY, L_RETURNFLAG, L_QUANTITY, L_EXTENDEDPRICE FROM {}",
        single_group_ground_truth_table(),
        vs_lineitem_table()
    ));
}

/// Excludes Exasol's echoed request JSON, which repeats the user's select list and would
/// satisfy a naive substring probe.
fn explain_virtual_pushdown_sql(conn: &mut ExaConn, query_sql: &str) -> String {
    let resp = conn.execute(&format!("EXPLAIN VIRTUAL {query_sql}"));
    let result_set = &resp["responseData"]["results"][0]["resultSet"];
    let cols = conn.fetch_result_columns(result_set);
    cols[1]
        .iter()
        .filter_map(|v| v.as_str())
        .collect::<Vec<_>>()
        .join(" ")
}

/// The virtual result must be exactly one merged row, never one row per shard (#194).
fn assert_single_group_matches_native_oracle(conn: &mut ExaConn, select_list: &str, tail: &str) {
    ensure_single_group_ground_truth_table(conn);

    let vs_sql = format!("SELECT {select_list} FROM {} {tail}", vs_lineitem_table());
    let oracle_sql = format!(
        "SELECT {select_list} FROM {} {tail}",
        single_group_ground_truth_table()
    );
    let actual = conn.query_columns(&vs_sql);
    let expected = conn.query_columns(&oracle_sql);

    assert_eq!(
        actual.len(),
        expected.len(),
        "column count must match the native oracle: {vs_sql}\n\
         actual: {actual:?}\nexpected: {expected:?}"
    );
    for (i, col) in actual.iter().enumerate() {
        assert_eq!(
            col.len(),
            1,
            "a single-group aggregate query must return exactly ONE merged row, \
             not one partial row per shard (#194): column {i} returned {} rows \
             for {vs_sql}\nactual: {actual:?}",
            col.len()
        );
    }
    for (i, (got_col, want_col)) in actual.iter().zip(expected.iter()).enumerate() {
        let (got, want) = (&got_col[0], &want_col[0]);
        if want.is_null() {
            assert!(
                got.is_null(),
                "column {i} must be NULL like the native oracle, got {got:?} for {vs_sql}"
            );
            continue;
        }
        let (got_num, want_num) = (parse_numeric(got), parse_numeric(want));
        assert!(
            (got_num - want_num).abs() <= 1e-9 * want_num.abs().max(1.0),
            "column {i} must equal the native oracle {want_num}, got {got_num} for {vs_sql}"
        );
    }
}

/// Scenario: ROUND(SUM(col)) over a sharded table merges to one row equal to the native oracle (#194)
#[test]
fn e2e_single_group_scalar_over_aggregate_round_sum_matches_native_oracle() {
    setup_e2e();
    let mut conn = exa_conn();

    let select_list = "ROUND(SUM(L_QUANTITY), 2)";
    let sql = format!("SELECT {select_list} FROM {}", vs_lineitem_table());
    assert_single_group_aggregate_pushed_down(&mut conn, &sql);
    assert_single_group_matches_native_oracle(&mut conn, select_list, "");
}

/// Scenario: ROUND(VARIANCE(col)), VARIANCE and VAR_SAMP succeed and match the native oracle (#188)
#[test]
fn e2e_single_group_scalar_over_variance_matches_native_oracle() {
    setup_e2e();
    let mut conn = exa_conn();

    let select_list = "ROUND(VARIANCE(L_EXTENDEDPRICE), 4)";
    let sql = format!("SELECT {select_list} FROM {}", vs_lineitem_table());
    assert_single_group_aggregate_pushed_down(&mut conn, &sql);
    assert_single_group_matches_native_oracle(&mut conn, select_list, "");

    assert_single_group_matches_native_oracle(&mut conn, "VARIANCE(L_EXTENDEDPRICE)", "");
    assert_single_group_matches_native_oracle(&mut conn, "ROUND(VAR_SAMP(L_EXTENDEDPRICE), 4)", "");
}

/// Scenario: a COUNT(*) shared by a plain and a scalar-over-aggregate item collapses into one partial column
#[test]
fn e2e_single_group_scalar_over_aggregate_shared_count_matches_native_oracle() {
    setup_e2e();
    let mut conn = exa_conn();

    let select_list = "COUNT(*), ROUND(SUM(L_QUANTITY) / COUNT(*), 2)";
    let sql = format!("SELECT {select_list} FROM {}", vs_lineitem_table());

    let pushed = explain_virtual_pushdown_sql(&mut conn, &sql);
    assert!(
        pushed.contains(r#""aggregates":[{"kind":"count"},{"kind":"sum","column":"L_QUANTITY"}]"#),
        "the shared COUNT(*) must be folded into ONE partial aggregate plan \
         alongside the SUM, got:\n{pushed}"
    );
    assert!(
        !pushed.contains("PARTIAL_count_1"),
        "a second partial count column means the shared inner aggregate was not \
         deduplicated, got:\n{pushed}"
    );

    assert_single_group_matches_native_oracle(&mut conn, select_list, "");
}

/// Scenario: interleaved plain and scalar-over-aggregate items keep select-list order and their own types
#[test]
fn e2e_single_group_scalar_over_aggregate_interleaved_matches_native_oracle() {
    setup_e2e();
    let mut conn = exa_conn();

    let select_list = "SUM(L_QUANTITY), ROUND(AVG(L_EXTENDEDPRICE), 3), MIN(L_QUANTITY), \
                       ROUND(100.0 * SUM(CASE WHEN L_RETURNFLAG='R' THEN 1 ELSE 0 END) \
                       / COUNT(*), 2)";
    let sql = format!("SELECT {select_list} FROM {}", vs_lineitem_table());
    assert_single_group_aggregate_pushed_down(&mut conn, &sql);
    assert_single_group_matches_native_oracle(&mut conn, select_list, "");
}

/// Scenario: an all-files-pruned predicate still yields one row with count 0 and a NULL scalar-wrapped SUM
#[test]
fn e2e_single_group_scalar_over_aggregate_all_files_pruned_returns_one_row() {
    setup_e2e();
    let mut conn = exa_conn();

    let select_list = "COUNT(*), ROUND(SUM(L_EXTENDEDPRICE), 2)";
    let tail = "WHERE L_ORDERKEY > 1000";
    let sql = format!("SELECT {select_list} FROM {} {tail}", vs_lineitem_table());

    let pushed = explain_virtual_pushdown_sql(&mut conn, &sql);
    assert!(
        pushed.contains("FROM DUAL") && !pushed.contains("LAKEHOUSE_SCAN"),
        "an all-files-pruned single-group request must take the empty-result \
         path (a literal row, no scan UDF), got:\n{pushed}"
    );

    assert_single_group_matches_native_oracle(&mut conn, select_list, tail);
}

/// Scenario: the decomposed request pushes aggregates into `aggregates`, with an empty projection and no projection expression (#194)
#[test]
fn e2e_single_group_scalar_over_aggregate_explain_virtual_shows_empty_projection() {
    setup_e2e();
    let mut conn = exa_conn();

    let sql = format!(
        "SELECT ROUND(SUM(L_QUANTITY), 2) FROM {}",
        vs_lineitem_table()
    );
    let pushed = explain_virtual_pushdown_sql(&mut conn, &sql);

    assert!(
        pushed.contains(r#""aggregates":[{"kind":"sum","column":"L_QUANTITY"}]"#),
        "the scalar-wrapped SUM must be decomposed into a non-empty scan-spec \
         'aggregates' field, got:\n{pushed}"
    );
    assert!(
        pushed.contains(r#""projection":[]"#),
        "the decomposed request reads 'aggregates', so 'projection' must stay \
         empty rather than splicing the full base row (#145/#194), got:\n{pushed}"
    );
    assert!(
        !pushed.contains(r#""expr""#),
        "no select-list expression may reach the scan spec: an aggregate \
         evaluated per shard is exactly the #194/#188 bug, got:\n{pushed}"
    );
}

/// Scenario: integer `/` pushes down as float division, 7/2 = 3.5 (#186)
#[test]
fn e2e_float_div_int_over_int_matches_native_oracle() {
    setup_e2e();
    let mut conn = exa_conn();

    let oracle_cols = conn.query_columns(
        "SELECT L_ORDERKEY / L_LINENUMBER FROM \
         (SELECT 7 AS L_ORDERKEY, 1 AS L_LINENUMBER UNION ALL SELECT 7, 2) \
         WHERE L_LINENUMBER = 2",
    );
    let oracle_value = parse_numeric(&oracle_cols[0][0]);
    assert_eq!(
        oracle_value, 3.5,
        "native oracle 7/2 must be 3.5, got {oracle_value}"
    );

    let vs_sql = format!(
        "SELECT L_ORDERKEY / L_LINENUMBER FROM {} WHERE L_ORDERKEY = 7 AND L_LINENUMBER = 2",
        vs_lineitem_table()
    );
    let vs_cols = conn.query_columns(&vs_sql);
    let vs_value = parse_numeric(&vs_cols[0][0]);
    assert_eq!(
        vs_value, oracle_value,
        "pushed-down L_ORDERKEY/L_LINENUMBER at L_ORDERKEY=7, L_LINENUMBER=2 \
         must match the native oracle {oracle_value} (int/int FLOAT_DIV must \
         not truncate), got {vs_value}"
    );
}

/// Scenario: a bare column division pushes down as a checked division with a DOUBLE emit type (#186, #370)
#[test]
fn e2e_float_div_pushes_checked_division_call_projection() {
    setup_e2e();
    let mut conn = exa_conn();

    let sql = format!(
        "SELECT L_ORDERKEY / L_LINENUMBER FROM {}",
        vs_lineitem_table()
    );
    let pushed = explain_virtual_pushdown_sql(&mut conn, &sql);

    let expected_projection = format!(
        r#""projection":[{{"expr":"{}(\"L_ORDERKEY\", \"L_LINENUMBER\")"}}]"#,
        vs_expression::CHECKED_FLOAT_DIV_FN
    );
    assert!(
        pushed.contains(&expected_projection),
        "expected the pushed projection to be a checked-division FLOAT_DIV \
         expression, got:\n{pushed}"
    );
    assert!(
        pushed.contains(r#"EMITS ("_LH_PROJ_0" DOUBLE PRECISION)"#),
        "expected the pushed EMITS clause to declare DOUBLE PRECISION for the \
         FLOAT_DIV projection, got:\n{pushed}"
    );
}

/// The state is the same for every UDF-raised error, so the message is what is asserted.
fn division_by_zero_failure_sql_code(conn: &mut ExaConn, sql: &str) -> String {
    let resp = conn.try_execute(sql);

    assert_eq!(
        resp["status"].as_str(),
        Some("error"),
        "a division by zero must fail the query rather than silently returning \
         a value or a row count native Exasol disagrees with:\n{sql}\n{resp}"
    );
    let message = resp["exception"]["text"]
        .as_str()
        .unwrap_or_default()
        .to_ascii_lowercase();
    assert!(
        message.contains("division by zero"),
        "the surfaced message must name a division by zero:\n{sql}\n{resp}"
    );
    assert!(
        !message.contains("assigned data could not be read"),
        "a user's own division by zero must NOT carry the storage-read framing, \
         which would send a support case looking at object storage:\n{sql}\n{resp}"
    );
    resp["exception"]["sqlCode"]
        .as_str()
        .unwrap_or_default()
        .to_string()
}

/// Exasol reports every UDF-raised error under this generic state, not native `22012`.
const UDF_ERROR_SQL_CODE: &str = "22002";

/// Scenario: a projected x/0 fails with the checked division's own division-by-zero message (#370)
#[test]
fn e2e_float_div_by_zero_projected_fails_with_division_by_zero() {
    setup_e2e();
    let mut conn = exa_conn();

    let sql = format!(
        "SELECT L_ORDERKEY / (L_LINENUMBER - L_LINENUMBER) FROM {} WHERE L_ORDERKEY = 7",
        vs_lineitem_table()
    );

    let sql_code = division_by_zero_failure_sql_code(&mut conn, &sql);

    assert_eq!(
        sql_code, UDF_ERROR_SQL_CODE,
        "the recorded SQL state for a scan-UDF-raised division by zero"
    );
}

/// Scenario: a projected 0/0 fails with the same division-by-zero message instead of a silent NULL
#[test]
fn e2e_zero_div_zero_projected_fails_with_division_by_zero() {
    setup_e2e();
    let mut conn = exa_conn();

    let sql = format!(
        "SELECT CAST(L_LINENUMBER - L_LINENUMBER AS DOUBLE) / (L_LINENUMBER - L_LINENUMBER) \
         FROM {} WHERE L_ORDERKEY = 7",
        vs_lineitem_table()
    );

    let sql_code = division_by_zero_failure_sql_code(&mut conn, &sql);

    assert_eq!(
        sql_code, UDF_ERROR_SQL_CODE,
        "0/0 must fail under the same SQL state as x/0, so the two shapes are \
         indistinguishable to a client"
    );
}

#[test]
fn e2e_float_div_filter_row_count_matches_native_oracle() {
    setup_e2e();
    let mut conn = exa_conn();

    let oracle_count = conn.query_scalar_i64(
        "SELECT COUNT(*) FROM \
         (SELECT 7 AS L_ORDERKEY, 1 AS L_LINENUMBER UNION ALL SELECT 7, 2) \
         WHERE L_ORDERKEY / L_LINENUMBER > 3",
    );
    assert_eq!(
        oracle_count, 2,
        "native oracle row count must be 2, got {oracle_count}"
    );

    let fixture_orderkey_7_count = conn.query_scalar_i64(&format!(
        "SELECT COUNT(*) FROM {} WHERE L_ORDERKEY = 7",
        vs_lineitem_table()
    ));
    assert_eq!(
        fixture_orderkey_7_count, LINES_PER_ORDER as i64,
        "the synthetic oracle table must describe the same fixture rows the \
         pushed-down filter runs over: L_ORDERKEY = 7 must hold {} rows in the \
         seeded fixture, got {fixture_orderkey_7_count}",
        LINES_PER_ORDER
    );
    assert_eq!(
        fixture_orderkey_7_count, oracle_count,
        "the synthetic oracle table must describe the same fixture rows the \
         pushed-down filter runs over: fixture row count for L_ORDERKEY = 7 \
         ({fixture_orderkey_7_count}) must match the oracle row count \
         ({oracle_count})"
    );

    let vs_sql = format!(
        "SELECT COUNT(*) FROM {} WHERE L_ORDERKEY / L_LINENUMBER > 3 AND L_ORDERKEY = 7",
        vs_lineitem_table()
    );
    let vs_count = conn.query_scalar_i64(&vs_sql);
    assert_eq!(
        vs_count, oracle_count,
        "pushed-down filter L_ORDERKEY/L_LINENUMBER > 3 AND L_ORDERKEY = 7 \
         must match the native oracle count {oracle_count} (truncated integer \
         division silently drops a matching row), got {vs_count}"
    );
}

/// Built from literals describing exactly the rows the pushed filter runs over.
fn native_lineitem_oracle() -> String {
    let rows: Vec<String> = (1..=FACT_ORDERS_ROWS)
        .flat_map(|orderkey| {
            (1..=LINES_PER_ORDER).map(move |linenumber| {
                format!("SELECT {orderkey} AS L_ORDERKEY, {linenumber} AS L_LINENUMBER")
            })
        })
        .collect();
    format!("({})", rows.join(" UNION ALL "))
}

/// Reads the filter field, not the whole pushed text: the function name also appears in
/// Exasol's echoed request.
fn pushed_scan_filter(pushed: &str) -> String {
    const KEY: &str = r#""filter":""#;
    let start = pushed
        .find(KEY)
        .unwrap_or_else(|| panic!("the pushed scan spec must carry a filter:\n{pushed}"))
        + KEY.len();
    let body = &pushed[start..];
    // The value is a JSON string inside a single-quoted SQL literal, so identifier quotes
    // are escaped; it ends at the first unescaped one.
    let bytes = body.as_bytes();
    let mut end = 0;
    while end < bytes.len() && !(bytes[end] == b'"' && (end == 0 || bytes[end - 1] != b'\\')) {
        end += 1;
    }
    body[..end].replace("\\\"", "\"")
}

/// The `projection` must be the bare selected column, so the call cannot be a select-list
/// expression. Exasol normalises `0 > <expr>` to `<expr> < 0`, so the comparison is not asserted.
fn assert_checked_division_reached_the_pushed_filter(pushed: &str) {
    let filter = pushed_scan_filter(pushed);
    assert!(
        filter.contains(&format!("{}(", vs_expression::CHECKED_FLOAT_DIV_FN)),
        "the pushed scan FILTER must carry the checked-division call, got \
         `{filter}`:\n{pushed}"
    );
    assert!(
        pushed.contains(r#""projection":["L_ORDERKEY"]"#),
        "the projection must be the bare selected column, so the division can \
         only have reached the scan as a predicate:\n{pushed}"
    );
}

/// Scenario: a division by zero in a pushed filter fails the query instead of changing the row count, for all four #370 shapes
#[test]
fn e2e_float_div_by_zero_in_filter_fails_like_native_exasol() {
    setup_e2e();
    let mut conn = exa_conn();

    let x_over_zero = "L_ORDERKEY / (L_LINENUMBER - L_LINENUMBER)";
    let zero_over_zero = "(L_LINENUMBER - L_LINENUMBER) / (L_LINENUMBER - L_LINENUMBER)";

    for divisor_shape in [x_over_zero, zero_over_zero] {
        for comparison in ["<", ">"] {
            let sql = format!(
                "SELECT L_ORDERKEY FROM {} WHERE 0 {comparison} {divisor_shape}",
                vs_lineitem_table()
            );

            let pushed = explain_virtual_pushdown_sql(&mut conn, &sql);
            assert_checked_division_reached_the_pushed_filter(&pushed);

            let sql_code = division_by_zero_failure_sql_code(&mut conn, &sql);
            assert_eq!(
                sql_code, UDF_ERROR_SQL_CODE,
                "every zero-divisor shape in predicate position must fail under \
                 one SQL state:\n{sql}"
            );
        }
    }
}

/// Scenario: a NULL divisor in a pushed predicate returns no rows without raising
#[test]
fn e2e_float_div_null_divisor_in_filter_returns_no_rows() {
    setup_e2e();
    let mut conn = exa_conn();

    let sql = format!(
        "SELECT L_ORDERKEY FROM {} \
         WHERE 0 < L_ORDERKEY / NULLIF(L_LINENUMBER - L_LINENUMBER, 0)",
        vs_lineitem_table()
    );

    let pushed = explain_virtual_pushdown_sql(&mut conn, &sql);
    assert_checked_division_reached_the_pushed_filter(&pushed);

    let rows = conn.query_row_count(&sql);
    assert_eq!(
        rows, 0,
        "a NULL divisor must return no rows and must NOT raise"
    );
}

/// Scenario: a zero-guarded division matches native Exasol guard-first and over-raises division-first (#392)
#[test]
fn e2e_float_div_guarded_by_a_non_zero_conjunct_matches_the_measured_outcome() {
    setup_e2e();
    let mut conn = exa_conn();

    let guard = "(L_LINENUMBER - 1) <> 0";
    let division = "0 < L_ORDERKEY / (L_LINENUMBER - 1)";
    let oracle = native_lineitem_oracle();
    let guarded_rows = (LINEITEM_ROWS / 2) as i64;

    let unguarded_oracle =
        conn.try_execute(&format!("SELECT L_ORDERKEY FROM {oracle} WHERE {division}"));
    assert_eq!(
        unguarded_oracle["status"].as_str(),
        Some("error"),
        "the oracle must really hold zero-divisor rows: native Exasol has to \
         raise for the UNGUARDED shape, else the guard below guards \
         nothing: {unguarded_oracle}"
    );

    let guard_first = format!("{guard} AND {division}");
    let division_first = format!("{division} AND {guard}");

    for predicate in [&guard_first, &division_first] {
        let oracle_rows = conn.query_row_count(&format!(
            "SELECT L_ORDERKEY FROM {oracle} WHERE {predicate}"
        ));
        assert_eq!(
            oracle_rows, guarded_rows,
            "native Exasol must return {guarded_rows} guarded rows WITHOUT \
             raising for `{predicate}` — the outcome task 1.2 recorded"
        );

        let vs_sql = format!(
            "SELECT L_ORDERKEY FROM {} WHERE {predicate}",
            vs_lineitem_table()
        );
        let filter = pushed_scan_filter(&explain_virtual_pushdown_sql(&mut conn, &vs_sql));
        assert!(
            filter.contains(r#"("L_LINENUMBER" - 1) <> 0"#)
                && filter.contains(&format!(
                    r#"{}("L_ORDERKEY", ("L_LINENUMBER" - 1))"#,
                    vs_expression::CHECKED_FLOAT_DIV_FN
                )),
            "both conjuncts must reach the scan in ONE pushed filter for \
             `{predicate}`, got `{filter}`"
        );
    }

    let guard_first_rows = conn.query_row_count(&format!(
        "SELECT L_ORDERKEY FROM {} WHERE {guard_first}",
        vs_lineitem_table()
    ));
    assert_eq!(
        guard_first_rows, guarded_rows,
        "GUARD FIRST must match native Exasol — {guarded_rows} rows, no error \
         — because the Parquet row filter applies the guard's row selection \
         before the division is evaluated"
    );

    // Holds while `datafusion.execution.parquet.reorder_filters` stays false: enabling it
    // sorts the one-column guard ahead of the division in both orders.
    let division_first_code = division_by_zero_failure_sql_code(
        &mut conn,
        &format!(
            "SELECT L_ORDERKEY FROM {} WHERE {division_first}",
            vs_lineitem_table()
        ),
    );
    assert_eq!(
        division_first_code, UDF_ERROR_SQL_CODE,
        "DIVISION FIRST raises where native Exasol returns {guarded_rows} rows: \
         the over-raise direction of tracked exception #392. If this ever stops \
         raising, the guard has started protecting the division in this order \
         too and #392 can narrow."
    );
}

/// Scenario: a division by zero in a pushed aggregate argument fails with the division-by-zero message
#[test]
fn e2e_float_div_by_zero_in_aggregate_argument_fails() {
    setup_e2e();
    let mut conn = exa_conn();

    let sql = format!(
        "SELECT SUM(L_ORDERKEY / (L_LINENUMBER - L_LINENUMBER)) FROM {}",
        vs_lineitem_table()
    );

    let pushed = explain_virtual_pushdown_sql(&mut conn, &sql);
    let expected_aggregate = format!(
        r#""aggregates":[{{"kind":"sum","arg_expr":"{}("#,
        vs_expression::CHECKED_FLOAT_DIV_FN
    );
    assert!(
        pushed.contains(&expected_aggregate),
        "the aggregate must be PUSHED with the checked division as its \
         argument, expected {expected_aggregate} — an Exasol-side aggregate \
         would never reach the scan:\n{pushed}"
    );

    let sql_code = division_by_zero_failure_sql_code(&mut conn, &sql);
    assert_eq!(
        sql_code, UDF_ERROR_SQL_CODE,
        "a zero divisor inside a pushed aggregate argument must fail under the \
         same SQL state as one in a projection or a filter"
    );
}

/// Scenario: GREATEST/LEAST return NULL when any argument is NULL, as in Exasol (#202)
#[test]
fn test_greatest_least_propagate_null_argument() {
    setup_e2e();
    let mut conn = exa_conn();

    let predicate_sql = format!(
        "SELECT COUNT(*) FROM {} WHERE LEAST(id, NULLIF(MOD(id, 5), 0)) IS NULL",
        vs_table()
    );
    let null_row_count = conn.query_scalar_i64(&predicate_sql);
    assert_eq!(
        null_row_count, 4,
        "LEAST(id, NULLIF(MOD(id, 5), 0)) IS NULL must match the 4 rows where \
         id is a multiple of 5, got {null_row_count}"
    );

    let predicate_pushed = explain_virtual_pushdown_sql(&mut conn, &predicate_sql);
    assert!(
        predicate_pushed.contains("CASE WHEN") && predicate_pushed.contains("least("),
        "the pushed scan spec must carry the NULL-guarded CASE rendering of \
         LEAST, not an unguarded call, got:\n{predicate_pushed}"
    );

    let value_sql = format!(
        "SELECT id, GREATEST(id, NULLIF(MOD(id, 5), 0)) FROM {} ORDER BY id",
        vs_table()
    );
    let cols = conn.query_columns(&value_sql);
    assert_eq!(cols.len(), 2, "expected 2 columns (id, greatest): {cols:?}");
    assert_eq!(
        cols[0].len(),
        SEED_TOTAL_ROWS,
        "expected {SEED_TOTAL_ROWS} rows: {cols:?}"
    );

    for (id_val, greatest_val) in cols[0].iter().zip(cols[1].iter()) {
        let id = parse_int(id_val);
        if id % 5 == 0 {
            assert!(
                greatest_val.is_null(),
                "GREATEST(id, NULLIF(MOD(id, 5), 0)) must be NULL at id={id} \
                 (a multiple of 5), got {greatest_val:?}"
            );
        } else {
            let greatest = parse_int(greatest_val);
            assert_eq!(
                greatest, id,
                "GREATEST(id, NULLIF(MOD(id, 5), 0)) must equal id={id} for a \
                 non-multiple-of-5 row, got {greatest}"
            );
        }
    }

    let value_pushed = explain_virtual_pushdown_sql(&mut conn, &value_sql);
    assert!(
        value_pushed.contains("CASE WHEN") && value_pushed.contains("greatest("),
        "the pushed scan spec must carry the NULL-guarded CASE rendering of \
         GREATEST, not an unguarded call, got:\n{value_pushed}"
    );
}

/// Scenario: `||`/CONCAT treat a NULL operand as an empty string, as in Exasol (#374)
#[test]
fn test_concat_null_operand_concatenates_non_null_parts() {
    setup_e2e();
    let mut conn = exa_conn();

    let value_sql = format!(
        "SELECT id, name || NULLIF(name, name) || '-suffix' FROM {} WHERE id <= 3 ORDER BY id",
        vs_table()
    );
    let cols = conn.query_columns(&value_sql);
    assert_eq!(cols.len(), 2, "expected 2 columns (id, concat): {cols:?}");
    assert_eq!(cols[0].len(), 3, "expected 3 rows (id <= 3): {cols:?}");
    let expected = ["event-01-suffix", "event-02-suffix", "event-03-suffix"];
    for (concat_val, expected_val) in cols[1].iter().zip(expected.iter()) {
        assert_eq!(
            concat_val.as_str(),
            Some(*expected_val),
            "name || NULLIF(name, name) || '-suffix' must concatenate the \
             non-NULL parts, got {concat_val:?}"
        );
    }

    let value_pushed = explain_virtual_pushdown_sql(&mut conn, &value_sql);
    assert!(
        value_pushed.contains("nullif(concat("),
        "the pushed scan spec must carry the nullif(concat(...), '') \
         rendering of CONCAT, not a bare concat or chained ||, got:\n{value_pushed}"
    );

    let filter_sql = format!(
        "SELECT COUNT(*) FROM {} WHERE (name || NULLIF(name, name)) = name",
        vs_table()
    );
    let filter_count = conn.query_scalar_i64(&filter_sql);
    assert_eq!(
        filter_count, SEED_TOTAL_ROWS as i64,
        "(name || NULLIF(name, name)) = name must match all {SEED_TOTAL_ROWS} \
         rows since the NULL operand contributes nothing, got {filter_count}"
    );

    let filter_pushed = explain_virtual_pushdown_sql(&mut conn, &filter_sql);
    assert!(
        filter_pushed.contains("nullif(concat("),
        "the pushed scan spec must carry the nullif(concat(...), '') \
         rendering of CONCAT, not a bare concat or chained ||, got:\n{filter_pushed}"
    );

    // Exasol's VARCHAR domain has no empty string, so an all-NULL CONCAT is NULL.
    let all_null_filter_sql = format!(
        "SELECT COUNT(*) FROM {} WHERE (NULLIF(name, name) || NULLIF(name, name)) IS NULL",
        vs_table()
    );
    let all_null_count = conn.query_scalar_i64(&all_null_filter_sql);
    assert_eq!(
        all_null_count, SEED_TOTAL_ROWS as i64,
        "an all-NULL-operand CONCAT must itself be NULL for all \
         {SEED_TOTAL_ROWS} rows, not the empty string a bare concat(...) \
         would render, got {all_null_count}"
    );

    let all_null_pushed = explain_virtual_pushdown_sql(&mut conn, &all_null_filter_sql);
    assert!(
        all_null_pushed.contains("nullif(concat("),
        "the pushed scan spec must carry the nullif(concat(...), '') \
         rendering of CONCAT, not a bare concat or chained ||, got:\n{all_null_pushed}"
    );
}
