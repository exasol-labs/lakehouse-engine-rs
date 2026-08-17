//! End-to-end integration tests for the lakehouse-engine Virtual Schema against
//! a native Unity Catalog OSS server (the second catalog kind), backed by the
//! base stack's MinIO and seeded with the vendored Delta fixtures (#325 harness).
//!
//! These tests run against the overlay stack (Exasol + MinIO + Unity Catalog),
//! brought up by `make unity-up`. They FAIL (never skip) when the stack is
//! unavailable — the same contract as the baseline `exasol-e2e` suite. #318
//! lists tables and their column metadata; `handle_pushdown` now routes a Unity
//! Catalog / Delta pushdown request through the same `TableScanResolver` and
//! `FormatReader` seam an Iceberg request uses (#320), so the round-trip
//! scenarios below issue real queries through Exasol and assert the rows they
//! return. `unity_delta_planning_agrees_under_vended_and_static_credentials`
//! separately exercises Delta table PLANNING directly through the seam
//! (`format_reader`/`ScanSource::UnityDelta`), bypassing `handle_pushdown`
//! entirely.
//!
//! All tests share one Exasol (one virtual schema), so they must run serially
//! (`--test-threads=1`); the `make test-e2e-unity` target passes the flag.
//!
//! The CONNECTION address is the docker-network Unity Catalog host and its
//! password supplies no CATALOG-auth field, because the OSS server's
//! authorization is disabled — but it does carry the MinIO endpoint and static
//! storage credentials the UDF-side scan reads through, since the OSS Unity
//! Catalog server vends no S3 endpoint of its own.
//! `unity_credentials_never_appear_in_output` pins the redaction contract on
//! the failure path.
#![cfg(feature = "unity-e2e")]

mod common;

use common::e2e_harness::{
    ADAPTER_SCRIPT_NAME, SCAN_SCRIPT_NAME, SCHEMA_NAME, SYS_PASSWORD, create_schema_and_scripts,
    exa_conn, explain_virtual_sql, has_broadcast_join_block, has_two_scan_wrapper, install_slc,
    parse_int, upload_so,
};
use common::exasol_ws::ExaConn;
use common::stack::{
    self, CatalogConnectionPassword, build_create_connection_sql, exasol_host, exasol_sql_port,
    local_stack_connection_password, wait_for_exasol, wait_for_minio, wait_for_url,
};

use lakehouse_catalog::{
    CatalogClient, CatalogTableIdent, ConnectionCreds, StorageBackend, UnityCatalogSession,
};
use lakehouse_engine::adapter::connection::storage_block;
use lakehouse_engine::adapter::pushdown::{
    ConnectionStorage, ResolvedScan, ScanSource, format_reader,
};
use lakehouse_engine::scan::spec::{DeleteMechanism, FileEntry};

use std::sync::OnceLock;
use std::time::Duration;

/// Virtual Schema over the seeded `unity.delta_e2e` namespace.
const VS_NAME: &str = "UNITY_DELTA_E2E_VS";
/// Catalog CONNECTION carrying the (no-auth) Unity Catalog address.
const CONN_NAME: &str = "UNITY_CATALOG_CREDS";
/// The seeded catalog + schema, addressed `catalog.schema`.
const UNITY_NAMESPACE: &str = "unity.delta_e2e";
/// Unity Catalog as reached from inside the Exasol UDF container (docker network).
const UNITY_CATALOG_URI_INTERNAL: &str = "http://unitycatalog:8080";

const READINESS_TIMEOUT: Duration = Duration::from_secs(30);

/// The eight seeded fixture tables, as their flatten-and-uppercase Exasol names.
const EXPECTED_TABLES: &[&str] = &[
    "TABLE_WITH_DV",
    "CM_NAME_MODE",
    "CM_ID_MODE",
    "BASIC_PARTITIONED",
    "MULTI_PART_STATS",
    "STATS_ALL_TYPES",
    "UNSHREDDED_VARIANT",
    "TYPE_WIDENING",
];

/// Unity Catalog REST host port (host-side). `LH_UNITY_PORT`, default 18080.
fn unity_port() -> u16 {
    std::env::var("LH_UNITY_PORT")
        .ok()
        .and_then(|s| s.trim().parse::<u16>().ok())
        .unwrap_or(18080)
}

/// Assert the Unity Catalog REST server is serving; panic if not.
///
/// A 2xx on the `catalogs` endpoint proves the server is actually serving, not
/// merely that the port is open — the same signal the compose healthcheck uses.
fn wait_for_unity_catalog() {
    let url = format!(
        "http://localhost:{}/api/2.1/unity-catalog/catalogs",
        unity_port()
    );
    wait_for_url(&url, READINESS_TIMEOUT);
}

// ---------------------------------------------------------------------------
// One-time setup (shared across the serial binary).
// ---------------------------------------------------------------------------

static SETUP_DONE: OnceLock<()> = OnceLock::new();

fn setup() {
    SETUP_DONE.get_or_init(|| {
        // Readiness — fail loud, never skip.
        wait_for_exasol();
        wait_for_minio();
        wait_for_unity_catalog();

        // Shared-harness provisioning (SLC + .so + scripts) — REUSED, never
        // redeclared, so the adapter script DDL is byte-identical to every other
        // E2E binary.
        install_slc();
        upload_so();
        let mut conn = exa_conn();
        create_schema_and_scripts(&mut conn);

        create_unity_virtual_schema(&mut conn);
    });
}

/// Create the Unity Catalog virtual schema over `unity.delta_e2e` through the
/// shared adapter script. The CONNECTION carries the Unity address plus a
/// password that now also carries the MinIO endpoint and static storage
/// credentials the UDF-side scan reads through; the `UNITY_CATALOG` catalog
/// kind routes createVirtualSchema through the native Unity Catalog client.
fn create_unity_virtual_schema(conn: &mut ExaConn) {
    // MinIO endpoint + static storage credentials, the SAME shape every other
    // E2E suite's CONNECTION carries: the OSS Unity Catalog server vends no
    // S3 endpoint of its own, so the UDF-side scan resolves object storage
    // through this CONNECTION rather than a test-process injection.
    let password = local_stack_connection_password();
    let create_conn_sql =
        build_create_connection_sql(CONN_NAME, UNITY_CATALOG_URI_INTERNAL, &password);
    conn.execute(&create_conn_sql);

    let _ = conn.try_execute(&format!("DROP VIRTUAL SCHEMA IF EXISTS {VS_NAME} CASCADE"));

    conn.execute(&format!(
        r#"CREATE VIRTUAL SCHEMA {VS_NAME}
USING {SCHEMA_NAME}.{ADAPTER_SCRIPT_NAME} WITH
  CATALOG_CONNECTION = '{CONN_NAME}'
  CATALOG_KIND       = 'UNITY_CATALOG'
  ICEBERG_NAMESPACE  = '{UNITY_NAMESPACE}'
  ALLOW_HTTP         = 'true'"#
    ));
}

// ---------------------------------------------------------------------------
// Result helpers.
// ---------------------------------------------------------------------------

/// The uppercased table names enumerated for `vs_name`.
fn enumerated_table_names(conn: &mut ExaConn, vs_name: &str) -> Vec<String> {
    let cols = conn.query_columns(&format!(
        "SELECT TABLE_NAME FROM SYS.EXA_ALL_TABLES WHERE TABLE_SCHEMA = '{vs_name}'"
    ));
    cols.first()
        .map(|c| {
            c.iter()
                .filter_map(|v| v.as_str().map(|s| s.to_uppercase()))
                .collect()
        })
        .unwrap_or_default()
}

/// The `(COLUMN_NAME, COLUMN_TYPE)` pairs declared for `table` under `vs_name`,
/// both uppercased.
fn column_types(conn: &mut ExaConn, vs_name: &str, table: &str) -> Vec<(String, String)> {
    let cols = conn.query_columns(&format!(
        "SELECT COLUMN_NAME, COLUMN_TYPE FROM SYS.EXA_ALL_COLUMNS \
         WHERE COLUMN_SCHEMA = '{vs_name}' AND COLUMN_TABLE = '{table}'"
    ));
    if cols.len() < 2 {
        return Vec::new();
    }
    cols[0]
        .iter()
        .zip(cols[1].iter())
        .filter_map(|(name, ty)| Some((name.as_str()?.to_uppercase(), ty.as_str()?.to_uppercase())))
        .collect()
}

/// Assert `column` is declared with an Exasol type in `expected`'s family,
/// tolerant of Exasol's `COLUMN_TYPE` rendering (whitespace, a `VARCHAR ... UTF8`
/// charset suffix, a `DOUBLE`/`DOUBLE PRECISION` alias) by matching on the
/// space-stripped prefix.
fn assert_col_type(cols: &[(String, String)], column: &str, expected: &str) {
    let actual = cols
        .iter()
        .find(|(name, _)| name == column)
        .unwrap_or_else(|| panic!("column {column} not declared; got {cols:?}"));
    let strip = |s: &str| s.chars().filter(|c| !c.is_whitespace()).collect::<String>();
    let actual_ty = strip(&actual.1);
    let expected_ty = strip(expected);
    assert!(
        actual_ty.starts_with(&expected_ty),
        "column {column}: expected Exasol type starting {expected}, got {}",
        actual.1
    );
}

// ---------------------------------------------------------------------------
// Create virtual schema lists the fixture tables and their columns.
// ---------------------------------------------------------------------------

/// createVirtualSchema over the seeded Unity Catalog namespace enumerates every
/// fixture table and declares representative columns with the expected
/// Exasol-mapped types, including an incompatible Spark type surfaced as VARCHAR.
/// Enumeration runs the native Unity Catalog client's single `GET /tables` sweep
/// over the no-auth OSS server; the tables appearing proves that client reached
/// the catalog and mapped its inline `columns[]`.
#[test]
fn unity_create_virtual_schema_lists_fixture_tables_and_columns() {
    setup();
    let mut conn = exa_conn();

    let tables = enumerated_table_names(&mut conn, VS_NAME);
    for expected in EXPECTED_TABLES {
        assert!(
            tables.iter().any(|t| t == expected),
            "createVirtualSchema must enumerate the seeded '{expected}' fixture table; got {tables:?}"
        );
    }

    // Representative scalar column set: LONG -> DECIMAL(20,0), STRING -> VARCHAR,
    // DOUBLE -> DOUBLE PRECISION.
    let cm_cols = column_types(&mut conn, VS_NAME, "CM_NAME_MODE");
    assert_col_type(&cm_cols, "ID", "DECIMAL(20,0)");
    assert_col_type(&cm_cols, "NAME", "VARCHAR(2000000)");
    assert_col_type(&cm_cols, "VALUE", "DOUBLE");

    // An incompatible Spark type (ARRAY) is declared as VARCHAR, not failed.
    let stats_cols = column_types(&mut conn, VS_NAME, "STATS_ALL_TYPES");
    assert_col_type(&stats_cols, "ARRAY_COL", "VARCHAR(2000000)");
}

// ---------------------------------------------------------------------------
// Fail-not-skip when the stack is down.
// ---------------------------------------------------------------------------

/// The Unity Catalog readiness contract is fail-loud: a readiness wait against an
/// unreachable stack PANICS (never returns cleanly), so a down stack surfaces as
/// a test failure, never a silent skip. Exercises the very `wait_for_url` helper
/// `wait_for_unity_catalog` is built on, pointed at a closed local port with a
/// short deadline.
#[test]
fn unity_suite_fails_when_stack_unavailable() {
    let result = std::panic::catch_unwind(|| {
        // 127.0.0.1:1 refuses immediately; the poll loop hits the short deadline
        // and panics rather than returning — the fail-not-skip contract.
        wait_for_url(
            "http://127.0.0.1:1/api/2.1/unity-catalog/catalogs",
            Duration::from_secs(2),
        );
    });
    assert!(
        result.is_err(),
        "a readiness wait against an unreachable Unity Catalog stack must panic (fail), \
         never return Ok (skip)"
    );
}

// ---------------------------------------------------------------------------
// No credential value ever appears in captured output / panic messages.
// ---------------------------------------------------------------------------

/// A failing, token-bearing Unity Catalog CONNECTION DDL executed through a
/// redacting `ExaConn` must not surface the SQL text or the bearer token in the
/// failure output. An obviously-fake sentinel carries the token, an invalid
/// trailing token forces the DDL-failure path, and the captured panic message is
/// asserted to contain neither the sentinel nor the SQL text.
#[test]
fn unity_credentials_never_appear_in_output() {
    const SENTINEL_TOKEN: &str = "UC_DUMMY_BEARER_TOKEN_SENTINEL";

    wait_for_exasol();
    let mut conn =
        ExaConn::connect_redacting(&exasol_host(), exasol_sql_port(), "sys", SYS_PASSWORD);

    let sentinel_password = CatalogConnectionPassword {
        token: Some(SENTINEL_TOKEN.to_string()),
        ..Default::default()
    };
    let base_sql = build_create_connection_sql(
        "UC_REDACTION_PROBE",
        UNITY_CATALOG_URI_INTERNAL,
        &sentinel_password,
    );
    let failing_sql = format!("{base_sql} THIS_TRAILING_TOKEN_MAKES_THE_STATEMENT_INVALID");

    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        conn.execute(&failing_sql);
    }));
    let payload = match result {
        Ok(_) => panic!("expected execute() to fail on the malformed credential-bearing DDL"),
        Err(p) => p,
    };
    let panic_msg = stack::panic_payload_message(&*payload).unwrap_or_default();

    assert!(
        !panic_msg.is_empty(),
        "expected a string panic payload from the failed redacting execute()"
    );
    assert!(
        !panic_msg.contains(&failing_sql),
        "redacting execute() failure must not echo the SQL text: {panic_msg}"
    );
    assert!(
        !panic_msg.contains(SENTINEL_TOKEN),
        "redacting execute() failure must not leak the bearer token: {panic_msg}"
    );
}

// ---------------------------------------------------------------------------
// Delta table planning through the FormatReader seam (bypasses handle_pushdown).
// ---------------------------------------------------------------------------

/// Unity Catalog REST base URL as reached from the test process (host-side).
fn unity_catalog_url() -> String {
    format!("http://localhost:{}", unity_port())
}

/// Credential fields for reading `unity.delta_e2e`'s MinIO-backed tables.
///
/// `endpoint`/`region` are the CONNECTION's own static store address. Under
/// vending they cross over into the vended backend through
/// `StaticStoreAddress::from(&ConnectionCreds)`, because the OSS Unity Catalog
/// server vends NO S3 endpoint of its own (`scripts/unity/README.md`) — the
/// client injects MinIO's address itself, exactly as it does under the fully
/// static run below.
fn delta_creds(use_vended_credentials: bool) -> ConnectionCreds {
    ConnectionCreds {
        warehouse: String::new(),
        endpoint: stack::minio_url(),
        region: "us-east-1".to_string(),
        access_key: "minioadmin".to_string(),
        secret_key: "minioadmin".to_string(),
        session_token: None,
        path_style: true,
        use_sigv4: false,
        use_vended_credentials,
        token: None,
        client_id: None,
        client_secret: None,
        oauth2_server_uri: None,
        scope: None,
        account_name: None,
        account_key: None,
        sas_token: None,
    }
}

/// The CONNECTION's own static storage backend — what `format_reader` reads
/// the log through when vending is disabled.
fn delta_static_storage() -> StorageBackend {
    storage_block(&delta_creds(false), true)
}

fn delta_e2e_table(name: &str) -> CatalogTableIdent {
    CatalogTableIdent {
        namespace: vec!["unity".to_string(), "delta_e2e".to_string()],
        name: name.to_string(),
    }
}

/// Resolve `table_name`'s Delta scan through the `FormatReader` seam: load the
/// table's metadata from the live Unity Catalog server, select the Delta reader
/// via `format_reader`, and resolve its scan. `use_vended_credentials` selects
/// which of the two credential modes the request exercises; `handle_pushdown`
/// is never reached, matching this plan's scope.
async fn resolve_delta_scan(table_name: &str, use_vended_credentials: bool) -> ResolvedScan {
    let creds = delta_creds(use_vended_credentials);
    let session = UnityCatalogSession::new(&unity_catalog_url(), creds.clone());
    let table = session
        .load_table(&delta_e2e_table(table_name))
        .await
        .unwrap_or_else(|e| panic!("load_table({table_name}) failed: {e}"));
    let storage = delta_static_storage();

    let reader = format_reader(
        ScanSource::UnityDelta {
            session: &session,
            table: &table,
        },
        &ConnectionStorage {
            storage: &storage,
            creds: &creds,
            allow_http: true,
        },
    )
    .unwrap_or_else(|e| panic!("format_reader({table_name}) failed: {e}"));

    reader
        .resolve_scan(None)
        .await
        .unwrap_or_else(|e| panic!("resolve_scan({table_name}) failed: {e}"))
}

/// Each resolved file's `letter` partition value, ordered by path — the live
/// counterpart of the offline pin in
/// `crates/lakehouse-engine/src/adapter/pushdown/format/delta_replay_tests.rs`.
///
/// Panics unless EVERY entry carries exactly one partition entry keyed `letter`:
/// partition values live only in the transaction log, so an empty map is a
/// resolution that silently lost them, which comparing two runs against each
/// other cannot detect.
fn path_sorted_letter_values(files: &[FileEntry]) -> Vec<Option<String>> {
    let mut carried: Vec<(&str, Option<String>)> = files
        .iter()
        .map(|entry| {
            let mut partition_values = entry.partition_values.iter();
            let (column, value) = partition_values.next().unwrap_or_else(|| {
                panic!(
                    "{} must carry its logged partition value, not an empty map",
                    entry.path
                )
            });
            assert!(
                partition_values.next().is_none(),
                "{} must carry exactly one partition entry: {:?}",
                entry.path,
                entry.partition_values
            );
            assert_eq!(
                column, "letter",
                "`letter` is basic_partitioned's only partition column"
            );
            (entry.path.as_str(), value.clone())
        })
        .collect();
    carried.sort_by(|(left, _), (right, _)| left.cmp(right));
    carried.into_iter().map(|(_, value)| value).collect()
}

fn rt() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime")
}

/// Scenario: Delta planning resolves its storage credential through the
/// table's own catalog.
///
/// `basic_partitioned` is resolved twice — once under vending (a real Unity
/// Catalog temporary-table-credentials request, vending a real MinIO STS session
/// minted and injected by the fixture harness, never a static key)
/// and once under the CONNECTION's own static MinIO credential — and both runs
/// must agree on the file list, the per-file partition values (carried in
/// `FileEntry::partition_values`), and the table root: both read the SAME transaction log
/// through two DIFFERENT credential paths that must terminate in an equivalent
/// view of the table. `effective_storage` is deliberately NOT compared, since
/// the two runs read through genuinely different credentials by design.
///
/// Agreement alone would be satisfied by two identically-empty partition maps, so the
/// `letter` values are pinned outright: six files, one partition entry each, and the
/// `letter=__HIVE_DEFAULT_PARTITION__/` file resolved to an explicit NULL rather than to
/// the directory literal. That pin mirrors the offline one in `delta_replay_tests.rs`,
/// which reads a local filesystem store and so could not catch a live-S3-only regression
/// that dropped partition values or resolved no Delta block at all.
///
/// `table_with_dv`'s single active file must carry a deletion-vector
/// reference, proving the reader returns the re-added `add` action rather than
/// the delete-free one it replaced.
///
/// Fails, never skips, when the stack is unreachable: `wait_for_minio` and
/// `wait_for_unity_catalog` panic on a timed-out readiness poll rather than
/// returning early.
#[test]
fn unity_delta_planning_agrees_under_vended_and_static_credentials() {
    wait_for_minio();
    wait_for_unity_catalog();

    let rt = rt();
    let vended = rt.block_on(resolve_delta_scan("basic_partitioned", true));
    let static_creds = rt.block_on(resolve_delta_scan("basic_partitioned", false));

    assert!(
        !vended.files.is_empty(),
        "basic_partitioned must resolve at least one active data file"
    );
    assert_eq!(
        vended.table_root, static_creds.table_root,
        "vended and static credential runs must resolve the identical table root"
    );
    assert_eq!(
        vended.files, static_creds.files,
        "vended and static credential runs must agree, entry for entry, on the resolved \
         file list"
    );

    let letters = path_sorted_letter_values(&vended.files);
    assert_eq!(
        letters,
        vec![
            None,
            Some("a".to_string()),
            Some("a".to_string()),
            Some("b".to_string()),
            Some("c".to_string()),
            Some("e".to_string()),
        ],
        "the live S3 path must carry every logged partition value, the Hive \
         default-partition file's explicit NULL among them"
    );
    assert!(
        !letters
            .iter()
            .any(|value| value.as_deref() == Some("__HIVE_DEFAULT_PARTITION__")),
        "the Hive default-partition DIRECTORY literal is never carried as a value: \
         {letters:?}"
    );

    let dv_scan = rt.block_on(resolve_delta_scan("table_with_dv", true));
    assert_eq!(
        dv_scan.files.len(),
        1,
        "table_with_dv must resolve exactly one active data file: {:?}",
        dv_scan.files
    );
    let has_deletion_vector = dv_scan.files[0]
        .deletes
        .iter()
        .any(|delete| matches!(delete, DeleteMechanism::DeltaDeletionVector { .. }));
    assert!(
        has_deletion_vector,
        "table_with_dv's single active file must carry a deletion-vector reference: {:?}",
        dv_scan.files[0]
    );
}

// ---------------------------------------------------------------------------
// Round-trip query scenarios (#320): the format-reader seam wired into
// production pushdown, exercised end to end through Exasol.
// ---------------------------------------------------------------------------

/// A virtual table reference, `VS_NAME.TABLE`.
fn table_ref(table: &str) -> String {
    format!("{VS_NAME}.{table}")
}

/// Parse the JSON value (object or array) starting at `start` — the index of its
/// opening `{` or `[` — within `text`, matching brackets through quoted strings so
/// an embedded `{`/`}`/`[`/`]` inside a string value cannot end the scan early.
/// Returns the parsed value and the index just past its closing bracket.
fn json_value_at(text: &str, start: usize) -> (serde_json::Value, usize) {
    let bytes = text.as_bytes();
    let (open, close) = match bytes[start] {
        b'{' => (b'{', b'}'),
        b'[' => (b'[', b']'),
        other => {
            panic!("expected a JSON object or array at offset {start}, found byte {other}: {text}")
        }
    };
    let mut depth = 0usize;
    let mut in_string = false;
    let mut escaped = false;
    let mut end = None;
    for (i, &b) in bytes[start..].iter().enumerate() {
        if in_string {
            match b {
                _ if escaped => escaped = false,
                b'\\' => escaped = true,
                b'"' => in_string = false,
                _ => {}
            }
            continue;
        }
        match b {
            b'"' => in_string = true,
            b if b == open => depth += 1,
            b if b == close => {
                depth -= 1;
                if depth == 0 {
                    end = Some(start + i + 1);
                    break;
                }
            }
            _ => {}
        }
    }
    let end = end.unwrap_or_else(|| panic!("unbalanced JSON at offset {start}: {text}"));
    let value = serde_json::from_str(&text[start..end])
        .unwrap_or_else(|e| panic!("expected valid JSON at offset {start} ({e}): {text}"));
    (value, end)
}

/// Scenario: A delete-free Delta table returns its rows end to end.
///
/// `multi_part_stats` (5 files, 5 rows, delete-free, unpartitioned) is this
/// engine's FIRST full round trip over a Delta table. `create_unity_virtual_schema`'s
/// CONNECTION carries the MinIO endpoint and static storage credentials
/// (`local_stack_connection_password`, the same credential shape and shared-harness
/// provisioning every other E2E binary uses), because the OSS Unity Catalog server
/// vends no S3 endpoint of its own — this table's non-NULL column values prove the
/// UDF-side scan actually resolved a credential from the CONNECTION; with none, the
/// read against MinIO would fail.
#[test]
fn unity_delta_delete_free_table_returns_its_rows() {
    setup();
    let mut conn = exa_conn();

    let count = conn.query_scalar_i64(&format!(
        "SELECT COUNT(*) FROM {}",
        table_ref("MULTI_PART_STATS")
    ));
    assert_eq!(
        count, 5,
        "multi_part_stats' five active data files hold five rows in total"
    );

    let cols = conn.query_columns(&format!(
        "SELECT ID, \"VALUE\" FROM {}",
        table_ref("MULTI_PART_STATS")
    ));
    assert_eq!(cols.len(), 2, "expected ID, VALUE columns: {cols:?}");
    assert_eq!(
        cols[0].len(),
        5,
        "SELECT must return the same 5 rows COUNT(*) reports: {cols:?}"
    );
    assert!(
        cols.iter().all(|col| col.iter().all(|v| !v.is_null())),
        "a delete-free table's rows must carry real column values, not NULL: {cols:?}"
    );
}

/// Scenario: A Delta table with deletion vectors returns only its live rows.
///
/// `table_with_dv` (1 file, 10 physical rows, a UUID-relative deletion vector of
/// cardinality 2) removes the rows whose `value` is 0 and 9.
#[test]
fn unity_delta_deletion_vector_table_returns_only_live_rows() {
    setup();
    let mut conn = exa_conn();

    let count = conn.query_scalar_i64(&format!(
        "SELECT COUNT(*) FROM {}",
        table_ref("TABLE_WITH_DV")
    ));
    assert_eq!(
        count, 8,
        "the deletion vector removes 2 of table_with_dv's 10 physical rows"
    );

    let cols = conn.query_columns(&format!(
        "SELECT \"VALUE\" FROM {}",
        table_ref("TABLE_WITH_DV")
    ));
    let values: Vec<i64> = cols[0].iter().map(parse_int).collect();
    assert_eq!(values.len(), 8, "expected 8 live values: {values:?}");
    assert!(
        !values.contains(&0) && !values.contains(&9),
        "the deleted values 0 and 9 must be absent: {values:?}"
    );

    let filtered = conn.query_scalar_i64(&format!(
        "SELECT COUNT(*) FROM {} WHERE \"VALUE\" = 0",
        table_ref("TABLE_WITH_DV")
    ));
    assert_eq!(
        filtered, 0,
        "a predicate selecting a deleted row must return no row: the deletion \
         vector is applied beneath the pushed-down filter"
    );
}

/// Scenario: A column-mapped Delta table returns values under its logical
/// column names.
///
/// `cm_id_mode` and `cm_name_mode` carry Parquet columns physically named
/// `col-<uuid>` while their Delta schemas declare `id`, `name`, `value`. Neither
/// column may be NULL — a logical-name-only binding against a `col-<uuid>`
/// physical name would produce exactly that.
///
/// The two fixtures are independently seeded delta-kernel-rs CDF tables that
/// share a schema shape, not two views of the same underlying data: a live run
/// shows `cm_id_mode` holding ids `{1,2,4}` and `cm_name_mode` holding ids
/// `{2,3,4}` with different names and values throughout. So this scenario
/// verifies each table's binding independently rather than asserting row
/// equality between them.
#[test]
fn unity_delta_column_mapped_tables_return_logical_column_values() {
    setup();
    let mut conn = exa_conn();

    let id_mode = conn.query_columns(&format!(
        "SELECT ID, NAME, \"VALUE\" FROM {} ORDER BY ID",
        table_ref("CM_ID_MODE")
    ));
    let name_mode = conn.query_columns(&format!(
        "SELECT ID, NAME, \"VALUE\" FROM {} ORDER BY ID",
        table_ref("CM_NAME_MODE")
    ));

    assert_eq!(
        id_mode.len(),
        3,
        "expected ID, NAME, VALUE columns: {id_mode:?}"
    );
    assert!(
        !id_mode[0].is_empty(),
        "cm_id_mode must return at least one row"
    );
    assert!(
        !name_mode[0].is_empty(),
        "cm_name_mode must return at least one row"
    );

    for (label, cols) in [("CM_ID_MODE", &id_mode), ("CM_NAME_MODE", &name_mode)] {
        for (col_idx, col_name) in ["ID", "NAME", "VALUE"].iter().enumerate() {
            assert!(
                cols[col_idx].iter().all(|v| !v.is_null()),
                "{label}.{col_name} must never be NULL: a logical-name-only \
                 binding against a col-<uuid> physical name would produce NULL: \
                 {cols:?}"
            );
        }
    }
}

/// Scenario: A partitioned Delta table returns its partition column values.
///
/// `basic_partitioned` (6 files, 6 rows) is partitioned by `letter`; one file
/// lives under the Hive default-partition directory because its `letter` is
/// NULL. The live values are pinned outright — `a,a,b,c,e,NULL` — the same
/// six-file fixture `unity_delta_planning_agrees_under_vended_and_static_credentials`
/// already proves at the `FormatReader` layer; this scenario proves the SAME
/// values reach a real query, its WHERE clause, and its GROUP BY.
#[test]
fn unity_delta_partitioned_table_returns_partition_values() {
    setup();
    let mut conn = exa_conn();

    let letters: Vec<Option<String>> = conn.query_columns(&format!(
        "SELECT LETTER FROM {}",
        table_ref("BASIC_PARTITIONED")
    ))[0]
        .iter()
        .map(|v| v.as_str().map(str::to_string))
        .collect();
    assert_eq!(
        letters.len(),
        6,
        "basic_partitioned's six files hold six rows: {letters:?}"
    );
    assert!(
        !letters
            .iter()
            .any(|l| l.as_deref() == Some("__HIVE_DEFAULT_PARTITION__")),
        "the Hive default-partition DIRECTORY literal must never surface as a \
         value: {letters:?}"
    );
    assert_eq!(
        letters.iter().filter(|l| l.is_none()).count(),
        1,
        "exactly one row's logged letter is NULL (the default-partition file): \
         {letters:?}"
    );

    let filtered = conn.query_scalar_i64(&format!(
        "SELECT COUNT(*) FROM {} WHERE LETTER = 'a'",
        table_ref("BASIC_PARTITIONED")
    ));
    assert_eq!(
        filtered, 2,
        "exactly the rows whose logged partition value is 'a' must match the filter"
    );

    let grouped = conn.query_columns(&format!(
        "SELECT LETTER, COUNT(*) FROM {} GROUP BY LETTER",
        table_ref("BASIC_PARTITIONED")
    ));
    assert_eq!(
        grouped.len(),
        2,
        "expected LETTER, COUNT(*) columns: {grouped:?}"
    );
    let mut group_counts: std::collections::HashMap<Option<String>, i64> = grouped[0]
        .iter()
        .zip(grouped[1].iter())
        .map(|(letter, count)| (letter.as_str().map(str::to_string), parse_int(count)))
        .collect();
    assert_eq!(
        group_counts.remove(&None),
        Some(1),
        "the NULL-letter group must hold exactly 1 row: {group_counts:?}"
    );
    assert_eq!(
        group_counts.remove(&Some("a".to_string())),
        Some(2),
        "group 'a' must hold exactly 2 rows: {group_counts:?}"
    );
    for letter in ["b", "c", "e"] {
        assert_eq!(
            group_counts.remove(&Some(letter.to_string())),
            Some(1),
            "group '{letter}' must hold exactly 1 row: {group_counts:?}"
        );
    }
    assert!(
        group_counts.is_empty(),
        "no group beyond NULL,a,b,c,e is expected: {group_counts:?}"
    );
}

/// Scenario: Join and aggregate pushdown reach a Delta table by the same route
/// as a scan.
///
/// Every assertion below is self-consistent against a ground-truth full scan
/// fetched in-process, rather than a fixture value pinned in the test, since
/// only `basic_partitioned`'s partition values are pinned upstream (the
/// previous scenario and `unity_delta_planning_agrees_under_vended_and_static_credentials`).
///
/// - a grouped aggregate (`GROUP BY ID`) over `multi_part_stats`
/// - an `ORDER BY ... LIMIT` top-N over `multi_part_stats`
/// - a broadcast-eligible inner equi-join whose broadcast side is
///   `basic_partitioned`, the PARTITIONED table, joined against `cm_id_mode`
///   (NOT `multi_part_stats`: a live run's active-file byte totals are
///   `basic_partitioned` 4505, `multi_part_stats` 3804, `cm_id_mode` 5253 —
///   `select_broadcast_sides` gives the broadcast/dimension role to the
///   SMALLER side, so pairing with `multi_part_stats` would make
///   `basic_partitioned` the FACT side instead, never exercising
///   `JoinSpec.partition_columns`. `cm_id_mode` is already seeded and larger
///   than `basic_partitioned`, so it reliably keeps `basic_partitioned` on the
///   broadcast side.)
#[test]
fn unity_delta_join_and_aggregate_pushdown_return_correct_rows() {
    setup();
    let mut conn = exa_conn();

    let raw_ids: Vec<i64> = conn
        .query_columns(&format!("SELECT ID FROM {}", table_ref("MULTI_PART_STATS")))[0]
        .iter()
        .map(parse_int)
        .collect();
    assert_eq!(
        raw_ids.len(),
        5,
        "multi_part_stats holds 5 rows: {raw_ids:?}"
    );

    // Grouped aggregate: GROUP BY ID must match a hand-computed grouping of the
    // same ground-truth ids.
    let grouped = conn.query_columns(&format!(
        "SELECT ID, COUNT(*) FROM {} GROUP BY ID",
        table_ref("MULTI_PART_STATS")
    ));
    let mut expected_group_counts: std::collections::HashMap<i64, i64> =
        std::collections::HashMap::new();
    for id in &raw_ids {
        *expected_group_counts.entry(*id).or_insert(0) += 1;
    }
    for (id, count) in grouped[0].iter().zip(grouped[1].iter()) {
        let key = parse_int(id);
        let actual_count = parse_int(count);
        let expected_count = expected_group_counts
            .remove(&key)
            .unwrap_or_else(|| panic!("unexpected group key {key}: {grouped:?}"));
        assert_eq!(
            actual_count, expected_count,
            "group {key}: COUNT(*) mismatch"
        );
    }
    assert!(
        expected_group_counts.is_empty(),
        "GROUP BY ID must cover every id the ground-truth scan saw: missing {expected_group_counts:?}"
    );

    // ORDER BY ... LIMIT: top-3 by ID DESC must match a full sort + truncate of
    // the same ground-truth ids.
    let mut expected_top3 = raw_ids.clone();
    expected_top3.sort_unstable_by(|a, b| b.cmp(a));
    expected_top3.truncate(3);
    let top3: Vec<i64> = conn.query_columns(&format!(
        "SELECT ID FROM {} ORDER BY ID DESC LIMIT 3",
        table_ref("MULTI_PART_STATS")
    ))[0]
        .iter()
        .map(parse_int)
        .collect();
    assert_eq!(
        top3, expected_top3,
        "ORDER BY ID DESC LIMIT 3 must match a full sort + truncate"
    );

    // Broadcast join: basic_partitioned (partitioned, the smaller side) joined
    // to cm_id_mode on NUMBER = ID.
    let join_sql = format!(
        "SELECT p.LETTER, c.ID FROM {} p JOIN {} c ON p.NUMBER = c.ID",
        table_ref("BASIC_PARTITIONED"),
        table_ref("CM_ID_MODE")
    );
    let pushed = explain_virtual_sql(&mut conn, &join_sql);
    assert!(
        has_broadcast_join_block(&pushed),
        "the join must drive one broadcast scan UDF, not the two-scan fallback: {pushed}"
    );
    assert!(
        !has_two_scan_wrapper(&pushed),
        "the join must not fall back to the two-scan Exasol-joined shape: {pushed}"
    );
    let common_table_root_idx = pushed
        .find("\"table_root\":\"")
        .unwrap_or_else(|| panic!("expected the surrounding common spec's table_root: {pushed}"));
    let common_table_root_start = common_table_root_idx + "\"table_root\":\"".len();
    let common_table_root_end = pushed[common_table_root_start..]
        .find('"')
        .map(|rel| common_table_root_start + rel)
        .unwrap_or_else(|| panic!("unterminated table_root string: {pushed}"));
    let common_table_root = &pushed[common_table_root_start..common_table_root_end];
    assert!(
        common_table_root.contains("cdf-column-mapping-id-mode"),
        "the sharded (fact) side must be cm_id_mode, the larger of the two: {pushed}"
    );

    let join_key_idx = pushed
        .find("\"join\":{")
        .unwrap_or_else(|| panic!("expected a join block in the pushed SQL: {pushed}"));
    let join_value_start = join_key_idx + "\"join\":".len();
    let (join_value, _) = json_value_at(&pushed, join_value_start);
    let join_table_root = join_value["table_root"]
        .as_str()
        .unwrap_or_else(|| panic!("join block must carry a table_root: {join_value}"));
    assert!(
        join_table_root.contains("basic_partitioned"),
        "the broadcast (dimension) side's join block must carry \
         basic_partitioned's table root, the PARTITIONED table: {pushed}"
    );

    // Ground-truth join, computed in-process from both tables' full contents.
    let cm_ids: Vec<i64> = conn
        .query_columns(&format!("SELECT ID FROM {}", table_ref("CM_ID_MODE")))[0]
        .iter()
        .map(parse_int)
        .collect();
    let distinct_cm_ids: std::collections::HashSet<i64> = cm_ids.iter().copied().collect();
    assert_eq!(
        distinct_cm_ids.len(),
        cm_ids.len(),
        "cm_id_mode's ID values must be distinct for the semi-join ground truth \
         (membership via cm_ids.contains) to equal a real join: {cm_ids:?}"
    );
    let base_cols = conn.query_columns(&format!(
        "SELECT LETTER, \"NUMBER\" FROM {}",
        table_ref("BASIC_PARTITIONED")
    ));
    let mut expected_join: Vec<(Option<String>, i64)> = base_cols[0]
        .iter()
        .zip(base_cols[1].iter())
        .map(|(letter, number)| (letter.as_str().map(str::to_string), parse_int(number)))
        .filter(|(_, number)| cm_ids.contains(number))
        .collect();
    expected_join.sort_by(|a, b| a.1.cmp(&b.1));

    let join_cols = conn.query_columns(&join_sql);
    let mut actual_join: Vec<(Option<String>, i64)> = join_cols[0]
        .iter()
        .zip(join_cols[1].iter())
        .map(|(letter, id)| (letter.as_str().map(str::to_string), parse_int(id)))
        .collect();
    actual_join.sort_by(|a, b| a.1.cmp(&b.1));

    assert!(
        !actual_join.is_empty(),
        "the join must return at least one matching row to prove more than an \
         empty-result coincidence: basic_partitioned NUMBER and cm_id_mode ID \
         must overlap"
    );
    assert_eq!(
        actual_join, expected_join,
        "the broadcast join result must match an in-process join over the two \
         tables' full contents, carrying basic_partitioned's LETTER value"
    );
}

/// Scenario: A Delta table using an unsupported reader feature fails the query
/// loud.
///
/// `type_widening` declares `typeWidening-preview` (tracked as issue #349) and
/// `unshredded_variant` declares `variantType-preview`; `DeltaSnapshot::open`
/// refuses both at plan time, before any log replay. The refusal must be the
/// protocol gate's own message — never something that looks like the per-column
/// type-mapping refusal (which names a column and cites #350) — and the session
/// must survive to prove no crashed UDF VM took it down.
#[test]
fn unity_delta_unsupported_reader_feature_fails_the_query_loud() {
    setup();
    let mut conn = exa_conn();

    for (table, feature, cites_349) in [
        ("TYPE_WIDENING", "typeWidening-preview", true),
        ("UNSHREDDED_VARIANT", "variantType-preview", false),
    ] {
        let resp = conn.try_execute(&format!("SELECT * FROM {} LIMIT 1", table_ref(table)));
        assert_eq!(
            resp["status"].as_str(),
            Some("error"),
            "{table} must fail the query loud, not return a row: {resp}"
        );
        let msg = resp["exception"]["text"].as_str().unwrap_or("");
        assert!(
            msg.contains(feature),
            "{table}'s error must name its actual unsupported reader feature: {msg}"
        );
        assert_eq!(
            msg.contains("#349"),
            cites_349,
            "{table}'s error must cite issue #349 only for typeWidening: {msg}"
        );
        assert!(
            !msg.contains("#350") && !msg.to_lowercase().contains("does not map at plan time"),
            "{table}'s error must be the protocol-gate refusal, not a column-typed \
             type-mapping error: {msg}"
        );
        assert!(
            !msg.to_lowercase().contains("minioadmin"),
            "{table}'s error text must not contain a credential value: {msg}"
        );
    }

    let survives = conn.query_scalar_i64("SELECT 1 FROM DUAL");
    assert_eq!(
        survives, 1,
        "the connection must survive both refusals: a crashed UDF VM would take \
         the session down, not return a clean SQL error"
    );
}

/// The 13 Delta types this engine maps for `stats_all_types`, in fixture column
/// order. The other 3 declared columns (`binary_col`, `map_col`,
/// `nested_struct`) are refused per column — exercised by
/// `unity_delta_refused_column_refuses_only_the_queries_naming_it`.
const STATS_ALL_TYPES_MAPPABLE_COLUMNS: &str = "BYTE_COL, SHORT_COL, INT_COL, LONG_COL, FLOAT_COL, DOUBLE_COL, DATE_COL, \
     TIMESTAMP_COL, TIMESTAMP_NTZ_COL, STRING_COL, DECIMAL_COL, BOOLEAN_COL, ARRAY_COL";

/// Scenario: A Delta table spanning varied types returns the expected Exasol
/// types and values.
///
/// `stats_all_types` carries one active Parquet file of 4 rows across its 13
/// mappable columns. `BYTE_COL`/`SHORT_COL` are checked by their real logged
/// values (not merely "present") to prove `byte`/`short` are not silently
/// NULLed by a missing mapping; `ARRAY_COL`'s bracketed rendering matches the
/// scan-level expression-adapter cast proven in `raw_scan_tests`.
#[test]
fn unity_delta_varied_types_return_their_expected_exasol_types_and_values() {
    setup();
    let mut conn = exa_conn();
    let table = table_ref("STATS_ALL_TYPES");

    let count = conn.query_scalar_i64(&format!("SELECT COUNT(*) FROM {table}"));
    assert_eq!(
        count, 4,
        "stats_all_types must carry exactly 4 rows: {count}"
    );

    let cols = column_types(&mut conn, VS_NAME, "STATS_ALL_TYPES");
    for (column, expected) in [
        ("BYTE_COL", "DECIMAL(3,0)"),
        ("SHORT_COL", "DECIMAL(5,0)"),
        ("INT_COL", "DECIMAL(10,0)"),
        ("LONG_COL", "DECIMAL(20,0)"),
        ("FLOAT_COL", "DOUBLE"),
        ("DOUBLE_COL", "DOUBLE"),
        ("DATE_COL", "DATE"),
        ("TIMESTAMP_COL", "TIMESTAMP"),
        ("TIMESTAMP_NTZ_COL", "TIMESTAMP"),
        ("STRING_COL", "VARCHAR(2000000)"),
        ("DECIMAL_COL", "DECIMAL(10,2)"),
        ("BOOLEAN_COL", "BOOLEAN"),
        ("ARRAY_COL", "VARCHAR(2000000)"),
    ] {
        assert_col_type(&cols, column, expected);
    }

    let select_sql = format!("SELECT {STATS_ALL_TYPES_MAPPABLE_COLUMNS} FROM {table}");
    let pushed = explain_virtual_sql(&mut conn, &select_sql);
    assert!(
        pushed.contains(SCAN_SCRIPT_NAME),
        "the mappable-column projection must drive the scan UDF, not an \
         unaccelerated fallback: {pushed}"
    );

    let rows = conn.query_columns(&select_sql);
    assert_eq!(rows.len(), 13, "expected 13 projected columns: {rows:?}");
    assert_eq!(rows[0].len(), 4, "expected 4 rows: {rows:?}");

    let byte_non_null: Vec<i64> = rows[0]
        .iter()
        .filter(|v| !v.is_null())
        .map(parse_int)
        .collect();
    assert_eq!(
        byte_non_null.len(),
        3,
        "BYTE_COL must carry 3 real logged values and 1 NULL, not be silently \
         NULLed by a missing mapping: {:?}",
        rows[0]
    );

    let short_non_null: Vec<i64> = rows[1]
        .iter()
        .filter(|v| !v.is_null())
        .map(parse_int)
        .collect();
    assert_eq!(
        short_non_null.len(),
        3,
        "SHORT_COL must carry 3 real logged values and 1 NULL, not be silently \
         NULLed by a missing mapping: {:?}",
        rows[1]
    );

    let array_col = &rows[12];
    let mut rendered: Vec<Option<String>> = array_col
        .iter()
        .map(|v| v.as_str().map(str::to_string))
        .collect();
    rendered.sort();
    assert_eq!(
        rendered,
        vec![
            None,
            Some("[1, 2, 3]".to_string()),
            Some("[4, 5]".to_string()),
            Some("[6]".to_string()),
        ],
        "ARRAY_COL must render each populated array bracketed and keep a NULL \
         array NULL, matching the scan's own field-id expression adapter cast: \
         {array_col:?}"
    );
}

/// Scenario: A Delta column this engine cannot render refuses only the queries
/// that name it.
///
/// `stats_all_types` carries `binary_col`, `map_col`, and `nested_struct` —
/// each refused per column (issue #350), not table-wide. A projection naming
/// one individually, a `SELECT *` (which widens to the full base row), and a
/// WHERE clause referencing one all refuse; the 13-column mappable projection
/// from `unity_delta_varied_types_return_their_expected_exasol_types_and_values`
/// still succeeds afterward on the SAME connection, proving the refusal is
/// per-request rather than session-poisoning.
#[test]
fn unity_delta_refused_column_refuses_only_the_queries_naming_it() {
    setup();
    let mut conn = exa_conn();
    let table = table_ref("STATS_ALL_TYPES");

    for (column, delta_name) in [
        ("BINARY_COL", "binary_col"),
        ("MAP_COL", "map_col"),
        ("NESTED_STRUCT", "nested_struct"),
    ] {
        let resp = conn.try_execute(&format!("SELECT {column} FROM {table}"));
        assert_eq!(
            resp["status"].as_str(),
            Some("error"),
            "{column} must refuse the query, not return a row: {resp}"
        );
        let msg = resp["exception"]["text"].as_str().unwrap_or("");
        assert!(
            msg.contains(delta_name) && msg.contains("#350"),
            "{column}'s refusal must name its Delta column and cite issue #350: {msg}"
        );
    }

    let star_resp = conn.try_execute(&format!("SELECT * FROM {table}"));
    assert_eq!(
        star_resp["status"].as_str(),
        Some("error"),
        "SELECT * widens to the full base row, so a refused column anywhere in \
         the table must refuse it too: {star_resp}"
    );

    let where_resp = conn.try_execute(&format!(
        "SELECT INT_COL FROM {table} WHERE BINARY_COL IS NOT NULL"
    ));
    assert_eq!(
        where_resp["status"].as_str(),
        Some("error"),
        "a WHERE clause referencing a refused column must refuse it even though \
         the select list names only a mappable column: {where_resp}"
    );

    let mappable = conn.query_columns(&format!(
        "SELECT {STATS_ALL_TYPES_MAPPABLE_COLUMNS} FROM {table}"
    ));
    assert_eq!(
        mappable.len(),
        13,
        "the mappable 13-column projection must still succeed on the same \
         connection, proving the refusals above were per-request: {mappable:?}"
    );
    assert_eq!(
        mappable[0].len(),
        4,
        "expected 4 rows from the mappable projection: {mappable:?}"
    );
}
