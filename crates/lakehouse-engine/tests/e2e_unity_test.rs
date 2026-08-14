//! End-to-end integration tests for the lakehouse-engine Virtual Schema against
//! a native Unity Catalog OSS server (the second catalog kind), backed by the
//! base stack's MinIO and seeded with the vendored Delta fixtures (#325 harness).
//!
//! These tests run against the overlay stack (Exasol + MinIO + Unity Catalog),
//! brought up by `make unity-up`. They FAIL (never skip) when the stack is
//! unavailable — the same contract as the baseline `exasol-e2e` suite. The suite
//! stops at createVirtualSchema for the pushdown path: #318 lists tables and
//! their column metadata and `handle_pushdown` runs no Delta scan (that lands in
//! #319/#320). `unity_delta_planning_agrees_under_vended_and_static_credentials`
//! exercises Delta table PLANNING directly through the `FormatReader` seam
//! (`format_reader`/`ScanSource::UnityDelta`), bypassing `handle_pushdown`
//! entirely — the only path this plan wires up.
//!
//! All tests share one Exasol (one virtual schema), so they must run serially
//! (`--test-threads=1`); the `make test-e2e-unity` target passes the flag.
//!
//! The CONNECTION address is the docker-network Unity Catalog host and its
//! password supplies no auth field, because the OSS server's authorization is
//! disabled. `unity_credentials_never_appear_in_output` pins the redaction
//! contract on the failure path.
#![cfg(feature = "unity-e2e")]

mod common;

use common::e2e_harness::{
    ADAPTER_SCRIPT_NAME, SCHEMA_NAME, SYS_PASSWORD, create_schema_and_scripts, exa_conn,
    install_slc, upload_so,
};
use common::exasol_ws::ExaConn;
use common::stack::{
    self, CatalogConnectionPassword, build_create_connection_sql, exasol_host, exasol_sql_port,
    wait_for_exasol, wait_for_minio, wait_for_url,
};

use lakehouse_catalog::{
    CatalogClient, CatalogTableIdent, ConnectionCreds, StorageBackend, UnityCatalogSession,
};
use lakehouse_engine::adapter::connection::storage_block;
use lakehouse_engine::adapter::pushdown::{
    ConnectionStorage, ResolvedScan, ScanSource, format_reader,
};
use lakehouse_engine::scan::spec::FileEntry;

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
/// shared adapter script. The CONNECTION carries the no-auth Unity address; the
/// `UNITY_CATALOG` catalog kind routes createVirtualSchema through the native
/// Unity Catalog client.
fn create_unity_virtual_schema(conn: &mut ExaConn) {
    // Default password: no warehouse, no auth field — the no-auth Unity mode.
    let password = CatalogConnectionPassword::default();
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
/// Panics unless EVERY entry carries a Delta block holding exactly one partition entry
/// keyed `letter`: partition values live only in the transaction log, so an empty map
/// (or an absent block) is a resolution that silently lost them, which comparing two
/// runs against each other cannot detect.
fn path_sorted_letter_values(files: &[FileEntry]) -> Vec<Option<String>> {
    let mut carried: Vec<(&str, Option<String>)> = files
        .iter()
        .map(|entry| {
            let delta = entry.delta.as_ref().unwrap_or_else(|| {
                panic!("every resolved Delta file must carry its per-file block: {entry:?}")
            });
            let mut partition_values = delta.partition_values.iter();
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
                delta.partition_values
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
/// must agree on the file list, the per-file partition values (carried inside
/// `FileEntry::delta`), and the table root: both read the SAME transaction log
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
    let deletion_vector = dv_scan.files[0]
        .delta
        .as_ref()
        .and_then(|delta| delta.deletion_vector.as_ref());
    assert!(
        deletion_vector.is_some(),
        "table_with_dv's single active file must carry a deletion-vector reference: {:?}",
        dv_scan.files[0]
    );
}
