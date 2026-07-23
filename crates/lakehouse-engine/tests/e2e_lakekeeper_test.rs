//! End-to-end integration tests for the lakehouse-engine Virtual Schema against
//! a Lakekeeper Iceberg REST catalog — OpenID-secured via Keycloak, backed by
//! the base stack's MinIO.
//!
//! These tests run against the overlay stack (Exasol + MinIO + Keycloak +
//! Lakekeeper). They FAIL (never skip) when the stack is unavailable — per
//! project rules, the same contract as the baseline `exasol-e2e` suite.
//!
//! All tests share one Exasol (two virtual schemas), so they must run serially
//! (`--test-threads=1`); the `make test-e2e-lakekeeper` target passes the flag.
//!
//! # What this suite proves
//!
//! Two DISTINCT OAuth2 client-credentials implementations reach Keycloak on the
//! green path, and each is verified independently:
//!   * `iceberg-catalog-rest`'s own built-in OAuth2 client — used by the
//!     createVirtualSchema enumeration path (`lakekeeper_create_virtual_schema_lists_tables_over_oidc`).
//!   * the adapter's own `oauth2_client_credentials_grant` — used by the
//!     scan/file-resolution path (`lakekeeper_static_creds_projection_filter_limit`,
//!     `lakekeeper_vended_creds_projection_filter`).
//!
//! Both static-credential and vended (STS/AssumeRole against MinIO) S3 read modes
//! are exercised as hard pass/fail requirements.
//!
//! # Setup (done once via `setup` called from each stack-dependent test)
//! 1. Wait for Exasol, MinIO, Keycloak, and Lakekeeper.
//! 2. Bootstrap Lakekeeper and create the static- and vended-credential warehouses.
//! 3. Seed the `events` table (identical 20-row shape as the baseline) into BOTH
//!    warehouses through the OIDC-secured catalog.
//! 4. Provision the SLC / `.so` / scripts via the SHARED harness (not redeclared).
//! 5. Create one Virtual Schema per warehouse.
//!
//! # Credential safety
//! No credential value (Keycloak client secret, bearer token, S3 access/secret
//! keys) is ever printed by these tests; `lakekeeper_credentials_never_appear_in_output`
//! pins the redaction contract on the failure path.
#![cfg(feature = "lakekeeper-e2e")]

mod common;

use common::e2e_harness::{
    ADAPTER_SCRIPT_NAME, SCAN_SCRIPT_NAME, SCHEMA_NAME, SYS_PASSWORD, VsProps,
    create_schema_and_scripts, create_virtual_schema_with_password, exa_conn, install_slc,
    parse_int, upload_so,
};
use common::exasol_ws::ExaConn;
use common::lakekeeper::{
    self, WAREHOUSE_STATIC, WAREHOUSE_VENDED, WarehouseProfile, lakekeeper_connection_password,
};
use common::seed::{
    E2E_NAMESPACE, E2E_TABLE, SEED_ROWS_SCORE_GT_15, SEED_TOTAL_ROWS, SeedCatalogAuth,
    seed_events_table_with_auth,
};
use common::stack::{
    CatalogConnectionPassword, build_create_connection_sql, exasol_host, exasol_sql_port,
    wait_for_exasol, wait_for_minio, wait_for_url,
};

use std::sync::OnceLock;
use std::time::Duration;

// ---------------------------------------------------------------------------
// Constants — the two warehouses each get their own CONNECTION + Virtual Schema.
// ---------------------------------------------------------------------------

/// Virtual Schema over the static-credential (delegation-off) warehouse.
const VS_STATIC: &str = "LK_STATIC_LAKEHOUSE";
/// Virtual Schema over the vended-credential (STS) warehouse.
const VS_VENDED: &str = "LK_VENDED_LAKEHOUSE";
/// Catalog CONNECTION for the static-credential warehouse.
const CONN_STATIC: &str = "LK_STATIC_CATALOG_CREDS";
/// Catalog CONNECTION for the vended-credential warehouse.
const CONN_VENDED: &str = "LK_VENDED_CATALOG_CREDS";

/// Lakekeeper catalog base URL as reached from inside the Exasol UDF container —
/// the Docker-network name plus the `/catalog` base-path segment. This is the
/// CONNECTION address the adapter's OAuth2 catalog path resolves the table under;
/// preserving the `/catalog` base path through `build_load_table_url` is exactly
/// what `lakekeeper_oauth_prefix_under_base_path_resolves` verifies.
const LAKEKEEPER_CATALOG_URI_INTERNAL: &str = "http://lakekeeper:8181/catalog";

/// Lakekeeper catalog base URL as reached from the host (mapped port), used only
/// for host-side seeding. Same `/catalog` base path as the UDF-internal URL.
fn lakekeeper_catalog_url_host() -> String {
    format!("http://localhost:{}/catalog", lakekeeper::lakekeeper_port())
}

fn vs_static_table() -> String {
    format!("{VS_STATIC}.{}", E2E_TABLE.to_uppercase())
}

fn vs_vended_table() -> String {
    format!("{VS_VENDED}.{}", E2E_TABLE.to_uppercase())
}

// ---------------------------------------------------------------------------
// One-time setup (shared across the serial binary).
// ---------------------------------------------------------------------------

static SETUP_DONE: OnceLock<()> = OnceLock::new();

fn setup() {
    SETUP_DONE.get_or_init(|| {
        // 1. Readiness — fail loud, never skip.
        wait_for_exasol();
        wait_for_minio();
        lakekeeper::wait_for_keycloak();
        lakekeeper::wait_for_lakekeeper();

        // 2. Bootstrap the server and create both warehouses (idempotent).
        lakekeeper::lakekeeper_bootstrap();
        lakekeeper::lakekeeper_create_warehouse(&WarehouseProfile::static_creds());
        lakekeeper::lakekeeper_create_warehouse(&WarehouseProfile::vended());

        // 3. Seed the events table into BOTH warehouses through the OIDC-secured
        //    catalog. Per the cross-module note, seeding authenticates with a
        //    host-side Keycloak bearer token (static-token catalog auth) rather
        //    than threading OAuth2 client-credentials through the seeder. A fresh
        //    token is fetched per warehouse so a short token lifetime cannot make
        //    a multi-call seed go stale mid-write.
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("tokio runtime");
        let host_catalog = lakekeeper_catalog_url_host();
        for warehouse in [WAREHOUSE_STATIC, WAREHOUSE_VENDED] {
            let token = lakekeeper::keycloak_client_credentials_token();
            let auth = SeedCatalogAuth { token: Some(token) };
            rt.block_on(async {
                seed_events_table_with_auth(&host_catalog, warehouse, auth)
                    .await
                    .unwrap_or_else(|e| {
                        panic!("seed events into Lakekeeper warehouse '{warehouse}': {e:#}")
                    });
            });
        }

        // 4. Shared-harness provisioning (SLC + .so + scripts) — REUSED, never
        //    redeclared. `lakekeeper_binary_uses_shared_harness_provisioning`
        //    asserts these shared scripts are what backs the scan path.
        install_slc();
        upload_so();
        let mut conn = exa_conn();
        create_schema_and_scripts(&mut conn);

        // 5. One Virtual Schema per warehouse via the shared password-parameterized
        //    helper (task 3.3), each carrying the Lakekeeper CONNECTION password and
        //    the `/catalog` base-path catalog URI.
        let static_pw = lakekeeper_connection_password(WAREHOUSE_STATIC, false);
        create_virtual_schema_with_password(
            &mut conn,
            &VsProps::new(VS_STATIC, E2E_NAMESPACE).with_catalog_conn_name(CONN_STATIC),
            LAKEKEEPER_CATALOG_URI_INTERNAL,
            &static_pw,
        );

        let vended_pw = lakekeeper_connection_password(WAREHOUSE_VENDED, true);
        create_virtual_schema_with_password(
            &mut conn,
            &VsProps::new(VS_VENDED, E2E_NAMESPACE).with_catalog_conn_name(CONN_VENDED),
            LAKEKEEPER_CATALOG_URI_INTERNAL,
            &vended_pw,
        );
    });
}

// ---------------------------------------------------------------------------
// Result helpers.
// ---------------------------------------------------------------------------

/// Column-major table names enumerated for a virtual schema, uppercased.
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

/// The `(id, name, score)` rows of `table`, ordered by id, as comparable tuples.
fn projection_rows(conn: &mut ExaConn, table: &str) -> Vec<(i64, String, f64)> {
    let cols = conn.query_columns(&format!("SELECT id, name, score FROM {table} ORDER BY id"));
    assert_eq!(
        cols.len(),
        3,
        "expected 3 columns (id, name, score): {cols:?}"
    );
    let n = cols[0].len();
    (0..n)
        .map(|i| {
            let id = parse_int(&cols[0][i]);
            let name = cols[1][i]
                .as_str()
                .unwrap_or_else(|| panic!("name not string: {:?}", cols[1][i]))
                .to_string();
            let score = cols[2][i]
                .as_f64()
                .or_else(|| cols[2][i].as_str().and_then(|s| s.parse().ok()))
                .unwrap_or_else(|| panic!("score not numeric: {:?}", cols[2][i]));
            (id, name, score)
        })
        .collect()
}

// ---------------------------------------------------------------------------
// 5.1 — harness bootstraps Lakekeeper and provisions both warehouses.
// ---------------------------------------------------------------------------

/// Setup bootstraps Lakekeeper and creates both MinIO-backed warehouses; a
/// Virtual Schema is created over each. Both VS existing in the catalog is the
/// downstream proof that bootstrap + warehouse creation + seeding all succeeded
/// (a VS cannot be created over a non-existent, unseeded warehouse).
#[test]
fn lakekeeper_bootstrap_and_warehouses_provision() {
    setup();
    let mut conn = exa_conn();

    for vs in [VS_STATIC, VS_VENDED] {
        let cols = conn.query_columns(&format!(
            "SELECT SCHEMA_NAME FROM SYS.EXA_ALL_VIRTUAL_SCHEMAS WHERE SCHEMA_NAME = '{vs}'"
        ));
        assert_eq!(
            cols.first().map(|c| c.len()).unwrap_or(0),
            1,
            "virtual schema {vs} must exist — its warehouse must have been bootstrapped, \
             created, and seeded"
        );
    }
}

// ---------------------------------------------------------------------------
// 5.2 — createVirtualSchema enumeration over the built-in OAuth2 client.
// ---------------------------------------------------------------------------

/// createVirtualSchema enumerates the seeded table over OAuth2 client-credentials
/// auth. Enumeration runs `iceberg-catalog-rest`'s OWN built-in OAuth2 client
/// against Keycloak (independent of the adapter's scan-time grant); the seeded
/// `EVENTS` table appearing in the schema proves that client authenticated.
#[test]
fn lakekeeper_create_virtual_schema_lists_tables_over_oidc() {
    setup();
    let mut conn = exa_conn();

    let tables = enumerated_table_names(&mut conn, VS_STATIC);
    assert!(
        tables.iter().any(|t| t == &E2E_TABLE.to_uppercase()),
        "createVirtualSchema must enumerate the seeded '{}' table over OIDC \
         (built-in OAuth2 client authenticated against Keycloak); got: {tables:?}",
        E2E_TABLE
    );
}

// ---------------------------------------------------------------------------
// 5.3 — static-credential projection + filter + LIMIT correctness.
// ---------------------------------------------------------------------------

/// End-to-end projection + filter + LIMIT over the static-credential warehouse
/// returns the correct rows. The scan exercises the adapter's OWN
/// `oauth2_client_credentials_grant` (a separate OAuth2 implementation from 5.2's
/// built-in enumeration client), then reads MinIO with the warehouse's static S3
/// credentials.
#[test]
fn lakekeeper_static_creds_projection_filter_limit() {
    setup();
    let mut conn = exa_conn();

    // Seeded shape (identical to the baseline): id 1..20, score = 5.0 * id.
    // score > 15.0 → id >= 4; LIMIT 5 → ids 4,5,6,7,8.
    let cols = conn.query_columns(&format!(
        "SELECT id, name, score FROM {} WHERE score > 15.0 LIMIT 5",
        vs_static_table()
    ));
    assert_eq!(
        cols.len(),
        3,
        "expected 3 columns (id, name, score): {cols:?}"
    );
    assert_eq!(
        cols[0].len(),
        5,
        "LIMIT 5 must return exactly 5 rows: {cols:?}"
    );

    for score in &cols[2] {
        let s = score
            .as_f64()
            .or_else(|| score.as_str().and_then(|v| v.parse().ok()))
            .unwrap_or_else(|| panic!("score not numeric: {score:?}"));
        assert!(s > 15.0, "filter violated: score {s} <= 15.0");
    }
    let ids: Vec<i64> = cols[0].iter().map(parse_int).collect();
    assert!(
        ids.iter().all(|&id| id >= 4),
        "id < 4 appeared (score would be <= 15): {ids:?}"
    );

    // Whole-table filter + total counts pin the known seed shape.
    let filtered = conn.query_row_count(&format!(
        "SELECT id FROM {} WHERE score > 15.0",
        vs_static_table()
    ));
    assert_eq!(
        filtered, SEED_ROWS_SCORE_GT_15 as i64,
        "WHERE score > 15.0 must return {SEED_ROWS_SCORE_GT_15} rows, got {filtered}"
    );
    let total = conn.query_row_count(&format!("SELECT id FROM {}", vs_static_table()));
    assert_eq!(
        total, SEED_TOTAL_ROWS as i64,
        "the static warehouse must hold {SEED_TOTAL_ROWS} seeded rows, got {total}"
    );
}

// ---------------------------------------------------------------------------
// 5.4 — vended-credential (STS/AssumeRole) projection + filter correctness. [expert]
// ---------------------------------------------------------------------------

/// End-to-end projection + filter over the VENDED-credential warehouse returns
/// rows identical to the static warehouse's, AND the access-delegation path is
/// proven to have executed.
///
/// The delegation proof is deterministic, not incidental: the vended CONNECTION
/// password carries NO static S3 credentials (empty access key, secret key, and
/// endpoint — verified below). The UDF therefore has no static way to reach
/// MinIO; the only path by which the scan can read the warehouse's data files is
/// the `X-Iceberg-Access-Delegation: vended-credentials` `loadTable` request that
/// vends short-lived MinIO STS credentials. A correct row set from an empty-static
/// connection is only reachable through that delegation path — mirroring the proof
/// standard the Glue vended-credential test established, applied against MinIO/STS.
#[test]
fn lakekeeper_vended_creds_projection_filter() {
    setup();
    let mut conn = exa_conn();

    // Deterministic delegation proof: the vended connection has no static S3 creds.
    let vended_pw = lakekeeper_connection_password(WAREHOUSE_VENDED, true);
    assert!(
        vended_pw.use_vended_credentials,
        "vended warehouse CONNECTION must request access delegation"
    );
    assert!(
        vended_pw.access_key.is_empty()
            && vended_pw.secret_key.is_empty()
            && vended_pw.endpoint.is_empty(),
        "vended CONNECTION must carry NO static S3 credentials, so a successful \
         scan is only reachable via the vended-credentials delegation path"
    );

    // Same query shape as the static warehouse — results must be identical.
    let cols = conn.query_columns(&format!(
        "SELECT id, name, score FROM {} WHERE score > 15.0 LIMIT 5",
        vs_vended_table()
    ));
    assert_eq!(
        cols.len(),
        3,
        "expected 3 columns (id, name, score): {cols:?}"
    );
    assert_eq!(
        cols[0].len(),
        5,
        "LIMIT 5 must return exactly 5 rows: {cols:?}"
    );
    for score in &cols[2] {
        let s = score
            .as_f64()
            .or_else(|| score.as_str().and_then(|v| v.parse().ok()))
            .unwrap_or_else(|| panic!("score not numeric: {score:?}"));
        assert!(
            s > 15.0,
            "filter violated over vended creds: score {s} <= 15.0"
        );
    }

    // The full ordered row set read via vended creds must equal the static one:
    // the two warehouses hold identical seed data, so vending must not corrupt or
    // truncate the result.
    let static_rows = projection_rows(&mut conn, &vs_static_table());
    let vended_rows = projection_rows(&mut conn, &vs_vended_table());
    assert_eq!(
        vended_rows, static_rows,
        "the vended-credential scan must return exactly the same rows as the \
         static-credential scan"
    );
    assert_eq!(
        vended_rows.len(),
        SEED_TOTAL_ROWS,
        "the vended warehouse must hold {SEED_TOTAL_ROWS} seeded rows"
    );
}

// ---------------------------------------------------------------------------
// 5.5 — fail-not-skip when the stack is down.
// ---------------------------------------------------------------------------

/// The Lakekeeper readiness contract is fail-loud: a readiness wait against an
/// unreachable stack PANICS (never returns cleanly), so a down stack surfaces as
/// a test failure, never a silent skip. This exercises the very `wait_for_url`
/// helper the Lakekeeper readiness waits (`wait_for_keycloak` / `wait_for_lakekeeper`)
/// are built on, pointed at a closed local port with a short deadline.
#[test]
fn lakekeeper_suite_fails_when_stack_unavailable() {
    let result = std::panic::catch_unwind(|| {
        // 127.0.0.1:1 refuses immediately; the poll loop hits the short deadline
        // and panics rather than returning — the fail-not-skip contract.
        wait_for_url("http://127.0.0.1:1/health", Duration::from_secs(2));
    });
    assert!(
        result.is_err(),
        "a readiness wait against an unreachable Lakekeeper stack must panic (fail), \
         never return Ok (skip)"
    );
}

// ---------------------------------------------------------------------------
// 5.6a — the scan path is provisioned from the shared harness definition.
// ---------------------------------------------------------------------------

/// The Lakekeeper binary provisions its scan path from the SHARED harness
/// definition (`create_schema_and_scripts` in `common/e2e_harness`), not a
/// duplicated local one. Both the adapter and scan scripts exist under the shared
/// schema and reference the shared `.so`, and both virtual schemas are created
/// `USING` that shared adapter script — proving reuse rather than duplication.
#[test]
fn lakekeeper_binary_uses_shared_harness_provisioning() {
    setup();
    let mut conn = exa_conn();

    for script in [ADAPTER_SCRIPT_NAME, SCAN_SCRIPT_NAME] {
        let resp = conn.execute(&format!(
            "SELECT SCRIPT_TEXT FROM EXA_ALL_SCRIPTS \
             WHERE SCRIPT_NAME = '{script}' AND SCRIPT_SCHEMA = '{SCHEMA_NAME}'"
        ));
        let body = resp["responseData"]["results"][0]["resultSet"]["data"][0][0]
            .as_str()
            .unwrap_or("")
            .to_string();
        assert!(
            body.contains("liblakehouse_engine.so") || body.contains("udf"),
            "shared script {SCHEMA_NAME}.{script} must reference the shared .so: {body}"
        );
    }

    // Both VS use the shared adapter script from the shared schema. Exasol 8
    // (`SYS.EXA_ALL_VIRTUAL_SCHEMAS`) exposes the adapter as the split
    // `ADAPTER_SCRIPT_SCHEMA`/`ADAPTER_SCRIPT_NAME` columns (there is no combined
    // `ADAPTER_SCRIPT` column), so reconstruct the qualified name the assertion
    // below checks against.
    let cols = conn.query_columns(&format!(
        "SELECT ADAPTER_SCRIPT_SCHEMA || '.' || ADAPTER_SCRIPT_NAME \
         FROM SYS.EXA_ALL_VIRTUAL_SCHEMAS \
         WHERE SCHEMA_NAME IN ('{VS_STATIC}', '{VS_VENDED}')"
    ));
    let adapters: Vec<String> = cols
        .first()
        .map(|c| {
            c.iter()
                .filter_map(|v| v.as_str().map(|s| s.to_uppercase()))
                .collect()
        })
        .unwrap_or_default();
    assert_eq!(
        adapters.len(),
        2,
        "both Lakekeeper virtual schemas must be present: {adapters:?}"
    );
    for adapter in &adapters {
        assert!(
            adapter.contains(&SCHEMA_NAME.to_uppercase())
                && adapter.contains(&ADAPTER_SCRIPT_NAME.to_uppercase()),
            "VS must be created USING the shared adapter script \
             {SCHEMA_NAME}.{ADAPTER_SCRIPT_NAME}, got: {adapter}"
        );
    }
}

// ---------------------------------------------------------------------------
// 5.6b — OAuth2 path resolves tables under the `/catalog` base path.
// ---------------------------------------------------------------------------

/// The OAuth2 client-credentials path resolves tables from Lakekeeper's
/// multi-warehouse catalog served under the `/catalog` base path. The CONNECTION
/// address carries `/catalog`; a projection query returning the correct rows
/// proves that BOTH `resolve_load_table_prefix`'s `GET /v1/config?warehouse=`
/// negotiation and `build_load_table_url`'s base-path preservation worked against
/// a real Lakekeeper — a malformed URL (dropped base path or mishandled
/// per-warehouse prefix) would have failed the `loadTable` fetch and returned no
/// rows.
#[test]
fn lakekeeper_oauth_prefix_under_base_path_resolves() {
    setup();
    let mut conn = exa_conn();

    // The live CONNECTION address must carry the `/catalog` base-path segment.
    // Exasol 8 exposes `CONNECTION_STRING` only via the DBA view
    // (`SYS.EXA_DBA_CONNECTIONS`); `SYS.EXA_ALL_CONNECTIONS` no longer carries it.
    // The suite connects as `sys` (a DBA), so the DBA view is readable.
    let cols = conn.query_columns(&format!(
        "SELECT CONNECTION_STRING FROM SYS.EXA_DBA_CONNECTIONS WHERE CONNECTION_NAME = '{CONN_STATIC}'"
    ));
    let address = cols
        .first()
        .and_then(|c| c.first())
        .and_then(|v| v.as_str())
        .unwrap_or_else(|| panic!("CONNECTION {CONN_STATIC} must exist with an address"));
    assert!(
        address.contains("/catalog"),
        "the catalog CONNECTION address must carry the `/catalog` base path, got: {address}"
    );

    // Resolving + scanning under that base path returns the seeded rows.
    let cols = conn.query_columns(&format!(
        "SELECT id, name FROM {} WHERE id = 7",
        vs_static_table()
    ));
    assert_eq!(cols.len(), 2, "expected 2 columns (id, name): {cols:?}");
    assert_eq!(
        cols[0].len(),
        1,
        "resolving under the `/catalog` base path must return the single id=7 row: {cols:?}"
    );
    assert_eq!(parse_int(&cols[0][0]), 7, "resolved row must be id=7");
}

// ---------------------------------------------------------------------------
// 5.6c — no credential value ever appears in captured output / panic messages.
// ---------------------------------------------------------------------------

/// A failing, credential-bearing Lakekeeper CONNECTION DDL executed through a
/// redacting `ExaConn` must not surface the SQL text or any credential value
/// (Keycloak client secret, S3 access/secret keys) in the failure output.
///
/// Mirrors the cloud suite's redaction negative test: obviously-fake sentinels
/// carry the credentials, an invalid trailing token forces the DDL-failure path,
/// and the captured panic message is asserted to contain none of the sentinels
/// nor the SQL text.
#[test]
fn lakekeeper_credentials_never_appear_in_output() {
    const SENTINEL_CLIENT_SECRET: &str = "LK_DUMMY_CLIENT_SECRET_SENTINEL";
    const SENTINEL_ACCESS_KEY: &str = "LK_DUMMY_ACCESS_KEY_SENTINEL";
    const SENTINEL_SECRET_KEY: &str = "LK_DUMMY_SECRET_KEY_SENTINEL";

    wait_for_exasol();
    let mut conn =
        ExaConn::connect_redacting(&exasol_host(), exasol_sql_port(), "sys", SYS_PASSWORD);

    let sentinel_password = CatalogConnectionPassword {
        warehouse: WAREHOUSE_STATIC.to_string(),
        access_key: SENTINEL_ACCESS_KEY.to_string(),
        secret_key: SENTINEL_SECRET_KEY.to_string(),
        path_style: true,
        client_id: Some("lakehouse".to_string()),
        client_secret: Some(SENTINEL_CLIENT_SECRET.to_string()),
        oauth2_server_uri: Some(
            "http://keycloak:8080/realms/iceberg/protocol/openid-connect/token".to_string(),
        ),
        ..Default::default()
    };
    let base_sql = build_create_connection_sql(
        "LK_REDACTION_PROBE",
        LAKEKEEPER_CATALOG_URI_INTERNAL,
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
    let panic_msg: String = if let Some(s) = payload.downcast_ref::<&str>() {
        (*s).to_string()
    } else if let Some(s) = payload.downcast_ref::<String>() {
        s.clone()
    } else {
        String::new()
    };

    assert!(
        !panic_msg.is_empty(),
        "expected a string panic payload from the failed redacting execute()"
    );
    assert!(
        !panic_msg.contains(&failing_sql),
        "redacting execute() failure must not echo the SQL text: {panic_msg}"
    );
    assert!(
        !panic_msg.contains(SENTINEL_CLIENT_SECRET)
            && !panic_msg.contains(SENTINEL_ACCESS_KEY)
            && !panic_msg.contains(SENTINEL_SECRET_KEY),
        "redacting execute() failure must not leak any credential value: {panic_msg}"
    );
}
