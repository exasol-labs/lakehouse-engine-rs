//! E2E tests against a Lakekeeper Iceberg REST catalog, OpenID-secured via
//! Keycloak and backed by the base stack's SeaweedFS.
//!
//! Tests share one Exasol with two virtual schemas, so they must run serially
//! (`--test-threads=1`). They FAIL (never skip) when the stack is unavailable.
//!
//! Two distinct OAuth2 client-credentials implementations are verified:
//! `iceberg-catalog-rest`'s built-in client (createVirtualSchema enumeration) and
//! the adapter's own `oauth2_client_credentials_grant` (scan/file resolution).
#![cfg(feature = "lakekeeper-e2e")]

mod common;

use common::e2e_harness::{
    ADAPTER_SCRIPT_NAME, SCAN_SCRIPT_NAME, SCHEMA_NAME, SYS_PASSWORD, VsProps,
    assert_decimal_orders_numerically, assert_text_columns, create_schema_and_scripts,
    create_virtual_schema_with_password, declared_type, exa_conn, expected_join_rows,
    explain_virtual_sql, fetch_join_rows, has_broadcast_join_block, has_two_scan_wrapper,
    install_slc, join_query, parse_int, upload_so,
};
use common::exasol_ws::ExaConn;
use common::lakekeeper::{
    self, WAREHOUSE_AUTHZ, WAREHOUSE_STATIC, WAREHOUSE_VENDED, WarehouseProfile,
    lakekeeper_connection_password,
};
use common::lakekeeper_authz::{
    AuthzFixture, CHECK_ID, CHECKER, Grant, MAPPED_DENIED, OPERATOR, Principal, READER_A, READER_B,
    Scope, TABLE_ALPHA, TABLE_BETA, TABLE_MISSING, batch_check, batch_check_request,
    ensure_authz_fixture, ensure_role, ensure_role_member, jwt_claims, lakekeeper_server_info,
    post_batch_check, provision_authz_fixture, whoami_id,
};
use common::seed::{
    DECIMAL_10_2_VALUES_TEXT, DECIMAL_18_0_VALUES_TEXT, DECIMAL_36_6_VALUES_TEXT,
    E2E_ALL_TYPES_TABLE, E2E_DIM_TABLE, E2E_FACT_TABLE, E2E_NAMESPACE, E2E_TABLE,
    SEED_ROWS_SCORE_GT_15, SEED_TOTAL_ROWS, SeedCatalogAuth, seed_all_types_with_auth,
    seed_events_table_with_auth, seed_star_schema_with_auth,
};
use common::stack::{
    self, CatalogConnectionPassword, build_create_connection_sql, exasol_host, exasol_sql_port,
    wait_for_exasol, wait_for_seaweedfs, wait_for_url,
};

use futures::TryStreamExt;
use lakehouse_catalog::{
    CatalogProps, CatalogSession, ConnectionCreds, StaticStoreAddress, StorageBackend,
    load_table_any_auth, redact_secret_values,
};
use object_store::aws::{AmazonS3, AmazonS3Builder};
use object_store::path::Path as ObjectStorePath;
use object_store::{ObjectStore, ObjectStoreExt};
use serde_json::Value;

use std::sync::OnceLock;
use std::time::Duration;

const VS_STATIC: &str = "LK_STATIC_LAKEHOUSE";
const VS_VENDED: &str = "LK_VENDED_LAKEHOUSE";
const CONN_STATIC: &str = "LK_STATIC_CATALOG_CREDS";
const CONN_VENDED: &str = "LK_VENDED_CATALOG_CREDS";

const LAKEKEEPER_CATALOG_URI_INTERNAL: &str = "http://lakekeeper:8181/catalog";

fn vs_static_table() -> String {
    format!("{VS_STATIC}.{}", E2E_TABLE.to_uppercase())
}

fn vs_vended_table() -> String {
    format!("{VS_VENDED}.{}", E2E_TABLE.to_uppercase())
}

static SETUP_DONE: OnceLock<()> = OnceLock::new();

fn setup() {
    SETUP_DONE.get_or_init(|| {
        wait_for_exasol();
        wait_for_seaweedfs();
        lakekeeper::wait_for_keycloak();
        lakekeeper::wait_for_lakekeeper();

        lakekeeper::lakekeeper_bootstrap();
        lakekeeper::lakekeeper_create_warehouse(&WarehouseProfile::static_creds());
        lakekeeper::lakekeeper_create_warehouse(&WarehouseProfile::vended());

        // A fresh token per warehouse so a short token lifetime cannot expire mid-seed.
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("tokio runtime");
        let host_catalog = lakekeeper::catalog_uri_host();
        for warehouse in [WAREHOUSE_STATIC, WAREHOUSE_VENDED] {
            let token = lakekeeper::keycloak_client_credentials_token();
            let auth = SeedCatalogAuth {
                token: Some(token),
                ..Default::default()
            };
            rt.block_on(async {
                seed_events_table_with_auth(&host_catalog, warehouse, auth.clone())
                    .await
                    .unwrap_or_else(|e| {
                        panic!("seed events into Lakekeeper warehouse '{warehouse}': {e:#}")
                    });
                if warehouse == WAREHOUSE_STATIC {
                    seed_all_types_with_auth(&host_catalog, warehouse, auth.clone())
                        .await
                        .unwrap_or_else(|e| {
                            panic!("seed all_types into Lakekeeper warehouse '{warehouse}': {e:#}")
                        });
                }
                if warehouse == WAREHOUSE_VENDED {
                    seed_star_schema_with_auth(&host_catalog, warehouse, auth)
                        .await
                        .unwrap_or_else(|e| {
                            panic!(
                                "seed star schema into Lakekeeper warehouse '{warehouse}': {e:#}"
                            )
                        });
                }
            });
        }

        install_slc();
        upload_so();
        let mut conn = exa_conn();
        create_schema_and_scripts(&mut conn);

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

#[test]
fn lakekeeper_static_creds_projection_filter_limit() {
    setup();
    let mut conn = exa_conn();

    // Seed: id 1..20, score = 5.0 * id.
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

/// Scenario: Decimals declare DECIMAL and order numerically through Lakekeeper and on ADLS Gen2
#[test]
fn lakekeeper_decimals_declare_their_precision_and_order_numerically() {
    setup();
    let mut conn = exa_conn();
    for (column, declared) in [
        ("C_DECIMAL_10_2", "DECIMAL(10,2)"),
        ("C_DECIMAL_18_0", "DECIMAL(18,0)"),
        ("C_DECIMAL_36_6", "DECIMAL(36,6)"),
    ] {
        assert_eq!(
            declared_type(&mut conn, VS_STATIC, E2E_ALL_TYPES_TABLE, column),
            declared
        );
    }
    assert_text_columns(
        &mut conn,
        &format!(
            "SELECT C_DECIMAL_10_2, C_DECIMAL_18_0, C_DECIMAL_36_6 FROM {VS_STATIC}.ALL_TYPES ORDER BY ID"
        ),
        &[
            DECIMAL_10_2_VALUES_TEXT,
            DECIMAL_18_0_VALUES_TEXT,
            DECIMAL_36_6_VALUES_TEXT,
        ],
    );
    assert_decimal_orders_numerically(&mut conn, VS_STATIC, E2E_ALL_TYPES_TABLE, "C_DECIMAL_18_0");
}

// With no static credential or store address to fall back on, rows can only come
// through the vended-credentials delegation.
#[test]
fn lakekeeper_vended_creds_projection_filter() {
    setup();
    let mut conn = exa_conn();

    let vended_pw = lakekeeper_connection_password(WAREHOUSE_VENDED, true);
    assert!(
        vended_pw.use_vended_credentials,
        "vended warehouse CONNECTION must request access delegation"
    );
    assert!(
        vended_pw.endpoint.is_empty()
            && vended_pw.region.is_empty()
            && vended_pw.access_key.is_empty()
            && vended_pw.secret_key.is_empty(),
        "a vended CONNECTION must carry NO static storage field: a static key pair would be \
         an unread credential, while a static endpoint or region would OVERRIDE the vended \
         store address"
    );

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

#[test]
fn lakekeeper_suite_fails_when_stack_unavailable() {
    let result = std::panic::catch_unwind(|| {
        wait_for_url("http://127.0.0.1:1/health", Duration::from_secs(2));
    });
    assert!(
        result.is_err(),
        "a readiness wait against an unreachable Lakekeeper stack must panic (fail), \
         never return Ok (skip)"
    );
}

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

    // Exasol 8 has no combined `ADAPTER_SCRIPT` column.
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

#[test]
fn lakekeeper_oauth_prefix_under_base_path_resolves() {
    setup();
    let mut conn = exa_conn();

    // Exasol 8 exposes `CONNECTION_STRING` only via the DBA view.
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
        !panic_msg.contains(SENTINEL_CLIENT_SECRET)
            && !panic_msg.contains(SENTINEL_ACCESS_KEY)
            && !panic_msg.contains(SENTINEL_SECRET_KEY),
        "redacting execute() failure must not leak any credential value: {panic_msg}"
    );
}

/// Deliberately not `Debug`: three fields are live credentials.
struct VendedProbe {
    table: &'static str,
    location: String,
    bucket: String,
    key_prefix: String,
    region: String,
    access_key: String,
    secret_key: String,
    session_token: Option<String>,
}

impl VendedProbe {
    fn secrets(&self) -> Vec<&str> {
        let mut secrets = vec![self.access_key.as_str(), self.secret_key.as_str()];
        secrets.extend(self.session_token.as_deref());
        secrets
    }
}

fn split_s3_uri(uri: &str) -> (String, String) {
    let rest = uri
        .strip_prefix("s3://")
        .or_else(|| uri.strip_prefix("s3a://"))
        .unwrap_or_else(|| panic!("expected an s3/s3a URI, got: {uri}"));
    let (bucket, key) = rest
        .split_once('/')
        .unwrap_or_else(|| panic!("expected a <bucket>/<key> URI form, got: {uri}"));
    (bucket.to_string(), key.to_string())
}

async fn probe_vended_credential(
    session: &CatalogSession,
    creds: &ConnectionCreds,
    table: &'static str,
) -> VendedProbe {
    let catalog = CatalogProps {
        warehouse: WAREHOUSE_VENDED.to_string(),
        table: format!("{E2E_NAMESPACE}.{table}"),
    };
    let result = load_table_any_auth(session, &catalog, creds)
        .await
        .unwrap_or_else(|e| {
            panic!("the access-delegated loadTable GET for {table} must succeed: {e}")
        });

    let location = result.metadata.location().to_string();
    assert!(
        !location.is_empty(),
        "Lakekeeper's loadTable response for {table} carries no table `location`: the credential \
         entry is selected BY that location, so the probe has no anchor to select with"
    );
    let (bucket, key_prefix) = split_s3_uri(&location);
    let backend = lakehouse_catalog::resolve_vended_storage(
        &result,
        &location,
        true,
        &StaticStoreAddress::from(creds),
    )
    .unwrap_or_else(|e| panic!("resolve_vended_storage for {table} ({location}) failed: {e}"));
    let StorageBackend::S3(props) = backend else {
        panic!("this fixture is SeaweedFS (s3://): {table} ({location}) vended a non-S3 backend");
    };

    assert!(
        !props.access_key.is_empty() && !props.secret_key.is_empty(),
        "the credential source Lakekeeper vended for {table} ({location}) carries no usable s3 \
         key pair, so it cannot be signed with and the cross-table probe would prove nothing"
    );

    VendedProbe {
        table,
        location,
        bucket,
        key_prefix,
        region: props.region,
        access_key: props.access_key,
        secret_key: props.secret_key,
        session_token: props.session_token,
    }
}

/// Uses the host-mapped SeaweedFS URL: the vended `s3.endpoint` is a Docker-network
/// address the test process cannot reach.
fn s3_client_as(probe: &VendedProbe, bucket: &str) -> AmazonS3 {
    let mut builder = AmazonS3Builder::new()
        .with_bucket_name(bucket)
        .with_access_key_id(&probe.access_key)
        .with_secret_access_key(&probe.secret_key)
        .with_endpoint(stack::seaweedfs_url())
        .with_allow_http(true)
        .with_virtual_hosted_style_request(false);
    if !probe.region.is_empty() {
        builder = builder.with_region(&probe.region);
    }
    if let Some(token) = &probe.session_token {
        builder = builder.with_token(token);
    }
    builder
        .build()
        .unwrap_or_else(|e| panic!("configure a SeaweedFS S3 client for bucket {bucket}: {e}"))
}

async fn first_parquet_under(
    store: &AmazonS3,
    key_prefix: &str,
    secrets: &[&str],
) -> ObjectStorePath {
    let prefix = ObjectStorePath::from(key_prefix);
    let mut listing = store.list(Some(&prefix));
    while let Some(meta) = listing.try_next().await.unwrap_or_else(|e| {
        panic!(
            "listing {key_prefix} with the table's OWN vended credential failed: {}",
            redact_secret_values(&e.to_string(), secrets)
        )
    }) {
        if meta.location.as_ref().ends_with(".parquet") {
            return meta.location;
        }
    }
    panic!("no .parquet data file under {key_prefix}: the star-schema seed must have written one")
}

// An ALLOWED cross read means this fixture cannot reproduce #294 and a green join
// test would prove only carriage. The own-credential control read rules out a
// broken probe. No credential value may reach output.
#[test]
fn lakekeeper_vended_credentials_are_scoped_per_table() {
    setup();

    let creds = lakekeeper::lakekeeper_host_connection_creds(WAREHOUSE_VENDED, true);
    assert!(
        creds.use_vended_credentials,
        "the probe CONNECTION must request access delegation: without that flag the loadTable \
         GET carries no X-Iceberg-Access-Delegation header and its response evidences nothing"
    );

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime for the vended-credential scope probe");

    rt.block_on(async {
        let session =
            CatalogSession::resolve(&lakekeeper::catalog_uri_host(), WAREHOUSE_VENDED, &creds)
                .await
                .unwrap_or_else(|e| {
                    panic!("CatalogSession::resolve against Lakekeeper must succeed: {e}")
                });

        let fact = probe_vended_credential(&session, &creds, E2E_FACT_TABLE).await;
        let dim = probe_vended_credential(&session, &creds, E2E_DIM_TABLE).await;
        let secrets = [fact.secrets(), dim.secrets()].concat();

        // Printed first so the observed state survives a failing control read.
        println!(
            "lakekeeper_vended_credentials_are_scoped_per_table:\n  \
             {} location={}\n  \
             {} location={}\n  \
             same vended access key: {}\n  \
             session token vended: {} / {}",
            fact.table,
            fact.location,
            dim.table,
            dim.location,
            fact.access_key == dim.access_key,
            fact.session_token.is_some(),
            dim.session_token.is_some(),
        );

        let dim_store = s3_client_as(&dim, &dim.bucket);
        let victim = first_parquet_under(&dim_store, &dim.key_prefix, &secrets).await;

        // Drained so a body-level failure cannot leave the control green.
        match dim_store.get(&victim).await {
            Ok(result) => result.bytes().await.map(|bytes| bytes.len()),
            Err(e) => Err(e),
        }
        .unwrap_or_else(|e| {
            panic!(
                "control read of {victim} with dim_customer's OWN vended credential failed, so a \
                 denial below could not be told apart from a broken probe: {}",
                redact_secret_values(&e.to_string(), &secrets)
            )
        });

        // Drained so a lazily-surfaced denial cannot read as success.
        let cross = match s3_client_as(&fact, &dim.bucket).get(&victim).await {
            Ok(result) => result.bytes().await.map(|bytes| bytes.len()),
            Err(e) => Err(e),
        };
        match &cross {
            Ok(len) => {
                println!("  cross-table read (fact_orders creds -> {victim}): ALLOWED, {len} bytes")
            }
            Err(e) => println!(
                "  cross-table read (fact_orders creds -> {victim}): DENIED, {}",
                redact_secret_values(&e.to_string(), &secrets)
            ),
        }

        assert!(
            cross.is_err(),
            "ALLOWED: fact_orders' vended credential read dim_customer's data file {victim}. The \
             two sides' vended credentials differ in VALUE but not in SCOPE, so this fixture \
             CANNOT reproduce issue #294 as a read error — a broadcast join that discards the \
             dimension side's credential still returns correct rows here, and a green join test \
             would prove only carriage, never necessity."
        );
    });
}

#[test]
fn lakekeeper_vended_broadcast_join_result_correct() {
    setup();
    let mut conn = exa_conn();

    let pushed_sql = explain_virtual_sql(&mut conn, &join_query(VS_VENDED));
    assert!(
        has_broadcast_join_block(&pushed_sql),
        "expected a broadcast join block in the pushed SQL: {pushed_sql}"
    );
    assert!(
        !has_two_scan_wrapper(&pushed_sql),
        "expected NO two-scan unaccelerated fallback wrapper in the pushed SQL: {pushed_sql}"
    );

    let actual = fetch_join_rows(&mut conn, VS_VENDED);
    let expected = expected_join_rows(&mut conn, VS_VENDED);

    assert_eq!(
        actual.len(),
        6,
        "expected 6 joined rows (orders 5..=10), got {}: {actual:?}",
        actual.len()
    );
    assert_eq!(
        actual, expected,
        "broadcast join result over the vended-credential warehouse must equal the \
         independently computed join.\nactual:   {actual:?}\nexpected: {expected:?}"
    );
}

const OPENFGA_BACKEND: &str = "openfga";

fn authz_fixture() -> &'static AuthzFixture {
    setup();
    ensure_authz_fixture()
}

fn grant(scope: Scope, relation: &'static str) -> Grant {
    Grant { scope, relation }
}

fn operator_allows(fixture: &AuthzFixture, principal: &Principal, table: &str) -> bool {
    let checks = [fixture.read_check(principal, table)];
    batch_check(&OPERATOR.token(), &checks).allowed(CHECK_ID)
}

#[test]
fn lakekeeper_stack_enforces_permissions() {
    setup();
    let info = lakekeeper_server_info();
    let backend = info["authz-backend"].as_str().unwrap_or("<none reported>");
    assert_eq!(
        backend, OPENFGA_BACKEND,
        "Lakekeeper reports authz-backend '{backend}'; the stack must run \
         LAKEKEEPER__AUTHZ_BACKEND={OPENFGA_BACKEND} (recreate it with `down -v` after the switch)"
    );
    let fixture = authz_fixture();
    assert!(
        !operator_allows(fixture, &READER_A, TABLE_BETA),
        "a principal without a grant on {TABLE_BETA} must be denied it"
    );
}

#[test]
fn lakekeeper_two_principals_hold_different_table_grants() {
    let fixture = authz_fixture();
    let checks = [
        fixture.read_check_for_user("a-alpha", fixture.principal_id(&READER_A), TABLE_ALPHA),
        fixture.read_check_for_user("a-beta", fixture.principal_id(&READER_A), TABLE_BETA),
        fixture.read_check_for_user("b-alpha", fixture.principal_id(&READER_B), TABLE_ALPHA),
        fixture.read_check_for_user("b-beta", fixture.principal_id(&READER_B), TABLE_BETA),
    ];

    let answer = batch_check(&OPERATOR.token(), &checks);

    assert_eq!(answer.status, 200, "{}", answer.body);
    assert!(answer.allowed("a-alpha"), "reader A must be allowed alpha");
    assert!(!answer.allowed("a-beta"), "reader A must be denied beta");
    assert!(!answer.allowed("b-alpha"), "reader B must be denied alpha");
    assert!(answer.allowed("b-beta"), "reader B must be allowed beta");
}

#[test]
fn authz_fixture_provisioning_removes_a_stale_reader_grant() {
    let fixture = authz_fixture();
    fixture.ensure_grant(
        fixture.principal_id(&READER_A),
        grant(Scope::Table(TABLE_BETA), "select"),
    );
    assert!(
        operator_allows(fixture, &READER_A, TABLE_BETA),
        "precondition: the stale grant takes effect"
    );

    let reprovisioned = provision_authz_fixture();

    assert!(
        !operator_allows(&reprovisioned, &READER_A, TABLE_BETA),
        "provisioning must remove a grant the fixture does not name"
    );
    assert!(operator_allows(&reprovisioned, &READER_A, TABLE_ALPHA));
}

/// The permission-check test's denied user must not inherit a read of the scanned table.
#[test]
fn authz_fixture_provisioning_removes_a_stale_grant_on_the_scanned_tables_namespace() {
    let fixture = authz_fixture();
    let denied_reads_events = |fixture: &AuthzFixture| {
        let check = fixture.read_check_at(CHECK_ID, MAPPED_DENIED, E2E_NAMESPACE, E2E_TABLE);
        batch_check(&OPERATOR.token(), &[check]).allowed(CHECK_ID)
    };
    fixture.ensure_grant(
        MAPPED_DENIED,
        grant(Scope::Namespace(E2E_NAMESPACE), "select"),
    );
    assert!(
        denied_reads_events(fixture),
        "precondition: the stale namespace grant takes effect"
    );

    let reprovisioned = provision_authz_fixture();

    assert!(
        !denied_reads_events(&reprovisioned),
        "provisioning must remove a namespace grant the fixture does not name"
    );
}

/// Proves only a direct client-credentials login. It does NOT prove that the id #415's
/// `USER_MAPPING` template derives from an Exasol user matches an existing grant (#415).
#[test]
fn authz_direct_login_principal_id_is_idp_prefix_and_token_subject() {
    let fixture = authz_fixture();
    let claims = jwt_claims(&READER_A.token());
    let sub = claims["sub"].as_str().expect("token carries a sub claim");

    assert_eq!(whoami_id(&READER_A), format!("oidc~{sub}"));
    let groups: Vec<usize> = sub.split('-').map(str::len).collect();
    assert_eq!(groups, [8, 4, 4, 4, 12], "sub is not a UUID: {sub}");
    assert!(sub.chars().all(|c| c.is_ascii_hexdigit() || c == '-'));

    let unregistered = "oidc~template-user@corp";
    fixture.ensure_grant(unregistered, grant(Scope::Table(TABLE_ALPHA), "select"));
    let checks = [fixture.read_check_for_user(CHECK_ID, unregistered, TABLE_ALPHA)];
    assert!(
        batch_check(&OPERATOR.token(), &checks).allowed(CHECK_ID),
        "an id that never logged in must still be grantable and allowed"
    );
}

#[test]
fn authz_check_for_another_identity_is_forbidden_without_grant_management() {
    let fixture = authz_fixture();
    let holdings: [(&str, Vec<Grant>); 3] = [
        ("no assignment", vec![]),
        ("server admin only", vec![grant(Scope::Server, "admin")]),
        (
            "warehouse select only",
            vec![grant(Scope::Warehouse, "select")],
        ),
    ];

    for (label, grants) in holdings {
        fixture.set_assignments(&CHECKER, &grants);
        let checks = [fixture.read_check(&READER_A, TABLE_ALPHA)];

        let answer = batch_check(&CHECKER.token(), &checks);

        assert_eq!(answer.status, 403, "checker with {label}: {}", answer.body);
        assert_eq!(
            answer.error_type(),
            Some("CannotInspectPermissions"),
            "checker with {label}: {}",
            answer.body
        );
        assert!(
            answer.body.get("results").is_none(),
            "checker with {label} must get no per-check results: {}",
            answer.body
        );
    }
    fixture.set_assignments(&CHECKER, &[]);
}

#[test]
fn authz_warehouse_manage_grants_allows_checking_another_identity() {
    let fixture = authz_fixture();
    fixture.set_assignments(&CHECKER, &[grant(Scope::Warehouse, "manage_grants")]);
    let checks = [
        fixture.read_check_for_user("a-alpha", fixture.principal_id(&READER_A), TABLE_ALPHA),
        fixture.read_check_for_user("a-beta", fixture.principal_id(&READER_A), TABLE_BETA),
        fixture.read_check_for_user("a-missing", fixture.principal_id(&READER_A), TABLE_MISSING),
    ];

    let by_checker = batch_check(&CHECKER.token(), &checks);
    let by_operator = batch_check(&OPERATOR.token(), &checks);
    fixture.set_assignments(&CHECKER, &[]);

    assert_eq!(by_checker.status, 200, "{}", by_checker.body);
    assert_eq!(
        by_checker.body, by_operator.body,
        "warehouse manage_grants must answer exactly as the operator does"
    );
}

/// Checks the unprefixed local topology only; a path-rewriting gateway is not covered.
#[test]
fn authz_management_api_is_mounted_beside_catalog_path_on_local_topology() {
    setup();
    let catalog_uri = lakekeeper::catalog_uri_host();
    let root = catalog_uri
        .strip_suffix("/catalog")
        .unwrap_or_else(|| panic!("catalog URI {catalog_uri} does not end in /catalog"));
    let client = lakekeeper::http_client();
    let token = lakekeeper::keycloak_client_credentials_token();

    let management_url = format!("{root}/management/v1/info");
    let config_url = format!("{catalog_uri}/v1/config?warehouse={WAREHOUSE_STATIC}");

    let management = client
        .get(&management_url)
        .bearer_auth(&token)
        .send()
        .unwrap_or_else(|e| panic!("GET {management_url} failed to send: {e}"));
    let catalog = client
        .get(&config_url)
        .bearer_auth(&token)
        .send()
        .unwrap_or_else(|e| panic!("GET {config_url} failed to send: {e}"));

    assert_eq!(
        management.status().as_u16(),
        200,
        "management API must sit at the root beside {catalog_uri}"
    );
    assert_eq!(
        catalog.status().as_u16(),
        200,
        "GET {config_url} must answer 200"
    );
}

struct FixtureCase {
    name: &'static str,
    caller: &'static Principal,
    identity: &'static Principal,
    table: &'static str,
}

const ALLOWED_CASE: FixtureCase = FixtureCase {
    name: "allowed",
    caller: &OPERATOR,
    identity: &READER_A,
    table: TABLE_ALPHA,
};
const DENIED_CASE: FixtureCase = FixtureCase {
    name: "denied",
    caller: &OPERATOR,
    identity: &READER_A,
    table: TABLE_BETA,
};
const MISSING_CASE: FixtureCase = FixtureCase {
    name: "missing",
    caller: &OPERATOR,
    identity: &READER_A,
    table: TABLE_MISSING,
};
const CANNOT_INSPECT_CASE: FixtureCase = FixtureCase {
    name: "cannot-inspect",
    caller: &READER_B,
    identity: &READER_A,
    table: TABLE_ALPHA,
};

const FIXTURE_CASES: [FixtureCase; 4] =
    [ALLOWED_CASE, DENIED_CASE, MISSING_CASE, CANNOT_INSPECT_CASE];

const CAPTURE_VARIABLE: &str = "LH_LAKEKEEPER_FIXTURE_CAPTURE";

fn fixture_path(case: &str) -> String {
    format!(
        "{}/../lakehouse-catalog/tests/fixtures/lakekeeper/batch-check/{case}.json",
        env!("CARGO_MANIFEST_DIR")
    )
}

fn normalized_exchange(fixture: &AuthzFixture, case: &FixtureCase) -> Value {
    let request = batch_check_request(&[fixture.read_check(case.identity, case.table)]);
    let answer = post_batch_check(&case.caller.token(), &request);
    fixture.normalize(&serde_json::json!({
        "case": case.name,
        "caller": case.caller.client_id,
        "request": request,
        "response": answer.to_value(),
    }))
}

/// The fields #415 depends on; message text may differ between Lakekeeper builds.
fn contract(document: &Value) -> Value {
    let response = &document["response"];
    serde_json::json!({
        "request": document["request"],
        "status": response["status"],
        "results": response["body"]["results"],
        "error.type": response["body"]["error"]["type"],
        "error.code": response["body"]["error"]["code"],
    })
}

#[test]
fn authz_batch_check_fixtures_match_live_contract() {
    let fixture = authz_fixture();

    for case in &FIXTURE_CASES {
        let live = normalized_exchange(fixture, case);
        let path = fixture_path(case.name);
        if std::env::var(CAPTURE_VARIABLE).as_deref() == Ok("1") {
            let text = serde_json::to_string_pretty(&live).expect("fixture serializes") + "\n";
            assert!(
                !fixture.carries_secret_or_live_id(&text),
                "captured fixture '{}' carries a secret, a token, or a live id",
                case.name
            );
            std::fs::write(&path, text).unwrap_or_else(|e| panic!("write {path}: {e}"));
            continue;
        }

        let text = std::fs::read_to_string(&path).unwrap_or_else(|e| {
            panic!("fixture {path} is unreadable ({e}); regenerate with {CAPTURE_VARIABLE}=1")
        });
        assert!(
            !fixture.carries_secret_or_live_id(&text),
            "fixture '{}' carries a secret, a token, or a live id",
            case.name
        );
        let recorded: Value = serde_json::from_str(&text)
            .unwrap_or_else(|e| panic!("fixture '{}' is not JSON: {e}", case.name));
        assert_eq!(
            contract(&live),
            contract(&recorded),
            "fixture '{}' no longer matches Lakekeeper",
            case.name
        );
    }
}

#[test]
fn authz_denied_and_missing_tables_answer_identically() {
    let fixture = authz_fixture();
    let answer = |table| {
        let checks = [fixture.read_check(&READER_A, table)];
        batch_check(&OPERATOR.token(), &checks)
    };

    let (denied, missing) = (answer(TABLE_BETA), answer(TABLE_MISSING));

    assert_eq!(denied.status, 200, "{}", denied.body);
    assert!(!denied.allowed(CHECK_ID));
    assert_eq!(denied.body, missing.body);
}

type ExasolUser = (&'static str, &'static str);

const VS_PERMISSION: &str = "LK_PERM_LAKEHOUSE";
const VS_PERMISSION_NO_JOIN: &str = "LK_PERM_NO_JOIN";
const VS_UNINSPECTING: &str = "LK_PERM_UNINSPECTING";
const CONN_PERMISSION: &str = "LK_PERM_CATALOG_CREDS";
const CONN_UNINSPECTING: &str = "LK_PERM_UNINSPECTING_CREDS";
const VIEW_SCHEMA: &str = "LK_PERM_VIEWS";
const ALLOWED_USER: ExasolUser = ("LK_PERM_ALLOWED", "LkPermAllowed2026x");
const DENIED_USER: ExasolUser = ("LK_PERM_DENIED", "LkPermDenied2026x");
const UNKNOWN_USER: ExasolUser = ("LK_PERM_UNKNOWN", "LkPermUnknown2026x");
const JOIN_ONE_USER: ExasolUser = ("LK_PERM_JOINONE", "LkPermJoinOne2026x");
const JOIN_BOTH_USER: ExasolUser = ("LK_PERM_JOINBOTH", "LkPermJoinBoth2026x");
const VIEW_OWNER_USER: ExasolUser = ("LK_PERM_VIEWOWNER", "LkPermViewOwner2026x");
const VIEW_READER_USER: ExasolUser = ("LK_PERM_VIEWREADER", "LkPermViewReader2026x");
const REVOKED_USER: ExasolUser = ("LK_PERM_REVOKED", "LkPermRevoked2026x");
const DESCRIBE_USER: ExasolUser = ("LK_PERM_DESCRIBE", "LkPermDescribe2026x");
const NAMESPACE_USER: ExasolUser = ("LK_PERM_NAMESPACE", "LkPermNamespace2026x");
const WAREHOUSE_USER: ExasolUser = ("LK_PERM_WAREHOUSE", "LkPermWarehouse2026x");
const ROLE_USER: ExasolUser = ("LK_PERM_ROLE", "LkPermRole2026x");
const PERMISSION_USERS: [ExasolUser; 12] = [
    ALLOWED_USER,
    DENIED_USER,
    UNKNOWN_USER,
    JOIN_ONE_USER,
    JOIN_BOTH_USER,
    VIEW_OWNER_USER,
    VIEW_READER_USER,
    REVOKED_USER,
    DESCRIBE_USER,
    NAMESPACE_USER,
    WAREHOUSE_USER,
    ROLE_USER,
];
const ROLE_NAME: &str = "lk-perm-readers";

/// Three lines, a tab, and double quotes, so the test also proves that Exasol stores and
/// passes the template byte for byte.
const PERMISSION_USER_MAPPING: &str = "{% if user is startingwith(\"LK_PERM_\") %}\n\
     \toidc~lk.{{ user[8:]|lower }}@lakehouse.test\n\
     {% endif %}";

static PERMISSION_SETUP_DONE: OnceLock<()> = OnceLock::new();

/// The Lakekeeper user id that `PERMISSION_USER_MAPPING` derives from the Exasol user.
fn mapped_principal((user, _): ExasolUser) -> String {
    format!(
        "oidc~lk.{}@lakehouse.test",
        user["LK_PERM_".len()..].to_lowercase()
    )
}

/// `lakehouse-reader-b` reads `/v1/config` but holds no grant management, so its batch-check
/// answers 403 `CannotInspectPermissions`.
fn uninspecting_password() -> CatalogConnectionPassword {
    CatalogConnectionPassword {
        client_id: Some(READER_B.client_id.to_string()),
        client_secret: Some(READER_B.client_secret.to_string()),
        ..lakekeeper_connection_password(WAREHOUSE_AUTHZ, false)
    }
}

fn permission_setup() {
    PERMISSION_SETUP_DONE.get_or_init(|| {
        authz_fixture();
        let mut sys = exa_conn();

        sys.execute(&format!("DROP SCHEMA IF EXISTS {VIEW_SCHEMA} CASCADE"));
        for vs in [VS_PERMISSION, VS_PERMISSION_NO_JOIN, VS_UNINSPECTING] {
            sys.execute(&format!("DROP VIRTUAL SCHEMA IF EXISTS {vs} CASCADE"));
        }
        for (user, _) in PERMISSION_USERS {
            sys.execute(&format!("DROP USER IF EXISTS {user} CASCADE"));
        }
        for conn in [CONN_PERMISSION, CONN_UNINSPECTING] {
            sys.execute(&format!("DROP CONNECTION IF EXISTS {conn}"));
        }
        for (user, password) in PERMISSION_USERS {
            sys.execute(&format!("CREATE USER {user} IDENTIFIED BY \"{password}\""));
            sys.execute(&format!("GRANT CREATE SESSION TO {user}"));
        }
        sys.execute(&format!(
            "GRANT CREATE SCHEMA, CREATE VIEW TO {}",
            VIEW_OWNER_USER.0
        ));

        let operator = lakekeeper_connection_password(WAREHOUSE_AUTHZ, false);
        for (conn, props) in [
            (CONN_PERMISSION, VsProps::new(VS_PERMISSION, E2E_NAMESPACE)),
            (
                CONN_PERMISSION,
                VsProps::new(VS_PERMISSION_NO_JOIN, E2E_NAMESPACE)
                    .with_join_broadcast_max_bytes("1"),
            ),
            (
                CONN_UNINSPECTING,
                VsProps::new(VS_UNINSPECTING, E2E_NAMESPACE),
            ),
        ] {
            create_virtual_schema_with_password(
                &mut sys,
                &props
                    .with_catalog_conn_name(conn)
                    .with_property("PERMISSION_CHECK", "LAKEKEEPER")
                    .with_property("USER_MAPPING", PERMISSION_USER_MAPPING),
                LAKEKEEPER_CATALOG_URI_INTERNAL,
                &operator,
            );
        }
        // Swapped in after the create, because `lakehouse-reader-b` cannot list the namespace.
        sys.execute(&build_create_connection_sql(
            CONN_UNINSPECTING,
            LAKEKEEPER_CATALOG_URI_INTERNAL,
            &uninspecting_password(),
        ));

        // The view reader reaches the virtual schema only through the owner's view.
        for vs in [VS_PERMISSION, VS_PERMISSION_NO_JOIN, VS_UNINSPECTING] {
            for (user, _) in PERMISSION_USERS {
                if user != VIEW_READER_USER.0 {
                    sys.execute(&format!("GRANT SELECT ON SCHEMA {vs} TO {user}"));
                }
            }
        }
    });
}

fn connect_as((user, password): ExasolUser) -> ExaConn {
    ExaConn::connect(&exasol_host(), exasol_sql_port(), user, password)
}

fn events_query(vs: &str) -> String {
    format!(
        "SELECT id FROM {vs}.{} ORDER BY id",
        E2E_TABLE.to_uppercase()
    )
}

fn refusal_text(response: &Value) -> String {
    assert_eq!(
        response["status"].as_str(),
        Some("error"),
        "the query must be refused: {response}"
    );
    response["exception"]["text"]
        .as_str()
        .unwrap_or_default()
        .to_string()
}

fn assert_no_secret_or_token(message: &str) {
    for secret in [
        OPERATOR.client_secret,
        READER_B.client_secret,
        "eyJ",
        "Bearer ",
    ] {
        assert!(
            !message.contains(secret),
            "the refusal leaks a secret or a token: {message}"
        );
    }
}

/// Runs `sql` as `user`, which Lakekeeper must refuse for each table in `denied` only.
fn assert_refused(user: ExasolUser, sql: &str, denied: &[&str], allowed: &[&str]) {
    let message = refusal_text(&connect_as(user).try_execute(sql));
    for table in denied {
        assert!(
            message.contains(table),
            "{}: must name {table}: {message}",
            user.0
        );
    }
    for table in allowed {
        assert!(
            !message.contains(table),
            "{}: must not name the readable {table}: {message}",
            user.0
        );
    }
    assert!(message.contains(&mapped_principal(user)), "{message}");
    assert_no_secret_or_token(&message);
}

fn event_ids(user: ExasolUser, vs: &str) -> Vec<i64> {
    connect_as(user).query_columns(&events_query(vs))[0]
        .iter()
        .map(parse_int)
        .collect()
}

fn all_event_ids() -> Vec<i64> {
    (1..=SEED_TOTAL_ROWS as i64).collect()
}

fn grant_only(user: ExasolUser, grants: &[Grant]) {
    authz_fixture().set_user_assignments(&mapped_principal(user), grants);
}

fn select_table(table: &'static str) -> Grant {
    grant(Scope::Table(table), "select")
}

fn qualified(table: &str) -> String {
    format!("{E2E_NAMESPACE}.{table}")
}

/// Scenario: Grants on the mapped principal decide a live Exasol user's query
#[test]
fn permission_check_grants_decide_each_mapped_users_query() {
    permission_setup();
    let stored = exa_conn().query_columns(&format!(
        "SELECT PROPERTY_VALUE FROM SYS.EXA_ALL_VIRTUAL_SCHEMA_PROPERTIES \
         WHERE SCHEMA_NAME = '{VS_PERMISSION}' AND PROPERTY_NAME = 'USER_MAPPING'"
    ));
    assert_eq!(
        stored
            .first()
            .and_then(|column| column.first())
            .and_then(Value::as_str),
        Some(PERMISSION_USER_MAPPING),
        "Exasol must store the USER_MAPPING template byte for byte"
    );

    assert_eq!(
        event_ids(ALLOWED_USER, VS_PERMISSION),
        all_event_ids(),
        "the user whose mapped principal holds a grant reads every seeded row"
    );
    for user in [DENIED_USER, UNKNOWN_USER] {
        assert_refused(
            user,
            &events_query(VS_PERMISSION),
            &[&qualified(E2E_TABLE)],
            &[],
        );
    }
}

/// Scenario: A failed batch-check refuses the query with an error that names the cause
#[test]
fn permission_check_refuses_when_the_connection_cannot_inspect_permissions() {
    permission_setup();

    let message =
        refusal_text(&connect_as(ALLOWED_USER).try_execute(&events_query(VS_UNINSPECTING)));

    for fragment in ["403", "CannotInspectPermissions", "manage_grants"] {
        assert!(
            message.contains(fragment),
            "the refusal must name {fragment:?}: {message}"
        );
    }
    assert_no_secret_or_token(&message);
}

/// Scenario: A denied table refuses the whole query
#[test]
fn permission_check_refuses_a_join_unless_the_user_may_read_both_tables() {
    permission_setup();
    grant_only(JOIN_ONE_USER, &[select_table(E2E_FACT_TABLE)]);
    grant_only(
        JOIN_BOTH_USER,
        &[select_table(E2E_FACT_TABLE), select_table(E2E_DIM_TABLE)],
    );

    for vs in [VS_PERMISSION, VS_PERMISSION_NO_JOIN] {
        let query = join_query(vs);
        assert_refused(
            JOIN_ONE_USER,
            &query,
            &[&qualified(E2E_DIM_TABLE)],
            &[&qualified(E2E_FACT_TABLE)],
        );

        let mut both = connect_as(JOIN_BOTH_USER);
        let pushed = explain_virtual_sql(&mut both, &query);
        assert_eq!(
            has_broadcast_join_block(&pushed),
            vs == VS_PERMISSION,
            "{vs}: join pushdown must be {} in: {pushed}",
            if vs == VS_PERMISSION { "on" } else { "off" }
        );
        let rows = fetch_join_rows(&mut both, vs);
        assert_eq!(
            rows.len(),
            6,
            "{vs}: the user with both grants reads the join: {rows:?}"
        );
    }
}

/// Which user the check sees when a view over the virtual schema is queried.
#[test]
fn permission_check_sees_the_user_who_queries_a_view_not_its_owner() {
    permission_setup();
    grant_only(VIEW_OWNER_USER, &[select_table(E2E_TABLE)]);
    grant_only(VIEW_READER_USER, &[]);
    let view = format!("{VIEW_SCHEMA}.EVENTS_V");
    let mut owner = connect_as(VIEW_OWNER_USER);
    owner.execute(&format!("CREATE SCHEMA {VIEW_SCHEMA}"));
    owner.execute(&format!(
        "CREATE VIEW {view} AS SELECT id FROM {VS_PERMISSION}.{}",
        E2E_TABLE.to_uppercase()
    ));
    owner.execute(&format!("GRANT SELECT ON {view} TO {}", VIEW_READER_USER.0));
    let query = format!("SELECT id FROM {view} ORDER BY id");

    let owner_ids: Vec<i64> = owner.query_columns(&query)[0]
        .iter()
        .map(parse_int)
        .collect();
    assert_eq!(owner_ids, all_event_ids(), "the owner holds the grant");

    assert_refused(VIEW_READER_USER, &query, &[&qualified(E2E_TABLE)], &[]);
}

/// Scenario: A denied table refuses the whole query
#[test]
fn permission_check_refuses_the_same_query_once_a_grant_is_revoked() {
    permission_setup();
    let query = events_query(VS_PERMISSION);
    grant_only(REVOKED_USER, &[select_table(E2E_TABLE)]);
    assert_eq!(event_ids(REVOKED_USER, VS_PERMISSION), all_event_ids());

    grant_only(REVOKED_USER, &[]);

    assert_refused(REVOKED_USER, &query, &[&qualified(E2E_TABLE)], &[]);
}

/// Scenario: A denied table refuses the whole query
#[test]
fn permission_check_refuses_a_user_with_describe_but_no_select() {
    permission_setup();
    grant_only(DESCRIBE_USER, &[grant(Scope::Table(E2E_TABLE), "describe")]);

    assert_refused(
        DESCRIBE_USER,
        &events_query(VS_PERMISSION),
        &[&qualified(E2E_TABLE)],
        &[],
    );
}

/// Scenario: Grants on the mapped principal decide a live Exasol user's query
#[test]
fn permission_check_honors_grants_inherited_from_namespace_warehouse_and_role() {
    permission_setup();
    let fixture = authz_fixture();
    grant_only(
        NAMESPACE_USER,
        &[grant(Scope::Namespace(E2E_NAMESPACE), "select")],
    );
    grant_only(WAREHOUSE_USER, &[grant(Scope::Warehouse, "select")]);
    grant_only(ROLE_USER, &[]);
    let role = ensure_role(ROLE_NAME);
    ensure_role_member(&role, &mapped_principal(ROLE_USER));
    fixture.ensure_role_grant(&role, select_table(E2E_TABLE));

    for user in [NAMESPACE_USER, WAREHOUSE_USER, ROLE_USER] {
        assert_eq!(
            event_ids(user, VS_PERMISSION),
            all_event_ids(),
            "{}: an inherited grant lets the user read the table",
            user.0
        );
    }
}

/// Scenario: A denied table refuses the whole query
#[test]
fn permission_check_refuses_explain_virtual_without_a_grant() {
    permission_setup();
    grant_only(DENIED_USER, &[select_table(TABLE_ALPHA)]);

    let message = refusal_text(
        &connect_as(DENIED_USER)
            .try_execute(&format!("EXPLAIN VIRTUAL {}", events_query(VS_PERMISSION))),
    );

    assert!(message.contains(&qualified(E2E_TABLE)), "{message}");
}

/// Scenario: One batch-check per query decides every table before any table is read
#[test]
fn permission_check_gates_an_aggregate_query() {
    permission_setup();
    let query = format!(
        "SELECT COUNT(*), SUM(id) FROM {VS_PERMISSION}.{}",
        E2E_TABLE.to_uppercase()
    );
    grant_only(ALLOWED_USER, &[select_table(E2E_TABLE)]);

    let columns = connect_as(ALLOWED_USER).query_columns(&query);

    assert_eq!(parse_int(&columns[0][0]), SEED_TOTAL_ROWS as i64);
    assert_refused(DENIED_USER, &query, &[&qualified(E2E_TABLE)], &[]);
}
