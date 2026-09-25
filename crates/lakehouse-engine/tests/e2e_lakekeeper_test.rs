//! E2E tests against a Lakekeeper Iceberg REST catalog, OpenID-secured via
//! Keycloak and backed by the base stack's MinIO.
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
    create_schema_and_scripts, create_virtual_schema_with_password, exa_conn, expected_join_rows,
    explain_virtual_sql, fetch_join_rows, has_broadcast_join_block, has_two_scan_wrapper,
    install_slc, join_query, parse_int, upload_so,
};
use common::exasol_ws::ExaConn;
use common::lakekeeper::{
    self, WAREHOUSE_STATIC, WAREHOUSE_VENDED, WarehouseProfile, lakekeeper_connection_password,
};
use common::seed::{
    E2E_DIM_TABLE, E2E_FACT_TABLE, E2E_NAMESPACE, E2E_TABLE, SEED_ROWS_SCORE_GT_15,
    SEED_TOTAL_ROWS, SeedCatalogAuth, seed_events_table_with_auth, seed_star_schema_with_auth,
};
use common::stack::{
    self, CatalogConnectionPassword, build_create_connection_sql, exasol_host, exasol_sql_port,
    wait_for_exasol, wait_for_minio, wait_for_url,
};

use futures::TryStreamExt;
use lakehouse_catalog::{
    CatalogProps, CatalogSession, ConnectionCreds, StaticStoreAddress, StorageBackend,
    load_table_any_auth, redact_secret_values,
};
use object_store::aws::{AmazonS3, AmazonS3Builder};
use object_store::path::Path as ObjectStorePath;
use object_store::{ObjectStore, ObjectStoreExt};

use std::sync::OnceLock;
use std::time::Duration;

const VS_STATIC: &str = "LK_STATIC_LAKEHOUSE";
const VS_VENDED: &str = "LK_VENDED_LAKEHOUSE";
const CONN_STATIC: &str = "LK_STATIC_CATALOG_CREDS";
const CONN_VENDED: &str = "LK_VENDED_CATALOG_CREDS";

const LAKEKEEPER_CATALOG_URI_INTERNAL: &str = "http://lakekeeper:8181/catalog";

fn lakekeeper_catalog_url_host() -> String {
    format!("http://localhost:{}/catalog", lakekeeper::lakekeeper_port())
}

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
        wait_for_minio();
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
        let host_catalog = lakekeeper_catalog_url_host();
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

/// Scenario: setup bootstraps Lakekeeper and a virtual schema exists over each warehouse
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

/// Scenario: createVirtualSchema enumerates the seeded table via the built-in OAuth2 client
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

/// Scenario: projection + filter + LIMIT over the static-credential warehouse returns correct rows
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

/// Scenario: a vended-credential scan with no static storage field returns the static warehouse's rows
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

/// Scenario: a readiness wait against an unreachable stack panics rather than skipping
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

/// Scenario: both virtual schemas use the shared harness's adapter and scan scripts
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

/// Scenario: the OAuth2 path resolves tables under the `/catalog` base path
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

/// Scenario: a failing credential-bearing CONNECTION DDL leaks neither SQL text nor credentials
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
        panic!("this fixture is MinIO (s3://): {table} ({location}) vended a non-S3 backend");
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

/// Uses the host-mapped MinIO URL: the vended `s3.endpoint` is a Docker-network
/// address the test process cannot reach.
fn s3_client_as(probe: &VendedProbe, bucket: &str) -> AmazonS3 {
    let mut builder = AmazonS3Builder::new()
        .with_bucket_name(bucket)
        .with_access_key_id(&probe.access_key)
        .with_secret_access_key(&probe.secret_key)
        .with_endpoint(stack::minio_url())
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
        .unwrap_or_else(|e| panic!("configure a MinIO S3 client for bucket {bucket}: {e}"))
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

/// Scenario: fact_orders' vended credential is denied reading dim_customer's data file (#294)
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
            CatalogSession::resolve(&lakekeeper_catalog_url_host(), WAREHOUSE_VENDED, &creds)
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

/// Scenario: a broadcast join over per-table-scoped vended credentials returns correct rows (#294)
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
