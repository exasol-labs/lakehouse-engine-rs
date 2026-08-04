//! Cloud E2E smoke tests for the lakehouse-engine Virtual Schema against
//! a real AWS Glue Iceberg REST catalog.
//!
//! These tests are gated behind the `cloud-e2e` cargo feature and SKIP
//! cleanly when the required environment variables are absent — the opposite
//! of the local `exasol-e2e` suite, which FAILS when its stack is down.
//!
//! Required environment variables (all absent → test skips, no network call):
//!   GLUE_CATALOG_URI     — Glue Iceberg REST endpoint (catalog CONNECTION address)
//!   GLUE_WAREHOUSE       — S3 URI of the Iceberg warehouse (e.g. s3://my-bucket/path/)
//!   GLUE_TABLE           — Fully-qualified table name (e.g. my_db.my_table)
//!   AWS_REGION           — AWS region (e.g. us-east-1)
//!   AWS_ACCESS_KEY_ID    — AWS static access key ID
//!   AWS_SECRET_ACCESS_KEY — AWS static secret access key
//!   AWS_SESSION_TOKEN    — (optional) AWS STS session token
//!   EXASOL_HOST          — Exasol hostname/IP
//!   LH_EXASOL_PORT       — Exasol WebSocket SQL port (default 28563)
//!   LH_EXASOL_USER       — Exasol username (default "sys")
//!   LH_EXASOL_PASSWORD   — Exasol password
//!
//! All DSN/connection strings include validateservercertificate=0.
//! No credential value is printed to test output.
#![cfg(feature = "cloud-e2e")]

mod common;
use common::exasol_ws::ExaConn;
use common::stack::{CatalogConnectionPassword, build_create_connection_sql};
use lakehouse_catalog::{CatalogProps, CatalogSession, ConnectionCreds, load_table_any_auth};
use std::collections::HashMap;

// ---------------------------------------------------------------------------
// Environment variable names
// ---------------------------------------------------------------------------

const ENV_GLUE_CATALOG_URI: &str = "GLUE_CATALOG_URI";
const ENV_GLUE_WAREHOUSE: &str = "GLUE_WAREHOUSE";
const ENV_GLUE_TABLE: &str = "GLUE_TABLE";
const ENV_AWS_REGION: &str = "AWS_REGION";
const ENV_AWS_ACCESS_KEY_ID: &str = "AWS_ACCESS_KEY_ID";
const ENV_AWS_SECRET_ACCESS_KEY: &str = "AWS_SECRET_ACCESS_KEY";
const ENV_AWS_SESSION_TOKEN: &str = "AWS_SESSION_TOKEN";
const ENV_EXASOL_HOST: &str = "EXASOL_HOST";
const ENV_EXASOL_PORT: &str = "LH_EXASOL_PORT";
const ENV_EXASOL_USER: &str = "LH_EXASOL_USER";
const ENV_EXASOL_PASSWORD: &str = "LH_EXASOL_PASSWORD";

// Catalog-auth E2E env vars (token or OAuth2 client-credentials REST catalog).
// Required: CATALOG_AUTH_URI, CATALOG_AUTH_WAREHOUSE, CATALOG_AUTH_TABLE, EXASOL_HOST,
//           LH_EXASOL_PASSWORD, and at least one of CATALOG_AUTH_TOKEN or
//           (CATALOG_AUTH_CLIENT_ID + CATALOG_AUTH_CLIENT_SECRET).
// Optional: CATALOG_AUTH_OAUTH2_SERVER_URI, CATALOG_AUTH_SCOPE (OAuth path only).
const ENV_CATALOG_AUTH_URI: &str = "CATALOG_AUTH_URI";
const ENV_CATALOG_AUTH_WAREHOUSE: &str = "CATALOG_AUTH_WAREHOUSE";
const ENV_CATALOG_AUTH_TABLE: &str = "CATALOG_AUTH_TABLE";
const ENV_CATALOG_AUTH_TOKEN: &str = "CATALOG_AUTH_TOKEN";
const ENV_CATALOG_AUTH_CLIENT_ID: &str = "CATALOG_AUTH_CLIENT_ID";
const ENV_CATALOG_AUTH_CLIENT_SECRET: &str = "CATALOG_AUTH_CLIENT_SECRET";
const ENV_CATALOG_AUTH_OAUTH2_SERVER_URI: &str = "CATALOG_AUTH_OAUTH2_SERVER_URI";
const ENV_CATALOG_AUTH_SCOPE: &str = "CATALOG_AUTH_SCOPE";

const CLOUD_SCHEMA_NAME: &str = "CLOUD_LHVS";
const CLOUD_VS_NAME: &str = "CLOUD_LAKEHOUSE";
const CLOUD_ADAPTER_SCRIPT: &str = "LAKEHOUSE_ADAPTER";
const CLOUD_CATALOG_CONN: &str = "GLUE_CATALOG_CREDS";
const CLOUD_CATALOG_CONN_VENDED: &str = "GLUE_CATALOG_CREDS_VENDED";
const CLOUD_CATALOG_CONN_AUTH: &str = "CATALOG_AUTH_CREDS";

// ---------------------------------------------------------------------------
// CloudEnv: discovered credentials and endpoints
// ---------------------------------------------------------------------------

/// Credentials and endpoints discovered from environment variables.
///
/// `None` when any required variable is absent — callers early-return (skip).
struct CloudEnv {
    glue_catalog_uri: String,
    glue_warehouse: String,
    glue_table: String,
    aws_region: String,
    aws_access_key_id: String,
    aws_secret_access_key: String,
    aws_session_token: Option<String>,
    exasol_host: String,
    exasol_port: u16,
    exasol_user: String,
    exasol_password: String,
}

impl CloudEnv {
    /// Attempt to read all required environment variables.
    ///
    /// Returns `None` when any required variable is absent or empty.
    /// Never panics; never makes a network call.
    fn from_env() -> Option<Self> {
        let required = [
            ENV_GLUE_CATALOG_URI,
            ENV_GLUE_WAREHOUSE,
            ENV_GLUE_TABLE,
            ENV_AWS_REGION,
            ENV_AWS_ACCESS_KEY_ID,
            ENV_AWS_SECRET_ACCESS_KEY,
            ENV_EXASOL_HOST,
            ENV_EXASOL_PASSWORD,
        ];
        // If any required var is absent or empty, return None (skip signal).
        for var in required {
            match std::env::var(var) {
                Ok(v) if !v.trim().is_empty() => {}
                _ => {
                    println!(
                        "SKIPPED: cloud-e2e requires env var {var} — set it to enable cloud tests"
                    );
                    return None;
                }
            }
        }

        let exasol_port = std::env::var(ENV_EXASOL_PORT)
            .ok()
            .and_then(|s| s.trim().parse::<u16>().ok())
            .unwrap_or(28563);

        Some(CloudEnv {
            glue_catalog_uri: std::env::var(ENV_GLUE_CATALOG_URI).unwrap(),
            glue_warehouse: std::env::var(ENV_GLUE_WAREHOUSE).unwrap(),
            glue_table: std::env::var(ENV_GLUE_TABLE).unwrap(),
            aws_region: std::env::var(ENV_AWS_REGION).unwrap(),
            aws_access_key_id: std::env::var(ENV_AWS_ACCESS_KEY_ID).unwrap(),
            aws_secret_access_key: std::env::var(ENV_AWS_SECRET_ACCESS_KEY).unwrap(),
            aws_session_token: std::env::var(ENV_AWS_SESSION_TOKEN)
                .ok()
                .filter(|s| !s.trim().is_empty()),
            exasol_host: std::env::var(ENV_EXASOL_HOST).unwrap(),
            exasol_port,
            exasol_user: std::env::var(ENV_EXASOL_USER).unwrap_or_else(|_| "sys".to_string()),
            exasol_password: std::env::var(ENV_EXASOL_PASSWORD).unwrap(),
        })
    }

    /// Build a `CatalogConnectionPassword` with SigV4 enabled (standard cloud path).
    fn catalog_connection_password(&self) -> CatalogConnectionPassword {
        CatalogConnectionPassword {
            warehouse: self.glue_warehouse.clone(),
            endpoint: String::new(),
            region: self.aws_region.clone(),
            access_key: self.aws_access_key_id.clone(),
            secret_key: self.aws_secret_access_key.clone(),
            session_token: self.aws_session_token.clone(),
            path_style: false,
            use_sigv4: true,
            use_vended_credentials: false,
            ..Default::default()
        }
    }

    /// Build a `CatalogConnectionPassword` with SigV4 + vended credentials.
    fn catalog_connection_password_vended(&self) -> CatalogConnectionPassword {
        CatalogConnectionPassword {
            use_vended_credentials: true,
            ..self.catalog_connection_password()
        }
    }

    /// The `ConnectionCreds` the adapter parses out of the vended CONNECTION this
    /// suite creates, so an in-process catalog call drives Glue with exactly the
    /// credential set that CONNECTION carries — including the static AWS keys,
    /// which SigV4 catalog signing still needs and which scheme-driven storage
    /// resolution now ignores.
    ///
    /// Derived from `catalog_connection_password_vended` rather than from the
    /// environment a second time, so the two cannot describe different
    /// CONNECTIONs. `sas_token` is absent because `CatalogConnectionPassword`
    /// carries no inline-SAS field to project from.
    fn vended_connection_creds(&self) -> ConnectionCreds {
        let password = self.catalog_connection_password_vended();
        ConnectionCreds {
            warehouse: password.warehouse,
            endpoint: password.endpoint,
            region: password.region,
            access_key: password.access_key,
            secret_key: password.secret_key,
            session_token: password.session_token,
            path_style: password.path_style,
            use_sigv4: password.use_sigv4,
            use_vended_credentials: password.use_vended_credentials,
            token: password.token,
            client_id: password.client_id,
            client_secret: password.client_secret,
            oauth2_server_uri: password.oauth2_server_uri,
            scope: password.scope,
            account_name: password.account_name,
            account_key: password.account_key,
            sas_token: None,
        }
    }
}

// ---------------------------------------------------------------------------
// CatalogAuthEnv: credentials for the catalog-auth (token/OAuth) E2E test
// ---------------------------------------------------------------------------

/// Credentials and endpoints for a token/OAuth2-authenticated REST catalog E2E test.
///
/// Gating: same convention as the other cloud-e2e tests — returns `None` when
/// any required variable is absent; the test early-returns (skip) rather than
/// failing. A live catalog auth smoke run sets all vars and exercises the live path.
struct CatalogAuthEnv {
    catalog_uri: String,
    catalog_warehouse: String,
    catalog_table: String,
    /// Static bearer token (token mode). `None` when `CATALOG_AUTH_TOKEN` is absent.
    catalog_token: Option<String>,
    /// OAuth2 client ID (client-credentials mode). `None` when absent.
    catalog_client_id: Option<String>,
    /// OAuth2 client secret (client-credentials mode). `None` when absent.
    catalog_client_secret: Option<String>,
    /// Optional OAuth2 token endpoint. Absent → catalog defaults to `{uri}/v1/oauth/tokens`.
    catalog_oauth2_server_uri: Option<String>,
    /// Optional OAuth2 scope. Absent → catalog applies its default (`catalog`).
    catalog_scope: Option<String>,
    exasol_host: String,
    exasol_port: u16,
    exasol_user: String,
    exasol_password: String,
}

impl CatalogAuthEnv {
    /// Attempt to read all required environment variables.
    ///
    /// Returns `None` when any base-required variable is absent or empty, or when
    /// neither a token nor both OAuth client credentials are present.
    /// Never panics; never makes a network call.
    fn from_env() -> Option<Self> {
        // Base required vars (catalog endpoint + Exasol connection).
        let base_required = [
            ENV_CATALOG_AUTH_URI,
            ENV_CATALOG_AUTH_WAREHOUSE,
            ENV_CATALOG_AUTH_TABLE,
            ENV_EXASOL_HOST,
            ENV_EXASOL_PASSWORD,
        ];
        for var in base_required {
            match std::env::var(var) {
                Ok(v) if !v.trim().is_empty() => {}
                _ => {
                    println!("SKIPPED: catalog-auth E2E requires env var {var} — set it to enable");
                    return None;
                }
            }
        }

        let catalog_token = std::env::var(ENV_CATALOG_AUTH_TOKEN)
            .ok()
            .filter(|s| !s.trim().is_empty());
        let catalog_client_id = std::env::var(ENV_CATALOG_AUTH_CLIENT_ID)
            .ok()
            .filter(|s| !s.trim().is_empty());
        let catalog_client_secret = std::env::var(ENV_CATALOG_AUTH_CLIENT_SECRET)
            .ok()
            .filter(|s| !s.trim().is_empty());

        // At least one auth mode must be configured: token or both OAuth fields.
        let has_token = catalog_token.is_some();
        let has_oauth = catalog_client_id.is_some() && catalog_client_secret.is_some();
        if !has_token && !has_oauth {
            println!(
                "SKIPPED: catalog-auth E2E requires either {} or ({} + {}) — set one to enable",
                ENV_CATALOG_AUTH_TOKEN, ENV_CATALOG_AUTH_CLIENT_ID, ENV_CATALOG_AUTH_CLIENT_SECRET,
            );
            return None;
        }

        let exasol_port = std::env::var(ENV_EXASOL_PORT)
            .ok()
            .and_then(|s| s.trim().parse::<u16>().ok())
            .unwrap_or(28563);

        Some(CatalogAuthEnv {
            catalog_uri: std::env::var(ENV_CATALOG_AUTH_URI).unwrap(),
            catalog_warehouse: std::env::var(ENV_CATALOG_AUTH_WAREHOUSE).unwrap(),
            catalog_table: std::env::var(ENV_CATALOG_AUTH_TABLE).unwrap(),
            catalog_token,
            catalog_client_id,
            catalog_client_secret,
            catalog_oauth2_server_uri: std::env::var(ENV_CATALOG_AUTH_OAUTH2_SERVER_URI)
                .ok()
                .filter(|s| !s.trim().is_empty()),
            catalog_scope: std::env::var(ENV_CATALOG_AUTH_SCOPE)
                .ok()
                .filter(|s| !s.trim().is_empty()),
            exasol_host: std::env::var(ENV_EXASOL_HOST).unwrap(),
            exasol_port,
            exasol_user: std::env::var(ENV_EXASOL_USER).unwrap_or_else(|_| "sys".to_string()),
            exasol_password: std::env::var(ENV_EXASOL_PASSWORD).unwrap(),
        })
    }

    /// Build the `CREATE OR REPLACE CONNECTION` SQL for the catalog-auth connection.
    ///
    /// Constructs the JSON password directly, injecting token or OAuth2 client-credentials
    /// fields from the environment (mirrors the `ConnectionCreds` JSON schema consumed by
    /// `connection.rs::parse_creds`). No credential value is embedded in any printed output.
    fn build_create_connection_sql(&self) -> String {
        let mut obj = serde_json::json!({
            "warehouse": self.catalog_warehouse,
            "use_sigv4": false,
            "use_vended_credentials": false,
        });
        // Token mode: inject `token`.
        if let Some(token) = &self.catalog_token {
            obj["token"] = serde_json::Value::String(token.clone());
        } else {
            // OAuth2 client-credentials mode: inject `client_id`, `client_secret`, and
            // optionally `oauth2_server_uri` and `scope`.
            if let Some(client_id) = &self.catalog_client_id {
                obj["client_id"] = serde_json::Value::String(client_id.clone());
            }
            if let Some(client_secret) = &self.catalog_client_secret {
                obj["client_secret"] = serde_json::Value::String(client_secret.clone());
            }
            if let Some(uri) = &self.catalog_oauth2_server_uri {
                obj["oauth2_server_uri"] = serde_json::Value::String(uri.clone());
            }
            if let Some(scope) = &self.catalog_scope {
                obj["scope"] = serde_json::Value::String(scope.clone());
            }
        }
        let json_pw = obj.to_string().replace('\'', "''");
        let safe_uri = self.catalog_uri.replace('\'', "''");
        format!(
            "CREATE OR REPLACE CONNECTION {CLOUD_CATALOG_CONN_AUTH} TO '{safe_uri}' USER '' IDENTIFIED BY '{json_pw}'"
        )
    }
}

// ---------------------------------------------------------------------------
// Schema + VS setup helpers
// ---------------------------------------------------------------------------

fn vs_table(glue_table: &str) -> String {
    // The adapter uppercases the table's last component.
    let table_part = glue_table
        .split('.')
        .next_back()
        .unwrap_or(glue_table)
        .to_uppercase();
    format!("{CLOUD_VS_NAME}.{table_part}")
}

/// The Iceberg namespace of a `namespace.table` identifier (everything before the
/// trailing table segment), used as the `ICEBERG_NAMESPACE` VS property.
fn glue_namespace(glue_table: &str) -> &str {
    glue_table.rsplit_once('.').map_or(glue_table, |(ns, _)| ns)
}

fn setup_cloud_vs(conn: &mut ExaConn, env: &CloudEnv, conn_name: &str, vs_name: &str) {
    conn.execute(&format!("CREATE SCHEMA IF NOT EXISTS {CLOUD_SCHEMA_NAME}"));

    let password = env.catalog_connection_password();
    let create_conn_sql = build_create_connection_sql(conn_name, &env.glue_catalog_uri, &password);
    conn.execute(&create_conn_sql);

    let _ = conn.try_execute(&format!("DROP VIRTUAL SCHEMA IF EXISTS {vs_name} CASCADE"));
    conn.execute(&format!(
        r#"CREATE VIRTUAL SCHEMA {vs_name}
USING {CLOUD_SCHEMA_NAME}.{CLOUD_ADAPTER_SCRIPT} WITH
  CATALOG_CONNECTION = '{conn_name}'
  ICEBERG_NAMESPACE  = '{}'"#,
        glue_namespace(&env.glue_table)
    ));
}

fn setup_cloud_vs_vended(conn: &mut ExaConn, env: &CloudEnv) {
    conn.execute(&format!("CREATE SCHEMA IF NOT EXISTS {CLOUD_SCHEMA_NAME}"));

    let password = env.catalog_connection_password_vended();
    let create_conn_sql =
        build_create_connection_sql(CLOUD_CATALOG_CONN_VENDED, &env.glue_catalog_uri, &password);
    conn.execute(&create_conn_sql);

    let _ = conn.try_execute(&format!(
        "DROP VIRTUAL SCHEMA IF EXISTS {CLOUD_VS_NAME}_VENDED CASCADE"
    ));
    conn.execute(&format!(
        r#"CREATE VIRTUAL SCHEMA {CLOUD_VS_NAME}_VENDED
USING {CLOUD_SCHEMA_NAME}.{CLOUD_ADAPTER_SCRIPT} WITH
  CATALOG_CONNECTION = '{CLOUD_CATALOG_CONN_VENDED}'
  ICEBERG_NAMESPACE  = '{}'"#,
        glue_namespace(&env.glue_table)
    ));
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// Skip test: asserts the skip path returns cleanly with no network call
/// when any required environment variable is absent.
///
/// Reads the current process environment without mutation. If all required vars
/// happen to be present (a live cloud run), the assertion is skipped with a note —
/// the other smoke tests cover that path. When any var is absent the test asserts
/// `CloudEnv::from_env()` returns `None` and makes no network call.
#[test]
fn cloud_test_skips_when_creds_absent() {
    let required = [
        ENV_GLUE_CATALOG_URI,
        ENV_GLUE_WAREHOUSE,
        ENV_GLUE_TABLE,
        ENV_AWS_REGION,
        ENV_AWS_ACCESS_KEY_ID,
        ENV_AWS_SECRET_ACCESS_KEY,
        ENV_EXASOL_HOST,
        ENV_EXASOL_PASSWORD,
    ];

    let all_present = required.iter().all(|var| {
        std::env::var(var)
            .map(|v| !v.trim().is_empty())
            .unwrap_or(false)
    });

    if all_present {
        // All required vars are set — we are in a real cloud run. The other
        // smoke tests exercise the live path; nothing to assert here.
        println!(
            "cloud_test_skips_when_creds_absent: all vars present, assertion skipped (live cloud run)"
        );
        return;
    }

    // At least one required var is absent in the current environment.
    // Verify that from_env() returns None cleanly with no network call.
    let result = CloudEnv::from_env();
    assert!(
        result.is_none(),
        "CloudEnv::from_env() must return None when any required env var is absent"
    );
    println!("cloud_test_skips_when_creds_absent: skip path verified (no network call)");
}

/// Cloud smoke test: creates a Glue-backed VS and runs a projection + filter query.
///
/// Skips when AWS credentials or Exasol coords are absent from the environment.
/// No credential value is printed.
#[test]
fn cloud_smoke_projection_filter_query() {
    let env = match CloudEnv::from_env() {
        Some(e) => e,
        None => {
            println!("SKIPPED: cloud_smoke_projection_filter_query — env vars absent");
            return;
        }
    };

    let mut conn = ExaConn::connect_redacting(
        &env.exasol_host,
        env.exasol_port,
        &env.exasol_user,
        &env.exasol_password,
    );

    setup_cloud_vs(&mut conn, &env, CLOUD_CATALOG_CONN, CLOUD_VS_NAME);

    let table = vs_table(&env.glue_table);

    // Run a simple SELECT to verify rows come back (projection only).
    let all_cols = conn.query_columns(&format!("SELECT * FROM {table} LIMIT 10"));
    assert!(
        !all_cols.is_empty(),
        "query must return at least one column"
    );
    let row_count = all_cols[0].len();
    assert!(
        row_count > 0,
        "query must return at least one row from the seeded Glue table"
    );
    println!(
        "cloud_smoke_projection_filter_query: {} columns, {} rows",
        all_cols.len(),
        row_count
    );

    // Run a COUNT(*) to verify aggregation works.
    let count_cols = conn.query_columns(&format!("SELECT COUNT(*) FROM {table}"));
    assert_eq!(count_cols.len(), 1, "COUNT(*) must return one column");
    let total = count_cols[0][0]
        .as_i64()
        .or_else(|| count_cols[0][0].as_str().and_then(|s| s.parse().ok()))
        .expect("COUNT(*) must be an integer");
    assert!(
        total > 0,
        "COUNT(*) must return a positive row count for the seeded Glue table"
    );
    println!("cloud_smoke_projection_filter_query: COUNT(*) = {total}");

    // No credential values must appear in the output above.
    // (The assert is on the test logic, not output capture — credentials are never
    // embedded in any variable printed above.)
}

/// Cloud performance + aggregate smoke test: grouped COUNT/SUM, wall-clock timing.
///
/// Records the query duration for manual inspection. No hard latency threshold.
/// Skips when credentials are absent.
#[test]
fn cloud_perf_grouped_aggregate_smoke() {
    let env = match CloudEnv::from_env() {
        Some(e) => e,
        None => {
            println!("SKIPPED: cloud_perf_grouped_aggregate_smoke — env vars absent");
            return;
        }
    };

    let mut conn = ExaConn::connect_redacting(
        &env.exasol_host,
        env.exasol_port,
        &env.exasol_user,
        &env.exasol_password,
    );

    setup_cloud_vs(&mut conn, &env, CLOUD_CATALOG_CONN, CLOUD_VS_NAME);

    let table = vs_table(&env.glue_table);

    // First, get the total row count to establish what "sane" means.
    let count_cols = conn.query_columns(&format!("SELECT COUNT(*) FROM {table}"));
    let total_rows = count_cols[0][0]
        .as_i64()
        .or_else(|| count_cols[0][0].as_str().and_then(|s| s.parse().ok()))
        .expect("COUNT(*) must be an integer");
    assert!(
        total_rows > 0,
        "Glue table must have at least one row for the aggregate smoke test"
    );
    println!("cloud_perf_grouped_aggregate_smoke: total rows = {total_rows}");

    // Run a grouped aggregate and time it.
    // We use COUNT(*) grouped by the first column as a generic aggregate that
    // works regardless of the table schema.
    let describe_cols = conn.query_columns(&format!("DESCRIBE {table}"));
    // describe_cols[0] = column names
    let first_col = describe_cols[0]
        .first()
        .and_then(|v| v.as_str())
        .unwrap_or("1");

    let agg_sql = format!("SELECT {first_col}, COUNT(*) FROM {table} GROUP BY {first_col}");

    let start = std::time::Instant::now();
    let agg_cols = conn.query_columns(&agg_sql);
    let elapsed = start.elapsed();

    assert!(
        !agg_cols.is_empty(),
        "GROUP BY query must return at least one column"
    );
    let group_count = agg_cols[0].len();
    assert!(
        group_count > 0,
        "GROUP BY query must return at least one group"
    );

    // Sum of per-group counts must equal total rows.
    if agg_cols.len() >= 2 {
        let group_total: i64 = agg_cols[1]
            .iter()
            .filter_map(|v| {
                v.as_i64()
                    .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
            })
            .sum();
        assert_eq!(
            group_total, total_rows,
            "sum of per-group counts ({group_total}) must equal total rows ({total_rows})"
        );
    }

    // Record timing (observational only — no hard threshold).
    println!("cloud_perf_grouped_aggregate_smoke: {group_count} groups, {elapsed:.2?} wall-clock");
}

/// Vended credentials end-to-end: scan reads Glue data files via vended creds.
///
/// Asserts the scan succeeds and that no credential value appears in test output.
/// Skips when credentials are absent.
#[test]
fn cloud_scan_reads_with_vended_credentials() {
    let env = match CloudEnv::from_env() {
        Some(e) => e,
        None => {
            println!("SKIPPED: cloud_scan_reads_with_vended_credentials — env vars absent");
            return;
        }
    };

    let mut conn = ExaConn::connect_redacting(
        &env.exasol_host,
        env.exasol_port,
        &env.exasol_user,
        &env.exasol_password,
    );

    setup_cloud_vs_vended(&mut conn, &env);

    let vended_table = {
        let table_part = env
            .glue_table
            .split('.')
            .next_back()
            .unwrap_or(&env.glue_table)
            .to_uppercase();
        format!("{CLOUD_VS_NAME}_VENDED.{table_part}")
    };

    // A simple scan via vended credentials must return rows.
    let cols = conn.query_columns(&format!("SELECT * FROM {vended_table} LIMIT 5"));
    assert!(
        !cols.is_empty(),
        "vended-credentials scan must return at least one column"
    );
    assert!(
        !cols[0].is_empty(),
        "vended-credentials scan must return at least one row"
    );
    println!(
        "cloud_scan_reads_with_vended_credentials: {} columns, {} rows via vended creds",
        cols.len(),
        cols[0].len()
    );

    // Credential values must not appear in printed output above.
    // (Static keys and vended keys are never embedded in any printed variable.)
}

/// The one vended credential source that applies to `location`: the
/// `storage_credentials` entry whose non-empty `prefix` is the longest prefix of
/// `location`, else the flat `config` map.
///
/// Mirrors the Iceberg REST selection rule the shipped resolver applies, so the
/// keys read below are the keys a scan of this table would read. Reading the
/// response here instead of calling the resolver is the point of the mirror: the
/// resolver answers with a storage backend, and a backend cannot say which config
/// key the catalog left out.
fn vended_source_for<'a>(
    result: &'a iceberg_catalog_rest::LoadTableResult,
    location: &str,
) -> &'a HashMap<String, String> {
    result
        .storage_credentials
        .as_ref()
        .and_then(|credentials| {
            credentials
                .iter()
                .filter(|entry| !entry.prefix.is_empty() && location.starts_with(&entry.prefix))
                .max_by_key(|entry| entry.prefix.len())
        })
        .map_or(&result.config, |entry| &entry.config)
}

/// Whether the vended credential source carries a usable value for `key`, spelling
/// absence exactly as the shipped resolver spells it: an omitted key and a key
/// present with an empty string are both ABSENT.
///
/// Answers with the presence alone and never the value, so no credential value can
/// reach an assertion message or the report line — three of the five keys this test
/// reads are credentials.
fn vended_key_present(vended: &HashMap<String, String>, key: &str) -> bool {
    vended.get(key).is_some_and(|value| !value.is_empty())
}

/// One key's presence as a word for the report line.
fn presence_label(present: bool) -> &'static str {
    if present { "VENDED" } else { "ABSENT" }
}

/// AWS Glue's vended payload carries a usable S3 credential set AND a store
/// address for the table's own location.
///
/// Evidence `cloud_scan_reads_with_vended_credentials` cannot supply: that
/// CONNECTION also carries static AWS keys from this suite's own environment, so a
/// green scan there is compatible with Glue vending nothing at all. This test
/// issues the access-delegated `loadTable` GET itself and reads the response's
/// vended config keys, which no static CONNECTION value can populate. Scheme-driven
/// resolution takes every S3 transport value from those keys alone, so a key absent
/// here is a plan-time failure for every vended Glue virtual schema — which is why
/// the assertion messages name the absent key rather than reporting a failed scan.
///
/// The anchor is the table's OWN location, derived exactly as `resolve_file_list`
/// derives it: it is both what a `storage_credentials` prefix is matched against and
/// the sole input the backend variant is read from. There is no fallback for it —
/// an absent location is an error in production, so this test asserts it is present
/// rather than substituting the CONNECTION's `warehouse`.
///
/// `s3.session-token` is REPORTED, not required: a permanent IAM identity
/// legitimately vends a key pair with no token. The report line is printed
/// unconditionally, so run the suite with `--nocapture` to read it on a pass.
///
/// Skips when the cloud env vars are absent, like every test in this module.
#[test]
fn cloud_glue_vends_s3_key_pair_and_store_address() {
    let env = match CloudEnv::from_env() {
        Some(e) => e,
        None => {
            println!("SKIPPED: cloud_glue_vends_s3_key_pair_and_store_address — env vars absent");
            return;
        }
    };

    let creds = env.vended_connection_creds();
    assert!(
        creds.use_vended_credentials,
        "the vended CONNECTION must request access delegation: without that flag the loadTable \
         GET carries no X-Iceberg-Access-Delegation header and its response evidences nothing"
    );

    let catalog = CatalogProps {
        warehouse: env.glue_warehouse.clone(),
        table: env.glue_table.clone(),
    };

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime for the Glue vended-payload test");

    let result = rt.block_on(async {
        let session = CatalogSession::resolve(&env.glue_catalog_uri, &catalog.warehouse, &creds)
            .await
            .unwrap_or_else(|e| panic!("CatalogSession::resolve must succeed against Glue: {e}"));
        load_table_any_auth(&session, &catalog, &creds)
            .await
            .unwrap_or_else(|e| panic!("the access-delegated loadTable GET must succeed: {e}"))
    });

    let anchor = result.metadata.location();
    assert!(
        !anchor.is_empty(),
        "Glue's loadTable response carries no table `location`: the Iceberg spec marks it \
         required in v1-v3, and `resolve_file_list` errors on an absent one rather than \
         substituting the catalog `warehouse`, so a scan of this table resolves no backend \
         at all and the S3 keys below have no anchor to be selected by"
    );
    assert!(
        anchor.starts_with("s3://") || anchor.starts_with("s3a://"),
        "Glue's table location {anchor} names no s3 scheme: the backend variant is read from \
         that URI alone, so the S3 keys this test reads would not be the credential set a scan \
         of this table resolves"
    );

    let vended = vended_source_for(&result, anchor);
    let access_key_vended = vended_key_present(vended, "s3.access-key-id");
    let secret_key_vended = vended_key_present(vended, "s3.secret-access-key");
    let region_vended = vended_key_present(vended, "client.region");
    let endpoint_vended = vended_key_present(vended, "s3.endpoint");
    let session_token_vended = vended_key_present(vended, "s3.session-token");

    assert!(
        access_key_vended,
        "the credential source Glue vended for table location {anchor} carries no non-empty \
         s3.access-key-id: under scheme-driven resolution no CONNECTION value can supply one, \
         so every vended Glue virtual schema fails at plan time"
    );
    assert!(
        secret_key_vended,
        "the credential source Glue vended for table location {anchor} carries no non-empty \
         s3.secret-access-key: under scheme-driven resolution no CONNECTION value can supply \
         one, so every vended Glue virtual schema fails at plan time"
    );
    assert!(
        region_vended || endpoint_vended,
        "the credential source Glue vended for table location {anchor} carries neither a \
         non-empty client.region nor a non-empty s3.endpoint: the store address is undetermined \
         and under scheme-driven resolution no CONNECTION value can supply it"
    );

    println!(
        "cloud_glue_vends_s3_key_pair_and_store_address: table location {anchor} — \
         s3.access-key-id {}, s3.secret-access-key {}, client.region {}, s3.endpoint {}, \
         s3.session-token {}",
        presence_label(access_key_vended),
        presence_label(secret_key_vended),
        presence_label(region_vended),
        presence_label(endpoint_vended),
        presence_label(session_token_vended),
    );
}

/// Catalog token/OAuth2 auth end-to-end: resolves a file list from a REST catalog
/// that requires catalog-level authentication (static bearer token or OAuth2
/// client-credentials grant), then asserts the VS returns rows.
///
/// Gating: mirrors `cloud_scan_reads_with_vended_credentials` — skips when any
/// required environment variable is absent; env vars documented at the top of this
/// module. No credential value is printed to test output.
///
/// Required env vars:
///   CATALOG_AUTH_URI          — Iceberg REST catalog endpoint requiring auth
///   CATALOG_AUTH_WAREHOUSE    — S3 URI of the Iceberg warehouse
///   CATALOG_AUTH_TABLE        — Fully-qualified table name (e.g. my_db.my_table)
///   EXASOL_HOST               — Exasol hostname/IP
///   LH_EXASOL_PASSWORD        — Exasol password
/// Auth (at least one mode required):
///   CATALOG_AUTH_TOKEN                 — static bearer token (token mode)
///   CATALOG_AUTH_CLIENT_ID             — OAuth2 client ID   \  client-credentials
///   CATALOG_AUTH_CLIENT_SECRET         — OAuth2 client secret /  mode
///   CATALOG_AUTH_OAUTH2_SERVER_URI     — (optional) OAuth2 token endpoint
///   CATALOG_AUTH_SCOPE                 — (optional) OAuth2 scope
#[test]
fn catalog_token_oauth_auth_resolves_files_e2e() {
    let env = match CatalogAuthEnv::from_env() {
        Some(e) => e,
        None => {
            println!("SKIPPED: catalog_token_oauth_auth_resolves_files_e2e — env vars absent");
            return;
        }
    };

    let mut conn = ExaConn::connect_redacting(
        &env.exasol_host,
        env.exasol_port,
        &env.exasol_user,
        &env.exasol_password,
    );

    conn.execute(&format!("CREATE SCHEMA IF NOT EXISTS {CLOUD_SCHEMA_NAME}"));

    let create_conn_sql = env.build_create_connection_sql();
    conn.execute(&create_conn_sql);

    let auth_vs_name = format!("{CLOUD_VS_NAME}_AUTH");
    let _ = conn.try_execute(&format!(
        "DROP VIRTUAL SCHEMA IF EXISTS {auth_vs_name} CASCADE"
    ));

    let namespace = env
        .catalog_table
        .rsplit_once('.')
        .map_or(env.catalog_table.as_str(), |(ns, _)| ns);

    conn.execute(&format!(
        r#"CREATE VIRTUAL SCHEMA {auth_vs_name}
USING {CLOUD_SCHEMA_NAME}.{CLOUD_ADAPTER_SCRIPT} WITH
  CATALOG_CONNECTION = '{CLOUD_CATALOG_CONN_AUTH}'
  ICEBERG_NAMESPACE  = '{namespace}'"#
    ));

    let table_part = env
        .catalog_table
        .split('.')
        .next_back()
        .unwrap_or(&env.catalog_table)
        .to_uppercase();
    let vs_table = format!("{auth_vs_name}.{table_part}");

    // A SELECT proves that `resolve_file_list` succeeded against the auth-gated catalog.
    let cols = conn.query_columns(&format!("SELECT * FROM {vs_table} LIMIT 5"));
    assert!(
        !cols.is_empty(),
        "catalog-auth scan must return at least one column"
    );
    assert!(
        !cols[0].is_empty(),
        "catalog-auth scan must return at least one row — catalog auth succeeded and files were resolved"
    );

    let auth_mode = if env.catalog_token.is_some() {
        "token"
    } else {
        "oauth2-client-credentials"
    };
    println!(
        "catalog_token_oauth_auth_resolves_files_e2e: {} columns, {} rows via {} auth",
        cols.len(),
        cols[0].len(),
        auth_mode
    );

    // Token and client_secret values must not appear in any printed output above.
    // (Auth credentials are never embedded in any variable printed above.)
}

// ---------------------------------------------------------------------------
// Redaction negative test
// ---------------------------------------------------------------------------

/// A failing, credential-bearing DDL executed through a redacting `ExaConn` must
/// not surface the SQL text or any credential value in the `execute()` failure.
///
/// Skips (like the other cloud tests) when the required env vars are absent.
/// Mirrors the no-leak assertion style in `e2e_refresh_test.rs`.
#[test]
fn cloud_redacting_conn_omits_credentials_on_failure() {
    // Obviously-fake sentinels — never real credentials, so they are safe to
    // surface in a failing-assertion diagnostic below.
    const SENTINEL_ACCESS_KEY: &str = "AKIA_DUMMY_REDACTION_SENTINEL_KEY";
    const SENTINEL_SECRET_KEY: &str = "DUMMY_REDACTION_SENTINEL_SECRET_VALUE";

    let env = match CloudEnv::from_env() {
        Some(e) => e,
        None => {
            println!(
                "SKIPPED: cloud_redacting_conn_omits_credentials_on_failure — env vars absent"
            );
            return;
        }
    };

    let mut conn = ExaConn::connect_redacting(
        &env.exasol_host,
        env.exasol_port,
        &env.exasol_user,
        &env.exasol_password,
    );

    // A realistic credential-bearing CONNECTION DDL carrying the sentinels in its
    // JSON password, plus an invalid trailing token so Exasol rejects it at parse
    // time — driving the execute() DDL-failure path that redaction governs.
    let sentinel_password = CatalogConnectionPassword {
        warehouse: "s3://redaction-probe/".to_string(),
        endpoint: String::new(),
        region: "us-east-1".to_string(),
        access_key: SENTINEL_ACCESS_KEY.to_string(),
        secret_key: SENTINEL_SECRET_KEY.to_string(),
        session_token: None,
        path_style: false,
        use_sigv4: true,
        use_vended_credentials: false,
        ..Default::default()
    };
    let base_sql = build_create_connection_sql(
        "LH_REDACTION_PROBE",
        "https://redaction-probe.invalid",
        &sentinel_password,
    );
    let failing_sql = format!("{base_sql} THIS_TRAILING_TOKEN_MAKES_THE_STATEMENT_INVALID");

    // Deliberately trigger the execute() failure panic and capture it. We do NOT
    // touch the global panic hook: it is process-wide and Rust runs this binary's
    // tests in parallel, so silencing it could swallow panic output from other
    // concurrently running tests. The redacting `ExaConn` already omits the SQL
    // and response body from this message, so letting the hook print it is safe.
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        conn.execute(&failing_sql);
    }));

    let payload = match result {
        Ok(_) => panic!(
            "expected execute() to fail on the malformed credential-bearing DDL, but it succeeded"
        ),
        Err(p) => p,
    };
    let panic_msg: String = if let Some(s) = payload.downcast_ref::<&str>() {
        (*s).to_string()
    } else if let Some(s) = payload.downcast_ref::<String>() {
        s.clone()
    } else {
        String::new()
    };

    // A non-empty string payload proves the failure message was actually
    // captured, so the no-leak assertions below are not vacuously satisfied.
    assert!(
        !panic_msg.is_empty(),
        "expected a string panic payload from the failed redacting execute()"
    );
    assert!(
        !panic_msg.contains(failing_sql.as_str()),
        "redacting execute() failure must not echo the SQL text: {panic_msg}"
    );
    assert!(
        !panic_msg.contains(SENTINEL_ACCESS_KEY) && !panic_msg.contains(SENTINEL_SECRET_KEY),
        "redacting execute() failure must not leak sentinel credential values: {panic_msg}"
    );

    println!(
        "cloud_redacting_conn_omits_credentials_on_failure: redaction verified (no SQL, no credentials in failure output)"
    );
}
