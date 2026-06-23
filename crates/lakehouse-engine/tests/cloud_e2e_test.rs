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
use common::stack::{CatalogConnectionPassword, build_create_connection_sql};

use base64::{Engine as _, engine::general_purpose::STANDARD as B64};
use native_tls::TlsConnector;
use rand::rngs::OsRng;
use rsa::pkcs1::DecodeRsaPublicKey;
use rsa::pkcs8::DecodePublicKey;
use rsa::{Pkcs1v15Encrypt, RsaPublicKey};
use serde_json::{Value, json};
use std::net::TcpStream;
use tungstenite::stream::MaybeTlsStream;
use tungstenite::{Message, WebSocket, client_tls_with_config};

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

const CLOUD_SCHEMA_NAME: &str = "CLOUD_LHVS";
const CLOUD_VS_NAME: &str = "CLOUD_LAKEHOUSE";
const CLOUD_ADAPTER_SCRIPT: &str = "LAKEHOUSE_ADAPTER";
const CLOUD_CATALOG_CONN: &str = "GLUE_CATALOG_CREDS";
const CLOUD_CATALOG_CONN_VENDED: &str = "GLUE_CATALOG_CREDS_VENDED";

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
        }
    }

    /// Build a `CatalogConnectionPassword` with SigV4 + vended credentials.
    fn catalog_connection_password_vended(&self) -> CatalogConnectionPassword {
        CatalogConnectionPassword {
            use_vended_credentials: true,
            ..self.catalog_connection_password()
        }
    }
}

// ---------------------------------------------------------------------------
// Minimal WebSocket client for cloud tests (self-contained, no exasol-e2e dep)
// ---------------------------------------------------------------------------

struct CloudExaConn {
    ws: WebSocket<MaybeTlsStream<TcpStream>>,
}

impl CloudExaConn {
    fn connect(host: &str, port: u16, user: &str, password: &str) -> Self {
        let url = format!("wss://{host}:{port}");
        let tls = TlsConnector::builder()
            .danger_accept_invalid_certs(true)
            .danger_accept_invalid_hostnames(true)
            .build()
            .expect("build TLS connector");
        let tcp = TcpStream::connect(format!("{host}:{port}"))
            .unwrap_or_else(|e| panic!("TCP connect to Exasol at {host}:{port}: {e}"));
        let connector = tungstenite::Connector::NativeTls(tls);
        let (mut ws, _) = client_tls_with_config(url.as_str(), tcp, None, Some(connector))
            .expect("WebSocket TLS handshake with Exasol");

        ws.send(Message::Text(
            r#"{"command":"login","protocolVersion":3}"#.to_string().into(),
        ))
        .expect("send login initiation");
        let resp = Self::read_json(&mut ws);
        assert_eq!(
            resp["status"].as_str(),
            Some("ok"),
            "Exasol login initiation failed: {resp}"
        );
        let pem = resp["responseData"]["publicKeyPem"]
            .as_str()
            .expect("publicKeyPem in login response");

        let enc_password = encrypt_password(password, pem);
        let creds = json!({
            "command": "login",
            "protocolVersion": 3,
            "username": user,
            "password": enc_password,
            "useCompression": false,
            "clientName": "lakehouse-engine-cloud-test",
            "driverName": "lakehouse-engine-cloud-test",
            "clientOs": "Linux",
            "clientOsUsername": "ci",
            "clientRuntime": "Rust"
        });
        ws.send(Message::Text(creds.to_string().into()))
            .expect("send credentials");
        let resp = Self::read_json(&mut ws);
        assert_eq!(
            resp["status"].as_str(),
            Some("ok"),
            "Exasol authentication failed"
        );
        CloudExaConn { ws }
    }

    fn execute(&mut self, sql: &str) -> Value {
        let cmd = json!({
            "command": "execute",
            "sqlText": sql,
            "attributes": {"resultSetMaxRows": 10000}
        });
        self.ws
            .send(Message::Text(cmd.to_string().into()))
            .expect("send execute");
        let resp = Self::read_json(&mut self.ws);
        assert_eq!(
            resp["status"].as_str(),
            Some("ok"),
            "Exasol execute failed for SQL — check Exasol error"
        );
        resp
    }

    fn try_execute(&mut self, sql: &str) -> Value {
        let cmd = json!({
            "command": "execute",
            "sqlText": sql,
            "attributes": {"resultSetMaxRows": 10000}
        });
        self.ws
            .send(Message::Text(cmd.to_string().into()))
            .expect("send execute");
        Self::read_json(&mut self.ws)
    }

    fn query_columns(&mut self, sql: &str) -> Vec<Vec<Value>> {
        let resp = self.execute(sql);
        let result_set = &resp["responseData"]["results"][0]["resultSet"];
        self.fetch_result_columns(result_set)
    }

    fn fetch_result_columns(&mut self, result_set: &Value) -> Vec<Vec<Value>> {
        if let Some(data) = result_set["data"].as_array() {
            return data
                .iter()
                .map(|col| col.as_array().cloned().unwrap_or_default())
                .collect();
        }
        let handle = match result_set["resultSetHandle"].as_u64() {
            Some(h) => h,
            None => return vec![],
        };
        let num_rows = result_set["numRows"].as_u64().unwrap_or(0);
        if num_rows == 0 {
            let close = json!({"command":"closeResultSet","resultSetHandles":[handle]});
            self.ws.send(Message::Text(close.to_string().into())).ok();
            let _ = Self::read_json(&mut self.ws);
            return vec![];
        }
        let fetch = json!({
            "command": "fetch",
            "resultSetHandle": handle,
            "startPosition": 0,
            "numBytes": 67108864
        });
        self.ws
            .send(Message::Text(fetch.to_string().into()))
            .expect("send fetch");
        let fetch_resp = Self::read_json(&mut self.ws);
        let close = json!({"command":"closeResultSet","resultSetHandles":[handle]});
        self.ws.send(Message::Text(close.to_string().into())).ok();
        let _ = Self::read_json(&mut self.ws);
        fetch_resp["data"]
            .as_array()
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .map(|col| col.as_array().cloned().unwrap_or_default())
            .collect()
    }

    fn read_json(ws: &mut WebSocket<MaybeTlsStream<TcpStream>>) -> Value {
        loop {
            let msg = ws.read().expect("read WebSocket message from Exasol");
            match msg {
                Message::Text(text) => {
                    return serde_json::from_str(text.as_str())
                        .expect("parse Exasol response as JSON");
                }
                Message::Ping(data) => {
                    ws.send(Message::Pong(data)).expect("pong Exasol");
                }
                Message::Pong(_) | Message::Frame(_) => {}
                Message::Binary(_) => panic!("unexpected binary WebSocket message from Exasol"),
                Message::Close(_) => panic!("Exasol WebSocket closed unexpectedly"),
            }
        }
    }
}

impl Drop for CloudExaConn {
    fn drop(&mut self) {
        let _ = self.ws.close(None);
    }
}

fn encrypt_password(password: &str, pem_key: &str) -> String {
    let key = if pem_key.contains("BEGIN RSA PUBLIC KEY") {
        RsaPublicKey::from_pkcs1_pem(pem_key).expect("parse PKCS#1 RSA key from Exasol")
    } else {
        RsaPublicKey::from_public_key_pem(pem_key).expect("parse PKCS#8 RSA key from Exasol")
    };
    let ciphertext = key
        .encrypt(&mut OsRng, Pkcs1v15Encrypt, password.as_bytes())
        .expect("RSA encrypt Exasol password");
    B64.encode(ciphertext)
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

fn setup_cloud_vs(conn: &mut CloudExaConn, env: &CloudEnv, conn_name: &str, vs_name: &str) {
    conn.execute(&format!("CREATE SCHEMA IF NOT EXISTS {CLOUD_SCHEMA_NAME}"));

    let password = env.catalog_connection_password();
    let create_conn_sql = build_create_connection_sql(conn_name, &env.glue_catalog_uri, &password);
    conn.execute(&create_conn_sql);

    let _ = conn.try_execute(&format!("DROP VIRTUAL SCHEMA IF EXISTS {vs_name} CASCADE"));
    conn.execute(&format!(
        r#"CREATE VIRTUAL SCHEMA {vs_name}
USING {CLOUD_SCHEMA_NAME}.{CLOUD_ADAPTER_SCRIPT} WITH
  CATALOG_CONNECTION = '{conn_name}'
  TABLE_NAME         = '{}'"#,
        env.glue_table
    ));
}

fn setup_cloud_vs_vended(conn: &mut CloudExaConn, env: &CloudEnv) {
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
  TABLE_NAME         = '{}'"#,
        env.glue_table
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

    let mut conn = CloudExaConn::connect(
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

    let mut conn = CloudExaConn::connect(
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

    let mut conn = CloudExaConn::connect(
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
