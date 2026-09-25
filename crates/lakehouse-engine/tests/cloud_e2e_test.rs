//! Cloud E2E smoke tests against a real AWS Glue Iceberg REST catalog. Unlike
//! the local `exasol-e2e` suite, these SKIP when their env vars are absent.
//! `GLUE_WAREHOUSE` is the AWS account id, not an S3 URI.
#![cfg(feature = "cloud-e2e")]

mod common;
use common::exasol_ws::ExaConn;
use common::stack::{CatalogConnectionPassword, build_create_connection_sql};
use lakehouse_catalog::{CatalogProps, CatalogSession, ConnectionCreds, load_table_any_auth};
use std::collections::HashMap;

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
const CLOUD_CATALOG_CONN_NO_REGION: &str = "GLUE_CATALOG_CREDS_NO_REGION";
const CLOUD_CATALOG_CONN_AUTH: &str = "CATALOG_AUTH_CREDS";

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

    fn catalog_connection_password_vended(&self) -> CatalogConnectionPassword {
        CatalogConnectionPassword {
            use_vended_credentials: true,
            ..self.catalog_connection_password()
        }
    }

    fn catalog_connection_password_without_region(&self) -> CatalogConnectionPassword {
        CatalogConnectionPassword {
            region: String::new(),
            ..self.catalog_connection_password()
        }
    }

    /// Derived from `catalog_connection_password_vended` so the two cannot describe
    /// different CONNECTIONs.
    fn vended_connection_creds(&self) -> ConnectionCreds {
        let password = self.catalog_connection_password_vended();
        ConnectionCreds {
            warehouse: password.warehouse,
            endpoint: password.endpoint,
            region: password.region,
            access_key: password.access_key,
            secret_key: password.secret_key,
            session_token: password.session_token,
            path_style: Some(password.path_style),
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

struct CatalogAuthEnv {
    catalog_uri: String,
    catalog_warehouse: String,
    catalog_table: String,
    catalog_token: Option<String>,
    catalog_client_id: Option<String>,
    catalog_client_secret: Option<String>,
    /// Absent: the catalog defaults to `{uri}/v1/oauth/tokens`.
    catalog_oauth2_server_uri: Option<String>,
    /// Absent: the catalog applies its default scope (`catalog`).
    catalog_scope: Option<String>,
    exasol_host: String,
    exasol_port: u16,
    exasol_user: String,
    exasol_password: String,
}

impl CatalogAuthEnv {
    fn from_env() -> Option<Self> {
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

    fn build_create_connection_sql(&self) -> String {
        let mut obj = serde_json::json!({
            "warehouse": self.catalog_warehouse,
            "use_sigv4": false,
            "use_vended_credentials": false,
        });
        if let Some(token) = &self.catalog_token {
            obj["token"] = serde_json::Value::String(token.clone());
        } else {
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

fn vs_table_name(glue_table: &str) -> String {
    glue_table
        .split('.')
        .next_back()
        .unwrap_or(glue_table)
        .to_uppercase()
}

fn vs_table(glue_table: &str) -> String {
    format!("{CLOUD_VS_NAME}.{}", vs_table_name(glue_table))
}

fn glue_namespace(glue_table: &str) -> &str {
    glue_table.rsplit_once('.').map_or(glue_table, |(ns, _)| ns)
}

struct CloudVsTarget<'a> {
    conn_name: &'a str,
    vs_name: &'a str,
    password: CatalogConnectionPassword,
}

fn setup_cloud_vs(conn: &mut ExaConn, env: &CloudEnv, target: &CloudVsTarget) {
    conn.execute(&format!("CREATE SCHEMA IF NOT EXISTS {CLOUD_SCHEMA_NAME}"));

    let create_conn_sql =
        build_create_connection_sql(target.conn_name, &env.glue_catalog_uri, &target.password);
    conn.execute(&create_conn_sql);

    let vs_name = target.vs_name;
    let _ = conn.try_execute(&format!("DROP VIRTUAL SCHEMA IF EXISTS {vs_name} CASCADE"));
    conn.execute(&format!(
        r#"CREATE VIRTUAL SCHEMA {vs_name}
USING {CLOUD_SCHEMA_NAME}.{CLOUD_ADAPTER_SCRIPT} WITH
  CATALOG_CONNECTION = '{}'
  NAMESPACE  = '{}'"#,
        target.conn_name,
        glue_namespace(&env.glue_table)
    ));
}

/// Scenario: the cloud suite skips cleanly with no network call when any required env var is absent
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
        println!(
            "cloud_test_skips_when_creds_absent: all vars present, assertion skipped (live cloud run)"
        );
        return;
    }

    let result = CloudEnv::from_env();
    assert!(
        result.is_none(),
        "CloudEnv::from_env() must return None when any required env var is absent"
    );
    println!("cloud_test_skips_when_creds_absent: skip path verified (no network call)");
}

/// Scenario: a Glue-backed VS answers a projection and a COUNT(*) query
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

    setup_cloud_vs(
        &mut conn,
        &env,
        &CloudVsTarget {
            conn_name: CLOUD_CATALOG_CONN,
            vs_name: CLOUD_VS_NAME,
            password: env.catalog_connection_password(),
        },
    );

    let table = vs_table(&env.glue_table);

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
}

/// Scenario: a region-less Glue CONNECTION lists the table, signing with the region derived from the standard Glue endpoint
#[test]
fn cloud_sigv4_region_derived_from_glue_endpoint_lists_table() {
    let env = match CloudEnv::from_env() {
        Some(e) => e,
        None => {
            println!(
                "SKIPPED: cloud_sigv4_region_derived_from_glue_endpoint_lists_table — env vars absent"
            );
            return;
        }
    };

    let standard_glue_uri = format!("https://glue.{}.amazonaws.com/iceberg", env.aws_region);
    let env = CloudEnv {
        glue_catalog_uri: standard_glue_uri,
        ..env
    };

    let mut conn = ExaConn::connect_redacting(
        &env.exasol_host,
        env.exasol_port,
        &env.exasol_user,
        &env.exasol_password,
    );

    let vs_name = format!("{CLOUD_VS_NAME}_NO_REGION");

    setup_cloud_vs(
        &mut conn,
        &env,
        &CloudVsTarget {
            conn_name: CLOUD_CATALOG_CONN_NO_REGION,
            vs_name: &vs_name,
            password: env.catalog_connection_password_without_region(),
        },
    );

    let table_names = conn.query_columns(&format!(
        "SELECT TABLE_NAME FROM SYS.EXA_ALL_TABLES WHERE TABLE_SCHEMA = '{vs_name}'"
    ));
    let listed: Vec<String> = table_names
        .first()
        .map(|c| {
            c.iter()
                .filter_map(|v| v.as_str().map(|s| s.to_uppercase()))
                .collect()
        })
        .unwrap_or_default();

    let expected_table = vs_table_name(&env.glue_table);

    assert!(
        listed.contains(&expected_table),
        "SigV4-signed catalog requests using the endpoint-derived region must list \
         the configured Glue table; found {listed:?}, expected {expected_table}"
    );

    println!(
        "cloud_sigv4_region_derived_from_glue_endpoint_lists_table: listed {} table(s), including {expected_table}",
        listed.len()
    );
}

/// Scenario: a grouped COUNT over the Glue table sums to the total row count (timing is reported, not asserted)
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

    setup_cloud_vs(
        &mut conn,
        &env,
        &CloudVsTarget {
            conn_name: CLOUD_CATALOG_CONN,
            vs_name: CLOUD_VS_NAME,
            password: env.catalog_connection_password(),
        },
    );

    let table = vs_table(&env.glue_table);

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

    let describe_cols = conn.query_columns(&format!("DESCRIBE {table}"));
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

    println!("cloud_perf_grouped_aggregate_smoke: {group_count} groups, {elapsed:.2?} wall-clock");
}

/// Scenario: a scan reads Glue data files via vended credentials
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

    setup_cloud_vs(
        &mut conn,
        &env,
        &CloudVsTarget {
            conn_name: CLOUD_CATALOG_CONN_VENDED,
            vs_name: &format!("{CLOUD_VS_NAME}_VENDED"),
            password: env.catalog_connection_password_vended(),
        },
    );

    let vended_table = format!("{CLOUD_VS_NAME}_VENDED.{}", vs_table_name(&env.glue_table));

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
}

/// Mirrors the resolver's longest-prefix selection (scheme lowercased, RFC 3986
/// §3.1) instead of calling it, because a resolved backend cannot say which config
/// key the catalog left out.
fn vended_source_for<'a>(
    result: &'a iceberg_catalog_rest::LoadTableResult,
    location: &str,
) -> &'a HashMap<String, String> {
    let location = lowercase_scheme(location);
    result
        .storage_credentials
        .as_ref()
        .and_then(|credentials| {
            credentials
                .iter()
                .filter(|entry| {
                    !entry.prefix.is_empty()
                        && location.starts_with(lowercase_scheme(&entry.prefix).as_str())
                })
                .max_by_key(|entry| entry.prefix.len())
        })
        .map_or(&result.config, |entry| &entry.config)
}

fn lowercase_scheme(uri: &str) -> String {
    match uri.split_once("://") {
        Some((scheme, rest)) => format!("{}://{rest}", scheme.to_ascii_lowercase()),
        None => uri.to_string(),
    }
}

/// Omitted and empty are both absent, as in the resolver. Returns presence only so
/// no credential value can reach an assertion message.
fn vended_key_present(vended: &HashMap<String, String>, key: &str) -> bool {
    vended.get(key).is_some_and(|value| !value.is_empty())
}

fn presence_label(present: bool) -> &'static str {
    if present { "VENDED" } else { "ABSENT" }
}

/// Scenario: Glue vends a usable S3 key pair for the table's own location
#[test]
fn cloud_glue_vends_the_s3_key_pair_for_the_table_location() {
    // Reads the `loadTable` response directly: the vended CONNECTION also carries
    // static keys, so a green scan alone cannot prove Glue vended anything.
    let env = match CloudEnv::from_env() {
        Some(e) => e,
        None => {
            println!(
                "SKIPPED: cloud_glue_vends_the_s3_key_pair_for_the_table_location — env vars absent"
            );
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
         required in v1-v3, and the Iceberg reader errors on an absent one rather than \
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
    println!(
        "cloud_glue_vends_the_s3_key_pair_for_the_table_location: table location {anchor} — \
         s3.access-key-id {}, s3.secret-access-key {}, client.region {}, s3.endpoint {}, \
         s3.session-token {}",
        presence_label(access_key_vended),
        presence_label(secret_key_vended),
        presence_label(region_vended),
        presence_label(endpoint_vended),
        presence_label(session_token_vended),
    );
}

/// Scenario: a token- or OAuth2-authenticated REST catalog resolves files and the VS returns rows
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
  NAMESPACE  = '{namespace}'"#
    ));

    let table_part = env
        .catalog_table
        .split('.')
        .next_back()
        .unwrap_or(&env.catalog_table)
        .to_uppercase();
    let vs_table = format!("{auth_vs_name}.{table_part}");

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
}

/// Scenario: a failing credential-bearing DDL on a redacting connection surfaces neither the SQL nor credentials
#[test]
fn cloud_redacting_conn_omits_credentials_on_failure() {
    // Fake sentinels, safe to surface in assertion messages.
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

    // The panic hook is left alone: it is process-wide and tests run in parallel.
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
