//! End-to-end coverage for AWS IAM role assumption (issue #139): a CONNECTION whose base
//! identity is denied the `warehouse` bucket reads it once it also names a role. The
//! `AssumeRole` call is issued by the official AWS SDK against SeaweedFS's STS endpoint, which
//! evaluates the role's trust policy and scopes the session to its attached policy
//! (`seaweedfs-iam.json`). SeaweedFS does not evaluate `sts:ExternalId`; that enforcement is
//! covered by the opt-in real-AWS `cloud_assume_role` tests. Per project rules this suite
//! FAILS (never skips) when the stack is unreachable.
#![cfg(feature = "exasol-e2e")]

mod common;

use common::e2e_harness::{
    VsProps, create_schema_and_scripts, create_virtual_schema_with_password, exa_conn,
    explain_virtual_sql, install_slc, try_create_virtual_schema_with_password, upload_so,
};
use common::raw_parquet::write_parquet_fixture;
use common::seed::{E2E_NAMESPACE, E2E_TABLE, SEED_ROWS_SCORE_GT_15, SEED_TOTAL_ROWS, seed_events};
use common::stack::{
    ASSUME_ROLE_ARN, ASSUME_ROLE_BASE_ACCESS_KEY, ASSUME_ROLE_BASE_SECRET_KEY,
    ASSUME_ROLE_EXTERNAL_ID, ASSUME_ROLE_UNKNOWN_ARN, CatalogConnectionPassword,
    iceberg_catalog_url, iceberg_catalog_url_internal, seaweedfs_url_internal, wait_for_exasol,
    wait_for_iceberg_catalog, wait_for_seaweedfs,
};

use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use std::sync::{Arc, OnceLock};

const UNREACHABLE_STS_ENDPOINT: &str = "http://seaweedfs:1";
const WRONG_BASE_SECRET: &str = "totally-wrong-base-secret-key-value";

const CONN_BASE: &str = "ASSUME_ROLE_BASE_CREDS";
const CONN_ROLE: &str = "ASSUME_ROLE_ROLE_CREDS";
const CONN_DIRECT: &str = "ASSUME_ROLE_DIRECT_CREDS";

const VS_BASE: &str = "ASSUME_ROLE_BASE_VS";
const VS_ROLE: &str = "ASSUME_ROLE_VS";
const VS_DIRECT: &str = "ASSUME_ROLE_DIRECT_VS";

const BASE_DIRECT: &str = "s3://warehouse/assume_role_direct/";

/// A CONNECTION password naming the role, the given external id, and the
/// SeaweedFS STS endpoint as `aws_sts_endpoint`. `access_key`/`secret_key` are always
/// the base identity's — a role CONNECTION requires the base key pair.
fn assume_role_password(secret_key: &str, external_id: &str) -> CatalogConnectionPassword {
    assume_role_password_at(secret_key, external_id, &seaweedfs_url_internal())
}

fn assume_role_password_at(
    secret_key: &str,
    external_id: &str,
    sts_endpoint: &str,
) -> CatalogConnectionPassword {
    CatalogConnectionPassword {
        warehouse: "s3://warehouse/".to_string(),
        endpoint: seaweedfs_url_internal(),
        region: "us-east-1".to_string(),
        access_key: ASSUME_ROLE_BASE_ACCESS_KEY.to_string(),
        secret_key: secret_key.to_string(),
        path_style: true,
        aws_assume_role_arn: Some(ASSUME_ROLE_ARN.to_string()),
        aws_external_id: Some(external_id.to_string()),
        aws_sts_endpoint: Some(sts_endpoint.to_string()),
        ..Default::default()
    }
}

/// A CONNECTION password carrying only the base identity — no role, no STS call.
fn base_only_password() -> CatalogConnectionPassword {
    CatalogConnectionPassword {
        warehouse: "s3://warehouse/".to_string(),
        endpoint: seaweedfs_url_internal(),
        region: "us-east-1".to_string(),
        access_key: ASSUME_ROLE_BASE_ACCESS_KEY.to_string(),
        secret_key: ASSUME_ROLE_BASE_SECRET_KEY.to_string(),
        path_style: true,
        ..Default::default()
    }
}

/// A direct-storage CONNECTION password naming the role: no `warehouse`
/// field (the direct-storage kind rejects it — it reaches no catalog service).
fn direct_storage_role_password() -> CatalogConnectionPassword {
    CatalogConnectionPassword {
        endpoint: seaweedfs_url_internal(),
        region: "us-east-1".to_string(),
        access_key: ASSUME_ROLE_BASE_ACCESS_KEY.to_string(),
        secret_key: ASSUME_ROLE_BASE_SECRET_KEY.to_string(),
        path_style: true,
        aws_assume_role_arn: Some(ASSUME_ROLE_ARN.to_string()),
        aws_external_id: Some(ASSUME_ROLE_EXTERNAL_ID.to_string()),
        aws_sts_endpoint: Some(seaweedfs_url_internal()),
        ..Default::default()
    }
}

fn vs_table(vs_name: &str, table: &str) -> String {
    format!("{vs_name}.{}", table.to_uppercase())
}

fn direct_batch(ids: &[i64]) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(ids.to_vec()))])
        .expect("build direct-storage fixture batch")
}

// ---------------------------------------------------------------------------
// One-time setup: seed fixtures, provision every Virtual Schema.
// ---------------------------------------------------------------------------

static SETUP_DONE: OnceLock<()> = OnceLock::new();

fn setup() {
    SETUP_DONE.get_or_init(|| {
        wait_for_exasol();
        wait_for_seaweedfs();
        wait_for_iceberg_catalog();

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("tokio runtime");
        rt.block_on(async {
            seed_events(&iceberg_catalog_url(), "s3://warehouse/")
                .await
                .expect("seed events table");
        });
        write_parquet_fixture(
            &format!("{BASE_DIRECT}rows/file1.parquet"),
            direct_batch(&[1, 2, 3]),
        );

        install_slc();
        upload_so();

        let mut conn = exa_conn();
        create_schema_and_scripts(&mut conn);

        create_virtual_schema_with_password(
            &mut conn,
            &VsProps::new(VS_BASE, E2E_NAMESPACE).with_catalog_conn_name(CONN_BASE),
            &iceberg_catalog_url_internal(),
            &base_only_password(),
        );
        create_virtual_schema_with_password(
            &mut conn,
            &VsProps::new(VS_ROLE, E2E_NAMESPACE).with_catalog_conn_name(CONN_ROLE),
            &iceberg_catalog_url_internal(),
            &assume_role_password(ASSUME_ROLE_BASE_SECRET_KEY, ASSUME_ROLE_EXTERNAL_ID),
        );
        create_virtual_schema_with_password(
            &mut conn,
            &VsProps::new(VS_DIRECT, "")
                .with_catalog_kind("DIRECT_STORAGE")
                .with_catalog_conn_name(CONN_DIRECT),
            BASE_DIRECT,
            &direct_storage_role_password(),
        );
    });
}

// ---------------------------------------------------------------------------
// Scenarios
// ---------------------------------------------------------------------------

#[test]
fn the_base_identity_alone_is_denied_the_warehouse_bucket() {
    setup();
    let query = format!("SELECT COUNT(*) FROM {}", vs_table(VS_BASE, E2E_TABLE));
    let denied = exa_conn().try_execute(&query);
    assert_eq!(
        denied["status"].as_str(),
        Some("error"),
        "the base identity alone must be denied: {denied}"
    );
    let msg = denied["exception"]["text"].as_str().unwrap_or("");
    assert!(
        msg.to_ascii_lowercase().contains("accessdenied")
            || msg.to_ascii_lowercase().contains("access denied"),
        "the denial must report the store's access-denied response: {msg}"
    );
    assert!(
        !msg.contains(ASSUME_ROLE_BASE_SECRET_KEY),
        "credential leaked in denial: {msg}"
    );
}

#[test]
fn role_connection_reads_the_rows_the_base_identity_was_denied() {
    setup();
    let count_sql = format!("SELECT COUNT(*) FROM {}", vs_table(VS_ROLE, E2E_TABLE));
    let filtered_sql = format!(
        "SELECT COUNT(*) FROM {} WHERE SCORE > 15.0",
        vs_table(VS_ROLE, E2E_TABLE)
    );
    assert_eq!(
        exa_conn().query_scalar_i64(&count_sql),
        SEED_TOTAL_ROWS as i64
    );
    assert_eq!(
        exa_conn().query_scalar_i64(&filtered_sql),
        SEED_ROWS_SCORE_GT_15 as i64
    );

    let plan = explain_virtual_sql(
        &mut exa_conn(),
        &format!(
            "SELECT ID, NAME, SCORE FROM {} WHERE SCORE > 15.0",
            vs_table(VS_ROLE, E2E_TABLE)
        ),
    );
    assert!(
        plan.contains(r#""sealed":{"name":"#),
        "EXPLAIN VIRTUAL must carry the sealed envelope: {plan}"
    );
    for secret in [ASSUME_ROLE_BASE_SECRET_KEY, ASSUME_ROLE_EXTERNAL_ID] {
        assert!(
            !plan.contains(secret),
            "credential or external id leaked in EXPLAIN VIRTUAL: {plan}"
        );
    }
}

#[test]
fn an_unknown_role_arn_fails_create_with_access_denied() {
    setup();
    let mut password = assume_role_password(ASSUME_ROLE_BASE_SECRET_KEY, ASSUME_ROLE_EXTERNAL_ID);
    password.aws_assume_role_arn = Some(ASSUME_ROLE_UNKNOWN_ARN.to_string());
    let resp = try_create_virtual_schema_with_password(
        &mut exa_conn(),
        &VsProps::new("ASSUME_ROLE_UNKNOWN_VS", E2E_NAMESPACE)
            .with_catalog_conn_name("ASSUME_ROLE_UNKNOWN_CREDS"),
        &iceberg_catalog_url_internal(),
        &password,
    );
    assert_eq!(resp["status"].as_str(), Some("error"), "{resp}");
    let msg = resp["exception"]["text"].as_str().unwrap_or("");
    assert!(msg.contains("AccessDenied"), "{msg}");
    assert!(msg.contains(ASSUME_ROLE_UNKNOWN_ARN), "{msg}");
    assert!(!msg.contains(ASSUME_ROLE_BASE_SECRET_KEY), "{msg}");
}

#[test]
fn a_user_outside_the_trust_policy_fails_create_with_access_denied() {
    setup();
    let mut password = assume_role_password(ASSUME_ROLE_BASE_SECRET_KEY, ASSUME_ROLE_EXTERNAL_ID);
    password.access_key = "lhadmin".to_string();
    password.secret_key = "lhadminsecret123".to_string();
    let resp = try_create_virtual_schema_with_password(
        &mut exa_conn(),
        &VsProps::new("ASSUME_ROLE_UNTRUSTED_VS", E2E_NAMESPACE)
            .with_catalog_conn_name("ASSUME_ROLE_UNTRUSTED_CREDS"),
        &iceberg_catalog_url_internal(),
        &password,
    );
    assert_eq!(resp["status"].as_str(), Some("error"), "{resp}");
    let msg = resp["exception"]["text"].as_str().unwrap_or("");
    assert!(msg.contains("AccessDenied"), "{msg}");
    assert!(msg.contains(ASSUME_ROLE_ARN), "{msg}");
    assert!(!msg.contains("lhadminsecret123"), "{msg}");
}

#[test]
fn an_unreachable_sts_endpoint_fails_create_naming_the_host_without_leaking_secrets() {
    setup();
    let resp = try_create_virtual_schema_with_password(
        &mut exa_conn(),
        &VsProps::new("ASSUME_ROLE_BAD_STS_VS", E2E_NAMESPACE)
            .with_catalog_conn_name("ASSUME_ROLE_BAD_STS_CREDS"),
        &iceberg_catalog_url_internal(),
        &assume_role_password_at(
            ASSUME_ROLE_BASE_SECRET_KEY,
            ASSUME_ROLE_EXTERNAL_ID,
            UNREACHABLE_STS_ENDPOINT,
        ),
    );
    assert_eq!(
        resp["status"].as_str(),
        Some("error"),
        "a role CONNECTION must reach its STS endpoint at CREATE: {resp}"
    );
    let msg = resp["exception"]["text"].as_str().unwrap_or("");
    assert!(msg.contains(ASSUME_ROLE_ARN), "{msg}");
    assert!(msg.contains("seaweedfs:1"), "{msg}");
    for secret in [ASSUME_ROLE_BASE_SECRET_KEY, ASSUME_ROLE_EXTERNAL_ID] {
        assert!(!msg.contains(secret), "credential leaked: {msg}");
    }
}

#[test]
fn a_wrong_base_secret_fails_create_with_http_403() {
    setup();
    let resp = try_create_virtual_schema_with_password(
        &mut exa_conn(),
        &VsProps::new("ASSUME_ROLE_BAD_SECRET_VS", E2E_NAMESPACE)
            .with_catalog_conn_name("ASSUME_ROLE_BAD_SECRET_CREDS"),
        &iceberg_catalog_url_internal(),
        &assume_role_password(WRONG_BASE_SECRET, ASSUME_ROLE_EXTERNAL_ID),
    );
    assert_eq!(
        resp["status"].as_str(),
        Some("error"),
        "a wrong base secret must fail CREATE VIRTUAL SCHEMA: {resp}"
    );
    let msg = resp["exception"]["text"].as_str().unwrap_or("");
    assert!(msg.contains("403"), "{msg}");
    assert!(
        !msg.contains(WRONG_BASE_SECRET),
        "wrong secret leaked: {msg}"
    );
    assert!(
        !msg.contains(ASSUME_ROLE_BASE_SECRET_KEY),
        "base secret leaked: {msg}"
    );
}

#[test]
fn direct_storage_role_connection_lists_and_reads_through_the_session() {
    setup();
    // Setup's own `create_virtual_schema_with_password` for VS_DIRECT already
    // proves the CREATE-time listing succeeded (DIRECT_STORAGE lists the
    // bucket at create time; it would have panicked under the base identity
    // alone). This asserts the read path too.
    assert_eq!(
        exa_conn().query_row_count(&format!("SELECT * FROM {VS_DIRECT}.ROWS")),
        3
    );
}
