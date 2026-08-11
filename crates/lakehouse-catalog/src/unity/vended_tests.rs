//! Tests for `resolve_uc_vended_storage`: the S3 and ADLS terminations, the
//! scheme-only variant selection, and the unsupported-scheme, missing-credential,
//! and plaintext-consent error paths — all pure, no network. Every error path is
//! asserted credential-safe.

use super::*;
use crate::{AdlsCred, StorageBackend};
use exasol_udf_sdk::error::UdfError;

const SECRET_KEY_SENTINEL: &str = "SECRET_ACCESS_KEY_SENTINEL_VALUE";
const SESSION_TOKEN_SENTINEL: &str = "SESSION_TOKEN_SENTINEL_VALUE";
const SAS_SENTINEL: &str = "sv=2021-08-06&sig=SAS_SENTINEL_VALUE";

fn aws_response(
    access: &str,
    secret: &str,
    token: Option<&str>,
    endpoint: Option<&str>,
) -> TemporaryTableCredentials {
    TemporaryTableCredentials {
        aws_temp_credentials: Some(AwsTempCredentials {
            access_key_id: access.to_string(),
            secret_access_key: secret.to_string(),
            session_token: token.map(str::to_string),
            endpoint: endpoint.map(str::to_string),
        }),
        azure_user_delegation_sas: None,
        gcp_oauth_token: None,
    }
}

fn azure_response(sas: &str) -> TemporaryTableCredentials {
    TemporaryTableCredentials {
        aws_temp_credentials: None,
        azure_user_delegation_sas: Some(AzureUserDelegationSas {
            sas_token: sas.to_string(),
        }),
        gcp_oauth_token: None,
    }
}

/// A response carrying BOTH credential families, so the variant a location
/// resolves to is proven to come from its scheme, not from the family present.
fn both_families() -> TemporaryTableCredentials {
    TemporaryTableCredentials {
        aws_temp_credentials: Some(AwsTempCredentials {
            access_key_id: "AK".to_string(),
            secret_access_key: "SK".to_string(),
            session_token: None,
            endpoint: None,
        }),
        azure_user_delegation_sas: Some(AzureUserDelegationSas {
            sas_token: "sas".to_string(),
        }),
        gcp_oauth_token: None,
    }
}

fn user_message(err: UdfError) -> String {
    match err {
        UdfError::User(msg) => msg,
        _ => panic!("expected a UdfError::User variant"),
    }
}

#[test]
fn s3_vended_response_terminates_in_s3_backend() {
    let response = aws_response(
        "ASIAKEY",
        SECRET_KEY_SENTINEL,
        Some(SESSION_TOKEN_SENTINEL),
        None,
    );

    let backend = resolve_uc_vended_storage(&response, "s3://bucket/orders", false)
        .expect("s3 vended resolves");

    match backend {
        StorageBackend::S3(props) => {
            assert_eq!(props.access_key, "ASIAKEY");
            assert_eq!(props.secret_key, SECRET_KEY_SENTINEL);
            assert_eq!(props.session_token.as_deref(), Some(SESSION_TOKEN_SENTINEL));
            assert!(
                props.endpoint.is_empty(),
                "no endpoint vended -> empty, no CONNECTION endpoint read"
            );
            assert!(props.region.is_empty(), "no CONNECTION region read");
        }
        StorageBackend::Adls { .. } => panic!("expected the S3 backend"),
    }
}

#[test]
fn adls_vended_response_terminates_in_adls_backend() {
    let response = azure_response(SAS_SENTINEL);

    let backend = resolve_uc_vended_storage(
        &response,
        "abfss://container@myacct.dfs.core.windows.net/path",
        false,
    )
    .expect("adls vended resolves");

    match backend {
        StorageBackend::Adls { account_name, cred } => {
            assert_eq!(
                account_name, "myacct",
                "account name recovered from the host"
            );
            assert!(matches!(cred, AdlsCred::Sas(sas) if sas == SAS_SENTINEL));
        }
        StorageBackend::S3(_) => panic!("expected the ADLS backend"),
    }
}

#[test]
fn variant_selected_from_location_scheme() {
    for scheme in ["s3", "s3a"] {
        let backend =
            resolve_uc_vended_storage(&both_families(), &format!("{scheme}://bucket/tbl"), false)
                .expect("resolves");
        assert!(
            matches!(backend, StorageBackend::S3(_)),
            "{scheme}:// selects S3 from the scheme alone"
        );
    }
    for scheme in ["abfs", "abfss"] {
        let location = format!("{scheme}://c@acct.dfs.core.windows.net/p");
        let backend =
            resolve_uc_vended_storage(&both_families(), &location, false).expect("resolves");
        assert!(
            matches!(backend, StorageBackend::Adls { .. }),
            "{scheme}:// selects ADLS from the scheme alone"
        );
    }
}

#[test]
fn unsupported_scheme_is_error() {
    let response = TemporaryTableCredentials {
        aws_temp_credentials: None,
        azure_user_delegation_sas: None,
        gcp_oauth_token: Some(GcpOauthToken {
            oauth_token: "gcp-secret-token".to_string(),
        }),
    };

    let msg =
        user_message(resolve_uc_vended_storage(&response, "gs://bucket/tbl", false).unwrap_err());

    assert!(
        msg.contains("gs://bucket/tbl"),
        "names the unsupported location: {msg}"
    );
    assert!(
        !msg.contains("gcp-secret-token"),
        "no credential value in the error: {msg}"
    );
}

#[test]
fn missing_matching_credential_is_error() {
    // An S3 location whose response carries only ADLS credentials.
    let s3_msg = user_message(
        resolve_uc_vended_storage(&azure_response(SAS_SENTINEL), "s3://bucket/tbl", false)
            .unwrap_err(),
    );
    assert!(s3_msg.contains("s3://bucket/tbl"));
    assert!(
        !s3_msg.contains(SAS_SENTINEL),
        "no vended secret in the error: {s3_msg}"
    );

    // An ADLS location whose response carries only S3 credentials.
    let adls_msg = user_message(
        resolve_uc_vended_storage(
            &aws_response("AK", SECRET_KEY_SENTINEL, None, None),
            "abfss://c@acct.dfs.core.windows.net/p",
            false,
        )
        .unwrap_err(),
    );
    assert!(adls_msg.contains("acct.dfs.core.windows.net"));
    assert!(
        !adls_msg.contains(SECRET_KEY_SENTINEL),
        "no vended secret in the error: {adls_msg}"
    );
}

#[test]
fn plaintext_endpoint_requires_allow_http() {
    let response = aws_response(
        "AK",
        SECRET_KEY_SENTINEL,
        Some(SESSION_TOKEN_SENTINEL),
        Some("http://minio:9000"),
    );

    let msg =
        user_message(resolve_uc_vended_storage(&response, "s3://bucket/tbl", false).unwrap_err());
    assert!(
        msg.contains("ALLOW_HTTP"),
        "names the ALLOW_HTTP property: {msg}"
    );
    assert!(
        msg.contains("http://minio:9000"),
        "names the plaintext endpoint: {msg}"
    );
    assert!(
        !msg.contains(SECRET_KEY_SENTINEL) && !msg.contains(SESSION_TOKEN_SENTINEL),
        "no vended secret in the error: {msg}"
    );

    // Honored with the operator's explicit consent.
    let backend = resolve_uc_vended_storage(&response, "s3://bucket/tbl", true)
        .expect("resolves with allow_http");
    match backend {
        StorageBackend::S3(props) => {
            assert_eq!(props.endpoint, "http://minio:9000");
            assert!(props.allow_http);
        }
        StorageBackend::Adls { .. } => panic!("expected the S3 backend"),
    }
}
