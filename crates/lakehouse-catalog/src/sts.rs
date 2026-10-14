//! Redaction and the `aws_sts_endpoint` consent rule wrap the official `aws-sdk-sts` client.

use crate::ConnectionCreds;
use crate::aws_error::{SdkFailure, sdk_failure};
use crate::creds::non_empty;
use crate::redaction::redact_error_text;
use aws_sdk_sts::config::{BehaviorVersion, Credentials, Region, timeout::TimeoutConfig};
use aws_sdk_sts::error::{ProvideErrorMetadata, SdkError};
use exasol_udf_sdk::error::UdfError;
use std::time::Duration;

const STS_TIMEOUT: Duration = Duration::from_secs(30);
const ROLE_SESSION_NAME: &str = "lakehouse-engine";
const UNRESOLVED_SIGNING_REGION: &str = "us-east-1";

/// Substituting the session key triple is safe: no reader after validation wants the base
/// identity. Errors carry no credential or query.
pub async fn resolve_aws_identity(
    creds: ConnectionCreds,
    catalog_uri: &str,
    allow_http: bool,
) -> Result<ConnectionCreds, UdfError> {
    resolve_aws_identity_within(creds, catalog_uri, allow_http, STS_TIMEOUT).await
}

pub(crate) async fn resolve_aws_identity_within(
    creds: ConnectionCreds,
    catalog_uri: &str,
    allow_http: bool,
    timeout: Duration,
) -> Result<ConnectionCreds, UdfError> {
    let Some(role_arn) = creds.assume_role_arn() else {
        return Ok(creds);
    };
    let redacted = |msg: String| UdfError::User(redact_error_text(&msg, &base_secrets(&creds)));
    let endpoint = non_empty(&creds.aws_sts_endpoint)
        .map(|stated| stated_endpoint(stated, allow_http))
        .transpose()
        .map_err(redacted)?;
    let region = creds
        .sigv4_signing_region(catalog_uri)
        .unwrap_or_else(|| UNRESOLVED_SIGNING_REGION.to_string());
    let session = assume_role(&creds, role_arn, endpoint, region, timeout)
        .await
        .map_err(redacted)?;
    Ok(ConnectionCreds {
        access_key: session.access_key,
        secret_key: session.secret_key,
        session_token: Some(session.session_token),
        ..creds
    })
}

fn base_secrets(creds: &ConnectionCreds) -> Vec<&str> {
    [
        Some(creds.secret_key.as_str()),
        non_empty(&creds.session_token),
        non_empty(&creds.aws_external_id),
    ]
    .into_iter()
    .flatten()
    .collect()
}

struct SessionCredentials {
    access_key: String,
    secret_key: String,
    session_token: String,
}

async fn assume_role(
    creds: &ConnectionCreds,
    role_arn: &str,
    endpoint: Option<url::Url>,
    region: String,
    timeout: Duration,
) -> Result<SessionCredentials, String> {
    let target = endpoint
        .as_ref()
        .map_or_else(|| "the regional STS endpoint".to_string(), endpoint_host);
    let failure = |reason: String| {
        format!("AWS STS AssumeRole of role '{role_arn}' at {target} failed: {reason}")
    };

    let base = Credentials::new(
        &creds.access_key,
        &creds.secret_key,
        non_empty(&creds.session_token).map(str::to_string),
        None,
        "lakehouse-connection",
    );
    let mut config = aws_sdk_sts::Config::builder()
        .behavior_version(BehaviorVersion::latest())
        .region(Region::new(region))
        .credentials_provider(base)
        .timeout_config(TimeoutConfig::builder().operation_timeout(timeout).build());
    if let Some(endpoint) = endpoint {
        config = config.endpoint_url(endpoint);
    }
    let client = aws_sdk_sts::Client::from_conf(config.build());

    let output = client
        .assume_role()
        .role_arn(role_arn)
        .role_session_name(ROLE_SESSION_NAME)
        .set_external_id(non_empty(&creds.aws_external_id).map(str::to_string))
        .send()
        .await
        .map_err(|error| failure(describe_error(&error, timeout)))?;

    let session = output
        .credentials()
        .ok_or_else(|| failure("the STS response carries no Credentials".into()))?;
    for (element, value) in [
        ("AccessKeyId", session.access_key_id()),
        ("SecretAccessKey", session.secret_access_key()),
        ("SessionToken", session.session_token()),
    ] {
        if value.trim().is_empty() {
            return Err(failure(format!(
                "the STS response carries no Credentials/{element}"
            )));
        }
    }
    Ok(SessionCredentials {
        access_key: session.access_key_id().trim().to_string(),
        secret_key: session.secret_access_key().trim().to_string(),
        session_token: session.session_token().trim().to_string(),
    })
}

fn endpoint_host(url: &url::Url) -> String {
    let host = url.host_str().unwrap_or_default();
    match url.port() {
        Some(port) => format!("{host}:{port}"),
        None => host.to_string(),
    }
}

fn stated_endpoint(stated: &str, allow_http: bool) -> Result<url::Url, String> {
    let url =
        url::Url::parse(stated).map_err(|e| format!("aws_sts_endpoint is not a valid URL: {e}"))?;
    match url.scheme() {
        "https" => Ok(url),
        "http" if allow_http => Ok(url),
        "http" => Err(format!(
            "aws_sts_endpoint {} uses plaintext http, which requires the ALLOW_HTTP virtual \
             schema property to be true, because the STS response carries the session secret",
            url.host_str().unwrap_or_default()
        )),
        other => Err(format!(
            "aws_sts_endpoint must use the https or http scheme, not '{other}'"
        )),
    }
}

fn describe_error<E: ProvideErrorMetadata>(error: &SdkError<E>, timeout: Duration) -> String {
    match sdk_failure(error, "STS") {
        SdkFailure::Service {
            status,
            code,
            message,
        } => {
            let mut description = format!("STS returned HTTP {status}");
            for part in [code, message].into_iter().flatten() {
                description.push_str(&format!(": {}", part.trim()));
            }
            description
        }
        SdkFailure::TimedOut => format!("the request timed out after {timeout:?}"),
        SdkFailure::Request(detail) => detail,
    }
}

#[cfg(test)]
#[path = "sts_tests.rs"]
mod tests;
