//! AWS STS `AssumeRole`: turns a credential set that names an IAM role into
//! the session identity a request acts as.
//!
//! The wire protocol, SigV4 signing, and endpoint resolution belong to the
//! official `aws-sdk-sts` client. This module owns only the `aws_sts_endpoint`
//! consent rule, the request timeout, and the redaction of every error.

use crate::ConnectionCreds;
use crate::creds::non_empty;
use crate::redaction::redact_error_text;
use aws_sdk_sts::config::{
    BehaviorVersion, Credentials, Region, retry::RetryConfig, timeout::TimeoutConfig,
};
use aws_sdk_sts::error::{ProvideErrorMetadata, SdkError};
use exasol_udf_sdk::error::UdfError;
use std::time::Duration;

const STS_TIMEOUT: Duration = Duration::from_secs(30);
const ROLE_SESSION_NAME: &str = "lakehouse-engine";
const UNRESOLVED_SIGNING_REGION: &str = "us-east-1";

/// The credential set a request acts as: `creds` unchanged, with no request,
/// when it names no `aws_assume_role_arn`; otherwise `creds` with only
/// `access_key`, `secret_key`, and `session_token` replaced by the session one
/// STS `AssumeRole` call returns, signed by the stated base key pair.
/// `catalog_uri` selects the signing region, and `allow_http` consents to a
/// plaintext `aws_sts_endpoint`.
///
/// The substitution is safe because no reader after validation wants the base
/// identity: SigV4 signing and static storage read the key triple, and vending
/// never does. Errors are `UdfError::User` and carry no credential or query.
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
        .retry_config(RetryConfig::disabled())
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

/// Built from STS's own status, code, and message only: the SDK's `Debug` and
/// `DisplayErrorContext` renderings embed the raw response and would echo its body.
fn describe_error<E, R>(error: &SdkError<E, R>, timeout: Duration) -> String
where
    E: std::error::Error + ProvideErrorMetadata + Send + Sync + 'static,
    R: ResponseStatus,
{
    match error {
        SdkError::TimeoutError(_) => format!("the request timed out after {timeout:?}"),
        SdkError::DispatchFailure(failure) => failure
            .as_connector_error()
            .map_or_else(|| "the request could not be sent".to_string(), |e| chain(e)),
        SdkError::ConstructionFailure(_) => "the request could not be built".to_string(),
        SdkError::ResponseError(context) => format!(
            "STS returned HTTP {} with an unreadable response",
            context.raw().status_code()
        ),
        SdkError::ServiceError(context) => {
            let mut description = format!("STS returned HTTP {}", context.raw().status_code());
            let meta = context.err().meta();
            for part in [meta.code(), meta.message()].into_iter().flatten() {
                description.push_str(&format!(": {}", part.trim()));
            }
            description
        }
        _ => "the request failed".to_string(),
    }
}

fn chain(error: &dyn std::error::Error) -> String {
    let mut description = error.to_string();
    let mut cause = error.source();
    while let Some(source) = cause {
        description.push_str(&format!(": {source}"));
        cause = source.source();
    }
    description
}

trait ResponseStatus {
    fn status_code(&self) -> u16;
}

impl ResponseStatus for aws_sdk_sts::config::http::HttpResponse {
    fn status_code(&self) -> u16 {
        self.status().as_u16()
    }
}

#[cfg(test)]
#[path = "sts_tests.rs"]
mod tests;
