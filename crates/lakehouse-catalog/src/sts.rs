//! Redaction and the `aws_sts_endpoint` consent rule wrap the official `aws-sdk-sts` client.

use crate::aws_error::{SdkFailure, sdk_failure};
use crate::creds::non_empty;
use crate::expiring::{ExpiringCredential, Lease, system_clock};
use crate::redaction::redact_error_text;
use crate::{ConnectionCreds, aws_credential::aws_lease};
use aws_sdk_sts::config::{BehaviorVersion, Credentials, Region, timeout::TimeoutConfig};
use aws_sdk_sts::error::{ProvideErrorMetadata, SdkError};
use exasol_udf_sdk::error::UdfError;
use iceberg_storage_opendal::AwsCredential;
use std::sync::Arc;
use std::time::Duration;

const STS_TIMEOUT: Duration = Duration::from_secs(30);
/// Assume the role again this long before its credentials expire.
const RENEWAL_SKEW: Duration = Duration::from_secs(5 * 60);
const ROLE_SESSION_NAME: &str = "lakehouse-engine";
const UNRESOLVED_SIGNING_REGION: &str = "us-east-1";

/// The CONNECTION's identity with a named role assumed: `creds` carry the session's key triple, as
/// a scan reads them, and `role` is the source that keeps the CONNECTION's base keys and assumes
/// the role again, for a writer that outlives the session. `role` is `None` when the CONNECTION
/// names no role. Errors carry no credential or query.
pub struct ResolvedIdentity {
    pub creds: ConnectionCreds,
    pub role: Option<AssumedRole>,
}

pub async fn resolve_aws_identity(
    creds: ConnectionCreds,
    catalog_uri: &str,
    allow_http: bool,
) -> Result<ResolvedIdentity, UdfError> {
    resolve_aws_identity_within(creds, catalog_uri, allow_http, STS_TIMEOUT).await
}

pub(crate) async fn resolve_aws_identity_within(
    creds: ConnectionCreds,
    catalog_uri: &str,
    allow_http: bool,
    timeout: Duration,
) -> Result<ResolvedIdentity, UdfError> {
    let Some(role_arn) = creds.assume_role_arn().map(str::to_string) else {
        return Ok(ResolvedIdentity { creds, role: None });
    };
    let redacted = |msg: String| UdfError::User(redact_error_text(&msg, &base_secrets(&creds)));
    let endpoint = non_empty(&creds.aws_sts_endpoint)
        .map(|stated| stated_endpoint(stated, allow_http))
        .transpose()
        .map_err(redacted)?;
    let region = creds
        .sigv4_signing_region(catalog_uri)
        .unwrap_or_else(|| UNRESOLVED_SIGNING_REGION.to_string());
    let clock = system_clock();
    let params = RoleParams {
        base: creds,
        role_arn,
        endpoint,
        region,
        timeout,
    };
    let first = params
        .assume(clock())
        .await
        .map_err(|msg| params.redacted(msg))?;
    let session = ConnectionCreds {
        access_key: first.value.access_key_id.clone(),
        secret_key: first.value.secret_access_key.clone(),
        session_token: first.value.session_token.clone(),
        ..params.base.clone()
    };
    let holder = ExpiringCredential::seeded(first, RENEWAL_SKEW, clock);
    Ok(ResolvedIdentity {
        creds: session,
        role: Some(AssumedRole {
            inner: Arc::new(RoleSource { params, holder }),
        }),
    })
}

/// The credentials of an assumed IAM role, assumed again before they expire. One holder serves
/// every reader of the role: Glue's SigV4 catalog signing and a writer's storage requests. A failed
/// renewal fails the request with its cause.
#[derive(Clone)]
pub struct AssumedRole {
    inner: Arc<RoleSource>,
}

struct RoleSource {
    params: RoleParams,
    holder: ExpiringCredential<AwsCredential>,
}

/// What one AssumeRole call needs; `base` holds the CONNECTION's own keys.
struct RoleParams {
    base: ConnectionCreds,
    role_arn: String,
    endpoint: Option<url::Url>,
    region: String,
    timeout: Duration,
}

impl AssumedRole {
    /// The role's credentials, assumed again first when they are within five minutes of expiry.
    pub async fn credential(&self) -> Result<AwsCredential, UdfError> {
        let source = &self.inner;
        source
            .holder
            .get(|now_ms| async move {
                source
                    .params
                    .assume(now_ms)
                    .await
                    .map_err(|msg| source.params.redacted(msg))
            })
            .await
    }

    /// The CONNECTION's base secrets and the role credential held now, for value-based redaction.
    pub fn secret_values(&self) -> Vec<String> {
        let source = &self.inner;
        let mut secrets: Vec<String> = base_secrets(&source.params.base)
            .into_iter()
            .map(str::to_string)
            .collect();
        if let Some(held) = source.holder.current() {
            secrets.extend(
                [
                    Some(held.access_key_id),
                    Some(held.secret_access_key),
                    held.session_token,
                ]
                .into_iter()
                .flatten()
                .filter(|value| !value.is_empty()),
            );
        }
        secrets
    }
}

impl RoleParams {
    fn redacted(&self, msg: String) -> UdfError {
        UdfError::User(redact_error_text(&msg, &base_secrets(&self.base)))
    }

    /// Assumes the role; the lease expires on the clock that read `now_ms`.
    async fn assume(&self, now_ms: u64) -> Result<Lease<AwsCredential>, String> {
        let session = assume_role(
            &self.base,
            &self.role_arn,
            self.endpoint.clone(),
            self.region.clone(),
            self.timeout,
        )
        .await?;
        Ok(aws_lease(
            session.access_key,
            session.secret_key,
            Some(session.session_token),
            session
                .expires_in_secs
                .map(|secs| now_ms.saturating_add(secs.saturating_mul(1000))),
        ))
    }
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
    /// Seconds from the response until the credentials expire, `None` when STS states no expiry.
    expires_in_secs: Option<u64>,
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
        expires_in_secs: lifetime_secs(session.expiration()),
    })
}

fn lifetime_secs(expiration: &aws_smithy_types::DateTime) -> Option<u64> {
    let at = std::time::SystemTime::try_from(*expiration).ok()?;
    Some(
        at.duration_since(std::time::SystemTime::now())
            .unwrap_or_default()
            .as_secs(),
    )
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
