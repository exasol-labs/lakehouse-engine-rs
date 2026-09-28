//! AWS STS `AssumeRole`: turns a credential set that names an IAM role into
//! the session identity a request acts as.
//!
//! Owns the whole STS protocol: the endpoint and signing-region rule, the
//! Query-API `GET` and its percent-encoding, SigV4 signing for service `sts`,
//! the request timeout, the XML response, and the redaction of every error.

use crate::ConnectionCreds;
use crate::creds::non_empty;
use crate::redaction::redact_error_text;
use crate::sigv4::sign_request;
use exasol_udf_sdk::error::UdfError;
use serde::Deserialize;
use std::time::Duration;

const STS_TIMEOUT: Duration = Duration::from_secs(30);
const API_VERSION: &str = "2011-06-15";
const ROLE_SESSION_NAME: &str = "lakehouse-engine";
const GLOBAL_ENDPOINT: &str = "https://sts.amazonaws.com";
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
    let endpoint = StsEndpoint {
        timeout,
        ..StsEndpoint::resolve(&creds, catalog_uri, allow_http).map_err(redacted)?
    };
    let session = assume_role(&creds, role_arn, &endpoint)
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

async fn assume_role(
    creds: &ConnectionCreds,
    role_arn: &str,
    endpoint: &StsEndpoint,
) -> Result<SessionCredentials, String> {
    let failure = |reason: String| {
        format!(
            "AWS STS AssumeRole of role '{role_arn}' at {} failed: {reason}",
            endpoint.host()
        )
    };
    let describe =
        |error: reqwest::Error| failure(describe_transport_error(error, endpoint.timeout));

    let client = reqwest::Client::builder()
        .timeout(endpoint.timeout)
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .map_err(describe)?;
    let mut url = endpoint.url.clone();
    url.set_query(Some(&assume_role_query(
        role_arn,
        non_empty(&creds.aws_external_id),
    )));
    let request = client.get(url).build().map_err(describe)?;
    let signed = sign_request(
        request,
        &creds.access_key,
        &creds.secret_key,
        non_empty(&creds.session_token),
        &endpoint.region,
        "sts",
    )
    .map_err(|e| failure(format!("the request could not be signed: {e}")))?;

    let response = client.execute(signed).await.map_err(describe)?;
    let status = response.status();
    let body = response.text().await.map_err(describe)?;
    if !status.is_success() {
        return Err(failure(describe_rejection(status, &body)));
    }
    session_credentials(&body).map_err(failure)
}

struct StsEndpoint {
    url: url::Url,
    region: String,
    timeout: Duration,
}

impl StsEndpoint {
    fn resolve(
        creds: &ConnectionCreds,
        catalog_uri: &str,
        allow_http: bool,
    ) -> Result<Self, String> {
        let region = creds.sigv4_signing_region(catalog_uri);
        let url = match non_empty(&creds.aws_sts_endpoint) {
            Some(stated) => stated_endpoint(stated, allow_http)?,
            None => default_endpoint(region.as_deref())?,
        };
        Ok(Self {
            url,
            region: region.unwrap_or_else(|| UNRESOLVED_SIGNING_REGION.to_string()),
            timeout: STS_TIMEOUT,
        })
    }

    fn host(&self) -> String {
        let host = self.url.host_str().unwrap_or_default();
        match self.url.port() {
            Some(port) => format!("{host}:{port}"),
            None => host.to_string(),
        }
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

fn default_endpoint(region: Option<&str>) -> Result<url::Url, String> {
    let mut url = url::Url::parse(GLOBAL_ENDPOINT).expect("the global STS endpoint is a URL");
    if let Some(region) = region {
        url.set_host(Some(&format!("sts.{region}.amazonaws.com")))
            .map_err(|_| {
                format!(
                    "region '{region}' does not form a valid STS endpoint host; state \
                     aws_sts_endpoint instead"
                )
            })?;
    }
    Ok(url)
}

fn assume_role_query(role_arn: &str, external_id: Option<&str>) -> String {
    [
        ("Action", "AssumeRole"),
        ("Version", API_VERSION),
        ("RoleArn", role_arn),
        ("RoleSessionName", ROLE_SESSION_NAME),
    ]
    .into_iter()
    .chain(external_id.map(|id| ("ExternalId", id)))
    .map(|(key, value)| format!("{}={}", encode_rfc3986(key), encode_rfc3986(value)))
    .collect::<Vec<_>>()
    .join("&")
}

/// SigV4's canonical query encoding, so the sent query is byte-identical to the
/// one aws-sigv4 signs and STS recomputes: only RFC 3986 unreserved bytes stay literal.
fn encode_rfc3986(value: &str) -> String {
    let mut encoded = String::with_capacity(value.len());
    for byte in value.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'~') {
            encoded.push(char::from(byte));
        } else {
            encoded.push_str(&format!("%{byte:02X}"));
        }
    }
    encoded
}

fn describe_transport_error(error: reqwest::Error, timeout: Duration) -> String {
    if error.is_timeout() {
        return format!("the request timed out after {timeout:?}");
    }
    let error = error.without_url();
    let mut description = error.to_string();
    let mut cause = std::error::Error::source(&error);
    while let Some(source) = cause {
        description.push_str(&format!(": {source}"));
        cause = source.source();
    }
    description
}

fn describe_rejection(status: reqwest::StatusCode, body: &str) -> String {
    let mut description = format!("STS returned HTTP {}", status.as_u16());
    if let Ok(StsDocument::ErrorResponse(response)) = quick_xml::de::from_str(body) {
        let detail = response.error.unwrap_or_default();
        for part in [detail.code, detail.message].into_iter().flatten() {
            let part = part.trim();
            if !part.is_empty() {
                description.push_str(&format!(": {part}"));
            }
        }
    }
    description
}

fn session_credentials(body: &str) -> Result<SessionCredentials, String> {
    let Ok(StsDocument::AssumeRoleResponse(response)) = quick_xml::de::from_str(body) else {
        return Err("the STS response is not a well-formed AssumeRoleResponse document".into());
    };
    let credentials = response
        .assume_role_result
        .and_then(|result| result.credentials)
        .unwrap_or_default();
    Ok(SessionCredentials {
        access_key: required_element(credentials.access_key_id, "AccessKeyId")?,
        secret_key: required_element(credentials.secret_access_key, "SecretAccessKey")?,
        session_token: required_element(credentials.session_token, "SessionToken")?,
    })
}

fn required_element(value: Option<String>, element: &str) -> Result<String, String> {
    value
        .map(|text| text.trim().to_string())
        .filter(|text| !text.is_empty())
        .ok_or_else(|| {
            format!("the STS AssumeRoleResponse carries no AssumeRoleResult/Credentials/{element}")
        })
}

struct SessionCredentials {
    access_key: String,
    secret_key: String,
    session_token: String,
}

#[derive(Deserialize)]
enum StsDocument {
    AssumeRoleResponse(AssumeRoleResponse),
    ErrorResponse(ErrorResponse),
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct AssumeRoleResponse {
    assume_role_result: Option<AssumeRoleResult>,
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct AssumeRoleResult {
    credentials: Option<CredentialsElement>,
}

#[derive(Default, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct CredentialsElement {
    access_key_id: Option<String>,
    secret_access_key: Option<String>,
    session_token: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct ErrorResponse {
    error: Option<ErrorElement>,
}

#[derive(Default, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct ErrorElement {
    code: Option<String>,
    message: Option<String>,
}

#[cfg(test)]
#[path = "sts_tests.rs"]
mod tests;
