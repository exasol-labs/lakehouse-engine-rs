//! SigV4 request-signing helper for AWS Glue catalog REST requests.
//!
//! Signs a `reqwest::Request` with an AWS SigV4 `Authorization` header. This is the
//! signing mechanism behind `iceberg_io`'s authenticated GET and `namespace`'s signed
//! enumeration; crate-private by design.
//!
//! Credential safety guarantees:
//!   - `aws_credential_types::Credentials` redacts `secret_access_key` in its `Debug`
//!     impl ("** redacted **") — the test `credentials_debug_redacts_secret` verifies this.
//!   - This module never stores raw key material in any struct. Keys are accepted as
//!     short-lived `&str` function parameters and are handed directly to the signing library.
//!   - `SigningError` from aws-sigv4 carries no credential fields.
use aws_credential_types::Credentials;
use aws_sigv4::http_request::{
    PayloadChecksumKind, SignableBody, SignableRequest, SigningError, SigningSettings, sign,
};
use aws_sigv4::sign::v4;
use aws_smithy_runtime_api::client::identity::Identity;
use std::time::SystemTime;

/// Sign a `reqwest::Request` for the given AWS service with SigV4 header-based signing.
///
/// Produces an `Authorization` header (`AWS4-HMAC-SHA256`), an `x-amz-date` header,
/// and — when a session token is present — an `x-amz-security-token` header.
///
/// Signing keys are never stored, logged, or embedded in any error message.
///
/// Crate-private: signing is a mechanism of this crate's catalog and storage
/// access, never a service it offers outward. Keeping it inside the crate also
/// keeps `aws_sigv4`'s `SigningError` off the public surface.
pub(crate) fn sign_request(
    mut request: reqwest::Request,
    access_key: &str,
    secret_key: &str,
    session_token: Option<&str>,
    region: &str,
    service: &str,
) -> Result<reqwest::Request, SigningError> {
    // Build credentials. `Credentials::Debug` redacts `secret_access_key` automatically.
    let creds = Credentials::new(
        access_key,
        secret_key,
        session_token.map(|s| s.to_string()),
        None,
        "lakehouse-engine",
    );
    let identity: Identity = creds.into();

    let mut settings = SigningSettings::default();
    // Emit `x-amz-content-sha256` and sign over the actual body hash. Without this,
    // signing UnsignedPayload but sending no such header makes AWS Glue recompute a
    // different payload hash → canonical-request mismatch → 403 SignatureDoesNotMatch.
    settings.payload_checksum_kind = PayloadChecksumKind::XAmzSha256;
    // All builder fields are set; `.expect` is unreachable.
    let params: aws_sigv4::http_request::SigningParams<'_> = v4::SigningParams::builder()
        .identity(&identity)
        .region(region)
        .name(service)
        .time(SystemTime::now())
        .settings(settings)
        .build()
        .expect("all SigningParams fields are set")
        .into();

    let url = request.url().to_string();

    // SigV4 canonicalization requires a `host` header. reqwest populates it
    // automatically on the wire, but we must supply it explicitly here because we
    // are constructing the SignableRequest before the HTTP stack adds it.
    let host_value: String = {
        let h = request.url().host_str().unwrap_or("");
        match request.url().port() {
            Some(p) => format!("{h}:{p}"),
            None => h.to_string(),
        }
    };

    // Collect existing headers plus the synthetic host header.
    let existing: Vec<(String, String)> = request
        .headers()
        .iter()
        .filter_map(|(name, value)| {
            value
                .to_str()
                .ok()
                .map(|v| (name.as_str().to_string(), v.to_string()))
        })
        .collect();

    let mut header_pairs: Vec<(&str, &str)> = vec![("host", host_value.as_str())];
    for (k, v) in &existing {
        header_pairs.push((k.as_str(), v.as_str()));
    }

    let signable = SignableRequest::new(
        request.method().as_str(),
        &url,
        header_pairs.into_iter(),
        // Read-path requests (loadTable / listTables / config) are GETs with no body.
        // Sign over the empty-body SHA256 (a constant) so Glue's recomputed canonical
        // request matches; paired with XAmzSha256 above it also sends the header.
        SignableBody::Bytes(&[]),
    )?;

    let (instructions, _signature) = sign(signable, &params)?.into_parts();

    // Apply Authorization + x-amz-date (+ optional x-amz-security-token) to the request.
    // aws-sigv4 only emits well-known header names and hex/base64-encoded ASCII values,
    // so both `parse()` calls are safe to unwrap.
    for (name, value) in instructions.headers() {
        let header_name = name
            .parse::<reqwest::header::HeaderName>()
            .expect("aws-sigv4 always emits valid header names");
        let header_value = reqwest::header::HeaderValue::from_str(value)
            .expect("aws-sigv4 always emits ASCII-safe header values");
        request.headers_mut().insert(header_name, header_value);
    }

    Ok(request)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[path = "sigv4_tests.rs"]
mod tests;
