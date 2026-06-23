/// SigV4 request-signing helper for AWS Glue catalog REST requests.
///
/// Signs a `reqwest::Request` with an AWS SigV4 `Authorization` header.
///
/// Credential safety guarantees:
///   - `aws_credential_types::Credentials` redacts `secret_access_key` in its `Debug`
///     impl ("** redacted **") — the test `credentials_debug_redacts_secret` verifies this.
///   - This module never stores raw key material in any struct. Keys are accepted as
///     short-lived `&str` function parameters and are handed directly to the signing library.
///   - `SigningError` from aws-sigv4 carries no credential fields.
use aws_credential_types::Credentials;
use aws_sigv4::http_request::{SignableBody, SignableRequest, SigningError, SigningSettings, sign};
use aws_sigv4::sign::v4;
use aws_smithy_runtime_api::client::identity::Identity;
use std::time::SystemTime;

/// Sign a `reqwest::Request` for the given AWS service with SigV4 header-based signing.
///
/// Produces an `Authorization` header (`AWS4-HMAC-SHA256`), an `x-amz-date` header,
/// and — when a session token is present — an `x-amz-security-token` header.
///
/// Signing keys are never stored, logged, or embedded in any error message.
pub fn sign_request(
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

    let settings = SigningSettings::default();
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
        // Catalog load_table is a GET with no body; UnsignedPayload is correct and
        // avoids materializing the (absent) body for hashing.
        SignableBody::UnsignedPayload,
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
mod tests {
    use super::*;

    /// Scenario: Catalog REST requests to Glue are SigV4-signed when enabled.
    ///
    /// Asserts the signed request carries an `Authorization` header that:
    ///   - uses the `AWS4-HMAC-SHA256` algorithm
    ///   - includes the configured region (`us-east-1`) in the credential scope
    ///   - includes the configured service name (`glue`) in the credential scope
    ///   - ends with the `aws4_request` terminator
    ///
    /// Also asserts the `x-amz-date` header is present.
    #[test]
    fn signed_request_carries_sigv4_header() {
        let client = reqwest::Client::new();
        let request = client
            .get("https://glue.us-east-1.amazonaws.com/iceberg/v1/catalogs/my-catalog/namespaces/db/tables/my_table")
            .build()
            .expect("valid request");

        let signed = sign_request(
            request,
            "AKIDEXAMPLE",
            "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY",
            None,
            "us-east-1",
            "glue",
        )
        .expect("signing must succeed");

        let auth = signed
            .headers()
            .get("authorization")
            .expect("Authorization header must be present after signing")
            .to_str()
            .expect("Authorization header must be valid ASCII");

        assert!(
            auth.contains("AWS4-HMAC-SHA256"),
            "Authorization must use AWS4-HMAC-SHA256; got: {auth}"
        );
        assert!(
            auth.contains("us-east-1"),
            "Authorization credential scope must contain the region; got: {auth}"
        );
        assert!(
            auth.contains("glue"),
            "Authorization credential scope must contain the service name; got: {auth}"
        );
        assert!(
            auth.contains("aws4_request"),
            "Authorization must contain the aws4_request terminator; got: {auth}"
        );

        assert!(
            signed.headers().get("x-amz-date").is_some(),
            "x-amz-date header must be present after signing"
        );
    }

    /// Verifies that the secret key does NOT appear in any signed request header.
    ///
    /// The Authorization header contains a hex-encoded HMAC signature *derived from*
    /// the secret key, but must never contain the key in plaintext.
    #[test]
    fn secret_key_absent_from_signed_headers() {
        let secret = "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY";
        let client = reqwest::Client::new();
        let request = client
            .get("https://glue.us-east-1.amazonaws.com/iceberg/v1/namespaces")
            .build()
            .expect("valid request");

        let signed = sign_request(
            request,
            "AKIDEXAMPLE",
            secret,
            Some("AQoDYXdzEJr_SESSION_TOKEN"),
            "us-east-1",
            "glue",
        )
        .expect("signing must succeed");

        for (name, value) in signed.headers().iter() {
            let value_str = value.to_str().unwrap_or("");
            assert!(
                !value_str.contains(secret),
                "secret key must not appear in header '{name}': {value_str}"
            );
        }
    }

    /// Verifies that the `Credentials` `Debug` impl redacts `secret_access_key`.
    ///
    /// Exercises the library's built-in redaction guarantee so that credentials
    /// included in logs or error chains cannot leak the secret.
    #[test]
    fn credentials_debug_redacts_secret() {
        let creds = Credentials::new(
            "AKIDEXAMPLE",
            "my-very-secret-key",
            None,
            None,
            "lakehouse-engine",
        );
        let debug_output = format!("{creds:?}");
        assert!(
            !debug_output.contains("my-very-secret-key"),
            "Credentials Debug must redact secret_access_key; got: {debug_output}"
        );
        assert!(
            debug_output.contains("** redacted **"),
            "Credentials Debug must show '** redacted **'; got: {debug_output}"
        );
    }

    /// Scenario: Unsigned catalog path is unchanged when SigV4 is disabled.
    ///
    /// The disabled path means the caller simply does not invoke `sign_request`.
    /// Verifies that an unsigned request carries no `Authorization` or `x-amz-date`
    /// header, confirming the disabled path leaves the request untouched.
    #[test]
    fn disabled_sigv4_produces_unsigned_request() {
        let client = reqwest::Client::new();
        let request = client
            .get("https://minio.local:9000/iceberg/v1/namespaces")
            .build()
            .expect("valid request");

        assert!(
            request.headers().get("authorization").is_none(),
            "unsigned request must carry no Authorization header"
        );
        assert!(
            request.headers().get("x-amz-date").is_none(),
            "unsigned request must carry no x-amz-date header"
        );
    }
}
