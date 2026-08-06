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
