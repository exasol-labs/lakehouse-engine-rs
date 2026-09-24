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

// ---------------------------------------------------------------------------
// Signing-region resolution (ConnectionCreds::sigv4_signing_region)
// ---------------------------------------------------------------------------

fn creds_stating_region(region: &str) -> ConnectionCreds {
    ConnectionCreds {
        region: region.into(),
        ..ConnectionCreds::default()
    }
}

/// Standard commercial AWS Glue endpoint addresses, each with the region its
/// host names: case, port, and path never affect the match.
const STANDARD_GLUE_ENDPOINTS: [(&str, &str); 5] = [
    ("https://glue.eu-west-1.amazonaws.com/iceberg", "eu-west-1"),
    ("https://GLUE.EU-WEST-1.AMAZONAWS.COM/iceberg", "eu-west-1"),
    (
        "https://glue.eu-west-1.amazonaws.com:443/iceberg",
        "eu-west-1",
    ),
    (
        "https://glue.eu-west-1.amazonaws.com:8443/iceberg",
        "eu-west-1",
    ),
    (
        "https://glue.ap-southeast-2.amazonaws.com/iceberg",
        "ap-southeast-2",
    ),
];

/// Addresses that are not a standard commercial AWS Glue endpoint, labelled by
/// the form each one represents.
const NON_STANDARD_ADDRESSES: [(&str, &str); 18] = [
    (
        "GovCloud",
        "https://glue.us-gov-west-1.amazonaws.com/iceberg",
    ),
    ("China", "https://glue.cn-north-1.amazonaws.com.cn/iceberg"),
    ("FIPS", "https://glue-fips.us-east-1.amazonaws.com/iceberg"),
    (
        "VPC interface",
        "https://vpce-0123456789abcdef0-abcdefgh.glue.us-east-1.vpce.amazonaws.com/iceberg",
    ),
    ("dual-stack", "https://glue.us-east-1.api.aws/iceberg"),
    ("private host", "https://catalog.internal.example/iceberg"),
    ("http scheme", "http://glue.us-east-1.amazonaws.com/iceberg"),
    (
        "userinfo form",
        "https://glue.us-east-1.amazonaws.com@evil.example/",
    ),
    (
        "trailing-dot host",
        "https://glue.us-east-1.amazonaws.com./iceberg",
    ),
    (
        "multi-label region",
        "https://glue.us-east-1.extra.amazonaws.com/iceberg",
    ),
    (
        "two-part region",
        "https://glue.us-east.amazonaws.com/iceberg",
    ),
    (
        "three-letter area",
        "https://glue.use-east-1.amazonaws.com/iceberg",
    ),
    (
        "non-digit ordinal",
        "https://glue.us-east-x.amazonaws.com/iceberg",
    ),
    ("empty subarea", "https://glue.us--1.amazonaws.com/iceberg"),
    (
        "empty ordinal",
        "https://glue.us-east-.amazonaws.com/iceberg",
    ),
    ("bare Glue domain", "https://glue.amazonaws.com/iceberg"),
    ("empty URI", ""),
    (
        "unparseable URI",
        "https://glue.us-east-1.amazonaws.com:99999/iceberg",
    ),
];

/// Scenario: A standard AWS Glue endpoint supplies the SigV4 signing region
/// when the CONNECTION omits region.
#[test]
fn standard_glue_endpoint_supplies_the_signing_region_when_none_is_stated() {
    let creds = creds_stating_region("");

    for (uri, expected) in STANDARD_GLUE_ENDPOINTS {
        assert_eq!(
            creds.sigv4_signing_region(uri).as_deref(),
            Some(expected),
            "a standard Glue endpoint must supply its own region: {uri}"
        );
    }
}

/// Scenario: A standard AWS Glue endpoint signs the catalog request even when
/// the CONNECTION states a different region.
#[test]
fn standard_glue_endpoint_region_signs_even_when_a_different_region_is_stated() {
    let creds = creds_stating_region("us-east-1");

    assert_eq!(
        creds
            .sigv4_signing_region("https://glue.eu-west-1.amazonaws.com/iceberg")
            .as_deref(),
        Some("eu-west-1"),
        "the endpoint's own region must sign, not the stated bucket region"
    );
}

/// Scenario: When SigV4 is enabled, access_key, secret_key, and a signing
/// region are required — no address form outside the standard Glue shape
/// supplies a signing region on its own.
#[test]
fn non_standard_addresses_supply_no_signing_region() {
    let creds = creds_stating_region("");

    for (form, uri) in NON_STANDARD_ADDRESSES {
        assert_eq!(
            creds.sigv4_signing_region(uri),
            None,
            "a {form} address must not supply a signing region: {uri}"
        );
    }
}

/// Scenario: a non-standard address signs with the stated `region`, unaffected
/// by the standard-Glue-endpoint rule.
#[test]
fn stated_region_signs_for_a_non_standard_address() {
    let creds = creds_stating_region("us-gov-west-1");

    for (form, uri) in NON_STANDARD_ADDRESSES {
        assert_eq!(
            creds.sigv4_signing_region(uri).as_deref(),
            Some("us-gov-west-1"),
            "a {form} address must sign with the stated region: {uri}"
        );
    }
}

// ---------------------------------------------------------------------------
// Signing-region refusal (required_signing_region)
// ---------------------------------------------------------------------------

/// The refusal names `region` and carries neither a credential value nor the
/// catalog URI: the exact-text pin below is what proves both absences.
#[test]
fn required_signing_region_refuses_without_a_signing_region() {
    let creds = ConnectionCreds {
        access_key: "AKID_SENTINEL".into(),
        secret_key: "SECRET_SENTINEL".into(),
        session_token: Some("SESSION_SENTINEL".into()),
        ..creds_stating_region("")
    };

    let err = required_signing_region(&creds, "https://catalog.internal.example/iceberg")
        .expect_err("no stated region and no standard Glue endpoint must refuse");

    let UdfError::User(msg) = err else {
        panic!("the refusal must be a user error, got {err:?}");
    };
    assert_eq!(
        msg,
        "SigV4 catalog signing requires a region: neither the stated region nor the catalog URI \
         supplies one"
    );
}
