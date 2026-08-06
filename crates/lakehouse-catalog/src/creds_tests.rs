use super::*;

#[test]
fn secret_values_lists_access_key_secret_key_and_session_token_in_order() {
    let storage = StorageProps {
        access_key: "AKIA_EXAMPLE".into(),
        secret_key: "static-secret-key".into(),
        session_token: Some("sts-session-token".into()),
        ..Default::default()
    };

    assert_eq!(
        storage.secret_values(),
        vec!["AKIA_EXAMPLE", "static-secret-key", "sts-session-token"]
    );
}

/// Every value this returns is fed to literal-value error redaction, so an
/// empty or absent credential must never enter the list — redacting `""`
/// would strip every character of the error message it guards.
#[test]
fn secret_values_omits_every_empty_or_absent_credential() {
    let no_access_key_no_token = StorageProps {
        access_key: String::new(),
        secret_key: "static-secret-key".into(),
        session_token: None,
        ..Default::default()
    };
    assert_eq!(
        no_access_key_no_token.secret_values(),
        vec!["static-secret-key"]
    );

    let no_secret_key_empty_token = StorageProps {
        access_key: "AKIA_EXAMPLE".into(),
        secret_key: String::new(),
        session_token: Some(String::new()),
        ..Default::default()
    };
    assert_eq!(
        no_secret_key_empty_token.secret_values(),
        vec!["AKIA_EXAMPLE"]
    );

    assert!(StorageProps::default().secret_values().is_empty());
}

/// [`StorageProps::default`] documents itself as equal to deserializing a
/// `StorageProps` whose every optional field is absent. Pinning that keeps the
/// hand-written `Default` and serde's `default_true` seam from drifting apart
/// now that the type sits behind a crate boundary from the scan spec it feeds.
#[test]
fn default_equals_deserializing_a_props_with_every_optional_field_absent() {
    let field_absent: StorageProps =
        serde_json::from_str(r#"{"endpoint":"","region":"","access_key":"","secret_key":""}"#)
            .unwrap();

    assert_eq!(StorageProps::default(), field_absent);
}

/// The manual `Debug` impl is the only thing standing between a
/// `ConnectionCreds` and a secret in a log line or a `{:?}`-formatted error,
/// so every secret-bearing field is asserted, not just the two the engine's
/// `parse_creds` tests happen to cover.
#[test]
fn debug_redacts_every_secret_bearing_field() {
    let creds = ConnectionCreds {
        warehouse: "wh".into(),
        endpoint: "http://s3.example.com".into(),
        region: "us-east-1".into(),
        access_key: "AKIA_EXAMPLE".into(),
        secret_key: "static-secret-key".into(),
        session_token: Some("sts-session-token".into()),
        path_style: true,
        use_sigv4: false,
        use_vended_credentials: false,
        token: Some("static-bearer-token".into()),
        client_id: Some("my-client-id".into()),
        client_secret: Some("oauth-client-secret".into()),
        oauth2_server_uri: Some("https://auth.example.com/token".into()),
        scope: Some("catalog:read".into()),
        account_name: Some("acct".into()),
        account_key: Some("static-account-key".into()),
        sas_token: Some("sv=…&sig=static-sas-signature".into()),
    };

    let debug = format!("{creds:?}");

    for secret in [
        "static-secret-key",
        "sts-session-token",
        "static-bearer-token",
        "oauth-client-secret",
        "static-account-key",
        "sv=…&sig=static-sas-signature",
    ] {
        assert!(
            !debug.contains(secret),
            "{secret} must not appear in Debug: {debug}"
        );
    }

    // Neither the S3 access key ID nor the OAuth2 client ID is a secret, and
    // both are diagnostically useful — they stay visible on purpose.
    assert!(debug.contains("AKIA_EXAMPLE"), "{debug}");
    assert!(debug.contains("my-client-id"), "{debug}");
}
