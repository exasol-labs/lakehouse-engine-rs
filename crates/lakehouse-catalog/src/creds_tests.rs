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

const TOKEN: &str = "static-bearer-token";
const CLIENT_ID: &str = "oauth-client-id";
const CLIENT_SECRET: &str = "oauth-client-secret";

fn creds_with(
    token: Option<&str>,
    client_id: Option<&str>,
    client_secret: Option<&str>,
) -> ConnectionCreds {
    ConnectionCreds {
        token: token.map(String::from),
        client_id: client_id.map(String::from),
        client_secret: client_secret.map(String::from),
        ..crate::test_support::creds_no_auth()
    }
}

/// `validate_creds` accepts three of the eight presence shapes, so the
/// classifier names one mode for each of those three and no mode for the other
/// five. All eight are driven here: a `_ => StaticToken(..)` wildcard slip
/// passes every accepted row, and only the rejected rows catch it.
#[test]
fn supplied_catalog_auth_names_one_mode_per_field_shape() {
    let pair_only = creds_with(None, Some(CLIENT_ID), Some(CLIENT_SECRET));
    match pair_only.supplied_catalog_auth() {
        SuppliedCatalogAuth::ClientCredentials {
            client_id,
            client_secret,
        } => {
            assert_eq!(client_id, CLIENT_ID);
            assert_eq!(client_secret, CLIENT_SECRET);
        }
        _ => panic!("a complete pair without a token must name ClientCredentials"),
    }

    let token_only = creds_with(Some(TOKEN), None, None);
    match token_only.supplied_catalog_auth() {
        SuppliedCatalogAuth::StaticToken(token) => assert_eq!(token, TOKEN),
        _ => panic!("a token without either OAuth2 field must name StaticToken"),
    }

    for (token, client_id, client_secret) in [
        (None, None, None),
        (None, Some(CLIENT_ID), None),
        (None, None, Some(CLIENT_SECRET)),
        (Some(TOKEN), Some(CLIENT_ID), None),
        (Some(TOKEN), None, Some(CLIENT_SECRET)),
        (Some(TOKEN), Some(CLIENT_ID), Some(CLIENT_SECRET)),
    ] {
        let creds = creds_with(token, client_id, client_secret);
        assert!(
            matches!(
                creds.supplied_catalog_auth(),
                SuppliedCatalogAuth::Unauthenticated
            ),
            "shape (token={}, client_id={}, client_secret={}) describes no mode",
            token.is_some(),
            client_id.is_some(),
            client_secret.is_some(),
        );
    }

    // An empty field is an absent field. Each position is emptied in the one
    // shape where that distinction changes the mode, so an `is_some()` reading
    // fails all three.
    let empty_token = creds_with(Some(""), Some(CLIENT_ID), Some(CLIENT_SECRET));
    assert!(
        matches!(
            empty_token.supplied_catalog_auth(),
            SuppliedCatalogAuth::ClientCredentials { .. }
        ),
        "an empty token leaves the complete pair, not the rejected all-three shape"
    );

    let empty_client_id = creds_with(Some(TOKEN), Some(""), None);
    match empty_client_id.supplied_catalog_auth() {
        SuppliedCatalogAuth::StaticToken(token) => assert_eq!(token, TOKEN),
        _ => panic!("an empty client_id leaves the token alone, not a partial pair"),
    }

    let empty_client_secret = creds_with(None, Some(CLIENT_ID), Some(""));
    assert!(
        matches!(
            empty_client_secret.supplied_catalog_auth(),
            SuppliedCatalogAuth::Unauthenticated
        ),
        "an empty client_secret cannot complete a pair"
    );
}
