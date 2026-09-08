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
        "AKIA_EXAMPLE",
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

    // The OAuth2 client ID is not a secret and is diagnostically useful — it
    // stays visible on purpose.
    assert!(debug.contains("my-client-id"), "{debug}");
}

#[test]
fn debug_redacts_every_storage_credential_field() {
    let props = StorageProps {
        endpoint: "http://minio:9000".into(),
        region: "us-east-1".into(),
        access_key: "AKIA_STORAGE_EXAMPLE".into(),
        secret_key: "static-storage-secret".into(),
        session_token: Some("storage-session-token".into()),
        allow_http: true,
        path_style: true,
    };
    let props_debug = format!("{props:?}");
    for secret in [
        "AKIA_STORAGE_EXAMPLE",
        "static-storage-secret",
        "storage-session-token",
    ] {
        assert!(!props_debug.contains(secret), "{props_debug}");
    }
    assert!(props_debug.contains("http://minio:9000"), "{props_debug}");
    assert!(props_debug.contains("us-east-1"), "{props_debug}");
    assert!(props_debug.contains("allow_http: true"), "{props_debug}");
    assert!(props_debug.contains("path_style: true"), "{props_debug}");

    let backend = StorageBackend::S3(props);
    let backend_debug = format!("{backend:?}");
    for secret in [
        "AKIA_STORAGE_EXAMPLE",
        "static-storage-secret",
        "storage-session-token",
    ] {
        assert!(!backend_debug.contains(secret), "{backend_debug}");
    }

    let storage_creds = StorageCreds {
        endpoint: "http://minio:9000".into(),
        region: "us-east-1".into(),
        access_key: "AKIA_STORAGE_EXAMPLE".into(),
        secret_key: "static-storage-secret".into(),
        session_token: Some("storage-session-token".into()),
        path_style: true,
        account_name: None,
        account_key: None,
        sas_token: None,
    };
    let creds_debug = format!("{storage_creds:?}");
    for secret in [
        "AKIA_STORAGE_EXAMPLE",
        "static-storage-secret",
        "storage-session-token",
    ] {
        assert!(!creds_debug.contains(secret), "{creds_debug}");
    }
    assert!(creds_debug.contains("http://minio:9000"), "{creds_debug}");
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

    // An empty field is an absent field.
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

const STORAGE_ENDPOINT: &str = "http://minio:9000";
const STORAGE_REGION: &str = "us-east-1";
const STORAGE_AK: &str = "AKIA_PROJECTED";
const STORAGE_SK: &str = "projected-secret-key";
const STORAGE_SESSION_TOKEN: &str = "projected-session-token";
const AZURE_ACCOUNT: &str = "projectedaccount";
const AZURE_ACCOUNT_KEY: &str = "projected-account-key";
const AZURE_SAS_TOKEN: &str = "projected-sas-token";
const OAUTH2_SERVER_URI: &str = "https://idp.example.com/token";
const OAUTH2_SCOPE: &str = "catalog-scope";

fn s3_storage_creds() -> StorageCreds {
    StorageCreds {
        endpoint: STORAGE_ENDPOINT.into(),
        region: STORAGE_REGION.into(),
        access_key: STORAGE_AK.into(),
        secret_key: STORAGE_SK.into(),
        session_token: Some(STORAGE_SESSION_TOKEN.into()),
        path_style: true,
        account_name: None,
        account_key: None,
        sas_token: None,
    }
}

fn s3_backend(allow_http: bool) -> StorageBackend {
    StorageBackend::S3(StorageProps {
        endpoint: STORAGE_ENDPOINT.into(),
        region: STORAGE_REGION.into(),
        access_key: STORAGE_AK.into(),
        secret_key: STORAGE_SK.into(),
        session_token: Some(STORAGE_SESSION_TOKEN.into()),
        allow_http,
        path_style: true,
    })
}

fn password_carrying_every_field() -> serde_json::Value {
    serde_json::json!({
        "endpoint": STORAGE_ENDPOINT,
        "region": STORAGE_REGION,
        "access_key": STORAGE_AK,
        "secret_key": STORAGE_SK,
        "session_token": STORAGE_SESSION_TOKEN,
        "path_style": false,
        "account_name": AZURE_ACCOUNT,
        "account_key": AZURE_ACCOUNT_KEY,
        "sas_token": AZURE_SAS_TOKEN,
        "warehouse": "warehouse",
        "use_sigv4": true,
        "use_vended_credentials": true,
        "token": TOKEN,
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "oauth2_server_uri": OAUTH2_SERVER_URI,
        "scope": OAUTH2_SCOPE,
    })
}

#[test]
fn from_json_reads_the_nine_storage_fields() {
    let creds = StorageCreds::from_json(&password_carrying_every_field());

    assert_eq!(creds.endpoint, STORAGE_ENDPOINT);
    assert_eq!(creds.region, STORAGE_REGION);
    assert_eq!(creds.access_key, STORAGE_AK);
    assert_eq!(creds.secret_key, STORAGE_SK);
    assert_eq!(creds.session_token.as_deref(), Some(STORAGE_SESSION_TOKEN));
    assert_eq!(creds.account_name.as_deref(), Some(AZURE_ACCOUNT));
    assert_eq!(creds.account_key.as_deref(), Some(AZURE_ACCOUNT_KEY));
    assert_eq!(creds.sas_token.as_deref(), Some(AZURE_SAS_TOKEN));
    assert!(
        !creds.path_style,
        "an explicit JSON `false` overrides the `true` default"
    );
}

#[test]
fn backend_selects_adls_only_for_an_account_name_with_exactly_one_azure_credential() {
    let account_key_only = StorageCreds {
        account_name: Some(AZURE_ACCOUNT.into()),
        account_key: Some(AZURE_ACCOUNT_KEY.into()),
        ..s3_storage_creds()
    };
    assert_eq!(
        account_key_only.backend(false),
        StorageBackend::Adls {
            account_name: AZURE_ACCOUNT.into(),
            cred: AdlsCred::AccountKey(AZURE_ACCOUNT_KEY.into()),
        }
    );

    let sas_token_only = StorageCreds {
        account_name: Some(AZURE_ACCOUNT.into()),
        sas_token: Some(AZURE_SAS_TOKEN.into()),
        ..s3_storage_creds()
    };
    assert_eq!(
        sas_token_only.backend(false),
        StorageBackend::Adls {
            account_name: AZURE_ACCOUNT.into(),
            cred: AdlsCred::Sas(AZURE_SAS_TOKEN.into()),
        }
    );

    let both_azure_credentials = StorageCreds {
        account_name: Some(AZURE_ACCOUNT.into()),
        account_key: Some(AZURE_ACCOUNT_KEY.into()),
        sas_token: Some(AZURE_SAS_TOKEN.into()),
        ..s3_storage_creds()
    };
    let account_name_alone = StorageCreds {
        account_name: Some(AZURE_ACCOUNT.into()),
        ..s3_storage_creds()
    };
    let account_key_without_an_account_name = StorageCreds {
        account_key: Some(AZURE_ACCOUNT_KEY.into()),
        ..s3_storage_creds()
    };
    let sas_token_without_an_account_name = StorageCreds {
        sas_token: Some(AZURE_SAS_TOKEN.into()),
        ..s3_storage_creds()
    };
    for (shape, creds) in [
        ("both Azure credentials", both_azure_credentials),
        ("an account name alone", account_name_alone),
        (
            "an account key without an account name",
            account_key_without_an_account_name,
        ),
        (
            "a SAS token without an account name",
            sas_token_without_an_account_name,
        ),
        ("no Azure field at all", s3_storage_creds()),
    ] {
        assert_eq!(
            creds.backend(false),
            s3_backend(false),
            "{shape} does not describe an Azure backend"
        );
    }
}

