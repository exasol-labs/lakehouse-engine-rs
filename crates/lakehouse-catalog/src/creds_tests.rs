use super::*;

#[test]
fn secret_values_omits_every_empty_or_absent_credential() {
    let full = StorageProps {
        access_key: "AK".into(),
        secret_key: "SK".into(),
        session_token: Some("TOK".into()),
        ..Default::default()
    };
    assert_eq!(full.secret_values(), vec!["AK", "SK", "TOK"]);

    let partial = StorageProps {
        secret_key: "SK".into(),
        session_token: Some(String::new()),
        ..Default::default()
    };
    assert_eq!(partial.secret_values(), vec!["SK"]);

    assert!(StorageProps::default().secret_values().is_empty());
}

#[test]
fn default_equals_deserializing_a_props_with_every_optional_field_absent() {
    let field_absent: StorageProps =
        serde_json::from_str(r#"{"endpoint":"","region":"","access_key":"","secret_key":""}"#)
            .unwrap();
    assert_eq!(StorageProps::default(), field_absent);
}

#[test]
fn debug_redacts_every_secret_bearing_field() {
    let creds = ConnectionCreds {
        warehouse: "wh".into(), endpoint: "http://s3.example.com".into(),
        region: "us-east-1".into(), access_key: "AKIA_EX".into(),
        secret_key: "SK".into(), session_token: Some("TOK".into()), path_style: true,
        use_sigv4: false, use_vended_credentials: false,
        token: Some("bearer".into()), client_id: Some("cid".into()),
        client_secret: Some("csecret".into()),
        oauth2_server_uri: Some("https://auth.example.com/token".into()),
        scope: Some("catalog:read".into()), account_name: Some("acct".into()),
        account_key: Some("akey".into()), sas_token: Some("sv=…&sig=sas".into()),
    };
    let debug = format!("{creds:?}");
    for secret in ["AKIA_EX", "SK", "TOK", "bearer", "csecret", "akey", "sv=…&sig=sas"] {
        assert!(!debug.contains(secret), "{secret} leaked: {debug}");
    }
    assert!(debug.contains("cid"));

    let props = StorageProps {
        endpoint: "http://minio:9000".into(), region: "us-east-1".into(),
        access_key: "AK2".into(), secret_key: "SK2".into(),
        session_token: Some("TOK2".into()), allow_http: true, path_style: true,
    };
    for (label, text) in [
        ("StorageProps", format!("{props:?}")),
        ("StorageBackend", format!("{:?}", StorageBackend::S3(props.clone()))),
        ("StorageCreds", format!("{:?}", StorageCreds {
            endpoint: props.endpoint.clone(), region: props.region.clone(),
            access_key: props.access_key.clone(), secret_key: props.secret_key.clone(),
            session_token: props.session_token.clone(), path_style: true,
            ..Default::default()
        })),
    ] {
        for s in ["AK2", "SK2", "TOK2"] {
            assert!(!text.contains(s), "{label} leaked {s}: {text}");
        }
    }
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
    let pair = creds_with(None, Some(CLIENT_ID), Some(CLIENT_SECRET));
    match pair.supplied_catalog_auth() {
        SuppliedCatalogAuth::ClientCredentials { client_id, client_secret } => {
            assert_eq!(client_id, CLIENT_ID);
            assert_eq!(client_secret, CLIENT_SECRET);
        }
        _ => panic!("a complete pair without a token must name ClientCredentials"),
    }

    let token = creds_with(Some(TOKEN), None, None);
    match token.supplied_catalog_auth() {
        SuppliedCatalogAuth::StaticToken(t) => assert_eq!(t, TOKEN),
        _ => panic!("a token without OAuth2 fields must name StaticToken"),
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
            matches!(creds.supplied_catalog_auth(), SuppliedCatalogAuth::Unauthenticated),
            "shape (token={}, id={}, secret={}) describes no mode",
            token.is_some(), client_id.is_some(), client_secret.is_some(),
        );
    }
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
        endpoint: STORAGE_ENDPOINT.into(), region: STORAGE_REGION.into(),
        access_key: STORAGE_AK.into(), secret_key: STORAGE_SK.into(),
        session_token: Some(STORAGE_SESSION_TOKEN.into()), path_style: true,
        ..Default::default()
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

