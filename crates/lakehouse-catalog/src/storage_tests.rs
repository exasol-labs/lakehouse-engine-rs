use super::*;

const ENDPOINT: &str = "http://minio.local:9000";
const REGION: &str = "us-east-1";
const ACCESS_KEY: &str = "AKIDEXAMPLE";
const SECRET_KEY: &str = "wJalrXUtnFEMI_EXAMPLE_KEY";
const SESSION_TOKEN: &str = "FwoGZXIvYXdzEXAMPLE_TOKEN";

/// Every connection field populated, a session token present, and
/// `allow_http` deliberately ON — the latter is NOT an iceberg storage config
/// key, so an exact map comparison against this fixture pins its absence.
fn populated_backend() -> StorageBackend {
    StorageBackend::S3(StorageProps {
        endpoint: ENDPOINT.into(),
        region: REGION.into(),
        access_key: ACCESS_KEY.into(),
        secret_key: SECRET_KEY.into(),
        session_token: Some(SESSION_TOKEN.into()),
        allow_http: true,
        path_style: true,
    })
}

/// The exact six-key iceberg config map [`populated_backend`] must produce.
fn expected_populated_config() -> HashMap<String, String> {
    HashMap::from([
        (S3_ENDPOINT.to_string(), ENDPOINT.to_string()),
        (S3_REGION.to_string(), REGION.to_string()),
        (S3_ACCESS_KEY_ID.to_string(), ACCESS_KEY.to_string()),
        (S3_SECRET_ACCESS_KEY.to_string(), SECRET_KEY.to_string()),
        (S3_SESSION_TOKEN.to_string(), SESSION_TOKEN.to_string()),
        (S3_PATH_STYLE_ACCESS.to_string(), "true".to_string()),
    ])
}

#[test]
fn catalog_storage_props_emits_every_populated_s3_key_and_nothing_else() {
    assert_eq!(
        populated_backend().catalog_storage_props(),
        expected_populated_config()
    );
}

#[test]
fn catalog_storage_props_omits_empty_connection_fields_and_an_absent_token() {
    let backend = StorageBackend::S3(StorageProps::default());

    assert_eq!(
        backend.catalog_storage_props(),
        HashMap::from([(S3_PATH_STYLE_ACCESS.to_string(), "true".to_string())]),
        "only path-style access is unconditional; every empty credential field \
         and the absent session token must be left out"
    );
}

/// A `Some("")` session token is gated on presence, NOT on being non-empty —
/// unlike the four connection fields. Preserved verbatim from the pre-refactor
/// `if let Some(token)` so the props map stays byte-identical.
#[test]
fn catalog_storage_props_emits_a_present_but_empty_session_token() {
    let backend = StorageBackend::S3(StorageProps {
        session_token: Some(String::new()),
        path_style: false,
        ..StorageProps::default()
    });

    assert_eq!(
        backend.catalog_storage_props(),
        HashMap::from([
            (S3_SESSION_TOKEN.to_string(), String::new()),
            (S3_PATH_STYLE_ACCESS.to_string(), "false".to_string()),
        ])
    );
}

#[test]
fn file_io_is_configured_from_exactly_the_catalog_storage_props() {
    assert_eq!(
        populated_backend().file_io().config().props(),
        &expected_populated_config()
    );
}

#[test]
fn secret_values_are_the_wrapped_props_secret_values() {
    let props = StorageProps {
        access_key: ACCESS_KEY.into(),
        secret_key: SECRET_KEY.into(),
        session_token: Some(SESSION_TOKEN.into()),
        ..StorageProps::default()
    };

    assert_eq!(
        StorageBackend::S3(props.clone()).secret_values(),
        props.secret_values()
    );
}

#[test]
fn s3_serializes_under_a_lowercase_externally_tagged_variant_key() {
    assert_eq!(
        serde_json::to_value(populated_backend()).expect("backend serializes"),
        serde_json::json!({
            "s3": {
                "endpoint": ENDPOINT,
                "region": REGION,
                "access_key": ACCESS_KEY,
                "secret_key": SECRET_KEY,
                "session_token": SESSION_TOKEN,
                "allow_http": true,
                "path_style": true,
            }
        })
    );
}

#[test]
fn s3_round_trips_through_its_tagged_encoding() {
    let backend = populated_backend();
    let encoded = serde_json::to_string(&backend).expect("backend serializes");

    assert_eq!(
        serde_json::from_str::<StorageBackend>(&encoded).expect("backend deserializes"),
        backend
    );
}

/// The externally-tagged decision, asserted from the decode side: a bare
/// (untagged) props object and an unknown or wrong-case variant key must
/// all be rejected rather than resolved by trial deserialization — for
/// both the `s3` and `adls` variants.
#[test]
fn only_matching_lowercase_variant_keys_decode() {
    for payload in [
        r#"{"endpoint":"","region":"","access_key":"","secret_key":""}"#,
        r#"{"S3":{"endpoint":"","region":"","access_key":"","secret_key":""}}"#,
        r#"{"azure":{"endpoint":"","region":"","access_key":"","secret_key":""}}"#,
        r#"{"Adls":{"account_name":"","cred":{"AccountKey":""}}}"#,
        r#"{"azure":{"account_name":"","cred":{"AccountKey":""}}}"#,
        r#"{"adls":{"endpoint":"","region":"","access_key":"","secret_key":""}}"#,
        r#"{"adls":{"account_name":"","cred":{"AccountKey":""}}}"#,
    ] {
        assert!(
            serde_json::from_str::<StorageBackend>(payload).is_err(),
            "payload must not decode to a storage backend: {payload}"
        );
    }
}

/// Mirrors [`populated_backend`]/[`expected_populated_config`] for the S3
/// arm: each `AdlsCred` state must produce exactly the account-name key
/// plus its one matching credential key, nothing else.
#[test]
fn adls_catalog_storage_props_emit_the_account_and_one_credential_key() {
    let account_key_backend = StorageBackend::Adls {
        account_name: "myaccount".into(),
        cred: AdlsCred::AccountKey("azure-static-key-secret".into()),
    };
    assert_eq!(
        account_key_backend.catalog_storage_props(),
        HashMap::from([
            (ADLS_ACCOUNT_NAME.to_string(), "myaccount".to_string()),
            (
                ADLS_ACCOUNT_KEY.to_string(),
                "azure-static-key-secret".to_string()
            ),
        ])
    );

    let sas_backend = StorageBackend::Adls {
        account_name: "myaccount".into(),
        cred: AdlsCred::Sas("sv=2024&sig=azure-sas-secret".into()),
    };
    assert_eq!(
        sas_backend.catalog_storage_props(),
        HashMap::from([
            (ADLS_ACCOUNT_NAME.to_string(), "myaccount".to_string()),
            (
                ADLS_SAS_TOKEN.to_string(),
                "sv=2024&sig=azure-sas-secret".to_string()
            ),
        ])
    );
}

#[test]
fn adls_file_io_is_configured_from_exactly_the_catalog_storage_props() {
    let backend = StorageBackend::Adls {
        account_name: "myaccount".into(),
        cred: AdlsCred::AccountKey("azure-static-key-secret".into()),
    };

    assert_eq!(
        backend.file_io().config().props(),
        &backend.catalog_storage_props()
    );
}

#[test]
fn adls_secret_values_are_the_one_credential_and_omit_an_empty_one() {
    let account_key_backend = StorageBackend::Adls {
        account_name: "myaccount".into(),
        cred: AdlsCred::AccountKey("azure-static-key-secret".into()),
    };
    assert_eq!(
        account_key_backend.secret_values(),
        vec!["azure-static-key-secret"]
    );

    let sas_backend = StorageBackend::Adls {
        account_name: "myaccount".into(),
        cred: AdlsCred::Sas("sv=2024&sig=azure-sas-secret".into()),
    };
    assert_eq!(
        sas_backend.secret_values(),
        vec!["sv=2024&sig=azure-sas-secret"]
    );

    let empty_key_backend = StorageBackend::Adls {
        account_name: "myaccount".into(),
        cred: AdlsCred::AccountKey(String::new()),
    };
    assert!(
        empty_key_backend.secret_values().is_empty(),
        "an empty credential must not surface as a secret to redact against"
    );
}

/// The manual `Debug` impl on `AdlsCred` is what stands between a
/// logged/`{:?}`-formatted error and a live storage credential, so both
/// credential states — standalone and wrapped in the `Adls` backend
/// variant — must never print the secret.
#[test]
fn adls_cred_is_redacted_in_debug_output() {
    let account_key = AdlsCred::AccountKey("azure-static-key-secret".into());
    let key_debug = format!("{account_key:?}");
    assert!(
        !key_debug.contains("azure-static-key-secret"),
        "{key_debug}"
    );
    assert!(key_debug.contains("[redacted]"), "{key_debug}");

    let sas = AdlsCred::Sas("sv=2024&sig=azure-sas-secret".into());
    let sas_debug = format!("{sas:?}");
    assert!(
        !sas_debug.contains("sv=2024&sig=azure-sas-secret"),
        "{sas_debug}"
    );
    assert!(sas_debug.contains("[redacted]"), "{sas_debug}");

    let backend = StorageBackend::Adls {
        account_name: "myaccount".into(),
        cred: account_key,
    };
    let backend_debug = format!("{backend:?}");
    assert!(
        !backend_debug.contains("azure-static-key-secret"),
        "{backend_debug}"
    );
}

#[test]
fn adls_serializes_under_a_lowercase_externally_tagged_variant_key() {
    let backend = StorageBackend::Adls {
        account_name: "myaccount".into(),
        cred: AdlsCred::AccountKey("azure-static-key-secret".into()),
    };

    assert_eq!(
        serde_json::to_value(backend).expect("backend serializes"),
        serde_json::json!({
            "adls": {
                "account_name": "myaccount",
                "cred": {"account_key": "azure-static-key-secret"},
            }
        })
    );
}

#[test]
fn adls_round_trips_through_its_tagged_encoding() {
    let backend = StorageBackend::Adls {
        account_name: "myaccount".into(),
        cred: AdlsCred::Sas("sv=2024&sig=azure-sas-secret".into()),
    };
    let encoded = serde_json::to_string(&backend).expect("backend serializes");

    assert_eq!(
        serde_json::from_str::<StorageBackend>(&encoded).expect("backend deserializes"),
        backend
    );
}
