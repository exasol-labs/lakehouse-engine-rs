use super::*;
use crate::adapter::catalog_kind::CatalogKind;
use crate::scan::spec::{AdlsCred, StorageProps};
use exasol_udf_sdk::connect_back::ConnectionObject;
use exasol_udf_sdk::test_support::TestContext;
use lakehouse_catalog::{StaticStoreAddress, StorageCreds};

fn with_conn(address: &str, password: &str) -> TestContext {
    TestContext::scalar(vec![]).with_connection(
        "MY_CONN",
        ConnectionObject {
            kind: "PASSWORD".into(),
            address: address.to_string(),
            user: "".to_string(),
            password: password.to_string(),
        },
    )
}

fn no_conn() -> TestContext {
    TestContext::scalar(vec![])
}

fn minimal_password() -> String {
    serde_json::json!({
        "warehouse": "wh",
        "endpoint": "http://s3.example.com",
        "region": "us-east-1",
        "access_key": "AKID",
        "secret_key": "SECRET",
        "path_style": true
    })
    .to_string()
}

#[test]
fn read_connection_parses_uri_and_creds() {
    let ctx = with_conn("http://catalog.example.com", &minimal_password());
    let resolved = read_connection(&ctx, Some("MY_CONN"), CatalogKind::IcebergRest).unwrap();

    assert_eq!(resolved.uri, "http://catalog.example.com");
    assert_eq!(resolved.creds.warehouse, "wh");
    assert_eq!(resolved.creds.endpoint, "http://s3.example.com");
    assert_eq!(resolved.creds.region, "us-east-1");
    assert_eq!(resolved.creds.access_key, "AKID");
    assert_eq!(resolved.creds.secret_key, "SECRET");
    assert_eq!(resolved.creds.session_token, None);
    assert!(!resolved.creds.use_sigv4);
    assert!(!resolved.creds.use_vended_credentials);
    assert_eq!(resolved.creds.path_style, Some(true));
}

#[test]
fn missing_connection_name_errors() {
    let ctx = no_conn();

    let err_none = read_connection(&ctx, None, CatalogKind::IcebergRest).unwrap_err();
    assert!(
        err_none
            .to_string()
            .contains("CATALOG_CONNECTION is required")
    );

    let err_empty = read_connection(&ctx, Some(""), CatalogKind::IcebergRest).unwrap_err();
    assert!(
        err_empty
            .to_string()
            .contains("CATALOG_CONNECTION is required")
    );

    assert!(!err_none.to_string().contains("SECRET"));
    assert!(!err_empty.to_string().contains("SECRET"));
}

#[test]
fn malformed_password_no_leak() {
    let bad_password = "not-json-at-all SECRET_VALUE_HERE";
    let ctx = with_conn("http://catalog.example.com", bad_password);
    let err = read_connection(&ctx, Some("MY_CONN"), CatalogKind::IcebergRest).unwrap_err();

    assert!(err.to_string().contains("not a valid JSON object"));
    assert!(!err.to_string().contains("not-json-at-all"));
    assert!(!err.to_string().contains("SECRET_VALUE_HERE"));
}

#[test]
fn json_array_password_no_leak() {
    let array_password = r#"["SECRET_IN_ARRAY", "OTHER_VAL"]"#;
    let ctx = with_conn("http://catalog.example.com", array_password);
    let err = read_connection(&ctx, Some("MY_CONN"), CatalogKind::IcebergRest).unwrap_err();

    assert!(err.to_string().contains("not a valid JSON object"));
    assert!(!err.to_string().contains("SECRET_IN_ARRAY"));
}

#[test]
fn missing_warehouse_rejected_s3_not_required() {
    let no_warehouse = serde_json::json!({
        "endpoint": "http://s3.example.com",
        "region": "us-east-1"
    })
    .to_string();
    let ctx = with_conn("http://catalog.example.com", &no_warehouse);
    let err = read_connection(&ctx, Some("MY_CONN"), CatalogKind::IcebergRest).unwrap_err();

    let msg = err.to_string();
    assert!(
        msg.contains("warehouse"),
        "must name missing field 'warehouse': {msg}"
    );
    assert!(
        !msg.contains("access_key"),
        "access_key is optional and must not be reported missing: {msg}"
    );
    assert!(
        !msg.contains("secret_key"),
        "secret_key is optional and must not be reported missing: {msg}"
    );
}

#[test]
fn warehouse_only_password_accepted_s3_optional() {
    let partial = serde_json::json!({ "warehouse": "wh" }).to_string();
    let ctx = with_conn("http://catalog.example.com", &partial);
    let resolved = read_connection(&ctx, Some("MY_CONN"), CatalogKind::IcebergRest).unwrap();
    let creds = &resolved.creds;

    assert_eq!(creds.warehouse, "wh");
    assert_eq!(creds.endpoint, "");
    assert_eq!(creds.region, "");
    assert_eq!(creds.access_key, "");
    assert_eq!(creds.secret_key, "");
    assert!(!creds.use_sigv4);
    assert!(!creds.use_vended_credentials);
}

#[test]
fn legacy_full_static_s3_password_still_accepted() {
    let ctx = with_conn("http://catalog.example.com", &minimal_password());
    let resolved = read_connection(&ctx, Some("MY_CONN"), CatalogKind::IcebergRest).unwrap();
    let creds = &resolved.creds;

    assert_eq!(creds.warehouse, "wh");
    assert_eq!(creds.endpoint, "http://s3.example.com");
    assert_eq!(creds.region, "us-east-1");
    assert_eq!(creds.access_key, "AKID");
    assert_eq!(creds.secret_key, "SECRET");
}

/// Scenario: a warehouse-only password defaults every S3 and auth field to absent, flags false.
#[test]
fn optional_fields_default() {
    let warehouse_only = serde_json::json!({ "warehouse": "wh" }).to_string();
    let ctx = with_conn("http://catalog.example.com", &warehouse_only);
    let resolved = read_connection(&ctx, Some("MY_CONN"), CatalogKind::IcebergRest).unwrap();
    let creds = &resolved.creds;

    assert_eq!(creds.endpoint, "", "endpoint must default to empty");
    assert_eq!(creds.region, "", "region must default to empty");
    assert_eq!(creds.access_key, "", "access_key must default to empty");
    assert_eq!(creds.secret_key, "", "secret_key must default to empty");

    assert_eq!(creds.token, None, "token must default to None");
    assert_eq!(creds.client_id, None, "client_id must default to None");
    assert_eq!(
        creds.client_secret, None,
        "client_secret must default to None"
    );
    assert_eq!(
        creds.oauth2_server_uri, None,
        "oauth2_server_uri must default to None"
    );
    assert_eq!(creds.scope, None, "scope must default to None");

    assert!(!creds.use_sigv4, "use_sigv4 must default to false");
    assert!(
        !creds.use_vended_credentials,
        "use_vended_credentials must default to false"
    );

    assert_eq!(
        creds.path_style, None,
        "path_style must default to unstated, not a resolved value"
    );
}

#[test]
fn optional_fields_set_when_supplied() {
    let password = serde_json::json!({
        "warehouse": "wh",
        "endpoint": "http://s3.example.com",
        "region": "us-east-1",
        "access_key": "AKID",
        "secret_key": "SECRET",
        "session_token": "STS_TOKEN",
        "path_style": false,
        "use_sigv4": true,
        "use_vended_credentials": true
    })
    .to_string();
    let ctx = with_conn("http://catalog.example.com", &password);
    let resolved = read_connection(&ctx, Some("MY_CONN"), CatalogKind::IcebergRest).unwrap();
    let creds = &resolved.creds;

    assert_eq!(creds.session_token.as_deref(), Some("STS_TOKEN"));
    assert_eq!(creds.path_style, Some(false));
    assert!(creds.use_sigv4);
    assert!(creds.use_vended_credentials);
}

#[test]
fn storage_block_maps_creds_to_storage_props() {
    let ctx = with_conn("http://catalog.example.com", &minimal_password());
    let resolved = read_connection(&ctx, Some("MY_CONN"), CatalogKind::IcebergRest).unwrap();
    let StorageBackend::S3(storage) = storage_block(&resolved.creds, false) else {
        panic!("S3 creds must select the S3 backend")
    };

    assert_eq!(storage.endpoint, "http://s3.example.com");
    assert_eq!(storage.region, "us-east-1");
    assert_eq!(storage.access_key, "AKID");
    assert_eq!(storage.secret_key, "SECRET");
    assert_eq!(storage.session_token, None);
    assert!(storage.path_style);
}

/// Distinctive values so a never-leaks assertion cannot pass by accident.
const AZURE_ACCOUNT_KEY: &str = "azure-shared-key-must-never-leak";
const AZURE_SAS: &str = "sv=2024-01-01&sig=azure-sas-signature-must-never-leak";
const S3_SECRET: &str = "s3-secret-must-never-leak";

/// Scenario: the account-key shape selects `AdlsCred::AccountKey` and leaves `sas_token` absent.
#[test]
fn account_key_creds_select_the_adls_backend() {
    let password = serde_json::json!({
        "warehouse": "wh",
        "account_name": "myaccount",
        "account_key": AZURE_ACCOUNT_KEY,
    })
    .to_string();
    let ctx = with_conn("http://catalog.example.com", &password);

    let resolved = read_connection(&ctx, Some("MY_CONN"), CatalogKind::IcebergRest).unwrap();

    assert_eq!(resolved.creds.account_name.as_deref(), Some("myaccount"));
    assert_eq!(
        resolved.creds.account_key.as_deref(),
        Some(AZURE_ACCOUNT_KEY)
    );
    assert_eq!(resolved.creds.sas_token, None);
    assert_eq!(
        storage_block(&resolved.creds, false),
        StorageBackend::Adls {
            account_name: "myaccount".to_string(),
            cred: AdlsCred::AccountKey(AZURE_ACCOUNT_KEY.to_string()),
        }
    );
}

/// Scenario: the SAS-token shape selects ADLS; `allow_http` leaves the Azure payload unchanged.
#[test]
fn sas_token_creds_select_the_adls_backend() {
    let password = serde_json::json!({
        "warehouse": "wh",
        "account_name": "myaccount",
        "sas_token": AZURE_SAS,
    })
    .to_string();
    let ctx = with_conn("http://catalog.example.com", &password);

    let resolved = read_connection(&ctx, Some("MY_CONN"), CatalogKind::IcebergRest).unwrap();

    assert_eq!(resolved.creds.sas_token.as_deref(), Some(AZURE_SAS));
    assert_eq!(resolved.creds.account_key, None);
    assert_eq!(
        storage_block(&resolved.creds, true),
        StorageBackend::Adls {
            account_name: "myaccount".to_string(),
            cred: AdlsCred::Sas(AZURE_SAS.to_string()),
        }
    );
}

/// Scenario: an Azure CONNECTION needs account_name and exactly one of account_key/sas_token.
#[test]
fn azure_creds_require_account_name_and_exactly_one_credential() {
    let shapes = [
        (
            serde_json::json!({ "warehouse": "wh", "account_key": AZURE_ACCOUNT_KEY }),
            "account_name is missing",
        ),
        (
            serde_json::json!({
                "warehouse": "wh",
                "account_name": "myaccount",
                "account_key": AZURE_ACCOUNT_KEY,
                "sas_token": AZURE_SAS,
            }),
            "account_key and sas_token are both present",
        ),
        (
            serde_json::json!({ "warehouse": "wh", "account_name": "myaccount" }),
            "neither account_key nor sas_token is present",
        ),
    ];

    for (password, expected_defect) in shapes {
        let ctx = with_conn("http://catalog.example.com", &password.to_string());

        let err = read_connection(&ctx, Some("MY_CONN"), CatalogKind::IcebergRest)
            .expect_err("a malformed Azure credential set must be rejected")
            .to_string();

        assert!(
            err.contains("account_name and exactly one of account_key and sas_token"),
            "{err}"
        );
        assert!(err.contains(expected_defect), "{err}");
        assert!(!err.contains(AZURE_ACCOUNT_KEY), "{err}");
        assert!(!err.contains(AZURE_SAS), "{err}");
    }
}

/// Scenario: mixed Azure and S3 fields are rejected, naming every field and echoing no value.
#[test]
fn mixed_azure_and_s3_credential_fields_are_rejected() {
    let password = serde_json::json!({
        "warehouse": "wh",
        "account_name": "myaccount",
        "account_key": AZURE_ACCOUNT_KEY,
        "region": "us-east-1",
        "secret_key": S3_SECRET,
    })
    .to_string();
    let ctx = with_conn("http://catalog.example.com", &password);

    let err = read_connection(&ctx, Some("MY_CONN"), CatalogKind::IcebergRest)
        .expect_err("a CONNECTION mixing Azure and S3 credential fields must be rejected")
        .to_string();

    assert!(
        err.contains("Azure and S3 storage credentials cannot both be supplied"),
        "{err}"
    );
    for supplied_field in ["account_name", "account_key", "region", "secret_key"] {
        assert!(
            err.contains(supplied_field),
            "{supplied_field} missing: {err}"
        );
    }
    assert!(!err.contains(AZURE_ACCOUNT_KEY), "{err}");
    assert!(!err.contains(S3_SECRET), "{err}");
}

/// Scenario: a CONNECTION naming no Azure field selects S3, with or without vending.
#[test]
fn absent_optional_fields_default_and_still_select_s3() {
    for use_vended_credentials in [false, true] {
        let password = serde_json::json!({
            "warehouse": "wh",
            "use_vended_credentials": use_vended_credentials,
        })
        .to_string();
        let ctx = with_conn("http://catalog.example.com", &password);

        let resolved = read_connection(&ctx, Some("MY_CONN"), CatalogKind::IcebergRest).unwrap();

        assert_eq!(resolved.creds.account_name, None);
        assert_eq!(resolved.creds.account_key, None);
        assert_eq!(resolved.creds.sas_token, None);
        assert_eq!(
            storage_block(&resolved.creds, false),
            StorageBackend::S3(StorageProps {
                path_style: false,
                ..Default::default()
            }),
            "a CONNECTION stating no storage field at all leaves path_style unstated, \
             which resolves to false"
        );
    }
}

/// Scenario: both readers derive an equal backend from a password omitting `path_style`.
#[test]
fn both_readers_derive_an_equal_backend_from_a_password_omitting_path_style() {
    let password = serde_json::json!({
        "warehouse": "wh",
        "endpoint": "http://s3.example.com",
        "region": "us-east-1",
        "access_key": "AKID",
        "secret_key": "SECRET",
        "use_vended_credentials": true,
    });

    let adapter_side = storage_block(&parse_creds(&password), false);
    let scan_side = lakehouse_catalog::StorageCreds::from_json(&password).backend(false);

    assert_eq!(adapter_side, scan_side);
    assert_eq!(
        adapter_side,
        StorageBackend::S3(StorageProps {
            endpoint: "http://s3.example.com".into(),
            region: "us-east-1".into(),
            access_key: "AKID".into(),
            secret_key: "SECRET".into(),
            path_style: false,
            ..Default::default()
        })
    );
}

/// Scenario: a non-vended endpoint without a stated `path_style` is rejected, naming the field.
#[test]
fn endpoint_without_a_stated_path_style_is_rejected_naming_the_field() {
    let password = serde_json::json!({
        "warehouse": "wh",
        "endpoint": "http://s3.example.com",
        "region": "us-east-1",
        "access_key": "AKID",
        "secret_key": "SECRET",
    })
    .to_string();
    let ctx = with_conn("http://catalog.example.com", &password);

    let err = read_connection(&ctx, Some("MY_CONN"), CatalogKind::IcebergRest)
        .expect_err("an endpoint without a stated path_style must be rejected");
    assert!(err.to_string().contains("path_style"), "{err}");
}

/// Scenario: either explicit `path_style` beside an endpoint is accepted.
#[test]
fn an_explicit_path_style_beside_an_endpoint_is_accepted_under_either_value() {
    for path_style in [true, false] {
        let password = serde_json::json!({
            "warehouse": "wh",
            "endpoint": "http://s3.example.com",
            "region": "us-east-1",
            "access_key": "AKID",
            "secret_key": "SECRET",
            "path_style": path_style,
        })
        .to_string();
        let ctx = with_conn("http://catalog.example.com", &password);

        read_connection(&ctx, Some("MY_CONN"), CatalogKind::IcebergRest).unwrap_or_else(|err| {
            panic!("an explicit path_style={path_style} beside an endpoint must be accepted: {err}")
        });
    }
}

/// Scenario: the `path_style` guard does not fire without an endpoint or under vending.
#[test]
fn the_path_style_guard_does_not_fire_without_an_endpoint_or_under_vending() {
    let no_endpoint = serde_json::json!({ "warehouse": "wh" }).to_string();
    let ctx = with_conn("http://catalog.example.com", &no_endpoint);
    read_connection(&ctx, Some("MY_CONN"), CatalogKind::IcebergRest)
        .expect("no endpoint at all must not trigger the path_style guard");

    let endpoint_with_vending = serde_json::json!({
        "warehouse": "wh",
        "endpoint": "http://s3.example.com",
        "region": "us-east-1",
        "access_key": "AKID",
        "secret_key": "SECRET",
        "use_vended_credentials": true,
    })
    .to_string();
    let ctx = with_conn("http://catalog.example.com", &endpoint_with_vending);
    read_connection(&ctx, Some("MY_CONN"), CatalogKind::IcebergRest)
        .expect("an endpoint under vending must not trigger the path_style guard");
}

/// Scenario: the `path_style` rejection names no credential value.
#[test]
fn the_path_style_rejection_names_no_credential_value() {
    let password = serde_json::json!({
        "warehouse": "wh",
        "endpoint": "http://s3.example.com",
        "region": "us-east-1",
        "access_key": "AKID_SENTINEL",
        "secret_key": "SECRET_SENTINEL",
    })
    .to_string();
    let ctx = with_conn("http://catalog.example.com", &password);

    let err = read_connection(&ctx, Some("MY_CONN"), CatalogKind::IcebergRest)
        .expect_err("an endpoint without a stated path_style must be rejected")
        .to_string();
    assert!(!err.contains("AKID_SENTINEL"), "{err}");
    assert!(!err.contains("SECRET_SENTINEL"), "{err}");
}

/// Scenario: static storage fields with vending are accepted and unused; other guards still fire.
#[test]
fn static_storage_fields_with_vending_are_accepted_and_unused() {
    let s3_password = serde_json::json!({
        "warehouse": "wh",
        "region": "us-east-1",
        "secret_key": S3_SECRET,
        "use_vended_credentials": true,
    })
    .to_string();
    let ctx = with_conn("http://catalog.example.com", &s3_password);
    let resolved = read_connection(&ctx, Some("MY_CONN"), CatalogKind::IcebergRest)
        .expect("a single S3 credential set together with vending must be accepted");
    assert!(resolved.creds.use_vended_credentials);
    assert_eq!(resolved.creds.secret_key, S3_SECRET);

    let azure_password = serde_json::json!({
        "warehouse": "wh",
        "account_name": "myaccount",
        "account_key": AZURE_ACCOUNT_KEY,
        "use_vended_credentials": true,
    })
    .to_string();
    let ctx = with_conn("http://catalog.example.com", &azure_password);
    let resolved = read_connection(&ctx, Some("MY_CONN"), CatalogKind::IcebergRest)
        .expect("a single Azure credential set together with vending must be accepted");
    assert!(resolved.creds.use_vended_credentials);
    assert_eq!(
        resolved.creds.account_key.as_deref(),
        Some(AZURE_ACCOUNT_KEY)
    );

    let mixed_password = serde_json::json!({
        "warehouse": "wh",
        "account_name": "myaccount",
        "account_key": AZURE_ACCOUNT_KEY,
        "region": "us-east-1",
        "secret_key": S3_SECRET,
        "use_vended_credentials": true,
    })
    .to_string();
    let ctx = with_conn("http://catalog.example.com", &mixed_password);
    let err = read_connection(&ctx, Some("MY_CONN"), CatalogKind::IcebergRest)
        .expect_err("mixed Azure and S3 credential fields must still be rejected under vending")
        .to_string();
    assert!(
        err.contains("Azure and S3 storage credentials cannot both be supplied"),
        "{err}"
    );
    assert!(!err.contains(AZURE_ACCOUNT_KEY), "{err}");
    assert!(!err.contains(S3_SECRET), "{err}");

    let sigv4_password = serde_json::json!({
        "warehouse": "wh",
        "use_sigv4": true,
        "use_vended_credentials": true,
        "secret_key": S3_SECRET,
        "region": "us-east-1",
    })
    .to_string();
    let ctx = with_conn("http://catalog.example.com", &sigv4_password);
    let err = read_connection(&ctx, Some("MY_CONN"), CatalogKind::IcebergRest)
        .expect_err("a missing access_key under SigV4 must still be rejected under vending")
        .to_string();
    assert!(err.contains("access_key"), "must name missing field: {err}");
    assert!(
        err.to_lowercase().contains("sigv4"),
        "must reference SigV4: {err}"
    );
    assert!(!err.contains(S3_SECRET), "{err}");
}

/// Scenario: `storage_block` falls through to S3 for an unvalidated Azure shape, never panicking.
#[test]
fn storage_block_falls_through_to_s3_for_an_unvalidated_azure_shape() {
    let both_credentials = parse_creds(&serde_json::json!({
        "warehouse": "wh",
        "account_name": "myaccount",
        "account_key": AZURE_ACCOUNT_KEY,
        "sas_token": AZURE_SAS,
    }));
    let no_account_name = parse_creds(&serde_json::json!({
        "warehouse": "wh",
        "account_key": AZURE_ACCOUNT_KEY,
    }));

    for creds in [both_credentials, no_account_name] {
        assert_eq!(
            storage_block(&creds, false),
            StorageBackend::S3(StorageProps {
                path_style: false,
                ..Default::default()
            }),
            "neither fixture states path_style, which resolves to false"
        );
    }
}

#[test]
fn catalog_block_maps_creds_to_catalog_props() {
    let ctx = with_conn("http://catalog.example.com", &minimal_password());
    let resolved = read_connection(&ctx, Some("MY_CONN"), CatalogKind::IcebergRest).unwrap();
    let catalog = catalog_block(&resolved.creds, "db.my_table");

    assert_eq!(resolved.uri, "http://catalog.example.com");
    assert_eq!(catalog.warehouse, "wh");
    assert_eq!(catalog.table, "db.my_table");
}

#[test]
fn token_parsed_from_json() {
    let json = serde_json::json!({
        "warehouse": "wh",
        "token": "my-secret-token"
    });
    let creds = parse_creds(&json);

    assert_eq!(creds.token.as_deref(), Some("my-secret-token"));
    assert_eq!(creds.client_id, None);
    assert_eq!(creds.client_secret, None);
    assert_eq!(creds.oauth2_server_uri, None);
    assert_eq!(creds.scope, None);
}

#[test]
fn token_redacted_in_debug_output() {
    let json = serde_json::json!({
        "warehouse": "wh",
        "token": "my-secret-token"
    });
    let creds = parse_creds(&json);
    let debug = format!("{creds:?}");

    assert!(
        !debug.contains("my-secret-token"),
        "token must not appear in Debug: {debug}"
    );
    assert!(
        debug.contains("[redacted]"),
        "Debug must show [redacted] for token: {debug}"
    );
}

#[test]
fn oauth_client_creds_parsed_from_json() {
    let json = serde_json::json!({
        "warehouse": "wh",
        "client_id": "my-client-id",
        "client_secret": "my-client-secret",
        "oauth2_server_uri": "https://auth.example.com/token",
        "scope": "catalog:read"
    });
    let creds = parse_creds(&json);

    assert_eq!(creds.client_id.as_deref(), Some("my-client-id"));
    assert_eq!(creds.client_secret.as_deref(), Some("my-client-secret"));
    assert_eq!(
        creds.oauth2_server_uri.as_deref(),
        Some("https://auth.example.com/token")
    );
    assert_eq!(creds.scope.as_deref(), Some("catalog:read"));
    assert_eq!(creds.token, None);
}

#[test]
fn oauth_optional_fields_absent_when_not_supplied() {
    let json = serde_json::json!({
        "warehouse": "wh",
        "client_id": "my-client-id",
        "client_secret": "my-client-secret"
    });
    let creds = parse_creds(&json);

    assert_eq!(creds.oauth2_server_uri, None);
    assert_eq!(creds.scope, None);
}

#[test]
fn client_secret_redacted_in_debug_output() {
    let json = serde_json::json!({
        "warehouse": "wh",
        "client_id": "my-client-id",
        "client_secret": "my-client-secret"
    });
    let creds = parse_creds(&json);
    let debug = format!("{creds:?}");

    assert!(
        !debug.contains("my-client-secret"),
        "client_secret must not appear in Debug: {debug}"
    );
    assert!(
        debug.contains("[redacted]"),
        "Debug must show [redacted] for client_secret: {debug}"
    );
    // client_id is not a secret, so it may appear.
    assert!(
        debug.contains("my-client-id"),
        "client_id should appear in Debug: {debug}"
    );
}

#[test]
fn has_catalog_auth_true_when_token_present() {
    let json = serde_json::json!({ "warehouse": "wh", "token": "tok" });
    let creds = parse_creds(&json);
    assert!(creds.has_catalog_auth());
}

#[test]
fn has_catalog_auth_true_when_client_creds_present() {
    let json = serde_json::json!({
        "warehouse": "wh",
        "client_id": "id",
        "client_secret": "secret"
    });
    let creds = parse_creds(&json);
    assert!(creds.has_catalog_auth());
}

#[test]
fn has_catalog_auth_true_when_only_client_id_present() {
    // Partial OAuth still signals catalog-auth intent for the SigV4 guard.
    let json = serde_json::json!({ "warehouse": "wh", "client_id": "id" });
    let creds = parse_creds(&json);
    assert!(creds.has_catalog_auth());
}

#[test]
fn has_catalog_auth_false_when_no_auth_fields() {
    let json = serde_json::json!({
        "warehouse": "wh",
        "endpoint": "http://s3.example.com",
        "region": "us-east-1",
        "access_key": "AKID",
        "secret_key": "SECRET"
    });
    let creds = parse_creds(&json);
    assert!(!creds.has_catalog_auth());
}

#[test]
fn new_auth_fields_default_to_none() {
    let json = serde_json::json!({
        "warehouse": "wh",
        "endpoint": "http://s3.example.com",
        "region": "us-east-1",
        "access_key": "AKID",
        "secret_key": "SECRET"
    });
    let creds = parse_creds(&json);

    assert_eq!(creds.token, None);
    assert_eq!(creds.client_id, None);
    assert_eq!(creds.client_secret, None);
    assert_eq!(creds.oauth2_server_uri, None);
    assert_eq!(creds.scope, None);
}

/// Scenario: static S3 credentials are optional regardless of catalog auth mode.
#[test]
fn s3_fields_optional_when_not_sigv4() {
    let pw = serde_json::json!({ "warehouse": "wh" }).to_string();
    let ctx = with_conn("http://catalog.example.com", &pw);
    let resolved = read_connection(&ctx, Some("MY_CONN"), CatalogKind::IcebergRest).unwrap();
    let creds = &resolved.creds;

    assert_eq!(creds.warehouse, "wh");
    assert_eq!(creds.endpoint, "");
    assert_eq!(creds.region, "");
    assert_eq!(creds.access_key, "");
    assert_eq!(creds.secret_key, "");
    assert!(!creds.use_sigv4);

    let pw_with_token = serde_json::json!({
        "warehouse": "wh",
        "token": "my-secret-token"
    })
    .to_string();
    let ctx2 = with_conn("http://catalog.example.com", &pw_with_token);
    read_connection(&ctx2, Some("MY_CONN"), CatalogKind::IcebergRest).unwrap();
}

/// Scenario: with SigV4 enabled, access_key, secret_key, and a signing region are required.
#[test]
fn sigv4_requires_access_secret_region() {
    let make_pw = |fields: serde_json::Value| {
        let mut obj = serde_json::json!({ "warehouse": "wh", "use_sigv4": true });
        if let (serde_json::Value::Object(base), serde_json::Value::Object(extra)) =
            (&mut obj, fields)
        {
            base.extend(extra);
        }
        obj.to_string()
    };

    let pw = make_pw(serde_json::json!({
        "secret_key": "s3cr3t-VALUE",
        "region": "us-east-1"
    }));
    let ctx = with_conn("http://catalog.example.com", &pw);
    let err = read_connection(&ctx, Some("MY_CONN"), CatalogKind::IcebergRest).unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains("access_key"), "must name missing field: {msg}");
    assert!(
        msg.to_lowercase().contains("sigv4"),
        "must reference SigV4: {msg}"
    );
    assert!(!msg.contains("s3cr3t-VALUE"), "must not leak value: {msg}");
    assert!(
        !msg.contains("https://glue.<region>.amazonaws.com"),
        "must not state the Glue-endpoint alternative when region is not named: {msg}"
    );

    let pw = make_pw(serde_json::json!({
        "access_key": "AKID-VALUE",
        "region": "us-east-1"
    }));
    let ctx = with_conn("http://catalog.example.com", &pw);
    let err = read_connection(&ctx, Some("MY_CONN"), CatalogKind::IcebergRest).unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains("secret_key"), "must name missing field: {msg}");
    assert!(
        msg.to_lowercase().contains("sigv4"),
        "must reference SigV4: {msg}"
    );
    assert!(!msg.contains("AKID-VALUE"), "must not leak value: {msg}");

    let pw = make_pw(serde_json::json!({
        "access_key": "AKID-VALUE",
        "secret_key": "s3cr3t-VALUE"
    }));
    let ctx = with_conn("http://catalog.example.com", &pw);
    let err = read_connection(&ctx, Some("MY_CONN"), CatalogKind::IcebergRest).unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains("region"), "must name missing field: {msg}");
    assert!(
        msg.to_lowercase().contains("sigv4"),
        "must reference SigV4: {msg}"
    );
    assert!(!msg.contains("s3cr3t-VALUE"), "must not leak value: {msg}");
    assert!(
        msg.contains("https://glue.<region>.amazonaws.com"),
        "must state the Glue-endpoint alternative when region is named: {msg}"
    );

    let pw = serde_json::json!({
        "warehouse": "wh",
        "use_sigv4": true,
        "use_vended_credentials": true,
        "access_key": "AKID-VALUE",
        "secret_key": "s3cr3t-VALUE"
        // region intentionally absent
    })
    .to_string();
    let ctx = with_conn("http://catalog.example.com", &pw);
    let err = read_connection(&ctx, Some("MY_CONN"), CatalogKind::IcebergRest).unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("region"),
        "must still require region with vended creds: {msg}"
    );
    assert!(!msg.contains("s3cr3t-VALUE"), "must not leak value: {msg}");
    assert!(!msg.contains("AKID-VALUE"), "must not leak value: {msg}");

    let pw = serde_json::json!({
        "warehouse": "wh",
        "use_sigv4": true,
        "access_key": "AKID-VALUE",
        "secret_key": "s3cr3t-VALUE",
        "region": "us-east-1"
        // endpoint intentionally absent
    })
    .to_string();
    let ctx = with_conn("http://catalog.example.com", &pw);
    read_connection(&ctx, Some("MY_CONN"), CatalogKind::IcebergRest)
        .expect("endpoint is optional under SigV4; must not be rejected");
}

/// Scenario: a standard Glue endpoint supplies the SigV4 region; `region` itself stays empty.
#[test]
fn sigv4_region_derived_from_standard_glue_endpoint_is_accepted() {
    let pw = serde_json::json!({
        "warehouse": "wh",
        "use_sigv4": true,
        "access_key": "AKID-VALUE",
        "secret_key": "s3cr3t-VALUE"
    })
    .to_string();
    let ctx = with_conn("https://glue.eu-west-1.amazonaws.com/iceberg", &pw);
    let resolved = read_connection(&ctx, Some("MY_CONN"), CatalogKind::IcebergRest)
        .expect("a standard Glue endpoint must supply the signing region");

    assert_eq!(resolved.creds.region, "");
    assert_eq!(StorageCreds::from(&resolved.creds).region, "");
    assert_eq!(StaticStoreAddress::from(&resolved.creds).region(), "");
}

/// Scenario: a static bearer token is exposed on the creds and redacted in Debug.
#[test]
fn token_exposed_on_creds() {
    let pw = serde_json::json!({
        "warehouse": "wh",
        "token": "my-secret-token"
    })
    .to_string();
    let ctx = with_conn("http://catalog.example.com", &pw);
    let resolved = read_connection(&ctx, Some("MY_CONN"), CatalogKind::IcebergRest).unwrap();
    let creds = &resolved.creds;

    assert_eq!(
        creds.token.as_deref(),
        Some("my-secret-token"),
        "token must be exposed on creds"
    );
    assert!(
        !creds.use_vended_credentials,
        "use_vended_credentials must default false"
    );
    let debug = format!("{creds:?}");
    assert!(
        !debug.contains("my-secret-token"),
        "token value must not appear in Debug: {debug}"
    );
}

/// Scenario: OAuth2 client credentials are exposed on the creds; the secret is redacted in Debug.
#[test]
fn oauth_client_creds_exposed_on_creds() {
    let pw = serde_json::json!({
        "warehouse": "wh",
        "client_id": "my-client-id",
        "client_secret": "my-client-secret"
    })
    .to_string();
    let ctx = with_conn("http://catalog.example.com", &pw);
    let resolved = read_connection(&ctx, Some("MY_CONN"), CatalogKind::IcebergRest).unwrap();
    let creds = &resolved.creds;

    assert_eq!(creds.client_id.as_deref(), Some("my-client-id"));
    assert_eq!(creds.client_secret.as_deref(), Some("my-client-secret"));
    assert_eq!(
        creds.oauth2_server_uri, None,
        "oauth2_server_uri must be absent"
    );
    assert_eq!(creds.scope, None, "scope must be absent");
    assert_eq!(creds.token, None);

    let debug = format!("{creds:?}");
    assert!(
        !debug.contains("my-client-secret"),
        "client_secret must not appear in Debug: {debug}"
    );

    let pw2 = serde_json::json!({
        "warehouse": "wh",
        "client_id": "my-client-id",
        "client_secret": "my-client-secret",
        "oauth2_server_uri": "https://auth.example.com/token",
        "scope": "catalog:read"
    })
    .to_string();
    let ctx2 = with_conn("http://catalog.example.com", &pw2);
    let resolved2 = read_connection(&ctx2, Some("MY_CONN"), CatalogKind::IcebergRest).unwrap();
    let creds2 = &resolved2.creds;

    assert_eq!(
        creds2.oauth2_server_uri.as_deref(),
        Some("https://auth.example.com/token")
    );
    assert_eq!(creds2.scope.as_deref(), Some("catalog:read"));
}

/// Scenario: incomplete OAuth2 client credentials are rejected naming only the missing field.
#[test]
fn incomplete_oauth_rejected_no_leak() {
    let pw = serde_json::json!({
        "warehouse": "wh",
        "client_id": "my-client-id"
    })
    .to_string();
    let ctx = with_conn("http://catalog.example.com", &pw);
    let err = read_connection(&ctx, Some("MY_CONN"), CatalogKind::IcebergRest).unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("client_secret"),
        "must name missing field client_secret: {msg}"
    );
    assert!(!msg.contains("my-client-id"), "must not leak value: {msg}");

    let pw2 = serde_json::json!({
        "warehouse": "wh",
        "client_secret": "my-client-secret"
    })
    .to_string();
    let ctx2 = with_conn("http://catalog.example.com", &pw2);
    let err2 = read_connection(&ctx2, Some("MY_CONN"), CatalogKind::IcebergRest).unwrap_err();
    let msg2 = err2.to_string();
    assert!(
        msg2.contains("client_id"),
        "must name missing field client_id: {msg2}"
    );
    assert!(
        !msg2.contains("my-client-secret"),
        "must not leak value: {msg2}"
    );
}

/// Scenario: a token beside a complete client_id/client_secret pair is rejected under both kinds.
#[test]
fn token_with_complete_oauth_pair_is_rejected_under_both_kinds() {
    let pw = serde_json::json!({
        "warehouse": "wh",
        "token": "sentinel-token-value",
        "client_id": "sentinel-client-id-value",
        "client_secret": "sentinel-client-secret-value"
    })
    .to_string();

    for kind in [CatalogKind::IcebergRest, CatalogKind::UnityCatalogNative] {
        let ctx = with_conn("http://catalog.example.com", &pw);
        let err = read_connection(&ctx, Some("MY_CONN"), kind).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("token"), "must name token: {msg}");
        assert!(msg.contains("client_id"), "must name client_id: {msg}");
        assert!(
            msg.contains("client_secret"),
            "must name client_secret: {msg}"
        );
        assert!(
            !msg.contains("sentinel-token-value"),
            "must not leak token value: {msg}"
        );
        assert!(
            !msg.contains("sentinel-client-id-value"),
            "must not leak client_id value: {msg}"
        );
        assert!(
            !msg.contains("sentinel-client-secret-value"),
            "must not leak client_secret value: {msg}"
        );
    }

    // token + client_id only: the ambiguous-pair rule must not fire; OAuth2 completeness does.
    let pw_partial = serde_json::json!({
        "warehouse": "wh",
        "token": "sentinel-token-value",
        "client_id": "sentinel-client-id-value"
    })
    .to_string();
    let ctx = with_conn("http://catalog.example.com", &pw_partial);
    let err = read_connection(&ctx, Some("MY_CONN"), CatalogKind::IcebergRest).unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("client_secret"),
        "must name missing client_secret: {msg}"
    );
    assert!(
        msg.contains("missing field: client_secret"),
        "rule 7 must fire, not rule 6: {msg}"
    );
    assert!(
        !msg.contains("mutually exclusive"),
        "rule 6 must not fire on a token beside half a pair: {msg}"
    );
}

/// Scenario: catalog token/OAuth auth and SigV4 are mutually exclusive.
#[test]
fn sigv4_and_catalog_auth_mutually_exclusive() {
    let pw_sigv4_token = serde_json::json!({
        "warehouse": "wh",
        "access_key": "AKID",
        "secret_key": "s3cr3t-VALUE",
        "region": "us-east-1",
        "use_sigv4": true,
        "token": "my-secret-token"
    })
    .to_string();
    let ctx = with_conn("http://catalog.example.com", &pw_sigv4_token);
    let err = read_connection(&ctx, Some("MY_CONN"), CatalogKind::IcebergRest).unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.to_lowercase().contains("sigv4"),
        "error must reference SigV4: {msg}"
    );
    assert!(
        !msg.contains("my-secret-token"),
        "must not leak token value: {msg}"
    );
    assert!(
        !msg.contains("s3cr3t-VALUE"),
        "must not leak secret_key value: {msg}"
    );

    let pw_sigv4_oauth = serde_json::json!({
        "warehouse": "wh",
        "access_key": "AKID",
        "secret_key": "s3cr3t-VALUE",
        "region": "us-east-1",
        "use_sigv4": true,
        "client_id": "my-client-id",
        "client_secret": "my-client-secret"
    })
    .to_string();
    let ctx2 = with_conn("http://catalog.example.com", &pw_sigv4_oauth);
    let err2 = read_connection(&ctx2, Some("MY_CONN"), CatalogKind::IcebergRest).unwrap_err();
    let msg2 = err2.to_string();
    assert!(
        msg2.to_lowercase().contains("sigv4"),
        "error must reference SigV4: {msg2}"
    );
    assert!(
        !msg2.contains("my-client-secret"),
        "must not leak client_secret: {msg2}"
    );
    assert!(
        !msg2.contains("s3cr3t-VALUE"),
        "must not leak secret_key: {msg2}"
    );
}

/// Scenario: under Unity Catalog, `warehouse` is optional and SigV4 is rejected.
#[test]
fn unity_kind_validation_skips_warehouse_and_rejects_sigv4() {
    let no_warehouse = serde_json::json!({ "token": "tok" }).to_string();
    let ctx = with_conn("http://catalog.example.com", &no_warehouse);
    let resolved = read_connection(&ctx, Some("MY_CONN"), CatalogKind::UnityCatalogNative)
        .expect("a Unity Catalog CONNECTION without warehouse must be accepted");
    assert_eq!(
        resolved.creds.warehouse, "",
        "warehouse stays empty and is not required under the Unity kind"
    );

    // SigV4 is rejected as a Unity-mode conflict even without its own required fields.
    let sigv4 = serde_json::json!({
        "use_sigv4": true,
        "secret_key": "SUPERSECRET"
    })
    .to_string();
    let ctx = with_conn("http://catalog.example.com", &sigv4);
    let err = read_connection(&ctx, Some("MY_CONN"), CatalogKind::UnityCatalogNative)
        .expect_err("SigV4 signing must be rejected under the Unity kind");
    let msg = err.to_string();
    assert!(msg.contains("SigV4"), "must name SigV4 signing: {msg}");
    assert!(
        msg.contains("Unity Catalog"),
        "must state SigV4 is not a Unity Catalog authentication mode: {msg}"
    );
    assert!(
        !msg.contains("access_key"),
        "must be the Unity-mode rejection, not the generic SigV4 missing-field error: {msg}"
    );
    assert!(
        !msg.contains("SUPERSECRET"),
        "must not leak any supplied credential value: {msg}"
    );
}

/// Scenario: under Iceberg REST, a missing `warehouse` is still rejected.
#[test]
fn iceberg_kind_validation_still_requires_warehouse() {
    let no_warehouse = serde_json::json!({
        "endpoint": "http://s3.example.com",
        "region": "us-east-1"
    })
    .to_string();
    let ctx = with_conn("http://catalog.example.com", &no_warehouse);
    let err = read_connection(&ctx, Some("MY_CONN"), CatalogKind::IcebergRest)
        .expect_err("Iceberg REST still requires warehouse");
    assert_eq!(
        err.to_string(),
        "CONNECTION 'MY_CONN' password is missing required field: warehouse"
    );
}

/// Scenario: the same CONNECTION validates differently by `CatalogKind`.
#[test]
fn validation_is_parameterized_by_catalog_kind() {
    let no_warehouse = serde_json::json!({ "token": "tok" }).to_string();
    let ctx = with_conn("http://catalog.example.com", &no_warehouse);
    assert!(
        read_connection(&ctx, Some("MY_CONN"), CatalogKind::IcebergRest).is_err(),
        "missing warehouse is rejected under Iceberg REST"
    );
    assert!(
        read_connection(&ctx, Some("MY_CONN"), CatalogKind::UnityCatalogNative).is_ok(),
        "missing warehouse is accepted under Unity Catalog"
    );

    let sigv4 = serde_json::json!({
        "warehouse": "wh",
        "use_sigv4": true,
        "access_key": "AKID",
        "secret_key": "SECRET",
        "region": "us-east-1"
    })
    .to_string();
    let ctx = with_conn("http://catalog.example.com", &sigv4);
    assert!(
        read_connection(&ctx, Some("MY_CONN"), CatalogKind::IcebergRest).is_ok(),
        "a complete SigV4 set is accepted under Iceberg REST"
    );
    assert!(
        read_connection(&ctx, Some("MY_CONN"), CatalogKind::UnityCatalogNative).is_err(),
        "SigV4 is rejected under Unity Catalog"
    );
}

/// Scenario: a Unity CONNECTION reuses the existing auth fields; a no-auth one is accepted.
#[test]
fn unity_connection_reuses_existing_auth_fields() {
    let oauth = serde_json::json!({
        "client_id": "my-client-id",
        "client_secret": "my-client-secret",
        "oauth2_server_uri": "https://auth.example.com/token",
        "scope": "catalog:read"
    })
    .to_string();
    let ctx = with_conn("http://catalog.example.com", &oauth);
    let resolved = read_connection(&ctx, Some("MY_CONN"), CatalogKind::UnityCatalogNative)
        .expect("a Unity CONNECTION with OAuth client credentials must be accepted");
    let creds = &resolved.creds;
    assert_eq!(creds.client_id.as_deref(), Some("my-client-id"));
    assert_eq!(creds.client_secret.as_deref(), Some("my-client-secret"));
    assert_eq!(
        creds.oauth2_server_uri.as_deref(),
        Some("https://auth.example.com/token")
    );
    assert_eq!(creds.scope.as_deref(), Some("catalog:read"));
    let debug = format!("{creds:?}");
    assert!(
        !debug.contains("my-client-secret"),
        "client_secret must not leak through Debug: {debug}"
    );

    let bearer = serde_json::json!({ "token": "my-secret-token" }).to_string();
    let ctx = with_conn("http://catalog.example.com", &bearer);
    let resolved = read_connection(&ctx, Some("MY_CONN"), CatalogKind::UnityCatalogNative)
        .expect("a Unity CONNECTION with a bearer token must be accepted");
    assert_eq!(resolved.creds.token.as_deref(), Some("my-secret-token"));

    // OSS Unity Catalog can run with authentication disabled.
    let no_auth = serde_json::json!({}).to_string();
    let ctx = with_conn("http://catalog.example.com", &no_auth);
    let resolved = read_connection(&ctx, Some("MY_CONN"), CatalogKind::UnityCatalogNative)
        .expect("a Unity CONNECTION with no auth fields must be accepted");
    assert_eq!(resolved.creds.token, None);
    assert_eq!(resolved.creds.client_id, None);
    assert_eq!(resolved.creds.client_secret, None);
}

#[test]
fn sealing_key_is_absent_for_a_password_carrying_no_secret_field_at_the_read_connection_boundary() {
    let keyless = serde_json::json!({"warehouse": "wh"}).to_string();
    let ctx = with_conn("http://catalog.example.com", &keyless);
    let resolved = read_connection(&ctx, Some("MY_CONN"), CatalogKind::IcebergRest)
        .expect("a warehouse-only password is an acceptable CONNECTION");
    assert_eq!(
        resolved.creds.warehouse, "wh",
        "positive control: the CONNECTION must actually have resolved"
    );
    assert!(
        resolved.sealed_storage_key.is_none(),
        "a password holding only a warehouse carries no key material, so no key \
         may exist on the Resolved value at all"
    );

    let with_secret = serde_json::json!({
        "warehouse": "wh",
        "region": "us-east-1",
        "access_key": "AKID",
        "secret_key": "SECRET",
    })
    .to_string();
    let ctx = with_conn("http://catalog.example.com", &with_secret);
    let resolved = read_connection(&ctx, Some("MY_CONN"), CatalogKind::IcebergRest)
        .expect("the installer's own default template shape must be accepted");
    assert!(
        resolved.sealed_storage_key.is_some(),
        "a non-empty secret_key is key material, so the boundary must derive a key"
    );
}

#[test]
fn direct_storage_requires_a_non_empty_storage_base_path() {
    let ctx = with_conn("", &serde_json::json!({}).to_string());

    let err = read_connection(&ctx, Some("MY_CONN"), CatalogKind::DirectStorage)
        .expect_err("an empty CONNECTION address must be rejected under direct storage");
    let msg = err.to_string();

    assert!(msg.contains("MY_CONN"), "must name the CONNECTION: {msg}");
    assert!(
        msg.to_lowercase().contains("storage base path"),
        "must state that a storage base path is expected, not a catalog URI: {msg}"
    );
    assert!(
        !msg.to_lowercase().contains("catalog uri"),
        "must not tell the operator to supply a catalog URI this kind never contacts: {msg}"
    );
}

#[test]
fn direct_storage_accepts_a_connection_without_warehouse() {
    let password = serde_json::json!({
        "access_key": "AKID",
        "secret_key": "SECRET",
        "region": "us-east-1",
    })
    .to_string();
    let ctx = with_conn("s3://bucket/lake", &password);

    let resolved = read_connection(&ctx, Some("MY_CONN"), CatalogKind::DirectStorage)
        .expect("a direct-storage CONNECTION without warehouse must be accepted");
    assert_eq!(resolved.creds.warehouse, "");
}

#[test]
fn direct_storage_rejects_every_catalog_auth_and_vending_field() {
    let cases: [(serde_json::Value, &str, &str); 6] = [
        (
            serde_json::json!({"warehouse": "wh", "access_key": "AKID", "secret_key": "SECRET"}),
            "warehouse",
            "wh",
        ),
        (
            serde_json::json!({"token": "SENTINEL_TOKEN", "access_key": "AKID", "secret_key": "SECRET"}),
            "token",
            "SENTINEL_TOKEN",
        ),
        (
            serde_json::json!({"client_id": "SENTINEL_CID", "access_key": "AKID", "secret_key": "SECRET"}),
            "client_id",
            "SENTINEL_CID",
        ),
        (
            serde_json::json!({"client_secret": "SENTINEL_CSECRET", "access_key": "AKID", "secret_key": "SECRET"}),
            "client_secret",
            "SENTINEL_CSECRET",
        ),
        (
            serde_json::json!({"oauth2_server_uri": "https://auth.example.com", "access_key": "AKID", "secret_key": "SECRET"}),
            "oauth2_server_uri",
            "https://auth.example.com",
        ),
        (
            serde_json::json!({"scope": "SENTINEL_SCOPE", "access_key": "AKID", "secret_key": "SECRET"}),
            "scope",
            "SENTINEL_SCOPE",
        ),
    ];

    for (password, field, value) in cases {
        let ctx = with_conn("s3://bucket/lake", &password.to_string());
        let err = read_connection(&ctx, Some("MY_CONN"), CatalogKind::DirectStorage)
            .expect_err(&format!("{field} must be rejected under direct storage"));
        let msg = err.to_string();
        assert!(msg.contains(field), "must name {field}: {msg}");
        assert!(
            !msg.contains(value),
            "must not leak the value of {field}: {msg}"
        );
    }
}

#[test]
fn direct_storage_rejects_use_sigv4_and_use_vended_credentials_when_true() {
    for field in ["use_sigv4", "use_vended_credentials"] {
        let password = serde_json::json!({
            field: true,
            "access_key": "AKID",
            "secret_key": "SECRET",
        })
        .to_string();
        let ctx = with_conn("s3://bucket/lake", &password);

        let err = read_connection(&ctx, Some("MY_CONN"), CatalogKind::DirectStorage).expect_err(
            &format!("{field}=true must be rejected under direct storage"),
        );
        assert!(err.to_string().contains(field), "{}", err);
    }
}

#[test]
fn direct_storage_accepts_use_sigv4_and_use_vended_credentials_when_explicitly_false() {
    for field in ["use_sigv4", "use_vended_credentials"] {
        let password = serde_json::json!({
            field: false,
            "access_key": "AKID",
            "secret_key": "SECRET",
            "region": "us-east-1",
        })
        .to_string();
        let ctx = with_conn("s3://bucket/lake", &password);

        read_connection(&ctx, Some("MY_CONN"), CatalogKind::DirectStorage).unwrap_or_else(|err| {
            panic!("{field}=false explicitly must be accepted under direct storage: {err}")
        });
    }
}

#[test]
fn direct_storage_rejects_s3_scheme_with_azure_shaped_credentials() {
    let password = serde_json::json!({
        "account_name": "myaccount",
        "account_key": AZURE_ACCOUNT_KEY,
    })
    .to_string();
    let ctx = with_conn("s3://bucket/lake", &password);

    let err = read_connection(&ctx, Some("MY_CONN"), CatalogKind::DirectStorage)
        .expect_err("an s3:// address with Azure-shaped credentials must be rejected");
    let msg = err.to_string();
    assert!(msg.contains("s3"), "{msg}");
    assert!(!msg.contains(AZURE_ACCOUNT_KEY), "{msg}");
}

#[test]
fn direct_storage_rejects_abfss_scheme_with_s3_shaped_credentials() {
    let password = serde_json::json!({
        "access_key": "AKID",
        "secret_key": S3_SECRET,
    })
    .to_string();
    let ctx = with_conn(
        "abfss://container@account.dfs.core.windows.net/lake",
        &password,
    );

    let err = read_connection(&ctx, Some("MY_CONN"), CatalogKind::DirectStorage)
        .expect_err("an abfss:// address with S3-shaped credentials must be rejected");
    let msg = err.to_string();
    assert!(msg.contains("abfss"), "{msg}");
    assert!(!msg.contains(S3_SECRET), "{msg}");
}

#[test]
fn direct_storage_rejects_plaintext_abfs_naming_abfss() {
    let password = serde_json::json!({
        "account_name": "myaccount",
        "account_key": AZURE_ACCOUNT_KEY,
    })
    .to_string();
    let ctx = with_conn(
        "abfs://container@account.dfs.core.windows.net/lake",
        &password,
    );

    let err = read_connection(&ctx, Some("MY_CONN"), CatalogKind::DirectStorage)
        .expect_err("a plaintext abfs:// address must be rejected under direct storage");
    let msg = err.to_string();
    assert!(
        msg.contains("abfss"),
        "must name 'abfss' as the secure spelling: {msg}"
    );
    assert!(!msg.contains(AZURE_ACCOUNT_KEY), "{msg}");
}

#[test]
fn direct_storage_accepts_matching_scheme_and_credential_shape() {
    let s3_password = serde_json::json!({
        "access_key": "AKID",
        "secret_key": "SECRET",
        "region": "us-east-1",
    })
    .to_string();
    let ctx = with_conn("s3://bucket/lake", &s3_password);
    read_connection(&ctx, Some("MY_CONN"), CatalogKind::DirectStorage)
        .expect("an s3:// address with S3-shaped credentials must be accepted");

    let azure_password = serde_json::json!({
        "account_name": "myaccount",
        "account_key": AZURE_ACCOUNT_KEY,
    })
    .to_string();
    let ctx = with_conn(
        "abfss://container@account.dfs.core.windows.net/lake",
        &azure_password,
    );
    read_connection(&ctx, Some("MY_CONN"), CatalogKind::DirectStorage)
        .expect("an abfss:// address with Azure-shaped credentials must be accepted");
}
