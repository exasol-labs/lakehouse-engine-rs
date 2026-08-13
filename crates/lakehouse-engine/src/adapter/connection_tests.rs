use super::*;
use crate::adapter::catalog_kind::CatalogKind;
use exasol_udf_sdk::connect_back::ConnectionObject;
use exasol_udf_sdk::error::UdfError;
use exasol_udf_sdk::value::Value;

// ---------------------------------------------------------------------------
// Minimal UdfContext stub for unit tests
// ---------------------------------------------------------------------------

struct StubCtx {
    conn: Option<ConnectionObject>,
}

impl StubCtx {
    fn with_conn(address: &str, password: &str) -> Self {
        StubCtx {
            conn: Some(ConnectionObject {
                kind: "PASSWORD".into(),
                address: address.to_string(),
                user: "".to_string(),
                password: password.to_string(),
            }),
        }
    }

    fn no_conn() -> Self {
        StubCtx { conn: None }
    }
}

impl UdfContext for StubCtx {
    fn num_columns(&self) -> usize {
        0
    }
    fn get(&self, _col: usize) -> Result<&Value, UdfError> {
        Err(UdfError::Type("none".into()))
    }
    fn emit(&mut self, _values: &[Value]) -> Result<(), UdfError> {
        Ok(())
    }
    fn next(&mut self) -> Result<bool, UdfError> {
        Ok(false)
    }
    fn connection(&self, _name: &str) -> Result<ConnectionObject, UdfError> {
        self.conn
            .clone()
            .ok_or_else(|| UdfError::User("no connection".into()))
    }
}

fn minimal_password() -> String {
    serde_json::json!({
        "warehouse": "wh",
        "endpoint": "http://s3.example.com",
        "region": "us-east-1",
        "access_key": "AKID",
        "secret_key": "SECRET"
    })
    .to_string()
}

// ---------------------------------------------------------------------------
// Scenario: read_connection_parses_uri_and_creds
// ---------------------------------------------------------------------------

#[test]
fn read_connection_parses_uri_and_creds() {
    let ctx = StubCtx::with_conn("http://catalog.example.com", &minimal_password());
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
    // path_style defaults to true (MinIO behaviour preserved)
    assert!(resolved.creds.path_style);
}

// ---------------------------------------------------------------------------
// Scenario: missing_connection_name_errors
// ---------------------------------------------------------------------------

#[test]
fn missing_connection_name_errors() {
    let ctx = StubCtx::no_conn();

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

    // No credential value in error
    assert!(!err_none.to_string().contains("SECRET"));
    assert!(!err_empty.to_string().contains("SECRET"));
}

// ---------------------------------------------------------------------------
// Scenario: malformed_password_no_leak
// ---------------------------------------------------------------------------

#[test]
fn malformed_password_no_leak() {
    let bad_password = "not-json-at-all SECRET_VALUE_HERE";
    let ctx = StubCtx::with_conn("http://catalog.example.com", bad_password);
    let err = read_connection(&ctx, Some("MY_CONN"), CatalogKind::IcebergRest).unwrap_err();

    // Error must say it's not a valid JSON object
    assert!(err.to_string().contains("not a valid JSON object"));
    // Must NOT echo the password text
    assert!(!err.to_string().contains("not-json-at-all"));
    assert!(!err.to_string().contains("SECRET_VALUE_HERE"));
}

#[test]
fn json_array_password_no_leak() {
    // Valid JSON but not an object (array) — should also be rejected
    let array_password = r#"["SECRET_IN_ARRAY", "OTHER_VAL"]"#;
    let ctx = StubCtx::with_conn("http://catalog.example.com", array_password);
    let err = read_connection(&ctx, Some("MY_CONN"), CatalogKind::IcebergRest).unwrap_err();

    assert!(err.to_string().contains("not a valid JSON object"));
    assert!(!err.to_string().contains("SECRET_IN_ARRAY"));
}

// ---------------------------------------------------------------------------
// Scenario: missing_required_fields_listed
// ---------------------------------------------------------------------------

#[test]
fn missing_warehouse_rejected_s3_not_required() {
    // Password omitting warehouse — only warehouse is required; the four S3
    // fields are optional and must NOT be reported as missing.
    let no_warehouse = serde_json::json!({
        "endpoint": "http://s3.example.com",
        "region": "us-east-1"
    })
    .to_string();
    let ctx = StubCtx::with_conn("http://catalog.example.com", &no_warehouse);
    let err = read_connection(&ctx, Some("MY_CONN"), CatalogKind::IcebergRest).unwrap_err();

    let msg = err.to_string();
    assert!(
        msg.contains("warehouse"),
        "must name missing field 'warehouse': {msg}"
    );
    // The optional S3 fields must NOT be reported as missing.
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
    // Warehouse alone is sufficient when SigV4 is not enabled; the four S3
    // fields default to empty (orthogonality + over-strictness fix).
    let partial = serde_json::json!({ "warehouse": "wh" }).to_string();
    let ctx = StubCtx::with_conn("http://catalog.example.com", &partial);
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
    // Backward-compat guard: a legacy full static-S3 password (warehouse + the
    // four S3 fields) validates and parses identically to before.
    let ctx = StubCtx::with_conn("http://catalog.example.com", &minimal_password());
    let resolved = read_connection(&ctx, Some("MY_CONN"), CatalogKind::IcebergRest).unwrap();
    let creds = &resolved.creds;

    assert_eq!(creds.warehouse, "wh");
    assert_eq!(creds.endpoint, "http://s3.example.com");
    assert_eq!(creds.region, "us-east-1");
    assert_eq!(creds.access_key, "AKID");
    assert_eq!(creds.secret_key, "SECRET");
}

// ---------------------------------------------------------------------------
// Scenario: optional_fields_default
// ---------------------------------------------------------------------------

/// Warehouse-only password: the four S3 fields AND the five new auth fields
/// must all default to absent/empty; use_sigv4 and use_vended_credentials
/// must default false.
#[test]
fn optional_fields_default() {
    let warehouse_only = serde_json::json!({ "warehouse": "wh" }).to_string();
    let ctx = StubCtx::with_conn("http://catalog.example.com", &warehouse_only);
    let resolved = read_connection(&ctx, Some("MY_CONN"), CatalogKind::IcebergRest).unwrap();
    let creds = &resolved.creds;

    // The four S3 fields default to empty when not supplied.
    assert_eq!(creds.endpoint, "", "endpoint must default to empty");
    assert_eq!(creds.region, "", "region must default to empty");
    assert_eq!(creds.access_key, "", "access_key must default to empty");
    assert_eq!(creds.secret_key, "", "secret_key must default to empty");

    // The five new auth fields default to None when not supplied.
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

    // Flags default to false.
    assert!(!creds.use_sigv4, "use_sigv4 must default to false");
    assert!(
        !creds.use_vended_credentials,
        "use_vended_credentials must default to false"
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
    let ctx = StubCtx::with_conn("http://catalog.example.com", &password);
    let resolved = read_connection(&ctx, Some("MY_CONN"), CatalogKind::IcebergRest).unwrap();
    let creds = &resolved.creds;

    assert_eq!(creds.session_token.as_deref(), Some("STS_TOKEN"));
    assert!(!creds.path_style);
    assert!(creds.use_sigv4);
    assert!(creds.use_vended_credentials);
}

// ---------------------------------------------------------------------------
// storage_block and catalog_block helpers
// ---------------------------------------------------------------------------

#[test]
fn storage_block_maps_creds_to_storage_props() {
    let ctx = StubCtx::with_conn("http://catalog.example.com", &minimal_password());
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

/// Distinctive so a "this value never reaches an error" assertion cannot pass
/// by accident: no substring of these appears in any field name or fixed
/// message text.
const AZURE_ACCOUNT_KEY: &str = "azure-shared-key-must-never-leak";
const AZURE_SAS: &str = "sv=2024-01-01&sig=azure-sas-signature-must-never-leak";
const S3_SECRET: &str = "s3-secret-must-never-leak";

/// The account-key shape resolves to `AdlsCred::AccountKey` and leaves
/// `sas_token` absent — the two Azure credential fields are never both
/// populated on a well-formed CONNECTION.
#[test]
fn account_key_creds_select_the_adls_backend() {
    let password = serde_json::json!({
        "warehouse": "wh",
        "account_name": "myaccount",
        "account_key": AZURE_ACCOUNT_KEY,
    })
    .to_string();
    let ctx = StubCtx::with_conn("http://catalog.example.com", &password);

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

/// `allow_http` is an S3-only knob, so passing it enabled must leave the Azure
/// payload identical — the variant carries no HTTP-scheme field at all.
#[test]
fn sas_token_creds_select_the_adls_backend() {
    let password = serde_json::json!({
        "warehouse": "wh",
        "account_name": "myaccount",
        "sas_token": AZURE_SAS,
    })
    .to_string();
    let ctx = StubCtx::with_conn("http://catalog.example.com", &password);

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

/// The three malformed Azure shapes: no account name, both credentials, and
/// neither credential. Each names its own defect and no supplied value.
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
        let ctx = StubCtx::with_conn("http://catalog.example.com", &password.to_string());

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

/// The rejection names every supplied field so the operator can see which
/// two credential sets collided, while echoing none of their values.
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
    let ctx = StubCtx::with_conn("http://catalog.example.com", &password);

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

/// A CONNECTION naming no Azure field is still an S3 CONNECTION, whether or
/// not it requests vended credentials.
#[test]
fn absent_optional_fields_default_and_still_select_s3() {
    for use_vended_credentials in [false, true] {
        let password = serde_json::json!({
            "warehouse": "wh",
            "use_vended_credentials": use_vended_credentials,
        })
        .to_string();
        let ctx = StubCtx::with_conn("http://catalog.example.com", &password);

        let resolved = read_connection(&ctx, Some("MY_CONN"), CatalogKind::IcebergRest).unwrap();

        assert_eq!(resolved.creds.account_name, None);
        assert_eq!(resolved.creds.account_key, None);
        assert_eq!(resolved.creds.sas_token, None);
        assert_eq!(
            storage_block(&resolved.creds, false),
            StorageBackend::S3(StorageProps::default())
        );
    }
}

/// A single well-formed credential set (S3 XOR Azure) together with
/// `use_vended_credentials = true` is ACCEPTED: `validate_creds` never reads that
/// flag, because SigV4 catalog signing needs `access_key`/`secret_key` regardless
/// of whether storage credentials end up vended. Static fields under vending go
/// unused, never rejected.
///
/// Also pins the mixed-fields guard and the SigV4 field requirement WITH vending
/// requested, so a regression skipping validation under vending would not pass
/// unnoticed.
#[test]
fn static_storage_fields_with_vending_are_accepted_and_unused() {
    let s3_password = serde_json::json!({
        "warehouse": "wh",
        "region": "us-east-1",
        "secret_key": S3_SECRET,
        "use_vended_credentials": true,
    })
    .to_string();
    let ctx = StubCtx::with_conn("http://catalog.example.com", &s3_password);
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
    let ctx = StubCtx::with_conn("http://catalog.example.com", &azure_password);
    let resolved = read_connection(&ctx, Some("MY_CONN"), CatalogKind::IcebergRest)
        .expect("a single Azure credential set together with vending must be accepted");
    assert!(resolved.creds.use_vended_credentials);
    assert_eq!(
        resolved.creds.account_key.as_deref(),
        Some(AZURE_ACCOUNT_KEY)
    );

    // The mixed-fields guard still fires under vending.
    let mixed_password = serde_json::json!({
        "warehouse": "wh",
        "account_name": "myaccount",
        "account_key": AZURE_ACCOUNT_KEY,
        "region": "us-east-1",
        "secret_key": S3_SECRET,
        "use_vended_credentials": true,
    })
    .to_string();
    let ctx = StubCtx::with_conn("http://catalog.example.com", &mixed_password);
    let err = read_connection(&ctx, Some("MY_CONN"), CatalogKind::IcebergRest)
        .expect_err("mixed Azure and S3 credential fields must still be rejected under vending")
        .to_string();
    assert!(
        err.contains("Azure and S3 storage credentials cannot both be supplied"),
        "{err}"
    );
    assert!(!err.contains(AZURE_ACCOUNT_KEY), "{err}");
    assert!(!err.contains(S3_SECRET), "{err}");

    // The SigV4 field requirement still fires under vending.
    let sigv4_password = serde_json::json!({
        "warehouse": "wh",
        "use_sigv4": true,
        "use_vended_credentials": true,
        "secret_key": S3_SECRET,
        "region": "us-east-1",
    })
    .to_string();
    let ctx = StubCtx::with_conn("http://catalog.example.com", &sigv4_password);
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

/// `storage_block` is total. A credential set that never passed
/// `validate_creds` — two Azure credentials at once, or a credential with no
/// account name — resolves deterministically to S3 instead of panicking,
/// because a panic inside a UDF is an abnormal VM exit and the engine
/// SIGKILLs every sibling VM of the statement part when one dies that way.
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
            StorageBackend::S3(StorageProps::default())
        );
    }
}

#[test]
fn catalog_block_maps_creds_to_catalog_props() {
    let ctx = StubCtx::with_conn("http://catalog.example.com", &minimal_password());
    let resolved = read_connection(&ctx, Some("MY_CONN"), CatalogKind::IcebergRest).unwrap();
    let catalog = catalog_block(&resolved.creds, "db.my_table");

    assert_eq!(resolved.uri, "http://catalog.example.com");
    assert_eq!(catalog.warehouse, "wh");
    assert_eq!(catalog.table, "db.my_table");
}

// ---------------------------------------------------------------------------
// Scenario: token is parsed and exposed; no leak via Debug
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// Scenario: OAuth2 client credentials are parsed and exposed; secret not leaked
// ---------------------------------------------------------------------------

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
    // client_id is not a secret — it may appear
    assert!(
        debug.contains("my-client-id"),
        "client_id should appear in Debug: {debug}"
    );
}

// ---------------------------------------------------------------------------
// Scenario: has_catalog_auth helper
// ---------------------------------------------------------------------------

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
    // Even partial oauth signals catalog-auth intent (incomplete; validation rejects it,
    // but the helper reports presence for the SigV4 guard)
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

// ---------------------------------------------------------------------------
// Scenario: new auth fields absent when not supplied (optional-defaults)
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// Scenario Coverage tests — exact names from the plan's Scenario Coverage table
// ---------------------------------------------------------------------------

/// Static S3 credentials are optional regardless of catalog auth mode.
///
/// A warehouse-only password with use_sigv4=false must be accepted; all four
/// S3 fields default to empty without triggering an error.
#[test]
fn s3_fields_optional_when_not_sigv4() {
    // No S3 fields, no auth fields — just warehouse.
    let pw = serde_json::json!({ "warehouse": "wh" }).to_string();
    let ctx = StubCtx::with_conn("http://catalog.example.com", &pw);
    let resolved = read_connection(&ctx, Some("MY_CONN"), CatalogKind::IcebergRest).unwrap();
    let creds = &resolved.creds;

    assert_eq!(creds.warehouse, "wh");
    assert_eq!(creds.endpoint, "");
    assert_eq!(creds.region, "");
    assert_eq!(creds.access_key, "");
    assert_eq!(creds.secret_key, "");
    assert!(!creds.use_sigv4);

    // Same should hold when a token is present (token + warehouse, still no S3).
    let pw_with_token = serde_json::json!({
        "warehouse": "wh",
        "token": "my-secret-token"
    })
    .to_string();
    let ctx2 = StubCtx::with_conn("http://catalog.example.com", &pw_with_token);
    read_connection(&ctx2, Some("MY_CONN"), CatalogKind::IcebergRest).unwrap();
}

/// When SigV4 is enabled, access_key, secret_key, and region are required.
///
/// Asserts:
/// - Missing any of the three fields with use_sigv4=true → rejected; error names the
///   missing field(s) and references SigV4; no value leaked.
/// - Fires identically when use_vended_credentials is also true.
/// - A missing `endpoint` alone does NOT trigger rejection under SigV4.
#[test]
fn sigv4_requires_access_secret_region() {
    // Helper: build a password with use_sigv4=true and only the supplied S3 fields.
    let make_pw = |fields: serde_json::Value| {
        let mut obj = serde_json::json!({ "warehouse": "wh", "use_sigv4": true });
        if let (serde_json::Value::Object(base), serde_json::Value::Object(extra)) =
            (&mut obj, fields)
        {
            base.extend(extra);
        }
        obj.to_string()
    };

    // --- Missing access_key ---
    let pw = make_pw(serde_json::json!({
        "secret_key": "s3cr3t-VALUE",
        "region": "us-east-1"
    }));
    let ctx = StubCtx::with_conn("http://catalog.example.com", &pw);
    let err = read_connection(&ctx, Some("MY_CONN"), CatalogKind::IcebergRest).unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains("access_key"), "must name missing field: {msg}");
    assert!(
        msg.to_lowercase().contains("sigv4"),
        "must reference SigV4: {msg}"
    );
    assert!(!msg.contains("s3cr3t-VALUE"), "must not leak value: {msg}");

    // --- Missing secret_key ---
    let pw = make_pw(serde_json::json!({
        "access_key": "AKID-VALUE",
        "region": "us-east-1"
    }));
    let ctx = StubCtx::with_conn("http://catalog.example.com", &pw);
    let err = read_connection(&ctx, Some("MY_CONN"), CatalogKind::IcebergRest).unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains("secret_key"), "must name missing field: {msg}");
    assert!(
        msg.to_lowercase().contains("sigv4"),
        "must reference SigV4: {msg}"
    );
    assert!(!msg.contains("AKID-VALUE"), "must not leak value: {msg}");

    // --- Missing region ---
    let pw = make_pw(serde_json::json!({
        "access_key": "AKID-VALUE",
        "secret_key": "s3cr3t-VALUE"
    }));
    let ctx = StubCtx::with_conn("http://catalog.example.com", &pw);
    let err = read_connection(&ctx, Some("MY_CONN"), CatalogKind::IcebergRest).unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains("region"), "must name missing field: {msg}");
    assert!(
        msg.to_lowercase().contains("sigv4"),
        "must reference SigV4: {msg}"
    );
    assert!(!msg.contains("s3cr3t-VALUE"), "must not leak value: {msg}");

    // --- Fires also when use_vended_credentials = true ---
    let pw = serde_json::json!({
        "warehouse": "wh",
        "use_sigv4": true,
        "use_vended_credentials": true,
        "access_key": "AKID-VALUE",
        "secret_key": "s3cr3t-VALUE"
        // region intentionally absent
    })
    .to_string();
    let ctx = StubCtx::with_conn("http://catalog.example.com", &pw);
    let err = read_connection(&ctx, Some("MY_CONN"), CatalogKind::IcebergRest).unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("region"),
        "must still require region with vended creds: {msg}"
    );
    assert!(!msg.contains("s3cr3t-VALUE"), "must not leak value: {msg}");
    assert!(!msg.contains("AKID-VALUE"), "must not leak value: {msg}");

    // --- Missing endpoint alone does NOT trigger rejection ---
    let pw = serde_json::json!({
        "warehouse": "wh",
        "use_sigv4": true,
        "access_key": "AKID-VALUE",
        "secret_key": "s3cr3t-VALUE",
        "region": "us-east-1"
        // endpoint absent — must NOT cause rejection
    })
    .to_string();
    let ctx = StubCtx::with_conn("http://catalog.example.com", &pw);
    read_connection(&ctx, Some("MY_CONN"), CatalogKind::IcebergRest)
        .expect("endpoint is optional under SigV4; must not be rejected");
}

/// Static bearer token is exposed on the resolved credentials.
///
/// token + warehouse-only accepted; token field exposed; use_vended_credentials
/// defaults false; no token value appears in Debug output.
#[test]
fn token_exposed_on_creds() {
    let pw = serde_json::json!({
        "warehouse": "wh",
        "token": "my-secret-token"
    })
    .to_string();
    let ctx = StubCtx::with_conn("http://catalog.example.com", &pw);
    let resolved = read_connection(&ctx, Some("MY_CONN"), CatalogKind::IcebergRest).unwrap();
    let creds = &resolved.creds;

    // Token is exposed on the struct.
    assert_eq!(
        creds.token.as_deref(),
        Some("my-secret-token"),
        "token must be exposed on creds"
    );
    // use_vended_credentials stays independent (defaults false).
    assert!(
        !creds.use_vended_credentials,
        "use_vended_credentials must default false"
    );
    // Token value must NOT leak through Debug.
    let debug = format!("{creds:?}");
    assert!(
        !debug.contains("my-secret-token"),
        "token value must not appear in Debug: {debug}"
    );
}

/// OAuth2 client credentials are exposed on the resolved credentials.
///
/// client_id + client_secret + warehouse-only accepted; fields exposed;
/// oauth2_server_uri/scope absent when omitted; no client_secret value leaked.
#[test]
fn oauth_client_creds_exposed_on_creds() {
    // With oauth2_server_uri + scope omitted.
    let pw = serde_json::json!({
        "warehouse": "wh",
        "client_id": "my-client-id",
        "client_secret": "my-client-secret"
    })
    .to_string();
    let ctx = StubCtx::with_conn("http://catalog.example.com", &pw);
    let resolved = read_connection(&ctx, Some("MY_CONN"), CatalogKind::IcebergRest).unwrap();
    let creds = &resolved.creds;

    assert_eq!(creds.client_id.as_deref(), Some("my-client-id"));
    assert_eq!(creds.client_secret.as_deref(), Some("my-client-secret"));
    // Optional fields absent when not supplied.
    assert_eq!(
        creds.oauth2_server_uri, None,
        "oauth2_server_uri must be absent"
    );
    assert_eq!(creds.scope, None, "scope must be absent");
    // token stays absent.
    assert_eq!(creds.token, None);

    // client_secret must NOT leak through Debug.
    let debug = format!("{creds:?}");
    assert!(
        !debug.contains("my-client-secret"),
        "client_secret must not appear in Debug: {debug}"
    );

    // With oauth2_server_uri + scope supplied — both exposed.
    let pw2 = serde_json::json!({
        "warehouse": "wh",
        "client_id": "my-client-id",
        "client_secret": "my-client-secret",
        "oauth2_server_uri": "https://auth.example.com/token",
        "scope": "catalog:read"
    })
    .to_string();
    let ctx2 = StubCtx::with_conn("http://catalog.example.com", &pw2);
    let resolved2 = read_connection(&ctx2, Some("MY_CONN"), CatalogKind::IcebergRest).unwrap();
    let creds2 = &resolved2.creds;

    assert_eq!(
        creds2.oauth2_server_uri.as_deref(),
        Some("https://auth.example.com/token")
    );
    assert_eq!(creds2.scope.as_deref(), Some("catalog:read"));
}

/// Incomplete OAuth2 client credentials rejected naming only the missing field.
///
/// Exactly one of client_id/client_secret present → rejected; error names only the
/// missing field; no value leaked.
#[test]
fn incomplete_oauth_rejected_no_leak() {
    // client_id present, client_secret missing.
    let pw = serde_json::json!({
        "warehouse": "wh",
        "client_id": "my-client-id"
    })
    .to_string();
    let ctx = StubCtx::with_conn("http://catalog.example.com", &pw);
    let err = read_connection(&ctx, Some("MY_CONN"), CatalogKind::IcebergRest).unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("client_secret"),
        "must name missing field client_secret: {msg}"
    );
    // Must not echo the client_id value.
    assert!(!msg.contains("my-client-id"), "must not leak value: {msg}");

    // client_secret present, client_id missing.
    let pw2 = serde_json::json!({
        "warehouse": "wh",
        "client_secret": "my-client-secret"
    })
    .to_string();
    let ctx2 = StubCtx::with_conn("http://catalog.example.com", &pw2);
    let err2 = read_connection(&ctx2, Some("MY_CONN"), CatalogKind::IcebergRest).unwrap_err();
    let msg2 = err2.to_string();
    assert!(
        msg2.contains("client_id"),
        "must name missing field client_id: {msg2}"
    );
    // Must not echo the client_secret value.
    assert!(
        !msg2.contains("my-client-secret"),
        "must not leak value: {msg2}"
    );
}

/// A CONNECTION supplying a `token` together with a complete `client_id`/
/// `client_secret` pair is rejected under both catalog kinds, naming all
/// three fields and leaking none of their values.
///
/// A third case supplies `token` and `client_id` only: rule 7 (OAuth2
/// completeness) still fires naming the missing `client_secret`, which is
/// the disjointness assertion — it fails if rule 6 were widened to fire on
/// a token beside any single OAuth2 field rather than the complete pair.
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
        let ctx = StubCtx::with_conn("http://catalog.example.com", &pw);
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

    // token + client_id only (client_secret missing): the ambiguous-pair rule
    // must not fire here, so rule 7 fires instead, naming only the missing field.
    let pw_partial = serde_json::json!({
        "warehouse": "wh",
        "token": "sentinel-token-value",
        "client_id": "sentinel-client-id-value"
    })
    .to_string();
    let ctx = StubCtx::with_conn("http://catalog.example.com", &pw_partial);
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

/// Catalog token/OAuth auth and SigV4 are mutually exclusive.
///
/// use_sigv4=true + token → rejected; use_sigv4=true + OAuth → rejected.
/// No auth value appears in the error message.
#[test]
fn sigv4_and_catalog_auth_mutually_exclusive() {
    // SigV4 + token combination.
    let pw_sigv4_token = serde_json::json!({
        "warehouse": "wh",
        "access_key": "AKID",
        "secret_key": "s3cr3t-VALUE",
        "region": "us-east-1",
        "use_sigv4": true,
        "token": "my-secret-token"
    })
    .to_string();
    let ctx = StubCtx::with_conn("http://catalog.example.com", &pw_sigv4_token);
    let err = read_connection(&ctx, Some("MY_CONN"), CatalogKind::IcebergRest).unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.to_lowercase().contains("sigv4"),
        "error must reference SigV4: {msg}"
    );
    // No secret or token value must appear.
    assert!(
        !msg.contains("my-secret-token"),
        "must not leak token value: {msg}"
    );
    assert!(
        !msg.contains("s3cr3t-VALUE"),
        "must not leak secret_key value: {msg}"
    );

    // SigV4 + OAuth combination.
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
    let ctx2 = StubCtx::with_conn("http://catalog.example.com", &pw_sigv4_oauth);
    let err2 = read_connection(&ctx2, Some("MY_CONN"), CatalogKind::IcebergRest).unwrap_err();
    let msg2 = err2.to_string();
    assert!(
        msg2.to_lowercase().contains("sigv4"),
        "error must reference SigV4: {msg2}"
    );
    // No secret value must appear.
    assert!(
        !msg2.contains("my-client-secret"),
        "must not leak client_secret: {msg2}"
    );
    assert!(
        !msg2.contains("s3cr3t-VALUE"),
        "must not leak secret_key: {msg2}"
    );
}

// ---------------------------------------------------------------------------
// Scenario: unity_kind_validation_skips_warehouse_and_rejects_sigv4
// ---------------------------------------------------------------------------

/// Under the Unity Catalog kind `warehouse` is not required and enabling AWS
/// SigV4 signing is rejected as not a Unity Catalog authentication mode.
#[test]
fn unity_kind_validation_skips_warehouse_and_rejects_sigv4() {
    // No warehouse, yet a valid Unity CONNECTION: accepted under the Unity kind.
    let no_warehouse = serde_json::json!({ "token": "tok" }).to_string();
    let ctx = StubCtx::with_conn("http://catalog.example.com", &no_warehouse);
    let resolved = read_connection(&ctx, Some("MY_CONN"), CatalogKind::UnityCatalogNative)
        .expect("a Unity Catalog CONNECTION without warehouse must be accepted");
    assert_eq!(
        resolved.creds.warehouse, "",
        "warehouse stays empty and is not required under the Unity kind"
    );

    // SigV4 is rejected even when its own required fields are absent: the error
    // must name the Unity-mode conflict, not the generic missing-field message.
    let sigv4 = serde_json::json!({
        "use_sigv4": true,
        "secret_key": "SUPERSECRET"
    })
    .to_string();
    let ctx = StubCtx::with_conn("http://catalog.example.com", &sigv4);
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

// ---------------------------------------------------------------------------
// Scenario: iceberg_kind_validation_still_requires_warehouse
// ---------------------------------------------------------------------------

/// Under the default Iceberg REST kind the missing-`warehouse` error is
/// byte-identical to the pre-feature message.
#[test]
fn iceberg_kind_validation_still_requires_warehouse() {
    let no_warehouse = serde_json::json!({
        "endpoint": "http://s3.example.com",
        "region": "us-east-1"
    })
    .to_string();
    let ctx = StubCtx::with_conn("http://catalog.example.com", &no_warehouse);
    let err = read_connection(&ctx, Some("MY_CONN"), CatalogKind::IcebergRest)
        .expect_err("Iceberg REST still requires warehouse");
    assert_eq!(
        err.to_string(),
        "CONNECTION 'MY_CONN' password is missing required field: warehouse"
    );
}

// ---------------------------------------------------------------------------
// Scenario: validation_is_parameterized_by_catalog_kind
// ---------------------------------------------------------------------------

/// The identical CONNECTION validates differently by kind: the warehouse
/// requirement and the SigV4 rule both flip on the `CatalogKind` argument.
#[test]
fn validation_is_parameterized_by_catalog_kind() {
    // No warehouse: rejected under Iceberg REST, accepted under Unity Catalog.
    let no_warehouse = serde_json::json!({ "token": "tok" }).to_string();
    let ctx = StubCtx::with_conn("http://catalog.example.com", &no_warehouse);
    assert!(
        read_connection(&ctx, Some("MY_CONN"), CatalogKind::IcebergRest).is_err(),
        "missing warehouse is rejected under Iceberg REST"
    );
    assert!(
        read_connection(&ctx, Some("MY_CONN"), CatalogKind::UnityCatalogNative).is_ok(),
        "missing warehouse is accepted under Unity Catalog"
    );

    // A well-formed SigV4 set: accepted under Iceberg REST, rejected under Unity.
    let sigv4 = serde_json::json!({
        "warehouse": "wh",
        "use_sigv4": true,
        "access_key": "AKID",
        "secret_key": "SECRET",
        "region": "us-east-1"
    })
    .to_string();
    let ctx = StubCtx::with_conn("http://catalog.example.com", &sigv4);
    assert!(
        read_connection(&ctx, Some("MY_CONN"), CatalogKind::IcebergRest).is_ok(),
        "a complete SigV4 set is accepted under Iceberg REST"
    );
    assert!(
        read_connection(&ctx, Some("MY_CONN"), CatalogKind::UnityCatalogNative).is_err(),
        "SigV4 is rejected under Unity Catalog"
    );
}

// ---------------------------------------------------------------------------
// Scenario: unity_connection_reuses_existing_auth_fields
// ---------------------------------------------------------------------------

/// A Unity CONNECTION carries auth through the SAME fields Iceberg REST uses —
/// no new credential field — and a no-auth Unity CONNECTION is accepted.
#[test]
fn unity_connection_reuses_existing_auth_fields() {
    // OAuth client credentials via the existing fields, no warehouse.
    let oauth = serde_json::json!({
        "client_id": "my-client-id",
        "client_secret": "my-client-secret",
        "oauth2_server_uri": "https://auth.example.com/token",
        "scope": "catalog:read"
    })
    .to_string();
    let ctx = StubCtx::with_conn("http://catalog.example.com", &oauth);
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

    // A static bearer token via the existing `token` field, no warehouse.
    let bearer = serde_json::json!({ "token": "my-secret-token" }).to_string();
    let ctx = StubCtx::with_conn("http://catalog.example.com", &bearer);
    let resolved = read_connection(&ctx, Some("MY_CONN"), CatalogKind::UnityCatalogNative)
        .expect("a Unity CONNECTION with a bearer token must be accepted");
    assert_eq!(resolved.creds.token.as_deref(), Some("my-secret-token"));

    // OSS Unity Catalog runs with authentication disabled: none supplied.
    let no_auth = serde_json::json!({}).to_string();
    let ctx = StubCtx::with_conn("http://catalog.example.com", &no_auth);
    let resolved = read_connection(&ctx, Some("MY_CONN"), CatalogKind::UnityCatalogNative)
        .expect("a Unity CONNECTION with no auth fields must be accepted");
    assert_eq!(resolved.creds.token, None);
    assert_eq!(resolved.creds.client_id, None);
    assert_eq!(resolved.creds.client_secret, None);
}
