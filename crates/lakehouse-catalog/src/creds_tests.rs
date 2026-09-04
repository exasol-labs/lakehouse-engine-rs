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

/// `debug_redacts_every_secret_bearing_field` above covers `access_key`
/// alongside every other secret field; this test isolates it, so a narrowing
/// of the redacted set that happened to leave `access_key` visible again
/// fails on a focused case rather than only a combined one.
#[test]
fn debug_redacts_the_connection_access_key() {
    let creds = ConnectionCreds {
        access_key: "AKIA_ISOLATED_EXAMPLE".into(),
        ..crate::test_support::creds_no_auth()
    };

    let debug = format!("{creds:?}");

    assert!(
        !debug.contains("AKIA_ISOLATED_EXAMPLE"),
        "access_key must not appear in Debug: {debug}"
    );
    // Positive control: the type still prints something for this field, it is
    // just not the literal value — an absence assertion alone would pass just
    // as well if the field were dropped from the output entirely.
    assert!(
        debug.contains("access_key"),
        "the field name itself stays visible, only the value is redacted: {debug}"
    );
}

/// The manual `Debug` impl on `StorageProps`, `StorageBackend`, and
/// `StorageCreds` is what stands between a logged/`{:?}`-formatted error and a
/// live storage credential — mirrors `debug_redacts_every_secret_bearing_field`
/// above, applied to the three storage-side types instead of `ConnectionCreds`.
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

/// The nine storage fields populated, with no Azure credential, so a test that
/// varies only the Azure shape says what it varies.
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

/// The S3 backend [`s3_storage_creds`] selects, for the given `allow_http`.
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

/// The nine storage key spellings alongside every catalog-auth spelling, so the
/// reader is exercised against the real CONNECTION password shape rather than a
/// storage-only subset of it.
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

/// The nine fields read as absent — empty for the four `String`s, `None` for
/// the four `Option`s — with `path_style` at its `true` default.
fn assert_every_storage_field_is_absent(creds: &StorageCreds, shape: &str) {
    assert_eq!(creds.endpoint, "", "{shape}");
    assert_eq!(creds.region, "", "{shape}");
    assert_eq!(creds.access_key, "", "{shape}");
    assert_eq!(creds.secret_key, "", "{shape}");
    assert_eq!(creds.session_token, None, "{shape}");
    assert_eq!(creds.account_name, None, "{shape}");
    assert_eq!(creds.account_key, None, "{shape}");
    assert_eq!(creds.sas_token, None, "{shape}");
    assert!(creds.path_style, "{shape}: path_style defaults to true");
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

/// Reproduces the exact reading the adapter's `parse_creds` applies today: a
/// field counts only as a non-empty JSON STRING, and `path_style` only as a
/// JSON bool. A `Deserialize` derive would reject the wrong-typed row instead
/// of defaulting it, and would keep `Some("")` as a present credential.
#[test]
fn from_json_treats_an_empty_absent_or_wrong_typed_field_as_absent() {
    let every_field_empty = serde_json::json!({
        "endpoint": "",
        "region": "",
        "access_key": "",
        "secret_key": "",
        "session_token": "",
        "account_name": "",
        "account_key": "",
        "sas_token": "",
    });
    assert_every_storage_field_is_absent(
        &StorageCreds::from_json(&every_field_empty),
        "every field empty",
    );

    assert_every_storage_field_is_absent(
        &StorageCreds::from_json(&serde_json::json!({})),
        "every field absent",
    );

    let every_field_the_wrong_type = serde_json::json!({
        "endpoint": 42,
        "region": true,
        "access_key": null,
        "secret_key": [],
        "session_token": {},
        "account_name": 0,
        "account_key": false,
        "sas_token": null,
        "path_style": "false",
    });
    assert_every_storage_field_is_absent(
        &StorageCreds::from_json(&every_field_the_wrong_type),
        "a non-string value is not a credential, and a string is not a path_style bool",
    );
}

/// The one selection rule: Azure needs an account name AND exactly one of
/// `account_key`/`sas_token`. Every other shape is S3 — including the two
/// ambiguous Azure shapes `validate_creds` rejects upstream, which reach here
/// only as a deterministic answer rather than a panic.
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

/// `allow_http` is an S3-only knob arriving as a parameter rather than a tenth
/// field: the Azure backend carries no HTTP-scheme field and ignores it.
#[test]
fn backend_applies_allow_http_to_the_s3_backend_and_ignores_it_for_adls() {
    assert_eq!(s3_storage_creds().backend(true), s3_backend(true));
    assert_eq!(s3_storage_creds().backend(false), s3_backend(false));

    let azure = StorageCreds {
        account_name: Some(AZURE_ACCOUNT.into()),
        sas_token: Some(AZURE_SAS_TOKEN.into()),
        ..s3_storage_creds()
    };
    assert_eq!(azure.backend(true), azure.backend(false));
}

#[test]
fn from_connection_creds_projects_the_nine_storage_fields() {
    let creds = ConnectionCreds {
        endpoint: STORAGE_ENDPOINT.into(),
        region: STORAGE_REGION.into(),
        access_key: STORAGE_AK.into(),
        secret_key: STORAGE_SK.into(),
        session_token: Some(STORAGE_SESSION_TOKEN.into()),
        path_style: true,
        account_name: Some(AZURE_ACCOUNT.into()),
        account_key: Some(AZURE_ACCOUNT_KEY.into()),
        sas_token: Some(AZURE_SAS_TOKEN.into()),
        token: Some(TOKEN.into()),
        client_id: Some(CLIENT_ID.into()),
        client_secret: Some(CLIENT_SECRET.into()),
        oauth2_server_uri: Some(OAUTH2_SERVER_URI.into()),
        scope: Some(OAUTH2_SCOPE.into()),
        ..crate::test_support::creds_no_auth()
    };

    let projected = StorageCreds::from(&creds);

    assert_eq!(projected.endpoint, STORAGE_ENDPOINT);
    assert_eq!(projected.region, STORAGE_REGION);
    assert_eq!(projected.access_key, STORAGE_AK);
    assert_eq!(projected.secret_key, STORAGE_SK);
    assert_eq!(
        projected.session_token.as_deref(),
        Some(STORAGE_SESSION_TOKEN)
    );
    assert_eq!(projected.account_name.as_deref(), Some(AZURE_ACCOUNT));
    assert_eq!(projected.account_key.as_deref(), Some(AZURE_ACCOUNT_KEY));
    assert_eq!(projected.sas_token.as_deref(), Some(AZURE_SAS_TOKEN));
    assert!(projected.path_style);
}

/// `StorageCreds::from_json` reads only the nine storage fields; none of its
/// resulting values may equal `client_secret`, even though the same password
/// carries it under a different key. This is the behavioral counterpart to
/// `catalog_public_surface.rs`'s structural probe that the type's DECLARATION
/// names no catalog-auth field at all — this test instead drives the READER
/// over a password carrying every field and inspects the values it produced.
#[test]
fn from_json_never_yields_a_value_equal_to_client_secret() {
    let creds = StorageCreds::from_json(&password_carrying_every_field());

    for value in [
        Some(creds.endpoint.as_str()),
        Some(creds.region.as_str()),
        Some(creds.access_key.as_str()),
        Some(creds.secret_key.as_str()),
        creds.session_token.as_deref(),
        creds.account_name.as_deref(),
        creds.account_key.as_deref(),
        creds.sas_token.as_deref(),
    ] {
        assert_ne!(
            value,
            Some(CLIENT_SECRET),
            "StorageCreds::from_json must never yield a value equal to client_secret"
        );
    }
}

/// The projection is a field-for-field copy. Re-normalizing an empty `Option`
/// here would make `StorageCreds::from(creds).backend(h)` select S3 where the
/// same `ConnectionCreds` describes Azure — the two readers disagreeing on a
/// shape `parse_creds` never produces but a hand-built `ConnectionCreds` does.
#[test]
fn from_connection_creds_does_not_re_normalize_an_empty_field() {
    let creds = ConnectionCreds {
        session_token: Some(String::new()),
        account_name: Some(String::new()),
        account_key: Some(String::new()),
        sas_token: None,
        ..crate::test_support::creds_no_auth()
    };

    let projected = StorageCreds::from(&creds);

    assert_eq!(projected.session_token.as_deref(), Some(""));
    assert_eq!(projected.account_name.as_deref(), Some(""));
    assert_eq!(projected.account_key.as_deref(), Some(""));
    assert_eq!(
        projected.backend(false),
        StorageBackend::Adls {
            account_name: String::new(),
            cred: AdlsCred::AccountKey(String::new()),
        }
    );
}
