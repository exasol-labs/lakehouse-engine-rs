/// Resolve an Exasol CONNECTION object into catalog and storage configuration.
///
/// The CONNECTION's `address` is the Iceberg REST catalog URI; the `password`
/// is a JSON object carrying credential and behavioural fields. Credential
/// values NEVER appear in any error message produced by this module.
use crate::scan::spec::{CatalogProps, StorageProps};
use exasol_udf_sdk::context::UdfContext;
use exasol_udf_sdk::error::UdfError;

/// The only unconditionally-required field in the CONNECTION password JSON.
///
/// The four S3 fields (`endpoint`, `region`, `access_key`, `secret_key`) are
/// optional at the base level; they are orthogonal to catalog authentication and
/// credential vending. `region`/`access_key`/`secret_key` become required only
/// when `use_sigv4` is enabled (see `read_connection`).
pub const REQUIRED_KEY: &str = "warehouse";

/// Parsed credential fields from a CONNECTION password JSON object.
///
/// Carries all optional flags so later work (SigV4 signing, credential vending,
/// catalog token/OAuth2 auth) can read them without touching the module again.
///
/// Secret-bearing fields (`secret_key`, `client_secret`, `token`) are excluded
/// from the derived `Debug` output via a manual impl to prevent accidental leaks.
#[derive(Clone)]
pub struct ConnectionCreds {
    pub warehouse: String,
    pub endpoint: String,
    pub region: String,
    pub access_key: String,
    pub secret_key: String,
    /// Optional STS session token. Absent when not supplied.
    pub session_token: Option<String>,
    /// Use path-style S3 access. Defaults to `true` to preserve MinIO behaviour.
    pub path_style: bool,
    /// Sign catalog REST requests with AWS SigV4. Defaults to `false` so
    /// existing MinIO/REST stacks behave exactly as before.
    pub use_sigv4: bool,
    /// Request short-lived vended S3 credentials via `load_table`. Defaults to
    /// `false` so existing stacks behave exactly as before.
    pub use_vended_credentials: bool,
    /// Static bearer token for catalog authentication. Absent when not supplied.
    pub token: Option<String>,
    /// OAuth2 client ID for catalog client-credentials flow. Absent when not supplied.
    pub client_id: Option<String>,
    /// OAuth2 client secret for catalog client-credentials flow. Absent when not supplied.
    pub client_secret: Option<String>,
    /// Optional OAuth2 token endpoint URI. Absent when not supplied.
    pub oauth2_server_uri: Option<String>,
    /// Optional OAuth2 scope. Absent when not supplied.
    pub scope: Option<String>,
}

impl std::fmt::Debug for ConnectionCreds {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConnectionCreds")
            .field("warehouse", &self.warehouse)
            .field("endpoint", &self.endpoint)
            .field("region", &self.region)
            .field("access_key", &self.access_key)
            .field("secret_key", &"[redacted]")
            .field(
                "session_token",
                &self.session_token.as_ref().map(|_| "[redacted]"),
            )
            .field("path_style", &self.path_style)
            .field("use_sigv4", &self.use_sigv4)
            .field("use_vended_credentials", &self.use_vended_credentials)
            .field("token", &self.token.as_ref().map(|_| "[redacted]"))
            .field("client_id", &self.client_id)
            .field(
                "client_secret",
                &self.client_secret.as_ref().map(|_| "[redacted]"),
            )
            .field("oauth2_server_uri", &self.oauth2_server_uri)
            .field("scope", &self.scope)
            .finish()
    }
}

impl ConnectionCreds {
    /// Returns `true` when catalog authentication credentials are present:
    /// either a static bearer `token` OR any OAuth2 client-credential field
    /// (`client_id` and/or `client_secret`). Partial OAuth still signals
    /// catalog-auth intent, so the SigV4 guard rejects it.
    ///
    /// Used exclusively for the SigV4 mutual-exclusivity check.
    pub fn has_catalog_auth(&self) -> bool {
        self.token.is_some() || self.client_id.is_some() || self.client_secret.is_some()
    }
}

/// Resolved CONNECTION: catalog URI plus parsed credentials.
#[derive(Debug)]
pub struct Resolved {
    pub uri: String,
    pub creds: ConnectionCreds,
}

/// Resolve a named Exasol CONNECTION into a catalog URI and credentials.
///
/// Credential-safe: the password value is never embedded in any returned error.
pub fn read_connection(ctx: &dyn UdfContext, name: Option<&str>) -> Result<Resolved, UdfError> {
    let name = match name {
        Some(n) if !n.is_empty() => n,
        _ => {
            return Err(UdfError::User("CATALOG_CONNECTION is required".into()));
        }
    };

    let conn = ctx
        .connection(name)
        .map_err(|_| UdfError::User(format!("CONNECTION '{name}' could not be resolved")))?;

    let uri = conn.address;
    if uri.is_empty() {
        return Err(UdfError::User(format!(
            "CONNECTION '{name}' has no address; expected the catalog URI"
        )));
    }

    // Never embed the password in the error message.
    let json: serde_json::Value = serde_json::from_str(&conn.password).map_err(|_| {
        UdfError::User(format!(
            "CONNECTION '{name}' password is not a valid JSON object"
        ))
    })?;

    if !json.is_object() {
        return Err(UdfError::User(format!(
            "CONNECTION '{name}' password is not a valid JSON object"
        )));
    }

    let creds = parse_creds(&json);
    validate_creds(name, &creds)?;
    Ok(Resolved { uri, creds })
}

/// Validate parsed credentials against the mode-aware credential contract.
///
/// Credential-safe: only field names — never values — appear in any error.
///
/// Rules, in precedence order:
/// 1. `warehouse` is the only unconditionally-required field.
/// 2. SigV4 and catalog token/OAuth authentication are mutually exclusive.
/// 3. When `use_sigv4` is enabled, `access_key`, `secret_key`, and `region` are
///    required (they sign the catalog `load_table` request ahead of any vended
///    credentials); this holds regardless of `use_vended_credentials`. `endpoint`
///    stays optional.
/// 4. OAuth2 client credentials require both `client_id` and `client_secret`.
fn validate_creds(name: &str, creds: &ConnectionCreds) -> Result<(), UdfError> {
    if creds.warehouse.is_empty() {
        return Err(UdfError::User(format!(
            "CONNECTION '{name}' password is missing required field: {REQUIRED_KEY}"
        )));
    }

    if creds.use_sigv4 && creds.has_catalog_auth() {
        return Err(UdfError::User(format!(
            "CONNECTION '{name}' enables SigV4 signing together with catalog \
             token/OAuth authentication; these cannot both be enabled"
        )));
    }

    if creds.use_sigv4 {
        let mut missing: Vec<&str> = Vec::new();
        if creds.access_key.is_empty() {
            missing.push("access_key");
        }
        if creds.secret_key.is_empty() {
            missing.push("secret_key");
        }
        if creds.region.is_empty() {
            missing.push("region");
        }
        if !missing.is_empty() {
            return Err(UdfError::User(format!(
                "CONNECTION '{name}' enables SigV4 signing but is missing field(s) \
                 required when SigV4 signing is enabled: {}",
                missing.join(", ")
            )));
        }
    }

    match (creds.client_id.is_some(), creds.client_secret.is_some()) {
        (true, false) => {
            return Err(UdfError::User(format!(
                "CONNECTION '{name}' OAuth2 client credentials require both \
                 client_id and client_secret; missing field: client_secret"
            )));
        }
        (false, true) => {
            return Err(UdfError::User(format!(
                "CONNECTION '{name}' OAuth2 client credentials require both \
                 client_id and client_secret; missing field: client_id"
            )));
        }
        _ => {}
    }

    Ok(())
}

fn str_field<'a>(obj: &'a serde_json::Value, key: &str) -> Option<&'a str> {
    obj.get(key)
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
}

fn parse_creds(json: &serde_json::Value) -> ConnectionCreds {
    ConnectionCreds {
        warehouse: str_field(json, "warehouse").unwrap_or("").to_string(),
        endpoint: str_field(json, "endpoint").unwrap_or("").to_string(),
        region: str_field(json, "region").unwrap_or("").to_string(),
        access_key: str_field(json, "access_key").unwrap_or("").to_string(),
        secret_key: str_field(json, "secret_key").unwrap_or("").to_string(),
        session_token: str_field(json, "session_token").map(|s| s.to_string()),
        path_style: json
            .get("path_style")
            .and_then(|v| v.as_bool())
            .unwrap_or(true),
        use_sigv4: json
            .get("use_sigv4")
            .and_then(|v| v.as_bool())
            .unwrap_or(false),
        use_vended_credentials: json
            .get("use_vended_credentials")
            .and_then(|v| v.as_bool())
            .unwrap_or(false),
        token: str_field(json, "token").map(|s| s.to_string()),
        client_id: str_field(json, "client_id").map(|s| s.to_string()),
        client_secret: str_field(json, "client_secret").map(|s| s.to_string()),
        oauth2_server_uri: str_field(json, "oauth2_server_uri").map(|s| s.to_string()),
        scope: str_field(json, "scope").map(|s| s.to_string()),
    }
}

/// Build `StorageProps` from resolved credentials.
pub fn storage_block(creds: &ConnectionCreds) -> StorageProps {
    StorageProps {
        endpoint: creds.endpoint.clone(),
        region: creds.region.clone(),
        access_key: creds.access_key.clone(),
        secret_key: creds.secret_key.clone(),
        session_token: creds.session_token.clone(),
        allow_http: false,
        path_style: creds.path_style,
    }
}

/// Build `CatalogProps` from resolved credentials, catalog URI, and table name.
pub fn catalog_block(creds: &ConnectionCreds, uri: &str, table: &str) -> CatalogProps {
    CatalogProps {
        uri: uri.to_string(),
        warehouse: creds.warehouse.clone(),
        table: table.to_string(),
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
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
        let resolved = read_connection(&ctx, Some("MY_CONN")).unwrap();

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

        let err_none = read_connection(&ctx, None).unwrap_err();
        assert!(
            err_none
                .to_string()
                .contains("CATALOG_CONNECTION is required")
        );

        let err_empty = read_connection(&ctx, Some("")).unwrap_err();
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
        let err = read_connection(&ctx, Some("MY_CONN")).unwrap_err();

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
        let err = read_connection(&ctx, Some("MY_CONN")).unwrap_err();

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
        let err = read_connection(&ctx, Some("MY_CONN")).unwrap_err();

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
        let resolved = read_connection(&ctx, Some("MY_CONN")).unwrap();
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
        let resolved = read_connection(&ctx, Some("MY_CONN")).unwrap();
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
        let resolved = read_connection(&ctx, Some("MY_CONN")).unwrap();
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
        let resolved = read_connection(&ctx, Some("MY_CONN")).unwrap();
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
        let resolved = read_connection(&ctx, Some("MY_CONN")).unwrap();
        let storage = storage_block(&resolved.creds);

        assert_eq!(storage.endpoint, "http://s3.example.com");
        assert_eq!(storage.region, "us-east-1");
        assert_eq!(storage.access_key, "AKID");
        assert_eq!(storage.secret_key, "SECRET");
        assert_eq!(storage.session_token, None);
        assert!(storage.path_style);
    }

    #[test]
    fn catalog_block_maps_creds_to_catalog_props() {
        let ctx = StubCtx::with_conn("http://catalog.example.com", &minimal_password());
        let resolved = read_connection(&ctx, Some("MY_CONN")).unwrap();
        let catalog = catalog_block(&resolved.creds, &resolved.uri, "db.my_table");

        assert_eq!(catalog.uri, "http://catalog.example.com");
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
        let resolved = read_connection(&ctx, Some("MY_CONN")).unwrap();
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
        read_connection(&ctx2, Some("MY_CONN")).unwrap();
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
        let err = read_connection(&ctx, Some("MY_CONN")).unwrap_err();
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
        let err = read_connection(&ctx, Some("MY_CONN")).unwrap_err();
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
        let err = read_connection(&ctx, Some("MY_CONN")).unwrap_err();
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
        let err = read_connection(&ctx, Some("MY_CONN")).unwrap_err();
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
        read_connection(&ctx, Some("MY_CONN"))
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
        let resolved = read_connection(&ctx, Some("MY_CONN")).unwrap();
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
        let resolved = read_connection(&ctx, Some("MY_CONN")).unwrap();
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
        let resolved2 = read_connection(&ctx2, Some("MY_CONN")).unwrap();
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
        let err = read_connection(&ctx, Some("MY_CONN")).unwrap_err();
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
        let err2 = read_connection(&ctx2, Some("MY_CONN")).unwrap_err();
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
        let err = read_connection(&ctx, Some("MY_CONN")).unwrap_err();
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
        let err2 = read_connection(&ctx2, Some("MY_CONN")).unwrap_err();
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
}
