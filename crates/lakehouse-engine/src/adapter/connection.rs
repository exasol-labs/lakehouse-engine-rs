/// Resolve an Exasol CONNECTION object into catalog and storage configuration.
///
/// The CONNECTION's `address` is the Iceberg REST catalog URI; the `password`
/// is a JSON object carrying credential and behavioural fields. Credential
/// values NEVER appear in any error message produced by this module.
use crate::scan::spec::{CatalogProps, StorageProps};
use exasol_udf_sdk::context::UdfContext;
use exasol_udf_sdk::error::UdfError;

/// Required fields that must be present and non-empty in the CONNECTION password JSON.
pub const REQUIRED_CRED_KEYS: &[&str] = &[
    "warehouse",
    "endpoint",
    "region",
    "access_key",
    "secret_key",
];

/// Parsed credential fields from a CONNECTION password JSON object.
///
/// Carries all optional flags so later work (SigV4 signing, credential vending)
/// can read them without touching the module again.
#[derive(Debug, Clone)]
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

    let missing: Vec<&str> = REQUIRED_CRED_KEYS
        .iter()
        .copied()
        .filter(|key| {
            json.get(key)
                .and_then(|v| v.as_str())
                .map(|s| s.is_empty())
                .unwrap_or(true)
        })
        .collect();

    if !missing.is_empty() {
        return Err(UdfError::User(format!(
            "CONNECTION '{name}' password is missing required fields: {}",
            missing.join(", ")
        )));
    }

    let creds = parse_creds(&json);
    Ok(Resolved { uri, creds })
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
    fn missing_required_fields_listed() {
        // Password that only has warehouse — missing endpoint, region, access_key, secret_key
        let partial = serde_json::json!({ "warehouse": "wh" }).to_string();
        let ctx = StubCtx::with_conn("http://catalog.example.com", &partial);
        let err = read_connection(&ctx, Some("MY_CONN")).unwrap_err();

        let msg = err.to_string();
        assert!(
            msg.contains("missing required fields"),
            "expected 'missing required fields' in: {msg}"
        );
        // Missing field NAMES must be listed
        assert!(
            msg.contains("endpoint"),
            "must name missing field 'endpoint': {msg}"
        );
        assert!(
            msg.contains("region"),
            "must name missing field 'region': {msg}"
        );
        assert!(
            msg.contains("access_key"),
            "must name missing field 'access_key': {msg}"
        );
        assert!(
            msg.contains("secret_key"),
            "must name missing field 'secret_key': {msg}"
        );
        // Must NOT list the field value "wh" — only field names
        assert!(!msg.contains("wh"), "must not leak field values: {msg}");
    }

    #[test]
    fn single_missing_field_named_in_error() {
        // All required fields present except secret_key
        let almost = serde_json::json!({
            "warehouse": "wh",
            "endpoint": "http://s3.example.com",
            "region": "us-east-1",
            "access_key": "AKID"
            // secret_key missing
        })
        .to_string();
        let ctx = StubCtx::with_conn("http://catalog.example.com", &almost);
        let err = read_connection(&ctx, Some("MY_CONN")).unwrap_err();

        let msg = err.to_string();
        assert!(
            msg.contains("secret_key"),
            "must name missing 'secret_key': {msg}"
        );
        assert!(
            !msg.contains("AKID"),
            "must not leak access_key value: {msg}"
        );
    }

    // ---------------------------------------------------------------------------
    // Scenario: optional_fields_default
    // ---------------------------------------------------------------------------

    #[test]
    fn optional_fields_default() {
        let ctx = StubCtx::with_conn("http://catalog.example.com", &minimal_password());
        let resolved = read_connection(&ctx, Some("MY_CONN")).unwrap();
        let creds = &resolved.creds;

        // session_token absent when not supplied
        assert_eq!(creds.session_token, None);
        // use_sigv4 defaults false
        assert!(!creds.use_sigv4, "use_sigv4 must default to false");
        // use_vended_credentials defaults false
        assert!(
            !creds.use_vended_credentials,
            "use_vended_credentials must default to false"
        );
        // path_style defaults true (preserves MinIO behaviour)
        assert!(creds.path_style, "path_style must default to true");
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
}
