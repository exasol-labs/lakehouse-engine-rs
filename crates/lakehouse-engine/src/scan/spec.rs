/// Scan specification that crosses the UDF argument boundary.
///
/// The adapter serializes this to a single JSON string passed as one VARCHAR
/// argument to the scan SET UDF. The scan UDF deserializes it from the input
/// `Value::String` via `ctx.get(0)`.
///
/// # ponytail: single-JSON-arg design — one VARCHAR column carries the whole spec.
/// Split into typed columns only if a size limit bites.
///
/// Credentials (`access_key`, `secret_key`) MUST NEVER appear in any error message.
use serde::{Deserialize, Serialize};

/// Storage connection properties (S3-compatible / MinIO).
/// Fields are plain Strings so serde handles them uniformly.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageProps {
    pub endpoint: String,
    pub region: String,
    pub access_key: String,
    pub secret_key: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_token: Option<String>,
    /// Enable HTTP (MinIO local dev typically uses HTTP, not HTTPS).
    #[serde(default)]
    pub allow_http: bool,
    /// Use path-style access (required for MinIO).
    #[serde(default = "default_true")]
    pub path_style: bool,
}

fn default_true() -> bool {
    true
}

impl StorageProps {
    /// The non-empty secret values (access key, secret key, session token).
    ///
    /// Used for value-based error redaction: any error string containing one of
    /// these literal values has it stripped before the error is surfaced.
    pub fn secret_values(&self) -> Vec<&str> {
        let mut secrets = Vec::new();
        for candidate in [self.access_key.as_str(), self.secret_key.as_str()] {
            if !candidate.is_empty() {
                secrets.push(candidate);
            }
        }
        if let Some(token) = self.session_token.as_deref()
            && !token.is_empty()
        {
            secrets.push(token);
        }
        secrets
    }
}

/// Iceberg REST catalog connection properties.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CatalogProps {
    pub uri: String,
    pub warehouse: String,
    /// Fully-qualified table identifier: "<namespace>.<table>".
    pub table: String,
}

/// The scan specification passed from the adapter to the scan SET UDF.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScanSpec {
    /// Explicit list of Parquet file URIs (S3 or s3a) assigned to this scan.
    /// The scan UDF registers ONLY these files — no catalog discovery.
    pub files: Vec<String>,

    /// Projected columns in order. Empty means "all columns" (no projection push).
    pub projection: Vec<String>,

    /// DataFusion SQL WHERE predicate fragment, already translated.
    /// None means no filter pushdown (Exasol keeps the predicate for correctness).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub filter: Option<String>,

    /// Row limit. None means no LIMIT pushdown.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<u64>,

    pub storage: StorageProps,
    pub catalog: CatalogProps,
}

impl ScanSpec {
    /// Serialize to a JSON string suitable for `Value::String`.
    pub fn to_json(&self) -> String {
        serde_json::to_string(self).expect("ScanSpec serialization is infallible")
    }

    /// Deserialize from a JSON string received from `ctx.get(0)`.
    /// Returns an error that does NOT include any credential values.
    pub fn from_json(s: &str) -> Result<Self, String> {
        serde_json::from_str(s).map_err(|e| {
            // Do not echo `s` — it contains credentials.
            format!("scan spec deserialization failed: {e}")
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_spec() -> ScanSpec {
        ScanSpec {
            files: vec![
                "s3://warehouse/db/table/data/part-00000.parquet".into(),
                "s3://warehouse/db/table/data/part-00001.parquet".into(),
            ],
            projection: vec!["id".into(), "name".into()],
            filter: Some("(\"ID\" > 10)".into()),
            limit: Some(100),
            storage: StorageProps {
                endpoint: "http://minio:9000".into(),
                region: "us-east-1".into(),
                access_key: "minioadmin".into(),
                secret_key: "minioadmin".into(),
                session_token: None,
                allow_http: true,
                path_style: true,
            },
            catalog: CatalogProps {
                uri: "http://iceberg-rest:8181".into(),
                warehouse: "warehouse".into(),
                table: "db.table".into(),
            },
        }
    }

    /// Scenario (D.2): Scan-spec round-trips through Value boundary.
    /// serialize → Value::String → deserialize equals original;
    /// credentials survive round-trip but never appear in error text on malformed input.
    #[test]
    fn scan_spec_round_trips_through_value_boundary() {
        let spec = sample_spec();

        // Serialize to JSON (→ the Value::String payload that crosses the UDF boundary).
        let json = spec.to_json();
        // The JSON must be valid UTF-8 string (Value::String is a Rust String).
        let _value_string: String = json.clone(); // satisfies Value::String ownership model.

        // Deserialize back: must equal original.
        let back = ScanSpec::from_json(&json).unwrap();
        assert_eq!(back.files.len(), 2);
        assert_eq!(back.projection, vec!["id", "name"]);
        assert_eq!(back.filter.as_deref(), Some("(\"ID\" > 10)"));
        assert_eq!(back.limit, Some(100));

        // Credentials survive the round-trip (they must reach the scan UDF).
        assert_eq!(back.storage.endpoint, "http://minio:9000");
        assert_eq!(back.storage.access_key, "minioadmin");
        assert_eq!(back.storage.secret_key, "minioadmin");
        assert!(back.storage.path_style);
        assert!(back.storage.allow_http);
        assert_eq!(back.catalog.table, "db.table");
        assert_eq!(back.catalog.uri, "http://iceberg-rest:8181");
    }

    #[test]
    fn optional_fields_omitted_when_none() {
        let mut spec = sample_spec();
        spec.filter = None;
        spec.limit = None;
        spec.storage.session_token = None;
        let json = spec.to_json();
        assert!(!json.contains("filter"));
        assert!(!json.contains("limit"));
        assert!(!json.contains("session_token"));
    }

    #[test]
    fn bad_json_error_does_not_leak_credentials() {
        let garbled =
            r#"{"storage": {"access_key": "SECRET", "secret_key": "TOPSECRET"}, incomplete"#;
        let err = ScanSpec::from_json(garbled).unwrap_err();
        // The error must not echo the raw input (which contains credentials).
        assert!(!err.contains("SECRET"));
        assert!(!err.contains("TOPSECRET"));
        // But it should say something useful.
        assert!(err.contains("scan spec deserialization failed"));
    }
}
