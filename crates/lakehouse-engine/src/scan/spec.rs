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

/// The kind of aggregate function to compute node-locally as a partial result.
///
/// COUNT(*) maps to `Count` (no column), COUNT(col) maps to `CountCol`.
/// AVG is decomposed into a (partial_sum, partial_count) pair in the scan UDF;
/// the adapter wrapper SQL performs the final division.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum AggKind {
    Count,
    CountCol,
    Sum,
    Min,
    Max,
    Avg,
}

/// One aggregate function in a pushed-down aggregate plan.
///
/// `column` is `None` for `COUNT(*)` and `Some(col_name)` for all other
/// variants.  The column name matches the projected column name (uppercase).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AggregatePlan {
    pub kind: AggKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub column: Option<String>,
}

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

    /// Ordered list of aggregate functions to compute as node-local partial
    /// results. `None` (the default) means row scanning; absent from JSON when
    /// serialized so pre-existing scan specs are backward-compatible.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub aggregates: Option<Vec<AggregatePlan>>,

    /// Rendered DataFusion SQL fragments for each GROUP BY key, in order.
    /// `None` means no GROUP BY pushdown (single-group or row scan).
    /// Present only for grouped aggregate scans.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub group_keys: Option<Vec<String>>,

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
            aggregates: None,
            group_keys: None,
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
        spec.aggregates = None;
        spec.group_keys = None;
        let json = spec.to_json();
        assert!(!json.contains("filter"));
        assert!(!json.contains("limit"));
        assert!(!json.contains("session_token"));
        assert!(
            !json.contains("aggregates"),
            "aggregates field must be absent when None: {json}"
        );
        assert!(
            !json.contains("group_keys"),
            "group_keys field must be absent when None: {json}"
        );
    }

    /// Task 4.1: Aggregate plan round-trips through JSON and does not appear in row-scan specs.
    #[test]
    fn aggregate_plan_round_trips_and_absent_from_row_scan() {
        // Row scan: aggregates must be absent.
        let row_spec = sample_spec();
        let row_json = row_spec.to_json();
        assert!(
            !row_json.contains("aggregates"),
            "row-scan spec must not carry aggregates field: {row_json}"
        );

        // Aggregate scan: round-trip with all supported kinds.
        let mut agg_spec = sample_spec();
        agg_spec.aggregates = Some(vec![
            AggregatePlan {
                kind: AggKind::Count,
                column: None,
            },
            AggregatePlan {
                kind: AggKind::CountCol,
                column: Some("ID".into()),
            },
            AggregatePlan {
                kind: AggKind::Sum,
                column: Some("AMOUNT".into()),
            },
            AggregatePlan {
                kind: AggKind::Min,
                column: Some("TS".into()),
            },
            AggregatePlan {
                kind: AggKind::Max,
                column: Some("TS".into()),
            },
            AggregatePlan {
                kind: AggKind::Avg,
                column: Some("AMOUNT".into()),
            },
        ]);
        let agg_json = agg_spec.to_json();
        assert!(
            agg_json.contains("aggregates"),
            "aggregate spec must carry the aggregates field: {agg_json}"
        );

        let back = ScanSpec::from_json(&agg_json).unwrap();
        let plans = back.aggregates.expect("aggregates must survive round-trip");
        assert_eq!(plans.len(), 6);
        assert_eq!(plans[0].kind, AggKind::Count);
        assert_eq!(plans[0].column, None);
        assert_eq!(plans[1].kind, AggKind::CountCol);
        assert_eq!(plans[1].column.as_deref(), Some("ID"));
        assert_eq!(plans[2].kind, AggKind::Sum);
        assert_eq!(plans[3].kind, AggKind::Min);
        assert_eq!(plans[4].kind, AggKind::Max);
        assert_eq!(plans[5].kind, AggKind::Avg);
        assert_eq!(plans[5].column.as_deref(), Some("AMOUNT"));
    }

    /// Task 2.1: group_keys round-trips through JSON and is absent from row-scan specs.
    #[test]
    fn group_keys_round_trips_and_absent_from_row_scan() {
        // Row scan: group_keys must be absent from serialized JSON.
        let row_spec = sample_spec();
        let row_json = row_spec.to_json();
        assert!(
            !row_json.contains("group_keys"),
            "row-scan spec must not carry group_keys field: {row_json}"
        );

        // Grouped scan: round-trip with Some group keys.
        let mut grouped_spec = sample_spec();
        grouped_spec.group_keys = Some(vec![
            "\"REGION\"".to_string(),
            "YEAR(\"EVENT_DATE\")".to_string(),
        ]);
        let grouped_json = grouped_spec.to_json();
        assert!(
            grouped_json.contains("group_keys"),
            "grouped spec must carry group_keys field: {grouped_json}"
        );

        let back = ScanSpec::from_json(&grouped_json).unwrap();
        let keys = back.group_keys.expect("group_keys must survive round-trip");
        assert_eq!(keys.len(), 2);
        assert_eq!(keys[0], "\"REGION\"");
        assert_eq!(keys[1], "YEAR(\"EVENT_DATE\")");
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
