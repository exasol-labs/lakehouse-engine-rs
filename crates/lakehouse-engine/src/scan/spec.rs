//! Scan specification types that cross the UDF argument boundary.
//!
//! The adapter splits the spec across TWO VARCHAR UDF arguments: the
//! shard-invariant [`CommonScanSpec`] serialized ONCE per fan-out (argument 0)
//! and the per-shard files JSON array (argument 1). The scan UDF reads both via
//! `ctx.get_string(0)` / `ctx.get_string(1)` and reconstitutes a [`ScanSpec`]
//! through [`ScanSpec::from_parts_json`]. Because [`CommonScanSpec`] has no
//! `files` field, "files is the only per-shard field" is a type-level guarantee.
//!
//! Credentials (`access_key`, `secret_key`) MUST NEVER appear in any error message.
use serde::{Deserialize, Serialize};

/// The kind of aggregate function to compute node-locally as a partial result.
///
/// COUNT(*) maps to `Count` (no column), COUNT(col) maps to `CountCol`.
/// AVG is decomposed into a (partial_sum, partial_count) pair in the scan UDF;
/// the adapter wrapper SQL performs the final division.
///
/// STDDEV/VARIANCE family are decomposed into a (cnt, sum, sum_sq) sufficient-
/// statistics triple; the wrapper reconstructs the population or sample statistic.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum AggKind {
    Count,
    CountCol,
    Sum,
    Min,
    Max,
    Avg,
    /// VAR_POP / VARIANCE_POP — divide final numer by N.
    VarPop,
    /// VAR_SAMP / VARIANCE / VARIANCE_SAMP — divide final numer by N-1.
    VarSamp,
    /// STDDEV_POP — sqrt(VAR_POP).
    StddevPop,
    /// STDDEV / STDDEV_SAMP — sqrt(VAR_SAMP).
    StddevSamp,
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
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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

/// One field in the logical schema carried by `ScanSpec::logical_schema`.
///
/// The `arrow_type` is a compact string tag produced by
/// `types::mapping::arrow_type_to_tag` and parsed back by
/// `types::mapping::arrow_type_from_tag`. Using a string tag rather than a
/// serialized `DataType` keeps the field credential-free and JSON-portable.
///
/// Supported tags:
/// - Primitives: `"bool"`, `"int32"`, `"int64"`, `"float32"`, `"float64"`,
///   `"utf8"`, `"date32"`
/// - Timestamps: `"timestamp_us"`, `"timestamp_ns"`,
///   `"timestamptz_us"`, `"timestamptz_ns"`
/// - Decimal: `"decimal128(p,s)"` (e.g. `"decimal128(18,4)"`)
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogicalField {
    /// Iceberg field-id for this column.
    pub field_id: i32,
    /// Current logical name (from the Iceberg schema at query time).
    pub name: String,
    /// Compact Arrow type tag (see struct doc for the tag vocabulary).
    pub arrow_type: String,
    /// Whether the column is nullable (`optional` in Iceberg terms).
    pub nullable: bool,
}

/// The shard-INVARIANT portion of a scan specification.
///
/// Holds every field the scan UDF reads that is identical across all shards of a
/// single query fan-out — i.e. everything EXCEPT the per-shard `files` list (and
/// excluding the adapter-side-only `catalog`). The adapter serializes this ONCE
/// as the first UDF argument; only the per-shard files list varies per invocation.
///
/// Because this struct structurally has no `files` field, "files is the only
/// per-shard field" is a type-level guarantee: the common blob can never carry a
/// stray `files` value.
///
/// Credentials (`storage.access_key`, `storage.secret_key`) MUST NEVER appear in
/// any error message produced by `from_json`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CommonScanSpec {
    /// Projected columns in order. Empty means "all columns" (no projection push).
    pub projection: Vec<String>,

    /// DataFusion SQL WHERE predicate fragment, already translated.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub filter: Option<String>,

    /// Row limit. None means no LIMIT pushdown.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<u64>,

    /// Ordered list of aggregate functions to compute as node-local partial results.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub aggregates: Option<Vec<AggregatePlan>>,

    /// Rendered DataFusion SQL fragments for each GROUP BY key, in order.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub group_keys: Option<Vec<String>>,

    /// Declared Exasol EMITS type string for each output column.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub emit_exa_types: Vec<String>,

    /// Full logical schema of the Iceberg table at query time.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub logical_schema: Vec<LogicalField>,

    pub storage: StorageProps,

    /// DataFusion `target_partitions` for this scan instance.
    #[serde(default = "default_one_usize")]
    pub df_target_partitions: usize,

    /// DataFusion `batch_size` (rows per Arrow RecordBatch) for this scan instance.
    #[serde(default = "default_batch_size")]
    pub df_batch_size: usize,

    /// Number of Tokio worker threads for the scan runtime.
    #[serde(default = "default_one_usize")]
    pub df_threads_per_udf: usize,

    /// Fraction of the net per-instance budget given to the DataFusion memory pool.
    #[serde(default = "default_memory_pool_fraction")]
    pub memory_pool_fraction: f64,

    /// Fixed container/binary RSS overhead (MB) subtracted from the per-instance limit.
    #[serde(default = "default_instance_overhead_mb")]
    pub instance_overhead_mb: u64,

    /// Connection-concurrency budget for the scan's S3-compatible object store
    /// (number of concurrent connections held warm per host).
    #[serde(default = "default_s3_max_connections")]
    pub s3_max_connections: usize,
}

impl CommonScanSpec {
    /// Serialize the shard-invariant common blob to a JSON string.
    ///
    /// The output never contains a `files` key (structurally impossible) nor a
    /// `catalog` key.
    pub fn to_json(&self) -> String {
        serde_json::to_string(self).expect("CommonScanSpec serialization is infallible")
    }

    /// Deserialize a common blob from a JSON string received from `ctx.get(0)`.
    ///
    /// Returns an error that does NOT include the raw input (which carries
    /// credentials).
    pub fn from_json(s: &str) -> Result<Self, String> {
        serde_json::from_str(s).map_err(|e| {
            // Do not echo `s` — it contains credentials.
            format!("scan common spec deserialization failed: {e}")
        })
    }
}

/// The scan specification passed from the adapter to the scan SET UDF.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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

    /// Declared Exasol EMITS type string for each output column, positionally
    /// aligned with the row-scan projection. The scan coerces each emitted Arrow
    /// column to the type this ExaType accepts (via `emit_batch`'s strict feed)
    /// before emitting. Populated by the adapter from the SAME types it writes
    /// into the EMITS clause. Empty (the default) for aggregate scans — which use
    /// the freely-coercing Value emit path — and for specs that predate this
    /// field (backward-compatible).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub emit_exa_types: Vec<String>,

    /// Full logical schema of the Iceberg table at query time: every column
    /// (not just the projected subset), each carrying its Iceberg field-id,
    /// current logical name, Arrow type tag, and nullability.
    ///
    /// The VS adapter populates this once at `resolve_file_list` from
    /// `table.metadata().current_schema()`. The scan UDF uses it to build the
    /// logical Arrow schema and install a `FieldIdExprAdapter` so column binding
    /// is field-id-first (name fallback) — correct across Iceberg schema evolution
    /// (renames, drops, nullable additions).
    ///
    /// Absent (empty, the default) for specs that predate this field; the scan
    /// UDF falls back to first-file schema inference (backward-compatible).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub logical_schema: Vec<LogicalField>,

    pub storage: StorageProps,

    /// DataFusion `target_partitions` for this scan instance.
    /// Controls the number of logical partitions DataFusion creates internally.
    /// Defaults to 1 (no intra-instance partitioning) so the cluster-level shard
    /// fan-out is the sole source of parallelism and nodes are not oversubscribed.
    /// Old specs that lack this field deserialize to 1 (backward-compatible).
    #[serde(default = "default_one_usize")]
    pub df_target_partitions: usize,

    /// DataFusion `batch_size` (rows per Arrow RecordBatch) for this scan instance.
    /// Controls the granularity of DataFusion's internal execution batches.
    /// Defaults to 8192 (DataFusion's own default).
    /// Old specs that lack this field deserialize to 8192 (backward-compatible).
    #[serde(default = "default_batch_size")]
    pub df_batch_size: usize,

    /// Number of Tokio worker threads for the scan runtime.
    /// When 1 (the default), `new_current_thread()` is used (one OS thread).
    /// When > 1, `new_multi_thread().worker_threads(n)` is used.
    /// Old specs that lack this field deserialize to 1 (backward-compatible).
    #[serde(default = "default_one_usize")]
    pub df_threads_per_udf: usize,

    /// Fraction of the net per-instance budget given to the DataFusion memory pool.
    /// Net budget = per-instance RSS limit − container overhead. Old specs that lack
    /// this field deserialize to 0.6 (backward-compatible).
    #[serde(default = "default_memory_pool_fraction")]
    pub memory_pool_fraction: f64,

    /// Fixed container/binary RSS overhead (MB) subtracted from the per-instance
    /// limit before applying `memory_pool_fraction`. Old specs that lack this field
    /// deserialize to 200 (backward-compatible).
    #[serde(default = "default_instance_overhead_mb")]
    pub instance_overhead_mb: u64,

    /// Connection-concurrency budget for the scan's S3-compatible object store:
    /// the number of concurrent connections held warm per host, independent of
    /// the CPU thread/partition budget (`df_target_partitions`/`df_threads_per_udf`).
    /// Old specs that lack this field deserialize to a conservative built-in
    /// default (backward-compatible), clamped to at least 1.
    #[serde(default = "default_s3_max_connections")]
    pub s3_max_connections: usize,
}

fn default_one_usize() -> usize {
    1
}

fn default_batch_size() -> usize {
    8192
}

fn default_memory_pool_fraction() -> f64 {
    0.6
}

fn default_instance_overhead_mb() -> u64 {
    200
}

/// Conservative built-in default for [`CommonScanSpec::s3_max_connections`] /
/// [`ScanSpec::s3_max_connections`] when the field is absent from JSON.
/// Shares the same value as the adapter's AUTO-fallback default so "the
/// default budget" is one number across the round-trip.
fn default_s3_max_connections() -> usize {
    crate::adapter::DEFAULT_S3_MAX_CONNECTIONS
}

impl ScanSpec {
    /// Serialize to a JSON string suitable for `Value::String`.
    pub fn to_json(&self) -> String {
        serde_json::to_string(self).expect("ScanSpec serialization is infallible")
    }

    /// Deserialize a whole `ScanSpec` from JSON; used by tests and as the
    /// pre-split equivalence baseline (production reconstitutes via `from_parts_json`).
    /// Returns an error that does NOT include any credential values.
    pub fn from_json(s: &str) -> Result<Self, String> {
        serde_json::from_str(s).map_err(|e| {
            // Do not echo `s` — it contains credentials.
            format!("scan spec deserialization failed: {e}")
        })
    }

    /// Extract the shard-invariant portion of this spec (everything except `files`).
    pub fn to_common(&self) -> CommonScanSpec {
        CommonScanSpec {
            projection: self.projection.clone(),
            filter: self.filter.clone(),
            limit: self.limit,
            aggregates: self.aggregates.clone(),
            group_keys: self.group_keys.clone(),
            emit_exa_types: self.emit_exa_types.clone(),
            logical_schema: self.logical_schema.clone(),
            storage: self.storage.clone(),
            df_target_partitions: self.df_target_partitions,
            df_batch_size: self.df_batch_size,
            df_threads_per_udf: self.df_threads_per_udf,
            memory_pool_fraction: self.memory_pool_fraction,
            instance_overhead_mb: self.instance_overhead_mb,
            s3_max_connections: self.s3_max_connections,
        }
    }

    /// Serialize the shard-invariant common blob once (the UDF's first argument).
    ///
    /// The output never contains a `files` key nor a `catalog` key.
    pub fn to_common_json(&self) -> String {
        self.to_common().to_json()
    }

    /// Reconstitute a full `ScanSpec` from a shard-invariant common spec and a
    /// per-shard files list. This is the SOLE way to reattach `files`, which makes
    /// `files` the only per-shard field by construction.
    pub fn from_parts(common: CommonScanSpec, files: Vec<String>) -> Self {
        Self {
            files,
            projection: common.projection,
            filter: common.filter,
            limit: common.limit,
            aggregates: common.aggregates,
            group_keys: common.group_keys,
            emit_exa_types: common.emit_exa_types,
            logical_schema: common.logical_schema,
            storage: common.storage,
            df_target_partitions: common.df_target_partitions,
            df_batch_size: common.df_batch_size,
            df_threads_per_udf: common.df_threads_per_udf,
            memory_pool_fraction: common.memory_pool_fraction,
            instance_overhead_mb: common.instance_overhead_mb,
            s3_max_connections: common.s3_max_connections,
        }
    }

    /// Reconstitute a full `ScanSpec` from the two UDF arguments: the common blob
    /// JSON (`ctx.get(0)`) and the per-shard files JSON (`ctx.get(1)`).
    ///
    /// Errors NEVER include the raw inputs (the common blob carries credentials).
    pub fn from_parts_json(common_json: &str, files_json: &str) -> Result<Self, String> {
        let common = CommonScanSpec::from_json(common_json)?;
        let files = Self::files_from_json(files_json)?;
        Ok(Self::from_parts(common, files))
    }

    /// Serialize a per-shard files list to the JSON array carried in the UDF's
    /// second argument. Paired with `files_from_json`.
    pub fn files_json(files: &[String]) -> String {
        serde_json::to_string(files).expect("files list serialization is infallible")
    }

    /// Deserialize a per-shard files list from the UDF's second argument.
    ///
    /// Returns an error that does NOT include the raw input.
    pub fn files_from_json(s: &str) -> Result<Vec<String>, String> {
        serde_json::from_str(s).map_err(|e| {
            // Do not echo `s`.
            format!("scan files deserialization failed: {e}")
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
            emit_exa_types: Vec::new(),
            logical_schema: Vec::new(),
            storage: StorageProps {
                endpoint: "http://minio:9000".into(),
                region: "us-east-1".into(),
                access_key: "minioadmin".into(),
                secret_key: "minioadmin".into(),
                session_token: None,
                allow_http: true,
                path_style: true,
            },
            df_target_partitions: 1,
            df_batch_size: 8192,
            df_threads_per_udf: 1,
            memory_pool_fraction: 0.6,
            instance_overhead_mb: 200,
            s3_max_connections: 8,
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

    /// `emit_exa_types` round-trips through JSON, is omitted when empty, and a
    /// legacy payload lacking it deserializes to an empty Vec (backward-compatible).
    #[test]
    fn emit_exa_types_round_trips_and_defaults_to_empty() {
        // Empty (default): the field is omitted from serialized JSON.
        let row_spec = sample_spec();
        assert!(row_spec.emit_exa_types.is_empty());
        let row_json = row_spec.to_json();
        assert!(
            !row_json.contains("emit_exa_types"),
            "empty emit_exa_types must be absent from JSON: {row_json}"
        );

        // Non-empty: the declared EMITS types survive the round-trip in order.
        let mut spec = sample_spec();
        spec.emit_exa_types = vec![
            "DECIMAL(20,0)".to_string(),
            "VARCHAR(2000000)".to_string(),
            "DOUBLE PRECISION".to_string(),
        ];
        let json = spec.to_json();
        assert!(
            json.contains("emit_exa_types"),
            "non-empty emit_exa_types must appear in JSON: {json}"
        );
        let back = ScanSpec::from_json(&json).unwrap();
        assert_eq!(
            back.emit_exa_types,
            vec![
                "DECIMAL(20,0)".to_string(),
                "VARCHAR(2000000)".to_string(),
                "DOUBLE PRECISION".to_string()
            ]
        );

        // Legacy payload without the field deserializes to an empty Vec.
        let legacy_json = r#"{
            "files": ["s3://w/f0.parquet"],
            "projection": [],
            "storage": {
                "endpoint": "http://minio:9000",
                "region": "us-east-1",
                "access_key": "k",
                "secret_key": "s"
            }
        }"#;
        let legacy = ScanSpec::from_json(legacy_json).unwrap();
        assert!(
            legacy.emit_exa_types.is_empty(),
            "missing emit_exa_types must default to empty (backward-compat)"
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

    /// Task 2.2: logical_schema round-trips through JSON (spec WITH the field) and
    /// a legacy spec WITHOUT it deserializes correctly (backward-compatible default).
    #[test]
    fn logical_schema_round_trips_and_defaults_to_empty() {
        // A spec with a populated logical_schema.
        let mut spec = sample_spec();
        spec.logical_schema = vec![
            LogicalField {
                field_id: 1,
                name: "id".to_string(),
                arrow_type: "int32".to_string(),
                nullable: false,
            },
            LogicalField {
                field_id: 2,
                name: "rating".to_string(),
                arrow_type: "float64".to_string(),
                nullable: true,
            },
            LogicalField {
                field_id: 3,
                name: "label".to_string(),
                arrow_type: "utf8".to_string(),
                nullable: true,
            },
            LogicalField {
                field_id: 4,
                name: "ts".to_string(),
                arrow_type: "timestamp_us".to_string(),
                nullable: true,
            },
            LogicalField {
                field_id: 5,
                name: "amount".to_string(),
                arrow_type: "decimal128(18,4)".to_string(),
                nullable: false,
            },
        ];
        let json = spec.to_json();

        // The field must appear in the serialized JSON when non-empty.
        assert!(
            json.contains("logical_schema"),
            "non-empty logical_schema must appear in JSON: {json}"
        );

        // Round-trip: all fields survive.
        let back = ScanSpec::from_json(&json).unwrap();
        let fields = &back.logical_schema;
        assert_eq!(fields.len(), 5);
        assert_eq!(fields[0].field_id, 1);
        assert_eq!(fields[0].name, "id");
        assert_eq!(fields[0].arrow_type, "int32");
        assert!(!fields[0].nullable);
        assert_eq!(fields[1].field_id, 2);
        assert_eq!(fields[1].name, "rating");
        assert_eq!(fields[1].arrow_type, "float64");
        assert!(fields[1].nullable);
        assert_eq!(fields[2].arrow_type, "utf8");
        assert_eq!(fields[3].arrow_type, "timestamp_us");
        assert_eq!(fields[4].arrow_type, "decimal128(18,4)");
        assert!(!fields[4].nullable);

        // A spec without logical_schema must omit the field from JSON.
        let row_spec = sample_spec();
        assert!(row_spec.logical_schema.is_empty());
        let row_json = row_spec.to_json();
        assert!(
            !row_json.contains("logical_schema"),
            "empty logical_schema must be absent from JSON: {row_json}"
        );

        // A legacy payload without the field deserializes to an empty Vec.
        let legacy_json = r#"{
            "files": ["s3://w/f0.parquet"],
            "projection": [],
            "storage": {
                "endpoint": "http://minio:9000",
                "region": "us-east-1",
                "access_key": "k",
                "secret_key": "s"
            }
        }"#;
        let legacy = ScanSpec::from_json(legacy_json).unwrap();
        assert!(
            legacy.logical_schema.is_empty(),
            "missing logical_schema must default to empty (backward-compat)"
        );
    }

    /// T8 — ScanSpec threading fields round-trip and default to 1 when absent.
    ///
    /// Verifies that:
    /// 1. Explicit `df_target_partitions` / `df_threads_per_udf` values survive
    ///    serialize → deserialize.
    /// 2. A legacy JSON payload that lacks these fields deserializes with both
    ///    fields defaulting to 1 (backward-compatible with pre-existing specs).
    #[test]
    fn scan_spec_threading_fields_round_trip_and_default_to_one() {
        // 1. Explicit values round-trip.
        let mut spec = sample_spec();
        spec.df_target_partitions = 4;
        spec.df_threads_per_udf = 2;
        let json = spec.to_json();
        let back = ScanSpec::from_json(&json).unwrap();
        assert_eq!(
            back.df_target_partitions, 4,
            "df_target_partitions must survive round-trip"
        );
        assert_eq!(
            back.df_threads_per_udf, 2,
            "df_threads_per_udf must survive round-trip"
        );

        // 2. The fields are present in the serialized JSON.
        assert!(
            json.contains("df_target_partitions"),
            "serialized JSON must carry df_target_partitions: {json}"
        );
        assert!(
            json.contains("df_threads_per_udf"),
            "serialized JSON must carry df_threads_per_udf: {json}"
        );

        // 3. A legacy payload without these fields deserializes with both defaulting to 1.
        let legacy_json = r#"{
            "files": ["s3://w/f0.parquet"],
            "projection": [],
            "storage": {
                "endpoint": "http://minio:9000",
                "region": "us-east-1",
                "access_key": "k",
                "secret_key": "s"
            }
        }"#;
        let legacy = ScanSpec::from_json(legacy_json).unwrap();
        assert_eq!(
            legacy.df_target_partitions, 1,
            "missing df_target_partitions must default to 1 (backward-compat)"
        );
        assert_eq!(
            legacy.df_threads_per_udf, 1,
            "missing df_threads_per_udf must default to 1 (backward-compat)"
        );
    }

    /// Task 4.3: df_batch_size round-trips through JSON and defaults correctly on a legacy spec.
    ///
    /// Verifies that:
    /// 1. An explicit `df_batch_size` value survives serialize → deserialize.
    /// 2. A legacy JSON payload lacking the field deserializes to 8192 (backward-compatible).
    #[test]
    fn df_batch_size_round_trips_and_defaults() {
        // 1. Explicit non-default value round-trips.
        let mut spec = sample_spec();
        spec.df_batch_size = 4096;
        let json = spec.to_json();
        let back = ScanSpec::from_json(&json).unwrap();
        assert_eq!(
            back.df_batch_size, 4096,
            "df_batch_size must survive round-trip"
        );

        // 2. The field is present in the serialized JSON.
        assert!(
            json.contains("df_batch_size"),
            "serialized JSON must carry df_batch_size: {json}"
        );

        // 3. A legacy payload without df_batch_size deserializes to 8192.
        let legacy_json = r#"{
            "files": ["s3://w/f0.parquet"],
            "projection": [],
            "storage": {
                "endpoint": "http://minio:9000",
                "region": "us-east-1",
                "access_key": "k",
                "secret_key": "s"
            }
        }"#;
        let legacy = ScanSpec::from_json(legacy_json).unwrap();
        assert_eq!(
            legacy.df_batch_size, 8192,
            "missing df_batch_size must default to 8192 (backward-compat)"
        );
    }

    /// Task 1.2: memory_pool_fraction and instance_overhead_mb round-trip and default correctly.
    ///
    /// Verifies that:
    /// 1. Explicit values survive serialize → deserialize.
    /// 2. A legacy JSON payload lacking both fields deserializes to 0.6 / 200.
    #[test]
    fn scan_spec_memory_fields_round_trip_and_default() {
        // 1. Explicit non-default values round-trip.
        let mut spec = sample_spec();
        spec.memory_pool_fraction = 0.5;
        spec.instance_overhead_mb = 256;
        let json = spec.to_json();
        let back = ScanSpec::from_json(&json).unwrap();
        assert_eq!(
            back.memory_pool_fraction, 0.5,
            "memory_pool_fraction must survive round-trip"
        );
        assert_eq!(
            back.instance_overhead_mb, 256,
            "instance_overhead_mb must survive round-trip"
        );

        // 2. Legacy payload without these fields → defaults 0.6 / 200.
        let legacy_json = r#"{
            "files": ["s3://w/f0.parquet"],
            "projection": [],
            "storage": {
                "endpoint": "http://minio:9000",
                "region": "us-east-1",
                "access_key": "k",
                "secret_key": "s"
            }
        }"#;
        let legacy = ScanSpec::from_json(legacy_json).unwrap();
        assert_eq!(
            legacy.memory_pool_fraction, 0.6,
            "missing memory_pool_fraction must default to 0.6 (backward-compat)"
        );
        assert_eq!(
            legacy.instance_overhead_mb, 200,
            "missing instance_overhead_mb must default to 200 (backward-compat)"
        );
    }

    /// Task 2.2: s3_max_connections round-trips through JSON and defaults to a
    /// conservative built-in budget (clamped to at least 1) when absent.
    ///
    /// Verifies that:
    /// 1. An explicit value survives serialize → deserialize.
    /// 2. A legacy JSON payload lacking the field deserializes to the built-in
    ///    default (backward-compatible).
    #[test]
    fn s3_max_connections_round_trips_and_defaults() {
        // 1. Explicit non-default value round-trips.
        let mut spec = sample_spec();
        spec.s3_max_connections = 32;
        let json = spec.to_json();
        let back = ScanSpec::from_json(&json).unwrap();
        assert_eq!(
            back.s3_max_connections, 32,
            "s3_max_connections must survive round-trip"
        );

        // 2. The field is present in the serialized JSON.
        assert!(
            json.contains("s3_max_connections"),
            "serialized JSON must carry s3_max_connections: {json}"
        );

        // 3. A legacy payload without the field deserializes to the built-in default.
        let legacy_json = r#"{
            "files": ["s3://w/f0.parquet"],
            "projection": [],
            "storage": {
                "endpoint": "http://minio:9000",
                "region": "us-east-1",
                "access_key": "k",
                "secret_key": "s"
            }
        }"#;
        let legacy = ScanSpec::from_json(legacy_json).unwrap();
        assert_eq!(
            legacy.s3_max_connections,
            default_s3_max_connections(),
            "missing s3_max_connections must default to the built-in budget (backward-compat)"
        );
        assert!(
            legacy.s3_max_connections >= 1,
            "default s3_max_connections must be clamped to at least 1"
        );

        // 4. The default also applies to CommonScanSpec (shard-invariant blob).
        let legacy_common_json = r#"{
            "projection": [],
            "storage": {
                "endpoint": "http://minio:9000",
                "region": "us-east-1",
                "access_key": "k",
                "secret_key": "s"
            }
        }"#;
        let legacy_common = CommonScanSpec::from_json(legacy_common_json).unwrap();
        assert_eq!(
            legacy_common.s3_max_connections,
            default_s3_max_connections(),
            "missing s3_max_connections must default on CommonScanSpec too (backward-compat)"
        );

        // 5. The value threads through the split (to_common) / merge (from_parts) impls.
        let split = spec.to_common();
        assert_eq!(
            split.s3_max_connections, 32,
            "to_common must carry s3_max_connections through the split"
        );
        let merged = ScanSpec::from_parts(split, spec.files.clone());
        assert_eq!(
            merged.s3_max_connections, 32,
            "from_parts must carry s3_max_connections through the merge"
        );
    }

    /// Task 1.3(a): the common blob serializes WITHOUT `files`, and reconstituting
    /// via `from_parts` (through JSON) yields a spec equal to the pre-split spec.
    #[test]
    fn from_parts_reconstitutes_equal_spec() {
        let original = sample_spec();

        // Split into the shard-invariant common blob + the per-shard files list.
        let common_json = original.to_common_json();
        let files_json = ScanSpec::files_json(&original.files);

        // The common blob must NOT carry the per-shard files list (type-level guarantee).
        assert!(
            !common_json.contains("\"files\""),
            "common blob must not contain a files key: {common_json}"
        );
        // Nor may any file URI value leak into the common blob.
        assert!(
            !common_json.contains("part-00000.parquet"),
            "common blob must not carry any file URI: {common_json}"
        );

        // The common blob round-trips on its own.
        let common_back = CommonScanSpec::from_json(&common_json).unwrap();
        assert_eq!(common_back, original.to_common());

        // from_parts_json reconstitutes a spec equal to the pre-split original.
        let reconstituted = ScanSpec::from_parts_json(&common_json, &files_json).unwrap();
        assert_eq!(reconstituted, original);

        // The struct-level from_parts is equivalent to the JSON round-trip.
        let via_struct = ScanSpec::from_parts(original.to_common(), original.files.clone());
        assert_eq!(via_struct, original);
    }

    /// Task 1.3(b): malformed common OR files JSON produces errors that never echo
    /// the raw input (which carries credentials).
    #[test]
    fn malformed_common_or_files_json_does_not_leak_credentials() {
        // Malformed common blob carrying credential-shaped values.
        let garbled_common =
            r#"{"storage": {"access_key": "SECRET", "secret_key": "TOPSECRET"}, incomplete"#;
        let err = CommonScanSpec::from_json(garbled_common).unwrap_err();
        assert!(
            !err.contains("SECRET"),
            "common error leaked a secret: {err}"
        );
        assert!(
            !err.contains("TOPSECRET"),
            "common error leaked a secret: {err}"
        );
        assert!(err.contains("scan common spec deserialization failed"));

        // Malformed files argument.
        let garbled_files = r#"["s3://w/SECRETFILE.parquet", incomplete"#;
        let files_err = ScanSpec::files_from_json(garbled_files).unwrap_err();
        assert!(
            !files_err.contains("SECRETFILE"),
            "files error leaked input: {files_err}"
        );
        assert!(files_err.contains("scan files deserialization failed"));

        // from_parts_json surfaces the common-arg error without leaking either input.
        let combined = ScanSpec::from_parts_json(garbled_common, "[]").unwrap_err();
        assert!(!combined.contains("SECRET"));
        assert!(!combined.contains("TOPSECRET"));
    }

    /// Task 1.3(c): `catalog` no longer appears in any serialized JSON.
    #[test]
    fn catalog_absent_from_all_serialized_json() {
        let spec = sample_spec();
        assert!(
            !spec.to_json().contains("catalog"),
            "full spec JSON must not contain a catalog key: {}",
            spec.to_json()
        );
        assert!(
            !spec.to_common_json().contains("catalog"),
            "common blob JSON must not contain a catalog key: {}",
            spec.to_common_json()
        );
    }
}
