use super::test_support::*;
use super::*;
use crate::scan::spec::{
    CommonScanSpec, FileEntry, LogicalField, ProjectionItem, ScanSpec, ScanStorage, StorageProps,
};

// The vended sentinels below repeat the literal values `lakehouse-catalog`'s own
// `test_support` uses, so both crates' assertions stay comparable.
const VENDED_AK: &str = "VENDED_AK_SENTINEL";
const VENDED_SK: &str = "VENDED_SK_SENTINEL";
const VENDED_TOK: &str = "VENDED_TOKEN_SENTINEL";
const VENDED_REGION: &str = "eu-west-2";

/// Scenario: Catalog auth props are never placed in any scan spec, even with vended credentials.
#[test]
fn catalog_auth_secrets_never_in_scan_spec_with_vending() {
    let vended_storage = StorageBackend::S3(StorageProps {
        endpoint: "https://s3.amazonaws.com".into(),
        region: VENDED_REGION.into(),
        access_key: VENDED_AK.into(),
        secret_key: VENDED_SK.into(),
        session_token: Some(VENDED_TOK.into()),
        path_style: false,
        ..Default::default()
    });

    let spec = ScanSpec {
        common: CommonScanSpec {
            projection: vec!["ID".into()],
            storage: ScanStorage::Inline(vended_storage),
            ..Default::default()
        },
        files: vec![FileEntry::new(
            "s3://warehouse/db/events/part-00000.parquet",
            1,
        )],
    };

    let json = spec.to_json();

    // Match `"<field>":` exactly, since e.g. `"session_token"` contains `"token"`.
    for field in [
        "\"token\":",
        "\"credential\":",
        "\"client_id\":",
        "\"client_secret\":",
        "\"oauth2_server_uri\":",
        "\"oauth2-server-uri\":",
        // scope appears in storage endpoint strings, so it is checked by key name only.
    ] {
        assert!(
            !json.contains(field),
            "ScanSpec JSON must not carry auth field key '{field}': {json}"
        );
    }

    assert!(
        json.contains(VENDED_AK),
        "vended access_key must be in storage: {json}"
    );
    assert!(
        json.contains(VENDED_TOK),
        "vended session_token must be in storage: {json}"
    );
}

/// Scenario: A grouped scan spec carries the group keys' rendered SQL fragments.
#[test]
fn grouped_scan_spec_carries_group_keys() {
    let group_keys = vec!["\"REGION\"".to_string(), "YEAR(\"TS\")".to_string()];
    let spec = ScanSpec {
        common: CommonScanSpec {
            aggregates: Some(vec![AggregatePlan {
                kind: AggKind::Count,
                column: None,
                arg_expr: None,
            }]),
            group_keys: Some(group_keys.clone()),
            storage: ScanStorage::Inline(sample_storage()),
            ..Default::default()
        },
        files: vec![FileEntry::new("s3://w/f0.parquet", 1)],
    };
    let json = spec.to_json();
    let back = ScanSpec::from_json(&json).expect("must round-trip");
    let keys = back.common.group_keys.expect("group_keys must be present");
    assert_eq!(keys, group_keys, "group_keys must survive spec round-trip");
}

/// Scenario: A LIKE-only filter still yields a DataFusion filter but no Iceberg pruning predicate.
#[test]
fn like_filter_yields_df_string_and_no_iceberg_predicate() {
    use crate::adapter::iceberg_predicate::to_iceberg_predicate;
    use iceberg::spec::{NestedField, Schema, Type};
    use std::sync::Arc;

    let schema = Schema::builder()
        .with_schema_id(1)
        .with_fields(vec![Arc::new(NestedField::optional(
            1,
            "name",
            Type::Primitive(iceberg::spec::PrimitiveType::String),
        ))])
        .build()
        .unwrap();

    let filter_json = serde_json::json!({
        "type": "predicate_like",
        "expression": {"type": "column", "name": "name"},
        "pattern": {"type": "literal_string", "value": "A%"}
    });

    let df_filter = render_df_filter_safe(&filter_json);
    assert!(
        df_filter.is_some(),
        "LIKE filter must still produce a DataFusion SQL string: {df_filter:?}"
    );

    let iceberg_pred = to_iceberg_predicate(&filter_json, &schema);
    assert!(
        iceberg_pred.is_none(),
        "LIKE filter must produce no Iceberg predicate"
    );
}

/// Scenario: `LENGTH(<DECIMAL>) > 5` renders the Exasol trim form exactly once through the WHERE pipeline (#211).
#[test]
fn where_filter_decimal_stringification_rewritten_to_trim() {
    let col_types = vec![("C_DECIMAL_A".to_string(), "DECIMAL(10,2)".to_string())];
    let filter_json = serde_json::json!({
        "type": "predicate_greater",
        "left": {
            "type": "function_scalar",
            "name": "LENGTH",
            "arguments": [{"type": "column", "name": "c_decimal_a"}]
        },
        "right": {"type": "literal_exactnumeric", "value": 5}
    });

    let rendered = Some(&filter_json)
        .and_then(|f| apply_type_rewrites(f, &col_types))
        .and_then(|f| render_df_filter_safe(&f))
        .expect("LENGTH(decimal) > 5 must render to a DataFusion filter");

    let trim_wrapper = "regexp_replace(regexp_replace(CAST(";
    assert_eq!(
        rendered.matches(trim_wrapper).count(),
        1,
        "the rewritten filter must carry the Exasol decimal-trim form EXACTLY ONCE \
             (string-fn guard wraps it, decimal rewrite must then no-op): {rendered}"
    );
    assert!(
        !rendered.contains(r#"character_length("C_DECIMAL_A")"#),
        "the filter must NOT stringify the bare decimal column untrimmed: {rendered}"
    );
}

/// Scenario: A DECIMAL column in a non-stringifying WHERE context stays a bare column reference.
#[test]
fn filter_decimal_comparison_not_rewritten() {
    let col_types = vec![("C_DECIMAL_A".to_string(), "DECIMAL(10,2)".to_string())];
    let filter_json = serde_json::json!({
        "type": "predicate_greater",
        "left": {"type": "column", "name": "c_decimal_a"},
        "right": {"type": "literal_exactnumeric", "value": 5}
    });

    let rendered = Some(&filter_json)
        .and_then(|f| apply_type_rewrites(f, &col_types))
        .and_then(|f| render_df_filter_safe(&f))
        .expect("c_decimal_a > 5 must render to a DataFusion filter");

    assert_eq!(
        rendered, r#"("C_DECIMAL_A" > 5)"#,
        "a DECIMAL column in a comparison must stay a bare, unwrapped column reference: {rendered}"
    );
    assert!(
        !rendered.contains("regexp_replace"),
        "a non-stringifying filter context must not be trimmed: {rendered}"
    );
}

/// Scenario: `UPPER(c_decimal_a) = 'X'` coerces the DECIMAL argument to the trimmed text form (#210).
#[test]
fn where_filter_string_fn_under_comparison_predicate_coerced() {
    let col_types = vec![("C_DECIMAL_A".to_string(), "DECIMAL(10,2)".to_string())];
    let filter_json = serde_json::json!({
        "type": "predicate_equal",
        "left": {
            "type": "function_scalar",
            "name": "UPPER",
            "arguments": [{"type": "column", "name": "c_decimal_a"}]
        },
        "right": {"type": "literal_string", "value": "X"}
    });

    let rendered = Some(&filter_json)
        .and_then(|f| apply_type_rewrites(f, &col_types))
        .and_then(|f| render_df_filter_safe(&f))
        .expect("UPPER(decimal) = 'X' must render to a DataFusion filter");

    assert!(
        rendered.contains("regexp_replace(regexp_replace(CAST("),
        "the DECIMAL argument nested under predicate_equal's left must be coerced \
             into the Exasol decimal-trim form: {rendered}"
    );
}

/// Scenario: `UPPER(c_double) = 'X'` keeps the predicate out of the scan spec (#210).
#[test]
fn where_filter_string_fn_over_double_declines() {
    let col_types = vec![("C_DOUBLE_A".to_string(), "DOUBLE PRECISION".to_string())];
    let filter_json = serde_json::json!({
        "type": "predicate_equal",
        "left": {
            "type": "function_scalar",
            "name": "UPPER",
            "arguments": [{"type": "column", "name": "c_double_a"}]
        },
        "right": {"type": "literal_string", "value": "X"}
    });

    let rendered = Some(&filter_json)
        .and_then(|f| apply_type_rewrites(f, &col_types))
        .and_then(|f| render_df_filter_safe(&f));

    assert!(
        rendered.is_none(),
        "UPPER over a DOUBLE PRECISION column must decline the whole filter, \
             not push a possibly-wrong text comparison: {rendered:?}"
    );
}

/// Scenario: A DECIMAL argument nested inside a LIKE subject's `UPPER` is coerced (#210).
#[test]
fn where_filter_upper_decimal_inside_like_subject_coerced() {
    let col_types = vec![("C_DECIMAL_A".to_string(), "DECIMAL(10,2)".to_string())];
    let filter_json = serde_json::json!({
        "type": "predicate_like",
        "expression": {
            "type": "function_scalar",
            "name": "UPPER",
            "arguments": [{"type": "column", "name": "c_decimal_a"}]
        },
        "pattern": {"type": "literal_string", "value": "1%"}
    });

    let rendered = Some(&filter_json)
        .and_then(|f| apply_type_rewrites(f, &col_types))
        .and_then(|f| render_df_filter_safe(&f))
        .expect("UPPER(decimal) LIKE '1%' must render to a DataFusion filter");

    assert!(
        rendered.contains("regexp_replace(regexp_replace(CAST("),
        "the DECIMAL argument nested inside the LIKE subject's UPPER call must be \
             coerced into the Exasol decimal-trim form, even though guard_like_subject \
             itself leaves this non-bare-column LIKE subject untouched: {rendered}"
    );
}

/// Scenario: A DECIMAL LIKE nested inside a CASE under `predicate_equal` declines the filter (#207).
#[test]
fn where_filter_like_decimal_inside_case_declines_whole_filter() {
    let col_types = vec![("AMOUNT".to_string(), "DECIMAL(9,2)".to_string())];
    let filter_json = serde_json::json!({
        "type": "predicate_equal",
        "left": {
            "type": "function_scalar_case",
            "name": "CASE",
            "arguments": [
                {
                    "type": "predicate_like",
                    "expression": {"type": "column", "name": "amount"},
                    "pattern": {"type": "literal_string", "value": "9%"}
                }
            ],
            "results": [
                {"type": "literal_exactnumeric", "value": 1},
                {"type": "literal_exactnumeric", "value": 0}
            ]
        },
        "right": {"type": "literal_exactnumeric", "value": 1}
    });

    let rendered = Some(&filter_json)
        .and_then(|f| apply_type_rewrites(f, &col_types))
        .and_then(|f| render_df_filter_safe(&f));

    assert!(
        rendered.is_none(),
        "a DECIMAL LIKE buried inside a function_scalar_case under predicate_equal's \
             left must decline the whole filter through the full wired chain, not push a \
             possibly-wrong native comparison: {rendered:?}"
    );
}

/// Scenario: Catalog auth props and the whole catalog block are never placed in any scan spec.
#[test]
fn scan_spec_carries_no_catalog_block() {
    const TOKEN_SENTINEL: &str = "TOKEN_SENTINEL_VALUE";
    const SECRET_SENTINEL: &str = "CLIENT_SECRET_SENTINEL_VALUE";
    const OAUTH_URI_SENTINEL: &str = "https://oauth-uri-sentinel.example/token";
    const SCOPE_SENTINEL: &str = "SCOPE_SENTINEL_VALUE";

    let spec = ScanSpec {
        common: CommonScanSpec {
            projection: vec!["ID".into(), "NAME".into()],
            filter: Some("(\"ID\" > 10)".into()),
            limit: Some(100),
            storage: ScanStorage::Inline(sample_storage()),
            ..Default::default()
        },
        files: vec![FileEntry::new(
            "s3://warehouse/db/events/part-00000.parquet",
            1,
        )],
    };

    let json = spec.to_json();

    assert!(
        !json.contains("catalog"),
        "ScanSpec JSON must not carry a catalog block: {json}"
    );
    assert!(
        !spec.to_common_json().contains("catalog"),
        "common blob must not carry a catalog block: {}",
        spec.to_common_json()
    );

    for field in [
        "token",
        "credential",
        "client_id",
        "client_secret",
        "oauth2_server_uri",
        "oauth2-server-uri",
        "scope",
    ] {
        assert!(
            !json.contains(field),
            "ScanSpec JSON must not carry auth field '{field}': {json}"
        );
    }

    for value in [
        TOKEN_SENTINEL,
        SECRET_SENTINEL,
        OAUTH_URI_SENTINEL,
        SCOPE_SENTINEL,
    ] {
        assert!(
            !json.contains(value),
            "ScanSpec JSON must not carry auth value '{value}': {json}"
        );
    }

    assert!(
        json.contains("minioadmin"),
        "storage S3 creds must still be present: {json}"
    );
}

/// Scenario: A pushdown scan spec's `logical_schema` carries field-ids, names, and nullability.
#[test]
fn pushdown_carries_logical_schema_in_common_arg() {
    use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};
    use std::sync::Arc;

    let schema = Schema::builder()
        .with_schema_id(1)
        .with_fields(vec![
            Arc::new(NestedField::required(
                1,
                "id",
                Type::Primitive(PrimitiveType::Int),
            )),
            Arc::new(NestedField::optional(
                2,
                "score",
                Type::Primitive(PrimitiveType::Double),
            )),
            Arc::new(NestedField::required(
                3,
                "label",
                Type::Primitive(PrimitiveType::String),
            )),
            Arc::new(NestedField::optional(
                4,
                "amount",
                Type::Primitive(PrimitiveType::Decimal {
                    precision: 18,
                    scale: 4,
                }),
            )),
        ])
        .build()
        .unwrap();

    let logical = build_logical_schema(&schema);

    assert_eq!(logical.len(), 4, "must carry all 4 fields");

    assert_eq!(logical[0].field_id, Some(1));
    assert_eq!(logical[0].name, "id");
    assert_eq!(logical[0].arrow_type, "int32");
    assert!(
        !logical[0].nullable,
        "required field must have nullable=false"
    );

    assert_eq!(logical[1].field_id, Some(2));
    assert_eq!(logical[1].name, "score");
    assert_eq!(logical[1].arrow_type, "float64");
    assert!(
        logical[1].nullable,
        "optional field must have nullable=true"
    );

    assert_eq!(logical[2].field_id, Some(3));
    assert_eq!(logical[2].name, "label");
    assert_eq!(logical[2].arrow_type, "utf8");
    assert!(!logical[2].nullable);

    assert_eq!(logical[3].field_id, Some(4));
    assert_eq!(logical[3].name, "amount");
    assert_eq!(logical[3].arrow_type, "decimal128(18,4)");
    assert!(logical[3].nullable);

    let spec = ScanSpec {
        common: CommonScanSpec {
            logical_schema: logical.clone(),
            storage: ScanStorage::Inline(sample_storage()),
            ..Default::default()
        },
        files: vec![],
    };
    let json = spec.to_json();
    let back = ScanSpec::from_json(&json).unwrap();
    assert_eq!(
        back.common.logical_schema.len(),
        4,
        "logical_schema must survive ScanSpec JSON round-trip"
    );
    assert_eq!(back.common.logical_schema[0], logical[0]);
    assert_eq!(back.common.logical_schema[3], logical[3]);

    let common_json = spec.to_common_json();
    let common_back = crate::scan::spec::CommonScanSpec::from_json(&common_json).unwrap();
    assert_eq!(
        common_back.logical_schema, logical,
        "logical_schema must be carried in the common arg"
    );
}

/// Scenario: Primitive required and nullable fields carry their Iceberg `initial-default` as raw scalar text.
#[test]
fn build_logical_schema_encodes_primitive_initial_default() {
    use iceberg::spec::{Literal, NestedField, PrimitiveType, Schema, Type};
    use std::sync::Arc;

    let schema = Schema::builder()
        .with_schema_id(1)
        .with_fields(vec![
            Arc::new(
                NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long))
                    .with_initial_default(Literal::long(7)),
            ),
            Arc::new(
                NestedField::optional(2, "note", Type::Primitive(PrimitiveType::String))
                    .with_initial_default(Literal::string("hi")),
            ),
        ])
        .build()
        .unwrap();

    let logical = build_logical_schema(&schema);

    assert_eq!(logical.len(), 2);

    assert_eq!(logical[0].field_id, Some(1));
    assert!(!logical[0].nullable, "required field must be non-nullable");
    assert_eq!(logical[0].arrow_type, "int64");
    assert_eq!(
        logical[0].initial_default.as_deref(),
        Some("7"),
        "required-with-default must encode its default"
    );

    assert_eq!(logical[1].field_id, Some(2));
    assert!(logical[1].nullable, "optional field must be nullable");
    assert_eq!(logical[1].arrow_type, "utf8");
    assert_eq!(
        logical[1].initial_default.as_deref(),
        Some("hi"),
        "nullable-with-default must encode its default"
    );
}

/// Scenario: A field with no `initial-default` encodes no default.
#[test]
fn build_logical_schema_omits_default_for_no_default_field() {
    use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};
    use std::sync::Arc;

    let schema = Schema::builder()
        .with_schema_id(1)
        .with_fields(vec![Arc::new(NestedField::optional(
            1,
            "plain",
            Type::Primitive(PrimitiveType::Int),
        ))])
        .build()
        .unwrap();

    let logical = build_logical_schema(&schema);

    assert_eq!(logical.len(), 1);
    assert!(
        logical[0].initial_default.is_none(),
        "a field without an initial-default must encode None"
    );
}

/// Scenario: A decimal outside Exasol's domain maps to `utf8` and encodes no default, or its mantissa would leak.
#[test]
fn build_logical_schema_omits_default_for_decimal_outside_exasol_domain() {
    use iceberg::spec::{Literal, NestedField, PrimitiveType, Schema, Type};
    use std::sync::Arc;

    let schema = Schema::builder()
        .with_schema_id(1)
        .with_fields(vec![
            Arc::new(
                NestedField::optional(
                    1,
                    "scale_over_precision",
                    Type::Primitive(PrimitiveType::Decimal {
                        precision: 5,
                        scale: 10,
                    }),
                )
                .with_initial_default(Literal::decimal(1234)),
            ),
            Arc::new(
                NestedField::optional(
                    2,
                    "zero_precision",
                    Type::Primitive(PrimitiveType::Decimal {
                        precision: 0,
                        scale: 0,
                    }),
                )
                .with_initial_default(Literal::decimal(0)),
            ),
        ])
        .build()
        .unwrap();

    let logical = build_logical_schema(&schema);

    assert_eq!(logical.len(), 2);
    for field in &logical {
        assert_eq!(
            field.arrow_type, "utf8",
            "field {} must fall back to the utf8 tag",
            field.name
        );
        assert!(
            field.initial_default.is_none(),
            "field {} must encode no default under the utf8 tag",
            field.name
        );
    }
}

/// Scenario: A struct `initial-default` encodes no default, a deliberate Exasol no-struct trade-off.
#[test]
fn build_logical_schema_omits_non_primitive_default() {
    use iceberg::spec::{Literal, NestedField, PrimitiveType, Schema, Struct, StructType, Type};
    use std::sync::Arc;

    let struct_type = Type::Struct(StructType::new(vec![Arc::new(NestedField::required(
        100,
        "x",
        Type::Primitive(PrimitiveType::Int),
    ))]));
    let struct_default = Literal::Struct(Struct::from_iter([Some(Literal::int(7))]));

    let schema = Schema::builder()
        .with_schema_id(1)
        .with_fields(vec![Arc::new(
            NestedField::optional(1, "meta", struct_type).with_initial_default(struct_default),
        )])
        .build()
        .unwrap();

    let logical = build_logical_schema(&schema);

    assert_eq!(logical.len(), 1);
    assert_eq!(
        logical[0].arrow_type, "utf8",
        "a struct maps to the JSON-fallback utf8 tag"
    );
    assert!(
        logical[0].initial_default.is_none(),
        "a non-primitive struct initial-default must encode NO default"
    );
}

/// Scenario: A field carrying only a `write-default` encodes no default.
#[test]
fn build_logical_schema_ignores_write_default() {
    use iceberg::spec::{Literal, NestedField, PrimitiveType, Schema, Type};
    use std::sync::Arc;

    let schema = Schema::builder()
        .with_schema_id(1)
        .with_fields(vec![Arc::new(
            NestedField::optional(1, "w", Type::Primitive(PrimitiveType::Int))
                .with_write_default(Literal::int(5)),
        )])
        .build()
        .unwrap();

    let logical = build_logical_schema(&schema);

    assert_eq!(logical.len(), 1);
    assert!(
        logical[0].initial_default.is_none(),
        "write-default must never be read into initial_default"
    );
}

/// Scenario: A serialized encoded default carries no storage credential.
#[test]
fn build_logical_schema_default_encoding_is_credential_free() {
    use iceberg::spec::{Literal, NestedField, PrimitiveType, Schema, Type};
    use std::sync::Arc;

    let schema = Schema::builder()
        .with_schema_id(1)
        .with_fields(vec![Arc::new(
            NestedField::optional(1, "label", Type::Primitive(PrimitiveType::String))
                .with_initial_default(Literal::string("plain-default")),
        )])
        .build()
        .unwrap();

    let logical = build_logical_schema(&schema);
    assert_eq!(logical[0].initial_default.as_deref(), Some("plain-default"));

    let json = serde_json::to_string(&logical).unwrap();
    for marker in ["access_key", "secret_key", "session_token", "endpoint"] {
        assert!(
            !json.contains(marker),
            "encoded default carrier must be credential-free, found '{marker}': {json}"
        );
    }
}

/// Scenario: A default-less schema round-trips unchanged and older specs deserialize identically.
#[test]
fn build_logical_schema_default_less_spec_round_trips_unchanged() {
    use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};
    use std::sync::Arc;

    let schema = Schema::builder()
        .with_schema_id(1)
        .with_fields(vec![
            Arc::new(NestedField::required(
                1,
                "id",
                Type::Primitive(PrimitiveType::Long),
            )),
            Arc::new(NestedField::optional(
                2,
                "name",
                Type::Primitive(PrimitiveType::String),
            )),
        ])
        .build()
        .unwrap();

    let logical = build_logical_schema(&schema);
    assert!(
        logical.iter().all(|f| f.initial_default.is_none()),
        "a default-less schema must encode no defaults"
    );

    let spec = ScanSpec {
        common: CommonScanSpec {
            logical_schema: logical.clone(),
            storage: ScanStorage::Inline(sample_storage()),
            ..Default::default()
        },
        files: vec![],
    };
    let json = spec.to_json();
    assert!(
        !json.contains("initial_default"),
        "absent defaults must be omitted from JSON: {json}"
    );
    let back = ScanSpec::from_json(&json).unwrap();
    assert_eq!(
        back.common.logical_schema, logical,
        "a default-less spec must round-trip unchanged"
    );
}

fn guard_col_types() -> Vec<(String, String)> {
    vec![
        ("REGION".to_string(), "VARCHAR(2000000)".to_string()),
        ("NAME".to_string(), "VARCHAR(2000000)".to_string()),
        ("AMOUNT".to_string(), "DECIMAL(18,2)".to_string()),
        ("ID".to_string(), "DECIMAL(20,0)".to_string()),
    ]
}

fn guard_events_request(pushdown_req: Json) -> Json {
    serde_json::json!({
        "involvedTables": [{
            "name": "EVENTS",
            "columns": [
                {"name": "REGION", "dataType": {"type": "varchar", "size": 2000000}},
                {"name": "NAME", "dataType": {"type": "varchar", "size": 2000000}},
                {"name": "AMOUNT", "dataType": {"type": "decimal", "precision": 18, "scale": 2}},
                {"name": "ID", "dataType": {"type": "decimal", "precision": 20, "scale": 0}},
            ],
        }],
        "pushdownRequest": pushdown_req,
    })
}

/// `projection_widened` is the dispatcher's routing flag (#196).
fn guard_dispatch_sql(
    request: &Json,
    proj_cols: Vec<ProjectionItem>,
    proj_types: Vec<String>,
    projection_widened: bool,
    limit: Option<u64>,
    logical_schema: Vec<LogicalField>,
) -> String {
    let result = guard_dispatch_result(
        request,
        proj_cols,
        proj_types,
        projection_widened,
        limit,
        logical_schema,
    )
    .expect("build_dispatch_sql must succeed for this declined-ORDER-BY fixture");
    result["sql"]
        .as_str()
        .expect("pushdown response must carry a sql field")
        .to_string()
}

/// An unrenderable pushed sort key is a `User` error, not SQL.
fn guard_dispatch_result(
    request: &Json,
    proj_cols: Vec<ProjectionItem>,
    proj_types: Vec<String>,
    projection_widened: bool,
    limit: Option<u64>,
    logical_schema: Vec<LogicalField>,
) -> Result<Json, UdfError> {
    let pushdown_req = pd(request);
    let has_order_by = order_by_present(&pushdown_req);
    build_dispatch_sql(
        request,
        &pushdown_req,
        proj_cols,
        proj_types,
        projection_widened,
        guard_col_types(),
        None,
        None,
        limit,
        has_order_by,
        &[vec![FileEntry::new("data/part-0.parquet", 1_000)]],
        "s3://warehouse/db/events".to_string(),
        logical_schema,
        Vec::new(),
        Vec::new(),
        &sample_scan_storage(),
        SCAN_UDF_NAME,
        DISTRIBUTE_FILES_UDF_NAME,
        4,
        8192,
        2,
        0.6,
        200,
        8,
    )
}

/// A shape `like_subject_type_guard` declines though Exasol renders it.
fn declined_like_on_decimal() -> Json {
    serde_json::json!({
        "type": "predicate_like",
        "expression": {"type": "column", "name": "AMOUNT", "tableName": "EVENTS"},
        "pattern": {"type": "literal_string", "value": "1%"},
    })
}

/// The empty logical schema declines the bounded top-N unconditionally, so the
/// decline route must win on its own.
fn dispatch_sql_for_body(pushdown_req_body: Json) -> String {
    let request = guard_events_request(pushdown_req_body);
    let pushdown_req = pd(&request);
    let col_types = guard_col_types();
    let (proj_cols, proj_types, projection_widened) =
        extract_projection(&request, &pushdown_req).expect("the fixture must project");
    let (filter, declined_filter) = classify_where_filter(
        pushdown_req.get("filter").filter(|f| !f.is_null()),
        &col_types,
    );
    let result = build_dispatch_sql(
        &request,
        &pushdown_req,
        proj_cols,
        proj_types,
        projection_widened,
        col_types,
        filter,
        declined_filter,
        extract_limit(&pushdown_req),
        order_by_present(&pushdown_req),
        &[vec![FileEntry::new("data/part-0.parquet", 1_000)]],
        "s3://warehouse/db/events".to_string(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        &sample_scan_storage(),
        SCAN_UDF_NAME,
        DISTRIBUTE_FILES_UDF_NAME,
        4,
        8192,
        2,
        0.6,
        200,
        8,
    )
    .expect("build_dispatch_sql must succeed for this fixture");
    result["sql"]
        .as_str()
        .expect("pushdown response must carry a sql field")
        .to_string()
}

/// Scenario: A declined WHERE filter routes every dispatch shape to the wrapper, applied exactly once.
#[test]
fn declined_filter_routes_every_dispatch_shape_to_qualified_wrapper() {
    let declined = declined_like_on_decimal();
    let id_col = serde_json::json!({"type": "column", "name": "ID", "tableName": "EVENTS"});
    let region_col = serde_json::json!({"type": "column", "name": "REGION", "tableName": "EVENTS"});
    let shapes = [
        (
            "row scan",
            serde_json::json!({
                "selectList": [id_col.clone()],
                "selectListDataTypes": [{"type": "decimal", "precision": 20, "scale": 0}],
                "filter": declined.clone(),
            }),
        ),
        (
            "grouped aggregate",
            serde_json::json!({
                "aggregationType": "group_by",
                "groupBy": [region_col.clone()],
                "selectList": [region_col, agg_item("COUNT", None, false)],
                "selectListDataTypes": [
                    {"type": "varchar", "size": 2000000},
                    {"type": "decimal", "precision": 18, "scale": 0},
                ],
                "filter": declined.clone(),
            }),
        ),
        (
            "ordered top-N",
            serde_json::json!({
                "selectList": [id_col.clone()],
                "selectListDataTypes": [{"type": "decimal", "precision": 20, "scale": 0}],
                "orderBy": [{
                    "type": "order_by_element",
                    "expression": id_col,
                    "isAscending": true,
                    "nullsLast": true,
                }],
                "limit": {"numElements": 5},
                "filter": declined.clone(),
            }),
        ),
    ];

    for (shape, body) in shapes {
        let sql = dispatch_sql_for_body(body);
        let where_at = sql.find(r#"AS "LHS_T0" WHERE "#).unwrap_or_else(|| {
            panic!("the {shape} shape must route to the qualified wrapper: {sql}")
        });
        assert!(
            sql[where_at..].contains("LIKE") && sql[where_at..].contains(r#""LHS_T0"."AMOUNT""#),
            "the {shape} shape's wrapper WHERE must carry the declined predicate, \
                 table-qualified: {sql}"
        );
        assert!(
            !sql.contains(r#""filter""#),
            "the {shape} shape's fan-out scan spec must carry no filter — the \
                 declined predicate is applied exactly once: {sql}"
        );
    }
}

/// Scenario: A trivially true filter is omitted with no wrapper.
#[test]
fn trivially_true_filter_omitted_without_wrapper() {
    let trivially_true = serde_json::json!({"type": "literal_bool", "value": true});
    let (filter, declined) = classify_where_filter(Some(&trivially_true), &guard_col_types());
    assert!(
        filter.is_none() && declined.is_none(),
        "a trivially-true filter is neither pushed nor declined: {filter:?} {declined:?}"
    );

    let sql = dispatch_sql_for_body(serde_json::json!({
        "selectList": [{"type": "column", "name": "ID", "tableName": "EVENTS"}],
        "selectListDataTypes": [{"type": "decimal", "precision": 20, "scale": 0}],
        "filter": trivially_true,
    }));

    assert!(
        !sql.contains("LHS_T0"),
        "a trivially-true filter must keep the wrapper-free fast scan: {sql}"
    );
    assert!(
        !sql.contains(r#""filter""#),
        "a trivially-true filter must not reach the scan spec: {sql}"
    );
}

/// Scenario: A `SELECT *` request with a declined filter projects the full base row, not only filter columns.
#[test]
fn declined_filter_with_absent_select_list_projects_full_row() {
    let sql = dispatch_sql_for_body(serde_json::json!({
        "filter": declined_like_on_decimal(),
    }));

    assert!(
        sql.contains(r#""projection":["REGION","NAME","AMOUNT","ID"]"#),
        "the inner scan must emit every base-row column in col_types order, not \
             only the filter's: {sql}"
    );
    assert!(
        sql.starts_with(
            r#"SELECT "LHS_T0"."REGION", "LHS_T0"."NAME", "LHS_T0"."AMOUNT", "LHS_T0"."ID" FROM ("#
        ),
        "the wrapper's outer select list must be the full base row, in order: {sql}"
    );
}

/// Scenario: A declined filter beside a real select list keeps referenced-column narrowing (#160).
#[test]
fn declined_filter_with_a_real_select_list_keeps_the_narrowing() {
    let sql = dispatch_sql_for_body(serde_json::json!({
        "selectList": [{"type": "column", "name": "ID", "tableName": "EVENTS"}],
        "selectListDataTypes": [{"type": "decimal", "precision": 20, "scale": 0}],
        "filter": declined_like_on_decimal(),
    }));

    assert!(
        sql.contains(r#""projection":["AMOUNT","ID"]"#),
        "the inner scan must narrow to the select list's and the declined filter's \
             columns, in col_types order — not the full base row: {sql}"
    );
    assert!(
        sql.starts_with(r#"SELECT "LHS_T0"."ID" FROM ("#),
        "the wrapper's outer select list must stay the request's own single item: {sql}"
    );
    let where_at = sql
        .find(r#"AS "LHS_T0" WHERE "#)
        .unwrap_or_else(|| panic!("must route to the qualified wrapper: {sql}"));
    assert!(
        sql[where_at..].contains(r#""LHS_T0"."AMOUNT""#),
        "the declined predicate's column must be projected AND qualified in the \
             wrapper's WHERE: {sql}"
    );
}

/// Scenario: A single-group scalar over an aggregate renders once over the merged partial (#194).
#[test]
fn single_group_scalar_over_aggregate_renders_the_scalar_over_the_merge() {
    let sql = dispatch_sql_for_body(serde_json::json!({
        "selectList": [{
            "type": "function_scalar",
            "name": "ROUND",
            "arguments": [
                agg_item("SUM", Some("AMOUNT"), false),
                {"type": "literal_exactnumeric", "value": 2},
            ],
        }],
        "selectListDataTypes": [{"type": "decimal", "precision": 36, "scale": 2}],
    }));

    assert!(
        sql.starts_with(r#"SELECT CAST(ROUND(SUM("PARTIAL_sum_0"), 2) AS DECIMAL(36,2)) FROM ("#),
        "the merge SELECT must wrap the merged partial in the scalar structure, \
         not emit a bare unwrapped SUM: {sql}"
    );
    assert!(
        sql.contains(r#""aggregates":[{"kind":"sum","column":"AMOUNT""#),
        "the per-shard scan must carry the inner aggregate as a partial plan: {sql}"
    );
}

/// Scenario: A DataFusion render decline leaves Iceberg manifest pruning on the original tree.
#[test]
fn iceberg_pruning_input_unchanged_when_df_render_declines() {
    use crate::adapter::iceberg_predicate::to_iceberg_predicate;
    use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};
    use std::sync::Arc;

    let schema = Schema::builder()
        .with_schema_id(1)
        .with_fields(vec![
            Arc::new(NestedField::required(
                1,
                "id",
                Type::Primitive(PrimitiveType::Long),
            )),
            Arc::new(NestedField::optional(
                2,
                "ts",
                Type::Primitive(PrimitiveType::Timestamp),
            )),
        ])
        .build()
        .unwrap();
    // `SECOND(ts, 3)` is a DataFusion arity decline Exasol renders; `LENGTH(amount) > 5`
    // is rewritten, so the equality assertion distinguishes original from rewritten.
    let filter = serde_json::json!({
        "type": "predicate_and",
        "expressions": [
            {
                "type": "predicate_greater",
                "left": {"type": "column", "name": "id"},
                "right": {"type": "literal_exactnumeric", "value": 5},
            },
            {
                "type": "predicate_greater",
                "left": {"type": "function_scalar", "name": "SECOND", "arguments": [
                    {"type": "column", "name": "ts"},
                    {"type": "literal_exactnumeric", "value": 3},
                ]},
                "right": {"type": "literal_exactnumeric", "value": 1},
            },
            {
                "type": "predicate_greater",
                "left": {"type": "function_scalar", "name": "LENGTH", "arguments": [
                    {"type": "column", "name": "amount"},
                ]},
                "right": {"type": "literal_exactnumeric", "value": 5},
            },
        ],
    });
    let col_types = vec![
        ("ID".to_string(), "DECIMAL(20,0)".to_string()),
        ("TS".to_string(), "TIMESTAMP".to_string()),
        ("AMOUNT".to_string(), "DECIMAL(18,2)".to_string()),
    ];
    assert_ne!(
        apply_type_rewrites(&filter, &col_types).as_ref(),
        Some(&filter),
        "fixture precondition: the type rewrites must CHANGE this tree, so the \
             assertion below can tell the original from the rewritten one"
    );

    let (scan_filter, declined) = classify_where_filter(Some(&filter), &col_types);

    assert!(
        scan_filter.is_none(),
        "the DataFusion render must decline this filter: {scan_filter:?}"
    );
    assert_eq!(
        declined,
        Some(&filter),
        "the declined tree must be the ORIGINAL, un-rewritten filter — the same \
             tree the resolver prunes with"
    );
    let pred = to_iceberg_predicate(&filter, &schema)
        .expect("the prunable conjunct must still yield an Iceberg predicate");
    assert!(
        format!("{pred}").contains("id"),
        "pruning must keep the prunable conjunct even though the sibling conjunct \
             declined: {pred}"
    );
}

/// Scenario: A literal-only select list hides its unprojected sort key in the scan and keeps arity 1 (#225, #189).
#[test]
fn declined_order_by_appends_unprojected_sort_key_as_hidden_column() {
    let request = guard_events_request(serde_json::json!({
        "selectList": [{"type": "literal_exactnumeric", "value": 1}],
        "selectListDataTypes": [{"type": "decimal", "precision": 1, "scale": 0}],
        "orderBy": [{
            "type": "order_by_element",
            "expression": {"type": "column", "name": "NAME"},
            "isAscending": true,
            "nullsLast": true
        }],
        "limit": {"numElements": 10}
    }));
    let proj_cols = vec![ProjectionItem::Expr {
        expr: "1".to_string(),
    }];
    let proj_types = vec!["DECIMAL(1,0)".to_string()];

    let sql = guard_dispatch_sql(&request, proj_cols, proj_types, false, Some(10), Vec::new());

    assert!(
        sql.contains(r#""projection":[{"expr":"1"},"NAME"]"#),
        "sort key NAME must be APPENDED to the derived projection: {sql}"
    );
    assert!(
        sql.contains(r#"EMITS ("_LH_PROJ_0" DECIMAL(1,0), "NAME" VARCHAR(2000000))"#),
        "EMITS must carry the visible expression column plus the hidden sort key: {sql}"
    );
    // The wrapper's list is joined immediately ahead of ` FROM (`, pinning the exact arity.
    assert!(
        sql.contains(r#"SELECT "_LH_PROJ_0" FROM ("#),
        "the wrapper must name ONLY the visible projection item: {sql}"
    );
    assert!(
        sql.contains(r#"ORDER BY "NAME""#),
        "the wrapper's outer ORDER BY must bind the hidden sort key: {sql}"
    );
    assert!(
        !sql.contains("REGION") && !sql.contains("AMOUNT") && !sql.contains("\"ID\""),
        "the projection must NOT widen to the full base row: {sql}"
    );
}

/// Scenario: A bare-column select list ordered by an unprojected column emits it hidden, keeping arity 1 (#225, #189).
#[test]
fn declined_order_by_wrapper_selects_only_original_select_list() {
    let request = guard_events_request(serde_json::json!({
        "selectList": [{"type": "column", "name": "NAME"}],
        "selectListDataTypes": [{"type": "varchar", "size": 2000000}],
        "orderBy": [{
            "type": "order_by_element",
            "expression": {"type": "column", "name": "ID"},
            "isAscending": true,
            "nullsLast": true
        }]
    }));
    let proj_cols = vec![ProjectionItem::Column("NAME".to_string())];
    let proj_types = vec!["VARCHAR(2000000)".to_string()];

    let sql = guard_dispatch_sql(&request, proj_cols, proj_types, false, None, Vec::new());

    assert!(
        sql.contains(r#"SELECT "NAME" FROM ("#),
        "the wrapper must name exactly the one derived projection item: {sql}"
    );
    assert!(
        emits_clause(&sql).contains("\"ID\""),
        "the scan must EMIT the hidden sort key: {}",
        emits_clause(&sql)
    );
    assert!(
        !outer_select_list(&sql).contains("\"ID\""),
        "the hidden sort key must NOT be visible in the outer select list: {}",
        outer_select_list(&sql)
    );
    assert!(
        sql.contains(r#"ORDER BY "ID""#),
        "the wrapper's outer ORDER BY must bind the hidden sort key: {sql}"
    );
    assert!(
        !sql.contains("SELECT *"),
        "the wrapper must never fall back to SELECT * over the wider emitted row: {sql}"
    );
}

/// Scenario: Hidden sort-key columns are appended at most once.
#[test]
fn declined_order_by_dedupes_repeated_and_projected_sort_keys() {
    let sort_key = |name: &str| {
        serde_json::json!({
            "type": "order_by_element",
            "expression": {"type": "column", "name": name},
            "isAscending": true,
            "nullsLast": true
        })
    };
    let request = guard_events_request(serde_json::json!({
        "selectList": [{"type": "column", "name": "NAME"}],
        "selectListDataTypes": [{"type": "varchar", "size": 2000000}],
        "orderBy": [
            sort_key("NAME"),
            sort_key("ID"),
            sort_key("NAME"),
            sort_key("ID"),
        ]
    }));
    let proj_cols = vec![ProjectionItem::Column("NAME".to_string())];
    let proj_types = vec!["VARCHAR(2000000)".to_string()];

    let sql = guard_dispatch_sql(&request, proj_cols, proj_types, false, None, Vec::new());

    assert!(
        sql.contains(r#""projection":["NAME","ID"]"#),
        "the already-projected NAME must not be re-appended and ID must be \
             appended once: {sql}"
    );
    let emits = emits_clause(&sql);
    assert_eq!(
        emits.matches("\"NAME\"").count(),
        1,
        "the already-visible NAME must appear in EMITS exactly once: {emits}"
    );
    assert_eq!(
        emits.matches("\"ID\"").count(),
        1,
        "a column named by two sort keys must be appended exactly once: {emits}"
    );
    assert_eq!(
        outer_select_list(&sql),
        "\"NAME\"",
        "the extension must not change the VISIBLE column count: {sql}"
    );
}

/// Scenario: When every sort key is already projected, the extension is inert and the top-N matches.
#[test]
fn declined_order_by_all_keys_projected_leaves_projection_untouched() {
    let request = guard_events_request(serde_json::json!({
        "selectList": [{"type": "column", "name": "NAME"}],
        "selectListDataTypes": [{"type": "varchar", "size": 2000000}],
        "orderBy": [{
            "type": "order_by_element",
            "expression": {"type": "column", "name": "NAME"},
            "isAscending": true,
            "nullsLast": true
        }],
        "limit": {"numElements": 5}
    }));
    let proj_cols = vec![ProjectionItem::Column("NAME".to_string())];
    let proj_types = vec!["VARCHAR(2000000)".to_string()];
    let logical_schema = vec![LogicalField {
        field_id: Some(2),
        name: "NAME".to_string(),
        arrow_type: "utf8".to_string(),
        nullable: true,
        initial_default: None,
        nested: None,
        physical_name: None,
    }];

    let sql = guard_dispatch_sql(
        &request,
        proj_cols,
        proj_types,
        false,
        Some(5),
        logical_schema,
    );

    assert!(
        sql.contains(r#""projection":["NAME"]"#),
        "an already-projected sort key must leave the projection untouched: {sql}"
    );
    assert!(
        !sql.contains("REGION") && !sql.contains("AMOUNT") && !sql.contains("\"ID\""),
        "nothing must be appended or widened when the sort key is projected: {sql}"
    );
    assert!(
        sql.contains(r#"ORDER BY "NAME""#) && sql.contains("LIMIT 5"),
        "a matched top-N must still form (sort key projected, native type): {sql}"
    );
    assert!(
        sql.starts_with("SELECT LAKEHOUSE_SCAN(") && !sql.contains(" FROM ("),
        "a matched top-N must not be wrapped in an outer SELECT … FROM (: {sql}"
    );
    let common = common_arg_literal(&sql);
    assert!(
        common.contains(r#""order_by":[{"column":"NAME","ascending":true,"nulls_last":true}]"#)
            && common.contains(r#""limit":5"#),
        "a matched top-N must carry the per-shard sort keys and limit: {common}"
    );
}

/// Scenario: A non-zero offset renders only on the declined wrapper, never ahead of it (#191).
#[test]
fn nonzero_offset_nulls_the_effective_limit() {
    let request = guard_events_request(serde_json::json!({
        "selectList": [{"type": "column", "name": "NAME"}],
        "selectListDataTypes": [{"type": "varchar", "size": 2000000}],
        "orderBy": [{
            "type": "order_by_element",
            "expression": {"type": "column", "name": "NAME"},
            "isAscending": true,
            "nullsLast": true
        }],
        "limit": {"numElements": 5, "offset": 2}
    }));
    let proj_cols = vec![ProjectionItem::Column("NAME".to_string())];
    let proj_types = vec!["VARCHAR(2000000)".to_string()];
    let logical_schema = vec![LogicalField {
        field_id: Some(2),
        name: "NAME".to_string(),
        arrow_type: "utf8".to_string(),
        nullable: true,
        initial_default: None,
        nested: None,
        physical_name: None,
    }];

    let sql = guard_dispatch_sql(
        &request,
        proj_cols,
        proj_types,
        false,
        Some(5),
        logical_schema,
    );

    assert_eq!(
        sql.matches("LIMIT").count(),
        1,
        "effective_limit must be nulled: no LIMIT may reach the fan-out ahead of \
             the declined wrapper's own window: {sql}"
    );
    assert!(
        sql.contains(r#"ORDER BY "NAME" ASC NULLS LAST LIMIT 5 OFFSET 2"#),
        "the declined wrapper must render the offset beside its own ORDER BY: {sql}"
    );
    let common = common_arg_literal(&sql);
    assert!(
        !common.contains("\"limit\"") && !common.contains("\"order_by\""),
        "the per-shard common blob must carry neither bound once effective_limit \
             is nulled: {common}"
    );
}

/// Scenario: The projection extension runs after `detect_topn`, so a matchable top-N still declines.
#[test]
fn declined_order_by_extension_runs_after_topn_detection() {
    let request = guard_events_request(serde_json::json!({
        "selectList": [{"type": "literal_exactnumeric", "value": 1}],
        "selectListDataTypes": [{"type": "decimal", "precision": 1, "scale": 0}],
        "orderBy": [{
            "type": "order_by_element",
            "expression": {"type": "column", "name": "NAME"},
            "isAscending": true,
            "nullsLast": true
        }],
        "limit": {"numElements": 5}
    }));
    let proj_cols = vec![ProjectionItem::Expr {
        expr: "1".to_string(),
    }];
    let proj_types = vec!["DECIMAL(1,0)".to_string()];
    // A non-JSON-fallback type, so that guard cannot be what declines the top-N.
    let logical_schema = vec![LogicalField {
        field_id: Some(2),
        name: "NAME".to_string(),
        arrow_type: "utf8".to_string(),
        nullable: true,
        initial_default: None,
        nested: None,
        physical_name: None,
    }];

    let sql = guard_dispatch_sql(
        &request,
        proj_cols,
        proj_types,
        false,
        Some(5),
        logical_schema,
    );

    let common = common_arg_literal(&sql);
    assert!(
        !common.contains("\"limit\""),
        "the top-N must have DECLINED, so no per-shard limit may reach the common \
             blob — the extension ran before detect_topn: {common}"
    );
    assert!(
        !common.contains("order_by"),
        "the top-N must have DECLINED, so no per-shard sort keys may reach the \
             common blob — the extension ran before detect_topn: {common}"
    );
    assert!(
        sql.contains(r#"SELECT "_LH_PROJ_0" FROM ("#),
        "the declined path must render the hidden-column wrapper; a matched top-N \
             renders none — the extension ran before detect_topn: {sql}"
    );
}

/// Scenario: An ORDER BY the adapter cannot bound as a top-N remains correctness-safe (#198).
#[test]
fn declined_order_by_renders_every_reachable_ordering_or_declines() {
    let unrenderable_expression = serde_json::json!({
        "type": "order_by_element",
        "expression": {"type": "no_such_node_type_in_either_dialect"},
        "isAscending": true,
        "nullsLast": true
    });
    let column_missing_nulls_last = serde_json::json!({
        "type": "order_by_element",
        "expression": {"type": "column", "name": "ID"},
        "isAscending": true
    });
    let renderable_expression = serde_json::json!({
        "type": "order_by_element",
        "expression": {
            "type": "function_scalar",
            "name": "ABS",
            "arguments": [{"type": "column", "name": "AMOUNT", "tableName": "EVENTS"}]
        },
        "isAscending": false,
        "nullsLast": true
    });
    let declining_shapes = [
        (
            "every element unrenderable",
            serde_json::json!([unrenderable_expression, column_missing_nulls_last]),
        ),
        (
            "renderable key first, unrenderable second",
            serde_json::json!([renderable_expression, unrenderable_expression]),
        ),
        (
            "unrenderable key first, renderable second",
            serde_json::json!([unrenderable_expression, renderable_expression]),
        ),
    ];

    for (facet, order_by) in declining_shapes {
        let request = guard_events_request(serde_json::json!({
            "selectList": [{"type": "column", "name": "NAME"}],
            "selectListDataTypes": [{"type": "varchar", "size": 2000000}],
            "orderBy": order_by,
            "limit": {"numElements": 7}
        }));
        let err = guard_dispatch_result(
            &request,
            vec![ProjectionItem::Column("NAME".to_string())],
            vec!["VARCHAR(2000000)".to_string()],
            false,
            Some(7),
            Vec::new(),
        )
        .expect_err(&format!(
            "{facet}: a pushed ordering the adapter cannot reproduce in full must \
                 decline, never return SQL"
        ));
        match err {
            UdfError::User(msg) => {
                assert!(
                    msg.contains("ORDER BY") && msg.contains("declined"),
                    "{facet}: the decline must name the unrenderable ORDER BY key: {msg}"
                );
                assert!(
                    msg.contains("not a native re-plan"),
                    "{facet}: the decline is a HARD error — Exasol does not re-plan \
                         natively, so the message must not imply a retry: {msg}"
                );
            }
            other => panic!("{facet}: must be a User decline, got {other:?}"),
        }
    }

    let unordered = guard_events_request(serde_json::json!({
        "selectList": [{"type": "column", "name": "NAME"}],
        "selectListDataTypes": [{"type": "varchar", "size": 2000000}],
        "limit": {"numElements": 7}
    }));
    let sql = guard_dispatch_sql(
        &unordered,
        vec![ProjectionItem::Column("NAME".to_string())],
        vec!["VARCHAR(2000000)".to_string()],
        false,
        Some(7),
        Vec::new(),
    );
    assert!(
        !sql.contains("ORDER BY"),
        "an absent orderBy must emit no ORDER BY at all: {sql}"
    );
    assert!(
        sql.starts_with("SELECT LAKEHOUSE_SCAN(") && !sql.contains(" FROM ("),
        "an absent orderBy must leave the fan-out unwrapped: {sql}"
    );
    assert!(
        sql.contains("LIMIT 7"),
        "an absent orderBy must not withhold the request LIMIT: {sql}"
    );
}

/// Scenario: A lone COUNT(DISTINCT) with both orderBy and LIMIT renders the LIMIT on the wrapper (#191).
#[test]
fn lone_count_distinct_with_order_by_still_renders_limit() {
    let request = guard_events_request(serde_json::json!({
        "selectList": [agg_item("COUNT", Some("ID"), true)],
        "selectListDataTypes": [{"type": "decimal", "precision": 18, "scale": 0}],
        "orderBy": [{
            "type": "order_by_element",
            "expression": {"type": "column", "name": "ID", "tableName": "EVENTS"},
            "isAscending": true,
            "nullsLast": true
        }]
    }));

    let sql = guard_dispatch_sql(
        &request,
        Vec::new(),
        Vec::new(),
        false,
        Some(10),
        Vec::new(),
    );

    assert!(
        sql.trim_end().ends_with("LIMIT 10"),
        "the wrapper must render the request's raw limit even though an \
             orderBy is present: {sql}"
    );
    assert!(
        !sql.contains("OFFSET"),
        "no offset can ever reach this wrapper (fact 6 — Exasol rejects OFFSET \
             on an ungrouped aggregated select before the adapter is consulted): {sql}"
    );
    assert_eq!(
        sql.matches("LIMIT").count(),
        1,
        "the LIMIT must land on the outer wrapper only, never leak into the \
             per-shard distinct fan-out sub-scan: {sql}"
    );
    assert!(
        !sql.contains("ORDER BY"),
        "the per-shard fan-out stays sort-free: no per-shard scan spec ever \
             carries an ORDER BY on this path: {sql}"
    );
}

/// Select-list arity equals the base table's column count.
fn widening_arity_coincidence_request() -> Json {
    guard_events_request(serde_json::json!({
        "selectList": [
            {"type": "column", "name": "REGION", "tableName": "EVENTS"},
            {"type": "column", "name": "NAME", "tableName": "EVENTS"},
            {"type": "column", "name": "AMOUNT", "tableName": "EVENTS"},
            {"type": "column", "name": "ID", "tableName": "EVENTS"},
        ],
        "selectListDataTypes": [
            {"type": "varchar", "size": 2000000},
            {"type": "varchar", "size": 2000000},
            {"type": "decimal", "precision": 18, "scale": 2},
            {"type": "decimal", "precision": 20, "scale": 0},
        ],
        "orderBy": [{
            "type": "order_by_element",
            "expression": {"type": "column", "name": "NAME", "tableName": "EVENTS"},
            "isAscending": true,
            "nullsLast": true
        }]
    }))
}

fn widening_arity_coincidence_projection() -> (Vec<ProjectionItem>, Vec<String>) {
    let cols = guard_col_types()
        .into_iter()
        .map(|(name, ty)| (ProjectionItem::Column(name), ty))
        .collect::<Vec<_>>();
    (
        cols.iter().map(|(c, _)| c.clone()).collect(),
        cols.iter().map(|(_, t)| t.clone()).collect(),
    )
}

/// Scenario: A widened projection routes to the wrapper even when its column count matches the select list (#196, #234).
#[test]
fn dispatch_widened_projection_at_matching_arity_routes_to_wrapper() {
    let request = widening_arity_coincidence_request();
    let (proj_cols, proj_types) = widening_arity_coincidence_projection();
    assert_eq!(
        proj_cols.len(),
        request["pushdownRequest"]["selectList"]
            .as_array()
            .expect("fixture select list")
            .len(),
        "the fixture must hold the arity coincidence the count comparison missed"
    );

    let sql = guard_dispatch_sql(&request, proj_cols, proj_types, true, None, Vec::new());

    assert!(
        sql.contains(r#"AS "LHS_T0""#),
        "a widened projection must route to the qualified single-table wrapper: {sql}"
    );
    assert!(
        sql.contains(
            r#"SELECT "LHS_T0"."REGION", "LHS_T0"."NAME", "LHS_T0"."AMOUNT", "LHS_T0"."ID" FROM ("#
        ),
        "the wrapper must render the ORIGINAL select list, qualified, so Exasol's \
             positional validation sees its own items: {sql}"
    );
    assert!(
        sql.contains(r#"ORDER BY "LHS_T0"."NAME""#),
        "the wrapper's own outer ORDER BY must order the result: {sql}"
    );
}

/// Scenario: The same projection with the widening flag clear stays on the scan path.
#[test]
fn dispatch_non_widened_projection_at_matching_arity_takes_scan_path() {
    let request = widening_arity_coincidence_request();
    let (proj_cols, proj_types) = widening_arity_coincidence_projection();

    let sql = guard_dispatch_sql(&request, proj_cols, proj_types, false, None, Vec::new());

    assert!(
        !sql.contains("LHS_T0"),
        "a per-select-list-item projection must NOT be routed to the qualified \
             single-table wrapper: {sql}"
    );
    assert!(
        sql.contains(&format!("{SCAN_UDF_NAME}(")),
        "the ordinary scan path must still drive the sharded scan UDF: {sql}"
    );
    assert!(
        sql.contains(r#"SELECT "REGION", "NAME", "AMOUNT", "ID" FROM ("#),
        "the scan path must emit the derived projection unqualified: {sql}"
    );
}

fn timestamp_cast_select_request(precision: u64) -> Json {
    serde_json::json!({
        "selectList": [{
            "type": "function_scalar_cast",
            "name": "CAST",
            "dataType": {"type": "TIMESTAMP", "fractionalSecondsPrecision": precision},
            "arguments": [{"type": "column", "name": "NAME", "tableName": "EVENTS"}]
        }],
        "selectListDataTypes": [
            {"type": "timestamp", "fractionalSecondsPrecision": precision}
        ],
    })
}

/// Scenario: A CAST to `TIMESTAMP(2)`, which DataFusion cannot parse, routes to the wrapper (#405).
#[test]
fn declined_timestamp_precision_cast_routes_to_qualified_wrapper() {
    let sql = dispatch_sql_for_body(timestamp_cast_select_request(2));

    assert!(
        sql.starts_with(r#"SELECT CAST("LHS_T0"."NAME" AS TIMESTAMP(2)) FROM ("#),
        "the declined CAST must be computed in the wrapper's outer select list: {sql}"
    );
    assert!(
        sql.contains(r#"AS "LHS_T0""#),
        "a declined CAST target must route to the qualified single-table wrapper: {sql}"
    );
    let emits = sql
        .find("EMITS (")
        .map(|at| &sql[at..])
        .unwrap_or_else(|| panic!("the wrapper must still drive the scan UDF: {sql}"));
    assert!(
        emits.contains(r#""NAME" VARCHAR(2000000)"#) && !emits.contains("TIMESTAMP"),
        "the scan must emit the raw column, never the declined CAST target: {sql}"
    );
}

/// Scenario: A CAST to `TIMESTAMP(6)`, which DataFusion parses, stays on the scan path.
#[test]
fn accepted_timestamp_precision_cast_takes_scan_path() {
    let sql = dispatch_sql_for_body(timestamp_cast_select_request(6));

    assert!(
        !sql.contains("LHS_T0"),
        "an accepted CAST target must NOT route to the qualified wrapper: {sql}"
    );
    assert!(
        sql.contains("TIMESTAMP(6)"),
        "the accepted CAST must reach the scan at its declared precision: {sql}"
    );
}

/// Scenario: A malformed identifier fails validation before the OAuth2 grant touches the network.
#[tokio::test]

async fn malformed_table_ident_fails_before_any_catalog_contact() {
    let creds = ConnectionCreds {
        warehouse: "warehouse".into(),
        endpoint: "http://minio:9000".into(),
        region: "us-east-1".into(),
        access_key: "minioadmin".into(),
        secret_key: "minioadmin".into(),
        session_token: None,
        path_style: Some(true),
        use_sigv4: false,
        use_vended_credentials: false,
        token: None,
        client_id: Some("oauth-client-id-sentinel".into()),
        client_secret: Some("oauth-client-secret-sentinel".into()),
        oauth2_server_uri: None,
        scope: None,
        account_name: None,
        account_key: None,
        sas_token: None,
    };

    let catalog = CatalogProps {
        warehouse: "warehouse".into(),
        table: "malformed_identifier_with_no_namespace_separator".into(),
    };

    // An empty `columns` array would fail in `project_columns` first, masking the ordering.
    let request = nq4_request();

    let conn = ResolvedConnectionConfig {
        catalog_uri: "http://127.0.0.1:1".to_string(),
        storage: sample_storage(),
        creds,
        allow_http: false,
        catalog_kind: CatalogKind::IcebergRest,
        connection_name: TEST_CONNECTION_NAME.to_string(),
        sealed_storage_key: Some(test_sealing_key()),
    };
    let result = handle_pushdown(
        &request, &conn, &catalog, None, 1, 1, 1, 1024, 1, 0.6, 200, 4, 1024,
    )
    .await;

    let err = result.expect_err("a malformed table identifier must fail");
    let message = err.to_string();
    assert!(
        message.contains("namespace.table"),
        "error must be parse_table_ident's own error, got: {message}"
    );
    assert!(
        !message.contains("OAuth2"),
        "error must not be the OAuth2 token request/transport error \
             (would mean the session was built before the identifier was \
             validated): {message}"
    );
}

async fn seam_handle_pushdown(
    request: &Json,
    catalog_uri: &str,
    catalog: &CatalogProps,
    catalog_kind: CatalogKind,
    creds: &ConnectionCreds,
) -> Result<Json, UdfError> {
    let conn = ResolvedConnectionConfig {
        catalog_uri: catalog_uri.to_string(),
        storage: sample_storage(),
        creds: creds.clone(),
        allow_http: true,
        catalog_kind,
        connection_name: TEST_CONNECTION_NAME.to_string(),
        sealed_storage_key: Some(test_sealing_key()),
    };
    handle_pushdown(
        request, &conn, catalog, None, 1, 1, 1, 1024, 1, 0.6, 200, 4, 1024,
    )
    .await
}

/// Scenario: Every pushdown request shape resolves through the one format-reader seam.
#[tokio::test]
async fn every_request_shape_resolves_through_the_format_reader_seam() {
    let iceberg = iceberg_catalog().await;
    let creds = unauthenticated_creds();
    let iceberg_catalog_props = CatalogProps {
        warehouse: "wh".into(),
        table: "db.t".into(),
    };
    let result = seam_handle_pushdown(
        &nq4_request(),
        &iceberg.uri,
        &iceberg_catalog_props,
        CatalogKind::IcebergRest,
        &creds,
    )
    .await
    .expect("a snapshotless Iceberg table resolves an empty pushdown result");
    assert_eq!(result["type"], "pushdown");
    assert_eq!(
        iceberg.targets(),
        vec![
            ICEBERG_CONFIG_TARGET.to_string(),
            ICEBERG_LOAD_TABLE_TARGET.to_string()
        ],
        "the Iceberg reader must be reached through the resolver"
    );

    let unity = RecordingCatalog::spawn(|target| {
        if target == UNITY_TABLE_TARGET {
            (200, locationless_delta_table_body())
        } else {
            (404, r#"{"message":"no such table"}"#.to_string())
        }
    })
    .await;
    let unity_catalog_props = CatalogProps {
        warehouse: "wh".into(),
        table: "cat.sch.orders".into(),
    };
    let err = seam_handle_pushdown(
        &nq4_request(),
        &unity.uri,
        &unity_catalog_props,
        CatalogKind::UnityCatalogNative,
        &creds,
    )
    .await
    .expect_err("a Delta table carrying no storage location cannot be planned");
    assert_eq!(
        unity.targets(),
        vec![UNITY_TABLE_TARGET.to_string()],
        "the Delta reader must be reached through the SAME resolver, not a routed-around path"
    );
    assert!(
        err.to_string().contains("cat.sch.orders"),
        "the Delta reader's own plan-time error must name the table: {err}"
    );
}

/// Scenario: One catalog session per request serves every table the request resolves.
#[tokio::test]
async fn a_two_leg_join_resolves_both_legs_on_one_catalog_session() {
    let iceberg = iceberg_catalog().await;
    let creds = unauthenticated_creds();
    let catalog_props = CatalogProps {
        warehouse: "wh".into(),
        table: "unused-on-the-join-path".into(),
    };

    let request = serde_json::json!({
        "involvedTables": [
            {
                "name": "CUSTOMER",
                "columns": [
                    {"name": "C_CUSTKEY", "dataType": {"type": "decimal", "precision": 20, "scale": 0}},
                ],
            },
            {
                "name": "ORDERS",
                "columns": [
                    {"name": "O_CUSTKEY", "dataType": {"type": "decimal", "precision": 20, "scale": 0}},
                ],
            },
        ],
        "pushdownRequest": {
            "type": "select",
            "from": {
                "type": "join",
                "join_type": "inner",
                "left": {"name": "CUSTOMER", "type": "table"},
                "right": {"name": "ORDERS", "type": "table"},
                "condition": {
                    "type": "predicate_equal",
                    "left": {"type": "column", "name": "C_CUSTKEY", "tableName": "CUSTOMER"},
                    "right": {"type": "column", "name": "O_CUSTKEY", "tableName": "ORDERS"},
                },
            },
            "selectList": [
                {"type": "column", "name": "C_CUSTKEY", "tableName": "CUSTOMER"},
                {"type": "column", "name": "O_CUSTKEY", "tableName": "ORDERS"},
            ],
        },
        "schemaMetadataInfo": {
            "properties": {},
            "adapterNotes": serde_json::json!({
                "TABLE_MAP": {"CUSTOMER": "db.customer", "ORDERS": "db.orders"}
            }).to_string(),
        },
    });

    seam_handle_pushdown(
        &request,
        &iceberg.uri,
        &catalog_props,
        CatalogKind::IcebergRest,
        &creds,
    )
    .await
    .expect("a two-leg join over snapshotless Iceberg tables resolves");

    assert_eq!(
        iceberg.targets(),
        vec![
            ICEBERG_CONFIG_TARGET.to_string(),
            "/v1/namespaces/db/tables/customer".to_string(),
            "/v1/namespaces/db/tables/orders".to_string(),
        ],
        "one /v1/config for the whole request, then exactly one loadTable per leg"
    );
}

/// Scenario: A Unity Catalog table's identity survives the round trip from the involved table.
#[tokio::test]
async fn a_malformed_unity_identifier_is_refused_in_unity_terms_before_any_catalog_contact() {
    let unity = RecordingCatalog::spawn(|_| (200, locationless_delta_table_body())).await;
    let creds = unauthenticated_creds();
    let catalog = CatalogProps {
        warehouse: "wh".into(),
        table: "orders".into(),
    };

    let err = seam_handle_pushdown(
        &nq4_request(),
        &unity.uri,
        &catalog,
        CatalogKind::UnityCatalogNative,
        &creds,
    )
    .await
    .expect_err("an identifier that addresses no Unity Catalog table must be refused");

    let message = err.to_string();
    assert!(
        message.contains("catalog.schema.table"),
        "the refusal must state the Unity Catalog address form: {message}"
    );
    assert!(
        !message.contains("namespace.table"),
        "Iceberg's identifier rule must not judge a Unity Catalog identifier: {message}"
    );
    assert!(
        unity.targets().is_empty(),
        "a malformed identifier must cost no catalog request: {:?}",
        unity.targets()
    );
}

fn refused_column_table_request(select_list: Json) -> Json {
    refused_column_table(serde_json::json!({"type": "select", "selectList": select_list}))
}

fn refused_column_table_request_with_filter(select_list: Json, filter: Json) -> Json {
    refused_column_table(serde_json::json!({
        "type": "select", "selectList": select_list, "filter": filter,
    }))
}

fn refused_column_table_select_star_request() -> Json {
    refused_column_table(serde_json::json!({"type": "select"}))
}

fn refused_column_table(pushdown_request: Json) -> Json {
    serde_json::json!({
        "involvedTables": [{
            "name": "ORDERS",
            "columns": [
                {"name": "INT_COL", "dataType": {"type": "decimal", "precision": 10, "scale": 0}},
                {"name": "BINARY_COL", "dataType": {"type": "varchar", "size": 2000000}},
            ],
        }],
        "pushdownRequest": pushdown_request,
    })
}

async fn refused_column_table_storage() -> crate::scan::spec::StorageBackend {
    delta_object_endpoint(vec![(
        delta_commit_zero_key("orders"),
        fileless_delta_commit(
            "orders",
            &[("int_col", "integer"), ("binary_col", "binary")],
        ),
    )])
    .await
}

fn column_item(name: &str) -> Json {
    serde_json::json!({"type": "column", "name": name, "tableName": "ORDERS"})
}

/// Scenario: A refused column refuses only the requests that read or emit it
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_refused_column_is_refused_before_the_zero_active_files_early_return() {
    let catalog = unity_delta_catalog().await;
    let storage = refused_column_table_storage().await;

    let planned = delta_pushdown(
        &refused_column_table_request(serde_json::json!([column_item("INT_COL")])),
        &catalog.uri,
        storage.clone(),
        "cat.sch.orders",
    )
    .await
    .expect("a request naming only the mappable column must plan");
    let sql = planned["sql"]
        .as_str()
        .expect("pushdown response must carry a sql field");
    assert!(
        !sql.contains(SCAN_UDF_NAME),
        "the table must resolve with NO active file, so the empty-result early return \
         answers this request — otherwise the ordering below proves nothing: {sql}"
    );

    let error = delta_pushdown(
        &refused_column_table_request(serde_json::json!([column_item("BINARY_COL")])),
        &catalog.uri,
        storage,
        "cat.sch.orders",
    )
    .await
    .expect_err("a request emitting the refused column must be refused, never answered empty");

    let message = match error {
        UdfError::User(message) => message,
        other => panic!("every refusal must be a user error, got {other:?}"),
    };
    assert!(
        message.contains("binary_col") && message.contains("#351"),
        "the refusal must be the gate's own message, naming the column and its reason: \
         {message}"
    );
}

/// Scenario: A refused column refuses only the requests that read or emit it
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_refused_delta_column_refuses_only_the_requests_that_reference_it() {
    let catalog = unity_delta_catalog().await;
    let storage = refused_column_table_storage().await;

    delta_pushdown(
        &refused_column_table_request(serde_json::json!([column_item("INT_COL")])),
        &catalog.uri,
        storage.clone(),
        "cat.sch.orders",
    )
    .await
    .expect("a projection naming only the mappable column must plan");

    let projection_error = delta_pushdown(
        &refused_column_table_request(serde_json::json!([column_item("BINARY_COL")])),
        &catalog.uri,
        storage.clone(),
        "cat.sch.orders",
    )
    .await
    .expect_err("a projection naming the refused column must be refused");
    assert_refuses_binary_col(projection_error);

    let where_error = delta_pushdown(
        &refused_column_table_request_with_filter(
            serde_json::json!([column_item("INT_COL")]),
            serde_json::json!({
                "type": "predicate_equal",
                "left": column_item("BINARY_COL"),
                "right": {"type": "literal_string", "value": "x"},
            }),
        ),
        &catalog.uri,
        storage.clone(),
        "cat.sch.orders",
    )
    .await
    .expect_err(
        "a WHERE filter referencing the refused column must be refused even though \
         the select list names only the mappable column",
    );
    assert_refuses_binary_col(where_error);

    let select_star_error = delta_pushdown(
        &refused_column_table_select_star_request(),
        &catalog.uri,
        storage,
        "cat.sch.orders",
    )
    .await
    .expect_err(
        "SELECT * widens to the full base row, so a refused column anywhere in the \
         table must refuse it too",
    );
    assert_refuses_binary_col(select_star_error);
}

fn assert_refuses_binary_col(error: UdfError) {
    let message = match error {
        UdfError::User(message) => message,
        other => panic!("every refusal must be a user error, got {other:?}"),
    };
    assert!(
        message.contains("binary_col") && message.contains("#351"),
        "the refusal must be the gate's own message, naming the column and its reason: \
         {message}"
    );
}

async fn refused_protocol_table_storage() -> crate::scan::spec::StorageBackend {
    delta_object_endpoint(vec![(
        delta_commit_zero_key("orders"),
        fileless_delta_commit_with_protocol(
            "orders",
            &[("int_col", "integer")],
            serde_json::json!({
                "minReaderVersion": 3,
                "minWriterVersion": 7,
                "readerFeatures": ["variantType"],
                "writerFeatures": ["variantType"],
            }),
        ),
    )])
    .await
}

/// Scenario: The Delta reader is reached from production pushdown under the Unity Catalog kind
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_unity_catalog_pushdown_gates_the_delta_protocol_and_refuses_per_column() {
    let catalog = unity_delta_catalog().await;
    let storage = refused_protocol_table_storage().await;

    let error = delta_pushdown(
        &refused_column_table_request(serde_json::json!([column_item("INT_COL")])),
        &catalog.uri,
        storage,
        "cat.sch.orders",
    )
    .await
    .expect_err("a reader feature outside the allow-list must refuse before any column gate");

    let message = match error {
        UdfError::User(message) => message,
        other => panic!("every refusal must be a user error, got {other:?}"),
    };
    assert!(
        message.contains("variantType"),
        "the refusal must be the protocol gate's own message, naming the unsupported feature: \
         {message}"
    );
}

/// Scenario: Enabling the kernel's skipping surfaces no statistic to the engine or the wire
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_non_pruning_delta_request_keeps_its_pre_change_field_set_and_carries_no_statistic() {
    let catalog = unity_delta_catalog().await;
    let add = serde_json::json!({"add": {
        "path": "part-0.parquet",
        "partitionValues": {},
        "size": 100,
        "modificationTime": 1,
        "dataChange": true,
    }});
    let storage = delta_object_endpoint(vec![(
        delta_commit_zero_key("orders"),
        format!(
            "{}{add}\n",
            fileless_delta_commit("orders", &[("int_col", "integer")])
        ),
    )])
    .await;

    let request = serde_json::json!({
        "involvedTables": [{
            "name": "ORDERS",
            "columns": [
                {"name": "INT_COL", "dataType": {"type": "decimal", "precision": 10, "scale": 0}},
            ],
        }],
        "pushdownRequest": {
            "type": "select",
            "selectList": [{"type": "column", "name": "INT_COL", "tableName": "ORDERS"}],
        },
    });

    let planned = delta_pushdown(&request, &catalog.uri, storage, "cat.sch.orders")
        .await
        .expect("a one-file table with no WHERE clause must drive the scan UDF");

    let sql = planned["sql"]
        .as_str()
        .expect("pushdown response must carry a sql field");
    assert!(
        sql.contains(SCAN_UDF_NAME),
        "one active file with no pruning filter must reach the scan path, not the \
         empty-result early return: {sql}"
    );

    let common = common_arg_literal(sql);
    let parsed: serde_json::Value =
        serde_json::from_str(common).expect("common blob must be valid JSON");
    let object = parsed
        .as_object()
        .expect("common blob must be a JSON object");
    let mut keys: Vec<&str> = object.keys().map(String::as_str).collect();
    keys.sort_unstable();
    assert_eq!(
        keys,
        vec![
            "df_batch_size",
            "df_target_partitions",
            "df_threads_per_udf",
            "instance_overhead_mb",
            "logical_schema",
            "memory_pool_fraction",
            "projection",
            "s3_max_connections",
            "storage",
            "table_root",
        ],
        "a non-pruning Delta request's common blob must keep exactly this plan's \
         pre-change field set: {common}"
    );

    let back = CommonScanSpec::from_json(common)
        .expect("the common blob must round-trip through CommonScanSpec");
    let expected = CommonScanSpec {
        table_root: "s3://bucket/orders".to_string(),
        projection: vec![ProjectionItem::Column("INT_COL".to_string())],
        logical_schema: vec![LogicalField {
            field_id: None,
            name: "int_col".to_string(),
            arrow_type: "int32".to_string(),
            nullable: true,
            initial_default: None,
            nested: None,
            physical_name: None,
        }],
        storage: back.storage.clone(),
        df_target_partitions: 1,
        df_batch_size: 1024,
        df_threads_per_udf: 1,
        memory_pool_fraction: 0.6,
        instance_overhead_mb: 200,
        s3_max_connections: 4,
        ..Default::default()
    };
    assert_eq!(
        back, expected,
        "a non-pruning Delta request's parsed CommonScanSpec must match its \
         pre-change shape field for field"
    );
}

/// Scenario: A refused column refuses only the requests that read or emit it
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_count_star_aggregate_is_admitted_over_a_table_with_a_refused_column() {
    let catalog = unity_delta_catalog().await;
    let storage = refused_column_table_storage().await;

    delta_pushdown(
        &refused_column_table_request(serde_json::json!([
            {"type": "function_aggregate", "name": "COUNT", "arguments": [], "distinct": false}
        ])),
        &catalog.uri,
        storage,
        "cat.sch.orders",
    )
    .await
    .expect(
        "COUNT(*) reads no column value, so it must not be refused by a column \
         this table cannot render",
    );
}

/// Scenario: A refused column refuses only the requests that read or emit it
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_aggregate_over_a_refused_column_is_refused() {
    let catalog = unity_delta_catalog().await;
    let storage = refused_column_table_storage().await;

    let error = delta_pushdown(
        &refused_column_table_request(serde_json::json!([{
            "type": "function_aggregate",
            "name": "MAX",
            "arguments": [column_item("BINARY_COL")],
            "distinct": false,
        }])),
        &catalog.uri,
        storage,
        "cat.sch.orders",
    )
    .await
    .expect_err("an aggregate reading the refused column must be refused");

    assert_refuses_binary_col(error);
}

/// Scenario: A refused column refuses only the requests that read or emit it
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_count_star_filtered_on_a_refused_column_is_refused() {
    let catalog = unity_delta_catalog().await;
    let storage = refused_column_table_storage().await;

    let error = delta_pushdown(
        &refused_column_table_request_with_filter(
            serde_json::json!([
                {"type": "function_aggregate", "name": "COUNT", "arguments": [], "distinct": false},
            ]),
            serde_json::json!({
                "type": "predicate_is_not_null",
                "expression": column_item("BINARY_COL"),
            }),
        ),
        &catalog.uri,
        storage,
        "cat.sch.orders",
    )
    .await
    .expect_err("a COUNT(*) filtered on the refused column reads it, so it must be refused");

    assert_refuses_binary_col(error);
}

/// Scenario: Resolved partition columns reach the scan spec for every side.
#[test]
fn resolved_partition_columns_reach_the_common_spec_and_the_join_spec() {
    let request = guard_events_request(serde_json::json!({"type": "select"}));
    let pushdown_req = pd(&request);
    let (proj_cols, proj_types, widened) =
        extract_projection(&request, &pushdown_req).expect("the EVENTS fixture must project");
    let result = build_dispatch_sql(
        &request,
        &pushdown_req,
        proj_cols,
        proj_types,
        widened,
        guard_col_types(),
        None,
        None,
        None,
        false,
        &[vec![FileEntry::new("data/part-0.parquet", 1_000)]],
        "s3://warehouse/db/events".to_string(),
        Vec::new(),
        Vec::new(),
        vec!["LETTER".to_string()],
        &sample_scan_storage(),
        SCAN_UDF_NAME,
        DISTRIBUTE_FILES_UDF_NAME,
        4,
        8192,
        2,
        0.6,
        200,
        8,
    )
    .expect("build_dispatch_sql must succeed for the EVENTS fixture");
    let sql = result["sql"]
        .as_str()
        .expect("pushdown response must carry a sql field");
    let common = common_arg_literal(sql);
    assert!(
        common.contains(r#""partition_columns":["LETTER"]"#),
        "the resolved partition columns must reach CommonScanSpec: {common}"
    );

    let fact = ResolvedJoinSide {
        table_name: "ORDERS".to_string(),
        table_identifier: "cat.sch.orders".to_string(),
        table_root: "s3://warehouse/lh/orders".to_string(),
        files: vec![FileEntry::new("s3://w/o-0.parquet", 100)],
        logical_schema: Vec::new(),
        name_mapping: Vec::new(),
        effective_storage: sample_storage(),
        partition_columns: Vec::new(),
        total_bytes: 100,
        refused_columns: Vec::new(),
    };
    let dimension = ResolvedJoinSide {
        table_name: "CUSTOMER".to_string(),
        table_identifier: "cat.sch.customer".to_string(),
        table_root: "s3://warehouse/lh/customer".to_string(),
        files: vec![FileEntry::new("s3://w/c-0.parquet", 10)],
        logical_schema: Vec::new(),
        name_mapping: Vec::new(),
        effective_storage: sample_storage(),
        partition_columns: vec!["REGION".to_string()],
        total_bytes: 10,
        refused_columns: Vec::new(),
    };
    let sides = JoinSides {
        fact,
        dimension,
        broadcast_eligible: true,
    };
    let rendered = super::joins::RenderedJoinPushdown {
        condition: "\"ORDERS\".\"CUSTOMER_KEY\" = \"CUSTOMER\".\"CUSTOMER_KEY\"".to_string(),
        filter: None,
        projection: vec![ProjectionItem::Column("CUSTOMER_KEY".to_string())],
        projection_types: vec!["DECIMAL(20,0)".to_string()],
    };
    let tuning = super::joins::JoinScanRequestConfig {
        cluster_nodes: 1,
        parallelism_factor: 1,
        df_target_partitions: 1,
        df_batch_size: 8192,
        df_threads_per_udf: 1,
        memory_pool_fraction: 0.6,
        instance_overhead_mb: 200,
        s3_max_connections: 8,
        connection: &super::test_support::TEST_CONNECTION,
    };
    let join_sql = super::joins::build_broadcast_join_sql(
        &sides,
        &rendered,
        super::joins::JoinWindowPlan::Unbounded,
        &tuning,
        SCAN_UDF_NAME,
        DISTRIBUTE_FILES_UDF_NAME,
    )
    .expect("selecting the wire storage must succeed")
    .expect("a broadcast-eligible plan with an unbounded window must render");
    assert!(
        join_sql.contains(r#""partition_columns":["REGION"]"#),
        "the dimension side's OWN partition columns must reach JoinSpec, independent of \
         the fact side's: {join_sql}"
    );
}

fn char_cast_key(size: u64, character_set: &str) -> Json {
    serde_json::json!({
        "type": "function_scalar_cast",
        "name": "CAST",
        "dataType": {"type": "CHAR", "size": size, "characterSet": character_set},
        "arguments": [{"type": "column", "name": "NAME"}]
    })
}

fn char_grouped_request(key: Json, key_type: Json, order_by: Option<Json>) -> Json {
    let mut body = serde_json::json!({
        "aggregationType": "group_by",
        "groupBy": [key.clone()],
        "selectList": [key, agg_item("COUNT", None, false)],
        "selectListDataTypes": [key_type, {"type": "decimal", "precision": 18, "scale": 0}],
    });
    if let Some(elements) = order_by {
        body["orderBy"] = elements;
    }
    guard_events_request(body)
}

fn unprojected_char_grouped_request(key: Json) -> Json {
    guard_events_request(serde_json::json!({
        "aggregationType": "group_by",
        "groupBy": [key],
        "selectList": [agg_item("COUNT", None, false)],
        "selectListDataTypes": [{"type": "decimal", "precision": 18, "scale": 0}],
    }))
}

/// JSON-encoded into the spec blob, then wrapped in a SQL string literal.
fn embedded_group_keys(fragments: &[String]) -> String {
    let encoded: Vec<String> = fragments
        .iter()
        .map(|f| serde_json::to_string(f).expect("a group-key fragment is JSON-encodable"))
        .collect();
    format!(r#""group_keys":[{}]"#, encoded.join(",")).replace('\'', "''")
}

/// Scenario: The grouped dispatcher blank-pads a CHAR(20) key for DataFusion and casts it back on merge (#192).
#[test]
fn grouped_char_declared_group_key_reaches_the_scan_spec_blank_padded() {
    let request = char_grouped_request(
        char_cast_key(20, "UTF8"),
        serde_json::json!({"type": "CHAR", "size": 20, "characterSet": "UTF8"}),
        None,
    );

    let sql = guard_dispatch_sql(
        &request,
        vec![ProjectionItem::Column("NAME".to_string())],
        vec!["VARCHAR(2000000)".to_string()],
        false,
        None,
        Vec::new(),
    );

    let fragment = r#"CAST("NAME" AS VARCHAR)"#;
    let padded = format!(
        "CASE WHEN character_length({fragment}) < 20 THEN rpad({fragment}, 20) \
             ELSE {fragment} END"
    );
    assert!(
        sql.contains(&embedded_group_keys(std::slice::from_ref(&padded))),
        "the scan spec must carry the blank-padded group key {padded}: {sql}"
    );
    assert!(
        sql.contains(r#"CAST("GK_0" AS CHAR(20))"#),
        "the outer merge wrapper must still cast the staging column to CHAR(20): {sql}"
    );
}

/// Scenario: An unprojected CHAR key is also padded, since no outer CAST would surface a mismatch (#192).
#[test]
fn unprojected_char_declared_group_key_reaches_the_scan_spec_blank_padded() {
    let request = unprojected_char_grouped_request(char_cast_key(20, "UTF8"));

    let sql = guard_dispatch_sql(
        &request,
        vec![ProjectionItem::Column("NAME".to_string())],
        vec!["VARCHAR(2000000)".to_string()],
        false,
        None,
        Vec::new(),
    );

    let fragment = r#"CAST("NAME" AS VARCHAR)"#;
    let padded = format!(
        "CASE WHEN character_length({fragment}) < 20 THEN rpad({fragment}, 20) \
             ELSE {fragment} END"
    );
    assert!(
        sql.contains(&embedded_group_keys(std::slice::from_ref(&padded))),
        "an unprojected CHAR(20) group key must still reach the scan padded: {sql}"
    );
}

/// Scenario: An unprojected VARCHAR `groupBy` key reaches the scan spec unpadded.
#[test]
fn unprojected_varchar_declared_group_key_reaches_the_scan_spec_unpadded() {
    let request = unprojected_char_grouped_request(serde_json::json!({
        "type": "function_scalar_cast",
        "name": "CAST",
        "dataType": {"type": "VARCHAR", "size": 10},
        "arguments": [{"type": "column", "name": "NAME"}]
    }));

    let sql = guard_dispatch_sql(
        &request,
        vec![ProjectionItem::Column("NAME".to_string())],
        vec!["VARCHAR(2000000)".to_string()],
        false,
        None,
        Vec::new(),
    );

    assert!(
        sql.contains(&embedded_group_keys(&[
            r#"CAST("NAME" AS VARCHAR)"#.to_string()
        ])),
        "an unprojected VARCHAR-declared group key must reach the scan unpadded: {sql}"
    );
    assert!(
        !sql.contains("rpad("),
        "no pad may be emitted for an unprojected VARCHAR-declared group key: {sql}"
    );
}

/// Scenario: The pad width survives the ` ASCII` character-set suffix (#192).
#[test]
fn grouped_ascii_char_group_key_is_padded_to_its_declared_width() {
    let request = char_grouped_request(
        char_cast_key(3, "ASCII"),
        serde_json::json!({"type": "CHAR", "size": 3, "characterSet": "ASCII"}),
        None,
    );

    let sql = guard_dispatch_sql(
        &request,
        vec![ProjectionItem::Column("NAME".to_string())],
        vec!["VARCHAR(2000000)".to_string()],
        false,
        None,
        Vec::new(),
    );

    let fragment = r#"CAST("NAME" AS VARCHAR)"#;
    let padded = format!(
        "CASE WHEN character_length({fragment}) < 3 THEN rpad({fragment}, 3) \
             ELSE {fragment} END"
    );
    assert!(
        sql.contains(&embedded_group_keys(std::slice::from_ref(&padded))),
        "a `CHAR(3) ASCII` group key must reach the scan padded to 3: {sql}"
    );
    assert!(
        sql.contains(r#"CAST("GK_0" AS CHAR(3) ASCII)"#),
        "the outer merge wrapper must still cast the staging column to CHAR(3) ASCII: {sql}"
    );
}

/// Scenario: A VARCHAR group key reaches the scan spec with no blank padding.
#[test]
fn grouped_varchar_declared_group_key_reaches_the_scan_spec_unpadded() {
    let request = char_grouped_request(
        serde_json::json!({"type": "column", "name": "REGION"}),
        serde_json::json!({"type": "varchar", "size": 10}),
        None,
    );

    let sql = guard_dispatch_sql(
        &request,
        vec![ProjectionItem::Column("REGION".to_string())],
        vec!["VARCHAR(10)".to_string()],
        false,
        None,
        Vec::new(),
    );

    assert!(
        sql.contains(&embedded_group_keys(&[r#""REGION""#.to_string()])),
        "a VARCHAR-declared group key must reach the scan unpadded: {sql}"
    );
    assert!(
        !sql.contains("rpad("),
        "no pad may be emitted for a VARCHAR-declared group key: {sql}"
    );
}

/// Scenario: An ORDER BY on a CHAR group key still resolves against the unpadded key.
#[test]
fn order_by_on_a_char_declared_group_key_still_resolves_to_its_output_ordinal() {
    let request = char_grouped_request(
        serde_json::json!({"type": "column", "name": "NAME"}),
        serde_json::json!({"type": "CHAR", "size": 20, "characterSet": "UTF8"}),
        Some(serde_json::json!([{
            "type": "order_by_element",
            "expression": {"type": "column", "name": "NAME"},
            "isAscending": true,
            "nullsLast": true
        }])),
    );

    let sql = guard_dispatch_sql(
        &request,
        vec![ProjectionItem::Column("NAME".to_string())],
        vec!["VARCHAR(2000000)".to_string()],
        false,
        None,
        Vec::new(),
    );

    assert!(
        sql.contains("ORDER BY 1"),
        "the sort on the CHAR-declared group key must resolve to output ordinal 1: {sql}"
    );
    assert!(
        sql.contains("rpad("),
        "the DataFusion-side copy must still be padded alongside the resolved \
             ORDER BY: {sql}"
    );
}

const PRUNED_TO_EMPTY_TABLE: &str = "pruned_to_empty";

fn letter_partitioned_commit() -> String {
    let protocol = serde_json::json!({"protocol": {"minReaderVersion": 1, "minWriterVersion": 2}});
    let metadata = serde_json::json!({"metaData": {
        "id": "pruned-to-empty",
        "format": {"provider": "parquet", "options": {}},
        "schemaString": serde_json::json!({"type": "struct", "fields": [
            {"name": "letter", "type": "string", "nullable": true, "metadata": {}},
            {"name": "number", "type": "integer", "nullable": true, "metadata": {}},
        ]})
        .to_string(),
        "partitionColumns": ["letter"],
        "configuration": {},
        "createdTime": 1,
    }});
    let adds = ["a", "b"]
        .into_iter()
        .map(|letter| {
            serde_json::json!({"add": {
                "path": format!("letter={letter}/part-0.parquet"),
                "partitionValues": {"letter": letter},
                "size": 100,
                "modificationTime": 1,
                "dataChange": true,
            }})
            .to_string()
        })
        .collect::<Vec<_>>()
        .join("\n");
    format!("{protocol}\n{metadata}\n{adds}\n")
}

fn letter_partitioned_request(letter: &str) -> Json {
    serde_json::json!({
        "involvedTables": [{
            "name": "PRUNED_TO_EMPTY",
            "columns": [
                {"name": "LETTER", "dataType": {"type": "varchar", "size": 2000000}},
                {"name": "NUMBER", "dataType": {"type": "decimal", "precision": 10, "scale": 0}},
            ],
        }],
        "pushdownRequest": {
            "type": "select",
            "selectList": [
                {"type": "column", "name": "LETTER", "tableName": "PRUNED_TO_EMPTY"},
                {"type": "column", "name": "NUMBER", "tableName": "PRUNED_TO_EMPTY"},
            ],
            "filter": {
                "type": "predicate_equal",
                "left": {"type": "column", "name": "LETTER", "tableName": "PRUNED_TO_EMPTY"},
                "right": {"type": "literal_string", "value": letter},
            },
        },
    })
}

/// Scenario: Equality on a partition column prunes every file in a non-matching partition
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_delta_request_pruned_to_no_file_takes_the_empty_result_route() {
    let catalog = unity_delta_catalog().await;
    let storage = delta_object_endpoint(vec![(
        delta_commit_zero_key(PRUNED_TO_EMPTY_TABLE),
        letter_partitioned_commit(),
    )])
    .await;
    let table = format!("cat.sch.{PRUNED_TO_EMPTY_TABLE}");

    let matching = delta_pushdown(
        &letter_partitioned_request("a"),
        &catalog.uri,
        storage.clone(),
        &table,
    )
    .await
    .expect("a filter matching one partition must plan a scan, not fail");
    let matching_sql = matching["sql"]
        .as_str()
        .expect("pushdown response must carry a sql field");
    assert!(
        matching_sql.contains(SCAN_UDF_NAME),
        "the fixture must reach the scan path when one file survives, otherwise \
         the pruned-to-empty assertions below prove nothing: {matching_sql}"
    );

    let pruned = delta_pushdown(
        &letter_partitioned_request("z"),
        &catalog.uri,
        storage,
        &table,
    )
    .await
    .expect("a filter matching no partition must answer empty, never error");
    let sql = pruned["sql"]
        .as_str()
        .expect("pushdown response must carry a sql field");

    assert!(
        !sql.contains(SCAN_UDF_NAME),
        "a fully-pruned file list must take the empty-result early return, not \
         invoke the scan UDF: {sql}"
    );
    assert!(
        !sql.contains(DISTRIBUTE_FILES_UDF_NAME),
        "a fully-pruned file list must fan out to no shard at all: {sql}"
    );
    assert!(
        !sql.contains(".parquet"),
        "a fully-pruned file list must embed no data-file path: {sql}"
    );
    assert!(
        sql.contains("WHERE 1=0"),
        "a fully-pruned row-scan request must render the typed zero-row shape: {sql}"
    );
}

fn letter_partitioned_select_request() -> Json {
    serde_json::json!({
        "involvedTables": [{
            "name": "PRUNED_TO_EMPTY",
            "columns": [
                {"name": "LETTER", "dataType": {"type": "varchar", "size": 2000000}},
                {"name": "NUMBER", "dataType": {"type": "decimal", "precision": 10, "scale": 0}},
            ],
        }],
        "pushdownRequest": {
            "type": "select",
            "selectList": [
                {"type": "column", "name": "LETTER", "tableName": "PRUNED_TO_EMPTY"},
                {"type": "column", "name": "NUMBER", "tableName": "PRUNED_TO_EMPTY"},
            ],
        },
    })
}

/// Scenario: The Delta reader is reached from production pushdown under the Unity Catalog kind
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_unity_catalog_pushdown_prunes_the_delta_file_list_by_its_filter() {
    let catalog = unity_delta_catalog().await;
    let table_name = "letter_pruned_by_filter";
    let table = format!("cat.sch.{table_name}");

    let filtered_storage = delta_object_endpoint(vec![(
        delta_commit_zero_key(table_name),
        letter_partitioned_commit(),
    )])
    .await;
    let filtered = delta_pushdown(
        &letter_partitioned_request("a"),
        &catalog.uri,
        filtered_storage,
        &table,
    )
    .await
    .expect("a filter matching one partition must plan a scan, not fail");
    let filtered_sql = filtered["sql"]
        .as_str()
        .expect("pushdown response must carry a sql field");

    let unfiltered_storage = delta_object_endpoint(vec![(
        delta_commit_zero_key(table_name),
        letter_partitioned_commit(),
    )])
    .await;
    let unfiltered = delta_pushdown(
        &letter_partitioned_select_request(),
        &catalog.uri,
        unfiltered_storage,
        &table,
    )
    .await
    .expect("an unfiltered request over the same fixture must plan a scan, not fail");
    let unfiltered_sql = unfiltered["sql"]
        .as_str()
        .expect("pushdown response must carry a sql field");

    let filtered_file_count = filtered_sql.matches(".parquet").count();
    let unfiltered_file_count = unfiltered_sql.matches(".parquet").count();
    assert_eq!(
        unfiltered_file_count, 2,
        "the two-file letter=a/letter=b fixture must embed both files when unfiltered: \
         unfiltered={unfiltered_sql}"
    );
    assert_eq!(
        filtered_file_count, 1,
        "a LETTER = 'a' partition-equality filter must prune the Delta file list to \
         exactly the one matching partition through production pushdown: \
         filtered={filtered_sql}"
    );
}

use crate::scan::sealed::{
    connection_password_carries_key_material, derive_sealed_storage_key, unseal_storage,
};

const SENTINEL_CONNECTION_NAME: &str = "SENTINEL_SCAN_STORAGE_CONNECTION";
const SENTINEL_ACCESS_KEY: &str = "SENTINEL_ACCESS_KEY_VALUE";
const SENTINEL_SECRET_KEY: &str = "SENTINEL_SECRET_KEY_VALUE";
const SENTINEL_SESSION_TOKEN: &str = "SENTINEL_SESSION_TOKEN_VALUE";
const SENTINEL_PASSWORD: &str = r#"{"warehouse":"wh","secret_key":"SENTINEL_SECRET_KEY_VALUE"}"#;

fn assert_no_sentinel_secret_leaked(text: &str) {
    for secret in [
        SENTINEL_ACCESS_KEY,
        SENTINEL_SECRET_KEY,
        SENTINEL_SESSION_TOKEN,
    ] {
        assert!(
            !text.contains(secret),
            "sentinel secret {secret:?} leaked in: {text}"
        );
    }
}

fn sentinel_creds(use_vended_credentials: bool) -> ConnectionCreds {
    ConnectionCreds {
        warehouse: "wh".into(),
        endpoint: "http://sentinel-minio:9000".into(),
        region: "sentinel-region-1".into(),
        access_key: SENTINEL_ACCESS_KEY.into(),
        secret_key: SENTINEL_SECRET_KEY.into(),
        session_token: Some(SENTINEL_SESSION_TOKEN.into()),
        path_style: Some(true),
        use_vended_credentials,
        ..Default::default()
    }
}

fn sentinel_effective_backend(creds: &ConnectionCreds) -> StorageBackend {
    crate::adapter::connection::storage_block(creds, true)
}

fn dispatch_result_for_body(
    pushdown_req_body: Json,
    logical_schema: Vec<LogicalField>,
    scan_storage: &ScanStorage,
) -> Result<String, UdfError> {
    let request = guard_events_request(pushdown_req_body);
    let pushdown_req = pd(&request);
    let col_types = guard_col_types();
    let (proj_cols, proj_types, projection_widened) =
        extract_projection(&request, &pushdown_req).expect("the fixture must project");
    let (filter, declined_filter) = classify_where_filter(
        pushdown_req.get("filter").filter(|f| !f.is_null()),
        &col_types,
    );
    let result = build_dispatch_sql(
        &request,
        &pushdown_req,
        proj_cols,
        proj_types,
        projection_widened,
        col_types,
        filter,
        declined_filter,
        extract_limit(&pushdown_req),
        order_by_present(&pushdown_req),
        &[vec![FileEntry::new("data/part-0.parquet", 1_000)]],
        "s3://warehouse/db/events".to_string(),
        logical_schema,
        Vec::new(),
        Vec::new(),
        scan_storage,
        SCAN_UDF_NAME,
        DISTRIBUTE_FILES_UDF_NAME,
        4,
        8192,
        2,
        0.6,
        200,
        8,
    )?;
    Ok(result["sql"]
        .as_str()
        .expect("pushdown response must carry a sql field")
        .to_string())
}

fn row_scan_body() -> Json {
    serde_json::json!({
        "selectList": [
            {"type": "column", "name": "REGION"},
            {"type": "column", "name": "AMOUNT"},
        ],
        "selectListDataTypes": [
            {"type": "varchar", "size": 2000000},
            {"type": "decimal", "precision": 18, "scale": 2},
        ],
    })
}

#[test]
fn no_connection_credential_reaches_the_generated_sql() {
    let body = row_scan_body();

    let static_creds = sentinel_creds(false);
    let static_storage = scan_storage_for(
        &static_creds,
        SENTINEL_CONNECTION_NAME,
        true,
        &sentinel_effective_backend(&static_creds),
        None,
    )
    .expect("static selection");
    let static_sql = dispatch_result_for_body(body.clone(), Vec::new(), &static_storage)
        .expect("static dispatch");
    assert!(
        static_sql.contains(SENTINEL_CONNECTION_NAME),
        "{static_sql}"
    );
    assert_no_sentinel_secret_leaked(&static_sql);

    let vended_creds = sentinel_creds(true);
    let vended_effective = sentinel_effective_backend(&vended_creds);
    let key = connection_password_carries_key_material(&vended_creds)
        .then(|| derive_sealed_storage_key(SENTINEL_PASSWORD))
        .expect("must carry key material");
    let vended_storage = scan_storage_for(
        &vended_creds,
        SENTINEL_CONNECTION_NAME,
        true,
        &vended_effective,
        Some(&key),
    )
    .expect("vended selection");
    let vended_sql =
        dispatch_result_for_body(body, Vec::new(), &vended_storage).expect("vended dispatch");
    assert!(vended_sql.contains("\"sealed\":{\"name\":"), "{vended_sql}");
    let common: Json = serde_json::from_str(common_arg_literal(&vended_sql)).unwrap();
    let selected: ScanStorage = serde_json::from_value(common["storage"].clone()).unwrap();
    let ScanStorage::Sealed { payload, .. } = &selected else {
        panic!("expected Sealed, got {selected:?}");
    };
    assert_eq!(&unseal_storage(payload, &key).unwrap(), &vended_effective);
    assert_no_sentinel_secret_leaked(&vended_sql);
}
