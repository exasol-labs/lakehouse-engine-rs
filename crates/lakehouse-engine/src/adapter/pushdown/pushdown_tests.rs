use super::test_support::*;
use super::*;
use crate::scan::spec::{
    ColumnRef, CommonScanSpec, FileEntry, LogicalField, ProjectionItem, ScanSpec, ScanStorage,
    StorageProps,
};
use crate::scan::test_support::WithFiles;
use crate::types::exasol_type::ExasolType;

// The vended sentinels below repeat the literal values `lakehouse-catalog`'s own
// `test_support` uses, so both crates' assertions stay comparable.
const VENDED_AK: &str = "VENDED_AK_SENTINEL";
const VENDED_SK: &str = "VENDED_SK_SENTINEL";
const VENDED_TOK: &str = "VENDED_TOKEN_SENTINEL";
const VENDED_REGION: &str = "eu-west-2";

/// Matched as `"<field>":` to avoid false positives (e.g. `"session_token"` contains
/// `"token"`); `scope` is too short and appears in storage endpoint strings.
const CATALOG_AUTH_KEYS: [&str; 6] = [
    "\"token\":",
    "\"credential\":",
    "\"client_id\":",
    "\"client_secret\":",
    "\"oauth2_server_uri\":",
    "\"oauth2-server-uri\":",
];

/// Scenario: Catalog auth props are never placed in any scan spec
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

    let spec = CommonScanSpec {
        projection: vec!["ID".into()],
        storage: ScanStorage::Inline(vended_storage),
        ..Default::default()
    }
    .with_files(vec![FileEntry::new(
        "s3://warehouse/db/events/part-00000.parquet",
        1,
    )]);

    let json = spec.to_json();

    for field in CATALOG_AUTH_KEYS {
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

/// Scenario: Catalog auth props are never placed in any scan spec
#[test]
fn catalog_auth_secrets_never_in_a_role_scan_spec() {
    const CATALOG_TOKEN: &str = "ROLE_CATALOG_TOKEN_SENTINEL";
    let role_creds = ConnectionCreds {
        access_key: SENTINEL_ACCESS_KEY.into(),
        secret_key: SENTINEL_SECRET_KEY.into(),
        session_token: Some(SENTINEL_SESSION_TOKEN.into()),
        token: Some(CATALOG_TOKEN.into()),
        aws_assume_role_arn: Some("arn:aws:iam::123456789012:role/lakehouse-reader".into()),
        ..unauthenticated_creds()
    };
    let session_storage = crate::adapter::connection::storage_block(&role_creds, true);
    let role_spec = CommonScanSpec {
        projection: vec!["ID".into()],
        storage: scan_storage_for(
            &role_creds,
            TEST_CONNECTION_NAME,
            true,
            &session_storage,
            Some(&test_sealing_key()),
        )
        .expect("a role CONNECTION carries key material"),
        ..Default::default()
    }
    .with_files(vec![FileEntry::new(
        "s3://warehouse/db/events/part-00000.parquet",
        1,
    )]);
    let role_json = role_spec.to_json();
    for field in CATALOG_AUTH_KEYS {
        assert!(!role_json.contains(field), "{field} in: {role_json}");
    }
    assert!(!role_json.contains(CATALOG_TOKEN), "{role_json}");
    assert_no_sentinel_secret_leaked(&role_json);
    let ScanStorage::Sealed { payload, .. } = &role_spec.common.storage else {
        panic!("a role storage block must be sealed: {role_json}");
    };
    assert_eq!(
        crate::scan::sealed::unseal_storage(payload, &test_sealing_key()).unwrap(),
        session_storage,
        "the envelope carries storage material only"
    );
}

/// Scenario: Grouped scan spec carries group-key rendered SQL fragments
#[test]
fn grouped_scan_spec_carries_group_keys() {
    let group_keys = vec!["\"REGION\"".to_string(), "YEAR(\"TS\")".to_string()];
    let spec = CommonScanSpec {
        aggregates: Some(vec![count_plan()]),
        group_keys: Some(group_keys.clone()),
        storage: ScanStorage::Inline(sample_storage()),
        ..Default::default()
    }
    .with_files(vec![FileEntry::new("s3://w/f0.parquet", 1)]);
    let json = spec.to_json();
    let back = ScanSpec::from_json(&json).expect("must round-trip");
    let keys = back.common.group_keys.expect("group_keys must be present");
    assert_eq!(keys, group_keys, "group_keys must survive spec round-trip");
}

#[test]
fn leading_wildcard_like_yields_df_string() {
    let df_filter = scan_filter_sql(
        &like(col("name"), "%A"),
        &column_types(&[("NAME", ExasolType::WIDEST_VARCHAR)]),
    );
    assert!(
        df_filter.is_some(),
        "LIKE filter must still produce a DataFusion SQL string: {df_filter:?}"
    );
}

#[test]
fn a_decimal_compared_with_a_number_renders_its_bare_column() {
    let col_types = vec![("C_DECIMAL_A".to_string(), ExasolType::decimal(10, 2))];
    let filter_json = compare("predicate_greater", col("c_decimal_a"), num_lit(5));

    let rendered = Some(&filter_json)
        .and_then(|f| scan_filter_sql(f, &column_types(&col_types)))
        .expect("c_decimal_a > 5 must render to a DataFusion filter");

    assert_eq!(
        rendered, r#"("C_DECIMAL_A" > 5)"#,
        "a DECIMAL column in a comparison must stay a bare, unwrapped column reference: {rendered}"
    );
}

#[test]
fn where_filter_string_fn_over_session_formatted_number_declines() {
    let col_types = vec![
        ("C_DOUBLE_A".to_string(), ExasolType::DoublePrecision),
        ("C_DECIMAL_A".to_string(), ExasolType::decimal(10, 2)),
    ];
    for column in ["c_double_a", "c_decimal_a"] {
        let cases = [
            predicate_equal(scalar_fn("UPPER", [col(column)]), str_lit("X")),
            compare(
                "predicate_greater",
                scalar_fn("LENGTH", [col(column)]),
                num_lit(5),
            ),
            like(scalar_fn("UPPER", [col(column)]), "1%"),
        ];
        for filter in cases {
            let rendered = scan_filter_sql(&filter, &column_types(&col_types));
            assert!(rendered.is_none(), "{column}: {rendered:?}");
        }
    }
}

#[test]
fn where_filter_like_decimal_inside_case_declines_whole_filter() {
    let col_types = vec![("AMOUNT".to_string(), ExasolType::decimal(9, 2))];
    let filter_json = predicate_equal(
        case_when([like(col("amount"), "9%")], [num_lit(1), num_lit(0)]),
        num_lit(1),
    );

    let rendered = Some(&filter_json).and_then(|f| scan_filter_sql(f, &column_types(&col_types)));

    assert!(
        rendered.is_none(),
        "a DECIMAL LIKE buried inside a function_scalar_case under predicate_equal's \
             left must decline the whole filter through the full wired chain, not push a \
             possibly-wrong native comparison: {rendered:?}"
    );
}

#[test]
fn scan_spec_carries_no_catalog_block() {
    const TOKEN_SENTINEL: &str = "TOKEN_SENTINEL_VALUE";
    const SECRET_SENTINEL: &str = "CLIENT_SECRET_SENTINEL_VALUE";
    const OAUTH_URI_SENTINEL: &str = "https://oauth-uri-sentinel.example/token";
    const SCOPE_SENTINEL: &str = "SCOPE_SENTINEL_VALUE";

    let spec = CommonScanSpec {
        projection: vec!["ID".into(), "NAME".into()],
        filter: Some("(\"ID\" > 10)".into()),
        limit: Some(100),
        storage: ScanStorage::Inline(sample_storage()),
        ..Default::default()
    }
    .with_files(vec![FileEntry::new(
        "s3://warehouse/db/events/part-00000.parquet",
        1,
    )]);

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

    let logical = declared_logical_schema(&schema);

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

    let spec = CommonScanSpec {
        logical_schema: logical.clone(),
        storage: ScanStorage::Inline(sample_storage()),
        ..Default::default()
    }
    .with_files(vec![]);
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

#[test]
fn iceberg_declaration_encodes_primitive_initial_default() {
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

    let logical = declared_logical_schema(&schema);

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

#[test]
fn iceberg_declaration_omits_default_for_no_default_field() {
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

    let logical = declared_logical_schema(&schema);

    assert_eq!(logical.len(), 1);
    assert!(
        logical[0].initial_default.is_none(),
        "a field without an initial-default must encode None"
    );
}

/// Scenario: A decimal initial-default outside Exasol's DECIMAL domain is not encoded as a numeric default
#[test]
fn iceberg_declaration_omits_default_for_decimal_outside_exasol_domain() {
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

    let logical = declared_logical_schema(&schema);

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

#[test]
fn iceberg_declaration_omits_non_primitive_default() {
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

    let logical = declared_logical_schema(&schema);

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

#[test]
fn iceberg_declaration_ignores_write_default() {
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

    let logical = declared_logical_schema(&schema);

    assert_eq!(logical.len(), 1);
    assert!(
        logical[0].initial_default.is_none(),
        "write-default must never be read into initial_default"
    );
}

#[test]
fn iceberg_declaration_default_encoding_is_credential_free() {
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

    let logical = declared_logical_schema(&schema);
    assert_eq!(logical[0].initial_default.as_deref(), Some("plain-default"));

    let json = serde_json::to_string(&logical).unwrap();
    for marker in ["access_key", "secret_key", "session_token", "endpoint"] {
        assert!(
            !json.contains(marker),
            "encoded default carrier must be credential-free, found '{marker}': {json}"
        );
    }
}

#[test]
fn iceberg_declaration_default_less_spec_round_trips_unchanged() {
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

    let logical = declared_logical_schema(&schema);
    assert!(
        logical.iter().all(|f| f.initial_default.is_none()),
        "a default-less schema must encode no defaults"
    );

    let spec = CommonScanSpec {
        logical_schema: logical.clone(),
        storage: ScanStorage::Inline(sample_storage()),
        ..Default::default()
    }
    .with_files(vec![]);
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

fn guard_col_types() -> Vec<(String, ExasolType)> {
    vec![
        ("REGION".to_string(), ExasolType::WIDEST_VARCHAR),
        ("NAME".to_string(), ExasolType::WIDEST_VARCHAR),
        ("AMOUNT".to_string(), ExasolType::decimal(18, 2)),
        ("ID".to_string(), ExasolType::decimal(20, 0)),
    ]
}

fn guard_events() -> PushdownRequestBuilder {
    pushdown_request("EVENTS", &guard_col_types())
}

/// `projection_widened` is the dispatcher's routing flag (#196).
fn guard_dispatch_sql(
    request: &Json,
    proj_cols: Vec<ProjectionItem>,
    proj_types: Vec<ExasolType>,
    projection_widened: bool,
    limit: Option<u64>,
    logical_schema: Vec<LogicalField>,
) -> String {
    let result = dispatch_with(
        request,
        projected_columns(&proj_cols, &proj_types),
        projection_widened,
        &column_types(&guard_col_types()),
        (None, None),
        limit,
        logical_schema,
        &sample_scan_storage(),
    )
    .expect("build_dispatch_sql must succeed for this fixture");
    result["sql"]
        .as_str()
        .expect("pushdown response must carry a sql field")
        .to_string()
}

/// [`build_dispatch_sql`] over the sample file list and scan settings, as the dispatcher calls it.
#[allow(clippy::too_many_arguments)]
fn dispatch_with(
    request: &Json,
    projection: Vec<ProjectedColumn>,
    projection_widened: bool,
    col_types: &ColumnTypes,
    (filter, declined_filter): (Option<String>, Option<&Json>),
    limit: Option<u64>,
    logical_schema: Vec<LogicalField>,
    scan_storage: &ScanStorage,
) -> Result<Json, UdfError> {
    let pushdown_req = pd(request);
    build_dispatch_sql(
        request,
        &PushdownRequest::parse(pushdown_req.clone())?,
        projection,
        projection_widened,
        scan_for(col_types, &pushdown_req),
        filter,
        declined_filter,
        limit,
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
        false,
    )
}

/// [`dispatch_with`] after the dispatcher's own projection and WHERE split of `request`.
fn dispatch_planned(
    request: &Json,
    col_types: &ColumnTypes,
    logical_schema: Vec<LogicalField>,
    scan_storage: &ScanStorage,
) -> Result<Json, UdfError> {
    let pushdown_req = pd(request);
    let (proj_cols, proj_types, projection_widened) =
        project_first_table(request, &pushdown_req).expect("the fixture must project");
    let filter = pushdown_req.get("filter").filter(|f| !f.is_null());
    dispatch_with(
        request,
        projected_columns(&proj_cols, &proj_types),
        projection_widened,
        col_types,
        scan_for(col_types, &pushdown_req).split_where(filter),
        extract_limit(&pushdown_req),
        logical_schema,
        scan_storage,
    )
}

/// A filter the scan declines though Exasol renders it.
fn declined_like_on_decimal() -> Json {
    like(table_col("EVENTS", "AMOUNT"), "1%")
}

/// The empty logical schema declines the bounded top-N unconditionally, so the
/// decline route must win on its own.
fn dispatch_sql_for_body(request: PushdownRequestBuilder) -> String {
    let (request, _) = request.build();
    let result = dispatch_planned(
        &request,
        &first_table_column_types(&request),
        Vec::new(),
        &sample_scan_storage(),
    )
    .expect("build_dispatch_sql must succeed for this fixture");
    result["sql"]
        .as_str()
        .expect("pushdown response must carry a sql field")
        .to_string()
}

#[test]
fn declined_filter_routes_every_dispatch_shape_to_qualified_wrapper() {
    let declined = declined_like_on_decimal();
    let id_col = table_col("EVENTS", "ID");
    let region_col = table_col("EVENTS", "REGION");
    let shapes = [
        (
            "row scan",
            guard_events()
                .column(id_col.clone())
                .filter(declined.clone()),
        ),
        (
            "grouped aggregate",
            guard_events()
                .group_by(vec![region_col.clone()])
                .column(region_col)
                .count_star()
                .filter(declined.clone()),
        ),
        (
            "ordered top-N",
            guard_events()
                .column(id_col.clone())
                .order_by(serde_json::json!([order_by_element(id_col, true, true)]))
                .limit(5)
                .filter(declined.clone()),
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

#[test]
fn declined_filter_with_absent_select_list_projects_full_row() {
    let sql = dispatch_sql_for_body(guard_events().filter(declined_like_on_decimal()));

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

#[test]
fn declined_filter_with_a_real_select_list_keeps_the_narrowing() {
    let sql = dispatch_sql_for_body(
        guard_events()
            .filter(declined_like_on_decimal())
            .column(table_col("EVENTS", "ID")),
    );

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

#[test]
fn single_group_scalar_over_aggregate_renders_the_scalar_over_the_merge() {
    let sql = dispatch_sql_for_body(guard_events().item(
        scalar_fn(
            "ROUND",
            [agg_item("SUM", Some("AMOUNT"), false), num_lit(2)],
        ),
        ExasolType::decimal(36, 2),
    ));

    assert!(
        sql.starts_with(
            r#"SELECT CAST(ROUND(CAST(SUM("PARTIAL_sum_0") AS DECIMAL(29,2)), 2) AS DECIMAL(36,2)) FROM ("#
        ),
        "the merge SELECT must wrap the merged partial in the scalar structure, \
         not emit a bare unwrapped SUM: {sql}"
    );
    assert!(
        sql.contains(r#""aggregates":[{"kind":"sum","column":"AMOUNT""#),
        "the per-shard scan must carry the inner aggregate as a partial plan: {sql}"
    );
}

#[test]
fn pruning_input_unchanged_when_df_render_declines() {
    // `SECOND(ts, 3)` is a DataFusion arity decline Exasol renders; `LENGTH(amount) > 5`
    // is rewritten, so the equality assertion distinguishes original from rewritten.
    let filter = and([
        compare("predicate_greater", col("id"), num_lit(5)),
        declined_second_predicate(col("ts")),
        compare(
            "predicate_greater",
            scalar_fn("LENGTH", [col("amount")]),
            num_lit(5),
        ),
    ]);
    let col_types = vec![
        ("ID".to_string(), ExasolType::decimal(20, 0)),
        ("TS".to_string(), ExasolType::BARE_TIMESTAMP),
        ("AMOUNT".to_string(), ExasolType::decimal(18, 2)),
    ];
    let (scan_filter, declined) = scan_over(&column_types(&col_types)).split_where(Some(&filter));

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
}

#[test]
fn declined_order_by_appends_unprojected_sort_key_as_hidden_column() {
    let request = guard_events()
        .order_by(serde_json::json!([order_by_element(
            col("NAME"),
            true,
            true
        )]))
        .limit(10)
        .item(num_lit(1), ExasolType::decimal(1, 0))
        .build()
        .0;
    let proj_cols = vec![ProjectionItem::Expr {
        expr: "1".to_string(),
    }];
    let proj_types = vec![ExasolType::decimal(1, 0)];

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

#[test]
fn declined_order_by_wrapper_selects_only_original_select_list() {
    let request = guard_events()
        .order_by(serde_json::json!([order_by_element(col("ID"), true, true)]))
        .column(col("NAME"))
        .build()
        .0;
    let proj_cols = vec![ProjectionItem::from("NAME")];
    let proj_types = vec![ExasolType::WIDEST_VARCHAR];

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

#[test]
fn declined_order_by_dedupes_repeated_and_projected_sort_keys() {
    let sort_key = |name: &str| order_by_element(col(name), true, true);
    let request = guard_events()
        .order_by(serde_json::json!([
            sort_key("NAME"),
            sort_key("ID"),
            sort_key("NAME"),
            sort_key("ID"),
        ]))
        .column(col("NAME"))
        .build()
        .0;
    let proj_cols = vec![ProjectionItem::from("NAME")];
    let proj_types = vec![ExasolType::WIDEST_VARCHAR];

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

#[test]
fn declined_order_by_all_keys_projected_leaves_projection_untouched() {
    let request = guard_events()
        .order_by(serde_json::json!([order_by_element(
            col("NAME"),
            true,
            true
        )]))
        .limit(5)
        .column(col("NAME"))
        .build()
        .0;
    let proj_cols = vec![ProjectionItem::from("NAME")];
    let proj_types = vec![ExasolType::WIDEST_VARCHAR];
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

#[test]
fn nonzero_offset_nulls_the_effective_limit() {
    let request = guard_events()
        .order_by(serde_json::json!([order_by_element(
            col("NAME"),
            true,
            true
        )]))
        .field("limit", serde_json::json!({"numElements": 5, "offset": 2}))
        .column(col("NAME"))
        .build()
        .0;
    let proj_cols = vec![ProjectionItem::from("NAME")];
    let proj_types = vec![ExasolType::WIDEST_VARCHAR];
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

#[test]
fn declined_order_by_extension_runs_after_topn_detection() {
    let request = guard_events()
        .order_by(serde_json::json!([order_by_element(
            col("NAME"),
            true,
            true
        )]))
        .limit(5)
        .item(num_lit(1), ExasolType::decimal(1, 0))
        .build()
        .0;
    let proj_cols = vec![ProjectionItem::Expr {
        expr: "1".to_string(),
    }];
    let proj_types = vec![ExasolType::decimal(1, 0)];
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

/// Scenario: An ORDER BY the adapter cannot bound as a top-N remains correctness-safe
#[test]
fn declined_order_by_renders_every_ordering_in_full() {
    let ordered = guard_events()
        .order_by(serde_json::json!([
            order_by_element(
                scalar_fn("ABS", [table_col("EVENTS", "AMOUNT")]),
                false,
                true
            ),
            order_by_element(table_col("EVENTS", "NAME"), true, false),
        ]))
        .limit(7)
        .column(col("NAME"))
        .build()
        .0;
    let sql = guard_dispatch_sql(
        &ordered,
        vec![ProjectionItem::from("NAME")],
        vec![ExasolType::WIDEST_VARCHAR],
        false,
        Some(7),
        Vec::new(),
    );
    assert!(
        sql.ends_with(r#" ORDER BY ABS("AMOUNT") DESC NULLS LAST, "NAME" ASC NULLS FIRST LIMIT 7"#),
        "the wrapper must render every pushed sort key, then the window: {sql}"
    );

    let unordered = guard_events().limit(7).column(col("NAME")).build().0;
    let sql = guard_dispatch_sql(
        &unordered,
        vec![ProjectionItem::from("NAME")],
        vec![ExasolType::WIDEST_VARCHAR],
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

#[test]
fn lone_count_distinct_with_order_by_still_renders_limit() {
    let request = guard_events()
        .order_by(serde_json::json!([order_by_element(
            table_col("EVENTS", "ID"),
            true,
            true
        )]))
        .item(
            agg_item("COUNT", Some("ID"), true),
            ExasolType::decimal(18, 0),
        )
        .build()
        .0;

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
    guard_events()
        .order_by(serde_json::json!([order_by_element(
            table_col("EVENTS", "NAME"),
            true,
            true
        )]))
        .column(table_col("EVENTS", "REGION"))
        .column(table_col("EVENTS", "NAME"))
        .column(table_col("EVENTS", "AMOUNT"))
        .column(table_col("EVENTS", "ID"))
        .build()
        .0
}

fn widening_arity_coincidence_projection() -> (Vec<ProjectionItem>, Vec<ExasolType>) {
    let cols = guard_col_types()
        .into_iter()
        .map(|(name, ty)| (ProjectionItem::from(name), ty))
        .collect::<Vec<_>>();
    (
        cols.iter().map(|(c, _)| c.clone()).collect(),
        cols.iter().map(|(_, t)| *t).collect(),
    )
}

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

fn timestamp_cast_select_request(precision: u8) -> PushdownRequestBuilder {
    let target = ExasolType::timestamp(precision);
    let mut columns = guard_col_types();
    columns.push(("TS".to_string(), ExasolType::timestamp(9)));
    pushdown_request("EVENTS", &columns).item(cast_to(target, table_col("EVENTS", "TS")), target)
}

#[test]
fn declined_timestamp_precision_cast_routes_to_qualified_wrapper() {
    let sql = dispatch_sql_for_body(timestamp_cast_select_request(2));

    assert!(
        sql.starts_with(r#"SELECT CAST("LHS_T0"."TS" AS TIMESTAMP(2)) FROM ("#),
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
        emits.contains(r#""TS" TIMESTAMP(9)"#) && !emits.contains("TIMESTAMP(2)"),
        "the scan must emit the raw column, never the declined CAST target: {sql}"
    );
}

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

const REFUSED_COLUMN_TABLE_COLUMNS: &[(&str, &str)] =
    &[("int_col", "integer"), ("variant_col", "variant")];

/// The log declares the refused column a string: a `variant` log needs the `variantType` feature,
/// which the reader refuses as a whole table, while the notes alone decide the column's refusal.
const REFUSED_COLUMN_TABLE_LOG_COLUMNS: &[(&str, &str)] =
    &[("int_col", "integer"), ("variant_col", "string")];

fn refused_column_table() -> PushdownRequestBuilder {
    pushdown_request(
        "ORDERS",
        &[
            ("INT_COL", ExasolType::decimal(10, 0)),
            ("VARIANT_COL", ExasolType::WIDEST_VARCHAR),
        ],
    )
    .field("type", "select")
}

fn refused_column_request(request: PushdownRequestBuilder) -> Json {
    with_delta_notes(
        request.build().0,
        "ORDERS",
        REFUSED_COLUMN_TABLE_COLUMNS,
        &[],
    )
}

async fn refused_column_table_storage() -> crate::scan::spec::StorageBackend {
    delta_object_endpoint(vec![(
        delta_commit_zero_key("orders"),
        fileless_delta_commit("orders", REFUSED_COLUMN_TABLE_LOG_COLUMNS),
    )])
    .await
}

fn column_item(name: &str) -> Json {
    table_col("ORDERS", name)
}

/// Scenario: A refused column refuses only the requests that read or emit it
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_refused_column_is_refused_before_the_zero_active_files_early_return() {
    let catalog = unity_delta_catalog().await;
    let storage = refused_column_table_storage().await;

    let planned = delta_pushdown(
        &refused_column_request(refused_column_table().column(column_item("INT_COL"))),
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
        &refused_column_request(refused_column_table().column(column_item("VARIANT_COL"))),
        &catalog.uri,
        storage,
        "cat.sch.orders",
    )
    .await
    .expect_err("a request emitting the refused column must be refused, never answered empty");

    let message = user_message(error);
    assert!(
        message.contains("variant_col") && message.contains("type 'variant'"),
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
        &refused_column_request(refused_column_table().column(column_item("INT_COL"))),
        &catalog.uri,
        storage,
        "cat.sch.orders",
    )
    .await
    .expect_err("a reader feature outside the allow-list must refuse before any column gate");

    let message = user_message(error);
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

    let (request, _) = pushdown_request("ORDERS", &[("INT_COL", ExasolType::decimal(10, 0))])
        .field("type", serde_json::json!("select"))
        .column(table_col("ORDERS", "INT_COL"))
        .build();
    let request = with_delta_notes(request, "ORDERS", &[("int_col", "integer")], &[]);

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
        projection: vec![ProjectionItem::from("INT_COL")],
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
        &refused_column_request(refused_column_table().count_star()),
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
        &refused_column_request(refused_column_table().item(
            agg_item_expr("MAX", column_item("VARIANT_COL"), false),
            ExasolType::WIDEST_VARCHAR,
        )),
        &catalog.uri,
        storage,
        "cat.sch.orders",
    )
    .await
    .expect_err("an aggregate reading the refused column must be refused");

    assert_refuses_variant_col(error);
}

/// Scenario: A refused column refuses only the requests that read or emit it
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_count_star_filtered_on_a_refused_column_is_refused() {
    let catalog = unity_delta_catalog().await;
    let storage = refused_column_table_storage().await;

    let error = delta_pushdown(
        &refused_column_request(
            refused_column_table()
                .count_star()
                .filter(is_not_null(column_item("VARIANT_COL"))),
        ),
        &catalog.uri,
        storage,
        "cat.sch.orders",
    )
    .await
    .expect_err("a COUNT(*) filtered on the refused column reads it, so it must be refused");

    assert_refuses_variant_col(error);
}

/// Scenario: Resolved partition columns reach the scan spec for every side
#[test]
fn resolved_partition_columns_reach_the_common_spec_and_the_join_spec() {
    let (request, _) = guard_events()
        .field("type", serde_json::json!("select"))
        .build();
    let pushdown_req = pd(&request);
    let (proj_cols, proj_types, widened) =
        project_first_table(&request, &pushdown_req).expect("the EVENTS fixture must project");
    let result = build_dispatch_sql(
        &request,
        &parsed_pushdown(&request),
        projected_columns(&proj_cols, &proj_types),
        widened,
        scan_for(&column_types(&guard_col_types()), &pushdown_req),
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
        false,
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
        column_types: column_types(NO_COLUMNS),
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
        column_types: column_types(NO_COLUMNS),
    };
    let sides = broadcast_sides(fact, dimension, 1);
    let rendered = rendered_join(
        "\"LHS_T0\".\"CUSTOMER_KEY\" = \"LHS_T1\".\"CUSTOMER_KEY\"",
        &[ProjectedColumn {
            item: ProjectionItem::Column(ColumnRef::qualified("LHS_T1", "CUSTOMER_KEY")),
            exa_type: ExasolType::decimal(20, 0),
        }],
    );
    let tuning = super::joins::JoinScanRequestConfig {
        cluster_nodes: 1,
        parallelism_factor: 1,
        df_target_partitions: 1,
        df_batch_size: 8192,
        df_threads_per_udf: 1,
        memory_pool_fraction: 0.6,
        instance_overhead_mb: 200,
        s3_max_connections: 8,
        binary_as_string: false,
        connection: &super::test_support::TEST_CONNECTION,
    };
    let join_sql = super::joins::build_broadcast_join_sql(
        &sides,
        &rendered,
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

/// A bare column `key` is declared in `key_type` by the table itself, as Exasol declares it.
fn char_grouped_request(key: Json, key_type: ExasolType, order_by: Option<Json>) -> Json {
    let key_column = key["type"] == "column";
    let columns: Vec<(String, ExasolType)> = guard_col_types()
        .into_iter()
        .map(|(name, exa_type)| {
            let is_key = key_column && key["name"] == name.as_str();
            (name, if is_key { key_type } else { exa_type })
        })
        .collect();
    let request = pushdown_request("EVENTS", &columns).group_by(vec![key.clone()]);
    let mut request = if key_column {
        request.column(key)
    } else {
        request.item(key, key_type)
    }
    .count_star();
    if let Some(elements) = order_by {
        request = request.order_by(elements);
    }
    request.build().0
}

fn unprojected_char_grouped_request(key: Json) -> Json {
    guard_events().group_by(vec![key]).count_star().build().0
}

/// JSON-encoded into the spec blob, then wrapped in a SQL string literal.
fn embedded_group_keys(fragments: &[String]) -> String {
    let encoded: Vec<String> = fragments
        .iter()
        .map(|f| serde_json::to_string(f).expect("a group-key fragment is JSON-encodable"))
        .collect();
    format!(r#""group_keys":[{}]"#, encoded.join(",")).replace('\'', "''")
}

#[test]
fn unprojected_varchar_declared_group_key_reaches_the_scan_spec_unpadded() {
    let request = unprojected_char_grouped_request(cast_to(ExasolType::varchar(10), col("NAME")));

    let sql = guard_dispatch_sql(
        &request,
        vec![ProjectionItem::from("NAME")],
        vec![ExasolType::WIDEST_VARCHAR],
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

#[test]
fn grouped_varchar_declared_group_key_reaches_the_scan_spec_unpadded() {
    let request = char_grouped_request(col("REGION"), ExasolType::varchar(10), None);

    let sql = guard_dispatch_sql(
        &request,
        vec![ProjectionItem::from("REGION")],
        vec![ExasolType::varchar(10)],
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

/// The notes CREATE records for the table [`letter_partitioned_commit`] logs.
fn with_letter_partitioned_notes(request: Json) -> Json {
    let columns = [("letter", "string"), ("number", "integer")];
    with_delta_notes(request, "PRUNED_TO_EMPTY", &columns, &["letter"])
}

fn letter_partitioned_request(letter: &str) -> Json {
    with_letter_partitioned_notes(
        pushdown_request(
            "PRUNED_TO_EMPTY",
            &[
                ("LETTER", ExasolType::WIDEST_VARCHAR),
                ("NUMBER", ExasolType::decimal(10, 0)),
            ],
        )
        .field("type", serde_json::json!("select"))
        .filter(predicate_equal(
            table_col("PRUNED_TO_EMPTY", "LETTER"),
            str_lit(letter),
        ))
        .column(table_col("PRUNED_TO_EMPTY", "LETTER"))
        .column(table_col("PRUNED_TO_EMPTY", "NUMBER"))
        .build()
        .0,
    )
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
    with_letter_partitioned_notes(
        pushdown_request(
            "PRUNED_TO_EMPTY",
            &[
                ("LETTER", ExasolType::WIDEST_VARCHAR),
                ("NUMBER", ExasolType::decimal(10, 0)),
            ],
        )
        .field("type", serde_json::json!("select"))
        .column(table_col("PRUNED_TO_EMPTY", "LETTER"))
        .column(table_col("PRUNED_TO_EMPTY", "NUMBER"))
        .build()
        .0,
    )
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
const SENTINEL_ROLE_ARN: &str = "arn:aws:iam::123456789012:role/sentinel-role";
const SENTINEL_EXTERNAL_ID: &str = "SENTINEL_EXTERNAL_ID_VALUE";
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
    request: PushdownRequestBuilder,
    logical_schema: Vec<LogicalField>,
    scan_storage: &ScanStorage,
) -> Result<String, UdfError> {
    let (request, _) = request.build();
    let result = dispatch_planned(
        &request,
        &column_types(&guard_col_types()),
        logical_schema,
        scan_storage,
    )?;
    Ok(result["sql"]
        .as_str()
        .expect("pushdown response must carry a sql field")
        .to_string())
}

fn row_scan_body() -> PushdownRequestBuilder {
    guard_events().column(col("REGION")).column(col("AMOUNT"))
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

    let role_creds = ConnectionCreds {
        aws_assume_role_arn: Some(SENTINEL_ROLE_ARN.into()),
        aws_external_id: Some(SENTINEL_EXTERNAL_ID.into()),
        ..sentinel_creds(false)
    };
    let role_effective = sentinel_effective_backend(&role_creds);
    let role_storage = scan_storage_for(
        &role_creds,
        SENTINEL_CONNECTION_NAME,
        true,
        &role_effective,
        Some(&key),
    )
    .expect("role selection");
    let role_sql = dispatch_result_for_body(row_scan_body(), Vec::new(), &role_storage)
        .expect("role dispatch");
    assert!(role_sql.contains("\"sealed\":{\"name\":"), "{role_sql}");
    assert!(!role_sql.contains("\"connection\":{"), "{role_sql}");
    let common: Json = serde_json::from_str(common_arg_literal(&role_sql)).unwrap();
    let selected: ScanStorage = serde_json::from_value(common["storage"].clone()).unwrap();
    let ScanStorage::Sealed { payload, .. } = &selected else {
        panic!("expected Sealed, got {selected:?}");
    };
    assert_eq!(&unseal_storage(payload, &key).unwrap(), &role_effective);
    assert_no_sentinel_secret_leaked(&role_sql);
    assert!(!role_sql.contains(SENTINEL_EXTERNAL_ID), "{role_sql}");
}
