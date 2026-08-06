use super::test_support::*;
use super::*;
use crate::scan::spec::{CommonScanSpec, FileEntry, ScanSpec, StorageProps};

// ---------------------------------------------------------------------------
// Task 4.4 — catalog-auth secrets never in ScanSpec
//
// Relocated from the former `pushdown/credentials.rs` when that module moved
// into `lakehouse-catalog`: the assertion is about the ENGINE's scan-spec
// serialization, and the catalog crate must not name `ScanSpec`,
// `CommonScanSpec`, or `FileEntry`. The four vended sentinels it reads are
// re-declared here with the same literal values the crate's own
// `test_support` uses, so both sides' assertions stay comparable.
// ---------------------------------------------------------------------------

const VENDED_AK: &str = "VENDED_AK_SENTINEL";
const VENDED_SK: &str = "VENDED_SK_SENTINEL";
const VENDED_TOK: &str = "VENDED_TOKEN_SENTINEL";
const VENDED_REGION: &str = "eu-west-2";

/// Scenario: Catalog auth props are never placed in any scan spec, even when
/// `use_vended_credentials` is enabled and vended creds are in the storage.
///
/// The ScanSpec must carry ONLY S3 storage credentials (vended or static).
/// Auth fields (`token`, `client_secret`, etc.) must never appear in the JSON.
#[test]
fn catalog_auth_secrets_never_in_scan_spec_with_vending() {
    // Build a spec with VENDED storage credentials (simulating what
    // resolve_file_list returns after vended extraction).
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
            emit_exa_types: vec!["DECIMAL(20,0)".into()],
            storage: vended_storage,
            ..Default::default()
        },
        files: vec![FileEntry::new(
            "s3://warehouse/db/events/part-00000.parquet",
            1,
        )],
    };

    let json = spec.to_json();

    // Auth field NAMES must never appear as JSON keys in the serialized spec.
    // Check for the exact key pattern `"<field>":` to avoid false-positives
    // from legitimate substrings (e.g. `"session_token"` contains `"token"`).
    for field in [
        "\"token\":",
        "\"credential\":",
        "\"client_id\":",
        "\"client_secret\":",
        "\"oauth2_server_uri\":",
        "\"oauth2-server-uri\":",
        // scope is too short and appears in storage endpoint strings, so it
        // is checked by key name only, above, not by a sentinel value.
    ] {
        assert!(
            !json.contains(field),
            "ScanSpec JSON must not carry auth field key '{field}': {json}"
        );
    }

    // Vended credentials MUST be present in the storage block.
    assert!(
        json.contains(VENDED_AK),
        "vended access_key must be in storage: {json}"
    );
    assert!(
        json.contains(VENDED_TOK),
        "vended session_token must be in storage: {json}"
    );
}

// ---------------------------------------------------------------------------
// ScanSpec GROUP BY — group-key fragments propagated to the scan spec
// ---------------------------------------------------------------------------

/// Grouped scan spec carries group-key rendered SQL fragments.
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
            storage: sample_storage(),
            ..Default::default()
        },
        files: vec![FileEntry::new("s3://w/f0.parquet", 1)],
    };
    let json = spec.to_json();
    let back = ScanSpec::from_json(&json).expect("must round-trip");
    let keys = back.common.group_keys.expect("group_keys must be present");
    assert_eq!(keys, group_keys, "group_keys must survive spec round-trip");
}

/// Scenario: A LIKE-only filter still yields a valid `ScanSpec.filter` (DataFusion
/// evaluates it) while `to_iceberg_predicate` returns `None` (no file pruning).
///
/// This confirms the correctness invariant: LIKE is not prunable but remains
/// fully enforced by DataFusion.
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

    // DataFusion path must still yield Some (LIKE is translatable to DataFusion SQL).
    let df_filter = render_df_filter_safe(&filter_json);
    assert!(
        df_filter.is_some(),
        "LIKE filter must still produce a DataFusion SQL string: {df_filter:?}"
    );

    // Iceberg path must be None — LIKE is not soundly prunable.
    let iceberg_pred = to_iceberg_predicate(&filter_json, &schema);
    assert!(
        iceberg_pred.is_none(),
        "LIKE filter must produce no Iceberg predicate"
    );
}

/// Wiring sanity: the WHERE-clause filter chain composes
/// `string_function_arg_type_guard` and `rewrite_decimal_stringifications` between
/// `like_subject_type_guard` and `render_df_filter_safe`, so a
/// `LENGTH(<DECIMAL column>) > 5` predicate renders with Exasol's trailing-zero-trim
/// form wrapping the column EXACTLY ONCE (issue #211's headline COUNT-divergence
/// repro) — NOT a bare `character_length("C_DECIMAL_A")` over DataFusion's untrimmed
/// decimal→string, and NOT a double-wrapped trim. `string_function_arg_type_guard`
/// coerces `LENGTH`'s bare DECIMAL argument into a `decimal_to_varchar_exasol` node
/// first, so by the time `rewrite_decimal_stringifications` runs, the argument is no
/// longer a bare column and its own CONCAT/LENGTH-specific DECIMAL handling is a
/// no-op — a composition `string_function_arg_type_guard`'s own unit tests cannot
/// observe, since `rewrite_decimal_stringifications` is only chained after it here.
/// Calls the same pipeline function `handle_pushdown` calls
/// (`apply_type_rewrites`, then `render_df_filter_safe`) on the
/// DataFusion-bound filter tree.
///
/// Mirrors only the NO-DECLINE half of production's filter pipeline:
/// `handle_pushdown` classifies through `classify_where_filter`, which routes a
/// DECLINED filter to the qualified single-table wrapper rather than into the scan
/// spec. This fixture renders, so the mirror and production agree.
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

/// Exhaustive coverage: a DECIMAL column in a NON-stringifying WHERE
/// filter context (`c_decimal_a > 5`, a `predicate_greater` — not a stringifier)
/// renders EXACTLY as before this fix through the same pipeline function
/// (`apply_type_rewrites`) as
/// `where_filter_decimal_stringification_rewritten_to_trim` — the DECIMAL column
/// stays a bare, unwrapped column reference, proving the WHERE-path wiring doesn't
/// over-wrap a non-stringifying context. `predicate_greater` is not a
/// `function_scalar`, so `string_function_arg_type_guard` has nothing to dispatch on
/// here and the rendering is byte-identical to before this guard was wired in.
///
/// Mirrors only the NO-DECLINE half of production's filter pipeline:
/// `handle_pushdown` classifies through `classify_where_filter`, which routes a
/// DECLINED filter to the qualified single-table wrapper rather than into the scan
/// spec. This fixture renders, so the mirror and production agree.
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

/// `UPPER(c_decimal_a) = 'X'` is a `predicate_equal`, whose `function_scalar` sits
/// under `left`. `string_function_arg_type_guard`'s post-order recursion — sharing
/// `rewrite_expr_tree`'s broad curated field list with `rewrite_decimal_stringifications`
/// — reaches it there, coercing the DECIMAL argument into the trimmed
/// `decimal_to_varchar_exasol` form through the same pipeline function
/// `handle_pushdown` calls (issue #210).
///
/// Mirrors only the NO-DECLINE half of production's filter pipeline:
/// `handle_pushdown` classifies through `classify_where_filter`, which routes a
/// DECLINED filter to the qualified single-table wrapper rather than into the scan
/// spec. This fixture renders, so the mirror and production agree.
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

/// `UPPER(c_double) = 'X'` must decline through the same pipeline function
/// `handle_pushdown` calls: DOUBLE PRECISION has no safe cast-to-text form that
/// matches Exasol's own conversion (same reasoning as `guard_like_subject`'s
/// BOOLEAN/DOUBLE/TIMESTAMP declines), so the whole filter is omitted rather than
/// pushed with a possibly-wrong text comparison (issue #210).
///
/// Mirrors only the SCAN-SPEC half of production's filter pipeline. The decline
/// keeps the predicate out of the scan spec — still true — but production no longer
/// OMITS it: there is no Exasol-side backstop, so `classify_where_filter` hands the
/// original tree to the qualified single-table wrapper, which applies it in its own
/// `WHERE`. That half is pinned by
/// `declined_filter_routes_every_dispatch_shape_to_qualified_wrapper`.
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

/// `UPPER(c_decimal_a) LIKE '1%'` proves the new guard's coercion reaches INSIDE a
/// LIKE subject that `like_subject_type_guard`'s own `guard_like_subject` leaves
/// completely untouched: the LIKE subject here is a `function_scalar` (`UPPER`), not
/// a bare `column`, so `guard_like_subject`'s bare-column dispatch has nothing to do
/// and passes the node through unchanged. `string_function_arg_type_guard` then
/// coerces the DECIMAL argument nested inside that same `UPPER` call (issue #210).
///
/// Mirrors only the NO-DECLINE half of production's filter pipeline:
/// `handle_pushdown` classifies through `classify_where_filter`, which routes a
/// DECLINED filter to the qualified single-table wrapper rather than into the scan
/// spec. This fixture renders, so the mirror and production agree.
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

/// Regression (#207 blind spot), through the same pipeline function
/// `handle_pushdown` calls: a DECIMAL-typed LIKE buried inside a
/// `function_scalar_case`'s `arguments`, itself nested under `predicate_equal`'s
/// `left`, must decline the whole filter — a `LIKE` at this non-junction position
/// is type-guarded like any other.
///
/// Mirrors only the SCAN-SPEC half of production's filter pipeline. The decline
/// keeps the predicate out of the scan spec — still true — but production no longer
/// OMITS it: there is no Exasol-side backstop, so `classify_where_filter` hands the
/// original tree to the qualified single-table wrapper, which applies it in its own
/// `WHERE`. That half is pinned by
/// `declined_filter_routes_every_dispatch_shape_to_qualified_wrapper`.
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

/// Scenario: Catalog auth props — and the whole catalog block — are never placed
/// in any scan spec.
///
/// The UDF-boundary secret invariant: auth lives on `ConnectionCreds` and is
/// consumed only in the planning-layer catalog build. A `ScanSpec` (serialized
/// for the UDF boundary) must carry no catalog block at all, none of the auth
/// field NAMES, nor any auth VALUE — the scan UDF never calls the catalog.
#[test]
fn scan_spec_carries_no_catalog_block() {
    // Distinctive sentinels: any of these surfacing in the serialized spec is a leak.
    const TOKEN_SENTINEL: &str = "TOKEN_SENTINEL_VALUE";
    const SECRET_SENTINEL: &str = "CLIENT_SECRET_SENTINEL_VALUE";
    const OAUTH_URI_SENTINEL: &str = "https://oauth-uri-sentinel.example/token";
    const SCOPE_SENTINEL: &str = "SCOPE_SENTINEL_VALUE";

    // Build a spec exactly as handle_pushdown does — auth creds exist but are
    // NEVER threaded into ScanSpec (it has no auth fields by construction).
    let spec = ScanSpec {
        common: CommonScanSpec {
            projection: vec!["ID".into(), "NAME".into()],
            filter: Some("(\"ID\" > 10)".into()),
            limit: Some(100),
            emit_exa_types: vec!["DECIMAL(20,0)".into(), "VARCHAR(2000000)".into()],
            storage: sample_storage(),
            ..Default::default()
        },
        files: vec![FileEntry::new(
            "s3://warehouse/db/events/part-00000.parquet",
            1,
        )],
    };

    let json = spec.to_json();

    // The dropped `catalog` block must not appear in the full spec nor the
    // shard-invariant common blob (the scan UDF never touches the catalog).
    assert!(
        !json.contains("catalog"),
        "ScanSpec JSON must not carry a catalog block: {json}"
    );
    assert!(
        !spec.to_common_json().contains("catalog"),
        "common blob must not carry a catalog block: {}",
        spec.to_common_json()
    );

    // No auth field NAMES (planning-layer concepts) in the serialized spec.
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

    // No auth VALUES, even if a future refactor wired creds in by mistake.
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

    // The storage block carries only the S3 storage credentials, exactly as
    // in the established credential flows.
    assert!(
        json.contains("minioadmin"),
        "storage S3 creds must still be present: {json}"
    );
}

// ---------------------------------------------------------------------------
// Task 3.2 — Pushdown spec carries logical schema field-ids
// ---------------------------------------------------------------------------

/// Scenario (pushdown-planning): A pushdown request produces a scan spec whose
/// `logical_schema` carries the expected field-ids, current names, and nullability.
///
/// Builds an in-memory Iceberg schema and verifies that `build_logical_schema`
/// produces a `Vec<LogicalField>` with the correct field-id, name, arrow_type
/// tag, and nullable flag for each field. This covers: required field (nullable=false),
/// optional field (nullable=true), and multiple Iceberg type families.
#[test]
fn pushdown_carries_logical_schema_in_common_arg() {
    use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};
    use std::sync::Arc;

    // Construct an Iceberg schema with 4 fields covering required, optional,
    // and several type families.
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

    // Field 1: required Int → nullable=false, arrow_type="int32"
    assert_eq!(logical[0].field_id, 1);
    assert_eq!(logical[0].name, "id");
    assert_eq!(logical[0].arrow_type, "int32");
    assert!(
        !logical[0].nullable,
        "required field must have nullable=false"
    );

    // Field 2: optional Double → nullable=true, arrow_type="float64"
    assert_eq!(logical[1].field_id, 2);
    assert_eq!(logical[1].name, "score");
    assert_eq!(logical[1].arrow_type, "float64");
    assert!(
        logical[1].nullable,
        "optional field must have nullable=true"
    );

    // Field 3: required String → nullable=false, arrow_type="utf8"
    assert_eq!(logical[2].field_id, 3);
    assert_eq!(logical[2].name, "label");
    assert_eq!(logical[2].arrow_type, "utf8");
    assert!(!logical[2].nullable);

    // Field 4: optional Decimal(18,4) → nullable=true, arrow_type="decimal128(18,4)"
    assert_eq!(logical[3].field_id, 4);
    assert_eq!(logical[3].name, "amount");
    assert_eq!(logical[3].arrow_type, "decimal128(18,4)");
    assert!(logical[3].nullable);

    // Verify round-trip through ScanSpec: logical_schema survives JSON serde.
    let spec = ScanSpec {
        common: CommonScanSpec {
            logical_schema: logical.clone(),
            storage: sample_storage(),
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

    // The logical schema is a shard-invariant field, so it must be carried in the
    // common (arg 0) blob — the scan UDF reads it identically for every shard.
    let common_json = spec.to_common_json();
    let common_back = crate::scan::spec::CommonScanSpec::from_json(&common_json).unwrap();
    assert_eq!(
        common_back.logical_schema, logical,
        "logical_schema must be carried in the common arg"
    );
}

// ---------------------------------------------------------------------------
// Task 3.1 — build_logical_schema encodes the Iceberg initial-default
// (Iceberg column-projection rule 3), once per query, into the scan spec.
// ---------------------------------------------------------------------------

/// The VS encodes each field's Iceberg `initial-default` once per query into
/// the scan spec: a PRIMITIVE required-with-default and a PRIMITIVE
/// nullable-with-default each carry their default as the raw scalar text keyed
/// to the field's Arrow-type tag.
#[test]
fn build_logical_schema_encodes_primitive_initial_default() {
    use iceberg::spec::{Literal, NestedField, PrimitiveType, Schema, Type};
    use std::sync::Arc;

    let schema = Schema::builder()
        .with_schema_id(1)
        .with_fields(vec![
            // Required (nullable=false) Long with an initial-default.
            Arc::new(
                NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long))
                    .with_initial_default(Literal::long(7)),
            ),
            // Nullable (optional) String with an initial-default.
            Arc::new(
                NestedField::optional(2, "note", Type::Primitive(PrimitiveType::String))
                    .with_initial_default(Literal::string("hi")),
            ),
        ])
        .build()
        .unwrap();

    let logical = build_logical_schema(&schema);

    assert_eq!(logical.len(), 2);

    // Required-with-default encodes the raw i64 scalar as decimal text.
    assert_eq!(logical[0].field_id, 1);
    assert!(!logical[0].nullable, "required field must be non-nullable");
    assert_eq!(logical[0].arrow_type, "int64");
    assert_eq!(
        logical[0].initial_default.as_deref(),
        Some("7"),
        "required-with-default must encode its default"
    );

    // Nullable-with-default encodes the string value verbatim.
    assert_eq!(logical[1].field_id, 2);
    assert!(logical[1].nullable, "optional field must be nullable");
    assert_eq!(logical[1].arrow_type, "utf8");
    assert_eq!(
        logical[1].initial_default.as_deref(),
        Some("hi"),
        "nullable-with-default must encode its default"
    );
}

/// A field with NO `initial-default` encodes no default (`None`).
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

/// A NON-primitive (struct) `initial-default` encodes NO default: Exasol has no
/// struct type (it surfaces as JSON-fallback VARCHAR), so the default is dropped
/// and the column falls through to NULL / required-error downstream — a
/// deliberate trade-off, not a silent gap.
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

/// `write-default` is never read: a field carrying ONLY a `write-default`
/// (no `initial-default`) encodes `None` — writes are irrelevant to reads.
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

/// The encoded default form is credential-free: it is a bare scalar value, so
/// the serialized `LogicalField` carrying it contains no storage credential.
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

    // Serializing the default carrier introduces no credential material — the
    // encoding is a bare scalar, never a connection/storage blob.
    let json = serde_json::to_string(&logical).unwrap();
    for marker in ["access_key", "secret_key", "session_token", "endpoint"] {
        assert!(
            !json.contains(marker),
            "encoded default carrier must be credential-free, found '{marker}': {json}"
        );
    }
}

/// A default-less schema round-trips unchanged: every `LogicalField` carries
/// `None`, the field is absent from the serialized JSON, and a spec authored
/// before the field existed deserializes identically (backward-compatible).
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
            storage: sample_storage(),
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

// ---------------------------------------------------------------------------
// Declined-ORDER-BY hidden sort-key columns (issues #225 / #189)
// ---------------------------------------------------------------------------

/// The fixed four-column `EVENTS` universe every guard test projects against
/// (mirrors `dispatch_golden`'s `base_col_types`).
fn guard_col_types() -> Vec<(String, String)> {
    vec![
        ("REGION".to_string(), "VARCHAR(2000000)".to_string()),
        ("NAME".to_string(), "VARCHAR(2000000)".to_string()),
        ("AMOUNT".to_string(), "DECIMAL(18,2)".to_string()),
        ("ID".to_string(), "DECIMAL(20,0)".to_string()),
    ]
}

/// Wrap a `pushdownRequest` body with the fixed `EVENTS` `involvedTables` block
/// (mirrors `dispatch_golden::events_request`).
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

/// Drive `build_dispatch_sql` — the real dispatcher, exactly as `dispatch_golden`
/// exercises it — for `request`/`proj_cols`/`proj_types`, returning the `sql`
/// field of its pushdown response. `has_order_by` is always `true`: every guard
/// test pushes an ORDER BY.
///
/// `projection_widened` is `extract_projection`'s widening signal for the
/// `proj_cols`/`proj_types` pair — the flag the dispatcher routes on (#196). The
/// declined-`ORDER BY` guard tests all pass `false`; the two widening-routing
/// tests pass the same inputs under both values.
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

/// [`guard_dispatch_sql`] WITHOUT the success expectation, for the decline
/// assertions: an unrenderable pushed sort key is a `User` error, not SQL.
///
/// `has_order_by` is DERIVED here via the production `order_by_present` rather
/// than hardcoded, so a fixture carrying no `orderBy` exercises the real
/// non-declined route.
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
        &sample_storage(),
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

// ---------------------------------------------------------------------------
// Declined WHERE filter self-applied in the adapter's own SQL (issue #279)
// ---------------------------------------------------------------------------

/// `AMOUNT LIKE '1%'` over the guard fixture's DECIMAL column — the live-verified
/// shape `like_subject_type_guard` declines, so `apply_type_rewrites` yields `None`
/// and the scan spec can carry no filter at all, while Exasol renders it fine.
fn declined_like_on_decimal() -> Json {
    serde_json::json!({
        "type": "predicate_like",
        "expression": {"type": "column", "name": "AMOUNT", "tableName": "EVENTS"},
        "pattern": {"type": "literal_string", "value": "1%"},
    })
}

/// Drive the real `build_dispatch_sql` over the fixed `EVENTS` fixture for a
/// `pushdownRequest` body, deriving EVERY dispatch input through the same
/// production helpers `handle_pushdown` uses — `extract_projection`,
/// `classify_where_filter`, `extract_limit`, `order_by_present` — so this harness
/// cannot drift from the pipeline it exercises. The logical schema is empty, which
/// declines the bounded top-N unconditionally: the decline route is asserted to win
/// over the shapes the classifier would otherwise pick, not to depend on them.
fn declined_dispatch_sql(pushdown_req_body: Json) -> String {
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
        &sample_storage(),
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

/// Scenario (pushdown-declined-filter-self-apply): a declined WHERE filter routes
/// EVERY single-table dispatch shape to the qualified wrapper, which applies the
/// predicate itself. Asserted over the three shapes whose renderings otherwise
/// diverge most — the bare row scan, the grouped partial/merge aggregate, and the
/// ordered top-N — because the decline route sits AHEAD of the routing classifier,
/// which is exactly what makes ONE route serve all five shapes.
///
/// Each shape asserts both halves of the guarantee: the predicate appears in the
/// wrapper's `WHERE`, and the fan-out scan spec carries no `"filter"` — applied
/// exactly once, never twice and never nowhere.
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
        let sql = declined_dispatch_sql(body);
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

/// A filter that renders trivially true is still OMITTED, with no wrapper: the
/// three outcomes `None` used to collapse are absent, trivially true, and declined,
/// and only the third needs self-applying. `classify_where_filter` hands back
/// neither a scan filter nor a declined tree here, so the request keeps the
/// wrapper-free fast scan.
#[test]
fn trivially_true_filter_omitted_without_wrapper() {
    let trivially_true = serde_json::json!({"type": "literal_bool", "value": true});
    let (filter, declined) = classify_where_filter(Some(&trivially_true), &guard_col_types());
    assert!(
        filter.is_none() && declined.is_none(),
        "a trivially-true filter is neither pushed nor declined: {filter:?} {declined:?}"
    );

    let sql = declined_dispatch_sql(serde_json::json!({
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

/// A `SELECT *` request with a declined filter projects the FULL base row, not just
/// the filter's columns. This shape reaches the qualified wrapper ONLY through the
/// decline route, and narrowing collects only the columns the rendered clauses NAME —
/// for a request with no `selectList` that is `AMOUNT` alone, which Exasol rejects
/// positionally (`04000` "Expected number of columns is 4 but pushdown query has 1").
/// `referenced_column_projection`'s no-select-list arm is what keeps both the inner
/// scan and the outer select list at the full base row, in `col_types` order.
///
/// Live-verified wire form: Exasol OMITS the `selectList` key for `SELECT *` (and
/// still sends a full-row `selectListDataTypes` beside it). The sibling test
/// `no_select_list_wire_forms_all_keep_the_full_base_row` pins the tolerated
/// variants, so a future Exasol that sends `[]` or `null` instead lands on the same
/// arm.
#[test]
fn declined_filter_with_absent_select_list_projects_full_row() {
    let sql = declined_dispatch_sql(serde_json::json!({
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

/// The counterpart: a declined filter beside a REAL select list KEEPS the
/// referenced-column narrowing (#160). The full-row projection is owed to the
/// `SELECT *` shape alone, not to the decline — so a request that names its columns
/// ships only the select list's and the filter's, never the whole row.
///
/// This is the route where narrowing matters most: the fan-out carries no filter (the
/// predicate is applied in the outer wrapper), so every row of the table crosses the
/// UDF boundary and column width is the only remaining lever.
#[test]
fn declined_filter_with_a_real_select_list_keeps_the_narrowing() {
    let sql = declined_dispatch_sql(serde_json::json!({
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

/// A DataFusion render decline changes what the ADAPTER renders, never what Iceberg
/// manifest pruning sees. `classify_where_filter` hands back the ORIGINAL,
/// un-rewritten tree — the very tree `handle_pushdown` forwards to
/// `resolve_file_list` — so a still-prunable conjunct sitting beside the declined
/// one keeps pruning exactly as many files as before.
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
    // Three conjuncts, each carrying its own load. `id > 5` is prunable and must
    // survive. `SECOND(ts, 3)` is refused by the DataFusion dialect on arity while
    // Exasol renders it — the render-decline cause, distinct from the type-guard
    // decline the LIKE fixtures above exercise. `LENGTH(amount) > 5` is REWRITTEN
    // by `rewrite_decimal_stringifications` into the Exasol trim form, so the
    // rewritten tree differs from this one by value and the equality assertion
    // below genuinely discriminates the original from the rewritten tree.
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
             tree resolve_file_list prunes with"
    );
    let pred = to_iceberg_predicate(&filter, &schema)
        .expect("the prunable conjunct must still yield an Iceberg predicate");
    assert!(
        format!("{pred}").contains("id"),
        "pruning must keep the prunable conjunct even though the sibling conjunct \
             declined: {pred}"
    );
}

/// Scenario (pushdown-planning-capability-extensions, issues #225 / #189): a
/// literal-only select list (`SELECT 1 FROM EVENTS`) with an `ORDER BY` on a column
/// the derived projection does not emit (`NAME`) APPENDS that sort key to the scan
/// as a HIDDEN column, and the wrapper names only the visible item explicitly.
///
/// This replaces the former full-base-row widening (issue #190), which forced the
/// scan's emitted set and the query's visible set equal and therefore returned all
/// four base columns where Exasol positionally expects the derived projection's one
/// — `sqlCode 04000 "Expected number of columns is 1 but pushdown query has N"`
/// (#225). The `REGION` / `AMOUNT` / `"ID"` absence assertions are what pin that.
///
/// `logical_schema` is deliberately EMPTY so `detect_topn` declines regardless of
/// the projection (it requires a logical-schema entry per sort key), isolating the
/// extension + wrapper shape from the top-N-match decision. That also makes this
/// test order-blind by construction — the extend-after-`detect_topn` invariant is
/// pinned separately by `declined_order_by_extension_runs_after_topn_detection`.
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

    // The scan spec APPENDS the sort key AFTER the original expression item, so the
    // per-shard scan actually emits the column the outer ORDER BY binds against.
    assert!(
        sql.contains(r#""projection":[{"expr":"1"},"NAME"]"#),
        "sort key NAME must be APPENDED to the derived projection: {sql}"
    );
    assert!(
        sql.contains(r#"EMITS ("_LH_PROJ_0" DECIMAL(1,0), "NAME" VARCHAR(2000000))"#),
        "EMITS must carry the visible expression column plus the hidden sort key: {sql}"
    );
    // One visible column, matching the one-item derived projection: the wrapper's
    // list is joined immediately ahead of ` FROM (`, so this pins the exact arity.
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

/// Scenario (pushdown-planning-capability-extensions, issues #225 / #189), the
/// bare-column shape: `SELECT name FROM EVENTS ORDER BY id` — one bare-projected
/// column, an `ORDER BY` on a DIFFERENT unprojected column, no `LIMIT`.
///
/// The scan's emitted set and the query's visible set are two different sets:
/// `"ID"` is EMITTED (so the outer `ORDER BY` can bind it) yet absent from the
/// visible select list, so the returned arity stays 1 — what Exasol validates
/// positionally. `SELECT *` would return 2 and be rejected with `04000`.
///
/// The absent `LIMIT` is what makes `detect_topn` decline here (a top-N needs a
/// bound), so no `logical_schema` entry is required.
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

/// Scenario (pushdown-planning-capability-extensions): hidden sort-key columns are
/// appended AT MOST ONCE. `ORDER BY name, id, name, id` over a projection that
/// already bare-projects `NAME` exercises BOTH dedupe paths in one fixture:
/// `NAME` is already emitted so it is never appended, and `ID` — named by two
/// sort keys — is appended exactly ONCE, because the membership test re-scans
/// `proj_cols` as it grows. A repeated EMITS identifier is a duplicate-column
/// error, so "not twice" is the assertion that matters.
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

/// Companion scenario: when every pushed sort key IS already a bare-projected
/// column the extension is INERT — nothing appended, nothing widened — and the
/// legitimately matched bounded top-N still forms exactly as before.
///
/// The matched path never reaches the declined-wrapper code at all: it renders
/// `proj_cols` directly as the FINAL visible EMITS with no wrapping
/// `SELECT … FROM (`, and carries the sort keys plus the limit into the per-shard
/// common blob. That is precisely why the extension must not run ahead of
/// `detect_topn` — a hidden column would leak straight into this path's result.
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
        field_id: 2,
        name: "NAME".to_string(),
        arrow_type: "utf8".to_string(),
        nullable: true,
        initial_default: None,
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
    // The fan-out IS the outermost query: no declined-path wrapper around it.
    assert!(
        sql.starts_with("SELECT LAKEHOUSE_SCAN(") && !sql.contains(" FROM ("),
        "a matched top-N must not be wrapped in an outer SELECT … FROM (: {sql}"
    );
    // Only the matched path pushes the bounded sort and the limit per shard.
    let common = common_arg_literal(&sql);
    assert!(
        common.contains(r#""order_by":[{"column":"NAME","ascending":true,"nulls_last":true}]"#)
            && common.contains(r#""limit":5"#),
        "a matched top-N must carry the per-shard sort keys and limit: {common}"
    );
}

/// S3 (`build_row_scan_sql`) is unreachable with an offset because the decline
/// (issue #191, fact 5) NULLS `effective_limit` before it ever reaches that
/// builder. Same fixture as
/// `declined_order_by_all_keys_projected_leaves_projection_untouched` — every
/// `detect_topn` guard would MATCH (single table, `NAME` projected as a bare
/// column, a populated non-JSON-fallback logical schema) — except this request
/// carries a NON-ZERO `offset`, which declines the bounded top-N and therefore
/// nulls `effective_limit`: neither the per-shard fan-out nor a bare outer
/// `LIMIT`/`OFFSET` may render ahead of the declined wrapper's own
/// `ORDER BY … LIMIT n OFFSET m` (through the shared `render_limit_offset` seam).
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
        field_id: 2,
        name: "NAME".to_string(),
        arrow_type: "utf8".to_string(),
        nullable: true,
        initial_default: None,
    }];

    let sql = guard_dispatch_sql(
        &request,
        proj_cols,
        proj_types,
        false,
        Some(5),
        logical_schema,
    );

    // The declined wrapper renders the offset window exactly once, on its own
    // ORDER BY — never a bare per-shard/outer LIMIT ahead of it.
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

/// The projection extension runs strictly AFTER `detect_topn` (decision [2]) — the
/// plan's most load-bearing ordering invariant, and one that is SILENT when
/// violated (a mis-ordered implementation reintroduces `04000` with a green suite).
///
/// The fixture deliberately gives `detect_topn` everything it needs to MATCH
/// except a projected sort key: exactly one involved table, `ORDER BY "NAME"` ASC
/// NULLS LAST, a `LIMIT 5`, and a POPULATED `logical_schema` typing `NAME` as
/// `utf8` (not a JSON-fallback type). Only the CALL ORDER decides the outcome:
///
/// - Correct order: `detect_topn` sees the pre-extension `[Expr("1")]`, finds
///   `NAME` unprojected and declines; the declined path then appends `NAME` as a
///   hidden column and renders the wrapper. Nothing per-shard.
/// - Extension first: `proj_cols` would already be `[Expr("1"), Column("NAME")]`,
///   so every remaining `detect_topn` guard passes, the bounded top-N MATCHES,
///   `"order_by"` and `"limit":5` land in the common blob, and NO wrapper is
///   rendered — failing all three assertions below. That path emits `proj_cols` as
///   the FINAL visible EMITS, so the hidden `NAME` would leak into the result too.
///
/// A `detect_topn`-only assertion over the pre-extension projection cannot pin
/// this: it holds whatever the call order (see `topn.rs`'s
/// `unsupported_order_by_shape_declines_topn`). Nor can the sibling tests above —
/// they force the decline via an empty `logical_schema` or an absent `LIMIT`, both
/// order-blind.
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
    // NAME as `utf8`: a native, non-JSON-fallback type, so the JSON-fallback guard
    // would NOT be what declines the top-N had the extension already run.
    let logical_schema = vec![LogicalField {
        field_id: 2,
        name: "NAME".to_string(),
        arrow_type: "utf8".to_string(),
        nullable: true,
        initial_default: None,
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

/// Scenario (pushdown-planning-capability-extensions, issue #198): "An ORDER BY
/// the adapter cannot bound as a top-N remains correctness-safe."
///
/// Exasol DELEGATES a pushed ordering and no longer re-applies its own backstop
/// sort, so the declined row-scan path has exactly two correctness-safe outcomes:
/// render the ordering in FULL, or decline with a `User` error naming the key.
/// Returning SQL that reproduces only PART of the pushed ordering is the
/// silent-wrong-order outcome this guard exists to make unreachable.
///
/// Three facets, and facet (b) is why the guard tests ANY unrenderable element
/// rather than ALL of them:
/// (a) every element unrenderable — both kinds: an expression node NEITHER
///     dialect knows, and a bare `column` node missing its `nullsLast` flag
///     (direction / NULL placement is never silently defaulted). This SUPERSEDES
///     `fix-225`'s "return the unwrapped SQL unchanged" rule for a NON-EMPTY
///     `orderBy`.
/// (b) MIXED — one renderable key and one not. An `all`-shaped guard would pass
///     this through and render a partial ordering, which is precisely the silent
///     corruption; only the unrenderable key's own ordering would be lost, and
///     nothing downstream would notice.
/// (c) ABSENT `orderBy` — unchanged: the unwrapped fan-out, no wrapper, no
///     decline. Nothing was delegated, so nothing must be reproduced.
#[test]
fn declined_order_by_renders_every_reachable_ordering_or_declines() {
    let unrenderable_expression = serde_json::json!({
        "type": "order_by_element",
        "expression": {"type": "no_such_node_type_in_either_dialect"},
        "isAscending": true,
        "nullsLast": true
    });
    // A bare column node whose NULL placement is absent: renderable as an
    // identifier, but not as an ORDER BY element.
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

    // (c) No `orderBy` at all: nothing was delegated, so the fan-out is returned
    // unwrapped and the LIMIT is NOT withheld.
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

// ---------------------------------------------------------------------------
// COUNT(DISTINCT) wrapper limit withholding is dead code (issue #191)
// ---------------------------------------------------------------------------

/// Regression (issue #191, plan `fix-191-order-by-offset`): a lone
/// `COUNT(DISTINCT)` request (Case 1) carrying BOTH a request-level `orderBy`
/// and a request-level LIMIT must render that LIMIT on the outer wrapper.
/// The now-deleted withholding (`let cd_limit = if has_order_by { None } else
/// { limit };`) used to drop the limit in exactly this case — dead code,
/// because Exasol never actually pushes an `orderBy` on an ungrouped
/// aggregate request (fact 7), but the withholding branch fired on ANY
/// `orderBy` this fixture forces regardless of whether Exasol would send one.
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

// ---------------------------------------------------------------------------
// Widened-projection routing at coincidental arity (issues #196 / #234)
// ---------------------------------------------------------------------------

/// A `RequestShape::RowScan` request whose select-list arity EQUALS the base
/// table's column count: four bare `EVENTS` columns, plus an `ORDER BY` the
/// adapter does not match as a bounded top-N. Both routing tests below drive the
/// dispatcher with these identical inputs and differ ONLY in the widening flag.
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

/// The four-column full base row, as `project_columns` returns it when it widens.
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

/// Scenario (pushdown-planning-capability-extensions, issues #196 / #234): a
/// WIDENED derived projection routes to `qualified_single_table_fallback_pushdown`
/// even when its column count COINCIDES with the select-list arity.
///
/// The count comparison this replaced was blind here — four base columns against
/// four select-list items looks like a clean per-item derivation, so the request
/// reached the raw scan path and Exasol rejected the positionally-mismatched types
/// (`04000` "Data type mismatch in column number N", reproduced live on a 10-item
/// select list over a 10-column table). Routing on the producer's own widening
/// signal cannot be fooled by the coincidence.
///
/// The `ORDER BY` also pins the early-return POSITION: the wrapper's own outer
/// `ORDER BY` is what orders the result, so the widened projection never reached
/// `detect_topn` or the declined-`ORDER BY` hidden-sort-key extension.
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

/// The mirror of `dispatch_widened_projection_at_matching_arity_routes_to_wrapper`:
/// the SAME request and the SAME four-item projection with the widening flag CLEAR
/// — a genuine `SELECT region, name, amount, id ... ORDER BY name` — stays on the
/// ordinary scan path and is NOT wrapped in the qualified fallback.
///
/// This pins the signal as load-bearing in BOTH directions: a later `, _`
/// destructuring that swallows the flag, or a hardcoded `true`, fails a host test
/// instead of silently unaccelerating every row scan.
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

// ---------------------------------------------------------------------------
// Parse-before-config ordering — regression coverage
// ---------------------------------------------------------------------------

/// A malformed `catalog.table` identifier against an unreachable `catalog_uri`
/// must fail with `parse_table_ident`'s own error, not a transport error from
/// the unreachable host.
///
/// Proves `handle_pushdown` validates the identifier BEFORE
/// `CatalogSession::resolve` runs the OAuth2 client-credentials grant (the
/// only branch of `resolve_catalog_auth` that makes network contact — the
/// no-auth and static-token branches never touch the network at all, so this
/// test would pass vacuously against a broken build-then-validate ordering
/// unless creds force the OAuth2 branch). `catalog_uri` is a closed local
/// port (`127.0.0.1:1`, connection refused) so a wrongly-ordered
/// implementation fails fast with a transport error instead of hanging.
#[tokio::test]
async fn malformed_table_ident_fails_before_any_catalog_contact() {
    let creds = ConnectionCreds {
        warehouse: "warehouse".into(),
        endpoint: "http://minio:9000".into(),
        region: "us-east-1".into(),
        access_key: "minioadmin".into(),
        secret_key: "minioadmin".into(),
        session_token: None,
        path_style: true,
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
        // No '.' separator: fails `parse_table_ident`'s validation before any
        // catalog HTTP is issued.
        table: "malformed_identifier_with_no_namespace_separator".into(),
    };

    // Two-column universe (non-empty): an empty `columns` array fails in
    // `project_columns` before the code path under test even runs, which
    // would mask the ordering this test proves.
    let request = nq4_request();

    let result = handle_pushdown(
        &request,
        "http://127.0.0.1:1",
        &sample_storage(),
        &catalog,
        None,
        1,
        1,
        1,
        1024,
        1,
        0.6,
        200,
        4,
        1024,
        &creds,
        false,
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

// ---------------------------------------------------------------------------
// CHAR-declared group-key blank padding through the real dispatcher (#192)
// ---------------------------------------------------------------------------

/// A `CAST(NAME AS CHAR(size))` select-list/`groupBy` node, declared with
/// `character_set` so both the plain and the ` ASCII`-suffixed declared type
/// can be driven through the dispatcher.
fn char_cast_key(size: u64, character_set: &str) -> Json {
    serde_json::json!({
        "type": "function_scalar_cast",
        "name": "CAST",
        "dataType": {"type": "CHAR", "size": size, "characterSet": character_set},
        "arguments": [{"type": "column", "name": "NAME"}]
    })
}

/// Wrap a single group key + `COUNT(*)` into the grouped request shape, with
/// the key's declared type at its own `selectListDataTypes` ordinal.
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

/// Wrap a single group key + `COUNT(*)` into the grouped request shape with the
/// key ABSENT from `selectList` (`SELECT COUNT(*) … GROUP BY <key>`), so its
/// declared type is reachable only through its own `groupBy` node.
fn unprojected_char_grouped_request(key: Json) -> Json {
    guard_events_request(serde_json::json!({
        "aggregationType": "group_by",
        "groupBy": [key],
        "selectList": [agg_item("COUNT", None, false)],
        "selectListDataTypes": [{"type": "decimal", "precision": 18, "scale": 0}],
    }))
}

/// The `group_keys` entry the emitted scan spec must carry, escaped exactly as
/// the dispatcher embeds it: JSON-encoded into the spec blob, then wrapped in a
/// SQL string literal (single quotes doubled).
fn embedded_group_keys(fragments: &[String]) -> String {
    let encoded: Vec<String> = fragments
        .iter()
        .map(|f| serde_json::to_string(f).expect("a group-key fragment is JSON-encodable"))
        .collect();
    format!(r#""group_keys":[{}]"#, encoded.join(",")).replace('\'', "''")
}

/// The dispatcher's grouped arm must hand the DataFusion side a BLANK-PADDED
/// copy of a `CHAR(20)`-declared group key, while the outer merge wrapper keeps
/// casting the staging column back to `CHAR(20)`. Without the pad, `'ab'` and
/// `'ab   '` reach the merge as two distinct `GK_0` values and the query returns
/// two rows where Exasol's own `CAST(x AS CHAR(20))` returns one (issue #192).
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

/// The same pad must reach the scan spec when the CHAR-declared key is NOT in
/// the select list (`SELECT COUNT(*) … GROUP BY CAST(NAME AS CHAR(20))`). This
/// shape is strictly more dangerous than the projected one: the outer wrapper
/// has no `CAST("GK_0" AS CHAR(20))` output column, so an unpadded key raises
/// no type mismatch — it just returns a row per trailing-blank variant where
/// Exasol returns one merged row (#192 review finding).
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

/// CONTROL for the unprojected path: a VARCHAR-declared `groupBy` node must
/// reach the scan spec unpadded. The `groupBy` fallback fires here (the node
/// carries a `dataType`), so this proves it resolves the declared type rather
/// than padding every unprojected group key.
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

/// The pad width must survive the ` ASCII` character-set suffix Exasol appends
/// to an ASCII-declared CHAR — the #192 primary shape. A parse that trims a
/// trailing `)` off the declared type would find no width here and ship the key
/// unpadded, reintroducing the wrong-row-count bug for every ASCII CHAR key.
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

/// CONTROL: a VARCHAR-declared group key must reach the scan spec byte-identical
/// to the pre-fix rendering — VARCHAR carries no blank padding, so wrapping it
/// would change grouping semantics for every ordinary string GROUP BY.
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

/// The padded copy goes ONLY to the DataFusion side: `build_grouped_order_by_clause`
/// matches a pushed `orderBy` against the UNPADDED rendered group keys, so a sort
/// on a CHAR-declared group key must still resolve to its output ordinal. Matching
/// against the padded copy instead would make it `Unresolvable` and turn every
/// `ORDER BY` over a CHAR group key into a hard pushdown decline.
///
/// The key is a bare column because `parse_sort_key_element` accepts only bare
/// columns as sort keys — an expression sort key declines for its own, unrelated
/// reason and would not exercise the padded/unpadded split at all.
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
