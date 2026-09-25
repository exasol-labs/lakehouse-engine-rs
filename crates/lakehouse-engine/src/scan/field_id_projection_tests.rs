use super::*;
use crate::scan::spec::{FileEntry, NameMappingEntry, ScanSpec};
use crate::scan::test_support::{inline_resolved, local_file_size, minimal_spec};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::expressions::{CastExpr, Column, Literal};

fn field_with_id(name: &str, dt: DataType, nullable: bool, id: i32) -> Field {
    Field::new(name, dt, nullable).with_metadata(HashMap::from([(
        PARQUET_FIELD_ID_META_KEY.to_string(),
        id.to_string(),
    )]))
}

fn field_no_id(name: &str, dt: DataType, nullable: bool) -> Field {
    Field::new(name, dt, nullable)
}

/// Binding is then embedded-field-id-then-physical-name.
fn bare_resolution() -> FieldIdResolution {
    FieldIdResolution {
        name_mapping: Vec::new(),
        declared_physical_names: HashMap::new(),
        defaults: HashMap::new(),
        nested_members: HashMap::new(),
    }
}

fn resolution_with_mapping(entries: &[(&str, i32)]) -> FieldIdResolution {
    FieldIdResolution {
        name_mapping: entries
            .iter()
            .map(|(name, field_id)| NameMappingEntry {
                name: (*name).to_string(),
                field_id: *field_id,
            })
            .collect(),
        ..bare_resolution()
    }
}

/// Keyed by physical name, as `index_declared_physical_names` builds it.
fn resolution_with_declared_names(entries: &[(&str, &str)]) -> FieldIdResolution {
    FieldIdResolution {
        declared_physical_names: entries
            .iter()
            .map(|(physical, logical)| ((*physical).to_string(), (*logical).to_string()))
            .collect(),
        ..bare_resolution()
    }
}

fn resolution_with_defaults(defaults: &[(&str, ScalarValue)]) -> FieldIdResolution {
    FieldIdResolution {
        defaults: defaults
            .iter()
            .map(|(name, value)| ((*name).to_string(), value.clone()))
            .collect(),
        ..bare_resolution()
    }
}

fn rewrite(
    logical: SchemaRef,
    physical: SchemaRef,
    column: Column,
) -> datafusion::error::Result<Arc<dyn PhysicalExpr>> {
    rewrite_with(logical, physical, bare_resolution(), column)
}

fn rewrite_with(
    logical: SchemaRef,
    physical: SchemaRef,
    resolution: FieldIdResolution,
    column: Column,
) -> datafusion::error::Result<Arc<dyn PhysicalExpr>> {
    let adapter = FieldIdExprAdapterFactory { resolution }
        .create(logical, physical)
        .expect("adapter creation");
    adapter.rewrite(Arc::new(column))
}

fn literal_value(expr: &Arc<dyn PhysicalExpr>) -> Option<ScalarValue> {
    expr.downcast_ref::<Literal>().map(|l| l.value().clone())
}

/// Accepts a bare `Column` or one in an identity cast (fields whose field-id metadata differs).
fn bound_physical_index(expr: &Arc<dyn PhysicalExpr>) -> Option<usize> {
    expr.downcast_ref::<Column>()
        .map(Column::index)
        .or_else(|| {
            expr.downcast_ref::<CastExpr>()
                .and_then(|cast| cast.expr().downcast_ref::<Column>())
                .map(Column::index)
        })
}

/// Scenario: a renamed column binds to the physical column by field-id, not by name.
#[test]
fn resolves_renamed_column_by_field_id() {
    let logical = Arc::new(Schema::new(vec![
        field_with_id("id", DataType::Int64, false, 1),
        field_with_id("rating", DataType::Int64, true, 2),
    ]));
    let physical = Arc::new(Schema::new(vec![
        field_with_id("id", DataType::Int64, false, 1),
        field_with_id("score", DataType::Int64, true, 2),
    ]));

    let result = rewrite(logical, physical, Column::new("rating", 1)).expect("rewrite ok");

    // Types match, so a plain Column at physical index 1 (no cast).
    let col = result
        .downcast_ref::<Column>()
        .expect("renamed column resolves to a Column, no cast");
    assert_eq!(col.index(), 1, "must bind to physical field-id-2 slot");
}

/// Scenario: a type divergence under one field-id is wrapped in a cast.
#[test]
fn casts_on_type_divergence_by_field_id() {
    let logical = Arc::new(Schema::new(vec![field_with_id(
        "amount",
        DataType::Int64,
        true,
        5,
    )]));
    let physical = Arc::new(Schema::new(vec![field_with_id(
        "amt",
        DataType::Int32,
        true,
        5,
    )]));

    let result = rewrite(logical, physical, Column::new("amount", 0)).expect("rewrite ok");
    let cast = result
        .downcast_ref::<CastExpr>()
        .expect("type divergence must produce a cast");
    let inner = cast
        .expr()
        .downcast_ref::<Column>()
        .expect("cast wraps the resolved physical column");
    assert_eq!(inner.index(), 0, "cast must wrap the field-id-5 slot");
}

/// Scenario: a dropped physical column is unreferenced while kept columns still bind.
#[test]
fn ignores_dropped_physical_column() {
    let logical = Arc::new(Schema::new(vec![field_with_id(
        "id",
        DataType::Int64,
        false,
        1,
    )]));
    let physical = Arc::new(Schema::new(vec![
        field_with_id("id", DataType::Int64, false, 1),
        field_with_id("legacy", DataType::Utf8, true, 7),
    ]));

    let result = rewrite(logical, physical, Column::new("id", 0)).expect("rewrite ok");
    let col = result
        .downcast_ref::<Column>()
        .expect("kept column resolves to a Column");
    assert_eq!(col.index(), 0);
}

/// Scenario: the logical Arrow schema carries field-ids, reconstructed types, and declared nullability.
#[test]
fn builds_logical_arrow_schema_with_field_ids() {
    use super::{build_logical_arrow_schema, field_id_of};
    use crate::scan::spec::LogicalField;

    let logical = vec![
        LogicalField {
            field_id: Some(1),
            name: "id".to_string(),
            arrow_type: "int64".to_string(),
            nullable: false,
            initial_default: None,
            nested: None,
            physical_name: None,
        },
        LogicalField {
            field_id: Some(2),
            name: "rating".to_string(),
            arrow_type: "float64".to_string(),
            nullable: true,
            initial_default: None,
            nested: None,
            physical_name: None,
        },
    ];

    let schema = build_logical_arrow_schema(&logical);

    assert_eq!(schema.fields().len(), 2);
    let id = schema.field(0);
    assert_eq!(id.name(), "id");
    assert_eq!(id.data_type(), &DataType::Int64);
    assert!(!id.is_nullable(), "non-nullable must be preserved");
    assert_eq!(field_id_of(id), Some(1), "field-id metadata must be tagged");

    let rating = schema.field(1);
    assert_eq!(rating.name(), "rating");
    assert_eq!(rating.data_type(), &DataType::Float64);
    assert!(rating.is_nullable(), "nullable must be preserved");
    assert_eq!(field_id_of(rating), Some(2));
}

/// Scenario: a logical field carrying no binding key binds by its own name.
#[test]
fn identity_bound_logical_field_carries_no_parquet_field_id_metadata() {
    use super::{build_logical_arrow_schema, field_id_of};
    use crate::scan::spec::LogicalField;

    let logical = vec![
        LogicalField {
            field_id: Some(1),
            name: "by_id".to_string(),
            arrow_type: "int64".to_string(),
            nullable: false,
            initial_default: None,
            nested: None,
            physical_name: None,
        },
        LogicalField {
            field_id: None,
            name: "by_identity".to_string(),
            arrow_type: "utf8".to_string(),
            nullable: true,
            initial_default: None,
            nested: None,
            physical_name: None,
        },
        LogicalField {
            field_id: None,
            name: "by_physical_name".to_string(),
            arrow_type: "utf8".to_string(),
            nullable: true,
            initial_default: None,
            nested: None,
            physical_name: Some("col-abc".to_string()),
        },
    ];

    let schema = build_logical_arrow_schema(&logical);

    assert_eq!(
        field_id_of(schema.field(0)),
        Some(1),
        "a field-id-bound field must stay tagged"
    );
    for index in [1, 2] {
        let field = schema.field(index);
        assert!(
            !field.metadata().contains_key(PARQUET_FIELD_ID_META_KEY),
            "'{}' carries no field-id, so it must carry no field-id metadata: {:?}",
            field.name(),
            field.metadata()
        );
    }
}

/// Scenario: a physical field without an embedded id binds via a matching name-mapping entry.
#[test]
fn name_mapping_resolves_no_field_id_column() {
    let logical = Schema::new(vec![
        field_with_id("id", DataType::Int64, false, 1),
        field_with_id("rating", DataType::Int64, true, 2),
    ]);
    // `score` -> id 2 -> `rating` via name-mapping.
    let physical = Schema::new(vec![field_no_id("score", DataType::Int64, true)]);

    let binding = bind_columns(
        &logical,
        &physical,
        &resolution_with_mapping(&[("score", 2)]),
    );

    assert_eq!(
        binding.renamed_physical.field(0).name(),
        "rating",
        "no-id field must resolve via name-mapping"
    );
    assert!(
        binding.bound_logical_names.contains("rating"),
        "a name-mapped column supplies real values, so it must count as bound"
    );
    assert!(
        !binding.bound_logical_names.contains("id"),
        "a logical field this file does not supply must stay unbound"
    );
}

/// Scenario: an embedded field-id wins over a conflicting name-mapping entry.
#[test]
fn embedded_field_id_wins_over_name_mapping() {
    let logical = Schema::new(vec![
        field_with_id("id", DataType::Int64, false, 1),
        field_with_id("rating", DataType::Int64, true, 2),
    ]);
    let physical = Schema::new(vec![field_with_id("score", DataType::Int64, true, 2)]);

    let binding = bind_columns(
        &logical,
        &physical,
        &resolution_with_mapping(&[("score", 1)]),
    );

    assert_eq!(
        binding.renamed_physical.field(0).name(),
        "rating",
        "embedded id 2 must win over a mapping to id 1"
    );
}

/// Scenario: without name-mapping the physical name is kept, and a matching one counts as bound.
#[test]
fn no_name_mapping_falls_back_to_physical_name() {
    let logical = Schema::new(vec![
        field_with_id("id", DataType::Int64, false, 1),
        field_with_id("rating", DataType::Int64, true, 2),
    ]);
    let physical = Schema::new(vec![
        field_no_id("score", DataType::Int64, true),
        field_no_id("rating", DataType::Int64, true),
    ]);

    let binding = bind_columns(&logical, &physical, &bare_resolution());

    assert_eq!(
        binding.renamed_physical.field(0).name(),
        "score",
        "no mapping must keep the physical name"
    );
    assert_eq!(
        binding.bound_logical_names,
        std::iter::once("rating".to_string()).collect(),
        "only the kept name matching a logical field binds"
    );
}

/// Scenario: name-mapping entries not covering a field leave its physical name unchanged.
#[test]
fn uncovered_name_mapping_falls_back_to_physical_name() {
    let logical = Schema::new(vec![
        field_with_id("id", DataType::Int64, false, 1),
        field_with_id("rating", DataType::Int64, true, 2),
    ]);
    let physical = Schema::new(vec![field_no_id("unknown", DataType::Int64, true)]);

    let binding = bind_columns(
        &logical,
        &physical,
        &resolution_with_mapping(&[("score", 2)]),
    );

    assert_eq!(
        binding.renamed_physical.field(0).name(),
        "unknown",
        "uncovered field must keep the physical name"
    );
    assert!(
        binding.bound_logical_names.is_empty(),
        "a physical field matching no logical name binds nothing"
    );
}

/// Scenario: an embedded field-id absent from the logical schema does not fall through to name-mapping.
#[test]
fn embedded_field_id_absent_from_logical_schema_skips_name_mapping() {
    let logical = Schema::new(vec![
        field_with_id("id", DataType::Int64, false, 1),
        field_with_id("rating", DataType::Int64, true, 2),
    ]);
    let physical = Schema::new(vec![field_with_id("score", DataType::Int64, true, 99)]);

    let binding = bind_columns(
        &logical,
        &physical,
        &resolution_with_mapping(&[("score", 2)]),
    );

    assert_eq!(
        binding.renamed_physical.field(0).name(),
        "score",
        "an unresolvable embedded id must NOT fall through to the name-mapping"
    );
}

/// Scenario: column projection binds by a logical field's declared physical name.
#[test]
fn declared_physical_name_wins_over_a_covering_name_mapping_entry() {
    // Name-mapping also reaches `other` (id 7) from `col-abc`; the declaration must win.
    let logical = Schema::new(vec![
        field_no_id("amount", DataType::Int64, true),
        field_with_id("other", DataType::Int64, true, 7),
    ]);
    let physical = Schema::new(vec![field_no_id("col-abc", DataType::Int64, true)]);
    let resolution = FieldIdResolution {
        declared_physical_names: HashMap::from([("col-abc".to_string(), "amount".to_string())]),
        ..resolution_with_mapping(&[("col-abc", 7)])
    };

    let binding = bind_columns(&logical, &physical, &resolution);

    assert_eq!(
        binding.renamed_physical.field(0).name(),
        "amount",
        "the declared physical name must claim the column over the name-mapping"
    );
    assert!(
        binding.bound_logical_names.contains("amount"),
        "the declaring logical field must count as bound"
    );
    assert!(
        !binding.bound_logical_names.contains("other"),
        "the name-mapped logical field must NOT also claim the column"
    );
}

/// Scenario: a declared physical name claims a field whose embedded id no logical field declares.
#[test]
fn declared_physical_name_claims_a_field_whose_embedded_id_is_unknown() {
    let logical = Schema::new(vec![field_no_id("amount", DataType::Int64, true)]);
    let physical = Schema::new(vec![field_with_id("col-abc", DataType::Int64, true, 42)]);

    let binding = bind_columns(
        &logical,
        &physical,
        &resolution_with_declared_names(&[("col-abc", "amount")]),
    );

    assert_eq!(
        binding.renamed_physical.field(0).name(),
        "amount",
        "an unmatched embedded id must not block the declared-physical-name step"
    );
    assert!(binding.bound_logical_names.contains("amount"));
}

/// Scenario: an identity-bound field binds when present and stays unbound when absent.
#[test]
fn identity_bound_fields_bind_by_their_own_name() {
    let logical = Schema::new(vec![
        field_no_id("id", DataType::Int64, false),
        field_no_id("val", DataType::Int64, true),
        field_no_id("added", DataType::Int64, true),
    ]);
    let physical = Schema::new(vec![
        field_no_id("id", DataType::Int64, false),
        field_no_id("val", DataType::Int64, true),
    ]);

    let binding = bind_columns(&logical, &physical, &bare_resolution());

    assert_eq!(binding.renamed_physical.field(0).name(), "id");
    assert_eq!(binding.renamed_physical.field(1).name(), "val");
    assert!(
        binding.bound_logical_names.contains("id") && binding.bound_logical_names.contains("val"),
        "an identity-bound field the file supplies must bind to real values"
    );
    assert!(
        !binding.bound_logical_names.contains("added"),
        "an identity-bound field the file lacks must stay unbound for the fill seam"
    );
}

/// Scenario: field-id resolution falls back to physical name when a file field carries no field-id.
#[test]
fn field_id_adapter_falls_back_to_name_without_field_id() {
    let logical = Arc::new(Schema::new(vec![
        field_with_id("id", DataType::Int64, false, 1),
        field_with_id("rating", DataType::Int64, true, 2),
    ]));
    let physical = Arc::new(Schema::new(vec![
        field_no_id("id", DataType::Int64, false),
        field_no_id("rating", DataType::Int64, true),
    ]));

    let result = rewrite(logical, physical, Column::new("rating", 1)).expect("rewrite ok");
    let bound_index = result
        .downcast_ref::<Column>()
        .map(Column::index)
        .or_else(|| {
            result
                .downcast_ref::<CastExpr>()
                .and_then(|c| c.expr().downcast_ref::<Column>())
                .map(Column::index)
        });
    assert_eq!(
        bound_index,
        Some(1),
        "name fallback must bind to the `rating` slot"
    );
}

/// Scenario: added nullable column absent from an older file is NULL-filled.
#[test]
fn field_id_adapter_null_fills_added_nullable_column() {
    let logical = Arc::new(Schema::new(vec![
        field_with_id("id", DataType::Int64, false, 1),
        field_with_id("note", DataType::Utf8, true, 9),
    ]));
    let physical = Arc::new(Schema::new(vec![field_with_id(
        "id",
        DataType::Int64,
        false,
        1,
    )]));

    let result = rewrite(logical, physical, Column::new("note", 1)).expect("rewrite ok");
    let lit = result
        .downcast_ref::<Literal>()
        .expect("added nullable missing column becomes a NULL literal");
    assert_eq!(*lit.value(), ScalarValue::Utf8(None));
}

/// Scenario: added required column missing from an older file errors cleanly.
#[test]
fn field_id_adapter_errors_on_missing_required_column() {
    let logical = Arc::new(Schema::new(vec![
        field_with_id("id", DataType::Int64, false, 1),
        field_with_id("mandatory", DataType::Utf8, false, 9),
    ]));
    let physical = Arc::new(Schema::new(vec![field_with_id(
        "id",
        DataType::Int64,
        false,
        1,
    )]));

    let err = rewrite(logical, physical, Column::new("mandatory", 1))
        .expect_err("missing required column must error");
    let text = err.to_string();
    assert!(
        text.contains("mandatory") && text.contains("missing"),
        "error must name the missing required column: {text}"
    );
}

/// Scenario: an absent field with an `initial-default` emits it, whether required or nullable.
#[test]
fn absent_field_with_initial_default_emits_default_literal() {
    let logical = Arc::new(Schema::new(vec![
        field_with_id("id", DataType::Int64, false, 1),
        field_with_id("required_added", DataType::Utf8, false, 9),
    ]));
    let physical = Arc::new(Schema::new(vec![field_with_id(
        "id",
        DataType::Int64,
        false,
        1,
    )]));
    let result = rewrite_with(
        Arc::clone(&logical),
        Arc::clone(&physical),
        resolution_with_defaults(&[(
            "required_added",
            ScalarValue::Utf8(Some("req-default".to_string())),
        )]),
        Column::new("required_added", 1),
    )
    .expect("required-absent-with-default must not error");
    assert_eq!(
        literal_value(&result),
        Some(ScalarValue::Utf8(Some("req-default".to_string()))),
        "a required absent field with a default must emit Literal(default), not error"
    );

    let logical = Arc::new(Schema::new(vec![
        field_with_id("id", DataType::Int64, false, 1),
        field_with_id("nullable_added", DataType::Int64, true, 9),
    ]));
    let result = rewrite_with(
        logical,
        physical,
        resolution_with_defaults(&[("nullable_added", ScalarValue::Int64(Some(-1)))]),
        Column::new("nullable_added", 1),
    )
    .expect("nullable-absent-with-default must not error");
    assert_eq!(
        literal_value(&result),
        Some(ScalarValue::Int64(Some(-1))),
        "a nullable absent field with a default must emit Literal(default), not NULL"
    );
}

/// Scenario: an absent nullable field without its own default NULL-fills, ignoring unrelated defaults.
#[test]
fn absent_nullable_without_default_is_null_filled() {
    let logical = Arc::new(Schema::new(vec![
        field_with_id("id", DataType::Int64, false, 1),
        field_with_id("note", DataType::Utf8, true, 9),
    ]));
    let physical = Arc::new(Schema::new(vec![field_with_id(
        "id",
        DataType::Int64,
        false,
        1,
    )]));
    let result = rewrite_with(
        logical,
        physical,
        resolution_with_defaults(&[(
            "elsewhere",
            ScalarValue::Utf8(Some("unrelated".to_string())),
        )]),
        Column::new("note", 1),
    )
    .expect("rewrite ok");
    assert_eq!(
        literal_value(&result),
        Some(ScalarValue::Utf8(None)),
        "a nullable absent field with no matching default must NULL-fill"
    );
}

/// Scenario: an absent required field without a default errors naming the column.
#[test]
fn absent_required_without_default_errors_cleanly() {
    let logical = Arc::new(Schema::new(vec![
        field_with_id("id", DataType::Int64, false, 1),
        field_with_id("mandatory", DataType::Utf8, false, 9),
    ]));
    let physical = Arc::new(Schema::new(vec![field_with_id(
        "id",
        DataType::Int64,
        false,
        1,
    )]));
    let err = rewrite_with(
        logical,
        physical,
        bare_resolution(),
        Column::new("mandatory", 1),
    )
    .expect_err("required-absent with no default must error");
    let text = err.to_string();
    assert!(
        text.contains("mandatory") && text.contains("missing"),
        "error must name the missing required column: {text}"
    );
}

/// Scenario: a present field is never defaulted, via any of the four claiming paths.
#[test]
fn present_field_binds_real_value_not_default() {
    let logical = Arc::new(Schema::new(vec![
        field_with_id("id", DataType::Int64, false, 1),
        field_with_id("rating", DataType::Int64, true, 2),
    ]));
    let resolution = resolution_with_defaults(&[("rating", ScalarValue::Int64(Some(999)))]);

    let physical = Arc::new(Schema::new(vec![
        field_with_id("id", DataType::Int64, false, 1),
        field_with_id("score", DataType::Int64, true, 2),
    ]));

    let result = rewrite_with(
        Arc::clone(&logical),
        physical,
        resolution.clone(),
        Column::new("rating", 1),
    )
    .expect("rewrite ok");
    let col = result
        .downcast_ref::<Column>()
        .expect("a present field-id must bind a real Column, not a default Literal");
    assert_eq!(col.index(), 1, "must bind the physical field-id-2 slot");

    let physical = Arc::new(Schema::new(vec![
        field_with_id("id", DataType::Int64, false, 1),
        field_no_id("score", DataType::Int64, true),
    ]));
    let name_mapped = FieldIdResolution {
        name_mapping: vec![NameMappingEntry {
            name: "score".to_string(),
            field_id: 2,
        }],
        ..resolution.clone()
    };

    let result = rewrite_with(
        Arc::clone(&logical),
        physical,
        name_mapped,
        Column::new("rating", 1),
    )
    .expect("rewrite ok");
    assert_eq!(
        bound_physical_index(&result).expect(
            "a field present via name-mapping must bind its real value, never a default Literal"
        ),
        1,
        "name-mapping must bind the score slot"
    );

    let physical = Arc::new(Schema::new(vec![
        field_with_id("id", DataType::Int64, false, 1),
        field_no_id("rating", DataType::Int64, true),
    ]));

    let result = rewrite_with(
        Arc::clone(&logical),
        physical,
        resolution.clone(),
        Column::new("rating", 1),
    )
    .expect("rewrite ok");
    assert_eq!(
        bound_physical_index(&result).expect(
            "a column the file supplies under the logical name must bind its real \
             value, never a default Literal"
        ),
        1,
        "the physical-name fallback must bind the rating slot"
    );

    // An embedded id no logical field claims: the physical name still supplies the column.
    let physical = Arc::new(Schema::new(vec![
        field_with_id("id", DataType::Int64, false, 1),
        field_with_id("rating", DataType::Int64, true, 99),
    ]));

    let result =
        rewrite_with(logical, physical, resolution, Column::new("rating", 1)).expect("rewrite ok");
    assert_eq!(
        bound_physical_index(&result).expect(
            "a column the file supplies under an unknown embedded field-id must bind \
             its real value, never a default Literal"
        ),
        1,
        "an unknown embedded id must still bind the rating slot"
    );
}

/// Scenario: one factory binds a real value for a file with the field and the default for one without.
#[test]
fn default_fill_decision_is_per_file() {
    let logical = Arc::new(Schema::new(vec![
        field_with_id("id", DataType::Int64, false, 1),
        field_with_id("added", DataType::Utf8, true, 9),
    ]));
    let factory = FieldIdExprAdapterFactory {
        resolution: resolution_with_defaults(&[(
            "added",
            ScalarValue::Utf8(Some("D".to_string())),
        )]),
    };

    let physical_present = Arc::new(Schema::new(vec![
        field_with_id("id", DataType::Int64, false, 1),
        field_with_id("added", DataType::Utf8, true, 9),
    ]));
    let adapter = factory
        .create(Arc::clone(&logical), physical_present)
        .expect("adapter creation");
    let present = adapter
        .rewrite(Arc::new(Column::new("added", 1)))
        .expect("rewrite ok");
    assert!(
        present.downcast_ref::<Column>().is_some(),
        "a file carrying field-id 9 must bind a real Column"
    );

    let physical_absent = Arc::new(Schema::new(vec![field_with_id(
        "id",
        DataType::Int64,
        false,
        1,
    )]));
    let adapter = factory
        .create(logical, physical_absent)
        .expect("adapter creation");
    let absent = adapter
        .rewrite(Arc::new(Column::new("added", 1)))
        .expect("rewrite ok");
    assert_eq!(
        literal_value(&absent),
        Some(ScalarValue::Utf8(Some("D".to_string()))),
        "a file lacking field-id 9 must emit the default literal"
    );
}

/// Scenario: column projection binds by Iceberg field-id across physical layouts.
#[tokio::test]
async fn field_id_adapter_reads_renamed_column_rows() {
    use super::super::raw_scan::{build_scan_sql, register_files};
    use crate::scan::session_config_for_spec;
    use crate::scan::spec::LogicalField;
    use arrow::array::{Array, Float64Array, Int64Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use datafusion::execution::context::SessionContext;
    use parquet::arrow::ArrowWriter;
    use std::collections::HashMap;

    // Pre-rename layout: id (field-id 1), score (field-id 2) = 10 * id.
    let dir = std::env::temp_dir().join(format!("lh_fieldid_rows_{}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("temp dir");
    let path = dir.join("renamed.parquet");

    let physical_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false).with_metadata(HashMap::from([(
            PARQUET_FIELD_ID_META_KEY.to_string(),
            "1".to_string(),
        )])),
        Field::new("score", DataType::Float64, false).with_metadata(HashMap::from([(
            PARQUET_FIELD_ID_META_KEY.to_string(),
            "2".to_string(),
        )])),
    ]));
    let ids: Vec<i64> = (1..=5).collect();
    let scores: Vec<f64> = ids.iter().map(|i| 10.0 * *i as f64).collect();
    {
        let file = std::fs::File::create(&path).expect("create parquet file");
        let mut writer =
            ArrowWriter::try_new(file, physical_schema.clone(), None).expect("arrow writer");
        let batch = RecordBatch::try_new(
            physical_schema,
            vec![
                Arc::new(Int64Array::from(ids.clone())),
                Arc::new(Float64Array::from(scores.clone())),
            ],
        )
        .expect("record batch");
        writer.write(&batch).expect("write batch");
        writer.close().expect("close writer");
    }
    let file_url = url::Url::from_file_path(&path)
        .expect("absolute path")
        .to_string();

    let logical = vec![
        LogicalField {
            field_id: Some(1),
            name: "id".to_string(),
            arrow_type: "int64".to_string(),
            nullable: false,
            initial_default: None,
            nested: None,
            physical_name: None,
        },
        LogicalField {
            field_id: Some(2),
            name: "rating".to_string(),
            arrow_type: "float64".to_string(),
            nullable: false,
            initial_default: None,
            nested: None,
            physical_name: None,
        },
    ];

    let mut spec = minimal_spec();
    let file_size = local_file_size(&file_url);
    spec.files = vec![FileEntry::new(file_url, file_size)];
    spec.common.logical_schema = logical;
    spec.common.projection = vec!["ID".into(), "RATING".into()];

    let ctx = SessionContext::new_with_config(session_config_for_spec(&spec));
    register_files(&ctx, "scan_target", &spec, &inline_resolved(&spec))
        .await
        .expect("register_files must succeed with logical schema");
    let sql = build_scan_sql(&ctx, "scan_target", &spec)
        .await
        .expect("build_scan_sql");
    let df = ctx.sql(&sql).await.expect("plan scan SQL");
    let batches = df.collect().await.expect("scan must read renamed column");

    let mut got: Vec<(i64, f64)> = Vec::new();
    for batch in &batches {
        let id_col = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("id column is Int64");
        let rating_col = batch
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("rating column is Float64");
        for row in 0..batch.num_rows() {
            assert!(!rating_col.is_null(row), "rating must not be NULL");
            got.push((id_col.value(row), rating_col.value(row)));
        }
    }
    got.sort_by_key(|(id, _)| *id);

    let expected: Vec<(i64, f64)> = ids.iter().map(|i| (*i, 10.0 * *i as f64)).collect();
    assert_eq!(
        got, expected,
        "RATING must read the physical `score` values (rating = 10*id)"
    );
}

/// Scenario: one shard over pre- and post-rename files binds each file's field-id-2 column.
#[tokio::test]
async fn field_id_adapter_reads_divergent_layouts_across_files() {
    use super::super::raw_scan::{build_scan_sql, register_files};
    use crate::scan::session_config_for_spec;
    use crate::scan::spec::LogicalField;
    use arrow::array::{Array, Float64Array, Int64Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use datafusion::execution::context::SessionContext;
    use parquet::arrow::ArrowWriter;
    use std::collections::HashMap;

    fn id_field() -> Field {
        Field::new("id", DataType::Int64, false).with_metadata(HashMap::from([(
            PARQUET_FIELD_ID_META_KEY.to_string(),
            "1".to_string(),
        )]))
    }
    fn score_field(physical_name: &str) -> Field {
        Field::new(physical_name, DataType::Float64, false).with_metadata(HashMap::from([(
            PARQUET_FIELD_ID_META_KEY.to_string(),
            "2".to_string(),
        )]))
    }

    let dir = std::env::temp_dir().join(format!("lh_fieldid_multi_{}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("temp dir");

    // ids 1..=3 as old `score`, 4..=6 as new `rating`; value = 10 * id.
    let write_file = |name: &str, physical_col: &str, ids: &[i64]| -> String {
        let schema = Arc::new(Schema::new(vec![id_field(), score_field(physical_col)]));
        let scores: Vec<f64> = ids.iter().map(|i| 10.0 * *i as f64).collect();
        let path = dir.join(name);
        let file = std::fs::File::create(&path).expect("create parquet file");
        let mut writer = ArrowWriter::try_new(file, schema.clone(), None).expect("arrow writer");
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(ids.to_vec())),
                Arc::new(Float64Array::from(scores)),
            ],
        )
        .expect("record batch");
        writer.write(&batch).expect("write batch");
        writer.close().expect("close writer");
        url::Url::from_file_path(&path)
            .expect("absolute path")
            .to_string()
    };
    let file_old = write_file("old_score.parquet", "score", &[1, 2, 3]);
    let file_new = write_file("new_rating.parquet", "rating", &[4, 5, 6]);

    let logical = vec![
        LogicalField {
            field_id: Some(1),
            name: "id".to_string(),
            arrow_type: "int64".to_string(),
            nullable: false,
            initial_default: None,
            nested: None,
            physical_name: None,
        },
        LogicalField {
            field_id: Some(2),
            name: "rating".to_string(),
            arrow_type: "float64".to_string(),
            nullable: false,
            initial_default: None,
            nested: None,
            physical_name: None,
        },
    ];

    let mut spec = minimal_spec();
    let old_size = local_file_size(&file_old);
    let new_size = local_file_size(&file_new);
    spec.files = vec![
        FileEntry::new(file_old, old_size),
        FileEntry::new(file_new, new_size),
    ];
    spec.common.logical_schema = logical;
    spec.common.projection = vec!["ID".into(), "RATING".into()];

    let ctx = SessionContext::new_with_config(session_config_for_spec(&spec));
    register_files(&ctx, "scan_target", &spec, &inline_resolved(&spec))
        .await
        .expect("register_files must succeed");
    let sql = build_scan_sql(&ctx, "scan_target", &spec)
        .await
        .expect("build_scan_sql");
    let df = ctx.sql(&sql).await.expect("plan scan SQL");
    let batches = df
        .collect()
        .await
        .expect("scan must read both physical layouts");

    let mut got: Vec<(i64, f64)> = Vec::new();
    for batch in &batches {
        let id_col = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("id column is Int64");
        let rating_col = batch
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("rating column is Float64");
        for row in 0..batch.num_rows() {
            assert!(!rating_col.is_null(row), "rating must not be NULL");
            got.push((id_col.value(row), rating_col.value(row)));
        }
    }
    got.sort_by_key(|(id, _)| *id);

    let expected: Vec<(i64, f64)> = (1..=6).map(|i| (i, 10.0 * i as f64)).collect();
    assert_eq!(
        got, expected,
        "both files must resolve field-id 2 to `rating`; rating = 10*id for ids 1..=6"
    );
}

/// Scenario: every primitive `initial-default` survives the scan-spec round-trip; a struct default encodes none.
#[test]
fn initial_default_round_trips_across_full_type_vocabulary() {
    use crate::scan::spec::LogicalField;

    // Float values round-trip exactly through Display/FromStr.
    let cases: Vec<(&str, &str, ScalarValue)> = vec![
        ("bool", "true", ScalarValue::Boolean(Some(true))),
        ("int32", "-42", ScalarValue::Int32(Some(-42))),
        (
            "int64",
            "9000000000",
            ScalarValue::Int64(Some(9_000_000_000)),
        ),
        ("float32", "1.5", ScalarValue::Float32(Some(1.5))),
        ("float64", "-2.25", ScalarValue::Float64(Some(-2.25))),
        (
            "utf8",
            "hello, default",
            ScalarValue::Utf8(Some("hello, default".to_string())),
        ),
        ("date32", "19723", ScalarValue::Date32(Some(19723))),
        (
            "timestamp_us",
            "1700000000000000",
            ScalarValue::TimestampMicrosecond(Some(1_700_000_000_000_000), None),
        ),
        (
            "timestamp_ns",
            "1700000000000000000",
            ScalarValue::TimestampNanosecond(Some(1_700_000_000_000_000_000), None),
        ),
        (
            "timestamptz_us",
            "1700000000000000",
            ScalarValue::TimestampMicrosecond(Some(1_700_000_000_000_000), Some("UTC".into())),
        ),
        (
            "timestamptz_ns",
            "1700000000000000000",
            ScalarValue::TimestampNanosecond(Some(1_700_000_000_000_000_000), Some("UTC".into())),
        ),
        (
            "decimal128(18,4)",
            "1234567",
            ScalarValue::Decimal128(Some(1_234_567), 18, 4),
        ),
    ];

    let mut spec = minimal_spec();
    spec.common.logical_schema = cases
        .iter()
        .enumerate()
        .map(|(i, (tag, encoded, _))| LogicalField {
            field_id: Some(i as i32 + 1),
            name: format!("c{i}"),
            arrow_type: (*tag).to_string(),
            nullable: true,
            initial_default: Some((*encoded).to_string()),
            nested: None,
            physical_name: None,
        })
        .collect();

    let json = spec.to_json();
    let back = ScanSpec::from_json(&json).expect("scan spec must round-trip");

    for ((tag, encoded, expected), field) in cases.iter().zip(back.common.logical_schema.iter()) {
        assert_eq!(field.arrow_type, *tag, "arrow_type tag survives round-trip");
        let encoded_back = field
            .initial_default
            .as_deref()
            .unwrap_or_else(|| panic!("initial_default for '{tag}' survives round-trip"));
        assert_eq!(encoded_back, *encoded, "encoded text survives round-trip");

        let reconstructed = reconstruct_initial_default(&field.arrow_type, encoded_back)
            .unwrap_or_else(|e| panic!("reconstruction for tag '{tag}' failed: {e}"));
        assert_eq!(
            reconstructed, *expected,
            "tag '{tag}' must reconstruct to the originally encoded value"
        );
    }

    // Defaults are bare scalars, so the serialized logical schema holds no storage secret.
    let logical_json = serde_json::to_string(&back.common.logical_schema).unwrap();
    for secret in ["testkey", "testsecret", "access_key", "secret_key"] {
        assert!(
            !logical_json.contains(secret),
            "the default carrier must be credential-free, found '{secret}': {logical_json}"
        );
    }

    // Exasol has no struct type, so a struct default encodes none and falls to NULL/required-error.
    {
        use iceberg::spec::{
            Literal, NestedField, PrimitiveType, Schema, Struct, StructType, Type,
        };

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
            .expect("schema builds");

        let logical = crate::adapter::pushdown::build_logical_schema(&schema);
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
}

fn nested_by_id(field_id: i32, name: &str) -> crate::scan::spec::NestedField {
    crate::scan::spec::NestedField {
        field_id: Some(field_id),
        name: name.to_string(),
        physical_name: None,
        nested: None,
    }
}

/// Delta `name` column mapping, whose file-side member names are opaque.
fn nested_by_physical_name(name: &str, physical_name: &str) -> crate::scan::spec::NestedField {
    crate::scan::spec::NestedField {
        field_id: None,
        name: name.to_string(),
        physical_name: Some(physical_name.to_string()),
        nested: None,
    }
}

fn struct_member_names(data_type: &DataType) -> Vec<&str> {
    match data_type {
        DataType::Struct(fields) => fields.iter().map(|f| f.name().as_str()).collect(),
        other => panic!("expected a resolved struct field, got {other}"),
    }
}

/// Scenario: nested members resolve onto the logical tree by the top-level binding order.
#[test]
fn nested_fields_resolve_to_logical_names_across_binding_keys() {
    use crate::scan::render_nested_column_as_json;
    use crate::scan::spec::NestedMembers;
    use arrow::array::{
        Array, ArrayRef, Int32Array, ListArray, MapArray, StringArray, StructArray,
    };
    use arrow::buffer::OffsetBuffer;
    use arrow::datatypes::Fields;

    // Field-id 11 renamed and after 12, an unclaimed member, and field-id 13 omitted.
    let addr_physical: ArrayRef = Arc::new(StructArray::from(vec![
        (
            Arc::new(field_with_id("city", DataType::Utf8, true, 12)),
            Arc::new(StringArray::from(vec!["Berlin"])) as ArrayRef,
        ),
        (
            Arc::new(field_with_id("street_v2", DataType::Utf8, true, 11)),
            Arc::new(StringArray::from(vec!["Main St"])) as ArrayRef,
        ),
        (
            Arc::new(field_with_id("junk", DataType::Int32, true, 99)),
            Arc::new(Int32Array::from(vec![7])) as ArrayRef,
        ),
    ]));
    let props_physical: ArrayRef = Arc::new(StructArray::from(vec![(
        Arc::new(field_no_id("col-i", DataType::Int32, true)),
        Arc::new(Int32Array::from(vec![1])) as ArrayRef,
    )]));
    let labels = Arc::new(StructArray::from(vec![(
        Arc::new(field_with_id("lbl", DataType::Utf8, true, 21)),
        Arc::new(StringArray::from(vec!["x", "y"])) as ArrayRef,
    )])) as ArrayRef;
    let tags_physical: ArrayRef = Arc::new(
        ListArray::try_new(
            Arc::new(Field::new("item", labels.data_type().clone(), true)),
            OffsetBuffer::new(vec![0, 2].into()),
            labels,
            None,
        )
        .expect("list builds"),
    );
    let map_values = Arc::new(StructArray::from(vec![(
        Arc::new(field_with_id("v_old", DataType::Int32, true, 31)),
        Arc::new(Int32Array::from(vec![5])) as ArrayRef,
    )])) as ArrayRef;
    let entries = StructArray::try_new(
        Fields::from(vec![
            Arc::new(Field::new("key", DataType::Utf8, false)),
            Arc::new(Field::new("value", map_values.data_type().clone(), true)),
        ]),
        vec![
            Arc::new(StringArray::from(vec!["a"])) as ArrayRef,
            map_values,
        ],
        None,
    )
    .expect("map entries build");
    let attrs_physical: ArrayRef = Arc::new(
        MapArray::try_new(
            Arc::new(Field::new("entries", entries.data_type().clone(), false)),
            OffsetBuffer::new(vec![0, 1].into()),
            entries,
            None,
            false,
        )
        .expect("map builds"),
    );

    let logical = Schema::new(vec![
        field_with_id("addr", DataType::Utf8, true, 10),
        field_no_id("props", DataType::Utf8, true),
        field_with_id("tags", DataType::Utf8, true, 20),
        field_with_id("attrs", DataType::Utf8, true, 30),
    ]);
    let physical = Schema::new(vec![
        field_with_id("addr_old", addr_physical.data_type().clone(), true, 10),
        field_no_id("col-p", props_physical.data_type().clone(), true),
        field_with_id("tags", tags_physical.data_type().clone(), true, 20),
        field_with_id("attrs", attrs_physical.data_type().clone(), true, 30),
    ]);
    let resolution = FieldIdResolution {
        declared_physical_names: HashMap::from([("col-p".to_string(), "props".to_string())]),
        nested_members: HashMap::from([
            (
                "addr".to_string(),
                NestedMembers::Struct {
                    fields: vec![
                        nested_by_id(11, "street"),
                        nested_by_id(12, "city"),
                        nested_by_id(13, "zip"),
                    ],
                },
            ),
            (
                "props".to_string(),
                NestedMembers::Struct {
                    fields: vec![nested_by_physical_name("inner_int", "col-i")],
                },
            ),
            (
                "tags".to_string(),
                NestedMembers::List {
                    element: Some(Box::new(NestedMembers::Struct {
                        fields: vec![nested_by_id(21, "label")],
                    })),
                },
            ),
            (
                "attrs".to_string(),
                NestedMembers::Map {
                    key: None,
                    value: Some(Box::new(NestedMembers::Struct {
                        fields: vec![nested_by_id(31, "amount")],
                    })),
                },
            ),
        ]),
        ..bare_resolution()
    };

    let binding = bind_columns(&logical, &physical, &resolution);

    assert_eq!(
        struct_member_names(binding.nested["addr"].resolved_field().data_type()),
        vec!["street", "city", "zip"],
        "members must take their logical names in logical order, with the unclaimed \
         physical member dropped and the absent logical field null-filled"
    );
    assert_eq!(
        struct_member_names(binding.nested["props"].resolved_field().data_type()),
        vec!["inner_int"],
        "a declared physical name must claim a member exactly as it claims a column"
    );

    let rendered = |column: &str, array: &ArrayRef| {
        let resolved = binding.nested[column]
            .apply(array)
            .expect("the resolution applies to its own column");
        render_nested_column_as_json(&resolved)
            .expect("a resolved nested column renders")
            .value(0)
            .to_string()
    };
    assert_eq!(
        rendered("addr", &addr_physical),
        r#"{"street":"Main St","city":"Berlin","zip":null}"#
    );
    assert_eq!(rendered("props", &props_physical), r#"{"inner_int":1}"#);
    assert_eq!(
        rendered("tags", &tags_physical),
        r#"[{"label":"x"},{"label":"y"}]"#
    );
    assert_eq!(rendered("attrs", &attrs_physical), r#"{"a":{"amount":5}}"#);
}

/// Scenario: a nested column absent from the file NULL-fills through the delegate's absent-column path.
#[test]
fn nested_column_absent_from_a_file_null_fills_as_the_logical_utf8() {
    use crate::scan::spec::NestedMembers;

    let logical = Arc::new(Schema::new(vec![
        field_with_id("id", DataType::Int64, false, 1),
        field_with_id("addr", DataType::Utf8, true, 10),
    ]));
    let physical = Arc::new(Schema::new(vec![field_with_id(
        "id",
        DataType::Int64,
        false,
        1,
    )]));
    let resolution = FieldIdResolution {
        nested_members: HashMap::from([(
            "addr".to_string(),
            NestedMembers::Struct {
                fields: vec![nested_by_id(11, "street")],
            },
        )]),
        ..bare_resolution()
    };

    let rewritten = rewrite_with(logical, physical, resolution, Column::new("addr", 1))
        .expect("an absent nullable nested column must NULL-fill, never error");

    assert_eq!(
        literal_value(&rewritten),
        Some(ScalarValue::Utf8(None)),
        "an absent nested column must fill with a NULL of the logical Utf8 type"
    );
}

/// Scenario: a nested column reaches `Utf8` by JSON rendering, never by a cast.
#[test]
fn nested_physical_column_bypasses_the_cast_and_yields_utf8() {
    use crate::scan::spec::NestedMembers;
    use arrow::array::{Array, ArrayRef, RecordBatch, StringArray, StructArray};

    let addr_physical: ArrayRef = Arc::new(StructArray::from(vec![(
        Arc::new(field_with_id("street_v2", DataType::Utf8, true, 11)),
        Arc::new(StringArray::from(vec!["Main St"])) as ArrayRef,
    )]));
    let logical = Arc::new(Schema::new(vec![field_with_id(
        "addr",
        DataType::Utf8,
        true,
        10,
    )]));
    // The rename and the diversion must compose: the wrapped child carries the file's own name.
    let physical = Arc::new(Schema::new(vec![field_with_id(
        "addr_old",
        addr_physical.data_type().clone(),
        true,
        10,
    )]));
    let resolution = FieldIdResolution {
        nested_members: HashMap::from([(
            "addr".to_string(),
            NestedMembers::Struct {
                fields: vec![nested_by_id(11, "street")],
            },
        )]),
        ..bare_resolution()
    };

    let binding = bind_columns(&logical, &physical, &resolution);
    let nested = binding.nested_columns();
    let delegate_physical = binding.delegate_physical_schema(&nested);

    assert!(
        !arrow::compute::can_cast_types(addr_physical.data_type(), &DataType::Utf8),
        "arrow-cast has no struct-to-text kernel, so the cast path cannot serve this column"
    );
    assert!(
        matches!(delegate_physical.field(0).data_type(), DataType::Struct(_)),
        "the delegate must resolve against the column's RESOLVED nested type"
    );
    assert_eq!(
        delegate_logical_schema(&logical, &delegate_physical, &nested).field(0),
        delegate_physical.field(0),
        "the delegate's two schemas must carry ONE identical field for a nested column — \
         name, type, nullability, and metadata together — because it answers ANY \
         difference with the cast this diversion exists to avoid"
    );

    let rewritten = rewrite_with(
        Arc::clone(&logical),
        Arc::clone(&physical),
        resolution,
        Column::new("addr", 0),
    )
    .expect("a nested column must rewrite rather than fail a castability check");

    assert!(
        rewritten.downcast_ref::<CastExpr>().is_none(),
        "a nested column must never be handed to a cast: {rewritten}"
    );
    assert_eq!(
        rewritten
            .data_type(&physical)
            .expect("the substituted expression reports its type"),
        DataType::Utf8,
        "the substituted expression must agree with the registered table schema"
    );
    let children = rewritten.children();
    assert_eq!(
        children
            .iter()
            .map(|child| child
                .downcast_ref::<Column>()
                .map(|c| (c.name(), c.index())))
            .collect::<Vec<_>>(),
        vec![Some(("addr_old", 0))],
        "the wrapped child must stay a bare Column at the file's REAL physical name, \
         which is what the opener's name-based projection and reassignment resolve"
    );

    let batch = RecordBatch::try_new(Arc::clone(&physical), vec![Arc::clone(&addr_physical)])
        .expect("a batch of the physical column");
    let evaluated = rewritten
        .evaluate(&batch)
        .expect("the substituted expression evaluates over the physical column")
        .into_array(batch.num_rows())
        .expect("an array result");
    assert_eq!(
        evaluated.data_type(),
        &DataType::Utf8,
        "rendering, not casting, is what yields Utf8"
    );
    assert_eq!(
        evaluated
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("the rendered column is Utf8")
            .value(0),
        r#"{"street":"Main St"}"#,
        "the rendering must key the document by the TABLE's member name"
    );
}

/// Scenario: a nested column declaring no member tree is left to the delegate, which fails loudly.
#[test]
fn a_nested_physical_column_with_no_descriptor_fails_the_cast_rather_than_rendering() {
    use arrow::array::{ArrayRef, StringArray, StructArray};

    let addr_physical: ArrayRef = Arc::new(StructArray::from(vec![(
        Arc::new(field_no_id("street", DataType::Utf8, true)),
        Arc::new(StringArray::from(vec!["Main St"])) as ArrayRef,
    )]));
    let logical = Arc::new(Schema::new(vec![field_no_id("addr", DataType::Utf8, true)]));
    let physical = Arc::new(Schema::new(vec![field_no_id(
        "addr",
        addr_physical.data_type().clone(),
        true,
    )]));

    let binding = bind_columns(&logical, &physical, &bare_resolution());
    assert!(
        binding.nested_columns().is_empty(),
        "a column with no declared member tree must not be diverted"
    );

    let error = rewrite_with(logical, physical, bare_resolution(), Column::new("addr", 0))
        .expect_err("an undeclared nested column must fail rather than render");
    let message = error.to_string();
    assert!(
        message.contains("addr"),
        "the failure must name the column it could not adapt, got: {message}"
    );
}

/// Scenario: a diverted column always comes with withheld Parquet row-filter pushdown.
#[test]
fn a_binding_that_diverts_a_column_always_withholds_row_filter_pushdown() {
    use crate::scan::raw_scan::scan_table_parquet_format;
    use crate::scan::spec::NestedMembers;
    use arrow::array::{ArrayRef, StringArray, StructArray};

    let addr_physical: ArrayRef = Arc::new(StructArray::from(vec![(
        Arc::new(field_no_id("street", DataType::Utf8, true)),
        Arc::new(StringArray::from(vec!["Main St"])) as ArrayRef,
    )]));
    let logical = Arc::new(Schema::new(vec![field_no_id("addr", DataType::Utf8, true)]));
    let physical = Arc::new(Schema::new(vec![field_no_id(
        "addr",
        addr_physical.data_type().clone(),
        true,
    )]));

    let declared = FieldIdResolution {
        nested_members: HashMap::from([(
            "addr".to_string(),
            NestedMembers::Struct {
                fields: vec![nested_by_physical_name("street", "street")],
            },
        )]),
        ..bare_resolution()
    };
    assert!(
        !bind_columns(&logical, &physical, &declared)
            .nested_columns()
            .is_empty(),
        "a declared member tree must divert its column"
    );
    assert!(
        !scan_table_parquet_format(&declared)
            .options()
            .global
            .pushdown_filters,
        "a table whose binding diverts a column must read WITHOUT Parquet row-filter pushdown"
    );

    let undeclared = bare_resolution();
    assert!(
        bind_columns(&logical, &physical, &undeclared)
            .nested_columns()
            .is_empty(),
        "no declared member tree means no diversion, so nothing is rendered to lose a predicate"
    );
}

/// Scenario: files with different column sets bind by name only, never by ordinal field-id.
#[test]
fn identity_binding_spans_files_with_different_column_sets() {
    let logical = Schema::new(vec![
        field_no_id("id", DataType::Int64, true),
        field_no_id("extra", DataType::Utf8, true),
        field_no_id("name", DataType::Utf8, true),
    ]);
    let first = Schema::new(vec![
        field_no_id("name", DataType::Utf8, true),
        field_no_id("id", DataType::Int64, true),
    ]);
    let second = Schema::new(vec![
        field_no_id("extra", DataType::Utf8, true),
        field_no_id("id", DataType::Int64, true),
    ]);

    let first_binding = bind_columns(&logical, &first, &bare_resolution());
    assert!(
        first_binding.bound_logical_names.contains("id")
            && first_binding.bound_logical_names.contains("name"),
        "the first file's own columns bind by name whatever their ordinal"
    );
    assert!(
        !first_binding.bound_logical_names.contains("extra"),
        "a column the first file does not carry stays unbound for the NULL fill"
    );

    let second_binding = bind_columns(&logical, &second, &bare_resolution());
    assert!(
        second_binding.bound_logical_names.contains("id")
            && second_binding.bound_logical_names.contains("extra"),
        "the second file's own columns bind by name too"
    );
    assert!(
        !second_binding.bound_logical_names.contains("name"),
        "a column the second file does not carry stays unbound for the NULL fill"
    );

    let logical_ref = Arc::new(logical);
    let in_first = rewrite(
        Arc::clone(&logical_ref),
        Arc::new(first),
        Column::new("id", 0),
    )
    .expect("rewrite ok");
    let in_second =
        rewrite(logical_ref, Arc::new(second), Column::new("id", 0)).expect("rewrite ok");

    assert_eq!(
        bound_physical_index(&in_first),
        Some(1),
        "`id` binds to its own name at the first file's ordinal 1"
    );
    assert_eq!(
        bound_physical_index(&in_second),
        Some(1),
        "`id` binds to its own name in the second file too, whose column set differs"
    );
}
