use super::*;
use crate::scan::spec::{FileEntry, ScanSpec};
use crate::scan::test_support::{local_file_size, minimal_spec};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::expressions::{CastExpr, Column, Literal};

/// A field tagged with its Iceberg field-id (`PARQUET:field_id`).
fn field_with_id(name: &str, dt: DataType, nullable: bool, id: i32) -> Field {
    Field::new(name, dt, nullable).with_metadata(HashMap::from([(
        PARQUET_FIELD_ID_META_KEY.to_string(),
        id.to_string(),
    )]))
}

/// A field carrying no field-id metadata (older writer).
fn field_no_id(name: &str, dt: DataType, nullable: bool) -> Field {
    Field::new(name, dt, nullable)
}

fn rewrite(
    logical: SchemaRef,
    physical: SchemaRef,
    column: Column,
) -> datafusion::error::Result<Arc<dyn PhysicalExpr>> {
    let adapter = FieldIdExprAdapterFactory {
        name_mapping: Vec::new(),
        defaults: HashMap::new(),
    }
    .create(logical, physical)
    .expect("adapter creation");
    adapter.rewrite(Arc::new(column))
}

/// Rewrite a single column through a factory carrying explicit
/// `name_mapping` and reconstructed `defaults` (field-id → `ScalarValue`),
/// so the absent-with-default fill seam can be exercised directly.
fn rewrite_with(
    logical: SchemaRef,
    physical: SchemaRef,
    name_mapping: Vec<crate::scan::spec::NameMappingEntry>,
    defaults: HashMap<i32, ScalarValue>,
    column: Column,
) -> datafusion::error::Result<Arc<dyn PhysicalExpr>> {
    let adapter = FieldIdExprAdapterFactory {
        name_mapping,
        defaults,
    }
    .create(logical, physical)
    .expect("adapter creation");
    adapter.rewrite(Arc::new(column))
}

/// The reconstructed `ScalarValue` from a `Literal`, for asserting the
/// injected default literal's value.
fn literal_value(expr: &Arc<dyn PhysicalExpr>) -> Option<ScalarValue> {
    expr.downcast_ref::<Literal>().map(|l| l.value().clone())
}

/// A renamed column (physical `score`, logical `rating`, same field-id 2)
/// binds to the physical column BY field-id, not by name.
#[test]
fn resolves_renamed_column_by_field_id() {
    let logical = Arc::new(Schema::new(vec![
        field_with_id("id", DataType::Int64, false, 1),
        field_with_id("rating", DataType::Int64, true, 2),
    ]));
    // Physical file predates the rename: field-id 2 is named `score`, at index 1.
    let physical = Arc::new(Schema::new(vec![
        field_with_id("id", DataType::Int64, false, 1),
        field_with_id("score", DataType::Int64, true, 2),
    ]));

    // The planner references the CURRENT logical name `rating`.
    let result = rewrite(logical, physical, Column::new("rating", 1)).expect("rewrite ok");

    // Types match, so it resolves to a plain physical Column (no cast),
    // and it must point at physical index 1 (the `score` slot).
    let col = result
        .downcast_ref::<Column>()
        .expect("renamed column resolves to a Column, no cast");
    assert_eq!(col.index(), 1, "must bind to physical field-id-2 slot");
}

/// A type divergence between the logical and physical field (same field-id)
/// is wrapped in a cast (delegated to the default adapter).
#[test]
fn casts_on_type_divergence_by_field_id() {
    let logical = Arc::new(Schema::new(vec![field_with_id(
        "amount",
        DataType::Int64,
        true,
        5,
    )]));
    // Same field-id 5 but a narrower physical type, and a different physical name.
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

/// A dropped column (present physically with an id absent from the logical
/// schema) is simply not referenced by the projection; the adapter leaves
/// the remaining physical fields resolvable by their logical names.
#[test]
fn ignores_dropped_physical_column() {
    let logical = Arc::new(Schema::new(vec![field_with_id(
        "id",
        DataType::Int64,
        false,
        1,
    )]));
    // Physical file still has an old, since-dropped column (field-id 7).
    let physical = Arc::new(Schema::new(vec![
        field_with_id("id", DataType::Int64, false, 1),
        field_with_id("legacy", DataType::Utf8, true, 7),
    ]));

    // The kept logical column `id` still binds correctly.
    let result = rewrite(logical, physical, Column::new("id", 0)).expect("rewrite ok");
    let col = result
        .downcast_ref::<Column>()
        .expect("kept column resolves to a Column");
    assert_eq!(col.index(), 0);
}

/// Task 4.2: the logical Arrow schema built from `ScanSpec::logical_schema`
/// tags each field with its Iceberg field-id, reconstructs the Arrow type
/// from the tag, and preserves the declared nullability.
#[test]
fn builds_logical_arrow_schema_with_field_ids() {
    use super::{build_logical_arrow_schema, field_id_of};
    use crate::scan::spec::LogicalField;

    let logical = vec![
        LogicalField {
            field_id: 1,
            name: "id".to_string(),
            arrow_type: "int64".to_string(),
            nullable: false,
            initial_default: None,
        },
        LogicalField {
            field_id: 2,
            name: "rating".to_string(),
            arrow_type: "float64".to_string(),
            nullable: true,
            initial_default: None,
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

/// Scenario: a physical field with NO embedded field-id, whose physical
/// name IS covered by a `name_mapping` entry pointing to a field-id that
/// IS present in the logical schema, resolves to that logical field's name.
#[test]
fn name_mapping_resolves_no_field_id_column() {
    use super::rename_physical_to_logical;
    use crate::scan::spec::NameMappingEntry;

    let logical = Schema::new(vec![
        field_with_id("id", DataType::Int64, false, 1),
        field_with_id("rating", DataType::Int64, true, 2),
    ]);
    // No embedded field-id: name-mapping maps `score` -> id 2 -> `rating`.
    let physical = Schema::new(vec![field_no_id("score", DataType::Int64, true)]);
    let mapping = vec![NameMappingEntry {
        name: "score".to_string(),
        field_id: 2,
    }];

    let renamed = rename_physical_to_logical(&logical, &physical, &mapping);

    assert_eq!(
        renamed.field(0).name(),
        "rating",
        "no-id field must resolve via name-mapping"
    );
}

/// Scenario: a physical field WITH an embedded field-id that resolves via
/// `logical_name_by_id` wins over a conflicting name-mapping entry for the
/// same physical name pointing at a DIFFERENT field-id; the name-mapping
/// is not consulted when an embedded field-id is present.
#[test]
fn embedded_field_id_wins_over_name_mapping() {
    use super::rename_physical_to_logical;
    use crate::scan::spec::NameMappingEntry;

    let logical = Schema::new(vec![
        field_with_id("id", DataType::Int64, false, 1),
        field_with_id("rating", DataType::Int64, true, 2),
    ]);
    let physical = Schema::new(vec![field_with_id("score", DataType::Int64, true, 2)]);
    let mapping = vec![NameMappingEntry {
        name: "score".to_string(),
        field_id: 1,
    }];

    let renamed = rename_physical_to_logical(&logical, &physical, &mapping);

    assert_eq!(
        renamed.field(0).name(),
        "rating",
        "embedded id 2 must win over a mapping to id 1"
    );
}

/// Scenario: `name_mapping` is empty/absent, so a physical field with no
/// embedded field-id keeps its physical name unchanged (today's existing
/// fallback, unaffected by name-mapping support).
#[test]
fn no_name_mapping_falls_back_to_physical_name() {
    use super::rename_physical_to_logical;

    let logical = Schema::new(vec![
        field_with_id("id", DataType::Int64, false, 1),
        field_with_id("rating", DataType::Int64, true, 2),
    ]);
    let physical = Schema::new(vec![field_no_id("score", DataType::Int64, true)]);

    let renamed = rename_physical_to_logical(&logical, &physical, &[]);

    assert_eq!(
        renamed.field(0).name(),
        "score",
        "no mapping must keep the physical name"
    );
}

/// Scenario: `name_mapping` has entries, but none cover this particular
/// physical field's name, so the physical name is kept unchanged (the
/// name-mapping augments but never replaces the fallback).
#[test]
fn uncovered_name_mapping_falls_back_to_physical_name() {
    use super::rename_physical_to_logical;
    use crate::scan::spec::NameMappingEntry;

    let logical = Schema::new(vec![
        field_with_id("id", DataType::Int64, false, 1),
        field_with_id("rating", DataType::Int64, true, 2),
    ]);
    let physical = Schema::new(vec![field_no_id("unknown", DataType::Int64, true)]);
    let mapping = vec![NameMappingEntry {
        name: "score".to_string(),
        field_id: 2,
    }];

    let renamed = rename_physical_to_logical(&logical, &physical, &mapping);

    assert_eq!(
        renamed.field(0).name(),
        "unknown",
        "uncovered field must keep the physical name"
    );
}

/// Edge case: an embedded field-id that is present but ABSENT from the
/// logical schema must NOT fall through to the name-mapping — it keeps
/// the physical name, exactly like the no-mapping fallback.
#[test]
fn embedded_field_id_absent_from_logical_schema_skips_name_mapping() {
    use super::rename_physical_to_logical;
    use crate::scan::spec::NameMappingEntry;

    let logical = Schema::new(vec![
        field_with_id("id", DataType::Int64, false, 1),
        field_with_id("rating", DataType::Int64, true, 2),
    ]);
    let physical = Schema::new(vec![field_with_id("score", DataType::Int64, true, 99)]);
    let mapping = vec![NameMappingEntry {
        name: "score".to_string(),
        field_id: 2,
    }];

    let renamed = rename_physical_to_logical(&logical, &physical, &mapping);

    assert_eq!(
        renamed.field(0).name(),
        "score",
        "an unresolvable embedded id must NOT fall through to the name-mapping"
    );
}

/// Scenario: field-id resolution falls back to physical name when a file
/// field carries no embedded field-id.
///
/// A file whose fields carry no `PARQUET:field_id` metadata cannot be bound
/// by id; the adapter falls through to the physical-name match so the
/// column is still resolved correctly.
#[test]
fn field_id_adapter_falls_back_to_name_without_field_id() {
    let logical = Arc::new(Schema::new(vec![
        field_with_id("id", DataType::Int64, false, 1),
        field_with_id("rating", DataType::Int64, true, 2),
    ]));
    // Physical file carries NO field-ids at all (older writer).
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
///
/// When a column was added to the schema AFTER a file was written, the file
/// simply does not contain the field. The adapter delegates to
/// `DefaultPhysicalExprAdapter` which returns a NULL literal for nullable
/// missing columns rather than erroring.
#[test]
fn field_id_adapter_null_fills_added_nullable_column() {
    let logical = Arc::new(Schema::new(vec![
        field_with_id("id", DataType::Int64, false, 1),
        field_with_id("note", DataType::Utf8, true, 9),
    ]));
    // Physical file predates the addition: field-id 9 is absent.
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
///
/// A REQUIRED (non-nullable) column that is absent from an older file must
/// produce a clean descriptive error — never wrong data or a silent NULL.
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

/// An absent field with a defined `initial-default` emits `Literal(default)`
/// — for BOTH a required-with-default and a nullable-with-default field
/// (rule 3 applies regardless of nullability, and the default must be
/// substituted BEFORE delegating so the required-absent path does not error).
#[test]
fn absent_field_with_initial_default_emits_default_literal() {
    // Required-with-default: id 9 is absent from the physical file.
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
    let defaults = HashMap::from([(9, ScalarValue::Utf8(Some("req-default".to_string())))]);

    let result = rewrite_with(
        Arc::clone(&logical),
        Arc::clone(&physical),
        Vec::new(),
        defaults,
        Column::new("required_added", 1),
    )
    .expect("required-absent-with-default must not error");
    assert_eq!(
        literal_value(&result),
        Some(ScalarValue::Utf8(Some("req-default".to_string()))),
        "a required absent field with a default must emit Literal(default), not error"
    );

    // Nullable-with-default: id 9 is absent; the default wins over NULL-fill.
    let logical = Arc::new(Schema::new(vec![
        field_with_id("id", DataType::Int64, false, 1),
        field_with_id("nullable_added", DataType::Int64, true, 9),
    ]));
    let defaults = HashMap::from([(9, ScalarValue::Int64(Some(-1)))]);

    let result = rewrite_with(
        logical,
        physical,
        Vec::new(),
        defaults,
        Column::new("nullable_added", 1),
    )
    .expect("nullable-absent-with-default must not error");
    assert_eq!(
        literal_value(&result),
        Some(ScalarValue::Int64(Some(-1))),
        "a nullable absent field with a default must emit Literal(default), not NULL"
    );
}

/// An absent NULLABLE field with NO default NULL-fills — the default map is
/// consulted per field-id, so a default for an UNRELATED id does not leak in.
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
    // A default exists, but for a DIFFERENT field-id (5), not for 9.
    let defaults = HashMap::from([(5, ScalarValue::Utf8(Some("unrelated".to_string())))]);

    let result = rewrite_with(
        logical,
        physical,
        Vec::new(),
        defaults,
        Column::new("note", 1),
    )
    .expect("rewrite ok");
    assert_eq!(
        literal_value(&result),
        Some(ScalarValue::Utf8(None)),
        "a nullable absent field with no matching default must NULL-fill"
    );
}

/// An absent REQUIRED field with NO default still errors cleanly (naming the
/// column), never a silent NULL or a bogus default.
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
    // No default for the required-absent field.
    let defaults = HashMap::new();

    let err = rewrite_with(
        logical,
        physical,
        Vec::new(),
        defaults,
        Column::new("mandatory", 1),
    )
    .expect_err("required-absent with no default must error");
    let text = err.to_string();
    assert!(
        text.contains("mandatory") && text.contains("missing"),
        "error must name the missing required column: {text}"
    );
}

/// A field PRESENT in the file — whether resolved by embedded field-id or by
/// name-mapping — binds to its REAL physical values and is NEVER defaulted,
/// even when a default exists for its field-id.
#[test]
fn present_field_binds_real_value_not_default() {
    use crate::scan::spec::NameMappingEntry;

    // Present by embedded field-id (renamed score->rating, id 2).
    let logical = Arc::new(Schema::new(vec![
        field_with_id("id", DataType::Int64, false, 1),
        field_with_id("rating", DataType::Int64, true, 2),
    ]));
    let physical = Arc::new(Schema::new(vec![
        field_with_id("id", DataType::Int64, false, 1),
        field_with_id("score", DataType::Int64, true, 2),
    ]));
    let defaults = HashMap::from([(2, ScalarValue::Int64(Some(999)))]);

    let result = rewrite_with(
        Arc::clone(&logical),
        physical,
        Vec::new(),
        defaults.clone(),
        Column::new("rating", 1),
    )
    .expect("rewrite ok");
    let col = result
        .downcast_ref::<Column>()
        .expect("a present field-id must bind a real Column, not a default Literal");
    assert_eq!(col.index(), 1, "must bind the physical field-id-2 slot");

    // Present by name-mapping (no embedded id; score->id 2->rating).
    let physical = Arc::new(Schema::new(vec![
        field_with_id("id", DataType::Int64, false, 1),
        field_no_id("score", DataType::Int64, true),
    ]));
    let mapping = vec![NameMappingEntry {
        name: "score".to_string(),
        field_id: 2,
    }];

    let result = rewrite_with(
        logical,
        physical,
        mapping,
        defaults,
        Column::new("rating", 1),
    )
    .expect("rewrite ok");
    // A name-mapped field carries no embedded field-id metadata, so the
    // default adapter binds the real physical column wrapped in an identity
    // cast (Int64→Int64) — same as the no-field-id name fallback. The point
    // is that it binds the REAL physical column (index 1), never a default
    // Literal, so accept either a bare Column or a cast-wrapped Column.
    let bound_index = result
        .downcast_ref::<Column>()
        .map(Column::index)
        .or_else(|| {
            result
                .downcast_ref::<CastExpr>()
                .and_then(|c| c.expr().downcast_ref::<Column>())
                .map(Column::index)
        })
        .expect(
            "a field present via name-mapping must bind a real physical Column \
             (bare or cast-wrapped), not a default Literal",
        );
    assert_eq!(bound_index, 1, "name-mapping must bind the score slot");
}

/// The fill decision is PER FILE: one factory (one reconstructed default map)
/// yields a real-value binding for a file that HAS the field-id and a
/// `Literal(default)` for a file that LACKS it.
#[test]
fn default_fill_decision_is_per_file() {
    let logical = Arc::new(Schema::new(vec![
        field_with_id("id", DataType::Int64, false, 1),
        field_with_id("added", DataType::Utf8, true, 9),
    ]));
    let factory = FieldIdExprAdapterFactory {
        name_mapping: Vec::new(),
        defaults: HashMap::from([(9, ScalarValue::Utf8(Some("D".to_string())))]),
    };

    // File B: the added column IS present — binds its real value.
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

    // File A: the added column is ABSENT — emits the default literal.
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
///
/// Row-level regression for the E2E `e2e_renamed_column_resolves_by_field_id`
/// failure: a Parquet file whose PHYSICAL column is `score` (field-id 2) is
/// registered through the production `register_files` path against a LOGICAL
/// schema that calls field-id 2 `rating`. Selecting `RATING` through the same
/// `build_scan_sql` the UDF runs must read the physical `score` values — the
/// projected output column must be remapped by field-id on the READ path, not
/// looked up by the (non-existent) physical name `rating`.
///
/// Before the fix this fails with the exact E2E error
/// (`Unable to get field named "rating". Valid fields: ["id", "score"]`)
/// because the projected `Column("rating")` is resolved by NAME against the
/// real physical file schema `[id, score]`.
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

    // Write a local Parquet file with PHYSICAL fields id (field-id 1) and
    // score (field-id 2) — the pre-rename layout. score = 10 * id.
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

    // Logical (current) schema: field-id 2 is now `rating`, not `score`.
    let logical = vec![
        LogicalField {
            field_id: 1,
            name: "id".to_string(),
            arrow_type: "int64".to_string(),
            nullable: false,
            initial_default: None,
        },
        LogicalField {
            field_id: 2,
            name: "rating".to_string(),
            arrow_type: "float64".to_string(),
            nullable: false,
            initial_default: None,
        },
    ];

    let mut spec = minimal_spec();
    let file_size = local_file_size(&file_url);
    spec.files = vec![FileEntry::new(file_url, file_size)];
    spec.common.logical_schema = logical;
    // The adapter pushes uppercase current-name projection.
    spec.common.projection = vec!["ID".into(), "RATING".into()];

    // Drive the EXACT production path: register_files + build_scan_sql, then
    // collect the resulting rows.
    let ctx = SessionContext::new_with_config(session_config_for_spec(&spec));
    register_files(&ctx, "scan_target", &spec)
        .await
        .expect("register_files must succeed with logical schema");
    let sql = build_scan_sql(&ctx, "scan_target", &spec)
        .await
        .expect("build_scan_sql");
    let df = ctx.sql(&sql).await.expect("plan scan SQL");
    let batches = df.collect().await.expect("scan must read renamed column");

    // Assert the RATING output column carries the physical `score` values.
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

/// Scenario: column projection binds by Iceberg field-id across physical layouts.
///
/// The multi-file mirror of the E2E: one shard covers a file written BEFORE a
/// rename (physical column `score`) and a file written AFTER it (physical column
/// `rating`), both carrying field-id 2. A single `ListingTable` over both must
/// bind each file's field-id-2 column to the current logical name `rating` — the
/// per-file expr adapter is created once per file, so divergent physical layouts
/// in the same shard each resolve correctly.
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

    // Write one file per physical layout. score = 10 * id; ids 1..=3 (old
    // `score`), 4..=6 (new `rating`).
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
            field_id: 1,
            name: "id".to_string(),
            arrow_type: "int64".to_string(),
            nullable: false,
            initial_default: None,
        },
        LogicalField {
            field_id: 2,
            name: "rating".to_string(),
            arrow_type: "float64".to_string(),
            nullable: false,
            initial_default: None,
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
    register_files(&ctx, "scan_target", &spec)
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

/// Task 3.3: every supported primitive `initial-default` survives the full
/// scan-spec serialization round-trip, across the ENTIRE Arrow-type-tag
/// vocabulary. For each case the encoded default is placed on a `LogicalField`,
/// the whole `ScanSpec` is serialized to JSON and back, and the value is
/// reconstructed to the exact `ScalarValue` (with its timezone / precision /
/// scale) against the field's tag. The SAME test proves a non-primitive
/// (struct) `initial-default` encodes NO default through the VS layer's
/// `build_logical_schema`, and that the default carrier is credential-free.
///
/// This is ONE parametrized test — NOT one `#[test]` per type.
#[test]
fn initial_default_round_trips_across_full_type_vocabulary() {
    use crate::scan::spec::LogicalField;

    // (arrow_type tag, encoded initial-default text, expected ScalarValue).
    // Float values are chosen to round-trip exactly through Display/FromStr.
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

    // Carry every primitive case on ONE ScanSpec so the assertion exercises the
    // real serialize → deserialize path once for the whole vocabulary.
    let mut spec = minimal_spec();
    spec.common.logical_schema = cases
        .iter()
        .enumerate()
        .map(|(i, (tag, encoded, _))| LogicalField {
            field_id: i as i32 + 1,
            name: format!("c{i}"),
            arrow_type: (*tag).to_string(),
            nullable: true,
            initial_default: Some((*encoded).to_string()),
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

    // The default carrier is credential-free: the encoded defaults are bare
    // scalars, so the serialized logical schema contains no storage secret
    // (minimal_spec's credentials are "testkey"/"testsecret", carried only in
    // the separate `storage` block).
    let logical_json = serde_json::to_string(&back.common.logical_schema).unwrap();
    for secret in ["testkey", "testsecret", "access_key", "secret_key"] {
        assert!(
            !logical_json.contains(secret),
            "the default carrier must be credential-free, found '{secret}': {logical_json}"
        );
    }

    // The SAME test: a non-primitive (struct) initial-default encodes NO default
    // through the VS layer's build_logical_schema — Exasol has no struct type,
    // so it surfaces as a JSON-fallback VARCHAR and falls through to NULL /
    // required-error downstream rather than a bogus default literal.
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
