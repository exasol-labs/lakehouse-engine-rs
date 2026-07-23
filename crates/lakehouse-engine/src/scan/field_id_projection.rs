//! Iceberg field-id projection: logical-schema construction, field-id-first
//! column binding (`FieldIdExprAdapterFactory` / `FieldIdExprAdapter`),
//! physical→logical rename resolution, and `initial-default` reconstruction.

use crate::scan::spec::NameMappingEntry;
use datafusion::physical_expr_adapter::{
    DefaultPhysicalExprAdapterFactory, PhysicalExprAdapter, PhysicalExprAdapterFactory,
};
use datafusion::scalar::ScalarValue;
use std::collections::HashMap;
use std::sync::Arc;

/// Arrow field-metadata key that carries the Iceberg field-id.
///
/// Re-exported from the arrow-58 parquet crate so the whole scan crate has one
/// canonical spelling; the logical-schema builder tags each field with it and
/// [`rename_physical_to_logical`] reads it off both the logical and physical schemas.
pub(crate) use parquet::arrow::PARQUET_FIELD_ID_META_KEY;

/// Read the Iceberg field-id off an Arrow field, if present.
///
/// Returns `None` when the field carries no `PARQUET:field_id` metadata (an older
/// writer) or the value is not a parseable `i32`.
fn field_id_of(field: &arrow::datatypes::Field) -> Option<i32> {
    field
        .metadata()
        .get(PARQUET_FIELD_ID_META_KEY)
        .and_then(|v| v.parse::<i32>().ok())
}

/// Factory for a field-id-aware [`PhysicalExprAdapter`], installed on the
/// `ListingTableConfig` via `with_expr_adapter_factory`. The Parquet opener calls
/// [`Self::create`] once per file, so files with divergent physical layouts each
/// bind correctly.
///
/// It does NOT reimplement schema adaptation. It composes two steps around
/// [`DefaultPhysicalExprAdapter`]:
///
/// 1. Feed the default a physical schema renamed to logical names by field-id
///    (see [`rename_physical_to_logical`]). The default then resolves each logical
///    column to the correct physical index and reuses its own behavior for the
///    rest — nullable-missing → NULL literal, type divergence → cast,
///    required-missing → error.
/// 2. Rename the default's OUTPUT columns back to the real physical names (at
///    their already-correct indices) — see [`FieldIdExprAdapter`].
///
/// # Why the output must be renamed back (the E2E `rating`/`score` failure)
///
/// The default adapter resolves columns by NAME, so feeding it logical names on
/// both sides makes it emit `Column`s carrying the LOGICAL name (`rating`). But in
/// DataFusion 54 the Parquet opener applies the expr adapter to the PROJECTION as
/// well as the filter, and every downstream consumer of the rewritten projection —
/// `build_projection_read_plan`, `reassign_expr_columns`, and `make_projector` —
/// resolves those `Column`s by NAME against the REAL physical file schema
/// (`score`). A projected `Column("rating")` therefore fails with
/// `Unable to get field named "rating"`. Renaming the output back to the real
/// physical name (order is preserved, so the index is already right) makes those
/// name-based lookups succeed while keeping the field-id binding.
///
/// Carries the query's flattened `schema.name-mapping.default` entries
/// (resolved once in the VS and threaded down via [`register_file_list`] /
/// [`PositionalDeleteScanTable`]), so [`Self::create`] can hand them to
/// [`rename_physical_to_logical`] for the no-embedded-field-id resolution step.
/// Empty when the table has no name-mapping property, in which case resolution
/// is unchanged from the field-id / physical-name fallback.
///
/// Also carries the query's reconstructed Iceberg `initial-default` values keyed
/// by field-id (built once from the logical schema via
/// [`reconstruct_initial_defaults`]). [`Self::create`] uses them to build the
/// per-file absent-with-default fill map so [`FieldIdExprAdapter::rewrite`] can
/// emit a `Literal(<default>)` for an absent field (Iceberg column-projection
/// rule 3) BEFORE delegating to the default adapter. Empty when no field carries
/// a default, in which case the absent-field behavior is unchanged (nullable →
/// NULL, required → clean error).
#[derive(Debug)]
pub(crate) struct FieldIdExprAdapterFactory {
    pub(crate) name_mapping: Vec<NameMappingEntry>,
    pub(crate) defaults: HashMap<i32, ScalarValue>,
}

/// Per-query field-id resolution metadata for one scan side (fact or
/// dimension): the flattened `schema.name-mapping.default` entries and the
/// reconstructed Iceberg `initial-default` values keyed by field-id, resolved
/// once in the VS alongside the logical schema. Grouped into one value so
/// [`register_file_list`] threads a single argument through
/// [`crate::scan::positional_deletes::PositionalDeleteScanTable::new`], which
/// in turn hands the same two values to [`FieldIdExprAdapterFactory`] on each
/// [`crate::scan::positional_deletes::PositionalDeleteScanTable::scan`] call.
#[derive(Debug, Clone)]
pub(crate) struct FieldIdResolution {
    pub(crate) name_mapping: Vec<NameMappingEntry>,
    pub(crate) defaults: HashMap<i32, ScalarValue>,
}

impl PhysicalExprAdapterFactory for FieldIdExprAdapterFactory {
    fn create(
        &self,
        logical_file_schema: arrow::datatypes::SchemaRef,
        physical_file_schema: arrow::datatypes::SchemaRef,
    ) -> datafusion::error::Result<Arc<dyn PhysicalExprAdapter>> {
        // Delegate to the default adapter over a physical schema whose fields are
        // renamed to their logical names by field-id. The default then resolves
        // each logical column to the correct physical INDEX (order is preserved by
        // the rename) and applies cast / NULL-fill / required-missing-error against
        // the logical field — the reused behavior.
        let renamed_physical = rename_physical_to_logical(
            &logical_file_schema,
            &physical_file_schema,
            &self.name_mapping,
        );

        // The absent-with-default fill map is PER FILE: a logical field-id absent
        // from THIS physical file that carries a reconstructed default is keyed by
        // its logical column index (what an incoming `Column` carries) so
        // `rewrite` can substitute a `Literal(<default>)` BEFORE delegating. A
        // field-id resolved by this file (embedded id or name-mapping) is present
        // and is NEVER defaulted, even if a default exists.
        let resolved_ids = resolved_logical_field_ids(
            &logical_file_schema,
            &physical_file_schema,
            &self.name_mapping,
        );
        let absent_default_by_index: HashMap<usize, ScalarValue> = logical_file_schema
            .fields()
            .iter()
            .enumerate()
            .filter_map(|(index, field)| {
                let id = field_id_of(field)?;
                if resolved_ids.contains(&id) {
                    return None;
                }
                self.defaults.get(&id).map(|value| (index, value.clone()))
            })
            .collect();

        let inner = DefaultPhysicalExprAdapterFactory
            .create(logical_file_schema, Arc::clone(&renamed_physical))?;
        Ok(Arc::new(FieldIdExprAdapter {
            inner,
            physical_file_schema,
            absent_default_by_index,
        }))
    }
}

/// Wraps [`DefaultPhysicalExprAdapter`] so field-id resolution reaches the
/// projection READ path, not just filter/predicate expressions.
///
/// The default adapter resolves columns by NAME. We feed it a physical schema
/// renamed to logical names (so it binds by field-id and reuses its cast /
/// NULL-fill / required-missing logic), which makes it emit `Column`s carrying
/// the LOGICAL name at the correct physical index. But every downstream consumer
/// in the Parquet opener — `build_projection_read_plan`, `reassign_expr_columns`,
/// and `make_projector` — resolves those `Column`s by NAME against the REAL
/// physical file schema (`score`, not `rating`). Left as-is a renamed column
/// projection fails with `Unable to get field named "rating"`.
///
/// So after delegating, we walk the rewritten expression and rename each
/// resolved `Column` back to the real physical field NAME at its (already
/// correct) index. Order is preserved by [`rename_physical_to_logical`], so the
/// column's index still points at the right physical slot; only the name must be
/// restored so the opener's name-based lookups succeed. NULL-filled columns
/// become `Literal`s (no `Column` to rename) and pass through untouched.
#[derive(Debug)]
struct FieldIdExprAdapter {
    inner: Arc<dyn PhysicalExprAdapter>,
    physical_file_schema: arrow::datatypes::SchemaRef,
    /// Absent-with-default fill map for THIS file, keyed by LOGICAL column index
    /// (the index an incoming `Column` carries). Populated only for logical
    /// field-ids absent from this physical file that carry a reconstructed
    /// Iceberg `initial-default`; a present field-id is never an entry, so a
    /// real-value binding is never overridden.
    absent_default_by_index: HashMap<usize, ScalarValue>,
}

impl PhysicalExprAdapter for FieldIdExprAdapter {
    fn rewrite(
        &self,
        expr: Arc<dyn datafusion::physical_expr::PhysicalExpr>,
    ) -> datafusion::error::Result<Arc<dyn datafusion::physical_expr::PhysicalExpr>> {
        use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode};
        use datafusion::physical_expr::PhysicalExpr;
        use datafusion::physical_expr::expressions::{Column, Literal};

        // Intercept the absent-with-default case BEFORE delegating: the default
        // adapter NULL-fills a nullable-absent field and ERRORS on a
        // required-absent field, so an absent field's Iceberg `initial-default`
        // (column-projection rule 3) must be substituted first. The incoming
        // `Column` indices are into the logical file schema, which is how
        // `absent_default_by_index` is keyed.
        let intercepted = expr
            .transform_down(|node| {
                if let Some(column) = node.downcast_ref::<Column>()
                    && let Some(default) = self.absent_default_by_index.get(&column.index())
                {
                    return Ok(Transformed::yes(
                        Arc::new(Literal::new(default.clone())) as Arc<dyn PhysicalExpr>
                    ));
                }
                Ok(Transformed::no(node))
            })
            .data()?;

        // Delegate the remainder: a present field-id binds to its real physical
        // values; an absent field with NO default NULL-fills (nullable) or errors
        // cleanly (required) inside the default adapter, unchanged.
        let rewritten = self.inner.rewrite(intercepted)?;

        // Rename each resolved logical `Column` name back to the real physical
        // field NAME at its (already correct) index so the opener's name-based
        // lookups succeed. Injected `Literal`s carry no `Column` and pass through.
        rewritten
            .transform_down(|node| {
                if let Some(column) = node.downcast_ref::<Column>() {
                    let real_name = self.physical_file_schema.field(column.index()).name();
                    if real_name != column.name() {
                        return Ok(Transformed::yes(Arc::new(Column::new(
                            real_name,
                            column.index(),
                        ))));
                    }
                }
                Ok(Transformed::no(node))
            })
            .data()
    }
}

/// Rename each physical field to the logical name that shares its Iceberg
/// field-id, preserving field order, type, nullability, and metadata.
///
/// Resolution per physical field:
/// 1. If it carries a `PARQUET:field_id` matching a logical field's id → adopt
///    that logical field's name (this is the rename/field-id binding). An
///    embedded field-id is authoritative: if it is absent from the logical
///    schema the physical name is kept and the name-mapping is NOT consulted.
/// 2. Else if it carries NO embedded field-id and `name_mapping` maps its
///    physical name to a field-id present in the logical schema → adopt that
///    logical field's name (Iceberg column-projection rule #2, honoring
///    `schema.name-mapping.default`).
/// 3. Otherwise (no field-id and no covering name-mapping entry, or a mapped
///    field-id absent from the logical schema) → keep the physical name
///    unchanged, which makes the default adapter's name lookup act as the
///    physical-name fallback (and leaves dropped columns unreferenced).
///
/// Assumes that post-rename logical names are unique among the referenced physical
/// fields. Name collisions from drop+rename-into-a-reused-name are a distinct,
/// still-open concern, NOT resolved by (or in scope for) name-mapping support:
/// `schema.name-mapping.default` maps CURRENT-state physical names to field-ids,
/// so it cannot disambiguate a dropped column whose old physical name was later
/// reused by an unrelated field.
fn rename_physical_to_logical(
    logical: &arrow::datatypes::Schema,
    physical: &arrow::datatypes::Schema,
    name_mapping: &[NameMappingEntry],
) -> arrow::datatypes::SchemaRef {
    use std::collections::HashMap;

    let logical_name_by_id: HashMap<i32, &str> = logical
        .fields()
        .iter()
        .filter_map(|f| field_id_of(f).map(|id| (id, f.name().as_str())))
        .collect();

    let field_id_by_physical_name: HashMap<&str, i32> = name_mapping
        .iter()
        .map(|entry| (entry.name.as_str(), entry.field_id))
        .collect();

    let renamed_fields: Vec<arrow::datatypes::FieldRef> = physical
        .fields()
        .iter()
        .map(|physical_field| {
            let logical_name = match field_id_of(physical_field) {
                // An embedded field-id is authoritative: resolve it, or keep the
                // physical name if that id is absent from the logical schema. The
                // name-mapping is never consulted for a field that carries an id.
                Some(id) => logical_name_by_id.get(&id).copied(),
                // No embedded field-id: consult the name-mapping (step 2), then
                // fall back to the physical name (step 3).
                None => field_id_by_physical_name
                    .get(physical_field.name().as_str())
                    .and_then(|id| logical_name_by_id.get(id).copied()),
            };
            match logical_name {
                Some(logical_name) if logical_name != physical_field.name() => {
                    Arc::new(physical_field.as_ref().clone().with_name(logical_name))
                }
                _ => Arc::clone(physical_field),
            }
        })
        .collect();

    Arc::new(arrow::datatypes::Schema::new_with_metadata(
        renamed_fields,
        physical.metadata().clone(),
    ))
}

/// Build the logical Arrow schema from the spec's query-time logical schema.
///
/// Each field is tagged with its Iceberg field-id (`PARQUET:field_id`) so
/// [`FieldIdExprAdapterFactory`] can bind physical file columns to it by id, and
/// carries the schema's declared nullability (Iceberg `optional`). The Arrow data
/// type is reconstructed from the compact tag via [`arrow_type_from_tag`].
pub(super) fn build_logical_arrow_schema(
    logical_schema: &[crate::scan::spec::LogicalField],
) -> arrow::datatypes::SchemaRef {
    use crate::types::mapping::arrow_type_from_tag;
    use std::collections::HashMap;

    let fields: Vec<arrow::datatypes::FieldRef> = logical_schema
        .iter()
        .map(|lf| {
            let field = arrow::datatypes::Field::new(
                &lf.name,
                arrow_type_from_tag(&lf.arrow_type),
                lf.nullable,
            )
            .with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                lf.field_id.to_string(),
            )]));
            Arc::new(field)
        })
        .collect();

    Arc::new(arrow::datatypes::Schema::new(fields))
}

/// Reconstruct a DataFusion [`ScalarValue`] from a [`LogicalField`]'s Arrow-type
/// tag and its encoded `initial_default` text — the scan-side inverse of the VS
/// layer's `encode_initial_default`.
///
/// The tag fixes the target `ScalarValue` variant (and its timezone /
/// precision / scale) via [`arrow_type_from_tag`], so the reconstructed value's
/// implied Arrow type matches the logical schema field built by
/// [`build_logical_arrow_schema`] exactly. The encoded text is the RAW primitive
/// scalar (a decimal integer for a temporal's days / micros / nanos, an `i128`
/// mantissa for a decimal), parsed directly here with no second temporal /
/// decimal parse — mirroring the `PrimitiveType`-keyed dispatch in
/// `iceberg_predicate::literal_to_datum` and `convert::arrow_value_at`.
///
/// A parse failure returns a clean `Err(String)` — never a panic — naming the
/// tag and the (inherently credential-free) encoded scalar, so a malformed spec
/// surfaces diagnostically rather than aborting the VM.
///
/// [`LogicalField`]: crate::scan::spec::LogicalField
pub(crate) fn reconstruct_initial_default(
    arrow_type_tag: &str,
    encoded: &str,
) -> Result<ScalarValue, String> {
    use crate::types::mapping::arrow_type_from_tag;
    use arrow::datatypes::{DataType, TimeUnit};

    fn parse_scalar<T: std::str::FromStr>(encoded: &str, tag: &str) -> Result<T, String> {
        encoded.parse::<T>().map_err(|_| {
            format!("initial-default '{encoded}' is not a valid value for arrow type tag '{tag}'")
        })
    }

    // Reconstruct against the SAME DataType the logical schema field is built
    // from, so the reconstructed value's timezone / precision / scale line up.
    let value = match arrow_type_from_tag(arrow_type_tag) {
        DataType::Boolean => ScalarValue::Boolean(Some(parse_scalar(encoded, arrow_type_tag)?)),
        DataType::Int32 => ScalarValue::Int32(Some(parse_scalar(encoded, arrow_type_tag)?)),
        DataType::Int64 => ScalarValue::Int64(Some(parse_scalar(encoded, arrow_type_tag)?)),
        DataType::Float32 => ScalarValue::Float32(Some(parse_scalar(encoded, arrow_type_tag)?)),
        DataType::Float64 => ScalarValue::Float64(Some(parse_scalar(encoded, arrow_type_tag)?)),
        DataType::Utf8 => ScalarValue::Utf8(Some(encoded.to_string())),
        DataType::Date32 => ScalarValue::Date32(Some(parse_scalar(encoded, arrow_type_tag)?)),
        DataType::Timestamp(TimeUnit::Microsecond, tz) => {
            ScalarValue::TimestampMicrosecond(Some(parse_scalar(encoded, arrow_type_tag)?), tz)
        }
        DataType::Timestamp(TimeUnit::Nanosecond, tz) => {
            ScalarValue::TimestampNanosecond(Some(parse_scalar(encoded, arrow_type_tag)?), tz)
        }
        DataType::Decimal128(precision, scale) => ScalarValue::Decimal128(
            Some(parse_scalar(encoded, arrow_type_tag)?),
            precision,
            scale,
        ),
        other => {
            return Err(format!(
                "initial-default reconstruction unsupported for arrow type '{other}' (tag '{arrow_type_tag}')"
            ));
        }
    };
    Ok(value)
}

/// Reconstruct every field's encoded Iceberg `initial-default` into a
/// `field_id → ScalarValue` map, built ONCE from the logical schema and handed
/// to the [`FieldIdExprAdapterFactory`] so the per-file fill seam can look a
/// default up by field-id. Fields with no `initial_default` contribute no entry;
/// a reconstruction failure aborts with a clean `Err` (never a panic).
pub(super) fn reconstruct_initial_defaults(
    logical_schema: &[crate::scan::spec::LogicalField],
) -> Result<HashMap<i32, ScalarValue>, String> {
    logical_schema
        .iter()
        .filter_map(|lf| {
            lf.initial_default.as_ref().map(|encoded| {
                reconstruct_initial_default(&lf.arrow_type, encoded)
                    .map(|value| (lf.field_id, value))
            })
        })
        .collect()
}

/// The set of logical field-ids that SOME physical field in this file resolves
/// to — the complement of the "absent from this file" set the fill seam needs.
///
/// Mirrors [`rename_physical_to_logical`]'s resolution exactly: an embedded
/// `PARQUET:field_id` is authoritative (and only counts when present in the
/// logical schema); a physical field with no embedded id resolves through
/// `name_mapping` (Iceberg column-projection rule 2). A physical field that
/// resolves to nothing (dropped column, or an embedded id absent from the
/// logical schema) contributes no id.
fn resolved_logical_field_ids(
    logical: &arrow::datatypes::Schema,
    physical: &arrow::datatypes::Schema,
    name_mapping: &[NameMappingEntry],
) -> std::collections::HashSet<i32> {
    let logical_ids: std::collections::HashSet<i32> = logical
        .fields()
        .iter()
        .filter_map(|f| field_id_of(f))
        .collect();

    let field_id_by_physical_name: HashMap<&str, i32> = name_mapping
        .iter()
        .map(|entry| (entry.name.as_str(), entry.field_id))
        .collect();

    physical
        .fields()
        .iter()
        .filter_map(|physical_field| match field_id_of(physical_field) {
            Some(id) if logical_ids.contains(&id) => Some(id),
            Some(_) => None,
            None => field_id_by_physical_name
                .get(physical_field.name().as_str())
                .copied()
                .filter(|id| logical_ids.contains(id)),
        })
        .collect()
}

#[cfg(test)]
mod tests {
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
        spec.logical_schema = logical;
        // The adapter pushes uppercase current-name projection.
        spec.projection = vec!["ID".into(), "RATING".into()];

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
            let mut writer =
                ArrowWriter::try_new(file, schema.clone(), None).expect("arrow writer");
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
        spec.logical_schema = logical;
        spec.projection = vec!["ID".into(), "RATING".into()];

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
                ScalarValue::TimestampNanosecond(
                    Some(1_700_000_000_000_000_000),
                    Some("UTC".into()),
                ),
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
        spec.logical_schema = cases
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

        for ((tag, encoded, expected), field) in cases.iter().zip(back.logical_schema.iter()) {
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
        let logical_json = serde_json::to_string(&back.logical_schema).unwrap();
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
                    NestedField::optional(1, "meta", struct_type)
                        .with_initial_default(struct_default),
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
}
