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
#[path = "field_id_projection_tests.rs"]
mod tests;
