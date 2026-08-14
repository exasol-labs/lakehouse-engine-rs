//! Column binding for the scan's logical schema: logical-schema construction,
//! the one binding pass that claims each physical field for a logical field
//! ([`bind_columns`]), the `PhysicalExprAdapter` that installs it
//! ([`FieldIdExprAdapterFactory`] / `FieldIdExprAdapter`), and `initial-default`
//! reconstruction.
//!
//! A logical field declares HOW it binds, and the pass dispatches on that
//! declaration: by field-id (an Iceberg field-id, or a Delta `id` column-mapping
//! id) against a physical field's embedded `PARQUET:field_id`; by a declared
//! physical name (Delta `name` column mapping) against the physical column's own
//! name; or by identity (Delta `none` column mapping) against the logical name
//! itself. Iceberg additionally falls back to `schema.name-mapping.default` and
//! then to the physical name.

use crate::scan::spec::NameMappingEntry;
use datafusion::physical_expr_adapter::{
    DefaultPhysicalExprAdapterFactory, PhysicalExprAdapter, PhysicalExprAdapterFactory,
};
use datafusion::scalar::ScalarValue;
use std::collections::HashMap;
use std::sync::Arc;

/// Arrow field-metadata key that carries a column's field-id.
///
/// Re-exported from the arrow-58 parquet crate so the whole scan crate has one
/// canonical spelling; [`build_logical_arrow_schema`] tags a field-id-bound
/// logical field with it (and ONLY such a field), and [`bind_columns`] reads it
/// off both the logical and physical schemas.
pub(crate) use parquet::arrow::PARQUET_FIELD_ID_META_KEY;

/// Read the field-id off an Arrow field, if present.
///
/// Returns `None` when the field carries no `PARQUET:field_id` metadata (an older
/// writer) or the value is not a parseable `i32`.
fn field_id_of(field: &arrow::datatypes::Field) -> Option<i32> {
    field
        .metadata()
        .get(PARQUET_FIELD_ID_META_KEY)
        .and_then(|v| v.parse::<i32>().ok())
}

/// Factory for the column-binding [`PhysicalExprAdapter`], installed on the
/// `ListingTableConfig` via `with_expr_adapter_factory`. The Parquet opener calls
/// [`Self::create`] once per file, so files with divergent physical layouts each
/// bind correctly.
///
/// It does NOT reimplement schema adaptation. It composes two steps around
/// [`DefaultPhysicalExprAdapter`]:
///
/// 1. Feed the default a physical schema renamed to the logical names its fields
///    were claimed by (see [`bind_columns`] for the field-id / declared-physical-name
///    / name-mapping / identity resolution order). The default then resolves each
///    logical column to the correct physical index and reuses its own behavior for
///    the rest — nullable-missing → NULL literal, type divergence → cast,
///    required-missing → error. Every binding strategy therefore shares ONE set of
///    adaptation semantics rather than getting a thinner path of its own.
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
/// name-based lookups succeed while keeping the binding.
///
/// Carries the query's whole [`FieldIdResolution`] — the binding tables and the
/// reconstructed `initial-default` values, resolved once in the VS and threaded
/// down via [`register_file_list`] / [`PositionalDeleteScanTable`]. Holding that
/// one value rather than mirroring its members keeps a new binding table from
/// needing a second home here.
#[derive(Debug)]
pub(crate) struct FieldIdExprAdapterFactory {
    pub(crate) resolution: FieldIdResolution,
}

/// Per-query column-binding metadata for one scan side (fact or dimension),
/// resolved once in the VS alongside the logical schema: the tables a physical
/// field is matched against, plus the reconstructed `initial-default` values an
/// absent logical column falls back to. Grouped into one value so
/// [`register_file_list`] threads a single argument through
/// [`crate::scan::positional_deletes::PositionalDeleteScanTable::new`], which in
/// turn hands the same value to [`FieldIdExprAdapterFactory`] on each
/// [`crate::scan::positional_deletes::PositionalDeleteScanTable::scan`] call.
#[derive(Debug, Clone)]
pub(crate) struct FieldIdResolution {
    /// Flattened `schema.name-mapping.default` entries: the table-level
    /// physical-name → field-id fallback for files whose columns carry no
    /// field-id at all. Empty when the table declares no name mapping.
    pub(crate) name_mapping: Vec<NameMappingEntry>,
    /// Logical column name keyed by the physical name that column DECLARES,
    /// built by [`index_declared_physical_names`]. Empty for a table whose
    /// fields all bind by field-id or by identity — every Iceberg table.
    pub(crate) declared_physical_names: HashMap<String, String>,
    /// Reconstructed `initial-default` values keyed by LOGICAL COLUMN NAME —
    /// the one key every logical field carries, now that a field-id is optional,
    /// and stable under projection as a column index would not be. Empty when no
    /// field declares a default.
    pub(crate) defaults: HashMap<String, ScalarValue>,
}

impl PhysicalExprAdapterFactory for FieldIdExprAdapterFactory {
    fn create(
        &self,
        logical_file_schema: arrow::datatypes::SchemaRef,
        physical_file_schema: arrow::datatypes::SchemaRef,
    ) -> datafusion::error::Result<Arc<dyn PhysicalExprAdapter>> {
        // Delegate to the default adapter over a physical schema whose fields are
        // renamed to the logical names that claimed them. The default then resolves
        // each logical column to the correct physical INDEX (order is preserved by
        // the rename) and applies cast / NULL-fill / required-missing-error against
        // the logical field — the reused behavior.
        let binding = bind_columns(
            &logical_file_schema,
            &physical_file_schema,
            &self.resolution,
        );

        // The absent-with-default fill map is PER FILE: a logical column that NO
        // physical field of THIS file claimed and that carries a reconstructed
        // default is keyed by its logical column index (what an incoming `Column`
        // carries) so `rewrite` can substitute a `Literal(<default>)` BEFORE
        // delegating. A claimed column is present and is NEVER defaulted, even if a
        // default exists.
        let absent_default_by_index: HashMap<usize, ScalarValue> = logical_file_schema
            .fields()
            .iter()
            .enumerate()
            .filter(|(_, field)| !binding.bound_logical_names.contains(field.name().as_str()))
            .filter_map(|(index, field)| {
                self.resolution
                    .defaults
                    .get(field.name())
                    .map(|value| (index, value.clone()))
            })
            .collect();

        let inner = DefaultPhysicalExprAdapterFactory
            .create(logical_file_schema, Arc::clone(&binding.renamed_physical))?;
        Ok(Arc::new(FieldIdExprAdapter {
            inner,
            physical_file_schema,
            absent_default_by_index,
        }))
    }
}

/// Wraps [`DefaultPhysicalExprAdapter`] so column binding reaches the projection
/// READ path, not just filter/predicate expressions.
///
/// The default adapter resolves columns by NAME. We feed it a physical schema
/// renamed to the logical names that claimed its fields (so it binds by whichever
/// key the logical field declares and reuses its cast / NULL-fill /
/// required-missing logic), which makes it emit `Column`s carrying
/// the LOGICAL name at the correct physical index. But every downstream consumer
/// in the Parquet opener — `build_projection_read_plan`, `reassign_expr_columns`,
/// and `make_projector` — resolves those `Column`s by NAME against the REAL
/// physical file schema (`score`, not `rating`). Left as-is a renamed column
/// projection fails with `Unable to get field named "rating"`.
///
/// So after delegating, we walk the rewritten expression and rename each
/// resolved `Column` back to the real physical field NAME at its (already
/// correct) index. Order is preserved by [`bind_columns`], so the column's index
/// still points at the right physical slot; only the name must be restored so the
/// opener's name-based lookups succeed. NULL-filled columns become `Literal`s (no
/// `Column` to rename) and pass through untouched.
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

/// One file's column binding: the physical schema renamed to the logical names
/// that claimed its fields, and the set of logical column names some physical
/// field claimed.
///
/// Both views come out of ONE pass because they are one decision seen twice: the
/// delegate adapter resolves a logical column by NAME against `renamed_physical`,
/// so a logical name present there is exactly a column this file supplies, and a
/// logical name absent from it is exactly a column the per-file
/// `initial-default` / NULL fill must cover.
struct ColumnBinding {
    renamed_physical: arrow::datatypes::SchemaRef,
    bound_logical_names: std::collections::HashSet<String>,
}

/// Bind one file's physical fields to the logical schema, renaming each physical
/// field to the logical name that claims it and preserving field order, type,
/// nullability, and metadata.
///
/// Resolution per physical field, first match wins:
/// 1. An embedded `PARQUET:field_id` matching a logical field's id → that logical
///    field's name. This is the Iceberg binding and the Delta `id`
///    column-mapping binding.
/// 2. A logical field DECLARING this physical field's name as its physical name →
///    that logical field's name (Delta `name` column mapping). It sits ABOVE the
///    name-mapping because a per-column declaration read from the table's own
///    metadata is authoritative while a name-mapping entry is a table-level
///    fallback, and it is reached even for a physical field carrying a field-id no
///    logical field declares — a `name`-mapped table's logical fields carry no ids
///    for step 1 to match.
/// 3. `name_mapping` mapping this physical name to a field-id present in the
///    logical schema → that logical field's name (Iceberg column-projection rule
///    2). Consulted ONLY for a physical field with NO embedded field-id, which is
///    the case `schema.name-mapping.default` exists for.
/// 4. Otherwise the physical name is kept unchanged. That is both the
///    physical-name fallback for a field-id-bound field and what makes an
///    identity-bound field (no field-id, no declared physical name) resolve — the
///    delegate's name lookup does the work in both cases. A dropped column keeps a
///    name no logical field carries and is simply never referenced.
///
/// A logical field counts as BOUND when the renamed schema supplies its name,
/// which is precisely the question the delegate will ask, so the fill seam and the
/// delegate can never disagree about whether a column is present in this file.
///
/// Assumes post-rename logical names are unique among the referenced physical
/// fields, and that no two logical fields declare the same physical name (the
/// Delta protocol guarantees the latter). Name collisions from
/// drop+rename-into-a-reused-name are a distinct, still-open concern, NOT resolved
/// by (or in scope for) name-mapping support: `schema.name-mapping.default` maps
/// CURRENT-state physical names to field-ids, so it cannot disambiguate a dropped
/// column whose old physical name was later reused by an unrelated field.
fn bind_columns(
    logical: &arrow::datatypes::Schema,
    physical: &arrow::datatypes::Schema,
    resolution: &FieldIdResolution,
) -> ColumnBinding {
    use std::collections::{HashMap, HashSet};

    let logical_name_by_id: HashMap<i32, &str> = logical
        .fields()
        .iter()
        .filter_map(|f| field_id_of(f).map(|id| (id, f.name().as_str())))
        .collect();

    let field_id_by_physical_name: HashMap<&str, i32> = resolution
        .name_mapping
        .iter()
        .map(|entry| (entry.name.as_str(), entry.field_id))
        .collect();

    let renamed_fields: Vec<arrow::datatypes::FieldRef> = physical
        .fields()
        .iter()
        .map(|physical_field| {
            let physical_name = physical_field.name().as_str();
            let embedded_id = field_id_of(physical_field);
            let logical_name = embedded_id
                .and_then(|id| logical_name_by_id.get(&id).copied())
                .or_else(|| {
                    resolution
                        .declared_physical_names
                        .get(physical_name)
                        .map(String::as_str)
                })
                .or_else(|| {
                    // Iceberg rule 2 covers only columns written without a field-id.
                    if embedded_id.is_some() {
                        return None;
                    }
                    field_id_by_physical_name
                        .get(physical_name)
                        .and_then(|id| logical_name_by_id.get(id).copied())
                });
            match logical_name {
                Some(logical_name) if logical_name != physical_name => {
                    Arc::new(physical_field.as_ref().clone().with_name(logical_name))
                }
                _ => Arc::clone(physical_field),
            }
        })
        .collect();

    let supplied_names: HashSet<&str> = renamed_fields
        .iter()
        .map(|field| field.name().as_str())
        .collect();
    let bound_logical_names: HashSet<String> = logical
        .fields()
        .iter()
        .map(|field| field.name())
        .filter(|name| supplied_names.contains(name.as_str()))
        .cloned()
        .collect();

    ColumnBinding {
        renamed_physical: Arc::new(arrow::datatypes::Schema::new_with_metadata(
            renamed_fields,
            physical.metadata().clone(),
        )),
        bound_logical_names,
    }
}

/// Build the logical Arrow schema from the spec's query-time logical schema.
///
/// Each field carries the schema's declared nullability (Iceberg `optional`) and
/// an Arrow data type reconstructed from the compact tag via
/// [`arrow_type_from_tag`]. A field that declares a field-id is ALSO tagged with
/// it (`PARQUET:field_id`) so [`bind_columns`] can match a physical field's
/// embedded id against it. A field that binds by a declared physical name or by
/// identity is tagged with NO field-id: a synthesized id is a value no writer ever
/// put in any file, and tagging one here would invite a false match against a file
/// that does carry ids.
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
            );
            let field = match lf.field_id {
                Some(field_id) => field.with_metadata(HashMap::from([(
                    PARQUET_FIELD_ID_META_KEY.to_string(),
                    field_id.to_string(),
                )])),
                None => field,
            };
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
/// `logical column name → ScalarValue` map, built ONCE from the logical schema and
/// handed to the [`FieldIdExprAdapterFactory`] so the per-file fill seam can look a
/// default up for a column no physical field claimed. Keyed by logical name
/// because that is the one key every logical field carries — a field-id belongs to
/// one binding strategy only, and a column index is not stable under projection.
/// Fields with no `initial_default` contribute no entry; a reconstruction failure
/// aborts with a clean `Err` (never a panic).
pub(super) fn reconstruct_initial_defaults(
    logical_schema: &[crate::scan::spec::LogicalField],
) -> Result<HashMap<String, ScalarValue>, String> {
    logical_schema
        .iter()
        .filter_map(|lf| {
            lf.initial_default.as_ref().map(|encoded| {
                reconstruct_initial_default(&lf.arrow_type, encoded)
                    .map(|value| (lf.name.clone(), value))
            })
        })
        .collect()
}

/// Index the logical schema's DECLARED physical names as
/// `physical name → logical column name`, built ONCE per registration and handed
/// to the [`FieldIdExprAdapterFactory`] so [`bind_columns`] can let a logical field
/// claim the physical column it names (step 2 — Delta `name` column mapping).
///
/// A field that binds by field-id or by identity declares no physical name and
/// contributes no entry, so the index is empty for every Iceberg table and step 2
/// is then a no-op.
pub(super) fn index_declared_physical_names(
    logical_schema: &[crate::scan::spec::LogicalField],
) -> HashMap<String, String> {
    logical_schema
        .iter()
        .filter_map(|lf| {
            lf.physical_name
                .as_ref()
                .map(|physical| (physical.clone(), lf.name.clone()))
        })
        .collect()
}

#[cfg(test)]
#[path = "field_id_projection_tests.rs"]
mod tests;
