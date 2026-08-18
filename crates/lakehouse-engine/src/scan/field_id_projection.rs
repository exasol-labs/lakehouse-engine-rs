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
//!
//! A NESTED logical field declares that same choice for each of its own members
//! ([`NestedMembers`]), and [`resolve_nested_field`] recurses the one binding pass
//! into them, so a member's field-id or declared physical name means at depth
//! exactly what it means at the top.

use crate::scan::raw_scan::NESTED_JSON_RENDER_UDF_NAME;
use crate::scan::render_nested_column_as_json;
use crate::scan::spec::{NameMappingEntry, NestedField, NestedMembers};
use crate::types::mapping::needs_nested_json_rendering;
use arrow::array::{
    Array, ArrayRef, FixedSizeListArray, LargeListArray, ListArray, MapArray, RecordBatch,
    StructArray, new_null_array,
};
use arrow::datatypes::{DataType, Field, FieldRef, Fields};
use datafusion::error::DataFusionError;
use datafusion::logical_expr::ColumnarValue;
use datafusion::physical_expr::PhysicalExpr;
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

/// The binding keys ONE logical field offers a physical field, in the order
/// [`claim_logical`] tries them. Built from a top-level logical column or from a
/// nested [`NestedField`] alike, which is what lets one order serve both depths.
struct BindingKeys<'a> {
    name: &'a str,
    field_id: Option<i32>,
    physical_name: Option<&'a str>,
}

/// The physical side of one claim attempt: the facts [`claim_logical`] reads about
/// the physical field being matched against a logical field's [`BindingKeys`].
struct PhysicalKeys<'a> {
    name: &'a str,
    embedded_id: Option<i32>,
    mapped_field_id: Option<i32>,
}

fn claim_logical(physical: PhysicalKeys<'_>, logical: &[BindingKeys<'_>]) -> Option<usize> {
    let with_field_id = |wanted: i32| {
        logical
            .iter()
            .position(|keys| keys.field_id == Some(wanted))
    };
    physical
        .embedded_id
        .and_then(with_field_id)
        .or_else(|| {
            logical
                .iter()
                .position(|keys| keys.physical_name == Some(physical.name))
        })
        .or_else(|| match physical.embedded_id {
            Some(_) => None,
            None => physical.mapped_field_id.and_then(with_field_id),
        })
        .or_else(|| logical.iter().position(|keys| keys.name == physical.name))
}

/// One file's resolution of one nested column onto the nested tree the table
/// declares: the Arrow field the resolved column takes — logical member names, in
/// logical order — and how each physical member reaches its logical slot.
///
/// The other half of the nested read path from
/// [`render_nested_column_as_json`](crate::scan::render_nested_column_as_json),
/// which turns the resolved array into the JSON documents Exasol reads: without the
/// resolution those documents would be keyed by the file's own member names, which
/// on a column-mapped table are opaque identifiers rather than the names the table
/// declares.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(super) struct NestedResolution {
    field: FieldRef,
    members: ResolvedMembers,
}

/// How one resolved member's array is built from its physical counterpart.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum ResolvedMembers {
    /// Nothing to restructure: the physical member IS the resolved member.
    Verbatim,
    /// One slot per logical field, in the logical tree's order.
    Struct(Vec<StructSlot>),
    /// The element resolution of a `list`, `large_list`, or `fixed_size_list`.
    Element(Box<NestedResolution>),
    /// The key and value resolutions of a `map`'s entries. The entries field and
    /// sortedness are not stored here: they are already carried by the enclosing
    /// [`NestedResolution::field`], retyped to `DataType::Map` in
    /// [`resolve_nested_field`], and read from there by [`NestedResolution::apply`].
    Entries {
        key: Box<NestedResolution>,
        value: Box<NestedResolution>,
    },
}

/// One logical struct field's slot: the physical child index that claimed it, or
/// `None` when no member of this file's struct binds it.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct StructSlot {
    source: Option<usize>,
    resolution: NestedResolution,
}

/// Resolve one file's physical nested field onto the logical tree `members`
/// declares: each struct member renamed to the logical name that claims it, the
/// members reordered into logical order, an unclaimed physical member dropped, and a
/// logical field this file's struct does not carry null-filled.
///
/// A struct member is claimed by [`claim_logical`], the same order [`bind_columns`]
/// applies to a top-level column. The `schema.name-mapping.default` fallback is not
/// reachable at depth: its nested entries go unparsed (issue #28), so no nested
/// member can carry a mapped field-id for step 3 to match.
///
/// A list's element and a map's key and value are POSITIONAL — one child slot each —
/// so they resolve by recursion alone, with no name or id to match, and only when the
/// member is itself a container the descriptor names.
///
/// A descriptor disagreeing with the file's own type — a `struct` tree over a column
/// this file wrote as something else — resolves VERBATIM, leaving the physical-to-
/// logical adaptation the file schema already goes through to decide what such a file
/// means, rather than second-guessing it here.
pub(super) fn resolve_nested_field(
    physical: &FieldRef,
    members: &NestedMembers,
) -> NestedResolution {
    let resolved = match (physical.data_type(), members) {
        (DataType::Struct(children), NestedMembers::Struct { fields }) => {
            let slots = claim_struct_slots(children, fields);
            let resolved_fields: Fields = slots
                .iter()
                .map(|slot| Arc::clone(&slot.resolution.field))
                .collect();
            Some((
                DataType::Struct(resolved_fields),
                ResolvedMembers::Struct(slots),
            ))
        }
        (DataType::List(element), NestedMembers::List { element: inner }) => {
            let element = resolve_member(element, inner.as_deref());
            Some((
                DataType::List(Arc::clone(&element.field)),
                ResolvedMembers::Element(Box::new(element)),
            ))
        }
        (DataType::LargeList(element), NestedMembers::List { element: inner }) => {
            let element = resolve_member(element, inner.as_deref());
            Some((
                DataType::LargeList(Arc::clone(&element.field)),
                ResolvedMembers::Element(Box::new(element)),
            ))
        }
        (DataType::FixedSizeList(element, size), NestedMembers::List { element: inner }) => {
            let element = resolve_member(element, inner.as_deref());
            Some((
                DataType::FixedSizeList(Arc::clone(&element.field), *size),
                ResolvedMembers::Element(Box::new(element)),
            ))
        }
        (DataType::Map(entries, sorted), NestedMembers::Map { key, value }) => {
            match entries.data_type() {
                DataType::Struct(pair) if pair.len() == 2 => {
                    let key = resolve_member(&pair[0], key.as_deref());
                    let value = resolve_member(&pair[1], value.as_deref());
                    let resolved_entries: FieldRef =
                        Arc::new(entries.as_ref().clone().with_data_type(DataType::Struct(
                            Fields::from(vec![Arc::clone(&key.field), Arc::clone(&value.field)]),
                        )));
                    Some((
                        DataType::Map(resolved_entries, *sorted),
                        ResolvedMembers::Entries {
                            key: Box::new(key),
                            value: Box::new(value),
                        },
                    ))
                }
                _ => None,
            }
        }
        _ => None,
    };

    match resolved {
        Some((data_type, members)) => NestedResolution {
            field: Arc::new(physical.as_ref().clone().with_data_type(data_type)),
            members,
        },
        None => verbatim(physical),
    }
}

/// The resolution of a member nothing restructures: its own physical field, used as
/// it stands.
fn verbatim(physical: &FieldRef) -> NestedResolution {
    NestedResolution {
        field: Arc::clone(physical),
        members: ResolvedMembers::Verbatim,
    }
}

/// Resolve one POSITIONAL member — a list element, a map key, a map value — which
/// recurses only when the descriptor names it as a container of its own.
fn resolve_member(physical: &FieldRef, members: Option<&NestedMembers>) -> NestedResolution {
    match members {
        Some(members) => resolve_nested_field(physical, members),
        None => verbatim(physical),
    }
}

/// Claim each physical member of one struct for the logical field that binds it,
/// then lay the slots out in LOGICAL order. A physical member claimed by a slot
/// another member already claimed is left unclaimed, so a duplicated binding key
/// cannot silently overwrite the first match.
///
/// A slot no member claims is typed [`DataType::Null`]: the descriptor carries names
/// and binding keys, never types, so the only honest Arrow type for a field this file
/// does not carry is the one that holds nothing but nulls — which is also what the
/// JSON encoder renders as an explicit `null`.
fn claim_struct_slots(children: &Fields, logical: &[NestedField]) -> Vec<StructSlot> {
    let keys: Vec<BindingKeys<'_>> = logical
        .iter()
        .map(|field| BindingKeys {
            name: &field.name,
            field_id: field.field_id,
            physical_name: field.physical_name.as_deref(),
        })
        .collect();

    let mut claimed: Vec<Option<usize>> = vec![None; logical.len()];
    for (index, child) in children.iter().enumerate() {
        if let Some(slot) = claim_logical(
            PhysicalKeys {
                name: child.name(),
                embedded_id: field_id_of(child),
                mapped_field_id: None,
            },
            &keys,
        ) && claimed[slot].is_none()
        {
            claimed[slot] = Some(index);
        }
    }

    logical
        .iter()
        .zip(claimed)
        .map(|(field, source)| {
            let resolution = match source {
                Some(index) => {
                    let resolved = resolve_member(&children[index], field.nested.as_ref());
                    NestedResolution {
                        field: Arc::new(resolved.field.as_ref().clone().with_name(&field.name)),
                        members: resolved.members,
                    }
                }
                None => verbatim(&Arc::new(Field::new(&field.name, DataType::Null, true))),
            };
            StructSlot { source, resolution }
        })
        .collect()
}

impl NestedResolution {
    /// The Arrow field the resolved column takes: logical member names, in logical
    /// order. The column's own name and nullability are the physical field's, since
    /// only its MEMBERS are resolved here.
    pub(super) fn resolved_field(&self) -> &FieldRef {
        &self.field
    }

    /// Restructure one file's physical array into [`Self::resolved_field`], so a
    /// consumer reading member names off the result reads the table's logical names.
    ///
    /// Fails when the array is not the type the resolution was built from, which can
    /// only mean the resolution was applied to a different column.
    pub(super) fn apply(&self, array: &ArrayRef) -> datafusion::error::Result<ArrayRef> {
        match &self.members {
            ResolvedMembers::Verbatim => Ok(Arc::clone(array)),
            ResolvedMembers::Struct(slots) => {
                let source: &StructArray = downcast_array(array, "struct")?;
                let mut fields = Vec::with_capacity(slots.len());
                let mut columns = Vec::with_capacity(slots.len());
                for slot in slots {
                    fields.push(Arc::clone(&slot.resolution.field));
                    columns.push(match slot.source {
                        Some(index) => slot.resolution.apply(source.column(index))?,
                        None => new_null_array(slot.resolution.field.data_type(), source.len()),
                    });
                }
                Ok(Arc::new(StructArray::try_new_with_length(
                    Fields::from(fields),
                    columns,
                    source.nulls().cloned(),
                    source.len(),
                )?))
            }
            ResolvedMembers::Element(element) => apply_to_list(array, element),
            ResolvedMembers::Entries { key, value } => {
                let (entries, sorted) = match self.field.data_type() {
                    DataType::Map(entries, sorted) => (entries, *sorted),
                    other => {
                        return Err(DataFusionError::Execution(format!(
                            "nested resolution for a map column carries non-struct entries \
                             of type {other}"
                        )));
                    }
                };
                let source: &MapArray = downcast_array(array, "map")?;
                let pair = source.entries();
                let resolved_entries = StructArray::try_new_with_length(
                    match entries.data_type() {
                        DataType::Struct(fields) => fields.clone(),
                        other => {
                            return Err(DataFusionError::Execution(format!(
                                "nested resolution for a map column carries non-struct entries \
                                 of type {other}"
                            )));
                        }
                    },
                    vec![key.apply(pair.column(0))?, value.apply(pair.column(1))?],
                    pair.nulls().cloned(),
                    pair.len(),
                );
                Ok(Arc::new(MapArray::try_new(
                    Arc::clone(entries),
                    source.offsets().clone(),
                    resolved_entries?,
                    source.nulls().cloned(),
                    sorted,
                )?))
            }
        }
    }
}

/// Rebuild one list array over its resolved element, keeping the offsets, length,
/// and nulls the file wrote — only the element's own layout is resolved.
fn apply_to_list(
    array: &ArrayRef,
    element: &NestedResolution,
) -> datafusion::error::Result<ArrayRef> {
    let field = Arc::clone(&element.field);
    match array.data_type() {
        DataType::List(_) => {
            let source: &ListArray = downcast_array(array, "list")?;
            Ok(Arc::new(ListArray::try_new(
                field,
                source.offsets().clone(),
                element.apply(source.values())?,
                source.nulls().cloned(),
            )?))
        }
        DataType::LargeList(_) => {
            let source: &LargeListArray = downcast_array(array, "large_list")?;
            Ok(Arc::new(LargeListArray::try_new(
                field,
                source.offsets().clone(),
                element.apply(source.values())?,
                source.nulls().cloned(),
            )?))
        }
        DataType::FixedSizeList(_, size) => {
            let source: &FixedSizeListArray = downcast_array(array, "fixed_size_list")?;
            Ok(Arc::new(FixedSizeListArray::try_new(
                field,
                *size,
                element.apply(source.values())?,
                source.nulls().cloned(),
            )?))
        }
        other => Err(DataFusionError::Execution(format!(
            "nested resolution for a list column was applied to a column of type {other}"
        ))),
    }
}

/// Downcast one physical array to the Arrow array type its resolution was built
/// from, naming the mismatch rather than panicking on it.
fn downcast_array<'a, T: Array + 'static>(
    array: &'a ArrayRef,
    expected: &str,
) -> datafusion::error::Result<&'a T> {
    array.as_any().downcast_ref::<T>().ok_or_else(|| {
        DataFusionError::Execution(format!(
            "nested resolution for a {expected} column was applied to a column of type {}",
            array.data_type()
        ))
    })
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
    /// The nested member tree each nested column exposes, keyed by LOGICAL COLUMN
    /// NAME and built by [`index_nested_members`]. It carries the same binding keys
    /// at depth that a top-level field carries, so [`bind_columns`] resolves a
    /// file's own nested layout onto the logical one. Empty for a table with no
    /// list, struct, or map column.
    pub(crate) nested_members: HashMap<String, NestedMembers>,
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

        // Divert every nested column around the delegate's cast: the delegate is
        // handed ONE identical field on both sides for such a column, so it emits a
        // bare `Column` for it, which `rewrite` then replaces with the JSON-rendering
        // expression. Every primitive column keeps the delegate's own
        // physical-to-logical cast untouched.
        let nested = binding.nested_columns();
        let delegate_physical = binding.delegate_physical_schema(&nested);
        let delegate_logical =
            delegate_logical_schema(&logical_file_schema, &delegate_physical, &nested);

        let inner =
            DefaultPhysicalExprAdapterFactory.create(delegate_logical, delegate_physical)?;
        Ok(Arc::new(FieldIdExprAdapter {
            inner,
            physical_file_schema,
            absent_default_by_index,
            nested,
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
///
/// That same pass also DIVERTS a nested column: no cast can carry a `List`, `Struct`,
/// or `Map` to the `Utf8` the logical schema declares, so the renamed `Column` is
/// wrapped in a [`NestedJsonRenderExpr`] that renders the column instead — see
/// [`delegate_logical_schema`] for why the delegate never attempts the cast itself.
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
    /// The nested columns of THIS file, keyed by PHYSICAL column index — the index
    /// every `Column` the delegate emits carries — each with the resolution that
    /// restructures the file's array to the table's logical member names.
    nested: HashMap<usize, NestedResolution>,
}

impl PhysicalExprAdapter for FieldIdExprAdapter {
    fn rewrite(
        &self,
        expr: Arc<dyn PhysicalExpr>,
    ) -> datafusion::error::Result<Arc<dyn PhysicalExpr>> {
        use datafusion::common::tree_node::{
            Transformed, TransformedResult, TreeNode, TreeNodeRecursion,
        };
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
        // lookups succeed, and wrap a nested column in its JSON rendering — the one
        // physical-to-logical adaptation no cast can express. Injected `Literal`s
        // carry no `Column` and pass through.
        rewritten
            .transform_down(|node| {
                let Some((index, keeps_name)) = node.downcast_ref::<Column>().map(|column| {
                    let real_name = self.physical_file_schema.field(column.index()).name();
                    (column.index(), real_name == column.name())
                }) else {
                    return Ok(Transformed::no(node));
                };
                let bound: Arc<dyn PhysicalExpr> = match keeps_name {
                    true => node,
                    false => Arc::new(Column::new(
                        self.physical_file_schema.field(index).name(),
                        index,
                    )),
                };
                match self.nested.get(&index) {
                    // Stop the walk at the substituted node: descending into it would
                    // meet the same column again and wrap it endlessly.
                    Some(resolution) => Ok(Transformed::new(
                        Arc::new(NestedJsonRenderExpr::new(bound, resolution.clone()))
                            as Arc<dyn PhysicalExpr>,
                        true,
                        TreeNodeRecursion::Jump,
                    )),
                    None => Ok(Transformed::new(
                        bound,
                        !keeps_name,
                        TreeNodeRecursion::Continue,
                    )),
                }
            })
            .data()
    }
}

/// The expression [`FieldIdExprAdapter`] substitutes for a nested physical column:
/// the file's own array restructured to the table's logical member names, then
/// rendered as one JSON document per value — which is how the column reaches the plan
/// as the `Utf8` the logical schema declares for it.
///
/// It exists because no cast can do this: arrow-cast has no `Struct → Utf8` or
/// `Map → Utf8` kernel, and its `List → Utf8` kernel renders Arrow display text rather
/// than JSON. Its child stays a bare `Column` carrying the file's REAL physical name,
/// so the Parquet opener's name-based projection read plan, column reassignment, and
/// projector still resolve the column against the file schema and actually read it.
#[derive(Debug, Eq)]
struct NestedJsonRenderExpr {
    input: Arc<dyn PhysicalExpr>,
    resolution: NestedResolution,
}

// Written out rather than derived because rust-lang/rust#78808 blocks deriving either
// trait for a struct holding an `Arc<dyn Trait>`.
impl PartialEq for NestedJsonRenderExpr {
    fn eq(&self, other: &Self) -> bool {
        self.input.eq(&other.input) && self.resolution.eq(&other.resolution)
    }
}

impl std::hash::Hash for NestedJsonRenderExpr {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.input.hash(state);
        self.resolution.hash(state);
    }
}

impl NestedJsonRenderExpr {
    fn new(input: Arc<dyn PhysicalExpr>, resolution: NestedResolution) -> Self {
        Self { input, resolution }
    }
}

impl std::fmt::Display for NestedJsonRenderExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{NESTED_JSON_RENDER_UDF_NAME}({})", self.input)
    }
}

impl PhysicalExpr for NestedJsonRenderExpr {
    fn data_type(
        &self,
        _input_schema: &arrow::datatypes::Schema,
    ) -> datafusion::error::Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn nullable(
        &self,
        _input_schema: &arrow::datatypes::Schema,
    ) -> datafusion::error::Result<bool> {
        Ok(true)
    }

    fn evaluate(&self, batch: &RecordBatch) -> datafusion::error::Result<ColumnarValue> {
        let array = self.input.evaluate(batch)?.into_array(batch.num_rows())?;
        let resolved = self.resolution.apply(&array)?;
        Ok(ColumnarValue::Array(Arc::new(
            render_nested_column_as_json(&resolved)?,
        )))
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> datafusion::error::Result<Arc<dyn PhysicalExpr>> {
        match <[Arc<dyn PhysicalExpr>; 1]>::try_from(children) {
            Ok([child]) => Ok(Arc::new(Self::new(child, self.resolution.clone()))),
            Err(children) => Err(DataFusionError::Internal(format!(
                "{NESTED_JSON_RENDER_UDF_NAME} renders exactly one column, so it takes exactly \
                 one child expression, but {} were given",
                children.len()
            ))),
        }
    }

    fn fmt_sql(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{NESTED_JSON_RENDER_UDF_NAME}(")?;
        self.input.fmt_sql(f)?;
        write!(f, ")")
    }
}

/// One file's column binding: the physical schema renamed to the logical names
/// that claimed its fields, the set of logical column names some physical field
/// claimed, and the nested resolution of each claimed nested column.
///
/// The first two views come out of ONE pass because they are one decision seen
/// twice: the delegate adapter resolves a logical column by NAME against
/// `renamed_physical`, so a logical name present there is exactly a column this file
/// supplies, and a logical name absent from it is exactly a column the per-file
/// `initial-default` / NULL fill must cover. The third is that same claim recursed
/// into a nested column's members.
struct ColumnBinding {
    renamed_physical: arrow::datatypes::SchemaRef,
    bound_logical_names: std::collections::HashSet<String>,
    /// Per-file nested resolution keyed by LOGICAL COLUMN NAME, one entry per bound
    /// column whose logical field declares a nested member tree. Empty for a table
    /// with no list, struct, or map column.
    nested: HashMap<String, NestedResolution>,
}

impl ColumnBinding {
    /// Every nested column of this file, keyed by its PHYSICAL index — the columns
    /// [`FieldIdExprAdapter`] renders to JSON and the delegate must therefore never
    /// cast — each carrying the resolution that restructures it to the table's
    /// logical member names.
    ///
    /// The logical field's DECLARED member tree is the necessary signal, the same one
    /// [`crate::scan::raw_scan::renders_nested_json`] reads to withhold Parquet
    /// row-filter pushdown. Keying on it here is what stops the two sites drifting:
    /// no column can be rendered while a pushdown DataFusion approves against the
    /// `Utf8` logical schema — and then drops against the physical nested schema,
    /// returning every row — stays on for its table. A physically nested column
    /// declaring no tree is therefore left to the delegate, which has no
    /// struct-to-text kernel and fails loudly rather than silently losing a predicate.
    ///
    /// A tree the file's own type contradicts resolves VERBATIM to that type, and a
    /// verbatim primitive is left to the delegate too: the JSON encoder would quote
    /// it rather than render a document.
    fn nested_columns(&self) -> HashMap<usize, NestedResolution> {
        self.renamed_physical
            .fields()
            .iter()
            .enumerate()
            .filter_map(|(index, field)| {
                let resolution = self.nested.get(field.name())?;
                needs_nested_json_rendering(resolution.resolved_field().data_type())
                    .then(|| (index, resolution.clone()))
            })
            .collect()
    }

    /// The physical schema the delegate resolves logical columns against: the renamed
    /// schema with each nested column carrying the type its resolution produces.
    ///
    /// The renamed schema is already the file AS THE LOGICAL SCHEMA SEES IT — it
    /// carries the logical name of every column it supplies rather than the file's
    /// own — and for a nested column that view reaches its members too, so the
    /// delegate compares the logical field against the member names and order the
    /// resolved array will carry rather than against the file's.
    fn delegate_physical_schema(
        &self,
        nested: &HashMap<usize, NestedResolution>,
    ) -> arrow::datatypes::SchemaRef {
        if nested.is_empty() {
            return Arc::clone(&self.renamed_physical);
        }
        let fields: Vec<FieldRef> = self
            .renamed_physical
            .fields()
            .iter()
            .enumerate()
            .map(|(index, field)| match nested.get(&index) {
                Some(resolution) => Arc::new(
                    field
                        .as_ref()
                        .clone()
                        .with_data_type(resolution.resolved_field().data_type().clone()),
                ),
                None => Arc::clone(field),
            })
            .collect();
        Arc::new(arrow::datatypes::Schema::new_with_metadata(
            fields,
            self.renamed_physical.metadata().clone(),
        ))
    }
}

/// The logical schema the delegate adapts TO, with each nested column's field taken
/// WHOLE from `delegate_physical` — name, type, nullability, and metadata together.
///
/// That whole-field substitution is what stops the delegate casting the column:
/// `DefaultPhysicalExprAdapter` emits a bare `Column` only when the logical and
/// physical fields are FULLY equal and answers any difference — a diverging data type,
/// a diverging nullability, or diverging metadata alone — with a cast. Substituting
/// only the resolved data type would leave a file whose nested column carries no
/// `PARQUET:field_id` differing in metadata, and arrow-cast has no `Struct → Utf8` or
/// `Map → Utf8` kernel at all while its `List → Utf8` kernel renders Arrow display
/// text rather than JSON.
///
/// A nested column ABSENT from this file has no entry, so its logical field stays the
/// `Utf8` the schema declares and the delegate's own NULL fill covers it exactly as it
/// covers an absent primitive.
fn delegate_logical_schema(
    logical: &arrow::datatypes::SchemaRef,
    delegate_physical: &arrow::datatypes::Schema,
    nested: &HashMap<usize, NestedResolution>,
) -> arrow::datatypes::SchemaRef {
    if nested.is_empty() {
        return Arc::clone(logical);
    }
    let substitute: HashMap<&str, &FieldRef> = nested
        .keys()
        .map(|index| {
            let field = &delegate_physical.fields()[*index];
            (field.name().as_str(), field)
        })
        .collect();
    let fields: Vec<FieldRef> = logical
        .fields()
        .iter()
        .map(|field| match substitute.get(field.name().as_str()) {
            Some(physical) => Arc::clone(physical),
            None => Arc::clone(field),
        })
        .collect();
    Arc::new(arrow::datatypes::Schema::new_with_metadata(
        fields,
        logical.metadata().clone(),
    ))
}

/// Bind one file's physical fields to the logical schema, renaming each physical
/// field to the logical name that claims it and preserving field order, type,
/// nullability, and metadata.
///
/// [`claim_logical`] decides which logical field claims a physical one — by embedded
/// field-id, by declared physical name, by `schema.name-mapping.default`, or by
/// identity, first match wins. An unclaimed physical field keeps its own name and is
/// simply never referenced: that is how a dropped column falls away.
///
/// A claimed field whose logical field declares a nested member tree is ALSO resolved
/// member-by-member by [`resolve_nested_field`], so the file's own member names,
/// order, and omissions are reconciled with the table's by the same binding order,
/// one level down.
///
/// A logical field counts as BOUND when the renamed schema supplies its name, which
/// is precisely the question the delegate will ask, so the fill seam and the delegate
/// can never disagree about whether a column is present in this file.
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

    let declared_physical_by_logical: HashMap<&str, &str> = resolution
        .declared_physical_names
        .iter()
        .map(|(physical_name, logical_name)| (logical_name.as_str(), physical_name.as_str()))
        .collect();
    let keys: Vec<BindingKeys<'_>> = logical
        .fields()
        .iter()
        .map(|field| BindingKeys {
            name: field.name().as_str(),
            field_id: field_id_of(field),
            physical_name: declared_physical_by_logical
                .get(field.name().as_str())
                .copied(),
        })
        .collect();

    let field_id_by_physical_name: HashMap<&str, i32> = resolution
        .name_mapping
        .iter()
        .map(|entry| (entry.name.as_str(), entry.field_id))
        .collect();

    let mut nested: HashMap<String, NestedResolution> = HashMap::new();
    let renamed_fields: Vec<arrow::datatypes::FieldRef> = physical
        .fields()
        .iter()
        .map(|physical_field| {
            let physical_name = physical_field.name().as_str();
            let claimed = claim_logical(
                PhysicalKeys {
                    name: physical_name,
                    embedded_id: field_id_of(physical_field),
                    mapped_field_id: field_id_by_physical_name.get(physical_name).copied(),
                },
                &keys,
            );
            let Some(logical_name) = claimed.map(|index| keys[index].name) else {
                return Arc::clone(physical_field);
            };
            if let Some(members) = resolution.nested_members.get(logical_name) {
                nested.insert(
                    logical_name.to_string(),
                    resolve_nested_field(physical_field, members),
                );
            }
            match logical_name == physical_name {
                true => Arc::clone(physical_field),
                false => Arc::new(physical_field.as_ref().clone().with_name(logical_name)),
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
        nested,
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

/// Index the logical schema's nested member trees as
/// `logical column name → members`, built ONCE per registration and handed to the
/// [`FieldIdExprAdapterFactory`] so [`bind_columns`] can resolve each file's own
/// nested layout onto the logical one.
///
/// A primitive column declares no tree and contributes no entry, so the index is
/// empty for a table with no list, struct, or map column and the nested resolution is
/// then reached for no column at all.
pub(super) fn index_nested_members(
    logical_schema: &[crate::scan::spec::LogicalField],
) -> HashMap<String, NestedMembers> {
    logical_schema
        .iter()
        .filter_map(|lf| {
            lf.nested
                .as_ref()
                .map(|members| (lf.name.clone(), members.clone()))
        })
        .collect()
}

#[cfg(test)]
#[path = "field_id_projection_tests.rs"]
mod tests;
