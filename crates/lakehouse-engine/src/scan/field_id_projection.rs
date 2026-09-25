//! A logical field declares how it binds: by field-id (Iceberg, or Delta `id` column mapping)
//! against `PARQUET:field_id`, by declared physical name (Delta `name` mapping), or by identity
//! (Delta `none` mapping); Iceberg also falls back to `schema.name-mapping.default` and then the
//! physical name. Nested members declare the same choice, and [`resolve_nested_field`] recurses
//! the one binding pass into them.

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

/// Only field-id-bound logical fields are tagged with it.
pub(crate) use parquet::arrow::PARQUET_FIELD_ID_META_KEY;

/// `None` without metadata (older writers) or when the value is not an `i32`.
fn field_id_of(field: &arrow::datatypes::Field) -> Option<i32> {
    field
        .metadata()
        .get(PARQUET_FIELD_ID_META_KEY)
        .and_then(|v| v.parse::<i32>().ok())
}

/// In the order [`claim_logical`] tries them; shared by top-level and nested fields.
struct BindingKeys<'a> {
    name: &'a str,
    field_id: Option<i32>,
    physical_name: Option<&'a str>,
}

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

/// Without it, the rendered JSON would be keyed by the file's member names, which on a
/// column-mapped table are opaque identifiers.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(super) struct NestedResolution {
    field: FieldRef,
    members: ResolvedMembers,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum ResolvedMembers {
    Verbatim,
    Struct(Vec<StructSlot>),
    Element(Box<NestedResolution>),
    /// The entries field and sortedness live in the enclosing [`NestedResolution::field`].
    Entries {
        key: Box<NestedResolution>,
        value: Box<NestedResolution>,
    },
}

/// `source` is `None` when no member of this file's struct binds the slot.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct StructSlot {
    source: Option<usize>,
    resolution: NestedResolution,
}

/// Struct members are renamed to their claiming logical name, reordered, unclaimed ones dropped,
/// and missing ones null-filled, using [`claim_logical`]. The name-mapping fallback is not
/// reachable at depth: nested entries go unparsed (issue #28).
///
/// List elements and map keys/values are positional and resolve by recursion alone. A descriptor
/// contradicting the file's own type resolves VERBATIM, leaving the file schema's adaptation to
/// decide.
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

fn verbatim(physical: &FieldRef) -> NestedResolution {
    NestedResolution {
        field: Arc::clone(physical),
        members: ResolvedMembers::Verbatim,
    }
}

fn resolve_member(physical: &FieldRef, members: Option<&NestedMembers>) -> NestedResolution {
    match members {
        Some(members) => resolve_nested_field(physical, members),
        None => verbatim(physical),
    }
}

/// A member claiming an already-claimed slot is left unclaimed, so a duplicate key cannot
/// overwrite the first match. Unclaimed slots are typed [`DataType::Null`]: the descriptor
/// carries no types, and a null array renders as explicit JSON `null`.
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
    /// Only members are resolved; the column's own name and nullability stay physical.
    pub(super) fn resolved_field(&self) -> &FieldRef {
        &self.field
    }

    /// Fails only if applied to a different column than it was built from.
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

/// Only the element's layout is resolved; offsets, length, and nulls stay as written.
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

/// The Parquet opener calls [`Self::create`] once per file, so divergent layouts each bind.
///
/// It composes around [`DefaultPhysicalExprAdapter`] instead of reimplementing adaptation:
/// 1. The default gets a physical schema renamed to the claiming logical names (see
///    [`bind_columns`]), so every binding strategy shares its NULL-fill/cast/required-missing
///    semantics.
/// 2. Output columns are renamed back to the real physical names (see [`FieldIdExprAdapter`]).
#[derive(Debug)]
pub(crate) struct FieldIdExprAdapterFactory {
    pub(crate) resolution: FieldIdResolution,
}

/// Per-query binding metadata for one scan side, resolved once in the VS.
#[derive(Debug, Clone)]
pub(crate) struct FieldIdResolution {
    /// Physical-name → field-id fallback for files whose columns carry no field-id.
    pub(crate) name_mapping: Vec<NameMappingEntry>,
    /// Physical name → logical name. Empty for every Iceberg table.
    pub(crate) declared_physical_names: HashMap<String, String>,
    /// Keyed by logical name: the one key every field carries, and stable under projection.
    pub(crate) defaults: HashMap<String, ScalarValue>,
    /// Keyed by logical name.
    pub(crate) nested_members: HashMap<String, NestedMembers>,
}

impl PhysicalExprAdapterFactory for FieldIdExprAdapterFactory {
    fn create(
        &self,
        logical_file_schema: arrow::datatypes::SchemaRef,
        physical_file_schema: arrow::datatypes::SchemaRef,
    ) -> datafusion::error::Result<Arc<dyn PhysicalExprAdapter>> {
        // The rename preserves order, so the default resolves each logical column to the
        // correct physical index.
        let binding = bind_columns(
            &logical_file_schema,
            &physical_file_schema,
            &self.resolution,
        );

        // Per file: only logical columns no physical field claimed get their default, keyed by
        // the logical index an incoming `Column` carries.
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

        // The delegate sees identical fields for a nested column, so it emits a bare `Column`
        // that `rewrite` replaces with the JSON-rendering expression.
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

/// The default adapter binds by name, so fed logical names it emits `Column`s with LOGICAL
/// names. In DataFusion 54 the Parquet opener applies the adapter to the projection too, and
/// `build_projection_read_plan`, `reassign_expr_columns`, and `make_projector` resolve by name
/// against the REAL file schema, failing with `Unable to get field named "rating"`. So resolved
/// columns are renamed back to the physical name at their already-correct index.
///
/// The same pass wraps nested columns in a [`NestedJsonRenderExpr`], since no cast carries a
/// `List`/`Struct`/`Map` to `Utf8`.
#[derive(Debug)]
struct FieldIdExprAdapter {
    inner: Arc<dyn PhysicalExprAdapter>,
    physical_file_schema: arrow::datatypes::SchemaRef,
    /// Keyed by LOGICAL column index; only absent fields with a reconstructed `initial-default`.
    absent_default_by_index: HashMap<usize, ScalarValue>,
    /// Keyed by PHYSICAL column index, the index every delegate-emitted `Column` carries.
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

        // Substitute an absent field's `initial-default` (column-projection rule 3) BEFORE
        // delegating, since the default adapter NULL-fills or errors on absent fields.
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

        let rewritten = self.inner.rewrite(intercepted)?;

        // Injected `Literal`s carry no `Column` and pass through.
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
                    // Jump: descending would meet the same column and wrap it endlessly.
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

/// Needed because arrow-cast has no `Struct`/`Map → Utf8` kernel and its `List → Utf8` yields
/// display text. The child stays a bare `Column` with the REAL physical name so the opener's
/// name-based lookups still read it.
#[derive(Debug, Eq)]
struct NestedJsonRenderExpr {
    input: Arc<dyn PhysicalExpr>,
    resolution: NestedResolution,
}

// Not derived: rust-lang/rust#78808 blocks deriving for a struct holding `Arc<dyn Trait>`.
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

/// The renamed schema and bound names come from one pass: the delegate resolves by name against
/// `renamed_physical`, so a name there is exactly a column this file supplies and an absent one
/// is exactly what the default/NULL fill must cover.
struct ColumnBinding {
    renamed_physical: arrow::datatypes::SchemaRef,
    bound_logical_names: std::collections::HashSet<String>,
    /// Keyed by LOGICAL COLUMN NAME.
    nested: HashMap<String, NestedResolution>,
}

impl ColumnBinding {
    /// Keyed by PHYSICAL index; these are rendered to JSON and never cast by the delegate.
    ///
    /// Keyed on the declared member tree, the same signal
    /// [`crate::scan::raw_scan::renders_nested_json`] uses to withhold row-filter pushdown, so a
    /// rendered column never keeps a pushdown that would drop its predicate. A verbatim primitive
    /// is left to the delegate: the JSON encoder would quote it rather than render a document.
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

    /// Nested columns carry their resolved type, so the delegate compares against the member
    /// names and order the resolved array will carry.
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

/// `DefaultPhysicalExprAdapter` emits a bare `Column` only when logical and physical fields are
/// FULLY equal, and casts on any difference, even metadata alone. Substituting the whole field
/// keeps it from attempting a cast arrow-cast cannot do. A nested column absent from this file
/// keeps its `Utf8` field and is NULL-filled like an absent primitive.
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

/// Preserves field order, type, nullability, and metadata; an unclaimed field keeps its name
/// and is never referenced, which is how a dropped column falls away. Claimed nested fields are
/// also resolved by [`resolve_nested_field`].
///
/// Assumes post-rename logical names are unique among referenced fields and no two logical
/// fields declare one physical name (guaranteed by Delta). A dropped column whose physical name
/// was later reused is not disambiguated: name mapping keys CURRENT names.
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

/// Only field-id-bound fields are tagged `PARQUET:field_id`: a synthesized id would invite a
/// false match against a file that does carry ids.
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

/// The scan-side inverse of the VS layer's `encode_initial_default`. The tag fixes the variant
/// via [`arrow_type_from_tag`], matching [`build_logical_arrow_schema`]; the encoded text is the
/// raw primitive (days/micros/nanos, or an `i128` decimal mantissa). Errors, never panics.
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

    // Same DataType as the logical schema field, so timezone/precision/scale line up.
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

/// Keyed by logical name, stable under projection. A reconstruction failure is a clean `Err`.
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

/// `physical name → logical column name`; empty for every Iceberg table.
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

/// `logical column name → members`; empty for a table with no nested column.
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
