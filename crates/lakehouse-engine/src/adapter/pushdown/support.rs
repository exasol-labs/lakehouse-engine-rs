use super::grouped_agg::{is_literal_selectlist_item, partial_emits_items};
use super::scalar_over_agg::{col_type_for, with_stat_grand_means};
use super::single_group_agg::{DistinctCount, SingleGroupItem};
use crate::adapter::column_notes::TableNotes;
use crate::adapter::connection::ConnectionCreds;
use crate::scan::sealed::{SealedStorageKey, seal_storage};
use crate::scan::spec::{
    AggregatePlan, ColumnRef, CommonScanSpec, FileEntry, LogicalField, ProjectionItem, ScanSpec,
    ScanStorage, StorageBackend, render_order_by_clause,
};
use crate::types::exasol_type::{ExasolType, exasol_type_from_json};
use crate::types::mapping::timestamp_literal_digits;
use exasol_udf_sdk::error::UdfError;
use serde_json::Value as Json;
use std::collections::HashMap;
use vs_expression::{
    ColumnNode, ColumnTypeLookup, render_scan_expression, render_scan_filter, walk_column_nodes,
};

use super::binary_filter::{BinaryLayout, compare_binary_bytes};

pub(super) const SCAN_UDF_NAME: &str = "LAKEHOUSE_SCAN";

/// Passthrough distributor that groups per-shard file-list rows by `shard_key` so
/// Exasol spreads the work units across nodes.
pub(super) const DISTRIBUTE_FILES_UDF_NAME: &str = "LAKEHOUSE_DISTRIBUTE_FILES";

/// Clamped to at least 1 and at most `min(file_count, 300)`, so no shard is empty.
pub use crate::exasol::group_count as shard_count;
pub(super) use crate::exasol::{quote_ident, sql_string_literal};

pub(super) fn shard_files_json<E: Clone + Into<FileEntry>>(files: &[E]) -> String {
    let entries: Vec<FileEntry> = files.iter().cloned().map(Into::into).collect();
    ScanSpec::files_json(&entries)
}

/// `request_limit` is the raw request limit, rendered as a trailing `LIMIT n` on the
/// one-row merge (a no-op for `n >= 1`, correct for `n = 0`).
pub struct AggregateMergeInputs {
    plan_types: Vec<ExasolType>,
    merge_select: Vec<String>,
    having: Option<String>,
    request_limit: Option<u64>,
}

impl AggregateMergeInputs {
    /// `None` for an empty `merge_select`: a narrower select list would fail Exasol's
    /// positional validation, so the caller must use its fallback.
    pub fn new(
        plan_types: Vec<ExasolType>,
        merge_select: Vec<String>,
        request_limit: Option<u64>,
    ) -> Option<Self> {
        if merge_select.is_empty() {
            return None;
        }
        Some(Self {
            plan_types,
            merge_select,
            having: None,
            request_limit,
        })
    }

    pub fn with_having(self, having: Option<String>) -> Self {
        Self { having, ..self }
    }
}

/// No `SELECT * FROM (...)` materialization wrapper (decision [5]). The spec's
/// `aggregates` and `merge_inputs` must be present or absent together. Grouped
/// aggregates use `build_grouped_aggregate_scan_sql` instead.
#[allow(clippy::too_many_arguments)]
pub fn build_scan_driving_sql<E: Clone + Into<FileEntry>>(
    spec_template: &ScanSpec,
    shards: &[Vec<E>],
    projection: &[ProjectedColumn],
    limit: Option<u64>,
    col_types: &ColumnTypes,
    merge_inputs: Option<&AggregateMergeInputs>,
    udf_name: &str,
    distribute_udf_name: &str,
) -> String {
    debug_assert_eq!(
        spec_template.common.aggregates.is_some(),
        merge_inputs.is_some(),
        "an aggregate spec must arrive with its merge inputs and a row-scan spec with none"
    );
    if let (Some(aggregates), Some(inputs)) =
        (spec_template.common.aggregates.as_deref(), merge_inputs)
    {
        build_aggregate_scan_sql(
            spec_template,
            shards,
            aggregates,
            col_types,
            inputs,
            udf_name,
            distribute_udf_name,
        )
    } else {
        build_row_scan_sql(
            spec_template,
            shards,
            projection,
            limit,
            udf_name,
            distribute_udf_name,
        )
    }
}

/// One projected item with the Exasol type its `EMITS` column declares, so the projection and its
/// types cannot fall out of step.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectedColumn {
    pub item: ProjectionItem,
    pub exa_type: ExasolType,
}

/// The items a scan spec projects, in projection order.
pub(super) fn projection_items(projection: &[ProjectedColumn]) -> Vec<ProjectionItem> {
    projection
        .iter()
        .map(|column| column.item.clone())
        .collect()
}

/// An `Expr` gets a positional synthetic name matching the scan side's aliasing,
/// never its rendered SQL, so repeated literals cannot collide.
pub(super) fn emits_ident(item: &ProjectionItem, index: usize) -> String {
    match item {
        ProjectionItem::Column(column) => quote_ident(&column.output_name()),
        ProjectionItem::Expr { .. } => quote_ident(&format!("_LH_PROJ_{index}")),
    }
}

/// A matched top-N carries the same `order_by` in the per-shard blob (a DataFusion
/// TopK) and the outer merge; both render through [`render_order_by_clause`] so they
/// agree on direction and NULL placement.
fn build_row_scan_sql<E: Clone + Into<FileEntry>>(
    spec_template: &ScanSpec,
    shards: &[Vec<E>],
    projection: &[ProjectedColumn],
    limit: Option<u64>,
    udf_name: &str,
    distribute_udf_name: &str,
) -> String {
    let emits = projection
        .iter()
        .enumerate()
        .map(|(i, column)| format!("{} {}", emits_ident(&column.item, i), column.exa_type))
        .collect::<Vec<_>>()
        .join(", ");

    let mut sql = build_fan_out_inner(spec_template, shards, &emits, udf_name, distribute_udf_name);

    // SQL requires ORDER BY before LIMIT.
    if !spec_template.common.order_by.is_empty() {
        sql.push_str(&format!(
            " ORDER BY {}",
            render_order_by_clause(&spec_template.common.order_by)
        ));
    }
    if let Some(n) = limit {
        sql.push_str(&format!(" LIMIT {n}"));
    }
    sql
}

/// `request_limit` must render even over the one-row merge: a pushed `LIMIT 0` must
/// return zero rows (#198).
fn build_aggregate_scan_sql<E: Clone + Into<FileEntry>>(
    spec_template: &ScanSpec,
    shards: &[Vec<E>],
    aggregates: &[AggregatePlan],
    col_types: &ColumnTypes,
    inputs: &AggregateMergeInputs,
    udf_name: &str,
    distribute_udf_name: &str,
) -> String {
    let emits_items = partial_emits_items(aggregates, col_types, &inputs.plan_types);
    let emits = emits_items.join(", ");
    let merge_select = inputs.merge_select.join(", ");

    let fan_out = build_fan_out_inner(spec_template, shards, &emits, udf_name, distribute_udf_name);
    let from = with_stat_grand_means(&fan_out, aggregates, &[]);

    let mut sql = format!("SELECT {merge_select} FROM ({from})");
    if let Some(having) = &inputs.having {
        sql.push_str(&format!(" HAVING {having}"));
    }
    if let Some(n) = inputs.request_limit {
        sql.push_str(&format!(" LIMIT {n}"));
    }
    sql
}

/// `SELECT COUNT(DISTINCT "V") FROM (<fan-out>)`. LIMIT goes only on the outer
/// wrapper: inside the fan-out it would truncate a shard's distinct set.
#[allow(clippy::too_many_arguments)]
pub(super) fn build_count_distinct_scan_sql<E: Clone + Into<FileEntry>>(
    base_spec: &ScanSpec,
    shards: &[Vec<E>],
    items: &[SingleGroupItem],
    col_types: &ColumnTypes,
    limit: Option<u64>,
    udf_name: &str,
    distribute_udf_name: &str,
) -> String {
    let [SingleGroupItem::Distinct(dc)] = items else {
        unreachable!(
            "build_count_distinct_scan_sql is dispatched only for a lone COUNT(DISTINCT) (Case 1)"
        )
    };
    let fan_out = build_distinct_fan_out(
        base_spec,
        shards,
        dc,
        col_types,
        udf_name,
        distribute_udf_name,
    );
    let mut sql = format!(r#"SELECT COUNT(DISTINCT "V") FROM ({fan_out})"#);
    if let Some(n) = limit {
        sql.push_str(&format!(" LIMIT {n}"));
    }
    sql
}

/// `"V"` carries the column's raw values, declared with its exact Exasol type, so
/// the outer `COUNT(DISTINCT "V")` dedups across shards with no cast.
fn build_distinct_fan_out<E: Clone + Into<FileEntry>>(
    base_spec: &ScanSpec,
    shards: &[Vec<E>],
    dc: &DistinctCount,
    col_types: &ColumnTypes,
    udf_name: &str,
    distribute_udf_name: &str,
) -> String {
    let Some(col) = dc.column.as_deref() else {
        unreachable!(
            "build_distinct_fan_out is dispatched only for a lone bare-column \
             COUNT(DISTINCT) (Case 1); an expression argument routes to the qualified \
             single-table wrapper instead"
        )
    };
    let value_type = col_type_for(Some(col), None, col_types, None);
    let proj_item = ProjectionItem::from(col);
    let arg_sql = quote_ident(col);
    let null_pred = format!("({arg_sql} IS NOT NULL)");
    let filter = match base_spec.common.filter.as_deref() {
        Some(f) if !f.is_empty() => Some(format!("({f}) AND {null_pred}")),
        _ => Some(null_pred),
    };
    let spec = ScanSpec {
        common: CommonScanSpec {
            projection: vec![proj_item],
            filter,
            limit: None,
            order_by: Vec::new(),
            aggregates: None,
            group_keys: None,
            distinct: true,
            ..base_spec.common.clone()
        },
        files: base_spec.files.clone(),
    };
    let emits = format!(r#""V" {value_type}"#);
    build_fan_out_inner(&spec, shards, &emits, udf_name, distribute_udf_name)
}

/// A nested distributor does the `GROUP BY shard_key` fan-out, wrapped by an outer
/// ungrouped scalar scan: with no top-level `GROUP BY`, Exasol streams the scan
/// output instead of materializing it into a temp table. The common blob is
/// serialized once; only per-shard file lists flow through the distributor.
pub fn build_fan_out_inner<E: Clone + Into<FileEntry>>(
    spec_template: &ScanSpec,
    shards: &[Vec<E>],
    emits: &str,
    udf_name: &str,
    distribute_udf_name: &str,
) -> String {
    let common_literal = sql_string_literal(&spec_template.to_common_json());

    // A scalar EMIT UDF over constant literals fires exactly once.
    if shards.len() == 1 {
        let files_literal = sql_string_literal(&shard_files_json(&shards[0]));
        return format!(
            "SELECT {udf}({common}, {files}) EMITS ({emits})",
            udf = udf_name,
            common = common_literal,
            files = files_literal,
            emits = emits,
        );
    }

    let values: Vec<String> = shards
        .iter()
        .enumerate()
        .map(|(i, files)| {
            let files_literal = sql_string_literal(&shard_files_json(files));
            format!("({i},{files_literal})")
        })
        .collect();
    let values_list = values.join(",");
    // The distributor has a static `EMITS`, so Exasol rejects a query-side `EMITS` on it.
    format!(
        "SELECT {udf}({common}, files) EMITS ({emits}) FROM (SELECT {distribute}(files) FROM (VALUES {values}) AS shards(shard_key, files) GROUP BY shard_key)",
        udf = udf_name,
        common = common_literal,
        emits = emits,
        distribute = distribute_udf_name,
        values = values_list,
    )
}

/// One involved table's columns as the pushdown request declares them.
#[derive(Default)]
pub(super) struct InvolvedColumns<'r> {
    /// `(uppercased name, Exasol type)` in declaration order.
    pub(super) types: Vec<(String, ExasolType)>,
    /// The table's raw column notes, in the same order.
    pub(super) notes: TableNotes<'r>,
}

/// Reads each column's name, Exasol type, and note in one pass. A column missing `name` is
/// skipped. A missing or invalid `dataType` fails the request: the adapter declared every column
/// itself, so no defaulted type may stand in for it.
pub(super) fn involved_columns<'r>(
    request: &'r Json,
    select_table: impl FnOnce(&'r [Json]) -> Option<&'r Json>,
) -> Result<InvolvedColumns<'r>, UdfError> {
    let table = request
        .get("involvedTables")
        .and_then(|v| v.as_array())
        .and_then(|tables| select_table(tables));
    let (types, columns) = table
        .and_then(|t| t.get("columns"))
        .and_then(|c| c.as_array())
        .into_iter()
        .flatten()
        .filter_map(|column| Some((column.get("name")?.as_str()?, column)))
        .map(|(name, column)| {
            let data_type = column
                .get("dataType")
                .ok_or_else(|| {
                    UdfError::User(format!("involved column {name} declares no dataType"))
                })
                .and_then(exasol_type_from_json)?;
            let note = column.get("adapterNotes").and_then(|note| note.as_str());
            Ok(((name.to_uppercase(), data_type), (name, note)))
        })
        .collect::<Result<Vec<_>, UdfError>>()?
        .into_iter()
        .unzip();
    Ok(InvolvedColumns {
        types,
        notes: TableNotes {
            table: table
                .and_then(|t| t.get("name"))
                .and_then(|n| n.as_str())
                .unwrap_or_default(),
            columns,
        },
    })
}

/// Curated deliberately; see [`rewrite_expr_tree`].
const EXPR_ARRAY_FIELDS: [&str; 3] = ["expressions", "arguments", "results"];

/// Curated deliberately; see [`rewrite_expr_tree`].
const EXPR_SINGLE_FIELDS: [&str; 5] = ["expression", "pattern", "left", "right", "basis"];

/// Post-order is load-bearing: Exasol encodes `a||b||c` as `CONCAT(a, CONCAT(b, c))`,
/// so only a check that sees rewritten children reaches nested occurrences, and an
/// already-coerced inner argument is not re-wrapped.
///
/// `f` returning `None` declines the whole tree; the caller must self-apply it.
///
/// The child fields are curated rather than walking every map value, so a
/// `dataType` sub-object or a `name` identifier is never handed to `f`.
fn rewrite_expr_tree(node: &Json, f: &impl Fn(&Json) -> Option<Json>) -> Option<Json> {
    let mut out = node.clone();
    for field in EXPR_ARRAY_FIELDS {
        if let Some(Json::Array(children)) = node.get(field) {
            let rewritten: Option<Vec<Json>> =
                children.iter().map(|c| rewrite_expr_tree(c, f)).collect();
            out[field] = Json::Array(rewritten?);
        }
    }
    for field in EXPR_SINGLE_FIELDS {
        if let Some(child) = node.get(field)
            && child.is_object()
        {
            out[field] = rewrite_expr_tree(child, f)?;
        }
    }
    f(&out)
}

/// The (relation alias, column) pair a `column` node names.
pub(super) fn column_node_ref(node: &Json) -> Option<ColumnRef> {
    let column = ColumnNode::parse(node)?;
    Some(ColumnRef {
        relation: column.table_alias.map(str::to_string),
        name: column.upper_name(),
    })
}

/// An entry with a relation alias binds only its own relation's column, so a column name two
/// join legs share never binds the other leg's column. One without binds by name alone.
pub(super) fn binds_column(entry: &ColumnRef, column: &ColumnRef) -> bool {
    entry.name == column.name && (entry.relation.is_none() || entry.relation == column.relation)
}

/// One column's Exasol type and, for a binary column, the raw-byte layout the scan reads. A binary
/// column declares the same Exasol type as a text column, so only the layout tells them apart.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct ColumnType {
    pub(super) exa_type: ExasolType,
    /// `None` for a column the scan reads as text.
    pub(super) binary: Option<BinaryLayout>,
}

/// The per-column type map a request's `column` nodes resolve against, one entry per (relation
/// alias, column) pair, matched through [`binds_column`].
///
/// A broadcast join qualifies every entry with its leg's alias. A single-table scan and an
/// N-scan leg each read one relation, so their entries stay unqualified and keep the bare-name
/// lookup.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ColumnTypes(Vec<(ColumnRef, ColumnType)>);

impl ColumnTypes {
    /// `involved`'s columns in declaration order, binary where their `declared` field is.
    pub(super) fn of(
        involved: &InvolvedColumns<'_>,
        declared: &[LogicalField],
        binary_as_string: bool,
    ) -> Self {
        let mut declared_by_name: HashMap<String, &LogicalField> = HashMap::new();
        for field in declared {
            declared_by_name
                .entry(field.name.to_uppercase())
                .or_insert(field);
        }
        Self(
            involved
                .types
                .iter()
                .map(|(name, exa_type)| {
                    let binary = declared_by_name
                        .get(name)
                        .copied()
                        .and_then(|field| BinaryLayout::of(field, binary_as_string));
                    let column_type = ColumnType {
                        exa_type: *exa_type,
                        binary,
                    };
                    (ColumnRef::from(name.as_str()), column_type)
                })
                .collect(),
        )
    }

    /// `relations` in leg order, so the full row lists each leg's columns in turn. Each leg's map
    /// is qualified with its alias, so a column name two legs share binds only its own leg.
    pub(super) fn per_relation<'a>(
        relations: impl IntoIterator<Item = (String, &'a ColumnTypes)>,
    ) -> Self {
        Self(
            relations
                .into_iter()
                .flat_map(|(relation, columns)| {
                    columns.0.iter().map(move |(column, column_type)| {
                        let qualified = ColumnRef::qualified(relation.clone(), column.name.clone());
                        (qualified, *column_type)
                    })
                })
                .collect(),
        )
    }

    /// Every map's entries in turn, unqualified, as one relation lists them.
    pub(super) fn concat<'a>(maps: impl IntoIterator<Item = &'a ColumnTypes>) -> Self {
        Self(
            maps.into_iter()
                .flat_map(|map| map.0.iter().cloned())
                .collect(),
        )
    }

    /// The entries whose column `keep` accepts, in map order.
    pub(super) fn narrowed(&self, keep: impl Fn(&ColumnRef) -> bool) -> Self {
        Self(
            self.0
                .iter()
                .filter(|(column, _)| keep(column))
                .cloned()
                .collect(),
        )
    }

    pub(super) fn iter(&self) -> impl Iterator<Item = &(ColumnRef, ColumnType)> {
        self.0.iter()
    }

    /// The first entry alone, empty when the map is.
    pub(super) fn first_only(&self) -> Self {
        Self(self.0.iter().take(1).cloned().collect())
    }

    pub(super) fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Every entry, each projected as a column under its own type.
    pub(super) fn projection(&self) -> Vec<ProjectedColumn> {
        self.0
            .iter()
            .map(|(column, column_type)| ProjectedColumn {
                item: ProjectionItem::Column(column.clone()),
                exa_type: column_type.exa_type,
            })
            .collect()
    }

    /// The type of the column `column` binds.
    pub(super) fn get(&self, column: &ColumnRef) -> Option<&ColumnType> {
        self.entry(column).map(|(_, column_type)| column_type)
    }

    pub(super) fn resolve(&self, node: &Json) -> Option<&(ColumnRef, ColumnType)> {
        self.entry(&column_node_ref(node)?)
    }

    fn entry(&self, column: &ColumnRef) -> Option<&(ColumnRef, ColumnType)> {
        self.0.iter().find(|(entry, _)| binds_column(entry, column))
    }
}

/// Deliberately does not test the node's `type`: callers treat a non-column node as a
/// pass-through but an unresolved type as a decline.
fn column_exa_type(node: &Json, col_types: &ColumnTypes) -> Option<ExasolType> {
    col_types
        .resolve(node)
        .map(|(_, column_type)| column_type.exa_type)
}

/// DataFusion truncates a timestamp literal to its column's unit, while Exasol compares at full
/// precision (#461). Workaround for apache/datafusion#22142, fixed by apache/datafusion#22906.
fn rewrite_overprecise_timestamp_comparisons(node: &Json, col_types: &ColumnTypes) -> Option<Json> {
    rewrite_expr_tree(node, &|out: &Json| {
        Some(rewrite_timestamp_comparison(out, col_types).unwrap_or_else(|| out.clone()))
    })
}

/// `None` leaves the node unchanged.
fn rewrite_timestamp_comparison(node: &Json, col_types: &ColumnTypes) -> Option<Json> {
    let compare = |op: &str, column: &Json, literal: &Json| {
        let node = serde_json::json!({"type": op, "left": column, "right": literal});
        rewrite_timestamp_comparison(&node, col_types).unwrap_or(node)
    };
    match node.get("type")?.as_str()? {
        "predicate_between" => {
            let column = node.get("expression")?;
            let (low, high) = (node.get("left")?, node.get("right")?);
            timestamp_literal_floor(column, low, col_types)
                .or_else(|| timestamp_literal_floor(column, high, col_types))?;
            Some(serde_json::json!({"type": "predicate_and", "expressions": [
                compare("predicate_greaterequal", column, low),
                compare("predicate_lessequal", column, high),
            ]}))
        }
        "predicate_in_constlist" => {
            let column = node.get("expression")?;
            let values = node.get("arguments")?.as_array()?;
            values
                .iter()
                .find_map(|value| timestamp_literal_floor(column, value, col_types))?;
            let expressions: Vec<Json> = values
                .iter()
                .map(|value| compare("predicate_equal", column, value))
                .collect();
            Some(serde_json::json!({"type": "predicate_or", "expressions": expressions}))
        }
        op => {
            let (left, right) = (node.get("left")?, node.get("right")?);
            let (column, floor, mirrored) = match timestamp_literal_floor(left, right, col_types) {
                Some(floor) => (left, floor, false),
                None => (
                    right,
                    timestamp_literal_floor(right, left, col_types)?,
                    true,
                ),
            };
            let op = match (op, mirrored) {
                ("predicate_less", true) => "predicate_greater",
                ("predicate_lessequal", true) => "predicate_greaterequal",
                ("predicate_greater", true) => "predicate_less",
                ("predicate_greaterequal", true) => "predicate_lessequal",
                (op, _) => op,
            };
            // `col <> col` and `col = col` keep a NULL column NULL, so an enclosing NOT stays exact.
            let (op, right) = match op {
                "predicate_equal" => ("predicate_notequal", column.clone()),
                "predicate_notequal" => ("predicate_equal", column.clone()),
                "predicate_less" | "predicate_lessequal" => ("predicate_lessequal", floor),
                "predicate_greater" | "predicate_greaterequal" => ("predicate_greater", floor),
                _ => return None,
            };
            Some(serde_json::json!({"type": op, "left": column, "right": right}))
        }
    }
}

/// `None` when the literal fits the column's declared precision.
fn timestamp_literal_floor(column: &Json, literal: &Json, col_types: &ColumnTypes) -> Option<Json> {
    let is = |node: &Json, kind: &str| node.get("type").and_then(|t| t.as_str()) == Some(kind);
    if !is(column, "column") || !is(literal, "literal_timestamp") {
        return None;
    }
    let digits = usize::from(column_exa_type(column, col_types)?.fractional_digits()?);
    let value = literal.get("value")?.as_str()?;
    if timestamp_literal_digits(value) <= digits {
        return None;
    }
    let (instant, fraction) = value.split_once('.')?;
    let floor = match digits {
        0 => instant.to_string(),
        d => format!("{instant}.{}", &fraction[..d]),
    };
    Some(serde_json::json!({"type": "literal_timestamp", "value": floor}))
}

/// Depth-insensitive: a nested aggregate is renderable SQL, so only this probe keeps
/// it off the per-shard scan.
fn contains_aggregate_node(node: &Json) -> bool {
    match node {
        Json::Object(map) => {
            map.get("type").and_then(|t| t.as_str()) == Some("function_aggregate")
                || map.values().any(contains_aggregate_node)
        }
        Json::Array(items) => items.iter().any(contains_aggregate_node),
        _ => false,
    }
}

impl ColumnTypeLookup for ColumnTypes {
    fn column_type(&self, column: &Json) -> Option<ExasolType> {
        column_exa_type(column, self)
    }
}

/// The adapter's own rewrites of a tree the scan computes: the timestamp-literal floor (#461) and
/// the binary byte comparison. They need the column types, so they run here, once.
fn scan_rewrite(expr: &Json, col_types: &ColumnTypes) -> Option<Json> {
    let tree = rewrite_overprecise_timestamp_comparisons(expr, col_types)?;
    Some(compare_binary_bytes(tree, col_types))
}

/// The sole owner of "SQL the DataFusion scan computes": every path renders through it, so every
/// path runs the same typed checks (#587). `None` declines, and the caller must apply the
/// expression in Exasol or decline the shape.
pub(super) fn scan_expression(expr: &Json, col_types: &ColumnTypes) -> Option<String> {
    render_scan_expression(&scan_rewrite(expr, col_types)?, col_types).ok()
}

/// [`scan_expression`] for a filter: `Ok(None)` when it is trivially true, `Err(())` when the
/// scan declines it.
pub(super) fn scan_filter(filter: &Json, col_types: &ColumnTypes) -> Result<Option<String>, ()> {
    let tree = scan_rewrite(filter, col_types).ok_or(())?;
    render_scan_filter(&tree, col_types).map_err(|_| ())
}

/// The rewritten tree when the scan accepts it, for a caller that keeps the tree and renders it
/// later through [`scan_filter`] with the same column types.
pub(super) fn type_accepted_rewrite(expr: &Json, col_types: &ColumnTypes) -> Option<Json> {
    let tree = scan_rewrite(expr, col_types)?;
    render_scan_expression(&tree, col_types).ok()?;
    Some(tree)
}

/// Returns `(scan_filter, declined)`, mutually exclusive (`_decision/045`).
/// `declined` is the original, un-rewritten tree, which Exasol applies itself.
pub(super) fn classify_where_filter<'a>(
    filter_json_raw: Option<&'a Json>,
    col_types: &ColumnTypes,
) -> (Option<String>, Option<&'a Json>) {
    match filter_json_raw {
        None => (None, None),
        Some(raw) => match scan_filter(raw, col_types) {
            Ok(sql) => (sql, None),
            Err(()) => (None, Some(raw)),
        },
    }
}

/// `all_cols` is the column universe: the first involved table, or both legs of a broadcast
/// join, each column qualified by its leg's alias.
///
/// `true` means the projection widened to the full base row (#196); returned because the arities
/// can coincide. A missing or empty select list is not a widening.
pub(super) fn project_columns(
    pushdown: &PushdownRequest,
    all_cols: &ColumnTypes,
) -> Result<(Vec<ProjectedColumn>, bool), UdfError> {
    let Some((first_col, first)) = all_cols.0.first() else {
        return Err(UdfError::User(
            "pushdown request has no column metadata".into(),
        ));
    };
    let full_row = || all_cols.projection();

    // Repeating `first_col` for an unprojectable item would duplicate EMITS
    // names, so the whole projection widens to the full row instead.
    let mut needs_full_fallback = false;
    let select_list = pushdown.json().get("selectList");
    let projection = match select_list {
        None | Some(Json::Null) => full_row(),
        Some(Json::Array(list)) if list.is_empty() => full_row().into_iter().take(1).collect(),
        Some(Json::Array(list)) => {
            let mut projected = Vec::with_capacity(list.len());
            for (i, e) in list.iter().enumerate() {
                // An aggregate at any depth must never be evaluated per shard: its value would be
                // an unmerged partial (#194).
                if contains_aggregate_node(e) {
                    needs_full_fallback = true;
                    continue;
                }
                let item_type = e.get("type").and_then(|t| t.as_str()).unwrap_or("");
                match item_type {
                    "column" => {
                        let (column, exa_type) = match (all_cols.resolve(e), column_node_ref(e)) {
                            (Some((column, column_type)), _) => {
                                (column.clone(), column_type.exa_type)
                            }
                            (None, Some(named)) => {
                                (ColumnRef::from(named.name), ExasolType::WIDEST_VARCHAR)
                            }
                            (None, None) => (first_col.clone(), first.exa_type),
                        };
                        projected.push(ProjectedColumn {
                            item: ProjectionItem::Column(column),
                            exa_type,
                        });
                    }
                    // A literal is pushed positionally so the emitted arity equals the
                    // select-list arity (#190).
                    t if is_literal_selectlist_item(t) || is_pushable_expression_node(t) => {
                        // The EMITS column type must equal Exasol's declared type.
                        match (scan_expression(e, all_cols), pushdown.emittable_select_type(i)) {
                            (Some(sql_frag), Some(exa_type)) => {
                                projected.push(ProjectedColumn {
                                    item: ProjectionItem::Expr { expr: sql_frag },
                                    exa_type,
                                });
                            }
                            _ => {
                                needs_full_fallback = true;
                            }
                        }
                    }
                    _ => {
                        needs_full_fallback = true;
                    }
                }
            }
            if needs_full_fallback {
                full_row()
            } else {
                projected
            }
        }
        _ => full_row(),
    };

    Ok((projection, needs_full_fallback))
}

pub(super) fn collect_all_column_names(
    value: &Json,
    names: &mut std::collections::HashSet<String>,
) {
    walk_column_nodes(value, &mut |column| {
        names.insert(column.upper_name());
    });
}

pub(super) fn extract_limit(pushdown_req: &Json) -> Option<u64> {
    pushdown_req
        .get("limit")
        .and_then(|l| l.get("numElements"))
        .and_then(|n| n.as_u64())
}

/// Exasol normalises `OFFSET 0` away, so an absent key and zero are the same request.
pub(super) fn extract_offset(pushdown_req: &Json) -> u64 {
    pushdown_req
        .get("limit")
        .and_then(|l| l.get("offset"))
        .and_then(|n| n.as_u64())
        .unwrap_or(0)
}

/// Callers must render their own `ORDER BY` first: Exasol rejects `OFFSET` without
/// one. A limit-less offset cannot occur, since Exasol ties `OFFSET` to `LIMIT`.
pub(super) fn render_limit_offset(limit: Option<u64>, offset: u64) -> String {
    match (limit, offset) {
        (None, _) => String::new(),
        (Some(n), 0) => format!(" LIMIT {n}"),
        (Some(n), m) => format!(" LIMIT {n} OFFSET {m}"),
    }
}

/// Exasol withholds `limit` when it cannot delegate the ordering, so this flag
/// triggers the anti-wrong-truncation guard (decision [4]).
pub(super) fn order_by_present(pushdown_req: &Json) -> bool {
    pushdown_req
        .get("orderBy")
        .and_then(|v| v.as_array())
        .is_some_and(|a| !a.is_empty())
}

/// A `pushdownRequest` paired with the type Exasol declares for each of its select-list items.
/// [`PushdownRequest::parse`] is the only constructor, so every holder has passed the strict type
/// and count check, and no consumer re-reads `selectListDataTypes` to turn an error into no type.
#[derive(Debug, Clone)]
pub struct PushdownRequest {
    json: Json,
    select_types: Vec<ExasolType>,
}

impl PushdownRequest {
    /// Fails when the list is absent, holds an invalid entry, or holds another number of entries
    /// than a `selectList` with items. An absent or JSON-null `selectList` is Exasol's `SELECT *`,
    /// whose list holds one entry per table column, so it skips the count check.
    pub fn parse(json: Json) -> Result<Self, UdfError> {
        let entries = json
            .get("selectListDataTypes")
            .and_then(Json::as_array)
            .ok_or_else(|| {
                UdfError::User(
                    "the pushdown request carries no selectListDataTypes array; the adapter \
                     needs Exasol 8.29 or later"
                        .into(),
                )
            })?;
        let select_types = entries
            .iter()
            .map(exasol_type_from_json)
            .collect::<Result<Vec<_>, _>>()?;
        if let Some(items) = json.get("selectList").and_then(Json::as_array)
            && !items.is_empty()
            && items.len() != select_types.len()
        {
            return Err(UdfError::User(format!(
                "the pushdown request declares {} selectListDataTypes entries for {} selectList items",
                select_types.len(),
                items.len()
            )));
        }
        Ok(Self { json, select_types })
    }

    /// The `pushdownRequest` object as Exasol sent it, or as a rewrite kept it.
    pub fn json(&self) -> &Json {
        &self.json
    }

    /// One type per select-list item, or per table column for `SELECT *`.
    pub fn select_types(&self) -> &[ExasolType] {
        &self.select_types
    }

    /// `None` only for an index past the list, which the count check rules out for every
    /// select-list item; the consumer then declines rather than guess a type.
    pub fn select_type(&self, select_index: usize) -> Option<ExasolType> {
        self.select_types.get(select_index).copied()
    }

    /// [`Self::select_type`] when a scan UDF can emit it. `None` means the scan has no value of
    /// the item's type, so Exasol must compute the item over the scan output.
    pub fn emittable_select_type(&self, select_index: usize) -> Option<ExasolType> {
        self.select_type(select_index)
            .filter(ExasolType::is_udf_emittable)
    }

    /// A rewrite of this request's JSON that keeps its select list, such as the table-qualified
    /// copy a broadcast join projects from, beside the types already checked for it.
    pub fn with_json(&self, json: Json) -> Self {
        Self {
            json,
            select_types: self.select_types.clone(),
        }
    }
}

/// Every non-literal node type the scan renders, except `predicate_greater[equal]`:
/// Exasol normalises `a > b` to `b < a`.
fn is_pushable_expression_node(node_type: &str) -> bool {
    matches!(
        node_type,
        "function_scalar"
            | "function_scalar_cast"
            | "function_scalar_extract"
            | "function_scalar_case"
            | "predicate_equal"
            | "predicate_less"
            | "predicate_lessequal"
            | "predicate_like"
            | "predicate_and"
            | "predicate_or"
            | "predicate_not"
            | "predicate_in_constlist"
            | "predicate_between"
            | "predicate_is_null"
            | "predicate_is_not_null"
            | "predicate_notequal"
    )
}

/// Exasol type-checks each pushdown output column against its declared type, so a value
/// computed in Exasol SQL whose type can differ is cast to the declared type.
pub(super) fn cast_to_declared_type(expr: &str, declared: ExasolType) -> String {
    format!("CAST({expr} AS {declared})")
}

/// The scan-spec storage block: a CONNECTION reference only when the scan UDF can
/// re-derive `effective` from the CONNECTION it reads, i.e. when the set neither
/// vends nor names a role, and otherwise `effective` sealed under the CONNECTION's
/// key, because a vended or assumed-role credential is not what the CONNECTION states.
pub(super) fn scan_storage_for(
    creds: &ConnectionCreds,
    connection_name: &str,
    allow_http: bool,
    effective: &StorageBackend,
    sealing_key: Option<&SealedStorageKey>,
) -> Result<ScanStorage, UdfError> {
    if !creds.use_vended_credentials && creds.assume_role_arn().is_none() {
        return Ok(ScanStorage::Connection {
            name: connection_name.to_string(),
            allow_http,
        });
    }
    let Some(key) = sealing_key else {
        return Err(UdfError::User(format!(
            "CONNECTION '{connection_name}' enables use_vended_credentials, but its \
             password carries no secret material to derive a sealing key from, so the \
             vended storage credential can be neither referenced by name nor sealed, and \
             this engine will not place it in the generated SQL in plaintext. The \
             criterion is connection_password_carries_key_material: at least one of \
             token, client_secret, secret_key, session_token, account_key, or sas_token \
             must be non-empty (an access_key id alone is an identifier, not a secret). \
             Remedy either by configuring catalog authentication or supplying that \
             CONNECTION's own storage secret, or by setting use_vended_credentials to \
             false so the credential travels as a CONNECTION reference instead; to \
             disable it, edit the CONNECTION's password"
        )));
    };
    Ok(ScanStorage::Sealed {
        name: connection_name.to_string(),
        payload: seal_storage(effective, key)?,
    })
}

#[cfg(test)]
#[path = "support_tests.rs"]
mod tests;
