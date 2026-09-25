use super::grouped_agg::{col_type_for, is_literal_selectlist_item, partial_emits_items};
use super::single_group_agg::{DistinctCount, SingleGroupItem};
use crate::adapter::connection::ConnectionCreds;
use crate::scan::sealed::{SealedStorageKey, seal_storage};
use crate::scan::spec::{
    AggregatePlan, CommonScanSpec, FileEntry, ProjectionItem, ScanSpec, ScanStorage,
    StorageBackend, render_order_by_clause,
};
use crate::types::mapping::{ExaTypeClass, classify_exa_type, exasol_type_from_json};
use exasol_udf_sdk::error::UdfError;
use serde_json::Value as Json;
use vs_expression::{render_df_filter_safe, render_expression_safe};

pub(super) const SCAN_UDF_NAME: &str = "LAKEHOUSE_SCAN";

/// Passthrough distributor that groups per-shard file-list rows by `shard_key` so
/// Exasol spreads the work units across nodes.
pub(super) const DISTRIBUTE_FILES_UDF_NAME: &str = "LAKEHOUSE_DISTRIBUTE_FILES";

/// Exasol distributes groups round-robin up to this count; above it they are
/// hash-partitioned (unbalanced).
const MAX_SHARD_COUNT: usize = 300;

/// Clamped to at least 1 and at most `min(file_count, 300)`, so no shard is empty.
pub fn shard_count(node_count: usize, parallelism_factor: usize, file_count: usize) -> usize {
    let raw = node_count.saturating_mul(parallelism_factor);
    let upper = file_count.clamp(1, MAX_SHARD_COUNT);
    raw.clamp(1, upper)
}

pub(super) fn shard_files_json<E: Clone + Into<FileEntry>>(files: &[E]) -> String {
    let entries: Vec<FileEntry> = files.iter().cloned().map(Into::into).collect();
    ScanSpec::files_json(&entries)
}

/// `request_limit` is the raw request limit, rendered as a trailing `LIMIT n` on the
/// one-row merge (a no-op for `n >= 1`, correct for `n = 0`).
pub struct AggregateMergeInputs {
    plan_types: Vec<String>,
    merge_select: Vec<String>,
    request_limit: Option<u64>,
}

impl AggregateMergeInputs {
    /// `None` for an empty `merge_select`: a narrower select list would fail Exasol's
    /// positional validation, so the caller must use its fallback.
    pub fn new(
        plan_types: Vec<String>,
        merge_select: Vec<String>,
        request_limit: Option<u64>,
    ) -> Option<Self> {
        if merge_select.is_empty() {
            return None;
        }
        Some(Self {
            plan_types,
            merge_select,
            request_limit,
        })
    }
}

/// No `SELECT * FROM (...)` materialization wrapper (decision [5]). The spec's
/// `aggregates` and `merge_inputs` must be present or absent together. Grouped
/// aggregates use `build_grouped_aggregate_scan_sql` instead.
#[allow(clippy::too_many_arguments)]
pub fn build_scan_driving_sql<E: Clone + Into<FileEntry>>(
    spec_template: &ScanSpec,
    shards: &[Vec<E>],
    proj_cols: &[ProjectionItem],
    proj_types: &[String],
    limit: Option<u64>,
    col_types: &[(String, String)],
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
            proj_cols,
            proj_types,
            limit,
            udf_name,
            distribute_udf_name,
        )
    }
}

/// An `Expr` gets a positional synthetic name matching the scan side's aliasing,
/// never its rendered SQL, so repeated literals cannot collide.
pub(super) fn emits_ident(item: &ProjectionItem, index: usize) -> String {
    match item {
        ProjectionItem::Column(name) => quote_ident(name),
        ProjectionItem::Expr { .. } => quote_ident(&format!("_LH_PROJ_{index}")),
    }
}

/// A matched top-N carries the same `order_by` in the per-shard blob (a DataFusion
/// TopK) and the outer merge; both render through [`render_order_by_clause`] so they
/// agree on direction and NULL placement.
fn build_row_scan_sql<E: Clone + Into<FileEntry>>(
    spec_template: &ScanSpec,
    shards: &[Vec<E>],
    proj_cols: &[ProjectionItem],
    proj_types: &[String],
    limit: Option<u64>,
    udf_name: &str,
    distribute_udf_name: &str,
) -> String {
    let emits = proj_cols
        .iter()
        .zip(proj_types.iter())
        .enumerate()
        .map(|(i, (item, ty))| format!("{} {}", emits_ident(item, i), ty))
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
    col_types: &[(String, String)],
    inputs: &AggregateMergeInputs,
    udf_name: &str,
    distribute_udf_name: &str,
) -> String {
    let emits_items = partial_emits_items(aggregates, col_types, &inputs.plan_types);
    let emits = emits_items.join(", ");
    let merge_select = inputs.merge_select.join(", ");

    let fan_out = build_fan_out_inner(spec_template, shards, &emits, udf_name, distribute_udf_name);

    let mut sql = format!("SELECT {merge_select} FROM ({fan_out})");
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
    col_types: &[(String, String)],
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
    col_types: &[(String, String)],
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
    let proj_item = ProjectionItem::Column(col.to_string());
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

/// A column missing `name` or `dataType` is skipped.
pub(super) fn column_types(
    request: &Json,
    select_table: impl FnOnce(&[Json]) -> Option<&Json>,
) -> Vec<(String, String)> {
    request
        .get("involvedTables")
        .and_then(|v| v.as_array())
        .and_then(|tables| select_table(tables))
        .and_then(|t| t.get("columns"))
        .and_then(|c| c.as_array())
        .map(|cols| {
            cols.iter()
                .filter_map(|c| {
                    let name = c.get("name")?.as_str()?.to_uppercase();
                    let dt_json = c.get("dataType")?;
                    Some((name, exasol_type_from_json(dt_json)))
                })
                .collect()
        })
        .unwrap_or_default()
}

pub(super) fn extract_all_column_types(request: &Json) -> Vec<(String, String)> {
    column_types(request, |tables: &[Json]| tables.first())
}

/// Exasol stamps every column with the query's `tableAlias`, but every scan is a
/// single relation with bare column names, so an alias-qualified reference does not
/// resolve. Must run after join attribution, which identifies legs by the
/// (`tableName`, `tableAlias`) pair (#193).
pub(super) fn strip_table_alias(expr: &Json) -> Json {
    match expr {
        Json::Object(map) => Json::Object(
            map.iter()
                .filter(|(key, _)| key.as_str() != "tableAlias")
                .map(|(key, value)| (key.clone(), strip_table_alias(value)))
                .collect(),
        ),
        Json::Array(items) => Json::Array(items.iter().map(strip_table_alias).collect()),
        other => other.clone(),
    }
}

/// `false` means the caller must self-apply the predicate in its own SQL, never
/// omit it: Exasol has no fallback for a delegated capability.
pub(super) fn datafusion_renderable(expr: &Json) -> bool {
    render_expression_safe(expr).is_some()
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

/// DataFusion does not implicitly cast a non-string `LIKE` subject to VARCHAR as
/// Exasol does, and column nodes carry no `dataType`, so this type-aware guard lives
/// in the adapter, not in `vs-expression` (#207, decision-log [1]).
///
/// For a bare-column subject: string types pass, DATE is wrapped as
/// `CAST(<col> AS VARCHAR)`, anything else or an unresolved name declines the whole
/// filter (the caller must self-apply it).
///
/// The DATE cast matches Exasol only under the default `NLS_DATE_FORMAT`; an altered
/// session format is a tracked exception (#216, decision-log [8]).
fn like_subject_type_guard(filter: &Json, col_types: &[(String, String)]) -> Option<Json> {
    rewrite_expr_tree(
        filter,
        &|out: &Json| match out.get("type").and_then(|t| t.as_str()) {
            Some("predicate_like" | "predicate_like_regexp") => guard_like_subject(out, col_types),
            _ => Some(out.clone()),
        },
    )
}

/// Folds with the same full-Unicode `to_uppercase` the `col_types` builders use.
/// Deliberately does not test the node's `type`: callers treat a non-column node as
/// a pass-through but an unresolved type as a decline.
fn column_exa_type<'t>(node: &Json, col_types: &'t [(String, String)]) -> Option<&'t str> {
    let name = node.get("name").and_then(|n| n.as_str())?.to_uppercase();
    col_types
        .iter()
        .find(|(n, _)| *n == name)
        .map(|(_, t)| t.as_str())
}

fn guard_like_subject(like_node: &Json, col_types: &[(String, String)]) -> Option<Json> {
    let subject = like_node.get("expression");

    let is_bare_column =
        subject.and_then(|s| s.get("type")).and_then(|t| t.as_str()) == Some("column");
    if !is_bare_column {
        return Some(like_node.clone());
    }
    let subject = subject.expect("is_bare_column implies expression is present");

    match column_exa_type(subject, col_types).map(classify_exa_type) {
        Some(ExaTypeClass::Character) => Some(like_node.clone()),
        // DataFusion's Date32→Utf8 cast is `YYYY-MM-DD`.
        Some(ExaTypeClass::Date) => {
            let mut out = like_node.clone();
            out["expression"] = wrap_cast_to_varchar(subject);
            Some(out)
        }
        // Other types' string forms diverge between engines, so decline rather than risk
        // a wrong or hard-failing cast (decision-log [2]).
        Some(ExaTypeClass::Decimal | ExaTypeClass::Other) | None => None,
    }
}

/// Exasol trims trailing DECIMAL scale zeros when stringifying (`2912.00`→`'2912'`);
/// DataFusion renders the full declared scale, a silent wrong result (#211).
///
/// Rewrites only a bare DECIMAL column that is the direct argument of a string
/// `CAST` (the whole cast is replaced), `CONCAT`, or `LENGTH`. Any other context,
/// and a computed argument whose type is unresolvable, is left untouched (a tracked
/// exception in the spec).
///
/// The closure never declines, so the `unwrap_or_else` fallback is unreachable.
fn rewrite_decimal_stringifications(node: &Json, col_types: &[(String, String)]) -> Json {
    rewrite_expr_tree(node, &|out: &Json| {
        let node_type = out.get("type").and_then(|t| t.as_str()).unwrap_or("");
        Some(match node_type {
            "function_scalar_cast" => {
                let target_is_string = out
                    .get("dataType")
                    .and_then(|d| d.get("type"))
                    .and_then(|t| t.as_str())
                    .map(|t| t.to_uppercase())
                    .is_some_and(|t| t == "VARCHAR" || t == "CHAR");
                if target_is_string
                    && let Some(Json::Array(args)) = out.get("arguments")
                    && let [arg] = args.as_slice()
                    && is_bare_decimal_column(arg, col_types)
                {
                    // Replace the whole cast; do not re-nest inside it.
                    return Some(wrap_decimal_to_varchar(arg));
                }
                out.clone()
            }
            "function_scalar" => {
                let fn_name = out
                    .get("name")
                    .and_then(|n| n.as_str())
                    .unwrap_or("")
                    .to_uppercase();
                let mut out = out.clone();
                if (fn_name == "CONCAT" || fn_name == "LENGTH")
                    && let Some(Json::Array(args)) = out.get("arguments")
                {
                    let rewritten: Vec<Json> = args
                        .iter()
                        .map(|a| {
                            if is_bare_decimal_column(a, col_types) {
                                wrap_decimal_to_varchar(a)
                            } else {
                                a.clone()
                            }
                        })
                        .collect();
                    out["arguments"] = Json::Array(rewritten);
                }
                out
            }
            _ => out.clone(),
        })
    })
    .unwrap_or_else(|| node.clone())
}

/// Integer columns arrive as `DECIMAL(p,0)`; the trim is a no-op on them.
fn is_bare_decimal_column(node: &Json, col_types: &[(String, String)]) -> bool {
    if node.get("type").and_then(|t| t.as_str()) != Some("column") {
        return false;
    }
    matches!(
        column_exa_type(node, col_types).map(classify_exa_type),
        Some(ExaTypeClass::Decimal)
    )
}

/// Never sent by Exasol; only this rewriter synthesizes it.
fn wrap_decimal_to_varchar(column: &Json) -> Json {
    serde_json::json!({
        "type": "decimal_to_varchar_exasol",
        "arguments": [column.clone()],
    })
}

/// Shared so the LIKE and string-function DATE branches are identical.
fn wrap_cast_to_varchar(node: &Json) -> Json {
    serde_json::json!({
        "type": "function_scalar_cast",
        "name": "CAST",
        "dataType": {"type": "VARCHAR"},
        "arguments": [node.clone()],
    })
}

/// String-typed arguments that Exasol implicitly converts to VARCHAR (#210).
#[derive(Debug, PartialEq, Eq)]
enum StringPositionArgs {
    /// Never declines: `CHR`/`UNICODECHR` take a genuine integer codepoint.
    NotGoverned,
    Coerce(Vec<usize>),
    /// This function at this arity is not rendered faithfully.
    Decline,
}

/// Every returned index is `< arg_count`.
///
/// `INSTR`/`LOCATE` beyond two arguments must decline: `vs-expression` silently drops
/// the extra arguments, so coercing would plan a truncated, wrong rendering (#228).
fn string_position_args(fn_name: &str, arg_count: usize) -> StringPositionArgs {
    let coerce_in_range = |indices: Vec<usize>| {
        StringPositionArgs::Coerce(indices.into_iter().filter(|i| *i < arg_count).collect())
    };
    match fn_name.to_uppercase().as_str() {
        "CONCAT" | "TRIM" | "LTRIM" | "RTRIM" | "REPLACE" | "TRANSLATE" => {
            coerce_in_range((0..arg_count).collect())
        }
        "LOWER" | "UPPER" | "ASCII" | "INITCAP" | "REVERSE" | "LENGTH" | "OCTET_LENGTH"
        | "UNICODE" | "SUBSTR" | "REPEAT" | "LEFT" | "RIGHT" => coerce_in_range(vec![0]),
        "LPAD" | "RPAD" if arg_count > 2 => coerce_in_range(vec![0, 2]),
        "LPAD" | "RPAD" => coerce_in_range(vec![0]),
        "INSTR" | "LOCATE" if arg_count > 2 => StringPositionArgs::Decline,
        "INSTR" | "LOCATE" => coerce_in_range(vec![0, 1]),
        _ => StringPositionArgs::NotGoverned,
    }
}

/// Exasol implicitly converts numeric or DATE string-function arguments to VARCHAR;
/// DataFusion fails at plan time (#210). A decline propagates to the whole tree and
/// must be self-applied by the caller. Must run before
/// [`rewrite_decimal_stringifications`] so a coerced argument is not double-wrapped.
fn string_function_arg_type_guard(node: &Json, col_types: &[(String, String)]) -> Option<Json> {
    rewrite_expr_tree(node, &|out: &Json| {
        if out.get("type").and_then(|t| t.as_str()) != Some("function_scalar") {
            return Some(out.clone());
        }
        let fn_name = out.get("name").and_then(|n| n.as_str()).unwrap_or("");
        let arg_count = out
            .get("arguments")
            .and_then(|a| a.as_array())
            .map_or(0, |a| a.len());
        match string_position_args(fn_name, arg_count) {
            StringPositionArgs::NotGoverned => Some(out.clone()),
            // `vs-expression` renders this arity incompletely (#228).
            StringPositionArgs::Decline => None,
            StringPositionArgs::Coerce(indices) => {
                let mut out = out.clone();
                for i in indices {
                    let coerced = coerce_string_position_arg(&out["arguments"][i], col_types)?;
                    out["arguments"][i] = coerced;
                }
                Some(out)
            }
        }
    })
}

/// A non-`column` argument is returned unchanged: its type is unresolvable, a tracked
/// exception (#223) that must not decline.
fn coerce_string_position_arg(arg: &Json, col_types: &[(String, String)]) -> Option<Json> {
    if arg.get("type").and_then(|t| t.as_str()) != Some("column") {
        return Some(arg.clone());
    }
    match column_exa_type(arg, col_types).map(classify_exa_type) {
        Some(ExaTypeClass::Character) => Some(arg.clone()),
        Some(ExaTypeClass::Date) => Some(wrap_cast_to_varchar(arg)),
        Some(ExaTypeClass::Decimal) => Some(wrap_decimal_to_varchar(arg)),
        // Other types' text forms diverge between engines; a cast would turn a crash into
        // a wrong answer.
        Some(ExaTypeClass::Other) | None => None,
    }
}

/// Order matters: the string-function guard must precede the decimal rewrite so a
/// coerced argument is not double-wrapped. `None` means a guard declined.
pub(super) fn apply_type_rewrites(expr: &Json, col_types: &[(String, String)]) -> Option<Json> {
    let expr = like_subject_type_guard(expr, col_types)?;
    let expr = string_function_arg_type_guard(&expr, col_types)?;
    Some(rewrite_decimal_stringifications(&expr, col_types))
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

/// The sole owner of "a tree the DataFusion scan may be handed". Renderability is
/// checked on the rewritten tree, since that is what the scan carries; checking the
/// raw tree would let an unrenderable tree be silently dropped, returning wrong rows.
pub(super) fn type_accepted_rewrite(expr: &Json, col_types: &[(String, String)]) -> Option<Json> {
    apply_type_rewrites(expr, col_types).filter(datafusion_renderable)
}

/// Returns `(scan_filter, declined)`, mutually exclusive (`_decision/045`).
/// `declined` is the original, un-rewritten tree: the type rewrites target the
/// DataFusion dialect only.
pub(super) fn classify_where_filter<'a>(
    filter_json_raw: Option<&'a Json>,
    col_types: &[(String, String)],
) -> (Option<String>, Option<&'a Json>) {
    match (
        filter_json_raw,
        filter_json_raw.and_then(|f| type_accepted_rewrite(f, col_types)),
    ) {
        (Some(raw), None) => (None, Some(raw)),
        (_, tree) => (tree.as_ref().and_then(render_df_filter_safe), None),
    }
}

/// The third element is the widening signal: `true` means the projection is the full
/// base row, not one item per select-list item (#196).
pub(super) fn extract_projection(
    request: &Json,
    pushdown_req: &Json,
) -> Result<(Vec<ProjectionItem>, Vec<String>, bool), UdfError> {
    project_columns(pushdown_req, extract_all_column_types(request))
}

/// Exasol rejects `TIMESTAMP WITH LOCAL TIME ZONE` as an EMITS output (sqlCode 22002).
fn is_valid_emits_output_type(ty: &str) -> bool {
    ty != "TIMESTAMP WITH LOCAL TIME ZONE"
}

/// `all_cols` is the column universe: the first involved table, or the union of both
/// tables for a broadcast join.
///
/// The widening flag is returned rather than re-derived downstream by comparing
/// arities, which coincide when the table width equals the select-list arity
/// (#196). A missing or empty select list is not a widening.
pub(super) fn project_columns(
    pushdown_req: &Json,
    all_cols: Vec<(String, String)>,
) -> Result<(Vec<ProjectionItem>, Vec<String>, bool), UdfError> {
    if all_cols.is_empty() {
        return Err(UdfError::User(
            "pushdown request has no column metadata".into(),
        ));
    }

    let type_by_upper = |name: &str| -> String {
        all_cols
            .iter()
            .find(|(n, _)| n == name)
            .map(|(_, t)| t.clone())
            .unwrap_or_else(|| "VARCHAR(2000000)".to_string())
    };

    let first_col_name = all_cols.first().map(|(n, _)| n.clone()).unwrap_or_default();

    let full_row = || -> (Vec<ProjectionItem>, Vec<String>) {
        let names = all_cols
            .iter()
            .map(|(n, _)| ProjectionItem::Column(n.clone()))
            .collect();
        let types = all_cols.iter().map(|(_, t)| t.clone()).collect();
        (names, types)
    };

    // Repeating `first_col_name` for an unprojectable item would duplicate EMITS
    // names, so the whole projection widens to the full row instead.
    let mut needs_full_fallback = false;
    let select_list = pushdown_req.get("selectList");
    let (proj_names, proj_types): (Vec<ProjectionItem>, Vec<String>) = match select_list {
        None | Some(Json::Null) => full_row(),
        Some(Json::Array(list)) if list.is_empty() => {
            let name = first_col_name;
            let ty = type_by_upper(&name);
            (vec![ProjectionItem::Column(name)], vec![ty])
        }
        Some(Json::Array(list)) => {
            let mut names = Vec::with_capacity(list.len());
            let mut types = Vec::with_capacity(list.len());
            for (i, e) in list.iter().enumerate() {
                // The EMITS column type must equal Exasol's declared `selectListDataTypes` entry.
                let declared_type = declared_select_type(pushdown_req, i);
                // An aggregate at any depth must never be evaluated per shard: its value would be
                // an unmerged partial (#194).
                if contains_aggregate_node(e) {
                    needs_full_fallback = true;
                    continue;
                }
                let Some(e) = apply_type_rewrites(e, &all_cols) else {
                    needs_full_fallback = true;
                    continue;
                };
                let e = &e;
                let item_type = e.get("type").and_then(|t| t.as_str()).unwrap_or("");
                match item_type {
                    "column" => {
                        let name = e
                            .get("name")
                            .and_then(|n| n.as_str())
                            .map(|s| s.to_uppercase())
                            .unwrap_or_else(|| first_col_name.clone());
                        let ty = type_by_upper(&name);
                        names.push(ProjectionItem::Column(name));
                        types.push(ty);
                    }
                    t if is_literal_selectlist_item(t) => {
                        // Pushed positionally so the emitted arity equals the select-list arity (#190).
                        match render_expression_safe(e) {
                            Some(sql_frag) => {
                                let ty = declared_type.clone();
                                if is_valid_emits_output_type(&ty) {
                                    names.push(ProjectionItem::Expr { expr: sql_frag });
                                    types.push(ty);
                                } else {
                                    needs_full_fallback = true;
                                }
                            }
                            None => {
                                needs_full_fallback = true;
                            }
                        }
                    }
                    // Every remaining node type `render_expression_safe` renders, except
                    // `predicate_greater[equal]`: Exasol normalises `a > b` to `b < a`.
                    "function_scalar"
                    | "function_scalar_cast"
                    // Synthesized from a DECIMAL-to-string cast by the rewrite above (#211).
                    | "decimal_to_varchar_exasol"
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
                    | "predicate_like_regexp" => {
                        match render_expression_safe(e) {
                            Some(sql_frag) => {
                                let ty = declared_type.clone();
                                if is_valid_emits_output_type(&ty) {
                                    names.push(ProjectionItem::Expr { expr: sql_frag });
                                    types.push(ty);
                                } else {
                                    needs_full_fallback = true;
                                }
                            }
                            None => {
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
                (names, types)
            }
        }
        _ => full_row(),
    };

    Ok((proj_names, proj_types, needs_full_fallback))
}

/// Blind descent (every field) is safe because a collect rebuilds nothing; do not
/// merge with the curated rewrite primitive, which must not enter `dataType`/`name`.
///
/// Case folding is left to callers on purpose: the join collectors fold ASCII-only
/// while `collect_all_column_names` folds Unicode (`ß` → `SS`), pinned by
/// `column_collectors_keep_divergent_case_folding`.
pub(super) fn walk_column_nodes(expr: &Json, f: &mut impl FnMut(&serde_json::Map<String, Json>)) {
    match expr {
        Json::Object(map) => {
            if map.get("type").and_then(|t| t.as_str()) == Some("column") {
                f(map);
            }
            for v in map.values() {
                walk_column_nodes(v, &mut *f);
            }
        }
        Json::Array(items) => {
            for item in items {
                walk_column_nodes(item, &mut *f);
            }
        }
        _ => {}
    }
}

pub(super) fn collect_all_column_names(
    value: &Json,
    names: &mut std::collections::HashSet<String>,
) {
    walk_column_nodes(value, &mut |map| {
        if let Some(name) = map.get("name").and_then(|n| n.as_str()) {
            names.insert(name.to_uppercase());
        }
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

pub(super) fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

pub(super) fn sql_string_literal(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
}

/// Its `VARCHAR(2000000)` fallback is the catch-all [`cast_to_declared_type`] reads as
/// "no usable declared type", so the two must stay together.
pub(super) fn declared_select_type(pushdown_req: &Json, select_index: usize) -> String {
    pushdown_req
        .get("selectListDataTypes")
        .and_then(|v| v.as_array())
        .and_then(|types| types.get(select_index))
        .map(exasol_type_from_json)
        .unwrap_or_else(|| "VARCHAR(2000000)".to_string())
}

/// `VARCHAR(2000000)` is exempt: it is the mapping's catch-all for unmappable types,
/// so casting to it would mislabel the value.
pub(super) fn cast_to_declared_type(expr: &str, declared: Option<&str>) -> String {
    match declared {
        Some(ty) if ty != "VARCHAR(2000000)" => format!("CAST({expr} AS {ty})"),
        _ => expr.to_string(),
    }
}

pub(super) fn scan_storage_for(
    creds: &ConnectionCreds,
    connection_name: &str,
    allow_http: bool,
    effective: &StorageBackend,
    sealing_key: Option<&SealedStorageKey>,
) -> Result<ScanStorage, UdfError> {
    if !creds.use_vended_credentials {
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
