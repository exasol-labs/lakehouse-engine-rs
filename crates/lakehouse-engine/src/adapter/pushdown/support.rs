//! Cross-cutting production helpers shared across the `pushdown` submodules.
//!
//! Extracted verbatim from the former flat `pushdown.rs`: the scan UDF-name
//! constants and shard-count math, the core scan-driving SQL builders, the
//! projection/limit/order-by extraction helpers, and small SQL/identifier
//! utilities. These are the items used across two or more capability clusters
//! (or by `handle_pushdown` plus a cluster), so they live here rather than in
//! any single capability module.

use super::grouped_agg::{
    cast_merge_items, col_type_for, is_literal_selectlist_item, partial_emits_items,
};
use super::single_group_agg::{DistinctCount, SingleGroupItem};
use crate::scan::spec::{
    AggregatePlan, CommonScanSpec, FileEntry, ProjectionItem, ScanSpec, render_order_by_clause,
};
use exasol_udf_sdk::error::UdfError;
use serde_json::Value as Json;
use vs_expression::render_expression_safe;

/// The registered SQL name of the scan SCALAR EMIT UDF entry point.
pub(super) const SCAN_UDF_NAME: &str = "LAKEHOUSE_SCAN";

/// The registered SQL name of the file-distributor LUA SET script.
/// The nested fan-out subquery groups the per-shard file-list rows by `shard_key`
/// through this passthrough distributor so Exasol spreads the work units across
/// nodes; the outer ungrouped scalar scan then streams over the distributed rows.
/// Like the scan/merge scripts it must be schema-qualified to resolve outside the
/// adapter schema.
pub(super) const DISTRIBUTE_FILES_UDF_NAME: &str = "LAKEHOUSE_DISTRIBUTE_FILES";

/// Maximum shard count: Exasol distributes groups round-robin below this threshold;
/// above it Exasol hash-partitions them (no longer balanced).
const MAX_SHARD_COUNT: usize = 300;

/// Compute the work-unit shard count G for a given cluster configuration.
///
/// G = clamp(node_count × parallelism_factor, 1, min(file_count, 300)).
///
/// - The product is saturating (no overflow).
/// - G is at least 1 and at most `file_count` so no shard is empty.
/// - G is also at most 300 to stay in Exasol's round-robin distribution regime.
///
/// When `file_count` is zero this returns 1 (caller should skip partition_files).
pub fn shard_count(node_count: usize, parallelism_factor: usize, file_count: usize) -> usize {
    let raw = node_count.saturating_mul(parallelism_factor);
    let upper = file_count.clamp(1, MAX_SHARD_COUNT);
    raw.clamp(1, upper)
}

/// Serialize one shard's file list to the per-shard UDF argument JSON.
///
/// Generic over the shard element so production (`FileEntry`, carrying its
/// positional-delete refs) and legacy/test call sites (bare `(path, size)`
/// tuples) share one path: each element is converted into a [`FileEntry`] via
/// `Into` — the identity conversion for a `FileEntry` (deletes preserved) and
/// the delete-free [`FileEntry::new`] for a tuple — before serialization.
pub(super) fn shard_files_json<E: Clone + Into<FileEntry>>(files: &[E]) -> String {
    let entries: Vec<FileEntry> = files.iter().cloned().map(Into::into).collect();
    ScanSpec::files_json(&entries)
}

/// Build the scan-driving SQL from a resolved file list partitioned into shards.
///
/// **Row queries** (no aggregates in spec) — the outer ungrouped scalar scan is the
/// top-level query; no `SELECT * FROM (...)` materialization wrapper (decision [5]):
/// - Single shard: `SELECT {udf}('{common}', '{files}') EMITS ({emits}) [ORDER BY …] [LIMIT n]`
/// - Multi-shard: `SELECT {udf}('{common}', files) EMITS ({emits}) FROM (distributor with GROUP BY shard_key) [ORDER BY …] [LIMIT n]`
///
/// **Aggregate queries** (spec carries `aggregates`, no `group_keys`):
/// - The outer merge SELECT sits directly over the scalar scan (never SELECT *).
/// - The EMITS clause and the outer merge follow the COLUMN CONTRACT from
///   `crate::scan::build_partial_agg_sql`.
///
/// For grouped aggregate queries (spec carries both `aggregates` and `group_keys`),
/// use `build_grouped_aggregate_scan_sql` directly.
///
/// `spec_template` carries the shared fields; only `files` is replaced per shard.
/// `col_types` is the full table column type map `(uppercase_name, exasol_type)` used
/// to assign the correct EMITS type per aggregate partial column.
/// `aggregate_types` holds the Exasol-declared result type of each aggregate (from
/// `aggregate_exasol_types`); the single-group merge casts each item to its declared
/// type. Pass `&[]` to emit uncast merge items (row scans never read it).
#[allow(clippy::too_many_arguments)]
pub fn build_scan_driving_sql<E: Clone + Into<FileEntry>>(
    spec_template: &ScanSpec,
    shards: &[Vec<E>],
    proj_cols: &[ProjectionItem],
    proj_types: &[String],
    limit: Option<u64>,
    col_types: &[(String, String)],
    aggregate_types: &[String],
    udf_name: &str,
    distribute_udf_name: &str,
) -> String {
    if let Some(aggregates) = spec_template.common.aggregates.as_deref() {
        build_aggregate_scan_sql(
            spec_template,
            shards,
            aggregates,
            col_types,
            aggregate_types,
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

/// The positional EMITS identifier for a projection item at select-list index `index`.
///
/// A [`Column`](ProjectionItem::Column) keeps its real quoted source-column name, so an
/// outer top-N `ORDER BY` that references a projected column by name still resolves. An
/// [`Expr`](ProjectionItem::Expr) gets a positional-unique SYNTHETIC name (`_LH_PROJ_{index}`,
/// matching the scan side's `raw_scan::build_scan_sql` aliasing), never its rendered SQL
/// text, so repeated literals never collide into a duplicate EMITS name. Returns the
/// already-quoted identifier.
pub(super) fn emits_ident(item: &ProjectionItem, index: usize) -> String {
    match item {
        ProjectionItem::Column(name) => quote_ident(name),
        ProjectionItem::Expr { .. } => quote_ident(&format!("_LH_PROJ_{index}")),
    }
}

/// Build the row-scan SQL (no aggregates) as an OUTER UNGROUPED scalar scan over the
/// nested distributor — no `SELECT * FROM (...)` materialization wrapper (decision
/// [5]). Result-equivalence (decision [7]): with no outer GROUP BY the returned rows
/// are exactly the union of every shard's rows.
///
/// ## Ordered top-N
///
/// When `spec_template.order_by` is non-empty the query is a matched ordered
/// top-N: the outer scalar select carries `ORDER BY <keys> LIMIT n` so the returned
/// SQL is self-contained (it does not depend on Exasol re-applying the ordering).
/// Each shard's common blob carries the SAME `order_by` keys (and `limit`), which the
/// scan UDF renders as a per-shard bounded `ORDER BY … LIMIT n` (a DataFusion TopK).
/// The outer merge `ORDER BY` and the per-shard `ORDER BY` render through the one
/// shared [`render_order_by_clause`] seam, so they agree on direction and NULL
/// placement — the correctness-critical invariant. `order_by` is empty for plain
/// (unordered) row scans.
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

    // The fan-out primitive returns the OUTER UNGROUPED scalar scan directly (with
    // the `GROUP BY shard_key` fan-out nested inside the distributor, or a from-less
    // scalar call on literals for a single shard). No `SELECT * FROM (...)` wrapper:
    // that was the un-flattenable materialization boundary this change removes
    // (decision [5]). Result-equivalence (decision [7]): with no outer GROUP BY the
    // returned rows are exactly the union of every shard's rows.
    let mut sql = build_fan_out_inner(spec_template, shards, &emits, udf_name, distribute_udf_name);

    // Outer merge ORDER BY, rendered once (empty when not a matched top-N), attached
    // DIRECTLY to the outer scalar select. SQL requires ORDER BY before LIMIT, so it
    // is appended ahead of the LIMIT clause. The per-shard common blob carries the
    // same keys so each shard runs the same bounded sort; this outer ORDER BY merges
    // the per-shard partial orderings.
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

/// Build the aggregate scan SQL: the outer merge SELECT aggregates the per-shard
/// partial columns DIRECTLY over the scalar scan (no `SELECT * FROM (...)` wrapper).
///
/// The EMITS clause names and types follow the COLUMN CONTRACT defined in
/// `crate::scan::build_partial_agg_sql`.  The outer merge SELECT consumes those
/// exact column names.
#[allow(clippy::too_many_arguments)]
fn build_aggregate_scan_sql<E: Clone + Into<FileEntry>>(
    spec_template: &ScanSpec,
    shards: &[Vec<E>],
    aggregates: &[AggregatePlan],
    col_types: &[(String, String)],
    aggregate_types: &[String],
    udf_name: &str,
    distribute_udf_name: &str,
) -> String {
    let emits_items = partial_emits_items(aggregates, col_types, aggregate_types);
    let emits = emits_items.join(", ");
    let merge_select = cast_merge_items(aggregates, aggregate_types).join(", ");

    // The outer merge SELECT sits DIRECTLY over the scalar scan — no
    // `SELECT * FROM (...)` between them (decision [5]). The primitive short-circuits
    // to a from-less scalar call for a single shard; for multi-shard it nests the
    // `GROUP BY shard_key` fan-out in the distributor. Either way the scalar scan
    // fires once per shard (one partial-agg row per shard), so the outer merge over
    // those partials equals the single-node aggregate (result-equivalence, [7]).
    let fan_out = build_fan_out_inner(spec_template, shards, &emits, udf_name, distribute_udf_name);

    format!("SELECT {merge_select} FROM ({fan_out})")
}

/// Build the outer wrapper SQL for a lone single-group `COUNT(DISTINCT ...)` — Case 1
/// (see `vs-adapter/pushdown-planning-count-distinct`).
///
/// The one distinct item becomes a DISTINCT row-scan fan-out — a single-column
/// projection with `distinct = true` and a NULL-excluding filter — whose per-shard
/// local distinct rows are counted by a native Exasol `COUNT(DISTINCT "V")`:
/// `SELECT COUNT(DISTINCT "V") FROM (<fan-out>)`.
///
/// This is the ONLY count-distinct shape that fans out. A Case 2/3 request (more than
/// one distinct, or a distinct mixed with an ordinary aggregate) NEVER reaches this
/// builder: the single-group dispatch (`is_lone_count_distinct`) declines it to the
/// qualified single-table wrapper, because no composition of several scalar-subquery
/// emitting-UDF calls in one select list compiles in Exasol (`sqlCode 04000`,
/// "emitting function in expression").
///
/// LIMIT/OFFSET/ORDER BY are NEVER pushed into the distinct fan-out (leaking one would
/// truncate the per-shard distinct set → a wrong count); the caller-guarded `limit` is
/// applied only to the outer wrapper. The native `COUNT(DISTINCT "V")` yields exactly
/// the type Exasol declares for a `COUNT(DISTINCT)`, so the count needs no output cast.
///
/// `base_spec` carries the shared shard-invariant fields (filter, storage, schema,
/// tuning) with `aggregates`/`projection`/`emit_exa_types` empty, `distinct` false,
/// and no LIMIT/ORDER BY.
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
    // Only Case 1 (exactly one COUNT(DISTINCT), nothing else) is dispatched here; the
    // single-group `is_lone_count_distinct` guard declines every Case 2/3 shape to the
    // qualified single-table wrapper before this builder is reached.
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

/// Build one DISTINCT row-scan fan-out for a lone `COUNT(DISTINCT <bare column>)`.
///
/// The fan-out is a row scan projecting ONLY the distinct column, with
/// `distinct = true` and the base WHERE narrowed by a NULL-excluding predicate on
/// the column. It carries NO LIMIT/OFFSET/ORDER BY (leaking one would truncate the
/// per-shard distinct set → a wrong count). The single emitted column is named `"V"`,
/// carrying the RAW VALUES of the counted column — one row per shard-local distinct
/// value — NOT a count. `"V"` is declared with the source column's own exact Exasol
/// type (from `col_types`), so the outer native `COUNT(DISTINCT "V")` dedups across
/// shards on exact values with no cast step.
///
/// Only a bare-column argument reaches this builder: [`is_lone_count_distinct`] requires
/// `dc.column.is_some()`, so a `COUNT(DISTINCT <expression>)` — alone or combined with
/// other aggregates — declines to the qualified single-table wrapper, where Exasol
/// evaluates the expression and DISTINCT natively over exact-typed base columns (no
/// `arrow::compute::cast(.., Utf8)` injectivity dependency, which could silently
/// undercount). An expression-argument `DistinctCount` (`column` `None`) is therefore
/// unreachable here.
///
/// [`is_lone_count_distinct`]: super::single_group_agg::is_lone_count_distinct
fn build_distinct_fan_out<E: Clone + Into<FileEntry>>(
    base_spec: &ScanSpec,
    shards: &[Vec<E>],
    dc: &DistinctCount,
    col_types: &[(String, String)],
    udf_name: &str,
    distribute_udf_name: &str,
) -> String {
    // Only a lone bare-column COUNT(DISTINCT) is dispatched here (Case 1): the
    // single-group `is_lone_count_distinct` guard requires `dc.column.is_some()`, so an
    // expression argument declines to the qualified single-table wrapper before this
    // builder is reached. Mirrors the sibling `unreachable!` in
    // `build_count_distinct_scan_sql` rather than silently emitting an empty identifier.
    let Some(col) = dc.column.as_deref() else {
        unreachable!(
            "build_distinct_fan_out is dispatched only for a lone bare-column \
             COUNT(DISTINCT) (Case 1); an expression argument routes to the qualified \
             single-table wrapper instead"
        )
    };
    // "V" carries the column's raw values → its source Exasol type, so the outer
    // native COUNT(DISTINCT "V") dedups across shards on exact values, no cast step.
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
            emit_exa_types: vec![value_type.clone()],
            ..base_spec.common.clone()
        },
        files: base_spec.files.clone(),
    };
    let emits = format!(r#""V" {value_type}"#);
    build_fan_out_inner(&spec, shards, &emits, udf_name, distribute_udf_name)
}

/// Builds the shard fan-out SELECT that Exasol distributes across nodes.
///
/// Emits a nested `LAKEHOUSE_DISTRIBUTE_FILES` LUA SET distributor — which does the
/// `GROUP BY shard_key` fan-out (NOT `IPROC()`) so work units spread round-robin
/// across nodes (G ≤ 300) and multiplex onto each node's core pool — wrapped by an
/// outer UNGROUPED scalar `LAKEHOUSE_SCAN('{common}', files)` scan. Separating the
/// fan-out from the scan is what lets Exasol STREAM the scan output: with no
/// top-level `GROUP BY`, the scalar scan's emitted rows are not buffered into a
/// materializing `tmp_subselect` temp table.
///
/// The shard-INVARIANT common blob (credentials, projection, filter, aggregates,
/// tuning knobs) is serialized ONCE via `to_common_json()` as the outer scalar
/// scan's first-argument literal; only the per-shard files list flows through the
/// distributor (one `VALUES` row per shard). Because the fan-out carries only the
/// file-list strings, its payload is independent of the data volume scanned.
///
/// A single-shard plan short-circuits the distributor entirely: a from-less scalar
/// call on literals (`SELECT {udf}('{common}', '{files}') EMITS (...)`) — a scalar
/// EMIT UDF over constant literals fires exactly once, so no driving relation is
/// needed. Callers attach `ORDER BY`/`LIMIT` or an outer merge directly to the
/// returned bare SELECT.
pub fn build_fan_out_inner<E: Clone + Into<FileEntry>>(
    spec_template: &ScanSpec,
    shards: &[Vec<E>],
    emits: &str,
    udf_name: &str,
    distribute_udf_name: &str,
) -> String {
    // Serialize the shard-invariant common blob exactly once.
    let common_literal = sql_string_literal(&spec_template.to_common_json());

    // Single-shard short-circuit: a from-less scalar call on literals. A scalar EMIT
    // UDF over constant literals fires exactly once, so the distributor and the inner
    // GROUP BY are unnecessary.
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
    // The distributor is a LUA SET script with a STATIC `EMITS (files VARCHAR(...))`
    // definition, so its call MUST NOT carry a query-side `EMITS` clause — supplying
    // one is rejected by Exasol ("static return argument definition. Dynamic return
    // arguments are not supported in this case"). Only the scan (dynamic-output SCALAR)
    // carries a query-side EMITS.
    format!(
        "SELECT {udf}({common}, files) EMITS ({emits}) FROM (SELECT {distribute}(files) FROM (VALUES {values}) AS shards(shard_key, files) GROUP BY shard_key)",
        udf = udf_name,
        common = common_literal,
        emits = emits,
        distribute = distribute_udf_name,
        values = values_list,
    )
}

/// Extract all columns and their Exasol types from the first involved table.
pub(super) fn extract_all_column_types(request: &Json) -> Vec<(String, String)> {
    request
        .get("involvedTables")
        .and_then(|v| v.as_array())
        .and_then(|tables| tables.first())
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

/// Extract the projected columns and their Exasol types from the pushdown request.
///
/// For `column` nodes: returns the uppercase column name and its Exasol type.
/// For scalar expression nodes (e.g. `function_scalar`): renders via the VS expression
/// translator and returns the rendered SQL fragment with type `VARCHAR(2000000)`.
/// If any select-list item can't be projected as-is (untranslatable scalar, or an
/// aggregate/unknown node), the whole projection falls back to the full base table
/// column set so Exasol can post-process the expression, GROUP BY, and aggregate —
/// correctness over pushdown. The returned projection is positional: exactly one
/// item per select-list item, in select-list order.
pub(super) fn extract_projection(
    request: &Json,
    pushdown_req: &Json,
) -> Result<(Vec<ProjectionItem>, Vec<String>), UdfError> {
    project_columns(pushdown_req, extract_all_column_types(request))
}

/// Whether an Exasol declared type is a valid UDF EMITS output type.
/// Exasol rejects `TIMESTAMP WITH LOCAL TIME ZONE` as an EMITS output
/// (sqlCode 22002), so a rendered item declared that type declines to the
/// full-base-row fallback instead of emitting an EMITS clause the scan rejects.
fn is_valid_emits_output_type(ty: &str) -> bool {
    ty != "TIMESTAMP WITH LOCAL TIME ZONE"
}

/// Resolve a pushdown request's select list into an ordered projection and its
/// positionally-aligned Exasol EMITS types, drawing from a given column universe.
///
/// `all_cols` is the `(UPPERCASE name, Exasol type)` set the projection may
/// reference: the first involved table for a single-table scan, or the disjoint
/// union of BOTH involved tables for a broadcast join. Factoring the select-list
/// logic here lets the join path reuse it verbatim — a projected column's EMITS
/// type is looked up in whichever side owns it, with no bespoke join code — while
/// the single-table path is unchanged.
pub(super) fn project_columns(
    pushdown_req: &Json,
    all_cols: Vec<(String, String)>,
) -> Result<(Vec<ProjectionItem>, Vec<String>), UdfError> {
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

    // Every column of the base row, each as a bare column reference. Used by the
    // no-select-list, unknown-node, and untranslatable-item fallbacks so Exasol
    // has the full row to post-process the query itself.
    let full_row = || -> (Vec<ProjectionItem>, Vec<String>) {
        let names = all_cols
            .iter()
            .map(|(n, _)| ProjectionItem::Column(n.clone()))
            .collect();
        let types = all_cols.iter().map(|(_, t)| t.clone()).collect();
        (names, types)
    };

    let select_list = pushdown_req.get("selectList");
    let (proj_names, proj_types): (Vec<ProjectionItem>, Vec<String>) = match select_list {
        None | Some(Json::Null) => full_row(),
        Some(Json::Array(list)) if list.is_empty() => {
            // Empty select list — project the first column only.
            let name = first_col_name;
            let ty = type_by_upper(&name);
            (vec![ProjectionItem::Column(name)], vec![ty])
        }
        Some(Json::Array(list)) => {
            // Exasol declares the result type of each selectList item in a parallel
            // `selectListDataTypes` array; the EMITS column type must equal it.
            let declared_types = pushdown_req
                .get("selectListDataTypes")
                .and_then(|v| v.as_array());
            let mut names = Vec::with_capacity(list.len());
            let mut types = Vec::with_capacity(list.len());
            // If any item can't be projected as-is (untranslatable scalar, or an
            // aggregate/unknown node), we can't emit a per-item projection — repeating
            // `first_col_name` would yield duplicate EMITS names. Instead project the
            // full base row so Exasol has every column to post-process the expression,
            // GROUP BY, and aggregate itself.
            let mut needs_full_fallback = false;
            for (i, e) in list.iter().enumerate() {
                let declared_type = declared_types
                    .and_then(|d| d.get(i))
                    .map(exasol_type_from_json);
                let item_type = e.get("type").and_then(|t| t.as_str()).unwrap_or("");
                match item_type {
                    "column" => {
                        // Bare column reference — use the column name and its Exasol type.
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
                        // A bare literal renders to a constant SQL expression (e.g.
                        // `NULL`, `'x'`, `5`) — push it positionally as an `Expr`,
                        // exactly like the scalar-expression branch below, so the
                        // emitted arity equals the select-list arity (issue #190).
                        match render_expression_safe(e) {
                            Some(sql_frag) => {
                                let ty = declared_type
                                    .clone()
                                    .unwrap_or_else(|| "VARCHAR(2000000)".to_string());
                                if is_valid_emits_output_type(&ty) {
                                    names.push(ProjectionItem::Expr { expr: sql_frag });
                                    types.push(ty);
                                } else {
                                    // Declared type is not a valid EMITS output (e.g.
                                    // TIMESTAMP WITH LOCAL TIME ZONE) — decline to the
                                    // full-base-row fallback rather than emit a clause
                                    // the scan rejects at scan time.
                                    needs_full_fallback = true;
                                }
                            }
                            None => {
                                // Translator declined — fall back to projecting the full row.
                                needs_full_fallback = true;
                            }
                        }
                    }
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
                    | "predicate_not" => {
                        // Scalar expression node — try to render it.
                        match render_expression_safe(e) {
                            Some(sql_frag) => {
                                let ty = declared_type
                                    .clone()
                                    .unwrap_or_else(|| "VARCHAR(2000000)".to_string());
                                if is_valid_emits_output_type(&ty) {
                                    names.push(ProjectionItem::Expr { expr: sql_frag });
                                    types.push(ty);
                                } else {
                                    // Declared type is not a valid EMITS output — decline
                                    // to the full-base-row fallback.
                                    needs_full_fallback = true;
                                }
                            }
                            None => {
                                // Untranslatable — fall back to projecting the full row.
                                needs_full_fallback = true;
                            }
                        }
                    }
                    _ => {
                        // Unknown / aggregate node — fall back to projecting the full row.
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

    Ok((proj_names, proj_types))
}

/// Extract LIMIT from the pushdown request.
pub(super) fn extract_limit(pushdown_req: &Json) -> Option<u64> {
    pushdown_req
        .get("limit")
        .and_then(|l| l.get("numElements"))
        .and_then(|n| n.as_u64())
}

/// Whether the pushdown request carries a non-empty `orderBy` array.
///
/// Exasol sends `orderBy` only when the adapter advertises an `ORDER_BY_*`
/// capability AND the query has an ORDER BY it can delegate; it withholds `limit`
/// entirely when it cannot delegate the ordering (verified live — decision log A1).
/// So this flag is the trigger for the anti-wrong-truncation guard (decision [4]):
/// when an `orderBy` is present but the request is not a matched ordered top-N, the
/// per-shard AND outer `LIMIT` are withheld and Exasol re-applies both clauses.
pub(super) fn order_by_present(pushdown_req: &Json) -> bool {
    pushdown_req
        .get("orderBy")
        .and_then(|v| v.as_array())
        .is_some_and(|a| !a.is_empty())
}

/// Derive an Exasol type string from the VS column dataType JSON.
pub(super) fn exasol_type_from_json(dt: &Json) -> String {
    let type_name = dt.get("type").and_then(|t| t.as_str()).unwrap_or("varchar");
    match type_name.to_lowercase().as_str() {
        "boolean" => "BOOLEAN".to_string(),
        "decimal" => {
            let p = dt.get("precision").and_then(|v| v.as_u64()).unwrap_or(18);
            let s = dt.get("scale").and_then(|v| v.as_u64()).unwrap_or(0);
            if p <= 36 && s <= 36 {
                format!("DECIMAL({p},{s})")
            } else {
                "VARCHAR(2000000)".to_string()
            }
        }
        "double" => "DOUBLE PRECISION".to_string(),
        "date" => "DATE".to_string(),
        "timestamp" => {
            let with_local_time_zone = dt
                .get("withLocalTimeZone")
                .and_then(|v| v.as_bool())
                .unwrap_or(false);
            if with_local_time_zone {
                "TIMESTAMP WITH LOCAL TIME ZONE".to_string()
            } else {
                "TIMESTAMP".to_string()
            }
        }
        _ => {
            // VARCHAR, CHAR, and all others.
            let size = dt.get("size").and_then(|v| v.as_u64()).unwrap_or(2000000);
            let capped = size.min(2000000);
            let is_ascii = dt
                .get("characterSet")
                .and_then(|v| v.as_str())
                .is_some_and(|cs| cs.eq_ignore_ascii_case("ASCII"));
            if is_ascii {
                format!("VARCHAR({capped}) ASCII")
            } else {
                format!("VARCHAR({capped})")
            }
        }
    }
}

/// Resolve the Exasol-declared type of each aggregate select-list item, in order.
///
/// Aggregates appear as `function_aggregate` items in `selectList`; the parallel
/// `selectListDataTypes` array gives each one's declared result type (e.g. COUNT(*)
/// → DECIMAL(18,0)). Falls back to `VARCHAR(2000000)` when not locatable.
pub(super) fn aggregate_exasol_types(pushdown_req: &Json) -> Vec<String> {
    let select_list = match pushdown_req.get("selectList").and_then(|v| v.as_array()) {
        Some(l) => l,
        None => return Vec::new(),
    };
    let declared_types = pushdown_req
        .get("selectListDataTypes")
        .and_then(|v| v.as_array());
    select_list
        .iter()
        .enumerate()
        .filter(|(_, item)| item.get("type").and_then(|t| t.as_str()) == Some("function_aggregate"))
        .map(|(idx, _)| {
            declared_types
                .and_then(|d| d.get(idx))
                .map(exasol_type_from_json)
                .unwrap_or_else(|| "VARCHAR(2000000)".to_string())
        })
        .collect()
}

/// Double-quote an identifier.
pub(super) fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// Produce a SQL string literal with single-quote escaping.
pub(super) fn sql_string_literal(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
}

/// Redact credential-shaped values from a catalog error message.
pub(super) fn redact_catalog_error(msg: &str) -> String {
    crate::scan::emit::redact_credentials(msg)
}

#[cfg(test)]
mod tests {
    use super::super::test_support::*;
    use super::*;
    use crate::scan::spec::{AggKind, DeleteFileContentType, SortKey};
    use vs_expression::render_df_filter_safe;

    /// `exasol_type_from_json` must read the `withLocalTimeZone` flag back off a
    /// `{"type":"timestamp", ...}` dataType JSON (the shape Exasol echoes back in
    /// `involvedTables[].columns[].dataType` for a VS column declared via
    /// `exasol_type_to_json`), not just the bare `"type"` string — otherwise a
    /// TIMESTAMP WITH LOCAL TIME ZONE column round-trips back into the pushdown
    /// path as plain TIMESTAMP and Exasol rejects the EMITS type mismatch.
    #[test]
    fn exasol_type_from_json_reads_with_local_time_zone_flag() {
        let tstz = serde_json::json!({"type": "timestamp", "withLocalTimeZone": true});
        assert_eq!(
            exasol_type_from_json(&tstz),
            "TIMESTAMP WITH LOCAL TIME ZONE"
        );

        let ts = serde_json::json!({"type": "timestamp"});
        assert_eq!(exasol_type_from_json(&ts), "TIMESTAMP");
    }

    /// `exasol_type_from_json` must read the `characterSet` field back off a
    /// `{"type":"varchar", ...}` dataType JSON (Exasol's wire format for CHAR/VARCHAR
    /// select-list items, e.g. `{"type":"CHAR","size":3,"characterSet":"ASCII"}` as
    /// confirmed by `vs-expression`'s `renders_cast_char_as_varchar` test) and append
    /// `" ASCII"` when it is `"ASCII"` (case-insensitively) — otherwise a CASE/literal
    /// expression Exasol declares as `VARCHAR(n) ASCII` round-trips back through our
    /// EMITS clause as bare `VARCHAR(n)`, which Exasol's type checker treats as
    /// `VARCHAR(n) UTF8` by default, causing a "Data type mismatch" pushdown error
    /// (issue #136 follow-up).
    #[test]
    fn exasol_type_from_json_propagates_ascii_character_set() {
        let ascii = serde_json::json!({"type": "VARCHAR", "size": 4, "characterSet": "ASCII"});
        assert_eq!(exasol_type_from_json(&ascii), "VARCHAR(4) ASCII");

        let no_charset = serde_json::json!({"type": "VARCHAR", "size": 4});
        assert_eq!(exasol_type_from_json(&no_charset), "VARCHAR(4)");
    }

    // ---------------------------------------------------------------------------
    // Task 1.2 — adapter carries positional deletes into the per-shard scan spec
    // ---------------------------------------------------------------------------

    /// A minimal delete-carrying row-scan `ScanSpec` template (files replaced per
    /// shard by the builder), used to assert what the per-shard/common arguments
    /// carry.
    fn delete_spec_template() -> ScanSpec {
        ScanSpec {
            common: CommonScanSpec {
                table_root: "s3://warehouse/db/table".into(),
                projection: vec![ProjectionItem::Column("ID".into())],
                emit_exa_types: vec!["DECIMAL(20,0)".into()],
                storage: sample_storage(),
                ..Default::default()
            },
            files: vec![],
        }
    }

    /// Positional deletes survive into the per-shard scan spec for BOTH
    /// `write.delete.granularity=file` (one data file → its own delete file) and
    /// `partition` (one delete file referenced by multiple data files).
    #[test]
    fn adapter_preserves_positional_deletes_into_scan_spec() {
        // file granularity: one data file carries its own positional-delete file.
        let file_gran = vec![FileEntry::with_deletes(
            "data/part-0.parquet",
            1000,
            vec![pos_delete("data/deletes/del-0.parquet", 50)],
        )];
        let back = ScanSpec::files_from_json(&shard_files_json(&file_gran)).unwrap();
        assert_eq!(back, file_gran, "file-granularity deletes must round-trip");
        assert_eq!(back[0].deletes.len(), 1);
        assert_eq!(
            back[0].deletes[0].content_type,
            DeleteFileContentType::PositionDeletes
        );

        // partition granularity: the SAME delete file is referenced by two data files.
        let shared = "data/deletes/part-del.parquet";
        let part_gran = vec![
            FileEntry::with_deletes("data/p0.parquet", 1, vec![pos_delete(shared, 80)]),
            FileEntry::with_deletes("data/p1.parquet", 1, vec![pos_delete(shared, 80)]),
        ];
        let back2 = ScanSpec::files_from_json(&shard_files_json(&part_gran)).unwrap();
        assert_eq!(
            back2, part_gran,
            "both data files must retain the shared partition delete"
        );
        assert_eq!(back2[1].deletes[0].path, shared);
    }

    /// A delete-carrying entry serializes with its content type on the wire; a
    /// delete-free entry stays the compact `[path, size]` 2-tuple (no wire bloat,
    /// backward-compatible with pre-delete payloads).
    #[test]
    fn delete_file_entry_carries_content_type_and_delete_free_stays_compact() {
        let with_del = vec![FileEntry::with_deletes(
            "d.parquet",
            5,
            vec![pos_delete("del.parquet", 2)],
        )];
        let json = shard_files_json(&with_del);
        assert!(
            json.contains("position_deletes"),
            "delete content type must appear on the wire: {json}"
        );
        let back = ScanSpec::files_from_json(&json).unwrap();
        assert_eq!(
            back[0].deletes[0].content_type,
            DeleteFileContentType::PositionDeletes
        );

        let free = vec![FileEntry::new("data/part-0.parquet", 1000)];
        assert_eq!(
            shard_files_json(&free),
            r#"[["data/part-0.parquet",1000]]"#,
            "delete-free entry must stay the compact 2-tuple form"
        );
    }

    /// Delete refs ride ONLY in the per-shard files argument, never in the
    /// shard-invariant common blob, and the common blob carries no serialized
    /// Iceberg schema or bound predicate (the minimal-surface decision).
    #[test]
    fn adapter_carries_delete_refs_per_shard_minimal_common_spec() {
        let spec_template = delete_spec_template();
        let shards = vec![vec![FileEntry::with_deletes(
            "data/part-0.parquet",
            1000,
            vec![pos_delete("data/deletes/del-0.parquet", 50)],
        )]];
        let sql = build_scan_driving_sql(
            &spec_template,
            &shards,
            &[ProjectionItem::Column("ID".into())],
            &["DECIMAL(20,0)".to_string()],
            None,
            &[],
            &[],
            SCAN_UDF_NAME,
            DISTRIBUTE_FILES_UDF_NAME,
        );
        assert!(
            sql.contains("del-0.parquet"),
            "per-shard files argument must carry the delete file: {sql}"
        );
        let common = common_arg_literal(&sql);
        assert!(
            !common.contains("del-0.parquet"),
            "common blob must NOT carry per-shard delete refs: {common}"
        );
        assert!(
            !common.contains("BoundPredicate") && !common.contains("bound_predicate"),
            "common blob must carry no serialized iceberg predicate: {common}"
        );
    }

    /// The shared fan-out primitive emits a nested `LAKEHOUSE_DISTRIBUTE_FILES`
    /// distributor (`GROUP BY shard_key` over the per-shard file lists) wrapped by an
    /// outer UNGROUPED scalar `LAKEHOUSE_SCAN('{common}', files)` select. The
    /// shard-invariant common blob is spliced exactly ONCE (the outer scalar's first
    /// argument); only the per-shard `files` strings flow through the distributor, so
    /// the fan-out payload is data-volume-independent.
    #[test]
    fn fan_out_primitive_wraps_distributor_in_ungrouped_scalar_scan() {
        let spec = delete_spec_template();
        let shards = vec![
            vec![FileEntry::new("data/part-0.parquet", 1000)],
            vec![FileEntry::new("data/part-1.parquet", 2000)],
        ];
        let emits = r#""ID" DECIMAL(20,0)"#;
        let sql = build_fan_out_inner(&spec, &shards, emits, "SCAN", "DISTRIBUTE");

        assert!(
            sql.contains("DISTRIBUTE(files) FROM (VALUES"),
            "distributor passthrough is called bare (its LUA EMITS is static): {sql}"
        );
        assert!(
            !sql.contains("DISTRIBUTE(files) EMITS"),
            "the statically-defined distributor call MUST NOT carry a query-side EMITS: {sql}"
        );
        assert!(
            sql.contains("AS shards(shard_key, files) GROUP BY shard_key"),
            "the GROUP BY shard_key fan-out must live in the distributor subquery: {sql}"
        );
        assert!(
            sql.contains(&format!(
                "SELECT SCAN('{}",
                spec.to_common_json().replace('\'', "''")
            )),
            "the outer scalar scan splices the common blob as its first-arg literal: {sql}"
        );
        assert!(
            sql.contains(", files) EMITS ("),
            "the outer scalar scan reads the bare distributed files column, not a literal: {sql}"
        );
        // The common blob (which carries table_root) appears exactly once: in the
        // outer scalar's first argument, never repeated per shard in the distributor.
        assert_eq!(
            sql.matches("s3://warehouse/db/table").count(),
            1,
            "common blob must be spliced exactly once, not per shard: {sql}"
        );
    }

    /// A single-shard plan short-circuits the distributor entirely: a from-less scalar
    /// `LAKEHOUSE_SCAN('{common}', '{files}')` call on literals (no distributor, no
    /// inner `GROUP BY`, no `VALUES` driving relation).
    #[test]
    fn single_shard_short_circuits_distributor_fromless() {
        let spec = delete_spec_template();
        let shards = vec![vec![FileEntry::new("data/part-0.parquet", 1000)]];
        let emits = r#""ID" DECIMAL(20,0)"#;
        let sql = build_fan_out_inner(&spec, &shards, emits, "SCAN", "DISTRIBUTE");

        assert!(
            sql.starts_with("SELECT SCAN("),
            "from-less scalar call: {sql}"
        );
        assert!(
            !sql.contains("DISTRIBUTE"),
            "no distributor for one shard: {sql}"
        );
        assert!(
            !sql.contains("GROUP BY shard_key"),
            "no shard_key grouping for one shard: {sql}"
        );
        assert!(!sql.contains("VALUES"), "no driving VALUES relation: {sql}");
        let files_literal = sql_string_literal(&shard_files_json(&shards[0]));
        assert!(
            sql.contains(&format!(", {files_literal}) EMITS (")),
            "the single shard's files must be spliced as a literal: {sql}"
        );
    }

    // ---------------------------------------------------------------------------
    // shard_count — cap/clamp boundary tests
    // ---------------------------------------------------------------------------

    /// Scenario: Shard count oversubscribes the cluster and is capped at 300.
    /// 10 nodes × 50 factor = 500, capped to 300.
    #[test]
    fn shard_count_oversubscribes_and_caps_at_300() {
        // 10 × 50 = 500 > 300 files; cap at 300.
        assert_eq!(shard_count(10, 50, 500), 300, "must be capped at 300");
        // 10 × 50 = 500 but only 350 files — still capped at 300 (min(350, 300)=300).
        assert_eq!(
            shard_count(10, 50, 350),
            300,
            "must be capped at min(files,300)=300"
        );
        // Exact cap: 1 × 300 = 300, 1000 files — stays 300.
        assert_eq!(shard_count(1, 300, 1000), 300, "exactly 300 must stay 300");
        // 1 × 301 = 301 > 300; capped at 300.
        assert_eq!(shard_count(1, 301, 1000), 300, "301 must be capped at 300");
    }

    /// Scenario: Fewer files than G produces one shard per file with no empty shards.
    /// node_count × parallelism_factor > file_count => clamp to file_count.
    #[test]
    fn shard_count_clamped_to_file_count_no_empty_shards() {
        // 10 × 8 = 80 but only 3 files; clamp to 3.
        assert_eq!(shard_count(10, 8, 3), 3, "must clamp to file_count=3");
        // 4 × 8 = 32 but only 5 files; clamp to 5.
        assert_eq!(shard_count(4, 8, 5), 5, "must clamp to file_count=5");
        // 1 × 1 = 1, file_count=1; stays 1.
        assert_eq!(shard_count(1, 1, 1), 1, "single file single shard");
        // Minimum clamp: 0 × 8 = 0, clamp to min(1, …) = 1.
        assert_eq!(shard_count(0, 8, 100), 1, "zero product must clamp to 1");
        // parallelism_factor=0: 5 × 0 = 0, clamp to 1.
        assert_eq!(shard_count(5, 0, 100), 1, "zero factor must clamp to 1");
    }

    /// Pushdown carries the table root ONCE in the common blob and per-shard file
    /// sizes travel into the shard payloads (verification scenario, CHANGED).
    #[test]
    fn pushdown_carries_table_root_and_sizes_in_common_and_shards() {
        let root = "s3://warehouse/db/events";
        let files = vec![
            (format!("{root}/part-00000.parquet"), 1024u64),
            (format!("{root}/part-00001.parquet"), 2048u64),
        ];
        // Two nodes → two shards (one file each) so a genuine fan-out is emitted.
        let sql = build_row_sql_with_root(
            files,
            root,
            vec!["ID".into()],
            vec!["DECIMAL(20,0)".into()],
            2,
        );

        // The table root is carried in the shard-invariant common blob.
        let common = common_arg_literal(&sql);
        assert!(
            common.contains(&format!(r#""table_root":"{root}""#)),
            "common blob must carry table_root once: {common}"
        );

        // Each per-shard payload carries its file's byte size as a [path,size] tuple.
        assert!(
            sql.contains(r#"[["part-00000.parquet",1024]]"#),
            "shard payload must carry relative path + size for file 0: {sql}"
        );
        assert!(
            sql.contains(r#"[["part-00001.parquet",2048]]"#),
            "shard payload must carry relative path + size for file 1: {sql}"
        );
    }

    /// The table root is stripped from every under-root path and appears EXACTLY
    /// ONCE (in the common literal), NEVER in a per-shard VALUES literal (NEW).
    #[test]
    fn table_root_stripped_from_under_root_paths_and_carried_once() {
        let root = "s3://warehouse/db/events";
        let files = vec![
            (format!("{root}/part-00000.parquet"), 1024u64),
            (format!("{root}/part-00001.parquet"), 2048u64),
        ];
        let sql = build_row_sql_with_root(
            files,
            root,
            vec!["ID".into()],
            vec!["DECIMAL(20,0)".into()],
            2,
        );

        // The root string occurs exactly once in the whole statement: in the common
        // blob's table_root. Stripped relative paths never repeat the prefix.
        assert_eq!(
            sql.matches(root).count(),
            1,
            "table root must appear exactly once (common blob only), never per shard: {sql}"
        );
        // That single occurrence lives in the common literal.
        assert!(
            common_arg_literal(&sql).contains(root),
            "the sole table-root occurrence must be in the common blob: {sql}"
        );
        // The per-shard VALUES section (everything after the common literal) carries
        // only relative paths.
        assert!(
            sql.contains("part-00000.parquet") && sql.contains("part-00001.parquet"),
            "shards must carry the relative file names: {sql}"
        );
    }

    /// A data-file path NOT under the table root is carried as a full absolute URI
    /// (NEW).
    #[test]
    fn path_not_under_root_stays_absolute() {
        let root = "s3://warehouse/db/events";
        let outside = "s3://other-bucket/external/f.parquet";
        let files = vec![
            (format!("{root}/part-00000.parquet"), 1024u64),
            (outside.to_string(), 2048u64),
        ];
        let sql = build_row_sql_with_root(
            files,
            root,
            vec!["ID".into()],
            vec!["DECIMAL(20,0)".into()],
            2,
        );

        // The under-root file is emitted relative.
        assert!(
            sql.contains(r#"["part-00000.parquet",1024]"#),
            "under-root path must be relativized: {sql}"
        );
        // The not-under-root file keeps its full absolute URI, with its size.
        assert!(
            sql.contains(&format!(r#"["{outside}",2048]"#)),
            "path outside the table root must stay absolute: {sql}"
        );
        // The table root is still carried exactly once (the absolute outside path
        // does not contain the root prefix).
        assert_eq!(
            sql.matches(root).count(),
            1,
            "table root must appear exactly once even with an out-of-root file: {sql}"
        );
    }

    /// Multi-shard fan-out carries the root once in the common literal and each
    /// per-shard literal is a `[[path,size],...]` tuple array (CHANGED).
    #[test]
    fn fan_out_carries_root_once_and_path_size_tuples_per_shard() {
        let root = "s3://warehouse/db/events";
        let files = vec![
            (format!("{root}/part-00000.parquet"), 1024u64),
            (format!("{root}/part-00001.parquet"), 2048u64),
        ];
        let sql = build_row_sql_with_root(
            files,
            root,
            vec!["ID".into()],
            vec!["DECIMAL(20,0)".into()],
            2,
        );

        // Fan-out shape: GROUP BY shard_key over a VALUES table, never IPROC().
        assert!(
            !sql.contains("IPROC()"),
            "fan-out must not use IPROC(): {sql}"
        );
        assert!(
            sql.contains("GROUP BY shard_key") && sql.contains("AS shards(shard_key, files)"),
            "fan-out must GROUP BY shard_key over the VALUES table: {sql}"
        );

        // Root carried once (common blob), not repeated per shard.
        assert_eq!(
            sql.matches(root).count(),
            1,
            "root must be serialized once in the common blob: {sql}"
        );

        // Each per-shard files literal is a JSON array of [path,size] 2-tuples.
        assert!(
            sql.contains(r#"[["part-00000.parquet",1024]]"#)
                && sql.contains(r#"[["part-00001.parquet",2048]]"#),
            "each shard literal must be a [[path,size],...] tuple array: {sql}"
        );
    }

    // ---------------------------------------------------------------------------
    // Scenario: Pushdown resolves the file list once and builds a scan-driving query
    // ---------------------------------------------------------------------------

    /// Pure SQL-building part of the pushdown scenario.
    /// The file list comes from a fixture (no catalog I/O).
    #[test]
    fn pushdown_resolves_files_once_builds_scan_sql() {
        let files = vec![
            "s3://warehouse/db/events/part-00000.parquet".into(),
            "s3://warehouse/db/events/part-00001.parquet".into(),
        ];
        let sql = build_sql_for_fixture(
            files.clone(),
            vec!["ID".into(), "NAME".into()],
            vec!["DECIMAL(20,0)".into(), "VARCHAR(2000000)".into()],
            None,
            None,
        );

        // The generated SQL must invoke the scan UDF with the spec embedded.
        assert!(
            sql.contains(SCAN_UDF_NAME),
            "SQL must reference the scan UDF: {sql}"
        );
        // The spec JSON (embedded as a SQL literal) contains the file path.
        assert!(
            sql.contains("part-00000.parquet"),
            "SQL must carry assigned files: {sql}"
        );
        assert!(
            sql.contains("part-00001.parquet"),
            "SQL must carry both files: {sql}"
        );
        // Must be the outer ungrouped scalar scan itself (no SELECT * wrapper).
        assert!(
            sql.starts_with(&format!("SELECT {SCAN_UDF_NAME}("))
                && !sql.contains("SELECT * FROM ("),
            "must be a real scalar scan-driving query, no materializing wrapper: {sql}"
        );
    }

    // ---------------------------------------------------------------------------
    // Scenario: Projection is pushed into the scan-driving query
    // ---------------------------------------------------------------------------

    #[test]
    fn projection_carried_in_common_literal_and_emits() {
        let sql = build_sql_for_fixture(
            vec!["s3://warehouse/f.parquet".into()],
            vec!["A".into(), "B".into()],
            vec!["DECIMAL(10,0)".into(), "VARCHAR(2000000)".into()],
            None,
            None,
        );

        // EMITS clause must list exactly the projected columns in order.
        assert!(
            sql.contains("\"A\" DECIMAL(10,0)"),
            "EMITS must carry col A: {sql}"
        );
        assert!(
            sql.contains("\"B\" VARCHAR(2000000)"),
            "EMITS must carry col B: {sql}"
        );

        // The projection lives in the common (arg 0) blob, not the per-shard files arg.
        let common = common_arg_literal(&sql);
        assert!(
            common.contains(r#""projection":["A","B"]"#),
            "common arg must carry the projection in order: {common}"
        );
        // The per-shard files arg must not carry projection metadata.
        assert!(
            !sql.contains(r#""files""#),
            "no ScanSpec files key must appear (files travel as a bare JSON array): {sql}"
        );
    }

    // ---------------------------------------------------------------------------
    // Scenario: Filter predicate is pushed into the scan spec (translatable) or
    // omitted (untranslatable) — never mistranslated.
    // ---------------------------------------------------------------------------

    #[test]
    fn pushdown_translates_or_omits_predicate() {
        // Translatable predicate: column > literal.
        let translatable = serde_json::json!({
            "type": "predicate_greater",
            "left": {"type": "column", "name": "age"},
            "right": {"type": "literal_exactnumeric", "value": 18}
        });
        let filter_rendered = render_df_filter_safe(&translatable);
        assert!(
            filter_rendered.is_some(),
            "translatable predicate must produce a filter string"
        );
        let filter_str = filter_rendered.unwrap();
        assert!(
            filter_str.contains(">"),
            "filter must include > operator: {filter_str}"
        );
        assert!(
            filter_str.contains("AGE") || filter_str.contains("\"AGE\""),
            "filter must reference the column: {filter_str}"
        );

        // Untranslatable predicate (e.g., an aggregate or unknown function):
        // render_df_filter_safe returns None → omitted from spec.
        let untranslatable = serde_json::json!({"type": "fn_custom_agg", "args": []});
        let omitted = render_df_filter_safe(&untranslatable);
        assert!(
            omitted.is_none(),
            "untranslatable predicate must be omitted (None), not mistranslated"
        );

        // Confirm omitting the filter still produces valid SQL (correctness backstop).
        let sql_no_filter = build_sql_for_fixture(
            vec!["s3://warehouse/f.parquet".into()],
            vec!["AGE".into()],
            vec!["DECIMAL(20,0)".into()],
            None, // omitted
            None,
        );
        assert!(
            sql_no_filter.contains(SCAN_UDF_NAME),
            "SQL must still be valid when filter is omitted"
        );

        // Confirm carrying the filter includes it in the spec JSON.
        let sql_with_filter = build_sql_for_fixture(
            vec!["s3://warehouse/f.parquet".into()],
            vec!["AGE".into()],
            vec!["DECIMAL(20,0)".into()],
            Some(filter_str),
            None,
        );
        assert!(
            sql_with_filter.contains(">"),
            "filter must survive into the spec literal: {sql_with_filter}"
        );
    }

    // ---------------------------------------------------------------------------
    // Scenario: LIMIT is pushed into the scan spec; also appears at Exasol level.
    // ---------------------------------------------------------------------------

    #[test]
    fn row_scan_limit_in_common_arg() {
        let sql = build_sql_for_fixture(
            vec!["s3://warehouse/f.parquet".into()],
            vec!["ID".into()],
            vec!["DECIMAL(20,0)".into()],
            None,
            Some(42),
        );

        // The outer SQL must contain LIMIT (Exasol-level backstop).
        assert!(
            sql.contains("LIMIT 42"),
            "outer SQL must carry LIMIT for correctness backstop: {sql}"
        );

        // For a row scan the LIMIT is retained in the common (arg 0) blob.
        let common = common_arg_literal(&sql);
        assert!(
            common.contains(r#""limit":42"#),
            "row-scan common arg must carry limit=42: {common}"
        );
    }

    // ---------------------------------------------------------------------------
    // Pre-existing helpers tests (unchanged)
    // ---------------------------------------------------------------------------

    #[test]
    fn limit_extracted_from_pushdown_request() {
        let req = serde_json::json!({"numElements": 42});
        assert_eq!(extract_limit(&req), None); // not nested under "limit"

        let req2 = serde_json::json!({"limit": {"numElements": 42}});
        assert_eq!(extract_limit(&req2), Some(42));
    }

    #[test]
    fn sql_string_literal_escapes_quotes() {
        let s = "it's a test";
        let lit = sql_string_literal(s);
        assert_eq!(lit, "'it''s a test'");
    }

    // ---------------------------------------------------------------------------
    // extract_projection — row-scan fallback must be duplicate-free
    // ---------------------------------------------------------------------------

    /// A select list mixing an untranslatable scalar and COUNT(*) must NOT emit
    /// repeated `first_col_name` columns (which Exasol rejects as duplicate EMITS).
    /// It falls back to the full, deduplicated base-table column set.
    #[test]
    fn extract_projection_fallback_is_duplicate_free() {
        let request = serde_json::json!({
            "involvedTables": [{
                "name": "EVENTS",
                "columns": [
                    {"name": "id", "dataType": {"type": "decimal", "precision": 20, "scale": 0}},
                    {"name": "name", "dataType": {"type": "varchar", "size": 2000000}},
                ],
            }],
        });
        // Untranslatable scalar (unknown function) + COUNT(*) aggregate — both items
        // would otherwise hit the first-column fallback arms.
        let pushdown_req = serde_json::json!({
            "selectList": [
                {"type": "function_scalar", "name": "TOTALLY_UNKNOWN_FN", "arguments": [
                    {"type": "column", "name": "id"}
                ]},
                {"type": "function_aggregate", "name": "COUNT", "arguments": [], "distinct": false},
            ],
            "selectListDataTypes": [
                {"type": "decimal", "precision": 20, "scale": 0},
                {"type": "decimal", "precision": 20, "scale": 0},
            ],
        });

        let (names, types) = extract_projection(&request, &pushdown_req).unwrap();

        let unique: std::collections::HashSet<&str> = names.iter().map(|p| p.emit_name()).collect();
        assert_eq!(
            unique.len(),
            names.len(),
            "projection must be duplicate-free, got: {names:?}"
        );
        assert_eq!(
            names,
            vec!["ID", "NAME"],
            "fallback must project the full base-table column set"
        );
        assert_eq!(
            names.len(),
            types.len(),
            "names and types must stay aligned"
        );
    }

    // ---------------------------------------------------------------------------
    // detect_aggregates — plan translation + fallback
    // ---------------------------------------------------------------------------

    // -----------------------------------------------------------------------
    // Expression-argument aggregates (Task 2.1 / 2.3)
    // -----------------------------------------------------------------------

    /// An aggregate select-list translates to a ScanSpec carrying
    /// the aggregate plan (kind+column) plus any pushed-down filter.
    #[test]
    fn aggregate_query_builds_partial_agg_spec() {
        // Build a spec_template as handle_pushdown would.
        let spec_template = ScanSpec {
            common: CommonScanSpec {
                projection: vec!["AMOUNT".into()],
                filter: Some("(\"REGION\" = 'EU')".into()),
                aggregates: Some(vec![
                    AggregatePlan {
                        kind: AggKind::Sum,
                        column: Some("AMOUNT".into()),
                        arg_expr: None,
                    },
                    AggregatePlan {
                        kind: AggKind::Count,
                        column: None,
                        arg_expr: None,
                    },
                ]),
                storage: sample_storage(),
                ..Default::default()
            },
            files: vec![],
        };

        // Build single-shard SQL and decode the embedded spec literal.
        let shards = vec![vec![("s3://warehouse/f.parquet".to_string(), 1u64)]];
        let col_types = vec![("AMOUNT".to_string(), "DOUBLE PRECISION".to_string())];
        let sql = build_scan_driving_sql(
            &spec_template,
            &shards,
            &["AMOUNT".into()],
            &["DOUBLE PRECISION".to_string()],
            None,
            &col_types,
            &[],
            SCAN_UDF_NAME,
            DISTRIBUTE_FILES_UDF_NAME,
        );

        // The spec JSON is embedded in the SQL literal; extract and parse it.
        // It lives between the first `'` and the matching unescaped `'` after the JSON.
        // Simpler: deserialize directly from the template (which is what gets embedded).
        let spec_json = {
            // Reconstruct the shard spec as the builder would.
            let mut s = spec_template.clone();
            s.files = vec![FileEntry::new("s3://warehouse/f.parquet", 1)];
            s.to_json()
        };
        let parsed = ScanSpec::from_json(&spec_json).expect("spec must parse");

        // The aggregate plan must be present with the right kinds and columns.
        let plans = parsed
            .common
            .aggregates
            .expect("aggregates must be in the spec");
        assert_eq!(plans.len(), 2);
        assert_eq!(plans[0].kind, AggKind::Sum);
        assert_eq!(plans[0].column.as_deref(), Some("AMOUNT"));
        assert_eq!(plans[1].kind, AggKind::Count);
        assert!(plans[1].column.is_none());

        // The filter must also be present.
        assert!(
            parsed.common.filter.is_some(),
            "filter must be carried in aggregate spec"
        );

        // The SQL must reference the UDF.
        assert!(sql.contains(SCAN_UDF_NAME));
    }

    // ---------------------------------------------------------------------------
    // Fan-out SQL shape — multi-shard GROUP BY shard_key, single-shard equivalence
    // ---------------------------------------------------------------------------

    /// Multi-shard fan-out serializes the shard-INVARIANT common blob EXACTLY ONCE
    /// (as the UDF's first argument literal) and carries only the per-shard files
    /// list in each `VALUES` row — no credential/tuning payload repeats per shard.
    #[test]
    fn fan_out_serializes_common_once_files_per_shard() {
        let files = vec![
            "s3://warehouse/shard0/part-000.parquet".into(),
            "s3://warehouse/shard1/part-001.parquet".into(),
            "s3://warehouse/shard2/part-002.parquet".into(),
        ];
        // cluster_nodes=3 forces 3 shards (one file each).
        let sql = build_sql_for_fixture_n(
            files,
            vec!["ID".into()],
            vec!["DECIMAL(20,0)".into()],
            None,
            None,
            3,
        );

        // Must use shard_key GROUP BY for the fan-out, NOT IPROC().
        assert!(
            !sql.contains("IPROC()"),
            "multi-shard SQL must NOT contain IPROC(): {sql}"
        );
        assert!(
            sql.contains("GROUP BY shard_key"),
            "multi-shard SQL must GROUP BY shard_key: {sql}"
        );

        // The VALUES table exposes the per-shard files column (arg 1), not a full spec.
        assert!(
            sql.contains("AS shards(shard_key, files)"),
            "fan-out must alias the VALUES table as shards(shard_key, files): {sql}"
        );
        // The UDF is called with two args: the common literal, then the files column.
        assert!(
            sql.contains(&format!("{SCAN_UDF_NAME}(")),
            "multi-shard SQL must invoke the scan UDF: {sql}"
        );
        assert!(
            sql.contains(", files) EMITS ("),
            "UDF must take the per-shard files column as its second argument: {sql}"
        );

        // The shard-invariant common blob must appear EXACTLY ONCE. The storage
        // endpoint and the tuning knobs live only in the common blob, so counting
        // them proves the credential/tuning payload is not repeated per shard.
        assert_eq!(
            sql.matches("http://minio:9000").count(),
            1,
            "storage endpoint (common blob) must appear exactly once, not per shard: {sql}"
        );
        assert_eq!(
            sql.matches("memory_pool_fraction").count(),
            1,
            "tuning payload (common blob) must appear exactly once, not per shard: {sql}"
        );

        // Each shard's file appears EXACTLY ONCE, in its own VALUES row.
        for file in ["part-000.parquet", "part-001.parquet", "part-002.parquet"] {
            assert_eq!(
                sql.matches(file).count(),
                1,
                "file {file} must appear exactly once (in one VALUES row): {sql}"
            );
        }

        // Exactly 3 VALUES entries (one files literal per shard).
        let values_start = sql.find("VALUES").expect("must have VALUES");
        let group_by_start = sql.find("GROUP BY").expect("must have GROUP BY");
        let values_section = &sql[values_start..group_by_start];
        let entry_count = values_section.matches("),(").count() + 1;
        assert_eq!(
            entry_count, 3,
            "must have 3 VALUES entries for 3 shards: {values_section}"
        );
    }

    /// The connection-concurrency budget (`s3_max_connections`) is a shard-INVARIANT
    /// tuning field — like `df_threads_per_udf` and `memory_pool_fraction` — so it must
    /// travel in the common blob (the UDF's first argument), serialized exactly once,
    /// never duplicated per shard and never silently dropped from the fan-out SQL.
    #[test]
    fn common_spec_carries_s3_max_connections_exactly_once() {
        let files = vec![
            "s3://warehouse/shard0/part-000.parquet".into(),
            "s3://warehouse/shard1/part-001.parquet".into(),
            "s3://warehouse/shard2/part-002.parquet".into(),
        ];
        // A distinctive, non-default value so it cannot be confused with the
        // built-in default (8) or any other numeric field in the spec.
        let distinctive_s3_max_connections = 37;
        let spec_template = ScanSpec {
            common: CommonScanSpec {
                projection: vec!["ID".into()],
                storage: sample_storage(),
                s3_max_connections: distinctive_s3_max_connections,
                ..Default::default()
            },
            files: vec![],
        };

        // Confirm the value round-trips through the shard-invariant common split
        // that `handle_pushdown` uses to build the fan-out (`ScanSpec::to_common`).
        let common = spec_template.to_common();
        assert_eq!(
            common.s3_max_connections, distinctive_s3_max_connections,
            "s3_max_connections must carry from ScanSpec into CommonScanSpec"
        );

        // cluster_nodes=3 forces 3 shards (one file each) — the same multi-shard
        // fan-out shape `handle_pushdown` builds via `build_scan_driving_sql`.
        let files_with_sizes: Vec<FileEntry> = files
            .into_iter()
            .map(|p: String| FileEntry::new(p, 1))
            .collect();
        let shards = crate::adapter::sharding::partition_files_by_bytes(files_with_sizes, 3);
        let sql = build_scan_driving_sql(
            &spec_template,
            &shards,
            &["ID".into()],
            &["DECIMAL(20,0)".to_string()],
            None,
            &[],
            &[],
            SCAN_UDF_NAME,
            DISTRIBUTE_FILES_UDF_NAME,
        );

        let needle = format!("\"s3_max_connections\":{distinctive_s3_max_connections}");
        assert_eq!(
            sql.matches(&needle).count(),
            1,
            "s3_max_connections must appear exactly once, in the shard-invariant \
             common blob, not per shard and not dropped: {sql}"
        );
    }

    // ---------------------------------------------------------------------------
    // Aggregate merge wrapper SQL — outer SELECT reconstructing partial results
    // ---------------------------------------------------------------------------

    /// Helper: build aggregate scan SQL from a set of aggregate plans.
    /// Uses an empty col_types map — aggregate columns default to DOUBLE PRECISION
    /// (correct for existing tests that use SCORE/AMOUNT as DOUBLE).
    fn build_agg_sql(
        agg_plans: Vec<AggregatePlan>,
        files: Vec<String>,
        cluster_nodes: usize,
    ) -> String {
        let spec_template = ScanSpec {
            common: CommonScanSpec {
                aggregates: Some(agg_plans),
                storage: sample_storage(),
                ..Default::default()
            },
            files: vec![],
        };
        let files_with_sizes: Vec<FileEntry> =
            files.into_iter().map(|p| FileEntry::new(p, 1)).collect();
        let shards =
            crate::adapter::sharding::partition_files_by_bytes(files_with_sizes, cluster_nodes);
        build_scan_driving_sql(
            &spec_template,
            &shards,
            &[],
            &[],
            None,
            &[],
            &[],
            SCAN_UDF_NAME,
            DISTRIBUTE_FILES_UDF_NAME,
        )
    }

    /// Aggregate wrapper merges partials: outer SELECT aggregates per-shard COUNT/SUM/MIN/MAX.
    /// Given COUNT/SUM/MIN/MAX aggregate plan: wrapper contains fan-out AND outer
    /// SUM/MIN/MAX over the partial columns in the right order.
    #[test]
    fn aggregate_wrapper_merges_partials() {
        let plans = vec![
            AggregatePlan {
                kind: AggKind::Count,
                column: None,
                arg_expr: None,
            },
            AggregatePlan {
                kind: AggKind::Sum,
                column: Some("AMOUNT".into()),
                arg_expr: None,
            },
            AggregatePlan {
                kind: AggKind::Min,
                column: Some("TS".into()),
                arg_expr: None,
            },
            AggregatePlan {
                kind: AggKind::Max,
                column: Some("TS".into()),
                arg_expr: None,
            },
        ];

        // Multi-shard: use 2 shards to exercise the fan-out + merge wrapper.
        let files = vec![
            "s3://warehouse/f0.parquet".into(),
            "s3://warehouse/f1.parquet".into(),
        ];
        let sql = build_agg_sql(plans, files, 2);

        // Must contain the shard_key fan-out (NOT IPROC).
        assert!(
            !sql.contains("IPROC()"),
            "aggregate SQL must NOT use IPROC: {sql}"
        );
        assert!(
            sql.contains("GROUP BY"),
            "aggregate SQL must use GROUP BY: {sql}"
        );
        assert!(
            sql.contains("shard_key"),
            "aggregate SQL must use shard_key fan-out: {sql}"
        );

        // Must wrap with outer merge aggregation.
        assert!(
            sql.contains("SUM("),
            "merge wrapper must contain SUM: {sql}"
        );
        assert!(
            sql.contains("MIN("),
            "merge wrapper must contain MIN: {sql}"
        );
        assert!(
            sql.contains("MAX("),
            "merge wrapper must contain MAX: {sql}"
        );

        // Must contain partial column names in the EMITS and in the merge.
        assert!(
            sql.contains("PARTIAL_count_0"),
            "must reference partial count column: {sql}"
        );
        assert!(
            sql.contains("PARTIAL_sum_1"),
            "must reference partial sum column: {sql}"
        );
        assert!(
            sql.contains("PARTIAL_min_2"),
            "must reference partial min column: {sql}"
        );
        assert!(
            sql.contains("PARTIAL_max_3"),
            "must reference partial max column: {sql}"
        );

        // The EMITS clause must declare the partial columns.
        assert!(
            sql.contains("EMITS"),
            "aggregate SQL must have EMITS: {sql}"
        );

        // The outer merge must not be SELECT *.
        assert!(
            !sql.contains("SELECT *"),
            "aggregate wrapper must not use SELECT *: {sql}"
        );
    }

    /// The outer single-group merge SELECT sits DIRECTLY over the scalar scan — no
    /// `SELECT * FROM (...)` between the merge and the scan (decision [5]). The scalar
    /// scan fires once per shard (the distributor emits one row per shard), so one
    /// partial-agg row per shard is produced and the outer SUM/MIN/MAX merge over
    /// those partials equals the single-node aggregate (result-equivalence, [7]).
    #[test]
    fn aggregate_merge_over_scalar_scan_no_wrapper() {
        let plans = vec![
            AggregatePlan {
                kind: AggKind::Count,
                column: None,
                arg_expr: None,
            },
            AggregatePlan {
                kind: AggKind::Sum,
                column: Some("AMOUNT".into()),
                arg_expr: None,
            },
        ];
        // Multi-shard: a genuine distributor fan-out under the merge.
        let sql = build_agg_sql(
            plans,
            vec!["s3://w/f0.parquet".into(), "s3://w/f1.parquet".into()],
            2,
        );

        assert!(
            !sql.contains("SELECT * FROM ("),
            "no materializing wrapper between merge and scan: {sql}"
        );
        // The merge is the outer SELECT; the scalar scan is the subquery it reads.
        assert!(
            sql.starts_with("SELECT ") && sql.contains(&format!("FROM (SELECT {SCAN_UDF_NAME}(")),
            "the outer merge SELECT must read directly from the scalar scan subquery: {sql}"
        );
        // The `GROUP BY shard_key` fan-out lives in the distributor, not the outer merge.
        assert!(
            sql.contains("GROUP BY shard_key"),
            "the fan-out GROUP BY shard_key must live inside the distributor: {sql}"
        );
    }

    /// Single-shard aggregate: the merge SELECT sits directly over a from-less scalar
    /// scan on literals — no distributor, no `SELECT * FROM (...)` wrapper.
    #[test]
    fn aggregate_single_shard_merge_over_fromless_scalar_scan() {
        let plans = vec![AggregatePlan {
            kind: AggKind::Count,
            column: None,
            arg_expr: None,
        }];
        let sql = build_agg_sql(plans, vec!["s3://w/only.parquet".into()], 1);

        assert!(
            !sql.contains("SELECT * FROM ("),
            "single-shard aggregate must not use a materializing wrapper: {sql}"
        );
        assert!(
            !sql.contains("VALUES") && !sql.contains("GROUP BY shard_key"),
            "single-shard aggregate short-circuits the distributor: {sql}"
        );
        assert!(
            sql.contains(&format!("FROM (SELECT {SCAN_UDF_NAME}(")),
            "the merge reads directly from the from-less scalar scan: {sql}"
        );
    }

    /// Single-group merge casts each aggregate to its Exasol-declared result type.
    /// `SELECT COUNT(score)` merges as `SUM("PARTIAL_count_0")` (DECIMAL(31,0)); Exasol
    /// declared DECIMAL(18,0) for the column and strictly validates the adapter's output
    /// type, so the merge item must be `CAST(SUM("PARTIAL_count_0") AS DECIMAL(18,0))`.
    #[test]
    fn single_group_merge_casts_to_declared_type() {
        let plans = vec![AggregatePlan {
            kind: AggKind::CountCol,
            column: Some("SCORE".into()),
            arg_expr: None,
        }];
        let spec_template = ScanSpec {
            common: CommonScanSpec {
                aggregates: Some(plans.clone()),
                storage: sample_storage(),
                ..Default::default()
            },
            files: vec![],
        };
        let shards = vec![vec![("s3://warehouse/f0.parquet".to_string(), 1u64)]];
        let col_types = vec![("SCORE".to_string(), "DECIMAL(18,0)".to_string())];
        let aggregate_types = vec!["DECIMAL(18,0)".to_string()];
        let sql = build_scan_driving_sql(
            &spec_template,
            &shards,
            &[],
            &[],
            None,
            &col_types,
            &aggregate_types,
            SCAN_UDF_NAME,
            DISTRIBUTE_FILES_UDF_NAME,
        );
        assert!(
            sql.contains(r#"CAST(SUM("PARTIAL_count_0") AS DECIMAL(18,0))"#),
            "single-group merge must cast COUNT to declared DECIMAL(18,0): {sql}"
        );
    }

    /// Single-group merge with no declared types emits the bare uncast merge expression.
    #[test]
    fn single_group_merge_uncast_without_declared_types() {
        let plans = vec![AggregatePlan {
            kind: AggKind::CountCol,
            column: Some("SCORE".into()),
            arg_expr: None,
        }];
        let spec_template = ScanSpec {
            common: CommonScanSpec {
                aggregates: Some(plans.clone()),
                storage: sample_storage(),
                ..Default::default()
            },
            files: vec![],
        };
        let shards = vec![vec![("s3://warehouse/f0.parquet".to_string(), 1u64)]];
        let sql = build_scan_driving_sql(
            &spec_template,
            &shards,
            &[],
            &[],
            None,
            &[],
            &[],
            SCAN_UDF_NAME,
            DISTRIBUTE_FILES_UDF_NAME,
        );
        assert!(
            sql.contains(r#"SUM("PARTIAL_count_0")"#) && !sql.contains("CAST(SUM"),
            "single-group merge without declared types must be uncast: {sql}"
        );
    }

    /// AVG wrapper divides merged sum by count with NULLIF(cnt, 0) guard.
    /// Given AVG plan: wrapper computes SUM(partial_avg_sum) / NULLIF(SUM(partial_avg_cnt),0).
    #[test]
    fn avg_wrapper_divides_sum_by_count_guarded() {
        let plans = vec![AggregatePlan {
            kind: AggKind::Avg,
            column: Some("SCORE".into()),
            arg_expr: None,
        }];
        let files = vec![
            "s3://warehouse/f0.parquet".into(),
            "s3://warehouse/f1.parquet".into(),
        ];
        let sql = build_agg_sql(plans, files, 2);

        // Must contain NULLIF guard for zero-count protection.
        assert!(
            sql.contains("NULLIF"),
            "AVG wrapper must contain NULLIF zero-guard: {sql}"
        );

        // Must divide: the / operator must appear in the outer merge context.
        assert!(
            sql.contains(" / "),
            "AVG wrapper must divide sum by count: {sql}"
        );

        // Must reference the AVG sum and count partial columns.
        assert!(
            sql.contains("PARTIAL_avg_sum_0"),
            "must reference partial avg sum: {sql}"
        );
        assert!(
            sql.contains("PARTIAL_avg_cnt_0"),
            "must reference partial avg count: {sql}"
        );

        // Must use SUM() for both the sum and count parts.
        let sum_count = sql.matches("SUM(").count();
        assert!(
            sum_count >= 2,
            "AVG wrapper must SUM both partial_avg_sum and partial_avg_cnt: {sql}"
        );

        // Must contain NULLIF(..., 0).
        assert!(
            sql.contains("NULLIF(") && sql.contains(", 0)"),
            "AVG wrapper NULLIF guard must guard against zero: {sql}"
        );
    }

    /// Single-shard aggregate path produces a correct merge wrapper.
    #[test]
    fn single_shard_aggregate_still_uses_merge_wrapper() {
        let plans = vec![
            AggregatePlan {
                kind: AggKind::Count,
                column: None,
                arg_expr: None,
            },
            AggregatePlan {
                kind: AggKind::Avg,
                column: Some("SCORE".into()),
                arg_expr: None,
            },
        ];
        let files = vec!["s3://warehouse/f0.parquet".into()];
        let sql = build_agg_sql(plans, files, 1);

        // Even single-shard aggregate must have an outer merge.
        assert!(
            sql.contains("SUM("),
            "single-shard aggregate must have SUM merge: {sql}"
        );
        assert!(
            sql.contains("NULLIF"),
            "single-shard AVG must have NULLIF guard: {sql}"
        );
        assert!(
            sql.contains("PARTIAL_count_0"),
            "single-shard must reference partial count: {sql}"
        );
        assert!(
            sql.contains("PARTIAL_avg_sum_1"),
            "single-shard must reference partial avg sum: {sql}"
        );
        assert!(
            sql.contains("PARTIAL_avg_cnt_1"),
            "single-shard must reference partial avg count: {sql}"
        );
    }

    // ---------------------------------------------------------------------------
    // Single-group COUNT(DISTINCT) — DISTINCT row-scan fan-out wrapper SQL
    // (replaces the removed LISTAGG/merge-UDF SQL shape; Tasks 5.3 / 5.6)
    // ---------------------------------------------------------------------------

    /// A `base_spec` matching the real caller contract (`handle_pushdown`): no
    /// projection/aggregates/limit/order-by, `distinct` false. Only `files` varies
    /// per shard; `build_count_distinct_scan_sql` derives each per-distinct fan-out
    /// (and any shared ordinary partial-aggregate scan) from this template.
    fn count_distinct_base_spec() -> ScanSpec {
        ScanSpec {
            common: CommonScanSpec {
                storage: sample_storage(),
                ..Default::default()
            },
            files: vec![],
        }
    }

    /// Scenario: Case 1 (single-group `COUNT(DISTINCT col)`, nothing else) wraps
    /// its DISTINCT row-scan fan-out in a plain, native `COUNT(DISTINCT "V")` —
    /// replacing the removed `'[' || LISTAGG(...) || ']'` merge-UDF SQL shape.
    /// Both UDF invocations (scan + distributor) are schema-qualified from the
    /// names passed in; there is no third (merge) UDF name to qualify anymore.
    #[test]
    fn count_distinct_wrapper_uses_native_count_distinct() {
        let base_spec = count_distinct_base_spec();
        let items = vec![SingleGroupItem::Distinct(DistinctCount {
            column: Some("L_SHIPMODE".into()),
            arg_expr: None,
        })];
        let col_types = vec![("L_SHIPMODE".to_string(), "VARCHAR(25)".to_string())];
        // Two shards → a genuine fan-out, not the single-shard short-circuit.
        let shards = vec![
            vec![("s3://warehouse/a.parquet".to_string(), 1u64)],
            vec![("s3://warehouse/b.parquet".to_string(), 1u64)],
        ];
        let sql = build_count_distinct_scan_sql(
            &base_spec,
            &shards,
            &items,
            &col_types,
            None,
            r#""VS_SCHEMA".LAKEHOUSE_SCAN"#,
            r#""VS_SCHEMA".LAKEHOUSE_DISTRIBUTE_FILES"#,
        );

        assert!(
            sql.starts_with(r#"SELECT COUNT(DISTINCT "V") FROM ("#),
            "Case 1 must be a plain native COUNT(DISTINCT) over one fan-out: {sql}"
        );
        assert!(
            sql.contains(r#""V" VARCHAR(25)"#),
            "the fan-out's single emitted column must be named V, with its native \
             (non-JSON) Exasol type: {sql}"
        );
        assert!(
            sql.contains(r#"\"L_SHIPMODE\" IS NOT NULL"#),
            "the fan-out must exclude NULLs from the distinct argument: {sql}"
        );
        assert!(
            sql.contains(r#""VS_SCHEMA".LAKEHOUSE_SCAN"#)
                && sql.contains(r#""VS_SCHEMA".LAKEHOUSE_DISTRIBUTE_FILES"#),
            "both the scan and distributor UDFs must be schema-qualified from the \
             names passed in: {sql}"
        );
        assert!(
            !sql.to_uppercase().contains("LISTAGG") && !sql.contains("DISTINCT_MERGE"),
            "the removed per-shard JSON-array LISTAGG merge-UDF shape must never \
             appear: {sql}"
        );
    }

    // The former `count_distinct_expression_arg_declares_varchar_value_type` test was
    // removed with the VARCHAR fan-out arm it exercised: a lone `COUNT(DISTINCT <expr>)`
    // no longer fans out at all (`is_lone_count_distinct` now requires a bare-column
    // argument). An expression-argument distinct declines to the qualified single-table
    // wrapper, where Exasol evaluates the expression and DISTINCT natively over
    // exact-typed base columns — covered by
    // `single_group_agg::lone_expression_count_distinct_declines_fan_out_to_wrapper`.

    // The former `multiple_count_distinct_columns_get_independent_fan_outs` (Case 2
    // asserting the `FROM DUAL` scalar-subquery shape) was removed with the Case 2/3
    // fan-out composition: only the lone-distinct Case 1 shape reaches
    // `build_count_distinct_scan_sql`. Case 2/3 now DECLINES the fan-out and routes to
    // the qualified single-table wrapper — asserted below.

    /// Scenario (task 6.5): a Case 2/3 select list (more than one `COUNT(DISTINCT)`,
    /// or a distinct mixed with an ordinary aggregate) is NOT dispatched to the
    /// distinct fan-out (`is_lone_count_distinct` is false) and instead routes to the
    /// shared qualified single-table wrapper. The wrapper renders every aggregate —
    /// each `COUNT(DISTINCT)` spliced VERBATIM — over a materialized raw scan narrowed
    /// to only the referenced columns (issue #160) and aliased `"LHS_T0"`. It is NOT a
    /// distinct fan-out (`COUNT(DISTINCT "V")`), NOT a bare row scan (`SELECT * FROM`),
    /// NOT a per-distinct SELECT-list scalar subquery (`(SELECT COUNT(DISTINCT "V")` —
    /// the blocked design, `sqlCode 04000` "emitting function in expression"), and NOT
    /// the removed `LISTAGG`/merge-UDF shape.
    #[test]
    fn multi_count_distinct_declines_to_qualified_wrapper() {
        use super::super::joins::{
            build_qualified_single_table_fallback_sql, referenced_column_projection,
        };
        use super::super::single_group_agg::{has_distinct, is_lone_count_distinct};

        // A column node carrying `tableName` so the wrapper alias-qualifies it.
        let cdist = |col: &str| {
            serde_json::json!({
                "type": "function_aggregate", "name": "COUNT", "distinct": true,
                "arguments": [{"type": "column", "name": col, "tableName": "T"}],
            })
        };
        // Case 2: two independent `COUNT(DISTINCT ...)` columns.
        let pushdown_req = serde_json::json!({
            "selectList": [cdist("CATEGORY"), cdist("REGION")],
            "selectListDataTypes": [
                {"type": "decimal", "precision": 18, "scale": 0},
                {"type": "decimal", "precision": 18, "scale": 0},
            ],
        });

        // Dispatch: a Case 2 shape is a distinct request that is NOT a lone distinct,
        // so the `mod.rs` branch declines the fan-out and takes the wrapper guard.
        let items = super::super::detect_aggregates(&pushdown_req)
            .expect("two COUNT(DISTINCT) items are detected as distinct fan-out descriptors");
        assert!(
            has_distinct(&items),
            "a Case 2 select list still carries distinct items"
        );
        assert!(
            !is_lone_count_distinct(&items),
            "more than one COUNT(DISTINCT) is NOT a lone distinct — it must decline the \
             fan-out and route to the qualified single-table wrapper"
        );

        // Build the wrapper exactly as the `mod.rs` Case 2/3 guard does: narrow the
        // inner scan to only the referenced columns, then render the aggregates over it.
        let all_cols = vec![
            ("CATEGORY".to_string(), "VARCHAR(25)".to_string()),
            ("REGION".to_string(), "VARCHAR(25)".to_string()),
            ("IRRELEVANT_COL".to_string(), "DECIMAL(20,0)".to_string()),
        ];
        let (proj, proj_types) = referenced_column_projection(&pushdown_req, &all_cols);
        let base = count_distinct_base_spec();
        let fan_out_spec = ScanSpec {
            common: CommonScanSpec {
                projection: proj,
                emit_exa_types: proj_types,
                ..base.common
            },
            files: base.files,
        };
        let request = serde_json::json!({"involvedTables": [{"name": "T"}]});
        let sql = build_qualified_single_table_fallback_sql(
            &request,
            &pushdown_req,
            &fan_out_spec,
            &[vec![("s3://warehouse/f0.parquet".to_string(), 1u64)]],
            SCAN_UDF_NAME,
            DISTRIBUTE_FILES_UDF_NAME,
        )
        .expect("Case 2/3 qualified wrapper must build");

        assert!(
            sql.contains(r#"AS "LHS_T0""#) && sql.contains("FROM ("),
            "Case 2/3 must be the qualified single-table wrapper (one aliased raw \
             fan-out subquery): {sql}"
        );
        assert_eq!(
            sql.matches("COUNT(DISTINCT").count(),
            2,
            "both COUNT(DISTINCT) aggregates must be spliced verbatim into the outer \
             wrapper — one per select item: {sql}"
        );
        assert!(
            !sql.contains(r#"COUNT(DISTINCT "V")"#),
            "Case 2/3 must NOT be a distinct row-scan fan-out (the Case 1 shape): {sql}"
        );
        assert!(
            !sql.contains("(SELECT COUNT(DISTINCT"),
            "Case 2/3 must NOT compose per-distinct SELECT-list scalar subqueries (the \
             blocked design, sqlCode 04000 'emitting function in expression'): {sql}"
        );
        assert!(
            !sql.starts_with("SELECT * FROM"),
            "Case 2/3 must NOT be a bare row scan (the 04000 column-count mismatch): {sql}"
        );
        assert!(
            !sql.to_uppercase().contains("LISTAGG") && !sql.contains("DISTINCT_MERGE"),
            "the removed per-shard JSON-array LISTAGG merge-UDF shape must never \
             appear: {sql}"
        );
        assert!(
            !sql.contains("IRRELEVANT_COL"),
            "issue #160: the narrowed inner scan must project only referenced columns \
             (CATEGORY, REGION), never the full base-table schema: {sql}"
        );
    }

    /// Scenario (plan-review finding): LIMIT/OFFSET/ORDER BY must never leak into
    /// a distinct fan-out — the fan-out builder unconditionally strips them from
    /// `base_spec` regardless of what a (possibly non-conforming) caller passes,
    /// so a leaked per-shard LIMIT can never truncate a shard's local distinct set
    /// into a wrong count. Covers Case 1 (a lone single-group `COUNT(DISTINCT)`), the
    /// only shape that fans out — Case 2/3 declines to the qualified single-table
    /// wrapper. The request-level `limit` argument (the outer `LIMIT` on
    /// `SELECT COUNT(DISTINCT c) FROM t LIMIT 1`) lands ONLY on the outer wrapper.
    #[test]
    fn count_distinct_fan_out_omits_limit_offset_order_by() {
        // A deliberately non-conforming base_spec: real callers (`handle_pushdown`)
        // always pass `limit: None, order_by: []`, but the fan-out builder must
        // strip these unconditionally rather than relying on the caller's contract.
        let mut poisoned_base_spec = count_distinct_base_spec();
        poisoned_base_spec.common.limit = Some(999);
        poisoned_base_spec.common.order_by = vec![SortKey {
            column: "POISON_KEY".into(),
            ascending: true,
            nulls_last: false,
        }];
        let col_types = vec![
            ("A".to_string(), "DECIMAL(20,0)".to_string()),
            ("B".to_string(), "DECIMAL(20,0)".to_string()),
        ];
        let shards = vec![
            vec![("s3://warehouse/a.parquet".to_string(), 1u64)],
            vec![("s3://warehouse/b.parquet".to_string(), 1u64)],
        ];

        let assert_only_outer_limit_no_order_by = |sql: &str, case: &str| {
            assert!(
                !sql.contains("POISON_KEY") && !sql.contains("999"),
                "{case}: a poisoned base_spec's LIMIT/ORDER BY must never leak into \
                 any distinct fan-out: {sql}"
            );
            assert_eq!(
                sql.matches("LIMIT").count(),
                1,
                "{case}: exactly one literal LIMIT (the outer wrapper's) may \
                 appear — none may leak into a per-shard fan-out subquery: {sql}"
            );
            assert!(
                sql.trim_end().ends_with("LIMIT 1"),
                "{case}: the request-level LIMIT must land on the outermost \
                 wrapper, after every fan-out subquery closes: {sql}"
            );
            assert!(
                !sql.contains("ORDER BY"),
                "{case}: no ORDER BY may appear — the fan-out never sorts: {sql}"
            );
        };

        // Case 1: a single distinct count — the only shape that fans out. (The Case 2
        // and Case 3 arms were removed with the Case 2/3 fan-out composition: those
        // shapes now decline to the qualified single-table wrapper, whose own
        // limit/order-by behavior is covered by task 6.5.)
        let case1_items = vec![SingleGroupItem::Distinct(DistinctCount {
            column: Some("A".into()),
            arg_expr: None,
        })];
        let sql1 = build_count_distinct_scan_sql(
            &poisoned_base_spec,
            &shards,
            &case1_items,
            &col_types,
            Some(1),
            SCAN_UDF_NAME,
            DISTRIBUTE_FILES_UDF_NAME,
        );
        assert_only_outer_limit_no_order_by(&sql1, "Case 1");
    }

    // ---------------------------------------------------------------------------
    // R.1: EMITS type correctness for SUM/MIN/MAX
    // ---------------------------------------------------------------------------

    // ---------------------------------------------------------------------------
    // FIX 1: grouped aggregate with invalid agg column type falls back
    // ---------------------------------------------------------------------------

    // ---------------------------------------------------------------------------
    // R.2: multi-shard row-scan must append outer LIMIT
    // ---------------------------------------------------------------------------

    /// R.2: multi-shard row scan with LIMIT must append LIMIT to the outer SQL.
    #[test]
    fn multi_shard_row_scan_appends_outer_limit() {
        let files = vec![
            "s3://warehouse/f0.parquet".into(),
            "s3://warehouse/f1.parquet".into(),
        ];
        // cluster_nodes=2 forces 2 shards.
        let sql = build_sql_for_fixture_n(
            files,
            vec!["ID".into()],
            vec!["DECIMAL(20,0)".into()],
            None,
            Some(10),
            2,
        );
        assert!(
            !sql.contains("IPROC()"),
            "must NOT use IPROC (uses shard_key): {sql}"
        );
        assert!(
            sql.contains("shard_key"),
            "must be multi-shard (uses shard_key): {sql}"
        );
        assert!(
            sql.contains("LIMIT 10"),
            "multi-shard row scan must append outer LIMIT 10: {sql}"
        );
    }

    // ---------------------------------------------------------------------------
    // Row scan — outer ungrouped scalar scan, no SELECT * materialization wrapper
    // (decision [5]); ORDER BY/LIMIT attach directly to the outer scalar select.
    // ---------------------------------------------------------------------------

    /// A multi-shard row scan drives an OUTER UNGROUPED scalar `LAKEHOUSE_SCAN` over
    /// the nested distributor — with NO `SELECT * FROM (...)` materialization wrapper
    /// (decision [5]). The scan itself is the top-level SELECT; the distributor
    /// subquery does the `GROUP BY shard_key` fan-out. Result-equivalence (decision
    /// [7]): the returned rows are the union of every shard's rows (no outer GROUP BY,
    /// so no dedup/aggregation).
    #[test]
    fn pushdown_builds_scalar_scan_driving_sql() {
        let sql = build_sql_for_fixture_n(
            vec!["s3://w/f0.parquet".into(), "s3://w/f1.parquet".into()],
            vec!["ID".into()],
            vec!["DECIMAL(20,0)".into()],
            None,
            None,
            2,
        );
        assert!(
            !sql.contains("SELECT * FROM ("),
            "the materializing SELECT * wrapper must be gone: {sql}"
        );
        assert!(
            sql.starts_with(&format!("SELECT {SCAN_UDF_NAME}(")),
            "the outer query is the ungrouped scalar scan itself: {sql}"
        );
        assert!(
            sql.contains("GROUP BY shard_key"),
            "the fan-out GROUP BY shard_key must live inside the distributor: {sql}"
        );
        assert!(
            sql.contains(&format!("{DISTRIBUTE_FILES_UDF_NAME}(files)")),
            "the distributor subquery must carry only the files column: {sql}"
        );
    }

    /// LIMIT attaches DIRECTLY to the outer ungrouped scalar select (after the
    /// distributor subquery closes), not to a `SELECT * FROM (...)` wrapper
    /// (decision [5]).
    #[test]
    fn limit_attaches_directly_to_outer_scalar_select() {
        let sql = build_sql_for_fixture_n(
            vec!["s3://w/f0.parquet".into(), "s3://w/f1.parquet".into()],
            vec!["ID".into()],
            vec!["DECIMAL(20,0)".into()],
            None,
            Some(7),
            2,
        );
        assert!(
            !sql.contains("SELECT * FROM ("),
            "no materializing wrapper between LIMIT and the scan: {sql}"
        );
        assert!(
            sql.trim_end().ends_with("LIMIT 7"),
            "LIMIT appends to the outer scalar select: {sql}"
        );
        // The LIMIT must sit OUTSIDE the distributor subquery — after its closing paren.
        let limit_pos = sql.rfind("LIMIT 7").expect("LIMIT present");
        let close_pos = sql[..limit_pos]
            .rfind(')')
            .expect("distributor subquery closes");
        assert!(
            close_pos < limit_pos,
            "LIMIT must follow the distributor subquery's closing paren: {sql}"
        );
    }

    /// Single-shard SQL uses the two-argument form `{udf}('<common>', '<files>')`:
    /// the common blob and the whole-file-list literal each appear exactly once. The
    /// scalar scan is a from-less call on literals with no fan-out markers and no
    /// `SELECT * FROM (...)` materialization wrapper (decision [5]).
    #[test]
    fn single_shard_two_arg_common_and_files_once() {
        let files = vec![
            "s3://warehouse/db/events/part-00000.parquet".into(),
            "s3://warehouse/db/events/part-00001.parquet".into(),
        ];
        let sql = build_sql_for_fixture_n(
            files.clone(),
            vec!["ID".into(), "NAME".into()],
            vec!["DECIMAL(20,0)".into(), "VARCHAR(2000000)".into()],
            None,
            None,
            1, // single node
        );

        // Must NOT contain multi-shard markers.
        assert!(
            !sql.contains("IPROC"),
            "single-shard SQL must not contain IPROC: {sql}"
        );
        assert!(
            !sql.contains("VALUES"),
            "single-shard SQL must not contain VALUES: {sql}"
        );
        assert!(
            !sql.contains("GROUP BY"),
            "single-shard SQL must not contain GROUP BY: {sql}"
        );

        // Must be the from-less scalar scan itself (no SELECT * materialization
        // wrapper) and invoke the scan UDF.
        assert!(
            sql.starts_with(&format!("SELECT {SCAN_UDF_NAME}("))
                && !sql.contains("SELECT * FROM ("),
            "single-shard SQL must be the from-less scalar scan, no wrapper: {sql}"
        );
        assert!(sql.contains("EMITS"), "must have EMITS clause: {sql}");
        assert!(
            sql.contains(SCAN_UDF_NAME),
            "must invoke the scan UDF: {sql}"
        );

        // The common blob is serialized ONCE (endpoint + tuning knob appear once each).
        assert_eq!(
            sql.matches("http://minio:9000").count(),
            1,
            "common blob (storage endpoint) must appear exactly once: {sql}"
        );
        assert_eq!(
            sql.matches("memory_pool_fraction").count(),
            1,
            "common blob (tuning payload) must appear exactly once: {sql}"
        );

        // Both files are carried once, together in the single files-list literal
        // (arg 1), which is a JSON array — not repeated across per-shard rows.
        assert_eq!(
            sql.matches("part-00000.parquet").count(),
            1,
            "must carry file 0 exactly once: {sql}"
        );
        assert_eq!(
            sql.matches("part-00001.parquet").count(),
            1,
            "must carry file 1 exactly once: {sql}"
        );
    }

    // ---------------------------------------------------------------------------
    // detect_group_by_aggregates — GROUP BY key extraction and aggregate detection
    // ---------------------------------------------------------------------------

    // ---------------------------------------------------------------------------
    // detect_group_by_aggregates — select-list order preservation (fix-grouped-agg-select-order)
    // ---------------------------------------------------------------------------

    // ---------------------------------------------------------------------------
    // partition_files_by_bytes — G shards balanced, disjoint, full coverage
    // ---------------------------------------------------------------------------

    /// File list partitioned into G shards via shard_count is balanced, disjoint,
    /// and covers every file with no empty shards.
    #[test]
    fn partition_files_g_shards_balanced_disjoint_full_coverage() {
        use std::collections::HashSet;
        // 3 nodes × 4 factor = 12, capped to 10 files → G = 10
        let file_names: Vec<String> = (0..10).map(|i| format!("file-{i}.parquet")).collect();
        let files: Vec<(String, u64)> = file_names
            .iter()
            .enumerate()
            .map(|(i, p)| (p.clone(), (i as u64 + 1) * 100))
            .collect();
        let g = shard_count(3, 4, files.len());
        assert_eq!(g, 10, "G must equal file_count when product > file_count");
        let shards = crate::adapter::sharding::partition_files_by_bytes(files.clone(), g);
        assert_eq!(shards.len(), 10, "must produce exactly G=10 shards");
        // No shard is empty.
        for (i, shard) in shards.iter().enumerate() {
            assert!(!shard.is_empty(), "shard {i} must not be empty");
        }
        // All files covered exactly once (compare by path; sizes travel alongside).
        let all: Vec<String> = shards.iter().flatten().map(|(p, _)| p.clone()).collect();
        let unique: HashSet<&String> = all.iter().collect();
        assert_eq!(
            unique.len(),
            all.len(),
            "files must be disjoint across shards"
        );
        assert_eq!(
            unique,
            file_names.iter().collect::<HashSet<_>>(),
            "all files must be covered"
        );
    }

    // ---------------------------------------------------------------------------
    // Row-scan SQL shape — GROUP BY shard_key fan-out, single-shard collapse
    // ---------------------------------------------------------------------------

    /// Multi-shard row-scan SQL uses GROUP BY shard_key, never IPROC().
    #[test]
    fn scan_driving_sql_groups_by_shard_key_not_iproc() {
        let files: Vec<(String, u64)> = (0..3)
            .map(|i| (format!("s3://warehouse/f{i}.parquet"), (i as u64 + 1) * 100))
            .collect();
        let g = shard_count(3, 1, files.len());
        let spec_template = ScanSpec {
            common: CommonScanSpec {
                projection: vec!["ID".into()],
                storage: sample_storage(),
                ..Default::default()
            },
            files: vec![],
        };
        let shards = crate::adapter::sharding::partition_files_by_bytes(files, g);
        let sql = build_scan_driving_sql(
            &spec_template,
            &shards,
            &["ID".into()],
            &["DECIMAL(20,0)".to_string()],
            None,
            &[],
            &[],
            SCAN_UDF_NAME,
            DISTRIBUTE_FILES_UDF_NAME,
        );
        assert!(
            !sql.contains("IPROC()"),
            "multi-shard SQL must NOT contain IPROC(): {sql}"
        );
        assert!(
            sql.contains("GROUP BY"),
            "multi-shard SQL must contain GROUP BY: {sql}"
        );
        assert!(
            sql.contains("shard_key"),
            "multi-shard SQL must use shard_key: {sql}"
        );
    }

    /// Single-shard collapses to the single-invocation form (no VALUES, no GROUP BY).
    #[test]
    fn single_shard_collapses_to_single_invocation() {
        let files = vec![("s3://warehouse/f0.parquet".to_string(), 500u64)];
        let g = shard_count(1, 1, files.len());
        let spec_template = ScanSpec {
            common: CommonScanSpec {
                projection: vec!["ID".into()],
                storage: sample_storage(),
                ..Default::default()
            },
            files: vec![],
        };
        let shards = crate::adapter::sharding::partition_files_by_bytes(files, g);
        let sql = build_scan_driving_sql(
            &spec_template,
            &shards,
            &["ID".into()],
            &["DECIMAL(20,0)".to_string()],
            None,
            &[],
            &[],
            SCAN_UDF_NAME,
            DISTRIBUTE_FILES_UDF_NAME,
        );
        assert!(
            !sql.contains("IPROC()"),
            "single-shard SQL must not contain IPROC: {sql}"
        );
        assert!(
            !sql.contains("VALUES"),
            "single-shard SQL must not contain VALUES: {sql}"
        );
        assert!(
            !sql.contains("GROUP BY"),
            "single-shard SQL must not contain GROUP BY: {sql}"
        );
        assert!(
            sql.starts_with(&format!("SELECT {SCAN_UDF_NAME}("))
                && !sql.contains("SELECT * FROM ("),
            "single-shard SQL must be the from-less scalar scan, no wrapper: {sql}"
        );
    }

    // ---------------------------------------------------------------------------
    // Non-decomposable aggregate fallback to row scan
    // ---------------------------------------------------------------------------

    // ---------------------------------------------------------------------------
    // STDDEV / VARIANCE decomposition into sufficient statistics
    // ---------------------------------------------------------------------------

    // ---------------------------------------------------------------------------
    // STDDEV/VARIANCE NULL-passthrough — N=0 (pop & samp) and N=1 (samp)
    // ---------------------------------------------------------------------------

    // ---------------------------------------------------------------------------
    // HAVING must not be silently dropped on grouped-path type-validation failure
    // ---------------------------------------------------------------------------

    // ---------------------------------------------------------------------------
    // Select-list scalar expression pushdown
    // ---------------------------------------------------------------------------

    /// A function_scalar in the select list renders to a SQL expression in the
    /// scan spec projection and EMITS clause.
    #[test]
    fn selectlist_scalar_expression_rendered_in_emits() {
        // Simulate a pushdown request with UPPER(name) in the select list.
        let upper_expr = serde_json::json!({
            "type": "function_scalar",
            "name": "UPPER",
            "arguments": [{"type": "column", "name": "NAME"}]
        });
        let request = serde_json::json!({
            "involvedTables": [{
                "columns": [
                    {"name": "ID", "dataType": {"type": "DECIMAL", "precision": 10, "scale": 0}},
                    {"name": "NAME", "dataType": {"type": "VARCHAR", "size": 100}},
                ]
            }],
            "pushdownRequest": {
                "selectList": [upper_expr],
            }
        });
        let pushdown_req = request["pushdownRequest"].clone();
        let (proj_cols, proj_types) = extract_projection(&request, &pushdown_req).unwrap();
        // The rendered expression should be carried as an Expr projection item, NOT
        // a bare Column — so the scan splices it verbatim instead of quoting it as a
        // phantom identifier.
        assert_eq!(proj_cols.len(), 1);
        assert!(
            matches!(proj_cols[0], ProjectionItem::Expr { .. }),
            "a rendered scalar expression must be an Expr projection item: {proj_cols:?}"
        );
        let rendered = proj_cols[0].emit_name();
        assert!(
            rendered.contains("UPPER") || rendered.contains("upper"),
            "projection must contain rendered expression: {proj_cols:?}"
        );
        // Type for an expression falls back to VARCHAR(2000000)
        assert_eq!(proj_types[0], "VARCHAR(2000000)");
    }

    /// A `function_scalar_cast` in the select list (the real Exasol wire node
    /// type for CAST — distinct from the generic `function_scalar`) renders as
    /// a `ProjectionItem::Expr`, not the full-base-row fallback (issue #136).
    #[test]
    fn selectlist_cast_node_rendered_in_emits() {
        let cast_expr = serde_json::json!({
            "type": "function_scalar_cast",
            "name": "CAST",
            "arguments": [{"type": "column", "name": "ID"}],
            "dataType": {"type": "VARCHAR", "size": 100}
        });
        let request = serde_json::json!({
            "involvedTables": [{
                "columns": [
                    {"name": "ID", "dataType": {"type": "DECIMAL", "precision": 10, "scale": 0}},
                    {"name": "NAME", "dataType": {"type": "VARCHAR", "size": 100}},
                ]
            }],
            "pushdownRequest": {
                "selectList": [cast_expr],
            }
        });
        let pushdown_req = request["pushdownRequest"].clone();
        let (proj_cols, _proj_types) = extract_projection(&request, &pushdown_req).unwrap();
        assert_eq!(
            proj_cols.len(),
            1,
            "a function_scalar_cast select-list item must not fall back to the full \
             base row: {proj_cols:?}"
        );
        assert!(
            matches!(proj_cols[0], ProjectionItem::Expr { .. }),
            "a rendered CAST expression must be an Expr projection item: {proj_cols:?}"
        );
        let rendered = proj_cols[0].emit_name();
        assert!(
            rendered.contains(r#"CAST("ID" AS VARCHAR)"#),
            "projection must contain the rendered CAST expression: {proj_cols:?}"
        );
    }

    /// A `function_scalar_extract` in the select list (the real Exasol wire node
    /// type for EXTRACT) renders as a `ProjectionItem::Expr`, not the
    /// full-base-row fallback (issue #136).
    #[test]
    fn selectlist_extract_node_rendered_in_emits() {
        let extract_expr = serde_json::json!({
            "type": "function_scalar_extract",
            "name": "EXTRACT",
            "toExtract": "YEAR",
            "arguments": [{"type": "column", "name": "EVENT_DATE"}]
        });
        let request = serde_json::json!({
            "involvedTables": [{
                "columns": [
                    {"name": "ID", "dataType": {"type": "DECIMAL", "precision": 10, "scale": 0}},
                    {"name": "EVENT_DATE", "dataType": {"type": "DATE"}},
                ]
            }],
            "pushdownRequest": {
                "selectList": [extract_expr],
            }
        });
        let pushdown_req = request["pushdownRequest"].clone();
        let (proj_cols, _proj_types) = extract_projection(&request, &pushdown_req).unwrap();
        assert_eq!(
            proj_cols.len(),
            1,
            "a function_scalar_extract select-list item must not fall back to the full \
             base row: {proj_cols:?}"
        );
        assert!(
            matches!(proj_cols[0], ProjectionItem::Expr { .. }),
            "a rendered EXTRACT expression must be an Expr projection item: {proj_cols:?}"
        );
        let rendered = proj_cols[0].emit_name();
        assert!(
            rendered.contains("date_part"),
            "projection must contain the rendered EXTRACT expression: {proj_cols:?}"
        );
    }

    /// A `function_scalar_case` in the select list (the real Exasol wire node
    /// type for CASE) renders as a `ProjectionItem::Expr`, not the
    /// full-base-row fallback (issue #136).
    #[test]
    fn selectlist_case_node_rendered_in_emits() {
        // Searched CASE (no `basis`): WHEN arguments are boolean predicates.
        let case_expr = serde_json::json!({
            "type": "function_scalar_case",
            "name": "CASE",
            "arguments": [
                {"type": "predicate_less",
                 "left": {"type": "column", "name": "SCORE"},
                 "right": {"type": "literal_exactnumeric", "value": "50"}}
            ],
            "results": [
                {"type": "literal_string", "value": "low"},
                {"type": "literal_string", "value": "high"}
            ]
        });
        let request = serde_json::json!({
            "involvedTables": [{
                "columns": [
                    {"name": "ID", "dataType": {"type": "DECIMAL", "precision": 10, "scale": 0}},
                    {"name": "SCORE", "dataType": {"type": "DECIMAL", "precision": 10, "scale": 0}},
                ]
            }],
            "pushdownRequest": {
                "selectList": [case_expr],
            }
        });
        let pushdown_req = request["pushdownRequest"].clone();
        let (proj_cols, _proj_types) = extract_projection(&request, &pushdown_req).unwrap();
        assert_eq!(
            proj_cols.len(),
            1,
            "a function_scalar_case select-list item must not fall back to the full \
             base row: {proj_cols:?}"
        );
        assert!(
            matches!(proj_cols[0], ProjectionItem::Expr { .. }),
            "a rendered CASE expression must be an Expr projection item: {proj_cols:?}"
        );
        let rendered = proj_cols[0].emit_name();
        assert!(
            rendered.contains("CASE"),
            "projection must contain the rendered CASE expression: {proj_cols:?}"
        );
    }

    /// A CAST to an unsupported target type (e.g. TIMESTAMP WITH LOCAL TIME
    /// ZONE, which `render_cast_target` deliberately declines — see
    /// `crates/vs-expression/src/lib.rs`) still falls back to the full base
    /// row: the `None` untranslatable branch is untouched by the #136 fix.
    #[test]
    fn selectlist_untranslatable_cast_falls_back_to_full_row() {
        let cast_expr = serde_json::json!({
            "type": "function_scalar_cast",
            "name": "CAST",
            "arguments": [{"type": "column", "name": "ID"}],
            "dataType": {"type": "TIMESTAMP", "withLocalTimeZone": true}
        });
        let request = serde_json::json!({
            "involvedTables": [{
                "columns": [
                    {"name": "ID", "dataType": {"type": "DECIMAL", "precision": 10, "scale": 0}},
                    {"name": "NAME", "dataType": {"type": "VARCHAR", "size": 100}},
                ]
            }],
            "pushdownRequest": {
                "selectList": [cast_expr],
            }
        });
        let pushdown_req = request["pushdownRequest"].clone();
        let (proj_cols, proj_types) = extract_projection(&request, &pushdown_req).unwrap();
        // Full base row fallback: both table columns, as bare Column items —
        // not the single rendered expression.
        assert_eq!(
            proj_cols,
            vec![
                ProjectionItem::Column("ID".into()),
                ProjectionItem::Column("NAME".into()),
            ],
            "an untranslatable CAST target must fall back to the full base row: {proj_cols:?}"
        );
        assert_eq!(proj_types, vec!["DECIMAL(10,0)", "VARCHAR(100)"]);
    }

    /// A single projected literal renders to ONE positional `Expr` projection
    /// item — not the full-base-row fallback (issue #190).
    #[test]
    fn selectlist_literal_rendered_as_positional_expr() {
        let literal = serde_json::json!({"type": "literal_exactnumeric", "value": 1});
        let request = serde_json::json!({
            "involvedTables": [{
                "columns": [
                    {"name": "ID", "dataType": {"type": "DECIMAL", "precision": 10, "scale": 0}},
                ]
            }],
            "pushdownRequest": {
                "selectList": [literal],
                "selectListDataTypes": [{"type": "decimal", "precision": 18, "scale": 0}],
            }
        });
        let pushdown_req = request["pushdownRequest"].clone();
        let (proj_cols, proj_types) = extract_projection(&request, &pushdown_req).unwrap();
        assert_eq!(
            proj_cols.len(),
            1,
            "a single literal must not fall back to the full base row: {proj_cols:?}"
        );
        assert!(
            matches!(proj_cols[0], ProjectionItem::Expr { .. }),
            "a rendered literal must be an Expr projection item: {proj_cols:?}"
        );
        assert_eq!(proj_cols[0].emit_name(), "1");
        assert_eq!(proj_types[0], "DECIMAL(18,0)");
    }

    /// `SELECT 1, name, 1` yields three positional projection items — the two `1`
    /// literals are NOT collapsed by value-based dedup (issue #190) — and
    /// `emits_ident` assigns each a distinct EMITS identifier: the real quoted
    /// column name for the `column` item, positional synthetic names for the two
    /// `Expr` items.
    #[test]
    fn selectlist_duplicate_literals_keep_distinct_positions() {
        let literal = serde_json::json!({"type": "literal_exactnumeric", "value": 1});
        let column = serde_json::json!({"type": "column", "name": "NAME"});
        let request = serde_json::json!({
            "involvedTables": [{
                "columns": [
                    {"name": "ID", "dataType": {"type": "DECIMAL", "precision": 10, "scale": 0}},
                    {"name": "NAME", "dataType": {"type": "VARCHAR", "size": 100}},
                ]
            }],
            "pushdownRequest": {
                "selectList": [literal.clone(), column, literal],
                "selectListDataTypes": [
                    {"type": "decimal", "precision": 18, "scale": 0},
                    {"type": "varchar", "size": 100},
                    {"type": "decimal", "precision": 18, "scale": 0},
                ],
            }
        });
        let pushdown_req = request["pushdownRequest"].clone();
        let (proj_cols, _proj_types) = extract_projection(&request, &pushdown_req).unwrap();

        assert_eq!(
            proj_cols.len(),
            3,
            "the two identical literals must NOT be collapsed: {proj_cols:?}"
        );
        assert!(
            matches!(proj_cols[0], ProjectionItem::Expr { .. }),
            "position 0 must be a rendered Expr: {proj_cols:?}"
        );
        assert_eq!(proj_cols[1], ProjectionItem::Column("NAME".into()));
        assert!(
            matches!(proj_cols[2], ProjectionItem::Expr { .. }),
            "position 2 must be a rendered Expr: {proj_cols:?}"
        );

        let ident_0 = emits_ident(&proj_cols[0], 0);
        let ident_1 = emits_ident(&proj_cols[1], 1);
        let ident_2 = emits_ident(&proj_cols[2], 2);
        assert_eq!(ident_0, quote_ident("_LH_PROJ_0"));
        assert_eq!(ident_1, quote_ident("NAME"));
        assert_eq!(ident_2, quote_ident("_LH_PROJ_2"));
        assert_ne!(
            ident_0, ident_2,
            "the two literal positions must not collide"
        );
        assert_ne!(ident_0, ident_1);
        assert_ne!(ident_1, ident_2);
    }

    /// A bare literal select-list item projects exactly one column — proving the
    /// full-row fallback is NOT taken for a plain literal (issue #190), even when
    /// the table has more than one column available to fall back to.
    #[test]
    fn selectlist_bare_literal_does_not_fall_back_to_full_row() {
        let literal = serde_json::json!({"type": "literal_string", "value": "x"});
        let request = serde_json::json!({
            "involvedTables": [{
                "columns": [
                    {"name": "ID", "dataType": {"type": "DECIMAL", "precision": 10, "scale": 0}},
                    {"name": "NAME", "dataType": {"type": "VARCHAR", "size": 100}},
                ]
            }],
            "pushdownRequest": {
                "selectList": [literal],
                "selectListDataTypes": [{"type": "varchar", "size": 100}],
            }
        });
        let pushdown_req = request["pushdownRequest"].clone();
        let (proj_cols, _proj_types) = extract_projection(&request, &pushdown_req).unwrap();
        assert_eq!(
            proj_cols.len(),
            1,
            "a bare literal must project exactly one column, not the full base row: {proj_cols:?}"
        );
    }

    /// A literal declared `TIMESTAMP WITH LOCAL TIME ZONE` renders successfully
    /// (via `render_expression_safe`) but declines to the full-base-row fallback
    /// because Exasol rejects that type as a UDF EMITS output (sqlCode 22002) —
    /// mirrors `selectlist_untranslatable_cast_falls_back_to_full_row`.
    #[test]
    fn selectlist_tstz_literal_falls_back_to_full_row() {
        let literal = serde_json::json!({
            "type": "literal_timestamp_utc",
            "value": "2024-03-01 10:00:00"
        });
        // Confirm the literal actually renders — the decline must be due to the
        // EMITS-type gate, not a render failure.
        assert!(
            render_expression_safe(&literal).is_some(),
            "literal_timestamp_utc must render via render_expression_safe"
        );
        let request = serde_json::json!({
            "involvedTables": [{
                "columns": [
                    {"name": "ID", "dataType": {"type": "DECIMAL", "precision": 10, "scale": 0}},
                    {"name": "NAME", "dataType": {"type": "VARCHAR", "size": 100}},
                ]
            }],
            "pushdownRequest": {
                "selectList": [literal],
                "selectListDataTypes": [{"type": "TIMESTAMP", "withLocalTimeZone": true}],
            }
        });
        let pushdown_req = request["pushdownRequest"].clone();
        let (proj_cols, proj_types) = extract_projection(&request, &pushdown_req).unwrap();
        assert_eq!(
            proj_cols,
            vec![
                ProjectionItem::Column("ID".into()),
                ProjectionItem::Column("NAME".into()),
            ],
            "a TIMESTAMP WITH LOCAL TIME ZONE literal must fall back to the full base row: {proj_cols:?}"
        );
        assert_eq!(proj_types, vec!["DECIMAL(10,0)", "VARCHAR(100)"]);
    }

    /// A plain `TIMESTAMP` (NOT with-local-time-zone) literal IS rendered as a
    /// positional `Expr`, never declined — locking the exact-match boundary in
    /// `is_valid_emits_output_type` so `TIMESTAMP` is never treated as a prefix of
    /// `TIMESTAMP WITH LOCAL TIME ZONE`.
    #[test]
    fn selectlist_plain_timestamp_literal_rendered_as_expr() {
        let literal = serde_json::json!({
            "type": "literal_timestamp",
            "value": "2024-03-01 10:00:00"
        });
        let request = serde_json::json!({
            "involvedTables": [{
                "columns": [
                    {"name": "ID", "dataType": {"type": "DECIMAL", "precision": 10, "scale": 0}},
                ]
            }],
            "pushdownRequest": {
                "selectList": [literal],
                "selectListDataTypes": [{"type": "TIMESTAMP"}],
            }
        });
        let pushdown_req = request["pushdownRequest"].clone();
        let (proj_cols, proj_types) = extract_projection(&request, &pushdown_req).unwrap();
        assert_eq!(
            proj_cols.len(),
            1,
            "a plain TIMESTAMP literal must not fall back to the full base row: {proj_cols:?}"
        );
        assert!(
            matches!(proj_cols[0], ProjectionItem::Expr { .. }),
            "a plain TIMESTAMP literal must be rendered as an Expr: {proj_cols:?}"
        );
        assert_eq!(proj_types[0], "TIMESTAMP");
    }

    /// An untranslatable select-list item falls back to the bare column.
    #[test]
    fn selectlist_untranslatable_item_falls_back_to_column() {
        // A node type the translator cannot handle
        let bad_expr = serde_json::json!({
            "type": "function_aggregate",  // aggregate in select list -> untranslatable as scalar expr
            "name": "SUM",
            "arguments": [{"type": "column", "name": "AMOUNT"}]
        });
        let request = serde_json::json!({
            "involvedTables": [{
                "columns": [
                    {"name": "AMOUNT", "dataType": {"type": "DECIMAL", "precision": 18, "scale": 2}},
                ]
            }],
            "pushdownRequest": {
                "selectList": [bad_expr],
            }
        });
        let pushdown_req = request["pushdownRequest"].clone();
        let (proj_cols, proj_types) = extract_projection(&request, &pushdown_req).unwrap();
        // Fall back to the first column name
        assert_eq!(proj_cols.len(), 1);
        assert_eq!(proj_cols[0], "AMOUNT");
        assert_eq!(proj_types[0], "DECIMAL(18,2)");
    }

    // ---------------------------------------------------------------------------
    // HAVING predicate — applied in the outer wrapper only, never in shard scan
    // ---------------------------------------------------------------------------

    // ---------------------------------------------------------------------------
    // Task 4.1 — Pushdown wiring: filter JSON reaches Iceberg predicate and
    // ScanSpec.filter (DataFusion string) is preserved on both paths.
    // ---------------------------------------------------------------------------

    /// Scenario: Filter predicate is pushed into the scan spec.
    ///
    /// For a translatable filter (equality on a typed column):
    /// - `ScanSpec.filter` (DataFusion SQL string) is `Some`.
    /// - `to_iceberg_predicate` over the same JSON + a matching schema is `Some`.
    ///
    /// Both coexist: Iceberg prunes files; DataFusion enforces row correctness.
    #[test]
    fn filter_in_common_arg() {
        use crate::adapter::iceberg_predicate::to_iceberg_predicate;
        use iceberg::spec::{NestedField, Schema, Type};
        use std::sync::Arc;

        // Build a minimal schema with an Int column "id".
        let schema = Schema::builder()
            .with_schema_id(1)
            .with_fields(vec![Arc::new(NestedField::required(
                1,
                "id",
                Type::Primitive(iceberg::spec::PrimitiveType::Int),
            ))])
            .build()
            .unwrap();

        let filter_json = serde_json::json!({
            "type": "predicate_equal",
            "left": {"type": "column", "name": "id"},
            "right": {"type": "literal_exactnumeric", "value": 42}
        });

        // DataFusion path: render_df_filter_safe must produce Some.
        let df_filter = render_df_filter_safe(&filter_json);
        assert!(
            df_filter.is_some(),
            "translatable filter must produce a DataFusion SQL string"
        );

        // Iceberg path: to_iceberg_predicate over the same JSON must produce Some.
        let iceberg_pred = to_iceberg_predicate(&filter_json, &schema);
        assert!(
            iceberg_pred.is_some(),
            "translatable filter must produce an Iceberg predicate"
        );

        // Confirm the DataFusion string survives into the common (arg 0) blob.
        let sql = build_sql_for_fixture(
            vec!["s3://warehouse/f.parquet".into()],
            vec!["ID".into()],
            vec!["DECIMAL(10,0)".into()],
            df_filter,
            None,
        );
        let common = common_arg_literal(&sql);
        assert!(
            common.contains("\"filter\"") && common.contains("42"),
            "filter must be pushed into the common arg: {common}"
        );
    }

    // ---------------------------------------------------------------------------
    // Task 4.3 — No credential in error text
    // ---------------------------------------------------------------------------

    /// Scenario: redact_catalog_error removes credential-shaped values from messages.
    #[test]
    fn redact_catalog_error_strips_credentials() {
        let msg = "GET failed: access_key=AKID_SECRET_VALUE region=us-east-1";
        let safe = redact_catalog_error(msg);
        assert!(
            !safe.contains("AKID_SECRET_VALUE"),
            "credential value must be redacted: {safe}"
        );
        assert!(
            safe.contains("access_key"),
            "label must be preserved: {safe}"
        );
    }
}
