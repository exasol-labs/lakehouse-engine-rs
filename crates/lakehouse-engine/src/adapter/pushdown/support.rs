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
use crate::types::mapping::{ExaTypeClass, classify_exa_type, exasol_type_from_json};
use exasol_udf_sdk::error::UdfError;
use serde_json::Value as Json;
use vs_expression::{render_df_filter_safe, render_expression_safe};

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
///
/// `request_limit` is the RAW request limit, distinct from `limit` (which the
/// row-scan sub-path renders unchanged). It is consumed ONLY by the aggregate
/// sub-path: `LIMIT n` on a guaranteed one-row merge is a no-op for `n >= 1` and
/// correct for `n = 0`. The row-scan sub-path ignores it entirely.
#[allow(clippy::too_many_arguments)]
pub fn build_scan_driving_sql<E: Clone + Into<FileEntry>>(
    spec_template: &ScanSpec,
    shards: &[Vec<E>],
    proj_cols: &[ProjectionItem],
    proj_types: &[String],
    limit: Option<u64>,
    request_limit: Option<u64>,
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
            request_limit,
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
///
/// `request_limit` renders as a trailing `LIMIT n` on the outer merge SELECT when
/// `Some(n)` — a no-op over the guaranteed one-row merge for `n >= 1`, correct for
/// `n = 0` (issue #198: a pushed `LIMIT 0` must return zero rows, not be silently
/// dropped because the aggregate sub-path had no limit value to render before this
/// parameter existed).
#[allow(clippy::too_many_arguments)]
fn build_aggregate_scan_sql<E: Clone + Into<FileEntry>>(
    spec_template: &ScanSpec,
    shards: &[Vec<E>],
    aggregates: &[AggregatePlan],
    col_types: &[(String, String)],
    aggregate_types: &[String],
    request_limit: Option<u64>,
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

    let mut sql = format!("SELECT {merge_select} FROM ({fan_out})");
    if let Some(n) = request_limit {
        sql.push_str(&format!(" LIMIT {n}"));
    }
    sql
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
/// truncate the per-shard distinct set → a wrong count); the request's raw `limit` is
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

/// The `(folded name, Exasol type)` columns of ONE involved table of `request`.
///
/// The ONE `col_types` builder: it owns the `involvedTables` walk, the `name`/`dataType`
/// read, and the [`exasol_type_from_json`] mapping. An absent `involvedTables`, a table
/// `select_table` does not select, and absent `columns` each yield an empty vec; a column
/// missing either `name` or `dataType` is skipped.
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

/// Extract all columns and their Exasol types from the first involved table.
pub(super) fn extract_all_column_types(request: &Json) -> Vec<(String, String)> {
    column_types(request, |tables: &[Json]| tables.first())
}

/// Deep-clone `expr` with every `tableAlias` key removed, so the reused
/// `vs-expression` translator renders BARE column names.
///
/// Exasol stamps EVERY `column` node with the query's `tableAlias` as soon as the
/// `FROM` aliases the table (`FROM fact_orders o` yields `tableAlias: "O"` on every
/// column, even one written unqualified), and the translator emits `"ALIAS"."NAME"`
/// whenever `tableAlias` is present. Every scan this adapter drives is a
/// SINGLE-relation scan whose relation exposes BARE uppercase column names, so an
/// alias-qualified reference does not resolve against it (`No field named
/// "O"."O_ORDERDATE"`). Both such callers therefore strip first: the single-table
/// pushdown chokepoint in `handle_pushdown` (issue #193) and the per-side join
/// fan-out leg (`build_side_fan_out_sql`).
///
/// This is a CALLER-side concern, not a translator default: the join outer wrapper
/// deliberately re-qualifies each column to its own subquery alias
/// (`annotate_columns_with_alias`), which overwrites any `tableAlias` a caller left
/// in place — so stripping upstream of it is harmless there too.
///
/// `tableName` is left intact (the translator ignores it; join conjunct attribution
/// and the wrapper's re-qualification both read it, and both run on `tableName`
/// alone).
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

/// Answers whether the DataFusion dialect can express `expr` as a scan-spec
/// filter.
///
/// A predicate this returns `false` for cannot be pushed into the
/// DataFusion-in-UDF scan spec; the caller MUST self-apply it in the adapter's
/// own returned SQL instead, never omit it (no Exasol-side fallback — see
/// CLAUDE.md § "Virtual Schema pushdown delegation"). A trivially-true
/// predicate (renders to `TRUE`/`NULL`) answers `true`: omitting a no-op
/// predicate is correct, not a decline.
pub(super) fn datafusion_renderable(expr: &Json) -> bool {
    render_expression_safe(expr).is_some()
}

/// The expression-grammar fields whose value is an ARRAY of child expressions.
/// Curated deliberately — see [`rewrite_expr_tree`].
const EXPR_ARRAY_FIELDS: [&str; 3] = ["expressions", "arguments", "results"];

/// The expression-grammar fields whose value is a SINGLE child expression.
/// Curated deliberately — see [`rewrite_expr_tree`].
const EXPR_SINGLE_FIELDS: [&str; 5] = ["expression", "pattern", "left", "right", "basis"];

/// Rewrite an Exasol pushdown expression tree bottom-up: at every node each curated
/// child is rewritten FIRST, then `f` is applied to that node with its
/// already-rewritten children in place.
///
/// The single owner of the traversal the type-aware guards share; each of them
/// supplies only its per-node decision as `f`.
///
/// ## Post-order is load-bearing
///
/// A guard's own check sees rewritten children, which is what makes a NESTED
/// occurrence reachable: Exasol encodes `a||b||c` as `CONCAT(a, CONCAT(b, c))`, so a
/// check inspecting only the outer node's direct arguments would never reach the
/// inner ones. It is also what stops a double rewrite at the outer level — an inner
/// argument already coerced is no longer a bare column when the outer check runs.
///
/// ## A decline propagates to the root
///
/// `f` returning `None` declines the WHOLE tree, not just the declining subtree: the
/// `None` travels out through every enclosing level via `?`. That mirrors the
/// all-or-nothing untranslatable-predicate backstop — the whole filter or the whole
/// select-list item is declined. This is NOT a case where Exasol safely evaluates the
/// decline natively — the caller must itself self-apply the declined filter (or fall
/// back to the base row for a select-list item). An INFALLIBLE rewriter composes as
/// the never-declining case: with a statically always-`Some` `f` the result is always
/// `Some`, so such a caller keeps its `-> Json` signature by unwrapping with a
/// fallback rather than a panic.
///
/// ## Why the field list is curated
///
/// [`EXPR_ARRAY_FIELDS`] and [`EXPR_SINGLE_FIELDS`] enumerate this grammar's
/// child-bearing fields by hand instead of walking every map value. A blind walk
/// would descend into a node's object-valued `dataType` sub-object (e.g.
/// `{"type":"VARCHAR"}`) and hand it to `f` as if it were an expression, letting a
/// guard rewrite a declared type. `name` is excluded too, though for a different
/// reason: it always carries a bare identifier string in this grammar, never an
/// object, so keeping it off the curated list keeps identifiers unrewritable.
/// Widening the reach therefore means adding a field to one of the two consts, which
/// all callers inherit.
///
/// Child shapes are matched as the grammar sends them: an array field is descended
/// into only when it really is a `Json::Array`, a single-child field only when the
/// child is an object. A leaf — a literal, a `column`, any non-object node — has no
/// curated children and is handed straight to `f`.
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

/// Make a pushed-down `LIKE` / `REGEXP_LIKE` filter type-safe for the DataFusion
/// scan, using the column-type map from [`extract_all_column_types`].
///
/// Exasol implicitly casts a non-string `LIKE`/`REGEXP_LIKE` subject to VARCHAR
/// before matching; DataFusion has no such coercion, so a pushed-down `LIKE` over a
/// DATE / DECIMAL / integer column hard-fails at scan time
/// (`There isn't a common type to coerce <Type> and Utf8 in LIKE expression`,
/// issue #207). A `LIKE` `column` node never carries a `dataType` on the wire —
/// column types live only in `involvedTables[0].columns` — so this type-aware
/// decision belongs in the adapter, not in the stateless (and sibling-shared)
/// `vs-expression` translator (decision-log [1]).
///
/// Walks the filter tree through [`rewrite_expr_tree`], the same post-order primitive
/// [`string_function_arg_type_guard`] and [`rewrite_decimal_stringifications`] run on:
/// every curated child under [`EXPR_ARRAY_FIELDS`]/[`EXPR_SINGLE_FIELDS`] is visited,
/// not only the `predicate_and`/`predicate_or`/`predicate_not` junctions a narrower,
/// pre-migration traversal used to stop at. A `predicate_like` buried inside a
/// `function_scalar_case`, under a comparison predicate's `left`/`right`, or inside a
/// scalar function's `arguments` is now reached the same as one sitting directly
/// under a junction — the #207 blind spot that narrower traversal left open is
/// closed. Any node that is not itself a `predicate_like`/`predicate_like_regexp` is
/// returned unchanged by the closure below; this guard only inspects `LIKE` subjects.
///
/// Widening the reach widens the decline trade [`guard_like_subject`] already made,
/// in two different directions depending on why it declines. Where the subject's
/// Exasol type RESOLVES to a non-string type, a `LIKE` newly reached by this wider
/// traversal used to hard-fail the DataFusion scan outright (`There isn't a common
/// type to coerce <Type> and Utf8 in LIKE expression`) and now declines to native
/// Exasol evaluation instead — strictly a fix, never a cost. Where the subject's
/// column NAME does not resolve in `col_types` (a lookup miss, fail-safe decline), a
/// `LIKE` that previously rendered unguarded — because the narrower traversal never
/// reached it — may now decline a filter that would have pushed down and worked:
/// slower, never wrong.
///
/// At each `predicate_like` / `predicate_like_regexp` whose `expression` (subject)
/// is a bare `column` node, the subject name is uppercased and looked up in
/// `col_types`, then dispatched:
///
/// | Exasol type | Action |
/// |-------------|--------|
/// | `VARCHAR…` / `CHAR…` | leave the node unchanged |
/// | `DATE` | rewrap the subject as `CAST(<col> AS VARCHAR)` (`function_scalar_cast`) |
/// | any other type (DECIMAL, DOUBLE, BOOLEAN, TIMESTAMP, …) | decline the whole filter |
/// | name not found in `col_types` (lookup miss) | decline the whole filter (fail-safe) |
///
/// A subject that is not a bare `column` (e.g. a computed scalar expression) leaves
/// the `LIKE` node unchanged (out of scope, pre-existing behavior).
///
/// The DATE `CAST` is Exasol-faithful only under the default `NLS_DATE_FORMAT`
/// (`YYYY-MM-DD`), which is both DataFusion's unconditional `Date32`→`Utf8` form and
/// Exasol's default; an altered session format is an accepted tracked exception
/// (#216, decision-log [8]).
///
/// ## Descending into a `LIKE` node's own children is inert
///
/// [`rewrite_expr_tree`] is post-order, so a `LIKE` node's `expression` (subject) and
/// `pattern` are rewritten BEFORE the node itself is dispatched to
/// [`guard_like_subject`]. That descent cannot disturb the dispatch: the per-node
/// closure acts on the two `LIKE` node types and clones everything else, so a bare
/// `column` subject (and a literal pattern) comes back as the same value and
/// [`guard_like_subject`] sees exactly the node it would have seen without the
/// descent. The `function_scalar_cast` the DATE arm wraps the subject in is
/// synthesized by the closure itself, after the descent is finished, so the traversal
/// never revisits it and cannot double-wrap it.
///
/// Skipping a NON-object child (which [`rewrite_expr_tree`] does, descending only into
/// an object) is inert for the same reason: a non-object node has no curated child
/// field of its own and falls to the closure's clone-everything-else arm, so
/// descending into one would only put back the value that is already in place.
///
/// Returns:
/// - `Some(tree)` — render this (possibly DATE-rewrapped) tree.
/// - `None` — decline the WHOLE top-level filter. A decline found anywhere in the
///   tree propagates to the outer call, mirroring the all-or-nothing
///   untranslatable-predicate backstop (`super`'s module header). A decline here is
///   NOT safely deferred to Exasol — the caller must itself self-apply the declined
///   filter (e.g. as an outer WHERE) rather than omit it (decision-log [3]).
fn like_subject_type_guard(filter: &Json, col_types: &[(String, String)]) -> Option<Json> {
    rewrite_expr_tree(
        filter,
        &|out: &Json| match out.get("type").and_then(|t| t.as_str()) {
            Some("predicate_like" | "predicate_like_regexp") => guard_like_subject(out, col_types),
            _ => Some(out.clone()),
        },
    )
}

/// The Exasol type `node`'s column name resolves to in `col_types`, if any.
///
/// The ONE `col_types` lookup for the type-rewrite guards: read `name`, fold it with the
/// full-Unicode `to_uppercase` to match the keys `extract_all_column_types` builds, then
/// scan. `involved_table_columns` applies that same Unicode fold to its own keys, so this
/// lookup's fold matches both builders' output for any column name — not a claim that the
/// two builders' lists are the same vector, which they are not (they differ by table
/// selection). Separately, `resolve_table_schema` Unicode-uppercases names before declaring
/// them, so no LOWERCASE name ever reaches this lookup — a premise guarded by
/// `non_ascii_table_and_column_stay_queryable`. Non-ASCII letters can still reach it (e.g.
/// `über` uppercases to `ÜBER`, not to an ASCII form); `to_uppercase` is idempotent, so the
/// name is already a fixed point regardless of whether it happens to be ASCII. `None` means
/// the type is not resolvable — the node carries no `name`, or its folded name is absent
/// from the list — two cases every caller already treats identically.
///
/// Deliberately does NOT test `node`'s `type` tag. A non-`column` node is a PASS-THROUGH
/// for [`guard_like_subject`] and [`coerce_string_position_arg`] but an unresolvable type
/// is a DECLINE, so absorbing the tag test here would turn every literal and computed
/// argument into a decline. Each caller keeps its own tag test and its own meaning for a
/// `None`.
fn column_exa_type<'t>(node: &Json, col_types: &'t [(String, String)]) -> Option<&'t str> {
    let name = node.get("name").and_then(|n| n.as_str())?.to_uppercase();
    col_types
        .iter()
        .find(|(n, _)| *n == name)
        .map(|(_, t)| t.as_str())
}

/// Type-check and, if needed, rewrite a single `predicate_like` /
/// `predicate_like_regexp` node. See [`like_subject_type_guard`] for the dispatch
/// table; returns `None` to decline the whole filter.
fn guard_like_subject(like_node: &Json, col_types: &[(String, String)]) -> Option<Json> {
    let subject = like_node.get("expression");

    // Only a bare `column` subject is in scope; anything else (a computed scalar
    // expression) is left untouched.
    let is_bare_column =
        subject.and_then(|s| s.get("type")).and_then(|t| t.as_str()) == Some("column");
    if !is_bare_column {
        return Some(like_node.clone());
    }
    let subject = subject.expect("is_bare_column implies expression is present");

    match column_exa_type(subject, col_types).map(classify_exa_type) {
        // String subject: DataFusion coerces it natively — leave unchanged.
        Some(ExaTypeClass::Character) => Some(like_node.clone()),
        // DATE: rewrap the subject as CAST(<col> AS VARCHAR); DataFusion's Date32→Utf8
        // cast is `YYYY-MM-DD`.
        Some(ExaTypeClass::Date) => {
            let mut out = like_node.clone();
            out["expression"] = wrap_cast_to_varchar(subject);
            Some(out)
        }
        // Every other non-string type (DECIMAL incl. integer DECIMAL(p,0), DOUBLE,
        // BOOLEAN, TIMESTAMP, …) and an unresolvable type decline the WHOLE filter: DataFusion's
        // formatting of these to string diverges from Exasol's, so a native-eval
        // fallback is safer than a silently-wrong or hard-failing cast (decision-log [2]).
        Some(ExaTypeClass::Decimal | ExaTypeClass::Other) | None => None,
    }
}

/// Rewrite every place a bare DECIMAL column is DIRECTLY stringified into a
/// `decimal_to_varchar_exasol` node wrapping that column, so the rendered SQL
/// reproduces Exasol's shortest-form DECIMAL→string conversion (trailing scale
/// zeros trimmed) instead of DataFusion's fixed-declared-scale rendering
/// (issue #211).
///
/// Exasol trims trailing scale zeros when converting a DECIMAL to string
/// (`2912.00`→`'2912'`); DataFusion's `CAST(decimal AS VARCHAR)` and its implicit
/// decimal→utf8 coercion (used by `CONCAT`/`||` and `LENGTH`) both render the full
/// declared scale — a silent wrong-result divergence. A DECIMAL column carries no
/// `dataType` on an expression node (types live only in
/// `involvedTables[0].columns`), so this type-aware rewrite belongs in the adapter,
/// not in the stateless `vs-expression` translator (mirrors [`like_subject_type_guard`]).
///
/// The three stringifier shapes rewritten (using the `col_types` map from
/// [`extract_all_column_types`]) are, when the DIRECT argument is a bare DECIMAL
/// column:
///
/// - `CAST(<decimal column> AS VARCHAR|CHAR)` — the WHOLE cast node is replaced with
///   `{"type":"decimal_to_varchar_exasol","arguments":[<column>]}` (the synthesized
///   node already produces the correct VARCHAR-typed trimmed string, so it is NOT
///   nested inside the original CAST).
/// - `CONCAT(...)` — each argument that is a bare DECIMAL column is replaced with a
///   `decimal_to_varchar_exasol`-wrapping node; every other argument is left as-is.
/// - `LENGTH(<decimal column>)` — its single argument is wrapped, same as CONCAT.
///
/// A DECIMAL column in ANY other context (arithmetic, comparison, `CAST` to a
/// non-string target, a `LIKE` subject, etc.) is left untouched, and a
/// non-DECIMAL column argument (or a computed-expression argument, whose type is
/// not resolvable from `col_types`) is left untouched — the latter a tracked
/// exception in the spec.
///
/// ## Post-order recursion is load-bearing
///
/// The traversal is [`rewrite_expr_tree`]: children are rewritten FIRST (post-order),
/// then the node's own stringifier check runs as that primitive's per-node closure.
/// Post-order is what makes NESTED occurrences reachable: Exasol encodes `a||b||c`
/// as NESTED `CONCAT(a, CONCAT(b, c))` (confirmed live for
/// `id||'-'||c_decimal_a`), so a rewriter inspecting only the OUTER `CONCAT`'s direct
/// arguments would never reach `c_decimal_a` (a direct argument only of the INNER
/// `CONCAT`). [`rewrite_expr_tree`] walks every field in [`EXPR_ARRAY_FIELDS`] and
/// [`EXPR_SINGLE_FIELDS`] — this codebase's curated child-bearing fields — so a
/// stringifier buried arbitrarily deep (inside a `CASE` branch, inside a logical
/// connective, inside another `CONCAT`) is still found and rewritten.
///
/// Reached via [`apply_type_rewrites`] — the one pipeline function that runs this
/// pass as the third step of its three-pass order for every caller — and is this
/// pass's only PRODUCTION caller (the pass corpus in `mod tests` calls it directly).
///
/// The closure passed to [`rewrite_expr_tree`] is statically always-`Some` — it has
/// no decline path, only unconditional per-node-type rewrites — so `rewrite_expr_tree`
/// can never actually return `None` here. The `.unwrap_or_else` below is therefore an
/// unreachable-in-practice fallback kept only to compose with `rewrite_expr_tree`'s
/// `Option`-returning signature, not a real decline path; it deliberately falls back
/// to a clone rather than panicking, so a change that somehow broke the invariant
/// would degrade to a no-op rewrite instead of taking down query planning.
fn rewrite_decimal_stringifications(node: &Json, col_types: &[(String, String)]) -> Json {
    rewrite_expr_tree(node, &|out: &Json| {
        // With children already rewritten (by `rewrite_expr_tree`'s post-order
        // traversal), check whether THIS node is one of the three stringifier shapes
        // over a (still-)bare DECIMAL column argument.
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
                    // Replace the WHOLE cast node — `decimal_to_varchar_exasol` already
                    // yields the correct VARCHAR-typed trimmed string; do not re-nest it
                    // inside the original CAST.
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
                // CONCAT and LENGTH both implicitly stringify each DECIMAL argument.
                // Per-argument replacement: wrap only the bare DECIMAL columns, leave
                // literals, non-DECIMAL columns, and already-recursed nested nodes as-is.
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
            // Any other node type (comparison predicate, arithmetic function, CAST to a
            // non-string target, …): return the children-rewritten node unchanged. A bare
            // DECIMAL column that is a direct argument here is NOT wrapped — this is what
            // keeps `c_decimal_a > 5` and `CAST(c_decimal_a AS DOUBLE)` untouched.
            _ => out.clone(),
        })
    })
    .unwrap_or_else(|| node.clone())
}

/// Whether `node` is a bare `column` node whose (uppercased) name resolves in
/// `col_types` to [`ExaTypeClass::Decimal`]. Integer columns are wire-encoded as
/// `DECIMAL(p,0)`, which also classifies as `Decimal` (harmless: the trim is a
/// no-op on a scale-0 value).
fn is_bare_decimal_column(node: &Json, col_types: &[(String, String)]) -> bool {
    if node.get("type").and_then(|t| t.as_str()) != Some("column") {
        return false;
    }
    matches!(
        column_exa_type(node, col_types).map(classify_exa_type),
        Some(ExaTypeClass::Decimal)
    )
}

/// Wrap a (bare-column) node in the adapter-synthesized `decimal_to_varchar_exasol`
/// node that `vs-expression`'s `render_expression_inner` renders via
/// `format_decimal_exasol_style`. This node is NEVER sent by Exasol on the wire — it
/// only ever appears because this rewriter synthesizes it.
fn wrap_decimal_to_varchar(column: &Json) -> Json {
    serde_json::json!({
        "type": "decimal_to_varchar_exasol",
        "arguments": [column.clone()],
    })
}

/// Wrap a node in an explicit `CAST(<node> AS VARCHAR)` (`function_scalar_cast`).
/// `render_cast_target`'s DataFusion arm renders `{"type":"VARCHAR"}` as bare
/// `VARCHAR` (no length), yielding `CAST(<node> AS VARCHAR)`. Shared by
/// [`guard_like_subject`] and [`coerce_string_position_arg`] so both DATE branches are
/// provably identical.
fn wrap_cast_to_varchar(node: &Json) -> Json {
    serde_json::json!({
        "type": "function_scalar_cast",
        "name": "CAST",
        "dataType": {"type": "VARCHAR"},
        "arguments": [node.clone()],
    })
}

/// Which arguments of an Exasol string function carry a STRING value — the ones
/// Exasol implicitly converts to VARCHAR before evaluating, and which therefore have
/// to be type-dispatched before the DataFusion scan sees them (issue #210). Returned
/// by [`string_position_args`].
#[derive(Debug, PartialEq, Eq)]
enum StringPositionArgs {
    /// Not a governed string function: the caller leaves the node unchanged and NEVER
    /// declines on it. Covers `CHR` / `UNICODECHR` (their single argument is a genuine
    /// integer codepoint) and every non-string function.
    NotGoverned,
    /// The string-position argument indices, every one guaranteed `< arg_count`.
    Coerce(Vec<usize>),
    /// Decline the whole tree: this function at this arity is not rendered faithfully.
    Decline,
}

/// Resolve which of a string function's arguments are string positions, from its name
/// and arity alone — a pure table, with no column types involved (those are
/// [`coerce_string_position_arg`]'s job).
///
/// | Function | String-position indices |
/// |----------|-------------------------|
/// | `CONCAT`, `TRIM`, `LTRIM`, `RTRIM`, `REPLACE`, `TRANSLATE` | all of `0..arg_count` |
/// | `LOWER`, `UPPER`, `ASCII`, `INITCAP`, `REVERSE`, `LENGTH`, `OCTET_LENGTH`, `UNICODE`, `SUBSTR`, `REPEAT`, `LEFT`, `RIGHT` | `[0]` — every further argument is a genuine number (a start offset, a length, a repeat count) |
/// | `LPAD`, `RPAD` | `[0, 2]` when the optional pad STRING is present, else `[0]` — never the numeric length at index 1 |
/// | `INSTR`, `LOCATE` | `[0, 1]`, clamped to the arity, at two or fewer arguments; [`StringPositionArgs::Decline`] beyond two |
/// | `CHR`, `UNICODECHR`, anything else | [`StringPositionArgs::NotGoverned`] |
///
/// `fn_name` is uppercased before matching. Every returned index is filtered to
/// `< arg_count`, so a caller may index `arguments` with it directly.
///
/// `INSTR` / `LOCATE` beyond two arguments DECLINE unconditionally on argument type,
/// and that branch must not be "simplified" into a `Coerce(vec![0, 1])`:
/// `vs-expression`'s `INSTR` / `LOCATE` arms
/// (`crates/vs-expression/src/lib.rs:741-772`) read only `args[0]` / `args[1]` and
/// silently drop the third and fourth, so coercing index 0 would let a TRUNCATED
/// rendering plan successfully and return a position computed from offset 1 — a
/// silently wrong answer, where today there is a loud DataFusion planning error
/// (tracked as issue #228). Declining keeps those calls on native Exasol evaluation.
///
/// `LOCATE`'s argument REORDER does not affect this table: Exasol's
/// `LOCATE(substring, string)` renders as `strpos(string, substring)`
/// (`crates/vs-expression/src/lib.rs:757-772`), but that swap happens at RENDER time,
/// after this dispatch — both of its indices are string positions either way.
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

/// Make every pushed-down Exasol string function type-safe for the DataFusion scan,
/// using the column-type map from [`extract_all_column_types`] (issue #210).
///
/// Exasol implicitly converts a numeric or DATE string-function argument to VARCHAR
/// before evaluating; DataFusion refuses and the scan dies at PLAN time, so each
/// string-position argument ([`string_position_args`] decides which those are) is
/// dispatched on its Exasol type by [`coerce_string_position_arg`] before rendering.
///
/// The traversal is the same [`rewrite_expr_tree`] primitive
/// [`rewrite_decimal_stringifications`] runs on, so both share one owner for the
/// post-order-plus-curated-field-list traversal. Both halves earn their keep:
/// post-order is what coerces the INNER call of a nested `UPPER(TRIM(<decimal
/// column>))` before the outer check runs (which then sees a non-column argument and
/// cannot re-wrap it), and the broad field list is what reaches a string function
/// under a COMPARISON predicate — `UPPER(c) = 'X'` is a `predicate_equal` with the
/// function under `left`, the shape issue #210's WHERE-clause repro takes.
///
/// Returns:
/// - `Some(tree)` — render this (possibly coerced) tree.
/// - `None` — decline. A decline anywhere in the tree propagates to the outer call
///   through `?`, mirroring [`like_subject_type_guard`]: the WHOLE filter or the
///   WHOLE select-list item is declined. This decline is NOT safely deferred to
///   Exasol — the caller must itself self-apply the declined filter (or fall back to
///   the base row for a select-list item) rather than omit it. A `NotGoverned` node
///   never declines, whatever its arguments' types.
///
/// Runs BEFORE [`rewrite_decimal_stringifications`] in [`apply_type_rewrites`], the
/// one pipeline function that chains the two and is the sole enforcer of that
/// ordering: a coerced argument is no longer a bare column, so the decimal rewriter
/// no-ops on it instead of double-wrapping.
fn string_function_arg_type_guard(node: &Json, col_types: &[(String, String)]) -> Option<Json> {
    rewrite_expr_tree(node, &|out: &Json| {
        // With children already guarded (by `rewrite_expr_tree`'s post-order
        // traversal), dispatch THIS node's own string-position arguments. Only a
        // `function_scalar` can be a string function; every other node type passes
        // through unchanged.
        if out.get("type").and_then(|t| t.as_str()) != Some("function_scalar") {
            return Some(out.clone());
        }
        // `fn_name` is passed un-uppercased: `string_position_args` uppercases it itself.
        let fn_name = out.get("name").and_then(|n| n.as_str()).unwrap_or("");
        let arg_count = out
            .get("arguments")
            .and_then(|a| a.as_array())
            .map_or(0, |a| a.len());
        match string_position_args(fn_name, arg_count) {
            // Not a governed string function: leave it alone WITHOUT declining, even over a
            // non-coercible argument — `CHR`/`UNICODECHR` take a genuine integer codepoint.
            StringPositionArgs::NotGoverned => Some(out.clone()),
            // An arity `vs-expression` renders incompletely (#228) — decline it rather than
            // push a truncated rendering.
            StringPositionArgs::Decline => None,
            StringPositionArgs::Coerce(indices) => {
                // Every index is clamped to `arg_count` by `string_position_args`, so a
                // non-empty `indices` implies `out["arguments"]` exists as an array —
                // indexing it here can never synthesize a missing field.
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

/// Type-dispatch ONE string-position argument:
///
/// | Exasol type | Action |
/// |-------------|--------|
/// | `VARCHAR…` / `CHAR…` | unchanged — DataFusion needs no help with a string |
/// | `DATE` | `CAST(<col> AS VARCHAR)`, i.e. `YYYY-MM-DD` in both engines' defaults |
/// | `DECIMAL…` (incl. integer `DECIMAL(p,0)`) | #211's trimmed `decimal_to_varchar_exasol` |
/// | any other resolvable type, or a lookup miss | `None` — decline, fail-safe |
///
/// A non-`column` argument (a literal, a computed expression) is returned UNCHANGED:
/// its type is not resolvable from `col_types`, a tracked exception (#223) that must not
/// decline.
fn coerce_string_position_arg(arg: &Json, col_types: &[(String, String)]) -> Option<Json> {
    if arg.get("type").and_then(|t| t.as_str()) != Some("column") {
        return Some(arg.clone());
    }
    match column_exa_type(arg, col_types).map(classify_exa_type) {
        Some(ExaTypeClass::Character) => Some(arg.clone()),
        Some(ExaTypeClass::Date) => Some(wrap_cast_to_varchar(arg)),
        Some(ExaTypeClass::Decimal) => Some(wrap_decimal_to_varchar(arg)),
        // BOOLEAN, DOUBLE PRECISION, TIMESTAMP, … and an unresolvable type all decline: their
        // text forms diverge between the two engines, so a cast would convert a crash
        // into a wrong answer (same reasoning as `guard_like_subject`).
        Some(ExaTypeClass::Other) | None => None,
    }
}

/// Run the ordered type-rewrite pass sequence over a JSON expression tree, before it
/// is rendered or projected for the DataFusion scan: [`like_subject_type_guard`] →
/// [`string_function_arg_type_guard`] → [`rewrite_decimal_stringifications`]. One
/// ordered pass list serves every caller, whether the tree is a whole filter or a
/// single select-list item.
///
/// - [`like_subject_type_guard`] (issue #207): may decline the whole tree, or rewrap
///   a DATE subject.
/// - [`string_function_arg_type_guard`] (issue #210): coerces string-position
///   arguments, or declines.
/// - [`rewrite_decimal_stringifications`] (issue #211): runs last, never declines.
///
/// The string-function guard MUST run BEFORE the decimal rewrite, not after: a
/// coerced argument is no longer a bare column, so the decimal rewriter no-ops on it
/// instead of double-wrapping it into two trim wrappers — see
/// [`string_function_arg_type_guard`]'s doc for the full argument.
///
/// The three passes disagree on fallibility — the first two decline via `Option`,
/// the decimal rewrite never declines — so this function is also the fallibility
/// bridge: `?` propagates either guard's decline, and the infallible decimal pass's
/// plain `Json` return is wrapped in `Some`, sparing every caller from re-deriving
/// that bridge itself.
///
/// Returns:
/// - `Some(tree)` — this (possibly rewritten) tree is safe to render or project.
/// - `None` — a guard declined somewhere in the tree; the caller decides what a
///   decline means for its own render surface.
pub(super) fn apply_type_rewrites(expr: &Json, col_types: &[(String, String)]) -> Option<Json> {
    let expr = like_subject_type_guard(expr, col_types)?;
    let expr = string_function_arg_type_guard(&expr, col_types)?;
    Some(rewrite_decimal_stringifications(&expr, col_types))
}

/// The SOLE owner of "a tree the DataFusion scan may be handed": one
/// [`apply_type_rewrites`] pass AND renderability established on the tree that pass
/// produced. `Some(tree)` is that pushable tree; `None` means either half rejected it
/// — a guard declined, or the rewritten tree does not render.
///
/// Renderability is checked on the REWRITTEN tree, never the raw one, because the
/// rewritten tree is what a scan spec carries: screening the raw tree would let a
/// type-accepted-but-unrenderable tree pass the screen, then vanish when
/// [`render_df_filter_safe`] silently dropped it — pushed nowhere, which returns wrong
/// rows.
///
/// Every push decision asks this one function, whole-filter
/// ([`classify_where_filter`]) or per-conjunct
/// ([`type_screened_leg_filter`](super::joins::rendering::type_screened_leg_filter)),
/// so adding a pipeline pass or changing which renderability check gates a push is a
/// one-site edit instead of two surfaces that can silently disagree. What a `None`
/// MEANS stays each caller's own decision: decline the whole surface, or route that
/// one conjunct into a residual `WHERE`.
pub(super) fn type_accepted_rewrite(expr: &Json, col_types: &[(String, String)]) -> Option<Json> {
    apply_type_rewrites(expr, col_types).filter(datafusion_renderable)
}

/// Splits a request's raw WHERE filter into the predicate the scan spec carries and
/// the predicate the adapter must self-apply. Returns `(scan_filter, declined)`,
/// mutually exclusive; both `None` for no filter or a trivially-true one. Sole owner
/// of this classification (see `_decision/045`), while the acceptance predicate
/// underneath it belongs to [`type_accepted_rewrite`]. `declined` is the original,
/// un-rewritten tree — the type rewrites target the DataFusion dialect only.
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

/// Extract the projected columns and their Exasol types from the pushdown request.
///
/// For `column` nodes: returns the uppercase column name and its Exasol type.
/// For scalar expression nodes (e.g. `function_scalar`) and literals: renders via the VS
/// expression translator and returns the rendered SQL fragment, typed by the item's
/// declared `selectListDataTypes` entry (falling back to `VARCHAR(2000000)` only when the
/// declared type is absent).
/// If any select-list item can't be projected as-is (untranslatable scalar, or an
/// aggregate/unknown node), the whole projection falls back to the full base table
/// column set so Exasol can post-process the expression, GROUP BY, and aggregate —
/// correctness over pushdown. The returned projection is positional: exactly one
/// item per select-list item, in select-list order.
///
/// The third element is the widening signal: `true` means the derived projection is
/// the full base row, NOT one item per select-list item, so every consumer that owes
/// Exasol a select-list-shaped result must route the request elsewhere (#196).
pub(super) fn extract_projection(
    request: &Json,
    pushdown_req: &Json,
) -> Result<(Vec<ProjectionItem>, Vec<String>, bool), UdfError> {
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
///
/// The third element is the widening signal: `true` means the derived projection is
/// the full base row, NOT one item per select-list item — a select-list item was
/// untranslatable, EMITS-rejected, or a node type this function deliberately keeps
/// off the projection (an aggregate), so the whole select list widened. It is the
/// producer's own decision, piped out verbatim rather than re-derived downstream by
/// comparing the projection's arity against the select list's: the two coincide
/// whenever the base table happens to have as many columns as the query selects
/// (#196). A `None`/empty/non-array select list is NOT a widening — the full base row
/// is the correct answer there, and `false` keeps a genuine `SELECT *` on the scan
/// path.
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

    // If any item can't be projected as-is (untranslatable scalar, EMITS-rejected
    // declared type, or an aggregate/unknown node), we can't emit a per-item
    // projection — repeating `first_col_name` would yield duplicate EMITS names.
    // Instead project the full base row so Exasol has every column to post-process
    // the expression, GROUP BY, and aggregate itself. Declared out here, not inside
    // the select-list arm, because it is also this function's third return value —
    // the widening signal its callers route on (#196).
    let mut needs_full_fallback = false;
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
            for (i, e) in list.iter().enumerate() {
                let declared_type = declared_types
                    .and_then(|d| d.get(i))
                    .map(exasol_type_from_json);
                // On `None` the item can't be
                // safely pushed down at all; fall back to the full base row for the
                // whole select list, like every other "untranslatable item" arm below.
                // `project_columns` has THREE callers — `extract_projection`
                // (single-table), `extract_join_projection` (`joins/rendering.rs`,
                // whose `col_types` is the UNION of both joined tables' columns), and
                // `joins/mod.rs`'s empty-side path — so this decline reaches the
                // broadcast-join SELECT list too, not just the single-table path.
                let Some(e) = apply_type_rewrites(e, &all_cols) else {
                    needs_full_fallback = true;
                    continue;
                };
                let e = &e;
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
                    // This arm must list every remaining node type `render_expression_safe`
                    // renders that isn't already handled by an earlier arm in this match,
                    // EXCEPT `function_aggregate` — an aggregate must reach the aggregate
                    // planner, not be evaluated per shard as a projection item — and
                    // `predicate_greater` / `predicate_greaterequal`, which are unreachable
                    // here: Exasol normalises `a > b` to `b < a` (`capabilities.rs:29-30`),
                    // so a select-list `>` already arrives as `predicate_less` (#196).
                    "function_scalar"
                    | "function_scalar_cast"
                    // A `function_scalar_cast` of a bare DECIMAL column to VARCHAR/CHAR
                    // is rewritten above into a TOP-LEVEL `decimal_to_varchar_exasol`
                    // node; it must dispatch to the SAME `render_expression_safe` branch
                    // rather than falling into the `_ =>` full-row fallback (issue #211).
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

    Ok((proj_names, proj_types, needs_full_fallback))
}

/// Walk every `column` node reachable from `expr`, invoking `f` once per node found.
///
/// Owns both the recursion and the `type == "column"` test because every current
/// caller acts only on `column` nodes — pushing that test back into each caller's
/// closure would just relocate one duplication into three smaller ones. Traversal
/// is blind (every object field, every array element) rather than schema-aware,
/// because a collect rebuilds nothing: blind descent is what reaches a column
/// buried inside a `CASE` or a function call, and it is safe precisely because
/// nothing here reconstructs the tree. This MUST NOT be merged with issue #257's
/// curated post-order rewrite primitive — that primitive edits the tree in place
/// and so cannot blindly descend into a node's own `dataType`/`name` sub-objects,
/// which a rewrite must leave untouched but a collect may safely enter.
///
/// Case folding is deliberately NOT owned here — each callback applies its own, and the
/// current callers deliberately disagree: `collect_all_column_names` below folds with
/// Unicode `to_uppercase`, while `column_tables` and `collect_side_column_names`
/// in `joins/rendering.rs` fold with `to_ascii_uppercase`. Those two MUST NOT be unified.
/// They differ for non-ASCII identifiers — `ß` folds to `SS` under Unicode but stays `ß`
/// under ASCII — and that divergence is pinned by `column_collectors_keep_divergent_case_folding`
/// in `joins/rendering.rs`, which feeds `straße` through `collect_all_column_names` and
/// `collect_side_column_names` and asserts they diverge (`STRASSE` vs `STRAßE`).
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

/// Recursively collect every bare-column reference's UPPERCASE name found anywhere
/// within `value` into `names` — walking arrays, objects, and nested `expression`
/// wrappers alike, so it works uniformly over a `selectList`, a `filter`/`having`
/// expression tree, or a `groupBy`/`orderBy` array for both of this function's
/// callers (the topn hidden-column path and the join wrapper's column projection).
/// Walking every nested field, not just top-level entries, matters because a column
/// can be buried inside a function call or operator — e.g. `SUM(CASE WHEN
/// region='R' THEN 1 END)` — and a missed nested reference would leave the inner
/// scan without a column the wrapper's rendered SQL names.
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

/// Extract LIMIT from the pushdown request.
pub(super) fn extract_limit(pushdown_req: &Json) -> Option<u64> {
    pushdown_req
        .get("limit")
        .and_then(|l| l.get("numElements"))
        .and_then(|n| n.as_u64())
}

/// Extract the OFFSET from the pushdown request; 0 when absent.
///
/// Exasol sends `limit.offset` only once `LIMIT_WITH_OFFSET` is advertised, and
/// normalises an explicit `OFFSET 0` away entirely — so an absent key and a zero
/// offset are the same request (verified live). Sibling of [`extract_limit`]
/// rather than a widened return type: most call sites need the limit alone.
pub(super) fn extract_offset(pushdown_req: &Json) -> u64 {
    pushdown_req
        .get("limit")
        .and_then(|l| l.get("offset"))
        .and_then(|n| n.as_u64())
        .unwrap_or(0)
}

/// Render the trailing window clause for a wrapper SELECT: the single seam every
/// site that renders a final `LIMIT` shares.
///
/// A zero offset renders the pre-offset ` LIMIT {n}` string byte-for-byte, so an
/// already-correct plan cannot change shape. Callers must render their own
/// `ORDER BY` ahead of this: Exasol's grammar rejects an `OFFSET` without one.
///
/// The `(None, _)` arm silently drops a non-zero offset with no limit; this is
/// unreachable in production — Exasol's grammar ties `OFFSET` to a `LIMIT` (fact 4),
/// so a bare offset without one is rejected before it ever reaches this function.
pub(super) fn render_limit_offset(limit: Option<u64>, offset: u64) -> String {
    match (limit, offset) {
        (None, _) => String::new(),
        (Some(n), 0) => format!(" LIMIT {n}"),
        (Some(n), m) => format!(" LIMIT {n} OFFSET {m}"),
    }
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

/// Wrap `expr` in `CAST(... AS <declared>)` unless `declared` is absent or the
/// `VARCHAR(2000000)` default, so a pushdown output column's type matches what
/// Exasol validates positionally against `selectListDataTypes`. `VARCHAR(2000000)`
/// is exempt because it is the catch-all `crate::types::mapping` returns for any
/// Arrow type it cannot map (see `mapping.rs:22-28`), so its presence signals "no
/// usable declared type" rather than a type Exasol actually declared, and casting
/// to it would mislabel the value.
pub(super) fn cast_to_declared_type(expr: &str, declared: Option<&str>) -> String {
    match declared {
        Some(ty) if ty != "VARCHAR(2000000)" => format!("CAST({expr} AS {ty})"),
        _ => expr.to_string(),
    }
}

#[cfg(test)]
#[path = "support_tests.rs"]
mod tests;
