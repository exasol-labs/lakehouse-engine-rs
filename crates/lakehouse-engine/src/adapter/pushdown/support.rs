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
///
/// Table selection and case fold are TWO parameters rather than one mode flag because the
/// two callers' choices correlate by accident, not by design: [`extract_all_column_types`]
/// takes the first table and folds full-Unicode, while `involved_table_columns` (joins
/// planning) finds a table by name and folds ASCII-only. Deriving one from the other would
/// record that unreconciled divergence as intended behavior. `fold_case` preserves nothing
/// observable — `resolve_table_schema` Unicode-uppercases every column name before either
/// caller sees it — so it is tracked for removal, collapsing this builder to one fold
/// (#270).
pub(super) fn column_types(
    request: &Json,
    select_table: impl FnOnce(&[Json]) -> Option<&Json>,
    fold_case: impl Fn(&str) -> String,
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
                    let name = fold_case(c.get("name")?.as_str()?);
                    let dt_json = c.get("dataType")?;
                    Some((name, exasol_type_from_json(dt_json)))
                })
                .collect()
        })
        .unwrap_or_default()
}

/// Extract all columns and their Exasol types from the first involved table.
pub(super) fn extract_all_column_types(request: &Json) -> Vec<(String, String)> {
    column_types(request, |tables: &[Json]| tables.first(), str::to_uppercase)
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
/// WHY this exists: once the adapter's capabilities response advertises a
/// predicate or function shape, Exasol delegates it fully and never
/// independently re-checks or re-applies it — there is no Exasol-side
/// fallback. A predicate this returns `false` for therefore cannot be pushed
/// into the DataFusion-in-UDF scan spec; the caller MUST self-apply it in the
/// adapter's own returned SQL instead, never omit it. A trivially-true
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
/// select-list item is declined. Exasol never independently re-checks or re-applies
/// an advertised capability, so this is NOT a case where Exasol safely evaluates the
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
///   untranslatable-predicate backstop (`mod.rs:14-15`). Exasol never independently
///   re-checks or re-applies an advertised capability, so a decline here is NOT
///   safely deferred to Exasol — the caller must itself self-apply the declined
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
/// scan. `involved_table_columns`' ASCII-folded keys agree for every column name the
/// adapter can declare — `resolve_table_schema` Unicode-uppercases names before declaring
/// them, so no declarable name can differ between the two folds. `None` means the type is
/// not resolvable — the node carries no `name`, or its folded name is absent from the list
/// — two cases every caller already treats identically.
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
///   WHOLE select-list item is declined. Exasol never independently re-checks or
///   re-applies an advertised capability, so this decline is NOT safely deferred to
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

/// Split a request's raw WHERE filter into the predicate the DataFusion scan spec
/// carries and the predicate the adapter must render itself.
///
/// Returns `(scan_filter, declined)`. The two are mutually exclusive, and BOTH are
/// `None` for a request that carries no filter or one that renders trivially true —
/// omitting a no-op predicate is correct, not a decline.
///
/// The single owner of that classification, so `build_dispatch_sql` never re-derives
/// renderability and the trivially-true rule keeps its one owner in
/// `crates/vs-expression`. Splitting matters because there is no Exasol-side
/// fallback: once the capabilities response advertises a predicate shape Exasol
/// delegates it fully and never re-applies it, so a predicate this hands back as
/// `declined` MUST be self-applied in the adapter's own returned SQL — the pre-#279
/// code collapsed both outcomes into one `None` and silently returned unfiltered
/// rows.
///
/// `declined` is the ORIGINAL, un-rewritten tree: the type rewrites target the
/// DataFusion dialect, whereas the self-apply site renders Exasol. A guard declining
/// the rewrite is itself a decline — the scan cannot carry the predicate either way.
pub(super) fn classify_where_filter<'a>(
    filter_json_raw: Option<&'a Json>,
    col_types: &[(String, String)],
) -> (Option<String>, Option<&'a Json>) {
    let rewritten = filter_json_raw.and_then(|f| apply_type_rewrites(f, col_types));
    match (filter_json_raw, &rewritten) {
        (Some(raw), None) => (None, Some(raw)),
        (Some(raw), Some(tree)) if !datafusion_renderable(tree) => (None, Some(raw)),
        _ => (rewritten.as_ref().and_then(render_df_filter_safe), None),
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
/// under ASCII — and no test exercises `collect_all_column_names`, `collect_column_tables`,
/// or `collect_side_column_names` with a non-ASCII column name, so unifying their folds
/// would still pass the whole suite.
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

#[cfg(test)]
mod tests {
    use super::super::test_support::*;
    use super::*;
    use crate::scan::spec::{AggKind, DeleteFileContentType, SortKey};
    use vs_expression::render_df_filter_safe;

    /// `walk_column_nodes` fires its callback exactly once per `column` node
    /// wherever one is nested — inside a function's `arguments` array, a `CASE`'s
    /// `results`, a comparison predicate's `left`/`right`, and even a `column`
    /// node's own child object — and never for a non-`column` object, a scalar,
    /// or an array node itself.
    #[test]
    fn walk_column_nodes_visits_every_nested_column_node_once() {
        let expr = serde_json::json!({
            "type": "function_scalar",
            "name": "PLUS",
            "arguments": [
                {"type": "column", "name": "A", "tableName": "T"},
                {"type": "literal_exactnumeric", "value": 1}
            ],
            "case": {
                "type": "case",
                "results": [
                    {"type": "column", "name": "B"},
                    {"type": "literal_exactnumeric", "value": 2}
                ]
            },
            "predicate": {
                "type": "predicate_equal",
                "left": {"type": "column", "name": "C"},
                "right": {
                    "type": "column",
                    "name": "D",
                    "nested": {"type": "column", "name": "E"}
                }
            }
        });

        let mut visited = Vec::new();
        walk_column_nodes(&expr, &mut |map| {
            visited.push(
                map.get("name")
                    .and_then(|n| n.as_str())
                    .unwrap()
                    .to_string(),
            );
        });
        visited.sort();

        assert_eq!(
            visited,
            vec!["A", "B", "C", "D", "E"],
            "every column node must fire exactly once, including one nested inside another column node"
        );
    }

    /// `walk_column_nodes` never invokes its callback for a non-container root —
    /// `Json::Null`, a scalar string, or a scalar number fall through the `_ => {}`
    /// arm untouched, and an empty object matches `Json::Object` but has no `type`
    /// key and no values to recurse into, so it is a no-op too. Production hands
    /// the primitive exactly such roots unguarded: `referenced_column_projection`
    /// (`joins/sql_builders.rs`) and `referenced_side_columns` (`rendering.rs`)
    /// pass `pushdown_req.get("groupBy")` / `get("orderBy")` / `get("selectList")`
    /// straight through with no `is_null()` guard.
    #[test]
    fn walk_column_nodes_never_invokes_callback_for_a_non_container_root() {
        let mut invocations: usize = 0;

        walk_column_nodes(&serde_json::Value::Null, &mut |_| invocations += 1);
        walk_column_nodes(&serde_json::json!("REGION"), &mut |_| invocations += 1);
        walk_column_nodes(&serde_json::json!(7), &mut |_| invocations += 1);
        walk_column_nodes(&serde_json::json!({}), &mut |_| invocations += 1);

        assert_eq!(
            invocations, 0,
            "a null, scalar, or empty-object root must be a no-op: groupBy/orderBy/selectList reach walk_column_nodes unguarded"
        );
    }

    /// `strip_table_alias` removes every `tableAlias` key at any nesting depth
    /// (issue #193) while preserving `tableName` and `name`, recursing through
    /// both nested objects and arrays.
    #[test]
    fn strip_table_alias_removes_alias_preserves_table_name_and_name_recursively() {
        let expr = serde_json::json!({
            "type": "function_scalar",
            "name": "PLUS",
            "tableAlias": "O",
            "arguments": [
                {"type": "column", "name": "O_ORDERDATE", "tableName": "FACT_ORDERS", "tableAlias": "O"},
                {"type": "literal_exactnumeric", "value": 1}
            ]
        });

        let stripped = strip_table_alias(&expr);

        assert_eq!(
            stripped,
            serde_json::json!({
                "type": "function_scalar",
                "name": "PLUS",
                "arguments": [
                    {"type": "column", "name": "O_ORDERDATE", "tableName": "FACT_ORDERS"},
                    {"type": "literal_exactnumeric", "value": 1}
                ]
            }),
            "every tableAlias key must be gone at every depth, while name/tableName survive"
        );
    }

    /// A predicate the DataFusion dialect can express answers `true`.
    #[test]
    fn datafusion_renderable_true_for_a_rendering_predicate() {
        let expr = serde_json::json!({
            "type": "predicate_greater",
            "left": {"type": "column", "name": "AGE"},
            "right": {"type": "literal_exactnumeric", "value": 18}
        });

        assert!(datafusion_renderable(&expr));
    }

    /// `SECOND(ts, 3)` is a DataFusion field-shortcut arity refusal (exactly 1
    /// argument permitted) — the dialect-asymmetric decline this plan's fix
    /// exists to self-apply rather than silently omit.
    #[test]
    fn datafusion_renderable_false_for_second_with_precision_arity_decline() {
        let expr = serde_json::json!({
            "type": "function_scalar",
            "name": "SECOND",
            "arguments": [
                {"type": "column", "name": "TS"},
                {"type": "literal_exactnumeric", "value": 3}
            ]
        });

        assert!(!datafusion_renderable(&expr));
    }

    /// A trivially-true `TRUE` literal answers `true`: `render_expression_safe`
    /// does not suppress it, so omitting it from the scan spec is a correct
    /// no-op, not a decline.
    #[test]
    fn datafusion_renderable_true_for_trivially_true_literal() {
        let expr = serde_json::json!({"type": "literal_bool", "value": true});

        assert!(datafusion_renderable(&expr));
    }

    /// `strip_table_alias` must not change the decline/accept answer:
    /// `handle_pushdown` screens the un-stripped tree while the N-scan leg
    /// renders the `tableAlias`-stripped one, and both must agree. Covers both
    /// directions: a predicate that declines under both dialects (below), and
    /// one that RENDERS under both (below) — the safety-critical direction,
    /// since `build_side_fan_out_sql` strips the alias and re-renders AFTER
    /// `renderable_only` screened the un-stripped tree, so a conjunct whose
    /// answer flipped `true` -> `false` under stripping would be silently
    /// dropped from the leg.
    #[test]
    fn datafusion_renderable_answer_unchanged_by_strip_table_alias() {
        let with_alias = serde_json::json!({
            "type": "function_scalar",
            "name": "SECOND",
            "tableAlias": "O",
            "arguments": [
                {"type": "column", "name": "TS", "tableName": "T", "tableAlias": "O"},
                {"type": "literal_exactnumeric", "value": 3}
            ]
        });
        let stripped = strip_table_alias(&with_alias);

        assert_eq!(
            datafusion_renderable(&with_alias),
            datafusion_renderable(&stripped),
            "stripping tableAlias must not change whether the DataFusion dialect accepts the predicate"
        );
        assert!(
            !datafusion_renderable(&stripped),
            "SECOND(ts, 3) must still decline once table-alias-stripped"
        );

        let renders_with_alias = serde_json::json!({
            "type": "predicate_greater",
            "left": {"type": "column", "name": "TS", "tableName": "T", "tableAlias": "O"},
            "right": {"type": "literal_exactnumeric", "value": 1}
        });
        let renders_stripped = strip_table_alias(&renders_with_alias);

        assert_eq!(
            datafusion_renderable(&renders_with_alias),
            datafusion_renderable(&renders_stripped),
            "stripping tableAlias must not change whether a RENDERING predicate is still accepted"
        );
        assert!(
            datafusion_renderable(&renders_stripped),
            "TS > 1 must still render once table-alias-stripped"
        );
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
    // kept out of it (untranslatable) — never mistranslated, never omitted from
    // the query.
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
        // render_df_filter_safe returning None keeps it out of the SCAN SPEC
        // only — the adapter must still self-apply it elsewhere (see
        // declined_filter_routes_every_dispatch_shape_to_qualified_wrapper).
        let untranslatable = serde_json::json!({"type": "fn_custom_agg", "args": []});
        let omitted = render_df_filter_safe(&untranslatable);
        assert!(
            omitted.is_none(),
            "untranslatable predicate must be omitted (None), not mistranslated"
        );

        // Confirm the scan SQL stays valid without a scan-spec filter — the
        // adapter applies the predicate itself elsewhere rather than relying
        // on any re-check by Exasol (see
        // declined_filter_routes_every_dispatch_shape_to_qualified_wrapper).
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

    /// `extract_offset` is a sibling accessor of `extract_limit`: 0 when the
    /// `offset` key is absent (the shape Exasol sends for a bare `LIMIT n` and,
    /// verified live, also for an explicit `OFFSET 0`), the value otherwise.
    #[test]
    fn offset_extracted_from_pushdown_request() {
        assert_eq!(extract_offset(&serde_json::json!({})), 0);
        assert_eq!(
            extract_offset(&serde_json::json!({"limit": {"numElements": 42}})),
            0
        );
        assert_eq!(extract_offset(&serde_json::json!({"offset": 3})), 0);
        assert_eq!(
            extract_offset(&serde_json::json!({"limit": {"numElements": 12, "offset": 3}})),
            3
        );
    }

    /// The one rendering seam every reachable wrapper SELECT routes through.
    /// The `offset == 0` arm MUST stay byte-identical to the pre-change
    /// ` LIMIT {n}` splice — every existing SQL-shape assertion depends on it.
    #[test]
    fn render_limit_offset_covers_absent_zero_and_nonzero_offset() {
        assert_eq!(render_limit_offset(None, 0), "");
        assert_eq!(render_limit_offset(None, 3), "");

        for n in [0_u64, 1, 12, u64::MAX] {
            assert_eq!(render_limit_offset(Some(n), 0), format!(" LIMIT {n}"));
        }

        assert_eq!(render_limit_offset(Some(12), 3), " LIMIT 12 OFFSET 3");
        assert_eq!(render_limit_offset(Some(0), 3), " LIMIT 0 OFFSET 3");
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

        let (names, types, _widened) = extract_projection(&request, &pushdown_req).unwrap();

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
            None,
            &[],
            &[],
            SCAN_UDF_NAME,
            DISTRIBUTE_FILES_UDF_NAME,
        )
    }

    /// The aggregate merge SELECT renders `LIMIT n` on the outer wrapper when
    /// `request_limit` is `Some(n)` — the render site issue #198 needs so a pushed
    /// `LIMIT 0` over a one-row aggregate merge returns zero rows instead of being
    /// silently dropped (no limit value reachable inside the aggregate sub-path
    /// carried the request's raw limit before this parameter existed).
    #[test]
    fn aggregate_merge_renders_request_limit_when_some() {
        let plans = vec![AggregatePlan {
            kind: AggKind::Count,
            column: None,
            arg_expr: None,
        }];
        let spec_template = ScanSpec {
            common: CommonScanSpec {
                aggregates: Some(plans),
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
            Some(0),
            &[],
            &[],
            SCAN_UDF_NAME,
            DISTRIBUTE_FILES_UDF_NAME,
        );
        assert!(
            sql.ends_with("LIMIT 0"),
            "aggregate merge must render the request LIMIT: {sql}"
        );
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
            None,
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
        let (proj_cols, proj_types, _widened) =
            extract_projection(&request, &pushdown_req).unwrap();
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
        let (proj_cols, _proj_types, _widened) =
            extract_projection(&request, &pushdown_req).unwrap();
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
        let (proj_cols, _proj_types, _widened) =
            extract_projection(&request, &pushdown_req).unwrap();
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
        let (proj_cols, _proj_types, _widened) =
            extract_projection(&request, &pushdown_req).unwrap();
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
        let (proj_cols, proj_types, _widened) =
            extract_projection(&request, &pushdown_req).unwrap();
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
        let (proj_cols, proj_types, _widened) =
            extract_projection(&request, &pushdown_req).unwrap();
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
        let (proj_cols, _proj_types, _widened) =
            extract_projection(&request, &pushdown_req).unwrap();

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
        let (proj_cols, _proj_types, _widened) =
            extract_projection(&request, &pushdown_req).unwrap();
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
        let (proj_cols, proj_types, _widened) =
            extract_projection(&request, &pushdown_req).unwrap();
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
        let (proj_cols, proj_types, _widened) =
            extract_projection(&request, &pushdown_req).unwrap();
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
        let (proj_cols, proj_types, _widened) =
            extract_projection(&request, &pushdown_req).unwrap();
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
    // Scenario: like_subject_type_guard dispatches LIKE subjects by Exasol type
    // (issue #207 regression coverage).
    // ---------------------------------------------------------------------------

    /// Scenario: LIKE on a VARCHAR or CHAR column pushes down unchanged.
    #[test]
    fn like_guard_varchar_subject_unchanged() {
        let filter = serde_json::json!({
            "type": "predicate_like",
            "expression": {"type": "column", "name": "name"},
            "pattern": {"type": "literal_string", "value": "A%"}
        });
        let col_types = vec![("NAME".to_string(), "VARCHAR(2000000)".to_string())];

        let result = like_subject_type_guard(&filter, &col_types);
        assert_eq!(
            result,
            Some(filter),
            "VARCHAR subject must be returned unchanged"
        );
    }

    /// Scenario: LIKE on a DATE column pushes down wrapped in CAST-to-VARCHAR.
    /// Regression: under the pre-fix code (`filter_json_raw.and_then(render_df_filter_safe)`
    /// with no guard) the tree is never mutated, so `expression` would still be the bare
    /// `column` node — this assertion on the rewritten `function_scalar_cast` shape is
    /// false under that old behavior.
    #[test]
    fn like_guard_date_subject_wraps_cast() {
        let column = serde_json::json!({"type": "column", "name": "signup_date"});
        let filter = serde_json::json!({
            "type": "predicate_like",
            "expression": column.clone(),
            "pattern": {"type": "literal_string", "value": "2024%"}
        });
        let col_types = vec![("SIGNUP_DATE".to_string(), "DATE".to_string())];

        let result = like_subject_type_guard(&filter, &col_types);
        let expected = serde_json::json!({
            "type": "predicate_like",
            "expression": {
                "type": "function_scalar_cast",
                "name": "CAST",
                "dataType": {"type": "VARCHAR"},
                "arguments": [column]
            },
            "pattern": {"type": "literal_string", "value": "2024%"}
        });
        assert_eq!(
            result,
            Some(expected),
            "DATE subject must be rewrapped in CAST(<col> AS VARCHAR)"
        );
    }

    /// Scenario: LIKE on a DECIMAL column declines the whole filter.
    /// Regression: the pre-fix code has no decline mechanism at this layer — a DECIMAL
    /// subject's `filter_json_raw` would pass straight through into `Some(...)` unmodified,
    /// so asserting `None` here is false under that old behavior.
    #[test]
    fn like_guard_decimal_subject_declines() {
        let filter = serde_json::json!({
            "type": "predicate_like",
            "expression": {"type": "column", "name": "amount"},
            "pattern": {"type": "literal_string", "value": "9%"}
        });
        let col_types = vec![("AMOUNT".to_string(), "DECIMAL(9,2)".to_string())];

        let result = like_subject_type_guard(&filter, &col_types);
        assert_eq!(
            result, None,
            "DECIMAL subject must decline the whole filter"
        );
    }

    /// Scenario: a `predicate_like` over a DECIMAL-typed column pins that
    /// [`apply_type_rewrites`] declines it — matching
    /// `like_guard_decimal_subject_declines` above, which calls the guard directly
    /// rather than through the pipeline. This test fails if the LIKE pass is ever
    /// dropped from the pipeline; one pipeline now serves both render surfaces.
    #[test]
    fn type_rewrite_pipeline_runs_like_guard() {
        let filter = serde_json::json!({
            "type": "predicate_like",
            "expression": {"type": "column", "name": "amount"},
            "pattern": {"type": "literal_string", "value": "9%"}
        });
        let col_types = vec![("AMOUNT".to_string(), "DECIMAL(9,2)".to_string())];

        assert_eq!(
            apply_type_rewrites(&filter, &col_types),
            None,
            "the type-rewrite pipeline's LIKE-subject guard must decline a DECIMAL subject"
        );
    }

    /// Scenario: LIKE on an integer column declines the whole filter. Exasol has no
    /// separate wire "INTEGER" type — an integer column arrives as `DECIMAL(20,0)`
    /// (confirmed via live payload capture this session).
    #[test]
    fn like_guard_integer_subject_declines() {
        let filter = serde_json::json!({
            "type": "predicate_like",
            "expression": {"type": "column", "name": "quantity"},
            "pattern": {"type": "literal_string", "value": "1%"}
        });
        let col_types = vec![("QUANTITY".to_string(), "DECIMAL(20,0)".to_string())];

        let result = like_subject_type_guard(&filter, &col_types);
        assert_eq!(
            result, None,
            "integer (DECIMAL(20,0)) subject must decline the whole filter"
        );
    }

    /// Scenario: LIKE on a non-column subject (a computed scalar expression) is left
    /// untouched, regardless of `col_types` — this is out of scope for the guard.
    #[test]
    fn like_guard_non_column_subject_untouched() {
        let filter = serde_json::json!({
            "type": "predicate_like",
            "expression": {
                "type": "function_scalar",
                "name": "UPPER",
                "arguments": [{"type": "column", "name": "amount"}]
            },
            "pattern": {"type": "literal_string", "value": "A%"}
        });
        // Even a DECIMAL entry for the underlying column must not trigger a decline,
        // since the LIKE subject itself is not a bare column.
        let col_types = vec![("AMOUNT".to_string(), "DECIMAL(9,2)".to_string())];

        let result = like_subject_type_guard(&filter, &col_types);
        assert_eq!(
            result,
            Some(filter),
            "non-column LIKE subject must be left unchanged"
        );
    }

    /// Scenario: LIKE on a bare column whose name is not present in `col_types` (a
    /// lookup miss) declines the whole filter (fail-safe).
    #[test]
    fn like_guard_unresolvable_column_declines() {
        let filter = serde_json::json!({
            "type": "predicate_like",
            "expression": {"type": "column", "name": "mystery"},
            "pattern": {"type": "literal_string", "value": "A%"}
        });
        let col_types = vec![("OTHER".to_string(), "VARCHAR(2000000)".to_string())];

        let result = like_subject_type_guard(&filter, &col_types);
        assert_eq!(
            result, None,
            "unresolvable column subject must decline the whole filter"
        );
    }

    /// Scenario: a nested non-string LIKE (inside a `predicate_and`) declines the
    /// entire enclosing filter, not just the LIKE sub-node.
    #[test]
    fn like_guard_nested_decimal_declines_whole_filter() {
        let filter = serde_json::json!({
            "type": "predicate_and",
            "expressions": [
                {
                    "type": "predicate_equal",
                    "left": {"type": "column", "name": "status"},
                    "right": {"type": "literal_string", "value": "OPEN"}
                },
                {
                    "type": "predicate_and",
                    "expressions": [
                        {
                            "type": "predicate_like",
                            "expression": {"type": "column", "name": "amount"},
                            "pattern": {"type": "literal_string", "value": "9%"}
                        }
                    ]
                }
            ]
        });
        let col_types = vec![
            ("STATUS".to_string(), "VARCHAR(2000000)".to_string()),
            ("AMOUNT".to_string(), "DECIMAL(9,2)".to_string()),
        ];

        let result = like_subject_type_guard(&filter, &col_types);
        assert_eq!(
            result, None,
            "a nested non-string LIKE must decline the whole top-level filter"
        );
    }

    /// Route `filter` through the production classification and then through the
    /// qualified single-table wrapper, returning the emitted SQL.
    ///
    /// Asserts the fixture genuinely declines before rendering: these tests are about
    /// WHERE a declined predicate ends up, so a fixture that quietly renders would
    /// assert nothing. The inner scan projects the whole `col_types` universe, which is
    /// what the production decline route passes as its projection override.
    fn declined_filter_wrapper_sql(filter: &Json, col_types: &[(String, String)]) -> String {
        let (scan_filter, declined) = classify_where_filter(Some(filter), col_types);
        assert!(
            scan_filter.is_none(),
            "fixture precondition: a declining filter must never reach the scan spec: \
             {scan_filter:?}"
        );
        let declined = declined.expect(
            "fixture precondition: a declining filter must be handed back for self-applying",
        );
        let request = serde_json::json!({"involvedTables": [{"name": "T"}]});
        let pushdown_req = serde_json::json!({"filter": filter.clone()});
        let fan_out_spec = ScanSpec {
            common: CommonScanSpec {
                projection: col_types
                    .iter()
                    .map(|(name, _)| ProjectionItem::Column(name.clone()))
                    .collect(),
                emit_exa_types: col_types.iter().map(|(_, ty)| ty.clone()).collect(),
                storage: sample_storage(),
                ..Default::default()
            },
            files: vec![],
        };
        super::super::joins::build_qualified_single_table_fallback_sql(
            &request,
            &pushdown_req,
            &fan_out_spec,
            &[vec![("s3://warehouse/f0.parquet".to_string(), 1u64)]],
            SCAN_UDF_NAME,
            DISTRIBUTE_FILES_UDF_NAME,
            Some(declined),
        )
        .expect("the wrapper must render the declined predicate")
    }

    /// The declined half of `like_guard_nested_decimal_declines_whole_filter`, carried
    /// through to its consequence: a nested non-string LIKE declines the ENTIRE
    /// enclosing filter, and that whole filter — the renderable `STATUS = 'OPEN'`
    /// conjunct included — is then applied by the adapter in the wrapper's `WHERE`, not
    /// omitted. Exasol re-applies nothing it delegated, so omitting either conjunct
    /// would return rows the query excludes.
    #[test]
    fn nested_like_decline_routes_to_wrapper_where() {
        let filter = serde_json::json!({
            "type": "predicate_and",
            "expressions": [
                {
                    "type": "predicate_equal",
                    "left": {"type": "column", "name": "STATUS", "tableName": "T"},
                    "right": {"type": "literal_string", "value": "OPEN"},
                },
                {
                    "type": "predicate_like",
                    "expression": {"type": "column", "name": "AMOUNT", "tableName": "T"},
                    "pattern": {"type": "literal_string", "value": "9%"},
                },
            ],
        });
        let col_types = vec![
            ("STATUS".to_string(), "VARCHAR(2000000)".to_string()),
            ("AMOUNT".to_string(), "DECIMAL(9,2)".to_string()),
        ];

        let sql = declined_filter_wrapper_sql(&filter, &col_types);

        let where_at = sql
            .find(r#"AS "LHS_T0" WHERE "#)
            .unwrap_or_else(|| panic!("the declined filter must reach the wrapper: {sql}"));
        assert!(
            sql[where_at..].contains(r#""LHS_T0"."AMOUNT" LIKE '9%'"#)
                && sql[where_at..].contains(r#""LHS_T0"."STATUS" = 'OPEN'"#),
            "the wrapper WHERE must carry the WHOLE declined filter, both conjuncts: {sql}"
        );
    }

    /// The declined half of `like_guard_integer_subject_declines`, carried through to
    /// its consequence: an integer column arrives as `DECIMAL(20,0)`, the LIKE declines
    /// the filter for the scan, and the adapter applies it itself in the wrapper.
    #[test]
    fn declined_like_on_integer_column_routes_to_wrapper_where() {
        let filter = serde_json::json!({
            "type": "predicate_like",
            "expression": {"type": "column", "name": "QUANTITY", "tableName": "T"},
            "pattern": {"type": "literal_string", "value": "1%"},
        });
        let col_types = vec![("QUANTITY".to_string(), "DECIMAL(20,0)".to_string())];

        let sql = declined_filter_wrapper_sql(&filter, &col_types);

        assert!(
            sql.contains(r#"AS "LHS_T0" WHERE ("LHS_T0"."QUANTITY" LIKE '1%')"#),
            "the integer-column LIKE must be applied in the wrapper WHERE: {sql}"
        );
    }

    /// The declined half of `like_guard_unresolvable_column_declines`, carried through
    /// to its consequence: an unresolvable subject type is a FAIL-SAFE decline, and a
    /// fail-safe decline must still self-apply. It cannot omit the predicate on the
    /// grounds that it could not type it — that reasoning is what returned unfiltered
    /// rows.
    ///
    /// The fixture is deliberately unreachable from Exasol, which only sends columns of
    /// the request's own `involvedTables`, so the assertion is about the routing
    /// decision alone: the emitted SQL names a column this artificial `col_types`
    /// universe does not project.
    #[test]
    fn declined_like_on_unresolvable_column_routes_to_wrapper_where() {
        let filter = serde_json::json!({
            "type": "predicate_like",
            "expression": {"type": "column", "name": "MYSTERY", "tableName": "T"},
            "pattern": {"type": "literal_string", "value": "A%"},
        });
        let col_types = vec![("OTHER".to_string(), "VARCHAR(2000000)".to_string())];

        let sql = declined_filter_wrapper_sql(&filter, &col_types);

        assert!(
            sql.contains(r#"AS "LHS_T0" WHERE ("LHS_T0"."MYSTERY" LIKE 'A%')"#),
            "an unresolvable-subject decline must still be applied by the adapter, \
             never omitted: {sql}"
        );
    }

    /// Scenario: REGEXP_LIKE on a DATE column pushes down wrapped in CAST-to-VARCHAR,
    /// same as `predicate_like` — both node types dispatch through `guard_like_subject`.
    #[test]
    fn like_guard_regexp_date_subject_wraps_cast() {
        let column = serde_json::json!({"type": "column", "name": "signup_date"});
        let filter = serde_json::json!({
            "type": "predicate_like_regexp",
            "expression": column.clone(),
            "pattern": {"type": "literal_string", "value": "2024.*"}
        });
        let col_types = vec![("SIGNUP_DATE".to_string(), "DATE".to_string())];

        let result = like_subject_type_guard(&filter, &col_types);
        let expected = serde_json::json!({
            "type": "predicate_like_regexp",
            "expression": {
                "type": "function_scalar_cast",
                "name": "CAST",
                "dataType": {"type": "VARCHAR"},
                "arguments": [column]
            },
            "pattern": {"type": "literal_string", "value": "2024.*"}
        });
        assert_eq!(
            result,
            Some(expected),
            "REGEXP_LIKE DATE subject must be rewrapped in CAST(<col> AS VARCHAR)"
        );
    }

    /// Scenario: a DECIMAL-typed LIKE wrapped in `predicate_not` declines the whole
    /// filter — the decline must propagate through `predicate_not`, not just through
    /// `predicate_and`/`predicate_or`.
    #[test]
    fn like_guard_not_wrapped_decimal_declines() {
        let filter = serde_json::json!({
            "type": "predicate_not",
            "expression": {
                "type": "predicate_like",
                "expression": {"type": "column", "name": "amount"},
                "pattern": {"type": "literal_string", "value": "9%"}
            }
        });
        let col_types = vec![("AMOUNT".to_string(), "DECIMAL(9,2)".to_string())];

        let result = like_subject_type_guard(&filter, &col_types);
        assert_eq!(
            result, None,
            "a DECIMAL LIKE inside predicate_not must decline the whole filter"
        );
    }

    /// Regression (#207 blind spot): a DECIMAL-typed LIKE buried inside a
    /// `function_scalar_case`'s `arguments` (a WHEN condition) must decline the whole
    /// filter, same as a LIKE nested under `predicate_and`/`predicate_or`/`predicate_not` —
    /// a `LIKE` at this non-junction position is type-guarded like any other.
    #[test]
    fn like_guard_decimal_inside_case_declines() {
        let filter = serde_json::json!({
            "type": "function_scalar_case",
            "name": "CASE",
            "arguments": [
                {
                    "type": "predicate_like",
                    "expression": {"type": "column", "name": "amount"},
                    "pattern": {"type": "literal_string", "value": "9%"}
                }
            ],
            "results": [
                {"type": "literal_exactnumeric", "value": 1},
                {"type": "literal_exactnumeric", "value": 0}
            ]
        });
        let col_types = vec![("AMOUNT".to_string(), "DECIMAL(9,2)".to_string())];

        let result = like_subject_type_guard(&filter, &col_types);
        assert_eq!(
            result, None,
            "a DECIMAL LIKE buried in a function_scalar_case's arguments must decline \
             the whole filter: {result:?}"
        );
    }

    /// Regression (#207 blind spot): a DATE-typed LIKE buried inside a
    /// `function_scalar_case`'s `arguments` (a WHEN condition) is rewritten in place as
    /// `CAST(<col> AS VARCHAR)`, with the enclosing CASE structure (its `results`
    /// THEN/ELSE branches) preserved unchanged — a `LIKE` at this non-junction position
    /// is type-guarded like any other.
    #[test]
    fn like_guard_date_inside_case_wraps_cast() {
        let column = serde_json::json!({"type": "column", "name": "signup_date"});
        let then_branch = serde_json::json!({"type": "literal_exactnumeric", "value": 1});
        let else_branch = serde_json::json!({"type": "literal_exactnumeric", "value": 0});
        let filter = serde_json::json!({
            "type": "function_scalar_case",
            "name": "CASE",
            "arguments": [
                {
                    "type": "predicate_like",
                    "expression": column.clone(),
                    "pattern": {"type": "literal_string", "value": "2024%"}
                }
            ],
            "results": [then_branch.clone(), else_branch.clone()]
        });
        let col_types = vec![("SIGNUP_DATE".to_string(), "DATE".to_string())];

        let result = like_subject_type_guard(&filter, &col_types);
        let expected = serde_json::json!({
            "type": "function_scalar_case",
            "name": "CASE",
            "arguments": [
                {
                    "type": "predicate_like",
                    "expression": {
                        "type": "function_scalar_cast",
                        "name": "CAST",
                        "dataType": {"type": "VARCHAR"},
                        "arguments": [column]
                    },
                    "pattern": {"type": "literal_string", "value": "2024%"}
                }
            ],
            "results": [then_branch, else_branch]
        });
        assert_eq!(
            result,
            Some(expected),
            "a DATE LIKE buried in a function_scalar_case's arguments must be rewrapped \
             in CAST(<col> AS VARCHAR) in place, with the CASE's results preserved: {result:?}"
        );
    }

    /// The widened traversal must not cost a working pushdown: a VARCHAR-typed LIKE
    /// buried inside a `function_scalar_case`'s `arguments` is now reached (it was not,
    /// pre-migration), but since VARCHAR needs no rewrap the returned tree must equal
    /// the input tree exactly, byte for byte.
    #[test]
    fn like_guard_varchar_inside_case_unchanged() {
        let filter = serde_json::json!({
            "type": "function_scalar_case",
            "name": "CASE",
            "arguments": [
                {
                    "type": "predicate_like",
                    "expression": {"type": "column", "name": "name"},
                    "pattern": {"type": "literal_string", "value": "A%"}
                }
            ],
            "results": [
                {"type": "literal_exactnumeric", "value": 1},
                {"type": "literal_exactnumeric", "value": 0}
            ]
        });
        let col_types = vec![("NAME".to_string(), "VARCHAR(20)".to_string())];

        let result = like_subject_type_guard(&filter, &col_types);
        assert_eq!(
            result,
            Some(filter),
            "a VARCHAR LIKE buried in a function_scalar_case's arguments must be \
             returned unchanged: {result:?}"
        );
    }

    // ---------------------------------------------------------------------------
    // rewrite_expr_tree — the shared post-order traversal primitive
    // ---------------------------------------------------------------------------

    /// Post-order: when `f` runs on a node, that node's curated children are
    /// already their rewritten selves. Proven without interior mutability — the
    /// closure copies the child's (rewritten) type onto the parent, so the
    /// assertion can only hold if the child was rewritten first.
    #[test]
    fn expr_tree_applies_f_to_children_before_their_parent() {
        let tree = serde_json::json!({
            "type": "outer",
            "expression": {"type": "inner"},
        });

        let out = rewrite_expr_tree(&tree, &|node| {
            let mut out = node.clone();
            if node.get("type").and_then(|t| t.as_str()) == Some("inner") {
                out["type"] = Json::from("inner_rewritten");
            } else {
                out["child_type_seen"] = node["expression"]["type"].clone();
            }
            Some(out)
        })
        .expect("an always-Some closure must never decline");

        assert_eq!(
            out["child_type_seen"],
            Json::from("inner_rewritten"),
            "the parent must see its already-rewritten child: {out}"
        );
    }

    /// A `None` from `f` at any depth declines the WHOLE tree — it propagates out
    /// through every enclosing level instead of dropping only the declining
    /// subtree.
    #[test]
    fn expr_tree_decline_deep_in_the_tree_propagates_to_the_root() {
        let tree = serde_json::json!({
            "type": "root",
            "expressions": [
                {"type": "keep"},
                {"type": "branch", "left": {"type": "decline_here"}},
            ],
        });

        let out = rewrite_expr_tree(&tree, &|node| {
            if node.get("type").and_then(|t| t.as_str()) == Some("decline_here") {
                return None;
            }
            Some(node.clone())
        });

        assert_eq!(
            out, None,
            "a declined descendant must decline the whole tree"
        );
    }

    /// Only the curated fields are descended into, and only in the shapes the
    /// grammar sends: an array field must be a `Json::Array`, a single-child field
    /// must be an object. A node's object-valued `dataType` sub-object is never
    /// handed to `f`, so no guard can rewrite a declared type; `name` is excluded
    /// too, since it always carries a bare identifier string, never an object.
    #[test]
    fn expr_tree_recurses_only_into_curated_fields_of_the_expected_shape() {
        let tree = serde_json::json!({
            "type": "root",
            "dataType": {"type": "VARCHAR"},
            "arguments": {"type": "not_an_array"},
            "pattern": "not_an_object",
            "expression": {"type": "curated_single"},
            "results": [{"type": "curated_array"}],
        });

        let out = rewrite_expr_tree(&tree, &|node| {
            let mut out = node.clone();
            out["visited"] = Json::Bool(true);
            Some(out)
        })
        .expect("an always-Some closure must never decline");

        assert_eq!(
            out["expression"]["visited"],
            Json::Bool(true),
            "curated field `expression` must be descended into: {out}"
        );
        assert_eq!(
            out["results"][0]["visited"],
            Json::Bool(true),
            "curated field `results` must be descended into: {out}"
        );
        for skipped in ["dataType", "arguments"] {
            assert_eq!(
                out[skipped]["visited"],
                Json::Null,
                "`{skipped}` must not be descended into: {out}"
            );
        }
        assert_eq!(
            out["pattern"],
            Json::from("not_an_object"),
            "a non-object single-child field must be left untouched: {out}"
        );
    }

    /// A non-object node reaches `f` too: the primitive has no leaf early-return,
    /// which is what lets the migrated walkers drop theirs.
    #[test]
    fn expr_tree_applies_f_to_a_non_object_node() {
        for leaf in [
            Json::Null,
            serde_json::json!("UPPER"),
            serde_json::json!(7),
            serde_json::json!([1, 2]),
        ] {
            assert_eq!(
                rewrite_expr_tree(&leaf, &|_| Some(Json::from("touched"))),
                Some(Json::from("touched")),
                "a non-object node must be handed to `f`: {leaf}"
            );
        }
    }

    // ---------------------------------------------------------------------------
    // rewrite_decimal_stringifications — issue #211 decimal-trim JSON rewrite
    // ---------------------------------------------------------------------------

    /// The column-type map shared by the rewrite and string-function-guard tests: one
    /// DECIMAL, one integer DECIMAL(p,0), one VARCHAR, one DATE, plus the three
    /// resolvable-but-non-coercible types the string-function guard must decline on
    /// (issue #210). The three additions cannot disturb the #211 rewrite assertions:
    /// those reference only the first four names, and every wired `project_columns`
    /// test here projects a single expression rather than the full base row.
    fn decimal_rewrite_col_types() -> Vec<(String, String)> {
        vec![
            ("C_DECIMAL_A".to_string(), "DECIMAL(10,2)".to_string()),
            ("ID".to_string(), "DECIMAL(20,0)".to_string()),
            ("NAME".to_string(), "VARCHAR(2000000)".to_string()),
            ("D".to_string(), "DATE".to_string()),
            ("C_DOUBLE_A".to_string(), "DOUBLE PRECISION".to_string()),
            ("C_BOOL_A".to_string(), "BOOLEAN".to_string()),
            ("C_TS_A".to_string(), "TIMESTAMP".to_string()),
        ]
    }

    fn decimal_column() -> Json {
        serde_json::json!({"type": "column", "name": "c_decimal_a"})
    }

    fn cast_to(target: &str, arg: Json) -> Json {
        serde_json::json!({
            "type": "function_scalar_cast",
            "name": "CAST",
            "dataType": {"type": target},
            "arguments": [arg],
        })
    }

    /// A non-object node is returned unchanged: `rewrite_expr_tree` finds no curated
    /// child on it, so the always-`Some` closure's catch-all arm clones it.
    #[test]
    fn decimal_rewrite_passes_through_non_object_node() {
        let col_types = decimal_rewrite_col_types();
        for node in [
            Json::Null,
            serde_json::json!("UPPER"),
            serde_json::json!(7),
            serde_json::json!([1, 2]),
        ] {
            assert_eq!(
                rewrite_decimal_stringifications(&node, &col_types),
                node.clone(),
                "a non-object node must be passed through: {node}"
            );
        }
    }

    /// `CAST(<decimal column> AS VARCHAR)` → the WHOLE cast node is replaced by a
    /// `decimal_to_varchar_exasol` node wrapping the column, which renders through
    /// `format_decimal_exasol_style` (the trailing-zero-trim regexp form).
    #[test]
    fn rewrite_cast_decimal_to_varchar_replaces_whole_node() {
        let node = cast_to("VARCHAR", decimal_column());
        let out = rewrite_decimal_stringifications(&node, &decimal_rewrite_col_types());

        assert_eq!(
            out.get("type").and_then(|t| t.as_str()),
            Some("decimal_to_varchar_exasol"),
            "the whole CAST node must be replaced, not nested: {out}"
        );
        let inner = &out["arguments"][0];
        assert_eq!(
            inner.get("name").and_then(|n| n.as_str()),
            Some("c_decimal_a"),
            "the wrapped node must be the original column: {out}"
        );
        // Rendering proves it goes through the trimming regexp form.
        let sql = render_expression_safe(&out).expect("must render");
        assert!(
            sql.contains(r#"CAST("C_DECIMAL_A" AS VARCHAR)"#) && sql.contains("regexp_replace"),
            "must render via format_decimal_exasol_style: {sql}"
        );
    }

    /// `CAST(<decimal column> AS CHAR)` is also a stringification → replaced.
    #[test]
    fn rewrite_cast_decimal_to_char_replaces_whole_node() {
        let node = cast_to("CHAR", decimal_column());
        let out = rewrite_decimal_stringifications(&node, &decimal_rewrite_col_types());
        assert_eq!(
            out.get("type").and_then(|t| t.as_str()),
            Some("decimal_to_varchar_exasol"),
            "CAST AS CHAR over a DECIMAL column must also be rewritten: {out}"
        );
    }

    /// The exact nested-CONCAT shape from the live capture
    /// (`CONCAT(ID, CONCAT('-', C_DECIMAL_A))`, i.e. `id||'-'||c_decimal_a`): ONLY
    /// `C_DECIMAL_A` (reachable only through the INNER CONCAT) gets wrapped; the
    /// non-decimal `ID` column and the `'-'` literal are untouched, and the nested
    /// structure is otherwise preserved. Guards the post-order-recursion BLOCKER.
    #[test]
    fn rewrite_nested_concat_wraps_only_inner_decimal() {
        let node = serde_json::json!({
            "type": "function_scalar",
            "name": "CONCAT",
            "arguments": [
                {"type": "column", "name": "id"},
                {
                    "type": "function_scalar",
                    "name": "CONCAT",
                    "arguments": [
                        {"type": "literal_string", "value": "-"},
                        {"type": "column", "name": "c_decimal_a"}
                    ]
                }
            ]
        });
        let out = rewrite_decimal_stringifications(&node, &decimal_rewrite_col_types());

        // Outer CONCAT preserved; ID (integer DECIMAL(20,0)) is itself decimal so it
        // IS wrapped as a direct outer-CONCAT argument — but the OUTER structure and
        // its argument count are preserved.
        assert_eq!(out.get("name").and_then(|n| n.as_str()), Some("CONCAT"));
        let outer_args = out["arguments"].as_array().unwrap();
        assert_eq!(
            outer_args.len(),
            2,
            "outer CONCAT arg count preserved: {out}"
        );

        // The inner CONCAT node is still a CONCAT; its literal '-' is untouched and
        // its C_DECIMAL_A argument is now wrapped.
        let inner = &outer_args[1];
        assert_eq!(inner.get("name").and_then(|n| n.as_str()), Some("CONCAT"));
        let inner_args = inner["arguments"].as_array().unwrap();
        assert_eq!(
            inner_args[0].get("type").and_then(|t| t.as_str()),
            Some("literal_string"),
            "the '-' literal must be untouched: {out}"
        );
        assert_eq!(
            inner_args[1].get("type").and_then(|t| t.as_str()),
            Some("decimal_to_varchar_exasol"),
            "the inner C_DECIMAL_A must be wrapped (post-order recursion reached it): {out}"
        );
        assert_eq!(
            inner_args[1]["arguments"][0]
                .get("name")
                .and_then(|n| n.as_str()),
            Some("c_decimal_a"),
        );
    }

    /// `CONCAT(NAME, C_DECIMAL_A)` — only the DECIMAL column is wrapped; the VARCHAR
    /// column is left as a bare column reference.
    #[test]
    fn rewrite_concat_wraps_only_decimal_leaves_varchar() {
        let node = serde_json::json!({
            "type": "function_scalar",
            "name": "CONCAT",
            "arguments": [
                {"type": "column", "name": "name"},
                {"type": "column", "name": "c_decimal_a"}
            ]
        });
        let out = rewrite_decimal_stringifications(&node, &decimal_rewrite_col_types());
        let args = out["arguments"].as_array().unwrap();
        assert_eq!(
            args[0].get("type").and_then(|t| t.as_str()),
            Some("column"),
            "the VARCHAR column must stay a bare column: {out}"
        );
        assert_eq!(
            args[1].get("type").and_then(|t| t.as_str()),
            Some("decimal_to_varchar_exasol"),
            "the DECIMAL column must be wrapped: {out}"
        );
    }

    /// `LENGTH(<decimal column>)` → its single argument is wrapped.
    #[test]
    fn rewrite_length_wraps_decimal_argument() {
        let node = serde_json::json!({
            "type": "function_scalar",
            "name": "LENGTH",
            "arguments": [decimal_column()]
        });
        let out = rewrite_decimal_stringifications(&node, &decimal_rewrite_col_types());
        assert_eq!(out.get("name").and_then(|n| n.as_str()), Some("LENGTH"));
        assert_eq!(
            out["arguments"][0].get("type").and_then(|t| t.as_str()),
            Some("decimal_to_varchar_exasol"),
            "LENGTH's DECIMAL argument must be wrapped: {out}"
        );
    }

    /// A non-DECIMAL bare column as a CAST / CONCAT / LENGTH argument is left
    /// COMPLETELY unchanged (VARCHAR and DATE both).
    #[test]
    fn rewrite_non_decimal_argument_unchanged() {
        let col_types = decimal_rewrite_col_types();

        let cast_varchar = cast_to(
            "VARCHAR",
            serde_json::json!({"type": "column", "name": "name"}),
        );
        assert_eq!(
            rewrite_decimal_stringifications(&cast_varchar, &col_types),
            cast_varchar,
            "CAST of a VARCHAR column must be unchanged"
        );

        let length_date = serde_json::json!({
            "type": "function_scalar",
            "name": "LENGTH",
            "arguments": [{"type": "column", "name": "d"}]
        });
        assert_eq!(
            rewrite_decimal_stringifications(&length_date, &col_types),
            length_date,
            "LENGTH of a DATE column must be unchanged"
        );
    }

    /// A computed-expression argument (e.g. `c_decimal_a * 2`) to a stringifier is
    /// left unchanged — its type is not resolvable from `col_types`, a tracked
    /// exception in the plan's scope. The argument is not a bare column, so neither
    /// the CAST replacement nor the CONCAT per-argument wrap fires on it.
    #[test]
    fn rewrite_computed_expression_argument_unchanged() {
        let computed = serde_json::json!({
            "type": "function_scalar",
            "name": "MULT",
            "arguments": [decimal_column(), {"type": "literal_exactnumeric", "value": 2}]
        });
        let col_types = decimal_rewrite_col_types();

        let cast = cast_to("VARCHAR", computed.clone());
        assert_eq!(
            rewrite_decimal_stringifications(&cast, &col_types),
            cast,
            "CAST of a computed DECIMAL expression must be left unchanged: it is not a bare column"
        );

        let concat = serde_json::json!({
            "type": "function_scalar",
            "name": "CONCAT",
            "arguments": [{"type": "column", "name": "name"}, computed]
        });
        assert_eq!(
            rewrite_decimal_stringifications(&concat, &col_types),
            concat,
            "a computed-expression CONCAT argument must be left unchanged"
        );
    }

    /// A DECIMAL column in a NON-stringifying context is NOT wrapped: neither a
    /// comparison predicate (`c_decimal_a > 5`) nor a CAST to a non-string target
    /// (`CAST(c_decimal_a AS DOUBLE)`). Proves the recursion does not over-wrap.
    #[test]
    fn rewrite_non_stringifying_context_unchanged() {
        let col_types = decimal_rewrite_col_types();

        let cmp = serde_json::json!({
            "type": "predicate_greater",
            "left": decimal_column(),
            "right": {"type": "literal_exactnumeric", "value": 5}
        });
        assert_eq!(
            rewrite_decimal_stringifications(&cmp, &col_types),
            cmp,
            "a DECIMAL column in a comparison must not be wrapped"
        );

        let cast_double = cast_to("DOUBLE", decimal_column());
        assert_eq!(
            rewrite_decimal_stringifications(&cast_double, &col_types),
            cast_double,
            "CAST(decimal AS DOUBLE) must not be wrapped"
        );
    }

    /// A DECIMAL stringification reachable ONLY through a `function_scalar_case` THEN
    /// branch (its `results` field) is still found and wrapped — proves the generic
    /// child recursion covers CASE's `results`, not just `arguments`.
    #[test]
    fn rewrite_reaches_decimal_inside_case_then_branch() {
        let node = serde_json::json!({
            "type": "function_scalar_case",
            "name": "CASE",
            "arguments": [
                {
                    "type": "predicate_greater",
                    "left": {"type": "column", "name": "id"},
                    "right": {"type": "literal_exactnumeric", "value": 0}
                }
            ],
            "results": [
                {
                    "type": "function_scalar",
                    "name": "CONCAT",
                    "arguments": [
                        {"type": "literal_string", "value": "x"},
                        {"type": "column", "name": "c_decimal_a"}
                    ]
                }
            ]
        });
        let out = rewrite_decimal_stringifications(&node, &decimal_rewrite_col_types());

        let then_concat = &out["results"][0];
        assert_eq!(
            then_concat.get("name").and_then(|n| n.as_str()),
            Some("CONCAT"),
            "the CASE THEN CONCAT must be preserved: {out}"
        );
        assert_eq!(
            then_concat["arguments"][1]
                .get("type")
                .and_then(|t| t.as_str()),
            Some("decimal_to_varchar_exasol"),
            "the DECIMAL inside the CASE THEN CONCAT must be wrapped: {out}"
        );
    }

    /// Wiring sanity check: a select-list `CAST(c_decimal_a AS VARCHAR(20))` over a
    /// bare DECIMAL column must
    /// route through `render_expression_safe` — yielding a SINGLE `ProjectionItem::Expr`
    /// carrying the trim, at the item's declared EMITS type — NOT degrade to the full
    /// base-row fallback. This proves both wiring changes: the unconditional rewrite of
    /// each select-list item AND `decimal_to_varchar_exasol` being recognized by the
    /// scalar `item_type` match arm.
    #[test]
    fn selectlist_decimal_cast_routed_not_full_row_fallback() {
        let pushdown_req = serde_json::json!({
            "selectList": [ cast_to("VARCHAR", decimal_column()) ],
            "selectListDataTypes": [ {"type": "VARCHAR", "size": 20} ],
        });
        let (items, types, _widened) =
            project_columns(&pushdown_req, decimal_rewrite_col_types()).expect("must project");

        assert_eq!(
            items.len(),
            1,
            "the CAST-to-VARCHAR item must project to a single expression, not the full base row: {items:?}"
        );
        let ProjectionItem::Expr { expr } = &items[0] else {
            panic!(
                "must be a rendered expression, not a bare column / full-row fallback: {items:?}"
            );
        };
        assert!(
            expr.contains(r#"CAST("C_DECIMAL_A" AS VARCHAR)"#) && expr.contains("regexp_replace"),
            "the projected expression must render the trimmed DECIMAL→string form: {expr}"
        );
        assert_eq!(
            types,
            vec!["VARCHAR(20)".to_string()],
            "the EMITS type must stay the item's declared selectListDataTypes type"
        );
    }

    /// Exhaustive coverage: the exact nested-CONCAT JSON shape confirmed
    /// live for `id||'-'||c_decimal_a` (`CONCAT(ID, CONCAT('-', C_DECIMAL_A))`) as a
    /// select-list item, through `project_columns`, renders `C_DECIMAL_A`'s CAST
    /// fragment specifically wrapped in `format_decimal_exasol_style`'s
    /// `regexp_replace` pair — proving the wiring (not just the isolated JSON rewrite
    /// already covered by `rewrite_nested_concat_wraps_only_inner_decimal`) reaches
    /// the nested inner-CONCAT argument at the `project_columns` level.
    ///
    /// `ID` is itself `DECIMAL(20,0)`, so it too is a direct outer-CONCAT argument and
    /// gets the same (harmless, no-op-on-scale-0) trim wrapper — documented behavior
    /// already asserted in `rewrite_nested_concat_wraps_only_inner_decimal`. This test
    /// only asserts what's specific to `C_DECIMAL_A`: its CAST fragment sits inside
    /// the trim wrapper.
    #[test]
    fn selectlist_nested_concat_decimal_arg_rewritten() {
        let item = serde_json::json!({
            "type": "function_scalar",
            "name": "CONCAT",
            "arguments": [
                {"type": "column", "name": "id"},
                {
                    "type": "function_scalar",
                    "name": "CONCAT",
                    "arguments": [
                        {"type": "literal_string", "value": "-"},
                        decimal_column()
                    ]
                }
            ]
        });
        let pushdown_req = serde_json::json!({
            "selectList": [ item ],
            "selectListDataTypes": [ {"type": "VARCHAR", "size": 2000000} ],
        });
        let (items, _types, _widened) =
            project_columns(&pushdown_req, decimal_rewrite_col_types()).expect("must project");

        assert_eq!(
            items.len(),
            1,
            "the nested-CONCAT item must project to a single expression, not the full base row: {items:?}"
        );
        let ProjectionItem::Expr { expr } = &items[0] else {
            panic!(
                "must be a rendered expression, not a bare column / full-row fallback: {items:?}"
            );
        };
        assert!(
            expr.contains(r#"regexp_replace(regexp_replace(CAST("C_DECIMAL_A" AS VARCHAR)"#),
            "the inner C_DECIMAL_A argument must be rendered through the trim wrapper: {expr}"
        );
    }

    /// Exhaustive coverage: `LENGTH(c_decimal_a)` as a select-list item,
    /// through `project_columns`, renders the trim-wrapped `character_length(...)` —
    /// the LENGTH-over-DECIMAL wiring at the projection level (mirrors
    /// `rewrite_length_wraps_decimal_argument`'s isolated JSON check).
    #[test]
    fn selectlist_length_decimal_arg_rewritten() {
        let item = serde_json::json!({
            "type": "function_scalar",
            "name": "LENGTH",
            "arguments": [decimal_column()]
        });
        let pushdown_req = serde_json::json!({
            "selectList": [ item ],
            "selectListDataTypes": [ {"type": "DECIMAL", "precision": 18, "scale": 0} ],
        });
        let (items, _types, _widened) =
            project_columns(&pushdown_req, decimal_rewrite_col_types()).expect("must project");

        assert_eq!(
            items.len(),
            1,
            "the LENGTH item must project to a single expression, not the full base row: {items:?}"
        );
        let ProjectionItem::Expr { expr } = &items[0] else {
            panic!(
                "must be a rendered expression, not a bare column / full-row fallback: {items:?}"
            );
        };
        assert!(
            expr.contains(
                "character_length(regexp_replace(regexp_replace(CAST(\"C_DECIMAL_A\" AS VARCHAR)"
            ),
            "LENGTH over a DECIMAL column must render the trim-wrapped character_length: {expr}"
        );
    }

    /// Exhaustive coverage: `CAST(<VARCHAR column> AS VARCHAR(20))` through
    /// `project_columns` renders EXACTLY as it did before this whole fix — a plain
    /// CAST, with no `regexp_replace` / `decimal_to_varchar_exasol` involvement.
    /// Proves the fix doesn't touch a non-DECIMAL stringification at the wired level.
    #[test]
    fn stringify_nondecimal_column_unchanged() {
        let pushdown_req = serde_json::json!({
            "selectList": [ cast_to("VARCHAR", serde_json::json!({"type": "column", "name": "name"})) ],
            "selectListDataTypes": [ {"type": "VARCHAR", "size": 20} ],
        });
        let (items, _types, _widened) =
            project_columns(&pushdown_req, decimal_rewrite_col_types()).expect("must project");

        assert_eq!(
            items.len(),
            1,
            "must project a single expression: {items:?}"
        );
        let ProjectionItem::Expr { expr } = &items[0] else {
            panic!("must be a rendered expression, not a full-row fallback: {items:?}");
        };
        assert_eq!(
            expr, r#"CAST("NAME" AS VARCHAR)"#,
            "a CAST over a non-DECIMAL column must render unchanged, exactly as before this fix: {expr}"
        );
    }

    /// Exhaustive coverage: `CAST(c_decimal_a * 2 AS VARCHAR)` through
    /// `project_columns` renders a plain, untrimmed CAST — proving the adapter-level
    /// wiring correctly leaves the tracked-exception computed-argument case alone
    /// (issue #223's scope), consistent with
    /// `rewrite_computed_expression_argument_unchanged` at the wired level.
    #[test]
    fn stringify_computed_decimal_arg_untouched() {
        let computed = serde_json::json!({
            "type": "function_scalar",
            "name": "MULT",
            "arguments": [decimal_column(), {"type": "literal_exactnumeric", "value": 2}]
        });
        let pushdown_req = serde_json::json!({
            "selectList": [ cast_to("VARCHAR", computed) ],
            "selectListDataTypes": [ {"type": "VARCHAR", "size": 2000000} ],
        });
        let (items, _types, _widened) =
            project_columns(&pushdown_req, decimal_rewrite_col_types()).expect("must project");

        assert_eq!(
            items.len(),
            1,
            "must project a single expression: {items:?}"
        );
        let ProjectionItem::Expr { expr } = &items[0] else {
            panic!("must be a rendered expression, not a full-row fallback: {items:?}");
        };
        assert_eq!(
            expr, r#"CAST(("C_DECIMAL_A" * 2) AS VARCHAR)"#,
            "a CAST of a computed DECIMAL expression must render unchanged: {expr}"
        );
        assert!(
            !expr.contains("regexp_replace"),
            "a computed-expression CAST must not be trimmed (tracked exception #223): {expr}"
        );
    }

    // ---------------------------------------------------------------------------
    // project_columns wiring — issue #210 string_function_arg_type_guard, run
    // BEFORE rewrite_decimal_stringifications on every select-list item
    // ---------------------------------------------------------------------------

    /// Scenario: `UPPER(c_decimal_a)` projects to a SINGLE expression carrying the
    /// trimmed decimal-to-string form (#211's node, reached through the new guard),
    /// at the item's declared `selectListDataTypes` type — not the full base row.
    #[test]
    fn selectlist_upper_decimal_arg_coerced_not_full_row() {
        let item = string_fn("UPPER", vec![decimal_column()]);
        let pushdown_req = serde_json::json!({
            "selectList": [ item ],
            "selectListDataTypes": [ {"type": "VARCHAR", "size": 2000000} ],
        });
        let (items, types, _widened) =
            project_columns(&pushdown_req, decimal_rewrite_col_types()).expect("must project");

        assert_eq!(
            items.len(),
            1,
            "UPPER(c_decimal_a) must project a single expression, not the full base row: {items:?}"
        );
        let ProjectionItem::Expr { expr } = &items[0] else {
            panic!("must be a rendered expression, not a full-row fallback: {items:?}");
        };
        assert!(
            expr.contains(r#"upper(regexp_replace(regexp_replace(CAST("C_DECIMAL_A" AS VARCHAR)"#),
            "UPPER's DECIMAL argument must render through the trimmed decimal-to-string form: {expr}"
        );
        assert_eq!(
            types,
            vec!["VARCHAR(2000000)".to_string()],
            "the EMITS type must stay the item's declared selectListDataTypes type"
        );
    }

    /// Scenario: `LOWER(c_date)` (the `d` fixture column, `DATE`-typed) projects a
    /// single expression containing `CAST("D" AS VARCHAR)`.
    #[test]
    fn selectlist_lower_date_arg_cast_to_varchar() {
        let item = string_fn("LOWER", vec![column("d")]);
        let pushdown_req = serde_json::json!({
            "selectList": [ item ],
            "selectListDataTypes": [ {"type": "VARCHAR", "size": 2000000} ],
        });
        let (items, _types, _widened) =
            project_columns(&pushdown_req, decimal_rewrite_col_types()).expect("must project");

        assert_eq!(
            items.len(),
            1,
            "LOWER(c_date) must project a single expression, not the full base row: {items:?}"
        );
        let ProjectionItem::Expr { expr } = &items[0] else {
            panic!("must be a rendered expression, not a full-row fallback: {items:?}");
        };
        assert!(
            expr.contains(r#"CAST("D" AS VARCHAR)"#),
            "LOWER's DATE argument must be wrapped in CAST(<col> AS VARCHAR): {expr}"
        );
    }

    /// Scenario: `UPPER(c_double)` (the `c_double_a` fixture column) degrades to the
    /// FULL base row with no error — `string_function_arg_type_guard` declines a
    /// resolvable-but-non-coercible column type, and `project_columns` falls back
    /// exactly like any other untranslatable select-list item.
    #[test]
    fn selectlist_string_fn_over_double_falls_back_to_full_row() {
        let col_types = decimal_rewrite_col_types();
        let item = string_fn("UPPER", vec![column("c_double_a")]);
        let pushdown_req = serde_json::json!({
            "selectList": [ item ],
            "selectListDataTypes": [ {"type": "VARCHAR", "size": 2000000} ],
        });
        let (items, types, _widened) =
            project_columns(&pushdown_req, col_types.clone()).expect("must project");

        assert_eq!(
            items.len(),
            col_types.len(),
            "UPPER(c_double_a) must fall back to the full base row, not a truncated projection: {items:?}"
        );
        let expected_names: Vec<ProjectionItem> = col_types
            .iter()
            .map(|(n, _)| ProjectionItem::Column(n.clone()))
            .collect();
        assert_eq!(
            items, expected_names,
            "the full-row fallback must project every base column unchanged"
        );
        let expected_types: Vec<String> = col_types.iter().map(|(_, t)| t.clone()).collect();
        assert_eq!(types, expected_types);
    }

    /// Scenario: `INSTR(c_decimal_a, '.')` projects a single expression whose FIRST
    /// `strpos` argument is the trimmed decimal form and whose SECOND argument is the
    /// untouched string literal `'.'` — `INSTR(string, substring)` -> `strpos(string,
    /// substring)`, so index 0 is the column being coerced, index 1 the literal left
    /// alone since it is not a bare column.
    #[test]
    fn selectlist_instr_decimal_arg_coerces_first_position_only() {
        let item = string_fn(
            "INSTR",
            vec![
                decimal_column(),
                serde_json::json!({"type": "literal_string", "value": "."}),
            ],
        );
        let pushdown_req = serde_json::json!({
            "selectList": [ item ],
            "selectListDataTypes": [ {"type": "DECIMAL", "precision": 18, "scale": 0} ],
        });
        let (items, _types, _widened) =
            project_columns(&pushdown_req, decimal_rewrite_col_types()).expect("must project");

        assert_eq!(
            items.len(),
            1,
            "INSTR(c_decimal_a, '.') must project a single expression, not the full base row: {items:?}"
        );
        let ProjectionItem::Expr { expr } = &items[0] else {
            panic!("must be a rendered expression, not a full-row fallback: {items:?}");
        };
        assert!(
            expr.starts_with(
                r#"strpos(regexp_replace(regexp_replace(CAST("C_DECIMAL_A" AS VARCHAR)"#
            ),
            "INSTR's first (string) argument must render the trimmed decimal form: {expr}"
        );
        assert!(
            expr.ends_with("'.')"),
            "INSTR's second (substring) argument, a literal, must be left untouched: {expr}"
        );
    }

    /// Scenario: `INSTR(c_varchar, 'b', 3)` (three arguments, all effectively
    /// VARCHAR/literal) degrades to the FULL base row rather than projecting a
    /// truncated `strpos` call — the #228 arity-decline path. `vs-expression` reads
    /// only `args[0]`/`args[1]` and silently drops the third; coercing index 0 here
    /// would let a truncated rendering plan successfully, so
    /// `string_position_args("INSTR", 3)` returns `Decline` regardless of every
    /// argument already being VARCHAR/literal.
    #[test]
    fn selectlist_instr_with_start_position_falls_back_to_full_row() {
        let col_types = decimal_rewrite_col_types();
        let item = string_fn(
            "INSTR",
            vec![
                column("name"),
                serde_json::json!({"type": "literal_string", "value": "b"}),
                serde_json::json!({"type": "literal_exactnumeric", "value": 3}),
            ],
        );
        let pushdown_req = serde_json::json!({
            "selectList": [ item ],
            "selectListDataTypes": [ {"type": "DECIMAL", "precision": 18, "scale": 0} ],
        });
        let (items, _types, _widened) =
            project_columns(&pushdown_req, col_types.clone()).expect("must project");

        assert_eq!(
            items.len(),
            col_types.len(),
            "the arity-decline INSTR must fall back to the full base row, not a truncated strpos: {items:?}"
        );
        let expected_names: Vec<ProjectionItem> = col_types
            .iter()
            .map(|(n, _)| ProjectionItem::Column(n.clone()))
            .collect();
        assert_eq!(
            items, expected_names,
            "the full-row fallback must project every base column unchanged"
        );
    }

    // ---------------------------------------------------------------------------
    // Select-list predicate node types added to the pushable whitelist (#196)
    // ---------------------------------------------------------------------------

    /// Each whitelisted select-list predicate node type (issue #196) renders as a
    /// positional `ProjectionItem::Expr` carrying the rendered SQL fragment and the
    /// declared `selectListDataTypes` type — not the full-base-row fallback.
    #[test]
    fn selectlist_predicate_node_projects_as_expr() {
        let cases: Vec<(&str, serde_json::Value, &str)> = vec![
            (
                "predicate_in_constlist",
                serde_json::json!({
                    "type": "predicate_in_constlist",
                    "expression": column("name"),
                    "arguments": [
                        {"type": "literal_string", "value": "a"},
                        {"type": "literal_string", "value": "b"},
                    ]
                }),
                r#"("NAME" IN ('a', 'b'))"#,
            ),
            (
                "predicate_between",
                serde_json::json!({
                    "type": "predicate_between",
                    "expression": column("id"),
                    "left": {"type": "literal_exactnumeric", "value": 1},
                    "right": {"type": "literal_exactnumeric", "value": 10},
                }),
                r#"("ID" BETWEEN 1 AND 10)"#,
            ),
            (
                "predicate_is_null",
                serde_json::json!({
                    "type": "predicate_is_null",
                    "expression": column("name"),
                }),
                r#"("NAME" IS NULL)"#,
            ),
            (
                "predicate_is_not_null",
                serde_json::json!({
                    "type": "predicate_is_not_null",
                    "expression": column("name"),
                }),
                r#"("NAME" IS NOT NULL)"#,
            ),
            (
                "predicate_notequal",
                serde_json::json!({
                    "type": "predicate_notequal",
                    "left": column("id"),
                    "right": {"type": "literal_exactnumeric", "value": 5},
                }),
                r#"("ID" <> 5)"#,
            ),
            (
                "predicate_like_regexp",
                serde_json::json!({
                    "type": "predicate_like_regexp",
                    "expression": column("name"),
                    "pattern": {"type": "literal_string", "value": "^a.*"},
                }),
                r#"regexp_like("NAME", '^a.*')"#,
            ),
        ];

        for (node_type, item, expected_frag) in cases {
            let pushdown_req = serde_json::json!({
                "selectList": [ item ],
                "selectListDataTypes": [ {"type": "boolean"} ],
            });
            let (items, types, _widened) =
                project_columns(&pushdown_req, decimal_rewrite_col_types())
                    .unwrap_or_else(|e| panic!("[{node_type}] must project: {e}"));

            assert_eq!(
                items.len(),
                1,
                "[{node_type}] must project a single expression, not the full base row: {items:?}"
            );
            let ProjectionItem::Expr { expr } = &items[0] else {
                panic!(
                    "[{node_type}] must be a rendered expression, not a full-row fallback: {items:?}"
                );
            };
            assert_eq!(
                expr, expected_frag,
                "[{node_type}] rendered fragment mismatch"
            );
            assert_eq!(
                types,
                vec!["BOOLEAN".to_string()],
                "[{node_type}] declared type mismatch"
            );
        }
    }

    /// A `function_aggregate` select-list item still widens to the full base row —
    /// pinning the whitelist's one deliberate exclusion (#196) as intentional, not
    /// incidental: an aggregate must reach the aggregate planner, not be evaluated
    /// per shard as a projection item.
    #[test]
    fn selectlist_function_aggregate_still_widens_to_full_row() {
        let item = serde_json::json!({
            "type": "function_aggregate",
            "name": "COUNT",
            "arguments": [],
            "distinct": false
        });
        let col_types = decimal_rewrite_col_types();
        let pushdown_req = serde_json::json!({
            "selectList": [ item ],
            "selectListDataTypes": [ {"type": "decimal", "precision": 20, "scale": 0} ],
        });
        let (items, types, widened) =
            project_columns(&pushdown_req, col_types.clone()).expect("must project");

        assert!(
            widened,
            "the widening must be REPORTED, not only performed: the dispatcher routes on \
             this flag alone (#196)"
        );
        assert_eq!(
            items.len(),
            col_types.len(),
            "function_aggregate must widen to the full base row, not project as an Expr: {items:?}"
        );
        let expected_names: Vec<ProjectionItem> = col_types
            .iter()
            .map(|(n, _)| ProjectionItem::Column(n.clone()))
            .collect();
        assert_eq!(
            items, expected_names,
            "the full-row fallback must project every base column unchanged"
        );
        let expected_types: Vec<String> = col_types.iter().map(|(_, t)| t.clone()).collect();
        assert_eq!(types, expected_types);
    }

    // ---------------------------------------------------------------------------
    // like_subject_type_guard wired into apply_type_rewrites — issue #219
    // select-list LIKE type coercion
    // ---------------------------------------------------------------------------

    /// Scenario: `predicate_like` over `d` (`DATE`) projects a SINGLE expression that
    /// rewraps the subject as `CAST("D" AS VARCHAR)`, mirroring the filter pipeline's
    /// DATE arm — not the full base row.
    #[test]
    fn selectlist_like_over_date_projects_cast_expr() {
        let item = serde_json::json!({
            "type": "predicate_like",
            "expression": column("d"),
            "pattern": {"type": "literal_string", "value": "2024%"}
        });
        let pushdown_req = serde_json::json!({
            "selectList": [ item ],
            "selectListDataTypes": [ {"type": "boolean"} ],
        });
        let (items, types, widened) =
            project_columns(&pushdown_req, decimal_rewrite_col_types()).expect("must project");

        assert!(
            !widened,
            "a DATE LIKE subject rewraps, it must not widen to the full base row"
        );
        assert_eq!(
            items.len(),
            1,
            "the DATE LIKE item must project a single expression, not the full base row: {items:?}"
        );
        let ProjectionItem::Expr { expr } = &items[0] else {
            panic!("must be a rendered expression, not a full-row fallback: {items:?}");
        };
        assert!(
            expr.contains(r#"CAST("D" AS VARCHAR)"#) && expr.contains("LIKE"),
            "the DATE subject must be rewrapped in CAST(<col> AS VARCHAR) before the LIKE: {expr}"
        );
        assert_eq!(types, vec!["BOOLEAN".to_string()]);
    }

    /// Scenario: a `predicate_like`/`predicate_like_regexp` over a subject that
    /// resolves to a non-string Exasol type (DECIMAL, integer DECIMAL(p,0), DOUBLE,
    /// BOOLEAN, TIMESTAMP) or does not resolve at all widens the WHOLE select list to
    /// the full base row — `Ok`, never `Err`. Mirrors
    /// `like_guard_decimal_subject_declines`'s dispatch table, now proven wired through
    /// `project_columns`.
    #[test]
    fn selectlist_like_over_non_string_subject_falls_back_to_full_row() {
        let col_types = decimal_rewrite_col_types();
        let cases: Vec<(&str, Json)> = vec![
            (
                "c_decimal_a (DECIMAL(10,2))",
                serde_json::json!({
                    "type": "predicate_like",
                    "expression": column("c_decimal_a"),
                    "pattern": {"type": "literal_string", "value": "1%"}
                }),
            ),
            (
                "id (DECIMAL(20,0), integer)",
                serde_json::json!({
                    "type": "predicate_like",
                    "expression": column("id"),
                    "pattern": {"type": "literal_string", "value": "1%"}
                }),
            ),
            (
                "c_double_a (DOUBLE PRECISION)",
                serde_json::json!({
                    "type": "predicate_like",
                    "expression": column("c_double_a"),
                    "pattern": {"type": "literal_string", "value": "1%"}
                }),
            ),
            (
                "c_bool_a (BOOLEAN)",
                serde_json::json!({
                    "type": "predicate_like",
                    "expression": column("c_bool_a"),
                    "pattern": {"type": "literal_string", "value": "1%"}
                }),
            ),
            (
                "c_ts_a (TIMESTAMP)",
                serde_json::json!({
                    "type": "predicate_like",
                    "expression": column("c_ts_a"),
                    "pattern": {"type": "literal_string", "value": "1%"}
                }),
            ),
            (
                "unresolvable column name",
                serde_json::json!({
                    "type": "predicate_like",
                    "expression": column("not_a_column"),
                    "pattern": {"type": "literal_string", "value": "1%"}
                }),
            ),
            (
                "predicate_like_regexp over c_decimal_a",
                serde_json::json!({
                    "type": "predicate_like_regexp",
                    "expression": column("c_decimal_a"),
                    "pattern": {"type": "literal_string", "value": "^1.*"}
                }),
            ),
        ];

        for (label, item) in cases {
            let pushdown_req = serde_json::json!({
                "selectList": [ item ],
                "selectListDataTypes": [ {"type": "boolean"} ],
            });
            let (items, types, widened) = project_columns(&pushdown_req, col_types.clone())
                .unwrap_or_else(|e| panic!("[{label}] must project (Ok), not error: {e}"));

            assert!(
                widened,
                "[{label}] a non-string LIKE subject must widen to the full base row"
            );
            assert_eq!(
                items.len(),
                col_types.len(),
                "[{label}] must fall back to the full base row, not a truncated projection: {items:?}"
            );
            let expected_names: Vec<ProjectionItem> = col_types
                .iter()
                .map(|(n, _)| ProjectionItem::Column(n.clone()))
                .collect();
            assert_eq!(
                items, expected_names,
                "[{label}] the full-row fallback must project every base column unchanged"
            );
            let expected_types: Vec<String> = col_types.iter().map(|(_, t)| t.clone()).collect();
            assert_eq!(types, expected_types, "[{label}] EMITS types mismatch");
        }
    }

    /// Scenario: a `predicate_like` over `c_decimal_a` nested inside a
    /// `function_scalar_case` still widens to the full base row — pinning that the
    /// guard's [`rewrite_expr_tree`] reach (a LIKE buried under a CASE, not only a
    /// bare top-level select-list item) is wired all the way through
    /// `project_columns`, not just the isolated `like_subject_type_guard` call.
    #[test]
    fn selectlist_like_inside_case_over_decimal_falls_back_to_full_row() {
        let col_types = decimal_rewrite_col_types();
        let case_expr = serde_json::json!({
            "type": "function_scalar_case",
            "name": "CASE",
            "arguments": [
                {
                    "type": "predicate_like",
                    "expression": column("c_decimal_a"),
                    "pattern": {"type": "literal_string", "value": "1%"}
                }
            ],
            "results": [
                {"type": "literal_string", "value": "yes"},
                {"type": "literal_string", "value": "no"}
            ]
        });
        let pushdown_req = serde_json::json!({
            "selectList": [ case_expr ],
            "selectListDataTypes": [ {"type": "VARCHAR", "size": 2000000} ],
        });
        let (items, types, widened) =
            project_columns(&pushdown_req, col_types.clone()).expect("must project (Ok)");

        assert!(
            widened,
            "a LIKE nested inside a CASE over a DECIMAL subject must widen the whole select list"
        );
        assert_eq!(
            items.len(),
            col_types.len(),
            "must fall back to the full base row, not a truncated projection: {items:?}"
        );
        let expected_names: Vec<ProjectionItem> = col_types
            .iter()
            .map(|(n, _)| ProjectionItem::Column(n.clone()))
            .collect();
        assert_eq!(
            items, expected_names,
            "the full-row fallback must project every base column unchanged"
        );
        let expected_types: Vec<String> = col_types.iter().map(|(_, t)| t.clone()).collect();
        assert_eq!(types, expected_types);
    }

    // ---------------------------------------------------------------------------
    // string_position_args — issue #210 string-position argument table
    // ---------------------------------------------------------------------------

    /// Every argument of `CONCAT`/`TRIM`/`LTRIM`/`RTRIM`/`REPLACE`/`TRANSLATE` is a
    /// string position, at every arity Exasol can send.
    #[test]
    fn string_position_args_coerces_every_argument_of_all_string_functions() {
        for name in ["CONCAT", "TRIM", "LTRIM", "RTRIM", "REPLACE", "TRANSLATE"] {
            assert_eq!(
                string_position_args(name, 1),
                StringPositionArgs::Coerce(vec![0]),
                "{name}/1 must coerce index 0"
            );
            assert_eq!(
                string_position_args(name, 2),
                StringPositionArgs::Coerce(vec![0, 1]),
                "{name}/2 must coerce both indices"
            );
            assert_eq!(
                string_position_args(name, 3),
                StringPositionArgs::Coerce(vec![0, 1, 2]),
                "{name}/3 must coerce every index"
            );
        }
    }

    /// Only the FIRST argument of these is a string position; any further argument is
    /// a genuine number (a start offset, a length, a repeat count).
    #[test]
    fn string_position_args_coerces_first_argument_only() {
        for name in [
            "LOWER",
            "UPPER",
            "ASCII",
            "INITCAP",
            "REVERSE",
            "LENGTH",
            "OCTET_LENGTH",
            "UNICODE",
            "SUBSTR",
            "REPEAT",
            "LEFT",
            "RIGHT",
        ] {
            for arg_count in 1..=3 {
                assert_eq!(
                    string_position_args(name, arg_count),
                    StringPositionArgs::Coerce(vec![0]),
                    "{name}/{arg_count} must coerce index 0 only"
                );
            }
        }
    }

    /// `LPAD`/`RPAD`'s numeric length argument (index 1) is always excluded, while
    /// their PAD-string argument (index 2, present only at arity > 2) is still
    /// coerced — the only mixed string/numeric arity in the table.
    /// `SUBSTR`/`REPEAT`/`LEFT`/`RIGHT`'s single-numeric-argument exclusion is already
    /// covered by `string_position_args_coerces_first_argument_only` above.
    #[test]
    fn string_position_args_excludes_numeric_arguments() {
        for name in ["LPAD", "RPAD"] {
            assert_eq!(
                string_position_args(name, 2),
                StringPositionArgs::Coerce(vec![0]),
                "{name}/2 has no pad-string argument to coerce"
            );
            assert_eq!(
                string_position_args(name, 3),
                StringPositionArgs::Coerce(vec![0, 2]),
                "{name}/3 must coerce the subject and the pad string, never the length"
            );
        }
    }

    /// `CHR`/`UNICODECHR` (their argument is a genuine integer codepoint) and every
    /// non-string function are NOT governed — the caller leaves such a node alone and
    /// never declines on it.
    #[test]
    fn string_position_args_not_governed_for_chr_and_non_string_functions() {
        for name in ["CHR", "UNICODECHR", "ABS", "CASE"] {
            for arg_count in 0..=3 {
                assert_eq!(
                    string_position_args(name, arg_count),
                    StringPositionArgs::NotGoverned,
                    "{name}/{arg_count} must not be governed"
                );
            }
        }
    }

    /// The name is uppercased before matching, so a lowercase `fn_name` resolves
    /// identically.
    #[test]
    fn string_position_args_matches_lowercase_function_name() {
        assert_eq!(
            string_position_args("upper", 1),
            string_position_args("UPPER", 1),
            "a lowercase name must resolve like its uppercase form"
        );
        assert_eq!(
            string_position_args("upper", 1),
            StringPositionArgs::Coerce(vec![0])
        );
        assert_eq!(
            string_position_args("instr", 3),
            StringPositionArgs::Decline,
            "a lowercase name must reach the arity decline too"
        );
    }

    /// No returned index may address past the end of the argument list — the caller
    /// indexes `arguments` with them directly.
    #[test]
    fn string_position_args_never_returns_out_of_range_index() {
        let governed = [
            "CONCAT",
            "TRIM",
            "LTRIM",
            "RTRIM",
            "REPLACE",
            "TRANSLATE",
            "LOWER",
            "UPPER",
            "ASCII",
            "INITCAP",
            "REVERSE",
            "LENGTH",
            "OCTET_LENGTH",
            "UNICODE",
            "SUBSTR",
            "REPEAT",
            "LEFT",
            "RIGHT",
            "LPAD",
            "RPAD",
            "INSTR",
            "LOCATE",
        ];
        for name in governed {
            for arg_count in 0..=5 {
                if let StringPositionArgs::Coerce(indices) = string_position_args(name, arg_count) {
                    for i in indices {
                        assert!(
                            i < arg_count,
                            "{name}/{arg_count} returned out-of-range index {i}"
                        );
                    }
                }
            }
        }
    }

    /// `INSTR`/`LOCATE` beyond two arguments decline on ARITY ALONE, whatever the
    /// argument types: `vs-expression` renders only `args[0]`/`args[1]` and silently
    /// drops the rest (#228), so coercing index 0 would turn today's loud DataFusion
    /// error into a silently wrong position. Exactly two arguments coerce both.
    #[test]
    fn string_position_args_declines_instr_locate_beyond_two_args() {
        assert_eq!(
            string_position_args("INSTR", 3),
            StringPositionArgs::Decline,
            "INSTR/3 drops its start-position argument — must decline"
        );
        assert_eq!(
            string_position_args("INSTR", 4),
            StringPositionArgs::Decline,
            "INSTR/4 drops its start-position and occurrence arguments — must decline"
        );
        assert_eq!(
            string_position_args("LOCATE", 3),
            StringPositionArgs::Decline,
            "LOCATE/3 drops its start-position argument — must decline"
        );
        for name in ["INSTR", "LOCATE"] {
            assert_eq!(
                string_position_args(name, 2),
                StringPositionArgs::Coerce(vec![0, 1]),
                "{name}/2 is rendered faithfully and must coerce both arguments"
            );
        }
    }

    // ---------------------------------------------------------------------------
    // string_function_arg_type_guard — issue #210 string-function argument typing
    // ---------------------------------------------------------------------------

    fn column(name: &str) -> Json {
        serde_json::json!({"type": "column", "name": name})
    }

    fn string_fn(name: &str, args: Vec<Json>) -> Json {
        serde_json::json!({
            "type": "function_scalar",
            "name": name,
            "arguments": args,
        })
    }

    fn trimmed_decimal(name: &str) -> Json {
        serde_json::json!({
            "type": "decimal_to_varchar_exasol",
            "arguments": [column(name)],
        })
    }

    fn cast_varchar(name: &str) -> Json {
        serde_json::json!({
            "type": "function_scalar_cast",
            "name": "CAST",
            "dataType": {"type": "VARCHAR"},
            "arguments": [column(name)],
        })
    }

    fn equals(left: Json, right: Json) -> Json {
        serde_json::json!({"type": "predicate_equal", "left": left, "right": right})
    }

    /// A non-object node has no children and no function dispatch — passed through.
    #[test]
    fn string_fn_guard_passes_through_non_object_node() {
        let col_types = decimal_rewrite_col_types();
        for node in [
            Json::Null,
            serde_json::json!("UPPER"),
            serde_json::json!(7),
            serde_json::json!([1, 2]),
        ] {
            assert_eq!(
                string_function_arg_type_guard(&node, &col_types),
                Some(node.clone()),
                "a non-object node must be passed through: {node}"
            );
        }
    }

    /// Scenario: a string-position VARCHAR or CHAR column argument pushes down
    /// unchanged — DataFusion needs no help with a genuine string.
    #[test]
    fn string_fn_guard_leaves_varchar_argument_unchanged() {
        let col_types = decimal_rewrite_col_types();
        for name in ["UPPER", "LOWER", "TRIM", "LTRIM", "CONCAT", "LENGTH"] {
            let node = string_fn(name, vec![column("name")]);
            assert_eq!(
                string_function_arg_type_guard(&node, &col_types),
                Some(node.clone()),
                "{name} over a VARCHAR column must be unchanged"
            );
        }
        // CHAR is dispatched by the same `starts_with` prefix pair as VARCHAR.
        let char_types = vec![("C_CHAR_A".to_string(), "CHAR(10)".to_string())];
        let node = string_fn("UPPER", vec![column("c_char_a")]);
        assert_eq!(
            string_function_arg_type_guard(&node, &char_types),
            Some(node.clone()),
            "a CHAR column argument must be unchanged"
        );
    }

    /// Scenario: a string-position DECIMAL column argument renders through Exasol's
    /// trimmed decimal-to-string form (#211's `decimal_to_varchar_exasol` node, reused
    /// verbatim so decimal formatting keeps a single owner). Integer columns arrive as
    /// `DECIMAL(p,0)` on the wire and are covered by the same branch.
    #[test]
    fn string_fn_guard_wraps_decimal_argument_in_trim() {
        let col_types = decimal_rewrite_col_types();

        let out =
            string_function_arg_type_guard(&string_fn("UPPER", vec![decimal_column()]), &col_types);
        assert_eq!(
            out,
            Some(string_fn("UPPER", vec![trimmed_decimal("c_decimal_a")])),
            "UPPER's DECIMAL argument must be wrapped in the trimmed-string node"
        );

        for name in ["TRIM", "LTRIM"] {
            assert_eq!(
                string_function_arg_type_guard(
                    &string_fn(name, vec![decimal_column()]),
                    &col_types
                ),
                Some(string_fn(name, vec![trimmed_decimal("c_decimal_a")])),
                "{name}'s DECIMAL argument must be wrapped"
            );
        }

        // Integer column (DECIMAL(20,0)) — issue #210's `UPPER(c_custkey)` repro shape.
        assert_eq!(
            string_function_arg_type_guard(&string_fn("UPPER", vec![column("id")]), &col_types),
            Some(string_fn("UPPER", vec![trimmed_decimal("id")])),
            "an integer DECIMAL(p,0) argument must be wrapped too"
        );

        // The wrapper is what renders Exasol's shortest form, not a plain CAST.
        let sql = render_expression_safe(
            &string_function_arg_type_guard(
                &string_fn("UPPER", vec![decimal_column()]),
                &col_types,
            )
            .expect("must not decline"),
        )
        .expect("must render");
        assert_eq!(
            sql,
            r#"upper(regexp_replace(regexp_replace(CAST("C_DECIMAL_A" AS VARCHAR), '(\.[0-9]*[1-9])0+$', '\1'), '\.0+$', ''))"#,
            "UPPER over a DECIMAL column must render the trimmed form: {sql}"
        );
    }

    /// Scenario: a string-position DATE column argument is wrapped in an explicit
    /// `CAST(<col> AS VARCHAR)` — DataFusion's Date32→Utf8 cast is `YYYY-MM-DD`, which
    /// is also Exasol's default `NLS_DATE_FORMAT` (issue #210's `LOWER(l_shipdate)`).
    #[test]
    fn string_fn_guard_casts_date_argument_to_varchar() {
        let col_types = decimal_rewrite_col_types();
        assert_eq!(
            string_function_arg_type_guard(&string_fn("LOWER", vec![column("d")]), &col_types),
            Some(string_fn("LOWER", vec![cast_varchar("d")])),
            "LOWER's DATE argument must be wrapped in CAST(<col> AS VARCHAR)"
        );
    }

    /// Scenario: a resolvable but non-coercible column type declines. BOOLEAN, DOUBLE
    /// and TIMESTAMP all have text forms that differ between the two engines
    /// (`TRUE`/`true`, the space/`T` separator), so a cast would turn a crash into a
    /// wrong answer — native Exasol evaluation is the only safe outcome.
    #[test]
    fn string_fn_guard_declines_boolean_double_and_timestamp_arguments() {
        let col_types = decimal_rewrite_col_types();
        for col in ["c_bool_a", "c_double_a", "c_ts_a"] {
            for name in ["UPPER", "TRIM", "CONCAT", "LENGTH"] {
                assert_eq!(
                    string_function_arg_type_guard(&string_fn(name, vec![column(col)]), &col_types),
                    None,
                    "{name} over {col} must decline"
                );
            }
        }
    }

    /// Scenario: a string-position argument whose column name does not resolve in
    /// `col_types` declines fail-safe.
    #[test]
    fn string_fn_guard_declines_unresolved_column_name() {
        let col_types = decimal_rewrite_col_types();
        assert_eq!(
            string_function_arg_type_guard(
                &string_fn("UPPER", vec![column("mystery")]),
                &col_types
            ),
            None,
            "an unresolvable column argument must decline"
        );
    }

    /// A `column` node with no `name` field is unresolvable — same fail-safe decline.
    #[test]
    fn string_fn_guard_declines_nameless_column_node() {
        let col_types = decimal_rewrite_col_types();
        let node = string_fn("UPPER", vec![serde_json::json!({"type": "column"})]);
        assert_eq!(
            string_function_arg_type_guard(&node, &col_types),
            None,
            "a nameless column argument must decline"
        );
    }

    /// The guard reaches a string function nested under a COMPARISON predicate (under
    /// `left`) — the shape issue #210's WHERE-clause repro takes.
    #[test]
    fn string_fn_guard_reaches_function_under_comparison_predicate() {
        let col_types = decimal_rewrite_col_types();
        let node = equals(
            string_fn("UPPER", vec![decimal_column()]),
            serde_json::json!({"type": "literal_string", "value": "X"}),
        );
        assert_eq!(
            string_function_arg_type_guard(&node, &col_types),
            Some(equals(
                string_fn("UPPER", vec![trimmed_decimal("c_decimal_a")]),
                serde_json::json!({"type": "literal_string", "value": "X"}),
            )),
            "a string function under `left` must be coerced"
        );
    }

    /// A decline anywhere in the tree propagates to the ROOT, so the caller declines
    /// the whole filter / select-list item rather than pushing a partially-guarded tree.
    #[test]
    fn string_fn_guard_nested_decline_propagates_to_root() {
        let col_types = decimal_rewrite_col_types();
        let filter = serde_json::json!({
            "type": "predicate_and",
            "expressions": [
                equals(column("name"), serde_json::json!({"type": "literal_string", "value": "X"})),
                {
                    "type": "predicate_not",
                    "expression": equals(
                        string_fn("UPPER", vec![column("c_double_a")]),
                        serde_json::json!({"type": "literal_string", "value": "X"})
                    )
                }
            ]
        });
        assert_eq!(
            string_function_arg_type_guard(&filter, &col_types),
            None,
            "a nested non-coercible string function must decline the whole tree"
        );
    }

    /// Only string-position indices are coerced: `SUBSTR`'s start/length, `REPEAT`'s
    /// count, `LEFT`/`RIGHT`'s length and `LPAD`'s length stay untouched, while
    /// `LPAD`'s PAD-STRING argument is coerced. The numeric positions here hold a
    /// DECIMAL column (`ID`), which WOULD be visibly rewritten if it were passed to
    /// the type dispatch — a literal int could not tell the two designs apart.
    #[test]
    fn string_fn_guard_leaves_numeric_position_arguments_untouched() {
        let col_types = decimal_rewrite_col_types();

        assert_eq!(
            string_function_arg_type_guard(
                &string_fn("SUBSTR", vec![decimal_column(), column("id"), column("id")]),
                &col_types
            ),
            Some(string_fn(
                "SUBSTR",
                vec![trimmed_decimal("c_decimal_a"), column("id"), column("id")]
            )),
            "SUBSTR's start and length arguments must stay bare columns"
        );

        for name in ["REPEAT", "LEFT", "RIGHT"] {
            assert_eq!(
                string_function_arg_type_guard(
                    &string_fn(name, vec![decimal_column(), column("id")]),
                    &col_types
                ),
                Some(string_fn(
                    name,
                    vec![trimmed_decimal("c_decimal_a"), column("id")]
                )),
                "{name}'s numeric argument must stay a bare column"
            );
        }

        // LPAD(str, length, pad): index 0 and 2 coerced, index 1 untouched.
        assert_eq!(
            string_function_arg_type_guard(
                &string_fn("LPAD", vec![decimal_column(), column("id"), column("d")]),
                &col_types
            ),
            Some(string_fn(
                "LPAD",
                vec![
                    trimmed_decimal("c_decimal_a"),
                    column("id"),
                    cast_varchar("d")
                ]
            )),
            "LPAD must coerce the subject and the pad string, never the length"
        );

        // A literal-int length is likewise never handed to the type dispatch.
        let length_literal = serde_json::json!({"type": "literal_exactnumeric", "value": 10});
        assert_eq!(
            string_function_arg_type_guard(
                &string_fn("LPAD", vec![decimal_column(), length_literal.clone()]),
                &col_types
            ),
            Some(string_fn(
                "LPAD",
                vec![trimmed_decimal("c_decimal_a"), length_literal]
            )),
            "a 2-argument LPAD must coerce index 0 only"
        );
    }

    /// Scenario: `INSTR` and `LOCATE` coerce BOTH of their two arguments. `LOCATE`'s
    /// render-time argument swap (`LOCATE(sub, str)` → `strpos(str, sub)`) happens
    /// after this guard, so both indices are string positions in either order.
    #[test]
    fn string_fn_guard_coerces_both_instr_and_locate_arguments() {
        let col_types = decimal_rewrite_col_types();

        assert_eq!(
            string_function_arg_type_guard(
                &string_fn("INSTR", vec![decimal_column(), column("d")]),
                &col_types
            ),
            Some(string_fn(
                "INSTR",
                vec![trimmed_decimal("c_decimal_a"), cast_varchar("d")]
            )),
            "INSTR must coerce both of its arguments"
        );

        assert_eq!(
            string_function_arg_type_guard(
                &string_fn("LOCATE", vec![column("d"), decimal_column()]),
                &col_types
            ),
            Some(string_fn(
                "LOCATE",
                vec![cast_varchar("d"), trimmed_decimal("c_decimal_a")]
            )),
            "LOCATE must coerce both of its arguments"
        );
    }

    /// Scenario: `INSTR` with 3 or 4 arguments and `LOCATE` with 3 decline THROUGH THE
    /// GUARD even when every argument is a VARCHAR column — the arity, not a type, is
    /// what declines (`vs-expression` drops the extra arguments, #228). The table-level
    /// counterpart is `string_position_args_declines_instr_locate_beyond_two_args`.
    #[test]
    fn string_fn_guard_declines_instr_locate_beyond_two_args() {
        let col_types = decimal_rewrite_col_types();
        let start = serde_json::json!({"type": "literal_exactnumeric", "value": 3});

        assert_eq!(
            string_function_arg_type_guard(
                &string_fn("INSTR", vec![column("name"), column("name"), start.clone()]),
                &col_types
            ),
            None,
            "INSTR/3 over VARCHAR arguments must still decline"
        );
        assert_eq!(
            string_function_arg_type_guard(
                &string_fn(
                    "INSTR",
                    vec![column("name"), column("name"), start.clone(), start.clone()]
                ),
                &col_types
            ),
            None,
            "INSTR/4 over VARCHAR arguments must still decline"
        );
        assert_eq!(
            string_function_arg_type_guard(
                &string_fn("LOCATE", vec![column("name"), column("name"), start]),
                &col_types
            ),
            None,
            "LOCATE/3 over VARCHAR arguments must still decline"
        );
    }

    /// Scenario: `CHR`/`UNICODECHR` are excluded — their single argument is a genuine
    /// integer codepoint, so it is neither coerced NOR a reason to decline (the
    /// difference between "not governed" and "declines on a bad argument"). Their
    /// children are still recursed.
    #[test]
    fn string_fn_guard_excludes_chr_and_unicodechr() {
        let col_types = decimal_rewrite_col_types();
        for name in ["CHR", "UNICODECHR"] {
            for arg in ["id", "c_double_a"] {
                let node = string_fn(name, vec![column(arg)]);
                assert_eq!(
                    string_function_arg_type_guard(&node, &col_types),
                    Some(node.clone()),
                    "{name}({arg}) must be left completely untouched"
                );
            }
        }

        // ... but a governed function nested INSIDE one is still reached.
        let nested = string_fn("CHR", vec![string_fn("LENGTH", vec![decimal_column()])]);
        assert_eq!(
            string_function_arg_type_guard(&nested, &col_types),
            Some(string_fn(
                "CHR",
                vec![string_fn("LENGTH", vec![trimmed_decimal("c_decimal_a")])]
            )),
            "a governed function under CHR must still be coerced"
        );
    }

    /// A column name in any letter case resolves — [`column_exa_type`] uppercases the
    /// name before the `col_types` lookup.
    #[test]
    fn string_fn_guard_resolves_case_mismatched_column_name() {
        let col_types = decimal_rewrite_col_types();
        let node = string_fn("UPPER", vec![column("C_DeCiMaL_a")]);
        assert_eq!(
            string_function_arg_type_guard(&node, &col_types),
            Some(string_fn("UPPER", vec![trimmed_decimal("C_DeCiMaL_a")])),
            "a mixed-case column name must resolve against the uppercase map"
        );
    }

    /// The one `col_types` lookup folds the node's name with the full-Unicode
    /// `to_uppercase`, so it resolves against the Unicode-folded list
    /// [`extract_all_column_types`] builds and MISSES the ASCII-folded list
    /// `involved_table_columns` builds.
    ///
    /// `STRAßE` is a CONSTRUCTED literal, not a name Exasol delivers: this crate
    /// uppercases every Iceberg field name itself before declaring it
    /// (`resolve_table_schema`, `file_resolution.rs:640`) and the full-Unicode fold maps
    /// `ß` to `SS`, so a real `straße` column reaches this lookup as `STRASSE` and no
    /// reachable name distinguishes the two folds. The literal is used here solely
    /// because Rust's two folds disagree on it, which is what makes the miss assertion
    /// falsifiable.
    #[test]
    fn column_exa_type_resolves_unicode_folded_list_and_misses_ascii_folded_list() {
        let node = column("STRAßE");
        let unicode_folded = [("STRASSE".to_string(), "VARCHAR(2000000)".to_string())];
        let ascii_folded = [("STRAßE".to_string(), "VARCHAR(2000000)".to_string())];

        assert_eq!(
            column_exa_type(&node, &unicode_folded),
            Some("VARCHAR(2000000)"),
            "`STRAßE`.to_uppercase() is `STRASSE`, the key `extract_all_column_types` builds"
        );
        assert_eq!(
            column_exa_type(&node, &ascii_folded),
            None,
            "`to_ascii_uppercase` leaves `STRAßE`, which the Unicode fold cannot match"
        );
    }

    /// A non-bare-column string-position argument (a literal, or a computed
    /// `c_decimal_a * 2`) is left unchanged and does NOT decline — a deliberate tracked
    /// exception (#223), mirroring #211's convention for computed arguments.
    #[test]
    fn string_fn_guard_leaves_computed_argument_unchanged() {
        let col_types = decimal_rewrite_col_types();

        let literal = string_fn(
            "UPPER",
            vec![serde_json::json!({"type": "literal_string", "value": "x"})],
        );
        assert_eq!(
            string_function_arg_type_guard(&literal, &col_types),
            Some(literal.clone()),
            "a literal argument must be left unchanged without declining"
        );

        let computed = string_fn(
            "UPPER",
            vec![string_fn(
                "MULT",
                vec![
                    decimal_column(),
                    serde_json::json!({"type": "literal_exactnumeric", "value": 2}),
                ],
            )],
        );
        assert_eq!(
            string_function_arg_type_guard(&computed, &col_types),
            Some(computed.clone()),
            "a computed argument must be left unchanged without declining"
        );
    }

    /// Post-order: the INNER string function's argument is coerced before the outer
    /// function's own check runs, so `UPPER(TRIM(c_decimal_a))` coerces the `TRIM`
    /// argument and leaves the (now non-column) `TRIM` node as UPPER's argument.
    #[test]
    fn string_fn_guard_coerces_inner_nested_string_function() {
        let col_types = decimal_rewrite_col_types();
        let node = string_fn("UPPER", vec![string_fn("TRIM", vec![decimal_column()])]);
        assert_eq!(
            string_function_arg_type_guard(&node, &col_types),
            Some(string_fn(
                "UPPER",
                vec![string_fn("TRIM", vec![trimmed_decimal("c_decimal_a")])]
            )),
            "the inner TRIM's DECIMAL argument must be coerced exactly once"
        );
    }
}
