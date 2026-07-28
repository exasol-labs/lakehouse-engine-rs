use crate::scan::spec::{LogicalField, ProjectionItem, SortKey, render_ordered};
use exasol_udf_sdk::error::UdfError;
use serde_json::Value as Json;
use vs_expression::render_expression_exasol_safe;

use super::support::{
    collect_all_column_names, emits_ident, extract_limit, extract_offset, render_limit_offset,
};

// ---------------------------------------------------------------------------
// Ordered top-N pushdown
// ---------------------------------------------------------------------------

/// Parse ONE `orderBy` element into a bare-column [`SortKey`].
///
/// Returns `None` when the element is not a bare `column` node or when its
/// `isAscending` / `nullsLast` flags are absent. This bare-column gate is
/// deliberately kept narrow: both `ORDER_BY_COLUMN` and `ORDER_BY_EXPRESSION`
/// are advertised (issue #198), so Exasol may send an expression sort key too —
/// [`parse_sort_flags`] handles those without constructing a [`SortKey`], keeping
/// the per-shard bounded top-N (`detect_topn`) eligible for bare columns only. The
/// column name is uppercased to match the adapter's canonical identifier casing.
/// This is the SINGLE bare-column parser, shared by [`detect_topn`] (which adds
/// projection + JSON-fallback gates on top) and [`parse_declined_sort_key`] (which
/// falls back to an EXPRESSION key when this gate rejects the element).
pub(super) fn parse_sort_key_element(element: &Json) -> Option<SortKey> {
    let expr = element.get("expression")?;
    if expr.get("type").and_then(|t| t.as_str()) != Some("column") {
        return None;
    }
    let column = expr
        .get("name")
        .and_then(|n| n.as_str())
        .map(|s| s.to_uppercase())?;
    let ascending = element.get("isAscending").and_then(|b| b.as_bool())?;
    let nulls_last = element.get("nullsLast").and_then(|b| b.as_bool())?;
    Some(SortKey {
        column,
        ascending,
        nulls_last,
    })
}

/// Parse ONE `orderBy` element's direction + NULL-placement flags:
/// `(isAscending, nullsLast)`.
///
/// Unlike [`parse_sort_key_element`] this imposes NO requirement on the element's
/// `expression` node, so an EXPRESSION or aggregate sort key — which is no bare
/// column and therefore yields no [`SortKey`] — can still reach the shared
/// [`render_ordered`](crate::scan::spec::render_ordered) seam with its flags intact.
/// It renders nothing and decides nothing about renderability; the caller renders the
/// ordering expression itself and declines if it cannot.
///
/// `None` when either flag is absent: an unexpected shape whose direction or NULL
/// placement must never be silently defaulted, since a wrong guess is a wrong order.
/// [`parse_sort_key_element`] keeps its own copy of this read so its bare-column gate
/// — the gate [`detect_topn`] eligibility rests on — stays byte-identical.
pub(super) fn parse_sort_flags(element: &Json) -> Option<(bool, bool)> {
    let ascending = element.get("isAscending").and_then(|b| b.as_bool())?;
    let nulls_last = element.get("nullsLast").and_then(|b| b.as_bool())?;
    Some((ascending, nulls_last))
}

/// ONE `orderBy` element of the declined row-scan path, parsed into what the outer
/// wrapper needs from it: an ordering to render, and the base columns that ordering
/// binds against (which the scan must therefore emit).
///
/// The two renderable shapes differ ONLY in those two things — both reach the same
/// [`render_ordered`] seam for direction and NULL placement.
#[derive(Debug)]
pub(super) enum ParsedSortKey {
    /// A bare `column` node: the ordering is the quoted column itself, and it is the
    /// single base column referenced. Byte-identical rendering to before #198.
    Column(SortKey),
    /// An EXPRESSION sort key (issue #198), Exasol-dialect-rendered over the
    /// UPPERCASE base columns it references — every one of which the scan must emit,
    /// hidden, for the wrapper's `ORDER BY` to bind against.
    Expression {
        rendered: String,
        columns: Vec<String>,
        ascending: bool,
        nulls_last: bool,
    },
    /// The element renders no ordering at all: neither a bare column nor an
    /// expression this adapter can express in Exasol SQL, or a missing direction /
    /// NULL-placement flag (never silently defaulted — a wrong guess is a wrong
    /// order). It contributes NOTHING: no hidden column, no `ORDER BY` element.
    /// Carrying it in the parsed list rather than dropping it is what lets a caller
    /// SEE that an element was dropped and decline instead of returning a silently
    /// under-ordered result.
    Unrenderable,
}

impl ParsedSortKey {
    /// The base columns this key's rendered ordering binds against — the columns the
    /// scan must emit. Empty for an [`Unrenderable`](Self::Unrenderable) key, which
    /// renders nothing and so needs nothing emitted.
    fn referenced_columns(&self) -> &[String] {
        match self {
            Self::Column(key) => std::slice::from_ref(&key.column),
            Self::Expression { columns, .. } => columns,
            Self::Unrenderable => &[],
        }
    }

    /// This key as one rendered `ORDER BY` element, or `None` when it renders no
    /// ordering. Both arms end in the same [`render_ordered`] seam — a bare column
    /// through [`SortKey::render_order_by_element`], which quotes the identifier and
    /// delegates — so direction and NULL placement cannot drift between them.
    fn render_order_by_element(&self) -> Option<String> {
        match self {
            Self::Column(key) => Some(key.render_order_by_element()),
            Self::Expression {
                rendered,
                ascending,
                nulls_last,
                ..
            } => Some(render_ordered(rendered, *ascending, *nulls_last)),
            Self::Unrenderable => None,
        }
    }
}

/// Parse ONE `orderBy` element for the declined row-scan path, in preference order:
/// a bare column ([`parse_sort_key_element`], unchanged), else the element's
/// expression rendered in the EXASOL dialect over the base columns it references,
/// else [`ParsedSortKey::Unrenderable`].
///
/// The Exasol dialect is the right one: this ordering is rendered on the outer
/// wrapper, which Exasol's own parser reads — never inside a DataFusion `ScanSpec`.
fn parse_declined_sort_key(element: &Json) -> ParsedSortKey {
    if let Some(key) = parse_sort_key_element(element) {
        return ParsedSortKey::Column(key);
    }
    let (Some((ascending, nulls_last)), Some(expr)) =
        (parse_sort_flags(element), element.get("expression"))
    else {
        return ParsedSortKey::Unrenderable;
    };
    let Some(rendered) = render_expression_exasol_safe(expr) else {
        return ParsedSortKey::Unrenderable;
    };
    let mut names = std::collections::HashSet::new();
    collect_all_column_names(expr, &mut names);
    let mut columns: Vec<String> = names.into_iter().collect();
    // Sorted, because the append order of the hidden columns decides the EMITS
    // clause and therefore the positional `_LH_PROJ_{i}` aliases: a HashSet's
    // iteration order would make the generated SQL differ between processes.
    columns.sort();
    ParsedSortKey::Expression {
        rendered,
        columns,
        ascending,
        nulls_last,
    }
}

/// Parse the whole `orderBy` clause WITHOUT the top-N match gates (projection
/// membership, JSON-fallback type). Used to render the self-contained final
/// `ORDER BY` on the DECLINE / non-matched paths: Exasol delegates a pushed ordering
/// and NO LONGER re-applies its own backstop sort, so the adapter must reproduce that
/// global sort in the SQL it returns even for shapes it does not optimize
/// (add-topn-pushdown B6).
///
/// EXACTLY one [`ParsedSortKey`] per element, in clause order — including
/// [`ParsedSortKey::Unrenderable`] for an element that renders nothing, so a caller
/// can tell a clause it rendered in FULL from one it rendered only in part. An absent
/// or non-array `orderBy` yields an empty list; see [`parse_declined_sort_key`] for
/// the per-element rules.
pub(super) fn parse_order_by_keys(pushdown_req: &Json) -> Vec<ParsedSortKey> {
    pushdown_req
        .get("orderBy")
        .and_then(|v| v.as_array())
        .map(|elements| elements.iter().map(parse_declined_sort_key).collect())
        .unwrap_or_default()
}

/// The correctness-safety guard on the declined row-scan path: a pushed ordering is
/// reproduced in FULL or declined outright, never in part.
///
/// Exasol DELEGATES a pushed `ORDER BY` and no longer re-applies its own backstop
/// sort, so an element [`parse_order_by_keys`] could not render has exactly one safe
/// disposition left — a hard `User` error. Returning SQL built from the surviving
/// keys would silently answer a DIFFERENT query than the one asked, with no signal
/// anywhere: the rows come back plausibly ordered, just not by the ordering
/// requested. There is no native re-plan to fall back to.
///
/// ANY unrenderable element declines, not merely all of them. A MIXED clause is the
/// dangerous case an `all`-shaped test would wave through: it renders a partial
/// ordering that looks entirely well-formed. "A non-empty `orderBy` yielding ZERO
/// renderable keys" is then just the subcase where every element is unrenderable,
/// covered by the same test — and it is the case that supersedes `fix-225`'s
/// "return the unwrapped SQL unchanged" rule (an ABSENT or EMPTY `orderBy` still
/// returns unchanged, since nothing was delegated).
///
/// Running BEFORE [`wrap_declined_order_by`] is what keeps that function's
/// empty-rendered-list guard structural rather than reachable: past this point every
/// surviving key renders an ordering, so a non-empty `orderBy` always yields a
/// non-empty `ORDER BY`.
///
/// The 1-based clause positions name the offending keys — an unrenderable element has
/// no rendered text to quote, so its position in the pushed clause is the only honest
/// identifier for it.
pub(super) fn ensure_every_sort_key_renders(keys: &[ParsedSortKey]) -> Result<(), UdfError> {
    let positions = keys
        .iter()
        .enumerate()
        .filter(|(_, key)| matches!(key, ParsedSortKey::Unrenderable))
        .map(|(i, _)| (i + 1).to_string())
        .collect::<Vec<_>>();
    if positions.is_empty() {
        return Ok(());
    }
    Err(UdfError::User(format!(
        "ORDER BY pushdown declined: sort key at clause position {} could not be rendered \
         for the declined row-scan wrapper; this is a hard error, not a native re-plan",
        positions.join(", ")
    )))
}

/// Detect the ordered-top-N shape and parse its sort keys.
///
/// Returns `Some(keys)` only when EVERY guard holds, so the caller may push the
/// keys as a per-shard bounded sort plus an outer merge `ORDER BY … LIMIT n`:
/// - exactly one involved table (no join),
/// - not a GROUP BY aggregate request (`aggregationType != "group_by"` and no
///   non-empty `groupBy`),
/// - no `having`,
/// - `limit` present with a NON-ZERO `offset` absent: a real offset declines, because
///   a per-shard `LIMIT n OFFSET m` would skip each shard's OWN first m rows and does
///   not compose into a global window (issue #191). The decline routes the request to
///   the row-scan wrapper, which renders the whole `ORDER BY … LIMIT n OFFSET m`
///   itself. A ZERO offset is the same request as an absent one — Exasol normalises
///   `OFFSET 0` away — so it must still match, which is why this is a non-zero test
///   and not an `offset`-key presence test,
/// - a non-empty `orderBy` in which EVERY element is a bare `column` node whose
///   uppercased name is one of the projected columns (`ProjectionItem::Column`),
/// - EVERY sort key column resolves to an Arrow type that does NOT require the
///   JSON-fallback VARCHAR cast (`needs_json_fallback` is false for its
///   `LogicalField.arrow_type`).
///
/// The JSON-fallback guard is a correctness requirement, not an optimization: for a
/// fallback-typed column the per-shard scan emits `CAST(col AS VARCHAR)` (a JSON
/// string) but its `ORDER BY col` sorts by the column's REAL native value (the cast
/// lives only in the SELECT list, not the FROM-clause row source the ORDER BY binds
/// against). Exasol's outer merge sees ONLY the emitted JSON string, so it re-ranks
/// lexicographically — a representation the per-shard sort never used. Per-shard and
/// merge would disagree on ranking and silently corrupt the global top-N. Declining
/// falls back to the safe raw-scan path (Exasol re-applies ORDER BY/LIMIT).
/// (The tag vocabulary collapses List/Struct/Binary/etc to `utf8`, so the reachable
/// fallback tag today is an out-of-range `decimal128(p>36,…)`; the guard is the
/// correct seam regardless and stays correct if the tag vocabulary is enriched.)
///
/// A sort key column absent from `logical_schema` declines defensively (rather than
/// assuming a safe type) — it should never happen, since the key is already required
/// to be a projected column.
///
/// Any deviation returns `None` — the caller then withholds the limit (never a
/// bare per-shard/outer LIMIT ahead of an ordering the adapter did not render) and
/// falls back to the pre-existing plan, leaving row selection to Exasol.
///
/// Only ever called on the pure row-scan path (no aggregates); the GROUP BY and
/// aggregate guards below make it self-contained and independently testable.
pub(super) fn detect_topn(
    request: &Json,
    pushdown_req: &Json,
    proj_cols: &[ProjectionItem],
    logical_schema: &[LogicalField],
) -> Option<Vec<SortKey>> {
    // A top-N needs a bound. Limit must be present, with a zero (or absent) offset.
    extract_limit(pushdown_req)?;
    if extract_offset(pushdown_req) != 0 {
        return None;
    }

    // Reject GROUP BY / grouped-aggregate shapes: ordered top-N over aggregated or
    // grouped results is out of scope (mission non-goal).
    if pushdown_req.get("aggregationType").and_then(|v| v.as_str()) == Some("group_by") {
        return None;
    }
    if pushdown_req
        .get("groupBy")
        .and_then(|v| v.as_array())
        .is_some_and(|a| !a.is_empty())
    {
        return None;
    }

    // Reject HAVING (only meaningful with grouping; a defensive belt with the above).
    if pushdown_req
        .get("having")
        .filter(|h| !h.is_null())
        .is_some()
    {
        return None;
    }

    // Single involved table only — a multi-table (join) shape declines.
    let table_count = request
        .get("involvedTables")
        .and_then(|v| v.as_array())
        .map(|t| t.len())
        .unwrap_or(0);
    if table_count != 1 {
        return None;
    }

    // Parse each sort key: it must be a bare `column` node that is also projected.
    let elements = pushdown_req.get("orderBy").and_then(|v| v.as_array())?;
    if elements.is_empty() {
        return None;
    }
    let mut keys = Vec::with_capacity(elements.len());
    for element in elements {
        // Bare-column shape + direction/NULL flags (shared parser); a missing flag
        // or a non-column node is an unexpected shape → decline.
        let key = parse_sort_key_element(element)?;
        // The sort key must be one of the projected columns (per the plan's scope:
        // sort on already-emitted columns, no extra machinery). An expression
        // projection (`ProjectionItem::Expr`) is never a bare-column sort target.
        let projected = proj_cols
            .iter()
            .any(|p| matches!(p, ProjectionItem::Column(c) if *c == key.column));
        if !projected {
            return None;
        }
        // Decline any sort key whose column requires the JSON-fallback VARCHAR cast:
        // its emitted representation (a JSON string) would not match the native value
        // the per-shard ORDER BY sorts by, so the outer merge would re-rank on the
        // wrong representation and corrupt the global top-N. Resolve the column's
        // Arrow type from its logical-schema tag (the only type info available at plan
        // time). A column absent from the logical schema declines defensively.
        let arrow_type = logical_schema
            .iter()
            .find(|f| f.name.to_uppercase() == key.column)
            .map(|f| crate::types::mapping::arrow_type_from_tag(&f.arrow_type))?;
        if crate::types::mapping::needs_json_fallback(&arrow_type) {
            return None;
        }
        keys.push(key);
    }
    Some(keys)
}

// ---------------------------------------------------------------------------
// Declined-ORDER-BY rendering (issues #225 / #189)
// ---------------------------------------------------------------------------

/// APPEND each column the sort keys REFERENCE that the derived projection does not
/// already emit, so the declined-`ORDER BY` wrapper's outer `ORDER BY` can bind
/// against columns the per-shard scan actually emits.
///
/// A bare-column key references exactly its own column; an EXPRESSION key references
/// every base column its rendered Exasol expression names (issue #198) — all of them
/// must be emitted, or the wrapper's `ORDER BY` names a column that is not in scope.
///
/// The scan's emitted-column set and the query's VISIBLE column set are two
/// different sets: an appended column is HIDDEN — emitted by the scan, reachable by
/// the wrapper's `ORDER BY`, and dropped again by the wrapper's explicit visible
/// select list ([`wrap_declined_order_by`]). This replaces the former full-base-row
/// widening (issue #190), which forced the two sets equal and therefore returned
/// every base column where Exasol positionally expects exactly the derived
/// projection's column count — `sqlCode 04000` (issue #225).
///
/// APPEND-ONLY is load-bearing: every original index is preserved, so
/// [`emits_ident`]'s positional `_LH_PROJ_{index}` and `raw_scan`'s matching
/// `AS _LH_PROJ_{i}` alias stay aligned by construction, and the wrapper's visible
/// prefix `0..visible_count` still names exactly the original items.
///
/// A column is appended AT MOST ONCE across ALL keys: the membership test re-scans
/// `proj_cols` as it grows, so it dedupes against the pre-existing
/// [`ProjectionItem::Column`] entries, against columns appended for an earlier key,
/// and against a column two references within one key name twice. A repeated EMITS
/// identifier would be a duplicate-column error.
///
/// A referenced column absent from `col_types` is SKIPPED and left unresolved — the
/// caller still renders it in the `ORDER BY`, exactly as before this fix. `col_types`
/// is the full `involvedTables[0].columns` list and every pushed sort key references
/// real table columns, so this is defensive and unreachable in practice; it
/// deliberately adds no machinery (decision [6]).
///
/// `proj_cols` and `proj_types` are extended in lockstep, preserving the 1:1
/// alignment the EMITS clause and the scan's per-column type coercion both rely on.
pub(super) fn extend_projection_with_sort_keys(
    proj_cols: &mut Vec<ProjectionItem>,
    proj_types: &mut Vec<String>,
    keys: &[ParsedSortKey],
    col_types: &[(String, String)],
) {
    for column in keys.iter().flat_map(ParsedSortKey::referenced_columns) {
        let already_emitted = proj_cols
            .iter()
            .any(|p| matches!(p, ProjectionItem::Column(c) if c == column));
        if already_emitted {
            continue;
        }
        let Some((name, exa_type)) = col_types.iter().find(|(name, _)| name == column) else {
            continue;
        };
        proj_cols.push(ProjectionItem::Column(name.clone()));
        proj_types.push(exa_type.clone());
    }
}

/// Wrap a declined-`ORDER BY` row-scan fan-out in a self-contained global
/// `ORDER BY` (plus the original window — `LIMIT n`, or `LIMIT n OFFSET m` — if any),
/// naming ONLY the visible columns.
///
/// The window renders through the shared [`render_limit_offset`] seam, so a zero
/// offset produces the pre-offset string byte-for-byte and a non-zero one (issue #191)
/// renders here rather than in the fan-out: a per-shard `LIMIT n OFFSET m` would skip
/// each shard's OWN first m rows and does not compose. Exasol's grammar rejects an
/// `OFFSET` without an `ORDER BY`, and the empty-`order_by` early return below is what
/// makes that structural — the window is only ever appended to a rendered `ORDER BY`.
///
/// Once `ORDER_BY_COLUMN` is advertised Exasol delegates the ordering and NO LONGER
/// re-applies its own backstop sort, so the adapter must reproduce that global sort
/// in the SQL it returns even for shapes it does not optimize (add-topn-pushdown B6).
///
/// The outer select list is the `emits_ident` of each of the FIRST `visible_count`
/// projection items — the derived projection as it stood before
/// [`extend_projection_with_sort_keys`] appended any hidden sort-key column. Inner
/// EMITS clause and outer select list therefore render through the ONE shared
/// [`emits_ident`] seam and cannot drift, and the returned arity equals the derived
/// projection's by construction (never `SELECT *` over a wider emitted row).
///
/// Each key renders through [`ParsedSortKey::render_order_by_element`], so a bare
/// column and an expression key differ only in the ordering expression itself and
/// share one direction/NULL-placement seam.
///
/// Two guards:
/// - NO key renders an ordering → `sql` is returned UNCHANGED, because wrapping would
///   emit an invalid bare `ORDER BY `. Reachable ONLY for an absent or empty
///   `orderBy`, i.e. an empty `keys`: a NON-EMPTY `orderBy` carrying an
///   [`ParsedSortKey::Unrenderable`] element never reaches here at all, because
///   [`ensure_every_sort_key_renders`] declines it upstream (#198). The test stays on
///   the RENDERED list rather than `keys.len()` so this remains a structural
///   guarantee and not an assumption about the caller's ordering of the two.
/// - `visible_count == 0` → falls back to `SELECT *`, since `SELECT  FROM (…)` is not
///   valid SQL. An empty row-scan projection is itself already impossible, so this is
///   a structural guard, not a reachable code path.
pub(super) fn wrap_declined_order_by(
    sql: &str,
    proj_cols: &[ProjectionItem],
    visible_count: usize,
    keys: &[ParsedSortKey],
    limit: Option<u64>,
    offset: u64,
) -> String {
    let order_by = keys
        .iter()
        .filter_map(ParsedSortKey::render_order_by_element)
        .collect::<Vec<_>>()
        .join(", ");
    if order_by.is_empty() {
        return sql.to_string();
    }
    let visible_list = if visible_count == 0 {
        "*".to_string()
    } else {
        proj_cols
            .iter()
            .take(visible_count)
            .enumerate()
            .map(|(i, item)| emits_ident(item, i))
            .collect::<Vec<_>>()
            .join(", ")
    };
    format!(
        "SELECT {visible_list} FROM ({sql}) ORDER BY {order_by}{}",
        render_limit_offset(limit, offset)
    )
}

#[cfg(test)]
mod tests {
    use super::super::support::{
        DISTRIBUTE_FILES_UDF_NAME, SCAN_UDF_NAME, aggregate_exasol_types, build_scan_driving_sql,
        extract_all_column_types, extract_projection, order_by_present, shard_count,
    };
    use super::super::test_support::*;
    use super::super::{detect_aggregates, ordinary_plans, validate_agg_col_types};
    use super::*;
    use crate::scan::spec::{CommonScanSpec, FileEntry, ScanSpec};
    use vs_expression::render_df_filter_safe;

    // -----------------------------------------------------------------------
    // Ordered top-N pushdown (B3)
    // -----------------------------------------------------------------------

    /// Reproduce `handle_pushdown`'s SYNCHRONOUS row-scan decision path (everything
    /// after `resolve_file_list`) so tests exercise the real `detect_topn`,
    /// `effective_limit` withholding glue, and `build_scan_driving_sql` — the exact
    /// composition production runs, minus the network file resolution.
    fn plan_scan_sql(request: &Json, files: Vec<(String, u64)>, cluster_nodes: usize) -> String {
        let pushdown_req = request
            .get("pushdownRequest")
            .cloned()
            .unwrap_or(Json::Null);
        let (mut proj_cols, mut proj_types, widened) =
            extract_projection(request, &pushdown_req).unwrap();
        let filter = pushdown_req
            .get("filter")
            .filter(|f| !f.is_null())
            .and_then(render_df_filter_safe);
        let limit = extract_limit(&pushdown_req);
        let has_order_by = order_by_present(&pushdown_req);
        let col_types = extract_all_column_types(request);

        let items = detect_aggregates(&pushdown_req)
            .filter(|it| validate_agg_col_types(&ordinary_plans(it), &col_types));
        let aggregates = items.map(|it| ordinary_plans(&it));
        // Production routes a widened projection to the qualified single-table
        // wrapper ONLY from the `RequestShape::RowScan` arm (`mod.rs`'s
        // `if projection_widened` sits inside it). An aggregate select list ALWAYS
        // widens — `project_columns` keeps aggregates off the projection — and never
        // reaches that guard, so the mirror must accept it on the aggregate path.
        assert!(
            !widened || aggregates.is_some(),
            "plan_scan_sql mirrors only the non-widened dispatch path; a widened row-scan fixture needs build_dispatch_sql, not this helper"
        );
        // Production always resolves a logical schema before detect_topn; reproduce
        // the LINEITEM schema every plan_scan_sql caller's request scans over.
        let logical_schema = lineitem_logical_schema();
        let topn = if aggregates.is_none() {
            detect_topn(request, &pushdown_req, &proj_cols, &logical_schema)
        } else {
            None
        };
        let order_by = topn.unwrap_or_default();
        let effective_limit = if has_order_by && order_by.is_empty() {
            None
        } else {
            limit
        };

        // Row-scan DECLINE path via the SHARED helpers the dispatcher calls, so this
        // mirror cannot drift from the real wrapping shape. Position is load-bearing on
        // both sides, exactly as in `build_dispatch_sql`: AFTER `detect_topn` (which
        // must see the pre-extension projection) and BEFORE the `spec_template` below
        // (whose `projection` / `emit_exa_types` must carry the appended hidden column
        // that the EMITS clause is built from).
        let visible_count = proj_cols.len();
        let declined_order_by = has_order_by && order_by.is_empty() && aggregates.is_none();
        let declined_sort_keys = if declined_order_by {
            let keys = parse_order_by_keys(&pushdown_req);
            // Mirrors the dispatcher's correctness-safety guard at the same position
            // (#198). Every fixture routed through this helper renders in full; a
            // declining one belongs on `build_dispatch_sql`, which returns the error.
            ensure_every_sort_key_renders(&keys).expect(
                "plan_scan_sql mirrors only fixtures whose pushed ORDER BY renders in full",
            );
            extend_projection_with_sort_keys(&mut proj_cols, &mut proj_types, &keys, &col_types);
            keys
        } else {
            Vec::new()
        };

        let spec_template = ScanSpec {
            common: CommonScanSpec {
                projection: proj_cols.clone(),
                filter,
                limit: effective_limit,
                order_by,
                aggregates,
                emit_exa_types: proj_types.clone(),
                storage: sample_storage(),
                ..Default::default()
            },
            files: vec![],
        };
        let files: Vec<FileEntry> = files.into_iter().map(FileEntry::from).collect();
        let g = shard_count(cluster_nodes, 1, files.len());
        let shards = crate::adapter::sharding::partition_files_by_bytes(files, g);
        let aggregate_types = aggregate_exasol_types(&pushdown_req);
        let sql = build_scan_driving_sql(
            &spec_template,
            &shards,
            &proj_cols,
            &proj_types,
            effective_limit,
            limit,
            &col_types,
            &aggregate_types,
            SCAN_UDF_NAME,
            DISTRIBUTE_FILES_UDF_NAME,
        );
        if declined_order_by {
            wrap_declined_order_by(
                &sql,
                &proj_cols,
                visible_count,
                &declined_sort_keys,
                limit,
                extract_offset(&pushdown_req),
            )
        } else {
            sql
        }
    }

    /// The logical schema production resolves for the NQ4 (LINEITEM) requests: both
    /// sort-eligible columns are in-range DECIMALs, so neither needs the JSON
    /// fallback and `detect_topn` matches. Field-ids are illustrative.
    fn lineitem_logical_schema() -> Vec<LogicalField> {
        vec![
            LogicalField {
                field_id: 1,
                name: "L_ORDERKEY".into(),
                arrow_type: "decimal128(20,0)".into(),
                nullable: true,
                initial_default: None,
            },
            LogicalField {
                field_id: 2,
                name: "L_EXTENDEDPRICE".into(),
                arrow_type: "decimal128(18,2)".into(),
                nullable: true,
                initial_default: None,
            },
        ]
    }

    /// [`parse_sort_flags`] reads direction + NULL placement off ANY `orderBy`
    /// element, with no column-node requirement, so an expression sort key can reach
    /// the shared `render_ordered` seam. [`parse_sort_key_element`]'s bare-column gate
    /// is untouched by it — the same expression element still yields no [`SortKey`],
    /// which is what keeps [`detect_topn`] eligibility unchanged.
    #[test]
    fn parse_sort_flags_reads_direction_and_nulls_without_column_gate() {
        let expression_element = serde_json::json!({
            "type": "order_by_element",
            "expression": {"type": "function_scalar", "name": "ABS", "arguments": [
                {"type": "column", "name": "L_EXTENDEDPRICE"}
            ]},
            "isAscending": false,
            "nullsLast": true
        });
        assert_eq!(
            parse_sort_flags(&expression_element),
            Some((false, true)),
            "an expression element's flags must parse"
        );
        assert!(
            parse_sort_key_element(&expression_element).is_none(),
            "the bare-column gate must still reject the same element"
        );

        let column_element = serde_json::json!({
            "type": "order_by_element",
            "expression": {"type": "column", "name": "L_ORDERKEY", "tableName": "LINEITEM"},
            "isAscending": true,
            "nullsLast": false
        });
        assert_eq!(parse_sort_flags(&column_element), Some((true, false)));

        // A missing flag is an unexpected shape on either side: no default is invented.
        for missing in ["isAscending", "nullsLast"] {
            let mut partial = expression_element.clone();
            partial.as_object_mut().unwrap().remove(missing);
            assert_eq!(
                parse_sort_flags(&partial),
                None,
                "a missing {missing} must not be defaulted"
            );
        }
    }

    /// Match: the ordered top-N wraps the fan-out in an outer `ORDER BY … LIMIT n`
    /// and carries the SAME sort keys + limit into the shard-invariant common blob
    /// (which the scan UDF renders as the per-shard bounded sort). Multi-shard so a
    /// real fan-out + merge is exercised.
    #[test]
    fn ordered_topn_emits_per_shard_and_outer_order_by() {
        let request = nq4_request();
        let files = vec![
            ("s3://w/part-0.parquet".to_string(), 1000u64),
            ("s3://w/part-1.parquet".to_string(), 1000u64),
        ];
        // Two nodes → two shards → a genuine GROUP BY shard_key fan-out.
        let sql = plan_scan_sql(&request, files, 2);

        // Outer merge ORDER BY, explicit direction + NULL placement, before LIMIT.
        assert!(
            sql.contains(r#"ORDER BY "L_EXTENDEDPRICE" DESC NULLS LAST LIMIT 20"#),
            "matched top-N must render an outer ORDER BY … LIMIT: {sql}"
        );
        // The per-shard common blob carries the identical sort keys AND the limit,
        // so every shard runs the same bounded sort (rendered by the scan UDF).
        let common = common_arg_literal(&sql);
        assert!(
            common.contains(
                r#""order_by":[{"column":"L_EXTENDEDPRICE","ascending":false,"nulls_last":true}]"#
            ),
            "common blob must carry the per-shard sort keys: {common}"
        );
        assert!(
            common.contains(r#""limit":20"#),
            "common blob must carry the per-shard limit: {common}"
        );
    }

    /// A NON-ZERO `limit.offset` DECLINES the bounded per-shard top-N, and the window
    /// is rendered ONCE — on the declined wrapper, beside the `ORDER BY` it renders
    /// itself: `ORDER BY … LIMIT n OFFSET m` (issue #191). A per-shard
    /// `LIMIT n OFFSET m` would skip each shard's OWN first m rows and does not
    /// compose, so the fan-out stays unbounded and unsorted.
    #[test]
    fn nonzero_offset_declines_bounded_topn() {
        let mut request = nq4_request();
        request["pushdownRequest"]["limit"] = serde_json::json!({"numElements": 20, "offset": 5});
        let projected = vec![
            ProjectionItem::Column("L_ORDERKEY".into()),
            ProjectionItem::Column("L_EXTENDEDPRICE".into()),
        ];
        assert!(
            detect_topn(
                &request,
                &pd(&request),
                &projected,
                &lineitem_logical_schema()
            )
            .is_none(),
            "a non-zero offset must decline the bounded top-N path"
        );

        let files = vec![
            ("s3://w/part-0.parquet".to_string(), 1000u64),
            ("s3://w/part-1.parquet".to_string(), 1000u64),
        ];
        let sql = plan_scan_sql(&request, files, 2);

        assert!(
            sql.contains(r#"ORDER BY "L_EXTENDEDPRICE" DESC NULLS LAST LIMIT 20 OFFSET 5"#),
            "the declined wrapper must render the full window beside its ORDER BY: {sql}"
        );
        assert_eq!(
            sql.matches("OFFSET").count(),
            1,
            "the offset belongs on the wrapper alone, never in the fan-out: {sql}"
        );
        let common = common_arg_literal(&sql);
        assert!(
            !common.contains("\"limit\"") && !common.contains("order_by"),
            "the per-shard fan-out must carry neither the limit nor the sort keys: {common}"
        );
    }

    /// `offset: 0` is the SAME request as an ABSENT `offset` key (Exasol normalises an
    /// explicit `OFFSET 0` away), so it must still MATCH the bounded top-N and yield
    /// byte-identical SQL: the guard is a non-zero test, not a presence test. A
    /// presence test behaves identically on today's Exasol but would silently decline
    /// every ordered LIMIT query cluster-wide on a future build that does attach
    /// `offset: 0`.
    #[test]
    fn zero_offset_still_matches_bounded_topn_byte_identically() {
        let baseline = nq4_request();
        let mut zero_offset = nq4_request();
        zero_offset["pushdownRequest"]["limit"] =
            serde_json::json!({"numElements": 20, "offset": 0});
        let projected = vec![
            ProjectionItem::Column("L_ORDERKEY".into()),
            ProjectionItem::Column("L_EXTENDEDPRICE".into()),
        ];
        let matched = detect_topn(
            &baseline,
            &pd(&baseline),
            &projected,
            &lineitem_logical_schema(),
        );
        assert!(matched.is_some(), "sanity: the baseline shape must match");
        assert_eq!(
            detect_topn(
                &zero_offset,
                &pd(&zero_offset),
                &projected,
                &lineitem_logical_schema()
            ),
            matched,
            "offset 0 must match the bounded top-N exactly as an absent offset does"
        );

        let files = || {
            vec![
                ("s3://w/part-0.parquet".to_string(), 1000u64),
                ("s3://w/part-1.parquet".to_string(), 1000u64),
            ]
        };
        let baseline_sql = plan_scan_sql(&baseline, files(), 2);
        assert_eq!(
            plan_scan_sql(&zero_offset, files(), 2),
            baseline_sql,
            "a zero offset must not change one byte of the generated SQL"
        );
        assert!(
            baseline_sql.contains(" LIMIT 20") && !baseline_sql.contains("OFFSET"),
            "the matched bounded top-N renders its LIMIT and no OFFSET: {baseline_sql}"
        );
    }

    /// Decline (sort key not projected): `ORDER BY` is present but the sort column
    /// is not in the projection, so the bounded top-N declines. The PER-SHARD sort
    /// keys and LIMIT are still withheld from the common blob (anti-wrong-truncation
    /// invariant, decision [4]), but the OUTER wrapper renders a self-contained
    /// global `ORDER BY … LIMIT n` (add-topn-pushdown B6): once `ORDER_BY_COLUMN` is
    /// advertised Exasol no longer re-applies its own backstop sort/limit, so the
    /// adapter reproduces it in the returned SQL.
    ///
    /// The unprojected sort key `L_EXTENDEDPRICE` is APPENDED to the scan as a HIDDEN
    /// column (issues #225 / #189) so that outer `ORDER BY` binds against a column the
    /// scan actually emits, while the wrapper's visible select list still names only
    /// `"L_ORDERKEY"` — the derived projection — keeping the returned arity at 1.
    #[test]
    fn order_by_present_without_topn_match_withholds_per_shard_limit() {
        // Project only L_ORDERKEY, but ORDER BY L_EXTENDEDPRICE (unprojected).
        let request = serde_json::json!({
            "involvedTables": [{
                "name": "LINEITEM",
                "columns": [
                    {"name": "L_ORDERKEY", "dataType": {"type": "decimal", "precision": 20, "scale": 0}},
                    {"name": "L_EXTENDEDPRICE", "dataType": {"type": "decimal", "precision": 18, "scale": 2}},
                ],
            }],
            "pushdownRequest": {
                "type": "select",
                "selectList": [
                    {"type": "column", "name": "L_ORDERKEY", "tableName": "LINEITEM"},
                ],
                "selectListDataTypes": [
                    {"type": "decimal", "precision": 20, "scale": 0},
                ],
                "orderBy": [{
                    "type": "order_by_element",
                    "expression": {"type": "column", "columnNr": 1, "name": "L_EXTENDEDPRICE", "tableName": "LINEITEM"},
                    "isAscending": false,
                    "nullsLast": true
                }],
                "limit": {"numElements": 20}
            }
        });
        // detect_topn declines the unprojected-key shape.
        assert!(
            detect_topn(
                &request,
                &pd(&request),
                &[ProjectionItem::Column("L_ORDERKEY".into())],
                &lineitem_logical_schema()
            )
            .is_none(),
            "unprojected sort key must decline the top-N path"
        );

        let files = vec![
            ("s3://w/part-0.parquet".to_string(), 1000u64),
            ("s3://w/part-1.parquet".to_string(), 1000u64),
        ];
        let sql = plan_scan_sql(&request, files, 2);

        // The OUTER wrapper renders a self-contained global ORDER BY + LIMIT
        // (reproducing Exasol's former backstop, which no longer runs).
        assert!(
            sql.contains(r#"ORDER BY "L_EXTENDEDPRICE" DESC NULLS LAST LIMIT 20"#),
            "declined shape must render a self-contained outer ORDER BY … LIMIT: {sql}"
        );
        // The wrapper's VISIBLE select list is the derived projection alone; the
        // appended sort key is emitted by the scan but dropped from the result.
        assert!(
            sql.contains(r#"SELECT "L_ORDERKEY" FROM ("#),
            "wrapper must name only the derived projection, never SELECT *: {sql}"
        );
        assert_eq!(
            outer_select_list(&sql),
            "\"L_ORDERKEY\"",
            "the hidden sort key must not be visible in the outer select list: {sql}"
        );
        assert!(
            emits_clause(&sql).contains("\"L_EXTENDEDPRICE\""),
            "the scan must EMIT the appended hidden sort key: {}",
            emits_clause(&sql)
        );
        // But the PER-SHARD common blob still carries NO sort keys and NO limit:
        // the fan-out stays unbounded and unsorted (anti-wrong-truncation invariant).
        let common = common_arg_literal(&sql);
        assert!(
            !common.contains("\"limit\""),
            "declined shape must withhold the per-shard LIMIT from the common blob: {common}"
        );
        assert!(
            !common.contains("order_by"),
            "declined shape must not carry sort keys into the common blob: {common}"
        );
    }

    /// A LINEITEM row-scan request whose `orderBy` is `order_by` — the shared fixture
    /// for the expression-sort-key cases. `select_list` names the VISIBLE columns; any
    /// column an `orderBy` expression references but the select list omits must reach
    /// the scan as an APPENDED HIDDEN column.
    fn lineitem_order_by_request(select_list: &[&str], order_by: Json, limit: Option<u64>) -> Json {
        let type_of = |name: &str| {
            if name == "L_ORDERKEY" {
                serde_json::json!({"type": "decimal", "precision": 20, "scale": 0})
            } else {
                serde_json::json!({"type": "decimal", "precision": 18, "scale": 2})
            }
        };
        let mut pushdown_req = serde_json::json!({
            "type": "select",
            "selectList": select_list
                .iter()
                .map(|name| serde_json::json!({"type": "column", "name": name, "tableName": "LINEITEM"}))
                .collect::<Vec<_>>(),
            "selectListDataTypes": select_list.iter().map(|n| type_of(n)).collect::<Vec<_>>(),
            "orderBy": order_by,
        });
        if let Some(n) = limit {
            pushdown_req["limit"] = serde_json::json!({"numElements": n});
        }
        serde_json::json!({
            "involvedTables": [{
                "name": "LINEITEM",
                "columns": [
                    {"name": "L_ORDERKEY", "dataType": type_of("L_ORDERKEY")},
                    {"name": "L_EXTENDEDPRICE", "dataType": type_of("L_EXTENDEDPRICE")},
                ],
            }],
            "pushdownRequest": pushdown_req,
        })
    }

    /// One `orderBy` element over `expression`, with explicit direction + NULL placement.
    fn order_by_element(expression: Json, ascending: bool, nulls_last: bool) -> Json {
        serde_json::json!({
            "type": "order_by_element",
            "expression": expression,
            "isAscending": ascending,
            "nullsLast": nulls_last
        })
    }

    /// `ABS(<column>)` — the canonical expression sort key from issue #198's repro.
    fn abs_of(column: &str) -> Json {
        serde_json::json!({"type": "function_scalar", "name": "ABS", "arguments": [
            {"type": "column", "name": column, "tableName": "LINEITEM"}
        ]})
    }

    /// A declined `ORDER BY` on an EXPRESSION renders that expression in the Exasol
    /// dialect on the outer wrapper and emits the base columns it references as
    /// HIDDEN scan columns — the expression-key twin of the bare-column case above
    /// (issue #198). The referenced column is absent from the select list, so it is
    /// APPENDED to the scan's emitted set and dropped again by the wrapper's explicit
    /// visible select list, keeping the returned arity at the derived projection's 1.
    #[test]
    fn declined_order_by_expression_appends_referenced_columns_as_hidden() {
        let request = lineitem_order_by_request(
            &["L_ORDERKEY"],
            serde_json::json!([order_by_element(abs_of("L_EXTENDEDPRICE"), false, true)]),
            Some(20),
        );
        let files = vec![
            ("s3://w/part-0.parquet".to_string(), 1000u64),
            ("s3://w/part-1.parquet".to_string(), 1000u64),
        ];
        let sql = plan_scan_sql(&request, files, 2);

        assert!(
            sql.contains(r#"ORDER BY abs("L_EXTENDEDPRICE") DESC NULLS LAST LIMIT 20"#),
            "the expression sort key must be rendered on the outer wrapper: {sql}"
        );
        assert_eq!(
            outer_select_list(&sql),
            "\"L_ORDERKEY\"",
            "the hidden referenced column must not be visible in the result: {sql}"
        );
        assert!(
            emits_clause(&sql).contains("\"L_EXTENDEDPRICE\""),
            "the scan must EMIT the referenced column the outer ORDER BY binds against: {}",
            emits_clause(&sql)
        );
        let common = common_arg_literal(&sql);
        assert!(
            common.contains(r#""projection":["L_ORDERKEY","L_EXTENDEDPRICE"]"#),
            "the scan spec must PROJECT the hidden column, not merely declare it in \
             EMITS — the extension runs BEFORE the spec_template literal, or the UDF \
             would never emit the column the EMITS clause promises: {common}"
        );
        assert!(
            !common.contains("order_by") && !common.contains("\"limit\""),
            "the per-shard common blob must stay clean (no sort keys, no limit): {common}"
        );
    }

    /// Two expression sort keys in ONE clause both render, in order, and their
    /// referenced base columns are appended AT MOST ONCE — deduped against each other
    /// (`L_EXTENDEDPRICE` is referenced by both keys) and against the existing
    /// select-list items (`L_ORDERKEY` is already projected). A repeated EMITS
    /// identifier would be a duplicate-column error.
    #[test]
    fn declined_order_by_two_expression_keys_renders_both_and_leaks_none() {
        let sum_expr = serde_json::json!({"type": "function_scalar", "name": "ADD", "arguments": [
            {"type": "column", "name": "L_EXTENDEDPRICE", "tableName": "LINEITEM"},
            {"type": "column", "name": "L_ORDERKEY", "tableName": "LINEITEM"}
        ]});
        let request = lineitem_order_by_request(
            &["L_ORDERKEY"],
            serde_json::json!([
                order_by_element(abs_of("L_EXTENDEDPRICE"), false, true),
                order_by_element(sum_expr, true, false),
            ]),
            None,
        );
        let files = vec![("s3://w/part-0.parquet".to_string(), 1000u64)];
        let sql = plan_scan_sql(&request, files, 1);

        assert!(
            sql.contains(
                r#"ORDER BY abs("L_EXTENDEDPRICE") DESC NULLS LAST, ("L_EXTENDEDPRICE" + "L_ORDERKEY") ASC NULLS FIRST"#
            ),
            "both expression sort keys must render, in clause order: {sql}"
        );
        let emits = emits_clause(&sql);
        assert_eq!(
            emits.matches("\"L_EXTENDEDPRICE\"").count(),
            1,
            "a column referenced by two keys must be appended exactly once: {emits}"
        );
        assert_eq!(
            emits.matches("\"L_ORDERKEY\"").count(),
            1,
            "an already-projected referenced column must not be appended again: {emits}"
        );
        assert_eq!(
            outer_select_list(&sql),
            "\"L_ORDERKEY\"",
            "no hidden column may leak into the visible select list: {sql}"
        );
    }

    /// Composition order (#198): an expression sort key whose referenced column IS
    /// already projected and which carries a `LIMIT` — the shape that would match the
    /// bounded top-N if the bare-column gate were widened. It must NOT: `detect_topn`
    /// still declines, the per-shard common blob carries neither sort keys nor a limit,
    /// and the query takes the declined wrapper path. The projection is left untouched
    /// (nothing to hide), proving the append dedupes against existing select-list items.
    #[test]
    fn expression_sort_key_declines_bounded_topn_and_takes_declined_path() {
        let request = lineitem_order_by_request(
            &["L_ORDERKEY", "L_EXTENDEDPRICE"],
            serde_json::json!([order_by_element(abs_of("L_EXTENDEDPRICE"), false, true)]),
            Some(20),
        );
        let projected = vec![
            ProjectionItem::Column("L_ORDERKEY".into()),
            ProjectionItem::Column("L_EXTENDEDPRICE".into()),
        ];
        assert!(
            detect_topn(
                &request,
                &pd(&request),
                &projected,
                &lineitem_logical_schema()
            )
            .is_none(),
            "an expression sort key must never match the bounded top-N"
        );

        let files = vec![("s3://w/part-0.parquet".to_string(), 1000u64)];
        let sql = plan_scan_sql(&request, files, 1);

        assert!(
            sql.contains(r#"ORDER BY abs("L_EXTENDEDPRICE") DESC NULLS LAST LIMIT 20"#),
            "the declined wrapper must render the ordering and the outer LIMIT: {sql}"
        );
        assert_eq!(
            outer_select_list(&sql),
            "\"L_ORDERKEY\", \"L_EXTENDEDPRICE\"",
            "the visible select list stays the derived projection: {sql}"
        );
        assert_eq!(
            emits_clause(&sql).matches("\"L_EXTENDEDPRICE\"").count(),
            1,
            "an already-projected referenced column must not be appended: {}",
            emits_clause(&sql)
        );
        let common = common_arg_literal(&sql);
        assert!(
            !common.contains("order_by") && !common.contains("\"limit\""),
            "the bounded top-N declined, so no per-shard sort keys or limit: {common}"
        );
    }

    /// Every unsupported ordered-query shape declines the top-N path (returns None),
    /// while the NQ4 shape matches. Covers: join (multiple involved tables), GROUP
    /// BY present, an expression (non-bare-column) sort key, ORDER BY with no LIMIT.
    #[test]
    fn unsupported_order_by_shape_declines_topn() {
        let projected = vec![
            ProjectionItem::Column("L_ORDERKEY".into()),
            ProjectionItem::Column("L_EXTENDEDPRICE".into()),
        ];

        // Baseline: the well-formed NQ4 shape matches.
        let ok = nq4_request();
        assert_eq!(
            detect_topn(&ok, &pd(&ok), &projected, &lineitem_logical_schema()),
            Some(vec![SortKey {
                column: "L_EXTENDEDPRICE".into(),
                ascending: false,
                nulls_last: true,
            }]),
            "the NQ4 shape must match"
        );

        // Join: two involved tables.
        let mut join = nq4_request();
        let extra_table = serde_json::json!({
            "name": "ORDERS",
            "columns": [{"name": "O_ORDERKEY", "dataType": {"type": "decimal", "precision": 20, "scale": 0}}]
        });
        join.get_mut("involvedTables")
            .and_then(|v| v.as_array_mut())
            .unwrap()
            .push(extra_table);
        assert!(
            detect_topn(&join, &pd(&join), &projected, &lineitem_logical_schema()).is_none(),
            "a multi-table (join) shape must decline"
        );

        // GROUP BY present.
        let mut grouped = nq4_request();
        grouped["pushdownRequest"]["aggregationType"] = serde_json::json!("group_by");
        grouped["pushdownRequest"]["groupBy"] =
            serde_json::json!([{"type": "column", "name": "L_ORDERKEY"}]);
        assert!(
            detect_topn(
                &grouped,
                &pd(&grouped),
                &projected,
                &lineitem_logical_schema()
            )
            .is_none(),
            "a GROUP BY shape must decline"
        );

        // Expression (non-bare-column) sort key.
        let mut expr_key = nq4_request();
        expr_key["pushdownRequest"]["orderBy"] = serde_json::json!([{
            "type": "order_by_element",
            "expression": {"type": "function_scalar", "name": "ABS", "arguments": [
                {"type": "column", "name": "L_EXTENDEDPRICE"}
            ]},
            "isAscending": false,
            "nullsLast": true
        }]);
        assert!(
            detect_topn(
                &expr_key,
                &pd(&expr_key),
                &projected,
                &lineitem_logical_schema()
            )
            .is_none(),
            "an expression sort key must decline (ORDER_BY_EXPRESSION unadvertised)"
        );

        // ORDER BY with no LIMIT: not a bounded top-N.
        let mut no_limit = nq4_request();
        no_limit["pushdownRequest"]
            .as_object_mut()
            .unwrap()
            .remove("limit");
        assert!(
            detect_topn(
                &no_limit,
                &pd(&no_limit),
                &projected,
                &lineitem_logical_schema()
            )
            .is_none(),
            "an ORDER BY without a LIMIT must decline"
        );
    }

    /// B3b correctness guard: a sort key whose column requires the JSON-fallback
    /// VARCHAR cast declines the top-N path, because the per-shard `ORDER BY col`
    /// sorts the native value while the emitted `CAST(col AS VARCHAR)` is a JSON
    /// string — so Exasol's outer merge would re-rank on the wrong representation.
    /// A plain in-range DECIMAL sort key still matches (regression guard), and a
    /// sort key absent from the logical schema declines defensively.
    #[test]
    fn json_fallback_typed_sort_key_declines_topn() {
        let projected = vec![
            ProjectionItem::Column("L_ORDERKEY".into()),
            ProjectionItem::Column("L_EXTENDEDPRICE".into()),
        ];
        let request = nq4_request();

        // Regression: plain in-range DECIMAL sort key (L_EXTENDEDPRICE) matches.
        assert!(
            detect_topn(
                &request,
                &pd(&request),
                &projected,
                &lineitem_logical_schema()
            )
            .is_some(),
            "a plain in-range DECIMAL sort key must still match the top-N shape"
        );

        // The sort key column typed as an OUT-OF-RANGE Decimal128 (emitted as
        // JSON-fallback VARCHAR): the reachable fallback tag from the logical-schema
        // vocabulary (List/Struct/Binary all collapse to `utf8`). Must decline.
        let fallback_schema = vec![
            LogicalField {
                field_id: 1,
                name: "L_ORDERKEY".into(),
                arrow_type: "decimal128(20,0)".into(),
                nullable: true,
                initial_default: None,
            },
            LogicalField {
                field_id: 2,
                name: "L_EXTENDEDPRICE".into(),
                arrow_type: "decimal128(40,6)".into(),
                nullable: true,
                initial_default: None,
            },
        ];
        assert!(
            crate::types::mapping::needs_json_fallback(
                &crate::types::mapping::arrow_type_from_tag("decimal128(40,6)")
            ),
            "sanity: the chosen tag must actually be a JSON-fallback type"
        );
        assert!(
            detect_topn(&request, &pd(&request), &projected, &fallback_schema).is_none(),
            "a JSON-fallback-typed sort key must decline the top-N path"
        );

        // The sort key column absent from the logical schema declines defensively.
        let missing_schema = vec![LogicalField {
            field_id: 1,
            name: "L_ORDERKEY".into(),
            arrow_type: "decimal128(20,0)".into(),
            nullable: true,
            initial_default: None,
        }];
        assert!(
            detect_topn(&request, &pd(&request), &projected, &missing_schema).is_none(),
            "a sort key absent from the logical schema must decline defensively"
        );
    }

    /// cap-ext scenario: an ORDER BY the adapter cannot bound as a top-N (here: no
    /// LIMIT) is correctness-safe. The bounded top-N declines (no per-shard sort, no
    /// per-shard limit in the common blob), but the OUTER wrapper renders a
    /// self-contained global `ORDER BY` (no LIMIT) — since once `ORDER_BY_COLUMN` is
    /// advertised Exasol no longer re-applies its own backstop sort (add-topn-pushdown
    /// B6), the adapter's returned SQL must specify the ordering itself.
    #[test]
    fn unbounded_order_by_falls_back_correctness_safe() {
        // ORDER BY a projected column but NO LIMIT (unbounded).
        let mut request = nq4_request();
        request["pushdownRequest"]
            .as_object_mut()
            .unwrap()
            .remove("limit");
        let files = vec![("s3://w/part-0.parquet".to_string(), 1000u64)];
        let sql = plan_scan_sql(&request, files, 1);
        assert!(
            sql.contains(r#"ORDER BY "L_EXTENDEDPRICE" DESC NULLS LAST"#),
            "unbounded ORDER BY must be rendered self-contained by the adapter: {sql}"
        );
        assert!(
            !sql.contains("LIMIT"),
            "unbounded ORDER BY must not carry any LIMIT: {sql}"
        );
        let common = common_arg_literal(&sql);
        assert!(
            !common.contains("order_by") && !common.contains("\"limit\""),
            "per-shard common blob must stay clean (no sort keys, no limit): {common}"
        );
    }

    /// Row-scan DECLINE with `order_by` but NO `limit` (projected sort column):
    /// the outer wrapper renders a self-contained global `ORDER BY` (no LIMIT), and
    /// the per-shard common blob stays clean. Proves the decline path no longer
    /// withholds the ordering entirely (add-topn-pushdown B6), independent of a
    /// LIMIT being present.
    #[test]
    fn row_scan_decline_order_by_no_limit_wraps_outer_order_by() {
        let request = serde_json::json!({
            "involvedTables": [{
                "name": "LINEITEM",
                "columns": [
                    {"name": "L_ORDERKEY", "dataType": {"type": "decimal", "precision": 20, "scale": 0}},
                    {"name": "L_EXTENDEDPRICE", "dataType": {"type": "decimal", "precision": 18, "scale": 2}},
                ],
            }],
            "pushdownRequest": {
                "type": "select",
                "selectList": [
                    {"type": "column", "name": "L_ORDERKEY", "tableName": "LINEITEM"},
                    {"type": "column", "name": "L_EXTENDEDPRICE", "tableName": "LINEITEM"},
                ],
                "selectListDataTypes": [
                    {"type": "decimal", "precision": 20, "scale": 0},
                    {"type": "decimal", "precision": 18, "scale": 2},
                ],
                "orderBy": [{
                    "type": "order_by_element",
                    "expression": {"type": "column", "columnNr": 1, "name": "L_EXTENDEDPRICE", "tableName": "LINEITEM"},
                    "isAscending": false,
                    "nullsLast": true
                }]
                // No "limit" key: no LIMIT clause anywhere.
            }
        });
        let files = vec![
            ("s3://w/part-0.parquet".to_string(), 1000u64),
            ("s3://w/part-1.parquet".to_string(), 1000u64),
        ];
        let sql = plan_scan_sql(&request, files, 2);

        assert!(
            sql.contains(r#"ORDER BY "L_EXTENDEDPRICE" DESC NULLS LAST"#),
            "no-LIMIT decline must still render a self-contained outer ORDER BY: {sql}"
        );
        assert!(
            !sql.contains("LIMIT"),
            "no LIMIT was requested, so none must be synthesized: {sql}"
        );
        let common = common_arg_literal(&sql);
        assert!(
            !common.contains("order_by") && !common.contains("\"limit\""),
            "per-shard common blob must stay clean (no sort keys, no limit): {common}"
        );
    }

    /// cap-ext scenario (#198): a pushed `ORDER BY` over a SINGLE-GROUP aggregate
    /// keeps the request's `LIMIT` — `SELECT COUNT(*) … ORDER BY COUNT(*) LIMIT 0`
    /// must return ZERO rows, not the one-row aggregate.
    ///
    /// Driven through the `plan_scan_sql` COMPOSITION mirror, not
    /// `build_scan_driving_sql` directly, and that is load-bearing: the leaf
    /// renderer takes no `orderBy`, so calling it directly could only hand-feed
    /// `request_limit: Some(0)` — the exact value production must derive for
    /// itself — and would pass with task 5.1's plumbing absent. The mirror instead
    /// reproduces the full dispatch: `order_by_present` is true, `detect_topn` is
    /// skipped because `aggregates.is_some()`, and the shared `effective_limit`
    /// guard therefore yields `None`. So a rendered `LIMIT 0` can only have arrived
    /// via the separate raw-`limit` → `request_limit` channel.
    ///
    /// Both halves are asserted: the outer merge SELECT ends in `LIMIT 0`, AND the
    /// per-shard common blob still carries NO `limit`. Together they pin the
    /// plumbing, the render site, and the untouched `effective_limit` withholding —
    /// a leaked per-shard `LIMIT 0` would zero out each shard's partial instead.
    #[test]
    fn aggregate_merge_renders_request_limit_zero_through_plan_composition() {
        let request = serde_json::json!({
            "involvedTables": [{
                "name": "LINEITEM",
                "columns": [
                    {"name": "L_ORDERKEY", "dataType": {"type": "decimal", "precision": 20, "scale": 0}},
                    {"name": "L_EXTENDEDPRICE", "dataType": {"type": "decimal", "precision": 18, "scale": 2}},
                ],
            }],
            "pushdownRequest": {
                "type": "select",
                "aggregationType": "single_group",
                "selectList": [agg_item("COUNT", None, false)],
                "selectListDataTypes": [{"type": "decimal", "precision": 20, "scale": 0}],
                "orderBy": [{
                    "type": "order_by_element",
                    "expression": agg_item("COUNT", None, false),
                    "isAscending": false,
                    "nullsLast": true
                }],
                "limit": {"numElements": 0}
            }
        });
        let files = vec![
            ("s3://w/part-0.parquet".to_string(), 1000u64),
            ("s3://w/part-1.parquet".to_string(), 1000u64),
        ];
        let sql = plan_scan_sql(&request, files, 2);

        assert!(
            sql.ends_with(" LIMIT 0"),
            "the outer aggregate merge SELECT must render the request's LIMIT 0: {sql}"
        );
        let common = common_arg_literal(&sql);
        assert!(
            !common.contains("\"limit\""),
            "effective_limit stays withheld: the per-shard common blob must carry no \
             limit, or each shard's partial aggregate would be zeroed out: {common}"
        );
    }
}
