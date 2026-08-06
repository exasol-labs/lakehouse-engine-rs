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
#[path = "topn_tests.rs"]
mod tests;
