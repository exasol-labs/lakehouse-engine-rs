# Decisions: fix-225-orderby-non-projected-column

## ADR: Extend the scan's emitted columns; never widen the visible projection

**ID:** hidden-sort-key-columns-not-full-row-widening
**Plan:** fix-225-orderby-non-projected-column
**Status:** Accepted

### Context

A pushed-down `ORDER BY <col>` fails when `<col>` is not a bare select-list item
(issue #225, same root cause as #189). The existing fix for a related bug (#190) widens
the adapter's derived projection to the full base row so the declined-`ORDER BY` wrapper's
outer `ORDER BY` resolves, but Exasol validates a returned pushdown query's column count
positionally against the original select list, so a widened row is rejected with
`sqlCode 04000`.

### Decision

On the declined-`ORDER BY` path, append each unprojected bare sort-key column, resolved by
name from `col_types`, to `proj_cols`/`proj_types` AFTER every original item, and have the
wrapper name only the original items explicitly via `emits_ident`. The scan's
emitted-column set and the query's visible column set become two different sets instead of
being forced equal by widening.

### Options Considered

| Option | Verdict |
|--------|---------|
| Append hidden sort-key columns after the original projection, name only originals in the wrapper | ✓ Chosen — preserves every original select-list index by construction, so `emits_ident` stays aligned without a second hand-maintained rule; matches issue #189's own suggested fix |
| Keep the full-base-row widening and add an explicit outer select list | ✗ Rejected — fixes arity but still scans and transports every base column for a narrow query |
| Decline the pushdown entirely for this shape | ✗ Rejected — a hard, user-visible failure for a very common shape (`SELECT a FROM t ORDER BY b`) |
| Drop the pushed `orderBy` and let Exasol sort | ✗ Rejected — Exasol does not re-apply a delegated `orderBy` once `ORDER_BY_COLUMN` is advertised, so this silently returns unordered rows |

### Consequences

A declined `ORDER BY` on an unprojected column now returns the correct rows in the correct
order with the select list's exact arity, instead of failing with `sqlCode 04000`. The scan
transports a small number of extra hidden columns rather than every base-table column.

## ADR: Run the projection extension after top-N detection, not before

**ID:** extend-order-by-projection-after-topn-detection
**Plan:** fix-225-orderby-non-projected-column
**Status:** Accepted

### Context

The pre-existing #190 guard ran its projection widening BEFORE `detect_topn`, on the
argument that a widened projection lets a bounded top-N match — "a strictly better,
equally well-formed outcome" — because every widened column stayed visible. That argument
does not hold for the new hidden-column extension: the matched top-N path emits `proj_cols`
directly as the FINAL visible EMITS with no wrapping select, so a hidden column reaching it
would leak into the result and reintroduce the arity mismatch.

### Decision

`detect_topn` is called on the ORIGINAL, pre-extension `proj_cols`; the hidden-sort-key
extension runs only once the shape is known to be declined, and — separately — before
`spec_template` so the EMITS clause and the scan-spec projection stay consistent.

### Options Considered

| Option | Verdict |
|--------|---------|
| Extend after `detect_topn`, before `spec_template` | ✓ Chosen — keeps the bounded top-N path's final EMITS free of hidden columns while keeping the declined path's EMITS and scan-spec projection consistent |
| Keep the extension before `detect_topn`, as the #190 guard does | ✗ Rejected — a hidden column reaching the matched top-N path leaks into the result and reintroduces the arity mismatch the fix is meant to close |

### Consequences

A shape whose sort key falls outside the derived projection now declines to an unbounded
per-shard scan instead of matching a bounded top-N over a widened projection. That widened
match never returned a usable result before (`sqlCode 04000`), so this is a correctness
gain, not a regression, though a bounded variant for these shapes remains future work.
Ordering is load-bearing for correctness, not stylistic, so it is pinned by a
dispatcher-level test (`declined_order_by_extension_runs_after_topn_detection`) fixtured so
`detect_topn` could match if the extension ran early.

## ADR: State ORDER BY correctness rules against the derived projection, not the select list

**ID:** state-order-by-rules-against-derived-projection-not-select-list-arity
**Plan:** fix-225-orderby-non-projected-column
**Status:** Accepted

### Context

`extract_projection`/`project_columns` has its own, separate full-base-row fallback
(`needs_full_fallback`) for an untranslatable select-list item, an unknown or aggregate
node, or a declared EMITS type Exasol rejects — mandated by the recorded scenario
"Projected constant whose declared EMITS type Exasol rejects declines to the full base
row". `proj_cols.len()` is therefore the length of that derived projection, not reliably
the select-list arity: for a select-list item that already trips that fallback, the
projection is already the full base row before this fix's extension runs, the extension is
inert, and the wrapper still returns every base column — still `sqlCode 04000`. Stating
this fix's guarantee against "the select list" instead of "the derived projection" would
make the recorded feature self-contradictory.

### Decision

Every rule this fix adds is stated against the adapter's DERIVED PROJECTION — the
projection-item list the adapter builds from the select list — never against the raw
select-list arity. A request whose derived projection the separate full-base-row fallback
has already widened routes to the qualified single-table wrapper before the
declined-`ORDER BY` path runs, and is out of this fix's scope.

### Options Considered

| Option | Verdict |
|--------|---------|
| State every guarantee against the derived projection, and name the composed pre-existing gap explicitly | ✓ Chosen — matches what the code actually guarantees and does not contradict the recorded EMITS-fallback scenario |
| State the guarantee against the raw select-list arity | ✗ Rejected — false whenever `extract_projection`'s own fallback has already widened the projection, and self-contradicts the recorded sibling scenario |

### Consequences

The composed gap — a select-list item that trips `extract_projection`'s own fallback,
combined with an `ORDER BY` on a column outside even that widened set — remains open and is
tracked as an accurately-scoped exception rather than silently implied fixed.

## ADR: Pin the detect_topn-then-extend ordering with a mis-ordering-capable test

**ID:** pin-extension-ordering-with-a-mis-ordering-capable-dispatcher-test
**Plan:** fix-225-orderby-non-projected-column
**Status:** Accepted

### Context

Calling `detect_topn` on the original, pre-extension `proj_cols` is load-bearing for
correctness (see the projection-extension-ordering ADR above), but the plan's original
regression coverage could not actually fail on a mis-ordered implementation: it only
asserted that `detect_topn` over the pre-extension projection returns `None`, true
regardless of call order, and the fixtures used to force a decline (an empty
`logical_schema`, an absent `LIMIT`) were themselves order-blind. A future implementation
could reintroduce the exact `sqlCode 04000` bug with a green `cargo test`.

### Decision

Replace the order-blind unit assertion with a dispatcher-level test
(`declined_order_by_extension_runs_after_topn_detection`) fixtured so `detect_topn` COULD
match if the extension ran early — a literal-only select list, `ORDER BY "NAME"`, a
populated `LIMIT`, and a `logical_schema` that types `NAME` — asserting the common blob
carries no `"limit"`/`"order_by"` and that the outer `SELECT "_LH_PROJ_0" FROM (` wrapper is
present.

### Options Considered

| Option | Verdict |
|--------|---------|
| A dispatcher-level test fixtured to make `detect_topn` eligible if run after the extension | ✓ Chosen — the only fixture shape that actually fails if the ordering regresses |
| Keep the original `detect_topn`-only assertion over the pre-extension projection | ✗ Rejected — true regardless of call order, so it cannot detect a mis-ordered implementation |

### Consequences

A future change that reorders the extension ahead of `detect_topn` now fails this test
instead of silently reintroducing the `sqlCode 04000` regression.
