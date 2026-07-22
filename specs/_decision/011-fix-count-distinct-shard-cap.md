# Decisions: fix-count-distinct-shard-cap

## ADR: Native-merge via Exasol's own COUNT(DISTINCT)

**ID:** count-distinct-native-merge
**Plan:** `fix-count-distinct-shard-cap`
**Status:** Accepted

### Context

`COUNT(DISTINCT col)` over a high-cardinality column failed with `ResourcesExhausted`.
The prior path made each shard compute a local distinct set, serialize it to a JSON
array bounded by a fixed per-shard byte/element cap, and union the per-shard arrays in a
scalar merge UDF. Cross-shard dedup needs every distinct value's text, so the per-shard
cost is `O(cardinality × value-width)` against a fixed budget — a real high-cardinality
column exceeds it.

### Decision

Each shard streams one row per shard-local distinct value; the outer wrapper runs a
plain `COUNT(DISTINCT "V")` over the union of those rows, letting Exasol's own aggregate
engine perform the cross-shard deduplication with its own spill and resize behavior.

### Options Considered

| Option | Verdict |
|--------|---------|
| Native `COUNT(DISTINCT "V")` merge | ✓ Chosen — byte-exact and free of an `O(cardinality)` per-shard budget without redundant I/O |
| Raise the fixed caps | ✗ Rejected — still `O(cardinality)`, only moves the ceiling |
| Fixed-width hash tokens | ✗ Rejected — still `O(cardinality)` |
| Value-hash shuffle | ✗ Rejected — forces every shard to scan every file, violating file-level no-overlap sharding |
| HyperLogLog / mergeable sketch | ✗ Rejected — approximate, violating the exact-count requirement |

### Consequences

The per-shard cap and its serialization code become dead. Distinct cardinality is
bounded only by Exasol's own distinct-aggregate engine, not by a fixed per-shard budget.

## ADR: Remove AggKind::CountDistinct entirely — reframe as a DISTINCT row-scan

**ID:** count-distinct-remove-aggkind-variant
**Plan:** `fix-count-distinct-shard-cap`
**Status:** Accepted

### Context

Once cross-shard dedup becomes Exasol's job (native-merge ADR above), `COUNT(DISTINCT
col)` no longer needs a per-shard aggregate partial — it needs one row per shard-local
distinct value, which is structurally a row-scan, not a partial aggregate.

### Decision

Delete the `AggKind::CountDistinct` variant, its `array_agg(DISTINCT)` partial, its
JSON/cap code, and the `LAKEHOUSE_DISTINCT_MERGE_COUNT` UDF. Detect `COUNT(DISTINCT
col)` at the same point but emit a row-scan-shaped spec (single-item projection +
`distinct` flag + NULL-excluding filter), reusing the existing `emit_batch` streaming
path.

### Options Considered

| Option | Verdict |
|--------|---------|
| Row-scan + `.distinct()`, reusing existing streaming path | ✓ Chosen — no new wire type or aggregate partial; removes an enum variant, a whole UDF, and its cap/serialization logic |
| Reinterpret the existing `CountDistinct` variant, keep partial/merge machinery | ✗ Rejected — preserves dead machinery once dedup moves to Exasol |

### Consequences

The codebase shrinks: an enum variant, a whole UDF entry point, and its cap/serialization
logic disappear, replaced by infrastructure that already exists for row scanning.

## ADR: The count stays byte-exact — approximate distinct is rejected

**ID:** count-distinct-exact-not-approximate
**Plan:** `fix-count-distinct-shard-cap`
**Status:** Accepted

### Context

Scaling `COUNT(DISTINCT)` past a fixed per-shard cap could be solved with an
industry-standard approximate sketch (HyperLogLog and similar, ~1-2% error) instead of
an exact merge.

### Decision

`COUNT(DISTINCT col)` MUST remain exact. Sketch-based counting is out of scope.

### Options Considered

| Option | Verdict |
|--------|---------|
| Exact native `COUNT(DISTINCT "V")` merge | ✓ Chosen — scales without trading exactness, per the native-merge ADR |
| Fixed-size mergeable sketches (HLL) | ✗ Rejected — trades exactness for scale, forbidden by the project's correctness-first mission |

### Consequences

No approximate-counting code path exists or is planned for `COUNT(DISTINCT)`; every
scaling fix for this function must preserve byte-exact results.

## ADR: Apply to every query shape via dedicated per-distinct fan-outs

**ID:** count-distinct-per-distinct-fan-outs
**Plan:** `fix-count-distinct-shard-cap`
**Status:** Accepted

> Superseded by `count-distinct-case-2-3-row-scan-fallback` — Exasol rejects an emitting
> UDF nested in a scalar subquery at compile time (`sqlCode 04000`), proven in live E2E.
> Retained for history; Case 1 (a lone single-group `COUNT(DISTINCT)`) is unaffected and
> remains as designed.

### Context

A distinct column needs one row per local distinct value, incompatible with the
one-row-per-shard partial-aggregate shape shared aggregates use. Every query shape,
including multiple distinct columns and a distinct mixed with ordinary aggregates, was
initially planned to reuse the same per-column fan-out mechanism.

### Decision

Give every `COUNT(DISTINCT col)` in a query its own dedicated fan-out, composed as an
independent SELECT-list scalar subquery (Case 2); non-distinct aggregates keep their
shared partial-aggregate scan with distinct subqueries bolted on (Case 3).

### Options Considered

| Option | Verdict |
|--------|---------|
| Independent scalar-subquery fan-out per distinct column | ✗ Rejected in practice — Exasol rejects an emitting UDF nested in a scalar subquery at compile time (`sqlCode 04000`) |
| Support only the single-distinct-only repro shape | ✗ Rejected at the time — considered too narrow a fix |

### Consequences

Live E2E proved this design does not compile in Exasol for any multi-distinct or
mixed-aggregate shape; see `count-distinct-case-2-3-row-scan-fallback` for the immediate
replacement and `count-distinct-case-2-3-qualified-wrapper` for the final design.

## ADR: Case 2/3 declines to the row-scan fallback

**ID:** count-distinct-case-2-3-row-scan-fallback
**Plan:** `fix-count-distinct-shard-cap`
**Status:** Accepted
**Supersedes:** count-distinct-per-distinct-fan-outs

> Superseded by `count-distinct-case-2-3-qualified-wrapper` — the decline TRIGGER (Case
> 2/3) and the reason the prior design failed are unchanged. What changed is the decline
> TARGET: a bare row-scan fallback assumed Exasol re-aggregates a declined pushdown,
> which round-2 review found false. Retained for history.

### Context

Live E2E (`q9b_multi_count_distinct_matches_single_node`) showed Exasol rejects an
emitting UDF call nested inside a SELECT-list scalar subquery at compile time (`sqlCode
04000`, "emitting function in expression") — a hard SQL-compilation restriction, not a
bug in the generated SQL's specific shape. No composition of multiple scalar-subquery
UDF calls in one SELECT list can work.

### Decision

Keep Case 1 (exactly one single-group `COUNT(DISTINCT)`, nothing else) exactly as
shipped. DECLINE Case 2/3 (more than one distinct item, or a distinct alongside any
ordinary aggregate): detection returns no pushable aggregate, so the request falls
through to the existing plain single-group row-scan fallback and Exasol's own engine
computes every aggregate over the returned rows.

### Options Considered

| Option | Verdict |
|--------|---------|
| Decline to the existing plain row-scan fallback | ✓ Chosen at the time — the clean, already-existing path, simpler than reshaping the fan-out SQL |
| Reshape the multi-distinct SQL some other scalar-subquery composition | ✗ Rejected — every shape still nests an emitting UDF in a scalar subquery, all `04000` |
| UNION-ALL of fan-outs plus outer grouping | ✗ Rejected — needlessly complex vs. the row-scan fallback |

### Consequences

Case 2/3 streams the referenced columns' rows and lets Exasol dedup/aggregate — more
rows on the wire than a fan-out would have. Round-2 review found the premise false
(Exasol never re-aggregates a declined pushdown); see
`count-distinct-case-2-3-qualified-wrapper` for the corrected design.

## ADR: Case 2/3 routes to a qualified single-table wrapper

**ID:** count-distinct-case-2-3-qualified-wrapper
**Plan:** `fix-count-distinct-shard-cap`
**Status:** Accepted
**Supersedes:** count-distinct-case-2-3-row-scan-fallback

### Context

The row-scan-fallback design asserted "Exasol's own engine computes every aggregate
over the returned rows." That is false: Exasol never re-aggregates a declined pushdown —
the adapter's returned SQL is the final answer it runs as-is. A bare row scan returns
the raw source columns where the request's `selectListDataTypes` expects N aggregate
columns, rejected at pushdown-validation time (`sqlCode 04000`, "Expected number of
columns is 1 but pushdown query has N" — the same bug class as issue #57). The original
scalar-subquery design (see `count-distinct-per-distinct-fan-outs`) failed for a
different `04000` (emitting UDF in a scalar subquery); both failures share one root
cause — the adapter's own SQL must produce the final shape.

### Decision

DECLINE the Case 2/3 fan-out and route to a qualified single-table wrapper — the same
pattern the grouped-aggregate decline fallback already uses
(`build_grouped_qualified_fallback_sql`, renamed `build_qualified_single_table_fallback_sql`
to serve both grouped and single-group declines). A new guard in `mod.rs`, before the
bare row scan and mirroring the grouped guard, intercepts the declined single-group
aggregate. The wrapper renders the exact single-group select list — every aggregate,
including each `COUNT(DISTINCT)`, spliced verbatim — over a materialized sharded raw
scan aliased once, so the adapter's own SQL produces the one-row N-column aggregated
result and Exasol passes it through. Both this wrapper and the grouped decline wrapper
obtain their inner-scan projection from one shared referenced-column helper, since
`proj_cols` cannot narrow an aggregate-shaped request (it is the full row via
`project_columns`'s `full_row()` fallback).

### Options Considered

| Option | Verdict |
|--------|---------|
| Qualified single-table wrapper reusing the grouped decline builder | ✓ Chosen — structurally identical to the proven grouped decline; aligns the non-empty shape with the already-correct empty-result shape |
| Bare row scan | ✗ Rejected — `04000` column-count mismatch, since Exasol never re-aggregates a declined pushdown |
| Per-distinct scalar subqueries | ✗ Rejected — `04000` emitting UDF in a scalar subquery |
| UNION-ALL of fan-outs plus outer grouping | ✗ Rejected — needlessly complex vs. reusing the existing qualified-wrapper builder |

### Consequences

Case 2/3 now compiles and returns the correct N-column aggregate shape in both the
empty and non-empty case. The single-group and grouped decline paths share one
referenced-column narrowing helper, closing issue #160 (the grouped fallback's
whole-table projection) in the same change.

## ADR: Narrow Case 1 to bare-column arguments — expression-argument distinct always routes to the qualified wrapper

**ID:** count-distinct-case-1-bare-column-only
**Plan:** `fix-count-distinct-review-findings`
**Status:** Accepted

### Context

`build_distinct_fan_out` (`support.rs`) declares the per-shard fan-out's value column
`"V"` with the argument's real Exasol type for a bare column, but `VARCHAR(2000000)` for
an expression argument — relying on `arrow::compute::cast(.., Utf8)` being injective on
the expression's native output type. That injectivity assumption was never proven and is
not generally true (e.g. two distinct timestamps can print identically after
string-cast truncation), so cross-shard dedup on the string form can silently undercount.
This convention was never captured as its own entry in this file; it existed only in
`build_distinct_fan_out`'s doc comment and code. The Case 2/3 qualified single-table
wrapper (`count-distinct-case-2-3-qualified-wrapper`, above) already evaluates every
aggregate, including `COUNT(DISTINCT <expr>)`, natively over exact-typed base columns
with no cast step — it was simply never the route a LONE expression-argument distinct
took.

### Decision

Narrow `is_lone_count_distinct` to require a bare-column argument
(`dc.column.is_some()`). A `COUNT(DISTINCT <expression>)` — lone or combined with any
other aggregate — no longer reaches `build_distinct_fan_out` at all: it always falls
into the existing `has_distinct && !is_lone_count_distinct` guard and routes to the
qualified single-table wrapper, exactly like a genuine multi-distinct or
mixed-aggregate request. The fan-out's `value_type` match collapses to the bare-column
case; the `None => "VARCHAR(2000000)"` arm and its cast-injectivity dependency are
deleted.

### Options Considered

| Option | Verdict |
|--------|---------|
| Route lone expression-argument distinct to the qualified wrapper | ✓ Chosen — the wrapper is already built, tested (Case 2/3), and exact; it needs no cast and no injectivity assumption |
| Keep the VARCHAR fan-out; document and test the injectivity assumption | ✗ Rejected — still approximate in principle; a documented assumption is not a proof, and a passing test cannot cover every Arrow type's string-cast behavior |
| Keep the VARCHAR fan-out; allowlist "safe to string-cast" expression output types | ✗ Rejected — adds an ongoing maintenance surface (a type allowlist) to preserve a shortcut the wrapper makes unnecessary |

### Consequences

Every `COUNT(DISTINCT <expression>)`, lone or combined, is now exact with no cast step
and no injectivity dependency — the same guarantee the wrapper already gives Case 2/3.
The `VARCHAR(2000000)` value-type arm and its dedicated test become dead code and are
removed. Case 1 now only ever fans out a bare column; its `"V"` column always carries
that column's real Exasol type, never a string-serialized intermediate value.

## Follow-up: Exasol-dialect CAST for the qualified wrapper

**Plan:** `fix-count-distinct-shard-cap`
**Status:** Accepted

The qualified single-table wrapper (`count-distinct-case-2-3-qualified-wrapper`) and the
grouped-aggregate outer-merge wrapper render their SELECT/WHERE fragments through
`crates/vs-expression`, whose CAST-target renderer mapped every `VARCHAR`/`CHAR` target to a
bare, length-less `VARCHAR`. That is correct only for the DataFusion-side scan-spec fragments
(`filter`/`projection`/`group_keys`): datafusion-sql rejects `VARCHAR(n)` with a length
unless `support_varchar_with_length` is enabled, which this project does not. But the wrapper
SQL is parsed by Exasol's OWN core engine, whose `VARCHAR` type has no length-less form — a
bare `VARCHAR` is a hard parse error (`sqlCode 04000`, "unexpected ')', expecting '('"), which
broke `COUNT(DISTINCT CAST(<col> AS CHAR(20)))` once it was routed to the wrapper (E2E
`count_distinct_expression_arg_via_wrapper_matches_single_node`).

The CAST-target renderer therefore split by dialect. `render_expression` and its `_safe`
twins keep the bare-`VARCHAR` DataFusion behavior for scan-spec fragments; new
`render_expression_exasol` / `render_expression_exasol_safe` / `render_df_filter_exasol_safe`
twins length-qualify character targets (`VARCHAR(n)`; `CHAR(n)` also → `VARCHAR(n)` per the
mission data-type table, from the width Exasol itself sent) and are used ONLY where Exasol
parses the rendered SQL: the qualified single-table / N-scan join wrapper (`joins.rs`) and the
grouped-merge wrapper (`grouped_agg.rs`). The dialect flows through a private `CastDialect`
parameter threaded across the shared recursive translator; the DataFusion-parsed
broadcast-join condition/filter and the grouped `group_keys`/renderability check stay on the
default DataFusion dialect.
