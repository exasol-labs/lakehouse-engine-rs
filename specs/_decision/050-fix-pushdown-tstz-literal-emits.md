# Decisions: fix-pushdown-tstz-literal-emits

## ADR: Reject the EMITS-type substitution: it is both value-lossy and rejected by Exasol

**ID:** reject-emits-type-substitution-for-tstz-literal
**Plan:** fix-pushdown-tstz-literal-emits
**Status:** Accepted

### Context

The brief proposed emitting a `TIMESTAMP WITH LOCAL TIME ZONE`-declared select-list item under
EMITS type plain `TIMESTAMP`, reasoning that Exasol stores/exchanges TSTZ as a UTC instant and the
declared EMITS type only affects `coerce_batch_to_exa_types`. Verified against the live E2E Exasol
container (2025.2.1, `SESSIONTIMEZONE = EUROPE/BERLIN`):
`CAST(TIMESTAMP '2024-03-01 10:00:00' AS TIMESTAMP WITH LOCAL TIME ZONE)` displays
`2024-03-01 10:00:00`, while its UTC representation — the value the UDF would emit under the
proposed substitution — is `2024-03-01 09:00:00`. Independently, Exasol validates the pushdown
response's per-column types positionally against `selectListDataTypes` and rejects a substituted
type outright.

### Decision

Do not substitute plain `TIMESTAMP` for a declared `TIMESTAMP WITH LOCAL TIME ZONE` EMITS type.

### Options Considered

| Option | Verdict |
|--------|---------|
| Substitute the type as the brief proposed | ✗ Rejected — value-lossy (surfaces the UTC wall clock where Exasol natively surfaces the session-local wall clock) AND rejected outright by Exasol's positional type check |
| Reject the substitution and find another repair | ✓ Chosen — see the routing ADR below |

### Consequences

The brief's decisive analogy — decision `007-fix-timestamptz-mapping` — differs in exactly the
load-bearing respect: there the VS DECLARES the Iceberg column plain `TIMESTAMP` at
`createVirtualSchema`, so Exasol makes no localization promise and the UTC wall clock IS the
contract. Here Exasol has already inferred TSTZ for the select-list item from its own expression
analysis, independent of the adapter's schema, so a localization promise exists. Every premise in
the brief was individually correct; the composition was not.

## ADR: Route a non-emittable select-list item to the qualified single-table wrapper

**ID:** route-non-emittable-selectlist-to-qualified-wrapper
**Plan:** fix-pushdown-tstz-literal-emits
**Status:** Accepted

### Context

A select-list item the scan UDF cannot emit — declared EMITS-invalid (`TIMESTAMP WITH LOCAL TIME
ZONE`) or session-context-dependent — previously fell back to responding with the full base row.
Verified live that this is an INVALID pushdown response, not a correct-but-unaccelerated one:
Exasol validates the response positionally against the request's `selectList` and rejects a
column-count mismatch with SQL state `04000` ("Expected number of columns is N but pushdown query
has M"), so the query FAILS outright. Reproduced for the literal branch, the scalar branch, a TSTZ
`FN_CAST` over a column, and every item position.

### Decision

Route the whole request to `qualified_single_table_fallback_pushdown` — the shape the
grouped-aggregate and multi-`COUNT(DISTINCT)` declines already use — instead of responding with the
full base row.

### Options Considered

| Option | Verdict |
|--------|---------|
| Route to the qualified single-table wrapper | ✓ Chosen — the mechanism already exists, is already specified normatively for two other decline shapes, and its documented contract ("the result column count and per-column types match Exasol's positional `selectListDataTypes` validation") is exactly the requirement |
| Keep declining to the full base row | ✗ Rejected — verified `04000` hard failure at every item position, not a correct-but-slow path |
| Append the item as a flat sibling scalar expression next to `LAKEHOUSE_SCAN(...) EMITS (...)` | ✗ Rejected — an EMITS call expands to a contiguous column block, so an item between two scan columns cannot be positioned, and a bare `SELECT CURRENT_TIMESTAMP FROM t` needs exactly one output column while the scan must still emit at least one to drive the rows |
| Withdraw the session-dependent capabilities so Exasol never delegates the item | ✗ Rejected (independently, this direction shipped anyway via the unrelated `fix-vs-expression-dialect` plan) — capabilities are global, not per-clause: withdrawal would also kill `WHERE ts < CURRENT_TIMESTAMP` predicate pushdown and Iceberg timestamptz-literal file pruning, and cannot cover `FN_CAST` to TSTZ, which is not separately withdrawable |
| Carry the value as a VARCHAR bearing a UTC offset | ✗ Rejected — Exasol's VARCHAR → TSTZ conversion honors only `NLS_TIMESTAMP_FORMAT` and rejects an offset suffix (SQL state 22018) |
| Read `SESSIONTIMEZONE` in the adapter over connect-back and compensate | ✗ Rejected — connect-back opens an independent session and cannot observe the user session's zone |

### Consequences

The wrapper returns the session-local value, reproduces a TSTZ literal exactly, supports arbitrary
column interleaving, and yields column type `TIMESTAMP(3) WITH LOCAL TIME ZONE` — verified
end-to-end at the SQL level against the deployed scan UDF. The routing predicate is reason-based
over the request rather than an arity comparison, so it cannot fire on the absent, empty, or
non-array `selectList` arms where the full base row is the correct response.

## ADR: Fix #218 rather than close it as a permanent design boundary

**ID:** fix-218-not-permanent-design-boundary
**Plan:** fix-pushdown-tstz-literal-emits
**Status:** Accepted

### Context

A round-1 draft of this plan concluded #218 should be closed as a permanent, deliberate design
boundary, resting on the premise that the full-base-row decline yields a correct-but-unaccelerated
result. Plan review found that premise unproven and required verification against the real pushdown
path rather than raw SQL alone.

### Decision

Rewrite the affected spec scenarios as a real fix — the item routes to the qualified wrapper and
Exasol evaluates it — and let the implementing PR close `#218` with `Closes #218`.

### Options Considered

| Option | Verdict |
|--------|---------|
| Fix #218 for real | ✓ Chosen — a verified fix exists at moderate cost, reusing an already-shipped mechanism |
| Close #218 as a permanent design boundary | ✗ Rejected — rested on a false premise: the decline is a hard query failure, not a correct-but-unaccelerated result, so there was nothing defensible to record as a boundary |
| Leave the exception open with no disposition | ✗ Rejected — a verified fix exists |

### Consequences

CLAUDE.md requires a known deviation to be either fixed or recorded as an accurately-scoped tracked
exception. The residual defects found while verifying are tracked instead: `#239` (filter-side
now-family divergence, independently resolved as a side effect of an unrelated capability
withdrawal), `#240` (`CHAR(n)` positional type mismatch), `#242` (the `literal_timestamputc`
wire-name defect's DataFusion half and the dead Iceberg `timestamptz` pruning arm), and `#231` (the
same routing gap on the broadcast-join path). `FN_CAST` to TSTZ over a column is excluded as a scope
boundary rather than filed as a new issue: it is the same `#218` `04000`, and after this fix it
fails with a named adapter error instead.

## ADR: Route from a reason-based predicate above `project_columns`, not from inside it

**ID:** reason-based-predicate-routes-above-project-columns
**Plan:** fix-pushdown-tstz-literal-emits
**Status:** Accepted

### Context

`project_columns` is called from two join call sites (`joins/rendering.rs:36`,
`joins/mod.rs:138`) in addition to the single-table row-scan path. A design that changed
`project_columns`' return type (e.g. a `SelectListPlan` enum) would force both join call sites to
handle the new variant — exactly the join-side change issue `#231` owns separately.

### Decision

Add a pure predicate over the request, callable from both the resolved-file dispatcher
(`build_dispatch_sql`) and the zero-file short-circuit (`empty_result_sql`), and leave
`project_columns`' signature and behavior unchanged.

### Options Considered

| Option | Verdict |
|--------|---------|
| A pure predicate above `project_columns`, called from both routing sites | ✓ Chosen — mirrors the existing `classify_request_shape` design: both paths route from one decision so their column shapes cannot drift; keeps both join call sites byte-identical |
| Change `project_columns` to return a `SelectListPlan` enum | ✗ Rejected — forces both join call sites to handle the new variant, which is the join-side change `#231` owns |
| PR #229's trigger — compare `selectList` length against `proj_cols.len()` | ✗ Rejected as broader than needed — an arity comparison also fires on the absent, empty, and non-array `selectList` arms, where the full base row IS the correct response |

### Consequences

The join path keeps today's behavior exactly, so issue `#231`'s description of the same gap on the
broadcast-join path stays accurate. At implementation time, this predicate was found to already
exist under a different name (`needs_full_fallback`/`projection_widened`), shipped by the
independent `fix-vs-expression-dialect` plan between this plan's drafting and its implementation —
this plan's own tasks for it were dropped as redundant, and new tests instead pin the
already-shipped mechanism as a regression guard.

## ADR: "No fix exists" was unproven — an unexplored alternative led to a verified real fix

**ID:** verified-real-fix-overturns-no-fix-exists-premise
**Plan:** fix-pushdown-tstz-literal-emits
**Status:** Accepted

### Context

Plan review confirmed the round-1 value-lossy physics of the proposed EMITS-type substitution but
found the round-1 "no fix exists" conclusion unproven, naming an unexplored alternative — an
Exasol-side sibling expression beside the emitting scan call — and requiring verification against
the real VS `pushdown` response path rather than raw SQL alone.

### Decision

Verify against the real pushdown path before concluding either way, rather than accepting the
round-1 conclusion.

### Options Considered

| Option | Verdict |
|--------|---------|
| Verify against the live E2E container and the real pushdown response | ✓ Chosen — overturned the plan's central premise entirely |
| Accept round-1's "no fix exists, permanent boundary" conclusion | ✗ Rejected — rested on an unverified assumption the reviewer correctly challenged |

### Consequences

`EXPLAIN VIRTUAL` and a plain query through the deployed VS showed
`SELECT CURRENT_TIMESTAMP FROM <vs_t> WHERE ID = 1` FAILS today with SQL state `04000` — Exasol
validates the pushdown response positionally against the request's `selectList`, so the full-base-row
decline is an INVALID response, not a correct-but-unaccelerated one. This made both issue #218's own
"Impact: Low, correctness is preserved" premise and the round-1 plan conclusion wrong. The reviewer's
own suggested sibling-expression shape was then evaluated and rejected on its own merits (see the
routing ADR above); the chosen fix instead reuses the already-shipped, already-specified qualified
single-table wrapper.
