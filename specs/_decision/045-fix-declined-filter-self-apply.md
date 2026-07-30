# Decisions: fix-declined-filter-self-apply

## ADR: There is no Exasol-side fallback for a predicate whose capability the adapter advertised

**ID:** no-exasol-side-fallback-for-an-advertised-capability
**Plan:** fix-declined-filter-self-apply
**Status:** Accepted

### Context

Three sites rendered a DataFusion-bound WHERE filter and treated a declined render as safe to
omit, assuming Exasol would keep and re-evaluate the predicate itself. Verified live against the
Docker Exasol stack (`exasol/docker-db:2025.2.1`), three separate decline sources each returned all
12 rows of `TYPED_DISTINCT_PROBE` where 0, 3, and 7 were correct, with `EXPLAIN VIRTUAL` confirming
the emitted SQL carried neither a scan-spec `"filter"` nor any `WHERE`. The documented pushdown
response carries exactly two fields, `type` and `sql`; `PushDownResponse` holds one member. Exasol
splits the query from the capabilities response alone, before the pushdown request exists, and
post-processes only what the adapter did not advertise.

### Decision

Record as a proven protocol fact that once the capabilities response advertises a predicate or
function shape, Exasol delegates it fully and never independently re-checks or re-applies it. The
adapter owns generating the equivalent SQL for anything it cannot faithfully push to DataFusion.
Recorded in `CLAUDE.md` as a general fact, with no discovery narrative and no issue number.

### Options Considered

| Option | Verdict |
|--------|---------|
| Record the disproven assumption as a corrected protocol fact | ✓ Chosen — proven live three separate ways; the only documented escape hatch (`EXCLUDED_CAPABILITIES`) is whole-capability and whole-schema at DDL time, not per-query |
| Treat the omission as merely unverified and leave it standing | ✗ Rejected — it was disproven, not merely unconfirmed; leaving it would reseed the same defect |

### Consequences

Every declined predicate must be self-applied in the adapter's own returned SQL; omission is never
a correct outcome once a capability is advertised. This fact becomes the shared justification for
every other decision in this plan.

## ADR: The recorded LIKE-guard consequence is corrected, not merely superseded

**ID:** correct-recorded-like-guard-consequence-not-merely-superseded
**Plan:** fix-declined-filter-self-apply
**Status:** Accepted
**Supersedes:** like-guard-in-adapter-not-vs-expression

### Context

`specs/_decision/026-fix-207-like-non-string-column.md`'s Consequences section asserted that a
non-string LIKE "declines pushdown of the whole top-level filter, so Exasol evaluates the predicate
natively instead", mirroring a named all-or-nothing backstop that does not exist. Decisions 031
(HAVING) and 035 (`fix-191-order-by-offset`) each corrected one clause of the same false family
without revisiting the WHERE-filter clause that issues #207, #219, and #215 all inherited
unquestioned.

### Decision

Correct decision 026's Consequences by name: the DECLINE SCOPE it chose — all-or-nothing, never
partial-filter rewriting — remains correct and is retained. Only the stated consequence changes:
the declined filter is applied by the adapter's own outer WHERE, not by Exasol.

### Options Considered

| Option | Verdict |
|--------|---------|
| Correct the record by name, citing the ADR it supersedes | ✓ Chosen — matches this project's explicit precedent (031, 035) for correcting rather than silently superseding a false family |
| Supersede without naming the error | ✗ Rejected — would leave the contradiction unaddressed and reintroduce the defect class in the permanent library |

### Consequences

The decision-log's factual record stays trustworthy across the HAVING, ORDER BY/OFFSET, and now
WHERE-filter clauses of the same false backstop family, closing the third and final recurrence.

## ADR: The single-table decline routes to the existing qualified single-table wrapper

**ID:** single-table-decline-routes-to-the-qualified-single-table-wrapper
**Plan:** fix-declined-filter-self-apply
**Status:** Accepted

### Context

The single-table path serves five request shapes: row scan, top-N, single-group aggregate, grouped
aggregate, and `COUNT(DISTINCT)`. A declined filter needs a place to be evaluated ahead of every
other clause those shapes render.

### Decision

On a filter decline, `build_dispatch_sql` routes the request to
`qualified_single_table_fallback_pushdown`, which renders the ORIGINAL (un-type-rewritten)
predicate as the wrapper's own `WHERE` between the raw fan-out and every other clause, with the
fan-out spec's `filter` set to `None`.

### Options Considered

| Option | Verdict |
|--------|---------|
| Route to the existing qualified single-table wrapper | ✓ Chosen — its fan-out is aggregate-free, sort-free, and LIMIT-free by construction, so the `WHERE` lands correctly for all five shapes; it already builds the `LHS_T0` alias map and renders every other clause in Exasol dialect |
| Wrap the emitted SQL in `SELECT * FROM (<emitted>) WHERE <predicate>` | ✗ Rejected — four of five shapes would filter AFTER aggregation or truncation, which is wrong |
| Add a bespoke row-scan-only wrapper, hard-error on the other four shapes | ✗ Rejected — strictly worse than reusing a path that already handles all five |

### Consequences

The wrapper-free fast path stays untouched for every request whose filter renders; a
materialization boundary appears only on the rare, already-slower decline path. The wrapper gains a
fourth route rather than a new shape.

## ADR: Screen renderability at the render consumer, not inside the shared partition classifier

**ID:** screen-renderability-at-the-render-consumer-not-in-the-partition-classifier
**Plan:** fix-declined-filter-self-apply
**Status:** Accepted

### Context

Plan-review round 1 found that the original design added the DataFusion-renderability condition
inside `side_local_filter`, which has a second production consumer the plan never named:
`plan_join` (`joins/mod.rs:122`) passes its result to `resolve_one_join_side` as that side's Iceberg
manifest-pruning predicate. The condition would have stripped declined conjuncts from pruning too —
more files opened, correct rows, no failing test — contradicting the plan's own non-goals and the
`pushdown-file-pruning` and `pushdown-declined-filter-self-apply` specs.

### Decision

Keep both partition functions (`side_local_filter`, `cross_side_residual_filter`) purely
structural and apply ONE renderability screen — `renderable_only` / `declined_only`, exact
complements — at the two render call sites inside `build_n_scan_join_sql`. Each side's Iceberg
manifest-pruning predicate keeps every side-local conjunct, screened or not.

### Options Considered

| Option | Verdict |
|--------|---------|
| Screen at the two render call sites, leave partition functions structural | ✓ Chosen — leaves the pruning consumer untouched; renderability matters only where rendering happens |
| Add the condition inside `side_local_filter` / `cross_side_residual_filter` | ✗ Rejected — silently degrades Iceberg pruning, a second production consumer the original design never named |
| Render the FULL filter into the outer `WHERE` unconditionally | ✗ Rejected — churns every join query's golden SQL and fails outright on any Exasol-unrenderable conjunct |
| Have `build_side_fan_out_sql` report back which conjuncts it did not push | ✗ Rejected — plumbing for a decision the partition can make directly |

### Consequences

A predicate-shaping condition added to a function serving both a pruning and a rendering consumer
degrades pruning invisibly — the general lesson this finding leaves behind: screen at the consumer,
not in the shared classifier.

## ADR: A residual render errors only on the non-suppressing renderer's `None`, never the suppressing one's

**ID:** error-only-on-the-non-suppressing-renders-none
**Plan:** fix-declined-filter-self-apply
**Status:** Accepted

### Context

Plan-review round 1 found that the original design errored whenever a non-empty residual set
rendered to `None` through `render_df_filter_qualified`. That renderer suppresses a trivially-true
result to `None` exactly as its DataFusion twin does, so a join carrying one trivially-true
top-level conjunct would have turned today's correct "no outer `WHERE`" into a hard client-facing
error — the same three-way `None` conflation this plan exists to delete, one dialect over.

### Decision

Gate the error on the NON-suppressing `render_expression_qualified` returning `None` for the
combined residual tree. Both the single-table wrapper and the N-scan wrapper state three outcomes —
absent, trivially true, unrenderable — and error only on the third.

### Options Considered

| Option | Verdict |
|--------|---------|
| Gate the error on the non-suppressing renderer alone | ✓ Chosen — the suppressing renderer cannot distinguish trivially-true from unrenderable; the non-suppressing one can |
| Error whenever `render_df_filter_qualified` returns `None` | ✗ Rejected — turns a correct no-op predicate into a hard failure |

### Consequences

A renderer that suppresses a no-op result must never decide unrenderability; that decision needs
the non-suppressing entry point. This rule applies identically to both wrappers in this plan.

## ADR: The decline route carries a projection override for absent- or empty-`selectList` shapes

**ID:** decline-route-carries-a-projection-override-for-select-star-shapes
**Plan:** fix-declined-filter-self-apply
**Status:** Accepted

### Context

Plan-review round 2 found that routing every dispatch shape to
`qualified_single_table_fallback_pushdown` on a decline sent a genuine `SELECT *` request (absent,
JSON-null, or empty-array `selectList` — a shape only the new route can reach) to
`referenced_column_projection`, which narrows the projection to the columns the rendered clauses
NAME. Exasol validates the pushdown result positionally, so the narrowed projection would have
turned "wrong rows" into a hard `04000` error or a silently truncated single-column result.

### Decision

Give `qualified_single_table_fallback_pushdown` a second new parameter, a pre-computed projection
override. The decline route passes the full base row (every `col_types` entry, in order, with its
Exasol type) when `selectList` is absent, JSON-null, or an empty array, and `None` everywhere else.
The guard lives at the decline route, not inside `referenced_column_projection`, which stays the
one shared column-narrowing walk for both the wrapper and the join-projection narrowing.

### Options Considered

| Option | Verdict |
|--------|---------|
| Add a projection override parameter at the decline route only | ✓ Chosen — keeps the one shared narrowing walk intact for its other callers |
| Fold the guard into `referenced_column_projection` | ✗ Rejected — that function is shared with the join wrapper's narrowing, which must keep its existing behavior |

### Consequences

Widening the set of request shapes a wrapper serves widens its column-shape contract: a route added
ahead of a classifier inherits every shape the classifier used to divert. The empty-array arity was
confirmed against the live Docker Exasol container rather than assumed from code.

## ADR: A native partial-pushdown acknowledgment mechanism is ruled out, not assumed absent

**ID:** no-native-partial-pushdown-acknowledgment-mechanism-exists
**Plan:** fix-declined-filter-self-apply
**Status:** Accepted

### Context

The interview asked whether Exasol's Virtual Schema protocol has any per-query mechanism for the
adapter to hand a predicate back as unhandled, rather than assuming none exists.

### Decision

Close the question as a negative finding. The documented pushdown response has exactly two fields,
`type` and `sql`, with one documented note describing `sql`. `PushDownResponse.java` holds a single
member and `ResponseJsonConverter` serializes those two keys only. The word "residual" and any
partial-pushdown equivalent appear nowhere in the adapter API reference or the Exasol Virtual
Schema documentation. The only incomplete-pushdown concept in the protocol runs the other direction,
Exasol to adapter, as an empty `selectList`.

### Options Considered

| Option | Verdict |
|--------|---------|
| Check the protocol and record a negative finding | ✓ Chosen — an unverified negative is what produced this issue in the first place |
| Proceed without checking | ✗ Rejected — the interview asked for the check |

### Consequences

Self-application is the only available mechanism for a declined predicate, not merely the chosen
one — there is no protocol-level alternative to design around in a future plan.
