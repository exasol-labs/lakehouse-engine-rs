# Decisions: fix-having-unmatched-aggregate-fallback

## ADR: An unrenderable HAVING is a routing outcome, not an error

**ID:** having-unrenderable-is-routing-not-error
**Plan:** fix-having-unmatched-aggregate-fallback
**Status:** Accepted

### Context

A grouped query whose HAVING references an aggregate absent from the select list failed at
`EXPLAIN VIRTUAL` time with SQL state `22002` / `F-UDF-CL-RUST-9001` (issue #195). The premise
behind that error — Exasol will not re-apply a HAVING the adapter advertised `AGGREGATE_HAVING`
for, so it must never be silently dropped — is established, not assumed: the adapter has exactly
two HAVING renderers and neither omits a HAVING, and the adapter's own code asserts the rule in
six places (`request_shape.rs` 16/70/85, `grouped_agg.rs` 3394, `file_resolution.rs` 1480,
`mod.rs` 363). Exasol's re-apply behavior is also shape-dependent: under `add-topn-pushdown`
B5/B6 (issues #225 / #189) an `orderBy` pushed together with a `limit` was fully delegated and no
backstop ran, returning wrong unsorted unbounded rows. But the premise only rules out routes that
DROP the HAVING; it does not require an error, and a route that preserves the HAVING already
exists.

### Decision

Route a grouped request whose HAVING cannot be rewritten over the partial/merge decomposition to
the existing qualified-single-table-wrapper fallback (`RequestShape::GroupByWrapper`) instead of
raising an error. The wrapper renders the HAVING as ordinary Exasol SQL over materialized rows, so
the HAVING is preserved and the advertised `AGGREGATE_HAVING` contract holds.

### Options Considered

| Option | Verdict |
|--------|---------|
| Route to the qualified single-table wrapper | ✓ Chosen — a correct native path already exists; the hard error was a false negative |
| Keep the hard error and document the unsupported shape | ✗ Rejected — the error is a false negative given a working native path |
| Stop advertising `AGGREGATE_HAVING` | ✗ Rejected — de-optimizes every HAVING query to fix one shape |
| Teach `render_having_over_merge` to synthesize a partial for the unprojected aggregate | ✗ Rejected — widens the per-shard EMITS clause and the merge decomposition for a shape the wrapper already serves correctly |

### Consequences

A HAVING referencing an unselected aggregate, a mixed AND/OR junction with one unmatched operand,
or a `DISTINCT` aggregate in a HAVING now succeeds via the wrapper instead of hard-erroring.
A non-numeric aggregate carrying a HAVING also now routes to the wrapper: Exasol's own engine
either implicitly converts (query succeeds) or raises its standard `22018` cast error naming the
offending value, rather than the adapter erroring pre-emptively.

## ADR: Renderability of the HAVING is a routing predicate, so it belongs in the classifier

**ID:** having-renderability-belongs-in-classifier
**Plan:** fix-having-unmatched-aggregate-fallback
**Status:** Accepted

### Context

Whether a HAVING can be rewritten over the `PARTIAL_*` merge columns decides WHICH shape is
reachable (partial/merge grouped, or the qualified single-table wrapper), not merely what SQL
that shape emits. The merge-render previously lived in `build_dispatch_sql`'s `Grouped` arm, a
path that had already committed to the `Grouped` shape — so a render failure discovered there had
no exit but an error. `file_resolution.rs::empty_result_sql` separately re-invokes the same
classification logic, so a decision made only in the dispatcher would leave the empty-result path
disagreeing about which shape a given request produces.

### Decision

Move the HAVING merge-render from `build_dispatch_sql` into `classify_request_shape`, and carry
the rendered fragment (or its absence) as part of the returned `RequestShape`. The dispatcher
splices the classifier's decision without re-rendering.

### Options Considered

| Option | Verdict |
|--------|---------|
| Move the render into the classifier | ✓ Chosen — the shape decision and the rendering decision are the same decision; a single owner keeps the dispatch and empty-result paths in agreement by construction |
| Leave the render in `mod.rs` and re-classify to `GroupByWrapper` on failure from inside the `Grouped` arm | ✗ Rejected — reintroduces a second routing tree, exactly what `vs-adapter/pushdown-module-structure` consolidated away (issue #175), and leaves the empty-result path still classifying the request as `Grouped` |

### Consequences

`RequestShape::Grouped.having` changes from `Option<&'a Json>` (the raw node) to `Option<String>`
(the pre-rendered merge SQL), which drops the `'a` lifetime from both `RequestShape` and
`classify_request_shape` since `having` was the enum's only borrow. The fully-pruned zero-row
path agrees with the non-empty path for free, because `empty_result_sql` re-invokes the same
classifier.

## ADR: Remove the non-numeric-aggregate-with-HAVING hard error in the same plan

**ID:** remove-non-numeric-having-hard-error
**Plan:** fix-having-unmatched-aggregate-fallback
**Status:** Accepted

### Context

`classify_request_shape` raised a second hard error when a select-list aggregate's column type
failed the numeric gate AND a HAVING was present, on the same disproven premise as the unmatched-
aggregate case: that a HAVING the adapter advertised `AGGREGATE_HAVING` for must not be silently
dropped. `GroupByWrapper` does not drop it — it renders the HAVING natively. Live capture against
the docker stack confirmed the wrapper's native `SUM(<VARCHAR>)` is not inherently invalid: a
numeric-looking VARCHAR succeeds (Exasol implicitly converts), and a genuinely non-numeric value
returns Exasol's own `22018` "invalid character value for cast" naming the offending value.

### Decision

Delete `classify_request_shape`'s non-numeric-with-HAVING `Err` block in this plan. Both grouped
declines — numeric-gate failure and unrenderable HAVING — share the one fall-through to the
`GroupByWrapper` tier, leaving the grouped tier total (returns no `Err` for any input).

### Options Considered

| Option | Verdict |
|--------|---------|
| Remove the hard error in this plan | ✓ Chosen — it is the same bug on the same disproven premise, 15 lines away in the same function; unifying it is a net deletion, and live capture shows the wrapper is strictly better on both branches |
| Defer to a follow-up issue | ✗ Rejected — would leave the function raising an error it has no reason to raise, immediately after this plan established why |

### Consequences

The grouped tier of `classify_request_shape` returns no `Err` at all after this plan; its return
type simplifies from `Result<RequestShape, UdfError>` to a bare `RequestShape`. A numeric-looking
VARCHAR aggregate with a HAVING now succeeds where it previously hard-errored; a genuinely
non-numeric one surfaces Exasol's own `22018` cast error instead of an adapter-level decline.

## ADR: The recorded HAVING-backstop clause was wrong and is corrected, not merely superseded

**ID:** correct-recorded-having-backstop-claim
**Plan:** fix-having-unmatched-aggregate-fallback
**Status:** Accepted

### Context

`specs/vs-adapter/pushdown-planning-capability-extensions/spec.md` recorded that an untranslatable
HAVING "SHALL be omitted from the wrapper SQL and retained by Exasol as a correctness backstop" —
the opposite of this plan's load-bearing premise. If that recorded rule were true, the fix would
be `having: None` on decline and the classifier/enum change would be unnecessary complexity.
Investigation against the implementation showed the recorded rule is wrong on both halves: no code
path omits a HAVING (the two renderers render or raise, never omit), and Exasol's re-apply
behavior is shape-dependent, not a reliable backstop, as demonstrated live under
`add-topn-pushdown` B5/B6 (issues #225 / #189) where the analogous ORDER-BY-backstop assumption
returned wrong, unsorted, unbounded rows.

### Decision

Correct the recorded spec via a `DELTA:CHANGED` on the affected scenario rather than leaving it as
an unaddressed contradiction. Narrow the correction to the HAVING claim only: the ORDER BY
reliance sentence in the same clause is kept verbatim, since `vs-adapter/pushdown-planning-topn`
records the same ORDER BY reliance for the same trigger set and is out of this plan's scope.

### Options Considered

| Option | Verdict |
|--------|---------|
| Correct the recorded clause with a scoped `DELTA:CHANGED` | ✓ Chosen — leaving the contradiction unaddressed would reintroduce, in the permanent library, the exact defect class (an incorrect backstop assumption) this plan exists to fix |
| Leave the recorded clause unchanged and treat the plan's premise as a new, separate fact | ✗ Rejected — would leave two contradictory normative statements about the same HAVING behavior in the merged library |

### Consequences

The capability-extensions spec no longer claims an Exasol HAVING/LIMIT backstop that does not
exist. `pushdown-planning-topn/spec.md` is deliberately left untouched, since its ORDER BY
reliance claim is unrelated to the corrected HAVING/LIMIT claim and remains accurate.
