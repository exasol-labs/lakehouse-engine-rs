# Decisions: refactor-pushdown-agg-dedup

## ADR: The single source of truth is a per-column descriptor, not a column count

**ID:** one-descriptor-owns-the-partial-column-set-not-a-count
**Plan:** refactor-pushdown-agg-dedup
**Status:** Accepted

### Context

The partial-aggregate column contract — how many columns each `AggKind` contributes, in what
order, under what name, and what an empty shard puts in each — was independently encoded at five
sites across two modules: `partial_select_items`, `emit_null_partial_row`, and
`partial_row_from_batch` in `scan/partial_agg.rs`, and `partial_emits_items` and
`merge_select_items` in `adapter/pushdown/grouped_agg.rs`. Nothing enforced agreement, and a
disagreement is silent: `partial_row_from_batch` advances a column index it maintains itself, and
`emit_null_partial_row` builds a `Vec<Value>` whose only contract is its length.

### Decision

Add `AggKind::partial_columns(&self) -> &'static [PartialAggColumn]` in
`crates/lakehouse-engine/src/scan/spec.rs`, with `PartialAggColumn` enumerating the ten distinct
partial columns the contract admits, and drive all five contract sites from it.

### Options Considered

| Option | Verdict |
|--------|---------|
| Per-`AggKind` descriptor returning `&'static [PartialAggColumn]` | ✓ Chosen — the only shape that drives all five sites: the name, the `EMITS` type, the DataFusion expression, and the 0-vs-NULL fallback each need per-column identity, not just an arity |
| A bare `partial_column_count() -> usize`, per the issue's literal suggestion | ✗ Rejected — a count alone drives only `partial_row_from_batch`; the other four sites would still match on `AggKind` independently |
| A `&'static [(&'static str, bool)]` of (name suffix, is-counter) pairs | ✗ Rejected — a tuple of primitives cannot be matched exhaustively, so the renderers would fall back to string comparison and a new column would be a silent default instead of a compile error |

### Consequences

Extending the contract is adding a case to an exhaustive match, which is a compile error at every
renderer, rather than editing a wildcard that silently defaults.

## ADR: The contract is quintuplicated, not triplicated, and the fifth site is in the other module

**ID:** partial-agg-contract-spans-five-sites-across-two-modules
**Plan:** refactor-pushdown-agg-dedup
**Status:** Accepted

### Context

Issue #179 named three scan-side functions in `scan/partial_agg.rs` as the column-count contract's
encoding sites. The issue's own failure statement — "partial rows misalign with the UDF `EMITS`
contract" — describes `partial_emits_items` in `adapter/pushdown/grouped_agg.rs`, which the issue's
site list omitted.

### Decision

Treat `partial_emits_items` and `merge_select_items` in `adapter/pushdown/grouped_agg.rs` as
in-scope contract sites alongside the three the issue names, bringing the total to five sites
across two modules.

### Options Considered

| Option | Verdict |
|--------|---------|
| Five sites across `scan/partial_agg.rs` and `adapter/pushdown/grouped_agg.rs` | ✓ Chosen — `partial_emits_items` IS the `EMITS` contract the issue's failure statement names, and `merge_select_items` hand-formats the same `PARTIAL_*` names the other four produce |
| Scope to the three scan-side functions the issue lists | ✗ Rejected — leaves the adapter's hand-written arity match as the exact cross-module disagreement the plan exists to remove, now split across a boundary no compiler or unit test spans |

### Consequences

Both modules already import `scan::spec`, so the shared descriptor adds no dependency edge and no
cycle — the wider scope costs no architectural change.

## ADR: Capture the missing golden baselines before touching production code

**ID:** capture-golden-baselines-before-any-production-edit
**Plan:** refactor-pushdown-agg-dedup
**Status:** Accepted

### Context

`dispatch_golden.rs` asserts full-string equality against committed fixtures, but none of its ten
existing fixtures covers an `AVG` or a statistical aggregate — `grep PARTIAL_avg` and
`grep PARTIAL_stat` over the fixture directory both return nothing. The six existing statistical
merge tests are all `.contains(...)` probes, so a fragment extraction that dropped a parenthesis or
swapped a denominator would leave every one of them green while changing every returned variance.

### Decision

Task 1.1 adds four pre-refactor golden fixtures — two `dispatch_golden` fixtures (single-group and
grouped, covering every `AggKind` and arity) and two scan-seam fixtures (via
`build_partial_agg_sql` and `build_grouped_partial_agg_sql`) — committed before any contract site
is edited.

### Options Considered

| Option | Verdict |
|--------|---------|
| Capture and commit new fixtures before any production edit | ✓ Chosen — the two multi-column arities (`AVG`, statistical) are exactly the ones the existing gate does not watch, and are exactly what the refactor is most likely to break |
| Rely on the ten existing fixtures and the six statistical `.contains` merge tests | ✗ Rejected — measured to cover neither arity; a broken extraction would pass every one of them |
| Add the fixtures after the refactor, as regression cover for future changes | ✗ Rejected — a fixture captured after the refactor pins whatever the refactor produced, which is the claim under test, not evidence for it |

### Consequences

The byte-identity claim is falsifiable against a pre-refactor baseline rather than self-certified
against the refactor's own output.

## ADR: Home the descriptor in scan/spec.rs, and keep that module serde-only

**ID:** partial-column-descriptor-lives-in-scan-spec-serde-only
**Plan:** refactor-pushdown-agg-dedup
**Status:** Accepted

### Context

Both the scan (`scan/partial_agg.rs`) and the adapter (`adapter/pushdown/grouped_agg.rs`) already
import `scan::spec`, the scan-spec wire-format module, which itself imports only `serde`. The
descriptor's empty-shard identity could be expressed as an SDK `Value` or as a boolean.

### Decision

Declare `PartialAggColumn`, `AggKind::partial_columns`, `is_counter`, and the shared
`partial_column_name` in `crates/lakehouse-engine/src/scan/spec.rs`, and express the empty-shard
identity as a boolean (`is_counter() -> bool`) rather than an SDK `Value`.

### Options Considered

| Option | Verdict |
|--------|---------|
| `scan/spec.rs`, boolean empty-shard identity | ✓ Chosen — both consumers already depend on this module, which depends on neither, so the descriptor adds no edge and no cycle; a boolean keeps the module serde-only |
| A new module owning the contract | ✗ Rejected — adds a module for one enum and two methods |
| `adapter/pushdown/support.rs` | ✗ Rejected — unreachable from the scan; would invert the dependency, making the wire format depend on the adapter |
| Return `Value::Int64(0)` / `Value::Null` from the descriptor directly | ✗ Rejected — drags `exasol_udf_sdk` into a module whose only current import is `serde`, for a two-value mapping the emit site can do itself |

### Consequences

`scan/spec.rs` gains no new dependency edge; the emit site alone maps `is_counter()` to the SDK
`Value` it needs.

## ADR: The shared CAST helper takes Option<&str>, lives in support, and excludes two arms on purpose

**ID:** shared-cast-helper-optional-declared-type-in-support-excludes-unconditional-arms
**Plan:** refactor-pushdown-agg-dedup
**Status:** Accepted

### Context

The declared-type CAST rule — wrap in `CAST(<expr> AS <ty>)` unless `<ty>` is the
`VARCHAR(2000000)` default — was written six times across `grouped_agg.rs` and
`file_resolution.rs`, one of which was a doc comment claiming to be the shared rule. Five of the
six sites read a possibly-absent declared type and already treat absence as no-cast; the sixth
took a non-optional `&str`. Two further sites, the `GroupKey` and `Aggregate` arms of
`empty_grouped_sql`, cast unconditionally — even to `VARCHAR(2000000)` — because a bare `NULL` in a
zero-row `SELECT … FROM DUAL WHERE 1=0` carries no `VARCHAR` type and would otherwise fail
Exasol's positional `selectListDataTypes` check.

### Decision

Move the canonical implementation to `adapter/pushdown/support.rs` as
`pub(super) fn cast_to_declared_type(expr: &str, declared: Option<&str>) -> String`, delegate the
six variable-type sites to it, and leave the two unconditional `empty_grouped_sql` arms untouched.

### Options Considered

| Option | Verdict |
|--------|---------|
| `Option<&str>` signature, in `support.rs`, two arms excluded | ✓ Chosen — matches what five of the six sites already do, sits reachable from both sibling submodules that need it, and preserves the two arms' correctness-driven unconditional cast |
| Keep the existing `&str` signature and add a separate `Option`-taking wrapper | ✗ Rejected — leaves two names for one rule |
| Keep the helper private in `grouped_agg.rs` and duplicate it for `file_resolution.rs` | ✗ Rejected — reintroduces the duplication one module over |
| Fold all eight cast sites in, including the two unconditional arms | ✗ Rejected — a correctness regression: applying the `VARCHAR(2000000)` guard there would emit a bare untyped `NULL` and fail Exasol's positional type validation |

### Consequences

Every stale doc-comment sentence asserting the rule "by convention" — `cast_to_declared_type`'s
three-site mirror list and `constant_projection_sql`'s "mirrors the group-key and aggregate cast
discipline" clause — is deleted, since the helper now enforces what those notes only claimed.

## ADR: Keep every existing test, including the six .contains merge probes

**ID:** keep-existing-contains-probes-alongside-golden-fixtures
**Plan:** refactor-pushdown-agg-dedup
**Status:** Accepted

### Context

The six statistical merge `.contains(...)` tests and the golden fixtures answer different
questions: a golden diff reports that some byte moved, while each probe names why one specific
guard exists (the `N <= 1` divisor, the `IS NULL` passthrough). Once golden fixtures assert
full-string equality, the probes could be read as redundant.

### Decision

Delete no test. Keep the six statistical merge probes, `stat_aggregate_emits_three_partial_columns`,
and `parse_agg_item_recognises_stat_functions`' own literal stat-family table, none of which is
rewritten to read the production descriptor or constant it verifies.

### Options Considered

| Option | Verdict |
|--------|---------|
| Keep every existing test unchanged | ✓ Chosen — a failing probe localizes the defect to a specific guard, which a golden diff alone cannot; keeping literals makes the byte-identity claim falsifiable rather than self-certified |
| Delete the six probes as redundant once the golden fixtures assert full-string equality | ✗ Rejected — a golden diff alone reports only that some byte moved, not which guard broke |
| Rewrite the test-side tables and arity assertions to read the production descriptor | ✗ Rejected — a test that derives its expectation from the code under test asserts the descriptor against itself and would pass any self-consistent renaming or mis-mapping |

### Consequences

No test is deleted or weakened by this plan; the same standard `vs-adapter/pushdown-col-types-consolidation`
applies to its own fold-pinning tests is applied here.
