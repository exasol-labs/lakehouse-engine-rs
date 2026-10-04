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

Treat `partial_emits_items` and `merge_select_items` in `adapter/pushdown/grouped_agg.rs` as in-scope contract sites alongside the three the issue names, bringing the total to five sites across two modules.

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

