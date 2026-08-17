# Decisions: add-delta-file-pruning

## ADR: The kernel prunes; the adapter only translates

**ID:** delta-kernel-prunes-adapter-only-translates
**Plan:** add-delta-file-pruning
**Status:** Accepted

### Context

`DeltaFormatReader::resolve_scan` dropped the request filter, so every Delta query read every active
file. `ScanBuilder::with_predicate` drives both partition pruning and stats-based skipping through one
private `DataSkippingFilter` pass inside `delta_kernel`, and `scan_metadata()` already returns a
selection vector `append_active_files` can consume.

### Decision

Build a `delta_kernel::Predicate` and hand it to `ScanBuilder::with_predicate`, then consume the
selection vector `scan_metadata()` already returns. Compare no bound, parse no stats JSON, and
post-filter no resolved file list.

### Options Considered

| Option | Verdict |
|--------|---------|
| Build the kernel predicate, consume the kernel's selection vector | ✓ Chosen — `append_active_files` already honours `selected`, so pruning costs one builder call and no restructuring; mirrors the Iceberg reader's `plan_files` contract |
| Read `add.stats` directly and filter the constructed `Vec<FileEntry>` | ✗ Rejected — the comparison logic lives in a `pub(crate)` `DataSkippingFilter` that cannot be reached or faithfully reimplemented; a second copy of Delta's bound semantics would drift from the kernel's |

### Consequences

Pruning correctness rides entirely on the kernel's own contract rather than a second, hand-rolled
bound-comparison implementation. The reader gains a predicate parameter and loses nothing else.

## ADR: Deleting StatsOptions::none() is the whole configuration change

**ID:** delta-pruning-delete-statsoptions-none-not-replace
**Plan:** add-delta-file-pruning
**Status:** Accepted

### Context

The shipped reader built its scan with `.with_stats(StatsOptions::none())`, whose documented contract
disables all stats work — "no stats output, no internal data skipping (even when a predicate is set)".
`Scan::skip_stats()` is true for exactly that construction, and `log_replay.rs` disables both partition
pruning and data-column skipping together on it. Adding a predicate without removing this call would
ship a silent no-op that every existing test would still pass.

### Decision

Delete the `.with_stats(StatsOptions::none())` call rather than replace it with a named mode, letting
`ScanBuilder` keep its own default.

### Options Considered

| Option | Verdict |
|--------|---------|
| Delete the call, let `ScanBuilder::new`'s own default apply | ✓ Chosen — that default already is the mode wanted: internal skipping on, no `stats_parsed` column surfaced |
| Set `StatsOptions::default()` explicitly | ✗ Rejected — naming a default the kernel already owns pins a value that could drift from the kernel's own default |
| Set `StatsOptions::all_struct()` | ✗ Rejected — requests a parsed-stats projection the reader never reads, costing metadata bandwidth for nothing |

### Consequences

The single most dangerous line in the plan is a deletion, not an addition — reviewers and future
readers must recognize that omitting it silently defeats the whole feature.

## ADR: Trust Delta's writer-side string-bound invariant, and say so

**ID:** delta-pruning-trust-writer-string-bound-invariant
**Plan:** add-delta-file-pruning
**Status:** Accepted

### Context

Delta protocol's "Per-file Statistics" footnote — "String columns are cut off at a fixed prefix
length" — reads as though a truncated `maxValues` could fall below the true max, which would make range
pruning drop real rows. This had to be verified rather than assumed, because it was the one finding
capable of making the whole feature unsound.

### Decision

Translate comparisons over string data columns and rely on `maxValues` being a true upper bound,
recording the reliance as a deliberate protocol-trust trade-off in the spec.

### Options Considered

| Option | Verdict |
|--------|---------|
| Translate string comparisons, trust the writer-side bound invariant | ✓ Chosen — delta-spark's `truncateMaxStringAgg` appends a tie-breaker or omits the stat rather than relaxing the bound; `parquet`'s `increment_utf8` upholds the same invariant independently; the kernel's own asymmetric timestamp-only compensation (`adjust_scalar_for_max_stat_truncation`) confirms the string half needs none |
| Refuse to translate any comparison over a string data column | ✗ Rejected — forfeits real pruning, including `multi_part_stats`'s `value` column, to defend against a writer no shipped implementation matches |

### Consequences

Pruning trusts a writer-side protocol invariant no shipped implementation violates. A writer that
emitted a bare untagged prefix would defeat this undetectably — a protocol-trust assumption shared by
every Delta reader, named rather than hidden.

## ADR: A third independent filter-JSON walker, with the shared IR filed rather than built

**ID:** delta-predicate-third-walker-defer-shared-ir
**Plan:** add-delta-file-pruning
**Status:** Accepted

### Context

Three components already walk the same Exasol filter JSON toward three different outputs: the Iceberg
translator produces an `iceberg::spec::Predicate`, the DataFusion renderer produces a SQL string via
`render_df_filter_safe`, and this feature adds a `delta_kernel::Predicate` translator. Their literal
vocabularies and bound-soundness contracts differ throughout.

### Decision

Write `delta_predicate.rs` as a third independent walker over the same filter JSON, structurally
mirroring `to_iceberg_predicate`'s node dispatch. Do not extract a shared predicate IR.

### Options Considered

| Option | Verdict |
|--------|---------|
| A third independent walker, structurally mirroring the Iceberg translator | ✓ Chosen — each walker's literal vocabulary and bound-soundness contract differs (Iceberg's `upper_bounds` carries no truncation caveat, Delta's does), and `render_df_filter_safe` exposes no typed AST to reuse |
| Extract a shared format-neutral predicate IR consumed by all three | ✗ Rejected — would unify three output types and two literal vocabularies into a fourth vocabulary sized by their union, a large refactor of shipped, tested code justified by no requirement in this plan; filed as a follow-up |

### Consequences

Three vocabularies stay duplicated rather than unified. The duplication is named explicitly so a third
format, if one arrives, makes the case for a shared IR rather than re-litigating this decision.

## ADR: Never construct a false predicate or an empty junction

**ID:** delta-pruning-never-construct-false-predicate
**Plan:** add-delta-file-pruning
**Status:** Accepted

### Context

`delta_kernel::Predicate::or_from([])` normalizes an empty disjunction to literal `false`, which would
prune every file and return no rows — a wrong-results bug wearing the costume of an optimization. The
exposure is real: the kernel has no usable IN (`eval_pred_in` returns `None` with no override), so every
IN list desugars to an OR-chain, and an IN list whose elements all fail to convert is exactly the empty
case.

### Decision

Return `None` before any junction constructor sees an empty set, and assert in tests that no input
produces `Predicate::literal(false)`.

### Options Considered

| Option | Verdict |
|--------|---------|
| Explicit early return before every junction constructor, tested directly | ✓ Chosen — closes the exposure at its source and is verifiable independent of the translator's overall structure |
| Rely on the translator's structure making the empty case unreachable | ✗ Rejected — unreachability by construction is one forgotten early return away from a silent wrong-results bug |

### Consequences

The empty-junction rule is enforced by an explicit guard and a dedicated test rather than by incidental
structure, so a future edit to the translator cannot reintroduce the false-predicate hazard unnoticed.
