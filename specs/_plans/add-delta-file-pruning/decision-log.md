# Decision Log: add-delta-file-pruning

## Interview

**Q:** Where should this live in the spec library?
**A:** "New feature: delta-file-pruning" — the recommended option. Rationale offered: it mirrors the
existing precedent that `vs-adapter/delta-table-planning` is already a separate feature from Iceberg's
file-resolution planning even though the externally-visible behavior is similar, because the
implementation mechanism (delta_kernel's internal `DataSkippingFilter`) is entirely unrelated to
iceberg-rust's manifest-level pruning. Delta-specific scenarios — delta_kernel predicate construction,
the `StatsOptions` choice, checkpoint interaction — stay out of the Iceberg-authored
`vs-adapter/pushdown-file-pruning` spec. Decision: author a NEW feature spec
`vs-adapter/delta-file-pruning`; do not add scenarios into `vs-adapter/pushdown-file-pruning`.

**Q:** How much predicate-operator coverage should this ship with?
**A:** "Full parity with `iceberg_predicate.rs`" — the recommended option. Decision: the Delta
translator covers equality, the four range comparisons, IN, BETWEEN, AND/OR/NOT, and every literal type
`iceberg_predicate.rs` handles (integer, string, float, timestamp, timestamptz), targeting
`delta_kernel::expressions::Predicate` instead of `iceberg::spec::Predicate`. No deferred or narrower
first version — this is the full scope of this plan, not a follow-up.

## Design Decisions

### [1] The kernel prunes; the adapter only translates

- **Decision:** Build a `delta_kernel::Predicate` and hand it to `ScanBuilder::with_predicate`, then
  consume the selection vector `scan_metadata()` already returns. Compare no bound, parse no stats JSON,
  and post-filter no resolved file list.
- **Alternatives:** Read `add.stats` ourselves and filter the constructed `Vec<FileEntry>`. Rejected:
  the comparison logic lives in a `pub(crate)` `DataSkippingFilter` we can neither reach nor faithfully
  reimplement, and a second copy of Delta's bound semantics — wide vs tight bounds, null-count special
  cases, timestamp-truncation compensation — would drift from the kernel's.
- **Rationale:** `append_active_files` already honours `selected`, so the pruning path costs one
  builder call and no restructuring. It also mirrors the Iceberg reader, which hands `plan_files` a
  predicate and consumes whatever it returns.
- **Promotes to ADR:** yes

### [2] Deleting `StatsOptions::none()` is the whole configuration change

- **Decision:** Delete the `.with_stats(StatsOptions::none())` call rather than replace it with a named
  mode, letting `ScanBuilder` keep its own default.
- **Alternatives:** `StatsOptions::default()` explicitly, or `all_struct()` to consume `stats_parsed`
  directly. Rejected: naming `default()` pins a default the kernel owns, and `all_struct()` would
  request a parsed-stats projection the reader never reads.
- **Rationale:** `Scan::skip_stats()` is `!synthesize_json && matches!(struct_stats, None)` — true for
  exactly `none()`. `ScanBuilder::new` already sets `StatsOptions::default()`, whose mode is precisely
  what is wanted: internal skipping on, no `stats_parsed` column surfaced. This is the plan's most
  dangerous line: adding a predicate WITHOUT deleting this call ships a silent no-op that every existing
  test still passes, because `log_replay.rs` disables "both data column skipping and partition pruning"
  together on it.
- **Promotes to ADR:** yes

### [3] Trust Delta's writer-side string-bound invariant, and say so

- **Decision:** Translate comparisons over string data columns and rely on `maxValues` being a true
  upper bound, recording the reliance as a deliberate protocol-trust trade-off in the spec.
- **Alternatives:** Refuse to translate any comparison over a string data column, pruning only on
  partition values and numeric bounds. Rejected: it forfeits real pruning — including
  `multi_part_stats`'s `value` column — to defend against a writer no shipped implementation matches.
- **Rationale:** The protocol's footnote ("String columns are cut off at a fixed prefix length") reads
  as though a truncated max could fall below the true max, which would make pruning drop real rows. It
  does not relax the normative "greater than or equal to all valid values": delta-spark's
  `truncateMaxStringAgg` appends a max-codepoint tie-breaker, extends the prefix up to `2 × prefixLen`
  when no tie-breaker is provably safe, and omits the stat when it cannot; `parquet`'s `increment_utf8`
  upholds the same invariant independently. The decisive evidence is an asymmetry inside the kernel: it
  compensates for the TIMESTAMP half of that same footnote (`adjust_scalar_for_max_stat_truncation`,
  −999 µs, because truncation there FLOORS) and deliberately does not for the string half. Verified
  against sources, not recalled — this was the one finding capable of making the whole feature unsound.
- **Promotes to ADR:** yes

### [4] A third independent filter-JSON walker, with the shared IR filed rather than built

- **Decision:** Write `delta_predicate.rs` as a third independent walker over the same filter JSON,
  structurally mirroring `to_iceberg_predicate`'s node dispatch. Do not extract a shared predicate IR.
- **Alternatives:** Extract a format-neutral typed predicate IR consumed by the Iceberg translator, the
  Delta translator, and `render_df_filter_safe`. Deferred to a follow-up issue, not smuggled in here.
- **Rationale:** The three consumers produce an `iceberg::spec::Predicate`, a `delta_kernel::Predicate`,
  and a SQL string; their literal vocabularies differ (Iceberg `Datum` vs kernel `Scalar` vs rendered
  text), and their bound-soundness contracts differ (Iceberg's `upper_bounds` carries no truncation
  caveat, Delta's does). `render_df_filter_safe` exposes no typed AST to reuse. A unifying IR sized by
  the union of all three would be a large refactor of shipped, tested code, justified by no requirement
  in this plan — and per `/speq:design-philosophy`, an interface should serve today's needs without
  forcing special cases, not tomorrow's speculated ones. The duplication is real and is named here so a
  third format makes the case rather than re-litigating it.
- **Promotes to ADR:** yes

### [5] Never construct a false predicate or an empty junction

- **Decision:** Return `None` before any junction constructor sees an empty set, and assert in tests
  that no input produces `Predicate::literal(false)`.
- **Alternatives:** Rely on the translator's structure making it unreachable. Rejected: it is one
  forgotten early return away.
- **Rationale:** `Predicate::or_from([])` normalizes an empty disjunction to literal `false`, which
  prunes every file and returns no rows — a wrong-results bug wearing the costume of an optimization.
  The exposure is real because the kernel has no usable IN (`eval_pred_in` returns `None` with no
  override), so every IN list becomes an OR-chain, and an IN whose elements all fail to convert is
  exactly the empty case.
- **Promotes to ADR:** yes

### [6] The translator lives in `format/`, not beside the Iceberg translator

- **Decision:** `crates/lakehouse-engine/src/adapter/pushdown/format/delta_predicate.rs`, declared
  privately in `format/mod.rs`, with a sibling `delta_predicate_tests.rs`.
- **Alternatives:** `adapter/delta_predicate.rs`, mirroring `adapter/iceberg_predicate.rs`'s `pub`
  placement at the adapter root.
- **Rationale:** The Iceberg translator's placement predates the format-reader seam. `delta_protocol`
  and `delta_schema` set the current precedent, and `vs-adapter/pushdown-module-structure` records that
  both "MUST NOT be added to the façade, because both are reached only from inside
  `adapter::pushdown::format`". The frozen façade therefore admits no item and needs no delta.
- **Promotes to ADR:** no

### [7] Build the predicate in `read_delta_log`

- **Decision:** `read_delta_log` gains `filter_json`, builds the predicate after
  `DeltaSnapshot::open`, and passes `Option<PredicateRef>` to `active_files`.
- **Alternatives:** Build it in `resolve_scan` (impossible — the schema exists only after the snapshot
  opens), or pass `filter_json` into `active_files` (rejected — the kernel-replay module would gain a
  second vocabulary to know).
- **Rationale:** `read_delta_log` already composes "open, build schema, replay", so the predicate's one
  dependency is already in scope there. It has exactly one production caller, so the threading is a
  one-line change per hop.
- **Promotes to ADR:** no

### [8] Gate the plan on observing a real prune before writing the translator

- **Decision:** Task 1.1 halts the plan unless a hand-built kernel predicate demonstrably prunes
  `multi-part-stats` to 2 files and `basic_partitioned` to 2 files, and task 1.2 pins that
  `StatsOptions::none()` is what suppresses it.
- **Alternatives:** Assume the fixtures prune and discover otherwise in the E2E suite.
- **Rationale:** `multi-part-stats` is `writeStatsAsStruct=true` + `writeStatsAsJson=false`, exactly the
  shape of delta-kernel-rs issue #2541 ("ScanFile.stats is null for struct-stats-only checkpoints"). The
  failure mode is silent: stats degrade to keep-everything, which passes a rows-correct assertion while
  proving nothing about pruning. A falsifiable gate at the front is cheaper than a false green at the
  end, and matches this project's rule that a claimed capability be verified rather than assumed.
- **Promotes to ADR:** no

### [9] The E2E split: file counts to the behavior feature, rows to the harness feature

- **Decision:** Assertions about WHICH files survive go in `vs-adapter/delta-file-pruning`; assertions
  about which ROWS return go in `e2e-harness/unity-catalog-e2e-harness-delta-queries`.
- **Alternatives:** Put all end-to-end pruning coverage in the new feature, touching no harness spec.
- **Rationale:** The harness feature's recorded charter is "every scenario that asserts the ROWS a query
  returns over a seeded Delta table". A rows-unchanged-under-pruning scenario is squarely that, and
  placing it elsewhere would split one charter across two features. The file-count half is a planning
  outcome and belongs with the mechanism.
- **Promotes to ADR:** no

### [10] Delta pruning statements supersede three recorded claims, not one

- **Decision:** The delta on `vs-adapter/delta-table-planning` supersedes the "reader SHALL still apply
  NO filter-based file pruning" clause AND corrects the REASON behind the "MUST NOT carry any per-file
  minimum or maximum statistic" clause, keeping that clause's obligation intact.
- **Alternatives:** Supersede only the pruning clause and leave the stats-wire clause untouched.
  Rejected: its stated reason ("stats-based file pruning is issue #321 and its wire shape is designed
  with its consumer") becomes false once #321 ships without a wire shape.
- **Rationale:** The obligation still holds and must keep holding — `ScanSpec` stays format-neutral per
  CLAUDE.md. But a spec clause whose justification has silently expired is the kind of quiet rot this
  project's supersession convention exists to prevent. Naming the reason change is what keeps "no
  statistic on the wire" a decision rather than an accident.
- **Promotes to ADR:** no

## Review Findings

<!-- Populated by speq-plan after plan-reviewer resolves a blocker, and by speq-implement after code review. -->
