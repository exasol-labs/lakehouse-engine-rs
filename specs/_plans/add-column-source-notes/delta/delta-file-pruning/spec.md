# Feature: Delta Plan-Time File Pruning

Translates the soundly-translatable nodes of the Exasol WHERE predicate into a
`delta_kernel::expressions::Predicate` handed to the Delta scan builder, so log replay drops files on
partition values and per-file min/max statistics before any Parquet byte is read — while the full
predicate stays applied above the scan as the sole source of row-level correctness.

<!-- DELTA:CHANGED -->
## Background

* **This is issue #321, the last deferral `delta/delta-table-planning` recorded.** Issue #320
  delivered Delta pushdown parity — projection, filter, LIMIT, GROUP BY, ORDER BY + LIMIT, and
  broadcast-join pushdown all reach a Delta table, and file-level sharding works unchanged — but the
  reader pruned nothing. This feature closes that gap; it is a performance feature, not a correctness
  one. Three deferral statements in `delta/delta-table-planning` are superseded, recorded in that
  feature's own delta.
* **This feature is the Delta sibling of `file-planning/pushdown-file-pruning`, not a delta on it.** The
  two share an outcome and share NO mechanism: Iceberg prunes through `iceberg`'s manifest-level
  `plan_files`, Delta through `delta_kernel`'s private `DataSkippingFilter` columnar pass over the
  transaction log. The predicate types, the literal vocabularies, the stat-soundness contracts, and
  the normative specs cited differ throughout, so a shared spec would state every rule twice with
  different justifications.
* **The kernel prunes; this engine only translates.** `ScanBuilder::with_predicate` drives BOTH
  partition pruning and stats-based skipping through one pass — `delta_kernel` 0.26's own
  `scan/log_replay.rs` says so where it disables them: "When skip_stats is enabled, disable both data
  column skipping and partition pruning. Both rely on the same DataSkippingFilter columnar pass, so
  they are controlled together." The adapter constructs the predicate and consumes the selection
  vector `scan_metadata()` already returns; it reads no stats value, compares no bound, and
  post-filters no resolved file. This mirrors the Iceberg reader, which hands `plan_files` a predicate
  and consumes whatever it returns.
* **Enabling the predicate REQUIRES removing `StatsOptions::none()`, or the feature silently does
  nothing.** The shipped reader builds its scan `.with_stats(StatsOptions::none())`, whose doc reads
  "**Disables all stats work**: no stats output, no internal data skipping (even when a predicate is
  set)." `Scan::skip_stats()` is true for exactly that construction
  (`!synthesize_json && matches!(struct_stats, StructStats::None)`), and `ScanBuilder::new` already
  defaults to `StatsOptions::default()` — JSON stats, internal skipping ON, no `stats_parsed` column
  surfaced. Deleting the call is therefore the whole configuration change.
* **No statistic reaches this engine or its wire format.** `delta/delta-table-planning`'s rule
  that the returned scan carries no per-file minimum or maximum HOLDS, with a new justification:
  pruning completes inside the kernel before a file entry exists, so the stats wire shape that rule
  deferred never acquired a consumer. `ScanSpec`, `FileEntry`, and `LogicalField` gain no field, per
  CLAUDE.md § "ScanSpec is format-neutral".
* **Pruning is sound-not-complete, and correctness lives above the scan.** Every emitted node is
  logically implied by the user predicate; a node that cannot be translated soundly is dropped, so the
  scan opens MORE files rather than skipping one that could hold a matching row. The full predicate is
  still evaluated — in the scan's DataFusion filter when the DataFusion dialect renders it, and
  otherwise in the adapter's own outer `WHERE` per `pushdown/pushdown-declined-filter-self-apply`,
  whose recorded rule that pruning receives the raw filter tree with every conjunct, renderable or
  not, applies unchanged to this reader. The kernel agrees in its own contract:
  `with_predicate` documents filtering as "best-effort and can produce false positives (rows that
  should have been filtered out but were kept)" — false positives only.
* **The kernel fails open at three layers, so an over-broad predicate costs pruning and never rows.**
  A reference to a column outside the stats set returns `None` from the `get_*_stat` methods and
  junction-folds to a NULL literal, documented as "References to other data columns fold to NULL
  (keeping the file)"; a predicate ineligible for skipping makes `DataSkippingFilter::new` return
  `None`, "equivalent to a trivial filter that always returns TRUE (= keeps all files)"; and the
  selection evaluator's `DISTINCT(predicate, false)` keeps a file whenever the predicate is true OR
  null. This is why translating imperfectly is safe and why the translator never needs to prove a
  column carries statistics.
* **Delta protocol § "Per-file Statistics" is what makes range pruning sound**, and its truncation
  footnote is a writer obligation rather than a reader hazard. The protocol requires `maxValues` be "A
  value that is greater than or equal to all valid values present in this file for this column" and
  `minValues` be "A value that is less than or equal to all valid values", and states "These
  upper/lower bounds are sufficient information for data skipping". The footnote "String columns are
  cut off at a fixed prefix length. Timestamp columns are truncated down to milliseconds" annotates
  how a writer PRODUCES the value; it does not relax the bound. Verified on both halves rather than
  assumed:
  - **Strings**: the writer appends a tie-breaker after truncating, so the stored max stays an upper
    bound. delta-spark's `truncateMaxStringAgg` documents exactly that — "ensuring the any value in
    this column is less than or equal to the truncated max in UTF-8 encoding" — appends ASCII DEL
    (U+007F) or U+10FFFF, extends the prefix up to twice the prefix length when no tie-breaker
    is provably safe, and
    returns `null` (omitting the stat) when it cannot. `parquet`'s `increment_utf8` upholds the same
    invariant independently for the arrow-rs writer. `delta_kernel` documents the invariant it relies
    on and correctly applies no string-side compensation.
  - **Timestamps**: truncation FLOORS and is genuinely unsound uncompensated, and the kernel
    compensates — `adjust_scalar_for_max_stat_truncation` subtracts 999 µs from a `Timestamp` or
    `TimestampNtz` bound because "Truncation floors to the nearest millisecond, so:
    `stored_max <= actual_max <= stored_max + 999us`".
  A writer that emitted a bare prefix would defeat this, and no reader can detect it. That is a
  protocol-trust assumption shared by every Delta reader, named here as a deliberate trade-off rather
  than a gap: the alternative is to prune on no string bound at all, forfeiting real pruning against a
  writer no shipped implementation matches.
* **`stats` is OPTIONAL per file** — Delta protocol § "Add File and Remove File" marks it optional —
  so a file whose `add` action carries no statistics is always kept.
* **`delta_kernel` has no usable IN.** `BinaryPredicateOp::In` exists, but
  `KernelPredicateEvaluator::eval_pred_in` returns `None` with no override anywhere in the crate, so an
  `In` predicate prunes nothing. An IN list therefore desugars into an OR-chain of equalities. That
  makes the empty-junction normalization load-bearing: `Predicate::or_from([])` returns `false`, which
  would prune EVERY file, so an IN list that yields no translatable element must produce no constraint
  instead of an empty OR.
* **`delta_kernel::Predicate` has no `negate()`.** Negation is the free function
  `Predicate::not(pred)`, unlike `iceberg::spec::Predicate`, whose `.negate()` method the Iceberg
  translator uses.
* **A timestamp literal MUST be parsed, never passed as a string.** `Scalar::Timestamp(i64)` and
  `Scalar::TimestampNtz(i64)` are MICROSECONDS since the epoch and `Scalar::Date(i32)` is DAYS;
  `PrimitiveType::parse_scalar(&str)` is the protocol-conformant parser. A bare string literal builds
  `Scalar::String`, whose comparison against a timestamp bound yields no ordering and silently prunes
  nothing. `parse_scalar("")` returns `Scalar::Null`, which is not a constraint either.
* **`Expression::column` takes a path iterator, not a name.** A flat column is
  `Expression::column(["NAME"])`; a bare `&str` iterates as `char` and does not compile.
* **Delta's schema lookup is case-sensitive while Exasol delivers upper-cased names.**
  `StructType::field` matches exactly, so the translator resolves a request column case-insensitively
  to the exact Delta field name — the same service `Schema::field_by_name_case_insensitive` performs
  for the Iceberg translator.
* **Not every column carries min/max.** `delta.dataSkippingNumIndexedCols` defaults to 32 leaf fields,
  and min/max exist only for skipping-eligible types — `Byte`, `Short`, `Integer`, `Long`, `Float`,
  `Double`, `Date`, `Timestamp`, `TimestampNtz`, `String`, `Decimal`. `Boolean`, `Binary`, `Void`, and
  every nested type carry `nullCount` only. The translator does not model this: a predicate over a
  statless column reaches the kernel and folds to keep-all.
* **Column mapping needs no physical-name handling in the translator — the kernel resolves
  logical→physical itself.**
  Under `delta.columnMapping.mode` of `name` or `id`, a table's logical column name differs from the
  physical name that keys `add.stats`. `snapshot.schema()` returns the LOGICAL names, so the
  translator resolves against logical names and emits a logical `Expression::column`; the kernel maps
  it to the physical stat path before comparing bounds. Verified empirically rather than read off the
  source: against the vendored `cdf-column-mapping-name-mode` fixture, whose logical `id` is stored as
  `col-80396d42-d765-483e-b86e-7ac1e13ef88c`, a predicate `id = 3` cut the 3 active files to the one
  file whose physical `minValues`/`maxValues` for that column is 3; against
  `cdf-column-mapping-id-mode`, whose logical `id` is stored as
  `col-b727ccd4-2c6f-43c0-b49e-2dfecc1f4e8b`, `id = 1` cut 3 active files to the one file bounded at
  1, and `id = 99` pruned to an empty list. Changing the literal moved the surviving file
  accordingly, so the selection is bound-driven and not incidental. Column mapping is therefore NOT a
  keep-all degradation and there is no pruning-completeness gap to track.
* **A request column resolves through its declared binding key (#426).** The table's column notes
  record each column's binding key at `createVirtualSchema` (`vs-adapter/column-source-notes`). The
  translator finds the request column's note by the uppercase fold, then the current snapshot field
  carrying the note's physical name (`name` mode) or field id (`id` mode), and emits that field's
  CURRENT logical name, which the kernel maps to the physical stat path. Under the `none` mode the
  declared name is the field's identity and resolves case-insensitively as before. Resolving by the
  declared name alone would read another column's statistics after a source-side rename between
  refreshes, and PROTOCOL.md § Reader Requirements for Column Mapping resolves "column level
  statistics" by physical name. A column whose key the current snapshot no longer carries cannot be
  translated, so its node imposes no constraint.
* **Exasol pre-normalises `>`→`<` and `>=`→`<=`**, recorded once in
  `file-planning/pushdown-file-pruning` and not restated here. The translator still handles the greater
  forms and still flips an operator whose column sits on the right, because the normalization governs
  which node kind arrives, not which side the column lands on.
* **The translator is a third independent walker over the same filter JSON, and that is deliberate.**
  No format-neutral predicate IR exists: `vs-expression`'s `render_df_filter_safe` renders a
  DataFusion SQL fragment and exposes no typed AST, and `adapter::iceberg_predicate` produces an
  `iceberg::spec::Predicate`. Extracting a shared IR would have to unify three output types, two
  literal vocabularies, and two stat-soundness contracts, none of which this feature needs — see the
  plan's decision log.
* **The translator stays private to `adapter::pushdown::format`**, so the frozen pushdown façade
  admits NO item and needs no delta against `pushdown/pushdown-module-structure`. This matches that
  feature's recorded rule for the Delta reader-protocol gate and the Delta type classifier, both
  reached only from inside `format`.
* **Apache Iceberg spec check — checked, and no Iceberg behavior changes.** No code on the Iceberg
  resolution path is touched: `adapter::iceberg_predicate`, `plan_files_from_table`, and the Iceberg
  reader are unedited, so the table spec's requirement that a scan filter files by "column bounds and
  counts that are stored by field id in manifests" is still satisfied exactly as
  `file-planning/pushdown-file-pruning` records it. The spec's ordered Column Projection resolution rule
  (1) — the partition-metadata rule — remains the deliberate, accurately-scoped trade-off
  `scan-read-path/scan-execution-field-id-projection` records, neither closed nor widened here. The
  one substantive difference this feature must not paper over is a bound-soundness asymmetry: Iceberg
  requires `upper_bounds` "must be greater than or equal to all non-null, non-Nan values in the column
  for the file" with NO truncation caveat, while Delta's identical requirement carries the prefix-cut
  footnote handled above.
* Every error this feature surfaces is a `UdfError`, never a panic, because a panic inside a UDF is an
  abnormal VM exit that makes the engine SIGKILL every sibling VM of the statement part. No error text
  carries a bearer token, an OAuth client secret, a vended storage key, or any other credential value.
<!-- /DELTA:CHANGED -->

## Scenarios

<!-- DELTA:NEW -->
### Scenario: A filter column resolves through its declared binding key

* *GIVEN* a local copy of a `name`-mode column-mapped Delta table with columns `a` and `b`, whose data files hold disjoint value ranges of each such that the file holding `a = 7` holds no `b = 7`, the column notes `createVirtualSchema` recorded for it, and a later commit appended to the copy's log that swaps the two display names without rewriting a data file
* *WHEN* the Delta format reader resolves the scan for a filter `A = 7` against those notes
* *THEN* the reader SHALL translate the filter on `A` to the field that carries `a`'s recorded physical name, now displayed as `b`, so pruning keeps the file whose statistics for that physical column admit 7
* *AND* a filter on a declared column whose physical name the current snapshot no longer carries SHALL prune no file for that conjunct
<!-- /DELTA:NEW -->
