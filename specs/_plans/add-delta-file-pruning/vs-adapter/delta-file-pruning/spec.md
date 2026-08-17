# Feature: Delta Plan-Time File Pruning

Translates the soundly-translatable nodes of the Exasol WHERE predicate into a
`delta_kernel::expressions::Predicate` handed to the Delta scan builder, so log replay drops files on
partition values and per-file min/max statistics before any Parquet byte is read — while the full
predicate stays applied above the scan as the sole source of row-level correctness.

## Background

* **This is issue #321, the last deferral `vs-adapter/delta-table-planning` recorded.** Issue #320
  delivered Delta pushdown parity — projection, filter, LIMIT, GROUP BY, ORDER BY + LIMIT, and
  broadcast-join pushdown all reach a Delta table, and file-level sharding works unchanged — but the
  reader pruned nothing. This feature closes that gap; it is a performance feature, not a correctness
  one. Three deferral statements in `vs-adapter/delta-table-planning` are superseded, recorded in that
  feature's own delta.
* **This feature is the Delta sibling of `vs-adapter/pushdown-file-pruning`, not a delta on it.** The
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
* **No statistic reaches this engine or its wire format.** `vs-adapter/delta-table-planning`'s rule
  that the returned scan carries no per-file minimum or maximum HOLDS, with a new justification:
  pruning completes inside the kernel before a file entry exists, so the stats wire shape that rule
  deferred never acquired a consumer. `ScanSpec`, `FileEntry`, and `LogicalField` gain no field, per
  CLAUDE.md § "ScanSpec is format-neutral".
* **Pruning is sound-not-complete, and correctness lives above the scan.** Every emitted node is
  logically implied by the user predicate; a node that cannot be translated soundly is dropped, so the
  scan opens MORE files rather than skipping one that could hold a matching row. The full predicate is
  still evaluated — in the scan's DataFusion filter when the DataFusion dialect renders it, and
  otherwise in the adapter's own outer `WHERE` per `vs-adapter/pushdown-declined-filter-self-apply`,
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
* **Column mapping needs no translator handling — the kernel resolves logical→physical itself.**
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
* **Exasol pre-normalises `>`→`<` and `>=`→`<=`**, recorded once in
  `vs-adapter/pushdown-file-pruning` and not restated here. The translator still handles the greater
  forms and still flips an operator whose column sits on the right, because the normalization governs
  which node kind arrives, not which side the column lands on.
* **The translator is a third independent walker over the same filter JSON, and that is deliberate.**
  No format-neutral predicate IR exists: `vs-expression`'s `render_df_filter_safe` renders a
  DataFusion SQL fragment and exposes no typed AST, and `adapter::iceberg_predicate` produces an
  `iceberg::spec::Predicate`. Extracting a shared IR would have to unify three output types, two
  literal vocabularies, and two stat-soundness contracts, none of which this feature needs — see the
  plan's decision log.
* **The translator stays private to `adapter::pushdown::format`**, so the frozen pushdown façade
  admits NO item and needs no delta against `vs-adapter/pushdown-module-structure`. This matches that
  feature's recorded rule for the Delta reader-protocol gate and the Delta type classifier, both
  reached only from inside `format`. The new submodule carries its own sibling `_tests.rs`, per the
  same feature's per-submodule test rule.
* **Apache Iceberg spec check — checked, and no Iceberg behavior changes.** No code on the Iceberg
  resolution path is touched: `adapter::iceberg_predicate`, `plan_files_from_table`, and the Iceberg
  reader are unedited, so the table spec's requirement that a scan filter files by "column bounds and
  counts that are stored by field id in manifests" is still satisfied exactly as
  `vs-adapter/pushdown-file-pruning` records it. The spec's ordered Column Projection resolution rule
  (1) — the partition-metadata rule — remains the deliberate, accurately-scoped trade-off
  `datafusion-scan/scan-execution-field-id-projection` records, neither closed nor widened here. The
  one substantive difference this feature must not paper over is a bound-soundness asymmetry: Iceberg
  requires `upper_bounds` "must be greater than or equal to all non-null, non-Nan values in the column
  for the file" with NO truncation caveat, while Delta's identical requirement carries the prefix-cut
  footnote handled above.
* Every error this feature surfaces is a `UdfError`, never a panic, because a panic inside a UDF is an
  abnormal VM exit that makes the engine SIGKILL every sibling VM of the statement part. No error text
  carries a bearer token, an OAuth client secret, a vended storage key, or any other credential value.

## Scenarios

### Scenario: Equality on a partition column prunes every file in a non-matching partition

* *GIVEN* a virtual schema over a Delta table partitioned by one column, whose active data files are distributed across partition values and include one file written to the Hive default-partition directory because its logged value is NULL
* *AND* a query whose WHERE clause is an equality on that partition column over a value the adapter can translate
* *WHEN* Exasol sends the corresponding `pushdown` request
* *THEN* the reader SHALL hand the Delta scan builder a `delta_kernel` predicate carrying that equality, and the resolved file list SHALL contain only the files whose logged partition value equals the requested value
* *AND* the files belonging to non-matching partitions SHALL NOT appear in the resolved file list nor in the scan-driving SQL
* *AND* the pruning SHALL be EXACT for a partition column rather than bounds-based, because Delta's `partitionValues` is a required per-file map holding the column's actual value, so a matching file is never dropped and a non-matching file is never kept
* *AND* an `IS NULL` predicate on that partition column SHALL resolve to the default-partition file alone, and MUST NOT match on the literal directory text `__HIVE_DEFAULT_PARTITION__`
* *AND* a predicate matching NO partition value SHALL resolve to an EMPTY file list and SHALL reach the adapter's existing empty-result route (`vs-adapter/pushdown-planning-empty-result`), so pruning everything returns no rows rather than erroring, fanning out to zero shards, or emitting a shard carrying an empty file list
* *AND* the reader MUST NOT filter the resolved file list itself after replay; the kernel's own selection vector SHALL be the only thing that drops a file

### Scenario: A range predicate prunes files whose min/max bounds exclude the value

* *GIVEN* a virtual schema over a Delta table whose active data files carry disjoint per-file min/max statistics for a numeric column
* *AND* a query whose WHERE clause is a range comparison or a `BETWEEN` on that column
* *WHEN* Exasol sends the corresponding `pushdown` request
* *THEN* the reader SHALL hand the scan builder the translated predicate so log replay evaluates each file's logged bounds, and a file whose bounds provably exclude every matching value SHALL NOT appear in the resolved file list
* *AND* a file whose bounds overlap the requested range SHALL remain in the resolved file list
* *AND* a file whose `add` action carries NO statistics SHALL remain in the resolved file list, because the Delta protocol marks `stats` optional and an absent bound proves nothing
* *AND* a `BETWEEN` SHALL desugar to a lower-bound AND an upper-bound comparison, and a bound that fails to translate SHALL be dropped while the other is kept, because dropping one bound only widens the surviving file set
* *AND* a comparison whose column sits on the RIGHT of the literal SHALL translate with the operator flipped, so the same predicate prunes identically in either written order

### Scenario: An untranslatable conjunct disables pruning for that conjunct only

* *GIVEN* a query whose WHERE clause is a translatable predicate AND an untranslatable one — for example an equality the translator emits alongside a `LIKE`, or alongside the explicitly-untranslatable not-equal
* *WHEN* Exasol sends the corresponding `pushdown` request
* *THEN* the reader SHALL emit a pruning predicate carrying ONLY the translatable conjunct and SHALL drop the untranslatable one
* *AND* a conjunction whose EVERY child is untranslatable SHALL yield NO predicate at all, and the reader MUST NOT substitute a literal-true predicate, because handing the kernel a trivial predicate buys no pruning and costs a stats pass
* *AND* the full original predicate SHALL still be applied — in the scan spec's filter when the DataFusion dialect renders it, and otherwise in the adapter's own outer `WHERE` per `vs-adapter/pushdown-declined-filter-self-apply` — so "untranslatable for Delta pruning" never means "unapplied"
* *AND* the query result SHALL be identical to the result the same query returns with no pruning predicate at all

### Scenario: An untranslatable branch of an OR disables pruning entirely

* *GIVEN* a query whose WHERE clause is a translatable predicate OR an untranslatable one
* *WHEN* Exasol sends the corresponding `pushdown` request
* *THEN* the reader SHALL emit NO pruning predicate derived from that disjunction, because a row satisfying the untranslatable branch MAY live in any file
* *AND* the resolved file list SHALL equal the unpruned file list for that disjunction
* *AND* the reader MUST NOT fold the translatable branches alone into a disjunction, because a narrower OR is not implied by the original and would drop files holding matching rows
* *AND* the query result SHALL be correct because the full predicate is still applied above the scan

### Scenario: An IN list prunes as an OR-chain of equalities and never as an empty junction

* *GIVEN* a query whose WHERE clause is an IN list of constants over a column the translator can resolve
* *WHEN* Exasol sends the corresponding `pushdown` request
* *THEN* the reader SHALL emit a disjunction of equalities, one per list element, and MUST NOT emit `delta_kernel`'s native IN predicate, because that predicate's skipping evaluator returns no result and therefore prunes nothing
* *AND* an IN list with ONE element MAY collapse to that single equality, matching the kernel's own single-element junction normalization
* *AND* an IN list in which ANY element fails to convert SHALL yield NO predicate, following the same all-branches rule the disjunction scenario states, because keeping a subset of the alternatives narrows the constraint beyond what the request implies
* *AND* an EMPTY translatable element set MUST NOT reach a junction constructor, because `Predicate::or_from` normalizes an empty disjunction to literal false, which would prune every file in the table and return no rows
* *AND* no code path in this feature SHALL construct a literal-false predicate under any input

### Scenario: A literal is typed from the column's Delta type or its node is dropped

* *GIVEN* pushdown requests carrying comparisons over Delta columns spanning the integer, string, floating-point, decimal, boolean, date, timestamp, and timezone-adjusted timestamp types
* *WHEN* the translator converts each request literal against the resolved column's own Delta type
* *THEN* each literal SHALL become the `Scalar` variant matching that column's type, and a literal whose request type cannot represent that column's type SHALL yield NO predicate for its node rather than a coerced or reinterpreted value
* *AND* a timestamp literal SHALL be parsed into MICROSECONDS since the epoch and a date literal into DAYS, through the kernel's own protocol-conformant scalar parser, and MUST NOT be handed to the kernel as a string scalar, which compares against no timestamp bound and silently prunes nothing
* *AND* an empty-string literal SHALL yield no predicate for its node, because the kernel's parser answers a null scalar for it and a null is not a constraint
* *AND* a column the table's schema does not declare SHALL yield no predicate for its node, resolved case-insensitively against the schema so an Exasol-upper-cased name still reaches its declared field
* *AND* a column whose Delta type is not a primitive SHALL yield no predicate for its node
* *AND* a not-equal comparison SHALL yield no predicate, because it is not soundly reducible to a bound test on a single range
* *AND* a `TIMESTAMP WITH LOCAL TIME ZONE` comparison SHALL yield no predicate and therefore never prune a Delta table today, because Exasol's real wire node type is `literal_timestamputc` while this translator's arm matches only the pre-existing synthetic name `literal_timestamp_utc`; this is the same gap already tracked against the Iceberg translator's identically-misspelled range-pruning arm as `(#242)`, inherited here rather than introduced, and it fails open — no wrong rows, only forgone pruning

### Scenario: Enabling the kernel's skipping surfaces no statistic to the engine or the wire

* *GIVEN* the shipped Delta reader, which builds its scan with the stats option that disables all stats work
* *WHEN* the reader is changed to hand the scan builder a pruning predicate
* *THEN* that stats option SHALL be removed so the scan builder keeps its own default, because the option's own contract disables internal data skipping even when a predicate is set, which would leave both partition and stats pruning inert
* *AND* the resolved scan MUST NOT carry any per-file minimum, maximum, null count, or record count, and the scan spec's file entries, common spec, and logical fields SHALL gain NO field, so every serialized Delta scan spec stays byte-identical for a request that prunes nothing
* *AND* the reader MUST NOT request the kernel's parsed-statistics output column, because it compares no bound itself and paying for a stats projection it never reads would cost metadata bandwidth for nothing
* *AND* the reader SHALL still replay the log ONCE per query and SHALL still return one entry per active path, so deletion-vector carriage, partition-value carriage, path verbatimness, and the size field are unchanged
* *AND* every scenario of `vs-adapter/delta-table-planning`, `vs-adapter/delta-reader-feature-gating`, and `vs-adapter/delta-type-mapping` MUST hold with no change to any test assertion or expected value, except the pruning statements that feature's own delta supersedes

### Scenario: A predicate the kernel cannot evaluate keeps every file

* *GIVEN* pushdown requests whose WHERE clauses target, in turn, a column the table declares but for which no file carries statistics, a boolean column, and a column compared against another column
* *WHEN* the reader hands each translated predicate to the scan builder
* *THEN* the resolved file list SHALL equal the unpruned file list in every case, and the request SHALL succeed
* *AND* the reader MUST NOT raise an error, refuse the request, or fall back to a non-pruning code path on account of a predicate the kernel cannot use, because the kernel's documented behavior for an unusable reference is to fold it to null and keep the file
* *AND* a predicate mixing one usable comparison with one unusable one SHALL still prune by the usable one, so a partly-unusable conjunction is not degraded to no pruning at all
* *AND* the reader MUST NOT inspect which columns carry statistics in order to decide what to translate, because the kernel already answers that question conservatively and a second copy of the rule would drift from it
* *AND* a table under Delta column mapping (`name` or `id` mode) SHALL NOT be such a case: a predicate phrased in the table's LOGICAL column name SHALL still prune, because the kernel resolves that name to the column's physical stat path itself, so the translator SHALL emit logical names as `snapshot.schema()` reports them and MUST NOT resolve physical names of its own

### Scenario: Pruning reaches every request shape and changes no result end to end

* *GIVEN* the seeded Delta fixtures `unity.delta_e2e.basic_partitioned`, partitioned by `letter` across six data files holding six rows, and `unity.delta_e2e.multi_part_stats`, five delete-free unpartitioned data files holding five rows whose per-file statistics are disjoint
* *WHEN* the suite issues a partition-column equality against the first, a stats-excluded range predicate against the second, and the same predicates again as a single-table scan, as an aggregate, and as one leg of a broadcast join
* *THEN* the generated pushdown SQL SHALL carry FEWER data files than the table's active file count for each pruning predicate, and the dropped files SHALL be exactly those the predicate provably excludes
* *AND* the rows each query returns SHALL be identical to the rows the same query returns with pruning inert, so pruning is observable in the plan and invisible in the result
* *AND* every request shape SHALL prune through the SAME seam, because the format-reader seam forwards the request's filter to the reader for a single-table scan and for each join leg alike (`vs-adapter/pushdown-format-neutral-resolution`), so no shape is left unpruned and none is gated on the table format
* *AND* a join SHALL prune each side by that side's OWN side-local predicate, and MUST NOT apply one side's predicate to the other's file list
* *AND* the suite MUST fail (not skip) when the Unity Catalog server, MinIO, or Exasol is unreachable
