# Feature: Delta Plan-Time File Pruning

The adapter translates the soundly translatable nodes of the Exasol WHERE predicate into a `delta_kernel::expressions::Predicate` and hands it to the Delta scan builder. Log replay then drops files on partition values and per-file min/max statistics before any Parquet byte is read. The full predicate stays applied above the scan and is the only source of row-level correctness.

## Background

* The kernel prunes and the engine only translates. `ScanBuilder::with_predicate` drives partition pruning and statistics-based skipping in one pass. The adapter builds the predicate and consumes the selection vector from `scan_metadata()`. It reads no statistic, compares no bound, and does not filter the resolved files itself.
* The reader must not build its scan with `StatsOptions::none()`, which disables internal data skipping even when a predicate is set. Removing that call restores the scan builder's default, which keeps skipping on.
* No statistic reaches the engine or the wire format. `ScanSpec`, `FileEntry`, and `LogicalField` gain no field, and the reader does not request the kernel's parsed-statistics column.
* Pruning is sound but not complete. Every emitted node is implied by the user predicate. A node that cannot be translated soundly is dropped, so the scan opens more files and never skips a file that could hold a matching row. The full predicate is applied in the scan's DataFusion filter when the DataFusion dialect renders it, and otherwise in the adapter's outer `WHERE` (`pushdown/pushdown-declined-filter-self-apply`).
* The kernel keeps a file when it cannot evaluate the predicate: a column outside the statistics set folds to NULL, a predicate ineligible for skipping keeps all files, and a predicate that is true or null keeps the file. The translator therefore never checks whether a column carries statistics.
* Delta protocol "Per-file Statistics" requires `maxValues` to be greater than or equal to, and `minValues` less than or equal to, all valid values in the file. String maxima stay upper bounds because the writer appends a tie-breaker after truncating. Timestamp truncation floors to the millisecond, and the kernel compensates by subtracting 999 microseconds from the bound. A writer that emits a bare string prefix would defeat the bound, and no reader can detect it. This is a protocol-trust assumption shared by every Delta reader.
* `stats` is optional per file (Delta protocol "Add File and Remove File"), so a file without statistics is always kept.
* `delta_kernel` cannot prune on `BinaryPredicateOp::In`, because `eval_pred_in` returns `None`. An IN list becomes an OR-chain of equalities. `Predicate::or_from([])` returns `false`, which would prune every file, so an IN list with no translatable element produces no constraint.
* `delta_kernel::Predicate` has no `negate()`. Negation is `Predicate::not(pred)`.
* A timestamp literal is parsed with `PrimitiveType::parse_scalar`. `Scalar::Timestamp` and `Scalar::TimestampNtz` hold microseconds since the epoch and `Scalar::Date` holds days. A string literal builds `Scalar::String`, which compares against no timestamp bound and prunes nothing. `parse_scalar("")` returns `Scalar::Null`, which is not a constraint.
* `Expression::column` takes a path iterator, so a flat column is `Expression::column(["NAME"])`.
* Exasol delivers upper-cased column names and Delta's `StructType::field` is case-sensitive. The translator resolves a request column through its declared binding key, recorded at `createVirtualSchema` in the column notes (`vs-adapter/column-source-notes`). It finds the note by the uppercase fold, then the current snapshot field with the note's physical name (`name` mode) or field id (`id` mode), and emits that field's current logical name. Under `none` mode it resolves the declared name case-insensitively. A column whose key the snapshot no longer carries yields no constraint.
* Under column mapping (`name` or `id` mode) the kernel maps the logical column name to the physical statistics path itself, so the translator emits logical names as `snapshot.schema()` reports them.
* `delta.dataSkippingNumIndexedCols` defaults to 32 leaf fields, and only skipping-eligible types carry min/max. A predicate over a column without statistics reaches the kernel and keeps every file.
* Exasol rewrites `>` to `<` and `>=` to `<=` (see `file-planning/pushdown-file-pruning`). The translator still handles the greater forms and flips the operator when the column sits on the right.
* The translator is private to `adapter::pushdown::format`, so the pushdown façade gains no item.
* A `TIMESTAMP WITH LOCAL TIME ZONE` comparison never prunes, because Exasol's wire node type is `literal_timestamputc` and the translator matches only `literal_timestamp_utc`. The Iceberg translator has the same gap (#242). The gap fails open.
* Every error is a `UdfError`, never a panic, and no error text carries a credential value.


## Scenarios

### Scenario: Equality on a partition column prunes every file in a non-matching partition

* *GIVEN* a virtual schema over a Delta table partitioned by one column, whose active data files are distributed across partition values and include one file written to the Hive default-partition directory because its logged value is NULL
* *AND* a query whose WHERE clause is an equality on that partition column over a value the adapter can translate
* *WHEN* Exasol sends the corresponding `pushdown` request
* *THEN* the reader SHALL hand the Delta scan builder a `delta_kernel` predicate carrying that equality, and the resolved file list SHALL contain only the files whose logged partition value equals the requested value
* *AND* the files belonging to non-matching partitions SHALL NOT appear in the resolved file list nor in the scan-driving SQL
* *AND* the pruning SHALL be EXACT for a partition column rather than bounds-based, because Delta's `partitionValues` is a required per-file map holding the column's actual value, so a matching file is never dropped and a non-matching file is never kept
* *AND* an `IS NULL` predicate on that partition column SHALL resolve to the default-partition file alone, and MUST NOT match on the literal directory text `__HIVE_DEFAULT_PARTITION__`
* *AND* a predicate matching NO partition value SHALL resolve to an EMPTY file list and SHALL reach the adapter's existing empty-result route (`file-planning/pushdown-planning-empty-result`), so pruning everything returns no rows rather than erroring, fanning out to zero shards, or emitting a shard carrying an empty file list
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
* *AND* the full original predicate SHALL still be applied — in the scan spec's filter when the DataFusion dialect renders it, and otherwise in the adapter's own outer `WHERE` per `pushdown/pushdown-declined-filter-self-apply` — so "untranslatable for Delta pruning" never means "unapplied"
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
* *AND* every scenario of `delta/delta-table-planning`, `delta/delta-reader-feature-gating`, and `delta/delta-type-mapping` MUST still hold, except the pruning statements that feature's own delta supersedes

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
* *AND* every request shape SHALL prune through the SAME seam, because the format-reader seam forwards the request's filter to the reader for a single-table scan and for each join leg alike (`file-planning/pushdown-format-neutral-resolution`), so no shape is left unpruned and none is gated on the table format
* *AND* a join SHALL prune each side by that side's OWN side-local predicate, and MUST NOT apply one side's predicate to the other's file list

### Scenario: A filter column resolves through its declared binding key

* *GIVEN* a local copy of a `name`-mode column-mapped Delta table with columns `a` and `b`, whose data files hold disjoint value ranges of each such that the file holding `a = 7` holds no `b = 7`, the column notes `createVirtualSchema` recorded for it, and a later commit appended to the copy's log that swaps the two display names without rewriting a data file
* *WHEN* the Delta format reader resolves the scan for a filter `A = 7` against those notes
* *THEN* the reader SHALL translate the filter on `A` to the field that carries `a`'s recorded physical name, now displayed as `b`, so pruning keeps the file whose statistics for that physical column admit 7
* *AND* a filter on a declared column whose physical name the current snapshot no longer carries SHALL prune no file for that conjunct

### Scenario: Delta pruning keeps every file with a matching row for every filter shape

* *GIVEN* a virtual schema over a partitioned Delta table whose files differ in partition value, per-file statistics, per-row-group bounds, per-page bounds, and NULL content, including a NULL partition value and a file whose statistics-only column is NULL in every row, and a native Exasol table that holds the same rows
* *WHEN* queries apply `NOT` over `AND`, `OR`, `NOT`, `IN`, `BETWEEN`, `IS NULL`, and `IS NOT NULL` on the partition column, the statistics-only column, and both, with a `LIKE`, a scalar function, or a predicate the DataFusion dialect declines as the untranslatable side
* *THEN* each query SHALL return the native table's rows, and the scanned files SHALL include every file that holds a matching row
* *AND* a `NOT` over a partly translated predicate (`file-planning/pushdown-file-pruning`) MUST NOT prune by its negation, while for a fully translated predicate the scanned files SHALL be exactly the files the kernel's partition and statistics evaluation keeps, so a change that turns pruning off fails
* *AND* a query that scans no file SHALL take the empty-result route, and every other query SHALL apply its predicate in the scan or in the adapter's outer `WHERE`, never drop it
