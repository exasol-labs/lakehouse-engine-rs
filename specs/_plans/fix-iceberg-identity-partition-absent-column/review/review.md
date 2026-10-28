# Plan Review Findings: fix-iceberg-identity-partition-absent-column

## Summary
- Axes checked: 6/6
- Total findings: 11 (Blockers: 3, Findings: 8)
- Intent Fidelity blockers: 0

## How this plan may fail

1. Recording the plan deletes 14 bullets of the `delta/delta-table-planning` Background, including two recorded scoped exceptions and the facts that the sibling `delta/delta-table-planning-declaration-checks` inherits. Routed to Requirement Quality, `[COMPLETENESS_GAP]`.
2. The Spark fixture job fails before any test runs: Iceberg 1.10.1 `add_files` cannot parse a `timestamp` partition string, and `ident_part_types` imports two timestamp identity columns. Routed to Feasibility, `[HIDDEN_DEPENDENCY]`.
3. DataFusion 55.2 skips the expression adapter for a file whose physical schema equals the logical file schema when no predicate exists. The fallback expression then reaches evaluation unreduced, and the plan defines no behavior for it. Routed to Feasibility, `[UNSTATED_ASSUMPTION]`.

## Intent Fidelity

[no objection — axis checked: the request asks for Column Projection rule (1) from the manifest partition struct. S1 to S8 implement it per file's own spec (`decision-log.md` [1]), with the interview's answers applied: the file column wins (S3), and a null partition value is present (S4). The nested-source exclusion is recorded as a scoped exception in the absent-fields Background, which AGENTS.md permits. Delta precedence is unchanged and was not asked for.]

## Feasibility

#### [HIDDEN_DEPENDENCY] BLOCKER
- Location: plan.md § Implementation Tasks, task 2.1 (`ident_part_types`); absent-fields delta § "A reconstructed identity value reads the same as a stored one for every admitted type"
- Issue: Task 2.1 imports row 1 and row 3 of `ident_part_types` through `add_files` with `identity` fields on `timestamp_ntz` and `timestamp` columns. The fixture job pins Iceberg 1.10.1 (`run_fixtures.sh` `ICEBERG_VERSION="1.10.1"`). In that release, `Conversions.fromPartitionString` has no case for `TIMESTAMP` or `TIMESTAMP_NANO`, and its `default` branch throws `UnsupportedOperationException("Unsupported type for fromPartitionString: ...")`. `add_files` builds each `DataFile` through `DataFiles.Builder.withPartitionValues` or `withPartitionPath`, and both call `fromPartitionString`. The import of a timestamp-partitioned Hive source therefore fails, the fixture job exits non-zero, and S6 cannot be tested as written. The repro covered only a `string` partition, so this was never exercised.
- Fix: First run one `add_files` of a Hive source partitioned by a `timestamp` column against the fixture Spark to confirm the failure. If it fails, make these edits. In plan.md task 2.1, remove `timestamp_ntz` and `timestamp` from the `ident_part_types` column list. In the absent-fields delta's S6 GIVEN, remove `timestamp` and `timestamptz` from the type list, and delete the AND step about the `timestamptz` column and `sqlCode 22002`. In plan.md § Verification, extend the bullet "Spark 3.5 cannot write ..." so that it also says Iceberg 1.10.1 `add_files` cannot import a `timestamp` or `timestamptz` identity partition (`Conversions.fromPartitionString`), which leaves the S7 unit test as the only proof for those two types. If the import succeeds, record the command and its result in `decision-log.md` § Repro and leave the plan unchanged.

#### [UNSTATED_ASSUMPTION] BLOCKER
- Location: plan.md § Implementation Tasks, tasks 1.3 and 1.4; decision-log.md § [2] Rationale
- Issue: Decision [2] says "the presence check must stay in the adapter", and task 1.4 has `FieldIdExprAdapter::rewrite` reduce every fallback expression. The planner's DataFusion 55.2 claims are correct: `opener/mod.rs:776-801` substitutes partition literals by name before the adapter is created, and `PhysicalExprAdapterFactory::create(logical, physical)` (`schema_rewriter.rs:175`) receives no file. The rationale misses one fact. `datafusion-datasource-parquet-55.2.0/src/opener/mod.rs:1009-1017` computes `needs_rewrite = prepared.predicate.is_some() || prepared.logical_file_schema != physical_file_schema` and skips the adapter for that file when it is false ("Skip the tree walk entirely in that case"). For such a file, the projection holds the fallback expression with its hidden child already replaced by a literal, and DataFusion evaluates it unreduced. The plan defines no evaluation for an unreduced fallback expression. An implementation that treats this state as unreachable fails the query, or emits the wrong column, for a file whose schema matches the logical schema exactly. The S3 and S4 unit tests check only the adapter's rewrite, so they cannot catch it.
- Fix: In plan.md task 1.3, add this sentence: "An unreduced fallback expression evaluates to its file-column child, because DataFusion skips the adapter only when the file's physical schema equals the logical file schema, which binds the field." In decision-log.md § [2] Rationale, add one sentence stating that DataFusion 55.2 skips the adapter for a file whose physical schema equals the logical file schema when no predicate exists (`opener/mod.rs:1009-1017`), so the expression's own evaluation covers that file. In plan.md § Verification, add a unit test in `partition_values_tests.rs` (for example `unreduced_fallback_evaluates_its_file_column`) that evaluates an unreduced fallback expression over a `RecordBatch` and expects the file column's values.

#### [UNSTATED_ASSUMPTION] FINDINGS
- Location: plan.md § Implementation Tasks, task 1.3; decision-log.md § [2] Consequences
- Issue: The hidden column must encode three per-file states: "no value", "null", and "a value". A `ScalarValue` of the column's declared type cannot tell "no value" from "null". The plan names no Arrow type for the hidden column. DataFusion also requires every `PartitionedFile` to carry a value for every partition column. `replace_columns_with_literals` (`schema_rewriter.rs:64-83`) substitutes by column name, so a hidden-column name that equals a file column name would replace the file column too.
- Fix: In plan.md task 1.3, state the hidden column's Arrow type, for example a nullable struct with one child of the declared type: a null struct means "no value", a null child means "null", and a non-null child is the value. State the naming rule that keeps the hidden name distinct from every logical and physical column name.

#### [NFR_IGNORED] FINDINGS
- Location: plan.md § Impact, bullet 3
- Issue: The bullet says the files argument "grows by one small map per data file". The map goes onto every data file of every Iceberg table that has an `identity` partition field. That includes natively written tables, which are the common case, not only migrated ones. Each shard's file list travels as one `files` VARCHAR value (`parallelism/work-unit-sharding`), and an Exasol VARCHAR holds at most 2,000,000 characters. No spec bounds this value, so the growth reduces the largest shard that still fits, and the plan neither quantifies nor states this.
- Fix: In plan.md § Impact bullet 3, state the growth per file (the JSON bytes of one `"<name>":"<value>"` pair per `identity` column), and state that a shard's `files` value stays bounded by Exasol's 2,000,000-character VARCHAR limit, which this plan does not change.

## Requirement Quality

#### [COMPLETENESS_GAP] BLOCKER
- Location: specs/_plans/fix-iceberg-identity-partition-absent-column/delta/delta-table-planning/spec.md § Background
- Issue: A `DELTA:CHANGED` Background replaces the whole section, and "any line the block omits is deleted from the permanent spec" (`/speq:spec-merge`). The recorded Background has 19 bullets (lines 7-25), and the delta keeps 5, so recording deletes these facts:
  - two scoped exceptions (line 21, a type change the declaration does not record; line 22, a column dropped after the last `REFRESH`). AGENTS.md requires each deviation to be fixed or recorded as a scoped exception, so deleting the record breaks that rule.
  - the Delta protocol "Column Mapping" quote (line 18), which is the normative citation behind the column-mapping scenario.
  - lines 17, 19, and 20 (`createVirtualSchema` reads no Delta log; the protocol honors the mode only with the `columnMapping` feature; DataFusion rewrites `c IS NULL` to `false` for a non-nullable field). `delta/delta-table-planning-declaration-checks` says "The scenarios here share the parent feature's Background", and its two scenarios rest on these facts.
  - line 23 (the reader-feature gate stays at pushdown), line 24 (`delta_kernel` leaves `add.path` percent-encoded and `ListingTableUrl::parse` decodes it), line 9's per-format needs, and line 11's injected object store.

  None of these facts relates to issue #99. Only line 12 had to change.
- Fix: In the delta-table-planning delta's `## Background`, restore recorded lines 7-25 verbatim. Change only line 12: keep "Delta never writes a partition column's value into the data file, so the scan spec must carry partition values." and replace its last two sentences with "The Iceberg reader leaves `partition_columns` empty and fills `partition_values` only with the `identity` partition values of each file's own spec: see `scan-read-path/scan-execution-field-id-projection-absent-fields`." Add one sentence to plan.md § Out of scope: "`delta/delta-table-planning`'s Background exceeds five bullets; trimming it needs its facts moved to the scenarios and the declaration-checks sibling first." The longer Background is pre-existing, and this review accepts it.

#### [COMPLETENESS_GAP] FINDINGS
- Location: specs/_plans/fix-iceberg-identity-partition-absent-column/scan-read-path/scan-execution-field-id-projection-absent-fields/spec.md § Background, bullet 4
- Issue: The cut drops two facts from recorded lines 11 and 13. First, the reason that a struct, list, or map `initial-default` is not applied: "Exasol has no struct, list or map types, and the Iceberg spec requires `unknown`, `variant`, `geometry` and `geography` columns to default to null." AGENTS.md calls a deviation forced by an Exasol type limit a named trade-off, and the new bullet gives only "the Arrow-type tag vocabulary ... is primitive-only". Second, "regardless of format version": the read path reads `initial-default` even on a v1 or v2 table. The rewritten bullet no longer says so.
- Fix: In bullet 4, after "is primitive-only", add "a named Exasol type-limit trade-off: Exasol has no struct, list, or map type, and the Iceberg spec requires `unknown`, `variant`, `geometry`, and `geography` columns to default to null". Change "The read path reads `initial-default` from the current schema" to "The read path reads `initial-default` from the current schema regardless of format version".

#### [IMPLEMENTATION_DETAIL] FINDINGS
- Location: same delta § Background, bullet 3
- Issue: "iceberg-rust fills it only for an absent column and skips a null value" and "For a file whose physical values equal its partition value, all three return the same rows" support no GIVEN, WHEN, or THEN step. They repeat `decision-log.md` [1] Alternatives and Rationale. Only the Iceberg Java null rule supports a step (S4's THEN).
- Fix: Delete both sentences from bullet 3. Keep "Iceberg Java fills an identity partition constant even when the file holds the column, and treats a null constant as present. This engine follows the spec text on precedence and Iceberg Java on null."

#### [REQUIREMENT_CONFLICT] FINDINGS
- Location: recorded `scan-read-path/scan-execution-partition-values` § "A materialized partition column is a first-class scan column" (last AND) and § "The logged partition value wins over a physically present partition column" (GIVEN); plan delta scan-execution-partition-values
- Issue: The recorded step "the UDF MAY use a file's partition values to skip that file entirely when the filter cannot be satisfied by them" covers every `partition_values` key. After this plan, `partition_values` also holds `identity` keys that are not authoritative when the file holds the column (S3). A skip on such a key drops rows that the file column satisfies. The log-wins GIVEN says "a partition column's name" without restricting it to `partition_columns`, so it can be read as overlapping S3.
- Fix: In the partition-values delta, add a `DELTA:CHANGED` block for "A materialized partition column is a first-class scan column" whose last AND reads "a file's partition values for the columns `partition_columns` lists". Add a `DELTA:CHANGED` block for "The logged partition value wins over a physically present partition column" whose GIVEN reads "a physical Parquet column bearing the name of a column the common spec's `partition_columns` lists".

## Task Breakdown

#### [TRACEABILITY_GAP] FINDINGS
- Location: plan.md § Verification, Scenario Coverage row "A scan with no partition columns is unchanged (CHANGED)"
- Issue: The row maps the changed scenario to `crates/lakehouse-engine/tests/it/scan_partition_values.rs`, a host test that writes files. `specs/testing.md` § Coverage rule says such a test "is no proof of a scenario", and every scenario needs at least one E2E test. The changed GIVEN adds Iceberg scans whose files carry no `identity` value, and no E2E test is mapped to it.
- Fix: In plan.md task 2.2, add an E2E test (for example `unpartitioned_iceberg_files_argument_carries_no_partition_values`) that runs `EXPLAIN VIRTUAL` over an existing unpartitioned Iceberg table and asserts that the files argument carries no `partition_values` key. Map the row to it, and tag the test with the scenario's `/// Scenario:` line.

## Design Depth

[no objection — ADR_OVERPROMOTION checked: [1] names criterion 4, states the ADR search result (`implement-iceberg-column-projection-rule-3-for-all-absent-fields`), supersedes that ADR, and its Decision holds no path, signature, or flag. [2] is `no`.]

[no objection — ARCHITECTURE_DRIFT checked: [1] names criterion 4, not 1 to 3, and carries `Architecture: no change` with a reason. plan.md adds no component, boundary, or dependency. `specs/architecture.md` line 88 forbids a reader from deriving a partition column at pushdown, and the Iceberg reader leaves `partition_columns` empty.]

[no objection — ADR_CONFLICT checked: ADR 002 is superseded explicitly by [1]. `partition-columns-via-datafusion-native-mechanism` still holds: the value enters through `table_partition_cols`, the projection builder keeps the index remap as `Column` expressions at the remapped scan indices, and no `ProjectionExec` is added. `format-reader-engine-owns-whole-resolution` holds: the change stays inside the Iceberg reader.]

#### [INFORMATION_COUPLING] FINDINGS
- Location: plan.md § Implementation Tasks, task 1.2
- Issue: The rule for turning a value of the file's Iceberg type into the declared type now lives in two places. At scan time, `DefaultPhysicalExprAdapter` casts a physical column, inside the limits that the field-id adapter's `admits` checks. At plan time, the Iceberg reader casts the partition literal. Task 1.2 says "with the Arrow cast the scan applies to a physical column" but names no shared function. A later change to the scan's cast, such as its options or timestamp handling, would leave the reader's cast unchanged. S7 compares against fixed expected values, so it would not detect that drift.
- Fix: In plan.md task 1.2, name the single function and cast options that both paths use: the cast that `DefaultPhysicalExprAdapter`'s cast expression applies in DataFusion 55.2, called from the Iceberg reader instead of a separate `arrow::compute::cast` call. State that S7's expected value for each type comes from that same function applied to a one-element array of the file's type.

## Prose Quality

#### [PROSE_UNCLEAR] FINDINGS
- Location: plan.md § Implementation Tasks, task 1.3
- Issue: "the expression reduces itself, given whether its field is bound in a file, to the file column, a literal of the declared type, or the file column for the absent-field chain" lists three outcomes without saying which state leads to which, and it names "the file column" twice. The task also packs seven separate changes into one paragraph.
- Fix: Replace that clause with three sentences. "A field the file binds reduces to the file column. An absent field with a value or a null reduces to a literal of the declared type, NULL for a null. An absent field with no value reduces to the file column, which the absent-field chain then resolves." Split the rest of task 1.3 into one sentence per change.
