# Plan Review Findings: add-export-partitioned-iceberg-table

## Summary
- Axes checked: 4/4
- Total findings: 8 (Blockers: 3, Findings: 5)
- Intent Fidelity blockers: 0

## Intent Fidelity
No objection: axis checked. Each acceptance criterion of #568 maps to a scenario: every transform (append scenario, `by_value` and `by_time` fixtures), NULL partition values (NULL scenario), pruning (filter scenario), bounded memory (many-partitions scenario), E2E per transform (Scenario Coverage table). The optional `PARTITION_BY` follows the interview answers (Spark/Iceberg-style list, refused for an existing table). The out-of-scope item (changing an existing table's spec) is honored by the refusal scenario. No extra work lacks a traceable need.

## Feasibility

#### [NFR_IGNORED] BLOCKER
- Location: intent.md § Directions D1, D2; plan.md § Implementation step 4; export/export-partitioned-iceberg-table/spec.md § Background (budget bullet); architecture.md § Constraints (writer budget line)
- Issue: The plan bounds memory by "sums `current_written_size()` over the open writers", and D1 states this counts "each open file's written bytes plus its in-progress row group". In the pinned iceberg-rust (`da5cc19`), `ParquetWriter::current_written_size` returns `bytes_written() + in_progress_size()`, and parquet 59.3.2 documents `in_progress_size` as the "Anticipated encoded size of the in progress row group", not memory: `memory_size()` is the memory estimate and "should be greater than or equal to" it (it adds unencoded dictionary keys and encoder buffers). The sum also charges nothing for the fixed cost of each open writer (column encoders, the async Parquet writer, the storage writer), so the number of open writers, and with it memory, grows with the partition count while the byte sum stays far below the budget. Step 4 also closes only one writer per write ("Above the budget it closes the largest open writer"), which does not restore the budget when many small writers are open. The design does not deliver the acceptance criterion "completes within a bounded amount of memory" for many partitions.
- Fix: In intent.md, rewrite D1 to state what the budget counts as a memory estimate: each open file's `current_written_size()` plus a fixed per-open-file charge (or a cap on open files derived from the budget), and state the charge's value and source. In plan.md step 4, close largest-first until the sum is back under the budget, and make the close policy a pure function over `(key, size)` pairs plus the open-file count. Update the export-partitioned-iceberg-table § Background budget bullet and the architecture.md § Constraints writer-budget line to the same rule.

#### [UNSTATED_ASSUMPTION] FINDINGS
- Location: plan.md § Implementation step 9; specs/testing.md § Correctness oracles
- Issue: `data_file_partitions` reads each data file's partition values and row ids "through iceberg-rust's scan planner and reader in the test process". testing.md § Correctness oracles states an E2E test "asserts only on what Exasol shows", and for an export scenario about catalog or storage state allows only "the catalog's REST API or list the warehouse bucket". Reading manifests through iceberg-rust is a third oracle that the rules do not name.
- Fix: Add a direction to intent.md stating that the partition placement scenarios read manifests and data files through iceberg-rust in the test process, why no Exasol-visible oracle can show a file's partition tuple, and add a matching rule to `specs/testing.md` § Correctness oracles in the plan's Implementation list.

## Requirement Quality

#### [AMBIGUOUS_REQUIREMENT] BLOCKER
- Location: export/export-partitioned-iceberg-table/spec.md § Scenario: An export into many partitions completes within a bounded amount of memory; intent.md § Directions D8; plan.md § Verification (`test_many_partitions_export_completes`)
- Issue: The THEN asserts only "the export SHALL succeed with a row count of 1,000,000" and the read-back. The source is one column `K = MOD(ROWNUM, 10000)`: 100 rows per partition, a few hundred encoded bytes per open file, a few MB over 10,000 files. That is far under the 64 MiB minimum budget of D2, so the planned writer never closes a file early and behaves exactly as an uncapped fan-out. D8 asserts "a load that an uncapped fan-out would exceed in the Docker Exasol" without evidence. The test cannot fail for an unbounded implementation, so the acceptance criterion has no discriminating test.
- Fix: Validate D8 against the Docker Exasol before the plan is approved: run the uncapped fan-out (today's `FanoutWriter` behavior) on the chosen load and record that it fails. Then size the scenario's source (row width, row count, partition count) so the planned budget is reached and the uncapped fan-out exceeds the memory limit, and state both outcomes in D8. Add a THEN step that the export wrote more data files than partitions, or another Exasol- or catalog-visible signal that the budget closed files, per testing.md § Correctness oracles ("A regression test uses a discriminating fixture").

#### [COMPLETENESS_GAP] FINDINGS
- Location: export/export-partitioned-iceberg-table/spec.md § Background (`PARTITION_BY` bullet) and § Scenario: An invalid PARTITION_BY fails the export before any data is read
- Issue: The pinned iceberg-rust `PartitionSpecBuilder` refuses two field lists that the scenario's refusal list does not name: a non-identity field whose derived name equals a schema column ("Cannot create partition with name ... that conflicts with schema field", for example `bucket(4, ID)` when the source also has a column `ID_bucket`), and redundant time transforms on one column (`dedup_name` is `time` for year, month, day, and hour, so `year(D), month(D)` fails with "Cannot add redundant partition"). Both reach the catalog create call instead of failing before any row is read. The grammar also states no whitespace rule, while the manual test uses `'day(D), bucket(4, ID)'`, and no way to name a column whose name holds `,`, `(`, `)`, or blanks.
- Fix: Add both shapes to the WHEN list of the invalid-`PARTITION_BY` scenario and to the `partition_by.rs` checks in plan.md step 1 and its unit case table. State in the § Background bullet that blanks around tokens are ignored, and either define a quoting rule for column names or state that such columns cannot be partition sources.

#### [COMPLETENESS_GAP] FINDINGS
- Location: plan.md § Implementation steps 8 and 10; specs/testing.md § Fixtures
- Issue: testing.md § Fixtures requires a fixture guard for every Spark fixture but `export_append` ("Every Spark fixture below but `export_append` has one"), and lists each Spark fixture with its shape. The plan adds the `e2e_export_partition` Spark fixture with no guard and no testing.md entry, and step 10 deletes the `partitioned` table while testing.md's `export_append` entry still lists "a partitioned table".
- Fix: Add to plan.md a fixture guard for `e2e_export_partition` (reading the catalog metadata, as the `export_append` guard does, to assert each table's spec and that every partition holds several rows), and an Implementation step that adds the fixture to `specs/testing.md` § Fixtures and drops "a partitioned table" from the `export_append` entry.

#### [COMPLETENESS_GAP] FINDINGS
- Location: export/export-partitioned-iceberg-table/spec.md § Scenario: A filter on a partition column reads only the exported files of matching partitions
- Issue: "a strict subset of the export's data files" is also met by per-file min/max pruning (pushdown/pushdown-file-pruning § Scenario: Range predicate prunes files via per-file min/max bounds), so the test can pass even if the exported files carry no usable partition values, for example on an identity or `truncate` field where each file's bounds already exclude the literal.
- Fix: Change the THEN to "the query SHALL scan exactly the export's data files whose partition can hold a matching row", or require in the GIVEN that each exported file's column bounds overlap the filter literal, and state the fixture choice in plan.md step 8.

## Design Depth

#### [ARCHITECTURE_DRIFT] BLOCKER
- Location: architecture.md (plan delta); specs/architecture.md § Interfaces
- Issue: plan.md § Impact states "New optional export option `PARTITION_BY`", and the export-options delta extends the statement form. specs/architecture.md § Interfaces spells out the `LAKEHOUSE_EXPORT` statement form ("`... [WRITERS_PER_NODE = '<n>'] [SINGLE_COMMIT = 'true'|'false']`"), and the plan's architecture delta has no Interfaces block, so the recorded interface will omit the new option.
- Fix: Add a `<!-- DELTA:CHANGED -->` `## Interfaces` block to the plan's architecture.md that adds `[PARTITION_BY = '<fields>']` to the `LAKEHOUSE_EXPORT(...)` line (and `[ALLOW_HTTP = 'true'|'false']`, which the recorded line already lacks), keeping every other Interfaces line verbatim.

#### [DESIGN_VIOLATION] FINDINGS
- Location: plan.md § Implementation steps 1 and 3; intent.md § Directions D4
- Issue: Information hiding: step 1 makes `partition_by.rs` the sole owner of the grammar, naming rule, and type table, but the name-collision and redundancy rules of the pinned iceberg-rust builder (see the completeness finding above) would then live in a second place, the catalog create call, and surface as a catalog error after the hook has passed.
- Fix: State in plan.md step 1 that `partition_by.rs` builds the spec with iceberg-rust's `PartitionSpecBuilder` against the export's own schema (bound, not only unbound) in the writer's type check, so every library rule fails in that one module before `create_table` or `stage_table`.

No ADR objection: axis checked. intent.md has no `ADR: yes` direction, and the plan conforms to the accepted ADRs on export commits (`export-has-no-final-commit-per-writer-commits`, `export-commit-loop-rechecks-writer-tag`) and on identity partition reads (`a-files-identity-partition-value-wins-over-its-column-ahead-of-the-initial-default`). The architecture delta's BASE matches `git hash-object specs/architecture.md`.
