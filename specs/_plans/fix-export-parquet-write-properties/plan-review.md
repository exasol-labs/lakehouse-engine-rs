# Plan Review Findings: fix-export-parquet-write-properties

## Summary
- Axes checked: 4/4
- Total findings: 7 (Blockers: 2, Findings: 5)
- Intent Fidelity blockers: 0

## Intent Fidelity
no objection — axis checked: each Behavior bullet and acceptance criterion of #670 maps to a scenario, an Implementation step, or a Verification row (codec and row group footer check, snappy append, three created-table properties, unpartitioned streaming and rolling, `docs/export.md`). The four interview answers (fail on an unusable property, no memory cap on row group size, Parquet regardless of `write.format.default`, no new feature) are carried out as decided. Out-of-scope items match the issue.

## Feasibility

#### [UNSTATED_ASSUMPTION] FINDINGS
- Location: plan.md § Implementation step 2; specs/_plans/fix-export-parquet-write-properties/export/export-iceberg-table/spec.md § Background, "Data files" bullet
- Issue: `ParquetWriterBuilder::from_table_properties` starts from `WriterProperties::builder()`, which keeps parquet-rs's `max_row_group_row_count` of 1,048,576. A row group therefore ends at whichever comes first, 1,048,576 rows or `write.parquet.row-group-size-bytes`. The plan and the docs step describe only the byte limit, so a narrow table's row groups end well below 128 MiB without explanation.
- Fix: Add a direction to intent.md stating that the row cap of 1,048,576 rows stays, and add that sentence to the "Data files" subsection in plan.md § Implementation step 5.

## Requirement Quality

#### [COMPLETENESS_GAP] BLOCKER
- Location: plan.md § Implementation step 2; export/export-append-iceberg-table/spec.md § Scenario "An unusable write property fails the export before any data is written"
- Issue: `write.parquet.row-group-size-bytes = 0` parses as a valid `usize`, and `from_table_properties` passes it to parquet-rs `set_max_row_group_bytes(Some(0))`, which panics (`assert_ne!(value, Some(0), "Cannot have a 0 max row group bytes")`). Step 2 handles only parse errors and `lzo`, so this value panics in the writer instead of failing with "an error that names the table, the property, and its value", which the interview answer requires.
- Fix: In plan.md § Implementation step 2, refuse `parquet_row_group_size_bytes() == 0` through the same error path as `lzo`, before calling `from_table_properties`. Add a third case row, `write.parquet.row-group-size-bytes = 0`, to `export_refuses_unusable_write_properties` in § Verification.

#### [AMBIGUOUS_REQUIREMENT] BLOCKER
- Location: export/export-iceberg-table/spec.md § Scenario "An export into an unpartitioned table writes its rows as they arrive"; plan.md § Verification, test `an_unpartitioned_sink_opens_its_data_file_at_the_first_write`; intent.md [D5]
- Issue: The step "SHALL hold no rows beyond the row group in progress" has no observable that a project-legal test can check. D5 admits that no E2E test separates streaming from buffering. The planned proof is a host test that opens the sink against the fake catalog of `iceberg_sink_tests.rs` and asserts on the recorded request log. `specs/testing.md` § Testing strategy forbids that: "A unit test ... starts no server, asserts nothing on a fake's request log", and "A fake server injects only the faults an E2E test cannot produce". No fault is injected here. The scenario's other step, rolling at `write.target-file-size-bytes`, is proven only by `export_append_follows_the_table_write_properties`, which the coverage table does not map to this scenario.
- Fix: In export/export-iceberg-table/spec.md, reduce the scenario to its observable step: a writer starts a new data file once the open one passes `write.target-file-size-bytes`. Map it in plan.md § Scenario Coverage to `export_append_follows_the_table_write_properties` as a second `/// Scenario:` line, or fold the step into the append scenario and drop this scenario. Keep "buffers no rows beyond the Parquet row group in progress" only as the architecture Constraints line the delta already holds. Drop `an_unpartitioned_sink_opens_its_data_file_at_the_first_write` and rewrite D5 to say the streaming rule is a constraint that code review checks, not a scenario.

#### [COMPLETENESS_GAP] FINDINGS
- Location: plan.md § Implementation step 2 ("whichever of the seven keys failed")
- Issue: `from_table_properties` reads more than the seven keys. It also parses the four `write.parquet.content-defined-chunking.*` keys (`cdc_enabled`, `cdc_min_chunk_size`, `cdc_max_chunk_size`, `cdc_norm_level`), and it rejects an out-of-range level (`write.parquet.compression-level = 30` with `zstd`) in `parquet_compression`, after the getter has parsed it successfully. Neither error is attributed to a key by the plan's mapping, so the error misses the property and value the interview requires. A table with content-defined chunking enabled also changes the export's files, which the Behavior and the "Data files" bullet do not state.
- Fix: In plan.md § Implementation step 2, list every key `from_table_properties` reads, including the four CDC keys, and map a compression level range error to `write.parquet.compression-level`. Add the CDC keys to the "Data files" bullet and to the docs step, or state that they are followed as Iceberg defines them.

#### [COMPLETENESS_GAP] FINDINGS
- Location: export/export-iceberg-table/spec.md § Background, "Data files" bullet; plan.md § Verification
- Issue: The Behavior bullet "A property the table lacks takes Iceberg's default (`zstd`, 128 MB row groups)" exists only as a Background line. Every table the export creates carries explicit properties, so no planned test exercises the default path. That path is reached only by an append into a table that another engine created without write properties.
- Fix: Add a scenario to export/export-append-iceberg-table/spec.md: an append into a table without `write.parquet.compression-codec` writes `zstd` files. Prove it in E2E by removing the property with the REST `remove-properties` update before an append, as a case row of `export_append_follows_the_table_write_properties`.

#### [REQUIREMENT_CONFLICT] FINDINGS
- Location: export/export-partitioned-iceberg-table/spec.md § Background, buffer bullet
- Issue: "roll over at `write.target-file-size-bytes`. A writer whose rows exceed the budget can therefore give a partition several data files, each smaller than `write.target-file-size-bytes`." The same bullet says a write-out rolls at the target size, so a write-out larger than the target gives a file that reaches or passes it, for example with a target of 4 MiB set on the table. The user's Behavior says "can stay below", not "each smaller".
- Fix: Replace "each smaller than `write.target-file-size-bytes`" with "so a partition's data files can stay below `write.target-file-size-bytes`". Use the same wording in plan.md § Implementation step 5.

#### [BACKGROUND_DRIFT] FINDINGS
- Location: export/export-iceberg-table/spec.md § Background, the three new bullets
- Issue: The Background grows from 10 to 13 bullets, and the new bullets encode behavior and facts that no scenario step depends on: "writes every data file as Parquet, whatever the table's `write.format.default`" (no scenario), "a larger row group size costs writer memory" (no scenario), the 256 MiB rationale, and the § Manifests quote "so the data files of one table may differ in format". The created-table property values sit in the Background while the scenario that checks them only points there. `/speq:plan` Short Background: 3 to 5 bullets that "never encode features, behavior, or requirements".
- Fix: Put the three property values directly in the THEN step of "A table the export creates carries its write properties". Turn the `write.format.default` rule into a step of the append scenario or leave it to `docs/export.md`. Move the memory-cost line and the 256 MiB rationale to the docs step, and move the Iceberg spec quotes to plan.md § Context. Keep one short "Data files" bullet that names the properties the writer follows.

## Design Depth
no objection — axis checked: the architecture delta's BASE matches `git hash-object specs/architecture.md` (241093df), and its three CHANGED blocks (Components, Data Flow, Constraints) differ from the base only in the export lines that match plan.md. No `ADR: yes` direction. The ADR digest holds no export ADR on buffering or file writing that the change contradicts (export-has-no-final-commit-per-writer-commits and export-commit-loop-rechecks-writer-tag are untouched). The sink choosing between a partition buffer and one open writer keeps the partition-split decision in `fanout.rs` and the write mode in the sink. Removing `Split` deepens `PartitionBuffer`.
