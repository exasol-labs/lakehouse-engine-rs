# Plan: remove-emit-exa-types

## Summary

Closes issue [#399](https://github.com/exasol-labs/lakehouse-engine-rs/issues/399) by bumping
`exasol-udf-sdk` to 0.26.0 and deleting `CommonScanSpec::emit_exa_types`, so the scan reads its
declared output types from `UdfContext::output_column` instead of a duplicate list shipped through
the scan-spec JSON. The same bump makes the SDK validate every `Value` row against its declared
column, which exposes four pre-existing partial-aggregate type mismatches that this plan must fix
in the same change to avoid regressing working queries.

Issue #399's Scope section states that `scan/partial_agg.rs` is "Not in scope", on the grounds that
`language-container-rs#89` "doesn't change anything there". That note predates the 0.26.0 release,
which bundled `language-container-rs` PR #105's row-validation change into the same bump, so it no
longer holds. The user decided explicitly to carry the resulting partial-aggregate fix under #399
rather than track it as a separate issue.

The implementing commit carries `Closes #399`, and no second issue trailer.

## Design

### Context

`LAKEHOUSE_SCAN` is declared `EMITS (...)`, the dynamic-output form. The call-site `EMITS` clause
the adapter generates is the only declaration of that call's output schema. The adapter built a
second copy of the same decision from the same `proj_types` vector and shipped it as
`CommonScanSpec::emit_exa_types`, which the scan trusted without checking. Two transmissions of one
decision, with nothing enforcing agreement: textbook back-door information leakage.

`exasol-udf-sdk` 0.26.0 closes the gap that forced the duplicate. `UdfContext::output_column(idx)`
returns the `ColumnInfo` the database reported for output column `idx`, carrying the `ExaType` the
column's values travel in. Upstream `language-container-rs` PR #105 wires these accessors from the
call-site `EMITS` list. Its live `column-meta` integration fixture covers one shape only: the same
entry point following a re-registered STATIC `EMITS` column list carried in the script DDL. It does
not cover the dynamic call-site form `LAKEHOUSE_SCAN` uses, which task 1.6 proves separately against
the local Exasol Docker container. The blocker issue `language-container-rs#89` is closed.

The same release adds row validation. `check_output_row` and `column_accepts`
(`exa-udf-runtime`'s `rowset.rs`) now reject a mismatched cell instead of coercing it. Reading the
matrix directly: a `Double` column accepts only `Value::Double`, an `Int32`/`Int64` column rejects
`Value::Numeric`, a `Numeric` column rejects `Value::Double`, and no numeric column accepts
`Value::String`. Four partial-aggregate paths in this repo violate those rules today and are
silently coerced. They become query failures the moment the pin moves, so the bump cannot land
without them fixed.

- **Goals** — one owner for the scan's declared output type, on both emit paths. A scan spec that
  carries no type declaration. No behavior change for any query that works today.
- **Non-Goals** — no change to the Iceberg-to-Exasol or Delta-to-Exasol mapping rules in
  `types/mapping.rs`. No change to the adapter's declared partial-aggregate types. No removal of
  `exasol_type_to_arrow`. No new seed fixture. No change to `logical_schema` or `name_mapping`.

### Decision

#### Architecture

The declared output column becomes the single authority for the emitted type, read from the
context at the emit boundary, on both paths.

```
adapter: proj_types ──▶ EMITS (...) clause  ──▶ Exasol parses it
                                                     │
                                   (declared output schema for this call)
                                                     ▼
                        scan UDF: UdfContext::output_column(i) ──▶ ExaType
                                                     │
                        ┌────────────────────────────┴──────────────────┐
                        ▼                                               ▼
        Arrow path: coerce_batch_to_exa_types                Value path: coerce the
        then ctx.emit_batch (raw scan, join scan)            partial-aggregate columns,
                                                             then arrow_value_at, ctx.emit
```

The removed edge is the second arrow the adapter used to draw: `proj_types` into
`CommonScanSpec::emit_exa_types` into the scan.

#### Patterns

| Pattern | Where | Why |
|---------|-------|-----|
| Read the authority, do not replicate its decision | `scan/emit.rs` `target_arrow_type` | `ExaType` reports the DECIMAL bin Exasol chose, so the replicated precision binning (scale-0 `p` at most 9 to Int32, at most 18 to Int64) is deleted rather than kept in sync |
| One coercion rule, two emit paths | `scan/emit.rs`, `scan/partial_agg.rs` | The `Value` path's four mismatches and the Arrow path's coercion have one cause and one fix. Both paths share `target_arrow_type` and agree on every declaration, a payload-less or out-of-range `Numeric` included, which both fail rather than route to `Utf8` |
| Fail loudly on a missing declaration | `scan/emit.rs` | An absent or wrong-arity output-column list is drift, and drift must name itself rather than degrade |
| Pass the value, do not round-trip it through a struct | `joins/sql_builders.rs` | `build_qualified_single_table_fallback_sql` read `proj_types` back off the spec it was just written into |

### Consequences

| Decision | Alternatives Considered | Rationale |
|----------|------------------------|-----------|
| Map `ExaType` to Arrow directly | Keep `exasol_type_to_arrow(&ColumnInfo.type_name)`, parsing the string the SDK also reports | The string path keeps the replicated DECIMAL binning that can disagree with the engine. `ExaType` is the bin the engine chose |
| No fallback when the declared list is absent or short | Fall back to view-type normalization, as the pre-#399 empty-list path did | The fallback existed for scan specs written before the field. With the field gone, an absent declaration is drift, and a silent fallback would hide exactly what acceptance criterion 4 asks to prove |
| Fix the partial-aggregate mismatches by reusing the ctx-driven coercion | Cast per aggregate kind in `partial_select_items` (`SUM(CAST(arg AS DOUBLE))` and so on) | The per-kind cast fixes the three `DOUBLE PRECISION` columns but not `MIN`/`MAX` over `decimal(p,0)` or the wide-decimal `SUM`, because the scan does not know those declared types. Reading the declared column covers every kind at once |
| Coerce only the partial-aggregate columns, never the group keys | Coerce the whole partial batch | An Arrow `cast(Date32 → Utf8)` formats differently from `value_to_gk_string`, which would change a group's merge identity |
| Retain `exasol_type_to_arrow` as `pub` with no production call site | Delete it as dead code | `datafusion-scan/type-mapping-module-structure` already records the same status for its inverse `arrow_to_exasol_type`: it is a CLAUDE.md-documented compliance surface, and removing a public API item is a scope ADD |
| Keep the name `coerce_batch_to_exa_types` | Rename it to match the new input | Two recorded scenarios (`datafusion-scan/type-relaxation`, `datafusion-scan/nested-json-rendering`) name it and stay true verbatim. It still coerces to declared Exasol types |

### Iceberg and Delta specification compliance

Neither specification governs the Exasol-side output declaration this plan moves. Apache Iceberg
`#### Schemas and Data Types` states "A table's **schema** is a list of named columns. Data types
are primitive, nested, or semi-structured", and its `#### Primitive Types` table is the
authoritative type list. Delta `Schema Serialization Format` holds the schema in the `metaData`
action's required `schemaString`. The Iceberg-to-Exasol and Delta-to-Exasol mapping rules stay in
`crates/lakehouse-engine/src/types/mapping.rs`, applied at `createVirtualSchema` and at `EMITS`
rendering, and this plan changes none of them. No new deviation arises, so no tracked exception is
needed.

## Features

| Feature | Status | Spec |
|---------|--------|------|
| datafusion-scan/scan-execution-value-conversion | CHANGED | `specs/_plans/remove-emit-exa-types/datafusion-scan/scan-execution-value-conversion/spec.md` |
| datafusion-scan/scan-execution-partial-agg | CHANGED | `specs/_plans/remove-emit-exa-types/datafusion-scan/scan-execution-partial-agg/spec.md` |
| datafusion-scan/scan-execution-spec-reconstitution | CHANGED | `specs/_plans/remove-emit-exa-types/datafusion-scan/scan-execution-spec-reconstitution/spec.md` |
| datafusion-scan/type-mapping-timestamp-precision | CHANGED | `specs/_plans/remove-emit-exa-types/datafusion-scan/type-mapping-timestamp-precision/spec.md` |
| e2e-harness/e2e-harness-scan-correctness | CHANGED | `specs/_plans/remove-emit-exa-types/e2e-harness/e2e-harness-scan-correctness/spec.md` |

### Recorded text this plan does not amend

Four other features mention `emit_exa_types`, none in a normative clause this change breaks, and
none carries a scenario this change touches. `speq plan validate` requires every delta file to
carry a scenario, so none of the four gets a delta. They are named here rather than left silent.

| Feature | Mention | Handling |
|---------|---------|----------|
| `vs-adapter/create-virtual-schema-declaration-details` | Background bullet naming `ScanSpec::emit_exa_types` as a second carrier of the scan's EMITS types | Becomes stale. Its scenario clause "the scan UDF entry point MUST NOT read `database_version()`" stays true, and `datafusion-scan/scan-execution-value-conversion`'s delta records the new mechanism |
| `vs-adapter/pushdown-planning-char-type-declaration` | Background bullet describing the CHAR route through `exasol_type_to_arrow` returning `None` | Becomes stale. Every CHAR scenario stays true: `ExaType::Char` routes to the same `Utf8` path |
| `sql-comprehension/vs-expression-translator-float-div` | A dated live `EXPLAIN VIRTUAL` capture quoting `"emit_exa_types":["DOUBLE PRECISION"]`, in one Background bullet and one scenario clause | Left as recorded. It is an accurate record of a measurement, and the clause's normative point, a `Float64` column matching a `DOUBLE PRECISION` declaration on the zero-copy fast path, is unchanged |
| `vs-adapter/pushdown-planning-capability-extensions` | A dated live capture quoting `"emit_exa_types":["TIMESTAMP(3)", …]` in the now-family withdrawal bullet | Left as recorded, same reason. The withdrawal rests on `CommonScanSpec` carrying no temporal field, which removing a type field does not change |

## Impact

Operators must install SLC v0.26.0 alongside this engine release. The SDK fingerprint
(`{exasol-udf-sdk version}:{rustc_hash}`) is checked at UDF load, so a v0.24.0 SLC rejects the new
`.so`. `make install-slc` and `deploy/scripts/install.sh` both derive the SLC version from the
workspace `exasol-udf-sdk` pin, so both follow the bump with no edit. `bench/run.sh` and
`deploy/scripts/secrets.sh` carry a hardcoded `0.21.0` that is already two releases stale and is
corrected here.

The scan-spec wire format changes: a row-scan common blob no longer carries an `emit_exa_types`
key. The `.so`, the SLC, and the adapter deploy together and the scan script DDL is recreated per
deployment, so no in-flight spec crosses a version boundary. No user-facing SQL behavior changes.

`AVG`, `STDDEV`, `STDDEV_POP`, `VARIANCE` and `VAR_POP` over an integer or decimal column, `SUM`
over a decimal with precision at least 27, and `MIN`/`MAX` over a `decimal(p,0)` with `p` at most
18 keep returning their current values. Without the partial-aggregate fix in this plan the SDK bump
alone would fail each of them with an `output column … is … but the value is …` error.

Two of the four mismatches carry unit coverage only. No seeded table and no bench dataset holds a
scale-0 decimal column or a decimal with precision at least 27, and § Design § Non-Goals keeps "No
new seed fixture", so the wide-decimal `SUM` and the `MIN`/`MAX` over `decimal(p,0)` are proven by
`partial_agg_tests.rs` and reach the engine untested at the integration and end-to-end level. The
`AVG`/`STDDEV`-over-integer mismatch is the one task 4.3 exercises against the running container.

## Dependencies

| Dependency | State |
|------------|-------|
| `language-container-rs#89` (column metadata on `UdfContext`) | Closed 2026-09-14 by PR #105 |
| `exasol-udf-sdk` / `exasol-udf-macros` 0.26.0 | Published |
| `cargo-exasol-udf` 0.26.0 | Published; `Makefile` installs `=$(SLC_VERSION)` |
| SLC release `v0.26.0` | Published 2026-09-14; verify both arch assets with `gh release view v0.26.0 --repo exasol-labs/language-container-rs` |

## Load-bearing assumptions

| Assumption | Evidence | How it is checked |
|------------|----------|-------------------|
| The SLC populates output-column metadata for a `EMITS (...)` dynamic-output SCALAR script, per call, from the call-site clause | Partial. Upstream PR #105 states the accessors "return a borrow out of slices the bridge already holds", and its live `column-meta` fixture covers only a STATIC `EMITS` list in the script DDL, re-registered against a second static list. The dynamic call-site form is not covered upstream | Task 1.6 proves the dynamic form on the running Docker Exasol before any removal lands. If `output_column_count()` reports 0 for `LAKEHOUSE_SCAN`, the whole design fails and the plan stops there |
| Exasol reports the `ExaType` bin it actually uses for a declared `DECIMAL(p,s)`, so reading it replaces the repo's binning heuristic exactly | `ColumnInfo.typ` is documented as "Mapped SQL type; the wire block a value of this column travels in", which is the same property the repo's heuristic approximates | The E2E scenario runs a `DECIMAL(20,0)`, a `DECIMAL(p,s)` and a `CAST(... AS DECIMAL(5,0))` column through the strict IPC feed |
| No behavior outside the emit boundary reads the removed field | Census of the whole repo found four production reads: `raw_scan.rs:62`, `join_scan.rs:57`, and two adapter read-backs in `joins/sql_builders.rs` | Compilation after task 3.1 |
| A column Exasol declares NUMERIC always reports both a `precision` and a `scale`, within what `Decimal128` represents | The protobuf `column_definition` (`exa-proto`'s `zmqcontainer.proto:33-36`) carries one `optional precision` and `optional scale` pair shared by every column type, and `column_from_pb` (`exa-zmq-protocol`'s `meta.rs:69-72`) forwards both into `ExaType::Numeric` unchecked. The `Option` is that shared-field artifact. Exasol's own DECIMAL domain is `1 ≤ p ≤ 36` and `0 ≤ s ≤ p` (CLAUDE.md § Data types), inside `Decimal128`'s maximum precision of 38 | Not asserted as a run-time invariant. Decision [9] makes both emit paths fail the call naming the column when it does not hold, so a violation surfaces as a named error rather than a wrong value |

This plan adds no spec coverage for the SDK-to-SLC version lockstep. It exercises the recorded
discipline without altering it, exactly as the recorded `add-test-context-migration` plan did.

## Migration

| Current | New |
|---------|-----|
| `exasol-udf-sdk` / `exasol-udf-macros` `0.24.0` in `[workspace.dependencies]` | `0.26.0` |
| Registered Rust SLC v0.24.0 | v0.26.0, installed by the same `make install-slc` |
| Common blob carries `"emit_exa_types":[…]` on row-scan specs | Key absent; 11 golden dispatch fixtures regenerated |
| `emit_stream(ctx, stream, secrets, exa_types, timers)` | `emit_stream(ctx, stream, secrets, timers)` |
| `build_qualified_single_table_fallback_sql(request, pushdown_req, fan_out_spec, shards, …)` | Same plus an explicit `proj_types: &[String]` |
| `join_fan_out_scan_spec(primary, projection, filter, emit_exa_types, join, inputs)` | Same without `emit_exa_types` |

## Implementation Tasks

### 1. SDK and SLC version lockstep

- [ ] 1.1 Verify `gh release view v0.26.0 --repo exasol-labs/language-container-rs` lists both the
  `lc-rust-0.26.0.tar.gz` and `lc-rust-0.26.0-aarch64.tar.gz` assets, and that
  `cargo info cargo-exasol-udf` shows 0.26.0. Record the result. If an asset is missing, stop.
- [ ] 1.2 Set `exasol-udf-sdk` and `exasol-udf-macros` to `0.26.0` in the root `Cargo.toml`
  `[workspace.dependencies]` and update `Cargo.lock`. Change no per-crate manifest.
- [ ] 1.3 Confirm `Makefile:129` `SLC_VERSION` and `deploy/scripts/install.sh`'s
  `resolve_engine_pinned_slc_version` both resolve 0.26.0 from that pin, and that
  `deploy/scripts/tests/install.test.sh` still passes (its `0.21.0` values are synthetic fixture
  data, not a pin).
- [ ] 1.4 Replace the hardcoded `0.21.0` default in `bench/run.sh` with the workspace
  `exasol-udf-sdk` pin, keeping `BENCH_SLC_VERSION` as the override, and set
  `deploy/scripts/secrets.sh`'s `BENCH_SLC_VERSION` to `0.26.0`.
- [ ] 1.5 Add the three defaulted accessors (`input_column`, `output_column_count`,
  `output_column`) to all three hand-written `UdfContext` impls in this repo. They are
  trait-defaulted, so a missing implementation compiles and then returns `Unimplemented` at run
  time, which decision [4]'s no-fallback rule turns into a test failure rather than a degradation.
  - `crates/lakehouse-engine/tests/scan_fixture/mod.rs:81` `BatchCapturingCtx`: a delegate over an
    inner `TestContext`, so each accessor forwards to `self.inner`.
  - `crates/lakehouse-engine/src/scan/test_support_tests.rs:82` `SinkCtx`: hand-rolled, not a
    `TestContext` delegate, so it needs its own accessor bodies. `raw_scan_tests.rs:1195` only
    imports it.
  - `crates/lakehouse-engine/src/scan/emit_tests.rs:66` `CapturingCtx`: hand-rolled, not a
    `TestContext` delegate, and the impl that drives `emit_stream` at `emit_tests.rs:150` and
    `:606`. It needs its own `output_column_count` and `output_column` bodies returning the
    declared columns the test sets, so it must gain a field carrying them.
- [ ] 1.6 Runs FIRST inside group B, not in group A. A ONE-TIME gate on the architectural bet, not
  a permanent regression test. Decision [4] removes every fallback for an absent or wrong-arity
  `output_column` list, so this task exists to prove, before that removal lands, that the SLC
  populates the accessors for the dynamic call-site `EMITS` form.
  Build the `.so` against 0.26.0, install SLC v0.26.0, and run ONE SINGLE-LEG scan query through
  the virtual schema with a temporary `udf_log!` of `output_column_count()` and each
  `output_column(i).type_name`, captured through
  `ALTER SESSION SET SCRIPT_OUTPUT_ADDRESS = '<host>:<port>'` against a `nc -l` listener the
  container can reach. Use a single-leg query, because CLAUDE.md records that the redirect
  destabilizes multi-leg join queries. Confirm the reported list equals the generated `EMITS (...)`
  clause item for item, record the captured list in the task result, then delete the temporary
  logging.
  No permanent test replaces it, by deliberate choice: a recurring assertion that the runtime list
  equals the declared list would primarily re-test the SDK and SLC's own contract, since
  `language-container-rs` populates both sides from the one `EMITS` clause the engine parsed,
  rather than testing this repo's logic.
  WARNING: a failure here stops the plan, because every later task depends on the accessors being
  populated for a dynamic-EMITS SCALAR script. [expert]

### 2. The declared output column becomes the emit authority

- [ ] 2.1 Add a `scan_fixture` helper that builds `Vec<ColumnInfo>` from explicit `ExaType` values,
  and a `BatchCapturingCtx` constructor that sets them via `TestContext::with_output_columns`.
- [ ] 2.2 Rewrite `target_arrow_type` to map an `ExaType` to its Arrow target, deleting the
  type-string parse and the replicated DECIMAL binning. Cover every `ExaType` variant. [expert]
- [ ] 2.3 Change `coerce_batch_to_exa_types` to take the declared column metadata, keeping its name
  and its zero-copy fast path. [expert]
- [ ] 2.4 Change `emit_stream` and `emit_one_batch` to resolve the declared columns from
  `ctx` once, before the batch loop, and to fail with a named error when
  `output_column_count()` disagrees with the batch column count or when `output_column(i)` errors
  for a carried column. Drop the `exa_types` parameter. [expert]
- [ ] 2.5 Drop the `&spec.common.emit_exa_types` argument at `scan/raw_scan.rs:62` and
  `scan/join_scan.rs:57`.
- [ ] 2.6 Coerce the partial-aggregate columns of the partial-agg batch to their declared `ExaType`
  before `arrow_value_at`, on both the single-group (`run_partial_aggregate`) and grouped
  (`run_grouped_partial_aggregate`) paths, skipping the leading group-key columns. Leave
  `value_to_gk_string` and `emit_null_partial_row` unchanged. [expert]
- [ ] 2.7 Rework the `emit_tests.rs` coercion tests to sweep `ExaType` variants instead of type
  strings, including the `Int32`/`Int64`/`Numeric` bins, `Char`, and the arity and missing-column
  error paths. Add one test asserting that a `Numeric` whose `precision` or `scale` is `None`, and
  one asserting that a `Numeric` whose `precision` or `scale` is outside what `Decimal128`
  represents, each fail the call naming the offending column. Assert that neither substitutes
  `Utf8`.
- [ ] 2.8 Add partial-aggregate unit tests for the four mismatches: `AVG` over `Int64` and `AVG`
  over `Decimal128` into a `DOUBLE PRECISION` column, `SUM` over a `Decimal128` wide enough to
  exceed `DECIMAL(36,s)`, `MIN`/`MAX` over `Decimal128(p,0)` with `p` at most 18, and a
  `Decimal128` value into the `DOUBLE PRECISION` column `NESTED_AGGREGATE_PLAN_TYPE`
  (`adapter/pushdown/scalar_over_agg.rs:28`) declares for an aggregate reached only nested inside a
  scalar. Add one further test on this path asserting that a partial-aggregate column declared
  `Numeric` with an absent or out-of-range `precision` or `scale` fails the call naming that column,
  matching the Arrow-path behavior task 2.7 asserts. Neither path substitutes `Utf8`. [expert]
- [ ] 2.9 Update `tests/micro_bench.rs`, which consumes `coerce_batch_to_exa_types` as a public API
  and is not feature-gated.
- [ ] 2.10 Set output columns on the 22 `TestContext` constructions under
  `crates/lakehouse-engine/tests/` that drive `emit_stream` through `run_raw_scan_with_session`,
  `run_join_scan_with_session`, `run_scan_one` or `run_scan`.

### 3. Remove the field and its adapter plumbing

- [ ] 3.1 Delete `CommonScanSpec::emit_exa_types` and its `Default` initializer in
  `scan/spec.rs`. Replace the `emit_exa_types_round_trips_and_defaults_to_empty` test in
  `scan/spec_tests.rs` with `common_blob_carries_no_emit_type_key`, which asserts the serialized
  common blob carries no such key for a row-scan spec.
- [ ] 3.2 Delete the field from the five production construction sites:
  `adapter/pushdown/mod.rs:369`, `adapter/pushdown/mod.rs:796`, `adapter/pushdown/support.rs:406`,
  `joins/sql_builders.rs:597`, `joins/sql_builders.rs:1113`, and remove the now-unused
  `join_fan_out_scan_spec` parameter and its two call sites.
- [ ] 3.3 Give `build_qualified_single_table_fallback_sql` an explicit `proj_types: &[String]`
  parameter and drop both read-backs at `joins/sql_builders.rs:1026` and `:1040`. Pass
  `fb_proj_types` from `qualified_single_table_fallback_pushdown`. [expert]
- [ ] 3.4 Update the adapter unit tests that construct or read the field: `pushdown_tests.rs`,
  `support_tests.rs`, `grouped_agg_tests.rs`, `test_support_tests.rs`, `topn_tests.rs`,
  `joins/sql_builders_tests.rs`, and the `tests/` helpers in `scan_batch_loop.rs`,
  `scan_telemetry.rs`, `scan_two_arg.rs`, `scan_plan_shape.rs`.
- [ ] 3.5 Regenerate the 11 golden dispatch fixtures under
  `adapter/pushdown/testdata/dispatch_golden/` and the four inline expected-SQL literals in
  `joins/sql_builders_tests.rs` and `pushdown_tests.rs`.

### 4. End-to-end proof

- [ ] 4.1 Add `crates/lakehouse-engine/tests/e2e_emit_declaration_test.rs` covering the
  value-correctness scenario over `typed_distinct_probe` and `complex_probe`, including the
  projected `CAST(<col> AS DECIMAL(5,0))` that reaches the `ExaType::Int32` bin. Assert the seeded
  values and the absence of the `emit_exa_types` key. Do NOT build a permanent runtime-versus-
  declared type-list comparison here; task 1.6 is the one-time proof of that agreement.
- [ ] 4.2 Replace the `"emit_exa_types":["DOUBLE PRECISION"]` assertion at
  `tests/e2e_scan_test.rs:4023` with the equivalent assertion on the generated `EMITS (...)`
  clause, and delete the now-vacuous assertion at `tests/e2e_scan_test.rs:951`.
- [ ] 4.3 Add an end-to-end partial-aggregate case running `AVG` and `STDDEV` over the seeded
  `long` column and over `typed_distinct_probe`'s `decimal(9,2)` and `decimal(20,4)` columns, so the
  `AvgSum`/`StatSum`/`StatSumSq` mismatch is exercised on a non-`double` column for the first time.
  Claim no more than that. The wide-decimal `SUM` and the `MIN`/`MAX` over a `decimal(p,0)` are NOT
  reachable from these fixtures, and § Design § Non-Goals keeps "No new seed fixture", so those two
  stay on unit coverage.

## Parallelization

| Group | Tasks | Depends on | Knowledge |
|-------|-------|------------|-----------|
| A: SDK and SLC lockstep | 1.1-1.5 | — | `Cargo.toml`, `Cargo.lock`, `Makefile`, `bench/run.sh`, `deploy/scripts/secrets.sh`, `crates/lakehouse-engine/tests/scan_fixture/mod.rs`, `crates/lakehouse-engine/src/scan/test_support_tests.rs`, `crates/lakehouse-engine/src/scan/emit_tests.rs` |
| B: Declared column as emit authority | 1.6, 2.1-2.10 | A (needs the 0.26.0 accessors) | spec deltas `datafusion-scan/scan-execution-value-conversion`, `datafusion-scan/scan-execution-partial-agg`, `datafusion-scan/type-mapping-timestamp-precision`; `crates/lakehouse-engine/src/scan/emit.rs`, `emit_tests.rs`, `raw_scan.rs`, `join_scan.rs`, `partial_agg.rs`, `partial_agg_tests.rs`, `crates/lakehouse-engine/tests/micro_bench.rs`, `crates/lakehouse-engine/tests/scan_*.rs` |
| C: Field removal and adapter plumbing | 3.1-3.5 | B (the scan must stop reading the field first) | spec delta `datafusion-scan/scan-execution-spec-reconstitution`; `crates/lakehouse-engine/src/scan/spec.rs`, `spec_tests.rs`, `crates/lakehouse-engine/src/adapter/pushdown/**`, `adapter/pushdown/testdata/dispatch_golden/**` |
| D: End-to-end proof | 4.1-4.3 | C (asserts the post-removal payload) | spec delta `e2e-harness/e2e-harness-scan-correctness`; `crates/lakehouse-engine/tests/e2e_emit_declaration_test.rs`, `crates/lakehouse-engine/tests/e2e_scan_test.rs`, `crates/lakehouse-engine/tests/common/seed.rs` |

The four groups run in sequence, not in parallel. Every pair shares either a compile-time contract
or a golden payload, so splitting them for concurrency would put two agents on one file.

## Dead Code Removal

| Type | Location | Reason |
|------|----------|--------|
| Field | `CommonScanSpec::emit_exa_types`, `crates/lakehouse-engine/src/scan/spec.rs:1148` | The call-site `EMITS` clause is the one declaration; the scan reads it through `UdfContext::output_column` |
| Parameter | `exa_types` on `emit_stream` and `emit_one_batch`, `scan/emit.rs` | Replaced by the context read |
| Parameter | `emit_exa_types` on `join_fan_out_scan_spec`, `joins/sql_builders.rs:582` | The field it fed is gone |
| Test | `emit_exa_types_round_trips_and_defaults_to_empty`, `scan/spec_tests.rs:183` | Tests a removed field |
| Assertion | `tests/e2e_scan_test.rs:951` | Asserts the aggregate common blob omits a key that no longer exists |
| Branch | The `exa_types` empty and short-list fallback in `coerce_batch_to_exa_types` | Existed for scan specs written before the field |
| Retained, NOT removed | `types::mapping::exasol_type_to_arrow` | Loses its production call site but stays `pub`: a CLAUDE.md-documented compliance surface and the recorded inverse of `arrow_to_exasol_type` |

## Verification

Issue #399's acceptance criterion 4, "confirm no drift between the declared `EMITS` clause and what
`ctx.output_column` reports at runtime, via E2E", is satisfied by task 1.6 alone. Task 1.6 runs a
real scan query against the local Exasol Docker container and compares the two lists item for item.
No permanent E2E scenario asserts that comparison, deliberately: the two lists are populated by
`language-container-rs` from the one `EMITS` clause the engine parsed, so a recurring assertion
would re-test the SDK and SLC's own contract rather than this repo's logic. A future reader looking
for that missing scenario should read this paragraph, not assume an omission.

Coverage of the four partial-aggregate mismatches splits. The `AvgSum`/`StatSum`/`StatSumSq`
mismatch carries unit and integration coverage. The wide-decimal `SUM`, the `MIN`/`MAX` over a
`decimal(p,0)`, and the nested-aggregate `DOUBLE PRECISION` declaration carry unit coverage only,
because no seed fixture or bench dataset reaches them and § Design § Non-Goals keeps "No new seed
fixture".

### Scenario Coverage

| Scenario | Test Type | Test Location | Test Name |
|----------|-----------|---------------|-----------|
| Output columns are coerced to the Arrow type the declared EMITS ExaType requires before emit_batch | Unit | `crates/lakehouse-engine/src/scan/emit_tests.rs` | `coerce_maps_every_exa_type_variant_to_its_arrow_target` |
| Output columns are coerced to the Arrow type the declared EMITS ExaType requires before emit_batch | Unit | `crates/lakehouse-engine/src/scan/emit_tests.rs` | `emit_stream_fails_when_declared_column_count_disagrees` |
| Output columns are coerced to the Arrow type the declared EMITS ExaType requires before emit_batch | Unit | `crates/lakehouse-engine/src/scan/emit_tests.rs` | `emit_stream_fails_on_numeric_with_absent_or_out_of_range_payload` |
| Every emitted partial-aggregate cell matches its declared output column | Unit | `crates/lakehouse-engine/src/scan/partial_agg_tests.rs` | `partial_agg_fails_on_numeric_with_absent_or_out_of_range_payload` |
| Output columns are coerced to the Arrow type the declared EMITS ExaType requires before emit_batch | Integration | `crates/lakehouse-engine/tests/scan_batch_loop.rs` | `raw_scan_coerces_every_column_to_its_declared_output_type` |
| Every emitted partial-aggregate cell matches its declared output column | Unit | `crates/lakehouse-engine/src/scan/partial_agg_tests.rs` | `partial_cells_conform_to_declared_output_columns` |
| Every emitted partial-aggregate cell matches its declared output column | Integration | `crates/lakehouse-engine/tests/e2e_emit_declaration_test.rs` | `partial_aggregates_over_integer_and_decimal_columns_return_values`, which reaches the `AvgSum`/`StatSum`/`StatSumSq` mismatch only |
| A TIMESTAMP(p) EMITS string maps back to the microsecond Arrow timestamp | Unit | `crates/lakehouse-engine/src/scan/emit_tests.rs` | `exa_type_timestamp_maps_to_microsecond_target` |
| The scan returns correct values across the type mix with no spec-carried emit types | Integration | `crates/lakehouse-engine/tests/e2e_emit_declaration_test.rs` | `scan_returns_correct_values_across_the_type_mix` |
| Consolidating the shard-invariant fields preserves the two-argument wire | Unit | `crates/lakehouse-engine/src/adapter/pushdown/dispatch_golden_tests.rs` | the 11 regenerated golden dispatch fixtures |
| Consolidating the shard-invariant fields preserves the two-argument wire | Unit | `crates/lakehouse-engine/src/scan/spec_tests.rs` | `common_blob_carries_no_emit_type_key` |

### Manual Testing

| Feature | Command | Expected Output |
|---------|---------|-----------------|
| SDK and SLC lockstep | `gh release view v0.26.0 --repo exasol-labs/language-container-rs` | Lists `lc-rust-0.26.0.tar.gz` and `lc-rust-0.26.0-aarch64.tar.gz` |
| SDK and SLC lockstep | `make -n install-slc \| grep lc-rust` | Names `lc-rust-0.26.0.tar.gz`, derived from the `Cargo.toml` pin |
| Declared column as emit authority | `docker compose up -d` then `make test-e2e` | 0 failures; a DB-backed test FAILS rather than skips without the stack |
| Field removal | `CAPTURE_SQL='SELECT * FROM <vs>.TYPED_DISTINCT_PROBE' cargo test --features exasol-e2e --test e2e_capture_pushdown -- --nocapture` | The captured common blob carries no `emit_exa_types` key and the statement carries an `EMITS (...)` clause |
| Partial-aggregate conformance | `SELECT AVG(L_QUANTITY), STDDEV(L_QUANTITY), MIN(O_TOTALPRICE) FROM <vs>.LINEITEM` through the virtual schema | Returns values; no `output column … is … but the value is …` error |

### Checklist

| Step | Command | Expected |
|------|---------|----------|
| Build | `make cross-udf-build` | Exit 0; `cargo exasol-udf validate` passes against SDK 0.26.0 |
| Test | `cargo test` | 0 failures |
| Test (E2E) | `docker compose up -d && make test-e2e` | 0 failures |
| Lint | `cargo clippy --all-targets` | 0 errors and 0 warnings |
| Format | `cargo fmt --check` | No changes |
| Specs | `speq feature validate` | Pass |
