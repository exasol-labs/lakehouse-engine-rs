# Plan: change-udf-sdk-upgrade

## Summary

Closes issue [#405](https://github.com/exasol-labs/lakehouse-engine-rs/issues/405) by moving the
`exasol-udf-sdk` and `exasol-udf-macros` pin from 0.26.1 to 0.28.1, which changes `ExaType` and
nothing else. The plan fixes every call site, corrects three specs whose `ExaType` claims become
false, and settles by live measurement whether the scan's fixed microsecond timestamp emit stays
correct now that `ExaType::Timestamp` reports the declared precision.

The implementing commit carries `Closes #405`.

## Design

### Context

A source diff of the two published crates bounds this change precisely. Between 0.26.1 and 0.28.1
the only file that changes in `exasol-udf-sdk/src` is `value.rs`, and the only item that changes in
it is the `ExaType` enum. `exasol-udf-macros` changes its version number and one `trybuild` stderr
fixture. `Value`, `ColumnInfo`, `UdfContext`, `abi.rs` and `connect_back.rs` are byte-identical.

```
 Numeric { precision: Option<u32>, scale: Option<u32> }  ->  Numeric { precision: u32, scale: u32 }
 String  { size: Option<u32> }                           ->  String { size: u32 }
 Char    { size: Option<u32> }                           ->  Char { size: u32 }
 Timestamp                                               ->  Timestamp { precision: u32 }
 TimestampTz, Geometry, HashType,
 IntervalYearToMonth, IntervalDayToSecond                ->  removed
```

The `.so` side of this bump is therefore mechanical. The risk sits on the other side of the ABI. The
SLC is the component that gained the precision, and it is the component that validates an incoming
Arrow IPC block against the declared column. This repo's scan always emits an Arrow
`Timestamp(Microsecond, None)` column, whatever precision the `EMITS` clause declared. Under 0.26.1
the SLC could not compare the two, because the type carried no precision. Under 0.28.1 it can. No
test in this repo has exercised that comparison.

- **Goals** — one pin move, one enum's call sites fixed, three specs made true again, and the
  timestamp-precision question answered from a live measurement rather than deferred.
- **Non-Goals** — no change to the version gate that chooses `TIMESTAMP(6)` against bare
  `TIMESTAMP`, no change to `types/mapping.rs` or its string-keyed `exasol_type_to_arrow`, no change
  to the Iceberg or Delta type contracts, and no change to `ColumnInfo`'s own optional fields, which
  no production code reads.

### Decision

`target_arrow_type` reads `ExaType::Timestamp { precision }` and ignores the precision. The scan
keeps the fixed microsecond Arrow target for every declared `p`.

Task 3 proves that on the catalog-column path, where no CAST sits between the scan and the emit
boundary. On an Exasol 8.x engine the version gate declares a catalog Iceberg `timestamp` column as
bare `TIMESTAMP`. The SLC reports a `precision` this plan records rather than assumes. The scan
still emits `Timestamp(Microsecond, None)`. If that reported `precision` is below 6, the emitted
value is finer than the declaration. The SLC's new precision check is then stressed for real.

Task 3.2 runs the E2E suite against `EXASOL_IMAGE=exasol/docker-db:8.29.13` to observe whether that
block is accepted and truncated to millisecond or rejected. It observes the reported `precision`
once by hand, because every other assertion passes identically whether the SLC reports 3 or 6. A
projected `CAST(x AS TIMESTAMP(p))` cannot serve as that proof:
`vs_expression::snap_timestamp_precision` renders the cast into the scan's own DataFusion SQL, so
DataFusion truncates the value before it reaches the emit boundary and the SLC check is never
exercised. If the measurement contradicts the kept behavior, the plan stops and the spec text and
`target_arrow_type` both need revision.

Two facts make a precision-following target the wrong answer even before the measurement. Arrow's
`TimeUnit` offers four units, so `p` in {1, 2, 4, 5, 7, 8} has no expressible target. And on the
catalog-column bare-`TIMESTAMP` path this plan measures, Exasol itself performs the truncation, so a
second truncation inside the scan would add a rule without adding a guarantee. That second fact is
scoped to that path deliberately. On a projected `CAST(x AS TIMESTAMP(p))` the truncation happens
inside DataFusion via `snap_timestamp_precision`, not in Exasol, and this plan neither verifies that
path nor rests any decision on it.

The CAST-declared `TIMESTAMP(p)` EMITS path is a real but separate SQL usage pattern, and it is the
only route to a declaration ABOVE microsecond resolution. It stays out of scope here and is tracked
as issue [#411](https://github.com/exasol-labs/lakehouse-engine-rs/issues/411).

### Consequences

| Decision | Alternatives Considered | Rationale |
|----------|------------------------|-----------|
| Keep the fixed microsecond emit target and record the measured reason | Coerce the Arrow column to the unit matching the declared `p` | Arrow has no unit for six of the ten precisions, and the engine already truncates. Task 3 turns an inherited limit into a measured decision |
| Delete the absent-payload NUMERIC drift guard | Re-derive `precision` and `scale` from `ColumnInfo::type_name` to keep detecting a defaulted payload | Re-deriving restores the replicated type-string parse that issue #399 deleted. The guard was already unreachable in practice, because a valid Exasol NUMERIC declaration carries both values |
| Delete the five removed-variant arms rather than map them to `Utf8` through a wildcard | Add a wildcard arm so a future variant change compiles | An exhaustive match turns the next upstream change into a compile error here, which is what caught this one |
| Extend `e2e_timestamp_precision_test.rs` rather than add a test binary | A new `e2e_sdk_028_test.rs` | That binary already seeds a microsecond-distinct probe, already owns the precision scenario, and `make test-e2e` already runs it |

### Iceberg and Delta specification compliance

Neither specification governs the Exasol-side output declaration this plan touches. Apache Iceberg
`#### Primitive Types` defines `timestamp` and `timestamptz` as "Timestamp, microsecond precision",
and this plan keeps the microsecond Arrow representation that carries those values end to end. Delta
`Schema Serialization Format` holds the schema in the `metaData` action's required `schemaString`,
which this plan does not read differently. The declared-precision truncation Exasol applies on the
pre-2025 version arm is already recorded as a named Exasol target-type limitation in
`datafusion-scan/type-mapping-timestamp-precision`, and this plan neither widens nor narrows it. No
new deviation arises, so no tracked exception is needed.

## Features

| Feature | Status | Spec |
|---------|--------|------|
| datafusion-scan/type-mapping-timestamp-precision | CHANGED | `specs/_plans/change-udf-sdk-upgrade/datafusion-scan/type-mapping-timestamp-precision/spec.md` |
| datafusion-scan/scan-execution-value-conversion | CHANGED | `specs/_plans/change-udf-sdk-upgrade/datafusion-scan/scan-execution-value-conversion/spec.md` |
| datafusion-scan/scan-execution-partial-agg | CHANGED | `specs/_plans/change-udf-sdk-upgrade/datafusion-scan/scan-execution-partial-agg/spec.md` |

### Recorded text this plan does not amend

`e2e-harness/e2e-harness-scan-correctness` names the `ExaType` variants `Int64`, `Int32`, `Numeric`,
`Double`, `String`, `Date`, `Timestamp` and `Boolean`. All eight survive the bump under the same
names, so that clause stays true. `datafusion-scan/type-mapping-module-structure` and
`vs-adapter/pushdown-col-types-consolidation` name `ExaTypeClass` and `classify_exa_type`, which are
this repo's own string-keyed enum in `types/mapping.rs` and carry no relation to the SDK type.

## Impact

Breaking for operators. The SDK fingerprint is `{version}:{rustc_hash}` and the SLC checks it at UDF
load, so a 0.28.1 SLC rejects a 0.26.1 `.so` and the reverse. Any deployment MUST rebuild the `.so`
with `make cross-udf-build` and reinstall the SLC in the same window. Both steps read the version
from the single `Cargo.toml` pin with no further edit: `Makefile:129`, `bench/run.sh`,
`deploy/scripts/install.sh` and the compile-time `SLC_VERSION` in
`crates/lakehouse-engine/tests/common/e2e_harness.rs` all derive it from that line.

No query result changes. No virtual-schema column declaration changes. No scan-spec wire format
changes.

## Dependencies

| Dependency | Check |
|------------|-------|
| `exasol-udf-sdk` 0.28.1 and `exasol-udf-macros` 0.28.1 on crates.io | Confirmed published |
| SLC release `v0.28.1` with both architecture assets | Confirmed: `lc-rust-0.28.1.tar.gz` and `lc-rust-0.28.1-aarch64.tar.gz` |
| A running local Exasol Docker stack for task 3 | `docker compose up -d`, which `make test-e2e` does NOT start |
| An Exasol 8.x image for task 3.2's bare-`TIMESTAMP` arm | `exasol/docker-db:8.29.13`, already run by `.github/workflows/ci.yml:523` and selected through the `EXASOL_IMAGE` variable `Makefile:3` and `docker-compose.yml:115` both read. Each switch between major versions needs `docker compose down -v` first |

## Implementation Tasks

### 1. Move the pin and take the compiler's census

- [ ] 1.1 Set `exasol-udf-sdk` and `exasol-udf-macros` to `0.28.1` in the root `Cargo.toml`
  `[workspace.dependencies]` and update `Cargo.lock`. Change no member manifest and no other file.
  Confirm `make print-slc-version` prints `0.28.1`.
- [ ] 1.2 Run `cargo check -p lakehouse-engine --all-targets` and then `cargo check -p
  lakehouse-engine --all-targets --features exasol-e2e,unity-e2e,lakekeeper-e2e,azure-e2e,cloud-e2e`.
  The second command is mandatory: a host `cargo test` does not build the feature-gated E2E test
  crates, and past signature changes in this repo broke only at that gate. If the five features do
  not resolve together, run one `cargo check` per feature instead and cover all five. Record the
  complete error list. Task 2 lists the files a full text census of every `ExaType` reference found. Any file the
  compiler names that task 2 does not list MUST be added to this plan before the group proceeds.

### 2. Fix the emit-boundary type surface

- [ ] 2.1 `crates/lakehouse-engine/src/scan/emit.rs` `target_arrow_type`: delete the
  `TimestampTz`, `Geometry`, `HashType`, `IntervalYearToMonth` and `IntervalDayToSecond` arms, match
  the timestamp arm as `ExaType::Timestamp { .. }` keeping `Timestamp(Microsecond, None)`, and leave
  the `Utf8` arm covering exactly `String { .. }`, `Char { .. }` and `Unsupported`. Keep the match
  exhaustive with no wildcard. Replace the arm's three-line comment with one line stating why the
  declared precision is ignored.
- [ ] 2.2 `emit.rs` `decimal_target`: take `precision: u32, scale: u32`. Delete the `Option` zip and
  the absent-payload half of the error message. Keep the `Decimal128` range check and the
  `UdfError::User` that names the column.
- [ ] 2.3 `crates/lakehouse-engine/src/scan/declared_columns_test_support_tests.rs`: drop `Some(...)`
  from `varchar()` and `numeric()`, and wrap the returns of `declared_size`, `declared_precision` and
  `declared_scale` in `Some(...)`, because `ColumnInfo`'s own fields stay `Option<u32>`.
- [ ] 2.4 `crates/lakehouse-engine/tests/scan_fixture/mod.rs`: the same three helper changes for
  `varchar()` and `decimal()`, plus `declared_type_name`, which loses the five removed arms and
  renders `ExaType::Timestamp { precision }` as `TIMESTAMP({precision})`. The twelve other files
  under `tests/` that name `ExaType` use only surviving variants or these helpers and need no edit.
- [ ] 2.5 `crates/lakehouse-engine/src/scan/emit_tests.rs`: update the variant sweep at `:330` for the
  ten-variant enum. Replace `exa_type_timestamp_maps_to_microsecond_target` with a test asserting
  `Timestamp(Microsecond, None)` for every `precision` 0 through 9, and delete its `TimestampTz`
  half. Delete the three absent-payload cases from
  `emit_stream_fails_on_numeric_with_absent_or_out_of_range_payload` and the one from
  `a_drifted_numeric_never_resolves_to_the_string_target`, keep both tests with their out-of-range
  cases, and rename both to drop "absent". Update the `Char { size }` sites at `:347` and `:484`.
- [ ] 2.6 `crates/lakehouse-engine/src/scan/partial_agg_tests.rs`: delete the "absent payload" case
  from `partial_agg_fails_on_numeric_with_absent_or_out_of_range_payload`, keep the two out-of-range
  cases, rename the test to drop "absent", and update the `ExaType::String { size }` site at `:972`.
- [ ] 2.7 `crates/lakehouse-engine/tests/micro_bench.rs`: update `numeric()` at `:69`, the two
  `ExaType::String { size }` sites at `:184` and `:246`, and the two `ExaType::Timestamp` sites at
  `:168` and `:257`.

### 3. Prove the microsecond emit against the live engine

- [ ] 3.1 Run `docker compose down -v` so the stack starts from a clean `exa-data` volume. Bring the
  stack up with `docker compose up -d` and wait for it, then run `make test-e2e`.
  The `make` target does NOT start the stack, and a DB-backed test FAILS rather than skips without
  it. The target rebuilds the `.so` in `rust:1.94-trixie`, and the harness downloads and registers
  SLC 0.28.1 by itself, because `tests/common/e2e_harness.rs` derives `SLC_VERSION` from the SDK's
  own compile-time fingerprint. This run is the `2025.1.16` baseline arm, where the version gate
  declares the catalog timestamp columns `TIMESTAMP(6)` and the emitted microsecond resolution equals
  the declaration. Confirm no fingerprint-mismatch error at UDF load and that
  `e2e_timestamp_precision_test` passes unchanged. WARNING: a fingerprint mismatch or a
  timestamp emit failure here stops the plan. [expert]
- [ ] 3.2 CAUTION: run `docker compose down -v` first. The `exa-data` named volume
  (`docker-compose.yml:128-129`) holds the 2025.1.16 data directory, and an 8.29.13 container started
  over it does not come up. The reset also wipes `minio-data`, so the Spark fixture job and the
  in-process seed re-run on the next `up`. Repeat the same stack bring-up and `make test-e2e` run with
  `EXASOL_IMAGE=exasol/docker-db:8.29.13` exported to BOTH the `docker compose up -d` step and the
  `make` invocation. No harness change is needed: `Makefile:3` and `docker-compose.yml:115` already
  read that variable, and `.github/workflows/ci.yml:523` already runs a CI leg on that exact image.
  On 8.29.13 the version gate declares the seeded catalog Iceberg `timestamp` and `timestamptz`
  columns as bare `TIMESTAMP`, while the scan emits `Timestamp(Microsecond, None)`. No CAST sits
  between the two, so this leg is the plan's only route to a below-declaration measurement at the
  emit boundary. Observe the `precision` the SLC reports for the bare-`TIMESTAMP` output column once,
  by hand, on the 8.29.13 leg, using `ALTER SESSION SET SCRIPT_OUTPUT_ADDRESS` with
  `%udf_debug_level debug` (CLAUDE.md § Live debugging), and record the observed value in
  `decision-log.md` entry `[1]`. If the reported precision is 6 or higher, the declaration is not
  below the emitted resolution, the probe proves nothing about the SLC precision check, and the plan
  STOPS for revision. Extend
  `iceberg_microsecond_timestamps_round_trip_at_the_declared_precision` with one assertion, through
  `isolated_pushdown_statement` (`crates/lakehouse-engine/tests/common/e2e_harness.rs:348`). Assert
  that the generated statement's `EMITS` list declares the probed timestamp column at the arm the
  oracle expects. The millisecond arm declares bare `TIMESTAMP` with no `(p)`, and the microsecond
  arm declares `TIMESTAMP(6)`. Add a fourth field to `ExpectedTimestampPrecision`
  (`crates/lakehouse-engine/tests/common/timestamp_precision.rs`) that carries the `EMITS`
  declaration string for the arm, bare `TIMESTAMP` on `MILLISECOND` and `TIMESTAMP(6)` on
  `MICROSECOND`, and assert against that field. Do NOT reuse `declared_column_type`, which is the
  `SYS.EXA_ALL_COLUMNS` rendering and is never bare. The assertion MUST branch on the oracle's arm,
  not on a fixed string, because the same test binary runs on both engine legs. Record the observed
  outcome: whether the SLC accepts the microsecond block into the bare-`TIMESTAMP` column and Exasol
  truncates each value to millisecond (`retained_at(micros, 3)`, `COUNT(DISTINCT) == 2`), or rejects
  the emit. [expert]
- [ ] 3.3 Record the measured behavior in `decision-log.md` entry `[1]`, naming both engine versions
  exercised (the pinned `2025.1.16` baseline from task 3.1, where the catalog column is declared
  `TIMESTAMP(6)` and the emitted resolution matches the declaration exactly, and `8.29.13` from task
  3.2, where bare `TIMESTAMP` sits below it), the declaration arm each exercised, the observed
  value for each, and the SLC-reported `precision` task 3.2 observed by hand. State whether the
  8.29.13 observation matches what
  `datafusion-scan/type-mapping-timestamp-precision`'s version-gate scenario already records for the
  pre-2025 arm, that "on bare `TIMESTAMP` Exasol truncates them to milliseconds". Record the limit of
  that confirmation plainly: no declaration ABOVE microsecond resolution is exercised, because the
  version gate declares only bare `TIMESTAMP` or `TIMESTAMP(6)` and the CAST route to a higher `p` is
  out of scope (issue #411). STOP and report if the 8.29.13 engine rejects the emitted microsecond
  value into the declared bare-`TIMESTAMP` column, if a value round-trips as anything other than
  its millisecond-truncated form, or if the `precision` task 3.2 observed for the bare-`TIMESTAMP`
  output column is 6 or higher: the spec text and `target_arrow_type` both need revision, which is
  a plan revision rather than an implementation choice. [expert]
- [ ] 3.4 Rewrite the live-engine clause of
  `specs/_plans/change-udf-sdk-upgrade/datafusion-scan/type-mapping-timestamp-precision/spec.md`'s
  scenario "A TIMESTAMP(p) EMITS string maps back to the microsecond Arrow timestamp" so it states
  exactly what tasks 3.1 and 3.2 measured: the engine versions, and the declaration arms actually
  exercised. The delta as authored already scopes that clause to the bare-`TIMESTAMP`-on-8.x arm and
  parks the above-microsecond arm as a limitation tracked by issue #411; correct it to the observed
  result if the measurement differs, and MUST NOT widen the SHALL past an arm the run reached.
  Replace the clause's placeholder for the SLC-reported `precision` with the value task 3.2 observed.
  [expert]
- [ ] 3.5 Re-run the 2025.1.16 leg after task 3.2 extends the scenario, so the extended test is green
  on both engine legs. Reset the stack first per task 3.2's CAUTION: run `docker compose down -v`
  before switching `EXASOL_IMAGE` back to 2025.1.16. [expert]

## Parallelization

| Group | Tasks | Depends on | Knowledge |
|-------|-------|------------|-----------|
| A: SDK pin and the emit-boundary type surface | 1.1-1.2, 2.1-2.7 | — | spec deltas `datafusion-scan/scan-execution-value-conversion`, `datafusion-scan/scan-execution-partial-agg`, `datafusion-scan/type-mapping-timestamp-precision`; `Cargo.toml`, `crates/lakehouse-engine/src/scan/emit.rs`, `crates/lakehouse-engine/src/scan/emit_tests.rs`, `crates/lakehouse-engine/src/scan/partial_agg_tests.rs`, `crates/lakehouse-engine/src/scan/declared_columns_test_support_tests.rs`, `crates/lakehouse-engine/tests/scan_fixture/mod.rs`, `crates/lakehouse-engine/tests/micro_bench.rs` |
| B: live timestamp-precision proof | 3.1-3.5 | A (needs the 0.28.1 `.so` and the matching SLC) | `specs/_plans/change-udf-sdk-upgrade/datafusion-scan/type-mapping-timestamp-precision/spec.md`, `crates/lakehouse-engine/tests/e2e_timestamp_precision_test.rs`, `crates/lakehouse-engine/tests/common/timestamp_precision.rs`, `docker-compose.yml`, the `test-e2e` target in `Makefile`; group B verifies the `TIMESTAMP(p)` scenario group A authored and owns its live-engine clause, which task 3.4 corrects to the measurement |

The two groups run in sequence, not in parallel. Group A authors every spec delta and owns every
source and unit-test file. Group B owns the live-engine files, and the single delta clause it shares
with group A is the live-engine SHALL that group A cannot settle without a measurement, which task
3.4 corrects once the run produces one. Group B carries the only `[expert]` tasks, so keeping it
separate prices the mechanical bump at the standard model.

## Dead Code Removal

| Type | Location | Reason |
|------|----------|--------|
| Match arms | `crates/lakehouse-engine/src/scan/emit.rs` `target_arrow_type` | `TimestampTz`, `Geometry`, `HashType`, `IntervalYearToMonth` and `IntervalDayToSecond` no longer exist |
| Branch | `crates/lakehouse-engine/src/scan/emit.rs` `decimal_target` | The absent-payload arm is unreachable once `precision` and `scale` are `u32` |
| Match arms | `crates/lakehouse-engine/tests/scan_fixture/mod.rs` `declared_type_name` | Same five removed variants |
| Test cases | `crates/lakehouse-engine/src/scan/emit_tests.rs` `emit_stream_fails_on_numeric_with_absent_or_out_of_range_payload`, `a_drifted_numeric_never_resolves_to_the_string_target` | The absent-payload cases are no longer constructible. The tests keep their out-of-range cases |
| Test case | `crates/lakehouse-engine/src/scan/partial_agg_tests.rs` `partial_agg_fails_on_numeric_with_absent_or_out_of_range_payload` | Same reason |
| Test assertion | `crates/lakehouse-engine/src/scan/emit_tests.rs` `exa_type_timestamp_maps_to_microsecond_target` | The `TimestampTz` half asserts a removed variant |

`exasol_type_to_arrow` and its `TIMESTAMP WITH LOCAL TIME ZONE` arm stay. The function is keyed by
type string, not by `ExaType`, and `datafusion-scan/type-mapping-module-structure` records it as a
CLAUDE.md compliance surface.

## Verification

### Scenario Coverage

| Scenario | Test Type | Test Location | Test Name |
|----------|-----------|---------------|-----------|
| A TIMESTAMP(p) EMITS string maps back to the microsecond Arrow timestamp | Unit | `crates/lakehouse-engine/src/scan/emit_tests.rs` | `exa_type_timestamp_maps_to_microsecond_target_at_every_precision` |
| A TIMESTAMP(p) EMITS string maps back to the microsecond Arrow timestamp | Integration (live, both engine legs) | `crates/lakehouse-engine/tests/e2e_timestamp_precision_test.rs` | `iceberg_microsecond_timestamps_round_trip_at_the_declared_precision` |
| Output columns are coerced to the Arrow type the declared EMITS ExaType requires before emit_batch | Unit | `crates/lakehouse-engine/src/scan/emit_tests.rs` | `coerce_batch_to_exa_types_casts_every_declared_variant`, `emit_stream_fails_on_numeric_with_out_of_range_payload` |
| Every emitted partial-aggregate cell matches its declared output column | Unit | `crates/lakehouse-engine/src/scan/partial_agg_tests.rs` | `partial_agg_fails_on_numeric_with_out_of_range_payload` |

The two `target_arrow_type` scenarios take unit tests because the function is pure computation with
no I/O. The live claim in the timestamp scenario, that the engine truncates rather than rejects,
takes the integration test, because only a running engine can answer it. That test carries the claim
only on the `8.29.13` leg, where the declared bare `TIMESTAMP` is coarser than the emitted
microsecond value; on the `2025.1.16` leg the declaration and the emitted resolution match, so that
leg is a regression guard rather than evidence for the claim.

### Manual Testing

| Feature | Command | Expected Output |
|---------|---------|-----------------|
| datafusion-scan/type-mapping-timestamp-precision | `docker compose down -v && docker compose up -d && make test-e2e` | `e2e_timestamp_precision_test` passes on the default `2025.1.16` image. No fingerprint-mismatch error at UDF load |
| datafusion-scan/type-mapping-timestamp-precision (8.x arm) | `docker compose down -v && export EXASOL_IMAGE=exasol/docker-db:8.29.13 && docker compose up -d && make test-e2e` | `e2e_timestamp_precision_test` passes. The emitted microsecond values round-trip millisecond-truncated into the bare-`TIMESTAMP` declaration, with no emit rejection |
| datafusion-scan/scan-execution-value-conversion | `make print-slc-version` | `0.28.1` |
| datafusion-scan/scan-execution-partial-agg | `cargo test -p lakehouse-engine partial_agg` | 0 failures |

### Checklist

| Step | Command | Expected |
|------|---------|----------|
| Build | `make cross-udf-build` | Exit 0 |
| Test (host) | `cargo test` | 0 failures |
| Test (E2E, 2025.x) | `docker compose down -v`, then `docker compose up -d` and `make test-e2e` | 0 failures |
| Test (E2E, 8.x) | `docker compose down -v`, then `EXASOL_IMAGE=exasol/docker-db:8.29.13` exported, `docker compose up -d` and `make test-e2e` | 0 failures |
| Compile gate (E2E crates) | `cargo check -p lakehouse-engine --all-targets --features exasol-e2e,unity-e2e,lakekeeper-e2e,azure-e2e,cloud-e2e` | Exit 0 |
| Lint | `cargo clippy --all-targets` | 0 errors and 0 warnings |
| Format | `cargo fmt` | No changes |
