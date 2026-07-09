# Verification Report: change-name-mapping-fallback

**Generated:** 2026-07-08

## Verdict

| Result | Details |
|--------|---------|
| **PASS** | Iceberg `schema.name-mapping.default` is now honored as a resolution step for data files with no embedded field-id, inserted between the embedded-field-id match and the existing physical-name fallback (unchanged for the no-mapping/uncovered case). All 8 implementation/test tasks complete, code review found one trivial formatting issue (fixed), full workspace suite green, UDF `.so` builds clean. |

| Check | Status |
|-------|--------|
| Build | ✓ |
| Tests | ✓ |
| Lint | ✓ |
| Format | ✓ |
| Scenario Coverage | ✓ |
| Manual Tests | ✓ |

## Test Evidence

### Coverage

| Type | Coverage % |
|------|------------|
| Unit | Not measured (no coverage tool configured in this repo) |
| Integration | Not measured (no coverage tool configured in this repo) |

### Test Results

| Type | Run | Passed | Ignored |
|------|-----|--------|---------|
| Unit + Integration (full workspace, `cargo test --workspace`) | 564 | 562 | 2 (unrelated micro-benchmarks, explicitly `#[ignore]`d in `tests/micro_bench.rs`) |
| New unit tests (this plan) | 9 | 9 | 0 |
| New integration tests (this plan) | 2 | 2 | 0 |
| E2E (`make test-e2e`, live Docker stack: Exasol + MinIO + Iceberg REST) | 78 | 78 | 0 |

### Manual Tests

| Test | Result |
|------|--------|
| `cargo test -p lakehouse-engine --test scan_name_mapping` | ✓ (2 passed, 1 suite, 0.61s) |
| `cargo test -p lakehouse-engine field_id_adapter` | ✓ (15 passed, 488 filtered out, 19 suites, 0.44s) |

## Tool Evidence

### Linter

```
$ cargo clippy --all-targets
cargo clippy: No issues found
```

### Formatter

```
$ cargo fmt --check
(exit 0, no diff)
```

One formatting issue (a double blank line in `scan/spec.rs`, introduced when a test was inserted)
was found by code review and fixed before this final run — see Notes.

### Build

```
$ make cross-musl-udf-build
   Finished `release` profile [optimized] target(s) in 21m 14s
$ ls -la target/release/liblakehouse_engine.so
-rwxr-xr-x 1 talos talos 163.1M ... target/release/liblakehouse_engine.so
```

## Scenario Coverage

| Domain | Feature | Scenario | Test Location | Test Name | Passes |
|--------|---------|----------|---------------|-----------|--------|
| datafusion-scan | scan-execution-field-id-projection | Column projection binds by Iceberg field-id across physical layouts | `crates/lakehouse-engine/tests/e2e_scan_test.rs` | `e2e_renamed_column_resolves_by_field_id` (existing, unchanged) | Pass |
| datafusion-scan | scan-execution-field-id-projection | Field-id resolution honors schema.name-mapping.default for a file field without an embedded field-id | `crates/lakehouse-engine/tests/scan_name_mapping.rs` | `name_mapping_resolves_no_field_id_column` | Pass |
| datafusion-scan | scan-execution-field-id-projection | Field-id resolution honors schema.name-mapping.default (embedded id precedence) | `crates/lakehouse-engine/src/scan/mod.rs` (`field_id_adapter` mod) | `embedded_field_id_wins_over_name_mapping` | Pass |
| datafusion-scan | scan-execution-field-id-projection | Field-id resolution falls back to physical name when no name-mapping resolves a file field | `crates/lakehouse-engine/src/scan/mod.rs` (`field_id_adapter` mod) | `no_name_mapping_falls_back_to_physical_name`, `uncovered_name_mapping_falls_back_to_physical_name` | Pass |
| datafusion-scan | scan-execution-field-id-projection | Field-id resolution falls back to physical name (read path) | `crates/lakehouse-engine/tests/scan_name_mapping.rs` | `empty_name_mapping_preserves_physical_name_binding` | Pass |
| datafusion-scan | scan-execution-field-id-projection | The VS resolves schema.name-mapping.default once per query into the scan spec | `crates/lakehouse-engine/src/adapter/pushdown.rs` | `resolves_name_mapping_flat_entries_once`, `absent_name_mapping_is_empty`, `malformed_name_mapping_errors_cleanly` | Pass |
| datafusion-scan | scan-execution-field-id-projection | The VS threads name-mapping through the scan spec (round-trip) | `crates/lakehouse-engine/src/scan/spec.rs` | `name_mapping_round_trips_and_defaults_to_empty` | Pass |
| datafusion-scan | scan-execution-field-id-projection | Added nullable column absent from an older file is NULL-filled | `crates/lakehouse-engine/tests/e2e_scan_test.rs` | existing coverage (unchanged) | Pass |
| datafusion-scan | scan-execution-field-id-projection | Added required column missing from an older file errors cleanly | `crates/lakehouse-engine/src/scan/mod.rs` (`field_id_adapter` mod) | existing coverage (unchanged) | Pass |
| datafusion-scan | scan-execution-field-id-projection | Scan without a logical schema falls back to first-file inference | `crates/lakehouse-engine/tests/scan_no_head_test.rs` | existing coverage (unchanged) | Pass |

Additional test beyond the plan's table, added during implementation to cover a correctness edge
case surfaced while rewiring the resolution order:

| Domain | Feature | Scenario | Test Location | Test Name | Passes |
|--------|---------|----------|---------------|-----------|--------|
| datafusion-scan | scan-execution-field-id-projection | An embedded field-id absent from the logical schema keeps the physical name and does not fall through to the name-mapping | `crates/lakehouse-engine/src/scan/mod.rs` (`field_id_adapter` mod) | `embedded_field_id_absent_from_logical_schema_skips_name_mapping` | Pass |

## Notes

- **Implementation approach**: built incrementally across 4 dependency-ordered groups by 6
  sub-agent runs — Group A (spec-field threading, foundation), Group B (VS-side parse + scan-side
  plumbing + doc-comment fix, run in parallel on disjoint files), Group C (the core
  `rename_physical_to_logical` resolution-order rewire, run solo as the highest-risk task), Group D
  (4 test tasks, run in parallel on disjoint files). No merge conflicts across any of the parallel
  runs.
- **Code review** (Phase 4) found the implementation correct end-to-end on the first pass — the
  precedence logic (embedded field-id → name-mapping → physical-name fallback, with the tricky
  "embedded-but-absent-from-schema" edge case correctly isolated), the VS-side parser (uses the
  pinned `iceberg` crate's own `NameMapping` deserializer, no hand-rolled JSON, no nested-`fields`
  recursion per the explicit scope cut), and the end-to-end spec/plumbing threading were all
  verified correct. The single finding was a cosmetic `cargo fmt` violation (a doubled blank line
  left by a test insertion), fixed directly; while fixing it, a stray "Task 1.2:" work-tracking
  reference was also dropped from that test's doc comment per this repo's no-ticket-refs-in-code
  comment guardrail.
- **Task 3.2's inline sanity test** was cleanly superseded: task 3.3 replaced it with 5 precisely
  named tests (matching the plan's Verification table names) plus one extra for the edge case
  above — no duplicate or orphaned test remains.
- **Dependency**: confirmed the pinned `iceberg` crate (git tag `v0.10.0-rc.2`, rev `be6cc96`)
  already exports `iceberg::spec::{NameMapping, MappedField, DEFAULT_SCHEMA_NAME_MAPPING}` with a
  spec-accurate `Deserialize` impl — no new dependency was added.
- **Out of scope, unchanged from plan**: nested `fields` name-mapping entries (deferred to
  follow-up issue #83, already filed), Iceberg column-projection rules #1 (partition
  Identity-Transform substitution) and #3 (`initial-default` values) — neither implemented
  anywhere in this engine, confirmed absent — and the drop+rename-into-a-reused-name collision case
  (investigated during planning and found to be a distinct, unrelated concern not solvable by
  name-mapping; only the misleading code comment referencing it was corrected).
- **E2E** (`make test-e2e`, `--features exasol-e2e`, against the live Exasol + MinIO + Iceberg REST
  Docker stack): all 78 tests pass, 0 failures — `e2e_capability_test` (8), `e2e_count_distinct_test`
  (6), `e2e_join_test` (10), `e2e_positional_deletes_test` (11), `e2e_scan_test` (43, re-run in
  isolation for direct evidence after the combined run's log was truncated by the output capture).
  No regressions in the existing field-id-projection E2E coverage. Note: this plan's own new
  name-mapping scenarios are exercised entirely through the Docker-free `run_raw_scan_with_session`
  integration path (`tests/scan_name_mapping.rs`) per the plan's own design — no E2E fixture
  carries a name-mapping property, so E2E coverage here is a regression check, not new-scenario
  coverage.
