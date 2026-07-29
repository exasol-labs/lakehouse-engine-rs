# Verification Report: refactor-type-mapping-single-source

## Verdict

| Result | Details |
|--------|---------|
| **PASS** | Arrow/Exasol type-mapping duplication (issue #176) consolidated into `types/mapping.rs`; byte-identical output verified by unedited pre-existing suite plus new characterization/divergence tests; full workspace test suite, live-Exasol E2E suite, lint, and format all green. |
| Code review | 6 findings — standard: 5 fixed, expert: 1 fixed |

| Check | Status |
|-------|--------|
| Build | ✓ (`cargo build -p lakehouse-engine` clean; `make cross-musl-udf-build` succeeded in 17m43s, produced `target/release/liblakehouse_engine.so`, 163.5 MB) |
| Tests | ✓ (`cargo test --workspace`: 877 passed, 0 failed) |
| Lint | ✓ (`cargo clippy --workspace --all-targets -- -D warnings`: clean) |
| Format | ✓ (`cargo fmt --all -- --check`: clean) |
| Scenario Coverage | ✓ (all 6 scenarios covered; 2 test names differ from the plan's literal names, see Notes) |
| Manual Tests | ✓ (all 6 checklist rows pass; 1 row's stated expectation is inaccurate, see Notes) |
| E2E | ✓ (`make test-e2e` against live Docker Exasol + Iceberg REST + MinIO + spark-iceberg-fixtures: 184 passed, 0 failed across 7 suites) |

## Test Evidence

### Test Results

| Type | Run | Passed | Ignored |
|------|-----|--------|---------|
| Unit (workspace, `cargo test --workspace`) | 877 | 877 | 2 (pre-existing bench smoke tests, unrelated to this plan) |
| Unit (`lakehouse-engine` lib only) | 697 | 697 | 0 |
| E2E (`make test-e2e`, live Exasol Docker stack) | 184 | 184 | 0 |

E2E breakdown: `e2e_capability_test` 60, `e2e_count_distinct_test` 16, `e2e_int96_timestamp_test` 7, `e2e_join_test` 15, `e2e_positional_deletes_test` 16, `e2e_refresh_test` 11, `e2e_scan_test` 59 — all `test result: ok`, 0 failed.

Baseline note: the `lakehouse-engine` lib test count moved 704 → 697 over the course of the plan: task 1 through 6 added tests (net +8 from the pre-refactor baseline), and the code-review expert fix then deleted 7 duplicate `sum_emit_type` single-input tests that were redundant with two new table-driven tests. No test was deleted without confirming its input/expectation was reproduced by a surviving table row (see review-findings.md expert fix and its completion report).

### Manual Tests

| Test | Result |
|------|--------|
| `cargo test --workspace` — 0 failures, no assertion/expected-value changed from pre-refactor | ✓ |
| `git diff --stat -- crates/lakehouse-engine/src` — `adapter/mod.rs` and `adapter/pushdown/support.rs` shrink, `types/mapping.rs` grows | ✓ shrink/grow direction correct; ✗ **but net line count does NOT fall** — see Notes |
| `grep -rn 'strip_prefix("DECIMAL(' crates/lakehouse-engine/src` — exactly one hit, in `types/mapping.rs` | ✓ (`types/mapping.rs:173`) |
| `grep -rn 'fn exasol_type_to_json\|fn exasol_type_from_json' crates/` — exactly one definition hit each, both in `types/mapping.rs` | ✓ (`types/mapping.rs:388`, `:423`; other grep hits are test-function names containing the same substring, not duplicate definitions) |
| `grep -rn 'unreachable!' crates/lakehouse-engine/src/scan/convert.rs` — no hits | ✓ |
| `make test-e2e` — 0 failures, confirms `createVirtualSchema` schema JSON and generated scan SQL still drive a live query end to end | ✓ 184 passed, 0 failed |

## Tool Evidence

### Linter

```
cargo clippy --workspace --all-targets -- -D warnings
Finished `dev` profile [unoptimized + debuginfo] target(s) in ...
(exit 0, no warnings/errors)
```

### Formatter

```
cargo fmt --all -- --check
(exit 0, no diff)
```

### Build

```
make cross-musl-udf-build
   Finished `release` profile [optimized] target(s) in 17m 43s
target/release/liblakehouse_engine.so — 163.5M
```

## Scenario Coverage

| Domain | Feature | Scenario | Test Location | Test Name | Passes |
|--------|---------|----------|---------------|-----------|--------|
| datafusion-scan | type-mapping-module-structure | One arm list decides both the Exasol type string and the JSON-fallback flag | `crates/lakehouse-engine/src/types/mapping.rs` | `varchar_type_string_alone_does_not_decide_the_json_fallback` | Pass |
| datafusion-scan | type-mapping-module-structure | One DECIMAL parser serves every Exasol type-string consumer (`exasol_type_to_json` side) | `crates/lakehouse-engine/src/types/mapping.rs` | `exasol_type_to_json_absent_decimal_scale_becomes_scale_zero_decimal`, `exasol_type_to_json_out_of_range_decimal_args_become_varchar`, `exasol_type_to_json_negative_decimal_scale_stays_signed`, `exasol_type_to_json_malformed_decimal_arg_lists_stay_varchar` | Pass (4 tests; plan named one test `exasol_type_to_json_pins_three_decimal_parser_divergences`, implementer split it into one test per divergence class plus a non-divergence guard test — same coverage, different names) |
| datafusion-scan | type-mapping-module-structure | One DECIMAL parser serves every Exasol type-string consumer (`sum_emit_type` side) | `crates/lakehouse-engine/src/adapter/pushdown/grouped_agg.rs` | `sum_emit_type_absent_scale_widens_to_a_scale_zero_decimal`, `sum_emit_type_never_echoes_a_non_canonical_scale_text`, `sum_emit_type_declines_every_precision_the_parser_rejects` | Pass (3 tests, post-review-fix; plan named one test `sum_emit_type_pins_decimal_parser_divergences` — implementer used the absent-scale case plus two table-driven invariant tests covering all 7 representative inputs, then review deleted 7 redundant single-input duplicates) |
| datafusion-scan | type-mapping-module-structure | The Exasol type string and VS dataType JSON conversions live in the type-mapping module | `crates/lakehouse-engine/src/types/mapping.rs` | `exasol_type_to_json_roundtrip`, `exasol_type_to_json_timestamp_with_local_time_zone`, `exasol_type_from_json_reads_with_local_time_zone_flag`, `exasol_type_from_json_reads_timestamp_fractional_seconds_precision`, `exasol_type_from_json_propagates_ascii_character_set` | Pass (all 5 relocated unedited, exact names match plan) |
| datafusion-scan | type-mapping-module-structure | The Arrow-to-Value converter dispatches on one flat arm per Arrow type | `crates/lakehouse-engine/src/scan/convert.rs` | `int64_uint32_uint64_convert_identically_through_flat_arms` | Pass (exact name match; test was renamed during review to match this plan-recorded name — see review-findings.md) |
| datafusion-scan | type-mapping-module-structure | One classifier names the Exasol type-string families the pushdown guards branch on | `crates/lakehouse-engine/src/types/mapping.rs` | `classify_exa_type_matches_pushdown_guard_predicates` | Pass (plan named it `classify_exa_type_reproduces_pushdown_guard_families` — same coverage, different name) |
| datafusion-scan / scan-execution / vs-adapter / pushdown-planning | (cross-cutting) | Behavior is unchanged across the refactor | `crates/lakehouse-engine/tests/` (whole directory, unedited) + all relocated in-source tests + live E2E | `cargo test --workspace` (877 passed), `make test-e2e` (184 passed) | Pass |

## Notes

- **Manual-test row deviation (flagged, not silently passed):** the plan's manual-testing checklist states `git diff --stat` should show "Net line count falls." The actual diff is `553 insertions(+), 292 deletions(-)` — a net **increase** of 261 lines, not a decrease. The per-file shrink/grow direction the row also names (`adapter/mod.rs` and `support.rs` shrink, `types/mapping.rs` grows) is correct. The net-increase comes from the plan's own mandated additions: characterization/divergence-pinning tests for the DECIMAL-parser consolidation (spec.md scenario 2's closed three-class and two-invariant requirements), the new `ExaTypeClass`/`classify_exa_type` unit test, and expanded doc comments (module doc, `parse_decimal_args` contract, `arrow_type_from_tag` separation note) — all explicitly required by the Implementation Tasks and Scenario Coverage table. This is reported as a discrepancy in the plan's own stated expectation, not a defect in the implementation; the qualitative goal (duplication removed, one owner per decision) is met independent of raw line count.
- **Test name deviations** (2 of 6 scenario rows): implementer agents used different test names than the plan's Scenario Coverage table for the two DECIMAL-parser tests and the `ExaTypeClass` test. Coverage is equivalent or greater (the `exasol_type_to_json` and `sum_emit_type` cases each split one planned test name into several, one per divergence class, which is finer-grained than the plan's single-test naming). Recorded here so the plan-to-code test-name mapping stays honest rather than silently assumed.
- `make cross-musl-udf-build`'s docker toolchain and a live Exasol/Iceberg/MinIO Docker stack were both available in this environment and used for real (not skipped) — the build produced an actual `.so`, and E2E ran the full 7-suite matrix against it per CLAUDE.md's "must fail, not skip, if no DB" rule.
- The `.gitignore` change and `specs/_plans/` directory visible in `git status` are this plan's own planning artifacts (decision-log.md, plan.md, tasks.md, review-findings.md) plus an unrelated pre-existing local `.gitignore` edit (adding `.claude/`) — out of scope for this plan's diff, not touched by any implementer agent.
