# Verification Report: remove-emit-exa-types

## Verdict

| Result | Details |
|--------|---------|
| **PASS** | `exasol-udf-sdk`/`exasol-udf-macros` bumped to 0.26.0; `CommonScanSpec::emit_exa_types` deleted; the scan reads its declared output type from `UdfContext::output_column` on both the Arrow and Value emit paths; the four pre-existing partial-aggregate type mismatches the 0.26.0 row-validation change would have exposed are fixed. |
| Code review | 11 findings — 11 fixed (8 standard, 3 expert) |

| Check | Status |
|-------|--------|
| Build | ✓ |
| Tests | ✓ |
| Lint | ✓ |
| Format | ✓ |
| Scenario Coverage | ✓ |
| Manual Tests | ✓ |

`speq feature validate` exits 1, but only on ERRORs in features this plan does not touch (`vs-adapter/connection-credentials-azure`, `vs-adapter/scan-spec-credential-reference`, and others), pre-existing on `main`. This plan touched no permanent spec file outside `specs/_plans/remove-emit-exa-types/`; all 5 of its own changed features validate at 0 errors.

## Test Evidence

### Test Results

| Type | Run | Passed | Failed |
|------|-----|--------|--------|
| Unit + integration (`cargo test`) | 53 binaries | 1658 | 0 |
| E2E (`docker compose up -d && make test-e2e`) | 30 binaries | 336 | 0 |

### Manual Tests

| Test | Result |
|------|--------|
| `gh release view v0.26.0 --repo exasol-labs/language-container-rs` lists both `lc-rust-0.26.0.tar.gz` and `lc-rust-0.26.0-aarch64.tar.gz` | ✓ |
| `make -n install-slc \| grep lc-rust` names `lc-rust-0.26.0.tar.gz`, derived from the `Cargo.toml` pin | ✓ |
| `docker compose up -d` then `make test-e2e`: 0 failures | ✓ |
| `scripts/capture-pushdown-payload.sh 'SELECT * FROM {table}'` against `TYPED_DISTINCT_PROBE`: captured common blob carries no `emit_exa_types` key; statement carries a full `EMITS ("ID" DECIMAL(20,0), "C_DECIMAL_A" DECIMAL(9,2), ..., "C_QTY" DECIMAL(20,0))` clause | ✓ |
| `scripts/capture-pushdown-payload.sh 'SELECT AVG(C_DECIMAL_A), STDDEV(C_DECIMAL_A), MIN(ID) FROM {table}'`: returns `[28.299, 17.393233326657683, "1"]`, no `output column … is … but the value is …` error | ✓ |

## Tool Evidence

### Build

```
cargo exasol-udf validate target/release/liblakehouse_engine.so
  LAKEHOUSE_ADAPTER: ABI version 9, fingerprint 0.26.0:rustc_1.94.1__e408947bf_2026-03-25_ — OK
  LAKEHOUSE_SCAN: ABI version 9, fingerprint 0.26.0:rustc_1.94.1__e408947bf_2026-03-25_ — OK
  LAKEHOUSE_VERSION: ABI version 9, fingerprint 0.26.0:rustc_1.94.1__e408947bf_2026-03-25_ — OK
✓ 3 UDF(s) validated in 'target/release/liblakehouse_engine.so'
```

### Linter

```
cargo clippy --all-targets
Finished `dev` profile [unoptimized + debuginfo] target(s) in 21.46s
0 warnings, 0 errors
```

### Formatter

```
cargo fmt --check
(no output — no changes needed)
```

## Scenario Coverage

| Feature | Scenario | Test Location | Test Name | Passes |
|---------|----------|----------------|-----------|--------|
| datafusion-scan/scan-execution-value-conversion | Output columns coerced to the declared EMITS ExaType before emit_batch | `src/scan/emit_tests.rs` | `coerce_maps_every_exa_type_variant_to_its_arrow_target` | Pass |
| datafusion-scan/scan-execution-value-conversion | Output columns coerced to the declared EMITS ExaType before emit_batch | `src/scan/emit_tests.rs` | `emit_stream_fails_when_declared_column_count_disagrees` | Pass |
| datafusion-scan/scan-execution-value-conversion | Output columns coerced to the declared EMITS ExaType before emit_batch | `src/scan/emit_tests.rs` | `emit_stream_fails_on_numeric_with_absent_or_out_of_range_payload` | Pass |
| datafusion-scan/scan-execution-value-conversion | Output columns coerced to the declared EMITS ExaType before emit_batch | `tests/scan_batch_loop.rs` | `raw_scan_coerces_every_column_to_its_declared_output_type` (added during review fixes) | Pass |
| datafusion-scan/scan-execution-value-conversion | A TIMESTAMP(p) EMITS string maps back to the microsecond Arrow timestamp | `src/scan/emit_tests.rs` | `exa_type_timestamp_maps_to_microsecond_target` | Pass |
| datafusion-scan/scan-execution-partial-agg | Every emitted partial-aggregate cell matches its declared output column | `src/scan/partial_agg_tests.rs` | `partial_agg_fails_on_numeric_with_absent_or_out_of_range_payload` | Pass |
| datafusion-scan/scan-execution-partial-agg | Every emitted partial-aggregate cell matches its declared output column | `src/scan/partial_agg_tests.rs` | `partial_cells_conform_to_declared_output_columns` | Pass |
| datafusion-scan/scan-execution-partial-agg | Every emitted partial-aggregate cell matches its declared output column (AvgSum/StatSum/StatSumSq over a non-double column) | `tests/e2e_scan_test.rs` | `partial_avg_stddev_over_non_double_columns` (named differently than plan.md's `partial_aggregates_over_integer_and_decimal_columns_return_values`; same scenario, live against `typed_distinct_probe`'s `long`/`decimal(9,2)`/`decimal(20,4)` columns) | Pass |
| datafusion-scan/scan-execution-spec-reconstitution | Consolidating the shard-invariant fields preserves the two-argument wire | `src/adapter/pushdown/dispatch_golden_tests.rs` | 11 regenerated golden dispatch fixtures | Pass |
| datafusion-scan/scan-execution-spec-reconstitution | Consolidating the shard-invariant fields preserves the two-argument wire | `src/scan/spec_tests.rs` | `common_blob_carries_no_emit_type_key` | Pass |
| e2e-harness/e2e-harness-scan-correctness | The scan returns correct values across the type mix with no spec-carried emit types | `tests/e2e_emit_declaration_test.rs` | `scan_returns_correct_values_across_type_mix_with_no_spec_carried_emit_types` (named differently than plan.md's `scan_returns_correct_values_across_the_type_mix`; same scenario) | Pass |

## Notes

- Task 1.6 (the one-time live E2E gate proving `UdfContext::output_column` populates for the dynamic
  call-site `EMITS` form `LAKEHOUSE_SCAN` uses) passed: the runtime-reported output-column list
  equaled the generated `EMITS (...)` clause item for item. Two facts from that run shaped the
  design: non-`Numeric` columns report `ColumnInfo.precision`/`scale` as `Some(0)`, so coercion
  reads the typed `ExaType::Numeric` variant payload, never the flat `ColumnInfo` fields; and
  `type_name` for VARCHAR is `"VARCHAR(2000000) UTF8"`, a spelling the deleted string parser would
  not have matched.
- Code review's expert findings included one correctness bug introduced by this plan's own draft:
  `coerce_column`'s `arrow::compute::cast` defaulted to `safe: true`, so an unrepresentable value
  (e.g. a `SUM` overflowing its declared `DECIMAL(36,s)`) would have silently become `NULL` on both
  emit paths instead of failing loudly. Fixed to `safe: false` with new overflow tests on both
  paths; a full `cargo test -p lakehouse-engine` rerun confirmed nothing relied on the old
  NULL-on-overflow behavior.
- Two scenario-coverage test names differ from the literal names in `plan.md`'s Scenario Coverage
  table (noted in the table above); the scenario intent and location are unchanged and both pass.
- The wide-decimal `SUM` and `MIN`/`MAX` over a `decimal(p,0)` mismatches stay on unit coverage only
  (`partial_agg_tests.rs`), as the plan's Non-Goals ("No new seed fixture") requires — no seeded
  table or bench dataset holds those column shapes.
- `types::mapping::exasol_type_to_arrow` was retained `pub` with no production call site, per the
  plan's decision to keep it as a CLAUDE.md-documented compliance surface and the recorded inverse
  of `arrow_to_exasol_type`.
- Mid-verification, an unrelated diagnostic command on the orchestrator's part (`git stash -u` +
  `git checkout main -- .`, used to compare `speq feature validate` output against `main`) stashed
  all uncommitted implementation work. It was recovered immediately via `git stash pop` with no
  data loss; `git status`/`git diff --stat` confirm all 51 changed files and all new untracked files
  are present. No repeat of that command was needed — baseline drift was instead confirmed by
  checking that this plan's diff touches no spec file outside its own plan directory.
