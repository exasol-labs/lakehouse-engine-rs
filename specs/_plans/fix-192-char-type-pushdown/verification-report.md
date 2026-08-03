# Verification Report: fix-192-char-type-pushdown

## Verdict

| Result | Details |
|--------|---------|
| **PASS** | All three #192 repro facets (equal-length CASE GROUP BY, explicit `CAST(col AS CHAR(n))`, bare string-literal GROUP BY key) now push down and return correct, native-Exasol-matching results instead of failing with `Data type mismatch`. The grouping-correctness gap found during adversarial plan review (a CHAR-declared group key must group on the blank-padded value, not the raw staging string) is fixed and E2E-verified against trailing-whitespace data. One expert-review finding (an unprojected CHAR group key silently skipping the pad) was fixed; one narrower residual gap (an unprojected group key whose CHAR type comes from a CASE-of-CAST-to-CHAR-branches expression with no top-level `dataType`) is deliberately left as a scoped, tracked exception — filed as [issue #293](https://github.com/exasol-labs/lakehouse-engine-rs/issues/293) and cited inline in the spec delta, per this project's "never a silent gap" rule. |
| Code review | 7 findings — 7 fixed (standard: 6, expert: 1) |

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
| Unit | All 8 implementation tasks' scenarios have a dedicated unit test (see Scenario Coverage below); no production branch added by this plan is untested |
| Integration | All 4 E2E-only scenarios (native-Exasol-comparison shapes) covered by dedicated E2E tests in `e2e_capability_test.rs` |

### Test Results

| Type | Run | Passed | Ignored |
|------|-----|--------|---------|
| Unit (`cargo test --workspace`) | 602 (lakehouse-engine --lib) + 88 (vs-expression --lib) + integration-test binaries (positional-deletes, refresh, scan, join, count-distinct, int96, etc. — non-`exasol-e2e`-gated portions) | all passed | 2 (pre-existing, unrelated to this plan) |
| Integration (E2E, `make test-e2e`, live Docker Exasol 2025.2.1) | 143 across all 7 gated test files (`e2e_scan_test`, `e2e_capability_test`, `e2e_count_distinct_test`, `e2e_join_test`, `e2e_positional_deletes_test`, `e2e_int96_timestamp_test`, `e2e_refresh_test`) | 143 | 0 |

`e2e_capability_test.rs` alone: 26 passed, 0 failed, including all 4 new CHAR tests and the pre-existing `e2e_selectlist_cast_extract_case_pushdown` VARCHAR/unequal-length-CASE control (unaffected by this change).

### Manual Tests

| Test | Result |
|------|--------|
| `cargo test -p lakehouse-engine char` (all CHAR unit tests + controls) | ✓ |
| `cargo test -p vs-expression cast_char` (DataFusion-dialect bare VARCHAR; every Exasol-dialect CHAR test asserts `CHAR(n) ASCII`; divergence guard passes) | ✓ |
| `cargo test -p lakehouse-engine scalar_over_merge` (grouped-merge wrapper renders `CHAR(20) ASCII`, incl. nested CAST) | ✓ |
| Live probe: `CASE WHEN c_decimal_a < 0 THEN 'NEG' ELSE 'POS' END` over Exasol 2025.2.1 → `CHAR(3) ASCII`; unequal-length control → `VARCHAR(4) ASCII` (planning-time fact, re-confirmed live during Task 4.1's residual-gap investigation) | ✓ |
| Live E2E: over-length CHAR value on both the grouped and projection facets fails cleanly (SQL state `22002`, a UDF-emit-boundary truncation error — distinct from native Exasol's parser-level `22001` for a direct literal CAST, documented in both tests and the spec) rather than silently truncating | ✓ |
| Live E2E: `'ab'` / `'ab   '` merge into one row (count 2) under `CAST(val AS CHAR(30))`, 3 total rows, matching native Exasol | ✓ |

## Tool Evidence

### Linter

```
cargo clippy --all-targets --all-features
    Checking lakehouse-engine v0.30.2 (.../crates/lakehouse-engine)
    Finished `dev` profile [unoptimized + debuginfo] target(s) in 11.36s
0 warnings, 0 errors
```

### Formatter

```
cargo fmt --check
(no output — clean)
```

## Scenario Coverage

| Domain | Feature | Scenario | Test Location | Test Name | Passes |
|--------|---------|----------|---------------|-----------|--------|
| vs-adapter | pushdown-planning-char-type-declaration | A CHAR-declared type renders as CHAR | `support.rs` | `exasol_type_from_json_renders_char_type` | Pass |
| vs-adapter | pushdown-planning-char-type-declaration | A CHAR-declared ASCII type carries the ASCII suffix | `support.rs` | `exasol_type_from_json_propagates_char_ascii_character_set` | Pass |
| vs-adapter | pushdown-planning-char-type-declaration | A CHAR size above Exasol's maximum is capped at 2,000 | `support.rs` | `exasol_type_from_json_caps_char_size_at_exasol_maximum` | Pass |
| vs-adapter | pushdown-planning-char-type-declaration | CHAR with no `size` falls back to the unknown-width convention | `support.rs` | `exasol_type_from_json_char_without_size_falls_back_to_unknown_width` | Pass |
| vs-adapter | pushdown-planning-char-type-declaration | An explicit CAST-to-CHAR select-list item projects with a CHAR EMITS type | `support.rs` | `project_columns_emits_char_type_for_cast_to_char_item` | Pass |
| vs-adapter | pushdown-planning-char-type-declaration | An equal-length CASE group key resolves to a CHAR group-key type | `grouped_agg.rs` | `group_key_exasol_types_resolves_char_case_key` | Pass |
| vs-adapter | pushdown-planning-char-type-declaration | An unprojected CHAR group key still resolves its type via `groupBy[slot]["dataType"]` | `grouped_agg.rs`, `mod.rs` | `group_key_exasol_types_resolves_char_type_for_unprojected_group_key`, `unprojected_char_declared_group_key_reaches_the_scan_spec_blank_padded` | Pass |
| vs-adapter | pushdown-planning-char-type-declaration | A CHAR group key is blank-padded to its declared width before grouping | `mod.rs` | `grouped_char_declared_group_key_reaches_the_scan_spec_blank_padded` | Pass |
| vs-adapter | pushdown-planning-char-type-declaration | The pad width survives the ` ASCII` character-set suffix | `mod.rs` | `grouped_ascii_char_group_key_is_padded_to_its_declared_width` | Pass |
| vs-adapter | pushdown-planning-char-type-declaration | The pad preserves an over-length value unmodified (no truncating construct) | `grouped_agg.rs` | pad-shape assertions in `blank_pad_char_group_keys` tests | Pass |
| vs-adapter | pushdown-planning-char-type-declaration | A CHAR group key over trailing-space data groups identically to native Exasol | `e2e_capability_test.rs` | `char_group_key_merges_trailing_space_variants_like_native` | Pass |
| vs-adapter | pushdown-planning-char-type-declaration | An over-length CHAR group-key value raises a clean error instead of merging a truncated group | `e2e_capability_test.rs` | `over_length_char_group_key_raises_truncation_error_like_native` | Pass |
| vs-adapter | pushdown-planning-char-type-declaration | An over-length CHAR projection value fails cleanly rather than truncating | `e2e_capability_test.rs` | `over_length_char_projection_fails_cleanly` | Pass |
| vs-adapter | pushdown-planning-char-type-declaration | A bare string-literal group-key projection casts to CHAR | `grouped_agg.rs` | `constant_projection_casts_literal_to_char` | Pass |
| vs-adapter | pushdown-planning-char-type-declaration | A CAST-to-CHAR item inside an Exasol-parsed wrapper declares a CHAR column (all 3 seam-2 consumers) | `joins/sql_builders.rs`, `grouped_agg.rs` | `qualified_count_distinct_cast_char_renders_exasol_char_target`, `n_scan_join_select_list_renders_exasol_char_target`, `scalar_over_merge_casts_to_exasol_char_target` | Pass |
| vs-adapter | pushdown-planning-char-type-declaration | A nested CAST-to-CHAR over a merged aggregate renders CHAR at both levels | `grouped_agg.rs` | `scalar_over_merge_nested_char_cast_renders_char_at_both_levels` | Pass |
| sql-comprehension | vs-expression-translator-scalar-ops | The two dialects still diverge on a CHAR target | `vs-expression/src/lib.rs` | `cast_char_target_diverges_between_dialects` | Pass |
| vs-adapter | pushdown-planning-char-type-declaration | A MIN or MAX over a CHAR-typed expression declares a CHAR partial column | `grouped_agg.rs` | `min_over_char_expression_declares_char_partial_and_merge_cast` | Pass |
| vs-adapter | pushdown-planning-char-type-declaration | A VARCHAR-declared type is unaffected | `grouped_agg.rs` | `group_key_exasol_types_resolves_varchar_key_unchanged`, `unprojected_varchar_declared_group_key_reaches_the_scan_spec_unpadded` | Pass |
| vs-adapter | pushdown-planning-char-type-declaration | A CHAR-typed LIKE subject keeps pushing down unchanged | `support.rs` | `like_guard_char_subject_unchanged` | Pass |
| vs-adapter | pushdown-planning-char-type-declaration | The four #192 query shapes execute end to end | `e2e_capability_test.rs` | `char_declared_pushdown_shapes_match_native` | Pass |
| sql-comprehension | vs-expression-translator-scalar-ops | The Exasol dialect renders a CHAR CAST target as CHAR, not VARCHAR | `vs-expression/src/lib.rs` | `renders_cast_char_as_exasol_char`, `renders_cast_char_exasol_dialect_includes_length` | Pass |
| sql-comprehension | vs-expression-translator-scalar-ops | CAST translates to DataFusion CAST syntax (control) | `vs-expression/src/lib.rs` | `renders_cast_char_as_datafusion_varchar` | Pass |

## Notes

- **Residual gap tracked, not silently left**: an unprojected grouped-aggregate group key whose declared `CHAR(n)` type comes from an expression with no usable top-level `dataType` (concretely: a `CASE` whose *branches* are themselves `CAST`-to-`CHAR`, rather than plain string literals) is still shipped unpadded. This is narrower than #192 — every scenario in this plan's spec, and every #192 repro shape, is unaffected and pads correctly whether projected or not. Filed as [exasol-labs/lakehouse-engine-rs#293](https://github.com/exasol-labs/lakehouse-engine-rs/issues/293) and cited inline in `specs/_plans/fix-192-char-type-pushdown/vs-adapter/pushdown-planning-char-type-declaration/spec.md`.
- **SQL state correction**: the plan's spec text anticipated Exasol's over-length truncation error as SQL state `22001` (matching a native, parser-level `CAST(<literal> AS CHAR(n))`). Live E2E runs against the Rust SLC's UDF-emit boundary showed the actual code is `22002` for both the grouped and projection over-length facets — a real, verified divergence between the parser-level and UDF-emit-boundary error paths, not a bug. Both E2E tests assert `22002` with a doc-comment explanation; the spec's own hedge on this point ("message text and origin differ") already anticipated exactly this.
- **Two adversarial-review passes contributed material design changes**, both captured in `decision-log.md`: the original plan-review pass found the `vs-expression` seam-2 gap (three Exasol-parsed wrapper paths bypass the adapter's type-derivation seam) and the group-key blank-padding gap; this implementation's code-review pass found the unprojected-group-key sentinel-value gap (fixed) and surfaced the CASE-of-CAST-branches residual (tracked, not fixed).
- No production code outside the 5 files identified in the plan's Implementation Tasks (plus the review-driven fix to `group_key_exasol_types`) was touched. Test-only files (`seed.rs`, `e2e_capability_test.rs`) carry only additive test/fixture code.
