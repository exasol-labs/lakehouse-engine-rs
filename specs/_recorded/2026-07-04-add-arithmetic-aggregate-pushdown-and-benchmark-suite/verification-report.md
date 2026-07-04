# Verification Report: add-arithmetic-aggregate-pushdown-and-benchmark-suite

## Verdict: PASS

All implementation tasks (11) complete, code review clean (3 LOW nits, none actioned), full automated
checklist green, all plan scenarios covered by passing tests. One item (Group C parallelism sweep)
concluded as a validated no-op per its own gating criteria — that is a pass, not a gap.

## Evidence

### Automated checklist
| Check | Command | Result |
|---|---|---|
| Build | `make cross-musl-udf-build` | Already current (built during task 1.6's `make test-e2e`) |
| Test | `cargo test --workspace` | 441 passed, 2 ignored, 0 failed |
| Lint | `cargo clippy --workspace --all-targets` | No issues found |
| Format | `cargo fmt --check` | Clean |
| E2E | `make test-e2e` (task 1.6) | 51 passed, 0 failed (e2e_scan_test 41, e2e_capability_test 7, e2e_count_distinct_test 3) |

### Scenario coverage
| Scenario | Test | Status |
|---|---|---|
| SUM over two-column binary-arithmetic argument pushes down | `e2e_scan_test::sum_two_column_product_pushes_down_matches_single_node` | PASS |
| SUM over two-column binary-arithmetic argument (plan shape) | `scan_plan_shape::sum_two_column_product_emits_aggregates_not_raw_scan` | PASS |
| DECIMAL two-column product widens to declared SUM type | `pushdown::decimal_product_sum_partial_widens_to_decimal_36` | PASS |
| Arithmetic operator capabilities advertised | `capabilities::` (2 assertions) | PASS |
| Untranslatable arithmetic argument falls back to row scan | `e2e_scan_test::untranslatable_aggregate_argument_falls_back_to_row_scan` | PASS |
| Arithmetic operators translate with reconciled live names | `vs_expression::renders_two_column_arithmetic_product` + `arithmetic_operator_set_matches_advertised_capabilities` | PASS |
| (Added, not in original plan) Mixed column+expression raw-scan projection | `scan_plan_shape::raw_scan_projects_mixed_column_and_expression_items` | PASS |
| (Added, not in original plan) Select-list expression pushdown E2E | `e2e_capability_test::e2e_selectlist_expression_pushdown` | PASS (was failing pre-fix) |

### Manual verification
| Check | Result |
|---|---|
| `bench/run.sh selftest` | OK |
| `grep -c 'NQ[1-5]\|nq[1-5]'` across run.sh/trino_compare.sh/athena_compare.sh/spark_queries.py | ≥5 each |
| Parallelism sweep (pf 8/16/24, 2 independent runs) | pf16 flat, pf24 regresses Q3 (+7.9%) and Q9b (+11.9%) — no-op confirmed |

## Deviations from plan (both necessary, both fixed within scope)

1. **`MUL`→`MULT` naming bug** (anticipated as a risk in the plan's Decision section, confirmed live and fixed in task 1.3).
2. **`ProjectionItem` structural fix** (task 1.6, not in the original plan) — advertising arithmetic capabilities broadly exposed a pre-existing latent bug in `scan/mod.rs::build_scan_sql`'s raw-scan projection builder, which assumed every projection entry was a bare column identifier. Fixed by introducing a `Column`/`Expr` enum mirroring the existing `AggregatePlan.column`/`arg_expr` split. This was a gating correctness fix, not scope creep — shipping task 1.2 without it would ship a regression.

## Outcome per plan item

| Item | Outcome |
|---|---|
| Parallelism-factor sweep | Validated no-op — no code changed, evidence recorded (decision-log [10]) |
| Two-column arithmetic aggregate pushdown | Shipped — `SUM(col_a OP col_b)` for `*`/`+`/`-`/`/` now decomposes into partial/merge pushdown instead of raw-emitting both columns |
| 5 new benchmark queries | Shipped — NQ1-NQ5 wired into all 4 dialect scripts |

## Next steps (outside this plan's scope, tracked by the outer orchestration)

- Run the full competitive benchmark (Exasol test1 + ephemeral Trino + Athena + EMR Serverless Spark) including NQ1-NQ5, and update `docs/performance.md`.
- `speq:record add-arithmetic-aggregate-pushdown-and-benchmark-suite` to merge these spec deltas into the permanent library.
