# Verification Report: add-capability-alignment

## BLUF

**PASS.** All implementation tasks complete; both test suites fully green against the live
Exasol + MinIO + Iceberg stack. Capability advertisement now round-trips: every advertised name is
backed by a working translator path or shard-associative aggregate decomposition.

| Check | Command | Result |
|-------|---------|--------|
| Build (`.so`, glibc 2.36) | `make cross-musl-udf-build` | ✅ exit 0 |
| Host unit tests | `cargo test` | ✅ 176 passed, 0 failed |
| Lint | `cargo clippy --all-targets --features exasol-e2e` | ✅ 0 errors; 8 pre-existing dead-code/collapsible-if warnings in `tests/common/` only (present on `main`) |
| Format | `cargo fmt --check` | ✅ clean |
| E2E (full) | `make test-e2e` | ✅ `e2e_capability_test` 7/7, `e2e_scan_test` 22/22 |

## Scenario coverage

All plan `Scenario Coverage` entries have passing tests. Translator unit tests
(`renders_*`, `unsupported_date_fn_falls_through`), capability-list tests
(`reports_audited_capability_set`, `reports_supported_aggregate_capabilities`),
`non_decomposable_aggregate_falls_back_to_row_scan`, and the 8 E2E capability-group tests all pass.

## Divergences from the plan (the plan's assumptions were wrong; reality was verified live)

These matter for `/speq:record` — the recorded specs should reflect REAL Exasol/DataFusion behavior,
not the plan's pre-implementation assumptions:

1. **EXTRACT encoding & rendering.** Exasol sends EXTRACT as node type `function_scalar_extract`
   with the field in a `toExtract` property (NOT `function_scalar` + `dateTimeField`). DataFusion 54
   (default features) has no `EXTRACT(field FROM expr)` ExprPlanner, so EXTRACT and the
   YEAR/MONTH/DAY/… shortcuts render as `date_part('<FIELD>', expr)`, not `EXTRACT(<FIELD> FROM expr)`.
2. **REGEXP_LIKE predicate.** Real Exasol node type is `predicate_like_regexp` (not
   `predicate_regexp_like`); SQL surface syntax is the infix predicate `<str> REGEXP_LIKE <pat>`.
3. **CASE / NULLIF expansion.** Exasol expands `NULLIF(MOD(id,5),0)` into a `function_scalar_case`
   node (simple/searched CASE). Advertising `FN_CASE` requires a `function_scalar_case` translator
   arm (added) — distinct from the `CASE` `function_scalar` form the plan described.
4. **Select-list / group-key result types.** Declared types live in a parallel top-level
   `selectListDataTypes` array, not a per-node `dataType`. Rendered scalar-expression projection
   columns must EMIT the declared Exasol type (else Exasol rejects on type mismatch).
5. **Aggregate merge output types.** Both single-group and grouped aggregate merges must
   `CAST(<merge_expr> AS <declared_type>)` — Exasol strictly validates the pushdown output column
   types against the query's declared aggregate result types (e.g. `COUNT` → `DECIMAL(18,0)`).

## Notes / latent fixes beyond the original plan

- Fixed a pre-existing single-group aggregate type-mismatch (uncast `SUM(PARTIAL_count_0)` widened to
  `DECIMAL(31,0)` vs declared `DECIMAL(18,0)`) — identical SQL on `main`, so it was a latent bug.
- Hardened the E2E harness `exasol_container()` to disambiguate by published SQL port (a second,
  unrelated Exasol stack on the host broke BucketFS credential extraction).
- Added `--test e2e_capability_test` to the Makefile `test-e2e` target so the new suite runs in the gate.

## Version

`lakehouse-engine` 0.4.0 → 0.5.0, `vs-expression` 0.1.0 → 0.2.0 (feature additions; `Cargo.lock` synced).
