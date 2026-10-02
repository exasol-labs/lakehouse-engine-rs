# Verification Report: test-lakekeeper-batch-check-fixtures-two-principals

## Verdict

| Result | Details |
|--------|---------|
| **PASS** | The Lakekeeper e2e stack enforces permissions through OpenFGA, and the suite passes with 52 tests, 0 failed, 0 ignored. |
| Code review | 21 findings — 21 fixed |

| Check | Status |
|-------|--------|
| Build | ✓ |
| Tests | ✓ |
| Lint | ✓ |
| Format | ✓ |
| Scenario Coverage | ✓ |
| Manual Tests | ✓ (partial, see Notes) |

## Test Evidence

### Coverage

| Type | Coverage % |
|------|------------|
| Unit | not measured |
| Integration | not measured |

### Test Results

| Type | Run | Passed | Ignored |
|------|-----|--------|---------|
| Unit (`cargo test`) | 1590 | 1590 | 0 |
| Integration (`make test-e2e-lakekeeper`) | 52 | 52 | 0 |

### Manual Tests

| Test | Result |
|------|--------|
| Cold stack: remove Lakekeeper, OpenFGA, Keycloak containers and databases, run the one-shot order, run the suite (implementer run, 47 tests before review fixes) | ✓ |
| `authz-backend` from `GET /management/v1/info` is `openfga` (asserted by `lakekeeper_stack_enforces_permissions`) | ✓ |
| Fixtures stable on re-run without `LH_LAKEKEEPER_FIXTURE_CAPTURE` (`authz_batch_check_fixtures_match_live_contract` in compare mode) | ✓ |
| Stack-down run fails and reports no ignored test | not re-run |

## Tool Evidence

### Linter

```
cargo clippy --all-targets -- -D warnings: exit 0
cargo clippy -p lakehouse-engine --all-targets --features lakekeeper-e2e -- -D warnings: exit 0
```

### Formatter

```
cargo fmt --check: exit 0
```

## Scenario Coverage

| Domain | Feature | Scenario | Test Location | Test Name | Passes |
|--------|---------|----------|---------------|-----------|--------|
| lakekeeper-e2e | lakekeeper-e2e-harness | The Lakekeeper stack enforces permissions | `crates/lakehouse-engine/tests/e2e_lakekeeper_test.rs` | `lakekeeper_stack_enforces_permissions` | Pass |
| lakekeeper-e2e | lakekeeper-e2e-harness | Two principals hold different table grants | `crates/lakehouse-engine/tests/e2e_lakekeeper_test.rs` | `lakekeeper_two_principals_hold_different_table_grants` | Pass |
| lakekeeper-e2e | lakekeeper-e2e-harness | The existing Lakekeeper scenarios pass with permissions enforced | `crates/lakehouse-engine/tests/e2e_lakekeeper_test.rs` | existing scenario tests on the OpenFGA stack | Pass |

## Notes

- The change is test infrastructure only. No adapter, UDF, or `.so` code changes, so no version bump applies.
- Task 1.7 results (OpenFGA `v1.14.2`) are in decision [5] of `decision-log.md` and in the fixtures README. No fact contradicted the `v1.8.16` probe.
- The tests that rewrite the checker principal's grants race at default test parallelism. `make test-e2e-lakekeeper` runs them serially.
- `make test-lakekeeper-local` did not run, because `jq` is missing on this host.
- The Azure e2e suite did not run here. Its compose file and CI job changed and clippy with `azure-e2e` is clean.
- The "stack down must fail" manual check was not re-run after the review fixes.
