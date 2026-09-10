# Verification Report: add-lakehouse-version-udf

## Verdict

| Result | Details |
|--------|---------|
| **PASS** | All automated checks green; 1 code-review finding fixed |
| Code review | 1 finding — 1 fixed |

| Check | Status |
|-------|--------|
| Build | ✓ |
| Tests | ✓ |
| Lint | ✓ |
| Format | ✓ |
| Shell lint | ✓ |
| Scenario Coverage | ✓ |
| Manual Tests | ✓ (E2E deferred to CI) |

## Test Evidence

### Test Results

| Type | Run | Passed | Failed | Ignored |
|------|-----|--------|--------|---------|
| Unit (`cargo test -p lakehouse-engine --lib`) | 1200 | 1200 | 0 | 0 |
| Installer (`make test-install`) | 499 | 499 | 0 | 0 |
| E2E (`make test-e2e`) | deferred to CI | — | — | — |

## Tool Evidence

### Linter

```
cargo clippy --all-targets — exit 0, 0 warnings
shellcheck -s bash deploy/scripts/install.sh deploy/scripts/tests/install.test.sh — exit 0
```

### Formatter

```
cargo fmt --check — exit 0, no changes
```

## Scenario Coverage

| Scenario | Test Type | Test Location | Test Name | Passes |
|----------|-----------|---------------|-----------|--------|
| Version entry point reports compiled engine version | Unit | `crates/lakehouse-engine/src/lib_tests.rs` | `engine_version_is_a_plain_semver_string` | Pass |
| Version entry point reports compiled engine version | E2E | `crates/lakehouse-engine/tests/e2e_version_udf_test.rs` | `version_udf_returns_the_compiled_crate_version` | Deferred to CI |
| Smoke test verifies deployed version matches downloaded release | Integration | `deploy/scripts/tests/install.test.sh` | `test_version_smoke_pass_and_fail` | Pass |

## Notes

- The E2E test (`e2e_version_udf_test.rs`) requires a running Exasol Docker container. It is wired into `make test-e2e` and asserted in `build_convention.rs`. Execution is deferred to CI.
- The code-review finding (S1: missing glob suffix on header-skip pattern in `extract_version_value`) was fixed before verification.
