# Verification Report: change-install-version-smoke-test

## Verdict

| Result | Details |
|--------|---------|
| **PASS** | Install verification now asks the deployed `.so` for its version via `LAKEHOUSE_VERSION()` and compares it against the resolved release version, replacing the error-kind classification of a placeholder `LAKEHOUSE_SCAN` call. All planned tasks and review fixes complete, all gates green. |
| Code review | 5 findings — 5 fixed |

| Check | Status |
|-------|--------|
| Build | ✓ (`cargo test` implies a clean build; no Rust source changed) |
| Tests | ✓ |
| Lint | ✓ |
| Format | ✓ |
| Scenario Coverage | ✓ |
| Manual Tests | not run (see Notes — plan gates on `make test-install` + `cargo test`, not a live Exasol install) |

## Test Evidence

### Test Results

| Type | Run | Passed | Failed |
|------|-----|--------|--------|
| Install-script (`make test-install`) | 522 | 522 | 0 |
| Cargo (`cargo test`, 52 binaries) | 1630 | 1630 | 0 |

## Tool Evidence

### Linter

```
shellcheck -s bash deploy/scripts/install.sh deploy/scripts/tests/install.test.sh
(v0.10.0 static release binary — none installed locally)
exit 0, no findings

cargo clippy --all-targets
exit 0, 0 warnings
```

### Formatter

```
cargo fmt --check
exit 0, no changes
```

## Scenario Coverage

| Domain | Feature | Scenario | Test Location | Test Name | Passes |
|--------|---------|----------|---------------|-----------|--------|
| packaging | version-udf | The version entry point reports the compiled engine version (recorded, unchanged) | `crates/lakehouse-engine/tests/e2e_version_udf_test.rs` | `version_udf_returns_the_compiled_crate_version` | Pass |
| packaging | version-udf | The install script creates the version script with the other deployment scripts | `deploy/scripts/tests/install.test.sh` | `test_four_scripts_ddl_saas_path_types` | Pass |
| packaging | version-udf | The install verification queries the version script and reads its reported value | `deploy/scripts/tests/install.test.sh` | `test_version_smoke_query_and_extraction` | Pass |
| packaging | version-udf | The install verification queries the version script and reads its reported value (every-install-path clause) | `deploy/scripts/tests/install.test.sh` | `test_version_smoke_runs_on_the_deployment_path` | Pass |
| packaging | version-udf | The install verification passes only on an exact version match | `deploy/scripts/tests/install.test.sh` | `test_version_smoke_pass_and_fail` | Pass |
| packaging | version-udf | The install documentation describes the version-based verification | `deploy/scripts/tests/install.test.sh` | `test_docs_describe_version_verification` | Pass |

## Notes

- No Rust source under `crates/` changed; `crates/lakehouse-engine/Cargo.toml`'s version is
  untouched, per the plan's explicit constraint (this change compiles into nothing that ships in
  the `.so`).
- `make cross-udf-build` and `make test-e2e` are not gates for this plan (per its Checklist) and
  were not run.
- Manual Testing (a live install against the Docker Exasol container) is listed in the plan but is
  operator/CI-facing verification, covered by CI's `install-script-e2e` job against the real
  release artifact; it was not exercised in this session since the plan's own Checklist gates on
  `make test-install` and `cargo test` only.
- Code review found 5 issues (4 standard, 1 expert), all fixed and re-verified:
  1. A missing `*)` default arm in `run_smoke_test`'s verdict `case` let an unclassifiable verdict
     silently report success — fixed with an `internal error` arm plus a new test case.
  2. `classify_version_smoke` took an unnecessary 4th parameter creating an undocumented
     call-ordering contract — reduced to 3 parameters, deriving the reported version internally.
  3. A redundant comment restating `extract_version_value`'s own case arms — trimmed to the
     non-obvious WHY only.
  4. A dead `EXAPUMP_WRONG_VERSION` override with no caller — replaced with a literal.
  5. [expert] The stubbed release version `0.26.3` was duplicated in three unlinked places with no
     enforcement of agreement — consolidated behind one exported `STUB_DEFAULT_ENGINE_TAG`
     constant, verified live that a `GH_ENGINE_TAG` override still tracks correctly through the
     stub chain.
