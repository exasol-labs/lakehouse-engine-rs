# Tasks: change-install-version-smoke-test

## PR Lifecycle
- [x] resolved
- [x] implemented
- [x] version-bumped — N/A: plan's Impact section requires no bump (no code compiled into the `.so` changed; `install.sh` is excluded from release assets and fetched from `main`)
- [ ] tested-green
- [ ] recorded
- [ ] pr-ready

## Phase 2: Implementation (Group A: install verification)
- [x] 1.1 Add `ddl_version` next to `ddl_scan` in `deploy/scripts/install.sh`
- [x] 1.2 Add `version_smoke_sql`; delete `smoke_test_sql`
- [x] 1.3 Add `extract_version_value` [expert]
- [x] 1.4 Replace `classify_fingerprint_response` with `classify_version_smoke` [expert]
- [x] 1.5 Rewrite `run_smoke_test` around the four verdicts
- [x] 1.6 Update the file header comment in `deploy/scripts/install.sh`
- [x] 2.1 Replace exapump stub's scan branch with version-query branch + 5 EXAPUMP_SMOKE_MODE values [expert]
- [x] 2.2 Rename `test_three_scripts_ddl_saas_path_types` → `test_four_scripts_ddl_saas_path_types`
- [x] 2.3 Replace `test_fingerprint_smoke_pass_and_fail` with `test_version_smoke_pass_and_fail`
- [x] 2.4 Add `test_version_smoke_query_and_extraction`
- [x] 2.5 Flip 3 remaining smoke-test assertions naming old behavior [expert]
- [x] 2.6 Add `test_docs_describe_version_verification`
- [x] 2.7 Add `test_version_smoke_runs_on_the_deployment_path`
- [x] 2.8 Update `main()`'s runner list
- [x] 3.1 Update `docs/install.md` "What the command does" steps 4-5
- [x] 3.2 Add version script DDL block to by-hand appendix; fix "All three scripts" sentence
- [x] 3.3 Rewrite fingerprint smoke test appendix; add "query the deployed version" appendix
- [x] 3.4 Update `install-script-e2e` job comment in `.github/workflows/ci.yml`

## Phase 4: Review Fixes
- [x] 4.1 Add a `*)` internal-error arm to `run_smoke_test`'s `case "$verdict"` in `deploy/scripts/install.sh`, plus a `test_version_smoke_pass_and_fail` case proving an unclassifiable verdict aborts
- [x] 4.2 Reduce `classify_version_smoke` to three parameters (`rc`, `output`, `expected`), deriving the reported version from `$output` internally
- [x] 4.3 Replace `extract_version_value`'s five-line header comment with the rationale only
- [x] 4.4 Replace the `EXAPUMP_WRONG_VERSION` override in `deploy/scripts/tests/install.test.sh` with the literal `9.9.9`
- [x] 4.5 Introduce the exported `STUB_DEFAULT_ENGINE_TAG` harness constant, derive both stubs and the version-mismatch assertion from it, and delete `EXAPUMP_LAKEHOUSE_VERSION` [expert]

## Phase 5: Verification
- [x] 5.1 Run test suite (`make test-install`, `cargo test`)
- [x] 5.2 Run linter (`shellcheck`, `cargo clippy`, `cargo fmt --check`)
