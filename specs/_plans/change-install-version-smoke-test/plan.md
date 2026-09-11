# Plan: change-install-version-smoke-test

## Summary

Replaces the install script's error-kind fingerprint smoke test with a positive version check that
calls `LAKEHOUSE_VERSION()` and compares the reported value against the resolved release version.
Adds the version script to the four statements the installer creates, and updates the install
documentation to match.

## Design

### Context

The install script verifies its own work by calling `LAKEHOUSE_SCAN('x', 'y')` with placeholder
arguments and classifying the KIND of error that comes back. A fingerprint mismatch fails the
install. Any other error passes it. That test carries three defects.

1. Success is an interpreted failure. The check passes on an error and has no positive signal.
2. It verifies nothing about WHICH artifact loaded. A stale `.so` from a prior release passes.
3. It couples install verification to the scan entry point's argument-deserialization behavior.

`LAKEHOUSE_VERSION()` shipped in release v0.45.0 (PR #390). The installer can now ask the deployed
`.so` what version it is and compare that answer against the release it just downloaded.

- **Goals**: verify positively that the intended artifact loaded, remove every scan-script call from
  the verification path, and keep the fingerprint-mismatch failure distinguishable.
- **Non-Goals**: no change to what `install-script-e2e` downloads, no change to the `.so` build or
  packaging, no engine-version floor check, and no fallback to the scan call.

### Decision

The install script asks the deployed artifact for its version and compares the answer against the
version it resolved from the GitHub release. The comparison is an exact string equality.

The comparison is sound because both sides derive from one source. CI reads
`crates/lakehouse-engine/Cargo.toml`'s `version` field and publishes the release as `tag=v$VER`
(`.github/workflows/ci.yml`). The UDF returns `env!("CARGO_PKG_VERSION")` of that same crate
(`crates/lakehouse-engine/src/lib.rs:53`). The installer strips the leading `v` from the resolved
tag (`normalize_version`). Both sides are therefore the same bare `X.Y.Z` string.

#### Architecture

```
GitHub release tag  ──normalize_version──▶  RESOLVED_ENGINE_VERSION
        │ (CI: tag=v$VER from Cargo.toml)              │
        ▼                                              ▼
  downloaded .so ──▶ LAKEHOUSE_VERSION() ──▶  classify_version_smoke
        (env!("CARGO_PKG_VERSION"))                    │
                                                       ▼
                          pass │ version-mismatch │ fingerprint-mismatch │ other-error
```

#### Patterns

| Pattern | Where | Why |
|---------|-------|-----|
| Positive assertion replaces error-kind classification | `run_smoke_test` | An install check must have a success signal, not an interpreted failure |
| Fixed projection alias | `version_smoke_sql` | `AS LAKEHOUSE_ENGINE_VERSION` makes the column header a known literal instead of a database-generated name, so the extractor has one fixed line to skip |
| Pure classifier separate from the reporter | `classify_version_smoke` | Four verdicts stay unit-testable through `source`, and the caller owns every message |
| Dedicated extractor per query | `extract_version_value` | The sibling `extract_query_value` skips lines matching `[0-9]*` to drop the row-count footer, which also discards a version like `0.45.0` |

### Consequences

| Decision | Alternatives Considered | Rationale |
|----------|------------------------|-----------|
| Full replacement of the scan-call verification | Keep the scan call as a secondary check | The user directed full removal. A retained scan call would reintroduce the coupling this plan removes. |
| Keep `fingerprint-mismatch` as a distinct verdict | Fold it into `other-error` | The SLC checks the fingerprint on loading ANY Rust UDF, so the failure reaches the version call too. Its remediation names the SLC version and differs from every other failure. |
| A second dedicated extractor | Parameterize `extract_query_value`; one extractor taking the header literal | The consolidated form is the better design and it would also fix the latent footer-skip defect in the SCRIPT_LANGUAGES read. That read's mis-parse would drop every registered language on the following `ALTER SYSTEM`, so touching it is a separate, higher-risk change. The duplication stays bounded at two callers, recorded in the spec Background, and consolidates at a third caller. |
| No engine-version floor | Refuse a `--lakehouse-version` below 0.45.0 | The user declined support for older releases. Such a run fails through the generic error path with the database's own message. |
| No engine crate version bump | Bump the minor version, per the default feat rule | This plan changes no code compiled into the `.so`. CI deliberately excludes `install.sh` from the release assets, and users fetch it from the contents API on `main` (`.github/workflows/ci.yml`, the comment above `files:`). A bump would publish a release whose only delta is the version string. |

## Features

| Feature | Status | Spec |
|---------|--------|------|
| packaging/version-udf | CHANGED | `specs/_plans/change-install-version-smoke-test/packaging/version-udf/spec.md` |

## Impact

Operators running the one-line install command see a new verification message and a new failure
mode. A successful install now reports `Version smoke test passed: LAKEHOUSE_VERSION() reports
<version>` in place of `Fingerprint smoke test passed`. An install whose deployed artifact reports
a version other than the resolved release version now FAILS, where it previously passed. That is
the intended behavior change: it catches a stale or wrong artifact that the old check accepted.

Breaking change for one input: an install pinned with `--lakehouse-version` below `0.45.0` now
fails at the verification step, because that release's `.so` exports no version entry point. The
user accepted this limitation. The installer applies no version floor and prints no special
message for it.

No change to the `.so`, to any UDF, to pushdown, or to query behavior. This plan changes no code
compiled into the `.so`, so it MUST NOT bump `crates/lakehouse-engine/Cargo.toml`'s `version`.

## Dependencies

Release v0.45.0 is published and carries `LAKEHOUSE_VERSION` (verified through
`gh api repos/exasol-labs/lakehouse-engine-rs/releases/latest`). CI's `install-script-e2e` job runs
`install.sh` with no `--lakehouse-version`, so it resolves that release and the new verification
passes there. No prerequisite work remains.

Prior art: commit `dac42c5` on the local branch `feat/add-lakehouse-version-udf` implemented this
exact change, and commit `5b3cbed` reverted the `install.sh`, `install.test.sh`, and `docs/install.md`
parts to split them into this follow-up. Run `git show 5b3cbed` to read the reverted diff. Treat it
as a reference, not as authority: the spec delta in this plan is the contract, and the reverted diff
already carries its own review fix (the header-skip glob suffix).

## Implementation Tasks

1. Replace the install script's verification path.
   1. 1.1 Add `ddl_version` next to `ddl_scan` in `deploy/scripts/install.sh`, emitting
      `CREATE OR REPLACE RUST SCALAR SCRIPT <schema>.LAKEHOUSE_VERSION()` with
      `RETURNS VARCHAR(100) AS` and the `%udf_object` path. Append its statement to the
      `create_engine_scripts` array between `ddl_scan` and `ddl_distribute_files`.
   2. 1.2 Add `version_smoke_sql`, emitting
      `SELECT <schema>.LAKEHOUSE_VERSION() AS LAKEHOUSE_ENGINE_VERSION`. Delete `smoke_test_sql`.
   3. 1.3 Add `extract_version_value`: skip the `[*` banner, a `LAKEHOUSE_ENGINE_VERSION*` header
      (glob suffix, to tolerate trailing whitespace), empty lines, `*Error*` lines, and the
      `* row in set` / `* rows in set` footer. Do NOT skip `[0-9]*` lines, which is the defect that
      forbids reusing `extract_query_value`. [expert]
   4. 1.4 Replace `classify_fingerprint_response` with `classify_version_smoke`, taking `rc`,
      `output`, `reported`, and `expected`. Return `fingerprint-mismatch` when the output contains
      `Fingerprint mismatch`, then `other-error` when `rc` is non-zero, then `version-mismatch`
      when `reported` differs from `expected`, then `pass`. Order matters: the fingerprint check
      precedes the return-code check. [expert]
   5. 1.5 Rewrite `run_smoke_test` around the four verdicts. `version-mismatch` names both the
      expected and the reported value, rendering an empty reported value through a
      `${reported:-<empty>}` placeholder. `other-error` surfaces the captured `$out` directly.
      `pass` logs `Version smoke test passed: LAKEHOUSE_VERSION() reports
      $RESOLVED_ENGINE_VERSION.`
   6. 1.6 Update the file header comment in `deploy/scripts/install.sh`: change "fingerprint smoke
      test" to "version smoke test" on line 4, and "its three scripts" to "its four scripts" on
      line 3. After task 1.1 the installer creates four scripts, so line 3 states a false count.
2. Update the install-script test suite.
   1. 2.1 Replace the exapump stub's `*"LAKEHOUSE_SCAN('x', 'y')"*` branch with
      `*"LAKEHOUSE_VERSION() AS LAKEHOUSE_ENGINE_VERSION"*` and five `EXAPUMP_SMOKE_MODE` values:
      default (banner, `LAKEHOUSE_ENGINE_VERSION` header, `${EXAPUMP_LAKEHOUSE_VERSION:-0.26.3}`,
      footer, exit 0), `version-mismatch` (same shape, `${EXAPUMP_WRONG_VERSION:-9.9.9}`, exit 0),
      `empty-version` (banner, header, `1 row in set` footer, no value line, exit 0),
      `fingerprint-mismatch` (the `F-UDF-CL-RUST-9001: Fingerprint mismatch` text on stderr, exit
      1), and `other-error` (`Error: connection to database lost` on stderr, exit 1). The default
      value `0.26.3` MUST track the curl stub's default `GH_ENGINE_TAG` of `v0.26.3`. Match on the
      alias, not on `LAKEHOUSE_VERSION()` alone: the bare form also appears inside the version DDL
      statement, and this branch precedes the `*"CREATE "*` branch. [expert]
   2. 2.2 Rename `test_three_scripts_ddl_saas_path_types` to `test_four_scripts_ddl_saas_path_types`.
      Add unit assertions on `ddl_version` (`RUST SCALAR SCRIPT`, `RETURNS VARCHAR(100)`, no
      `EMITS`) and integration assertions that the run issued the version DDL. Assert that the
      next-step template emits no `FOR SCRIPT LHVS.LAKEHOUSE_VERSION` grant line.
   3. 2.3 Replace `test_fingerprint_smoke_pass_and_fail` with `test_version_smoke_pass_and_fail`,
      covering all four verdicts across five cases: pass, version-mismatch (names both `0.26.3` and
      `9.9.9`, prints no `query-ready`), fingerprint-mismatch (names the SLC), other-error
      (surfaces the database error text), and `EXAPUMP_SMOKE_MODE=empty-version` (exits non-zero
      and the failure message carries the literal `<empty>`). The empty-version case is the only
      test of task 1.5's `${reported:-<empty>}` placeholder.
   4. 2.4 Add `test_version_smoke_query_and_extraction`. Unit-test `version_smoke_sql` and
      `extract_version_value` through `source "$INSTALLER"`, the pattern the DDL assertions already
      use. Cover a bare version value, a header line carrying trailing whitespace, and a banner and
      footer around the value. Assert from the run log that the executed verification SQL is the
      aliased version query and that no `LAKEHOUSE_SCAN` call appears.
   5. 2.5 Flip the three remaining smoke-test assertions that name the old behavior:
      `test_stdin_piped_invocation_no_body_consumption` (its output assertion and its executed-SQL
      assertion), `test_bucketfs_full_run_artifact_shapes`, and `test_skip_slc_gating`. [expert]
   6. 2.6 Add `test_docs_describe_version_verification`: read `$REPO_ROOT/docs/install.md` and
      assert it carries the version-script DDL with `RETURNS VARCHAR(100)`, the
      `SELECT LHVS.LAKEHOUSE_VERSION();` by-hand check, and no `LAKEHOUSE_SCAN('x', 'y')` call.
      In the same test, read `$REPO_ROOT/.github/workflows/ci.yml` and assert that the file carries
      no `fingerprint smoke test` text and that it names `LAKEHOUSE_VERSION`. That covers the
      scenario's CI half, which task 3.4 implements. Assert the exact phrase `fingerprint smoke
      test`, not the bare word `fingerprint`: `ci.yml` lines 33, 74, and 95 use `fingerprint` for
      the unrelated SDK and build-cache fingerprints.
   7. 2.7 Add `test_version_smoke_runs_on_the_deployment_path`: run the installer through `main()`
      with `--deployment <name> --arch x86_64`, so the verification runs after the Exasol Personal
      ssh branch. Point `HOME` at a sandbox fake home and write `write_local_deployment_fixture`'s
      output into `$HOME/.exasol/personal/deployments/<name>`, because `DEPLOYMENT_ROOT` derives
      from `$HOME` at `deploy/scripts/install.sh:43` and takes no environment override. Export
      `GH_ASSET_TARBALL="$ENGINE_TARBALL_GOOD"` so the local `.so` extraction succeeds. Assert exit
      0 and that the run log carries `LAKEHOUSE_VERSION() AS LAKEHOUSE_ENGINE_VERSION`.

      This is the first test in the suite that drives `--deployment` through `main()`. Every
      existing Personal-path test sources the installer and calls `deploy_personal_local` or
      `resolve_deployment_transport` directly, and `run_smoke_test` is called from `main()` at
      `install.sh:1529`, after that branch. The stubs support the full run. The descriptor supplies
      host, user and password, so connectivity resolves to host mode with no `--profile` or
      `--dsn`. The ssh and scp stubs answer every `push_*_to_vm` and `test -e` call. The curl stub
      serves both release downloads from `GH_ASSET_TARBALL`.
   8. 2.8 Update `main()`'s runner list: the two renamed functions plus the three new ones.
3. Update the install documentation.
   1. 3.1 In `docs/install.md`, change the "What the command does" step 4 to name four scripts
      including `LAKEHOUSE_VERSION`, and step 5 to describe the version comparison.
   2. 3.2 Add the version script's DDL block to the by-hand script appendix. Describe what the
      script returns and that it needs no CONNECTION. Correct the closing "All three scripts MUST
      be in the same schema" sentence: the co-location rule binds the adapter, scan, and
      distributor scripts, which the adapter calls schema-qualified.
   3. 3.3 Rewrite the "Appendix: fingerprint smoke test by hand" section as a version check around
      `SELECT LHVS.LAKEHOUSE_VERSION();`, listing the plain-version, fingerprint-mismatch, and
      other-error outcomes. Add an "Appendix: query the deployed version" section presenting the
      same query as a standalone operator diagnostic.
   4. 3.4 Update the `install-script-e2e` job comment in `.github/workflows/ci.yml` that names the
      "fingerprint smoke test" and the sandboxes it spawns, so it stops describing removed
      behavior. Comment text only, no job or step change.

## Parallelization

| Group | Tasks | Depends on | Knowledge |
|-------|-------|------------|-----------|
| A: install verification | 1.1-1.6, 2.1-2.8, 3.1-3.4 | — | spec delta `packaging/version-udf`; `deploy/scripts/install.sh`, `deploy/scripts/tests/install.test.sh`, `docs/install.md`, `.github/workflows/ci.yml` |

One group. Every task serves one spec delta, and the test file asserts both the script's behavior
and the documentation's content, so no split avoids a shared file. Splitting the script from its
test suite would slice by layer and force two agents to derive the same verdict model.

## Dead Code Removal

| Type | Location | Reason |
|------|----------|--------|
| Function | `smoke_test_sql`, `deploy/scripts/install.sh` | Replaced by `version_smoke_sql`. Its only caller is `run_smoke_test`. |
| Function | `classify_fingerprint_response`, `deploy/scripts/install.sh` | Replaced by `classify_version_smoke`. |
| Test | `test_fingerprint_smoke_pass_and_fail`, `deploy/scripts/tests/install.test.sh` | Replaced by `test_version_smoke_pass_and_fail`. |
| Stub branch | `*"LAKEHOUSE_SCAN('x', 'y')"*` and its `anomaly` mode, `deploy/scripts/tests/install.test.sh` | The verification issues no scan call, so the placeholder-argument response modes have no caller. |
| Doc section | "Appendix: fingerprint smoke test by hand", `docs/install.md` | Replaced by the version check appendix. |

The `anomaly` verdict has no successor. It existed only to catch the impossible case of the
placeholder scan spec returning rows. A version query that succeeds is the expected outcome, so a
zero return code is no longer an anomaly.

## Verification

### Scenario Coverage

| Scenario | Test Type | Test Location | Test Name |
|----------|-----------|---------------|-----------|
| The version entry point reports the compiled engine version (recorded, unchanged) | Integration | `crates/lakehouse-engine/tests/e2e_version_udf_test.rs` | `version_udf_returns_the_compiled_crate_version` |
| The install script creates the version script with the other deployment scripts | Integration | `deploy/scripts/tests/install.test.sh` | `test_four_scripts_ddl_saas_path_types` |
| The install verification queries the version script and reads its reported value | Integration | `deploy/scripts/tests/install.test.sh` | `test_version_smoke_query_and_extraction` |
| The install verification queries the version script and reads its reported value (every-install-path clause) | Integration | `deploy/scripts/tests/install.test.sh` | `test_version_smoke_runs_on_the_deployment_path` |
| The install verification passes only on an exact version match | Integration | `deploy/scripts/tests/install.test.sh` | `test_version_smoke_pass_and_fail` |
| The install documentation describes the version-based verification | Integration | `deploy/scripts/tests/install.test.sh` | `test_docs_describe_version_verification` |

Two tests carry the "runs on every install path" clause of the query scenario.
`test_skip_slc_gating` runs the full `--skip-slc` path end to end against the stubs, and task 2.5
flips its assertion to the new message. Task 2.7's
`test_version_smoke_runs_on_the_deployment_path` runs the Exasol Personal `--deployment` path
through `main()`. No existing test covers that path: `deployment_local_ssh_transport` asserts only
connection resolution, and every other Personal-path test bypasses `main()`.

### Manual Testing

| Feature | Command | Expected Output |
|---------|---------|-----------------|
| packaging/version-udf | `docker compose up -d --wait exasol`, then create an exapump profile against it, then `bash deploy/scripts/install.sh --profile local-bucketfs` | Exit 0 and the line `Version smoke test passed: LAKEHOUSE_VERSION() reports 0.45.0.` |
| packaging/version-udf | `exapump sql -d "$DSN" "SELECT LHVS.LAKEHOUSE_VERSION()"` after that install | One row holding `0.45.0` |
| packaging/version-udf | `bash deploy/scripts/install.sh --profile local-bucketfs --lakehouse-version 0.44.1` | Non-zero exit and a `version smoke test failed:` line carrying the database's own error. v0.44.1 is the last release before v0.45.0, so its `.so` exports no version entry point. |

The BucketFS write password comes out of the container's EXAConf, the same way CI's
`install-script-e2e` job reads it. In this checkout the container is
`lakehouse-engine-rs-2-exasol-1`.

### Checklist

| Step | Command | Expected |
|------|---------|----------|
| Install-script tests | `make test-install` | 0 failures |
| Shell lint | `shellcheck -s bash deploy/scripts/install.sh deploy/scripts/tests/install.test.sh` | 0 findings |
| Test | `cargo test` | 0 failures |
| Lint | `cargo clippy --all-targets` | 0 warnings |
| Format | `cargo fmt --check` | No changes |

`make cross-udf-build` and `make test-e2e` are not gates for this plan. It changes no Rust source,
so the `.so` is unaffected. CI's `install-script-e2e` job covers the live install path. Local
`shellcheck` is absent on this machine, so fetch the static release binary rather than skipping the
step: `make lint-install` reports a skip instead of failing when `shellcheck` is missing.
