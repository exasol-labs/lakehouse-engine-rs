# Plan: add-lakehouse-version-udf

## Summary

Adds `LAKEHOUSE_VERSION()`, a third Rust entry point in the engine `.so` that reports the compiled
crate version through plain SQL. The installer replaces its provoke-an-error fingerprint smoke test
with a positive check: the returned version must equal the release it downloaded.

Closes #387.

## Design

### Context

Two problems share one cause: nothing in the deployed artifact reports its own version.

The installer's current smoke test calls `LAKEHOUSE_SCAN('x', 'y')` and classifies the *kind* of
error it gets back. A fingerprint mismatch fails the install. Any other error passes it. Success
is treated as an anomaly. The check is therefore inverted, and it verifies only that the `.so`
loaded against a compatible SLC. It cannot detect a stale or wrong artifact that loads correctly.

An operator has no way to ask which engine version is installed. Reading the BucketFS file name
does not answer it, because the installer uploads to a fixed path.

- **Goals** — one SQL call reports the deployed engine version. The installer verifies the loaded
  artifact is the release it downloaded, by exact version match.
- **Non-Goals** — no build metadata beyond the crate version (no git SHA, no build date). No
  version reporting through the adapter's Virtual Schema protocol. No change to the adapter or
  scan entry points.

### Decision

#### Architecture

The version value has exactly one owner: a crate-level constant fed by `CARGO_PKG_VERSION`. The
entry point returns it. The installer compares the returned string against the release version it
already resolved. No module boundary and no abstraction is added on either side.

```
crates/lakehouse-engine/Cargo.toml  version = "X.Y.Z"
        │                                    │
        │ env!("CARGO_PKG_VERSION")          │ CI: tag = "v$VER"
        ▼                                    ▼
  ENGINE_VERSION const                GitHub release vX.Y.Z
        │                                    │
  LAKEHOUSE_VERSION()                 install.sh: RESOLVED_ENGINE_VERSION
        │                                    │
        └────────► string equality ◄─────────┘
                   (installer smoke test)
```

The exact-match check is sound because CI derives the release tag from the same manifest field the
constant reads (`.github/workflows/ci.yml`, `release` job: `VER=$(grep -m1 '^version'
crates/lakehouse-engine/Cargo.toml ...)`, `tag=v$VER`). The installer strips the leading `v` via
`normalize_version`. Both sides therefore hold the same bare `X.Y.Z` string.

#### Patterns

| Pattern | Where | Why |
|---------|-------|-----|
| `RETURNS`-shape UDF derived from the Rust return type | `crates/lakehouse-engine/src/lib.rs` | `exasol-udf-macros` 0.24.0 selects RETURNS from a `Result<Option<T>, UdfError>` signature, so the entry point needs no `emits(...)` annotation |
| Single-owner version constant | `crates/lakehouse-engine/src/lib.rs` | The value is read in one place and testable without a database |

## Features

| Feature | Status | Spec |
|---------|--------|------|
| packaging/version-udf | NEW | `specs/_plans/add-lakehouse-version-udf/packaging/version-udf/spec.md` |

## Impact

Operators gain `SELECT <schema>.LAKEHOUSE_VERSION()`. It needs no catalog CONNECTION and no Virtual
Schema.

The installer gains one new abort condition. An install whose loaded artifact reports a version
different from the downloaded release now fails. That case previously passed, because the old smoke
test read only the error kind. This is stricter behavior, not a breaking interface change: no
existing script signature, VS property, or flag changes.

Older releases installed via `--lakehouse-version` that predate this entry point are not supported
by the new smoke test. The `--lakehouse-version` flag is expected to target releases that include
`LAKEHOUSE_VERSION()` going forward.

## Requirements

| Requirement | Details |
|-------------|---------|
| Version extraction | `extract_query_value` skips lines matching `[0-9]*`, so it discards a version such as `0.45.0`. The version path needs its own extractor. Alias the projection (for example `AS LAKEHOUSE_ENGINE_VERSION`) so the column header is a known literal to skip. |
| Test layout | The Rust unit test goes in a sibling `crates/lakehouse-engine/src/lib_tests.rs`, declared with `#[cfg(test)] #[path = "lib_tests.rs"] mod tests;` per the project's test-layout rule. |
| E2E contract | The new E2E suite MUST fail, not skip, when no Exasol container is available, and MUST be listed in `make test-e2e`. |

## Dependencies

None beyond the pinned `exasol-udf-macros` 0.24.0, which already derives the `RETURNS` shape from a
`Result<Option<T>, UdfError>` signature.

## Implementation Tasks

1. Rust entry point
   - [ ] 1.1 Add an `ENGINE_VERSION` crate constant fed by `env!("CARGO_PKG_VERSION")` and the
     `LAKEHOUSE_VERSION` entry point returning it, in `crates/lakehouse-engine/src/lib.rs`. Update
     the module header doc from two entry points to three.
   - [ ] 1.2 Add `crates/lakehouse-engine/src/lib_tests.rs` asserting `ENGINE_VERSION` is a
     non-empty plain `X.Y.Z` string, and declare the sibling test module in `lib.rs`.
   - [ ] 1.3 Add a `VERSION_SCRIPT_NAME` constant and its `RETURNS VARCHAR(100)` DDL to
     `create_schema_and_scripts` in `crates/lakehouse-engine/tests/common/e2e_harness.rs`.
   - [ ] 1.4 Add `crates/lakehouse-engine/tests/e2e_version_udf_test.rs`: call the script against
     the built `.so` and assert the returned value equals the crate version, with no CONNECTION and
     no Virtual Schema created.
   - [ ] 1.5 Add the new suite to `make test-e2e` in the `Makefile`, and assert that wiring in
     `crates/lakehouse-engine/tests/build_convention.rs`.

2. Installer script
   - [ ] 2.1 Add `ddl_version` and append its statement in `create_engine_scripts`.
   - [ ] 2.2 Replace `smoke_test_sql` and `classify_fingerprint_response` with a version-based
     smoke test: call `LAKEHOUSE_VERSION()`, extract the value (dedicated extractor — see
     Requirements), compare against `RESOLVED_ENGINE_VERSION`. Verdicts: pass (exact match),
     version-mismatch, fingerprint-mismatch, other-error.
   - [ ] 2.3 Confirm `print_next_step_template` emits no `GRANT ACCESS ON CONNECTION` line for
     `LAKEHOUSE_VERSION`.

3. Installer tests
   - [ ] 3.1 Add `LAKEHOUSE_VERSION()` response modes to the exapump stub in
     `deploy/scripts/tests/install.test.sh`: matching version, differing version, fingerprint
     mismatch, and an unrelated error.
   - [ ] 3.2 Extend the engine-scripts DDL test to four scripts, asserting the `RUST SCALAR SCRIPT
     ... RETURNS VARCHAR(100)` shape for `LAKEHOUSE_VERSION` and the absence of a CONNECTION grant
     line for it.
   - [ ] 3.3 Replace `test_fingerprint_smoke_pass_and_fail` with version smoke-test cases: pass on
     exact match, fail on version mismatch, fail on fingerprint mismatch, fail on other error.

4. Documentation
   - [ ] 4.1 Update the "What the command does" step list in `docs/install.md`: four scripts
     including `LAKEHOUSE_VERSION`, and the smoke test described as a positive version check.
   - [ ] 4.2 Add the `LAKEHOUSE_VERSION` DDL to the manual-install appendix, and correct the
     "All three scripts MUST be in the same schema" sentence: that co-location rule applies to the
     adapter, scan, and distributor scripts, which the adapter calls schema-qualified.
   - [ ] 4.3 Rewrite the smoke-test appendix around `SELECT LHVS.LAKEHOUSE_VERSION();` and its
     three outcomes, and document the standalone version query as an operator task.

## Parallelization

| Group | Tasks | Depends on | Knowledge |
|-------|-------|------------|-----------|
| A: version entry point and its installer contract | 1.1-1.5, 2.1-2.3, 3.1-3.3, 4.1-4.3 | — | spec delta `packaging/version-udf`; `crates/lakehouse-engine/src/lib.rs`, `crates/lakehouse-engine/src/lib_tests.rs`, `crates/lakehouse-engine/tests/e2e_version_udf_test.rs`, `crates/lakehouse-engine/tests/common/e2e_harness.rs`, `crates/lakehouse-engine/tests/build_convention.rs`, `Makefile`, `deploy/scripts/install.sh`, `deploy/scripts/tests/install.test.sh`, `docs/install.md` |

One group — every task implements the single `packaging/version-udf` delta.

## Dead Code Removal

| Type | Location | Reason |
|------|----------|--------|
| Function | `classify_fingerprint_response` in `install.sh` | Replaced by the version-based smoke test |
| Function | `smoke_test_sql` in `install.sh` | Replaced by the version-based smoke test |

## Verification

### Scenario Coverage

| Scenario | Test Type | Test Location | Test Name |
|----------|-----------|---------------|-----------|
| The version entry point reports the compiled engine version | Unit | `crates/lakehouse-engine/src/lib_tests.rs` | `engine_version_is_a_plain_semver_string` |
| The version entry point reports the compiled engine version | Integration | `crates/lakehouse-engine/tests/e2e_version_udf_test.rs` | `version_udf_returns_the_compiled_crate_version` |
| The smoke test verifies the deployed version matches the downloaded release | Integration | `deploy/scripts/tests/install.test.sh` | `test_version_smoke_*` (pass, mismatch, fingerprint, error) |

### Manual Testing

| Feature | Command | Expected Output |
|---------|---------|-----------------|
| packaging/version-udf | `docker compose up -d --wait exasol minio iceberg-rest && make test-e2e` | 0 failures, including the new version suite |
| packaging/version-udf | `exapump sql -d "$LH_DSN" "SELECT LHVS.LAKEHOUSE_VERSION()"` | The crate version in `crates/lakehouse-engine/Cargo.toml`, for example `0.45.0` |
| packaging/version-udf | `make test-install` | 0 failures, including the new version smoke-test cases |
| packaging/version-udf | `shellcheck -s bash deploy/scripts/install.sh deploy/scripts/tests/install.test.sh` | No findings |

### Checklist

| Step | Command | Expected |
|------|---------|----------|
| Build | `make cross-udf-build` | Exit 0 |
| Test | `cargo test` | 0 failures |
| Test (installer) | `make test-install` | 0 failures |
| Test (E2E) | `make test-e2e` | 0 failures |
| Lint | `cargo clippy --all-targets` | 0 errors and 0 warnings |
| Lint (shell) | `shellcheck -s bash deploy/scripts/install.sh deploy/scripts/tests/install.test.sh` | 0 findings |
| Format | `cargo fmt` | No changes |
