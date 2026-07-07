# Verification Report: change-lc-rs-sdk-0-20-3

**Generated:** 2026-07-07

## Verdict

| Result | Details |
|--------|---------|
| **PASS** | lc-rs / exasol-udf-sdk / exasol-udf-macros / SLC bumped 0.20.2 -> 0.20.3; SLC/`.so` fingerprint pairing proven end-to-end via `make test-e2e`; no behavioral change |

| Check | Status |
|-------|--------|
| Build (`make cross-musl-udf-build`) | ✓ |
| Tests (`cargo test`, host) | ✓ |
| Tests (`make test-e2e`) | ✓ |
| Lint (`cargo clippy --all-targets`) | ✓ |
| Format (`cargo fmt --check`) | ✓ |
| Scenario Coverage | ✓ (no new/changed scenarios — see plan §Verification) |
| Manual Tests | ✓ |

## Test Evidence

### Test Results

| Type | Run | Passed | Ignored |
|------|-----|--------|---------|
| Unit (host `cargo test`) | 541 | 539 | 2 |
| E2E (`make test-e2e`, local Exasol Docker) | 74 (5 binaries: scan/capability/count_distinct/join/positional_deletes) | 74 | 0 |

### Manual Tests

| Test | Result |
|------|--------|
| lc-rs 0.20.3 SLC/SDK pairing loads (`make test-e2e`) | ✓ — after fixing a plan gap (see Notes), all E2E suites green, no `F-UDF-CL-RUST-9001` |
| SLC version registered (`install-slc` / in-harness `install_slc()` output) | ✓ — downloads and registers `lc-rust-0.20.3.tar.gz` under the `RUST` alias |

## Tool Evidence

### Linter

```
cargo clippy --all-targets: No issues found
```

### Formatter

```
cargo fmt --check: no diff, exit 0
```

## Scenario Coverage

| Domain | Feature | Scenario | Test Location | Test Name | Passes |
|--------|---------|----------|---------------|-----------|--------|
| _(none — no spec delta; pure dependency bump)_ | — | — | Existing E2E suite via `make test-e2e` | all existing E2E tests | Pass |

## Notes

- **Plan gap found and fixed during verification.** `make test-e2e` initially failed with
  `F-UDF-CL-RUST-9001: Fingerprint mismatch: expected 0.20.2:... found 0.20.3:...`. Root
  cause: each of the 5 E2E test files carries its own hardcoded
  `const SLC_VERSION: &str = "0.20.2"` (independent of the `Makefile`'s `SLC_VERSION` var),
  used by that file's in-process `install_slc()` helper — the plan's task 5 only covered
  the `Makefile` variable. Fixed by bumping all 5 consts to `"0.20.3"`; re-ran `make
  test-e2e` clean twice. See decision-log.md for the full writeup.
- **Code review**: one fix applied (4 stale `0.20.2` narrative comments in
  `e2e_scan_test.rs`, comment-only, no rebuild needed); one deliberate follow-up deferred
  (deduplicating the 5x `SLC_VERSION` const into `tests/common/mod.rs` — out of scope for
  a pure version-bump PR, tracked in decision-log.md).
- **Fingerprint research (task 7, expert)**: confirmed MATCH — lc-rs v0.20.3's release
  Dockerfile/CI both build on `rust:1.94-bookworm`, identical to this repo's UDF builder
  image. No CLAUDE.md or builder-image change needed.
- Version bumped `0.24.0` -> `0.24.1` (patch — dependency/tooling bump, no API/ABI/behavior
  change from this repo's perspective).
