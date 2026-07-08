# Tasks: change-lc-rs-sdk-0-20-3

## Phase 1: Tracking issue
- [x] 1 File the GitHub tracking issue first (orchestrator: `gh issue create`) -> #75

## Phase 2: Implementation (Group A — manifest + doc edits + lock refresh)
- [x] 2 Bump exasol-udf-sdk to 0.20.3 in /Cargo.toml
- [x] 3 Bump exasol-udf-sdk + exasol-udf-macros to 0.20.3 in crates/lakehouse-engine/Cargo.toml
- [x] 4 Refresh Cargo.lock (cargo update -p exasol-udf-sdk -p exasol-udf-macros)
- [x] 5 Bump SLC_VERSION 0.20.2 -> 0.20.3 in Makefile
- [x] 6 Update exasol-udf-sdk/exasol-udf-macros version refs in CLAUDE.md "Build" section

## Phase 2: Implementation (Group A' — expert)
- [x] 7 Verify rustc fingerprint pairing for lc-rs 0.20.3 vs rust:1.94-bookworm [expert] -> MATCH, both built on rust:1.94-bookworm

## Phase 3: Verification (orchestrator-run, per plan Checklist)
- [x] 8 make cross-musl-udf-build -> exit 0, .so rebuilt (163.1M, rust:1.94-bookworm)
- [x] 9 make test-e2e -> exit 0 after fixing task 11; all suites ok (scan/capability/count_distinct/join/positional_deletes)
- [x] 10 cargo test / cargo clippy --all-targets / cargo fmt --check -> 539 passed/2 ignored, clippy clean, fmt clean

## Phase 4: Plan-gap fix (discovered during verification)
- [x] 11 Bump 5x hardcoded `const SLC_VERSION: &str = "0.20.2"` in
      crates/lakehouse-engine/tests/e2e_{scan,capability,count_distinct,join,positional_deletes}_test.rs
      to "0.20.3" — the plan only covered the Makefile's SLC_VERSION var; each E2E test file's own
      in-process `install_slc()` re-downloads/re-uploads the SLC from its OWN hardcoded const,
      independent of Makefile. Root cause of `F-UDF-CL-RUST-9001: Fingerprint mismatch: expected
      0.20.2:... found 0.20.3:...` — the harness kept installing the old SLC against the new .so.
