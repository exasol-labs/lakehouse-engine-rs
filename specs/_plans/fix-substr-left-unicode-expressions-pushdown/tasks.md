# Tasks: fix-substr-left-unicode-expressions-pushdown

## PR Lifecycle
- [x] resolved
- [x] implemented
- [x] version-bumped
- [ ] tested-green
- [ ] recorded
- [ ] pr-ready

## Phase 2: Implementation (Group A)
- [x] 2.1 Enable `unicode_expressions` on `crates/lakehouse-engine/Cargo.toml`'s datafusion dependency
- [x] 2.2 Run `git diff Cargo.lock`, confirm no change
- [x] 2.3 Correct stale namespace list in root `Cargo.toml` comment (add `workspace-arm64`)
- [x] 2.4 Add `crates/lakehouse-engine/tests/scan_substr_expression.rs` host regression test
- [x] 2.5 Assert select-list `substr(...)` evaluates correctly
- [x] 2.6 Assert filter-position `substr(...) = '<literal>'` evaluates correctly
- [x] 2.7 Assert `left(...)` still plans alongside `substr(...)`
- [x] 2.8 Start docker stack, capture pushdown payload for `LEFT(...)`, record finding in decision-log.md
- [x] 2.9 Add `e2e_substr_left_pushdown` to `crates/lakehouse-engine/tests/e2e_capability_test.rs`
- [x] 2.10 Assert pushdown SQL contains `substr(` via `explain_virtual_sql`
- [x] 2.11 Confirm new test runs under `make test-e2e`

## Phase 3: Verification
- [x] 3.1 Run build/test/e2e/lint/format/lockfile checklist
- [x] 3.2 Scenario coverage audit
- [x] 3.3 Manual verification

## Phase 4: Review Fixes
- [x] 4.1 In `crates/lakehouse-engine/tests/scan_substr_expression.rs`, trim the parenthetical from the `dummy_storage` doc comment so it reads `/// Storage props are never dialed for a local `file://` scan; a placeholder\n/// keeps the spec well-formed.`
