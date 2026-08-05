# Verification Report: fix-broadcast-join-per-side-storage-credentials

## Verdict

| Result | Details |
|--------|---------|
| **PASS** | Both defects are fixed and reproduced end-to-end against live Docker Exasol/Lakekeeper/MinIO: issue #294 (a broadcast join silently read the dimension side through the fact side's storage credential) and issue #303 (the broadcast renderer preserved Exasol's native `tableAlias`, breaking every aliased join before the credential path was even reached). The task 1.9 gate — which must fail with a genuine `403 Forbidden` credential error before the fix and pass after — did exactly that. A code-review pass surfaced 10 findings, including one real security bug (the join-scan redaction set covered only the fact side's secrets); all 10 are fixed and independently re-verified. |
| Code review | 10 findings — standard: 4, expert: 6 — all 10 fixed |

| Check | Status |
|-------|--------|
| Build | ✓ |
| Tests | ✓ |
| Lint | ✓ |
| Format | ✓ |
| Scenario Coverage | ✓ |
| Manual Tests | ✓ |

## Test Evidence

### Coverage

| Type | Coverage % |
|------|------------|
| Unit | Every new/changed production code path (`store_router.rs`, `object_store.rs`'s per-side rework, `JoinSpec::storage`, redaction union, alias stripping) has a dedicated unit test; no coverage percentage is tracked by this project's tooling. |
| Integration | Every plan scenario (see § Scenario Coverage below) maps to a passing integration or E2E test; the two live E2E suites (join, Lakekeeper) are the project's required cross-cutting integration gate. |

### Test Results

| Type | Run | Passed | Ignored |
|------|-----|--------|---------|
| Unit (`cargo test -p lakehouse-engine --lib`) | 782 | 782 | 0 |
| Unit (full workspace `cargo test`) | all crates (lakehouse-engine, lakehouse-catalog, vs-expression) | all green | 2 (pre-existing, unrelated) |
| Integration (`make test-e2e`, live Exasol/MinIO/iceberg-rest, fresh `.so`) | 230 (8 binaries) | 230 | 0 |
| Integration (`make test-e2e-lakekeeper`, live Exasol/MinIO/Keycloak/Lakekeeper, fresh `.so`) | 23 | 23 | 0 |

Per-binary breakdown of the 230-test `make test-e2e` run: `e2e_capability_test` 77/77, `e2e_count_distinct_test` 18/18, `e2e_int96_timestamp_test` 9/9, `e2e_join_test` 25/25 (includes the 4 tests task 1.10 repaired to genuinely exercise the broadcast path at row-fetch time), `e2e_non_ascii_identifier_test` 8/8, `e2e_positional_deletes_test` 18/18, `e2e_refresh_test` 13/13, `e2e_scan_test` 62/62.

### Manual Tests

| Test | Result |
|------|--------|
| `docker compose up ... && make test-e2e-lakekeeper` — `lakekeeper_vended_broadcast_join_result_correct` and `lakekeeper_vended_credentials_are_scoped_per_table` pass | ✓ EXIT=0, 23/23 |
| `make test-e2e` — all 18 (now 25) `e2e_join_test` tests pass, 4 of them (task 1.10) now exercising the broadcast path at row-fetch time | ✓ EXIT=0, 25/25 |
| `cargo test -p lakehouse-engine --lib golden_broadcast_join_sql_unchanged -- --nocapture` | ✓ 1/1, golden shows `"storage"` as the last key inside `"join"`, holding a credential different from the top-level `"storage"` |
| `cargo test -p lakehouse-engine --lib render_broadcast_join_strips_native_table_alias` | ✓ 3/3 (condition, filter, projection all render bare) |
| `cargo test -p lakehouse-engine --lib store_router` | ✓ 18/18, including the unroutable-path error naming the path and both roots |
| `cargo test -p lakehouse-engine --lib common_blob_wire_is_byte_stable` | ✓ 1/1, non-join wire byte-identical |
| Task 1.9 gate re-run BEFORE the fix (`lakekeeper_vended_broadcast_join_result_correct`) | ✓ Failed as required, with literal `403 Forbidden` (see decision-log.md) |
| Task 7.1 gate re-run AFTER the fix (same test) | ✓ Passed, 6/6 correct joined rows |

## Tool Evidence

### Linter

```
cargo clippy --all-targets   (workspace, final pass after all code-review fixes)
    Finished `dev` profile [unoptimized + debuginfo] target(s) in 11.45s
CLIPPY_EXIT=0 — zero warnings, zero errors
```

Independently re-verified inline during code review: the new `store_router.rs`'s `#[deny(clippy::missing_trait_methods)]` claim is real (a standalone reproduction with one method omitted fails clippy), and no `dead_code` lint fired on `PrefixRoutingObjectStore`/`RoutedSide` after their public export was removed (task fix 3).

### Formatter

```
cargo fmt --check   (workspace, final pass)
FMT_EXIT=0 — no diff
```

## Scenario Coverage

| Domain | Feature | Scenario | Test Location | Test Name | Passes |
|--------|---------|----------|---------------|-----------|--------|
| datafusion-scan | scan-execution-join | Scan reconstitutes a join scan spec carrying two file lists | `tests/scan_join_test.rs` | `join_spec_reconstitutes_two_file_lists` | Pass |
| datafusion-scan | scan-execution-join | Scan registers both tables and executes the inner equi-join | `tests/scan_join_test.rs` | `join_registers_each_side_against_its_own_backend` | Pass |
| datafusion-scan | scan-execution-join | Each join side reads its files through its own storage credential | `src/scan/object_store.rs` | `each_side_inner_store_is_built_from_its_own_backend` (mutation-tested provenance gate) | Pass |
| datafusion-scan | scan-execution-join | Each join side reads its files through its own storage credential (routing) | `tests/scan_join_test.rs` | router-level routing coverage via `store_router.rs`'s own unit tests (the file-local `FailingStub` duplicate was removed as a code-review finding) | Pass |
| datafusion-scan | scan-execution-join | Two join sides in different buckets register two separate stores | `src/scan/object_store.rs` | `join_sides_in_two_buckets_register_two_stores` | Pass |
| datafusion-scan | scan-execution-join | A requested path owned by no join side is a clear error | `src/scan/store_router.rs` | `path_outside_every_side_errors_naming_path_and_roots` | Pass |
| datafusion-scan | scan-execution-join | Scan reports a clear error when an assigned join file is unreadable, redacting BOTH sides' credentials | `tests/scan_join_test.rs` | `unreadable_join_file_error_redacts_both_sides_credentials` + the falsifiable `a_dimension_side_read_failure_redacts_the_dimension_sides_credential` (added during code review; mutation-tested RED/GREEN) | Pass |
| datafusion-scan | scan-execution-memory-and-credentials | Scan reads data files with vended credentials carried in the scan spec | `src/scan/object_store.rs` | `each_side_size_index_holds_only_its_own_files` | Pass |
| datafusion-scan | scan-execution-positional-deletes | Positional-delete files are read with the same vended credentials, dimension side included | `tests/scan_positional_deletes.rs` | dimension-side delete-file join case | Pass |
| datafusion-scan | scan-execution-spec-reconstitution | Consolidating the shard-invariant fields preserves the two-argument wire | `src/scan/spec.rs` | `join_block_round_trips_through_split_and_merge`, `common_blob_wire_is_byte_stable` | Pass |
| datafusion-scan | scan-execution-connection-concurrency | Scan configures its object store from the resolved connection budget, per side | `src/scan/object_store.rs` | `each_side_store_gets_the_full_connection_budget` | Pass |
| vs-adapter | pushdown-planning-join | Broadcast-eligible inner equi-join is planned as a broadcast fan-out, each side carrying its own storage | `src/adapter/pushdown/joins/sql_builders.rs` | `golden_broadcast_join_sql_unchanged`, `broadcast_carries_each_sides_own_storage` | Pass |
| vs-adapter | pushdown-planning-cloud-credentials | A join whose sides resolve to different storage backends is rejected at plan time; credential-only divergence is served | `src/adapter/pushdown/joins/planning.rs` | `adls_sides_on_different_storage_accounts_are_rejected`, `s3_sides_differing_only_in_credentials_carry_both_backends` | Pass |
| vs-adapter | pushdown-planning-join | Broadcast join condition, filter, and projection strip Exasol's native `tableAlias` before rendering | `src/adapter/pushdown/joins/rendering.rs` | `render_broadcast_join_strips_native_table_alias_from_condition`, `..._from_filter`, `..._from_projection` | Pass |
| e2e-harness | lakekeeper-e2e-harness | A two-table broadcast join over a vended-credential warehouse returns correct rows | `tests/e2e_lakekeeper_test.rs` | `lakekeeper_vended_broadcast_join_result_correct` (task 1.9/7.1 gate re-run) | Pass |
| e2e-harness | lakekeeper-e2e-harness | The vended credential scope divergence the defect needs is established by observation | `tests/e2e_lakekeeper_test.rs` | `lakekeeper_vended_credentials_are_scoped_per_table` (rewritten during code review to call the shipped `resolve_vended_storage` instead of reimplementing it) | Pass |

## Notes

**Two independent defects, one plan.** Issue #294 (per-side credential collapse) was the plan's original target; issue #303 (alias-qualification breaking every aliased broadcast query before the credential path was even reached) was discovered mid-investigation and folded in, because without it the reproduction gate could never observe #294 at all (see decision-log.md's "Task 1.4 follow-up investigation" and "Round 2"). Both are now fixed and both have their own GitHub issue (#303, #304) — #304 is the pre-existing, deliberately-out-of-scope multi-bucket-per-side refusal named as a tracked exception, not a new defect.

**Code review found and fixed a genuine security gap.** `run_join_scan_with_session` was redacting scan errors against only the fact side's `secret_values()`; since this plan's own fix lets the dimension side carry a different credential, a dimension-side read failure could have leaked that side's access key/secret key/session token into a surfaced error message — undermining the exact defect class this plan exists to close (decision [8]). The fix centralizes the union rule in one place (`ScanSpec::all_secret_values`) and is backed by a mutation-tested falsifiable test (RED against the old fact-side-only set, GREEN against the union).

**Two infrastructure-drift incidents during verification, both diagnosed and resolved, neither a code defect.** (1) Bringing up the base `docker-compose.yml` stack for the join-suite verification recreated the `exasol` container without the Lakekeeper overlay's network config, breaking its route to `keycloak` for a subsequent Lakekeeper run — fixed by re-applying both compose files together (`docker-compose.yml` + `docker-compose.lakekeeper.yml`), after which both suites passed against the same container. (2) An implementer-expert-agent applying code-review fixes hit a genuine ~30-minute Serena MCP tool stall (`replace_content`, regex mode) followed by a second stall on retry; the agent was stopped and the remaining mechanical steps (the dependency-cycle fix and the vended-credential-probe rewrite) were completed directly with plain Read/Edit, verified compiling, tested, and re-verified live against the Docker stack.

**All 10 code-review findings are fixed**, not merely triaged — 6 expert (the redaction bug + its falsifiable regression test, a duplicate 120-line test stub removed in favor of `store_router.rs`'s own stronger unit tests, an over-long constructor signature collapsed via an existing bundle type, a module dependency cycle broken by relocating a pure helper to the module that owns its data shape, and a test-only reimplementation of the shipped vended-credential-selection rule replaced with a call to the real `pub` resolver) and 4 standard (a redundant double alias-strip, a magic-number date bound that could silently drift from the constant it was meant to track, a duplicated redaction helper, and 4 missing doc comments on cross-binary test helpers). Every fix's outcome, with test evidence, is recorded inline in `review-findings.md`.

**Deliberate, documented deviations from the plan's literal text — none affecting correctness:**
- Prefix routing applies on every group of a join spec, including single-side groups (not only when two sides share a bucket) — this matches the plan/spec's normative "SHALL apply this routing on EVERY spec carrying a non-empty join block" language, which takes precedence over an informal implementation brief.
- A routing error surfaces as `object_store::Error::Generic` (wrapped by DataFusion), not literally `UdfError::User` as one task's prose stated — the `UdfError::User` path is unreachable from `build_session_context` because `side_size_index` rejects a malformed path first.
- `each_side_store_gets_the_full_connection_budget` cannot fully falsify a hypothetical `N / side_count` division (a built `AmazonS3` doesn't expose its pool config back out); it pins the budget value passed into each `build_side_store` call instead. Stated in the test's own doc.

**Unstudied, explicitly out of scope (per plan § Non-Goals):** whether a real client driver's own default fetch-size/row-limit attribute reproduces the same broadcast-suppression this project's own test harness had (the `resultSetMaxRows` finding from task 1.4's investigation) remains unverified in production. A live Databricks two-table vended broadcast-join E2E is deferred (decision [13]); the Lakekeeper `sts-enabled` warehouse stands in under the same per-table vending contract.

Ready for `/speq:record`.
