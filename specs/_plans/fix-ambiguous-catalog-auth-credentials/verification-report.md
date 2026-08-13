# Verification Report: fix-ambiguous-catalog-auth-credentials

## Verdict

| Result | Details |
|--------|---------|
| **PASS** | Rule 6 rejects a token beside a complete OAuth2 pair under both catalog kinds; one classifier (`ConnectionCreds::supplied_catalog_auth`) now owns the mode decision for all three consumers, including the grant function a first review pass missed. Build, full test suite, E2E, lint, and format all green. |
| Code review | 5 findings — 5 fixed (1 expert, 4 standard) |

| Check | Status |
|-------|--------|
| Build | ✓ (`make cross-musl-udf-build`, exit 0) |
| Tests | ✓ (`cargo test`, 0 failures) |
| E2E | ✓ (`make test-e2e`, 254 passed, 0 failed) |
| Lint | ✓ (`cargo clippy --workspace --all-targets --all-features -- -D warnings`, 0 warnings) |
| Format | ✓ (`cargo fmt --check`, no diffs) |
| Scenario Coverage | ✓ |
| Manual Tests | ✓ |

## Test Evidence

### Test Results

| Type | Run | Passed | Failed |
|------|-----|--------|--------|
| Unit (`cargo test`, workspace) | full workspace | 990+ across all crates (`lakehouse-catalog` lib 159, `lakehouse-engine` lib 806, plus doc/integration binaries) | 0 |
| E2E (`make test-e2e`, `exasol-e2e` feature) | 9 test binaries against a live Docker stack | 254 | 0 |

### Manual Tests

| Test | Result |
|------|--------|
| `cargo test -p lakehouse-engine --lib adapter::connection` | ✓ |
| `cargo test -p lakehouse-catalog --lib creds` | ✓ |
| `cargo test -p lakehouse-catalog --lib auth::` | ✓ |
| `cargo test -p lakehouse-catalog --lib unity::auth` | ✓ |
| `grep -rn 'fn non_empty' crates/lakehouse-catalog/src/` → exactly one line, `creds.rs` | ✓ |
| `grep -rn 'pub enum SuppliedCatalogAuth\|pub fn supplied_catalog_auth' crates/lakehouse-catalog/src/` → no output | ✓ |
| `docker compose up -d --wait` (isolated stack) then `make test-e2e` | ✓ — `create_vs_ambiguous_catalog_auth_errors_no_secret` passes; error names all three fields, leaks none of the three sentinel values |
| Python sweep for any fixture supplying both `token` and `client_id` | ✓ — no output (no such fixture remains) |

## Tool Evidence

### Linter

```
cargo clippy --workspace --all-targets --all-features -- -D warnings
Finished `dev` profile [unoptimized + debuginfo] target(s) — 0 warnings, 0 errors
```

### Formatter

```
cargo fmt --check
(no output — no diffs)
```

## Scenario Coverage

| Domain | Feature | Scenario | Test Location | Test Name | Passes |
|--------|---------|----------|---------------|-----------|--------|
| vs-adapter | connection-credentials-catalog-auth | A CONNECTION supplying both a static token and OAuth2 client credentials is rejected | `crates/lakehouse-engine/src/adapter/connection_tests.rs` | `token_with_complete_oauth_pair_is_rejected_under_both_kinds` | Pass |
| vs-adapter | connection-credentials-catalog-auth | A CONNECTION supplying both a static token and OAuth2 client credentials is rejected | `crates/lakehouse-engine/tests/e2e_scan_test.rs` | `create_vs_ambiguous_catalog_auth_errors_no_secret` | Pass |
| vs-adapter | connection-credentials-catalog-auth | One classifier decides the catalog-auth mode and every consumer reads it | `crates/lakehouse-catalog/src/creds_tests.rs` | `supplied_catalog_auth_names_one_mode_per_field_shape` | Pass |
| vs-adapter | connection-credentials-catalog-auth | One classifier decides the catalog-auth mode and every consumer reads it | `crates/lakehouse-catalog/src/auth_tests.rs`, `crates/lakehouse-catalog/src/unity/auth_tests.rs` | `resolve_catalog_auth_is_unauthenticated_for_the_validation_rejected_shape`, `inject_catalog_auth_props_injects_nothing_for_the_validation_rejected_shape`, `resolve_unity_auth_is_unauthenticated_for_the_validation_rejected_shape` | Pass |
| vs-adapter | connection-credentials | Credential validation is parameterized by the resolved catalog kind | `crates/lakehouse-engine/src/adapter/connection_tests.rs` | `validation_is_parameterized_by_catalog_kind`, `token_with_complete_oauth_pair_is_rejected_under_both_kinds` | Pass |
| vs-adapter | connection-credentials | A Unity Catalog CONNECTION reuses the existing auth fields without a new credential field | `crates/lakehouse-engine/src/adapter/connection_tests.rs` | `unity_connection_reuses_existing_auth_fields`, `token_with_complete_oauth_pair_is_rejected_under_both_kinds` | Pass |
| vs-adapter | rest-catalog-oauth-auth | Static bearer token is attached to unsigned catalog requests | `crates/lakehouse-catalog/src/auth_tests.rs` | `build_rest_catalog_sets_token_prop`, `inject_catalog_auth_props_injects_nothing_for_the_validation_rejected_shape` | Pass |
| vs-adapter | rest-catalog-oauth-auth | OAuth2 client credentials drive the catalog client-credentials grant | `crates/lakehouse-catalog/src/auth_tests.rs` | `build_rest_catalog_sets_credential_and_oauth_props`, `resolve_catalog_auth_is_unauthenticated_for_the_validation_rejected_shape` | Pass |
| vs-adapter | unity-catalog-auth | A personal access token is applied as the bearer verbatim | `crates/lakehouse-catalog/src/unity/auth_tests.rs` | `pat_is_applied_as_bearer_verbatim`, `resolve_unity_auth_is_unauthenticated_for_the_validation_rejected_shape` | Pass |
| vs-adapter | unity-catalog-auth | OAuth machine-to-machine mints a bearer token via the client-credentials grant | `crates/lakehouse-catalog/src/unity/auth_tests.rs` | `oauth_m2m_mints_bearer_via_client_credentials` | Pass |
| vs-adapter | catalog-crate-structure | The crate exposes the concept-level API and hides every mechanism step | `crates/lakehouse-catalog/tests/catalog_public_surface.rs` | whole file (unchanged) | Pass |

## Notes

- **Plan-directed deviation, surfaced and verified.** Task 3.2's implementer found that the plan's § Test Disposition wrongly marked `build_rest_catalog_sets_credential_and_oauth_props` as UNCHANGED: its case (c) supplied a token beside a complete pair and asserted "credential wins" — the exact silent precedence issue #331 reports. That case was deleted (cases (a), (b), (d) kept, (d) relabelled (c)); the token+pair input it used to cover is now owned by `inject_catalog_auth_props_injects_nothing_for_the_validation_rejected_shape` with the corrected expectation. No coverage was lost.
- **Code review found a third, previously-undetected copy of the mode decision.** `oauth2_client_credentials_grant` re-derived pair-completeness via two `non_empty(...).ok_or_else(...)` calls even after tasks 3.2/3.3 routed the two callers through the classifier. Fixed by matching on `creds.supplied_catalog_auth()` there too, closing the last back door the plan's own Consequences table didn't anticipate. A new unit test, `oauth2_grant_errors_for_the_validation_rejected_shape`, pins the refusal against a transport error so the two failure modes can't be confused.
- **E2E evidence gathering required isolating this session's Docker stack.** A sibling worktree (`lakehouse-engine-rs-2`) was running its own instance of this repo's Docker Compose stack, bound to the same hardcoded network name (`lakehouse-engine`) and the same default host ports as this checkout — a pre-existing gap in `docker-compose.yml` (ports are env-overridable; the network name/subnet/static IPs are not, despite a comment claiming otherwise). The first `make test-e2e` run unintentionally reused that shared stack and was cut short when the other session tore it down mid-run. Final E2E evidence (254 passed, 0 failed) is from a fully isolated stack for this session (distinct Compose project name, distinct network name, distinct host ports), built and torn down without touching the sibling stack. This is a pre-existing repo-infrastructure gap, out of scope for this plan; not filed as an issue at the user's direction for this session.
- One E2E rerun hit two transient failures (`e2e_int96_far_future_timestamp_scans_without_overflow`, `e2e_int96_fixture_present_and_int96_encoded`) from a one-shot Spark fixture container ("`spark-iceberg-fixtures`", unrelated to this plan) not yet visible to the REST catalog when the suite started immediately after `docker compose up --wait` returned. Confirmed via the fixture container's own logs (exit 0, all four fixture scripts completed) and a direct REST-catalog query that the table existed moments later; both tests pass on rerun. Unrelated to any file this plan touches.
