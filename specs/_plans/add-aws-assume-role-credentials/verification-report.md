# Verification Report: add-aws-assume-role-credentials

## Verdict

| Result | Details |
|--------|---------|
| **PASS** | An AWS-role CONNECTION assumes its role through one signed STS call per request, session credentials sign Glue/S3 access, and the scan receives them only inside the sealed envelope. All local automated checks are green. The opt-in cloud (real AWS) and `tofu plan` rows remain for a human with live AWS credentials, per the plan's own stated gate. |
| Code review | 18 findings — 18 fixed (17 standard, 1 expert) |

| Check | Status |
|-------|--------|
| Build (`make cross-udf-build`) | ✓ |
| Tests (`cargo test --workspace`) | ✓ |
| Lint (`cargo clippy --all-targets`) | ✓ |
| Format (`cargo fmt --check`) | ✓ |
| Dependencies (`cargo deny check`) | ✓ |
| Lockfile (no new `[[package]]`) | ✓ |
| E2E (`make test-e2e`) | ✓ |
| E2E Unity (`make test-e2e-unity`) | ✓ |
| Scenario Coverage | ✓ |
| Manual Tests | ✓ (local rows); cloud/AWS-provisioning rows deferred — see Notes |

## Test Evidence

### Test Results

| Type | Run | Passed | Failed | Ignored |
|------|-----|--------|--------|---------|
| Unit + integration (`cargo test --workspace`) | 1 | 1847 | 0 | 2 (pre-existing, `micro_bench.rs`, unrelated to this plan) |
| E2E (`make test-e2e`, 17 binaries incl. `e2e_assume_role_test`) | 1 | 413 | 0 | 0 |
| E2E Unity (`make test-e2e-unity`) | 1 | 29 | 0 | 0 |

### Manual Tests

| Test | Result |
|------|--------|
| connection-credentials-assume-role: role CONNECTION reads the seeded rows, stub request count grows | ✓ (proven by `role_connection_reads_the_rows_the_base_identity_was_denied`, live Docker Exasol) |
| connection-credentials-assume-role: base identity alone is denied, stub count unchanged | ✓ (`the_base_identity_alone_is_denied_the_warehouse_bucket`) |
| connection-credentials-assume-role: external id without a role is rejected | ✓ (`external_id_without_a_role_is_rejected`, unit) |
| scan-spec-credential-reference: `EXPLAIN VIRTUAL` carries the sealed envelope, no base secret or external id | ✓ (asserted inside `role_connection_reads_the_rows_the_base_identity_was_denied`) |
| assume-role-e2e: `make test-e2e` | ✓ 0 failures |
| assume-role-e2e (Unity Catalog): `make test-e2e-unity` | ✓ 0 failures, `unity_role_connection_reads_a_delta_table_through_the_session` passes |
| cloud-e2e-harness: `cargo test --features cloud-e2e cloud_assume_role` against real AWS | Not run — no AWS credentials available in this sandbox. Compiles, lints clean, and skips cleanly (naming the absent variable) with 15 passed / 0 failed under `--features cloud-e2e` with no variables exported. Per the plan's own task 4.2 gate, a human must run this with the four SSM-sourced variables exported and confirm 2 passed, 0 skipped before the PR is marked ready. |
| AWS provisioning: `tofu plan` | Not run — no `tofu`/`terraform` binary in this sandbox, and `deploy/data-stack`'s state is never applied from an agent session (S3 backend, real AWS account). The Terraform in `deploy/data-stack/*.tf` was hand-reviewed for HCL correctness in place of `tofu validate`/`tofu fmt -check`; a human should run both before applying. |

## Tool Evidence

### Linter

```
cargo clippy --all-targets: 0 warnings, 0 errors (workspace)
cargo clippy -p lakehouse-catalog -p lakehouse-engine --all-targets --features exasol-e2e,unity-e2e,cloud-e2e: clean
```

### Formatter

```
cargo fmt --all --check: no changes
```

### Dependencies

```
cargo deny check: advisories ok, bans ok, licenses ok, sources ok
git diff Cargo.lock: quick-xml 0.39.4 added as a dependency line; no new [[package]] entry
  (already resolved transitively through object_store 0.13.2, as the plan predicted)
```

## Scenario Coverage

Every scenario the plan's § Verification > Scenario Coverage lists has a corresponding test
function present in the tree, and every one of those tests passed in the runs above (0 failures
in any suite that contains them). A handful of test names changed during the Phase 4 review-fix
pass; the plan's Scenario Coverage table and this plan's `catalog-crate-public-surface-extensions`
spec delta were updated in place to track the renames (see `review-findings.md` items 4.6-4.9,
4.13-4.14, 4.20). The two opt-in cloud scenarios (`cloud_assume_role_reaches_glue_and_s3_through_the_role`,
`cloud_assume_role_base_identity_alone_is_denied`) exist, compile, and skip cleanly without AWS
credentials; they have not run against real AWS in this session.

| Domain | Feature | Scenario | Test Location | Passes |
|--------|---------|----------|---------------|--------|
| lakehouse-catalog | connection-credentials-assume-role | Validation, request/response shape, endpoint resolution, failure redaction | `crates/lakehouse-catalog/src/{creds_tests,sts_tests,session_tests}.rs` | Pass |
| lakehouse-engine adapter | connection-credentials-assume-role | Once-per-request resolution at both entry points, sealed transport, vending interaction | `crates/lakehouse-engine/src/adapter/{connection_tests,adapter_tests}.rs`, `crates/lakehouse-engine/src/adapter/pushdown/{support_tests,pushdown_tests}.rs` | Pass |
| lakehouse-engine adapter | connection-credentials-direct-storage | A direct-storage role CONNECTION seals its storage block | `crates/lakehouse-engine/src/adapter/pushdown/pushdown_tests.rs` | Pass |
| lakehouse-engine adapter | delta format reader | A role leaves the vended path unchanged | `crates/lakehouse-engine/src/adapter/pushdown/format/delta_format_reader_tests.rs` | Pass |
| lakehouse-catalog | catalog-crate-public-surface-extensions | `resolve_aws_identity` and `ConnectionCreds::assume_role_arn()` are public, reviewed surface | `crates/lakehouse-catalog/tests/catalog_public_surface.rs` | Pass |
| e2e-harness | assume-role-e2e (Iceberg REST + direct storage) | Base denial, role success, EXPLAIN VIRTUAL sealed envelope, wrong external id, wrong base secret, direct-storage role read | `crates/lakehouse-engine/tests/e2e_assume_role_test.rs` | Pass |
| e2e-harness | assume-role-e2e (Unity Catalog) | Role CONNECTION reads a Delta table through the session | `crates/lakehouse-engine/tests/e2e_unity_test.rs` | Pass |
| e2e-harness | cloud-e2e-harness | Real AWS STS/Glue/S3 through an assumed role; base identity denied by Glue | `crates/lakehouse-engine/tests/cloud_e2e_test.rs` | Compiles, lints, skips cleanly; not run against real AWS |
| build convention | assume-role suite wiring | `make test-e2e`/`test-e2e-unity`/CI start the STS stub and run the assume-role binary | `crates/lakehouse-engine/tests/build_convention.rs` | Pass |

## Notes

- **Stack setup gap found and fixed during verification, not a regression.** The Docker stack left
  running by the implementation sub-agents had not gone through the full CI bring-up sequence: it
  was missing the `spark-iceberg-fixtures` one-shot fixture-authoring job and had no
  `LH_EXASOL_CPUSET` constraint. The first `make test-e2e` run under this gap failed 2 pre-existing,
  unrelated tests (`e2e_int96_timestamp_test`'s far-future fixture, missing because
  `spark-iceberg-fixtures` never ran) and, after that was fixed, 1 more
  (`adapter_detects_container_cpuset`, which explicitly panics with "PRECONDITION UNMET" rather
  than silently passing when the container's CPU set is not narrower than the host's). Neither
  failure touches assume-role code. Tearing the stack down and rebuilding it through the full CI
  sequence (`minio-init` → `exasol`/`minio`/`iceberg-rest`/`sts-stub` → `spark-iceberg-fixtures`,
  with `LH_EXASOL_CPUSET=0-1`) resolved both, and the full local suite (`make test-e2e` and
  `make test-e2e-unity`) is now green.
- **`cargo-deny` and `tofu`/`terraform` are not preinstalled in this sandbox.** `cargo-deny` was
  installed for this session (`cargo install cargo-deny --locked --version 0.19.9`) and the check
  now runs clean. No OpenTofu/Terraform binary was available, so `deploy/data-stack/*.tf` was
  reviewed by hand instead of `tofu validate`/`tofu fmt -check`; a human should run both before
  `tofu apply`, per `CLAUDE.md`'s deployment-state rules (S3 backend, real AWS account, never
  applied from an agent session).
- **Two rows are intentionally not run here**: the cloud E2E suite against real AWS
  (`cloud_assume_role_*`) and `tofu plan` for `deploy/data-stack`. Both need live AWS access this
  sandbox does not have. The plan's own task 4.2 states the PR is not marked ready until the cloud
  row has run and reported 2 passed, 0 skipped — that step is for the human merging this PR.
- Implementation spanned five sub-agent groups (catalog-crate AWS identity; adapter identity wiring
  and sealed transport; local E2E; cloud E2E and AWS provisioning; documentation), each verified
  independently, followed by one code-review pass (18 findings) and one consolidated fix pass (all
  18 fixed, re-verified green).
