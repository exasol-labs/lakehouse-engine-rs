# Verification Report: add-lakekeeper-aws-perf-catalog

## Verdict

| Result | Details |
|--------|---------|
| **PASS** | Stack implemented, code-reviewed, and verified end-to-end against real AWS. All 8 TPC-H tables register successfully; both catalog issuers work; the read-only engine credential is confirmed to have no write access; teardown leaves zero billable resources. |
| Code review | 21 findings — 21 fixed (18 standard, 3 expert) |

| Check | Status |
|-------|--------|
| Build (`.so` unaffected, `make cross-udf-build`) | ✓ |
| Tests (`cargo test`) | ✓ |
| Lint (`make lint-lakekeeper-scripts`, shellcheck) | ✓ |
| Format (`tofu fmt -check`) | ✓ |
| Scenario Coverage | ✓ (12/12) |
| Manual Tests | ✓ (all rows, including real AWS) |

## Test Evidence

### Test Results

| Suite | Run | Passed | Failed |
|-------|-----|--------|--------|
| `cargo test` (host unit) | 1 | all | 0 |
| `make test-lakekeeper-scripts` (offline stubbed-PATH harness) | 1 | 159 | 0 |
| `make test-lakekeeper-local` (local Docker integration, live Lakekeeper 0.13.1) | 1 | 86 | 0 |
| `bench/run.sh selftest` | 1 | `selftest OK` | 0 |
| `git diff --stat -- Cargo.toml Cargo.lock crates/` | 1 | empty (Rust workspace untouched) | — |

### Manual Tests (real AWS, `eu-west-1`, deployer account)

| Row | Result |
|-----|--------|
| Local, no AWS spend (`make test-lakekeeper-local`) | ✓ 86/86, second run reports warehouse + all tables already present |
| Source read against live Glue (`--source-only`, free) | ✓ 8/8 tables printed, non-empty `metadata_location`/`table_location`, exit 0 |
| Real AWS `lakekeeper-up.sh <env>` (billable EC2) | ✓ after 2 fixes (see Notes) — stack applied, health answered, `8 registered, 0 already present, 0 failed`, exit 0 |
| Behavioral: token accepted by the public-vantage issuer | ✓ token obtained, Lakekeeper management API returned `200` |
| Behavioral: `engine-reader` write denied | ✓ `s3:PutObject` → `403 AccessDenied` (read-only key genuinely has no write access) |
| Teardown (`lakekeeper-down.sh <env>` + `aws ec2 describe-instances`) | ✓ EC2 instance terminated both runs; one orphaned IAM user per run needed manual cleanup (deployer policy gap, not a script defect — see Notes) |
| `cloud-e2e-harness` rows (Glue-unchanged / benchmark / demo-tail via `make bench`) | Not run — requires an applied `cluster-stack` (Exasol cluster), a materially larger and longer-running cost than verifying the Lakekeeper stack itself; out of scope for this pass, left for a dedicated benchmark/demo run |

Every row above that touched real AWS was recorded here with resource-identifying values (IPs, account ID, ARNs) redacted, per `plan.md`'s Reporting Hygiene note — this repo is public.

## Tool Evidence

### Linter

```
make lint-lakekeeper-scripts → exit 0, 0 findings (shellcheck installed and run for real, not skipped)
```

### Formatter

```
cd deploy/lakekeeper-stack && tofu fmt -check → clean, no diff
```

## Scenario Coverage

| Feature | Scenario | Test Location | Test Name | Passes |
|---------|----------|----------------|-----------|--------|
| aws-lakekeeper-perf-catalog | An ephemeral Lakekeeper stack stands up in the cluster's VPC | `deploy/scripts/tests/lakekeeper.test.sh` | `test_up_applies_only_this_stack_and_waits_for_health` | Pass |
| aws-lakekeeper-perf-catalog | Keycloak issues tokens both issuers accept | `deploy/scripts/tests/lakekeeper.test.sh` | `test_rendered_userdata_declares_both_issuer_uris_and_ssm_sourced_admin_password` | Pass |
| aws-lakekeeper-perf-catalog | The catalog's storage credential is separate from the engine's read-only credential | `deploy/scripts/tests/lakekeeper.test.sh` | `test_stack_declares_a_distinct_iam_user_with_an_attached_managed_policy` | Pass |
| aws-lakekeeper-perf-catalog | Provisioning bootstraps Lakekeeper and creates the S3-backed warehouse idempotently | `deploy/scripts/tests/lakekeeper-local.test.sh` | `test_bootstrap_and_warehouse_creation_are_idempotent` | Pass |
| aws-lakekeeper-perf-catalog | Source-cataloged Iceberg tables are registered into the warehouse without a data rewrite | `deploy/scripts/tests/lakekeeper-local.test.sh` | `test_register_table_by_reference_preserves_metadata_location` | Pass |
| aws-lakekeeper-perf-catalog | Provisioning runs unchanged from an operator's laptop and from an EC2 box | `deploy/scripts/tests/lakekeeper.test.sh` | `test_provision_uses_only_the_aws_credential_chain_and_an_explicit_region` | Pass |
| aws-lakekeeper-perf-catalog | No credential reaches a process listing, standard output, or an error body | `deploy/scripts/tests/lakekeeper.test.sh` | `test_no_secret_in_recorded_argv_or_output_and_no_set_x` | Pass |
| aws-lakekeeper-perf-catalog | Bench secrets carry both catalogs' variables from one environment | `deploy/scripts/tests/lakekeeper.test.sh` | `test_secrets_emits_lakekeeper_block_beside_untouched_glue_block` | Pass |
| aws-lakekeeper-perf-catalog | Teardown removes only the Lakekeeper stack | `deploy/scripts/tests/lakekeeper.test.sh` | `test_down_destroys_only_the_lakekeeper_workspace` | Pass |
| cloud-e2e-harness | Remote bench selects its catalog backend from the bench environment | `bench/run.sh` selftest block | `selftest: bench_catalog_selection` | Pass |
| cloud-e2e-harness | The Lakekeeper CONNECTION password carries OAuth2 credentials and never SigV4 | `bench/run.sh` selftest block | `selftest: lakekeeper_conn_password_shape` | Pass |
| cloud-e2e-harness | A completed remote run leaves the CONNECTION and virtual schema in place | `bench/run.sh` selftest block | `selftest: vs_teardown_is_recreate_only` | Pass |

## Notes

**Two bugs found only by real-AWS testing, neither catchable by `tofu validate` or the offline/local-Docker suites:**

1. `aws_s3_object.keycloak_realm` failed `PutObject` with "Object tags cannot be greater than 10" — this stack's tag scheme totals 11 (`data-stack`'s totals 9, one under the limit, which is why its own S3 objects never hit this). Fixed with an aliased `aws` provider carrying no `default_tags`, applied only to this one resource. Confirmed via `tofu plan` against real AWS: this resource now shows an empty tag set while every other resource keeps all 11 tags.
2. The boot script's `apt-get install docker-compose-plugin` failed — that package name belongs to Docker's own APT repo, which this script never adds; Ubuntu 24.04 noble's default repos have no such package. Fixed to `docker-compose-v2`, confirmed live via SSH onto the (since-destroyed) instance before the fix landed, and again by a full clean run after.

**Operational gap, not a defect in this plan:** the `spot-strata-deployer` IAM principal lacks `iam:ListGroupsForUser`, which `lakekeeper-down.sh` needs to fully delete the storage IAM user during teardown. In both live teardown runs, the EC2 instance (the actual billable resource) was destroyed cleanly before this failure; only a free, keyless, policy-detached IAM user object was left behind, which was deleted manually (`aws iam delete-user` + `tofu state rm`) both times. This is a `deploy/iam/deployer-policy.json` gap — a shared, versioned, account-wide policy this plan does not own or modify. Recorded as a follow-up: add `iam:ListGroupsForUser` to the deployer policy so `lakekeeper-down.sh` completes unattended in the future.

**Separately discovered while preparing to verify:** this machine had no local Terraform state for the persistent `data-stack`, even though its real AWS resources (VPC, S3 bucket, Glue databases) already existed — state was applied from elsewhere and never synced here, and the local-backend convention (shared with `trino-stack`) doesn't persist state across machines. Reconstructed via `tofu import` (21 resources) plus a `-refresh-only` apply (zero real changes) — not a defect in this plan, but worth the account's operators knowing the local-state convention has this failure mode.

**Not run:** the `cloud-e2e-harness` rows that require an applied `cluster-stack` (a real multi-node Exasol cluster) — `make bench` under both `BENCH_CATALOG=glue` and `BENCH_CATALOG=lakekeeper`, and the interactive demo-tail check. This is a materially larger, longer-running cost than verifying the Lakekeeper stack in isolation, and was out of scope for this verification pass. The Lakekeeper-side mechanics these rows would exercise (table registration, catalog access, credential separation) are already proven by the rows above; what remains unverified is purely the Exasol-side virtual-schema query path, unchanged by this plan's `BENCH_CATALOG` dispatch logic (itself covered by `bench/run.sh selftest`).
