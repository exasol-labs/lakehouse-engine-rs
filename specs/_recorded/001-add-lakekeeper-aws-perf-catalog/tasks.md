# Tasks: add-lakekeeper-aws-perf-catalog

## Phase 2: Implementation (Group A)
- [x] 1.1 `deploy/lakekeeper-stack/providers.tf`, `variables.tf`, `locals.tf`
- [x] 1.2 `deploy/lakekeeper-stack/main.tf`
- [x] 1.3 `deploy/lakekeeper-stack/outputs.tf`
- [x] 4.1 `bench/run.sh` — BENCH_CATALOG dispatch

## Phase 2: Implementation (Group B)
- [x] 1.4 `deploy/lakekeeper-stack/lakekeeper-userdata.sh.tftpl` — boot-and-fetch half
- [x] 4.2 `bench/run.sh` selftest additions

## Phase 2: Implementation (Group C)
- [x] 1.5 `deploy/lakekeeper-stack/lakekeeper-userdata.sh.tftpl` — compose-and-wait half
- [x] 2.1 `deploy/scripts/lakekeeper-provision.sh` — source half
- [x] 4.3 `deploy/scripts/secrets.sh`

## Phase 2: Implementation (Group D)
- [x] 2.2 `deploy/scripts/lakekeeper-provision.sh` — target half [expert]
- [x] 3.2 `deploy/scripts/lakekeeper-down.sh`

## Phase 2: Implementation (Group E)
- [x] 3.1 `deploy/scripts/lakekeeper-up.sh`
- [x] 5.2 `deploy/scripts/tests/lakekeeper-local.test.sh` [expert]

## Phase 2: Implementation (Group D2)
- [x] 2.3 `deploy/scripts/lakekeeper-provision.sh` — register-outcome read-back fix (decision [29]) [expert]

## Phase 2: Implementation (Group F)
- [x] 5.1 `deploy/scripts/tests/lakekeeper.test.sh`
- [x] 5.3 `Makefile` wiring
- [x] 6.1 `deploy/README.md` — Lakekeeper section + demo runbook
- [x] 6.2 `bench/README.md`, `bench/.env.example`
- [x] 6.3 `deploy/DEMO.md` — standalone presenter script

## Phase 4: Code Review
- [x] 4.review Review all changed files — 21 findings (18 standard, 3 expert), all fixed

## Phase 4: Review Fixes (Expert)
<!-- Indices are 4.e1.. rather than 4.1.. because Phase 2 already owns task ids 4.1-4.3. -->
- [x] 4.e1 `deploy/lakekeeper-stack/outputs.tf` + `deploy/scripts/lakekeeper-up.sh` — publish `data_ssm_root` from the already-declared `data.terraform_remote_state.data`, read `DATA_SSM` from that stack output instead of composing `/<project>/$ENV`, delete the now-wrong comment, and add `test_up_reads_the_data_stack_ssm_root_from_the_stack_output` to `deploy/scripts/tests/lakekeeper.test.sh` [expert]
- [x] 4.e2 `bench/run.sh` — make the config file overridable via `BENCH_ENV_FILE`, gate the sourcing block on its existence, isolate the three `bench_catalog_selection` subprocesses with `BENCH_ENV_FILE=/dev/null` plus `env -u` of every remote variable, add a fourth assertion covering the new knob, and document it in `bench/.env.example` [expert]
- [x] 4.e3 `deploy/scripts/tests/lakekeeper.test.sh` — extend the `tofu` stub for `apply`/`destroy` recording and `output -raw`, add `test_up_applies_only_this_stack_and_waits_for_health` and `test_down_destroys_only_the_lakekeeper_workspace`, rename the five diverging test functions to the plan's § Scenario Coverage names, and split the AWS-credential-chain assertions into `test_provision_uses_only_the_aws_credential_chain_and_an_explicit_region` [expert]

## Phase 4: Review Fixes (Standard)
<!-- Indices are 4.sN.. rather than 4.1.. because Phase 2 already owns 4.1-4.3 and the expert fixes own 4.e1-4.e3. -->
- [x] 4.s1 `bench/run.sh` — [SHRINKABLE] fix `catalog_header_field` newline loss at the report header call site and add a selftest assertion covering the blank separator line for both `remote` and `docker` targets
- [x] 4.s2 `Makefile` — [SUPPRESSED_WARNING] rewrite `lint-lakekeeper-scripts` as if/else so a shellcheck failure propagates instead of being swallowed by `||`; fix the unquoted `$buckets`/`$missing` expansions the run then reports
- [x] 4.s3 `deploy/scripts/lakekeeper-provision.sh` — [CONTEXTLESS_ERROR] `derive_bucket_and_prefix` must reject a non-`s3://…/` table location by name and location instead of silently dropping it from the bucket set
- [x] 4.s4 `deploy/scripts/lakekeeper-provision.sh` — [AMBIENT_STATE_READ] pass the namespaces/register URIs as explicit parameters to `create_namespace`, `register_table`, and `confirm_registered_metadata_location` instead of reading run-computed globals
- [x] 4.s5 `deploy/scripts/lakekeeper-provision.sh` — [MIXED_ABSTRACTION_LEVEL] extract `register_all_tables` and `report_registration_summary` out of the Run section so it reads as a sequence of step calls
- [x] 4.s6 `deploy/lakekeeper-stack/lakekeeper-userdata.sh.tftpl` — [SWALLOWED_ERROR] check the exit status of `apt-get update`/`install`, `systemctl enable --now docker`, and the realm-export `aws s3 cp`, plus assert the fetched realm file is non-empty
- [x] 4.s7 `deploy/scripts/lakekeeper-up.sh` — [CONTEXTLESS_ERROR] install an EXIT trap after `tofu apply` that reminds the operator the box is billing and names `lakekeeper-down.sh $ENV`, and add the same reminder to the health-poll error message
- [x] 4.s8 `deploy/scripts/tests/lakekeeper.test.sh` — [UNUSED_VARIABLE] delete the dead `REPO_ROOT` assignment
- [x] 4.s9 `deploy/scripts/tests/lakekeeper.test.sh` — [SUPPRESSED_WARNING] narrow the file-scope shellcheck disable to just SC2030/SC2031 and move a scoped SC2034 disable to the `local TPL_*` block it actually covers
- [x] 4.s10 `deploy/scripts/tests/lakekeeper.test.sh` — [UNTESTED_ERROR_PATH] add and register 6 new test functions covering missing required env var, unknown CLI argument, reserved target namespace, mixed-bucket rejection, empty derived key prefix, and warehouse read-back prefix disagreement
- [x] 4.s11 `deploy/scripts/tests/lakekeeper.test.sh` — [MISSING_BOUNDARY_TEST] extend `test_provision_request_bodies_shape` with aws-flavor assertions and add tests for the s3-compat storage-profile branch, multi-depth prefix derivation, and the single-table prefix boundary
- [x] 4.s12 `deploy/README.md` — [OUTDATED_COMMENT] add the missing one-time `tofu init` to the quick-start block plus the cluster-stack prerequisite sentence
- [x] 4.s13 `deploy/README.md` — [OUTDATED_COMMENT] add the missing `tofu workspace new/select` step to the unwrapped runbook's `cluster-stack` apply
- [x] 4.s14 `deploy/README.md` — [OUTDATED_COMMENT] fix the "four-service" service count/name mismatch and add the cluster-stack `tofu apply` to the bench-remote.sh chain description
- [x] 4.s15 `deploy/README.md` + `deploy/DEMO.md` — [OUTDATED_COMMENT] prefix the teardown and unwrapped setup lines with `AWS_PROFILE=<project>-deployer`
- [x] 4.s16 `deploy/README.md` — [OUTDATED_COMMENT] add `DEMO.md` to the Files list and link to it from the Demo-runbook heading
- [x] 4.s17 `deploy/README.md` + `bench/README.md` — [OUTDATED_COMMENT] rejoin the line-wrapped inline code spans and hyphenated word so no backticked path renders with an embedded space
- [x] 4.s18 `bench/README.md` — [OUTDATED_COMMENT] fix the `remote` mode bullet to describe Glue-or-Lakekeeper instead of Glue-only, and reword the malformed `BENCH_CATALOG` lead-in sentence
- [x] 4.s19 `deploy/DEMO.md` — [UNMEASURED_OPTIMIZATION] replace the unverified sub-second latency claim with a mechanism-only claim and fix the row-count mismatch (180M vs "hundreds of millions")

## Phase 5: Verification
- [x] 5a Automated checklist — all 8 items pass (Rust untouched, `.so` build, `cargo test`, offline harness 159/159, bench selftest, local docker integration 86/86, `tofu validate`/`fmt`, shellcheck 0 errors)
- [x] 5b Scenario coverage audit — all 12 scenarios map to a real, passing test
- [x] 5c Manual testing — all rows pass, including real AWS (see Phase 7 for two bugs found and fixed along the way); resources confirmed fully torn down after each run

## Phase 6: Verification Report
- [x] 6.report Generate verification-report.md

## Phase 7: Live Verification Fixes
<!-- Discovered by a real `tofu apply` against real AWS during manual testing (Phase 5c), not by
     `tofu validate`. See decision-log.md § Review Findings, [live-verification]. -->
- [x] 7.1 `deploy/lakekeeper-stack/providers.tf`, `deploy/lakekeeper-stack/main.tf` — fix `aws_s3_object.keycloak_realm` PutObject 400 (S3 object tag limit is 10, `default_tags` merges 11) by adding an aliased `aws` provider with no `default_tags` and routing only that one resource to it via `provider = aws.no_default_tags`
- [x] 7.2 `deploy/lakekeeper-stack/lakekeeper-userdata.sh.tftpl` — fix `apt-get install` failure (`E: Unable to locate package docker-compose-plugin`) by replacing the Docker-official-repo package name `docker-compose-plugin` with Ubuntu noble's own default-repo package `docker-compose-v2` in the boot-and-fetch install line and its paired error message
- [x] 7.3 `deploy/scripts/tests/lakekeeper.test.sh` — `test_realm_s3_key_outside_tpch_prefix` hardcoded exact `key    = "..."` whitespace, which 7.1's added `provider = aws.no_default_tags` attribute shifted via `tofu fmt`'s realignment (longer attribute name widens the gutter). Loosened both assertions to match the quoted S3 key value alone, independent of surrounding attribute alignment
