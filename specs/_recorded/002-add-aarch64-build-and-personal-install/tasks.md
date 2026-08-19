# Tasks: add-aarch64-build-and-personal-install

## Phase 2: Implementation (Group A — token cleanup + about.toml + Makefile)
- [x] 2.1 Remove GitHub token handling from install.sh
- [x] 2.2 Remove GitHub token references from install.test.sh
- [x] 2.3 Remove GitHub token references from docs/install.md
- [x] 2.4 Add aarch64-unknown-linux-gnu to about.toml targets array
- [x] 2.5 Update Makefile SLC_RELEASE_URL to accept ARCH variable

## Phase 2: Implementation (Group B — CI pipeline)
- [x] 2.6 Convert build-so CI job to a matrix (x86_64 + aarch64) [expert]
- [x] 2.7 Add standalone arm64 CI job (cargo test --workspace)
- [x] 2.8 Update release job to download+package both architecture artifacts [expert]

## Phase 2: Implementation (Group C — install.sh --arch)
- [x] 2.9 Add --arch flag to install.sh parse_args
- [x] 2.10 Add resolve_arch_suffix helper to install.sh
- [x] 2.11 Update download_slc and download_engine to use arch-suffixed asset names

## Phase 2: Implementation (Group D — install.sh --deployment)
- [x] 2.12 Add deployment descriptor functions to install.sh [expert]
- [x] 2.13 Add --deployment flag + transport selection logic to main
- [x] 2.14 Add SSH transport function deploy_personal_local [expert]
- [x] 2.15 Add architecture auto-detection in --deployment local path

## Phase 2: Implementation (Group E — install.sh help)
- [x] 2.16 Update install.sh usage/help text for --arch and --deployment

## Phase 2: Implementation (Group F — tests)
- [x] 2.17 Add install script tests for --arch flag
- [x] 2.18 Add install script tests for resolve_arch_suffix
- [x] 2.19 Add install script tests for deployment descriptor functions
- [x] 2.20 Add install script tests for --deployment transport selection

## Phase 2: Implementation (Group G — docs)
- [x] 2.21 Update docs/install.md with Exasol Personal section + --arch flag

## Phase 3: Verification
- [ ] 3.1 Run cargo test / build / clippy / fmt
- [ ] 3.2 Run shellcheck on install.sh and install.test.sh
- [ ] 3.3 Run make test-install
- [ ] 3.4 Scenario coverage audit against plan's Verification table
- [ ] 3.5 Manual verification (install.sh --arch aarch64; --deployment against Exasol Personal local where feasible)

## Phase 4: Review Fixes (Expert)
- [x] 4.1 Add ssh/scp recording stubs and five SSH-transport tests to install.test.sh; repoint the plan's Verification row [expert]
- [x] 4.2 Replace the fixed VM reconcile sleep with a bounded vm_wait_for_reconciled_path poll + tests [expert]
- [x] 4.3 Validate and normalize the Makefile's ARCH; add makefile_slc_url_arch_aware test [expert]
- [x] 4.4 Gate the release job on arm64 and record the decision [expert]

## Phase 4: Review Fixes (Standard)
- [x] 4.5 In install.sh `read_descriptor_field`, capture jq's stderr into `jq_err` (2>&1 capture, same shape as `push_slc_to_vm`'s `scp said: $out`) and append `jq said: $jq_err` to the existing JSON-parse error message
- [x] 4.6 In install.sh `download_engine`, replace `asset="lakehouse-engine$suffix.tar.gz"` with `asset="${ENGINE_ASSET%.tar.gz}$suffix.tar.gz"`
- [x] 4.7 In install.sh `detect_host_arch`, add explicit `x86_64|amd64` case, make `*)` call `err` naming the detected value and instructing `--arch x86_64|aarch64`, then `return 1`; propagate via `ARG_ARCH="$(detect_host_arch)" || return 1` in `resolve_deployment_transport`; add a test near `deployment_local_autodetects_arch` stubbing `uname -m` to `ppc64le` and asserting nonzero exit naming the value and `--arch`
- [x] 4.8 In install.sh `resolve_deployment_transport`, replace the empty-string `--bfs-bucket` check with `[[ ! "$ARG_BFS_BUCKET" =~ ^[A-Za-z0-9._-]+$ ]]` rejecting and naming the rejected value and allowed character set; add a test asserting a bucket value containing a single quote is rejected with nonzero exit before transport selection
- [x] 4.9 In install.test.sh, add and register in `main()`: `deployment_rejects_saas_target`, `deployment_rejects_profile_and_dsn` (ARG_PROFILE and ARG_DSN cases), `deployment_rejects_empty_bfs_bucket`, following the `deployment_requires_jq` subshell shape
- [x] 4.10 In install.test.sh, change both `WORKDIR="$(mktemp -d)"` in `arch_aarch64_selects_suffixed_assets` to `WORKDIR="$(mktemp -d "$SANDBOX/arch-workdir.XXXXXX")"`
- [x] 4.11 In install.test.sh, split `arch_default_is_x86_64`: keep the default-case assertion under that name, move the two explicit-flag assertions into a new `arch_explicit_flag_is_stored_and_marks_set` with its own echo header, registered in `main()`
- [x] 4.12 In install.test.sh, cut `write_local_deployment_fixture`/`write_cloud_deployment_fixture` to 2 params (`dir` / `dir backend`); add `write_local_deployment_fixture_custom_connection dir` for the host `descriptor.example`/dbPort `52164`/user `dbadmin` case; update all call sites and delete trailing signature comments
- [x] 4.13 In install.test.sh, add a combined fail-fast prerequisite guard near the sandbox setup checking both `jq` and `make` are on PATH, exiting with a FATAL message if either is missing
- [x] 4.14 In install.test.sh, delete net-new prose comment lines and section-banner blocks (diff against `main`) near the deployment-fixture-writer definitions, sandbox setup, and new deployment/arch test groups; keep `# shellcheck disable=...` directive lines; do not touch pre-existing comments
- [x] 4.15 In docs/install.md, add a sentence at the end of the "Download the release tarball" section instructing aarch64 readers to rename the download to `lakehouse-engine.tar.gz` before continuing; insert a blank line after the prerequisites list's last bullet before the `## Install with one command` heading
