# Verification Report: add-aarch64-build-and-personal-install

## Verdict

| Result | Details |
|--------|---------|
| **PASS** | All 21 plan tasks implemented, all 15 code-review findings fixed and re-verified, all runnable checklist commands green. `make cross-musl-udf-build` could not run — Docker is not available on this host — a pre-existing environment gap unrelated to this diff (no Rust source file changed). |
| Code review | 15 findings — standard: 11 fixed, expert: 4 fixed |

| Check | Status |
|-------|--------|
| Build (`make cross-musl-udf-build`) | ⚠ not run — Docker unavailable on this host; no `.rs` file in the diff |
| Tests (`cargo test`) | ✓ |
| Lint (`cargo clippy --all-targets`) | ✓ |
| Format (`cargo fmt --all -- --check`) | ✓ |
| ShellCheck | ✓ |
| Install tests (`make test-install`) | ✓ |
| Scenario Coverage | ✓ |
| Manual Tests | ✓ |

## Test Evidence

### Test Results

| Type | Run | Passed | Failed |
|------|-----|--------|--------|
| Rust workspace (`cargo test`) | all crates | ok (0 relevant — no `.rs` files in this diff; ran as a regression check) | 0 |
| Install script (`make test-install` / `install.test.sh`) | 498 | 498 | 0 |

Test count grew through the pipeline: 353 (Group A baseline) → 400 (Group F new tests) → 481 (expert review fixes: SSH transport coverage, Makefile arch-validation test, verified-wait test) → 498 (standard review fixes: error-path, injection-guard, and comment-sweep tests). No regression at any step.

### Manual Tests

| Test | Result |
|------|--------|
| `parse_args --arch aarch64` stores `ARG_ARCH=aarch64`, `ARG_ARCH_SET=1` | ✓ |
| `parse_args --arch mips` rejected: `ERROR: --arch must be 'x86_64' or 'aarch64'; got 'mips'.`, rc=1 | ✓ |
| `resolve_deployment_transport` against the real live Exasol Personal `default` deployment (read-only descriptor resolution, no SSH/ALTER SYSTEM issued): resolved backend `local`, transport `ssh`, host `127.0.0.1:8563`, user `sys`, SSH port `53025`, key path `.../local/node_access.pem` — matches the live descriptor exactly, and auto-detected this host's architecture as `aarch64` without an explicit `--arch` flag | ✓ |
| `make -n install-slc` (no ARCH) → unsuffixed `lc-rust-<v>.tar.gz` URL | ✓ |
| `make -n install-slc ARCH=aarch64` → `-aarch64`-suffixed URL | ✓ |
| `make -n install-slc ARCH=arm64` → normalizes to the same `-aarch64`-suffixed URL | ✓ |
| `make -n install-slc ARCH=bogus` → `$(error ...)`, naming the rejected value and accepted values | ✓ |
| `install.sh --deployment my-local-db` full SSH install / `install.sh --arch aarch64 --profile ci-bucketfs` full run against a real target | not run — plan's Non-Goals exclude aarch64 E2E (no arm64 docker-db image); this host has no Docker at all, so the BucketFS-mode E2E path is also unavailable here. Behavior is covered by 498 unit tests including 5 new SSH-transport tests with mutation-tested coverage (verified to fail when `ALTER SYSTEM`→`ALTER SESSION` or the `.so` destination is corrupted) |

## Tool Evidence

### Linter

```
cargo clippy --all-targets: exit 0, no warnings
shellcheck -s bash deploy/scripts/install.sh deploy/scripts/tests/install.test.sh: exit 0, no output
actionlint .github/workflows/ci.yml: exit 0, clean (run during expert review-fix pass)
```

### Formatter

```
cargo fmt --all -- --check: exit 0, no diff
```

## Scenario Coverage

| Scenario | Test Type | Test Location | Test Name | Passes |
|----------|-----------|----------------|-----------|--------|
| CI builds .so for both x86_64 and aarch64 via matrix | Integration (CI) | `.github/workflows/ci.yml` | `build-so` matrix legs | Pass (actionlint clean; not exercised by a live CI run in this session) |
| Release publishes architecture-distinguished tarballs | Integration (CI) | `.github/workflows/ci.yml` | `release` job | Pass (statically verified; gated on `arm64` per review fix) |
| arm64 CI job runs unit tests without coverage or E2E | Integration (CI) | `.github/workflows/ci.yml` | `arm64` job | Pass |
| about.toml includes aarch64 in license-check targets | Integration (CI) | `.github/workflows/ci.yml` | `licenses` job | Pass |
| install.sh contains no reference to GITHUB_TOKEN or --github-token | Unit | `deploy/scripts/tests/install.test.sh` | absence verified: `grep -c github.token install.sh` = 0 | Pass |
| install.sh curl calls work without auth header args | Unit | `deploy/scripts/tests/install.test.sh` | `test_version_resolution_default_and_override` | Pass |
| Default architecture is x86_64 | Unit | `deploy/scripts/tests/install.test.sh` | `arch_default_is_x86_64` | Pass |
| --arch aarch64 selects aarch64-suffixed assets | Unit | `deploy/scripts/tests/install.test.sh` | `arch_aarch64_selects_suffixed_assets` | Pass |
| --arch flag rejects invalid values | Unit | `deploy/scripts/tests/install.test.sh` | `arch_invalid_value_rejected` | Pass |
| Explicit --arch flag is stored and marks itself set | Unit | `deploy/scripts/tests/install.test.sh` | `arch_explicit_flag_is_stored_and_marks_set` | Pass |
| Makefile install-slc computes architecture-aware SLC URL | Unit | `deploy/scripts/tests/install.test.sh` | `makefile_slc_url_arch_aware` | Pass |
| --deployment local auto-detects architecture | Unit | `deploy/scripts/tests/install.test.sh` | `deployment_local_autodetects_arch` | Pass |
| Explicit --arch overrides auto-detection | Unit | `deploy/scripts/tests/install.test.sh` | `arch_override_beats_autodetect` | Pass |
| Unsupported uname fails detection loudly (review fix) | Unit | `deploy/scripts/tests/install.test.sh` | `deployment_local_unsupported_uname_fails_detection` | Pass |
| --deployment local installs over SSH | Unit | `deploy/scripts/tests/install.test.sh` | `deployment_local_pushes_artifacts_over_ssh`, `deployment_local_registers_script_languages_with_alter_system`, `deployment_local_skip_slc_skips_push_and_registration`, `deployment_local_ssh_failures_are_actionable`, `deployment_local_requires_ssh_and_scp` | Pass |
| SSH transport waits for VM reconciliation before DDL (review fix) | Unit | `deploy/scripts/tests/install.test.sh` | `deployment_local_waits_for_reconciled_paths` | Pass |
| --deployment cloud uses BucketFS HTTP upload | Unit | `deploy/scripts/tests/install.test.sh` | `deployment_cloud_bfs_transport` | Pass |
| --deployment cloud without --bfs-write-password fails | Unit | `deploy/scripts/tests/install.test.sh` | `deployment_cloud_requires_bfs_password` | Pass |
| --deployment requires jq | Unit | `deploy/scripts/tests/install.test.sh` | `deployment_requires_jq` | Pass |
| CLI flags override deployment descriptor values | Unit | `deploy/scripts/tests/install.test.sh` | `deployment_cli_overrides_descriptor` | Pass |
| Missing deployment directory fails | Unit | `deploy/scripts/tests/install.test.sh` | `deployment_missing_dir_fails` | Pass |
| --deployment rejects SaaS/profile/dsn conflicts and empty bucket (review fix) | Unit | `deploy/scripts/tests/install.test.sh` | `deployment_rejects_saas_target`, `deployment_rejects_profile_and_dsn`, `deployment_rejects_empty_bfs_bucket`, `deployment_rejects_bfs_bucket_with_invalid_characters` | Pass |
| jq descriptor-read failures surface jq's own stderr (review fix) | Unit | `deploy/scripts/tests/install.test.sh` | `read_descriptor_field_reports_jq_stderr` | Pass |

## Notes

- **No Rust source changed.** This plan's diff is entirely CI YAML, `install.sh`/`install.test.sh` bash, `Makefile`, `about.toml`, and `docs/install.md` — `cargo test`/`clippy`/`fmt` were run as a regression check and pass, unaffected by this change.
- **`make cross-musl-udf-build` not run**: Docker is unavailable on this host (verified via `docker info`). This is an environment limitation, not a code defect, and matches the plan's own acknowledgment that this is an Apple Silicon dev machine.
- **No version bump in this PR**, per explicit instruction for this run.
- **Code review found and fixed a real security issue**: an unvalidated `--bfs-bucket` value was interpolated unescaped into a remote `rm -rf` command issued over SSH — a bucket name containing a single quote could execute arbitrary shell on the target VM. Fixed with character-class validation (`^[A-Za-z0-9._-]+$`) before the review round completed; not shipped.
- **A no-new-code-comments instruction governed this entire run.** Implementer agents initially followed the touched files' pre-existing dense-doc-comment convention (~119 net-new comment lines across `install.sh`, `Makefile`, `ci.yml`); two dedicated compliance-sweep passes stripped them (one mid-run, one folded into the standard review-fix pass) down to a single deliberate, explicitly authorized exception: an extension of the pre-existing `release` job `needs:` rationale comment explaining why `arm64` gates the release unlike the deliberately-excluded `e2e-azure`.
- **`vm_wait_for_reconciled_path` is per-artifact, not a single combined wait** — a deliberate deviation from the review finding's literal wording, made because `--skip-slc` runs don't push an SLC tree to wait for; per-path polling avoids inventing a new failure mode for that case.
- **Decision `[9]`** was added to `decision-log.md` for the `arm64` job now gating the `release` job — see that file for the full Decision/Alternatives/Rationale record.
- **aarch64 E2E remains untested against a live cluster**, an explicit plan Non-Goal (no arm64 docker-db image exists). The SSH transport's correctness is instead covered by 5 new unit tests with mutation-tested assertions (flipping `ALTER SYSTEM`→`ALTER SESSION` or corrupting the `.so` destination path was confirmed to fail 8 of them).
