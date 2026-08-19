# Plan: add-aarch64-build-and-personal-install

## Summary

Add aarch64 build support and Exasol Personal deployment support. CI builds and releases the engine `.so` for both x86_64 and aarch64; the install script gains `--arch` and `--deployment` flags for architecture-aware asset selection and Exasol Personal install.

## Design

### Context

Exasol Personal runs on Apple Silicon (aarch64) VMs and cloud backends. The engine currently builds and releases only x86_64 artifacts, and the install script has no path for Exasol Personal's SSH-based transport or its deployment descriptor model. The upstream lc-rs project already ships dual-architecture SLC releases and a `--deployment` install path; this plan mirrors those patterns for the engine.

- **Goals** -- build and release the engine `.so` for both x86_64 and aarch64; make the install script and Makefile architecture-aware; add Exasol Personal deployment support (local SSH and cloud passthrough) to the install script
- **Non-Goals** -- aarch64 E2E testing (no arm64 docker-db image exists); cross-compilation (each architecture builds on its native runner); Exasol Personal-specific Makefile targets beyond SLC download

### Decision

#### Architecture

```
CI Workflow
  build-so (matrix: x86_64 + aarch64)
    ├── ubuntu-latest          → lakehouse-engine-so-x86_64
    └── ubuntu-24.04-arm       → lakehouse-engine-so-aarch64

  arm64 (standalone, no E2E)
    └── ubuntu-24.04-arm       → cargo test --workspace

  release
    ├── downloads lakehouse-engine-so-x86_64  → lakehouse-engine.tar.gz
    └── downloads lakehouse-engine-so-aarch64 → lakehouse-engine-aarch64.tar.gz

install.sh
  --arch flag
    ├── x86_64 (default)       → unsuffixed asset names
    └── aarch64                → -aarch64 suffixed asset names

  --deployment flag
    ├── local backend          → SSH transport + ALTER SYSTEM
    └── cloud backend          → BucketFS HTTP (existing path)
```

#### Patterns

| Pattern | Where | Why |
|---------|-------|-----|
| Native per-arch runner | CI `build-so` matrix | No QEMU overhead; native glibc match; mirrors lc-rs `build-slc` pattern |
| Unsuffixed x86_64 / suffixed aarch64 | Release assets, install.sh, Makefile | Backward compatibility for existing users and docs; mirrors lc-rs naming |
| Architecture-keyed cache | arm64 CI job | Prevents x86_64/aarch64 target/ cross-contamination; mirrors lc-rs cache key |
| Deployment descriptor model | install.sh `--deployment` | Reuses Exasol Personal's deployment.json/secrets.json; mirrors lc-rs install.sh |
| SSH filesystem transport | install.sh `--deployment` local | Personal-local has no BucketFS HTTP endpoint; mirrors lc-rs `extract_slc_into_bucketfs` |
| Architecture auto-detection | install.sh `--deployment` local | Personal-local on Apple Silicon is always aarch64; `uname -m` detects it |

### Consequences

| Decision | Alternatives Considered | Rationale |
|----------|------------------------|-----------|
| Native runners, not cross-compilation | QEMU or cross-rs | Native build avoids QEMU's 5-10x slowdown on a 33-min cold build; keeps the glibc match simple; same approach lc-rs proved works |
| x86_64 unsuffixed, aarch64 suffixed | Both suffixed, or both unsuffixed with arch subdirs | Backward compatibility: existing docs, curl one-liners, and CI consumers reference `lakehouse-engine.tar.gz` without a suffix |
| Standalone arm64 unit-test job, not a matrix leg of unit-tests | Matrix over unit-tests | Unit-tests uses `cargo-llvm-cov` + Sonar upload, both x86_64-only tooling; a separate job avoids conditional complexity |
| Mirror lc-rs deployment patterns | New design from scratch | lc-rs already solved deployment descriptor parsing, SSH transport, SCRIPT_LANGUAGES merge, and cloud passthrough; mirroring avoids divergence and shares the proven test patterns |
| `--arch` defaults to x86_64, not auto-detection | Always auto-detect | SaaS and BucketFS targets have no `uname -m` relationship to the Exasol cluster's architecture; auto-detection is only meaningful for Personal-local where the host IS the Exasol VM |

## Features

| Feature | Status | Spec |
|---------|--------|------|
| aarch64 CI Build and Release | NEW | `packaging/aarch64-ci-build/spec.md` |
| Architecture-Aware Install | NEW | `packaging/architecture-aware-install/spec.md` |
| Exasol Personal Deployment Install | NEW | `packaging/personal-deployment-install/spec.md` |

## Impact

Existing x86_64 users see no change: the default `--arch` is x86_64, the x86_64 release tarball keeps its unsuffixed name, and the install script's default behavior is unchanged. aarch64 users (Exasol Personal on Apple Silicon) gain a supported install path. CI gains a second build leg and an arm64 unit-test job, increasing CI minutes but not blocking the existing pipeline.

Breaking changes: none. The new `--arch` and `--deployment` flags are additive.

## Dependencies

| Dependency | Purpose | Status |
|------------|---------|--------|
| `ubuntu-24.04-arm` GitHub Actions runner | Native aarch64 builds | Available (GitHub-hosted) |
| `rust:1.94-bookworm` on arm64 | Builder image must support aarch64 | Available (multi-arch image) |
| lc-rs dual-arch releases | SLC assets with `-aarch64` suffix | Already shipping |
| `jq` | Deployment descriptor parsing | Required on operator's machine for `--deployment` only |

## Implementation Tasks

1. Remove GitHub token handling from `install.sh`: delete `ARG_GITHUB_TOKEN` global (line 46) and its `parse_args` seeding (line 390); remove `--github-token` from the flag-list case (line 415) and the value-assignment case (line 427); delete the entire `set_github_auth_args` function (lines 631-643) and the `GITHUB_AUTH_ARGS=()` global (line 636); strip every `"${GITHUB_AUTH_ARGS[@]+"${GITHUB_AUTH_ARGS[@]}"}"` expansion from the `curl` calls in `resolve_engine_pinned_slc_version` (line 670), `resolve_versions` (line 699), and `download_release_asset` (lines 1044, 1055); remove the `set_github_auth_args` call in `resolve_versions` (line 694) and `download_release_asset` (line 1042); remove the rate-limit hint from the error message in `resolve_versions` (line 700); remove `--github-token` from the `usage()` flag listing (lines 344-346); update the header comment (lines 8-10) to drop the token mention
2. Remove GitHub token references from `install.test.sh`: delete the `GITHUB_TOKEN="STUBGHTOKEN123"` stub in `reset_env` (line 341) and its comment (line 340); delete the entire `test_github_token_is_optional` test (lines 409-431) and its `main()` call (line 1696); remove `ARG_GITHUB_TOKEN="STUBGHTOKEN123"` assignments from `test_version_resolution_default_and_override` (lines 666, 686) and `test_download_release_asset_rest_api` (line 862); remove the Bearer-token assertion from the version-resolution test (line 678)
3. Remove GitHub token references from `docs/install.md`: delete the optional-token bullet (lines 29-32); delete the `--github-token` row from the flags table (line 101)
4. Add `"aarch64-unknown-linux-gnu"` to `about.toml` targets array
5. Convert `build-so` CI job to a matrix with `{ubuntu-latest, x86_64}` and `{ubuntu-24.04-arm, aarch64}` legs; architecture-key the Docker-side cargo cache; name uploaded artifacts `lakehouse-engine-so-<arch>` [expert]
6. Add standalone `arm64` CI job on `ubuntu-24.04-arm`: `cargo test --workspace`, architecture-keyed cache, no E2E/coverage/Sonar
7. Update release job to download both architecture artifacts, package `lakehouse-engine.tar.gz` (x86_64) and `lakehouse-engine-aarch64.tar.gz` (aarch64), attach both to the GitHub Release [expert]
8. Add `--arch` flag to install.sh `parse_args`: validate against `x86_64|aarch64`, store in `ARG_ARCH`, default `x86_64`
9. Add `resolve_arch_suffix` helper to install.sh: returns `""` for x86_64, `"-aarch64"` for aarch64
10. Update `download_slc` and `download_engine` to use architecture-suffixed asset names via `resolve_arch_suffix`
11. Add deployment descriptor functions to install.sh: `deployment_backend`, `deployment_field`, `deployment_ssh_port`, `deployment_key_path`, `deployment_db_password`, `resolve_deployment_connection`, `require_cloud_bfs_password` -- mirror lc-rs patterns, use `jq` [expert]
12. Add `--deployment` flag to install.sh `parse_args`; add deployment transport selection logic to `main`: local backend (SSH + ALTER SYSTEM), cloud backend (BucketFS HTTP fallthrough)
13. Add SSH transport function `deploy_personal_local`: scp artifacts to VM, extract into BucketFS directory, register SCRIPT_LANGUAGES with merge-preserving update [expert]
14. Add architecture auto-detection in `--deployment local` path: `uname -m` maps `arm64`/`aarch64` to `aarch64`, everything else to `x86_64`; explicit `--arch` overrides
15. Update `install.sh` usage/help text: document `--arch` and `--deployment` flags with examples
16. Update Makefile `SLC_RELEASE_URL` to accept `ARCH` variable and compute architecture-suffixed URL
17. Add install script tests for `--arch` flag: default x86_64, explicit aarch64, invalid value rejection
18. Add install script tests for `resolve_arch_suffix`: both architectures, asset name composition
19. Add install script tests for deployment descriptor functions: SSH port, key path, backend, connection resolution, CLI overrides, missing descriptor failures -- mirror lc-rs `install-personal-test.sh` patterns
20. Add install script tests for `--deployment` transport selection: local backend detection, cloud backend detection, jq requirement, missing directory
21. Update `docs/install.md`: add Exasol Personal section, `--arch` flag to the flags table, architecture-aware manual download URLs, update prerequisites with `jq` note

## Parallelization

| Parallel Group | Tasks |
|----------------|-------|
| Group A: token cleanup + about.toml + Makefile | 1, 2, 3, 4, 16 |
| Group B: CI pipeline | 5, 6, 7 |
| Group C: install.sh --arch | 8, 9, 10 |
| Group D: install.sh --deployment | 11, 12, 13, 14 |
| Group E: install.sh help | 15 |
| Group F: tests | 17, 18, 19, 20 |
| Group G: docs | 21 |

Sequential dependencies:
- Tasks 1-3 (token cleanup) run first within Group A -- they remove code that tasks 8-15 would otherwise have to work around
- Group C depends on Group A (arch suffix convention must be established; token cleanup clears the curl calls that --arch will modify)
- Group D depends on Group C (deployment path uses arch-aware downloads)
- Group E depends on Group C + D (help text documents both flags)
- Group F depends on Group A + C + D (tests exercise the new functions and must not reference removed token code)
- Group G depends on Group E (docs align with help text)
- Group B is independent (CI changes are YAML, not bash)

## Dead Code Removal

| Type | Location | Reason |
|------|----------|--------|
| Global variable | `install.sh` `ARG_GITHUB_TOKEN`, `GITHUB_AUTH_ARGS` | Project is public; token raises rate limit not needed at ~3 calls/install |
| Function | `install.sh` `set_github_auth_args` | Only consumer of `ARG_GITHUB_TOKEN` |
| Flag | `install.sh` `--github-token` (parse_args + usage) | Flag surface for a dead feature |
| Auth header expansions | `install.sh` `resolve_engine_pinned_slc_version`, `resolve_versions`, `download_release_asset` | `GITHUB_AUTH_ARGS` expansions in curl calls |
| Test | `install.test.sh` `test_github_token_is_optional` | Tests removed feature |
| Test stubs | `install.test.sh` `GITHUB_TOKEN` stub in `reset_env`, `ARG_GITHUB_TOKEN` assignments in version/download tests | Support removed feature |
| Docs | `docs/install.md` `--github-token` row, optional-token bullet | Documents removed feature |

## Verification

### Scenario Coverage

| Scenario | Test Type | Test Location | Test Name |
|----------|-----------|---------------|-----------|
| CI builds .so for both x86_64 and aarch64 via matrix | Integration (CI) | `.github/workflows/ci.yml` | `build-so` matrix legs |
| Release publishes architecture-distinguished tarballs | Integration (CI) | `.github/workflows/ci.yml` | `release` job |
| arm64 CI job runs unit tests without coverage or E2E | Integration (CI) | `.github/workflows/ci.yml` | `arm64` job |
| about.toml includes aarch64 in license-check targets | Integration (CI) | `.github/workflows/ci.yml` | `licenses` job (existing, covers both targets) |
| install.sh contains no reference to GITHUB_TOKEN or --github-token | Unit | `deploy/scripts/tests/install.test.sh` | Verified by absence: `test_github_token_is_optional` deleted; `grep -c github.token install.sh` = 0 |
| install.sh curl calls work without auth header args | Unit | `deploy/scripts/tests/install.test.sh` | `test_version_resolution_default_and_override` (existing, updated to remove token setup) |
| Default architecture is x86_64 | Unit | `deploy/scripts/tests/install.test.sh` | `arch_default_is_x86_64` |
| --arch aarch64 selects aarch64-suffixed assets | Unit | `deploy/scripts/tests/install.test.sh` | `arch_aarch64_selects_suffixed_assets` |
| --arch flag rejects invalid values | Unit | `deploy/scripts/tests/install.test.sh` | `arch_invalid_value_rejected` |
| Makefile install-slc computes architecture-aware SLC URL | Unit | `deploy/scripts/tests/install.test.sh` | `makefile_slc_url_arch_aware` |
| --deployment local auto-detects architecture | Unit | `deploy/scripts/tests/install.test.sh` | `deployment_local_autodetects_arch` |
| Explicit --arch overrides auto-detection | Unit | `deploy/scripts/tests/install.test.sh` | `arch_override_beats_autodetect` |
| --deployment local installs over SSH | Unit | `deploy/scripts/tests/install.test.sh` | `deployment_local_pushes_artifacts_over_ssh`, `deployment_local_registers_script_languages_with_alter_system`, `deployment_local_skip_slc_skips_push_and_registration`, `deployment_local_ssh_failures_are_actionable`, `deployment_local_requires_ssh_and_scp` |
| --deployment cloud uses BucketFS HTTP upload | Unit | `deploy/scripts/tests/install.test.sh` | `deployment_cloud_bfs_transport` |
| --deployment cloud without --bfs-write-password fails | Unit | `deploy/scripts/tests/install.test.sh` | `deployment_cloud_requires_bfs_password` |
| --deployment requires jq | Unit | `deploy/scripts/tests/install.test.sh` | `deployment_requires_jq` |
| CLI flags override deployment descriptor values | Unit | `deploy/scripts/tests/install.test.sh` | `deployment_cli_overrides_descriptor` |
| Missing deployment directory fails | Unit | `deploy/scripts/tests/install.test.sh` | `deployment_missing_dir_fails` |

### Manual Testing

| Feature | Command | Expected Output |
|---------|---------|-----------------|
| aarch64 CI Build and Release | Push a version-bump commit to main with CI matrix changes | GitHub Release contains both `lakehouse-engine.tar.gz` and `lakehouse-engine-aarch64.tar.gz` |
| Architecture-Aware Install | `bash deploy/scripts/install.sh --arch aarch64 --profile ci-bucketfs` | Downloads aarch64-suffixed assets, installs onto Exasol |
| Exasol Personal Deployment Install | `bash deploy/scripts/install.sh --deployment my-local-db` | Resolves connection from deployment.json, copies over SSH, registers SCRIPT_LANGUAGES |
| Install script tests | `make test-install` | All tests pass, including new architecture and deployment tests |

### Checklist

| Step | Command | Expected |
|------|---------|----------|
| Build | `make cross-musl-udf-build` | Exit 0 |
| Test | `cargo test` | 0 failures |
| Lint | `cargo clippy --all-targets && cargo fmt` | 0 errors/warnings |
| Format | `cargo fmt --all -- --check` | No changes |
| ShellCheck | `shellcheck -s bash deploy/scripts/install.sh deploy/scripts/tests/install.test.sh` | 0 errors |
| Install tests | `make test-install` | 0 failures |
