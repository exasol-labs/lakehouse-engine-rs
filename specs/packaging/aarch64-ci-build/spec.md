# Feature: aarch64 CI Build and Release

CI builds the UDF `.so` for both x86_64 and aarch64 on native runners, runs arm64 unit tests, and publishes architecture-distinguished release assets. x86_64 keeps the historical unsuffixed tarball name for backward compatibility; aarch64 gets a `-aarch64` suffix.

## Background

- The UDF `.so` is built inside `rust:1.94-bookworm` via `make cross-musl-udf-build`, never on the host directly
- `about.toml` controls which targets `cargo-about` checks for license compliance
- E2E tests stay x86_64-only because `exasol/docker-db` publishes amd64-only images
- The lc-rs project already ships the same dual-architecture pattern: `lc-rust-<ver>.tar.gz` (x86_64) and `lc-rust-<ver>-aarch64.tar.gz` (aarch64)

## Scenarios

### Scenario: CI builds the .so for both x86_64 and aarch64 via a matrix

* *GIVEN* the CI workflow is triggered on a non-draft PR, push to main, or merge_group event
* *WHEN* the `build-so` job runs
* *THEN* it MUST execute two matrix legs: `{runner: ubuntu-latest, arch: x86_64}` and `{runner: ubuntu-24.04-arm, arch: aarch64}`
* *AND* each leg MUST build the `.so` inside the `rust:1.94-bookworm` builder image on its native runner architecture
* *AND* each leg MUST upload the build artifact with a distinct name including the architecture (`lakehouse-engine-so-x86_64`, `lakehouse-engine-so-aarch64`)

### Scenario: Release publishes architecture-distinguished tarballs

* *GIVEN* a release is triggered by a version bump on main or a tag push
* *AND* both architecture build artifacts are available
* *WHEN* the release job packages the tarballs
* *THEN* the x86_64 tarball MUST be named `lakehouse-engine.tar.gz` (unsuffixed, backward compatible)
* *AND* the aarch64 tarball MUST be named `lakehouse-engine-aarch64.tar.gz`
* *AND* both tarballs MUST be attached to the GitHub Release

### Scenario: arm64 CI job runs unit tests without coverage or E2E

* *GIVEN* the CI workflow is triggered
* *WHEN* the `arm64` job runs on `ubuntu-24.04-arm`
* *THEN* it MUST run `cargo test --workspace` (unit tests only)
* *AND* it MUST NOT run E2E tests, coverage instrumentation, or Sonar analysis
* *AND* its cargo cache key MUST include `runner.arch` to prevent cross-architecture cache poisoning

### Scenario: about.toml includes aarch64 in license-check targets

* *GIVEN* the `about.toml` configuration file
* *WHEN* `cargo about generate` runs
* *THEN* the `targets` array MUST include both `"x86_64-unknown-linux-gnu"` and `"aarch64-unknown-linux-gnu"`
