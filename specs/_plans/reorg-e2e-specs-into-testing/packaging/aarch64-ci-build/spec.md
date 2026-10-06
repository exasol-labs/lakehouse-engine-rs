<!-- DELTA:CHANGED -->
# Feature: aarch64 CI Build and Release

CI builds the UDF `.so` for both x86_64 and aarch64 on native runners and publishes architecture-distinguished release assets. x86_64 keeps the historical unsuffixed tarball name for backward compatibility; aarch64 gets a `-aarch64` suffix.
<!-- /DELTA:CHANGED -->

<!-- DELTA:CHANGED -->
## Background

- The UDF `.so` is built inside `rust:1.94-trixie` via `make cross-udf-build`, never on the host directly
- `about.toml` controls which targets `cargo-about` checks for license compliance
- The lc-rs project already ships the same dual-architecture pattern: `lc-rust-<ver>.tar.gz` (x86_64) and `lc-rust-<ver>-aarch64.tar.gz` (aarch64)
<!-- /DELTA:CHANGED -->

## Scenarios

<!-- DELTA:REMOVED -->
### Scenario: arm64 CI job runs unit tests without coverage or E2E

* *GIVEN* the CI workflow is triggered
* *WHEN* the `arm64` job runs on `ubuntu-24.04-arm`
* *THEN* it MUST run `cargo test --workspace` (unit tests only)
* *AND* it MUST NOT run E2E tests, coverage instrumentation, or Sonar analysis
* *AND* its cargo cache key MUST include `runner.arch` to prevent cross-architecture cache poisoning
<!-- /DELTA:REMOVED -->
