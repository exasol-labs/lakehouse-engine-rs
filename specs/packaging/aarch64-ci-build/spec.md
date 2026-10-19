# Feature: aarch64 CI Build and Release

CI builds the UDF `.so` for both x86_64 and aarch64 on native runners and publishes architecture-distinguished release assets. x86_64 keeps the unsuffixed tarball name; aarch64 gets a `-aarch64` suffix.

## Background

- The UDF `.so` is built inside the builder image via `make cross-udf-build`
- `about.toml` controls which targets `cargo-about` checks for license compliance

## Scenarios

### Scenario: CI builds the .so for both x86_64 and aarch64 via a matrix

* *GIVEN* the CI workflow is triggered on a non-draft PR, push to main, or merge_group event
* *WHEN* the `build-so` job runs
* *THEN* it MUST execute two matrix legs: an x86_64 leg on an x86_64 runner and an aarch64 leg on a native aarch64 runner
* *AND* each leg MUST build the `.so` inside the builder image on its native runner architecture
* *AND* each leg MUST upload the build artifact with a distinct name including the architecture (`lakehouse-engine-so-x86_64`, `lakehouse-engine-so-aarch64`)

### Scenario: Release publishes architecture-distinguished tarballs

* *GIVEN* a release is triggered by a version bump on main or a tag push
* *AND* both architecture build artifacts are available
* *WHEN* the release job packages the tarballs
* *THEN* the x86_64 tarball MUST be named `lakehouse-engine.tar.gz` (unsuffixed, backward compatible)
* *AND* the aarch64 tarball MUST be named `lakehouse-engine-aarch64.tar.gz`
* *AND* both tarballs MUST be attached to the GitHub Release

### Scenario: about.toml includes aarch64 in license-check targets

* *GIVEN* the `about.toml` configuration file
* *WHEN* `cargo about generate` runs
* *THEN* the `targets` array MUST include both `"x86_64-unknown-linux-gnu"` and `"aarch64-unknown-linux-gnu"`
