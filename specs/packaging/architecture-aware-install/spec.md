# Feature: Architecture-Aware Install

The install script and Makefile select the correct release asset for the target Exasol host's CPU architecture. x86_64 is the default. An explicit `--arch` flag overrides all paths. Exasol Personal local deployments auto-detect architecture via `uname -m`.

## Background

- Release asset naming: `lakehouse-engine.tar.gz` (x86_64), `lakehouse-engine-aarch64.tar.gz` (aarch64)
- SLC asset naming: `lc-rust-<ver>.tar.gz` (x86_64), `lc-rust-<ver>-aarch64.tar.gz` (aarch64)
- Bash 3.2+ compatibility required (stock macOS); no `${VAR^^}`, use `tr` for case conversion
- The `--arch` flag is new to this project; lc-rs does not have one

## Scenarios

### Scenario: Default architecture is x86_64

* *GIVEN* the install script is invoked without `--arch` and without `--deployment`
* *WHEN* asset names are resolved
* *THEN* the engine asset name MUST be `lakehouse-engine.tar.gz` (unsuffixed)
* *AND* the SLC asset name MUST be `lc-rust-<version>.tar.gz` (unsuffixed)

### Scenario: --arch aarch64 selects aarch64-suffixed assets

* *GIVEN* the install script is invoked with `--arch aarch64`
* *WHEN* asset names are resolved
* *THEN* the engine asset name MUST be `lakehouse-engine-aarch64.tar.gz`
* *AND* the SLC asset name MUST be `lc-rust-<version>-aarch64.tar.gz`

### Scenario: --arch flag rejects invalid values

* *GIVEN* the install script is invoked with `--arch sparc64`
* *WHEN* argument parsing completes
* *THEN* the script MUST exit with a non-zero status
* *AND* the error message MUST name the valid values (`x86_64`, `aarch64`)

### Scenario: Makefile install-slc computes architecture-aware SLC URL

* *GIVEN* the Makefile `install-slc` target
* *WHEN* `ARCH` is set to `aarch64`
* *THEN* `SLC_RELEASE_URL` MUST resolve to the `-aarch64.tar.gz` suffixed URL
* *AND* when `ARCH` is unset or `x86_64`, `SLC_RELEASE_URL` MUST resolve to the unsuffixed URL

### Scenario: --deployment local auto-detects architecture

* *GIVEN* the install script is invoked with `--deployment <name>` on an `aarch64` host
* *AND* no explicit `--arch` flag is given
* *WHEN* the deployment backend is `local`
* *THEN* the script MUST auto-detect the architecture via `uname -m`
* *AND* asset names MUST use the detected architecture's suffix convention

### Scenario: Explicit --arch overrides auto-detection

* *GIVEN* the install script is invoked with `--deployment <name>` and `--arch x86_64` on an `aarch64` host
* *WHEN* asset names are resolved
* *THEN* the script MUST use x86_64 (unsuffixed) asset names regardless of the host architecture
