# Decisions: add-aarch64-build-and-personal-install

## ADR: Native per-architecture runners, not cross-compilation

**ID:** aarch64-per-arch-native-runners
**Plan:** add-aarch64-build-and-personal-install
**Status:** Accepted

### Context

The engine released only x86_64 artifacts, and aarch64 needs a build strategy. A cold release build takes 33 minutes natively, and the SLC requires an exact glibc match.

### Decision

Each architecture builds on its own native GitHub Actions runner, `ubuntu-latest` for x86_64 and `ubuntu-24.04-arm` for aarch64, inside the same builder image.

### Options Considered

| Option | Verdict |
|--------|---------|
| QEMU emulation on one x86_64 runner | Rejected: a 5-10x slowdown makes a 33-minute build impractical |
| `cross-rs` cross-compilation | Rejected: glibc cross-link complexity conflicts with the SLC's exact-match constraint |

### Consequences

CI gains a second native build leg at the cost of extra runner minutes.

## ADR: `--arch` defaults to x86_64, not auto-detection

**ID:** arch-flag-defaults-x86_64
**Plan:** add-aarch64-build-and-personal-install
**Status:** Accepted

### Context

The install script runs on an operator's machine, which for SaaS and BucketFS targets has no relationship to the cluster's architecture.

### Decision

`--arch` defaults to `x86_64`. Auto-detection from `uname -m` runs only for `--deployment local`, where the operator's machine is the Exasol Personal VM host.

### Options Considered

| Option | Verdict |
|--------|---------|
| Always auto-detect from the operator's machine | Rejected: wrong for every target whose architecture differs from the operator's |

### Consequences

Existing x86_64 invocations are unchanged. Inference applies only where host and target match.
