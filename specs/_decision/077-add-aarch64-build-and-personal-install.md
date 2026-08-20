# Decisions: add-aarch64-build-and-personal-install

## ADR: Native per-architecture runners, not cross-compilation

**ID:** aarch64-per-arch-native-runners
**Plan:** add-aarch64-build-and-personal-install
**Status:** Accepted

### Context

The engine built and released only x86_64 artifacts. Adding aarch64 required choosing a build
strategy for the second architecture. A cold release build takes 33 minutes on native hardware.
QEMU emulation adds a 5-10x slowdown, making it impractical at that baseline. Cross-compilation
via `cross-rs` introduces glibc cross-link complexity that conflicts with the SLC's exact-glibc-match
constraint (`rust:1.94-bookworm`, glibc 2.36).

### Decision

Each architecture builds on its own native GitHub Actions runner — `ubuntu-latest` for x86_64,
`ubuntu-24.04-arm` for aarch64 — inside the same `rust:1.94-bookworm` builder image.

### Options Considered

| Option | Verdict |
|--------|---------|
| Native per-arch runners | ✓ Chosen — avoids QEMU's 5-10x slowdown and cross-rs's glibc cross-link fragility; lc-rs already validated this with its `build-slc` matrix |
| QEMU emulation on a single x86_64 runner | ✗ Rejected — 5-10x slowdown makes a 33-minute cold build impractical |
| `cross-rs` cross-compilation | ✗ Rejected — glibc cross-link complexity fights the SLC's exact-match constraint |

### Consequences

CI gains a second native build leg instead of an emulated or cross-compiled one, keeping build
time and the glibc match simple at the cost of one additional runner-minutes line item.

## ADR: x86_64 unsuffixed, aarch64 suffixed release asset naming

**ID:** aarch64-asset-naming-unsuffixed-x86
**Plan:** add-aarch64-build-and-personal-install
**Status:** Accepted

### Context

Existing docs, CI consumers, and the `curl` one-liner in `docs/install.md` all reference
`lakehouse-engine.tar.gz` without a suffix. Introducing a second architecture required a naming
scheme that would not break every prior installation guide.

### Decision

The x86_64 release tarball keeps the historical unsuffixed name `lakehouse-engine.tar.gz`. The
aarch64 tarball is named `lakehouse-engine-aarch64.tar.gz`. The same convention applies to SLC
asset names (`lc-rust-<ver>.tar.gz` / `lc-rust-<ver>-aarch64.tar.gz`).

### Options Considered

| Option | Verdict |
|--------|---------|
| x86_64 unsuffixed, aarch64 `-aarch64` suffixed | ✓ Chosen — backward compatible with every existing doc and CI reference; matches the lc-rs convention already shipping |
| Both architectures suffixed | ✗ Rejected — breaks every existing unsuffixed reference to `lakehouse-engine.tar.gz` |
| Architecture subdirectories in the release | ✗ Rejected — same backward-compatibility break, plus new release-layout surface |

### Consequences

Existing x86_64 users see no change to asset names or download URLs. aarch64 users and tooling
must know to append `-aarch64` — the install script's `--arch` flag and `resolve_arch_suffix`
helper encapsulate that so operators do not need to.

## ADR: `--arch` defaults to x86_64, not auto-detection

**ID:** arch-flag-defaults-x86_64
**Plan:** add-aarch64-build-and-personal-install
**Status:** Accepted

### Context

The install script runs on an operator's machine, which is not always the Exasol cluster's host.
For SaaS and BucketFS targets, the operator's machine architecture has no relationship to the
target cluster's architecture — auto-detecting from `uname -m` would select aarch64 assets on an
Apple Silicon developer machine even when the target Exasol cluster is x86_64.

### Decision

The `--arch` flag defaults to `x86_64`. Auto-detection via `uname -m` activates only inside the
`--deployment local` path, where the operator's machine IS the Exasol Personal VM host.

### Options Considered

| Option | Verdict |
|--------|---------|
| Default to x86_64; auto-detect only for `--deployment local` | ✓ Chosen — auto-detection is correct exactly where host and target architecture are guaranteed to match |
| Always auto-detect from the operator's machine | ✗ Rejected — wrong for every SaaS/BucketFS target where operator and cluster architecture differ |

### Consequences

Existing x86_64 install invocations keep their current behavior unchanged. Only the
`--deployment local` path gains architecture inference, scoped to the one case where it is safe.

## ADR: The arm64 unit-test job gates the release

**ID:** arm64-unit-test-gates-release
**Plan:** add-aarch64-build-and-personal-install
**Status:** Accepted

### Context

`e2e-azure` is excluded from the release job's `needs:` because a live third-party account can
fail for reasons unrelated to the code, and an outage there would block every release. The new
`arm64` job is a GitHub-hosted runner running the same `cargo test --workspace` as `unit-tests`,
with the same reliability profile — not in the `e2e-azure` class. Because aarch64 E2E is a
deliberate non-goal, `arm64` is the only architecture-specific signal covering the aarch64 asset
at all.

### Decision

`arm64` is listed in the `release` job's `needs:`, so a failing aarch64 `cargo test --workspace`
blocks the release that publishes `lakehouse-engine-aarch64.tar.gz`. The rationale is written into
the existing `needs:` comment block beside the documented `e2e-azure` exclusion.

### Options Considered

| Option | Verdict |
|--------|---------|
| Gate the release on `arm64` | ✓ Chosen — same reliability profile as `unit-tests`; leaving it out ships the aarch64 asset with strictly weaker gating than x86_64 |
| Leave `arm64` ungated, like `e2e-azure` | ✗ Rejected — `arm64` is not a flaky third-party dependency; ungating it removes the only aarch64-specific signal |
| Gate the release on an aarch64 E2E job instead | ✗ Rejected — aarch64 E2E is a deliberate non-goal (no arm64 `docker-db` image exists) |

### Consequences

A failing aarch64 unit-test run blocks the release, matching the x86_64 asset's gating strength.
The comment beside the `e2e-azure` exclusion prevents a future reader from "fixing" the new gate
by removing it.
