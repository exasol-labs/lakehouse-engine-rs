# Decisions: add-aarch64-build-and-personal-install

## ADR: Native per-architecture runners, not cross-compilation

**ID:** aarch64-per-arch-native-runners
**Plan:** add-aarch64-build-and-personal-install
**Status:** Accepted

### Context

The engine needs an aarch64 build next to x86_64. A cold release build takes 33 minutes natively, and the SLC requires an exact glibc match.

### Decision

Each architecture builds on its own native GitHub Actions runner, inside the same builder image.

### Options Considered

| Option | Verdict |
|--------|---------|
| QEMU emulation on one x86_64 runner | Rejected: a 5-10x slowdown makes the 33-minute build impractical |
| `cross-rs` cross-compilation | Rejected: glibc cross-link complexity conflicts with the SLC's exact-match constraint |

### Consequences

- CI gains a second native build job and uses more runner minutes.

