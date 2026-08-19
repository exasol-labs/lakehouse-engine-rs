# Decision Log: add-aarch64-build-and-personal-install

## Interview

**Q:** Single plan or split?
**A:** Single plan covering all 7 work areas.

**Q:** Where should docs live?
**A:** In both `install.sh --help` and in `docs/install.md` (already titled "Install & Deploy").

**Q:** lc-rs reference location?
**A:** Local checkout at `../language-container-rs`.

## Design Decisions

### [1] Native per-arch runners, not cross-compilation

- **Decision:** Each architecture builds on its own native GitHub Actions runner (`ubuntu-latest` for x86_64, `ubuntu-24.04-arm` for aarch64) inside the same `rust:1.94-bookworm` builder image.
- **Alternatives:** QEMU emulation on a single x86_64 runner; `cross-rs` cross-compilation toolchain.
- **Rationale:** A cold release build takes 33 minutes on native hardware. QEMU adds a 5-10x slowdown, making it impractical. Cross-compilation introduces glibc cross-link complexity that the SLC match constraint makes fragile. lc-rs already validated the native-runner approach with `build-slc`'s matrix.
- **Promotes to ADR:** yes

### [2] x86_64 unsuffixed, aarch64 suffixed asset naming

- **Decision:** The x86_64 release tarball keeps the historical unsuffixed name `lakehouse-engine.tar.gz`. The aarch64 tarball gets `lakehouse-engine-aarch64.tar.gz`.
- **Alternatives:** Both suffixed (`-x86_64` and `-aarch64`); architecture subdirectories in the release.
- **Rationale:** Backward compatibility. Existing docs, CI consumers, and the `curl` one-liner in `docs/install.md` all reference `lakehouse-engine.tar.gz` without a suffix. Changing it breaks every prior installation guide. lc-rs uses the same convention (`lc-rust-<ver>.tar.gz` for x86_64, `lc-rust-<ver>-aarch64.tar.gz` for aarch64).
- **Promotes to ADR:** yes

### [3] Standalone arm64 unit-test job, not a matrix leg of unit-tests

- **Decision:** arm64 unit tests run as a standalone `arm64` CI job, separate from the existing `unit-tests` job.
- **Alternatives:** Converting `unit-tests` into a matrix over architectures.
- **Rationale:** `unit-tests` uses `cargo-llvm-cov` for coverage instrumentation and feeds Sonar analysis. Both tools are x86_64-specific in this pipeline. A matrix would require conditional skips for coverage/Sonar on the arm64 leg, adding complexity. A standalone job is cleaner and mirrors lc-rs's `arm64` job.
- **Promotes to ADR:** no

### [4] --arch defaults to x86_64, not auto-detection

- **Decision:** The `--arch` flag defaults to `x86_64`. Auto-detection via `uname -m` activates only in the `--deployment local` path.
- **Alternatives:** Always auto-detect from the operator's machine.
- **Rationale:** For SaaS and BucketFS targets, the operator's machine architecture has no relationship to the Exasol cluster's architecture. Auto-detection would select aarch64 assets on a developer's Apple Silicon Mac, then upload them to an x86_64 Exasol cluster. Only Personal-local, where the operator's machine IS the Exasol host, makes auto-detection correct.
- **Promotes to ADR:** yes

### [5] Mirror lc-rs deployment patterns

- **Decision:** The `--deployment` flag, deployment descriptor parsing, SSH transport, SCRIPT_LANGUAGES merge, and cloud passthrough mirror the lc-rs `install.sh` implementation.
- **Alternatives:** A new design specific to the engine's install script.
- **Rationale:** lc-rs already solved these problems with tested, macOS-compatible (Bash 3.2+) code. Mirroring avoids divergence between the two install scripts that operators run sequentially (SLC first, then engine). The lc-rs test patterns (`install-personal-test.sh`) transfer directly.
- **Promotes to ADR:** no

### [6] E2E testing stays x86_64-only

- **Decision:** E2E tests run only on x86_64 runners. arm64 CI covers unit tests only.
- **Alternatives:** QEMU-emulated arm64 E2E; waiting for an arm64 docker-db image.
- **Rationale:** `exasol/docker-db` publishes amd64-only images. QEMU-emulating a privileged multi-GB database image is impractical. arm64 end-to-end verification stays manual against Exasol Personal until an arm64 docker-db image is available. This is the same decision lc-rs made.
- **Promotes to ADR:** no

### [7] Deployment transport selection mirrors lc-rs branching

- **Decision:** The `--deployment` flag reads `.backend` from `deployment.json`. `"local"` activates SSH transport with ALTER SYSTEM; any other value falls through to the existing BucketFS HTTP upload path with connection details resolved from the descriptor.
- **Alternatives:** Separate `--deployment-local` and `--deployment-cloud` flags; always SSH for Personal.
- **Rationale:** A single `--deployment` flag with runtime discrimination matches the lc-rs interface exactly. Cloud backends expose the same BucketFS HTTP endpoint as a normal cluster, so reusing the existing upload path avoids duplication. The operator does not need to know which backend type their deployment uses.
- **Promotes to ADR:** no

### [8] Remove GitHub token handling from install.sh

- **Decision:** Delete all `--github-token`/`GITHUB_TOKEN`/`GITHUB_AUTH_ARGS` handling from `install.sh`, its tests, and its docs. Curl calls to the GitHub REST API run unauthenticated.
- **Alternatives:** Keep the token as optional auth; remove only the flag but keep the env var.
- **Rationale:** Both repos are public. A single install invocation makes ~3 GitHub API calls, well under the 60 req/hr unauthenticated rate limit. The token support adds flag surface, test surface, and doc surface for no benefit. Since this plan already touches `install.sh` extensively (adding `--arch` and `--deployment`), the cleanup is cheaper now than as a follow-up. Folding it in means the new `--arch`/`--deployment` code writes into already-cleaned curl calls instead of having to work around the auth-header expansions.
- **Promotes to ADR:** no

### [9] The arm64 unit-test job gates the release

- **Decision:** `arm64` is listed in the `release` job's `needs:`, so a failing aarch64 `cargo test --workspace` blocks the release that publishes `lakehouse-engine-aarch64.tar.gz`. The rationale is written into the existing `needs:` comment block beside the documented `e2e-azure` exclusion.
- **Alternatives:** Leave `arm64` ungated like `e2e-azure`; gate the release on an aarch64 E2E job instead.
- **Rationale:** `e2e-azure` is excluded because a live third-party account can fail for reasons unrelated to the code, and an outage there would block every release. `arm64` is not in that class: it is a GitHub-hosted runner running the same `cargo test` as `unit-tests`, with the same reliability profile. Since aarch64 E2E is a deliberate non-goal (decision [6]), `arm64` is the only architecture-specific signal covering the aarch64 asset at all — leaving it out would ship the second architecture on strictly weaker gating than the first. The comment is required because an unexplained new gate sitting next to a documented non-gate invites a future reader to "fix" it by removal.
- **Promotes to ADR:** no

## Review Findings

