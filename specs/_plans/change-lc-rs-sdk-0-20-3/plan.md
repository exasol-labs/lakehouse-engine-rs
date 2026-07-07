# Plan: change-lc-rs-sdk-0-20-3

## Summary

Bump the pinned `language-container-rs` (lc-rs) toolchain — `exasol-udf-sdk` /
`exasol-udf-macros` and the downloaded Rust SLC — from `0.20.2` to `0.20.3` to pick
up lc-rs v0.20.3's unconditional fast string-block encoding for the emit/ingest paths;
this is a drop-in version bump with no API, ABI (`EXA_UDF_ABI_VERSION` stays 6), or
behavioral change from this repo's perspective, so no spec deltas are produced.

## Context

lc-rs v0.20.3 (PR #44, resolves lc-rs issue #29;
`https://github.com/exasol-labs/language-container-rs/releases/tag/v0.20.3`) replaces
`chrono`'s generic `.format()` and `Decimal`'s generic `Display` with hand-rolled
fixed-format Decimal/Date/Timestamp formatters on both the emit path
(`value_to_block_string`) and ingest path (`decode_string_block`), falling back to the
original path for values outside the common representable range. It ships the
optimization unconditionally — no new API surface, no feature flag, no `UdfContext`
trait change, ABI version unchanged (net zero ABI change). Measured +28% to +46% on a
live Exasol DB for a NUMERIC/DATE/TIMESTAMP-heavy row shape.

Because it is a pure version bump, this repo's emit path (`ctx.emit_batch` / `ctx.emit`)
and any ingest path benefit automatically without code changes. The work is: bump the
three pinned version strings (SDK, macros, SLC), refresh `Cargo.lock`, update the
version references in `CLAUDE.md`, verify the SLC↔`.so` rustc fingerprint pairing still
holds, and prove the pairing end-to-end via `make test-e2e`.

- **Goals** — adopt lc-rs 0.20.3 across dependency manifests, the SLC download version,
  and the repo's build documentation; confirm the SDK/SLC fingerprint pairing loads
  cleanly against the local Exasol Docker stack.
- **Non-Goals** — no code changes to emit/ingest logic (the optimization is internal to
  lc-rs); no pushdown, schema-mapping, or execution-logic change; no permanent-spec
  edits (see decision-log [1]); not touching the pre-existing uncommitted `.gitignore`
  change in the working tree.

## Features

No feature specs are added or changed. This plan is tracked via `plan.md` and the
GitHub tracking issue only. Rationale is recorded in `decision-log.md` [1].

| Feature | Status | Spec |
|---------|--------|------|
| _(none)_ | — | Task-only plan; no spec delta |

## Dependencies

- lc-rs v0.20.3 release artifacts must be published and reachable: the crates
  `exasol-udf-sdk` / `exasol-udf-macros` 0.20.3 on the registry, and
  `lc-rust-0.20.3.tar.gz` at the GitHub releases URL that `install-slc` downloads.
- Local Exasol Docker stack available for `make test-e2e` (must fail, not skip, if
  absent, per workspace rules).

## Implementation Tasks

Work happens on the existing `feat/change-lc-rs-sdk-0-20-3` branch (already created off
`main`). Do not create a different branch.

1. **File the GitHub tracking issue first.** Per this repo's CLAUDE.md "Feature
   tracking" rule, create a tracking issue via `ghbrk gh issue create` (title e.g.
   "Bump lc-rs SDK/macros/SLC to 0.20.3 for fast string-block encoding") before starting
   the version-bump edits. Capture the issue number so the implementing commit can
   reference it with `Closes #<n>`. (If the PR pipeline's git-pr-agent files this issue,
   this task is satisfied by that — do not double-file.)
2. Bump the root workspace dependency in `/Cargo.toml` (line ~55):
   `exasol-udf-sdk = { version = "0.20.3", features = ["emit-arrow"] }`.
3. Bump the crate dependencies in `crates/lakehouse-engine/Cargo.toml` (lines ~21-22):
   `exasol-udf-sdk = { version = "0.20.3", features = ["emit-arrow"] }` and
   `exasol-udf-macros = { version = "0.20.3" }`.
4. Refresh `Cargo.lock`:
   `cargo update -p exasol-udf-sdk -p exasol-udf-macros` (or equivalent). Confirm the
   lock resolves both to 0.20.3 and that no unrelated crates move. (Depends on tasks 2, 3.)
5. Bump `SLC_VERSION` in `Makefile` (line ~85) from `0.20.2` to `0.20.3` — this is the
   version `install-slc` downloads (`lc-rust-$(SLC_VERSION).tar.gz`) and registers as the
   `RUST` script-language alias.
6. Update the SDK/SLC version references in this repo's `CLAUDE.md` "Build" section:
   change `exasol-udf-sdk` **0.20.2** + `exasol-udf-macros` **0.20.2** and the
   "v0.20.2 SLC fingerprint" mention to `0.20.3`. Do not touch the unrelated
   `language-container-rs 0.14.0` multi-entry-point capability reference.
7. **Verify the rustc fingerprint pairing holds for 0.20.3.** [expert]
   `EXA_SDK_FINGERPRINT = "{exasol-udf-sdk version}:{rustc_hash}"` is checked at UDF
   load; a mismatch rejects the `.so` with `F-UDF-CL-RUST-9001: Fingerprint mismatch`.
   Inspect the lc-rs v0.20.3 release/Dockerfile (or the SLC build metadata) to confirm
   it is built with the same rustc as this repo's `rust:1.94-bookworm` UDF builder image.
   If the builder rustc diverges from 1.94, the UDF builder image and CLAUDE.md "Build"
   section must be updated in lockstep — flag this as a blocking risk and raise it before
   rebuilding rather than assuming it matches. (This is the one non-mechanical step: it
   requires reasoning about the SDK-version:rustc-hash pairing and cross-checking an
   external release artifact. If the release page is unreachable, do not assume a match —
   the `make test-e2e` UDF-load step (task 9) is the authoritative check and will surface
   a mismatch as a load failure.)
8. Rebuild the `.so` via `make cross-musl-udf-build` (inside `rust:1.94-bookworm`; never
   `cargo build --release` on the host). Make's mtime check rebuilds only the changed
   UDF crate. (Depends on tasks 2-4, 7.)
9. Run `make test-e2e` against the local Exasol Docker stack. This also runs
   `install-slc`, which downloads and registers the 0.20.3 SLC, so the run exercises the
   real SDK↔SLC fingerprint pairing end-to-end — a mismatch fails UDF load rather than
   silently falling back to the old formatter path. (Depends on tasks 5, 8.)
10. Run `cargo test`, `cargo clippy --all-targets`, and `cargo fmt --check` clean.
    (Depends on tasks 2-4.)

## Parallelization

| Parallel Group | Tasks |
|----------------|-------|
| Group A (manifest + doc edits + fingerprint research) | 2, 3, 5, 6, 7 |
| Group B (lock refresh) | 4 |
| Group C (host checks) | 10 |
| Group D (build) | 8 |
| Group E (e2e) | 9 |

Sequential dependencies:
- Task 1 (file issue) precedes all edit work.
- Group A → Group B (lock refresh depends on manifest edits 2, 3).
- Group A + Group B → Group C (host `cargo` checks depend on manifests + lock).
- Group A + Group B → Group D (build depends on manifests + lock + fingerprint check).
- Group D → Group E (e2e depends on the freshly built `.so`; also needs task 5).

## Dead Code Removal

None. A version bump removes no code; no obsolete functions, tests, or modules result.

## Verification

### Scenario Coverage

No new or changed scenarios — this is a dependency-version bump with no behavioral
change, so no spec deltas exist to map. Existing behavior remains covered by the current
integration/E2E suite, which is exercised end-to-end by `make test-e2e`. The version
bump is proven correct by the SLC↔`.so` fingerprint pairing loading cleanly (task 9) and
the existing suite passing unchanged.

| Scenario | Test Type | Test Location | Test Name |
|----------|-----------|---------------|-----------|
| _(none — no spec delta)_ | — | Existing E2E suite via `make test-e2e` | existing tests unchanged |

### Manual Testing

| Feature | Command | Expected Output |
|---------|---------|-----------------|
| lc-rs 0.20.3 SLC/SDK pairing loads | `make test-e2e` | SLC 0.20.3 installs and registers; UDF `.so` loads (no `F-UDF-CL-RUST-9001` fingerprint error); all E2E queries return correct results, exit 0 |
| SLC version registered | inspect `install-slc` output during `make test-e2e` | Downloads `lc-rust-0.20.3.tar.gz` and registers it under the `RUST` alias |

### Checklist

| Step | Command | Expected |
|------|---------|----------|
| Build (`.so`) | `make cross-musl-udf-build` | Exit 0; `.so` rebuilt in `rust:1.94-bookworm` |
| Test (E2E) | `make test-e2e` | Exit 0; 0 failures; UDF loads under 0.20.3 SLC |
| Test (host) | `cargo test` | 0 failures |
| Lint | `cargo clippy --all-targets` | 0 errors/warnings |
| Format | `cargo fmt --check` | No changes |
