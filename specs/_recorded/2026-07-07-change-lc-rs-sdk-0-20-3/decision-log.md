# Decision Log: change-lc-rs-sdk-0-20-3

Date: 2026-07-07

## Interview

Headless run — no live interview was conducted. Requirements were supplied by the
requester and are captured below as Q/A-shaped intent for the record.

**Q:** What is the change and why?
**A:** Bump `language-container-rs` (lc-rs) / `exasol-udf-sdk` / `exasol-udf-macros` and
the downloaded Rust SLC from 0.20.2 to 0.20.3, to pick up lc-rs v0.20.3's fast
string-block encoding for emit/ingest (PR #44, resolves lc-rs issue #29). It ships the
optimization unconditionally — no new API, no feature flag, no `UdfContext` trait change,
`EXA_UDF_ABI_VERSION` stays 6. Measured +28% to +46% on a live Exasol DB for a
NUMERIC/DATE/TIMESTAMP-heavy row shape. Drop-in bump; this repo's emit/ingest paths
benefit automatically without code changes.

**Q:** Should a GitHub tracking issue be filed?
**A:** Yes — project convention (CLAUDE.md "Feature tracking"): file via
`ghbrk gh issue create` before/at the start of work and reference it with `Closes #<n>`
in the implementing commit. Include filing it as the first task (or note git-pr-agent
files it).

**Q:** Which files carry the version and must change?
**A:** `/Cargo.toml` (`exasol-udf-sdk`), `crates/lakehouse-engine/Cargo.toml`
(`exasol-udf-sdk` + `exasol-udf-macros`), `Cargo.lock` (via `cargo update`), `Makefile`
(`SLC_VERSION`), and the two SDK version references in this repo's `CLAUDE.md` Build
section.

**Q:** Any compatibility risk to verify?
**A:** Yes — the SLC↔`.so` rustc fingerprint (`EXA_SDK_FINGERPRINT`). lc-rs 0.20.3 must
be built with the same rustc as the `rust:1.94-bookworm` UDF builder or the `.so` is
rejected at load with `F-UDF-CL-RUST-9001: Fingerprint mismatch`. Not verified by the
requester — treat it as a verification task/risk, do not assert it matches.

**Q:** How is the SDK/SLC pairing actually proven?
**A:** Rebuild the `.so` with `make cross-musl-udf-build`, then `make test-e2e` (which
runs `install-slc` and loads the UDF against the local Exasol Docker stack). A mismatch
fails UDF load rather than silently using the old formatter path. Also run `cargo test`,
`cargo clippy --all-targets`, `cargo fmt --check` clean.

**Q:** Does this warrant new spec deltas?
**A:** Default to the minimal-footprint option (task-only plan, no spec deltas) unless a
spec explicitly pins the 0.20.2 version or documents version-specific behavior that would
go stale — in which case update only that reference.

## Design Decisions

### [1] No spec deltas — task-only plan

- **Decision:** Produce no permanent-spec (`specs/<domain>/<feature>/spec.md`) deltas.
  The change is tracked solely by `plan.md` and the GitHub tracking issue.
- **Alternatives:** (a) Author a delta under `packaging/` to record the SDK version.
  (b) Update the stale version reference found in
  `specs/packaging/single-so-two-entry-points/spec.md`.
- **Rationale:** A version bump with zero API/ABI/behavioral change from this repo's
  perspective adds no scenario and changes no documented behavior. A search of the
  permanent spec library found exactly one SDK version pin — line ~10 of
  `single-so-two-entry-points/spec.md` — and it reads `exasol-udf-sdk` **0.14.0**, not
  0.20.2. It is pre-existing narrative drift unrelated to a 0.20.2→0.20.3 bump; editing
  it here would be scope creep that fixes an older drift this plan did not cause.
  (Other 0.20.2 mentions live only in `_recorded/`, `backlog.md`, and the top-level
  `decision-log.md` — archival records, not permanent feature specs.)
- **Promotes to ADR:** no

### [2] Stale 0.14.0 reference in single-so-two-entry-points is out of scope

- **Decision:** Leave the `exasol-udf-sdk`/`exasol-udf-macros` **0.14.0** reference in
  `specs/packaging/single-so-two-entry-points/spec.md` untouched; note it as a future
  spec-hygiene candidate.
- **Alternatives:** Bump it to 0.20.3 as part of this plan.
- **Rationale:** It is unrelated to this bump (already stale at 0.14.0 while the repo is
  on 0.20.2) and a narrative bullet rather than a scenario clause. Fixing it belongs to a
  dedicated spec-hygiene pass so this dependency bump stays minimal and reviewable.
- **Promotes to ADR:** no

### [3] Fingerprint verification is a first-class, non-mechanical task

- **Decision:** Tag the rustc-fingerprint compatibility check as `[expert]` and gate the
  rebuild on it; treat a builder-rustc divergence from 1.94 as a blocking risk.
- **Alternatives:** Treat the bump as purely mechanical and rely only on `make test-e2e`
  to surface any mismatch.
- **Rationale:** `EXA_SDK_FINGERPRINT = "{sdk version}:{rustc_hash}"` is checked at UDF
  load; an SLC built with a rustc other than the `rust:1.94-bookworm` builder's rejects
  the `.so` with `F-UDF-CL-RUST-9001`. Reasoning about the version:hash pairing and
  cross-checking the external release artifact is the one step that needs judgment, not
  rote editing. `make test-e2e` remains the authoritative end-to-end check, but catching
  a divergence before the rebuild saves a wasted build cycle and documents the required
  builder-image/CLAUDE.md lockstep update if it diverges.
- **Promotes to ADR:** no

## Review Findings

<!-- Populated by speq-implement after code review. -->

### Plan gap found during verification: hardcoded SLC_VERSION in test harness

`make test-e2e` initially failed with `F-UDF-CL-RUST-9001: Fingerprint mismatch: expected
0.20.2:... found 0.20.3:...`. Root cause: each of the 5 E2E test files
(`e2e_scan_test.rs`, `e2e_capability_test.rs`, `e2e_count_distinct_test.rs`,
`e2e_join_test.rs`, `e2e_positional_deletes_test.rs`) has its own
`const SLC_VERSION: &str = "0.20.2"`, independent of the `Makefile`'s `SLC_VERSION` var,
used by that file's in-process `install_slc()` to download+register the SLC. The plan's
task 5 only covered the `Makefile` variable. Fixed by bumping all 5 consts to `"0.20.3"`;
re-ran `make test-e2e` clean.

### Code review: one fix applied, one follow-up deferred

`code-reviewer` flagged 4 stale `0.20.2` narrative comments in `e2e_scan_test.rs` (lines
11, 94, 108, 149) left behind by the const bump — fixed in this PR (comment-only, no
rebuild needed).

Flagged, deliberately deferred: the 5x duplicated `const SLC_VERSION` mirrors an
already-blessed duplication pattern (`e2e_capability_test.rs`'s own
`// ponytail: duplicate of e2e_scan_test setup` comment). A shared `tests/common/mod.rs`
constant would collapse 5 bump sites to 1 and remove exactly this "miss one, get a
fingerprint mismatch" hazard — but it changes test structure beyond a pure dependency
bump, so it's left as a follow-up rather than folded into this PR.
