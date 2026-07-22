# Plan: change-iceberg-datafusion-deps

## Summary

Move the three iceberg crates from the `v0.10.0-rc.2` git-tag pin to the crates.io `0.10.0` release and bump `datafusion` from `54` (resolving 54.0.0) to `54.1.0`. Both are non-breaking with no behavior delta, so this plan ships no scan/pushdown/schema behavior-relevant spec deltas; it promotes one ADR — superseding the git-tag-pin ADR `pin-iceberg-0-10-0-rc-2-via-git-tag-not-a-crates-io-exact-version-pin` — and retains the "DataFusion 54.0.0" verification-epoch anchors in ADR 014 and the date-fns spec as a frozen epoch guarded by the `exasol-e2e` `e2e_date_functions_in_filter` test (issue #107); see § DataFusion version-anchor reconciliation.

## Design

Skipped. This is a mechanical dependency-version bump, not a new feature or design change. The research findings that justify the "no behavior delta" claim are recorded below and in `decision-log.md`.

### Research findings

**All target versions are published on crates.io (2026-07-21):** `iceberg` 0.10.0, `iceberg-catalog-rest` 0.10.0, `iceberg-storage-opendal` 0.10.0, and `datafusion` 54.1.0. Verified via the crates.io API. No fabricated release — the RC-only BLOCKER condition does not apply.

**The iceberg rc.2 → final v0.10.0 delta is 25 commits / 51 files and introduces no public API change on this repo's code paths.** Verified via the GitHub compare API (`v0.10.0-rc.2...v0.10.0`) and the 0.10.0 CHANGELOG. The delta breaks down as:

| Delta category | Commits | Impact on this repo |
|---|---|---|
| CI / build tooling (toml-cli, Python-bindings cache, changelog, verify_rc, workflow trigger) | 5+ | None — not shipped |
| Transitive dep bumps: reqwest 0.12.28→0.13.3, zeroize 1.8.2→1.9.0, RUSTSEC-2026-0190/0194/0195 fixes | 5 | Lockfile only — reqwest 0.13.x now coexists with this repo's direct reqwest 0.12 (iceberg-catalog-rest private dep); no API crossing |
| iceberg-datafusion INSERT semantics (#2712 empty-insert count, #2714 reject non-append) | 2 | None — the `iceberg-datafusion` crate is not a dependency here |
| Writer: `ParquetWriterBuilder::from_table_properties` (#2561) | 1 | None — additive; `ParquetWriterBuilder::new`/`new_with_match_mode` unchanged, and the test seed uses `::new` |
| Manifest-writer encryption constructor changes (#2666, #2628) | 2 | None — the repo never constructs `ManifestWriter`/`ManifestWriterBuilder` directly; it appends via `Transaction` fast-append |

No delta commit touches `RestCatalog`/`RestCatalogBuilder`, the scan/read path, positional-delete row-selection, or the arrow/roaring/fastnum version requirements.

**datafusion 54.0.0 → 54.1.0 is non-breaking.** Verified via the 54.1.0 changelog: 19 commits, entirely bug fixes and docs (join-execution correctness, regex matching, CTE nullability, Parquet page-index loading, subquery/null-coercion). No breaking API change, no arrow version change (stays `^58.3.0`), no object_store version change (stays `^0.13.2`). The existing `datafusion = "54"` constraint already permits 54.1.0.

**Resolution invariants hold.** `roaring` stays on the 0.11 line (locked 0.11.4), `arrow`/`parquet` stay on 58 (locked 58.3.0), `fastnum` stays `>=0.7.5` (rustc 1.94 satisfied by `rust-toolchain.toml`), and the workspace stays a single arrow-58 tree end to end.

### Iceberg table-spec compliance

Per `CLAUDE.md`, changes touching scanning, pushdown, or schema/type handling must be checked against the Apache Iceberg table spec. **This bump introduces no such behavior change.** The rc.2 → final delta touches only CI, transitive dependencies, iceberg's own datafusion-INSERT integration (unused here), and additive/unused writer constructors. Iceberg scanning, snapshot/manifest reading, positional-delete application, and schema/type mapping are byte-for-byte unchanged versus the current rc.2 pin. No Iceberg-spec deviation is introduced, fixed, or exposed; no tracked exception is required.

**Compile-invisible risk (PR #67 lesson).** PR #67 (the rc.2 bump this plan supersedes) hit a runtime-only API requirement (`TableBuilder.runtime()`) that a compile-only diff did not surface; it was caught by E2E. The rc.2 → final delta contains no comparable runtime-contract change on this repo's paths, but the verification below runs the full E2E suite to guard against a compile-invisible regression regardless.

### DataFusion version-anchor reconciliation

Two recorded artifacts state "DataFusion 54.0.0" in present tense: ADR 014 (`specs/_decision/014-add-date-arithmetic-pushdown.md:81,100`) and the date-fns spec (`specs/sql-comprehension/vs-expression-translator-date-fns/spec.md:26-29,31,47,54,58`). This plan RETAINS those strings as a frozen verification-epoch marker and authors no CHANGED delta against either artifact. This resolves plan-review BLOCKER 2 via the frozen-epoch path rather than the delta-correct path (rationale in decision-log § Review Findings).

**The 54.1.0 bump does not disturb the anchored behavior.** The 54.1.0 changelog is 19 bug-fix/docs commits; none touches date/interval type coercion, the `Integer × Interval` multiply kernel, or the arrow-rs version — arrow stays 58.3.0 — so the arrow-rs#9030 plan-time error and the `is_date_minus_date → Int64` coercion that ADR 014 and the date-fns spec depend on are unchanged (verified; see decision-log Design Decision [8]). The behaviors the anchors describe still hold under 54.1.0.

**Freeze rather than rewrite.** The "54.0.0" strings record the exact version each rendering was empirically executed against ("executing each rendering through DataFusion 54.0.0", `is_date_minus_date` inspected). Rewriting them to "54.1.0" would assert a per-function parity run this plan does not perform — the plan runs the standing suite as a regression guard, not a fresh per-function rendering verification against 54.1.0. ADR 014 is an Accepted historical ADR, and this repo's convention preserves an accepted ADR's factual content and flips only its status on supersession (see `001-migrate-legacy-decision-log.md:144`), so retroactively editing "54.0.0" inside it would rewrite history. No behavior delta exists, so a CHANGED spec delta — which signals a behavior change — would misrepresent the bump.

**Standing guard.** The `exasol-e2e`-gated `e2e_date_functions_in_filter` and `*_BETWEEN` parity tests in `crates/lakehouse-engine/tests/e2e_capability_test.rs` (issue #107) execute the advertised date-function pushdowns end-to-end against live Exasol. They fail (not skip) if 54.1.0 silently changed date/interval behavior, so the frozen "54.0.0" wording stays safe under the bump. These tests run as part of the § Verification `make test-e2e` step.

## Features

No feature spec changes. This is a pure dependency-version bump with no observable behavior delta, so no `spec.md` deltas are authored.

| Feature | Status | Spec |
|---------|--------|------|
| (none) | — | — |

## Dependencies

| Dependency | From | To |
|---|---|---|
| `iceberg` | git tag `v0.10.0-rc.2` (commit be6cc96e) | crates.io `0.10.0` |
| `iceberg-catalog-rest` | git tag `v0.10.0-rc.2` | crates.io `0.10.0` |
| `iceberg-storage-opendal` | git tag `v0.10.0-rc.2` (`default-features = false`, `opendal-s3`) | crates.io `0.10.0` (same features) |
| `datafusion` | `"54"` (locked 54.0.0) | `"54.1"` (locked 54.1.0) |

## Migration

| Current | New |
|---------|-----|
| `iceberg = { git = "https://github.com/apache/iceberg-rust", tag = "v0.10.0-rc.2" }` | `iceberg = "0.10.0"` |
| `iceberg-catalog-rest = { git = ..., tag = "v0.10.0-rc.2" }` | `iceberg-catalog-rest = "0.10.0"` |
| `iceberg-storage-opendal = { git = ..., tag = "v0.10.0-rc.2", default-features = false, features = ["opendal-s3"] }` | `iceberg-storage-opendal = { version = "0.10.0", default-features = false, features = ["opendal-s3"] }` |
| `datafusion = { version = "54", ... }` | `datafusion = { version = "54.1", ... }` |
| `Cargo.lock` iceberg source `git+…?tag=v0.10.0-rc.2` | `registry+…crates.io-index`, version 0.10.0 |
| `Cargo.lock` datafusion 54.0.0 | 54.1.0 |

## Implementation Tasks

1. **Edit the workspace manifest** (`Cargo.toml`).
   - 1.1 Switch `iceberg`, `iceberg-catalog-rest`, and `iceberg-storage-opendal` from the `git`/`tag` form to the crates.io version form `"0.10.0"`, preserving `iceberg-storage-opendal`'s `default-features = false, features = ["opendal-s3"]`.
   - 1.2 Bump `datafusion = { version = "54", ... }` to `version = "54.1"`.
   - 1.3 Rewrite the RC-provenance comments (lines ~30-49): drop "not yet on crates.io"; state the crates.io `0.10.0` pin; keep the roaring-0.11 and fastnum-rustc-1.94 rationale, re-anchored to `0.10.0`. Update the datafusion comment from "54.0.0 co-resolves on arrow ^58.3.0" to "54.1.0 co-resolves on arrow ^58.3.0".
2. **Edit the crate manifest comment** (`crates/lakehouse-engine/Cargo.toml`).
   - 2.1 Update the dev-dependency comment at line ~83 from "iceberg 0.10.0-rc.2's writer stack" to "iceberg 0.10.0's writer stack".
3. **Update source and test comments that name the RC** (accuracy — the workspace no longer pins an RC).
   - 3.1 `src/scan/positional_deletes.rs` (lines ~80, ~795): change the vendored-code provenance references from tag `v0.10.0-rc.2` to `v0.10.0`.
   - 3.2 `tests/e2e_positional_deletes_test.rs` (line ~342), `tests/common/seed.rs` (lines ~4, ~1716): change `0.10.0-rc.2` to `0.10.0`.
4. **Regenerate `Cargo.lock`** against the registry.
   - 4.1 Run `cargo update -p iceberg -p iceberg-catalog-rest -p iceberg-storage-opendal` to move the three crates from the git source to the crates.io `0.10.0` registry source, and `cargo update -p datafusion --precise 54.1.0`.
   - 4.2 Confirm the lock still holds one arrow (58.3.0) and one parquet (58.3.0), `roaring` 0.11.x, `fastnum` `>=0.7.5`; confirm the iceberg sources read `registry+…crates.io-index`; note the newly-present reqwest 0.13.x line (expected, coexists with direct 0.12).
5. **Verify the vendored positional-delete function stays in sync.**
   - 5.1 Diff this repo's vendored `build_deletes_row_selection` (`src/scan/positional_deletes.rs`) against iceberg `0.10.0`'s source. The rc.2 → final commit analysis predicts no change; confirm identical. If it differs, re-sync the vendored copy and record the diff.
6. **Verify the build and full test suites** (see § Verification for commands and expected results).
7. **Verify DataFusion 54.1.0 leaves the frozen date/interval version anchors valid** (see § DataFusion version-anchor reconciliation).
   - 7.1 Confirm from the DataFusion 54.1.0 changelog (`dev/changelog/54.1.0.md`) that no commit touches date/interval type coercion, the `Integer × Interval` multiply kernel, or the arrow-rs version (arrow stays 58.3.0) / arrow-rs#9030 status. Record the confirmation in `decision-log.md` (already captured as Design Decision [8]; re-confirm against the published changelog and note any drift).
   - 7.2 Confirm the `exasol-e2e` `e2e_date_functions_in_filter` and `*_BETWEEN` parity tests (issue #107, `crates/lakehouse-engine/tests/e2e_capability_test.rs`) pass under 54.1.0 — this is covered by task 6's `make test-e2e` run; call out any failure as a real behavior regression that reopens BLOCKER 2's frozen-epoch resolution.
8. **Record-time ADR supersession** (executed by `speq record`, not by implementation).
   - 8.1 When `speq record` promotes decision-log Design Decision [2] to an ADR, it MUST flip the superseded pin ADR (`pin-iceberg-0-10-0-rc-2-via-git-tag-not-a-crates-io-exact-version-pin`, in `specs/_decision/001-migrate-legacy-decision-log.md`) from `**Status:** Accepted` to `**Status:** Superseded by <new-ADR-slug>`, following the convention at `001-migrate-legacy-decision-log.md:144` and `011-fix-count-distinct-shard-cap.md:184`. The pin ADR's factual content stays intact; only its status line changes.

## Parallelization

| Parallel Group | Tasks |
|----------------|-------|
| Group A (manifest + comment edits) | 1.1, 1.2, 1.3, 2.1, 3.1, 3.2 |
| Group B (lockfile) | 4.1, 4.2 |
| Group C (vendored-code parity) | 5.1 |
| Group D (build + test verification) | 6 (all checklist steps), 7.2 |
| Group E (changelog version-anchor review, independent) | 7.1 |
| Record-time (executed by `speq record`, outside implementation) | 8.1 |

Sequential dependencies:
- Group A → Group B (the lock regenerates from the edited manifests)
- Group B → Group C, Group D (parity check and builds run against the resolved tree)
- Group E is pure changelog review; it depends on nothing and can run alongside Group A
- Record-time task 8.1 runs only at `speq record`, after implementation and review complete

## Dead Code Removal

No code (functions, modules, tests) is removed. The only removals are stale comment fragments: the "not yet on crates.io" phrasing and the `v0.10.0-rc.2` version strings, retired in tasks 1.3, 2.1, and 3. `POSITION_DELETE_DESIGN.md` (untracked, uncommitted scratch file) also names `0.10.0-rc.2` at line 159; it is out of scope for this plan and left untouched.

## Verification

### Scenario Coverage

No scenarios change. The regression guard is the full existing suite passing unchanged against the bumped dependencies.

| Scenario | Test Type | Test Location | Test Name |
|----------|-----------|---------------|-----------|
| (no scenario delta) | Regression | entire `crates/*/tests` + unit suites | full `cargo test` + `make test-e2e` run |

### Manual Testing

| Feature | Command | Expected Output |
|---------|---------|-----------------|
| Registry resolution (iceberg + datafusion) | `cargo update -p iceberg -p iceberg-catalog-rest -p iceberg-storage-opendal && cargo update -p datafusion --precise 54.1.0` | Lock updated; iceberg sources become `registry+…crates.io-index` at 0.10.0; datafusion at 54.1.0; exit 0 |
| Single arrow-58 tree preserved | `cargo tree -i arrow` | One arrow 58.3.0 node; no arrow 57 or 59 |
| Positional-delete read path unchanged | `cargo test --features exasol-e2e positional_deletes` | 0 failures |
| Frozen date-fn version anchors hold under 54.1.0 (#107) | `cargo test --features exasol-e2e e2e_date_functions_in_filter` (and the `*_BETWEEN` parity tests) | 0 failures; confirms 54.1.0 did not change advertised date/interval pushdown behavior, so the "DataFusion 54.0.0" anchors in ADR 014 and the date-fns spec stay accurate as a frozen epoch |

### Checklist

| Step | Command | Expected |
|------|---------|----------|
| Lint | `cargo clippy --all-targets` | 0 errors/warnings |
| Format | `cargo fmt --check` | No changes |
| Test (host unit) | `cargo test` | 0 failures |
| Build (UDF .so) | `make cross-musl-udf-build` | `.so` built in `rust:1.94-bookworm`; exit 0 |
| Test (E2E) | `make test-e2e` | 0 failures; fails (not skips) if no DB |
