# Plan Review Findings: change-unity-listing-delta-base-filter (round 2)

## Summary
- Axes checked: 6/6
- Total findings: 1 (Blockers: 0, Advisory: 1)
- Intent Fidelity blockers: 0

## Premortem

Two failure stories drove the round-2 pass:

1. **Red test survives the revision.** BLOCKER 1's fix names one fixture but the audit misses another `list_tables`-driven fixture that omits `data_source_format`, so `cargo test -p lakehouse-catalog` is still left red. → Checked against the actual fixture file; audit is complete and correct.
2. **New delta contradicts the feature it edits.** The added `catalog-crate-structure` delta supersedes a scenario clause that does not match the recorded text, or records a `pub`-set shape the code will not produce, so `/speq:record` merges a self-contradicting spec. → Checked against the recorded spec via the CLI; supersession is verbatim-faithful.

## Round-1 Blocker Recheck

- **Resolved: [COMPLETENESS_GAP] catalog client test fixtures omit `data_source_format`.** Task 5 now instructs adding `"data_source_format":"DELTA"` to BOTH `MANAGED` page bodies of `follows_pagination_across_pages` (verified: `crates/lakehouse-catalog/src/unity/client_tests.rs` lines 172 and 178 carry `table_type:"MANAGED"` with NO `data_source_format`, so under the new filter both route to `skipped` and the `vec!["t1","t2"]` assertion at line 197 goes red without the edit). The task's audit of every other `list_tables`-driven fixture is complete and accurate against the file: `lists_tables_in_catalog_schema`/`tables_page_body` (line 15 `orders` already carries `DELTA`; `orders_summary` VIEW at line 19 is intentionally skipped, covered by the task's assertion update); `request_failure_is_credential_safe_error` (500 path, errors before classification, lines 206–241); `identical_request_shape_oss_and_databricks`/`empty_tables_body` (`{"tables":[]}`, lines 34–36, 243–279); `single_table_body` drives `load_table`, not `list_tables` (lines 26–32, 120, 149). No `list_tables` caller is left unaudited.

- **Resolved: [REQUIREMENT_CONFLICT] missing catalog-crate-structure delta for the extended public surface.** `specs/_plans/change-unity-listing-delta-base-filter/vs-adapter/catalog-crate-structure/spec.md` now exists. Its DELTA:CHANGED reproduces recorded scenario "One shared catalog-client trait and its neutral types become the crate's operation surface" and edits only the final listing-type clause — from the recorded "a listing type carrying the resolved tables plus the identifiers the catalog reported as not loadable" (confirmed verbatim via `speq feature get`) to a skipped set pairing an identifier with a neutral reason; the rest of the scenario is byte-preserved. Its DELTA:NEW scenario records `SkipReason` and `SkippedTable` joining the `pub` surface, `CatalogListing.skipped` becoming `Vec<SkippedTable>`, the crate-private `data_source_format` constraint, and the reachability-probe edit. plan.md § Features adds `catalog-crate-structure | CHANGED` (line 68); decision-log [3] now lists `vs-adapter/catalog-crate-structure` in its considered set and explains why a delta is required for it and not the three behavioral specs. The DELTA:NEW follows the same SUPERSEDE-in-a-later-scenario pattern the recorded feature already uses for its two prior surface extensions, so no contradiction with the base "exactly these items SHALL be `pub`" enumeration.

## Intent Fidelity

No objection — axis checked. The revisions add no scope. The new `catalog-crate-structure` delta records the internal Rust-crate surface change (`SkipReason`, `SkippedTable`, `Vec<SkippedTable>`) that Task 1 already required; it is a spec-fidelity recording, not new behavior, and the Impact section correctly frames it as an internal `.so` API surface, not a user- or operator-facing change. The filter rule, the VIEW inversion, and the per-exclusion warn behavior still match interview A1/A2 exactly across all three deltas.

## Feasibility

No objection — axis checked. Every file the revised task list names exists: `crates/lakehouse-catalog/src/client_tests.rs`, `crates/lakehouse-catalog/tests/catalog_public_surface.rs`, `crates/lakehouse-engine/src/adapter/adapter_tests.rs`, and `crates/lakehouse-engine/src/adapter/catalog_client_tests.rs`. The two advisory-acted-on preconditions (OSS `data_source_format` presence; shallow-clone wire shape) are now tracked inline against #323 per the project `(#n)` pattern rather than an ephemeral decision-log note.

## Requirement Quality

No objection — axis checked. Both round-1 BLOCKERs are resolved (see recheck). The three spec deltas are well-formed: matched DELTA open/close markers throughout, and each delta's SUPERSEDES clauses quote the recorded text they replace. The `catalog-crate-structure` DELTA:NEW and DELTA:CHANGED both describe the `CatalogListing.skipped` reshape at compatible specificity (conceptual in the trait scenario, concrete `Vec<SkippedTable>` in the surface scenario) with no conflict.

## Task Breakdown

#### [TRACEABILITY_GAP] ADVISORY
- Location: plan.md § Implementation Tasks Task 8 (and § Parallelization sequential-dependencies note); `crates/lakehouse-engine/src/adapter/adapter_tests.rs`
- Issue: Task 8 and the Parallelization note name the target test `build_listing_virtual_tables_matches_pre_refactor_output`, but no test by that name exists in the file (or the crate) — `grep` returns zero matches. The actual test that constructs `skipped: vec![cat_ident(...)]` (line 1658) and asserts on the returned `skipped` (line 1690) is `iceberg_listing_is_behavior_identical_behind_the_trait` (declared line 1637). The cited line numbers (1658, 1690) and the described change (the `skipped` element type becomes `SkippedTable` when Task 4 reshapes `build_listing_virtual_tables`'s return) are correct, so the task stays executable via the line anchors — hence advisory, not blocking. The wrong name was carried forward verbatim from the round-1 Task 7 into the new Task 8 during the split.
- Fix: In plan.md § Implementation Tasks Task 8 and the § Parallelization sequential-dependencies note, rename the referenced test from `build_listing_virtual_tables_matches_pre_refactor_output` to `iceberg_listing_is_behavior_identical_behind_the_trait`.

The Task 7 → Task 7 + Task 8 split is otherwise sound. Verified the dependency assignments against the files: Task 7's `catalog_public_surface.rs` and `crates/lakehouse-catalog/src/client_tests.rs` assert on Task 1's types and Task 2's Iceberg 404-skip (`unloadable_table_is_reported_skipped_not_failed`, line 489 asserts `listing.skipped` from `resolve_listing`), so "after Tasks 1 and 2" is correct; Task 8's `adapter_tests.rs` destructures and asserts on Task 4's reshaped return, so "after Task 4" is correct. All four Group-3 test tasks touch disjoint file sets, so the parallel grouping holds.

## Design Depth

No objection — axis checked. The declined round-1 INFORMATION_LEAKAGE advisory is now a recorded deliberate trade-off, and it is reflected consistently everywhere: decision [2]'s "Message wording is intentionally co-owned" paragraph, the plan.md Patterns row retitled "Single owner of the skip decision; co-owned wording", and all three deltas (each says `NotDeltaBaseTable` carries the disqualifying `table_type`/`data_source_format` as neutral detail, the client sets the reason, the adapter renders the sentence). No dangling single-owner-of-wording claim survives elsewhere. The `data_source_format`-stays-crate-private invariant is preserved in every delta — only its rendered value travels inside the opaque `NotDeltaBaseTable` detail, never as a matchable neutral field.

## Prose Quality

No objection — axis checked. The new `catalog-crate-structure` delta's feature-description line leads with a verb and stays within the two-sentence structure; its Background bullets name the superseded clause precisely and quote it. decision-log [3]'s revised Rationale and the amended decision [2] paragraph are terse and front-loaded. No BLUF, terseness, or ambiguity violation that blocks actionability.
