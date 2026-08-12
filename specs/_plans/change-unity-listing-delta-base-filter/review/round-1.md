# Plan Review Findings: change-unity-listing-delta-base-filter (round 1)

## Summary
- Axes checked: 6/6
- Total findings: 6 (Blockers: 2, Advisory: 4)
- Intent Fidelity blockers: 0

## Premortem

Three failure stories drove this review:

1. **Silent empty schema on OSS.** The filter admits an entry only when `data_source_format == DELTA`; an entry lacking the field defaults to `None` and is excluded. If OSS Unity Catalog's `GET /tables` list response does not populate `data_source_format` (spike-verified only against Databricks), every OSS Delta fixture is dropped — `make test-e2e-unity` returns an empty schema and real OSS deployments list zero tables. → Feasibility [UNSTATED_ASSUMPTION].
2. **Plan-internal red test.** A client test fixture that omits `data_source_format` on a `MANAGED` entry is silently excluded by the new filter, so a test asserting the entry is returned fails — yet no task updates that fixture. → Requirement Quality [COMPLETENESS_GAP].
3. **Stale permanent spec.** The plan extends the `lakehouse-catalog` public surface (new `pub` types, changed field shape) and edits its reachability probe, but authors no delta for the spec that pins that surface, so `/speq:record` merges an enumeration the code contradicts. → Requirement Quality [REQUIREMENT_CONFLICT].

## Intent Fidelity

No objection — axis checked. The filter rule (`table_type` ∈ {MANAGED, EXTERNAL} AND `data_source_format` == DELTA; VIEW / non-DELTA / other excluded; shallow clones included as MANAGED/EXTERNAL+DELTA; one warn line per exclusion) matches interview A1/A2 exactly. The recorded VIEW-listing scenario is correctly inverted: `unity-catalog-create-virtual-schema` marks "Create virtual schema lists a Unity Catalog view with its columns and no storage location" DELTA:REMOVED and adds the exclusion NEW scenario; `unity-catalog-client` supersedes the VIEW clause of the CHANGED list scenario. No drift, creep, or silent reduction. The Iceberg-path `SkipReason` refactor traces directly to invariant #2 (byte-identical Iceberg warning) plus interview A2 (Unity-specific reason), not gold-plating.

## Feasibility

#### [UNSTATED_ASSUMPTION] ADVISORY
- Location: plan.md § Verification → Manual Testing (row 3, `make test-e2e-unity`); decision-log.md § Design Decisions [6]
- Issue: The plan asserts the OSS regression "still list[s] correctly" and decision [6] justifies adding no OSS exclusion fixtures because "the wire vocabulary is spike-verified against live Databricks." But `SPIKE_UC_CLIENT.md` line 73 confirms `data_source_format` in the **list** response only against live Databricks; line 98 states OSS "list/get/temp-creds" paths work without confirming the OSS list emits `data_source_format`. The whole filter turns on that field being present and uppercase `DELTA`. If OSS UC (the #325 harness) omits or lower-cases it, all-Delta fixtures are excluded (`None`/`"delta"` ≠ `"DELTA"`), so `make test-e2e-unity` yields an empty schema and — more seriously — production OSS deployments silently list zero Delta tables, a regression from #318's list-everything behavior. This is exactly the "no assumptions without checking against a running instance" discipline in CLAUDE.md.
- Fix: Before implement, verify the OSS #325 `GET /tables` list response carries `data_source_format=DELTA` for the vendored delta-kernel-rs fixtures; record the result in decision-log.md. If it does not, seed the field into the #325 fixtures or adjust the filter's absent-value handling, and state the OSS dependency in the plan.md Manual Testing "Expected Output" cell. If it cannot be verified pre-implement, record it as an explicit tracked assumption with the same rigor as decision [5].

#### [UNSTATED_ASSUMPTION] ADVISORY
- Location: plan.md § Dependencies (shallow-clone bullet); decision-log.md [5]
- Issue: The shallow-clone wire-shape claim is recorded as an assumption (good), but its follow-up is only "SHOULD be verified ... (see decision log)". A decision-log note with no issue, owner, or milestone is not a *tracked* follow-up under the project verification discipline — it is a note that will not resurface.
- Fix: In plan.md § Dependencies, replace "see decision log" with a concrete tracked follow-up — create a GitHub issue for live/OSS shallow-clone wire-shape verification (or fold it into #323's fixture-matrix scope) and cite its number inline, matching the `Closes #<n>` / `(#27)` tracking pattern the project uses.

## Requirement Quality

#### [COMPLETENESS_GAP] BLOCKER
- Location: plan.md § Implementation Tasks Task 5 (and Task 7); `crates/lakehouse-catalog/src/unity/client_tests.rs` `follows_pagination_across_pages` (lines 172, 178)
- Issue: `follows_pagination_across_pages` builds two `GET /tables` page bodies whose entries are `{"table_type":"MANAGED", ...}` with **no** `data_source_format`, then asserts `listing.tables` maps to `vec!["t1", "t2"]`. Under the new filter (`data_source_format` serde-defaults to `None` ≠ `DELTA`), both entries are routed into `skipped`, so `listing.tables` is empty and the assertion fails. This is the identical trap the plan explicitly handles for the engine `table_entry` fixture (Task 6), left unhandled here. Task 5 enumerates only `lists_tables_in_catalog_schema` plus two new tests; Task 7 covers only the `skipped` element-type change (this test does not touch `skipped`), so no task fixes it. Following the plan literally leaves `cargo test -p lakehouse-catalog` red, breaking the plan's own Checklist ("Test: cargo test → 0 failures").
- Fix: In plan.md § Implementation Tasks Task 5, add `"data_source_format":"DELTA"` to both `MANAGED` page bodies of `follows_pagination_across_pages` (`crates/lakehouse-catalog/src/unity/client_tests.rs` lines 172 and 178), and instruct the implementer to audit every other `list_tables`-driven fixture in that file (lines 44, 187, 225, 255, 259) for a missing `data_source_format` and name each required edit.

#### [REQUIREMENT_CONFLICT] BLOCKER
- Location: decision-log.md § Design Decisions [3]; plan.md § Implementation Tasks Task 1 and Task 7; recorded spec `vs-adapter/catalog-crate-structure`
- Issue: Decision [3] performs the plan's spec-impact analysis and concludes no delta is needed for `vs-adapter/create-virtual-schema`, `vs-adapter/catalog-kind-selection`, or `vs-adapter/pushdown-catalog-session` — but it never evaluates `vs-adapter/catalog-crate-structure`, whose scenario "The crate exposes the concept-level API and hides every mechanism step" normatively enumerates the `lakehouse-catalog` `pub` set ("exactly these items SHALL be `pub`") and pins it with the reachability probe at `crates/lakehouse-catalog/tests/catalog_public_surface.rs` ("SHALL name every item of that `pub` set"). Task 1 adds `pub SkipReason` and `pub SkippedTable` to that surface and changes the public shape of `CatalogListing.skipped` from `Vec<CatalogTableIdent>` to `Vec<SkippedTable>`; Task 7 edits the probe (line 169, `skipped: vec![ident]`) to match. The prior Unity-client surface extension was itself recorded as a delta (scenario "The native Unity Catalog client extends the crate's public surface through an explicit reviewed edit"), so extending the surface without a delta this time leaves the permanent spec's enumeration contradicting the code and probe after `/speq:record` — the silent gap the probe discipline exists to prevent.
- Fix: Add a spec delta `specs/_plans/change-unity-listing-delta-base-filter/vs-adapter/catalog-crate-structure/spec.md` recording that the crate's enumerated public surface gains `SkipReason` and `SkippedTable` and that `CatalogListing.skipped` changes to `Vec<SkippedTable>`, and update decision-log.md [3] to include `catalog-crate-structure` in its considered set. If a delta is genuinely unwarranted, add a decision-log entry stating precisely why the recorded `pub`-set enumeration and its probe stay accurate after Task 7's edit.

## Task Breakdown

#### [TASK_GRANULARITY] ADVISORY
- Location: plan.md § Implementation Tasks Task 7; § Parallelization (Group 2)
- Issue: Task 7 bundles four test files and is placed in Group 2 (parallel with Tasks 2, 3, 4), but its `crates/lakehouse-engine/src/adapter/adapter_tests.rs` portion asserts on the output of `build_listing_virtual_tables` (the `build_listing_virtual_tables_matches_pre_refactor_output` test, ~lines 1658–1690), whose return shape Task 4 changes (`Vec<CatalogTableIdent>` → `Vec<SkippedTable>`). That portion cannot compile or pass until Task 4 lands, so the two are not independent as the parallel grouping claims. The remaining files (`catalog_public_surface.rs`, catalog `client_tests.rs`) depend on Task 1/Task 2 instead.
- Fix: In plan.md § Parallelization, move Task 7's `adapter_tests.rs` update into Group 3 (after Task 4), or split Task 7 per file so each file's update follows its producing task — the Iceberg-site/catalog fixtures after Task 2, `adapter_tests.rs` after Task 4.

## Design Depth

#### [INFORMATION_LEAKAGE] ADVISORY
- Location: plan.md § Implementation Tasks Task 3 and Task 1; decision-log.md [2]; Patterns table row "Single owner of the skip decision + reason"
- Issue: Decision [2] states "the client owns *why*; the adapter owns the log channel and the *sentence*," yet Task 3 has the client pre-format `SkipReason::NotDeltaBaseTable { detail: String }` as `table_type=<raw>` / `data_source_format=<raw>` — a fragment of the warn sentence. Message presentation is then co-authored across the catalog crate and the adapter, weakening the single-owner claim: a change to how the disqualifier reads forces edits on both sides.
- Fix: In Task 1/Task 3 and decision [2], carry structured data instead of a formatted string — e.g. `NotDeltaBaseTable { field: DisqualifyingField, value: String }` with `DisqualifyingField` ∈ {`TableType`, `DataSourceFormat`} — and let the adapter render the `field=value` fragment, keeping all warn wording adapter-side. If the pre-formatted string is deliberate, amend decision [2] to say message presentation is intentionally co-owned rather than adapter-owned.

## Prose Quality

No objection — axis checked. plan.md Summary leads with the conclusion in two sentences within the cap; Goals/Non-Goals/Impact start with verbs and quantify the filter rule concretely; decision-log Rationale prose is terse and unambiguous; spec-delta Background bullets name the superseded clauses precisely. No BLUF, terseness, or ambiguity violations that block actionability.
