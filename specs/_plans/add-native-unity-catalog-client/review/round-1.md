# Plan Review Findings: add-native-unity-catalog-client (round 1)

<!-- Fresh round-1 pass over the TRAIT-BASED revision (enum-matched fork → one shared
     `CatalogClient` trait). This file's earlier content reviewed the prior column-listing
     revision; it is overwritten here per the orchestrator's round-1 instruction, matching the
     established overwrite practice this file's own prior header noted. The recorded
     `[plan-review]` BLOCKER-1/BLOCKER-2 (storage-backend-enum third-selector clauses) are
     settled in decision-log.md § Review Findings, are NOT reintroduced by this revision, and
     are NOT re-litigated here. -->

## Summary
- Axes checked: 6/6
- Total findings: 6 (Blockers: 2, Advisory: 4)
- Intent Fidelity blockers: 0

## Premortem (three failure stories)

1. **The build never goes green.** An implementer follows task 1.1 and adds `async-trait` to
   `crates/lakehouse-catalog/Cargo.toml`. The pre-existing convention test
   `catalog_crate_boundary.rs::catalog_manifest_declares_no_execution_engine_dependency` — whose
   `FORBIDDEN_DIRECT_DEPENDENCIES` list contains `"async-trait"` — fails immediately, and the plan's
   own `cargo test → 0 failures` gate can never pass. Routes to Requirement Quality
   (REQUIREMENT_CONFLICT), BLOCKER.
2. **A deletion breaks an unlisted probe and slips a recorded baseline.** `resolve_table_schema`
   is deleted per task 3.3, which names only the two `tests/` files. The in-crate probe
   `src/adapter/pushdown_surface_probe_tests.rs` also imports it (item 20 of a 22-item frozen
   façade baseline) → the workspace stops compiling; and with no `vs-adapter/pushdown-module-structure`
   delta, the baseline reduction is unauthorized and would record as a silent façade change. Routes
   to Requirement Quality (REQUIREMENT_CONFLICT), BLOCKER.
3. **The one-session guarantee regresses on a literal read.** An implementer reads task 1.2
   ("builds exactly ONE `CatalogSession` … and calls `load_table` per identifier") and calls the
   trait's `load_table(&self, ident)` per identifier; because `IcebergRestCatalogClient` holds no
   session, each call rebuilds one, so a non-empty namespace performs N grants instead of one. The
   mapped test `enumeration_builds_exactly_one_session` catches it, but only after wasted cycles.
   Routes to Design Depth (SHALLOW_DESIGN), ADVISORY.

## Intent Fidelity

no objection — axis checked. The verbatim ask ("avoid two paths for Iceberg and UC … the same
Catalog abstraction … to list tables and load a table … a common trait … a common interface") is
operationalized exactly: ONE `CatalogClient` trait with `list_tables` and `load_table`, both kinds
implementing it (`catalog-crate-structure` delta Scenario 1; `unity-catalog-client` Scenario 1),
and a single listing pipeline that reads `Box<dyn CatalogClient>` and MUST NOT name or match
`CatalogKind`, gated by a source-level anti-fork probe (`catalog-kind-selection` § "The catalog
kind is matched at one construction site and nowhere else"; `unity-catalog-create-virtual-schema` §
"Both catalog kinds enumerate through one shared listing pipeline"). The deferral of the
file-planning/scan method to #319/#320 (interview A1), the `CatalogClient` naming, and the
`IcebergRestCatalogClient`-composes-`CatalogSession` receiver (interview Q6, decision [10]) are
FIXED interview decisions and are not challenged. No scope creep, no silent reduction.

## Feasibility

no objection — axis checked. The load-bearing byte-identical-refactor claims verify against the
code: `resolve_namespace_virtual_tables` (`adapter/mod.rs:320`) and `resolve_table_schema`
(`adapter/pushdown/file_resolution.rs:610`) each have exactly ONE production caller — the
createVirtualSchema listing path — so deleting them does not orphan the scan path, which resolves
files through `resolve_file_list` (`file_resolution.rs:223`), not `resolve_table_schema`.
`CatalogSession` (`session.rs:179`) holds `client`/`catalog_uri`/`auth`/`prefix` and neither
`storage` nor `creds`, confirming decision [10]'s premise. The OSS inline-columns risk that reddened
prior review is now gated: task 4.2 confirms the OSS fixture's `GET /tables` inlines `columns[]`
before the column assertion (task 4.3) is authored. Compile-level breakage from the deletions is
real but is captured as Requirement-Quality blockers below rather than duplicated here.

## Requirement Quality

#### [REQUIREMENT_CONFLICT] BLOCKER
- Location: plan.md § Dependencies (line 109) and § Implementation Tasks 1.1 (line 114); `vs-adapter/catalog-crate-structure` delta § Background (line 7)
- Issue: The recorded `vs-adapter/catalog-crate-structure` Scenario "The catalog access layer lives in a standalone crate the engine depends on one way" (recorded spec line 40) states: "`lakehouse-catalog`'s manifest MUST NOT declare `arrow`, `parquet`, `datafusion`, `object_store`, `roaring`, `async-trait`, `tracing`, or `exasol-udf-macros` as a direct dependency". This is enforced by an existing test — `crates/lakehouse-catalog/tests/catalog_crate_boundary.rs::catalog_manifest_declares_no_execution_engine_dependency` — whose `FORBIDDEN_DIRECT_DEPENDENCIES` array lists `"async-trait"` (line 25) and asserts the manifest declares none of them. Plan task 1.1 says "Add `async-trait` to the crate manifest" and § Dependencies says `lakehouse-catalog` "gains a manifest line only" — the exact forbidden action. The catalog-crate-structure delta supersedes only the `pub`-item enumeration and the `list_namespace_tables` demotion (delta Background line 7); it does NOT supersede the manifest MUST-NOT clause, and no task edits `catalog_crate_boundary.rs`. The plan's note that async-trait is "already a direct `lakehouse-engine` dependency present in `Cargo.lock`" is irrelevant: the trait lives in `lakehouse-catalog`, the crate the ban and the test cover, and async-trait is NOT a workspace dependency, so this is a genuinely new manifest declaration. Result: `cargo test` (plan Checklist) cannot pass, and `/speq:record` would merge a manifest line the recorded spec forbids.
- Fix: Pick one and record it. Option A — make `CatalogClient` dyn-compatible WITHOUT `async-trait`: declare each trait method to return `Pin<Box<dyn Future<Output = Result<…, UdfError>> + Send + '_>>` (no new dependency); delete "Add `async-trait` to the crate manifest" from task 1.1 and the async-trait sentence from § Dependencies. Option B — add an explicit supersession to the `vs-adapter/catalog-crate-structure` delta removing `async-trait` from the recorded MUST-NOT-declare list (rationale: async-trait is a proc-macro that reaches no execution-engine code), add a task to edit `crates/lakehouse-catalog/tests/catalog_crate_boundary.rs` dropping `"async-trait"` from `FORBIDDEN_DIRECT_DEPENDENCIES`, and correct the § Dependencies wording.

#### [REQUIREMENT_CONFLICT] BLOCKER
- Location: plan.md § Implementation Tasks 3.3 (line 127), § Dead Code Removal (line 161), § Features (lines 90-100)
- Issue: Deleting `resolve_table_schema` and its `pub use` (task 3.3) removes an item from the `pushdown` module's frozen public façade. That façade is pinned by TWO probes: `crates/lakehouse-engine/tests/pushdown_public_surface.rs` (external) AND `crates/lakehouse-engine/src/adapter/pushdown_surface_probe_tests.rs` (in-crate). The in-crate probe's `use` list is a 22-item frozen baseline (lines 19-26) that includes `resolve_table_schema` (line 24), and its own doc comment (lines 15-16) states: "Changing the set or the count requires a spec delta against `vs-adapter/pushdown-module-structure`." Task 3.3 names only `tests/pushdown_public_surface.rs` and `tests/catalog_session_signatures.rs` — it does NOT edit `src/adapter/pushdown_surface_probe_tests.rs`, so the workspace fails to compile after the deletion (unresolved import). And the plan includes NO delta against `vs-adapter/pushdown-module-structure` (§ Features has no such row), so the façade-baseline reduction (22→21 in-crate, and the external baseline) is unauthorized and would record as a silent structural change.
- Fix: Add a CHANGED delta `vs-adapter/pushdown-module-structure` recording the façade-baseline reduction (drop `resolve_table_schema`; state the new counts for both probes) and add it to plan.md § Features. Add `crates/lakehouse-engine/src/adapter/pushdown_surface_probe_tests.rs` to task 3.3's edit list: drop the `resolve_table_schema` import and change the "22-item" count in its doc comment to 21. Reflect the probe-file edit in the § Dead Code Removal `resolve_table_schema` row.

#### [AMBIGUOUS_REQUIREMENT] ADVISORY
- Location: `vs-adapter/unity-catalog-client/spec.md` Scenario 1 (line 24) and Scenario 2 (line 33); `vs-adapter/unity-catalog-create-virtual-schema/spec.md` § Background (line 7) and § "Unity Catalog Spark column types map to Exasol types sufficient for listing" (line 44)
- Issue: The neutral column's source-tagged Unity descriptor is specified as carrying "its name and Unity Catalog type name" (client Scenarios 1-2), with the create-vs Background giving bare scalar examples "`LONG`, `STRING`, `INT`". But the mapping scenario requires "`DECIMAL(p,s)` with `p` at most 36 and `s` at most 36" to be declared "`DECIMAL(p,s)`". A bare `type_name` ("DECIMAL") carries no precision or scale, so the exhaustive mapping match cannot produce `DECIMAL(p,s)` from the descriptor as described. The descriptor's content is under-specified for the DECIMAL case the plan places in scope, so the mapping is not testable as written for a parameterized decimal.
- Fix: In `unity-catalog-client` Scenarios 1-2 and the create-vs Background and Spark-types scenario, specify that the Unity source-tagged descriptor carries the full parameterized Spark type — the type name plus precision and scale (from `type_precision`/`type_scale`, or `type_text`) — sufficient to declare `DECIMAL(p,s)`, not the bare `type_name` alone.

## Task Breakdown

#### [TRACEABILITY_GAP] ADVISORY
- Location: plan.md § Implementation Tasks 3.3 (line 127) and § Dead Code Removal (line 160); test `create_virtual_schema_over_empty_namespace_contacts_no_catalog_session` at `crates/lakehouse-engine/src/adapter/adapter_tests.rs:1718`
- Issue: The plan deletes `resolve_namespace_virtual_tables`, but the existing test `create_virtual_schema_over_empty_namespace_contacts_no_catalog_session` (adapter_tests.rs:1718) calls it directly at line 1746 — the very line decision [10] and the interview brief cite as the guarantee anchor. Neither the task list nor the Dead Code Removal table states this test's fate. The empty-namespace no-grant guarantee is relocated to a new `crates/lakehouse-catalog/src/client_tests.rs::empty_namespace_builds_no_session_and_no_grant`, but the old engine test is left dangling: it breaks compilation, and if it is simply deleted without confirming the new test reproduces the identical scenario (unreachable URI + OAuth client-credentials + empty namespace → success), the guarantee can silently weaken.
- Fix: In task 3.3 (or the § Dead Code Removal table) explicitly name `create_virtual_schema_over_empty_namespace_contacts_no_catalog_session` for migration: state that its unreachable-URI + OAuth + empty-namespace assertion moves to the catalog-crate test against `IcebergRestCatalogClient::list_tables`, and that the engine test is removed once `resolve_namespace_virtual_tables` is deleted.

## Design Depth

#### [SHALLOW_DESIGN] ADVISORY
- Location: plan.md § Implementation Tasks 1.2 (line 115); decision-log.md § Design Decision [9] Rationale (line 87)
- Issue: Task 1.2 says `list_tables` "builds exactly ONE `CatalogSession` for the whole enumeration and calls `load_table` per identifier." The trait method is `load_table(&self, ident: &CatalogTableIdent)` (task 1.1), and `IcebergRestCatalogClient` holds only `catalog_uri`/`storage`/`creds` — no session (`CatalogSession` has no storage/creds fields; its sole constructor is `resolve(catalog_uri, warehouse, creds)`). If `list_tables` calls the trait `self.load_table(ident)` per identifier, each call rebuilds a session → N sessions, contradicting the one-session guarantee that the mapped test `enumeration_builds_exactly_one_session` pins. The consistent design uses a private session-taking per-identifier helper, which leaves the trait `load_table` with NO Iceberg production caller in #318 — falsifying decision [9]'s claim that `load_table` "is the Iceberg listing path's own per-table step, promoted to the trait." `load_table` is still warranted (the user asked for "list tables and load a table"; it is the #319/#320 scan-path single-table load), so it is NOT YAGNI — but the task wording is a regression trap and the recorded justification is inaccurate.
- Fix: Reword task 1.2 so `list_tables` builds one `CatalogSession` and reuses it across identifiers via a private per-identifier helper DISTINCT from the trait `load_table`. In decision-log [9], restate load_table's warrant as the user-requested single-table load and the #319/#320 scan-path source, not "the listing path's own per-table step."

## Prose Quality

#### [PROSE_BLOAT] ADVISORY
- Location: plan.md § Summary (lines 5)
- Issue: The Summary's two sentences run ~30 and ~38 words, each exceeding the 25-word cap in `/speq:writing-guardrails`. The first also stacks two "Iceberg REST" clauses ("beside Iceberg REST … defaults to Iceberg REST") before reaching the load-bearing conclusion (one shared `CatalogClient` surface).
- Fix: Split the Summary into shorter sentences, each ≤25 words, leading with the conclusion that both catalog kinds share one `CatalogClient` operation surface.
