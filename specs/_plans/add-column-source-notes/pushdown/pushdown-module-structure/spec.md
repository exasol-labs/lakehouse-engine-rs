# Feature: Pushdown Module Structure

Decomposes the virtual-schema pushdown-planning code into single-responsibility submodules behind a preserved public façade, keeps behavior byte-identical, and co-locates each submodule's tests.

The running history of internal-duplication extractions this decomposition made room for — the shared dispatch base, the shared fallback-guard helper, the shared request-shape classifier, the shared column-collecting and type-rewrite traversal primitives, and the shared type-rewrite pipeline — is tracked separately in `pushdown/pushdown-module-dedup-consolidation`, split out once that history's scenario count crossed this library's per-spec organization threshold.

## Background

* The refactor changes code organization only. It changes no query, pushdown, file-pruning, or type-handling behavior, so every scenario in the `pushdown-planning*` and `file-planning/pushdown-file-pruning` features stays accurate and unedited.
* The pushdown planning layer decomposes into cohesive capability submodules (catalog credentials, file resolution, single-group aggregate, grouped aggregate, joins, top-N, namespace listing) plus one shared support submodule for cross-cutting SQL-builder and utility helpers. The exact submodule list is a design decision recorded in the plan, not a normative contract.
* `crate::adapter::pushdown` becomes a directory module (`pushdown/mod.rs` plus sibling files), so the import path `crate::adapter::pushdown::<name>` is unchanged for every consumer.
* A cross-submodule private helper widens to the narrowest visibility that compiles (`pub(super)`), never to a broader public than it had before.
* The CI/lint file-size guardrail (the second half of issue #129) is out of scope for this feature and remains open under issue #129.
* The frozen `crate::adapter::pushdown::<name>` façade is redrawn ONCE, deliberately, when the catalog access layer leaves the crate (issue #204). No `pushdown-planning*` scenario changes, because the redraw removes items rather than altering any decision or any generated SQL.
* The façade stays FROZEN after the redraw. This delta changes what the baseline IS, not whether there is one: the two probe files still fail the build on any unplanned narrowing, and a further change to the item set still needs its own spec delta.
* The baseline the two probes cite, `specs/_plans/refactor-adapter-pushdown-modules/public-surface-baseline.txt`, no longer exists. `/speq:record` archived it with plan `refactor-adapter-pushdown-modules` into `specs/_recorded/`, which this project gitignores, so both probes point at a path that cannot be read. The probes' own `use` lists are the only surviving baseline and are promoted to being it.
* The `credentials` submodule named in this feature's second Background bullet is dissolved by the extraction, not merely renamed: its catalog HTTP, auth, session, and vended code becomes the `lakehouse-catalog` crate. `catalog/catalog-crate-structure` owns the new boundary. The submodule list stays "a design decision recorded in the plan, not a normative contract", exactly as that bullet already says.
* `resolve_vended_storage` is deliberately NOT added to the pushdown façade as a replacement for the two items it retires. Adding it would re-create the coupling the redraw removes: a probe test in `lakehouse-engine` asserting a `lakehouse-catalog` concept through a re-export. The new crate carries its own probe instead.
* This feature's `pub(super)` visibility rule is unaffected. `redact_catalog_error` narrows out of `pushdown/support.rs` entirely — it is deleted and its callers repointed at the catalog crate's `redact_credentials` — which the rule permits — it caps how far a cross-submodule helper may WIDEN and does not forbid a helper leaving once its callers move.
* This delta redraws the frozen `crate::adapter::pushdown::<name>` façade a second deliberate time (plan `add-native-unity-catalog-client`, issue #318). `resolve_table_schema` leaves the façade because the shared `CatalogClient` listing pipeline replaces its ONLY production caller: its load-and-extract half moves into `IcebergRestCatalogClient::load_table` in `lakehouse-catalog`, and its Exasol-mapping-and-uppercasing half moves into the shared listing pipeline. No `pushdown-planning*` scenario changes, because the redraw removes one item and alters no decision and no generated SQL.
* The façade stays FROZEN after this redraw: the two probe files still fail the build on any unplanned narrowing, and a further change to the item set still needs its own spec delta.
* Both probe files are edited by this plan: `src/adapter/pushdown_surface_probe_tests.rs` drops the `resolve_table_schema` import and changes its doc-comment count from "22-item" to "21-item"; `tests/pushdown_public_surface.rs` drops the `resolve_table_schema` import and changes its doc comment from "12 items … subset of that probe's 22" to "11 items … subset of that probe's 21".
* **This delta is issue #135. It amends ONE clause of ONE scenario and changes no module boundary.** The directory layout, the frozen public façade, its external-vantage probes, the submodule-owns-its-tests rule, and every façade-admission scenario are UNCHANGED.
* **WIDENS this feature's behavior-preservation carve-out from the VARIANT TAG to the whole `storage` VALUE.** The recorded clause's own `THEN` already carves out "the scan spec's `storage` value wherever an assertion embeds one" at value granularity; only the `AND` that follows narrows to the tag, because `storage-access/storage-backend-enum` (issue #274) changed nothing but what enclosed the backend's encoding. `storage-access/scan-spec-credential-reference` replaces the value itself — a connection reference carrying no credential, or a sealed envelope over that backend's byte-identical (pre-encryption) encoding — so the two halves of the recorded scenario now agree at the same granularity.
* **The gate is not weakened by the widening.** Everything outside the `storage` value stays byte-identical, which is the property this scenario exists to prove; the eighteen credential-bearing `dispatch_golden` fixtures are regenerated for that value alone, and the six `empty_*` fixtures — which carry no `storage` value — stay byte-identical and are asserted unchanged.
* **This delta adds ONE scenario and is issue #319.** It records the format-reader seam's addition to
  the pushdown façade and the two probe counts that change with it. Both probe doc comments state
  that changing the set or the count requires a spec delta against this feature; this scenario is
  that delta.
* **The seam is a new `format` submodule of `adapter::pushdown`, not a new top-level module.** A
  top-level `format` module would have to call `adapter::pushdown::resolve_file_list` for its Iceberg
  arm, pointing a lower layer at the delivery-mechanism layer above it — and #320 will point
  `handle_pushdown` back at the seam, closing that edge into a module cycle. Placing the seam inside
  `pushdown` makes both edges within-layer sibling calls.
* **`resolve_file_list` keeps its name, its `pub` visibility, and its signature**, so the recorded
  clause that it "ALONE SHALL KEEP its name and its `pub` visibility on the façade" holds unedited and
  no existing probe entry moves.
* **No item is removed, narrowed, or widened. Only additions.** The recorded byte-identity clauses on
  generated scan-driving SQL hold unedited, because no existing code path changes.
* The two concrete format readers stay module-private and appear on NEITHER probe: the selection
  function returns a boxed trait object, so no caller — in-crate or external — names a reader type.
* **This delta is issue #320.** The façade is frozen, so the collapse of the Iceberg file resolver
  requires an explicit reviewed edit to both probes and their stated counts.
* No item is added, narrowed, or widened by this delta. Exactly one item is removed.
* **This delta adds ONE façade item and is issue #322.** The Delta type-mapping refusal
  (`delta/delta-type-mapping`) travels on `ResolvedScan`, whose new field names the columns the
  reader refused and why. `ResolvedScan` is already on both probes and already externally `pub`, and an
  external test crate reads its fields, so the field's type must be nameable at the same visibility.
  Both probe doc comments state that changing the set or the count requires a spec delta against this
  feature; this scenario is that delta.
* **No item is removed, narrowed, or widened. Exactly one is added.**
* **The refused-column list rides on `ResolvedScan` rather than on `ScanSpec`.** `ResolvedScan` is the
  adapter-internal resolution result; `ScanSpec` is the wire format. Putting the list on the wire would
  add a field the scan never reads and would need the `ScanSpec` format-neutrality rule widened for
  nothing. The Iceberg reader returns an EMPTY list, so the field is format-neutral by construction.
* **The protocol gate and the type classifier are module-private and appear on NEITHER probe.** Both
  are reached only from inside `adapter::pushdown::format`, whose submodule list this feature already
  records as "a design decision recorded in the plan, not a normative contract".
* **This delta adds ONE façade item.** `build_scan_driving_sql`'s three aggregate-only
  parameters — the per-plan `EMITS` type list, the caller-assembled merge SELECT, and the raw
  request limit — collapse into one value whose absence IS the row-scan path, replacing a
  prose-only "row scans read neither: pass `&[]`" contract. `build_scan_driving_sql` is
  already on both probes and already externally `pub`, and an external test crate calls it on
  the aggregate path, so the new parameter's type must be nameable at the same visibility.
  Both probe doc comments state that changing the set or the count requires a spec delta
  against this feature; this scenario is that delta.

## Scenarios

<!-- DELTA:NEW -->
### Scenario: The column declaration entry point replaces the Iceberg logical-schema builder on the pushdown façade

* *GIVEN* the frozen `crate::adapter::pushdown::<name>` baseline asserted by two compile-time probes: `src/adapter/pushdown_surface_probe_tests.rs` naming 27 items from an in-crate vantage and `tests/pushdown_public_surface.rs` naming the 17 externally-`pub` items
* *WHEN* the declaration of each catalog-declared column moves from the format readers' pushdown path to `createVirtualSchema`, per `vs-adapter/column-source-notes`
* *THEN* EXACTLY ONE item SHALL be added to the façade at crate visibility: the function that declares a listed table's columns from their neutral source types, and EXACTLY ONE item SHALL be removed, `build_logical_schema`, whose only production caller was the Iceberg reader's pushdown path
* *AND* the in-crate probe SHALL still name 27 items and the external probe 17, and both probe doc comments SHALL state the counts, so the swap stays visible in review
* *AND* every test that built an Iceberg logical schema through `build_logical_schema` SHALL build it through the added item, keeping its expected values unchanged
* *AND* the per-format declaration helpers (the Iceberg field builder, the Spark-type classifier, and the Delta schema classifier) MUST NOT be added to the façade, because the added item is their only caller outside `adapter::pushdown::format`
<!-- /DELTA:NEW -->

