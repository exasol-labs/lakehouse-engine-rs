# Feature: Pushdown Module Structure

Decomposes the virtual-schema pushdown-planning code into single-responsibility submodules behind a preserved public façade, keeps behavior byte-identical, and co-locates each submodule's tests.

The running history of internal-duplication extractions this decomposition made room for — the shared dispatch base, the shared fallback-guard helper, the shared request-shape classifier, the shared column-collecting and type-rewrite traversal primitives, and the shared type-rewrite pipeline — is tracked separately in `vs-adapter/pushdown-module-dedup-consolidation`, split out once that history's scenario count crossed this library's per-spec organization threshold.

## Background

* The refactor changes code organization only. It changes no query, pushdown, file-pruning, or type-handling behavior, so every scenario in the `vs-adapter/pushdown-planning*` and `vs-adapter/pushdown-file-pruning` features stays accurate and unedited.
* The pushdown planning layer decomposes into cohesive capability submodules (catalog credentials, file resolution, single-group aggregate, grouped aggregate, joins, top-N, namespace listing) plus one shared support submodule for cross-cutting SQL-builder and utility helpers. The exact submodule list is a design decision recorded in the plan, not a normative contract.
* `crate::adapter::pushdown` becomes a directory module (`pushdown/mod.rs` plus sibling files), so the import path `crate::adapter::pushdown::<name>` is unchanged for every consumer.
* A cross-submodule private helper widens to the narrowest visibility that compiles (`pub(super)`), never to a broader public than it had before.
* The CI/lint file-size guardrail (the second half of issue #129) is out of scope for this feature and remains open under issue #129.
* The frozen `crate::adapter::pushdown::<name>` façade is redrawn ONCE, deliberately, when the catalog access layer leaves the crate (issue #204). No `vs-adapter/pushdown-planning*` scenario changes, because the redraw removes items rather than altering any decision or any generated SQL.
* The façade stays FROZEN after the redraw. This delta changes what the baseline IS, not whether there is one: the two probe files still fail the build on any unplanned narrowing, and a further change to the item set still needs its own spec delta.
* The baseline the two probes cite, `specs/_plans/refactor-adapter-pushdown-modules/public-surface-baseline.txt`, no longer exists. `/speq:record` archived it with plan `refactor-adapter-pushdown-modules` into `specs/_recorded/`, which this project gitignores, so both probes point at a path that cannot be read. The probes' own `use` lists are the only surviving baseline and are promoted to being it.
* The `credentials` submodule named in this feature's second Background bullet is dissolved by the extraction, not merely renamed: its catalog HTTP, auth, session, and vended code becomes the `lakehouse-catalog` crate. `vs-adapter/catalog-crate-structure` owns the new boundary. The submodule list stays "a design decision recorded in the plan, not a normative contract", exactly as that bullet already says.
* `resolve_vended_storage` is deliberately NOT added to the pushdown façade as a replacement for the two items it retires. Adding it would re-create the coupling the redraw removes: a probe test in `lakehouse-engine` asserting a `lakehouse-catalog` concept through a re-export. The new crate carries its own probe instead.
* This feature's `pub(super)` visibility rule is unaffected. `redact_catalog_error` narrows out of `pushdown/support.rs` entirely — it is deleted and its callers repointed at the catalog crate's `redact_credentials` — which the rule permits — it caps how far a cross-submodule helper may WIDEN and does not forbid a helper leaving once its callers move.
* This delta redraws the frozen `crate::adapter::pushdown::<name>` façade a second deliberate time (plan `add-native-unity-catalog-client`, issue #318). `resolve_table_schema` leaves the façade because the shared `CatalogClient` listing pipeline replaces its ONLY production caller: its load-and-extract half moves into `IcebergRestCatalogClient::load_table` in `lakehouse-catalog`, and its Exasol-mapping-and-uppercasing half moves into the shared listing pipeline. No `vs-adapter/pushdown-planning*` scenario changes, because the redraw removes one item and alters no decision and no generated SQL.
* The façade stays FROZEN after this redraw: the two probe files still fail the build on any unplanned narrowing, and a further change to the item set still needs its own spec delta.
* Both probe files are edited by this plan: `src/adapter/pushdown_surface_probe_tests.rs` drops the `resolve_table_schema` import and changes its doc-comment count from "22-item" to "21-item"; `tests/pushdown_public_surface.rs` drops the `resolve_table_schema` import and changes its doc comment from "12 items … subset of that probe's 22" to "11 items … subset of that probe's 21".
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
  (`vs-adapter/delta-type-mapping`) travels on `ResolvedScan`, whose new field names the columns the
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

## Scenarios

### Scenario: Public pushdown façade resolves at every pre-refactor path

* *GIVEN* a `name → visibility` snapshot of every symbol reachable via `crate::adapter::pushdown::<name>`, captured from the pre-refactor module before any code moves, as amended once by the catalog extraction's planned three-item release
* *WHEN* the same extraction re-runs against the refactored `pushdown/mod.rs` façade and all in-repo consumers compile
* *THEN* the re-extracted `name → visibility` set MUST diff empty against the CURRENT baseline — the 21-item in-crate probe `use` list and the 11-item external probe `use` list — so no reachable item is added, removed, narrowed, or widened outside a scenario that plans it
* *AND* every path `crate::adapter::pushdown::<name>` that survives the catalog extraction MUST still resolve to the same item at the same external visibility (`pub` or `pub(crate)`), the three released items being `extract_vended_keys`, `merge_vended_into_storage`, and `list_namespace_tables`
* *AND* the `adapter`, `scan`, and `capabilities` consumers MUST compile without editing any surviving `use crate::adapter::pushdown::...` path
* *AND* a `#[cfg(test)]` reachability probe naming every remaining `pub` and `pub(crate)` item from outside the `pushdown` module MUST compile, so an effective narrowing masked by a re-export is a compile error

### Scenario: The pushdown façade releases exactly the three items the catalog extraction relocates

* *GIVEN* the frozen `crate::adapter::pushdown::<name>` baseline asserted by two compile-time probes — `src/adapter/pushdown_surface_probe_tests.rs` naming 25 items from an in-crate vantage and `tests/pushdown_public_surface.rs` naming the 15 externally-`pub` items — both citing a baseline file that `/speq:record` archived into the gitignored `specs/_recorded/` tree and that therefore no longer exists
* *WHEN* the catalog access layer moves into the `lakehouse-catalog` crate and the vended mechanism functions are demoted
* *THEN* EXACTLY THREE items SHALL leave the façade and no other item SHALL be added, removed, narrowed, or widened: `extract_vended_keys` and `merge_vended_into_storage` leave because they become crate-private in `lakehouse-catalog`, and `list_namespace_tables` leaves because it relocates to `lakehouse_catalog::list_namespace_tables`
* *AND* the in-crate probe SHALL name 21 items and the external probe SHALL name 11, and both MUST compile, so any further narrowing is a build failure rather than a silent gap
* *AND* `resolve_file_list` ALONE SHALL KEEP its name and its `pub` visibility on the façade while its first parameter becomes `&lakehouse_catalog::CatalogSession`, because a signature change is not a surface change and `vs-adapter/pushdown-catalog-session` owns it; `resolve_table_schema` is DELETED from the façade by the `add-native-unity-catalog-client` plan (issue #318), recorded in the scenario below
* *AND* `resolve_vended_storage` MUST NOT be added to the pushdown façade; it SHALL be reachable only as `lakehouse_catalog::resolve_vended_storage`
* *AND* `CatalogSession` MUST NOT be re-exported at `crate::adapter::pushdown::CatalogSession`, because external callers name it on the crate that declares it and a second path would be a redundant alias
* *AND* both probe doc comments SHALL state that the probe's own `use` list IS the baseline, and NEITHER SHALL cite `specs/_plans/refactor-adapter-pushdown-modules/public-surface-baseline.txt`, so the surface contract stops depending on a file that cannot be read
* *AND* the `lakehouse-catalog` crate SHALL carry its own external-vantage probe, so the boundary the three departing items move to is guarded the same way the one they left is — `vs-adapter/catalog-crate-structure` owns that probe's contents

### Scenario: Behavior is unchanged across the refactor

* *GIVEN* the pre-refactor unit and integration test suites for the pushdown planning layer
* *WHEN* the suites run against the refactored code
* *THEN* every test MUST pass with no change to any test assertion or expected value, EXCEPT the scan spec's `storage` value wherever an assertion embeds one
* *AND* the scan-driving SQL generated for a given pushdown request MUST be byte-identical to the pre-refactor output EXCEPT for that `storage` value's variant tag, whose tagged payload `vs-adapter/storage-backend-enum` requires to be byte-identical to the untagged encoding

### Scenario: Each pushdown submodule owns its tests

* *GIVEN* the refactored pushdown submodules
* *WHEN* the test suite compiles
* *THEN* each capability submodule MUST contain a `#[cfg(test)] mod tests` covering only that submodule's own items
* *AND* no single central pushdown test module SHALL remain
* *AND* a test helper shared across submodules MUST live in one shared `#[cfg(test)]` support module rather than being duplicated

### Scenario: The pushdown façade drops resolve_table_schema when the shared catalog-client pipeline replaces its only caller

* *GIVEN* the frozen `crate::adapter::pushdown::<name>` baseline asserted by two compile-time probes — `src/adapter/pushdown_surface_probe_tests.rs` naming 22 items from an in-crate vantage and `tests/pushdown_public_surface.rs` naming 12 externally-`pub` items — with `resolve_table_schema` among both lists
* *WHEN* the shared `CatalogClient` listing pipeline (plan `add-native-unity-catalog-client`, issue #318) replaces `resolve_table_schema`'s only production caller and the function is deleted
* *THEN* `resolve_table_schema` SHALL leave the façade and no other item SHALL be added, removed, narrowed, or widened, so the in-crate probe SHALL name 21 items and the external probe SHALL name 11 items, and both MUST compile
* *AND* `resolve_file_list` SHALL KEEP its name and its `pub` visibility on the façade, so the scan path's file-resolution entry point is unaffected by the deletion
* *AND* both probe doc comments SHALL state the reduced count — 21 in-crate, 11 external — because the compiler catches only narrowing, not deletion, so the count is what makes the removal visible in review
* *AND* the façade SHALL stay FROZEN after this redraw: any further change to the item set requires its own spec delta against `vs-adapter/pushdown-module-structure`

### Scenario: The format-reader seam extends the pushdown façade through an explicit reviewed edit

* *GIVEN* the frozen façade baseline asserted by two compile-time probes — the in-crate probe
  `crates/lakehouse-engine/src/adapter/pushdown_surface_probe_tests.rs` naming 21 items and the
  external-vantage probe `crates/lakehouse-engine/tests/pushdown_public_surface.rs` naming 11
* *WHEN* the table-format reader seam lands as a `format` submodule of `adapter::pushdown`
* *THEN* EXACTLY FIVE items SHALL be ADDED to the façade at `pub` visibility and no item SHALL be
  removed, narrowed, or widened: the format-reader trait, the resolved-scan result type the trait
  returns, the scan-source type the selection matches on, the selection function itself, and
  `ConnectionStorage` — a code-review fix (`TOO_MANY_ARGUMENTS`) that collapsed the CONNECTION's
  static storage backend, its resolved credentials, and the resolved `ALLOW_HTTP` consent gate,
  previously threaded as three separate parameters through the selection function and both reader
  constructors, into one struct
* *AND* the in-crate probe SHALL name 26 items and the external probe SHALL name 16, and both MUST
  compile, so any narrowing is a build failure rather than a silent gap
* *AND* both concrete format readers — the Iceberg one and the Delta one — SHALL stay private to the
  `format` submodule and SHALL appear on NEITHER probe, because the selection function returns a boxed
  trait object and a caller that could name a reader type could bypass the selection this seam exists
  to centralize
* *AND* each new submodule of `format` MUST carry its own sibling `_tests.rs` covering only that
  submodule's own items, per this feature's recorded per-submodule test rule
* *AND* `resolve_file_list` SHALL keep its name, its `pub` visibility, its signature, and every one of
  its call sites, so no existing probe entry changes and the shipped Iceberg planning path is
  byte-identical (see `vs-adapter/delta-table-planning`)
* *AND* both probe doc comments SHALL be updated to state their new counts, keeping the recorded rule
  that the probe's own `use` list IS the baseline and no separate baseline file is consulted

### Scenario: Collapsing the Iceberg file resolver removes exactly one item from the pushdown façade

* *GIVEN* the frozen façade baseline asserted by two compile-time probes — the in-crate probe naming 26
  items and the external-vantage probe `tests/pushdown_public_surface.rs` naming 16 — with
  `resolve_file_list` on BOTH lists
* *WHEN* `vs-adapter/pushdown-format-neutral-resolution` routes every production and test caller through
  the format-reader seam and the Iceberg reader absorbs the resolver's body
* *THEN* EXACTLY ONE item SHALL be REMOVED from the façade — `resolve_file_list` — and no item SHALL be
  added, narrowed, or widened, so the in-crate probe SHALL name 25 items and the external probe SHALL
  name 15, and both MUST compile
* *AND* the per-request scan-source resolver this feature's caller introduces MUST NOT be added to the
  façade; it SHALL be reachable only through its own submodule path, because its only consumers are the
  single-table path and the join legs, both descendants of `adapter::pushdown`
* *AND* both probe doc comments SHALL state the reduced counts — 25 in-crate, 15 external — because the
  compiler catches only narrowing, not deletion, so the count is what makes the removal visible in
  review
* *AND* the recorded rule that the probe's own `use` list IS the baseline SHALL hold unchanged, and
  neither probe SHALL cite a separate baseline file
* *AND* any submodule of `adapter::pushdown` this collapse adds MUST carry its own sibling `_tests.rs`
  covering only that submodule's own items, per this feature's recorded per-submodule test rule

### Scenario: The pushdown façade admits exactly one item for the Delta refused-column list

* *GIVEN* the frozen `crate::adapter::pushdown::<name>` baseline asserted by two compile-time probes —
  `src/adapter/pushdown_surface_probe_tests.rs` naming 25 items from an in-crate vantage and
  `tests/pushdown_public_surface.rs` naming the 15 externally-`pub` items
* *WHEN* `ResolvedScan` gains the field that carries the columns the format reader refused
* *THEN* EXACTLY ONE item SHALL be added to the façade — the type naming one refused column and its
  refusal reason — and no other item SHALL be added, removed, narrowed, or widened
* *AND* the in-crate probe SHALL name 26 items and the external probe SHALL name 16, and both probe doc
  comments SHALL state the new counts, so a later removal stays visible in review
* *AND* the added type SHALL be a named struct rather than a `(String, String)` tuple, because an
  external test crate reads `ResolvedScan`'s fields and a positional pair gives its two strings no
  distinguishing name at the read site
* *AND* the Delta reader-protocol gate and the Delta type classifier MUST NOT be added to the façade,
  because both are reached only from inside `adapter::pushdown::format` and a probe entry for either
  would freeze a submodule-internal helper

