# Feature: Pushdown Module Structure

Decomposes the virtual-schema pushdown-planning code into single-responsibility submodules behind a frozen public façade. Each submodule owns its tests. `pushdown/pushdown-module-dedup-consolidation` tracks the shared helpers extracted from the duplicated code.

## Background

* `crate::adapter::pushdown` is a directory module (`pushdown/mod.rs` plus sibling files). The import path `crate::adapter::pushdown::<name>` stays the same for every consumer. The set of capability submodules is a design decision, not a contract.
* Two compile-time probes freeze the façade: `crates/lakehouse-engine/src/adapter/pushdown_surface_probe_tests.rs` from an in-crate vantage, and `crates/lakehouse-engine/tests/pushdown_public_surface.rs` from an external vantage that names the externally `pub` items. Each probe's `use` list is the baseline, and its doc comment states the item count. The build fails on any unplanned narrowing. A change to the item set or the counts needs a spec delta against this feature.
* A signature change on a façade item is not a surface change.
* The `lakehouse-catalog` crate carries its own external-vantage probe: see `catalog/catalog-crate-structure`.
* The table-format reader seam is the `format` submodule of `adapter::pushdown`. The Iceberg and Delta readers stay private to it, and the selection function returns a boxed trait object.
* The refused-column list travels on `ResolvedScan`, not on `ScanSpec`, which is the wire format.

## Scenarios

### Scenario: The public pushdown façade matches the item sets of both surface probes

* *GIVEN* a `name → visibility` snapshot of every symbol reachable via `crate::adapter::pushdown::<name>`
* *WHEN* the same extraction re-runs against the `pushdown/mod.rs` façade and all in-repo consumers compile
* *THEN* the re-extracted `name → visibility` set MUST diff empty against the current baseline, the 27-item in-crate probe `use` list and the 17-item external probe `use` list, so no reachable item is added, removed, narrowed, or widened outside a scenario that plans it
* *AND* every path `crate::adapter::pushdown::<name>` MUST resolve to the same item at the same external visibility (`pub` or `pub(crate)`) for the `adapter`, `scan`, and `capabilities` consumers
* *AND* a `#[cfg(test)]` reachability probe naming every remaining `pub` and `pub(crate)` item from outside the `pushdown` module MUST compile, so an effective narrowing masked by a re-export is a compile error

### Scenario: The pushdown façade carries no catalog access items and no re-export of the catalog crate's types

* *GIVEN* the façade baseline asserted by two compile-time probes: `src/adapter/pushdown_surface_probe_tests.rs` naming 27 items from an in-crate vantage and `tests/pushdown_public_surface.rs` naming the 17 externally-`pub` items
* *WHEN* the catalog access layer lives in the `lakehouse-catalog` crate
* *THEN* `extract_vended_keys`, `merge_vended_into_storage`, and `list_namespace_tables` SHALL NOT be on the façade, because the vended mechanism is `lakehouse_catalog::resolve_vended_storage` and the namespace listing is internal to `lakehouse-catalog`
* *AND* `resolve_vended_storage` and `CatalogSession` MUST NOT be re-exported at `crate::adapter::pushdown::`, because external callers name them on the crate that declares them and a second path would be a redundant alias
* *AND* both probe doc comments SHALL state that the probe's own `use` list IS the baseline and the item count, and the `lakehouse-catalog` crate SHALL carry its own external-vantage probe (`catalog/catalog-crate-structure` owns its contents)

### Scenario: Each pushdown submodule owns its tests

* *GIVEN* the refactored pushdown submodules
* *WHEN* the test suite compiles
* *THEN* each capability submodule MUST contain a `#[cfg(test)] mod tests` covering only that submodule's own items
* *AND* no single central pushdown test module SHALL remain
* *AND* a test helper shared across submodules MUST live in one shared `#[cfg(test)]` support module rather than being duplicated

### Scenario: The pushdown façade carries no resolve_table_schema because the shared catalog-client pipeline declares columns

* *GIVEN* the façade baseline asserted by two compile-time probes: `src/adapter/pushdown_surface_probe_tests.rs` naming 27 items from an in-crate vantage and `tests/pushdown_public_surface.rs` naming 17 externally-`pub` items
* *WHEN* the shared `CatalogClient` listing pipeline declares each table's columns at `createVirtualSchema`
* *THEN* `resolve_table_schema` SHALL NOT be on the façade, and neither probe SHALL name it
* *AND* both probe doc comments SHALL state the item count, because the compiler catches only narrowing, not deletion, so the count is what makes a removal visible in review
* *AND* the façade SHALL stay FROZEN: any change to the item set requires its own spec delta against `pushdown/pushdown-module-structure`

### Scenario: The format-reader seam extends the pushdown façade through an explicit reviewed edit

* *GIVEN* the façade baseline asserted by two compile-time probes: the in-crate probe `crates/lakehouse-engine/src/adapter/pushdown_surface_probe_tests.rs` naming 27 items and the external-vantage probe `crates/lakehouse-engine/tests/pushdown_public_surface.rs` naming 17
* *WHEN* the table-format reader seam lives in the `format` submodule of `adapter::pushdown`
* *THEN* the façade SHALL carry, at `pub` visibility, the format-reader trait, the resolved-scan result type the trait returns, the scan-source type the selection matches on, the selection function itself, and `ConnectionStorage`, the struct that bundles the CONNECTION's static storage backend, its resolved credentials, and the resolved `ALLOW_HTTP` consent gate
* *AND* both concrete format readers, the Iceberg one and the Delta one, SHALL stay private to the `format` submodule and SHALL appear on NEITHER probe, because the selection function returns a boxed trait object and a caller that could name a reader type could bypass the selection this seam exists to centralize
* *AND* each submodule of `format` MUST carry its own sibling `_tests.rs` covering only that submodule's own items, per this feature's per-submodule test rule

### Scenario: The pushdown façade carries no separate Iceberg file-resolution function

* *GIVEN* the façade baseline asserted by two compile-time probes: the in-crate probe naming 27 items and the external-vantage probe `tests/pushdown_public_surface.rs` naming 17
* *WHEN* `file-planning/pushdown-format-neutral-resolution` routes every production and test caller through the format-reader seam and the Iceberg reader holds the resolver's body
* *THEN* the façade SHALL NOT carry a separate Iceberg file-resolution function, and the per-request scan-source resolver MUST NOT be added to it; it SHALL be reachable only through its own submodule path, because its only consumers are the single-table path and the join legs, both descendants of `adapter::pushdown`
* *AND* both probe doc comments SHALL state their counts, 27 in-crate and 17 external, and the probe's own `use` list IS the baseline, so neither probe cites a separate baseline file
* *AND* any submodule of `adapter::pushdown` MUST carry its own sibling `_tests.rs` covering only that submodule's own items, per this feature's per-submodule test rule

### Scenario: The pushdown façade admits exactly one item for the Delta refused-column list

* *GIVEN* the façade baseline asserted by two compile-time probes: `src/adapter/pushdown_surface_probe_tests.rs` from an in-crate vantage and `tests/pushdown_public_surface.rs` naming the externally-`pub` items
* *WHEN* `ResolvedScan` carries the columns the format reader refused
* *THEN* the façade SHALL carry the type naming one refused column and its refusal reason, as a named struct rather than a `(String, String)` tuple, because an external test crate reads `ResolvedScan`'s fields and a positional pair gives its two strings no distinguishing name at the read site
* *AND* the in-crate probe SHALL name 27 items and the external probe 17, and both probe doc comments SHALL state the counts
* *AND* the Delta reader-protocol gate and the Delta type classifier MUST NOT be added to the façade, because both are reached only from inside `adapter::pushdown::format` and a probe entry for either would freeze a submodule-internal helper

### Scenario: The pushdown façade admits exactly one item for the aggregate merge inputs

* *GIVEN* the façade baseline asserted by two compile-time probes: `src/adapter/pushdown_surface_probe_tests.rs` naming 27 items from an in-crate vantage and `tests/pushdown_public_surface.rs` naming the 17 externally-`pub` items
* *WHEN* `build_scan_driving_sql` takes `aggregate_types`, `merge_select`, and `request_limit` as a single optional value
* *THEN* the façade SHALL carry the type holding those aggregate merge inputs, and `build_scan_driving_sql` SHALL keep its `pub` visibility on the façade
* *AND* both probe doc comments SHALL state the counts, 27 and 17
* *AND* the type's fields SHALL be private, constructed only through a fallible constructor that refuses an empty merge SELECT, so the malformed `SELECT  FROM (...)` an empty one would render is unrepresentable rather than documented

### Scenario: The pushdown façade carries the column declaration entry point and no Iceberg logical-schema builder

* *GIVEN* the façade baseline asserted by two compile-time probes: `src/adapter/pushdown_surface_probe_tests.rs` naming 27 items from an in-crate vantage and `tests/pushdown_public_surface.rs` naming the 17 externally-`pub` items
* *WHEN* each catalog-declared column is declared at `createVirtualSchema`, per `vs-adapter/column-source-notes`
* *THEN* the façade SHALL carry, at crate visibility, the function that declares a listed table's columns from their neutral source types, and SHALL NOT carry an Iceberg logical-schema builder
* *AND* the per-format declaration helpers (the Iceberg field builder, the Spark-type classifier, and the Delta schema classifier) MUST NOT be added to the façade, because the declaration function is their only caller outside `adapter::pushdown::format`
