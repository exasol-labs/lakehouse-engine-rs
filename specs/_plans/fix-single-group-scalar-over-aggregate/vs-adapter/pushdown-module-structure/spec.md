# Feature: Pushdown Module Structure

Decomposes the virtual-schema pushdown-planning code into single-responsibility submodules behind a preserved public façade, keeps behavior byte-identical, and co-locates each submodule's tests.

## Background

<!-- DELTA:NEW -->
* **This delta adds ONE façade item.** `build_scan_driving_sql`'s three aggregate-only
  parameters — the per-plan `EMITS` type list, the caller-assembled merge SELECT, and the raw
  request limit — collapse into one value whose absence IS the row-scan path, replacing a
  prose-only "row scans read neither: pass `&[]`" contract. `build_scan_driving_sql` is
  already on both probes and already externally `pub`, and an external test crate calls it on
  the aggregate path, so the new parameter's type must be nameable at the same visibility.
  Both probe doc comments state that changing the set or the count requires a spec delta
  against this feature; this scenario is that delta.
<!-- /DELTA:NEW -->

## Scenarios

<!-- DELTA:NEW -->
### Scenario: The pushdown façade admits exactly one item for the aggregate merge inputs

* *GIVEN* the frozen `crate::adapter::pushdown::<name>` baseline asserted by two compile-time probes —
  `src/adapter/pushdown_surface_probe_tests.rs` naming 26 items from an in-crate vantage and
  `tests/pushdown_public_surface.rs` naming the 16 externally-`pub` items
* *WHEN* `build_scan_driving_sql`'s `aggregate_types`, `merge_select`, and `request_limit` parameters
  collapse into a single optional value carrying all three
* *THEN* EXACTLY ONE item SHALL be added to the façade — the type carrying those aggregate merge
  inputs — and no other item SHALL be added, removed, narrowed, or widened
* *AND* the in-crate probe SHALL name 27 items and the external probe SHALL name 17, and both probe doc
  comments SHALL state the new counts, so a later removal stays visible in review
* *AND* `build_scan_driving_sql` SHALL KEEP its name and its `pub` visibility on the façade, because a
  signature change is not a surface change
* *AND* the added type's fields SHALL be private, constructed only through a fallible constructor that
  refuses an empty merge SELECT, so the malformed `SELECT  FROM (...)` an empty one would render is
  unrepresentable rather than documented
<!-- /DELTA:NEW -->
