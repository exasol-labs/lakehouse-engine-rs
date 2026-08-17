# Feature: Pushdown Module Structure

Decomposes the virtual-schema pushdown-planning code into single-responsibility submodules behind a preserved public façade, keeps behavior byte-identical, and co-locates each submodule's tests.

## Background

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

<!-- DELTA:NEW -->
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
<!-- /DELTA:NEW -->
