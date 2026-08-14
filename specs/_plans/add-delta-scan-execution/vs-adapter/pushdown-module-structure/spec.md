# Feature: Pushdown Module Structure

Decomposes the pushdown planning code into single-responsibility submodules behind a frozen public
façade whose item set is asserted by two compile-time probes, and co-locates each submodule's tests.

## Background

* **This delta is issue #320.** The façade is frozen, so the collapse of the Iceberg file resolver
  requires an explicit reviewed edit to both probes and their stated counts.
* No item is added, narrowed, or widened. Exactly one item is removed.

## Scenarios

<!-- DELTA:NEW -->
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
<!-- /DELTA:NEW -->
