# Feature: Pushdown Module Structure

Decomposes the virtual-schema pushdown-planning code into single-responsibility submodules behind a preserved public façade, keeps behavior byte-identical, and co-locates each submodule's tests.

## Background

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
* **No item is removed, narrowed, or widened.** Only additions. The recorded byte-identity clauses on
  generated scan-driving SQL hold unedited, because no existing code path changes.
* The two concrete format readers stay module-private and appear on NEITHER probe: the selection
  function returns a boxed trait object, so no caller — in-crate or external — names a reader type.

## Scenarios

<!-- DELTA:NEW -->
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
<!-- /DELTA:NEW -->
