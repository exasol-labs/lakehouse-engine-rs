# Feature: Pushdown Joins Module Structure

Splits join pushdown into concern-owned submodules behind an unchanged façade, with a golden-SQL baseline pinning every join code path's built string across the split.

## Background

<!-- DELTA:NEW -->
* This delta carves the scan spec's `storage` value out of ONE clause — the byte-for-byte golden-baseline clause of the "Generated join SQL is byte-identical across the split" scenario. It amends no other clause, supersedes no Background bullet, and changes no module boundary, visibility rule, or render path.
* `vs-adapter/storage-backend-enum` (issue #274) wraps the scan spec's `storage` value in an externally-tagged backend variant. Three of the four golden strings embed a scan spec and therefore change: the broadcast join, the N-scan fallback, and the grouped-qualified fallback. The fourth — `ineligible_join_decline`'s `UdfError` message — embeds no scan spec and is unedited, so the decline path keeps an untouched full-string gate.
* The carve-out permits an edit to the `storage` value ALONE. Every other byte of each golden string stays as captured, which is what keeps this scenario's full-string equality assertion a working proof rather than a retired one.
<!-- /DELTA:NEW -->

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: Generated join SQL is byte-identical across the split

* *GIVEN* a pre-refactor golden-SQL baseline capturing the exact built string from each join code path this refactor's duplication reductions can touch, before any code moves: a broadcast join (`build_broadcast_join_sql`), an N-scan fallback (`build_n_scan_join_sql`, with a fixture that includes both a side-local WHERE conjunct and a cross-side residual conjunct so the `side_local_filter`/`cross_side_residual_filter` shared shape is exercised), a grouped-qualified fallback (`build_grouped_qualified_fallback_sql`), and an ineligible decline (`ineligible_join_decline`'s `UdfError` message)
* *WHEN* the split and any duplication extraction complete and each code path is re-planned against the refactored code
* *THEN* a full-string equality assertion over each code path's built SQL — or the decline `UdfError` message — MUST equal its captured golden baseline byte-for-byte EXCEPT for the scan spec's `storage` value, which `vs-adapter/storage-backend-enum` re-encodes as an externally-tagged backend variant over a byte-identical payload, verified by characterization tests that assert the entire returned string rather than a substring
* *AND* the permitted `storage` edit SHALL be the ONLY edit to any of the four golden strings, and the `ineligible_join_decline` message MUST stay unedited because it embeds no scan spec, so the decline path retains a fully untouched full-string gate
* *AND* every scenario in `vs-adapter/pushdown-planning-join` and `vs-adapter/pushdown-planning-join-fallback` MUST still pass with no change to any test assertion or expected value outside a `storage` value, including `plan_join`'s empty-side short-circuit (which delegates to `pushdown::file_resolution::empty_result_sql`, a function this refactor does not move or touch, so it is verified by the existing suite rather than a new golden test here)
<!-- /DELTA:CHANGED -->
