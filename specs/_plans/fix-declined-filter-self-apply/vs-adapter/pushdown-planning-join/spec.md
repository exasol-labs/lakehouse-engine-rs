# Feature: Pushdown Planning — Join

Pushes a broadcast-eligible two-table inner equi-join into the node-local DataFusion scan by
replicating the smaller side's file list in the shard-invariant common spec, with a fall-through to
the unified unaccelerated fallback for every join outside the broadcast contract.

## Background

* The broadcast contract already requires "a condition/filter/projection the `crates/vs-expression`
  translator can render; any deviation is served by the unified unaccelerated fallback". This delta
  does not change that contract — it makes the FILTER half of it actually enforced. The broadcast
  renderer previously conflated an absent filter with a filter present but unrenderable, so a
  declined filter produced a broadcast plan carrying no filter at all, which no clause then applied.
  See `vs-adapter/pushdown-declined-filter-self-apply`.
* Broadcast SQL has no outer `WHERE`. Its projection is narrowed to the select-list items, so a
  filter-only column is not even in scope for one. Declining to the N-scan fallback — which owns a
  qualified outer `WHERE` — is therefore the only place the predicate can be applied without
  widening the projection, and widening already triggers the recorded projection-widened decline.

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: Broadcast join projection and filter are rendered per involved table

* *GIVEN* a broadcast-eligible inner equi-join `pushdown` request over two involved tables
* *WHEN* the adapter resolves the projection and renders the WHERE filter
* *THEN* the adapter SHALL resolve each projected column's Exasol output type from the involved table it belongs to, matching the column against that table's involved-table column metadata
* *AND* the scan-driving SQL's declared EMITS column list SHALL match the projected join output columns in order and type
* *AND* a WHERE filter over columns of either side SHALL be rendered via the same `crates/vs-expression` translator path used for single-table filters and carried in the common spec
* *AND* a filter that is PRESENT and non-trivial but that the translator DECLINES SHALL cause the adapter to decline the broadcast plan and take the unified unaccelerated fallback, exactly as an unrenderable join condition already does, because the broadcast SQL carries no outer `WHERE` in which the predicate could be applied
* *AND* the adapter SHALL distinguish an ABSENT or trivially-true filter, which leaves the broadcast plan eligible and emits no scan-spec filter, from a DECLINED one, which forfeits the broadcast plan
* *AND* the adapter MUST NOT emit a broadcast plan whose scan spec omits a declined predicate, because Exasol re-applies nothing it delegated and the result would carry extra rows
<!-- /DELTA:CHANGED -->
