# Feature: Pushdown Planning — Empty Result

When plan-time file pruning (driven by the pushed-down filter) eliminates
every data file for a query, the adapter still returns a `pushdown` response
whose output column shape matches what the same query would have produced with
matching data. The short-circuit is shape-aware: it emits the correct empty
instance of whichever plan the non-empty path would have committed to — a
row-scan projection, a single-group aggregate row, or a grouped-aggregate
result — so Exasol's positional pushdown validation always accepts the response
instead of rejecting it for a column-count/type mismatch.

## Background

* **This delta is issue #135 and it changes no rule of this feature.** Every capability, translation, request-shape, and SQL-shape rule here is UNCHANGED. What changes is the credential claim.
* **This feature's recorded Background bullet "Credentials MUST NOT appear in any returned SQL string or error message." stays TRUE on this path.** An empty-result plan emits no scan-spec storage value of any kind — neither a credential, nor a connection reference, nor the sealed envelope of issue [#378](https://github.com/exasol-labs/lakehouse-engine-rs/issues/378) — so the recorded claim holds here trivially; the scoped claim for storage-carrying paths lives in `vs-adapter/scan-spec-credential-reference`, which this feature CITES.
* **`vs-adapter/scan-spec-credential-reference` owns the reference contract, the resolution, the sealed vended envelope that closes #378, and the required grant.** This feature CITES it and restates none of it, so the two do not drift.

## Scenarios

<!-- DELTA:NEW -->
### Scenario: An empty-result plan emits no storage block at all

* *GIVEN* a pushdown request this feature short-circuits to an empty result, over a virtual schema whose CONNECTION supplies static storage credentials
* *WHEN* the adapter renders the empty-result SQL
* *THEN* the returned SQL string SHALL carry NO scan-spec storage value of any kind, so it contains neither a credential nor a connection reference, and this feature's credential claim holds unconditionally on its own path
* *AND* the six committed `empty_*` golden pushdown-SQL fixtures SHALL therefore stay BYTE-IDENTICAL across this change and SHALL be asserted unchanged, unlike the eighteen credential-bearing fixtures which are regenerated
* *AND* no credential value SHALL appear in any error message this feature's path raises
<!-- /DELTA:NEW -->
