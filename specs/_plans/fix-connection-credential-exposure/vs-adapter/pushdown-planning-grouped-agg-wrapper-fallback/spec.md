# Feature: Pushdown Planning — Grouped Aggregation Wrapper Fallback

Extends `vs-adapter/pushdown-planning-grouped-agg` with what happens when a grouped
request cannot be decomposed into the partial/merge shape at all — an undecomposable
select-list item, a HAVING that references an aggregate absent from the select list, or
an ORDER BY that resolves to neither a group key nor a select-list aggregate. The
adapter falls back to a qualified single-table wrapper that renders the grouped select
list, GROUP BY, HAVING, ORDER BY, and LIMIT as ordinary Exasol SQL over a materialized
sharded raw scan. The fallback is never an error: the wrapper preserves the HAVING
natively, so the adapter keeps the `AGGREGATE_HAVING` contract it advertised (issue
#195), and it renders an otherwise-unresolvable grouped `ORDER BY` natively too (issue
#198). The inner sharded raw scan MUST project only the columns the request references
(group keys, select-list aggregate arguments, filter, and any HAVING/ORDER BY columns),
not the full base-table schema (issue #160); the narrowing is computed by a single
shared referenced-column helper reused by the single-group `COUNT(DISTINCT)` Case 2/3
qualified-wrapper decline (`vs-adapter/pushdown-planning-count-distinct`), so both
decline paths narrow identically.

## Background

* **This delta is issue #135 and it changes no rule of this feature.** Every capability, translation, request-shape, and SQL-shape rule here is UNCHANGED. What changes is the credential claim.
* **SUPERSEDES this feature's recorded Background bullet "Credentials MUST NOT appear in any returned SQL string or error message."** That sentence is unscoped and is FALSE for a vended storage credential, both before and after this plan. The scoped replacement: a CONNECTION-supplied storage credential is carried as a connection REFERENCE and does not appear in the returned SQL; a VENDED storage credential still appears there under the tracked exception issue [#378](https://github.com/exasol-labs/lakehouse-engine-rs/issues/378); no credential of either kind appears in an error message.
* **`vs-adapter/scan-spec-credential-reference` owns the reference contract, the resolution, the required grant, and the #378 residual.** This feature CITES it and restates none of it, so the two do not drift.

## Scenarios

<!-- DELTA:NEW -->
### Scenario: The grouped-aggregate wrapper fallback's generated SQL carries a credential reference, not a credential

* *GIVEN* a GROUP BY request served by the qualified wrapper fallback rather than by partial/merge decomposition, over a virtual schema whose CONNECTION supplies static storage credentials and does not enable `use_vended_credentials`
* *WHEN* the adapter renders the scan-driving SQL for that request
* *THEN* the returned SQL string MUST NOT contain the CONNECTION's `access_key`, `secret_key`, `session_token`, `account_key`, or `sas_token` value in any encoding, because the shard-invariant common scan-spec argument carries a connection REFERENCE under `vs-adapter/scan-spec-credential-reference`
* *AND* the same request with `use_vended_credentials` enabled SHALL carry the vended credential INLINE in that SQL — the tracked exception issue #378 — so this feature's credential claim is SCOPED to CONNECTION-supplied credentials and MUST NOT be read as unconditional
* *AND* no credential value of either kind SHALL appear in any error message this feature's path raises
<!-- /DELTA:NEW -->
