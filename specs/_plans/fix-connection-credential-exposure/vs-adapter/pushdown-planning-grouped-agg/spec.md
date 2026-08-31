# Feature: Pushdown Planning — Grouped Aggregation

Extends `vs-adapter/pushdown-planning` with the GROUP BY aggregate detection and
scan-driving SQL generation scenarios. When Exasol delegates a `GROUP BY` aggregate
query, the adapter detects the shape, renders group-key expressions via the VS
expression translator, builds a grouped common scan spec spliced once as the scalar
scan UDF's first argument, and generates fan-out SQL that runs DataFusion GROUP BY
inside each scalar-scan invocation and merges the partials in an outer wrapper.
Cluster fan-out (`GROUP BY shard_key`) lives inside the nested
`LAKEHOUSE_DISTRIBUTE_FILES` distributor subquery; the outer wrapper re-groups the
scalar scan's emitted partial rows on the user group keys. See
`vs-adapter/pushdown-planning-grouped-agg-scalar-over-aggregate` for
scalar-function-wrapping-aggregates select items on this same path, and
`vs-adapter/pushdown-planning-grouped-agg-wrapper-fallback` for what happens when a
grouped request cannot be decomposed into this partial/merge shape at all.

## Background

* **This delta is issue #135 and it changes no rule of this feature.** Every capability, translation, request-shape, and SQL-shape rule here is UNCHANGED. What changes is the credential claim.
* **SUPERSEDES this feature's recorded Background bullet "Credentials MUST NOT appear in any returned SQL string or error message."** That sentence is unscoped and is FALSE for a vended storage credential, both before and after this plan. The scoped replacement: a CONNECTION-supplied storage credential is carried as a connection REFERENCE and does not appear in the returned SQL; a VENDED storage credential still appears there under the tracked exception issue [#378](https://github.com/exasol-labs/lakehouse-engine-rs/issues/378); no credential of either kind appears in an error message.
* **`vs-adapter/scan-spec-credential-reference` owns the reference contract, the resolution, the required grant, and the #378 residual.** This feature CITES it and restates none of it, so the two do not drift.

## Scenarios

<!-- DELTA:NEW -->
### Scenario: A grouped-aggregate request's generated SQL carries a credential reference, not a credential

* *GIVEN* a GROUP BY aggregate pushdown request over a virtual schema whose CONNECTION supplies static storage credentials and does not enable `use_vended_credentials`
* *WHEN* the adapter renders the scan-driving SQL for that request
* *THEN* the returned SQL string MUST NOT contain the CONNECTION's `access_key`, `secret_key`, `session_token`, `account_key`, or `sas_token` value in any encoding, because the shard-invariant common scan-spec argument carries a connection REFERENCE under `vs-adapter/scan-spec-credential-reference`
* *AND* the same request with `use_vended_credentials` enabled SHALL carry the vended credential INLINE in that SQL — the tracked exception issue #378 — so this feature's credential claim is SCOPED to CONNECTION-supplied credentials and MUST NOT be read as unconditional
* *AND* no credential value of either kind SHALL appear in any error message this feature's path raises
<!-- /DELTA:NEW -->
