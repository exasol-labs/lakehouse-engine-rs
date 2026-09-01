# Feature: Pushdown Planning — ORDER BY Capability

Extends pushdown planning (`vs-adapter/pushdown-planning`) with the getCapabilities-level
advertisement of ordered-sort-key capabilities — `ORDER_BY_COLUMN` (bare column sort keys)
and `ORDER_BY_EXPRESSION` (expression or aggregate sort keys, issue #198) — plus
`LIMIT_WITH_OFFSET` (issue #191), each gated on a correctness-safe rendering path across
every ordered shape the adapter can reach. Per-path rendering mechanics live in the sibling
pushdown-planning features: `vs-adapter/pushdown-planning-topn` (declined row-scan wrapper
and the matched bounded top-N), `vs-adapter/pushdown-planning-grouped-agg` (grouped merge
`ORDER BY`), `vs-adapter/pushdown-planning-grouped-agg-wrapper-fallback` (unresolvable
grouped `ORDER BY`), `vs-adapter/pushdown-planning-join-fallback` (the qualified
single-table and N-scan join wrapper), `vs-adapter/pushdown-planning-single-group-agg` and
`vs-adapter/pushdown-planning-count-distinct` (the one-row merge SELECTs).

## Background

* **This delta is issue #135 and it changes no rule of this feature.** Every capability, translation, request-shape, and SQL-shape rule here is UNCHANGED. What changes is the credential claim.
* **SUPERSEDES this feature's recorded Background bullet "Credentials MUST NOT appear in any returned SQL string or error message."** That sentence is unscoped and was FALSE for a vended storage credential before this plan. The scoped replacement: a CONNECTION-supplied storage credential is carried as a connection REFERENCE and does not appear in the returned SQL; a VENDED storage credential appears there ONLY inside the AES-GCM-sealed envelope of `vs-adapter/scan-spec-credential-reference` — issue [#378](https://github.com/exasol-labs/lakehouse-engine-rs/issues/378), CLOSED by this plan — never in plaintext; no credential of either kind appears in an error message.
* **`vs-adapter/scan-spec-credential-reference` owns the reference contract, the resolution, the sealed vended envelope that closes #378, and the required grant.** This feature CITES it and restates none of it, so the two do not drift.

## Scenarios

<!-- DELTA:NEW -->
### Scenario: An ordered request's generated SQL carries a credential reference, not a credential

* *GIVEN* a pushdown request carrying a pushed ordering under this feature's advertised ORDER BY capabilities, over a virtual schema whose CONNECTION supplies static storage credentials and does not enable `use_vended_credentials`
* *WHEN* the adapter renders the scan-driving SQL for that request
* *THEN* the returned SQL string MUST NOT contain the CONNECTION's `access_key`, `secret_key`, `session_token`, `account_key`, or `sas_token` value in any encoding, because the shard-invariant common scan-spec argument carries a connection REFERENCE under `vs-adapter/scan-spec-credential-reference`
* *AND* the same request with `use_vended_credentials` enabled SHALL carry the vended credential ONLY inside the sealed envelope `vs-adapter/scan-spec-credential-reference` specifies — issue #378, closed by this plan — so no credential value appears in PLAINTEXT in that SQL under either setting
* *AND* no credential value of either kind SHALL appear in any error message this feature's path raises
<!-- /DELTA:NEW -->
