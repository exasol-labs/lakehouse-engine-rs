# Feature: Pushdown Planning — Aggregate Extensions

Extends pushdown planning (`vs-adapter/pushdown-planning`) with the newly advertised
aggregate-side capabilities: HAVING clause pushdown over the partial/merge decomposition,
and decomposable statistical aggregate pushdown via sufficient statistics. Also records the
boundary of aggregate decomposition — which aggregates fall back to row scanning rather than
being pushed as a partial/merge plan.

## Background

* **This delta is issue #135 and it changes no rule of this feature.** Every capability, translation, request-shape, and SQL-shape rule here is UNCHANGED. What changes is the credential claim.
* **SUPERSEDES this feature's recorded Background bullet "Credentials MUST NOT appear in any returned SQL string or error message."** That sentence is unscoped and was FALSE for a vended storage credential before this plan. The scoped replacement: a CONNECTION-supplied storage credential is carried as a connection REFERENCE and does not appear in the returned SQL; a VENDED storage credential appears there ONLY inside the AES-GCM-sealed envelope of `vs-adapter/scan-spec-credential-reference` — issue [#378](https://github.com/exasol-labs/lakehouse-engine-rs/issues/378), CLOSED by this plan — never in plaintext; no credential of either kind appears in an error message.
* **`vs-adapter/scan-spec-credential-reference` owns the reference contract, the resolution, the sealed vended envelope that closes #378, and the required grant.** This feature CITES it and restates none of it, so the two do not drift.

## Scenarios

<!-- DELTA:NEW -->
### Scenario: An extended-aggregate request's generated SQL carries a credential reference, not a credential

* *GIVEN* a pushdown request using one of this feature's extended aggregate functions, over a virtual schema whose CONNECTION supplies static storage credentials and does not enable `use_vended_credentials`
* *WHEN* the adapter renders the scan-driving SQL for that request
* *THEN* the returned SQL string MUST NOT contain the CONNECTION's `access_key`, `secret_key`, `session_token`, `account_key`, or `sas_token` value in any encoding, because the shard-invariant common scan-spec argument carries a connection REFERENCE under `vs-adapter/scan-spec-credential-reference`
* *AND* the same request with `use_vended_credentials` enabled SHALL carry the vended credential ONLY inside the sealed envelope `vs-adapter/scan-spec-credential-reference` specifies — issue #378, closed by this plan — so no credential value appears in PLAINTEXT in that SQL under either setting
* *AND* no credential value of either kind SHALL appear in any error message this feature's path raises
<!-- /DELTA:NEW -->
