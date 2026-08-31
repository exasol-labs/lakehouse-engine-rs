# Feature: Pushdown Planning — Capability Extensions

Extends pushdown planning (`vs-adapter/pushdown-planning`) with the getCapabilities-level
capability advertisements for scalar and type-conversion functions the adapter has added
since the base feature: arithmetic operator scalar functions, CAST/unary-negation, and ISO
week — plus the capabilities that were considered and deliberately kept absent (regexp
scalar functions, bitwise operator functions). Each advertised capability is gated on a
`crates/vs-expression` translator arm that renders it faithfully; each absent capability
records why no faithful translation exists. Ordered-sort-key capability advertisement
(`ORDER_BY_COLUMN` / `ORDER_BY_EXPRESSION`) lives in its own sibling feature,
`vs-adapter/pushdown-planning-order-by-capability`. Related capability-driven extensions —
scalar select-list expression pushdown, HAVING pushdown, statistical aggregates, and literal
projection — live in their own sibling features too (see the "See also" note at the end of
the Background).

## Background

* **This delta is issue #135 and it changes no rule of this feature.** Every capability, translation, request-shape, and SQL-shape rule here is UNCHANGED. What changes is the credential claim.
* **SUPERSEDES this feature's recorded Background bullet "Credentials MUST NOT appear in any returned SQL string or error message."** That sentence is unscoped and is FALSE for a vended storage credential, both before and after this plan. The scoped replacement: a CONNECTION-supplied storage credential is carried as a connection REFERENCE and does not appear in the returned SQL; a VENDED storage credential still appears there under the tracked exception issue [#378](https://github.com/exasol-labs/lakehouse-engine-rs/issues/378); no credential of either kind appears in an error message.
* **`vs-adapter/scan-spec-credential-reference` owns the reference contract, the resolution, the required grant, and the #378 residual.** This feature CITES it and restates none of it, so the two do not drift.

## Scenarios

<!-- DELTA:NEW -->
### Scenario: A request using an extended capability carries a credential reference, not a credential

* *GIVEN* a pushdown request exercising one of this feature's extended advertised capabilities, over a virtual schema whose CONNECTION supplies static storage credentials and does not enable `use_vended_credentials`
* *WHEN* the adapter renders the scan-driving SQL for that request
* *THEN* the returned SQL string MUST NOT contain the CONNECTION's `access_key`, `secret_key`, `session_token`, `account_key`, or `sas_token` value in any encoding, because the shard-invariant common scan-spec argument carries a connection REFERENCE under `vs-adapter/scan-spec-credential-reference`
* *AND* the same request with `use_vended_credentials` enabled SHALL carry the vended credential INLINE in that SQL — the tracked exception issue #378 — so this feature's credential claim is SCOPED to CONNECTION-supplied credentials and MUST NOT be read as unconditional
* *AND* no credential value of either kind SHALL appear in any error message this feature's path raises
<!-- /DELTA:NEW -->
