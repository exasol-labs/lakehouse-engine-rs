# Feature: Pushdown Planning — Grouped Aggregate Credential Reference

Extends `vs-adapter/pushdown-planning-grouped-agg` with the credential-exposure
guarantee for the GROUP BY aggregate pushdown path: the generated scan-driving SQL
carries a CONNECTION credential reference, never a plaintext credential, under either
setting of `use_vended_credentials`.

## Background

* A CONNECTION-supplied storage credential is carried as a connection REFERENCE and MUST NOT appear in any returned SQL. A VENDED storage credential appears in a returned SQL string ONLY inside the AES-GCM-sealed envelope of `vs-adapter/scan-spec-credential-reference` — issue [#378](https://github.com/exasol-labs/lakehouse-engine-rs/issues/378), CLOSED by that feature — never in plaintext. No credential of either kind appears in an error message.

## Scenarios

### Scenario: A grouped-aggregate request's generated SQL carries a credential reference, not a credential

* *GIVEN* a GROUP BY aggregate pushdown request over a virtual schema whose CONNECTION supplies static storage credentials and does not enable `use_vended_credentials`
* *WHEN* the adapter renders the scan-driving SQL for that request
* *THEN* the returned SQL string MUST NOT contain the CONNECTION's `access_key`, `secret_key`, `session_token`, `account_key`, or `sas_token` value in any encoding, because the shard-invariant common scan-spec argument carries a connection REFERENCE under `vs-adapter/scan-spec-credential-reference`
* *AND* the same request with `use_vended_credentials` enabled SHALL carry the vended credential ONLY inside the sealed envelope `vs-adapter/scan-spec-credential-reference` specifies — issue #378, closed by this plan — so no credential value appears in PLAINTEXT in that SQL under either setting
* *AND* no credential value of either kind SHALL appear in any error message this feature's path raises
