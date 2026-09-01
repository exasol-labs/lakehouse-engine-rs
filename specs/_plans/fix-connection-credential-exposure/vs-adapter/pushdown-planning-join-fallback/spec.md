# Feature: Pushdown Planning — Join Fallback

Extends pushdown planning with the SINGLE unified renderer that serves every inner
equi-join outside the two-table broadcast contract. Each involved table is scanned
independently through its own sharded fan-out subquery — nested `LAKEHOUSE_DISTRIBUTE_FILES`
distributor over an ungrouped `LAKEHOUSE_SCAN` SCALAR EMIT UDF — and all N legs are
reconstructed into the original inner join by Exasol's core engine. The FROM clause is
rendered as a left-to-right `INNER JOIN … ON` chain (not a comma cross-join with one flat
`WHERE`): each join condition attaches to the `ON` of the join point at which every table
it references is in scope, and each leg's leg-local `WHERE` conjuncts are pushed into
that leg's fan-out so DataFusion prunes and filters per leg. Column references are
attributed to a JOIN LEG — one occurrence of a table in the FROM tree — never to a table
name; the leg-attribution binding itself, and its self-join-specific correctness properties,
are owned by the sibling feature `vs-adapter/pushdown-planning-join-fallback-self-join`. The
unaccelerated fallback has exactly one implementation for all N ≥ 2 legs.

## Background

* **This delta is issue #135 and it changes no rule of this feature.** Every capability, translation, request-shape, and SQL-shape rule here is UNCHANGED. What changes is the credential claim.
* **SUPERSEDES this feature's recorded Background bullet "Credentials MUST NOT appear in any returned SQL string or error message."** That sentence is unscoped and was FALSE for a vended storage credential before this plan. The scoped replacement: a CONNECTION-supplied storage credential is carried as a connection REFERENCE and does not appear in the returned SQL; a VENDED storage credential appears there ONLY inside the AES-GCM-sealed envelope of `vs-adapter/scan-spec-credential-reference` — issue [#378](https://github.com/exasol-labs/lakehouse-engine-rs/issues/378), CLOSED by this plan — never in plaintext; no credential of either kind appears in an error message.
* **`vs-adapter/scan-spec-credential-reference` owns the reference contract, the resolution, the sealed vended envelope that closes #378, and the required grant.** This feature CITES it and restates none of it, so the two do not drift.

## Scenarios

<!-- DELTA:NEW -->
### Scenario: The unaccelerated join fallback wrapper's generated SQL carries a credential reference, not a credential

* *GIVEN* a join pushdown request that declines the broadcast path and is served by the unaccelerated multi-scan wrapper, over a virtual schema whose CONNECTION supplies static storage credentials and does not enable `use_vended_credentials`
* *WHEN* the adapter renders the scan-driving SQL for that request
* *THEN* the returned SQL string MUST NOT contain the CONNECTION's `access_key`, `secret_key`, `session_token`, `account_key`, or `sas_token` value in any encoding, because the shard-invariant common scan-spec argument carries a connection REFERENCE under `vs-adapter/scan-spec-credential-reference`
* *AND* the same request with `use_vended_credentials` enabled SHALL carry the vended credential ONLY inside the sealed envelope `vs-adapter/scan-spec-credential-reference` specifies — issue #378, closed by this plan — so no credential value appears in PLAINTEXT in that SQL under either setting
* *AND* no credential value of either kind SHALL appear in any error message this feature's path raises
<!-- /DELTA:NEW -->
