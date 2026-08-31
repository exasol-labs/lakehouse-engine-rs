# Feature: Pushdown Planning — ORDER BY + LIMIT (TopN)

Pushes an ordered top-N query (single table, no GROUP BY, no aggregate, no non-zero OFFSET)
down as a decomposed partial/merge top-N: each shard computes its own bounded local top-N
over only its assigned files and emits at most `n` rows, and Exasol merges the per-shard
rows with a final `ORDER BY … LIMIT n`. The merge `ORDER BY … LIMIT n` attaches DIRECTLY
to the outer ungrouped scalar scan select over the nested distributor subquery (or the
from-less single-shard select), with no `SELECT * FROM (...)` wrapper. This replaces the
raw-emit-the-whole-table-then-sort-in-Exasol path for the common "top-N by a sort key"
query, so only `≤ (shard_count × n)` rows cross the UDF boundary instead of every
matching row. Every ordered shape the adapter cannot bound that way — including an
expression or aggregate sort key, and any non-zero OFFSET — takes the declined path, which
renders the retained ordering AND the retained final window itself.

## Background

* **This delta is issue #135 and it changes no rule of this feature.** Every capability, translation, request-shape, and SQL-shape rule here is UNCHANGED. What changes is the credential claim.
* **SUPERSEDES this feature's recorded Background bullet "Credentials MUST NOT appear in any returned SQL string or error message."** That sentence is unscoped and is FALSE for a vended storage credential, both before and after this plan. The scoped replacement: a CONNECTION-supplied storage credential is carried as a connection REFERENCE and does not appear in the returned SQL; a VENDED storage credential still appears there under the tracked exception issue [#378](https://github.com/exasol-labs/lakehouse-engine-rs/issues/378); no credential of either kind appears in an error message.
* **`vs-adapter/scan-spec-credential-reference` owns the reference contract, the resolution, the required grant, and the #378 residual.** This feature CITES it and restates none of it, so the two do not drift.

## Scenarios

<!-- DELTA:NEW -->
### Scenario: An ordered top-N request's generated SQL carries a credential reference, not a credential

* *GIVEN* an ORDER BY + LIMIT pushdown request over a virtual schema whose CONNECTION supplies static storage credentials and does not enable `use_vended_credentials`
* *WHEN* the adapter renders the scan-driving SQL for that request
* *THEN* the returned SQL string MUST NOT contain the CONNECTION's `access_key`, `secret_key`, `session_token`, `account_key`, or `sas_token` value in any encoding, because the shard-invariant common scan-spec argument carries a connection REFERENCE under `vs-adapter/scan-spec-credential-reference`
* *AND* the same request with `use_vended_credentials` enabled SHALL carry the vended credential INLINE in that SQL — the tracked exception issue #378 — so this feature's credential claim is SCOPED to CONNECTION-supplied credentials and MUST NOT be read as unconditional
* *AND* no credential value of either kind SHALL appear in any error message this feature's path raises
<!-- /DELTA:NEW -->
