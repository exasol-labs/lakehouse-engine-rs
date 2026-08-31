# Feature: Lakekeeper E2E Harness (OIDC + MinIO)

End-to-end test suite that verifies the lakehouse VS query path against a
Lakekeeper Iceberg REST catalog — the open-source, OpenID-secured, multi-warehouse
Rust catalog — backed by MinIO object storage, proving real interoperability rather
than a connectivity smoke test. The suite authenticates to the catalog with the
engine's existing OAuth2 client-credentials CONNECTION fields (an external Keycloak
IdP issues the token), resolves tables through Lakekeeper's per-warehouse
`overrides.prefix`, and reads data files under both static S3 credentials and
Lakekeeper's default vended (STS) credentials. It is additive: the existing
unauthenticated `exasol-e2e` baseline is unchanged, and this suite runs behind its
own `lakekeeper-e2e` cargo feature.

## Background

* **This delta is issue #135. It amends TWO scenarios and adds no fixture, no warehouse, and no version gate.** The Lakekeeper bootstrap, the OAuth2 catalog auth, the static and vended warehouses, the broadcast-join assertions, and the scope-divergence observation are all UNCHANGED.
* **The vended arm's "no credential value in any returned SQL" clause is SUPERSEDED because it was never true and is not made true by this change.** A vended credential travels INLINE in the scan-spec storage block, tracked as issue [#378](https://github.com/exasol-labs/lakehouse-engine-rs/issues/378). What the suite can assert is that no credential appears in TEST OUTPUT, and that no STATIC CONNECTION credential appears in the returned SQL — which for these two vended-warehouse scenarios is vacuous, because the CONNECTION supplies no static storage field at all. The clause is therefore rewritten to assert what the fixture can actually falsify rather than left asserting the opposite of the shipped behaviour.
* **The suite also acquires the scan-script grant from the shared harness definition**, issued AFTER the `CREATE OR REPLACE CONNECTION` that provisions the connection, because a connection replacement drops the grant — verified live on Exasol 2025.2.1. `e2e-harness/e2e-harness` owns that shared-definition requirement and this feature inherits it.

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: End-to-end scan over a vended-credential Lakekeeper warehouse returns correct rows

* *GIVEN* a virtual schema over a seeded Iceberg table in the `sts-enabled` Lakekeeper warehouse, whose CONNECTION supplies OAuth2 catalog auth, sets `use_vended_credentials` true, and supplies NO static S3 storage field
* *WHEN* a user runs a projection + filter query through the virtual schema
* *THEN* the adapter SHALL send the `X-Iceberg-Access-Delegation: vended-credentials` header on the `loadTable` request, extract the short-lived vended S3 credentials Lakekeeper returns, and carry them into every per-shard scan spec
* *AND* the adapter SHALL take the store's endpoint and path-style flag from that same vended response BECAUSE the CONNECTION states no `endpoint` and no `region`, so this scenario is the positive case for vended addressing filling in while the CONNECTION is silent — SUPERSEDING the recorded clause that framed it as the adapter reading no CONNECTION storage field at all
* *AND* the test SHALL assert the vended CONNECTION carries an empty `access_key`, `secret_key`, `endpoint`, and `region`, because that empty shape is now the PRECONDITION of the precedence case under test as well as the delegation proof: a non-empty CONNECTION `endpoint` would win over the vended one and the scan would evidence nothing about vended addressing
* *AND* the adapter SHALL honour that vended plain-`http://` endpoint because the harness sets `ALLOW_HTTP = 'true'`, so this scenario is the positive case for the operator-consent gate on plaintext transport
* *AND* the scan SHALL read the MinIO data files using the vended credentials and return rows identical to the same query run over the static-credential warehouse
* *AND* no vended or static credential value SHALL appear in any test output, and no STATIC CONNECTION credential value SHALL appear in any returned SQL string, while the VENDED credential DOES appear there under the tracked exception issue [#378](https://github.com/exasol-labs/lakehouse-engine-rs/issues/378) — SUPERSEDING the recorded clause that forbade both, which was FALSE for the returned SQL
* *AND* the test MUST fail (not skip) when the Docker stack is unavailable
<!-- /DELTA:CHANGED -->

<!-- DELTA:CHANGED -->
### Scenario: A two-table broadcast join over a vended-credential warehouse returns correct rows

* *GIVEN* the `sts-enabled` Lakekeeper warehouse seeded with BOTH star-schema tables through the OIDC-secured catalog, and one virtual schema over that warehouse's namespace whose CONNECTION supplies OAuth2 catalog auth, sets `use_vended_credentials` true, and supplies NO static S3 storage field
* *AND* a harness connection that declares no row cap, which is the harness default and therefore requires no opt-out call at the call site
* *WHEN* a user runs an inner equi-join of the two tables through that one virtual schema
* *THEN* the adapter SHALL plan a broadcast fan-out, so the pushed SQL carries the compact scan-spec join block and NOT the two-scan `LHS_T0` / `LHS_T1` unaccelerated wrapper
* *AND* that broadcast fan-out SHALL hold when the joined rows are fetched, not only when the plan is inspected through `EXPLAIN VIRTUAL`, because row-fetch-time verification is the only check that confirms the broadcast plan was actually executed rather than merely selected
* *AND* the adapter SHALL resolve a vended credential for EACH table independently, sending `X-Iceberg-Access-Delegation: vended-credentials` on each side's `loadTable` request
* *AND* the emitted scan spec SHALL carry the fact side's vended backend as its whole-spec `storage` value and the dimension side's vended backend inside the join block, so neither side's credential is discarded
* *AND* the joined rows SHALL equal the join computed independently from the two tables read un-joined through the same virtual schema, because a one-warehouse fixture has no second warehouse to cross-check against
* *AND* no vended credential value SHALL appear in any test output, while BOTH sides' vended credentials DO appear in the returned SQL string surfaced by the test under the tracked exception issue [#378](https://github.com/exasol-labs/lakehouse-engine-rs/issues/378) — SUPERSEDING the recorded clause that forbade both, which was FALSE for the returned SQL
* *AND* the test MUST fail (not skip) when the Docker stack is unavailable
<!-- /DELTA:CHANGED -->
