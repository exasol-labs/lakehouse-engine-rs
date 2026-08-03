# Feature: Lakekeeper E2E Harness (OIDC + MinIO)

End-to-end test suite that verifies the lakehouse VS query path against a
Lakekeeper Iceberg REST catalog — the open-source, OpenID-secured, multi-warehouse
Rust catalog — backed by MinIO object storage, proving real interoperability rather
than a connectivity smoke test. The suite authenticates to the catalog with the
engine's existing OAuth2 client-credentials CONNECTION fields (an external Keycloak
IdP issues the token), resolves tables through Lakekeeper's per-warehouse
`overrides.prefix`, and reads data files under both static S3 credentials and
Lakekeeper's default vended (STS) credentials.

## Background

<!-- DELTA:NEW -->
* **This delta promotes an existing assertion from a stronger-than-necessary proof to the required shape, and changes no fixture, no warehouse, and no query.** It implements issue #276, slice D of six (A-F). `vs-adapter/pushdown-planning-cloud-credentials` now derives the effective scan storage SOLELY from the `loadTable` response when `use_vended_credentials` is true.
* **This suite is the characterization gate that makes the strict rule safe, and it needs no behavioural change to be one.** The vended CONNECTION already supplies an empty `endpoint`, `region`, `access_key`, and `secret_key` and a false `path_style`, so there was never a static value for the shipped preservation rule to backfill and the strict rule is a NO-OP for this path. That is the evidence the rule is compatible with a live vended stack rather than only with unit fixtures.
* **Lakekeeper's live vended config supplies the store address, live-verified.** It carries `s3.endpoint` (`http://minio:9000/`) and `s3.path-style-access` (`true`), so this path satisfies the strict rule's "a vended payload must name a region or an endpoint" requirement through the endpoint and needs no vended `client.region`.
* **`ALLOW_HTTP` stays the operator's consent gate for the vended plain-HTTP endpoint, and this suite already sets it.** The harness emits `ALLOW_HTTP = 'true'` (`crates/lakehouse-engine/tests/common/e2e_harness.rs:270`) and Lakekeeper vends a plain-`http://` MinIO endpoint, so the vended endpoint is honoured and the scan reaches MinIO. Deriving the permission from the vended endpoint's scheme instead was rejected as a security regression: it would let a catalog downgrade the transport with no operator control (see `vs-adapter/pushdown-planning-cloud-credentials`). This suite is consequently the positive case for the consent gate — vended plain-HTTP endpoint plus `ALLOW_HTTP = 'true'` reads successfully.
<!-- /DELTA:NEW -->

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: End-to-end scan over a vended-credential Lakekeeper warehouse returns correct rows

* *GIVEN* a virtual schema over a seeded Iceberg table in the `sts-enabled` Lakekeeper warehouse, whose CONNECTION supplies OAuth2 catalog auth, sets `use_vended_credentials` true, and supplies NO static S3 storage field
* *WHEN* a user runs a projection + filter query through the virtual schema
* *THEN* the adapter SHALL send the `X-Iceberg-Access-Delegation: vended-credentials` header on the `loadTable` request, extract the short-lived vended S3 credentials Lakekeeper returns, and carry them into every per-shard scan spec
* *AND* the adapter SHALL take the store's endpoint and path-style flag from that same vended response, so the scan reaches MinIO without reading any CONNECTION storage field
* *AND* the adapter SHALL honour that vended plain-`http://` endpoint because the harness sets `ALLOW_HTTP = 'true'`, so this scenario is the positive case for the operator-consent gate on plaintext transport
* *AND* the scan SHALL read the MinIO data files using the vended credentials and return rows identical to the same query run over the static-credential warehouse
* *AND* the test SHALL assert the vended CONNECTION carries an empty `access_key`, `secret_key`, and `endpoint`, because under the strict vended rule that empty shape is the REQUIRED shape rather than merely a delegation proof — a passing scan from it is only reachable through the vended response
* *AND* no vended or static credential value SHALL appear in any returned SQL string or test output
* *AND* the test MUST fail (not skip) when the Docker stack is unavailable
<!-- /DELTA:CHANGED -->
