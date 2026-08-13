# Feature: Lakekeeper E2E Harness

Provisions a local Lakekeeper + MinIO stack with a static-credential and an STS-vending warehouse, and drives the engine end to end against both.

## Background

* **This delta restates ONE clause's reason and is issue #330.** `vs-adapter/pushdown-planning-cloud-credentials` now resolves the vended store address from the CONNECTION when the CONNECTION states one and from the `loadTable` response otherwise, so "the vended endpoint reaches the store" is no longer true unconditionally — it is true HERE because this suite's vended CONNECTION carries an empty `endpoint` and an empty `region`.
* **SUPERSEDES the strict-address-rule justification.** The recorded bullet read: "Lakekeeper's live vended config supplies the store address, live-verified. It carries `s3.endpoint` (`http://minio:9000/`) and `s3.path-style-access` (`true`), so this path satisfies the strict rule's 'a vended payload must name a region or an endpoint' requirement through the endpoint and needs no vended `client.region`." The live-verified FACTS stand. The requirement they satisfied no longer exists: an empty vended address is now legal. What the fixture now demonstrates is the OTHER half of the precedence rule — vended addressing filling in while the CONNECTION is silent.
* **The suite's existing empty-CONNECTION assertion becomes the guard that keeps this scenario meaningful**, so it is promoted from a delegation proof to the precondition of the precedence case under test. Without it, a CONNECTION `endpoint` would win and the scan would prove nothing about the vended one.
* This suite is a declared characterization gate for the change: it is the only in-repo suite that reads a real vended payload end to end, and it MUST pass unedited except for the assertion promoted above.

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
* *AND* no vended or static credential value SHALL appear in any returned SQL string or test output
* *AND* the test MUST fail (not skip) when the Docker stack is unavailable
<!-- /DELTA:CHANGED -->
