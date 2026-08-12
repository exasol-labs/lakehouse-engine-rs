# Feature: Azure E2E Harness (ADLS Gen2 + Lakekeeper)

Verifies `abfss://` reads through the lakehouse VS against a real Azure Data Lake Storage Gen2 account, over both credential paths.

## Background

* **This delta repoints ONE cross-reference and changes no assertion; it is issue #330.** The recorded bullet "**`abfss://` needs no plaintext consent, so the vended Azure arm is NOT the counterpart of the S3 arm's `ALLOW_HTTP` positive case**" closes: "The `abfs://`-requires-consent branch stays unit-covered in `crates/lakehouse-catalog/src/vended.rs`." That branch MOVES: issue #330 gives the `abfs://` consent gate ONE shared home in `crates/lakehouse-catalog/src/storage.rs`, reached by both vended selectors, with its unit coverage in the sibling `storage_tests.rs`.
* **The bullet's substance is unchanged and is not superseded.** `abfss` stays ungated because it names TLS transport; this suite's `ALLOW_HTTP = 'true'` stays inert on this path; and this scenario stays NOT the Azure counterpart of the S3 plaintext case. Only where the counterpart branch is covered changes — and it now covers BOTH catalog kinds rather than the Iceberg selector alone.
* No assertion, fixture, provisioning step, or container-lifecycle rule of this feature changes.

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: End-to-end scan over the vended-credential ADLS warehouse returns correct rows

* *GIVEN* a virtual schema over an Iceberg table seeded into the per-run `<container>-vended` ADLS warehouse, whose CONNECTION supplies OAuth2 catalog authentication, sets `use_vended_credentials` true, and supplies no storage field of any kind
* *WHEN* a user runs `SELECT <subset of columns> FROM <vs>.<table> WHERE <predicate> LIMIT <n>` through that virtual schema
* *THEN* the adapter SHALL send the `X-Iceberg-Access-Delegation: vended-credentials` header on the `loadTable` request, read the SAS Lakekeeper returns under the `adls.sas-token.<host>` key for the table location's own storage host, derive the ADLS account name from that same host rather than from the CONNECTION, and carry both into every per-shard scan spec
* *AND* the scan SHALL read the `abfss://` data files with that SAS and return rows identical to the same query run over the static-credential warehouse in the same container
* *AND* the test SHALL assert that the vended CONNECTION carries no `account_name` key and no `account_key` key at all, and an empty `endpoint`, `region`, `access_key`, and `secret_key`, because that empty shape is the REQUIRED shape rather than merely a delegation proof — with no account name and no account key to fall back to, a passing scan is reachable only through the vended SAS
* *AND* Lakekeeper SHALL report this warehouse's `sas-enabled` as `true` and its `filesystem` as the same container the static warehouse uses, read back through the management API rather than assumed from the request the harness sent
* *AND* the scenario SHALL NOT depend on `ALLOW_HTTP`: `abfss://` names TLS transport and needs no plaintext consent, so the property the shared VS DDL sets on every virtual schema is inert here, and this scenario is NOT the Azure counterpart of the S3 arm's plaintext-consent case; the `abfs://` counterpart is governed by ONE shared consent gate covering BOTH vended selectors, unit-covered in the shared vended home (`crates/lakehouse-catalog/src/storage.rs`) rather than in the Iceberg selector alone
* *AND* no vended SAS value and no account-key value SHALL appear in any returned SQL string or test output, and the suite MUST NOT itself request `loadTable` to inspect the vended payload, because that would place a live SAS in the test process's memory and output for diagnostic value only
* *AND* because both credential arms share one fixture and one test function, every assertion specific to the vended arm except the cross-arm row comparison SHALL run BEFORE the static arm's assertions, so a static-arm QUERY or ASSERTION regression cannot mask the vended proof this scenario adds; a static-arm PROVISIONING failure aborts the shared fixture before any assertion runs and does mask it, which is the residual cost of one fixture, reduced but not removed by provisioning the vended arm's warehouse, seed, and virtual schema BEFORE the static arm's
* *AND* the test MUST fail (not skip) when the local stack or the Azure account is unavailable
<!-- /DELTA:CHANGED -->
