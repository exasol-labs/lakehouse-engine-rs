# Feature: Azure E2E Harness (ADLS Gen2 + Lakekeeper)

Verifies `abfss://` reads through the lakehouse VS against a real Azure Data Lake
Storage Gen2 account, over both credential paths: the static path (Lakekeeper
delegation off, account key carried in the Exasol CONNECTION) and the vended path
(Lakekeeper delegation on, a SAS minted per `loadTable` and carried by no
CONNECTION field). See `azure-e2e/azure-e2e-harness-operations` for the suite's
operational-contract scenarios — credential-variable and stack-availability
failure modes, container naming, the Make target, the gitignored credential file,
shared-harness provisioning, and DDL-failure output redaction.

## Background

* **This delta is issue #135. It amends ONE scenario and changes no fixture, container lifecycle, or warehouse provisioning rule.** The per-run container, the two credential-mode warehouses, the static arm, the SAS-by-host selection, the account-name derivation, the assertion ordering, and the container deletion are all UNCHANGED.
* **The vended arm's "no SAS in any returned SQL" clause is SUPERSEDED because it was never true and is not made true by this change.** A vended SAS travels INLINE in the scan-spec storage block, tracked as issue [#378](https://github.com/exasol-labs/lakehouse-engine-rs/issues/378). The account-KEY half of the clause holds and is strengthened: this warehouse's CONNECTION carries no `account_key` at all, and a CONNECTION that did carry one would now reach the SQL as a REFERENCE only, under `vs-adapter/scan-spec-credential-reference`.
* **The suite also acquires the scan-script grant from the shared harness definition**, issued AFTER the `CREATE OR REPLACE CONNECTION` that provisions the connection, because a connection replacement drops the grant — verified live on Exasol 2025.2.1. `e2e-harness/e2e-harness` owns that requirement and this feature inherits it.

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
* *AND* no vended SAS value and no account-key value SHALL appear in any test output, and no account-key value SHALL appear in any returned SQL string, while the vended SAS DOES appear there under the tracked exception issue [#378](https://github.com/exasol-labs/lakehouse-engine-rs/issues/378) — SUPERSEDING the recorded clause that forbade both for the SAS; and the suite MUST NOT itself request `loadTable` to inspect the vended payload, because that would place a live SAS in the test process's memory and output for diagnostic value only
* *AND* because both credential arms share one fixture and one test function, every assertion specific to the vended arm except the cross-arm row comparison SHALL run BEFORE the static arm's assertions, so a static-arm QUERY or ASSERTION regression cannot mask the vended proof this scenario adds; a static-arm PROVISIONING failure aborts the shared fixture before any assertion runs and does mask it, which is the residual cost of one fixture, reduced but not removed by provisioning the vended arm's warehouse, seed, and virtual schema BEFORE the static arm's
* *AND* the test MUST fail (not skip) when the local stack or the Azure account is unavailable
<!-- /DELTA:CHANGED -->
