# Feature: Cloud E2E Harness (Glue + SigV4)

An opt-in end-to-end smoke and performance test that exercises the full lakehouse
query path against a real Exasol cluster and a real AWS Glue Iceberg REST catalog
loaded with meaningful data, verifying that CONNECTION-object credentials, SigV4
catalog signing, and vended S3 credentials all work against the live cloud stack.
Unlike the local Docker harness (which must FAIL when its stack is down), this cloud
test is opt-in: it SKIPS cleanly when the AWS credentials are not configured, so it is
safe to run in CI or trigger manually without a permanently-attached cloud account.

## Background

<!-- DELTA:NEW -->
* **This delta makes the Glue vended path's dependency on Glue's own vended payload FALSIFIABLE, and adds nothing else.** It implements issue #276, slice D of six (A-F). `vs-adapter/pushdown-planning-cloud-credentials` now derives the effective scan storage SOLELY from the `loadTable` response when `use_vended_credentials` is true, so the static `region` this suite's CONNECTION supplies for SigV4 signing no longer reaches the scan's S3 storage.
* **The unverified premise is Glue's WHOLE vended S3 credential set, not the region alone — and the currently-green test cannot evidence any of it.** `CloudEnv::catalog_connection_password_vended` is `catalog_connection_password()` plus `use_vended_credentials: true`, so the vended CONNECTION carries a static `access_key`, `secret_key`, AND `session_token` from the AWS environment (`crates/lakehouse-engine/tests/cloud_e2e_test.rs:139-159`). The shipped preservation rule preserves all three when the response vends nothing, so `cloud_scan_reads_with_vended_credentials` passing today is fully compatible with Glue vending ZERO storage credentials — the scan would simply read with the test's own static AWS keys. The strict rule removes that mask: an absent vended key pair now kills the Glue vended path at plan time. Three keys are therefore at stake, not one — `s3.access-key-id`, `s3.secret-access-key`, and the store address (`client.region`, since Glue vends no `s3.endpoint`) — plus `s3.session-token`, whose absence beside a vended TEMPORARY key pair now yields `None` instead of the preserved static token and fails at read time rather than plan time.
* **None of that could be checked in the planning environment**, which has no AWS credentials and skips this suite. The premise is therefore asserted by the scenario below rather than assumed anywhere in the plan, and the failure mode for the address case is a clear plan-time error naming the absent key rather than a silent misroute to a region-less S3 URL.
* **No in-repo suite covers Databricks Unity Catalog, which reaches this same path.** `specs/mission.md` Core Capability 7 makes Databricks-managed Iceberg a first-class target, and `crates/lakehouse-catalog/src/vended.rs` names its flat-`config` fixture "the Databricks Unity Catalog shape where `storage_credentials` is empty and vended creds live in the flat config". A Unity Catalog response vending a key pair but neither `client.region` nor `s3.endpoint` now fails at plan time with the same clear address error, and no suite in this repository can observe it.
* **The suite's opt-in SKIP semantics are unchanged.** A missing AWS credential still skips cleanly; only the assertions inside the vended scenario change. That skip is exactly why the new assertion is a verification OBLIGATION on this suite rather than a gate the workspace `cargo test` run can discharge.
* **No credential value may appear in the new assertion's failure output.** The assertion reports which config KEY was absent from the vended response, never a vended or static value, matching the existing rule that this suite's credential-bearing DDL failures print neither the SQL nor the Exasol response.
<!-- /DELTA:NEW -->

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: Vended credentials are exercised end to end against Glue

* *GIVEN* the AWS credentials are present and the CONNECTION enables `use_vended_credentials`
* *AND* that CONNECTION supplies a static `access_key`, `secret_key`, and `region` because `use_sigv4` requires them for catalog signing
* *WHEN* the test runs a scan query whose data files are read using credentials vended by Glue's `load_table` response
* *THEN* the scan SHALL successfully read the data files using the vended credentials
* *AND* the scan SHALL succeed WITHOUT reading any static storage field from the CONNECTION, so a successful row set proves Glue's vended response alone determined both the credentials and the store address
* *AND* the test SHALL assert that the credential source selected from Glue's vended `loadTable` response carries a non-empty `s3.access-key-id` AND a non-empty `s3.secret-access-key`, because a passing scan alone cannot evidence them — this suite's CONNECTION carries static AWS keys that the shipped preservation rule would have read instead
* *AND* the test SHALL assert that same source carries a non-empty `client.region` or a non-empty `s3.endpoint`, because the strict vended rule places the store from those two values alone
* *AND* the test SHALL REPORT whether `s3.session-token` is present, because an absent vended token beside a vended temporary key pair now yields no token and fails at read time rather than plan time
* *AND* when any asserted key is absent, the test MUST fail naming that config key, rather than passing on a credential or store address the CONNECTION happened to supply
* *AND* the test output MUST NOT contain any vended or static credential value
<!-- /DELTA:CHANGED -->
