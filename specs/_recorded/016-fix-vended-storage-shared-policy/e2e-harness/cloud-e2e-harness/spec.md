# Feature: Cloud E2E Harness

Drives the engine against a real AWS Glue Iceberg REST catalog and S3 from an opt-in, env-gated suite that skips cleanly when AWS credentials are absent.

## Background

* **This delta REDUCES one verification obligation and adds nothing else; it is issue #330.** `vs-adapter/pushdown-planning-cloud-credentials` now resolves the vended store address from the CONNECTION when the CONNECTION states one and from the `loadTable` response otherwise, so Glue's static `region` — which `use_sigv4` already requires for catalog signing — places the store.
* **SUPERSEDES the premise that Glue's vended `client.region` is load-bearing.** The recorded bullet counted "three keys at stake, not one — `s3.access-key-id`, `s3.secret-access-key`, and the store address (`client.region`, since Glue vends no `s3.endpoint`)". Two remain at stake. The address is no longer one of them, because an absent vended address is now legal and the CONNECTION's `region` fills it.
* **The credential half of the obligation is UNCHANGED and stays hard.** The suite's CONNECTION carries static AWS keys, so a passing scan alone still cannot evidence that Glue vended a key pair. That assertion stays a failure, not a report.
* **The address key becomes an OBSERVATION rather than an assertion, and the reason is that its absence is no longer a defect.** Reporting what Glue vends still has diagnostic value — it is the only in-repo window onto a real cloud vended payload — but failing the suite on it would assert a requirement the engine no longer has.
* **SUPERSEDES the Databricks Unity Catalog gap bullet's conclusion.** That bullet ended: "A Unity Catalog response vending a key pair but neither `client.region` nor `s3.endpoint` now fails at plan time with the same clear address error, and no suite in this repository can observe it." That failure mode is DELETED by issue #330 — which is what the issue's defect 2 was about — so the unobservable failure is gone rather than still unobserved. The bullet's other half stands: no in-repo suite covers Databricks Unity Catalog.
* No credential value may appear in any assertion or report output. The suite reports which config KEY was present or absent, never a vended or static value. The opt-in SKIP semantics are unchanged.

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: Vended credentials are exercised end to end against Glue

* *GIVEN* the AWS credentials are present and the CONNECTION enables `use_vended_credentials`
* *AND* that CONNECTION supplies a static `access_key`, `secret_key`, and `region` because `use_sigv4` requires them for catalog signing
* *WHEN* the test runs a scan query whose data files are read using credentials vended by Glue's `load_table` response
* *THEN* the scan SHALL successfully read the data files using the vended credentials
* *AND* the scan SHALL succeed WITHOUT reading any static CREDENTIAL from the CONNECTION, so a successful row set proves Glue's vended response alone supplied the access key, secret key, and session token
* *AND* the test SHALL assert that the credential source selected from Glue's vended `loadTable` response carries a non-empty `s3.access-key-id` AND a non-empty `s3.secret-access-key`, because a passing scan alone cannot evidence them — this suite's CONNECTION carries static AWS keys that a credential fallback would have read instead
* *AND* the test SHALL REPORT whether that same source carries a non-empty `client.region` or a non-empty `s3.endpoint` and MUST NOT fail when it carries neither, because the store address now resolves from the CONNECTION's `region` when the response states none — SUPERSEDING the recorded clause that asserted this key as a pass/fail gate
* *AND* the test SHALL REPORT whether `s3.session-token` is present, because an absent vended token beside a vended temporary key pair yields no token and fails at read time rather than plan time
* *AND* when an ASSERTED key is absent, the test MUST fail naming that config key, rather than passing on a credential the CONNECTION happened to supply
* *AND* the test output MUST NOT contain any vended or static credential value
<!-- /DELTA:CHANGED -->
