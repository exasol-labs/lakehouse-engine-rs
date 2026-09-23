# Feature: Cloud E2E Harness (Glue + SigV4)

Unchanged by this plan, and reproduced here only because the delta validator requires a description. The recorded text stands as written in `specs/e2e-harness/cloud-e2e-harness/spec.md`.

<!-- DELTA:CHANGED -->
## Background

The test is gated behind a dedicated cargo feature distinct from `exasol-e2e` (e.g.
`cloud-e2e`), so the local-Docker suite's fail-when-down semantics are never changed.
The cloud test discovers its credentials and endpoints from environment variables. The
Exasol CONNECTION used by the virtual schema is created from those same environment
values. All DSN/connection strings include `validateservercertificate=0`. No credential
value is printed to test output. The same cloud path is driven by the remote bench
harness (`bench/run.sh` with `BENCH_TARGET=remote`), which builds the
`CREATE VIRTUAL SCHEMA` statement against the live cluster from the bench environment.
The cloud suite drives Exasol through the shared `common/exasol_ws::ExaConn` WebSocket
client — the same client the local Docker suite uses — connected in a redacting mode so
credential-bearing SQL never reaches test output.
* **This delta makes the Glue vended path's dependency on Glue's own vended payload FALSIFIABLE, and adds nothing else.** It implements issue #276, slice D of six (A-F). `vs-adapter/pushdown-planning-cloud-credentials` now derives the effective scan storage SOLELY from the `loadTable` response when `use_vended_credentials` is true, so the static `region` this suite's CONNECTION supplies for SigV4 signing no longer reaches the scan's S3 storage.
* **The unverified premise is Glue's WHOLE vended S3 credential set, not the region alone — and the currently-green test cannot evidence any of it.** `CloudEnv::catalog_connection_password_vended` is `catalog_connection_password()` plus `use_vended_credentials: true`, so the vended CONNECTION carries a static `access_key`, `secret_key`, AND `session_token` from the AWS environment (`crates/lakehouse-engine/tests/cloud_e2e_test.rs:139-159`). The shipped preservation rule preserves all three when the response vends nothing, so `cloud_scan_reads_with_vended_credentials` passing today is fully compatible with Glue vending ZERO storage credentials — the scan would simply read with the test's own static AWS keys. The strict rule removes that mask: an absent vended key pair now kills the Glue vended path at plan time. Three keys are therefore at stake, not one — `s3.access-key-id`, `s3.secret-access-key`, and the store address (`client.region`, since Glue vends no `s3.endpoint`) — plus `s3.session-token`, whose absence beside a vended TEMPORARY key pair now yields `None` instead of the preserved static token and fails at read time rather than plan time.
* **None of that could be checked in the planning environment**, which has no AWS credentials and skips this suite. The premise is therefore asserted by the scenario below rather than assumed anywhere in the plan, and the failure mode for the address case is a clear plan-time error naming the absent key rather than a silent misroute to a region-less S3 URL.
* **No in-repo suite covers Databricks Unity Catalog, which reaches this same path.** `specs/mission.md` Core Capability 7 makes Databricks-managed Iceberg a first-class target, and `crates/lakehouse-catalog/src/vended.rs` names its flat-`config` fixture "the Databricks Unity Catalog shape where `storage_credentials` is empty and vended creds live in the flat config". A Unity Catalog response vending a key pair but neither `client.region` nor `s3.endpoint` now fails at plan time with the same clear address error, and no suite in this repository can observe it.
* **The suite's opt-in SKIP semantics are unchanged.** A missing AWS credential still skips cleanly; only the assertions inside the vended scenario change. That skip is exactly why the new assertion is a verification OBLIGATION on this suite rather than a gate the workspace `cargo test` run can discharge.
* **No credential value may appear in the new assertion's failure output.** The assertion reports which config KEY was absent from the vended response, never a vended or static value, matching the existing rule that this suite's credential-bearing DDL failures print neither the SQL nor the Exasol response.
* **This delta REDUCES one verification obligation and adds nothing else; it is issue #330.** `vs-adapter/pushdown-planning-cloud-credentials` now resolves the vended store address from the CONNECTION when the CONNECTION states one and from the `loadTable` response otherwise, so the static `region` that this suite's Glue CONNECTION states places the store.
* **SUPERSEDES the premise that Glue's vended `client.region` is load-bearing.** The recorded bullet counted "three keys at stake, not one — `s3.access-key-id`, `s3.secret-access-key`, and the store address (`client.region`, since Glue vends no `s3.endpoint`)". Two remain at stake. The address is no longer one of them, because an absent vended address is now legal and the CONNECTION's `region` fills it.
* **The credential half of the obligation is UNCHANGED and stays hard.** The suite's CONNECTION carries static AWS keys, so a passing scan alone still cannot evidence that Glue vended a key pair. That assertion stays a failure, not a report.
* **The address key becomes an OBSERVATION rather than an assertion, and the reason is that its absence is no longer a defect.** Reporting what Glue vends still has diagnostic value — it is the only in-repo window onto a real cloud vended payload — but failing the suite on it would assert a requirement the engine no longer has.
* **SUPERSEDES the Databricks Unity Catalog gap bullet's conclusion.** That bullet ended: "A Unity Catalog response vending a key pair but neither `client.region` nor `s3.endpoint` now fails at plan time with the same clear address error, and no suite in this repository can observe it." That failure mode is DELETED by issue #330 — which is what the issue's defect 2 was about — so the unobservable failure is gone rather than still unobserved. The bullet's other half stands: no in-repo suite covers Databricks Unity Catalog.
* **This delta adds a catalog-selection arm to the remote bench target and changes nothing else.** The remote target has always been Glue-only: `bench/run.sh`'s `remote)` arm unconditionally builds one hardcoded SigV4/Glue CONNECTION password. It now selects between that arm and a Lakekeeper arm, with Glue the default, so an existing remote run is unchanged when the new variable is unset. The AWS-side Lakekeeper deployment and the table registration that fills it are owned by `lakekeeper-e2e/aws-lakekeeper-perf-catalog`; this feature owns only the harness's choice between the two catalogs.
* **The two arms need separate password builders, not one parameterized builder.** `docs/catalogs.md` § "Connection fields" states the adapter rejects a CONNECTION combining `use_sigv4` with `client_id` or `client_secret`. Glue requires `use_sigv4`; Lakekeeper requires the OAuth2 client-credentials fields. The shapes are mutually exclusive by adapter rule, so the split mirrors the existing `build_conn_password_local` / `build_conn_password_cloud` split rather than adding a flag to one function.
* **The Lakekeeper arm needs `ALLOW_HTTP` and the Glue arm must not have it.** The AWS Lakekeeper endpoint and its Keycloak token endpoint are plain HTTP inside the VPC, the same condition the docker target already passes `ALLOW_HTTP` for. Glue and S3 are HTTPS, and the offline selftest at `bench/run.sh:129` asserts an extra-properties block with no `ALLOW_HTTP`, keying off the `allow_http=false` argument to `build_vs_extra_props` rather than off the remote arm. This delta records that behavior as a requirement on the Glue arm for the first time; it is not narrowing an existing recorded requirement.
* **No engine, adapter, or CONNECTION-field change is in scope.** Lakekeeper support is already shipped (`docs/catalogs.md`, `lakekeeper-e2e/lakekeeper-e2e-harness`). Both arms produce a CONNECTION and a virtual schema through the same catalog-agnostic DDL the harness already emits, and `CATALOG_KIND` stays unset on both, because Lakekeeper is an Iceberg REST catalog exactly as Glue is.
* **The cloud E2E test itself is unchanged.** This delta touches the bench harness only. The `cloud-e2e` cargo-feature suite keeps its Glue-only opt-in skip semantics and gains no Lakekeeper scenario.
* **A live demo reuses the schema a benchmark run leaves behind, so the harness's existing teardown timing becomes a requirement.** `bench/run.sh:351-352` drops and recreates the virtual schema at the START of a run and never at the end, and the CONNECTION at `bench/run.sh:344` is a `CREATE OR REPLACE` with no matching drop. An operator demonstrating the engine against Lakekeeper therefore runs the benchmark once and then queries the surviving virtual schema interactively. That is the ONLY difference between the benchmark and demo contexts: no separate query set, warehouse, namespace, report format, or selection variable exists, and nothing in the harness branches on which one is running. This delta records the existing teardown timing as a requirement so a later cleanup addition cannot silently break the demo; it changes no behavior today.

<!-- /DELTA:CHANGED -->

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: Vended credentials are exercised end to end against Glue

* *GIVEN* the AWS credentials are present and the CONNECTION enables `use_vended_credentials`
* *AND* that CONNECTION supplies a static `access_key`, `secret_key`, and `region`, because SigV4 signs the catalog requests with them and the stated `region` also places the store
* *WHEN* the test runs a scan query whose data files are read using credentials vended by Glue's `load_table` response
* *THEN* the scan SHALL successfully read the data files using the vended credentials
* *AND* the scan SHALL succeed WITHOUT reading any static CREDENTIAL from the CONNECTION, so a successful row set proves Glue's vended response alone supplied the access key, secret key, and session token
* *AND* the test SHALL assert that the credential source selected from Glue's vended `loadTable` response carries a non-empty `s3.access-key-id` AND a non-empty `s3.secret-access-key`, because a passing scan alone cannot evidence them — this suite's CONNECTION carries static AWS keys that a credential fallback would have read instead
* *AND* the test SHALL REPORT whether that same source carries a non-empty `client.region` or a non-empty `s3.endpoint` and MUST NOT fail when it carries neither, because the store address now resolves from the CONNECTION's `region` when the response states none — SUPERSEDING the recorded clause that asserted this key as a pass/fail gate
* *AND* the test SHALL REPORT whether `s3.session-token` is present, because an absent vended token beside a vended temporary key pair yields no token and fails at read time rather than plan time
* *AND* when an ASSERTED key is absent, the test MUST fail naming that config key, rather than passing on a credential the CONNECTION happened to supply
* *AND* the test output MUST NOT contain any vended or static credential value

<!-- /DELTA:CHANGED -->

<!-- DELTA:NEW -->
### Scenario: A Glue CONNECTION that omits region lists the Glue table through SigV4-signed catalog requests

* *GIVEN* the AWS credential and endpoint environment variables are present
* *AND* `GLUE_CATALOG_URI` begins with `https://glue.<AWS_REGION>.amazonaws.com/`, so it is a standard AWS Glue endpoint for the region that `AWS_REGION` holds
* *AND* an Exasol CONNECTION whose address is `GLUE_CATALOG_URI` and whose password JSON enables SigV4, supplies the key pair, and omits `region`
* *WHEN* the test creates a virtual schema over the Glue namespace through that CONNECTION
* *THEN* `CREATE VIRTUAL SCHEMA` SHALL succeed
* *AND* the virtual schema SHALL list the configured Glue table, which proves that Glue accepted the namespace-enumeration and `loadTable` requests signed for the region that the endpoint host names
* *AND* the test SHALL read no data file through that virtual schema, because a signing region derived from the endpoint places no S3 store
* *AND* when `GLUE_CATALOG_URI` does not begin with that prefix, the test SHALL skip with a message that names the reason, and MUST NOT fail
* *AND* the test output MUST NOT contain any credential value
<!-- /DELTA:NEW -->
