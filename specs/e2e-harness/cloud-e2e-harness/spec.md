# Feature: Cloud E2E Harness (Glue + SigV4)

An opt-in end-to-end smoke and performance test that exercises the full lakehouse
query path against a real Exasol cluster and a real AWS Glue Iceberg REST catalog
loaded with meaningful data, verifying that CONNECTION-object credentials, SigV4
catalog signing, and vended S3 credentials all work against the live cloud stack.
Unlike the local Docker harness (which must FAIL when its stack is down), this cloud
test is opt-in: it SKIPS cleanly when the AWS credentials are not configured, so it is
safe to run in CI or trigger manually without a permanently-attached cloud account.
The remote bench harness drives the same cloud path and now selects its catalog backend,
defaulting to Glue.

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
* **This delta REDUCES one verification obligation and adds nothing else; it is issue #330.** `vs-adapter/pushdown-planning-cloud-credentials` now resolves the vended store address from the CONNECTION when the CONNECTION states one and from the `loadTable` response otherwise, so Glue's static `region` — which `use_sigv4` already requires for catalog signing — places the store.
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

## Scenarios

### Scenario: Cloud smoke test queries a real Glue-backed virtual schema

* *GIVEN* the AWS credential and endpoint environment variables are present
* *AND* an Exasol cluster reachable from the test, with the SLC and engine `.so` installed
* *AND* an Exasol CONNECTION whose address is the Glue Iceberg REST endpoint and whose password JSON enables SigV4 and supplies the region and keys
* *WHEN* the test creates a virtual schema over a Glue-managed Iceberg table loaded with meaningful data and runs a projection + filter query through it
* *THEN* the query SHALL return rows consistent with the seeded data (correct columns, predicate honoured)
* *AND* the test SHALL exercise the SigV4-signed catalog path and the S3 data-file read path end to end

### Scenario: Cloud test skips cleanly when AWS credentials are absent

* *GIVEN* the AWS credential environment variables are not set
* *WHEN* the cloud E2E test runs
* *THEN* the test SHALL skip without failing
* *AND* the test MUST NOT attempt any network call to AWS or Exasol
* *AND* the skip behaviour MUST be distinct from the local-Docker suite, which still FAILS when its stack is down

### Scenario: Cloud performance smoke records timing and row-count sanity

* *GIVEN* the AWS credentials are present and a Glue-backed virtual schema over a meaningfully-sized table
* *WHEN* the test runs an aggregate query (e.g. a grouped COUNT/SUM) through the virtual schema
* *THEN* the result row count and aggregate values SHALL be sane (non-zero, matching the seeded data shape)
* *AND* the test SHALL record the wall-clock query duration for manual inspection
* *AND* the test MUST NOT assert a hard latency threshold (timing is observational, not a pass/fail gate)

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

### Scenario: Remote bench wires NR_OF_CORES and PARALLELISM_FACTOR into the virtual schema

* *GIVEN* the remote bench target running against a real Glue catalog and external Exasol cluster
* *AND* `BENCH_NR_OF_CORES` and `BENCH_PARALLELISM_FACTOR` set in the bench environment
* *WHEN* the bench harness builds the `CREATE VIRTUAL SCHEMA` statement for the remote target
* *THEN* the harness SHALL pass `NR_OF_CORES` and `PARALLELISM_FACTOR` as virtual-schema properties on the remote target, just as the docker target already does
* *AND* the property values SHALL come from `BENCH_NR_OF_CORES` and `BENCH_PARALLELISM_FACTOR`, applying the same defaults the docker path uses when those variables are unset
* *AND* the remote path MUST NOT emit an empty extra-properties block that drops these parallelism knobs (the prior behaviour where the cluster ran at its built-in defaults)

### Scenario: Cloud suite drives Exasol through the shared redacting WebSocket client

* *GIVEN* the AWS credential and endpoint environment variables are present
* *AND* the cloud E2E suite opens its Exasol WebSocket SQL session
* *WHEN* it issues credential-bearing DDL (a CONNECTION carrying SigV4 or vended keys) that fails
* *THEN* the suite SHALL use the shared `common/exasol_ws::ExaConn` client — the same client the local Docker suite uses — connected in a redacting mode that omits the SQL statement and the Exasol response body from the `execute()` DDL-failure panic; this redaction covers the `execute()` DDL-failure path only, leaving `query_scalar_i64`, `query_row_count`, and the `connect()` auth-failure assertion unredacted for this fold (the cloud suite passes no credential-bearing SQL through them and the login response carries no credential value)
* *AND* that `execute()` failure message MUST NOT print the failing SQL or the Exasol response, so no static, SigV4, or vended credential value embedded in the credential-bearing DDL SHALL appear in the failure output
* *AND* the redacting behaviour MUST be opt-in, so the local Docker suite still includes the failing SQL in its `execute()` failure messages for debuggability

### Scenario: Remote bench selects its catalog backend from the bench environment

* *GIVEN* the remote bench target running against an external Exasol cluster
* *AND* the catalog-selection variable is unset in the bench environment
* *WHEN* the harness resolves its catalog configuration
* *THEN* the harness SHALL take the Glue path, requiring the same variable set, building the same catalog URI and CONNECTION password JSON, and emitting the same virtual-schema properties as it did before this delta, so an existing remote run is unchanged
* *AND* when the variable names Lakekeeper, the harness SHALL require the Lakekeeper catalog URI, warehouse name, OAuth2 client id, client secret, and token endpoint, and SHALL fail naming any one of them that is empty
* *AND* when the variable names Lakekeeper, the CONNECTION address SHALL be the Lakekeeper catalog URI and the CONNECTION password SHALL be built by a Lakekeeper-specific builder, not by the Glue builder with different arguments
* *AND* when the variable holds any other value, the harness SHALL exit with an error naming the accepted values, rather than silently falling back to either catalog
* *AND* the docker target's behavior MUST NOT change under any value of that variable, because the local stack has exactly one catalog
* *AND* both remote arms SHALL keep passing `NR_OF_CORES` and `PARALLELISM_FACTOR` as virtual-schema properties from the same bench variables with the same defaults, per this feature's recorded remote-parallelism scenario
* *AND* the benchmark report header SHALL name the catalog the run used, so two reports over the same data are distinguishable after the fact — this is the ONLY report-output change on the Glue arm, whose required variables, catalog URI, CONNECTION password, virtual-schema properties, query set, and row counts all stay unchanged
* *AND* that header field SHALL carry the catalog NAME only and MUST NOT carry an `s3://`-shaped value, because `bench/import_ceiling.sh:29` greps the whole report file with `grep -oE 's3://[^"]*/lineitem'` to derive its table root, so an `s3://`-shaped header value poisons that downstream script
* *AND* that header field SHALL be emitted on the REMOTE target ONLY, and the DOCKER target's report header SHALL carry no such field under any value of the catalog-selection variable, including unset. `bench/run.sh:378-383` writes ONE header block for every target, so the field MUST be emitted conditionally rather than unconditionally: the selection variable defaults to `glue`, and the local stack's catalog is neither Glue nor the deployed Lakekeeper, so an unconditional field would label a local run `catalog=glue` and write that false value into `bench/reports/*.txt`

### Scenario: The Lakekeeper CONNECTION password carries OAuth2 credentials and never SigV4

* *GIVEN* the remote bench target with the catalog-selection variable naming Lakekeeper
* *WHEN* the harness builds the CONNECTION password JSON
* *THEN* that JSON SHALL carry the Lakekeeper warehouse NAME, the OAuth2 client id, the client secret, and the OAuth2 token endpoint
* *AND* it SHALL carry the static S3 endpoint, region, access key, and secret key, so the scan reads the data files with the existing read-only credentials rather than credentials vended by the catalog
* *AND* it MUST NOT set the SigV4 flag and MUST NOT set the vended-credentials flag, because the adapter rejects SigV4 combined with OAuth2 client credentials and the deployed warehouse disables credential vending
* *AND* every value embedded in that JSON SHALL have its single quotes doubled before the JSON reaches a SQL string literal, matching the escaping the Glue builder already applies
* *AND* the harness SHALL set `ALLOW_HTTP` on the virtual schema for this arm, because both the Lakekeeper catalog endpoint and its token endpoint are plain HTTP; the Glue arm's extra-properties block SHALL still carry no `ALLOW_HTTP`
* *AND* no client secret, access key, or secret key value SHALL be printed by the harness or appear in its offline self-check output

### Scenario: A completed remote run leaves the CONNECTION and virtual schema in place

* *GIVEN* an operator demonstrating the engine live against the Lakekeeper catalog, who queries the
  virtual schema by hand AFTER a benchmark run finishes
* *WHEN* the harness completes a remote run
* *THEN* it SHALL leave both the CONNECTION and the virtual schema present and queryable, because the
  interactive demo has no other way to obtain them and this feature deliberately adds no
  setup-only mode, no demo-specific DDL path, and no suite-selection variable
* *AND* the harness SHALL drop the virtual schema ONLY as the first half of a drop-then-create pair at
  the START of a run, never as a cleanup step at the end, and SHALL NOT drop the CONNECTION at all
* *AND* the offline self-check SHALL assert that invariant against the harness's own source text, so a
  later cleanup addition fails the check rather than silently removing the demo's query surface
* *AND* that guarantee SHALL be scoped to the HARNESS ONLY, and MUST NOT be read as a guarantee about
  the operator's workflow. `deploy/scripts/bench-remote.sh:55` installs `trap teardown EXIT`, whose
  handler runs `deploy/scripts/cluster-down.sh <env>` on EVERY exit path — success, failure, and
  interrupt alike — unless `KEEP_ALIVE=1` was exported. That teardown destroys the Exasol cluster,
  and with it the CONNECTION and the virtual schema this scenario requires the harness to leave
  behind. The harness leaves both in place; the wrapper then removes the cluster they live in
* *AND* the offline self-check's source-text guard SHALL therefore be understood as covering
  `bench/run.sh` alone. It reads no other file, so it stays green while a default `bench-remote.sh`
  run ends the demo, and it MUST NOT be cited as evidence that a demo surface survives an
  operator-level run
* *AND* a live demo MUST NOT be driven by a default `bench-remote.sh` invocation, and
  `deploy/README.md` SHALL state the FULL ORDERED sequence for both surviving forms rather than
  leaving the operator to infer it from this feature's teardown timing. WRAPPER form:
  `deploy/scripts/lakekeeper-up.sh <env>` FIRST, then `AWS_PROFILE=... BENCH_CATALOG=lakekeeper
  KEEP_ALIVE=1 ./bench-remote.sh <env>`, which performs the `cluster-stack` `tofu apply`,
  `cluster-up.sh`, `secrets.sh`, and `make bench` itself at its own steps `[1/4]` through `[4/4]`.
  UNWRAPPED form: the `cluster-stack` `tofu apply` of `deploy/README.md` § "2. Test cluster", then
  `deploy/scripts/lakekeeper-up.sh <env>`, then `deploy/scripts/cluster-up.sh <env>`, then
  `deploy/scripts/secrets.sh <env>`, then `BENCH_CATALOG=lakekeeper make bench`
* *AND* both forms SHALL carry `BENCH_CATALOG=lakekeeper` explicitly. The variable defaults to
  `glue`, and `deploy/scripts/bench-remote.sh` passes caller-exported `BENCH_*` through to
  `make bench` untouched, so a wrapper invocation omitting it demonstrates GLUE at a live customer
  session
* *AND* both forms SHALL run `deploy/scripts/lakekeeper-up.sh <env>` BEFORE `secrets.sh <env>` —
  including the `secrets.sh` call `bench-remote.sh` makes internally at its step `[3/4]`, which is
  why the wrapper form places `lakekeeper-up.sh` before the whole wrapper invocation. `secrets.sh`
  emits the Lakekeeper block only while a Lakekeeper stack workspace exists for that same
  environment (`aws-lakekeeper-perf-catalog` § "Scenario: Bench secrets carry both catalogs'
  variables from one environment"), and `BENCH_CATALOG=lakekeeper` then fails naming the first empty
  variable
* *AND* both forms SHALL be documented as leaving a RUNNING, BILLING Exasol cluster behind, so the
  runbook closes with an explicit `deploy/scripts/cluster-down.sh <env>` and
  `deploy/scripts/lakekeeper-down.sh <env>` after the interactive session ends
