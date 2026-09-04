# Feature: Cloud E2E Harness (Glue + SigV4)

An opt-in end-to-end smoke and performance test that exercises the full lakehouse query path against a real Exasol cluster and a real AWS Glue Iceberg REST catalog loaded with meaningful data, verifying that CONNECTION-object credentials, SigV4 catalog signing, and vended S3 credentials all work against the live cloud stack. The remote bench harness drives the same cloud path and now selects its catalog backend, defaulting to Glue.

## Background

<!-- DELTA:NEW -->
* **This delta adds a catalog-selection arm to the remote bench target and changes nothing else.** The remote target has always been Glue-only: `bench/run.sh`'s `remote)` arm unconditionally builds one hardcoded SigV4/Glue CONNECTION password. It now selects between that arm and a Lakekeeper arm, with Glue the default, so an existing remote run is unchanged when the new variable is unset. The AWS-side Lakekeeper deployment and the table registration that fills it are owned by `e2e-harness/aws-lakekeeper-perf-catalog`; this feature owns only the harness's choice between the two catalogs.
* **The two arms need separate password builders, not one parameterized builder.** `docs/catalogs.md` § "Connection fields" states the adapter rejects a CONNECTION combining `use_sigv4` with `client_id` or `client_secret`. Glue requires `use_sigv4`; Lakekeeper requires the OAuth2 client-credentials fields. The shapes are mutually exclusive by adapter rule, so the split mirrors the existing `build_conn_password_local` / `build_conn_password_cloud` split rather than adding a flag to one function.
* **The Lakekeeper arm needs `ALLOW_HTTP` and the Glue arm must not have it.** The AWS Lakekeeper endpoint and its Keycloak token endpoint are plain HTTP inside the VPC, the same condition the docker target already passes `ALLOW_HTTP` for. Glue and S3 are HTTPS, and the offline selftest at `bench/run.sh:129` asserts an extra-properties block with no `ALLOW_HTTP`, keying off the `allow_http=false` argument to `build_vs_extra_props` rather than off the remote arm. This delta records that behavior as a requirement on the Glue arm for the first time; it is not narrowing an existing recorded requirement.
* **No engine, adapter, or CONNECTION-field change is in scope.** Lakekeeper support is already shipped (`docs/catalogs.md`, `e2e-harness/lakekeeper-e2e-harness`). Both arms produce a CONNECTION and a virtual schema through the same catalog-agnostic DDL the harness already emits, and `CATALOG_KIND` stays unset on both, because Lakekeeper is an Iceberg REST catalog exactly as Glue is.
* **The cloud E2E test itself is unchanged.** This delta touches the bench harness only. The `cloud-e2e` cargo-feature suite keeps its Glue-only opt-in skip semantics and gains no Lakekeeper scenario.
* **A live demo reuses the schema a benchmark run leaves behind, so the harness's existing teardown timing becomes a requirement.** `bench/run.sh:351-352` drops and recreates the virtual schema at the START of a run and never at the end, and the CONNECTION at `bench/run.sh:344` is a `CREATE OR REPLACE` with no matching drop. An operator demonstrating the engine against Lakekeeper therefore runs the benchmark once and then queries the surviving virtual schema interactively. That is the ONLY difference between the benchmark and demo contexts: no separate query set, warehouse, namespace, report format, or selection variable exists, and nothing in the harness branches on which one is running. This delta records the existing teardown timing as a requirement so a later cleanup addition cannot silently break the demo; it changes no behavior today.
<!-- /DELTA:NEW -->

## Scenarios

<!-- DELTA:NEW -->
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
<!-- /DELTA:NEW -->
