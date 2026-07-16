# Feature: SaaS Install Script — Artifact Deploy, Verification, and Stdin Safety

The SaaS installer uploads the engine and SLC tarballs through the SaaS presigned-URL
dance, creates the four product scripts at the SaaS `%udf_object` path, verifies the load
with a fingerprint smoke test, and stops at a query-ready product install — printing the
next-step `CREATE CONNECTION` / `CREATE VIRTUAL SCHEMA` SQL as a template rather than
creating dataset-specific catalog objects. It also defines the subprocess-stdin discipline
that keeps the installer safe when distributed as a piped one-liner. Split out of
`packaging/saas-install-script` to keep deploy/verification scenarios separate from
preflight and connectivity. See `packaging/saas-install-script` for prerequisite,
connectivity, and target-environment checks, and
`packaging/saas-install-script-slc-registration` for the RUST `SCRIPT_LANGUAGES`
read-modify-write that runs before this stage.

## Background

* The distributed one-liner pipes the installer body into `bash` over stdin. Every subprocess
  the installer spawns — `curl` (GitHub REST for release downloads, SaaS REST for the file API)
  and `exapump sql` — MUST take all input through arguments, flags, or files, and MUST run with
  stdin redirected from `/dev/null` (or otherwise closed), so no subprocess consumes the
  remaining piped script body and truncates or corrupts the install.
* Uploaded tarballs land under `uploads/default`: the engine at
  `/buckets/uploads/default/lakehouse-engine/udf/liblakehouse_engine.so` and the SLC under
  `/buckets/uploads/default/rustslc/`.
* Re-running the installer against a database that already holds `lakehouse-engine.tar.gz` or
  `rustslc.tar.gz` at the same SaaS file key succeeds and overwrites the existing object: POSTing
  for a presigned URL against an existing key returns a fresh URL, and the subsequent PUT replaces
  the stored object. This is confirmed SaaS file-API behavior (deploy history `staging-saas-deploy`:
  "re-uploading the same key is enough to swap the .so"), not a new edge case requiring its own
  test scenario.
* The fingerprint smoke test mirrors `docs/install.md`'s documented rule verbatim: fail only on the
  message substring `Fingerprint mismatch`; treat any other error as the expected pass. The bare
  error code `F-UDF-CL-RUST-9001` is NOT a valid discriminator — the `exasol-udf-macros` FFI shim
  raises it for every hard Rust-UDF error (`vs-adapter/pushdown-planning-join-fallback`), and the
  scan-spec deserialization error is deliberately not a fixed string
  (`datafusion-scan/scan-execution-spec-reconstitution`), so neither is a matchable literal.
* All four scripts are created in one schema (default `LHVS`): `LAKEHOUSE_ADAPTER`
  (RUST ADAPTER), `LAKEHOUSE_SCAN` (RUST SCALAR, dynamic `EMITS (...)`),
  `LAKEHOUSE_DISTINCT_MERGE_COUNT` (RUST SCALAR, `RETURNS DECIMAL(20,0)`), and
  `LAKEHOUSE_DISTRIBUTE_FILES` (LUA SET passthrough).

## Scenarios

### Scenario: Artifacts upload through the SaaS presigned-URL dance

* *GIVEN* resolved engine and SLC versions and a reachable SaaS database
* *WHEN* the script uploads each tarball
* *THEN* the script SHALL download the engine `lakehouse-engine.tar.gz` and the public `lc-rust-<version>.tar.gz` through the GitHub REST API over `curl` (the release-asset download flow below), renaming the SLC tarball to `rustslc.tar.gz` before upload
* *AND* for each tarball the script SHALL POST to the SaaS files endpoint to obtain a presigned URL and then PUT the tarball to that URL back-to-back, adding no extra headers to the PUT
* *AND* the script SHALL confirm each uploaded file is listed by the SaaS files API before proceeding

### Scenario: Release assets download through the authenticated GitHub REST API

* *GIVEN* a resolved release tag for a repository (private lakehouse-engine-rs or public language-container-rs) and a non-empty GitHub token
* *WHEN* the script downloads a release asset
* *THEN* the script SHALL fetch the release JSON for that tag from the GitHub REST API over `curl` (stdin from `/dev/null`, `GITHUB_TOKEN` bearer `Authorization` header), locate the target asset by matching its `name`, and extract the asset's numeric `id` with a no-jq bash-regex helper
* *AND* the script SHALL download the asset bytes through `GET https://api.github.com/repos/<repo>/releases/assets/<id>` sending both the header `Accept: application/octet-stream` and the `GITHUB_TOKEN` bearer `Authorization` header
* *AND* the script SHALL follow the GitHub redirect to the signed storage host with curl `-L`, and MUST NOT forward the `Authorization` header to that host (curl's default `-L` behavior, never `--location-trusted`), because the signed URL carries its own credentials and rejects a second authentication mechanism
* *AND* WHEN the asset `name` is absent from the release JSON, or the asset download fails, THEN the script MUST exit non-zero with a message naming the repository, the tag, and the asset

### Scenario: Four scripts are created at the SaaS path with correct script types

* *GIVEN* the engine `.so` has been uploaded to the SaaS bucket
* *WHEN* the script creates the deployment DDL
* *THEN* the script SHALL create all four scripts in the target schema (default `LHVS`) referencing the SaaS `%udf_object` path `/buckets/uploads/default/lakehouse-engine/udf/liblakehouse_engine.so`
* *AND* `LAKEHOUSE_SCAN` MUST be a RUST SCALAR SCRIPT with a dynamic `EMITS (...)` declaration, never a SET SCRIPT and never a static EMITS
* *AND* `LAKEHOUSE_DISTINCT_MERGE_COUNT` MUST be a RUST SCALAR SCRIPT declared `RETURNS DECIMAL(20,0)`, and `LAKEHOUSE_DISTRIBUTE_FILES` MUST be a LUA SET passthrough script
* *AND* the DDL SHALL use `CREATE SCHEMA IF NOT EXISTS` and `CREATE OR REPLACE ... SCRIPT` so a re-run neither errors nor duplicates any script

### Scenario: Fingerprint smoke test decides install success

* *GIVEN* the four scripts have been created
* *WHEN* the script runs the two-argument fingerprint smoke query `SELECT LHVS.LAKEHOUSE_SCAN('x','y') EMITS (r VARCHAR(2000000)) FROM (SELECT 1)`
* *THEN* WHEN the response contains the substring `Fingerprint mismatch` THEN the script MUST fail the install non-zero and instruct the user to align the registered SLC version with the release's `exasol-udf-sdk`/`exasol-udf-macros` pin
* *AND* WHEN the query returns any other error THEN the script SHALL treat the smoke test as passed, because the placeholder arguments `'x'`/`'y'` are intentionally not a valid scan spec, so any non-fingerprint error is the expected and correct outcome
* *AND* WHEN the query unexpectedly returns rows with no error at all THEN the script MUST fail the install non-zero and surface that anomaly, because the placeholder arguments can never deserialize as a valid scan spec
* *AND* the script MUST discriminate on the `Fingerprint mismatch` message substring, never on the bare `F-UDF-CL-RUST-9001` error code, which the `exasol-udf-macros` FFI shim raises for every hard Rust-UDF error

### Scenario: Install stops at the product and prints the next-step template

* *GIVEN* the scripts are created and the smoke test passed
* *WHEN* the script completes
* *THEN* the script MUST NOT create any `CONNECTION` or `VIRTUAL SCHEMA` object
* *AND* the script SHALL print a ready-to-edit `CREATE CONNECTION` and `CREATE VIRTUAL SCHEMA` SQL template as the documented next step
* *AND* the script SHALL exit zero to signal a query-ready product install

### Scenario: External dependency failure surfaces an actionable error

* *GIVEN* the script is executing an external step
* *WHEN* a SaaS REST call returns 404 for the account or database id, a presigned upload fails, or an `exapump sql` statement errors
* *THEN* the script MUST exit non-zero with a message naming the failed step and the likely cause
* *AND* the script MUST NOT report install success after any such failure
* *AND* the script MUST NOT print the PAT or any connection password in its output

### Scenario: Installer survives being piped to bash over stdin

* *GIVEN* the installer is invoked through the distributed one-liner form, piped into `bash -s --` over stdin rather than saved and executed as a local file
* *WHEN* the installer spawns its `curl` and `exapump sql` subprocesses
* *THEN* every subprocess MUST receive its input through arguments, flags, or files, never through inherited stdin
* *AND* every subprocess MUST run with stdin redirected from `/dev/null` so it cannot consume the remaining piped script body
* *AND* the install MUST run to completion without truncation or corruption of the script body
* *AND* the test harness MUST exercise the installer through this stdin-piped invocation path, not only as a locally saved and executed file
</content>
