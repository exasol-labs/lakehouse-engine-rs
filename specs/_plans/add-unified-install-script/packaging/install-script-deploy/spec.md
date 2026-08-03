# Feature: Install Script — Engine Deployment and Verification

The engine half of `deploy/scripts/install.sh`: download the prebuilt release artifact, upload it through the resolved target's channel, verify it actually landed, create the three deployment scripts against the mode-correct `%udf_object`, and prove the result loads with a fingerprint smoke test. The installer **never builds from source** — it always downloads a prebuilt `lakehouse-engine.tar.gz` release asset through the authenticated GitHub REST API, because the repository is private. It **stops at a query-ready product install**: it creates the deployment schema and its scripts, then prints a `CONNECTION` / `CREATE VIRTUAL SCHEMA` template for the operator to edit, and creates no dataset-specific catalog object itself.

This feature also owns the installer's cross-cutting runtime safety properties: every subprocess reads stdin from `/dev/null` so piping the script into `bash` cannot truncate it, and no credential is ever printed on any path.

Scope: `download_release_asset`, `download_engine`, `install_engine`, `extract_engine_so`, `upload_artifact`, `saas_upload_file`, `saas_verify_listed`, `bucketfs_upload_file`, `bucketfs_verify_listed`, `bucketfs_wait_for_path`, `exapump_bucketfs`, `exapump_bfs_flags`, `create_engine_scripts` and the `ddl_*` builders, `run_smoke_test`, `classify_fingerprint_response`, and `print_next_step_template`. Targeting, prerequisites, credentials and version resolution are `packaging/install-script-targeting`; the SLC step is `packaging/install-script-slc-registration`.

## Background

* The artifact is the `lakehouse-engine.tar.gz` release asset, containing `udf/liblakehouse_engine.so`. One `.so` exports both RUST entry points (see `packaging/single-so-two-entry-points`).
* GitHub's release-asset download is a two-step authenticated dance without `jq`: `GET /repos/<repo>/releases/tags/<tag>` for the release JSON, a bounded scan of its `assets` array for the asset's numeric id, then `GET /repos/<repo>/releases/assets/<id>` with `Accept: application/octet-stream`.
* The two upload channels are the only genuine difference between the targets, and `upload_artifact` is the single function that branches on `TARGET_MODE`: SaaS addresses an upload by its files-API key, BucketFS by its bucket-relative path, and each mode ignores the other's argument.
* The engine artifact shape is deliberately **asymmetric**: SaaS uploads the tarball and lets the SaaS bucket auto-extract it, while BucketFS extracts locally and uploads the bare `.so` to `udf/liblakehouse_engine.so` — the exact path `make bucketfs-upload-so` and every E2E `%udf_object` already use. Only the SLC relies on BucketFS archive auto-extraction, in both modes.
* Three deployment objects are created, all in the same schema (`LHVS` by default): `LAKEHOUSE_ADAPTER` (RUST ADAPTER SCRIPT), `LAKEHOUSE_SCAN` (RUST SCALAR SCRIPT with dynamic `EMITS (...)`), and `LAKEHOUSE_DISTRIBUTE_FILES` (a standalone LUA SET SCRIPT passthrough that is not part of the `.so`).
* The fingerprint smoke test invokes `LAKEHOUSE_SCAN('x','y')` with placeholder arguments. A valid install ALWAYS errors on those arguments — they are not a parseable scan spec — so a non-fingerprint error is the PASS signal, a fingerprint-mismatch error is the failure, and returning rows is an impossibility that indicates something else is wrong.
* The documented install path pipes the script into `bash` over stdin, so the not-yet-parsed remainder of the script body IS the shell's stdin. Any subprocess inheriting and reading it would truncate execution silently, at a point that varies with buffer sizes.

## Scenarios

### Scenario: The engine artifact is a prebuilt release asset fetched through the authenticated API

* *GIVEN* a run that has resolved an engine version
* *WHEN* the installer obtains the engine artifact
* *THEN* it SHALL download the `lakehouse-engine.tar.gz` asset from the release at the resolved tag, and SHALL NEVER build the `.so` from source
* *AND* it SHALL first `GET /repos/<repo>/releases/tags/<tag>` with the bearer token, then resolve the asset's numeric id by NAME from that response, then `GET /repos/<repo>/releases/assets/<id>` with `Accept: application/octet-stream`
* *AND* the asset-id lookup SHALL be bounded to the response's `assets` array and to the assets' own field depth, so a nested `uploader.id`, or a name appearing in some other array-of-objects field, can never be mistaken for the asset id
* *AND* the asset-id lookup SHALL be independent of field order within an asset and of asset order within the array
* *AND* an absent or non-matching asset name SHALL exit non-zero naming the repository, the tag and the asset
* *AND* the download SHALL follow redirects with plain `-L` and SHALL NEVER use `--location-trusted`, so `curl` strips the `Authorization` header across the cross-host redirect to signed storage — forwarding it would break the host-signed URL

### Scenario: A SaaS engine upload is a presigned exchange, verified against the files listing

* *GIVEN* a `saas` target run
* *WHEN* the installer uploads the engine artifact
* *THEN* it SHALL upload the TARBALL unextracted under the files-API key `lakehouse-engine.tar.gz`, letting the SaaS bucket auto-extract it into the layout `%udf_object` already encodes
* *AND* it SHALL first POST to the database's files endpoint to obtain a presigned URL, then PUT the local file to that URL
* *AND* the presigned URL SHALL be JSON-un-escaped before use, because a backend that HTML-escapes `&` returns a URL whose query-parameter separators are numeric escapes — collapsing every parameter after the first into one unparsable blob and producing an S3 `AuthorizationQueryParametersError`
* *AND* the PUT SHALL add no `Authorization` header, because the presigned URL is host-signed and rejects a second auth mechanism
* *AND* a PUT that never completes SHALL surface `curl`'s own transport diagnostic, and a PUT that completes with a non-2xx status SHALL surface BOTH the status code AND the storage host's response body, since the body is the only way to distinguish a signature mismatch from an expired URL
* *AND* the upload SHALL then be verified by listing the database's files and matching the QUOTED file name, so a stored `rustslc.tar.gz.bak` cannot satisfy a check for `rustslc.tar.gz`

### Scenario: A BucketFS engine upload is a local extraction plus an exapump copy, verified by listing

* *GIVEN* a `bucketfs` target run
* *WHEN* the installer uploads the engine artifact
* *THEN* it SHALL first extract the archive locally with `tar -xzf` into the run's temporary working directory
* *AND* it SHALL fail by name if the archive does not contain a non-empty `udf/liblakehouse_engine.so` member, naming both the archive and the expected member
* *AND* it SHALL upload the BARE extracted `.so` — never the tarball — to the bucket-relative path `udf/liblakehouse_engine.so`, which is the exact path `make bucketfs-upload-so` and every E2E `%udf_object` already use
* *AND* the upload SHALL go through `exapump bucketfs cp` and SHALL NEVER be a raw HTTP PUT
* *AND* the `exapump bucketfs` invocation SHALL carry the run's connectivity flag plus ONLY the `--bfs-*` overrides the caller actually supplied, leaving every unsupplied value to `exapump`'s own resolution
* *AND* a failed copy SHALL exit non-zero naming the local file, the bucket path, and `exapump`'s own stderr verbatim
* *AND* the upload SHALL then be verified by listing the path's parent directory and matching the basename as a WHOLE listing entry, so a stored `liblakehouse_engine.so.bak` cannot satisfy a check for `liblakehouse_engine.so`
* *AND* the verification SHALL be a BOUNDED retry rather than a single check, because BucketFS unpacks an uploaded archive asynchronously and a path accepted by the upload can be absent from the very next listing; exhausting the retries SHALL exit non-zero naming the path, the try count, and the bucket to check

### Scenario: Neither target ever touches the other target's channel

* *GIVEN* a completed run in either target mode
* *WHEN* its external calls are examined
* *THEN* a `bucketfs` run SHALL make no SaaS control-plane call, no presigned POST and no raw upload PUT
* *AND* a `saas` run SHALL make no `exapump bucketfs` call
* *AND* the mode branch SHALL exist in exactly one place — the upload dispatcher — so the difference between the targets stays auditable

### Scenario: Three deployment scripts are created against the mode-correct `%udf_object`

* *GIVEN* an uploaded and verified engine artifact
* *WHEN* the installer creates the deployment objects
* *THEN* it SHALL execute, in order: `CREATE SCHEMA IF NOT EXISTS <schema>`; `CREATE OR REPLACE RUST ADAPTER SCRIPT <schema>.LAKEHOUSE_ADAPTER`; `CREATE OR REPLACE RUST SCALAR SCRIPT <schema>.LAKEHOUSE_SCAN(common VARCHAR(2000000), files VARCHAR(2000000)) EMITS (...)`; and `CREATE OR REPLACE LUA SET SCRIPT <schema>.LAKEHOUSE_DISTRIBUTE_FILES(files VARCHAR(2000000)) EMITS (files VARCHAR(2000000))`
* *AND* the schema SHALL default to `LHVS` and be overridable with `--schema`
* *AND* both RUST scripts SHALL reference `TARGET_SO_UDF_OBJECT` as their `%udf_object`, so the DDL names the path the artifact was actually uploaded to for this target
* *AND* the scan script SHALL be a SCALAR script with dynamic `EMITS (...)`, never a SET script
* *AND* the LUA distributor SHALL declare no `%udf_object` and SHALL NOT reference the uploaded `.so`
* *AND* the use of `CREATE OR REPLACE` and `CREATE SCHEMA IF NOT EXISTS` SHALL make the whole step idempotent, so re-running the installer upgrades an existing install rather than failing
* *AND* a failed statement SHALL exit non-zero naming the schema and the statement that failed

### Scenario: A fingerprint smoke test proves the artifact loads against the registered SLC

* *GIVEN* a completed engine install
* *WHEN* the installer runs `SELECT <schema>.LAKEHOUSE_SCAN('x', 'y') EMITS (r VARCHAR(2000000)) FROM (SELECT 1)`
* *THEN* an error containing `Fingerprint mismatch` SHALL be classified as a FAILURE: the installer SHALL exit non-zero stating that the registered SLC does not match this release's `exasol-udf-sdk` / `exasol-udf-macros` pin, and SHALL point at `--slc-version` as the remedy
* *AND* a statement that SUCCEEDS and returns rows SHALL be classified as an ANOMALY and SHALL exit non-zero, because the placeholder arguments can never be a valid scan spec and so a valid install can never return rows for them
* *AND* any OTHER error SHALL be classified as a PASS — the `.so` loaded, the entry point dispatched, and it rejected the placeholder scan spec, which is exactly the expected behavior
* *AND* the test SHALL require no catalog credentials and no data, so it runs on every install
* *AND* the run SHALL NOT report a successful install unless the smoke test passes

### Scenario: The installer stops at a query-ready product install and creates no catalog object

* *GIVEN* a smoke test that passed
* *WHEN* the installer finishes
* *THEN* it SHALL print a `CREATE OR REPLACE CONNECTION` template and a `CREATE VIRTUAL SCHEMA … USING <schema>.LAKEHOUSE_ADAPTER` template to stdout, with placeholder values for the catalog URI, warehouse, region and credentials
* *AND* it SHALL EXECUTE neither statement, because both are dataset-specific and belong to the operator
* *AND* it SHALL state that these objects are the next step and are not created by the installer
* *AND* it SHALL then report that the engine is installed and query-ready in the deployment schema

### Scenario: Piping the script into bash cannot truncate it

* *GIVEN* the documented one-liner, which pipes the script body into `bash -s -- <flags>` over stdin
* *WHEN* the installer runs
* *THEN* EVERY subprocess it spawns — every `curl` and every `exapump` invocation — SHALL read stdin from `/dev/null`
* *AND* a stdin-piped run SHALL reach the same end state as a saved-file run: version resolution, both uploads, all three scripts, the smoke test and the template
* *AND* no subprocess SHALL ever observe data present on the installer's own stdin, so the not-yet-parsed remainder of the script body can never be consumed
* *AND* this SHALL be proven per-subprocess by a sentinel payload on the installer's stdin, not by inspection, because a single missing redirect fails silently and at a point that varies with buffer sizes

### Scenario: No credential is printed on any path

* *GIVEN* any run, successful or failing, in any connectivity mode and either target
* *WHEN* the installer's combined stdout and stderr are examined
* *THEN* the SaaS PAT, the SQL password and the BucketFS write password SHALL NOT appear
* *AND* this SHALL hold for a credential supplied on the command line, one carried in a DSN, and one read from an exapump profile
* *AND* it SHALL hold on failure paths — a failed reachability check, a failed upload, a failed derivation — as well as on success

### Scenario: Every external failure names the step, the cause and the remedy

* *GIVEN* a failure originating outside the installer — an unreachable database, a rejected upload, a missing release asset, a privilege denial
* *WHEN* the installer reports it
* *THEN* the message SHALL name the step that failed
* *AND* it SHALL surface the external tool's own diagnostic — `curl`'s stderr, the storage host's response body, or `exapump`'s stderr — rather than replacing it with a generic message
* *AND* it SHALL name what the operator needs to check or change
* *AND* the run SHALL exit non-zero without reporting a successful install
* *AND* the temporary working directory SHALL be removed on exit, on every path

### Scenario: A real BucketFS install is verified continuously against a live Exasol

* *GIVEN* every push and pull request
* *WHEN* CI runs
* *THEN* the `install-script` job SHALL ShellCheck both the installer and its test harness and run the stubbed test suite with no network access
* *AND* the `install-script-e2e` job SHALL run the REAL BucketFS install flow against a live `exasol` compose service, using a REAL prior GitHub release fetched through the authenticated API with NO version pin — matching a real user's default invocation
* *AND* both jobs SHALL be required checks AND SHALL block the `release` job, because `main` is a live distribution channel with no release gate before a user's next `curl | bash`

## Limitations

* **SaaS mode has no CI job.** It needs a real SaaS tenant and a real PAT that CI does not have. The SaaS upload, DDL and smoke-test path is proven only by the stubbed integration suite (which exercises the presigned exchange, the JSON un-escaping, the failure surfacing and the listing verification against recording fakes) and by hand-testing against a live tenant. This is a **named tracked exception**, following this repository's convention that a known gap is stated explicitly rather than left silent; it is tracked in issue [#252](https://github.com/exasol-labs/lakehouse-engine-rs/issues/252), and a dedicated follow-up issue for "SaaS-mode integration test for `install.sh`" should be filed if one does not already exist.
* **The engine `.so` is not checksummed after upload.** Verification establishes that a whole-token-matching entry appears in the listing, not that its bytes match what was sent. Byte-level proof comes indirectly, from the fingerprint smoke test loading the artifact.
* **No uninstall.** The installer creates and replaces; it never drops a script, removes an uploaded artifact, or restores a prior `SCRIPT_LANGUAGES` value.
