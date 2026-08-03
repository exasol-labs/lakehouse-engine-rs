# Feature: Install Script — Rust SLC Registration

The Rust Script Language Container step of `deploy/scripts/install.sh`. Before any `RUST` script can load, the target database needs the `language-container-rs` SLC uploaded into its bucket and a `RUST=` alias registered in the database-wide `SCRIPT_LANGUAGES` parameter. The installer performs both on every run unless `--skip-slc` is given: it downloads the SLC release asset through the authenticated GitHub REST API, uploads it as a **tarball** through whichever channel the resolved target uses, then reads the current `SCRIPT_LANGUAGES` value and writes back a value that appends or in-place-replaces the single `RUST=` segment, preserving every other registered language.

`ALTER SYSTEM SET SCRIPT_LANGUAGES` replaces the entire persisted value, so a blind write would drop every other language on the database. Read-modify-write, never a blind overwrite, is the load-bearing property of this feature.

Scope: `register_slc`, `download_slc`, `read_script_languages`, `compute_script_languages`, and the `--skip-slc` gate in `main`. The download mechanism itself (`download_release_asset`) and the per-target upload and verification primitives (`upload_artifact`, `bucketfs_wait_for_path`) are specified in `packaging/install-script-deploy`; version resolution and the mode-resolved `TARGET_RUST_LANG_SEGMENT` are specified in `packaging/install-script-targeting`.

## Background

* The SLC asset is `lc-rust-<version>.tar.gz`, published on `exasol-labs/language-container-rs`. The installer renames it locally to `rustslc.tar.gz` before uploading, so the uploaded name — and therefore the extracted directory name the RUST alias points at — is stable across SLC versions.
* The SLC goes up as a **tarball in both target modes**, unlike the engine `.so`. BucketFS auto-extracts an uploaded `X.tar.gz` into a sibling directory `X`, and the RUST alias points at that extracted directory (`.../rustslc/exaudf/exaudfclient`), not at the archive. Auto-extraction is therefore load-bearing for the SLC and only for the SLC — the engine path deliberately sidesteps it by uploading a bare `.so` (see `packaging/install-script-deploy`).
* The RUST alias carries no SLC version: the version lives in the uploaded tarball's content. The alias differs only by target — `uploads/default/rustslc` on SaaS, `bfsdefault/<bucket>/slc/lakehouse-rustslc` on BucketFS — and is supplied to this feature as the already-resolved `TARGET_RUST_LANG_SEGMENT`.
* `SCRIPT_LANGUAGES` is read from `EXA_PARAMETERS` over the run's normal SQL connectivity, and written with `ALTER SYSTEM SET`, which requires the SYSTEM (admin) privilege.
* The alias list is a space-separated sequence of `NAME=<url>` entries whose order is preserved by the rewrite.
* Registering the SLC is a database-wide, one-time-ish act, while installing the engine is per-deployment. `--skip-slc` exists so the two can be decoupled: an already-registered SLC, or an account without `ALTER SYSTEM`, does not block the engine install.

## Scenarios

### Scenario: The SLC release asset is downloaded and uploaded as a tarball for the resolved target

* *GIVEN* a run that has resolved an SLC version and has not been given `--skip-slc`
* *WHEN* the installer registers the SLC
* *THEN* it SHALL download the asset named `lc-rust-<resolved-version>.tar.gz` from the `language-container-rs` release at the resolved tag, through the authenticated GitHub REST API
* *AND* it SHALL rename the downloaded file to `rustslc.tar.gz` before uploading, so the extracted directory name the RUST alias targets is independent of the SLC version
* *AND* it SHALL upload the file as a TARBALL in BOTH target modes, because the RUST alias points at the directory BucketFS extracts from that archive rather than at the archive itself
* *AND* in `saas` mode it SHALL upload it under the files-API key `rustslc.tar.gz`
* *AND* in `bucketfs` mode it SHALL upload it to the bucket-relative path `slc/lakehouse-rustslc.tar.gz` and SHALL then wait for that path to appear in a bucket listing before proceeding, because BucketFS unpacks an uploaded archive asynchronously

### Scenario: SCRIPT_LANGUAGES is read before it is written, and every other language survives

* *GIVEN* a database whose `SCRIPT_LANGUAGES` already registers other languages, for example `PYTHON3=builtin_python3 JAVA=builtin_java`
* *AND* no `RUST=` entry is present
* *WHEN* the installer registers the SLC
* *THEN* it SHALL read the current value from `EXA_PARAMETERS` first
* *AND* it SHALL compute the new value by APPENDING the mode-resolved RUST segment, preserving every existing entry and its original order
* *AND* only then SHALL it issue `ALTER SYSTEM SET SCRIPT_LANGUAGES` with the computed value
* *AND* it SHALL NEVER write a fixed literal, because `ALTER SYSTEM SET` replaces the entire persisted value and a blind write would drop `PYTHON3`, `JAVA` and every other registered language

### Scenario: An existing RUST entry is replaced in place, and a re-run is idempotent

* *GIVEN* a database whose `SCRIPT_LANGUAGES` already carries a `RUST=` entry — from a prior installer run, from a different SLC location, or from an unrelated registration
* *WHEN* the installer computes the new value
* *THEN* it SHALL replace that single `RUST=` entry IN PLACE with the mode-resolved segment, at its original position in the list
* *AND* the result SHALL contain exactly one `RUST=` entry, never two
* *AND* every non-`RUST` entry SHALL keep its original value and its original position
* *AND* feeding the computed value back through the same computation SHALL yield an identical value, so re-running the installer against its own prior result changes nothing

### Scenario: The RUST segment written is the one the resolved target requires

* *GIVEN* a resolved target mode
* *WHEN* the installer computes the new `SCRIPT_LANGUAGES` value
* *THEN* the segment appended or substituted SHALL be `TARGET_RUST_LANG_SEGMENT` — the `uploads/default/rustslc` alias in `saas` mode, and the `bfsdefault/<bucket>/slc/lakehouse-rustslc` alias in `bucketfs` mode, carrying the bucket in BOTH halves of the alias
* *AND* the segment SHALL be taken from the resolved target layout rather than from a literal in the registration step, so the alias and the upload destination cannot diverge
* *AND* the segment SHALL carry no SLC version, because the version lives in the uploaded tarball's content

### Scenario: An empty SCRIPT_LANGUAGES read is an anomaly and hard-fails

* *GIVEN* a `SELECT SYSTEM_VALUE FROM EXA_PARAMETERS WHERE PARAMETER_NAME='SCRIPT_LANGUAGES'` that SUCCEEDS but whose output parses to an empty value
* *WHEN* the installer reads the current value
* *THEN* it SHALL treat this as an anomaly — an unexpected query-output shape — and NOT as a legitimate empty state, because a live Exasol database always has at least one script language registered
* *AND* it SHALL exit non-zero with a message naming `SCRIPT_LANGUAGES` and explaining that appending to an empty value would drop every pre-existing language
* *AND* it SHALL NOT issue `ALTER SYSTEM SET SCRIPT_LANGUAGES`, so no language is wiped
* *AND* it SHALL NOT report a successful install

### Scenario: A privilege failure on ALTER SYSTEM aborts the install with a named cause

* *GIVEN* a connecting account that lacks the SYSTEM (admin) privilege required to register a script language
* *WHEN* the installer issues `ALTER SYSTEM SET SCRIPT_LANGUAGES`
* *THEN* the statement SHALL fail and the installer SHALL exit non-zero
* *AND* the message SHALL state that the connecting account likely lacks the SYSTEM (admin) privilege
* *AND* the run SHALL abort at that point: it SHALL NOT continue to the engine install, and SHALL NOT report a successful install, because an engine `.so` registered against an unregistered language would fail at first query rather than at install time
* *AND* `--skip-slc` SHALL be the documented escape for this case, letting the engine install proceed against an SLC the database already carries

### Scenario: `--skip-slc` drops exactly the SLC steps and nothing else

* *GIVEN* an invocation carrying `--skip-slc`, in either target mode
* *WHEN* the installer runs
* *THEN* it SHALL log that the SLC step was skipped and why
* *AND* it SHALL NOT download the SLC asset, SHALL NOT upload it, SHALL NOT read `SCRIPT_LANGUAGES`, and SHALL NOT issue `ALTER SYSTEM SET SCRIPT_LANGUAGES`
* *AND* it SHALL still resolve and print the SLC version, so the operator can see which version the database already needs to carry
* *AND* every downstream step SHALL run unchanged: the engine download and upload, the three `CREATE SCRIPT` objects, the fingerprint smoke test, and the next-step template
* *AND* a mismatch between the already-registered SLC and this engine release SHALL therefore surface at the fingerprint smoke test rather than being silently accepted

## Limitations

* **`ALTER SYSTEM SET SCRIPT_LANGUAGES` has never been confirmed to succeed under an Exasol SaaS tenant's default privileges.** Neither this implementation nor its predecessor (PR #141) verified it against a live tenant. What IS specified and tested is the failure path: a privilege-denied `ALTER SYSTEM` aborts the run with a named cause, and `--skip-slc` is the documented escape. This is a named, untested boundary — not a known-broken behavior and not a silent gap. Tracked with the installer's other follow-ups in issue [#252](https://github.com/exasol-labs/lakehouse-engine-rs/issues/252).
* **No SaaS-mode automated coverage.** The SaaS SLC upload and registration path is exercised only by the stubbed test suite and by hand-testing against a live tenant; the `install-script-e2e` CI job covers the BucketFS target only. See `packaging/install-script-deploy` for the full statement of this tracked exception.
* **The `RUST` alias name is fixed.** The installer always writes the entry under the key `RUST`, so a database that deliberately registers a differently-named Rust alias will end up with both, and Exasol will resolve whichever the script DDL names. The installer offers no flag to choose the alias key.
