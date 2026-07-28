# Feature: Install Script — Targeting and Preflight

Everything `deploy/scripts/install.sh` decides **before it touches the network**: which tools must be present, which credentials are required, which install target this run is for, which paths that target implies, and which artifact versions to fetch. The installer supports two targets — Exasol SaaS, and BucketFS (Exasol AsApp, Docker, and on-premise, which all speak the same BucketFS HTTP interface through `exapump`) — and detects which one it is installing to from the flags it was given, rather than from a required target flag. Every check in this feature runs before the first download, so a misconfigured run costs zero bytes and produces one named error.

This feature also owns the installer's own **distribution contract**: how the script reaches the operator's shell, and how a run is pinned for reproducibility.

Scope: `parse_args`, `validate_required`, `validate_connectivity`, `check_prereqs`, `resolve_target_mode`, `resolve_bfs_bucket_from_profile`, `resolve_target_layout`, `validate_bucketfs_required`, `resolve_saas_pat`, `resolve_saas_base`, `resolve_versions`, and `usage`, plus the documented one-liner in `docs/install.md`. The SLC step is `packaging/install-script-slc-registration`; the engine artifact, the DDL and the smoke test are `packaging/install-script-deploy`.

## Background

* The installer is a single bash file (3.2+, stock macOS), with no `jq` and no TOML library. It is **sourceable**: `main` runs only when the file is executed or piped (`BASH_SOURCE[0]` guard), so every function below is unit-testable in isolation.
* Two upload channels exist and nothing else differs between the targets: SaaS uploads through a control-plane REST API and a presigned-URL exchange; BucketFS uploads through `exapump bucketfs cp`. Prerequisites, connectivity, version resolution, `SCRIPT_LANGUAGES`, the DDL, the smoke test and the next-step template are target-independent.
* The mode-parameterized core is four globals seeded once by `resolve_target_layout` — `TARGET_SO_UDF_OBJECT`, `TARGET_RUST_LANG_SEGMENT`, `TARGET_SLC_BFS_PATH`, `TARGET_ENGINE_BFS_PATH`. Every later step reads a `TARGET_*` global instead of a target-specific constant.
* `exapump bucketfs` is **not** `exapump sql`: it accepts no `--dsn`, `--user` or `--password`, only a named profile plus `--bfs-*` overrides. It also requires at least one profile to exist in the exapump config **even when every `--bfs-*` flag is supplied explicitly** — verified live against an empty `~/.exapump/config.toml`, where `exapump bucketfs ls` fails with `Error: No profiles found in config`.
* BucketFS paths given to `exapump` are **bucket-relative**: no leading slash and no bucket segment (`udf/liblakehouse_engine.so`), because `exapump` builds its URL as `<scheme>://<bfs-host>:<bfs-port>/<bucket>/<path>` and takes the bucket from `--bfs-bucket` or the profile. The bucket name DOES appear in `%udf_object` and in the RUST alias, because those strings are read by the Exasol engine, not by `exapump`.
* On Exasol SaaS the personal access token IS the SQL password, so the REST bearer credential is derived from whichever connectivity mode was chosen rather than asked for a second time. This reasoning is SaaS-specific and does not generalize — see `packaging/install-script-slc-registration` and `packaging/install-script-deploy` for the BucketFS write password, which is only ever checked for presence.
* The repository is private, so no unauthenticated URL works for the script or for a release asset. One bearer token (`--github-token` / `GITHUB_TOKEN`) authenticates every GitHub access, including the fetch of the installer itself; the same token is sent to the public `language-container-rs`, keeping exactly one authenticated code path.
* The RUST `SCRIPT_LANGUAGES` alias carries no SLC version — the version lives in the uploaded tarball's content — so `TARGET_RUST_LANG_SEGMENT` varies by target only, never by version.

## Scenarios

### Scenario: A missing prerequisite tool stops the run before any network or SQL call

* *GIVEN* an invocation whose arguments are otherwise valid
* *AND* `exapump` or `curl` is absent from `PATH`
* *WHEN* the installer runs
* *THEN* it SHALL exit non-zero naming the missing tool and an install URL for it
* *AND* it SHALL make no GitHub call, no SaaS REST call, no `exapump bucketfs` call and no SQL statement
* *AND* when the resolved target mode is `bucketfs`, `tar` SHALL additionally be required, because that target extracts `liblakehouse_engine.so` out of the release archive locally before uploading it
* *AND* a SaaS-target run on a machine with no `tar` SHALL still succeed, because the SaaS target uploads the archive unextracted

### Scenario: The BucketFS target requires an existing exapump profile in every connectivity mode

* *GIVEN* a BucketFS-target invocation
* *AND* the exapump config (`${EXAPUMP_CONFIG:-$HOME/.exapump/config.toml}`) contains no profile at all
* *WHEN* the installer reaches its BucketFS reachability preflight
* *THEN* the run SHALL fail, because `exapump bucketfs` itself refuses with `Error: No profiles found in config`
* *AND* this SHALL hold even in `--dsn` or `--host` connectivity mode with `--bfs-host`, `--bfs-port`, `--bfs-bucket` and `--bfs-write-password` all supplied explicitly — the requirement belongs to `exapump bucketfs`, which accepts no DSN or user/password flags of its own
* *AND* the documented prerequisites SHALL state this as a requirement of the BucketFS target rather than as an edge case

### Scenario: A missing GitHub token stops the run before the first network call

* *GIVEN* an invocation with neither `--github-token` nor `GITHUB_TOKEN` set
* *WHEN* the installer validates its required inputs
* *THEN* it SHALL exit non-zero naming both `--github-token` and `GITHUB_TOKEN`, and SHALL state that the token needs read access to the private `exasol-labs/lakehouse-engine-rs` repository
* *AND* it SHALL make no network call before failing, because a token is required by both targets and by every subsequent GitHub access

### Scenario: Exactly one connectivity mode is required

* *GIVEN* an invocation for either target
* *WHEN* the installer resolves connectivity
* *THEN* exactly one of `--profile`, `--dsn`/`EXAPUMP_DSN`, or the `--host`/`--user`/`--password` triple SHALL be supplied
* *AND* zero modes or more than one mode SHALL exit non-zero with a message naming all three, before any network call
* *AND* the resolved mode SHALL be recorded once in `CONNECTIVITY_MODE` and consulted by every later step, so no step re-derives it

### Scenario: Host connectivity requires a port and percent-encodes the assembled DSN

* *GIVEN* an invocation using `--host`/`--user`/`--password`
* *WHEN* the installer assembles the exapump DSN
* *THEN* all three flags SHALL be required, and `--host` SHALL be rejected unless it carries a port (`myhost:8563`), with the message stating that there is no separate `--port` flag
* *AND* the user and password SHALL be percent-encoded to the RFC 3986 unreserved set before interpolation, so a reserved character (`@`, `:`, `/`, `?`, `#`) in either cannot corrupt or re-point the `exasol://` DSN
* *AND* the assembled DSN SHALL carry `validateservercertificate=0`, so a self-signed container certificate is accepted

### Scenario: The install target is auto-detected from the SaaS id pair

* *GIVEN* an invocation for either target
* *WHEN* `resolve_target_mode` runs, before any other validation
* *THEN* supplying **both** `--account-id` and `--database-id` SHALL resolve the target mode to `saas`
* *AND* supplying **neither** SHALL resolve it to `bucketfs`, the default target covering Exasol AsApp, Docker and on-premise
* *AND* supplying **exactly one** of the two SHALL exit non-zero, naming both flags and the Exasol SaaS web console as where to find them — a partial pair SHALL NEVER silently resolve to `bucketfs`
* *AND* the mode SHALL be resolved FIRST, before the required-input and connectivity checks, because it doubles as the SaaS-id validation and every later step branches on it

### Scenario: `--target` asserts the detected mode and never selects it

* *GIVEN* an invocation that also passes `--target <saas|bucketfs>`
* *WHEN* `resolve_target_mode` runs
* *THEN* a value other than `saas` or `bucketfs` SHALL be rejected, listing the two valid values
* *AND* a value that AGREES with the detected mode SHALL pass through, leaving the detected mode unchanged
* *AND* a value that DISAGREES SHALL exit non-zero, naming both the flag value and the detected mode, and stating which flags to add or drop to reach the asserted target
* *AND* `--target` SHALL NEVER select the mode, so the SaaS ids remain the single source of truth and the two can never disagree silently

### Scenario: The target layout resolves to the mode's paths

* *GIVEN* a resolved target mode
* *WHEN* `resolve_target_layout` runs
* *THEN* in `saas` mode `TARGET_SO_UDF_OBJECT` SHALL be `/buckets/uploads/default/lakehouse-engine/udf/liblakehouse_engine.so`, `TARGET_RUST_LANG_SEGMENT` SHALL be the `uploads/default/rustslc` RUST alias, and both BucketFS path globals SHALL be empty, because SaaS addresses an upload by its files-API key instead of by a path
* *AND* in `bucketfs` mode `TARGET_SO_UDF_OBJECT` SHALL be `buckets/bfsdefault/<bucket>/udf/liblakehouse_engine.so`, `TARGET_RUST_LANG_SEGMENT` SHALL be the matching `bfsdefault/<bucket>/slc/lakehouse-rustslc` alias, `TARGET_SLC_BFS_PATH` SHALL be `slc/lakehouse-rustslc.tar.gz` and `TARGET_ENGINE_BFS_PATH` SHALL be `udf/liblakehouse_engine.so`
* *AND* the two BucketFS upload paths SHALL be bucket-relative — no leading slash, no bucket segment — while the bucket name SHALL appear in `TARGET_SO_UDF_OBJECT` and in BOTH halves of `TARGET_RUST_LANG_SEGMENT`
* *AND* every later install step SHALL read these globals rather than a target-specific constant

### Scenario: The BucketFS bucket is resolved from the profile before the target layout is built

* *GIVEN* a `bucketfs` target run in `profile` connectivity mode
* *AND* the named profile sets a non-default `bfs_bucket`
* *AND* `--bfs-bucket` was NOT given explicitly
* *WHEN* the installer resolves its target layout
* *THEN* `resolve_bfs_bucket_from_profile` SHALL run FIRST and adopt the profile's `bfs_bucket`, so `TARGET_SO_UDF_OBJECT` and `TARGET_RUST_LANG_SEGMENT` name the same bucket `exapump` will actually upload into
* *AND* the upload destination and the DDL's `%udf_object` SHALL therefore always name the same bucket — before this resolution the run passed every upload and verification step, because they all target the bucket `exapump` picks, while the DDL pointed at `default`, leaving Exasol looking for the `.so` in a bucket it was never uploaded to: a silent split-brain discoverable only at first query
* *AND* an explicitly given `--bfs-bucket` SHALL always win over the profile's value
* *AND* the resolution SHALL be a no-op in `saas` mode, in `dsn` or `host` connectivity mode (no profile to read), and when the profile carries no `bfs_bucket` key
* *AND* only an explicitly supplied `--bfs-bucket` SHALL be forwarded to `exapump`; an unsupplied default SHALL NOT be echoed back, so `exapump`'s own profile resolution stays authoritative

### Scenario: BucketFS-target required fields are validated before any network call

* *GIVEN* a `bucketfs` target run
* *WHEN* the installer validates its BucketFS inputs
* *THEN* in `profile` connectivity mode the BucketFS write password SHALL be required to be OBTAINABLE — supplied as `--bfs-write-password`, or present as `bfs_write_password` in the named profile — and the failure message SHALL name the flag, the config key, the profile section and the config file path
* *AND* in `dsn` or `host` connectivity mode BOTH `--bfs-host` and `--bfs-write-password` SHALL be required explicitly, with the message stating that `exapump bucketfs` accepts no DSN or user/password flags
* *AND* the installer SHALL only establish that the write password is present; it SHALL NOT read, print, store or re-derive its value, because the secret's owner is `exapump`'s own configuration
* *AND* this validation SHALL run before the reachability preflight and before any download, so a missing password never costs a transfer

### Scenario: The SaaS REST credential is derived from the connectivity credential, never asked for twice

* *GIVEN* a `saas` target run
* *WHEN* the installer resolves its REST bearer credential
* *THEN* it SHALL derive `RESOLVED_PAT` from the chosen connectivity mode — `--password` in host mode, the DSN's percent-decoded password segment in dsn mode, or the named profile's `password` key read from `${EXAPUMP_CONFIG:-$HOME/.exapump/config.toml}` in profile mode — because on Exasol SaaS the PAT IS the SQL password
* *AND* there SHALL be no `--pat` or `EXASOL_PAT` input
* *AND* a derivation that fails SHALL error immediately, naming the mode and what was missing, and SHALL NOT fall through to another mode and SHALL NOT proceed with an empty value
* *AND* no error path and no success path SHALL ever print the derived value
* *AND* the profile TOML read SHALL be a bounded scan of the named `[profile]` section only, ending at the next `[`-headed section, so a same-named key in a different section can never be returned

### Scenario: Reachability is confirmed per target before the first download

* *GIVEN* a validated invocation
* *WHEN* the installer performs its preflight
* *THEN* a `saas` run SHALL GET the database through the SaaS control plane and, on failure, name the account id, the database id, the PAT and `--staging` as the things to verify
* *AND* a `bucketfs` run SHALL perform an empty-path `exapump bucketfs ls` against the resolved bucket and, on failure, name the bucket, `--bfs-host`, `--bfs-port` and the write password, and SHALL surface `exapump`'s own diagnostic verbatim
* *AND* both SHALL complete before the temporary working directory is created and before any release asset is downloaded

### Scenario: Artifact versions resolve to the pinned value or to the latest release

* *GIVEN* an invocation with or without `--lakehouse-version` / `--slc-version`
* *WHEN* the installer resolves versions
* *THEN* a supplied version SHALL be used directly, normalized so that both `1.2.3` and `v1.2.3` are accepted, and the corresponding `releases/latest` call SHALL be skipped entirely
* *AND* an unsupplied version SHALL be resolved from that repository's `GET /repos/<repo>/releases/latest`, authenticated with the bearer token, reading `tag_name`
* *AND* a failure to resolve or to parse SHALL exit non-zero naming the repository and, for the private engine repo, stating that the token needs read access to it
* *AND* both resolved versions SHALL be printed to stdout as user-facing output, including when `--skip-slc` was given, so the operator can see which SLC version the database already needs to carry

### Scenario: The SaaS control-plane base is production unless `--staging` is given

* *GIVEN* a `saas` target run
* *WHEN* the installer builds a control-plane URL
* *THEN* the base SHALL be `https://cloud.exasol.com` by default
* *AND* `--staging` SHALL select `https://cloud-staging.exasol.com`
* *AND* a default run SHALL never contact the staging host

### Scenario: The installer is distributed through the authenticated GitHub contents API on `main`

* *GIVEN* an operator installing with the documented one-liner
* *WHEN* they fetch the script
* *THEN* the documented URL SHALL be the authenticated GitHub **contents API** for `deploy/scripts/install.sh` on `main`, sent with `Authorization: Bearer $GITHUB_TOKEN` and `Accept: application/vnd.github.raw`, and piped into `bash -s -- <flags>`
* *AND* the installer SHALL NOT be published as a release asset, because a release asset would freeze the installer at its release-time state and ship an installer bug forever inside every past release
* *AND* a plain `raw.githubusercontent.com` or `releases/download/...` URL SHALL NOT be documented, because neither works against a private repository
* *AND* a run SHALL be pinnable in two independent dimensions: `?ref=<tag>` appended to the same contents-API URL pins the SCRIPT, while `--lakehouse-version` and `--slc-version` pin the ARTIFACTS; the documentation SHALL present them together, because pinning only one gives a mismatched pair
* *AND* because `main` is therefore a live distribution channel with no release gate before a user's next fetch, the install-script CI jobs SHALL be required checks AND SHALL block the `release` job

### Scenario: `--help` describes both targets and makes no call

* *GIVEN* an invocation with `--help` or `-h`
* *WHEN* the installer runs
* *THEN* it SHALL exit 0 after printing usage that names the script as `install.sh`, states the auto-detection rule for both targets, and lists the connectivity flags, the shared flags, the SaaS-only flags and the BucketFS-only flags in separate sections
* *AND* the usage SHALL state that the SaaS REST credential is derived automatically and has no flag, and that `exapump bucketfs` accepts no DSN or user/password flags
* *AND* it SHALL give one example invocation per target
* *AND* it SHALL make no network call, no SQL call, and SHALL require no credential

### Scenario: A flag belonging to the other target is rejected, not silently ignored

* *GIVEN* an invocation carrying a flag its resolved target does not use — `--staging` with neither `--account-id` nor `--database-id` given (BucketFS detected), or any of `--bfs-host`/`--bfs-port`/`--bfs-bucket`/`--bfs-write-password` given alongside both `--account-id` and `--database-id` (SaaS detected)
* *WHEN* the installer runs
* *THEN* `resolve_target_mode` SHALL fail before any network call, naming the conflicting flag(s) and the detected mode, rather than parsing the flag and silently proceeding
* *AND* an untouched `--bfs-bucket` default (no `--bfs-bucket` given) SHALL NOT be treated as a conflict on a SaaS run — only a flag the caller actually supplied counts
* *AND* the full set of targeting conflicts the installer SHALL detect is: a `--target` value disagreeing with the detected mode, a partial SaaS id pair, `--staging` without both SaaS ids, and any `--bfs-*` flag given together with both SaaS ids — closing what was tracked as a named limitation during initial implementation, when a cross-mode flag was briefly accepted and ignored

## Limitations

* **A BucketFS write password containing whitespace is unsupported** when passed as `--bfs-write-password`, because the `--bfs-*` override list is deliberately word-split by its caller. The supported route for such a password is the profile's `bfs_write_password` key, which the installer never reads.
* **Coupling to `exapump`'s configuration shape.** Profile-mode SaaS PAT derivation and profile-mode `bfs_bucket` / `bfs_write_password` resolution all parse `${EXAPUMP_CONFIG:-$HOME/.exapump/config.toml}` directly, assuming flat `[profile]` sections with quoted scalar values. If `exapump` changes that location or format, these three reads must change with it.
