# Plan: add-unified-install-script

> **Retrospective plan.** The implementation landed first, across seven commits on
> `feat/add-unified-install-script` (`9c9ef89`, `9497bfc`, `d1964c5`, `5a128d2`, `13ef42a`,
> `3ce40a9`, `9bf6d84`). This plan and its spec deltas were authored afterwards, from the
> shipped `deploy/scripts/install.sh` and `deploy/scripts/tests/install.test.sh` as the source
> of truth, so the specs record what the code actually does rather than what was intended. The
> Implementation Tasks section is therefore a record, not a forward plan; every task is already
> checked off in `tasks.md`. Tracked in issue
> [#252](https://github.com/exasol-labs/lakehouse-engine-rs/issues/252).

## Summary

Add `deploy/scripts/install.sh`: one `curl | bash` installer that provisions `lakehouse-engine`
onto **any** Exasol deployment — Exasol SaaS, or Exasol AsApp / Docker / on-premise (all three
reached through BucketFS via `exapump`). The script auto-detects its install target from the
flags it was given, registers the Rust SLC (on by default, `--skip-slc` opts out), uploads the
engine `.so`, creates the three `CREATE SCRIPT` objects in the deployment schema, and runs a
fingerprint smoke test. It never builds from source: it always downloads a prebuilt release
asset through the authenticated GitHub REST API, because this repository is private. It stops at
a query-ready product install and prints a `CONNECTION` / `CREATE VIRTUAL SCHEMA` template; it
creates no dataset-specific catalog object.

Three new features under `specs/packaging/`: `install-script-targeting` (what the script decides
before it touches the network), `install-script-slc-registration` (the Rust SLC step), and
`install-script-deploy` (the engine artifact, the DDL, and the smoke test).

## Context

`lakehouse-engine` had no product install path. `docs/install.md` documented a build-from-source
route (`make cross-musl-udf-build` + `make bucketfs-upload-so`) and a fully manual route (download
the release tarball, upload it through a UI or a raw HTTP PUT, then hand-write the DDL). Both
require the operator to know the BucketFS path layout, the RUST `SCRIPT_LANGUAGES` alias grammar,
and the exact `%udf_object` string — three places where a silent typo produces a load failure at
first query rather than at install time.

An earlier attempt exists: PR [#141](https://github.com/exasol-labs/lakehouse-engine-rs/pull/141)
on `origin/feat/add-saas-install-script` (closing issue #140) shipped a **SaaS-only**
`deploy/scripts/install-saas.sh`, reviewed but never merged. It carried three ADRs on that branch:
`008-add-saas-install-script.md` (idempotent `SCRIPT_LANGUAGES` read-modify-write),
`009-change-saas-install-github-token.md` (`GITHUB_TOKEN` + `curl` replacing the `gh` CLI), and
`010-fix-saas-install-pat-password-redundancy.md` (derive the SaaS PAT from the connectivity
credential, drop `--pat`). That branch was too stale to rebase — `main`'s `specs/` tree was
reorganized underneath it — so the work was **ported and generalized** onto a fresh branch instead.
This plan supersedes PR #141: all three of its ADRs are carried forward here, two of them re-scoped
(see `decision-log.md` decisions [2], [3], and [5]).

The generalization is the substance of this plan. SaaS reaches its bucket through a control-plane
REST API and a presigned-URL exchange; Exasol AsApp, Docker, and on-premise all reach theirs through the
raw BucketFS HTTP interface, which `exapump bucketfs` already speaks. Those are two genuinely
different upload channels with two different path layouts, but everything either side of the
upload — prerequisites, connectivity, version resolution, `SCRIPT_LANGUAGES`, the DDL, the smoke
test, the next-step template — is identical.

- **Goals** — one command, one script, both targets; correct on a re-run; fails before it spends a
  byte of download when it is misconfigured; never prints a credential; safe when piped into `bash`
  over stdin; verified continuously in CI against a real Exasol for the BucketFS target.
- **Non-Goals** — building the `.so` from source (always a prebuilt release asset); creating any
  dataset-specific catalog object (`CONNECTION`, `VIRTUAL SCHEMA`); uninstalling; a Windows/PowerShell
  variant; installing `exapump` or `curl` on the operator's behalf.

## Design

### Context

Four forces shape the script.

1. **Two upload channels, one everything-else.** The SaaS files API and `exapump bucketfs` share no
   verb, no path grammar, and no credential. Every other step is target-independent.
2. **The repository is private.** No unauthenticated URL works — not for the script itself, not for
   the release asset. Every GitHub access must carry a bearer token.
3. **It is piped into `bash` over stdin.** The script body arrives on the shell's stdin. Any
   subprocess that inherits and reads that stdin truncates the not-yet-parsed remainder of the
   script.
4. **`exapump bucketfs` is not `exapump sql`.** It takes no `--dsn`, no `--user`, no `--password`;
   it reads its connection from a named profile plus `--bfs-*` overrides, and it requires at least
   one profile to exist in `~/.exapump/config.toml` **even when every `--bfs-*` flag is supplied
   explicitly** (verified live: with an empty config, `exapump bucketfs ls` fails with
   `Error: No profiles found in config`).

### Decision

One sourceable bash script with a **mode-parameterized core**. The target mode is resolved first,
from the flags themselves; it seeds four `TARGET_*` globals that every later step reads instead of
reading a target-specific constant directly; and exactly one function — `upload_artifact` — branches
on it for I/O. `main` runs only when the file is executed or piped (`BASH_SOURCE[0]` guard), so the
whole function set is sourceable and unit-testable without running an install.

#### Architecture

```
  flags ─▶ parse_args
             │
             ▼
        resolve_target_mode ──────────── saas   ⇐ BOTH --account-id and --database-id
             │                            bucketfs ⇐ NEITHER  (default target)
             │                            exactly one ⇒ hard error
             │                            --target is an ASSERTION, never a selector
             ▼
        validate_required (GITHUB_TOKEN)
        validate_connectivity ────────── profile | dsn | host   (exactly one)
        resolve_bfs_bucket_from_profile   ← adopts the profile's bfs_bucket (bucket-drift fix)
        resolve_target_layout ─────────▶ TARGET_SO_UDF_OBJECT
                                         TARGET_RUST_LANG_SEGMENT
                                         TARGET_SLC_BFS_PATH     (bucketfs only)
                                         TARGET_ENGINE_BFS_PATH  (bucketfs only)
        check_prereqs (exapump, curl; + tar only in bucketfs mode)
             │
             ├─ saas     ─▶ resolve_saas_pat ─▶ saas_db_reachable
             └─ bucketfs ─▶ validate_bucketfs_required ─▶ bucketfs_reachable
             │
             ▼   ── everything above happens BEFORE the first byte is downloaded ──
        mktemp -d WORKDIR (trap rm -rf on EXIT)
        resolve_versions ───────────────  latest via /releases/latest, or the pinned flag
             │
        register_slc  (unless --skip-slc)
             │  download_slc → upload_artifact(tarball) → [bucketfs: wait_for_path]
             │  read_script_languages → compute_script_languages → ALTER SYSTEM SET
             ▼
        install_engine
             │  download_engine
             │  saas:     upload_artifact(TARBALL, files-API key)      ← SaaS bucket auto-extracts
             │  bucketfs: extract_engine_so → upload_artifact(BARE .so) → wait_for_path
             │  create_engine_scripts  (SCHEMA + ADAPTER + SCAN + DISTRIBUTE_FILES)
             ▼
        run_smoke_test ── classify_fingerprint_response: mismatch|anomaly ⇒ fail, else pass
             ▼
        print_next_step_template   ── stops here; creates no catalog object
```

#### Patterns

| Pattern | Where | Why |
|---------|-------|-----|
| Mode resolved first, then four `TARGET_*` globals | `resolve_target_mode` → `resolve_target_layout` | Every later step reads a resolved global, never a target-specific constant, so a new target is a new `case` arm rather than a new `if` in ten places |
| Exactly one I/O branch on `TARGET_MODE` | `upload_artifact` | The two upload channels are the only genuine difference; funnelling them through one dispatcher keeps the difference auditable |
| Validate and preflight before the first download | `main`'s ordering | A misconfigured run costs zero bytes and produces one clear error, not a 401 halfway through a transfer |
| `</dev/null` on **every** subprocess | every `curl` / `exapump` invocation | The script body is on stdin when piped into `bash`; one missing redirect truncates the remaining script. Proven per-subprocess by a sentinel-payload test, not by inspection |
| Bounded scans instead of `jq` / a TOML parser | `extract_asset_id_by_name`, `read_profile_key` | The stated prerequisite list is bash + curl + exapump. Both scans are bounded to their block (the `assets` array; the named `[profile]` section) so a same-named key elsewhere can never match |
| Read-modify-write, never a blind write | `read_script_languages` → `compute_script_languages` | `ALTER SYSTEM SET SCRIPT_LANGUAGES` replaces the whole value; a blind write drops `PYTHON3`/`JAVA` |
| Whole-token verification, never a substring test | `saas_verify_listed`, `bucketfs_verify_listed` | `liblakehouse_engine.so.bak` must not satisfy a check for `liblakehouse_engine.so` |
| Bounded retry around the BucketFS listing | `bucketfs_wait_for_path` | BucketFS unpacks an uploaded `.tar.gz` asynchronously; a path accepted by the PUT can be absent from the very next listing |
| Presence check, never a value read, for the BucketFS write password | `validate_bucketfs_required` | The password's home is `exapump`'s own config; the installer only needs to know it exists so the failure is early and named |
| Sourceable file, `main` gated on `BASH_SOURCE[0]` | end of `install.sh` | Lets `install.test.sh` unit-test pure functions in isolation and drive the full installer through both a saved-file and a stdin-piped invocation |

### Consequences

| Decision | Alternatives Considered | Rationale |
|----------|------------------------|-----------|
| One script, target auto-detected from flags | Two scripts (`install-saas.sh` + `install-bucketfs.sh`); one script with a required `--target` | The two targets differ only in the upload channel and the path layout; two scripts would duplicate ~85% of the body and drift. A required `--target` adds a flag whose value is already implied by the SaaS ids |
| `--target` is an assertion only | `--target` selects the mode | A selector makes two sources of truth disagree silently; an assertion turns the disagreement into an error naming both |
| BucketFS I/O only through `exapump bucketfs` | A raw `curl -X PUT https://w:pass@host:port/...` | Reuses the workspace's owned CLI and its credential resolution; keeps the write password out of a process argv the installer constructs, and out of the installer's own error paths |
| SaaS uploads the engine TARBALL; BucketFS uploads the BARE `.so` | Upload the tarball in both modes and rely on BucketFS auto-extraction | The bare-`.so` path (`udf/liblakehouse_engine.so`) is the one `make bucketfs-upload-so` and every E2E `%udf_object` already use; keeping it removes any dependency on the auto-extract-into-a-sibling-directory rule for the engine. The SLC still goes up as a tarball in both modes, because the RUST alias points at the extracted directory |
| Distribute via the GitHub contents API on `main`, `?ref=<tag>` to pin | Attach `install.sh` as a release asset | A release asset freezes the installer at its release-time state; an installer bug then ships forever inside every past release. `main` + an optional `?ref=` gives a fixed-by-default installer and explicit pinning when reproducibility matters. Gated by making the two install-script CI jobs block `release` |
| SLC registration on by default, `--skip-slc` to opt out | Off by default, `--install-slc` to opt in | The whole point is one command on a bare database; an opt-in flag makes the default invocation fail its own smoke test. `--skip-slc` covers the already-registered case and the no-`ALTER SYSTEM`-privilege case |
| Adopt the profile's `bfs_bucket` when `--bfs-bucket` was not given | Leave `ARG_BFS_BUCKET` at its `default` fallback | `exapump` resolves the profile's real bucket for the upload while the installer built its DDL paths from `default` — upload and verify both pass, then the DDL points at a bucket the `.so` was never uploaded to. A silent split-brain, fixed by `resolve_bfs_bucket_from_profile` |
| Derive the SaaS PAT from the connectivity credential; never derive the BucketFS write password | Ask for both explicitly; derive both | On SaaS the PAT *is* the SQL password — one secret, asked once. BucketFS's write password is a genuinely different secret with its own `exapump`-owned home, so the installer only checks it is present |
| BucketFS-target E2E in CI; SaaS-mode has no CI job | Mock SaaS in CI; ship without a live install test | The BucketFS target is testable against a real Exasol compose service on every push. SaaS needs a real tenant and a real PAT that CI does not have — recorded as a named tracked exception against #252, not a silent gap |

## Features

| Feature | Status | Spec |
|---------|--------|------|
| packaging/install-script-targeting | NEW | `packaging/install-script-targeting/spec.md` |
| packaging/install-script-slc-registration | NEW | `packaging/install-script-slc-registration/spec.md` |
| packaging/install-script-deploy | NEW | `packaging/install-script-deploy/spec.md` |

The three replace the never-merged `packaging/saas-install-script` feature drafted on
`origin/feat/add-saas-install-script`; the `saas-` name prefix is deliberately dropped, since the
feature is no longer SaaS-specific.

## Dependencies

Runtime prerequisites on the operator's machine, checked by `check_prereqs` before any network
call:

| Dependency | Required in | Note |
|---|---|---|
| `exapump` | both targets | All SQL and all BucketFS I/O |
| `curl` | both targets | All GitHub and all SaaS REST access |
| `tar` | BucketFS target only | Local extraction of the engine `.so` |
| bash 3.2+ | both targets | Stock macOS bash; no `jq`, no associative arrays |
| ≥1 `exapump` profile in `~/.exapump/config.toml` | BucketFS target, **every** connectivity mode | `exapump bucketfs` needs a profile even with all `--bfs-*` flags given |
| A GitHub token with read access to `exasol-labs/lakehouse-engine-rs` | both targets | The repo is private; the same token is sent to the public `language-container-rs` too |

External services: the GitHub REST API (release resolution + asset download), the Exasol SaaS
control plane (SaaS target), the target Exasol database and its BucketFS (BucketFS target).

No change to any Rust crate, to the `.so`, or to the DDL the E2E harness already creates.

## Implementation Tasks

Recorded after the fact; see `tasks.md` for the per-task record.

1. **Port and parameterize** (`9c9ef89`, `9497bfc`)
   1. Port PR #141's SaaS-only `install-saas.sh` to `deploy/scripts/install.sh` verbatim, keeping its
      credential derivation, no-`jq` JSON scanning, presigned-URL upload, DDL, and smoke test.
   2. Introduce `TARGET_MODE`, `resolve_target_mode` (auto-detect from the SaaS id pair, `--target`
      as an assertion), `resolve_target_layout` (the four `TARGET_*` globals), and `upload_artifact`
      as the single I/O dispatcher. Move every direct read of a SaaS-specific constant behind a
      `TARGET_*` global.
2. **BucketFS target** (`d1964c5`)
   1. Add `exapump_bfs_flags`, `exapump_bucketfs`, `bucketfs_reachable`, `bucketfs_upload_file`,
      `bucketfs_verify_listed`, `bucketfs_wait_for_path`, and `extract_engine_so`.
   2. Add `validate_bucketfs_required` (presence-only check of the write password; `--bfs-host` and
      `--bfs-write-password` mandatory outside profile connectivity mode), and make `tar` a
      BucketFS-only prerequisite.
   3. Wire the bucket-relative SLC and engine paths and the `bfsdefault` `%udf_object` / RUST alias.
3. **`--skip-slc` and mode-aware usage** (`5a128d2`)
4. **Bucket-drift fix** (`13ef42a`) — `resolve_bfs_bucket_from_profile`, run before
   `resolve_target_layout`.
5. **CI** (`3ce40a9`) — `install-script` (ShellCheck + `make test-install`) and `install-script-e2e`
   (a real BucketFS install against a live `exasol` compose service, using a real prior release and
   no version pin); both block `release`.
6. **Docs** (`9bf6d84`) — rewrite `docs/install.md` around the one-line command, keeping
   build-from-source and the fully-manual path as appendices.
7. **Spec deltas** (this plan) — authored retrospectively from the shipped script.

## Verification

### Scenario Coverage

Every scenario below is covered by `deploy/scripts/tests/install.test.sh` (plain bash, no
framework), which stubs `exapump` and `curl` as recording fake executables on a temporary `PATH`,
sources the installer's pure functions for unit checks, and drives the full installer through both
`bash install.sh …` and `cat install.sh | bash -s -- …`.

| Scenario | Test Type | Test Location | Test Name |
|----------|-----------|---------------|-----------|
| A missing prerequisite fails before any network or SQL call | Integration (stubbed) | `deploy/scripts/tests/install.test.sh` | `test_missing_prereq_fails_fast` |
| A missing GitHub token fails before any network call | Integration (stubbed) | same | `test_missing_github_token_fails_fast` |
| Exactly one connectivity mode is required | Integration (stubbed) | same | `test_connectivity_mode_either_or` |
| `--host` must carry the port; credentials are percent-encoded into the DSN | Integration (stubbed) | same | `test_host_mode_requires_port`, `test_host_dsn_percent_encodes_credentials`, `test_url_decode_roundtrip`, `test_extract_dsn_password` |
| The SaaS REST credential is derived per connectivity mode and never printed | Unit + Integration | same | `test_read_profile_key`, `test_resolve_saas_pat_per_mode` |
| The target mode is auto-detected; one SaaS id alone is an error | Unit | same | `test_resolve_target_mode_partial_saas_ids`, `test_resolve_target_mode_bucketfs_autodetect` |
| `--target` asserts and never selects | Unit + Integration | same | `test_target_flag_conflict_detection` |
| The target layout resolves per mode, and `--bfs-bucket` propagates into the DDL but not into the upload path | Unit | same | `test_resolve_target_layout_saas_values`, `test_resolve_target_layout_bucketfs_values`, `test_exapump_bfs_flags` |
| A profile's `bfs_bucket` is adopted before the DDL paths are built | Unit | same | `test_resolve_bfs_bucket_from_profile` |
| Versions resolve from `releases/latest` by default and skip that call when pinned | Integration (stubbed) | same | `test_version_resolution_default_and_override` |
| `SCRIPT_LANGUAGES` append / in-place replace / idempotent re-run | Unit | same | `test_script_languages_append_preserves_existing`, `test_script_languages_replace_rust_idempotent` |
| An empty `SCRIPT_LANGUAGES` read hard-fails and never issues `ALTER SYSTEM` | Integration (stubbed) | same | `test_empty_script_languages_read_hard_fails` |
| The SaaS presigned-URL dance, its JSON un-escaping, and its whole-token listing check | Unit + Integration | same | `test_presigned_upload_dance`, `test_presigned_url_json_unescaping`, `test_saas_verify_listed_quoted_match` |
| The release asset is resolved by name and downloaded without leaking the token past the redirect | Unit + Integration | same | `test_release_asset_download_via_rest`, `test_extract_asset_id_by_name_realistic` |
| BucketFS upload argv shape, failure surfacing, whole-entry verification and bounded retry | Unit | same | `test_bucketfs_upload_argv_shape`, `test_bucketfs_upload_failure_surfaces_stderr`, `test_bucketfs_verify_listed_and_wait`, `test_bucketfs_reachable_preflight` |
| BucketFS-required fields are validated before any call | Integration (stubbed) | same | `test_validate_bucketfs_required_before_any_call` |
| The engine `.so` is extracted locally, and a bad archive names the missing member | Unit | same | `test_extract_engine_so` |
| The per-target artifact shapes, and that neither mode touches the other's channel | Integration (stubbed) | same | `test_bucketfs_full_run_artifact_shapes`, `test_saas_run_never_touches_bucketfs`, `test_tar_required_only_in_bucketfs_mode` |
| The three scripts are created with the mode-correct `%udf_object` | Unit + Integration | same | `test_three_scripts_ddl_saas_path_types` |
| The fingerprint smoke test passes on a non-fingerprint error and fails on a mismatch or an anomaly | Integration (stubbed) | same | `test_fingerprint_smoke_pass_and_fail` |
| The installer stops before any catalog object and prints the template | Integration (stubbed) | same | `test_stops_at_product_prints_template` |
| Every external failure is actionable, and no credential is ever printed | Integration (stubbed) | same | `test_external_failure_actionable` |
| A stdin-piped invocation is never truncated, and no subprocess inherits stdin | Integration (stubbed) | same | `test_stdin_piped_invocation_no_body_consumption` |
| `--skip-slc` drops exactly the SLC steps and nothing else | Integration (stubbed) | same | `test_skip_slc_gating` |
| `--help` is mode-aware and makes no call | Integration (stubbed) | same | `test_usage_is_mode_aware` |
| A real BucketFS install against a live Exasol, from a real prior release | E2E (CI) | `.github/workflows/ci.yml` | job `install-script-e2e` |

**Named tracked exception:** SaaS mode has no automated end-to-end coverage. It needs a real SaaS
tenant and a real PAT, which CI does not have; the SaaS path is proven only by the stubbed
integration tests above and by hand-testing against a live tenant. Tracked in issue
[#252](https://github.com/exasol-labs/lakehouse-engine-rs/issues/252); a dedicated follow-up issue
for "SaaS-mode integration test for `install.sh`" should be filed if one does not already exist.
This is a named exception, not a silent gap.

### Manual Testing

| Feature | Command | Expected Output |
|---------|---------|-----------------|
| packaging/install-script-targeting | `bash deploy/scripts/install.sh --help` | Both targets documented; exit 0; no network call |
| packaging/install-script-targeting | `bash deploy/scripts/install.sh --target saas --profile p` | Error naming `--target saas`, the detected mode `'bucketfs'`, and both SaaS ids |
| packaging/install-script-slc-registration | `docker compose up -d exasol` then `bash deploy/scripts/install.sh --profile <docker-profile>` | `Setting SCRIPT_LANGUAGES (RUST segment append/replace).`; `EXA_PARAMETERS` afterwards shows the pre-existing languages plus exactly one `RUST=` entry |
| packaging/install-script-deploy | same run | `Uploaded … to BucketFS path udf/liblakehouse_engine.so`, `Verified BucketFS path …`, `Fingerprint smoke test passed`, then the `CREATE VIRTUAL SCHEMA` template |
| packaging/install-script-deploy | re-run the same command | Same result, no duplicate language entry, no error — the install is idempotent |

Live-verified this way against this repo's own `docker-compose.yml` Exasol container: sourced the
script and drove `resolve_target_layout` → `bucketfs_reachable` → `extract_engine_so` →
`upload_artifact` → `bucketfs_wait_for_path` → `create_engine_scripts` → `run_smoke_test` against
the real container with the real built `.so`, real `exapump bucketfs cp`, and real DDL, reaching a
genuine fingerprint-match smoke-test pass.

### Checklist

| Step | Command | Expected |
|------|---------|----------|
| Lint (shell) | `shellcheck -s bash deploy/scripts/install.sh deploy/scripts/tests/install.test.sh` | 0 findings |
| Test (installer) | `make test-install` | `RESULT: <n> passed, 0 failed` |
| Test (unit) | `cargo test` | 0 failures |
| Lint | `cargo clippy --all-targets` | 0 errors/warnings |
| Format | `cargo fmt` | No changes |
| CI | `install-script` and `install-script-e2e` | Both green; both block `release` |
| Spec validate | `speq plan validate add-unified-install-script` | Pass |
