# Tasks: add-unified-install-script

> **Retrospective record.** Implementation preceded this plan; every task below was completed
> before the plan and its spec deltas were written. Each group names the commit that carried it.

## Phase 2: Implementation (Group A — port the SaaS installer, `9c9ef89`)
- [x] 1.1 Port PR #141's `install-saas.sh` to `deploy/scripts/install.sh`, keeping its credential
      derivation, no-`jq` JSON scanning (`extract_json_string_field`, `json_unescape`,
      `extract_asset_id_by_name`), `read_profile_key` TOML scan, presigned-URL upload, three-script
      DDL, fingerprint smoke test, and next-step template.
- [x] 1.2 Keep the file sourceable: `main` runs only when executed or piped
      (`BASH_SOURCE[0]` guard), so pure functions are unit-testable without an install.
- [x] 1.3 Port the test harness to `deploy/scripts/tests/install.test.sh`; add `make test-install`
      and `make lint-install`.

## Phase 2: Implementation (Group B — parameterize for a target mode, `9497bfc`)
- [x] 2.1 Add `resolve_target_mode`: auto-detect from the SaaS id pair (both ⇒ `saas`, neither ⇒
      `bucketfs`, exactly one ⇒ hard error naming both flags); `--target` validates its value and
      asserts against the detected mode, never selects it.
- [x] 2.2 Add `resolve_target_layout` seeding `TARGET_SO_UDF_OBJECT`, `TARGET_RUST_LANG_SEGMENT`,
      `TARGET_SLC_BFS_PATH`, `TARGET_ENGINE_BFS_PATH`; move every direct read of a SaaS-specific
      constant behind a `TARGET_*` global.
- [x] 2.3 Add `upload_artifact` as the single mode-branching I/O dispatcher (SaaS addresses by
      files-API key, BucketFS by bucket-relative path; each ignores the other's argument).
- [x] 2.4 Generalize `compute_script_languages(current, segment)` to take the segment as a
      parameter instead of appending a fixed SaaS literal.
- [x] 2.5 Resolve the target mode FIRST in `main`, before `validate_required` — it doubles as the
      SaaS-id validation, and every later step branches on it.
- [x] 2.6 Tests: `test_resolve_target_mode_partial_saas_ids`, `test_resolve_target_layout_saas_values`.

## Phase 2: Implementation (Group C — the BucketFS target, `d1964c5`)
- [x] 3.1 Add `exapump_bfs_flags` (emit only the `--bfs-*` overrides actually supplied) and
      `exapump_bucketfs` (append the connectivity flag, disable globbing around the deliberate
      word-split, `</dev/null`).
- [x] 3.2 Add `bucketfs_reachable` (empty-path `ls` preflight, analogous to `saas_db_reachable`).
- [x] 3.3 Add `bucketfs_upload_file` (always `exapump bucketfs cp`, never a raw HTTP PUT) and
      `bucketfs_verify_listed` (whole-listing-entry match, not a substring test). [expert]
- [x] 3.4 Add `bucketfs_wait_for_path`: bounded retry around the listing, because BucketFS unpacks
      an uploaded `.tar.gz` asynchronously.
- [x] 3.5 Add `extract_engine_so`: local `tar -xzf`, failing by name when
      `udf/liblakehouse_engine.so` is absent or empty; wire the BucketFS arm of `install_engine` to
      upload the BARE `.so` while SaaS keeps uploading the tarball.
- [x] 3.6 Add `validate_bucketfs_required`: presence-only check of the write password (profile
      mode), `--bfs-host` + `--bfs-write-password` mandatory in dsn/host mode; runs before any
      network call.
- [x] 3.7 Make `tar` a BucketFS-only prerequisite in `check_prereqs`.
- [x] 3.8 Confirm the bucket-relative path grammar against a live container (`cp f /default/x/f`
      lands at `default/x/f` INSIDE the bucket; `ls /default` is "Path not found") and record it in
      the constants block; the bucket appears only in `%udf_object` / the RUST alias, which the
      Exasol engine reads, not `exapump`.
- [x] 3.9 Tests: `test_resolve_target_mode_bucketfs_autodetect`, `test_target_flag_conflict_detection`,
      `test_resolve_target_layout_bucketfs_values`, `test_exapump_bfs_flags`,
      `test_bucketfs_upload_argv_shape`, `test_bucketfs_upload_failure_surfaces_stderr`,
      `test_bucketfs_verify_listed_and_wait`, `test_bucketfs_reachable_preflight`,
      `test_validate_bucketfs_required_before_any_call`, `test_extract_engine_so`,
      `test_bucketfs_full_run_artifact_shapes`, `test_saas_run_never_touches_bucketfs`,
      `test_tar_required_only_in_bucketfs_mode`. Stub `exapump bucketfs` as a state-file-backed fake
      bucket so `cp`/`ls`/async-unpack delay are all exercised.

## Phase 2: Implementation (Group D — `--skip-slc` and mode-aware usage, `5a128d2`)
- [x] 4.1 Add `--skip-slc`: skip the SLC download, upload and `ALTER SYSTEM`, log the reason, still
      resolve and report the SLC version, leave engine install + DDL + smoke test untouched.
- [x] 4.2 Rewrite `usage()` for both target modes: the auto-detection rule, per-mode flag sections,
      the "`exapump bucketfs` takes no DSN" note, and one example per target.
- [x] 4.3 Tests: `test_skip_slc_gating` (both modes), `test_usage_is_mode_aware`.

## Phase 2: Implementation (Group E — bucket-drift fix, `13ef42a`)
- [x] 5.1 Add `resolve_bfs_bucket_from_profile`, called from `main` BEFORE `resolve_target_layout`:
      in bucketfs + profile mode with no explicit `--bfs-bucket`, adopt the profile's `bfs_bucket`.
      Fixes a silent split-brain where `exapump` uploaded into the profile's bucket while the DDL's
      `%udf_object` / RUST alias pointed at `default` — every step reported success and the install
      was broken, discoverable only at first query. [expert]
- [x] 5.2 Add `ARG_BFS_BUCKET_SET` so an unsupplied default is never echoed back to `exapump` while
      an explicit `--bfs-bucket` always wins over the profile.
- [x] 5.3 Test `test_resolve_bfs_bucket_from_profile`, including the end-to-end assertion that
      `resolve_target_layout` then builds `%udf_object` in the ADOPTED bucket, plus the four no-op
      cases (explicit flag, no `bfs_bucket` key, saas mode, dsn mode).

## Phase 2: Implementation (Group F — CI, `3ce40a9`)
- [x] 6.1 Add the `install-script` job: ShellCheck over both files + `make test-install` (stubbed,
      no network).
- [x] 6.2 Add the `install-script-e2e` job: real BucketFS install against a live `exasol` compose
      service — create an `exapump` profile from the container's own `EXAConf` write password, then
      run `bash deploy/scripts/install.sh --profile ci-bucketfs` with NO version pin, so it resolves
      a real prior release exactly as a user's default invocation does. [expert]
- [x] 6.3 Make both jobs block `release` (`release.needs`), because `main` is a live distribution
      channel with no release gate in front of a user's `curl | bash`; record the reason at the job.
- [x] 6.4 Record in the workflow that SaaS mode has NO CI job — it needs a real tenant and PAT that
      CI does not have — as a named tracked exception against #252, not a silent gap.

## Phase 3: Verification (Group G — docs and live run, `9bf6d84`)
- [x] 7.1 Rewrite `docs/install.md` around the one-line command: prerequisites (including the
      "at least one `exapump` profile must exist, in every connectivity mode" requirement for
      BucketFS targets), one command per target, what the command does, the full flag table,
      `?ref=<tag>` + `--lakehouse-version` / `--slc-version` pinning, `--skip-slc`, and the
      next-step VS setup. Keep build-from-source and the fully-manual network-restricted path as
      appendices. Update `docs/index.md` and `README.md` to match.
- [x] 7.2 Run the checklist: `shellcheck` clean on both files; `make test-install` all green.
- [x] 7.3 Live-verify the BucketFS target against this repo's own `docker-compose.yml` Exasol
      container: source the script and drive `resolve_target_layout` → `bucketfs_reachable` →
      `extract_engine_so` → `upload_artifact` → `bucketfs_wait_for_path` → `create_engine_scripts`
      → `run_smoke_test` with the real built `.so`, real `exapump bucketfs cp` and real DDL. Result:
      a genuine fingerprint-MATCH smoke-test pass, not merely "some other error, therefore pass".
      The container already had a matching RUST SLC registered from prior work, so `register_slc`
      was not driven by this run — the `install-script-e2e` job's unpinned full run is what
      exercises SLC upload, BucketFS auto-extraction and `ALTER SYSTEM` end to end.

## Phase 4: Specification (Group H — this plan)
- [x] 8.1 Author `plan.md` and `decision-log.md` retrospectively from the shipped script and its
      tests, carrying PR #141's three ADRs forward: 008 generalized (fixed literal → mode-resolved
      segment), 009 verbatim (and now also load-bearing for script distribution), 010 verbatim but
      explicitly scoped to SaaS mode and paired with the contrasting BucketFS-write-password
      decision.
- [x] 8.2 Author the three spec deltas: `packaging/install-script-targeting`,
      `packaging/install-script-slc-registration`, `packaging/install-script-deploy` — named without
      the old branch's `saas-` prefix, since the feature is no longer SaaS-specific.
- [x] 8.3 Name every known limitation in a spec rather than leaving it implicit: no SaaS CI job
      (#252); `ALTER SYSTEM` under SaaS default privileges never confirmed.
