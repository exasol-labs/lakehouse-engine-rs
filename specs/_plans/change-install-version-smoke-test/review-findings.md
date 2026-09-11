# Code Review Findings: change-install-version-smoke-test

## Summary
- Files reviewed: 4
- Total findings: 5 (standard: 4, expert: 1)

Verification run during review (evidence, not findings):
- `make test-install` → `RESULT: 519 passed, 0 failed`
- `shellcheck -s bash deploy/scripts/install.sh deploy/scripts/tests/install.test.sh` (v0.10.0 static binary, since the host has none) → exit 0, no findings
- `git diff HEAD -- crates/lakehouse-engine/Cargo.toml` → empty; `version = "0.45.0"` unchanged, as the plan's Impact section requires
- `RETURNS VARCHAR(100)` in `ddl_version` and `docs/install.md` matches `crates/lakehouse-engine/tests/common/e2e_harness.rs:180-183`, so the installer, the docs and the E2E harness agree on the version script's DDL
- `print_next_step_template` emits only `GRANT ACCESS ON CONNECTION ... FOR SCRIPT` lines for `LAKEHOUSE_ADAPTER` and `LAKEHOUSE_SCAN`, so the spec's "no CONNECTION grant for the version script" clause holds and its assertion targets the real grant shape
- No markdown link in `docs/` targets the renamed `#appendix-fingerprint-smoke-test-by-hand` anchor, so the rename breaks no cross-reference

## Standard fixes

### deploy/scripts/install.sh

#### [SWALLOWED_ERROR] An unrecognized smoke-test verdict reports the install as successful
- Location: lines 1450-1464 (`run_smoke_test`, the `case "$verdict" in ... esac`)
- Issue: the `case` has arms for `fingerprint-mismatch`, `version-mismatch`, `other-error` and `pass` but no `*)` default arm. A `case` that matches nothing evaluates to status 0, and `esac` is the last statement in `run_smoke_test`, so an empty or unexpected `$verdict` makes the function return 0. `main` then treats the install as verified (`run_smoke_test || exit 1`, line 1571) and prints the query-ready next-step template. `$verdict` is empty whenever the `classify_version_smoke` command substitution produces no output — the script runs without `set -e`, so such a failure is silent. Every other dispatch `case` in this file already guards against this: `run_sql` (line 1168) and `upload_artifact` (line 1158) both end with `*) err "internal error: ..."; return 1`.
- Fix: In `deploy/scripts/install.sh`, add a final arm to the `case "$verdict"` statement in `run_smoke_test`, immediately after the `pass)` arm and before `esac`: `*) err "internal error: unrecognized version smoke test verdict '$verdict'"; return 1 ;;`. Match the wording style of the existing `internal error:` arms in `run_sql` and `upload_artifact`. Then add a case to `test_version_smoke_pass_and_fail` in `deploy/scripts/tests/install.test.sh` that proves an unclassifiable result aborts: source the installer, call `run_smoke_test` with a stubbed `classify_version_smoke` that prints nothing, and assert a non-zero return plus the `internal error` text.

#### [TOO_MANY_ARGUMENTS] `classify_version_smoke` takes four parameters
- Location: lines 1427-1428 (`classify_version_smoke() { local rc="$1" output="$2" reported="$3" expected="$4"`), called at line 1450
- Issue: the function takes four positional parameters, above the three-argument guardrail. The fourth exists only because the caller extracts the reported value itself at line 1449 and hands it back in. That split also creates an undocumented ordering contract: `run_smoke_test` must call `extract_version_value` before `classify_version_smoke`, and nothing in either signature makes that unreachable if skipped. The classifier already receives `$output`, which is the only input `extract_version_value` needs, so the parameter carries no information the function does not already hold.
- Fix: In `deploy/scripts/install.sh`, change `classify_version_smoke` to take three parameters — `local rc="$1" output="$2" expected="$3"` — and replace its `if [[ "$reported" != "$expected" ]]` test with `if [[ "$(extract_version_value "$output")" != "$expected" ]]`. Update the call at line 1450 to `verdict="$(classify_version_smoke "$rc" "$out" "$RESOLVED_ENGINE_VERSION")"`. Keep `run_smoke_test`'s own `reported="$(extract_version_value "$out")"` local, which the `version-mismatch` message still needs. Keep the existing verdict order (fingerprint check, then return code, then version comparison) exactly as it is. Re-run `make test-install` and confirm all five `test_version_smoke_pass_and_fail` cases still pass.

#### [REDUNDANT_COMMENT] Half of `extract_version_value`'s header comment restates its own case arms
- Location: lines 1195-1199
- Issue: the comment runs five lines. Its first two sentences carry the non-obvious WHY (a version value begins with a digit, so the sibling `extract_query_value`'s `[0-9]*` arm cannot be reused). The remainder — "it instead skips the known column header literal for the aliased `LAKEHOUSE_VERSION()` projection and the trailing `N row(s) in set` footer line by its distinctive suffix" — restates the `LAKEHOUSE_ENGINE_VERSION*` and `*' row in set'|*' rows in set'` arms that sit four lines below it, describing what the code does rather than why. CLAUDE.md's comment rule for this repo is no comment unless the WHY is non-obvious.
- Fix: In `deploy/scripts/install.sh`, replace the five-line comment above `extract_version_value` with just the rationale, no restatement of the case arms: `# A version value begins with a digit, so this cannot reuse extract_query_value, whose [0-9]* arm` / `# drops the row-count footer. It skips that footer by suffix instead.` Leave the comment above `classify_version_smoke` (lines 1422-1426) unchanged — it documents a real ordering contract.

### deploy/scripts/tests/install.test.sh

#### [DEAD_FLEXIBILITY] `EXAPUMP_WRONG_VERSION` is an override no test ever sets
- Location: line 168 (`echo "${EXAPUMP_WRONG_VERSION:-9.9.9}"`)
- Issue: `EXAPUMP_WRONG_VERSION` is read in the exapump stub's `version-mismatch` branch and set nowhere — `grep -n 'EXAPUMP_WRONG_VERSION' deploy/scripts/tests/install.test.sh` returns only this read. It is also absent from `reset_env`'s unset list (lines 456-463), so were a test ever to set it, it would leak into every later test. It is an extension point with zero callers.
- Fix: In `deploy/scripts/tests/install.test.sh`, replace `echo "${EXAPUMP_WRONG_VERSION:-9.9.9}"` at line 168 with `echo "9.9.9"`. Leave the paired assertion at line 1030 (`"smoke version-mismatch: names the reported version" ... "9.9.9"`) as it is. Do not touch `EXAPUMP_LAKEHOUSE_VERSION` here — the Expert finding below removes it as part of a larger change.

## Expert fixes

### deploy/scripts/tests/install.test.sh

#### [MAGIC_NUMBER] The stubbed release version `0.26.3` is repeated in three independent places
- Location: line 184 (`echo "${EXAPUMP_LAKEHOUSE_VERSION:-0.26.3}"`), line 248 (`"${GH_ENGINE_TAG:-v0.26.3}"`), line 1029 (`assert_contains ... "0.26.3"`)
- Issue: the stubbed default release version is written out three times with no named constant. The curl stub decides it as a tag (`v0.26.3`), the exapump stub independently repeats it as a bare version (`0.26.3`), and an assertion repeats it a third time. Nothing enforces agreement; the only link is the prose comment at lines 179-180 ("MUST track the curl stub's default `GH_ENGINE_TAG`"). Two consequences follow. First, any test that overrides `GH_ENGINE_TAG` for a full `run_file` run now fails at verification, because the exapump stub keeps reporting `0.26.3` while the installer expects the overridden version — four tests already set `GH_ENGINE_TAG="v1.2.3"` (lines 760, 778, 822, 834) and are safe only because none of them drives a full run. Second, `EXAPUMP_LAKEHOUSE_VERSION` (line 184) exists solely to paper over that coupling and is set by no test, so it is a never-varied override on top of a duplicated constant.
- Fix: In `deploy/scripts/tests/install.test.sh`, introduce one exported harness constant next to the other sandbox constants (near `ENGINE_TARBALL_GOOD`, line 380): `export STUB_DEFAULT_ENGINE_TAG="v0.26.3"`. It must be `export`ed, because the stubs run as separate processes on `RUN_PATH`. In the curl stub, replace `"${GH_ENGINE_TAG:-v0.26.3}"` at line 248 with `"${GH_ENGINE_TAG:-$STUB_DEFAULT_ENGINE_TAG}"`. In the exapump stub's default smoke branch, replace `echo "${EXAPUMP_LAKEHOUSE_VERSION:-0.26.3}"` at line 184 with two lines that derive the reported version from the same tag the curl stub serves: `tag="${GH_ENGINE_TAG:-$STUB_DEFAULT_ENGINE_TAG}"` then `echo "${tag#v}"`. Delete the `EXAPUMP_LAKEHOUSE_VERSION` override entirely and replace the comment at lines 179-180 with one line stating that the reported version is derived from the served release tag so a `GH_ENGINE_TAG` override stays consistent. Replace the literal at line 1029 with `"${STUB_DEFAULT_ENGINE_TAG#v}"`. Do NOT leave a literal `v0.26.3` fallback inside either stub body: a fallback would silently hide a missing `export` and let every test keep passing for the wrong reason. Verify the export actually reaches the stubs before trusting the suite — temporarily set `GH_ENGINE_TAG=v9.8.7` around one `run_file "${HAPPY_ARGS[@]}"` call and confirm the smoke test still passes (proving the stub tracked the override), then revert that probe and re-run `make test-install` to 0 failures.
