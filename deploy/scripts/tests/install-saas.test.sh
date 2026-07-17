#!/usr/bin/env bash
# shellcheck disable=SC1090,SC2030,SC2031,SC2034
# SC1090: $INSTALLER is a computed-but-stable path; shellcheck can't trace symbols across it, hence
#   the SC2034 "unused" false positives below on vars only read inside the sourced file.
# SC2030/SC2031: PATH/env changes are deliberately scoped to a subshell per test run, by design.
# Test harness for deploy/scripts/install-saas.sh. Plain bash, no framework, no jq.
#
# It stubs exapump/curl as fake executables on a temp PATH (recording their argv to a log
# and returning canned output per scenario), sources the installer's pure functions directly for
# unit checks, and drives the full installer through BOTH a saved-file invocation and the
# stdin-piped `cat install-saas.sh | bash -s -- ...` form. Covers every scenario in
# specs/_plans/change-saas-install-github-token/packaging/saas-install-script/spec.md.
#
# Run: bash deploy/scripts/tests/install-saas.test.sh

set -uo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
INSTALLER="$HERE/../install-saas.sh"
ORIG_PATH="$PATH"
BASH_BIN="$(command -v bash)"

RUST_SEGMENT="RUST=localzmq+protobuf:///uploads/default/rustslc?lang=rust#buckets/uploads/default/rustslc/exaudf/exaudfclient"

PASS=0
FAIL=0

pass() { PASS=$((PASS + 1)); printf '  ok   %s\n' "$1"; }
fail() { FAIL=$((FAIL + 1)); printf '  FAIL %s\n' "$1"; [[ -n "${2:-}" ]] && printf '       %s\n' "$2"; }

assert_eq()          { if [[ "$2" == "$3" ]]; then pass "$1"; else fail "$1" "expected [$2] got [$3]"; fi; }
assert_contains()    { if [[ "$2" == *"$3"* ]]; then pass "$1"; else fail "$1" "missing [$3]"; fi; }
assert_not_contains(){ if [[ "$2" != *"$3"* ]]; then pass "$1"; else fail "$1" "found [$3] (should be absent)"; fi; }
assert_rc_zero()     { if [[ "$2" -eq 0 ]]; then pass "$1"; else fail "$1" "expected rc 0 got $2"; fi; }
assert_rc_nonzero()  { if [[ "$2" -ne 0 ]]; then pass "$1"; else fail "$1" "expected nonzero rc got $2"; fi; }

count_occurrences() { # needle haystack -> count
  local n="$1" rest="$2" c=0
  while [[ "$rest" == *"$n"* ]]; do
    c=$((c + 1))
    rest="${rest#*"$n"}"
  done
  printf '%s' "$c"
}

# --- sandbox + stubs ---------------------------------------------------------
SANDBOX="$(mktemp -d)"
trap 'rm -rf "$SANDBOX"' EXIT

STUBDIR="$SANDBOX/stubs"
MISSING_CURL_DIR="$SANDBOX/missing-curl"
MISSING_EXAPUMP_DIR="$SANDBOX/missing-exapump"
mkdir -p "$STUBDIR" "$MISSING_CURL_DIR" "$MISSING_EXAPUMP_DIR"

STUB_LOG="$SANDBOX/stub.log"
export STUB_LOG
: > "$STUB_LOG"

write_exapump_stub() {
  cat > "$1/exapump" <<'STUB'
#!/usr/bin/env bash
printf 'exapump %s\n' "$*" >> "${STUB_LOG:-/dev/null}"
if [[ "${STUB_REPORT_STDIN:-0}" == "1" ]]; then _sd="$(cat)"; [[ -n "$_sd" ]] && printf 'STDIN_LEAK[exapump %s]\n' "$*" >> "${STUB_LOG:-/dev/null}"; fi
sql="${!#}"
case "$sql" in
  *"SELECT SYSTEM_VALUE FROM EXA_PARAMETERS"*)
    if [[ "${EXAPUMP_SL_EMPTY:-0}" == "1" ]]; then
      # Success exit, but a body that parses to no value (only header/banner/footer lines).
      echo "[connected to stub]"
      echo "SYSTEM_VALUE"
      echo "0 rows in set"
      exit 0
    fi
    echo "[connected to stub]"
    echo "SYSTEM_VALUE"
    echo "${EXAPUMP_SCRIPT_LANGUAGES:-PYTHON3=builtin_python3 JAVA=builtin_java}"
    echo "1 row in set"
    exit 0 ;;
  *"ALTER SYSTEM SET SCRIPT_LANGUAGES"*)
    if [[ "${EXAPUMP_ALTER_FAIL:-0}" == "1" ]]; then
      echo "Error: insufficient privileges: SYSTEM privilege required" >&2
      exit 1
    fi
    exit 0 ;;
  *"LAKEHOUSE_SCAN('x', 'y')"*)
    case "${EXAPUMP_SMOKE_MODE:-pass}" in
      mismatch) echo "F-UDF-CL-RUST-9001: Fingerprint mismatch: expected 0.20.3:rustc_1.94, found 0.19.0:rustc_1.90" >&2; exit 1 ;;
      anomaly)  echo "R"; echo "some-unexpected-row"; exit 0 ;;
      *)        echo "F-UDF-CL-RUST-9001: error deserializing scan spec: expected value at line 1 column 1" >&2; exit 1 ;;
    esac ;;
  *"CREATE "*)
    if [[ "${EXAPUMP_DDL_FAIL:-0}" == "1" ]]; then echo "Error: object creation failed" >&2; exit 1; fi
    exit 0 ;;
  *) exit 0 ;;
esac
STUB
  chmod +x "$1/exapump"
}

write_curl_stub() {
  cat > "$1/curl" <<'STUB'
#!/usr/bin/env bash
printf 'curl %s\n' "$*" >> "${STUB_LOG:-/dev/null}"
if [[ "${STUB_REPORT_STDIN:-0}" == "1" ]]; then _sd="$(cat)"; [[ -n "$_sd" ]] && printf 'STDIN_LEAK[curl %s]\n' "$*" >> "${STUB_LOG:-/dev/null}"; fi
method="GET"
has_upload=0
outfile=""
prev=""
for a in "$@"; do
  if [[ "$prev" == "-o" ]]; then outfile="$a"; fi
  case "$a" in
    POST) method="POST" ;;
    PUT) method="PUT" ;;
    --upload-file|-T) has_upload=1 ;;
  esac
  prev="$a"
done
url="${!#}"
if [[ "$has_upload" -eq 1 || "$method" == "PUT" ]]; then
  if [[ "${CURL_PUT_TRANSPORT_FAIL:-0}" == "1" ]]; then echo "curl: (7) Failed to connect: PUT transport failed" >&2; exit 7; fi
  # Mimic real curl's `-o <file> -w '%{http_code}'`: the body goes to -o's file, the status
  # code alone is printed to stdout, and curl itself exits 0 for any completed HTTP response
  # (2xx or not) since the real script deliberately omits -f on this call.
  [[ -n "$outfile" ]] && printf '%s' "${CURL_PUT_BODY:-}" > "$outfile"
  printf '%s' "${CURL_PUT_HTTP_CODE:-200}"
  exit 0
elif [[ "$method" == "POST" ]]; then
  if [[ "${CURL_POST_FAIL:-0}" == "1" ]]; then echo "curl: (22) POST failed" >&2; exit 22; fi
  fname="${url##*/}"
  if [[ "${CURL_POST_URL_ESCAPED:-0}" == "1" ]]; then
    printf '{"url":"https://presigned.example.com/put/%s?X-Amz-Algorithm=AWS4-HMAC-SHA256\u0026X-Amz-Signature=abc\u0026exp=600"}\n' "$fname"
  else
    printf '{"url":"https://presigned.example.com/put/%s?X-Amz-Signature=abc&exp=600"}\n' "$fname"
  fi
  exit 0
else
  case "$url" in
    */files)
      if [[ "${CURL_LIST_MISSING:-0}" == "1" ]]; then
        printf '{"files":[]}\n'
      elif [[ "${CURL_LIST_SUFFIX_ONLY:-0}" == "1" ]]; then
        printf '{"files":["rustslc.tar.gz.bak"]}\n'
      else
        printf '{"files":["lakehouse-engine.tar.gz","rustslc.tar.gz"]}\n'
      fi
      exit 0 ;;
    */releases/latest)
      # GET /repos/<repo>/releases/latest -> {"tag_name": "...", ...}
      case "$url" in
        *language-container-rs*) printf '{\n  "tag_name": "%s",\n  "name": "slc"\n}\n' "${GH_SLC_TAG:-v0.21.0}" ;;
        *)                       printf '{\n  "tag_name": "%s",\n  "name": "engine"\n}\n' "${GH_ENGINE_TAG:-v0.26.3}" ;;
      esac
      exit 0 ;;
    */releases/tags/*)
      # GET /repos/<repo>/releases/tags/<tag> -> release JSON with an "assets" array, indented
      # exactly as extract_asset_id_by_name expects (2-space "assets", 4-space object braces,
      # 6-space "id"/"name" fields).
      asset_name="lakehouse-engine.tar.gz"
      case "$url" in
        *language-container-rs*)
          ver="${GH_SLC_TAG:-v0.21.0}"; ver="${ver#v}"
          asset_name="lc-rust-$ver.tar.gz"
          ;;
      esac
      if [[ "${GH_ASSET_MISSING:-0}" == "1" ]]; then
        asset_name="does-not-match-any-asset.tar.gz"
      fi
      printf '{\n  "assets": [\n    {\n      "id": 555,\n      "name": "%s"\n    }\n  ]\n}\n' "$asset_name"
      exit 0 ;;
    */releases/assets/*)
      # GET /repos/<repo>/releases/assets/<id> (Accept: octet-stream) -> writes bytes to -o path.
      [[ -n "$outfile" ]] && printf 'stub-asset-bytes\n' > "$outfile"
      exit 0 ;;
    *)
      if [[ "${CURL_DB_UNREACHABLE:-0}" == "1" ]]; then echo "curl: (22) The requested URL returned error: 404" >&2; exit 22; fi
      printf '{"id":"stub-db","name":"stub"}\n'
      exit 0 ;;
  esac
fi
STUB
  chmod +x "$1/curl"
}

write_exapump_stub "$STUBDIR"
write_curl_stub "$STUBDIR"
# missing-curl dir: exapump only (no curl)
write_exapump_stub "$MISSING_CURL_DIR"
# missing-exapump dir: curl only (no exapump)
write_curl_stub "$MISSING_EXAPUMP_DIR"

RUN_PATH="$STUBDIR:$ORIG_PATH"

EXAPUMP_CONFIG_FIXTURE="$SANDBOX/exapump-config.toml"
cat > "$EXAPUMP_CONFIG_FIXTURE" <<'TOML'
[other]
host = "decoy"
password = "DECOY_SHOULD_NEVER_BE_USED"

[staging]
host = "decoy-host"
password = "SECRETPAT123"
TOML

reset_env() {
  unset GH_ENGINE_TAG GH_SLC_TAG GH_ASSET_MISSING 2>/dev/null || true
  unset EXAPUMP_SMOKE_MODE EXAPUMP_ALTER_FAIL EXAPUMP_DDL_FAIL EXAPUMP_SCRIPT_LANGUAGES EXAPUMP_SL_EMPTY 2>/dev/null || true
  unset CURL_POST_FAIL CURL_POST_URL_ESCAPED CURL_PUT_TRANSPORT_FAIL CURL_PUT_HTTP_CODE CURL_PUT_BODY CURL_LIST_MISSING CURL_LIST_SUFFIX_ONLY CURL_DB_UNREACHABLE 2>/dev/null || true
  unset EXAPUMP_DSN STUB_REPORT_STDIN 2>/dev/null || true
  # Stub non-empty token so happy-path runs don't each have to set it individually.
  export GITHUB_TOKEN="STUBGHTOKEN123"
  # Sandboxed exapump config so profile-mode runs never touch the real ~/.exapump/config.toml.
  export EXAPUMP_CONFIG="$EXAPUMP_CONFIG_FIXTURE"
  RUN_PATH="$STUBDIR:$ORIG_PATH"
  : > "$STUB_LOG"
}

run_file() {
  LAST_OUT="$( ( export PATH="$RUN_PATH"; exec "$BASH_BIN" "$INSTALLER" "$@" ) 2>&1 )"
  LAST_RC=$?
}

run_stdin() {
  LAST_OUT="$( cat "$INSTALLER" | ( export PATH="$RUN_PATH"; exec "$BASH_BIN" -s -- "$@" ) 2>&1 )"
  LAST_RC=$?
}

# File-mode run whose OWN stdin carries a payload; used to prove subprocesses read /dev/null.
run_file_with_stdin() {
  local payload="$1"; shift
  LAST_OUT="$( printf '%s' "$payload" | ( export PATH="$RUN_PATH"; exec "$BASH_BIN" "$INSTALLER" "$@" ) 2>&1 )"
  LAST_RC=$?
}

log_content() { printf '%s' "$(<"$STUB_LOG")"; }

# Common valid arguments for a happy-path run (profile connectivity mode).
HAPPY_ARGS=(--account-id ACC1 --database-id DB1 --profile staging)

# ============================================================================
# Scenario tests
# ============================================================================

test_missing_prereq_fails_fast() {
  echo "== test_missing_prereq_fails_fast =="
  reset_env
  RUN_PATH="$MISSING_CURL_DIR"
  run_file --account-id ACC1 --database-id DB1 --profile staging
  assert_rc_nonzero "missing curl: nonzero exit" "$LAST_RC"
  assert_contains "missing curl: names curl" "$LAST_OUT" "curl"
  assert_contains "missing curl: gives install URL" "$LAST_OUT" "https://curl.se"
  assert_eq "missing curl: no network/SQL call made" "" "$(log_content)"

  reset_env
  RUN_PATH="$MISSING_EXAPUMP_DIR"
  run_file --account-id ACC1 --database-id DB1 --profile staging
  assert_rc_nonzero "missing exapump: nonzero exit" "$LAST_RC"
  assert_contains "missing exapump: names exapump" "$LAST_OUT" "exapump"
  assert_eq "missing exapump: no network/SQL call made" "" "$(log_content)"
}

test_missing_github_token_fails_fast() {
  echo "== test_missing_github_token_fails_fast =="
  reset_env
  unset GITHUB_TOKEN
  run_file --account-id ACC1 --database-id DB1 --profile staging
  assert_rc_nonzero "missing github token: nonzero exit" "$LAST_RC"
  assert_contains "missing github token: names GITHUB_TOKEN" "$LAST_OUT" "GITHUB_TOKEN"
  assert_contains "missing github token: names --github-token" "$LAST_OUT" "--github-token"
  assert_eq "missing github token: no network/SQL call made before failing" "" "$(log_content)"
}

test_connectivity_mode_either_or() {
  echo "== test_connectivity_mode_either_or =="
  reset_env
  run_file --account-id ACC1 --database-id DB1 --profile staging --host h --user u --password p
  assert_rc_nonzero "both modes: nonzero exit" "$LAST_RC"
  assert_contains "both modes: states exactly one mode" "$LAST_OUT" "exactly one connectivity mode"
  assert_eq "both modes: no network call made" "" "$(log_content)"

  reset_env
  run_file --account-id ACC1 --database-id DB1
  assert_rc_nonzero "no mode: nonzero exit" "$LAST_RC"
  assert_contains "no mode: states exactly one mode" "$LAST_OUT" "exactly one connectivity mode"

  reset_env
  run_file --account-id ACC1 --database-id DB1 --profile staging
  assert_rc_zero "single mode proceeds to success" "$LAST_RC"
}

test_host_mode_requires_port() {
  echo "== test_host_mode_requires_port =="
  reset_env
  run_file --account-id ACC1 --database-id DB1 --host myhost --user u --password p
  assert_rc_nonzero "host no port: nonzero exit" "$LAST_RC"
  assert_contains "host no port: message mentions port" "$LAST_OUT" "port"
  assert_eq "host no port: no network call made" "" "$(log_content)"

  reset_env
  run_file --account-id ACC1 --database-id DB1 --host myhost:8563 --user u --password p
  assert_rc_zero "host with port: proceeds" "$LAST_RC"
}

test_host_dsn_percent_encodes_credentials() {
  echo "== test_host_dsn_percent_encodes_credentials =="
  reset_env
  run_file --account-id ACC1 --database-id DB1 \
    --host myhost:8563 --user 'us:er@x' --password 'p@ss/word?#'
  assert_rc_zero "encode: host-mode install still succeeds" "$LAST_RC"
  local log; log="$(log_content)"
  assert_contains "encode: DSN carries percent-encoded user" "$log" "us%3Aer%40x"
  assert_contains "encode: DSN carries percent-encoded password" "$log" "p%40ss%2Fword%3F%23"
  assert_not_contains "encode: raw reserved-char user:password sequence absent from DSN" \
    "$log" "us:er@x:p@ss"
}

test_dsn_mode_happy_path() {
  echo "== test_dsn_mode_happy_path =="
  reset_env
  run_file --account-id ACC1 --database-id DB1 --dsn "exasol://user:SECRETPAT123@dsnhost:8563"
  assert_rc_zero "dsn mode: install succeeds" "$LAST_RC"
}

test_url_decode_roundtrip() {
  echo "== test_url_decode_roundtrip =="
  local decoded
  decoded="$( source "$INSTALLER"; url_decode "p%40ss%2Fword%3F%23" )"
  assert_eq "url_decode: reverses url_encode's percent-encoding" "p@ss/word?#" "$decoded"

  decoded="$( source "$INSTALLER"; url_decode "abc%" )"
  assert_eq "url_decode: a trailing bare '%' passes through literally" "abc%" "$decoded"

  decoded="$( source "$INSTALLER"; url_decode "abc%4" )"
  assert_eq "url_decode: a truncated '%4' (one hex digit) passes through literally" "abc%4" "$decoded"
}

test_extract_dsn_password() {
  echo "== test_extract_dsn_password =="
  local pw rc
  pw="$( source "$INSTALLER"; extract_dsn_password "exasol://user:SECRETPAT123@host:8563" )"
  assert_eq "extract_dsn_password: returns the still-encoded password segment" "SECRETPAT123" "$pw"

  pw="$( source "$INSTALLER"; extract_dsn_password "exasol://host:8563" )"
  rc=$?
  assert_rc_nonzero "extract_dsn_password: no ':password@' segment fails" "$rc"
  assert_eq "extract_dsn_password: no output when password segment absent" "" "$pw"

  pw="$( source "$INSTALLER"; extract_dsn_password "exasol://user:pass@word@host:8563" )"
  assert_eq "extract_dsn_password: a raw '@' inside the password is preserved" "pass@word" "$pw"

  pw="$( source "$INSTALLER"; extract_dsn_password "exasol://user:p%40ss@host:8563" )"
  assert_eq "extract_dsn_password: an already-percent-encoded segment is returned as-is" "p%40ss" "$pw"

  pw="$( source "$INSTALLER"; extract_dsn_password "exasol://user:@host:8563" )"
  rc=$?
  assert_rc_zero "extract_dsn_password: an empty password segment still succeeds" "$rc"
  assert_eq "extract_dsn_password: empty password segment yields empty output" "" "$pw"
}

test_read_profile_password() {
  echo "== test_read_profile_password =="
  local pw rc

  pw="$( source "$INSTALLER"; read_profile_password "staging" "$EXAPUMP_CONFIG_FIXTURE" )"
  assert_eq "read_profile_password: resolves staging's password" "SECRETPAT123" "$pw"
  assert_not_contains "read_profile_password: never returns the other section's decoy" \
    "$pw" "DECOY_SHOULD_NEVER_BE_USED"

  pw="$( source "$INSTALLER"; read_profile_password "no-such-profile" "$EXAPUMP_CONFIG_FIXTURE" )"
  rc=$?
  assert_rc_nonzero "read_profile_password: unknown profile fails" "$rc"
  assert_eq "read_profile_password: no output for unknown profile" "" "$pw"

  pw="$( source "$INSTALLER"; read_profile_password "staging" "$SANDBOX/does-not-exist.toml" )"
  rc=$?
  assert_rc_nonzero "read_profile_password: nonexistent config file fails" "$rc"
  assert_eq "read_profile_password: no output for nonexistent config file" "" "$pw"

  local no_password_fixture="$SANDBOX/no-password-config.toml"
  cat > "$no_password_fixture" <<'TOML'
[nopass]
host = "some-host"

[staging]
host = "decoy-host"
password = "SECRETPAT123"
TOML
  pw="$( source "$INSTALLER"; read_profile_password "nopass" "$no_password_fixture" )"
  rc=$?
  assert_rc_nonzero "read_profile_password: section with no password key before the next section fails" "$rc"
  assert_eq "read_profile_password: no output when the password key is missing" "" "$pw"
}

test_resolve_pat_per_mode() {
  echo "== test_resolve_pat_per_mode =="
  local result rc

  result="$(
    source "$INSTALLER"
    CONNECTIVITY_MODE="host"
    ARG_PASSWORD="SECRETPW456"
    if resolve_pat; then printf 'ok:%s\n' "$RESOLVED_PAT"; else printf 'fail\n'; fi
  )"
  assert_eq "resolve_pat: host mode derives from ARG_PASSWORD" "ok:SECRETPW456" "$result"

  result="$(
    source "$INSTALLER"
    CONNECTIVITY_MODE="dsn"
    ARG_DSN="exasol://user:SECRETPAT123@host:8563"
    if resolve_pat; then printf 'ok:%s\n' "$RESOLVED_PAT"; else printf 'fail\n'; fi
  )"
  assert_eq "resolve_pat: dsn mode derives from the DSN password segment" "ok:SECRETPAT123" "$result"

  result="$(
    source "$INSTALLER"
    CONNECTIVITY_MODE="profile"
    ARG_PROFILE="staging"
    EXAPUMP_CONFIG="$EXAPUMP_CONFIG_FIXTURE"
    if resolve_pat; then printf 'ok:%s\n' "$RESOLVED_PAT"; else printf 'fail\n'; fi
  )"
  assert_eq "resolve_pat: profile mode derives from the exapump config fixture" "ok:SECRETPAT123" "$result"

  local dsn_err
  dsn_err="$(
    source "$INSTALLER"
    CONNECTIVITY_MODE="dsn"
    ARG_DSN="exasol://host:8563"
    resolve_pat 2>&1
  )"
  rc=$?
  assert_rc_nonzero "resolve_pat: dsn mode fails without a password segment" "$rc"
  assert_not_contains "resolve_pat: dsn failure message names no credential value" "$dsn_err" "SECRETPAT123"

  local profile_err
  profile_err="$(
    source "$INSTALLER"
    CONNECTIVITY_MODE="profile"
    ARG_PROFILE="no-such-profile"
    EXAPUMP_CONFIG="$EXAPUMP_CONFIG_FIXTURE"
    resolve_pat 2>&1
  )"
  rc=$?
  assert_rc_nonzero "resolve_pat: profile mode fails for an unknown profile" "$rc"
  assert_not_contains "resolve_pat: unknown-profile failure never leaks the fixture password" \
    "$profile_err" "SECRETPAT123"
  assert_not_contains "resolve_pat: unknown-profile failure never leaks the decoy password" \
    "$profile_err" "DECOY_SHOULD_NEVER_BE_USED"
}

test_missing_required_ids_fail_fast() {
  echo "== test_missing_required_ids_fail_fast =="
  reset_env
  run_file --database-id DB1 --profile staging
  assert_rc_nonzero "missing account-id: nonzero exit" "$LAST_RC"
  assert_contains "missing account-id: names it" "$LAST_OUT" "--account-id"
  assert_contains "missing account-id: points to SaaS console" "$LAST_OUT" "SaaS web console"
  assert_eq "missing account-id: no network call" "" "$(log_content)"

  reset_env
  run_file --account-id ACC1 --profile staging
  assert_rc_nonzero "missing database-id: nonzero exit" "$LAST_RC"
  assert_contains "missing database-id: names it" "$LAST_OUT" "--database-id"
}

test_version_resolution_default_and_override() {
  echo "== test_version_resolution_default_and_override =="
  reset_env
  local out
  out="$(
    export PATH="$STUBDIR:$ORIG_PATH" STUB_LOG GH_ENGINE_TAG="v1.2.3" GH_SLC_TAG="v4.5.6"
    source "$INSTALLER"
    ARG_GITHUB_TOKEN="STUBGHTOKEN123"
    ARG_LAKEHOUSE_VERSION=""; ARG_SLC_VERSION=""
    resolve_versions
  )"
  assert_contains "default: resolves latest engine tag" "$out" "1.2.3"
  assert_contains "default: resolves latest SLC tag" "$out" "4.5.6"
  assert_contains "default: prints engine version line" "$out" "Resolved lakehouse-engine version"
  assert_contains "default: prints SLC version line" "$out" "Resolved language-container (SLC) version"
  local log; log="$(log_content)"
  assert_contains "default: curl hits the releases/latest endpoint" "$log" "releases/latest"
  assert_contains "default: curl sends the Bearer token header" "$log" "Authorization: Bearer STUBGHTOKEN123"

  reset_env
  out="$(
    export PATH="$STUBDIR:$ORIG_PATH" STUB_LOG GH_ENGINE_TAG="v1.2.3" GH_SLC_TAG="v4.5.6"
    source "$INSTALLER"
    ARG_GITHUB_TOKEN="STUBGHTOKEN123"
    ARG_LAKEHOUSE_VERSION="9.9.9"; ARG_SLC_VERSION="8.8.8"
    resolve_versions
  )"
  assert_contains "override: uses engine override" "$out" "9.9.9"
  assert_contains "override: uses SLC override" "$out" "8.8.8"
  assert_not_contains "override: ignores latest engine tag" "$out" "1.2.3"
  assert_not_contains "override: ignores latest SLC tag" "$out" "4.5.6"
  log="$(log_content)"
  assert_not_contains "override: skips the releases/latest fetch entirely" "$log" "releases/latest"
}

test_script_languages_append_preserves_existing() {
  echo "== test_script_languages_append_preserves_existing =="
  local out
  out="$( source "$INSTALLER"; compute_script_languages "PYTHON3=builtin_python3 JAVA=builtin_java" )"
  assert_contains "append: preserves PYTHON3" "$out" "PYTHON3=builtin_python3"
  assert_contains "append: preserves JAVA" "$out" "JAVA=builtin_java"
  assert_contains "append: adds the exact RUST segment" "$out" "$RUST_SEGMENT"
  assert_eq "append: existing order preserved, RUST last" \
    "PYTHON3=builtin_python3 JAVA=builtin_java $RUST_SEGMENT" "$out"
  assert_eq "append: exactly one RUST entry" "1" "$(count_occurrences 'RUST=' "$out")"
}

test_script_languages_replace_rust_idempotent() {
  echo "== test_script_languages_replace_rust_idempotent =="
  local out out2
  out="$( source "$INSTALLER"; compute_script_languages "PYTHON3=p RUST=stale_alias JAVA=j" )"
  assert_eq "replace: in place, non-RUST unchanged" \
    "PYTHON3=p $RUST_SEGMENT JAVA=j" "$out"
  assert_eq "replace: exactly one RUST entry" "1" "$(count_occurrences 'RUST=' "$out")"
  # Idempotency: feeding the result back yields an identical value.
  out2="$( source "$INSTALLER"; compute_script_languages "$out" )"
  assert_eq "replace: idempotent re-run" "$out" "$out2"
  assert_eq "replace: still exactly one RUST entry" "1" "$(count_occurrences 'RUST=' "$out2")"
}

test_empty_script_languages_read_hard_fails() {
  echo "== test_empty_script_languages_read_hard_fails =="
  # A successful read (exit 0) that yields an empty/unparseable SCRIPT_LANGUAGES value must be
  # treated as an anomaly and HARD-FAIL, never silently proceed: computing from "" would drop
  # every pre-existing language, and issuing ALTER SYSTEM SET would wipe them.
  reset_env
  export EXAPUMP_SL_EMPTY=1
  run_file "${HAPPY_ARGS[@]}"
  assert_rc_nonzero "empty SL read: nonzero exit" "$LAST_RC"
  assert_contains "empty SL read: error names SCRIPT_LANGUAGES" "$LAST_OUT" "SCRIPT_LANGUAGES"
  assert_not_contains "empty SL read: no success reported" "$LAST_OUT" "query-ready"
  local log; log="$(log_content)"
  assert_not_contains "empty SL read: never issues ALTER SYSTEM SET (no language wipe)" \
    "$log" "ALTER SYSTEM SET SCRIPT_LANGUAGES"
}

test_presigned_upload_dance() {
  echo "== test_presigned_upload_dance =="
  reset_env
  run_file "${HAPPY_ARGS[@]}"
  assert_rc_zero "upload: install succeeds" "$LAST_RC"
  local log; log="$(log_content)"
  assert_contains "upload: resolves release-by-tag JSON for the asset id lookup" "$log" "releases/tags/"
  assert_contains "upload: downloads the asset via the authenticated octet-stream GET" "$log" "releases/assets/"
  assert_contains "upload: SLC renamed to rustslc.tar.gz (POST key)" "$log" "/files/rustslc.tar.gz"
  assert_contains "upload: engine POST key" "$log" "/files/lakehouse-engine.tar.gz"
  assert_contains "upload: POST to obtain presigned url" "$log" "-X POST"
  assert_contains "upload: PUT to presigned url" "$log" "-X PUT"
  assert_contains "upload: verifies via files listing" "$log" "/databases/DB1/files"
  local put_lines
  put_lines="$(printf '%s\n' "$log" | grep -- '--upload-file' || true)"
  assert_not_contains "upload: PUT adds no Authorization header" "$put_lines" "Authorization"
}

test_presigned_url_json_unescaping() {
  echo "== test_presigned_url_json_unescaping =="
  # Direct unit test: some SaaS-backend JSON encoders (notably Go's encoding/json, which
  # HTML-escapes '&', '<', '>' by default) return the presigned URL with its '&'
  # query-parameter separators as the 6-character numeric escape rather than a literal '&'.
  # extract_json_string_field must un-escape that back to a real '&', or every parameter after
  # the first collapses into the previous one's value -- exactly the live failure mode seen
  # against Exasol SaaS staging (HTTP 400 AuthorizationQueryParametersError on the PUT).
  local raw escaped_url
  raw="$(printf '{"url":"https://bucket.s3.amazonaws.com/key?X-Amz-Algorithm=AWS4-HMAC-SHA256%su0026X-Amz-Credential=abc%su0026X-Amz-Signature=xyz"}' '\' '\')"
  escaped_url="$(
    export PATH="$STUBDIR:$ORIG_PATH"
    source "$INSTALLER"
    extract_json_string_field "$raw" "url"
  )"
  assert_eq "unescape: numeric-escaped ampersands become real '&' separators" \
    "https://bucket.s3.amazonaws.com/key?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=abc&X-Amz-Signature=xyz" \
    "$escaped_url"

  # Integration: the full installer must still succeed end-to-end when the SaaS files POST
  # response itself carries an escaped presigned URL, and the PUT curl invocation actually
  # logged must carry the real, un-escaped '&'-joined query string.
  reset_env
  export CURL_POST_URL_ESCAPED=1
  run_file "${HAPPY_ARGS[@]}"
  assert_rc_zero "unescape: install still succeeds against an escaped presigned URL" "$LAST_RC"
  local log; log="$(log_content)"
  assert_contains "unescape: PUT hits the URL with a real '&' between params" "$log" "X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Signature=abc"
}

test_release_asset_download_via_rest() {
  echo "== test_release_asset_download_via_rest =="
  reset_env
  run_file "${HAPPY_ARGS[@]}"
  assert_rc_zero "asset rest: install succeeds" "$LAST_RC"
  local log; log="$(log_content)"
  assert_contains "asset rest: id-lookup step (release-by-tag GET) happens" "$log" "releases/tags/"
  local dl_lines
  dl_lines="$(printf '%s\n' "$log" | grep -- 'releases/assets/' || true)"
  assert_contains "asset rest: final download follows redirects (-L within the -fsSL cluster)" "$dl_lines" "-fsSL"
  assert_contains "asset rest: final download sends the octet-stream accept header" "$dl_lines" "Accept: application/octet-stream"
  assert_not_contains "asset rest: final download never escalates to --location-trusted" "$dl_lines" "--location-trusted"

  reset_env
  export GH_ASSET_MISSING=1
  local out rc
  out="$(
    export PATH="$STUBDIR:$ORIG_PATH" STUB_LOG GH_ASSET_MISSING
    source "$INSTALLER"
    ARG_GITHUB_TOKEN="STUBGHTOKEN123"
    download_release_asset "$ENGINE_REPO" "v1.2.3" "$ENGINE_ASSET" "$SANDBOX/does-not-matter" 2>&1
  )"
  rc=$?
  assert_rc_nonzero "asset rest: missing/non-matching asset name fails non-zero" "$rc"
  assert_contains "asset rest: error names the repo" "$out" "exasol-labs/lakehouse-engine-rs"
  assert_contains "asset rest: error names the tag" "$out" "v1.2.3"
  assert_contains "asset rest: error names the asset" "$out" "lakehouse-engine.tar.gz"
}

test_extract_asset_id_by_name_realistic() {
  echo "== test_extract_asset_id_by_name_realistic =="
  # Direct unit test of the no-jq asset-id lookup against a realistic GitHub release JSON
  # (2-space pretty-print). Harder than the stub fixtures: two assets with the TARGET NOT first,
  # each asset's "name" ordered BEFORE its "id", and each carrying a nested "uploader" object with
  # its OWN numeric "id" at deeper (8-space) indent. A trailing "mentions" array AFTER the assets
  # block carries a name absent from assets, to prove the scan is bounded to the assets block and
  # cannot misfire on a later array-of-objects field.
  local fixture id rc first_id mentions_only rc2 none rc3
  fixture="$(cat <<'JSON'
{
  "tag_name": "v1.2.3",
  "assets": [
    {
      "url": "https://api.github.com/repos/x/y/releases/assets/111",
      "name": "other-asset.tar.gz",
      "id": 111,
      "uploader": {
        "login": "octocat",
        "id": 9001
      }
    },
    {
      "url": "https://api.github.com/repos/x/y/releases/assets/222",
      "name": "lakehouse-engine.tar.gz",
      "id": 222,
      "uploader": {
        "login": "hubot",
        "id": 9002
      }
    }
  ],
  "mentions": [
    {
      "name": "only-in-mentions.tar.gz",
      "id": 7777
    }
  ]
}
JSON
)"

  id="$( source "$INSTALLER"; extract_asset_id_by_name "$fixture" "lakehouse-engine.tar.gz" )"
  rc=$?
  assert_rc_zero "extract: target present (not first) resolves" "$rc"
  assert_eq "extract: resolves the target asset's own id (222), not its uploader id (9002) or the wrong asset" "222" "$id"

  first_id="$( source "$INSTALLER"; extract_asset_id_by_name "$fixture" "other-asset.tar.gz" )"
  assert_eq "extract: first asset resolves to its own id (111), uploader id (9001) ignored" "111" "$first_id"

  # Bounded to the assets block: a name present ONLY in the trailing "mentions" array must NOT
  # resolve to that later array's id (the pre-fix scan-to-EOF false positive).
  mentions_only="$( source "$INSTALLER"; extract_asset_id_by_name "$fixture" "only-in-mentions.tar.gz" )"
  rc2=$?
  assert_rc_nonzero "extract: name found only outside the assets block never matches" "$rc2"
  assert_eq "extract: no id printed for a name outside the assets block" "" "$mentions_only"

  # Non-matching name against the rich fixture still fails cleanly.
  none="$( source "$INSTALLER"; extract_asset_id_by_name "$fixture" "no-such-asset.tar.gz" )"
  rc3=$?
  assert_rc_nonzero "extract: non-matching name returns failure" "$rc3"
  assert_eq "extract: no id printed for a non-matching name" "" "$none"
}

test_saas_verify_listed_quoted_match() {
  echo "== test_saas_verify_listed_quoted_match =="
  reset_env
  local no_match
  no_match="$(
    export PATH="$STUBDIR:$ORIG_PATH" STUB_LOG CURL_LIST_SUFFIX_ONLY=1
    source "$INSTALLER"
    ARG_ACCOUNT_ID=ACC1 ARG_DATABASE_ID=DB1 RESOLVED_PAT=SECRETPAT123 ARG_STAGING=0
    if saas_verify_listed "rustslc.tar.gz"; then echo yes; else echo no; fi
  )"
  assert_eq "suffix collision: does not false-positive on a longer stored name" "no" "$no_match"

  local exact_match
  exact_match="$(
    export PATH="$STUBDIR:$ORIG_PATH" STUB_LOG
    source "$INSTALLER"
    ARG_ACCOUNT_ID=ACC1 ARG_DATABASE_ID=DB1 RESOLVED_PAT=SECRETPAT123 ARG_STAGING=0
    if saas_verify_listed "rustslc.tar.gz"; then echo yes; else echo no; fi
  )"
  assert_eq "exact match: still verifies a real upload" "yes" "$exact_match"
}

test_four_scripts_ddl_saas_path_types() {
  echo "== test_four_scripts_ddl_saas_path_types =="
  # Unit: DDL string shapes.
  local scan dmc dist adapter schema
  scan="$( source "$INSTALLER"; ddl_scan LHVS /buckets/uploads/default/lakehouse-engine/udf/liblakehouse_engine.so )"
  assert_contains "scan is RUST SCALAR" "$scan" "RUST SCALAR SCRIPT"
  assert_contains "scan uses dynamic EMITS" "$scan" "EMITS (...)"
  assert_not_contains "scan is never a SET script" "$scan" "SET SCRIPT"
  dmc="$( source "$INSTALLER"; ddl_distinct_merge_count LHVS /x.so )"
  assert_contains "distinct-merge RETURNS DECIMAL(20,0)" "$dmc" "RETURNS DECIMAL(20,0)"
  dist="$( source "$INSTALLER"; ddl_distribute_files LHVS )"
  assert_contains "distribute is LUA SET" "$dist" "LUA SET SCRIPT"
  adapter="$( source "$INSTALLER"; ddl_adapter LHVS /buckets/uploads/default/lakehouse-engine/udf/liblakehouse_engine.so )"
  assert_contains "adapter references SaaS %udf_object path" "$adapter" "/buckets/uploads/default/lakehouse-engine/udf/liblakehouse_engine.so"

  # Integration: the four scripts are actually created + CREATE SCHEMA IF NOT EXISTS.
  reset_env
  run_file "${HAPPY_ARGS[@]}"
  assert_rc_zero "ddl: install succeeds" "$LAST_RC"
  local log; log="$(log_content)"
  assert_contains "ddl: CREATE SCHEMA IF NOT EXISTS" "$log" "CREATE SCHEMA IF NOT EXISTS LHVS"
  assert_contains "ddl: LAKEHOUSE_ADAPTER RUST ADAPTER" "$log" "RUST ADAPTER SCRIPT LHVS.LAKEHOUSE_ADAPTER"
  assert_contains "ddl: LAKEHOUSE_SCAN RUST SCALAR" "$log" "RUST SCALAR SCRIPT LHVS.LAKEHOUSE_SCAN"
  assert_contains "ddl: dynamic EMITS on scan" "$log" "EMITS (...)"
  assert_contains "ddl: DISTINCT_MERGE_COUNT DECIMAL(20,0)" "$log" "LHVS.LAKEHOUSE_DISTINCT_MERGE_COUNT"
  assert_contains "ddl: DISTINCT_MERGE_COUNT RETURNS DECIMAL(20,0)" "$log" "RETURNS DECIMAL(20,0)"
  assert_contains "ddl: DISTRIBUTE_FILES LUA SET" "$log" "LUA SET SCRIPT LHVS.LAKEHOUSE_DISTRIBUTE_FILES"
  assert_contains "ddl: uses CREATE OR REPLACE" "$log" "CREATE OR REPLACE"
}

test_fingerprint_smoke_pass_and_fail() {
  echo "== test_fingerprint_smoke_pass_and_fail =="
  reset_env
  export EXAPUMP_SMOKE_MODE=pass
  run_file "${HAPPY_ARGS[@]}"
  assert_rc_zero "smoke pass: install succeeds" "$LAST_RC"
  assert_contains "smoke pass: reported passed" "$LAST_OUT" "Fingerprint smoke test passed"

  reset_env
  export EXAPUMP_SMOKE_MODE=mismatch
  run_file "${HAPPY_ARGS[@]}"
  assert_rc_nonzero "smoke mismatch: nonzero exit" "$LAST_RC"
  assert_contains "smoke mismatch: reports fingerprint failure" "$LAST_OUT" "fingerprint smoke test FAILED"
  assert_contains "smoke mismatch: remediation mentions SLC alignment" "$LAST_OUT" "SLC"
  assert_not_contains "smoke mismatch: does not print success" "$LAST_OUT" "query-ready"

  reset_env
  export EXAPUMP_SMOKE_MODE=anomaly
  run_file "${HAPPY_ARGS[@]}"
  assert_rc_nonzero "smoke anomaly: nonzero exit" "$LAST_RC"
  assert_contains "smoke anomaly: surfaces the anomaly" "$LAST_OUT" "anomaly"
}

test_stops_at_product_prints_template() {
  echo "== test_stops_at_product_prints_template =="
  reset_env
  run_file "${HAPPY_ARGS[@]}"
  assert_rc_zero "template: install succeeds" "$LAST_RC"
  assert_contains "template: prints CREATE CONNECTION" "$LAST_OUT" "CREATE OR REPLACE CONNECTION LAKEHOUSE_CATALOG_CREDS"
  assert_contains "template: prints CREATE VIRTUAL SCHEMA" "$LAST_OUT" "CREATE VIRTUAL SCHEMA"
  assert_contains "template: references the adapter script" "$LAST_OUT" "LHVS.LAKEHOUSE_ADAPTER"
  local log; log="$(log_content)"
  assert_not_contains "template: does NOT execute CREATE VIRTUAL SCHEMA" "$log" "CREATE VIRTUAL SCHEMA"
  assert_not_contains "template: does NOT execute CREATE CONNECTION" "$log" "CREATE OR REPLACE CONNECTION"
}

test_target_base_default_and_override() {
  echo "== test_target_base_default_and_override =="
  # Unit
  local prod staging
  prod="$( source "$INSTALLER"; ARG_STAGING=0; resolve_saas_base )"
  staging="$( source "$INSTALLER"; ARG_STAGING=1; resolve_saas_base )"
  assert_eq "base: default is production" "https://cloud.exasol.com" "$prod"
  assert_eq "base: --staging selects staging" "https://cloud-staging.exasol.com" "$staging"

  # Integration: default target
  reset_env
  run_file "${HAPPY_ARGS[@]}"
  local log; log="$(log_content)"
  assert_contains "base: prod REST calls hit cloud.exasol.com" "$log" "https://cloud.exasol.com/api/v1"
  assert_not_contains "base: prod run never hits staging" "$log" "cloud-staging.exasol.com"

  # Integration: staging target
  reset_env
  run_file --account-id ACC1 --database-id DB1 --profile staging --staging
  log="$(log_content)"
  assert_contains "base: --staging REST calls hit cloud-staging.exasol.com" "$log" "https://cloud-staging.exasol.com/api/v1"
}

test_external_failure_actionable() {
  echo "== test_external_failure_actionable =="
  # DB reachability 404
  reset_env
  export CURL_DB_UNREACHABLE=1
  run_file "${HAPPY_ARGS[@]}"
  assert_rc_nonzero "db 404: nonzero exit" "$LAST_RC"
  assert_contains "db 404: names the reachability step" "$LAST_OUT" "not reachable"
  assert_not_contains "db 404: no success reported" "$LAST_OUT" "query-ready"

  # Presigned upload failure (POST for the presigned URL fails)
  reset_env
  export CURL_POST_FAIL=1
  run_file "${HAPPY_ARGS[@]}"
  assert_rc_nonzero "upload fail: nonzero exit" "$LAST_RC"
  assert_contains "upload fail: names upload step" "$LAST_OUT" "upload"
  assert_contains "upload fail: surfaces curl's own diagnostic" "$LAST_OUT" "POST failed"

  # Presigned upload failure (PUT never completes -- transport error, no HTTP response at all):
  # curl's own stderr must surface in the error message rather than being discarded, since it is
  # the only source of the actual cause (DNS, TLS, connection refused, ...).
  reset_env
  export CURL_PUT_TRANSPORT_FAIL=1
  run_file "${HAPPY_ARGS[@]}"
  assert_rc_nonzero "put transport fail: nonzero exit" "$LAST_RC"
  assert_contains "put transport fail: names upload step" "$LAST_OUT" "upload"
  assert_contains "put transport fail: surfaces curl's own diagnostic" "$LAST_OUT" "PUT transport failed"

  # Presigned upload failure (PUT completes but the host rejects it, e.g. HTTP 400/403): the
  # response BODY -- not just the status code -- must surface, since the storage host's own
  # error detail is the only way to tell a signature mismatch from an expired URL from anything
  # else. This is the exact shape hit live against Exasol SaaS staging (HTTP 400).
  reset_env
  export CURL_PUT_HTTP_CODE=400
  export CURL_PUT_BODY='<Error><Code>InvalidArgument</Code><Message>bad request</Message></Error>'
  run_file "${HAPPY_ARGS[@]}"
  assert_rc_nonzero "put http fail: nonzero exit" "$LAST_RC"
  assert_contains "put http fail: names upload step" "$LAST_OUT" "upload"
  assert_contains "put http fail: reports the HTTP status" "$LAST_OUT" "400"
  assert_contains "put http fail: surfaces the response body detail" "$LAST_OUT" "InvalidArgument"

  # exapump ALTER SYSTEM privilege failure
  reset_env
  export EXAPUMP_ALTER_FAIL=1
  run_file "${HAPPY_ARGS[@]}"
  assert_rc_nonzero "alter fail: nonzero exit" "$LAST_RC"
  assert_contains "alter fail: mentions admin/privilege" "$LAST_OUT" "privilege"

  # Credential safety: PAT and password never printed (host mode, failing run + success run).
  reset_env
  export CURL_DB_UNREACHABLE=1
  run_file --account-id ACC1 --database-id DB1 --host myhost:8563 --user myuser --password SECRETPW456
  assert_not_contains "creds: PAT absent from failing output" "$LAST_OUT" "SECRETPAT123"
  assert_not_contains "creds: password absent from failing output" "$LAST_OUT" "SECRETPW456"

  reset_env
  run_file --account-id ACC1 --database-id DB1 --host myhost:8563 --user myuser --password SECRETPW456
  assert_rc_zero "creds: host-mode install succeeds" "$LAST_RC"
  assert_not_contains "creds: PAT absent from success output" "$LAST_OUT" "SECRETPAT123"
  assert_not_contains "creds: password absent from success output" "$LAST_OUT" "SECRETPW456"

  # Credential safety: the profile-fixture password never printed (profile mode, success run).
  # HAPPY_ARGS uses --profile staging, whose fixture password is SECRETPAT123.
  reset_env
  run_file "${HAPPY_ARGS[@]}"
  assert_rc_zero "creds: profile-mode install succeeds" "$LAST_RC"
  assert_not_contains "creds: profile fixture password absent from success output" "$LAST_OUT" "SECRETPAT123"
}

test_stdin_piped_invocation_no_body_consumption() {
  echo "== test_stdin_piped_invocation_no_body_consumption =="
  reset_env
  run_stdin "${HAPPY_ARGS[@]}"
  assert_rc_zero "stdin-piped: install succeeds without truncation" "$LAST_RC"
  assert_contains "stdin-piped: reaches the smoke-test pass" "$LAST_OUT" "Fingerprint smoke test passed"
  assert_contains "stdin-piped: reaches the next-step template" "$LAST_OUT" "CREATE VIRTUAL SCHEMA"
  local log; log="$(log_content)"
  # If any subprocess had consumed the piped body, execution would truncate before these.
  assert_contains "stdin-piped: GitHub release resolved (releases/latest reached)" "$log" "releases/latest"
  assert_contains "stdin-piped: release-by-tag asset id lookup reached (releases/tags)" "$log" "releases/tags/"
  assert_contains "stdin-piped: release asset downloaded (releases/assets)" "$log" "releases/assets/"
  assert_contains "stdin-piped: SLC uploaded (body not truncated early)" "$log" "/files/rustslc.tar.gz"
  assert_contains "stdin-piped: four scripts created" "$log" "LHVS.LAKEHOUSE_DISTRIBUTE_FILES"
  assert_contains "stdin-piped: smoke-test SQL executed (reached end)" "$log" "LAKEHOUSE_SCAN('x', 'y')"

  # Per-subprocess proof that stdin is redirected from /dev/null: run in file mode with a sentinel
  # payload on the installer's OWN stdin, and make every stub report if it read any stdin. A
  # correctly-redirected subprocess reads /dev/null (nothing); a single missing </dev/null would
  # let a subprocess read the installer's stdin and leak the sentinel.
  reset_env
  export STUB_REPORT_STDIN=1
  run_file_with_stdin "SENTINEL_STDIN_PAYLOAD_9c3f"$'\n' "${HAPPY_ARGS[@]}"
  assert_rc_zero "stdin-redirect: install still succeeds with data on installer stdin" "$LAST_RC"
  assert_not_contains "stdin-redirect: no subprocess reported inherited stdin" "$(log_content)" "STDIN_LEAK"
  assert_not_contains "stdin-redirect: sentinel never reached any subprocess" "$(log_content)" "SENTINEL_STDIN_PAYLOAD_9c3f"
}

# ============================================================================
main() {
  test_missing_prereq_fails_fast
  test_missing_github_token_fails_fast
  test_connectivity_mode_either_or
  test_host_mode_requires_port
  test_host_dsn_percent_encodes_credentials
  test_dsn_mode_happy_path
  test_url_decode_roundtrip
  test_extract_dsn_password
  test_read_profile_password
  test_resolve_pat_per_mode
  test_missing_required_ids_fail_fast
  test_version_resolution_default_and_override
  test_script_languages_append_preserves_existing
  test_script_languages_replace_rust_idempotent
  test_empty_script_languages_read_hard_fails
  test_presigned_upload_dance
  test_presigned_url_json_unescaping
  test_release_asset_download_via_rest
  test_extract_asset_id_by_name_realistic
  test_saas_verify_listed_quoted_match
  test_four_scripts_ddl_saas_path_types
  test_fingerprint_smoke_pass_and_fail
  test_stops_at_product_prints_template
  test_target_base_default_and_override
  test_external_failure_actionable
  test_stdin_piped_invocation_no_body_consumption

  echo ""
  echo "=================================================="
  printf 'RESULT: %d passed, %d failed\n' "$PASS" "$FAIL"
  echo "=================================================="
  [[ "$FAIL" -eq 0 ]]
}

main "$@"
