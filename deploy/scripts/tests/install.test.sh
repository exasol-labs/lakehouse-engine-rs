#!/usr/bin/env bash
# shellcheck disable=SC1090,SC2030,SC2031,SC2034
# SC1090: $INSTALLER is a computed-but-stable path; shellcheck can't trace symbols across it, hence
#   the SC2034 "unused" false positives below on vars only read inside the sourced file.
# SC2030/SC2031: PATH/env changes are deliberately scoped to a subshell per test run, by design.
# Test harness for deploy/scripts/install.sh. Plain bash, no framework, no jq.
#
# It stubs exapump/curl as fake executables on a temp PATH (recording their argv to a log
# and returning canned output per scenario), sources the installer's pure functions directly for
# unit checks, and drives the full installer through BOTH a saved-file invocation and the
# stdin-piped `cat install.sh | bash -s -- ...` form. Covers every scenario in
# specs/_plans/change-saas-install-github-token/packaging/saas-install-script/spec.md.
#
# Run: bash deploy/scripts/tests/install.test.sh

set -uo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
INSTALLER="$HERE/../install.sh"
REPO_ROOT="$(cd "$HERE/../../.." && pwd)"
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

# assert_precedes label haystack first second -- both must be present, and the
# FIRST occurrence of `first` must come before the first occurrence of `second`.
# Statement ORDER is load-bearing in the next-step template: the adapter's
# connection grant has to exist before CREATE VIRTUAL SCHEMA runs.
assert_precedes() {
  local hay="$2" a="$3" b="$4"
  if [[ "$hay" != *"$a"* ]]; then fail "$1" "missing [$a]"; return; fi
  if [[ "$hay" != *"$b"* ]]; then fail "$1" "missing [$b]"; return; fi
  local pre_a="${hay%%"$a"*}" pre_b="${hay%%"$b"*}"
  if (( ${#pre_a} < ${#pre_b} )); then pass "$1"; else fail "$1" "[$a] does not precede [$b]"; fi
}

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
MISSING_SSH_DIR="$SANDBOX/missing-ssh"
MISSING_SCP_DIR="$SANDBOX/missing-scp"
mkdir -p "$STUBDIR" "$MISSING_CURL_DIR" "$MISSING_EXAPUMP_DIR" "$MISSING_SSH_DIR" "$MISSING_SCP_DIR"

STUB_LOG="$SANDBOX/stub.log"
export STUB_LOG
: > "$STUB_LOG"

for _prereq in jq make; do
  command -v "$_prereq" >/dev/null 2>&1 || {
    printf 'FATAL: %s is required to run the --deployment tests; install it and re-run.\n' "$_prereq" >&2
    exit 1
  }
done

write_exapump_stub() {
  cat > "$1/exapump" <<'STUB'
#!/usr/bin/env bash
printf 'exapump %s\n' "$*" >> "${STUB_LOG:-/dev/null}"
if [[ "${STUB_REPORT_STDIN:-0}" == "1" ]]; then _sd="$(cat)"; [[ -n "$_sd" ]] && printf 'STDIN_LEAK[exapump %s]\n' "$*" >> "${STUB_LOG:-/dev/null}"; fi

# --- `exapump bucketfs <cp|ls|rm>` -------------------------------------------
# Models a real bucket well enough to exercise the installer's verify/retry logic: `cp` records
# the destination in a state file, `ls <prefix>` reports that state's immediate children (exiting
# non-zero on no match, exactly as the real CLI's "Path not found"), and `ls` with no path is the
# always-succeeding top-level reachability probe.
if [[ "${1:-}" == "bucketfs" ]]; then
  _sub="${2:-}"
  shift 2 2>/dev/null || true
  _pos=()
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --profile|--bfs-host|--bfs-port|--bfs-bucket|--bfs-write-password|--bfs-read-password|--bfs-tls|--bfs-validate-certificate)
        shift 2 ;;
      -r|--recursive) shift ;;
      *) _pos+=("$1"); shift ;;
    esac
  done
  _state="${STUB_BFS_STATE:-/dev/null}"
  case "$_sub" in
    cp)
      if [[ "${EXAPUMP_BFS_CP_FAIL:-0}" == "1" ]]; then
        echo "Error: BucketFS returned HTTP 403 Forbidden" >&2
        exit 1
      fi
      printf '%s\n' "${_pos[1]:-}" >> "$_state"
      echo "Uploaded ${_pos[0]:-} to ${_pos[1]:-}" >&2
      exit 0 ;;
    ls)
      if [[ "${EXAPUMP_BFS_LS_FAIL:-0}" == "1" ]]; then
        echo "Error: BucketFS is not reachable at stub-bfs-host:2581" >&2
        exit 1
      fi
      _prefix="${_pos[0]:-}"
      if [[ -z "$_prefix" ]]; then
        # Top-level probe: always succeeds, even against an empty bucket.
        if [[ -f "$_state" ]]; then
          while IFS= read -r _e; do [[ -n "$_e" ]] && printf '%s\n' "${_e%%/*}"; done < "$_state"
        fi
        exit 0
      fi
      if [[ "${EXAPUMP_BFS_NEVER_LIST:-0}" == "1" ]]; then exit 1; fi
      # Simulate BucketFS's asynchronous unpack: the first N path listings find nothing.
      _delay="${EXAPUMP_BFS_LS_DELAY:-0}"
      if [[ "$_delay" -gt 0 ]]; then
        _cf="$_state.delay"
        _n=0; [[ -f "$_cf" ]] && _n="$(cat "$_cf")"
        _n=$((_n + 1)); printf '%s' "$_n" > "$_cf"
        if [[ "$_n" -le "$_delay" ]]; then exit 1; fi
      fi
      [[ -f "$_state" ]] || exit 1
      _found=0
      while IFS= read -r _e; do
        case "$_e" in
          "$_prefix"/*) _rest="${_e#"$_prefix"/}"; printf '%s\n' "${_rest%%/*}"; _found=1 ;;
        esac
      done < "$_state"
      [[ "$_found" -eq 1 ]] || exit 1
      exit 0 ;;
    *) exit 0 ;;
  esac
fi

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
    */Cargo.toml)
      if [[ "${CURL_CARGO_TOML_FAIL:-0}" == "1" ]]; then
        echo "curl: (22) The requested URL returned error: 404" >&2
        exit 22
      fi
      if [[ "${GH_CARGO_TOML_MISSING_PIN:-0}" == "1" ]]; then
        printf '[workspace.dependencies]\niceberg = "0.10.0"\n'
        exit 0
      fi
      # Includes a decoy comment mentioning "exasol-udf-sdk" ahead of the real pin, and another
      # package's "= \"...\"" line in between, exactly like the real root Cargo.toml -- a version
      # extractor that isn't anchored to the start of the exasol-udf-sdk line (e.g. a greedy
      # multi-line regex) would latch onto the decoy or the other package's version instead.
      cat <<TOML
[workspace.dependencies]
# internally, the same tree as datafusion/exasol-udf-sdk below -- the workspace
iceberg = "0.10.0"
exasol-udf-sdk = { version = "${GH_ENGINE_SDK_VERSION:-0.21.0}", features = ["emit-arrow"] }
TOML
      exit 0 ;;
    */releases/download/*)
      if [[ "${GH_ASSET_MISSING:-0}" == "1" ]]; then
        echo "curl: (22) The requested URL returned error: 404" >&2
        exit 22
      fi
      if [[ -n "$outfile" ]]; then
        if [[ -n "${GH_ASSET_TARBALL:-}" ]]; then
          cat "$GH_ASSET_TARBALL" > "$outfile"
        else
          printf 'stub-asset-bytes\n' > "$outfile"
        fi
      fi
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

write_ssh_stub() {
  cat > "$1/ssh" <<'STUB'
#!/usr/bin/env bash
printf 'ssh %s\n' "$*" >> "${STUB_LOG:-/dev/null}"
if [[ "${SSH_FAIL:-0}" == "1" ]]; then
  echo "ssh: connect to host 127.0.0.1 port 52341: Connection refused" >&2
  exit 255
fi
_cmd="${!#}"
case "$_cmd" in
  "test -e "*)
    if [[ "${SSH_PATH_NEVER:-0}" == "1" ]]; then exit 1; fi
    _delay="${SSH_PATH_DELAY:-0}"
    if [[ "$_delay" -gt 0 ]]; then
      _cf="${STUB_SSH_STATE:-/dev/null}.delay"
      _n=0; [[ -f "$_cf" ]] && _n="$(cat "$_cf")"
      _n=$((_n + 1)); printf '%s' "$_n" > "$_cf"
      if [[ "$_n" -le "$_delay" ]]; then exit 1; fi
    fi
    exit 0 ;;
esac
exit 0
STUB
  chmod +x "$1/ssh"
}

write_scp_stub() {
  cat > "$1/scp" <<'STUB'
#!/usr/bin/env bash
printf 'scp %s\n' "$*" >> "${STUB_LOG:-/dev/null}"
if [[ "${SCP_FAIL:-0}" == "1" ]]; then
  echo "scp: transfer to the deployment VM failed: Permission denied" >&2
  exit 1
fi
exit 0
STUB
  chmod +x "$1/scp"
}

write_exapump_stub "$STUBDIR"
write_curl_stub "$STUBDIR"
write_ssh_stub "$STUBDIR"
write_scp_stub "$STUBDIR"
# missing-curl dir: exapump only (no curl)
write_exapump_stub "$MISSING_CURL_DIR"
# missing-exapump dir: curl only (no exapump)
write_curl_stub "$MISSING_EXAPUMP_DIR"
for _d in "$MISSING_SSH_DIR" "$MISSING_SCP_DIR"; do
  write_exapump_stub "$_d"
  write_curl_stub "$_d"
  _p="$(command -v tar 2>/dev/null)" && ln -sf "$_p" "$_d/tar"
done
write_scp_stub "$MISSING_SSH_DIR"
write_ssh_stub "$MISSING_SCP_DIR"
unset _d _p

RUN_PATH="$STUBDIR:$ORIG_PATH"

EXAPUMP_CONFIG_FIXTURE="$SANDBOX/exapump-config.toml"
cat > "$EXAPUMP_CONFIG_FIXTURE" <<'TOML'
[other]
host = "decoy"
password = "DECOY_SHOULD_NEVER_BE_USED"

[staging]
host = "decoy-host"
password = "SECRETPAT123"

[bfsprofile]
host = "bfs-decoy-host"
password = "SECRETPAT123"
bfs_write_password = "BFSWRITEPW789"

[bfsprofile-custom-bucket]
host = "bfs-decoy-host"
password = "SECRETPAT123"
bfs_write_password = "BFSWRITEPW789"
bfs_bucket = "custom"
TOML

# --- engine-archive fixtures --------------------------------------------------
# GOOD: contains udf/liblakehouse_engine.so, the member the BucketFS target extracts and uploads.
# BAD:  a well-formed .tar.gz WITHOUT that member, to prove extract_engine_so names what is absent.
mkdir -p "$SANDBOX/fixture-good/udf" "$SANDBOX/fixture-bad/other"
printf 'fake-elf-bytes\n' > "$SANDBOX/fixture-good/udf/liblakehouse_engine.so"
printf 'unrelated\n' > "$SANDBOX/fixture-bad/other/readme.txt"
ENGINE_TARBALL_GOOD="$SANDBOX/engine-good.tar.gz"
ENGINE_TARBALL_BAD="$SANDBOX/engine-bad.tar.gz"
tar -czf "$ENGINE_TARBALL_GOOD" -C "$SANDBOX/fixture-good" udf
tar -czf "$ENGINE_TARBALL_BAD" -C "$SANDBOX/fixture-bad" other

write_local_deployment_fixture() {
  local dir="$1"
  mkdir -p "$dir/local"
  printf '{"backend":"local","connection":{"host":"127.0.0.1","sshPort":52341,"dbPort":8563,"username":"sys"}}\n' \
    > "$dir/deployment.json"
  printf '{"dbPassword":"fixture-secret"}\n' > "$dir/secrets.json"
  printf 'fake-node-key\n' > "$dir/local/node_access.pem"
  chmod 600 "$dir/local/node_access.pem"
}

write_local_deployment_fixture_custom_connection() {
  local dir="$1"
  mkdir -p "$dir/local"
  printf '{"backend":"local","connection":{"host":"descriptor.example","sshPort":52341,"dbPort":52164,"username":"dbadmin"}}\n' \
    > "$dir/deployment.json"
  printf '{"dbPassword":"fixture-secret"}\n' > "$dir/secrets.json"
  printf 'fake-node-key\n' > "$dir/local/node_access.pem"
  chmod 600 "$dir/local/node_access.pem"
}

write_cloud_deployment_fixture() {
  local dir="$1" backend="$2"
  mkdir -p "$dir"
  printf '{"backend":"%s","connection":{"host":"cloud.example","dbPort":8563,"username":"sys"}}\n' \
    "$backend" > "$dir/deployment.json"
  printf '{"dbPassword":"fixture-secret"}\n' > "$dir/secrets.json"
}

UNAME_ARM64_DIR="$SANDBOX/uname-arm64"
mkdir -p "$UNAME_ARM64_DIR"
cat > "$UNAME_ARM64_DIR/uname" <<'STUB'
#!/usr/bin/env bash
if [[ "${1:-}" == "-m" ]]; then printf 'arm64\n'; else printf '\n'; fi
STUB
chmod +x "$UNAME_ARM64_DIR/uname"

UNAME_PPC64LE_DIR="$SANDBOX/uname-ppc64le"
mkdir -p "$UNAME_PPC64LE_DIR"
cat > "$UNAME_PPC64LE_DIR/uname" <<'STUB'
#!/usr/bin/env bash
if [[ "${1:-}" == "-m" ]]; then printf 'ppc64le\n'; else printf '\n'; fi
STUB
chmod +x "$UNAME_PPC64LE_DIR/uname"

NO_JQ_DIR="$SANDBOX/no-jq"
mkdir -p "$NO_JQ_DIR"

# --- a PATH with no `tar` -----------------------------------------------------
# A symlink farm holding exactly the external commands install.sh needs, minus tar. Used as the
# WHOLE PATH so `command -v tar` genuinely fails while everything else still works.
NOTAR_DIR="$SANDBOX/no-tar"
mkdir -p "$NOTAR_DIR"
write_exapump_stub "$NOTAR_DIR"
write_curl_stub "$NOTAR_DIR"
# `bash` is in the farm because the stubs' `#!/usr/bin/env bash` shebang resolves it through PATH.
for _c in bash env mktemp mv rm cat tr cut mkdir sleep; do
  _p="$(command -v "$_c" 2>/dev/null)" && ln -sf "$_p" "$NOTAR_DIR/$_c"
done
unset _c _p

STUB_BFS_STATE="$SANDBOX/bfs-state.txt"
export STUB_BFS_STATE

STUB_SSH_STATE="$SANDBOX/ssh-state.txt"
export STUB_SSH_STATE

DEPLOYMENT_NODE_KEY="$SANDBOX/node_access.pem"
printf 'fake-node-key\n' > "$DEPLOYMENT_NODE_KEY"
chmod 600 "$DEPLOYMENT_NODE_KEY"

reset_env() {
  unset GH_ENGINE_TAG GH_SLC_TAG GH_ASSET_MISSING GH_ASSET_TARBALL 2>/dev/null || true
  unset EXAPUMP_SMOKE_MODE EXAPUMP_ALTER_FAIL EXAPUMP_DDL_FAIL EXAPUMP_SCRIPT_LANGUAGES EXAPUMP_SL_EMPTY 2>/dev/null || true
  unset EXAPUMP_BFS_CP_FAIL EXAPUMP_BFS_LS_FAIL EXAPUMP_BFS_NEVER_LIST EXAPUMP_BFS_LS_DELAY 2>/dev/null || true
  unset SSH_FAIL SCP_FAIL SSH_PATH_NEVER SSH_PATH_DELAY 2>/dev/null || true
  unset CURL_POST_FAIL CURL_POST_URL_ESCAPED CURL_PUT_TRANSPORT_FAIL CURL_PUT_HTTP_CODE CURL_PUT_BODY CURL_LIST_MISSING CURL_LIST_SUFFIX_ONLY CURL_DB_UNREACHABLE 2>/dev/null || true
  unset EXAPUMP_DSN STUB_REPORT_STDIN 2>/dev/null || true
  # Sandboxed exapump config so profile-mode runs never touch the real ~/.exapump/config.toml.
  export EXAPUMP_CONFIG="$EXAPUMP_CONFIG_FIXTURE"
  RUN_PATH="$STUBDIR:$ORIG_PATH"
  : > "$STUB_LOG"
  : > "$STUB_BFS_STATE"
  rm -f "$STUB_BFS_STATE.delay" "$STUB_SSH_STATE.delay"
}

run_file() {
  LAST_OUT="$( ( export PATH="$RUN_PATH"; exec "$BASH_BIN" "$INSTALLER" "$@" ) 2>&1 )"
  LAST_RC=$?
}

run_stdin() {
  # shellcheck disable=SC2002  # deliberate: this models the real `curl | bash` one-liner as a
  # PIPE, not a redirect -- a redirect would still feed stdin correctly, but it would not be
  # testing the pipe shape the docs actually tell users to run.
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

# BucketFS-target happy path: no SaaS ids at all, and a profile that carries bfs_write_password.
BFS_HAPPY_ARGS=(--profile bfsprofile)

BFS_RUST_SEGMENT="RUST=localzmq+protobuf:///bfsdefault/default/slc/lakehouse-rustslc?lang=rust#buckets/bfsdefault/default/slc/lakehouse-rustslc/exaudf/exaudfclient"
BFS_SO_UDF_OBJECT="buckets/bfsdefault/default/udf/liblakehouse_engine.so"

# Runs the installer against the BucketFS target with a real engine-archive fixture in place.
run_file_bfs() {
  export GH_ASSET_TARBALL="$ENGINE_TARBALL_GOOD"
  run_file "$@"
}

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

test_read_profile_key() {
  echo "== test_read_profile_key =="
  local pw rc

  pw="$( source "$INSTALLER"; read_profile_key "staging" password "$EXAPUMP_CONFIG_FIXTURE" )"
  assert_eq "read_profile_key: resolves staging's password" "SECRETPAT123" "$pw"
  assert_not_contains "read_profile_key: never returns the other section's decoy" \
    "$pw" "DECOY_SHOULD_NEVER_BE_USED"

  pw="$( source "$INSTALLER"; read_profile_key "no-such-profile" password "$EXAPUMP_CONFIG_FIXTURE" )"
  rc=$?
  assert_rc_nonzero "read_profile_key: unknown profile fails" "$rc"
  assert_eq "read_profile_key: no output for unknown profile" "" "$pw"

  pw="$( source "$INSTALLER"; read_profile_key "staging" password "$SANDBOX/does-not-exist.toml" )"
  rc=$?
  assert_rc_nonzero "read_profile_key: nonexistent config file fails" "$rc"
  assert_eq "read_profile_key: no output for nonexistent config file" "" "$pw"

  local no_password_fixture="$SANDBOX/no-password-config.toml"
  cat > "$no_password_fixture" <<'TOML'
[nopass]
host = "some-host"

[staging]
host = "decoy-host"
password = "SECRETPAT123"
TOML
  pw="$( source "$INSTALLER"; read_profile_key "nopass" password "$no_password_fixture" )"
  rc=$?
  assert_rc_nonzero "read_profile_key: section with no password key before the next section fails" "$rc"
  assert_eq "read_profile_key: no output when the password key is missing" "" "$pw"
}

test_resolve_saas_pat_per_mode() {
  echo "== test_resolve_saas_pat_per_mode =="
  local result rc

  result="$(
    source "$INSTALLER"
    CONNECTIVITY_MODE="host"
    ARG_PASSWORD="SECRETPW456"
    if resolve_saas_pat; then printf 'ok:%s\n' "$RESOLVED_PAT"; else printf 'fail\n'; fi
  )"
  assert_eq "resolve_saas_pat: host mode derives from ARG_PASSWORD" "ok:SECRETPW456" "$result"

  result="$(
    source "$INSTALLER"
    CONNECTIVITY_MODE="dsn"
    ARG_DSN="exasol://user:SECRETPAT123@host:8563"
    if resolve_saas_pat; then printf 'ok:%s\n' "$RESOLVED_PAT"; else printf 'fail\n'; fi
  )"
  assert_eq "resolve_saas_pat: dsn mode derives from the DSN password segment" "ok:SECRETPAT123" "$result"

  result="$(
    source "$INSTALLER"
    CONNECTIVITY_MODE="profile"
    ARG_PROFILE="staging"
    EXAPUMP_CONFIG="$EXAPUMP_CONFIG_FIXTURE"
    if resolve_saas_pat; then printf 'ok:%s\n' "$RESOLVED_PAT"; else printf 'fail\n'; fi
  )"
  assert_eq "resolve_saas_pat: profile mode derives from the exapump config fixture" "ok:SECRETPAT123" "$result"

  local dsn_err
  dsn_err="$(
    source "$INSTALLER"
    CONNECTIVITY_MODE="dsn"
    ARG_DSN="exasol://host:8563"
    resolve_saas_pat 2>&1
  )"
  rc=$?
  assert_rc_nonzero "resolve_saas_pat: dsn mode fails without a password segment" "$rc"
  assert_not_contains "resolve_saas_pat: dsn failure message names no credential value" "$dsn_err" "SECRETPAT123"

  local profile_err
  profile_err="$(
    source "$INSTALLER"
    CONNECTIVITY_MODE="profile"
    ARG_PROFILE="no-such-profile"
    EXAPUMP_CONFIG="$EXAPUMP_CONFIG_FIXTURE"
    resolve_saas_pat 2>&1
  )"
  rc=$?
  assert_rc_nonzero "resolve_saas_pat: profile mode fails for an unknown profile" "$rc"
  assert_not_contains "resolve_saas_pat: unknown-profile failure never leaks the fixture password" \
    "$profile_err" "SECRETPAT123"
  assert_not_contains "resolve_saas_pat: unknown-profile failure never leaks the decoy password" \
    "$profile_err" "DECOY_SHOULD_NEVER_BE_USED"
}

test_resolve_target_mode_partial_saas_ids() {
  echo "== test_resolve_target_mode_partial_saas_ids =="
  local mode rc out

  mode="$( source "$INSTALLER"; ARG_ACCOUNT_ID=ACC1; ARG_DATABASE_ID=DB1; resolve_target_mode )"
  rc=$?
  assert_rc_zero "resolve_target_mode: both SaaS ids resolve a mode" "$rc"
  assert_eq "resolve_target_mode: both SaaS ids resolve to saas" "saas" "$mode"

  out="$( source "$INSTALLER"; ARG_ACCOUNT_ID=ACC1; ARG_DATABASE_ID=""; resolve_target_mode 2>&1 )"
  rc=$?
  assert_rc_nonzero "resolve_target_mode: only --account-id fails" "$rc"
  assert_contains "resolve_target_mode: partial-ids error names --account-id" "$out" "--account-id"
  assert_contains "resolve_target_mode: partial-ids error names --database-id" "$out" "--database-id"

  out="$( source "$INSTALLER"; ARG_ACCOUNT_ID=""; ARG_DATABASE_ID=DB1; resolve_target_mode 2>&1 )"
  rc=$?
  assert_rc_nonzero "resolve_target_mode: only --database-id fails" "$rc"
  assert_contains "resolve_target_mode: partial-ids error names --account-id" "$out" "--account-id"
  assert_contains "resolve_target_mode: partial-ids error names --database-id" "$out" "--database-id"
}

test_resolve_target_layout_saas_values() {
  echo "== test_resolve_target_layout_saas_values =="
  local so segment
  so="$( source "$INSTALLER"; resolve_target_layout; printf '%s' "$TARGET_SO_UDF_OBJECT" )"
  assert_eq "resolve_target_layout: saas TARGET_SO_UDF_OBJECT is the SaaS bucket .so path" \
    "/buckets/uploads/default/lakehouse-engine/udf/liblakehouse_engine.so" "$so"

  segment="$( source "$INSTALLER"; resolve_target_layout; printf '%s' "$TARGET_RUST_LANG_SEGMENT" )"
  assert_eq "resolve_target_layout: saas TARGET_RUST_LANG_SEGMENT is the SaaS RUST alias" \
    "$RUST_SEGMENT" "$segment"
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
    export PATH="$STUBDIR:$ORIG_PATH" STUB_LOG GH_ENGINE_TAG="v1.2.3" GH_ENGINE_SDK_VERSION="4.5.6"
    source "$INSTALLER"
    ARG_LAKEHOUSE_VERSION=""; ARG_SLC_VERSION=""
    resolve_versions
  )"
  assert_contains "default: resolves latest engine tag" "$out" "1.2.3"
  assert_contains "default: resolves SLC version from the engine release's Cargo.toml pin" "$out" "4.5.6"
  assert_contains "default: prints engine version line" "$out" "Resolved lakehouse-engine version"
  assert_contains "default: prints SLC version line" "$out" "Resolved language-container (SLC) version"
  local log; log="$(log_content)"
  assert_contains "default: curl hits the engine releases/latest endpoint" "$log" "releases/latest"
  assert_contains "default: curl fetches the engine's Cargo.toml at the resolved tag" \
    "$log" "v1.2.3/Cargo.toml"
  assert_not_contains "default: never queries language-container-rs's own latest release" \
    "$log" "language-container-rs/releases/latest"

  reset_env
  out="$(
    export PATH="$STUBDIR:$ORIG_PATH" STUB_LOG GH_ENGINE_TAG="v1.2.3" GH_ENGINE_SDK_VERSION="4.5.6"
    source "$INSTALLER"
    ARG_LAKEHOUSE_VERSION="9.9.9"; ARG_SLC_VERSION="8.8.8"
    resolve_versions
  )"
  assert_contains "override: uses engine override" "$out" "9.9.9"
  assert_contains "override: uses SLC override" "$out" "8.8.8"
  assert_not_contains "override: ignores latest engine tag" "$out" "1.2.3"
  assert_not_contains "override: ignores engine release's Cargo.toml SLC pin" "$out" "4.5.6"
  log="$(log_content)"
  assert_not_contains "override: skips the releases/latest fetch entirely" "$log" "releases/latest"
  assert_not_contains "override: skips the Cargo.toml pin lookup entirely" "$log" "contents/Cargo.toml"
}

test_slc_version_defaults_to_engine_pin_not_slc_latest() {
  echo "== test_slc_version_defaults_to_engine_pin_not_slc_latest =="
  # Regression test for #305: language-container-rs published v0.21.1 with no matching
  # lakehouse-engine-rs release, and the installer's old default (query language-container-rs's
  # own "latest" release) resolved v0.21.1 as the SLC version even though the latest ENGINE
  # release was still built and fingerprinted against v0.21.0 -- a guaranteed fingerprint
  # mismatch. The default must track the engine release's own exasol-udf-sdk pin instead, even
  # when the two repos' "latest" releases disagree.
  reset_env
  local out
  out="$(
    export PATH="$STUBDIR:$ORIG_PATH" STUB_LOG \
      GH_ENGINE_TAG="v0.32.1" GH_ENGINE_SDK_VERSION="0.21.0" GH_SLC_TAG="v0.21.1"
    source "$INSTALLER"
    ARG_LAKEHOUSE_VERSION=""; ARG_SLC_VERSION=""
    resolve_versions
  )"
  assert_contains "drift: resolves the engine release's own SDK pin as the SLC version" "$out" "0.21.0"
  assert_not_contains "drift: does not resolve language-container-rs's independently-newer latest release" \
    "$out" "0.21.1"
  local log; log="$(log_content)"
  assert_not_contains "drift: never queries language-container-rs's own latest release" \
    "$log" "language-container-rs/releases/latest"
}

test_slc_version_pin_lookup_failure_modes() {
  echo "== test_slc_version_pin_lookup_failure_modes =="
  reset_env
  local out rc
  out="$(
    export PATH="$STUBDIR:$ORIG_PATH" STUB_LOG GH_ENGINE_TAG="v1.2.3" CURL_CARGO_TOML_FAIL=1
    source "$INSTALLER"
    ARG_LAKEHOUSE_VERSION=""; ARG_SLC_VERSION=""
    resolve_versions 2>&1
  )"
  rc=$?
  assert_rc_nonzero "fetch failure: resolve_versions fails" "$rc"
  assert_contains "fetch failure: error names Cargo.toml" "$out" "Cargo.toml"
  assert_contains "fetch failure: error suggests --slc-version as the escape hatch" "$out" "--slc-version"

  reset_env
  out="$(
    export PATH="$STUBDIR:$ORIG_PATH" STUB_LOG GH_ENGINE_TAG="v1.2.3" GH_CARGO_TOML_MISSING_PIN=1
    source "$INSTALLER"
    ARG_LAKEHOUSE_VERSION=""; ARG_SLC_VERSION=""
    resolve_versions 2>&1
  )"
  rc=$?
  assert_rc_nonzero "missing pin: resolve_versions fails" "$rc"
  assert_contains "missing pin: error names exasol-udf-sdk" "$out" "exasol-udf-sdk"
  assert_contains "missing pin: error suggests --slc-version as the escape hatch" "$out" "--slc-version"
}

test_script_languages_append_preserves_existing() {
  echo "== test_script_languages_append_preserves_existing =="
  local out
  out="$( source "$INSTALLER"; compute_script_languages "PYTHON3=builtin_python3 JAVA=builtin_java" "$RUST_SEGMENT" )"
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
  out="$( source "$INSTALLER"; compute_script_languages "PYTHON3=p RUST=stale_alias JAVA=j" "$RUST_SEGMENT" )"
  assert_eq "replace: in place, non-RUST unchanged" \
    "PYTHON3=p $RUST_SEGMENT JAVA=j" "$out"
  assert_eq "replace: exactly one RUST entry" "1" "$(count_occurrences 'RUST=' "$out")"
  # Idempotency: feeding the result back yields an identical value.
  out2="$( source "$INSTALLER"; compute_script_languages "$out" "$RUST_SEGMENT" )"
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
  assert_contains "upload: downloads assets via direct github.com URL" "$log" "releases/download/"
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
  # shellcheck disable=SC1003  # each '\' argument is a literal one-character backslash string,
  # not an escape attempt -- %s substitutes it to build a literal "&" in the fixture JSON.
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
  assert_rc_zero "asset download: install succeeds" "$LAST_RC"
  local log; log="$(log_content)"
  assert_contains "asset download: uses direct github.com/releases/download URL" "$log" "releases/download/"
  assert_not_contains "asset download: no API-based asset lookup" "$log" "releases/assets/"

  reset_env
  export GH_ASSET_MISSING=1
  local out rc
  out="$(
    export PATH="$STUBDIR:$ORIG_PATH" STUB_LOG GH_ASSET_MISSING
    source "$INSTALLER"
    download_release_asset "$ENGINE_REPO" "v1.2.3" "$ENGINE_ASSET" "$SANDBOX/does-not-matter" 2>&1
  )"
  rc=$?
  assert_rc_nonzero "asset download: missing asset fails non-zero" "$rc"
  assert_contains "asset download: error names the repo" "$out" "exasol-labs/lakehouse-engine-rs"
  assert_contains "asset download: error names the tag" "$out" "v1.2.3"
  assert_contains "asset download: error names the asset" "$out" "lakehouse-engine.tar.gz"
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

test_three_scripts_ddl_saas_path_types() {
  echo "== test_three_scripts_ddl_saas_path_types =="
  # Unit: DDL string shapes.
  local scan dist adapter schema
  scan="$( source "$INSTALLER"; ddl_scan LHVS /buckets/uploads/default/lakehouse-engine/udf/liblakehouse_engine.so )"
  assert_contains "scan is RUST SCALAR" "$scan" "RUST SCALAR SCRIPT"
  assert_contains "scan uses dynamic EMITS" "$scan" "EMITS (...)"
  assert_not_contains "scan is never a SET script" "$scan" "SET SCRIPT"
  dist="$( source "$INSTALLER"; ddl_distribute_files LHVS )"
  assert_contains "distribute is LUA SET" "$dist" "LUA SET SCRIPT"
  adapter="$( source "$INSTALLER"; ddl_adapter LHVS /buckets/uploads/default/lakehouse-engine/udf/liblakehouse_engine.so )"
  assert_contains "adapter references SaaS %udf_object path" "$adapter" "/buckets/uploads/default/lakehouse-engine/udf/liblakehouse_engine.so"

  # Integration: the three scripts are actually created + CREATE SCHEMA IF NOT EXISTS.
  reset_env
  run_file "${HAPPY_ARGS[@]}"
  assert_rc_zero "ddl: install succeeds" "$LAST_RC"
  local log; log="$(log_content)"
  assert_contains "ddl: CREATE SCHEMA IF NOT EXISTS" "$log" "CREATE SCHEMA IF NOT EXISTS LHVS"
  assert_contains "ddl: LAKEHOUSE_ADAPTER RUST ADAPTER" "$log" "RUST ADAPTER SCRIPT LHVS.LAKEHOUSE_ADAPTER"
  assert_contains "ddl: LAKEHOUSE_SCAN RUST SCALAR" "$log" "RUST SCALAR SCRIPT LHVS.LAKEHOUSE_SCAN"
  assert_contains "ddl: dynamic EMITS on scan" "$log" "EMITS (...)"
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
  assert_contains "template: prints the NAMESPACE property" "$LAST_OUT" "NAMESPACE          = "
  assert_contains "template: references the adapter script" "$LAST_OUT" "LHVS.LAKEHOUSE_ADAPTER"
  local log; log="$(log_content)"
  assert_not_contains "template: does NOT execute CREATE VIRTUAL SCHEMA" "$log" "CREATE VIRTUAL SCHEMA"
  assert_not_contains "template: does NOT execute CREATE CONNECTION" "$log" "CREATE OR REPLACE CONNECTION"
}

test_next_step_template_emits_the_scan_script_grant_and_the_replace_warning() {
  echo "== test_next_step_template_emits_the_scan_script_grant_and_the_replace_warning =="
  reset_env
  run_file "${HAPPY_ARGS[@]}"
  assert_rc_zero "grant template: install succeeds" "$LAST_RC"
  assert_contains "grant template: creates the deployment-scoped role" "$LAST_OUT" "CREATE ROLE LAKEHOUSE_ENGINE_ROLE_LHVS;"

  # BOTH scripts resolve the CONNECTION and BOTH therefore need the grant. The adapter's was
  # missing until task 1.3b's live probe showed a non-DBA owner cannot even create the virtual
  # schema without it.
  assert_contains "grant template: grants script-scoped connection access to the ADAPTER" "$LAST_OUT" \
    "GRANT ACCESS ON CONNECTION LAKEHOUSE_CATALOG_CREDS FOR SCRIPT LHVS.LAKEHOUSE_ADAPTER TO LAKEHOUSE_ENGINE_ROLE_LHVS;"
  assert_contains "grant template: grants script-scoped connection access to the SCAN script" "$LAST_OUT" \
    "GRANT ACCESS ON CONNECTION LAKEHOUSE_CATALOG_CREDS FOR SCRIPT LHVS.LAKEHOUSE_SCAN TO LAKEHOUSE_ENGINE_ROLE_LHVS;"

  # The adapter resolves the CONNECTION while CREATE VIRTUAL SCHEMA runs, so an operator copying
  # the template top-to-bottom must reach the grant first.
  assert_precedes "grant template: the adapter grant precedes CREATE VIRTUAL SCHEMA" "$LAST_OUT" \
    "FOR SCRIPT LHVS.LAKEHOUSE_ADAPTER TO LAKEHOUSE_ENGINE_ROLE_LHVS;" "CREATE VIRTUAL SCHEMA <MY_LAKEHOUSE>"
  assert_precedes "grant template: the scan grant precedes CREATE VIRTUAL SCHEMA" "$LAST_OUT" \
    "FOR SCRIPT LHVS.LAKEHOUSE_SCAN TO LAKEHOUSE_ENGINE_ROLE_LHVS;" "CREATE VIRTUAL SCHEMA <MY_LAKEHOUSE>"

  # The grantee is the VIRTUAL SCHEMA OWNER, not each querying user (task 1.3b, verified live in
  # both directions). Adding a reader is plain RBAC and involves no connection grant at all.
  assert_contains "grant template: names the virtual schema OWNER as the grantee" "$LAST_OUT" \
    "against the VIRTUAL SCHEMA OWNER,"
  assert_contains "grant template: says the check is NOT per querying user" "$LAST_OUT" \
    "not against each querying user"
  assert_contains "grant template: holds the role on the eventual VS owner" "$LAST_OUT" \
    "will OWN the virtual schema"
  assert_contains "grant template: adding a reader is a plain SELECT grant" "$LAST_OUT" \
    "GRANT SELECT ON SCHEMA <MY_LAKEHOUSE> TO <new-user>;"
  assert_contains "grant template: the role goes to a further VS owner, not a reader" "$LAST_OUT" \
    "GRANT LAKEHOUSE_ENGINE_ROLE_LHVS TO <other-vs-owner>;"
  assert_not_contains "grant template: does NOT frame the role as a per-reader grant" "$LAST_OUT" \
    "GRANT LAKEHOUSE_ENGINE_ROLE_LHVS TO <new-user>;"

  # A DBA owner needs none of this, and Exasol refuses both GRANT forms to SYS outright
  # (SQL state 42500, verified live) -- so the template must say so rather than printing a
  # block that fails when the installer runs as SYS.
  assert_contains "grant template: warns that both GRANT forms are refused for SYS" "$LAST_OUT" \
    "cannot grant connections to SYS"
  assert_contains "grant template: names the non-DBA owner's CREATE VIRTUAL SCHEMA privilege" "$LAST_OUT" \
    "CREATE VIRTUAL SCHEMA system privilege"

  assert_contains "grant template: CREATE ROLE has no IF NOT EXISTS warning" "$LAST_OUT" "NO 'IF NOT EXISTS' form"
  assert_contains "grant template: names the EXA_ALL_ROLES pre-check" "$LAST_OUT" "EXA_ALL_ROLES"
  assert_contains "grant template: CREATE OR REPLACE CONNECTION drops the grant warning" "$LAST_OUT" \
    "CREATE OR REPLACE CONNECTION LAKEHOUSE_CATALOG_CREDS"
  assert_contains "grant template: CREATE OR REPLACE SCRIPT also drops the grant" "$LAST_OUT" \
    "CREATE OR REPLACE SCRIPT LHVS.LAKEHOUSE_SCAN"
  local log; log="$(log_content)"
  assert_not_contains "grant template: does NOT execute CREATE ROLE" "$log" "CREATE ROLE"
  assert_not_contains "grant template: does NOT execute the connection grant" "$log" "GRANT ACCESS ON CONNECTION"

  # Profile connectivity mode carries no known ARG_USER: the template falls back to a placeholder
  # rather than printing an empty grantee.
  assert_contains "grant template: falls back to a placeholder installing-user under profile mode" "$LAST_OUT" \
    "GRANT LAKEHOUSE_ENGINE_ROLE_LHVS TO <installing-user>;"

  # Host connectivity mode DOES know the installing user: it names it, not the placeholder.
  reset_env
  run_file --account-id ACC1 --database-id DB1 --host myhost:8563 --user grantee_user --password p
  assert_rc_zero "grant template (host mode): install succeeds" "$LAST_RC"
  assert_contains "grant template (host mode): grants the role to the actual installing user" "$LAST_OUT" \
    "GRANT LAKEHOUSE_ENGINE_ROLE_LHVS TO grantee_user;"
}

test_next_step_template_skips_the_grant_block_for_a_sys_installer() {
  echo "== test_next_step_template_skips_the_grant_block_for_a_sys_installer =="
  reset_env
  run_file --account-id ACC1 --database-id DB1 --host myhost:8563 --user sys --password p
  assert_rc_zero "sys installer: install succeeds" "$LAST_RC"
  assert_not_contains "sys installer: does not print CREATE ROLE" "$LAST_OUT" \
    "CREATE ROLE LAKEHOUSE_ENGINE_ROLE_LHVS;"
  assert_not_contains "sys installer: does not print the GRANT to sys" "$LAST_OUT" \
    "GRANT LAKEHOUSE_ENGINE_ROLE_LHVS TO sys;"
  assert_contains "sys installer: still names the refusal note" "$LAST_OUT" \
    "cannot grant roles to SYS"
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
  assert_contains "stdin-piped: release asset downloaded (releases/download)" "$log" "releases/download/"
  assert_contains "stdin-piped: SLC uploaded (body not truncated early)" "$log" "/files/rustslc.tar.gz"
  assert_contains "stdin-piped: three scripts created" "$log" "LHVS.LAKEHOUSE_DISTRIBUTE_FILES"
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
# BucketFS target mode
# ============================================================================

test_resolve_target_mode_bucketfs_autodetect() {
  echo "== test_resolve_target_mode_bucketfs_autodetect =="
  local mode rc out

  mode="$( source "$INSTALLER"; ARG_ACCOUNT_ID=""; ARG_DATABASE_ID=""; resolve_target_mode )"
  rc=$?
  assert_rc_zero "autodetect: neither SaaS id resolves a mode" "$rc"
  assert_eq "autodetect: neither SaaS id resolves to bucketfs (the default target)" "bucketfs" "$mode"

  mode="$( source "$INSTALLER"; ARG_ACCOUNT_ID=ACC1; ARG_DATABASE_ID=DB1; resolve_target_mode )"
  assert_eq "autodetect: both SaaS ids still resolve to saas" "saas" "$mode"

  # Exactly-one-set stays an error in both directions (unchanged behaviour).
  out="$( source "$INSTALLER"; ARG_ACCOUNT_ID=ACC1; ARG_DATABASE_ID=""; resolve_target_mode 2>&1 )"
  rc=$?
  assert_rc_nonzero "autodetect: exactly one id is still an error, never bucketfs" "$rc"
  assert_not_contains "autodetect: partial pair never silently becomes bucketfs" "$out" "bucketfs"
}

test_target_flag_conflict_detection() {
  echo "== test_target_flag_conflict_detection =="
  local mode rc out

  mode="$( source "$INSTALLER"; ARG_ACCOUNT_ID=""; ARG_DATABASE_ID=""; ARG_TARGET=bucketfs; resolve_target_mode )"
  rc=$?
  assert_rc_zero "--target: agreeing bucketfs assertion passes" "$rc"
  assert_eq "--target: agreeing bucketfs assertion yields bucketfs" "bucketfs" "$mode"

  mode="$( source "$INSTALLER"; ARG_ACCOUNT_ID=ACC1; ARG_DATABASE_ID=DB1; ARG_TARGET=saas; resolve_target_mode )"
  rc=$?
  assert_rc_zero "--target: agreeing saas assertion passes" "$rc"
  assert_eq "--target: agreeing saas assertion yields saas" "saas" "$mode"

  out="$( source "$INSTALLER"; ARG_ACCOUNT_ID=""; ARG_DATABASE_ID=""; ARG_TARGET=saas; resolve_target_mode 2>&1 )"
  rc=$?
  assert_rc_nonzero "--target saas with no ids: nonzero" "$rc"
  assert_contains "--target saas with no ids: names the flag" "$out" "--target saas"
  assert_contains "--target saas with no ids: names the detected mode" "$out" "'bucketfs'"

  out="$( source "$INSTALLER"; ARG_ACCOUNT_ID=ACC1; ARG_DATABASE_ID=DB1; ARG_TARGET=bucketfs; resolve_target_mode 2>&1 )"
  rc=$?
  assert_rc_nonzero "--target bucketfs with both ids: nonzero" "$rc"
  assert_contains "--target bucketfs with both ids: names the flag" "$out" "--target bucketfs"
  assert_contains "--target bucketfs with both ids: names the detected mode" "$out" "'saas'"

  out="$( source "$INSTALLER"; ARG_ACCOUNT_ID=""; ARG_DATABASE_ID=""; ARG_TARGET=nonsense; resolve_target_mode 2>&1 )"
  rc=$?
  assert_rc_nonzero "--target: an unknown value is rejected" "$rc"
  assert_contains "--target: unknown value lists the two valid ones" "$out" "'saas' or 'bucketfs'"

  # End to end through parse_args: the flag is accepted and takes a value.
  reset_env
  run_file --target saas "${HAPPY_ARGS[@]}"
  assert_rc_zero "--target saas: full saas run still succeeds" "$LAST_RC"

  reset_env
  run_file --target bucketfs "${HAPPY_ARGS[@]}"
  assert_rc_nonzero "--target bucketfs against SaaS ids: full run refuses" "$LAST_RC"
  assert_eq "--target conflict: no network call made" "" "$(log_content)"

  # --staging only means anything on a SaaS run; giving it with no SaaS ids is a mistake, not a
  # silently-ignored no-op.
  out="$( source "$INSTALLER"; ARG_ACCOUNT_ID=""; ARG_DATABASE_ID=""; ARG_STAGING=1; resolve_target_mode 2>&1 )"
  rc=$?
  assert_rc_nonzero "--staging with no SaaS ids: nonzero" "$rc"
  assert_contains "--staging with no SaaS ids: names --staging" "$out" "--staging"
  assert_contains "--staging with no SaaS ids: names the detected mode" "$out" "BucketFS"

  reset_env
  run_file --staging "${BFS_HAPPY_ARGS[@]}"
  assert_rc_nonzero "--staging against a full bucketfs run: refuses" "$LAST_RC"
  assert_eq "--staging conflict: no network call made" "" "$(log_content)"

  # Any --bfs-* flag only means anything on a BucketFS run; giving one alongside both SaaS ids is
  # a mistake, not a silently-ignored no-op.
  out="$( source "$INSTALLER"; ARG_ACCOUNT_ID=ACC1; ARG_DATABASE_ID=DB1; ARG_BFS_HOST=somehost; resolve_target_mode 2>&1 )"
  rc=$?
  assert_rc_nonzero "--bfs-host with both SaaS ids: nonzero" "$rc"
  assert_contains "--bfs-host with both SaaS ids: names --bfs-host" "$out" "--bfs-host"
  assert_contains "--bfs-host with both SaaS ids: names the detected mode" "$out" "SaaS"

  out="$( source "$INSTALLER"; ARG_ACCOUNT_ID=ACC1; ARG_DATABASE_ID=DB1; ARG_BFS_BUCKET=other; ARG_BFS_BUCKET_SET=1; resolve_target_mode 2>&1 )"
  rc=$?
  assert_rc_nonzero "--bfs-bucket with both SaaS ids: nonzero" "$rc"
  assert_contains "--bfs-bucket with both SaaS ids: names --bfs-bucket" "$out" "--bfs-bucket"

  # A --bfs-bucket left at its unset default must NOT be mistaken for an explicit flag.
  mode="$( source "$INSTALLER"; ARG_ACCOUNT_ID=ACC1; ARG_DATABASE_ID=DB1; resolve_target_mode )"
  rc=$?
  assert_rc_zero "saas with untouched --bfs-bucket default: still passes" "$rc"
  assert_eq "saas with untouched --bfs-bucket default: yields saas" "saas" "$mode"

  reset_env
  run_file --bfs-write-password whatever "${HAPPY_ARGS[@]}"
  assert_rc_nonzero "--bfs-write-password against a full saas run: refuses" "$LAST_RC"
  assert_eq "--bfs-write-password conflict: no network call made" "" "$(log_content)"
}

test_resolve_target_layout_bucketfs_values() {
  echo "== test_resolve_target_layout_bucketfs_values =="
  local so segment slc engine

  so="$( source "$INSTALLER"; TARGET_MODE=bucketfs; resolve_target_layout; printf '%s' "$TARGET_SO_UDF_OBJECT" )"
  assert_eq "layout bucketfs: %udf_object is the generic bfsdefault .so path" "$BFS_SO_UDF_OBJECT" "$so"

  segment="$( source "$INSTALLER"; TARGET_MODE=bucketfs; resolve_target_layout; printf '%s' "$TARGET_RUST_LANG_SEGMENT" )"
  assert_eq "layout bucketfs: RUST alias is the generic bfsdefault segment" "$BFS_RUST_SEGMENT" "$segment"

  slc="$( source "$INSTALLER"; TARGET_MODE=bucketfs; resolve_target_layout; printf '%s' "$TARGET_SLC_BFS_PATH" )"
  assert_eq "layout bucketfs: SLC upload path is bucket-relative (no bucket segment, no leading /)" \
    "slc/lakehouse-rustslc.tar.gz" "$slc"

  engine="$( source "$INSTALLER"; TARGET_MODE=bucketfs; resolve_target_layout; printf '%s' "$TARGET_ENGINE_BFS_PATH" )"
  assert_eq "layout bucketfs: engine upload path is bucket-relative" \
    "udf/liblakehouse_engine.so" "$engine"

  # --bfs-bucket propagates into every place the BUCKET NAME belongs: the %udf_object path and both
  # halves of the RUST alias. It must NOT appear in the exapump upload paths -- exapump selects the
  # bucket via --bfs-bucket and prefixes it onto the path itself.
  so="$( source "$INSTALLER"; TARGET_MODE=bucketfs; ARG_BFS_BUCKET=other; resolve_target_layout; printf '%s' "$TARGET_SO_UDF_OBJECT" )"
  assert_eq "layout --bfs-bucket other: %udf_object carries the bucket" \
    "buckets/bfsdefault/other/udf/liblakehouse_engine.so" "$so"

  segment="$( source "$INSTALLER"; TARGET_MODE=bucketfs; ARG_BFS_BUCKET=other; resolve_target_layout; printf '%s' "$TARGET_RUST_LANG_SEGMENT" )"
  assert_eq "layout --bfs-bucket other: RUST alias carries the bucket in both halves" \
    "RUST=localzmq+protobuf:///bfsdefault/other/slc/lakehouse-rustslc?lang=rust#buckets/bfsdefault/other/slc/lakehouse-rustslc/exaudf/exaudfclient" \
    "$segment"

  slc="$( source "$INSTALLER"; TARGET_MODE=bucketfs; ARG_BFS_BUCKET=other; resolve_target_layout; printf '%s' "$TARGET_SLC_BFS_PATH" )"
  assert_eq "layout --bfs-bucket other: SLC upload path stays bucket-relative" \
    "slc/lakehouse-rustslc.tar.gz" "$slc"

  # SaaS mode leaves the BucketFS-only globals empty.
  slc="$( source "$INSTALLER"; TARGET_MODE=saas; resolve_target_layout; printf '%s' "$TARGET_SLC_BFS_PATH" )"
  assert_eq "layout saas: no SLC BucketFS path" "" "$slc"
  engine="$( source "$INSTALLER"; TARGET_MODE=saas; resolve_target_layout; printf '%s' "$TARGET_ENGINE_BFS_PATH" )"
  assert_eq "layout saas: no engine BucketFS path" "" "$engine"
}

test_exapump_bfs_flags() {
  echo "== test_exapump_bfs_flags =="
  local flags

  # The bucket is ALWAYS emitted, even at its "default" default -- never left to exapump's own
  # bucket resolution, so a stray default profile in ~/.exapump/config.toml can't diverge from the
  # bucket path this script assumes when building TARGET_SO_UDF_OBJECT/TARGET_RUST_LANG_SEGMENT.
  flags="$( source "$INSTALLER"; exapump_bfs_flags )"
  assert_eq "bfs flags: nothing given -> only the resolved default bucket is emitted" "--bfs-bucket default" "$flags"

  flags="$( source "$INSTALLER"; ARG_BFS_BUCKET=default; ARG_BFS_BUCKET_SET=0; exapump_bfs_flags )"
  assert_eq "bfs flags: an unsupplied --bfs-bucket default is still echoed back" "--bfs-bucket default" "$flags"

  flags="$( source "$INSTALLER"; ARG_BFS_BUCKET=other; ARG_BFS_BUCKET_SET=1; exapump_bfs_flags )"
  assert_eq "bfs flags: an explicit --bfs-bucket is echoed back" "--bfs-bucket other" "$flags"

  flags="$(
    source "$INSTALLER"
    ARG_BFS_HOST=bfshost; ARG_BFS_PORT=2581; ARG_BFS_BUCKET=other; ARG_BFS_BUCKET_SET=1
    ARG_BFS_WRITE_PASSWORD=BFSWRITEPW789
    exapump_bfs_flags
  )"
  assert_eq "bfs flags: everything given is echoed back exactly, in flag order" \
    "--bfs-host bfshost --bfs-port 2581 --bfs-bucket other --bfs-write-password BFSWRITEPW789" "$flags"

  flags="$( source "$INSTALLER"; ARG_BFS_HOST=bfshost; exapump_bfs_flags )"
  assert_eq "bfs flags: the resolved bucket accompanies any other supplied subset" "--bfs-host bfshost --bfs-bucket default" "$flags"
}

test_resolve_bfs_bucket_from_profile() {
  echo "== test_resolve_bfs_bucket_from_profile =="
  local bucket

  # Profile names a non-default bucket, user gave no --bfs-bucket: must adopt the profile's
  # bucket, so TARGET_SO_UDF_OBJECT/TARGET_RUST_LANG_SEGMENT (built from ARG_BFS_BUCKET) end up
  # pointing at the SAME bucket exapump itself will upload into.
  bucket="$(
    source "$INSTALLER"
    TARGET_MODE=bucketfs; CONNECTIVITY_MODE=profile; ARG_PROFILE=bfsprofile-custom-bucket
    EXAPUMP_CONFIG="$EXAPUMP_CONFIG_FIXTURE"
    resolve_bfs_bucket_from_profile
    printf '%s' "$ARG_BFS_BUCKET"
  )"
  assert_eq "bucket drift: adopts the profile's bfs_bucket when none was given explicitly" "custom" "$bucket"

  # An explicit --bfs-bucket always wins, even if the profile names a different one.
  bucket="$(
    source "$INSTALLER"
    TARGET_MODE=bucketfs; CONNECTIVITY_MODE=profile; ARG_PROFILE=bfsprofile-custom-bucket
    EXAPUMP_CONFIG="$EXAPUMP_CONFIG_FIXTURE"
    ARG_BFS_BUCKET=explicit; ARG_BFS_BUCKET_SET=1
    resolve_bfs_bucket_from_profile
    printf '%s' "$ARG_BFS_BUCKET"
  )"
  assert_eq "bucket drift: an explicit --bfs-bucket is never overridden by the profile" "explicit" "$bucket"

  # A profile with no bfs_bucket field at all: default is left untouched.
  bucket="$(
    source "$INSTALLER"
    TARGET_MODE=bucketfs; CONNECTIVITY_MODE=profile; ARG_PROFILE=bfsprofile
    EXAPUMP_CONFIG="$EXAPUMP_CONFIG_FIXTURE"
    resolve_bfs_bucket_from_profile
    printf '%s' "$ARG_BFS_BUCKET"
  )"
  assert_eq "bucket drift: no-op when the profile has no bfs_bucket field" "default" "$bucket"

  # saas mode / dsn / host connectivity: always a no-op, regardless of profile content.
  bucket="$(
    source "$INSTALLER"
    TARGET_MODE=saas; CONNECTIVITY_MODE=profile; ARG_PROFILE=bfsprofile-custom-bucket
    EXAPUMP_CONFIG="$EXAPUMP_CONFIG_FIXTURE"
    resolve_bfs_bucket_from_profile
    printf '%s' "$ARG_BFS_BUCKET"
  )"
  assert_eq "bucket drift: no-op in saas mode" "default" "$bucket"

  bucket="$(
    source "$INSTALLER"
    TARGET_MODE=bucketfs; CONNECTIVITY_MODE=dsn
    resolve_bfs_bucket_from_profile
    printf '%s' "$ARG_BFS_BUCKET"
  )"
  assert_eq "bucket drift: no-op in dsn connectivity mode (no profile to read)" "default" "$bucket"

  # End-to-end proof: after resolution, resolve_target_layout builds paths in the ADOPTED bucket.
  local so
  so="$(
    source "$INSTALLER"
    TARGET_MODE=bucketfs; CONNECTIVITY_MODE=profile; ARG_PROFILE=bfsprofile-custom-bucket
    EXAPUMP_CONFIG="$EXAPUMP_CONFIG_FIXTURE"
    resolve_bfs_bucket_from_profile
    resolve_target_layout
    printf '%s' "$TARGET_SO_UDF_OBJECT"
  )"
  assert_eq "bucket drift: resolve_target_layout uses the profile-adopted bucket, not 'default'" \
    "buckets/bfsdefault/custom/udf/liblakehouse_engine.so" "$so"
}

test_bucketfs_upload_argv_shape() {
  echo "== test_bucketfs_upload_argv_shape =="
  # profile mode: --profile is present, and only the explicitly-given --bfs-* overrides follow.
  reset_env
  local rc
  rc="$(
    export PATH="$STUBDIR:$ORIG_PATH" STUB_LOG STUB_BFS_STATE
    source "$INSTALLER"
    CONNECTIVITY_MODE=profile; ARG_PROFILE=bfsprofile; TARGET_MODE=bucketfs
    if bucketfs_upload_file "/tmp/local.so" "udf/liblakehouse_engine.so" >/dev/null 2>&1; then echo 0; else echo 1; fi
  )"
  assert_eq "upload argv profile: succeeds" "0" "$rc"
  local log; log="$(log_content)"
  assert_contains "upload argv profile: exact cp shape" "$log" \
    "exapump bucketfs cp /tmp/local.so udf/liblakehouse_engine.so --profile bfsprofile"
  assert_not_contains "upload argv profile: never a raw curl PUT" "$log" "curl"

  # dsn mode: no --profile at all; the required explicit --bfs-* overrides carry the connection.
  reset_env
  rc="$(
    export PATH="$STUBDIR:$ORIG_PATH" STUB_LOG STUB_BFS_STATE
    source "$INSTALLER"
    CONNECTIVITY_MODE=dsn; ARG_DSN="exasol://u:p@h:8563"; TARGET_MODE=bucketfs
    ARG_BFS_HOST=bfshost; ARG_BFS_WRITE_PASSWORD=BFSWRITEPW789
    if bucketfs_upload_file "/tmp/local.so" "udf/liblakehouse_engine.so" >/dev/null 2>&1; then echo 0; else echo 1; fi
  )"
  assert_eq "upload argv dsn: succeeds" "0" "$rc"
  log="$(log_content)"
  assert_contains "upload argv dsn: exact cp shape with the bfs overrides" "$log" \
    "exapump bucketfs cp /tmp/local.so udf/liblakehouse_engine.so --bfs-host bfshost --bfs-bucket default --bfs-write-password BFSWRITEPW789"
  assert_not_contains "upload argv dsn: no --profile flag (there is no profile in dsn mode)" "$log" "--profile"
  assert_not_contains "upload argv dsn: exapump bucketfs is never given a -d/--dsn (it has no such flag)" "$log" "bucketfs cp /tmp/local.so udf/liblakehouse_engine.so -d"
}

test_bucketfs_upload_failure_surfaces_stderr() {
  echo "== test_bucketfs_upload_failure_surfaces_stderr =="
  reset_env
  local out
  out="$(
    export PATH="$STUBDIR:$ORIG_PATH" STUB_LOG STUB_BFS_STATE EXAPUMP_BFS_CP_FAIL=1
    source "$INSTALLER"
    CONNECTIVITY_MODE=profile; ARG_PROFILE=bfsprofile; TARGET_MODE=bucketfs
    bucketfs_upload_file "/tmp/local.so" "udf/liblakehouse_engine.so" 2>&1
  )"
  local rc=$?
  assert_rc_nonzero "upload fail: nonzero" "$rc"
  assert_contains "upload fail: names the local file" "$out" "/tmp/local.so"
  assert_contains "upload fail: names the bucket path" "$out" "udf/liblakehouse_engine.so"
  assert_contains "upload fail: surfaces exapump's own stderr verbatim" "$out" "BucketFS returned HTTP 403 Forbidden"
}

test_bucketfs_verify_listed_and_wait() {
  echo "== test_bucketfs_verify_listed_and_wait =="
  reset_env
  # Whole-token match: a stored 'liblakehouse_engine.so.bak' must not satisfy a check for
  # 'liblakehouse_engine.so'.
  printf 'udf/liblakehouse_engine.so.bak\n' > "$STUB_BFS_STATE"
  local answer
  answer="$(
    export PATH="$STUBDIR:$ORIG_PATH" STUB_LOG STUB_BFS_STATE
    source "$INSTALLER"
    CONNECTIVITY_MODE=profile; ARG_PROFILE=bfsprofile
    if bucketfs_verify_listed "udf/liblakehouse_engine.so"; then echo yes; else echo no; fi
  )"
  assert_eq "verify listed: no false positive on a longer stored name" "no" "$answer"

  printf 'udf/liblakehouse_engine.so\n' >> "$STUB_BFS_STATE"
  answer="$(
    export PATH="$STUBDIR:$ORIG_PATH" STUB_LOG STUB_BFS_STATE
    source "$INSTALLER"
    CONNECTIVITY_MODE=profile; ARG_PROFILE=bfsprofile
    if bucketfs_verify_listed "udf/liblakehouse_engine.so"; then echo yes; else echo no; fi
  )"
  assert_eq "verify listed: an exact entry verifies" "yes" "$answer"

  # Retry-then-hit: the first two listings find nothing (async unpack), the third succeeds.
  reset_env
  printf 'slc/lakehouse-rustslc.tar.gz\n' > "$STUB_BFS_STATE"
  local out rc
  out="$(
    export PATH="$STUBDIR:$ORIG_PATH" STUB_LOG STUB_BFS_STATE EXAPUMP_BFS_LS_DELAY=2
    source "$INSTALLER"
    CONNECTIVITY_MODE=profile; ARG_PROFILE=bfsprofile; ARG_BFS_BUCKET=default
    bucketfs_wait_for_path "slc/lakehouse-rustslc.tar.gz" 5 0 2>&1
  )"
  rc=$?
  assert_rc_zero "wait for path: retries past an asynchronous unpack and then succeeds" "$rc"
  assert_contains "wait for path: reports the verified path" "$out" "slc/lakehouse-rustslc.tar.gz"
  assert_eq "wait for path: took exactly 3 ls attempts (2 misses + 1 hit)" \
    "3" "$(count_occurrences 'exapump bucketfs ls' "$(log_content)")"

  # Retry-then-fail: names the path and the try count, never hangs.
  reset_env
  out="$(
    export PATH="$STUBDIR:$ORIG_PATH" STUB_LOG STUB_BFS_STATE EXAPUMP_BFS_NEVER_LIST=1
    source "$INSTALLER"
    CONNECTIVITY_MODE=profile; ARG_PROFILE=bfsprofile; ARG_BFS_BUCKET=default
    bucketfs_wait_for_path "slc/lakehouse-rustslc.tar.gz" 3 0 2>&1
  )"
  rc=$?
  assert_rc_nonzero "wait for path: gives up nonzero after the cap" "$rc"
  assert_contains "wait for path: failure names the path" "$out" "slc/lakehouse-rustslc.tar.gz"
  assert_contains "wait for path: failure names the try count" "$out" "3 tries"
  assert_eq "wait for path: capped at exactly 3 ls attempts" \
    "3" "$(count_occurrences 'exapump bucketfs ls' "$(log_content)")"
}

test_bucketfs_reachable_preflight() {
  echo "== test_bucketfs_reachable_preflight =="
  reset_env
  export EXAPUMP_BFS_LS_FAIL=1
  run_file_bfs "${BFS_HAPPY_ARGS[@]}"
  assert_rc_nonzero "bfs preflight: unreachable bucket exits nonzero" "$LAST_RC"
  assert_contains "bfs preflight: names the bucket" "$LAST_OUT" "bucket 'default'"
  assert_contains "bfs preflight: points at the likely cause" "$LAST_OUT" "--bfs-host"
  assert_contains "bfs preflight: surfaces exapump's own diagnostic" "$LAST_OUT" "not reachable at stub-bfs-host"
  local log; log="$(log_content)"
  assert_not_contains "bfs preflight: fails before any release download" "$log" "releases/"
}

test_validate_bucketfs_required_before_any_call() {
  echo "== test_validate_bucketfs_required_before_any_call =="
  # profile mode, profile has no bfs_write_password and none was passed.
  reset_env
  run_file_bfs --profile staging
  assert_rc_nonzero "bfs required: missing write password exits nonzero" "$LAST_RC"
  assert_contains "bfs required: names the flag" "$LAST_OUT" "--bfs-write-password"
  assert_contains "bfs required: names the config key" "$LAST_OUT" "bfs_write_password"
  assert_contains "bfs required: names the profile section" "$LAST_OUT" "[staging]"
  assert_eq "bfs required: fails BEFORE any curl/exapump call is made" "" "$(log_content)"

  # profile mode with the password supplied directly still proceeds even if the profile lacks it.
  reset_env
  run_file_bfs --profile staging --bfs-write-password BFSWRITEPW789
  assert_rc_zero "bfs required: an explicit --bfs-write-password satisfies profile mode" "$LAST_RC"

  # dsn mode: no profile to fall back on, so BOTH --bfs-host and --bfs-write-password are required.
  reset_env
  run_file_bfs --dsn "exasol://user:SECRETPAT123@dsnhost:8563"
  assert_rc_nonzero "bfs required dsn: nonzero" "$LAST_RC"
  assert_contains "bfs required dsn: names --bfs-write-password" "$LAST_OUT" "--bfs-write-password"
  assert_contains "bfs required dsn: names --bfs-host" "$LAST_OUT" "--bfs-host"
  assert_contains "bfs required dsn: explains exapump bucketfs takes no DSN" "$LAST_OUT" "no DSN"
  assert_eq "bfs required dsn: fails before any call" "" "$(log_content)"

  reset_env
  run_file_bfs --dsn "exasol://user:SECRETPAT123@dsnhost:8563" --bfs-host bfshost --bfs-write-password BFSWRITEPW789
  assert_rc_zero "bfs required dsn: both flags given proceeds to success" "$LAST_RC"

  # host mode: same requirement.
  reset_env
  run_file_bfs --host myhost:8563 --user u --password p
  assert_rc_nonzero "bfs required host: nonzero" "$LAST_RC"
  assert_contains "bfs required host: names --bfs-host" "$LAST_OUT" "--bfs-host"
  assert_eq "bfs required host: fails before any call" "" "$(log_content)"
}

test_extract_engine_so() {
  echo "== test_extract_engine_so =="
  local out rc

  out="$( source "$INSTALLER"; extract_engine_so "$ENGINE_TARBALL_GOOD" "$SANDBOX/extract-ok" )"
  rc=$?
  assert_rc_zero "extract: a well-formed engine archive extracts" "$rc"
  assert_eq "extract: prints the extracted .so path" "$SANDBOX/extract-ok/udf/liblakehouse_engine.so" "$out"
  if [[ -s "$SANDBOX/extract-ok/udf/liblakehouse_engine.so" ]]; then
    pass "extract: the extracted .so exists and is non-empty"
  else
    fail "extract: the extracted .so exists and is non-empty"
  fi

  out="$( source "$INSTALLER"; extract_engine_so "$ENGINE_TARBALL_BAD" "$SANDBOX/extract-bad" 2>&1 )"
  rc=$?
  assert_rc_nonzero "extract: an archive without the .so member fails" "$rc"
  assert_contains "extract: error names the expected member" "$out" "udf/liblakehouse_engine.so"
  assert_contains "extract: error names the archive" "$out" "engine-bad.tar.gz"

  out="$( source "$INSTALLER"; extract_engine_so "$SANDBOX/no-such-archive.tar.gz" "$SANDBOX/extract-missing" 2>&1 )"
  rc=$?
  assert_rc_nonzero "extract: a missing archive fails" "$rc"
  assert_contains "extract: missing-archive error names the archive" "$out" "no-such-archive.tar.gz"
}

test_bucketfs_full_run_artifact_shapes() {
  echo "== test_bucketfs_full_run_artifact_shapes =="
  reset_env
  run_file_bfs "${BFS_HAPPY_ARGS[@]}"
  assert_rc_zero "bfs run: install succeeds" "$LAST_RC"
  assert_contains "bfs run: reaches the smoke-test pass" "$LAST_OUT" "Fingerprint smoke test passed"
  assert_contains "bfs run: reaches the next-step template" "$LAST_OUT" "CREATE VIRTUAL SCHEMA"
  local log; log="$(log_content)"

  # The SLC goes up as a TARBALL (BucketFS must auto-extract it).
  assert_contains "bfs run: SLC uploaded as a tarball to the bucket-relative slc/ path" "$log" \
    "bucketfs cp "
  assert_contains "bfs run: SLC destination is slc/lakehouse-rustslc.tar.gz" "$log" \
    "rustslc.tar.gz slc/lakehouse-rustslc.tar.gz"

  # The ENGINE goes up as a BARE .so, extracted locally first -- never the tarball.
  assert_contains "bfs run: engine uploaded as the extracted bare .so" "$log" \
    "extracted/udf/liblakehouse_engine.so udf/liblakehouse_engine.so"
  assert_not_contains "bfs run: the engine TARBALL is never uploaded to BucketFS" "$log" \
    "lakehouse-engine.tar.gz udf/"

  # Nothing SaaS is touched at all.
  assert_not_contains "bfs run: never contacts the SaaS control plane" "$log" "cloud.exasol.com"
  assert_not_contains "bfs run: never calls the SaaS accounts API" "$log" "/api/v1/accounts"
  assert_not_contains "bfs run: no presigned POST dance" "$log" "-X POST"
  assert_not_contains "bfs run: no raw HTTP PUT upload" "$log" "--upload-file"

  # DDL and SCRIPT_LANGUAGES use the generic bfsdefault layout.
  assert_contains "bfs run: %udf_object uses the bfsdefault .so path" "$log" "$BFS_SO_UDF_OBJECT"
  assert_contains "bfs run: ALTER SYSTEM registers the bfsdefault RUST alias" "$log" "$BFS_RUST_SEGMENT"
  assert_contains "bfs run: three scripts created" "$log" "LHVS.LAKEHOUSE_DISTRIBUTE_FILES"

  # Both uploads are verified through a listing before the run proceeds.
  assert_contains "bfs run: verifies the SLC path by listing" "$log" "exapump bucketfs ls slc"
  assert_contains "bfs run: verifies the engine path by listing" "$log" "exapump bucketfs ls udf"

  # --bfs-bucket propagates end to end.
  reset_env
  run_file_bfs "${BFS_HAPPY_ARGS[@]}" --bfs-bucket other
  assert_rc_zero "bfs run --bfs-bucket other: install succeeds" "$LAST_RC"
  log="$(log_content)"
  assert_contains "bfs run --bfs-bucket other: exapump is told the bucket" "$log" "--bfs-bucket other"
  assert_contains "bfs run --bfs-bucket other: %udf_object carries it" "$log" "buckets/bfsdefault/other/udf/liblakehouse_engine.so"
  assert_not_contains "bfs run --bfs-bucket other: no stale default-bucket .so path in the DDL" "$log" \
    "buckets/bfsdefault/default/udf"
}

test_saas_run_never_touches_bucketfs() {
  echo "== test_saas_run_never_touches_bucketfs =="
  reset_env
  run_file "${HAPPY_ARGS[@]}"
  assert_rc_zero "saas run: still succeeds" "$LAST_RC"
  local log; log="$(log_content)"
  assert_not_contains "saas run: never invokes exapump bucketfs" "$log" "exapump bucketfs"
  assert_contains "saas run: still uploads the engine TARBALL via the files API" "$log" "/files/lakehouse-engine.tar.gz"
  assert_contains "saas run: SLC still uploaded as a tarball too" "$log" "/files/rustslc.tar.gz"
}

test_tar_required_only_in_bucketfs_mode() {
  echo "== test_tar_required_only_in_bucketfs_mode =="
  reset_env
  RUN_PATH="$NOTAR_DIR"
  run_file_bfs "${BFS_HAPPY_ARGS[@]}"
  assert_rc_nonzero "no tar + bucketfs: nonzero exit" "$LAST_RC"
  assert_contains "no tar + bucketfs: names tar" "$LAST_OUT" "'tar' not found"
  assert_eq "no tar + bucketfs: fails before any call" "" "$(log_content)"

  reset_env
  RUN_PATH="$NOTAR_DIR"
  run_file "${HAPPY_ARGS[@]}"
  assert_rc_zero "no tar + saas: install still succeeds (SaaS uploads the tarball as-is)" "$LAST_RC"
  assert_not_contains "no tar + saas: never complains about tar" "$LAST_OUT" "'tar' not found"
}

test_skip_slc_gating() {
  echo "== test_skip_slc_gating =="
  # BucketFS mode (the newer, riskier path): --skip-slc drops the SLC download, the SLC upload and
  # the ALTER SYSTEM, but leaves the engine install, the DDL and the smoke test untouched.
  reset_env
  run_file_bfs "${BFS_HAPPY_ARGS[@]}" --skip-slc
  assert_rc_zero "skip-slc bfs: install still succeeds" "$LAST_RC"
  assert_contains "skip-slc bfs: says why the SLC step was skipped" "$LAST_OUT" "Skipping SLC registration (--skip-slc)"
  local log; log="$(log_content)"
  assert_not_contains "skip-slc bfs: SLC never uploaded" "$log" "slc/lakehouse-rustslc.tar.gz"
  assert_not_contains "skip-slc bfs: SLC asset never downloaded" "$log" "language-container-rs/releases/tags"
  assert_not_contains "skip-slc bfs: SCRIPT_LANGUAGES never read" "$log" "SELECT SYSTEM_VALUE FROM EXA_PARAMETERS"
  assert_not_contains "skip-slc bfs: ALTER SYSTEM never issued" "$log" "ALTER SYSTEM SET SCRIPT_LANGUAGES"
  # ... while everything downstream of the SLC still runs.
  assert_contains "skip-slc bfs: engine .so still uploaded" "$log" "extracted/udf/liblakehouse_engine.so udf/liblakehouse_engine.so"
  assert_contains "skip-slc bfs: DDL still created" "$log" "LHVS.LAKEHOUSE_DISTRIBUTE_FILES"
  assert_contains "skip-slc bfs: smoke test still run" "$log" "LAKEHOUSE_SCAN('x', 'y')"
  assert_contains "skip-slc bfs: still prints the next-step template" "$LAST_OUT" "CREATE VIRTUAL SCHEMA"
  # The SLC VERSION is still resolved and reported, so the user can see what the DB must already have.
  assert_contains "skip-slc bfs: SLC version still reported" "$LAST_OUT" "Resolved language-container (SLC) version"

  # SaaS mode gates on the same flag.
  reset_env
  run_file "${HAPPY_ARGS[@]}" --skip-slc
  assert_rc_zero "skip-slc saas: install still succeeds" "$LAST_RC"
  log="$(log_content)"
  assert_not_contains "skip-slc saas: SLC never uploaded" "$log" "/files/rustslc.tar.gz"
  assert_contains "skip-slc saas: engine still uploaded" "$log" "/files/lakehouse-engine.tar.gz"

  # Default (no --skip-slc) still does the whole thing.
  reset_env
  run_file_bfs "${BFS_HAPPY_ARGS[@]}"
  assert_rc_zero "default: install succeeds" "$LAST_RC"
  assert_not_contains "default: no skip notice" "$LAST_OUT" "Skipping SLC registration"
  log="$(log_content)"
  assert_contains "default: SLC uploaded" "$log" "slc/lakehouse-rustslc.tar.gz"
  assert_contains "default: ALTER SYSTEM issued" "$log" "ALTER SYSTEM SET SCRIPT_LANGUAGES"
}

test_usage_is_mode_aware() {
  echo "== test_usage_is_mode_aware =="
  reset_env
  run_file --help
  assert_rc_zero "usage: --help exits 0" "$LAST_RC"
  assert_contains "usage: names this script" "$LAST_OUT" "install.sh"
  assert_not_contains "usage: no leftover install-saas.sh program name" "$LAST_OUT" "install-saas.sh"
  assert_contains "usage: documents the saas target" "$LAST_OUT" "--account-id"
  assert_contains "usage: documents the bucketfs target" "$LAST_OUT" "--bfs-write-password"
  assert_contains "usage: documents --target" "$LAST_OUT" "--target <saas|bucketfs>"
  assert_contains "usage: documents --skip-slc" "$LAST_OUT" "--skip-slc"
  assert_contains "usage: gives a saas example" "$LAST_OUT" "install.sh --account-id ACC --database-id DB --profile"
  assert_contains "usage: gives a bucketfs example" "$LAST_OUT" "install.sh --profile my-exasol --bfs-write-password"
  assert_eq "usage: --help makes no network/SQL call" "" "$(log_content)"
}

arch_default_is_x86_64() {
  echo "== arch_default_is_x86_64 =="
  local result
  result="$(
    source "$INSTALLER"
    parse_args
    printf 'arch=%s set=%s\n' "$ARG_ARCH" "$ARG_ARCH_SET"
  )"
  assert_eq "arch: no --arch flag defaults to x86_64" "arch=x86_64 set=0" "$result"
}

arch_explicit_flag_is_stored_and_marks_set() {
  echo "== arch_explicit_flag_is_stored_and_marks_set =="
  local result
  result="$(
    source "$INSTALLER"
    parse_args --arch aarch64
    printf 'arch=%s set=%s\n' "$ARG_ARCH" "$ARG_ARCH_SET"
  )"
  assert_eq "arch: explicit --arch aarch64 is accepted and stored" "arch=aarch64 set=1" "$result"

  result="$(
    source "$INSTALLER"
    parse_args --arch x86_64
    printf 'arch=%s set=%s\n' "$ARG_ARCH" "$ARG_ARCH_SET"
  )"
  assert_eq "arch: explicit --arch x86_64 is accepted and stored" "arch=x86_64 set=1" "$result"
}

arch_invalid_value_rejected() {
  echo "== arch_invalid_value_rejected =="
  local out rc
  out="$( source "$INSTALLER"; parse_args --arch mips 2>&1 )"
  rc=$?
  assert_rc_nonzero "arch: --arch mips is rejected" "$rc"
  assert_contains "arch: error names x86_64 as a valid value" "$out" "x86_64"
  assert_contains "arch: error names aarch64 as a valid value" "$out" "aarch64"

  reset_env
  run_file --arch sparc64 --profile staging
  assert_rc_nonzero "arch: full run with an invalid --arch value exits nonzero" "$LAST_RC"
  assert_contains "arch: full-run error names the rejected value" "$LAST_OUT" "sparc64"
  assert_contains "arch: full-run error names x86_64 as a valid value" "$LAST_OUT" "x86_64"
  assert_contains "arch: full-run error names aarch64 as a valid value" "$LAST_OUT" "aarch64"
  assert_eq "arch: invalid --arch fails before any network/SQL call" "" "$(log_content)"
}

resolve_arch_suffix_returns_expected_values() {
  echo "== resolve_arch_suffix_returns_expected_values =="
  local suffix
  suffix="$( source "$INSTALLER"; resolve_arch_suffix "x86_64" )"
  assert_eq "resolve_arch_suffix: x86_64 has no suffix" "" "$suffix"

  suffix="$( source "$INSTALLER"; resolve_arch_suffix "aarch64" )"
  assert_eq "resolve_arch_suffix: aarch64 suffix is -aarch64" "-aarch64" "$suffix"
}

arch_aarch64_selects_suffixed_assets() {
  echo "== arch_aarch64_selects_suffixed_assets =="
  local out

  out="$(
    source "$INSTALLER"
    # shellcheck disable=SC2317,SC2329
    download_release_asset() { printf 'asset=%s\n' "$3"; : > "$4"; return 0; }
    ARG_ARCH="x86_64"
    RESOLVED_SLC_VERSION="0.21.0"
    WORKDIR="$(mktemp -d "$SANDBOX/arch-workdir.XXXXXX")"
    download_slc
    download_engine
  )"
  assert_contains "x86_64: SLC asset name is unsuffixed" "$out" "asset=lc-rust-0.21.0.tar.gz"
  assert_contains "x86_64: engine asset name is unsuffixed" "$out" "asset=lakehouse-engine.tar.gz"

  out="$(
    source "$INSTALLER"
    # shellcheck disable=SC2317,SC2329
    download_release_asset() { printf 'asset=%s\n' "$3"; : > "$4"; return 0; }
    ARG_ARCH="aarch64"
    RESOLVED_SLC_VERSION="0.21.0"
    WORKDIR="$(mktemp -d "$SANDBOX/arch-workdir.XXXXXX")"
    download_slc
    download_engine
  )"
  assert_contains "aarch64: SLC asset name carries the -aarch64 suffix" "$out" "asset=lc-rust-0.21.0-aarch64.tar.gz"
  assert_contains "aarch64: engine asset name carries the -aarch64 suffix" "$out" "asset=lakehouse-engine-aarch64.tar.gz"
}

deployment_ssh_port_resolution() {
  echo "== deployment_ssh_port_resolution =="
  local dir port out rc
  dir="$(mktemp -d "$SANDBOX/dep-ssh-port.XXXXXX")"

  printf '{"connection":{"host":"127.0.0.1","sshPort":52341,"dbPort":8563}}\n' > "$dir/deployment.json"
  port="$( source "$INSTALLER"; deployment_ssh_port "$dir" )"
  assert_eq "deployment_ssh_port: reads connection.sshPort" "52341" "$port"

  printf '{"connection":{"host":"127.0.0.1","sshPort":52999,"dbPort":8563}}\n' > "$dir/deployment.json"
  port="$( source "$INSTALLER"; deployment_ssh_port "$dir" )"
  assert_eq "deployment_ssh_port: a reassigned port is read fresh, never cached" "52999" "$port"

  printf '{"connection":{"host":"127.0.0.1"}}\n' > "$dir/deployment.json"
  out="$( source "$INSTALLER"; deployment_ssh_port "$dir" 2>&1 )"
  rc=$?
  assert_rc_nonzero "deployment_ssh_port: no sshPort field fails" "$rc"
  assert_contains "deployment_ssh_port: error names sshPort" "$out" "sshPort"

  rm -f "$dir/deployment.json"
  out="$( source "$INSTALLER"; deployment_ssh_port "$dir" 2>&1 )"
  rc=$?
  assert_rc_nonzero "deployment_ssh_port: a missing descriptor fails" "$rc"
}

deployment_key_path_resolution() {
  echo "== deployment_key_path_resolution =="
  local path
  path="$( source "$INSTALLER"; deployment_key_path "/some/dep/dir" )"
  assert_eq "deployment_key_path: the node key sits under <dir>/local/node_access.pem" \
    "/some/dep/dir/local/node_access.pem" "$path"
}

read_descriptor_field_reports_jq_stderr() {
  echo "== read_descriptor_field_reports_jq_stderr =="
  local dir out rc
  dir="$(mktemp -d "$SANDBOX/dep-badjson.XXXXXX")"
  printf 'not valid json{' > "$dir/deployment.json"

  out="$( source "$INSTALLER"; read_descriptor_field "$dir/deployment.json" '.backend' 2>&1 )"
  rc=$?
  assert_rc_nonzero "read_descriptor_field: malformed JSON fails" "$rc"
  assert_contains "read_descriptor_field: error names jq" "$out" "jq said:"
  assert_contains "read_descriptor_field: error carries jq's own diagnostic" "$out" "parse error"
}

deployment_backend_discrimination() {
  echo "== deployment_backend_discrimination =="
  local dir backend out rc
  dir="$(mktemp -d "$SANDBOX/dep-backend.XXXXXX")"

  printf '{"backend":"local","connection":{"host":"127.0.0.1"}}\n' > "$dir/deployment.json"
  backend="$( source "$INSTALLER"; deployment_backend "$dir" )"
  assert_eq "deployment_backend: a local descriptor reports the local backend" "local" "$backend"

  printf '{"backend":"aws","connection":{"host":"h.example"}}\n' > "$dir/deployment.json"
  backend="$( source "$INSTALLER"; deployment_backend "$dir" )"
  assert_eq "deployment_backend: a cloud descriptor reports its own backend name" "aws" "$backend"

  printf '{"connection":{"host":"h.example"}}\n' > "$dir/deployment.json"
  out="$( source "$INSTALLER"; deployment_backend "$dir" 2>&1 )"
  rc=$?
  assert_rc_nonzero "deployment_backend: a descriptor with no .backend field fails" "$rc"
  assert_contains "deployment_backend: error names the missing .backend field" "$out" "backend"

  printf '{"backend":"","connection":{"host":"h.example"}}\n' > "$dir/deployment.json"
  out="$( source "$INSTALLER"; deployment_backend "$dir" 2>&1 )"
  rc=$?
  assert_rc_nonzero "deployment_backend: a descriptor with an empty .backend field fails" "$rc"
}

deployment_connection_resolves_from_fixture() {
  echo "== deployment_connection_resolves_from_fixture =="
  local dir out
  dir="$(mktemp -d "$SANDBOX/dep-conn.XXXXXX")"
  write_local_deployment_fixture_custom_connection "$dir"

  out="$(
    source "$INSTALLER"
    ARG_HOST=""; ARG_USER=""; ARG_PASSWORD=""
    resolve_deployment_connection "$dir" "$PERSONAL_DB_HOST_DEFAULT"
    printf 'host=%s user=%s password=%s\n' "$ARG_HOST" "$ARG_USER" "$ARG_PASSWORD"
  )"
  assert_eq "resolve_deployment_connection: host:port, user and password all resolve from the fixture descriptor" \
    "host=descriptor.example:52164 user=dbadmin password=fixture-secret" "$out"
}

deployment_cli_overrides_descriptor() {
  echo "== deployment_cli_overrides_descriptor =="
  local dir out
  dir="$(mktemp -d "$SANDBOX/dep-override.XXXXXX")"
  write_local_deployment_fixture_custom_connection "$dir"

  out="$(
    source "$INSTALLER"
    ARG_HOST="override.example"; ARG_USER=""; ARG_PASSWORD="override"
    resolve_deployment_connection "$dir" "$PERSONAL_DB_HOST_DEFAULT"
    printf 'host=%s user=%s password=%s\n' "$ARG_HOST" "$ARG_USER" "$ARG_PASSWORD"
  )"
  assert_eq "override: explicit --host and --password win, unoverridden port and user still resolve from the descriptor" \
    "host=override.example:52164 user=dbadmin password=override" "$out"
}

deployment_missing_dir_fails() {
  echo "== deployment_missing_dir_fails =="
  local nonexistent_root out rc
  nonexistent_root="$SANDBOX/no-deployments-here"

  out="$(
    source "$INSTALLER"
    DEPLOYMENT_ROOT="$nonexistent_root"
    TARGET_MODE="bucketfs"
    ARG_PROFILE=""; ARG_DSN=""
    ARG_DEPLOYMENT="nonexistent"
    resolve_deployment_transport 2>&1
  )"
  rc=$?
  assert_rc_nonzero "missing deployment dir: nonzero exit" "$rc"
  assert_contains "missing deployment dir: error names the expected path" "$out" \
    "$nonexistent_root/nonexistent"
}

deployment_requires_jq() {
  echo "== deployment_requires_jq =="
  local out rc
  out="$(
    source "$INSTALLER"
    PATH="$NO_JQ_DIR"
    TARGET_MODE="bucketfs"
    ARG_PROFILE=""; ARG_DSN=""
    ARG_DEPLOYMENT="my-db"
    resolve_deployment_transport 2>&1
  )"
  rc=$?
  assert_rc_nonzero "deployment requires jq: nonzero exit when jq is absent from PATH" "$rc"
  assert_contains "deployment requires jq: error names jq" "$out" "jq"
}

deployment_rejects_saas_target() {
  echo "== deployment_rejects_saas_target =="
  local out rc
  out="$(
    source "$INSTALLER"
    TARGET_MODE="saas"
    ARG_PROFILE=""; ARG_DSN=""
    ARG_DEPLOYMENT="my-db"
    resolve_deployment_transport 2>&1
  )"
  rc=$?
  assert_rc_nonzero "deployment rejects saas target: nonzero exit" "$rc"
  assert_contains "deployment rejects saas target: error names --account-id" "$out" "--account-id"
}

deployment_rejects_profile_and_dsn() {
  echo "== deployment_rejects_profile_and_dsn =="
  local out rc
  out="$(
    source "$INSTALLER"
    TARGET_MODE="bucketfs"
    ARG_PROFILE="staging"; ARG_DSN=""
    ARG_DEPLOYMENT="my-db"
    resolve_deployment_transport 2>&1
  )"
  rc=$?
  assert_rc_nonzero "deployment rejects --profile: nonzero exit" "$rc"
  assert_contains "deployment rejects --profile: error names --profile" "$out" "--profile"

  out="$(
    source "$INSTALLER"
    TARGET_MODE="bucketfs"
    ARG_PROFILE=""; ARG_DSN="exasol://sys:pw@127.0.0.1:8563"
    ARG_DEPLOYMENT="my-db"
    resolve_deployment_transport 2>&1
  )"
  rc=$?
  assert_rc_nonzero "deployment rejects --dsn: nonzero exit" "$rc"
  assert_contains "deployment rejects --dsn: error names --dsn" "$out" "--dsn"
}

deployment_rejects_empty_bfs_bucket() {
  echo "== deployment_rejects_empty_bfs_bucket =="
  local dir out rc
  dir="$(mktemp -d "$SANDBOX/dep-emptybucket.XXXXXX")"
  write_local_deployment_fixture "$dir"

  out="$(
    source "$INSTALLER"
    DEPLOYMENT_ROOT="$(dirname "$dir")"
    TARGET_MODE="bucketfs"
    ARG_PROFILE=""; ARG_DSN=""
    ARG_ARCH="x86_64"; ARG_ARCH_SET=1
    ARG_DEPLOYMENT="$(basename "$dir")"
    ARG_BFS_BUCKET=""
    resolve_deployment_transport 2>&1
  )"
  rc=$?
  assert_rc_nonzero "deployment rejects empty --bfs-bucket: nonzero exit" "$rc"
  assert_contains "deployment rejects empty --bfs-bucket: error names --bfs-bucket" "$out" "--bfs-bucket"
}

deployment_cloud_requires_bfs_password() {
  echo "== deployment_cloud_requires_bfs_password =="
  local dir out rc
  dir="$(mktemp -d "$SANDBOX/dep-cloud-nopw.XXXXXX")"
  write_cloud_deployment_fixture "$dir" aws

  out="$(
    source "$INSTALLER"
    DEPLOYMENT_ROOT="$(dirname "$dir")"
    TARGET_MODE="bucketfs"
    ARG_PROFILE=""; ARG_DSN=""
    ARG_DEPLOYMENT="$(basename "$dir")"
    ARG_BFS_WRITE_PASSWORD=""
    resolve_deployment_transport 2>&1
  )"
  rc=$?
  assert_rc_nonzero "cloud deployment without --bfs-write-password: nonzero exit" "$rc"
  assert_contains "cloud deployment without --bfs-write-password: error names the flag" \
    "$out" "--bfs-write-password"
  assert_contains "cloud deployment without --bfs-write-password: error says it is required for a cloud deployment" \
    "$out" "cloud"
}

deployment_cloud_bfs_transport() {
  echo "== deployment_cloud_bfs_transport =="
  local dir out
  dir="$(mktemp -d "$SANDBOX/dep-cloud.XXXXXX")"
  write_cloud_deployment_fixture "$dir" aws

  out="$(
    source "$INSTALLER"
    DEPLOYMENT_ROOT="$(dirname "$dir")"
    TARGET_MODE="bucketfs"
    ARG_PROFILE=""; ARG_DSN=""
    ARG_DEPLOYMENT="$(basename "$dir")"
    ARG_BFS_WRITE_PASSWORD="secret"
    resolve_deployment_transport 2>&1
    printf 'rc=%s transport=%s host=%s bfs_host=%s\n' "$?" "$DEPLOYMENT_TRANSPORT" "$ARG_HOST" "$ARG_BFS_HOST"
  )"
  assert_contains "cloud deployment: resolves successfully" "$out" "rc=0"
  assert_contains "cloud deployment: selects the bucketfs (HTTP) transport" "$out" "transport=bucketfs"
  assert_contains "cloud deployment: connection host:port resolves from the descriptor" "$out" "host=cloud.example:8563"
  assert_contains "cloud deployment: the BucketFS host derives from the resolved connection host" "$out" "bfs_host=cloud.example"
}

deployment_rejects_bfs_bucket_with_invalid_characters() {
  echo "== deployment_rejects_bfs_bucket_with_invalid_characters =="
  local dir out rc
  dir="$(mktemp -d "$SANDBOX/dep-badbucket.XXXXXX")"
  write_local_deployment_fixture "$dir"

  out="$(
    source "$INSTALLER"
    DEPLOYMENT_ROOT="$(dirname "$dir")"
    TARGET_MODE="bucketfs"
    ARG_PROFILE=""; ARG_DSN=""
    ARG_ARCH="x86_64"; ARG_ARCH_SET=1
    ARG_DEPLOYMENT="$(basename "$dir")"
    ARG_BFS_BUCKET="mal'icious"
    resolve_deployment_transport 2>&1
  )"
  rc=$?
  assert_rc_nonzero "bfs-bucket with a single quote: nonzero exit" "$rc"
  assert_contains "bfs-bucket with a single quote: error names the rejected value" "$out" "mal'icious"
  assert_contains "bfs-bucket with a single quote: error names the allowed character set" "$out" "A-Za-z0-9._-"
}

deployment_local_ssh_transport() {
  echo "== deployment_local_ssh_transport =="
  local dir out
  dir="$(mktemp -d "$SANDBOX/dep-local.XXXXXX")"
  write_local_deployment_fixture "$dir"

  out="$(
    source "$INSTALLER"
    DEPLOYMENT_ROOT="$(dirname "$dir")"
    TARGET_MODE="bucketfs"
    ARG_PROFILE=""; ARG_DSN=""
    ARG_ARCH="x86_64"; ARG_ARCH_SET=1
    ARG_DEPLOYMENT="$(basename "$dir")"
    resolve_deployment_transport 2>&1
    printf 'rc=%s transport=%s ssh_port=%s key=%s\n' \
      "$?" "$DEPLOYMENT_TRANSPORT" "$DEPLOYMENT_SSH_PORT" "$DEPLOYMENT_KEY_PATH"
  )"
  assert_contains "local deployment: resolves successfully" "$out" "rc=0"
  assert_contains "local deployment: selects the ssh transport" "$out" "transport=ssh"
  assert_contains "local deployment: the ssh port comes from the descriptor" "$out" "ssh_port=52341"
  assert_contains "local deployment: the node key path sits under the deployment's local/ dir" \
    "$out" "key=$dir/local/node_access.pem"
}

deployment_local_autodetects_arch() {
  echo "== deployment_local_autodetects_arch =="
  local dir out
  dir="$(mktemp -d "$SANDBOX/dep-local-arch.XXXXXX")"
  write_local_deployment_fixture "$dir"

  out="$(
    source "$INSTALLER"
    PATH="$UNAME_ARM64_DIR:$ORIG_PATH"
    DEPLOYMENT_ROOT="$(dirname "$dir")"
    TARGET_MODE="bucketfs"
    ARG_PROFILE=""; ARG_DSN=""
    ARG_ARCH="x86_64"; ARG_ARCH_SET=0
    ARG_DEPLOYMENT="$(basename "$dir")"
    resolve_deployment_transport >/dev/null 2>&1
    printf 'arch=%s\n' "$ARG_ARCH"
  )"
  assert_eq "local deployment: an aarch64 host is auto-detected via uname -m" "arch=aarch64" "$out"
}

deployment_local_unsupported_uname_fails_detection() {
  echo "== deployment_local_unsupported_uname_fails_detection =="
  local dir out rc
  dir="$(mktemp -d "$SANDBOX/dep-local-badarch.XXXXXX")"
  write_local_deployment_fixture "$dir"

  out="$(
    source "$INSTALLER"
    PATH="$UNAME_PPC64LE_DIR:$ORIG_PATH"
    DEPLOYMENT_ROOT="$(dirname "$dir")"
    TARGET_MODE="bucketfs"
    ARG_PROFILE=""; ARG_DSN=""
    ARG_ARCH="x86_64"; ARG_ARCH_SET=0
    ARG_DEPLOYMENT="$(basename "$dir")"
    resolve_deployment_transport 2>&1
  )"
  rc=$?
  assert_rc_nonzero "unsupported uname -m: resolve_deployment_transport fails" "$rc"
  assert_contains "unsupported uname -m: error names the detected value" "$out" "ppc64le"
  assert_contains "unsupported uname -m: error instructs --arch" "$out" "--arch"
}

arch_override_beats_autodetect() {
  echo "== arch_override_beats_autodetect =="
  local dir out
  dir="$(mktemp -d "$SANDBOX/dep-local-override.XXXXXX")"
  write_local_deployment_fixture "$dir"

  out="$(
    source "$INSTALLER"
    PATH="$UNAME_ARM64_DIR:$ORIG_PATH"
    DEPLOYMENT_ROOT="$(dirname "$dir")"
    TARGET_MODE="bucketfs"
    ARG_PROFILE=""; ARG_DSN=""
    ARG_ARCH="x86_64"; ARG_ARCH_SET=1
    ARG_DEPLOYMENT="$(basename "$dir")"
    resolve_deployment_transport >/dev/null 2>&1
    printf 'arch=%s\n' "$ARG_ARCH"
  )"
  assert_eq "an explicit --arch x86_64 overrides auto-detection on an aarch64 host" "arch=x86_64" "$out"
}

run_make_install_slc_dry() {
  local arch="$1"
  if [[ -n "$arch" ]]; then
    MAKE_OUT="$( cd "$REPO_ROOT" && BUCKETFS_WRITE_PASS=stub make -n install-slc "ARCH=$arch" 2>&1 )"
  else
    MAKE_OUT="$( cd "$REPO_ROOT" && BUCKETFS_WRITE_PASS=stub make -n install-slc 2>&1 )"
  fi
  MAKE_RC=$?
}

slc_url_from_make_output() {
  printf '%s\n' "$1" | grep -o 'https://github.com/[^"]*lc-rust[^"]*\.tar\.gz' | head -1
}

makefile_slc_url_arch_aware() {
  echo "== makefile_slc_url_arch_aware =="
  local default_url x86_url aarch64_url arm64_url

  run_make_install_slc_dry ""
  assert_rc_zero "makefile arch: an unset ARCH is accepted" "$MAKE_RC"
  default_url="$(slc_url_from_make_output "$MAKE_OUT")"
  assert_contains "makefile arch: an unset ARCH composes an lc-rust release URL" "$default_url" "lc-rust-"
  assert_not_contains "makefile arch: an unset ARCH composes the unsuffixed URL" "$default_url" "-aarch64"

  run_make_install_slc_dry "x86_64"
  assert_rc_zero "makefile arch: ARCH=x86_64 is accepted" "$MAKE_RC"
  x86_url="$(slc_url_from_make_output "$MAKE_OUT")"
  assert_eq "makefile arch: ARCH=x86_64 composes the same URL as an unset ARCH" "$default_url" "$x86_url"

  run_make_install_slc_dry "aarch64"
  assert_rc_zero "makefile arch: ARCH=aarch64 is accepted" "$MAKE_RC"
  aarch64_url="$(slc_url_from_make_output "$MAKE_OUT")"
  assert_contains "makefile arch: ARCH=aarch64 composes the -aarch64-suffixed URL" \
    "$aarch64_url" "-aarch64.tar.gz"

  run_make_install_slc_dry "arm64"
  assert_rc_zero "makefile arch: ARCH=arm64 (what uname -m prints on Apple Silicon) is accepted" "$MAKE_RC"
  arm64_url="$(slc_url_from_make_output "$MAKE_OUT")"
  assert_eq "makefile arch: ARCH=arm64 normalizes to the ARCH=aarch64 URL, as install.sh does" \
    "$aarch64_url" "$arm64_url"

  run_make_install_slc_dry "totally-bogus"
  assert_rc_nonzero "makefile arch: an unsupported ARCH exits nonzero" "$MAKE_RC"
  assert_contains "makefile arch: the rejection names the rejected value" "$MAKE_OUT" "totally-bogus"
  assert_contains "makefile arch: the rejection names x86_64 as an accepted value" "$MAKE_OUT" "x86_64"
  assert_contains "makefile arch: the rejection names aarch64 as an accepted value" "$MAKE_OUT" "aarch64"
  assert_not_contains "makefile arch: an unsupported ARCH composes no download URL at all" \
    "$MAKE_OUT" "releases/download"
}

run_deploy_personal_local() {
  local skip_slc="$1" workdir
  workdir="$(mktemp -d "$SANDBOX/dep-push.XXXXXX")"
  LAST_OUT="$(
    export PATH="$STUBDIR:$ORIG_PATH" STUB_LOG STUB_SSH_STATE
    source "$INSTALLER"
    TARGET_MODE=bucketfs
    CONNECTIVITY_MODE=dsn; ARG_DSN="exasol://sys:pw@127.0.0.1:8563"
    ARG_BFS_BUCKET=default
    ARG_SKIP_SLC="$skip_slc"
    DEPLOYMENT_TRANSPORT=ssh
    DEPLOYMENT_KEY_PATH="$DEPLOYMENT_NODE_KEY"
    DEPLOYMENT_SSH_PORT=52341
    RESOLVED_SLC_VERSION="0.21.0"
    RESOLVED_ENGINE_VERSION="1.2.3"
    VM_RECONCILE_TRIES=3
    VM_RECONCILE_POLL_SECONDS=0
    resolve_target_layout
    WORKDIR="$workdir"
    mkdir -p "$WORKDIR/extracted/udf"
    # shellcheck disable=SC2317,SC2329
    download_slc() { printf 'slc-bytes\n' > "$WORKDIR/rustslc.tar.gz"; }
    # shellcheck disable=SC2317,SC2329
    download_engine() { printf 'engine-bytes\n' > "$WORKDIR/$ENGINE_ASSET"; }
    # shellcheck disable=SC2317,SC2329
    extract_engine_so() {
      printf 'so-bytes\n' > "$WORKDIR/extracted/udf/liblakehouse_engine.so"
      printf '%s\n' "$WORKDIR/extracted/udf/liblakehouse_engine.so"
    }
    deploy_personal_local 2>&1
  )"
  LAST_RC=$?
}

deployment_local_pushes_artifacts_over_ssh() {
  echo "== deployment_local_pushes_artifacts_over_ssh =="
  reset_env
  run_deploy_personal_local 0
  assert_rc_zero "local ssh push: deploy_personal_local succeeds" "$LAST_RC"
  local log; log="$(log_content)"
  assert_contains "local ssh push: scp carries the node key and the descriptor's ssh port" \
    "$log" "-i $DEPLOYMENT_NODE_KEY -P 52341"
  assert_contains "local ssh push: ssh carries the node key and the descriptor's ssh port" \
    "$log" "-i $DEPLOYMENT_NODE_KEY -p 52341"
  assert_contains "local ssh push: the SLC tarball is staged on the VM under /tmp" \
    "$log" "root@127.0.0.1:/tmp/lakehouse-rustslc.tar.gz"
  assert_contains "local ssh push: the SLC is extracted into the VM bucket's slc tree" \
    "$log" "tar -xzf '/tmp/lakehouse-rustslc.tar.gz' -C '/var/lib/exa/bucketfs/bfsdefault/default/slc/lakehouse-rustslc'"
  assert_contains "local ssh push: the extracted SLC tree is checked for its exaudfclient" \
    "$log" "test -x '/var/lib/exa/bucketfs/bfsdefault/default/slc/lakehouse-rustslc/exaudf/exaudfclient'"
  assert_contains "local ssh push: the engine .so is staged on the VM under /tmp" \
    "$log" "root@127.0.0.1:/tmp/liblakehouse_engine.so"
  assert_contains "local ssh push: the engine .so lands at the VM bucket's udf path" \
    "$log" "mv -f '/tmp/liblakehouse_engine.so' '/var/lib/exa/bucketfs/bfsdefault/default/udf/liblakehouse_engine.so'"
  assert_contains "local ssh push: the DDL points at the bucket path the .so was installed to" \
    "$log" "$BFS_SO_UDF_OBJECT"
  assert_not_contains "local ssh push: never uploads over the BucketFS HTTP endpoint" "$log" "exapump bucketfs"
  assert_not_contains "local ssh push: never contacts the SaaS control plane" "$log" "cloud.exasol.com"
}

deployment_local_registers_script_languages_with_alter_system() {
  echo "== deployment_local_registers_script_languages_with_alter_system =="
  reset_env
  export EXAPUMP_SCRIPT_LANGUAGES="PYTHON3=builtin_python3 JAVA=builtin_java"
  run_deploy_personal_local 0
  assert_rc_zero "local ssh register: deploy_personal_local succeeds" "$LAST_RC"
  local log alter_line
  log="$(log_content)"
  alter_line="$(printf '%s\n' "$log" | grep 'ALTER SYSTEM SET SCRIPT_LANGUAGES' || true)"
  assert_contains "local ssh register: registers with ALTER SYSTEM" "$log" "ALTER SYSTEM SET SCRIPT_LANGUAGES"
  assert_not_contains "local ssh register: never uses ALTER SESSION" "$log" "ALTER SESSION"
  assert_contains "local ssh register: the pre-existing PYTHON3 entry survives the merge" \
    "$alter_line" "PYTHON3=builtin_python3"
  assert_contains "local ssh register: the pre-existing JAVA entry survives the merge" \
    "$alter_line" "JAVA=builtin_java"
  assert_contains "local ssh register: the VM bucket's RUST alias is registered" \
    "$alter_line" "$BFS_RUST_SEGMENT"
  assert_eq "local ssh register: exactly one RUST entry" "1" "$(count_occurrences 'RUST=' "$alter_line")"
}

deployment_local_skip_slc_skips_push_and_registration() {
  echo "== deployment_local_skip_slc_skips_push_and_registration =="
  reset_env
  run_deploy_personal_local 1
  assert_rc_zero "local ssh skip-slc: deploy_personal_local succeeds" "$LAST_RC"
  assert_contains "local ssh skip-slc: says why the SLC step was skipped" \
    "$LAST_OUT" "Skipping SLC upload and registration (--skip-slc)"
  local log; log="$(log_content)"
  assert_not_contains "local ssh skip-slc: the SLC is never pushed to the VM" "$log" "lakehouse-rustslc.tar.gz"
  assert_not_contains "local ssh skip-slc: SCRIPT_LANGUAGES is never read" \
    "$log" "SELECT SYSTEM_VALUE FROM EXA_PARAMETERS"
  assert_not_contains "local ssh skip-slc: ALTER SYSTEM is never issued" "$log" "ALTER SYSTEM SET SCRIPT_LANGUAGES"
  assert_contains "local ssh skip-slc: the engine .so is still pushed to the VM" \
    "$log" "root@127.0.0.1:/tmp/liblakehouse_engine.so"
  assert_contains "local ssh skip-slc: the three scripts are still created" "$log" "LHVS.LAKEHOUSE_DISTRIBUTE_FILES"
}

deployment_local_ssh_failures_are_actionable() {
  echo "== deployment_local_ssh_failures_are_actionable =="
  local out rc

  reset_env
  out="$(
    export PATH="$STUBDIR:$ORIG_PATH" STUB_LOG SCP_FAIL=1
    source "$INSTALLER"
    TARGET_MODE=bucketfs; ARG_BFS_BUCKET=default
    DEPLOYMENT_KEY_PATH="$DEPLOYMENT_NODE_KEY"; DEPLOYMENT_SSH_PORT=52341
    resolve_target_layout
    push_slc_to_vm "$SANDBOX/whatever-slc.tar.gz" 2>&1
  )"
  rc=$?
  assert_rc_nonzero "scp fail: push_slc_to_vm exits nonzero" "$rc"
  assert_contains "scp fail: names the staged destination on the VM" "$out" "/tmp/lakehouse-rustslc.tar.gz"
  assert_contains "scp fail: surfaces scp's own stderr" "$out" "Permission denied"

  reset_env
  out="$(
    export PATH="$STUBDIR:$ORIG_PATH" STUB_LOG SCP_FAIL=1
    source "$INSTALLER"
    TARGET_MODE=bucketfs; ARG_BFS_BUCKET=default
    DEPLOYMENT_KEY_PATH="$DEPLOYMENT_NODE_KEY"; DEPLOYMENT_SSH_PORT=52341
    resolve_target_layout
    push_engine_so_to_vm "$SANDBOX/whatever.so" 2>&1
  )"
  rc=$?
  assert_rc_nonzero "scp fail: push_engine_so_to_vm exits nonzero" "$rc"
  assert_contains "scp fail: names the staged .so destination on the VM" "$out" "/tmp/liblakehouse_engine.so"
  assert_contains "scp fail: engine push surfaces scp's own stderr" "$out" "Permission denied"

  reset_env
  out="$(
    export PATH="$STUBDIR:$ORIG_PATH" STUB_LOG SSH_FAIL=1
    source "$INSTALLER"
    TARGET_MODE=bucketfs; ARG_BFS_BUCKET=default
    DEPLOYMENT_KEY_PATH="$DEPLOYMENT_NODE_KEY"; DEPLOYMENT_SSH_PORT=52341
    resolve_target_layout
    push_slc_to_vm "$SANDBOX/whatever-slc.tar.gz" 2>&1
  )"
  rc=$?
  assert_rc_nonzero "ssh fail: the SLC extraction step exits nonzero" "$rc"
  assert_contains "ssh fail: names the SLC destination directory on the VM" \
    "$out" "/var/lib/exa/bucketfs/bfsdefault/default/slc/lakehouse-rustslc"
  assert_contains "ssh fail: surfaces ssh's own stderr" "$out" "Connection refused"

  reset_env
  out="$(
    export PATH="$STUBDIR:$ORIG_PATH" STUB_LOG SSH_FAIL=1
    source "$INSTALLER"
    TARGET_MODE=bucketfs; ARG_BFS_BUCKET=default
    DEPLOYMENT_KEY_PATH="$DEPLOYMENT_NODE_KEY"; DEPLOYMENT_SSH_PORT=52341
    resolve_target_layout
    push_engine_so_to_vm "$SANDBOX/whatever.so" 2>&1
  )"
  rc=$?
  assert_rc_nonzero "ssh fail: the .so install step exits nonzero" "$rc"
  assert_contains "ssh fail: names the .so destination on the VM" \
    "$out" "/var/lib/exa/bucketfs/bfsdefault/default/udf/liblakehouse_engine.so"
  assert_contains "ssh fail: .so install surfaces ssh's own stderr" "$out" "Connection refused"

  reset_env
  out="$(
    export PATH="$STUBDIR:$ORIG_PATH" STUB_LOG SSH_FAIL=1
    source "$INSTALLER"
    DEPLOYMENT_KEY_PATH="$DEPLOYMENT_NODE_KEY"; DEPLOYMENT_SSH_PORT=52341
    ssh_vm_reachable 2>&1
  )"
  rc=$?
  assert_rc_nonzero "ssh fail: the reachability preflight exits nonzero" "$rc"
  assert_contains "ssh fail: preflight names the ssh endpoint" "$out" "127.0.0.1:52341"
  assert_contains "ssh fail: preflight names the node key it used" "$out" "$DEPLOYMENT_NODE_KEY"
  assert_contains "ssh fail: preflight points at the port being reassigned on restart" "$out" "exasol status"
  assert_contains "ssh fail: preflight surfaces ssh's own stderr" "$out" "Connection refused"
}

deployment_local_waits_for_reconciled_paths() {
  echo "== deployment_local_waits_for_reconciled_paths =="
  local engine_path slc_path out rc log
  engine_path="/var/lib/exa/bucketfs/bfsdefault/default/udf/liblakehouse_engine.so"
  slc_path="/var/lib/exa/bucketfs/bfsdefault/default/slc/lakehouse-rustslc"

  reset_env
  out="$(
    export PATH="$STUBDIR:$ORIG_PATH" STUB_LOG STUB_SSH_STATE SSH_PATH_DELAY=2
    source "$INSTALLER"
    DEPLOYMENT_KEY_PATH="$DEPLOYMENT_NODE_KEY"; DEPLOYMENT_SSH_PORT=52341
    vm_wait_for_reconciled_path "$engine_path" 5 0 2>&1
  )"
  rc=$?
  assert_rc_zero "vm wait: retries past a path the VM has not exposed yet, then succeeds" "$rc"
  assert_contains "vm wait: reports the verified path" "$out" "$engine_path"
  assert_eq "vm wait: took exactly 3 probes (2 misses + 1 hit)" "3" \
    "$(count_occurrences "test -e '$engine_path'" "$(log_content)")"

  reset_env
  out="$(
    export PATH="$STUBDIR:$ORIG_PATH" STUB_LOG STUB_SSH_STATE SSH_PATH_NEVER=1
    source "$INSTALLER"
    DEPLOYMENT_KEY_PATH="$DEPLOYMENT_NODE_KEY"; DEPLOYMENT_SSH_PORT=52341
    ARG_BFS_BUCKET=default
    vm_wait_for_reconciled_path "$slc_path" 3 0 2>&1
  )"
  rc=$?
  assert_rc_nonzero "vm wait: gives up nonzero once the deadline passes, never hangs" "$rc"
  assert_contains "vm wait: the failure names the polled path" "$out" "$slc_path"
  assert_contains "vm wait: the failure names the deadline it waited out" "$out" "3 checks"
  assert_eq "vm wait: capped at exactly 3 probes" "3" \
    "$(count_occurrences "test -e '$slc_path'" "$(log_content)")"

  reset_env
  run_deploy_personal_local 0
  assert_rc_zero "vm wait: the happy path still succeeds" "$LAST_RC"
  log="$(log_content)"
  assert_contains "vm wait: the extracted SLC tree is verified on the VM" "$log" "test -e '$slc_path'"
  assert_contains "vm wait: the engine .so is verified on the VM" "$log" "test -e '$engine_path'"
  assert_not_contains "vm wait: no fixed-duration sleep stands in for the verification" \
    "$log" "Waiting 3s"

  reset_env
  run_deploy_personal_local 1
  assert_rc_zero "vm wait --skip-slc: still succeeds" "$LAST_RC"
  log="$(log_content)"
  assert_not_contains "vm wait --skip-slc: never waits for an SLC tree it did not push" "$log" "test -e '$slc_path'"
  assert_contains "vm wait --skip-slc: still verifies the engine .so" "$log" "test -e '$engine_path'"

  reset_env
  export SSH_PATH_NEVER=1
  run_deploy_personal_local 0
  assert_rc_nonzero "vm wait: deploy_personal_local fails when the VM never exposes a copied path" "$LAST_RC"
  assert_contains "vm wait: that failure names the path the VM never exposed" \
    "$LAST_OUT" "did not expose '$slc_path'"
  log="$(log_content)"
  assert_not_contains "vm wait: SCRIPT_LANGUAGES is never registered against unverified artifacts" \
    "$log" "ALTER SYSTEM SET SCRIPT_LANGUAGES"
  assert_not_contains "vm wait: no scripts are created against unverified artifacts" \
    "$log" "LHVS.LAKEHOUSE_DISTRIBUTE_FILES"
}

deployment_local_requires_ssh_and_scp() {
  echo "== deployment_local_requires_ssh_and_scp =="
  local out rc

  out="$(
    source "$INSTALLER"
    PATH="$MISSING_SSH_DIR"
    TARGET_MODE=bucketfs
    DEPLOYMENT_TRANSPORT=ssh
    check_prereqs 2>&1
  )"
  rc=$?
  assert_rc_nonzero "missing ssh: check_prereqs exits nonzero" "$rc"
  assert_contains "missing ssh: names ssh as the missing tool" "$out" "required tool 'ssh' not found"
  assert_not_contains "missing ssh: does not also blame scp, which is present" \
    "$out" "required tool 'scp' not found"

  out="$(
    source "$INSTALLER"
    PATH="$MISSING_SCP_DIR"
    TARGET_MODE=bucketfs
    DEPLOYMENT_TRANSPORT=ssh
    check_prereqs 2>&1
  )"
  rc=$?
  assert_rc_nonzero "missing scp: check_prereqs exits nonzero" "$rc"
  assert_contains "missing scp: names scp as the missing tool" "$out" "required tool 'scp' not found"
  assert_not_contains "missing scp: does not also blame ssh, which is present" \
    "$out" "required tool 'ssh' not found"

  out="$(
    source "$INSTALLER"
    PATH="$MISSING_SSH_DIR"
    TARGET_MODE=bucketfs
    DEPLOYMENT_TRANSPORT=""
    check_prereqs 2>&1
  )"
  rc=$?
  assert_rc_zero "no ssh transport: a missing ssh is not required at all" "$rc"
}

# ============================================================================
main() {
  test_missing_prereq_fails_fast
  test_connectivity_mode_either_or
  test_host_mode_requires_port
  test_host_dsn_percent_encodes_credentials
  test_dsn_mode_happy_path
  test_url_decode_roundtrip
  test_extract_dsn_password
  test_read_profile_key
  test_resolve_saas_pat_per_mode
  test_resolve_target_mode_partial_saas_ids
  test_resolve_target_layout_saas_values
  test_missing_required_ids_fail_fast
  test_version_resolution_default_and_override
  test_slc_version_defaults_to_engine_pin_not_slc_latest
  test_slc_version_pin_lookup_failure_modes
  test_script_languages_append_preserves_existing
  test_script_languages_replace_rust_idempotent
  test_empty_script_languages_read_hard_fails
  test_presigned_upload_dance
  test_presigned_url_json_unescaping
  test_release_asset_download_via_rest
  test_saas_verify_listed_quoted_match
  test_three_scripts_ddl_saas_path_types
  test_fingerprint_smoke_pass_and_fail
  test_stops_at_product_prints_template
  test_next_step_template_emits_the_scan_script_grant_and_the_replace_warning
  test_next_step_template_skips_the_grant_block_for_a_sys_installer
  test_target_base_default_and_override
  test_external_failure_actionable
  test_stdin_piped_invocation_no_body_consumption
  test_resolve_target_mode_bucketfs_autodetect
  test_target_flag_conflict_detection
  test_resolve_target_layout_bucketfs_values
  test_exapump_bfs_flags
  test_resolve_bfs_bucket_from_profile
  test_bucketfs_upload_argv_shape
  test_bucketfs_upload_failure_surfaces_stderr
  test_bucketfs_verify_listed_and_wait
  test_bucketfs_reachable_preflight
  test_validate_bucketfs_required_before_any_call
  test_extract_engine_so
  test_bucketfs_full_run_artifact_shapes
  test_saas_run_never_touches_bucketfs
  test_tar_required_only_in_bucketfs_mode
  test_skip_slc_gating
  test_usage_is_mode_aware
  arch_default_is_x86_64
  arch_explicit_flag_is_stored_and_marks_set
  arch_invalid_value_rejected
  resolve_arch_suffix_returns_expected_values
  arch_aarch64_selects_suffixed_assets
  makefile_slc_url_arch_aware
  deployment_ssh_port_resolution
  deployment_key_path_resolution
  read_descriptor_field_reports_jq_stderr
  deployment_backend_discrimination
  deployment_connection_resolves_from_fixture
  deployment_cli_overrides_descriptor
  deployment_missing_dir_fails
  deployment_requires_jq
  deployment_rejects_saas_target
  deployment_rejects_profile_and_dsn
  deployment_rejects_empty_bfs_bucket
  deployment_cloud_requires_bfs_password
  deployment_cloud_bfs_transport
  deployment_rejects_bfs_bucket_with_invalid_characters
  deployment_local_ssh_transport
  deployment_local_autodetects_arch
  deployment_local_unsupported_uname_fails_detection
  arch_override_beats_autodetect
  deployment_local_pushes_artifacts_over_ssh
  deployment_local_registers_script_languages_with_alter_system
  deployment_local_skip_slc_skips_push_and_registration
  deployment_local_ssh_failures_are_actionable
  deployment_local_waits_for_reconciled_paths
  deployment_local_requires_ssh_and_scp

  echo ""
  echo "=================================================="
  printf 'RESULT: %d passed, %d failed\n' "$PASS" "$FAIL"
  echo "=================================================="
  [[ "$FAIL" -eq 0 ]]
}

main "$@"
