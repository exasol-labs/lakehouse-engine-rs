#!/usr/bin/env bash
# One-command installer that provisions lakehouse-engine onto an Exasol SaaS database:
# registers the Rust SLC, uploads and registers the engine .so plus its four scripts, and
# verifies the load with a fingerprint smoke test. Stops at a query-ready product install and
# prints the next-step CONNECTION / VIRTUAL SCHEMA template; it does NOT create catalog objects.
#
# Distributed one-liner (piped into bash over stdin):
#   gh api -H "Accept: application/vnd.github.raw" \
#     repos/exasol-labs/lakehouse-engine-rs/contents/deploy/scripts/install-saas.sh \
#   | EXASOL_PAT=$PAT bash -s -- --account-id $ACC --database-id $DB --profile staging
#
# The file is sourceable: its functions can be sourced and unit-tested without running the
# installer, because `main` runs only when the file is executed or piped, never when sourced.
#
# Bash 3.2+ (stock macOS). No jq. Every subprocess (gh/curl/exapump) reads stdin from /dev/null
# so a subprocess cannot consume the remaining piped script body.

# --- Constants ---------------------------------------------------------------
SAAS_PROD_BASE="https://cloud.exasol.com"
SAAS_STAGING_BASE="https://cloud-staging.exasol.com"
ENGINE_REPO="exasol-labs/lakehouse-engine-rs"
SLC_REPO="exasol-labs/language-container-rs"
ENGINE_ASSET="lakehouse-engine.tar.gz"
ENGINE_SO_PATH="/buckets/uploads/default/lakehouse-engine/udf/liblakehouse_engine.so"
DEFAULT_SCHEMA="LHVS"
RUST_LANG_SEGMENT="RUST=localzmq+protobuf:///uploads/default/rustslc?lang=rust#buckets/uploads/default/rustslc/exaudf/exaudfclient"

# --- Global state (defaults; parse_args re-seeds arg-derived ones) -----------
ARG_ACCOUNT_ID=""
ARG_DATABASE_ID=""
ARG_PAT=""
ARG_PROFILE=""
ARG_DSN=""
ARG_HOST=""
ARG_USER=""
ARG_PASSWORD=""
ARG_STAGING=0
ARG_LAKEHOUSE_VERSION=""
ARG_SLC_VERSION=""
ARG_SCHEMA="$DEFAULT_SCHEMA"
ARG_HELP=0

CONNECTIVITY_MODE=""
HOST_DSN=""
WORKDIR=""
RESOLVED_ENGINE_TAG=""
RESOLVED_ENGINE_VERSION=""
RESOLVED_SLC_TAG=""
RESOLVED_SLC_VERSION=""

# --- Output helpers ----------------------------------------------------------
# Progress goes to stderr; user-facing deliverables (resolved versions, template) to stdout.
emit() { printf '%s\n' "$*"; }
log()  { printf '%s\n' "$*" >&2; }
err()  { printf 'ERROR: %s\n' "$*" >&2; }

have_cmd() { command -v "$1" >/dev/null 2>&1; }

# Percent-encodes a string for safe inclusion in a DSN's userinfo component (RFC 3986 unreserved
# set only: A-Za-z0-9-_.~). --user/--password may contain reserved URI characters (@, :, /, ?, #)
# that would otherwise corrupt or change the meaning of the exasol:// DSN built by string
# interpolation.
url_encode() {
  local s="$1" i c out=""
  local len=${#s}
  for ((i = 0; i < len; i++)); do
    c="${s:i:1}"
    case "$c" in
      [A-Za-z0-9.~_-]) out+="$c" ;;
      *) out+="$(printf '%%%02X' "'$c")" ;;
    esac
  done
  printf '%s\n' "$out"
}

# --- JSON helpers ------------------------------------------------------------
# Extracts a top-level JSON string field by name (no jq; bash regex). Returns 1 if absent.
extract_json_string_field() {
  local json="$1" field="$2"
  local re="\"$field\"[[:space:]]*:[[:space:]]*\"([^\"]*)\""
  if [[ "$json" =~ $re ]]; then
    printf '%s\n' "${BASH_REMATCH[1]}"
    return 0
  fi
  return 1
}

usage() {
  cat <<'USAGE'
install-saas.sh - install lakehouse-engine onto an Exasol SaaS database.

Required:
  --account-id <id>        SaaS account id (from the SaaS web console)
  --database-id <id>       SaaS database id (from the SaaS web console)
  --pat <token>            SaaS personal access token (or set EXASOL_PAT)

Connectivity (exactly one mode):
  --profile <name>         exapump named profile
  --dsn <dsn>              direct exapump DSN (or set EXAPUMP_DSN)
  --host <host:port> --user <u> --password <p>
                           direct connection assembled into a DSN;
                           --host MUST include the port (e.g. myhost:8563) — there is no --port flag

Optional:
  --staging                target cloud-staging.exasol.com (default: cloud.exasol.com)
  --schema <name>          deployment schema (default: LHVS)
  --lakehouse-version <v>  pin the engine version (default: latest release)
  --slc-version <v>        pin the SLC version (default: latest release)
  --help                   show this help

The script stops at a query-ready product install and prints a CONNECTION / VIRTUAL SCHEMA
template as the next step; it does not create catalog objects.
USAGE
}

# --- Argument parsing --------------------------------------------------------
parse_args() {
  ARG_ACCOUNT_ID=""
  ARG_DATABASE_ID=""
  ARG_PAT="${EXASOL_PAT:-}"
  ARG_PROFILE=""
  ARG_DSN="${EXAPUMP_DSN:-}"
  ARG_HOST=""
  ARG_USER=""
  ARG_PASSWORD=""
  ARG_STAGING=0
  ARG_LAKEHOUSE_VERSION=""
  ARG_SLC_VERSION=""
  ARG_SCHEMA="$DEFAULT_SCHEMA"
  ARG_HELP=0

  while [[ $# -gt 0 ]]; do
    local flag="$1"
    case "$flag" in
      --staging) ARG_STAGING=1; shift; continue ;;
      --help|-h) ARG_HELP=1; shift; continue ;;
      --account-id|--database-id|--pat|--profile|--dsn|--host|--user|--password|--schema|--lakehouse-version|--slc-version) ;;
      *) err "unknown argument: $flag"; return 1 ;;
    esac
    if [[ $# -lt 2 ]]; then
      err "$flag requires a value"
      return 1
    fi
    local value="$2"
    case "$flag" in
      --account-id)        ARG_ACCOUNT_ID="$value" ;;
      --database-id)       ARG_DATABASE_ID="$value" ;;
      --pat)               ARG_PAT="$value" ;;
      --profile)           ARG_PROFILE="$value" ;;
      --dsn)               ARG_DSN="$value" ;;
      --host)              ARG_HOST="$value" ;;
      --user)              ARG_USER="$value" ;;
      --password)          ARG_PASSWORD="$value" ;;
      --schema)            ARG_SCHEMA="$value" ;;
      --lakehouse-version) ARG_LAKEHOUSE_VERSION="$value" ;;
      --slc-version)       ARG_SLC_VERSION="$value" ;;
    esac
    shift 2
  done
  return 0
}

validate_required() {
  local missing=0
  if [[ -z "$ARG_ACCOUNT_ID" ]]; then
    err "missing --account-id (find it in the Exasol SaaS web console)"
    missing=1
  fi
  if [[ -z "$ARG_DATABASE_ID" ]]; then
    err "missing --database-id (find it in the Exasol SaaS web console under your database)"
    missing=1
  fi
  if [[ -z "$ARG_PAT" ]]; then
    err "missing SaaS PAT: pass --pat or set EXASOL_PAT (create a personal access token in the Exasol SaaS web console)"
    missing=1
  fi
  [[ "$missing" -eq 0 ]]
}

# Prints the resolved connectivity mode (profile|dsn|host) on stdout, or errors and returns 1.
# Reads the ARG_* globals directly (consistent with the rest of the file).
validate_connectivity() {
  local modes=0 chosen=""
  if [[ -n "$ARG_PROFILE" ]]; then modes=$((modes + 1)); chosen="profile"; fi
  if [[ -n "$ARG_DSN" ]]; then modes=$((modes + 1)); chosen="dsn"; fi
  if [[ -n "$ARG_HOST" || -n "$ARG_USER" || -n "$ARG_PASSWORD" ]]; then
    modes=$((modes + 1)); chosen="host"
  fi
  if [[ "$modes" -ne 1 ]]; then
    err "exactly one connectivity mode is required: --profile, OR --dsn/EXAPUMP_DSN, OR --host/--user/--password"
    return 1
  fi
  if [[ "$chosen" == "host" ]]; then
    if [[ -z "$ARG_HOST" || -z "$ARG_USER" || -z "$ARG_PASSWORD" ]]; then
      err "host connectivity mode requires all of --host, --user, and --password"
      return 1
    fi
    if [[ ! "$ARG_HOST" =~ ^[^[:space:]]+:[0-9]+$ ]]; then
      err "--host must be host:port (e.g. myhost:8563); got '$ARG_HOST' with no port. There is no separate --port flag."
      return 1
    fi
  fi
  printf '%s\n' "$chosen"
  return 0
}

check_prereqs() {
  local ok=1
  have_cmd gh      || { err "required tool 'gh' (GitHub CLI) not found on PATH. Install it: https://cli.github.com/"; ok=0; }
  have_cmd exapump || { err "required tool 'exapump' not found on PATH. Install it: https://github.com/exasol-labs/exapump"; ok=0; }
  have_cmd curl    || { err "required tool 'curl' not found on PATH. Install it via your OS package manager: https://curl.se/"; ok=0; }
  [[ "$ok" -eq 1 ]]
}

check_gh_auth() {
  if ! gh auth status </dev/null >/dev/null 2>&1; then
    err "GitHub CLI is not authenticated. Run: gh auth login  (needed for private $ENGINE_REPO release access)."
    return 1
  fi
}

# --- Target base -------------------------------------------------------------
resolve_saas_base() {
  if [[ "$ARG_STAGING" -eq 1 ]]; then
    printf '%s\n' "$SAAS_STAGING_BASE"
  else
    printf '%s\n' "$SAAS_PROD_BASE"
  fi
}

# --- Version resolution ------------------------------------------------------
normalize_version() {
  local v="$1"
  printf '%s\n' "${v#v}"
}

version_to_tag() {
  local v="$1"
  case "$v" in
    v*) printf '%s\n' "$v" ;;
    *)  printf '%s\n' "v$v" ;;
  esac
}

resolve_versions() {
  if [[ -n "$ARG_LAKEHOUSE_VERSION" ]]; then
    RESOLVED_ENGINE_TAG="$(version_to_tag "$ARG_LAKEHOUSE_VERSION")"
  else
    local ej
    if ! ej="$(gh api "repos/$ENGINE_REPO/releases/latest" </dev/null 2>&1)"; then
      err "could not resolve the latest $ENGINE_REPO release via 'gh api'. Ensure gh is authenticated and has access to the private repo."
      return 1
    fi
    if ! RESOLVED_ENGINE_TAG="$(extract_json_string_field "$ej" "tag_name")"; then
      err "could not parse a tag_name from the latest $ENGINE_REPO release response."
      return 1
    fi
  fi
  RESOLVED_ENGINE_VERSION="$(normalize_version "$RESOLVED_ENGINE_TAG")"

  if [[ -n "$ARG_SLC_VERSION" ]]; then
    RESOLVED_SLC_TAG="$(version_to_tag "$ARG_SLC_VERSION")"
  else
    local sj
    if ! sj="$(gh api "repos/$SLC_REPO/releases/latest" </dev/null 2>&1)"; then
      err "could not resolve the latest $SLC_REPO release via 'gh api'."
      return 1
    fi
    if ! RESOLVED_SLC_TAG="$(extract_json_string_field "$sj" "tag_name")"; then
      err "could not parse a tag_name from the latest $SLC_REPO release response."
      return 1
    fi
  fi
  RESOLVED_SLC_VERSION="$(normalize_version "$RESOLVED_SLC_TAG")"

  emit "Resolved lakehouse-engine version: $RESOLVED_ENGINE_VERSION (tag $RESOLVED_ENGINE_TAG)"
  emit "Resolved language-container (SLC) version: $RESOLVED_SLC_VERSION (tag $RESOLVED_SLC_TAG)"
  return 0
}

# --- SaaS REST helpers -------------------------------------------------------
saas_db_reachable() {
  local base url
  base="$(resolve_saas_base)"
  url="$base/api/v1/accounts/$ARG_ACCOUNT_ID/databases/$ARG_DATABASE_ID"
  if ! curl -fsS -H "Authorization: Bearer $ARG_PAT" "$url" </dev/null >/dev/null 2>&1; then
    local target="production"
    [[ "$ARG_STAGING" -eq 1 ]] && target="staging"
    err "SaaS database not reachable: GET /api/v1/accounts/<account>/databases/<database> failed on the $target target. Verify --account-id, --database-id, and the PAT (and --staging if applicable)."
    return 1
  fi
}

saas_verify_listed() {
  local filename="$1" base url resp
  base="$(resolve_saas_base)"
  url="$base/api/v1/accounts/$ARG_ACCOUNT_ID/databases/$ARG_DATABASE_ID/files"
  if ! resp="$(curl -fsS -H "Authorization: Bearer $ARG_PAT" "$url" </dev/null 2>&1)"; then
    return 1
  fi
  # Match the quoted JSON string, not a bare substring: without the quote boundary,
  # "rustslc.tar.gz" would also match a longer stored name like "rustslc.tar.gz.bak".
  [[ "$resp" == *"\"$filename\""* ]]
}

# Atomic POST-presigned-then-PUT upload; verifies the file is listed afterwards.
saas_upload_file() {
  local local_path="$1" filename="$2" base url resp presigned
  base="$(resolve_saas_base)"
  url="$base/api/v1/accounts/$ARG_ACCOUNT_ID/databases/$ARG_DATABASE_ID/files/$filename"
  if ! resp="$(curl -fsS -X POST -H "Authorization: Bearer $ARG_PAT" "$url" </dev/null 2>&1)"; then
    err "SaaS upload of $filename failed: could not obtain a presigned URL (POST files endpoint). Check the account/database id and PAT scopes."
    return 1
  fi
  if ! presigned="$(extract_json_string_field "$resp" "url")"; then
    err "SaaS upload of $filename failed: the files endpoint response contained no presigned 'url' field."
    return 1
  fi
  if ! curl -fsS -X PUT --upload-file "$local_path" "$presigned" </dev/null >/dev/null 2>&1; then
    err "SaaS upload of $filename failed: PUT to the presigned URL failed (the URL expires ~600s and is host-signed)."
    return 1
  fi
  if ! saas_verify_listed "$filename"; then
    err "SaaS upload of $filename failed verification: the file was not listed by the files API after upload."
    return 1
  fi
  log "Uploaded and verified $filename."
  return 0
}

# --- SQL execution -----------------------------------------------------------
run_sql() {
  local sql="$1"
  case "$CONNECTIVITY_MODE" in
    profile) exapump sql --profile "$ARG_PROFILE" "$sql" </dev/null ;;
    dsn)     exapump sql -d "$ARG_DSN" "$sql" </dev/null ;;
    host)    exapump sql -d "$HOST_DSN" "$sql" </dev/null ;;
    *)       err "internal error: connectivity mode not resolved"; return 1 ;;
  esac
}

# Filters exapump tabular output down to the first data line.
extract_query_value() {
  local raw="$1" line
  while IFS= read -r line; do
    case "$line" in
      \[*)            continue ;;
      SYSTEM_VALUE*)  continue ;;
      [0-9]*)         continue ;;
      "")             continue ;;
      *Error*)        continue ;;
    esac
    line="${line#"${line%%[![:space:]]*}"}"
    line="${line%"${line##*[![:space:]]}"}"
    printf '%s\n' "$line"
    return 0
  done <<EOF_QV
$raw
EOF_QV
  return 0
}

read_script_languages() {
  local out value
  if ! out="$(run_sql "SELECT SYSTEM_VALUE FROM EXA_PARAMETERS WHERE PARAMETER_NAME='SCRIPT_LANGUAGES'" 2>&1)"; then
    err "could not read the current SCRIPT_LANGUAGES value from EXA_PARAMETERS."
    return 1
  fi
  value="$(extract_query_value "$out")"
  if [[ -z "${value//[[:space:]]/}" ]]; then
    err "the SCRIPT_LANGUAGES read from EXA_PARAMETERS succeeded but yielded an empty value. A live Exasol database always has at least one script language registered, so this is an anomaly (an unexpected query-output shape), not a legitimate empty state. Refusing to proceed: appending the RUST segment to an empty value would drop every pre-existing language once ALTER SYSTEM SET SCRIPT_LANGUAGES is issued."
    return 1
  fi
  printf '%s\n' "$value"
}

# Appends the fixed RUST segment, or replaces a single existing RUST= segment in place,
# preserving every other language entry and its order. Yields exactly one RUST= entry.
compute_script_languages() {
  local current="$1"
  local restore_glob=0
  case "$-" in
    *f*) : ;;
    *)   restore_glob=1; set -f ;;
  esac
  local -a tokens
  # shellcheck disable=SC2206  # intentional word-splitting of a space-separated alias list; set -f above guards globbing
  tokens=( $current )
  [[ "$restore_glob" -eq 1 ]] && set +f

  local result="" placed=0 tok
  if [[ ${#tokens[@]} -gt 0 ]]; then
    for tok in "${tokens[@]}"; do
      [[ -z "$tok" ]] && continue
      if [[ "$tok" == RUST=* ]]; then
        if [[ "$placed" -eq 0 ]]; then
          result="${result:+$result }$RUST_LANG_SEGMENT"
          placed=1
        fi
      else
        result="${result:+$result }$tok"
      fi
    done
  fi
  if [[ "$placed" -eq 0 ]]; then
    result="${result:+$result }$RUST_LANG_SEGMENT"
  fi
  printf '%s\n' "$result"
}

# --- DDL strings -------------------------------------------------------------
ddl_create_schema() {
  printf 'CREATE SCHEMA IF NOT EXISTS %s' "$1"
}

ddl_adapter() {
  printf 'CREATE OR REPLACE RUST ADAPTER SCRIPT %s.LAKEHOUSE_ADAPTER AS\n%%udf_object %s' "$1" "$2"
}

ddl_scan() {
  printf 'CREATE OR REPLACE RUST SCALAR SCRIPT %s.LAKEHOUSE_SCAN(common VARCHAR(2000000), files VARCHAR(2000000))\nEMITS (...) AS\n%%udf_object %s' "$1" "$2"
}

ddl_distinct_merge_count() {
  printf 'CREATE OR REPLACE RUST SCALAR SCRIPT %s.LAKEHOUSE_DISTINCT_MERGE_COUNT(partials VARCHAR(2000000))\nRETURNS DECIMAL(20,0) AS\n%%udf_object %s' "$1" "$2"
}

ddl_distribute_files() {
  printf 'CREATE OR REPLACE LUA SET SCRIPT %s.LAKEHOUSE_DISTRIBUTE_FILES(files VARCHAR(2000000))\nEMITS (files VARCHAR(2000000)) AS\nfunction run(ctx)\n    repeat\n        ctx.emit(ctx.files)\n    until not ctx.next()\nend' "$1"
}

smoke_test_sql() {
  printf "SELECT %s.LAKEHOUSE_SCAN('x', 'y') EMITS (r VARCHAR(2000000)) FROM (SELECT 1)" "$1"
}

# --- Install steps -----------------------------------------------------------
download_slc() {
  local asset="lc-rust-$RESOLVED_SLC_VERSION.tar.gz"
  if ! gh release download "$RESOLVED_SLC_TAG" --repo "$SLC_REPO" --pattern "$asset" --dir "$WORKDIR" --clobber </dev/null >/dev/null 2>&1; then
    err "failed to download $asset (tag $RESOLVED_SLC_TAG) from $SLC_REPO via gh."
    return 1
  fi
  if ! mv -f "$WORKDIR/$asset" "$WORKDIR/rustslc.tar.gz"; then
    err "failed to rename $asset to rustslc.tar.gz."
    return 1
  fi
}

download_engine() {
  if ! gh release download "$RESOLVED_ENGINE_TAG" --repo "$ENGINE_REPO" --pattern "$ENGINE_ASSET" --dir "$WORKDIR" --clobber </dev/null >/dev/null 2>&1; then
    err "failed to download $ENGINE_ASSET (tag $RESOLVED_ENGINE_TAG) from $ENGINE_REPO via gh."
    return 1
  fi
}

register_slc() {
  log "Installing Rust SLC $RESOLVED_SLC_VERSION ..."
  download_slc || return 1
  saas_upload_file "$WORKDIR/rustslc.tar.gz" "rustslc.tar.gz" || return 1
  local current new
  if ! current="$(read_script_languages)"; then
    return 1
  fi
  new="$(compute_script_languages "$current")"
  log "Setting SCRIPT_LANGUAGES (RUST segment append/replace)."
  if ! run_sql "ALTER SYSTEM SET SCRIPT_LANGUAGES='$new'" >/dev/null 2>&1; then
    err "ALTER SYSTEM SET SCRIPT_LANGUAGES failed. The connecting account likely lacks the SYSTEM (admin) privilege required to register a script language."
    return 1
  fi
  return 0
}

create_engine_scripts() {
  local schema="$ARG_SCHEMA" so="$ENGINE_SO_PATH" stmt
  local -a statements=(
    "$(ddl_create_schema "$schema")"
    "$(ddl_adapter "$schema" "$so")"
    "$(ddl_scan "$schema" "$so")"
    "$(ddl_distinct_merge_count "$schema" "$so")"
    "$(ddl_distribute_files "$schema")"
  )
  for stmt in "${statements[@]}"; do
    if ! run_sql "$stmt" >/dev/null 2>&1; then
      err "failed to create a deployment script in schema $schema (statement starting: ${stmt%%$'\n'*})."
      return 1
    fi
  done
  return 0
}

install_engine() {
  log "Installing lakehouse-engine $RESOLVED_ENGINE_VERSION ..."
  download_engine || return 1
  saas_upload_file "$WORKDIR/$ENGINE_ASSET" "$ENGINE_ASSET" || return 1
  create_engine_scripts || return 1
  return 0
}

# mismatch -> fingerprint failure; anomaly -> unexpected rows; pass -> any other error.
classify_fingerprint_response() {
  local rc="$1" output="$2"
  if [[ "$output" == *"Fingerprint mismatch"* ]]; then
    printf 'mismatch\n'
    return 0
  fi
  if [[ "$rc" -eq 0 ]]; then
    printf 'anomaly\n'
    return 0
  fi
  printf 'pass\n'
  return 0
}

run_smoke_test() {
  local sql out rc verdict
  sql="$(smoke_test_sql "$ARG_SCHEMA")"
  if out="$(run_sql "$sql" 2>&1)"; then rc=0; else rc=$?; fi
  verdict="$(classify_fingerprint_response "$rc" "$out")"
  case "$verdict" in
    mismatch)
      err "fingerprint smoke test FAILED: the registered SLC does not match this release's exasol-udf-sdk/exasol-udf-macros pin. Align the SLC version (see --slc-version) with the engine release and re-run."
      return 1 ;;
    anomaly)
      err "fingerprint smoke test anomaly: the placeholder scan spec ('x','y') returned rows with no error, which can never happen for a valid install. Aborting."
      return 1 ;;
    pass)
      log "Fingerprint smoke test passed (a non-fingerprint error is expected for the placeholder args)."
      return 0 ;;
  esac
}

print_next_step_template() {
  local schema="$1"
  emit ""
  emit "=== Next step: create the catalog CONNECTION and VIRTUAL SCHEMA (NOT created by this installer) ==="
  emit "-- These objects are dataset-specific. Edit the placeholders below and run the SQL yourself:"
  emit ""
  emit "CREATE OR REPLACE CONNECTION LAKEHOUSE_CATALOG_CREDS"
  emit "  TO '<catalog-uri>'"
  emit "  USER ''"
  emit "  IDENTIFIED BY '{"
  emit "    \"warehouse\":  \"<warehouse>\","
  emit "    \"region\":     \"<region>\","
  emit "    \"access_key\": \"<access_key>\","
  emit "    \"secret_key\": \"<secret_key>\""
  emit "  }';"
  emit ""
  emit "CREATE VIRTUAL SCHEMA <MY_LAKEHOUSE>"
  emit "USING $schema.LAKEHOUSE_ADAPTER WITH"
  emit "  CATALOG_CONNECTION = 'LAKEHOUSE_CATALOG_CREDS'"
  emit "  ICEBERG_NAMESPACE  = '<namespace>'"
  emit "  ALLOW_HTTP         = 'false';"
}

# --- Entry point -------------------------------------------------------------
main() {
  set -uo pipefail

  parse_args "$@" || exit 1
  if [[ "$ARG_HELP" -eq 1 ]]; then
    usage
    exit 0
  fi

  validate_required || exit 1
  if ! CONNECTIVITY_MODE="$(validate_connectivity)"; then
    exit 1
  fi
  if [[ "$CONNECTIVITY_MODE" == "host" ]]; then
    local enc_user enc_password
    enc_user="$(url_encode "$ARG_USER")"
    enc_password="$(url_encode "$ARG_PASSWORD")"
    HOST_DSN="exasol://$enc_user:$enc_password@$ARG_HOST?validateservercertificate=0"
  fi

  check_prereqs || exit 1
  check_gh_auth || exit 1
  saas_db_reachable || exit 1

  if ! WORKDIR="$(mktemp -d "${TMPDIR:-/tmp}/lhvs-install.XXXXXX" 2>/dev/null)"; then
    err "failed to create a temporary working directory."
    exit 1
  fi
  trap 'rm -rf "$WORKDIR"' EXIT

  resolve_versions || exit 1
  register_slc || exit 1
  install_engine || exit 1
  run_smoke_test || exit 1

  print_next_step_template "$ARG_SCHEMA"
  emit ""
  emit "lakehouse-engine is installed and query-ready in schema $ARG_SCHEMA."
  exit 0
}

if [[ -z "${BASH_SOURCE[0]:-}" || "${BASH_SOURCE[0]}" == "${0}" ]]; then
  main "$@"
fi
