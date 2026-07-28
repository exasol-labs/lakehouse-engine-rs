#!/usr/bin/env bash
# One-command installer that provisions lakehouse-engine onto an Exasol SaaS database:
# registers the Rust SLC, uploads and registers the engine .so plus its three scripts, and
# verifies the load with a fingerprint smoke test. Stops at a query-ready product install and
# prints the next-step CONNECTION / VIRTUAL SCHEMA template; it does NOT create catalog objects.
#
# Distributed one-liner (piped into bash over stdin):
#   curl -fsSL -H "Authorization: Bearer $GITHUB_TOKEN" \
#     -H "Accept: application/vnd.github.raw" \
#     https://api.github.com/repos/exasol-labs/lakehouse-engine-rs/contents/deploy/scripts/install.sh \
#   | bash -s -- --account-id $ACC --database-id $DB --profile staging
#
# The file is sourceable: its functions can be sourced and unit-tested without running the
# installer, because `main` runs only when the file is executed or piped, never when sourced.
#
# Bash 3.2+ (stock macOS). No jq. Every subprocess (curl/exapump) reads stdin from /dev/null
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
ARG_GITHUB_TOKEN=""
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
RESOLVED_PAT=""
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

# Percent-decodes a string; the exact inverse of url_encode() above.
url_decode() {
  local s="$1" i c out=""
  local len=${#s}
  for ((i = 0; i < len; i++)); do
    c="${s:i:1}"
    if [[ "$c" == "%" && $((i + 2)) -lt len ]]; then
      out+="$(printf "\\$(printf '%03o' "0x${s:i+1:2}")")"
      i=$((i + 2))
    else
      out+="$c"
    fi
  done
  printf '%s\n' "$out"
}

# Extracts the still-percent-encoded password segment from an exasol://user:password@host...
# DSN. Splits on the LAST '@' before the host (the greedy match preserves a RAW, un-encoded '@'
# inside the password, e.g. 'user:pass@word@host' extracts 'pass@word') and the FIRST ':' after
# the scheme (separating user from password). Returns 1 with no output if the DSN has no
# ':password@' segment (no '@' at all, or no ':' before it).
extract_dsn_password() {
  local dsn="$1"
  local re='^[a-zA-Z][a-zA-Z0-9+.-]*://[^:]*:(.*)@.*$'
  if [[ "$dsn" =~ $re ]]; then
    printf '%s\n' "${BASH_REMATCH[1]}"
    return 0
  fi
  return 1
}

# --- JSON helpers ------------------------------------------------------------
# Un-escapes a raw JSON string value: the \\uXXXX numeric escape (ASCII range only -- sufficient
# for the presigned URLs and tags this script extracts, which are pure ASCII) plus the common
# single-char escapes. Needed because some backends (notably Go's encoding/json, which
# HTML-escapes '&', '<', '>' by default) return presigned URLs with their '&' query-parameter
# separators replaced by the literal six-character escape sequence for '&' -- collapsing every
# parameter after the first into one unparsable blob. That surfaces as S3 rejecting the request
# with AuthorizationQueryParametersError ("X-Amz-Algorithm only supports ..."), because the value
# curl actually sends for X-Amz-Algorithm ends up being "AWS4-HMAC-SHA256" concatenated with the
# rest of the still-escaped query string, rather than the bare algorithm name.
json_unescape() {
  local rest="$1" out="" chunk hex dec ch
  while [[ "$rest" == *'\'* ]]; do
    chunk="${rest%%\\*}"
    out+="$chunk"
    rest="${rest#*\\}"
    case "$rest" in
      u[0-9A-Fa-f][0-9A-Fa-f][0-9A-Fa-f][0-9A-Fa-f]*)
        hex="${rest:1:4}"
        rest="${rest:5}"
        dec=$((16#$hex))
        if [[ "$dec" -lt 128 ]]; then
          ch="$(printf "\\$(printf '%03o' "$dec")")"
        else
          ch="?"
        fi
        out+="$ch"
        ;;
      \"*) out+='"'; rest="${rest:1}" ;;
      \\*) out+='\'; rest="${rest:1}" ;;
      /*)  out+='/'; rest="${rest:1}" ;;
      n*)  out+=$'\n'; rest="${rest:1}" ;;
      t*)  out+=$'\t'; rest="${rest:1}" ;;
      r*)  out+=$'\r'; rest="${rest:1}" ;;
      *)   out+="\\${rest:0:1}"; rest="${rest:1}" ;;
    esac
  done
  out+="$rest"
  printf '%s\n' "$out"
}

# Extracts a top-level JSON string field by name (no jq; bash regex), un-escaping its value.
# Returns 1 if absent.
extract_json_string_field() {
  local json="$1" field="$2"
  local re="\"$field\"[[:space:]]*:[[:space:]]*\"([^\"]*)\""
  if [[ "$json" =~ $re ]]; then
    json_unescape "${BASH_REMATCH[1]}"
    return 0
  fi
  return 1
}

# Given a GitHub release JSON blob (as returned by GET /releases/latest or /releases/tags/<tag>)
# and an asset file name, prints that asset's numeric id. Returns 1 if the asset is absent.
#
# No jq: GitHub's REST API stably pretty-prints its JSON with a fixed 2-space indent, so each
# element of the "assets" array is delimited by a line that is exactly a 4-space-indented "{" /
# "}," pair, and the asset's OWN fields sit at 6-space indent -- one level shallower than a
# nested object's fields (e.g. "uploader", 8-space indent). Scanning strictly at the 6-space
# depth means a nested object's own "id" (every asset carries an "uploader" with its own numeric
# "id") is never mistaken for the asset's id. The scan is bounded to the "assets": [ ... ] block
# -- entered on the 2-space "assets": [ line and exited on its matching 2-space "]" close --
# rather than scanning the whole response, so it can't misfire on some other, unrelated
# array-of-objects field the release schema might grow in the future. Because both
# "id" and "name" are captured independently per asset block, field order within the block and
# the order of assets within the array are both irrelevant to correctness.
extract_asset_id_by_name() {
  local json="$1" target="$2"
  local in_assets=0 in_asset=0 id="" name="" line
  local assets_start_re='^  "assets": \[[[:space:]]*$'
  local assets_end_re='^  \],?[[:space:]]*$'
  local asset_open_re='^    \{[[:space:]]*$'
  local asset_close_re='^    \},?[[:space:]]*$'
  local id_field_re='^      "id"[[:space:]]*:[[:space:]]*([0-9]+),?[[:space:]]*$'
  local name_field_re='^      "name"[[:space:]]*:[[:space:]]*"([^"]*)",?[[:space:]]*$'

  while IFS= read -r line; do
    if [[ "$in_assets" -eq 0 ]]; then
      [[ "$line" =~ $assets_start_re ]] && in_assets=1
      continue
    fi
    if [[ "$in_asset" -eq 0 ]]; then
      if [[ "$line" =~ $assets_end_re ]]; then
        in_assets=0
        continue
      fi
      if [[ "$line" =~ $asset_open_re ]]; then
        in_asset=1
        id=""
        name=""
      fi
      continue
    fi
    if [[ "$line" =~ $asset_close_re ]]; then
      if [[ -n "$name" && "$name" == "$target" && -n "$id" ]]; then
        printf '%s\n' "$id"
        return 0
      fi
      in_asset=0
      continue
    fi
    if [[ "$line" =~ $id_field_re ]]; then
      id="${BASH_REMATCH[1]}"
      continue
    fi
    if [[ "$line" =~ $name_field_re ]]; then
      name="${BASH_REMATCH[1]}"
      continue
    fi
  done <<EOF_ASSETS
$json
EOF_ASSETS
  return 1
}

# --- Credential resolution ---------------------------------------------------
# Prints the exapump config.toml path this installer must read to mirror what
# `exapump sql --profile <name>` itself would resolve (confirmed via `strings $(command -v
# exapump)`: exapump honors EXAPUMP_CONFIG as a full-file-path override).
exapump_config_path() {
  printf '%s\n' "${EXAPUMP_CONFIG:-$HOME/.exapump/config.toml}"
}

# Reads the `password` key out of the named `[profile]` TOML section in $config_path. Bounded
# scan (same discipline as extract_asset_id_by_name's bounded JSON block): only lines between the
# named section's own header and the next `[`-headed header (or EOF) are considered, so a
# same-named key in a different section is never matched. Returns 1 with no output if the file,
# section, or key is absent.
read_profile_password() {
  local profile="$1" config_path="$2" line trimmed_line
  local other_section_re='^\[.*\][[:space:]]*$'
  local password_re="^password[[:space:]]*=[[:space:]]*[\"']([^\"']*)[\"'][[:space:]]*\$"
  local in_section=0

  [[ -f "$config_path" ]] || return 1

  while IFS= read -r line; do
    if [[ "$in_section" -eq 0 ]]; then
      trimmed_line="${line%"${line##*[![:space:]]}"}"
      [[ "$trimmed_line" == "[$profile]" ]] && in_section=1
      continue
    fi
    if [[ "$line" =~ $other_section_re ]]; then
      return 1
    fi
    if [[ "$line" =~ $password_re ]]; then
      printf '%s\n' "${BASH_REMATCH[1]}"
      return 0
    fi
  done <"$config_path"
  return 1
}

# Sets the global RESOLVED_PAT from whichever connectivity credential is already in use: on
# Exasol SaaS the PAT IS the SQL password, so this derives the one REST bearer credential instead
# of asking the user to supply it a second time. Never prints the resolved value.
resolve_pat() {
  case "$CONNECTIVITY_MODE" in
    host)
      RESOLVED_PAT="$ARG_PASSWORD"
      ;;
    dsn)
      local encoded
      if ! encoded="$(extract_dsn_password "$ARG_DSN")" || [[ -z "$encoded" ]]; then
        err "could not derive the SaaS REST credential: dsn connectivity mode requires a DSN with a ':password@' segment, but none was found in --dsn/EXAPUMP_DSN."
        return 1
      fi
      RESOLVED_PAT="$(url_decode "$encoded")"
      ;;
    profile)
      local config_path
      config_path="$(exapump_config_path)"
      if ! RESOLVED_PAT="$(read_profile_password "$ARG_PROFILE" "$config_path")" || [[ -z "$RESOLVED_PAT" ]]; then
        err "could not derive the SaaS REST credential: no 'password' key found for profile '$ARG_PROFILE' in $config_path."
        return 1
      fi
      ;;
    *)
      err "internal error: connectivity mode not resolved"
      return 1
      ;;
  esac
  return 0
}

usage() {
  cat <<'USAGE'
install-saas.sh - install lakehouse-engine onto an Exasol SaaS database.

Required:
  --account-id <id>        SaaS account id (from the SaaS web console)
  --database-id <id>       SaaS database id (from the SaaS web console)
  --github-token <token>   GitHub token with read access to the private lakehouse-engine-rs
                           repo (or set GITHUB_TOKEN)

Connectivity (exactly one mode):
  --profile <name>         exapump named profile
  --dsn <dsn>              direct exapump DSN (or set EXAPUMP_DSN)
  --host <host:port> --user <u> --password <p>
                           direct connection assembled into a DSN;
                           --host MUST include the port (e.g. myhost:8563) — there is no --port flag

  The SaaS REST API credential (Bearer token) is derived automatically from whichever
  connectivity mode is used above -- on Exasol SaaS the PAT IS the SQL password, so there is no
  separate flag for it.

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
  ARG_GITHUB_TOKEN="${GITHUB_TOKEN:-}"
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
      --account-id|--database-id|--github-token|--profile|--dsn|--host|--user|--password|--schema|--lakehouse-version|--slc-version) ;;
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
      --github-token)      ARG_GITHUB_TOKEN="$value" ;;
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
  if [[ -z "$ARG_GITHUB_TOKEN" ]]; then
    err "missing GitHub token: pass --github-token or set GITHUB_TOKEN (a token with read access to the private $ENGINE_REPO repository)"
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
  have_cmd exapump || { err "required tool 'exapump' not found on PATH. Install it: https://github.com/exasol-labs/exapump"; ok=0; }
  have_cmd curl    || { err "required tool 'curl' not found on PATH. Install it via your OS package manager: https://curl.se/"; ok=0; }
  [[ "$ok" -eq 1 ]]
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
    if ! ej="$(curl -fsS -H "Authorization: Bearer $ARG_GITHUB_TOKEN" "https://api.github.com/repos/$ENGINE_REPO/releases/latest" </dev/null 2>&1)"; then
      err "could not resolve the latest $ENGINE_REPO release via the GitHub REST API. Ensure GITHUB_TOKEN/--github-token has read access to the private repo."
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
    if ! sj="$(curl -fsS -H "Authorization: Bearer $ARG_GITHUB_TOKEN" "https://api.github.com/repos/$SLC_REPO/releases/latest" </dev/null 2>&1)"; then
      err "could not resolve the latest $SLC_REPO release via the GitHub REST API."
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
  if ! curl -fsS -H "Authorization: Bearer $RESOLVED_PAT" "$url" </dev/null >/dev/null 2>&1; then
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
  if ! resp="$(curl -fsS -H "Authorization: Bearer $RESOLVED_PAT" "$url" </dev/null 2>&1)"; then
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
  if ! resp="$(curl -fsS -X POST -H "Authorization: Bearer $RESOLVED_PAT" "$url" </dev/null 2>&1)"; then
    err "SaaS upload of $filename failed: could not obtain a presigned URL (POST files endpoint). Check the account/database id and PAT scopes. curl said: $resp"
    return 1
  fi
  if ! presigned="$(extract_json_string_field "$resp" "url")"; then
    err "SaaS upload of $filename failed: the files endpoint response contained no presigned 'url' field. Response: $resp"
    return 1
  fi
  # No -f here: on a non-2xx response we need the response BODY (the storage host's own error
  # detail, e.g. an S3 <Error><Code>/<Message> block) to know WHY the PUT was rejected -- -f
  # would suppress that body along with the status line. -w prints just the status code to
  # stdout once the body itself is diverted to a file, and stderr is captured separately for a
  # transport-level failure (connection refused, timeout, ...) that never got an HTTP response.
  local put_body_file="$WORKDIR/${filename}.put-response" put_err_file="$WORKDIR/${filename}.put-stderr"
  local put_http_code
  if ! put_http_code="$(curl -sS -o "$put_body_file" -w '%{http_code}' -X PUT --upload-file "$local_path" "$presigned" </dev/null 2>"$put_err_file")"; then
    err "SaaS upload of $filename failed: PUT to the presigned URL failed before completing (transport error). curl said: $(cat "$put_err_file" 2>/dev/null)"
    return 1
  fi
  if [[ "$put_http_code" != 2* ]]; then
    err "SaaS upload of $filename failed: PUT to the presigned URL returned HTTP $put_http_code (the URL expires ~600s and is host-signed). Response body: $(tr -d '\n' <"$put_body_file" 2>/dev/null | cut -c1-2000)"
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

ddl_distribute_files() {
  printf 'CREATE OR REPLACE LUA SET SCRIPT %s.LAKEHOUSE_DISTRIBUTE_FILES(files VARCHAR(2000000))\nEMITS (files VARCHAR(2000000)) AS\nfunction run(ctx)\n    repeat\n        ctx.emit(ctx.files)\n    until not ctx.next()\nend' "$1"
}

smoke_test_sql() {
  printf "SELECT %s.LAKEHOUSE_SCAN('x', 'y') EMITS (r VARCHAR(2000000)) FROM (SELECT 1)" "$1"
}

# --- Install steps -----------------------------------------------------------
# Downloads a named release asset for $repo at $tag into $dest_path via the authenticated
# GitHub REST API: resolve the release-by-tag JSON, look up the asset id by name (no jq --
# see extract_asset_id_by_name), then GET the asset binary by id.
#
# Uses plain -L, never --location-trusted: the assets endpoint authenticates the API call with
# our Authorization header, then 302s to a signed, host-authenticated storage URL that rejects a
# second auth mechanism. curl's default behavior since 7.58 is to strip Authorization across a
# cross-host redirect -- exactly what's needed here. --location-trusted would force the header
# through the redirect and break the signed URL.
download_release_asset() {
  local repo="$1" tag="$2" asset_name="$3" dest_path="$4"
  local release_json asset_id dl_err

  if ! release_json="$(curl -fsS -H "Authorization: Bearer $ARG_GITHUB_TOKEN" \
      "https://api.github.com/repos/$repo/releases/tags/$tag" </dev/null 2>&1)"; then
    err "could not fetch release '$tag' from $repo via the GitHub REST API."
    return 1
  fi

  if ! asset_id="$(extract_asset_id_by_name "$release_json" "$asset_name")"; then
    err "asset '$asset_name' not found in $repo release '$tag'."
    return 1
  fi

  if ! dl_err="$(curl -fsSL -H "Authorization: Bearer $ARG_GITHUB_TOKEN" \
      -H "Accept: application/octet-stream" \
      -o "$dest_path" \
      "https://api.github.com/repos/$repo/releases/assets/$asset_id" </dev/null 2>&1)"; then
    err "failed to download asset '$asset_name' (id $asset_id) from $repo release '$tag': $dl_err"
    return 1
  fi
  return 0
}

download_slc() {
  local asset="lc-rust-$RESOLVED_SLC_VERSION.tar.gz"
  download_release_asset "$SLC_REPO" "$RESOLVED_SLC_TAG" "$asset" "$WORKDIR/$asset" || return 1
  if ! mv -f "$WORKDIR/$asset" "$WORKDIR/rustslc.tar.gz"; then
    err "failed to rename $asset to rustslc.tar.gz."
    return 1
  fi
}

download_engine() {
  download_release_asset "$ENGINE_REPO" "$RESOLVED_ENGINE_TAG" "$ENGINE_ASSET" "$WORKDIR/$ENGINE_ASSET" || return 1
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
  resolve_pat || exit 1

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
