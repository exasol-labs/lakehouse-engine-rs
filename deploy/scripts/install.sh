#!/usr/bin/env bash
# One-command installer that provisions lakehouse-engine onto an Exasol SaaS or BucketFS
# database: registers the Rust SLC, uploads and registers the engine .so plus its three
# scripts, and verifies the load with a fingerprint smoke test. Stops at a query-ready
# product install and prints the next-step CONNECTION / VIRTUAL SCHEMA template; it does
# NOT create catalog objects.
#
# Distributed one-liner (piped into bash over stdin). Both source repos are public, so no
# token is required; pass --github-token/GITHUB_TOKEN only to raise the unauthenticated
# 60-requests/hour GitHub API rate limit:
#   curl -fsSL -H "Accept: application/vnd.github.raw" \
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

# Generic-BucketFS (Exasol AsApp / Docker / on-premise) layout. Both paths are BUCKET-RELATIVE with no
# leading slash and no bucket segment, because that is the grammar `exapump bucketfs cp|ls|rm`
# expects: exapump builds its URL as <scheme>://<bfs-host>:<bfs-port>/<bucket>/<path>, so the
# bucket comes from --bfs-bucket / the profile's bfs_bucket, never from the path argument.
# (Verified against a live Exasol container: `exapump bucketfs cp f /default/x/f` with bucket
# 'default' creates 'default/x/f' INSIDE the default bucket, and `exapump bucketfs ls /default`
# fails with "Path not found".) The bucket DOES appear in the %udf_object / RUST alias strings
# below, because those are read by the Exasol engine, not by exapump.
DEFAULT_BFS_BUCKET="default"
BFS_SLC_PATH="slc/lakehouse-rustslc.tar.gz"
BFS_ENGINE_SO_PATH="udf/liblakehouse_engine.so"

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
ARG_TARGET=""
ARG_BFS_HOST=""
ARG_BFS_PORT=""
ARG_BFS_BUCKET="$DEFAULT_BFS_BUCKET"
ARG_BFS_BUCKET_SET=0
ARG_BFS_WRITE_PASSWORD=""
ARG_SKIP_SLC=0
ARG_HELP=0

CONNECTIVITY_MODE=""
TARGET_MODE=""
TARGET_SO_UDF_OBJECT=""
TARGET_RUST_LANG_SEGMENT=""
TARGET_SLC_BFS_PATH=""
TARGET_ENGINE_BFS_PATH=""
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
    if [[ "$c" == "%" && $((i + 2)) -lt len && "${s:i+1:2}" =~ ^[0-9A-Fa-f]{2}$ ]]; then
      # shellcheck disable=SC2059  # the inner printf emits only octal digits (0-7), never a
      # format specifier, so the outer printf's format string is always a plain "\NNN" escape.
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
  # shellcheck disable=SC1003  # a literal single backslash inside single quotes, not an
  # escape attempt -- this is the correct, portable way to match/emit one '\' character.
  while [[ "$rest" == *'\'* ]]; do
    chunk="${rest%%\\*}"
    out+="$chunk"
    rest="${rest#*\\}"
    # shellcheck disable=SC1003  # the \\*) branch's '\' is a literal one-character backslash
    # string, not an escape attempt -- directives can't attach to a single case branch, so this
    # covers the whole case statement below.
    case "$rest" in
      u[0-9A-Fa-f][0-9A-Fa-f][0-9A-Fa-f][0-9A-Fa-f]*)
        hex="${rest:1:4}"
        rest="${rest:5}"
        dec=$((16#$hex))
        if [[ "$dec" -lt 128 ]]; then
          # shellcheck disable=SC2059  # same octal-only guarantee as url_decode() above.
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

# Reads the named `$key` out of the named `[profile]` TOML section in $config_path. Bounded
# scan (same discipline as extract_asset_id_by_name's bounded JSON block): only lines between the
# named section's own header and the next `[`-headed header (or EOF) are considered, so a
# same-named key in a different section is never matched. Returns 1 with no output if the file,
# section, or key is absent.
read_profile_key() {
  local profile="$1" key="$2" config_path="$3" line trimmed_line
  local other_section_re='^\[.*\][[:space:]]*$'
  local key_re="^${key}[[:space:]]*=[[:space:]]*[\"']([^\"']*)[\"'][[:space:]]*\$"
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
    if [[ "$line" =~ $key_re ]]; then
      printf '%s\n' "${BASH_REMATCH[1]}"
      return 0
    fi
  done <"$config_path"
  return 1
}

# Sets the global RESOLVED_PAT from whichever connectivity credential is already in use: on
# Exasol SaaS the PAT IS the SQL password, so this derives the one REST bearer credential instead
# of asking the user to supply it a second time. Never prints the resolved value.
resolve_saas_pat() {
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
      if ! RESOLVED_PAT="$(read_profile_key "$ARG_PROFILE" password "$config_path")" || [[ -z "$RESOLVED_PAT" ]]; then
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
install.sh - install lakehouse-engine onto an Exasol database.

Two install targets, auto-detected from the arguments:
  saas      Exasol SaaS      - selected by giving BOTH --account-id and --database-id
  bucketfs  Exasol AsApp / Docker / on-premise - selected by giving NEITHER (the default)

Connectivity (both modes; exactly one of the three):
  --profile <name>          exapump named profile
  --dsn <dsn>               direct exapump DSN (or set EXAPUMP_DSN)
  --host <host:port> --user <u> --password <p>
                            direct connection assembled into a DSN; --host MUST include the port
                            (e.g. myhost:8563) - there is no --port flag

Optional in both modes (both repos are public; a token only raises the unauthenticated
60-requests/hour GitHub API rate limit):
  --github-token <token>    GitHub token (or set GITHUB_TOKEN)

SaaS target only:
  --account-id <id>         SaaS account id (from the SaaS web console)
  --database-id <id>        SaaS database id (from the SaaS web console)
  --staging                 target cloud-staging.exasol.com (default: cloud.exasol.com)

  The SaaS REST credential (Bearer token) is derived automatically from whichever connectivity
  mode is used above - on Exasol SaaS the PAT IS the SQL password, so there is no flag for it.

BucketFS target only:
  --bfs-host <host>         BucketFS host (default: the profile's bfs_host, else its host)
  --bfs-port <port>         BucketFS port (default: the profile's bfs_port, else 2581)
  --bfs-bucket <name>       BucketFS bucket (default: default)
  --bfs-write-password <p>  BucketFS write password (default: the profile's bfs_write_password)

  Uploads go through `exapump bucketfs cp`, which reads its connection from the exapump profile
  and the --bfs-* overrides only - it accepts no DSN or user/password flags. So with --dsn or
  --host connectivity, --bfs-host AND --bfs-write-password must both be given explicitly.

Both modes:
  --target <saas|bucketfs>  assert the auto-detected target; fails on disagreement
  --schema <name>           deployment schema (default: LHVS)
  --lakehouse-version <v>   pin the engine version (default: latest release)
  --slc-version <v>         pin the SLC version (default: the version pinned by the resolved
                            engine release's own exasol-udf-sdk dependency, NOT
                            language-container-rs's own latest release)
  --skip-slc                do not download, upload or register the Rust SLC; install the engine
                            against the SLC already registered on the database
  --help                    show this help

Examples:
  install.sh --account-id ACC --database-id DB --profile saas-prod
  install.sh --profile my-exasol --bfs-write-password "$BFSPASS"

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
  ARG_TARGET=""
  ARG_BFS_HOST=""
  ARG_BFS_PORT=""
  ARG_BFS_BUCKET="$DEFAULT_BFS_BUCKET"
  ARG_BFS_BUCKET_SET=0
  ARG_BFS_WRITE_PASSWORD=""
  ARG_SKIP_SLC=0
  ARG_HELP=0

  while [[ $# -gt 0 ]]; do
    local flag="$1"
    case "$flag" in
      --staging)  ARG_STAGING=1; shift; continue ;;
      --skip-slc) ARG_SKIP_SLC=1; shift; continue ;;
      --help|-h)  ARG_HELP=1; shift; continue ;;
      --account-id|--database-id|--github-token|--profile|--dsn|--host|--user|--password|--schema|--lakehouse-version|--slc-version) ;;
      --target|--bfs-host|--bfs-port|--bfs-bucket|--bfs-write-password) ;;
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
      --target)            ARG_TARGET="$value" ;;
      --bfs-host)          ARG_BFS_HOST="$value" ;;
      --bfs-port)          ARG_BFS_PORT="$value" ;;
      --bfs-bucket)        ARG_BFS_BUCKET="$value"; ARG_BFS_BUCKET_SET=1 ;;
      --bfs-write-password) ARG_BFS_WRITE_PASSWORD="$value" ;;
    esac
    shift 2
  done
  return 0
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

# --- Target mode & layout ----------------------------------------------------
# Prints the resolved install target mode (saas|bucketfs) on stdout, or errors and returns 1.
# Reads the ARG_* globals directly (consistent with the rest of the file).
#
# The mode is AUTO-DETECTED from the SaaS ids, because those ids are the only thing a SaaS install
# needs that a BucketFS install cannot use: both given -> saas, neither given -> bucketfs (the
# default target: Exasol AsApp, Docker, on-premise). Exactly one given is always a mistake and errors.
# --target is an optional assertion: it never selects a mode, it only fails the run when the
# caller's stated intent disagrees with what the flags actually describe.
#
# Also rejects flags that only make sense for the OTHER target: --staging when no SaaS ids were
# given, or any --bfs-* flag when both SaaS ids were given. A silently-ignored flag here would
# read as "I told it to use staging / a custom bucket" while the run quietly did something else.
resolve_target_mode() {
  local detected=""
  if [[ -n "$ARG_ACCOUNT_ID" && -n "$ARG_DATABASE_ID" ]]; then
    detected="saas"
  elif [[ -n "$ARG_ACCOUNT_ID" || -n "$ARG_DATABASE_ID" ]]; then
    err "the Exasol SaaS target needs BOTH --account-id and --database-id; only one of the two was given. Find both in the Exasol SaaS web console."
    return 1
  else
    detected="bucketfs"
  fi

  if [[ "$detected" == "bucketfs" && "$ARG_STAGING" -eq 1 ]]; then
    err "--staging is a SaaS-only flag, but no --account-id/--database-id were given (BucketFS target detected). Drop --staging, or pass both ids for an Exasol SaaS install."
    return 1
  fi
  if [[ "$detected" == "saas" ]]; then
    local bfs_flags_given=""
    [[ -n "$ARG_BFS_HOST" ]] && bfs_flags_given="$bfs_flags_given --bfs-host"
    [[ -n "$ARG_BFS_PORT" ]] && bfs_flags_given="$bfs_flags_given --bfs-port"
    [[ "$ARG_BFS_BUCKET_SET" -eq 1 ]] && bfs_flags_given="$bfs_flags_given --bfs-bucket"
    [[ -n "$ARG_BFS_WRITE_PASSWORD" ]] && bfs_flags_given="$bfs_flags_given --bfs-write-password"
    if [[ -n "$bfs_flags_given" ]]; then
      err "BucketFS-only flag(s)$bfs_flags_given were given, but --account-id and --database-id were both given too (Exasol SaaS target detected). SaaS uploads go through its own REST API, not BucketFS -- drop the BucketFS flag(s), or drop --account-id/--database-id for a BucketFS install."
      return 1
    fi
  fi

  if [[ -n "$ARG_TARGET" ]]; then
    case "$ARG_TARGET" in
      saas|bucketfs) ;;
      *) err "--target must be 'saas' or 'bucketfs'; got '$ARG_TARGET'."; return 1 ;;
    esac
    if [[ "$ARG_TARGET" != "$detected" ]]; then
      if [[ "$detected" == "saas" ]]; then
        err "--target $ARG_TARGET conflicts with the detected target mode 'saas': --account-id and --database-id were both given, which selects the Exasol SaaS target. Drop those two ids for a BucketFS install."
      else
        err "--target $ARG_TARGET conflicts with the detected target mode 'bucketfs': neither --account-id nor --database-id was given, which selects the BucketFS target. Pass both ids for an Exasol SaaS install."
      fi
      return 1
    fi
  fi

  printf '%s\n' "$detected"
  return 0
}

# If bucketfs mode + profile connectivity + no explicit --bfs-bucket, resolves ARG_BFS_BUCKET
# from the profile's own bfs_bucket field. Must run before resolve_target_layout, and before
# exapump_bfs_flags is ever consulted. Without this, ARG_BFS_BUCKET stays at its "default" default
# while exapump itself resolves the profile's bfs_bucket for the actual upload -- an install that
# passes every upload/verify step (they all target the bucket exapump picks) yet builds
# %udf_object/RUST-alias paths (via resolve_target_layout) pointing at "default", so Exasol looks
# for the .so in a bucket it was never uploaded to. A no-op in saas mode, dsn/host connectivity
# mode (no profile to read), or when --bfs-bucket was already given explicitly.
resolve_bfs_bucket_from_profile() {
  if [[ "$TARGET_MODE" != "bucketfs" || "$CONNECTIVITY_MODE" != "profile" || "$ARG_BFS_BUCKET_SET" -eq 1 ]]; then
    return 0
  fi
  local profile_bucket
  if profile_bucket="$(read_profile_key "$ARG_PROFILE" bfs_bucket "$(exapump_config_path)")" && [[ -n "$profile_bucket" ]]; then
    ARG_BFS_BUCKET="$profile_bucket"
  fi
  return 0
}

# Seeds the mode-parameterized TARGET_* globals used by the install steps, so those steps never
# read a target-specific constant directly. Call only after resolve_target_mode has resolved a
# mode, and after resolve_bfs_bucket_from_profile so ARG_BFS_BUCKET already reflects the bucket
# exapump will actually use. TARGET_SLC_BFS_PATH / TARGET_ENGINE_BFS_PATH are BucketFS-only (SaaS
# addresses its uploads by presigned-URL file key instead, so they stay empty there).
resolve_target_layout() {
  case "$TARGET_MODE" in
    bucketfs)
      TARGET_SO_UDF_OBJECT="buckets/bfsdefault/$ARG_BFS_BUCKET/udf/liblakehouse_engine.so"
      TARGET_RUST_LANG_SEGMENT="RUST=localzmq+protobuf:///bfsdefault/$ARG_BFS_BUCKET/slc/lakehouse-rustslc?lang=rust#buckets/bfsdefault/$ARG_BFS_BUCKET/slc/lakehouse-rustslc/exaudf/exaudfclient"
      TARGET_SLC_BFS_PATH="$BFS_SLC_PATH"
      TARGET_ENGINE_BFS_PATH="$BFS_ENGINE_SO_PATH"
      ;;
    saas|*)
      TARGET_SO_UDF_OBJECT="$ENGINE_SO_PATH"
      TARGET_RUST_LANG_SEGMENT="$RUST_LANG_SEGMENT"
      TARGET_SLC_BFS_PATH=""
      TARGET_ENGINE_BFS_PATH=""
      ;;
  esac
  return 0
}

# BucketFS-target required fields. Runs BEFORE any network call, so a missing write password can
# never cost a download. `exapump bucketfs` has NO --dsn/--host/--user/--password flags of its own
# (unlike `exapump sql`), so outside profile connectivity mode there is nothing to fall back on and
# --bfs-host plus --bfs-write-password must be supplied explicitly.
validate_bucketfs_required() {
  local missing=0
  case "$CONNECTIVITY_MODE" in
    profile)
      if [[ -z "$ARG_BFS_WRITE_PASSWORD" ]]; then
        local config_path resolved
        config_path="$(exapump_config_path)"
        if ! resolved="$(read_profile_key "$ARG_PROFILE" bfs_write_password "$config_path")" || [[ -z "$resolved" ]]; then
          err "missing the BucketFS write password: pass --bfs-write-password, or add a 'bfs_write_password' key to the [$ARG_PROFILE] section of $config_path."
          missing=1
        fi
      fi
      ;;
    dsn|host)
      if [[ -z "$ARG_BFS_WRITE_PASSWORD" ]]; then
        err "missing --bfs-write-password: 'exapump bucketfs' takes no DSN or user/password flags, so in $CONNECTIVITY_MODE connectivity mode the BucketFS write password must be given explicitly (or switch to --profile and set 'bfs_write_password' there)."
        missing=1
      fi
      if [[ -z "$ARG_BFS_HOST" ]]; then
        err "missing --bfs-host: 'exapump bucketfs' takes no DSN or host flags, so in $CONNECTIVITY_MODE connectivity mode the BucketFS host must be given explicitly (or switch to --profile and set 'bfs_host' there)."
        missing=1
      fi
      ;;
    *)
      err "internal error: connectivity mode not resolved"
      return 1
      ;;
  esac
  [[ "$missing" -eq 0 ]]
}

check_prereqs() {
  local ok=1
  have_cmd exapump || { err "required tool 'exapump' not found on PATH. Install it: https://github.com/exasol-labs/exapump"; ok=0; }
  have_cmd curl    || { err "required tool 'curl' not found on PATH. Install it via your OS package manager: https://curl.se/"; ok=0; }
  if [[ "$TARGET_MODE" == "bucketfs" ]]; then
    have_cmd tar   || { err "required tool 'tar' not found on PATH. The BucketFS install target extracts liblakehouse_engine.so out of the engine archive locally before uploading it. Install it via your OS package manager."; ok=0; }
  fi
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

# --- GitHub auth --------------------------------------------------------------
# Populates GITHUB_AUTH_ARGS with the -H Authorization header args for a GitHub REST call, or
# leaves it empty. Both lakehouse-engine-rs and language-container-rs are public repos, so a
# token is optional -- it only raises the unauthenticated 60-requests/hour rate limit. An empty
# token must never be sent: GitHub rejects a malformed `Authorization: Bearer` (no value) with
# 401, which is worse than sending no Authorization header at all.
GITHUB_AUTH_ARGS=()
set_github_auth_args() {
  GITHUB_AUTH_ARGS=()
  if [[ -n "$ARG_GITHUB_TOKEN" ]]; then
    GITHUB_AUTH_ARGS=(-H "Authorization: Bearer $ARG_GITHUB_TOKEN")
  fi
  return 0
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

# Fetches the engine's root Cargo.toml AT the resolved engine release tag and extracts the
# exasol-udf-sdk version it pins there -- the SLC version that release actually requires. This
# repo keeps the SLC and its exasol-udf-sdk dependency in exact lockstep (same version number both
# places; see CLAUDE.md and the Makefile's install-slc SLC_VERSION default), so the pin IS the
# right default SLC version -- language-container-rs's own "latest" release is NOT: the two repos
# release independently, so SLC can ship ahead of any engine release that has picked it up yet.
# That exact drift (language-container-rs v0.21.1 with no matching engine release) broke this
# default before this fix; see #305.
resolve_engine_pinned_slc_version() {
  local tag="$1"
  local toml line sdk_version=""
  if ! toml="$(curl -fsS "${GITHUB_AUTH_ARGS[@]+"${GITHUB_AUTH_ARGS[@]}"}" \
      -H "Accept: application/vnd.github.raw" \
      "https://api.github.com/repos/$ENGINE_REPO/contents/Cargo.toml?ref=$tag" </dev/null 2>&1)"; then
    err "could not fetch Cargo.toml for $ENGINE_REPO release '$tag' via the GitHub REST API. Pass --slc-version to skip this lookup."
    return 1
  fi
  while IFS= read -r line; do
    case "$line" in
      exasol-udf-sdk*)
        local re='version[[:space:]]*=[[:space:]]*"([^"]+)"'
        [[ "$line" =~ $re ]] && sdk_version="${BASH_REMATCH[1]}"
        break
        ;;
    esac
  done <<< "$toml"
  if [[ -z "$sdk_version" ]]; then
    err "could not find an exasol-udf-sdk version pin in $ENGINE_REPO release '$tag''s Cargo.toml. Pass --slc-version explicitly."
    return 1
  fi
  printf '%s\n' "$sdk_version"
  return 0
}

resolve_versions() {
  set_github_auth_args
  if [[ -n "$ARG_LAKEHOUSE_VERSION" ]]; then
    RESOLVED_ENGINE_TAG="$(version_to_tag "$ARG_LAKEHOUSE_VERSION")"
  else
    local ej
    if ! ej="$(curl -fsS "${GITHUB_AUTH_ARGS[@]+"${GITHUB_AUTH_ARGS[@]}"}" "https://api.github.com/repos/$ENGINE_REPO/releases/latest" </dev/null 2>&1)"; then
      err "could not resolve the latest $ENGINE_REPO release via the GitHub REST API. If this is a rate limit, pass --github-token/GITHUB_TOKEN."
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
    # DEFAULT: the SLC version this resolved engine release was built and fingerprinted against --
    # NOT language-container-rs's own latest release. See resolve_engine_pinned_slc_version above.
    local sdk_version
    if ! sdk_version="$(resolve_engine_pinned_slc_version "$RESOLVED_ENGINE_TAG")"; then
      return 1
    fi
    RESOLVED_SLC_TAG="$(version_to_tag "$sdk_version")"
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

# --- BucketFS helpers (exapump) ----------------------------------------------
# Prints, space-separated, the --bfs-* overrides passed to every `exapump bucketfs` call. Host,
# port and write-password are passed only when the caller actually supplied them, leaving those
# to exapump's own resolution (profile field, then smart default). The bucket is the one exception:
# ARG_BFS_BUCKET is ALWAYS passed, using its fully-resolved value (explicit --bfs-bucket, or the
# profile's bfs_bucket via resolve_bfs_bucket_from_profile, or the script's own "default" fallback)
# -- never left to exapump's own bucket resolution. Without this, dsn/host connectivity mode (which
# has no profile of its own) could still have exapump silently resolve the bucket from whatever
# default profile happens to exist in ~/.exapump/config.toml, diverging from the "default" bucket
# this script assumes when building TARGET_SO_UDF_OBJECT/TARGET_RUST_LANG_SEGMENT -- an upload that
# succeeds against one bucket while the DDL points at another.
#
# The result is meant to be word-split by the caller, so no value may contain whitespace. That is
# true of a host, a port and a bucket name by construction; a BucketFS write password containing a
# space is the one unsupported case -- pass it through the profile's bfs_write_password instead.
exapump_bfs_flags() {
  local out=""
  if [[ -n "$ARG_BFS_HOST" ]]; then out="$out --bfs-host $ARG_BFS_HOST"; fi
  if [[ -n "$ARG_BFS_PORT" ]]; then out="$out --bfs-port $ARG_BFS_PORT"; fi
  out="$out --bfs-bucket $ARG_BFS_BUCKET"
  if [[ -n "$ARG_BFS_WRITE_PASSWORD" ]]; then out="$out --bfs-write-password $ARG_BFS_WRITE_PASSWORD"; fi
  printf '%s\n' "${out# }"
  return 0
}

# Runs `exapump bucketfs <args...>` with this run's connectivity flag and BucketFS overrides
# appended. Globbing is disabled around the deliberate word-split of exapump_bfs_flags so a '*'
# inside a password can never expand into file names. stdin is /dev/null so the subprocess cannot
# consume the piped script body.
exapump_bucketfs() {
  local conn_arg="" restore_glob=0 rc
  if [[ "$CONNECTIVITY_MODE" == "profile" ]]; then conn_arg="--profile $ARG_PROFILE"; fi
  case "$-" in
    *f*) : ;;
    *)   restore_glob=1; set -f ;;
  esac
  # shellcheck disable=SC2046,SC2086  # intentional word-splitting; see exapump_bfs_flags
  exapump bucketfs "$@" $conn_arg $(exapump_bfs_flags) </dev/null
  rc=$?
  [[ "$restore_glob" -eq 1 ]] && set +f
  return "$rc"
}

# Preflight, analogous to saas_db_reachable: an empty-path listing of the target bucket. exapump
# resolves the bucket itself (--bfs-bucket / profile), so no path argument is passed -- a bucket
# name IS NOT a valid path component for `exapump bucketfs ls`.
bucketfs_reachable() {
  local out
  if ! out="$(exapump_bucketfs ls 2>&1)"; then
    err "BucketFS bucket '$ARG_BFS_BUCKET' is not reachable: 'exapump bucketfs ls' failed. Verify --bfs-host, --bfs-port and the BucketFS write password (or the profile's bfs_* keys). exapump said: $out"
    return 1
  fi
  return 0
}

# Uploads one local file to a bucket-relative BucketFS path. Always via `exapump bucketfs cp`,
# never a raw HTTP PUT.
bucketfs_upload_file() {
  local local_path="$1" bucket_path="$2" out
  if ! out="$(exapump_bucketfs cp "$local_path" "$bucket_path" 2>&1)"; then
    err "BucketFS upload of '$local_path' to bucket path '$bucket_path' failed. exapump said: $out"
    return 1
  fi
  log "Uploaded $local_path to BucketFS path $bucket_path."
  return 0
}

# Analogous to saas_verify_listed: lists the parent directory and requires the basename to appear
# as a WHOLE listing entry. A line-exact comparison (not a substring test) is what keeps
# 'liblakehouse_engine.so' from matching a stored 'liblakehouse_engine.so.bak'.
bucketfs_verify_listed() {
  local bucket_path="$1" parent base out line
  base="${bucket_path##*/}"
  parent="${bucket_path%/*}"
  [[ "$parent" == "$bucket_path" ]] && parent=""
  if ! out="$(exapump_bucketfs ls "$parent" 2>/dev/null)"; then
    return 1
  fi
  while IFS= read -r line; do
    line="${line%"${line##*[![:space:]]}"}"
    [[ "$line" == "$base" ]] && return 0
  done <<EOF_BFS_LS
$out
EOF_BFS_LS
  return 1
}

# Bounded retry around bucketfs_verify_listed. BucketFS unpacks an uploaded .tar.gz
# asynchronously, so a path can be accepted by the PUT and still be absent from the very next
# listing; this waits for it rather than racing it.
bucketfs_wait_for_path() {
  local bucket_path="$1" tries="${2:-5}" sleep_seconds="${3:-2}" i=1
  while [[ "$i" -le "$tries" ]]; do
    if bucketfs_verify_listed "$bucket_path"; then
      log "Verified BucketFS path $bucket_path."
      return 0
    fi
    if [[ "$i" -lt "$tries" ]]; then
      sleep "$sleep_seconds"
    fi
    i=$((i + 1))
  done
  err "BucketFS path '$bucket_path' did not appear in the bucket listing after $tries tries. The upload reported success, so check that bucket '$ARG_BFS_BUCKET' is the bucket the database actually reads."
  return 1
}

# Unpacks the engine release archive locally and prints the path of the extracted .so. The
# BucketFS target uploads that bare .so (see install_engine for why), so the member must exist.
extract_engine_so() {
  local tarball_path="$1" destdir="$2" so_path out
  if ! out="$(mkdir -p "$destdir" 2>&1)"; then
    err "could not create the extraction directory '$destdir': $out"
    return 1
  fi
  if ! out="$(tar -xzf "$tarball_path" -C "$destdir" 2>&1)"; then
    err "could not extract the engine archive '$tarball_path' into '$destdir': $out"
    return 1
  fi
  so_path="$destdir/udf/liblakehouse_engine.so"
  if [[ ! -s "$so_path" ]]; then
    err "the engine archive '$tarball_path' does not contain a non-empty member 'udf/liblakehouse_engine.so' (looked for '$so_path' after extraction)."
    return 1
  fi
  printf '%s\n' "$so_path"
  return 0
}

# --- Upload dispatch ---------------------------------------------------------
# The ONE seam between the two target modes: SaaS addresses an upload by its files-API key,
# BucketFS by its bucket-relative path. Each mode ignores the other's argument.
upload_artifact() {
  local local_path="$1" saas_key="$2" bfs_path="$3"
  case "$TARGET_MODE" in
    saas)     saas_upload_file "$local_path" "$saas_key" ;;
    bucketfs) bucketfs_upload_file "$local_path" "$bfs_path" ;;
    *)        err "internal error: install target mode not resolved"; return 1 ;;
  esac
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
  local current="$1" segment="$2"
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
          result="${result:+$result }$segment"
          placed=1
        fi
      else
        result="${result:+$result }$tok"
      fi
    done
  fi
  if [[ "$placed" -eq 0 ]]; then
    result="${result:+$result }$segment"
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
  set_github_auth_args

  if ! release_json="$(curl -fsS "${GITHUB_AUTH_ARGS[@]+"${GITHUB_AUTH_ARGS[@]}"}" \
      "https://api.github.com/repos/$repo/releases/tags/$tag" </dev/null 2>&1)"; then
    err "could not fetch release '$tag' from $repo via the GitHub REST API."
    return 1
  fi

  if ! asset_id="$(extract_asset_id_by_name "$release_json" "$asset_name")"; then
    err "asset '$asset_name' not found in $repo release '$tag'."
    return 1
  fi

  if ! dl_err="$(curl -fsSL "${GITHUB_AUTH_ARGS[@]+"${GITHUB_AUTH_ARGS[@]}"}" \
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
  # The SLC goes up as a TARBALL in both modes: BucketFS itself must auto-extract it, because the
  # RUST alias points at the extracted rustslc/ directory, not at the archive.
  upload_artifact "$WORKDIR/rustslc.tar.gz" "rustslc.tar.gz" "$TARGET_SLC_BFS_PATH" || return 1
  if [[ "$TARGET_MODE" == "bucketfs" ]]; then
    # SaaS verifies synchronously inside saas_upload_file; BucketFS needs the bounded wait.
    bucketfs_wait_for_path "$TARGET_SLC_BFS_PATH" || return 1
  fi
  local current new
  if ! current="$(read_script_languages)"; then
    return 1
  fi
  new="$(compute_script_languages "$current" "$TARGET_RUST_LANG_SEGMENT")"
  log "Setting SCRIPT_LANGUAGES (RUST segment append/replace)."
  if ! run_sql "ALTER SYSTEM SET SCRIPT_LANGUAGES='$new'" >/dev/null 2>&1; then
    err "ALTER SYSTEM SET SCRIPT_LANGUAGES failed. The connecting account likely lacks the SYSTEM (admin) privilege required to register a script language."
    return 1
  fi
  return 0
}

create_engine_scripts() {
  local schema="$ARG_SCHEMA" so="$TARGET_SO_UDF_OBJECT" stmt
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

# Deliberate artifact-shape asymmetry between the two targets:
#  * SaaS uploads the engine TARBALL and lets the SaaS bucket auto-extract it into the layout
#    ENGINE_SO_PATH already encodes.
#  * BucketFS extracts locally and uploads the BARE .so to udf/liblakehouse_engine.so -- the exact
#    path `make bucketfs-upload-so` has always used and every E2E test's %udf_object points at.
#    Only the SLC relies on BucketFS archive auto-extraction, in both modes.
install_engine() {
  log "Installing lakehouse-engine $RESOLVED_ENGINE_VERSION ..."
  download_engine || return 1
  if [[ "$TARGET_MODE" == "bucketfs" ]]; then
    local so_path
    if ! so_path="$(extract_engine_so "$WORKDIR/$ENGINE_ASSET" "$WORKDIR/extracted")"; then
      return 1
    fi
    upload_artifact "$so_path" "" "$TARGET_ENGINE_BFS_PATH" || return 1
    bucketfs_wait_for_path "$TARGET_ENGINE_BFS_PATH" || return 1
  else
    upload_artifact "$WORKDIR/$ENGINE_ASSET" "$ENGINE_ASSET" "" || return 1
  fi
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
  emit "  NAMESPACE          = '<namespace>'"
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

  # The target mode is resolved FIRST: it doubles as the --account-id/--database-id validation, and
  # every later step (which fields are required, which tools are needed, which preflight runs)
  # branches on it.
  if ! TARGET_MODE="$(resolve_target_mode)"; then
    exit 1
  fi
  if ! CONNECTIVITY_MODE="$(validate_connectivity)"; then
    exit 1
  fi
  resolve_bfs_bucket_from_profile
  resolve_target_layout || exit 1
  if [[ "$CONNECTIVITY_MODE" == "host" ]]; then
    local enc_user enc_password
    enc_user="$(url_encode "$ARG_USER")"
    enc_password="$(url_encode "$ARG_PASSWORD")"
    HOST_DSN="exasol://$enc_user:$enc_password@$ARG_HOST?validateservercertificate=0"
  fi
  check_prereqs || exit 1

  # Per-target credential derivation + reachability preflight. Both arms must complete before the
  # first download, so a misconfigured run costs no bytes.
  case "$TARGET_MODE" in
    saas)
      resolve_saas_pat || exit 1
      saas_db_reachable || exit 1
      ;;
    bucketfs)
      validate_bucketfs_required || exit 1
      bucketfs_reachable || exit 1
      ;;
  esac

  if ! WORKDIR="$(mktemp -d "${TMPDIR:-/tmp}/lhvs-install.XXXXXX" 2>/dev/null)"; then
    err "failed to create a temporary working directory."
    exit 1
  fi
  trap 'rm -rf "$WORKDIR"' EXIT

  resolve_versions || exit 1
  if [[ "$ARG_SKIP_SLC" -eq 1 ]]; then
    log "Skipping SLC registration (--skip-slc)."
  else
    register_slc || exit 1
  fi
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
