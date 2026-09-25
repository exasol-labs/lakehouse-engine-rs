#!/usr/bin/env bash
#   curl -fsSL -H "Accept: application/vnd.github.raw" \
#     https://api.github.com/repos/exasol-labs/lakehouse-engine-rs/contents/deploy/scripts/install.sh \
#   | bash -s -- --account-id $ACC --database-id $DB --profile staging
#
# Bash 3.2+ (stock macOS). Sourceable for unit tests: `main` runs only when executed or piped.
# Every subprocess reads stdin from /dev/null so it cannot consume the rest of the piped script.

SAAS_PROD_BASE="https://cloud.exasol.com"
SAAS_STAGING_BASE="https://cloud-staging.exasol.com"
ENGINE_REPO="exasol-labs/lakehouse-engine-rs"
SLC_REPO="exasol-labs/language-container-rs"
EXAPUMP_INSTALL_URL="https://raw.githubusercontent.com/exasol-labs/exapump/main/install.sh"
ENGINE_ASSET="lakehouse-engine.tar.gz"
ENGINE_SO_PATH="/buckets/uploads/default/lakehouse-engine/udf/liblakehouse_engine.so"
DEFAULT_SCHEMA="LHVS"
RUST_LANG_SEGMENT="RUST=localzmq+protobuf:///uploads/default/rustslc?lang=rust#buckets/uploads/default/rustslc/exaudf/exaudfclient"

# Bucket-relative, no bucket segment: exapump takes the bucket from --bfs-bucket, and a bucket
# in the path is created as a subdirectory. The engine-read %udf_object / RUST alias strings do
# include the bucket.
DEFAULT_BFS_BUCKET="default"
BFS_SERVICE="bfsdefault"
BFS_SLC_PATH="slc/lakehouse-rustslc.tar.gz"
BFS_ENGINE_SO_PATH="udf/liblakehouse_engine.so"

DEPLOYMENT_ROOT="$HOME/.exasol/personal/deployments"
DEPLOYMENT_DESCRIPTOR="deployment.json"
SECRETS_DESCRIPTOR="secrets.json"
LOCAL_BACKEND="local"
PERSONAL_DB_HOST_DEFAULT="127.0.0.1"
PERSONAL_DB_PORT_DEFAULT="8563"
PERSONAL_DB_USER_DEFAULT="sys"
# Env-overridable so tests can run the retry loops without real waits.
BUCKETFS_REACHABLE_TRIES="${BUCKETFS_REACHABLE_TRIES:-30}"
BUCKETFS_REACHABLE_POLL_SECONDS="${BUCKETFS_REACHABLE_POLL_SECONDS:-2}"
# Exasol Personal 2.3+ local deployments have no BucketFS HTTP endpoint: the SLC goes through
# `exasol slc custom`, the .so into the host-side BucketFS directory (a new <service>/<bucket>/
# directory creates that bucket).
LAUNCHER_SLC_ALIAS="RUST"
LAUNCHER_SLC_LANGUAGE="rust"
PERSONAL_EXA_RELATIVE_PATH="local/runtime/exa"
PERSONAL_BUCKET_TRIES="${PERSONAL_BUCKET_TRIES:-30}"
PERSONAL_BUCKET_POLL_SECONDS="${PERSONAL_BUCKET_POLL_SECONDS:-1}"

ARG_ACCOUNT_ID=""
ARG_DATABASE_ID=""
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
ARG_ARCH="x86_64"
ARG_ARCH_SET=0
ARG_DEPLOYMENT=""
ARG_HELP=0

DEPLOYMENT_TRANSPORT=""
DEPLOYMENT_DIR=""
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

# Progress goes to stderr; deliverables (resolved versions, template) to stdout.
emit() { printf '%s\n' "$*"; }
log()  { printf '%s\n' "$*" >&2; }
err()  { printf 'ERROR: %s\n' "$*" >&2; }

have_cmd() { command -v "$1" >/dev/null 2>&1; }

# Prompts only when stdin and stdout are both ttys: under curl|bash stdin is the script, so a
# prompt would hang or no-op. exapump's installer does not update this process's PATH, so its
# install dir is prepended explicitly.
ensure_exapump() {
  have_cmd exapump && return 0
  if [[ -t 0 && -t 1 ]]; then
    local reply
    printf 'exapump not found on PATH. Install it now via %s? [Y/n] ' "$EXAPUMP_INSTALL_URL" >&2
    read -r reply
    case "$reply" in
      ''|y|Y|yes|YES|Yes) ;;
      *) err "exapump not installed. Install it yourself: https://github.com/exasol-labs/exapump"; return 1 ;;
    esac
  else
    log "exapump not found on PATH; installing it automatically from $EXAPUMP_INSTALL_URL"
  fi
  if ! curl -fsSL --proto =https "$EXAPUMP_INSTALL_URL" </dev/null | sh; then
    err "exapump auto-install failed. Install it manually: https://github.com/exasol-labs/exapump"
    return 1
  fi
  local install_dir="${EXAPUMP_INSTALL_DIR:-$HOME/.local/bin}"
  have_cmd exapump || PATH="$install_dir:$PATH"
  have_cmd exapump || {
    err "exapump installed to $install_dir but is still not runnable. Add $install_dir to PATH and re-run."
    return 1
  }
  log "exapump installed: $(exapump --version </dev/null 2>&1 | head -1)"
  return 0
}

# Keeps only the RFC 3986 unreserved set, so credentials cannot corrupt the DSN userinfo.
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

url_decode() {
  local s="$1" i c out=""
  local len=${#s}
  for ((i = 0; i < len; i++)); do
    c="${s:i:1}"
    if [[ "$c" == "%" && $((i + 2)) -lt len && "${s:i+1:2}" =~ ^[0-9A-Fa-f]{2}$ ]]; then
      # shellcheck disable=SC2059  # the inner printf emits only octal digits, never a format specifier.
      out+="$(printf "\\$(printf '%03o' "0x${s:i+1:2}")")"
      i=$((i + 2))
    else
      out+="$c"
    fi
  done
  printf '%s\n' "$out"
}

# Returns the still-encoded password. Splits on the LAST '@' so a raw '@' in the password
# survives ('user:pass@word@host' -> 'pass@word'); returns 1 if there is no ':password@'.
extract_dsn_password() {
  local dsn="$1"
  local re='^[a-zA-Z][a-zA-Z0-9+.-]*://[^:]*:(.*)@.*$'
  if [[ "$dsn" =~ $re ]]; then
    printf '%s\n' "${BASH_REMATCH[1]}"
    return 0
  fi
  return 1
}

# Go's encoding/json HTML-escapes '&' as & in presigned URLs; left escaped, S3 rejects the
# request with AuthorizationQueryParametersError. \uXXXX is decoded for ASCII only.
json_unescape() {
  local rest="$1" out="" chunk hex dec ch
  # shellcheck disable=SC1003  # a literal single backslash, not an escape attempt.
  while [[ "$rest" == *'\'* ]]; do
    chunk="${rest%%\\*}"
    out+="$chunk"
    rest="${rest#*\\}"
    # shellcheck disable=SC1003  # the \\*) branch's '\' is a literal backslash, not an escape attempt.
    case "$rest" in
      u[0-9A-Fa-f][0-9A-Fa-f][0-9A-Fa-f][0-9A-Fa-f]*)
        hex="${rest:1:4}"
        rest="${rest:5}"
        dec=$((16#$hex))
        if [[ "$dec" -lt 128 ]]; then
          # shellcheck disable=SC2059  # inner printf emits only octal digits.
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

extract_json_string_field() {
  local json="$1" field="$2"
  local re="\"$field\"[[:space:]]*:[[:space:]]*\"([^\"]*)\""
  if [[ "$json" =~ $re ]]; then
    json_unescape "${BASH_REMATCH[1]}"
    return 0
  fi
  return 1
}

# exapump honors EXAPUMP_CONFIG as a full-file-path override.
exapump_config_path() {
  printf '%s\n' "${EXAPUMP_CONFIG:-$HOME/.exapump/config.toml}"
}

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

# On Exasol SaaS the PAT is the SQL password, so the REST bearer is derived from the connectivity credential.
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

read_descriptor_field() {
  local descriptor="$1" jq_path="$2" value jq_err
  if [[ ! -r "$descriptor" ]]; then
    err "no readable Exasol Personal deployment file at '$descriptor'."
    return 1
  fi
  if ! jq_err="$(jq -r "$jq_path // empty" "$descriptor" </dev/null 2>&1)"; then
    err "could not parse '$descriptor' as JSON while reading '$jq_path'. Re-run once the deployment has finished starting. jq said: $jq_err"
    return 1
  fi
  value="$jq_err"
  printf '%s\n' "$value"
  return 0
}

deployment_backend() {
  local dir="$1" backend
  if ! backend="$(read_descriptor_field "$dir/$DEPLOYMENT_DESCRIPTOR" '.backend')"; then
    return 1
  fi
  if [[ -z "$backend" ]]; then
    err "'$dir/$DEPLOYMENT_DESCRIPTOR' carries no '.backend' field, so the install transport (local 'exasol slc custom' vs cloud BucketFS HTTP upload) cannot be determined."
    return 1
  fi
  printf '%s\n' "$backend"
  return 0
}

deployment_field() {
  local dir="$1" jq_path="$2" default="$3" value
  if ! value="$(read_descriptor_field "$dir/$DEPLOYMENT_DESCRIPTOR" "$jq_path")"; then
    return 1
  fi
  if [[ -z "$value" ]]; then
    value="$default"
  fi
  printf '%s\n' "$value"
  return 0
}

deployment_db_password() {
  read_descriptor_field "$1/$SECRETS_DESCRIPTOR" '.dbPassword'
}

resolve_deployment_connection() {
  local dir="$1" default_host="$2"
  local descriptor_host descriptor_port descriptor_user host_part port_part

  if ! descriptor_host="$(deployment_field "$dir" '.connection.host' "$default_host")"; then
    return 1
  fi
  if ! descriptor_port="$(deployment_field "$dir" '.connection.dbPort' "$PERSONAL_DB_PORT_DEFAULT")"; then
    return 1
  fi
  if ! descriptor_user="$(deployment_field "$dir" '.connection.username' "$PERSONAL_DB_USER_DEFAULT")"; then
    return 1
  fi

  host_part="$descriptor_host"
  port_part="$descriptor_port"
  if [[ -n "$ARG_HOST" ]]; then
    if [[ "$ARG_HOST" == *:* ]]; then
      host_part="${ARG_HOST%:*}"
      port_part="${ARG_HOST##*:}"
    else
      host_part="$ARG_HOST"
    fi
  fi
  if [[ -z "$host_part" ]]; then
    err "no database host for the deployment: '$dir/$DEPLOYMENT_DESCRIPTOR' carries no '.connection.host'. Pass --host <host:port>."
    return 1
  fi
  if [[ -z "$port_part" ]]; then
    err "no database port for the deployment: '$dir/$DEPLOYMENT_DESCRIPTOR' carries no '.connection.dbPort' and --host named none. Pass --host <host:port>."
    return 1
  fi
  ARG_HOST="$host_part:$port_part"

  if [[ -z "$ARG_USER" ]]; then
    ARG_USER="$descriptor_user"
  fi
  if [[ -z "$ARG_PASSWORD" ]]; then
    if ! ARG_PASSWORD="$(deployment_db_password "$dir")"; then
      return 1
    fi
  fi
  if [[ -z "$ARG_PASSWORD" ]]; then
    err "no database password for the deployment: '$dir/$SECRETS_DESCRIPTOR' carries no '.dbPassword'. Pass --password."
    return 1
  fi
  return 0
}

require_cloud_bfs_password() {
  if [[ -z "$ARG_BFS_WRITE_PASSWORD" ]]; then
    err "Exasol Personal provisions no BucketFS password: --bfs-write-password is required for a cloud deployment (any backend other than '$LOCAL_BACKEND')."
    return 1
  fi
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
  --arch <x86_64|aarch64>   target CPU architecture (default: x86_64, or the host's for a local
                            --deployment); selects unsuffixed vs -aarch64-suffixed release assets
  --deployment <name>       target an Exasol Personal deployment by name, resolving connection and
                            backend from $HOME/.exasol/personal/deployments/<name>/deployment.json;
                            local backend (Exasol Personal 2.3+) writes into the deployment's
                            BucketFS directory and installs the SLC with `exasol slc custom`;
                            cloud backend falls through to the BucketFS HTTP path above
                            (--bfs-write-password required for cloud)
  --help                    show this help

Examples:
  install.sh --account-id ACC --database-id DB --profile saas-prod
  install.sh --profile my-exasol --bfs-write-password "$BFSPASS"
  install.sh --profile my-exasol --arch aarch64
  install.sh --deployment my-local-db
  install.sh --deployment my-cloud-db --bfs-write-password "$BFSPASS"

The script stops at a query-ready product install and prints a CONNECTION / VIRTUAL SCHEMA
template as the next step; it does not create catalog objects.

If exapump is missing, it is auto-installed via its own public installer (prompting for
confirmation on an interactive terminal; proceeding automatically otherwise, e.g. the curl|bash
one-liner). Set EXAPUMP_INSTALL_DIR to change where it lands (default: $HOME/.local/bin).
USAGE
}

parse_args() {
  ARG_ACCOUNT_ID=""
  ARG_DATABASE_ID=""
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
  ARG_ARCH="x86_64"
  ARG_ARCH_SET=0
  ARG_DEPLOYMENT=""
  ARG_HELP=0

  while [[ $# -gt 0 ]]; do
    local flag="$1"
    case "$flag" in
      --staging)  ARG_STAGING=1; shift; continue ;;
      --skip-slc) ARG_SKIP_SLC=1; shift; continue ;;
      --help|-h)  ARG_HELP=1; shift; continue ;;
      --account-id|--database-id|--profile|--dsn|--host|--user|--password|--schema|--lakehouse-version|--slc-version) ;;
      --target|--bfs-host|--bfs-port|--bfs-bucket|--bfs-write-password|--arch|--deployment) ;;
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
      --deployment)        ARG_DEPLOYMENT="$value" ;;
      --arch)
        case "$value" in
          x86_64|aarch64) ;;
          *) err "--arch must be 'x86_64' or 'aarch64'; got '$value'."; return 1 ;;
        esac
        ARG_ARCH="$value"
        ARG_ARCH_SET=1
        ;;
    esac
    shift 2
  done
  return 0
}

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

# --target only asserts, never selects. Flags for the other target are rejected rather than
# silently ignored.
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

resolve_deployment_transport() {
  if [[ "$TARGET_MODE" == "saas" ]]; then
    err "--deployment installs into an Exasol Personal deployment, which is never an Exasol SaaS database (it has no SaaS account/database id and no SaaS files API). Drop --account-id/--database-id, or drop --deployment."
    return 1
  fi
  if [[ -n "$ARG_PROFILE" || -n "$ARG_DSN" ]]; then
    err "--deployment resolves host, user and password from the deployment's own descriptor, so it cannot be combined with --profile or --dsn. Note EXAPUMP_DSN seeds --dsn too: unset it to install into a deployment."
    return 1
  fi
  have_cmd jq || {
    err "required tool 'jq' not found on PATH. --deployment reads the Exasol Personal deployment descriptor with it. Install it via your OS package manager: https://jqlang.github.io/jq/"
    return 1
  }

  DEPLOYMENT_DIR="$DEPLOYMENT_ROOT/$ARG_DEPLOYMENT"
  if [[ ! -d "$DEPLOYMENT_DIR" ]]; then
    err "no Exasol Personal deployment directory at '$DEPLOYMENT_DIR'. List the deployments on this machine with 'exasol deployment list'."
    return 1
  fi

  local backend
  if ! backend="$(deployment_backend "$DEPLOYMENT_DIR")"; then
    return 1
  fi

  if [[ "$backend" != "$LOCAL_BACKEND" ]]; then
    DEPLOYMENT_TRANSPORT="bucketfs"
    resolve_deployment_connection "$DEPLOYMENT_DIR" "" || return 1
    require_cloud_bfs_password || return 1
    if [[ -z "$ARG_BFS_HOST" ]]; then
      ARG_BFS_HOST="${ARG_HOST%:*}"
    fi
    log "Deployment '$ARG_DEPLOYMENT' has backend '$backend': installing over its BucketFS HTTP endpoint at $ARG_BFS_HOST."
    return 0
  fi

  DEPLOYMENT_TRANSPORT="launcher"
  local bfs_flags_given=""
  [[ -n "$ARG_BFS_HOST" ]] && bfs_flags_given="$bfs_flags_given --bfs-host"
  [[ -n "$ARG_BFS_PORT" ]] && bfs_flags_given="$bfs_flags_given --bfs-port"
  [[ -n "$ARG_BFS_WRITE_PASSWORD" ]] && bfs_flags_given="$bfs_flags_given --bfs-write-password"
  if [[ -n "$bfs_flags_given" ]]; then
    err "BucketFS HTTP flag(s)$bfs_flags_given were given, but deployment '$ARG_DEPLOYMENT' has backend '$LOCAL_BACKEND', which has no BucketFS HTTP endpoint: the engine is written straight into its BucketFS directory. Drop the flag(s); only --bfs-bucket applies."
    return 1
  fi
  if [[ ! "$ARG_BFS_BUCKET" =~ ^[A-Za-z0-9_-][A-Za-z0-9._-]*$ ]]; then
    err "--bfs-bucket '$ARG_BFS_BUCKET' is not a valid bucket name: it names a directory under the deployment's BucketFS directory, so it must match [A-Za-z0-9_-][A-Za-z0-9._-]*."
    return 1
  fi
  resolve_deployment_connection "$DEPLOYMENT_DIR" "$PERSONAL_DB_HOST_DEFAULT" || return 1
  if [[ "$ARG_ARCH_SET" -eq 0 ]]; then
    ARG_ARCH="$(detect_host_arch)" || return 1
  fi
  log "Deployment '$ARG_DEPLOYMENT' has backend '$LOCAL_BACKEND': installing $ARG_ARCH artifacts through 'exasol slc custom' and the deployment's BucketFS directory."
  return 0
}

# Must run before resolve_target_layout, so the DDL paths name the same bucket the upload uses.
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

resolve_target_layout() {
  case "$TARGET_MODE" in
    bucketfs)
      TARGET_SO_UDF_OBJECT="buckets/$BFS_SERVICE/$ARG_BFS_BUCKET/udf/liblakehouse_engine.so"
      TARGET_RUST_LANG_SEGMENT="RUST=localzmq+protobuf:///$BFS_SERVICE/$ARG_BFS_BUCKET/slc/lakehouse-rustslc?lang=rust#buckets/$BFS_SERVICE/$ARG_BFS_BUCKET/slc/lakehouse-rustslc/exaudf/exaudfclient"
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

# `exapump bucketfs` takes no DSN/host/user flags, so outside profile mode --bfs-host and
# --bfs-write-password must be explicit.
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
  ensure_exapump || ok=0
  have_cmd curl    || { err "required tool 'curl' not found on PATH. Install it via your OS package manager: https://curl.se/"; ok=0; }
  if [[ "$TARGET_MODE" == "bucketfs" ]]; then
    have_cmd tar   || { err "required tool 'tar' not found on PATH. The BucketFS install target extracts liblakehouse_engine.so out of the engine archive locally before uploading it. Install it via your OS package manager."; ok=0; }
  fi
  if [[ "$DEPLOYMENT_TRANSPORT" == "launcher" ]]; then
    have_cmd exasol || { err "required tool 'exasol' (the Exasol Personal launcher CLI) not found on PATH. A local Exasol Personal deployment is installed through 'exasol slc custom'."; ok=0; }
  fi
  [[ "$ok" -eq 1 ]]
}

resolve_saas_base() {
  if [[ "$ARG_STAGING" -eq 1 ]]; then
    printf '%s\n' "$SAAS_STAGING_BASE"
  else
    printf '%s\n' "$SAAS_PROD_BASE"
  fi
}

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

resolve_arch_suffix() {
  local arch="$1"
  case "$arch" in
    aarch64) printf '%s\n' "-aarch64" ;;
    *)       printf '%s\n' "" ;;
  esac
}

detect_host_arch() {
  local machine
  machine="$(uname -m 2>/dev/null)"
  case "$machine" in
    arm64|aarch64) printf '%s\n' "aarch64" ;;
    x86_64|amd64)  printf '%s\n' "x86_64" ;;
    *)
      err "could not map the detected host architecture '$machine' (from 'uname -m') to a supported value. Pass --arch x86_64|aarch64 explicitly."
      return 1
      ;;
  esac
}

# The SLC must match the engine's pinned exasol-udf-sdk version exactly; the SLC repo's own latest
# release can be ahead of any engine release (#305).
resolve_engine_pinned_slc_version() {
  local tag="$1"
  local toml line sdk_version=""
  if ! toml="$(curl -fsS \
      "https://raw.githubusercontent.com/$ENGINE_REPO/$tag/Cargo.toml" </dev/null 2>&1)"; then
    err "could not fetch Cargo.toml for $ENGINE_REPO at '$tag'. Pass --slc-version to skip this lookup."
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
  if [[ -n "$ARG_LAKEHOUSE_VERSION" ]]; then
    RESOLVED_ENGINE_TAG="$(version_to_tag "$ARG_LAKEHOUSE_VERSION")"
  else
    local ej
    if ! ej="$(curl -fsS "https://api.github.com/repos/$ENGINE_REPO/releases/latest" </dev/null 2>&1)"; then
      err "could not resolve the latest $ENGINE_REPO release via the GitHub REST API."
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
  # Quoted so "x.tar.gz" does not match "x.tar.gz.bak".
  [[ "$resp" == *"\"$filename\""* ]]
}

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
  # No -f: a rejected PUT's body carries the storage host's error detail.
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

# The bucket is always passed so exapump cannot resolve a different one from a default profile
# than the one the DDL paths name. The output is word-split: a write password containing a space
# must come from the profile instead.
exapump_bfs_flags() {
  local out=""
  if [[ -n "$ARG_BFS_HOST" ]]; then out="$out --bfs-host $ARG_BFS_HOST"; fi
  if [[ -n "$ARG_BFS_PORT" ]]; then out="$out --bfs-port $ARG_BFS_PORT"; fi
  out="$out --bfs-bucket $ARG_BFS_BUCKET"
  if [[ -n "$ARG_BFS_WRITE_PASSWORD" ]]; then out="$out --bfs-write-password $ARG_BFS_WRITE_PASSWORD"; fi
  # Without a profile, exapump validates certificates; these targets are self-signed by default.
  if [[ "$CONNECTIVITY_MODE" != "profile" ]]; then out="$out --bfs-validate-certificate false"; fi
  printf '%s\n' "${out# }"
  return 0
}

# Globbing is off around the word-split so a '*' in a password cannot expand to file names.
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

# A fresh container's SQL port can open before BucketFS HTTP listens, so this retries, but only
# on exapump's "not reachable" connection error; anything else is a config error and fails fast.
# shellcheck disable=SC2120  # $1/$2 are overridden only from install.test.sh
bucketfs_reachable() {
  local tries="${1:-$BUCKETFS_REACHABLE_TRIES}" sleep_seconds="${2:-$BUCKETFS_REACHABLE_POLL_SECONDS}" i=1 out
  while [[ "$i" -le "$tries" ]]; do
    if out="$(exapump_bucketfs ls 2>&1)"; then
      return 0
    fi
    if [[ "$out" != *"not reachable"* ]]; then
      err "BucketFS bucket '$ARG_BFS_BUCKET' is not usable: 'exapump bucketfs ls' failed. Verify --bfs-host, --bfs-port and the BucketFS write password (or the profile's bfs_* keys). exapump said: $out"
      return 1
    fi
    if [[ "$i" -lt "$tries" ]]; then
      sleep "$sleep_seconds"
    fi
    i=$((i + 1))
  done
  err "BucketFS bucket '$ARG_BFS_BUCKET' is not reachable after $tries tries: 'exapump bucketfs ls' failed. Verify --bfs-host, --bfs-port and the BucketFS write password (or the profile's bfs_* keys). exapump said: $out"
  return 1
}

bucketfs_upload_file() {
  local local_path="$1" bucket_path="$2" out
  if ! out="$(exapump_bucketfs cp "$local_path" "$bucket_path" 2>&1)"; then
    err "BucketFS upload of '$local_path' to bucket path '$bucket_path' failed. exapump said: $out"
    return 1
  fi
  log "Uploaded $local_path to BucketFS path $bucket_path."
  return 0
}

# Line-exact match so 'x.so' does not match 'x.so.bak'.
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

# BucketFS unpacks an uploaded .tar.gz asynchronously, so the path can lag the upload.
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

exasol_launcher() {
  exasol "$@" --deployment-dir "$DEPLOYMENT_DIR" </dev/null
}

launcher_slc_list() {
  local out
  if ! out="$(exasol_launcher slc list --json 2>&1)"; then
    err "'exasol slc list' failed for deployment '$ARG_DEPLOYMENT'. Check that it is running ('exasol status') and that the launcher is Exasol Personal 2.3 or later ('exasol version'). exasol said: $out"
    return 1
  fi
  printf '%s\n' "$out"
}

launcher_slc_action() {
  local list installed
  list="$(launcher_slc_list)" || return 1
  if ! installed="$(jq -r --arg alias "$LAUNCHER_SLC_ALIAS" \
      '[.[] | select(.type == "custom" and .alias == $alias)] | length' <<<"$list" 2>&1)"; then
    err "could not parse 'exasol slc list --json' output. jq said: $installed"
    return 1
  fi
  if [[ "$installed" -gt 0 ]]; then printf 'update\n'; else printf 'install\n'; fi
}

personal_bucket_dir() {
  printf '%s\n' "$DEPLOYMENT_DIR/$PERSONAL_EXA_RELATIVE_PATH/bucketfs/$BFS_SERVICE/$ARG_BFS_BUCKET"
}

# Replaced via rename so a UDF VM that already mapped the old .so never reads a half-written file.
install_engine_so_into_bucket() {
  local so_path="$1" dest dest_dir out
  dest_dir="$(personal_bucket_dir)/${TARGET_ENGINE_BFS_PATH%/*}"
  dest="$(personal_bucket_dir)/$TARGET_ENGINE_BFS_PATH"
  if ! out="$(mkdir -p "$dest_dir" 2>&1 && cp "$so_path" "$dest.partial" 2>&1 && chmod 0644 "$dest.partial" 2>&1 && mv -f "$dest.partial" "$dest" 2>&1)"; then
    rm -f "$dest.partial"
    err "could not write the engine .so to '$dest': $out"
    return 1
  fi
  log "Wrote the engine .so to $dest."
  return 0
}

# The engine registers a newly created bucket directory in bucketfs.conf within seconds; scripts
# created before that point cannot resolve their /buckets path.
wait_for_personal_bucket() {
  local conf="$DEPLOYMENT_DIR/$PERSONAL_EXA_RELATIVE_PATH/bucketfs.conf" i=1 path service bucket rest
  while [[ "$i" -le "$PERSONAL_BUCKET_TRIES" ]]; do
    if [[ -r "$conf" ]]; then
      while read -r path service bucket rest || [[ -n "$path" ]]; do
        if [[ "$service" == "$BFS_SERVICE" && "$bucket" == "$ARG_BFS_BUCKET" ]]; then
          return 0
        fi
      done <"$conf"
    fi
    if [[ "$i" -lt "$PERSONAL_BUCKET_TRIES" ]]; then
      sleep "$PERSONAL_BUCKET_POLL_SECONDS"
    fi
    i=$((i + 1))
  done
  err "the database did not register BucketFS bucket '$BFS_SERVICE/$ARG_BFS_BUCKET' in '$conf' after $PERSONAL_BUCKET_TRIES checks. Check that deployment '$ARG_DEPLOYMENT' is running ('exasol status')."
  return 1
}

deploy_personal_launcher() {
  local so_path action out
  log "Installing lakehouse-engine $RESOLVED_ENGINE_VERSION into the deployment's BucketFS directory ..."
  download_engine || return 1
  if ! so_path="$(extract_engine_so "$WORKDIR/$ENGINE_ASSET" "$WORKDIR/extracted")"; then
    return 1
  fi
  install_engine_so_into_bucket "$so_path" || return 1

  if [[ "$ARG_SKIP_SLC" -eq 1 ]]; then
    log "Skipping SLC install (--skip-slc)."
  else
    download_slc || return 1
    action="$(launcher_slc_action)" || return 1
    log "Running 'exasol slc custom $action --alias $LAUNCHER_SLC_ALIAS' for Rust SLC $RESOLVED_SLC_VERSION (restarts the database) ..."
    if ! out="$(exasol_launcher slc custom "$action" --alias "$LAUNCHER_SLC_ALIAS" \
        --language "$LAUNCHER_SLC_LANGUAGE" --source "$WORKDIR/rustslc.tar.gz" --auto-approve 2>&1)"; then
      err "'exasol slc custom $action' failed for deployment '$ARG_DEPLOYMENT'. exasol said: $out"
      return 1
    fi
  fi
  wait_for_personal_bucket || return 1
  create_engine_scripts || return 1
  return 0
}

upload_artifact() {
  local local_path="$1" saas_key="$2" bfs_path="$3"
  case "$TARGET_MODE" in
    saas)     saas_upload_file "$local_path" "$saas_key" ;;
    bucketfs) bucketfs_upload_file "$local_path" "$bfs_path" ;;
    *)        err "internal error: install target mode not resolved"; return 1 ;;
  esac
}

run_sql() {
  local sql="$1"
  case "$CONNECTIVITY_MODE" in
    profile) exapump sql --profile "$ARG_PROFILE" "$sql" </dev/null ;;
    dsn)     exapump sql -d "$ARG_DSN" "$sql" </dev/null ;;
    host)    exapump sql -d "$HOST_DSN" "$sql" </dev/null ;;
    *)       err "internal error: connectivity mode not resolved"; return 1 ;;
  esac
}

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

# A version value begins with a digit, so this cannot reuse extract_query_value, whose [0-9]* arm
# drops the row-count footer. It skips that footer by suffix instead.
extract_version_value() {
  local raw="$1" line
  while IFS= read -r line; do
    case "$line" in
      \[*)                                     continue ;;
      LAKEHOUSE_ENGINE_VERSION*)               continue ;;
      "")                                      continue ;;
      *Error*)                                 continue ;;
      *' row in set'|*' rows in set')          continue ;;
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

ddl_create_schema() {
  printf 'CREATE SCHEMA IF NOT EXISTS %s' "$1"
}

ddl_adapter() {
  printf 'CREATE OR REPLACE RUST ADAPTER SCRIPT %s.LAKEHOUSE_ADAPTER AS\n%%udf_object %s' "$1" "$2"
}

ddl_scan() {
  printf 'CREATE OR REPLACE RUST SCALAR SCRIPT %s.LAKEHOUSE_SCAN(common VARCHAR(2000000), files VARCHAR(2000000))\nEMITS (...) AS\n%%udf_object %s' "$1" "$2"
}

ddl_version() {
  printf 'CREATE OR REPLACE RUST SCALAR SCRIPT %s.LAKEHOUSE_VERSION()\nRETURNS VARCHAR(100) AS\n%%udf_object %s' "$1" "$2"
}

ddl_distribute_files() {
  printf 'CREATE OR REPLACE LUA SET SCRIPT %s.LAKEHOUSE_DISTRIBUTE_FILES(files VARCHAR(2000000))\nEMITS (files VARCHAR(2000000)) AS\nfunction run(ctx)\n    repeat\n        ctx.emit(ctx.files)\n    until not ctx.next()\nend' "$1"
}

version_smoke_sql() {
  printf 'SELECT %s.LAKEHOUSE_VERSION() AS LAKEHOUSE_ENGINE_VERSION' "$1"
}

download_release_asset() {
  local repo="$1" tag="$2" asset_name="$3" dest_path="$4"
  local dl_err
  if ! dl_err="$(curl -fsSL --proto =https -o "$dest_path" \
      "https://github.com/$repo/releases/download/$tag/$asset_name" </dev/null 2>&1)"; then
    err "failed to download asset '$asset_name' from $repo release '$tag': $dl_err"
    return 1
  fi
  return 0
}

download_slc() {
  local suffix asset
  suffix="$(resolve_arch_suffix "$ARG_ARCH")"
  asset="lc-rust-$RESOLVED_SLC_VERSION$suffix.tar.gz"
  download_release_asset "$SLC_REPO" "$RESOLVED_SLC_TAG" "$asset" "$WORKDIR/$asset" || return 1
  if ! mv -f "$WORKDIR/$asset" "$WORKDIR/rustslc.tar.gz"; then
    err "failed to rename $asset to rustslc.tar.gz."
    return 1
  fi
}

download_engine() {
  local suffix asset
  suffix="$(resolve_arch_suffix "$ARG_ARCH")"
  asset="${ENGINE_ASSET%.tar.gz}$suffix.tar.gz"
  download_release_asset "$ENGINE_REPO" "$RESOLVED_ENGINE_TAG" "$asset" "$WORKDIR/$asset" || return 1
  if [[ "$asset" != "$ENGINE_ASSET" ]] && ! mv -f "$WORKDIR/$asset" "$WORKDIR/$ENGINE_ASSET"; then
    err "failed to rename $asset to $ENGINE_ASSET."
    return 1
  fi
}

register_script_languages() {
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

register_slc() {
  log "Installing Rust SLC $RESOLVED_SLC_VERSION ..."
  download_slc || return 1
  # Uploaded as a tarball: the RUST alias points at BucketFS's auto-extracted directory.
  upload_artifact "$WORKDIR/rustslc.tar.gz" "rustslc.tar.gz" "$TARGET_SLC_BFS_PATH" || return 1
  if [[ "$TARGET_MODE" == "bucketfs" ]]; then
    bucketfs_wait_for_path "$TARGET_SLC_BFS_PATH" || return 1
  fi
  register_script_languages || return 1
  return 0
}

create_engine_scripts() {
  local schema="$ARG_SCHEMA" so="$TARGET_SO_UDF_OBJECT" stmt
  local -a statements=(
    "$(ddl_create_schema "$schema")"
    "$(ddl_adapter "$schema" "$so")"
    "$(ddl_scan "$schema" "$so")"
    "$(ddl_version "$schema" "$so")"
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

# SaaS uploads the tarball and relies on auto-extraction into ENGINE_SO_PATH; BucketFS uploads the
# bare .so to the path `make bucketfs-upload-so` and the E2E %udf_object use.
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

# Fingerprint is checked before rc: a fingerprint rejection also exits non-zero.
classify_version_smoke() {
  local rc="$1" output="$2" expected="$3"
  if [[ "$output" == *"Fingerprint mismatch"* ]]; then
    printf 'fingerprint-mismatch\n'
    return 0
  fi
  if [[ "$rc" -ne 0 ]]; then
    printf 'other-error\n'
    return 0
  fi
  if [[ "$(extract_version_value "$output")" != "$expected" ]]; then
    printf 'version-mismatch\n'
    return 0
  fi
  printf 'pass\n'
  return 0
}

run_smoke_test() {
  local sql out rc reported verdict
  sql="$(version_smoke_sql "$ARG_SCHEMA")"
  if out="$(run_sql "$sql" 2>&1)"; then rc=0; else rc=$?; fi
  reported="$(extract_version_value "$out")"
  verdict="$(classify_version_smoke "$rc" "$out" "$RESOLVED_ENGINE_VERSION")"
  case "$verdict" in
    fingerprint-mismatch)
      err "fingerprint smoke test FAILED: the registered SLC does not match this release's exasol-udf-sdk/exasol-udf-macros pin. Align the SLC version (see --slc-version) with the engine release and re-run."
      return 1 ;;
    version-mismatch)
      err "version smoke test FAILED: expected LAKEHOUSE_VERSION() to report $RESOLVED_ENGINE_VERSION, but got '${reported:-<empty>}'."
      return 1 ;;
    other-error)
      err "version smoke test failed: $out"
      return 1 ;;
    pass)
      log "Version smoke test passed: LAKEHOUSE_VERSION() reports $RESOLVED_ENGINE_VERSION."
      return 0 ;;
    *)
      err "internal error: unrecognized version smoke test verdict '$verdict'"
      return 1 ;;
  esac
}

print_next_step_template() {
  local schema="$1"
  local role="LAKEHOUSE_ENGINE_ROLE_${schema}"
  local installing_user="${ARG_USER:-<installing-user>}"
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
  emit "-- Grant the VS OWNER's scripts CONNECTION access (BEFORE CREATE VIRTUAL SCHEMA). See docs/security.md."
  emit ""
  case "$installing_user" in
    [Ss][Yy][Ss])
      emit "-- SYS holds every CONNECTION implicitly; skip the grants."
      ;;
    *)
      emit "CREATE ROLE $role;"
      emit "GRANT ACCESS ON CONNECTION LAKEHOUSE_CATALOG_CREDS FOR SCRIPT $schema.LAKEHOUSE_ADAPTER TO $role;"
      emit "GRANT ACCESS ON CONNECTION LAKEHOUSE_CATALOG_CREDS FOR SCRIPT $schema.LAKEHOUSE_SCAN TO $role;"
      emit "GRANT $role TO $installing_user;"
      ;;
  esac
  emit ""
  emit "CREATE VIRTUAL SCHEMA <MY_LAKEHOUSE>"
  emit "USING $schema.LAKEHOUSE_ADAPTER WITH"
  emit "  CATALOG_CONNECTION = 'LAKEHOUSE_CATALOG_CREDS'"
  emit "  NAMESPACE          = '<namespace>'"
  emit "  ALLOW_HTTP         = 'false';"
  emit ""
  emit "-- Readers: GRANT SELECT ON SCHEMA <MY_LAKEHOUSE> TO <user>; CREATE OR REPLACE drops ACCESS grants."
}

main() {
  set -uo pipefail

  parse_args "$@" || exit 1
  if [[ "$ARG_HELP" -eq 1 ]]; then
    usage
    exit 0
  fi

  if ! TARGET_MODE="$(resolve_target_mode)"; then
    exit 1
  fi
  if [[ -n "$ARG_DEPLOYMENT" ]]; then
    resolve_deployment_transport || exit 1
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

  # Preflight before any download, so a misconfigured run costs no bytes.
  case "$TARGET_MODE" in
    saas)
      resolve_saas_pat || exit 1
      saas_db_reachable || exit 1
      ;;
    bucketfs)
      if [[ "$DEPLOYMENT_TRANSPORT" == "launcher" ]]; then
        launcher_slc_list >/dev/null || exit 1
      else
        validate_bucketfs_required || exit 1
        # shellcheck disable=SC2119  # tries/sleep_seconds default; see bucketfs_reachable
        bucketfs_reachable || exit 1
      fi
      ;;
  esac

  if ! WORKDIR="$(mktemp -d "${TMPDIR:-/tmp}/lhvs-install.XXXXXX" 2>/dev/null)"; then
    err "failed to create a temporary working directory."
    exit 1
  fi
  trap 'rm -rf "$WORKDIR"' EXIT

  resolve_versions || exit 1
  if [[ "$DEPLOYMENT_TRANSPORT" == "launcher" ]]; then
    deploy_personal_launcher || exit 1
  else
    if [[ "$ARG_SKIP_SLC" -eq 1 ]]; then
      log "Skipping SLC registration (--skip-slc)."
    else
      register_slc || exit 1
    fi
    install_engine || exit 1
  fi
  run_smoke_test || exit 1

  print_next_step_template "$ARG_SCHEMA"
  emit ""
  emit "lakehouse-engine is installed and query-ready in schema $ARG_SCHEMA."
  exit 0
}

if [[ -z "${BASH_SOURCE[0]:-}" || "${BASH_SOURCE[0]}" == "${0}" ]]; then
  main "$@"
fi
