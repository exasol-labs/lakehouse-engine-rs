#!/usr/bin/env bash
# Credentials never appear on any process's argv: they travel via environment variables or a
# curl/jq stdin config. Shell tracing must never be enabled in this file (it would print them);
# the option is deliberately not named here because a source scan asserts its absence.
set -euo pipefail

usage() {
  echo "usage: $(basename "$0") [--source-only]" >&2
  exit 1
}

SOURCE_ONLY=0
case "${1:-}" in
  --source-only) SOURCE_ONLY=1; shift ;;
  "") ;;
  *) usage ;;
esac
[ "$#" -eq 0 ] || usage

require_var() {
  local name="$1"
  if [ -z "${!name:-}" ]; then
    echo "FATAL: required environment variable $name is not set" >&2
    exit 1
  fi
}

LK_SOURCE_KIND="${LK_SOURCE_KIND:-glue}"

case "$LK_SOURCE_KIND" in
  glue)
    require_var LK_SOURCE_REGION
    require_var LK_SOURCE_DATABASE
    ;;
  rest)
    require_var LK_SOURCE_CATALOG_URI
    require_var LK_SOURCE_TOKEN_URI
    require_var LK_SOURCE_CLIENT_ID
    require_var LK_SOURCE_CLIENT_SECRET
    require_var LK_SOURCE_NAMESPACE
    ;;
  *)
    echo "FATAL: LK_SOURCE_KIND must be 'glue' or 'rest', got '$LK_SOURCE_KIND'" >&2
    exit 1
    ;;
esac

if [ "$SOURCE_ONLY" -eq 0 ]; then
  require_var LK_TARGET_CATALOG_URI
  require_var LK_TARGET_TOKEN_URI
  require_var LK_TARGET_CLIENT_ID
  require_var LK_TARGET_CLIENT_SECRET
  require_var LK_TARGET_WAREHOUSE
  require_var LK_TARGET_NAMESPACE
  require_var LK_TARGET_REGION
  require_var LK_TARGET_ACCESS_KEY_ID
  require_var LK_TARGET_SECRET_ACCESS_KEY
fi

# Lakekeeper wire values for the storage profile's `flavor` and `path-style-access`.
if [ -n "${LK_TARGET_S3_ENDPOINT:-}" ]; then
  TARGET_S3_FLAVOR="s3-compat"
  TARGET_S3_PATH_STYLE="true"
else
  TARGET_S3_FLAVOR="aws"
  TARGET_S3_PATH_STYLE="false"
fi

# client_secret goes via --config on stdin, never argv (world-readable process listing).
oauth2_token() {
  local token_uri="$1" client_id="$2" client_secret="$3"
  curl -sf --request POST "$token_uri" --config - <<CURLCFG | jq -r '.access_token // empty'
data = "grant_type=client_credentials"
data = "client_id=$client_id"
data = "client_secret=$client_secret"
CURLCFG
}

# Token goes via --config on stdin; the passed-through args must carry no credential.
curl_bearer() {
  local token="$1"; shift
  curl -sf --config - "$@" <<CURLCFG
header = "Authorization: Bearer $token"
CURLCFG
}

urlencode() {
  jq -rn --arg v "$1" '$v|@uri'
}

# The common prefix is computed over whole path segments, never raw substrings: "part" is a
# byte-wise prefix of "partsupp", but both tables share only "tpch.db".

dirname_path() {
  local p="$1"
  case "$p" in
    */*) printf '%s' "${p%/*}" ;;
    *) printf '%s' "" ;;
  esac
}

is_ancestor_or_equal() {
  local ancestor="$1" path="$2"
  [ "$ancestor" = "$path" ] && return 0
  [ -z "$ancestor" ] && return 0
  case "$path" in
    "$ancestor"/*) return 0 ;;
    *) return 1 ;;
  esac
}

# Prints "bucket<TAB>key_prefix".
derive_bucket_and_prefix() {
  local triples="$1"

  local invalid
  invalid="$(jq -r '.[] | select((.table_location | test("^s3://[^/]+/")) | not) | "\(.name): \(.table_location)"' <<<"$triples")"
  if [ -n "$invalid" ]; then
    echo "FATAL: only s3://<bucket>/<key> table locations are supported; offending table(s):" >&2
    printf '%s\n' "$invalid" >&2
    exit 1
  fi

  local buckets
  buckets="$(jq -r '.[].table_location | capture("^s3://(?<b>[^/]+)/").b' <<<"$triples" | sort -u)"
  if [ "$(printf '%s\n' "$buckets" | grep -c .)" -ne 1 ]; then
    echo "FATAL: table locations span more than one S3 bucket: $(tr '\n' ' ' <<<"$buckets")" >&2
    exit 1
  fi
  local bucket="$buckets"

  local common="" first=1 key ancestor
  while IFS= read -r key; do
    ancestor="$(dirname_path "$key")"
    if [ "$first" -eq 1 ]; then
      common="$ancestor"
      first=0
    else
      while ! is_ancestor_or_equal "$common" "$ancestor"; do
        common="$(dirname_path "$common")"
      done
    fi
  done < <(jq -r '.[].table_location | sub("^s3://[^/]+/"; "") | sub("/$"; "")' <<<"$triples")

  if [ -z "$common" ]; then
    echo "FATAL: table locations under bucket '$bucket' share no common key prefix" >&2
    exit 1
  fi

  printf '%s\t%s\n' "$bucket" "$common"
}

# Both producers emit a JSON array of {name, metadata_location, table_location}.

fetch_source_triples_glue() {
  local tables_json
  tables_json="$(aws glue get-tables --region "$LK_SOURCE_REGION" \
    --database-name "$LK_SOURCE_DATABASE" \
    --query 'TableList[].{name:Name,metadata_location:Parameters.metadata_location}' \
    --output json)"

  local missing
  missing="$(jq -r '.[] | select(.metadata_location == null or .metadata_location == "") | .name' <<<"$tables_json")"
  if [ -n "$missing" ]; then
    echo "FATAL: Glue table(s) with no metadata_location parameter: $(tr '\n' ' ' <<<"$missing")" >&2
    exit 1
  fi

  local triples="[]" name metadata_location table_location
  while IFS=$'\t' read -r name metadata_location; do
    table_location="$(aws s3 cp "$metadata_location" - --region "$LK_SOURCE_REGION" | jq -r '.location // empty')"
    if [ -z "$table_location" ]; then
      echo "FATAL: metadata document at $metadata_location for table '$name' carries no 'location'" >&2
      exit 1
    fi
    triples="$(jq --arg name "$name" --arg metadata_location "$metadata_location" \
      --arg table_location "$table_location" \
      '. + [{name: $name, metadata_location: $metadata_location, table_location: $table_location}]' \
      <<<"$triples")"
  done < <(jq -r '.[] | [.name, .metadata_location] | @tsv' <<<"$tables_json")

  printf '%s\n' "$triples"
}

fetch_source_triples_rest() {
  local token
  token="$(oauth2_token "$LK_SOURCE_TOKEN_URI" "$LK_SOURCE_CLIENT_ID" "$LK_SOURCE_CLIENT_SECRET")"
  if [ -z "$token" ]; then
    echo "FATAL: no access token returned by $LK_SOURCE_TOKEN_URI" >&2
    exit 1
  fi

  local base="$LK_SOURCE_CATALOG_URI/v1"
  if [ -n "${LK_SOURCE_WAREHOUSE:-}" ]; then
    local prefix
    prefix="$(curl_bearer "$token" "$base/config?warehouse=$(urlencode "$LK_SOURCE_WAREHOUSE")" \
      | jq -r '.overrides.prefix // .defaults.prefix // empty')"
    [ -n "$prefix" ] && base="$base/$prefix"
  fi

  local table_names
  table_names="$(curl_bearer "$token" "$base/namespaces/$LK_SOURCE_NAMESPACE/tables" \
    | jq -r '.identifiers[].name')"
  if [ -z "$table_names" ]; then
    echo "FATAL: source namespace '$LK_SOURCE_NAMESPACE' has no tables" >&2
    exit 1
  fi

  local triples="[]" name load metadata_location table_location
  while IFS= read -r name; do
    load="$(curl_bearer "$token" "$base/namespaces/$LK_SOURCE_NAMESPACE/tables/$name")"
    metadata_location="$(jq -r '."metadata-location" // empty' <<<"$load")"
    table_location="$(jq -r '.metadata.location // empty' <<<"$load")"
    if [ -z "$metadata_location" ] || [ -z "$table_location" ]; then
      echo "FATAL: loadTable response for '$name' carries no metadata-location/location" >&2
      exit 1
    fi
    triples="$(jq --arg name "$name" --arg metadata_location "$metadata_location" \
      --arg table_location "$table_location" \
      '. + [{name: $name, metadata_location: $metadata_location, table_location: $table_location}]' \
      <<<"$triples")"
  done <<<"$table_names"

  printf '%s\n' "$triples"
}

case "$LK_SOURCE_KIND" in
  glue) SOURCE_TRIPLES="$(fetch_source_triples_glue)" ;;
  rest) SOURCE_TRIPLES="$(fetch_source_triples_rest)" ;;
esac

IFS=$'\t' read -r SOURCE_BUCKET SOURCE_KEY_PREFIX < <(derive_bucket_and_prefix "$SOURCE_TRIPLES")

if [ "$SOURCE_ONLY" -eq 1 ]; then
  jq -n --argjson tables "$SOURCE_TRIPLES" --arg bucket "$SOURCE_BUCKET" \
    --arg key_prefix "$SOURCE_KEY_PREFIX" \
    '{tables: $tables, bucket: $bucket, key_prefix: $key_prefix}'
  exit 0
fi

# One week. Required by Lakekeeper v0.13.1's soft-delete profile (no serde default).
SOFT_DELETE_EXPIRATION_SECONDS=604800

# The caller's catalog host fixes the vantage (public vs private IP); only the management path
# is derived from it.
TARGET_CATALOG_BASE="${LK_TARGET_CATALOG_URI%/}"
TARGET_MANAGEMENT_BASE="${TARGET_CATALOG_BASE%/catalog}/management/v1"

# Reserved by Lakekeeper.
case "$LK_TARGET_NAMESPACE" in
  system|examples|information_schema)
    echo "FATAL: target namespace '$LK_TARGET_NAMESPACE' is reserved by Lakekeeper" >&2
    exit 1
    ;;
esac

# Response bodies are captured for classification but NEVER printed: an error response can
# quote back the warehouse request, which carries the storage secret access key.
RESPONSE_DIR="$(mktemp -d)"
trap 'rm -rf "$RESPONSE_DIR"' EXIT
RESPONSE_BODY="$RESPONSE_DIR/response.json"

# jq reads these from its environment rather than --arg: /proc/<pid>/cmdline is world-readable,
# /proc/<pid>/environ is not. Without the export they render as JSON null.
export LK_TARGET_ACCESS_KEY_ID LK_TARGET_SECRET_ACCESS_KEY

# Prints only the HTTP status ("000" if unreachable); the body stays in $RESPONSE_BODY so a
# credential-bearing response can never be interpolated into a message.
curl_bearer_status() {
  local token="$1"; shift
  local status
  # curl leaves a stale body in place when it never connects.
  : >"$RESPONSE_BODY"
  status="$(
    curl -s -o "$RESPONSE_BODY" -w '%{http_code}' --config - "$@" <<CURLCFG
header = "Authorization: Bearer $token"
CURLCFG
  )" || status="000"
  printf '%s' "$status"
}

response_reports_storage_profile_overlap() {
  grep -qiE 'storageprofileoverlap|overlaps with existing warehouse' "$RESPONSE_BODY" 2>/dev/null
}

response_reports_location_already_taken() {
  grep -qiE 'locationalreadytaken|location.{0,40}already.{0,40}taken' "$RESPONSE_BODY" 2>/dev/null
}

# Bodies are built with `jq -n`, never string interpolation, for correct escaping.

bootstrap_request_body() {
  jq -n -c '{"accept-terms-of-use": true, "is-operator": true}'
}

# No STS role identifier: it is required only when sts-enabled is true.
target_storage_profile() {
  local profile
  profile="$(jq -n -c \
    --arg bucket "$SOURCE_BUCKET" \
    --arg key_prefix "$SOURCE_KEY_PREFIX" \
    --arg region "$LK_TARGET_REGION" \
    --arg flavor "$TARGET_S3_FLAVOR" \
    '{type: "s3", bucket: $bucket, "key-prefix": $key_prefix, region: $region,
      flavor: $flavor, "sts-enabled": false}')"
  if [ -n "${LK_TARGET_S3_ENDPOINT:-}" ]; then
    profile="$(jq -c \
      --arg endpoint "$LK_TARGET_S3_ENDPOINT" \
      --argjson path_style "$TARGET_S3_PATH_STYLE" \
      '. + {endpoint: $endpoint, "path-style-access": $path_style}' <<<"$profile")"
  fi
  printf '%s' "$profile"
}

# Credentials MUST stay env.LK_TARGET_*, never --arg/--argjson (world-readable argv).
warehouse_request_body() {
  jq -n -c \
    --arg warehouse_name "$LK_TARGET_WAREHOUSE" \
    --argjson storage_profile "$(target_storage_profile)" \
    --argjson expiration_seconds "$SOFT_DELETE_EXPIRATION_SECONDS" \
    '{
       "warehouse-name": $warehouse_name,
       "storage-profile": $storage_profile,
       "storage-credential": {
         "type": "s3",
         "credential-type": "access-key",
         "access-key-id": env.LK_TARGET_ACCESS_KEY_ID,
         "secret-access-key": env.LK_TARGET_SECRET_ACCESS_KEY
       },
       "delete-profile": {"type": "soft", "expiration-seconds": $expiration_seconds}
     }'
}

namespace_request_body() {
  jq -n -c --arg namespace "$LK_TARGET_NAMESPACE" '{namespace: [$namespace], properties: {}}'
}

# A re-run must never replace a table's recorded metadata pointer.
register_request_body() {
  jq -n -c --arg name "$1" --arg metadata_location "$2" \
    '{name: $name, "metadata-location": $metadata_location, overwrite: false}'
}

# Steps that can fail fatally are called at top level, never inside `$( )`: `exit` there leaves
# only the subshell.

# Any ambiguity answers "not bootstrapped" so bootstrap is attempted rather than skipped.
server_is_bootstrapped() {
  local token="$1" status
  status="$(curl_bearer_status "$token" --request GET "$TARGET_MANAGEMENT_BASE/info")"
  case "$status" in
    2??) ;;
    *) return 1 ;;
  esac
  jq -e '.bootstrapped == true' "$RESPONSE_BODY" >/dev/null 2>&1
}

bootstrap_server() {
  local token="$1" status
  status="$(curl_bearer_status "$token" --request POST "$TARGET_MANAGEMENT_BASE/bootstrap" \
    --header 'Content-Type: application/json' \
    --data @<(bootstrap_request_body))"
  case "$status" in
    2??|409) return 0 ;;
  esac
  echo "FATAL: bootstrap POST $TARGET_MANAGEMENT_BASE/bootstrap returned HTTP $status" >&2
  exit 1
}

# Lakekeeper 0.13.1 reports a duplicate warehouse as a 400 storage-profile overlap, not a 409.
# An overlap is not proof of an identical warehouse, so confirm_warehouse_storage_profile follows.
create_warehouse() {
  local token="$1" status
  status="$(curl_bearer_status "$token" --request POST "$TARGET_MANAGEMENT_BASE/warehouse" \
    --header 'Content-Type: application/json' \
    --data @<(warehouse_request_body))"
  case "$status" in
    2??)
      echo "==> warehouse '$LK_TARGET_WAREHOUSE': created"
      return 0
      ;;
    409)
      echo "==> warehouse '$LK_TARGET_WAREHOUSE': already present (HTTP 409)"
      return 0
      ;;
    400)
      if response_reports_storage_profile_overlap; then
        echo "==> warehouse '$LK_TARGET_WAREHOUSE': already present (HTTP 400, storage-profile overlap)"
        return 0
      fi
      ;;
  esac
  # The response body is withheld deliberately: this request carried the storage secret key.
  echo "FATAL: create-warehouse POST $TARGET_MANAGEMENT_BASE/warehouse for warehouse '$LK_TARGET_WAREHOUSE' returned HTTP $status" >&2
  exit 1
}

confirm_warehouse_storage_profile() {
  local token="$1" status profile returned_bucket returned_prefix
  status="$(curl_bearer_status "$token" --request GET "$TARGET_MANAGEMENT_BASE/warehouse")"
  case "$status" in
    2??) ;;
    *)
      echo "FATAL: list-warehouse GET $TARGET_MANAGEMENT_BASE/warehouse for warehouse '$LK_TARGET_WAREHOUSE' returned HTTP $status" >&2
      exit 1
      ;;
  esac

  profile="$(jq -c --arg name "$LK_TARGET_WAREHOUSE" \
    '[.warehouses[]? | select(.name == $name) | .["storage-profile"]][0] // empty' \
    "$RESPONSE_BODY" 2>/dev/null || printf '')"
  if [ -z "$profile" ]; then
    echo "FATAL: list-warehouse GET $TARGET_MANAGEMENT_BASE/warehouse reported no warehouse named '$LK_TARGET_WAREHOUSE' carrying a storage profile" >&2
    exit 1
  fi

  returned_bucket="$(jq -r '.bucket // ""' <<<"$profile")"
  returned_prefix="$(jq -r '.["key-prefix"] // ""' <<<"$profile")"
  if [ "$returned_bucket" != "$SOURCE_BUCKET" ] || [ "$returned_prefix" != "$SOURCE_KEY_PREFIX" ]; then
    echo "FATAL: warehouse '$LK_TARGET_WAREHOUSE' does not match the derived table location: expected bucket '$SOURCE_BUCKET' and key prefix '$SOURCE_KEY_PREFIX', got bucket '$returned_bucket' and key prefix '$returned_prefix'" >&2
    exit 1
  fi
  echo "==> warehouse '$LK_TARGET_WAREHOUSE': confirmed at s3://$returned_bucket/$returned_prefix"
}

# Sets rather than prints WAREHOUSE_PREFIX so its fatal paths terminate the script.
# Lakekeeper 0.13.1 publishes `prefix` in `defaults`, not `overrides`.
WAREHOUSE_PREFIX=""
resolve_warehouse_prefix() {
  local token="$1" status config_uri
  config_uri="$TARGET_CATALOG_BASE/v1/config?warehouse=$(urlencode "$LK_TARGET_WAREHOUSE")"
  status="$(curl_bearer_status "$token" --request GET "$config_uri")"
  case "$status" in
    2??) ;;
    *)
      echo "FATAL: config GET $TARGET_CATALOG_BASE/v1/config for warehouse '$LK_TARGET_WAREHOUSE' returned HTTP $status" >&2
      exit 1
      ;;
  esac
  WAREHOUSE_PREFIX="$(jq -r '.overrides.prefix // .defaults.prefix // empty' "$RESPONSE_BODY" 2>/dev/null || printf '')"
}

# Lakekeeper does not auto-create a namespace on register.
create_namespace() {
  local namespaces_uri="$1" token="$2" status
  status="$(curl_bearer_status "$token" --request POST "$namespaces_uri" \
    --header 'Content-Type: application/json' \
    --data @<(namespace_request_body))"
  case "$status" in
    2??)
      echo "==> namespace '$LK_TARGET_NAMESPACE': created"
      return 0
      ;;
    409)
      echo "==> namespace '$LK_TARGET_NAMESPACE': already present"
      return 0
      ;;
  esac
  echo "FATAL: create-namespace POST $namespaces_uri for namespace '$LK_TARGET_NAMESPACE' returned HTTP $status" >&2
  exit 1
}

CONFIRMED_OUTCOME="confirmed"

# The read-back decides the outcome because Lakekeeper 0.13.1 returns a byte-identical 409 for a
# real registration gap and an ordinary re-run. A different pointer reads back 2xx with the stale
# location; a location held under another table name reads back 404 (never created).
confirm_registered_metadata_location() {
  local namespaces_uri="$1" token="$2" name="$3" submitted="$4" status returned
  status="$(curl_bearer_status "$token" --request GET \
    "$namespaces_uri/$LK_TARGET_NAMESPACE/tables/$name")"
  case "$status" in
    2??) ;;
    *) printf 'readback-http-%s' "$status"; return 0 ;;
  esac
  returned="$(jq -r '."metadata-location" // empty' "$RESPONSE_BODY" 2>/dev/null || printf '')"
  if [ "$returned" = "$submitted" ]; then
    printf '%s' "$CONFIRMED_OUTCOME"
  else
    printf 'location-mismatch'
  fi
}

# 2xx and 409 are provisional until the read-back confirms them. The location-taken check must
# run before the read-back, which overwrites $RESPONSE_BODY. Never exits, so every table is tried.
register_table() {
  local register_uri="$1" namespaces_uri="$2" token="$3" name="$4" metadata_location="$5"
  local status reports_location_taken=0
  local provisional confirmation
  status="$(curl_bearer_status "$token" --request POST "$register_uri" \
    --header 'Content-Type: application/json' \
    --data @<(register_request_body "$name" "$metadata_location"))"
  if response_reports_location_already_taken; then
    reports_location_taken=1
  fi

  case "$status" in
    2??) provisional="registered" ;;
    409) provisional="already-registered" ;;
    *)
      if [ "$reports_location_taken" -eq 1 ]; then
        printf 'location-already-taken'
      else
        printf 'http-%s' "$status"
      fi
      return 0
      ;;
  esac

  if [ "$reports_location_taken" -eq 1 ]; then
    echo "      $name: register response reported the location as already taken (HTTP $status); the read-back decides the outcome" >&2
  fi

  confirmation="$(confirm_registered_metadata_location "$namespaces_uri" "$token" "$name" "$metadata_location")"
  if [ "$confirmation" = "$CONFIRMED_OUTCOME" ]; then
    printf '%s' "$provisional"
  else
    printf '%s' "$confirmation"
  fi
}

register_all_tables() {
  local token="$1" register_uri="$2" namespaces_uri="$3"
  local table_name table_metadata_location table_outcome

  echo "==> Registering $(jq -r 'length' <<<"$SOURCE_TRIPLES") table(s) into namespace '$LK_TARGET_NAMESPACE' of warehouse '$LK_TARGET_WAREHOUSE':"
  while IFS=$'\t' read -r table_name table_metadata_location; do
    table_outcome="$(register_table "$register_uri" "$namespaces_uri" "$token" "$table_name" "$table_metadata_location")"
    case "$table_outcome" in
      registered)
        REGISTERED+=("$table_name")
        echo "      $table_name: registered"
        ;;
      already-registered)
        ALREADY_PRESENT+=("$table_name")
        echo "      $table_name: already registered"
        ;;
      location-mismatch)
        FAILED+=("$table_name")
        echo "      $table_name: FAILED, registered table's metadata-location does not match the one this run submitted ($table_metadata_location); the catalog was NOT updated to point at it"
        ;;
      readback-http-*)
        FAILED+=("$table_name")
        echo "      $table_name: FAILED, confirming loadTable GET $namespaces_uri/$LK_TARGET_NAMESPACE/tables/$table_name returned HTTP ${table_outcome#readback-http-}, so the registration could not be confirmed"
        ;;
      location-already-taken)
        FAILED+=("$table_name")
        echo "      $table_name: FAILED, register POST $register_uri rejected the location as already taken"
        ;;
      *)
        FAILED+=("$table_name")
        echo "      $table_name: FAILED, register POST $register_uri returned HTTP ${table_outcome#http-}"
        ;;
    esac
  done < <(jq -r '.[] | [.name, .metadata_location] | @tsv' <<<"$SOURCE_TRIPLES")
}

report_registration_summary() {
  echo "==> Summary: ${#REGISTERED[@]} registered, ${#ALREADY_PRESENT[@]} already present, ${#FAILED[@]} failed"

  if [ "${#FAILED[@]}" -gt 0 ]; then
    echo "FATAL: registration into namespace '$LK_TARGET_NAMESPACE' of warehouse '$LK_TARGET_WAREHOUSE' failed for: ${FAILED[*]}" >&2
    exit 1
  fi
}

TARGET_TOKEN="$(oauth2_token "$LK_TARGET_TOKEN_URI" "$LK_TARGET_CLIENT_ID" "$LK_TARGET_CLIENT_SECRET")" \
  || TARGET_TOKEN=""
if [ -z "$TARGET_TOKEN" ]; then
  echo "FATAL: OAuth2 client-credentials grant at $LK_TARGET_TOKEN_URI returned no access token" >&2
  exit 1
fi

if server_is_bootstrapped "$TARGET_TOKEN"; then
  echo "==> Lakekeeper server: already bootstrapped"
else
  bootstrap_server "$TARGET_TOKEN"
  echo "==> Lakekeeper server: bootstrapped"
fi

create_warehouse "$TARGET_TOKEN"
confirm_warehouse_storage_profile "$TARGET_TOKEN"

resolve_warehouse_prefix "$TARGET_TOKEN"
TARGET_PREFIXED_BASE="$TARGET_CATALOG_BASE/v1"
if [ -n "$WAREHOUSE_PREFIX" ]; then
  TARGET_PREFIXED_BASE="$TARGET_PREFIXED_BASE/$WAREHOUSE_PREFIX"
fi
TARGET_NAMESPACES_URI="$TARGET_PREFIXED_BASE/namespaces"
TARGET_REGISTER_URI="$TARGET_NAMESPACES_URI/$LK_TARGET_NAMESPACE/register"

create_namespace "$TARGET_NAMESPACES_URI" "$TARGET_TOKEN"

REGISTERED=()
ALREADY_PRESENT=()
FAILED=()

register_all_tables "$TARGET_TOKEN" "$TARGET_REGISTER_URI" "$TARGET_NAMESPACES_URI"
report_registration_summary
