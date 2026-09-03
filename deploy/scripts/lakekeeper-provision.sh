#!/usr/bin/env bash
# Registers Iceberg tables already cataloged in a source catalog into a target Lakekeeper
# warehouse by reference (register-table) — no data rewrite, no Parquet/manifest/metadata
# file is ever written by this script.
#
# Configuration is via LK_SOURCE_* / LK_TARGET_* environment variables ONLY. The only accepted
# command-line argument is --source-only. No credential is ever placed on this script's own
# command line, and no credential reaches any process this script spawns through THAT process's
# command line either — every credential travels via an environment variable, an SSM
# SecureString (read by the caller before invoking this script), or a curl/jq file descriptor.
# Shell tracing MUST NOT be enabled anywhere in this file: it prints every expanded command,
# including the credential-bearing ones. The banned option is not spelled out here on purpose —
# an offline source-text scan asserts the whole file is free of it, and a comment quoting the
# token would fail that scan.
#
# Section 1 (task 2.1 — source half): env validation, the two source producers (glue|rest)
# normalized to one (name, metadata_location, table_location) triple per table, bucket/key-prefix
# derivation, target S3 flavor/path-style derivation, and the --source-only mode.
# Section 2 (task 2.2 — target half): OAuth2 token, server-info read, bootstrap, warehouse create
# + confirming read-back, warehouse-prefix resolution, namespace create, one register call per
# table, and the per-table summary. Each register outcome is settled by its own confirming
# read-back (task 2.3), for the reason confirm_registered_metadata_location documents.
set -euo pipefail

# ==============================================================================================
# Section 1: source half (task 2.1)
# ==============================================================================================

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

# --- Environment validation --------------------------------------------------------------------

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

# The target half (task 2.2) needs every one of these, but validating their presence up front —
# before any source read runs — means a misconfigured full run fails fast rather than after
# already reading Glue/S3. Skipped entirely in --source-only mode, which never touches the
# target catalog at all.
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

# Target S3 flavor + path-style — DERIVED from whether LK_TARGET_S3_ENDPOINT is set, never a
# separate toggle (plan.md "Derive, don't configure"). Unset => real AWS S3, virtual-hosted
# addressing. Set (e.g. MinIO for local verification) => S3-compatible, path-style addressing.
#
# Both hold Lakekeeper's own WIRE values, not internal enum names: the storage profile's `flavor`
# field is a kebab-case tagged value ("aws" / "s3-compat") and `path-style-access` is a JSON
# boolean. Keeping the wire spelling here rather than translating it in section 2 leaves one
# owner for the decision instead of a derivation and a lookup table that can disagree.
if [ -n "${LK_TARGET_S3_ENDPOINT:-}" ]; then
  TARGET_S3_FLAVOR="s3-compat"
  TARGET_S3_PATH_STYLE="true"
else
  TARGET_S3_FLAVOR="aws"
  TARGET_S3_PATH_STYLE="false"
fi

# --- Shared no-argv-credential helpers (used by both the "rest" source producer here and by the
# target half task 2.2 appends) ------------------------------------------------------------------

# OAuth2 client-credentials token request. client_secret reaches curl ONLY via --config on stdin,
# never via -d/--data on curl's own argv, because argv is a world-readable process listing.
oauth2_token() {
  local token_uri="$1" client_id="$2" client_secret="$3"
  curl -sf --request POST "$token_uri" --config - <<CURLCFG | jq -r '.access_token // empty'
data = "grant_type=client_credentials"
data = "client_id=$client_id"
data = "client_secret=$client_secret"
CURLCFG
}

# Bearer-authenticated curl call. The token reaches curl ONLY via --config on stdin, never via a
# -H argv token. Remaining args are passed straight through to curl (method, URL, --data-binary,
# etc.) and MUST carry no credential themselves.
curl_bearer() {
  local token="$1"; shift
  curl -sf --config - "$@" <<CURLCFG
header = "Authorization: Bearer $token"
CURLCFG
}

urlencode() {
  jq -rn --arg v "$1" '$v|@uri'
}

# --- Bucket / common key-prefix derivation ------------------------------------------------------
#
# dirname_path / is_ancestor_or_equal implement the "shorten-to-parent" rule by walking whole
# PATH SEGMENTS, never raw substrings. This is what makes the derivation safe for TPC-H's
# part/partsupp shape: "part" is a byte-wise prefix of "partsupp", but dirname_path("tpch.db/part")
# and dirname_path("tpch.db/partsupp") are both exactly "tpch.db" — the segment boundary is never
# split mid-word the way a raw longest-common-substring computation would split it.

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

# arg: JSON array of triples -> prints "bucket<TAB>key_prefix" on success, exits non-zero on
# a table location that is not an s3://<bucket>/<key> URL, a mixed-bucket set, or an empty
# derived prefix.
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

# --- Source producers -----------------------------------------------------------------------
#
# Both producers normalize to the SAME shape: a JSON array of {name, metadata_location,
# table_location} objects. Everything downstream of fetch_source_triples (bucket/prefix
# derivation and, in task 2.2, registration) knows only this triple — never how a catalog is
# read (plan.md "Source read normalizes to one triple").

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

# ==============================================================================================
# Section 2: target half (task 2.2)
# ==============================================================================================
# Consumes: $SOURCE_TRIPLES (the normalized JSON array), $SOURCE_BUCKET, $SOURCE_KEY_PREFIX,
# $TARGET_S3_FLAVOR, $TARGET_S3_PATH_STYLE, the oauth2_token / urlencode helpers above, and
# every validated LK_TARGET_* variable.
#
# Order (plan.md "Provisioning order"): token -> server info -> bootstrap -> warehouse create ->
# confirming read-back -> warehouse prefix -> namespace -> one register call per table, each with
# its own confirming read-back -> summary.

# One week. Lakekeeper v0.13.1's soft TabularDeleteProfile variant carries no serde default for
# `expiration-seconds`, so the field is REQUIRED and a body omitting it fails warehouse creation.
# The value matches upstream's own tests/migrations/create-warehouse/soft-delete-1week.json.
SOFT_DELETE_EXPIRATION_SECONDS=604800

# --- Target endpoint bases ----------------------------------------------------------------------
#
# LK_TARGET_CATALOG_URI is used VERBATIM for the Iceberg REST surface. Its HOST fixes the vantage
# (public IP from a laptop, private IP from inside the VPC) and the caller owns that choice, so
# nothing here rewrites, derives, or vantage-corrects the host. The management API is the same
# deployment's other API root on that same host and no LK_TARGET_* variable carries it, so its
# PATH — and only its path — is derived by swapping the /catalog suffix for /management/v1.
TARGET_CATALOG_BASE="${LK_TARGET_CATALOG_URI%/}"
TARGET_MANAGEMENT_BASE="${TARGET_CATALOG_BASE%/catalog}/management/v1"

# Lakekeeper reserves these namespace names. Rejecting one up front fails the whole run with a
# clear cause instead of surfacing as eight identical per-table registration errors.
case "$LK_TARGET_NAMESPACE" in
  system|examples|information_schema)
    echo "FATAL: target namespace '$LK_TARGET_NAMESPACE' is reserved by Lakekeeper" >&2
    exit 1
    ;;
esac

# --- Captured response bodies -------------------------------------------------------------------
#
# Classifying an idempotent re-run needs the body of calls that may legitimately fail, so each
# response is captured to a file in a mktemp -d directory removed by an EXIT trap. It is NEVER
# printed on any path: the warehouse request carries the storage secret access key, and an error
# response can quote the offending request back.
RESPONSE_DIR="$(mktemp -d)"
trap 'rm -rf "$RESPONSE_DIR"' EXIT
RESPONSE_BODY="$RESPONSE_DIR/response.json"

# The warehouse body's two credential fields reach jq through jq's own ENVIRONMENT rather than a
# --arg token, because /proc/<pid>/cmdline is world-readable while /proc/<pid>/environ is not.
# This export is what puts them there: a value that arrived as a plain shell variable rather than
# an exported one satisfies the section-1 validation but renders as JSON null inside `jq -n`.
export LK_TARGET_ACCESS_KEY_ID LK_TARGET_SECRET_ACCESS_KEY

# --- Target request plumbing ----------------------------------------------------------------------

# Bearer-authenticated request that CLASSIFIES rather than fails. Writes the response body to
# $RESPONSE_BODY and prints ONLY the HTTP status code on stdout ("000" when the request never
# reached a server). The body deliberately does not come back as a return value — keeping it in a
# file is what makes it impossible to interpolate a credential-bearing response into a message.
#
# The token reaches curl through --config on stdin, never a -H argv token. Remaining args pass
# straight through and MUST carry no credential themselves.
curl_bearer_status() {
  local token="$1"; shift
  local status
  # curl leaves a previous call's body in place when it never connects, so truncate first: a
  # stale body would be classified as if it belonged to this request.
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

# --- Request bodies -------------------------------------------------------------------------------
#
# Every body is built by `jq -n`, never by string interpolation or a heredoc: bash has no
# compile-time JSON checking, and jq -n is the only construction that guarantees a well-formed
# body and correct escaping of a value carrying a quote, a backslash, or a newline.

bootstrap_request_body() {
  jq -n -c '{"accept-terms-of-use": true, "is-operator": true}'
}

# The S3 storage profile. `sts-enabled` is false, so the profile carries no STS role identifier —
# that field is required only when AWS-flavored credential vending is on. The endpoint and
# path-style pair appear only for an S3-compatible store, so the AWS run and the local MinIO
# verification differ by exactly the values section 1 derived.
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

# The one credential-bearing body. `access-key-id` and `secret-access-key` are read from jq's
# environment (env.LK_TARGET_*) and MUST NOT be moved to --arg or --argjson: that would place the
# storage secret in jq's own world-readable process listing, exactly the exposure the --config
# rule closes for curl. Every non-credential field keeps --arg. The canonical credential field
# names are used rather than the aliased aws-* spellings the in-repo Rust E2E harness happens to
# send. The soft delete-profile is named explicitly rather than left to the server default.
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

# `overwrite` is a literal JSON false in the program text rather than an omitted or null field,
# so a re-run can never replace a table's recorded metadata pointer.
register_request_body() {
  jq -n -c --arg name "$1" --arg metadata_location "$2" \
    '{name: $name, "metadata-location": $metadata_location, overwrite: false}'
}

# --- Provisioning steps ---------------------------------------------------------------------------
#
# Every step below that can fail fatally is called at TOP LEVEL, never inside a command
# substitution: `exit` inside `$( )` leaves only the subshell, which would turn a fatal
# misconfiguration into a silently-empty value.

# The server-info endpoint answers 401 to an anonymous caller once authentication is enabled, so
# the token is obtained before the first management call. Any ambiguity — unreachable, non-2xx,
# unparseable, or field absent — answers "not bootstrapped" so the request is attempted rather
# than silently skipped. The server id is NOT consulted: it is always populated.
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

# 2xx, 409, and a 400 whose body reports a storage-profile overlap are all accepted, because
# Lakekeeper 0.13.1 reports a duplicate warehouse as a 400 rather than a 409. Every accepted
# outcome is then confirmed by confirm_warehouse_storage_profile: the reported error is about
# OVERLAPPING profiles rather than an identical warehouse, so for warehouses sharing a bucket the
# already-present reading is an inference and only a read-back makes it a fact.
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

# Without this read-back a shifted key prefix, or a different overlapping warehouse in the same
# bucket, is swallowed as success and every table is then registered into a warehouse whose
# bucket and prefix the script never confirmed. Both the expected and the returned values are
# named; neither is a credential, and the listing does not echo storage credentials.
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

# Lakekeeper serves the Iceberg REST surface under a per-warehouse prefix. Sets WAREHOUSE_PREFIX
# rather than printing it, so its own fatal paths really terminate the script.
#
# The Iceberg REST config document carries catalog properties in two objects, `defaults` and
# `overrides`, and the spec lets a server publish a property in either; a client merges defaults
# first and lets overrides win. Lakekeeper 0.13.1 publishes `prefix` in `defaults` (confirmed
# against the local stack), so reading `overrides` alone yields an empty prefix and every
# subsequent catalog call lands on an unprefixed path the server answers 404 for.
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

# Lakekeeper does not auto-create a namespace on register, so this runs first and treats an
# already-exists answer as success.
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

# The word confirm_registered_metadata_location prints when the read-back agrees with what this run
# submitted. Named rather than repeated as a literal, because that function and register_table have
# to spell it identically for a confirmed table to keep its provisional outcome.
CONFIRMED_OUTCOME="confirmed"

# Reads the just-registered table back and prints CONFIRMED_OUTCOME when the catalog really holds
# the metadata location this run submitted for it, or the failing outcome word when it does not.
#
# This read-back — not the register response's own text — is what the exit code rests on, because
# Lakekeeper 0.13.1 answers a genuine registration gap and an ordinary already-registered re-run
# with a BYTE-IDENTICAL body (decision [29]):
#
#   409 {"error":{"message":"Tabular with the same name already exists in the namespace",
#                 "type":"AlreadyExistsException","code":409, ...}}
#
# Verified live on 0.13.1, the two gap shapes behind that one body read back differently, so each
# gets its own outcome rather than one blurred label: a target table holding a DIFFERENT pointer
# answers 2xx with the stale location, while a location already held under ANOTHER table name
# answers 404 — the table was never created. Reporting that 404 as a pointer mismatch would send an
# operator looking for a discrepancy that does not exist.
#
# A read-back that cannot be performed at all confirms nothing and so is never a success either.
#
# The URI is the source producer's own loadTable path, built on the target's prefixed base.
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

# Registers one table BY REFERENCE and prints an outcome word.
#
# A 2xx and a 409 are both only PROVISIONAL here: neither proves the catalog ends up holding this
# run's metadata location for this name, so each is confirmed by a read-back before it becomes a
# success. Any other status is a definitive failure and is reported without a read-back — there is
# nothing to confirm.
#
# response_reports_location_already_taken is read BEFORE the read-back and only ever logged on the
# provisional path: it and the read-back share $RESPONSE_BODY, so the read-back overwrites the
# register response, and its text is a documentation aid rather than the classification.
#
# Never exits: one table's failure must not stop the remaining tables from being attempted, so
# the caller collects every outcome and decides the exit code once.
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

# Registers every table in $SOURCE_TRIPLES into the given register/namespaces URIs, populating
# the module-level REGISTERED / ALREADY_PRESENT / FAILED outcome arrays and printing one line per
# table. Never exits on a single table's failure — see register_table's own doc comment — so the
# caller decides the run's overall exit code once every table has been attempted.
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

# Prints the per-outcome tally and exits 1 naming every failed table when any registration failed.
report_registration_summary() {
  echo "==> Summary: ${#REGISTERED[@]} registered, ${#ALREADY_PRESENT[@]} already present, ${#FAILED[@]} failed"

  if [ "${#FAILED[@]}" -gt 0 ]; then
    echo "FATAL: registration into namespace '$LK_TARGET_NAMESPACE' of warehouse '$LK_TARGET_WAREHOUSE' failed for: ${FAILED[*]}" >&2
    exit 1
  fi
}

# --- Run ------------------------------------------------------------------------------------------

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
