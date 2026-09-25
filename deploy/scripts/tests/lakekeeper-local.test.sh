#!/usr/bin/env bash
# Live integration test for lakekeeper-provision.sh against the local Docker stack; fails rather
# than skips when the stack is down.
#
#   docker compose -f docker-compose.yml -f docker-compose.lakekeeper.yml up -d --wait \
#     minio iceberg-rest keycloak lakekeeper-db lakekeeper-migrate lakekeeper
#   bash deploy/scripts/tests/lakekeeper-local.test.sh
#
# Source tables live in the iceberg-rest fixture, not Lakekeeper: Lakekeeper rejects overlapping
# warehouse key prefixes (400 CreateWarehouseStorageProfileOverlap), and on AWS the data exists
# before any warehouse. Source tables are dropped (never purged) before registering, because
# Lakekeeper 0.13.1 reports location and name conflicts with the same 409 body.
#
# Nothing is cleaned up; `docker compose ... down -v` clears it.

set -uo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROVISION="$HERE/../lakekeeper-provision.sh"

# Must stay in sync with docker-compose.yml / docker-compose.lakekeeper.yml.
KEYCLOAK_PORT="${LH_KEYCLOAK_PORT:-28080}"
LAKEKEEPER_PORT="${LH_LAKEKEEPER_PORT:-28181}"
SOURCE_REST_PORT="${LH_REST_PORT:-18181}"

KEYCLOAK_REALM_URI="http://localhost:$KEYCLOAK_PORT/realms/iceberg"
TOKEN_URI="$KEYCLOAK_REALM_URI/protocol/openid-connect/token"
OAUTH_CLIENT_ID="lakehouse"
OAUTH_CLIENT_SECRET="lakehouse-engine-secret"

SOURCE_CATALOG="http://localhost:$SOURCE_REST_PORT"
TARGET_CATALOG="http://localhost:$LAKEKEEPER_PORT/catalog"
TARGET_MANAGEMENT="http://localhost:$LAKEKEEPER_PORT/management/v1"

WAREHOUSE_BUCKET="warehouse"
WAREHOUSE_REGION="us-east-1"
# Dialed by the Lakekeeper container itself, hence the docker-network name.
WAREHOUSE_S3_ENDPOINT="http://minio:9000"
WAREHOUSE_ACCESS_KEY_ID="minioadmin"
WAREHOUSE_SECRET_ACCESS_KEY="minioadmin"

# Unique per run so warehouse names and key prefixes never collide across runs.
RUN_ID="lktest_$(date +%Y%m%d%H%M%S)_$$"

PASS=0
FAIL=0

pass() { PASS=$((PASS + 1)); printf '  ok   %s\n' "$1"; }
fail() { FAIL=$((FAIL + 1)); printf '  FAIL %s\n' "$1"; [[ -n "${2:-}" ]] && printf '       %s\n' "$2"; }

assert_eq()          { if [[ "$2" == "$3" ]]; then pass "$1"; else fail "$1" "expected [$2] got [$3]"; fi; }
assert_contains()    { if [[ "$2" == *"$3"* ]]; then pass "$1"; else fail "$1" "missing [$3]"; fi; }
assert_rc_zero()     { if [[ "$2" -eq 0 ]]; then pass "$1"; else fail "$1" "expected rc 0 got $2"; fi; }
assert_rc_nonzero()  { if [[ "$2" -ne 0 ]]; then pass "$1"; else fail "$1" "expected a non-zero rc, got 0"; fi; }

assert_2xx() {
  case "$2" in
    2??) pass "$1" ;;
    *) fail "$1" "expected 2xx got $2 $(response_error)" ;;
  esac
}

# Never echo the raw body: an error response can quote back the warehouse request's secret.
response_error() {
  jq -r 'if .error then "(\(.error.type // "?"): \(.error.message // "?"))" else "" end' \
    "$BODY_FILE" 2>/dev/null || printf ''
}

SANDBOX="$(mktemp -d)"
trap 'rm -rf "$SANDBOX"' EXIT
BODY_FILE="$SANDBOX/response.json"

fatal() { printf 'FATAL: %s\n' "$1" >&2; exit 1; }

require_command() {
  command -v "$1" >/dev/null 2>&1 || fatal "$1 is required to run this test; install it and re-run"
}

require_endpoint() {
  local name="$1" url="$2" status
  status="$(curl -s -o /dev/null -w '%{http_code}' --max-time 10 "$url" 2>/dev/null)"
  case "$status" in
    2??) return 0 ;;
  esac
  fatal "$name is not answering at $url (HTTP $status). This test requires the local Docker stack and never skips. Start it with:
  docker compose -f docker-compose.yml -f docker-compose.lakekeeper.yml up -d --wait minio iceberg-rest keycloak lakekeeper-db lakekeeper-migrate lakekeeper"
}

require_stack() {
  require_command curl
  require_command jq
  [ -r "$PROVISION" ] || fatal "provisioning script not found at $PROVISION"
  # The realm discovery document, not Keycloak liveness, proves the realm import finished.
  require_endpoint "Keycloak realm 'iceberg'" "$KEYCLOAK_REALM_URI/.well-known/openid-configuration"
  require_endpoint "Lakekeeper" "http://localhost:$LAKEKEEPER_PORT/health"
  require_endpoint "source Iceberg REST catalog" "$SOURCE_CATALOG/v1/config"
}

# One token serves both catalogs; the source fixture ignores it.
TOKEN=""

refresh_token() {
  TOKEN="$(curl -sf --request POST "$TOKEN_URI" \
    -d grant_type=client_credentials \
    -d "client_id=$OAUTH_CLIENT_ID" \
    -d "client_secret=$OAUTH_CLIENT_SECRET" | jq -r '.access_token // empty')"
  [ -n "$TOKEN" ] || fatal "Keycloak client-credentials grant at $TOKEN_URI returned no access token"
}

# These print the HTTP status and leave the body in $BODY_FILE. No `|| echo 000` fallback: curl
# already prints 000 when unreachable. Truncate first: curl leaves a stale body when it never
# connects.

api_get() {
  : >"$BODY_FILE"
  curl -s -o "$BODY_FILE" -w '%{http_code}' --max-time 60 \
    -H "Authorization: Bearer $TOKEN" "$1" 2>/dev/null
}

api_post() {
  : >"$BODY_FILE"
  curl -s -o "$BODY_FILE" -w '%{http_code}' --max-time 60 --request POST "$1" \
    -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
    --data "$2" 2>/dev/null
}

# purgeRequested must stay a literal false: a purge deletes the objects register-by-reference needs.
drop_source_table() {
  : >"$BODY_FILE"
  curl -s -o "$BODY_FILE" -w '%{http_code}' --max-time 60 --request DELETE \
    -H "Authorization: Bearer $TOKEN" \
    "$SOURCE_CATALOG/v1/namespaces/$1/tables/$2?purgeRequested=false" 2>/dev/null
}

create_source_namespace() {
  api_post "$SOURCE_CATALOG/v1/namespaces" \
    "$(jq -n --arg ns "$1" '{namespace: [$ns], properties: {}}')"
}

# The fixture derives the location from namespace and table name, so the names are the shape
# under test.
create_source_table() {
  api_post "$SOURCE_CATALOG/v1/namespaces/$1/tables" \
    "$(jq -n --arg name "$2" '{
         name: $name,
         schema: {type: "struct", "schema-id": 0,
                  fields: [{id: 1, name: "id", required: true, type: "long"}]}
       }')"
}

source_metadata_location() {
  api_get "$SOURCE_CATALOG/v1/namespaces/$1/tables/$2" >/dev/null
  jq -r '."metadata-location" // empty' "$BODY_FILE" 2>/dev/null || printf ''
}

source_table_count() {
  api_get "$SOURCE_CATALOG/v1/namespaces/$1/tables" >/dev/null
  jq -r '(.identifiers // []) | length' "$BODY_FILE" 2>/dev/null || printf '0'
}

create_target_warehouse() {
  api_post "$TARGET_MANAGEMENT/warehouse" \
    "$(jq -n --arg name "$1" --arg bucket "$WAREHOUSE_BUCKET" --arg key_prefix "$2" \
        --arg region "$WAREHOUSE_REGION" --arg endpoint "$WAREHOUSE_S3_ENDPOINT" \
        --arg access_key_id "$WAREHOUSE_ACCESS_KEY_ID" \
        --arg secret_access_key "$WAREHOUSE_SECRET_ACCESS_KEY" \
        '{
           "warehouse-name": $name,
           "storage-profile": {
             type: "s3", bucket: $bucket, "key-prefix": $key_prefix, region: $region,
             flavor: "s3-compat", "sts-enabled": false,
             endpoint: $endpoint, "path-style-access": true
           },
           "storage-credential": {
             type: "s3", "credential-type": "access-key",
             "access-key-id": $access_key_id, "secret-access-key": $secret_access_key
           },
           "delete-profile": {type: "soft", "expiration-seconds": 604800}
         }')"
}

target_catalog_base() {
  local prefix
  api_get "$TARGET_CATALOG/v1/config?warehouse=$(jq -rn --arg v "$1" '$v|@uri')" >/dev/null
  prefix="$(jq -r '.overrides.prefix // .defaults.prefix // empty' "$BODY_FILE" 2>/dev/null || printf '')"
  if [ -n "$prefix" ]; then
    printf '%s' "$TARGET_CATALOG/v1/$prefix"
  else
    printf '%s' "$TARGET_CATALOG/v1"
  fi
}

create_target_namespace() {
  api_post "$1/namespaces" "$(jq -n --arg ns "$2" '{namespace: [$ns], properties: {}}')"
}

register_table() {
  api_post "$1/namespaces/$2/register" \
    "$(jq -n --arg name "$3" --arg metadata_location "$4" \
        '{name: $name, "metadata-location": $metadata_location, overwrite: false}')"
}

registered_metadata_location() {
  api_get "$1/namespaces/$2/tables/$3" >/dev/null
  jq -r '."metadata-location" // empty' "$BODY_FILE" 2>/dev/null || printf ''
}

PROVISION_SOURCE_NAMESPACE="${RUN_ID}_src"
PROVISION_WAREHOUSE="${RUN_ID}_wh"
PROVISION_TARGET_NAMESPACE="tpch"

run_provision() {
  local source_namespace="$1" warehouse="$2" logfile="$3"
  LK_SOURCE_KIND=rest \
  LK_SOURCE_CATALOG_URI="$SOURCE_CATALOG" \
  LK_SOURCE_TOKEN_URI="$TOKEN_URI" \
  LK_SOURCE_CLIENT_ID="$OAUTH_CLIENT_ID" \
  LK_SOURCE_CLIENT_SECRET="$OAUTH_CLIENT_SECRET" \
  LK_SOURCE_NAMESPACE="$source_namespace" \
  LK_TARGET_CATALOG_URI="$TARGET_CATALOG" \
  LK_TARGET_TOKEN_URI="$TOKEN_URI" \
  LK_TARGET_CLIENT_ID="$OAUTH_CLIENT_ID" \
  LK_TARGET_CLIENT_SECRET="$OAUTH_CLIENT_SECRET" \
  LK_TARGET_WAREHOUSE="$warehouse" \
  LK_TARGET_NAMESPACE="$PROVISION_TARGET_NAMESPACE" \
  LK_TARGET_REGION="$WAREHOUSE_REGION" \
  LK_TARGET_ACCESS_KEY_ID="$WAREHOUSE_ACCESS_KEY_ID" \
  LK_TARGET_SECRET_ACCESS_KEY="$WAREHOUSE_SECRET_ACCESS_KEY" \
  LK_TARGET_S3_ENDPOINT="$WAREHOUSE_S3_ENDPOINT" \
    bash "$PROVISION" >"$logfile" 2>&1
}

test_bootstrap_and_warehouse_creation_are_idempotent() {
  echo "test_bootstrap_and_warehouse_creation_are_idempotent"
  refresh_token

  local ns="$PROVISION_SOURCE_NAMESPACE" table
  assert_2xx "source namespace $ns created" "$(create_source_namespace "$ns")"
  for table in customer part partsupp; do
    assert_2xx "source table $table created" "$(create_source_table "$ns" "$table")"
  done

  # Data first, warehouse second, as on AWS.
  local first="$SANDBOX/provision-1.log" second="$SANDBOX/provision-2.log" rc
  run_provision "$ns" "$PROVISION_WAREHOUSE" "$first"; rc=$?
  assert_rc_zero "first provisioning run exits 0" "$rc"
  local out; out="$(cat "$first")"
  assert_contains "first run creates the warehouse over the populated prefix" \
    "$out" "warehouse '$PROVISION_WAREHOUSE': created"
  assert_contains "first run confirms the warehouse storage profile by read-back" \
    "$out" "confirmed at s3://$WAREHOUSE_BUCKET/$ns"
  assert_contains "first run creates the target namespace" \
    "$out" "namespace '$PROVISION_TARGET_NAMESPACE': created"
  for table in customer part partsupp; do
    assert_contains "first run registers $table" "$out" "      $table: registered"
  done
  assert_contains "first run registers every table and fails none" \
    "$out" "Summary: 3 registered, 0 already present, 0 failed"

  run_provision "$ns" "$PROVISION_WAREHOUSE" "$second"; rc=$?
  assert_rc_zero "second provisioning run exits 0" "$rc"
  out="$(cat "$second")"
  assert_contains "second run finds the server already bootstrapped" \
    "$out" "Lakekeeper server: already bootstrapped"
  assert_contains "second run finds the warehouse already present" \
    "$out" "warehouse '$PROVISION_WAREHOUSE': already present"
  assert_contains "second run confirms the same storage profile" \
    "$out" "confirmed at s3://$WAREHOUSE_BUCKET/$ns"
  assert_contains "second run finds the target namespace already present" \
    "$out" "namespace '$PROVISION_TARGET_NAMESPACE': already present"
  for table in customer part partsupp; do
    assert_contains "second run finds $table already registered" "$out" "      $table: already registered"
  done
  assert_contains "second run registers nothing and fails nothing" \
    "$out" "Summary: 0 registered, 3 already present, 0 failed"
}

# Registers must answer 2xx: on 0.13.1 a rejected location is a 409 indistinguishable from an
# already-present name, so accepting 409 would make the check unfalsifiable.
register_pair_by_reference() {
  local label="$1" first="$2" second="$3"
  local ns="${RUN_ID}_${label}" warehouse="${RUN_ID}_${label}_wh" target_namespace="tpch"
  local first_location second_location base

  assert_2xx "$label: source namespace created" "$(create_source_namespace "$ns")"
  assert_2xx "$label: source table $first created" "$(create_source_table "$ns" "$first")"
  assert_2xx "$label: source table $second created" "$(create_source_table "$ns" "$second")"

  first_location="$(source_metadata_location "$ns" "$first")"
  second_location="$(source_metadata_location "$ns" "$second")"
  assert_contains "$label: $first has a metadata location under the shared prefix" \
    "$first_location" "s3://$WAREHOUSE_BUCKET/$ns/$first/"
  assert_contains "$label: $second has a metadata location under the shared prefix" \
    "$second_location" "s3://$WAREHOUSE_BUCKET/$ns/$second/"

  assert_2xx "$label: source table $first dropped without purge" "$(drop_source_table "$ns" "$first")"
  assert_2xx "$label: source table $second dropped without purge" "$(drop_source_table "$ns" "$second")"
  assert_eq "$label: no live source table still holds either location" 0 "$(source_table_count "$ns")"

  assert_2xx "$label: warehouse created over the already-populated key prefix" \
    "$(create_target_warehouse "$warehouse" "$ns")"
  base="$(target_catalog_base "$warehouse")"
  assert_2xx "$label: target namespace created" "$(create_target_namespace "$base" "$target_namespace")"

  assert_2xx "$label: $first registers by reference" \
    "$(register_table "$base" "$target_namespace" "$first" "$first_location")"
  assert_2xx "$label: $second registers by reference after $first" \
    "$(register_table "$base" "$target_namespace" "$second" "$second_location")"

  assert_eq "$label: registered $first still points at the source metadata document" \
    "$first_location" "$(registered_metadata_location "$base" "$target_namespace" "$first")"
  assert_eq "$label: registered $second still points at the source metadata document" \
    "$second_location" "$(registered_metadata_location "$base" "$target_namespace" "$second")"
}

test_register_table_by_reference_preserves_metadata_location() {
  echo "test_register_table_by_reference_preserves_metadata_location"
  refresh_token

  # Positive control: neither location is a prefix of the other.
  register_pair_by_reference control orders customer

  # part is a byte-wise prefix of partsupp (reported upstream as LocationAlreadyTaken). Both orders,
  # since only the forward one was observed live.
  register_pair_by_reference collide_fwd part partsupp
  register_pair_by_reference collide_rev partsupp part
}

# Lakekeeper 0.13.1 answers both rejections below with the same 409 AlreadyExistsException body,
# so only the read-back can tell them from an ordinary re-run. Both cases must fail the run.

# Sets TARGET_BASE rather than printing it, since its assertions also write to stdout.
TARGET_BASE=""
prepare_target_for() {
  local label="$1" warehouse="$2" key_prefix="$3"
  assert_2xx "$label: warehouse created over the already-populated key prefix" \
    "$(create_target_warehouse "$warehouse" "$key_prefix")"
  TARGET_BASE="$(target_catalog_base "$warehouse")"
  assert_2xx "$label: target namespace created" \
    "$(create_target_namespace "$TARGET_BASE" "$PROVISION_TARGET_NAMESPACE")"
}

# Register answers 409; the read-back answers 200 with the stale pointer.
test_register_rejects_a_target_table_holding_a_different_metadata_location() {
  echo "test_register_rejects_a_target_table_holding_a_different_metadata_location"
  refresh_token

  local ns="${RUN_ID}_stale" warehouse="${RUN_ID}_stale_wh"
  local submitted decoy log rc out

  assert_2xx "stale: source namespace created" "$(create_source_namespace "$ns")"
  assert_2xx "stale: source table alpha created" "$(create_source_table "$ns" "alpha")"
  assert_2xx "stale: source table decoy created" "$(create_source_table "$ns" "decoy")"
  submitted="$(source_metadata_location "$ns" "alpha")"
  decoy="$(source_metadata_location "$ns" "decoy")"

  assert_2xx "stale: source table decoy dropped without purge" "$(drop_source_table "$ns" "decoy")"
  assert_eq "stale: the source enumerates alpha alone" 1 "$(source_table_count "$ns")"

  prepare_target_for stale "$warehouse" "$ns"
  assert_2xx "stale: target alpha pre-registered at the WRONG metadata location" \
    "$(register_table "$TARGET_BASE" "$PROVISION_TARGET_NAMESPACE" "alpha" "$decoy")"

  log="$SANDBOX/provision-stale.log"
  run_provision "$ns" "$warehouse" "$log"; rc=$?
  out="$(cat "$log")"

  assert_rc_nonzero "stale: the run exits non-zero rather than reporting a complete registration" "$rc"
  assert_contains "stale: alpha is reported as a metadata-location mismatch" \
    "$out" "      alpha: FAILED, registered table's metadata-location does not match"
  assert_contains "stale: the mismatch is counted as failed, not as already present" \
    "$out" "Summary: 0 registered, 0 already present, 1 failed"
  assert_eq "stale: the target table still holds the pointer the script did not submit" \
    "$decoy" "$(registered_metadata_location "$TARGET_BASE" "$PROVISION_TARGET_NAMESPACE" "alpha")"
  if [[ "$submitted" != "$decoy" ]]; then
    pass "stale: the submitted and stale pointers really do differ"
  else
    fail "stale: the submitted and stale pointers really do differ" "the fixture is not exercising a mismatch"
  fi
}

# Register answers the same 409; the read-back answers 404 (the table was never created).
test_register_rejects_a_location_already_held_by_another_table() {
  echo "test_register_rejects_a_location_already_held_by_another_table"
  refresh_token

  local ns="${RUN_ID}_taken" warehouse="${RUN_ID}_taken_wh"
  local submitted log rc out

  assert_2xx "taken: source namespace created" "$(create_source_namespace "$ns")"
  assert_2xx "taken: source table beta created" "$(create_source_table "$ns" "beta")"
  submitted="$(source_metadata_location "$ns" "beta")"

  prepare_target_for taken "$warehouse" "$ns"
  assert_2xx "taken: beta's location claimed first under a different table name" \
    "$(register_table "$TARGET_BASE" "$PROVISION_TARGET_NAMESPACE" "beta_other" "$submitted")"

  log="$SANDBOX/provision-taken.log"
  run_provision "$ns" "$warehouse" "$log"; rc=$?
  out="$(cat "$log")"

  assert_rc_nonzero "taken: the run exits non-zero rather than reporting a complete registration" "$rc"
  assert_contains "taken: beta is reported as an unconfirmable read-back, naming the status" \
    "$out" "      beta: FAILED, confirming loadTable GET"
  assert_contains "taken: the read-back status is the 404 the catalog actually answered" \
    "$out" "returned HTTP 404"
  assert_contains "taken: the rejection is counted as failed, not as already present" \
    "$out" "Summary: 0 registered, 0 already present, 1 failed"
  assert_eq "taken: no table named beta was ever created in the target namespace" \
    "" "$(registered_metadata_location "$TARGET_BASE" "$PROVISION_TARGET_NAMESPACE" "beta")"
}

main() {
  require_stack

  echo "=================================================="
  echo "lakekeeper-provision.sh — local Docker integration"
  echo "  Lakekeeper       $TARGET_CATALOG"
  echo "  source catalog   $SOURCE_CATALOG"
  echo "  run id           $RUN_ID"
  echo "=================================================="

  test_bootstrap_and_warehouse_creation_are_idempotent
  test_register_table_by_reference_preserves_metadata_location
  test_register_rejects_a_target_table_holding_a_different_metadata_location
  test_register_rejects_a_location_already_held_by_another_table

  echo ""
  echo "=================================================="
  printf 'RESULT: %d passed, %d failed\n' "$PASS" "$FAIL"
  echo "=================================================="
  [[ "$FAIL" -eq 0 ]]
}

main "$@"
