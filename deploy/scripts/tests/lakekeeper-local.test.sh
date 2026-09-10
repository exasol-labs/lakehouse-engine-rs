#!/usr/bin/env bash
# Local Docker integration verification for deploy/scripts/lakekeeper-provision.sh.
#
#   docker compose -f docker-compose.yml -f docker-compose.lakekeeper.yml up -d --wait \
#     minio iceberg-rest keycloak lakekeeper-db lakekeeper-migrate lakekeeper
#   bash deploy/scripts/tests/lakekeeper-local.test.sh
#
# This harness drives a real Lakekeeper 0.13.1 over HTTP. It NEVER runs against AWS: every URL it
# builds is a localhost port published by the compose stack, and it FAILS rather than skips when
# the stack is unavailable, so a green result always means the flow was exercised end to end.
#
# Two checks, named for the scenarios they close:
#
#   test_bootstrap_and_warehouse_creation_are_idempotent
#     Runs the provisioning script TWICE over one throwaway source namespace and asserts the
#     second run creates nothing — server already bootstrapped, warehouse already present, every
#     table already registered — while the first run created all three.
#
#   test_register_table_by_reference_preserves_metadata_location
#     Registers three throwaway table pairs by reference and asserts each registered table still
#     points at the source metadata.json: a non-colliding pair as the positive control, and a
#     part/partsupp-shaped pair whose locations differ only by a non-slash-delimited suffix, in
#     BOTH registration orders. Only the forward order was ever observed live, so a single-order
#     check could not detect a Lakekeeper version that rejects the reverse one.
#
#   test_register_outcome_is_confirmed_by_metadata_location_read_back
#     Drives the two register rejections that Lakekeeper 0.13.1 reports with a BYTE-IDENTICAL
#     `409 AlreadyExistsException` body, and asserts the script fails each one instead of
#     reporting it as an already-registered success. Both shapes are pre-built against the target
#     catalog before the script runs, so the script meets exactly the state a real gap produces.
#
# WHY THE STACK'S ICEBERG REST FIXTURE CREATES THE THROWAWAY TABLES, NOT LAKEKEEPER.
# Lakekeeper rejects a warehouse whose key prefix contains, or is contained by, an existing
# warehouse's — confirmed live on 0.13.1 as HTTP 400 CreateWarehouseStorageProfileOverlap. A
# Lakekeeper-hosted source warehouse would therefore sit over the exact prefix the target
# warehouse needs, and no target warehouse could be created at all. Creating the data through
# Lakekeeper would also invert the ordering under test: on AWS the TPC-H objects exist long before
# any Lakekeeper warehouse does, so every case here populates the key prefix FIRST and creates the
# warehouse SECOND. The fixture is the producer the plan's live spike used and it derives
# s3://warehouse/<namespace>/<table>, which is the production location shape. It ignores the
# bearer token it is sent, which is what lets the provisioning script's OAuth2 `rest` source
# producer read it unchanged.
#
# WHY EVERY THROWAWAY TABLE IS DROPPED BEFORE IT IS REGISTERED.
# With the source entry gone, no live table in any catalog on the stack holds the location, so a
# colliding pair that registers cleanly is evidence about Lakekeeper's location rule rather than
# about exact-location reuse — and a register that still succeeds proves the operation reads the
# physical metadata.json by reference. That distinction is not academic on 0.13.1: a location
# conflict and a name conflict are both reported as HTTP 409 AlreadyExistsException with the same
# message, so an undropped source table would turn a real rejection into a silent "already
# registered". `drop_source_table` is the only destructive call in this plan's deliverables, it
# exists only in this file, and its purge switch is a literal false.
#
# This run's warehouses, namespaces and objects are deliberately left behind — nothing is purged.
# `docker compose -f docker-compose.yml -f docker-compose.lakekeeper.yml down -v` clears them.

set -uo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROVISION="$HERE/../lakekeeper-provision.sh"

# --- Stack coordinates ---------------------------------------------------------------------------
#
# Ports follow docker-compose.yml / docker-compose.lakekeeper.yml's own env-override convention, so
# a suite that had to pick free ports can point this harness at the same ones. Every other value
# below is fixed by the compose files and their header comments; keep the two in sync.

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
# MinIO as the LAKEKEEPER CONTAINER reaches it — a docker-network name, not a published port. This
# is the value Lakekeeper stores in the warehouse's storage profile and dials itself.
WAREHOUSE_S3_ENDPOINT="http://minio:9000"
WAREHOUSE_ACCESS_KEY_ID="minioadmin"
WAREHOUSE_SECRET_ACCESS_KEY="minioadmin"

# Every artifact this run creates carries this stem, so repeated runs never collide on a warehouse
# name and never derive an overlapping key prefix.
RUN_ID="lktest_$(date +%Y%m%d%H%M%S)_$$"

# --- Assertions ------------------------------------------------------------------------------------

PASS=0
FAIL=0

pass() { PASS=$((PASS + 1)); printf '  ok   %s\n' "$1"; }
fail() { FAIL=$((FAIL + 1)); printf '  FAIL %s\n' "$1"; [[ -n "${2:-}" ]] && printf '       %s\n' "$2"; }

assert_eq()          { if [[ "$2" == "$3" ]]; then pass "$1"; else fail "$1" "expected [$2] got [$3]"; fi; }
assert_contains()    { if [[ "$2" == *"$3"* ]]; then pass "$1"; else fail "$1" "missing [$3]"; fi; }
assert_rc_zero()     { if [[ "$2" -eq 0 ]]; then pass "$1"; else fail "$1" "expected rc 0 got $2"; fi; }
assert_rc_nonzero()  { if [[ "$2" -ne 0 ]]; then pass "$1"; else fail "$1" "expected a non-zero rc, got 0"; fi; }

# A 2xx family check for the calls whose exact success code is the server's choice.
assert_2xx() {
  case "$2" in
    2??) pass "$1" ;;
    *) fail "$1" "expected 2xx got $2 $(response_error)" ;;
  esac
}

# The server-authored error type and message only. The raw body is never echoed: the warehouse
# request carries a storage secret and an error response can quote its request back.
response_error() {
  jq -r 'if .error then "(\(.error.type // "?"): \(.error.message // "?"))" else "" end' \
    "$BODY_FILE" 2>/dev/null || printf ''
}

# --- Sandbox ------------------------------------------------------------------------------------

SANDBOX="$(mktemp -d)"
trap 'rm -rf "$SANDBOX"' EXIT
BODY_FILE="$SANDBOX/response.json"

# --- Pre-flight ----------------------------------------------------------------------------------
#
# Fail loudly and specifically. A missing stack is the single most likely reason this file is run
# and gets an unexpected result, so each probe names the URL that did not answer and the command
# that brings the stack up, rather than letting the first assertion fail with an HTTP 000.

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
  # The realm's discovery document, not Keycloak's own liveness: a 2xx here proves realm import
  # finished, which is what makes the client-credentials grant below answerable.
  require_endpoint "Keycloak realm 'iceberg'" "$KEYCLOAK_REALM_URI/.well-known/openid-configuration"
  require_endpoint "Lakekeeper" "http://localhost:$LAKEKEEPER_PORT/health"
  require_endpoint "source Iceberg REST catalog" "$SOURCE_CATALOG/v1/config"
}

# --- Catalog plumbing ------------------------------------------------------------------------------
#
# One bearer token serves both catalogs. Lakekeeper validates it against the host-published issuer
# the compose file adds to LAKEKEEPER__OPENID_ADDITIONAL_ISSUERS; the source fixture is
# unauthenticated and ignores the header, which is exactly what lets the provisioning script's
# OAuth2 `rest` producer read it with no special case.

TOKEN=""

refresh_token() {
  TOKEN="$(curl -sf --request POST "$TOKEN_URI" \
    -d grant_type=client_credentials \
    -d "client_id=$OAUTH_CLIENT_ID" \
    -d "client_secret=$OAUTH_CLIENT_SECRET" | jq -r '.access_token // empty')"
  [ -n "$TOKEN" ] || fatal "Keycloak client-credentials grant at $TOKEN_URI returned no access token"
}

# Each of these prints the HTTP status on stdout and leaves the response body in $BODY_FILE.
# curl writes its --write-out template on every exit path, printing 000 when the request never
# reached a server, so no fallback is layered on top: a second one would print 000 twice and turn
# an unreachable stack into an unreadable status. The body file is truncated first, because curl
# leaves a previous call's body in place when it never connects.

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

# THE one destructive call in this plan's deliverables. purgeRequested is a literal false in the
# program text rather than an omitted or computed value: a purge would delete the very objects the
# subsequent register-by-reference has to find, and no variant of this call may ever gain the
# ability to purge. It only ever names a namespace this run created moments earlier.
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

# The fixture derives the table location from the namespace and table name, so the pair of names a
# caller passes IS the location shape under test. The schema is the smallest legal Iceberg struct:
# nothing here reads a row, only the metadata document's location.
create_source_table() {
  api_post "$SOURCE_CATALOG/v1/namespaces/$1/tables" \
    "$(jq -n --arg name "$2" '{
         name: $name,
         schema: {type: "struct", "schema-id": 0,
                  fields: [{id: 1, name: "id", required: true, type: "long"}]}
       }')"
}

# Prints the table's current metadata-location. Empty when the load failed, which the caller's
# assertion then reports.
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

# Lakekeeper serves the Iceberg REST surface under a per-warehouse prefix advertised by the config
# document. Reading `overrides` then `defaults` mirrors both the Iceberg REST merge order and what
# the provisioning script does, so this harness resolves the prefix the same way the code under
# test does rather than hard-coding where 0.13.1 happens to publish it.
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

# `overwrite` is a literal false, matching the provisioning script: a register that would replace a
# recorded metadata pointer is never what this harness means to assert.
register_table() {
  api_post "$1/namespaces/$2/register" \
    "$(jq -n --arg name "$3" --arg metadata_location "$4" \
        '{name: $name, "metadata-location": $metadata_location, overwrite: false}')"
}

registered_metadata_location() {
  api_get "$1/namespaces/$2/tables/$3" >/dev/null
  jq -r '."metadata-location" // empty' "$BODY_FILE" 2>/dev/null || printf ''
}

# --- Check 1: idempotent provisioning ---------------------------------------------------------------

PROVISION_SOURCE_NAMESPACE="${RUN_ID}_src"
PROVISION_WAREHOUSE="${RUN_ID}_wh"
PROVISION_TARGET_NAMESPACE="tpch"

# Runs the script under test over the named source namespace and target warehouse, writes its
# combined output to the named file, and returns the script's own exit status. Check 1's source set
# carries a non-colliding table AND the part/partsupp pair, because on AWS the script registers
# exactly that shape out of Glue in one run.
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

  # No warehouse exists over s3://$WAREHOUSE_BUCKET/$ns at this point and the three tables' objects
  # already do: data first, warehouse second, the AWS ordering.
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

# --- Check 2: register by reference ----------------------------------------------------------------

# Creates both named tables in a throwaway source namespace, records their metadata locations,
# drops both without purging, then creates a Lakekeeper warehouse over their now-populated key
# prefix and registers them in the order the arguments give. Every register must answer 2xx: on
# 0.13.1 a rejected location comes back as 409, indistinguishable by body from an already-present
# name, so accepting 409 here would make the whole check unfalsifiable.
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

  # Positive control: two locations neither of which is a prefix of the other. It fails only if the
  # harness itself is broken, which is what makes a colliding-pair failure attributable.
  register_pair_by_reference control orders customer

  # s3://.../part is a byte-wise prefix of s3://.../partsupp with no slash at the boundary — the
  # TPC-H shape an upstream issue reports as LocationAlreadyTaken. Both orders, because the live
  # spike only ever observed the forward one.
  register_pair_by_reference collide_fwd part partsupp
  register_pair_by_reference collide_rev partsupp part
}

# --- Check 3: the register outcome rests on a read-back, not on the response text --------------------
#
# Lakekeeper 0.13.1 answers BOTH of the register rejections below with the same body, captured live:
#
#   409 {"error":{"message":"Tabular with the same name already exists in the namespace",
#                 "type":"AlreadyExistsException","code":409, ...}}
#
# The words "location" and "taken" appear in neither, so response-text matching cannot separate a
# genuine registration gap from an ordinary already-registered re-run, and the script folded both
# into already-registered success until the confirming read-back replaced that classification.
#
# Each case below pre-builds the exact target state a real gap produces and then runs the script
# against it, rather than asserting the classification through a stub: the whole reason the earlier
# text-matching classification survived review is that it looked right and only a live 0.13.1
# disproved it.
#
# Both cases must FAIL the run. A green result here means a partial registration can no longer be
# mistaken for a complete one.

# Prepares one target warehouse over the given already-populated key prefix, with the target
# namespace created, so the script under test meets an already-present warehouse and namespace
# exactly as it does on a re-run. Sets TARGET_BASE rather than printing it: this function also
# asserts, and an assertion writes to stdout, so a printed return value would come back to the
# caller with every `ok` line glued to the front of the URL.
TARGET_BASE=""
prepare_target_for() {
  local label="$1" warehouse="$2" key_prefix="$3"
  assert_2xx "$label: warehouse created over the already-populated key prefix" \
    "$(create_target_warehouse "$warehouse" "$key_prefix")"
  TARGET_BASE="$(target_catalog_base "$warehouse")"
  assert_2xx "$label: target namespace created" \
    "$(create_target_namespace "$TARGET_BASE" "$PROVISION_TARGET_NAMESPACE")"
}

# CASE A — the registered name is present but points somewhere else. The source table's metadata
# pointer moved (a new snapshot on AWS; a different table's document here) while Lakekeeper still
# holds the old one. Register answers 409, and the read-back answers 200 with the STALE pointer.
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

  # Dropping decoy without purging leaves its metadata document in place but takes it out of the
  # source enumeration, so the script submits alpha's pointer and only alpha's.
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
  # The submitted pointer never landed: the run reported a gap instead of masking one.
  if [[ "$submitted" != "$decoy" ]]; then
    pass "stale: the submitted and stale pointers really do differ"
  else
    fail "stale: the submitted and stale pointers really do differ" "the fixture is not exercising a mismatch"
  fi
}

# CASE B — the location is already held by a DIFFERENT table name, the shape decision [29] was
# written for. Register answers the same 409, but the read-back answers 404: the table the script
# tried to register does not exist in the catalog at all.
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

# --- Run -------------------------------------------------------------------------------------------

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
