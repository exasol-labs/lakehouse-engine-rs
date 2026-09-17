#!/usr/bin/env bash
# PARKED -- see README section 13. OPA answers the table question itself (an
# ungranted table makes the residual {} = DENY), so this Lakekeeper path is not
# part of the feature; it is kept as evidence for a possible separate,
# table-only alternative for a deployment without OPA.
#
# Question: if Lakekeeper grants alice SELECT on one table but not another, can
# OPA enforce that? This exercises the exact API call Lakekeeper's own OPA bridge
# makes (authz/opa-bridge/policies/lakekeeper/check.rego
# require_table_access_simple -> POST /management/v1/action/batch-check), so the
# result here is the result the bridge would get.
#
# REQUIRES the OpenFGA authz backend. The repo's plain lakekeeper compose runs
# authz-backend=allow-all, which has NO permission model: /permissions/** is 404
# and batch-check returns allowed:true for everyone, including a user that does
# not exist. Bring the stack up with the spike's overlay:
#
#   docker compose -f docker-compose.yml -f docker-compose.lakekeeper.yml \
#     -f spikes/opa-row-filtering/docker-compose.openfga.yml \
#     up -d minio minio-init minio-lakekeeper-init keycloak openfga \
#            lakekeeper-db lakekeeper-migrate lakekeeper
#
# Fails (never skips) when unreachable, per the project's E2E contract.
set -euo pipefail
SPIKE_DIR="$(cd "$(dirname "$0")/.." && pwd)"
OUT="$SPIKE_DIR/evidence/12-lakekeeper-table-grants.txt"
KC="http://localhost:${LH_KEYCLOAK_PORT:-28080}/realms/iceberg"
LK="http://localhost:${LH_LAKEKEEPER_PORT:-28181}"
MGMT="$LK/management/v1"
WH=opa_spike_wh
NS=sales

T=$(curl -s -X POST "$KC/protocol/openid-connect/token" \
	-d grant_type=client_credentials -d client_id=lakehouse \
	-d client_secret=lakehouse-engine-secret | jq -r .access_token)
[ -n "$T" ] && [ "$T" != null ] || { echo "no token from $KC" >&2; exit 1; }
api() { local m=$1 p=$2; shift 2; curl -sS -X "$m" "$MGMT$p" -H "Authorization: Bearer $T" \
	-H 'Content-Type: application/json' "$@"; }
curl -sf "$LK/health" >/dev/null || { echo "Lakekeeper unreachable at $LK" >&2; exit 1; }

BACKEND=$(api GET /info | jq -r '."authz-backend"')
{
	echo "### Lakekeeper $(api GET /info | jq -r .version), authz-backend = $BACKEND"
	echo "### caller = the engine's own service account (client_credentials, client_id=lakehouse)"
	echo "### API under test: POST /management/v1/action/batch-check"
	echo "###   the SAME call Lakekeeper's OPA bridge makes in check.rego"
	echo
} >"$OUT"
[ "$BACKEND" = openfga ] || { echo "authz-backend is '$BACKEND', need 'openfga' -- see header of this script" | tee -a "$OUT" >&2; exit 1; }

echo "-- setup --" >>"$OUT"
api POST /bootstrap -d '{"accept-terms-of-use":true,"is-operator":true}' >/dev/null 2>&1 || true
api POST /warehouse -d "$(jq -nc --arg wh "$WH" '{
  "warehouse-name": $wh,
  "storage-profile": {"type":"s3","bucket":"warehouse","endpoint":"http://minio:9000",
    "region":"local-01","path-style-access":true,"flavor":"s3-compat",
    "sts-enabled":false,"key-prefix":$wh},
  "storage-credential": {"type":"s3","credential-type":"access-key",
    "aws-access-key-id":"minioadmin","aws-secret-access-key":"minioadmin"},
  "delete-profile": {"type":"hard"}}')" >/dev/null 2>&1 || true
WID=$(api GET /warehouse | jq -r --arg wh "$WH" '.warehouses[]|select(.name==$wh)|.id')
[ -n "$WID" ] || { echo "warehouse $WH not created" >&2; exit 1; }
echo "   warehouse $WH id=$WID" >>"$OUT"

# Two real tables via the Iceberg REST API, so Lakekeeper owns real table ids.
IRC="$LK/catalog/v1/$WID"
curl -sS -X POST "$IRC/namespaces" -H "Authorization: Bearer $T" -H 'Content-Type: application/json' \
	-d "{\"namespace\":[\"$NS\"]}" >/dev/null 2>&1 || true
for t in orders_public orders_secret; do
	curl -sS -X POST "$IRC/namespaces/$NS/tables" -H "Authorization: Bearer $T" \
		-H 'Content-Type: application/json' -d "$(jq -nc --arg n "$t" '{
		name: $n,
		schema: {type:"struct", "schema-id":0, fields:[
			{id:1,name:"id",required:true,type:"long"},
			{id:2,name:"region",required:false,type:"string"}]}}')" >/dev/null 2>&1 || true
done
echo "   tables: $(curl -s "$IRC/namespaces/$NS/tables" -H "Authorization: Bearer $T" | jq -c '[.identifiers[].name]')" >>"$OUT"

# Lakekeeper's table id == the Iceberg table-uuid it hands back on loadTable.
tid() { curl -s "$IRC/namespaces/$NS/tables/$1" -H "Authorization: Bearer $T" \
	| jq -r '.metadata."table-uuid"'; }
PUB_ID=$(tid orders_public); SEC_ID=$(tid orders_secret)
echo "   orders_public id=$PUB_ID" >>"$OUT"
echo "   orders_secret id=$SEC_ID" >>"$OUT"

# Provision alice. Lakekeeper keys users by IdP subject, prefixed "oidc~".
ALICE="oidc~opa-spike-alice"
api POST /user -d "$(jq -nc --arg id "$ALICE" '{id:$id, name:"Alice Spike", "user-type":"human", email:"alice@example.com", "update-if-exists":true}')" >/dev/null 2>&1 || true
AU=$(api GET /user | jq -r --arg n "Alice Spike" '.users[]|select(.name==$n)|.id' | head -1)
echo "   lakekeeper user id for alice = ${AU:-<not provisioned>}" >>"$OUT"
AU=${AU:-$ALICE}

{
	echo
	echo "-- GRANT alice 'select' on orders_public ONLY (nothing at all on orders_secret) --"
} >>"$OUT"
for tbl_id in "$PUB_ID"; do
	code=$(api POST "/permissions/warehouse/$WID/table/$tbl_id/assignments" \
		-o /tmp/lk-grant.out -w '%{http_code}' \
		-d "$(jq -nc --arg u "$AU" '{writes:[{type:"select", user:$u}]}')")
	# 409 TupleAlreadyExistsError just means a previous run already granted it.
	echo "   POST /permissions/warehouse/$WID/table/$tbl_id/assignments {select -> alice} -> HTTP $code $(head -c 120 /tmp/lk-grant.out)" >>"$OUT"
done
echo "   assignments now on orders_public: $(api GET "/permissions/warehouse/$WID/table/$PUB_ID/assignments" | jq -c '.assignments')" >>"$OUT"
echo "   assignments now on orders_secret: $(api GET "/permissions/warehouse/$WID/table/$SEC_ID/assignments" | jq -c '.assignments')" >>"$OUT"

check() { # check <table-name> <user> <action>
	local body
	body=$(jq -nc --arg w "$WID" --arg ns "$NS" --arg t "$1" --arg u "$2" --arg a "$3" '{
		"error-on-not-found": false,
		checks: [{operation: {table: {action: {action: $a}, "warehouse-id": $w,
			namespace: [$ns], table: $t}}, identity: {user: $u}}]}')
	printf '  %-22s %-13s %-28s -> ' "$1" "$3" "$2" >>"$OUT"
	api POST /action/batch-check -d "$body" | jq -c '.results[0] // .' >>"$OUT"
}

{
	echo
	echo "-- ASK batch-check ON ALICE'S BEHALF (caller is the service account, subject is alice) --"
} >>"$OUT"
check orders_public "$AU" get_metadata
check orders_public "$AU" read_data
check orders_secret "$AU" get_metadata
check orders_secret "$AU" read_data

{
	echo
	echo "-- NEGATIVE CONTROLS --"
} >>"$OUT"
check orders_public "oidc~no-such-user-at-all" read_data
check orders_public "$AU" write_data
check orders_nonexistent "$AU" read_data

echo "wrote $OUT"
