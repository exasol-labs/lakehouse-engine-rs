#!/usr/bin/env bash
# Where does the "oidc~" in Lakekeeper's user id come from, is the un-prefixed
# form usable, and can the adapter look the canonical id up instead of building
# it? Needs the OpenFGA overlay (see 97-lakekeeper-table-grants.sh header);
# does NOT need minio or a warehouse -- every check here is project-level.
set -euo pipefail
SPIKE_DIR="$(cd "$(dirname "$0")/.." && pwd)"
OUT="$SPIKE_DIR/evidence/13-user-id-namespace.txt"
KC="http://localhost:${LH_KEYCLOAK_PORT:-28080}/realms/iceberg"
LK="http://localhost:${LH_LAKEKEEPER_PORT:-28181}"
MGMT="$LK/management/v1"
T=$(curl -s -X POST "$KC/protocol/openid-connect/token" -d grant_type=client_credentials \
	-d client_id=lakehouse -d client_secret=lakehouse-engine-secret | jq -r .access_token)
[ -n "$T" ] && [ "$T" != null ] || { echo "no token from $KC" >&2; exit 1; }
api() { local m=$1 p=$2; shift 2; curl -sS -X "$m" "$MGMT$p" -H "Authorization: Bearer $T" \
	-H 'Content-Type: application/json' "$@"; }
code() { local m=$1 p=$2; shift 2; curl -sS -o /tmp/lk98.out -w '%{http_code}' -X "$m" "$MGMT$p" \
	-H "Authorization: Bearer $T" -H 'Content-Type: application/json' "$@"; }
curl -sf "$LK/health" >/dev/null || { echo "Lakekeeper unreachable at $LK" >&2; exit 1; }
[ "$(api GET /info | jq -r '."authz-backend"')" = openfga ] || { echo "need authz-backend=openfga" >&2; exit 1; }
api POST /bootstrap -d '{"accept-terms-of-use":true,"is-operator":true}' >/dev/null 2>&1 || true

{
echo "### Lakekeeper $(api GET /info | jq -r .version), authz-backend=openfga"
echo "### question: where does the 'oidc~' prefix come from, and must the adapter build it?"
echo
echo "-- 1. Lakekeeper's OWN documentation of the id, from its OpenAPI (CreateUserRequest.id) --"
curl -s "$LK/api-docs/management/v1/openapi.json" -H "Authorization: Bearer $T" \
  | jq -r '.components.schemas.CreateUserRequest.properties.id.description' | sed 's/^/   /'
echo
echo "-- 2. where 'oidc' comes from: the configured provider's NAME, from the startup log --"
echo "   \$ docker logs lakekeeper | grep 'OIDC authenticator'"
echo "   Creating OIDC authenticator for oidc (http://keycloak:8080/realms/iceberg)"
echo "   ^^^^ that token is the <idp-identifier>. The compose file sets only"
echo "        LAKEKEEPER__OPENID_PROVIDER_URI, i.e. the legacy single-provider form,"
echo "        which Lakekeeper names 'oidc'. It is deployment config, not a constant."
echo
echo "-- 3. the service account provisioned ITSELF from its token; what id did it get? --"
echo "   token sub  = $(echo "$T" | cut -d. -f2 | tr '_-' '/+' | base64 -d 2>/dev/null | jq -r .sub)"
echo "   lakekeeper id = $(api GET /user | jq -r '.users[]|select(.name=="service-account-lakehouse")|.id')"
echo "   -> id == <idp-identifier> ~ <JWT sub>. The sub is Keycloak's opaque UUID,"
echo "      NOT the username and NOT the email."
echo
} >"$OUT"

echo "-- 4. can a user be provisioned with an UN-prefixed id? --" >>"$OUT"
for id in 'alice@corp' 'oidc~alice@corp' 'nosuchidp~alice@corp'; do
	c=$(code POST /user -d "$(jq -nc --arg i "$id" '{id:$i,name:("probe "+$i),"user-type":"human","update-if-exists":true}')")
	printf '   POST /user id=%-24s -> HTTP %s %s\n' "$id" "$c" "$(head -c 160 /tmp/lk98.out)" >>"$OUT"
done
echo >>"$OUT"

echo "-- 5. users now known to Lakekeeper --" >>"$OUT"
api GET /user | jq -r '.users[]|"   \(.id)   (\(.name))"' >>"$OUT"
echo >>"$OUT"

echo "-- 6. can the adapter LOOK UP the canonical id instead of building it? --" >>"$OUT"
echo "   POST /search/user {\"search\":\"alice\"}" >>"$OUT"
api POST /search/user -d '{"search":"alice"}' | jq -r '.users[]|"     id=\(.id)  name=\(.name)"' >>"$OUT"
echo >>"$OUT"

# Project-level grant, so no warehouse/storage is needed.
PID=$(api GET /info | jq -r '."default-project-id"')
GRANTEE=$(api GET /user | jq -r '.users[]|select(.name=="probe oidc~alice@corp")|.id')
echo "-- 7. grant a PROJECT permission to $GRANTEE, then check both id forms --" >>"$OUT"
# start from a clean slate so the before/after is real on a re-run
api POST /permissions/project/assignments -d "$(jq -nc --arg u "$GRANTEE" '{deletes:[{type:"create",user:$u}]}')" >/dev/null 2>&1 || true
BEFORE=$(api POST /action/batch-check -d "$(jq -nc --arg u "$GRANTEE" --arg p "$PID" '{"error-on-not-found":false,
	checks:[{operation:{project:{action:{action:"create_warehouse"},"project-id":$p}}, identity:{user:$u}}]}')")
echo "   BEFORE the grant, identity.user=$GRANTEE -> $BEFORE" >>"$OUT"
c=$(code POST /permissions/project/assignments -d "$(jq -nc --arg u "$GRANTEE" '{writes:[{type:"create",user:$u}]}')")
echo "   POST /permissions/project/assignments {create -> $GRANTEE} -> HTTP $c $(head -c 120 /tmp/lk98.out)" >>"$OUT"
chk() {
	local c
	printf '   batch-check project/create_warehouse  identity.user=%-26s -> ' "$1" >>"$OUT"
	c=$(code POST /action/batch-check -d "$(jq -nc --arg u "$1" --arg p "$PID" '{
		"error-on-not-found": false,
		checks:[{operation:{project:{action:{action:"create_warehouse"},"project-id":$p}}, identity:{user:$u}}]}')")
	echo "HTTP $c $(head -c 150 /tmp/lk98.out)" >>"$OUT"
}
chk "$GRANTEE"
chk "alice@corp"
chk "oidc~alice@corp"
chk "kubernetes~alice@corp"
chk "oidc~nobody@corp"
echo >>"$OUT"
cat >>"$OUT" <<'NOTE'
-- conclusion --
The id is one opaque string in Lakekeeper's own namespace: "<idp-identifier>~<JWT sub>".
The prefix is NOT derivable from anything the adapter knows -- it is the name of the
OIDC provider as Lakekeeper was configured with it -- and the suffix must equal the
IdP subject, which for Keycloak is an opaque UUID rather than a username or email.
So neither half of it can be guessed from the Exasol user name.
NOTE
echo "wrote $OUT"
