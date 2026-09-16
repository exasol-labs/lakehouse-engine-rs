#!/usr/bin/env bash
# Round 2, the alternative to minting: give each user their OWN Exasol
# CONNECTION object holding their own catalog credential. No engine-minted
# tokens, no engine-hosted JWKS, no IdP key import.
#
# Two questions, both measured live:
#   1. Does Exasol's privilege model actually isolate one user's CONNECTION
#      from another's, inside a UDF?
#   2. What catalog identity does the credential in that CONNECTION present?
#      (If it is a per-user OAuth client, barrier 2 comes straight back.)
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=lib.sh
source "$HERE/lib.sh"
source "$SPIKE_DIR/.spike-state"

EXA_DSN="${EXA_DSN:-exasol://sys:exasol@localhost:${LH_EXASOL_PORT:-28563}?validateservercertificate=0}"
sql() { exapump sql -d "$EXA_DSN" "$1" >/dev/null 2>&1; }
# Run the probe as ALICE_EXA and report what Exasol said, pass or fail.
as_alice() {
  local out
  out="$(exapump sql -d "$A_DSN" "SELECT SPIKE.CONNPROBE('$1');" 2>&1 || true)"
  { grep -o "OK user=[A-Z_]*\|insufficient privileges[^(]*" <<<"$out" || echo "$out"; } \
    | head -1 | sed 's/^/     /'
}
kc_admin() {
  curl -sS -X POST "$KC/realms/master/protocol/openid-connect/token" \
    -d grant_type=password -d client_id=admin-cli \
    -d username=admin -d password=admin | jq -r '.access_token'
}

say "C.1  Exasol: which grant actually lets a UDF read a CONNECTION?"
if ! docker ps --filter name=exasol --format '{{.Names}}' | grep -q .; then
  note "SKIPPED: the repo's main stack (docker-compose.yml) is not running."
else
sql "DROP SCHEMA IF EXISTS SPIKE CASCADE;" || true
sql "CREATE SCHEMA SPIKE;"
sql "CREATE OR REPLACE LUA SCALAR SCRIPT SPIKE.CONNPROBE(cname VARCHAR(128)) RETURNS VARCHAR(2000) AS
function run(ctx)
  local ok, c = pcall(exa.get_connection, ctx.cname)
  if ok then return 'OK user=' .. tostring(c.user) else return 'DENIED ' .. tostring(c) end
end"
for u in ALICE_EXA BOB_EXA; do
  sql "DROP USER IF EXISTS $u CASCADE;" || true
  sql "CREATE USER $u IDENTIFIED BY \"pw_$u\";"
  sql "GRANT CREATE SESSION TO $u;"
  sql "GRANT EXECUTE ON SCRIPT SPIKE.CONNPROBE TO $u;"
  sql "CREATE OR REPLACE CONNECTION LH_CAT_$u TO 'http://lakekeeper:8181/catalog' USER '$u' IDENTIFIED BY 'token-for-$u';"
done

note "grant 1: GRANT CONNECTION LH_CAT_ALICE_EXA TO ALICE_EXA"
sql "GRANT CONNECTION LH_CAT_ALICE_EXA TO ALICE_EXA;"
A_DSN="exasol://ALICE_EXA:pw_ALICE_EXA@localhost:${LH_EXASOL_PORT:-28563}?validateservercertificate=0"
as_alice LH_CAT_ALICE_EXA

note "grant 2: ... WITH ADMIN OPTION"
sql "GRANT CONNECTION LH_CAT_ALICE_EXA TO ALICE_EXA WITH ADMIN OPTION;"
as_alice LH_CAT_ALICE_EXA

note "grant 3: GRANT ACCESS ON CONNECTION ... FOR SCRIPT ... TO ..."
sql "GRANT ACCESS ON CONNECTION LH_CAT_ALICE_EXA FOR SCRIPT SPIKE.CONNPROBE TO ALICE_EXA;"
as_alice LH_CAT_ALICE_EXA

note "cross-user: ALICE_EXA reaching for BOB_EXA's connection"
sql "GRANT ACCESS ON CONNECTION LH_CAT_BOB_EXA FOR SCRIPT SPIKE.CONNPROBE TO BOB_EXA;"
as_alice LH_CAT_BOB_EXA
cat <<'TXT'
   Measured: only the third form works. Plain GRANT CONNECTION and WITH ADMIN
   OPTION both leave exa.get_connection denied inside the UDF. The cross-user
   attempt is correctly refused ("insufficient privileges for using connection
   LH_CAT_BOB_EXA in script CONNPROBE", SQL state 22001).
   So Exasol DOES isolate per-user credentials properly — the mechanism is sound.
   The cost is the DDL: per user, 1 CONNECTION + 1 GRANT ACCESS ... FOR SCRIPT
   per scan script, redone whenever the credential rotates.
TXT
fi

say "C.2  What identity does a per-user credential present to the catalog?"
ADM="$(kc_admin)"

note "Variant (i): a per-user OAuth CLIENT (client_credentials in the CONNECTION)"
curl -sS -X POST -H "Authorization: Bearer $ADM" -H 'Content-Type: application/json' \
  "$KC/admin/realms/$REALM/clients" --data '{
    "clientId":"lh-user-alice","enabled":true,"protocol":"openid-connect",
    "publicClient":false,"serviceAccountsEnabled":true,
    "standardFlowEnabled":false,"secret":"alice-client-secret",
    "protocolMappers":[{"name":"lakekeeper-audience","protocol":"openid-connect",
      "protocolMapper":"oidc-audience-mapper","consentRequired":false,
      "config":{"included.custom.audience":"lakekeeper","access.token.claim":"true",
                "id.token.claim":"false","introspection.token.claim":"true"}}]}' >/dev/null 2>&1 || true
T_CLIENT=$(curl -sS -X POST "$KC_TOKEN_URL" -d grant_type=client_credentials \
  -d client_id=lh-user-alice -d client_secret=alice-client-secret | jq -r '.access_token')
if [ "$T_CLIENT" != "null" ] && [ -n "$T_CLIENT" ]; then
  note "sub in the token   : $(decode_jwt "$T_CLIENT" | jq -r '.claims.sub' 2>/dev/null || echo '(see transcript)')"
  note "alice's real sub   : $ALICE_SUB"
  note "whoami             : $(curl -sS -H "Authorization: Bearer $T_CLIENT" "$MGMT/whoami" | jq -c '{id,name} // .error.message')"
  note "loadTable alice_table -> HTTP $(req_code "$T_CLIENT" GET "$PREFIX/namespaces/$NAMESPACE/tables/alice_table")"
  note "The token is valid (same audience as any other catalog client) but its"
  note "sub is that of the SERVICE ACCOUNT, so the catalog sees a principal it has"
  note "never heard of: whoami 404s and the table read 404s. Barrier 2 returns in"
  note "full — the customer must re-grant every table to a new machine identity"
  note "per user. 0 of the existing grants apply."
fi

note ""
note "Variant (ii): the user's OWN credentials (ROPC) in the CONNECTION"
T_ROPC=$(user_token alice alice)
note "whoami             : $(curl -sS -H "Authorization: Bearer $T_ROPC" "$MGMT/whoami" | jq -c '{id,name}')"
note "loadTable alice_table -> HTTP $(req_code "$T_ROPC" GET "$PREFIX/namespaces/$NAMESPACE/tables/alice_table")   (expect 200)"
note "loadTable bob_table   -> HTTP $(req_code "$T_ROPC" GET "$PREFIX/namespaces/$NAMESPACE/tables/bob_table")   (expect 404)"
cat <<'TXT'
   Variant (ii) is the only per-user-CONNECTION shape that preserves existing
   grants: the token carries the human's own sub, so `oidc~<sub>` matches what
   the customer granted in the UI. But the CONNECTION then stores the user's IdP
   PASSWORD, and the resource-owner-password grant is deprecated by OAuth 2.1,
   off by default in Entra ID and Okta, and incompatible with MFA/SSO — which is
   usually the whole reason the customer runs an IdP.
TXT

say "C.3  Onboarding steps for per-user CONNECTIONs"
cat <<'TXT'
   Per user, variant (i) — per-user OAuth client:
     1. create an OAuth client / service account in the IdP
     2. create the Exasol CONNECTION with its client id + secret
     3. GRANT ACCESS ON CONNECTION ... FOR SCRIPT ... TO <user>
     4. re-grant, in Lakekeeper, every table the human already has, to the new
        machine principal
     (+ repeat 2 on every secret rotation)
     => 4 steps per user, and it violates criterion 2 (existing grants change).
   Per user, variant (ii) — the user's own password:
     1. create the Exasol CONNECTION holding the user's IdP password
     2. GRANT ACCESS ON CONNECTION ... FOR SCRIPT ... TO <user>
     (+ redo 1 on every password change)
     => 2 steps per user, grants unchanged, but it requires a deprecated grant
        type and defeats SSO/MFA.
TXT

say "C.4  Cleanup"
# This probe writes into the repo's MAIN Exasol stack, which the E2E suite also
# uses. Leave nothing behind.
if docker ps --filter name=exasol --format '{{.Names}}' | grep -q .; then
  for u in ALICE_EXA BOB_EXA; do
    sql "DROP CONNECTION IF EXISTS LH_CAT_$u;" || true
    sql "DROP USER IF EXISTS $u CASCADE;" || true
  done
  sql "DROP SCHEMA IF EXISTS SPIKE CASCADE;" || true
  note "dropped SPIKE schema, ALICE_EXA/BOB_EXA and their CONNECTION objects"
fi
