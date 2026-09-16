#!/usr/bin/env bash
# Round 2, candidate 2a: can the engine assert the user's ROLES in the minted
# token, so existing role-based grants apply even though the principal id is
# `engine~<sub>` rather than `oidc~<sub>`?
#
# Requires LAKEKEEPER__OPENID_ROLES_CLAIM=roles and
# LAKEKEEPER__OPENID_PROVIDERS__ENGINE__ROLES_CLAIM=roles (set in the compose file).
#
# Result: NO. Measured three ways, ending with the structural reason.
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=lib.sh
source "$HERE/lib.sh"
source "$SPIKE_DIR/.spike-state"

TOKEN="$(engine_token)"

say "2a.1  A Lakekeeper role that holds select on bob_table"
ROLE_ID=$(curl -sS -H "Authorization: Bearer $TOKEN" "$MGMT/role" \
  | jq -r '.roles[] | select(.name=="customer-analysts") | .id')
if [ -z "$ROLE_ID" ]; then
  ROLE_ID=$(curl -sS -X POST "$MGMT/role" -H "Authorization: Bearer $TOKEN" \
    -H 'Content-Type: application/json' \
    --data '{"name":"customer-analysts","description":"role the customer already grants in the UI"}' \
    | jq -r '.id')
fi
note "role id $ROLE_ID"
note "NOTE: a Lakekeeper role id is a bare UUID, not <provider>~<name>."
req "$TOKEN" POST "$MGMT/permissions/warehouse/$WH_ID/table/$BOB_TBL/assignments" \
  --data "$(jq -n --arg r "$ROLE_ID" '{writes:[{type:"select",role:$r}],deletes:[]}')" >/dev/null || true
note "granted select on bob_table to role customer-analysts"

say "2a.2  Engine-minted token for ALICE asserting roles: [customer-analysts]"
T_MINT=$(engine_mint "$ALICE_SUB" --claim 'roles=["customer-analysts"]')
decode_jwt "$T_MINT"
note "listTables (bob's namespace view) -> HTTP $(req_code "$T_MINT" GET "$PREFIX/namespaces/$NAMESPACE/tables")"
note "loadTable bob_table            -> HTTP $(req_code "$T_MINT" GET "$PREFIX/namespaces/$NAMESPACE/tables/bob_table")"
note "If the roles claim carried authorization this would be 200."

say "2a.3  Control: a GENUINE Keycloak token carrying the same roles claim"
# Rules out 'the engine provider is special'. alice logs in through Keycloak for
# real; a protocol mapper puts roles:["customer-analysts"] in the token.
T_KC=$(user_token alice alice)
note "genuine-token loadTable bob_table -> HTTP $(req_code "$T_KC" GET "$PREFIX/namespaces/$NAMESPACE/tables/bob_table")"
note "The same code. Token roles are not an authorization input for ANY provider,"
note "so this is not a quirk of the engine-minted provider."

say "2a.4  Control: direct role MEMBERSHIP does work"
req "$TOKEN" POST "$MGMT/permissions/role/$ROLE_ID/assignments" \
  --data "$(jq -n --arg u "engine~$ALICE_SUB" '{writes:[{type:"assignee",user:$u}],deletes:[]}')" >/dev/null || true
note "after assigning engine~alice to the role, loadTable bob_table -> HTTP $(req_code "$T_MINT" GET "$PREFIX/namespaces/$NAMESPACE/tables/bob_table")"
note "200 — so the grant itself is real; only the TOKEN-asserted path is inert."
# Undo, so the rest of the suite sees the documented fixture grants.
req "$TOKEN" POST "$MGMT/permissions/role/$ROLE_ID/assignments" \
  --data "$(jq -n --arg u "engine~$ALICE_SUB" '{writes:[],deletes:[{type:"assignee",user:$u}]}')" >/dev/null || true

say "2a.5  Structural cause (source, v0.13.1 and v0.13.5)"
cat <<'TXT'
   authn.rs         extract_and_set_token_roles() parses ROLES_CLAIM and calls
                    RequestMetadata::set_token_roles().
   request_metadata RequestMetadata::token_roles() is the only reader — and it
                    has no caller anywhere outside its own module
                    (`grep -rn '\.token_roles()' crates/` returns nothing).
   authn.rs:654     Actor::Role is built ONLY from the x-assume-role-id request
                    header, never from a token claim.
   admission.rs     "admission is deliberately a distinct layer from
                    authorization" — the roles claim feeds admission, not authz.
   Verdict: ROLES_CLAIM cannot carry authorization. 2a is dead, not merely
   awkward, and no configuration change revives it.
TXT
