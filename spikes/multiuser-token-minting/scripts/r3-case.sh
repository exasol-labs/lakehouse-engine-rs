#!/usr/bin/env bash
# ROUND 3, item 3 — case.
#
# Exasol folds unquoted identifiers to upper case, so ctx.current_user() returns
# ALICE for `CREATE USER alice`. The admin, meanwhile, types the principal string
# by hand. So: are `exasol~ALICE` and `exasol~alice` one principal or two, and do
# the grant API and the token path agree about it?
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=r3-lib.sh
source "$HERE/r3-lib.sh"

ADMIN="$(r3_admin)"
ASSIGN="$R3_MGMT/permissions/warehouse/$WH_ID/table/$ALICE_TBL/assignments"

show_assignments() {
  curl -sS -H "Authorization: Bearer $ADMIN" "$ASSIGN" \
    | jq -c '[.assignments[] | select(.user != null) | .user]' | sed 's/^/     /'
}
grant_user()  { req_code "$ADMIN" POST "$ASSIGN" --data "$(jq -n --arg u "$1" '{writes:[{type:"select",user:$u}],deletes:[]}')"; }
revoke_user() { req_code "$ADMIN" POST "$ASSIGN" --data "$(jq -n --arg u "$1" '{writes:[],deletes:[{type:"select",user:$u}]}')"; }

say "3.3.0  Starting point — only exasol~alice (lower case) is granted"
show_assignments

say "3.3.1  Token path: does the catalog fold the subject claim?"
for s in alice ALICE Alice; do
  t="$(r3_mint "$s")"
  id="$(curl -sS -H "Authorization: Bearer $t" "$R3_MGMT/whoami" | jq -r '.error.message // .id')"
  printf '     sub=%-6s -> alice_table HTTP %s   catalog says: %s\n' \
    "$s" "$(r3_code "$t" alice_table)" "$id"
done
note "-> the subject is taken verbatim. ALICE is a different principal from alice."

say "3.3.2  Grant API: does IT fold?"
note "grant select to exasol~ALICE -> HTTP $(grant_user 'exasol~ALICE')"
note "assignments now:"
show_assignments
note "-> two separate entries. The grant API stores the string verbatim too,"
note "   so the grant API and the token path agree: both are case-SENSITIVE."

say "3.3.3  With exasol~ALICE granted, the upper-case token works"
for s in alice ALICE; do
  printf '     sub=%-6s -> alice_table HTTP %s   bob_table HTTP %s\n' \
    "$s" "$(r3_code "$(r3_mint "$s")" alice_table)" "$(r3_code "$(r3_mint "$s")" bob_table)"
done

say "3.3.4  They are genuinely two principals — revoke one, the other survives"
note "revoke exasol~ALICE -> HTTP $(revoke_user 'exasol~ALICE')"
for s in alice ALICE; do
  printf '     sub=%-6s -> alice_table HTTP %s\n' "$s" "$(r3_code "$(r3_mint "$s")" alice_table)"
done
show_assignments

say "3.3.5  The user directory does not merge them either"
for id in "exasol~alice" "exasol~ALICE"; do
  note "POST /management/v1/user id=$id -> HTTP $(req_code "$ADMIN" POST "$R3_MGMT/user" \
    --data "$(jq -n --arg i "$id" '{id:$i, name:"case probe", "user-type":"human", "update-if-exists":true}')")"
done
note "GET /management/v1/user?name=case%20probe:"
curl -sS -H "Authorization: Bearer $ADMIN" "$R3_MGMT/user?name=case+probe" \
  | jq -c '[.users[] | .id]' | sed 's/^/     /'
note "-> two distinct user records with the same display name."

say "3.3.6  What the admin sees when they type the other case"
note "the failure is a 404 on the table, not an authentication error:"
req "$(r3_mint ALICE)" GET "$R3_TBL/alice_table" | head -6 | sed 's/^/     /'
note "-> indistinguishable from 'the table does not exist'. A case mismatch is a"
note "   silent authorization miss, which is exactly why the adapter must normalise."
