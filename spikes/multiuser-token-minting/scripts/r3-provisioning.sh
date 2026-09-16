#!/usr/bin/env bash
# ROUND 3, item 4 — principal provisioning.
#
# The design asks the admin to grant to `exasol~alice`. Can they do that on day
# one, BEFORE alice has ever run a query? Two routes are tested: a plain grant to
# a principal that does not exist, and pre-creating it with POST
# /management/v1/user. Uses fresh names (carol, dave) so nothing inherits state
# from items 1-3.
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=r3-lib.sh
source "$HERE/r3-lib.sh"

ADMIN="$(r3_admin)"

say "3.4.0  A dedicated table, and two principals that have never been seen"
CODE=$(req_code "$ADMIN" POST "$R3_TBL" --data "$(jq -n '{
  name: "carol_table", "stage-create": false,
  schema: {type:"struct","schema-id":0,"identifier-field-ids":[],
           fields:[{id:1,name:"id",required:true,type:"long"}]}}')")
note "create carol_table -> HTTP $CODE"
CAROL_TBL="$(curl -sS -H "Authorization: Bearer $ADMIN" "$R3_TBL/carol_table" | jq -r '.metadata."table-uuid"')"
note "carol_table $CAROL_TBL"
note "user directory before anything: $(curl -sS -H "Authorization: Bearer $ADMIN" "$R3_MGMT/user?name=carol" | jq -c '[.users[].id]')"
note "whoami as exasol~carol:"
body "$(r3_mint carol)" GET "$R3_MGMT/whoami" | jq -c '{code: (.error.code // 200), message: (.error.message // .id)}' | sed 's/^/     /'

say "3.4.1  Route 1 — grant straight to a principal that does not exist"
note "grant select on carol_table to exasol~carol -> HTTP $(req_code "$ADMIN" POST \
  "$R3_MGMT/permissions/warehouse/$WH_ID/table/$CAROL_TBL/assignments" \
  --data '{"writes":[{"type":"select","user":"exasol~carol"}],"deletes":[]}')"
note "the assignment persisted:"
curl -sS -H "Authorization: Bearer $ADMIN" \
  "$R3_MGMT/permissions/warehouse/$WH_ID/table/$CAROL_TBL/assignments" \
  | jq -c '[.assignments[] | {type, who: (.user // .role)}]' | sed 's/^/     /'

say "3.4.2  Carol's first ever query"
T_CAROL="$(r3_mint carol)"
note "carol_table -> HTTP $(r3_code "$T_CAROL" carol_table)   alice_table -> HTTP $(r3_code "$T_CAROL" alice_table)"
note "listTables -> $(curl -sS -H "Authorization: Bearer $T_CAROL" "$R3_TBL" | jq -c '.identifiers')"
note "-> authorization worked on the FIRST query, with no user record anywhere."

say "3.4.3  Did the query create a user record?"
note "user directory after the query: $(curl -sS -H "Authorization: Bearer $ADMIN" "$R3_MGMT/user?name=carol" | jq -c '[.users[].id]')"
note "whoami as carol, after querying:"
body "$T_CAROL" GET "$R3_MGMT/whoami" | jq -c '{code: (.error.code // 200), message: (.error.message // .id)}' | sed 's/^/     /'
note "-> no. Catalog data-plane calls do NOT self-register. The principal is"
note "   authorized and invisible in the user list at the same time."

say "3.4.4  Route 2 — pre-create the principal"
note "POST /management/v1/user id=exasol~dave -> HTTP $(req_code "$ADMIN" POST "$R3_MGMT/user" \
  --data '{"id":"exasol~dave","name":"Dave (Exasol)","user-type":"human","update-if-exists":true}')"
note "the record:"
curl -sS -H "Authorization: Bearer $ADMIN" "$R3_MGMT/user?name=Dave" \
  | jq -c '[.users[] | {id, name, "last-updated-with"}]' | sed 's/^/     /'
note "grant select on carol_table to exasol~dave -> HTTP $(req_code "$ADMIN" POST \
  "$R3_MGMT/permissions/warehouse/$WH_ID/table/$CAROL_TBL/assignments" \
  --data '{"writes":[{"type":"select","user":"exasol~dave"}],"deletes":[]}')"
T_DAVE="$(r3_mint dave)"
note "dave: carol_table -> HTTP $(r3_code "$T_DAVE" carol_table)   alice_table -> HTTP $(r3_code "$T_DAVE" alice_table)"
note "whoami as dave:"
body "$T_DAVE" GET "$R3_MGMT/whoami" | jq -c '{id, name, "user-type"}' | sed 's/^/     /'

say "3.4.5  The difference the two routes actually make"
note "both authorize identically. The only difference is visibility:"
printf '     %-34s %s\n' "exasol~carol (grant only)" "$(curl -sS -H "Authorization: Bearer $ADMIN" "$R3_MGMT/user?name=carol" | jq -c '[.users[].id]')"
printf '     %-34s %s\n' "exasol~dave  (pre-created)" "$(curl -sS -H "Authorization: Bearer $ADMIN" "$R3_MGMT/user?name=Dave" | jq -c '[.users[].id]')"
note "a pre-created principal is searchable by display name and answers whoami;"
note "a grant-only principal is neither, yet is fully authorized."
