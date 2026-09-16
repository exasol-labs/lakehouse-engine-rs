#!/usr/bin/env bash
# ROUND 3, item 6 — per-user vended credentials in the `exasol~` namespace.
#
# Round 1 (option D) proved vending scopes to the table prefix for principals
# named engine~<uuid>. This re-runs it for exasol~alice / exasol~bob on the
# combined catalog, because the principal id is what Lakekeeper resolves the
# storage permission from.
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=r3-lib.sh
source "$HERE/r3-lib.sh"

MC_IMAGE="quay.io/minio/mc:RELEASE.2025-08-13T08-35-41Z"

say "3.6.0  Warehouse storage profile (sts-enabled is the precondition)"
curl -sS -H "Authorization: Bearer $(r3_admin)" "$R3_MGMT/warehouse" \
  | jq -c --arg w "$WAREHOUSE" '.warehouses[] | select(.name==$w) | ."storage-profile"
      | {bucket, "key-prefix", flavor, "sts-enabled", endpoint}' | sed 's/^/     /'

vend() { curl -sS "$R3_TBL/$2" -H "Authorization: Bearer $1" \
           -H 'X-Iceberg-Access-Delegation: vended-credentials'; }

say "3.6.1  Vend to exasol~alice and exasol~bob"
T_ALICE="$(r3_mint alice)"; T_BOB="$(r3_mint bob)"
A_RESP="$(vend "$T_ALICE" alice_table)"; B_RESP="$(vend "$T_BOB" bob_table)"
A_META="$(jq -r '."metadata-location"' <<<"$A_RESP")"
B_META="$(jq -r '."metadata-location"' <<<"$B_RESP")"
note "alice_table metadata: $A_META"
note "bob_table   metadata: $B_META"
jq -c '."storage-credentials"[0] | {prefix, expires: .config."expiration-time"}' <<<"$A_RESP" | sed 's/^/     alice cred: /'
jq -c '."storage-credentials"[0] | {prefix, expires: .config."expiration-time"}' <<<"$B_RESP" | sed 's/^/     bob   cred: /'
note '-> scoped to the TABLE prefix, same as round 1. The exasol~ namespace'
note "   changes nothing about the storage scoping."

cred() { jq -r --arg k "$2" '.config[$k]' <<<"$1"; }
A_CREDS="$(cred "$A_RESP" 's3.access-key-id'):$(cred "$A_RESP" 's3.secret-access-key'):$(cred "$A_RESP" 's3.session-token')"

mc_sh() {
  docker run --rm --network "$NET" -e "MC_HOST_v=http://$1@minio:9000" \
    --entrypoint /bin/sh "$MC_IMAGE" -c "$2" 2>&1 | tr -s '\n' | cut -c1-200
}
s3_cat() { mc_sh "$1" "mc cat 'v/${2#s3://}' >/dev/null 2>/tmp/e; echo \"exit=\$?\"; cat /tmp/e"; }

say "3.6.2  Storage layer, with ALICE's vended credentials"
note "alice creds -> alice_table metadata (expect success):"
s3_cat "$A_CREDS" "$A_META" | sed 's/^/     /'
note "alice creds -> bob_table metadata (MUST fail):"
s3_cat "$A_CREDS" "$B_META" | sed 's/^/     /'
note "alice creds -> list the whole warehouse prefix (MUST fail):"
mc_sh "$A_CREDS" "mc ls -r v/warehouse/spike/ >/dev/null 2>/tmp/e; echo \"exit=\$?\"; cat /tmp/e" | sed 's/^/     /'
note "alice creds -> list her own table prefix (expect success):"
mc_sh "$A_CREDS" "mc ls -r 'v/${A_META#s3://}' >/dev/null 2>/tmp/e; echo \"exit=\$?\"; cat /tmp/e" | sed 's/^/     /'

say "3.6.3  The catalog refuses first anyway"
note "loadTable bob_table as exasol~alice -> HTTP $(r3_code "$T_ALICE" bob_table)"
vend "$T_ALICE" bob_table | jq -c '{error: .error.type, message: .error.message}' | sed 's/^/     /'

say "3.6.4  Control — the warehouse's own static key is NOT scoped"
mc_sh "lakekeeper:lakekeeper-secret-key" \
  "mc cat 'v/${B_META#s3://}' >/dev/null 2>/tmp/e; echo \"warehouse static key -> bob_table exit=\$?\"; cat /tmp/e" | sed 's/^/     /'
note "-> the scoping comes from the per-request STS session, not the stored credential."
