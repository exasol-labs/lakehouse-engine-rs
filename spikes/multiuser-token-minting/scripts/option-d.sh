#!/usr/bin/env bash
# OPTION D — per-user vended credentials.
#
# With a user-scoped token and an STS-enabled warehouse, are the vended S3
# credentials scoped to that user? Concretely: take alice's vended credentials
# and try to read bob's table's Parquet/metadata objects straight from MinIO.
# It must fail.
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=lib.sh
source "$HERE/lib.sh"
source "$SPIKE_DIR/.spike-state"

TBL_URL="$PREFIX/namespaces/$NAMESPACE/tables"
NET="spike-multiuser-token-minting"
MC_IMAGE="quay.io/minio/mc:RELEASE.2025-08-13T08-35-41Z"

say "D.0  Warehouse storage profile"
curl -sS -H "Authorization: Bearer $(engine_token)" "$MGMT/warehouse" \
  | jq -c --arg w "$WAREHOUSE" '.warehouses[] | select(.name==$w) | ."storage-profile"
      | {bucket, "key-prefix", flavor, "sts-enabled", endpoint, "path-style-access"}' | sed 's/^/   /'

# loadTable with credential vending, as a given user.
vend() {
  curl -sS "$TBL_URL/$2" -H "Authorization: Bearer $1" \
    -H 'X-Iceberg-Access-Delegation: vended-credentials'
}

say "D.1  Vend credentials to alice for alice_table"
T_ALICE="$(engine_mint "$ALICE_SUB")"
T_BOB="$(engine_mint "$BOB_SUB")"
A_RESP="$(vend "$T_ALICE" alice_table)"
B_RESP="$(vend "$T_BOB" bob_table)"

A_META="$(jq -r '."metadata-location"' <<<"$A_RESP")"
B_META="$(jq -r '."metadata-location"' <<<"$B_RESP")"
note "alice_table metadata: $A_META"
note "bob_table   metadata: $B_META"
jq -c '."storage-credentials"[0] | {prefix, expires: .config."expiration-time"}' <<<"$A_RESP" | sed 's/^/   alice cred: /'
jq -c '."storage-credentials"[0] | {prefix, expires: .config."expiration-time"}' <<<"$B_RESP" | sed 's/^/   bob   cred: /'
note "-> the vended credential is scoped to the TABLE prefix, not to the warehouse."

cred() { jq -r --arg k "$2" '.config[$k]' <<<"$1"; }
A_AK="$(cred "$A_RESP" 's3.access-key-id')"; A_SK="$(cred "$A_RESP" 's3.secret-access-key')"; A_ST="$(cred "$A_RESP" 's3.session-token')"

say "D.2  Read objects from MinIO directly with ALICE's vended credentials"
# mc takes a session token as the third field of the MC_HOST_ URL.
# The mc image has no sed/head, so the error file is dumped with cat.
mc_sh() {
  local creds="$1" script="$2"
  docker run --rm --network "$NET" -e "MC_HOST_v=http://$creds@minio:9000" \
    --entrypoint /bin/sh "$MC_IMAGE" -c "$script" 2>&1 | tr -s '\n' | cut -c1-200
}
s3_cat() {
  local creds="$1" uri="$2"
  mc_sh "$creds" "mc cat 'v/${uri#s3://}' >/dev/null 2>/tmp/e; echo \"exit=\$?\"; cat /tmp/e"
}
A_CREDS="$A_AK:$A_SK:$A_ST"
note "alice creds -> alice_table metadata (expect success):"
s3_cat "$A_CREDS" "$A_META" | sed 's/^/     /'
note "alice creds -> bob_table metadata (MUST fail):"
s3_cat "$A_CREDS" "$B_META" | sed 's/^/     /'
note "alice creds -> list the whole warehouse prefix (MUST fail):"
mc_sh "$A_CREDS" "mc ls -r v/warehouse/spike/ >/dev/null 2>/tmp/e; echo \"exit=\$?\"; cat /tmp/e" | sed 's/^/     /'
note "alice creds -> list her own table prefix (expect success):"
mc_sh "$A_CREDS" "mc ls -r 'v/${A_META#s3://}' >/dev/null 2>/tmp/e; echo \"exit=\$?\"; cat /tmp/e" | sed 's/^/     /'

say "D.3  The catalog layer already refuses bob's table to alice"
note "loadTable bob_table as alice -> HTTP $(req_code "$T_ALICE" GET "$TBL_URL/bob_table")"
note "so alice cannot even obtain a credential for it:"
vend "$T_ALICE" bob_table | jq -c '{error: .error.type, message: .error.message}' | sed 's/^/     /'

say "D.4  Control — the warehouse's own static key is NOT scoped"
note "minioadmin (root) and the lakekeeper STS user both reach every table:"
mc_sh "lakekeeper:lakekeeper-secret-key" \
  "mc cat 'v/${B_META#s3://}' >/dev/null 2>/tmp/e; echo \"warehouse static key -> bob_table exit=\$?\"; cat /tmp/e" | sed 's/^/     /'
note "-> the scoping comes from the STS session Lakekeeper mints per request,"
note "   not from the warehouse credential itself."
