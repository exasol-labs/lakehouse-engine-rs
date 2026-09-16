#!/usr/bin/env bash
# ROUND 3, items 1 and 2 — A+1a as ONE configuration, and a non-UUID subject.
#
# Round 1 tested Option A against an nginx issuer; round 2 tested BucketFS
# hosting as the PRIMARY provider. Neither ran the combination the design
# actually ships: the customer's Keycloak as the primary `oidc` provider AND the
# engine as a secondary provider `exasol` served from BucketFS.
#
# Item 2 then replaces the UUID subject every previous round used with the
# Exasol user name itself. `sub = alice` -> principal `exasol~alice` is the
# load-bearing assumption of the whole design.
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=r3-lib.sh
source "$HERE/r3-lib.sh"

say "3.1.0  Publish the engine's issuer documents to BucketFS"
extract_chain_root
note "BucketFS TLS chain (leaf then root; both CA:TRUE, same subject):"
for f in "$R3_WORK"/chain/cert-*.pem; do
  printf '     %s\n' "$(openssl x509 -in "$f" -noout -subject -ext subjectAltName 2>/dev/null | tr '\n' ' ')"
done
export BFSPASS="$(bfs_password)"
write_docs "$R3_WORK/docs" "$R3_ISS" "$SPIKE_DIR/keys/engine-signing-key.pem:lakehouse-engine-1"
publish_to_bucketfs "$R3_WORK/docs"
note "published to bfs://$BFS_PATH  (issuer $R3_ISS)"
note "anonymous read back through the BucketFS HTTPS port:"
curl -skI "https://localhost:${LH_BUCKETFS_PORT:-22581}/$BFS_PATH/jwks.json" \
  | sed -n '1p;/[Cc]ontent-[Tt]ype/p' | sed 's/^/     /'
curl -sk "https://localhost:${LH_BUCKETFS_PORT:-22581}/$BFS_PATH/.well-known/openid-configuration" \
  | jq -c '{issuer, jwks_uri}' | sed 's/^/     /'

say "3.1.1  The combined configuration — both providers on one catalog"
cat <<CFG
     LAKEKEEPER__OPENID_PROVIDER_URI=$KC_URI            # the customer's IdP, id \`oidc\`
     LAKEKEEPER__OPENID_AUDIENCE=lakekeeper
     LAKEKEEPER__OPENID_SUBJECT_CLAIM=sub
     LAKEKEEPER__OPENID_PROVIDERS__EXASOL__URI=$R3_ISS
     LAKEKEEPER__OPENID_PROVIDERS__EXASOL__AUDIENCE=lakekeeper
     LAKEKEEPER__OPENID_PROVIDERS__EXASOL__SUBJECT_CLAIMS=sub
     SSL_CERT_FILE=<the BucketFS chain ROOT>
CFG
note "no ADDITIONAL_ISSUERS anywhere: this is the configuration a customer runs."
start_combined
note "startup log:"
authenticators

say "3.1.2  A genuine Keycloak token, against this catalog"
T_KC="$(user_token_net alice alice)"
note "whoami -> $(r3_whoami "$T_KC" | jq -c '{id, name}')"
matrix "keycloak alice" "$T_KC"
T_KC_BOB="$(user_token_net bob bob)"
matrix "keycloak bob" "$T_KC_BOB"

say "3.1.3  An engine-minted token, same catalog, before any exasol~ grant"
T_UUID="$(r3_mint "$ALICE_SUB")"
note "sub = the Keycloak UUID (what rounds 1 and 2 used):"
req "$T_UUID" GET "$R3_MGMT/whoami" | sed 's/^/     /'
matrix "exasol-minted (uuid sub)" "$T_UUID"
note "-> authenticated (not 401), but it is a DIFFERENT principal from oidc~<uuid>."
note "   That separate namespace is the design, not a defect: the admin grants to it on purpose."

say "3.2.1  Item 2 — a non-UUID subject"
ADMIN="$(r3_admin)"
T_ALICE="$(r3_mint alice)"
note "minted claims:"
decode_jwt "$T_ALICE" | jq -c '{header: .header, iss: .claims.iss, sub: .claims.sub, aud: .claims.aud}' | sed 's/^/     /'
note "the principal id the catalog derives, read back from the catalog itself:"
curl -sS -H "Authorization: Bearer $T_ALICE" "$R3_MGMT/whoami" \
  | jq -r '"     " + (.error.message // ("resolved id " + .id))'

say "3.2.2  Grant to exasol~alice and exasol~bob"
for pair in "$ALICE_TBL:exasol~alice" "$BOB_TBL:exasol~bob"; do
  tbl="${pair%%:*}"; usr="${pair#*:}"
  note "grant select on ${tbl} to $usr -> HTTP $(req_code "$ADMIN" POST \
    "$R3_MGMT/permissions/warehouse/$WH_ID/table/$tbl/assignments" \
    --data "$(jq -n --arg u "$usr" '{writes:[{type:"select",user:$u}],deletes:[]}')")"
done
note "assignments on alice_table now:"
curl -sS -H "Authorization: Bearer $ADMIN" \
  "$R3_MGMT/permissions/warehouse/$WH_ID/table/$ALICE_TBL/assignments" \
  | jq -c '[.assignments[] | {type, who: (.user // .role)}]' | sed 's/^/     /'

say "3.2.3  Authorization behaves for exasol~alice and exasol~bob"
T_BOB="$(r3_mint bob)"
matrix "exasol~alice" "$T_ALICE"
matrix "exasol~bob"   "$T_BOB"
note "listTables is filtered, not merely table-GET:"
for who in alice bob; do
  case "$who" in alice) T="$T_ALICE";; bob) T="$T_BOB";; esac
  note "  exasol~$who -> $(curl -sS -H "Authorization: Bearer $T" "$R3_TBL" | jq -c '.identifiers')"
done

say "3.2.4  Both token sources served simultaneously by the same catalog"
note "interleaved, same catalog process, no restart between calls:"
for i in 1 2; do
  matrix "keycloak alice (round $i)" "$(user_token_net alice alice)"
  matrix "exasol~alice (round $i)"   "$(r3_mint alice)"
done
note "-> A+1a is one working configuration. Genuine IdP tokens and engine-minted"
note "   tokens authenticate side by side against the same catalog."
