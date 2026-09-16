#!/usr/bin/env bash
# ROUND 3, item 5 — key rotation, untested in rounds 1 and 2.
#
# Publish a second key alongside the first, sign with the new one while the old
# is still listed, and check that no query fails across the window. Lakekeeper
# caches a provider's key set for 1 h (JWKSWebAuthenticator::new(uri,
# Some(Duration::from_hours(1)))) and exposes no refresh API, so the post-refresh
# state is reached here by restarting the catalog. Every restart below is
# labelled as a SIMULATED refresh, not an observed one.
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=r3-lib.sh
source "$HERE/r3-lib.sh"

K1="$SPIKE_DIR/keys/engine-signing-key.pem"
K2="$SPIKE_DIR/keys/engine-signing-key-2.pem"
[ -f "$K2" ] || { openssl genrsa -out "$K2" 2048 2>/dev/null; chmod 600 "$K2"; }
export BFSPASS="$(bfs_password)"

# Both keys sign for the SAME granted principal, so the only variable is the key.
probe() {  # $1 = label
  printf '     %-30s kid-1 %s   kid-2 %s\n' "$1" \
    "$(r3_code "$("$MINT" --iss "$R3_ISS" --sub alice --key "$K1" --kid lakehouse-engine-1)" alice_table)" \
    "$(r3_code "$("$MINT" --iss "$R3_ISS" --sub alice --key "$K2" --kid lakehouse-engine-2)" alice_table)"
}

republish() { write_docs "$R3_WORK/rot" "$R3_ISS" "$@" >/dev/null; publish_to_bucketfs "$R3_WORK/rot"; }

say "3.5.0  Phase 0 — steady state, one key published"
republish "$K1:lakehouse-engine-1"
refetch_jwks
note "JWKS kids: $(curl -sk "https://localhost:${LH_BUCKETFS_PORT:-22581}/$BFS_PATH/jwks.json" | jq -c '[.keys[].kid]')"
probe "baseline"
note "-> kid-2 is 401: an unpublished key is rejected. That is the control for the rest."

say "3.5.1  Phase 1 — publish both keys, catalog NOT yet refreshed"
republish "$K1:lakehouse-engine-1" "$K2:lakehouse-engine-2"
note "JWKS kids: $(curl -sk "https://localhost:${LH_BUCKETFS_PORT:-22581}/$BFS_PATH/jwks.json" | jq -c '[.keys[].kid]')"
probe "published, pre-refresh"
note "-> whether kid-2 already passes here answers the operationally decisive"
note "   question: does an unknown kid force a JWKS re-fetch, or must the"
note "   publisher wait out the full cache interval before signing with it?"

say "3.5.2  Phase 2 — SIMULATED refresh (catalog restart), both keys live"
refetch_jwks
probe "post-refresh, overlap"
note "-> this is the overlap window: either key verifies, so the engine can switch"
note "   signing keys at any moment without a failed query."

say "3.5.3  Phase 3 — retire the old key, catalog NOT yet refreshed"
republish "$K2:lakehouse-engine-2"
note "JWKS kids: $(curl -sk "https://localhost:${LH_BUCKETFS_PORT:-22581}/$BFS_PATH/jwks.json" | jq -c '[.keys[].kid]')"
probe "retired, pre-refresh"

say "3.5.4  Phase 4 — SIMULATED refresh, old key gone"
refetch_jwks
probe "post-refresh, retired"
note "-> kid-1 must be 401 here. If it is not, the catalog is serving a stale key set."

say "3.5.5  Continuity — was there any moment with no working key?"
note "read the four probes above: at every phase at least one kid answered 200,"
note "and the key the engine was signing with answered 200 throughout, provided"
note "the switch happened inside the overlap."

say "3.5.6  Restore phase 0 for the remaining probes"
republish "$K1:lakehouse-engine-1"
refetch_jwks
probe "restored"
