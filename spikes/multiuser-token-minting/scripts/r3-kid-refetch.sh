#!/usr/bin/env bash
# ROUND 3, item 5 (addendum) — the unknown-kid re-fetch, isolated.
#
# Phase 1 of r3-rotation showed a newly published key accepted seconds after
# publication, with no catalog restart; phase 3 showed a removed key still
# accepted. Together those say the key set IS cached and an unknown kid forces a
# re-fetch. This isolates the second half and asks the operational follow-up: is
# the re-fetch attempted on EVERY unknown kid?
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=r3-lib.sh
source "$HERE/r3-lib.sh"

K1="$SPIKE_DIR/keys/engine-signing-key.pem"
K2="$SPIKE_DIR/keys/engine-signing-key-2.pem"
export BFSPASS="$(bfs_password)"

say "3.5.7  Isolate the re-fetch"
write_docs "$R3_WORK/rot" "$R3_ISS" "$K1:lakehouse-engine-1" >/dev/null
publish_to_bucketfs "$R3_WORK/rot"
refetch_jwks
note "published kids: $(curl -sk "https://localhost:${LH_BUCKETFS_PORT:-22581}/$BFS_PATH/jwks.json" | jq -c '[.keys[].kid]')  (catalog just fetched them)"
note "kid-2, not published          -> HTTP $(r3_code "$("$MINT" --iss "$R3_ISS" --sub alice --key "$K2" --kid lakehouse-engine-2)" alice_table)"
write_docs "$R3_WORK/rot" "$R3_ISS" "$K1:lakehouse-engine-1" "$K2:lakehouse-engine-2" >/dev/null
publish_to_bucketfs "$R3_WORK/rot"
note "published kids: $(curl -sk "https://localhost:${LH_BUCKETFS_PORT:-22581}/$BFS_PATH/jwks.json" | jq -c '[.keys[].kid]')  (no catalog restart)"
note "kid-2, now published          -> HTTP $(r3_code "$("$MINT" --iss "$R3_ISS" --sub alice --key "$K2" --kid lakehouse-engine-2)" alice_table)"
note "kid-99, never published       -> HTTP $(r3_code "$("$MINT" --iss "$R3_ISS" --sub alice --key "$K2" --kid kid-99)" alice_table)"
note "-> a key published after the last fetch is accepted without a restart, so"
note "   the catalog re-reads the JWKS when it meets a kid it does not hold."

say "3.5.8  Is the re-fetch attempted on every unknown kid?"
# If each unknown kid costs a round trip to BucketFS, unknown-kid requests are
# measurably slower than known-kid ones. Ten of each.
time_n() {  # $1 = label, $2 = kid mode
  local t0 t1 i kid
  t0=$(date +%s.%N)
  for i in $(seq 10); do
    case "$2" in
      known)   kid=lakehouse-engine-1 ;;
      unknown) kid="never-published-$RANDOM-$i" ;;
    esac
    r3_code "$("$MINT" --iss "$R3_ISS" --sub alice --key "$K1" --kid "$kid")" alice_table >/dev/null
  done
  t1=$(date +%s.%N)
  printf '     %-26s %s s for 10 requests\n' "$1" "$(python3 -c "print(f'{$t1 - $t0:.2f}')")"
}
time_n "known kid (cached)"   known
time_n "unknown kid"          unknown
time_n "known kid (cached)"   known
note "the mint cost is identical in both rows, so the difference is the catalog's."

say "3.5.9  Restore"
write_docs "$R3_WORK/rot" "$R3_ISS" "$K1:lakehouse-engine-1" >/dev/null
publish_to_bucketfs "$R3_WORK/rot"
refetch_jwks
note "kid-1 -> HTTP $(r3_code "$("$MINT" --iss "$R3_ISS" --sub alice --key "$K1" --kid lakehouse-engine-1)" alice_table)"
