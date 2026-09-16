#!/usr/bin/env bash
# Full reproduction, from nothing to every captured transcript.
#
#   spikes/multiuser-token-minting/scripts/run-all.sh
#
# Tears the stack down first, so a run never inherits state from the last one.
# Round 2's BucketFS and per-user-CONNECTION probes additionally need the repo's
# main stack (docker compose up -d exasol from the repo root); they say so and
# skip if it is absent.
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=lib.sh
source "$HERE/lib.sh"

"$HERE/down.sh" >/dev/null 2>&1 || true
"$HERE/up.sh"
"$HERE/provision.sh" | tee "$EVIDENCE/provision.txt"
bash "$HERE/probe-additional-issuers.sh" 2>&1 | tee "$EVIDENCE/probe-additional-issuers.txt"

# Round 1: which minting mechanism works at all.
for opt in a b b-control c d; do
  bash "$HERE/option-$opt.sh" 2>&1 | tee "$EVIDENCE/option-$opt.txt"
done

# Round 2: can the winning mechanism actually be operated by a customer.
for r2 in barrier2-roles-claim hosting merged-jwks idp-key-registration \
          provider-inversion subject-resolution per-user-connections \
          static-key-upstream; do
  bash "$HERE/r2-$r2.sh" 2>&1 | tee "$EVIDENCE/r2-$r2.txt"
done

say "All transcripts written to $EVIDENCE"
