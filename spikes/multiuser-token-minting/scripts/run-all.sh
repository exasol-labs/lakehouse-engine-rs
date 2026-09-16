#!/usr/bin/env bash
# Full reproduction, from nothing to every captured transcript.
#
#   spikes/multiuser-token-minting/scripts/run-all.sh
#
# Tears the stack down first, so a run never inherits state from the last one.
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=lib.sh
source "$HERE/lib.sh"

"$HERE/down.sh" >/dev/null 2>&1 || true
"$HERE/up.sh"
"$HERE/provision.sh" | tee "$EVIDENCE/provision.txt"
bash "$HERE/probe-additional-issuers.sh" 2>&1 | tee "$EVIDENCE/probe-additional-issuers.txt"
for opt in a b b-control c d; do
  bash "$HERE/option-$opt.sh" 2>&1 | tee "$EVIDENCE/option-$opt.txt"
done
say "All transcripts written to $EVIDENCE"
