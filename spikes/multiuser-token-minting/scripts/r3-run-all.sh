#!/usr/bin/env bash
# Round 3 end to end: teardown, bring-up, provision, then items 1-8 in order,
# transcripts into evidence/r3-*.txt. Tears down first so no run inherits state.
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=lib.sh
source "$HERE/lib.sh"
mkdir -p "$EVIDENCE"

if [ "${R3_FRESH:-1}" = "1" ]; then
  "$HERE/down.sh"
  "$HERE/up.sh"
  "$HERE/provision.sh" 2>&1 | tee "$EVIDENCE/r3-provision.txt"
fi

run() {  # $1 = script stem
  echo "### $1"
  "$HERE/r3-$1.sh" 2>&1 | tee "$EVIDENCE/r3-$1.txt"
}

run combined      # items 1, 2
run case          # item 3
run provisioning  # item 4
run rotation      # item 5
run kid-refetch   # item 5, addendum
run vending       # item 6
run negative      # item 7
run bucketfs      # item 8
run availability  # item 8, blast radius

"$HERE/r3-cleanup.sh"

echo
echo "round 3 transcripts:"
ls -la "$EVIDENCE"/r3-*.txt
