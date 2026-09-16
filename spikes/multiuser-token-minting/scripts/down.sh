#!/usr/bin/env bash
# Tear the spike stack down, volumes included — every script re-provisions from
# scratch, so a stale volume is only a source of false results.
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=lib.sh
source "$HERE/lib.sh"
"${COMPOSE[@]}" --profile legacy down -v --remove-orphans
