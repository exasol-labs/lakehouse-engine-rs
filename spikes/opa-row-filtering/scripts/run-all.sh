#!/usr/bin/env bash
# Reproduce every piece of evidence in evidence/.
set -euo pipefail
D="$(cd "$(dirname "$0")" && pwd)"
"$D/../.bin/opa" check "$D/../policies" 2>/dev/null || "$D/10-capture-row-filters.sh" >/dev/null
bash "$D/10-capture-row-filters.sh"
bash "$D/20-capture-column-masks.sh"
bash "$D/30-failure-modes.sh"
bash "$D/40-latency.sh"
bash "$D/50-translate-probe.sh" >/dev/null
echo
echo "Exasol identity check (needs a live container):"
bash "$D/60-exasol-identity.sh" || echo "  SKIPPED/FAILED -- see script header"
