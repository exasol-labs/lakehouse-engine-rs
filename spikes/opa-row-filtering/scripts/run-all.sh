#!/usr/bin/env bash
# Reproduce every piece of evidence in evidence/.
# Scripts needing a live container say so in their own header and are listed
# separately at the bottom; the OPA-only ones run unattended.
set -euo pipefail
D="$(cd "$(dirname "$0")" && pwd)"
bash "$D/10-capture-row-filters.sh"
bash "$D/20-capture-column-masks.sh"
bash "$D/30-failure-modes.sh"
bash "$D/40-latency.sh"
bash "$D/50-translate-probe.sh" >/dev/null
bash "$D/70-partial-evaluation.sh"
bash "$D/95-per-user-data.sh"
bash "$D/96-table-gate.sh"
bash "$D/85-ucast-target.sh"
bash "$D/80-compile-probe.sh" >/dev/null   # consumes compiled/ from 70, 90, 95, 96
echo
echo "Needs a live Exasol container (docker compose up -d exasol):"
bash "$D/60-exasol-identity.sh" || echo "  FAILED -- see script header"
echo
echo "Needs a live Keycloak (see script header):"
bash "$D/90-idp-auth.sh" || echo "  FAILED -- see script header"
echo
echo "PARKED (README section 13) -- needs Lakekeeper + the OpenFGA overlay:"
bash "$D/97-lakekeeper-table-grants.sh" || echo "  FAILED -- see script header"
bash "$D/98-user-id-namespace.sh" || echo "  FAILED -- see script header"
