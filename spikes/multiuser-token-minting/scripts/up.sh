#!/usr/bin/env bash
# Bring the spike stack up from nothing: keys -> images -> services -> ready.
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=lib.sh
source "$HERE/lib.sh"

"$HERE/gen-keys.sh"

say "Starting spike stack"
"${COMPOSE[@]}" up -d --wait \
  minio engine-issuer keycloak openfga-db openfga lakekeeper-db lakekeeper
# One-shot jobs are not in the --wait set (a clean exit is not "healthy").
"${COMPOSE[@]}" up -d minio-init
docker wait "$("${COMPOSE[@]}" ps -q minio-init)" >/dev/null

say "Ready"
note "Keycloak     $KC        (realm $REALM, admin/admin)"
note "Lakekeeper   $LK"
note "OpenFGA      http://localhost:${SPK_OPENFGA_HTTP_PORT:-38082}"
note "MinIO        $MINIO      (minioadmin/minioadmin)"
note "Engine issuer http://localhost:$SPK_ISSUER_PORT"
