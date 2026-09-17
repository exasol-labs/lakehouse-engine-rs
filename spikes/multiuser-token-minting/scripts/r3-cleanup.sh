#!/usr/bin/env bash
# Remove everything round 3 left outside its own compose stack: the throwaway
# catalogs, the proxy, and the issuer documents published into the repo's MAIN
# Exasol container's BucketFS.
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=r3-lib.sh
source "$HERE/r3-lib.sh"

docker rm -f lk-r3 lk-r3-avail lk-r3-probe lk-r3-req bfs-proxy >/dev/null 2>&1 || true
note "removed throwaway containers"
unpublish_from_bucketfs
note "removed bfs://$BFS_PATH/{jwks.json,.well-known/openid-configuration}"
