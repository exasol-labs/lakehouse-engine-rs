#!/usr/bin/env bash
# Captures EXPLAIN VIRTUAL output and the real execution result for one SQL
# statement against the local Docker stack (see docs/debugging-pushdown.md).
# {table} is substituted with the seeded typed_distinct_probe VS table name:
#   scripts/capture-pushdown-payload.sh 'SELECT COUNT(*) FROM {table} WHERE c_date LIKE '"'"'2024%'"'"''
# Leaves the stack running; tear down with `docker compose down -v`.
set -euo pipefail
cd "$(dirname "$0")/.."

if [ $# -ne 1 ]; then
  echo "usage: $0 '<SQL statement, use {table} for the VS table name>'" >&2
  exit 1
fi

make cross-udf-build

docker compose up -d minio-init
init_exit=$(docker wait "$(docker compose ps -q minio-init)")
if [ "$init_exit" != "0" ]; then
  echo "minio-init exited $init_exit (bucket creation failed)" >&2
  docker compose logs minio-init
  exit 1
fi
docker compose up -d --wait exasol minio iceberg-rest

CAPTURE_SQL="$1" cargo test --features exasol-e2e --test e2e_capture_pushdown \
  -- --nocapture --test-threads=1 capture_pushdown_payload
