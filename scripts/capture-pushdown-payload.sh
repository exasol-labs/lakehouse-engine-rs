#!/usr/bin/env bash
# Capture what the Virtual Schema adapter generates for a given SQL statement,
# against the local Exasol + MinIO + Iceberg REST Docker stack: the EXPLAIN
# VIRTUAL output (adapter-generated scan SQL / scan-spec JSON) and the real
# execution result (rows or the actual runtime error).
#
# Reusable diagnostic tool — see docs/debugging-pushdown.md for the seeded
# table's columns/types and example invocations. Not part of `make test-e2e`;
# this is a single-query, one-off capture, not the full E2E suite.
#
# Usage:
#   scripts/capture-pushdown-payload.sh 'SELECT COUNT(*) FROM {table} WHERE c_date LIKE '"'"'2024%'"'"''
#
# {table} is substituted with the seeded typed_distinct_probe VS table name.
#
# Brings the stack up if not already running and leaves it running afterward
# so follow-up queries are cheap; tear it down yourself when done:
#   docker compose down -v
set -euo pipefail
cd "$(dirname "$0")/.."

if [ $# -ne 1 ]; then
  echo "usage: $0 '<SQL statement, use {table} for the VS table name>'" >&2
  exit 1
fi

make cross-musl-udf-build

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
