#!/usr/bin/env bash
# Docker-mode delete-authoring helper: runs the apache/spark:3.5.7 image against
# scripts/spark-fixtures/create_tpch_deletes.sql to author a merge-on-read,
# 5%-position-deleted copy of the baseline TPC-H namespace (see that file's
# header for the full MOR/position-delete rationale and the #340 drop
# condition). Docker mode is always `rest_catalog` (see run_fixtures.sh), so
# that catalog name is fixed here rather than taken as an argument.
#
# Usage: bench/make_deletes_docker.sh <source_ns> <target_ns>
# Caller contract (bench/run.sh, Task B.2): the local stack must already be up
# (`docker compose up -d`) -- this script only runs Spark against the already-
# running iceberg-rest/minio services, it does not start the stack itself.
#
# Idempotent: create_tpch_deletes.sql has no `DROP TABLE IF EXISTS` (re-running
# it would double-delete an already-deleted target and break the 5% contract),
# so this script skips the Spark run if <target_ns> already has all 8 TPC-H
# tables -- mirroring tpch_loader.rs's "skip if already present" idempotency,
# via a REST-catalog `curl` (the same catalog run_fixtures.sh/run.sh already use)
# instead of pulling in a Rust/pyiceberg dependency just to list tables.
set -euo pipefail

SOURCE_NS="${1:?usage: make_deletes_docker.sh <source_ns> <target_ns>}"
TARGET_NS="${2:?usage: make_deletes_docker.sh <source_ns> <target_ns>}"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

CATALOG="rest_catalog"
REST_PORT="${LH_REST_PORT:-18181}"
TPCH_TABLES="region nation supplier customer part partsupp orders lineitem"
# Docker-compose's explicit network `name:` override (docker-compose.yml) --
# fixed regardless of COMPOSE_PROJECT_NAME, same network spark-iceberg-fixtures
# and iceberg-rest/minio join.
COMPOSE_NETWORK="lakehouse-engine"

already_populated() {
  local out n
  out="$(curl -fsS "http://localhost:${REST_PORT}/v1/namespaces/${TARGET_NS}/tables" 2>/dev/null)" || return 1
  n="$(printf '%s' "$out" | jq --arg tables "$TPCH_TABLES" '
    ($tables | split(" ")) as $want
    | [.identifiers[].name] as $have
    | [$want[] | select(. as $t | $have | index($t))] | length
  ' 2>/dev/null)" || return 1
  [ "${n:-0}" -eq 8 ]
}

if already_populated; then
  echo "== make_deletes_docker: '${TARGET_NS}' already has all 8 TPC-H tables -- skipping =="
  exit 0
fi

echo "== make_deletes_docker: authoring '${TARGET_NS}' from '${SOURCE_NS}' via Spark =="

ICEBERG_VERSION="1.10.1"
SPARK_PACKAGES="org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:${ICEBERG_VERSION},org.apache.iceberg:iceberg-aws-bundle:${ICEBERG_VERSION}"

# SAME SPARK_CONF as scripts/spark-fixtures/run_fixtures.sh (local REST catalog +
# MinIO, Ivy cache workaround). Duplicated rather than sourced: there is no
# "source a bash array from another script" primitive across a `docker run`
# boundary. Keep in lockstep with run_fixtures.sh if the catalog/MinIO wiring
# ever changes.
SPARK_CONF=(
  --master "local[*]"
  --packages "$SPARK_PACKAGES"
  --conf spark.jars.ivy=/tmp/ivy2
  --conf spark.sql.shuffle.partitions=1
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
  --conf spark.sql.catalog.rest_catalog=org.apache.iceberg.spark.SparkCatalog
  --conf spark.sql.catalog.rest_catalog.type=rest
  --conf spark.sql.catalog.rest_catalog.uri=http://iceberg-rest:8181
  --conf spark.sql.catalog.rest_catalog.warehouse=s3://warehouse/
  --conf spark.sql.catalog.rest_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO
  --conf spark.sql.catalog.rest_catalog.s3.endpoint=http://minio:9000
  --conf spark.sql.catalog.rest_catalog.s3.path-style-access=true
  --conf spark.sql.catalog.rest_catalog.s3.access-key-id=minioadmin
  --conf spark.sql.catalog.rest_catalog.s3.secret-access-key=minioadmin
  --conf spark.sql.defaultCatalog=rest_catalog
  --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000
  --conf spark.hadoop.fs.s3a.access.key=minioadmin
  --conf spark.hadoop.fs.s3a.secret.key=minioadmin
  --conf spark.hadoop.fs.s3a.path.style.access=true
)

docker run --rm \
  --network "$COMPOSE_NETWORK" \
  -e AWS_REGION=us-east-1 \
  -v "${SCRIPT_DIR}/../scripts/spark-fixtures:/fixtures:ro" \
  apache/spark:3.5.7 \
  /opt/spark/bin/spark-sql "${SPARK_CONF[@]}" \
  -d "catalog=${CATALOG}" -d "source_ns=${SOURCE_NS}" -d "target_ns=${TARGET_NS}" \
  -f /fixtures/create_tpch_deletes.sql

echo "== make_deletes_docker: done =="
