#!/usr/bin/env bash
# Authors a merge-on-read, 5%-position-deleted copy of a TPC-H namespace in the local Docker
# stack via scripts/spark-fixtures/create_tpch_deletes.sql. The stack must already be up.
# Usage: bench/make_deletes_docker.sh <source_ns> <target_ns>
#
# Skips when <target_ns> already has all 8 tables: re-running the SQL would double-delete and
# break the 5% contract.
set -euo pipefail

SOURCE_NS="${1:?usage: make_deletes_docker.sh <source_ns> <target_ns>}"
TARGET_NS="${2:?usage: make_deletes_docker.sh <source_ns> <target_ns>}"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

CATALOG="rest_catalog"
REST_PORT="${LH_REST_PORT:-18181}"
TPCH_TABLES="region nation supplier customer part partsupp orders lineitem"
# Fixed by docker-compose.yml's network `name:`, independent of COMPOSE_PROJECT_NAME.
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

# Keep in lockstep with scripts/spark-fixtures/run_fixtures.sh's SPARK_CONF.
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
