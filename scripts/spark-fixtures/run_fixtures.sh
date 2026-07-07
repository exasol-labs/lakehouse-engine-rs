#!/usr/bin/env bash
# Runs the Apache Spark (Iceberg Spark runtime) fixture scripts that author
# Iceberg merge-on-read positional-delete, deletion-vector, mixed-mechanism,
# and still-unsupported-mechanism tables for the lakehouse-engine E2E stack,
# against the SAME shared Iceberg REST catalog + MinIO the rest of the stack
# uses.
#
# UPSTREAM TRACKING (apache/iceberg-rust#340): iceberg-rust 0.10 has no
# position-delete writer, and pyiceberg is copy-on-write only, so Apache
# Spark — an official Apache Iceberg ecosystem engine — is used instead. A
# plain Spark `DELETE FROM` against a `write.delete.mode=merge-on-read` table
# commits Parquet POSITION deletes (Flink's row-level upsert connectors are
# the ones that commit EQUALITY deletes instead), which is exactly the delete
# mechanism this feature applies on read. The deletion-vector fixture instead
# sets `format-version=3` so the SAME merge-on-read DELETE commits a Puffin
# deletion vector — a delete mechanism this engine now APPLIES on read (see
# `datafusion-scan/scan-execution-deletion-vectors`). The mixed-mechanism
# fixture upgrades format-version mid-fixture so ONE table ends up with a
# data file under each mechanism (the v2→v3 migration shape). The ORC fixture
# exercises a mechanism that remains genuinely unsupported (equality deletes
# cannot be produced by this stack — only Flink writes them, and Flink is not
# part of this stack).
#
# DROP CONDITION: once #340 lands and iceberg-rust exposes a position-delete
# writer, replace the first two steps with native Rust fixture authoring in
# tests/common/seed.rs (matching its other seed tables), and delete this
# script, the positional-delete fixture .sql files, and the
# spark-iceberg-fixtures docker-compose service. The deletion-vector and
# mixed-mechanism fixtures have a SEPARATE drop condition — see their own
# header comments.
set -euo pipefail

ICEBERG_VERSION="1.10.1"
SPARK_PACKAGES="org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:${ICEBERG_VERSION},org.apache.iceberg:iceberg-aws-bundle:${ICEBERG_VERSION}"

# rest_catalog: the SAME REST catalog (iceberg-rest:8181) + MinIO (minio:9000,
# bucket s3://warehouse/) every other E2E fixture/table uses — see
# tests/common/seed.rs's build_seed_catalog and docker-compose.yml's
# iceberg-rest service env.
SPARK_CONF=(
  --master "local[*]"
  --packages "$SPARK_PACKAGES"
  # The official Spark image runs as a non-root user with no passwd/home
  # entry, so Ivy's default cache dir (derived from the JVM `user.home`
  # system property, NOT the $HOME env var) resolves to an unwritable path
  # ("/nonexistent/.ivy2") and dependency resolution fails. Point Ivy at a
  # path under the container's own (world-writable) /tmp instead. Container-
  # local and not volume-backed — deliberately: a named volume defaults to
  # root:root ownership, unwritable by this image's non-root user, and fixing
  # that would need its own permission-init step for a one-shot job that runs
  # once per stack bring-up. The `--packages` download (~150 MB) is repeated
  # on every `docker compose up`; that's the accepted trade-off for staying
  # simple.
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

echo "=== spark-iceberg-fixtures: write.delete.granularity=file MOR fixture ==="
/opt/spark/bin/spark-sql "${SPARK_CONF[@]}" -f /fixtures/create_file_granularity_fixture.sql

echo "=== spark-iceberg-fixtures: write.delete.granularity=partition MOR fixture ==="
/opt/spark/bin/spark-sql "${SPARK_CONF[@]}" -f /fixtures/create_partition_granularity_fixture.sql

echo "=== spark-iceberg-fixtures: format-version=3 Puffin deletion-vector fixture ==="
/opt/spark/bin/spark-sql "${SPARK_CONF[@]}" -f /fixtures/create_deletion_vector_fixture.sql

echo "=== spark-iceberg-fixtures: mixed positional-delete + deletion-vector fixture ==="
/opt/spark/bin/spark-sql "${SPARK_CONF[@]}" -f /fixtures/create_mixed_mechanism_fixture.sql

echo "=== spark-iceberg-fixtures: still-unsupported ORC data file fixture ==="
/opt/spark/bin/spark-sql "${SPARK_CONF[@]}" -f /fixtures/create_orc_unsupported_fixture.sql

echo "=== spark-iceberg-fixtures: done ==="
