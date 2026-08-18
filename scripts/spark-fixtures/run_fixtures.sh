#!/usr/bin/env bash
# Runs the Apache Spark (Iceberg Spark runtime) fixture scripts that author
# Iceberg merge-on-read positional-delete tables (and one unsupported-delete
# table) for the lakehouse-engine E2E stack, against the SAME shared Iceberg
# REST catalog + MinIO the rest of the stack uses.
#
# UPSTREAM TRACKING (apache/iceberg-rust#340): iceberg-rust 0.10 has no
# position-delete writer, and pyiceberg is copy-on-write only, so Apache
# Spark — an official Apache Iceberg ecosystem engine — is used instead. A
# plain Spark `DELETE FROM` against a `write.delete.mode=merge-on-read` table
# commits Parquet POSITION deletes (Flink's row-level upsert connectors are
# the ones that commit EQUALITY deletes instead), which is exactly the delete
# mechanism this feature applies on read. The third fixture below instead sets
# `format-version=3` so the SAME merge-on-read DELETE commits a Puffin
# deletion vector — a delete mechanism this feature deliberately REJECTS at
# plan time (see create_deletion_vector_fixture.sql).
#
# DROP CONDITION: once #340 lands and iceberg-rust exposes a position-delete
# writer, replace the first two steps with native Rust fixture authoring in
# tests/common/seed.rs (matching its other seed tables), and delete this
# script, the two positional-delete fixture .sql files, and the
# spark-iceberg-fixtures docker-compose service. The deletion-vector fixture
# has a SEPARATE drop condition — see its own header comment.
set -euo pipefail

ICEBERG_VERSION="1.10.1"
SPARK_PACKAGES="org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:${ICEBERG_VERSION},org.apache.iceberg:iceberg-aws-bundle:${ICEBERG_VERSION}"

# hadoop-aws (Hadoop's S3AFileSystem + its AWS SDK) is needed ONLY by the INT96
# fixture below, whose native (non-Iceberg) Parquet write to MinIO and whose
# add_files read of that `s3://` source both go through Hadoop's filesystem — not
# Iceberg's S3FileIO (iceberg-aws-bundle), which serves the other fixtures. The
# base apache/spark image ships no hadoop-aws, so it is added on that one
# invocation. Pin to the image's bundled Hadoop (hadoop-client-*-3.3.4.jar).
HADOOP_AWS_VERSION="3.3.4"

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

echo "=== spark-iceberg-fixtures: type-promotion fixture ==="
/opt/spark/bin/spark-sql "${SPARK_CONF[@]}" -f /fixtures/create_iceberg_type_promotion_fixture.sql

# INT96 far-future-timestamp fixture (issue #143). Unlike the merge-on-read
# fixtures above (authored by Spark's *Iceberg* writer, which emits INT64
# regardless of outputTimestampType), this one needs a genuinely INT96-encoded
# data file, so it writes a *native* Spark Parquet file and registers it via the
# Iceberg add_files procedure — see create_int96_timestamp_fixture.sql for the
# full rationale. Two extra args make this the one fixture that can't reuse
# SPARK_CONF verbatim:
#   * The trailing --packages adds hadoop-aws for the native S3 write / add_files
#     read (see HADOOP_AWS_VERSION above); it still lists the Iceberg runtime, and
#     spark-submit's last --packages wins, so nothing from SPARK_CONF is lost.
#   * fs.s3.impl aliases the `s3` scheme to S3AFileSystem so the native write
#     lands under `s3://` (Hadoop 3.3.4 binds only `s3a` by default), matching the
#     scheme the scan UDF registers its object store under.
echo "=== spark-iceberg-fixtures: INT96 far-future-timestamp fixture ==="
/opt/spark/bin/spark-sql "${SPARK_CONF[@]}" \
  --packages "${SPARK_PACKAGES},org.apache.hadoop:hadoop-aws:${HADOOP_AWS_VERSION}" \
  --conf spark.hadoop.fs.s3.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
  -f /fixtures/create_int96_timestamp_fixture.sql

echo "=== spark-iceberg-fixtures: done ==="
