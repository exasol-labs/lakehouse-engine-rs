#!/usr/bin/env bash
# Spark authors the positional-delete fixtures because iceberg-rust has no
# position-delete writer and pyiceberg is copy-on-write only
# (apache/iceberg-rust#340). Once #340 lands, move the two positional-delete
# fixtures into tests/common/seed.rs and drop them from here.
set -euo pipefail

ICEBERG_VERSION="1.10.1"
SPARK_PACKAGES="org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:${ICEBERG_VERSION},org.apache.iceberg:iceberg-aws-bundle:${ICEBERG_VERSION}"

# hadoop-aws is needed only by the INT96 fixture's native Parquet write; pinned
# to the image's bundled Hadoop (3.3.4).
HADOOP_AWS_VERSION="3.3.4"

SPARK_CONF=(
  --master "local[*]"
  --packages "$SPARK_PACKAGES"
  # The image's non-root user has no home, so Ivy's default cache dir
  # (derived from JVM user.home, not $HOME) is unwritable.
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

# INT96 (#143) needs a native Spark Parquet file, so it adds hadoop-aws (the
# last --packages wins, so the Iceberg runtime is relisted) and aliases `s3` to
# S3AFileSystem so the write lands under the scheme the scan UDF registers.
echo "=== spark-iceberg-fixtures: INT96 far-future-timestamp fixture ==="
/opt/spark/bin/spark-sql "${SPARK_CONF[@]}" \
  --packages "${SPARK_PACKAGES},org.apache.hadoop:hadoop-aws:${HADOOP_AWS_VERSION}" \
  --conf spark.hadoop.fs.s3.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
  -f /fixtures/create_int96_timestamp_fixture.sql

echo "=== spark-iceberg-fixtures: done ==="
