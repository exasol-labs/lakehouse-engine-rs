#!/usr/bin/env bash
# Spark authors the positional-delete fixtures because iceberg-rust has no
# position-delete writer and pyiceberg is copy-on-write only
# (apache/iceberg-rust#340). Once #340 lands, move the two positional-delete
# fixtures into tests/common/seed.rs and drop them from here.
set -euo pipefail

ICEBERG_VERSION="1.10.1"
# hadoop-aws, pinned to the image's bundled Hadoop, serves only the INT96
# fixture's native Parquet write.
HADOOP_AWS_VERSION="3.3.4"
SPARK_PACKAGES="org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:${ICEBERG_VERSION},org.apache.iceberg:iceberg-aws-bundle:${ICEBERG_VERSION},org.apache.hadoop:hadoop-aws:${HADOOP_AWS_VERSION}"

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
  --conf spark.sql.catalog.rest_catalog.s3.endpoint=http://seaweedfs:8333
  --conf spark.sql.catalog.rest_catalog.s3.path-style-access=true
  --conf spark.sql.catalog.rest_catalog.s3.access-key-id=lhadmin
  --conf spark.sql.catalog.rest_catalog.s3.secret-access-key=lhadminsecret123
  --conf spark.sql.defaultCatalog=rest_catalog
  # INT96 (#143): aliases `s3` to S3AFileSystem so Spark's native write lands
  # under the scheme the scan UDF registers.
  --conf spark.hadoop.fs.s3.impl=org.apache.hadoop.fs.s3a.S3AFileSystem
  --conf spark.hadoop.fs.s3a.endpoint=http://seaweedfs:8333
  --conf spark.hadoop.fs.s3a.access.key=lhadmin
  --conf spark.hadoop.fs.s3a.secret.key=lhadminsecret123
  --conf spark.hadoop.fs.s3a.path.style.access=true
)

# INT96 runs last: its SETs switch the shared session's Parquet writer.
FIXTURES=(
  create_file_granularity_fixture.sql
  create_partition_granularity_fixture.sql
  create_deletion_vector_fixture.sql
  create_unsupported_format_fixture.sql
  create_iceberg_type_promotion_fixture.sql
  create_int96_timestamp_fixture.sql
)
(cd /fixtures && cat "${FIXTURES[@]}") > /tmp/fixtures.sql
/opt/spark/bin/spark-sql "${SPARK_CONF[@]}" -f /tmp/fixtures.sql
