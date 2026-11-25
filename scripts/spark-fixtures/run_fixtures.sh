#!/usr/bin/env bash
# Spark authors the positional-delete fixtures because iceberg-rust has no
# position-delete writer and pyiceberg is copy-on-write only
# (apache/iceberg-rust#340). Once #340 lands, move the two positional-delete
# fixtures into tests/common/seed.rs and drop them from here.
set -euo pipefail

# The exadockerci4/spark-lakehouse Iceberg image bakes in the Iceberg runtime,
# its AWS integration, and hadoop-aws, so the job downloads nothing.
SPARK_CONF=(
  --master "local[*]"
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
  # INT96 (#143), identity-partition (#99), and name-mapping (#83): aliases `s3`
  # to S3AFileSystem so Spark's native writes land under the scheme the scan UDF
  # registers.
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
  create_identity_partition_fixture.sql
  create_name_mapping_fixture.sql
  create_export_append_fixture.sql
  create_export_partition_fixture.sql
  create_int96_timestamp_fixture.sql
)
(cd /fixtures && cat "${FIXTURES[@]}") > /tmp/fixtures.sql
/opt/spark/bin/spark-sql "${SPARK_CONF[@]}" -f /tmp/fixtures.sql
