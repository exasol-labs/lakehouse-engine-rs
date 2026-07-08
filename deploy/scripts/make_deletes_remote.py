#!/usr/bin/env python3
"""Author the `tpch_deletes` Glue database (Iceberg v2 merge-on-read, 5% position-deleted) from the
pristine `tpch` Glue tables, as a Spark job on AWS EMR Serverless.

ONE-TIME remote data-prep, analogous to gen_load.py — run ONCE before a remote delete-bench, NOT
invoked by bench-remote.sh / spark_compare.sh automatically (submit it via
deploy/scripts/make-deletes-remote.sh). Idempotent: if the 8 target tables already exist it skips
cleanly, because create_tpch_deletes.sql deliberately has no DROP and re-applying the DELETE would
double-delete and corrupt the deterministic-5% contract.

Uses Spark's Iceberg GlueCatalog exactly as spark_queries.py does (the catalog config block below is
copied verbatim — same catalog wiring, same job). EMR Serverless has no internet egress, so the
submit uses the release's locally-bundled Iceberg jar
(--conf spark.jars=/usr/share/aws/iceberg/lib/iceberg-spark3-runtime.jar); credentials come from the
job execution role — no static keys.

  spark-submit make_deletes_remote.py <warehouse_s3_uri> [source_ns=tpch] [target_ns=tpch_deletes]

DELETE LOGIC IS A DELIBERATE DUPLICATION OF scripts/spark-fixtures/create_tpch_deletes.sql (task A.1).
That .sql file is the single source of truth for the docker-mode caller (bench/make_deletes_docker.sh),
but an EMR Serverless job runs from a lone S3 entrypoint with no access to other repo files at
runtime, so the CREATE+DELETE pairs are reimplemented here natively. The two MUST be kept in
lockstep: same 8 tables, same per-table surrogate key, the same `% 20 = 0` deterministic-5%
predicate, and the same `format-version=2` + `write.{delete,update,merge}.mode=merge-on-read`
TBLPROPERTIES. See create_tpch_deletes.sql's header for the MOR/position-delete rationale and the
apache/iceberg-rust#340 drop condition (when #340 lands, delete this file and that .sql together).
"""
import sys

from pyspark.sql import SparkSession

# The remote Iceberg catalog is always `glue` here (createVirtualSchema wiring + spark_queries.py).
CATALOG = "glue"

# (table, surrogate_key) — MUST match create_tpch_deletes.sql exactly (see module docstring).
# LINEITEM is keyed on l_orderkey (NOT l_linenumber) so its position deletes spread across all of
# LINEITEM's data files; PARTSUPP on ps_partkey deletes all suppliers for ~5% of parts (uniform ~5%).
TABLES = [
    ("region", "r_regionkey"),
    ("nation", "n_nationkey"),
    ("supplier", "s_suppkey"),
    ("customer", "c_custkey"),
    ("part", "p_partkey"),
    ("partsupp", "ps_partkey"),
    ("orders", "o_orderkey"),
    ("lineitem", "l_orderkey"),
]

# format-version=2 + merge-on-read is what makes `DELETE FROM` commit Parquet POSITION deletes (the
# read path this benchmark exercises); copy-on-write would rewrite data files instead. Verbatim from
# create_tpch_deletes.sql's TBLPROPERTIES.
TBLPROPERTIES = (
    "'format-version'='2',"
    "'write.delete.mode'='merge-on-read',"
    "'write.update.mode'='merge-on-read',"
    "'write.merge.mode'='merge-on-read'"
)


def existing_target_tables(spark, target_ns):
    """Names of tables already present in the target namespace ({} if the namespace does not exist)."""
    try:
        rows = spark.sql(f"SHOW TABLES IN {CATALOG}.{target_ns}").collect()
    except Exception:
        return set()
    return {r.tableName for r in rows}


def main():
    warehouse_s3_uri = sys.argv[1]
    source_ns = sys.argv[2] if len(sys.argv) > 2 else "tpch"
    target_ns = sys.argv[3] if len(sys.argv) > 3 else "tpch_deletes"

    spark = (
        SparkSession.builder.appName("lakehouse-engine-make-deletes")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.glue", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.glue.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
        .config("spark.sql.catalog.glue.warehouse", warehouse_s3_uri)
        .config("spark.hadoop.hive.metastore.client.factory.class",
                "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory")
        .getOrCreate()
    )

    print(f"authoring {CATALOG}.{target_ns} from {CATALOG}.{source_ns} (5% MOR position-deletes)", flush=True)

    # Idempotency: this is a one-time job and the SQL has no DROP (fails loudly on a dirty target), so
    # skip cleanly if every target table is already present rather than double-CTAS / double-DELETE.
    already = existing_target_tables(spark, target_ns)
    if all(t in already for t, _ in TABLES):
        print(f"SKIP: {CATALOG}.{target_ns} already has all {len(TABLES)} tables — nothing to do", flush=True)
        print("DONE", flush=True)
        spark.stop()
        return

    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.{target_ns}")

    for table, surrogate_key in TABLES:
        target = f"{CATALOG}.{target_ns}.{table}"
        source = f"{CATALOG}.{source_ns}.{table}"
        spark.sql(f"CREATE TABLE {target} USING iceberg TBLPROPERTIES ({TBLPROPERTIES}) AS SELECT * FROM {source}")
        spark.sql(f"DELETE FROM {target} WHERE {surrogate_key} % 20 = 0")
        print(f"  {table}: authored + 5% deleted (key {surrogate_key})", flush=True)

    print("DONE", flush=True)
    spark.stop()


if __name__ == "__main__":
    main()
