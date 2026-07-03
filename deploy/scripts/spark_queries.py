#!/usr/bin/env python3
"""Q1-Q9b TPC-H benchmark driver for the EMR Serverless "Spark" side of the competitive engine
comparison (bench/spark_compare.sh). Reads the SAME Glue-cataloged S3 data as the lakehouse
engine, Athena, and Trino, and runs the same query set (see bench/run.sh for the canonical Exasol
dialect this is translated from).

Uses Spark's Iceberg GlueCatalog implementation (talks to AWS Glue directly via the AWS SDK, the
same "native Glue" pattern used for Trino's iceberg.catalog.type=glue — Glue IS the catalog here,
not a generic REST endpoint, so no REST/SigV4 config is needed). Requires
`--conf spark.jars=/usr/share/aws/iceberg/lib/iceberg-spark3-runtime.jar` on the job submission
(EMR Serverless has no internet egress by default, so `spark.jars.packages`, which fetches from
Maven Central via Ivy, times out — found live-verifying against a real EMR Serverless run).
Credentials come from the EMR Serverless job execution role — no static keys.

  spark-submit spark_queries.py <warehouse_s3_uri>
"""
import sys
import time

from pyspark.sql import SparkSession

QUERIES = [
    ("q1", """
        SELECT n.n_name, r.r_name, COUNT(*) AS suppliers
        FROM glue.tpch.supplier s JOIN glue.tpch.nation n ON s.s_nationkey = n.n_nationkey
        JOIN glue.tpch.region r ON n.n_regionkey = r.r_regionkey
        GROUP BY n.n_name, r.r_name ORDER BY n.n_name
    """),
    ("q2", """
        SELECT COUNT(*) AS rows_joined FROM glue.tpch.customer c
        JOIN glue.tpch.orders o ON c.c_custkey = o.o_custkey
        JOIN glue.tpch.lineitem l ON o.o_orderkey = l.l_orderkey
    """),
    ("q3", """
        SELECT o.o_orderpriority, COUNT(*) AS cnt, SUM(l.l_extendedprice) AS revenue
        FROM glue.tpch.orders o JOIN glue.tpch.lineitem l ON o.o_orderkey = l.l_orderkey
        WHERE o.o_orderdate >= DATE '1994-01-01' AND o.o_orderdate < DATE '1995-01-01'
        GROUP BY o.o_orderpriority ORDER BY o.o_orderpriority
    """),
    ("q4", """
        SELECT l_returnflag, l_linestatus, SUM(l_quantity) AS sum_qty,
               SUM(l_extendedprice) AS sum_base_price, AVG(l_discount) AS avg_disc,
               COUNT(*) AS count_order
        FROM glue.tpch.lineitem WHERE l_shipdate <= DATE '1998-09-01'
        GROUP BY l_returnflag, l_linestatus ORDER BY l_returnflag, l_linestatus
    """),
    # Q5-Q9b probe specific pushdown strengths/weaknesses beyond Q1-Q4 — identical SQL
    # (dialect-adjusted) in bench/run.sh, bench/athena_compare.sh, bench/trino_compare.sh.
    ("q5", """
        SELECT o.o_orderpriority, COUNT(*) AS cnt, SUM(l.l_extendedprice) AS revenue
        FROM glue.tpch.orders o JOIN glue.tpch.lineitem l ON o.o_orderkey = l.l_orderkey
        GROUP BY o.o_orderpriority ORDER BY o.o_orderpriority
    """),
    ("q6", """
        SELECT l_returnflag, l_linestatus, SUM(l_quantity) AS sum_qty,
               SUM(l_extendedprice) AS sum_base_price, AVG(l_discount) AS avg_disc,
               COUNT(*) AS count_order
        FROM glue.tpch.lineitem
        GROUP BY l_returnflag, l_linestatus ORDER BY l_returnflag, l_linestatus
    """),
    ("q7", """
        SELECT COUNT(*) FROM (
            SELECT l_orderkey, COUNT(*) AS cnt FROM glue.tpch.lineitem GROUP BY l_orderkey
        ) t
    """),
    ("q8", "SELECT COUNT(*) FROM glue.tpch.lineitem WHERE l_shipdate = DATE '1995-06-15'"),
    ("q9a", "SELECT SUM(l_quantity) FROM glue.tpch.lineitem"),
    ("q9b", """
        SELECT COUNT(*),
               SUM(l_orderkey), SUM(l_partkey), SUM(l_suppkey), SUM(l_linenumber),
               SUM(l_quantity), SUM(l_extendedprice), SUM(l_discount), SUM(l_tax),
               COUNT(DISTINCT l_returnflag), COUNT(DISTINCT l_linestatus),
               MIN(l_shipdate), MAX(l_commitdate), MIN(l_receiptdate),
               COUNT(DISTINCT l_shipinstruct), COUNT(DISTINCT l_shipmode),
               SUM(length(l_comment))
        FROM glue.tpch.lineitem
    """),
]


def main():
    warehouse_s3_uri = sys.argv[1]

    spark = (
        SparkSession.builder.appName("lakehouse-engine-competitive-bench")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.glue", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.glue.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
        .config("spark.sql.catalog.glue.warehouse", warehouse_s3_uri)
        .config("spark.hadoop.hive.metastore.client.factory.class",
                "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory")
        .getOrCreate()
    )

    for name, sql in QUERIES:
        t0 = time.time()
        spark.sql(sql).collect()
        elapsed = time.time() - t0
        # Scraped back out of the driver stdout log by bench/spark_compare.sh — keep this exact format.
        print(f"elapsed: {name} {elapsed:.2f}s")

    spark.stop()


if __name__ == "__main__":
    main()
