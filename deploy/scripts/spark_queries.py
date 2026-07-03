#!/usr/bin/env python3
"""Q1-Q4 TPC-H benchmark driver for the EMR Serverless "Spark" side of the competitive engine
comparison (bench/spark_compare.sh). Reads the SAME Glue Iceberg REST catalog + S3 data as the
lakehouse engine, Athena, and Trino, and runs the same query set (see bench/run.sh lines ~321-349
for the canonical Exasol dialect this is translated from).

Configures Spark's Iceberg REST catalog with SigV4 signing against Glue (same auth model as the
lakehouse engine's own catalog connection). Credentials come from the EMR Serverless job execution
role — no static keys.

  spark-submit spark_queries.py <glue_uri> <glue_warehouse> <region>
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
]


def main():
    glue_uri, glue_warehouse, region = sys.argv[1], sys.argv[2], sys.argv[3]

    spark = (
        SparkSession.builder.appName("lakehouse-engine-competitive-bench")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.glue", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.glue.type", "rest")
        .config("spark.sql.catalog.glue.uri", glue_uri)
        .config("spark.sql.catalog.glue.warehouse", glue_warehouse)
        .config("spark.sql.catalog.glue.rest.sigv4-enabled", "true")
        .config("spark.sql.catalog.glue.rest.signing-region", region)
        .config("spark.sql.catalog.glue.rest.signing-name", "glue")
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
