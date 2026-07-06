[lakehouse-engine](../README.md) › [Docs](index.md) › Performance

---

# Performance

TPC-H sf=30 (8-table schema, `lineitem` 180M rows, 60 Parquet files, AWS Glue Iceberg catalog), same data for every engine. Live-verified 2026-07-06.

| Engine | Resources |
|---|---|
| lakehouse-engine-rs | Exasol `test1`, 2× `r8i.2xlarge` |
| Trino (native) | 2× `r8i.2xlarge`, ephemeral, fresh cluster |
| Trino (IMPORT FROM JDBC) | 2× `r8i.2xlarge`, ephemeral, fresh cluster (via Exasol `test1`) |
| Athena | on-demand workgroup |
| Spark | EMR Serverless |

Fastest time per query in **bold**.

| Query | lakehouse-engine-rs | Trino (2-node) | Athena | Spark (EMR Serverless) | IMPORT FROM JDBC (Trino) |
|---|---|---|---|---|---|
| Q1 (3-way join, wiring) | **1.67 s** | 2.81 s | 2.43 s | 18.89 s | 3.13 s |
| Q2 (3-way join, big scan) | 17.09 s | 9.71 s | **2.14 s** | 42.89 s | 8.77 s |
| Q3 (join + filter + GROUP BY) | 15.10 s | 5.12 s | **2.37 s** | 29.20 s | 4.91 s |
| Q4 (pricing summary, filter) | 3.89 s | **2.09 s** | 3.43 s | 18.94 s | 3.22 s |
| Q5 (Q3, no filter) | 18.06 s | 6.63 s | **2.16 s** | 35.38 s | 7.57 s |
| Q6 (Q4, no filter) | 3.56 s | **1.34 s** | 2.52 s | 16.52 s | 2.62 s |
| Q7 (high-cardinality GROUP BY) | 5.98 s | **2.04 s** | 2.30 s | 12.15 s | 2.71 s |
| Q8 (selective filter) | 2.34 s | **0.73 s** | 0.99 s | 4.38 s | 2.22 s |
| Q9a (narrow projection) | 1.17 s | **0.65 s** | 1.57 s | 4.51 s | 2.15 s |
| Q9b (wide projection) | **11.36 s** | 11.91 s | 27.93 s | 57.31 s | 12.78 s |
| NQ1 (arithmetic aggregate: `SUM(price*discount)`) | 3.96 s | **1.91 s** | 2.27 s | 10.72 s | 3.26 s |
| NQ2 (LIKE + IN filter pushdown) | 4.19 s | **2.38 s** | 2.71 s | 11.45 s | 3.67 s |
| NQ3 (4-way join, part/partsupp) | 4.40 s | **1.55 s** | 4.45 s | 3.88 s | 2.47 s |
| NQ4 (ORDER BY + LIMIT top-N) | 2.13 s | **1.62 s** | 2.34 s | 11.18 s | 2.88 s |
| NQ5 (tuple GROUP BY + HAVING + AVG) | 2.63 s | **0.54 s** | 2.06 s | 5.39 s | 2.18 s |

Reproduce: `RUN_TRINO_COMPARISON=1 bench/compare_all.sh` ([`bench/README.md`](../bench/README.md)).
