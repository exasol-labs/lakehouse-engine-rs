[lakehouse-engine](../README.md) › [Docs](index.md) › Performance

---

# Performance

Live-verified 2026-07-03 on `test1` (2-node Exasol `r8i.2xlarge` cluster), TPC-H `lineitem`
(180M rows, 60 Parquet files, AWS Glue Iceberg catalog), all four engines reading the same data.
`lakehouse-engine-rs` is the post-#56 measurement; Athena/Trino/Spark are carried over unchanged
from the initial pass. **Bold** marks the fastest engine per query.

| Query | lakehouse-engine-rs | AWS Athena | Trino (2-node) | Spark (EMR Serverless) |
|---|---|---|---|---|
| Q1 (wiring) | **1.80 s** | 1.87 s | 7.05 s | 16.80 s |
| Q2 (3-way join) | 16.54 s | **2.54 s** | 12.19 s | 43.59 s |
| Q3 (filter+groupby) | 14.45 s | **2.99 s** | 8.37 s | 31.51 s |
| Q4 (pricing summary) | 4.05 s | **3.19 s** | 5.56 s | 21.46 s |
| Q5 (Q3, no filter) | 18.03 s | **2.50 s** | 10.25 s | 38.07 s |
| Q6 (Q4, no filter) | 3.50 s | **2.71 s** | 4.98 s | 18.77 s |
| Q7 (high-card. GROUP BY) | 5.58 s | **1.62 s** | 5.20 s | 12.84 s |
| Q8 (selective filter) | 1.86 s | **0.93 s** | 4.25 s | 5.62 s |
| Q9a (narrow projection) | **1.21 s** | 2.39 s | 3.78 s | 5.64 s |
| Q9b (wide projection) | **10.76 s** | 44.98 s | 15.19 s | 59.19 s |

Reproduce: `bench/compare_all.sh` ([`bench/README.md`](../bench/README.md)).
