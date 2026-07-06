[lakehouse-engine](../README.md) › [Docs](index.md) › Performance

---

# Performance

Live-verified 2026-07-06 on `test1` (2-node Exasol `r8i.2xlarge` cluster), TPC-H sf=30 (full
8-table schema, `lineitem` 180M rows, 60 Parquet files, AWS Glue Iceberg catalog), all four engines
reading the same data. `lakehouse-engine-rs` numbers are post-optimization (arithmetic
aggregate pushdown + ordered top-N pushdown, see `specs/_recorded/`). **Win/Loss is scored only
against Trino** (the stated competitive target); Athena and Spark are included for reference, not
scored. The "IMPORT FROM JDBC" column is Exasol's own native `IMPORT FROM JDBC` reader pushing
each query down as a sub-select over a JDBC connection to Trino — a third access pattern, not
itself the scored competitive target, so it's informational and excluded from Win/Loss.

**Measurement methodology (Trino, both columns).** Native and IMPORT-FROM-JDBC each ran against
their own freshly-booted, cold Trino cluster (2-node `r8i.2xlarge`, provisioned, measured, and
torn down independently) — a shared cluster would let whichever measurement ran first JIT-warm
Trino and cache Iceberg manifest/split listings for the other, skewing the comparison. Native
Trino is measured via ONE persistent Trino CLI session for the whole 15-query batch, launched via
SSH onto a Trino worker node — not the operator's machine, not the coordinator — so it pays the
same client-overhead profile (no per-query container/JVM cold start) and network-hop shape
(operator machine → the cluster's own node, over the internet; that node → the thing being
measured, intra-VPC) as the VS and IMPORT-FROM-JDBC measurements, both of which submit one
`exapump` call per query to Exasol. An earlier version of this benchmark spun up a fresh Docker
container + JVM cold start **per query** from the operator's machine, reaching Trino over the
public internet; that made native Trino look markedly slower than it is, which is why the very
first IMPORT-FROM-JDBC numbers appeared to beat native Trino on every single query — they were
never a fair comparison. See `bench/trino_compare.sh`'s header comment for the full audit and fix.

| Query | lakehouse-engine-rs | Trino (2-node) | vs Trino | Athena | Spark (EMR Serverless) | IMPORT FROM JDBC (Trino) |
|---|---|---|---|---|---|---|
| Q1 (3-way join, wiring) | 1.67 s | 2.81 s | **WIN** | 2.43 s | 18.89 s | 3.13 s |
| Q2 (3-way join, big scan) | 17.09 s | 9.71 s | LOSS | 2.14 s | 42.89 s | 8.77 s |
| Q3 (join + filter + GROUP BY) | 15.10 s | 5.12 s | LOSS | 2.37 s | 29.20 s | 4.91 s |
| Q4 (pricing summary, filter) | 3.89 s | 2.09 s | LOSS | 3.43 s | 18.94 s | 3.22 s |
| Q5 (Q3, no filter) | 18.06 s | 6.63 s | LOSS | 2.16 s | 35.38 s | 7.57 s |
| Q6 (Q4, no filter) | 3.56 s | 1.34 s | LOSS | 2.52 s | 16.52 s | 2.62 s |
| Q7 (high-cardinality GROUP BY) | 5.98 s | 2.04 s | LOSS | 2.30 s | 12.15 s | 2.71 s |
| Q8 (selective filter) | 2.34 s | 0.73 s | LOSS | 0.99 s | 4.38 s | 2.22 s |
| Q9a (narrow projection) | 1.17 s | 0.65 s | LOSS | 1.57 s | 4.51 s | 2.15 s |
| Q9b (wide projection) | 11.36 s | 11.91 s | **WIN** | 27.93 s | 57.31 s | 12.78 s |
| NQ1 (arithmetic aggregate: `SUM(price*discount)`) | 3.96 s | 1.91 s | LOSS | 2.27 s | 10.72 s | 3.26 s |
| NQ2 (LIKE + IN filter pushdown) | 4.19 s | 2.38 s | LOSS | 2.71 s | 11.45 s | 3.67 s |
| NQ3 (4-way join, part/partsupp) | 4.40 s | 1.55 s | LOSS | 4.45 s | 3.88 s | 2.47 s |
| NQ4 (ORDER BY + LIMIT top-N) | 2.13 s | 1.62 s | LOSS | 2.34 s | 11.18 s | 2.88 s |
| NQ5 (tuple GROUP BY + HAVING + AVG) | 2.63 s | 0.54 s | LOSS | 2.06 s | 5.39 s | 2.18 s |

**2 wins, 13 losses vs Trino.** This table replaces an earlier version scored 11 wins / 4 losses —
that scoring was an artifact of the Trino measurement bug described above, not a change in either
engine's actual performance. lakehouse-engine-rs's own numbers are unchanged from before; only how
Trino was measured changed. See "Analysis" below.

## Analysis

Fixing the Trino measurement bug (above) revealed that lakehouse-engine-rs is slower than native
Trino on most of these queries at this scale, not faster. The two wins:

- **Q1** (3-way dimension join, small tables: supplier/nation/region) — the VS wins on a query
  shaped like a wiring/smoke test, not a demanding scan.
- **Q9b** (wide projection, 16 aggregates over the full `lineitem` scan) — the two engines are
  within 5% of each other; not a meaningful win either way.

The other 13 losses split into two groups:

- **Join-shaped queries (Q2, Q3, Q5, NQ3)**: the VS does not push joins into the DataFusion UDF —
  each join leg is scanned and pushed down independently (with its own filter/projection
  pushdown), and the join itself executes in Exasol over the imported results. The fact table
  (`lineitem`, or `part`/`partsupp` for NQ3) must be emitted in full because no predicate or
  aggregate from the other join leg can be pushed into its scan without cross-leg visibility — i.e.
  without real join pushdown, which is an explicit non-goal (it would require a UDF invocation to
  see more than its own assigned file shard, violating the file-level work-assignment principle
  this engine is built on). This is a structural, accepted limitation.
- **Single-table scan/aggregate queries (Q4, Q6, Q7, Q8, Q9a, NQ1, NQ2, NQ4, NQ5)**: these already
  use the VS's pushdown (projection, filter, aggregate, or top-N as applicable) and still lose to
  Trino, in some cases by a wide margin (Q9a: 1.17s vs 0.65s; NQ5: 2.63s vs 0.54s). This is not
  explained by missing pushdown capability — it reflects that Trino's distributed execution engine
  is simply faster at these workloads on this hardware, at this scale, than the current
  DataFusion-in-UDF architecture. Closing this gap is future work, not something this table
  currently claims.

Reproduce: `RUN_TRINO_COMPARISON=1 bench/compare_all.sh` ([`bench/README.md`](../bench/README.md)).
