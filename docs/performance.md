[lakehouse-engine](../README.md) › [Docs](index.md) › Performance

---

# Performance

Live-verified 2026-07-04 on `test1` (2-node Exasol `r8i.2xlarge` cluster), TPC-H sf=30 (full
8-table schema, `lineitem` 180M rows, 60 Parquet files, AWS Glue Iceberg catalog), all four engines
reading the same data. `lakehouse-engine-rs` numbers are post-optimization (arithmetic
aggregate pushdown + ordered top-N pushdown, see `specs/_recorded/`). Trino ran on an ephemeral
2-node `r8i.2xlarge` cluster spun up for this run and torn down immediately after. **Win/Loss is
scored only against Trino** (the stated competitive target); Athena and Spark are included for
reference, not scored.

| Query | lakehouse-engine-rs | Trino (2-node) | vs Trino | Athena | Spark (EMR Serverless) |
|---|---|---|---|---|---|
| Q1 (3-way join, wiring) | 1.67 s | 7.51 s | **WIN** | 2.43 s | 18.89 s |
| Q2 (3-way join, big scan) | 17.09 s | 12.30 s | LOSS | 2.14 s | 42.89 s |
| Q3 (join + filter + GROUP BY) | 15.10 s | 8.79 s | LOSS | 2.37 s | 29.20 s |
| Q4 (pricing summary, filter) | 3.89 s | 5.50 s | **WIN** | 3.43 s | 18.94 s |
| Q5 (Q3, no filter) | 18.06 s | 9.71 s | LOSS | 2.16 s | 35.38 s |
| Q6 (Q4, no filter) | 3.56 s | 4.62 s | **WIN** | 2.52 s | 16.52 s |
| Q7 (high-cardinality GROUP BY) | 5.98 s | 5.28 s | LOSS | 2.30 s | 12.15 s |
| Q8 (selective filter) | 2.34 s | 4.05 s | **WIN** | 0.99 s | 4.38 s |
| Q9a (narrow projection) | 1.17 s | 3.69 s | **WIN** | 1.57 s | 4.51 s |
| Q9b (wide projection) | 11.36 s | 15.65 s | **WIN** | 27.93 s | 57.31 s |
| NQ1 (arithmetic aggregate: `SUM(price*discount)`) | 3.96 s | 5.31 s | **WIN** | 2.27 s | 10.72 s |
| NQ2 (LIKE + IN filter pushdown) | 4.19 s | 5.53 s | **WIN** | 2.71 s | 11.45 s |
| NQ3 (4-way join, part/partsupp) | 4.40 s | 4.97 s | **WIN** | 4.45 s | 3.88 s |
| NQ4 (ORDER BY + LIMIT top-N) | 2.13 s | 4.71 s | **WIN** | 2.34 s | 11.18 s |
| NQ5 (tuple GROUP BY + HAVING + AVG) | 2.63 s | 3.87 s | **WIN** | 2.06 s | 5.39 s |

**11 wins, 4 losses vs Trino.** The 4 losses (Q2, Q3, Q5, Q7) are join-shaped or high-cardinality
GROUP BY queries; see "Known losses" below for why they are structural, not unoptimized.

## What changed this round

- **NQ1 arithmetic aggregate pushdown** (`SUM(a * b)`): previously fell back to raw-emitting both
  columns (5.96 s baseline); the VS now advertises the arithmetic binary-operator capabilities and
  decomposes the product into a node-local partial SUM, landing at 3.96 s — a win against Trino
  that was a loss before.
- **NQ4 ordered top-N pushdown** (`ORDER BY ... LIMIT n`): previously raw-emitted the full 180M-row
  table for Exasol to sort (12.03 s baseline); the VS now pushes a per-shard bounded top-N
  (DataFusion TopK) merged by an explicit Exasol-side `ORDER BY ... LIMIT n`, landing at 2.13 s — a
  5.65x speedup, also now beating Trino.

## Known losses (accepted, structural)

Q2, Q3, and Q5 are join-shaped queries. The VS does not push joins into the DataFusion UDF — each
join leg is scanned and pushed down independently (with its own filter/projection pushdown), and
the join itself executes in Exasol over the imported results. For these queries the fact table
(`lineitem`) must be emitted in full because no predicate or aggregate from the other join leg can
be pushed into its scan without cross-leg visibility — i.e. without real join pushdown, which is an
explicit non-goal (it would require a UDF invocation to see more than its own assigned file shard,
violating the file-level work-assignment principle this engine is built on). Exasol's own join
execution (build side, join order) is confirmed correct via `PROFILE`; the cost is entirely the raw
row emission, not the join itself. Q7 (a ~45M-group high-cardinality `GROUP BY`) is already using
partial-aggregate pushdown correctly; its ~13% gap against Trino is an inherent shuffle cost both
engines pay and is not considered worth further optimization.

Reproduce: `bench/compare_all.sh` ([`bench/README.md`](../bench/README.md)).
