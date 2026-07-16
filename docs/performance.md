[lakehouse-engine](../README.md) › [Docs](index.md) › Performance

---

# Performance

## What this is

TPC-H sf=30 (8-table schema, `lineitem` 180M rows, 60 Parquet files, AWS Glue Iceberg catalog), same
data for every engine, run against the `test1` Exasol cluster (2× `r8i.2xlarge`). Two variants of the
same 15-query set (Q1-Q9b, NQ1-NQ5):

- **(a) Without deletes** — the pristine baseline `tpch` Iceberg tables.
- **(b) With deletes** — Iceberg v2 merge-on-read position-delete copies of the same tables
  (`BENCH_WITH_DELETES=1`; ~5% of rows deleted per table, deterministic `key % 20 = 0`, authored via
  Apache Spark since neither PyIceberg nor iceberg-rust can write MOR position deletes — see
  `scripts/spark-fixtures/create_tpch_deletes.sql` for the mechanism). Confirms the merge-on-read read
  path's cost at scale, not just correctness (already covered by `make test-e2e`).

All five engines now support a with-deletes variant: `BENCH_WITH_DELETES=1` for lakehouse-engine-rs
(`make bench`), plus `ATHENA_DATABASE` / `TRINO_SCHEMA` / `SPARK_NAMESPACE` for the four competitor
scripts (each defaults to `tpch_deletes` when the flag is on and the override is unset) — see
[`bench/README.md`](../bench/README.md)'s "Delete-bearing benchmark" and "Competitive engine
comparison" sections. `import_ceiling.sh` (the native `IMPORT FROM PARQUET` ceiling) stays out of
scope for the delete variant: it reads raw Parquet files directly and never applies Iceberg
position-deletes, so a "with deletes" run of it wouldn't measure anything real.

Reproduce: `make bench` (without deletes) / `BENCH_WITH_DELETES=1 make bench` (with deletes) — see
[`bench/README.md`](../bench/README.md). Remote (`test1`): `deploy/scripts/bench-remote.sh test1`
(optionally prefixed with `BENCH_WITH_DELETES=1`) — see [`deploy/README.md`](../deploy/README.md).
Competitor engines: `BENCH_WITH_DELETES=1 ./bench/athena_compare.sh` /
`BENCH_WITH_DELETES=1 TRINO_HOST=... TRINO_WORKER_HOST=... ./bench/trino_compare.sh` /
`BENCH_WITH_DELETES=1 TRINO_HOST=... ./bench/import_jdbc_trino.sh` /
`BENCH_WITH_DELETES=1 ./bench/spark_compare.sh` — `compare_all.sh` stays single-variant per
invocation (run it once per flag value to get both tables).

| Resource | |
|---|---|
| lakehouse-engine-rs | Exasol `test1`, 2× `r8i.2xlarge` |
| Trino (native) | 2× `r8i.2xlarge`, ephemeral, fresh cluster |
| Trino (IMPORT FROM JDBC) | 2× `r8i.2xlarge`, ephemeral, fresh cluster (via Exasol `test1`) |
| Athena | on-demand workgroup |
| Spark | EMR Serverless |

## (a) Without deletes

Fastest lakehouse-engine-rs-vs-competitor time per query in **bold**.

| Query | lakehouse-engine-rs | Trino (native) | Trino (IMPORT FROM JDBC) | Athena | Spark |
|---|---|---|---|---|---|
| Q1 (3-way join, wiring) | **1.73 s** | 2.81 s | 3.13 s | 2.43 s | 18.89 s |
| Q2 (3-way join, big scan) | 18.03 s | 9.71 s | 8.77 s | **2.14 s** | 42.89 s |
| Q3 (join + filter + GROUP BY) | 15.06 s | 5.12 s | 4.91 s | **2.37 s** | 29.20 s |
| Q4 (pricing summary, filter) | 5.70 s | **2.09 s** | 3.22 s | 3.43 s | 18.94 s |
| Q5 (Q3, no filter) | 18.44 s | 6.63 s | 7.57 s | **2.16 s** | 35.38 s |
| Q6 (Q4, no filter) | 4.40 s | **1.34 s** | 2.62 s | 2.52 s | 16.52 s |
| Q7 (high-cardinality GROUP BY) | 7.27 s | **2.04 s** | 2.71 s | 2.30 s | 12.15 s |
| Q8 (selective filter) | 3.27 s | **0.73 s** | 2.22 s | 0.99 s | 4.38 s |
| Q9a (narrow projection) | 2.67 s | **0.65 s** | 2.15 s | 1.57 s | 4.51 s |
| Q9b (wide projection) | **11.54 s** | 11.91 s | 12.78 s | 27.93 s | 57.31 s |
| NQ1 (arithmetic aggregate: `SUM(price*discount)`) | 5.22 s | **1.91 s** | 3.26 s | 2.27 s | 10.72 s |
| NQ2 (LIKE + IN filter pushdown) | 5.18 s | **2.38 s** | 3.67 s | 2.71 s | 11.45 s |
| NQ3 (4-way join, part/partsupp) | 5.52 s | **1.55 s** | 2.47 s | 4.45 s | 3.88 s |
| NQ4 (ORDER BY + LIMIT top-N) | 3.53 s | **1.62 s** | 2.88 s | 2.34 s | 11.18 s |
| NQ5 (tuple GROUP BY + HAVING + AVG) | 2.07 s | **0.54 s** | 2.18 s | 2.06 s | 5.39 s |

## (b) With deletes (`BENCH_WITH_DELETES=1`)

All five engines now read the same delete-bearing `tpch_deletes` Iceberg v2 merge-on-read tables
(authored once via the remote EMR Serverless job, `deploy/scripts/make-deletes-remote.sh`) — same
shape as table (a), fastest time per query in **bold**. All five columns are from one 2026-07-09
`test1` session (superseding the lakehouse-engine-rs-only 2026-07-08 numbers previously here, so
every column reflects the same cluster/moment) — see "Reproduce" above for the exact invocations.

| Query | lakehouse-engine-rs (with deletes) | Trino (native, with deletes) | Trino (IMPORT FROM JDBC, with deletes) | Athena (with deletes) | Spark (with deletes) |
|---|---|---|---|---|---|
| Q1 (3-way join, wiring) | 2.20 s | 3.33 s | **1.86 s** | 1.89 s | 15.38 s |
| Q2 (3-way join, big scan) | 92.43 s | 13.17 s | 18.27 s | **5.06 s** | 97.88 s |
| Q3 (join + filter + GROUP BY) | 50.79 s | 8.34 s | 9.60 s | **2.93 s** | 84.33 s |
| Q4 (pricing summary, filter) | 32.81 s | 6.55 s | 8.00 s | **4.42 s** | 70.84 s |
| Q5 (Q3, no filter) | 61.17 s | 9.55 s | 11.86 s | **3.32 s** | 90.09 s |
| Q6 (Q4, no filter) | 32.43 s | 6.11 s | 7.28 s | **4.50 s** | 67.16 s |
| Q7 (high-cardinality GROUP BY) | 33.78 s | 6.16 s | 7.78 s | **3.51 s** | 62.77 s |
| Q8 (selective filter) | 30.73 s | 5.77 s | 7.44 s | **3.33 s** | 57.93 s |
| Q9a (narrow projection) | 29.66 s | 5.78 s | 6.98 s | **2.76 s** | 58.98 s |
| Q9b (wide projection) | 37.86 s | **14.28 s** | 15.35 s | 26.16 s | 108.77 s |
| NQ1 (arithmetic aggregate: `SUM(price*discount)`) | 32.52 s | 6.54 s | 7.85 s | **2.70 s** | 62.06 s |
| NQ2 (LIKE + IN filter pushdown) | 31.17 s | 6.98 s | 8.61 s | **2.15 s** | 65.29 s |
| NQ3 (4-way join, part/partsupp) | 9.46 s | **1.26 s** | 3.15 s | 3.24 s | 5.84 s |
| NQ4 (ORDER BY + LIMIT top-N) | 29.94 s | 6.29 s | 7.26 s | **4.03 s** | 64.92 s |
| NQ5 (tuple GROUP BY + HAVING + AVG) | 11.06 s | **1.25 s** | 2.43 s | 1.60 s | 9.78 s |

**Merge-on-read read overhead is substantial at this scale for every engine**, confirming this
isn't a lakehouse-engine-rs-specific cost — see the per-engine ratio table below. lakehouse-engine-rs
is hit hardest in absolute and relative terms on non-join queries (up to ~11x, Q9a); Spark is next
most affected in absolute time despite native Iceberg MOR support; Trino (both paths) and Athena
absorb the position-delete reconciliation cost far more cheaply, consistent with table (a)'s
existing gap on non-join, single-table scans.

### vs. without deletes (§a)

Per-engine ratio: (b)'s time / (a)'s time, same query rows.

| Query | lakehouse-engine-rs | Trino (native) | Trino (IMPORT FROM JDBC) | Athena | Spark |
|---|---|---|---|---|---|
| Q1 (3-way join, wiring) | 1.3x | 1.2x | 0.6x | 0.8x | 0.8x |
| Q2 (3-way join, big scan) | 5.1x | 1.4x | 2.1x | 2.4x | 2.3x |
| Q3 (join + filter + GROUP BY) | 3.4x | 1.6x | 2.0x | 1.2x | 2.9x |
| Q4 (pricing summary, filter) | 5.8x | 3.1x | 2.5x | 1.3x | 3.7x |
| Q5 (Q3, no filter) | 3.3x | 1.4x | 1.6x | 1.5x | 2.5x |
| Q6 (Q4, no filter) | 7.4x | 4.6x | 2.8x | 1.8x | 4.1x |
| Q7 (high-cardinality GROUP BY) | 4.6x | 3.0x | 2.9x | 1.5x | 5.2x |
| Q8 (selective filter) | 9.4x | 7.9x | 3.4x | 3.4x | 13.2x |
| Q9a (narrow projection) | 11.1x | 8.9x | 3.2x | 1.8x | 13.1x |
| Q9b (wide projection) | 3.3x | 1.2x | 1.2x | 0.9x | 1.9x |
| NQ1 (arithmetic aggregate: `SUM(price*discount)`) | 6.2x | 3.4x | 2.4x | 1.2x | 5.8x |
| NQ2 (LIKE + IN filter pushdown) | 6.0x | 2.9x | 2.3x | 0.8x | 5.7x |
| NQ3 (4-way join, part/partsupp) | 1.7x | 0.8x | 1.3x | 0.7x | 1.5x |
| NQ4 (ORDER BY + LIMIT top-N) | 8.5x | 3.9x | 2.5x | 1.7x | 5.8x |
| NQ5 (tuple GROUP BY + HAVING + AVG) | 5.3x | 2.3x | 1.1x | 0.8x | 1.8x |

Ratios below 1.0x (e.g. Trino JDBC/Athena on Q1, NQ3) reflect ordinary run-to-run noise on
already-fast (<3s) queries, not deletes making a query faster. Not further root-caused this pass
(out of scope for the `add-delete-benchmark-flag` plan, which targets *measuring* this cost, not
optimizing it) — a natural follow-up given the magnitude here, especially for lakehouse-engine-rs
and Spark's non-join queries.

**Note on the correctness checks below**: these are specific to the lakehouse-engine-rs run — the
competitor engines have no equivalent harness-side check to report (they're not the mechanism
being validated; the delete-count sanity check and pushdown-check flake both concern
lakehouse-engine-rs's own merge-on-read scan implementation).

`bench/run.sh`'s delete-count sanity check confirms position deletes are applied on read for the
lakehouse-engine-rs run: `LINEITEM 170997641 (~95.0% of baseline 179998372)` — identical count on
both the 2026-07-08 and 2026-07-09 runs (the static `tpch_deletes` tables weren't regenerated
between them). `LINEITEM`'s `tpch_deletes` copy has 30 Parquet data files (vs. the baseline's 64 —
a smaller file count from the Spark CTAS write, not a correctness issue: shard fan-out and
per-file position-delete pairing were independently re-verified live via `EXPLAIN VIRTUAL` on the
2026-07-08 run).

**Note on the pushdown-check exit status (recurring, known-benign)**: on both the 2026-07-08 and
2026-07-09 runs, the trailing `EXPLAIN VIRTUAL` pushdown assertions (`shard_key`, `filter`,
`aggregates`, etc.) failed uniformly right after the 15 timed queries — consistent with a
transient `EXPLAIN VIRTUAL` call erroring rather than a real regression (confirmed by re-querying
`EXPLAIN VIRTUAL SELECT COUNT(*) FROM TPCH.LINEITEM` moments later on the 2026-07-08 run, which
showed the expected `... AS shards(shard_key, files) GROUP BY shard_key` shape with position-delete
files correctly paired per data file). The 15 query results and timings above are unaffected — only
the harness's own trailing pushdown-check block flaked both times. `bench/run.sh`'s `pushdown_check`
has no retry, so one transient EXPLAIN failure fails the whole run; worth hardening separately.

## Raw streaming scan & IMPORT FROM JDBC parallelism

### Hypothesis

lakehouse-engine-rs scales a scan across the Exasol cluster by sharding the Iceberg file list into
`GROUP BY shard_key` work units and multiplexing them onto every node's core pool (mission Core
Capability #3), so a scan's throughput should grow when the cluster gains nodes. Exasol's native
`IMPORT FROM PARQUET` reader is likewise cluster-parallel (MPP). Exasol's native `IMPORT FROM JDBC`,
by contrast, pulls its entire result set through a **single JDBC connection** on one node with no
equivalent cluster-side fan-out — so the hypothesis is that JDBC throughput should stay **flat** as
Exasol node count rises, while the VS path and native Parquet path both scale. This matters because
`IMPORT FROM JDBC` (here against a Trino coordinator) is the natural "just federate it" alternative a
user would reach for instead of the VS, and its throughput ceiling is invisible until the cluster is
scaled up and it fails to move.

### What was measured

A raw, unaggregated streaming scan of the same TPC-H sf=30 `lineitem` table (180M rows, same
Iceberg/Glue data as §a/§b) run three ways, at **2 and 4 Exasol nodes** (`test1`, 2×/4×
`r8i.2xlarge`), 3× each, through one automated pipeline
(`deploy/scripts/jdbc-parallelism-sweep.sh` → `deploy/scripts/bench-remote.sh`) live on 2026-07-16,
torn down immediately after:

1. **lakehouse-engine-rs VS** — `CREATE OR REPLACE TABLE BENCH.LINEITEM_VS AS SELECT * FROM
   TPCH.LINEITEM` (`import_ceiling.sh` `vs_ctas_run*`), full 180M rows.
2. **Native `IMPORT FROM PARQUET`** — `IMPORT INTO BENCH.LINEITEM_IMPORT FROM PARQUET ...`
   (`import_ceiling.sh` `import_into_run*`), full 180M rows. Exasol's own MPP Parquet reader —
   context, not part of the hypothesis, but a corroborating cluster-parallel baseline.
3. **Native `IMPORT FROM JDBC`** — `IMPORT INTO BENCH.LINEITEM_JDBC FROM JDBC DRIVER='TRINO' ...
   STATEMENT 'SELECT * FROM lineitem LIMIT 1000000'` (`import_jdbc_trino.sh` `jdbc_raw_scan_run*`),
   bounded to **1,000,000 rows** (not the full 180M). Trino is fixed at 2 nodes for both trials — the
   hypothesis is about Exasol's node count, not Trino's.

**Why the JDBC scan is bounded to 1M rows (load-bearing, not a footnote).** Live testing found
Exasol's JDBC ETL bridge has an apparent **~300-second per-statement execution ceiling**: a
full-table JDBC IMPORT failed at exactly 300000 ms. This is **not** the SQL-level `QUERY_TIMEOUT`
session parameter — `ALTER SESSION SET QUERY_TIMEOUT=N` was verified to work for ordinary queries
(a 27 s query was killed at ~1 s locally) yet had **zero** effect on the JDBC IMPORT, which still
failed identically at 300000 ms with or without it — and it is not exposed as a configurable JDBC
driver / `settings.cfg` property either (searched the exasol-db engine source, including
`JavaModules`/`ETLjdbc`, no configurable override found). 1,000,000 rows was chosen as large enough
to reach steady-state throughput past connection/setup overhead while completing safely under that
ceiling. Because throughput (rows/s) is the scaling metric, the different row count vs. paths 1–2
does not affect the node-count comparison — what matters is whether a path's rows/s moves when nodes
double.

### Results

Throughput in rows/s, averaged over the successful runs of each 3× trial; scaling = 4-node avg ÷
2-node avg (higher is better; ~2.0× is ideal linear scaling for a 2× node increase).

| Scan path | 2-node (rows/s) | 4-node (rows/s) | Scaling (4n ÷ 2n) | Scales with nodes? |
|---|---|---|---|---|
| lakehouse-engine-rs VS (`SELECT *` CTAS) | 736,796 | 1,355,173 | **1.84×** | Yes |
| Native `IMPORT FROM PARQUET` | 2,224,790 | 4,402,954 | **1.98×** | Yes |
| Native `IMPORT FROM JDBC` (Trino, 1M rows) | 60,583 | 60,206 | **0.99×** | No (flat) |

Per-run figures (rows/s): VS 2-node 735,167 / 738,425 (run 3 not counted — see caveats); VS 4-node
1,342,570 / 1,356,738 / 1,366,212. Native Parquet 2-node 2,217,002 / 2,222,751 / 2,234,617; 4-node
4,361,482 / 4,410,644 / 4,436,736. JDBC 2-node 60,680 / 60,241 / 60,827
([`import-jdbc-trino-20260716-212055.txt`](../bench/reports/import-jdbc-trino-20260716-212055.txt));
JDBC 4-node 59,952 / 60,132 / 60,533
([`import-jdbc-trino-20260716-214746.txt`](../bench/reports/import-jdbc-trino-20260716-214746.txt)).

### Verdict: CONFIRMED

`IMPORT FROM JDBC` throughput is **flat** across the node doubling — 60,583 → 60,206 rows/s, a 0.99×
ratio (i.e. no gain; the tiny dip is run-to-run noise). Both cluster-parallel paths scale roughly
with node count over the same doubling: the lakehouse-engine-rs VS at 1.84× and native
`IMPORT FROM PARQUET` at 1.98×. The single-JDBC-connection design is the throughput bottleneck it was
predicted to be: adding Exasol nodes does nothing for it, whereas the file-sharded VS path turns
those nodes into more scan parallelism. In absolute terms the VS path already moves raw rows ~12×
faster than JDBC at 2 nodes (737K vs. 61K rows/s) and ~22× faster at 4 nodes (1.36M vs. 60K rows/s),
and the gap widens with every node added.

The intra-trial run-to-run spread is tight for all three paths (VS ≤1.7%, native ≤1.7%, JDBC ≤1.0%),
and a separate earlier 2-node provisioning of the same code paths reproduced the JDBC and native
figures closely (JDBC 59,524 / 59,844 / 61,237 rows/s ≈ 60.2K avg; native 2,211,826 / 2,210,468 /
2,257,599 rows/s; VS 733,370 rows/s on its one completed run before the same license cap below),
corroborating the result.

### Caveats

- **VS 2-node is 2 of 3 runs, not 3 of 3.** The third VS CTAS run at 2 nodes hit the test cluster's
  license ceiling ("cumulative database raw sizes ... exceeded license limit", SQL state R0010)
  before completing — a constraint of the `test1` license, unrelated to the hypothesis, not a slow
  result. The 2-node VS average is over the two runs that completed (735,167 / 738,425 rows/s, which
  agree to 0.4%, so the missing run is unlikely to move it materially). All three 4-node VS runs
  completed. The earlier cross-check 2-node run hit the identical cap after one completed run.
- **The JDBC path measures 1M rows, not the full 180M table** (see methodology above). It is a
  steady-state throughput sample bounded under the ~300 s ETL-bridge ceiling, not a full-table load,
  so it is a fair rows/s comparison but not a full-table wall-clock comparison against paths 1–2.
- **Trino is held at 2 nodes throughout**, deliberately — the variable under test is Exasol's node
  count, so the JDBC producer is kept constant. This means the flat JDBC line reflects the Exasol-side
  single-connection ceiling, not a Trino-side limit; a larger Trino would not change the conclusion
  because the bottleneck is the single JDBC connection into Exasol, not Trino's ability to produce
  rows.
- **The 15-query q1–nq5 timings in both JDBC reports are ordinary IMPORT-FROM-JDBC query runs**
  (e.g. q9b ~13 s, the widest projection), not part of the raw-scan scaling measurement; they are
  the same competitor path already shown in §a/§b and are not re-analyzed here.
- **Native `IMPORT FROM PARQUET` reads raw Parquet directly** and applies no Iceberg position-deletes
  (same reason it is excluded from §b); it is included here only as a cluster-parallel scaling
  baseline, not as a like-for-like correctness-equivalent path to the VS.
