[lakehouse-engine](../README.md) › [Docs](index.md) › Benchmark

---

# Benchmark queries

This page documents the **benchmark query set** so you can inspect and reproduce it. The suite is a
TPC-H-derived set of 15 queries, each written to exercise a specific pushdown path (projection,
filter, LIMIT, Top-N, single-group and GROUP BY aggregation, COUNT(DISTINCT), arithmetic-argument
aggregates, and 3- and 4-way joins). It runs the full VS query path against a live system.

For timings and scaling results, see [performance.md](performance.md). This page carries no numbers.

The canonical queries live in [`bench/run.sh`](../bench/run.sh). Dialect-translated copies for
cross-engine comparison exist in `bench/athena_compare.sh`, `bench/trino_compare.sh`,
`bench/import_jdbc_trino.sh`, and `deploy/scripts/spark_queries.py`; the VS-dialect versions below
are the source of truth. `${VS}` is the virtual schema name (the bench creates it as `TPCH`).

## Running it yourself

```bash
make bench                  # build the .so, run the suite, write bench/reports/<name>-<ts>.txt
./bench/run.sh selftest      # offline self-check of the script's string logic (no DB needed)
```

Configuration comes from a gitignored `bench/.env` (copy `bench/.env.example`). `BENCH_TARGET`
picks the mode and defaults to `docker`:

- **`docker` (default)** - self-contained. `docker compose up -d` brings up MinIO, an Iceberg REST
  catalog, and Exasol; TPC-H is loaded into the local catalog by a cargo test binary
  (`tpch_loader.rs`). No AWS and no `.env` are needed.
- **`remote`** - runs against a real AWS Glue catalog and an external Exasol cluster. TPC-H must be
  pre-loaded into Glue by the operator; remote mode never loads data. Required `.env` variables:

  ```
  AWS_REGION, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY,
  GLUE_CATALOG_URI, GLUE_WAREHOUSE, ICEBERG_NAMESPACE,
  EXASOL_HOST, EXASOL_SYS_PASSWORD, BUCKETFS_WRITE_PASS
  ```

Other knobs include `BENCH_WITH_DELETES` (re-run against 5%-position-deleted Iceberg v2
merge-on-read copies), `BENCH_NR_OF_CORES`, `BENCH_PARALLELISM_FACTOR`, the `BENCH_DF_*` DataFusion
threading knobs, and `BENCH_S3_MAX_CONNECTIONS`. For the full knob reference and how to interpret a
run's output, see [`bench/README.md`](../bench/README.md).

Each run writes a timestamped report to `bench/reports/<name>-<ts>.txt`. Those reports are
gitignored and never committed; run the suite yourself to produce your own.

## Query catalog

Two groups: `Q1`-`Q9b` cover the core join and aggregate shapes; `NQ1`-`NQ5` probe specific
pushdown targets (arithmetic aggregates, LIKE/IN filters, Top-N, a 4-way join, and GROUP BY +
HAVING).

### Q1 - supplier × nation × region (wiring check)

3-way join and small-table sanity check.

```sql
SELECT n.N_NAME, r.R_NAME, COUNT(*) AS suppliers
FROM ${VS}.SUPPLIER s
JOIN ${VS}.NATION n ON s.S_NATIONKEY = n.N_NATIONKEY
JOIN ${VS}.REGION r ON n.N_REGIONKEY = r.R_REGIONKEY
GROUP BY n.N_NAME, r.R_NAME
ORDER BY n.N_NAME;
```

### Q2 - customer × orders × lineitem (big 3-way scan)

Full 3-way join across the largest tables; pure join throughput.

```sql
SELECT COUNT(*) AS rows_joined
FROM ${VS}.CUSTOMER c
JOIN ${VS}.ORDERS o   ON c.C_CUSTKEY  = o.O_CUSTKEY
JOIN ${VS}.LINEITEM l ON o.O_ORDERKEY = l.L_ORDERKEY;
```

### Q3 - orders × lineitem + filter + GROUP BY

Join, date-range filter, and GROUP BY together.

```sql
SELECT o.O_ORDERPRIORITY, COUNT(*) AS cnt, SUM(l.L_EXTENDEDPRICE) AS revenue
FROM ${VS}.ORDERS o
JOIN ${VS}.LINEITEM l ON o.O_ORDERKEY = l.L_ORDERKEY
WHERE o.O_ORDERDATE >= DATE '1994-01-01' AND o.O_ORDERDATE < DATE '1995-01-01'
GROUP BY o.O_ORDERPRIORITY
ORDER BY o.O_ORDERPRIORITY;
```

### Q4 - lineitem pricing summary (TPC-H Q1 shape)

Canonical TPC-H Q1; multi-column aggregate pushdown.

```sql
SELECT L_RETURNFLAG, L_LINESTATUS,
       SUM(L_QUANTITY) AS sum_qty, SUM(L_EXTENDEDPRICE) AS sum_base_price,
       AVG(L_DISCOUNT) AS avg_disc, COUNT(*) AS count_order
FROM ${VS}.LINEITEM
WHERE L_SHIPDATE <= DATE '1998-09-01'
GROUP BY L_RETURNFLAG, L_LINESTATUS
ORDER BY L_RETURNFLAG, L_LINESTATUS;
```

### Q5 - orders × lineitem GROUP BY, no filter

Q3 without the `WHERE`; isolates the filter pushdown's contribution against Q3.

```sql
SELECT o.O_ORDERPRIORITY, COUNT(*) AS cnt, SUM(l.L_EXTENDEDPRICE) AS revenue
FROM ${VS}.ORDERS o
JOIN ${VS}.LINEITEM l ON o.O_ORDERKEY = l.L_ORDERKEY
GROUP BY o.O_ORDERPRIORITY
ORDER BY o.O_ORDERPRIORITY;
```

### Q6 - lineitem pricing summary, no filter

Q4 without the `WHERE`; an unfiltered aggregate scan over the full table.

```sql
SELECT L_RETURNFLAG, L_LINESTATUS,
       SUM(L_QUANTITY) AS sum_qty, SUM(L_EXTENDEDPRICE) AS sum_base_price,
       AVG(L_DISCOUNT) AS avg_disc, COUNT(*) AS count_order
FROM ${VS}.LINEITEM
GROUP BY L_RETURNFLAG, L_LINESTATUS
ORDER BY L_RETURNFLAG, L_LINESTATUS;
```

### Q7 - high-cardinality GROUP BY

A GROUP BY with roughly 45M distinct groups; stresses the aggregate and shuffle path.

```sql
SELECT COUNT(*) FROM (
  SELECT L_ORDERKEY, COUNT(*) AS cnt
  FROM ${VS}.LINEITEM
  GROUP BY L_ORDERKEY
) t;
```

### Q8 - highly selective filter

A single-day equality filter matching under 0.05% of rows; filter-pushdown selectivity.

```sql
SELECT COUNT(*) FROM ${VS}.LINEITEM WHERE L_SHIPDATE = DATE '1995-06-15';
```

### Q9a - narrow projection

Single-column full scan; minimal projection width.

```sql
SELECT SUM(L_QUANTITY) FROM ${VS}.LINEITEM;
```

### Q9b - wide projection + expression aggregates + COUNT(DISTINCT)

All 16 lineitem columns, expression-argument aggregates, and COUNT(DISTINCT) pushdown in one query.

```sql
SELECT COUNT(*),
       SUM(L_ORDERKEY), SUM(L_PARTKEY), SUM(L_SUPPKEY), SUM(L_LINENUMBER),
       SUM(L_QUANTITY), SUM(L_EXTENDEDPRICE), SUM(L_DISCOUNT), SUM(L_TAX),
       COUNT(DISTINCT L_RETURNFLAG), COUNT(DISTINCT L_LINESTATUS),
       MIN(L_SHIPDATE), MAX(L_COMMITDATE), MIN(L_RECEIPTDATE),
       COUNT(DISTINCT L_SHIPINSTRUCT), COUNT(DISTINCT L_SHIPMODE),
       SUM(LENGTH(L_COMMENT))
FROM ${VS}.LINEITEM;
```

### NQ1 - arithmetic aggregate pushdown (TPC-H Q6 shape)

SUM over a binary-arithmetic expression, with a BETWEEN and range filters.

```sql
SELECT SUM(L_EXTENDEDPRICE * L_DISCOUNT) AS revenue
FROM ${VS}.LINEITEM
WHERE L_SHIPDATE >= DATE '1994-01-01' AND L_SHIPDATE < DATE '1995-01-01'
  AND L_DISCOUNT BETWEEN 0.05 AND 0.07 AND L_QUANTITY < 24;
```

### NQ2 - LIKE + IN filter

IN-list and LIKE pattern-match pushdown.

```sql
SELECT COUNT(*) FROM ${VS}.LINEITEM
WHERE L_SHIPMODE IN ('AIR','REG AIR') AND L_COMMENT LIKE '%late%';
```

### NQ3 - part × partsupp × supplier × nation (4-way join + filter)

4-way join with mixed equality and LIKE filters.

```sql
SELECT COUNT(*) AS cnt, SUM(ps.PS_SUPPLYCOST) AS total_cost
FROM ${VS}.PART p
JOIN ${VS}.PARTSUPP ps ON p.P_PARTKEY = ps.PS_PARTKEY
JOIN ${VS}.SUPPLIER s  ON ps.PS_SUPPKEY = s.S_SUPPKEY
JOIN ${VS}.NATION n    ON s.S_NATIONKEY = n.N_NATIONKEY
WHERE p.P_SIZE = 15 AND p.P_TYPE LIKE '%BRASS%' AND n.N_NAME = 'GERMANY';
```

### NQ4 - Top-N (ORDER BY + LIMIT)

Top-N pushdown: a per-shard bounded sort merged Exasol-side.

```sql
SELECT L_ORDERKEY, L_EXTENDEDPRICE
FROM ${VS}.LINEITEM
ORDER BY L_EXTENDEDPRICE DESC
LIMIT 20;
```

### NQ5 - GROUP BY + HAVING

High-cardinality group filter with AVG.

```sql
SELECT O_ORDERPRIORITY, O_ORDERSTATUS, COUNT(*) AS cnt, AVG(O_TOTALPRICE) AS avg_price
FROM ${VS}.ORDERS
GROUP BY O_ORDERPRIORITY, O_ORDERSTATUS
HAVING COUNT(*) > 1000000
ORDER BY O_ORDERPRIORITY, O_ORDERSTATUS;
```

## Example remote catalog config

For `BENCH_TARGET=remote`, `bench/.env` points the suite at an AWS Glue Iceberg REST catalog and an
external Exasol cluster. The values below are placeholders; substitute your own. `GLUE_WAREHOUSE` is
the Glue catalog id (your AWS account id), not an `s3://` path.

```bash
BENCH_TARGET=remote

# AWS Glue Iceberg REST catalog + S3
AWS_REGION=us-east-1
AWS_ACCESS_KEY_ID=<your-access-key-id>
AWS_SECRET_ACCESS_KEY=<your-secret-access-key>
# AWS_SESSION_TOKEN=<optional-sts-token>
GLUE_CATALOG_URI=https://glue.us-east-1.amazonaws.com/iceberg
GLUE_WAREHOUSE=123456789012          # Glue catalog id (AWS account id), NOT an s3:// path
ICEBERG_NAMESPACE=tpch               # namespace holding the TPC-H tables
# AWS_S3_ENDPOINT=                   # default https://s3.$AWS_REGION.amazonaws.com

# External Exasol cluster
EXASOL_HOST=<your-exasol-host>
LH_EXASOL_PORT=28563
LH_BUCKETFS_PORT=22581
EXASOL_SYS_PASSWORD=<your-sys-password>
BUCKETFS_WRITE_PASS=<your-bucketfs-write-password>
```

If your deployment reaches the cluster over SSH, use a generic profile and key placeholder such as
`<your-aws-profile>` and `<your-key-file>`; never commit real credentials or account ids.

---

Historical numeric results (timings, scaling, overhead) live in
[performance.md](performance.md) and are not reproduced here.
