[lakehouse-engine](../README.md) › [Docs](index.md) › Benchmark

---

# Benchmark queries

This page documents the **benchmark query set** so you can inspect and reproduce it. The suite is a
TPC-H-derived set of 15 queries. Each query exercises one specific pushdown path:

- projection
- filter
- LIMIT
- Top-N
- single-group aggregation
- GROUP BY aggregation
- COUNT(DISTINCT)
- arithmetic-argument aggregates
- 3-way and 4-way joins

The suite runs the full VS query path against a live system.

This page documents the query set only. It carries no timing or scaling numbers.

The canonical queries live in [`bench/run.sh`](../bench/run.sh). Dialect-translated copies exist for
comparison against other engines. `${VS}` is the virtual schema name. The bench creates it as
`TPCH`.

## Running it yourself

```bash
make bench                  # build the .so, run the suite, write bench/reports/<name>-<ts>.txt
./bench/run.sh selftest      # offline self-check of the script's string logic (no DB needed)
```

The configuration comes from a gitignored `bench/.env` file. Copy `bench/.env.example` to create it.
`BENCH_TARGET` selects the mode. The default is `docker`:

- **`docker` (default)** — self-contained. `docker compose up -d` starts MinIO, an Iceberg REST
  catalog, and Exasol. The bench loads TPC-H into the local catalog automatically. This mode needs
  no AWS account and no `.env` file.
- **`remote`** — runs against a real AWS Glue catalog and an external Exasol cluster. The operator
  must pre-load TPC-H into Glue. Remote mode never loads data. Required `.env` variables:

  ```
  AWS_REGION, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY,
  GLUE_CATALOG_URI, GLUE_WAREHOUSE, ICEBERG_NAMESPACE,
  EXASOL_HOST, EXASOL_SYS_PASSWORD, BUCKETFS_WRITE_PASS
  ```

Other configuration variables include:

- `BENCH_WITH_DELETES` — re-runs the suite against 5%-position-deleted Iceberg v2 merge-on-read
  copies
- `BENCH_NR_OF_CORES`
- `BENCH_PARALLELISM_FACTOR`
- the `BENCH_DF_*` DataFusion threading variables
- `BENCH_S3_MAX_CONNECTIONS`

For the full variable reference and instructions to read the output of a run, see
[`bench/README.md`](../bench/README.md).

Each run writes a timestamped report to `bench/reports/<name>-<ts>.txt`. The reports are gitignored
and never committed. Run the suite yourself to produce your own reports.

## Query catalog

The catalog has two groups. `Q1`-`Q9b` cover the core join and aggregate shapes. `NQ1`-`NQ5` test
specific pushdown targets:

- arithmetic aggregates
- LIKE and IN filters
- Top-N
- a 4-way join
- GROUP BY with HAVING

### Q1 - supplier × nation × region (wiring check)

3-way join and small-table check.

```sql
SELECT n.N_NAME, r.R_NAME, COUNT(*) AS suppliers
FROM ${VS}.SUPPLIER s
JOIN ${VS}.NATION n ON s.S_NATIONKEY = n.N_NATIONKEY
JOIN ${VS}.REGION r ON n.N_REGIONKEY = r.R_REGIONKEY
GROUP BY n.N_NAME, r.R_NAME
ORDER BY n.N_NAME;
```

### Q2 - customer × orders × lineitem (big 3-way scan)

Full 3-way join across the largest tables. This query measures join throughput only.

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

Canonical TPC-H Q1. It uses multi-column aggregate pushdown.

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

Q3 without the `WHERE` clause. A comparison with Q3 isolates the contribution of the filter
pushdown.

```sql
SELECT o.O_ORDERPRIORITY, COUNT(*) AS cnt, SUM(l.L_EXTENDEDPRICE) AS revenue
FROM ${VS}.ORDERS o
JOIN ${VS}.LINEITEM l ON o.O_ORDERKEY = l.L_ORDERKEY
GROUP BY o.O_ORDERPRIORITY
ORDER BY o.O_ORDERPRIORITY;
```

### Q6 - lineitem pricing summary, no filter

Q4 without the `WHERE` clause. It is an unfiltered aggregate scan over the full table.

```sql
SELECT L_RETURNFLAG, L_LINESTATUS,
       SUM(L_QUANTITY) AS sum_qty, SUM(L_EXTENDEDPRICE) AS sum_base_price,
       AVG(L_DISCOUNT) AS avg_disc, COUNT(*) AS count_order
FROM ${VS}.LINEITEM
GROUP BY L_RETURNFLAG, L_LINESTATUS
ORDER BY L_RETURNFLAG, L_LINESTATUS;
```

### Q7 - high-cardinality GROUP BY

A GROUP BY with approximately 45M distinct groups. It puts load on the aggregate and shuffle path.

```sql
SELECT COUNT(*) FROM (
  SELECT L_ORDERKEY, COUNT(*) AS cnt
  FROM ${VS}.LINEITEM
  GROUP BY L_ORDERKEY
) t;
```

### Q8 - highly selective filter

A single-day equality filter that matches less than 0.05% of the rows. It measures filter-pushdown
selectivity.

```sql
SELECT COUNT(*) FROM ${VS}.LINEITEM WHERE L_SHIPDATE = DATE '1995-06-15';
```

### Q9a - narrow projection

Single-column full scan with the minimum projection width.

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

Top-N pushdown. Each shard does a bounded sort, and Exasol merges the results.

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

## Example remote catalog configuration

For `BENCH_TARGET=remote`, `bench/.env` points the suite at an AWS Glue Iceberg REST catalog and an
external Exasol cluster. The values below are placeholders. Replace them with your own values.
`GLUE_WAREHOUSE` is the Glue catalog id, which is your AWS account id. It is not an `s3://` path.

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

If your deployment reaches the cluster over SSH, use generic profile and key placeholders such as
`<your-aws-profile>` and `<your-key-file>`. Never commit real credentials or account ids.

---

Run the suite with `make bench` to produce timing, scaling, and overhead numbers for your own
environment. This page publishes no such numbers.
