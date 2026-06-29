#!/usr/bin/env python3
"""Generate TPC-H + wide perf tables with DuckDB and write them as Iceberg tables.

Runs on the temporary data-gen EC2 (instance role provides AWS creds). Writes to the AWS Glue
catalog so both the lakehouse engine (via Glue's Iceberg REST endpoint) and Athena can query them.

  python gen_load.py --region eu-west-1 --warehouse <account_id> \
    --glue-uri https://glue.eu-west-1.amazonaws.com/iceberg \
    --bucket spot-strata-data-lakehouse-<acct> \
    --tpch-db tpch --perf-db perf \
    --tpch-scale 30 --lineitem-files 20 --perf-sizes 10,20,30,40,80 --perf-files 8

  python gen_load.py --self-check        # offline: tiny data into a local sqlite Iceberg catalog

Deps: duckdb, pyiceberg[glue,pyarrow], pyarrow, boto3.
"""
import argparse
import os
import sys
import tempfile

import duckdb
import pyarrow.parquet as pq

# 20-column wide perf schema (varied typical types). Generated from a row index `i`.
PERF_SELECT = """
SELECT
  i                                                            AS id,
  (i % 1000)::INTEGER                                          AS c_int1,
  ((i * 7) % 50000)::INTEGER                                   AS c_int2,
  ((i * 13) % 100)::SMALLINT                                   AS c_int3,
  (i % 7)::INTEGER                                             AS c_int4,
  (random() * 1e6)::BIGINT                                     AS c_bigint1,
  ((i * 3) % 1000000)::BIGINT                                  AS c_bigint2,
  random()                                                     AS c_double1,
  (random() * 1000)::DOUBLE                                    AS c_double2,
  (random() * 100)::FLOAT                                      AS c_float,
  round((random() * 9999)::DECIMAL(10,2), 2)                   AS c_dec1,
  round((random() * 1e6)::DECIMAL(18,4), 4)                    AS c_dec2,
  'str_' || (i % 10000)::VARCHAR                               AS c_str1,
  md5(i::VARCHAR)                                              AS c_str2,
  'cat_' || (i % 50)::VARCHAR                                  AS c_str3,
  repeat('x', 20)                                              AS c_str4,
  'region_' || (i % 5)::VARCHAR                                AS c_region,
  (DATE '2020-01-01' + (i % 2000)::INTEGER)                    AS c_date,
  (TIMESTAMP '2020-01-01 00:00:00' + ((i % 100000)::INTEGER * INTERVAL 1 SECOND)) AS c_ts,
  (i % 2 = 0)                                                  AS c_bool
FROM range({start}, {end}) t(i)
"""

TPCH_TABLES = ["region", "nation", "supplier", "customer", "part", "partsupp", "orders", "lineitem"]
TPCH_BIG = {"lineitem", "orders"}


def build_catalog(args):
    """Glue catalog for the real run; a local sqlite catalog for --self-check."""
    from pyiceberg.catalog import load_catalog
    if args.self_check:
        warehouse = os.path.join(args.workdir, "warehouse")
        os.makedirs(warehouse, exist_ok=True)
        return load_catalog("local", **{
            "type": "sql",
            "uri": f"sqlite:///{os.path.join(args.workdir, 'catalog.db')}",
            "warehouse": f"file://{warehouse}",
        })
    return load_catalog("glue", **{
        "type": "glue",
        "glue.region": args.region,
        "s3.region": args.region,
        "warehouse": f"s3://{args.bucket}/",
    })


def ensure_namespace(catalog, ns):
    try:
        catalog.create_namespace(ns)
    except Exception:
        pass  # already exists (Glue dbs are created by OpenTofu; sqlite needs this)


def recreate_table(catalog, ident, schema):
    try:
        catalog.drop_table(ident)
    except Exception:
        pass
    return catalog.create_table(ident, schema=schema)


def write_in_slices(table, arrow_table, n_files):
    """Append the arrow table as >= n_files data files (drives shard fan-out)."""
    n = max(1, n_files)
    rows = arrow_table.num_rows
    if rows == 0:
        table.append(arrow_table)
        return
    step = max(1, (rows + n - 1) // n)
    for start in range(0, rows, step):
        table.append(arrow_table.slice(start, min(step, rows - start)))


def gen_tpch(con, catalog, db, scale, n_files):
    con.execute("INSTALL tpch; LOAD tpch;")
    con.execute(f"CALL dbgen(sf={scale});")
    ensure_namespace(catalog, (db,))
    for tbl in TPCH_TABLES:
        arrow = con.execute(f"SELECT * FROM {tbl}").to_arrow_table()
        t = recreate_table(catalog, (db, tbl), arrow.schema)
        write_in_slices(t, arrow, n_files if tbl in TPCH_BIG else 1)
        print(f"  tpch.{tbl}: {arrow.num_rows} rows", flush=True)


def bytes_per_row(con, sample=200_000):
    """Calibrate compressed parquet bytes/row for the perf schema (compression varies)."""
    sql = PERF_SELECT.format(start=0, end=sample)
    arrow = con.execute(sql).to_arrow_table()
    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as f:
        pq.write_table(arrow, f.name, compression="snappy")
        size = os.path.getsize(f.name)
    os.unlink(f.name)
    return max(1.0, size / sample), arrow.schema


def gen_perf(con, catalog, db, sizes_gb, n_files):
    ensure_namespace(catalog, (db,))
    bpr, schema = bytes_per_row(con)
    print(f"  perf calibration: ~{bpr:.1f} bytes/row", flush=True)
    for gb in sizes_gb:
        total_rows = int(gb * 1e9 / bpr)
        ident = (db, f"t_{int(gb)}g")
        t = recreate_table(catalog, ident, schema)
        n = max(1, n_files)
        step = max(1, (total_rows + n - 1) // n)
        written = 0
        for start in range(0, total_rows, step):
            end = min(start + step, total_rows)
            arrow = con.execute(PERF_SELECT.format(start=start, end=end)).to_arrow_table()
            t.append(arrow)          # one data file per chunk; memory bounded by chunk size
            written += arrow.num_rows
        print(f"  perf.{ident[1]}: {written} rows (~{gb} GB)", flush=True)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--region")
    ap.add_argument("--warehouse")
    ap.add_argument("--glue-uri")
    ap.add_argument("--bucket")
    ap.add_argument("--tpch-db", default="tpch")
    ap.add_argument("--perf-db", default="perf")
    ap.add_argument("--tpch-scale", type=float, default=30)
    ap.add_argument("--lineitem-files", type=int, default=20)
    ap.add_argument("--perf-sizes", default="10,20,30,40,80")
    ap.add_argument("--perf-files", type=int, default=8)
    ap.add_argument("--self-check", action="store_true")
    ap.add_argument("--workdir", default=tempfile.mkdtemp(prefix="genload-"))
    args = ap.parse_args()

    con = duckdb.connect()
    con.execute(f"PRAGMA threads={os.cpu_count() or 4};")
    catalog = build_catalog(args)

    if args.self_check:
        return self_check(con, catalog)

    sizes = [float(s) for s in args.perf_sizes.split(",") if s.strip()]
    print(f"Generating TPC-H sf={args.tpch_scale} -> glue:{args.tpch_db}", flush=True)
    gen_tpch(con, catalog, args.tpch_db, args.tpch_scale, args.lineitem_files)
    print(f"Generating perf {sizes} GB -> glue:{args.perf_db}", flush=True)
    gen_perf(con, catalog, args.perf_db, sizes, args.perf_files)
    print("DONE", flush=True)


def self_check(con, catalog):
    """Offline: tiny tpch + a tiny perf table into a local sqlite Iceberg catalog; assert round-trip."""
    gen_tpch(con, catalog, "tpch", scale=0.01, n_files=3)
    # tiny perf table (~50k rows), reuse the slicing path
    ensure_namespace(catalog, ("perf",))
    arrow = con.execute(PERF_SELECT.format(start=0, end=50_000)).to_arrow_table()
    assert len(arrow.schema) == 20, f"perf schema must be 20 cols, got {len(arrow.schema)}"
    t = recreate_table(catalog, ("perf", "t_tiny"), arrow.schema)
    write_in_slices(t, arrow, 4)

    # read back via the catalog and assert counts
    n_perf = catalog.load_table(("perf", "t_tiny")).scan().to_arrow().num_rows
    n_region = catalog.load_table(("tpch", "region")).scan().to_arrow().num_rows
    assert n_perf == 50_000, f"perf readback {n_perf} != 50000"
    assert n_region == 5, f"tpch.region readback {n_region} != 5"
    # >=4 data files were requested for the perf table
    files = list(catalog.load_table(("perf", "t_tiny")).scan().plan_files())
    assert len(files) >= 4, f"expected >=4 perf data files, got {len(files)}"
    print(f"SELF-CHECK OK: perf={n_perf} rows in {len(files)} files, tpch.region={n_region} rows")


if __name__ == "__main__":
    sys.exit(main())
