#!/usr/bin/env python3
"""Generate TPC-H + wide perf + bronze ERP tables with DuckDB and write them as Iceberg tables.

Runs on the temporary data-gen EC2 (instance role provides AWS creds). Writes to the AWS Glue
catalog so both the lakehouse engine (via Glue's Iceberg REST endpoint) and Athena can query them.

  python gen_load.py --region eu-west-1 --warehouse <account_id> \
    --glue-uri https://glue.eu-west-1.amazonaws.com/iceberg \
    --bucket spot-strata-data-lakehouse-<acct> \
    --tpch-db tpch --perf-db perf --erp-db erp \
    --tpch-scale 30 --lineitem-files 20 --perf-sizes 10,20,30,40,80 --perf-files 8 \
    --erp-customers 75000 --erp-products 60000 --erp-orders 250000 --erp-invoices 100000

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

# Bronze ERP dataset: every "real world messy" column is deliberately a STRING, even the ones that
# are conceptually a date/number/boolean — same idea as a real dirty ERP extract, cleaned up later
# in a downstream silver layer. i % 3 cycles through 3 dirty variants per column so no format
# happens to dominate. FK columns reference the other tables' id format directly (no join needed,
# same style as PERF_SELECT's pure range()-driven generation).

ERP_CUSTOMERS_SELECT = """
SELECT
  'CUST' || lpad(i::VARCHAR, 8, '0')                                             AS customer_id,
  'Customer ' || i::VARCHAR                                                      AS name,
  to_json(struct_pack(
      email   := 'customer' || i::VARCHAR || '@example.com',
      phone   := '+1-555-' || lpad((i % 10000)::VARCHAR, 4, '0'),
      address := struct_pack(
          street := (i % 9999)::VARCHAR || ' Main St',
          city   := (['Springfield', 'Riverside', 'Franklin', 'Georgetown', 'Clinton'])[(i % 5) + 1],
          zip    := lpad((10000 + i % 90000)::VARCHAR, 5, '0')
      )
  ))                                                                             AS contact_json,
  (['Retail', 'retail', 'RETAIL', 'Whsle', 'Wholesale'])[(i % 5) + 1]            AS segment,
  CASE i % 3
    WHEN 0 THEN strftime(DATE '2019-01-01' + (i % 2200)::INTEGER, '%Y-%m-%d')
    WHEN 1 THEN strftime(DATE '2019-01-01' + (i % 2200)::INTEGER, '%m/%d/%Y')
    ELSE (epoch_ms((DATE '2019-01-01' + (i % 2200)::INTEGER)::TIMESTAMP))::VARCHAR
  END                                                                             AS signup_date,
  CASE i % 3
    WHEN 0 THEN '$' || round(500 + (i % 50000) / 10.0, 2)::VARCHAR
    WHEN 1 THEN round(500 + (i % 50000) / 10.0, 2)::VARCHAR
    ELSE replace(round(500 + (i % 50000) / 10.0, 2)::VARCHAR, '.', ',')
  END                                                                             AS credit_limit,
  CASE i % 3
    WHEN 0 THEN (CASE WHEN i % 2 = 0 THEN 'Y' ELSE 'N' END)
    WHEN 1 THEN (CASE WHEN i % 2 = 0 THEN '1' ELSE '0' END)
    ELSE (CASE WHEN i % 2 = 0 THEN 'true' ELSE 'false' END)
  END                                                                             AS is_active
FROM range({start}, {end}) t(i)
"""

ERP_PRODUCTS_SELECT = """
SELECT
  'SKU-' || lpad(i::VARCHAR, 5, '0')                                             AS product_id,
  'Product ' || i::VARCHAR                                                       AS name,
  (['Electronics', 'Home', 'Outdoor', 'Apparel', 'Toys'])[(i % 5) + 1]           AS category,
  CASE i % 3
    WHEN 0 THEN '$' || round(5 + (i % 20000) / 10.0, 2)::VARCHAR
    WHEN 1 THEN round(5 + (i % 20000) / 10.0, 2)::VARCHAR
    ELSE replace(round(5 + (i % 20000) / 10.0, 2)::VARCHAR, '.', ',')
  END                                                                             AS price,
  to_json(struct_pack(
      weight_kg  := round(0.1 + (i % 500) / 10.0, 2),
      dimensions := struct_pack(l := (i % 50) + 1, w := (i % 40) + 1, h := (i % 30) + 1),
      tags       := list_value((['electronics', 'clearance', 'new', 'sale', 'featured'])[(i % 5) + 1],
                                (['bestseller', 'limited', 'imported', 'eco', 'refurbished'])[(i % 5) + 1])
  ))                                                                             AS attributes_json,
  (['electronics', 'clearance', 'new'])[(i % 3) + 1] || ',' ||
    (['sale', 'featured', 'bestseller'])[(i % 3) + 1]                            AS tags_csv,
  CASE i % 3
    WHEN 0 THEN strftime(DATE '2018-01-01' + (i % 2800)::INTEGER, '%Y-%m-%d')
    WHEN 1 THEN strftime(DATE '2018-01-01' + (i % 2800)::INTEGER, '%m/%d/%Y')
    ELSE (epoch_ms((DATE '2018-01-01' + (i % 2800)::INTEGER)::TIMESTAMP))::VARCHAR
  END                                                                             AS created_at
FROM range({start}, {end}) t(i)
"""

# Two line items per order, deterministic product refs (mod n_products keeps them valid ids).
ERP_ORDERS_SELECT = """
SELECT
  'ORD' || lpad(i::VARCHAR, 8, '0')                                              AS order_id,
  'CUST' || lpad(((i * 7) % {n_customers})::VARCHAR, 8, '0')                     AS customer_id,
  CASE i % 3
    WHEN 0 THEN strftime(DATE '2022-01-01' + (i % 900)::INTEGER, '%Y-%m-%d')
    WHEN 1 THEN strftime(DATE '2022-01-01' + (i % 900)::INTEGER, '%m/%d/%Y')
    ELSE (epoch_ms((DATE '2022-01-01' + (i % 900)::INTEGER)::TIMESTAMP))::VARCHAR
  END                                                                             AS order_date,
  (['shipped', 'Shipped', 'SHIPPED', 'cancelled', 'Cancelled'])[(i % 5) + 1]     AS status,
  'SKU-' || lpad(((i * 11) % {n_products})::VARCHAR, 5, '0') || ':' ||
    ((i % 5) + 1)::VARCHAR || ':' || round(10 + (i % 990) / 10.0, 2)::VARCHAR || ';' ||
  'SKU-' || lpad(((i * 17 + 3) % {n_products})::VARCHAR, 5, '0') || ':' ||
    ((i % 3) + 1)::VARCHAR || ':' || round(5 + (i % 490) / 10.0, 2)::VARCHAR      AS line_items_csv,
  to_json(struct_pack(
      address := (i % 9999)::VARCHAR || ' Oak Ave',
      carrier := (['UPS', 'FedEx', 'DHL'])[(i % 3) + 1],
      tracking := 'TRK' || lpad(i::VARCHAR, 10, '0')
  ))                                                                             AS shipping_json,
  CASE i % 3
    WHEN 0 THEN '$' || round(50 + (i % 100000) / 10.0, 2)::VARCHAR
    WHEN 1 THEN round(50 + (i % 100000) / 10.0, 2)::VARCHAR
    ELSE replace(round(50 + (i % 100000) / 10.0, 2)::VARCHAR, '.', ',')
  END                                                                             AS total_amount
FROM range({start}, {end}) t(i)
"""

# order_index = (i*3) % n_orders is a bijection over 0..n_orders-1 (3 and n_orders are coprime for
# the default 250000), so sampling i in [0, n_invoices) with n_invoices < n_orders picks that many
# distinct orders with no collisions and leaves the rest uninvoiced (matches a real dirty source).
ERP_INVOICES_SELECT = """
SELECT
  'INV' || lpad(i::VARCHAR, 8, '0')                                              AS invoice_id,
  'ORD' || lpad(((i * 3) % {n_orders})::VARCHAR, 8, '0')                         AS order_id,
  CASE i % 3
    WHEN 0 THEN strftime(DATE '2022-01-05' + (i % 900)::INTEGER, '%Y-%m-%d')
    WHEN 1 THEN strftime(DATE '2022-01-05' + (i % 900)::INTEGER, '%m/%d/%Y')
    ELSE (epoch_ms((DATE '2022-01-05' + (i % 900)::INTEGER)::TIMESTAMP))::VARCHAR
  END                                                                             AS invoice_date,
  CASE i % 3
    WHEN 0 THEN '$' || round(50 + (i % 100000) / 10.0, 2)::VARCHAR
    WHEN 1 THEN round(50 + (i % 100000) / 10.0, 2)::VARCHAR
    ELSE replace(round(50 + (i % 100000) / 10.0, 2)::VARCHAR, '.', ',')
  END                                                                             AS amount_due,
  to_json(struct_pack(
      net_days     := ([15, 30, 45, 60])[(i % 4) + 1],
      discount_pct := round((i % 5) / 2.0, 1),
      method       := (['wire', 'card', 'ach'])[(i % 3) + 1]
  ))                                                                             AS payment_terms_json,
  CASE i % 3
    WHEN 0 THEN (CASE WHEN i % 2 = 0 THEN 'Y' ELSE 'N' END)
    WHEN 1 THEN (CASE WHEN i % 2 = 0 THEN '1' ELSE '0' END)
    ELSE (CASE WHEN i % 2 = 0 THEN 'true' ELSE 'false' END)
  END                                                                             AS paid_flag
FROM range({start}, {end}) t(i)
"""


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


def gen_erp(con, catalog, db, n_customers, n_products, n_orders, n_invoices):
    """Bronze ERP dataset: customers/products/orders/invoices, all dirty string columns."""
    if n_orders % 3 == 0:
        raise ValueError(
            f"--erp-orders={n_orders} is divisible by 3: invoices.order_id is derived as "
            "(i*3) % n_orders, which is only a collision-free permutation over 0..n_orders-1 "
            "when gcd(3, n_orders) == 1. Pick an --erp-orders not divisible by 3."
        )
    ensure_namespace(catalog, (db,))
    tables = [
        ("customers", ERP_CUSTOMERS_SELECT.format(start=0, end=n_customers)),
        ("products", ERP_PRODUCTS_SELECT.format(start=0, end=n_products)),
        ("orders", ERP_ORDERS_SELECT.format(start=0, end=n_orders,
                                             n_customers=n_customers, n_products=n_products)),
        ("invoices", ERP_INVOICES_SELECT.format(start=0, end=n_invoices, n_orders=n_orders)),
    ]
    for tbl, sql in tables:
        arrow = con.execute(sql).to_arrow_table()
        t = recreate_table(catalog, (db, tbl), arrow.schema)
        t.append(arrow)
        print(f"  erp.{tbl}: {arrow.num_rows} rows", flush=True)


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
    ap.add_argument("--erp-db", default="erp")
    ap.add_argument("--erp-customers", type=int, default=75_000)
    ap.add_argument("--erp-products", type=int, default=60_000)
    ap.add_argument("--erp-orders", type=int, default=250_000)
    ap.add_argument("--erp-invoices", type=int, default=100_000)
    ap.add_argument("--skip-tpch", action="store_true", help="Don't touch the tpch tables (they recreate-in-place).")
    ap.add_argument("--skip-perf", action="store_true", help="Don't touch the perf tables (they recreate-in-place).")
    ap.add_argument("--skip-erp", action="store_true", help="Don't touch the erp tables (they recreate-in-place).")
    ap.add_argument("--self-check", action="store_true")
    ap.add_argument("--workdir", default=tempfile.mkdtemp(prefix="genload-"))
    args = ap.parse_args()

    con = duckdb.connect()
    # DuckDB needs a home dir to install the tpch extension; $HOME is empty in cloud-init/root context.
    con.execute(f"SET home_directory='{args.workdir}';")
    con.execute(f"PRAGMA threads={os.cpu_count() or 4};")
    catalog = build_catalog(args)

    if args.self_check:
        return self_check(con, catalog)

    if not args.skip_tpch:
        print(f"Generating TPC-H sf={args.tpch_scale} -> glue:{args.tpch_db}", flush=True)
        gen_tpch(con, catalog, args.tpch_db, args.tpch_scale, args.lineitem_files)
    if not args.skip_perf:
        sizes = [float(s) for s in args.perf_sizes.split(",") if s.strip()]
        print(f"Generating perf {sizes} GB -> glue:{args.perf_db}", flush=True)
        gen_perf(con, catalog, args.perf_db, sizes, args.perf_files)
    if not args.skip_erp:
        print(f"Generating erp -> glue:{args.erp_db}", flush=True)
        gen_erp(con, catalog, args.erp_db, args.erp_customers, args.erp_products,
                args.erp_orders, args.erp_invoices)
    print("DONE", flush=True)


def self_check(con, catalog):
    """Offline: tiny tpch + a tiny perf table + a tiny erp dataset into a local sqlite Iceberg
    catalog; assert round-trip."""
    gen_tpch(con, catalog, "tpch", scale=0.01, n_files=3)
    # tiny perf table (~50k rows), reuse the slicing path
    ensure_namespace(catalog, ("perf",))
    arrow = con.execute(PERF_SELECT.format(start=0, end=50_000)).to_arrow_table()
    assert len(arrow.schema) == 20, f"perf schema must be 20 cols, got {len(arrow.schema)}"
    t = recreate_table(catalog, ("perf", "t_tiny"), arrow.schema)
    write_in_slices(t, arrow, 4)

    # tiny erp dataset (customers=50, products=40, orders=60, invoices=20)
    gen_erp(con, catalog, "erp", n_customers=50, n_products=40, n_orders=61, n_invoices=20)

    # read back via the catalog and assert counts
    n_perf = catalog.load_table(("perf", "t_tiny")).scan().to_arrow().num_rows
    n_region = catalog.load_table(("tpch", "region")).scan().to_arrow().num_rows
    n_erp_orders = catalog.load_table(("erp", "orders")).scan().to_arrow().num_rows
    n_erp_invoices = catalog.load_table(("erp", "invoices")).scan().to_arrow().num_rows
    assert n_perf == 50_000, f"perf readback {n_perf} != 50000"
    assert n_region == 5, f"tpch.region readback {n_region} != 5"
    assert n_erp_orders == 61, f"erp.orders readback {n_erp_orders} != 61"
    assert n_erp_invoices == 20, f"erp.invoices readback {n_erp_invoices} != 20"
    # >=4 data files were requested for the perf table
    files = list(catalog.load_table(("perf", "t_tiny")).scan().plan_files())
    assert len(files) >= 4, f"expected >=4 perf data files, got {len(files)}"
    print(f"SELF-CHECK OK: perf={n_perf} rows in {len(files)} files, tpch.region={n_region} rows, "
          f"erp.orders={n_erp_orders} rows, erp.invoices={n_erp_invoices} rows")


if __name__ == "__main__":
    sys.exit(main())
