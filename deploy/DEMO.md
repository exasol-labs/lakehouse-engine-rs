# Live demo script — Lakekeeper-backed TPC-H

A standalone, presenter-facing script for a live customer demo of the lakehouse engine against the
opt-in Lakekeeper catalog. Keep this document open during the demo — it has every command and query
you need, in order, so there's no need to switch over to `deploy/README.md` mid-session.

`deploy/README.md`'s "Lakekeeper (ephemeral, opt-in)" section is the operational reference: it
explains WHY these commands are in this order (the `bench-remote.sh` teardown trap, the
`BENCH_CATALOG=lakekeeper` requirement, the `lakekeeper-up.sh`-before-`secrets.sh` ordering) and
carries the unwrapped, fine-grained-control alternative. This document does not repeat any of that —
if a command below needs explaining beyond its own line, that section has the explanation.

## Setup

Run these in order, from the repo root. This is the WRAPPER form from `deploy/README.md`'s Demo
runbook — the default path:

```bash
AWS_PROFILE=spot-strata-deployer deploy/scripts/lakekeeper-up.sh myenv
AWS_PROFILE=spot-strata-deployer BENCH_CATALOG=lakekeeper KEEP_ALIVE=1 deploy/scripts/bench-remote.sh myenv
```

The first command stands up the ephemeral Lakekeeper box and registers the eight TPC-H tables into
it. The second applies the Exasol cluster, wires `bench/.env`, and runs the full timed query suite
once against the Lakekeeper-backed virtual schema — `KEEP_ALIVE=1` is what keeps the cluster (and
your demo) alive afterwards. When it finishes, the `TPCH` virtual schema and its `CONNECTION` are
live and queryable — continue straight into the queries below.

## Live queries

A short, curated set — picked for a clear, fast answer on stage, not `make bench`'s full 15-query
timed sweep. Run each from a SQL client connected to `EXASOL_HOST`/`LH_EXASOL_PORT` (from the
`bench/.env` the setup step above just wrote).

**1. Prove it's live — a three-way join across the small dimension tables**

```sql
SELECT n.N_NAME, r.R_NAME, COUNT(*) AS suppliers
FROM TPCH.SUPPLIER s
JOIN TPCH.NATION n ON s.S_NATIONKEY = n.N_NATIONKEY
JOIN TPCH.REGION r ON n.N_REGIONKEY = r.R_REGIONKEY
GROUP BY n.N_NAME, r.R_NAME
ORDER BY n.N_NAME;
```

*Talking point: this is a real multi-table JOIN answered instantly straight out of the
Lakekeeper-backed catalog — the engine treats it exactly like Glue, because it's the same query
path and the same physical TPC-H data, just a different catalog underneath.*

**2. A boardroom number — revenue impact of a discount band**

```sql
SELECT SUM(L_EXTENDEDPRICE * L_DISCOUNT) AS revenue
FROM TPCH.LINEITEM
WHERE L_SHIPDATE >= DATE '1994-01-01' AND L_SHIPDATE < DATE '1995-01-01'
  AND L_DISCOUNT BETWEEN 0.05 AND 0.07 AND L_QUANTITY < 24;
```

*Talking point: one number, computed by pushing the whole filter-and-multiply-and-sum expression
down into the DataFusion scan running on each Exasol node — Exasol never pulls a raw row across
the wire to do this arithmetic itself.*

**3. Needle in a haystack — a highly selective filter over 180M+ rows**

```sql
SELECT COUNT(*) FROM TPCH.LINEITEM WHERE L_SHIPDATE = DATE '1995-06-15';
```

*Talking point: against a table with 180 million rows, the filter is pushed down and the scan
skips almost everything rather than reading the table and filtering afterwards.*

**4. Top 20 by value — ORDER BY + LIMIT, not a full sort**

```sql
SELECT L_ORDERKEY, L_EXTENDEDPRICE FROM TPCH.LINEITEM
ORDER BY L_EXTENDEDPRICE DESC LIMIT 20;
```

*Talking point: each node computes its own bounded top-20 and Exasol merges those small partial
results — nobody sorts the whole table to answer a top-20 question.*

## Teardown

Close every demo with both of these, in either order — don't leave the session without running
them:

```bash
AWS_PROFILE=spot-strata-deployer deploy/scripts/cluster-down.sh myenv
AWS_PROFILE=spot-strata-deployer deploy/scripts/lakekeeper-down.sh myenv
```

Both boxes bill for as long as they exist. Verify via `aws ec2 describe-instances` that nothing is
still running before considering the demo done.
