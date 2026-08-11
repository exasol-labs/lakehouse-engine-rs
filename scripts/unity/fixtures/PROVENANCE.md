# Vendored Delta fixture provenance (spike #325)

These are prebuilt Delta Lake tables copied verbatim from the **delta-kernel-rs**
test-data set at tag **v0.26.0** (`kernel/tests/data/`), the same reader chosen
in spike #317. Apache-2.0 licensed. They are read fixtures — never mutated.

| Directory | Upstream fixture | What it exercises | Serves | Verified read over MinIO |
|---|---|---|---|---|
| `table-with-dv-small/` | `table-with-dv-small` (dir) | **Deletion vector** applied | #320 | 10 raw → **8** rows (DV) |
| `cdf-column-mapping-name-mode/` | `cdf-column-mapping-name-mode.tar.zst` | **Column mapping (name)** | #320 | `col-<uuid>` → `[id,name,value]` |
| `cdf-column-mapping-id-mode/` | `cdf-column-mapping-id-mode.tar.zst` | **Column mapping (id)** — #317 parked in #325 | #320 | `col-<uuid>` → `[id,name,value]` |
| `basic_partitioned/` | `basic_partitioned` (dir) | **Partitioned** (`letter`), 6 data files, minRV1 | #319, #321 | `[letter,number,a_float]`, 6 rows |
| `multi-part-stats/` | `v1-multi-part-struct-stats-only` (dir) | **Multi-file + per-file stats** (5 files) | #321 | `[id,value]`, 5 rows |
| `stats-all-types/` | `stats-writing-all-types/delta` | **Broad types** incl. array/map/struct/binary (→JSON VARCHAR); declares `timestampNtz`+`columnMapping` | #322 (types) | 16 cols, 4 rows |
| `unshredded-variant/` | `unshredded-variant.tar.zst` | **Unsupported reader feature** `variantType-preview` + nested variant/array/struct/map | #322 (fail-loud) | reads in kernel, 102 rows |
| `type-widening/` | `type-widening` (dir) | **Unsupported reader feature** `typeWidening-preview` (+`timestampNtz`); numeric/decimal widening | #322 (fail-loud) | reads in kernel, 2 rows |

All verified over MinIO/S3 during the spike: `UC resolve → UC vend static creds →
delta-kernel-rs read with a client-side MinIO endpoint override`.

**#322 gating note:** the delta-kernel reader *reads* the "unsupported" tables
(`variantType`, `typeWidening`, `timestampNtz`) without error — so #322's fail-loud
check cannot rely on the kernel erroring. The engine must inspect the Delta
`protocol.readerFeatures` and refuse features Exasol can't faithfully represent,
independently of kernel capability. `stats-all-types` deliberately sits on the
boundary (it declares `timestampNtz`) and forces the "gate vs. map to Exasol
TIMESTAMP" decision in #322.

## Why vendored (not authored at bring-up)

delta-rs (the `deltalake` Python package) **cannot write** deletion vectors or
column mapping, and its reader **cannot even read** them (DV → unsupported reader
feature; column mapping → reader version 2 unsupported). So — unlike the Iceberg
positional-delete fixtures, which Apache Spark authors at bring-up — the reliable
source for these two Delta correctness features is the prebuilt kernel test tables.
They are tiny (~64 KB total), so vendoring keeps the harness self-contained and
network-free at seed time (supporting the fail-not-skip contract).

## Modifications from upstream

- `.crc` sidecar files stripped (Delta ignores them; keeps the tree clean).
- `cdf-column-mapping-name-mode/_change_data/` (CDF change-data files) removed —
  not read by a plain snapshot scan; re-added only if a CDF test needs it.

## Two notes for later issues

- `cdf-table-with-dv` (the single-file tarball) is a poor DV fixture: it reads
  **10** rows (no DV at its latest snapshot). Use `table-with-dv-small` for a
  DV-applied assertion.
- **Bespoke/custom-schema** DV or column-mapping tables (beyond these two) must be
  authored with **Apache Spark + delta-spark 3.3.0** (Spark 3.5.x) — the same
  one-shot pattern as `scripts/spark-fixtures/`. See SPIKE_UC_DELTA_HARNESS.md §Q3.
