# Decisions: fix-int96-timestamp-overflow

## ADR: Coerce INT96 to microsecond, UTC, on read

**ID:** int96-coerce-microsecond-utc-on-read
**Plan:** `fix-int96-timestamp-overflow`
**Status:** Accepted

### Context

The scan bypasses iceberg-rust's own reader, so a custom `ParquetFormat` never inherits
iceberg-rust's INT96 fix. With `coerce_int96` unset, arrow-rs decodes any Parquet INT96
timestamp column as `Timestamp(Nanosecond)`, whose i64 range spans only 1677-09-21 to
2262-04-11. A far-future INT96 value (e.g. `9999-12-31 23:59:59`, written by legacy tools
such as Fivetran) overflows at decode time on a plain `SELECT *` (issue #143).

### Decision

Set `coerce_int96 = "us"` and `coerce_int96_tz = "UTC"` on every `ParquetFormat` the scan
constructs. INT96 timestamp columns decode as `Timestamp(Microsecond, "UTC")`.

### Options Considered

| Option | Verdict |
|--------|---------|
| `coerce_int96 = "us"`, `coerce_int96_tz = "UTC"` | ✓ Chosen — matches Iceberg Java's pragmatic default and the Iceberg spec's microsecond `timestamp`/`timestamptz` definitions; an INT96 instant is UTC |
| `coerce_int96 = "ns"` (arrow-rs default) | ✗ Rejected — the source of the overflow being fixed |
| A defensive clamp or out-of-range fallback on top of the decode | ✗ Rejected — explicitly declined in the interview; root-cause only |

### Consequences

Far-future INT96 timestamps through year 9999 decode and scan without overflow. INT96's
sub-microsecond digits are truncated, a named trade-off consistent with Iceberg's
microsecond `timestamp` model. Values above Exasol's own `TIMESTAMP` maximum (year > 9999)
remain unscannable, now failing at the Exasol emit boundary instead of at arrow-decode.

## ADR: Author the INT96 fixture via Spark native-write + `add_files`, not `INSERT INTO`

**ID:** int96-fixture-spark-native-write-add-files
**Plan:** `fix-int96-timestamp-overflow`
**Status:** Accepted

### Context

Verifying the INT96 decode fix requires a genuine INT96-encoded Iceberg data file — a
normal INT64-microsecond fixture cannot reproduce the arrow-rs nanosecond overflow. Iceberg's
own Spark writer emits Parquet INT64 regardless of `spark.sql.parquet.outputTimestampType`,
so a plain `INSERT INTO <iceberg_table>` cannot produce INT96.

### Decision

Write the INT96 Parquet file with Spark's native writer
(`spark.sql.parquet.outputTimestampType=INT96`), then import it into an Iceberg table via
the Iceberg `add_files` procedure, which registers the file as-is without rewrite.

### Options Considered

| Option | Verdict |
|--------|---------|
| Native-write + `add_files` | ✓ Chosen — the only path that lands genuine INT96 in an Iceberg table; mirrors the real-world Hive/Fivetran path behind issue #143 |
| `INSERT INTO <iceberg_table>` after setting `outputTimestampType=INT96` | ✗ Rejected — Iceberg's Spark writer emits INT64 regardless of this setting, so it would silently fail to reproduce the bug |

### Consequences

The fixture reproduces issue #143's exact failure shape using only existing E2E stack
tooling (Spark, the shared REST catalog, MinIO) with no new dependency. Because `add_files`
registers the file as-is, the fixture step MUST assert the committed data file is
physically INT96, so a silent INT64 result or an unexpected rewrite fails loudly instead of
passing vacuously.
