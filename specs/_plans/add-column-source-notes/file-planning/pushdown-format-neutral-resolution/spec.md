# Feature: Format-Neutral Pushdown Resolution

Routes every pushdown request through the table-format reader seam, so the ONE thing a pushdown needs
from a table's format — its active file list, its logical schema, its partition columns, its table
root, and the storage those were resolved through — is produced by the reader that owns that format
and consumed by a pipeline that names no format at all. This is what makes a Delta table queryable:
the refusal that blocked every Unity Catalog pushdown is replaced by a resolution seam, not by a
Delta-shaped branch, so a single-table scan, each leg of a broadcast join, and every aggregate shape
reach a Delta table by the same route they reach an Iceberg one.

## Background

The seam already exists and is exercised only by tests: a `FormatReader` returns a `ResolvedScan`,
`format_reader` selects the reader by exhaustively matching a `ScanSource`, and both concrete readers
are private to that module. Production pushdown reaches none of it — it calls the Iceberg-only file
resolver directly, from the single-table path and from each join leg, and the adapter refuses a Unity
Catalog pushdown outright before any of that runs.

Resolution economy is a recorded property this feature preserves: a request resolves its catalog
session ONCE and reuses it for every table it touches, so a two-leg join performs no more catalog
authentication round-trips than a single-table scan.

Pushdown SQL shape is format-agnostic in this engine: projection, filter, LIMIT, ORDER BY, aggregate
decomposition, and join eligibility are decided from the request's SQL alone and are unchanged by
this feature. Only file resolution differs per format.

* **This delta is issue #135. It amends ONE scenario and changes no resolution rule.** The one format-reader seam, the one catalog session per request, the single catalog-kind match site, the Unity identity round trip, the resolver collapse, the partition-column propagation, the loud plan-time failure, and the kind-blind capability advertisement are all UNCHANGED.
* **SUPERSEDES this feature's byte-identity gate for the `storage` value alone.** The generated SQL and the serialized per-shard scan specs stay byte-identical EXCEPT for the `storage` value, which becomes the tagged wrapper of `storage-access/scan-spec-credential-reference`. Every other byte of every request's output is unchanged, which is what keeps this gate meaningful rather than waived.

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: Every pushdown request shape resolves through the one format-reader seam

* *GIVEN* four pushdown requests over the SAME virtual schema — a single-table projection scan, a
  single-table aggregate, a grouped aggregate, and a broadcast inner equi-join with two legs
* *WHEN* the adapter plans each request
* *THEN* the adapter SHALL obtain each table's file list, table root, name mapping, and effective
  storage from the format-reader seam, and each table's logical schema, partition columns, and
  refused columns from that table's column notes (`vs-adapter/column-source-notes`), assembled into
  one `ResolvedScan`, and MUST NOT call any format-specific file resolver directly
* *AND* the adapter SHALL parse a table's column notes at ONE site in the per-request resolver and
  SHALL pass the parsed declaration to the format reader, so every reader receives the same
  declaration and no reader parses notes itself
* *AND* every parameter the resolver and the reader take SHALL be read by every format arm, so no
  signature carries a value only one format consumes
* *AND* the adapter SHALL apply that rule to EVERY leg of the join, so a join leg and a single-table
  scan resolve identically
* *AND* the adapter MUST NOT gate any request shape on the table format or the catalog kind, so
  enabling a format enables every shape at once and a shape that fails does so as a defect rather than
  as a refusal. The one exception is `vs-adapter/lakekeeper-permission-check`, which refuses every
  shape alike while `PERMISSION_CHECK = 'LAKEKEEPER'` is set under any catalog kind other than
  Iceberg REST
* *AND* the SQL the adapter generates for each request SHALL be decided from the request alone, so the
  same request over an Iceberg table and over a Delta table with the same columns yields the same
  pushdown decisions
<!-- /DELTA:CHANGED -->
