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
* **SUPERSEDES this feature's byte-identity gate for the `storage` value alone.** The generated SQL and the serialized per-shard scan specs stay byte-identical EXCEPT for the `storage` value, which becomes the tagged wrapper of `vs-adapter/scan-spec-credential-reference`. Every other byte of every request's output is unchanged, which is what keeps this gate meaningful rather than waived.

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: A Unity Catalog table's identity survives the round trip from the involved table

* *GIVEN* a pushdown request under the Unity Catalog kind whose involved virtual table name maps,
  through the identifier recorded at create time, to a three-level Unity Catalog identifier
* *WHEN* the adapter resolves that identifier into the table its format reader plans
* *THEN* the adapter SHALL recover the catalog table identifier's namespace segments and table name
  from the recorded identifier and SHALL load exactly that table from the Unity Catalog
* *AND* the loaded table SHALL be the one whose catalog-reported full name equals the recorded
  identifier, because the Unity Catalog addresses a table by that same dotted full name and the
  segmentation is therefore lossless
* *AND* a recorded identifier that yields no table name SHALL fail with an error naming the
  unresolvable identifier, and MUST NOT resolve a different table
* *AND* a pushdown request whose involved virtual table name is absent from the recorded mapping SHALL keep failing with the error naming the unknown virtual table, unchanged for both catalog kinds
<!-- /DELTA:CHANGED -->

<!-- DELTA:CHANGED -->
### Scenario: A table the reader cannot plan fails the query loud at plan time

* *GIVEN* a pushdown request under the Unity Catalog kind naming a table whose Delta schema declares a
  type this engine does not map, and a second request naming a table the catalog reports in a format
  no Unity Catalog reader plans, such as `ICEBERG`
* *WHEN* the adapter plans each request
* *THEN* the adapter SHALL return the reader's own clean plan-time error, naming the column and its
  Delta type or naming the table and its reported format, and MUST NOT return a scan-driving SQL
  response
* *AND* the adapter MUST NOT fall back to the Iceberg resolution path, emit a partial file list, or
  return rows, because a table that cannot be planned has no correct partial answer
* *AND* the error MUST be returned as an error value, never raised as a panic
* *AND* the error message MUST NOT contain any credential value
<!-- /DELTA:CHANGED -->
