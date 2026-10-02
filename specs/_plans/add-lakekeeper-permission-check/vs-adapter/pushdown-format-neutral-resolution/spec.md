# Feature: Format-Neutral Pushdown Resolution

Routes every pushdown request through the table-format reader seam, so the ONE thing a pushdown needs
from a table's format — its active file list, its logical schema, its partition columns, its table
root, and the storage those were resolved through — is produced by the reader that owns that format
and consumed by a pipeline that names no format at all. This is what makes a Delta table queryable:
the refusal that blocked every Unity Catalog pushdown is replaced by a resolution seam, not by a
Delta-shaped branch, so a single-table scan, each leg of a broadcast join, and every aggregate shape
reach a Delta table by the same route they reach an Iceberg one.

## Background

* Unchanged by this plan. This section is unmarked, so `speq record` ignores it and the recorded Background survives intact. See `specs/vs-adapter/pushdown-format-neutral-resolution/spec.md`.

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: Every pushdown request shape resolves through the one format-reader seam

* *GIVEN* four pushdown requests over the SAME virtual schema — a single-table projection scan, a
  single-table aggregate, a grouped aggregate, and a broadcast inner equi-join with two legs
* *WHEN* the adapter plans each request
* *THEN* the adapter SHALL obtain each table's file list, logical schema, partition columns, table
  root, name mapping, and effective storage from a `ResolvedScan` returned by the format-reader seam,
  and MUST NOT call any format-specific file resolver directly
* *AND* the adapter SHALL apply that rule to EVERY leg of the join, so a join leg and a single-table
  scan resolve identically
* *AND* the adapter MUST NOT gate any request shape on the table format or the catalog kind, so
  enabling a format enables every shape at once and a shape that fails does so as a defect rather than
  as a refusal. The one exception is `vs-adapter/lakekeeper-permission-check`, which refuses every
  shape alike while `PERMISSION_CHECK = 'LAKEKEEPER'` is set under a catalog kind with no permission
  client
* *AND* the SQL the adapter generates for each request SHALL be decided from the request alone, so the
  same request over an Iceberg table and over a Delta table with the same columns yields the same
  pushdown decisions
<!-- /DELTA:CHANGED -->

<!-- DELTA:CHANGED -->
### Scenario: One catalog session per request serves every table the request resolves

* *GIVEN* a broadcast-join pushdown request whose two legs name two different tables in one virtual schema, under any catalog kind
* *WHEN* the adapter resolves both legs
* *THEN* the adapter SHALL build the request's catalog session EXACTLY ONCE and SHALL resolve both legs through it, so the request performs no more catalog authentication round-trips than a single-table request over the same virtual schema
* *AND* the adapter MUST NOT build a second session per leg, per shape, or per format reader
* *AND* the number of catalog round-trips an Iceberg request performs SHALL be unchanged from before this feature, except for the one batch-check request that `vs-adapter/pushdown-catalog-session` admits while `PERMISSION_CHECK = 'LAKEKEEPER'` is set
* *AND* under a catalog kind whose session is an OBJECT STORE rather than a catalog client, the adapter SHALL build that store EXACTLY ONCE per request and SHALL share it across every leg, so a two-table join opens one store and its admission limiter bounds the whole request rather than each leg
<!-- /DELTA:CHANGED -->
