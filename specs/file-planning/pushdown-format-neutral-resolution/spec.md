# Feature: Format-Neutral Pushdown Resolution

Routes every pushdown request through the table-format reader seam. The reader that owns a table's format produces its active file list, logical schema, partition columns, table root, and storage, and the rest of the pipeline names no format. A single-table scan, each leg of a broadcast join, and every aggregate shape therefore reach a Delta table by the same route as an Iceberg table.

## Background

* A table's file list comes from its format's own metadata: Iceberg metadata and manifests, the Delta log, a Glue partition list, or a Parquet directory listing. Pushdown SQL shape (projection, filter, LIMIT, ORDER BY, aggregate decomposition, join eligibility) is decided from the request's SQL alone, and only file resolution differs per format.
* `file-planning/pushdown-statement-state` states what the pushdown requests of one statement share.
* The `storage` value of the generated SQL and of each serialized per-shard scan spec is the tagged wrapper of `storage-access/scan-spec-credential-reference`.

## Scenarios

### Scenario: Every pushdown request shape resolves through the one format-reader seam

* *GIVEN* four pushdown requests over the SAME virtual schema, a single-table projection scan, a
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

### Scenario: A Unity Catalog table's identity survives the round trip from the involved table

* *GIVEN* a pushdown request under the Unity Catalog kind whose involved table's identifier note,
  recorded per `vs-adapter/create-virtual-schema`, holds a three-level Unity Catalog identifier
* *WHEN* the adapter resolves that identifier into the table its format reader plans
* *THEN* the adapter SHALL recover the catalog table identifier's namespace segments and table name
  from the recorded identifier and SHALL load exactly that table from the Unity Catalog
* *AND* the loaded table SHALL be the one whose catalog-reported full name equals the recorded
  identifier, because the Unity Catalog addresses a table by that same dotted full name and the
  segmentation is therefore lossless
* *AND* a recorded identifier that yields no table name SHALL fail with an error naming the
  unresolvable identifier, and MUST NOT resolve a different table
* *AND* a pushdown request whose involved table carries no readable identifier note SHALL fail with the error `file-planning/pushdown-planning-file-resolution` specifies for a missing note, under every catalog kind

### Scenario: Iceberg requests planned through the format-reader seam keep their golden SQL, scan specs, and file pruning

* *GIVEN* the shipped Iceberg pushdown suites, the golden dispatch fixtures, the scan-spec encodings,
  the generated scan-driving SQL for raw, TopN, single-group aggregate, grouped aggregate, and
  broadcast-join requests, and the Iceberg E2E suites
* *WHEN* those requests are planned through the format-reader seam instead of the direct file resolver
* *THEN* the generated SQL and the serialized per-shard scan specs MUST be byte-identical to their
  pre-feature output for every request EXCEPT for the `storage` value, which carries the tagged
  wrapper of `storage-access/scan-spec-credential-reference`
* *AND* the Iceberg-level file pruning the resolver applies from the request's filter SHALL still be
  applied, because the filter is forwarded to the reader unchanged

### Scenario: The Iceberg file resolver is collapsed into its reader and leaves the façade

* *GIVEN* the Iceberg format reader, whose whole body forwards its arguments to the separately
  published Iceberg file-resolution function, kept in that shape only until this feature removed its
  direct callers
* *WHEN* production pushdown and every test call site route through the format-reader seam
* *THEN* the file resolver's body SHALL move INTO the Iceberg reader and the separately published
  function SHALL be DELETED, so exactly one path resolves an Iceberg file list
* *AND* no production or test call site SHALL name that function afterwards; every caller SHALL reach
  the same resolution through the format-reader seam
* *AND* the Iceberg reader SHALL still return EMPTY partition columns, EMPTY partition values on every
  file entry, and a field-id on every logical field, so the Iceberg wire encoding is unchanged

### Scenario: Resolved partition columns reach the scan spec for every side

* *GIVEN* a pushdown request over a partitioned table whose resolved scan carries ordered partition
  column names, planned once as a single-table scan and once as the broadcast side of a join whose
  other side is a different partitioned table
* *WHEN* the adapter assembles the per-shard scan specs
* *THEN* the shard-invariant common spec SHALL carry the SCANNED table's partition column names, and
  each per-shard file entry SHALL carry that file's partition values, so the scan can materialize them
* *AND* the join spec SHALL carry the BROADCAST side's OWN partition column names alongside that
  side's file list, logical schema, name mapping, table root, and storage, so neither side is given the
  other's partition columns
* *AND* a request whose resolved scans carry NO partition columns SHALL produce a serialized scan spec
  byte-identical to its pre-feature encoding, on both the common spec and the join spec

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

### Scenario: Capability advertisement stays blind to the catalog kind

* *GIVEN* three virtual schemas over the same adapter, one created under each catalog kind
* *WHEN* Exasol requests the adapter's capabilities for each
* *THEN* the adapter SHALL return the SAME capability set for all three, and MUST NOT read, branch on, or receive the catalog kind while assembling it
* *AND* a capability the adapter advertises SHALL therefore be one it satisfies under EVERY kind, because Exasol re-applies nothing it delegated and a kind-conditional capability would return wrong rows rather than a deferred check

### Scenario: A request's table resolutions run concurrently rather than one after another

* *GIVEN* a broadcast-join pushdown request whose legs each resolve one table
* *WHEN* the adapter resolves the request's legs
* *THEN* the adapter SHALL resolve every leg CONCURRENTLY, so a two-leg join's plan-time latency approaches its slowest leg rather than the sum of its legs
* *AND* the concurrency SHALL apply to EVERY catalog kind and EVERY table format, so the Delta log reads of two Delta legs overlap in time instead of the second leg's reads starting after the first leg's end
* *AND* the generated SQL and the serialized per-shard scan specs MUST NOT depend on the order in which the legs finish
* *AND* the error a failing leg produces SHALL be the same error that the leg produces alone, and a request with more than one failing leg SHALL surface one of those errors rather than a combined or masked one
* *AND* the per-request in-flight object-store bound SHALL be unchanged by the concurrency
