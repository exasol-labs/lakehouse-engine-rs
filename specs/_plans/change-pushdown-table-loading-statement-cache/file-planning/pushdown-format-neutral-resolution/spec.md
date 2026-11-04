# Feature: Format-Neutral Pushdown Resolution

Routes every pushdown request through the table-format reader seam. The reader that owns a table's format produces its active file list, logical schema, partition columns, table root, and storage, and the rest of the pipeline names no format. A single-table scan, each leg of a broadcast join, and every aggregate shape therefore reach a Delta table by the same route as an Iceberg table.

<!-- DELTA:CHANGED -->
## Background

* A table's file list comes from its format's own metadata: Iceberg metadata and manifests, the Delta log, a Glue partition list, or a Parquet directory listing. Pushdown SQL shape (projection, filter, LIMIT, ORDER BY, aggregate decomposition, join eligibility) is decided from the request's SQL alone, and only file resolution differs per format.
* Exasol sends one pushdown request per pushed-down query block, so a UNION ALL branch, each reference to a CTE, and a subquery arrive as separate requests. One adapter VM serves every request of one statement on one virtual schema.
* The `storage` value of the generated SQL and of each serialized per-shard scan spec is the tagged wrapper of `storage-access/scan-spec-credential-reference`.
<!-- /DELTA:CHANGED -->

## Scenarios

<!-- DELTA:REMOVED -->
### Scenario: One catalog session per request serves every table the request resolves

* *GIVEN* the pushdown requests of one statement on one virtual schema
* *WHEN* the adapter resolves their tables
* *THEN* this scenario SHALL be REMOVED, because the catalog session now serves the whole statement rather than one request
* *AND* it SHALL be REPLACED by "The pushdowns of one statement share one catalog session per virtual schema" below
<!-- /DELTA:REMOVED -->

<!-- DELTA:NEW -->
### Scenario: The pushdowns of one statement share one catalog session per virtual schema

* *GIVEN* a statement that Exasol plans as several pushdown requests on one virtual schema, such as `SELECT * FROM vs.t1 UNION ALL SELECT * FROM vs.t1 UNION ALL SELECT * FROM vs.t2`, under any catalog kind
* *WHEN* the adapter plans every request of the statement
* *THEN* the adapter SHALL authenticate to the catalog at most once for the statement: at most one `/v1/config` lookup for an Iceberg REST catalog, and at most one OAuth2 token request for an Iceberg REST catalog under OAuth2 client credentials
* *AND* a later request of the statement SHALL send no `/v1/config` lookup and no Iceberg REST token request of its own
* *AND* each request's pushdown SQL SHALL be byte-identical to the SQL that the request yields as the only request of its statement, except for the `storage` value
* *AND* the statement SHALL return the rows that its branches return when each runs as a statement of its own
<!-- /DELTA:NEW -->

<!-- DELTA:NEW -->
### Scenario: Every reference to one table in a statement loads the table once and prunes it with its own filter

* *GIVEN* a statement that references one table of a virtual schema more than once, with a different filter per reference, either in one request (a self-join) or in several requests (a UNION ALL of the table with itself, or a CTE referenced twice)
* *WHEN* the adapter plans every reference
* *THEN* the adapter SHALL load the table once for the statement: one catalog table lookup (an Iceberg REST `loadTable` GET, a Unity Catalog table GET, or a Glue `GetTable`), at most one vended-credential request, and one read of the table's metadata file, Delta log listing, Glue partition list, or Parquet directory listing
* *AND* every reference to an Iceberg or Delta table SHALL read the snapshot that was current when the statement loaded the table, as the Iceberg table spec states: "Readers use the snapshot that was current when they load the table metadata", and the Delta protocol states: "readers can read a consistent snapshot of a Delta table, even in the face of concurrent writes"
* *AND* each reference SHALL prune the file list with its own filter, so it plans the same files that it plans as the only reference of its statement
* *AND* each reference to a Glue Hive table SHALL list the files of the partitions that its own filter keeps, because the kept partitions differ per reference
* *AND* a later statement, including the next statement of one `EXECUTE SCRIPT`, SHALL load the table again, so it reads the table's state at that statement's planning
<!-- /DELTA:NEW -->

<!-- DELTA:NEW -->
### Scenario: Two virtual schemas in one statement use their own CONNECTION

* *GIVEN* two virtual schemas on one adapter script and one catalog, each with its own CONNECTION, and one statement that reads a table through each of them
* *WHEN* the adapter plans the statement
* *THEN* each virtual schema's requests SHALL use that schema's own CONNECTION and properties
* *AND* each virtual schema SHALL load a table with its own CONNECTION, so a statement that reads one catalog table through both virtual schemas loads that table twice, once per virtual schema
* *AND* the statement SHALL return, for each branch, the rows that the branch returns as a statement of its own
<!-- /DELTA:NEW -->

<!-- DELTA:CHANGED -->
### Scenario: A request's table resolutions run concurrently rather than one after another

* *GIVEN* a broadcast-join pushdown request whose legs each resolve one table
* *WHEN* the adapter resolves the request's legs
* *THEN* the adapter SHALL resolve every leg CONCURRENTLY, so a two-leg join's plan-time latency approaches its slowest leg rather than the sum of its legs
* *AND* the concurrency SHALL apply to EVERY catalog kind and EVERY table format, so the Delta log reads of two Delta legs overlap in time instead of the second leg's reads starting after the first leg's end
* *AND* the generated SQL and the serialized per-shard scan specs MUST NOT depend on the order in which the legs finish
* *AND* the error a failing leg produces SHALL be the same error that the leg produces alone, and a request with more than one failing leg SHALL surface one of those errors rather than a combined or masked one
* *AND* the per-request in-flight object-store bound SHALL be unchanged by the concurrency
<!-- /DELTA:CHANGED -->
