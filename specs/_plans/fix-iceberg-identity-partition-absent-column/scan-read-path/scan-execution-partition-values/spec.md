# Feature: DataFusion Scan Execution, Partition Value Materialization

Produces a partition column that has no physical counterpart in a data file from the file's logged partition value, as a scan-time constant inside DataFusion. Projection, filters, aggregation and pruning then see the real values instead of NULL.

<!-- DELTA:CHANGED -->
## Background

* The shard-invariant common spec carries the table's ordered `partition_columns`. Each file entry carries a `partition_values` map with one entry per partition column, holding the serialized value or an explicit absent value for NULL: see `delta/delta-table-planning`. A `partition_values` key that `partition_columns` does not list is a per-file value for a column the data file may hold, which the Iceberg reader writes for an `identity` partition field: see `scan-read-path/scan-execution-field-id-projection-absent-fields`. The scan dispatches on whether these fields are populated and on whether `partition_columns` lists a key, never on the table format.
* The Delta protocol (`delta-io/delta`, `PROTOCOL.md`) makes the transaction log the authoritative source of a file's partition values. The directory layout is a writer convention. A writer may also materialize the partition column into the Parquet file, and the log stays authoritative then too.
* The protocol stores partition values as strings: numbers as their string form, dates as `{year}-{month}-{day}`, booleans as `"true"` or `"false"`. An empty string means a null partition value.
<!-- /DELTA:CHANGED -->

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: The logged partition value wins over a physically present partition column

* *GIVEN* a scan invocation whose assigned data file DOES carry a physical Parquet column bearing the
  name of a column the common spec's `partition_columns` lists, the shape a writer produces under the
  Delta IcebergCompat features
* *WHEN* the scan UDF reads that file
* *THEN* the UDF SHALL emit the value logged for that file in its partition values, and MUST NOT read
  the physical column, because the protocol makes the transaction log the authoritative source for a
  partition column's value
* *AND* the UDF MUST NOT report a schema conflict, duplicate the column, or fail, so a
  partition-materializing writer and a partition-omitting writer produce the same scan result
<!-- /DELTA:CHANGED -->

<!-- DELTA:CHANGED -->
### Scenario: A materialized partition column is a first-class scan column

* *GIVEN* a scan spec over a partitioned table whose common spec carries a projection that omits one
  partition column, a filter predicate over another partition column, and, in a second spec over the
  same table, a grouped aggregate whose group key is a partition column
* *WHEN* the scan UDF builds and runs each DataFusion plan
* *THEN* the filter SHALL be evaluated against the MATERIALIZED partition values, so the emitted rows
  are exactly those whose partition value satisfies the predicate, because Exasol re-applies nothing
  it delegated
* *AND* the grouped aggregate SHALL group on the materialized values, so a partition column is a
  valid group key
* *AND* a partition column the projection omits MUST NOT appear in the emitted rows
* *AND* the UDF MAY use a file's partition values for the columns `partition_columns` lists to skip
  that file entirely when the filter cannot be satisfied by them, and MUST NOT emit a row from a file
  it skips on that basis
<!-- /DELTA:CHANGED -->

<!-- DELTA:CHANGED -->
### Scenario: A scan with no partition columns is unchanged

* *GIVEN* a scan invocation whose common spec names NO partition columns and whose file entries carry
  EMPTY partition values: every unpartitioned Delta scan, and every Iceberg scan whose data files
  carry no `identity` partition value
* *WHEN* the scan UDF registers those files and runs the scan
* *THEN* the registered table's schema, the generated scan-driving SQL, the physical plan shape, and
  the emitted rows SHALL all be identical to their pre-feature form for the same spec
* *AND* the serialized scan spec for such a request SHALL stay byte-identical to its pre-feature
  encoding, because both partition fields are omitted from the wire when empty
<!-- /DELTA:CHANGED -->
