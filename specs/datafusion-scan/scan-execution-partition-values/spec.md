# Feature: DataFusion Scan Execution — Partition Value Materialization

Extends the scan UDF so a partition column that has no physical counterpart in a data file is
produced from that file's logged partition value instead of read from Parquet. A partitioned table
stores a partition column's value once per file in its catalog metadata rather than once per row in
the file, so a scan that reads only Parquet returns NULL for every partition column. This feature
closes that gap by giving each assigned file its partition values as scan-time constants, inside
DataFusion, so projection, filters, aggregation, and pruning all observe the real values.

## Background

The scan spec already carries both halves of this, format-neutrally and unread: the shard-invariant
common spec carries the table's ordered `partition_columns`, and each file entry carries its own
`partition_values` map — one entry per partition column, holding the serialized value or an explicit
absent value for NULL (see `vs-adapter/delta-table-planning`). Nothing in this feature is
Delta-specific: the same two fields are what an Iceberg identity-transform partition value (issue
#99) and a future Hive-style partition value would populate, and the scan dispatches on whether the
fields are populated, never on which table format produced them.

The Delta Lake protocol specification (`delta-io/delta`, `PROTOCOL.md`, `master`) is what makes the
log authoritative rather than the directory layout or the file content:

- *"By default, the reference implementation stores data files in directories that are named based
  on the partition values for data in that file (i.e. `part1=value1/part2=value2/...`). This
  directory format is only used to follow existing conventions and is not required by the protocol.
  Actual partition values for a file must be read from the transaction log."*
- *"Values for all partition columns present in the schema MUST be present for all files in the
  table."* and *"Columns present in the schema of the table MAY be missing from data files. Readers
  SHOULD fill these missing columns in with `null`."*
- Partition Value Serialization — *"Partition values are stored as strings, using the following
  formats. An empty string for any type translates to a `null` partition value."*, with `numeric
  types` as *"The string representation of the number"*, `date` as *"Encoded as
  `{year}-{month}-{day}`"*, `boolean` as *"Encoded as the string \"true\" or \"false\""*.
- A partition column is not always absent from the file: under IcebergCompatV1 a writer must
  *"Require that partition column values are materialized into any Parquet data file that is present
  in the table, placed after the data columns in the parquet schema"*. The log stays authoritative
  in that case too, per the quote above.

Exasol delegates an advertised predicate fully and never re-applies it, so a filter over a partition
column is this engine's to satisfy: materializing the value is what makes that filter correct rather
than merely deferred.

## Scenarios

### Scenario: A partition column absent from the data file is materialized per file

* *GIVEN* a scan invocation whose common spec names one partition column and whose assigned files
  come from DIFFERENT partitions, each file entry carrying its own value for that column, and whose
  Parquet files carry no physical column of that name
* *WHEN* the scan UDF registers those files and runs the scan
* *THEN* the UDF SHALL produce that column's value for each row as a CONSTANT taken from the value
  logged for the file that row came from, so one scan invocation emits different partition values for
  rows originating in different files
* *AND* the emitted column SHALL appear under the partition column's declared logical name, at its
  declared position in the scan's output, with its declared logical type — the position and type the
  scan already declares for it
* *AND* the UDF MUST NOT emit NULL for a partition column whose file entry logs a value, which is
  what a Parquet-only read would produce
* *AND* the UDF MUST NOT derive the value from the file's directory path, because the directory
  layout is a writer convention rather than the column's value

### Scenario: An absent partition value materializes NULL, never the partition-directory text

* *GIVEN* a scan invocation whose assigned files include one file whose entry carries an EXPLICIT
  absent value for a partition column, and one file whose entry carries the EMPTY string for that
  same column
* *WHEN* the scan UDF materializes that column
* *THEN* the UDF SHALL emit NULL for every row of BOTH files, because the protocol serializes a null
  partition value as an empty string and the scan spec carries it as an explicit absent value
* *AND* the UDF MUST NOT emit the Hive default-partition directory name, the empty string, or any
  other sentinel in place of NULL

### Scenario: A partition value is converted to its column's declared type

* *GIVEN* a scan invocation whose partition columns include a non-string column — an integer, a
  decimal, a date, and a boolean column — each file entry carrying that column's value in the
  protocol's string serialization
* *WHEN* the scan UDF materializes those columns
* *THEN* the UDF SHALL convert each serialized value into the column's DECLARED logical type before it
  reaches the query, so a numeric partition column arrives as a number rather than as its digits
* *AND* a serialized value the declared type cannot represent SHALL fail the scan with a clean user
  error naming the partition column, its declared type, and the rejected value, and MUST NOT be
  silently coerced, truncated, or replaced with NULL
* *AND* that error MUST be returned as an error value, never raised as a panic, and MUST NOT contain
  any storage access key, secret key, or session token

### Scenario: The logged partition value wins over a physically present partition column

* *GIVEN* a scan invocation whose assigned data file DOES carry a physical Parquet column bearing a
  partition column's name — the shape a writer produces under the Delta IcebergCompat features
* *WHEN* the scan UDF reads that file
* *THEN* the UDF SHALL emit the value logged for that file in its partition values, and MUST NOT read
  the physical column, because the protocol makes the transaction log the authoritative source for a
  partition column's value
* *AND* the UDF MUST NOT report a schema conflict, duplicate the column, or fail, so a
  partition-materializing writer and a partition-omitting writer produce the same scan result

### Scenario: A materialized partition column is a first-class scan column

* *GIVEN* a scan spec over a partitioned table whose common spec carries a projection that omits one
  partition column, a filter predicate over another partition column, and — in a second spec over the
  same table — a grouped aggregate whose group key is a partition column
* *WHEN* the scan UDF builds and runs each DataFusion plan
* *THEN* the filter SHALL be evaluated against the MATERIALIZED partition values, so the emitted rows
  are exactly those whose partition value satisfies the predicate, because Exasol re-applies nothing
  it delegated
* *AND* the grouped aggregate SHALL group on the materialized values, so a partition column is a
  valid group key
* *AND* a partition column the projection omits MUST NOT appear in the emitted rows
* *AND* the UDF MAY use a file's partition values to skip that file entirely when the filter cannot be
  satisfied by them, and MUST NOT emit a row from a file it skips on that basis

### Scenario: A scan with no partition columns is unchanged

* *GIVEN* a scan invocation whose common spec names NO partition columns and whose file entries carry
  EMPTY partition values — every Iceberg scan, and every unpartitioned Delta scan
* *WHEN* the scan UDF registers those files and runs the scan
* *THEN* the registered table's schema, the generated scan-driving SQL, the physical plan shape, and
  the emitted rows SHALL all be identical to their pre-feature form for the same spec
* *AND* the serialized scan spec for such a request SHALL stay byte-identical to its pre-feature
  encoding, because both partition fields are omitted from the wire when empty

### Scenario: Each side of a broadcast join materializes its own partition columns

* *GIVEN* a broadcast inner equi-join whose two sides are separate partitioned tables with DIFFERENT
  partition columns
* *WHEN* the scan UDF registers both sides into one session and runs the joined plan
* *THEN* each side SHALL materialize the partition columns THAT side declares, from the partition
  values THAT side's file entries carry, so neither side's partition columns leak into the other
* *AND* a join whose broadcast side is partitioned SHALL return the same rows as the same join with
  that side's partition columns projected from a single-table scan, so partitioning the broadcast side
  changes nothing about the join result
* *AND* a join whose sides carry NO partition columns SHALL produce a scan spec byte-identical to its
  pre-feature encoding
