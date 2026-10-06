# Feature: Glue Catalog Client

Reads the AWS Glue Data Catalog API for the `GLUE` catalog kind. The client lists the tables of one Glue database, routes each table to the Iceberg reader, the Parquet reader, or a visible skip, and returns the registered partitions of a table. It implements the shared `CatalogClient` trait, so the shared createVirtualSchema listing pipeline lists its tables.

## Background

* The client calls `GetTables`, `GetTable`, and `GetPartitions`. The CONNECTION address is the endpoint. The signing region is the region `vs-adapter/connection-credentials-sigv4` resolves.
* The client requests partitions with `ExcludeColumnSchema` set, because the reader reads only values, locations, and formats.
* The client calls Glue through `aws-sdk-glue`, which retries throttling and transient errors, parses the service error code, and corrects its signing time from the service time before it retries (ADR 097). Service behavior is verified against real Glue by `glue-e2e/glue-e2e-harness`.
* AWS Glue API, `CatalogId`: "If none is provided, the AWS account ID is used by default." Glue rejects an empty value with `400 InvalidInputException "CatalogId ID cannot be empty."` (verified live, #410).
* Glue returns service errors as HTTP 400 with a JSON body whose `__type` names the error, also for a missing table or database (`EntityNotFoundException`, verified live, #410).
* AWS Glue API, `GetPartitions` response `NextToken`: "A continuation token, if the returned list of partitions does not include the last one." Glue returned a non-null `NextToken` after the last page (verified live, #410).
* A partition's `Values` are positional against the table's `PartitionKeys`. Hive writes a NULL partition value as the literal `__HIVE_DEFAULT_PARTITION__` (verified live against Athena, #410).

## Scenarios

<!-- DELTA:REMOVED -->
### Scenario: The client routes a table by its declared table type before its storage descriptor

* *GIVEN* a Glue database holding an Iceberg table (`Parameters.table_type` `ICEBERG`), a second one with `table_type` `iceberg`, an Athena Delta table (`table_type` `delta`, `SequenceFileInputFormat`), a Hive Parquet table (no `table_type`, `MapredParquetInputFormat`), Hive ORC and text tables, a symlink table (`SymlinkTextInputFormat` with `ParquetHiveSerDe`), and a view (`TableType` `VIRTUAL_VIEW`)
* *WHEN* the client lists the database
* *THEN* the client SHALL admit both Iceberg tables with the Iceberg format tag, comparing `table_type` case-insensitively
* *AND* a present `table_type` other than `ICEBERG` SHALL be a skip naming that value, and MUST NOT fall through to the storage-descriptor check
* *AND* a table without `table_type` SHALL be admitted with the Parquet format tag only when its `InputFormat` is `org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat`, so the symlink table is a skip although its SerDe is Parquet
* *AND* every other input format, and a view, SHALL be a skip naming the input format or the `TableType`
* *AND* each skip SHALL be a skipped-table entry whose reason names the Glue value that decided it
<!-- /DELTA:REMOVED -->

<!-- DELTA:REMOVED -->
### Scenario: An Iceberg table takes its columns from its current metadata file

* *GIVEN* an admitted Iceberg table whose `Parameters.metadata_location` names its `metadata.json`, and whose Glue columns carry Hive type strings with `iceberg.field.id` parameters
* *WHEN* the client lists the database, and separately loads the table for a pushdown
* *THEN* the listing SHALL read that metadata file through the CONNECTION's storage and SHALL take the columns from its current schema
* *AND* the client MUST NOT read the table's Glue columns in either case
* *AND* the neutral table SHALL carry the metadata location in both cases
* *AND* the load for a pushdown MUST NOT read the metadata file, because the Iceberg planner reads it (`vs-adapter/glue-table-planning`)
* *AND* an Iceberg table with an absent or empty `metadata_location` SHALL be a skip naming that parameter
* *AND* a metadata file the client cannot read SHALL fail the listing with an error naming the table
<!-- /DELTA:REMOVED -->

<!-- DELTA:REMOVED -->
### Scenario: A Parquet table declares its Glue columns and partition keys

* *GIVEN* an admitted Hive Parquet table with storage-descriptor columns, `PartitionKeys` `p_int int`, `p_date date`, and `p_str string`, and a `Location` ending in `/`
* *WHEN* the client lists or loads the table
* *THEN* the neutral table's columns SHALL be the storage-descriptor columns followed by the partition keys, each carrying its Glue `Type` string verbatim
* *AND* the neutral table's partition columns SHALL be the partition-key names in `PartitionKeys` order
* *AND* the neutral table's storage location SHALL be the storage-descriptor `Location` without its trailing `/`
* *AND* a Parquet table whose `Parameters."projection.enabled"` is `true`, compared case-insensitively, SHALL be a skip stating that partition projection registers no partition in Glue, because reading it would return zero rows
<!-- /DELTA:REMOVED -->

<!-- DELTA:REMOVED -->
### Scenario: Partitions carry their Glue values, location, and format

* *GIVEN* a partitioned Parquet table with a partition whose `Values` are `1`, `2024-01-01`, `a`, a partition whose first value is `__HIVE_DEFAULT_PARTITION__`, a partition whose storage descriptor declares no `InputFormat`, and a partition whose `InputFormat` is `OrcInputFormat`
* *WHEN* the pushdown asks the client for the table's partitions
* *THEN* each partition SHALL carry one value per partition key, keyed by the key name, taken from Glue `Values` and never from the location path
* *AND* `__HIVE_DEFAULT_PARTITION__` SHALL become an absent value, which the scan reads as NULL
* *AND* each partition's format SHALL come from its own `InputFormat`, falling back to the table's when the partition declares none, so the ORC partition reports that it is not Parquet and names its input format
* *AND* each location SHALL be returned verbatim apart from a removed trailing `/`
* *AND* a partition whose value count differs from the partition-key count SHALL fail with an error naming the partition's location
* *AND* an unpartitioned table, which registers no partition in Glue, SHALL read as ONE Parquet partition at the table's own location, with no value and no `GetPartitions` call
<!-- /DELTA:REMOVED -->

<!-- DELTA:REMOVED -->
### Scenario: Every listing follows its continuation tokens and stops on an empty page

* *GIVEN* a `GetTables` or `GetPartitions` page that is short, empty, or carries an absent, empty, or non-empty `NextToken`
* *WHEN* the client decides whether to request the next page
* *THEN* the client SHALL request the next page only when the page is non-empty and carries a non-empty `NextToken`
* *AND* the client SHALL stop on an absent or empty `NextToken` or on an empty page, and MUST NOT stop on a page shorter than the maximum page size
<!-- /DELTA:REMOVED -->

<!-- DELTA:REMOVED -->
### Scenario: The CatalogId is sent only when the CONNECTION names one

* *GIVEN* one CONNECTION without `warehouse`, one whose `warehouse` is only whitespace, and one with `warehouse` `123456789012`
* *WHEN* the client builds its Glue requests
* *THEN* the client SHALL set `CatalogId` `123456789012` for the third CONNECTION
* *AND* the client MUST NOT set a `CatalogId` for the first two, never an empty string
<!-- /DELTA:REMOVED -->

<!-- DELTA:REMOVED -->
### Scenario: A Glue NAMESPACE names exactly one database

* *GIVEN* the NAMESPACE values `sales.eu`, an empty NAMESPACE, and an empty database name
* *WHEN* the client lists the namespace
* *THEN* the listing SHALL fail with an error stating that a Glue NAMESPACE names exactly one database
* *AND* the client MUST NOT call Glue
<!-- /DELTA:REMOVED -->

<!-- DELTA:REMOVED -->
### Scenario: A failed Glue call names the operation, the subject, and the cause

* *GIVEN* Glue calls that fail with `400 EntityNotFoundException`, `AccessDeniedException` with HTTP 400 and with HTTP 404, `400 InvalidInputException`, an HTTP 503 without an error code, an elapsed operation timeout, or a request failure, with a service message that echoes the CONNECTION's credentials
* *WHEN* the client classifies each failure and words its error
* *THEN* the client SHALL classify a service error by its service error code, and MUST NOT classify it by the HTTP status
* *AND* the error SHALL name the Glue operation and the database or table it addressed
* *AND* `EntityNotFoundException` SHALL state that the database or table does not exist, another service error SHALL name its code and message, an error without a code SHALL name its HTTP status, and a timeout SHALL name the 30 second deadline
* *AND* no error SHALL contain a credential value
<!-- /DELTA:REMOVED -->

<!-- DELTA:REMOVED -->
### Scenario: Every Glue call retries within a bounded time

* *GIVEN* the client's Glue configuration
* *WHEN* the client calls Glue
* *THEN* the client SHALL use the SDK's standard retry mode, which backs off exponentially with jitter, with up to 5 attempts per call
* *AND* each call SHALL carry a 30 second operation timeout, retries included, because it runs inside a pushdown
<!-- /DELTA:REMOVED -->

<!-- DELTA:REMOVED -->
### Scenario: A clock-skew signing failure names the clock

* *GIVEN* a signing-time rejection that persists after the SDK's corrected retries: `RequestTimeTooSkewed`, `RequestExpired`, or an `InvalidSignatureException` whose message starts with `Signature expired` or `Signature not yet current`
* *WHEN* the client classifies the rejection and words its error
* *THEN* the error SHALL state that the Exasol node's clock differs from AWS time
* *AND* that error MUST NOT read as a credential error
* *AND* an `InvalidSignatureException` for a mismatched signature MUST NOT be classified as a clock error
<!-- /DELTA:REMOVED -->
