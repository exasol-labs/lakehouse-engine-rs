# Feature: Glue Catalog Client

Reads the AWS Glue Data Catalog API for the `GLUE` catalog kind. The client lists the tables of one Glue database, routes each table to the Iceberg reader, the Parquet reader, or a visible skip, and returns the registered partitions of a table. It implements the shared `CatalogClient` trait, so the shared createVirtualSchema listing pipeline lists its tables.

## Background

* The client calls `GetTables`, `GetTable`, and `GetPartitions`. The CONNECTION address is the endpoint. The signing region is the region `vs-adapter/connection-credentials-sigv4` resolves.
* AWS Glue API, `CatalogId`: "If none is provided, the AWS account ID is used by default." Glue rejects an empty value with `400 InvalidInputException "CatalogId ID cannot be empty."` (verified live, #410).
* Glue returns service errors as HTTP 400 with a JSON body whose `__type` names the error, also for a missing table or database (`EntityNotFoundException`, verified live, #410).
* AWS Glue API, `GetPartitions` response `NextToken`: "A continuation token, if the returned list of partitions does not include the last one." Glue returned a non-null `NextToken` after the last page (verified live, #410).
* A partition's `Values` are positional against the table's `PartitionKeys`. Hive writes a NULL partition value as the literal `__HIVE_DEFAULT_PARTITION__` (verified live against Athena, #410).

## Scenarios

### Scenario: The client routes a table by its declared table type before its storage descriptor

* *GIVEN* a Glue database holding an Iceberg table (`Parameters.table_type` `ICEBERG`), a second one with `table_type` `iceberg`, an Athena Delta table (`table_type` `delta`, `SequenceFileInputFormat`), a Hive Parquet table (no `table_type`, `MapredParquetInputFormat`), Hive ORC, text, JSON, and Avro tables, a symlink table (`SymlinkTextInputFormat` with `ParquetHiveSerDe`), and a view (`TableType` `VIRTUAL_VIEW`)
* *WHEN* the client lists the database
* *THEN* the client SHALL admit both Iceberg tables with the Iceberg format tag, comparing `table_type` case-insensitively
* *AND* a present `table_type` other than `ICEBERG` SHALL be a skip naming that value, and MUST NOT fall through to the storage-descriptor check
* *AND* a table without `table_type` SHALL be admitted with the Parquet format tag only when its `InputFormat` is `org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat`, so the symlink table is a skip although its SerDe is Parquet
* *AND* every other input format, and a view, SHALL be a skip naming the input format or the `TableType`
* *AND* each skip SHALL be a skipped-table entry whose reason names the Glue value that decided it

### Scenario: An Iceberg table takes its columns from its current metadata file

* *GIVEN* an admitted Iceberg table whose `Parameters.metadata_location` names its `metadata.json`, and whose Glue columns carry Hive type strings with `iceberg.field.id` parameters
* *WHEN* the client lists the database, and separately loads the table for a pushdown
* *THEN* the listing SHALL read that metadata file through the CONNECTION's storage and SHALL take the columns from its current schema
* *AND* the client MUST NOT read the table's Glue columns
* *AND* the neutral table SHALL carry the metadata location in both cases
* *AND* the load for a pushdown MUST NOT read the metadata file, because the Iceberg planner reads it (`vs-adapter/glue-table-planning`)
* *AND* an Iceberg table with an absent or empty `metadata_location` SHALL be a skip naming that parameter
* *AND* a metadata file the client cannot read SHALL fail the listing with an error naming the table

### Scenario: A Parquet table declares its Glue columns and partition keys

* *GIVEN* an admitted Hive Parquet table with storage-descriptor columns, `PartitionKeys` `p_int int`, `p_date date`, and `p_str string`, and a `Location` ending in `/`
* *WHEN* the client lists or loads the table
* *THEN* the neutral table's columns SHALL be the storage-descriptor columns followed by the partition keys, each carrying its Glue `Type` string verbatim
* *AND* the neutral table's partition columns SHALL be the partition-key names in `PartitionKeys` order
* *AND* the neutral table's storage location SHALL be the storage-descriptor `Location` without its trailing `/`
* *AND* a Parquet table whose `Parameters."projection.enabled"` is `true`, compared case-insensitively, SHALL be a skip stating that partition projection registers no partition in Glue, because reading it would return zero rows

### Scenario: Partitions carry their Glue values, location, and format

* *GIVEN* a partitioned Parquet table with a partition whose `Values` are `1`, `2024-01-01`, `a`, a partition whose first value is `__HIVE_DEFAULT_PARTITION__`, a partition whose storage descriptor declares no `InputFormat`, and a partition whose `InputFormat` is `OrcInputFormat`
* *WHEN* the pushdown asks the client for the table's partitions
* *THEN* each partition SHALL carry one value per partition key, keyed by the key name, taken from Glue `Values` and never from the location path
* *AND* `__HIVE_DEFAULT_PARTITION__` SHALL become an absent value, which the scan reads as NULL
* *AND* each partition's format SHALL come from its own `InputFormat`, falling back to the table's when the partition declares none, so the ORC partition reports that it is not Parquet and names its input format
* *AND* each location SHALL be returned verbatim apart from a removed trailing `/`
* *AND* a partition whose value count differs from the partition-key count SHALL fail with an error naming the partition's location

### Scenario: Every listing follows its continuation tokens and stops on an empty page

* *GIVEN* `GetTables` and `GetPartitions` results split across pages, where the last non-empty page carries a non-null `NextToken` and the following page is empty
* *WHEN* the client lists tables or partitions
* *THEN* the client SHALL request the next page while the response carries a non-empty `NextToken` and a non-empty result list
* *AND* the client SHALL stop on an absent or empty `NextToken` or on an empty page, and MUST NOT stop on a page shorter than the maximum page size
* *AND* the client SHALL request partitions with `ExcludeColumnSchema` set, because the reader reads only values, locations, and formats

### Scenario: The CatalogId is sent only when the CONNECTION names one, and the NAMESPACE names one database

* *GIVEN* one CONNECTION without `warehouse` and one with `warehouse` `123456789012`, and the NAMESPACE values `sales` and `sales.eu`
* *WHEN* the client lists the namespace
* *THEN* the client SHALL send `CatalogId` `123456789012` for the second CONNECTION and MUST NOT send a `CatalogId` for the first, never an empty string
* *AND* `sales` SHALL name the Glue database
* *AND* `sales.eu` SHALL fail with an error stating that a Glue NAMESPACE names exactly one database

### Scenario: Service errors are classified by their error code, not their HTTP status

* *GIVEN* Glue answers `400 EntityNotFoundException` for a missing database, the same for a missing table, `400 AccessDeniedException`, and `400 InvalidInputException`
* *WHEN* the client handles each answer
* *THEN* the client SHALL classify each error by the service error code, and MUST NOT classify it by the HTTP status
* *AND* a missing database or table SHALL fail with an error naming it and stating that it does not exist
* *AND* every error SHALL name the Glue operation, the error code, and the service message
* *AND* no error SHALL contain a credential value

### Scenario: Throttling and server errors are retried within a bounded time

* *GIVEN* a Glue endpoint that answers `ThrottlingException` or HTTP 503 twice and then succeeds, and a second endpoint that answers `ThrottlingException` to every attempt
* *WHEN* the client calls each endpoint
* *THEN* the client SHALL retry with exponential backoff and jitter, up to 5 attempts per call, so the first call succeeds
* *AND* each call SHALL succeed or fail within 30 seconds, retries included, because it runs inside a pushdown
* *AND* the second call SHALL fail with an error naming the operation and `ThrottlingException`

### Scenario: A clock-skew signing failure names the clock

* *GIVEN* a Glue endpoint that rejects every signature because the request time differs from the service time
* *WHEN* the client calls it
* *THEN* the client SHALL correct its signing time from the service time and retry, as the SDK does
* *AND* a rejection that persists SHALL fail with an error stating that the Exasol node's clock differs from AWS time
* *AND* that error MUST NOT read as a credential error

### Scenario: The client signs only with the CONNECTION's credentials

* *GIVEN* environment variables `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, and `AWS_REGION` naming another identity and region, and an AWS profile file
* *WHEN* the client signs a request
* *THEN* the client SHALL sign with the CONNECTION's `access_key`, `secret_key`, and optional `session_token`, for the resolved signing region
* *AND* the client MUST NOT read a credential or a region from the environment, a profile file, or instance metadata
