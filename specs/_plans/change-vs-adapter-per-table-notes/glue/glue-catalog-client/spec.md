# Feature: Glue Catalog Client

Reads the AWS Glue Data Catalog API for the `GLUE` catalog kind. The client lists the tables of one Glue database, routes each table to the Iceberg reader, the Parquet reader, or a visible skip, and returns the registered partitions of a table. It implements the shared `CatalogClient` trait, so the shared createVirtualSchema listing pipeline lists its tables.

<!-- DELTA:CHANGED -->
## Background

* The client calls `GetTables`, `GetTable`, and `GetPartitions` through `aws-sdk-glue`, which retries throttling and transient errors, parses the service error code, and corrects its signing time from the service time before it retries.
* The identifier-only listing calls `GetTables` with `AttributesToGet` `NAME`. Glue then returns only each table's name and pages with `NextToken` like an unfiltered call.
* AWS Glue API, `CatalogId`: "If none is provided, the AWS account ID is used by default." Glue rejects an empty value with `400 InvalidInputException "CatalogId ID cannot be empty."`.
* Glue returns service errors as HTTP 400 with a JSON body whose `__type` names the error, including a missing table or database (`EntityNotFoundException`).
* AWS Glue API, `GetPartitions` response `NextToken`: "A continuation token, if the returned list of partitions does not include the last one." Glue can return a non-null `NextToken` after the last page.
* A partition's `Values` are positional against the table's `PartitionKeys`. Hive writes a NULL partition value as the literal `__HIVE_DEFAULT_PARTITION__`.
<!-- /DELTA:CHANGED -->

## Scenarios

<!-- DELTA:NEW -->
### Scenario: A scoped refresh lists Glue table names and loads only the requested tables

* *GIVEN* a Glue database and a partial refresh per `vs-adapter/refresh-and-set-properties`
* *WHEN* the adapter lists the database's identifiers through the shared trait and then loads the tables it selects
* *THEN* the identifier-only listing SHALL call `GetTables` with `AttributesToGet` `NAME`, follow continuation tokens per the scenario "Every listing follows its continuation tokens and stops on an empty page", and return one neutral identifier per table, views and skipped tables included
* *AND* the load SHALL call `GetTable` once per given identifier and route or skip each table per the scenario "The client routes a table by its declared table type before its storage descriptor"
* *AND* an admitted table SHALL equal the neutral table the full listing returns, with an Iceberg table's columns read per the scenario "An Iceberg table takes its columns from its current metadata file"
<!-- /DELTA:NEW -->
