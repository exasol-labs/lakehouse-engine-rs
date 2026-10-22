# Feature: DataFusion Scan Execution, Iceberg Deletion Vector Application

The scan UDF applies Iceberg v3 deletion vectors on read, so a query over a merge-on-read v3 table returns the post-delete rows (#12).

## Background

The normative shapes this feature reads, quoted from the Iceberg table spec and the Puffin spec (`apache/iceberg`, `format/spec.md` and `format/puffin-spec.md`, tag `apache-iceberg-1.12.0`):

- Manifest fields: `referenced_data_file`: *"Setting the referenced file is required for deletion vectors."* `content_offset` and `content_size_in_bytes`: *"these values are required and must exactly match the `offset` and `length` stored in the Puffin footer for the deletion vector blob."* `record_count`: *"Number of records in this file, or the cardinality of a deletion vector"*.
- Scan Planning: a position delete file applies only when *"There is no deletion vector that must be applied to the data file"*.
- Delete Formats: *"there can be at most one deletion vector for a given data file in a snapshot."*
- Puffin `deletion-vector-v1`: *"Omit `compression-codec`; `deletion-vector-v1` is not compressed"*.

The adapter carries each deletion vector as the Puffin deletion-vector delete mechanism: the Puffin file's path and size, the blob's offset and length, and the manifest's `record_count`. Which vector applies to which data file follows iceberg-rust's delete matching, which also drops a data file's positional delete files when a vector applies to it.

The scan fetches only the blobs' byte ranges, because one Puffin file holds the vectors of many data files. It merges the ranges of one Puffin file that lie less than 1 MiB apart (object_store's `OBJECT_STORE_COALESCE_DEFAULT`) into one request of at most 8 MiB. Each request holds its own permit of the connection-budget limiter that every delete read shares. It decodes each blob as soon as its request returns, with iceberg-rust's decoder, and feeds the positions into the pipeline that every row-position delete mechanism shares (`scan-read-path/scan-execution-row-position-deletes`).

## Scenarios

### Scenario: Iceberg deletion vectors are applied on read

* *GIVEN* a virtual schema over an Iceberg v3 table whose data files carry deletion vectors, including two vectors stored in one Puffin file, a vector that replaced an earlier one, and a data file that keeps a format-version-2 positional delete file
* *WHEN* a query reads the table
* *THEN* the scan SHALL emit no row that a data file's vector marks deleted, and every other row unchanged
* *AND* the result SHALL be the same however the data files are distributed across shards

### Scenario: Iceberg deletion vectors compose with projection, filter, LIMIT, and aggregates

* *GIVEN* a virtual schema over an Iceberg v3 table whose data files carry deletion vectors
* *WHEN* a query projects columns, filters, applies ORDER BY with LIMIT, or aggregates
* *THEN* the rows that the filter, the LIMIT, and the aggregates see SHALL already exclude the deleted rows

### Scenario: A shard fetches only its own blobs of a shared Puffin file within the connection budget

* *GIVEN* a shard whose assigned data files carry deletion vectors stored as blobs of one Puffin file
* *WHEN* the scan reads those vectors
* *THEN* the scan SHALL fetch only the byte ranges of the blobs that the shard's data files need, never the whole Puffin file
* *AND* the scan SHALL fetch blobs that lie less than 1 MiB apart in one ranged request, unless that request would exceed 8 MiB
* *AND* each ranged request SHALL hold its own permit of the shared size-N limiter, so the shard's in-flight Puffin reads never exceed N
* *AND* each data file SHALL receive the positions of its own blob only

### Scenario: An Iceberg deletion vector that cannot be applied fails the scan

* *GIVEN* a data file whose deletion vector's blob range lies outside its Puffin file, whose blob fails its length, magic, or CRC-32 check, or whose decoded positions disagree with the manifest's `record_count`
* *WHEN* the scan prepares that data file
* *THEN* the scan SHALL return an error naming the data file before it emits any row of that file
* *AND* the error MUST NOT contain any storage access key, secret key, or session token
