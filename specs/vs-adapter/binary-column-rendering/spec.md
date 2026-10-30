# Feature: Binary Column Rendering

A binary column reads as lowercase hexadecimal text on every source and at every depth. Exasol has no binary type, and lowercase hex is the default text format of `HASHTYPE`, so a user can cast the result to `HASHTYPE`.

## Background

* Iceberg declares three binary types: `binary`, `fixed(L)`, and `uuid` (Iceberg table spec, Primitive Types). Delta, Unity Catalog, and Glue declare one, `binary`.
* A catalog-declared source decides by its declared type, never by a data file's physical type.
* Direct storage declares no table type, so it decides by what each Parquet file declares. The locked `parquet` crate folds an unannotated `BYTE_ARRAY` and the `BSON`, `ENUM`, `GEOMETRY`, and `GEOGRAPHY` annotations to Arrow `Binary`, and a `UUID`-annotated or unannotated `FIXED_LEN_BYTE_ARRAY(L)` to `FixedSizeBinary(L)`.
* Exasol rejects `HASHTYPE` as a UDF output type, so the column is declared `VARCHAR`.
* Exasol stores an empty `VARCHAR` as NULL, so an empty binary value reads as NULL.
* Lowercase hex sorts in the order of the bytes, so a range filter, `MIN`, `MAX`, and `ORDER BY` over the text match the bytes.
* Exasol pushes a filter literal on a binary column as a plain string literal. Exasol compares strings case-sensitively, and only lowercase hex of whole bytes names a byte string.
* `BINARY_AS_STRING` mirrors Spark's `spark.sql.parquet.binaryAsString` for writers that store text as binary, such as Impala, Hive, and early Spark.

## Scenarios

### Scenario: A binary column reads as lowercase hex on every source

* *GIVEN* an Iceberg table with `binary`, `fixed(16)`, `uuid`, `struct<x: binary>`, `map<binary, int>`, and `map<uuid, int>` columns, a Delta table with a `binary` column under no column mapping and under `name` column mapping, a `struct<x: binary>` column, and a `map<binary, int>` column, a Unity Parquet and a Glue Parquet table each with a `binary` and a `struct<x: binary>` column, and a direct-storage directory with unannotated `BYTE_ARRAY`, `BSON`, `UUID`, and unannotated `FIXED_LEN_BYTE_ARRAY(16)` columns
* *AND* the variable-length columns hold a value that is not valid UTF-8, an empty value, and NULL
* *WHEN* a query selects each column
* *THEN* the scan SHALL return each value as its lowercase hexadecimal text, with no byte altered
* *AND* an empty value and NULL SHALL both read as NULL
* *AND* a nested binary member SHALL render as a lowercase hexadecimal JSON string inside the column's JSON document, and an empty member as `""`
* *AND* a binary, `fixed(L)`, or `uuid` map key SHALL render as its lowercase hexadecimal JSON object name, and an empty key as `""`

### Scenario: The listing declares a binary column

* *GIVEN* the tables of the first scenario
* *WHEN* createVirtualSchema lists them
* *THEN* a `fixed(L)` or `uuid` column, and a direct-storage `FIXED_LEN_BYTE_ARRAY(L)` column, SHALL be declared `VARCHAR(2L)`
* *AND* every other binary column SHALL be declared `VARCHAR(2000000)`

### Scenario: A filter on a binary column selects what native Exasol selects over the hex text

* *GIVEN* a binary column and a native Exasol copy of its hex text
* *WHEN* a query filters the column with `=`, `<>`, `<`, `<=`, `>`, `>=`, `IN`, `BETWEEN`, `IS NULL`, `IS NOT NULL`, `LIKE`, a string function, `NOT`, `OR`, an uppercase or odd-length literal, or orders or aggregates it with `ORDER BY`, `MIN`, or `MAX`
* *THEN* the query SHALL return the rows the same query returns on the native copy
* *AND* a comparison, `IN`, or `BETWEEN` against a lowercase hex literal of whole bytes, which the filter's root reaches through `AND` and `OR` only, SHALL compare the raw bytes with a binary literal, so Parquet statistics prune and row filtering stays on
* *AND* such a comparison on a variable-length column whose operator admits the empty value (`<>`, `<`, `<=`) SHALL also require a non-empty value
* *AND* a `fixed(L)` column SHALL compare the raw bytes only against a literal of `L` bytes
* *AND* every other use SHALL operate on the hex text, with row filtering on and statistics pruning lost

### Scenario: A binary value too long for its hex rendering fails the query

* *GIVEN* a binary column holding a value of more than 1,000,000 bytes
* *WHEN* a query reads the column
* *THEN* the scan SHALL fail the query with an error naming the column and the value's size
* *AND* a query that reads no such value SHALL succeed

### Scenario: BINARY_AS_STRING reads a variable-length binary column as UTF-8 text

* *GIVEN* a virtual schema whose `BINARY_AS_STRING` property is `TRUE`
* *WHEN* a query reads, filters, or aggregates a top-level variable-length binary column of any table and source
* *THEN* the adapter SHALL pass the property to the scan as a flag, and the declared column type SHALL stay `VARCHAR(2000000)`
* *AND* the scan SHALL keep the column's type binary and convert each value to text through a function that validates it, reusing the binary buffers without copying
* *AND* a value that is not valid UTF-8 SHALL fail the query with an error naming the column, also when the query filters or aggregates the column without returning it, and MUST NOT be returned altered or as NULL
* *AND* the column's filters SHALL use plain string semantics, with row filtering on, and the adapter MUST NOT rewrite them into byte comparisons
* *AND* a `fixed(L)` or `uuid` column, and a binary member of a nested column, SHALL keep its hex rendering
* *AND* a change of the property through `ALTER VIRTUAL SCHEMA ... SET` SHALL apply from the next query, with no `REFRESH`

### Scenario: A catalog string column over binary file data reads as text

* *GIVEN* a Glue Parquet table whose catalog declares `name string`, and a data file that stores `name` as a Parquet `BYTE_ARRAY` without the string annotation
* *WHEN* a query reads `name`
* *THEN* the scan SHALL render it as text (`scan-types/type-relaxation`), because the catalog declares it a string
* *AND* a value that is not valid UTF-8 SHALL fail the query and MUST NOT be returned altered

### Scenario: A direct-storage ENUM column reads as text

* *GIVEN* a direct-storage directory whose Parquet files carry an `id` column, a `kind` column stored as an `ENUM`-annotated `BYTE_ARRAY`, and an `s` struct column whose member `k` is `ENUM`-annotated
* *WHEN* the pushdown plans `SELECT id, kind` and `SELECT s`
* *THEN* `SELECT id, kind` SHALL succeed and return each `kind` value as its UTF-8 text
* *AND* `SELECT s` SHALL fail at plan time with a reason naming `s`, the member `k`, and the type `enum`, because the JSON renderer would print the member's text as hex

### Scenario: A geospatial column is refused

* *GIVEN* an Iceberg `geometry` or `geography` column, or a direct-storage `GEOMETRY`- or `GEOGRAPHY`-annotated column, at any depth
* *WHEN* the pushdown plans a request that reads or emits the column
* *THEN* the request SHALL fail at plan time with a reason naming the column, the member when the column nests one, and the type
* *AND* the listing SHALL still declare the column `VARCHAR(2000000)`
