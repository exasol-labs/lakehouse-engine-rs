# Feature: Binary Column Refusal

Refuses at plan time every column whose type is binary, on every source and at every depth, until issue #351 defines a faithful rendering for binary. The listing still declares each such column, so only a query that reads it fails.

## Background

* Iceberg declares three binary types. Iceberg table spec, § Primitive Types: `binary` is an "Arbitrary-length byte array", `fixed(L)` is a "Fixed-length byte array of length L", and `uuid` holds "Universally unique identifiers" and "Should use 16-byte fixed". Refusing them is a deliberate, scoped exception until #351.
* Delta, Unity Catalog, and Glue declare one binary type, `binary`.
* A catalog-declared source decides by its declared type, never by a data file's physical type.
* Direct storage declares no table type, so it decides by what each Parquet file declares. Parquet LogicalTypes, § STRING: "`STRING` may only be used to annotate the `BYTE_ARRAY` primitive type and indicates that the byte array should be interpreted as a UTF-8 encoded character string." § ENUM: "Applications using a data model lacking a native enum type should interpret `ENUM` annotated field as a UTF-8 encoded string." § UUID: "`UUID` annotates a 16-byte `FIXED_LEN_BYTE_ARRAY` primitive type."
* `parquet` 58.3.0 (the locked version) folds an unannotated `BYTE_ARRAY` and the `ENUM`, `BSON`, `GEOMETRY`, and `GEOGRAPHY` annotations to Arrow `Binary` (`src/arrow/schema/primitive.rs:280-300`), and a `UUID`-annotated or unannotated `FIXED_LEN_BYTE_ARRAY(L)` to `FixedSizeBinary(L)` (`:348`). An embedded Arrow schema can fold an unannotated `BYTE_ARRAY` to `LargeBinary` or `BinaryView` instead.
* An unannotated `BYTE_ARRAY` holds either a legacy string (Impala and Hive write strings this way) or arbitrary bytes, and the footer cannot tell them apart. Refusing it and a `UUID` column is a user-approved breaking change.
* A top-level `ENUM` column is text by the Parquet spec, so it is read as text. A nested `ENUM` member is refused, because the JSON renderer reads a member's physical type and would render the text as hexadecimal (#TBD).
* The refusal reason names the type in the source's own terms: `binary`, `fixed(L)`, or `uuid`, and on direct storage also `bson`, `geometry`, or `geography`.

## Scenarios

### Scenario: A binary column is refused on every catalog-declared format at every depth

* *GIVEN* an Iceberg table with columns `id int`, `b binary`, `f fixed(16)`, `u uuid`, and `s struct<x: binary>`, and a Delta, a Unity Parquet, and a Glue Parquet table each with an `id` and a `binary` column
* *WHEN* the pushdown plans, for each table, a `SELECT` of each binary column, a `SELECT *`, a filter on a binary column, `SELECT id`, and `SELECT COUNT(*)`
* *THEN* every request that reads or emits a binary column, or a column containing a binary member, SHALL fail at plan time with a reason naming the column, its declared type (`binary`, `fixed(16)`, or `uuid`), and issue #351
* *AND* `SELECT id` and `SELECT COUNT(*)` SHALL succeed on every table
* *AND* a table whose every column is refused SHALL be refused as a whole

### Scenario: The listing declares a binary column

* *GIVEN* the tables of the first scenario, and a direct-storage directory whose files carry an unannotated `BYTE_ARRAY` column and a `UUID` column
* *WHEN* createVirtualSchema lists them
* *THEN* each binary column SHALL be declared `VARCHAR(2000000)`, so the virtual table lists every column and only a query that reads the column fails

### Scenario: A catalog string column over binary file data reads as text

* *GIVEN* a Glue Parquet table whose catalog declares `name string`, and a data file that stores `name` as a Parquet `BYTE_ARRAY` without the string annotation
* *WHEN* a query reads `name`
* *THEN* the scan SHALL admit the column and render it as text (`datafusion-scan/type-relaxation`), because the catalog declares it a string
* *AND* a value that is not valid UTF-8 SHALL fail the query and MUST NOT be returned altered
* *AND* a catalog-declared source SHALL refuse a column only by its declared type

### Scenario: A direct-storage unannotated BYTE_ARRAY column is refused

* *GIVEN* a direct-storage directory whose Parquet files carry an `id` column, a `legacy_name` column stored as a `BYTE_ARRAY` with no annotation and no embedded Arrow schema, a `name` column stored as a `STRING`-annotated `BYTE_ARRAY`, and an `s` struct column whose member is an unannotated `BYTE_ARRAY`
* *WHEN* the pushdown plans `SELECT legacy_name`, `SELECT s`, `SELECT *`, a filter on `legacy_name`, `SELECT id, name`, and `SELECT COUNT(*)`
* *THEN* each request that reads or emits `legacy_name` or `s` SHALL fail at plan time with a reason naming the column, the type `binary`, and issue #351
* *AND* `SELECT id, name` and `SELECT COUNT(*)` SHALL succeed
* *AND* a column that an embedded Arrow schema folds to `LargeBinary` or `BinaryView` SHALL be refused naming `binary`, and a `BSON`, `GEOMETRY`, or `GEOGRAPHY` column naming `bson`, `geometry`, or `geography`
* *AND* a directory whose every column is refused SHALL be refused as a whole

### Scenario: A direct-storage ENUM column reads as text

* *GIVEN* a direct-storage directory whose Parquet files carry an `id` column, a `kind` column stored as an `ENUM`-annotated `BYTE_ARRAY`, and an `s` struct column whose member `k` is `ENUM`-annotated
* *WHEN* the pushdown plans `SELECT id, kind` and `SELECT s`
* *THEN* `SELECT id, kind` SHALL succeed and return each `kind` value as its UTF-8 text
* *AND* `SELECT s` SHALL fail at plan time with a reason naming `s`, the member `k`, and the type `enum` (#TBD)

### Scenario: A direct-storage UUID or unannotated fixed-length column is refused

* *GIVEN* a direct-storage directory whose Parquet files carry an `id` column, a `uid` column stored as a `UUID`-annotated `FIXED_LEN_BYTE_ARRAY(16)`, and a `digest` column stored as an unannotated `FIXED_LEN_BYTE_ARRAY(16)`
* *WHEN* the pushdown plans `SELECT uid`, `SELECT digest`, and `SELECT id`
* *THEN* `SELECT uid` SHALL fail at plan time with a reason naming `uid`, the type `uuid`, and issue #351
* *AND* `SELECT digest` SHALL fail at plan time with a reason naming `digest`, the type `fixed(16)`, and issue #351
* *AND* `SELECT id` SHALL succeed
