# Type matrix baseline on unchanged main

Run: `LH_EXASOL_CPUSET=0-1 cargo test --features exasol-e2e --test e2e_type_matrix_test -- --test-threads=1` against Docker Exasol 2025.1.16, MinIO, and `iceberg-rest-fixture`, with the UDF `.so` built by `make cross-udf-build` from `main` (75cb878) plus test-only changes. Result: both tests FAIL, with 8 mismatches on direct storage and 4 on Iceberg. Every mismatch is a binary-family row that expects the refusal of `vs-adapter/binary-column-refusal`. Every other row matches its expectation.

Display texts below are the `value_to_string` of the Exasol WebSocket cell (`SELECT id, <col> ... ORDER BY id`, rows 1, 2, 3; row 3 is NULL in every column). Every declaration is `SYS.EXA_ALL_COLUMNS.COLUMN_TYPE`, whitespace and the `UTF8` suffix removed.

## Direct storage (`TYPE_MATRIX_DS`, `s3://warehouse/type_matrix/`)

| Column | Source type | Declared | Row 1, row 2 | Matches |
|---|---|---|---|---|
| c_int8 | Int8 | DECIMAL(3,0) | 127, -128 | yes |
| c_int16 | Int16 | DECIMAL(5,0) | 32767, -32768 | yes |
| c_int32 | Int32 | DECIMAL(10,0) | 2147483647, -2147483648 | yes |
| c_int64 | Int64 | DECIMAL(20,0) | 9223372036854775807, -9223372036854775808 | yes |
| c_uint8 | UInt8 | DECIMAL(3,0) | 255, 0 | yes |
| c_uint16 | UInt16 | DECIMAL(5,0) | 65535, 0 | yes |
| c_uint32 | UInt32 | DECIMAL(20,0) | 4294967295, 0 | yes |
| c_uint64 | UInt64 | DECIMAL(20,0) | 18446744073709551615, 0 | yes |
| c_float32 | Float32 | DOUBLE | 1.5, -0.25 | yes |
| c_float64 | Float64 | DOUBLE | 2.5, -0.125 | yes |
| c_boolean | Boolean | BOOLEAN | true, false | yes |
| c_utf8 | Utf8 | VARCHAR(2000000) | héllo, wörld | yes |
| c_largeutf8 | LargeUtf8 | VARCHAR(2000000) | héllo, wörld | yes |
| c_decimal128_10_2 | Decimal128(10,2) | DECIMAL(10,2) | 12.34, -0.05 | yes |
| c_decimal128_38_10 | Decimal128(38,10) | VARCHAR(2000000) | 1234567890123456789012345678.9012345678, -0.0000000005 | yes (display text recorded) |
| c_decimal256_50_2 | Decimal256(50,2) | VARCHAR(2000000) | 12.34, -0.05 | yes (display text recorded) |
| c_date32 | Date32 | DATE | 2024-01-15, 1970-01-01 | yes |
| c_timestamp | Timestamp(us) | TIMESTAMP(3) (bare `TIMESTAMP`) | 2024-01-15 10:30:45.123000, 1970-01-01 00:00:00.000000 | yes |
| c_time32 | Time32(ms) | VARCHAR(2000000) | 12:34:56, 00:00:00 | yes (display text recorded) |
| c_time64 | Time64(us) | VARCHAR(2000000) | 12:34:56.123456, 00:00:00 | yes (display text recorded) |
| c_duration | Duration(us) | VARCHAR(2000000) | `0 days 0 hours 0 mins 1.500000 secs`, `0 days 0 hours 0 mins 0.000000 secs` | yes (display text recorded) |
| c_interval | Interval(DayTime) | VARCHAR(2000000) | `1 days 2.000 secs`, `0 secs` | yes (display text recorded) |
| c_list | List<Int32> | VARCHAR(2000000) | `[1,2]`, `[]` | yes |
| c_struct | Struct<a: Int32, b: Utf8> | VARCHAR(2000000) | `{"a":1,"b":"x"}`, `{"a":2,"b":null}` | yes |
| c_map | Map<Utf8, Int32> | VARCHAR(2000000) | `{"k1":1,"k2":2}`, `{}` | yes |
| c_int96 (annotated) | INT96 | TIMESTAMP(3) | 2024-01-15 10:30:45.000000, 1970-01-01 00:00:00.000000 | yes |
| c_enum (annotated) | BYTE_ARRAY (ENUM) | VARCHAR(2000000) | red, green | yes |
| c_binary | Binary | VARCHAR(2000000) | error: `VM error: F-UDF-CL-RUST-9001: UDF error: UDF run returned error code 1: Type error: emit_batch: IPC read: Invalid argument error: Invalid UTF8 sequence at string index 0 (0..4): invalid utf-8 sequence of 1 bytes from index 0` | no, expects refusal |
| c_largebinary | LargeBinary | VARCHAR(2000000) | same error as c_binary | no, expects refusal |
| c_fixedsizebinary | FixedSizeBinary(16) | VARCHAR(2000000) | error: `scan failed: assigned data could not be read: Execution error: Cannot cast column 'c_fixedsizebinary' from 'FixedSizeBinary(16)' (physical data type) to 'Utf8' (logical data type): Error during planning: Cannot cast struct field 'c_fixedsizebinary' from type FixedSizeBinary(16) to type Utf8` | no, expects refusal |
| c_uuid (annotated) | FIXED_LEN_BYTE_ARRAY(16) (UUID) | VARCHAR(2000000) | error: same cast error, naming `c_uuid` | no, expects refusal |
| c_byte_array (annotated) | BYTE_ARRAY, valid UTF-8, no annotation, no Arrow schema | VARCHAR(2000000) | legacy-a, legacy-b | no, expects refusal |
| c_struct_binary | Struct<x: Binary> | VARCHAR(2000000) | `{"x":"fffe0080"}`, `{"x":"c328"}` (hexadecimal) | no, expects refusal |
| c_struct_enum (annotated) | Struct<k: ENUM> | VARCHAR(2000000) | `{"k":"626c7565"}`, `{"k":"616d626572"}` (hexadecimal) | no, expects refusal |
| c_bytes (binary_values) | BYTE_ARRAY, not valid UTF-8, no annotation | VARCHAR(2000000) | error: `Type error: emit_batch: IPC read: Invalid argument error: Invalid UTF8 sequence at string index 0 (0..4): invalid utf-8 sequence of 1 bytes from index 0` | no, expects refusal |

`SELECT id` returns 1, 2, 3 on every table.

## Iceberg (`TYPE_MATRIX_ICEBERG`, namespace `e2e_type_matrix`)

Every row that Iceberg can hold matches its expectation, with the same declarations and display texts as direct storage, except:

| Column | Source type | Declared | Row 1, row 2 | Matches |
|---|---|---|---|---|
| c_timestamp | timestamp | TIMESTAMP(6) | 2024-01-15 10:30:45.123456, 1970-01-01 00:00:00.000000 | yes |
| c_binary | binary | VARCHAR(2000000) | error: same `emit_batch ... Invalid UTF8 sequence` error as direct storage | no, expects refusal |
| c_fixedsizebinary | fixed(16) | VARCHAR(2000000) | error: same `Cannot cast column 'c_fixedsizebinary'` error | no, expects refusal |
| c_uuid | uuid | VARCHAR(2000000) | error: `Cannot cast column 'c_uuid' from 'FixedSizeBinary(16)' (physical data type) to 'Utf8' (logical data type)` | no, expects refusal |
| c_struct_binary | struct<x: binary> | VARCHAR(2000000) | `{"x":"fffe0080"}`, `{"x":"c328"}` (hexadecimal) | no, expects refusal |
| c_bytes (binary_values) | string over bytes that are not valid UTF-8 | VARCHAR(2000000) | error: `scan failed: assigned data could not be read: Parquet error: Arrow: Parquet argument error: Parquet error: encountered non UTF-8 data: invalid utf-8 sequence of 1 bytes from index 0` | yes, fails as required |

## Stop-condition check (task 0.4)

| Condition | Observed | Holds |
|---|---|---|
| A declaration or non-binary value contradicts `datafusion-scan/type-mapping` or `datafusion-scan/nested-json-rendering`, or a `CAST(col AS VARCHAR)` type fails the scan | Every declaration matches the mapping table. Every non-binary value matches the native value, the nested JSON document, or the Arrow display text. Decimal128(38,10), Decimal256, Time32, Time64, Duration, and Interval scan without error. | no |
| The top-level `ENUM` column does not return its UTF-8 text | `c_enum` returns `red`, `green`. | no |
| The direct-storage `binary_values` column returns its bytes faithfully | It fails the query with an Arrow IPC UTF-8 error. A refusal removes no working rendering. | no |
| The Iceberg `binary_values` column returns an altered value instead of failing | It fails with `encountered non UTF-8 data`. | no |

No stop condition holds. The group needs no plan return.

## Observations that are not stop conditions

* Direct storage declares a `Timestamp(us)` and an `INT96` column as `TIMESTAMP(3)`, the bare `TIMESTAMP` of `arrow_to_exasol_type`, while Iceberg declares `TIMESTAMP(6)`. This is the recorded split (`datafusion-scan/type-mapping-timestamp-precision`: Arrow-input stays bare, catalog-declared follows the version gate). The row expectations state it.
* A valid-UTF-8 unannotated `BYTE_ARRAY` (`c_byte_array`) reads as text on `main`. Refusing it is the user-approved breaking change of `vs-adapter/binary-column-refusal`, so the row expects the refusal.
* A nested `ENUM` member and a nested `binary` member render as hexadecimal inside the JSON document on `main`, which confirms the Background of `vs-adapter/binary-column-refusal`.
* No new bug needs an Open Questions entry. No row was set to an observed outcome with `(#TBD)`.

## Fixture notes for the next groups

* The Iceberg `binary_values` data file is written with `parquet`'s `SerializedFileWriter` and committed through a `DataFileBuilder` fast-append. `iceberg-rust`'s writer fails `create_and_append` on invalid UTF-8 (`close data file writer ... invalid utf-8 sequence`), so the plan's `seed.rs create_and_append` route cannot hold this row.
* The `Refused` expectation lists fragments that must all appear in the error (`["binary", "#351"]`, `["fixed(16)", "#351"]`, `["uuid", "#351"]`, `["enum"]`). The `#351` fragment keeps a column name that contains `uuid` from passing on the cast error. Group D1 tightens the fragments to the final refusal text.
* The `glue` column of the matrix, `glue-e2e` cfg, and the Glue `string` over `BYTE_ARRAY` case belong to task 7.1 and are not in this table yet.

## Spec compliance baseline (text for plan.md § Spec compliance)

Direct-storage baseline on `main`: every Arrow type of the matrix declares its recorded Exasol type and returns its recorded value. The binary family (Binary, LargeBinary, FixedSizeBinary, UUID, an unannotated `BYTE_ARRAY`, a nested binary member, a nested `ENUM` member) is the only set that departs from the post-plan expectation, and each departure is the refusal this plan adds.
