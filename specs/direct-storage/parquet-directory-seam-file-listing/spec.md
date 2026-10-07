# Feature: Parquet Directory Seam: File Listing

This feature covers how the Parquet directory seam lists the data files under a storage prefix. It lists recursively in a deterministic order, applies one of two file patterns, and excludes hidden segments and zero-length objects. It applies a file-keep predicate to each file's partition values before any footer is read, and it lists a catalog-registered location by its raw object key.

It also covers the listing answer, which serves a caller whose catalog already declares the schema and the partition columns. That answer returns the files and their partition values, reads no footer, and shares its file rules with the schema-resolving answer.

## Background

* The seam names NO catalog kind, NO table format, and NO Exasol virtual-schema property. It takes
  an object store, a prefix, a file pattern, a merge mode, a partitioning switch, and a file-keep
  predicate over partition values. Any later consumer that needs "the Parquet files under this
  prefix and their schema" calls it directly.
* It answers TWO questions. The schema-resolving answer serves direct-storage table enumeration,
  which maps the folded schema to the neutral catalog columns the listing pipeline declares, and
  direct-storage query planning, which maps it to the scan spec's logical fields. One
  implementation is what keeps `MERGE_SCHEMA` and `HIVE_PARTITIONING` from needing a second policy
  per path. The listing answer serves the catalog-declared Parquet reader
  (`unity-catalog/unity-parquet-table-planning`, `glue/glue-table-planning`), whose catalog
  declares the schema.
* The plain listing step, which applies the file pattern and the fixed hidden-segment rule, is
  separate from the folder-name partition inference layered on it. The Glue reader uses the plain
  step alone, because Glue supplies each partition's values.
* Hive-style partition discovery belongs to this seam. `direct-storage/direct-storage-hive-partitioning`
  specifies its rules.
* See `direct-storage/parquet-directory-seam` for the seam's single-function contract, the merge mode, and the schema fold over Parquet footers.

## Scenarios

### Scenario: Data files are listed recursively in a deterministic order

* *GIVEN* a storage prefix holding `p1.parquet`, `a/p2.parquet`, `a/b/p3.parquet`, `_SUCCESS`, `_metadata`, `p1.parquet.crc`, `_staging/p4.parquet`, and `.hidden/p5.parquet`
* *WHEN* the seam lists that prefix's data files under the Parquet-at-any-depth file pattern
* *THEN* it SHALL return `p1.parquet`, `a/p2.parquet`, and `a/b/p3.parquet`, recursing to unlimited depth, so a plain subdirectory contributes files rather than being skipped or becoming its own unit
* *AND* it SHALL return ONLY objects whose name ends in `.parquet`, so `_SUCCESS`, `_metadata`, and `p1.parquet.crc` are excluded by that rule alone
* *AND* it SHALL exclude every object any of whose path segments below the prefix begins with `_` or `.`, so `_staging/p4.parquet` and `.hidden/p5.parquet` are excluded even though their file names qualify
* *AND* it SHALL carry each returned file's byte size from the listing response, so no consumer issues an object-store HEAD for a size the listing already reported
* *AND* it SHALL return the files in a DETERMINISTIC order that does not depend on the store's listing order, because the sample-one-file mode reads the same footer on the enumeration path and the plan path only when both see one order

### Scenario: A file-keep predicate narrows the files before any footer is read

* *GIVEN* a prefix whose files sit under `year=2025/` and `year=2026/` directories, and a file-keep predicate that rejects every file whose `year` value is not `2026`
* *WHEN* the seam resolves that prefix
* *THEN* it SHALL declare the partition columns from the UNFILTERED listing, so the declared columns never depend on the predicate
* *AND* it SHALL evaluate the predicate on each file's partition values before it reads any footer, and SHALL return only the kept files
* *AND* the fold-every-file mode SHALL read the footers of the kept files alone, so a rejected file costs no footer read
* *AND* the sample-one-file mode SHALL still read the footer of the first file in the UNFILTERED listing, kept or not, so the enumeration path and the plan path sample the same file
* *AND* table enumeration SHALL pass a predicate that keeps every file

### Scenario: The listing answer serves a caller that declares its own partition columns

* *GIVEN* a storage prefix holding `year=2024/region=eu/a.parquet`, `Year=2025/b.parquet`, `other=x/c.parquet`, and `_SUCCESS`, and a caller that declares the partition columns `year` and `region`
* *WHEN* the caller asks the seam for that prefix's files against those declared columns
* *THEN* the seam SHALL return the files the recursive-listing rules of this feature select, in the same deterministic order, each with its byte size and its partition values
* *AND* each file's partition-value map SHALL carry EVERY declared column, keyed by the caller's spelling and filled from the path segment whose key equals that column under the uppercase fold, the deepest such segment winning, so `Year=2025` fills `year` and a column with no segment reads no value
* *AND* a segment naming no declared column SHALL contribute nothing, so `other=x` is a plain directory

### Scenario: The listing answer reads no footer and shares its file rules with the schema-resolving answer

* *GIVEN* the same declaring caller and storage prefix
* *WHEN* the caller asks the seam for that prefix's files against its declared columns
* *THEN* the seam SHALL read NO footer, fold NO schema, and declare NO partition column from the paths, because the caller owns the schema, and the file-keep predicate SHALL run on the filled values before the files are returned, exactly as it does for the schema-resolving answer
* *AND* the listing answer SHALL share the listing, the data-file rule, the segment parser, and the value decoding with the schema-resolving answer, so the two answers cannot disagree about which objects are data files or how a segment value decodes

### Scenario: The file pattern selects the listing depth and the file-name rule

* *GIVEN* a storage prefix holding `a.parquet`, `b` (no extension), `sub/c.parquet`, `sub/d`, `_SUCCESS`, `.hidden`, and a zero-length object `e.parquet`
* *WHEN* the seam lists that prefix under each of its two file patterns, Parquet-at-any-depth and any-direct-child
* *THEN* Parquet-at-any-depth SHALL return `a.parquet` and `sub/c.parquet`, objects at any depth whose name ends in `.parquet`
* *AND* any-direct-child SHALL return `a.parquet` and `b`, the prefix's direct children of any name
* *AND* any-direct-child SHALL list through a delimiter listing, so no object below a subdirectory is fetched
* *AND* under both patterns, an object with a segment below the prefix that begins with `_` or `.`, and a zero-length object, SHALL NOT be a data file
* *AND* the pattern SHALL be an internal parameter: direct storage and Unity Parquet pass Parquet-at-any-depth, Glue passes any-direct-child, and no virtual-schema property sets it

### Scenario: A catalog-registered location is listed by its raw object key

* *GIVEN* the Glue location `s3://bucket/tbl/p_str=a b%2Fc/` and an object stored at the literal key `tbl/p_str=a b%2Fc/f1`
* *WHEN* the seam derives the store prefix of that location as a raw key and lists it
* *THEN* the prefix SHALL be `tbl/p_str=a b%2Fc`, taken verbatim without percent-decoding, so the listing returns `f1`
* *AND* the path the seam returns for `f1` SHALL resolve back to the same object key through the scan's path reconstruction
* *AND* the seam SHALL decode a direct-storage or Unity Parquet storage URI as a URL
