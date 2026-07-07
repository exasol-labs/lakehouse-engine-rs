# Feature: Pushdown Planning — File-List Encoding (Table Root + Relative Paths)

Extends pushdown planning (`vs-adapter/pushdown-planning`) with the per-shard file-list wire
encoding: the Iceberg table root is carried once in the shard-invariant common spec, and each
per-shard file entry (data file and its associated delete files) is emitted relative to that root
when the root is an actual prefix of the file's path, or as an absolute URI otherwise.

## Background

* The Iceberg table root (`table.metadata().location()`) is shard-invariant and carried ONCE in the
  common spec, never repeated per shard.
* A path is emitted RELATIVE only when the root is an actual prefix of it; any path not under the
  root is emitted unchanged as an absolute URI, so an absolute path is never corrupted.
* Delete-file paths follow the SAME relative/absolute encoding as data-file paths, and each
  delete-file entry additionally carries its delete content type.

## Scenarios

<!-- DELTA:NEW -->
### Scenario: Delete-file paths use the same relative/absolute encoding as data files

* *GIVEN* a resolved merge-on-read file list whose data files carry associated Parquet positional-delete files, some under the table root and (possibly) some not
* *WHEN* the adapter builds the scan-driving SQL
* *THEN* the adapter SHALL encode each delete-file path with the SAME rule it applies to data-file paths — relative (root-stripped) when the table root is an actual prefix, absolute unchanged otherwise
* *AND* the reconstructed absolute delete-file path (table root joined with a relative entry, or an absolute entry passed through) SHALL equal the original resolved delete-file URI
* *AND* the adapter MUST NOT corrupt an absolute delete-file path by stripping a non-prefix match
<!-- /DELTA:NEW -->

<!-- DELTA:NEW -->
### Scenario: Each delete-file entry carries its content type so the scan can reject non-positional deletes

* *GIVEN* a resolved file list whose data files carry associated delete files
* *WHEN* the adapter emits each data file's associated delete-file references into the per-shard files argument
* *THEN* the adapter SHALL carry, per delete-file entry, its delete content type (e.g. positional) alongside its path and byte size, so the scan UDF's read-time backstop can reject any non-positional delete file
* *AND* the per-delete-file surface SHALL be limited to path, byte size, and content type — no additional Iceberg metadata is carried per delete file
<!-- /DELTA:NEW -->
