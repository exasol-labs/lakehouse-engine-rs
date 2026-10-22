# Feature: Pushdown Planning, File-List Encoding (Table Root + Relative Paths)

Extends pushdown planning (`pushdown/pushdown-planning`) with the per-shard file-list wire encoding. The table root is carried once in the shard-invariant common spec. Each per-shard file entry (data file and its delete files) is relative to that root when the root is a prefix of the path, and an absolute URI otherwise.

## Background

* The table root is the value the format reader resolves for the table. For Iceberg it is `table.metadata().location()`.
* A path is emitted relative only when the root is an actual prefix of it, because Iceberg data-file paths can live outside `metadata.location()` (`write.data.path`, `write.object-storage.enabled` hash injection, migrated or Databricks layouts).
* Delete-file paths use the same relative or absolute encoding, and each delete-file entry also carries its delete content type.

## Scenarios

### Scenario: Table root is carried once and paths under it are emitted relative

* *GIVEN* a resolved data-file list in which every data-file URI lies under the table root the format reader resolved for that table
* *WHEN* the adapter builds the scan-driving SQL
* *THEN* the adapter SHALL carry the table root exactly once in the shard-invariant common spec argument
* *AND* for each file whose URI begins with the table root, the adapter SHALL strip that root prefix and emit only the remaining relative path in the per-shard argument, so the repeated table-location prefix is shipped once rather than once per file
* *AND* the reconstructed absolute path (table root joined with the relative entry) SHALL equal the original resolved data-file URI

### Scenario: A data-file path not under the table root is carried as an absolute path

* *GIVEN* a resolved data-file list containing at least one data-file URI that does NOT lie under the table root (on the Iceberg side, for example a `write.data.path` / `write.object-storage.enabled` hash-injected, migrated, or Databricks layout)
* *WHEN* the adapter builds the scan-driving SQL
* *THEN* the adapter SHALL emit that file's full absolute URI unchanged in the per-shard argument, stripping the table root ONLY from paths for which the root is an actual prefix
* *AND* the adapter MUST NOT strip a partial or non-prefix match, so no absolute path is ever corrupted into an unresolvable relative path
* *AND* a per-shard payload MAY mix relative entries (paths under the root) and absolute entries (paths not under the root) within the same query
* *AND* the strip decision SHALL be made from the path and the root ALONE, so one path and root produce one encoding whichever format reader resolved them

### Scenario: Delete-file paths use the same relative/absolute encoding as data files

* *GIVEN* a resolved merge-on-read file list whose data files carry associated Parquet positional-delete files, some under the table root and (possibly) some not
* *WHEN* the adapter builds the scan-driving SQL
* *THEN* the adapter SHALL encode each delete-file path with the SAME rule it applies to data-file paths, relative (root-stripped) when the table root is an actual prefix, absolute unchanged otherwise
* *AND* the reconstructed absolute delete-file path (table root joined with a relative entry, or an absolute entry passed through) SHALL equal the original resolved delete-file URI
* *AND* the adapter MUST NOT corrupt an absolute delete-file path by stripping a non-prefix match

### Scenario: Each delete-file entry carries its content type so the scan can reject unsupported deletes

* *GIVEN* a resolved file list whose data files carry associated delete files
* *WHEN* the adapter emits each data file's associated delete-file references into the per-shard files argument
* *THEN* the adapter SHALL carry, per delete-file entry, its delete content type (positional delete, Puffin deletion vector, or equality delete) alongside its path and byte size, so the scan UDF's read-time backstop can reject an equality delete
* *AND* a Puffin deletion-vector entry SHALL also carry the blob's `content_offset` and `content_size_in_bytes` and the manifest's `record_count`, because the scan fetches only that blob and checks its cardinality
* *AND* every other delete-file entry SHALL be limited to path, byte size, and content type, with no additional Iceberg metadata

### Scenario: Generated SQL carries a credential reference, not a credential

* *GIVEN* a pushdown request through this feature's path
* *WHEN* the adapter generates the pushdown SQL
* *THEN* the credential-reference guarantee of `storage-access/scan-spec-credential-reference` SHALL apply, so no credential value appears in generated SQL or error messages
