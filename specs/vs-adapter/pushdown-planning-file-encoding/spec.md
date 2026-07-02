# Feature: Pushdown Planning — File-List Encoding (Table Root + Relative Paths)

Extends pushdown planning (`vs-adapter/pushdown-planning`) with the per-shard file-list wire
encoding: the Iceberg table root is carried once in the shard-invariant common spec, and each
per-shard file entry is emitted relative to that root when the root is an actual prefix of the
file's path, or as an absolute URI otherwise.

## Background

* The Iceberg table root (`table.metadata().location()`, already resolved at the resolve-once
  seam as the vended-credential anchor) is a shard-invariant value, so it is carried ONCE in the
  common scan-spec argument — never repeated per shard.
* A file path is emitted RELATIVE to the table root only when the root is an actual prefix of
  the path; any path not under the root is emitted unchanged as an absolute URI.
* Iceberg data-file paths are NOT guaranteed to live under `metadata.location()` —
  `write.data.path`, `write.object-storage.enabled` hash injection, and migrated/Databricks
  layouts can place files elsewhere — so stripping is conditional, never unconditional.
* Credentials MUST NOT appear in any returned SQL string or error message.

## Scenarios

### Scenario: Table root is carried once and paths under it are emitted relative

* *GIVEN* a resolved data-file list in which every data-file URI lies under the Iceberg table root (`table.metadata().location()`)
* *WHEN* the adapter builds the scan-driving SQL
* *THEN* the adapter SHALL carry the table root exactly once in the shard-invariant common spec argument
* *AND* for each file whose URI begins with the table root, the adapter SHALL strip that root prefix and emit only the remaining relative path in the per-shard argument, so the repeated table-location prefix is shipped once rather than once per file
* *AND* the reconstructed absolute path (table root joined with the relative entry) SHALL equal the original resolved data-file URI

### Scenario: A data-file path not under the table root is carried as an absolute path

* *GIVEN* a resolved data-file list containing at least one data-file URI that does NOT lie under the Iceberg table root (for example a `write.data.path` / `write.object-storage.enabled` hash-injected, migrated, or Databricks layout)
* *WHEN* the adapter builds the scan-driving SQL
* *THEN* the adapter SHALL emit that file's full absolute URI unchanged in the per-shard argument, stripping the table root ONLY from paths for which the root is an actual prefix
* *AND* the adapter MUST NOT strip a partial or non-prefix match, so no absolute path is ever corrupted into an unresolvable relative path
* *AND* a per-shard payload MAY mix relative entries (paths under the root) and absolute entries (paths not under the root) within the same query
