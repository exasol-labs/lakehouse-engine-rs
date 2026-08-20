# Feature: Pushdown Planning — File-List Encoding (Table Root + Relative Paths)

Extends pushdown planning (`vs-adapter/pushdown-planning`) with the per-shard file-list wire
encoding: the table root is carried once in the shard-invariant common spec, and each
per-shard file entry (data file and its associated delete files) is emitted relative to that
root when the root is an actual prefix of the file's path, or as an absolute URI otherwise.

## Background

* **This delta corrects format-scoped naming and is issue #324. The encoding is unchanged, byte for byte.** The table root is the neutral `table_root` the format reader resolves — `table.metadata().location()` on the Iceberg side, the Delta table location on the Delta side — so the strip-when-prefix rule this feature owns applies to every resolved file list. Naming it "the Iceberg table root" narrowed a rule that was never narrow.
* **The Iceberg-spec grounding for the CONDITIONAL strip stays scoped to Iceberg and is NOT generalized.** The recorded reason — Iceberg data-file paths are not guaranteed to live under `metadata.location()` because of `write.data.path`, `write.object-storage.enabled` hash injection, and migrated/Databricks layouts — is an Iceberg statement with Iceberg evidence. This delta states no equivalent Delta claim, because none has been checked against the Delta protocol. The rule needs no such claim: it is conditional for every format, and a path that IS under the root is stripped and a path that is not is passed through, whichever reader produced it.

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: Table root is carried once and paths under it are emitted relative

* *GIVEN* a resolved data-file list in which every data-file URI lies under the table root the format reader resolved for that table
* *WHEN* the adapter builds the scan-driving SQL
* *THEN* the adapter SHALL carry the table root exactly once in the shard-invariant common spec argument
* *AND* for each file whose URI begins with the table root, the adapter SHALL strip that root prefix and emit only the remaining relative path in the per-shard argument, so the repeated table-location prefix is shipped once rather than once per file
* *AND* the reconstructed absolute path (table root joined with the relative entry) SHALL equal the original resolved data-file URI
<!-- /DELTA:CHANGED -->

<!-- DELTA:CHANGED -->
### Scenario: A data-file path not under the table root is carried as an absolute path

* *GIVEN* a resolved data-file list containing at least one data-file URI that does NOT lie under the table root (on the Iceberg side, for example a `write.data.path` / `write.object-storage.enabled` hash-injected, migrated, or Databricks layout)
* *WHEN* the adapter builds the scan-driving SQL
* *THEN* the adapter SHALL emit that file's full absolute URI unchanged in the per-shard argument, stripping the table root ONLY from paths for which the root is an actual prefix
* *AND* the adapter MUST NOT strip a partial or non-prefix match, so no absolute path is ever corrupted into an unresolvable relative path
* *AND* a per-shard payload MAY mix relative entries (paths under the root) and absolute entries (paths not under the root) within the same query
* *AND* the strip decision SHALL be made from the path and the root ALONE, so one path and root produce one encoding whichever format reader resolved them
<!-- /DELTA:CHANGED -->
