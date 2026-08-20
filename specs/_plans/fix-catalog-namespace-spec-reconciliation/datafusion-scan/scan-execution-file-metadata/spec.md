# Feature: DataFusion Scan Execution — File Metadata (No-HEAD Registration)

Extends `datafusion-scan/scan-execution` with how the scan UDF turns a per-shard `(path,
size)` file entry into a registered, absolute, sized file — without issuing a per-file
object-store `HEAD` request the adapter's already-resolved size makes redundant — and extends
the same no-HEAD guarantee to the associated positional-delete files.

## Background

* **This delta corrects format-scoped naming in two scenario GIVENs and is issue #324. It changes no behavior.** The common blob's `table_root` is a neutral field both format readers populate, so the relative/absolute resolution rule this feature owns is reached identically by an Iceberg scan and a Delta one. Naming it "the Iceberg table root" narrowed a rule that was never narrow.

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: Relative paths resolve against the table root and absolute paths pass through

* *GIVEN* a scan invocation whose common spec carries a non-empty table root and whose per-shard files argument mixes relative entries (paths under that root) with at least one absolute entry (a path not under the root, carrying its own `://` scheme)
* *WHEN* the scan UDF resolves its assigned files for registration
* *THEN* the UDF SHALL join each relative entry onto the table root (normalizing the boundary `/`) to form the absolute URI, and SHALL pass each already-absolute entry through unchanged
* *AND* the set of registered absolute file URIs SHALL equal the original resolved data-file URIs the adapter partitioned into this shard
* *AND* when the common spec carries an empty table root, the UDF SHALL treat every entry as absolute and join none of them
* *AND* this rule SHALL apply to a spec produced by EITHER format reader, because the table root is a neutral field both populate
<!-- /DELTA:CHANGED -->

<!-- DELTA:CHANGED -->
### Scenario: Delete-file relative and absolute paths resolve like data-file paths

* *GIVEN* a scan invocation whose common spec carries a non-empty table root and whose per-shard files argument mixes relative delete-file entries (paths under that root) with at least one absolute delete-file entry (a path not under the root)
* *WHEN* the scan UDF resolves a data file's associated delete files for reading
* *THEN* the UDF SHALL join each relative delete-file entry onto the table root to form its absolute URI and SHALL pass each already-absolute delete-file entry through unchanged, exactly as it does for data-file paths
* *AND* when the common spec carries an empty table root, the UDF SHALL treat every delete-file entry as absolute and join none of them
<!-- /DELTA:CHANGED -->
