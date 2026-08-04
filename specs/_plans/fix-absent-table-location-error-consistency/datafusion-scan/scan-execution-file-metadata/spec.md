# Feature: DataFusion Scan Execution — File Metadata (No-HEAD Registration)

Extends `datafusion-scan/scan-execution` with how the scan UDF turns a per-shard `(path,
size)` file entry into a registered, absolute, sized file — without issuing a per-file
object-store `HEAD` request the adapter's already-resolved size makes redundant — and extends
the same no-HEAD guarantee to the associated positional-delete files.

## Background

* **The empty-table-root clauses are retained as a wire-format totality property, not as a
  reachable path.** `vs-adapter/pushdown-planning` now rejects a `loadTable` response carrying
  an empty table metadata `location` before the vended/static storage split, so the adapter can
  no longer emit a common spec whose table root is empty. This feature's three empty-table-root
  clauses — two normative `SHALL` clauses and one descriptive Background bullet — therefore
  describe an input the current adapter cannot produce. Those three clauses are the recorded
  Background bullet beginning "When the common spec carries an empty table root" and the final
  clause of each of the two scenarios reproduced below. They are retained deliberately and their
  text is UNCHANGED: they make the path-resolution rule a total function over the wire format, so
  a scan spec reaching the UDF with an empty root still resolves deterministically instead of
  joining paths onto nothing. They MUST NOT be deleted or converted into an error — an empty root
  is unreachable from a `loadTable` response, which makes the branch unreachable rather than dead,
  and the UDF is not the component that validates a catalog response. The scan-side rejoin this
  property governs is `reconstruct_abs_uri`
  (`crates/lakehouse-engine/src/scan/object_store.rs:250`).

## Scenarios

The two scenarios below are reproduced with their normative text BYTE-IDENTICAL to the recorded
spec. They appear here only to name the empty-table-root clauses the Background bullet retains,
so merging this delta changes no scenario wording.

<!-- DELTA:CHANGED -->
### Scenario: Relative paths resolve against the table root and absolute paths pass through

* *GIVEN* a scan invocation whose common spec carries a non-empty Iceberg table root and whose per-shard files argument mixes relative entries (paths under that root) with at least one absolute entry (a path not under the root, carrying its own `://` scheme)
* *WHEN* the scan UDF resolves its assigned files for registration
* *THEN* the UDF SHALL join each relative entry onto the table root (normalizing the boundary `/`) to form the absolute URI, and SHALL pass each already-absolute entry through unchanged
* *AND* the set of registered absolute file URIs SHALL equal the original resolved data-file URIs the adapter partitioned into this shard
* *AND* when the common spec carries an empty table root, the UDF SHALL treat every entry as absolute and join none of them
<!-- /DELTA:CHANGED -->

<!-- DELTA:CHANGED -->
### Scenario: Delete-file relative and absolute paths resolve like data-file paths

* *GIVEN* a scan invocation whose common spec carries a non-empty Iceberg table root and whose per-shard files argument mixes relative delete-file entries (paths under that root) with at least one absolute delete-file entry (a path not under the root)
* *WHEN* the scan UDF resolves a data file's associated delete files for reading
* *THEN* the UDF SHALL join each relative delete-file entry onto the table root to form its absolute URI and SHALL pass each already-absolute delete-file entry through unchanged, exactly as it does for data-file paths
* *AND* when the common spec carries an empty table root, the UDF SHALL treat every delete-file entry as absolute and join none of them
<!-- /DELTA:CHANGED -->
