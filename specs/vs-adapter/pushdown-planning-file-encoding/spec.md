# Feature: Pushdown Planning — File-List Encoding (Table Root + Relative Paths)

Extends pushdown planning (`vs-adapter/pushdown-planning`) with the per-shard file-list wire
encoding: the table root is carried once in the shard-invariant common spec, and each
per-shard file entry (data file and its associated delete files) is emitted relative to that
root when the root is an actual prefix of the file's path, or as an absolute URI otherwise.

## Background

* The table root is the neutral value the format reader resolves at the resolve-once seam — the
  Iceberg reader's own source is `table.metadata().location()`, which doubles as its
  vended-credential anchor. The root is shard-invariant, so it is carried ONCE in the common
  scan-spec argument — never repeated per shard.
* A file path is emitted RELATIVE to the table root only when the root is an actual prefix of
  the path; any path not under the root is emitted unchanged as an absolute URI.
* Iceberg data-file paths are NOT guaranteed to live under `metadata.location()` —
  `write.data.path`, `write.object-storage.enabled` hash injection, and migrated/Databricks
  layouts can place files elsewhere — so stripping is conditional, never unconditional.
* Delete-file paths follow the SAME relative/absolute encoding as data-file paths, and each
  delete-file entry additionally carries its delete content type.
* A CONNECTION-supplied storage credential is carried as a connection REFERENCE and MUST NOT appear in any returned SQL. A VENDED storage credential appears in a returned SQL string ONLY inside the AES-GCM-sealed envelope of `vs-adapter/scan-spec-credential-reference` — issue [#378](https://github.com/exasol-labs/lakehouse-engine-rs/issues/378), CLOSED by that feature — never in plaintext. No credential of either kind appears in an error message.

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
* *THEN* the adapter SHALL encode each delete-file path with the SAME rule it applies to data-file paths — relative (root-stripped) when the table root is an actual prefix, absolute unchanged otherwise
* *AND* the reconstructed absolute delete-file path (table root joined with a relative entry, or an absolute entry passed through) SHALL equal the original resolved delete-file URI
* *AND* the adapter MUST NOT corrupt an absolute delete-file path by stripping a non-prefix match

### Scenario: Each delete-file entry carries its content type so the scan can reject non-positional deletes

* *GIVEN* a resolved file list whose data files carry associated delete files
* *WHEN* the adapter emits each data file's associated delete-file references into the per-shard files argument
* *THEN* the adapter SHALL carry, per delete-file entry, its delete content type (e.g. positional) alongside its path and byte size, so the scan UDF's read-time backstop can reject any non-positional delete file
* *AND* the per-delete-file surface SHALL be limited to path, byte size, and content type — no additional Iceberg metadata is carried per delete file

### Scenario: The encoded scan spec carries a credential reference, not a credential

* *GIVEN* a pushdown request whose per-shard file lists are encoded against a shared table root, over a virtual schema whose CONNECTION supplies static storage credentials and does not enable `use_vended_credentials`
* *WHEN* the adapter renders the scan-driving SQL for that request
* *THEN* the returned SQL string MUST NOT contain the CONNECTION's `access_key`, `secret_key`, `session_token`, `account_key`, or `sas_token` value in any encoding, because the shard-invariant common scan-spec argument carries a connection REFERENCE under `vs-adapter/scan-spec-credential-reference`
* *AND* the same request with `use_vended_credentials` enabled SHALL carry the vended credential ONLY inside the sealed envelope `vs-adapter/scan-spec-credential-reference` specifies — issue #378, closed by this plan — so no credential value appears in PLAINTEXT in that SQL under either setting
* *AND* no credential value of either kind SHALL appear in any error message this feature's path raises
