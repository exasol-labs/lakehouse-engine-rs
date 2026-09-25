# Feature: Delta Table Planning

Resolves a Delta Lake table into the engine's existing `ScanSpec` shape at plan time, so file-level
sharding, the pushdown wire format, streaming emit, and the memory model are reused for the Delta
path exactly as they already are for Iceberg.

## Background

* Unchanged by this plan. This section is unmarked, so `speq record` ignores it and the recorded Background survives intact. See `specs/vs-adapter/delta-table-planning/spec.md`.

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: Delta planning resolves its storage credential through the table's own catalog

* *GIVEN* a Delta table registered in a Unity Catalog, and a CONNECTION that names no
  `aws_assume_role_arn` (`vs-adapter/connection-credentials-assume-role` owns that case) and that
  either enables `use_vended_credentials` or supplies static storage credentials
* *WHEN* the Delta format reader resolves that table's scan
* *THEN* under vending the reader SHALL request per-table, short-lived, scoped credentials from that
  Unity Catalog against the table's own catalog-assigned vending key, and SHALL terminate the
  response in a `StorageBackend` through the ONE shared vended-storage policy
  (`vs-adapter/unity-catalog-vended-credentials`), so the `abfs://` plaintext-consent gate and the S3
  address rule apply identically to the Iceberg and the Delta path
* *AND* under vending the reader SHALL fail with an error naming the table when its catalog reported
  no vending key, and MUST NOT fall back to the CONNECTION's static credential, because a silent
  fallback would read object storage with a credential the operator did not select for this table
* *AND* with vending disabled the reader SHALL use the CONNECTION's static storage backend unchanged
* *AND* the reader SHALL return the EFFECTIVE storage backend alongside the file list, and the
  shard-invariant common spec SHALL carry that backend SEALED (the AES-GCM envelope of `vs-adapter/scan-spec-credential-reference`) under vending and a REFERENCE to the CONNECTION that supplied it with vending disabled, both of which the scan UDF resolves to a field-for-field equal backend under that same feature
* *AND* every error the reader surfaces from this point on SHALL be redacted against the effective
  storage's secret values, and MUST NOT contain any vended or static credential value
<!-- /DELTA:CHANGED -->
