# Feature: Format-Neutral Pushdown Resolution

Routes every pushdown request through the table-format reader seam, so the ONE thing a pushdown needs
from a table's format — its active file list, its logical schema, its partition columns, its table
root, and the storage those were resolved through — is produced by the reader that owns that format
and consumed by a pipeline that names no format at all. This is what makes a Delta table queryable:
the refusal that blocked every Unity Catalog pushdown is replaced by a resolution seam, not by a
Delta-shaped branch, so a single-table scan, each leg of a broadcast join, and every aggregate shape
reach a Delta table by the same route they reach an Iceberg one.

## Background

* **This delta is issue #135. It amends ONE scenario and changes no resolution rule.** The one format-reader seam, the one catalog session per request, the single catalog-kind match site, the Unity identity round trip, the resolver collapse, the partition-column propagation, the loud plan-time failure, and the kind-blind capability advertisement are all UNCHANGED.
* **SUPERSEDES this feature's byte-identity gate for the `storage` value alone.** The generated SQL and the serialized per-shard scan specs stay byte-identical EXCEPT for the `storage` value, which becomes the tagged wrapper of `vs-adapter/scan-spec-credential-reference`. Every other byte of every request's output is unchanged, which is what keeps this gate meaningful rather than waived.

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: Iceberg pushdown output is byte-identical across the rewiring

* *GIVEN* the shipped Iceberg pushdown suites — the golden dispatch fixtures, the scan-spec encodings,
  the generated scan-driving SQL for raw, TopN, single-group aggregate, grouped aggregate, and
  broadcast-join requests, and the Iceberg E2E suites
* *WHEN* those requests are planned through the format-reader seam instead of the direct file resolver
* *THEN* the generated SQL and the serialized per-shard scan specs MUST be byte-identical to their
  pre-feature output for every request EXCEPT for the `storage` value, which carries the tagged
  wrapper of `vs-adapter/scan-spec-credential-reference`
* *AND* every existing test MUST pass with no change to any assertion or expected value
* *AND* the Iceberg-level file pruning the resolver applies from the request's filter SHALL still be
  applied, because the filter is forwarded to the reader unchanged
<!-- /DELTA:CHANGED -->
