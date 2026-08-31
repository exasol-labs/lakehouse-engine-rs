# Feature: Pushdown Planning

Translates an Exasol query against the virtual schema into a pushdown plan: it captures
the requested projection, filter, LIMIT, and any supported aggregate, and emits the SQL
that drives the DataFusion scan over the table identity, file list, byte sizes,
delete-file references, and logical schema resolved once by
`vs-adapter/pushdown-planning-file-resolution`. Cluster fan-out is separated from the
scan: a nested `LAKEHOUSE_DISTRIBUTE_FILES` LUA SET distributor subquery (`GROUP BY
shard_key`) spreads each shard's per-file list across nodes, and an outer ungrouped
`LAKEHOUSE_SCAN` SCALAR EMIT UDF scans each distributed file list node-locally and streams
the rows. The scan-driving SQL splices the shard-invariant parts (projection, filter,
LIMIT, logical schema, a storage credential reference or a vended credential inline,
and the table root) once as the scalar scan
UDF's first-argument common literal and flows each shard's per-file subset through the
distributor as the second argument. A single-shard plan short-circuits the distributor and
calls the scalar scan directly on the file-list literal. See
`vs-adapter/pushdown-planning-file-encoding` for the table-root-once and relative/absolute
path encoding rules. See `vs-adapter/pushdown-planning-nested-aggregate-fallback` for the
guard against composed requests (e.g. an outer aggregate over an inner grouped-aggregate
sub-select) that don't map onto the source table's own columns. Single-group aggregate
pushdown (capability advertisement, partial-aggregate scan-spec translation, wrapper merge
SQL, and AVG sum/count decomposition) is covered separately in
`vs-adapter/pushdown-planning-single-group-agg`.

## Background

* **This delta is issue #135. It adds ONE scenario, amends two Background enumerations and one description sentence, and changes no planning rule.** Projection, filter, LIMIT, the distributor fan-out, the schema qualification, the node-count handshake read, the empty-`projection` rule for aggregate shapes, and the declined-filter self-apply correction are all UNCHANGED.
* **SUPERSEDES this feature's recorded Background bullet "Credentials MUST NOT appear in any returned SQL string or error message, and MUST NOT be repeated per shard."** That sentence was FALSE against the implemented tree: `crates/lakehouse-engine/src/adapter/pushdown/support.rs:441` serialized the resolved storage backend into a SQL string literal with no encoding, and the committed golden fixtures contain `"access_key"` and `"secret_key"` in plaintext. The scoped replacement: a CONNECTION-supplied storage credential is carried as a connection REFERENCE and does not appear in the returned SQL; a VENDED storage credential still appears there under the tracked exception issue [#378](https://github.com/exasol-labs/lakehouse-engine-rs/issues/378); no credential of either kind appears in an error message. The once-per-fan-out rule is UNCHANGED and applies to the reference exactly as it applied to the credential.
* **SUPERSEDES the two enumerations that list "credentials" among the shard-invariant common spec's contents** — the feature description's parenthetical and the first Background bullet's. The common spec now carries, per side, EITHER a reference to the Exasol CONNECTION that supplies that side's storage credentials OR an inline storage backend the planning layer vended.
* **`vs-adapter/scan-spec-credential-reference` owns the reference contract, the one pure variant-selection function, the resolution, the required grant, the #378 residual, and the builder-path guarantee test.** This feature CITES it and restates none of it.
* **This feature is the parent of the pushdown-planning family, so the scoped claim is stated here once** and each sibling feature carrying the same unscoped bullet has its own delta in this plan.

## Scenarios

<!-- DELTA:NEW -->
### Scenario: The common scan-spec literal carries a credential reference, not a credential

* *GIVEN* a pushdown request of any shape over a virtual schema whose CONNECTION supplies static storage credentials and does not enable `use_vended_credentials`
* *WHEN* the adapter splices the shard-invariant common spec into the scan-driving SQL as the scalar scan UDF's first argument
* *THEN* that literal SHALL carry the value of the `CATALOG_CONNECTION` virtual-schema property plus the resolved `ALLOW_HTTP` value as its storage reference, and the returned SQL string MUST NOT contain the CONNECTION's `access_key`, `secret_key`, `session_token`, `account_key`, or `sas_token` value in any encoding
* *AND* the reference SHALL be spliced ONCE for the whole fan-out and MUST NOT be repeated per shard, exactly as the credential was, because it is shard-invariant
* *AND* the same request with `use_vended_credentials` enabled SHALL carry the vended credential INLINE in that literal — the tracked exception issue #378 — so this feature's credential claim is SCOPED to CONNECTION-supplied credentials and MUST NOT be read as unconditional
* *AND* no credential value of either kind SHALL appear in any error message the pushdown path raises
<!-- /DELTA:NEW -->
