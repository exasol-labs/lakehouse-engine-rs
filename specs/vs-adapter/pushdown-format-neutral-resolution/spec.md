# Feature: Format-Neutral Pushdown Resolution

Routes every pushdown request through the table-format reader seam, so the ONE thing a pushdown needs
from a table's format — its active file list, its logical schema, its partition columns, its table
root, and the storage those were resolved through — is produced by the reader that owns that format
and consumed by a pipeline that names no format at all. This is what makes a Delta table queryable:
the refusal that blocked every Unity Catalog pushdown is replaced by a resolution seam, not by a
Delta-shaped branch, so a single-table scan, each leg of a broadcast join, and every aggregate shape
reach a Delta table by the same route they reach an Iceberg one.

## Background

The seam already exists and is exercised only by tests: a `FormatReader` returns a `ResolvedScan`,
`format_reader` selects the reader by exhaustively matching a `ScanSource`, and both concrete readers
are private to that module. Production pushdown reaches none of it — it calls the Iceberg-only file
resolver directly, from the single-table path and from each join leg, and the adapter refuses a Unity
Catalog pushdown outright before any of that runs.

Resolution economy is a recorded property this feature preserves: a request resolves its catalog
session ONCE and reuses it for every table it touches, so a two-leg join performs no more catalog
authentication round-trips than a single-table scan.

Pushdown SQL shape is format-agnostic in this engine: projection, filter, LIMIT, ORDER BY, aggregate
decomposition, and join eligibility are decided from the request's SQL alone and are unchanged by
this feature. Only file resolution differs per format.

* **This delta is issue #135. It amends ONE scenario and changes no resolution rule.** The one format-reader seam, the one catalog session per request, the single catalog-kind match site, the Unity identity round trip, the resolver collapse, the partition-column propagation, the loud plan-time failure, and the kind-blind capability advertisement are all UNCHANGED.
* **SUPERSEDES this feature's byte-identity gate for the `storage` value alone.** The generated SQL and the serialized per-shard scan specs stay byte-identical EXCEPT for the `storage` value, which becomes the tagged wrapper of `vs-adapter/scan-spec-credential-reference`. Every other byte of every request's output is unchanged, which is what keeps this gate meaningful rather than waived.

## Scenarios

### Scenario: Every pushdown request shape resolves through the one format-reader seam

* *GIVEN* four pushdown requests over the SAME virtual schema — a single-table projection scan, a
  single-table aggregate, a grouped aggregate, and a broadcast inner equi-join with two legs
* *WHEN* the adapter plans each request
* *THEN* the adapter SHALL obtain each table's file list, logical schema, partition columns, table
  root, name mapping, and effective storage from a `ResolvedScan` returned by the format-reader seam,
  and MUST NOT call any format-specific file resolver directly
* *AND* the adapter SHALL apply that rule to EVERY leg of the join, so a join leg and a single-table
  scan resolve identically
* *AND* the adapter MUST NOT gate any request shape on the table format or the catalog kind, so
  enabling a format enables every shape at once and a shape that fails does so as a defect rather than
  as a refusal
* *AND* the SQL the adapter generates for each request SHALL be decided from the request alone, so the
  same request over an Iceberg table and over a Delta table with the same columns yields the same
  pushdown decisions

### Scenario: One catalog session per request serves every table the request resolves

* *GIVEN* a broadcast-join pushdown request whose two legs name two different tables in one virtual
  schema, under either catalog kind
* *WHEN* the adapter resolves both legs
* *THEN* the adapter SHALL build the request's catalog session EXACTLY ONCE and SHALL resolve both legs
  through it, so the request performs no more catalog authentication round-trips than a single-table
  request over the same virtual schema
* *AND* the adapter MUST NOT build a second session per leg, per shape, or per format reader
* *AND* the number of catalog round-trips an Iceberg request performs SHALL be unchanged from before
  this feature

### Scenario: The catalog kind is matched at one added construction site and nowhere else

* *GIVEN* the resolved catalog kind and the recorded rule that its variant names appear in no
  production module beyond the enum's own declaration, its resolver, the catalog-client construction
  site, credential validation, and the pushdown refusal
* *WHEN* the pushdown path builds the per-request scan-source resolver
* *THEN* the adapter SHALL match the catalog kind EXHAUSTIVELY at exactly ONE site in the pushdown
  path, which yields the per-request resolver, so a third catalog kind is a compile error there rather
  than a silent fall-through
* *AND* that site SHALL REPLACE the pushdown refusal in the recorded list of production sites
  permitted to name a catalog-kind variant, so the permitted-site count is unchanged and no
  per-request-shape fork is introduced
* *AND* the source-level probe asserting that list SHALL be updated to name the construction site
  instead of the refusal, and MUST NOT be weakened into permitting an unnamed set of sites
* *AND* the format-reader selection site MUST NOT match the catalog kind, because it matches the scan
  source

### Scenario: A Unity Catalog table's identity survives the round trip from the involved table

* *GIVEN* a pushdown request under the Unity Catalog kind whose involved virtual table name maps,
  through the identifier recorded at create time, to a three-level Unity Catalog identifier
* *WHEN* the adapter resolves that identifier into the table the Delta reader plans
* *THEN* the adapter SHALL recover the catalog table identifier's namespace segments and table name
  from the recorded identifier and SHALL load exactly that table from the Unity Catalog
* *AND* the loaded table SHALL be the one whose catalog-reported full name equals the recorded
  identifier, because the Unity Catalog addresses a table by that same dotted full name and the
  segmentation is therefore lossless
* *AND* a recorded identifier that yields no table name SHALL fail with an error naming the
  unresolvable identifier, and MUST NOT resolve a different table
* *AND* a pushdown request whose involved virtual table name is absent from the recorded mapping SHALL keep failing with the error naming the unknown virtual table, unchanged for both catalog kinds

### Scenario: Iceberg pushdown output is byte-identical across the rewiring

* *GIVEN* the shipped Iceberg pushdown suites — the golden dispatch fixtures, the scan-spec encodings,
  the generated scan-driving SQL for raw, TopN, single-group aggregate, grouped aggregate, and
  broadcast-join requests, and the Iceberg E2E suites
* *WHEN* those requests are planned through the format-reader seam instead of the direct file resolver
* *THEN* the generated SQL and the serialized per-shard scan specs MUST be byte-identical to their
  pre-feature output for every request EXCEPT for the `storage` value, which carries the tagged
  wrapper of `vs-adapter/scan-spec-credential-reference`
* *AND* every existing test MUST pass with no change to any assertion or expected value, EXCEPT for
  the two edits this delta's `storage` carve-out forces and no others: the eighteen credential-bearing
  golden dispatch fixtures are REGENERATED so their `storage` value carries the wrapper, and
  `common_blob_wire_is_byte_stable`'s pinned bytes gain the wrapper around the same backend encoding
* *AND* those two edits MUST change the `storage` value and nothing else — the six `empty_*` fixtures
  carry no `storage` value and SHALL stay byte-identical, and no assertion MUST be weakened, disabled,
  or deleted to accommodate the change
* *AND* the Iceberg-level file pruning the resolver applies from the request's filter SHALL still be
  applied, because the filter is forwarded to the reader unchanged

### Scenario: The Iceberg file resolver is collapsed into its reader and leaves the façade

* *GIVEN* the Iceberg format reader, whose whole body forwards its arguments to the separately
  published Iceberg file-resolution function, kept in that shape only until this feature removed its
  direct callers
* *WHEN* production pushdown and every test call site route through the format-reader seam
* *THEN* the file resolver's body SHALL move INTO the Iceberg reader and the separately published
  function SHALL be DELETED, so exactly one path resolves an Iceberg file list
* *AND* no production or test call site SHALL name that function afterwards; every caller SHALL reach
  the same resolution through the format-reader seam
* *AND* the compile-time signature pin asserting that Iceberg file resolution takes a SHARED catalog
  session SHALL be retained against the scan source that now carries the session, so the shared-session
  contract keeps a compile-time guard
* *AND* the Iceberg reader SHALL still return EMPTY partition columns, EMPTY partition values on every
  file entry, and a field-id on every logical field, so the Iceberg wire encoding is unchanged

### Scenario: Resolved partition columns reach the scan spec for every side

* *GIVEN* a pushdown request over a partitioned table whose resolved scan carries ordered partition
  column names, planned once as a single-table scan and once as the broadcast side of a join whose
  other side is a different partitioned table
* *WHEN* the adapter assembles the per-shard scan specs
* *THEN* the shard-invariant common spec SHALL carry the SCANNED table's partition column names, and
  each per-shard file entry SHALL carry that file's partition values, so the scan can materialize them
* *AND* the join spec SHALL carry the BROADCAST side's OWN partition column names alongside that
  side's file list, logical schema, name mapping, table root, and storage, so neither side is given the
  other's partition columns
* *AND* a request whose resolved scans carry NO partition columns SHALL produce a serialized scan spec
  byte-identical to its pre-feature encoding, on both the common spec and the join spec

### Scenario: A table the reader cannot plan fails the query loud at plan time

* *GIVEN* a pushdown request under the Unity Catalog kind naming a table whose Delta schema declares a
  type this engine does not map, and a second request naming a table the catalog reports in a non-Delta
  format
* *WHEN* the adapter plans each request
* *THEN* the adapter SHALL return the reader's own clean plan-time error — naming the column and its
  Delta type, or naming the table and its reported format — and MUST NOT return a scan-driving SQL
  response
* *AND* the adapter MUST NOT fall back to the Iceberg resolution path, emit a partial file list, or
  return rows, because a table that cannot be planned has no correct partial answer
* *AND* the error MUST be returned as an error value, never raised as a panic
* *AND* the error message MUST NOT contain any credential value

### Scenario: Capability advertisement stays blind to the catalog kind

* *GIVEN* two virtual schemas over the same adapter, one created under each catalog kind
* *WHEN* Exasol requests the adapter's capabilities for each
* *THEN* the adapter SHALL return the SAME capability set for both, and MUST NOT read, branch on, or
  receive the catalog kind while assembling it
* *AND* a capability the adapter advertises SHALL therefore be one it satisfies under BOTH kinds,
  because Exasol re-applies nothing it delegated and a kind-conditional capability would return wrong
  rows rather than a deferred check
