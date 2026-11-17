# Decisions: migrate-legacy-decision-log

## ADR: Single Authoritative DataFusion-to-Exasol Type Mapping with JSON Fallback

**ID:** single-authoritative-datafusion-to-exasol-type-mapping-with-json-fallback
**Plan:** `add-datafusion-iceberg-scan-pushdown`
**Status:** Accepted

### Context

Exasol has no array, list, struct, or map type. The Arrow-to-Exasol mapping applies at schema declaration and at row conversion, and a divergence between the two causes runtime errors.

### Decision

One mapping table governs both sites. Compatible Arrow types map to native Exasol types. Incompatible types (List, Struct, Map, Union, Binary, Duration, Time, Interval, Decimal256, and Decimal128 above precision or scale 36) become a JSON string declared as `VARCHAR(2000000)`.

### Options Considered

| Option | Verdict |
|--------|---------|
| Reject incompatible columns at schema creation | Rejected: makes tables with list or struct columns unusable |
| Drop incompatible columns | Rejected: silently hides data |

### Consequences

- Every Iceberg column is surfaced.
- A new type must be added at both sites.
- Out-of-range Decimal128 values pay a JSON serialization round-trip.

---

## ADR: Capability Invariant: Advertise Only What the Engine Can Back Correctly

**ID:** capability-invariant-advertise-only-what-the-engine-can-back-correctly
**Plan:** `add-capability-alignment`
**Status:** Accepted

### Context

Over-advertising a wrongly translated function causes silent wrong results, while under-advertising only costs performance.

### Decision

The adapter advertises a capability only when it has a correct path: a translator arm that emits a correct DataFusion fragment, or an aggregate plan with a correct shard-associative partial and merge. Names without a path are removed.

### Options Considered

| Option | Verdict |
|--------|---------|
| Maximise the advertised set and rely on Exasol | Rejected: wrongly translated capabilities return silently wrong results |

### Consequences

- A new capability needs its translator arm or decomposition first.

---

## ADR: New `adapter/iceberg_predicate.rs` Module; `iceberg-rust` Types Stay out of `vs-expression`

**ID:** new-adapter-iceberg-predicate-rs-module-iceberg-rust-types-stay-out-of-vs-expression
**Plan:** `add-iceberg-predicate-pruning`
**Status:** Accepted

### Context

Iceberg pruning needs Iceberg predicate values built from the Exasol filter JSON. The `vs-expression` crate has no `iceberg-rust` dependency.

### Decision

A dedicated engine module translates the Exasol filter JSON into an optional Iceberg predicate. `vs-expression` is not extended.

### Options Considered

| Option | Verdict |
|--------|---------|
| Extend `vs-expression` to emit Iceberg predicates | Rejected: adds `iceberg-rust` as a dependency of the translator crate |

### Consequences

- `vs-expression` stays free of `iceberg-rust`.

---

## ADR: Decode-Emit Overlap Buffer Is Conditional / Measure-First; Not Committed

**ID:** decode-emit-overlap-buffer-is-conditional-measure-first-not-committed
**Plan:** `change-engine-throughput`
**Status:** Accepted

### Context

A bounded queue between the DataFusion stream and the emit calls could overlap object-storage read latency with send-back time. It adds concurrency complexity and holds a batch in memory per slot, against the fetch-one, emit, drop discipline that bounds memory.

### Decision

The engine builds no overlap buffer. It becomes a requirement only if phase telemetry shows that emit and object-storage import are both material and serialized.

### Options Considered

| Option | Verdict |
|--------|---------|
| Build the buffer now | Rejected: telemetry must first show that decoupling pays |

### Consequences

- Memory stays bounded by the batch size.
- The scan is import-bound on the far-VPC path.

---

## ADR: Catalog Auth Credentials Live on `ConnectionCreds`, Never on the UDF-Boundary Payload

**ID:** catalog-auth-credentials-live-on-connectioncreds-never-on-the-udf-boundary-payload
**Plan:** `add-rest-catalog-oauth-auth`
**Status:** Accepted

### Context

REST-catalog authentication (a bearer token or OAuth2 client credentials) must reach the catalog build. The catalog and storage properties are serialized into the scan spec, which crosses the UDF boundary, and the scan UDF never calls the catalog.

### Decision

The auth fields live on the planning-layer connection credentials and are injected when the REST catalog is built. No auth field enters the catalog properties, storage properties, or scan spec.

### Options Considered

| Option | Verdict |
|--------|---------|
| Add the auth fields to the catalog properties or scan spec | Rejected: carries catalog secrets across a boundary the scan node never needs |

### Consequences

- A unit test guards that no auth field name or value appears in a serialized scan spec.

---

## ADR: Raw-Scan Projection Gets an Explicit `ProjectionItem` Tag Instead of a Syntactic Heuristic

**ID:** raw-scan-projection-gets-an-explicit-projectionitem-tag-instead-of-a-syntactic-heuristic
**Plan:** `add-arithmetic-aggregate-pushdown-and-benchmark-suite`
**Status:** Accepted

### Context

Bare column names and rendered expression fragments shared one list of strings, and the scan SQL builder quoted every entry as an identifier. An expression such as `("SCORE" * 2)` became a phantom column name and failed at query time.

### Decision

Each projection entry is tagged as a column or an expression where the adapter knows the kind. A column keeps its quoting and JSON-fallback cast, and an expression is spliced verbatim, like the filter.

### Options Considered

| Option | Verdict |
|--------|---------|
| Syntactic heuristic in the SQL builder | Rejected: fragile, and an exotic quoted column name would be misclassified |
| Check whether the item is a field in the Arrow schema | Rejected: implicit, and changes the defensive path for a bare column absent from the schema |
| Parallel list of flags | Rejected: positional alignment is fragile |

### Consequences

- A column still serializes as a bare JSON string, so the wire format is unchanged.
- Anything crossing the adapter-to-scan boundary tags identifiers and rendered SQL separately and never overloads one string field.

---

## ADR: Decline the Top-N Shape When a Sort Key Column Needs the JSON-Fallback VARCHAR Cast

**ID:** decline-the-top-n-shape-when-a-sort-key-column-needs-the-json-fallback-varchar-cast
**Plan:** `add-topn-pushdown`
**Status:** Accepted

### Context

When a sort key needs the JSON-fallback VARCHAR cast, the per-shard `ORDER BY` uses the native value but the emitted value is a JSON string. Exasol's merge re-ranks that string lexicographically, so the per-shard and merge rankings disagree and the global top-N is silently wrong.

### Decision

The adapter declines the whole top-N shape and falls back to the raw scan when a sort key needs the JSON fallback cast or is absent from the logical schema.

### Options Considered

| Option | Verdict |
|--------|---------|
| Emit the sort key uncast and cast a duplicate projection column | Rejected: needs extra trailing columns, disproportionate for a shape no known query hits |
| Sort the merge on the pre-cast value | Rejected: Exasol only receives the emitted representation |

### Consequences

- A unit test covers the decline.
- The guard gains weight as the logical schema preserves richer types.

---

## ADR: Per-Side Predicate Pushdown for Joins by `tableName` Conjunct Attribution

**ID:** per-side-predicate-pushdown-for-joins-by-tablename-conjunct-attribution
**Plan:** `add-join-pushdown-broadcast`
**Status:** Accepted

### Context

Filtering each join side before it reaches Exasol needs the WHERE split by side. Passing the whole WHERE to each side is unsound: Iceberg predicate translation resolves columns by name only, so on a shared column name it applies the other side's conjunct and wrongly prunes files.

### Decision

For an inner equi-join, the adapter attributes each WHERE conjunct to a side by its columns' `tableName` and pushes it to that side only when all its columns belong to that table, for manifest pruning and as the fallback leg's filter. Cross-table conjuncts, the join condition, and any OR spanning both tables stay in the outer WHERE, which remains the correctness backstop. Each fallback leg projects only the columns the outer wrapper references for that side.

### Options Considered

| Option | Verdict |
|--------|---------|
| Pass the whole WHERE to each side and rely on predicate translation dropping unknown columns | Rejected: unsound on shared column names, since name-only resolution prunes wrongly |

### Consequences

- Both routes get manifest pruning per side, and fallback legs filter before emitting.
- Pruned byte totals make side selection and the broadcast threshold more accurate.
- Tests cover shared column names, and the leg filter renders bare column names while the broadcast path keeps the native alias.

---

## ADR: Author Delete-Bearing Benchmark Tables via Apache Spark `DELETE FROM` on a v2 Merge-on-Read Table

**ID:** author-delete-bearing-benchmark-tables-via-apache-spark-delete-from-on-a-v2-merge-on-read-table
**Plan:** `add-delete-benchmark-flag`
**Status:** Accepted

### Context

The delete benchmark variant needs Iceberg v2 merge-on-read TPC-H tables with about 5% of rows position-deleted. No in-repo Iceberg writer can author position-delete files.

### Decision

Apache Spark authors the tables: a `DELETE FROM` on a v2 merge-on-read copy of each TPC-H table with a deterministic surrogate-key predicate. Docker mode reuses the existing Spark compose fixture, and remote mode reuses the existing EMR Serverless application.

### Options Considered

| Option | Verdict |
|--------|---------|
| PyIceberg `table.delete()` | Rejected: copy-on-write only, so it authors no position-delete files |
| iceberg-rust writer | Rejected: it has no position-delete writer |

### Consequences

- The benchmark depends on Spark only at authoring time, never at query time.

---

## ADR: Full Push-Down of Grouped Scalar-Over-Aggregate Select Items (Primary), Qualified Wrapper as Residual Fallback

**ID:** full-push-down-of-grouped-scalar-over-aggregate-select-items-primary-qualified-wrapper-as-residual-fallback
**Plan:** `fix-scalar-over-aggregate-grouped-pushdown`
**Status:** Accepted

### Context

A grouped query with a scalar function wrapping aggregates, such as `ROUND(100.0 * SUM(CASE ...) / COUNT(*), 2)`, matched no aggregate shape. It fell to a bare raw row scan that returns the wrong column count for a `group_by` request and failed with a column-count mismatch.

### Decision

The adapter folds the item's inner aggregates into the existing partial aggregate decomposition and renders the scalar wrapper over the merged partials in the outer query, at the item's original position. It falls back to a qualified single-table wrapper only when an inner aggregate cannot be decomposed (`DISTINCT`, a non-numeric statistic argument, an untranslatable argument, or a non-aggregate non-group-key node). It never falls back to a bare row scan for a grouped request.

### Options Considered

| Option | Verdict |
|--------|---------|
| Route every grouped scalar-over-aggregate through the qualified wrapper | Rejected: ships every matching row per group to Exasol, defeating node-local aggregation. Kept only as the residual fallback |

### Consequences

- A scalar-over-aggregate item keeps node-local aggregation.
- An undecomposable grouped shape gets a wrapper with the correct column count.

---

## ADR: Split Fan-Out Is the Sole Scan Path, Applied Unconditionally

**ID:** split-fan-out-is-the-sole-scan-path-applied-unconditionally
**Plan:** `change-scan-fanout-to-scalar-emit`
**Status:** Accepted

### Context

Exasol materializes a `GROUP BY shard_key` raw-row SET-scan emit into temp-DB RAM, so memory grows with scanned data volume for every wrapper that drives it. A nested distributor script driving an outer ungrouped scalar scan keeps that memory constant.

### Decision

The distributor plus scalar scan is the only scan path, with no flag, property, or planner mode, and it applies to every wrapper: raw scan, single-group and grouped aggregate, top-N, broadcast join, and join fallback. Merge, group, and join logic runs in the outer ungrouped query over the scalar scan's partial rows. Single-shard plans skip the distributor and call the scalar scan directly.

### Options Considered

| Option | Verdict |
|--------|---------|
| Keep a local A/B flag | Rejected: diagnostic scaffolding, dead weight once the fix is proven |
| Per-query mode switch | Rejected: adds a branch and maintenance burden for no benefit |

### Consequences

- Every scan-driving wrapper shares one fan-out shape, and scan-output memory stays constant.
- `LAKEHOUSE_SCAN` itself is the SCALAR script, so there is one scan entry point and no separate SET variant.

---

## ADR: The File Distributor Is a LUA SET Script, Not a Rust `.so` Entry Point

**ID:** the-file-distributor-is-a-lua-set-script-not-a-rust-so-entry-point
**Plan:** `change-scan-fanout-to-scalar-emit`
**Status:** Accepted

### Context

Fan-out needs a `GROUP BY shard_key` relation so Exasol distributes shard groups round-robin across nodes. That relation only re-emits each shard's file-list string for the outer scalar scan, so it needs no DataFusion, Parquet access, or Rust scan logic.

### Decision

`LAKEHOUSE_DISTRIBUTE_FILES` is a pure LUA SET passthrough script with its own DDL. It is not a Rust `.so` entry point.

### Options Considered

| Option | Verdict |
|--------|---------|
| Third Rust entry point in the `.so` | Rejected: adds Rust, packaging, and fingerprint surface for a pure re-emit |

### Consequences

- The `.so` exports only the adapter, scan, and scalar distinct-merge entry points.
- The distributor has a small footprint independent of data volume.

---

## ADR: N-Scan Join Fallback Renders `INNER JOIN … ON` With Greedy-Attach and Per-Leg Filter Pushdown

**ID:** n-scan-join-fallback-renders-inner-join-on-with-greedy-attach-and-per-leg-filter-pushdown
**Plan:** `change-scan-fanout-to-scalar-emit`
**Status:** Accepted

### Context

The unaccelerated join fallback needs a FROM shape. A comma cross join with one flat WHERE leaves side-local filters unpushed and is less favorable to Exasol's optimizer.

### Decision

The fallback FROM is a left-to-right `INNER JOIN ... ON` chain. Each join condition attaches at the earliest join point where every table it references is in scope, decided by the `tableName`s it touches and never by column name, so shared column names stay qualified. A join point with no resolvable condition renders `ON 1=1`.

### Options Considered

| Option | Verdict |
|--------|---------|
| Keep the comma cross join and flat WHERE | Rejected: correct but leaves side-local filters unpushed and gives Exasol's optimizer a less favorable shape |

### Consequences

- DataFusion prunes and filters each leg instead of Exasol filtering after full materialization.
