# Decisions: migrate-legacy-decision-log

## ADR: Single Authoritative DataFusion-to-Exasol Type Mapping with JSON Fallback

**ID:** single-authoritative-datafusion-to-exasol-type-mapping-with-json-fallback
**Plan:** `add-datafusion-iceberg-scan-pushdown`
**Status:** Accepted

### Context

Exasol has no array, list, struct, or map type. The Arrow-to-Exasol mapping applies in both the `createVirtualSchema` declaration and the scan's Arrow-to-Value conversion, and a divergence causes runtime errors.

### Decision

One mapping table governs both sites. Compatible Arrow types map to native Exasol types. Incompatible types (List, Struct, Map, Union, Binary, Duration, Time, Interval, Decimal256, and Decimal128 with precision or scale above 36) are serialized to a JSON string and declared as `VARCHAR(2000000)`.

### Options Considered

| Option | Verdict |
|--------|---------|
| Reject incompatible columns at `createVirtualSchema` | Rejected: makes tables with list or struct columns unusable |
| Drop incompatible columns | Rejected: silently hides data |

### Consequences

Every Iceberg column is surfaced. A future type addition must update both code sites, and out-of-range Decimal128 values pay a JSON serialization round-trip.

---

## ADR: Capability Invariant — Advertise Only What the Engine Can Back Correctly

**ID:** capability-invariant-advertise-only-what-the-engine-can-back-correctly
**Plan:** `add-capability-alignment`
**Status:** Accepted

### Context

The adapter's `CAPABILITIES` list and the `vs-expression` translator had drifted. Some advertised names did not exist in Exasol's vocabulary, and many supported functions were not advertised. Over-advertising a wrongly translated function causes silent wrong results, while under-advertising only costs performance.

### Decision

Every name in `CAPABILITIES` must have a correct path: a translator arm that emits a correct DataFusion fragment, or an aggregate plan with a correct shard-associative partial and merge. Additions need a working path, and names without one are removed. `FN_PRED_GREATER` and `FN_PRED_GREATEREQUAL` are deleted.

### Options Considered

| Option | Verdict |
|--------|---------|
| Maximise the advertised set and rely on Exasol | Rejected: wrongly translated capabilities return silently wrong results |

### Consequences

A new `FN_*` entry needs its translator arm or decomposition first.

---

## ADR: New `adapter/iceberg_predicate.rs` Module; `iceberg-rust` Types Stay out of `vs-expression`

**ID:** new-adapter-iceberg-predicate-rs-module-iceberg-rust-types-stay-out-of-vs-expression
**Plan:** `add-iceberg-predicate-pruning`
**Status:** Accepted

### Context

Iceberg pruning builds `iceberg::expr::Predicate` values from the Exasol filter JSON. `vs-expression` has no `iceberg-rust` dependency.

### Decision

A dedicated engine module translates the Exasol filter JSON into an optional Iceberg predicate. `vs-expression` is not extended.

### Options Considered

| Option | Verdict |
|--------|---------|
| Extend `vs-expression` to emit Iceberg predicates | Rejected: adds `iceberg-rust` as a dependency of the translator crate |

### Consequences

Iceberg predicate translation lives in the engine, and `vs-expression` stays free of `iceberg-rust`.

---

## ADR: Decode-Emit Overlap Buffer Is Conditional / Measure-First; Not Committed

**ID:** decode-emit-overlap-buffer-is-conditional-measure-first-not-committed
**Plan:** `change-engine-throughput`
**Status:** Accepted

### Context

A bounded producer/consumer queue between the DataFusion stream and the emit calls could overlap S3 read latency with send-back time. It adds concurrency complexity and holds a batch in memory per slot, against the fetch-one, emit, drop discipline that bounds memory.

### Decision

The engine does not build the `DF_MAX_BUFFERED_BATCHES` buffer. It becomes a requirement only if phase telemetry shows that emit and object-storage import are both material and serialized, and that decoupling them gains throughput. The gate failed: emit took about 2 ms against about 650 ms for import on the far-VPC workload (ADR-037).

### Options Considered

| Option | Verdict |
|--------|---------|
| Build the buffer now | Rejected: measure first, since telemetry must show decoupling pays |

### Consequences

Memory stays bounded by `batch_size`, and the scan is import-bound on the far-VPC path.

---

## ADR: Catalog Auth Credentials Live on `ConnectionCreds`, Never on the UDF-Boundary Payload

**ID:** catalog-auth-credentials-live-on-connectioncreds-never-on-the-udf-boundary-payload
**Plan:** `add-rest-catalog-oauth-auth`
**Status:** Accepted

### Context

REST-catalog authentication (a static bearer `token` or an OAuth2 `client_id` and `client_secret`) must reach the catalog build. `CatalogProps` and `StorageProps` are serialized into `ScanSpec`, which crosses the UDF boundary, and the scan UDF never calls the catalog.

### Decision

The auth fields (`token`, `client_id`, `client_secret`, `oauth2_server_uri`, `scope`) live on `ConnectionCreds` within the planning layer and are injected when the REST catalog is built. No auth field enters `CatalogProps`, `StorageProps`, or `ScanSpec`.

### Options Considered

| Option | Verdict |
|--------|---------|
| Widen `CatalogProps` or `ScanSpec` with the auth fields | Rejected: carries catalog secrets across a boundary the scan node never needs |

### Consequences

A unit test guards that no auth field name or value appears in a serialized scan spec.

---

## ADR: Constant-Projection-Over-`GROUP BY` Placeholder Drives the Existing Grouped Scan Instead of the Row-Scan Path

**ID:** constant-projection-over-group-by-placeholder-drives-the-existing-grouped-scan-instead-of-the-row-scan-path
**Plan:** `fix-nested-aggregate-pushdown`
**Status:** Accepted

### Context

For `SELECT COUNT(*) FROM (SELECT id, COUNT(*) FROM t GROUP BY id)`, Exasol sends one flat `group_by` pushdown whose `selectList` is a single `literal_null` placeholder, as the optimizer's "count the groups" rewrite. `detect_group_by_aggregates` rejected it because `NULL` matched no group key. The row-scan path then pushed `NULL` as a phantom column that DataFusion rejects. A row-scan fallback returns one row per source row, not per group, which is wrong for duplicate group-key values such as `LINEITEM.L_ORDERKEY` (#52).

### Decision

`detect_group_by_aggregates` treats a pure-literal select-list item as a constant "count the groups" projection and drives the existing grouped scan with an empty aggregate-plan list. This keeps one row per distinct group for any key cardinality. `extract_projection` also no longer pushes a rendered literal as a column name.

### Options Considered

| Option | Verdict |
|--------|---------|
| Tighten the guard so the row-scan fallback engages | Rejected: returns the raw row count, not the group count, for duplicate keys |
| Return an error to force native retry | Rejected: a VS has no native data path, so the query just fails |

### Consequences

The regression test covers a duplicate-key group column, because unique-key data cannot tell the grouped fix from the row-scan fallback.

---

## ADR: Raw-Scan Projection Gets an Explicit `ProjectionItem` Tag Instead of a Syntactic Heuristic

**ID:** raw-scan-projection-gets-an-explicit-projectionitem-tag-instead-of-a-syntactic-heuristic
**Plan:** `add-arithmetic-aggregate-pushdown-and-benchmark-suite`
**Status:** Accepted

### Context

`extract_projection` put bare column names and rendered expression fragments in one `Vec<String>`. `build_scan_sql` quoted every entry as an identifier, turning `("SCORE" * 2)` into a phantom column name and failing live with `No field named "(""SCORE"" * 2)"`.

### Decision

`ScanSpec.projection` and `CommonScanSpec.projection` hold `ProjectionItem` values, an untagged enum of `Column(String)` and `Expr { expr: String }`, tagged where `extract_projection` knows the kind. `Column` keeps its CAST-for-JSON-fallback and quoting. `Expr` is spliced verbatim, like `spec.filter`.

### Options Considered

| Option | Verdict |
|--------|---------|
| Syntactic heuristic in `build_scan_sql` | Rejected: fragile, and an exotic quoted column name would be misclassified |
| Check whether the item is a field in the Arrow schema | Rejected: implicit, and changes the defensive path for a bare column absent from the schema |
| Parallel `Vec<bool>` or `Vec<Option<String>>` | Rejected: positional alignment is fragile |

### Consequences

A `Column` still serializes as a bare JSON string, so the common wire format is unchanged. Never overload one string field for both a bare identifier and rendered SQL: the `column`, `arg_expr`, and `ProjectionItem` pattern is the convention for anything crossing the adapter-to-scan boundary.

---

## ADR: Decline the Top-N Shape When a Sort Key Column Needs the JSON-Fallback VARCHAR Cast

**ID:** decline-the-top-n-shape-when-a-sort-key-column-needs-the-json-fallback-varchar-cast
**Plan:** `add-topn-pushdown`
**Status:** Accepted

### Context

For a sort key whose Arrow type needs the JSON-fallback VARCHAR cast, the per-shard `ORDER BY` binds to the native value, but the emitted value is a JSON string. Exasol's merge re-ranks that string lexicographically, so the per-shard and merge rankings disagree and the global top-N is silently wrong. NQ4 is not affected, since `L_EXTENDEDPRICE` is a plain in-range DECIMAL.

### Decision

`detect_topn` resolves each sort key's Arrow type from the logical schema and declines the whole top-N shape, falling back to the raw scan, when the type needs the JSON fallback cast or the column is absent from the schema.

### Options Considered

| Option | Verdict |
|--------|---------|
| Emit the sort key uncast and cast a duplicate projection column | Rejected: needs extra trailing EMITS columns, disproportionate for a shape no known query hits |
| Sort the merge on the pre-cast value | Rejected: Exasol only receives the emitted representation |

### Consequences

A unit test covers this. The guard becomes fully load-bearing once the logical-schema tag vocabulary preserves richer types.

---

## ADR: Drop `tpchgen-arrow`; build arrow-58 batches from `tpchgen` core directly

**ID:** drop-tpchgen-arrow-build-arrow-58-batches-from-tpchgen-core-directly
**Plan:** `change-iceberg-rust-0-10-bump`
**Status:** Accepted

### Context

`tpchgen-arrow` has releases for arrow 57 and arrow 59 but none for arrow 58, which the iceberg 0.10 writer expects. Keeping it leaves a second arrow tree in the dev and e2e graph and needs an Arrow IPC bridge. `tpchgen` core has no arrow dependency.

### Decision

The `tpchgen-arrow` dependency is removed. The seed and TPC-H loader code builds arrow-58 `RecordBatch`es from `tpchgen` core with the workspace arrow builders.

### Options Considered

| Option | Verdict |
|--------|---------|
| Keep `tpchgen-arrow` for arrow 57 with an IPC bridge | Rejected: leaves arrow 57 in the dev lock and adds an IPC round-trip |
| Use `tpchgen-arrow` for arrow 59 with an IPC bridge | Rejected: adds an arrow tree newer than the workspace's, API churn, and the same bridge |
| Drop the TPC-H loader | Rejected: it backs the live smoke test |

### Consequences

The dev and e2e graph has a single arrow-58 tree, and a future arrow bump needs no `tpchgen-arrow` release. The cost is about 100 to 200 lines of test-only column builders.

---

## ADR: Per-Side Predicate Pushdown for Joins by `tableName` Conjunct Attribution

**ID:** per-side-predicate-pushdown-for-joins-by-tablename-conjunct-attribution
**Plan:** `add-join-pushdown-broadcast`
**Status:** Accepted

### Context

Each side of a join over-scanned. Both join routes resolved file lists with no filter, so neither side got Iceberg manifest pruning. The two-scan fallback built each leg's `ScanSpec` with `filter: None`, so each leg shipped every row to Exasol. It also projected each table's full involved-column set. Passing the whole WHERE to each side is unsound: `to_iceberg_predicate` resolves columns by name only, so on a shared column name it applies the other side's conjunct to this side and wrongly prunes files.

### Decision

For an inner equi-join, the adapter attributes each WHERE conjunct to a side by its columns' `tableName`, the signal ADR-085 already uses. A conjunct is side-local when every column it references belongs to one table. Only side-local conjuncts are pushed per side. Cross-table conjuncts, the join condition, and any OR spanning both tables stay in the outer wrapper's WHERE, which remains the correctness backstop. The side-local predicate feeds manifest pruning on both routes and the two-scan leg's `ScanSpec.filter`. Each fallback leg's projection narrows to the columns the outer wrapper references for that side, including the full WHERE's columns. The leg filter strips `tableAlias` and renders bare, because a per-side fan-out is a single-table scan whose relation exposes bare column names. The broadcast path keeps the native alias.

### Options Considered

| Option | Verdict |
|--------|---------|
| Pass the whole WHERE to each side and rely on `to_iceberg_predicate` dropping unknown columns | Rejected: unsound on shared column names, since name-only resolution prunes wrongly |

### Consequences

Both routes get manifest pruning per side, and the fallback legs filter and prune before emitting. Pruned byte totals also make side selection and the broadcast threshold more accurate. Unit tests cover the shared-column-name case so ADR-085 cannot regress, and a regression test pins the bare rendering in the fan-out and the alias-preserving rendering in broadcast.

---

## ADR: Reuse the Iceberg Crate's NameMapping Deserializer

**ID:** reuse-the-iceberg-crate-s-namemapping-deserializer
**Plan:** `change-name-mapping-fallback`
**Status:** Accepted

### Context

The `schema.name-mapping.default` table property is JSON with kebab-case, an optional `field-id`, and nested `fields`. The `iceberg` crate already exports `NameMapping` and `MappedField` for exactly this shape.

### Decision

The engine parses the property with `serde_json::from_str::<iceberg::spec::NameMapping>`.

### Options Considered

| Option | Verdict |
|--------|---------|
| Hand-rolled serde struct | Rejected: reinvents a deserializer the dependency provides |

### Consequences

Parsing correctness tracks the `iceberg` crate's spec fidelity.

---

## ADR: Author Delete-Bearing Benchmark Tables via Apache Spark `DELETE FROM` on a v2 Merge-on-Read Table

**ID:** author-delete-bearing-benchmark-tables-via-apache-spark-delete-from-on-a-v2-merge-on-read-table
**Plan:** `add-delete-benchmark-flag`
**Status:** Accepted

### Context

The `BENCH_WITH_DELETES` benchmark variant needs Iceberg v2 merge-on-read TPC-H tables with about 5% of rows position-deleted per table, to measure the merge-on-read read path. No in-repo Iceberg writer could author position-delete files.

### Decision

The tables are authored with Apache Spark's Iceberg runtime: a `DELETE FROM` on a `write.delete.mode=merge-on-read`, format-version 2 copy of each TPC-H table, with a deterministic `<surrogate_key> % 20 = 0` predicate. Docker mode reuses the existing `apache/spark` compose fixture image, and remote mode reuses the EMR Serverless application used by `spark_compare.sh`.

### Options Considered

| Option | Verdict |
|--------|---------|
| PyIceberg `table.delete()` | Rejected: copy-on-write only, so it authors no position-delete files |
| iceberg-rust writer | Rejected: iceberg-rust 0.10 has no position-delete writer (`apache/iceberg-rust#340`) |

### Consequences

The benchmark depends on Spark only at authoring time, never at query time. The two authoring entry points are `scripts/spark-fixtures/create_tpch_deletes.sql` (docker) and `deploy/scripts/make_deletes_remote.py` (remote).

---

## ADR: Full Push-Down of Grouped Scalar-Over-Aggregate Select Items (Primary), Qualified Wrapper as Residual Fallback

**ID:** full-push-down-of-grouped-scalar-over-aggregate-select-items-primary-qualified-wrapper-as-residual-fallback
**Plan:** `fix-scalar-over-aggregate-grouped-pushdown`
**Status:** Accepted

### Context

A single-table grouped query with a scalar function wrapping aggregates, such as `ROUND(100.0 * SUM(CASE ...) / COUNT(*), 2)`, hard-failed with a `04000` column-count mismatch (#82). `detect_group_by_aggregates` classified the item as neither aggregate, literal, nor group key, returned `None`, and fell to a bare raw row scan that returns the wrong column count for a `group_by` request.

### Decision

The adapter folds the item's inner aggregates into the existing partial `AggregatePlan` decomposition and renders the scalar wrapper over the merged partials in the outer wrapper, at the item's original select-list position. It falls back to a qualified single-table wrapper only when an inner aggregate cannot be decomposed: `DISTINCT`, a non-numeric statistic argument, an untranslatable argument, or a non-aggregate non-group-key node. It never falls back to a bare row scan for a grouped request. `render_having_operand` recurses into a merge-aware renderer, `render_scalar_over_merge`, which rewrites each nested `function_aggregate` to its merged expression, matched to the `AggregatePlan` list by kind and argument.

### Options Considered

| Option | Verdict |
|--------|---------|
| Route every grouped scalar-over-aggregate through the qualified single-table wrapper | Rejected: ships every matching row per group to Exasol, defeating node-local aggregation. Kept only as the residual fallback |

### Consequences

A scalar-over-aggregate item pushes down like a top-level aggregate and keeps node-local aggregation. An undecomposable grouped shape gets a qualified wrapper with the correct column count.

---

## ADR: Split Fan-Out Is the Sole Scan Path, Applied Unconditionally

**ID:** split-fan-out-is-the-sole-scan-path-applied-unconditionally
**Plan:** `change-scan-fanout-to-scalar-emit`
**Status:** Accepted

### Context

Exasol materializes the `GROUP BY shard_key` raw-row emit fan-out into temp-DB RAM on the transparent VS path, so memory grows with scanned data volume. A spike showed that a nested `LAKEHOUSE_DISTRIBUTE_FILES` LUA SET distributor driving an outer ungrouped scalar `LAKEHOUSE_SCAN` keeps memory constant (about 2.5 GB, against 22 GB or more). The materialization affects every wrapper that drove the old SET scan.

### Decision

The distributor plus scalar scan is the only scan path, with no flag, VS property, or planner mode. It applies to every wrapper: raw scan, single-group aggregate, grouped aggregate, top-N, broadcast join, and the N-scan join fallback. Merge, group, and join logic moves to the outer ungrouped query over the scalar scan's partial rows. Single-shard plans skip the distributor and call the scalar scan directly.

### Options Considered

| Option | Verdict |
|--------|---------|
| Keep the spike's local A/B flag | Rejected: diagnostic scaffolding, dead weight once the fix is proven |
| Per-query mode switch | Rejected: adds a branch and maintenance burden for no benefit |

### Consequences

Every scan-driving wrapper shares one fan-out shape and scan-output memory stays constant. All wrappers convert in one plan, not incrementally.

---

## ADR: `LAKEHOUSE_SCAN` Itself Becomes SCALAR — One Scan Entry Point, Not Two

**ID:** lakehouse-scan-itself-becomes-scalar-one-scan-entry-point-not-two
**Plan:** `change-scan-fanout-to-scalar-emit`
**Status:** Accepted

### Context

Streaming-only scan output requires the scan UDF to run as a SCALAR EMIT script, not a SET script. The scan logic is identical for both, and only the DDL script type and the run loop differ.

### Decision

The existing `LAKEHOUSE_SCAN` symbol's DDL script type changes from SET to SCALAR in place.

### Options Considered

| Option | Verdict |
|--------|---------|
| Add a separate `LAKEHOUSE_SCAN_SCALAR` entry point | Rejected: doubles packaging and fingerprint surface for identical logic, and the two would drift |

### Consequences

The SET script type is retired from the scan path, and the symbol name is unchanged.

---

## ADR: The File Distributor Is a LUA SET Script, Not a Rust `.so` Entry Point

**ID:** the-file-distributor-is-a-lua-set-script-not-a-rust-so-entry-point
**Plan:** `change-scan-fanout-to-scalar-emit`
**Status:** Accepted

### Context

Fan-out still needs a `GROUP BY shard_key` relation so Exasol distributes shard groups round-robin across nodes. That relation only re-emits each shard's file-list string for the outer scalar scan, so it needs no DataFusion, Parquet access, or Rust scan logic.

### Decision

`LAKEHOUSE_DISTRIBUTE_FILES` is a pure LUA SET passthrough script with its own DDL, schema-qualified like the scan and distinct-merge scripts. It is not a Rust `.so` entry point.

### Options Considered

| Option | Verdict |
|--------|---------|
| Third Rust entry point in the `.so` | Rejected: adds Rust, packaging, and fingerprint surface for a pure re-emit |

### Consequences

The `.so` still exports exactly the adapter, scan, and scalar distinct-merge entry points. The distributor references no `.so` and declares no `%udf_object`, so its footprint is small and independent of data volume.

---

## ADR: N-Scan Join Fallback Renders `INNER JOIN … ON` With Greedy-Attach and Per-Leg Filter Pushdown

**ID:** n-scan-join-fallback-renders-inner-join-on-with-greedy-attach-and-per-leg-filter-pushdown
**Plan:** `change-scan-fanout-to-scalar-emit`
**Status:** Accepted

### Context

The unaccelerated join fallback rendered its FROM as a comma cross join with one flat WHERE. Converting every leg to the distributor and scalar-scan fan-out was a chance to render the join shape Exasol plans best and to push each side's own filters into its leg.

### Decision

The fallback FROM is a left-to-right `INNER JOIN ... ON` chain. Each join condition attaches at the earliest join point where every table it references is in scope, decided by the set of `tableName`s it touches and never by column name, so shared column names stay qualified. A join point with no resolvable condition renders `ON 1=1`. Each side's side-local WHERE conjuncts go into that side's fan-out leg as a DataFusion filter, and only residual conjuncts (cross-table, OR-spanning, or untagged) stay in the outer WHERE.

### Options Considered

| Option | Verdict |
|--------|---------|
| Keep the comma cross join and flat WHERE | Rejected: correct but leaves side-local filters unpushed and gives Exasol's optimizer a less favorable shape |

### Consequences

DataFusion prunes and filters each leg instead of Exasol filtering after full materialization.
