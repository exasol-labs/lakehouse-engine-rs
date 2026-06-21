# Decision Log: add-datafusion-iceberg-scan-pushdown

Date: 2026-06-21

## Interview

**Q:** How much of the phased roadmap should this plan cover?
**A:** Slice + pushdown — phase 1 (DataFusion-in-UDF over an Iceberg table returning
rows through the VS) PLUS projection/filter/LIMIT pushdown into the scan, in one plan.
Multi-node file sharding, Databricks access, and aggregation pushdown are explicitly
DEFERRED to later `/speq:plan` calls — noted as roadmap/non-goals here, not designed now.

**Q:** What Iceberg catalog + storage target should this slice connect to first?
**A:** Iceberg REST catalog + MinIO (S3-compatible) in Docker, run alongside the Exasol
container. This is the E2E target. Real AWS S3/Glue and Databricks Unity are out of
scope for this plan.

**Q (out-of-band, dependency update):** Which SDK version and packaging shape?
**A:** Base the plan on language-container-rs 0.14.0 (publish in flight). Use
`exasol-udf-sdk` 0.14.0 (connect-back) + `exasol-udf-macros` 0.14.0, not 0.13.1
(mission.md is stale on this). Exploit 0.14.0's new multiple-UDF-entry-points-per-`.so`
capability: a single `.so` exports both the VS adapter entry point and the DataFusion
scan SET-UDF entry point. crates.io may still show 0.13.1 while 0.14.0 lands; depend on
0.14.0 with a documented path-dependency fallback to the local sibling crates.

## Design Decisions

### [1] One crate / one `.so` with two named entry points

- **Decision:** Ship the VS adapter and the DataFusion scan SET UDF as two
  `#[exasol_udf]` entry points in a single `cdylib` crate that builds to one `.so`.
- **Alternatives:** Two crates → two `.so` files (the pre-0.14.0 `strata-rs` shape),
  each uploaded and registered separately.
- **Rationale:** 0.14.0 newly supports multiple entry points per `.so` (commit a11795a,
  live two-entry E2E in d67c977). One artifact means one BucketFS upload and a single
  build target; it also directly exercises the new capability the team wants validated.
- **Promotes to ADR:** yes

### [2] Adapter drives a scan SET UDF (not a cache-populating library call)

- **Decision:** The adapter's `pushdown` response is SQL that invokes the scan SET UDF
  with an explicit file list; the UDF runs DataFusion and emits rows.
- **Alternatives:** Mirror `strata-rs`, where the adapter calls a `populate_cache()`
  library function via connect-back and returns a plain `SELECT` from a cache table.
- **Rationale:** The PoC hypothesis is DataFusion-in-UDF as the distributed execution
  substrate; caching/materialization are explicit mission non-goals. Execution must run
  inside the UDF, not pre-populate a cache.
- **Promotes to ADR:** yes

### [3] Resolve metadata once in the adapter; pass an explicit file list to the UDF

- **Decision:** The adapter resolves the Iceberg snapshot + data-file list exactly once
  during `pushdown` and hands the file list to the scan UDF as an argument. The UDF
  never discovers files itself.
- **Alternatives:** Let each UDF invocation re-resolve metadata from the catalog.
- **Rationale:** Mission constraint — resolve once per query, not once per node. Even
  though this slice is single-invocation, the explicit hand-off is the exact seam
  multi-node file sharding will later exploit.
- **Promotes to ADR:** yes

### [4] Value-only boundary with batch-by-batch incremental emit

- **Decision:** Convert each Arrow `RecordBatch` to SDK `Value` rows inside the UDF and
  `ctx.emit` them before fetching the next batch, dropping each batch; rely on the
  4,000,000-byte auto-flush. No Arrow type crosses the `.so` boundary.
- **Alternatives:** Collect the full result set then emit; or attempt to pass Arrow
  across the boundary.
- **Rationale:** The `.so` links its own Arrow copy with different `TypeId`s, so Arrow
  cannot cross safely; and materializing the full set violates the streaming constraint
  and risks memory blowups. This is a project-wide invariant from CLAUDE.md/mission.
- **Promotes to ADR:** yes

### [5] Iceberg REST catalog + MinIO as the first (and only) E2E target

- **Decision:** Target an Iceberg REST catalog + MinIO in Docker; no Nessie/Polaris/
  Lakekeeper variety, no real S3/Glue, no Databricks for this plan.
- **Alternatives:** The sibling project's catalog overlays (Nessie, Polaris,
  Lakekeeper, Unity).
- **Rationale:** Interview decision; the thinnest reproducible target that proves the
  hypothesis. Catalog variety is a later concern.
- **Promotes to ADR:** no

### [6] Single UDF invocation over the whole file list (no sharding yet)

- **Decision:** Assign the entire resolved file list to one scan UDF invocation.
- **Alternatives:** Shard files across invocations/nodes now.
- **Rationale:** Multi-node parallelism is deferred; a single invocation is the minimal
  slice that proves the inner loop while the resolve-once seam preserves the sharding
  path for later.
- **Promotes to ADR:** no

### [7] Depend on 0.14.0 from crates.io with a path-dependency fallback

- **Decision:** Declare `exasol-udf-sdk`/`exasol-udf-macros` at `0.14.0`; document a
  fallback to path-depend on the local sibling crates until 0.14.0 is downloadable.
- **Alternatives:** Path-depend unconditionally.
- **Rationale:** 0.14.0 publish is in flight (crates.io still shows 0.13.1 as latest).
  A version dependency is the clean end state; the path fallback unblocks work now.
- **Promotes to ADR:** no

### [8] Mirror sibling Makefile / compose / BucketFS conventions

- **Decision:** Reuse `strata-rs` conventions: `cross-musl-udf-build` (docker
  `rust:1.92-bookworm`, persistent cargo registry volume, `-p` crate flag), gated
  `test-e2e`, BucketFS HTTPS upload on port 2581, `SCRIPT_LANGUAGES` registration, all
  DSNs with `validateservercertificate=0`.
- **Alternatives:** Invent a new build/deploy workflow.
- **Rationale:** The siblings already solved SLC install + E2E; convergence (possible
  future monorepo) is a stated goal. Mirroring reduces risk and effort.
- **Promotes to ADR:** no

### [9] DataFusion→Exasol type mapping with JSON fallback for incompatible types

- **Decision:** A single authoritative Arrow→Exasol mapping table governs both
  `createVirtualSchema` schema declaration and the scan's Arrow→`Value` conversion.
  Compatible Arrow types map directly (Boolean→BOOLEAN, integers→DECIMAL,
  Float→DOUBLE PRECISION, Utf8→VARCHAR, Date32→DATE, Timestamp→TIMESTAMP / TIMESTAMP
  WITH LOCAL TIME ZONE, in-range Decimal128→DECIMAL(p,s)). Types Exasol cannot
  represent — List, LargeList, FixedSizeList, Struct, Map, Union, Binary, LargeBinary,
  FixedSizeBinary, Duration, Time32, Time64, Interval, Decimal256, and out-of-range
  Decimal128 (p>36 or s>36) — are serialized to a JSON string in the scan UDF
  (`CAST(col AS VARCHAR)` / `arrow_cast`) and declared as `VARCHAR(2000000)`.
- **Alternatives:** Reject/error on incompatible columns at `createVirtualSchema`; or
  drop incompatible columns from the virtual table.
- **Rationale:** User framing — DataFusion lets Exasol read vectors, lists, and structs,
  but result sets must always come back in Exasol-compatible types. JSON serialization
  surfaces complex Parquet data as queryable VARCHAR strings instead of failing or
  hiding columns. One shared mapping keeps the declared schema and the emitted values in
  agreement.
- **Promotes to ADR:** yes

## Review Findings

<!-- Populated by speq-implement after code review. -->
