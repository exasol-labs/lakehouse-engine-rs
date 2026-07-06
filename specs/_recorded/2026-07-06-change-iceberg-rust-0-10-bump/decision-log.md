# Decision Log: change-iceberg-rust-0-10-bump

Date: 2026-07-06

Tracking: GitHub issue #65 (https://github.com/exasol-labs/lakehouse-engine-rs/issues/65),
cross-linked to #11. Implementing commit references `Closes #65` (not `#11`).

## Interview

**Q:** Should this plan cover just the version bump, or also wire in issue #11's delete-application
fix (routing scans through `ArrowReaderBuilder`)?
**A:** Bump only. Land the version bump, arrow-tree unification, and any required API-shape fixes as
its own plan; #11's delete work is a separate follow-on plan that builds on this one. Because deletes
are out of scope, this plan must NOT touch `ArrowReaderBuilder` at all — today's code never calls it
(only `plan_files()`/`FileScanTask` path+size metadata crosses from iceberg into the DataFusion side;
no iceberg reader runs in production). The iceberg-rust APIs actually exercised are: catalog
construction (`RestCatalogBuilder` + `OpenDalStorageFactory::S3`), `list_tables`,
`LoadTableResult`/`TableMetadata`, `iceberg::table::Table`,
`table.scan().with_filter(...).select_all().build()` → `plan_files()` → `FileScanTask`
(`data_file_path()`, `file_size_in_bytes`), `expr::{Predicate, Reference, Datum}` + `spec::Schema`,
`TableIdent`/`NamespaceIdent`, `spec::{Type, PrimitiveType}`, and (test-only) the full writer stack.
The bump's job is to make ALL of these compile and behave identically against 0.10.0-rc.2.

**Q:** How should the 0.10.0-rc.2 pin be expressed in Cargo.toml?
**A:** An exact pin rather than a loose range, since it is a pre-release RC whose API can still churn
— a later 0.10.x/rc bump should be deliberate and reviewed, not automatic. Verify during planning
whether it is a crates.io `=0.10.0-rc.2` pin or a git-tag pin.

**Q:** Should this get its own GitHub issue?
**A:** Yes — filed as GitHub issue #65, cross-linked to #11. Reference `Closes #65` in the
implementing commit, per the repo's feature-tracking convention.

## Design Decisions

### [1] Pin iceberg 0.10.0-rc.2 via git tag, not a crates.io exact-version pin

- **Decision:** Pin `iceberg`, `iceberg-catalog-rest`, `iceberg-storage-opendal` to the git tag
  `v0.10.0-rc.2` (commit `be6cc96eaeb1cac4574cabb11ea6e1e92e0aad45`) of `apache/iceberg-rust`.
- **Alternatives:** `version = "=0.10.0-rc.2"` from crates.io — rejected, because verification showed
  crates.io publishes only up to 0.9.1; 0.10.0-rc.2 is a git tag, never published to crates.io. A
  bare `rev = <sha>` — rejected in favor of the human-readable, immutable tag.
- **Rationale:** A git-tag pin is the only mechanism that resolves the RC, and it honors the
  interview's "deliberate, reviewed bump" intent — the tag is immutable and a later RC/GA requires an
  explicit edit.
- **Promotes to ADR:** yes

### [2] Unify the production/iceberg arrow tree on 58; do NOT bump the workspace arrow major

- **Decision:** iceberg 0.10.0-rc.2 is on arrow/parquet 58 (verified against the tag's `Cargo.toml`),
  so the production 57/58 split collapses onto arrow-58 without any change to the workspace `arrow`,
  `datafusion`, or SDK pins.
- **Alternatives:** Move the workspace arrow to 59 (to match tpchgen-arrow 3.0.0) — rejected: it
  would drag datafusion 54, `exasol-udf-sdk` 0.20.2, and iceberg 0.10 (all arrow-58) off their
  pinned tree, a far larger and out-of-scope change.
- **Rationale:** The bump's real target — the arrow-57/58 split that existed only because iceberg
  0.9.1 linked arrow 57 — is fully eliminated by keeping everything on arrow 58.
- **Promotes to ADR:** yes

### [3] Drop `tpchgen-arrow`; build arrow-58 batches from `tpchgen` core — single arrow tree, no bridge

- **Decision:** `tpchgen-arrow` publishes 2.0.2 (arrow 57) and 3.0.0 (arrow 59) but **no arrow-58**
  release, while the 0.10 iceberg writer now expects arrow-58 batches. Rather than keep a second
  (divergent) arrow tree in the dev graph and bridge across it, **remove the `tpchgen-arrow`
  dependency entirely** and construct arrow-58 `RecordBatch`es directly in `seed.rs`/`tpch_loader.rs`
  from `tpchgen` **core** (which has **zero dependencies** — no arrow at all — verified in
  `Cargo.lock`), using the workspace `arrow` 58 builders. Result: the dev/e2e graph collapses onto a
  **single arrow-58 tree** — no arrow 57, no arrow 59, no Arrow IPC bridge anywhere.
- **Alternatives (both rejected — chosen by the user 2026-07-06):**
  - *Keep tpchgen-arrow 2.0.2 (arrow 57) + Arrow IPC bridge* — least code churn, but leaves arrow 57
    permanently in the dev lock and introduces an IPC round-trip in `seed.rs` that does not exist
    today (today tpchgen-arrow and iceberg 0.9.1 are both arrow-57, so batches feed the writer
    directly). Two dev arrow trees persist.
  - *Bump tpchgen-arrow to 3.0.0 (arrow 59) + IPC bridge* (this plan's original preferred) — purges
    arrow 57 only by introducing arrow **59**, a tree *newer* than the workspace's 58, plus tpchgen
    generator API churn plus the same IPC bridge. Strictly worse than either other option.
  - *Feed tpchgen-arrow batches straight into the writer* — impossible across incompatible arrow type
    trees. *Drop the TPC-H loader* — rejected, it backs the live smoke test.
- **Rationale:** `tpchgen` core is a pure row generator with no arrow dependency, so building the
  arrow-58 columns by hand is the only path that reaches a genuinely single arrow tree and removes
  the cross-tree hand-off altogether — matching the bump's whole purpose (unify on arrow 58). The
  cost is bounded, test-only code (~100–200 lines of column builders across the 8 TPC-H tables that
  `tpch_loader.rs` uses; today handled by tpchgen-arrow's `RegionArrow`/`SupplierArrow`/… iterators),
  which replaces the `RecordBatchIterator`-based emitters. No production code is affected. This also
  removes a future-arrow-bump hazard rather than deferring it.
- **Note for #11 / future arrow bumps:** because the generator batches are now built with the
  workspace arrow directly, a future workspace arrow bump needs no coordinated tpchgen-arrow release.
- **Promotes to ADR:** yes

### [4] Author no spec deltas; leave the stale `0.9.1` prose in cloud-credentials Background untouched

- **Decision:** The bump changes zero observable behavior, so the plan authors no scenario deltas and
  edits no permanent spec file. The `iceberg-catalog-rest 0.9.1` reference in the Background of
  `vs-adapter/pushdown-planning-cloud-credentials` is illustrative (it explains why the self-issued
  `loadTable` GET is required); it is left for refresh when that feature is next recorded.
- **Alternatives:** Restate an unchanged scenario as a CHANGED delta just to have a delta — rejected
  as noise. Edit the spec Background directly during the plan — rejected: specs are updated via the
  record flow, and the version string is non-normative.
- **Rationale:** speq deltas are scenario-scoped; nothing behavioral changes here. The doc-consistency
  refresh (CLAUDE.md + comments) is handled as a non-spec task (plan Task 12).
- **Promotes to ADR:** no

### [5] Do not touch `ArrowReaderBuilder`

- **Decision:** Leave all reader-path code alone. Production never invokes `ArrowReaderBuilder`, so
  its reported 0.10 signature change (a new `runtime` argument) is irrelevant to this bump.
- **Alternatives:** Adopt the reader now — rejected, that is #11's delete-application scope.
- **Rationale:** Keeps the bump strictly mechanical and behavior-preserving; the reader migration is
  a separate, deliberate change with its own tests.
- **Promotes to ADR:** no

## Review Findings

<!-- Populated by speq-implement after code review. -->
