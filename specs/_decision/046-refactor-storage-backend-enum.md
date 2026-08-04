# Decisions: refactor-storage-backend-enum

## ADR: Externally-tagged lowercase serde representation for the storage backend

**ID:** storage-backend-externally-tagged-lowercase-serde
**Plan:** `refactor-storage-backend-enum`
**Status:** Accepted

### Context

`StorageBackend` wraps `StorageProps` and travels in the scan spec's shard-invariant common blob, serialized once per fan-out and consumed only within the same deploy's `.so` (`datafusion-scan/scan-execution-spec-reconstitution` records that there is no cross-version wire-compatibility requirement). Slice C of issue #274 will add an Azure variant, so the wire encoding chosen here has to support variant discrimination without ambiguity once a second variant exists.

### Decision

`StorageBackend` serializes externally tagged with a lowercase variant key, so the scan spec's `storage` value becomes `{"s3":{...}}` with the payload's own bytes unchanged. The tag lands in this slice, not in slice C.

### Options Considered

| Option | Verdict |
|--------|---------|
| Externally tagged, lowercase | ✓ Chosen — unambiguous variant selection; safe under the same-`.so`-produces-and-consumes guarantee |
| `#[serde(untagged)]` | ✗ Rejected — variant selection by trial deserialization; once a second variant exists, which credentials get used depends on which shape parses first, and a genuine field error degrades to "data did not match any variant" — not a corner to cut on a credentials path |
| Internally tagged (`{"backend":"s3",...}`) | ✗ Rejected — costs the same churn as externally tagged with no gain |

### Consequences

Five features (`scan-execution-spec-reconstitution`, `pushdown-module-structure`, `pushdown-col-types-consolidation`, `pushdown-joins-module-structure`, `pushdown-catalog-session`) needed a narrow `storage`-value carve-out in their byte-identical-output gates. Every non-`storage` byte of every golden staying identical is what keeps each gate a working proof rather than a retired one. Landing the tag now, while only one variant exists, means every byte of the change outside the tag is self-evidently proof that nothing else moved.

---

## ADR: Three methods on the enum plus one engine-side dispatching function

**ID:** storage-backend-three-methods-one-engine-dispatch
**Plan:** `refactor-storage-backend-enum`
**Status:** Accepted

### Context

Issue #274 lists four methods for `StorageBackend` (including DataFusion object-store registration), but `vs-adapter/catalog-crate-structure` normatively forbids `lakehouse-catalog` from declaring `object_store` or `datafusion` as a direct dependency, so a registration method cannot be constructed on a catalog-crate type.

### Decision

`StorageBackend` publishes `secret_values`, `catalog_storage_props`, and `file_io`. DataFusion object-store registration stays engine-side as one plain function in `crates/lakehouse-engine/src/scan/object_store.rs` that matches on the backend.

### Options Considered

| Option | Verdict |
|--------|---------|
| Three enum methods + one engine-side function | ✓ Chosen — respects the crate-boundary dependency ban; a free function with one match arm is the smallest construct that closes the gap |
| Four methods on the enum (issue #274's literal scope list) | ✗ Rejected — not constructible: the catalog crate cannot depend on `object_store` or `datafusion` |
| An engine-side extension trait implemented for `StorageBackend` | ✗ Rejected — an interface with one implementation and one method is the "interface with one implementation" red flag; a plain function is strictly smaller |

### Consequences

The backend decision now has two owners instead of one: the enum owns WHICH backend, the engine's single registration function owns that backend's object store. This is boundary-forced and stated openly in the plan and spec rather than left implicit. The engine-side S3-aware call count drops from four sites to one.

---

## ADR: Four further live features required a `storage`-value golden carve-out

**ID:** storage-backend-golden-carveout-second-wave
**Plan:** `refactor-storage-backend-enum`
**Status:** Accepted

### Context

Plan review (round 1) found that four live features normatively pinned the exact `dispatch_golden` goldens and join golden-SQL assertions this plan edits, requiring them to pass UNEDITED — a requirement the plan's own wire-tag change would break. Only `scan-execution-spec-reconstitution` had been amended to carve out the `storage` value; `catalog-crate-structure`, `pushdown-module-structure`, `pushdown-col-types-consolidation`, and `pushdown-joins-module-structure` had not, and one of those deltas even claimed "every other scenario of this feature is unchanged" while leaving its own byte-identical-SQL clause intact.

### Decision

Each of the four affected features gets a narrow carve-out permitting an edit to the `storage` value ALONE in its byte-identical-output clauses, leaving every other byte of every golden and every non-golden assertion as a live regression gate.

### Options Considered

| Option | Verdict |
|--------|---------|
| Narrow, per-clause `storage`-value carve-out in each affected feature | ✓ Chosen — keeps the cross-refactor behavior-preservation gate falsifiable rather than retiring it |
| Leave the clauses as originally written and accept the contradiction | ✗ Rejected — would record the spec library as self-contradictory and silently retire the repo's only cross-refactor gate |

### Consequences

`catalog-crate-structure`'s Background bullet was corrected from claiming two amended clauses to three amended scenarios. All four features gained explicit CHANGED rows and Scenario Coverage rows in `plan.md`. Round 2 of review found five FURTHER live clauses across three features (including one, `pushdown-catalog-session`, with no delta directory at all) that the round-1 fix had missed — the defect class, not just the four named clauses, needed applying, which produced the `pushdown-catalog-session` delta and widened the other two.

---

## ADR: The exhaustive variant-naming owner list is capped at five permitted modules

**ID:** storage-backend-exhaustive-variant-naming-owners
**Plan:** `refactor-storage-backend-enum`
**Status:** Accepted

### Context

Plan review (round 1) found that `vs-adapter/storage-backend-enum` scenario 2's original clause permitted only the enum's own methods and the engine's registration function to match a `StorageBackend` variant or read its payload — a restriction the plan's own tasks violated in three further places (`vended.rs`'s S3 arm, `connection.rs::storage_block`'s construction, and each crate's `#[cfg(test)]` support modules), and which named no single owner for backend SELECTION, the exact question slice C needs answered.

### Decision

The clause names five permitted owners exhaustively — the enum's own methods, the engine's single registration function, `resolve_vended_storage`'s S3 arm (credential overlay only, forbidden from changing the variant), `storage_block` (construction at the CONNECTION-parsing entry point), and `CommonScanSpec`'s manual `impl Default` (a construction site, not a selection site) — with any `#[cfg(test)]` module permitted as a sixth, unbounded category. `storage_block` is additionally named the ONLY place a backend is SELECTED FROM INPUT.

### Options Considered

| Option | Verdict |
|--------|---------|
| Five named production owners + unbounded `#[cfg(test)]` carve-out, with `storage_block` as the sole selection site | ✓ Chosen — matches what the plan's own tasks actually do, and gives slice C exactly one home for URI-scheme- or property-driven selection |
| Leave the original two-owner clause as written | ✗ Rejected — unsatisfiable against the plan's own design; forbade the plan's own tasks |

### Consequences

A later round-2 finding showed the five-owner list still collided with a round-1 `impl Default` completeness fix; the list was corrected to be explicit that a fixed `S3` placeholder construction is not a "selection from input" and does not count against the sole-selection clause. The test-only carve-out widened from "each crate's own support module" to any `#[cfg(test)]` module.
