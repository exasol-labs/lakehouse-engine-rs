# Decisions: refactor-catalog-crate-extraction

## ADR: `lakehouse-catalog` owns the three credential/config types; no shared-types crate

**ID:** catalog-crate-owns-credential-types-no-shared-types-crate
**Plan:** refactor-catalog-crate-extraction
**Status:** Accepted

### Context

`CatalogSession` is `pub(crate)` in `lakehouse-engine`, so `resolve_file_list` cannot take it directly and plan `refactor-catalog-http-session` (#185) shipped a public single-use wrapper over a `pub(crate)` core. A crate boundary is the only construct in Rust that makes `CatalogSession` genuinely `pub` while keeping `CatalogAuth`, the OAuth grant, and the prefix lookup unreachable. That boundary forces a decision on `ConnectionCreds`, `CatalogProps`, and `StorageProps`: the catalog code depends on all three, and the dependency edge can only run `lakehouse-engine` → `lakehouse-catalog`, so any type both crates name must be declared in the catalog crate.

### Decision

Two crates, not three. `StorageProps`, `CatalogProps`, and `ConnectionCreds` are declared exactly once, in `lakehouse-catalog`, and re-exported from `lakehouse_engine::scan::spec` and `lakehouse_engine::adapter::connection` at their pre-move paths and visibilities, so no consumer's `use` line changes. The Exasol-facing parsers (`read_connection`, `validate_creds`, `parse_creds`, `storage_block`, `catalog_block`, `REQUIRED_KEY`) stay in the engine.

### Options Considered

| Option | Verdict |
|--------|---------|
| Two crates; catalog crate declares the three shared types, re-exported at pre-move paths | ✓ Chosen — one definition, one serde contract, zero conversion code, and every consuming file (4 `scan/*.rs`, 10 `adapter/**`, 13 `tests/*.rs`) compiles unedited |
| A third `lakehouse-types` crate holding all three | ✗ Rejected — groups by technical role, the wrong axis per `/speq:design-philosophy`, and separates `ConnectionCreds` from every function that interprets it |
| Leave the types in `lakehouse-engine` | ✗ Rejected — `lakehouse-catalog` would then need `lakehouse-engine`, a dependency cycle |
| Parallel `CatalogAuthConfig` / `S3Config` types plus boundary conversions | ✗ Rejected — duplicates ~10 of `ConnectionCreds`' 14 fields and the whole of `StorageProps`, is the back-door duplication `/speq:design-philosophy` warns against on a struct that must stay byte-stable on the UDF wire, and forces every one of the 1,805 moved test lines to stop constructing `ConnectionCreds` directly |

### Consequences

`StorageProps`, `CatalogProps`, and `ConnectionCreds` split on evidence: `CatalogProps` is mis-homed in `scan/spec.rs` today (no `scan/*.rs` module names it), `ConnectionCreds` appears in 11 planning-layer files and none in `scan/`, and only `StorageProps` has two genuine owners — the catalog produces it, the scan UDF consumes it across the UDF boundary as JSON. Placing each definition with its producer and re-exporting at the consumer's path keeps one definition and zero conversion code, at the cost of a naming smell the crate's own doc comment states outright: an S3 storage config type lives in a crate named "catalog".

## ADR: Issue #214 is absorbed and closed as subsumed, not executed first and not blocked on

**ID:** absorb-214-deliver-vended-consolidation-once-in-final-shape
**Plan:** refactor-catalog-crate-extraction
**Status:** Accepted

### Context

Issue #214 (the `resolve_vended_storage` consolidation) had not landed when this plan started: no `resolve_vended_storage` existed in the codebase, and `pushdown/mod.rs` still re-exported `extract_vended_keys` / `merge_vended_into_storage` directly. Issue #204 (this plan) unfreezes the pushdown façade that #214's defining constraint — "zero public-surface change" — depends on, so the two issues' relationship needed an explicit ordering decision.

### Decision

This plan performs the `resolve_vended_storage` consolidation itself, once, directly in its final shape: concept-level, `pub` on `lakehouse-catalog`, with the seven mechanism functions crate-private and the extractors normalized to uniform `Option`. Issue #214 closes as subsumed. The code moves verbatim first (parity gate: the existing suite unedited), then the logic consolidates (parity gate: the six absence/precedence cases).

### Options Considered

| Option | Verdict |
|--------|---------|
| Absorb #214, deliver the consolidation once in final shape | ✓ Chosen — one consolidation, one review, one behavior-parity gate, with the same two falsifiable checkpoints a #214-then-#204 ordering would have given, without a throwaway intermediate shape |
| Block on #214 as an unmet prerequisite | ✗ Rejected — #214's only stated purpose is to prepare #204; blocking produces no work and waits on an issue whose deliverable this plan is about to redraw anyway |
| Execute #214 verbatim as task 1, then redraw the façade | ✗ Rejected — #214's "zero public-surface change" constraint exists only because the façade is frozen, and #204 unfreezes it in the same change; executing #214 first means building a `pub(super)` shape whose only reason to exist is a constraint this plan deletes, and keeps `extract_vended_keys` / `merge_vended_into_storage` `pub` for one extra commit with no verification benefit |

### Consequences

Issue #214 closes as subsumed rather than shipping its own PR. The risk a single larger change raises is answered by ordering — verbatim move, then consolidate — rather than by splitting into two issues.

## ADR: The crate boundary is drawn at catalog access, not at Iceberg file planning

**ID:** catalog-crate-boundary-at-access-not-file-planning
**Plan:** refactor-catalog-crate-extraction
**Status:** Accepted

### Context

Issue #204's title is "extract catalog HTTP/session code into a dedicated crate". Iceberg file planning (`resolve_file_list`, `plan_files_from_table`, `ensure_supported_delete_mechanisms`, `build_logical_schema`, `parse_name_mapping`, `empty_result_sql`, `relativize_shards_to_root`) consumes the catalog's output and produces scan-spec wire types (`FileEntry`, `LogicalField`, `NameMappingEntry`, `DeleteFileRef`) and Arrow type tags. Where to draw the crate boundary relative to that code needed an explicit decision.

### Decision

File planning stays in `lakehouse-engine`. The crate takes catalog authentication, the session, the `loadTable` GET, namespace enumeration, SigV4 signing, vended-storage resolution, the credential types, and credential redaction.

### Options Considered

| Option | Verdict |
|--------|---------|
| Boundary at catalog access only; file planning stays in the engine | ✓ Chosen — matches the issue's own scope, and lets the crate's manifest exclude `arrow`, `parquet`, `datafusion`, and `object_store` as direct dependencies — a machine-checkable form of the boundary |
| Move file resolution into the crate too, so `resolve_file_list` is a `lakehouse-catalog` function | ✗ Rejected — drags `FileEntry`, `LogicalField`, `NameMappingEntry`, `DeleteFileRef`, and the Arrow type-tag mapping across the boundary, making the catalog crate own the scan-spec wire format: a second responsibility and a far larger blast radius than the issue asks for |

### Consequences

The catalog crate's manifest carries no execution-engine dependency, verified by a dedicated boundary test (`catalog_manifest_declares_no_execution_engine_dependency`) and by `cargo tree -p lakehouse-catalog --depth 1` showing none of `arrow`, `parquet`, `datafusion`, `object_store`, or `roaring`. The scan-spec wire format and Arrow type mapping remain a single-crate concern.
</content>
