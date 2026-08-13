# Tasks: fix-decimal-precision-scale-guard

## Phase 1: Verification Gate (Group A)
- [x] 1.1 Bring up Docker Exasol, run the four DECIMAL(p,s) probes, record verbatim results in decision-log.md § Live Captures. STOP if either bad shape is accepted.

## Phase 2: Implementation (Group B)
- [x] 2.1 Add failing unit tests to mapping_tests.rs per § Test Disposition; confirm they fail against unmodified guards.
- [x] 2.2 Add private `decimal_to_exasol` to mapping.rs; repoint both call sites; update unity doc comment; remove dead arms/comment.
- [x] 3.1 CLAUDE.md § Data types: add one sentence on the catalog-decimal domain beneath the Arrow-to-Exasol table.

## Phase 4: Review Fixes
- [x] 4.1 mapping.rs: give the Exasol-catalog-decimal domain exactly one owner both directions read — add the shared predicate, branch the renamed `catalog_decimal_to_exasol` on it, route `iceberg_primitive_to_arrow`'s decimal arm through it, restate its parity doc comment, and pin `(0,0)`/`(5,10)` → `Utf8` in `iceberg_type_to_arrow_maps_all_families` [expert]
- [x] 4.2 mapping.rs: name Exasol's max decimal precision as `EXASOL_DECIMAL_MAX_PRECISION` beside `DECIMAL_INT64_MAX_PRECISION` and read it in the shared predicate instead of the bare `36` [expert]
- [x] 4.3 mapping_tests.rs: add `(37, 0, "VARCHAR(2000000)")` after the `(36, 36, ...)` row and `(5, 6, "VARCHAR(2000000)")` after the `(5, 10, ...)` row in the `cases` array in `catalog_decimal_guard_is_shared_by_both_source_kinds`, pinning both off-by-one guard edges
- [x] 4.4 CLAUDE.md: fix `DECIMAL(p≤36, s≤36)` to `DECIMAL(1≤p≤36, 0≤s≤p)` on line 222, and replace the lines 242-244 paragraph to state Exasol's own DECIMAL domain (not a catalog-path-specific extra requirement) with the live-captured rejections

## Phase 5: Verification
- [x] 5.1 Run build/test/lint/format/spec-validation checklist
- [x] 5.2 Scenario coverage audit
- [x] 5.3 Manual testing (Exasol probes + targeted cargo test runs)
