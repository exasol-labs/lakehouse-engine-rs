# Plan: fix-iceberg-initial-default-rendered-types

## Summary

An Iceberg column added with an `initial-default` of type `time`, `decimal` with precision above 36, `binary`, `fixed(L)`, or `uuid` reads that default on rows of files written before the column existed, instead of NULL or the required-absent error (#367). A default the adapter cannot render at the column's declared type refuses the column instead of reading NULL.

## Context

- Iceberg `format/spec.md`, "Column Projection", rule (3): "Return the default value if it has a defined `initial-default`". The engine follows it for every primitive except these five types, which return rule (4), NULL, or fail a REQUIRED column with `Non-nullable column ... is missing from the physical schema`.
- `encode_initial_default` (`crates/lakehouse-engine/src/adapter/pushdown/format/iceberg.rs`) encodes only primitives with a native Arrow tag and returns `None` for the five types.
- `reconstruct_initial_default` (`crates/lakehouse-engine/src/scan/field_id_projection.rs`) has no arm for the `binary` and `fixed_size_binary(L)` tags.
- `render_identity_value` in the same adapter file already renders an Iceberg literal as the scan reads a stored value of the column, with lowercase hex for `binary`, `fixed`, and `uuid`, and `scan::partition_scalar` parses that form back (#546).
- The recorded scenario "A decimal initial-default outside Exasol's DECIMAL domain is not encoded as a numeric default" named the NULL fallback a trade-off. Rendering the default as text removes the need for it.
- `render_identity_value` and `partition_scalar` word their errors for a partition value ("the partition struct holds ...", "a binary partition value must be hex"). Serving a second caller needs a context-free cause and a neutral name.
- `identity_hex` hex-encodes any `Binary` literal without a length check, so a wrong-length `fixed(L)` default would reach the scan and fail every query on the table.
- Arrow rejects a `Decimal128` whose scale exceeds its precision, so the renderer cannot render a `decimal(5,10)` default.
- `scan-types/type-mapping-iceberg-logical` names the `initial-default` encoding gate as a reader of the DECIMAL domain check; the delta rewords it, because the domain now picks the raw or the rendered form.
- Nested (`struct`, `list`, `map`) defaults stay unencoded, as recorded.

## Features

| Feature | Status | Spec |
|---------|--------|------|
| scan-read-path/scan-execution-field-id-projection-absent-fields | CHANGED | `specs/_plans/fix-iceberg-initial-default-rendered-types/scan-read-path/scan-execution-field-id-projection-absent-fields/spec.md` |
| scan-types/type-mapping-iceberg-logical | CHANGED | `specs/_plans/fix-iceberg-initial-default-rendered-types/scan-types/type-mapping-iceberg-logical/spec.md` |

## Impact

After the next `CREATE`, `REFRESH`, or `SET PROPERTIES` of a virtual schema, because the encoded default lives in the column note:
- Rows of pre-add files return the declared `initial-default` for `time`, `decimal(p>36)`, `binary`, `fixed(L)`, and `uuid` columns, where they returned NULL. A query over such a REQUIRED column succeeds where it failed.
- A column whose primitive `initial-default` cannot be rendered at its declared type (a `decimal` with scale above precision, a `fixed(L)` default of another length) becomes a refused column: a query that reads it fails with a clean reason, and other columns stay queryable.
- The scan-spec wire form of every existing default is unchanged. No breaking change.

## Dependencies

- None. `hex` and the identity-partition renderer are already in the adapter crate.

## Dead Code Removal

| Type | Location | Reason |
|------|----------|--------|
| Test | `crates/lakehouse-engine/src/adapter/pushdown/pushdown_tests.rs` `iceberg_declaration_omits_default_for_decimal_outside_exasol_domain` | Asserts the removed scenario; replaced by the rendered-default tests below |
| Doc table | `crates/lakehouse-engine/src/scan/spec.rs` `LogicalField` `initial_default` encoding table | The "JSON-fallback `utf8` encodes `None`" row no longer holds; add the rendered-text and hex rows |

## Verification

### Scenario Coverage

| Scenario | Test Type | Test Location | Test Name |
|----------|-----------|---------------|-----------|
| The VS encodes each field's Iceberg initial-default once per query into the scan spec | Unit | `crates/lakehouse-engine/src/adapter/pushdown/format/iceberg_tests.rs` | `declaration_encodes_every_primitive_initial_default` |
| Every supported primitive initial-default survives the scan-spec serialization round-trip | Unit | `crates/lakehouse-engine/src/scan/field_id_projection_tests.rs` | `initial_default_round_trips_across_full_type_vocabulary` (extended with `binary`, `fixed_size_binary(L)`, and a wrong-length `fixed` hex) |
| An initial-default of a type without a native Arrow tag reads as a stored value of its column reads | Integration (E2E) | `crates/lakehouse-e2e/tests/e2e_local/scan.rs` | `e2e_added_columns_initial_default_fill_all_types`, gaining this scenario's `/// Scenario:` line. Fixture `seed_added_columns_initial_default` gains `c_time`, REQUIRED `c_time_req`, `c_dec38`, `c_binary`, `c_fixed`, `c_uuid`, with post-add rows carrying the same values; the test asserts each pre-add value equals the post-add value of the same column in the same result set, besides the expected literal |
| A primitive initial-default that cannot be read at its column's declared type refuses the column | Unit | `crates/lakehouse-engine/src/adapter/pushdown/format/iceberg_tests.rs` | `declaration_refuses_a_column_whose_initial_default_cannot_be_rendered` (`decimal(5,10)`, wrong-length `fixed(4)`, and a hand-built mismatched literal; asserts the reason names `initial-default`, not a partition) |
| A catalog-declared DECIMAL outside Exasol's DECIMAL domain falls back to VARCHAR | Unit | existing test of that scenario | unchanged; the delta rewords one cross-reference only |

### Manual Testing

| Feature | Command | Expected Output |
|---------|---------|-----------------|
| scan-read-path/scan-execution-field-id-projection-absent-fields | `make e2e-up test-e2e`, then with `exapump`: `SELECT "id", "c_time", "c_time_req", "c_dec38", "c_binary", "c_fixed", "c_uuid" FROM <vs>."INITDEF" ORDER BY "id"` | Rows `id` 1 to 3 show `22:31:08.123456`, `22:31:08.123456`, `123.45`, `deadbeef`, `01020304`, `0123456789abcdef0123456789abcdef`, each identical to the post-add row written with the same values; no `22002` error |

### Checklist

| Step | Command | Expected |
|------|---------|----------|
| Build | `make cross-udf-build` | Exit 0 |
| Test | `cargo test --workspace` | 0 failures |
| E2E | `make e2e-up test-e2e` | 0 failures |
| Lint | `make fmt-check lint` | 0 errors/warnings |
