# Plan: add-timestamp-precision-versioning

## Summary

Declare Iceberg and Delta timestamp columns as `TIMESTAMP(6)` on Exasol 2025.x and later — gated on
`ctx.database_version()` — so Exasol stops silently truncating the microsecond digits the Iceberg spec
stores, and keep the bare `TIMESTAMP` declaration on 8.x, which has no parameterized-precision type.
Add an 8.29.x leg to the core E2E CI matrix and the first E2E assertions that can actually observe
sub-millisecond fidelity.

## Design

### Context

Exasol's bare `TIMESTAMP` is `TIMESTAMP(3)`. Both catalog-declaration producers —
`iceberg_primitive_to_exasol` for Iceberg and `unity_type_name_to_exasol` for Delta — hardcode that
bare string, so Exasol truncates three sub-millisecond digits off every timestamp value on every
version. The Iceberg spec defines `timestamp`/`timestamptz` as microsecond types, so this is a spec
deviation rather than a target-type trade-off. No test caught it: every timestamp assertion in the
suite reads a value at seconds resolution, a whole-millisecond fixture, or a prefix-matched type name.

Exasol 8.x has no parameterized `TIMESTAMP`, so the declaration cannot simply change — it has to
depend on the engine version, which is a kind of decision this codebase has never made before.
`UdfContext::database_version()` has zero call sites here today.

- **Goals** — one owner for the version rule and both declaration strings; microsecond fidelity on
  2025.x for Iceberg *and* Delta; byte-identical behavior on 8.x; E2E assertions that fail on
  truncation; CI coverage of both engine lines.
- **Non-Goals** — `TIMESTAMP(9)`; changing the `timestamptz`-to-`TIMESTAMP` zone-flattening trade-off;
  a general version-comparison or feature-flag framework; a user-settable precision property; touching
  the pushdown, CAST, or emit-coercion paths, which already handle `TIMESTAMP(p)`.

### Decision

#### Architecture

The version is read once, at the adapter edge, and travels inward as a plain `Copy` value. The
type-mapping module owns the decision but never touches the UDF context.

```
handle_create_virtual_schema(ctx)                     adapter/mod.rs
  │  ctx.database_version()  ── read ONCE per request
  ▼
TimestampPrecision::from_database_version(&str)        types/mapping.rs   ◀── single owner:
  │                                                                          version rule +
  │  plain Copy value                                                        both declaration strings
  ▼
build_listing_virtual_tables(ns, listing, precision)   adapter/mod.rs
  ▼
column_source_type_to_exasol(source_type, precision)   types/mapping.rs
  ├── iceberg_type_to_exasol ──▶ iceberg_primitive_to_exasol   (Iceberg)
  └── unity_type_name_to_exasol                                (Delta)
  ▼
exasol_type_to_json("TIMESTAMP(6)")  ──▶  {"type":"timestamp","fractionalSecondsPrecision":6}
  ▼
Exasol declares the VS column TIMESTAMP(6), echoes fractionalSecondsPrecision on the pushdown request
  ▼
exasol_type_from_json ──▶ EMITS "TIMESTAMP(6)" ──▶ exasol_type_to_arrow ──▶ Timestamp(Microsecond)
                                                   (all three ALREADY correct — no change)
```

#### Patterns

| Pattern | Where | Why |
|---------|-------|-----|
| Read context at the edge, thread a plain value | `handle_create_virtual_schema` | The recorded `cluster_nodes_from_context` shape: `types/mapping.rs` stays free of `UdfContext`, reads no ambient state, and does no I/O |
| One owner for a decision with two producers | `TimestampPrecision` in `types/mapping.rs` | Two producers already hardcoded the same literal — the exact shape that let the catalog-decimal guard drift into four copies before #329 |
| Named `Copy` enum, not a `bool` | `TimestampPrecision::{Millisecond, Microsecond}` | A `bool` threaded through five signatures inverts silently; the variant names carry the meaning at every call site |
| Independent test oracle | E2E version-to-precision helper | A test that derives its expectation by calling the rule under test cannot fail when that rule is wrong |
| Modern default on unknown input | empty/unparseable version | Loud failure on an unrecognised engine beats silent truncation on every known one |

### Consequences

| Decision | Alternatives Considered | Rationale |
|----------|------------------------|-----------|
| `TimestampPrecision` enum owns the rule and both strings, in `types/mapping.rs` | A `bool` parameter; a free function returning `&'static str`; a broader `EngineFeatures` struct | The module already owns Exasol's type domain (`exasol_representable_catalog_decimal`). A `bool` is invertible at five call sites; a broader struct generalizes for a second use case that does not exist |
| Parse the leading dot-separated component, gate `>= 2025` | Full semver comparison; explicit `8.` prefix match; regex | Exasol moved to calendar versioning after 8.x, so one integer threshold separates the lines. The observed strings are exactly the image tags `8.29.13` and `2025.2.1` |
| Empty **and** unparseable both yield `TIMESTAMP(6)` | Default to bare `TIMESTAMP` (the conservative option); distinguish empty from unparseable | User's explicit choice. An unknown engine that rejects `TIMESTAMP(6)` fails visibly at `createVirtualSchema`; the conservative default would truncate data silently on every engine it misjudged. Two arms for two no-information inputs would drift |
| Fix the Delta path via `unity_type_name_to_exasol` | Thread the gate through `arrow_to_exasol_type`, as issue #359's scope text says | The Delta declaration reaches Exasol only through the Unity Catalog kind. `arrow_to_exasol_type` has no call site in the crate (recorded in `datafusion-scan/type-mapping-module-structure`), so gating it changes no observable answer |
| No version read in the scan UDF | Read `database_version()` there too | The scan's `EMITS` types come from the pushdown request's own `dataType` JSON, which Exasol derives from the declaration. A second read would give one decision two owners |
| Keep one CI matrix leg named exactly `E2E` | Rename both legs; add an aggregate gate job named `E2E` | `E2E` is a required check on `main`'s ruleset. Renaming both legs blocks every PR until an admin edits the ruleset; an aggregate job reports *skipped* when its dependency fails, and GitHub counts skipped as satisfied — silently unblocking a red E2E |
| Verify live before implementing | Trust the issue's findings and Exasol's docs | CLAUDE.md § Verification discipline: that Exasol accepts `fractionalSecondsPrecision` in a `createVirtualSchema` response, declares `TIMESTAMP(6)`, echoes the field back, and preserves microseconds across `emit_batch` are all live-SQL claims no document settles |

## Features

| Feature | Status | Spec |
|---------|--------|------|
| datafusion-scan/type-mapping | CHANGED | `specs/_plans/add-timestamp-precision-versioning/datafusion-scan/type-mapping/spec.md` |
| vs-adapter/create-virtual-schema | CHANGED | `specs/_plans/add-timestamp-precision-versioning/vs-adapter/create-virtual-schema/spec.md` |
| vs-adapter/unity-catalog-create-virtual-schema | CHANGED | `specs/_plans/add-timestamp-precision-versioning/vs-adapter/unity-catalog-create-virtual-schema/spec.md` |
| vs-adapter/delta-type-mapping | CHANGED | `specs/_plans/add-timestamp-precision-versioning/vs-adapter/delta-type-mapping/spec.md` |
| e2e-harness/e2e-harness | CHANGED | `specs/_plans/add-timestamp-precision-versioning/e2e-harness/e2e-harness/spec.md` |
| e2e-harness/unity-catalog-e2e-harness-delta-queries | CHANGED | `specs/_plans/add-timestamp-precision-versioning/e2e-harness/unity-catalog-e2e-harness-delta-queries/spec.md` |

## Impact

**Timestamp columns gain three digits of precision on Exasol 2025.x and later.** An Iceberg or Delta
`timestamp`/`timestamptz` column is declared `TIMESTAMP(6)` instead of bare `TIMESTAMP`, so
`SYS.EXA_ALL_COLUMNS` reports `TIMESTAMP(6)`, values render six fractional digits instead of three, and
two rows differing only below millisecond stop collapsing under `DISTINCT`, `GROUP BY`, and equality.
Exasol 8.x is byte-identical to today.

**Not a breaking change to any query, but a visible change to rendered output.** A client that
string-matches a timestamp's rendered form, or that pins the declared column type, sees the wider form.
Numeric and date-function results are unaffected. Nothing about the emitted VALUE changes — Exasol
simply stops discarding digits it was given all along.

**Existing virtual schemas keep the old declaration until refreshed.** Exasol persists the declared
column types from `createVirtualSchema`, so a schema created before this ships still reports bare
`TIMESTAMP` until `ALTER VIRTUAL SCHEMA <name> REFRESH` (or a re-create) runs. Operators who want
microsecond fidelity on existing schemas must refresh them.

**One operator action in CI.** The 8.29.x matrix leg reports under a new status-check name and must be
added to `main`'s ruleset required checks — the same step issue #336 already tracks for `e2e-azure`.
The leg named `E2E` is unchanged, so no PR is blocked in the meantime.

## Dependencies

None new. `exasol-udf-sdk` 0.22.1 already exposes `UdfContext::database_version()`. The 8.29.x E2E leg
needs only the published `exasol/docker-db:8.29.13` image, already proven green by PR #358.

## Migration

| Current | New |
|---------|-----|
| Iceberg/Delta timestamp column declared bare `TIMESTAMP` on every engine | `TIMESTAMP(6)` on Exasol 2025.x and later; bare `TIMESTAMP` on 8.x |
| Virtual schema created before this change | Keeps its persisted bare-`TIMESTAMP` declaration until `ALTER VIRTUAL SCHEMA <name> REFRESH` |
| CI `e2e` job: one leg on `2025.2.1` | Two legs — `2025.2.1` (check name `E2E`, unchanged) and `8.29.13` (new check name, needs adding to the ruleset) |
| `CLAUDE.md`'s Data types table maps Arrow `Timestamp(_, _)` to bare `TIMESTAMP` | Same row states the version gate in one line and points at the `datafusion-scan/type-mapping` spec for the rule (task 12) |

## Implementation Tasks

1. **Live-verify the precision surface against the Docker stack, before any declaration changes** —
   record every capture in `decision-log.md`. On `exasol/docker-db:2025.2.1`: (a) a `createVirtualSchema`
   column `dataType` of `{"type":"timestamp","fractionalSecondsPrecision":6}` is accepted and
   `SYS.EXA_ALL_COLUMNS` reports the column as `TIMESTAMP(6)`; (b) the pushdown request's
   `involvedTables` column `dataType` echoes `fractionalSecondsPrecision`; (c) `TIMESTAMP(6)` is
   accepted as a scan-script `EMITS` output type; (d) a microsecond value survives the `emit_batch`
   round trip with all six digits; (e) which live-session source carries the engine version
   (`SYS.EXA_METADATA` `PARAM_NAME='databaseVersion'` versus the WebSocket login response's version
   field) and what `ctx.database_version()` reports. Then on `exasol/docker-db:8.29.13`: repeat (a) to
   capture what 8.x does with the field, and confirm `ctx.database_version()` reports an `8.`-leading
   string. `EXASOL_IMAGE=exasol/docker-db:8.29.13` selects the stack; check for a stray `bench/.env`
   first. [expert]
2. Add `TimestampPrecision` to `crates/lakehouse-engine/src/types/mapping.rs`: a two-variant `Copy`
   enum owning both declaration strings and `from_database_version(&str)` (leading dot-separated
   component parsed as an integer, `>= 2025` → microsecond, everything else including empty and
   unparseable → the documented default). Unit-test the matrix over `2025.2.1`, `8.29.13`, `7.1.20`,
   `2026.1.0`, `""`, `v2025.2.1`, `unknown`, `.2.1`, and a bare `2025`. [expert]
3. Thread the resolved precision through the declaration pipeline and read the context once. Exact
   call-site census — every site below must be updated, and there are no others in the workspace:
   production `types/mapping.rs:421`, `:434`, `:439`, `adapter/mod.rs:285`, `:606`; the new signature
   on `iceberg_primitive_to_exasol`, `iceberg_type_to_exasol`, `column_source_type_to_exasol`,
   `unity_type_name_to_exasol`, `build_listing_virtual_tables`; tests
   `types/mapping_tests.rs:325-398` (16 sites), `:967-1090` (10 sites), `:1146`,
   `adapter/adapter_tests.rs:1706`, `adapter/catalog_client_tests.rs:73`, `:75`, and the
   function-pointer surface probe at `catalog_client_tests.rs:17`, whose declared type changes too. No
   feature-gated E2E test crate calls any of them. Both timestamp arms read `TimestampPrecision`;
   `compatible_exasol_type`'s `Timestamp(_, _)` arm gains one line stating it is deliberately outside
   the gate. [expert]
4. Give `exasol_type_to_json` a `TIMESTAMP(p)` arm returning
   `{"type":"timestamp","fractionalSecondsPrecision":p}`, matched after the exact `TIMESTAMP WITH LOCAL
   TIME ZONE` and bare `TIMESTAMP` arms so both keep their recorded objects, and before the catch-all
   that would otherwise declare the column `VARCHAR(2000000)`.
5. Add the shared E2E precision oracle to `crates/lakehouse-engine/tests/common/`: read the running
   engine's version from the live session (source pinned by task 1), map it to an expected precision
   with the helper's OWN explicit table, and expose the expected declared type string. It MUST NOT call
   `TimestampPrecision::from_database_version` and MUST NOT read `EXASOL_IMAGE`.
6. Add `seed_timestamp_precision_probe` to `crates/lakehouse-engine/tests/common/seed.rs`: its own
   Iceberg namespace and table with `id` (`long`), a `timestamp` column, and a `timestamptz` column,
   each holding `2024-01-01 00:00:00.000001`, `.000002`, `.123456`, `.123457` as
   `TimestampMicrosecondArray` values, following the existing `seed_non_ascii_identifier` own-namespace
   pattern and `typed_probe`'s batch construction.
7. Add `crates/lakehouse-engine/tests/e2e_timestamp_precision_test.rs`: its own VS over the new
   namespace; assert the exact declared `COLUMN_TYPE` for both columns, the full rendered fractional
   digits, and `COUNT(DISTINCT)` per column, branching on the task-5 oracle (4 distinct and six digits
   at precision 6; 2 distinct and the seeded millisecond prefix at precision 3); prove the query reaches
   the scan script via `EXPLAIN VIRTUAL`. Add the binary to the `test-e2e` make target. [expert]
8. Repair `e2e_upper_timestamp_declines_to_native_oracle` in
   `crates/lakehouse-engine/tests/e2e_capability_test.rs`: its oracle
   `UPPER(CAST(TIMESTAMP '2024-01-01 00:00:00.100' AS TIMESTAMP))` renders three fractional digits while
   the VS side now renders six, so the CAST target must carry the declared type the task-5 oracle
   reports.
9. Add the exact, version-aware Delta declared-type assertion to
   `crates/lakehouse-engine/tests/e2e_unity_test.rs` for `TIMESTAMP_COL`, `TIMESTAMP_NTZ_COL`, and
   `DATE_TIMESTAMP_NTZ`, reading the task-5 oracle. Leave the existing prefix-tolerant expectations in
   place.
10. Matrix the core `e2e` job in `.github/workflows/ci.yml` over two images. Matrix entries carry the
    image, the status-check name, and the failure-log artifact name; the `2025.x` leg's check name stays
    exactly `E2E`; the job env gains `EXASOL_IMAGE: ${{ matrix.image }}`; the `Upload Exasol logs` step
    uses the per-leg artifact name. `release`'s `needs: [e2e, …]` is unchanged and waits for both legs.
    `e2e-lakekeeper`, `e2e-unity`, and `e2e-azure` stay single-version. No `docker-compose.yml` or
    Makefile change.
11. Run the full local E2E suite against both images and confirm green on each, then reconcile any
    remaining precision-sensitive assertion the run surfaces against this plan's Scenario Coverage
    table rather than by loosening it.
12. Update the `Timestamp(_, _)` row of `CLAUDE.md`'s Data types mapping table (currently
    `| Timestamp(_, _) | TIMESTAMP |`) to the version-gated summary: `TIMESTAMP(6)` on Exasol 2025.x and
    later or an unrecognized version, bare `TIMESTAMP` (millisecond) on 8.x, and a pointer to the
    `datafusion-scan/type-mapping` spec for the exact rule. One table row — add no prose section and do
    not restate the parsing or fallback logic.

## Parallelization

| Parallel Group | Tasks |
|----------------|-------|
| Group A | 1 |
| Group B | 2, 6, 10 |
| Group C | 3, 4, 5, 12 |
| Group D | 7, 8, 9 |
| Group E | 11 |

Sequential dependencies:
- Group A → Group B (task 1's captures pin task 5's version source and confirm the declaration is
  accepted at all; nothing else may land before it)
- Group B → Group C (task 3 threads the task-2 type; task 5 needs task 1's pinned version source; task 12
  documents the task-2 rule and needs its threshold and precision names final — it depends on nothing else)
- Group C → Group D (every E2E assertion needs the declaration change and the task-5 oracle in place)
- Group D → Group E

## Dead Code Removal

| Type | Location | Reason |
|------|----------|--------|
| None | — | Nothing becomes obsolete under this plan |

`arrow_to_exasol_type` (`crates/lakehouse-engine/src/types/mapping.rs`) is pre-existing dead code —
`datafusion-scan/type-mapping-module-structure` already records that it has no call site in the crate —
and is deliberately kept. It is the recorded, unit-tested statement of the Arrow-input mapping table;
deleting it would remove that coverage for a cleanup issue #359 does not ask for. Its exclusion from the
version gate is recorded as a scenario so the omission cannot read as an oversight.

## Verification

### Scenario Coverage

| Scenario | Test Type | Test Location | Test Name |
|----------|-----------|---------------|-----------|
| A catalog timestamp column is declared TIMESTAMP(6) on Exasol 2025.x and later | Unit | `crates/lakehouse-engine/src/types/mapping_tests.rs` | `timestamp_declaration_is_version_gated_for_both_catalog_kinds` |
| An empty or unparseable database version declares the microsecond precision | Unit | `crates/lakehouse-engine/src/types/mapping_tests.rs` | `unreadable_database_version_declares_microsecond_precision` |
| The Arrow-input type resolver stays outside the version gate | Unit | `crates/lakehouse-engine/src/types/mapping_tests.rs` | `arrow_input_resolver_stays_outside_the_timestamp_version_gate` |
| Iceberg timestamptz maps to plain Exasol TIMESTAMP | Unit | `crates/lakehouse-engine/src/types/mapping_tests.rs` | `iceberg_timestamptz_declares_timestamp_at_the_gated_precision` |
| createVirtualSchema reads the database version once and threads the resolved precision | Integration | `crates/lakehouse-engine/tests/e2e_timestamp_precision_test.rs` | `iceberg_microsecond_timestamps_round_trip_at_the_declared_precision` |
| createVirtualSchema reads the database version once and threads the resolved precision (threading half) | Unit | `crates/lakehouse-engine/src/adapter/catalog_client_tests.rs` | `build_listing_virtual_tables_declares_timestamp_at_the_given_precision` |
| A TIMESTAMP(p) column declaration serializes fractionalSecondsPrecision | Unit | `crates/lakehouse-engine/src/types/mapping_tests.rs` | `exasol_type_to_json_renders_timestamp_fractional_seconds_precision` |
| Unity Catalog Spark column types map to Exasol types sufficient for listing | Unit | `crates/lakehouse-engine/src/types/mapping_tests.rs` | `unity_timestamp_names_declare_the_gated_precision` |
| Every Delta type Exasol represents natively maps to its own Arrow tag | Integration | `crates/lakehouse-engine/tests/e2e_unity_test.rs` | `unity_delta_timestamp_columns_declare_the_exact_gated_precision` |
| Microsecond-distinct Iceberg timestamps round-trip at the declared precision | Integration | `crates/lakehouse-engine/tests/e2e_timestamp_precision_test.rs` | `iceberg_microsecond_timestamps_round_trip_at_the_declared_precision` |
| The E2E suite gates on both supported Exasol major versions | Integration | `.github/workflows/ci.yml` | the `e2e` job's two matrix legs (`E2E`, `E2E (8.29.13)`), each running `make test-e2e` in full |
| A VS timestamp compared as a rendered string uses a precision-matched oracle | Integration | `crates/lakehouse-engine/tests/e2e_capability_test.rs` | `e2e_upper_timestamp_declines_to_native_oracle` |
| A Delta timestamp column's declared Exasol type is asserted exactly at the engine's precision | Integration | `crates/lakehouse-engine/tests/e2e_unity_test.rs` | `unity_delta_timestamp_columns_declare_the_exact_gated_precision` |

The four `mapping_tests.rs` scenarios are unit-tested because each is pure computation over a string or
a type descriptor with no I/O; every one of them is *also* exercised end to end by the round-trip
integration test, which is what proves the declaration reaches Exasol. The two Delta scenarios share one
test because they assert the same observable — the declared `COLUMN_TYPE` of the same three columns.

### Manual Testing

| Feature | Command | Expected Output |
|---------|---------|-----------------|
| datafusion-scan/type-mapping | `EXASOL_IMAGE=exasol/docker-db:2025.2.1 make test-e2e` then `SELECT COLUMN_NAME, COLUMN_TYPE FROM SYS.EXA_ALL_COLUMNS WHERE COLUMN_SCHEMA='<vs>' AND COLUMN_NAME LIKE '%TS%';` | Every Iceberg timestamp column reports `TIMESTAMP(6)` |
| vs-adapter/create-virtual-schema | `SELECT ADAPTER_NOTES FROM SYS.EXA_ALL_VIRTUAL_SCHEMAS WHERE VIRTUAL_SCHEMA_NAME='<vs>';` after `ALTER VIRTUAL SCHEMA <vs> REFRESH` | Refresh succeeds; `adapterNotes` carries no precision entry (the value is re-derived per request, never persisted) |
| vs-adapter/unity-catalog-create-virtual-schema | `SELECT COLUMN_NAME, COLUMN_TYPE FROM SYS.EXA_ALL_COLUMNS WHERE COLUMN_SCHEMA='<unity_vs>' AND COLUMN_TABLE='STATS_ALL_TYPES';` | `TIMESTAMP_COL` and `TIMESTAMP_NTZ_COL` both report `TIMESTAMP(6)` |
| vs-adapter/delta-type-mapping | `SELECT TIMESTAMP_COL, TIMESTAMP_NTZ_COL FROM <unity_vs>.STATS_ALL_TYPES;` | Values render six fractional digits, unchanged in magnitude |
| e2e-harness/e2e-harness | `EXASOL_IMAGE=exasol/docker-db:8.29.13 make test-e2e` | Suite passes; the precision test asserts the millisecond arm and the declared type is bare `TIMESTAMP` |
| e2e-harness/unity-catalog-e2e-harness-delta-queries | `make test-e2e-unity` | Passes, including the new exact declared-type assertion |

### Checklist

| Step | Command | Expected |
|------|---------|----------|
| Build | `make cross-musl-udf-build` | Exit 0 |
| Test | `cargo test` | 0 failures |
| E2E (2025.x) | `EXASOL_IMAGE=exasol/docker-db:2025.2.1 make test-e2e` | 0 failures |
| E2E (8.29.x) | `EXASOL_IMAGE=exasol/docker-db:8.29.13 make test-e2e` | 0 failures |
| E2E (Unity/Delta) | `make test-e2e-unity` | 0 failures |
| Lint | `cargo clippy --all-targets` | 0 errors/warnings |
| Format | `cargo fmt` | No changes |
</content>
