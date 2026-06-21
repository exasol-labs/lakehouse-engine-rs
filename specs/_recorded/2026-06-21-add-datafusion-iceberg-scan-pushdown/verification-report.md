# Verification Report: add-datafusion-iceberg-scan-pushdown

**Generated:** 2026-06-21

## Verdict

| Result | Details |
|--------|---------|
| **PASS** | DataFusion-in-UDF Iceberg scan with projection/filter/LIMIT pushdown works end-to-end against a live Docker stack (Exasol 2025.2.1 + MinIO + Iceberg REST). Proven reproducible from a clean `docker compose down -v && up` with **no manual container patching**. |

| Check | Status |
|-------|--------|
| Build (`make cross-musl-udf-build`) | ✓ exit 0, `liblakehouse_vs.so` (164 MB) in `rust:1.92-bookworm` |
| Tests (host unit) | ✓ 39 passed / 0 failed |
| Tests (E2E live Docker) | ✓ 9 passed / 0 failed |
| Lint (`cargo clippy --all-targets --features exasol-e2e`) | ✓ 0 errors (5 pre-existing `tests/common/` style nits only) |
| Format (`cargo fmt --check`) | ✓ clean |
| Scenario Coverage | ✓ all plan scenarios covered by a passing test or manual step |
| Manual Tests | ✓ all 5 manual checks pass |

## Two-entry-point packaging (single `.so`)

```
$ nm -D target/release/liblakehouse_vs.so | grep __exa_udf_entry_
0000000001f25e00 T __exa_udf_entry_LAKEHOUSE_SCAN
0000000001f25e10 T __exa_udf_entry_LAKEHOUSE_VS_ADAPTER
```
One crate → one `.so` → both the VS adapter and the DataFusion scan SET UDF (language-container-rs 0.14.0 multi-entry capability), uploaded once to BucketFS and referenced by both `CREATE SCRIPT` statements.

## Test Results

| Type | Run | Passed | Failed |
|------|-----|--------|--------|
| Unit (host, debug) | 39 | 39 | 0 |
| Integration (E2E, `--features exasol-e2e`, `--test-threads=1`) | 9 | 9 | 0 |

E2E suite wall time: 142.8 s.

### Manual Tests

| Feature | Command | Result |
|---------|---------|--------|
| single-so-two-entry-points | `make cross-musl-udf-build && nm -D …liblakehouse_vs.so \| grep __exa_udf_entry_` | ✓ two symbols |
| e2e-harness | `docker compose down -v && up` → `make install-slc` → `make test-e2e` | ✓ stack starts clean; SLC 0.14.0 registers; 9/9 pass |
| create-virtual-schema | `CREATE VIRTUAL SCHEMA` over the seeded Iceberg table | ✓ columns mapped to Exasol types |
| pushdown-planning + datafusion-scan | `SELECT id, name, score FROM MY_LAKEHOUSE.EVENTS WHERE score > 15.0 LIMIT 5` | ✓ (see gate evidence) |
| type-mapping | mixed-column round-trip (int/string/double/date/timestamp) | ✓ all five Exasol types correct |

### Gate query evidence (through the VS, live)

```
SELECT id, name, score FROM MY_LAKEHOUSE.EVENTS WHERE score > 15.0 LIMIT 5
ID,NAME,SCORE
4,event-04,20.0
5,event-05,25.0
6,event-06,30.0
7,event-07,35.0
8,event-08,40.0
```
Projection (3 of 5 cols), filter (`score > 15.0`, 17 of 20 rows qualify), and LIMIT (5) all pushed into the DataFusion scan. Full-type round-trip for id=1: `1, event-01, 5.0, 2024-01-01, 2024-01-01T00:00:00` → DECIMAL / VARCHAR / DOUBLE / DATE / TIMESTAMP.

## Scenario Coverage

| Feature | Scenario | Test | Passes |
|---------|----------|------|--------|
| create-virtual-schema | reports pushdown capabilities | `capabilities.rs::reports_projection_filter_limit_only` | ✓ |
| create-virtual-schema | maps Iceberg schema | `e2e_scan_test::create_vs_maps_iceberg_schema` | ✓ |
| create-virtual-schema | unreachable catalog errors, no secret | `e2e_scan_test::create_vs_unreachable_catalog_errors_no_secret` | ✓ |
| pushdown-planning | resolves files once, builds scan SQL | `pushdown.rs::pushdown_resolves_files_once_builds_scan_sql` | ✓ |
| pushdown-planning | projection pushed | `pushdown.rs::pushdown_carries_projection` | ✓ |
| pushdown-planning | filter translated or omitted | `pushdown.rs::pushdown_translates_or_omits_predicate` | ✓ |
| pushdown-planning | LIMIT pushed | `pushdown.rs::pushdown_carries_limit` | ✓ |
| scan-execution | registers only assigned files | `e2e_scan_test::scan_registers_only_assigned_files` / gate | ✓ |
| scan-execution | filter restricts rows | `e2e_scan_test::scan_filter_restricts_rows` | ✓ |
| scan-execution | LIMIT caps rows | `e2e_scan_test::scan_limit_caps_rows` | ✓ |
| scan-execution | batch-by-batch incremental emit | `emit.rs::emits_batch_by_batch_without_materializing` | ✓ |
| scan-execution | Arrow → Value variants | `convert.rs::arrow_columns_map_to_value_variants` | ✓ |
| scan-execution | incompatible cols → JSON strings | `convert.rs::incompatible_columns_emit_json_strings` | ✓ |
| scan-execution | unreadable file errors, no secret | `e2e_scan_test::scan_unreadable_file_errors_no_secret` | ✓ |
| type-mapping | compatible types | `mapping.rs::compatible_types_map_to_exasol_type` | ✓ |
| type-mapping | in-range Decimal128 | `mapping.rs::decimal128_in_range_maps_to_decimal` | ✓ |
| type-mapping | out-of-range Decimal128 → VARCHAR/JSON | `mapping.rs::decimal128_out_of_range_maps_to_varchar_json` | ✓ |
| type-mapping | incompatible types → VARCHAR/JSON | `mapping.rs::incompatible_types_map_to_varchar_json` | ✓ |
| type-mapping | mixed-column round-trip | `e2e_scan_test::mixed_column_parquet_round_trips` | ✓ |
| single-so-two-entry-points | both entry symbols exported | `two_entry_points_test::so_exports_both_entry_symbols` | ✓ |
| single-so-two-entry-points | both scripts resolve one artifact | `e2e_scan_test::both_scripts_resolve_one_artifact` | ✓ |
| single-so-two-entry-points | host release build documented unloadable | `build_convention::host_release_build_documented_unloadable` | ✓ |
| e2e-harness | projection+filter+LIMIT returns correct rows | `e2e_scan_test::e2e_projection_filter_limit_returns_correct_rows` | ✓ |
| e2e-harness | suite fails when stack unavailable | `e2e_scan_test::e2e_fails_when_stack_unavailable` | ✓ |

Complex list/struct **column seeding** was deferred (iceberg-rust 0.9.1 has no struct/list Parquet writer); the incompatible-type → VARCHAR(2000000)/JSON path is fully covered by host unit tests (`convert.rs`, `mapping.rs`) instead of a seeded E2E column.

## Notes & accepted PoC tradeoffs

- **Durable DNS for the UDF** — Exasol's exaconf regenerates `/etc/hosts` and `/etc/resolv.conf` (→8.8.8.8) at boot, wiping Docker's `extra_hosts` entries, so docker-network service names don't resolve inside the UDF. Fixed durably via static IPs + an entrypoint-wrapper watcher in `docker-compose.yml` that re-adds the `minio`/`iceberg-rest` host entries after exaconf runs. This is why the first green run needed a manual patch; it no longer does. `// ponytail:` ceiling: a watcher loop, not a config knob — acceptable for a PoC compose.
- **Credentials in scan SQL (accepted PoC risk)** — S3 access/secret keys are embedded in the scan-driving SQL literal (inside the ScanSpec JSON) the adapter returns; Exasol may log/profile that SQL. Documented at `adapter/pushdown.rs` with the upgrade path (reference an Exasol CONNECTION object by name, or fetch via connect-back at scan time). Error paths now redact the literal secret values defensively. Follow-up for a hardening plan.
- **Two-Arrow-tree split** — datafusion/SDK link arrow 58; iceberg-rust 0.9.1 links arrow/parquet 57 internally. Intentional and safe because only SDK `Value` crosses the `.so` boundary; the test seed constructs iceberg-facing Arrow/Parquet objects with v57 types via aliased dev-deps.

## Conclusion

Working software, verified against live Docker on this machine and reproducible from a clean stack. Ready for `/speq:record`.
