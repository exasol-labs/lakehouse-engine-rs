# Feature: E2E Harness

Provisions one shared scan path for every E2E test binary and gates the suite on a live Exasol stack. This delta changes no recorded description. The paragraph here is plan-local context only.

## Background

* This delta amends exactly ONE scenario, "Every E2E binary provisions the scan path from one shared
  harness definition", and is issue #407. It adds no harness module, no Make target, no Docker
  service, and no environment variable.
* The varying per-binary parameter list gains the properties the direct-storage catalog kind reads.
  A binary that creates a virtual schema over a raw Parquet directory supplies a `CATALOG_KIND`.
  Such a binary optionally supplies a `NAMESPACE` naming a storage subtree instead of a catalog
  namespace. It optionally supplies a `MERGE_SCHEMA`. Each is a parameter of the one shared
  definition, exactly as the Iceberg namespace and the catalog CONNECTION name already are.
* The scripts, the grants, and the `.so` upload are UNCHANGED. The direct-storage binary therefore
  provisions the same scan path as every other binary. The script DDL stays byte-identical
  across binaries.
* The fail-never-skip rule is unchanged and binds the new binary identically.
* `e2e-harness/direct-storage-e2e` owns what the new binary asserts. This feature owns only that the
  binary provisions through the shared definition rather than re-declaring it.

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: Every E2E binary provisions the scan path from one shared harness definition

* *GIVEN* the `exasol-e2e` test binaries under `crates/lakehouse-engine/tests`, each with its own `OnceLock`-guarded setup
* *AND* a single shared `common/e2e_harness` module defining the SLC install, the `.so` upload, the script creation, and the Virtual Schema creation
* *WHEN* any binary's setup provisions the lakehouse VS scan path
* *THEN* the binary SHALL install `LAKEHOUSE_SCAN`, `LAKEHOUSE_DISTRIBUTE_FILES`, and the adapter script from that shared definition, so the script DDL is byte-identical across every binary
* *AND* the shared definition SHALL issue `GRANT ACCESS ON CONNECTION ... FOR SCRIPT` for both scripts to `CURRENT_USER` after the last `CREATE OR REPLACE` of either object (both drop the grant), skipping `SYS` (Exasol refuses it; DBA holds all CONNECTIONs implicitly)
* *AND* the per-binary Virtual Schema properties that vary (VS name, the namespace property, catalog CONNECTION name, `PARALLELISM_FACTOR`, `JOIN_BROADCAST_MAX_BYTES`, `CATALOG_KIND`, and `MERGE_SCHEMA`) SHALL be supplied as explicit parameters rather than by re-declaring the provisioning logic, SUPERSEDING the recorded list, which named an Iceberg namespace and omitted the two catalog-kind-dependent properties
* *AND* a parameter a binary does not supply SHALL be OMITTED from the generated `CREATE VIRTUAL SCHEMA` rather than emitted empty, so an Iceberg binary's DDL is byte-identical to the one it generates today and no existing binary's assertions change
* *AND* an end-to-end query through any binary's Virtual Schema SHALL return results identical to the single-node DataFusion equivalent, and the affected tests MUST fail (not skip) when the Exasol Docker container or MinIO is unavailable
<!-- /DELTA:CHANGED -->
