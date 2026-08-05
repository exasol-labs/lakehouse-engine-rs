# Feature: Lakekeeper E2E Harness (OIDC + MinIO)

End-to-end test suite that verifies the lakehouse VS query path against a
Lakekeeper Iceberg REST catalog — the open-source, OpenID-secured, multi-warehouse
Rust catalog — backed by MinIO object storage, proving real interoperability rather
than a connectivity smoke test. The suite authenticates to the catalog with the
engine's existing OAuth2 client-credentials CONNECTION fields (an external Keycloak
IdP issues the token), resolves tables through Lakekeeper's per-warehouse
`overrides.prefix`, and reads data files under both static S3 credentials and
Lakekeeper's default vended (STS) credentials.

## Background

<!-- DELTA:NEW -->
* **This delta adds the two-table vended broadcast-join coverage issue #294 needs, in ONE warehouse.** Two warehouses would be untestable for a join: two warehouses mean two virtual schemas and two adapters, and Exasol never hands either adapter a join to push down. The fixture is therefore two tables in the `lakehouse_vended` warehouse (`sts-enabled: true`), whose per-table vended credentials the adapter resolves independently.
* **The suite currently seeds ONE table (`events`) per warehouse, so a second table is the fixture work.** `seed_star_schema` is the existing purpose-built broadcast-join fixture — `dim_customer` (5 rows, 1 file) and `fact_orders` (10 rows, 2 files), with deliberately disjoint `C_*` / `O_*` column prefixes so the adapter's disjoint-column guard admits bare-name broadcast rendering. It is unusable against Lakekeeper only because it builds its catalog through the UNAUTHENTICATED seed wrapper; an authenticated variant is the whole change. `events` and `labels` share an `id` column and would trip the disjoint-column guard, so they cannot substitute.
* **The vended MinIO user's own IAM policy is BUCKET-scoped, so the fixture needs no policy change.** `minio-lakekeeper-init` attaches a policy allowing `s3:GetObject`/`PutObject`/`DeleteObject`/`ListBucket`/`GetBucketLocation` on `arn:aws:s3:::warehouse` and `arn:aws:s3:::warehouse/*`. Both warehouses are rooted in that one bucket and separated by a per-warehouse `key-prefix`, so a second table under `lakehouse_vended` is already covered.
* **Whether this fixture can reproduce the #294 DEFECT — as opposed to proving the FIX carries per-side credentials — is an open empirical question this suite answers, not an assumption.** The two sides' vended credential VALUES already differ today, because `resolve_vended_storage` runs per side and each call mints its own STS session. Value divergence is enough to test the carriage fix. It is NOT enough to reproduce the defect: if both sessions grant whole-bucket access, reading the dimension side through the fact side's credential simply succeeds. A failing pre-fix repro requires the two sessions' SCOPE to diverge, so the fact side's credential is genuinely DENIED on the dimension side's prefix. `plan.md` § Implementation Tasks makes establishing that the FIRST task and an explicit gate.
* **The default broadcast threshold already makes this join broadcast-eligible.** Both virtual schemas are created without `JOIN_BROADCAST_MAX_BYTES`, so both run at the 128 MiB default, and `dim_customer`'s single small file is far below it — it becomes the dimension side with no per-test configuration.
* **The shared test harness's hardcoded `resultSetMaxRows: 10000` on every `execute()` call is a test-harness artifact that suppresses broadcast eligibility, discovered while reproducing this scenario.** Exasol turns a session's `resultSetMaxRows` attribute into a `limit` on the pushdown request, and the adapter's `join_requires_exasol_postprocessing` routes any limit-carrying join to the unaccelerated fallback — never the broadcast path this scenario verifies — regardless of whether a real client or the adapter itself would otherwise choose broadcast. This is a property of the SHARED WEBSOCKET TEST CLIENT (`crates/lakehouse-engine/tests/common/exasol_ws.rs`), not of the adapter or the scan. The suite's dedicated connection for this scenario opts out via a SCOPED `ExaConn::unbounded_result_sets()` builder method, so the join genuinely reaches the broadcast path when its rows are fetched, not only when its plan is inspected via `EXPLAIN VIRTUAL` (which never carried the limit in the first place, so a shape-only check could pass while the row-fetch silently ran the fallback).
<!-- /DELTA:NEW -->

## Scenarios

<!-- DELTA:NEW -->
### Scenario: A two-table broadcast join over a vended-credential warehouse returns correct rows

* *GIVEN* the `sts-enabled` Lakekeeper warehouse seeded with BOTH star-schema tables through the OIDC-secured catalog, and one virtual schema over that warehouse's namespace whose CONNECTION supplies OAuth2 catalog auth, sets `use_vended_credentials` true, and supplies NO static S3 storage field
* *WHEN* a user runs an inner equi-join of the two tables through that one virtual schema
* *THEN* the adapter SHALL plan a broadcast fan-out, so the pushed SQL carries the compact scan-spec join block and NOT the two-scan `LHS_T0` / `LHS_T1` unaccelerated wrapper
* *AND* the adapter SHALL resolve a vended credential for EACH table independently, sending `X-Iceberg-Access-Delegation: vended-credentials` on each side's `loadTable` request
* *AND* the emitted scan spec SHALL carry the fact side's vended backend as its whole-spec `storage` value and the dimension side's vended backend inside the join block, so neither side's credential is discarded
* *AND* the joined rows SHALL equal the join computed independently from the two tables read un-joined through the same virtual schema, because a one-warehouse fixture has no second warehouse to cross-check against
* *AND* no vended credential value SHALL appear in any returned SQL string surfaced by the test or in any test output
* *AND* the test MUST fail (not skip) when the Docker stack is unavailable

### Scenario: The vended credential scope divergence the defect needs is established by observation

* *GIVEN* both star-schema tables seeded into the ONE `sts-enabled` Lakekeeper warehouse
* *WHEN* the suite requests each table's `loadTable` response with access delegation and inspects the credential the response vends for that table
* *THEN* the suite SHALL record, per table, the `prefix` of the `storage_credentials` entry the adapter selects for that table's location, so whether Lakekeeper scopes the vended entry to the table location or to the warehouse root is an observed fact rather than a documentation claim
* *AND* the suite SHALL attempt a read of ONE data file belonging to the OTHER table using the first table's vended credential, and SHALL record whether that read is denied
* *AND* a DENIED cross-table read SHALL be asserted as the property that makes the broadcast join's per-side credential carriage load-bearing rather than cosmetic
* *AND* an ALLOWED cross-table read SHALL fail the suite with a message stating that this fixture cannot reproduce the defect as a read error, so the gap is surfaced rather than concealed by a passing join test
* *AND* no vended credential value SHALL appear in any recorded output or failure message
<!-- /DELTA:NEW -->
