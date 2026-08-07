# Feature: Lakekeeper E2E Harness (OIDC + MinIO)

End-to-end test suite that verifies the lakehouse VS query path against a
Lakekeeper Iceberg REST catalog — the open-source, OpenID-secured, multi-warehouse
Rust catalog — backed by MinIO object storage, proving real interoperability rather
than a connectivity smoke test. The suite authenticates to the catalog with the
engine's existing OAuth2 client-credentials CONNECTION fields (an external Keycloak
IdP issues the token), resolves tables through Lakekeeper's per-warehouse
`overrides.prefix`, and reads data files under both static S3 credentials and
Lakekeeper's default vended (STS) credentials. It is additive: the existing
unauthenticated `exasol-e2e` baseline is unchanged, and this suite runs behind its
own `lakekeeper-e2e` cargo feature.

## Background

* Every scenario runs against a local Docker stack of Exasol, MinIO, Lakekeeper, a
  PostgreSQL metadata database, and a Keycloak IdP, and MUST fail (never skip) when
  the stack is unavailable — the same fail-loud discipline as `e2e-harness/e2e-harness`,
  and the opposite of `e2e-harness/cloud-e2e-harness`.
* The suite is gated behind a dedicated `lakekeeper-e2e` cargo feature, distinct from
  `exasol-e2e` and `cloud-e2e`, so the fast unauthenticated baseline suite is never
  altered.
* Lakekeeper serves the Iceberg REST API under a base-path prefix (`/catalog`) and is
  multi-warehouse: the CONNECTION `warehouse` field carries a Lakekeeper
  warehouse-name (not an S3 URI), which the catalog resolves to a per-warehouse
  `overrides.prefix` via `GET /v1/config?warehouse=<name>`.
* Catalog authentication uses the engine's existing OAuth2 client-credentials fields
  (`client_id`, `client_secret`, `oauth2_server_uri`, `scope`) per
  `vs-adapter/rest-catalog-oauth-auth`; no new CONNECTION field is introduced.
  Keycloak is pre-seeded from a realm-export JSON with a confidential client granting
  the client-credentials flow and an audience mapper whose `aud` claim matches
  Lakekeeper's `LAKEKEEPER__OPENID_AUDIENCE`.
* Lakekeeper requires a one-time bootstrap and runtime warehouse creation through its
  management API (`POST /management/v1/bootstrap`, `POST /management/v1/warehouse`),
  both authenticated with a Keycloak-issued bearer token. The harness performs these
  steps in-process, mirroring how the baseline harness performs SLC install and VS
  creation in Rust.
* The `sts-enabled` warehouse requires MinIO's STS AssumeRole endpoint enabled and an
  IAM policy/role Lakekeeper assumes to vend short-lived credentials scoped to the
  warehouse bucket. The stack configures this deterministically, so the
  vended-credential path is a hard pass/fail requirement, not best-effort. Both the
  static-credential and vended-credential scans MUST pass; neither is skipped or treated
  as optional.
* All DSN/connection strings include `validateservercertificate=0`. No credential
  value (client secret, bearer token, static or vended S3 key) appears in test output.
* The scan-path provisioning (SLC install, `.so` upload, script DDL, VS creation)
  reuses the shared `common/e2e_harness` definition per `e2e-harness/e2e-harness`;
  only the CONNECTION password, warehouse-name, and namespace vary per binary.
* VS properties use docker-network-internal URLs (catalog `http://lakekeeper:8181/catalog`,
  MinIO `http://minio:9000`, token endpoint on the Keycloak service) because the
  adapter UDF runs inside the Exasol container.
* **This delta promotes an existing assertion from a stronger-than-necessary proof to the required shape, and changes no fixture, no warehouse, and no query.** It implements issue #276, slice D of six (A-F). `vs-adapter/pushdown-planning-cloud-credentials` now derives the effective scan storage SOLELY from the `loadTable` response when `use_vended_credentials` is true.
* **This suite is the characterization gate that makes the strict rule safe, and it needs no behavioural change to be one.** The vended CONNECTION already supplies an empty `endpoint`, `region`, `access_key`, and `secret_key` and a false `path_style`, so there was never a static value for the shipped preservation rule to backfill and the strict rule is a NO-OP for this path. That is the evidence the rule is compatible with a live vended stack rather than only with unit fixtures.
* **Lakekeeper's live vended config supplies the store address, live-verified.** It carries `s3.endpoint` (`http://minio:9000/`) and `s3.path-style-access` (`true`), so this path satisfies the strict rule's "a vended payload must name a region or an endpoint" requirement through the endpoint and needs no vended `client.region`.
* **`ALLOW_HTTP` stays the operator's consent gate for the vended plain-HTTP endpoint, and this suite already sets it.** The harness emits `ALLOW_HTTP = 'true'` (`crates/lakehouse-engine/tests/common/e2e_harness.rs:270`) and Lakekeeper vends a plain-`http://` MinIO endpoint, so the vended endpoint is honoured and the scan reaches MinIO. Deriving the permission from the vended endpoint's scheme instead was rejected as a security regression: it would let a catalog downgrade the transport with no operator control (see `vs-adapter/pushdown-planning-cloud-credentials`). This suite is consequently the positive case for the consent gate — vended plain-HTTP endpoint plus `ALLOW_HTTP = 'true'` reads successfully.
* **This delta adds the two-table vended broadcast-join coverage issue #294 needs, in ONE warehouse.** Two warehouses would be untestable for a join: two warehouses mean two virtual schemas and two adapters, and Exasol never hands either adapter a join to push down. The fixture is therefore two tables in the `lakehouse_vended` warehouse (`sts-enabled: true`), whose per-table vended credentials the adapter resolves independently.
* **The suite currently seeds ONE table (`events`) per warehouse, so a second table is the fixture work.** `seed_star_schema` is the existing purpose-built broadcast-join fixture — `dim_customer` (5 rows, 1 file) and `fact_orders` (10 rows, 2 files), with deliberately disjoint `C_*` / `O_*` column prefixes so the adapter's disjoint-column guard admits bare-name broadcast rendering. It is unusable against Lakekeeper only because it builds its catalog through the UNAUTHENTICATED seed wrapper; an authenticated variant is the whole change. `events` and `labels` share an `id` column and would trip the disjoint-column guard, so they cannot substitute.
* **The vended MinIO user's own IAM policy is BUCKET-scoped, so the fixture needs no policy change.** `minio-lakekeeper-init` attaches a policy allowing `s3:GetObject`/`PutObject`/`DeleteObject`/`ListBucket`/`GetBucketLocation` on `arn:aws:s3:::warehouse` and `arn:aws:s3:::warehouse/*`. Both warehouses are rooted in that one bucket and separated by a per-warehouse `key-prefix`, so a second table under `lakehouse_vended` is already covered.
* **Whether this fixture can reproduce the #294 DEFECT — as opposed to proving the FIX carries per-side credentials — is an open empirical question this suite answers, not an assumption.** The two sides' vended credential VALUES already differ today, because `resolve_vended_storage` runs per side and each call mints its own STS session. Value divergence is enough to test the carriage fix. It is NOT enough to reproduce the defect: if both sessions grant whole-bucket access, reading the dimension side through the fact side's credential simply succeeds. A failing pre-fix repro requires the two sessions' SCOPE to diverge, so the fact side's credential is genuinely DENIED on the dimension side's prefix.
* **The default broadcast threshold already makes this join broadcast-eligible.** Both virtual schemas are created without `JOIN_BROADCAST_MAX_BYTES`, so both run at the 128 MiB default, and `dim_customer`'s single small file is far below it — it becomes the dimension side with no per-test configuration.
* **The shared test harness declares no row cap by default, and this suite's connection must stay uncapped — not because a cap is inert, but because it is not.** The shared WebSocket test client (`crates/lakehouse-engine/tests/common/exasol_ws.rs`) sends Exasol's own documented default — `0`, no limit — unless a call site declares a cap through `ExaConn::capped_result_sets(n)`. A declared `resultSetMaxRows` cap DOES reach the adapter as a pushdown `limit` on a real query execution, confirmed by directly capturing the adapter's incoming request (bypassing `EXPLAIN VIRTUAL`, which is a separate exchange that cannot observe this) across all seven statement shapes measured, including the broadcast-eligible inner equi-join. For a join, ANY pushed `limit` disqualifies broadcast pushdown via `join_requires_exasol_postprocessing` and falls back to the unaccelerated two-scan (`LHS_T0`/`LHS_T1`) wrapper. This suite's own connection never calls `capped_result_sets`, so its broadcast join test is unaffected in practice — but that is because the connection stays uncapped by choice, not because the mechanism doesn't exist. See `docs/debugging-pushdown.md`'s measured shape matrix for the full comparison, including the broadcast-join row. Verifying the broadcast path at row-fetch time and not only at `EXPLAIN VIRTUAL` time is still valuable as a genuine end-to-end check — it confirms the joined rows actually come back through the broadcast plan, not merely that the plan was selected.

## Scenarios

### Scenario: Harness bootstraps Lakekeeper and creates the MinIO-backed warehouses

* *GIVEN* a running stack with Lakekeeper, its PostgreSQL metadata database, MinIO, and Keycloak healthy
* *AND* MinIO configured with its STS AssumeRole endpoint enabled and an IAM policy/role granting read access to the warehouse bucket, so Lakekeeper can vend short-lived credentials
* *WHEN* the harness provisions the catalog before any query
* *THEN* the harness SHALL obtain a bearer token from Keycloak via the OAuth2 client-credentials grant and authenticate every management-API request with it
* *AND* the harness SHALL `POST /management/v1/bootstrap` once and then `POST /management/v1/warehouse` for each test warehouse, each with an `s3` storage profile whose `flavor` is `s3-compat`, `endpoint` is the internal MinIO URL, `path-style-access` is true, and `region`/`bucket` match the MinIO stack
* *AND* the harness SHALL create one warehouse with S3 access delegation disabled (`sts-enabled` and `remote-signing-enabled` false) for the static-credential path and one warehouse with `sts-enabled` true — referencing the MinIO STS role — for the vended-credential path
* *AND* no client secret or bearer token value SHALL appear in test output

### Scenario: createVirtualSchema enumerates Lakekeeper tables over OAuth2 client-credentials auth

* *GIVEN* a seeded Iceberg table in a Lakekeeper warehouse and an Exasol CONNECTION whose address is `http://lakekeeper:8181/catalog`, whose `warehouse` is the Lakekeeper warehouse-name, and whose JSON password supplies `client_id`, `client_secret`, `oauth2_server_uri` (the Keycloak token endpoint), and `scope`
* *WHEN* the harness issues `CREATE VIRTUAL SCHEMA` against the Lakekeeper-backed CONNECTION
* *THEN* the adapter SHALL authenticate to Lakekeeper with a token obtained from the OAuth2 client-credentials grant and enumerate every table in the configured namespace
* *AND* the created virtual schema SHALL expose the seeded table with its column schema mapped to Exasol types
* *AND* the `client_secret` and bearer token MUST NOT appear in any returned SQL string, error message, or test output

### Scenario: End-to-end scan over a static-credential Lakekeeper warehouse returns correct rows

* *GIVEN* a virtual schema over a seeded Iceberg table in the delegation-disabled Lakekeeper warehouse, whose CONNECTION supplies OAuth2 catalog auth and static MinIO `access_key`/`secret_key` with `use_vended_credentials` false
* *WHEN* a user runs `SELECT <subset of columns> FROM <vs>.<table> WHERE <predicate> LIMIT <n>`
* *THEN* the query SHALL return exactly the rows that satisfy the predicate, capped at `n`, projected to the selected columns
* *AND* the returned values SHALL match the seeded source data
* *AND* the test MUST fail (not skip) when the Docker stack is unavailable

### Scenario: End-to-end scan over a vended-credential Lakekeeper warehouse returns correct rows

* *GIVEN* a virtual schema over a seeded Iceberg table in the `sts-enabled` Lakekeeper warehouse, whose CONNECTION supplies OAuth2 catalog auth, sets `use_vended_credentials` true, and supplies NO static S3 storage field
* *WHEN* a user runs a projection + filter query through the virtual schema
* *THEN* the adapter SHALL send the `X-Iceberg-Access-Delegation: vended-credentials` header on the `loadTable` request, extract the short-lived vended S3 credentials Lakekeeper returns, and carry them into every per-shard scan spec
* *AND* the adapter SHALL take the store's endpoint and path-style flag from that same vended response, so the scan reaches MinIO without reading any CONNECTION storage field
* *AND* the adapter SHALL honour that vended plain-`http://` endpoint because the harness sets `ALLOW_HTTP = 'true'`, so this scenario is the positive case for the operator-consent gate on plaintext transport
* *AND* the scan SHALL read the MinIO data files using the vended credentials and return rows identical to the same query run over the static-credential warehouse
* *AND* the test SHALL assert the vended CONNECTION carries an empty `access_key`, `secret_key`, and `endpoint`, because under the strict vended rule that empty shape is the REQUIRED shape rather than merely a delegation proof — a passing scan from it is only reachable through the vended response
* *AND* no vended or static credential value SHALL appear in any returned SQL string or test output
* *AND* the test MUST fail (not skip) when the Docker stack is unavailable

### Scenario: Lakekeeper suite fails when the stack is unavailable

* *GIVEN* the Lakekeeper, Keycloak, or Exasol service is not reachable
* *WHEN* the `lakekeeper-e2e` suite runs
* *THEN* the suite SHALL fail
* *AND* the suite MUST NOT report the affected tests as skipped or passed

### Scenario: Lakekeeper binary provisions the scan path from the shared harness definition

* *GIVEN* the `lakekeeper-e2e` test binary under `crates/lakehouse-engine/tests`
* *AND* the shared `common/e2e_harness` module defining the SLC install, the `.so` upload, and the script creation
* *WHEN* the binary's setup provisions the lakehouse VS scan path
* *THEN* the binary SHALL install `LAKEHOUSE_SCAN`, `LAKEHOUSE_DISTRIBUTE_FILES`, and the adapter script from that shared definition, so the script DDL is byte-identical to every other E2E binary
* *AND* the Lakekeeper-specific CONNECTION password (OAuth2 client-credentials plus warehouse-name and MinIO endpoint), the warehouse-name, and the namespace SHALL be supplied as explicit parameters rather than by re-declaring the provisioning logic
* *AND* an end-to-end query through the Lakekeeper virtual schema SHALL return results identical to the single-node DataFusion equivalent

### Scenario: A two-table broadcast join over a vended-credential warehouse returns correct rows

* *GIVEN* the `sts-enabled` Lakekeeper warehouse seeded with BOTH star-schema tables through the OIDC-secured catalog, and one virtual schema over that warehouse's namespace whose CONNECTION supplies OAuth2 catalog auth, sets `use_vended_credentials` true, and supplies NO static S3 storage field
* *AND* a harness connection that declares no row cap, which is the harness default and therefore requires no opt-out call at the call site
* *WHEN* a user runs an inner equi-join of the two tables through that one virtual schema
* *THEN* the adapter SHALL plan a broadcast fan-out, so the pushed SQL carries the compact scan-spec join block and NOT the two-scan `LHS_T0` / `LHS_T1` unaccelerated wrapper
* *AND* that broadcast fan-out SHALL hold when the joined rows are fetched, not only when the plan is inspected through `EXPLAIN VIRTUAL`, because row-fetch-time verification is the only check that confirms the broadcast plan was actually executed rather than merely selected
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
