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
