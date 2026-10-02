# Feature: Lakekeeper Authorization Contract (OpenFGA + batch-check)

An end-to-end suite that pins Lakekeeper's permission-check contract on its open-source OpenFGA
authorizer. It establishes which principal id a grant names, which privilege lets a caller check
another identity, where the management API sits relative to the catalog URI, and what `batch-check`
answers for an allowed, a denied, and a missing table. Its committed fixtures are the contract the
adapter's permission client (#415) builds on.

## Background

* The stack is `docker-compose.yml` plus `docker-compose.lakekeeper.yml` plus
  `docker-compose.lakekeeper.openfga.yml`. The OpenFGA overlay adds OpenFGA and switches the
  `lakekeeper` service to `LAKEKEEPER__AUTHZ_BACKEND=openfga`.
* The suite is gated behind the `lakekeeper-authz-e2e` cargo feature. It MUST fail, never skip,
  when its stack is unavailable, the same discipline as `lakekeeper-e2e/lakekeeper-e2e-harness`.
* The `lakehouse` client bootstraps Lakekeeper as server `operator` and writes every grant. The
  harness creates three more client-credentials clients: `lakehouse-reader-a`,
  `lakehouse-reader-b`, and `lakehouse-checker`.
* The fixture is warehouse `lakehouse_authz`, namespace `authz`, and the tables `authz_alpha` and
  `authz_beta`. `lakehouse-reader-a` holds `select` on
  `authz_alpha` only. `lakehouse-reader-b` holds `select` on `authz_beta` only. The id
  `oidc~template-user@corp` never logs in and holds `select` on `authz_alpha` only.
  `lakehouse-checker` holds no standing grant.
* A check is `POST /management/v1/action/batch-check` with the body
  `{"checks": [{"id", "identity": {"user": "<idp-id>~<subject>"}, "operation": {"table": {"action": {"action": "read_data"}, "namespace": ["authz"], "table", "warehouse-id"}}}], "error-on-not-found": false}`.
  A 200 response is `{"results": [{"id", "allowed"}]}`.
* A live response matches a fixture's shape when the HTTP status is equal, every JSON object has
  the same keys with the same JSON value types at every depth, and the `allowed`, `error.type`, and
  `error.code` values are equal. Message text and `Error ID` values are volatile and excluded.
* The fixtures live in `crates/lakehouse-catalog/tests/fixtures/lakekeeper/batch-check/`. Each
  file holds the calling client id, the request, the status, and the response, with warehouse,
  table, and principal ids replaced by placeholders.

## Scenarios

### Scenario: The OpenFGA overlay runs Lakekeeper on the OpenFGA authorizer

* *GIVEN* the three-overlay stack is up and the harness has provisioned it
* *WHEN* the harness reads `GET /management/v1/info` with the `lakehouse` token
* *THEN* `authz-backend` SHALL be `openfga`
* *AND* `bootstrapped` SHALL be true

### Scenario: The authorization suite fails when its stack is unavailable

* *GIVEN* Keycloak, Lakekeeper, or OpenFGA is not reachable, or Lakekeeper reports an `authz-backend` other than `openfga`
* *WHEN* the `lakekeeper-authz-e2e` suite runs
* *THEN* the suite SHALL fail
* *AND* the suite MUST NOT report the affected tests as skipped or passed

### Scenario: The shared realm file carries no authorization-suite principal

* *GIVEN* `scripts/keycloak-realm-iceberg.json`, which also provisions the AWS deployment through `deploy/lakekeeper-stack/main.tf`
* *WHEN* the suite reads the file's `clients` list
* *THEN* no client SHALL carry the id `lakehouse-reader-a`, `lakehouse-reader-b`, or `lakehouse-checker`

### Scenario: Two principals hold different table grants

* *GIVEN* the provisioned fixture grants
* *WHEN* the `lakehouse` operator batch-checks `read_data` on `authz_alpha` and `authz_beta` for each reader, naming the reader in `identity`
* *THEN* `lakehouse-reader-a` SHALL be allowed on `authz_alpha` and denied on `authz_beta`
* *AND* `lakehouse-reader-b` SHALL be allowed on `authz_beta` and denied on `authz_alpha`

### Scenario: A principal's Lakekeeper id is its IdP prefix and its token subject

* *GIVEN* each reader registered itself through `POST /management/v1/user` with no `id`
* *WHEN* the suite reads the reader's id from `GET /management/v1/whoami` and decodes the claims of the reader's access token
* *THEN* the id SHALL equal `oidc~` followed by the token's `sub` claim
* *AND* `sub` SHALL be a UUID that differs from the token's `preferred_username`, so no template over a user name derives the id of a principal that logged in directly
* *AND* a batch-check for `oidc~template-user@corp` on `authz_alpha` SHALL answer allowed, so a grant to a template-derived id takes effect before that user ever logs in

### Scenario: A caller without a grant-management privilege cannot check another identity

* *GIVEN* `lakehouse-checker` holds, in turn, no assignment, only server `admin`, and only `select` on `lakehouse_authz`
* *WHEN* `lakehouse-checker` batch-checks `read_data` on `authz_alpha` for `lakehouse-reader-a` in each state
* *THEN* each request SHALL fail as a whole with HTTP 403 and error type `CannotInspectPermissions`
* *AND* no response SHALL carry `results`

### Scenario: Warehouse manage_grants lets a caller check another identity

* *GIVEN* `lakehouse-checker` holds only `manage_grants` on `lakehouse_authz`
* *WHEN* `lakehouse-checker` batch-checks `read_data` on `authz_alpha` and `authz_beta` for `lakehouse-reader-a`
* *THEN* the response SHALL be 200 with `authz_alpha` allowed and `authz_beta` denied
* *AND* both answers SHALL equal the `lakehouse` operator's answers for the same checks

### Scenario: The management API is mounted beside the catalog path, not under it

* *GIVEN* the catalog URI `http://localhost:<port>/catalog`, the host-side address of the CONNECTION's `http://lakekeeper:8181/catalog`
* *WHEN* the suite requests `management/v1/info` relative to the catalog URI's parent and relative to the catalog URI itself
* *THEN* the parent-relative request SHALL answer 200
* *AND* the catalog-relative request SHALL answer 404

### Scenario: The catalog never advertises the management base, so a client cannot derive it safely

* *GIVEN* the same catalog URI
* *WHEN* the suite reads `GET /catalog/v1/config?warehouse=lakehouse_authz` without and with the header `x-forwarded-prefix: /lk`
* *THEN* `overrides.uri` SHALL be `http://localhost:<port>/catalog` without the header and `http://localhost:<port>/lk/catalog` with it
* *AND* no `overrides` or `defaults` value SHALL contain `/management`
* *AND* `GET /lk/management/v1/info` SHALL answer 404, because the external prefix belongs to a proxy and not to Lakekeeper's routes

### Scenario: Committed batch-check fixtures match the live contract

* *GIVEN* the fixtures `allowed.json`, `denied.json`, `missing.json`, and `cannot-inspect.json`, captured from Lakekeeper `v0.13.1` on OpenFGA `v1.8.16`
* *AND* a stack that runs the same Lakekeeper and OpenFGA versions
* *WHEN* the suite replays each fixture's request as its calling client, with the placeholders substituted
* *THEN* each live response SHALL match its fixture's shape
* *AND* `missing.json` SHALL record a 200 with `allowed: false` for a table that does not exist, the same answer `denied.json` records
* *AND* no fixture SHALL contain a token, a client secret, or a live warehouse, table, or principal id
