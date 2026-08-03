# Feature: Azure E2E Harness (ADLS Gen2 + Lakekeeper)

Verifies `abfss://` reads through the lakehouse VS against a real Azure Data Lake
Storage Gen2 account. Covers the static credential path: Lakekeeper delegation off,
account key carried in the Exasol CONNECTION.

## Background

* Azure has no working local substitute, so this suite is real-cloud by necessity,
  not by preference. Azurite's `dfs` endpoint is incomplete, and Lakekeeper
  v0.13.1's `adls` profile always addresses `https://<account-name>.<host>` with a
  bare hostname and no port, so an Azurite endpoint is not expressible through
  that profile at all.
* `make test-e2e-azure` is the single home for every Azure E2E case. Because
  storage is always real, the fixture-versus-cloud axis the S3 suites split on
  collapses, leaving only the credential axis: the vended (SAS-delegated) sibling
  case joins this same target and this same stack in a later slice.
* The suite is gated behind a dedicated `azure-e2e` cargo feature, distinct from
  `exasol-e2e`, `lakekeeper-e2e`, and `cloud-e2e`, and MUST fail (never skip) when
  the local stack, the credential variables, or the Azure account is unavailable —
  the same fail-loud discipline as `e2e-harness/e2e-harness` and
  `e2e-harness/lakekeeper-e2e-harness`, and the opposite of
  `e2e-harness/cloud-e2e-harness`.
* **Two credential paths, two purposes, never conflated.** The harness's own
  container setup and teardown authenticates with an **Entra ID service principal**,
  because the official Azure blob crate offers no account-key auth. The data path
  actually under test authenticates with the **account key carried in the Exasol
  CONNECTION** — that is the `AdlsCred::AccountKey` path this slice exists to
  verify. The Virtual Schema, the scan, and the seed writer never see the service
  principal; only the container guard uses it.
* Credentials reach the suite as five plain environment variables:
  `AZURE_STORAGE_ACCOUNT_NAME` and `AZURE_STORAGE_ACCOUNT_KEY` for the data path,
  and `AZURE_TENANT_ID`, `AZURE_CLIENT_ID`, and `AZURE_CLIENT_SECRET` for the
  container lifecycle. One contract, two fills: locally a gitignored `test.env`
  supplies them and the Make target loads it when the file exists; in CI the job
  sets the same five, the account name from a repository variable and the other four
  from repository secrets.
* The storage account MUST be a StorageV2 account with **hierarchical namespace
  enabled**. Lakekeeper v0.13.1 makes this a setup instruction for ADLS warehouses
  (`docs/docs/storage.md`: "Make sure to select 'Enable hierarchical namespace'…
  For existing Storage Accounts make sure 'Hierarchical namespace: Enabled' is shown
  in the 'Overview' page") and its ADLS backend drives the DFS surface throughout
  with no HNS detection or fallback. Enabling HNS on an existing account is a
  one-way upgrade, so a non-HNS account blocks this suite behind an Azure admin
  action rather than a code change.
* `AZURE_STORAGE_ACCOUNT_KEY` is an unscopable, full-account credential: it cannot be
  restricted to one container or rotated per consumer, and it grants complete
  data-plane control over the whole account. The slice cannot avoid it — it is the
  credential under test — so the account SHOULD be a dedicated test-only storage
  account holding no other data.
* The service principal needs the **Storage Blob Data Contributor** role on the test
  storage account, and no more. Container create and delete are the
  `Microsoft.Storage/storageAccounts/blobServices/containers/write` and `/delete`
  actions, which that built-in role carries. A narrower custom role granting only
  the `containers/blobs/*` data actions cannot create or delete a container and
  fails with 403 — the container lifecycle is not a blob data action.
* Lakekeeper never creates the ADLS filesystem (container). Its ADLS storage layer
  only opens clients on an existing filesystem, and warehouse creation validates
  physical access by writing and then deleting a probe object under the configured
  location. The harness therefore creates the container before creating the
  warehouse, and a wrong account key or a missing container fails warehouse
  creation immediately rather than surfacing later as a scan error.
* CI schedules the job only where the account key is reachable; a fork pull request
  has no access to the repository secret. Not scheduling a job is distinct from the
  suite skipping: whenever the suite does run, an absent variable or an unreachable
  service fails it.
* **Known ceiling — orphaned containers.** The per-run container is deleted by a
  `Drop` guard, which runs both on a normal return and while unwinding from a test
  panic (this workspace compiles tests with unwinding panics, so a panicking test
  still cleans up). It does not run when the process is killed, so a cancelled CI
  run leaves its container behind. The `lhrs-e2e-<user>-<millis>` name keeps an
  orphan attributable to this suite, to a user, and to one run. Azure Blob
  lifecycle-management policies act on blobs, never on containers, so no lifecycle
  rule can sweep an orphaned container — removing one requires an out-of-band
  scheduled sweep (Azure CLI or a Function) owned outside this repository and
  tracked as a follow-up issue (#291). The account already holds leftovers from
  earlier spike runs, so the sweep is a real operational need, not a hypothetical.
* No credential value — account key, Keycloak client secret, or bearer token —
  appears in test output. The warehouse-creation request body carries the account
  key, so the shared warehouse-creation helper's existing contract holds for the
  ADLS arm too: a failure message names the endpoint and the HTTP status only,
  never the response body.
* Data-file URIs are `abfss://<container>@<account>.dfs.core.windows.net/<key-prefix>/...`,
  the location shape Lakekeeper v0.13.1 derives from an `adls` storage profile.
* An Azure CONNECTION supplies `account_name` and `account_key` and leaves every
  static S3 storage field (`endpoint`, `region`, `access_key`, `secret_key`,
  `session_token`) absent. The adapter rejects a CONNECTION that names both an
  Azure and an S3 storage field as an ambiguous credential set, per
  `vs-adapter/storage-backend-enum`.
* Scan-path provisioning (SLC install, `.so` upload, script DDL, Virtual Schema
  creation) reuses the shared `common/e2e_harness` definition per
  `e2e-harness/e2e-harness`; only the CONNECTION password, warehouse name, and
  namespace vary.
* This suite adds coverage only. The production Azure read path it exercises — the
  `Adls` storage backend, its CONNECTION shape, and the Azure object-store
  registration — is unchanged by this feature.

## Scenarios

### Scenario: Harness provisions a per-run container and a delegation-disabled ADLS warehouse

* *GIVEN* a healthy local stack of Exasol, Lakekeeper, its PostgreSQL metadata database, and Keycloak, plus all five Azure variables naming a reachable storage account and a service principal holding Storage Blob Data Contributor on it
* *WHEN* the suite provisions its Azure fixture before any query
* *THEN* the harness SHALL create a new blob container named `lhrs-e2e-<sanitized-user>-<millis>` in that account, authenticating with the Entra ID service principal, before creating the warehouse — because Lakekeeper creates no filesystem and validates physical access at warehouse-creation time
* *AND* the harness SHALL create the warehouse with an `adls` storage profile whose `account-name` is the configured account, whose `filesystem` is that container, whose `key-prefix` is the warehouse name, and whose `sas-enabled` is `false`, paired with an `az` storage credential of `credential-type` `shared-access-key` carrying the account key
* *AND* the warehouse name SHALL carry the same per-run suffix as the container, so a repeated local run never binds to a surviving warehouse whose container has already been deleted
* *AND* the harness SHALL reach Lakekeeper through the one existing warehouse-creation helper rather than declaring a second POST path, so that helper's idempotency handling and its credential-safe failure messages keep any account-key value out of test output on the ADLS arm too

### Scenario: End-to-end scan over the static-credential ADLS warehouse returns correct rows

* *GIVEN* a virtual schema over an Iceberg table seeded into the per-run ADLS warehouse
* *AND* a CONNECTION naming the per-run warehouse and supplying OAuth2 catalog authentication plus `account_name` and `account_key`, setting `use_vended_credentials` false, and carrying neither a static S3 storage field nor any Entra ID service-principal field
* *WHEN* a user runs `SELECT <subset of columns> FROM <vs>.<table> WHERE <predicate> LIMIT <n>`
* *THEN* the query SHALL return exactly the rows that satisfy the predicate, capped at `n`, projected to the selected columns, with values matching the seeded source data
* *AND* every seeded data-file path SHALL be an `abfss://<container>@<account>.dfs.core.windows.net/` location, confirming the scan read real Azure storage rather than a local fallback, and SHALL be read with the account key carried in the CONNECTION, because a `sas-enabled: false` warehouse vends no credential regardless of the request headers
* *AND* the test MUST fail (not skip) when the local stack or the Azure account is unavailable

### Scenario: Per-run container is deleted when its owning scope ends, including on panic

* *GIVEN* a per-run container created through the harness's container guard under the Entra ID service principal
* *WHEN* the scope owning that guard ends, whether by returning normally or by unwinding from a panic
* *THEN* the guard SHALL delete that container, including when the scope ends inside an active Tokio runtime context, because driving an async delete on the ambient runtime would panic and abort the process mid-unwind
* *AND* a delete failure encountered while unwinding SHALL be reported without panicking a second time, so the original test failure remains the reported one
* *AND* a name collision at create time SHALL fail the run, because the millisecond-suffixed name makes a collision a defect rather than a tolerable state
* *AND* a container already absent at delete time SHALL be treated as deleted

### Scenario: Container name is legal for Azure and Lakekeeper whatever the user name contains

* *GIVEN* a `$USER` value containing uppercase letters, dots, consecutive punctuation, or nothing at all
* *WHEN* the harness derives the per-run container name
* *THEN* the derived name SHALL consist of 3 to 63 characters drawn only from lowercase letters, digits, and hyphens
* *AND* the name SHALL NOT contain consecutive hyphens and SHALL NOT begin or end with a hyphen, because Lakekeeper rejects such a filesystem name at warehouse creation
* *AND* the name SHALL retain both the `lhrs-e2e-` prefix and the millisecond suffix, so an orphaned container stays attributable to this suite and to one run

### Scenario: Azure suite fails when a required credential variable is absent

* *GIVEN* any one of `AZURE_STORAGE_ACCOUNT_NAME`, `AZURE_STORAGE_ACCOUNT_KEY`, `AZURE_TENANT_ID`, `AZURE_CLIENT_ID`, or `AZURE_CLIENT_SECRET` is unset or empty
* *WHEN* the `azure-e2e` suite runs
* *THEN* the suite SHALL fail with a message naming the missing variable, because the official Azure blob crate supplies no environment-scanning credential and a silently absent variable would otherwise surface as an authorization failure
* *AND* that message MUST NOT contain any credential value
* *AND* the suite MUST NOT report the affected tests as skipped or passed

### Scenario: Azure suite fails when the local stack is unavailable

* *GIVEN* Lakekeeper, Keycloak, or Exasol is not reachable
* *WHEN* the `azure-e2e` suite runs
* *THEN* the suite SHALL fail
* *AND* the suite MUST NOT report the affected tests as skipped or passed

### Scenario: The Azure Make target rebuilds the .so before running the suite

* *GIVEN* the repository Makefile
* *WHEN* an operator runs `make test-e2e-azure`
* *THEN* the target SHALL rebuild the `.so` through the containerized build before running any test, because a stale binary would make this suite a vacuous gate
* *AND* the target SHALL load `test.env` into the environment when that file exists and SHALL NOT fail when it does not, because CI supplies the same five variables directly
* *AND* the target SHALL run the `azure-e2e` suite with `--test-threads=1`, because all its tests share one Exasol provisioning

### Scenario: Local credential file cannot be committed

* *GIVEN* the repository working tree
* *WHEN* an operator fills in local Azure credentials to run the suite
* *THEN* `.gitignore` SHALL list `test.env`, so a filled-in credential file is never committable
* *AND* a committed `test.env.example` SHALL name all five variables and SHALL state which two reach the CONNECTION under test and which three drive only the container lifecycle
* *AND* `test.env.example` MUST NOT contain a real credential value

### Scenario: Azure binary provisions the scan path from the shared harness definition

* *GIVEN* the `azure-e2e` test binary under `crates/lakehouse-engine/tests`
* *AND* the shared `common/e2e_harness` module defining the SLC install, the `.so` upload, and the script creation
* *WHEN* the binary's setup provisions the lakehouse VS scan path
* *THEN* the binary SHALL install `LAKEHOUSE_SCAN`, `LAKEHOUSE_DISTRIBUTE_FILES`, and the adapter script from that shared definition, so the script DDL is byte-identical to every other E2E binary
* *AND* the Azure-specific CONNECTION password, warehouse name, and namespace SHALL be supplied as explicit parameters rather than by re-declaring the provisioning logic
* *AND* the seed path SHALL reach `abfss://` by selecting an ADLS storage backend on the one shared seed-catalog configuration, NOT by forking the table-create-write-commit logic

### Scenario: No Azure credential value appears in output when credential-bearing DDL fails

* *GIVEN* an Azure CONNECTION DDL carrying sentinel `account_name` and `account_key` values, made syntactically invalid so its execution fails
* *WHEN* the suite executes it through the redacting Exasol WebSocket client
* *THEN* the failure output MUST NOT contain the SQL text
* *AND* the failure output MUST NOT contain either sentinel value
