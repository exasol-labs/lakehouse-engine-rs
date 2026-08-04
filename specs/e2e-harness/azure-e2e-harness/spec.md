# Feature: Azure E2E Harness (ADLS Gen2 + Lakekeeper)

Verifies `abfss://` reads through the lakehouse VS against a real Azure Data Lake
Storage Gen2 account, over both credential paths: the static path (Lakekeeper
delegation off, account key carried in the Exasol CONNECTION) and the vended path
(Lakekeeper delegation on, a SAS minted per `loadTable` and carried by no
CONNECTION field). See `e2e-harness/azure-e2e-harness-operations` for the suite's
operational-contract scenarios — credential-variable and stack-availability
failure modes, container naming, the Make target, the gitignored credential file,
shared-harness provisioning, and DDL-failure output redaction.

## Background

* Azure has no working local substitute, so this suite is real-cloud by necessity,
  not by preference. Azurite's `dfs` endpoint is incomplete, and Lakekeeper
  v0.13.1's `adls` profile always addresses `https://<account-name>.<host>` with a
  bare hostname and no port, so an Azurite endpoint is not expressible through
  that profile at all.
* `make test-e2e-azure` is the single home for every Azure E2E case. Because
  storage is always real, the fixture-versus-cloud axis the S3 suites split on
  collapses, leaving only the credential axis — and both of its values now live in
  this one target and this one stack: the static (account-key) arm and the vended
  (SAS-delegated) arm.
* The suite is gated behind a dedicated `azure-e2e` cargo feature, distinct from
  `exasol-e2e`, `lakekeeper-e2e`, and `cloud-e2e`, and MUST fail (never skip) when
  the local stack, the credential variables, or the Azure account is unavailable —
  the same fail-loud discipline as `e2e-harness/e2e-harness` and
  `e2e-harness/lakekeeper-e2e-harness`, and the opposite of
  `e2e-harness/cloud-e2e-harness`.
* **Three credential roles, never conflated.** The harness's own container setup
  and teardown authenticates with an **Entra ID service principal**, because the
  official Azure blob crate offers no account-key auth. The **account key**
  authenticates the static arm's Exasol CONNECTION (the `AdlsCred::AccountKey`
  path), both arms' seed writes, and both warehouses' Lakekeeper storage
  credential. The **vended SAS** authenticates the vended arm's scan and nothing
  else: Lakekeeper mints it from that same account key and returns it per
  `loadTable`, and no CONNECTION field carries it (the `AdlsCred::Sas` path). The
  Virtual Schemas, the scans, and the seed writer never see the service principal;
  only the container guard uses it.
* Credentials reach the suite as five plain environment variables:
  `AZURE_STORAGE_ACCOUNT_NAME` and `AZURE_STORAGE_ACCOUNT_KEY` for the data path,
  and `AZURE_TENANT_ID`, `AZURE_CLIENT_ID`, and `AZURE_CLIENT_SECRET` for the
  container lifecycle. One contract, two fills: locally a gitignored `test.env`
  supplies them and the Make target loads it when the file exists; in CI the job
  sets the same five, the account name from a repository variable and the other four
  from repository secrets.
* **The vended arm introduces no new environment variable, Make target, Docker
  service, or `CatalogConnectionPassword` field.** Lakekeeper mints the SAS from
  the same `AZURE_STORAGE_ACCOUNT_KEY` the static arm already carries, so the
  five-variable contract stands unchanged. There is no `sas_token` CONNECTION field
  to add either: the vended CONNECTION carries no storage credential at all.
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
  credential under test, and the credential Lakekeeper delegates from — so the
  account SHOULD be a dedicated test-only storage account holding no other data.
* The service principal needs the **Storage Blob Data Contributor** role on the test
  storage account, and no more. Container create and delete are the
  `Microsoft.Storage/storageAccounts/blobServices/containers/write` and `/delete`
  actions, which that built-in role carries. A narrower custom role granting only
  the `containers/blobs/*` data actions cannot create or delete a container and
  fails with 403 — the container lifecycle is not a blob data action.
* Lakekeeper never creates the ADLS filesystem (container). Its ADLS storage layer
  only opens clients on an existing filesystem, and warehouse creation validates
  physical access by writing and then deleting a probe object under the configured
  location. The harness therefore creates the container before creating either
  warehouse, and a wrong account key or a missing container fails warehouse
  creation immediately rather than surfacing later as a scan error.
* **Both credential modes share one per-run container; only the warehouse differs.**
  Two Lakekeeper warehouses sit over that one container — `<container>-static` with
  `sas-enabled: false` and `<container>-vended` with `sas-enabled: true` — each with
  `key-prefix` equal to its own warehouse name, so neither prefix is a prefix of the
  other. This is the topology `e2e-harness/lakekeeper-e2e-harness` already runs over
  its one shared MinIO `warehouse` bucket. One container halves the live-Azure
  provisioning cost of a second credential arm and keeps the orphan surface at one
  resource — and that is all it buys. It does not make the cross-arm row comparison
  stronger: the two arms are seeded independently into two disjoint `key-prefix`es, so
  they are two Iceberg tables over two sets of Parquet files. The comparison is
  meaningful because both arms are seeded from the one deterministic 20-row shape every
  E2E suite uses (id 1..20, score = 5.0 × id), which would hold across two containers
  too. Cross-arm row equality is therefore NOT evidence that both arms read the same
  bytes.
* **The vended CONNECTION's required shape is the empty one, and one shared helper
  branch is what makes it so.** Both credential-vending suites build their vended
  CONNECTION password from the one shared helper's vended branch, which returns the
  OAuth2 catalog-authentication fields alone and populates no storage field of any
  backend. Serialized, that password carries no `account_name` and no `account_key`
  key at all, plus an empty `endpoint`, `region`, `access_key`, and `secret_key` (the
  adapter reads an empty string as absent). One unit test pins that shape for both
  backends, so the Azure arm needs no second helper that could drift from the S3 one.
  With no account name and no account key present, scheme-driven resolution has
  nothing to fall back to, so a passing scan is reachable only through the vended SAS.
* **`abfss://` needs no plaintext consent, so the vended Azure arm is NOT the
  counterpart of the S3 arm's `ALLOW_HTTP` positive case.** `resolve_vended_storage`
  matches `abfss` unconditionally and gates only `abfs` on `allow_http`. The shared
  VS DDL sets `ALLOW_HTTP = 'true'` on every virtual schema it creates
  (`crates/lakehouse-engine/tests/common/e2e_harness.rs:270`), so both Azure virtual
  schemas carry it — inert on this path. The `abfs://`-requires-consent branch stays
  unit-covered in `crates/lakehouse-catalog/src/vended.rs`.
* **The vended SAS key shape is not specified by the Iceberg REST spec, which makes
  a live run its only conformance evidence.** The REST Catalog OpenAPI specification
  enumerates `config` keys under `## AWS Configurations` only and names no ADLS key
  anywhere: "Credentials for ADLS / GCS / S3 / … are provided through the
  `storage-credentials` field. Clients must first check whether the respective
  credentials exist in the `storage-credentials` field before checking the `config`
  for credentials." The host-suffixed `adls.sas-token.<host>` key the adapter parses
  comes from the reference implementation
  (`ADLS_SAS_TOKEN_PREFIX = "adls.sas-token."`,
  `azure/src/main/java/org/apache/iceberg/azure/AzureProperties.java:43`), not from
  the specification. No spec reading can hold Lakekeeper to that key shape; this
  suite's passing run is the only thing that does.
* **Seeding a `sas-enabled: true` warehouse needs no defence against its own vended
  credential, by key shape rather than by flag.** `RestCatalog::load_file_io` merges
  a table's `loadTable` `config` OVER the builder props, which is why the MinIO arm
  installs a static credential loader to stop vended STS keys reaching its seed
  writes. The ADLS arm needs no equivalent: Lakekeeper vends the host-suffixed
  `adls.sas-token.<host>`, while iceberg-rust reads only the flat `adls.sas-token`
  and `adls.account-key` (`iceberg-0.10.0/src/io/storage/config/azdls.rs:34-38`), so
  a vended key under the host-suffixed name cannot reach `AzdlsConfig` and cannot
  displace the seed's account key. Both warehouses are therefore seeded through the
  one shared seed-catalog configuration with no per-arm override. The iceberg-rust
  half of that argument is read off the source above; the Lakekeeper half — that
  Lakekeeper emits the SAS ONLY host-suffixed and never as a flat `adls.sas-token` —
  is settled only by the live run, so a vended-arm seed write failure means that
  premise broke rather than that the account key is wrong.
* **This suite cannot cover SAS expiry, and MUST NOT be read as covering it.**
  iceberg-rust 0.10.0 binds a static SAS at FileIO build time with no refresh, so a
  query outliving the SAS validity window fails mid-scan; that ceiling is recorded
  against the extraction path in `vs-adapter/pushdown-planning-cloud-credentials`,
  not here. A 20-row, two-file scan completes far inside any validity window
  Lakekeeper issues, so this suite neither exercises the ceiling nor flakes on it.
* CI schedules the `E2E (Azure)` job on the same events as the `E2E` and `E2E
  (Lakekeeper)` jobs, forks included; a draft pull request is the one exclusion, and
  it is not this job's own — the job cascade-skips through its `needs` on the `.so`
  build job, whose draft guard it inherits. A fork pull request cannot read the
  account-key secret, so the job runs and fails loudly naming the missing variable,
  per this spec's fail-loud-never-skip contract for an absent credential variable.
  That failure blocks nothing: `E2E (Azure)` is not a required status check on `main`
  (`Check & Lint`, `Unit Tests`, `License Check`, `E2E`, and `E2E (Lakekeeper)` are),
  so a fork pull request with a red `E2E (Azure)` can still be merged.
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
* **A second credential arm adds no Azure-side orphan.** The container is the only
  cloud resource, and deleting it removes both warehouses' blobs. Each run does
  leave two per-run warehouse registrations rather than one, but those are Lakekeeper
  metadata rows in the local PostgreSQL database — a local-only accumulation already
  accepted for a single warehouse.
* No credential value — account key, Keycloak client secret, bearer token, or vended
  SAS — appears in test output. Both warehouse-creation request bodies carry the
  account key, so the shared warehouse-creation helper's existing contract holds for
  both ADLS arms: a failure message names the endpoint and the HTTP status only,
  never the response body.
* Data-file URIs are `abfss://<container>@<account>.dfs.core.windows.net/<key-prefix>/...`,
  the location shape Lakekeeper v0.13.1 derives from an `adls` storage profile. The
  two arms differ only in `<key-prefix>`.
* A static-arm Azure CONNECTION supplies `account_name` and `account_key` and leaves
  every static S3 storage field (`endpoint`, `region`, `access_key`, `secret_key`,
  `session_token`) absent. The adapter rejects a CONNECTION that names both an
  Azure and an S3 storage field as an ambiguous credential set, per
  `vs-adapter/storage-backend-enum`. A vended-arm CONNECTION supplies no storage
  field at all, which the adapter accepts: its Azure-versus-S3 ambiguity rule fires
  only when some Azure field is present, and `use_vended_credentials` discards
  whatever static storage block the CONNECTION would otherwise yield.
* This suite adds coverage only. The production Azure read path it exercises is
  unchanged by this feature: the `Adls` storage backend, its CONNECTION shape, the
  Azure object-store registration, and the vended-SAS extraction in
  `crates/lakehouse-catalog/src/vended.rs`.

## Scenarios

### Scenario: Harness provisions a per-run container and one ADLS warehouse per credential mode

* *GIVEN* a healthy local stack of Exasol, Lakekeeper, its PostgreSQL metadata database, and Keycloak, plus all five Azure variables naming a reachable storage account and a service principal holding Storage Blob Data Contributor on it
* *WHEN* the suite provisions its Azure fixture before any query
* *THEN* the harness SHALL create a new blob container named `lhrs-e2e-<sanitized-user>-<millis>` in that account, authenticating with the Entra ID service principal, before creating either warehouse — because Lakekeeper creates no filesystem and validates physical access at warehouse-creation time
* *AND* the harness SHALL create exactly two warehouses over that one container, `<container>-static` with `sas-enabled` `false` and `<container>-vended` with `sas-enabled` `true`, each carrying an `adls` storage profile whose `account-name` is the configured account, whose `filesystem` is that container, and whose `key-prefix` is its own warehouse name — so neither `key-prefix` is a prefix of the other, which Lakekeeper requires — and each paired with an `az` storage credential of `credential-type` `shared-access-key` holding the SAME account key, because Lakekeeper mints the vended SAS from that account key rather than from a second credential
* *AND* both warehouse names SHALL carry the same per-run suffix as the container, so a repeated local run never binds to a surviving warehouse whose container has already been deleted
* *AND* the harness SHALL create both warehouses through the one existing warehouse-creation helper rather than declaring a second POST path, so that helper's idempotency handling and its credential-safe failure messages keep any account-key value out of test output on both arms
* *AND* the harness SHALL seed both warehouses through the one shared seed-catalog configuration carrying the account key, with no per-arm override, because a SAS vended under the host-suffixed `adls.sas-token.<host>` key cannot reach the flat account-key property the seed's FileIO reads
* *AND* the vended arm SHALL require no additional environment variable, no additional Make target, no additional Docker service, and no additional CONNECTION storage field

### Scenario: End-to-end scan over the static-credential ADLS warehouse returns correct rows

* *GIVEN* a virtual schema over an Iceberg table seeded into the per-run `<container>-static` ADLS warehouse
* *AND* a CONNECTION naming that warehouse and supplying OAuth2 catalog authentication plus `account_name` and `account_key`, setting `use_vended_credentials` false, and carrying neither a static S3 storage field nor any Entra ID service-principal field
* *WHEN* a user runs `SELECT <subset of columns> FROM <vs>.<table> WHERE <predicate> LIMIT <n>`
* *THEN* the query SHALL return exactly the rows that satisfy the predicate, capped at `n`, projected to the selected columns, with values matching the seeded source data
* *AND* every seeded data-file path SHALL be an `abfss://<container>@<account>.dfs.core.windows.net/` location under this warehouse's own `key-prefix`, disjoint from the vended sibling's, confirming the scan read real Azure storage rather than a local fallback, and SHALL be read with the account key carried in the CONNECTION — which Lakekeeper reporting this warehouse's `sas-enabled` as `false` on readback is what keeps true while a SAS-vending sibling shares the container, because a `sas-enabled: false` warehouse vends no credential regardless of the request headers
* *AND* the test MUST fail (not skip) when the local stack or the Azure account is unavailable

### Scenario: End-to-end scan over the vended-credential ADLS warehouse returns correct rows

* *GIVEN* a virtual schema over an Iceberg table seeded into the per-run `<container>-vended` ADLS warehouse, whose CONNECTION supplies OAuth2 catalog authentication, sets `use_vended_credentials` true, and supplies no storage field of any kind
* *WHEN* a user runs `SELECT <subset of columns> FROM <vs>.<table> WHERE <predicate> LIMIT <n>` through that virtual schema
* *THEN* the adapter SHALL send the `X-Iceberg-Access-Delegation: vended-credentials` header on the `loadTable` request, read the SAS Lakekeeper returns under the `adls.sas-token.<host>` key for the table location's own storage host, derive the ADLS account name from that same host rather than from the CONNECTION, and carry both into every per-shard scan spec
* *AND* the scan SHALL read the `abfss://` data files with that SAS and return rows identical to the same query run over the static-credential warehouse in the same container
* *AND* the test SHALL assert that the vended CONNECTION carries no `account_name` key and no `account_key` key at all, and an empty `endpoint`, `region`, `access_key`, and `secret_key`, because that empty shape is the REQUIRED shape rather than merely a delegation proof — with no account name and no account key to fall back to, a passing scan is reachable only through the vended SAS
* *AND* Lakekeeper SHALL report this warehouse's `sas-enabled` as `true` and its `filesystem` as the same container the static warehouse uses, read back through the management API rather than assumed from the request the harness sent
* *AND* the scenario SHALL NOT depend on `ALLOW_HTTP`: `abfss://` names TLS transport and needs no plaintext consent, so the property the shared VS DDL sets on every virtual schema is inert here, and this scenario is NOT the Azure counterpart of the S3 arm's plaintext-consent case
* *AND* no vended SAS value and no account-key value SHALL appear in any returned SQL string or test output, and the suite MUST NOT itself request `loadTable` to inspect the vended payload, because that would place a live SAS in the test process's memory and output for diagnostic value only
* *AND* because both credential arms share one fixture and one test function, every assertion specific to the vended arm except the cross-arm row comparison SHALL run BEFORE the static arm's assertions, so a static-arm QUERY or ASSERTION regression cannot mask the vended proof this scenario adds; a static-arm PROVISIONING failure aborts the shared fixture before any assertion runs and does mask it, which is the residual cost of one fixture, reduced but not removed by provisioning the vended arm's warehouse, seed, and virtual schema BEFORE the static arm's
* *AND* the test MUST fail (not skip) when the local stack or the Azure account is unavailable

### Scenario: Per-run container is deleted when its owning scope ends, including on panic

* *GIVEN* a per-run container created through the harness's container guard under the Entra ID service principal
* *WHEN* the scope owning that guard ends, whether by returning normally or by unwinding from a panic
* *THEN* the guard SHALL delete that container — the one container holding BOTH warehouses' data, so a second credential arm adds no Azure-side orphan surface — including when the scope ends inside an active Tokio runtime context, because driving an async delete on the ambient runtime would panic and abort the process mid-unwind
* *AND* a delete failure encountered while unwinding SHALL be reported without panicking a second time, so the original test failure remains the reported one
* *AND* a name collision at create time SHALL fail the run, because the millisecond-suffixed name makes a collision a defect rather than a tolerable state
* *AND* a container already absent at delete time SHALL be treated as deleted
