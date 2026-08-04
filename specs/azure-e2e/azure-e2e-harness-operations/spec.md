# Feature: Azure E2E Harness — Operational Contract

Operational-contract scenarios for the Azure E2E suite: container-name legality,
fail-loud behavior when a credential variable or the local stack is missing, the
Make target's rebuild-and-serialize behavior, the gitignored local credential
file, shared-harness scan-path provisioning, and output redaction on a
credential-bearing DDL failure. Split out of `azure-e2e/azure-e2e-harness` to
keep that feature's credential-path proof scenarios (provisioning, the two
credential-mode scans, and container-guard teardown) separate from this suite's
tooling and failure-mode contract.

## Background

* Every scenario here belongs to the same `azure-e2e` cargo feature and the same
  `make test-e2e-azure` target as `azure-e2e/azure-e2e-harness`, and shares its
  fail-loud-never-skip discipline for a missing credential variable or an
  unreachable local stack.
* See `azure-e2e/azure-e2e-harness` for the full environmental context: the five
  credential variables and their two roles, the HNS storage-account requirement,
  the service-principal role scope, and the CI scheduling and required-status-check
  contract.

## Scenarios

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
