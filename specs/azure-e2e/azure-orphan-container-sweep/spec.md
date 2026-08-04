# Feature: Azure Orphan Container Sweep

Reclaims Azure blob containers that the `azure-e2e` suite leaks when its `Drop`
guard is skipped, by periodically deleting stale `lhrs-e2e-` containers from the
shared test storage account.

## Background

* This sweep is the out-of-band backstop for the gap named in
  `azure-e2e/azure-e2e-harness`: the per-run container's `Drop` guard runs on
  normal return and on panic unwind, but never when the process is killed
  (`SIGKILL`, CI cancellation, OOM), so a killed run orphans its container
  permanently. Azure Blob lifecycle-management policies act on blobs, never on
  containers, so no lifecycle rule can reclaim an orphan.
* The sweep is a scheduled GitHub Actions workflow hosted **in this repository**,
  not tooling owned outside it. It runs on a `schedule` cron of `0 2 * * 1`
  (weekly, Monday 02:00 UTC) and on `workflow_dispatch`.
* The sweep authenticates with the same Entra ID service principal the `azure-e2e`
  harness uses for container lifecycle, read as three repository secrets
  `AZURE_TENANT_ID`, `AZURE_CLIENT_ID`, and `AZURE_CLIENT_SECRET`. That principal
  holds Storage Blob Data Contributor, which carries container list and delete.
  The storage account name is the repository variable `AZURE_STORAGE_ACCOUNT_NAME`.
* The sweep never reads `AZURE_STORAGE_ACCOUNT_KEY`. The account key is the
  data-path credential the `azure-e2e` scan exists to verify; the sweep touches
  only the container control plane, so it needs the service principal and nothing
  more.
* The sweep lists only containers whose name begins with `lhrs-e2e-`, enumerating
  each candidate as its name followed by its `last_modified` in that fixed field
  order, so each row's name and timestamp are read positionally without ambiguity.
  Every other container in the account is out of reach, so a sweep can never delete
  non-suite data even in a shared account.
* The retention discriminator is Azure's `last_modified` container property, not
  the millisecond suffix embedded in the container name. A container is stale when
  its `last_modified` is more than 24 hours before the run start.
* The sweep step runs under `set -euo pipefail`. Any `az` command that exits
  non-zero fails the workflow run red. The sweep wires no notification path; a red
  run is the whole signal.
* `workflow_dispatch` exposes a `dry_run` boolean input that defaults to `true`, so
  a manual run previews by default: it lists the containers a real run would delete
  and deletes nothing. The `schedule` trigger carries no input, so a scheduled run
  always deletes.
* No credential value — client secret or access token — appears in the run log.
* This feature adds operational tooling only. It changes neither the `azure-e2e`
  suite nor its `Drop`-guard cleanup, whose in-process teardown remains the
  first-line mechanism this sweep only backstops.

## Scenarios

### Scenario: Scheduled run reclaims stale orphaned containers

* *GIVEN* the test storage account holds containers named with the `lhrs-e2e-` prefix whose `last_modified` is more than 24 hours before the run start
* *AND* the four sweep variables name a reachable account and a service principal holding Storage Blob Data Contributor on it
* *WHEN* the scheduled `0 2 * * 1` trigger fires, which carries no `dry_run` input
* *THEN* the run SHALL delete every `lhrs-e2e-` container whose `last_modified` is older than 24 hours, authenticating with the Entra ID service principal and never the account key, and SHALL delete no other container
* *AND* a container already absent when its delete is issued SHALL be treated as deleted, because `az storage container delete` without `--fail-not-exist` succeeds on a missing container
* *AND* the run SHALL delete for real rather than preview, because the `dry_run` default applies only to the `workflow_dispatch` path

### Scenario: A container within the 24-hour retention floor is never swept

* *GIVEN* a `lhrs-e2e-` container whose `last_modified` is at most 24 hours before the run start, as a currently running or just-finished `azure-e2e` run would have
* *WHEN* any sweep run — scheduled or manual, real or dry-run — evaluates that container
* *THEN* the run SHALL retain that container and MUST NOT delete it
* *AND* the retention decision SHALL read the `last_modified` container property, not the millisecond suffix in the container name, so an in-flight run is never race-deleted

### Scenario: Sweep with nothing to reclaim succeeds without deleting

* *GIVEN* no `lhrs-e2e-` container in the account has a `last_modified` older than 24 hours, whether because none carries the prefix or because every match is within the 24-hour floor
* *WHEN* a scheduled or dry-run sweep runs
* *THEN* the run SHALL succeed and exit green
* *AND* the run SHALL delete no container
* *AND* the empty candidate list SHALL NOT fail the step under `set -euo pipefail`

### Scenario: Manual dispatch previews by default

* *GIVEN* an operator triggers the workflow through `workflow_dispatch` without setting `dry_run`
* *WHEN* the run executes
* *THEN* `dry_run` SHALL default to `true`
* *AND* the run SHALL list every `lhrs-e2e-` container older than 24 hours that a real run would delete, and MUST NOT delete any container
* *AND* the guard SHALL decide dry-run by comparing the input against the string `'true'`, because a `workflow_dispatch` boolean input arrives as the string `'true'` or `'false'` and the non-empty string `'false'` is otherwise truthy

### Scenario: Manual dispatch with dry-run disabled deletes for real

* *GIVEN* an operator triggers the workflow through `workflow_dispatch` with `dry_run` set to `false`
* *WHEN* the run executes
* *THEN* the run SHALL delete every `lhrs-e2e-` container older than 24 hours, applying the same selection the scheduled run applies

### Scenario: Sweep fails loudly when a required variable is absent

* *GIVEN* any one of `AZURE_STORAGE_ACCOUNT_NAME`, `AZURE_TENANT_ID`, `AZURE_CLIENT_ID`, or `AZURE_CLIENT_SECRET` is unset or empty
* *WHEN* the sweep runs
* *THEN* the run SHALL fail with a message naming the missing variable, mirroring the `azure-e2e` fail-loud contract for an absent credential variable
* *AND* that message MUST NOT contain any credential value

### Scenario: Any Azure CLI failure fails the run

* *GIVEN* the sweep step runs under `set -euo pipefail`
* *WHEN* `az login`, `az storage container list`, or `az storage container delete` exits non-zero for a reason other than an already-absent container
* *THEN* the workflow run SHALL fail red
* *AND* the run MUST NOT swallow the error or report success

### Scenario: No credential value appears in the run log

* *GIVEN* the sweep authenticates with the service principal and runs `az` commands
* *WHEN* the run log is captured
* *THEN* no credential value — client secret or access token — SHALL appear in the log
* *AND* the step MUST NOT echo `AZURE_CLIENT_SECRET`, relying on the secret's value reaching `az` only through the masked environment
