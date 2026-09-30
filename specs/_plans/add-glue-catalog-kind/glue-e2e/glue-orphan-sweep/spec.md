# Feature: Glue Orphan Fixture Sweep

Deletes the Glue databases and S3 objects that a killed Glue E2E run left behind. A scheduled GitHub workflow runs the sweep, modeled on `azure-e2e/azure-orphan-container-sweep`.

## Background

* A run's teardown cannot run when its process is killed, so a per-run database `lh_e2e_<run id>` and its prefix `lh_e2e/<run id>/` can outlive the run.
* The sweep reads the same `GLUE_ACCESS_KEY_ID`, `GLUE_SECRET_ACCESS_KEY`, `GLUE_REGION`, and `GLUE_FIXTURE_BUCKET` as the suite, and uses the AWS CLI.
* A running suite finishes well within 24 hours, so a fixture older than 24 hours is an orphan.

## Scenarios

### Scenario: A scheduled run deletes stale orphaned fixtures

* *GIVEN* a Glue database `lh_e2e_alice_1700000000000` created more than 24 hours ago, and objects under `lh_e2e/` last modified more than 24 hours ago
* *WHEN* the weekly scheduled sweep runs
* *THEN* the sweep SHALL delete that database and those objects
* *AND* the sweep SHALL print each deleted name and a final count

### Scenario: A fixture younger than 24 hours is never swept

* *GIVEN* a Glue database `lh_e2e_*` created less than 24 hours ago, and objects under its prefix modified less than 24 hours ago
* *WHEN* the sweep runs
* *THEN* the sweep MUST NOT delete them
* *AND* the sweep SHALL judge age by the database `CreateTime` and the object `LastModified`, and MUST NOT judge it by the milliseconds in the name
* *AND* the sweep MUST NOT touch a database whose name does not start with `lh_e2e_` or an object outside `lh_e2e/`

### Scenario: A manual dispatch previews by default

* *GIVEN* a manual dispatch of the workflow
* *WHEN* the dispatch leaves `dry_run` at its default, and a second dispatch sets `dry_run` to false
* *THEN* the first run SHALL print what it would delete and SHALL delete nothing
* *AND* the second run SHALL delete, exactly as a scheduled run does

### Scenario: The sweep fails loudly when a variable is absent or an AWS call fails

* *GIVEN* one of the four variables is unset or empty, or an AWS CLI call returns an error
* *WHEN* the sweep runs
* *THEN* the run SHALL fail, naming the missing variable or the failed call

### Scenario: No credential value appears in the run log

* *GIVEN* any sweep run
* *WHEN* the run writes its log
* *THEN* the log MUST NOT contain the secret access key
