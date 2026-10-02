# Feature: Glue E2E Harness

Runs the `GLUE` catalog kind end to end against a real AWS Glue Data Catalog and S3 bucket, with a local Exasol. Each run creates its own Glue database and S3 prefix and removes both, so two concurrent runs never share a fixture.

## Background

* The suite reads `GLUE_ACCESS_KEY_ID` and `GLUE_SECRET_ACCESS_KEY` (repository secrets) and `GLUE_REGION` and `GLUE_FIXTURE_BUCKET` (repository variables) from the environment or from `./test.env`.
* The gate mirrors the Azure gate (`azure-e2e/azure-e2e-harness-operations`): a real cloud account, a local Exasol, and no place in `release`'s needs, so an AWS outage or a rotated key cannot block a release.
* The fixture set: an Iceberg table written by `iceberg-rust` and registered with `table_type` `ICEBERG` and its `metadata_location`; the unpartitioned Hive Parquet tables `all_types` and `binary_values` of `datafusion-scan/type-mapping-live-coverage`, typed by Hive type strings, whose data files have no file extension, with `all_types` carrying every Hive type of `vs-adapter/glue-hive-type-mapping`; a Hive Parquet table partitioned by `int`, `date`, and `string` keys, with a NULL partition, a partition whose value is `a b/c`, a partition outside the table location, a partition addressed with `s3a://`, and an ORC partition; and a partition-projection table, a view, an ORC table, and an Athena-style Delta table.

## Scenarios

### Scenario: Each run provisions its own Glue database and S3 prefix and removes both, including on panic

* *GIVEN* the four configuration variables are set
* *WHEN* the suite starts and later ends, once normally and once after a test panics
* *THEN* the harness SHALL create the Glue database `lh_e2e_<run id>` and SHALL write every fixture object under `s3://<GLUE_FIXTURE_BUCKET>/lh_e2e/<run id>/`
* *AND* the run id SHALL be the sanitized user name and the epoch milliseconds, made of lowercase letters, digits, and `_` only
* *AND* the harness SHALL delete the database, its tables, and every object under the prefix when the owning scope ends, in both cases
* *AND* a teardown failure SHALL name the leaked database or prefix and MUST NOT panic
* *AND* CI and a local run SHALL build the fixtures from the same harness code

### Scenario: The listing includes the routed tables and records every skip

* *GIVEN* the registered fixtures
* *WHEN* a virtual schema over the run's database is created
* *THEN* the virtual schema SHALL list the Iceberg table and the Parquet tables with their Exasol column types
* *AND* `SKIPPED_TABLES` SHALL record the projection table, the view, the ORC table, and the Delta table, each with its reason
* *AND* the harness SHALL have written no data file for the ORC partition, the projection table, the view, the ORC table, or the Delta table

### Scenario: Queries through pushdown return the expected rows

* *GIVEN* the same virtual schema
* *WHEN* the suite queries each table
* *THEN* the Iceberg table SHALL return its rows
* *AND* projection, filter, and LIMIT pushdown SHALL return the same rows as the unpushed query
* *AND* the NULL, `a b/c`, out-of-root, and `s3a://` partitions SHALL return their rows with their Glue values
* *AND* a query that reads the ORC partition SHALL fail naming it
* *AND* a partition predicate SHALL reduce the file list of the pushed scan, checked with `COUNT(*)` and `LIMIT` queries so the run reads few bytes

### Scenario: The suite fails, never skips, when a variable or the stack is missing

* *GIVEN* one of the four variables is unset or empty, or the local Exasol is not running
* *WHEN* the suite runs
* *THEN* the suite SHALL fail, naming the missing variable and never its value, or naming the unavailable stack
* *AND* a missing variable SHALL fail the suite before the harness creates any cloud resource

### Scenario: The Make target and the CI job run the suite as a non-release gate

* *GIVEN* the repository's `Makefile`, `.github/workflows/ci.yml`, and `test.env.example`
* *WHEN* a developer runs `make test-e2e-glue` or CI runs the `e2e-glue` job
* *THEN* `test-e2e-glue` SHALL depend on `cross-udf-build`, SHALL source `./test.env` when present, and SHALL run `cargo test --features glue-e2e --test e2e_glue_test -- --test-threads=1` on the same recipe line
* *AND* the `e2e-glue` job SHALL need only `build-so`, SHALL read the two secrets and the two variables, and SHALL NOT appear in `release`'s needs
* *AND* a pull request from a fork, which receives no secrets, SHALL fail the job loudly, as for the Azure gate
* *AND* `test.env.example` SHALL list each of the four variables as `<NAME>=placeholder`

### Scenario: No credential value appears in output

* *GIVEN* a run whose CONNECTION DDL or query fails
* *WHEN* the suite reports the failure
* *THEN* no test output, panic message, or log line SHALL contain the secret access key or the session token
