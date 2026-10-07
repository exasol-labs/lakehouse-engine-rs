# Feature: Remote Benchmark Harness

## Background

`bench/run.sh` runs the benchmark query set against the local Docker stack (`BENCH_TARGET=docker`) or against an external Exasol cluster in AWS (`BENCH_TARGET=remote`). An operator also uses the remote target for a live demo: the demo queries the virtual schema a benchmark run leaves behind. Benchmark and demo differ only in who issues the commands. No setting selects between them.

## Scenarios

### Scenario: The remote target selects and configures its catalog

* *GIVEN* the remote target of `bench/run.sh`
* *WHEN* an operator runs it with a `BENCH_CATALOG` value
* *THEN* the following SHALL hold: The remote target selects its catalog from `BENCH_CATALOG`. Unset or `glue` takes the Glue path with the same required variables, catalog URI, CONNECTION password, and virtual schema properties as before the selector existed. `lakekeeper` takes the Lakekeeper path. Any other value exits with an error that names the accepted values. The docker target ignores the variable, because the local stack has one catalog.
* *AND* the following SHALL hold: The Lakekeeper path requires the Lakekeeper catalog URI, warehouse name, OAuth2 client id, client secret, and token endpoint, and fails naming the first empty one.
* *AND* the following SHALL hold: Each catalog has its own CONNECTION password builder, because the adapter rejects a CONNECTION that combines `use_sigv4` with `client_id` or `client_secret`. The Lakekeeper password carries the warehouse name, the OAuth2 client id, client secret, and token endpoint, plus the static S3 endpoint, region, access key, and secret key. It sets neither the SigV4 flag nor the vended-credentials flag. Both builders double every single quote in an embedded value before the JSON reaches a SQL string literal.
* *AND* the following SHALL hold: The Lakekeeper path sets `ALLOW_HTTP` on the virtual schema, because the in-VPC Lakekeeper and Keycloak endpoints are plain HTTP. The Glue path's extra-properties block carries no `ALLOW_HTTP`.

### Scenario: Both remote paths pass tuning properties and name the catalog

* *GIVEN* a remote benchmark run
* *WHEN* the harness creates the virtual schema and writes the report
* *THEN* the following SHALL hold: Both remote paths pass `PARALLELISM_FACTOR` as a virtual schema property from `BENCH_PARALLELISM_FACTOR`, with the same default the docker target uses, and never emit an empty extra-properties block. The harness passes no `NR_OF_CORES` property and reads no `BENCH_NR_OF_CORES` variable, because the adapter detects the core count on its executing node. The remote run sizes its per-instance thread and connection budgets from that detected count.
* *AND* the following SHALL hold: On the remote target only, the report header names the catalog the run used. The field carries the catalog name and never an `s3://`-shaped value, because `bench/import_ceiling.sh` greps the report for `s3://.../lineitem`. The docker target's header carries no such field.

### Scenario: The harness leaves the virtual schema and CONNECTION queryable for a demo

* *GIVEN* a benchmark run that ends
* *WHEN* an operator then runs a demo against the virtual schema
* *THEN* the following SHALL hold: The harness drops the virtual schema only as the first half of a drop-then-create pair at the start of a run. It never drops the virtual schema at the end of a run and never drops the CONNECTION, so both stay queryable for a demo. The offline self-check asserts this against the source text of `bench/run.sh` only.
* *AND* the following SHALL hold: That guarantee covers the harness, not the operator wrapper. `deploy/scripts/bench-remote.sh` runs `deploy/scripts/cluster-down.sh <env>` on every exit unless `KEEP_ALIVE=1` is exported, which destroys the cluster and with it the CONNECTION and the virtual schema. A live demo therefore never uses a default `bench-remote.sh` run.
* *AND* the following SHALL hold: `deploy/README.md` states the full ordered demo runbook in two forms, both with `BENCH_CATALOG=lakekeeper` set explicitly and both running `deploy/scripts/lakekeeper-up.sh <env>` before any `secrets.sh <env>`. Wrapper form: `lakekeeper-up.sh <env>`, then `BENCH_CATALOG=lakekeeper KEEP_ALIVE=1 ./bench-remote.sh <env>`. Unwrapped form: the `cluster-stack` `tofu apply`, then `lakekeeper-up.sh <env>`, `cluster-up.sh <env>`, `secrets.sh <env>`, and `BENCH_CATALOG=lakekeeper make bench`. The runbook ends with `cluster-down.sh <env>` and `lakekeeper-down.sh <env>`, because both forms leave a running, billing cluster.

### Scenario: The offline self-check guards the extra-properties block

* *GIVEN* the offline self-check of the harness
* *WHEN* it runs
* *THEN* the following SHALL hold: The offline self-check asserts the extra-properties block of each path and prints no client secret, access key, or secret key.
