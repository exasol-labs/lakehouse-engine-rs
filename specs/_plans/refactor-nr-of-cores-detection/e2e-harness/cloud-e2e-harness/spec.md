# Feature: Cloud E2E Harness (Glue + SigV4)

Unchanged by this plan, and reproduced here only because the delta validator requires a description. The recorded text stands as written in `specs/e2e-harness/cloud-e2e-harness/spec.md`.

## Background

* Unchanged by this plan. This section is unmarked, so `speq record` ignores it and the recorded Background survives intact. See `specs/e2e-harness/cloud-e2e-harness/spec.md`.

## Scenarios

<!-- DELTA:REMOVED -->
### Scenario: Remote bench wires NR_OF_CORES and PARALLELISM_FACTOR into the virtual schema

* *GIVEN* the remote bench target running against a real Glue catalog and external Exasol cluster
* *AND* `BENCH_NR_OF_CORES` and `BENCH_PARALLELISM_FACTOR` set in the bench environment
* *WHEN* the bench harness builds the `CREATE VIRTUAL SCHEMA` statement for the remote target
* *THEN* the harness SHALL pass `NR_OF_CORES` and `PARALLELISM_FACTOR` as virtual-schema properties on the remote target, just as the docker target already does
* *AND* the property values SHALL come from `BENCH_NR_OF_CORES` and `BENCH_PARALLELISM_FACTOR`, applying the same defaults the docker path uses when those variables are unset
* *AND* the remote path MUST NOT emit an empty extra-properties block that drops these parallelism knobs (the prior behaviour where the cluster ran at its built-in defaults)
<!-- /DELTA:REMOVED -->

<!-- DELTA:NEW -->
### Scenario: Remote bench wires PARALLELISM_FACTOR into the virtual schema

* *GIVEN* the remote bench target running against a real Glue catalog and external Exasol cluster
* *AND* `BENCH_PARALLELISM_FACTOR` set in the bench environment
* *WHEN* the bench harness builds the `CREATE VIRTUAL SCHEMA` statement for the remote target
* *THEN* the harness SHALL pass `PARALLELISM_FACTOR` as a virtual-schema property on the remote target, just as the docker target already does, taking the value from `BENCH_PARALLELISM_FACTOR` and applying the same default the docker path uses when that variable is unset, and MUST NOT emit an empty extra-properties block that drops it
* *AND* the harness MUST NOT pass an `NR_OF_CORES` virtual-schema property on either target, and MUST NOT read a `BENCH_NR_OF_CORES` variable, because the adapter no longer reads that property; the harness's offline self-check SHALL assert the extra-properties block against that shape, so a stale `NR_OF_CORES` expectation fails the check rather than passing silently
* *AND* the remote run SHALL therefore size its per-instance thread and connection budgets from the cluster node's own detected core count, which is a behavior change from the prior fixed override and has no remote-side equivalent lever
<!-- /DELTA:NEW -->

<!-- DELTA:CHANGED -->
### Scenario: Remote bench selects its catalog backend from the bench environment

* *GIVEN* the remote bench target running against an external Exasol cluster
* *AND* the catalog-selection variable is unset in the bench environment
* *WHEN* the harness resolves its catalog configuration
* *THEN* the harness SHALL take the Glue path, requiring the same variable set, building the same catalog URI and CONNECTION password JSON, and emitting the same virtual-schema properties as it did before this delta, so an existing remote run is unchanged
* *AND* when the variable names Lakekeeper, the harness SHALL require the Lakekeeper catalog URI, warehouse name, OAuth2 client id, client secret, and token endpoint, and SHALL fail naming any one of them that is empty
* *AND* when the variable names Lakekeeper, the CONNECTION address SHALL be the Lakekeeper catalog URI and the CONNECTION password SHALL be built by a Lakekeeper-specific builder, not by the Glue builder with different arguments
* *AND* when the variable holds any other value, the harness SHALL exit with an error naming the accepted values, rather than silently falling back to either catalog
* *AND* the docker target's behavior MUST NOT change under any value of that variable, because the local stack has exactly one catalog
* *AND* both remote arms SHALL keep passing `PARALLELISM_FACTOR` as a virtual-schema property from the same bench variable with the same default, per this feature's recorded remote-parallelism scenario
* *AND* the benchmark report header SHALL name the catalog the run used, so two reports over the same data are distinguishable after the fact — this is the ONLY report-output change on the Glue arm, whose required variables, catalog URI, CONNECTION password, virtual-schema properties, query set, and row counts all stay unchanged
* *AND* that header field SHALL carry the catalog NAME only and MUST NOT carry an `s3://`-shaped value, because `bench/import_ceiling.sh:29` greps the whole report file with `grep -oE 's3://[^"]*/lineitem'` to derive its table root, so an `s3://`-shaped header value poisons that downstream script
* *AND* that header field SHALL be emitted on the REMOTE target ONLY, and the DOCKER target's report header SHALL carry no such field under any value of the catalog-selection variable, including unset. `bench/run.sh` writes ONE header block for every target, so the field MUST be emitted conditionally rather than unconditionally: the selection variable defaults to `glue`, and the local stack's catalog is neither Glue nor the deployed Lakekeeper, so an unconditional field would label a local run `catalog=glue` and write that false value into `bench/reports/*.txt`
<!-- /DELTA:CHANGED -->
