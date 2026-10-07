# Feature: AWS Lakekeeper Benchmark Catalog

## Background

An opt-in, ephemeral Lakekeeper Iceberg REST catalog in the AWS benchmark environment serves the TPC-H tables that Glue already catalogs, by reference. OpenTofu and bash deploy and provision it. It adds no engine, adapter, CONNECTION field, or virtual schema property, and no Rust code. With no new variable set, every existing `deploy/` and `bench/` path behaves as before.

## Scenarios

### Scenario: The Lakekeeper stack is separate and ephemeral

* *GIVEN* an operator on a `data-stack` environment
* *WHEN* the operator runs `lakekeeper-up.sh` and later `lakekeeper-down.sh`
* *THEN* the following SHALL hold: `deploy/scripts/lakekeeper-up.sh <env>` applies a separate OpenTofu stack layered on `data-stack`, modeled on `deploy/trino-stack/`. It reads `data-stack` values only and no `cluster-stack` output, so it can run before the cluster exists. No `data-stack`, `cluster-stack`, or `trino-stack` apply creates, changes, or destroys it.
* *AND* the following SHALL hold: The stack creates one EC2 instance in the `data-stack` subnet, named with the `<project>-<env>-lakekeeper` prefix and carrying the shared `exa:*` default tags. Its user data runs PostgreSQL, Keycloak, a run-once Lakekeeper `migrate`, and Lakekeeper `serve`, mirroring `docker-compose.lakekeeper.yml`, and reads the `data-stack` S3 bucket.
* *AND* the following SHALL hold: The security group admits SSH from the operator allowlist only, and the Lakekeeper and Keycloak ports from the operator allowlist and the VPC CIDR. The allowlist defaults to the apply machine's public IP as a `/32`, resolved at apply time.
* *AND* the following SHALL hold: The script waits for the Lakekeeper health endpoint before provisioning, then prints the connection details and a cost-and-teardown reminder that names `lakekeeper-down.sh <env>`.
* *AND* the following SHALL hold: The stack needs no change to `deploy/iam/deployer-policy.json`, because every resource it creates is wildcard-permitted or carries the `<project>-*` prefix.
* *AND* the following SHALL hold: The stack publishes every non-secret connection value under its own SSM root as a `String` parameter (warehouse name, OAuth2 client id, catalog and token URIs for both vantages) and the OAuth2 client secret as a `SecureString`. It also exposes the same values as outputs. Outputs and parameters never diverge, and the stack generates no second copy of a value.
* *AND* the following SHALL hold: `deploy/scripts/lakekeeper-down.sh <env>` destroys that environment's Lakekeeper workspace only: the instance, its security group, its IAM user and access key, and its SSM parameters. It never touches the `data-stack` bucket, the Glue catalog, the Exasol cluster, or the Trino stack. A later `lakekeeper-up.sh` yields a working catalog again from a clean box.

### Scenario: OIDC tokens validate across both network vantages

* *GIVEN* the Lakekeeper box with a public and a private IP
* *WHEN* the UDF and the operator each obtain a token
* *THEN* the following SHALL hold: The box has a public IP for the operator and a private IP for the cluster. Keycloak stamps the token issuer from the request host. Lakekeeper's primary OIDC provider names the private-IP issuer, which the UDF's token carries. Its additional issuers name the public-IP issuer, which the operator's provisioning token carries.
* *AND* the following SHALL hold: The realm (`iceberg`), client id (`lakehouse`), client secret, and audience (`lakekeeper`) are the values in `scripts/keycloak-realm-iceberg.json`, the same ones the local Lakekeeper suite uses. The stack never generates a second client secret. That committed secret is a named, accepted seam whose only control is the security group, and `deploy/README.md` § Known seams names it.
* *AND* the following SHALL hold: The Keycloak health gate tests the imported realm: it succeeds only once `/realms/iceberg/.well-known/openid-configuration` returns a body containing `jwks_uri`.
* *AND* the following SHALL hold: The stack generates the PostgreSQL password, the Lakekeeper metadata-encryption key, and the Keycloak bootstrap admin password, and stores them as SSM `SecureString` parameters under a Lakekeeper-specific root. None of them is a literal from `docker-compose.lakekeeper.yml`. No generated secret appears in script output or in a non-sensitive OpenTofu output.

### Scenario: The stack's storage credential is write-capable only for Lakekeeper's own warehouse validation

* *GIVEN* the stack's warehouse and the engine's read-only CONNECTION
* *WHEN* the stack applies and the query path runs
* *THEN* the following SHALL hold: The engine's `engine-reader` IAM user is read-only, and Lakekeeper validates a new warehouse by writing, reading, and deleting a probe object. The stack therefore creates its own IAM user, `<project>-<env>-lakekeeper`, with object put, get, and delete plus bucket list on the `data-stack` bucket and nothing else. It builds the user from `aws_iam_user`, `aws_iam_policy`, `aws_iam_user_policy_attachment`, and `aws_iam_access_key`, never from an inline `aws_iam_user_policy`, because the deployer policy lacks `iam:PutUserPolicy`.
* *AND* the following SHALL hold: The write and delete grants are bucket-wide, a named, accepted risk: the warehouse prefix is derived after the apply, so the policy cannot name it.
* *AND* the following SHALL hold: The stack stores that user's key pair as SSM `SecureString` parameters, the only channel to the provisioning script, and destroys the user with the stack. The deployment never disables Lakekeeper's storage validation.
* *AND* the following SHALL hold: The Exasol CONNECTION keeps the read-only `engine-reader` keys, and the warehouse sets `sts-enabled` false, so the query path gains no write permission.
* *AND* the following SHALL hold: The warehouse's `delete-profile` is the soft profile, sent as `{"type": "soft", "expiration-seconds": 604800}`. It defers file removal and does not prevent it, so it is the secondary control. The primary control is that `deploy/scripts/lakekeeper-provision.sh` contains no destructive verb: no HTTP `DELETE` (`-X DELETE`, `--request DELETE`), no `purgeRequested`, and no `aws s3 rm`, `aws s3api delete-object`, or `aws s3api delete-objects`. A source-text check scans that one script. `lakekeeper-down.sh` and `deploy/scripts/tests/lakekeeper-local.test.sh` are out of its scope.
* *AND* the following SHALL hold: The local verification script is the single permitted exception: it drops its own throwaway source tables without purging, never against AWS, and registers a non-colliding table pair as a positive control.
* *AND* the following SHALL hold: The provisioning hop to Lakekeeper and Keycloak is plain HTTP, a named, accepted seam recorded as the fourth entry of `deploy/README.md` § Known seams. From the operator's laptop, the warehouse body with the write-and-delete key pair, the OAuth2 client secret, and every bearer token cross the internet in cleartext. A captured key stays usable until `lakekeeper-down.sh` destroys the user. Every deployment includes at least one such public-vantage run, because `lakekeeper-up.sh` always provisions from the operator's machine. The security group bounds who can reach the port. It is a reachability control, not a confidentiality control, and no artifact claims otherwise.

### Scenario: The provisioning script handles credentials safely and runs at either site

* *GIVEN* `lakekeeper-provision.sh`
* *WHEN* it runs from an operator machine or an EC2 run site
* *THEN* the following SHALL hold: One script serves both run sites, and `lakekeeper-up.sh` invokes it. It obtains AWS credentials only through the AWS CLI's standard chain. It never passes `--profile`, never reads `~/.aws/credentials` or `~/.aws/config`, and never queries the instance metadata service. It passes an explicit `--region` on every AWS call, because an EC2 instance profile supplies no region. It never invokes `tofu`, `ssh`, or `scp`.
* *AND* the following SHALL hold: `lakekeeper-up.sh` and `lakekeeper-down.sh` run on an operator machine only. An EC2 run site re-provisions by calling the provisioning script directly against a stack already applied. It reads every `LK_SOURCE_*` value from the `data-stack` SSM root and every `LK_TARGET_*` value from this stack's SSM root, and reads no OpenTofu output. It needs an instance profile with `ssm:GetParameter` on both roots, `kms:Decrypt`, `glue:GetTables`, and `s3:GetObject` on the data prefix. The operator supplies that box. No stack in this repository creates it.
* *AND* the following SHALL hold: The target URIs belong to the run site: a caller inside the VPC receives the private-IP URIs, a caller outside receives the public-IP URIs. The script reads them verbatim from `LK_TARGET_*` and never rewrites them.
* *AND* the following SHALL hold: No credential appears in the argv of any process the script spawns, including `curl`, `jq`, and `aws`, because `/proc/<pid>/cmdline` is world-readable. Credentials reach `curl` through a file descriptor. A credential-bearing request body reads the credential from `jq`'s environment (`env.LK_TARGET_SECRET_ACCESS_KEY`, `env.LK_TARGET_ACCESS_KEY_ID`), never from `--arg`. Non-credential values may use `--arg`.
* *AND* the following SHALL hold: Every request body is built with `jq -n` and typed argument flags, never by string interpolation or a heredoc. `set -x` appears nowhere in the script.
* *AND* the following SHALL hold: A response body kept for classification goes to a file under a `mktemp -d` directory removed by an `EXIT` trap, and no error path prints it. An error message names the endpoint, the table or warehouse, and the HTTP status only. No success path echoes a credential.
* *AND* the following SHALL hold: The offline stubbed-PATH harness records the argv of every stubbed command and asserts that no secret appears in any of them. It also asserts each emitted request body's exact structure against the Lakekeeper v0.13.1 shapes. The local Docker verification sends every body to a real Lakekeeper 0.13.1.

### Scenario: The provisioning script bootstraps Lakekeeper and creates one warehouse

* *GIVEN* a Lakekeeper server
* *WHEN* the provisioning script runs
* *THEN* the following SHALL hold: The script obtains a Keycloak token through the client-credentials grant before its first management request. It decides whether to bootstrap from the server-info `bootstrapped` boolean, treating an ambiguous answer as not bootstrapped. The bootstrap request accepts the terms of use, and both success and `409 Conflict` count as bootstrapped.
* *AND* the following SHALL hold: The script creates exactly one warehouse whose S3 profile names the `data-stack` bucket, its region, `sts-enabled` false, the soft delete profile, and the derived key prefix. The profile uses the AWS S3 flavor with no path-style setting when no S3 endpoint is configured, and the S3-compatibility flavor with path-style access when one is. It carries no STS role. The storage credential uses the canonical `access-key-id` and `secret-access-key` field names.
* *AND* the following SHALL hold: Warehouse creation succeeds over a prefix that already holds the source tables' files.
* *AND* the following SHALL hold: Any 2xx, a `409 Conflict`, and a `400 Bad Request` reporting a storage-profile overlap count as already present. After any already-present answer, the script reads the warehouse back and fails, naming expected and returned values, unless the bucket and key prefix equal the derived ones. Any other status fails naming the endpoint, the warehouse, and the status, never the body.
* *AND* the following SHALL hold: A re-run against a provisioned server exits successfully and changes nothing.

### Scenario: The provisioning script registers every source table by reference

* *GIVEN* a source namespace of Iceberg tables
* *WHEN* the provisioning script registers them
* *THEN* the following SHALL hold: The script enumerates the source namespace (the Glue database at SSM `/<project>/<env>/namespace/tpch` on AWS) instead of a fixed table list, and normalizes every table to `(name, metadata_location, table_location)`. `LK_SOURCE_KIND=glue`, the default, reads the source with `aws glue get-tables`. `LK_SOURCE_KIND=rest` reads it from an OAuth2-bearer Iceberg REST catalog, so the local verification drives the same downstream code. The Glue Iceberg REST endpoint is not used, because `curl --aws-sigv4` cannot send the session token an instance profile needs.
* *AND* the following SHALL hold: The script fails naming a table that has no `metadata_location`. It takes each table's root from the `location` field inside the metadata document.
* *AND* the following SHALL hold: The script derives the warehouse bucket and key prefix so that every table's metadata location and recorded root are strict sublocations of `s3://<bucket>/<key-prefix>`, shortening a prefix equal to a table root to its parent, because Lakekeeper rejects a location equal to the warehouse base. It fails naming the tables when two tables sit in different buckets, and fails naming the bucket when the derived prefix would be empty.
* *AND* the following SHALL hold: The script creates the target namespace before registering, treating already-exists as success, and fails naming the namespace when it is one Lakekeeper reserves (`system`, `examples`, `information_schema`).
* *AND* the following SHALL hold: For each table, the script sends one register-table request (`POST {catalog-base}/v1/{prefix}/namespaces/{namespace}/register`) with the metadata location verbatim, the source name byte-identical, and `overwrite` set explicitly to `false`. It writes, copies, or rewrites no metadata, manifest, or data file.
* *AND* the following SHALL hold: An already-registered table counts as success. The script confirms every fresh `2xx` and every unidentified `409` with a `loadTable` read-back of the metadata location, and treats a mismatch as a distinct failure, because Lakekeeper 0.13.1 answers a location conflict and an already-registered re-run with the same `409` body. Response-text matching may be logged and is never the sole basis for the outcome.
* *AND* the following SHALL hold: The script prints a per-table summary of registered, already-present, and failed tables, and exits non-zero when any table failed.
* *AND* the following SHALL hold: `--source-only` runs enumeration and normalization, prints each triple, and exits. It sends no target request and writes nothing, and exits non-zero naming a table without `metadata_location`. Any other argument is rejected.
* *AND* the following SHALL hold: The TPC-H `part`/`partsupp` location shape registers without error on Lakekeeper 0.13.1. The local verification registers that pair in both orders as a regression test.

### Scenario: Bench secrets gain the Lakekeeper block only when a workspace exists

* *GIVEN* `deploy/scripts/secrets.sh <env>`
* *WHEN* it runs
* *THEN* the following SHALL hold: `deploy/scripts/secrets.sh <env>` keeps the existing Glue, AWS, and Exasol variables in `bench/.env` unchanged. When a Lakekeeper workspace exists for the same environment, it adds the Lakekeeper catalog URI, warehouse name, OAuth2 client id, client secret, and token endpoint, with every URI built from the box's private IP. It never sets `BENCH_CATALOG`. Without a Lakekeeper workspace, it omits the block, prints a note, and exits successfully. The file keeps owner-only permissions, and no secret is echoed.
