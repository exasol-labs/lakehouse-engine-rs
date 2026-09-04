# Feature: AWS Lakekeeper Perf Catalog

Adds an opt-in, ephemeral Lakekeeper Iceberg REST catalog to the AWS perf-test environment, holding the already-loaded TPC-H Iceberg tables by reference, deployed and provisioned entirely by OpenTofu and bash.

## Background

* The engine already supports Lakekeeper: Iceberg REST plus OAuth2 client-credentials, documented in
  `docs/catalogs.md` § "Lakekeeper (OIDC via Keycloak + MinIO)" and exercised by
  `lakekeeper-e2e/lakekeeper-e2e-harness` against a local Docker stack. This feature adds **no** engine,
  adapter, catalog-crate, CONNECTION-field, or virtual-schema-property change, and adds no Rust code
  of any kind. It is deployment, provisioning, and bench wiring only.
* Three run contexts exercise the engine, and this feature serves the last two. **e2e** runs against
  local Docker in CI and is out of scope — `crates/lakehouse-engine/tests/e2e_lakekeeper_test.rs` and
  `crates/lakehouse-engine/tests/common/lakekeeper.rs` are untouched. **benchmark** is an automated,
  unattended performance run against the AWS Exasol cluster. **demo** is an interactive run against
  that same cluster, driven by an operator during a live customer session. Benchmark and demo share
  ONE AWS stack, ONE provisioning path, ONE warehouse, and ONE namespace; they differ only in who
  issues the commands and what happens afterwards, so nothing in this feature branches on which one
  is running and no suite selector exists.
* Glue stays the default. This feature is ADDITIVE: with no new environment variable set, every
  existing `deploy/` and `bench/` path keeps today's required variables, catalog URI, CONNECTION
  password, virtual-schema properties, query set, and row counts. The one report-output change — a
  `catalog=` field on the benchmark report header, present on both arms — belongs to the catalog
  toggle, which `e2e-harness/cloud-e2e-harness` owns. This feature owns the AWS-side deployment and
  the provisioning script that fills it.
* The lifecycle model is **ephemeral**, mirroring `deploy/trino-stack/`: a separate OpenTofu stack
  layered on the persistent `data-stack`, created and destroyed only by an explicit `*-up.sh` /
  `*-down.sh` run, never by another stack's apply, and carrying the same cost/teardown banner.
* Provisioning is a THIRD script, `deploy/scripts/lakekeeper-provision.sh`, called by
  `lakekeeper-up.sh` rather than folded into it. The two have different credential requirements —
  the lifecycle script needs OpenTofu and deployer IAM, the provisioning script needs only Glue and
  S3 reads plus network reach to Lakekeeper — and the source-only pre-flight below MUST run with no
  stack applied at all, which a script beginning with `tofu apply` cannot offer.
* The container set mirrors `docker-compose.lakekeeper.yml` exactly — PostgreSQL, Keycloak importing
  `scripts/keycloak-realm-iceberg.json`, and Lakekeeper (`migrate` then `serve`) — so the realm
  (`iceberg`), client id (`lakehouse`), client secret, and audience (`lakekeeper`) the CONNECTION uses
  on AWS are the same ones the local E2E suite already proves. Keycloak is included rather than
  skipped, because the customer's own deployment is OIDC-secured and the local stack's contract is
  the one already under test.
* Registration is by reference, not by copy. The Iceberg REST Catalog API's register-table operation
  records an existing `metadata.json` location in the target catalog. No Parquet file, manifest, or
  metadata file is written, read back, or rewritten, so the two catalogs address one physical copy of
  the TPC-H data on S3.
* Lakekeeper serves that operation at `POST {catalog-base}/v1/{prefix}/namespaces/{namespace}/register`,
  where `{catalog-base}` is the `/catalog`-suffixed base URI the CONNECTION already uses and
  `{prefix}` is the warehouse prefix from `GET {catalog-base}/v1/config?warehouse=<name>`. The request
  body carries `name`, `metadata-location`, and an `overwrite` flag, kebab-case. Verified against the
  v0.13.1 source tree: the route is registered unconditionally, with no feature flag and no
  enable/disable configuration, and it has been implemented since 0.6.1.
* Lakekeeper enforces TWO independent location checks on every register call, and neither can be
  relaxed by configuration in v0.13.1: the submitted `metadata-location` MUST be a strict sublocation
  of the warehouse's `s3://<bucket>/<key-prefix>`, AND so must the `location` recorded inside that
  metadata document. Equality with the warehouse base fails — the location must be strictly below it.
  This is what forces the warehouse's bucket and prefix to be derived from the tables rather than
  configured.
* The source catalog is read through the AWS CLI, not through Glue's Iceberg REST endpoint. The
  alternative, `curl --aws-sigv4` against `https://glue.<region>.amazonaws.com/iceberg`
  (`deploy/data-stack/main.tf:7`), is rejected on a hard constraint rather than on taste: it requires
  `--user <access-key>:<secret-key>` and emits no `x-amz-security-token` header, so it cannot use the
  temporary credentials an EC2 instance profile supplies, which breaks the run-from-either-location
  requirement outright. `aws glue get-tables` signs every request through the CLI's own credential
  chain and therefore works unchanged from both run sites.
* The source read normalizes every table to one triple — `(name, metadata_location, table_location)`
  — and nothing downstream knows how that triple was obtained. Two producers exist. `LK_SOURCE_KIND=glue`
  is the default and the AWS path. `LK_SOURCE_KIND=rest` reads the same triple from an OAuth2-bearer
  Iceberg REST `loadTable`, and exists so the local Docker verification drives the identical
  derivation, warehouse, and registration code the AWS run uses; without it the whole target half
  would be unverifiable off the billable path.
* `bench/make_deletes_docker.sh:36` already drives an Iceberg REST catalog from bash with plain
  `curl`, and `secrets.sh`, `cluster-up.sh`, and `trino-up.sh` all already require `jq`, so this
  feature introduces no new host dependency. The "Bash 3.2+, no jq" constraint at
  `deploy/scripts/install.sh:17` belongs to that script alone, because end users fetch it with
  `curl | bash`; it does not govern operator-facing tooling in `deploy/scripts/`.
* Bash has no compile-time JSON-shape checking, which the earlier Rust design got for free from
  `lakehouse-catalog`'s `pub` request types. Three controls replace the compiler and are normative in
  the scenarios below: every request body is built with `jq -n --arg` rather than string
  interpolation, so a malformed body is impossible and escaping is correct; the offline stubbed-PATH
  harness captures each emitted body and asserts its exact structure against the v0.13.1 shapes; and
  the local Docker verification sends every body to a real Lakekeeper 0.13.1, which rejects a wrong
  shape.
* Bash also reaches destructive verbs the Rust design could not: `aws s3 rm` and `aws s3api
  delete-object` sit on the same PATH as the read calls. The no-destructive-path rule below therefore
  covers the AWS CLI as well as HTTP `DELETE`.
* The local verification MUST drop each throwaway source table from the source namespace WITHOUT
  purging its files before registering it into the target namespace. The reason is experiment design:
  a live table still holding the location under test would make the assertion about exact-location
  reuse rather than about the location rule itself. A purge-drop MUST NOT be used even here, so the
  drop's purge switch stays `false`. This drop is the SINGLE permitted exception to the
  no-destructive-path rule stated in the storage-credential scenario below: it lives in the test
  script `deploy/scripts/tests/lakekeeper-local.test.sh`, not in
  `deploy/scripts/lakekeeper-provision.sh`, it acts only on tables the verification itself created,
  and it never runs against AWS. The verification MUST also register a NON-colliding table pair
  through the same path as a positive control, so a blanket failure cannot be mistaken for the
  specific collision.
* The `part`/`partsupp` location collision does NOT reproduce against Lakekeeper 0.13.1, and this
  feature therefore carries no mitigation for it. Lakekeeper enforces non-overlapping table locations
  within a warehouse, and an upstream issue reports `LocationAlreadyTaken` when one table's location
  is a non-slash-delimited prefix of another's — TPC-H's `part` and `partsupp` are exactly that shape.
  A live run settled it with the production location shape rather than a synthetic one. That run's
  conditions were: Lakekeeper 0.13.1, the local Docker stack, MinIO as the object store — therefore
  the S3-COMPATIBILITY storage flavor with path-style addressing — and the registration order `part`
  then `partsupp`. An Iceberg REST catalog derived `s3://warehouse/tpch_src/part` and
  `s3://warehouse/tpch_src/partsupp` from its own default location rule, a Lakekeeper warehouse was
  created over that same already-populated `tpch_src` prefix, and both tables registered by reference
  into it — HTTP `200` each, `partsupp` immediately after `part`, both listed afterwards. Those are
  catalog-API facts, independent of whether the caller is Rust or bash. The local verification keeps
  that pair as a permanent regression test and MUST register it in BOTH orders, because the run
  exercised only one, so a future Lakekeeper version that reintroduces the rejection fails on a
  laptop rather than on a billable cluster.
* Two URI vantages exist for one deployment and MUST NOT be conflated. The vantage is fixed by WHERE
  A CALLER RUNS, not by which script it is. Three callers exist: the operator's laptop reaches the
  box by public IP; the Exasol UDF reaches it by private IP from inside the VPC, the same constraint
  `deploy/trino-stack/outputs.tf:10-13` records for in-VPC JDBC; and an EC2 caller running
  `lakekeeper-provision.sh` inside the VPC takes the private-IP vantage for the same reason the UDF
  does. Keycloak stamps the token's `iss` from the request host, so both issuers must be accepted.
* This feature touches no scanning, pushdown, or schema/type-handling code, so the Iceberg and Delta
  specification-compliance obligation in `CLAUDE.md` yields no normative clause to quote here beyond
  the register-table wire contract cited above.

## Scenarios

### Scenario: An ephemeral Lakekeeper stack stands up in the cluster's VPC

* *GIVEN* the persistent `data-stack` is applied, publishing a VPC id, subnet id, and S3 bucket
* *AND* the `cluster-stack` Exasol cluster for the same environment either already runs in that VPC
  or is applied later — this stack reads `data-stack` values ONLY and no `cluster-stack` output, so
  its apply does not depend on the cluster's existence. That is what lets the demo runbook's wrapper
  form run `lakekeeper-up.sh <env>` before `bench-remote.sh` applies the cluster
* *WHEN* the operator runs `deploy/scripts/lakekeeper-up.sh <env>`
* *THEN* the stack SHALL create exactly one EC2 instance in the `data-stack` subnet, named with the
  `<project>-<env>-lakekeeper` prefix and carrying the same `exa:*` default-tag block every other
  stack in `deploy/` applies
* *AND* the instance SHALL run four containers from its user-data — PostgreSQL, Keycloak, a run-once
  Lakekeeper `migrate`, and Lakekeeper `serve` — reading the `data-stack` S3 bucket rather than MinIO
* *AND* the security group SHALL admit SSH from the operator allowlist ONLY, and SHALL admit the
  Lakekeeper port and the Keycloak port from the operator allowlist AND the VPC CIDR, because the
  Exasol UDF connects from the cluster nodes rather than from the operator's machine
* *AND* that allowlist SHALL default to the apply machine's own public IP `/32`, resolved at apply
  time, rather than to any wider range, matching `deploy/cluster-stack/main.tf:8-19`
* *AND* the stack MUST NOT be created, modified, or destroyed by any `data-stack`, `cluster-stack`, or
  `trino-stack` apply, so no other workflow can start a billable Lakekeeper box implicitly
* *AND* the script SHALL wait for Lakekeeper to answer its health endpoint before provisioning, and
  SHALL print the connection details plus an explicit cost-and-teardown reminder naming
  `lakekeeper-down.sh <env>`
* *AND* the stack SHALL require no change to `deploy/iam/deployer-policy.json`, because every
  resource it creates is either already wildcard-permitted or named with the `<project>-*` prefix
  that policy's IAM statement already covers
* *AND* the stack SHALL ALSO publish every non-secret connection value under its own SSM root as a
  plain `String` parameter — the warehouse name, the OAuth2 client id, and the catalog and token URIs
  for BOTH vantages — and the OAuth2 client secret as a `SecureString` parameter, so that a caller
  holding no OpenTofu workspace state can assemble a complete `LK_TARGET_*` environment from SSM
  alone per the run-site scenario below. `String` rather than `SecureString` is correct for the first
  set: a URI, a warehouse name, and a public OAuth2 client id are not secrets
* *AND* those parameters SHALL be a PUBLICATION of values the stack already computes or reads, never
  a second generated copy: the client id and client secret are copied verbatim from
  `scripts/keycloak-realm-iceberg.json`, which stays their single owner per the Keycloak scenario
  below, and the same values are exposed as stack outputs for callers that do hold the workspace
  state. The outputs and the parameters MUST NOT diverge

### Scenario: Keycloak issues tokens both issuers accept

* *GIVEN* the deployed box has one public IP the operator's laptop reaches and one private IP the
  Exasol cluster nodes reach
* *WHEN* the stack renders Lakekeeper's OIDC configuration
* *THEN* Lakekeeper's primary OIDC provider URI SHALL name the PRIVATE-IP Keycloak issuer, because
  that is the issuer stamped into the token the UDF obtains at query time
* *AND* its additional-issuers setting SHALL name the PUBLIC-IP Keycloak issuer, so the operator-side
  provisioning script's token is accepted by the same server
* *AND* the realm name, OAuth2 client id, client secret, and audience SHALL be the values
  `scripts/keycloak-realm-iceberg.json` already defines for the local Docker stack, so the AWS
  CONNECTION carries the same field set `lakekeeper-e2e/lakekeeper-e2e-harness` already proves and
  introduces no new CONNECTION field
* *AND* the Keycloak health gate SHALL test the IMPORTED realm rather than Keycloak liveness alone,
  mirroring the `/dev/tcp` probe at `docker-compose.lakekeeper.yml:53-65`, which succeeds only once
  `/realms/iceberg/.well-known/openid-configuration` returns a body containing `jwks_uri`
* *AND* the PostgreSQL password, the Lakekeeper metadata-encryption key, and the Keycloak bootstrap
  admin password SHALL be generated by the stack and stored as SSM `SecureString` parameters under a
  Lakekeeper-specific SSM root, following the pattern `deploy/cluster-stack` already uses for its own
  passwords
* *AND* the Keycloak bootstrap admin credential MUST NOT be the literal `admin` / `admin` that
  `docker-compose.lakekeeper.yml:44-45` sets for the local stack, and the metadata-encryption key
  MUST NOT be the literal `This-is-NOT-Secure!` that file sets at lines 96 and 111, because this box
  carries a public IP and its Keycloak port is deliberately opened to the operator allowlist as well
  as the VPC CIDR
* *AND* the OAuth2 client secret SHALL remain the repo-committed value in
  `scripts/keycloak-realm-iceberg.json`, recorded as a NAMED, ACCEPTED SEAM whose only control is the
  security group — the stack SHALL NOT overwrite it with a generated value, because the CONNECTION
  contract `lakekeeper-e2e/lakekeeper-e2e-harness` proves is defined by that file and a second generated
  copy would give one value two owners
* *AND* `deploy/README.md` § Known seams SHALL name that client-secret seam explicitly, so it is an
  accepted risk on the record rather than an oversight
* *AND* no generated secret SHALL appear in any script's standard output or in an OpenTofu
  non-sensitive output value

### Scenario: The catalog's storage credential is separate from the engine's read-only credential

* *GIVEN* the `data-stack` `engine-reader` IAM user, whose policy carries two statements
  (`deploy/data-stack/main.tf:100-126`) — a `GlueRead` statement with seven read-only Glue actions on
  `Resource = "*"`, and an `S3Read` statement granting `GetObject`, `ListBucket`, and
  `GetBucketLocation` on the warehouse bucket — and therefore no S3 write or delete permission at all
* *AND* Lakekeeper validates a warehouse's storage access at creation by writing, reading back, and
  deleting a probe object under a random path inside the warehouse's own prefix, then asserting that
  the probe's own random path is empty — so warehouse creation fails outright on a read-only credential
* *WHEN* the Lakekeeper stack is applied
* *THEN* the stack SHALL create its own IAM user named with the `<project>-<env>-lakekeeper` prefix,
  whose policy grants exactly the actions that probe needs — object put, get, and delete, plus bucket
  list for the recursive cleanup — scoped to the `data-stack` bucket and its objects and to no other
  resource
* *AND* that user SHALL be built from `aws_iam_user`, `aws_iam_policy`, `aws_iam_user_policy_attachment`,
  and `aws_iam_access_key`, mirroring `deploy/data-stack/main.tf:94-136`; an inline
  `aws_iam_user_policy` MUST NOT be used, because `deploy/iam/deployer-policy.json` §
  `IamForEngineReaderAndInstanceProfiles` grants `iam:CreateUser`, `iam:CreatePolicy`,
  `iam:AttachUserPolicy`, and `iam:CreateAccessKey` but NOT `iam:PutUserPolicy`, so an inline policy
  fails with AccessDenied at apply time after the EC2 instance is already billing
* *AND* the write and delete grants SHALL be bucket-wide, recorded as a NAMED, ACCEPTED RISK rather
  than narrowed: the warehouse key prefix is derived by the provisioning script from the source tables
  AFTER the stack is applied, so the policy cannot name the prefix it covers. Where the
  provisioning-time derivation and the apply-time policy conflict, THE POLICY YIELDS — it stays
  bucket-wide and the derivation is unchanged
* *AND* the created warehouse's `delete-profile` SHALL be the SOFT profile, named explicitly rather
  than left to the server default, because these tables are registered by reference against the one
  physical copy of the TPC-H data and a hard profile puts that copy one purge-drop away from immediate
  deletion
* *AND* that profile SHALL be sent in the exact wire shape Lakekeeper v0.13.1 declares — a tagged
  object `{"type": "soft", "expiration-seconds": <integer>}`, kebab-case, with `604800` (one week)
  as the value, matching upstream's own `tests/migrations/create-warehouse/soft-delete-1week.json`.
  `expiration-seconds` is REQUIRED: the `TabularDeleteProfile::Soft` variant carries no serde default,
  so a request omitting it fails warehouse creation. The local precedent at
  `crates/lakehouse-engine/tests/common/lakekeeper.rs:414` sends the HARD form and is therefore NOT a
  shape to copy
* *AND* the soft profile SHALL be recorded as a DELAY WINDOW rather than a guarantee, because it
  DEFERS file removal instead of preventing it: a soft-profile drop schedules an expiration task for
  `expiration-seconds` in the future and still purges the files when `purgeRequested` was true, and a
  `force` drop replaces the warehouse's soft profile with the hard one outright. The clause below is
  therefore the primary control, and the soft profile is the secondary one
* *AND* `deploy/scripts/lakekeeper-provision.sh` SHALL contain no destructive verb of any kind — no
  HTTP `DELETE` written as `-X DELETE` or `--request DELETE`, no `purgeRequested` parameter, and no
  `aws s3 rm`, `aws s3api delete-object`, or `aws s3api delete-objects` — so no argument, environment
  variable, or error path can reach a destructive operation. The AWS CLI is covered as well as HTTP,
  because bash reaches both from the same PATH
* *AND* that source-text assertion SHALL scan `deploy/scripts/lakekeeper-provision.sh` and no other
  file. `deploy/scripts/lakekeeper-down.sh` is OUT of scope, because destroying the ephemeral stack is
  its entire purpose, and `deploy/scripts/tests/lakekeeper-local.test.sh` is OUT of scope by design,
  because it performs the single permitted non-purging drop described in this feature's Background
* *AND* the deployment MUST NOT disable that validation, because the probe is the only check that the
  credential and prefix actually work, and skipping it defers the same failure to the first
  register call with a worse error
* *AND* the stack SHALL store that user's access key id and secret access key as SSM `SecureString`
  parameters, which are the ONLY channel by which they reach the provisioning script
* *AND* the onward hop from the provisioning script to Lakekeeper is NOT encrypted, and SHALL be
  recorded as a NAMED, ACCEPTED SEAM rather than left unstated. Lakekeeper and Keycloak are reached
  over plain HTTP. From the PUBLIC-IP vantage — the operator's laptop — the warehouse-creation body
  carrying this write-and-delete key pair, the OAuth2 client secret in the token-grant form, and the
  bearer token in every subsequent request all cross the public internet in cleartext. The
  security-group `/32` allowlist restricts which hosts can OPEN a connection; it does not conceal
  traffic in transit from an observer on the path, so it is NOT a CONFIDENTIALITY control for this
  exposure. No TLS termination, no certificate, and no tunnel is added by this feature
* *AND* the accepted bounds on that seam SHALL be stated with it, and stated accurately. The
  TRANSMISSION lasts one provisioning run, while a CAPTURED credential stays usable until
  `lakekeeper-down.sh` destroys the IAM user with the stack — so the credential's lifetime, not the
  transmission window, is the bound that applies to an observer who captured it. That key pair is
  created and destroyed with the ephemeral stack and is scoped to the `data-stack` bucket alone.
  `deploy/README.md` § Known seams SHALL carry this as its fourth entry, naming each of the three
  credential kinds explicitly, so it is an accepted risk on the record rather than an oversight
* *AND* no artifact SHALL state or imply that choosing the in-VPC run site avoids this exposure.
  EVERY deployment carries at least one public-vantage cleartext provisioning run, benchmark
  included: `lakekeeper-up.sh` is an operator-machine script in BOTH contexts per the run-site
  scenario below, it always invokes `lakekeeper-provision.sh`, and that run takes the PUBLIC-IP
  `LK_TARGET_*` URIs because it runs outside the VPC. The IN-VPC vantage is clean only for the
  OPTIONAL re-provision the run-site scenario describes, against a stack already provisioned the
  exposed way
* *AND* the security group SHALL be recorded as the REACHABILITY control that bounds this seam in
  practice, and MUST NOT be recorded as a confidentiality one. `allowed_cidrs` defaults to the apply
  machine's own resolved public IP `/32` per the stack scenario above, so the Lakekeeper and Keycloak
  ports admit that single address plus the VPC CIDR and nothing else on the internet — that bounds
  WHO CAN REACH the plaintext port. It neither encrypts the traffic nor conceals it from an observer
  on the network path BETWEEN an already-allowlisted client and the box
* *AND* the credential SHALL be created and destroyed with the ephemeral stack, so it does not outlive
  the box `lakekeeper-down.sh` removes
* *AND* the Exasol CONNECTION's static S3 fields SHALL keep using the existing read-only
  `engine-reader` key pair, so the query path — the only long-lived consumer — gains no write
  permission on the benchmark bucket
* *AND* the created warehouse SHALL set `sts-enabled` false, so the scan path reads S3 with those
  static read-only keys rather than with credentials vended by the catalog

### Scenario: Provisioning runs unchanged from an operator's laptop and from an EC2 box

* *GIVEN* the demo runs `deploy/scripts/lakekeeper-provision.sh` interactively from an operator's
  laptop, which holds AWS credentials as a named profile or an SSO session
* *AND* the benchmark runs the SAME script unattended from an EC2 box, which holds AWS credentials as
  an IAM instance profile serving temporary credentials with a session token
* *WHEN* the script authenticates to AWS and to Lakekeeper
* *THEN* it SHALL obtain AWS credentials ONLY through the AWS CLI's own standard credential chain, so
  a static profile and an instance profile both resolve with no change to the script
* *AND* it MUST NOT pass `--profile` to any AWS call, MUST NOT read `~/.aws/credentials` or
  `~/.aws/config` itself, and MUST NOT query the instance metadata service at `169.254.169.254` for
  credentials, because each of those hardcodes one of the two run sites
* *AND* it SHALL pass an EXPLICIT `--region` on every AWS call, because an EC2 instance profile
  supplies credentials but no region, so a script relying on a profile's configured region works on a
  laptop and fails on EC2. This is a deliberate departure from `deploy/scripts/secrets.sh:29`, which
  omits `--region` because it only ever runs from an operator's machine
* *AND* it SHALL reach Lakekeeper through the OAuth2 client-credentials grant, which carries no
  location assumption of its own, so only network reachability differs between the two run sites
* *AND* it MUST NOT invoke `tofu`, `ssh`, or `scp`, because those belong to `lakekeeper-up.sh` and
  each would add a prerequisite the benchmark run site is not required to hold
* *AND* the same script SHALL be the one `lakekeeper-up.sh` invokes, so there is exactly one
  provisioning implementation and neither run site can drift from the other
* *AND* this two-run-site guarantee SHALL cover `deploy/scripts/lakekeeper-provision.sh` ONLY.
  `lakekeeper-up.sh` and `lakekeeper-down.sh` are OPERATOR-MACHINE scripts in BOTH contexts,
  benchmark included: they require `tofu`, a deployer-grade IAM principal, and this stack's
  OpenTofu workspace state, none of which any EC2 run site in this feature is required to hold. No
  orchestrator stack, instance profile, or task in this feature makes an EC2 box able to run them,
  and nothing in this feature SHALL claim otherwise
* *AND* the EC2 run site SHALL therefore be exercised by invoking `lakekeeper-provision.sh` DIRECTLY
  against a stack already applied from the operator's machine. Because provisioning is idempotent,
  such a run is a valid re-provision rather than a second deployment
* *AND* that caller SHALL obtain EVERY `LK_SOURCE_*` and `LK_TARGET_*` value from SSM ALONE and MUST
  NOT read any OpenTofu output, because reading an output requires the workspace state the clause
  above says the run site does not hold. `LK_SOURCE_*` comes from the `data-stack` SSM root.
  `LK_TARGET_*` comes from this stack's SSM root: the warehouse name, the OAuth2 client id, and the
  PRIVATE-IP catalog and token URIs as plain `String` parameters per the stack scenario above, and
  the OAuth2 client secret and the storage key pair as `SecureString` parameters
* *AND* that run site SHALL hold an instance profile granting `ssm:GetParameter` on BOTH SSM roots,
  `kms:Decrypt` for the `SecureString` parameters, `glue:GetTables` for the source enumeration, and
  `s3:GetObject` on the data prefix for the metadata-document read. No stack, task, or resource in
  this feature creates such a box or such an instance profile — the run site is a prerequisite the
  operator supplies, exactly as the operator's own laptop is
* *AND* the TARGET URIs SHALL be a property of the RUN SITE rather than of the calling script: a
  caller INSIDE the VPC receives the PRIVATE-IP catalog and token URIs, and a caller OUTSIDE it
  receives the PUBLIC-IP ones, because `deploy/trino-stack/outputs.tf:10-13` records that a same-VPC
  client reaching another instance's public IP routes out through the internet gateway and back,
  which is unreliable and asymmetric
* *AND* the script SHALL read those URIs verbatim from its `LK_TARGET_*` variables and MUST NOT
  derive, rewrite, or vantage-correct them, so the run site's own caller — `lakekeeper-up.sh` for the
  laptop vantage, the EC2 caller for the in-VPC vantage — is the single owner of that choice and the
  script carries no location logic of its own

### Scenario: No credential reaches a process listing, standard output, or an error body

* *GIVEN* the provisioning script handles a Keycloak client secret, a bearer token, and the
  warehouse's S3 access key id and secret access key
* *WHEN* it issues any authenticated request
* *THEN* no credential SHALL appear in the argv of ANY process the script spawns — `curl`, `jq`,
  `aws`, or any other — because `/proc/<pid>/cmdline` is world-readable and the rule protects the
  value, not one command. It MUST NOT appear in a `-u`, `-d`, `--data`, `-H`, or URL-query token, and
  MUST NOT appear in a `jq --arg` or `--argjson` token either
* *AND* every credential SHALL reach `curl` through a FILE DESCRIPTOR — a configuration read from
  standard input, or a request body supplied by process substitution — because a process listing
  exposes argv to every local user
* *AND* the script MUST NOT enable shell tracing: `set -x` SHALL appear nowhere in it, because tracing
  prints every expanded command including credential values
* *AND* every request body SHALL be constructed with `jq -n` and its typed argument flags rather than
  by string interpolation or a heredoc, so a value containing a quote, a backslash, or a newline
  cannot break the body's structure or leak into an adjacent field
* *AND* a CREDENTIAL-BEARING body SHALL take that credential from `jq`'s ENVIRONMENT rather than from
  a `--arg` token — the warehouse body reads its S3 secret as `env.LK_TARGET_SECRET_ACCESS_KEY` and
  its access key id as `env.LK_TARGET_ACCESS_KEY_ID` — because `jq -n --arg secret-access-key
  "$SECRET"` puts the value in jq's own argv and exposes exactly what the `curl` rule above exists to
  prevent. `/proc/<pid>/environ` is owner-restricted where `/proc/<pid>/cmdline` is not, so the
  environment route costs nothing and closes the hole. Non-credential values MAY continue to use
  `--arg`, which keeps the well-formedness guarantee unchanged
* *AND* the offline harness's no-secret-in-argv assertion SHALL cover the recorded argv of EVERY
  stubbed command, not `curl` alone, so this rule is checkable rather than asserted
* *AND* any response body captured for classification SHALL be written to a file under a directory
  created by `mktemp -d` and removed by an `EXIT` trap, and MUST NOT be printed on any error path,
  because the warehouse-creation request carried a storage secret that an error response can echo back
* *AND* every error message SHALL name the endpoint, the table or warehouse, and the HTTP status only
* *AND* no credential SHALL be echoed to standard output on any success path either, including the
  per-table summary

### Scenario: Provisioning bootstraps Lakekeeper and creates the S3-backed warehouse idempotently

* *GIVEN* a freshly started Lakekeeper reachable over plain HTTP at its management endpoint
* *WHEN* the provisioning script runs
* *THEN* it SHALL obtain a bearer token from Keycloak through the OAuth2 client-credentials grant
  BEFORE its first management request, because the server-info endpoint answers `401` to an anonymous
  caller once authentication is enabled
* *AND* it SHALL decide whether to bootstrap from the server-info response's `bootstrapped` boolean —
  never from the presence or absence of a server id, which is always populated — treating an
  ambiguous or unparseable answer as "not bootstrapped" so the request is attempted rather than
  silently skipped
* *AND* the bootstrap request SHALL accept the terms of use, which is its only required field, and the
  script SHALL treat both its success status and a `409 Conflict` as already-bootstrapped success
* *AND* it SHALL create exactly one warehouse whose S3 storage profile names the `data-stack` bucket,
  its region, `sts-enabled` false, an explicit soft `delete-profile`, and the key prefix derived from
  the tables themselves per the registration scenario below
* *AND* warehouse creation SHALL succeed over a key prefix that ALREADY CONTAINS the source tables'
  data and metadata files, because that is the AWS ordering — the TPC-H data is loaded long before this
  warehouse exists. Only the probe object's own random path is required to be empty after validation;
  the warehouse prefix itself is not. Verified live under these conditions: Lakekeeper 0.13.1, the
  local Docker stack, MinIO as the object store — therefore the S3-COMPATIBILITY flavor with path-style
  addressing. Creating a warehouse whose key prefix already held two Iceberg tables' data and metadata
  answered HTTP `201`. The AWS S3 flavor with virtual-hosted addressing is NOT covered by that run and
  is first exercised on the real AWS box
* *AND* that profile SHALL declare the AWS S3 flavor and no path-style setting when no S3 endpoint is
  configured, and the S3-compatibility flavor with path-style addressing when one is, so the local
  verification run and the AWS run differ by exactly that one value
* *AND* the profile MUST NOT carry an STS role identifier, because `sts-enabled` is false and a role
  identifier is required only when AWS-flavored credential vending is on
* *AND* the storage credential SHALL be an access-key credential carrying the dedicated write-capable
  key pair under the canonical `access-key-id` and `secret-access-key` field names, rather than the
  aliased `aws-access-key-id` / `aws-secret-access-key` spellings the local E2E harness happens to use
  at `crates/lakehouse-engine/tests/common/lakekeeper.rs:369-370`
* *AND* it SHALL treat any 2xx, a `409 Conflict`, and a `400 Bad Request` whose body reports a
  storage-profile overlap as warehouse-already-present success — because Lakekeeper 0.13.1 reports a
  duplicate warehouse as a 400 rather than a 409, the same classification
  `crates/lakehouse-engine/tests/common/lakekeeper.rs` records from live observation
* *AND* after ANY already-present classification it SHALL read the warehouse back and FAIL unless the
  returned storage profile's bucket and key prefix equal the derived bucket and the derived key prefix,
  naming both the expected and the returned values. The error Lakekeeper reports is
  `CreateWarehouseStorageProfileOverlap`, which is about OVERLAPPING storage profiles rather than about
  an identical warehouse, and `crates/lakehouse-engine/tests/common/lakekeeper.rs:395-399` records that
  for warehouses sharing a bucket the already-present reading is an unverified inference, so certainty
  requires reading the warehouse back. Without that read-back a shifted prefix or a
  different overlapping warehouse in the same bucket is swallowed as success, and tables are then
  registered into a warehouse whose bucket and prefix the script never confirmed
* *AND* it SHALL fail on any other status, naming the endpoint, the warehouse name, and the status
  code, and MUST NOT include the response body, because that request carried a storage secret
* *AND* re-running the script against an already-provisioned server SHALL exit successfully and change
  nothing, because the box is ephemeral and every `lakekeeper-up.sh` run provisions again

### Scenario: Source-cataloged Iceberg tables are registered into the warehouse without a data rewrite

* *GIVEN* the TPC-H Iceberg tables already loaded on S3 and cataloged in the source namespace the
  bench environment names — the Glue database `data-stack` publishes at SSM
  `/<project>/<env>/namespace/tpch` in the AWS deployment
* *WHEN* the provisioning script runs
* *THEN* it SHALL enumerate that namespace rather than carry a hardcoded table list, so a different
  namespace needs no code change and a table added to the source is picked up automatically
* *AND* it SHALL normalize every enumerated table to the triple `(name, metadata_location,
  table_location)`, so every downstream step — derivation, warehouse creation, and registration — is
  independent of how the source catalog was read
* *AND* it SHALL select the source reader by configuration between a GLUE reader, which enumerates
  with `aws glue get-tables` and takes each table's `metadata_location` from that table's Glue
  parameters, and an ICEBERG REST reader, which enumerates and loads over an OAuth2-bearer REST
  catalog. The REST reader exists so the local verification exercises the identical downstream code;
  the GLUE reader is the AWS path
* *AND* it SHALL fail naming that table when a source entry carries no `metadata_location`
* *AND* it SHALL take each table's own recorded root location from the `location` field INSIDE the
  metadata document at that `metadata_location`, rather than inferring the root by parsing the
  metadata file's path, because that recorded field is the second value Lakekeeper location-checks
* *AND* it SHALL derive the warehouse's bucket and key prefix from those values so that BOTH the
  metadata location and the recorded root of every table are STRICT sublocations of
  `s3://<bucket>/<key-prefix>` — a prefix equal to any table's own root SHALL be shortened to its
  parent, because Lakekeeper rejects a location equal to the warehouse base rather than below it
* *AND* it SHALL fail naming the tables involved when two tables resolve to different buckets, rather
  than widening the prefix to cover both
* *AND* it SHALL fail naming the bucket when the derived key prefix would be EMPTY — the case where the
  tables sit directly under the bucket root, or where the shorten-to-parent rule fires at the top
  level — rather than creating a bucket-root warehouse, because an empty key prefix makes the
  warehouse base the bucket itself and gives the derivation nothing left to shorten on the next table
* *AND* it SHALL create the target namespace in Lakekeeper before registering, treating an
  already-exists answer as success, because Lakekeeper does not auto-create a namespace on register
* *AND* it SHALL fail naming the namespace when the target namespace is one Lakekeeper reserves
  (`system`, `examples`, `information_schema`), rather than surfacing that as a per-table error
* *AND* for each table it SHALL issue one Iceberg REST register-table request carrying that same
  metadata location verbatim, and MUST NOT write, copy, or rewrite any metadata file, manifest, or
  data file
* *AND* the name registered in Lakekeeper SHALL be byte-identical to the name the source catalog
  reported, so the virtual schema exposes the same table names under either catalog and the existing
  benchmark query set runs unchanged against both
* *AND* it SHALL treat an already-registered table as success rather than an error, and SHALL send the
  register request's `overwrite` flag EXPLICITLY as `false` rather than omitting it or sending JSON
  `null`, so a re-run never replaces a table's recorded metadata pointer
* *AND* it SHALL confirm every non-definitively-failed register outcome (a fresh `2xx`, or a `409` not
  otherwise identified) with a `loadTable` read-back of the just-registered table, comparing the
  returned `metadata-location` against the value this run submitted, and SHALL treat a mismatch as a
  DISTINCT, always-failing outcome rather than folding it into already-registered success. This
  read-back — not response-text sniffing — is the mechanism the exit code rests on: verified live
  against Lakekeeper 0.13.1, a genuine location conflict with a different table and an ordinary
  already-registered re-run answer with a byte-identical `409 AlreadyExistsException` body, so text
  matching alone cannot tell them apart (decision [29]). Response-text matching for a
  distinguishing "location already taken" message MAY still be logged when Lakekeeper does emit one,
  as a documentation aid, but MUST NOT be the sole basis for the success/failure classification.
  Lakekeeper 0.13.1 does NOT reject the TPC-H `part`/`partsupp` shape — that was verified live, and no
  mitigation for it exists in this feature — so a read-back mismatch, if it ever occurs, signals a
  real registration gap, a changed Lakekeeper version, or a genuinely overlapping location, never a
  repeat run
* *AND* it SHALL print a per-table summary distinguishing registered, already-present, and failed
  tables, and SHALL exit non-zero when any table failed, so a partial registration cannot be mistaken
  for a complete one
* *AND* it SHALL support a SOURCE-ONLY mode, selected by a `--source-only` command-line argument, which
  runs the enumeration and normalization above, prints each table's name, `metadata_location`, and
  `table_location`, and then exits. In that mode the script MUST NOT issue any target-catalog request
  at all — no token request, no management call, no warehouse creation, no namespace creation, and no
  register call — and MUST NOT write anything, so the source half of the design can be exercised
  against a live source catalog with no target deployment and no billable resource. It SHALL exit
  non-zero naming any table that carries no `metadata_location`, exactly as the full flow does
* *AND* it SHALL reject any command-line argument other than `--source-only`, rather than ignoring it,
  so a typo cannot silently start a full provisioning run against AWS

### Scenario: Bench secrets carry both catalogs' variables from one environment

* *GIVEN* a deployed `cluster-stack` environment
* *WHEN* the operator runs `deploy/scripts/secrets.sh <env>` while a Lakekeeper stack workspace exists
  for that same environment
* *THEN* the generated `bench/.env` SHALL contain the existing Glue, AWS, and Exasol variables with
  the same names and values as before this feature
* *AND* it SHALL additionally contain the Lakekeeper catalog URI, warehouse name, OAuth2 client id,
  client secret, and token endpoint, with every URI built from the box's PRIVATE IP because the
  consumer of these values is the Exasol cluster, not the operator's machine
* *AND* it MUST NOT set the catalog-selection variable, so an existing consumer of `bench/.env`
  continues to run against Glue until the operator asks otherwise
* *AND* when no Lakekeeper stack workspace exists for that environment, it SHALL omit the Lakekeeper
  block, print a note saying so, and exit successfully, so Glue-only environments are unaffected
* *AND* the generated file SHALL keep owner-only permissions, and no secret value SHALL be echoed to
  standard output

### Scenario: Teardown removes only the Lakekeeper stack

* *GIVEN* an applied Lakekeeper stack for an environment
* *WHEN* the operator runs `deploy/scripts/lakekeeper-down.sh <env>`
* *THEN* it SHALL destroy that environment's Lakekeeper workspace only — the EC2 instance, its
  security group, its IAM user and access key, and its SSM parameters
* *AND* it MUST NOT touch the `data-stack` bucket, the Glue catalog, the Exasol cluster, or the Trino
  stack, so the TPC-H data and every existing benchmark path survive teardown
* *AND* a later `lakekeeper-up.sh` for the same environment SHALL yield a working catalog again from a
  clean box, because provisioning is idempotent and registers the same unchanged S3 metadata
  locations
