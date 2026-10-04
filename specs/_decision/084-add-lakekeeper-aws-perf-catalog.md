# Decisions: add-lakekeeper-aws-perf-catalog

## ADR: The provisioning tool is a separate binary-only workspace member

**ID:** lakekeeper-provisioning-rust-binary-member
**Plan:** add-lakekeeper-aws-perf-catalog
**Status:** Accepted

### Context

Lakekeeper provisioning needed a code home that would not let its HTTP-client and runtime dependencies enter the shipped `.so`'s build graph. A `[[bin]]` inside `lakehouse-catalog` was considered, since that crate already compiles into the cdylib.

### Decision

Add `crates/lakekeeper-provision`, a binary-only workspace member depending on `lakehouse-catalog`, kept out of the `.so` by the workspace's package-scoped build line (`cargo build --release -p lakehouse-engine`) and a `dependency_direction.rs` test.

### Options Considered

| Option | Verdict |
|--------|---------|
| Separate binary-only workspace member (`crates/lakekeeper-provision`) | ✓ Chosen — Cargo's package-scoped build keeps its features out of the shipped `.so`'s graph |
| A `[[bin]]` inside `lakehouse-catalog` | ✗ Rejected — Cargo applies a crate's `[dependencies]` to every target, so the bin's dependencies would enter the library build that compiles into the `.so` |

### Consequences

Superseded by decision `lakekeeper-provisioning-bash-not-rust` before implementation began — no `crates/lakekeeper-provision` was ever created, no manifest changed, and the guarding test was never written. Recorded for the historical record of a design reviewed across two adversarial rounds.

## ADR: The Lakekeeper write side is raw authenticated HTTP, not an `iceberg_catalog_rest::RestCatalog`

**ID:** lakekeeper-write-side-raw-http-not-rest-catalog
**Plan:** add-lakekeeper-aws-perf-catalog
**Status:** Accepted

### Context

Lakekeeper's Management API (bootstrap, warehouse creation) and its Iceberg REST namespace-create and register-table calls both need OAuth2 client-credentials auth. Configuring a full `RestCatalog` for this would mean re-declaring REST auth property keys (`credential`, `oauth2-server-uri`, `scope`) that `lakehouse-catalog` holds crate-private.

### Decision

The provisioning script issues the Lakekeeper Management API and Iceberg REST namespace/register calls itself, over one HTTP client holding one Keycloak token.

### Options Considered

| Option | Verdict |
|--------|---------|
| Raw authenticated HTTP over one client, one token | ✓ Chosen — one owner of the provisioning protocol, no widened crate surface |
| Build a `RestCatalog` and call `Catalog::register_table` | ✗ Rejected — needs crate-private REST auth property keys `iceberg-catalog-rest` 0.10.0 exports no constants for, forcing either a duplicated literal or a widened public surface guarded by a reachability probe |

### Consequences

The "library owns wire shapes, tool owns transport" half of this decision was later superseded by `lakekeeper-bash-json-body-construction-controls` once the tool moved to bash, which cannot import typed request structs at all. The "raw HTTP, one client, one token" half survives unchanged, now expressed with `curl` rather than `reqwest`.

## ADR: The catalog gets its own write-capable storage credential; the engine keeps read-only keys

**ID:** lakekeeper-storage-credential-separate-from-engine-reader
**Plan:** add-lakekeeper-aws-perf-catalog
**Status:** Accepted

### Context

Lakekeeper validates a warehouse's storage access at creation by writing, reading back, and deleting a probe object under a random path inside the warehouse prefix — verified against the v0.13.1 source tree. The existing `engine-reader` IAM user grants only `GetObject`, `ListBucket`, and `GetBucketLocation`.

### Decision

The stack creates an IAM user named with the `<project>-<env>-lakekeeper` prefix, granting object read, write, and delete on the `data-stack` bucket, and stores its key pair in SSM `SecureString`. That key pair becomes the warehouse's storage credential; the Exasol CONNECTION keeps the existing read-only `engine-reader` key pair.

### Options Considered

| Option | Verdict |
|--------|---------|
| Dedicated write-capable IAM user for the warehouse | ✓ Chosen — a read-only credential cannot create a warehouse at all, and splitting keeps the long-lived query path read-only |
| Reuse `engine-reader` for both | ✗ Rejected — its policy grants no S3 write or delete, so warehouse creation fails outright |
| Disable Lakekeeper's storage validation with its runtime skip setting | ✗ Rejected — upstream documents it as unsuitable for production; it hides a misconfiguration until the first register call and does not relax the location rule anyway |

### Consequences

No `deploy/iam/deployer-policy.json` change is required, since the deployer's IAM statement already covers `<project>-*`-named users, policies, and access keys. The write-capable grant's blast radius (bucket-wide, not prefix-scoped) is separately named as an accepted risk in `lakekeeper-bucket-wide-write-accepted-risk`.

## ADR: Two URI vantages, keyed on where the caller runs

**ID:** lakekeeper-catalog-uri-vantage-by-caller-location
**Plan:** add-lakekeeper-aws-perf-catalog
**Status:** Accepted

### Context

The stack's Lakekeeper box has both a public and a private IP. Three distinct callers reach it — the operator's laptop, the Exasol UDF inside the VPC, and an optional in-VPC EC2 provisioning caller — and a same-VPC client reaching the box's public IP routes out through the internet gateway and back, which `deploy/trino-stack/outputs.tf` already records as unreliable for a long-lived connection.

### Decision

The stack outputs both vantages. The vantage a caller receives is determined by its LOCATION, not by which script is calling: `secrets.sh` writes PRIVATE-IP URIs for the in-VPC UDF, `lakekeeper-up.sh` passes PUBLIC-IP URIs because it runs outside the VPC, and an in-VPC EC2 caller running the provisioning script directly also receives PRIVATE-IP URIs. Lakekeeper's OIDC configuration accepts tokens issued from both vantages.

### Options Considered

| Option | Verdict |
|--------|---------|
| Vantage keyed on the caller's network location | ✓ Chosen — matches the actual routing constraint regardless of which script happens to call |
| Vantage keyed on which script is calling | ✗ Rejected — conflates script identity with location and leaves the in-VPC EC2 run site's URI undefined |
| One public-IP URI for both vantages | ✗ Rejected — same-VPC clients route unreliably through the IGW, and Keycloak stamps the token's `iss` from the request host, so a single-issuer configuration rejects one caller |

### Consequences

Every URI-consuming caller must state its own network location correctly; the script itself carries no location logic and reads `LK_TARGET_*` verbatim, per `lakekeeper-provisioning-script-dual-run-site`.

## ADR: Bucket-wide write is an accepted, named risk; the warehouse is soft-delete and the tool has no destructive path

**ID:** lakekeeper-bucket-wide-write-accepted-risk
**Plan:** add-lakekeeper-aws-perf-catalog
**Status:** Accepted

### Context

The warehouse's key prefix is derived from the source tables at provisioning time (after apply), so the IAM policy written at apply time cannot name that prefix and must instead grant object put/get/delete across the whole `data-stack` bucket.

### Decision

Accept the bucket-wide grant as a named risk. Four compensating controls bound it: `deploy/scripts/lakekeeper-provision.sh` contains no destructive verb of any kind (no HTTP `DELETE`, no `purgeRequested`, no destructive `aws s3` verb); the warehouse's `delete-profile` is explicitly the SOFT profile (`{"type": "soft", "expiration-seconds": 604800}`); the credential is created and destroyed with the ephemeral stack; and the Exasol CONNECTION keeps the read-only `engine-reader` key pair.

### Options Considered

| Option | Verdict |
|--------|---------|
| Bucket-wide grant, four compensating controls, absent destructive verb as the primary control | ✓ Chosen — the guard prefix cannot be known before the tables are read, which happens after apply |
| Scope the grant to a stack-configured guard prefix | ✗ Rejected — its correct default cannot be verified at plan time and a wrong default fails the apply |
| Leave the `delete-profile` at the server default | ✗ Rejected — the existing harness sends the HARD form, which would be silently inherited with no recorded decision |

### Consequences

Register-by-reference means Lakekeeper's tables point at the one physical TPC-H copy, so a hard delete-profile plus any purge-drop would delete the benchmark's only data. The soft profile is documented as a delay window, not a guarantee, since a `force` drop bypasses it entirely.

## ADR: One provisioning script serves both run sites; the lifecycle pair stays laptop-only

**ID:** lakekeeper-provisioning-script-dual-run-site
**Plan:** add-lakekeeper-aws-perf-catalog
**Status:** Accepted

### Context

The user required the provisioning script to run "either on the laptop for demo, or from this EC2 to perform the performance benchmark." `lakekeeper-up.sh`/`lakekeeper-down.sh` need `tofu`, deployer-grade IAM, and this stack's own OpenTofu workspace state, none of which any task in this plan gives an EC2 box, and `deploy/` has no orchestrator stack to hold one.

### Decision

`deploy/scripts/lakekeeper-provision.sh` runs unchanged from an operator's laptop and from an EC2 box, authenticating to AWS only through the CLI's standard credential chain (no `--profile`, no `~/.aws/` read, no direct instance-metadata query) and passing an explicit `--region` on every call. `lakekeeper-up.sh` and `lakekeeper-down.sh` stay operator-machine scripts in both contexts; the EC2 run site is reached by invoking the provisioning script directly against an already-applied stack, reading every `LK_*` value from SSM alone.

### Options Considered

| Option | Verdict |
|--------|---------|
| One run-site-agnostic provisioning script; lifecycle pair stays laptop-only | ✓ Chosen — matches the user's stated requirement without building an unrequested EC2 orchestrator |
| Laptop-only with static keys | ✗ Rejected by the user |
| EC2-only, with `lakekeeper-up.sh` copying the script over SSH | ✗ Rejected by the user; also makes the free source-only laptop pre-flight impossible |
| A `--profile`/`LK_AWS_PROFILE` argument to select credentials explicitly | ✗ Rejected — reintroduces the location assumption the requirement forbids |

### Consequences

An instance profile granting `ssm:GetParameter`, `kms:Decrypt`, `glue:GetTables`, and `s3:GetObject` is a stated prerequisite for the EC2 run site, but no stack or task in this plan creates such a box or profile — it is supplied by the operator, like the laptop itself.

## ADR: Credentials reach `curl` through a file descriptor, never through argv

**ID:** lakekeeper-credentials-via-file-descriptor-not-argv
**Plan:** add-lakekeeper-aws-perf-catalog
**Status:** Accepted

### Context

Bash has no built-in guarantee against a credential landing in a spawned process's argv, which is world-readable via `/proc/<pid>/cmdline`. The Rust design got this property for free because `reqwest` takes headers as function arguments; bash's `curl -H`, `jq --arg`, and similar forms do not.

### Decision

No credential appears in the argv of any process the script spawns. Credentials reach `curl` through standard input or process substitution, never `-u`/`-d`/`-H`/URL-query tokens. A credential-bearing `jq -n` body reads its value from `env.<VAR>` rather than `--arg`/`--argjson`. `set -x` is banned outright. Captured response bodies live under a `mktemp -d` directory removed by an `EXIT` trap and are never printed on an error path.

### Options Considered

| Option | Verdict |
|--------|---------|
| File-descriptor/environment-based credential passing, argv scan over every spawned process | ✓ Chosen — closes the exposure `/proc/<pid>/cmdline` presents to any local user |
| Ordinary `curl -H "Authorization: Bearer $token"` | ✗ Rejected — argv is world-readable while the request is in flight |
| Write the credential to a temp file and pass its path | ✗ Rejected where avoidable — puts the secret on disk, at risk if the process crashes before the `EXIT` trap |

### Consequences

The offline harness stubs `jq` as a recording wrapper alongside `curl`, `aws`, `tofu`, and `ssh`, so the credential-hygiene assertion covers every spawned process, not `curl` alone. This decision explicitly addresses a local-observer threat model only; the network-observer exposure is separately named in `lakekeeper-provisioning-traffic-cleartext-accepted-seam`.

## ADR: Provisioning is bash, not Rust

**ID:** lakekeeper-provisioning-bash-not-rust
**Plan:** add-lakekeeper-aws-perf-catalog
**Status:** Accepted
**Supersedes:** lakekeeper-provisioning-rust-binary-member

### Context

Two adversarial review rounds had approved a Rust binary-only workspace member design (`crates/lakekeeper-provision`) for Lakekeeper provisioning, with a draft PR already open. The user then redirected the design directly: "I do not want lakekeeper provisioning being part of the Rust code."

### Decision

Provisioning is `deploy/scripts/lakekeeper-provision.sh`, using `curl`, `jq`, and the AWS CLI. No crate is added, no manifest changes, and `Cargo.lock` is untouched.

### Options Considered

| Option | Verdict |
|--------|---------|
| Bash script in `deploy/scripts/`, matching the repo's other up/down and provisioning tooling | ✓ Chosen — user-directed, and removes an entire class of hazard (feature unification reaching the shipped `.so`) by construction |
| The reviewed Rust design (`crates/lakekeeper-provision`, six modules, sibling tests, dependency-direction guard) | ✗ Rejected by the user |

### Consequences

The `exasol-udf-sdk` boundary seam the Rust design had to accept (a laptop tool linking the UDF runtime's error type) disappears entirely. Three costs are incurred and separately recorded: lost compile-time JSON checking (`lakekeeper-bash-json-body-construction-controls`), lost automatic argv safety (`lakekeeper-credentials-via-file-descriptor-not-argv`), and a source read that can no longer go through the shared Iceberg REST client (`lakekeeper-glue-source-read-aws-cli-normalized-triple`).

Accept the loss and replace it with three controls: every request body is built with `jq -n` and typed argument flags, never string interpolation or a heredoc; the offline stubbed-PATH harness captures each emitted body and asserts its exact structure with `jq -e` against the v0.13.1 wire shapes; and the local Docker verification sends every body to a real Lakekeeper 0.13.1, which rejects a wrong shape.

The source read normalizes every table to `(name, metadata_location, table_location)`.

## ADR: Provisioning traffic stays cleartext; the public-vantage exposure is a named, accepted seam

**ID:** lakekeeper-provisioning-traffic-cleartext-accepted-seam
**Plan:** add-lakekeeper-aws-perf-catalog
**Status:** Accepted

### Context

Lakekeeper and Keycloak are reached over plain HTTP from both vantages. `lakekeeper-up.sh` is an operator-machine script in both the benchmark and demo contexts and always invokes the provisioning script with the PUBLIC-IP URIs, so every deployment carries at least one run that sends the OAuth2 client secret, the resulting bearer token, and the warehouse's write-and-delete S3 key pair across the public internet in cleartext.

### Decision

No TLS termination, certificate, reverse proxy, or SSH tunnel is added. The exposure is accepted and recorded as a fourth named seam in `deploy/README.md` § Known seams, alongside the storage-credential and OAuth2-client-secret seams. The security-group `/32` allowlist is recorded as a reachability control only — it bounds who may connect, not who may observe traffic in transit.

### Options Considered

| Option | Verdict |
|--------|---------|
| Accept the cleartext exposure as a documented, bounded risk | ✓ Chosen — user-decided directly after the review blocker was raised; the allowlist already limits reachability to the operator's own address |
| Require an SSH tunnel for the laptop vantage | ✗ Rejected — adds an `ssh`/key-file prerequisite to the lifecycle script and complicates the URI-vantage rule for a bounded exposure |
| Terminate TLS with a self-signed certificate | ✗ Rejected — needs a trust decision on both callers and `ALLOW_HTTP` handling on the UDF side; a self-signed cert accepted without verification barely improves on cleartext against the same observer |

### Consequences

The credential's usable lifetime — not the one-run transmission window — is the bound that matters to an observer who captures it; that lifetime runs until `lakekeeper-down.sh` destroys the IAM user with the ephemeral stack. The decision is explicitly reversible: adding TLS or a tunnel later changes no interface the script owns, since it reads `LK_TARGET_*` verbatim.
