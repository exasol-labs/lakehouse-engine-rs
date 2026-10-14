# Decisions: add-lakekeeper-aws-perf-catalog

## ADR: The provisioning tool is a separate binary-only workspace member

**ID:** lakekeeper-provisioning-rust-binary-member
**Plan:** add-lakekeeper-aws-perf-catalog
**Status:** Accepted

### Context

Lakekeeper provisioning needed a code home that keeps its HTTP-client and runtime dependencies out of the shipped `.so`'s build graph.

### Decision

Provisioning would live in a binary-only workspace member, `crates/lakekeeper-provision`, that depends on `lakehouse-catalog` and stays out of the `.so` through the package-scoped build line and a `dependency_direction.rs` test.

### Options Considered

| Option | Verdict |
|--------|---------|
| A `[[bin]]` inside `lakehouse-catalog` | Rejected: a crate's dependencies apply to every target, so the bin's dependencies would enter the `.so` build |

### Consequences

Superseded by `lakekeeper-provisioning-bash-not-rust` before implementation. No crate, manifest change, or test was ever created.

## ADR: The Lakekeeper write side is raw authenticated HTTP, not an `iceberg_catalog_rest::RestCatalog`

**ID:** lakekeeper-write-side-raw-http-not-rest-catalog
**Plan:** add-lakekeeper-aws-perf-catalog
**Status:** Accepted

### Context

Lakekeeper's Management API and its Iceberg REST namespace-create and register-table calls need OAuth2 client-credentials auth. A full `RestCatalog` would need REST auth property keys that `lakehouse-catalog` holds crate-private.

### Decision

The provisioning script issues the Management API and Iceberg REST calls itself over one HTTP client with one Keycloak token.

### Options Considered

| Option | Verdict |
|--------|---------|
| Build a `RestCatalog` and call `register_table` | Rejected: needs crate-private auth keys with no exported constants, forcing a duplicated literal or a widened public surface |

### Consequences

Once the tool moved to bash, `lakekeeper-bash-json-body-construction-controls` superseded the part where the library owns wire shapes. The raw-HTTP, one-client, one-token part survives, now with `curl`.

## ADR: The catalog gets its own write-capable storage credential; the engine keeps read-only keys

**ID:** lakekeeper-storage-credential-separate-from-engine-reader
**Plan:** add-lakekeeper-aws-perf-catalog
**Status:** Accepted

### Context

Lakekeeper validates a warehouse at creation by writing, reading, and deleting a probe object under its prefix. The existing `engine-reader` IAM user grants only read and list access.

### Decision

The stack creates a dedicated IAM user with object read, write, and delete on the `data-stack` bucket and stores its key pair in SSM `SecureString`. That key pair is the warehouse's storage credential, and the Exasol CONNECTION keeps the read-only `engine-reader` key pair.

### Options Considered

| Option | Verdict |
|--------|---------|
| Reuse `engine-reader` | Rejected: it cannot write or delete, so warehouse creation fails |
| Disable Lakekeeper's storage validation | Rejected: upstream documents it as unsuitable for production, and it hides misconfiguration until the first register call |

### Consequences

The deployer policy needs no change, because it already covers `<project>-*` users. The bucket-wide grant is an accepted risk in `lakekeeper-bucket-wide-write-accepted-risk`.

## ADR: Two URI vantages, keyed on where the caller runs

**ID:** lakekeeper-catalog-uri-vantage-by-caller-location
**Plan:** add-lakekeeper-aws-perf-catalog
**Status:** Accepted

### Context

The Lakekeeper box has a public and a private IP, and three callers reach it: the operator's laptop, the Exasol UDF inside the VPC, and an optional in-VPC EC2 provisioning caller. A same-VPC client reaching the public IP routes unreliably through the internet gateway.

### Decision

The stack outputs both vantages, and a caller gets the one that matches its location. `secrets.sh` writes private-IP URIs for the in-VPC UDF, `lakekeeper-up.sh` passes public-IP URIs because it runs outside the VPC, and an in-VPC EC2 caller gets private-IP URIs. Lakekeeper's OIDC configuration accepts tokens issued from both vantages.

### Options Considered

| Option | Verdict |
|--------|---------|
| Key the vantage on which script is calling | Rejected: conflates script identity with location and leaves the in-VPC EC2 URI undefined |
| One public-IP URI for both | Rejected: same-VPC routing is unreliable, and Keycloak stamps `iss` from the request host, so a single-issuer setup rejects one caller |

### Consequences

Each caller states its own network location. The script has no location logic and reads `LK_TARGET_*` verbatim.

## ADR: Bucket-wide write is an accepted, named risk; the warehouse is soft-delete and the tool has no destructive path

**ID:** lakekeeper-bucket-wide-write-accepted-risk
**Plan:** add-lakekeeper-aws-perf-catalog
**Status:** Accepted

### Context

The warehouse key prefix is derived from the source tables after apply, so the apply-time IAM policy cannot name it and must grant object access across the whole `data-stack` bucket.

### Decision

The bucket-wide grant is an accepted risk bounded by four controls. The provisioning script contains no destructive verb (no HTTP `DELETE`, no `purgeRequested`, no destructive `aws s3` verb). The warehouse uses the soft `delete-profile` with a 604800-second expiration. The credential is created and destroyed with the ephemeral stack. The Exasol CONNECTION keeps the read-only key pair.

### Options Considered

| Option | Verdict |
|--------|---------|
| Scope the grant to a stack-configured guard prefix | Rejected: a correct default cannot be verified at plan time, and a wrong default fails the apply |
| Leave the `delete-profile` at the server default | Rejected: the existing harness sends the hard form, which would be inherited without a recorded decision |

### Consequences

Registered tables point at the one physical TPC-H copy, so a hard profile plus a purge-drop would delete the benchmark's only data. The soft profile is a delay window, not a guarantee, because a `force` drop bypasses it.

## ADR: One provisioning script serves both run sites; the lifecycle pair stays laptop-only

**ID:** lakekeeper-provisioning-script-dual-run-site
**Plan:** add-lakekeeper-aws-perf-catalog
**Status:** Accepted

### Context

The user required the provisioning script to run either on a laptop for demos or from an EC2 box for the benchmark. `lakekeeper-up.sh` and `lakekeeper-down.sh` need `tofu`, deployer-grade IAM, and the stack's OpenTofu state, which no EC2 box in this plan has.

### Decision

`lakekeeper-provision.sh` runs unchanged from a laptop or an EC2 box. It authenticates to AWS only through the CLI's standard credential chain and passes an explicit `--region` on every call. The up and down scripts stay operator-machine scripts, and the EC2 run site invokes the provisioning script directly against an applied stack, reading every `LK_*` value from SSM.

### Options Considered

| Option | Verdict |
|--------|---------|
| Laptop-only with static keys | Rejected by the user |
| EC2-only, with `lakekeeper-up.sh` copying the script over SSH | Rejected by the user, and it rules out the free source-only laptop pre-flight |
| A `--profile` argument for explicit credentials | Rejected: reintroduces the location assumption |

### Consequences

The EC2 run site needs an operator-supplied instance profile granting `ssm:GetParameter`, `kms:Decrypt`, `glue:GetTables`, and `s3:GetObject`. No stack or task in this plan creates it.

## ADR: Credentials reach `curl` through a file descriptor, never through argv

**ID:** lakekeeper-credentials-via-file-descriptor-not-argv
**Plan:** add-lakekeeper-aws-perf-catalog
**Status:** Accepted

### Context

A credential in a process's argv is world-readable through `/proc/<pid>/cmdline`. Bash forms such as `curl -H` and `jq --arg` put it there.

### Decision

No credential appears in the argv of any process the script spawns. Credentials reach `curl` through standard input or process substitution, and credential-bearing `jq -n` bodies read from `env.<VAR>`. `set -x` is banned. Response bodies live in a `mktemp -d` directory removed by an `EXIT` trap and are never printed on an error path.

### Options Considered

| Option | Verdict |
|--------|---------|
| `curl -H "Authorization: Bearer $token"` | Rejected: argv is world-readable while the request runs |
| Write the credential to a temp file and pass its path | Rejected where avoidable: puts the secret on disk, at risk if the process dies before the `EXIT` trap |

### Consequences

The offline harness stubs `jq`, `curl`, `aws`, `tofu`, and `ssh` as recording wrappers, so the argv scan covers every spawned process. The decision addresses a local observer only. The network-observer exposure is in `lakekeeper-provisioning-traffic-cleartext-accepted-seam`.

## ADR: Provisioning is bash, not Rust

**ID:** lakekeeper-provisioning-bash-not-rust
**Plan:** add-lakekeeper-aws-perf-catalog
**Status:** Accepted
**Supersedes:** lakekeeper-provisioning-rust-binary-member

### Context

Two review rounds had approved the Rust provisioning member. The user then directed that Lakekeeper provisioning not be part of the Rust code.

### Decision

Provisioning is `deploy/scripts/lakekeeper-provision.sh`, using `curl`, `jq`, and the AWS CLI. No crate is added, and no manifest or `Cargo.lock` changes.

### Options Considered

| Option | Verdict |
|--------|---------|
| The reviewed Rust binary member | Rejected by the user |

### Consequences

Feature unification can no longer reach the shipped `.so`. Bash loses compile-time JSON checking, so every request body is built with `jq -n` and typed argument flags, never interpolation or a heredoc. The offline stubbed-PATH harness asserts each emitted body's structure with `jq -e` against the v0.13.1 wire shapes, and local Docker verification sends every body to a real Lakekeeper 0.13.1. Argv safety is handled in `lakekeeper-credentials-via-file-descriptor-not-argv`. The source read can no longer use the shared Iceberg REST client, so it normalizes every table to `(name, metadata_location, table_location)` (`lakekeeper-glue-source-read-aws-cli-normalized-triple`).

## ADR: Provisioning traffic stays cleartext; the public-vantage exposure is a named, accepted seam

**ID:** lakekeeper-provisioning-traffic-cleartext-accepted-seam
**Plan:** add-lakekeeper-aws-perf-catalog
**Status:** Accepted

### Context

Lakekeeper and Keycloak are reached over plain HTTP. `lakekeeper-up.sh` always uses the public-IP URIs, so every deployment sends the OAuth2 client secret, a bearer token, and the warehouse's write-and-delete S3 key pair across the public internet in cleartext at least once.

### Decision

The deployment adds no TLS termination, certificate, reverse proxy, or SSH tunnel. The exposure is a named seam in `deploy/README.md` under Known seams. The security-group `/32` allowlist bounds who may connect, not who may observe traffic.

### Options Considered

| Option | Verdict |
|--------|---------|
| Require an SSH tunnel for the laptop vantage | Rejected: adds an `ssh` and key-file prerequisite and complicates the URI rule for a bounded exposure |
| Terminate TLS with a self-signed certificate | Rejected: needs a trust decision on both callers and `ALLOW_HTTP` handling in the UDF, and barely improves on cleartext against the same observer |

### Consequences

The relevant bound is the credential's usable lifetime, which runs until `lakekeeper-down.sh` destroys the IAM user with the stack. Adding TLS or a tunnel later changes no interface the script owns.
