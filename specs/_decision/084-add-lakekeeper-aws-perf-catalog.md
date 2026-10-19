# Decisions: add-lakekeeper-aws-perf-catalog

## ADR: The catalog gets its own write-capable storage credential; the engine keeps read-only keys

**ID:** lakekeeper-storage-credential-separate-from-engine-reader
**Plan:** add-lakekeeper-aws-perf-catalog
**Status:** Accepted

### Context

Lakekeeper validates a warehouse at creation by writing, reading, and deleting a probe object. The existing read-only IAM user cannot do that.

### Decision

The stack creates a dedicated IAM user with object read, write, and delete on the data bucket and stores its key pair in SSM. That key pair is the warehouse's storage credential. The Exasol CONNECTION keeps the read-only key pair.

### Options Considered

| Option | Verdict |
|--------|---------|
| Reuse `engine-reader` | Rejected: warehouse creation fails |
| Disable Lakekeeper's storage validation | Rejected: upstream calls it unsuitable for production, and it hides misconfiguration until the first register call |

### Consequences

- The deployer policy needs no change.
- The bucket-wide grant is an accepted risk, recorded in its own decision.

## ADR: Two URI vantages, keyed on where the caller runs

**ID:** lakekeeper-catalog-uri-vantage-by-caller-location
**Plan:** add-lakekeeper-aws-perf-catalog
**Status:** Accepted

### Context

The Lakekeeper box has a public and a private IP. Three callers reach it: the operator's laptop, the Exasol UDF inside the VPC, and an optional in-VPC EC2 caller. A same-VPC client reaching the public IP routes unreliably.

### Decision

The stack outputs both addresses, and each caller gets the one that matches its location: private for in-VPC callers, public for callers outside the VPC. Lakekeeper's OIDC configuration accepts tokens issued from both.

### Options Considered

| Option | Verdict |
|--------|---------|
| Key the vantage on which script is calling | Rejected: conflates script identity with location and leaves the in-VPC EC2 address undefined |
| One public-IP URI for both | Rejected: same-VPC routing is unreliable, and Keycloak stamps the token issuer from the request host, so a single-issuer setup rejects one caller |

### Consequences

- Each caller states its own network location, and the script has no location logic.

## ADR: Bucket-wide write is an accepted, named risk; the warehouse is soft-delete and the tool has no destructive path

**ID:** lakekeeper-bucket-wide-write-accepted-risk
**Plan:** add-lakekeeper-aws-perf-catalog
**Status:** Accepted

### Context

The warehouse key prefix is derived from the source tables after apply, so the apply-time IAM policy cannot name it and must grant object access across the whole data bucket.

### Decision

The bucket-wide grant is an accepted risk bounded by four controls. The provisioning script has no destructive verb. The warehouse uses a soft delete profile with a 7-day expiration. The credential lives only as long as the ephemeral stack. The Exasol CONNECTION keeps the read-only key pair.

### Options Considered

| Option | Verdict |
|--------|---------|
| Scope the grant to a stack-configured guard prefix | Rejected: a default cannot be verified at plan time, and a wrong one fails the apply |
| Leave the `delete-profile` at the server default | Rejected: the existing harness sends the hard form |

### Consequences

- Registered tables point at the one physical TPC-H copy, so a hard profile plus a purge-drop would delete the benchmark's only data.
- The soft profile is a delay window, not a guarantee, because a forced drop bypasses it.

## ADR: One provisioning script serves both run sites; the lifecycle pair stays laptop-only

**ID:** lakekeeper-provisioning-script-dual-run-site
**Plan:** add-lakekeeper-aws-perf-catalog
**Status:** Accepted

### Context

The provisioning script must run on a laptop for demos or from an EC2 box for the benchmark. The up and down scripts need OpenTofu, deployer-grade IAM, and the stack state, which no EC2 box has.

### Decision

One provisioning script runs unchanged from a laptop or an EC2 box. It authenticates to AWS only through the CLI's standard credential chain and passes an explicit region on every call. The up and down scripts stay operator-machine scripts, and the EC2 site runs the provisioning script directly against an applied stack, reading its settings from SSM.

### Options Considered

| Option | Verdict |
|--------|---------|
| Laptop-only with static keys | Rejected: cannot run from an EC2 box |
| EC2-only, with `lakekeeper-up.sh` copying the script over SSH | Rejected: rules out the free source-only laptop pre-flight |
| A `--profile` argument for explicit credentials | Rejected: reintroduces a location assumption |

### Consequences

- The EC2 run site needs an operator-supplied instance profile granting `ssm:GetParameter`, `kms:Decrypt`, `glue:GetTables`, and `s3:GetObject`.

## ADR: Credentials reach `curl` through a file descriptor, never through argv

**ID:** lakekeeper-credentials-via-file-descriptor-not-argv
**Plan:** add-lakekeeper-aws-perf-catalog
**Status:** Accepted

### Context

A credential in a process's argv is world-readable through `/proc/<pid>/cmdline`. Forms such as `curl -H` and `jq --arg` put it there.

### Decision

No credential appears in the argv of any process the script spawns. Credentials reach `curl` through standard input or process substitution, and `jq` bodies read them from the environment. Tracing is banned. Response bodies live in a temp directory removed on exit and are never printed on an error path.

### Options Considered

| Option | Verdict |
|--------|---------|
| `curl -H "Authorization: Bearer $token"` | Rejected: argv is world-readable while the request runs |
| Write the credential to a temp file and pass its path | Rejected where avoidable: puts the secret on disk, at risk if the process dies before cleanup |

### Consequences

- The offline harness stubs every external tool with a recording wrapper, so the argv scan covers every spawned process.
- This addresses a local observer only. Network exposure is a separate accepted seam.

## ADR: Provisioning is bash, not Rust

**ID:** lakekeeper-provisioning-bash-not-rust
**Plan:** add-lakekeeper-aws-perf-catalog
**Status:** Accepted

### Context

Lakekeeper provisioning must not be part of the Rust code.

### Decision

Provisioning is a bash script using `curl`, `jq`, and the AWS CLI. No crate is added.

### Options Considered

| Option | Verdict |
|--------|---------|
| A Rust binary workspace member | Rejected: provisioning must stay out of the Rust code |

### Consequences

- Cargo feature unification can no longer reach the shipped `.so`.
- Bash loses compile-time JSON checking, so every request body is built with `jq -n` and typed argument flags, never interpolation or a heredoc. An offline harness asserts each body's structure, and local Docker verification sends every body to a real Lakekeeper.
- The source read cannot use the shared Iceberg REST client, so it normalizes every table to a name, metadata location, and table location.

## ADR: Provisioning traffic stays cleartext; the public-vantage exposure is a named, accepted seam

**ID:** lakekeeper-provisioning-traffic-cleartext-accepted-seam
**Plan:** add-lakekeeper-aws-perf-catalog
**Status:** Accepted

### Context

Lakekeeper and Keycloak are reached over plain HTTP. The up script uses the public address, so every deployment sends the OAuth2 client secret, a bearer token, and the warehouse's write-and-delete S3 key pair across the public internet in cleartext at least once.

### Decision

The deployment adds no TLS, reverse proxy, or SSH tunnel, and the exposure is documented as a known seam. The security-group allowlist bounds who may connect, not who may observe traffic.

### Options Considered

| Option | Verdict |
|--------|---------|
| Require an SSH tunnel for the laptop vantage | Rejected: adds an SSH and key-file prerequisite and complicates the address rule for a bounded exposure |
| Terminate TLS with a self-signed certificate | Rejected: needs a trust decision on both callers and plaintext handling in the UDF, and barely improves on cleartext against the same observer |

### Consequences

- The exposure is bounded by the credential's lifetime, which ends when the down script destroys the IAM user.
- Adding TLS or a tunnel later changes no interface the script owns.
