# Plan: add-lakekeeper-aws-perf-catalog

## Summary

Add an opt-in Lakekeeper catalog to the AWS perf-test environment, selected by a new `BENCH_CATALOG` toggle that defaults to Glue. The catalog is deployed by an ephemeral OpenTofu stack and filled by one bash script; Glue's variables, CONNECTION, query set, and row counts stay unchanged.

## Design

### Context

Three run contexts exercise the engine, and only the last two are in scope here.

| Context | Where | Driver | Cost | Status |
|---------|-------|--------|------|--------|
| e2e | local Docker, CI | `cargo test` | none | Exists (`crates/lakehouse-engine/tests/e2e_lakekeeper_test.rs`, `tests/common/lakekeeper.rs`). Out of scope — untouched by this plan |
| benchmark | AWS Exasol cluster | automated, unattended | billable EC2 | In scope |
| demo | AWS Exasol cluster | interactive, an operator during a live customer session | billable EC2 | In scope |

Benchmark and demo share ONE AWS Lakekeeper stack and ONE provisioning path. They differ only in who runs the commands and what happens afterwards, so nothing in the tooling branches on which one is running (decision [22]).

The engine already speaks Lakekeeper — Iceberg REST plus OAuth2 client-credentials, documented in `docs/catalogs.md` and proven by `e2e-harness/lakekeeper-e2e-harness` against a local Docker stack. Two pieces are missing: a Lakekeeper deployment the Exasol cluster nodes can reach, and a way to point the remote bench at it.

The TPC-H data must not be reloaded. It is already on S3 and cataloged in Glue, so the second catalog has to address the same physical files. The Iceberg REST Catalog API's register-table operation does exactly that: it records an existing `metadata.json` location in a target catalog, writing nothing.

- **Goals** — one additional catalog option, reachable from the Exasol cluster; zero engine, adapter, CONNECTION-field, `.so`, or Rust-workspace change; zero change to existing Glue behavior; no data copy; one provisioning path that runs unchanged from an operator's laptop and from an EC2 box; explicit apply and destroy with the cost posture the rest of `deploy/` has.
- **Non-Goals** — replacing Glue; a Rust provisioning tool of any kind; a separate demo stack, demo warehouse, demo namespace, or demo query set; vended or STS credentials for the AWS warehouse (static read-only keys only, matching the Glue path's own upgrade seam); a persistent Lakekeeper deployment; a CI-automated AWS run; consolidating the E2E harness's Lakekeeper bootstrap helpers.

### Decision

Four deliverables, each with one owner: an OpenTofu stack that deploys the catalog, one bash script that provisions it, two lifecycle scripts that sequence the two, and a bench toggle that selects between catalogs. No Rust is written or changed.

#### Architecture

```
 operator machine — BOTH contexts   AWS (data-stack VPC)
 ┌────────────────────────────┐    ┌────────────────────────────────────────────┐
 │ demo: interactive          │    │ lakekeeper-stack (ephemeral EC2)           │
 │ benchmark: unattended      │    │   postgres + keycloak:8080 +               │
 │ AWS creds: profile / SSO   │    │   lakekeeper:8181 (migrate, serve)         │
 │ needs: tofu, deployer IAM, │    │   SG ingress: 8181, 8080                   │
 │        this stack's state  │    │     -> allowed_cidrs (apply machine /32)   │
 └──────────────┬─────────────┘    │     + VPC CIDR (Exasol nodes)              │
                │                  │                                            │
 lakekeeper-up.sh <env>            │ data-stack: S3 bucket + Glue (:443)        │
   1 tofu apply ─────────────────> │   TPC-H Iceberg tables, ONE physical copy  │
   2 health wait ────────────────> │                                            │
   3 lakekeeper-provision.sh       │ cluster-stack: Exasol nodes                │
       LK_TARGET_* = PUBLIC IP     │   SG ingress: 8563/8443/2581               │
       aws glue get-tables ──────> │     -> allowed_cidrs (same default)        │
       aws s3 cp <metadata.json>─> │   (:443, SigV4 by the AWS CLI)             │
       curl token ───────────────> │   (:8080 Keycloak — CLEARTEXT, seam [28])  │
       curl bootstrap/warehouse/   │                                            │
            namespace/register ──> │   (:8181 Lakekeeper — CLEARTEXT, seam[28]) │
                                   │                                            │
 secrets.sh <env> ───────────────> │   (:22 SSH, private IP)                    │
   writes bench/.env, private-IP   │                                            │
   Lakekeeper URIs                 │   UDF ── OAuth2 :8181 ──> lakekeeper       │
 BENCH_CATALOG=lakekeeper          │   UDF ── static keys :443 ──> S3           │
 make bench ─────────────────────> │   (:8563 DB client)                        │
 then: interactive SQL (demo) ───> │   (:8563, same virtual schema)             │
                                   │                                            │
 ─ ─ ─ optional in-VPC run site ─ ─│─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ │
   any EC2 box in the VPC, creds:  │   lakekeeper-provision.sh ONLY,            │
   instance role; LK_* from SSM    │   LK_TARGET_* = PRIVATE IP,                │
   only, no tofu output ─────────> │   idempotent re-provision (:8080, :8181)   │
                                   └────────────────────────────────────────────┘

Ports without a source arrow above (8080/8181, 8563/8443/2581, all exposed to
allowed_cidrs) are reachable from the operator's machine ONLY because that
machine's public IP is the CIDR `tofu apply` recorded — not from the internet
at large. The `/32` allowlist governs who may CONNECT; it does not encrypt the
cleartext hops marked above (decision [28]).
```

The lifecycle pair and the provisioning script have different run-site claims, and only the second one is run-site agnostic.

`lakekeeper-up.sh` and `lakekeeper-down.sh` are OPERATOR-MACHINE scripts in both contexts, benchmark included. They need `tofu`, a deployer-grade IAM principal, and this stack's OpenTofu workspace state. No stack, task, instance profile, or prerequisite in this plan gives an EC2 box any of the three, and `deploy/` contains no orchestrator stack to put one there — it holds `cluster-stack/`, `data-stack/`, `iam/`, `scripts/`, and `trino-stack/` and nothing else. Building one is out of scope (decision [23]).

`lakekeeper-provision.sh` is the run-site-agnostic half, and it is the script the user's answer was about. It contains no location-dependent step: it authenticates to AWS through the AWS CLI's standard credential chain and to Lakekeeper through Keycloak's OAuth2 client-credentials grant, both identical from a laptop and from EC2. An in-VPC EC2 caller runs it directly against an already-applied stack, reading every `LK_*` value from SSM alone — never from an OpenTofu output, which needs the workspace state that box does not hold — with `LK_TARGET_*` carrying the PRIVATE-IP URIs; because provisioning is idempotent, that is a valid re-provision rather than a second deployment. Task 1.2 therefore publishes the warehouse name, the OAuth2 client id, and both vantages' catalog and token URIs as plain `String` SSM parameters beside the `SecureString` ones. § Manual Testing carries the full-flow EC2 run that closes the claim.

#### Patterns

| Pattern | Where | Why |
|---------|-------|-----|
| Ephemeral stack layered on persistent `data-stack` | `deploy/lakekeeper-stack/` | Copies `trino-stack`'s cost posture: nothing else applies it, so no forgotten variable starts a billable box |
| One up/down script pair, matching the repo convention | `deploy/scripts/lakekeeper-up.sh`, `lakekeeper-down.sh` | `cluster-up.sh`/`cluster-down.sh` and `trino-up.sh`/`trino-down.sh` already establish the shape, the workspace handling, and the cost banner |
| Lifecycle and provisioning are separate scripts | `lakekeeper-up.sh` vs `lakekeeper-provision.sh` | Different credential requirements (deployer IAM + OpenTofu vs Glue/S3 read) and different run sites; the source-only pre-flight MUST run with no stack applied at all, which a script starting with `tofu apply` cannot offer |
| Source read normalizes to one triple | `lakekeeper-provision.sh` source step | Everything downstream knows only `(name, metadata_location, table_location)`, so the Glue producer and the local-verification REST producer are the only code that knows how a catalog is read |
| Split password builders, one per catalog | `bench/run.sh` | `docs/catalogs.md` makes SigV4 and OAuth2 client credentials mutually exclusive, so the payloads are not two settings of one shape |
| Derive, don't configure | warehouse bucket + key prefix; target S3 flavor | A configured prefix that disagrees with the data silently produces a warehouse that rejects the registration it exists for |
| Every JSON body built by `jq -n` | `lakekeeper-provision.sh` | Bash has no compile-time JSON checking; `jq -n` is the only construction that guarantees well-formed output and correct escaping (decision [24]) |
| No credential in ANY spawned process's argv | `lakekeeper-provision.sh` | `-u`, `-d`, `-H`, and `jq --arg` alike put their value in a world-readable process listing; `--config -`, `--data @<(...)`, and `jq`'s `env.<VAR>` do not (decision [25]) |
| Two URI vantages, both explicit | stack outputs, OIDC issuers | A same-VPC client must use the private IP; Keycloak stamps `iss` from the request host, so both issuers must be accepted |
| Stubbed-PATH bash test harness | `deploy/scripts/tests/lakekeeper.test.sh` | `install.test.sh` already proves this pattern gives shell deliverables real regression coverage with no network |

#### Key interfaces

| Interface | Shape |
|-----------|-------|
| `lakekeeper-provision.sh` configuration | `LK_SOURCE_*` and `LK_TARGET_*` environment variables only; no credential on the command line. Its only arguments are the optional `--source-only` flag and nothing else |
| Source table triple | `(name, metadata_location, table_location)` per table, as a JSON array on the source step's stdout. `LK_SOURCE_KIND=glue` (default) reads it with `aws glue get-tables` plus `aws s3 cp` of the metadata document; `LK_SOURCE_KIND=rest` reads it with an OAuth2-bearer Iceberg REST `loadTable`, and exists so the local Docker verification drives the same downstream code |
| Provisioning order | enumerate source → fetch metadata and root locations → derive bucket and prefix → token → bootstrap → warehouse → **read the warehouse back and confirm its storage profile's bucket and key prefix equal the derived values** → resolve warehouse prefix → namespace → register each table → summary |
| Idempotency classification | bootstrap: 2xx or 409; warehouse: 2xx, 409, or 400 whose body reports a storage-profile overlap, each followed by the confirming read-back; namespace and table: 2xx or already-exists |
| No destructive verb | `deploy/scripts/lakekeeper-provision.sh` contains no `-X DELETE`, no `--request DELETE`, no `purgeRequested`, and no `aws s3 rm` / `aws s3api delete-object` / `aws s3api delete-objects`. The ban covers the AWS CLI as well as HTTP, because bash reaches both. `lakekeeper-down.sh`'s `tofu destroy` and the local verification's non-purging drop are OUT of that scan by design: neither lives in the provisioning script |
| AWS authentication | The AWS CLI's standard credential chain, unmodified. The script MUST NOT pass `--profile`, read `~/.aws/`, or query `169.254.169.254` itself, and MUST pass an explicit `--region` on every AWS call |
| Bench selection | `BENCH_CATALOG` unset or `glue` → today's SigV4 payload; `lakekeeper` → OAuth2 payload plus `ALLOW_HTTP`; anything else → hard error |

### Consequences

| Decision | Alternatives Considered | Rationale |
|----------|------------------------|-----------|
| Provisioning is bash (`curl` + `jq` + `aws`) | A Rust binary-only workspace member (`crates/lakekeeper-provision`), which the two earlier review rounds planned in detail | User-directed. It also removes the `.so`-fingerprint and feature-unification risk the Rust route had to defend against, and matches every other deliverable in `deploy/scripts/` |
| Ephemeral stack with explicit up/down scripts | A toggle in the persistent `data-stack`, as EMR Serverless has | An EC2 box bills continuously, unlike an idle serverless application; user-confirmed |
| Keycloak included | Run Lakekeeper unauthenticated behind the security group | Matches the customer's real deployment and the CONNECTION contract already under test; user-confirmed |
| Benchmark and demo share one stack and one provisioning path, with no suite selector | A `SUITE=benchmark\|demo` variable threaded through the stack, the provisioning script, and `bench/run.sh` | Nothing branches on it. `bench/run.sh:351-352` drops and recreates the virtual schema at the START of a run and never at the end, so the schema a benchmark run leaves behind IS the demo's query surface (decision [22]) |
| Source read uses `aws glue get-tables`, not Glue's Iceberg REST endpoint | `curl --aws-sigv4` against the Glue Iceberg REST catalog at `https://glue.<region>.amazonaws.com/iceberg` (`deploy/data-stack/main.tf:7`) | `curl --aws-sigv4` needs `--user <key>:<secret>` and adds no `x-amz-security-token`, so it cannot use the temporary credentials an EC2 instance profile supplies. That breaks the run-from-either-location requirement outright. No `--aws-sigv4` call exists anywhere in this repo's shell today |
| `curl` + `jq` for the catalog protocol | A dependency-free implementation in the style of `deploy/scripts/install.sh`, which declares "Bash 3.2+ (stock macOS). No jq." (`install.sh:17`) | That constraint belongs to `install.sh` alone, because end users fetch it with `curl \| bash`. Every operator-facing script in `deploy/scripts/` already requires `jq` — `secrets.sh`, `cluster-up.sh`, `trino-up.sh` — and `bench/make_deletes_docker.sh:36` already drives an Iceberg REST catalog with plain `curl` |
| Dedicated write-capable IAM user for the warehouse | Reuse the read-only `engine-reader` key pair | Lakekeeper validates warehouse storage access on creation; the query path stays read-only, which is the property that matters |
| Local Docker verification of the provisioning path | Verify only against AWS | Falsifies the register-by-reference assumption on a laptop before any billable resource exists |

## Features

| Feature | Status | Spec |
|---------|--------|------|
| aws-lakekeeper-perf-catalog | NEW | `e2e-harness/aws-lakekeeper-perf-catalog/spec.md` |
| cloud-e2e-harness | CHANGED | `e2e-harness/cloud-e2e-harness/spec.md` |

## Impact

Operators gain a second, opt-in catalog for the remote benchmark and for live demos. With `BENCH_CATALOG` unset, `make bench`, `deploy/scripts/secrets.sh`, and every existing stack keep today's required variables, catalog URI, CONNECTION password, virtual-schema properties, query set, and row counts, so there is no breaking change.

One output changes on both REMOTE arms: the benchmark report header gains a `catalog=` field naming the catalog the run used. That field carries the catalog name only and never an `s3://`-shaped value, because `bench/import_ceiling.sh:29` greps the whole report file for `s3://[^"]*/lineitem` to derive its table root.

The DOCKER target's header is unchanged and carries no `catalog=` field at all. `bench/run.sh:378-383` writes one header block for every target, so the field must be emitted conditionally: `BENCH_CATALOG` defaults to `glue`, and the local stack's catalog is neither Glue nor the AWS Lakekeeper box, so the cheapest unconditional implementation would label a local MinIO run `catalog=glue` and that false label would survive into `bench/reports/*.txt`.

Four things are new for an operator. `deploy/lakekeeper-stack/` needs a one-time `tofu init`. `lakekeeper-up.sh` creates a billable EC2 instance that only `lakekeeper-down.sh` removes. `bench/.env` regenerated by `secrets.sh` now also carries Lakekeeper credentials, still at owner-only permissions and still gitignored.

`deploy/scripts/bench-remote.sh`'s teardown trap cuts both ways, and both directions matter. It does NOT cover the Lakekeeper stack, so `lakekeeper-down.sh <env>` is a separate mandatory step — the same relationship `deploy/trino-stack` already has to that wrapper. It DOES cover the Exasol cluster: `bench-remote.sh:55` installs `trap teardown EXIT`, and the handler runs `cluster-down.sh <env>` on every exit path unless `KEEP_ALIVE=1` was exported. That destroys the cluster carrying the CONNECTION and the virtual schema, so a default `bench-remote.sh` run ends the demo it just set up. The demo runbook in task 6.1 therefore gives the full ordered sequence in both forms — `lakekeeper-up.sh` first, then either `BENCH_CATALOG=lakekeeper KEEP_ALIVE=1 ./bench-remote.sh <env>` or the unwrapped chain — never the bare wrapper.

The `selftest: vs_teardown_is_recreate_only` guard is a source-text check over `bench/run.sh` alone and cannot see this: it stays green while the wrapper destroys the surface. That is why the constraint is recorded in the `cloud-e2e-harness` spec and in the README runbook rather than left to the test.

One accepted security seam is new and is named rather than implied. Lakekeeper and Keycloak are reached over plain HTTP, so a public-vantage provisioning run sends the OAuth2 client secret, the bearer token, and the warehouse's write-capable S3 key pair over the public internet in cleartext to the box's public IP.

EVERY deployment carries at least one such run, benchmark included. `lakekeeper-up.sh` is an operator-machine script in both contexts and always provisions, and that run gets the public-IP URIs because it runs outside the VPC. The in-VPC vantage is clean only for the optional re-provision decision [23] describes, against a stack already provisioned the exposed way — so choosing the EC2 run site does not avoid the hop.

The `/32` security-group allowlist is the practical control and is a REACHABILITY one: `allowed_cidrs` defaults to the apply machine's resolved public IP, so the plaintext ports admit that one address plus the VPC CIDR and nothing else on the internet. It does not encrypt the traffic and does not stop an observer on the path between an allowlisted client and the box. The transmitted key pair stays usable until `lakekeeper-down.sh` destroys the IAM user, and it is scoped to the `data-stack` bucket alone. Decision [28] records the choice; `deploy/README.md` § Known seams carries it as a fourth entry beside the static-credentials, committed-client-secret, and bucket-wide-write seams.

The Rust workspace is untouched. No crate is added, no manifest changes, and the `.so`, its SLC fingerprint, and the Exasol-side DDL are unaffected. `cargo test` and `make cross-udf-build` behave exactly as before, so this plan cannot change the shipped artifact.

## Requirements

| Requirement | Details |
|-------------|---------|
| No Rust | No crate is added, removed, or modified. The root `Cargo.toml`, every crate manifest, and `Cargo.lock` stay byte-identical. Provisioning is `deploy/scripts/lakekeeper-provision.sh` only |
| No data rewrite | Registration carries the source `metadata.json` location verbatim; no Parquet, manifest, or metadata file is written |
| Same table names under both catalogs | The registered name equals the source name, so the existing benchmark query set runs unchanged |
| One path, two run sites | `lakekeeper-provision.sh` runs unchanged from an operator's laptop and from an EC2 box. It obtains AWS credentials only through the AWS CLI's standard chain, so a static profile and an instance profile both work with no code change; it MUST pass an explicit `--region` on every AWS call, because an instance profile supplies credentials but no region. An EC2 caller reads every `LK_*` value from SSM alone and no OpenTofu output, because it holds no workspace state (decision [23]) |
| No secret on stdout or in argv | Every credential travels by environment variable, SSM `SecureString`, or a `curl` file descriptor. The no-argv half binds EVERY process the script spawns, not `curl` alone: a credential-bearing `jq -n` body reads its value from `env.<VAR>` and never from `--arg`, because jq's `/proc/<pid>/cmdline` is world-readable on the same terms curl's is. `set -x` MUST NOT be enabled anywhere in the provisioning script. Error messages name endpoint, table, and status only |
| Idempotent provisioning | Every `lakekeeper-up.sh` run re-provisions a fresh box and must succeed against an already-provisioned one |
| No IAM policy change | Every principal the stack creates is named with the `spot-strata-*` prefix. `deploy/iam/deployer-policy.json` § `IamForEngineReaderAndInstanceProfiles` already grants 41 IAM actions on `user/spot-strata-*`, `role/spot-strata-*`, `policy/spot-strata-*`, and `instance-profile/spot-strata-*` — including the role, instance-profile, `PassRole`, `PutRolePolicy`, and matching `Delete*` verbs task 1.2 and `lakekeeper-down.sh` need. The one route that policy forbids is an inline USER policy: `iam:PutUserPolicy` is the absent verb, unlike `iam:PutRolePolicy`, so `aws_iam_user_policy` MUST NOT be used or the apply fails after the EC2 instance is already billing |
| No destructive path | `deploy/scripts/lakekeeper-provision.sh` has no HTTP `DELETE`, no `purgeRequested`, and no destructive `aws s3` verb, so nothing the script can be told to do reaches the one physical TPC-H copy. The warehouse's soft `delete-profile` DEFERS rather than prevents file removal — a soft-profile drop schedules an expiration task for `expiration-seconds` in the future, and a `force` drop bypasses the profile entirely — so the absent code path is the primary control and the soft profile is a delay window, not a guarantee |

## Dependencies

- OpenTofu ≥ 1.6 with `hashicorp/aws ~> 5.60`, `hashicorp/http ~> 3.4`, `hashicorp/random ~> 3.6` — the same provider set `cluster-stack` and `trino-stack` already pin.
- Pinned container images, matching `docker-compose.lakekeeper.yml`: `postgres:17`, `quay.io/keycloak/keycloak:26.0.7`, `quay.io/lakekeeper/catalog:v0.13.1`.
- `scripts/keycloak-realm-iceberg.json`, reused unchanged. It is 21 KB, above EC2 user-data's 16 KB cap, so the stack delivers it through S3 rather than inlining it.
- Host tooling for `lakekeeper-provision.sh`: `bash` 4+, `curl`, `jq`, and AWS CLI v2. No `curl` SigV4 support is required, because the AWS CLI signs every AWS request. `lakekeeper-up.sh` and `lakekeeper-down.sh` additionally need `tofu`, deployer IAM permissions, and this stack's own OpenTofu workspace state, which is why they are operator-machine scripts in both contexts and no EC2 run site in this plan is expected to run them; the provisioning script needs none of the three. Both sets are already required by `install-prereqs.sh` and the existing `deploy/scripts/*` family.
- Prerequisite work: an applied `data-stack`. A running `cluster-stack` environment is required before `make bench`, but NOT before `lakekeeper-up.sh` — the Lakekeeper stack reads `data-stack` values only and no `cluster-stack` output, which is what lets the demo runbook's wrapper form run `lakekeeper-up.sh <env>` before `bench-remote.sh` applies the cluster at its step `[1/4]`.

## Risks and open verification points

Planning settled four mechanism assumptions by reading the Lakekeeper v0.13.1 source tree. One live Lakekeeper 0.13.1 run additionally confirmed two of them — bullets 3 and 4 below. These are catalog-API facts, independent of whether the caller is Rust or bash, so the move to bash changes none of them; the script must implement the same request shapes and the same idempotency classification. Two risks remain open; § Manual Testing rows 2 and 3 close them, and only row 3 creates a billable resource.

Every live-run claim below comes from one spike under one set of conditions: Lakekeeper 0.13.1, the local Docker stack, MinIO as the object store, therefore the S3-COMPATIBILITY storage flavor with path-style addressing, and the registration order `part` then `partsupp`.

- **Register-table is served, implemented, and ungated.** The route is registered unconditionally, has been implemented since 0.6.1, and has no feature flag or enable/disable configuration. Its served path is `/catalog/v1/{prefix}/namespaces/{ns}/register`, so the script builds it on the `/catalog`-suffixed base URI the CONNECTION already uses, not on the management base.
- **Location containment is strict, checked twice, and cannot be relaxed.** Both the submitted `metadata-location` and the `location` recorded inside that metadata document must be strict sublocations of `s3://<bucket>/<key-prefix>`; equality with the base fails. No location-relaxing setting exists in this version — the CORS-only allowed-origin setting is unrelated. This confirms decision 5 and adds the shorten-to-parent rule now spelled out in the spec.
- **Warehouse creation writes a probe object, and accepts an already-populated prefix.** Creation writes, reads back, and deletes a probe object under a random path inside the warehouse prefix. It then asserts that the probe's own random path is empty. The warehouse prefix itself MAY already hold data. The live spike confirmed it. The spike created a warehouse with `key-prefix: tpch_src` over a prefix that already carried two Iceberg tables' data and metadata, and Lakekeeper answered `201`. That is the AWS ordering: data first, warehouse second. Every existing MinIO test in this repo runs the opposite order — warehouse first, then data — so the spike is the first exercise of the AWS ordering, on MinIO. A read-only credential still cannot create a warehouse, so decision 6 is required rather than precautionary. The probe's random identifiers cannot collide with the TPC-H data. A skip-validation runtime setting exists but is rejected: it hides a real misconfiguration and does not relax the location rule anyway.
- **The `part`/`partsupp` location shape registers cleanly — no mitigation needed.** Lakekeeper enforces non-overlapping table locations within a warehouse. An upstream issue reports `LocationAlreadyTaken` when one table's location is a non-slash-delimited prefix of another's, and TPC-H contains exactly that shape. The live spike reproduced the production shape rather than a synthetic one. The unauthenticated Iceberg REST catalog derived `s3://warehouse/tpch_src/part` and `s3://warehouse/tpch_src/partsupp` from its own default location rule. The spike then created a Lakekeeper warehouse over that same already-populated `tpch_src` prefix and registered both tables into it by reference. Each answered `200`, `partsupp` immediately after `part`, and both were listed afterwards. This plan therefore carries NO mitigation for the collision: no second warehouse, no per-table warehouse override, no excluded table. Task 5.2 keeps the collision pair as a permanent regression test, registering it in BOTH orders so the test does not depend on the one order the spike happened to use.
- **Open: the AWS S3 storage flavor with virtual-hosted addressing is unexercised.** Decision [16] makes the storage flavor the single value that differs between the local run and AWS, and every planned automated test takes the S3-compatibility branch against MinIO with path-style addressing. The AWS-flavor branch — no path-style setting, virtual-hosted addressing, real S3 — has no automated coverage anywhere off the billable path. § Manual Testing row 3, the first `lakekeeper-up.sh` against AWS, is its first exercise. Consequence if it fails: warehouse creation or the first register call fails on an already-billing box, which `lakekeeper-down.sh` then removes.

**Open: whether the `data-stack` Glue catalog exposes each Iceberg table's `metadata_location` as a Glue table parameter.** This risk REPLACES the earlier plan's Glue Iceberg-REST `loadTable` risk, which is now moot: decision [27] reads the source through `aws glue get-tables`, not through Glue's Iceberg REST endpoint, so the optional REST `metadata-location` field is no longer on any code path. The new question is whether `TableList[].Parameters.metadata_location` is populated. Apache Iceberg's `GlueCatalog` writes that parameter on every commit, and the `data-stack` tables were loaded through an Iceberg writer, so the expectation is high but unverified in this account. Every planned local test uses the `rest` source producer, so the `glue` producer is unexercised off the manual path.

§ Manual Testing row 2 closes it. It is an OPERATOR step, not an implementation task: `lakekeeper-provision.sh --source-only` is run against the live `data-stack` Glue catalog from the operator's laptop, with Glue and S3 reads only and no EC2. That step MUST be run before the first `lakekeeper-up.sh` against AWS, so the source half is falsified on a laptop rather than on an already-billing box. If the parameter is absent the fallback is `aws glue get-table`'s `StorageDescriptor.Location` plus a listing of that table's `metadata/` prefix to find the newest `metadata.json`, and that branch is unplanned.

That step also checks the derivation against a concrete expectation. `deploy/data-stack/main.tf:71-74` gives the `tpch` Glue database `location_uri = "s3://<bucket>/tpch.db"`, so the eight tables should sit at `s3://<bucket>/tpch.db/<table>/` and the derived key prefix should be `tpch.db` — strictly above every table root, so the shorten-to-parent rule does not fire and the empty-prefix rejection is not reached. A `--source-only` run printing anything else means the derivation and the data disagree, which is exactly the failure this step exists to catch before an EC2 instance is billing.

Two smaller findings are already reflected in the spec: the server-info endpoint answers `401` to an anonymous caller, so the Keycloak token must be obtained before the first management call; and the bootstrap request's only required field is the terms-of-use acceptance.

`crates/lakehouse-engine/tests/common/lakekeeper.rs` records from live observation that Lakekeeper 0.13.1 reports a duplicate warehouse as `400` with a storage-profile overlap rather than `409`. That file's own comment (`lakekeeper.rs:395-399`) adds the caveat this plan must honor: for warehouses sharing a bucket, the mapping from that 400 to "identical warehouse already exists" is an unverified inference, so a caller needing certainty must read the warehouse back. The script therefore confirms every already-present classification by reading the warehouse's storage profile. The classification itself stays a live-observed rule encoded in two places; the plan schedules a consolidation follow-up rather than refactoring the E2E harness here (decision 12).

**Risk introduced by the move to bash: no compile-time JSON-shape checking.** The Rust design got its wire shapes from `lakehouse-catalog`'s `pub` request types, so no JSON key was hand-spelled and a typo failed to compile. Bash hand-spells every key. Three controls replace the compiler, in decreasing strength: every request body is built with `jq -n --arg` rather than string interpolation, so a malformed body is impossible and escaping is correct; the offline stubbed-PATH harness captures each request body the script emits and asserts its exact structure with `jq -e` against the v0.13.1 shapes recorded in the spec; and the local Docker verification sends every body to a real Lakekeeper 0.13.1, which rejects a wrong shape. See decision [24].

## Implementation Tasks

- [ ] 1.1 `deploy/lakekeeper-stack/providers.tf` and `variables.tf`, mirroring `trino-stack/providers.tf:9-36`'s `exa:*` `base_tags` + `date_tags` locals and its provider pins (`aws ~> 5.60`, `http ~> 3.4`), plus `trino-stack/variables.tf`'s `env_name`, `key_pair_name`, `allowed_cidrs` (default `[]`), `ttl_days`, and `created_date` variables. This task ALSO creates `deploy/lakekeeper-stack/locals.tf` — a NEW file, not part of `main.tf` — holding ONLY the `jsondecode(file(...))` local that parses `scripts/keycloak-realm-iceberg.json` for the realm name (`iceberg`), OAuth2 client id (`lakehouse`), client secret, and audience (`lakekeeper`). Putting it in its own file gives that local one owner and keeps tasks 1.2 and 1.3 off each other's files. `local.prefix`, `data.http.my_ip`, and `local.effective_cidrs` are NOT declared here — they belong to `main.tf` (task 1.2), matching `cluster-stack/main.tf:1-19` and `trino-stack/main.tf:13-20`
- [ ] 1.2 `deploy/lakekeeper-stack/main.tf`: `local.prefix`, the `data "http" "my_ip"` lookup against `https://checkip.amazonaws.com`, and `local.effective_cidrs` defaulting to that address `/32` when `var.allowed_cidrs` is empty, exactly as `cluster-stack/main.tf:8-19` does; the security group, admitting SSH from `local.effective_cidrs` only and ports 8181 and 8080 from `concat(local.effective_cidrs, [vpc_cidr])`, mirroring `trino-stack/main.tf:43-67`; the instance role with `s3:GetObject` scoped to the realm object; the realm export as a managed S3 object under a key that MUST sit outside any prefix the provisioning script can derive from the table locations (a dedicated `lakekeeper/` top-level key, never under the `tpch.db/` data prefix `data-stack/main.tf:71-74` sets as the Glue database's `location_uri`), so the object cannot land inside the warehouse prefix Lakekeeper's probe asserts on; the dedicated IAM user built as `aws_iam_user` + `aws_iam_policy` + `aws_iam_user_policy_attachment` + `aws_iam_access_key`, mirroring `deploy/data-stack/main.tf:94-136` — `aws_iam_user_policy` MUST NOT be used, because `deploy/iam/deployer-policy.json` grants no `iam:PutUserPolicy` and the apply would fail after the instance is already billing; generated passwords including the Keycloak bootstrap admin password and the Lakekeeper metadata-encryption key, which MUST NOT be the local compose file's literal `This-is-NOT-Secure!` (`docker-compose.lakekeeper.yml:96,111`); SSM parameters under this stack's own root; and the EC2 instance. The SSM set is BOTH kinds: `SecureString` for the PostgreSQL password, the metadata-encryption key, the Keycloak bootstrap admin password, the storage key pair, and the OAuth2 client secret; plain `String` for the warehouse name, the OAuth2 client id, and the catalog and token URIs of BOTH vantages, which are not secrets. The `String` set exists so an in-VPC caller holding no OpenTofu workspace state can assemble a complete `LK_TARGET_*` environment from SSM alone (decision [23]); it publishes the same values task 1.3 exposes as outputs and MUST NOT diverge from them. The client id and client secret are copied verbatim from the `locals.tf` `jsondecode` local, never regenerated, so `scripts/keycloak-realm-iceberg.json` stays their single owner
- [ ] 1.3 `deploy/lakekeeper-stack/outputs.tf` ONLY: public and private hosts, both ports, warehouse name, per-vantage catalog and token URIs, SSM root, security-group id. The realm name, OAuth2 client id, client secret, and audience are READ FROM the `jsondecode(file(...))` local that task 1.1 declares in `locals.tf` — this task MUST NOT re-declare that local, and MUST NOT retype those values as literals in `variables.tf`. Task 1.2 publishes the warehouse name, the OAuth2 client id, the client secret, and both vantages' catalog and token URIs to SSM as well, for the in-VPC caller that holds no workspace state; the outputs here and those parameters carry the same values and MUST NOT diverge
- [ ] 1.4 `deploy/lakekeeper-stack/lakekeeper-userdata.sh.tftpl`, boot-and-fetch half: Docker Engine install on the chosen AMI, IMDSv2 discovery of the instance's own private and public IPv4 addresses at boot (no `aws_eip` is added; the script substitutes both addresses into the compose file), and the instance-profile-authenticated S3 GET of the realm export
- [ ] 1.5 `deploy/lakekeeper-stack/lakekeeper-userdata.sh.tftpl`, compose-and-wait half: the four-service compose file with the realm-import-gated ordering the local stack proves, `LAKEKEEPER__OPENID_PROVIDER_URI` on the private-IP issuer and `LAKEKEEPER__OPENID_ADDITIONAL_ISSUERS` on the public-IP one, the Keycloak bootstrap admin password read from SSM rather than the compose file's literal `admin`, and a health wait. The Keycloak health gate MUST test the imported `iceberg` realm, not just Keycloak liveness — the local compose file needed a documented `/dev/tcp` realm-import probe to get that ordering right
- [ ] 2.1 `deploy/scripts/lakekeeper-provision.sh`, source half: `set -euo pipefail` with NO `set -x` anywhere; `LK_SOURCE_*` / `LK_TARGET_*` environment validation; the two source producers behind one normalized `(name, metadata_location, table_location)` JSON array — `LK_SOURCE_KIND=glue` (default) via `aws glue get-tables --database-name` plus `aws s3 cp <metadata_location> -` piped to `jq -r .location`, and `LK_SOURCE_KIND=rest` via an OAuth2-bearer Iceberg REST `loadTable`; the bucket and common-key-prefix derivation with the shorten-to-parent rule, mixed-bucket rejection, and empty-derived-prefix rejection; target S3 flavor and path-style derived from whether `LK_TARGET_S3_ENDPOINT` is set; and the `--source-only` mode, which prints the triples and exits BEFORE the first target-catalog call and before any write — no token request, no bootstrap, no warehouse, no namespace, no register. Every AWS call MUST pass an explicit `--region` and MUST NOT pass `--profile`; the script MUST NOT read `~/.aws/` or contact `169.254.169.254` itself, so the AWS CLI's own credential chain is the only credential source and a laptop profile and an EC2 instance profile both work unchanged
- [ ] 2.2 `deploy/scripts/lakekeeper-provision.sh`, target half: Keycloak client-credentials token; server-info read; bootstrap with 409-as-success; warehouse create with the 2xx/409/400-overlap classification plus the confirming read-back that fails unless the returned storage profile's bucket and key prefix equal the derived values; warehouse-prefix resolution from `GET /v1/config?warehouse=`; namespace create; one register call per table with `overwrite` sent explicitly as `false`; and the per-table registered/already-present/failed summary with a non-zero exit when any table failed. EVERY request body MUST be built with `jq -n` — never string interpolation or a heredoc — and every credential MUST reach `curl` through a file descriptor (`--config -` on stdin for the bearer header and the token-grant form, `--data @<(jq -n ...)` for the warehouse body that carries the S3 secret key), never through a `-u`, `-d`, or `-H` argv token. The no-argv rule binds EVERY spawned process, not `curl` alone, so the warehouse body MUST read its credential fields from jq's ENVIRONMENT — `'{"access-key-id": env.LK_TARGET_ACCESS_KEY_ID, "secret-access-key": env.LK_TARGET_SECRET_ACCESS_KEY}'` — and MUST NOT pass either value through `jq --arg` or `--argjson`, because that puts the secret in jq's own world-readable `/proc/<pid>/cmdline` exactly as a `curl -H` token would. Non-credential fields keep using `--arg`. The `delete-profile` is sent explicitly as the soft profile in its exact v0.13.1 wire shape — `{"type": "soft", "expiration-seconds": <integer>}`, kebab-case, with a one-week `604800` value matching upstream's own `tests/migrations/create-warehouse/soft-delete-1week.json`. `expiration-seconds` is REQUIRED: the upstream enum variant carries no serde default, so omitting it fails warehouse creation. Response bodies are captured to a temp file in a `mktemp -d` removed by an `EXIT` trap and MUST NOT be printed on any error path, because the warehouse request carried a storage secret; a location-already-taken register rejection is reported as a DISTINCT named failure and MUST NOT be folded into already-registered success [expert]
- [ ] 3.1 `deploy/scripts/lakekeeper-up.sh`: workspace select or create, apply, poll the health endpoint, read SSM, map the environment onto the provisioning script's `LK_*` variables, run `lakekeeper-provision.sh`, print connection details and the cost-and-teardown banner. Follows `trino-up.sh`'s structure, workspace handling, `-var` set (`env_name`, `key_pair_name`, `created_date`), health-poll loop, and closing banner shape. The source variables are mapped from the `data-stack` SSM root `/spot-strata/<env>`, whose parameter names `deploy/data-stack/main.tf:151-185` fixes: `/region`, `/bucket`, and `/namespace/tpch` — the same three `secrets.sh:22-27` already reads. The target variables come from this stack's own outputs and SSM `SecureString` parameters. The operator MUST NOT have to set any `LK_*` variable by hand for the AWS path
- [ ] 3.2 `deploy/scripts/lakekeeper-down.sh`: destroy that environment's workspace only, then drop it. Follows `trino-down.sh`
- [ ] 4.1 `bench/run.sh`: `BENCH_CATALOG` dispatch inside the existing `remote)` arm, `build_conn_password_lakekeeper`, per-arm `require` lists, per-arm `ALLOW_HTTP`, hard error on an unknown value. The report header gains a `catalog=<name>` field carrying the catalog NAME only, emitted on the REMOTE target ONLY. `bench/run.sh:378-383` writes one header block for every target, so the field is emitted conditionally the same way `delete_header_suffix` already conditions its own suffix; the DOCKER target's header keeps exactly today's bytes and carries no `catalog=` field, because `BENCH_CATALOG` defaults to `glue` and labelling a local MinIO run `catalog=glue` would write a false value into `bench/reports/*.txt`
- [ ] 4.2 Extend `bench/run.sh selftest` with the catalog-selection and Lakekeeper-password assertions, plus two header assertions: that the remote header's `catalog=` field never matches `s3://` — `bench/import_ceiling.sh:29` greps the whole report file for `s3://[^"]*/lineitem`, so an `s3://`-shaped header value poisons that downstream script — and that the DOCKER target's header carries no `catalog=` field at all, under every `BENCH_CATALOG` value including `glue`, `lakekeeper`, and unset. ALSO add `selftest: vs_teardown_is_recreate_only`, a source-text guard over `bench/run.sh`'s own file asserting that `DROP VIRTUAL SCHEMA` appears exactly once and is immediately followed by `CREATE VIRTUAL SCHEMA` (the drop-then-create pair at `bench/run.sh:351-352`), and that `DROP CONNECTION` appears nowhere — the invariant the live demo rests on, since the schema a benchmark run leaves behind is the demo's only query surface. Follow the file's existing assertion style: `case`-based shape checks with a failing `*)` branch, and exit-status `if` guards, as at `bench/run.sh:119,123,129`
- [ ] 4.3 `deploy/scripts/secrets.sh`: emit the Lakekeeper block from the stack's outputs and SSM using private-IP URIs, omit it with a printed note when no workspace exists, leave `BENCH_CATALOG` unset, keep the Glue block byte-identical
- [ ] 5.1 `deploy/scripts/tests/lakekeeper.test.sh`: the offline stubbed-PATH harness, following `deploy/scripts/tests/install.test.sh`'s structure and its `pass`/`fail`/`assert_*` helpers. Stub `tofu`, `aws`, `ssh`, `curl`, AND `jq` on a temporary PATH, recording every argv and every request body to a log. The `jq` stub is a recording wrapper that logs its own argv and then delegates to the real `jq` by absolute path, so body construction still works while jq's command line becomes assertable — it is stubbed because `jq --arg` would otherwise put a credential in a world-readable process listing that a `curl`-only assertion cannot see. The harness's own assertions MUST call the real `jq` by absolute path so they are never served by the stub. Assert the stack's ingress, IAM, and OIDC declarations: that the IAM user carries an attached managed policy and no inline user policy, that the rendered user-data declares both issuer URIs, that the Keycloak bootstrap admin password comes from SSM and the literal `admin` password appears nowhere, that the realm object's S3 key sits outside the TPC-H data prefix, and that the stack declares plain `String` SSM parameters for the warehouse name, the OAuth2 client id, and both vantages' catalog and token URIs beside its `SecureString` ones, so an in-VPC caller can assemble a complete `LK_TARGET_*` environment from SSM alone. Assert the generated `bench/.env`. Assert the provisioning script's REQUEST BODIES with `jq -e` — the bootstrap body, the warehouse body including the exact soft `delete-profile` shape and the canonical `access-key-id` / `secret-access-key` credential field names, the namespace body, and the register body with `overwrite` serialized as an explicit `false` — because bash has no compile-time JSON checking and this harness is what replaces it. Assert the credential-hygiene rules: no recorded argv token from ANY stubbed command — `curl`, `jq`, `aws`, `tofu`, or `ssh` — ever contains a secret value, and specifically that the warehouse body's access key id and secret access key reach `jq` through `env.LK_TARGET_ACCESS_KEY_ID` / `env.LK_TARGET_SECRET_ACCESS_KEY` and appear in no `--arg` or `--argjson` token; no AWS call passes `--profile`, every AWS call passes `--region`, and the script's own text contains no `set -x`, no `-X DELETE`, no `--request DELETE`, no `purgeRequested`, and no `aws s3 rm` / `aws s3api delete-object` / `aws s3api delete-objects`
- [ ] 5.2 `deploy/scripts/tests/lakekeeper-local.test.sh`: the local Docker integration verification. Run `lakekeeper-provision.sh` twice against the `docker-compose.lakekeeper.yml` stack with `LK_SOURCE_KIND=rest`, asserting the second run reports the warehouse already present and every table already registered. Round-trip Lakekeeper-created throwaway tables through register-table into a target namespace. Pre-populate the target key prefix with those tables' objects BEFORE creating the target warehouse, so the AWS ordering (data first, warehouse second) is the ordering under test. Drop each throwaway source table from the source namespace WITHOUT purging its files before registering it into the target, so no live table still holds the location and the assertion is about the location rule rather than about exact-location reuse. This test script is the ONE permitted site of a drop call: it is a test file, not the provisioning script task 5.1 scans, and it never runs against AWS. A purge-drop MUST NOT be used even here — the drop's purge switch stays `false`. Assert that a non-colliding pair registers successfully as the positive control, and that a `part`/`partsupp`-shaped pair whose locations differ only by a non-slash-delimited suffix ALSO registers successfully. Register that colliding pair in BOTH orders — `part` then `partsupp`, and `partsupp` then `part` — because the live spike used only the first order, so a single-order test cannot detect a Lakekeeper version that rejects only the reverse. Fails, never skips, when the stack is down [expert]
- [ ] 5.3 Wire it in: this task is the SOLE owner of the `Makefile` in this plan. It adds `test-lakekeeper-scripts` (task 5.1), `test-lakekeeper-local` (task 5.2), and `lint-lakekeeper-scripts` (shellcheck over the three new scripts), following the `test-install` (`Makefile:216-221`) and `lint-install` (`Makefile:223-228`) precedent including the latter's `command -v shellcheck` guard, and adds all three to `.PHONY` (`Makefile:267`). NO CI job is added, for either the shell tests or `tofu fmt`/`tofu validate`. `ci.yml:401-405` states why the `install-script` job exists: `install.sh` is fetched straight off `main` by every user's `curl | bash` one-liner with no release gate. These scripts have no such exposure, no workflow gates any of the four existing stacks, and `bench/run.sh selftest` is likewise absent from CI (`bench/run.sh:4`). Adding CI coverage for `deploy/` shell and OpenTofu is a separate, repo-wide concern; the checks stay local pre-commit steps in § Verification § Checklist
- [ ] 6.1 `deploy/README.md`: a "Lakekeeper (ephemeral, opt-in)" subsection with the same cost callout shape as the Trino one; BOTH directions of `bench-remote.sh`'s teardown trap — that it does not cover the Lakekeeper stack, so `lakekeeper-down.sh <env>` is a separate mandatory step, AND that `bench-remote.sh:55`'s `trap teardown EXIT` runs `cluster-down.sh <env>` on every exit path unless `KEEP_ALIVE=1` was exported, destroying the Exasol cluster that carries the CONNECTION and the virtual schema; the Files list; and a DEMO RUNBOOK. The runbook MUST state that a DEFAULT `bench-remote.sh <env>` run ENDS the demo, and MUST give the FULL ORDERED sequence for both surviving forms. WRAPPER form: `deploy/scripts/lakekeeper-up.sh <env>` FIRST, then `AWS_PROFILE=... BENCH_CATALOG=lakekeeper KEEP_ALIVE=1 ./bench-remote.sh <env>` — the wrapper does the `cluster-stack` `tofu apply`, `cluster-up.sh`, `secrets.sh`, and `make bench` itself at its steps `[1/4]` through `[4/4]` (`bench-remote.sh:61-70`). UNWRAPPED form: the `cluster-stack` `tofu apply` of `deploy/README.md` § "2. Test cluster", then `deploy/scripts/lakekeeper-up.sh <env>`, then `deploy/scripts/cluster-up.sh <env>`, then `deploy/scripts/secrets.sh <env>`, then `BENCH_CATALOG=lakekeeper make bench` — `cluster-up.sh` runs c4 against nodes the apply already created, so the sequence cannot start at it. Both forms MUST carry `BENCH_CATALOG=lakekeeper` explicitly, because the variable defaults to `glue` and the wrapper passes caller-exported `BENCH_*` through untouched, so omitting it demonstrates Glue at a live customer session. Both forms MUST run `lakekeeper-up.sh <env>` BEFORE `secrets.sh <env>`, including the `secrets.sh` call the wrapper makes internally at step `[3/4]`, because `secrets.sh` emits the Lakekeeper block only while a Lakekeeper stack workspace exists for that environment. The runbook then closes with the interactive SQL session and an explicit `deploy/scripts/cluster-down.sh <env>` plus `deploy/scripts/lakekeeper-down.sh <env>`, because `KEEP_ALIVE=1` and the unwrapped path both leave a billing cluster behind. It MUST name the fact the runbook rests on — `bench/run.sh:351-352` drops and recreates the virtual schema at the start of a run and never at the end — and MUST state that `selftest: vs_teardown_is_recreate_only` guards `bench/run.sh` only and cannot see the wrapper's trap. Four Known-seams entries: the static-credentials seam, the repo-committed OAuth2 client secret whose only control is the security group, the bucket-wide write grant on the catalog's storage credential, and the cleartext-provisioning seam — that a public-vantage `lakekeeper-up.sh` / `lakekeeper-provision.sh` run carries the OAuth2 client secret, the resulting bearer token, and the warehouse's write-capable S3 access key id and secret access key over plain HTTP across the public internet to the box's public IP, that EVERY deployment carries at least one such run because `lakekeeper-up.sh` is operator-machine-only in both contexts and always provisions (so the in-VPC vantage avoids the hop only for the optional re-provision), and that the security-group `/32` allowlist bounds WHO CAN REACH the plaintext port but neither encrypts the traffic nor stops an observer on the path between an allowlisted client and the box (decision [28])
- [ ] 6.2 `bench/README.md` modes and gotchas, and `bench/.env.example`: the new variables and the warehouse-is-a-name gotcha beside the existing Glue one
- [ ] 6.3 `deploy/DEMO.md`: a standalone, presenter-facing script for the live customer demo, separate from task 6.1's README runbook (which stays the operational reference and is cited here, not restated). Contains: the exact ordered setup commands (link to task 6.1's WRAPPER form as the default path); a short, curated list of SQL queries to run live against the Lakekeeper-backed virtual schema, picked for clear, fast results rather than task 4.2's full bench sweep, each with a one-line talking point; and the exact teardown commands (`cluster-down.sh <env>` and `lakekeeper-down.sh <env>`) as the closing step, so a presenter has them in the same document they are reading from mid-demo rather than needing to switch to `deploy/README.md`

## Parallelization

| Parallel Group | Tasks |
|----------------|-------|
| Group A | 1.1, 1.2, 1.3, 4.1 |
| Group B | 1.4, 4.2 |
| Group C | 1.5, 2.1, 4.3 |
| Group D | 2.2, 3.2 |
| Group E | 3.1, 5.2 |
| Group F | 5.1, 5.3, 6.1, 6.2 |

Sequential dependencies:
- `1.4 -> 1.5` — both tasks edit `lakekeeper-userdata.sh.tftpl`, and 1.5's compose file substitutes the addresses 1.4 discovers
- `2.1 -> 2.2` — both tasks edit `lakekeeper-provision.sh`, and 2.2's flow consumes the normalized triple and the derived prefix 2.1 produces, so running them concurrently is a read-modify-write race on one file
- `4.1 -> 4.2` — both tasks edit `bench/run.sh`, and 4.2's selftest asserts the dispatch 4.1 introduces
- `1.2, 1.3 -> 4.3` — `secrets.sh` reads the stack outputs task 1.3 declares
- `2.2 -> 3.1` — the up-script runs the finished script, so its variable, argument, and exit-code contract is fixed by 2.2
- `2.2 -> 5.2` — the local verification drives the finished provisioning flow
- `2.2 -> 5.1` — the offline harness asserts the request bodies 2.2 emits
- Group A → Group B (`1.4` needs the stack's variables; `4.1 -> 4.2` crosses this boundary)
- Group B → Group C (`1.4 -> 1.5` crosses this boundary)
- Group C → Group D (`2.1 -> 2.2` crosses this boundary)
- Group D → Group E (`2.2 -> 3.1` and `2.2 -> 5.2` cross this boundary)
- Group E → Group F (`2.2 -> 5.1`; the docs describe the finished scripts)

Per-group file ownership. Every task below names the file or files it touches, so the no-shared-file claim is checkable rather than asserted. `LKS` abbreviates `deploy/lakekeeper-stack`, `DS` abbreviates `deploy/scripts`.

| Group | Task | Files touched |
|-------|------|---------------|
| A | 1.1 | `LKS/providers.tf`, `LKS/variables.tf`, `LKS/locals.tf` |
| A | 1.2 | `LKS/main.tf` |
| A | 1.3 | `LKS/outputs.tf` |
| A | 4.1 | `bench/run.sh` |
| B | 1.4 | `LKS/lakekeeper-userdata.sh.tftpl` |
| B | 4.2 | `bench/run.sh` |
| C | 1.5 | `LKS/lakekeeper-userdata.sh.tftpl` |
| C | 2.1 | `DS/lakekeeper-provision.sh` |
| C | 4.3 | `DS/secrets.sh` |
| D | 2.2 | `DS/lakekeeper-provision.sh` |
| D | 3.2 | `DS/lakekeeper-down.sh` |
| E | 3.1 | `DS/lakekeeper-up.sh` |
| E | 5.2 | `DS/tests/lakekeeper-local.test.sh` |
| F | 5.1 | `DS/tests/lakekeeper.test.sh` |
| F | 5.3 | `Makefile` |
| F | 6.1 | `deploy/README.md` |
| F | 6.2 | `bench/README.md`, `bench/.env.example` |

No file appears twice within any one group. The three files that two tasks each need are split across group boundaries: `bench/run.sh` (4.1 in A, 4.2 in B), `LKS/lakekeeper-userdata.sh.tftpl` (1.4 in B, 1.5 in C), and `DS/lakekeeper-provision.sh` (2.1 in C, 2.2 in D). One file, one owner, one group.

## Dead Code Removal

| Type | Location | Reason |
|------|----------|--------|
| None | — | Purely additive. No existing function, test, or module is replaced: `build_conn_password_cloud`, `build_conn_password_local`, `build_vs_extra_props`, the `remote)` arm's Glue path, and every existing stack and script stay in use unchanged. The Rust `crates/lakekeeper-provision` this plan previously proposed was never written, so its removal from the plan deletes no code |

## Verification

### Scenario Coverage

| Scenario | Test Type | Test Location | Test Name |
|----------|-----------|---------------|-----------|
| An ephemeral Lakekeeper stack stands up in the cluster's VPC | Integration | `deploy/scripts/tests/lakekeeper.test.sh` | `test_up_applies_only_this_stack_and_waits_for_health` |
| Keycloak issues tokens both issuers accept | Integration | `deploy/scripts/tests/lakekeeper.test.sh` | `test_rendered_userdata_declares_both_issuer_uris_and_ssm_sourced_admin_password` |
| The catalog's storage credential is separate from the engine's read-only credential | Integration | `deploy/scripts/tests/lakekeeper.test.sh` | `test_stack_declares_a_distinct_iam_user_with_an_attached_managed_policy` |
| Provisioning bootstraps Lakekeeper and creates the S3-backed warehouse idempotently | Integration | `deploy/scripts/tests/lakekeeper-local.test.sh` | `test_bootstrap_and_warehouse_creation_are_idempotent` |
| Source-cataloged Iceberg tables are registered into the warehouse without a data rewrite | Integration | `deploy/scripts/tests/lakekeeper-local.test.sh` | `test_register_table_by_reference_preserves_metadata_location` |
| Provisioning runs unchanged from an operator's laptop and from an EC2 box | Integration | `deploy/scripts/tests/lakekeeper.test.sh` | `test_provision_uses_only_the_aws_credential_chain_and_an_explicit_region` |
| No credential reaches a process listing, standard output, or an error body | Integration | `deploy/scripts/tests/lakekeeper.test.sh` | `test_no_secret_in_recorded_argv_or_output_and_no_set_x` |
| Bench secrets carry both catalogs' variables from one environment | Integration | `deploy/scripts/tests/lakekeeper.test.sh` | `test_secrets_emits_lakekeeper_block_beside_untouched_glue_block` |
| Teardown removes only the Lakekeeper stack | Integration | `deploy/scripts/tests/lakekeeper.test.sh` | `test_down_destroys_only_the_lakekeeper_workspace` |
| Remote bench selects its catalog backend from the bench environment | Integration | `bench/run.sh` selftest block | `selftest: bench_catalog_selection` |
| The Lakekeeper CONNECTION password carries OAuth2 credentials and never SigV4 | Integration | `bench/run.sh` selftest block | `selftest: lakekeeper_conn_password_shape` |
| A completed remote run leaves the CONNECTION and virtual schema in place | Integration | `bench/run.sh` selftest block | `selftest: vs_teardown_is_recreate_only` |

`selftest: vs_teardown_is_recreate_only` covers the harness's own source text and nothing else. It cannot see `deploy/scripts/bench-remote.sh:55`'s EXIT trap, which destroys the cluster carrying the CONNECTION and the virtual schema unless `KEEP_ALIVE=1` was exported, so a green result is not evidence that an operator-level run left a demo surface behind. Task 6.1's runbook carries that half.

Three rows name a shell test that can only assert what a rendered template declares, never that a token is accepted or that a policy grants what it says. Those three test names say exactly that, and § Manual Testing carries the behavioral half as a step on the real box.

The stack scenario's SSM-publication clauses ride on that same declaration scan: `test_up_applies_only_this_stack_and_waits_for_health` asserts the plain `String` parameters exist beside the `SecureString` ones. The run-site scenario's instance-profile clause has no automated owner and is not meant to have one — it constrains a box this plan does not create — so § Manual Testing row 5 carries it as a stated prerequisite.

The storage-credential scenario's no-destructive-path clause and its soft-`delete-profile` clause are both covered by `deploy/scripts/tests/lakekeeper.test.sh`, which scans the provisioning script's own text for the forbidden verbs and asserts the warehouse request body's `delete-profile` shape with `jq -e`.

Request-body coverage in `deploy/scripts/tests/lakekeeper.test.sh` replaces the Rust design's sibling `_tests.rs` unit tests one for one, because bash offers no compile-time JSON checking:

- Environment validation, source-kind selection, target flavor and path-style derivation, and the run-mode parse: `--source-only` selects the source-only mode, no argument selects the full flow, and an unknown argument is rejected.
- Bucket and common-prefix derivation, mixed-bucket rejection, empty-derived-prefix rejection, and the shorten-to-parent rule.
- Status classification, bootstrapped-flag parsing, storage-profile read-back comparison, and request-body shapes including the soft `delete-profile`'s exact wire shape: `{"type": "soft", "expiration-seconds": <integer>}`. `expiration-seconds` carries no serde default upstream, so the test asserts the field is present and integer-valued.
- Prefix-aware endpoint construction, register and namespace body shapes with `overwrite` serialized as an explicit `false`, table-identifier mapping, the full register-response status classification (registered, already-registered as success, location-already-taken as a DISTINCT named failure, any other status as a named failure), and the reserved-namespace rejection for `system`, `examples`, and `information_schema`.

### Manual Testing

| Feature | Command | Expected Output |
|---------|---------|-----------------|
| aws-lakekeeper-perf-catalog (local, no AWS spend) | `docker compose -f docker-compose.yml -f docker-compose.lakekeeper.yml up -d --wait minio keycloak lakekeeper-db lakekeeper-migrate lakekeeper` then `make test-lakekeeper-local` | Both integration checks pass; the second provisioning run reports the warehouse already present and every table already registered |
| aws-lakekeeper-perf-catalog (source read side against live Glue — no billable resource; MUST run before the first AWS `lakekeeper-up.sh`) | `AWS_PROFILE=spot-strata-deployer deploy/scripts/lakekeeper-provision.sh --source-only` with the `LK_SOURCE_*` variables pointing at the `data-stack` Glue database | Eight table names printed, each with a non-empty `metadata_location` and a non-empty `table_location`; Glue and S3 reads only; no EC2 instance, warehouse, or write. The script exits non-zero naming any table whose Glue entry carries no `metadata_location` parameter |
| aws-lakekeeper-perf-catalog (real AWS, only with explicit go-ahead — billable EC2) | `AWS_PROFILE=spot-strata-deployer deploy/scripts/lakekeeper-up.sh <env>` | Stack applied; health endpoint answers; per-table summary lists every TPC-H table as registered; connection details and the cost-and-teardown banner printed |
| aws-lakekeeper-perf-catalog (the EC2 run site, SOURCE half — free, no target call) | On an EC2 box carrying an instance profile and no `~/.aws/credentials`: `AWS_REGION=<region> deploy/scripts/lakekeeper-provision.sh --source-only` with the same `LK_SOURCE_*` variables | Identical output to the laptop run above, with no `AWS_PROFILE` set and no credentials file present, confirming the AWS CLI credential chain resolves the instance profile with no change to the script |
| aws-lakekeeper-perf-catalog (the EC2 run site, FULL provisioning flow — closes the "runs from either location" claim; needs the stack already applied from the operator's machine, and an in-VPC box the operator supplies, since this plan creates no such box and no such instance profile) | On that same in-VPC EC2 box, whose instance profile grants `ssm:GetParameter` on both SSM roots, `kms:Decrypt`, `glue:GetTables`, and `s3:GetObject` on the data prefix, against the stack a laptop `lakekeeper-up.sh <env>` already applied: export `LK_SOURCE_*` from the `data-stack` SSM root and every `LK_TARGET_*` value from this stack's SSM root — warehouse name, OAuth2 client id, and the PRIVATE-IP catalog and token URIs from its plain `String` parameters, client secret and storage key pair from its `SecureString` parameters — reading NO OpenTofu output, then run `deploy/scripts/lakekeeper-provision.sh` with no arguments | The full target half runs from EC2 — token, server-info, bootstrap, warehouse create plus the confirming read-back, namespace, and one register call per table — and the per-table summary reports every table already present, exit 0. Nothing is created twice, confirming the flow is idempotent and run-site agnostic. This is the ONLY step that exercises the target half from EC2; `--source-only` above cannot |
| aws-lakekeeper-perf-catalog (behavioral half of the template-only shell tests) | On the applied box: request a token from the PUBLIC-IP Keycloak issuer and call the Lakekeeper management API with it, then repeat from a cluster node against the PRIVATE-IP issuer; separately, call `aws s3api put-object` on the `data-stack` bucket with the `engine-reader` key pair | Both tokens accepted by the same Lakekeeper server; the `engine-reader` put is denied, confirming the query path holds no write permission |
| cloud-e2e-harness (Glue unchanged) | `deploy/scripts/secrets.sh <env>` then `make bench` | Report header names the Glue catalog; row counts and query set identical to a pre-change `bench/reports/*.txt` run for the same environment |
| cloud-e2e-harness (benchmark suite) | `BENCH_CATALOG=lakekeeper make bench` | Virtual schema exposes the same eight TPC-H table names; per-table row counts match the Glue run for the same data; timings recorded for comparison |
| cloud-e2e-harness (demo suite — same stack, same run, interactive tail) | After the benchmark run above, from a SQL client: `SELECT COUNT(*) FROM <virtual_schema>.LINEITEM` and any ad-hoc TPC-H query | The virtual schema and its CONNECTION are still present and queryable, because the harness drops and recreates them at the start of a run and never at the end |
| aws-lakekeeper-perf-catalog (teardown) | `deploy/scripts/lakekeeper-down.sh <env>` then `aws ec2 describe-instances` | The Lakekeeper instance is terminated; the Exasol cluster, S3 bucket, and Glue catalog still present |

### Checklist

| Step | Command | Expected |
|------|---------|----------|
| Rust workspace untouched | `git diff --stat -- Cargo.toml Cargo.lock crates/` | No output |
| Build (`.so` unaffected) | `make cross-udf-build` | Exit 0 |
| Test (host unit) | `cargo test` | 0 failures, unchanged from the pre-change baseline |
| Test (shell harness, offline) | `make test-lakekeeper-scripts` | 0 failures |
| Test (bench self-check) | `./bench/run.sh selftest` | prints `selftest OK` |
| Test (local provisioning integration) | `make test-lakekeeper-local` | 0 failures |
| Terraform | `cd deploy/lakekeeper-stack && tofu init -backend=false && tofu validate && tofu fmt -check` | Success, no reformatting |
| Lint (shell) | `make lint-lakekeeper-scripts` | 0 errors |

## Notes for the PR

Per `CLAUDE.md` § "Feature tracking", this feature must also be tracked as a GitHub issue. Add that issue reference to the PR description and to the implementing commit (`Closes #<n>`). Also open the consolidation follow-up issue that decision 12 schedules — sharing the Lakekeeper bootstrap and warehouse idempotency classification between `deploy/scripts/lakekeeper-provision.sh` and `crates/lakehouse-engine/tests/common/lakekeeper.rs` — and cite it in the new feature's spec at record time.

PR #380 was opened against the earlier Rust-based design. Its description must be rewritten to match this plan before review: the `crates/lakekeeper-provision` deliverable is gone, and the Rust workspace is untouched.
