# `deploy/` — programmatic AWS perf-test environment

Reproducible AWS environment for benchmarking the lakehouse engine. Two OpenTofu stacks:

- **`data-stack/`** — persistent. VPC, S3 gateway endpoint, S3 warehouse bucket, Glue Iceberg
  databases `tpch` and `perf`, an Athena workgroup, a scoped `engine-reader` IAM user, and SSM
  secrets. It also runs a temporary data-gen EC2 instance that loads TPC-H and wide perf tables,
  then self-terminates.
- **`cluster-stack/`** — ephemeral, one per `$env_name` (OpenTofu workspace). N Exasol EC2 nodes
  (2 by default) with two EBS volumes each, an IP-allowlisted security group, and the cluster
  passwords in SSM.

The catalog is AWS Glue's Iceberg REST endpoint. Both the lakehouse engine (SigV4 REST + S3) and
AWS Athena (natively) can query it. `make bench` runs unchanged against it.

Every resource is named `<project>-<env_name>-*` and tagged per the `exa:*` policy.

## 0. One-time setup

```bash
# Toolchain on this machine (OpenTofu, AWS CLI v2, jq, ...):
sudo deploy/scripts/install-prereqs.sh

# IAM deployer principal — follow the step-by-step:
deploy/iam/SETUP.md         # create the deployer user, attach deployer-policy.json, make a key
export AWS_PROFILE=<project>-deployer
aws sts get-caller-identity # verify

# An EC2 key pair for the cluster nodes. Seed it once — this generates the key, imports the EC2
# key pair, and stores the private key in SSM SecureString for the whole team:
deploy/scripts/rotate-cluster-key.sh          # default key name: <project>-key
```

This key is shared across every `env_name` workspace. `cluster-up.sh` fetches it from SSM
automatically when it is not already local, so a teammate with deployer credentials needs no
manual key-sharing step. If the key is lost, compromised, or due for scheduled rotation, run
`rotate-cluster-key.sh` again — see the script's header comment for the exact mechanism.

## 1. Data + catalog stack (persistent — do once)

```bash
cd deploy/data-stack
tofu init
tofu apply                              # VPC, S3, Glue dbs, Athena, engine-reader, SSM
tofu apply -var run_data_gen=true       # launch the data-gen EC2 (loads tpch+perf, self-terminates)

# watch progress (instance self-terminates when DONE):
aws ssm get-parameter --name "$(tofu output -raw datagen_status_param)" --query Parameter.Value --output text
tofu apply -var run_data_gen=false      # reconcile state after it's gone (data persists in S3/Glue)
```

Sizing knobs live in `variables.tf`: `tpch_scale_factor` (default 30, about 5-6 GB lineitem),
`perf_table_sizes_gb` (default `[10,20,30,40,80]`), `lineitem_files`, `perf_files`,
`datagen_instance_type`, `datagen_scratch_gb`. Same-region EC2-to-S3 transfer has no
data-transfer cost. The only standing cost is S3 storage, about 110 GiB for the full set.

> **Perf table sizes are approximate.** `gen_load.py` estimates bytes per row from a small
> sample, but snappy compresses better at scale. `t_80g` lands at about 46 GiB on disk (57% of
> the 80 GB label). Row-count ratios (10:20:30:40:80) are exact. Bump the targets or the
> calibration sample for precise on-disk sizes.

## 2. Test cluster (ephemeral — per benchmark)

```bash
cd deploy/cluster-stack
tofu init
tofu workspace new myenv                # or: tofu workspace select myenv
tofu apply -var env_name=myenv -var key_pair_name=<project>-key
#   defaults: node_count=2, reserve_nodes=0, instance_type=r8i.2xlarge, os_disk_gb=50, data_disk_gb=300
#   allowed_cidrs defaults to THIS machine's public IP /32 — add CIDRs (or 0.0.0.0/0) to widen.

../scripts/cluster-up.sh myenv          # render .ccc/config, c4 host play, wait for DB
#   (auto-fetches the shared SSH key from SSM if it isn't already local — no manual key copy needed)
../scripts/secrets.sh myenv             # write bench/.env (host + Glue creds from outputs/SSM)
```

## 3. Run the perf test (unchanged)

```bash
cd ../..        # repo root
make bench       # builds .so, installs SLC + .so to BucketFS, runs Q1–Q4, writes bench/reports/
```

Athena benchmark (same catalog): `bench/athena_compare.sh` runs the Q1-Q4 set against the
`<project>-<env>-athena` workgroup automatically (see `bench/README.md`). No infra to stand up.

> **Quick path (recommended):** `deploy/scripts/bench-remote.sh <env>` chains the `cluster-stack`
> `tofu apply` with steps 2, 3, and 5 (`cluster-up.sh` → `secrets.sh` → `make bench` →
> `cluster-down.sh`) into one command. It always tears the cluster down — on success, failure, or
> interrupt — because it installs its teardown trap before bringing anything up. Any
> `BENCH_*`/`LAKEHOUSE_*` env var you export (for example `BENCH_WITH_DELETES=1`) flows through
> untouched to `make bench`:
> ```bash
> AWS_PROFILE=<project>-deployer deploy/scripts/bench-remote.sh test1
> AWS_PROFILE=<project>-deployer BENCH_WITH_DELETES=1 deploy/scripts/bench-remote.sh test1
> ```
> A live `r8i.2xlarge` × N cluster bills continuously, so guaranteed teardown is the point. The
> script's final line states whether teardown ran, but verify actual termination yourself with
> `aws ec2 describe-instances`. The manual step 2/3/5 sequence above still works — use it directly
> for fine-grained control, for example leaving the cluster up between repeated `make bench` runs.

### Delete-bearing benchmark prerequisite (remote, one-time per environment)

`BENCH_WITH_DELETES=1` (see `bench/README.md`) runs the perf test against Iceberg v2
merge-on-read, 5%-position-deleted copies of the TPC-H tables. The default
(`BENCH_WITH_DELETES=0`) is byte-for-byte identical to the existing benchmark behavior. In remote
mode, author the delete-bearing tables once per environment before running — `run.sh` does not
author them itself in remote mode, unlike docker mode:

```bash
cd deploy/data-stack   # requires enable_emr_serverless=true (see section 4)
export EMR_SERVERLESS_APP_ID=$(tofu output -raw emr_serverless_app_id)
export EMR_SERVERLESS_ROLE_ARN=$(tofu output -raw emr_serverless_job_role_arn)
export SPARK_DELETES_SCRIPT_S3_URI=$(tofu output -raw spark_deletes_script_s3_uri)
export SPARK_LOG_S3_URI=$(tofu output -raw emr_serverless_log_uri)
cd ../.. && deploy/scripts/make-deletes-remote.sh
```

This submits `deploy/scripts/make_deletes_remote.py`, which builds the `tpch_deletes` Glue
database (8 MOR tables, deterministic ~5% position deletes) from the existing `tpch` Glue tables.
It is idempotent and safe to re-run. Skip this step and `run.sh BENCH_WITH_DELETES=1` hard-errors,
pointing back at this script.

## 4. Competitive comparison (Athena / Trino / Spark, opt-in)

This section covers standing up the Trino/Spark compute. See `bench/README.md`'s "Competitive
engine comparison" section for the compare scripts themselves.

### Trino (ephemeral, opt-in)

`deploy/trino-stack/` mirrors `cluster-stack/`: a coordinator and worker cluster running Trino in
Docker, sized by `instance_type`/`node_count` (default `r8i.2xlarge` × 2, matching an Exasol
`test1` node). Its Iceberg connector talks to Glue directly via `iceberg.catalog.type=glue`,
authenticated by an instance-profile role — no static keys.

```bash
cd deploy/trino-stack && tofu init
../scripts/trino-up.sh myenv    # tofu apply + wait for the coordinator + all workers to join
export TRINO_HOST=<printed coordinator ip>
cd ../.. && bench/trino_compare.sh
../scripts/trino-down.sh myenv
```

> **Cost / teardown:** these EC2 nodes bill while they exist, and `r8i.2xlarge` × 2 costs
> meaningfully more than a single small box. Only an explicit `trino-up.sh` run creates them. Tear
> them down immediately after the benchmark run with `trino-down.sh <env>` — there is no
> auto-stop. Verify with `aws ec2 describe-instances` that both nodes terminated.

### Spark / EMR Serverless (pay-per-job, opt-in)

Spark runs via AWS EMR Serverless instead of a persistent or ephemeral cluster: an application
resource that costs nothing at rest and auto-stops after an idle job. It is added to
`deploy/data-stack` behind a toggle, off by default:

```bash
cd deploy/data-stack
tofu apply -var enable_emr_serverless=true    # creates the (idle, $0) EMR Serverless application

export EMR_SERVERLESS_APP_ID=$(tofu output -raw emr_serverless_app_id)
export EMR_SERVERLESS_ROLE_ARN=$(tofu output -raw emr_serverless_job_role_arn)
export SPARK_SCRIPT_S3_URI=$(tofu output -raw spark_script_s3_uri)
export SPARK_LOG_S3_URI=$(tofu output -raw emr_serverless_log_uri)
cd ../.. && bench/spark_compare.sh
```

> **Cost / teardown:** the application is free while idle. It bills only for vCPU/memory while a
> job runs, and `auto_stop_configuration` stops it after 15 idle minutes even if you forget. To
> remove it entirely: `tofu apply -var enable_emr_serverless=false` in `data-stack`.

> **One-time prerequisite:** the `<project>-deployer` policy needs an added `emr-serverless:*`
> statement (already in `deploy/iam/deployer-policy.json`). Bump the live policy version once per
> account — see "Updating the policy" in `deploy/iam/SETUP.md`.

## Lakekeeper (ephemeral, opt-in)

A second, opt-in Iceberg REST catalog for the `remote` bench target and for live demos, selected
by `BENCH_CATALOG=lakekeeper` (default `glue`). `deploy/lakekeeper-stack/` layers on the
persistent `data-stack` and stands up one EC2 box running PostgreSQL, Keycloak, and Lakekeeper —
the same service set as `docker-compose.lakekeeper.yml`. `deploy/scripts/lakekeeper-provision.sh`
then registers every already-cataloged TPC-H Iceberg table into it by reference — no data
rewrite, no second physical copy. With `BENCH_CATALOG` unset, Glue is unaffected: every required
variable, catalog URI, CONNECTION password, virtual-schema property, query set, and row count
stays as it is today.

`secrets.sh` and `make bench` also need an applied `cluster-stack` plus a completed
`cluster-up.sh <env>` (§ "2. Test cluster" above). See the Demo runbook below for the full
ordered sequence.

```bash
cd deploy/lakekeeper-stack && tofu init && cd ../..   # one-time
AWS_PROFILE=<project>-deployer deploy/scripts/lakekeeper-up.sh myenv   # apply + provision
deploy/scripts/secrets.sh myenv                                         # adds the Lakekeeper block to bench/.env
BENCH_CATALOG=lakekeeper make bench
deploy/scripts/lakekeeper-down.sh myenv                                 # destroy it — see cost note below
```

> **Cost / teardown:** this EC2 box bills while it exists. Only an explicit `lakekeeper-up.sh` run
> creates it. Tear it down immediately after the benchmark run or demo with
> `lakekeeper-down.sh <env>` — there is no auto-stop. Verify with `aws ec2 describe-instances`
> that the instance terminated.

### `bench-remote.sh`'s teardown trap cuts both ways

`deploy/scripts/bench-remote.sh` installs `trap teardown EXIT`. That trap's relationship to the
Lakekeeper stack runs in two directions:

- **It does not cover the Lakekeeper stack.** The trap only knows about the Exasol
  `cluster-stack`, the same relationship `deploy/trino-stack` has to this wrapper.
  `lakekeeper-down.sh <env>` is always a separate, mandatory step.
- **It does cover the Exasol cluster — this is the direction that ends a demo by surprise.**
  Unless you export `KEEP_ALIVE=1`, the trap runs `cluster-down.sh <env>` on every exit path:
  success, failure, or interrupt. This destroys the Exasol cluster carrying the CONNECTION and
  the virtual schema a Lakekeeper run just built. A default `bench-remote.sh <env>` run therefore
  ends the demo it just set up. The runbook below gives the two sequences that survive it.

### Demo runbook

See `DEMO.md` for the presenter-facing companion — a standalone, copy-pasteable script for
running the demo live.

A live customer demo and the automated benchmark share one stack, one provisioning path, one
warehouse, one namespace, and one virtual schema. Only who issues the commands afterward differs,
not what the tooling does. Two sequences below survive the trap above. The bare wrapper
(`bench-remote.sh <env>` with no `KEEP_ALIVE`) is neither of them, and ends the demo.

Both sequences must carry `BENCH_CATALOG=lakekeeper` explicitly: `bench/run.sh` defaults to
`glue`, and the wrapper passes every caller-exported `BENCH_*`/`LAKEHOUSE_*` variable through
untouched. Omitting it silently demonstrates Glue at a live customer session. Both sequences must
also run `lakekeeper-up.sh <env>` before `secrets.sh <env>` — including the `secrets.sh` call the
wrapper makes internally at its step `[3/4]` — because `secrets.sh` emits the Lakekeeper block
into `bench/.env` only while a Lakekeeper stack workspace exists for that environment.

**Wrapper form** (default path). `bench-remote.sh` performs the `cluster-stack` `tofu apply`,
`cluster-up.sh`, `secrets.sh`, and `make bench` itself, at its steps `[1/4]`-`[4/4]`:

```bash
AWS_PROFILE=<project>-deployer deploy/scripts/lakekeeper-up.sh myenv
AWS_PROFILE=<project>-deployer BENCH_CATALOG=lakekeeper KEEP_ALIVE=1 deploy/scripts/bench-remote.sh myenv
```

**Unwrapped form** (fine-grained control). `cluster-up.sh` runs c4 against nodes an apply already
created, so this sequence starts with the `cluster-stack` apply from § "2. Test cluster" above:

```bash
cd deploy/cluster-stack
tofu workspace new myenv   # or: tofu workspace select myenv
tofu apply -var env_name=myenv -var key_pair_name=<project>-key
cd ../..

AWS_PROFILE=<project>-deployer deploy/scripts/lakekeeper-up.sh myenv
AWS_PROFILE=<project>-deployer deploy/scripts/cluster-up.sh myenv
AWS_PROFILE=<project>-deployer deploy/scripts/secrets.sh myenv
BENCH_CATALOG=lakekeeper make bench
```

Either sequence leaves the CONNECTION and the `TPCH` virtual schema in place and queryable
afterward. `bench/run.sh` drops and recreates the virtual schema as one pair at the start of a
run, never at the end, so the schema a benchmark run leaves behind is the demo's query surface.
Continue straight into the interactive tail from a SQL client connected to
`EXASOL_HOST`/`LH_EXASOL_PORT` (from the just-written `bench/.env`):

```sql
SELECT COUNT(*) FROM TPCH.LINEITEM;
-- ... any ad-hoc TPC-H query against TPCH.* ...
```

`./bench/run.sh selftest`'s `vs_teardown_is_recreate_only` check guards this invariant, but only
as a source-text check over `bench/run.sh` itself. It cannot see `bench-remote.sh`'s EXIT trap. A
green result proves the harness's own teardown timing is correct — it is not evidence that an
operator-level run left the demo surface behind. Close every demo with both teardown commands,
regardless of which form stood it up:

```bash
AWS_PROFILE=<project>-deployer deploy/scripts/cluster-down.sh myenv
AWS_PROFILE=<project>-deployer deploy/scripts/lakekeeper-down.sh myenv
```

## 5. Tear down

```bash
cd deploy/cluster-stack && ../scripts/cluster-down.sh myenv   # destroy cluster, keep data
# eventually:
cd ../data-stack && tofu destroy                              # remove data + catalog
```

## Security model

All cluster ingress (SSH/c4 ports 22/20002/20003, client ports 8563/8443/2581) is restricted to
`allowed_cidrs`, which defaults to this machine's public IP. Add CIDRs (or `0.0.0.0/0`) to open
internet access. Inter-node traffic uses a self-referencing security-group rule. The DB uses a
self-signed certificate — clients pass `validateservercertificate=0`. Secrets live only in SSM
SecureString (KMS) and the gitignored `bench/.env`. tfstate password values are marked sensitive.

The cluster SSH private key is also SSM-only, shared across every `env_name` workspace (see § "0.
One-time setup" above for the create/rotate flow). The deployer principal already has the needed
`ssm:*`/KMS grants (`iam/deployer-policy.json`: `CoreInfra` + `KmsForSsmSecureString`). The fetch
writes the key straight into a `0600` file — it is never printed and never world- or
group-readable, even transiently.

## Enabling co-workers

Tofu state is local and gitignored (`.terraform.lock.hcl` is tracked, `*.tfstate` is not).
`cluster-stack` reads the data-stack state via a filesystem path (`providers.tf`). A teammate on
another machine has no state and cannot run `tofu output`, which `cluster-up.sh`/`secrets.sh`
need. Two paths, by what the teammate needs:

**A. Run benchmarks against an existing cluster (for example `test1`) — no tofu, no AWS
credentials.**

1. Allowlist their IP on the cluster security group (the default allowlist is only the deploying
   machine):
   ```bash
   cd deploy/cluster-stack
   tofu apply -var env_name=test1 -var key_pair_name=<project>-key \
     -var 'allowed_cidrs=["<your-ip>/32","<coworker-ip>/32"]'   # or ["0.0.0.0/0"] to open it
   ```
2. Send them the gitignored `bench/.env` over a secure channel. It is self-contained (host,
   ports, Glue `engine-reader` credentials, DB and BucketFS passwords). They need neither AWS
   credentials nor the EC2 key.
3. They run `sudo deploy/scripts/install-prereqs.sh` (Docker, `exapump`, make toolchain), then
   `make bench` from the repo root.

**B. Deploy their own cluster.**

1. Give them a deployer principal. Create a second IAM user with the same
   `iam/deployer-policy.json` (cleaner than sharing one key), then
   `export AWS_PROFILE=<project>-deployer`.
2. Import their EC2 public key and pass it as `key_pair_name`:
   `aws ec2 import-key-pair --key-name <project>-key-<name> --public-key-material fileb://~/.ssh/<key>.pub`.
3. Migrate both stacks to a shared S3 backend (see below) so state and the cross-stack
   `remote_state` read work off one machine. Without this, only the original deployer's machine
   can manage the stacks.
4. They apply only `cluster-stack` with their own `env_name` (own workspace, own state), then
   `cluster-up.sh <env>` → `secrets.sh <env>` → `make bench`. Never re-apply the shared
   `data-stack` — it holds the loaded data and catalog for everyone.

> **Shared S3 backend (team upgrade).** Add a `backend "s3"` block (bucket plus `dynamodb_table`
> for locking) to both `providers.tf` files, and repoint `cluster-stack`'s
> `terraform_remote_state.data` to `backend = "s3"`. Then any deployer with the deployer profile
> shares one authoritative state and can manage the same environments. Do this before more than
> one person deploys.

## Known seams

- **BucketFS write password** (`cluster-up.sh`): set best-effort via confd. If the confd verb
  differs on your Exasol build, set it once in the Admin UI (`https://<node1>:8443`) to match
  `/<project>/cluster/<env>/bucketfs_password` in SSM, then re-run `make bench`.
- **Engine-to-Glue auth uses static keys in the Exasol CONNECTION.** The engine reads credentials
  from JSON, not the instance role — that is the `engine-reader` user. Upgrade path: teach the
  adapter the default credential chain.
- **The Glue interface VPC endpoint is off** (paid). The free S3 gateway endpoint is on. Add the
  interface endpoint if Glue API latency matters.
- **EMR Serverless teardown needs a manual stop first.** `tofu apply -var
  enable_emr_serverless=false` (or `=true` to resize `maximumCapacity`) fails with
  `ValidationException: Application ... must be in [CREATED, STOPPED]` if the app auto-started for
  a job and has not hit its idle timeout yet. Run `aws emr-serverless stop-application
  --application-id <id>` (poll `get-application` for `STOPPED`) before re-applying. Found
  live-verifying: the app costs nothing while `STARTED`-but-idle, so this only blocks the
  Terraform operation, not billing.
- **Lakekeeper's storage credential uses static keys, not vended/STS credentials.** The
  warehouse's write-capable AWS access key ID and secret access key are a static pair
  (`deploy/lakekeeper-stack`), matching the same upgrade seam the Glue path's `engine-reader` key
  pair already has above. Upgrade path: teach the storage credential the default credential chain
  or Lakekeeper's vended-credentials support.
- **The Lakekeeper OAuth2 client secret is committed to this repository.**
  `scripts/keycloak-realm-iceberg.json`'s client secret ships as-is to the AWS box. The stack does
  not overwrite it, because the CONNECTION contract `e2e-harness/lakekeeper-e2e-harness` proves is
  defined by that exact file. Its only control is the security group's `/32` allowlist.
- **The catalog's storage credential grants object put/get/delete plus bucket list across the
  whole `data-stack` bucket**, not scoped to the warehouse's own key prefix — that prefix only
  exists after `lakekeeper-provision.sh` reads the source catalog at provisioning time, after the
  stack has already applied its IAM policy. Compensating controls, strongest to weakest:
  `lakekeeper-provision.sh` contains no destructive verb of any kind
  (`deploy/scripts/tests/lakekeeper.test.sh` scans its source text for one). The warehouse's
  `delete-profile` is the SOFT profile, a one-week delay window rather than a guarantee — a
  `force` drop or a purge-drop still removes files. The credential is created and destroyed with
  the ephemeral stack. The Exasol CONNECTION itself keeps the read-only `engine-reader` key pair,
  so the query path never holds write access.
- **Lakekeeper and Keycloak are reached over plain HTTP, from both vantages — provisioning
  traffic is cleartext.** A public-vantage `lakekeeper-up.sh` / `lakekeeper-provision.sh` run
  sends the OAuth2 client secret, the resulting bearer token, and the warehouse's write-capable S3
  access key ID and secret access key over the public internet in the clear, to the box's public
  IP. Every deployment carries at least one such run: `lakekeeper-up.sh` is an operator-machine
  script in both the benchmark and demo contexts, and it always provisions. The in-VPC vantage
  avoids this hop only for the optional re-provision case, never for the first, mandatory one. The
  security-group `/32` allowlist (`allowed_cidrs`, defaulting to the apply machine's own resolved
  public IP) is the practical control here, and it is a reachability control only. It bounds who
  can reach the plaintext port, but it neither encrypts the traffic nor stops an observer on the
  network path between an already-allowlisted client and the box.
