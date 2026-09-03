# `deploy/` — programmatic AWS perf-test environment

Reproducible AWS environment for benchmarking the lakehouse engine. Two OpenTofu stacks:

- **`data-stack/`** — *persistent*. VPC + S3 gateway endpoint, S3 warehouse bucket, Glue Iceberg
  databases `tpch` + `perf`, an Athena workgroup, a scoped `engine-reader` IAM user, SSM secrets,
  and a *temporary* data-gen EC2 that generates TPC-H + wide perf tables and **self-terminates**.
- **`cluster-stack/`** — *ephemeral*, one per `$env_name` (OpenTofu workspaces). N Exasol EC2 nodes
  (default 2 active) with two EBS volumes each, an IP-allowlisted security group, and the cluster
  passwords in SSM.

The catalog is **AWS Glue's Iceberg REST endpoint** — queryable by both the lakehouse engine (SigV4
REST + S3) and **AWS Athena** (natively). `make bench` runs unchanged against it.

Everything is named **`spot-strata-<env_name>-*`** and tagged per the `exa:*` policy.

## 0. One-time setup

```bash
# Toolchain on this machine (OpenTofu, AWS CLI v2, jq, ...):
sudo deploy/scripts/install-prereqs.sh

# IAM deployer principal — follow the step-by-step:
deploy/iam/SETUP.md         # create user spot-strata-deployer, attach deployer-policy.json, make a key
export AWS_PROFILE=spot-strata-deployer
aws sts get-caller-identity # verify

# An EC2 key pair for the cluster nodes. Seed it ONCE (generates the key, imports the EC2 key pair,
# and stores the private key in SSM SecureString for the whole team):
deploy/scripts/rotate-cluster-key.sh          # default key name: spot-strata-key
```

This is the **single shared cluster SSH key** across all `env_name` workspaces (`test1` and any
future named clusters). Its private half lives only in SSM SecureString at
`/spot-strata/deploy/ssh_key/<key_pair_name>` — the same source of truth as the cluster passwords.
`cluster-up.sh` **auto-fetches it from SSM if it isn't already at `~/.ssh/<key_pair_name>` (or
`KEY_FILE=...`)** and copies it into `~/.ssh`, so a teammate with deployer credentials needs **no
manual key-sharing step** (and the later `secrets.sh` then finds it locally).
Re-run `rotate-cluster-key.sh` to rotate the key when it is lost, compromised, or on a schedule (see
"Rotating the shared key" below). If per-environment keys are ever wanted, that is a separate future
change — today it is deliberately one shared key.

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

Sizing knobs (defaults in `variables.tf`): `tpch_scale_factor=30` (≈5–6 GB lineitem),
`perf_table_sizes_gb=[10,20,30,40,80]`, `lineitem_files`, `perf_files`, `datagen_instance_type`,
`datagen_scratch_gb`. Same-region EC2→S3 means **no data-transfer cost**; the only standing cost is
S3 storage (~110 GiB for the full set).

> **Perf table sizes are approximate (row-count calibrated) and undershoot the nominal label** —
> `gen_load.py` estimates bytes/row from a small sample, but snappy compresses better at scale, so
> `t_80g` lands ≈46 GiB on disk (~57% of 80). Row-count ratios (10:20:30:40:80) are exact. Bump the
> targets or the calibration sample if you need precise on-disk sizes.

## 2. Test cluster (ephemeral — per benchmark)

```bash
cd deploy/cluster-stack
tofu init
tofu workspace new myenv                # or: tofu workspace select myenv
tofu apply -var env_name=myenv -var key_pair_name=spot-strata-key
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
`spot-strata-<env>-athena` workgroup automatically (see `bench/README.md`); no infra to stand up.

> **Quick path (recommended):** `deploy/scripts/bench-remote.sh <env>` chains the `cluster-stack`
> `tofu apply` and steps 2-3-5 (`cluster-up.sh` → `secrets.sh` → `make bench` → `cluster-down.sh`)
> into one command that
> **always** tears the cluster down — on success, failure, or interrupt — because it installs its
> teardown trap *before* bringing anything up. Any `BENCH_*`/`LAKEHOUSE_*` env you export (e.g.
> `BENCH_WITH_DELETES=1`) flows through untouched to `make bench`:
> ```bash
> AWS_PROFILE=spot-strata-deployer deploy/scripts/bench-remote.sh test1
> AWS_PROFILE=spot-strata-deployer BENCH_WITH_DELETES=1 deploy/scripts/bench-remote.sh test1
> ```
> Same cost-safety framing as the Trino teardown warning below: a live `r8i.2xlarge` × N cluster
> bills continuously, so guaranteed teardown is the whole point — the script's final line states
> whether teardown ran, but **verify actual termination yourself** via `aws ec2 describe-instances`
> before considering the run done. The manual step 2/3/5 sequence above still works and is what
> the wrapper does under the hood — use it directly for fine-grained control (e.g. leaving the
> cluster up between repeated `make bench` runs).

### Delete-bearing benchmark prerequisite (remote, one-time per environment)

`BENCH_WITH_DELETES=1` (see `bench/README.md`) runs the perf test against Iceberg v2
merge-on-read, 5%-position-deleted copies of the TPC-H tables; the default
(`BENCH_WITH_DELETES=0`) is byte-for-byte identical to the benchmark's existing behavior. In
remote mode the delete-bearing tables must be
pre-authored once per environment — `run.sh` never authors them itself (unlike docker mode) —
via a one-time EMR Serverless job, same shape as `spark_compare.sh` below:

```bash
cd deploy/data-stack   # requires enable_emr_serverless=true (see section 4)
export EMR_SERVERLESS_APP_ID=$(tofu output -raw emr_serverless_app_id)
export EMR_SERVERLESS_ROLE_ARN=$(tofu output -raw emr_serverless_job_role_arn)
export SPARK_DELETES_SCRIPT_S3_URI=$(tofu output -raw spark_deletes_script_s3_uri)
export SPARK_LOG_S3_URI=$(tofu output -raw emr_serverless_log_uri)
cd ../.. && deploy/scripts/make-deletes-remote.sh
```

This submits `deploy/scripts/make_deletes_remote.py`, which authors the `tpch_deletes` Glue
database (8 MOR tables, deterministic ~5% position deletes) from the existing `tpch` Glue tables —
idempotent, safe to re-run. Skip this and `run.sh BENCH_WITH_DELETES=1` hard-errors pointing back
at this script.

## 4. Competitive comparison (Athena / Trino / Spark, opt-in)

Runs the same TPC-H tables/queries through the engines people put next to a lakehouse. See
`bench/README.md`'s "Competitive engine comparison" section for the compare scripts themselves —
this section covers standing up the Trino/Spark compute they need.

### Trino (ephemeral, opt-in)

A new OpenTofu stack, `deploy/trino-stack/`, mirroring `cluster-stack/`: a real coordinator +
worker cluster running Trino in Docker, sized by `instance_type`/`node_count`
(default `r8i.2xlarge` × 2 — matching an Exasol `test1` node's type and the cluster's node
count, so Trino and lakehouse-engine-rs run on identical hardware). Its Iceberg connector uses
`iceberg.catalog.type=glue` (talks to the Glue Data Catalog directly via the AWS SDK, not the
REST endpoint the lakehouse engine uses) against the same S3 bucket, authenticated via an
instance-profile role (no static keys). The coordinator also runs worker tasks
(`node-scheduler.include-coordinator=true`), mirroring Exasol's every-node-executes model.

```bash
cd deploy/trino-stack && tofu init
../scripts/trino-up.sh myenv    # tofu apply + wait for the coordinator + all workers to join
export TRINO_HOST=<printed coordinator ip>
cd ../.. && bench/trino_compare.sh
../scripts/trino-down.sh myenv  # deploy/scripts/, from repo root: deploy/scripts/trino-down.sh myenv
```

> **Cost / teardown: these EC2 nodes bill while they exist — and `r8i.2xlarge` × 2 costs
> meaningfully more than a single small box.** They are created ONLY by an explicit `trino-up.sh`
> run — nothing else applies this stack — and MUST be torn down immediately after the benchmark
> run with `trino-down.sh <env>`. There is no auto-stop. Verify via `aws ec2 describe-instances`
> that both nodes actually terminated before considering a run done.

### Spark / EMR Serverless (pay-per-job, opt-in)

Rather than a persistent/ephemeral Spark cluster, Spark runs via **AWS EMR Serverless**: an
application resource that costs nothing at rest and auto-stops after an idle job (no explicit
teardown to forget). Added to `deploy/data-stack` behind a toggle, off by default:

```bash
cd deploy/data-stack
tofu apply -var enable_emr_serverless=true    # creates the (idle, $0) EMR Serverless application

export EMR_SERVERLESS_APP_ID=$(tofu output -raw emr_serverless_app_id)
export EMR_SERVERLESS_ROLE_ARN=$(tofu output -raw emr_serverless_job_role_arn)
export SPARK_SCRIPT_S3_URI=$(tofu output -raw spark_script_s3_uri)
export SPARK_LOG_S3_URI=$(tofu output -raw emr_serverless_log_uri)
cd ../.. && bench/spark_compare.sh
```

> **Cost / teardown: the application itself is free while idle** (billed only for vCPU/memory
> while a job runs; `auto_stop_configuration` stops it after 15 idle minutes even if you forget).
> To remove it entirely: `tofu apply -var enable_emr_serverless=false` in `data-stack`.

> **One-time prerequisite:** the `spot-strata-deployer` policy needs an added `emr-serverless:*`
> statement (already in `deploy/iam/deployer-policy.json` as of this PR) — bump the live policy
> version once per account: see "Updating the policy" in `deploy/iam/SETUP.md`.

## Lakekeeper (ephemeral, opt-in)

A second, opt-in Iceberg REST catalog for the `remote` bench target and for live demos, selected by
`BENCH_CATALOG=lakekeeper` (default `glue`). `deploy/lakekeeper-stack/` is a separate OpenTofu stack,
layered on the persistent `data-stack`, that stands up one EC2 box running PostgreSQL + Keycloak +
Lakekeeper migrate + Lakekeeper — the same four-service set as `docker-compose.lakekeeper.yml`.
`deploy/scripts/lakekeeper-provision.sh` then registers every already-cataloged TPC-H Iceberg table into it BY
REFERENCE — no data rewrite, no second physical copy of the data. Glue is unaffected: with
`BENCH_CATALOG` unset, every existing required variable, catalog URI, CONNECTION password, virtual-schema property, query set, and row count stays exactly as it is today.

`secrets.sh` and `make bench` additionally require an applied `cluster-stack` plus a completed
`cluster-up.sh <env>` (§ "2. Test cluster" above) — see the Demo runbook below for the full
ordered sequence.

```bash
cd deploy/lakekeeper-stack && tofu init && cd ../..   # one-time
AWS_PROFILE=spot-strata-deployer deploy/scripts/lakekeeper-up.sh myenv   # apply + provision
deploy/scripts/secrets.sh myenv                                         # adds the Lakekeeper block to bench/.env
BENCH_CATALOG=lakekeeper make bench
deploy/scripts/lakekeeper-down.sh myenv                                 # destroy it — see cost note below
```

> **Cost / teardown: this EC2 box bills while it exists.** It is created ONLY by an explicit
> `lakekeeper-up.sh` run — nothing else applies this stack — and MUST be torn down immediately after
> the benchmark run or demo with `lakekeeper-down.sh <env>`. There is no auto-stop. Verify via
> `aws ec2 describe-instances` that the instance actually terminated before considering a run done.

### `bench-remote.sh`'s teardown trap cuts both ways

`deploy/scripts/bench-remote.sh` installs `trap teardown EXIT`, and that trap's relationship to the
Lakekeeper stack runs in two directions — both matter, and neither is the other:

- **It does NOT cover the Lakekeeper stack.** The trap only knows about the Exasol `cluster-stack` —
  the same relationship `deploy/trino-stack` already has to this wrapper. `lakekeeper-down.sh <env>`
  is always a separate, mandatory step, regardless of which sequence below stood the demo up.
- **It DOES cover the Exasol cluster, and this is the direction that ends a demo by surprise.**
  Unless `KEEP_ALIVE=1` was exported, the trap runs `cluster-down.sh <env>` on **every** exit path —
  success, failure, or interrupt — which destroys the Exasol cluster carrying the CONNECTION and the
  virtual schema a Lakekeeper run just built. **A default `bench-remote.sh <env>` run therefore ENDS
  the demo it just set up.** The runbook below gives the two sequences that survive it.

### Demo runbook

See `DEMO.md` for the presenter-facing companion to this operational reference — a standalone,
copy-pasteable script for running the demo live.

A live customer demo and the automated benchmark share one stack, one provisioning path, one
warehouse, one namespace, and one virtual schema — the difference is entirely in who issues the
commands afterwards, not in what the tooling does. Two sequences survive the trap above; the bare
wrapper (`bench-remote.sh <env>` with no `KEEP_ALIVE`) is neither of them and ends the demo.

Both sequences below MUST carry `BENCH_CATALOG=lakekeeper` **explicitly**: `bench/run.sh` defaults to
`glue`, and the wrapper passes every caller-exported `BENCH_*`/`LAKEHOUSE_*` variable through
untouched, so omitting it silently demonstrates Glue at a live customer session. Both sequences MUST
also run `lakekeeper-up.sh <env>` **before** `secrets.sh <env>` runs — including the `secrets.sh` call
the wrapper makes internally at its step `[3/4]` — because `secrets.sh` emits the Lakekeeper block
into `bench/.env` only while a Lakekeeper stack workspace exists for that environment.

**Wrapper form** (default path). `bench-remote.sh` performs the `cluster-stack` `tofu apply`,
`cluster-up.sh`, `secrets.sh`, and `make bench` itself, at its steps `[1/4]`–`[4/4]`:

```bash
AWS_PROFILE=spot-strata-deployer deploy/scripts/lakekeeper-up.sh myenv
AWS_PROFILE=spot-strata-deployer BENCH_CATALOG=lakekeeper KEEP_ALIVE=1 deploy/scripts/bench-remote.sh myenv
```

**Unwrapped form** (fine-grained control). `cluster-up.sh` runs c4 against nodes an apply already
created, so the sequence cannot start there — it starts with the `cluster-stack` apply from § "2.
Test cluster" above:

```bash
cd deploy/cluster-stack
tofu workspace new myenv   # or: tofu workspace select myenv
tofu apply -var env_name=myenv -var key_pair_name=spot-strata-key
cd ../..

AWS_PROFILE=spot-strata-deployer deploy/scripts/lakekeeper-up.sh myenv
AWS_PROFILE=spot-strata-deployer deploy/scripts/cluster-up.sh myenv
AWS_PROFILE=spot-strata-deployer deploy/scripts/secrets.sh myenv
BENCH_CATALOG=lakekeeper make bench
```

Either sequence leaves the CONNECTION and the `TPCH` virtual schema in place and queryable
afterwards: `bench/run.sh` drops and recreates the virtual schema as one pair at the START of a run
and never at the end, so the schema a benchmark run leaves behind IS the demo's query surface.
Continue straight into the interactive tail from a SQL client connected to `EXASOL_HOST`/
`LH_EXASOL_PORT` (from the just-written `bench/.env`):

```sql
SELECT COUNT(*) FROM TPCH.LINEITEM;
-- ... any ad-hoc TPC-H query against TPCH.* ...
```

`./bench/run.sh selftest`'s `vs_teardown_is_recreate_only` check guards this invariant, but only as a
source-text check over `bench/run.sh` itself — it cannot see `bench-remote.sh`'s EXIT trap, so a
green result is evidence the harness's OWN teardown timing is correct, and MUST NOT be read as
evidence that an operator-level run left the demo surface behind. Close every demo with both teardown
commands, regardless of which form stood it up:

```bash
AWS_PROFILE=spot-strata-deployer deploy/scripts/cluster-down.sh myenv
AWS_PROFILE=spot-strata-deployer deploy/scripts/lakekeeper-down.sh myenv
```

## 5. Tear down

```bash
cd deploy/cluster-stack && ../scripts/cluster-down.sh myenv   # destroy cluster, keep data
# eventually:
cd ../data-stack && tofu destroy                              # remove data + catalog
```

## Security model

All cluster ingress (SSH/c4 ports 22/20002/20003, client ports 8563/8443/2581) is restricted to
`allowed_cidrs`, which **defaults to this machine's public IP**. Internet access is opt-in: add
CIDRs (or `0.0.0.0/0`) to the variable. Inter-node traffic is via a self-referencing SG rule. The DB
uses a self-signed cert — clients pass `validateservercertificate=0`. Secrets live only in SSM
SecureString (KMS) and the gitignored `bench/.env`; tfstate password values are marked sensitive.

The **cluster SSH private key** is also SSM-only: one shared key for all `env_name` workspaces,
stored (SecureString) at `/spot-strata/deploy/ssh_key/<key_pair_name>` and auto-fetched by
`cluster-up.sh` when it is not already local. Nothing needs `ssm:*`/KMS beyond what the
deployer principal already has (`iam/deployer-policy.json`: `CoreInfra` + `KmsForSsmSecureString`),
so this reuses the existing grants. The fetch writes the key straight into a `0600` file — it is
never printed and never world/group-readable, even transiently.

### Rotating the shared key

Run the helper whenever the key is **lost, compromised, or due for scheduled rotation**:

```bash
AWS_PROFILE=spot-strata-deployer deploy/scripts/rotate-cluster-key.sh   # default key: spot-strata-key
```

It generates a fresh ed25519 key, re-imports the EC2 key pair under the same name, and overwrites the
SSM SecureString parameter — the private key only ever touches a `0700` tempdir that is shredded on
exit. Rotation applies to nodes created **after** it runs (existing nodes keep the old
`authorized_keys`), so re-provision affected clusters (`tofu apply` + `cluster-up.sh <env>`) to move
them onto the new key. This was the fix after the original `test1` key was lost with no shared copy:
storing the key in SSM means a lost local copy is no longer a lockout.

## Enabling co-workers

Tofu state is **local and gitignored** (`.terraform.lock.hcl` is tracked; `*.tfstate` is not), and
`cluster-stack` reads the data-stack state via a **filesystem path** (`providers.tf`). So a teammate
on another machine has no state and cannot `tofu output` — which `cluster-up.sh`/`secrets.sh` need.
Two paths, by what the teammate actually needs:

**A. Just *run* benchmarks against an existing cluster (e.g. `test1`) — no tofu, no AWS creds.**

1. Allowlist their IP on the cluster SG (default allowlist is only the deploying machine):
   ```bash
   cd deploy/cluster-stack
   tofu apply -var env_name=test1 -var key_pair_name=spot-strata-key \
     -var 'allowed_cidrs=["<your-ip>/32","<coworker-ip>/32"]'   # or ["0.0.0.0/0"] to open it
   ```
2. Send them the gitignored `bench/.env` over a secure channel — it is self-contained (host, ports,
   Glue `engine-reader` creds, DB + BucketFS passwords). They need neither AWS creds nor the EC2 key.
3. They `sudo deploy/scripts/install-prereqs.sh` (Docker + `exapump` + make toolchain) and run
   `make bench` from the repo root.

**B. *Deploy* their own cluster.**

1. Give them a deployer principal — create a second IAM user with the same `iam/deployer-policy.json`
   (cleaner than sharing one key), then `export AWS_PROFILE=spot-strata-deployer`.
2. Import their EC2 public key and pass it as `key_pair_name`:
   `aws ec2 import-key-pair --key-name spot-strata-key-<name> --public-key-material fileb://~/.ssh/<key>.pub`.
3. **Migrate both stacks to a shared S3 backend** (see below) — the real fix so state and the
   cross-stack `remote_state` read work off one machine. Without it, only the original deployer's
   machine can manage the stacks.
4. They apply **only** `cluster-stack` with their own `env_name` (own workspace = own state), then
   `cluster-up.sh <env>` → `secrets.sh <env>` → `make bench`. Never re-apply the shared `data-stack`
   (it holds the loaded data + catalog for everyone).

> **Shared S3 backend (team upgrade).** Add a `backend "s3"` block (bucket + `dynamodb_table` for
> locking) to both `providers.tf` files and repoint `cluster-stack`'s `terraform_remote_state.data`
> to `backend = "s3"`. Then any deployer with the deployer profile shares one authoritative state and
> can manage the same environments. Do this before more than one person deploys.

## Known seams

- **BucketFS write password** (`cluster-up.sh`): set best-effort via confd. If the confd verb differs
  on your Exasol build, set it once in the Admin UI (`https://<node1>:8443`) to match
  `/spot-strata/cluster/<env>/bucketfs_password` in SSM, then re-run `make bench`.
- **Engine→Glue auth** uses static keys in the Exasol CONNECTION (the engine reads creds from JSON,
  not the instance role) — that's the `engine-reader` user. Upgrade path: teach the adapter the
  default credential chain.
- **Glue interface VPC endpoint** is off (paid); the free S3 gateway endpoint is on. Add it if Glue
  API latency matters.
- **EMR Serverless teardown needs a manual stop first** — `tofu apply -var
  enable_emr_serverless=false` (or `=true` to resize `maximumCapacity`) fails with
  `ValidationException: Application ... must be in [CREATED, STOPPED]` if the app auto-started for
  a job and hasn't hit its idle timeout yet. Run `aws emr-serverless stop-application
  --application-id <id>` (poll `get-application` for `STOPPED`) before re-applying. Found
  live-verifying — the app costs nothing while `STARTED`-but-idle, so this only blocks the
  Terraform operation, not billing.
- **Lakekeeper's storage credential uses static keys, not vended/STS credentials.** The warehouse's
  write-capable AWS access key id and secret access key are a static pair (`deploy/lakekeeper-stack`),
  matching the same upgrade seam the Glue path's `engine-reader` key pair already has above. Upgrade
  path: teach the storage credential the default credential chain or Lakekeeper's vended-credentials
  support.
- **The Lakekeeper OAuth2 client secret is committed to this repository.** `scripts/keycloak-realm-iceberg.json`'s client secret ships as-is to the AWS box; the stack does not overwrite it, because
  the CONNECTION contract `e2e-harness/lakekeeper-e2e-harness` already proves is defined by that exact
  file. Its only control is the security group's `/32` allowlist.
- **The catalog's storage credential grants object put/get/delete plus bucket list across the WHOLE
  `data-stack` bucket**, not scoped to the warehouse's own key prefix — that prefix only exists after
  `lakekeeper-provision.sh` reads the source catalog at provisioning time, after the stack has already
  applied its IAM policy. Compensating controls, from strongest to weakest: `lakekeeper-provision.sh`
  contains no destructive verb of any kind (`deploy/scripts/tests/lakekeeper.test.sh` scans its source
  text for one); the warehouse's `delete-profile` is the SOFT profile, a one-week delay window rather
  than a guarantee — a `force` drop or a purge-drop still removes files; the credential is created and
  destroyed with the ephemeral stack; and the Exasol CONNECTION itself keeps the read-only
  `engine-reader` key pair, so the query path never holds write access.
- **Lakekeeper and Keycloak are reached over plain HTTP, from both vantages — provisioning traffic is
  cleartext.** A public-vantage `lakekeeper-up.sh` / `lakekeeper-provision.sh` run sends the OAuth2
  client secret, the resulting bearer token, and the warehouse's write-capable S3 access key id and
  secret access key over the public internet in the clear, to the box's public IP. EVERY deployment
  carries at least one such run: `lakekeeper-up.sh` is an operator-machine script in both the
  benchmark and demo contexts and always provisions, so the in-VPC vantage avoids this hop only for
  the optional re-provision case, never for the first, mandatory one. The security-group `/32`
  allowlist (`allowed_cidrs`, defaulting to the apply machine's own resolved public IP) is the
  practical control here, and it is a REACHABILITY control only — it bounds WHO CAN REACH the
  plaintext port, but it neither encrypts the traffic nor stops an observer on the network path
  between an already-allowlisted client and the box.

## Files

```
deploy/
  DEMO.md   # presenter-facing live-demo script (task 6.3)
  iam/{deployer-policy.json, SETUP.md}
  data-stack/{providers,variables,main,outputs}.tf  datagen-userdata.sh.tftpl
    # + EMR Serverless application (enable_emr_serverless, opt-in) for the Spark comparison
  cluster-stack/{providers,variables,main,outputs}.tf
  trino-stack/{providers,variables,main,outputs}.tf  trino-userdata.sh.tftpl
    # ephemeral Trino cluster (coordinator + workers) for the competitive comparison (opt-in)
  lakekeeper-stack/{providers,variables,locals,main,outputs}.tf  lakekeeper-userdata.sh.tftpl
    # ephemeral Lakekeeper catalog (postgres + keycloak + lakekeeper) for BENCH_CATALOG=lakekeeper (opt-in)
  scripts/{install-prereqs.sh, gen_load.py, cluster-up.sh, cluster-down.sh, secrets.sh,
           trino-up.sh, trino-down.sh, spark_queries.py,
           lakekeeper-up.sh, lakekeeper-down.sh, lakekeeper-provision.sh,
             # apply+register / destroy the Lakekeeper stack; the provisioning script also runs
             # standalone, unchanged, from an operator's laptop or from an in-VPC EC2 box
           bench-remote.sh,             # cluster-up -> secrets -> make bench -> cluster-down, one command
           make_deletes_remote.py, make-deletes-remote.sh,  # one-time remote delete-prep (BENCH_WITH_DELETES)
           rotate-cluster-key.sh,       # seed/rotate the shared cluster SSH key in SSM (issue #89)
           tests/lakekeeper.test.sh, tests/lakekeeper-local.test.sh}
             # offline stubbed-PATH harness + local Docker integration verification (make
             # test-lakekeeper-scripts / test-lakekeeper-local)
```

Related, outside `deploy/` (documented in `bench/README.md`'s "Delete-bearing benchmark" section):
`scripts/spark-fixtures/create_tpch_deletes.sql` (the delete-authoring SQL, shared by docker + remote)
and `bench/make_deletes_docker.sh` (the docker-mode caller).
