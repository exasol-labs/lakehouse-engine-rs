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

# An EC2 key pair for the cluster nodes (import your public key):
aws ec2 import-key-pair --key-name spot-strata-key \
  --public-key-material fileb://~/.ssh/id_ed25519.pub
# keep the matching private key at ~/.ssh/spot-strata-key.pem (or set KEY_FILE=...)
```

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
../scripts/secrets.sh myenv             # write bench/.env (host + Glue creds from outputs/SSM)
```

## 3. Run the perf test (unchanged)

```bash
cd ../..        # repo root
make bench       # builds .so, installs SLC + .so to BucketFS, runs Q1–Q4, writes bench/reports/
```

Athena benchmark (same catalog): `bench/athena_compare.sh` runs the Q1-Q4 set against the
`spot-strata-<env>-athena` workgroup automatically (see `bench/README.md`); no infra to stand up.

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

## Files

```
deploy/
  iam/{deployer-policy.json, SETUP.md}
  data-stack/{providers,variables,main,outputs}.tf  datagen-userdata.sh.tftpl
    # + EMR Serverless application (enable_emr_serverless, opt-in) for the Spark comparison
  cluster-stack/{providers,variables,main,outputs}.tf
  trino-stack/{providers,variables,main,outputs}.tf  trino-userdata.sh.tftpl
    # ephemeral Trino cluster (coordinator + workers) for the competitive comparison (opt-in)
  scripts/{install-prereqs.sh, gen_load.py, cluster-up.sh, cluster-down.sh, secrets.sh,
           trino-up.sh, trino-down.sh, spark_queries.py}
```
