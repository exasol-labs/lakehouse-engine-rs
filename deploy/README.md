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
S3 storage (~180 GB).

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

Athena benchmark (same catalog): run queries against the `tpch` / `perf` databases in the
`spot-strata-data-athena` workgroup, e.g. `SELECT count(*) FROM tpch.lineitem`.

## 4. Tear down

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

## Known seams

- **BucketFS write password** (`cluster-up.sh`): set best-effort via confd. If the confd verb differs
  on your Exasol build, set it once in the Admin UI (`https://<node1>:8443`) to match
  `/spot-strata/cluster/<env>/bucketfs_password` in SSM, then re-run `make bench`.
- **Engine→Glue auth** uses static keys in the Exasol CONNECTION (the engine reads creds from JSON,
  not the instance role) — that's the `engine-reader` user. Upgrade path: teach the adapter the
  default credential chain.
- **Glue interface VPC endpoint** is off (paid); the free S3 gateway endpoint is on. Add it if Glue
  API latency matters.

## Files

```
deploy/
  iam/{deployer-policy.json, SETUP.md}
  data-stack/{providers,variables,main,outputs}.tf  datagen-userdata.sh.tftpl
  cluster-stack/{providers,variables,main,outputs}.tf
  scripts/{install-prereqs.sh, gen_load.py, cluster-up.sh, cluster-down.sh, secrets.sh}
```
