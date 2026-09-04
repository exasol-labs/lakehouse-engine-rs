# IAM deployer setup

OpenTofu provisions everything as a single **deployer** IAM principal. You (account admin) create
that principal once, attach `deployer-policy.json`, and hand the access key to your AWS CLI. All
later `tofu`/script runs use it. OpenTofu itself then creates the scoped `<project>-data-engine-reader`
user that the Exasol CONNECTION uses — you do **not** create that one.

The policy is **service-broad but account-scoped**: full EC2/S3/Glue/Athena/SSM (this is a sandbox
perf account), KMS limited to SSM SecureString, and IAM limited to `<project>-*` resources so the
deployer cannot escalate beyond this project's own users/roles. Tighten further later if needed.

## Option A — AWS Console

1. **IAM → Users → Create user**
   - User name: `<project>-deployer`
   - Do **not** enable console access (programmatic only).
2. **Permissions → Attach policies directly → Create policy → JSON tab**
   - Paste the contents of `deploy/iam/deployer-policy.json`.
   - Name it `<project>-deployer-policy`, create, then attach it to the user.
3. **User → Security credentials → Create access key**
   - Use case: *Command Line Interface (CLI)*.
   - Copy the **Access key ID** and **Secret access key** (the secret is shown once).
4. (Recommended) Tag the user per policy: `exa:Project=SPOT`, `exa:Owner=marco.naetlitz`,
   `exa:Department=ENG`, `exa:CostCenter=70010`, `exa:Environment=development`.

## Option B — AWS CLI (run as an existing admin profile)

```bash
cd deploy/iam

# 1. create the managed policy
POLICY_ARN=$(aws iam create-policy \
  --policy-name <project>-deployer-policy \
  --policy-document file://deployer-policy.json \
  --query 'Policy.Arn' --output text)

# 2. create the user + attach
aws iam create-user --user-name <project>-deployer \
  --tags Key=exa:Project,Value=SPOT Key=exa:Owner,Value=marco.naetlitz \
         Key=exa:Department,Value=ENG Key=exa:CostCenter,Value=70010 \
         Key=exa:Environment,Value=development
aws iam attach-user-policy --user-name <project>-deployer --policy-arn "$POLICY_ARN"

# 3. create an access key (capture the output — secret is shown once)
aws iam create-access-key --user-name <project>-deployer
```

## Configure the CLI with the deployer key

Pick a profile name and the region where you will deploy the stacks:

```bash
aws configure --profile <project>-deployer   # enter the access key, secret key, and region when prompted

export AWS_PROFILE=<project>-deployer
aws sts get-caller-identity      # must return the <project>-deployer ARN
```

Keep `AWS_PROFILE=<project>-deployer` exported (or pass `--profile`) for all `tofu` and script
runs. The account ID and region from `get-caller-identity` become the stack's `aws_account_id`
and `region` variables.

## Updating the policy (for example, for EMR Serverless / Spark benchmarking)

This policy is bootstrapped once and lives outside Terraform's management. A later change to
`deployer-policy.json` (for example the `EmrServerlessForSparkBenchmark` statement, added for
`bench/spark_compare.sh`) needs a one-time manual bump. The deployer already has
`iam:CreatePolicyVersion` on its own policy, so it can self-apply:

```bash
POLICY_ARN=$(aws iam list-policies --query \
  "Policies[?PolicyName=='<project>-deployer-policy'].Arn" --output text)
aws iam create-policy-version --policy-arn "$POLICY_ARN" \
  --policy-document file://deploy/iam/deployer-policy.json --set-as-default
```
