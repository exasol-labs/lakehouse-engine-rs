# Ephemeral Lakekeeper catalog (Postgres + Keycloak + Lakekeeper serve, single EC2 box) layered on
# the persistent data-stack, benchmarked/demoed against the SAME Glue-cataloged S3 TPC-H data via
# register-table (no data rewrite). Cost-safety: applied/destroyed explicitly per run
# (deploy/scripts/lakekeeper-up.sh / lakekeeper-down.sh) — never touched by data-stack,
# cluster-stack, or trino-stack applies, so nothing here runs unless someone asks for it.
data "aws_caller_identity" "current" {}

locals {
  prefix     = "spot-strata-${var.env_name}-lakekeeper"
  vpc_id     = data.terraform_remote_state.data.outputs.vpc_id
  subnet_id  = data.terraform_remote_state.data.outputs.subnet_id
  bucket     = data.terraform_remote_state.data.outputs.bucket
  bucket_arn = "arn:aws:s3:::${local.bucket}"
  account_id = data.aws_caller_identity.current.account_id
  ssm_root   = "/spot-strata/lakekeeper/${var.env_name}"

  # Single-node appliance (Postgres + Keycloak + Lakekeeper) — not the box under performance
  # measurement, so it does not need to match an Exasol/Trino node's shape like those stacks do.
  instance_type = "t3.large"

  # Pinned container images (Dependencies section) — matches docker-compose.lakekeeper.yml's
  # defaults exactly, so the AWS box runs the same versions the local stack is verified against.
  postgres_image   = "postgres:17"
  keycloak_image   = "quay.io/keycloak/keycloak:26.0.7"
  lakekeeper_image = "quay.io/lakekeeper/catalog:v0.13.1"

  warehouse_name = "${local.prefix}-warehouse"

  # Ingress allowlist: explicit var, else this machine's public IP /32 (resolved at apply).
  my_ip_cidr      = "${chomp(data.http.my_ip.response_body)}/32"
  effective_cidrs = length(var.allowed_cidrs) > 0 ? var.allowed_cidrs : [local.my_ip_cidr]

  # Two URI vantages (decision [7]): a same-VPC client (the Exasol UDF) uses the PRIVATE IP; the
  # operator's laptop (lakekeeper-up.sh, lakekeeper-provision.sh) uses the PUBLIC IP. Keycloak
  # stamps `iss` from the request host, so both issuers must be accepted (task 1.5).
  catalog_uri_public  = "http://${aws_instance.lakekeeper.public_ip}:8181/catalog"
  catalog_uri_private = "http://${aws_instance.lakekeeper.private_ip}:8181/catalog"
  token_uri_public    = "http://${aws_instance.lakekeeper.public_ip}:8080/realms/${local.oidc_realm}/protocol/openid-connect/token"
  token_uri_private   = "http://${aws_instance.lakekeeper.private_ip}:8080/realms/${local.oidc_realm}/protocol/openid-connect/token"
}

data "http" "my_ip" {
  url = "https://checkip.amazonaws.com"
}

data "aws_ami" "ubuntu" {
  most_recent = true
  owners      = ["099720109477"] # Canonical
  filter {
    name   = "name"
    values = ["ubuntu/images/hvm-ssd*/ubuntu-noble-24.04-amd64-server-*"]
  }
}

data "aws_vpc" "this" {
  id = local.vpc_id
}

# --- Security group ---------------------------------------------------------
resource "aws_security_group" "lakekeeper" {
  name        = "${local.prefix}-sg"
  description = "Lakekeeper catalog ${var.env_name}"
  vpc_id      = local.vpc_id
  tags        = { Name = "${local.prefix}-sg" }
}

resource "aws_security_group_rule" "ssh" {
  type              = "ingress"
  from_port         = 22
  to_port           = 22
  protocol          = "tcp"
  cidr_blocks       = local.effective_cidrs
  security_group_id = aws_security_group.lakekeeper.id
  description       = "SSH from allowlist"
}

# 8181 (Lakekeeper) and 8080 (Keycloak) additionally need the whole VPC CIDR, not just the
# operator's IP: the Exasol UDF's OAuth2 token request and Iceberg REST scan calls execute FROM
# the cluster nodes (same VPC/subnet as this stack), not from the operator's machine.
resource "aws_security_group_rule" "catalog_ports" {
  for_each          = toset(["8181", "8080"])
  type              = "ingress"
  from_port         = tonumber(each.value)
  to_port           = tonumber(each.value)
  protocol          = "tcp"
  cidr_blocks       = concat(local.effective_cidrs, [data.aws_vpc.this.cidr_block])
  security_group_id = aws_security_group.lakekeeper.id
  description       = "port ${each.value} from allowlist + VPC (Exasol UDF connect)"
}

resource "aws_security_group_rule" "egress" {
  type              = "egress"
  from_port         = 0
  to_port           = 0
  protocol          = "-1"
  cidr_blocks       = ["0.0.0.0/0"]
  security_group_id = aws_security_group.lakekeeper.id
  description       = "all egress"
}

# --- Keycloak realm export, delivered via S3 (EC2 user-data is capped at 16 KB; the export is
# 21 KB). The key sits under a dedicated top-level `lakekeeper/` prefix, never under the `tpch.db/`
# data prefix data-stack/main.tf:71-74 sets as the Glue database's location_uri, so this object
# cannot land inside the warehouse prefix the provisioning script later derives from the table
# locations and Lakekeeper's creation probe asserts on (decision [8]).
resource "aws_s3_object" "keycloak_realm" {
  provider = aws.no_default_tags
  bucket   = local.bucket
  key      = "lakekeeper/keycloak-realm-iceberg.json"
  source   = "${path.module}/../../scripts/keycloak-realm-iceberg.json"
  etag     = filemd5("${path.module}/../../scripts/keycloak-realm-iceberg.json")
}

# --- IAM: instance role for the box's own boot script -----------------------
resource "aws_iam_role" "lakekeeper" {
  name = "${local.prefix}-role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "ec2.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
  tags = { Name = "${local.prefix}-role" }
}

resource "aws_iam_role_policy" "lakekeeper_boot" {
  name = "${local.prefix}-boot"
  role = aws_iam_role.lakekeeper.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid      = "RealmExportRead"
        Effect   = "Allow"
        Action   = ["s3:GetObject"]
        Resource = ["${local.bucket_arn}/${aws_s3_object.keycloak_realm.key}"]
      },
      {
        # Task 1.5's compose file substitutes the Keycloak bootstrap admin password and the
        # Lakekeeper metadata-encryption key this stack generates below, in place of the local
        # stack's insecure literals — read back from this stack's own SSM root at boot.
        Sid      = "OwnSecretsRead"
        Effect   = "Allow"
        Action   = ["ssm:GetParameter", "ssm:GetParameters"]
        Resource = "arn:aws:ssm:${var.region}:${local.account_id}:parameter${local.ssm_root}/*"
      },
      {
        Sid      = "OwnSecretsDecrypt"
        Effect   = "Allow"
        Action   = ["kms:Decrypt"]
        Resource = "*"
      }
    ]
  })
}

resource "aws_iam_instance_profile" "lakekeeper" {
  name = "${local.prefix}-profile"
  role = aws_iam_role.lakekeeper.name
}

# --- IAM: dedicated write-capable storage user for the Lakekeeper warehouse -
# Separate from the data-stack `engine-reader` user, whose policy grants read-only S3 access
# (decision [6]). Lakekeeper validates a warehouse's storage access at creation by writing,
# reading back, and deleting a probe object, so it needs a credential with put/delete/list, which
# the read-only query-path credential cannot provide.
resource "aws_iam_user" "lakekeeper_storage" {
  name = "${local.prefix}-storage"
  tags = { Name = "${local.prefix}-storage" }
}

resource "aws_iam_policy" "lakekeeper_storage" {
  name = "${local.prefix}-storage-policy"
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        # Bucket-wide write+delete is a NAMED, ACCEPTED RISK (decision [6] / spec "The catalog's
        # storage credential is separate..."): the warehouse key prefix is derived by the
        # provisioning script from the source tables AFTER this stack is applied, so this
        # apply-time policy cannot name the prefix it will cover.
        Sid      = "WarehouseStorageReadWrite"
        Effect   = "Allow"
        Action   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket", "s3:GetBucketLocation"]
        Resource = [local.bucket_arn, "${local.bucket_arn}/*"]
      }
    ]
  })
}

# aws_iam_user_policy (inline) MUST NOT be used: deploy/iam/deployer-policy.json grants no
# iam:PutUserPolicy, so an inline policy would fail with AccessDenied at apply time after the EC2
# instance is already billing.
resource "aws_iam_user_policy_attachment" "lakekeeper_storage" {
  user       = aws_iam_user.lakekeeper_storage.name
  policy_arn = aws_iam_policy.lakekeeper_storage.arn
}

resource "aws_iam_access_key" "lakekeeper_storage" {
  user = aws_iam_user.lakekeeper_storage.name
}

# --- Generated passwords -> SSM SecureString --------------------------------
# MUST NOT be the local compose file's literal `admin` / `This-is-NOT-Secure!`
# (docker-compose.lakekeeper.yml:44-45,96,111) — this box carries a public IP.
resource "random_password" "db" {
  length  = 20
  special = false
}
resource "random_password" "metadata_encryption_key" {
  length  = 32
  special = false
}
resource "random_password" "keycloak_admin" {
  length  = 20
  special = false
}

# --- EC2 instance ------------------------------------------------------------
resource "aws_instance" "lakekeeper" {
  ami                         = data.aws_ami.ubuntu.id
  instance_type               = local.instance_type
  subnet_id                   = local.subnet_id
  vpc_security_group_ids      = [aws_security_group.lakekeeper.id]
  key_name                    = var.key_pair_name
  iam_instance_profile        = aws_iam_instance_profile.lakekeeper.name
  associate_public_ip_address = true

  root_block_device {
    volume_size           = 30
    volume_type           = "gp3"
    delete_on_termination = true
  }

  # No aws_eip: task 1.4's boot script discovers the instance's own private and public IPv4
  # addresses via IMDSv2 and substitutes both into the compose file itself, so no Terraform
  # attribute of this same resource needs to appear in its own user-data.
  user_data = templatefile("${path.module}/lakekeeper-userdata.sh.tftpl", {
    region           = var.region
    bucket           = local.bucket
    realm_s3_key     = aws_s3_object.keycloak_realm.key
    ssm_root         = local.ssm_root
    oidc_realm       = local.oidc_realm
    oidc_audience    = local.oidc_audience
    postgres_image   = local.postgres_image
    keycloak_image   = local.keycloak_image
    lakekeeper_image = local.lakekeeper_image
  })

  tags = { Name = local.prefix }
}

# --- SSM parameters (single source of truth for lakekeeper-up.sh, secrets.sh, and an in-VPC
# lakekeeper-provision.sh caller holding no OpenTofu workspace state; decision [23]) -------------
# SecureString: values a caller must never see in plaintext outside an authenticated read.
resource "aws_ssm_parameter" "db_password" {
  name  = "${local.ssm_root}/db_password"
  type  = "SecureString"
  value = random_password.db.result
}

resource "aws_ssm_parameter" "metadata_encryption_key" {
  name  = "${local.ssm_root}/metadata_encryption_key"
  type  = "SecureString"
  value = random_password.metadata_encryption_key.result
}

resource "aws_ssm_parameter" "keycloak_admin_password" {
  name  = "${local.ssm_root}/keycloak_admin_password"
  type  = "SecureString"
  value = random_password.keycloak_admin.result
}

resource "aws_ssm_parameter" "storage_access_key_id" {
  name  = "${local.ssm_root}/storage/access_key_id"
  type  = "SecureString"
  value = aws_iam_access_key.lakekeeper_storage.id
}

resource "aws_ssm_parameter" "storage_secret_access_key" {
  name  = "${local.ssm_root}/storage/secret_access_key"
  type  = "SecureString"
  value = aws_iam_access_key.lakekeeper_storage.secret
}

# Copied verbatim from scripts/keycloak-realm-iceberg.json (locals.tf), never regenerated — that
# file stays the client secret's single owner (decision [2] / spec "Keycloak issues tokens...").
resource "aws_ssm_parameter" "oauth2_client_secret" {
  name  = "${local.ssm_root}/oauth2/client_secret"
  type  = "SecureString"
  value = local.oidc_client_secret
}

# Plain String: a URI, a warehouse name, and a public OAuth2 client id are not secrets. Published
# so an in-VPC caller holding no OpenTofu workspace state can assemble a complete LK_TARGET_*
# environment from SSM alone. These MUST NOT diverge from outputs.tf (task 1.3).
resource "aws_ssm_parameter" "warehouse_name" {
  name  = "${local.ssm_root}/warehouse_name"
  type  = "String"
  value = local.warehouse_name
}

resource "aws_ssm_parameter" "oauth2_client_id" {
  name  = "${local.ssm_root}/oauth2/client_id"
  type  = "String"
  value = local.oidc_client_id
}

resource "aws_ssm_parameter" "catalog_uri_public" {
  name  = "${local.ssm_root}/catalog_uri/public"
  type  = "String"
  value = local.catalog_uri_public
}

resource "aws_ssm_parameter" "catalog_uri_private" {
  name  = "${local.ssm_root}/catalog_uri/private"
  type  = "String"
  value = local.catalog_uri_private
}

resource "aws_ssm_parameter" "token_uri_public" {
  name  = "${local.ssm_root}/token_uri/public"
  type  = "String"
  value = local.token_uri_public
}

resource "aws_ssm_parameter" "token_uri_private" {
  name  = "${local.ssm_root}/token_uri/private"
  type  = "String"
  value = local.token_uri_private
}
