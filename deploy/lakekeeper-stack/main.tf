data "aws_caller_identity" "current" {}

locals {
  prefix     = "spot-strata-${var.env_name}-lakekeeper"
  vpc_id     = data.terraform_remote_state.data.outputs.vpc_id
  subnet_id  = data.terraform_remote_state.data.outputs.subnet_id
  bucket     = data.terraform_remote_state.data.outputs.bucket
  bucket_arn = "arn:aws:s3:::${local.bucket}"
  account_id = data.aws_caller_identity.current.account_id
  ssm_root   = "/spot-strata/lakekeeper/${var.env_name}"

  instance_type = "t3.large"

  # Must match docker-compose.lakekeeper.yml's defaults.
  postgres_image   = "postgres:17"
  keycloak_image   = "quay.io/keycloak/keycloak:26.0.7"
  lakekeeper_image = "quay.io/lakekeeper/catalog:v0.13.1"

  warehouse_name = "${local.prefix}-warehouse"

  my_ip_cidr      = "${chomp(data.http.my_ip.response_body)}/32"
  effective_cidrs = length(var.allowed_cidrs) > 0 ? var.allowed_cidrs : [local.my_ip_cidr]

  # In-VPC clients (the Exasol UDF) use the private IP, the operator's laptop the public IP.
  # Keycloak stamps `iss` from the request host, so both issuers must be accepted.
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

# The Exasol UDF's token and Iceberg REST calls originate from the cluster nodes in this VPC.
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

# Delivered via S3 because the 21 KB export exceeds the 16 KB user-data cap. Kept outside the
# `tpch.db/` prefix so it cannot land inside the warehouse prefix derived from table locations.
resource "aws_s3_object" "keycloak_realm" {
  provider = aws.no_default_tags
  bucket   = local.bucket
  key      = "lakekeeper/keycloak-realm-iceberg.json"
  source   = "${path.module}/../../scripts/keycloak-realm-iceberg.json"
  etag     = filemd5("${path.module}/../../scripts/keycloak-realm-iceberg.json")
}

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

# Separate from the read-only engine-reader: Lakekeeper validates a new warehouse by writing,
# reading back, and deleting a probe object.
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
        # Accepted risk: bucket-wide, because the warehouse prefix is only derived after apply.
        Sid      = "WarehouseStorageReadWrite"
        Effect   = "Allow"
        Action   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket", "s3:GetBucketLocation"]
        Resource = [local.bucket_arn, "${local.bucket_arn}/*"]
      }
    ]
  })
}

# Not an inline aws_iam_user_policy: the deployer policy grants no iam:PutUserPolicy.
resource "aws_iam_user_policy_attachment" "lakekeeper_storage" {
  user       = aws_iam_user.lakekeeper_storage.name
  policy_arn = aws_iam_policy.lakekeeper_storage.arn
}

resource "aws_iam_access_key" "lakekeeper_storage" {
  user = aws_iam_user.lakekeeper_storage.name
}

# Generated rather than the local compose file's insecure literals: this box has a public IP.
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

  # The boot script discovers its own IPs via IMDSv2, so no aws_eip is needed.
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

# SSM lets an in-VPC caller with no OpenTofu state assemble a complete LK_TARGET_* environment.
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

resource "aws_ssm_parameter" "oauth2_client_secret" {
  name  = "${local.ssm_root}/oauth2/client_secret"
  type  = "SecureString"
  value = local.oidc_client_secret
}

# Must not diverge from outputs.tf.
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
