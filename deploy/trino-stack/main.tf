# Single ephemeral Trino node, benchmarked against the SAME Glue Iceberg REST catalog + S3 data as
# the lakehouse engine and Athena. Cost-safety: this stack is applied/destroyed explicitly per
# benchmark run (deploy/scripts/trino-up.sh / trino-down.sh) — never touched by data-stack or
# cluster-stack applies, so nothing here runs unless someone asks for it.
locals {
  prefix     = "spot-strata-${var.env_name}-trino"
  vpc_id     = data.terraform_remote_state.data.outputs.vpc_id
  subnet_id  = data.terraform_remote_state.data.outputs.subnet_id
  bucket     = data.terraform_remote_state.data.outputs.bucket
  bucket_arn = "arn:aws:s3:::${local.bucket}"
  glue_uri   = data.terraform_remote_state.data.outputs.glue_uri
  glue_wh    = data.terraform_remote_state.data.outputs.glue_warehouse

  # Ingress allowlist: explicit var, else this machine's public IP /32 (resolved at apply).
  my_ip_cidr      = "${chomp(data.http.my_ip.response_body)}/32"
  effective_cidrs = length(var.allowed_cidrs) > 0 ? var.allowed_cidrs : [local.my_ip_cidr]
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

# --- Security group ---------------------------------------------------------
resource "aws_security_group" "trino" {
  name        = "${local.prefix}-sg"
  description = "Trino benchmark node ${var.env_name}"
  vpc_id      = local.vpc_id
  tags        = { Name = "${local.prefix}-sg" }
}

resource "aws_security_group_rule" "ingress" {
  for_each          = toset(["22", "8080"]) # SSH, Trino UI/API
  type              = "ingress"
  from_port         = tonumber(each.value)
  to_port           = tonumber(each.value)
  protocol          = "tcp"
  cidr_blocks       = local.effective_cidrs
  security_group_id = aws_security_group.trino.id
  description       = "port ${each.value} from allowlist"
}

resource "aws_security_group_rule" "egress" {
  type              = "egress"
  from_port         = 0
  to_port           = 0
  protocol          = "-1"
  cidr_blocks       = ["0.0.0.0/0"]
  security_group_id = aws_security_group.trino.id
  description       = "all egress"
}

# --- IAM: instance profile for Trino's native S3 filesystem + Glue REST catalog reads ------
# An instance-profile role (default AWS credential chain) is used instead of static keys or the
# REST catalog's vended credentials — Trino's native-s3 filesystem supports IMDS directly, so no
# secret plumbing is needed on the box (unlike Exasol's CONNECTION object, which forces static keys).
resource "aws_iam_role" "trino" {
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

resource "aws_iam_role_policy" "trino_read" {
  name = "${local.prefix}-read"
  role = aws_iam_role.trino.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "GlueRead"
        Effect = "Allow"
        Action = [
          "glue:GetCatalog",
          "glue:GetDatabase",
          "glue:GetDatabases",
          "glue:GetTable",
          "glue:GetTables",
          "glue:GetPartition",
          "glue:GetPartitions"
        ]
        Resource = "*"
      },
      {
        Sid      = "S3Read"
        Effect   = "Allow"
        Action   = ["s3:GetObject", "s3:ListBucket", "s3:GetBucketLocation"]
        Resource = [local.bucket_arn, "${local.bucket_arn}/*"]
      }
    ]
  })
}

resource "aws_iam_instance_profile" "trino" {
  name = "${local.prefix}-profile"
  role = aws_iam_role.trino.name
}

# --- Trino node --------------------------------------------------------------
resource "aws_instance" "trino" {
  ami                         = data.aws_ami.ubuntu.id
  instance_type               = var.instance_type
  subnet_id                   = local.subnet_id
  vpc_security_group_ids      = [aws_security_group.trino.id]
  key_name                    = var.key_pair_name
  iam_instance_profile        = aws_iam_instance_profile.trino.name
  associate_public_ip_address = true

  root_block_device {
    volume_size           = 50
    volume_type           = "gp3"
    delete_on_termination = true
  }

  user_data = templatefile("${path.module}/trino-userdata.sh.tftpl", {
    trino_image = var.trino_image_tag
    glue_uri    = local.glue_uri
    glue_wh     = local.glue_wh
    region      = var.region
  })

  tags = { Name = local.prefix }
}
