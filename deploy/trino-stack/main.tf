# Ephemeral Trino cluster (coordinator + workers, node_count matching Exasol test1), benchmarked
# against the SAME Glue catalog + S3 data as the lakehouse engine and Athena. Cost-safety: this
# stack is applied/destroyed explicitly per benchmark run (deploy/scripts/trino-up.sh /
# trino-down.sh) — never touched by data-stack or cluster-stack applies, so nothing here runs
# unless someone asks for it.
locals {
  prefix     = "spot-strata-${var.env_name}-trino"
  vpc_id     = data.terraform_remote_state.data.outputs.vpc_id
  subnet_id  = data.terraform_remote_state.data.outputs.subnet_id
  bucket     = data.terraform_remote_state.data.outputs.bucket
  bucket_arn = "arn:aws:s3:::${local.bucket}"

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

data "aws_vpc" "this" {
  id = local.vpc_id
}

# --- Security group ---------------------------------------------------------
resource "aws_security_group" "trino" {
  name        = "${local.prefix}-sg"
  description = "Trino benchmark node ${var.env_name}"
  vpc_id      = local.vpc_id
  tags        = { Name = "${local.prefix}-sg" }
}

resource "aws_security_group_rule" "ingress" {
  for_each          = toset(["22"]) # SSH: operator allowlist only
  type              = "ingress"
  from_port         = tonumber(each.value)
  to_port           = tonumber(each.value)
  protocol          = "tcp"
  cidr_blocks       = local.effective_cidrs
  security_group_id = aws_security_group.trino.id
  description       = "port ${each.value} from allowlist"
}

# 8080 (Trino UI/API) additionally needs the whole VPC CIDR, not just the operator's IP: Exasol's
# IMPORT FROM JDBC executes the JDBC connection FROM the Exasol cluster nodes (test1, same VPC/
# subnet as this stack), not from the operator's machine. Without this, every IMPORT FROM JDBC
# query times out at the SG (ETL-5402 "Error fetching next") since the coordinator never receives
# Exasol's TCP handshake — live-verified running bench/import_jdbc_trino.sh against test1.
resource "aws_security_group_rule" "ingress_trino_port" {
  type              = "ingress"
  from_port         = 8080
  to_port           = 8080
  protocol          = "tcp"
  cidr_blocks       = concat(local.effective_cidrs, [data.aws_vpc.this.cidr_block])
  security_group_id = aws_security_group.trino.id
  description       = "port 8080 from allowlist + VPC (Exasol IMPORT FROM JDBC)"
}

resource "aws_security_group_rule" "internode" {
  type              = "ingress"
  from_port         = 0
  to_port           = 0
  protocol          = "-1"
  self              = true
  security_group_id = aws_security_group.trino.id
  description       = "inter-node all traffic (coordinator/worker discovery and queries)"
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

# --- Trino cluster: separate coordinator + worker resources (mirrors Exasol's every-node-executes
# model — the coordinator also runs worker tasks). Split into two resource blocks rather than one
# count-based resource: a worker referencing the coordinator's private_ip is a normal, acyclic
# dependency, but the SAME reference from within a single count-based aws_instance.trino resource
# would make index 0 statically depend on itself (Terraform's dependency graph doesn't evaluate
# the count.index==0 ternary before building the graph) — a real cycle error at plan time.
resource "aws_instance" "trino_coordinator" {
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

  # The coordinator's own discovery.uri is localhost — it hosts the discovery service itself, so
  # this is a plan-time-known literal with no dependency on any other resource.
  user_data = templatefile("${path.module}/trino-userdata.sh.tftpl", {
    trino_image    = var.trino_image_tag
    region         = var.region
    is_coordinator = true
    discovery_uri  = "http://localhost:8080"
    node_count     = var.node_count
  })

  tags = { Name = "${local.prefix}-n11" }
}

resource "aws_instance" "trino_worker" {
  count                       = var.node_count - 1
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

  # Points at the coordinator's private IP — a normal cross-resource dependency, so OpenTofu
  # creates the coordinator first and resolves its private IP before rendering worker user-data.
  user_data = templatefile("${path.module}/trino-userdata.sh.tftpl", {
    trino_image    = var.trino_image_tag
    region         = var.region
    is_coordinator = false
    discovery_uri  = "http://${aws_instance.trino_coordinator.private_ip}:8080"
    node_count     = var.node_count
  })

  tags = { Name = "${local.prefix}-n1${count.index + 2}" }
}
