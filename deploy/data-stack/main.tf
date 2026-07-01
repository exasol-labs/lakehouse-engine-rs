data "aws_caller_identity" "current" {}

locals {
  prefix     = "spot-strata-${var.env_name}"
  account_id = data.aws_caller_identity.current.account_id
  bucket     = "${local.prefix}-lakehouse-${local.account_id}"
  glue_uri   = "https://glue.${var.region}.amazonaws.com/iceberg"
  # Glue's Iceberg REST endpoint requires the warehouse as 'catalogs/{accountId}'.
  glue_warehouse = "catalogs/${local.account_id}"
  ssm_root       = "/spot-strata/${var.env_name}"
}

# --- Network: VPC + public subnet + IGW + S3 gateway endpoint --------------
resource "aws_vpc" "this" {
  cidr_block           = var.vpc_cidr
  enable_dns_support   = true
  enable_dns_hostnames = true
  tags                 = { Name = "${local.prefix}-vpc" }
}

resource "aws_internet_gateway" "this" {
  vpc_id = aws_vpc.this.id
  tags   = { Name = "${local.prefix}-igw" }
}

resource "aws_subnet" "public" {
  vpc_id                  = aws_vpc.this.id
  cidr_block              = var.subnet_cidr
  map_public_ip_on_launch = true
  tags                    = { Name = "${local.prefix}-subnet-public" }
}

resource "aws_route_table" "public" {
  vpc_id = aws_vpc.this.id
  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.this.id
  }
  tags = { Name = "${local.prefix}-rt-public" }
}

resource "aws_route_table_association" "public" {
  subnet_id      = aws_subnet.public.id
  route_table_id = aws_route_table.public.id
}

# Free gateway endpoint => EC2<->S3 stays on the AWS backbone (no NAT, no transfer cost).
resource "aws_vpc_endpoint" "s3" {
  vpc_id            = aws_vpc.this.id
  service_name      = "com.amazonaws.${var.region}.s3"
  vpc_endpoint_type = "Gateway"
  route_table_ids   = [aws_route_table.public.id]
  tags              = { Name = "${local.prefix}-s3-endpoint" }
}

# --- S3 warehouse bucket ----------------------------------------------------
resource "aws_s3_bucket" "warehouse" {
  bucket = local.bucket
  tags   = { Name = local.bucket }
}

resource "aws_s3_bucket_public_access_block" "warehouse" {
  bucket                  = aws_s3_bucket.warehouse.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# --- Glue Iceberg databases (tables created by gen_load.py) -----------------
resource "aws_glue_catalog_database" "tpch" {
  name         = var.tpch_db_name
  location_uri = "s3://${aws_s3_bucket.warehouse.bucket}/${var.tpch_db_name}.db"
}

resource "aws_glue_catalog_database" "perf" {
  name         = var.perf_db_name
  location_uri = "s3://${aws_s3_bucket.warehouse.bucket}/${var.perf_db_name}.db"
}

# --- Athena (benchmark consumer of the same Glue catalog) -------------------
resource "aws_athena_workgroup" "perf" {
  name = "${local.prefix}-athena"
  configuration {
    enforce_workgroup_configuration    = true
    publish_cloudwatch_metrics_enabled = true
    result_configuration {
      output_location = "s3://${aws_s3_bucket.warehouse.bucket}/athena-results/"
    }
  }
  tags = { Name = "${local.prefix}-athena" }
}

# --- engine-reader IAM user (creds embedded in the Exasol CONNECTION) -------
resource "aws_iam_user" "engine_reader" {
  name = "${local.prefix}-engine-reader"
  tags = { Name = "${local.prefix}-engine-reader" }
}

resource "aws_iam_policy" "engine_reader" {
  name = "${local.prefix}-engine-reader-policy"
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
        Resource = [aws_s3_bucket.warehouse.arn, "${aws_s3_bucket.warehouse.arn}/*"]
      }
    ]
  })
}

resource "aws_iam_user_policy_attachment" "engine_reader" {
  user       = aws_iam_user.engine_reader.name
  policy_arn = aws_iam_policy.engine_reader.arn
}

resource "aws_iam_access_key" "engine_reader" {
  user = aws_iam_user.engine_reader.name
}

# --- SSM parameters (single source of truth for secrets.sh + bench) ---------
resource "aws_ssm_parameter" "engine_access_key_id" {
  name  = "${local.ssm_root}/engine/access_key_id"
  type  = "SecureString"
  value = aws_iam_access_key.engine_reader.id
}

resource "aws_ssm_parameter" "engine_secret_access_key" {
  name  = "${local.ssm_root}/engine/secret_access_key"
  type  = "SecureString"
  value = aws_iam_access_key.engine_reader.secret
}

resource "aws_ssm_parameter" "glue_uri" {
  name  = "${local.ssm_root}/glue/uri"
  type  = "String"
  value = local.glue_uri
}

resource "aws_ssm_parameter" "glue_warehouse" {
  name  = "${local.ssm_root}/glue/warehouse"
  type  = "String"
  value = local.glue_warehouse
}

resource "aws_ssm_parameter" "region" {
  name  = "${local.ssm_root}/region"
  type  = "String"
  value = var.region
}

resource "aws_ssm_parameter" "bucket" {
  name  = "${local.ssm_root}/bucket"
  type  = "String"
  value = aws_s3_bucket.warehouse.bucket
}

resource "aws_ssm_parameter" "tpch_namespace" {
  name  = "${local.ssm_root}/namespace/tpch"
  type  = "String"
  value = var.tpch_db_name
}

resource "aws_ssm_parameter" "perf_namespace" {
  name  = "${local.ssm_root}/namespace/perf"
  type  = "String"
  value = var.perf_db_name
}

# --- Temporary data-gen EC2 (count gated; self-terminates) ------------------
data "aws_ami" "ubuntu" {
  count       = var.run_data_gen ? 1 : 0
  most_recent = true
  owners      = ["099720109477"] # Canonical
  filter {
    name   = "name"
    values = ["ubuntu/images/hvm-ssd*/ubuntu-noble-24.04-amd64-server-*"]
  }
}

resource "aws_iam_role" "datagen" {
  count = var.run_data_gen ? 1 : 0
  name  = "${local.prefix}-datagen-role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "ec2.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
  tags = { Name = "${local.prefix}-datagen-role" }
}

resource "aws_iam_role_policy" "datagen" {
  count = var.run_data_gen ? 1 : 0
  name  = "${local.prefix}-datagen-write"
  role  = aws_iam_role.datagen[0].id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "GlueWrite"
        Effect = "Allow"
        Action = [
          "glue:GetCatalog", "glue:GetDatabase", "glue:GetDatabases",
          "glue:CreateTable", "glue:UpdateTable", "glue:DeleteTable",
          "glue:GetTable", "glue:GetTables",
          "glue:GetPartition", "glue:GetPartitions", "glue:BatchCreatePartition"
        ]
        Resource = "*"
      },
      {
        Sid      = "S3Write"
        Effect   = "Allow"
        Action   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket", "s3:GetBucketLocation"]
        Resource = [aws_s3_bucket.warehouse.arn, "${aws_s3_bucket.warehouse.arn}/*"]
      },
      {
        Sid      = "SsmStatus"
        Effect   = "Allow"
        Action   = ["ssm:PutParameter"]
        Resource = "arn:aws:ssm:${var.region}:${local.account_id}:parameter${local.ssm_root}/datagen/*"
      }
    ]
  })
}

resource "aws_iam_instance_profile" "datagen" {
  count = var.run_data_gen ? 1 : 0
  name  = "${local.prefix}-datagen-profile"
  role  = aws_iam_role.datagen[0].name
}

# Upload the generator to S3 so user-data fetches it (avoids the 16 KB user-data limit).
resource "aws_s3_object" "gen_load" {
  count  = var.run_data_gen ? 1 : 0
  bucket = aws_s3_bucket.warehouse.id
  key    = "scripts/gen_load.py"
  source = "${path.module}/../scripts/gen_load.py"
  etag   = filemd5("${path.module}/../scripts/gen_load.py")
}

resource "aws_instance" "datagen" {
  count                       = var.run_data_gen ? 1 : 0
  ami                         = data.aws_ami.ubuntu[0].id
  instance_type               = var.datagen_instance_type
  subnet_id                   = aws_subnet.public.id
  iam_instance_profile        = aws_iam_instance_profile.datagen[0].name
  key_name                    = var.key_pair_name != "" ? var.key_pair_name : null
  associate_public_ip_address = true

  instance_initiated_shutdown_behavior = "terminate"

  root_block_device {
    volume_size           = var.datagen_scratch_gb
    volume_type           = "gp3"
    delete_on_termination = true
  }

  user_data = templatefile("${path.module}/datagen-userdata.sh.tftpl", {
    region         = var.region
    bucket         = aws_s3_bucket.warehouse.bucket
    script_key     = aws_s3_object.gen_load[0].key
    tpch_db        = var.tpch_db_name
    perf_db        = var.perf_db_name
    warehouse      = local.account_id
    glue_uri       = local.glue_uri
    scale          = var.tpch_scale_factor
    lineitem_files = var.lineitem_files
    perf_sizes     = join(",", [for s in var.perf_table_sizes_gb : tostring(s)])
    perf_files     = var.perf_files
    done_param     = "${local.ssm_root}/datagen/last_status"
  })

  tags = { Name = "${local.prefix}-datagen" }
}
