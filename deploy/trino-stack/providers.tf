terraform {
  required_version = ">= 1.6"
  required_providers {
    aws  = { source = "hashicorp/aws", version = "~> 5.60" }
    http = { source = "hashicorp/http", version = "~> 3.4" }
  }

  # Shared S3 state (was local backend, see data-stack/providers.tf for why). Bucket/key/region come
  # from `tofu init -backend-config=backend.hcl` (gitignored), not hardcoded here.
  backend "s3" {}
}

locals {
  base_tags = {
    "exa:Department"         = var.department
    "exa:Environment"        = var.environment
    "exa:Workload"           = var.workload
    "exa:Project"            = var.project
    "exa:CostCenter"         = var.cost_center
    "exa:Owner"              = var.owner
    "exa:ManagedBy"          = "opentofu"
    "exa:DataClassification" = var.data_classification
    "exa:AutoShutdown"       = "true"
  }
  # CreatedDate/ExpiryDate are recommended for temp resources; include only when created_date is set
  # (keeps default_tags plan-known -> avoids the tags_all "inconsistent final plan" provider bug).
  date_tags = var.created_date != "" ? {
    "exa:CreatedDate" = var.created_date
    "exa:ExpiryDate"  = formatdate("YYYY-MM-DD", timeadd("${var.created_date}T00:00:00Z", "${var.ttl_days * 24}h"))
  } : {}
  default_tags = merge(local.base_tags, local.date_tags)
}

provider "aws" {
  region = var.region

  default_tags {
    tags = local.default_tags
  }
}

# Read the persistent data-stack outputs (same VPC/subnet, S3 bucket, Glue REST catalog).
# var.tofu_state_bucket comes from terraform.tfvars (gitignored) — not hardcoded (see backend above).
data "terraform_remote_state" "data" {
  backend = "s3"
  config = {
    bucket = var.tofu_state_bucket
    key    = "tofu-state/data-stack/terraform.tfstate"
    region = var.region
  }
}
