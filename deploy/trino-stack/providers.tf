terraform {
  required_version = ">= 1.6"
  required_providers {
    aws  = { source = "hashicorp/aws", version = "~> 5.60" }
    http = { source = "hashicorp/http", version = "~> 3.4" }
  }

  # Bucket/key/region come from `tofu init -backend-config=backend.hcl` (gitignored).
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
  # Only set when created_date is given: keeps default_tags plan-known, avoiding the provider's
  # tags_all "inconsistent final plan" bug.
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

data "terraform_remote_state" "data" {
  backend = "s3"
  config = {
    bucket = var.tofu_state_bucket
    key    = "tofu-state/data-stack/terraform.tfstate"
    region = var.region
  }
}
