terraform {
  required_version = ">= 1.6"
  required_providers {
    aws    = { source = "hashicorp/aws", version = "~> 5.60" }
    http   = { source = "hashicorp/http", version = "~> 3.4" }
    random = { source = "hashicorp/random", version = "~> 3.6" }
  }
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

# AWS S3 object tagging caps at 10 tags per object (vs. 50 for most other taggable resource types).
# local.default_tags already carries 11 entries, which every OTHER resource in this stack accepts
# fine under the default provider above — but aws_s3_object.keycloak_realm is the one S3 OBJECT
# (not bucket) this stack creates, so it alone needs an aliased provider with no default_tags to
# opt out of that merge. Referenced ONLY by aws_s3_object.keycloak_realm in main.tf.
provider "aws" {
  alias  = "no_default_tags"
  region = var.region
}

# Read the persistent data-stack outputs (same VPC/subnet, S3 bucket, Glue REST catalog the
# provisioning script reads its source tables from).
data "terraform_remote_state" "data" {
  backend = "local"
  config = {
    path = "${path.module}/../data-stack/terraform.tfstate"
  }
}
