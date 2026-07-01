terraform {
  required_version = ">= 1.6"
  required_providers {
    aws = { source = "hashicorp/aws", version = "~> 5.60" }
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
  }
  # CreatedDate is recommended (not mandatory); include only when provided (keeps tags plan-known).
  default_tags = var.created_date != "" ? merge(local.base_tags, { "exa:CreatedDate" = var.created_date }) : local.base_tags
}

# Credentials come from the environment (AWS_PROFILE=spot-strata-deployer or env vars).
provider "aws" {
  region = var.region

  default_tags {
    tags = local.default_tags
  }
}
