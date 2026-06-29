terraform {
  required_version = ">= 1.6"
  required_providers {
    aws  = { source = "hashicorp/aws", version = "~> 5.60" }
    time = { source = "hashicorp/time", version = "~> 0.12" }
  }
}

# Credentials come from the environment (AWS_PROFILE=spot-strata-deployer or env vars).
provider "aws" {
  region = var.region

  default_tags {
    tags = {
      "exa:Department"         = var.department
      "exa:Environment"        = var.environment
      "exa:Workload"           = var.workload
      "exa:Project"            = var.project
      "exa:CostCenter"         = var.cost_center
      "exa:Owner"              = var.owner
      "exa:ManagedBy"          = "opentofu"
      "exa:DataClassification" = var.data_classification
      "exa:CreatedDate"        = formatdate("YYYY-MM-DD", time_static.created.rfc3339)
    }
  }
}

resource "time_static" "created" {}
