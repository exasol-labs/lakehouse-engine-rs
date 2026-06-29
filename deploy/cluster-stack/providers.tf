terraform {
  required_version = ">= 1.6"
  required_providers {
    aws    = { source = "hashicorp/aws", version = "~> 5.60" }
    time   = { source = "hashicorp/time", version = "~> 0.12" }
    random = { source = "hashicorp/random", version = "~> 3.6" }
    http   = { source = "hashicorp/http", version = "~> 3.4" }
  }
}

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
      "exa:ExpiryDate"         = formatdate("YYYY-MM-DD", timeadd(time_static.created.rfc3339, "${var.ttl_days * 24}h"))
      "exa:AutoShutdown"       = "true"
    }
  }
}

resource "time_static" "created" {}

# Read the persistent data-stack outputs (same VPC/subnet so cluster sits next to S3/Glue).
data "terraform_remote_state" "data" {
  backend = "local"
  config = {
    path = "${path.module}/../data-stack/terraform.tfstate"
  }
}
