variable "region" {
  type        = string
  default     = "eu-west-1"
  description = "Must match the data-stack region."
}

variable "env_name" {
  type        = string
  description = "Test environment name; resources are named spot-strata-<env_name>-trino-*. Pass per benchmark run."
}

variable "instance_type" {
  type        = string
  default     = "r8i.2xlarge"
  description = "Matches an Exasol test1 node's instance type (8 vCPU/64 GB), so Trino and lakehouse-engine-rs run on identical hardware."
}

variable "node_count" {
  type        = number
  default     = 2
  description = "Matches Exasol test1's node count. Node 0 is the coordinator (also runs worker tasks, mirroring Exasol's every-node-executes model); the rest are workers."
}

variable "trino_image_tag" {
  type        = string
  default     = "trinodb/trino:465"
  description = "Pinned Trino Docker image (docker.io); bump deliberately."
}

variable "key_pair_name" {
  type        = string
  description = "Existing EC2 key pair name for SSH debugging (private key on this machine)."
}

variable "allowed_cidrs" {
  type        = list(string)
  default     = []
  description = "Ingress allowlist for SSH/Trino UI+API. Empty => trino-up.sh injects this machine's public IP /32."
}

variable "ttl_days" {
  type        = number
  default     = 2
  description = "Sets exa:ExpiryDate = created + ttl_days (housekeeping signal; not auto-enforced). Short default: this is a benchmark box, tear it down when done."
}

# --- exa:* tag values -------------------------------------------------------
variable "department" {
  type    = string
  default = "ENG"
}
variable "environment" {
  type    = string
  default = "development"
}
variable "workload" {
  type    = string
  default = "benchmark"
}
variable "project" {
  type    = string
  default = "SPOT"
}
variable "cost_center" {
  type    = string
  default = "70010"
}
variable "owner" {
  type    = string
  default = "marco.naetlitz"
}
variable "data_classification" {
  type    = string
  default = "internal"
}
variable "created_date" {
  type        = string
  default     = ""
  description = "exa:CreatedDate (YYYY-MM-DD); empty omits CreatedDate + ExpiryDate. trino-up.sh passes today's date."
}
