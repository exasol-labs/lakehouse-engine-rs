variable "region" {
  type        = string
  default     = "eu-west-1"
  description = "Must match the data-stack region."
}

variable "env_name" {
  type        = string
  description = "Test environment name; resources are named spot-strata-<env_name>-*. Pass per cluster."
}

variable "node_count" {
  type        = number
  default     = 2
  description = "Active Exasol database nodes."
}

variable "reserve_nodes" {
  type        = number
  default     = 0
  description = "Hot-reserve hosts (CCC_PLAY_RESERVE_NODES). Total EC2 = node_count + reserve_nodes."
}

variable "instance_type" {
  type    = string
  default = "r8i.2xlarge"
}

variable "os_disk_gb" {
  type        = number
  default     = 50
  description = "Root volume (OS)."
}

variable "data_disk_gb" {
  type        = number
  default     = 300
  description = "Second blank volume; Exasol claims it (CCC_HOST_DATADISK=/dev/nvme1n1)."
}

variable "key_pair_name" {
  type        = string
  description = "Existing EC2 key pair name for SSH + c4 (private key on this machine)."
}

variable "allowed_cidrs" {
  type        = list(string)
  default     = []
  description = "Ingress allowlist for SSH/c4/DB/AdminUI/BucketFS. Empty => cluster-up.sh injects this machine's public IP /32. Add 0.0.0.0/0 to open to the internet."
}

variable "ttl_days" {
  type        = number
  default     = 7
  description = "Sets exa:ExpiryDate = created + ttl_days (housekeeping signal; not auto-enforced)."
}

variable "exasol_image_tag" {
  type        = string
  default     = "exasol-2025.2.1"
  description = "c4 play target (@<tag>)."
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
  default = "database"
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
  description = "exa:CreatedDate (YYYY-MM-DD); empty omits CreatedDate + ExpiryDate. Scripts pass today's date."
}
