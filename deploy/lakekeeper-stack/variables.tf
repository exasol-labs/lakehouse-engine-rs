variable "region" {
  type        = string
  default     = "eu-west-1"
  description = "Must match the data-stack region."
}

variable "env_name" {
  type        = string
  description = "Test environment name; resources are named spot-strata-<env_name>-lakekeeper-*. Pass per benchmark/demo run."
}

variable "key_pair_name" {
  type        = string
  description = "Existing EC2 key pair name for SSH debugging (private key on this machine)."
}

variable "allowed_cidrs" {
  type        = list(string)
  default     = []
  description = "Ingress allowlist for SSH/Lakekeeper/Keycloak. Empty => lakekeeper-up.sh injects this machine's public IP /32."
}

variable "ttl_days" {
  type        = number
  default     = 2
  description = "Sets exa:ExpiryDate = created + ttl_days (housekeeping signal; not auto-enforced). Short default: this is an ephemeral catalog box, tear it down when done."
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
  description = "exa:CreatedDate (YYYY-MM-DD); empty omits CreatedDate + ExpiryDate. lakekeeper-up.sh passes today's date."
}
