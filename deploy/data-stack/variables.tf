variable "region" {
  type        = string
  default     = "eu-west-1"
  description = "AWS region; also derives the Glue Iceberg REST URI."
}

variable "env_name" {
  type        = string
  default     = "data"
  description = "Stack env name; every resource is named spot-strata-<env_name>-*."
}

variable "vpc_cidr" {
  type    = string
  default = "10.42.0.0/16"
}

variable "subnet_cidr" {
  type    = string
  default = "10.42.1.0/24"
}

# --- data generation (temporary EC2) ---------------------------------------
variable "run_data_gen" {
  type        = bool
  default     = false
  description = "When true, launch the temporary data-gen EC2 (self-terminates after loading)."
}

variable "datagen_instance_type" {
  type        = string
  default     = "c7i.4xlarge"
  description = "Larger compute/EBS-throughput instance just for generating + uploading the ~180 GB perf set."
}

variable "datagen_scratch_gb" {
  type        = number
  default     = 400
  description = "Root/scratch disk on the data-gen EC2 (DuckDB staging headroom)."
}

variable "key_pair_name" {
  type        = string
  default     = ""
  description = "Optional EC2 key pair for SSHing into the data-gen EC2 to debug. Empty = no SSH key."
}

# --- dataset sizing ---------------------------------------------------------
variable "tpch_scale_factor" {
  type        = number
  default     = 30
  description = "TPC-H scale factor (~30 => lineitem 5-6 GB parquet)."
}

variable "lineitem_files" {
  type        = number
  default     = 20
  description = "Number of parquet files for big TPC-H tables (shard fan-out)."
}

variable "perf_table_sizes_gb" {
  type        = list(number)
  default     = [10, 20, 30, 40, 80]
  description = "Sizes (GB) of the wide perf tables; one table per size (perf.t_<n>g)."
}

variable "perf_files" {
  type        = number
  default     = 8
  description = "Number of parquet files per perf table."
}

variable "tpch_db_name" {
  type    = string
  default = "tpch"
}

variable "perf_db_name" {
  type    = string
  default = "perf"
}

# --- Spark benchmark (EMR Serverless, opt-in) ------------------------------
variable "enable_emr_serverless" {
  type        = bool
  default     = false
  description = "When true, create the EMR Serverless Spark application (billed only while a job runs; nothing when idle). Off by default: nothing shall be started unless used."
}

variable "emr_serverless_idle_timeout_minutes" {
  type        = number
  default     = 15
  description = "Auto-stop idle timeout — even a forgotten 'started' application costs nothing beyond this window."
}

variable "emr_serverless_max_capacity" {
  type        = number
  default     = 4
  description = "Max vCPU for the application's driver+executors (memory scales as 4x this, in GB)."
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
  description = "exa:CreatedDate (YYYY-MM-DD); empty omits the tag. Kept plan-known to avoid tags_all churn."
}
