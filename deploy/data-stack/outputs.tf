output "region" {
  value = var.region
}

output "account_id" {
  value = local.account_id
}

output "vpc_id" {
  value = aws_vpc.this.id
}

output "subnet_id" {
  value = aws_subnet.public.id
}

output "prefix" {
  value = local.prefix
}

output "bucket" {
  value = aws_s3_bucket.warehouse.bucket
}

output "glue_uri" {
  value = local.glue_uri
}

output "glue_warehouse" {
  value = local.glue_warehouse
}

output "tpch_database" {
  value = aws_glue_catalog_database.tpch.name
}

output "perf_database" {
  value = aws_glue_catalog_database.perf.name
}

output "athena_workgroup" {
  value = aws_athena_workgroup.perf.name
}

output "ssm_root" {
  value = local.ssm_root
}

output "engine_reader_user" {
  value = aws_iam_user.engine_reader.name
}

output "datagen_status_param" {
  value = "${local.ssm_root}/datagen/last_status"
}

output "datagen_public_ip" {
  value       = var.run_data_gen ? aws_instance.datagen[0].public_ip : null
  description = "SSH here to tail /var/log/datagen.log while loading (key_pair_name required)."
}
