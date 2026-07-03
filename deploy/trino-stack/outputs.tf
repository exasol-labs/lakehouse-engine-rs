output "env_name" {
  value = var.env_name
}

output "trino_coordinator_host" {
  value       = aws_instance.trino_coordinator.public_ip
  description = "Point bench/trino_compare.sh's TRINO_HOST here — only the coordinator accepts client query submissions."
}

output "trino_worker_hosts" {
  value = aws_instance.trino_worker[*].public_ip
}

output "trino_port" {
  value = 8080
}

output "node_count" {
  value = var.node_count
}

output "security_group_id" {
  value = aws_security_group.trino.id
}

output "allowed_cidrs" {
  value = local.effective_cidrs
}
