output "env_name" {
  value = var.env_name
}

output "prefix" {
  value = local.prefix
}

output "node_count" {
  value = var.node_count
}

output "reserve_nodes" {
  value = var.reserve_nodes
}

# Ordered (count index) — cluster-up.sh feeds these to .ccc/config in the SAME order.
output "internal_ips" {
  value = aws_instance.node[*].private_ip
}

output "external_ips" {
  value = aws_instance.node[*].public_ip
}

output "node_names" {
  value = aws_instance.node[*].tags["Name"]
}

output "first_node_ip" {
  value = aws_instance.node[0].public_ip
}

output "key_pair_name" {
  value = var.key_pair_name
}

output "security_group_id" {
  value = aws_security_group.exasol.id
}

output "allowed_cidrs" {
  value = local.effective_cidrs
}

output "ssm_root" {
  value = local.ssm_root
}

# Pull from the data-stack so secrets.sh has everything from cluster outputs.
output "data_ssm_root" {
  value = data.terraform_remote_state.data.outputs.ssm_root
}

output "exasol_db_port" {
  value = 8563
}

output "bucketfs_port" {
  value = 2581
}
