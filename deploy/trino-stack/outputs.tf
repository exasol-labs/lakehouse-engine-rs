output "env_name" {
  value = var.env_name
}

output "trino_host" {
  value = aws_instance.trino.public_ip
}

output "trino_port" {
  value = 8080
}

output "security_group_id" {
  value = aws_security_group.trino.id
}

output "allowed_cidrs" {
  value = local.effective_cidrs
}
