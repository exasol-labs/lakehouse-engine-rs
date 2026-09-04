output "env_name" {
  value = var.env_name
}

output "public_host" {
  value       = aws_instance.lakekeeper.public_ip
  description = "Operator-machine vantage (lakekeeper-up.sh, lakekeeper-provision.sh run from a laptop)."
}

output "private_host" {
  value       = aws_instance.lakekeeper.private_ip
  description = "Same-VPC vantage (the Exasol UDF; an in-VPC lakekeeper-provision.sh re-provision run)."
}

output "lakekeeper_port" {
  value = 8181
}

output "keycloak_port" {
  value = 8080
}

output "warehouse_name" {
  value       = local.warehouse_name
  description = "Lakekeeper warehouse NAME, not an s3:// path (docs/catalogs.md 'Warehouse is a name, not a path'). Published to SSM identically; see main.tf's warehouse_name parameter."
}

output "catalog_uri_public" {
  value = local.catalog_uri_public
}

output "catalog_uri_private" {
  value = local.catalog_uri_private
}

output "token_uri_public" {
  value = local.token_uri_public
}

output "token_uri_private" {
  value = local.token_uri_private
}

# Realm name, OAuth2 client id, client secret, and audience are READ from the jsondecode(file(...))
# local declared in locals.tf (task 1.1) — scripts/keycloak-realm-iceberg.json stays their single
# owner (decision [2]). Never retyped as literals here.
output "oidc_realm" {
  value = local.oidc_realm
}

output "oidc_client_id" {
  value = local.oidc_client_id
}

output "oidc_client_secret" {
  value     = local.oidc_client_secret
  sensitive = true
}

output "oidc_audience" {
  value = local.oidc_audience
}

output "ssm_root" {
  value = local.ssm_root
}

# The DATA-stack's SSM root, re-published so lakekeeper-up.sh can read it instead of composing it.
# It is NOT derivable from this stack's env_name: data-stack defaults env_name to "data" and is
# applied with no override, so the two names differ in every documented run. Sourced from the same
# data.terraform_remote_state.data declared in providers.tf, so this introduces no cluster-stack
# dependency (plan.md § Dependencies).
output "data_ssm_root" {
  value = data.terraform_remote_state.data.outputs.ssm_root
}

output "security_group_id" {
  value = aws_security_group.lakekeeper.id
}

output "allowed_cidrs" {
  value = local.effective_cidrs
}
