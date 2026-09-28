# scripts/keycloak-realm-iceberg.json is the single owner of the realm, client id, secret, and
# audience; never retype them as literals.
locals {
  keycloak_realm = jsondecode(file("${path.module}/../../scripts/keycloak-realm-iceberg.json"))

  oidc_realm = local.keycloak_realm.realm

  oidc_client = [
    for c in local.keycloak_realm.clients : c
    if c.clientId == "lakehouse"
  ][0]

  oidc_client_id     = local.oidc_client.clientId
  oidc_client_secret = local.oidc_client.secret

  oidc_audience = [
    for m in local.oidc_client.protocolMappers : m.config["included.custom.audience"]
    if m.protocolMapper == "oidc-audience-mapper"
  ][0]
}
