# Single owner of the OIDC realm name, the OAuth2 client id, its secret, and the audience:
# `scripts/keycloak-realm-iceberg.json` (decision [2] / [15]). Read here so main.tf (task 1.2)
# and outputs.tf (task 1.3) both consume the same parsed values instead of a second, divergent
# copy — neither file re-declares this local or retypes these values as literals.
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
