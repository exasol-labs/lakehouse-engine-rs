# OPA's OWN API gate. With `--authentication=token --authorization=basic`, OPA
# puts the raw bearer token in `input.identity` and validates NOTHING itself.
# This policy is what turns that into real OIDC authentication, against the SAME
# Keycloak realm the engine already uses for the Iceberg REST catalog.
package system.authz

import rego.v1

# The shared IdP. Same realm, same issuer, same JWKS as LAKEKEEPER__OPENID_PROVIDER_URI.
issuer := "http://localhost:28080/realms/iceberg"

jwks_url := "http://localhost:28080/realms/iceberg/protocol/openid-connect/certs"

# JWKS is fetched once and cached, so this costs nothing per request.
jwks := http.send({
	"method": "GET",
	"url": jwks_url,
	"force_cache": true,
	"force_cache_duration_seconds": 3600,
	"caching_mode": "serialized",
}).raw_body

# Verify signature, issuer and audience in one call. `aud` is the audience the
# engine's own catalog token already carries.
verified := [valid, header, claims] if {
	[valid, header, claims] := io.jwt.decode_verify(input.identity, {
		"cert": jwks,
		"iss": issuer,
		"aud": "lakekeeper",
	})
}

default allow := false

# Health is unauthenticated on purpose, so a load balancer can probe it.
allow if input.path == ["health"]

allow if {
	verified[0] == true
	# Only the engine's own service account may ask for decisions.
	verified[2].azp == "lakehouse"
}
