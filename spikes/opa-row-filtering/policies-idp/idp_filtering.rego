# Row filtering where the user's GROUPS come from the SHARED IdP, not from a map
# hardcoded in the policy. This is the piece section 3 of the README said was
# missing: groups without per-user catalog identity.
#
# The pattern is lifted from Lakekeeper's own OPA bridge
# (authz/opa-bridge/policies/lakekeeper/authentication.rego), which holds a
# client_credentials grant against the same IdP and caches the token.
package idp

import rego.v1

realm := object.get(opa.runtime().env, "KC_REALM_URL", "http://localhost:28080/realms/iceberg")

admin := object.get(opa.runtime().env, "KC_ADMIN_URL", "http://localhost:28080/admin/realms/iceberg")

# OPA's own service identity at the shared IdP. Same client, same secret, same
# realm the engine uses for the Iceberg REST catalog.
token := http.send({
	"method": "POST",
	"url": sprintf("%s/protocol/openid-connect/token", [realm]),
	"headers": {"Content-type": "application/x-www-form-urlencoded"},
	"raw_body": sprintf("grant_type=client_credentials&client_id=%v&client_secret=%v", [
		object.get(opa.runtime().env, "KC_CLIENT_ID", "lakehouse"),
		object.get(opa.runtime().env, "KC_CLIENT_SECRET", "lakehouse-engine-secret"),
	]),
	"force_cache": true,
	"force_cache_duration_seconds": 150,
	"caching_mode": "deserialized",
}).body.access_token

# Resolve the Exasol user name to an IdP user id.
# NOTE: in production the join key is EXA_DBA_USERS.OPENID_SUBJECT (verified to
# exist in evidence/06), which is the IdP `sub`. Matching on username here keeps
# the spike self-contained.
user_id := id if {
	resp := http.send({
		"method": "GET",
		"url": sprintf("%s/users?exact=true&username=%s", [admin, input.user]),
		"headers": {"Authorization": sprintf("Bearer %v", [token])},
		"force_cache": true,
		"force_cache_duration_seconds": 60,
		"caching_mode": "deserialized",
	})
	id := resp.body[0].id
}

groups contains g.name if {
	resp := http.send({
		"method": "GET",
		"url": sprintf("%s/users/%s/groups", [admin, user_id]),
		"headers": {"Authorization": sprintf("Bearer %v", [token])},
		"force_cache": true,
		"force_cache_duration_seconds": 60,
		"caching_mode": "deserialized",
	})
	some g in resp.body
}

# ---- the actual row policy, driven by IdP groups -------------------------
allow if {
	"eu_staff" in groups
	input.row.region in {"EU", "UK", "CH"}
}

allow if {
	"analyst" in groups
	not "eu_staff" in groups
	input.row.region == "US"
	input.row.classification != "SECRET"
}
