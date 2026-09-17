#!/usr/bin/env bash
# Authentication: can OPA share the IdP the Iceberg REST catalog already uses,
# so the engine presents ONE credential to both, and can OPA resolve the
# querying user's groups from that same IdP?
#
# Needs the repo's Keycloak:
#   docker compose -f docker-compose.yml -f docker-compose.lakekeeper.yml up -d keycloak
# Fails (never skips) when it is unreachable, per the project's E2E contract.
set -euo pipefail
SPIKE_DIR="$(cd "$(dirname "$0")/.." && pwd)"
OUT="$SPIKE_DIR/evidence/10-idp-auth.txt"
OPA="$SPIKE_DIR/.bin/opa"
KC="http://localhost:${LH_KEYCLOAK_PORT:-28080}/realms/iceberg"
CID="${KC_CLIENT_ID:-lakehouse}"
CSEC="${KC_CLIENT_SECRET:-lakehouse-engine-secret}"

curl -sf "$KC/.well-known/openid-configuration" >/dev/null || {
	echo "Keycloak unreachable at $KC" >&2; exit 1; }

{
	echo "### Keycloak realm iceberg, from docker-compose.lakekeeper.yml"
	echo "### issuer:   $(curl -s "$KC/.well-known/openid-configuration" | jq -r .issuer)"
	echo "### jwks:     $(curl -s "$KC/.well-known/openid-configuration" | jq -r .jwks_uri)"
	echo "### client:   $CID  (confidential, service account, client_credentials)"
	echo "### This is the SAME realm/client Lakekeeper is configured against:"
	grep -E 'LAKEKEEPER__OPENID' "$SPIKE_DIR/../../docker-compose.lakekeeper.yml" | sed 's/^/###   /'
	echo
} >"$OUT"

TOK=$(curl -s -X POST "$KC/protocol/openid-connect/token" \
	-d grant_type=client_credentials -d "client_id=$CID" -d "client_secret=$CSEC" | jq -r .access_token)

{
	echo "=============================================================="
	echo "PART 1 -- the engine authenticating ITSELF to OPA."
	echo "The token below is obtained with the same client_credentials grant"
	echo "lakehouse-catalog/src/auth.rs already uses for the REST catalog."
	echo "=============================================================="
	echo "-- claims of that token:"
	echo "$TOK" | cut -d. -f2 | tr '_-' '/+' | base64 -d 2>/dev/null | jq -c '{iss,aud,azp,sub,preferred_username}'
	echo
	echo "-- OPA started with: --authentication=token --authorization=basic"
	echo "-- gate policy: policies-authz/system/authz.rego, which verifies the JWT"
	echo "   against the realm JWKS with io.jwt.decode_verify (iss + aud + signature)"
	echo
} >>"$OUT"

pkill -f 'opa run --server --addr 127.0.0.1:8182' 2>/dev/null || true
"$OPA" run --server --addr 127.0.0.1:8182 --authentication=token --authorization=basic \
	--log-level error "$SPIKE_DIR/policies-authz" "$SPIKE_DIR/policies" >/dev/null 2>&1 &
AUTHZ_PID=$!
sleep 3

Q='{"query":"data.filtering2.allow == true","input":{"user":"alice"},"unknowns":["input.row"]}'
row() { # row <label> [curl args...]
	printf '  %-46s ' "$1" >>"$OUT"; shift
	code=$(curl -sS -o /tmp/opa_authz.txt -w '%{http_code}' -X POST http://127.0.0.1:8182/v1/compile \
		-H 'Content-Type: application/json' "$@" -d "$Q" || echo 000)
	printf 'HTTP %s  %s\n' "$code" "$(jq -c 'if .result then "decision returned" else .message // . end' </tmp/opa_authz.txt 2>/dev/null || head -c 60 /tmp/opa_authz.txt)" >>"$OUT"
}
echo "-- API auth matrix:" >>"$OUT"
row "valid Keycloak token (azp=$CID)" -H "Authorization: Bearer $TOK"
row "NO Authorization header"
row "garbage bearer token" -H "Authorization: Bearer not-a-jwt"
row "valid token, last signature byte changed" -H "Authorization: Bearer ${TOK%?}X"
printf '  %-46s ' "unauthenticated /health (allowed on purpose)" >>"$OUT"
printf 'HTTP %s\n\n' "$(curl -sS -o /dev/null -w '%{http_code}' http://127.0.0.1:8182/health)" >>"$OUT"
kill $AUTHZ_PID 2>/dev/null || true

{
	echo "=============================================================="
	echo "PART 2 -- OPA resolving the QUERYING USER's groups from the same IdP."
	echo "policies-idp/idp_filtering.rego holds its own client_credentials grant,"
	echo "the pattern Lakekeeper's own OPA bridge uses"
	echo "(authz/opa-bridge/policies/lakekeeper/authentication.rego)."
	echo "=============================================================="
	echo "-- Keycloak group membership, set up by this script's fixtures:"
} >>"$OUT"
for u in OPA_ALICE OPA_BOB; do
	printf '     %-12s ' "$u" >>"$OUT"
	"$OPA" eval --format=json --data "$SPIKE_DIR/policies-idp/idp_filtering.rego" \
		--input <(echo "{\"user\":\"$u\"}") 'data.idp.groups' 2>/dev/null |
		jq -c '.result[0].expressions[0].value' >>"$OUT"
done

{
	echo
	echo "-- NEGATIVE CONTROL: partial evaluation does NOT run http.send by default."
	echo "   Without the option, the IdP lookups stay UNEVALUATED in the residual,"
	echo "   which is untranslatable -- and leaks the client secret into the output:"
	echo
	"$OPA" eval --partial --format=pretty --unknowns 'input.row' \
		--data "$SPIKE_DIR/policies-idp/idp_filtering.rego" \
		--input <(echo '{"user":"OPA_ALICE"}') 'data.idp.allow' 2>&1 |
		grep -oE 'http\.send|data\.partial\.idp\.groups|client_secret=%v' | sort | uniq -c | sed 's/^/     /'
	echo "     (counts of unresolved constructs in the residual)"
	echo
	echo "-- WITH the option, the IdP calls are resolved away and only row"
	echo "   conditions remain. CLI flag: --nondeterminstic-builtins (sic)."
	echo "   Compile API: {\"options\": {\"nondeterministicBuiltins\": true}} in the BODY."
	echo "   Query-string forms do NOT work; verified below."
	echo
} >>"$OUT"

pkill -f 'opa run --server --addr 127.0.0.1:8183' 2>/dev/null || true
"$OPA" run --server --addr 127.0.0.1:8183 --log-level error "$SPIKE_DIR/policies-idp" >/dev/null 2>&1 &
IDP_PID=$!
sleep 2
body() { echo "{\"query\":\"data.idp.allow == true\",\"input\":{\"user\":\"$1\"},\"unknowns\":[\"input.row\"]${2:-}}"; }
OPT=',"options":{"nondeterministicBuiltins":true}'

{
	echo "-- which spelling actually enables it:"
	for spec in "plain body (default)::" "?nondeterministic-builtins=true:?nondeterministic-builtins=true:" "body options.nondeterministicBuiltins::$OPT"; do
		IFS=: read -r label qs opt <<<"$spec"
		printf '     %-42s ' "$label"
		curl -sS -X POST "http://127.0.0.1:8183/v1/compile$qs" -H 'Content-Type: application/json' \
			-d "$(body OPA_ALICE "$opt")" |
			jq -c 'if .result.support then "support module: http.send NOT evaluated" elif .result.queries then "resolved: \(.result.queries|length) query(ies)" else . end'
	done
	echo
	echo "-- per-user residuals with the option on (also written to compiled/idp-*.json):"
} >>"$OUT"

for u in OPA_ALICE OPA_BOB OPA_NOBODY; do
	f="$SPIKE_DIR/compiled/idp-$(echo "$u" | tr 'A-Z_' 'a-z-').json"
	curl -sS -X POST 'http://127.0.0.1:8183/v1/compile' -H 'Content-Type: application/json' \
		-d "$(body "$u" "$OPT")" >"$f"
	{
		printf '     %-12s ' "$u"
		"$OPA" eval --partial --format=pretty --nondeterminstic-builtins --unknowns 'input.row' \
			--data "$SPIKE_DIR/policies-idp/idp_filtering.rego" --input <(echo "{\"user\":\"$u\"}") \
			'data.idp.allow' 2>/dev/null | grep -E 'input\.row|undefined' | tr -s ' ' | tr '\n' ';' |
			sed 's/│//g'
		echo
	} >>"$OUT"
done

{
	echo
	echo "     OPA_NOBODY has no IdP account at all, so the policy is undefined for"
	echo "     them, which compiles to DENY. Contrast the row-filters endpoint in"
	echo "     evidence/03, where an unknown user meant UNRESTRICTED."
	echo
	echo "=============================================================="
	echo "PART 3 -- what the IdP round trips cost"
	echo "=============================================================="
} >>"$OUT"

# Cold: OPA restarted so its http.send cache is empty.
kill $IDP_PID 2>/dev/null || true
sleep 1
"$OPA" run --server --addr 127.0.0.1:8183 --log-level error "$SPIKE_DIR/policies-idp" >/dev/null 2>&1 &
IDP_PID=$!
sleep 2
cold=$(curl -sS -o /dev/null -w '%{time_total}' -X POST 'http://127.0.0.1:8183/v1/compile' \
	-H 'Content-Type: application/json' -d "$(body OPA_ALICE "$OPT")")
: >/tmp/opa_warm.txt
for _ in $(seq 1 100); do
	curl -sS -o /dev/null -w '%{time_total}\n' -X POST 'http://127.0.0.1:8183/v1/compile' \
		-H 'Content-Type: application/json' -d "$(body OPA_ALICE "$OPT")" >>/tmp/opa_warm.txt
done
{
	python3 -c "print(f'  cold (empty http.send cache: token + user + groups) = {float('$cold')*1000:.1f} ms')"
	python3 -c "
import statistics
v=sorted(float(l)*1000 for l in open('/tmp/opa_warm.txt') if l.strip())
print(f'  warm, n={len(v)}: median = {statistics.median(v):.3f} ms   p95 = {v[int(len(v)*0.95)]:.3f} ms   max = {v[-1]:.3f} ms')
print(f'  (cache TTLs in the policy: 150 s for the token, 60 s for user and groups)')
"
} >>"$OUT"
kill $IDP_PID 2>/dev/null || true

echo "wrote $OUT"
