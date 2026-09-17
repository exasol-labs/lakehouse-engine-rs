#!/usr/bin/env bash
# The mechanism the first pass missed: OPA PARTIAL EVALUATION (the Compile API).
#
# Instead of asking the policy for a SQL string, declare the table row UNKNOWN.
# OPA evaluates everything it does know (the user, the role map, any data
# document) and returns the RESIDUAL conditions on the unknown row as a
# structured AST. Nothing in the pipeline is SQL until we compile it.
source "$(dirname "$0")/lib.sh"
opa_up

OUT="$EVIDENCE/08-opa-partial-evaluation.txt"
COMPILED="$SPIKE_DIR/compiled"
mkdir -p "$COMPILED"

C() { # C <json-body>
	curl -sS -X POST "$OPA_URL/v1/compile" -H 'Content-Type: application/json' -d "$1"
}
body() { echo "{\"query\":\"data.filtering2.allow == true\",\"input\":{\"user\":\"$1\"},\"unknowns\":[\"input.row\"]}"; }

{
	echo "### OPA $("$OPA" version | head -1)"
	echo "### endpoint: POST $OPA_URL/v1/compile"
	echo "### policy:   policies/filtering2.rego  (no SQL anywhere in it)"
	echo "### unknown:  input.row"
	echo
	echo "=============================================================="
	echo "The source form, for readability (opa eval --partial --format=pretty)"
	echo "=============================================================="
	"$OPA" eval --partial --format=pretty --unknowns 'input.row' \
		--data "$POLICIES/filtering.rego" --input <(echo '{"user":"alice"}') \
		'data.filtering.allow' 2>&1
	echo
	echo "=============================================================="
	echo "TRUTH-VALUE CONTROLS: how the API encodes always-true and always-false."
	echo "These are what make a three-way answer possible."
	echo "=============================================================="
	printf '%-34s ' "query '1 == 1' (always true):"
	C '{"query":"1 == 1","unknowns":["input.row"]}' | jq -c .
	printf '%-34s ' "query '1 == 2' (unsatisfiable):"
	C '{"query":"1 == 2","unknowns":["input.row"]}' | jq -c .
	printf '%-34s ' "query with a residual:"
	C '{"query":"input.row.x == 1","unknowns":["input.row"]}' | jq -c .
	echo
	echo "  queries: [[]]  = one EMPTY conjunction = unconditionally TRUE  = allow all"
	echo "  (no queries)   = unsatisfiable                                = DENY all"
	echo "  queries: [...] = residual conditions                          = filter"
	echo
	echo "=============================================================="
	echo "PER-USER RESPONSES (also written to compiled/<user>.json)"
	echo "=============================================================="
} >"$OUT"

for u in alice bob root carol; do
	C "$(body "$u")" >"$COMPILED/$u.json"
	{
		echo "-------- user = $u"
		jq -c '.result' "$COMPILED/$u.json"
		echo
	} >>"$OUT"
done

# A shape the translation layer must refuse, captured from the real API.
C '{"query":"startswith(input.row.o_comment, \"eu\")","unknowns":["input.row"]}' >"$COMPILED/unsupported.json"
{
	echo "-------- a shape the compiler must REFUSE (startswith)"
	jq -c '.result' "$COMPILED/unsupported.json"
	echo
	echo "=============================================================="
	echo "Does the BODY-QUERY route honour a SQL/UCAST target Accept header?"
	echo "=============================================================="
} >>"$OUT"

for mt in application/vnd.opa.sql.postgresql+json application/vnd.opa.ucast.all+json; do
	printf '%-46s ' "$mt" >>"$OUT"
	got=$(curl -sS -X POST "$OPA_URL/v1/compile" -H 'Content-Type: application/json' \
		-H "Accept: $mt" -d "$(body alice)" | jq -c 'if .result.queries then "IGNORED, returned the plain AST" else . end')
	echo "$got" >>"$OUT"
done
{
	echo
	echo "No: POST /v1/compile with the query in the BODY always returns the AST."
	echo
	echo "CORRECTION (see evidence/16): this is NOT because the build lacks targets."
	echo "There is a SECOND route, POST /v1/compile/<policy path>, where the target"
	echo "IS honoured and UCAST/SQL come back. This spike first concluded targets"
	echo "were unavailable by testing only the route above. They are available, and"
	echo "UCAST is the better input to translate than this AST."
} >>"$OUT"

echo "wrote $OUT"
