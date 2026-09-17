#!/usr/bin/env bash
# CORRECTION to pass 2. The Compile API has TWO routes:
#
#   POST /v1/compile              query in the BODY, always returns the raw AST
#   POST /v1/compile/<path>       policy path in the URL, target chosen by the
#                                 Accept header -- UCAST or SQL
#
# Pass 2 sent the target Accept header to the first route, where it is ignored,
# and concluded this OPA build has no targets. It does. This probes the second
# route.
source "$(dirname "$0")/lib.sh"
ensure_opa
OUT="$EVIDENCE/16-ucast-target.txt"
PORT=8188
pkill -f "opa run --server --addr 127.0.0.1:$PORT" 2>/dev/null || true
setsid "$OPA" run --server --addr "127.0.0.1:$PORT" --log-level error \
	"$SPIKE_DIR/policies-ucast" "$SPIKE_DIR/policies-tables" "$SPIKE_DIR/policies" \
	>/tmp/opa_ucast.log 2>&1 </dev/null &
disown
for _ in $(seq 1 30); do curl -sf "http://127.0.0.1:$PORT/health" >/dev/null 2>&1 && break; done

ask() { # ask <policy-path> <accept-suffix> <input-json> [extra-body-json]
	local extra=${4:-'{}'}
	curl -sS -X POST "http://127.0.0.1:$PORT/v1/compile/$1" \
		-H 'Content-Type: application/json' -H "Accept: application/vnd.opa.$2+json" \
		-d "$(jq -nc --argjson i "$3" --argjson x "$extra" '{input:$i, unknowns:["input.row"]} + $x')"
}

{
echo "### OPA $("$OPA" version | sed -n '1s/Version: //p'), open-source build"
echo
echo "=============================================================="
echo "1. THE TWO ROUTES"
echo "=============================================================="
printf '  POST /v1/compile            (query in body, Accept: ucast)  -> '
curl -sS -X POST "http://127.0.0.1:$PORT/v1/compile" -H 'Content-Type: application/json' \
	-H 'Accept: application/vnd.opa.ucast.all+json' \
	-d '{"query":"data.gate.allow == true","input":{"user":"ALICE","table":"ORDERS_PUBLIC"},"unknowns":["input.row"]}' |
	head -c 120 | tr -d '\n'
echo '  <- raw AST, target IGNORED'
printf '  POST /v1/compile/gate/allow (path in URL,  Accept: ucast)  -> '
ask gate/allow ucast.all '{"user":"ALICE","table":"ORDERS_PUBLIC"}' | tr -d '\n'
echo '  <- UCAST'
echo
echo "=============================================================="
echo "2. THE THREE TRUTH VALUES SURVIVE, and are easier to tell apart"
echo "=============================================================="
for pair in 'ALICE ORDERS_PUBLIC' 'ALICE CUSTOMERS' 'ALICE ORDERS_SECRET' 'NOBODY ORDERS_PUBLIC'; do
	set -- $pair
	printf '  %-7s %-14s -> ' "$1" "$2"
	ask gate/allow ucast.all "$(jq -nc --arg u "$1" --arg t "$2" '{user:$u,table:$t}')" | head -c 120 | tr -d '\n'
	echo
done
cat <<'NOTE'
    FILTER    = {"result":{"query":{...}}}
    ALLOW ALL = {"result":{"query":{}}}      <- query present but empty
    DENY      = {}                           <- no `result` key at all
NOTE
echo
echo "=============================================================="
echo "3. OR-of-AND: the compound shape, from policies/filtering2.rego"
echo "=============================================================="
for u in alice bob root carol; do
	printf '  %-6s UCAST -> ' "$u"; ask filtering2/allow ucast.all "$(jq -nc --arg u "$u" '{user:$u}')" | tr -d '\n'; echo
	printf '  %-6s SQL   -> ' "$u"; ask filtering2/allow sql.postgresql "$(jq -nc --arg u "$u" '{user:$u}')" | tr -d '\n'; echo
done
echo
echo "=============================================================="
echo "4. THE CASES THE HAND-WRITTEN AST COMPILER HAD TO HANDLE"
echo "=============================================================="
for r in inject unsupported numeric crosstable; do
	printf '  %-12s UCAST -> ' "$r"; ask "ucastprobe/$r" ucast.all '{}' | head -c 200 | tr -d '\n'; echo
	printf '  %-12s SQL   -> ' "$r"; ask "ucastprobe/$r" sql.postgresql '{}' | head -c 200 | tr -d '\n'; echo
done
cat <<'NOTE'
    inject      UCAST carries the literal as a JSON *value*, so there is no SQL
                text to break out of -- injection is structurally impossible,
                not merely escaped. The SQL target escapes it correctly.
    unsupported `startswith` IS inside the UCAST fragment, so the fragment is
                WIDER than the hand-written allowlist in df-probe. The SQL
                target renders it as LIKE.
    numeric     UCAST keeps 100000 a JSON number. The SQL target emits
                `< E'100000'` -- a NUMBER as a STRING literal. Defect; another
                reason to take UCAST and render SQL ourselves.
    crosstable  a reference outside the unknown row -> {} = DENY. Fails closed.
NOTE
echo
echo "=============================================================="
echo "5. FRAGMENTS AND OPTIONS"
echo "=============================================================="
printf '  Accept ucast.minimal on `in`   -> '
ask gate/allow ucast.minimal '{"user":"ALICE","table":"ORDERS_PUBLIC"}' | head -c 190 | tr -d '\n'; echo
echo "    -> ucast.minimal is a NARROWER fragment and refuses with a NAMED error"
echo "       (pe_fragment_error), which is a stronger guarantee than our own"
echo "       allowlist: OPA itself rejects an untranslatable policy."
printf '  unknowns omitted                -> '
curl -sS -X POST "http://127.0.0.1:$PORT/v1/compile/ucastprobe/numeric" \
	-H 'Content-Type: application/json' -H 'Accept: application/vnd.opa.ucast.all+json' \
	-d '{"input":{}}' | tr -d '\n'
echo '   <- required; omitting it DENIES'
printf '  options.nondeterministicBuiltins -> '
ask ucastprobe/numeric ucast.all '{}' '{"options":{"nondeterministicBuiltins":true}}' | head -c 110 | tr -d '\n'
echo '  <- accepted on this route too (needed for the IdP variant)'
printf '  Accept: application/json         -> '
curl -sS -X POST "http://127.0.0.1:$PORT/v1/compile/ucastprobe/numeric" \
	-H 'Content-Type: application/json' -H 'Accept: application/json' \
	-d '{"input":{},"unknowns":["input.row"]}' | tr -d '\n'
echo
echo
cat <<'NOTE'
-- conclusion --
Use POST /v1/compile/<policy path> with Accept: application/vnd.opa.ucast.all+json.
The policy path rides in the URL, so ONE URI carries the whole endpoint
configuration and no separate OPA_QUERY property is needed -- the same shape
Trino's opa.policy.uri uses. Translate UCAST rather than the raw AST: typed
values, no injection surface, a wider operator fragment, and OPA names its own
refusals. Do NOT take the SQL targets: none is a DataFusion dialect and the
numeric literal above is rendered as a string.
NOTE
} >"$OUT"
pkill -f "opa run --server --addr 127.0.0.1:$PORT" 2>/dev/null || true
echo "wrote $OUT"
