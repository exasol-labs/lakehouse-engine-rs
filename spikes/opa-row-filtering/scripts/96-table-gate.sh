#!/usr/bin/env bash
# Can OPA answer the COARSE-GRAIN table question too, or is Lakekeeper's
# permission check needed for that? One /v1/compile call, one policy, three
# distinct answers: DENY (no table access), ALLOW ALL (table, no row limit),
# FILTER (table + row limit). The table gate is the degenerate row filter.
source "$(dirname "$0")/lib.sh"
ensure_opa
OUT="$EVIDENCE/14-table-gate.txt"
PORT=8185
pkill -f "opa run --server --addr 127.0.0.1:$PORT" 2>/dev/null || true
"$OPA" run --server --addr "127.0.0.1:$PORT" --log-level error "$SPIKE_DIR/policies-tables" >/dev/null 2>&1 &
PID=$!
sleep 2
{
	echo "### policy: policies-tables/gate.rego    data: policies-tables/data.json"
	echo "### the whole permission model -- table grants AND row limits in one document:"
	jq . "$SPIKE_DIR/policies-tables/data.json" | sed 's/^/###   /'
	echo
	echo "### ONE call per (user, table): POST /v1/compile"
	echo "###   query   = data.gate.allow == true"
	echo "###   input   = {user, table}"
	echo "###   unknowns= [input.row]"
	echo
	echo "=============================================================="
	printf '%-8s %-15s %-34s %s\n' USER TABLE 'COMPILE RESULT' MEANING
	echo "=============================================================="
} >"$OUT"
row() { # row <user> <table>
	local f resp
	f="$SPIKE_DIR/compiled/tbl-$(echo "$1-$2" | tr 'A-Z' 'a-z').json"
	curl -sS -X POST "http://127.0.0.1:$PORT/v1/compile" -H 'Content-Type: application/json' \
		-d "$(jq -nc --arg u "$1" --arg t "$2" '{query:"data.gate.allow == true",
			input:{user:$u, table:$t}, unknowns:["input.row"]}')" >"$f"
	resp=$(jq -c '.result' "$f")
	local meaning
	case "$resp" in
	'{}') meaning="DENY -- no rows may be read" ;;
	'{"queries":[[]]}') meaning="ALLOW ALL -- no filter needed" ;;
	*queries*) meaning="FILTER on region in [$(jq -r '[.result.queries[][].terms[]|select(.type=="set" or .type=="array").value[].value]|join(",")' "$f")]" ;;
	*) meaning="OTHER (inspect)" ;;
	esac
	printf '%-8s %-15s %-34s %s\n' "$1" "$2" "$(echo "$resp" | cut -c1-33)" "$meaning" >>"$OUT"
}
row ALICE  ORDERS_PUBLIC
row ALICE  CUSTOMERS
row ALICE  ORDERS_SECRET
row BOB    ORDERS_PUBLIC
row CAROL  ORDERS_PUBLIC
row BOB    CUSTOMERS
row NOBODY ORDERS_PUBLIC

{
	echo
	echo "-- the two DENY cases in full, so there is no doubt they are empty --"
	printf '  ALICE  / ORDERS_SECRET : '; jq -c '.result' "$SPIKE_DIR/compiled/tbl-alice-orders_secret.json"
	printf '  NOBODY / ORDERS_PUBLIC : '; jq -c '.result' "$SPIKE_DIR/compiled/tbl-nobody-orders_public.json"
	echo
	echo "-- ALICE / ORDERS_PUBLIC residual, the row conditions that survived --"
	"$OPA" eval --partial --format=pretty --unknowns 'input.row' \
		--data "$SPIKE_DIR/policies-tables/gate.rego" --data "$SPIKE_DIR/policies-tables/data.json" \
		--input <(echo '{"user":"ALICE","table":"ORDERS_PUBLIC"}') 'data.gate.allow' 2>/dev/null |
		sed 's/│//g' | sed 's/^/  /'
	echo
	echo "-- CONTROL: is DENY really different from a FILTER that matches nothing? --"
	echo "   CAROL / ORDERS_PUBLIC is a FILTER on region APAC. The fixture has no APAC"
	echo "   rows, so it returns 0 rows -- but the ANSWER is a filter, not a denial."
	echo "   ALICE / ORDERS_SECRET is {}: there is no filter to apply at all, because"
	echo "   the table itself is not granted."
	echo "   Both end in no data, and the adapter must still treat them differently:"
	echo "   an empty result set vs a refused query. Executed in evidence/09 as"
	echo "   tbl-carol-orders_public (FILTER, 0 rows) vs tbl-alice-orders_secret (DENY ALL)."
	echo
	echo "-- latency, 100 calls (ALICE / ORDERS_PUBLIC) --"
} >>"$OUT"
: >/tmp/opa_gate.txt
for _ in $(seq 1 100); do
	curl -sS -o /dev/null -w '%{time_total}\n' -X POST "http://127.0.0.1:$PORT/v1/compile" \
		-H 'Content-Type: application/json' \
		-d '{"query":"data.gate.allow == true","input":{"user":"ALICE","table":"ORDERS_PUBLIC"},"unknowns":["input.row"]}' >>/tmp/opa_gate.txt
done
python3 -c "
import statistics
v=sorted(float(l)*1000 for l in open('/tmp/opa_gate.txt') if l.strip())
print(f'  median = {statistics.median(v):.3f} ms   p95 = {v[int(len(v)*0.95)]:.3f} ms   max = {v[-1]:.3f} ms')
" >>"$OUT"
kill $PID 2>/dev/null || true
echo "wrote $OUT"
