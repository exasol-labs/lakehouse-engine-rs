#!/usr/bin/env bash
# The simplest option: permissions defined PER USER as plain OPA data.
# No groups in the request, no http.send, no IdP call at decision time.
# In production the JSON arrives as an OPA bundle, which OPA polls and caches.
source "$(dirname "$0")/lib.sh"
ensure_opa
OUT="$EVIDENCE/11-per-user-data.txt"
PORT=8184
pkill -f "opa run --server --addr 127.0.0.1:$PORT" 2>/dev/null || true
"$OPA" run --server --addr "127.0.0.1:$PORT" --log-level error "$SPIKE_DIR/policies-data" >/dev/null 2>&1 &
PID=$!
sleep 2
{
	echo "### policy: policies-data/perms.rego      data: policies-data/data.json"
	echo "### the whole permission model:"
	jq . "$SPIKE_DIR/policies-data/data.json" | sed 's/^/###   /'
	echo
	echo "=============================================================="
	echo "Partial evaluation resolves the data document completely, because data is KNOWN."
	echo "=============================================================="
} >"$OUT"
for u in ALICE BOB ROOT NOBODY; do
	f="$SPIKE_DIR/compiled/data-$(echo "$u" | tr 'A-Z' 'a-z').json"
	curl -sS -X POST "http://127.0.0.1:$PORT/v1/compile" -H 'Content-Type: application/json' \
		-d "{\"query\":\"data.perms.allow == true\",\"input\":{\"user\":\"$u\"},\"unknowns\":[\"input.row\"]}" >"$f"
	{
		printf '  %-8s ' "$u"
		"$OPA" eval --partial --format=pretty --unknowns 'input.row' \
			--data "$SPIKE_DIR/policies-data/perms.rego" --data "$SPIKE_DIR/policies-data/data.json" \
			--input <(echo "{\"user\":\"$u\"}") 'data.perms.allow' 2>/dev/null |
			{ grep -E 'input\.row|undefined' || echo '(empty residual = ALLOW ALL)'; } |
			sed 's/│//g' | tr -s ' ' | tr '\n' ';'
		jq -cr 'if .result.queries then " [\(.result.queries|length) query]" else " [no queries = DENY]" end' "$f"
	} >>"$OUT"
done
{
	echo
	echo "  ROOT has unrestricted=true, so its residual is empty = ALLOW ALL."
	echo "  NOBODY is absent from the data, so the policy is undefined = DENY."
	echo
	echo "-- NEGATIVE CONTROL: no http.send anywhere in the residual --"
	printf '  occurrences of http.send in the compiled residual: '
	grep -co "http.send" "$SPIKE_DIR/compiled/data-alice.json" 2>/dev/null || echo 0
	echo
	echo "-- latency, 100 calls --"
} >>"$OUT"
: >/tmp/opa_data.txt
for _ in $(seq 1 100); do
	curl -sS -o /dev/null -w '%{time_total}\n' -X POST "http://127.0.0.1:$PORT/v1/compile" \
		-H 'Content-Type: application/json' \
		-d '{"query":"data.perms.allow == true","input":{"user":"ALICE"},"unknowns":["input.row"]}' >>/tmp/opa_data.txt
done
python3 -c "
import statistics
v=sorted(float(l)*1000 for l in open('/tmp/opa_data.txt') if l.strip())
print(f'  median = {statistics.median(v):.3f} ms   p95 = {v[int(len(v)*0.95)]:.3f} ms   max = {v[-1]:.3f} ms')
" >>"$OUT"
kill $PID 2>/dev/null || true
echo "wrote $OUT"
