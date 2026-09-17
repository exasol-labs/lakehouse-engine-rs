#!/usr/bin/env bash
# Item 6: cost of the plan-time OPA call.
#
# Two things are measured:
#   1. round-trip latency of ONE GetRowFilters call (loopback, OPA in-process
#      policy, no bundle fetch) -- the floor for a co-located sidecar.
#   2. N sequential row-filter calls for an N-table query, because trino-opa has
#      NO batch row-filter endpoint: OpaConfig.java declares opaBatchUri,
#      opaColumnMaskingUri and opaBatchColumnMaskingUri, but the row-filter URI
#      (`opa.policy.row-filters-uri`) has no batch counterpart, and
#      OpaAccessControl.getRowFilters(context, tableName) takes ONE table.
source "$(dirname "$0")/lib.sh"
opa_up

OUT="$EVIDENCE/04-latency.txt"
N="${N:-300}"

cat >"$BIN_DIR/req.json" <<'JSON'
{"input":{"context":{"identity":{"user":"alice","groups":["analyst","eu_staff"]},"softwareStack":{"trinoVersion":"476"}},
"action":{"operation":"GetRowFilters","resource":{"table":{"catalogName":"lakehouse","schemaName":"sales","tableName":"orders"}}}}}
JSON

{
	echo "### host: $(uname -srm)"
	echo "### OPA:  $("$OPA" version | head -1)  (loopback, --server, policies from disk)"
	echo "### N  =  $N sequential POSTs to /v1/data/trino/rowFilters"
	echo
} >"$OUT"

# Warm up.
for _ in $(seq 1 20); do post trino/rowFilters "$BIN_DIR/req.json" >/dev/null; done

: >"$BIN_DIR/times.txt"
for _ in $(seq 1 "$N"); do
	curl -sS -o /dev/null -w '%{time_total}\n' -X POST "$OPA_URL/v1/data/trino/rowFilters" \
		-H 'Content-Type: application/json' --data-binary "@$BIN_DIR/req.json" >>"$BIN_DIR/times.txt"
done

python3 - "$BIN_DIR/times.txt" >>"$OUT" <<'PY'
import statistics, sys
v = sorted(float(l) * 1000 for l in open(sys.argv[1]) if l.strip())
def p(q): return v[min(len(v) - 1, int(len(v) * q))]
print("single-call round trip, ms (includes one fresh curl process per call):")
print(f"  n      = {len(v)}")
print(f"  min    = {v[0]:.3f}")
print(f"  median = {statistics.median(v):.3f}")
print(f"  p95    = {p(0.95):.3f}")
print(f"  p99    = {p(0.99):.3f}")
print(f"  max    = {v[-1]:.3f}")
print(f"  mean   = {statistics.fmean(v):.3f}")
PY

# Per-process curl overhead is a large share of the above; measure it away by
# doing all N in ONE curl invocation with keep-alive.
{
	echo
	echo "same N calls in ONE curl process (HTTP keep-alive, no per-call fork):"
} >>"$OUT"
args=()
for _ in $(seq 1 "$N"); do
	args+=(--next -sS -o /dev/null -X POST "$OPA_URL/v1/data/trino/rowFilters"
		-H 'Content-Type: application/json' --data-binary "@$BIN_DIR/req.json" -w '%{time_total}\n')
done
curl "${args[@]:1}" >"$BIN_DIR/times2.txt" 2>/dev/null
python3 - "$BIN_DIR/times2.txt" >>"$OUT" <<'PY'
import statistics, sys
v = sorted(float(l) * 1000 for l in open(sys.argv[1]) if l.strip())
def p(q): return v[min(len(v) - 1, int(len(v) * q))]
print(f"  n      = {len(v)}")
print(f"  min    = {v[0]:.3f}")
print(f"  median = {statistics.median(v):.3f}")
print(f"  p95    = {p(0.95):.3f}")
print(f"  p99    = {p(0.99):.3f}")
print(f"  max    = {v[-1]:.3f}")
print()
for k in (1, 2, 5, 10, 20):
    print(f"  projected plan-time cost for a {k:>2}-table query "
          f"({k} sequential row-filter calls): {statistics.median(v) * k:.2f} ms")
PY

echo "wrote $OUT"
