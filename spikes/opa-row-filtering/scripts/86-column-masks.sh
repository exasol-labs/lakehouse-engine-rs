#!/usr/bin/env bash
# Column masks over the SAME Compile API call that produces the row filter.
# Probes: does `result.masks` ride along with `result.query`; is `replace` the
# only mask function; what happens on a mask function, mask value, table key or
# mask_rule name the adapter did not expect; and do the METADATA `unknowns` /
# `mask_rule` annotations remove those fields from the request.
source "$(dirname "$0")/lib.sh"
ensure_opa
OUT="$EVIDENCE/17-column-masks.txt"
PORT=8189
pkill -f "opa run --server --addr 127.0.0.1:$PORT" 2>/dev/null || true
setsid "$OPA" run --server --addr "127.0.0.1:$PORT" --log-level error \
	"$SPIKE_DIR/policies-masks" >/tmp/opa_masks.log 2>&1 </dev/null &
disown
for _ in $(seq 1 30); do curl -sf "http://127.0.0.1:$PORT/health" >/dev/null 2>&1 && break; done

# ask <policy-path> <accept-suffix> <body-json>
ask() {
	curl -sS -X POST "http://127.0.0.1:$PORT/v1/compile/$1" \
		-H 'Content-Type: application/json' -H "Accept: application/vnd.opa.$2+json" \
		-d "$3"
}
body() { jq -nc --arg u "$1" '{input:{user:$u}}'; }         # no unknowns: METADATA supplies them

{
echo "### OPA $("$OPA" version | sed -n '1s/Version: //p'), open-source build"
echo "### policies-masks/{masks,maskmissing,maskopt}.rego"
echo
echo "=============================================================="
echo "1. ONE CALL, BOTH ANSWERS"
echo "=============================================================="
printf '  ALICE (filter + mask)  -> '; ask masks/include ucast.all "$(body ALICE)" | tr -d '\n'; echo
cat <<'NOTE'
    `result.query` and `result.masks` come back in the SAME response to the
    SAME call. No second endpoint, so no OPA_..._MASKING_URI property.
NOTE
echo
echo "=============================================================="
echo "2. MASKS ACROSS THE THREE TRUTH VALUES"
echo "=============================================================="
for u in ALICE ROOT CAROL; do
	printf '  %-9s -> ' "$u"; ask masks/include ucast.all "$(body $u)" | tr -d '\n'; echo
done
cat <<'NOTE'
    ALICE  FILTER    -> query + masks
    ROOT   ALLOW ALL -> query {} + masks; `"region": {}` is the documented
                        "no mask" form and survives verbatim
    CAROL  DENY      -> {} : her mask rule IS defined and is NOT returned.
                        Nothing to apply masks to, so the adapter refuses.
NOTE
echo
echo "=============================================================="
echo "3. METADATA vs REQUEST BODY"
echo "=============================================================="
printf '  masks/include,  body {input} only, METADATA unknowns+mask_rule -> '
ask masks/include ucast.all "$(body ALICE)" | jq -c 'if .result then {query:.result.query,masks:.result.masks} else . end' | tr -d '\n'; echo
printf '  maskopt/include, no METADATA, no unknowns, no maskRule         -> '
ask maskopt/include ucast.all '{"input":{}}' | tr -d '\n'; echo
printf '  maskopt/include, request unknowns + options.maskRule           -> '
ask maskopt/include ucast.all '{"input":{},"unknowns":["input.row"],"options":{"maskRule":"data.maskopt.column_masks"}}' | tr -d '\n'; echo
cat <<'NOTE'
    `unknowns` and the mask rule can live EITHER in the policy's METADATA or in
    the request body. With the annotation the request carries only `input`, so
    the adapter has no unknowns list and no rule name to get wrong -- and
    omitting both from an unannotated policy still DENIES (fails closed).
NOTE
echo
echo "=============================================================="
echo "4. THE MASK CONTRACT ITSELF"
echo "=============================================================="
printf '  TYPED     replace.value = 0 (a number)      -> '; ask masks/include ucast.all "$(body TYPED)" | tr -d '\n'; echo
printf '  WEIRD     {"hash": {...}} (unsupported fn)  -> '; ask masks/include ucast.all "$(body WEIRD)" | tr -d '\n'; echo
printf '  OTHERTBL  mask on a table not in unknowns   -> '; ask masks/include ucast.all "$(body OTHERTBL)" | tr -d '\n'; echo
printf '  maskmissing  mask_rule names a missing rule -> '; ask maskmissing/include ucast.all '{"input":{}}' | tr -d '\n'; echo
echo
echo "=============================================================="
echo "4b. MISCONFIGURING THE MASK RULE"
echo "=============================================================="
printf '  request maskRule -> MISSING rule    -> '; ask maskopt/include ucast.all '{"input":{},"unknowns":["input.row"],"options":{"maskRule":"data.maskopt.nope"}}' | tr -d '\n'; echo
printf '  request maskRule -> bogus package   -> '; ask maskopt/include ucast.all '{"input":{},"unknowns":["input.row"],"options":{"maskRule":"data.nosuchpkg.masks"}}' | tr -d '\n'; echo
printf '  request maskRule -> NON-OBJECT rule -> '; ask maskopt/include ucast.all '{"input":{},"unknowns":["input.row"],"options":{"maskRule":"data.maskopt.column_masks_bad"}}' | tr -d '\n' | head -c 160; echo
printf '  request maskRule -> no `data.` ref  -> '; ask maskopt/include ucast.all '{"input":{},"unknowns":["input.row"],"options":{"maskRule":"column_masks"}}' | tr -d '\n'; echo
cat <<'NOTE'
    A mask rule that does not exist -- by METADATA (`maskmissing` above) or by
    request option -- answers HTTP 200 with the filter and NO `masks` key and NO
    error. The row filter still applies; the masking silently does not. This is
    the one half of the contract that does NOT fail closed, and it is
    indistinguishable from a policy that simply masks nothing.
    A mask rule whose value is not the table->column->fn object is the loud
    case: HTTP 500 `convert masks`.
NOTE
echo
echo "=============================================================="
echo "5. DO THE SQL TARGETS CARRY MASKS TOO?"
echo "=============================================================="
for a in sql.postgresql sql.sqlite multitarget; do
	printf '  %-16s -> ' "$a"; ask masks/include "$a" "$(body ALICE)" | head -c 220 | tr -d '\n'; echo
done
cat <<'NOTE'
    Masks ride on the SQL targets too, unchanged -- they are target-independent.
    `multitarget` answers `{"result":{}}` without `options.targetDialects`.
NOTE
echo
cat <<'NOTE'
-- conclusion --
Masking needs NO configuration surface: `result.masks` rides on the same
response as `result.query`, so there is no per-column request to batch and no
second URI to configure. What it needs is an ADAPTER-SIDE allowlist, because
OPA validates none of it: an unsupported mask function, a mask on a table that
is not the unknown row, and a mask value of any JSON type all pass through
verbatim with HTTP 200. And a mask_rule that does not exist returns the filter
with no masks and no error, so the adapter cannot tell "no masks" from
"misconfigured masks" -- the masking half fails OPEN.
NOTE
} >"$OUT"
pkill -f "opa run --server --addr 127.0.0.1:$PORT" 2>/dev/null || true
echo "wrote $OUT"
