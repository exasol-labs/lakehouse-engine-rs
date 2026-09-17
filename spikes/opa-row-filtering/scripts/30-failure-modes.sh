#!/usr/bin/env bash
# Item 5: capture what the wire actually looks like for each failure class.
source "$(dirname "$0")/lib.sh"
opa_up

OUT="$EVIDENCE/03-failure-modes.txt"
: >"$OUT"

body() {
	cat >"$BIN_DIR/req.json" <<JSON
{"input":{"context":{"identity":{"user":"alice","groups":["analyst"]},"softwareStack":{"trinoVersion":"476"}},
"action":{"operation":"GetRowFilters","resource":{"table":{"catalogName":"lakehouse","schemaName":"sales","tableName":"$1"}}}}}
JSON
}

show() { # show <label> <url>
	echo "==============================================================" >>"$OUT"
	echo "CASE $1" >>"$OUT"
	echo "-- url: $2" >>"$OUT"
	code=$(curl -sS -o "$BIN_DIR/resp.txt" -w '%{http_code}' -X POST "$2" \
		-H 'Content-Type: application/json' --data-binary "@$BIN_DIR/req.json" 2>"$BIN_DIR/err.txt" || echo "CURL_FAIL")
	echo "-- http status: $code" >>"$OUT"
	echo "-- body:" >>"$OUT"
	cat "$BIN_DIR/resp.txt" >>"$OUT" 2>/dev/null
	echo >>"$OUT"
	[[ -s "$BIN_DIR/err.txt" ]] && { echo "-- curl stderr:" >>"$OUT"; cat "$BIN_DIR/err.txt" >>"$OUT"; }
	echo >>"$OUT"
}

body orders

show "A. healthy call, user with a policy (baseline)" "$OPA_URL/v1/data/trino/rowFilters"

body orders
sed -i 's/"alice"/"carol"/' "$BIN_DIR/req.json"
show "B. healthy call, user the policy says NOTHING about (carol)" "$OPA_URL/v1/data/trino/rowFilters"

body orders
show "C. MISTYPED policy path (rule does not exist)" "$OPA_URL/v1/data/trino/rowFiltersTypo"

body orders
show "D. MISTYPED package path (package does not exist)" "$OPA_URL/v1/data/no/such/package/rowFilters"

body orders
show "E. rego runtime error (1/0), default builtin-error handling" "$OPA_URL/v1/data/spike/failure/rowFilters"

body orders
show "F. rego runtime error (1/0), strict-builtin-errors=true" "$OPA_URL/v1/data/spike/failure/rowFilters?strict-builtin-errors=true"

body orders
show "G. policy returns a NON-STRING expression (42)" "$OPA_URL/v1/data/spike/failure/badTypeRowFilters"

body orders
show "H. policy reads resource.table.columns (absent from GetRowFilters)" "$OPA_URL/v1/data/spike/failure/schemaProbe"

body orders
# Deliberately a high, unlikely port: 18181 is the Iceberg REST catalog's port in
# this repo's compose file, so using it made this control hit a real server.
show "I. OPA UNREACHABLE (nothing listening on :45871)" "http://127.0.0.1:45871/v1/data/trino/rowFilters"

echo "wrote $OUT"
