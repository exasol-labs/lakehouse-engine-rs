#!/usr/bin/env bash
# Capture real GetColumnMask pairs, both the per-column and the batch shape.
# Unlike GetRowFilters, these requests DO carry the column name and its type
# (TrinoColumn.java: catalogName, schemaName, tableName, columnName, columnType).
source "$(dirname "$0")/lib.sh"
opa_up

OUT="$EVIDENCE/02-column-masks.txt"
: >"$OUT"
echo "### endpoint A: POST $OPA_URL/v1/data/trino/columnMask      (one request per column)" >>"$OUT"
echo "### endpoint B: POST $OPA_URL/v1/data/trino/batchColumnMasks (one request per table)" >>"$OUT"
echo >>"$OUT"

ctx='{"identity":{"user":"alice","groups":["analyst"]},"softwareStack":{"trinoVersion":"476"},"queryId":"20260917_101500_00001_abcde"}'

for pair in c_phone:VARCHAR c_name:VARCHAR c_acctbal:"DECIMAL(15,2)" c_address:VARCHAR c_custkey:BIGINT; do
	IFS=: read -r colname coltype <<<"$pair"
	cat >"$BIN_DIR/req.json" <<JSON
{"input":{"context":$ctx,"action":{"operation":"GetColumnMask","resource":{"column":{"catalogName":"lakehouse","schemaName":"sales","tableName":"customer","columnName":"$colname","columnType":"$coltype"}}}}}
JSON
	{
		echo "=============================================================="
		echo "CASE (per-column) column=$colname type=$coltype"
		echo "-------- REQUEST ---------------------------------------------"
		jq -c '.input.action' "$BIN_DIR/req.json"
		echo "-------- RESPONSE --------------------------------------------"
		post trino/columnMask "$BIN_DIR/req.json" | jq -c .
		echo
	} >>"$OUT"
done

cat >"$BIN_DIR/req.json" <<JSON
{"input":{"context":$ctx,"action":{"operation":"GetColumnMask","filterResources":[
 {"column":{"catalogName":"lakehouse","schemaName":"sales","tableName":"customer","columnName":"c_phone","columnType":"VARCHAR"}},
 {"column":{"catalogName":"lakehouse","schemaName":"sales","tableName":"customer","columnName":"c_name","columnType":"VARCHAR"}},
 {"column":{"catalogName":"lakehouse","schemaName":"sales","tableName":"customer","columnName":"c_custkey","columnType":"BIGINT"}},
 {"column":{"catalogName":"lakehouse","schemaName":"sales","tableName":"customer","columnName":"c_address","columnType":"VARCHAR"}}
]}}}
JSON
{
	echo "=============================================================="
	echo "CASE (batch) 4 columns in ONE request"
	echo "-------- REQUEST ---------------------------------------------"
	jq -c '.input.action' "$BIN_DIR/req.json"
	echo "-------- RESPONSE --------------------------------------------"
	post trino/batchColumnMasks "$BIN_DIR/req.json" | jq .
} >>"$OUT"

echo "wrote $OUT"
