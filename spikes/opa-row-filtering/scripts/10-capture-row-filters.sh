#!/usr/bin/env bash
# Capture real GetRowFilters request/response pairs.
#
# The request body is byte-for-byte what trino-opa's OpaHighLevelClient
# .getRowFilterExpressionsFromOpa() serializes: {context, action{operation,
# resource{table}}}. TrinoTable is built with the (CatalogSchemaTableName) ctor,
# which leaves `columns` and `properties` NULL, and @JsonInclude(NON_NULL) drops
# them -- so the request carries NO schema. That absence is the finding.
source "$(dirname "$0")/lib.sh"
opa_up

OUT="$EVIDENCE/01-row-filters.txt"
: >"$OUT"

req() { # req <user> <groups-json> <table>
	cat >"$BIN_DIR/req.json" <<JSON
{
  "input": {
    "context": {
      "identity": { "user": "$1", "groups": $2 },
      "softwareStack": { "trinoVersion": "476" },
      "queryId": "20260917_101500_00001_abcde"
    },
    "action": {
      "operation": "GetRowFilters",
      "resource": { "table": { "catalogName": "lakehouse", "schemaName": "sales", "tableName": "$3" } }
    }
  }
}
JSON
}

{
	echo "### OPA $("$OPA" version | head -1)"
	echo "### endpoint: POST $OPA_URL/v1/data/trino/rowFilters"
	echo "### policy:   policies/row_filters.rego"
	echo
} >>"$OUT"

for spec in \
	"alice:[\"analyst\",\"eu_staff\"]:orders" \
	"bob:[\"analyst\"]:orders" \
	"alice:[\"analyst\"]:orders_multi" \
	"alice:[\"analyst\"]:orders_in" \
	"alice:[\"analyst\"]:orders_null" \
	"alice:[\"analyst\"]:orders_date" \
	"alice:[\"analyst\"]:orders_subquery" \
	"alice:[\"analyst\"]:orders_func" \
	"alice:[\"analyst\"]:orders_session" \
	"alice:[\"analyst\"]:orders_asidentity" \
	"root:[\"admin\"]:orders" \
	"carol:[\"analyst\"]:orders"; do
	IFS=: read -r user groups table <<<"$spec"
	req "$user" "$groups" "$table"
	{
		echo "=============================================================="
		echo "CASE user=$user table=lakehouse.sales.$table"
		echo "-------- REQUEST ---------------------------------------------"
		jq -c '.input' "$BIN_DIR/req.json"
		echo "-------- RESPONSE --------------------------------------------"
		post trino/rowFilters "$BIN_DIR/req.json" | jq .
		echo
	} >>"$OUT"
done

echo "wrote $OUT"
