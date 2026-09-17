# Column-mask policies, both the per-column and the batch shape.
#   per-column (OpaColumnMaskQueryResult.java): {"result": {"expression": "...", "identity"?: "..."}}
#                                               absent rule -> {} -> Optional.empty -> no mask
#   batch      (OpaBatchColumnMaskQueryResult.java): {"result": [{"index": i, "viewExpression": {...}}]}
package trino

import rego.v1

# `is_admin` is defined once, in row_filters.rego (same `trino` package).

# ---- per-column endpoint: input.action.resource.column ----------------------
col := input.action.resource.column

# Mask 1: full suppression.
columnMask := {"expression": "NULL"} if {
	not is_admin
	col.tableName == "customer"
	col.columnName == "c_phone"
}

# Mask 2: partial reveal via string functions + concatenation.
columnMask := {"expression": "'****' || substring(c_name, -3)"} if {
	not is_admin
	col.tableName == "customer"
	col.columnName == "c_name"
}

# Mask 3: conditional mask -- the mask itself is row-dependent.
columnMask := {"expression": "CASE WHEN c_nationkey = 3 THEN c_acctbal ELSE NULL END"} if {
	not is_admin
	col.tableName == "customer"
	col.columnName == "c_acctbal"
}

# Mask 4: hashing (a very common real policy) -- Trino built-in.
columnMask := {"expression": "to_hex(sha256(to_utf8(c_address)))"} if {
	not is_admin
	col.tableName == "customer"
	col.columnName == "c_address"
}

# ---- batch endpoint: input.action.filterResources[i].column -----------------
batchColumnMasks contains {
	"index": i,
	"viewExpression": {"expression": "NULL"},
} if {
	not is_admin
	some i
	c := input.action.filterResources[i].column
	c.tableName == "customer"
	c.columnName == "c_phone"
}

batchColumnMasks contains {
	"index": i,
	"viewExpression": {"expression": "'****' || substring(c_name, -3)"},
} if {
	not is_admin
	some i
	c := input.action.filterResources[i].column
	c.tableName == "customer"
	c.columnName == "c_name"
}

batchColumnMasks contains {
	"index": i,
	"viewExpression": {"expression": "to_hex(sha256(to_utf8(c_address)))"},
} if {
	not is_admin
	some i
	c := input.action.filterResources[i].column
	c.tableName == "customer"
	c.columnName == "c_address"
}
