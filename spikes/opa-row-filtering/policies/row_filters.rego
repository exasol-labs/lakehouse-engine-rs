# Row-filter policy in the exact shape Trino's trino-opa plugin expects.
#
# Contract (plugin/trino-opa .../schema/OpaRowFiltersQueryResult.java):
#   response  = {"result": [ {"expression": "<sql>", "identity": "<user>"?}, ... ]}
# Each expression behaves as an additional WHERE clause; multiple are ANDed.
#
# Each table below deliberately exercises a DIFFERENT filter shape so the
# captured transcripts can be fed one-by-one into the translation analysis.
package trino

import rego.v1

table := input.action.resource.table

tid := sprintf("%s.%s.%s", [table.catalogName, table.schemaName, table.tableName])

# ---------------------------------------------------------------------------
# Attribute source. NOTE: this is the policy's OWN data, keyed by user name.
# It is what lets a bare user name be enough -- see README item 3.
# ---------------------------------------------------------------------------
user_region := {
	"alice": "EU",
	"bob": "US",
}

user_group := {
	"alice": {"analyst", "eu_staff"},
	"bob": {"analyst"},
	"root": {"admin"},
}

default is_admin := false

is_admin if "admin" in user_group[input.context.identity.user]

# Shape A: simple equality against a policy-resolved constant.
rowFilters contains {"expression": sprintf("region = '%s'", [user_region[input.context.identity.user]])} if {
	tid == "lakehouse.sales.orders"
	not is_admin
}

# Shape B: two filters for one table -> plugin ANDs them.
rowFilters contains {"expression": "region = 'EU'"} if {
	tid == "lakehouse.sales.orders_multi"
	not is_admin
}

rowFilters contains {"expression": "o_totalprice < 100000"} if {
	tid == "lakehouse.sales.orders_multi"
	not is_admin
}

# Shape C: IN list.
rowFilters contains {"expression": "region IN ('EU', 'UK', 'CH')"} if {
	tid == "lakehouse.sales.orders_in"
	not is_admin
}

# Shape D: three-valued-logic handling. A bare `= 'PUBLIC'` would drop NULL
# rows; a real policy has to say what happens to unclassified rows.
rowFilters contains {"expression": "(classification IS NULL OR classification <> 'SECRET')"} if {
	tid == "lakehouse.sales.orders_null"
	not is_admin
}

# Shape E: typed date literal + interval arithmetic (Trino dialect).
rowFilters contains {"expression": "o_orderdate >= DATE '2024-01-01' AND o_orderdate < current_date - INTERVAL '7' DAY"} if {
	tid == "lakehouse.sales.orders_date"
	not is_admin
}

# Shape F: correlated subquery against a mapping table. This is the shape
# real-world "entitlement table" policies use.
rowFilters contains {"expression": "region IN (SELECT region FROM security.acl.user_region WHERE user_name = CURRENT_USER)"} if {
	tid == "lakehouse.sales.orders_subquery"
	not is_admin
}

# Shape G: engine-specific scalar function, no DataFusion equivalent by name.
rowFilters contains {"expression": "regexp_like(o_comment, '^(?i)eu-')"} if {
	tid == "lakehouse.sales.orders_func"
	not is_admin
}

# Shape H: session/identity function referenced inside the expression rather
# than resolved by the policy.
rowFilters contains {"expression": "owner = CURRENT_USER"} if {
	tid == "lakehouse.sales.orders_session"
	not is_admin
}

# Shape I: filter evaluated as a DIFFERENT identity (the `identity` field).
rowFilters contains {
	"expression": "dept = 'FINANCE'",
	"identity": "audit_svc",
} if {
	tid == "lakehouse.sales.orders_asidentity"
	not is_admin
}
