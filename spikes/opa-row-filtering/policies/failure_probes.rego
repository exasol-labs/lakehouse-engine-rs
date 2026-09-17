# Deliberately broken / degenerate rules, used only by 30-failure-modes.sh to
# capture how OPA reports each class of failure over the wire.
package spike.failure

import rego.v1

# A rule whose body raises a builtin error (division by zero).
rowFilters contains {"expression": sprintf("x = %d", [1 / input.zero])} if {
	input.action.operation == "GetRowFilters"
}

# A rule that returns a non-string expression -- the Java codec expects String.
badTypeRowFilters contains {"expression": 42}

# A rule that tries to read the table schema from a GetRowFilters request.
# The request has no `columns` field, so this is always undefined.
schemaProbe := input.action.resource.table.columns
