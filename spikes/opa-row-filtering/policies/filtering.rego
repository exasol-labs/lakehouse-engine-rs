# Data-filtering style policy: the ROW is unknown. OPA partial-evaluates the
# policy against everything it DOES know (the user, the role map) and returns
# the residual conditions on the unknown row.
#
# Nothing here is SQL. The policy talks about row fields; the residual is an
# expression tree that a consumer compiles into its own dialect.
package filtering

import rego.v1

user_region := {"alice": "EU", "bob": "US"}

user_roles := {
	"alice": {"analyst", "eu_staff"},
	"bob": {"analyst"},
	"root": {"admin"},
}

default allow := false

# An admin sees every row: no residual condition at all.
allow if "admin" in user_roles[input.user]

# Everyone else is confined to their own region, and cannot see SECRET rows.
allow if {
	not "admin" in user_roles[input.user]
	input.row.region == user_region[input.user]
	input.row.classification != "SECRET"
}

# A second, disjunctive grant: eu_staff may also see UK and CH rows.
allow if {
	"eu_staff" in user_roles[input.user]
	input.row.region in {"UK", "CH"}
	input.row.o_totalprice < 100000
}
