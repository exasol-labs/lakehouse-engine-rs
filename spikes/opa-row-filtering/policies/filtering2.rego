# Same policy as filtering.rego but WITHOUT `default allow := false`, so partial
# evaluation returns a flat set of `queries` (a disjunction of conjunctions)
# instead of a support module. This is the shape a compiler wants.
package filtering2

import rego.v1

user_region := {"alice": "EU", "bob": "US"}

user_roles := {
	"alice": {"analyst", "eu_staff"},
	"bob": {"analyst"},
	"root": {"admin"},
}

allow if "admin" in user_roles[input.user]

allow if {
	not "admin" in user_roles[input.user]
	input.row.region == user_region[input.user]
	input.row.classification != "SECRET"
}

allow if {
	"eu_staff" in user_roles[input.user]
	input.row.region in {"UK", "CH"}
	input.row.o_totalprice < 100000
}
