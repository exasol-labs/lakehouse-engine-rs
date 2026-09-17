# Permissions defined PER USER as plain data, with no groups and no HTTP.
# In production this JSON arrives as an OPA bundle (opa run --bundle / a bundle
# server), which OPA polls and caches. It is `data`, so partial evaluation
# resolves it completely.
package perms

import rego.v1

p := data.permissions[input.user]

allow if p.unrestricted

allow if {
	not p.unrestricted
	input.row.region in p.regions
	input.row.classification != "SECRET"
}
