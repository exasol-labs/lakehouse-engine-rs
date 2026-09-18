# Column masks over the Compile API path route: does one call carry both the
# row filter and the masks, and is `replace` really the only function?
package masks

import rego.v1

# METADATA
# scope: document
# description: filter rule; names the mask rule and the unknowns in metadata
# compile:
#   unknowns: ["input.row"]
#   mask_rule: data.masks.column_masks
default include := false

include if {                                   # FILTER + mask
	input.user == "ALICE"
	input.row.region in ["EU", "UK"]
}

include if input.user == "ROOT"                # ALLOW ALL + mask

include if input.user == "TYPED"               # ALLOW ALL, non-string mask value

include if input.user == "WEIRD"               # ALLOW ALL, unsupported mask fn

include if input.user == "OTHERTBL"            # ALLOW ALL, mask on an unknown table

# CAROL is never included -> DENY. Her mask below must not matter.

column_masks := {"row": {"o_comment": {"replace": {"value": "<redacted>"}}}} if input.user == "ALICE"

column_masks := {"row": {"o_comment": {"replace": {"value": "<redacted>"}},
	"region": {}}} if input.user == "ROOT"       # `{}` = documented "no mask"

column_masks := {"row": {"o_totalprice": {"replace": {"value": 0}}}} if input.user == "TYPED"

column_masks := {"row": {"o_comment": {"hash": {"algorithm": "sha256"}}}} if input.user == "WEIRD"

column_masks := {"orders": {"o_comment": {"replace": {"value": "x"}}}} if input.user == "OTHERTBL"

column_masks := {"row": {"o_comment": {"replace": {"value": "<redacted>"}}}} if input.user == "CAROL"
