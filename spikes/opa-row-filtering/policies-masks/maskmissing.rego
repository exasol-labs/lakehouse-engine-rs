# mask_rule points at a rule that does not exist: error, or silently no masks?
package maskmissing

import rego.v1

# METADATA
# scope: document
# compile:
#   unknowns: ["input.row"]
#   mask_rule: data.maskmissing.does_not_exist
default include := false

include if input.row.region == "EU"
