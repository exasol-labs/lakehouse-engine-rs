# No METADATA at all: probes request-side options.maskRule and request-side unknowns.
package maskopt

import rego.v1

default include := false

include if input.row.region == "EU"

column_masks := {"row": {"o_comment": {"replace": {"value": "<redacted>"}}}}

# A mask rule whose value is not the documented table->column->fn object.
column_masks_bad := "not-an-object"
