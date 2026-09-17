# Probes for the path-form Compile API with a target (Accept header).
package ucastprobe

import rego.v1

# A string literal carrying a quote and a SQL comment, to see what each target
# does with it.
inject if input.row.region == "EU' OR 1=1 -- "

# A builtin outside the UCAST/SQL fragment.
unsupported if startswith(input.row.o_comment, "eu")

# A numeric comparison, to see how each target types the literal.
numeric if input.row.o_totalprice < 100000

# A reference to something that is NOT the unknown row.
crosstable if data.somewhere.else == "x"
