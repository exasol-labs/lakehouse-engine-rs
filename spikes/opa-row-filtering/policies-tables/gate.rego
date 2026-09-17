# ONE policy answering BOTH questions: may this user touch this table at all,
# and which of its rows may they see. The table gate is not a separate
# mechanism -- it is the degenerate case of the row filter: if no `allow` rule
# can ever hold, partial evaluation returns no queries at all, which is DENY.
package gate

import rego.v1

# Absent user or absent table -> `grant` is undefined -> every rule below is
# undefined -> the Compile API returns {} = DENY. No `default` rule, which is
# what keeps the output a flat `queries` list instead of a support module.
grant := data.permissions[input.user].tables[input.table]

# Table granted with no row restriction -> empty residual = ALLOW ALL.
allow if grant.all_rows

# Table granted with a row restriction -> residual = the row conditions.
allow if {
	not grant.all_rows
	input.row.region in grant.regions
}
