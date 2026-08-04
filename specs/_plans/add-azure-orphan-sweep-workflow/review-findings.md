# Code Review Findings: add-azure-orphan-sweep-workflow

## Summary
- Files reviewed: 1 (`.github/workflows/azure-orphan-sweep.yml`)
- Total findings: 0 (standard: 0, expert: 0)

Reviewed against the full `/speq:code-review` taxonomy and the brief's five focus
points. All cleared:

- **`set -euo pipefail` (line 35)** — `containers=$(az ...)` propagates a non-zero
  `az` exit under `-e`; the `<<< "$containers"` here-string keeps the loop in the
  current shell so `candidates` survives; `candidates=$((candidates + 1))` is an
  assignment, not the `((expr))`-returns-0 `set -e` trap; the EOF `read` is a loop
  condition, exempt.
- **No credential leak** — no `set -x`; the secret reaches `az` only as a quoted
  argument; the three secrets are `secrets.*` so GitHub masks them anyway; `az login`
  default output carries no secret or token; the precondition echoes only `$var`
  (the name). Satisfies scenarios 6 and 8.
- **Empty container list can't fail the step** — `[ -n "$containers" ]` (line 94)
  gates the loop; command substitution strips a lone trailing newline. Satisfies
  scenario 3.
- **DRY_RUN string comparison** — the trigger expression resolves to the string
  `"true"`/`"false"` in all three paths (dispatch+true, dispatch+false, schedule),
  and `[ "$DRY_RUN" = "true" ]` is the documented-safe handling of the boolean-input
  gotcha.
- **Precondition never echoes a value** — `${!var+set}` detects unset without
  tripping `set -u`, and `${!var}` runs only in the `elif` (set) branch; only the
  variable name is printed.

The explanatory comments convey non-obvious bash/Actions rationale (capture-not-pipe,
string-compare, no `--fail-not-exist`), i.e. design intent rather than redundant
"what" comments, so they are not comment-quality defects. The inline-in-YAML design
and the un-unit-tested bash are ratified decisions (decision-log [3]) and are not
raised. The positional multiselect-list projection (round-1 fix #3) is correctly
implemented.

## Standard fixes
[none]

## Expert fixes
[none]
