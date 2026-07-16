# Decisions: add-saas-install-script

## ADR: Idempotent SCRIPT_LANGUAGES read-modify-write, never a blind overwrite

**ID:** idempotent-script-languages-read-modify-write-not-blind-overwrite
**Plan:** `add-saas-install-script`
**Status:** Accepted

### Context

`ALTER SYSTEM SET SCRIPT_LANGUAGES` replaces the entire persisted value. A real SaaS
database already registers other languages (e.g. `PYTHON3`, `JAVA`) before the installer
runs. The RUST alias is version-independent — the SLC version lives in the uploaded
tarball content, not in the alias string — so the segment to add is a fixed literal
pointing at the `rustslc` name at the SaaS `uploads/default` bucket path where the SLC
tarball uploads.

### Decision

Read the persisted `SCRIPT_LANGUAGES` system value from `EXA_PARAMETERS` first, then
append the fixed segment
`RUST=localzmq+protobuf:///uploads/default/rustslc?lang=rust#buckets/uploads/default/rustslc/exaudf/exaudfclient`
if no `RUST=` entry exists, or replace the single existing `RUST=` segment in place if one
does, before issuing `ALTER SYSTEM SET`.

### Options Considered

| Option | Verdict |
|--------|---------|
| Read-modify-write: preserve existing languages, append or replace the single RUST segment in place | ✓ Chosen — a real cluster already registers other languages; preserving them and de-duplicating RUST keeps re-runs safe |
| Set a fixed `SCRIPT_LANGUAGES` string | ✗ Rejected — `ALTER SYSTEM SET` replaces the entire value, so a blind write would drop `PYTHON3`/`JAVA`/`R` and break other UDFs |

### Consequences

Re-running the installer against a database with other registered languages, or against
one from a prior installer run, never drops or duplicates a language entry. The
registration step must read the current value before every write, adding one extra
`exapump sql` round-trip per run.
