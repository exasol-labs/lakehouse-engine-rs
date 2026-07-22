# Decisions: fix-namespace-noniceberg-table-skip

## ADR: Skip a per-table load only on HTTP 404; abort on every other error

**ID:** namespace-skip-non-iceberg-on-404-only
**Plan:** `fix-namespace-noniceberg-table-skip`
**Status:** Accepted

### Context

`createVirtualSchema` lists every table in a configured namespace, then resolves each
table's schema via a per-table `loadTable`. A mixed Iceberg/Hive namespace (a real AWS
Glue estate) contains non-Iceberg tables whose `loadTable` returns HTTP 404
(`NoSuchIcebergTableException`). The adapter propagated that error and aborted the whole
`createVirtualSchema` call, making one Hive table block every Iceberg table in the
namespace (#138). `UdfError` is SDK-owned and carries only opaque `String` variants — no
structured status field — and the single catalog error site already flattens the status
into the message as `catalog returned HTTP 404: …`.

### Decision

During `createVirtualSchema` enumeration, a per-table `loadTable` returning HTTP 404
causes that table to be skipped with a warning; any other failure (transport error,
non-404 HTTP status) aborts the whole request. A classifier, `is_table_not_found`,
detects the 404 case by matching the code-authored `catalog returned HTTP 404` prefix
in the `UdfError::User` message via `starts_with` (not `contains`), so a "404" appearing
inside a redacted non-404 body cannot false-match. The `catalog returned HTTP 404: …`
format is a load-bearing contract, pinned by a unit test at the catalog error site.

### Options Considered

| Option | Verdict |
|--------|---------|
| Skip only on HTTP 404, matched via a controlled message prefix | ✓ Chosen — 404 is the Iceberg REST OpenAPI `NoSuchTableException` signal and the exact status Glue returns for a non-Iceberg table; keys on a code-controlled prefix, not arbitrary catalog body text |
| Skip on any per-table error | ✗ Rejected — masks auth, throttling, and outage faults behind a silent partial schema |
| Thread a structured `u16` status through `authed_get_json` / `resolve_table_schema` | ✗ Rejected — disproportionate for a bug fix; would also perturb the `/v1/config` caller and credential-redaction tests |

### Consequences

A mixed Iceberg/Hive namespace now yields a virtual schema of its Iceberg tables instead
of failing entirely. Genuine catalog faults (auth, throttling, outage) still abort loudly.
The discriminator depends on the exact wording of the catalog error site's message, so
that site's format is now pinned by a contract test rather than free to change silently.

## ADR: No table-filter property in this bug fix

**ID:** namespace-skip-no-table-filter-property
**Plan:** `fix-namespace-noniceberg-table-skip`
**Status:** Accepted

### Context

Beyond skip-and-warn, an explicit include/exclude table-filter property
(`TABLE_INCLUDE`/`TABLE_EXCLUDE`) was considered as a way to let operators curate which
Iceberg tables a namespace exposes.

### Decision

Do not add a table-filter property in this fix. Skip-and-warn on HTTP 404 is the whole
fix for issue #138.

### Options Considered

| Option | Verdict |
|--------|---------|
| Skip-and-warn only, no new property | ✓ Chosen — fully resolves the reported failure with no new configuration surface |
| Add `TABLE_INCLUDE`/`TABLE_EXCLUDE` properties, or both filter + skip | ✗ Rejected — a separable enhancement that also touches property persistence and adapterNotes; out of scope for a bug fix |

### Consequences

The fix stays minimal and scoped to #138. A table-filter property remains available as
a follow-up issue if operators later want to curate which Iceberg tables are exposed.

## ADR: Classifier keys on a code-authored message prefix, not a structured status

**ID:** namespace-skip-classifier-message-prefix
**Plan:** `fix-namespace-noniceberg-table-skip`
**Status:** Accepted

### Context

The plan initially assumed `is_table_not_found(err: &UdfError)` could read a structured
HTTP status off `UdfError`. Plan review found `UdfError` (SDK-owned,
`exasol-udf-sdk-0.20.3`) has only opaque `String` variants — no status field — so a
status-keyed classifier against `&UdfError` was unachievable as originally stated, and
contradicted the plan's own stated prohibition against string matching.

### Decision

`is_table_not_found` matches the code-authored, deterministic `catalog returned HTTP 404`
prefix via `starts_with`, since the single 404 site (`credentials.rs:425`) already
flattens the status into `UdfError::User("catalog returned HTTP 404: …")`, and
`redact_error` preserves that prefix (it strips only secret/credential substrings). No
`credentials.rs` change is required.

### Options Considered

| Option | Verdict |
|--------|---------|
| Match the code-authored `catalog returned HTTP 404` prefix via `starts_with` | ✓ Chosen — narrow, pinned by a unit test, keys on our own emitted status prefix rather than arbitrary catalog body text; needs no new error type |
| Introduce a crate-local `u16`-status error type threaded through `authed_get_json` → `resolve_table_schema` | ✗ Rejected — disproportionate for a bug fix; perturbs the `/v1/config` caller and credential-redaction tests |

### Consequences

The `catalog returned HTTP 404: …` prefix at `credentials.rs:425` becomes a load-bearing
contract, pinned by `catalog_error_message_uses_http_status_prefix`. Any future change to
that message's wording MUST preserve the prefix or update the classifier and its test in
lockstep.
