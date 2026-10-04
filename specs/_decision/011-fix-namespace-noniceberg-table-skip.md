# Decisions: fix-namespace-noniceberg-table-skip

## ADR: Skip a per-table load only on HTTP 404; abort on every other error

**ID:** namespace-skip-non-iceberg-on-404-only
**Plan:** `fix-namespace-noniceberg-table-skip`
**Status:** Accepted

### Context

A mixed Iceberg/Hive namespace contains non-Iceberg tables whose table load returns HTTP 404, and one such table aborted the whole virtual schema creation. The SDK error type carries only opaque strings, and the catalog error site already writes the status into the message.

### Decision

During virtual schema creation, the adapter skips a table with a warning when its load returns HTTP 404, and aborts on every other failure. It detects the 404 by matching the code-authored message prefix, not by searching the message body. The message format is a contract, pinned by a unit test.

### Options Considered

| Option | Verdict |
|--------|---------|
| Skip on any per-table error | Rejected: masks auth, throttling, and outage faults behind a silent partial schema |
| Thread a structured status through the catalog calls | Rejected: disproportionate for a bug fix and disturbs other callers and redaction tests |

### Consequences

The discriminator depends on the exact wording of the catalog error message, so changing that message requires changing the pinned test.
