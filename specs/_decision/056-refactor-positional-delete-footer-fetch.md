# Decisions: refactor-positional-delete-footer-fetch

## ADR: Deadlock freedom rests on no-hold-and-wait, not on phase ordering

**ID:** footer-fetch-no-hold-and-wait
**Plan:** refactor-positional-delete-footer-fetch
**Status:** Accepted

### Context

Within one provider, Phase A (delete-file reads) drops its permits before Phase B (data-file footer fetches) starts. A broadcast join runs two providers concurrently, so one provider's Phase A permits and the other's Phase B permits coexist on the one shared semaphore.

### Decision

Every fan-out task in both phases acquires one permit, holds it across one object-store read, and releases it. No task holds a permit while awaiting another permit, and no task awaits another task.

### Options Considered

| Option | Verdict |
|--------|---------|
| Rely on phase ordering | Rejected: true only within one provider, and a broadcast join makes the phases coexist |

### Consequences

Implementation and review check the no-hold-and-wait property directly. Any future fan-out sharing the semaphore keeps the one-permit, one-read, no-nesting shape.
