# Decisions: refactor-positional-delete-footer-fetch

## ADR: Deadlock freedom rests on no-hold-and-wait, not on phase ordering

**ID:** footer-fetch-no-hold-and-wait
**Plan:** refactor-positional-delete-footer-fetch
**Status:** Accepted

### Context

Delete-file reads (Phase A) and data-file footer fetches (Phase B) share one semaphore. A broadcast join runs two providers concurrently, so one provider's Phase A and the other's Phase B hold permits at the same time.

### Decision

Every fan-out task in both phases acquires one permit, holds it across one object-store read, and releases it. No task holds a permit while awaiting another permit, and no task awaits another task.

### Options Considered

| Option | Verdict |
|--------|---------|
| Rely on phase ordering | Rejected: ordering holds only within one provider, and a broadcast join overlaps the phases |

### Consequences

- Review checks the no-hold-and-wait property directly.
- Any fan-out sharing the semaphore keeps the one-permit, one-read, no-nesting shape.
