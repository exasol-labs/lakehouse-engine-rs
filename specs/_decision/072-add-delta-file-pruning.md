# Decisions: add-delta-file-pruning

## ADR: The kernel prunes; the adapter only translates

**ID:** delta-kernel-prunes-adapter-only-translates
**Plan:** add-delta-file-pruning
**Status:** Accepted

### Context

Delta queries read every active file because the request filter was not used for pruning. `delta_kernel` already performs partition pruning and stats-based skipping from a predicate and returns a selection vector.

### Decision

The adapter translates the filter into a kernel predicate and uses the kernel's selection vector. It compares no bounds, parses no stats, and does not post-filter the file list.

### Options Considered

| Option | Verdict |
|--------|---------|
| Read `add.stats` and filter the file list in the adapter | Rejected: a second copy of Delta bound semantics would drift from the kernel's |

### Consequences

- Pruning correctness rests on the kernel's contract, not on adapter code.

## ADR: Trust Delta's writer-side string-bound invariant, and say so

**ID:** delta-pruning-trust-writer-string-bound-invariant
**Plan:** add-delta-file-pruning
**Status:** Accepted

### Context

The Delta protocol truncates string statistics at a fixed prefix length. A truncated `maxValues` below the true maximum would make range pruning drop real rows.

### Decision

The adapter translates comparisons over string columns and relies on `maxValues` being a true upper bound. This is a deliberate trust in the protocol.

### Options Considered

| Option | Verdict |
|--------|---------|
| Refuse to translate string comparisons | Rejected: forfeits real pruning to guard against a writer that no shipped implementation matches |

### Consequences

- Common writers keep the bound valid, and the kernel compensates only for timestamps.
- A writer that emits a bare untagged prefix defeats pruning undetectably. Every Delta reader shares this assumption.

