# Decisions: add-delta-file-pruning

## ADR: The kernel prunes; the adapter only translates

**ID:** delta-kernel-prunes-adapter-only-translates
**Plan:** add-delta-file-pruning
**Status:** Accepted

### Context

`DeltaFormatReader::resolve_scan` dropped the request filter, so every Delta query read every active file. `delta_kernel` already performs partition pruning and stats-based skipping from a predicate and returns a selection vector.

### Decision

The adapter translates the filter into a `delta_kernel` predicate and consumes the kernel's selection vector. It compares no bounds, parses no stats JSON, and does not post-filter the resolved file list.

### Options Considered

| Option | Verdict |
|--------|---------|
| Read `add.stats` and filter the file list in the adapter | Rejected: the kernel's comparison logic is unreachable, and a second copy of Delta bound semantics would drift |

### Consequences

Pruning correctness rests on the kernel's contract, not a hand-rolled comparison.

## ADR: Trust Delta's writer-side string-bound invariant, and say so

**ID:** delta-pruning-trust-writer-string-bound-invariant
**Plan:** add-delta-file-pruning
**Status:** Accepted

### Context

The Delta protocol says string statistics are cut off at a fixed prefix length. A truncated `maxValues` below the true maximum would make range pruning drop real rows.

### Decision

The adapter translates comparisons over string columns and relies on `maxValues` being a true upper bound. The spec records this as a deliberate protocol-trust trade-off.

### Options Considered

| Option | Verdict |
|--------|---------|
| Refuse to translate string comparisons | Rejected: forfeits real pruning to defend against a writer that no shipped implementation matches |

### Consequences

delta-spark and `parquet` both keep the bound valid, and the kernel compensates only for timestamps. A writer that emitted a bare untagged prefix would defeat pruning undetectably, an assumption every Delta reader shares.

## ADR: A third independent filter-JSON walker, with the shared IR filed rather than built

**ID:** delta-predicate-third-walker-defer-shared-ir
**Plan:** add-delta-file-pruning
**Status:** Accepted

### Context

The Iceberg translator and the DataFusion renderer already walk the same Exasol filter JSON, and Delta pruning adds a third walker. Their literal vocabularies and bound-soundness contracts differ.

### Decision

`delta_predicate.rs` is a third independent walker that mirrors the Iceberg translator's node dispatch. No shared predicate IR is extracted.

### Options Considered

| Option | Verdict |
|--------|---------|
| Shared format-neutral predicate IR for all three | Rejected: a large refactor of shipped code that no requirement here justifies, filed as a follow-up |

### Consequences

Three vocabularies stay duplicated. A third format would make the case for a shared IR.

## ADR: Never construct a false predicate or an empty junction

**ID:** delta-pruning-never-construct-false-predicate
**Plan:** add-delta-file-pruning
**Status:** Accepted

### Context

The kernel turns an empty disjunction into literal `false`, which prunes every file and returns no rows. The kernel has no usable IN, so every IN list becomes an OR-chain, and an IN list whose elements all fail to convert is the empty case.

### Decision

The translator returns no predicate before any junction constructor sees an empty set. Tests assert that no input produces a literal false predicate.

### Options Considered

| Option | Verdict |
|--------|---------|
| Rely on the translator's structure to make the empty case unreachable | Rejected: one forgotten early return from a silent wrong-results bug |

### Consequences

An explicit guard and a dedicated test prevent a later edit from reintroducing the hazard unnoticed.
