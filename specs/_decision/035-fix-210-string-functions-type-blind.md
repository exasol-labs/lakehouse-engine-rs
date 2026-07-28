# Decisions: fix-210-string-functions-type-blind

## ADR: Decline pushdown, never cast, for BOOLEAN/DOUBLE/TIMESTAMP string-position arguments

**ID:** string-fn-decline-noncoercible-types-not-cast
**Plan:** fix-210-string-functions-type-blind
**Status:** Accepted

### Context

`UPPER`/`LOWER`/`TRIM`/`INSTR`/`LOCATE` and the rest of the Exasol string-function family hard-fail
DataFusion planning when a string-position argument resolves to a non-VARCHAR/CHAR/DATE/DECIMAL
Exasol column type (issue #210). The Apache Iceberg table spec's Primitive Types table defines
`boolean` as "True or false", `double` as "64-bit IEEE 754 floating point", and `timestamp` as
"Timestamp, microsecond precision, without timezone" — none is assigned a text form, so Exasol and
DataFusion each pick their own: Exasol renders BOOLEAN as `TRUE`/`FALSE` and TIMESTAMP
space-separated, DataFusion renders `true`/`false` and `T`-separated.

### Decision

`coerce_string_position_arg` returns `None` — declining the whole tree, per #207's
`Option<Json>` contract — for any resolvable string-position column type other than VARCHAR, CHAR,
DATE, or DECIMAL. It never emits a `CAST(<col> AS VARCHAR)` for BOOLEAN, DOUBLE, or TIMESTAMP.

### Options Considered

| Option | Verdict |
|--------|---------|
| Decline pushdown, evaluate natively in Exasol | ✓ Chosen — the only branch that cannot silently change a result |
| Emit `CAST(<col> AS VARCHAR)` for BOOLEAN/DOUBLE/TIMESTAMP | ✗ Rejected — the two engines' text forms diverge, so a cast turns a loud hard failure into a quiet wrong answer |

### Consequences

Extends the reasoning #207 recorded for declining a DECIMAL LIKE subject before #211 supplied a
faithful formatter: a resolvable-but-unformattable type stays a native-fallback, never a guessed
cast. Any future string-position type addition must supply a proven Exasol-faithful text form
before it can move from decline to coerce.

## ADR: INSTR/LOCATE beyond two arguments declines pushdown unconditionally on argument type

**ID:** instr-locate-arity-decline-over-type-coerce
**Plan:** fix-210-string-functions-type-blind
**Status:** Accepted

### Context

`crates/vs-expression/src/lib.rs:741-772` renders `INSTR`/`LOCATE` by reading only `args[0]` and
`args[1]`; a 3rd (start position) or 4th (occurrence) argument is silently dropped (issue #228).
Before this plan, a non-VARCHAR argument at that arity hard-failed at DataFusion planning, which
masked the dropped-argument defect behind a loud error. An unconditional `[0, 1]` coercion of the
first two arguments — the plan's initial design — would let that node plan successfully and return
a position computed from a truncated rendering: a silently wrong result, not a crash.

### Decision

`string_position_args` gained a third outcome, `Decline`, returned for `INSTR`/`LOCATE` with
`arg_count > 2` unconditionally on every argument's type — including an all-VARCHAR call. The
guard declines the whole tree rather than coercing indices 0 and 1.

### Options Considered

| Option | Verdict |
|--------|---------|
| Decline the call outright at arg_count > 2, independent of argument type | ✓ Chosen — converts a masked wrong-result defect into an explicit native fallback, and corrects the pre-existing all-VARCHAR case too |
| Coerce indices 0 and 1 regardless of arity | ✗ Rejected — lets a truncated rendering plan successfully and silently return the wrong position |
| Render the dropped 3rd/4th argument faithfully | ✗ Rejected — a different, arity-typed defect (#228), out of this plan's typing-only scope |

### Consequences

`string_position_args(fn_name, arg_count) -> StringPositionArgs` stays one pure, standalone
function with three outcomes (`NotGoverned` / `Coerce(indices)` / `Decline`), superseding its
earlier `Option<Vec<usize>>` signature. `INSTR(c_varchar, 'b', 3)`, previously a silently wrong
`strpos` pushdown ignoring the start position, now declines to Exasol's native evaluation at both
wired surfaces. A faithful rendering of the dropped arguments remains tracked separately (#228).
