# Decision Log: add-integer-div-pushdown

## Interview

- Q: Scope? → A: #615 part 3: push DIV down when both operands are integers; decline otherwise.
- Q: Emulate DIV over decimal or double operands? → A: No, decline; Exasol computes them.
- Q: Rendering? → A: A checked integer-division scan function, like FLOAT_DIV's.
- Q: Refactor FLOAT_DIV? → A: Only share one checked-division module; FLOAT_DIV behavior unchanged.
- Q: Result type of the integer division function? → A: Decimal128 with the numerator's precision, scale 0.
- Q: E2E positions? → A: Select list (all sign cases), WHERE, zero divisor fails, decimal operand falls back.
- Q: Split into several plans? → A: No, one plan.

## Design Decisions

### [1] A checked scan function computes DIV, not DataFusion's `/`

- **Decision:** The DataFusion side of `DIV` calls `vs_checked_int_div`, which widens both operands to 128-bit integers, truncates toward zero, and returns `DECIMAL(<numerator precision>,0)`.
- **Rationale:** DataFusion 55's `/` matches `DIV` only for two `Int64` or two `Int32` operands. Over `Decimal128(p,0)`, the type an Iceberg `long` and most integer expressions reach the scan as, it returns a scaled decimal (`-3.5`) that a cast rounds to `-4`. `i64::MIN / -1` overflows. Its zero-divisor error is not recognised by type, so it surfaces with the storage-read framing.
- **Alternatives:** `CAST(<l> AS BIGINT) / CAST(<r> AS BIGINT)` truncates correctly, but it cannot hold an operand wider than 19 digits and it overflows on `i64::MIN / -1`.
- **Consequences:** The function accepts only integer Arrow types, so the typed scan pass must decline every other operand at plan time. A non-integer type that still reaches the function fails scan planning instead of returning a wrong value.
- **Promotes to ADR:** no

## Review Findings
