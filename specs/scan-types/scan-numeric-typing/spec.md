# Feature: Scan Numeric Typing

The scan evaluates pushed numeric expressions with Exasol's typing rules, so a pushed query returns the result native Exasol returns for the same query.

## Background

* Exasol combines DOUBLE with DECIMAL in DOUBLE. DataFusion combines them in DECIMAL, which overflows for large DOUBLE values and rounds small ones to zero.
* Exasol computes exact arithmetic on 128-bit values. It limits a materialized value to 36 digits, and it limits the operand that `+` or `-` rescales to the larger scale to `DECIMAL(36, larger scale)`. The scan does not limit the rescaled operand, a difference that `docs/semantic-differences.md` records. DataFusion's integer arithmetic and its `SUM` wrap around on overflow.
* Exasol compares exact values exactly at any width, and it returns DOUBLE from a value combinator whose common DECIMAL type exceeds 36 digits. DataFusion clamps the common type of two exact operands to 38 digits, which fails the cast of a wider value.
* One constructor builds every scan session. It installs a rewrite rule that runs before DataFusion's type coercion, and it registers the functions that the rule calls. Planning that derives the scan's result types uses the same constructor.

## Scenarios

### Scenario: DOUBLE combined with DECIMAL evaluates in DOUBLE

* *GIVEN* a pushed comparison, `IN` list, `BETWEEN`, `CASE`, `GREATEST`, `LEAST`, `COALESCE`, `NULLIF`, or `NVL` whose operands include both a DOUBLE and a DECIMAL value
* *WHEN* the scan evaluates it
* *THEN* the scan SHALL convert the DECIMAL operands to DOUBLE before it compares or combines them
* *AND* a `CASE` operand and its `WHEN` values SHALL be unified separately from the `THEN` and `ELSE` results
* *AND* a comparison of a DECIMAL column with a DOUBLE literal SHALL convert the column, so that comparison prunes no Parquet file or row group

### Scenario: Wide exact operands compare exactly and combine in DOUBLE

* *GIVEN* a pushed comparison, `IN` list, `BETWEEN`, or `CASE` operand whose integer and DECIMAL operands need a common type wider than 38 digits
* *WHEN* the scan evaluates it
* *THEN* the scan SHALL compare the operands exactly in a 256-bit decimal of the common type
* *AND* a `CASE` result, `GREATEST`, `LEAST`, `COALESCE`, or `NVL` whose integer and DECIMAL operands need a common type wider than 36 digits SHALL return DOUBLE, each operand counting at most 36 digits as Exasol declares it

### Scenario: A fractional numeric literal compares exactly

* *GIVEN* a pushed numeric literal with a fraction or an exponent, such as `4000000000000000000.5` or a 36-digit DECIMAL value
* *WHEN* the scan parses and evaluates the expression that holds it
* *THEN* the scan SHALL read the literal as DECIMAL, as Exasol does, and compare it exactly with an integer or DECIMAL operand
* *AND* the literal SHALL keep every digit in a comparison, an `IN` list, `BETWEEN`, and arithmetic

### Scenario: Exact arithmetic and SUM do not wrap

* *GIVEN* a pushed `+`, `-`, `*`, or unary minus over integer or decimal operands, or a `SUM` over an integer or decimal value
* *WHEN* the scan evaluates it
* *THEN* the result type SHALL follow Exasol's rule: `*` adds the operands' precisions and scales, `+` and `-` keep the larger scale and the larger integer part plus one digit, and an integer literal counts only its own digits
* *AND* the scan SHALL compute the result in 128 bits, so an intermediate value beyond 36 digits that fits 128 bits SHALL NOT raise
* *AND* a `SUM` SHALL accumulate in a type that cannot wrap around: a 128-bit decimal for an argument of at most 28 digits, and a 256-bit decimal for a wider one

### Scenario: Exact overflow raises Exasol's numeric value out of range

* *GIVEN* a pushed exact expression whose value exceeds 128 bits, or a projected value or `SUM` total that exceeds its declared `DECIMAL` precision
* *WHEN* the scan evaluates it in a projection, a filter, a broadcast join, or an aggregate
* *THEN* the query SHALL fail with a message that starts with `data exception - numeric value out of range`, as native Exasol's message does
* *AND* the overflow SHALL be recorded in the session's failure slot, so the message survives the Parquet row filter, which flattens errors to text
