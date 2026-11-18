# Feature: Scan Numeric Typing

The scan evaluates pushed numeric expressions with Exasol's typing rules, so a pushed query returns the result native Exasol returns for the same query.

## Background

* Exasol combines DOUBLE with DECIMAL in DOUBLE. DataFusion combines them in DECIMAL, which overflows for large DOUBLE values and rounds small ones to zero.
* Exasol computes exact arithmetic on 128-bit values. It limits a materialized value to 36 digits, and it limits the operand that `+` or `-` rescales to the larger scale to `DECIMAL(36, larger scale)`. The scan does not limit the rescaled operand. DataFusion's integer arithmetic and its `SUM` wrap around on overflow.
* Exasol compares exact values exactly at any width, and it returns DOUBLE from a value combinator whose common DECIMAL type exceeds 36 digits. DataFusion clamps the common type of two exact operands to 38 digits, which fails the cast of a wider value.
* One constructor builds every scan session. It installs a rewrite rule that runs before DataFusion's type coercion, and it registers the functions that the rule calls. Planning that derives the scan's result types uses the same constructor.

## Scenarios

<!-- DELTA:NEW -->
### Scenario: A string function's numeric argument rounds to an integer as Exasol does

* *GIVEN* a pushed `LPAD`, `RPAD`, `REPEAT`, `LEFT`, `RIGHT`, `SUBSTR`, `CHR`, or `UNICODECHR` whose length, count, start, or code argument is any integer, DECIMAL, or DOUBLE expression, such as `RPAD(C_VARCHAR, 2 + MOD(ID, 2))` or `SUBSTR(C_VARCHAR, C_DOUBLE)`
* *WHEN* the scan evaluates it in a select list, a filter, a GROUP BY key, an aggregate argument, or a join condition
* *THEN* the scan SHALL compute the function and SHALL NOT fail the query for the argument's type
* *AND* the scan SHALL round a non-integer argument to the nearest integer, a value halfway between two integers rounding away from zero, so `RPAD('ab', 3.5)` returns 4 characters as native Exasol does
<!-- /DELTA:NEW -->

<!-- DELTA:NEW -->
### Scenario: A string function's numeric argument outside Exasol's range fails the query

* *GIVEN* a pushed function of the previous scenario whose rounded numeric argument lies outside Exasol's argument range of ±999,999,999, whatever its type
* *WHEN* the scan evaluates it
* *THEN* the query SHALL fail with a message that starts with `data exception - numeric value out of range`, as native Exasol's message does
* *AND* the scan SHALL NOT truncate, wrap, or compute the function over the argument
<!-- /DELTA:NEW -->

<!-- DELTA:NEW -->
### Scenario: A negative count or length makes a string function return NULL as Exasol does

* *GIVEN* a pushed `LPAD`, `RPAD`, `LEFT`, or `RIGHT` whose count, or a `SUBSTR` whose length, is negative after rounding
* *WHEN* the scan evaluates it
* *THEN* the function SHALL return NULL, as native Exasol does
* *AND* a pushed `REPEAT` whose count is negative after rounding SHALL fail the query with a message that starts with `data exception - negative numbers not allowed in function REPEAT`
<!-- /DELTA:NEW -->
