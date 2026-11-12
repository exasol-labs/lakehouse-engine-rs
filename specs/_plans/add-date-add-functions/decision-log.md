# Decision Log: add-date-add-functions

## Interview

- Q: Scope of the request → A: Item 4 of #615 only: `ADD_DAYS`, `ADD_WEEKS`, `ADD_HOURS`, `ADD_MINUTES`. Items 1 to 3 and 5 are other plans.
- Q: Does the issue set the approach? → A: Yes. Teach the typed pass to tell a DATE from a TIMESTAMP (both are `TypeClass::Other` today), pick the rendering by argument type, advertise each function only once its DATE and TIMESTAMP forms match native Exasol on a live database.
- Q: Live probe on the local Exasol, result types → A: `ADD_DAYS`/`ADD_WEEKS` return DATE for a DATE and TIMESTAMP(p) for a TIMESTAMP(p). `ADD_HOURS`/`ADD_MINUTES` return TIMESTAMP(0) for a DATE and TIMESTAMP(p) for a TIMESTAMP(p). `ADD_DAYS(DATE, NULL)` is a DATE typed NULL.
- Q: Live probe on the local Exasol, fractional count → A: Exasol rounds it (`ADD_DAYS(DATE, 1.5)` adds 2 days, `ADD_HOURS(TIMESTAMP, 1.5)` adds 2 hours).
- Q: A count that is not provably an integer (DOUBLE, DECIMAL with scale, unknown type) → A: Reproduce Exasol's rounding in the scan. Do not decline it. The rounding rule (ties, negatives, DECIMAL, DOUBLE) must be verified live.
- Q: A form that fails live parity (TIMESTAMP(9) argument, argument of unproven class such as a CASE or aggregate result) → A: Decline that form and still advertise the function. Record each declined form in `docs/semantic-differences.md`. A function whose common DATE and TIMESTAMP forms cannot match stays unadvertised.
- Q: (after review) MIN, MAX, and a CASE next to NULL carry the DATE or TIMESTAMP class: decline or push? → A: Push them and prove parity with live E2E rows.
- Q: (after review) Dates before 1582-10-15, where Exasol uses the Julian calendar → A: The scan computes proleptic Gregorian results. Document the divergence in `docs/semantic-differences.md` with a spec scenario.

## Design Decisions

### [1] The scan computes the four functions with scan-session functions, not with DataFusion interval arithmetic

- **Decision:** The DataFusion dialect renders each function as a call to a scalar function that the scan session registers (`vs_add_days`, `vs_add_weeks`, `vs_add_hours`, `vs_add_minutes`). The typed pass picks the argument's rendering by its class: `CAST(arg AS DATE)` for a DATE and `CAST(arg AS TIMESTAMP(6))` for a TIMESTAMP, so the scan function receives only `Date32` or a microsecond timestamp without a time zone. One scan module owns Exasol's count rounding, the `ADD_DAYS` and `ADD_WEEKS` count limit, and the datetime range check.
- **Rationale:** DataFusion built-ins cannot reproduce three measured Exasol rules. DataFusion's `round` returns 0 for the DOUBLE count 0.49999999999999994, where Exasol adds 1. A result after 9999-12-31 is a valid Arrow value, so a filter keeps a row where native Exasol fails the query. DataFusion 55.2 also rejects `INTERVAL * integer` and has no function that builds an interval from a number. `vs_checked_float_div` sets the precedent of a scan-session function that reproduces an Exasol failure. The CAST makes the function's input follow the Exasol type and not the Arrow type that holds the value: an Iceberg `timestamptz` column is a zoned Arrow timestamp, and on Exasol 8.x a nanosecond catalog column is declared bare `TIMESTAMP`.
- **Alternatives:** Render `CAST(arg AS DATE) + round(n)` for days and `arg + arrow_cast(round(n) * 3600, 'Duration(Second)')` for hours. Rejected, because it differs at the DOUBLE rounding edge and returns rows where Exasol fails.
- **Promotes to ADR:** no

### [2] A TIMESTAMP(7) to TIMESTAMP(9) argument declines

- **Decision:** The typed pass declines the four functions over a TIMESTAMP class with a precision above 6.
- **Rationale:** The microsecond rendering of decision [1] drops the digits below a microsecond. A nanosecond rendering keeps them, but its Arrow range ends at 2262-04-11, so `ADD_DAYS` over it fails in the scan for a later result that native Exasol returns. The declined form runs in Exasol SQL and matches native Exasol.
- **Promotes to ADR:** no

### [3] Only columns, literals, CASTs, and the four functions derive a DATE or TIMESTAMP class

- **Decision:** The typed pass gives the DATE and TIMESTAMP classes to columns, literals, CASTs, and the four functions. The pass's existing rules carry a class through `MIN`, `MAX`, and a CASE branch next to NULL. A CASE, GREATEST, LEAST, or NULLIF over two date values, and every other date function, keeps the class of a type the pass does not derive, so the four functions over it run in Exasol.
- **Rationale:** A class claims that the scan's value equals Exasol's value of that type. The E2E rows of this plan check that claim live for columns, literals, CASTs, the four functions, `MIN` and `MAX`, and a CASE whose other branches are NULL. A scalar over a split `MIN` or `MAX` renders in Exasol's merge wrapper, so its rows compare the outcome with native Exasol on whatever path Exasol picks. Each further source needs its own check: DataFusion's `date_trunc`, for example, returns a nanosecond timestamp for a DATE argument, where Exasol returns a DATE. The common column and literal forms need no further source.
- **Alternatives:** Derive classes for `DATE_TRUNC`, `TO_DATE`, `TO_TIMESTAMP`, and a union of two dates. Rejected for this plan, because each needs its own parity check.
- **Promotes to ADR:** no

## Review Findings

### [plan-review] A TIMESTAMP literal finer than a microsecond, compared with a date addition

- **Finding:** A pushed filter that compares an `ADD_*` result with a TIMESTAMP literal of more than 6 fractional digits returns wrong rows, because DataFusion truncates the literal to the result's microsecond unit (apache/datafusion#22142). The adapter's literal floor applies only to a column operand.
- **Direction change:** The adapter's literal floor also accepts an `ADD_DAYS`, `ADD_WEEKS`, `ADD_HOURS`, or `ADD_MINUTES` call as the operand, with 6 digits. A 6-digit floor is exact for every call the scan computes, because the scan returns whole microseconds. A call that the typed pass declines takes the fallback, which applies the unrewritten tree. Declining such a comparison in the typed pass was rejected: it moves the filter to Exasol, and it adds a second site of the DataFusion workaround that must go when a DataFusion release carries apache/datafusion#22906.
- **Promotes to ADR:** no
