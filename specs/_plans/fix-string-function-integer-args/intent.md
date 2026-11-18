# Intent: fix-string-function-integer-args

## Interview

- Q: Where to convert the argument to Int64: scan analyzer rule or `vs-expression` typed pass? → A: Scan analyzer rule, by Arrow type.
- Q: Functions in scope: the eight from #604, or also the ROUND/TRUNC scale? → A: The eight; ROUND/TRUNC is a follow-up.
- Q: An argument beyond the BIGINT range? → A: The query fails with an error.
- Q: Negative arguments, which DataFusion's `rpad` panics on? → A: Match native, except a negative `SUBSTR` start, a follow-up.
- Q: Branch for the plan files? → A: New branch `fix/604-string-function-integer-args` from `origin/main`.

## Directions

- [D1] Every integer argument (Int32, Decimal128(p,0)), not only fractional ones, converts to `Int64`; DataFusion's `lpad`, `rpad`, `left`, `right` signatures accept only `Int64` exactly.
- [D2] A halfway value rounds away from zero, also for a negative `SUBSTR` start; live, native `SUBSTR('abcd', -1.5)` and `SUBSTR('abcd', -2)` both return `'cd'`.
- [D3] An argument outside ±999,999,999, `Int64` included, fails with Exasol's `numeric value out of range` message through the session failure slot; native Exasol rejects it live, so a wider BIGINT range would return rows native refuses.
- [D4] The issue's "unit test pins the rendered scan SQL" becomes a unit test of the scan rule's results per Arrow type, because the scan SQL no longer changes.
- [D5] A non-numeric argument stays unwrapped and fails as today; the `vs-expression` typed pass already declines a text argument at these positions, so none reaches the scan.
- [D6] A NULL argument goes through the conversion UDF and returns a NULL `Int64`, so DataFusion's exact `Int64` signatures never see an untyped NULL.
- [D7] The conversion wraps DataFusion's seven functions in the scan session instead of a rewrite in the numeric typing rule; DataFusion checks their signatures while it plans the SQL, before any analyzer rule runs.
