# Feature: Type Mapping Module Structure

Unchanged by this plan, and reproduced here only because the delta validator requires a description. The recorded text stands as written in `specs/datafusion-scan/type-mapping-module-structure/spec.md`.

## Background

* Unchanged by this plan. This section is unmarked, so `speq record` ignores it and the recorded Background survives intact. See `specs/datafusion-scan/type-mapping-module-structure/spec.md`.

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: One classifier names the Exasol type-string families the pushdown guards branch on

* *GIVEN* the pushdown consumers `guard_like_subject` and the string-conversion decline check `string_conversion_declined` (`adapter/pushdown/support.rs`), which each branch over the same coarse Exasol-type-string family vocabulary — a `VARCHAR` or `CHAR` prefix, the exact string `DATE`, a `DECIMAL` prefix, and everything else — and differ on one family: `guard_like_subject` declines `DECIMAL` with everything else, while the decline check passes it with `VARCHAR`, `CHAR`, and `DATE`
* *WHEN* a consumer needs that family split
* *THEN* `types/mapping.rs` SHALL expose, as `pub` items, an `ExaTypeClass` enum with exactly the variants `Character`, `Date`, `Decimal`, and `Other`, and a `classify_exa_type` function taking an Exasol type string and returning that enum, whose doc comment names its consumers `guard_like_subject` and `string_conversion_declined` in `adapter/pushdown/support.rs`
* *AND* the function SHALL classify by these predicates EXACTLY, pinned literally so no equivalent-looking variant can pass: `Character` iff the string `starts_with("VARCHAR")` OR `starts_with("CHAR")`; `Decimal` iff it `starts_with("DECIMAL")` with NO open paren — an implementation testing `starts_with("DECIMAL(")` MUST be rejected, because it silently diverges from both consumers on the bare string `DECIMAL`; `Date` iff the string EQUALS `DATE` exactly; `Other` otherwise
* *AND* `classify_exa_type` SHALL take a NON-optional `&str`, because the column-lookup miss that both consumers ALSO decline on is not a type family and cannot be expressed by the classifier; that `Option<&str>` stays each consumer's own branch
* *AND* a unit test SHALL pin these representative strings: `VARCHAR(4) ASCII` and `CHAR(2)` as `Character`; `DECIMAL(20,0)` AND bare `DECIMAL` with no arguments as `Decimal` — the bare case is what separates the correct predicate from `starts_with("DECIMAL(")`, so omitting it leaves the contract untested; `DATE` as `Date`; and `TIMESTAMP` and `DOUBLE PRECISION` as `Other`
<!-- /DELTA:CHANGED -->
