# Feature: Type Mapping Module Structure

Gives each repeated type-classification shape in the Arrow and Exasol mapping exactly one implementation. This delta changes no recorded description. The paragraph here is plan-local context only.

## Background

* **This delta amends exactly TWO scenario clauses and is issue #407.** Both clauses rest on the
  same fact: `arrow_to_exasol_type` has no call site in the crate. The direct-storage catalog kind
  declares its columns from Parquet footers. Those footers carry Arrow types rather than catalog
  type names. That kind therefore becomes the function's FIRST consumer. Every other clause, every
  other scenario, and every Background bullet of this feature is unchanged.
* **The first amended clause is the retention clause** of "One arm list decides both the Exasol type
  string and the JSON-fallback flag". That clause states that `arrow_to_exasol_type` is retained
  `pub` even though it has NO call site anywhere in the crate. The retention SURVIVES and its reason
  strengthens. The function is now called. It is retained because it is used rather than because
  removing it would have exceeded a refactor's mandate.
* **The second amended clause is the per-producer reachability argument** of "One DECIMAL parser
  serves every Exasol type-string consumer". That clause's `arrow_to_exasol_type` segment argues
  that the function can render a negative or `s > p` scale but reaches no consumer, because nothing
  calls it. The call site now exists. A reachability argument over the new producer's own input
  domain replaces the segment, in the same per-producer form the clause already requires.
* **The Arrow-input guard is NOT changed and the two decimal guards stay separate.** The shared
  catalog guard takes an unsigned scale and tests `s <= p`. The Arrow-input guard takes a SIGNED
  `i8` scale and has no `s <= p` analogue. `datafusion-scan/type-mapping` records that separation
  deliberately, so folding the Arrow direction into the catalog predicate would accept a NEGATIVE
  scale that Exasol rejects. This delta therefore argues reachability rather than tightening a
  guard.
* **The reachability argument is the Apache Parquet format's own constraint on its `DECIMAL`
  logical type.** The precision is an integer greater than zero. The scale is between zero and
  the precision inclusive. A decimal read from a Parquet footer therefore satisfies `1 <= p` and
  `0 <= s <= p` before any guard runs. The input classes this feature names, a negative scale
  and `s > p`, are unreachable from the new producer.
* **The Arrow-input guard's `p <= 36 && s <= 36` test is sufficient over that domain.** With
  `s <= p`, a precision at or below 36 forces a scale at or below 36. A precision above 36
  falls to `VARCHAR(2000000)`. Every `DECIMAL` string the new producer can emit is therefore a
  valid Exasol decimal. The existing recorded bound `0 <= p,s <= 36` still holds for every
  producer in the repo.
* This feature keeps owning WHERE the mapping lives. `datafusion-scan/type-mapping` owns what a
  Parquet-sourced column maps to. `vs-adapter/parquet-directory-seam` owns which Arrow type each
  column folds to before the mapping sees it.

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: One arm list decides both the Exasol type string and the JSON-fallback flag

* *GIVEN* `arrow_to_exasol_type` and `needs_json_fallback` in `crates/lakehouse-engine/src/types/mapping.rs`, which each walk their own copy of the same Arrow arm list — Boolean, Int8/16/32/64, UInt8/16/32/64, Float32/64, Utf8, LargeUtf8, Date32, `Timestamp(_, _)`, in-range `Decimal128` — and each close with the same wildcard fallback
* *WHEN* the adapter resolves a column's Exasol type, or the scan decides whether a column needs JSON serialization
* *THEN* exactly one classifier in `types/mapping.rs` SHALL own that arm list, and both functions SHALL read their answer from it rather than re-matching on `DataType`
* *AND* the classifier SHALL encode the JSON-fallback flag EXPLICITLY rather than deriving it from the returned type string, because `Utf8` and `LargeUtf8` map to `VARCHAR(2000000)` with NO JSON fallback while an out-of-range `Decimal128` maps to the SAME string WITH one — the string alone cannot separate them
* *AND* the classifier SHALL be declared private to `types/mapping.rs`, because `arrow_to_exasol_type` and `needs_json_fallback` are its only consumers and hiding the arm list entirely is what makes the module deep
* *AND* `arrow_to_exasol_type` SHALL keep its `fn(&DataType) -> String` signature and `needs_json_fallback` its `fn(&DataType) -> bool`, so none of `needs_json_fallback`'s FOUR call sites changes — `adapter/pushdown/topn.rs` (lines 337 and 1230), `scan/join_scan.rs` (line 233), and `scan/raw_scan.rs` (line 367)
* *AND* `arrow_to_exasol_type` SHALL be retained as `pub` because it is the only public name for the Arrow-to-Exasol-string direction that CLAUDE.md § Data types documents as a compliance surface, and its inverse `exasol_type_to_arrow` is public and paired with it. This SUPERSEDES the clause's earlier reason — that the function has NO call site anywhere in the crate, every reference to it outside its own definition being a doc comment or a test, so removing it would be a scope ADD beyond issue #176's deduplication mandate. The direct-storage catalog kind declares its columns from Parquet footers, which carry Arrow types, so the function now HAS an in-crate consumer and is retained because it is used
* *AND* `arrow_value_at` (`scan/convert.rs`) SHALL NOT consume the classifier, because its dispatch key is the Arrow `DataType` and its arms carry conversion logic the classifier does not model — the NaN domain error for `Float32`/`Float64` and the `i64::MAX` overflow fallback for `UInt64` — so routing it through the classifier would add indirection without removing duplication
* *AND* every existing assertion in the `types/mapping.rs` test module MUST pass unedited, because the new consumer changes no arm, no guard, and no returned string
<!-- /DELTA:CHANGED -->

<!-- DELTA:CHANGED -->
### Scenario: One DECIMAL parser serves every Exasol type-string consumer

* *GIVEN* the canonical private `parse_decimal_args` in `types/mapping.rs` and the two hand-rolled `strip_prefix("DECIMAL(")` copies that re-derive it — one in `exasol_type_to_json`, one in `sum_emit_type` (`adapter/pushdown/grouped_agg.rs`)
* *WHEN* any of the three reads the precision and scale out of an Exasol `DECIMAL` type string
* *THEN* `parse_decimal_args` SHALL be the only implementation, declared `pub(crate)`, and both copies SHALL be deleted rather than retained as wrappers
* *AND* the parser SHALL keep its present contract unchanged — an already-uppercased and already-trimmed input, `(p, s)` returned as `(u8, i8)`, an absent scale defaulting to `0`, and `None` for anything that is not a well-formed `DECIMAL` declaration
* *AND* `sum_emit_type` MUST NOT gain an uppercasing step, because every producer of its `col_ty` argument already emits uppercase and adding one would change the result for a lowercase input
* *AND* `exasol_type_to_json` SHALL serialize the parsed scale as a SIGNED JSON number, so a negative Arrow decimal scale can never wrap into a large unsigned value
* *AND* a unit test SHALL pin the NEW answer at EVERY input class where consolidation changes it, stated separately per parser because the two profiles differ. For `sum_emit_type` the divergence set is defined by two INVARIANTS rather than by enumeration, because today it echoes the raw scale slice verbatim and discards the precision entirely. Over an input of the form `DECIMAL(<precision>,<scale>)`: (a) every scale whose text is not the canonical `i8` rendering diverges — untrimmed whitespace, a leading `+`, leading zeros, non-numeric text, a further comma, empty — because today that raw text is interpolated unchanged where after consolidation only a canonical rendering can emerge; and (b) every precision `parse_decimal_args` rejects diverges — outside `u8`, negative, non-numeric, empty — because today the precision is bound as `_p` and never read, so an unparseable precision still yields a `DECIMAL(36,…)` answer. The representative inputs below MUST each be pinned, and any further input satisfying either invariant is a divergence of the same class
* *AND* `exasol_type_to_json` SHALL have exactly THREE such classes: an absent scale (`DECIMAL(10)`) moves from a VARCHAR object to a decimal object of scale `0`; a precision or scale outside the parser's `u8`/`i8` range (`DECIMAL(300,2)`, `DECIMAL(10,200)`) moves from a decimal object to a VARCHAR object; and a negative scale (`DECIMAL(10,-2)`) moves from a VARCHAR object to a decimal object of scale `-2`. A three-argument input (`DECIMAL(10,2,3)`) and an empty argument list (`DECIMAL()`) are NOT divergences — both already fall through to VARCHAR before and after
* *AND* `sum_emit_type`'s representative inputs SHALL be: an absent scale (`DECIMAL(10)`) moves from `DOUBLE PRECISION` to `DECIMAL(36,0)`; an untrimmed scale (`DECIMAL(10, 2)`) moves from `DECIMAL(36, 2)` to `DECIMAL(36,2)`; a leading-`+` scale (`DECIMAL(10,+2)`) moves from `DECIMAL(36,+2)` to `DECIMAL(36,2)`; a non-numeric scale (`DECIMAL(10,X)`) moves from `DECIMAL(36,X)` to `DOUBLE PRECISION`; a scale outside `i8` (`DECIMAL(10,200)`) moves from `DECIMAL(36,200)` to `DOUBLE PRECISION`; a three-argument input (`DECIMAL(10,2,3)`) moves from `DECIMAL(36,2,3)` to `DOUBLE PRECISION`; a precision outside `u8` (`DECIMAL(300,2)`) moves from `DECIMAL(36,2)` to `DOUBLE PRECISION`; and a non-numeric precision (`DECIMAL(X,2)`) moves from `DECIMAL(36,2)` to `DOUBLE PRECISION`. Inputs two through six are instances of invariant (a); the last two are instances of invariant (b). The absent-scale input is the one representative neither invariant generates — with no comma there is no scale text to diverge, and a precision `parse_decimal_args` rejects does NOT diverge here (`DECIMAL(X)` is `DOUBLE PRECISION` both before and after), so the move comes solely from `parse_decimal_args` defaulting an absent scale to `0` where `sum_emit_type` today declines the input entirely
* *AND* `sum_emit_type` SHALL thereby GAIN a whitespace-trimming step it does not have today, because `parse_decimal_args` trims each argument before parsing; this is the untrimmed-scale class above and is an intended consequence of consolidation, not an incidental one
* *AND* every input class above MUST be unreachable from every producer in this repo, argued PER PRODUCER rather than by one universal bound, because no such universal bound holds: `iceberg_primitive_to_exasol` and `unity_type_name_to_exasol` (`types/mapping.rs`) each emit a `DECIMAL` string only through the ONE shared `catalog_decimal_to_exasol` guard `datafusion-scan/type-mapping` owns — branching on the extracted predicate `exasol_representable_catalog_decimal`, `1 <= p <= EXASOL_DECIMAL_MAX_PRECISION (36) && s <= p` over unsigned precision and scale — and return `VARCHAR(2000000)` otherwise, so `0 <= p,s <= 36` holds by that single guard rather than by any external claim or by two guards agreeing, and `iceberg_type_to_exasol` and `column_source_type_to_exasol` only delegate to them; `exasol_type_from_json` always emits both arguments and reads each from an unsigned JSON number under a `p <= 36 && s <= 36` guard, so `0 <= p,s <= 36` — though it does NOT check `s <= p`, which is deliberate and is NOT folded into the shared guard, because its input is Exasol's own `dataType` JSON for a type Exasol already accepted rather than catalog wire input; and `arrow_to_exasol_type` CAN render a negative or `s > p` scale, since its guard is `*p <= 36 && *s <= 36` over an `i8` scale with no lower bound and no `s <= p` check, but its ONE producer is the direct-storage kind's Parquet-footer column list, reaching it through the scan-spec tag vocabulary's lossless round trip, and the Apache Parquet format constrains its `DECIMAL` logical type to a precision above zero and a scale between zero and the precision inclusive, so `1 <= p` and `0 <= s <= p` hold on arrival and the guard admits only pairs with `0 <= s <= p <= 36`. This SUPERSEDES the clause's earlier `arrow_to_exasol_type` segment, which concluded from the function having NO call site anywhere in the crate that its output reaches neither `exasol_type_to_json` nor `sum_emit_type`; the recorded bound `0 <= p,s <= 36` is unchanged and now rests on the producer's input domain rather than on the absence of a producer
* *AND* the Arrow-input guard MUST NOT be folded into the shared catalog predicate, because that predicate takes an UNSIGNED scale and the Arrow one is a SIGNED `i8`, so the fold would admit a negative scale that Exasol rejects — the separation `datafusion-scan/type-mapping` records deliberately
* *AND* `arrow_type_from_tag`'s lowercase `decimal128(p,s)` parse SHALL stay a separate implementation and SHALL NOT be folded behind a prefix parameter, because it reads the internal `ScanSpec::logical_schema` tag vocabulary rather than the Exasol SQL type grammar — a different wire format, changing for a different reason, and one that requires both arguments where `parse_decimal_args` defaults an absent scale to `0`
* *AND* no prefix parameter SHALL be added to `parse_decimal_args`, because every remaining consumer passes the same `DECIMAL(` prefix and a single-valued parameter is the decision-the-module-declined-to-make that `/speq:design-philosophy` rejects
<!-- /DELTA:CHANGED -->
