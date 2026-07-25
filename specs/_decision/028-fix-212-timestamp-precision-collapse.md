# Decisions: fix-212-timestamp-precision-collapse

## ADR: TIMESTAMP precision field is fractionalSecondsPrecision, not precision

**ID:** timestamp-precision-field-is-fractional-seconds-precision
**Plan:** fix-212-timestamp-precision-collapse
**Status:** Accepted

### Context

A pushed-down `CAST(... AS TIMESTAMP(p))` with `p != 3` collapsed to bare `TIMESTAMP` (Exasol's
default `TIMESTAMP(3)`), making Exasol reject the pushdown with `Data type mismatch ... Expected
TIMESTAMP(6), but got TIMESTAMP(3)` (SQL error 04000, issue #212). The originating brief instructed
reading a `precision` field from the dataType JSON, "already empirically verified — do not
re-derive." `scripts/capture-pushdown-payload.sh` echoes only the adapter's output scan-spec JSON,
never the input dataType descriptor, so that field name was never actually observed.

### Decision

Both `exasol_type_from_json` (adapter EMITS derivation) and `render_cast_target` (vs-expression
CAST rendering) read `fractionalSecondsPrecision` for a TIMESTAMP's fractional-seconds precision,
not `precision`.

### Options Considered

| Option | Verdict |
|--------|---------|
| Read `fractionalSecondsPrecision` | ✓ Chosen — matches Exasol's `virtual-schema-common-java` data-type API doc, the reference fixture `pushdown_request_alltypes.json` (`C_TIMESTAMP_4` = `{"type":"TIMESTAMP","fractionalSecondsPrecision":7}`), and the repo's own committed test fixtures |
| Read `precision` per the brief | ✗ Rejected — Exasol uses `precision` only for `DECIMAL` (with `scale`) and `INTERVAL` (with `fraction`), never for TIMESTAMP; this would make the fix a silent no-op that only fails at E2E |
| Read `fractionalSecondsPrecision` with a `precision` fallback | ✗ Rejected as over-engineering — Exasol never sends `precision` on a TIMESTAMP |

### Consequences

The default of 3 explains the observed symptom (`got TIMESTAMP(3)`). Any future TIMESTAMP-precision
work in this codebase reads `fractionalSecondsPrecision`, matching Exasol's documented field name
rather than the brief's uncaptured assumption.

## ADR: DataFusion-dialect CAST rendering snaps TIMESTAMP precision to the nearest supported unit

**ID:** timestamp-precision-snap-nearest-datafusion-dialect
**Plan:** fix-212-timestamp-precision-collapse
**Status:** Accepted

### Context

DataFusion 54's SQL frontend parses `CAST(x AS TIMESTAMP(p))` only for `p` in `{0,3,6,9}`
(Second/Millisecond/Microsecond/Nanosecond); it rejects `p` in `{1,2,4,5,7,8}` with a parse error.
Exasol's own parser accepts every `p` in 0-9. A pushed-down CAST rendered for the DataFusion
dialect must pick a supported `p` whenever the declared precision falls in the unsupported set.

### Decision

`render_cast_target`'s `Dialect::DataFusion` arm snaps an unsupported precision to the nearest of
`{0,3,6,9}` (`0→0, 1→0, 2→3, 4→3, 5→6, 7→6, 8→9`; identity on `0/3/6/9`; clamp `>9` to 9).
`Dialect::Exasol` and the EMITS clause render `p` verbatim, unaffected by this snap.

### Options Considered

| Option | Verdict |
|--------|---------|
| Snap to the NEAREST supported unit | ✓ Chosen — honors the recorded design (brief, STATUS.md); each of the three gaps (0-3, 3-6, 6-9) has a non-integer midpoint (1.5, 4.5, 7.5), so "nearest" is unambiguous for every value in 0-9 |
| Ceil to the next supported unit (always ≥ requested precision) | ✗ Rejected — would make every snap lossless before EMITS truncation, but diverges from the recorded "nearest" design without a defect it fixes |

### Consequences

An up-snap (`2→3`, `5→6`, `8→9`) produces a finer DataFusion timestamp that the EMITS-declared
Exasol column truncates back to the requested `p`, keeping the round-trip faithful. The sole
down-snap (`1→0`) drops the tenths digit for the exotic `TIMESTAMP(1)` cast — an accepted,
explicitly named trade-off, since the Iceberg source stores microsecond-precision timestamps and
DataFusion 54 cannot parse `TIMESTAMP(1)` regardless.
