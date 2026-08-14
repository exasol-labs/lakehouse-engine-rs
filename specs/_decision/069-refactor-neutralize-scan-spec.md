# Decisions: refactor-neutralize-scan-spec

## ADR: The Delta column-mapping MODE is not carried in the scan spec at all

**ID:** delta-column-mapping-mode-not-carried-on-wire
**Plan:** `refactor-neutralize-scan-spec`
**Status:** Accepted

### Context

Issue #342 names a new neutral home for per-COLUMN binding data (`LogicalField`'s field-id and
physical-name keys) but names none for the Delta column-mapping MODE itself. The mode decides which
binding key each `LogicalField` gets — `id` mode populates a field-id, `name` mode populates a
physical name, `none` mode populates neither — so a home for the mode still had to be chosen even
though the issue's own type shapes left it implicit.

### Decision

`DeltaColumnMappingMode` disappears with `DeltaTableSpec` and gets no neutral home. The mode is
consumed entirely at plan time, where it decides which binding key each `LogicalField` carries. The
scan side reads the key it was given and never reads a mode.

### Options Considered

| Option | Verdict |
|--------|---------|
| Consume the mode at plan time into each field's binding key; carry no mode on the wire | ✓ Chosen — closes the gap without adding a field, since nothing downstream reads a mode after the change |
| Move `column_mapping_mode` onto `CommonScanSpec` as a neutral-ish table-level value | ✗ Rejected — its only consumer is the binding-key choice, so a carried mode would be a second home for one decision, free to disagree with the keys it was supposed to explain |
| Keep a minimal Delta-named table block holding just the mode | ✗ Rejected outright — issue #342 exists to remove exactly that block, and a one-field block is the shallowest possible module |

### Consequences

Every `LogicalField` carries exactly one binding key (or none, for identity binding), decided once
at plan time, with nothing left on the wire that could drift from it. A future column-mapping mode
(if Delta ever adds one) still resolves to one of the three existing binding-key shapes rather than
reopening a mode field on `ScanSpec`.
