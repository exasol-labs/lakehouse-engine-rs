# Decision Log: fix-scan-second-fraction

## Interview

- Q: Which SECOND arguments does the scan compute? → A: column or CAST with a declared type (p from `exact_expression_type`); anything else declines to Exasol
- Q: Type the SECOND fragment as DECIMAL(2+p,p) so an unselected SECOND group key keeps the split aggregate? → A: yes, in this plan
- Given by issue #603: #587 is merged (PR #596), so the typed renderer exists; E2E over TIMESTAMP(6) and TIMESTAMP(9) vs a native copy; unit pins for p=0,3,6,9 and DATE; `SECOND(ts, n)` stays Exasol-computed
- Q: DATE argument: SECOND(<date>) typed DECIMAL(2,0) per issue, or value 0 with no exact type (8.29.13 types it DECIMAL(5,3))? → A: value 0, no exact type
- Q: EXTRACT(SECOND FROM <date>) fails natively; decline so Exasol raises the same error? → A: decline, match native

## Design Decisions

### [1] SECOND over a DATE computes 0 with no exact type, and EXTRACT(SECOND) over a DATE declines

- **Decision:** The scan computes `SECOND(<date>)` as 0, but the fragment carries no exact type and is not an integer for the text-conversion rule. `EXTRACT(SECOND FROM <date>)` declines, so Exasol raises its own error.
- **Rationale:** The user chose both behaviors in the interview (`## Interview`, last two answers). Live checks show the DATE type differs by version: `SECOND(<date>)` is `DECIMAL(2,0)` on 2025.1.16 and `DECIMAL(5,3)` on 8.29.13, and `specs/architecture.md` § Constraints supports 8.29.x and later. `EXTRACT(SECOND FROM <date>)` fails natively on both versions with `Feature not supported` (0A000), while the virtual schema returns 0 today. The value 0 is the same on both versions, so a projected item or a filter still runs in the scan.
- **Alternatives:** Type `SECOND(<date>)` as `DECIMAL(2,0)`, as issue #603's proposed fix states. Rejected: on 8.29.13 a nested aggregate over it or a text conversion of it would differ from native Exasol. Compute `EXTRACT(SECOND FROM <date>)` as 0, as the issue's type table lists it with `SECOND`. Rejected: native Exasol raises an error there.
- **Consequences:** An unprojected `GROUP BY SECOND(<date>)` and a nested aggregate over `SECOND(<date>)` run in the wrapper. A query with `EXTRACT(SECOND FROM <date>)` fails as it does over a native table.
- **Promotes to ADR:** no

### [2] Scale the fraction by multiplying with an exact decimal constant, not by dividing

- **Decision:** The fragment casts the cut integer from `date_part` to `DECIMAL(2+p, 0)`, multiplies it by the constant 10^-p cast from text to `DECIMAL(p, p)`, and casts the product to `DECIMAL(2+p, p)`.
- **Rationale:** A scratch DataFusion 55.2.0 program returned exact values for p = 0, 3, 6, and 9, NULL for a NULL argument, 58.25 for a pre-1970 value, and 59.999 (cut) for `59.999999` at p = 3. Arrow's decimal product keeps the scale s1 + s2, so no digit is lost.
- **Alternatives:** Divide by `CAST(10^p AS DECIMAL)`. Rejected: Arrow's decimal quotient has the scale s1 + 4, so `CAST(date_part('microsecond', ts) AS DECIMAL(8,0)) / CAST(1000000 AS DECIMAL(7,0))` returned `59.1234` for `59.123456`.
- **Promotes to ADR:** no

### [3] The adapter copies a typing rule only when it depends on exact argument types alone and holds on every supported version

- **Decision:** A split-aggregate column takes the type Exasol declares, the source column type, or a copied typing rule, or the split declines. The adapter copies a rule only when the rule depends on nothing but the exact types of its arguments and holds on every supported Exasol version.
- **Rationale:** Criterion 2 of `/speq:adr-rules` rule 2: the rule binds every later attempt to type an expression. The ADR digest holds `split-aggregate-columns-take-exact-types-or-decline`, whose Decision names the aggregate rule as the one rule the adapter copies. The `SECOND` typing that the interview requires contradicts it, so this entry supersedes it.
- **Alternatives:** Plan the expression in DataFusion and map its Arrow type. Rejected: DataFusion types `date_part` as `Int32` and a decimal quotient with scale s1 + 4, unlike Exasol. Copy Exasol's general expression typing. Rejected: those rules depend on literal values, the session, and the Exasol version, so the copy would drift. Keep a fixed default type. Rejected: any fixed type is wrong for text, dates, or exact decimals.
- **Consequences:** The feature specs list the rules that qualify, so a new rule that meets the policy needs no new ADR. Today two qualify: an aggregate's result type and `DECIMAL(2+p, p)` for `SECOND` over a `TIMESTAMP(p)` argument. An expression that no rule types still runs in the wrapper when it is a nested aggregate argument or an unprojected group key.
- **Supersedes:** split-aggregate-columns-take-exact-types-or-decline
- **Architecture:** no change: the rule lives in the scan translator that `specs/architecture.md` § Constraints already names as the one place a scan type rule is written
- **Promotes to ADR:** yes

## Review Findings

### [plan-review] SECOND and EXTRACT(SECOND) over a DATE departed from issue #603

- **Finding:** `[INTENT_DRIFT]`: the plan gave `SECOND(<date>)` no exact type, where issue #603 states `DECIMAL(2,0)`, and declined `EXTRACT(SECOND FROM <date>)`, where the issue lists it with `SECOND`. Only the first departure was put to the user.
- **Direction change:** None to the design. The user confirmed both departures (`## Interview`). Decision [1] now records that choice and credits `DECIMAL(2,0)` to the issue, and the open question is closed.
- **Promotes to ADR:** no
