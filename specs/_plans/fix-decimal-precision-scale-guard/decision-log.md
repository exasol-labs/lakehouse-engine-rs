# Decision Log: fix-decimal-precision-scale-guard

## Interview

**Q:** Keep issue #329's decision to reject a fail-loud approach and keep the silent VARCHAR fallback for a malformed precision/scale, rather than reconsidering threading `Result` through `column_source_type_to_exasol` → `build_listing_virtual_tables`?
**A:** Keep the VARCHAR fallback — do not introduce fail-loud or `Result` threading. The VARCHAR fallback already absorbs these cases, and reachability is low: both bad inputs require a misbehaving catalog.

**Q:** Per CLAUDE.md § Verification discipline (no SQL-capability claim without checking it against a live Exasol instance), should the plan carry an explicit task to verify against the Docker Exasol container that `DECIMAL(0,0)` and `DECIMAL(5,10)` are actually rejected, rather than relying on documented limits?
**A:** Yes — add an explicit live-verify task: run `SELECT CAST(1 AS DECIMAL(0,0));` and `SELECT CAST(1 AS DECIMAL(5,10));` against the Docker Exasol container to confirm both are rejected, before or alongside implementing the fix.

## Design Decisions

### [1] One shared private guard, not two corrected copies

- **Decision:** Add a private `fn decimal_to_exasol(precision: u32, scale: u32) -> String` to `crates/lakehouse-engine/src/types/mapping.rs` and collapse both `iceberg_primitive_to_exasol` and `unity_type_name_to_exasol` onto it, rather than adding the missing conditions to each guard in place.
- **Alternatives:** Fix each guard where it sits — smaller diff, no new item. Rejected: two independently-correct copies still agree by coincidence, and the next reader can correct one and miss the other. That recurrence of one decision in two places is the back-door leakage `/speq:design-philosophy` singles out.
- **Rationale:** The duplication is the defect, not just the missing conditions. One owner makes the two catalog kinds identical by construction. The helper is private because both consumers live in the same file, so hiding the predicate entirely is what makes the module deep.
- **Promotes to ADR:** no

### [2] Silent VARCHAR fallback, no fail-loud path

- **Decision:** A precision or scale outside Exasol's `DECIMAL` domain returns `VARCHAR(2000000)`. `column_source_type_to_exasol` and `build_listing_virtual_tables` stay infallible.
- **Alternatives:** Return `Result` and abort the enumeration with a named error. Rejected in the interview and in issue #329.
- **Rationale:** The identical fallback already absorbs `p > 36`; giving two neighbouring pairs a louder treatment splits one policy in two. Fail-loud costs new error variants, new tests, and a fallible signature on a path where every other type maps totally.
- **Promotes to ADR:** no

### [3] The Iceberg spec does NOT constrain p and s the way issue #329 claims

- **Decision:** Justify the guard solely by the Exasol target-type limitation, and record in the `datafusion-scan/type-mapping` delta that the Iceberg spec permits both bad pairs.
- **Alternatives:** Carry issue #329's premise forward ("the Iceberg spec constrains p and s the same way") and argue the inputs are out-of-spec. Rejected: the spec text does not say that.
- **Rationale:** The Primitive Types table gives `decimal(P,S)` exactly one constraint — "Scale is fixed, precision must be 38 or less". No lower bound on `P`, no relation between `S` and `P`. A catalog serving `decimal(0,0)` or `decimal(5,10)` therefore violates nothing normative, so "only a misbehaving catalog produces it" is not a sound basis for the guard. This strengthens the case for the fix rather than weakening it, and it stops a future reader from re-deriving a wrong reason.
- **Promotes to ADR:** yes

### [4] The Unity `type_precision` default is a second, closer route to `p = 0`

- **Decision:** Record in both the `datafusion-scan/type-mapping` and `vs-adapter/unity-catalog-create-virtual-schema` deltas that `neutral_column`'s `.unwrap_or(0)` turns an absent wire `type_precision` into `p = 0`.
- **Alternatives:** State reachability as "requires a misbehaving catalog" and stop, per issue #329's reachability table. Rejected: that table lists "p or s absent" and "`p == 0`" as separate rows, but on the Unity path the first produces the second.
- **Rationale:** An omitted optional field is a weaker precondition than a malformed one. Naming the route keeps the low-reachability claim honest instead of overstating it.
- **Promotes to ADR:** no

### [5] Deleting "or `type_text`" is part of this fix, not an unrelated tidy-up

- **Decision:** Delete both occurrences of "or `type_text`" from `specs/vs-adapter/unity-catalog-create-virtual-schema/spec.md` — the Background paragraph and the Spark-column-types scenario clause — in this plan.
- **Alternatives:** Split it into its own documentation change.
- **Rationale:** The phrase advertises a recovery path for exactly the null-`type_precision` case this guard now absorbs. `ColumnInfo` declares no `type_text` field and deserializes no such value, so a reader debugging a `p = 0` column would hunt for a fallback the code cannot take. The phantom path and the guard are two halves of one defect.
- **Promotes to ADR:** no

### [6] Two scenarios of the Unity spec change, not one

- **Decision:** Also amend the GIVEN of "Unity Catalog Spark column types map to Exasol types sufficient for listing" from "`DECIMAL(p,s)` with `p` at most 36 and `s` at most 36" to Exasol's real domain.
- **Alternatives:** Amend only the incompatible-type scenario, as issue #329 scoped it.
- **Rationale:** Left alone, that GIVEN would claim `DECIMAL(5,10)` maps to `DECIMAL(5,10)` while its sibling scenario claims the same input maps to `VARCHAR(2000000)`. Two recorded scenarios contradicting each other is worse than either being stale alone.
- **Promotes to ADR:** no

### [7] `arrow_to_exasol_type` and `exasol_type_from_json` stay out of scope

- **Decision:** Leave both same-shaped guards untouched, and record why in the `datafusion-scan/type-mapping` delta so the omission reads as a decision.
- **Alternatives:** Extend the helper to all four sites for one uniform rule.
- **Rationale:** The Arrow guard takes `Decimal128(u8, i8)`, whose SIGNED scale is legitimately negative and has no `s ≤ p` analogue; its input is DataFusion's own schema, not catalog wire input, and `type-mapping-module-structure` already records that it has no call site in the crate. `exasol_type_from_json` reads `u64` precision and scale out of Exasol's own `dataType` JSON for a type Exasol already accepted — and once this fix lands no invalid decimal can be declared for Exasol to echo back. Neither is a scoping preference; both are genuine domain mismatches.
- **Promotes to ADR:** no

### [8] The `type-mapping-module-structure` delta corrects a stale verbatim guard quotation

- **Decision:** Amend that feature's per-producer reachability clause, which quotes `iceberg_primitive_to_exasol`'s guard verbatim as `*precision <= 36 && *scale <= 36`, and add the Unity producer the clause predates. Drop the `mapping.rs:193` line citation rather than re-pin it.
- **Alternatives:** Leave the clause alone since its conclusion still holds.
- **Rationale:** The clause exists to make the reachability argument falsifiable per producer, so a quotation of code that no longer exists defeats its whole purpose. The conclusion (`0 <= p,s <= 36`) survives a fortiori under the narrower guard. The line number had already drifted off the function before this plan started; re-pinning only resets a counter that drifts again.
- **Promotes to ADR:** no

### [9] The live capture gates the fix rather than following it

- **Decision:** Task 1.1 runs before any code change and includes two POSITIVE controls, `DECIMAL(1,0)` and `DECIMAL(36,36)`. If either bad shape is accepted, implementation stops.
- **Alternatives:** Run the probe alongside or after the fix; probe only the two bad shapes.
- **Rationale:** The probe is what makes the plan's premise a capture rather than a claim, so it has to be able to invalidate the plan — which it cannot do after the code is written. The controls matter for the same reason: a probe where every statement is rejected is equally consistent with a broken invocation, and `(36,36)` in particular is the boundary the new `s ≤ p` half could plausibly have broken.
- **Promotes to ADR:** no

### [10] No `[expert]` task

- **Decision:** Tag no task `[expert]`.
- **Alternatives:** Tag task 2.2 as a correctness-sensitive type-mapping change.
- **Rationale:** The change is one pure two-integer predicate plus two call-site substitutions in a single file. No concurrency, no ordering hazard, no cross-file refactor, no novel algorithm — the criteria `/speq:planning` sets for the tag. Over-tagging spends tokens without reducing defect risk.
- **Promotes to ADR:** no

## Live Captures

Captured 2026-08-13 against the already-running, healthy `exasol/docker-db:2025.2.1` container
(`lakehouse-engine-rs-2-exasol-1`, up 22h) bound to the same ports the plan's DSN targets —
`docker compose up -d --wait exasol` in this checkout hit `Error response from daemon: Address
already in use` on `28563`/`22581` because that container already holds them, so no second
container was started; the probes ran against the existing one via
`exapump sql "<stmt>" -d "exasol://sys:exasol@localhost:28563?validateservercertificate=0"`.

| Probe | Expected | Observed |
|---|---|---|
| `SELECT CAST(1 AS DECIMAL(0,0))` | rejected | **Rejected.** `Query execution failed: Protocol error: illegal precision value: 0 [line 1, column 29] (Session: 1873391607888150528) (SQL state: 42000)` |
| `SELECT CAST(1 AS DECIMAL(5,10))` | rejected | **Rejected.** `Query execution failed: Protocol error: illegal scale value: 10 [line 1, column 30] (Session: 1873391608034623488) (SQL state: 42000)` |
| `SELECT CAST(1 AS DECIMAL(1,0))` (control) | succeeds | **Succeeded.** Returned `1` (1 row) |
| `SELECT CAST(1 AS DECIMAL(36,36))` (control) | succeeds | **As literally written, failed** — but not with a type-domain rejection. Error: `Query execution failed: Protocol error: data exception - numeric value out of range: value 1 is not in [ -0.999999999999999999999999999999999999 .. 0.999999999999999999999999999999999999 ] in cast of expression 1 (Session: 1873391608313413632) (SQL state: 22003)`. `DECIMAL(36,36)` leaves zero integer digits, so the literal `1` cannot fit in it — this is a value-range exception (SQL state `22003`, "numeric value out of range"), categorically different from the two bad shapes' type-domain rejection (SQL state `42000`, "illegal precision/scale value"). Re-ran with a value that fits the shape, `SELECT CAST(0.5 AS DECIMAL(36,36))`: **succeeded**, returning `0.500000000000000000000000000000000000` (1 row). This confirms `DECIMAL(36,36)` is itself a legal, accepted type shape — the boundary case the new `s ≤ p` guard half depends on is intact. |

**Verdict: PASS.** Both bad shapes are rejected at the type-validation layer (SQL state `42000`,
"illegal precision/scale value") with distinct, specific error text naming the offending field
(`precision` vs `scale`) — not a generic parse failure, so this is a real type-domain rejection,
not a broken probe. Both controls confirm the domain's edges are legal: `DECIMAL(1,0)` (the `p=1`
floor) succeeds outright, and `DECIMAL(36,36)` (the `p=36, s=p` ceiling) is accepted as a type and
only fails on the unrelated, expected value-range exception when asked to hold a value that
shape cannot represent — confirmed distinct from a type rejection by re-running with a
representable value. The plan's premise — Exasol's `DECIMAL` domain is `1 ≤ p ≤ 36`, `0 ≤ s ≤ p` —
holds. Proceeding to task 2.1.

## Review Findings

<!-- No adversarial plan-review round ran for this plan. -->
