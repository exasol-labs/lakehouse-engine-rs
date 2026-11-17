# Intent: add-clock-function-pushdown

## Interview

- Q: One plan or split clock pushdown from the `SESSION_TIME_ZONE` property? → A: One plan
- Q: When to advertise `CURRENT_DATE`, `CURRENT_TIMESTAMP`, `LOCALTIMESTAMP`? → A: Always, difference documented
- Q: Clock calls in SQL that Exasol runs (wrapper, self-applied filter, join)? → A: Fold everywhere
- Q: Which `SESSION_TIME_ZONE` values, and how to test a numeric `TZ`? → A: The names `SELECT TIME_ZONE_NAME FROM EXA_TIME_ZONES` lists; test numeric `TZ` with a `%env TZ` line
- Q: Should `CURRENT_DATE + 1` style arithmetic be folded by the adapter? → A: No, Exasol already sends it as a literal
- Q: Handle `literal_timestamputc` too? → A: Yes, in this plan
- Q: (implementation) `ALTER VIRTUAL SCHEMA ... SET TIME_ZONE` is an Exasol syntax error and a quoted name is refused, so which name? → A: `SESSION_TIME_ZONE`

## Directions

- [D1] The adapter folds by rewriting each clock node of the request into the literal node it already handles (`literal_date`, `literal_timestamp`) before planning; pruning, scan, wrapper, and join paths then need no clock arm.
- [D2] `CURRENT_TIMESTAMP` and `LOCALTIMESTAMP` fold into the `TIMESTAMP WITH LOCAL TIME ZONE` value of the instant, and every such value follows one rule: inside a select-list item that Exasol types `TIMESTAMP WITH LOCAL TIME ZONE` it stays `literal_timestamputc` of the UTC instant, so the projection shows the session's native value; everywhere else it becomes the plain `literal_timestamp` of the folding-zone wall clock, which the scan, pruning, and the adapter's constant checks all handle. The declared type decides, because Exasol 8.29.13 types `LOCALTIMESTAMP` `TIMESTAMP WITH LOCAL TIME ZONE` and Exasol 2025.1.16 types it `TIMESTAMP` (captured live).
- [D3] The clock is read once per adapter VM and shared by every virtual schema the VM serves; Exasol runs one adapter VM per statement, so this is one reading per statement.
- [D4] A folded timestamp carries as many fractional digits as the precision argument that Exasol 2025 sends with each timestamp function (3 unless the call names one), and millisecond digits on Exasol 8, which sends no argument (captured live on 8.29.13 and 2025.1.16).
- [D5] A `SESSION_TIME_ZONE` value resolves through the tz database of `chrono-tz`, already in the graph through Arrow, compared without regard to case. The `EXA_TIME_ZONES` names of Exasol 8.29.13 and 2025.1.16 together are exactly its 597 zone names (captured live), so an embedded copy of the list would reject nothing more; a unit test fails when a `chrono-tz` upgrade adds a name the captured list lacks.
- [D6] `TZ` is read as a zone name, or as the POSIX form `UTC±H[:MM]` with the sign inverted; an unset or unreadable `TZ` fails only a request that holds a clock function or a compared TSTZ literal whose zone comes from `TZ`.
- [D7] `DBTIMEZONE` and `SESSIONTIMEZONE` stay unadvertised: the adapter cannot read the session zone, and `DBTIMEZONE` alone has no realistic filter use.
- [D9] A compared `literal_timestamputc` folds into the folding-zone wall clock like `CURRENT_TIMESTAMP`, so `ts > ADD_HOURS(CURRENT_TIMESTAMP, -1)` and explicit TSTZ literals prune; this moves the session-zone difference to every TSTZ comparison, and `EXCLUDED_CAPABILITIES = 'LITERAL_TIMESTAMP_UTC'` restores native evaluation.
- [D10] One clock reading holds per statement on one virtual schema; nothing establishes that two virtual schemas of one statement share an adapter VM, so a statement over two virtual schemas may fold two readings, and `docs/semantic-differences.md` says so.
- [D11] That Exasol sets a numeric `DBTIMEZONE` as `TZ=UTC±N` with the sign inverted is unconfirmed on a database with a numeric OS zone; the parse also accepts `UTC±H:MM` and `Etc/GMT±N`, and an open question stays until a live check.
- [D8] Fold the clock functions and compared TSTZ literals in the adapter in a zone the virtual schema names, not the session zone, because no adapter or UDF request carries the session zone and connect-back opens a different session. ADR: yes, criterion 4 (rejects keeping the functions unadvertised so Exasol evaluates them in the session zone), ADR search: `route-non-emittable-selectlist-to-qualified-wrapper` records the connect-back rejection for TSTZ literals only, no ADR covers the clock functions
