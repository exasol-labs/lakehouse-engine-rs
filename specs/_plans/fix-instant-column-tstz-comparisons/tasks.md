# Tasks: fix-instant-column-tstz-comparisons

- [x] 1. Fold a TSTZ value compared with an instant column to its UTC date and time, with `LOCALTIMESTAMP` an instant on Exasol 8 only; unit test `tstz_values_compared_with_an_instant_column_fold_to_utc` (pushdown-planning-clock-functions, session-time-zone)
- [x] 2. E2E `clock.rs`: oracle TSTZ copy of `TZ`, `a_tstz_value_compared_with_an_instant_column_compares_the_instants`, `a_plain_value_compared_with_an_instant_column_compares_its_utc_date_and_time`, the instant-column query of `an_unreadable_tz_fails_only_a_query_that_needs_it`, and the Dead Code Removal of the `TZ` arms
- [x] 3. E2E `pruning.rs`: the `TZ` case of `CLOCK_CASES` in a `PACIFIC/KIRITIMATI` session with UTC bounds, and the TSTZ-literal case on the zone-less timestamp column
- [x] 4. Docs: `semantic-differences.md` "Instant columns" section and clock section, `capabilities.md`, `tuning.md` (`SESSION_TIME_ZONE`), `capability-matrix.md` (`TIMESTAMP_UTC`)
- [x] 5. Verification: build, unit tests, local E2E, lint, format, manual checks
