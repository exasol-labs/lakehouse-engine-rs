# Tasks: add-clock-function-pushdown

- [x] 1. Capture the clock request nodes on Exasol 8.29.13 and 2025.1.16, and the `EXA_TIME_ZONES` lists of both (Implementation step 1)
- [x] 2. Adjust `intent.md` D2 and D4, the spec deltas, and `plan.md` to the captured shapes
- [x] 3. `chrono-tz` workspace dependency (Dependencies)
- [x] 4. `adapter/time_zone.rs`, with the captured `EXA_TIME_ZONES` lists as test data, and its unit tests (Implementation step 2)
- [x] 5. `SESSION_TIME_ZONE` in `adapter/properties.rs`, and its unit tests (Implementation step 3)
- [x] 6. `adapter/pushdown/clock.rs`: the statement clock and the fold, and its unit tests (Implementation step 4)
- [x] 7. Carry the clock in `PushdownContext` and fold in `handle_pushdown` (Implementation step 5)
- [x] 8. Advertise the five capabilities; update `reports_audited_capability_set`, `now_family_falls_through`, and the three comments (Implementation step 6, Dead Code Removal)
- [x] 9. E2E `e2e_local/clock.rs` with its ten scenario tests, declared in `main.rs`
- [x] 10. E2E `clock_and_tstz_filters_prune_files` in `pruning.rs`
- [x] 11. E2E `session_time_zone_accepts_exasol_zone_names_and_rejects_others` and the `SESSION_TIME_ZONE` row of `create_accepts_every_known_property_name` in `property_validation.rs`
- [x] 12. E2E `capability.rs`: rename and extend `e2e_clock_projection_matches_native_session_local_value`, delete `e2e_now_family_matches_native_oracle`
- [x] 13. Docs: `tuning.md`, `semantic-differences.md`, `capabilities.md`, `capability-matrix.md`, `install.md` (Implementation step 7)
