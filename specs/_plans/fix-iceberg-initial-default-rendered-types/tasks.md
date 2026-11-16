# Tasks: fix-iceberg-initial-default-rendered-types

- [x] 1. Reproduce #367: extend the `initdef` E2E fixture with `c_time`, REQUIRED `c_time_req`, `c_dec38`, `c_binary`, `c_fixed`, `c_uuid`, assert pre-add equals post-add, run against Docker Exasol and see it fail
- [x] 2. Scan: `reconstruct_initial_default` decodes `binary` and `fixed_size_binary(L)` from hex via the hex branch of `partition_scalar`; extend `initial_default_round_trips_across_full_type_vocabulary` with both tags and a wrong-length `fixed`
- [x] 3. Adapter: make the identity renderer's cause context-free with a neutral name; identity partition caller keeps its partition wording
- [x] 4. Adapter: `encode_initial_default` renders `time`, `decimal(37..38,s)`, `binary`, `fixed`, `uuid` via the shared renderer, checks `fixed(L)` length, and returns an error that `declaration` turns into a column refusal; unit tests `declaration_encodes_every_primitive_initial_default` and `declaration_refuses_a_column_whose_initial_default_cannot_be_rendered`
- [x] 5. Dead code: delete `iceberg_declaration_omits_default_for_decimal_outside_exasol_domain`; update the `LogicalField.initial_default` encoding table in `scan/spec.rs`
- [x] 6. E2E green: rebuild the `.so`, run the `initdef` E2E test and the full local E2E suite
