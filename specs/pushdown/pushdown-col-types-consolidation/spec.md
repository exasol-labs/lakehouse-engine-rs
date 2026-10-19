# Feature: Pushdown Col Types Consolidation

Gives the pushdown type-rewrite guards' `col_types` handling one owner per decision: one lookup helper, one builder, and one Exasol-type-family classifier.

## Background

* Resolving a bare column node's Exasol type is two decisions in two owners. `support` owns node, uppercased name, and `col_types` scan. `types::mapping` owns type string to type family. The column-lookup miss stays out of the classifier, which takes a non-optional `&str`. `scan-types/type-mapping-module-structure` owns `ExaTypeClass` and `classify_exa_type`.
* The `col_types` list is produced and consumed with one fold, the Unicode `to_uppercase`. The ASCII fold would differ only for a name with a lowercase non-ASCII letter, and no such name reaches the adapter: `build_listing_virtual_tables` in `adapter/mod.rs` uppercases every declared column name with `to_uppercase` at `createVirtualSchema`. Exasol serves an Iceberg `straße` column as `STRASSE`. `tests/e2e_non_ascii_identifier_test.rs::non_ascii_table_and_column_stay_queryable` guards that premise against a Docker Exasol container.
* A name already produced by `to_uppercase` is a fixed point of both `to_uppercase` and `to_ascii_uppercase`, so the folds agree on every declared name.
* `joins/rendering.rs::referenced_leg_columns` narrows the builder's names by membership in the set `collect_leg_column_names` builds with `to_ascii_uppercase`. If the premise broke, a mixed leg would drop a diverging column from the fan-out leg while the outer wrapper still references it. `referenced_leg_columns` falls back to the full column list only when narrowing yields nothing.
* Four other `col_types` scans are not targets of the lookup helper, because their input is an already-resolved column name rather than a JSON node: `project_columns`'s `type_by_upper` closure, the declined-`ORDER BY` hidden-column appender in `topn.rs`, `col_type_for` in `grouped_agg.rs`, and the logical-field scan in `scan/join_scan.rs`. `parse_sort_key_element` in `topn.rs` resolves an identifier, not a type.
* The builder wrapper supplies one table selection and nothing else (the find-by-name closure), so it is a partial application and not a pass-through.
* The generated SQL stays byte-identical except for the scan spec's `storage` value, which `storage-access/scan-spec-credential-reference` encodes.

## Scenarios

### Scenario: One helper resolves a bare column node's Exasol type for every type-rewrite guard

* *GIVEN* the three per-node type decisions in `pushdown/support.rs`: `guard_like_subject`, `is_bare_decimal_column`, and `coerce_string_position_arg`
* *WHEN* any of the three decides what to do with a bare `column` node
* *THEN* exactly ONE helper in the shared `support` submodule, `column_exa_type`, SHALL own the resolve, taking the node and the `col_types` slice and returning `Option<&str>`, the resolved Exasol type string or nothing, and each of the three guards SHALL retain NO name-folding and NO `col_types` scan of its own
* *AND* the helper MUST NOT test the node's `type` tag, because a non-`column` node is NOT a failed lookup: `guard_like_subject` and `coerce_string_position_arg` return such a node UNCHANGED whereas a failed lookup makes them decline, so each guard SHALL keep its own `type == "column"` test ahead of the call, the pass-through for the two `Option<Json>` guards and `false` for `is_bare_decimal_column`
* *AND* the helper SHALL fold the name with the Unicode `to_uppercase` and MUST NOT use `to_ascii_uppercase`, SHALL return `None` for BOTH a nameless column node and a name absent from `col_types`, and SHALL be private to `support`, because its only three consumers live there
* *AND* for a column name whose Unicode and ASCII folds differ, such as `STRAßE` (`to_uppercase` yields `STRASSE` while `to_ascii_uppercase` leaves it unchanged), the helper MUST resolve a type against a Unicode-folded `col_types` list and MUST return `None` against an ASCII-folded one, and Exasol never delivers such a name, because the adapter uppercases every declared column name at `createVirtualSchema`

### Scenario: One builder produces the column-type list for both the first-table and the named-table selection

* *GIVEN* `involved_columns` in `pushdown/support.rs`, the ONE builder of the `(UPPERCASE name, Exasol type)` list, which takes the request and a table-selection closure and returns the column types together with the column notes in one pass
* *WHEN* the adapter builds the list for a single-table request, or for one named table of a join
* *THEN* `involved_columns` SHALL take exactly ONE parameter besides `request`, the table selection, and SHALL apply the Unicode `str::to_uppercase` in its OWN body, never `str::to_ascii_uppercase`, because `to_uppercase` is the fold `column_exa_type` applies on the consuming side and the fold the adapter applies when it declares each column
* *AND* the single-table path SHALL call `involved_columns` with a first-table selection, and `involved_table_columns` in `pushdown/joins/planning.rs` SHALL stay a partial application that supplies only the find-by-name selection, so it remains a named decision rather than a body forwarding the same arguments
* *AND* the selection parameter SHALL stay a selection CLOSURE and MUST NOT be reshaped into an `Option<&str>` table name or any other data-shaped mode argument
* *AND* the ASCII folds of the join column collectors (`collect_leg_column_names`, `possible_side_column_names`) SHALL stay UNCHANGED, and the agreement of `referenced_leg_columns` with the builder SHALL rest solely on the upstream normalization that every declared name is already Unicode-uppercased and is therefore a fixed point of both folds

### Scenario: The three type-rewrite guards read their type family from the shared classifier

* *GIVEN* the Exasol type families `scan-types/type-mapping-module-structure` records as the `ExaTypeClass` enum and the `classify_exa_type` function in `types/mapping.rs`
* *WHEN* a guard decides what to do with a resolved Exasol type string
* *THEN* each of the three guards SHALL obtain its type family by calling `classify_exa_type` and SHALL retain NO `starts_with` test and NO string equality test against an Exasol type string
* *AND* the classifier's contract SHALL remain owned by `scan-types/type-mapping-module-structure` and is consumed here, NOT restated, so the bare argumentless string `DECIMAL` classifies as DECIMAL in all three guards
* *AND* the two `Option<Json>`-returning guards SHALL each match EXHAUSTIVELY over `Option<ExaTypeClass>` with NO wildcard arm, and their arm sets SHALL differ in exactly ONE arm, `Decimal`: `guard_like_subject` SHALL decline it alongside `Other` and the lookup miss, while `coerce_string_position_arg` SHALL coerce it through `wrap_decimal_to_varchar`, as a visible match arm and not behind a coercion-policy trait, a shared dispatch table, or a parameter
* *AND* `is_bare_decimal_column` SHALL remain a two-way `Decimal`-versus-everything-else test and MAY use a non-exhaustive form, and the column-lookup miss SHALL stay each guard's OWN branch and MUST NOT be routed through the classifier with a placeholder or sentinel string in place of an absent type
