use super::*;
use serde_json::json;

// --- Column ---

#[test]
fn renders_column_as_quoted_uppercase_ident() {
    let expr = json!({"type": "column", "name": "region"});
    let sql = render_expression(&expr).unwrap();
    assert_eq!(sql, r#""REGION""#);
}

#[test]
fn renders_column_with_embedded_quotes() {
    let expr = json!({"type": "column", "name": r#"my"col"#});
    let sql = render_expression(&expr).unwrap();
    // embedded " must be doubled
    assert_eq!(sql, r#""MY""COL""#);
}

#[test]
fn renders_table_qualified_column_when_alias_present() {
    let expr = json!({"type": "column", "name": "id", "tableAlias": "LHS_FACT"});
    let sql = render_expression(&expr).unwrap();
    assert_eq!(sql, r#""LHS_FACT"."ID""#);
}

#[test]
fn empty_table_alias_falls_back_to_bare_column() {
    let expr = json!({"type": "column", "name": "id", "tableAlias": ""});
    let sql = render_expression(&expr).unwrap();
    assert_eq!(sql, r#""ID""#);
}

// --- Literals ---

#[test]
fn renders_string_literal() {
    let expr = json!({"type": "literal_string", "value": "hello"});
    assert_eq!(render_expression(&expr).unwrap(), "'hello'");
}

#[test]
fn renders_string_literal_with_single_quote_escaped() {
    let expr = json!({"type": "literal_string", "value": "it's"});
    assert_eq!(render_expression(&expr).unwrap(), "'it''s'");
}

#[test]
fn renders_null_literal() {
    let expr = json!({"type": "literal_null"});
    assert_eq!(render_expression(&expr).unwrap(), "NULL");
}

#[test]
fn renders_bool_literal() {
    let t = json!({"type": "literal_bool", "value": true});
    let f = json!({"type": "literal_bool", "value": false});
    assert_eq!(render_expression(&t).unwrap(), "TRUE");
    assert_eq!(render_expression(&f).unwrap(), "FALSE");
}

#[test]
fn renders_numeric_literal() {
    let expr = json!({"type": "literal_exactnumeric", "value": 42});
    assert_eq!(render_expression(&expr).unwrap(), "42");
}

#[test]
fn renders_date_literal() {
    let expr = json!({"type": "literal_date", "value": "2024-01-15"});
    assert_eq!(render_expression(&expr).unwrap(), "DATE '2024-01-15'");
}

#[test]
fn renders_timestamp_literal() {
    let expr = json!({"type": "literal_timestamp", "value": "2024-01-15 12:00:00"});
    assert_eq!(
        render_expression(&expr).unwrap(),
        "arrow_cast('2024-01-15 12:00:00', 'Timestamp(Microsecond, None)')"
    );
}

#[test]
fn renders_far_future_timestamp_literal() {
    // Literal reproduction of issue #155's overflow scenario: a bare
    // `TIMESTAMP '...'` form types as Timestamp(Nanosecond) and overflows on
    // far-future values during simplify_expressions; arrow_cast pins
    // microsecond precision so this renders cleanly. Optimizer behavior is
    // covered by `timestamp_literal_precision_test` in `lakehouse-engine`.
    let expr = json!({"type": "literal_timestamp", "value": "9999-12-31 23:59:59"});
    assert_eq!(
        render_expression(&expr).unwrap(),
        "arrow_cast('9999-12-31 23:59:59', 'Timestamp(Microsecond, None)')"
    );
}

// --- Comparison predicates ---

#[test]
fn renders_simple_equality() {
    let expr = json!({
        "type": "predicate_equal",
        "left": {"type": "column", "name": "id"},
        "right": {"type": "literal_exactnumeric", "value": 10}
    });
    assert_eq!(render_expression(&expr).unwrap(), r#"("ID" = 10)"#);
}

// --- Logical connectives ---

#[test]
fn renders_and_predicate() {
    let expr = json!({
        "type": "predicate_and",
        "expressions": [
            {"type": "predicate_greater", "left": {"type": "column", "name": "age"}, "right": {"type": "literal_exactnumeric", "value": 18}},
            {"type": "predicate_less", "left": {"type": "column", "name": "age"}, "right": {"type": "literal_exactnumeric", "value": 65}}
        ]
    });
    let sql = render_expression(&expr).unwrap();
    assert!(sql.contains("AND"), "AND not found in: {sql}");
}

#[test]
fn renders_or_predicate() {
    let expr = json!({
        "type": "predicate_or",
        "expressions": [
            {"type": "predicate_equal", "left": {"type": "column", "name": "status"}, "right": {"type": "literal_string", "value": "A"}},
            {"type": "predicate_equal", "left": {"type": "column", "name": "status"}, "right": {"type": "literal_string", "value": "B"}}
        ]
    });
    let sql = render_expression(&expr).unwrap();
    assert!(sql.contains("OR"), "OR not found in: {sql}");
}

#[test]
fn renders_not_predicate() {
    let expr = json!({
        "type": "predicate_not",
        "expression": {"type": "predicate_equal", "left": {"type": "column", "name": "active"}, "right": {"type": "literal_bool", "value": true}}
    });
    let sql = render_expression(&expr).unwrap();
    assert!(sql.contains("NOT"), "NOT not found in: {sql}");
}

#[test]
fn renders_empty_and_as_true() {
    let expr = json!({"type": "predicate_and", "expressions": []});
    assert_eq!(render_expression(&expr).unwrap(), "TRUE");
}

#[test]
fn renders_empty_or_as_false() {
    let expr = json!({"type": "predicate_or", "expressions": []});
    assert_eq!(render_expression(&expr).unwrap(), "FALSE");
}

// --- IS NULL / IS NOT NULL ---

#[test]
fn renders_is_null() {
    let expr = json!({"type": "predicate_is_null", "expression": {"type": "column", "name": "x"}});
    assert_eq!(render_expression(&expr).unwrap(), r#"("X" IS NULL)"#);
}

#[test]
fn renders_is_not_null() {
    let expr =
        json!({"type": "predicate_is_not_null", "expression": {"type": "column", "name": "x"}});
    assert_eq!(render_expression(&expr).unwrap(), r#"("X" IS NOT NULL)"#);
}

// --- IN ---

#[test]
fn renders_in_constlist() {
    let expr = json!({
        "type": "predicate_in_constlist",
        "expression": {"type": "column", "name": "status"},
        "arguments": [
            {"type": "literal_string", "value": "A"},
            {"type": "literal_string", "value": "B"}
        ]
    });
    let sql = render_expression(&expr).unwrap();
    assert!(sql.contains("IN"), "IN not found: {sql}");
    assert!(sql.contains("'A'"), "'A' not found: {sql}");
    assert!(sql.contains("'B'"), "'B' not found: {sql}");
}

#[test]
fn renders_empty_in_as_false() {
    let expr = json!({
        "type": "predicate_in_constlist",
        "expression": {"type": "column", "name": "x"},
        "arguments": []
    });
    assert_eq!(render_expression(&expr).unwrap(), "FALSE");
}

#[test]
fn renders_in_constlist_strips_null() {
    let expr = json!({
        "type": "predicate_in_constlist",
        "expression": {"type": "column", "name": "status"},
        "arguments": [
            {"type": "literal_string", "value": "A"},
            {"type": "literal_null"},
            {"type": "literal_date", "value": null}
        ]
    });
    let sql = render_expression(&expr).unwrap();
    assert!(sql.contains("'A'"), "'A' not found: {sql}");
    assert!(!sql.contains("NULL"), "NULL should not survive: {sql}");
}

#[test]
fn renders_all_null_in_as_false() {
    let expr = json!({
        "type": "predicate_in_constlist",
        "expression": {"type": "column", "name": "x"},
        "arguments": [
            {"type": "literal_null"},
            {"type": "literal_date", "value": null}
        ]
    });
    assert_eq!(render_expression(&expr).unwrap(), "FALSE");
}

#[test]
fn renders_not_in_constlist_strips_null() {
    let expr = json!({
        "type": "predicate_not",
        "expression": {
            "type": "predicate_in_constlist",
            "expression": {"type": "column", "name": "status"},
            "arguments": [
                {"type": "literal_string", "value": "A"},
                {"type": "literal_null"},
                {"type": "literal_date", "value": null}
            ]
        }
    });
    let sql = render_expression(&expr).unwrap();
    assert_eq!(sql, r#"(NOT ("STATUS" IN ('A')))"#);
}

// --- BETWEEN ---

#[test]
fn renders_between() {
    let expr = json!({
        "type": "predicate_between",
        "expression": {"type": "column", "name": "age"},
        "left": {"type": "literal_exactnumeric", "value": 18},
        "right": {"type": "literal_exactnumeric", "value": 65}
    });
    let sql = render_expression(&expr).unwrap();
    assert!(sql.contains("BETWEEN"), "BETWEEN not found: {sql}");
    assert!(sql.contains("18"), "low bound not found: {sql}");
    assert!(sql.contains("65"), "high bound not found: {sql}");
}

// --- LIKE ---

#[test]
fn renders_like_without_escape() {
    let expr = json!({
        "type": "predicate_like",
        "expression": {"type": "column", "name": "name"},
        "pattern": {"type": "literal_string", "value": "A%"}
    });
    let sql = render_expression(&expr).unwrap();
    assert!(sql.contains("LIKE"), "LIKE not found: {sql}");
    assert!(!sql.contains("ESCAPE"), "ESCAPE should be absent: {sql}");
}

#[test]
fn renders_like_with_escape() {
    let expr = json!({
        "type": "predicate_like",
        "expression": {"type": "column", "name": "name"},
        "pattern": {"type": "literal_string", "value": "A!%"},
        "escape_char": "!"
    });
    let sql = render_expression(&expr).unwrap();
    assert!(sql.contains("LIKE"), "LIKE not found: {sql}");
    assert!(sql.contains("ESCAPE"), "ESCAPE not found: {sql}");
    assert!(sql.contains("'!'"), "escape char not found: {sql}");
}

// --- Arithmetic ---

#[test]
fn renders_arithmetic_add() {
    let expr = json!({
        "type": "function_scalar",
        "name": "ADD",
        "arguments": [
            {"type": "column", "name": "a"},
            {"type": "literal_exactnumeric", "value": 1}
        ]
    });
    assert_eq!(render_expression(&expr).unwrap(), r#"("A" + 1)"#);
}

#[test]
fn renders_arithmetic_sub() {
    let expr = json!({
        "type": "function_scalar",
        "name": "SUB",
        "arguments": [
            {"type": "column", "name": "a"},
            {"type": "literal_exactnumeric", "value": 1}
        ]
    });
    assert_eq!(render_expression(&expr).unwrap(), r#"("A" - 1)"#);
}

#[test]
fn renders_arithmetic_mul() {
    // Exasol emits the multiplication node as "MULT" (from capability FN_MULT),
    // not "MUL" — verified via the FN_-strip convention in decision-log [7].
    let expr = json!({
        "type": "function_scalar",
        "name": "MULT",
        "arguments": [
            {"type": "column", "name": "a"},
            {"type": "literal_exactnumeric", "value": 2}
        ]
    });
    assert_eq!(render_expression(&expr).unwrap(), r#"("A" * 2)"#);
}

/// Regression guard: the legacy node name "MUL" must NOT be recognized. Exasol
/// never emits it (the capability is FN_MULT → node "MULT"); if the match arm
/// ever regresses back to "MUL", the advertised set and the translator would
/// silently diverge and multiplication pushdown would fall back to a row scan.
#[test]
fn legacy_mul_name_is_not_recognized() {
    let expr = json!({
        "type": "function_scalar",
        "name": "MUL",
        "arguments": [
            {"type": "column", "name": "a"},
            {"type": "column", "name": "b"}
        ]
    });
    assert!(
        render_expression_safe(&expr).is_none(),
        "the obsolete \"MUL\" node name must not translate; Exasol emits \"MULT\""
    );
}

/// Two-column binary arithmetic (both operands are column references), the exact
/// NQ1 shape `L_EXTENDEDPRICE * L_DISCOUNT`. This is what unblocks the two-column
/// SUM(col * col) pushdown once FN_MULT is advertised (capabilities.rs, task 1.2):
/// the expression-argument aggregate path renders this fragment for the scan SQL.
#[test]
fn renders_two_column_arithmetic_product() {
    let expr = json!({
        "type": "function_scalar",
        "name": "MULT",
        "arguments": [
            {"type": "column", "name": "l_extendedprice"},
            {"type": "column", "name": "l_discount"}
        ]
    });
    assert_eq!(
        render_expression(&expr).unwrap(),
        r#"("L_EXTENDEDPRICE" * "L_DISCOUNT")"#
    );
}

/// Lockstep guard (translator side): the arithmetic binary-operator node names the
/// translator recognizes must correspond 1:1 to the arithmetic capabilities
/// advertised in `crates/lakehouse-engine/src/adapter/capabilities.rs`
/// (`FN_ADD`, `FN_SUB`, `FN_MULT`, `FN_FLOAT_DIV`) — each capability name with the
/// `FN_` prefix stripped. If capabilities advertises an operator the translator
/// doesn't render (or renders a name that isn't advertised), Exasol either declines
/// the pushdown (silent row-scan fallback, no speedup) or the fragment never reaches
/// a live query. Both operands are columns to exercise the two-column shape.
///
/// The advertised capability strings live in a different crate; the authoritative
/// cross-crate assertion (reading `CAPABILITIES` and driving `render_expression`)
/// is deferred until task 1.2 populates the const — see decision-log deferred note.
/// This table is the translator-side half kept in sync by construction.
#[test]
fn arithmetic_operator_set_matches_advertised_capabilities() {
    // (capability name, node name = capability minus FN_, expected rendering)
    let arithmetic = [
        (
            "FN_ADD",
            "ADD",
            r#"("L_EXTENDEDPRICE" + "L_DISCOUNT")"#.to_string(),
        ),
        (
            "FN_SUB",
            "SUB",
            r#"("L_EXTENDEDPRICE" - "L_DISCOUNT")"#.to_string(),
        ),
        (
            "FN_MULT",
            "MULT",
            r#"("L_EXTENDEDPRICE" * "L_DISCOUNT")"#.to_string(),
        ),
        (
            "FN_FLOAT_DIV",
            "FLOAT_DIV",
            r#"(CAST("L_EXTENDEDPRICE" AS DOUBLE) / "L_DISCOUNT")"#.to_string(),
        ),
    ];
    for (cap, node, expected) in arithmetic {
        // node name must be the capability with the FN_ prefix removed
        assert_eq!(
            node,
            cap.strip_prefix("FN_").unwrap(),
            "node name must equal capability {cap} minus FN_ prefix"
        );
        let expr = json!({
            "type": "function_scalar",
            "name": node,
            "arguments": [
                {"type": "column", "name": "l_extendedprice"},
                {"type": "column", "name": "l_discount"}
            ]
        });
        assert_eq!(
            render_expression(&expr).unwrap(),
            expected,
            "translator must render advertised capability {cap} (node {node})"
        );
    }
}

#[test]
fn renders_arithmetic_div() {
    let expr = json!({
        "type": "function_scalar",
        "name": "FLOAT_DIV",
        "arguments": [
            {"type": "column", "name": "a"},
            {"type": "literal_exactnumeric", "value": 2}
        ]
    });
    assert_eq!(
        render_expression(&expr).unwrap(),
        r#"(CAST("A" AS DOUBLE) / 2)"#
    );
}

#[test]
fn float_div_casts_column_left_operand_against_column_right_operand() {
    let expr = json!({
        "type": "function_scalar",
        "name": "FLOAT_DIV",
        "arguments": [
            {"type": "column", "name": "a"},
            {"type": "column", "name": "b"}
        ]
    });
    assert_eq!(
        render_expression(&expr).unwrap(),
        r#"(CAST("A" AS DOUBLE) / "B")"#
    );
}

#[test]
fn float_div_casts_literal_left_operand_against_column_right_operand() {
    let expr = json!({
        "type": "function_scalar",
        "name": "FLOAT_DIV",
        "arguments": [
            {"type": "literal_exactnumeric", "value": 10},
            {"type": "column", "name": "b"}
        ]
    });
    assert_eq!(
        render_expression(&expr).unwrap(),
        r#"(CAST(10 AS DOUBLE) / "B")"#
    );
}

#[test]
fn float_div_casts_literal_left_operand_against_literal_right_operand() {
    let expr = json!({
        "type": "function_scalar",
        "name": "FLOAT_DIV",
        "arguments": [
            {"type": "literal_exactnumeric", "value": 10},
            {"type": "literal_exactnumeric", "value": 4}
        ]
    });
    assert_eq!(
        render_expression(&expr).unwrap(),
        r#"(CAST(10 AS DOUBLE) / 4)"#
    );
}

#[test]
fn float_div_casts_nested_expression_left_operand_against_column_right_operand() {
    let expr = json!({
        "type": "function_scalar",
        "name": "FLOAT_DIV",
        "arguments": [
            {
                "type": "function_scalar",
                "name": "ADD",
                "arguments": [
                    {"type": "column", "name": "a"},
                    {"type": "column", "name": "b"}
                ]
            },
            {"type": "column", "name": "c"}
        ]
    });
    assert_eq!(
        render_expression(&expr).unwrap(),
        r#"(CAST(("A" + "B") AS DOUBLE) / "C")"#
    );
}

#[test]
fn float_div_casts_nested_expression_left_operand_against_literal_right_operand() {
    let expr = json!({
        "type": "function_scalar",
        "name": "FLOAT_DIV",
        "arguments": [
            {
                "type": "function_scalar",
                "name": "ADD",
                "arguments": [
                    {"type": "column", "name": "a"},
                    {"type": "column", "name": "b"}
                ]
            },
            {"type": "literal_exactnumeric", "value": 2}
        ]
    });
    assert_eq!(
        render_expression(&expr).unwrap(),
        r#"(CAST(("A" + "B") AS DOUBLE) / 2)"#
    );
}

#[test]
fn float_div_casts_aggregate_left_operand_against_column_right_operand() {
    let expr = json!({
        "type": "function_scalar",
        "name": "FLOAT_DIV",
        "arguments": [
            {
                "type": "function_aggregate",
                "name": "SUM",
                "arguments": [{"type": "column", "name": "amount"}],
                "distinct": false
            },
            {"type": "column", "name": "c"}
        ]
    });
    assert_eq!(
        render_expression(&expr).unwrap(),
        r#"(CAST(SUM("AMOUNT") AS DOUBLE) / "C")"#
    );
}

#[test]
fn float_div_casts_aggregate_left_operand_against_literal_right_operand() {
    let expr = json!({
        "type": "function_scalar",
        "name": "FLOAT_DIV",
        "arguments": [
            {
                "type": "function_aggregate",
                "name": "SUM",
                "arguments": [{"type": "column", "name": "amount"}],
                "distinct": false
            },
            {"type": "literal_exactnumeric", "value": 2}
        ]
    });
    assert_eq!(
        render_expression(&expr).unwrap(),
        r#"(CAST(SUM("AMOUNT") AS DOUBLE) / 2)"#
    );
}

#[test]
fn float_div_with_null_left_operand_casts_the_null_literal() {
    let expr = json!({
        "type": "function_scalar",
        "name": "FLOAT_DIV",
        "arguments": [
            {"type": "literal_null"},
            {"type": "column", "name": "b"}
        ]
    });
    assert_eq!(
        render_expression(&expr).unwrap(),
        r#"(CAST(NULL AS DOUBLE) / "B")"#
    );
}

#[test]
fn float_div_with_null_right_operand_divides_by_null() {
    let expr = json!({
        "type": "function_scalar",
        "name": "FLOAT_DIV",
        "arguments": [
            {"type": "column", "name": "a"},
            {"type": "literal_null"}
        ]
    });
    assert_eq!(
        render_expression(&expr).unwrap(),
        r#"(CAST("A" AS DOUBLE) / NULL)"#
    );
}

#[test]
fn float_div_null_over_zero_casts_the_null_literal_over_a_zero_literal() {
    let expr = json!({
        "type": "function_scalar",
        "name": "FLOAT_DIV",
        "arguments": [
            {"type": "literal_null"},
            {"type": "literal_exactnumeric", "value": 0}
        ]
    });
    assert_eq!(
        render_expression(&expr).unwrap(),
        r#"(CAST(NULL AS DOUBLE) / 0)"#
    );
}

#[test]
fn renders_arithmetic_neg() {
    let expr = json!({
        "type": "function_scalar",
        "name": "NEG",
        "arguments": [
            {"type": "column", "name": "a"}
        ]
    });
    assert_eq!(render_expression(&expr).unwrap(), r#"(-"A")"#);
}

#[test]
fn neg_composes_with_aggregate_decomposition() {
    // SUM(-col): the NEG arm must render correctly as an aggregate argument,
    // not only standalone. `function_aggregate` recurses into its argument
    // (the same arithmetic-aggregate decomposition path exercised by
    // `render_expression_renders_scalar_wrapping_aggregates`), so a NEG node
    // nested under SUM must flow through unchanged.
    let sum_neg = json!({
        "type": "function_aggregate",
        "name": "SUM",
        "arguments": [{
            "type": "function_scalar",
            "name": "NEG",
            "arguments": [{"type": "column", "name": "col"}]
        }],
        "distinct": false
    });
    assert_eq!(render_expression(&sum_neg).unwrap(), r#"SUM((-"COL"))"#);
}

// --- CAST ---
//
// Fixtures use the real Exasol wire shape `{"type":"function_scalar_cast",
// "name":"CAST","dataType":{...},"arguments":[...]}` — the shape the engine
// actually emits, NOT the earlier `{"type":"function_scalar",...}` shape
// whose mismatch let a dispatch bug hide (CAST never reached its arm).

#[test]
fn renders_cast_varchar() {
    let expr = json!({
        "type": "function_scalar_cast",
        "name": "CAST",
        "arguments": [{"type": "column", "name": "x"}],
        "dataType": {"type": "VARCHAR", "size": 100}
    });
    assert_eq!(render_expression(&expr).unwrap(), r#"CAST("X" AS VARCHAR)"#);
}

#[test]
fn renders_cast_decimal() {
    let expr = json!({
        "type": "function_scalar_cast",
        "name": "CAST",
        "arguments": [{"type": "column", "name": "x"}],
        "dataType": {"type": "DECIMAL", "precision": 10, "scale": 2}
    });
    assert_eq!(
        render_expression(&expr).unwrap(),
        r#"CAST("X" AS DECIMAL(10,2))"#
    );
}

#[test]
fn renders_cast_double() {
    let expr = json!({
        "type": "function_scalar_cast",
        "name": "CAST",
        "arguments": [{"type": "column", "name": "x"}],
        "dataType": {"type": "DOUBLE"}
    });
    assert_eq!(render_expression(&expr).unwrap(), r#"CAST("X" AS DOUBLE)"#);
}

#[test]
fn renders_cast_date() {
    let expr = json!({
        "type": "function_scalar_cast",
        "name": "CAST",
        "arguments": [{"type": "column", "name": "x"}],
        "dataType": {"type": "DATE"}
    });
    assert_eq!(render_expression(&expr).unwrap(), r#"CAST("X" AS DATE)"#);
}

#[test]
fn renders_cast_char_as_datafusion_varchar() {
    // Exasol sends CHAR as {"type":"CHAR","size":n,"characterSet":...}. The
    // DataFusion dialect renders that target as a bare, length-less VARCHAR:
    // Arrow has only Utf8 (no CHAR type) and datafusion-sql rejects a length
    // on a character target. The Exasol dialect deliberately DIVERGES and
    // renders CHAR(n) — see `renders_cast_char_as_exasol_char`.
    let expr = json!({
        "type": "function_scalar_cast",
        "name": "CAST",
        "arguments": [{"type": "column", "name": "x"}],
        "dataType": {"type": "CHAR", "size": 3, "characterSet": "ASCII"}
    });
    assert_eq!(render_expression(&expr).unwrap(), r#"CAST("X" AS VARCHAR)"#);
}

#[test]
fn renders_cast_char_as_exasol_char() {
    // The Exasol-dialect twin of `renders_cast_char_as_datafusion_varchar`:
    // Exasol declares a CHAR-target result column CHAR(n) and validates the
    // pushdown positionally against that declaration, so the same node must
    // render CHAR(n) — carrying the ` ASCII` suffix Exasol declared — rather
    // than collapsing to VARCHAR(n) (#192).
    let expr = json!({
        "type": "function_scalar_cast",
        "name": "CAST",
        "arguments": [{"type": "column", "name": "x"}],
        "dataType": {"type": "CHAR", "size": 3, "characterSet": "ASCII"}
    });
    assert_eq!(
        render_expression_exasol(&expr).unwrap(),
        r#"CAST("X" AS CHAR(3) ASCII)"#
    );
}

#[test]
fn renders_cast_bool_to_varchar_as_exasol_case_uppercase() {
    // #200: CAST(<bool> AS VARCHAR) must render Exasol's TRUE/FALSE
    // casing, not DataFusion's lowercase boolean->Utf8 cast.
    let expr = json!({
        "type": "function_scalar_cast",
        "name": "CAST",
        "arguments": [{
            "type": "predicate_greater",
            "left": {"type": "column", "name": "c_acctbal"},
            "right": {"type": "literal_exactnumeric", "value": 0}
        }],
        "dataType": {"type": "VARCHAR", "size": 10}
    });
    assert_eq!(
        render_expression(&expr).unwrap(),
        r#"(CASE ("C_ACCTBAL" > 0) WHEN TRUE THEN 'TRUE' WHEN FALSE THEN 'FALSE' ELSE NULL END)"#
    );
}

#[test]
fn renders_cast_bool_to_varchar_uses_case_for_any_predicate_source() {
    // A boolean-producing predicate other than a comparison (here
    // `predicate_is_null`) is detected the same way, confirming the CASE
    // rewrite isn't special-cased to `predicate_greater` alone. Runtime
    // NULL-preservation itself (a NULL comparison falling through the
    // CASE's `ELSE NULL`, never 'NULL' or a coerced 'FALSE') is exercised
    // end-to-end in `boolean_to_string_casing_test.rs`.
    let expr = json!({
        "type": "function_scalar_cast",
        "name": "CAST",
        "arguments": [{
            "type": "predicate_is_null",
            "expression": {"type": "column", "name": "x"}
        }],
        "dataType": {"type": "VARCHAR", "size": 10}
    });
    assert_eq!(
        render_expression(&expr).unwrap(),
        r#"(CASE ("X" IS NULL) WHEN TRUE THEN 'TRUE' WHEN FALSE THEN 'FALSE' ELSE NULL END)"#
    );
}

#[test]
fn renders_cast_boolean() {
    let expr = json!({
        "type": "function_scalar_cast",
        "name": "CAST",
        "arguments": [{"type": "column", "name": "x"}],
        "dataType": {"type": "BOOLEAN"}
    });
    assert_eq!(render_expression(&expr).unwrap(), r#"CAST("X" AS BOOLEAN)"#);
}

#[test]
fn renders_cast_timestamp_without_local_time_zone() {
    // Plain TIMESTAMP with a present fractionalSecondsPrecision of 3:
    // Exasol sends {"type":"TIMESTAMP","withLocalTimeZone":false,
    // "fractionalSecondsPrecision":3}. A present precision now renders
    // verbatim `TIMESTAMP(3)` (issue #212); 3 is a DataFusion-supported unit,
    // so the DataFusion dialect renders it identically (identity snap).
    let expr = json!({
        "type": "function_scalar_cast",
        "name": "CAST",
        "arguments": [{"type": "column", "name": "x"}],
        "dataType": {"type": "TIMESTAMP", "withLocalTimeZone": false, "fractionalSecondsPrecision": 3}
    });
    assert_eq!(
        render_expression(&expr).unwrap(),
        r#"CAST("X" AS TIMESTAMP(3))"#
    );
}

#[test]
fn renders_cast_timestamp_precision_per_dialect() {
    // Build a CAST-to-TIMESTAMP expression node with the given dataType.
    fn cast(data_type: Json) -> Json {
        json!({
            "type": "function_scalar_cast",
            "name": "CAST",
            "arguments": [{"type": "column", "name": "x"}],
            "dataType": data_type
        })
    }

    // Exasol dialect renders any precision 0-9 VERBATIM (Exasol's parser
    // accepts every fractional-seconds precision).
    for p in [0u64, 6, 9] {
        let expr = cast(json!({"type": "TIMESTAMP", "fractionalSecondsPrecision": p}));
        assert_eq!(
            render_expression_exasol(&expr).unwrap(),
            format!(r#"CAST("X" AS TIMESTAMP({p}))"#),
            "Exasol dialect must render TIMESTAMP({p}) verbatim"
        );
    }

    // DataFusion dialect renders a supported precision VERBATIM (identity
    // snap for 6).
    let expr = cast(json!({"type": "TIMESTAMP", "fractionalSecondsPrecision": 6}));
    assert_eq!(
        render_expression(&expr).unwrap(),
        r#"CAST("X" AS TIMESTAMP(6))"#
    );

    // DataFusion dialect SNAPS an unsupported precision to the nearest
    // supported unit: 5 -> 6 (DataFusion 54 parses TIMESTAMP(p) only for
    // {0,3,6,9}).
    let expr = cast(json!({"type": "TIMESTAMP", "fractionalSecondsPrecision": 5}));
    assert_eq!(
        render_expression(&expr).unwrap(),
        r#"CAST("X" AS TIMESTAMP(6))"#
    );

    // Absent precision renders bare TIMESTAMP in BOTH dialects (unchanged),
    // whether the dataType omits withLocalTimeZone entirely or sets it false.
    for data_type in [
        json!({"type": "TIMESTAMP"}),
        json!({"type": "TIMESTAMP", "withLocalTimeZone": false}),
    ] {
        let expr = cast(data_type.clone());
        assert_eq!(
            render_expression(&expr).unwrap(),
            r#"CAST("X" AS TIMESTAMP)"#,
            "DataFusion dialect: absent precision must render bare TIMESTAMP for {data_type}"
        );
        assert_eq!(
            render_expression_exasol(&expr).unwrap(),
            r#"CAST("X" AS TIMESTAMP)"#,
            "Exasol dialect: absent precision must render bare TIMESTAMP for {data_type}"
        );
    }
}

#[test]
fn cast_to_unsupported_target_declines() {
    // Exasol CAST targets with no faithful DataFusion 54 equivalent. Each is
    // sent with the dataType descriptor shape shown (verified against the
    // Exasol virtual-schema data-types API). The translator declines these
    // targets (Err in raising mode, None in safe mode); there is no
    // Exasol-side re-check of an advertised capability, so it is the
    // caller's job to decide what to do with that `None`/`Err` — the
    // adapter's declined-predicate route errors rather than omitting the
    // CAST.
    //
    // TIMESTAMP WITH LOCAL TIME ZONE is the trap: Exasol serialises it as
    // type "TIMESTAMP" with `withLocalTimeZone: true` — NOT a distinct type
    // string — so a naive "TIMESTAMP" arm would silently render it as plain
    // TIMESTAMP and drop its session-timezone/UTC-normalisation semantics.
    let unsupported = [
        json!({"type": "INTERVAL", "fromTo": "YEAR TO MONTH", "precision": 2}),
        json!({"type": "INTERVAL", "fromTo": "DAY TO SECONDS", "precision": 2, "fraction": 2}),
        json!({"type": "GEOMETRY", "srid": 4326}),
        json!({"type": "HASHTYPE", "bytesize": 16}),
        json!({"type": "TIMESTAMP", "withLocalTimeZone": true, "fractionalSecondsPrecision": 9}),
    ];
    for data_type in unsupported {
        let expr = json!({
            "type": "function_scalar_cast",
            "name": "CAST",
            "arguments": [{"type": "column", "name": "x"}],
            "dataType": data_type.clone()
        });
        assert!(
            render_expression(&expr).is_err(),
            "CAST to {data_type} must raise in raising mode"
        );
        assert!(
            render_expression_safe(&expr).is_none(),
            "CAST to {data_type} must be None in safe mode"
        );
    }
}

#[test]
fn renders_cast_nested_function_scalar_defensive() {
    // Defensive alternate encoding: CAST nested inside a generic
    // `function_scalar` node. Real Exasol traffic uses `function_scalar_cast`
    // (see the fixtures above), but the nested arm is kept — like the
    // REGEXP_LIKE alternate encoding — and must still render identically via
    // the shared `render_cast` body.
    let expr = json!({
        "type": "function_scalar",
        "name": "CAST",
        "arguments": [{"type": "column", "name": "x"}],
        "dataType": {"type": "VARCHAR", "size": 100}
    });
    assert_eq!(render_expression(&expr).unwrap(), r#"CAST("X" AS VARCHAR)"#);
}

// --- decimal_to_varchar_exasol (issue #211) ---
//
// Adapter-synthesized node, never sent by Exasol on the wire. Fixtures
// exercise the render arm in isolation (arity + wrapping), independent of
// the adapter-side rewrite that synthesizes this node (a later task).

#[test]
fn renders_decimal_to_varchar_exasol() {
    let expr = json!({
        "type": "decimal_to_varchar_exasol",
        "arguments": [{"type": "column", "name": "c_decimal_a"}]
    });
    let expected = format_decimal_exasol_style(r#""C_DECIMAL_A""#);
    assert_eq!(render_expression(&expr).unwrap(), expected);
    assert_eq!(render_expression_safe(&expr).unwrap(), expected);
}

#[test]
fn decimal_to_varchar_exasol_wrong_arity_errors() {
    let no_args = json!({
        "type": "decimal_to_varchar_exasol",
        "arguments": []
    });
    let two_args = json!({
        "type": "decimal_to_varchar_exasol",
        "arguments": [
            {"type": "column", "name": "c_decimal_a"},
            {"type": "column", "name": "c_decimal_b"}
        ]
    });
    for expr in [&no_args, &two_args] {
        assert!(
            render_expression(expr).is_err(),
            "decimal_to_varchar_exasol with non-unary arguments must raise: {expr}"
        );
        assert!(
            render_expression_safe(expr).is_none(),
            "decimal_to_varchar_exasol with non-unary arguments must be None in safe mode: {expr}"
        );
    }
}

#[test]
fn format_decimal_exasol_style_renders_exact_regex_sql() {
    // Pins the emitted SQL text itself (no DataFusion runtime involved).
    // Runtime correctness of this regex against a real engine is covered
    // by the E2E tests in `crates/lakehouse-engine/tests/e2e_capability_test.rs`
    // (`e2e_decimal_cast_trims_trailing_zeros` and friends).
    assert_eq!(
        format_decimal_exasol_style("some_col"),
        r#"regexp_replace(regexp_replace(CAST(some_col AS VARCHAR), '(\.[0-9]*[1-9])0+$', '\1'), '\.0+$', '')"#
    );
}

// --- Error / safe-mode ---

#[test]
fn unsupported_node_returns_error() {
    let expr = json!({"type": "fn_sum", "operands": []});
    let err = render_expression(&expr).unwrap_err();
    assert!(
        err.to_string().contains("fn_sum"),
        "error must name the unsupported type: {err}"
    );
}

#[test]
fn unsupported_node_returns_none_in_safe_mode() {
    let expr = json!({"type": "fn_sum", "operands": []});
    assert!(render_expression_safe(&expr).is_none());
}

#[test]
fn true_filter_returns_none_in_safe_mode() {
    let expr = json!({"type": "literal_bool", "value": true});
    assert!(render_df_filter_safe(&expr).is_none());
}

#[test]
fn null_filter_returns_none_in_safe_mode() {
    let expr = json!({"type": "literal_null"});
    assert!(render_df_filter_safe(&expr).is_none());
}

// --- UTC timestamp literal ---

#[test]
fn renders_timestamp_utc_literal() {
    let expr = json!({"type": "literal_timestamp_utc", "value": "2024-03-01 10:00:00"});
    let sql = render_expression(&expr).unwrap();
    assert_eq!(
        sql,
        "arrow_cast('2024-03-01 10:00:00+00:00', 'Timestamp(Microsecond, Some(\"UTC\"))')"
    );
}

#[test]
fn renders_timestamp_literals_as_bare_timestamp_in_exasol_dialect() {
    // `arrow_cast` is DataFusion-only: Exasol's core engine rejects the
    // wrapper SQL with "function or script ARROW_CAST not found" (42000,
    // verified on live Exasol 2025.2.1). The Exasol dialect re-emits the bare
    // `TIMESTAMP '<value>'` literal Exasol's own compiler sent, while the
    // DataFusion rendering stays byte-identical so the scan keeps the
    // explicit microsecond typing that issue #155 depends on.
    //
    // `literal_timestamp_utc` is NOT bare in the Exasol dialect — see
    // `renders_timestamp_utc_literal_via_convert_tz_in_exasol_dialect` below.
    let ts = json!({"type": "literal_timestamp", "value": "2024-01-15 12:00:00"});
    let ts_utc = json!({"type": "literal_timestamp_utc", "value": "2024-03-01 10:00:00"});

    let ts_exasol = render_expression_exasol(&ts).unwrap();
    assert_eq!(ts_exasol, "TIMESTAMP '2024-01-15 12:00:00'");
    assert!(
        !ts_exasol.contains("arrow_cast"),
        "Exasol rejects arrow_cast with sqlCode 42000: {ts_exasol}"
    );

    // Internal single quotes are doubled, exactly as for `literal_string`, so
    // no literal value can terminate the quoted literal early.
    assert_eq!(
        render_expression_exasol(&json!({
            "type": "literal_timestamp",
            "value": "2024-01-15 12:00:00' OR '1'='1"
        }))
        .unwrap(),
        "TIMESTAMP '2024-01-15 12:00:00'' OR ''1''=''1'"
    );

    // The DataFusion dialect is frozen.
    assert_eq!(
        render_expression(&ts).unwrap(),
        "arrow_cast('2024-01-15 12:00:00', 'Timestamp(Microsecond, None)')"
    );
    assert_eq!(
        render_expression(&ts_utc).unwrap(),
        "arrow_cast('2024-03-01 10:00:00+00:00', 'Timestamp(Microsecond, Some(\"UTC\"))')"
    );
}

#[test]
fn renders_timestamp_utc_literal_via_convert_tz_in_exasol_dialect() {
    // The wire value is UTC-normalized (Exasol's TIMESTAMP literal format,
    // `YYYY-MM-DD HH24:MI:SS.FF9`, has no offset field and rejects one with
    // sqlCode 22018, so the value carries no zone marker of its own).
    // Converting it into SESSIONTIMEZONE and re-declaring it TSTZ reproduces
    // the value Exasol's own engine computes for the equivalent native
    // expression — verified live (#218): a BARE comparison of this literal
    // against a plain-TIMESTAMP column disagrees with Exasol's own
    // TIMESTAMP-vs-TSTZ coercion rule (session-local interpretation of the
    // naive side), so the Exasol dialect must NOT render it bare like
    // `literal_timestamp` does.
    let value = "2024-03-01 10:00:00";
    let ts_utc = json!({"type": "literal_timestamp_utc", "value": value});

    let exasol = render_expression_exasol(&ts_utc).unwrap();
    assert_eq!(
        exasol,
        "CAST(CONVERT_TZ(TIMESTAMP '2024-03-01 10:00:00', 'UTC', SESSIONTIMEZONE) \
         AS TIMESTAMP WITH LOCAL TIME ZONE)"
    );
    assert!(
        !exasol.contains("+00:00"),
        "Exasol rejects an offset in a TIMESTAMP literal with sqlCode 22018: {exasol}"
    );

    assert!(
        render_expression(&ts_utc).unwrap().contains("+00:00"),
        "the DataFusion dialect keeps the offset that types the literal UTC"
    );
}

#[test]
fn literal_timestamputc_wire_name_renders_exasol_only() {
    // Exasol's real wire node name for a TSTZ literal is `literal_timestamputc`
    // (no underscore before `utc`) — `literal_timestamp_utc` above never
    // matches real traffic (#242). The Exasol dialect accepts the real name
    // and renders it identically to `literal_timestamp_utc`.
    let value = "2024-03-01 09:00:00";
    let real_wire_name = json!({"type": "literal_timestamputc", "value": value});
    assert_eq!(
        render_expression_exasol(&real_wire_name).unwrap(),
        "CAST(CONVERT_TZ(TIMESTAMP '2024-03-01 09:00:00', 'UTC', SESSIONTIMEZONE) \
         AS TIMESTAMP WITH LOCAL TIME ZONE)"
    );

    // The DataFusion dialect keeps declining it — the SAME unmatched/`None`
    // outcome as an entirely unknown node type — so the pushed `ScanSpec.filter`
    // stays byte-identical for every request. Locked here so a later change
    // cannot silently widen the scan filter without also touching this test
    // (#242 stays a deliberate, tracked deferral, not accepted by accident).
    assert!(render_expression_safe(&real_wire_name).is_none());
}

#[test]
fn renders_null_valued_tstz_literal_bare_in_exasol_dialect() {
    // NULL stays bare (no CONVERT_TZ/CAST) for both TSTZ literal node
    // types: CAST/CONVERT_TZ add nothing to a three-valued NULL comparison
    // or projection, and `TIMESTAMP NULL` is a syntax error on Exasol
    // (`unexpected TIMESTAMP_`, 42000), so `render_exasol_timestamp_literal`'s
    // existing bare-`NULL` short-circuit is reused rather than duplicated.
    for node_type in ["literal_timestamp_utc", "literal_timestamputc"] {
        for node in [
            json!({"type": node_type, "value": null}),
            json!({"type": node_type}),
        ] {
            assert_eq!(
                render_expression_exasol(&node).unwrap(),
                "NULL",
                "{node_type} with a null/absent value must render bare NULL"
            );
        }
    }
}

#[test]
fn renders_null_valued_timestamp_literal_per_dialect() {
    // `TIMESTAMP NULL` is a syntax error on Exasol ("unexpected TIMESTAMP_",
    // 42000, verified live on 2025.2.1), so an absent or JSON-null `value`
    // renders as the bare NULL keyword rather than as a typed literal.
    //
    // The DataFusion dialect is ASYMMETRIC across the two node types, and
    // that asymmetry is frozen rather than a defect to align:
    // `literal_timestamp` wraps the NULL keyword in `arrow_cast`, while
    // `literal_timestamp_utc` short-circuits to bare `NULL` before it can
    // build a cast. Pinning both stops a later reader from "fixing" one to
    // match the other and silently changing a frozen DataFusion rendering.
    let cases = [
        (
            "literal_timestamp",
            "arrow_cast(NULL, 'Timestamp(Microsecond, None)')",
        ),
        ("literal_timestamp_utc", "NULL"),
    ];
    for (node_type, expected_datafusion) in cases {
        for (variant, node) in [
            (
                "carrying a JSON-null value",
                json!({"type": node_type, "value": null}),
            ),
            ("with no value key at all", json!({"type": node_type})),
        ] {
            assert_eq!(
                render_expression_exasol(&node).unwrap(),
                "NULL",
                "{node_type} {variant}, Exasol dialect"
            );
            assert_eq!(
                render_expression(&node).unwrap(),
                expected_datafusion,
                "{node_type} {variant}, DataFusion dialect"
            );
        }
    }
}

// --- REGEXP_LIKE predicate and function_scalar ---

#[test]
fn renders_regexp_like() {
    // Test as predicate node (Exasol's infix REGEXP_LIKE encoding)
    let expr = json!({
        "type": "predicate_like_regexp",
        "expression": {"type": "column", "name": "name"},
        "pattern": {"type": "literal_string", "value": "^A.*"}
    });
    let sql = render_expression(&expr).unwrap();
    assert_eq!(sql, r#"regexp_like("NAME", '^A.*')"#);

    // Test as function_scalar REGEXP_LIKE
    let expr2 = json!({
        "type": "function_scalar",
        "name": "REGEXP_LIKE",
        "arguments": [
            {"type": "column", "name": "name"},
            {"type": "literal_string", "value": "^B.*"}
        ]
    });
    let sql2 = render_expression(&expr2).unwrap();
    assert_eq!(sql2, r#"regexp_like("NAME", '^B.*')"#);
}

#[test]
fn renders_regexp_like_as_infix_predicate_in_exasol_dialect() {
    // Exasol's parser has no `regexp_like(...)` function; it accepts only the
    // infix `(<subject> REGEXP_LIKE <pattern>)` predicate form
    // ("syntax error, unexpected REGEXP_LIKE_", 42000). Both wire encodings —
    // the dedicated `predicate_like_regexp` node type and the alternate
    // `function_scalar` REGEXP_LIKE encoding — must render that same infix
    // form on the Exasol-parsed path, byte-identically to each other, while
    // the DataFusion-dialect rendering of both stays `regexp_like(s, p)`.
    let predicate = json!({
        "type": "predicate_like_regexp",
        "expression": {"type": "column", "name": "name"},
        "pattern": {"type": "literal_string", "value": "^A.*"}
    });
    let scalar = json!({
        "type": "function_scalar",
        "name": "REGEXP_LIKE",
        "arguments": [
            {"type": "column", "name": "name"},
            {"type": "literal_string", "value": "^A.*"}
        ]
    });

    let predicate_exasol = render_expression_exasol(&predicate).unwrap();
    let scalar_exasol = render_expression_exasol(&scalar).unwrap();
    assert_eq!(predicate_exasol, r#"("NAME" REGEXP_LIKE '^A.*')"#);
    assert_eq!(
        scalar_exasol, predicate_exasol,
        "the two REGEXP_LIKE encodings must render byte-identically in the Exasol dialect"
    );

    assert_eq!(
        render_expression(&predicate).unwrap(),
        r#"regexp_like("NAME", '^A.*')"#
    );
    assert_eq!(
        render_expression(&scalar).unwrap(),
        r#"regexp_like("NAME", '^A.*')"#
    );
}

#[test]
fn regexp_like_predicate_missing_operand_errors_in_both_dialects() {
    let missing_pattern = json!({
        "type": "predicate_like_regexp",
        "expression": {"type": "column", "name": "name"}
    });
    assert!(render_expression(&missing_pattern).is_err());
    assert!(render_expression_exasol(&missing_pattern).is_err());

    let missing_expression = json!({
        "type": "predicate_like_regexp",
        "pattern": {"type": "literal_string", "value": "^A.*"}
    });
    assert!(render_expression(&missing_expression).is_err());
    assert!(render_expression_exasol(&missing_expression).is_err());
}

#[test]
fn regexp_like_scalar_arity_errors_in_both_dialects() {
    let one_arg = json!({
        "type": "function_scalar",
        "name": "REGEXP_LIKE",
        "arguments": [{"type": "column", "name": "name"}]
    });
    assert!(render_expression(&one_arg).is_err());
    assert!(render_expression_exasol(&one_arg).is_err());
}

// --- Math scalar functions (ABS/ROUND/SIGN→signum/trig/...) ---

#[test]
fn renders_math_scalar_functions() {
    // 1-arg functions
    let cases_1arg = [
        ("ABS", "abs"),
        ("FLOOR", "floor"),
        ("CEIL", "ceil"),
        ("SQRT", "sqrt"),
        ("EXP", "exp"),
        ("LN", "ln"),
        ("SIGN", "signum"),
        ("DEGREES", "degrees"),
        ("RADIANS", "radians"),
        ("SIN", "sin"),
        ("COS", "cos"),
        ("TAN", "tan"),
        ("ASIN", "asin"),
        ("ACOS", "acos"),
        ("ATAN", "atan"),
        ("SINH", "sinh"),
        ("COSH", "cosh"),
        ("TANH", "tanh"),
        ("COT", "cot"),
    ];
    for (exasol, df) in cases_1arg {
        let expr = json!({
            "type": "function_scalar",
            "name": exasol,
            "arguments": [{"type": "column", "name": "x"}]
        });
        let sql = render_expression(&expr).unwrap();
        assert_eq!(sql, format!(r#"{df}("X")"#), "failed for {exasol}");
    }

    // 2-arg: POWER, ATAN2
    let expr = json!({
        "type": "function_scalar",
        "name": "POWER",
        "arguments": [
            {"type": "column", "name": "x"},
            {"type": "literal_exactnumeric", "value": 2}
        ]
    });
    assert_eq!(render_expression(&expr).unwrap(), r#"power("X", 2)"#);

    let expr = json!({
        "type": "function_scalar",
        "name": "ATAN2",
        "arguments": [
            {"type": "column", "name": "y"},
            {"type": "column", "name": "x"}
        ]
    });
    assert_eq!(render_expression(&expr).unwrap(), r#"atan2("Y", "X")"#);

    // 1-or-2-arg: ROUND, TRUNC, LOG
    let expr = json!({
        "type": "function_scalar",
        "name": "ROUND",
        "arguments": [{"type": "column", "name": "v"}, {"type": "literal_exactnumeric", "value": 2}]
    });
    assert_eq!(render_expression(&expr).unwrap(), r#"round("V", 2)"#);

    let expr = json!({
        "type": "function_scalar",
        "name": "TRUNC",
        "arguments": [{"type": "column", "name": "v"}]
    });
    assert_eq!(render_expression(&expr).unwrap(), r#"trunc("V")"#);

    // Arity error: ABS with 2 args
    let expr = json!({
        "type": "function_scalar",
        "name": "ABS",
        "arguments": [
            {"type": "column", "name": "x"},
            {"type": "column", "name": "y"}
        ]
    });
    assert!(render_expression_safe(&expr).is_none());
}

// --- MOD → % operator (DataFusion) / MOD(...) (Exasol) ---

#[test]
fn renders_mod_as_operator() {
    let expr = json!({
        "type": "function_scalar",
        "name": "MOD",
        "arguments": [
            {"type": "column", "name": "a"},
            {"type": "literal_exactnumeric", "value": 3}
        ]
    });
    assert_eq!(render_expression(&expr).unwrap(), r#"("A" % 3)"#);
}

#[test]
fn renders_mod_as_function_call_in_exasol_dialect() {
    // https://github.com/exasol-labs/lakehouse-engine-rs/issues/197
    // Exasol's parser rejects `%` — an Exasol-side wrapper (e.g. the
    // COUNT(DISTINCT ...) outer wrapper) must render MOD(a, b) instead.
    let expr = json!({
        "type": "function_scalar",
        "name": "MOD",
        "arguments": [
            {"type": "column", "name": "a"},
            {"type": "literal_exactnumeric", "value": 3}
        ]
    });
    assert_eq!(render_expression_exasol(&expr).unwrap(), r#"MOD("A", 3)"#);
    // DataFusion-dialect rendering of the same node must stay unchanged.
    assert_eq!(render_expression(&expr).unwrap(), r#"("A" % 3)"#);
}

// --- String scalar functions (CONCAT/LENGTH→character_length/INSTR+LOCATE→strpos/...) ---

#[test]
fn renders_string_scalar_functions() {
    // Pass-through lowercased
    let cases_lower = [
        "LOWER",
        "UPPER",
        "TRIM",
        "LTRIM",
        "RTRIM",
        "REPLACE",
        "REPEAT",
        "REVERSE",
        "LPAD",
        "RPAD",
        "ASCII",
        "CHR",
        "INITCAP",
        "LEFT",
        "RIGHT",
        "TRANSLATE",
    ];
    for name in cases_lower {
        let expr = json!({
            "type": "function_scalar",
            "name": name,
            "arguments": [{"type": "column", "name": "s"}]
        });
        let sql = render_expression(&expr).unwrap();
        assert_eq!(
            sql,
            format!(r#"{}("S")"#, name.to_lowercase()),
            "failed for {name}"
        );
    }

    // LENGTH → character_length
    let expr = json!({
        "type": "function_scalar",
        "name": "LENGTH",
        "arguments": [{"type": "column", "name": "s"}]
    });
    assert_eq!(
        render_expression(&expr).unwrap(),
        r#"character_length("S")"#
    );

    // OCTET_LENGTH → octet_length
    let expr = json!({
        "type": "function_scalar",
        "name": "OCTET_LENGTH",
        "arguments": [{"type": "column", "name": "s"}]
    });
    assert_eq!(render_expression(&expr).unwrap(), r#"octet_length("S")"#);

    // UNICODE → ascii
    let expr = json!({
        "type": "function_scalar",
        "name": "UNICODE",
        "arguments": [{"type": "column", "name": "s"}]
    });
    assert_eq!(render_expression(&expr).unwrap(), r#"ascii("S")"#);

    // UNICODECHR → chr
    let expr = json!({
        "type": "function_scalar",
        "name": "UNICODECHR",
        "arguments": [{"type": "column", "name": "n"}]
    });
    assert_eq!(render_expression(&expr).unwrap(), r#"chr("N")"#);

    // SUBSTR → substr (same name, but explicit mapping)
    let expr = json!({
        "type": "function_scalar",
        "name": "SUBSTR",
        "arguments": [
            {"type": "column", "name": "s"},
            {"type": "literal_exactnumeric", "value": 1},
            {"type": "literal_exactnumeric", "value": 3}
        ]
    });
    assert_eq!(render_expression(&expr).unwrap(), r#"substr("S", 1, 3)"#);

    // INSTR: INSTR(string, substring) → strpos(string, substring)
    let expr = json!({
        "type": "function_scalar",
        "name": "INSTR",
        "arguments": [
            {"type": "literal_string", "value": "hello"},
            {"type": "literal_string", "value": "ll"}
        ]
    });
    assert_eq!(render_expression(&expr).unwrap(), "strpos('hello', 'll')");

    // LOCATE: LOCATE(substring, string) → strpos(string, substring) — operands reordered
    let expr = json!({
        "type": "function_scalar",
        "name": "LOCATE",
        "arguments": [
            {"type": "literal_string", "value": "ll"},
            {"type": "literal_string", "value": "hello"}
        ]
    });
    assert_eq!(render_expression(&expr).unwrap(), "strpos('hello', 'll')");
}

// --- CONCAT → chained `||` (NULL-propagating, unlike DataFusion's concat()) ---

#[test]
fn renders_concat_as_chained_pipe_operator() {
    // Two args: joined with `||`, not concat() — concat() silently turns a
    // NULL operand into empty string (#200's GROUP BY repro shape).
    let expr = json!({
        "type": "function_scalar",
        "name": "CONCAT",
        "arguments": [
            {"type": "column", "name": "s"},
            {"type": "literal_string", "value": ""}
        ]
    });
    assert_eq!(render_expression(&expr).unwrap(), r#"("S" || '')"#);

    // Three args: chained, still no concat() call.
    let expr = json!({
        "type": "function_scalar",
        "name": "CONCAT",
        "arguments": [
            {"type": "column", "name": "a"},
            {"type": "column", "name": "b"},
            {"type": "column", "name": "c"}
        ]
    });
    assert_eq!(render_expression(&expr).unwrap(), r#"("A" || "B" || "C")"#);
}

#[test]
fn renders_concat_bool_operand_as_exasol_case() {
    // A boolean-producing argument (here `predicate_equal`) is rewritten to
    // the Exasol-cased CASE form before joining — DataFusion's `||` falls
    // back to its lowercase boolean->Utf8 cast for a raw boolean operand
    // otherwise (#200).
    let expr = json!({
        "type": "function_scalar",
        "name": "CONCAT",
        "arguments": [
            {"type": "predicate_equal",
             "left": {"type": "column", "name": "active"},
             "right": {"type": "literal_bool", "value": true}},
            {"type": "literal_string", "value": ""}
        ]
    });
    assert_eq!(
        render_expression(&expr).unwrap(),
        r#"((CASE ("ACTIVE" = TRUE) WHEN TRUE THEN 'TRUE' WHEN FALSE THEN 'FALSE' ELSE NULL END) || '')"#
    );
}

// --- CASE WHEN ... THEN ... ELSE ... END ---

#[test]
fn renders_case_when() {
    // CASE WHEN cond THEN result END (no else)
    let expr = json!({
        "type": "function_scalar",
        "name": "CASE",
        "arguments": [
            {"type": "predicate_equal",
             "left": {"type": "column", "name": "status"},
             "right": {"type": "literal_string", "value": "A"}},
            {"type": "literal_exactnumeric", "value": 1}
        ]
    });
    let sql = render_expression(&expr).unwrap();
    assert_eq!(sql, r#"CASE WHEN ("STATUS" = 'A') THEN 1 END"#);

    // CASE WHEN c1 THEN r1 WHEN c2 THEN r2 ELSE else END
    let expr2 = json!({
        "type": "function_scalar",
        "name": "CASE",
        "arguments": [
            {"type": "predicate_equal",
             "left": {"type": "column", "name": "x"},
             "right": {"type": "literal_exactnumeric", "value": 1}},
            {"type": "literal_string", "value": "one"},
            {"type": "predicate_equal",
             "left": {"type": "column", "name": "x"},
             "right": {"type": "literal_exactnumeric", "value": 2}},
            {"type": "literal_string", "value": "two"},
            {"type": "literal_string", "value": "other"}
        ]
    });
    let sql2 = render_expression(&expr2).unwrap();
    assert_eq!(
        sql2,
        r#"CASE WHEN ("X" = 1) THEN 'one' WHEN ("X" = 2) THEN 'two' ELSE 'other' END"#
    );

    // Empty CASE (< 2 args) → error
    let expr3 = json!({
        "type": "function_scalar",
        "name": "CASE",
        "arguments": []
    });
    assert!(render_expression_safe(&expr3).is_none());
}

// --- GREATEST / LEAST ---

#[test]
fn renders_greatest_least() {
    let expr = json!({
        "type": "function_scalar",
        "name": "GREATEST",
        "arguments": [
            {"type": "column", "name": "a"},
            {"type": "column", "name": "b"},
            {"type": "column", "name": "c"}
        ]
    });
    assert_eq!(
        render_expression(&expr).unwrap(),
        r#"CASE WHEN "A" IS NULL OR "B" IS NULL OR "C" IS NULL THEN NULL ELSE greatest("A", "B", "C") END"#
    );

    let expr2 = json!({
        "type": "function_scalar",
        "name": "LEAST",
        "arguments": [
            {"type": "column", "name": "x"},
            {"type": "literal_exactnumeric", "value": 0}
        ]
    });
    assert_eq!(
        render_expression(&expr2).unwrap(),
        r#"CASE WHEN "X" IS NULL OR 0 IS NULL THEN NULL ELSE least("X", 0) END"#
    );
}

#[test]
fn renders_greatest_least_single_argument_guard() {
    let greatest = json!({
        "type": "function_scalar",
        "name": "GREATEST",
        "arguments": [{"type": "column", "name": "a"}]
    });
    assert_eq!(
        render_expression(&greatest).unwrap(),
        r#"CASE WHEN "A" IS NULL THEN NULL ELSE greatest("A") END"#
    );

    let least = json!({
        "type": "function_scalar",
        "name": "LEAST",
        "arguments": [{"type": "column", "name": "a"}]
    });
    assert_eq!(
        render_expression(&least).unwrap(),
        r#"CASE WHEN "A" IS NULL THEN NULL ELSE least("A") END"#
    );
}

#[test]
fn renders_greatest_least_with_literal_null_argument() {
    let expr = json!({
        "type": "function_scalar",
        "name": "LEAST",
        "arguments": [
            {"type": "column", "name": "x"},
            {"type": "column", "name": "y"},
            {"type": "literal_null"}
        ]
    });
    assert_eq!(
        render_expression(&expr).unwrap(),
        r#"CASE WHEN "X" IS NULL OR "Y" IS NULL OR NULL IS NULL THEN NULL ELSE least("X", "Y", NULL) END"#
    );
}

#[test]
fn renders_greatest_least_nested_argument_once_referenced_twice() {
    let expr = json!({
        "type": "function_scalar",
        "name": "GREATEST",
        "arguments": [
            {
                "type": "function_scalar",
                "name": "ABS",
                "arguments": [{"type": "column", "name": "y"}]
            },
            {"type": "column", "name": "z"}
        ]
    });
    assert_eq!(
        render_expression(&expr).unwrap(),
        r#"CASE WHEN abs("Y") IS NULL OR "Z" IS NULL THEN NULL ELSE greatest(abs("Y"), "Z") END"#
    );
}

#[test]
fn renders_nested_greatest_guard_referencing_the_inner_case_twice() {
    let expr = json!({
        "type": "function_scalar",
        "name": "GREATEST",
        "arguments": [
            {
                "type": "function_scalar",
                "name": "GREATEST",
                "arguments": [
                    {"type": "column", "name": "a"},
                    {"type": "column", "name": "b"}
                ]
            },
            {"type": "column", "name": "c"}
        ]
    });
    assert_eq!(
        render_expression(&expr).unwrap(),
        r#"CASE WHEN CASE WHEN "A" IS NULL OR "B" IS NULL THEN NULL ELSE greatest("A", "B") END IS NULL OR "C" IS NULL THEN NULL ELSE greatest(CASE WHEN "A" IS NULL OR "B" IS NULL THEN NULL ELSE greatest("A", "B") END, "C") END"#
    );
}

#[test]
fn renders_greatest_least_empty_argument_list_errors() {
    for name in ["GREATEST", "LEAST"] {
        let expr = json!({
            "type": "function_scalar",
            "name": name,
            "arguments": []
        });
        assert!(
            render_expression(&expr).is_err(),
            "{name} empty args must raise"
        );
        assert!(
            render_expression_safe(&expr).is_none(),
            "{name} empty args must be None in safe mode"
        );
    }
}

#[test]
fn greatest_least_without_arguments_key_errors() {
    for name in ["GREATEST", "LEAST"] {
        let expr = json!({
            "type": "function_scalar",
            "name": name
        });
        let err = render_expression(&expr)
            .expect_err(&format!("{name} without arguments key must raise"));
        assert!(
            err.to_string().contains("missing 'arguments'"),
            "{name} error should mention missing 'arguments', got: {err}"
        );
        assert!(
            render_expression_safe(&expr).is_none(),
            "{name} without arguments key must be None in safe mode"
        );
    }
}

// --- NULLIFZERO / ZEROIFNULL ---

#[test]
fn renders_nullifzero_zeroifnull() {
    let expr = json!({
        "type": "function_scalar",
        "name": "NULLIFZERO",
        "arguments": [{"type": "column", "name": "v"}]
    });
    assert_eq!(render_expression(&expr).unwrap(), r#"nullif("V", 0)"#);

    let expr2 = json!({
        "type": "function_scalar",
        "name": "ZEROIFNULL",
        "arguments": [{"type": "column", "name": "v"}]
    });
    assert_eq!(render_expression(&expr2).unwrap(), r#"coalesce("V", 0)"#);
}

// --- NULLIF (two-arg) ---

/// NULLIF(MOD(id,5),0) — the group key from test_group_by_null_key_grouping —
/// must render so the grouped-aggregate path (not the row-scan fallback) handles it.
#[test]
fn renders_nullif_of_mod() {
    let expr = json!({
        "type": "function_scalar",
        "name": "NULLIF",
        "arguments": [
            {
                "type": "function_scalar",
                "name": "MOD",
                "arguments": [
                    {"type": "column", "name": "id"},
                    {"type": "literal_exactnumeric", "value": "5"}
                ]
            },
            {"type": "literal_exactnumeric", "value": "0"}
        ]
    });
    assert_eq!(
        render_expression(&expr).unwrap(),
        r#"nullif(("ID" % 5), 0)"#
    );
}

// --- CASE (function_scalar_case) ---

/// Exasol expands NULLIF(MOD(id,5),0) into a simple CASE before pushdown:
///   CASE MOD(id,5) WHEN 0 THEN NULL ELSE MOD(id,5) END
/// This is the actual group key Exasol pushes in test_group_by_null_key_grouping
/// (FN_CASE is advertised), so the grouped-aggregate path — not the row-scan
/// fallback — must render it.
#[test]
fn renders_simple_case_from_nullif_expansion() {
    let mod_node = json!({
        "type": "function_scalar",
        "name": "MOD",
        "arguments": [
            {"type": "column", "name": "ID"},
            {"type": "literal_exactnumeric", "value": "5"}
        ]
    });
    let expr = json!({
        "type": "function_scalar_case",
        "name": "CASE",
        "basis": mod_node,
        "arguments": [{"type": "literal_exactnumeric", "value": "0"}],
        "results": [
            {"type": "literal_null"},
            mod_node
        ]
    });
    assert_eq!(
        render_expression(&expr).unwrap(),
        r#"(CASE ("ID" % 5) WHEN 0 THEN NULL ELSE ("ID" % 5) END)"#
    );
}

/// Searched CASE (no `basis`): WHEN arguments are boolean predicates.
#[test]
fn renders_searched_case_without_basis() {
    let expr = json!({
        "type": "function_scalar_case",
        "name": "CASE",
        "arguments": [
            {"type": "predicate_less",
             "left": {"type": "column", "name": "SCORE"},
             "right": {"type": "literal_exactnumeric", "value": "50"}}
        ],
        "results": [
            {"type": "literal_string", "value": "low"},
            {"type": "literal_string", "value": "high"}
        ]
    });
    assert_eq!(
        render_expression(&expr).unwrap(),
        r#"(CASE WHEN ("SCORE" < 50) THEN 'low' ELSE 'high' END)"#
    );
}

/// CASE with no ELSE branch: results.len() == arguments.len().
#[test]
fn renders_case_without_else() {
    let expr = json!({
        "type": "function_scalar_case",
        "name": "CASE",
        "basis": {"type": "column", "name": "ID"},
        "arguments": [{"type": "literal_exactnumeric", "value": "1"}],
        "results": [{"type": "literal_string", "value": "one"}]
    });
    assert_eq!(
        render_expression(&expr).unwrap(),
        r#"(CASE "ID" WHEN 1 THEN 'one' END)"#
    );
}

// --- EXTRACT and field-shortcut date functions ---

#[test]
fn renders_extract() {
    // Exasol sends EXTRACT as its own node type with the field in `toExtract`.
    let expr = json!({
        "type": "function_scalar_extract",
        "name": "EXTRACT",
        "toExtract": "YEAR",
        "arguments": [{"type": "column", "name": "ts"}]
    });
    assert_eq!(
        render_expression(&expr).unwrap(),
        r#"date_part('YEAR', "TS")"#
    );

    let expr2 = json!({
        "type": "function_scalar_extract",
        "name": "EXTRACT",
        "toExtract": "MONTH",
        "arguments": [{"type": "column", "name": "ts"}]
    });
    assert_eq!(
        render_expression(&expr2).unwrap(),
        r#"date_part('MONTH', "TS")"#
    );
}

#[test]
fn renders_extract_as_exasol_extract_from_in_exasol_dialect() {
    // Exasol's parser has no DATE_PART function; the EXTRACT-carrying node
    // must render Exasol's own EXTRACT(<FIELD> FROM <src>) form, with the
    // field as a bare keyword (not a quoted string literal), on the
    // Exasol-parsed path. The DataFusion-dialect rendering of the same
    // node stays unchanged (date_part('<FIELD>', <src>)).
    let expr = json!({
        "type": "function_scalar_extract",
        "name": "EXTRACT",
        "toExtract": "DAY",
        "arguments": [{"type": "column", "name": "ts"}]
    });
    assert_eq!(
        render_expression_exasol(&expr).unwrap(),
        r#"EXTRACT(DAY FROM "TS")"#
    );
    assert_eq!(
        render_expression(&expr).unwrap(),
        r#"date_part('DAY', "TS")"#
    );
}

#[test]
fn renders_year_month_day_extract() {
    let shortcuts = ["YEAR", "MONTH", "DAY", "HOUR", "MINUTE", "SECOND"];
    for field in shortcuts {
        let expr = json!({
            "type": "function_scalar",
            "name": field,
            "arguments": [{"type": "column", "name": "ts"}]
        });
        let sql = render_expression(&expr).unwrap();
        assert_eq!(
            sql,
            format!(r#"date_part('{field}', "TS")"#),
            "failed for {field}"
        );
    }
}

// --- DATE_TRUNC ---

#[test]
fn renders_date_trunc() {
    let expr = json!({
        "type": "function_scalar",
        "name": "DATE_TRUNC",
        "arguments": [
            {"type": "literal_string", "value": "month"},
            {"type": "column", "name": "ts"}
        ]
    });
    assert_eq!(
        render_expression(&expr).unwrap(),
        r#"date_trunc('month', "TS")"#
    );
}

// --- CURRENT_DATE / SYSDATE / CURRENT_TIMESTAMP / SYSTIMESTAMP: withdrawn ---

#[test]
fn now_family_falls_through() {
    // The now-family is the one translation this change RETIRES rather than
    // re-renders, because no rendering can be right in either dialect. Exasol's
    // four names are three semantics over one instant — CURRENT_TIMESTAMP reads
    // it in the session zone, SYSTIMESTAMP the same instant in the database
    // zone, and CURRENT_DATE/SYSDATE are TO_DATE of each — while the scan UDF
    // receives neither SESSIONTIMEZONE nor DBTIMEZONE, opens no connect-back
    // session, and holds no statement anchor. It read its container clock in UTC
    // once per shard, so a select-list SYSTIMESTAMP returned 15:02:02 through the
    // virtual schema against 17:02:03 natively in the same session, and one
    // statement returned two different timestamps over a two-file table.
    //
    // Withdrawal is total and paired: the four names carry no
    // TRANSLATED_SCALAR_FNS row, so the gate declines them before any per-name
    // arm, and capabilities.rs advertises none of them, so Exasol never delegates
    // one and evaluates its own clock instead. Pinned to the generic decline text
    // as well as the name (same reason as
    // `bitwise_operator_functions_fall_through`): a future arm that merely
    // validated arity would also name the function, and would silently defeat
    // this decline-lock.
    for name in [
        "CURRENT_DATE",
        "SYSDATE",
        "CURRENT_TIMESTAMP",
        "SYSTIMESTAMP",
    ] {
        let expr = json!({"type": "function_scalar", "name": name, "arguments": []});
        for (dialect, rendered) in [
            ("DataFusion", render_expression(&expr)),
            ("Exasol", render_expression_exasol(&expr)),
        ] {
            let err = rendered.unwrap_err().to_string();
            assert!(
                err.contains("unsupported scalar function"),
                "{name} must fall through the generic unsupported-scalar-function \
                 path in the {dialect} dialect: {err}"
            );
            assert!(
                err.contains(name),
                "the {dialect}-dialect error must name '{name}': {err}"
            );
        }
        assert!(
            render_expression_safe(&expr).is_none(),
            "{name} must be None in the DataFusion safe variant"
        );
        assert!(
            render_expression_exasol_safe(&expr).is_none(),
            "{name} must be None in the Exasol safe variant"
        );
    }
}

// --- TO_DATE / TO_TIMESTAMP with optional format arg ---

#[test]
fn renders_to_date_to_timestamp() {
    // TO_DATE with 1 arg
    let expr = json!({
        "type": "function_scalar",
        "name": "TO_DATE",
        "arguments": [{"type": "column", "name": "s"}]
    });
    assert_eq!(render_expression(&expr).unwrap(), r#"to_date("S")"#);

    // TO_DATE with format
    let expr2 = json!({
        "type": "function_scalar",
        "name": "TO_DATE",
        "arguments": [
            {"type": "column", "name": "s"},
            {"type": "literal_string", "value": "%Y-%m-%d"}
        ]
    });
    assert_eq!(
        render_expression(&expr2).unwrap(),
        r#"to_date("S", '%Y-%m-%d')"#
    );

    // TO_TIMESTAMP with 1 arg
    let expr3 = json!({
        "type": "function_scalar",
        "name": "TO_TIMESTAMP",
        "arguments": [{"type": "column", "name": "s"}]
    });
    assert_eq!(render_expression(&expr3).unwrap(), r#"to_timestamp("S")"#);

    // TO_TIMESTAMP with format
    let expr4 = json!({
        "type": "function_scalar",
        "name": "TO_TIMESTAMP",
        "arguments": [
            {"type": "column", "name": "s"},
            {"type": "literal_string", "value": "%Y-%m-%d %H:%M:%S"}
        ]
    });
    assert_eq!(
        render_expression(&expr4).unwrap(),
        r#"to_timestamp("S", '%Y-%m-%d %H:%M:%S')"#
    );
}

// --- WEEK (ISO-8601) ---

#[test]
fn renders_week_as_iso_date_part() {
    let expr = json!({
        "type": "function_scalar",
        "name": "WEEK",
        "arguments": [{"type": "column", "name": "d"}]
    });
    assert_eq!(
        render_expression(&expr).unwrap(),
        r#"date_part('week', "D")"#
    );
}

#[test]
fn renders_week_at_year_boundary_dates() {
    // ISO-8601 parity is what gates FN_WEEK. The translator emits
    // date_part('week', <arg>); DataFusion 54 maps 'week' → DatePart::Week →
    // chrono iso_week().week(), and Exasol WEEK is documented ISO-8601, so the
    // two agree at year boundaries. Verified by executing the rendered call in
    // DataFusion 54 for these boundary dates:
    //   2021-01-01 (Fri) → 53   (ISO week 53 of 2020)
    //   2020-12-31 (Thu) → 53
    //   2019-12-30 (Mon) → 1    (ISO week 1 of 2020)
    //   2023-01-01 (Sun) → 52   (ISO week 52 of 2022)
    // The translator itself only renders the call; this test pins the
    // rendering for boundary-date arguments so the parity target stays fixed.
    let boundary_dates = ["2021-01-01", "2020-12-31", "2019-12-30", "2023-01-01"];
    for date in boundary_dates {
        let expr = json!({
            "type": "function_scalar",
            "name": "WEEK",
            "arguments": [{"type": "literal_date", "value": date}]
        });
        assert_eq!(
            render_expression(&expr).unwrap(),
            format!("date_part('week', DATE '{date}')"),
            "failed for boundary date {date}"
        );
    }
}

#[test]
fn week_with_wrong_arity_falls_back() {
    let expr = json!({
        "type": "function_scalar",
        "name": "WEEK",
        "arguments": [
            {"type": "column", "name": "d"},
            {"type": "column", "name": "e"}
        ]
    });
    assert!(render_expression(&expr).is_err());
    assert!(render_expression_safe(&expr).is_none());
}

// ADD_HOURS / ADD_MINUTES have no rendering test: they were withdrawn after
// E2E parity (task 3.1) showed the microsecond round-trip diverges on a DATE
// argument (Exasol expects TIMESTAMP(0), the rendering yields TIMESTAMP(3)).
// They now fall through — see `unsupported_date_functions_decline_in_both_dialects`.

// --- DAYS_BETWEEN (whole-day date difference) ---

#[test]
fn renders_days_between_as_date_difference() {
    // DATE - DATE yields an Int64 day count in DataFusion 54.0.0
    // (is_date_minus_date in type_coercion/binary.rs → ret: Int64). Outer parens
    // keep the difference composition-safe as an operand (same convention as the
    // FN_ADD/SUB/MULT arms).
    let expr = json!({
        "type": "function_scalar",
        "name": "DAYS_BETWEEN",
        "arguments": [
            {"type": "column", "name": "a"},
            {"type": "column", "name": "b"}
        ]
    });
    assert_eq!(
        render_expression(&expr).unwrap(),
        r#"(CAST("A" AS DATE) - CAST("B" AS DATE))"#
    );
}

// --- HOURS/MINUTES/SECONDS_BETWEEN (epoch-second differences) ---

#[test]
fn renders_time_between_as_epoch_difference() {
    let hours = json!({
        "type": "function_scalar",
        "name": "HOURS_BETWEEN",
        "arguments": [
            {"type": "column", "name": "a"},
            {"type": "column", "name": "b"}
        ]
    });
    assert_eq!(
        render_expression(&hours).unwrap(),
        r#"((date_part('epoch', "A") - date_part('epoch', "B")) / 3600)"#
    );

    let minutes = json!({
        "type": "function_scalar",
        "name": "MINUTES_BETWEEN",
        "arguments": [
            {"type": "column", "name": "a"},
            {"type": "column", "name": "b"}
        ]
    });
    assert_eq!(
        render_expression(&minutes).unwrap(),
        r#"((date_part('epoch', "A") - date_part('epoch', "B")) / 60)"#
    );

    let seconds = json!({
        "type": "function_scalar",
        "name": "SECONDS_BETWEEN",
        "arguments": [
            {"type": "column", "name": "a"},
            {"type": "column", "name": "b"}
        ]
    });
    assert_eq!(
        render_expression(&seconds).unwrap(),
        r#"(date_part('epoch', "A") - date_part('epoch', "B"))"#
    );
}

#[test]
fn between_fns_reject_wrong_arity() {
    for name in [
        "DAYS_BETWEEN",
        "HOURS_BETWEEN",
        "MINUTES_BETWEEN",
        "SECONDS_BETWEEN",
    ] {
        let one_arg = json!({
            "type": "function_scalar",
            "name": name,
            "arguments": [{"type": "column", "name": "a"}]
        });
        assert!(
            render_expression(&one_arg).is_err(),
            "{name} 1-arg must raise"
        );
        assert!(
            render_expression_safe(&one_arg).is_none(),
            "{name} 1-arg must be None in safe mode"
        );

        let three_args = json!({
            "type": "function_scalar",
            "name": name,
            "arguments": [
                {"type": "column", "name": "a"},
                {"type": "column", "name": "b"},
                {"type": "column", "name": "c"}
            ]
        });
        assert!(
            render_expression(&three_args).is_err(),
            "{name} 3-arg must raise"
        );
        assert!(
            render_expression_safe(&three_args).is_none(),
            "{name} 3-arg must be None in safe mode"
        );
    }
}

/// Pins the dialect asymmetry `fix-declined-filter-self-apply` relies on:
/// `SECOND` is a DataFusion field-shortcut (exactly 1 argument) but an Exasol
/// `VerbatimCall` (any arity, rendered as written). A 2-argument `SECOND(ts, 3)`
/// therefore declines under the DataFusion dialect while still rendering under
/// the Exasol dialect — the asymmetry a declined filter must be self-applied
/// through, in Exasol's own dialect, rather than omitted.
#[test]
fn second_with_precision_declines_for_datafusion_renders_for_exasol() {
    let expr = json!({
        "type": "function_scalar",
        "name": "SECOND",
        "arguments": [
            {"type": "column", "name": "ts"},
            {"type": "literal_exactnumeric", "value": 3}
        ]
    });

    assert!(
        render_expression_safe(&expr).is_none(),
        "SECOND(ts, 3) must decline under the DataFusion dialect"
    );
    assert!(
        render_expression_exasol_safe(&expr).is_some(),
        "SECOND(ts, 3) must still render under the Exasol dialect"
    );
}

// --- Integer division DIV is deliberately not translated ---

#[test]
fn div_falls_through_as_unsupported() {
    // Exasol DIV truncates toward zero (verified live: DIV(-7,2) = -3, not
    // floor's -4) and matches DataFusion's integer `/`. But DataFusion 54 has
    // no `div` builtin, and a `TRUNC(m/n)` emulation would diverge from
    // Exasol on DOUBLE-operand division by zero (Exasol raises SQL state
    // 22012; DataFusion float division yields infinity). DIV operand types
    // aren't carried in the expression node, so the translator can't
    // selectively render only the safe integer case. DIV must therefore
    // decline so Exasol evaluates it.
    let expr = json!({
        "type": "function_scalar",
        "name": "DIV",
        "arguments": [
            {"type": "column", "name": "a"},
            {"type": "column", "name": "b"}
        ]
    });
    let err = render_expression(&expr).unwrap_err();
    assert!(
        err.to_string().contains("DIV"),
        "error must name DIV as unsupported: {err}"
    );
    assert!(
        render_expression_safe(&expr).is_none(),
        "DIV must be None in safe mode without panicking"
    );
}

// --- Conversion format functions TO_CHAR/TO_NUMBER are deliberately not translated ---

#[test]
fn to_char_and_to_number_fall_through_as_unsupported() {
    // DataFusion 54 `to_char` uses strftime masks (not Exasol's Oracle-style
    // format models) and rejects numeric formatting; DataFusion 54 has no
    // `to_number` at all. Both must therefore decline so Exasol evaluates
    // them; a no-format string-to-number conversion stays reachable via CAST.
    let unsupported = ["TO_CHAR", "TO_NUMBER"];
    for name in unsupported {
        let expr = json!({
            "type": "function_scalar",
            "name": name,
            "arguments": [
                {"type": "column", "name": "a"},
                {"type": "literal_string", "value": "999.99"}
            ]
        });
        let err = render_expression(&expr).unwrap_err();
        assert!(
            err.to_string().contains(name),
            "error must name the unsupported function '{name}': {err}"
        );
        assert!(
            render_expression_safe(&expr).is_none(),
            "{name} must be None in safe mode without panicking"
        );
    }
}

// --- Regexp scalar functions are deliberately not translated (issue #106) ---

#[test]
fn regexp_scalar_functions_decline_in_both_dialects() {
    // The Rust `regex` crate (DataFusion 54) rejects backreferences and
    // lookaround that Exasol's PCRE dialect accepts (blocks all four),
    // lacks regexp_substr (blocks REGEXP_SUBSTR), and REGEXP_REPLACE /
    // REGEXP_INSTR's argument shapes differ from Exasol's position/
    // occurrence/return options (REGEXP_COUNT's shape actually aligns) —
    // so all four scalar regexp functions decline (issue #106).
    //
    // The decline is a property of the declaration, not of a dialect: these
    // names carry no TRANSLATED_SCALAR_FNS row, so the gate declines them
    // identically in both dialects. Asserting the Exasol dialect too is what
    // stops the verbatim rule from quietly re-admitting a name whose Exasol
    // form would parse but whose semantics were never the reason it declined.
    let unsupported = [
        "REGEXP_REPLACE",
        "REGEXP_SUBSTR",
        "REGEXP_INSTR",
        "REGEXP_COUNT",
    ];
    for name in unsupported {
        let expr = json!({
            "type": "function_scalar",
            "name": name,
            "arguments": [
                {"type": "column", "name": "s"},
                {"type": "literal_string", "value": "a+"}
            ]
        });
        let expected = format!("unsupported scalar function: {name}");
        assert_eq!(
            render_expression(&expr).unwrap_err().to_string(),
            expected,
            "the DataFusion dialect must decline {name}"
        );
        assert_eq!(
            render_expression_exasol(&expr).unwrap_err().to_string(),
            expected,
            "the Exasol dialect must decline {name}"
        );
        assert!(
            render_expression_safe(&expr).is_none(),
            "{name} must be None in the DataFusion safe variant"
        );
        assert!(
            render_expression_exasol_safe(&expr).is_none(),
            "{name} must be None in the Exasol safe variant"
        );
    }
}

// --- Bitwise operator functions are deliberately not translated (issue #108) ---

#[test]
fn bitwise_operator_functions_fall_through() {
    // Exasol's eleven bit functions operate on an UNSIGNED 64-bit domain
    // (0..=18446744073709551615, result DECIMAL(20,0)); none has a faithful
    // DataFusion 54.0.0 translation, so all eleven decline (issue #108). Two
    // distinct blocker classes:
    //
    //   1. Operator-backed but signed-domain (BIT_AND/OR/XOR/LSHIFT/RSHIFT):
    //      DataFusion's `&`/`|`/`#`/`<<`/`>>` (Operator::BitwiseAnd/Or/Xor/
    //      ShiftLeft/ShiftRight) act on the SIGNED operand type. Any bit-63-set
    //      result is unsigned-large in Exasol but negative under Int64, and
    //      Int64 -> DECIMAL(20,0) carries the negative value; `>>` is arithmetic
    //      (sign-extend) vs Exasol's logical (zero-fill). Operand types aren't
    //      carried in the expression node, so the type/value-blind translator
    //      cannot restrict to the safe subset (the recorded DIV limitation).
    //   2. No DataFusion 54.0.0 operator or builtin at all (BIT_NOT/LROTATE/
    //      RROTATE/CHECK/SET/TO_NUM): unary `~` is `not_impl_err`, and
    //      datafusion-functions registers no rotate/bit-test/bit-set/bits-to-
    //      number scalar (only the unrelated string `bit_length`).
    //
    // Both classes fall through the generic unsupported-`function_scalar` path;
    // this test pins that decline (no dedicated production arm exists).
    let unsupported = [
        "BIT_AND",
        "BIT_OR",
        "BIT_XOR",
        "BIT_NOT",
        "BIT_LSHIFT",
        "BIT_RSHIFT",
        "BIT_LROTATE",
        "BIT_RROTATE",
        "BIT_CHECK",
        "BIT_SET",
        "BIT_TO_NUM",
    ];
    for name in unsupported {
        let expr = json!({
            "type": "function_scalar",
            "name": name,
            "arguments": [
                {"type": "column", "name": "a"},
                {"type": "column", "name": "b"}
            ]
        });
        let err = render_expression(&expr).unwrap_err();
        let err_string = err.to_string();
        // Pinned to the generic fallthrough text, not just the function name: a
        // future dedicated arm that merely validates arity (e.g. modeled on NEG's
        // arity-check error) would also produce a message containing `name`, which
        // would silently defeat this decline-lock for the six functions with no
        // DataFusion builtin at all (BIT_NOT/LROTATE/RROTATE/CHECK/SET/TO_NUM).
        assert!(
            err_string.contains("unsupported scalar function"),
            "{name} must fall through the generic unsupported-scalar-function path: {err}"
        );
        assert!(
            err_string.contains(name),
            "error must name the unsupported function '{name}': {err}"
        );
        assert!(
            render_expression_safe(&expr).is_none(),
            "{name} must be None in safe mode without panicking"
        );
    }
}

// --- The declaration gates the dispatch (issue #209) ---

#[test]
fn undeclared_scalar_function_declines_in_both_dialects() {
    // `TRANSLATED_SCALAR_FNS` declares the whole translated `function_scalar`
    // surface, and the gate at the head of that arm reads it BEFORE any
    // per-name arm runs. A name the declaration does not carry is therefore
    // declined in BOTH dialects, with the same `unsupported scalar function:
    // <name>` message the generic fall-through raised before the gate existed.
    // That is what makes a per-name arm added without a declaration row
    // unreachable, rather than silently rendering DataFusion SQL on the
    // Exasol-parsed path.
    //
    // SUBSTRING and SOUNDEX are real Exasol functions this translator does not
    // translate. The remaining rows pin the gate's own edges: the name is
    // uppercased before the lookup, the declaration is consulted before the
    // `arguments` key (so an undeclared name declines as undeclared, not as
    // malformed), and a node carrying no `name` key declines under the empty
    // name.
    let arg = json!([{"type": "column", "name": "a"}]);
    let cases = [
        (
            "SUBSTRING",
            json!({"type": "function_scalar", "name": "SUBSTRING", "arguments": arg.clone()}),
        ),
        (
            "SOUNDEX",
            json!({"type": "function_scalar", "name": "SOUNDEX", "arguments": arg.clone()}),
        ),
        (
            "SUBSTRING",
            json!({"type": "function_scalar", "name": "substring", "arguments": arg.clone()}),
        ),
        (
            "SUBSTRING",
            json!({"type": "function_scalar", "name": "SUBSTRING"}),
        ),
        ("", json!({"type": "function_scalar", "arguments": arg})),
    ];
    for (declined_name, expr) in cases {
        let expected = format!("unsupported scalar function: {declined_name}");
        assert_eq!(
            render_expression(&expr).unwrap_err().to_string(),
            expected,
            "DataFusion dialect must decline the undeclared node {expr}"
        );
        assert_eq!(
            render_expression_exasol(&expr).unwrap_err().to_string(),
            expected,
            "Exasol dialect must decline the undeclared node {expr}"
        );
        assert!(
            render_expression_safe(&expr).is_none(),
            "DataFusion safe variant must be None for {expr}"
        );
        assert!(
            render_expression_exasol_safe(&expr).is_none(),
            "Exasol safe variant must be None for {expr}"
        );
    }
}

#[test]
fn regexp_scalar_exclusion_leaves_regexp_like_untouched() {
    // The scalar-regexp exclusion (issue #106) must not affect the REGEXP_LIKE
    // predicate path (FN_PRED_REGEXP_LIKE stays advertised): both encodings
    // still render.
    let predicate = json!({
        "type": "predicate_like_regexp",
        "expression": {"type": "column", "name": "name"},
        "pattern": {"type": "literal_string", "value": "^A.*"}
    });
    assert_eq!(
        render_expression(&predicate).unwrap(),
        r#"regexp_like("NAME", '^A.*')"#
    );
    let scalar = json!({
        "type": "function_scalar",
        "name": "REGEXP_LIKE",
        "arguments": [
            {"type": "column", "name": "name"},
            {"type": "literal_string", "value": "^B.*"}
        ]
    });
    assert_eq!(
        render_expression(&scalar).unwrap(),
        r#"regexp_like("NAME", '^B.*')"#
    );
}

// --- Unsupported date functions return an error ---

#[test]
fn unsupported_date_functions_decline_in_both_dialects() {
    // Remaining excluded set per the date-fns spec Background: the date-arithmetic,
    // date-difference, and other date scalars whose DataFusion 54 equivalents still
    // diverge from Exasol (or don't exist at all). DAYS_BETWEEN, HOURS_BETWEEN,
    // MINUTES_BETWEEN, and SECONDS_BETWEEN are no longer here — they now have real
    // translator arms (see the disposition table in `add-date-arithmetic-pushdown`)
    // and are covered by their own rendering tests instead. ADD_HOURS/ADD_MINUTES
    // ARE still here: their arm was withdrawn after E2E parity (task 3.1) showed
    // the microsecond round-trip diverges on a DATE argument (Exasol expects
    // TIMESTAMP(0), the rendering yields TIMESTAMP(3)).
    //
    // Every one of these names EXISTS in Exasol, so the Exasol-dialect assertion
    // is the load-bearing half: the verbatim rule could render each of them as a
    // compiling call, and it deliberately does not. Absence from
    // TRANSLATED_SCALAR_FNS is what keeps them Exasol's own work. The four
    // now-family names decline the same way but have their own test,
    // `now_family_falls_through`, which records why no rendering can be right.
    let unsupported = [
        // Date-arithmetic
        "ADD_HOURS",
        "ADD_MINUTES",
        "ADD_DAYS",
        "ADD_SECONDS",
        "ADD_WEEKS",
        "ADD_MONTHS",
        "ADD_YEARS",
        // Date-difference
        "MONTHS_BETWEEN",
        "YEARS_BETWEEN",
        // Other date scalars
        "DAYOFWEEK",
        "LAST_DAY",
        "CONVERT_TZ",
        "POSIX_TIME",
    ];
    for name in unsupported {
        let expr = json!({
            "type": "function_scalar",
            "name": name,
            "arguments": [{"type": "column", "name": "x"}]
        });
        let expected = format!("unsupported scalar function: {name}");
        assert_eq!(
            render_expression(&expr).unwrap_err().to_string(),
            expected,
            "the DataFusion dialect must decline {name}"
        );
        assert_eq!(
            render_expression_exasol(&expr).unwrap_err().to_string(),
            expected,
            "the Exasol dialect must decline {name}"
        );
        assert!(
            render_expression_safe(&expr).is_none(),
            "{name} must be None in the DataFusion safe variant"
        );
        assert!(
            render_expression_exasol_safe(&expr).is_none(),
            "{name} must be None in the Exasol safe variant"
        );
    }
}

// --- Aggregate function nodes (function_aggregate) ---

#[test]
fn render_expression_renders_aggregate_nodes() {
    // SUM(col) — aggregate name spliced verbatim, bare column argument recursed.
    let sum = json!({
        "type": "function_aggregate",
        "name": "SUM",
        "arguments": [{"type": "column", "name": "col"}],
        "distinct": false
    });
    assert_eq!(render_expression(&sum).unwrap(), r#"SUM("COL")"#);

    // COUNT(*) — empty argument list is the star case.
    let count_star = json!({
        "type": "function_aggregate",
        "name": "COUNT",
        "arguments": [],
        "distinct": false
    });
    assert_eq!(render_expression(&count_star).unwrap(), "COUNT(*)");

    // COUNT(DISTINCT col) — distinct keyword precedes the rendered argument.
    let count_distinct = json!({
        "type": "function_aggregate",
        "name": "COUNT",
        "arguments": [{"type": "column", "name": "col"}],
        "distinct": true
    });
    assert_eq!(
        render_expression(&count_distinct).unwrap(),
        r#"COUNT(DISTINCT "COL")"#
    );

    // AVG(col).
    let avg = json!({
        "type": "function_aggregate",
        "name": "AVG",
        "arguments": [{"type": "column", "name": "col"}],
        "distinct": false
    });
    assert_eq!(render_expression(&avg).unwrap(), r#"AVG("COL")"#);

    // A column argument carrying a tableAlias renders table-qualified via the
    // shared `column` arm — nested aggregate arguments qualify over a join.
    let sum_qualified = json!({
        "type": "function_aggregate",
        "name": "SUM",
        "arguments": [{"type": "column", "name": "amount", "tableAlias": "O"}],
        "distinct": false
    });
    assert_eq!(
        render_expression(&sum_qualified).unwrap(),
        r#"SUM("O"."AMOUNT")"#
    );
}

#[test]
fn render_expression_renders_scalar_wrapping_aggregates() {
    // The reported failing select item:
    //   ROUND(100.0 * SUM(CASE WHEN l_returnflag='R' THEN 1 ELSE 0 END) / COUNT(*), 2)
    let sum_case = json!({
        "type": "function_aggregate",
        "name": "SUM",
        "arguments": [{
            "type": "function_scalar",
            "name": "CASE",
            "arguments": [
                {"type": "predicate_equal",
                 "left": {"type": "column", "name": "l_returnflag"},
                 "right": {"type": "literal_string", "value": "R"}},
                {"type": "literal_exactnumeric", "value": 1},
                {"type": "literal_exactnumeric", "value": 0}
            ]
        }],
        "distinct": false
    });
    let count_star = json!({
        "type": "function_aggregate",
        "name": "COUNT",
        "arguments": [],
        "distinct": false
    });
    let round = json!({
        "type": "function_scalar",
        "name": "ROUND",
        "arguments": [
            {"type": "function_scalar", "name": "FLOAT_DIV", "arguments": [
                {"type": "function_scalar", "name": "MULT", "arguments": [
                    {"type": "literal_double", "value": 100.0},
                    sum_case
                ]},
                count_star
            ]},
            {"type": "literal_exactnumeric", "value": 2}
        ]
    });

    let sql = render_expression_safe(&round).expect("scalar-over-aggregate must render");
    assert!(
        sql.contains(r#"SUM(CASE WHEN ("L_RETURNFLAG" = 'R') THEN 1 ELSE 0 END)"#),
        "nested SUM(CASE ...) must be spliced verbatim: {sql}"
    );
    assert!(
        sql.contains("COUNT(*)"),
        "nested COUNT(*) must render as the star case: {sql}"
    );
}

#[test]
fn aggregate_with_unrenderable_argument_declines() {
    let bad = json!({
        "type": "function_aggregate",
        "name": "SUM",
        "arguments": [{"type": "totally_unknown_node"}],
        "distinct": false
    });
    assert!(
        render_expression(&bad).is_err(),
        "an unrenderable argument must raise in raising mode"
    );
    assert!(
        render_expression_safe(&bad).is_none(),
        "an unrenderable argument must be None in safe mode"
    );
}

// --- CAST dialect split (DataFusion vs Exasol) ---
//
// The SAME expression node renders differently depending on which parser
// will consume the fragment: DataFusion's SQL frontend rejects a length on
// VARCHAR (bare `VARCHAR`), while Exasol's own parser REQUIRES a length
// (`VARCHAR(n)`). These guard the Exasol-dialect entry points and the
// divergence between the two so a future refactor cannot silently collapse
// them back together.

#[test]
fn renders_cast_varchar_exasol_dialect_includes_length() {
    let expr = json!({
        "type": "function_scalar_cast", "name": "CAST",
        "arguments": [{"type": "column", "name": "x"}],
        "dataType": {"type": "VARCHAR", "size": 100}
    });
    assert_eq!(
        render_expression_exasol(&expr).unwrap(),
        r#"CAST("X" AS VARCHAR(100))"#
    );
}

#[test]
fn renders_cast_char_exasol_dialect_includes_length() {
    let expr = json!({
        "type": "function_scalar_cast", "name": "CAST",
        "arguments": [{"type": "column", "name": "x"}],
        "dataType": {"type": "CHAR", "size": 3, "characterSet": "ASCII"}
    });
    assert_eq!(
        render_expression_exasol(&expr).unwrap(),
        r#"CAST("X" AS CHAR(3) ASCII)"#
    );
}

/// Divergence guard: the SAME CHAR node must render bare `VARCHAR` in the
/// DataFusion dialect and length-qualified `CHAR(n)` in the Exasol dialect.
/// If a future change collapses the two dialects together, exactly one of
/// these assertions fails, catching the regression that reintroduces either
/// the "unexpected ')', expecting '(' " Exasol parse error / the `CHAR(n)`
/// vs `VARCHAR(n)` "Data type mismatch" pushdown rejection (#192), or the
/// datafusion-sql "length not supported" error, depending on direction.
#[test]
fn cast_char_target_diverges_between_dialects() {
    let expr = json!({
        "type": "function_scalar_cast", "name": "CAST",
        "arguments": [{"type": "column", "name": "c_varchar"}],
        "dataType": {"type": "CHAR", "size": 20, "characterSet": "ASCII"}
    });
    // DataFusion dialect: bare VARCHAR, no length.
    assert_eq!(
        render_expression(&expr).unwrap(),
        r#"CAST("C_VARCHAR" AS VARCHAR)"#
    );
    // Exasol dialect: the declared fixed-width CHAR, length-qualified.
    assert_eq!(
        render_expression_exasol(&expr).unwrap(),
        r#"CAST("C_VARCHAR" AS CHAR(20) ASCII)"#
    );
}

/// Defensive fallback: a character CAST target with no `size` (which a real
/// Exasol-sent dataType always carries, but be defensive) renders the
/// project's "unknown/incompatible width" default `VARCHAR(2000000)` in the
/// Exasol dialect — never bare `VARCHAR`, which Exasol would reject.
#[test]
fn renders_cast_varchar_exasol_dialect_without_size_falls_back() {
    let expr = json!({
        "type": "function_scalar_cast", "name": "CAST",
        "arguments": [{"type": "column", "name": "x"}],
        "dataType": {"type": "VARCHAR"}
    });
    assert_eq!(
        render_expression_exasol(&expr).unwrap(),
        r#"CAST("X" AS VARCHAR(2000000))"#
    );
}

/// The same defensive fallback for a size-less CHAR target: it keeps the
/// project's `VARCHAR(2000000)` "unknown/incompatible width" convention
/// rather than inventing a CHAR width, because Exasol caps CHAR at 2,000 and
/// this crate does not synthesise a width it was not sent. Unreachable from a
/// real Exasol dataType, which always carries `size`.
#[test]
fn renders_cast_char_exasol_dialect_without_size_falls_back_to_varchar_default() {
    let expr = json!({
        "type": "function_scalar_cast", "name": "CAST",
        "arguments": [{"type": "column", "name": "x"}],
        "dataType": {"type": "CHAR", "characterSet": "ASCII"}
    });
    assert_eq!(
        render_expression_exasol(&expr).unwrap(),
        r#"CAST("X" AS VARCHAR(2000000))"#
    );
}

// --- Exasol-dialect verbatim rendering, per family (issue #209) ---
//
// In the Exasol dialect the translator renders what Exasol sent: the same name,
// argument order, and argument count, taken from the node's own uppercased
// `name`. The expression tree came from Exasol's own compiler, so reproducing
// its call means Exasol's engine evaluates exactly the call it emitted — which
// is why these renderings need no arity check and cannot be wrong.
//
// Every test below is PAIRED: it asserts the Exasol-dialect rendering and, on
// the SAME node, that the DataFusion-dialect rendering is unchanged. That
// pairing is what freezes the DataFusion output while the Exasol output moves,
// and it is the convention `renders_cast_timestamp_precision_per_dialect`
// established. `renders_mod_as_function_call_in_exasol_dialect` above is the
// same shape for the one arm (#197) that already owned both dialects.

#[test]
fn renders_math_family_verbatim_in_exasol_dialect() {
    // Exasol has every one of these names natively, so the Exasol dialect
    // re-emits the call. The DataFusion dialect keeps its lowercase mapping,
    // including SIGN -> signum, whose name Exasol does not have at all.
    let one_arg = [
        ("ABS", "abs"),
        ("FLOOR", "floor"),
        ("CEIL", "ceil"),
        ("SQRT", "sqrt"),
        ("EXP", "exp"),
        ("LN", "ln"),
        ("SIGN", "signum"),
        ("DEGREES", "degrees"),
        ("RADIANS", "radians"),
        ("SIN", "sin"),
        ("COS", "cos"),
        ("TAN", "tan"),
        ("ASIN", "asin"),
        ("ACOS", "acos"),
        ("ATAN", "atan"),
        ("SINH", "sinh"),
        ("COSH", "cosh"),
        ("TANH", "tanh"),
        ("COT", "cot"),
    ];
    for (exasol_name, df_name) in one_arg {
        let expr = json!({
            "type": "function_scalar",
            "name": exasol_name,
            "arguments": [{"type": "column", "name": "x"}]
        });
        assert_eq!(
            render_expression_exasol(&expr).unwrap(),
            format!(r#"{exasol_name}("X")"#),
            "the Exasol dialect must render {exasol_name} verbatim"
        );
        assert_eq!(
            render_expression(&expr).unwrap(),
            format!(r#"{df_name}("X")"#),
            "the DataFusion dialect must stay unchanged for {exasol_name}"
        );
    }

    // ROUND / TRUNC / LOG take 1 or 2 arguments and POWER / ATAN2 exactly 2;
    // the Exasol dialect reproduces whichever count Exasol sent.
    let two_arg = [
        ("ROUND", "round"),
        ("TRUNC", "trunc"),
        ("LOG", "log"),
        ("POWER", "power"),
        ("ATAN2", "atan2"),
    ];
    for (exasol_name, df_name) in two_arg {
        let expr = json!({
            "type": "function_scalar",
            "name": exasol_name,
            "arguments": [
                {"type": "column", "name": "v"},
                {"type": "literal_exactnumeric", "value": 2}
            ]
        });
        assert_eq!(
            render_expression_exasol(&expr).unwrap(),
            format!(r#"{exasol_name}("V", 2)"#),
            "the Exasol dialect must render {exasol_name} verbatim"
        );
        assert_eq!(
            render_expression(&expr).unwrap(),
            format!(r#"{df_name}("V", 2)"#),
            "the DataFusion dialect must stay unchanged for {exasol_name}"
        );
    }
}

#[test]
fn renders_sign_as_native_sign_in_exasol_dialect() {
    // The headline failure of issue #209: `SELECT l_returnflag, SIGN(SUM(
    // l_discount) - 0.5) ... GROUP BY l_returnflag` aborted with "function or
    // script SIGNUM not found" (42000), because the grouped-aggregate wrapper
    // splices this rendering into SQL that Exasol's own core engine parses.
    //
    // SIGN is also why the gate sits AHEAD of `match fn_name.as_str()` instead
    // of being a widened guard inside it: the math arm matches SIGN and precedes
    // any such guard, so an in-place widening would still have rendered `signum`.
    // Arm order now carries no dialect precedence at all.
    let expr = json!({
        "type": "function_scalar",
        "name": "SIGN",
        "arguments": [{
            "type": "function_scalar",
            "name": "SUB",
            "arguments": [
                {"type": "function_aggregate", "name": "SUM",
                 "arguments": [{"type": "column", "name": "l_discount"}]},
                {"type": "literal_double", "value": 0.5}
            ]
        }]
    });
    let exasol = render_expression_exasol(&expr).unwrap();
    assert_eq!(exasol, r#"SIGN((SUM("L_DISCOUNT") - 0.5))"#);
    assert!(
        !exasol.contains("signum"),
        "the Exasol dialect must not emit DataFusion's signum: {exasol}"
    );
    assert_eq!(
        render_expression(&expr).unwrap(),
        r#"signum((SUM("L_DISCOUNT") - 0.5))"#
    );
}

#[test]
fn renders_string_family_verbatim_in_exasol_dialect() {
    // Issue #210 shipped this family's Exasol rendering with no translator-side
    // test; this is that test. Four of the names have no DataFusion function of
    // the same name at all (LENGTH -> character_length, OCTET_LENGTH ->
    // octet_length, UNICODE -> ascii, UNICODECHR -> chr), which is why the
    // DataFusion dialect keeps its name-mapping arm.
    let one_arg = [
        ("LOWER", "lower"),
        ("UPPER", "upper"),
        ("TRIM", "trim"),
        ("LTRIM", "ltrim"),
        ("RTRIM", "rtrim"),
        ("REPLACE", "replace"),
        ("REPEAT", "repeat"),
        ("REVERSE", "reverse"),
        ("LPAD", "lpad"),
        ("RPAD", "rpad"),
        ("ASCII", "ascii"),
        ("CHR", "chr"),
        ("INITCAP", "initcap"),
        ("LEFT", "left"),
        ("RIGHT", "right"),
        ("TRANSLATE", "translate"),
        ("LENGTH", "character_length"),
        ("OCTET_LENGTH", "octet_length"),
        ("UNICODE", "ascii"),
        ("UNICODECHR", "chr"),
    ];
    for (exasol_name, df_name) in one_arg {
        let expr = json!({
            "type": "function_scalar",
            "name": exasol_name,
            "arguments": [{"type": "column", "name": "s"}]
        });
        assert_eq!(
            render_expression_exasol(&expr).unwrap(),
            format!(r#"{exasol_name}("S")"#),
            "the Exasol dialect must render {exasol_name} verbatim"
        );
        assert_eq!(
            render_expression(&expr).unwrap(),
            format!(r#"{df_name}("S")"#),
            "the DataFusion dialect must stay unchanged for {exasol_name}"
        );
    }

    // SUBSTR carries its own explicit mapping and a 3-argument shape.
    let substr = json!({
        "type": "function_scalar",
        "name": "SUBSTR",
        "arguments": [
            {"type": "column", "name": "s"},
            {"type": "literal_exactnumeric", "value": 1},
            {"type": "literal_exactnumeric", "value": 3}
        ]
    });
    assert_eq!(
        render_expression_exasol(&substr).unwrap(),
        r#"SUBSTR("S", 1, 3)"#
    );
    assert_eq!(render_expression(&substr).unwrap(), r#"substr("S", 1, 3)"#);
}

#[test]
fn renders_instr_locate_verbatim_with_start_arg_in_exasol_dialect() {
    // Exasol's INSTR(string, substring [, start]) and LOCATE(substring, string
    // [, start]) already understand the optional start position, so the Exasol
    // dialect has nothing to translate: reproducing the name, order, and count is
    // the whole rendering, and an arity check there could only reject valid input
    // Exasol's own compiler emitted (issue #210).
    //
    // The DataFusion dialect maps both onto strpos(string, substring), which
    // takes no start position — so it reorders LOCATE's operands and DROPS a
    // third argument. That drop is a pre-existing limitation of the DataFusion
    // rendering, outside this change's scope (which freezes DataFusion output);
    // it is pinned here so the Exasol side cannot silently regress onto it.
    let instr = json!({
        "type": "function_scalar",
        "name": "INSTR",
        "arguments": [
            {"type": "literal_string", "value": "hello"},
            {"type": "literal_string", "value": "l"},
            {"type": "literal_exactnumeric", "value": 3}
        ]
    });
    assert_eq!(
        render_expression_exasol(&instr).unwrap(),
        "INSTR('hello', 'l', 3)"
    );
    assert_eq!(render_expression(&instr).unwrap(), "strpos('hello', 'l')");

    let locate = json!({
        "type": "function_scalar",
        "name": "LOCATE",
        "arguments": [
            {"type": "literal_string", "value": "l"},
            {"type": "literal_string", "value": "hello"},
            {"type": "literal_exactnumeric", "value": 3}
        ]
    });
    assert_eq!(
        render_expression_exasol(&locate).unwrap(),
        "LOCATE('l', 'hello', 3)"
    );
    assert_eq!(render_expression(&locate).unwrap(), "strpos('hello', 'l')");
}

#[test]
fn renders_greatest_least_verbatim_in_exasol_dialect() {
    // Both names already parse in Exasol, so the rendering changes only in case.
    // They join the verbatim rule anyway: a rule applied to some names and not
    // others cannot be reasoned about, because the next reader cannot tell which
    // renderings are principled and which merely happen to work.
    let greatest = json!({
        "type": "function_scalar",
        "name": "GREATEST",
        "arguments": [
            {"type": "column", "name": "a"},
            {"type": "column", "name": "b"},
            {"type": "column", "name": "c"}
        ]
    });
    assert_eq!(
        render_expression_exasol(&greatest).unwrap(),
        r#"GREATEST("A", "B", "C")"#
    );
    assert_eq!(
        render_expression(&greatest).unwrap(),
        r#"CASE WHEN "A" IS NULL OR "B" IS NULL OR "C" IS NULL THEN NULL ELSE greatest("A", "B", "C") END"#
    );

    let least = json!({
        "type": "function_scalar",
        "name": "LEAST",
        "arguments": [
            {"type": "column", "name": "x"},
            {"type": "literal_exactnumeric", "value": 0}
        ]
    });
    assert_eq!(
        render_expression_exasol(&least).unwrap(),
        r#"LEAST("X", 0)"#
    );
    assert_eq!(
        render_expression(&least).unwrap(),
        r#"CASE WHEN "X" IS NULL OR 0 IS NULL THEN NULL ELSE least("X", 0) END"#
    );
}

#[test]
fn renders_nullifzero_zeroifnull_verbatim_in_exasol_dialect() {
    // DataFusion has neither name, so it emulates: NULLIFZERO(v) ->
    // nullif(v, 0) and ZEROIFNULL(v) -> coalesce(v, 0). Exasol has both
    // natively, and the verbatim rendering gains parity by construction — there
    // is no emulation left on that path to diverge.
    let nullifzero = json!({
        "type": "function_scalar",
        "name": "NULLIFZERO",
        "arguments": [{"type": "column", "name": "v"}]
    });
    assert_eq!(
        render_expression_exasol(&nullifzero).unwrap(),
        r#"NULLIFZERO("V")"#
    );
    assert_eq!(render_expression(&nullifzero).unwrap(), r#"nullif("V", 0)"#);

    let zeroifnull = json!({
        "type": "function_scalar",
        "name": "ZEROIFNULL",
        "arguments": [{"type": "column", "name": "v"}]
    });
    assert_eq!(
        render_expression_exasol(&zeroifnull).unwrap(),
        r#"ZEROIFNULL("V")"#
    );
    assert_eq!(
        render_expression(&zeroifnull).unwrap(),
        r#"coalesce("V", 0)"#
    );
}

#[test]
fn renders_nullif_verbatim_in_exasol_dialect() {
    // NULLIF is one of the names where the two dialects differ only in case, so
    // this test carries the composition check as well: the verbatim gate renders
    // its arguments in the SAME dialect it was called with, which is why the
    // nested MOD becomes Exasol's MOD(a, b) and not DataFusion's `%`. Without
    // that, a wrapper-bound NULLIF(MOD(id, 5), 0) group key would still splice
    // `%` — which Exasol's parser rejects — into Exasol-parsed SQL.
    let expr = json!({
        "type": "function_scalar",
        "name": "NULLIF",
        "arguments": [
            {
                "type": "function_scalar",
                "name": "MOD",
                "arguments": [
                    {"type": "column", "name": "id"},
                    {"type": "literal_exactnumeric", "value": "5"}
                ]
            },
            {"type": "literal_exactnumeric", "value": "0"}
        ]
    });
    assert_eq!(
        render_expression_exasol(&expr).unwrap(),
        r#"NULLIF(MOD("ID", 5), 0)"#
    );
    assert_eq!(
        render_expression(&expr).unwrap(),
        r#"nullif(("ID" % 5), 0)"#
    );
}

#[test]
fn renders_date_field_shortcuts_verbatim_in_exasol_dialect() {
    // `SELECT COUNT(DISTINCT YEAR(l_shipdate)) FROM <vs>.LINEITEM` aborted with
    // "function or script DATE_PART not found" (42000): Exasol has no DATE_PART
    // at all, but it has every one of these six shortcuts natively.
    for field in ["YEAR", "MONTH", "DAY", "HOUR", "MINUTE", "SECOND"] {
        let expr = json!({
            "type": "function_scalar",
            "name": field,
            "arguments": [{"type": "column", "name": "ts"}]
        });
        assert_eq!(
            render_expression_exasol(&expr).unwrap(),
            format!(r#"{field}("TS")"#),
            "the Exasol dialect must render {field} verbatim"
        );
        assert_eq!(
            render_expression(&expr).unwrap(),
            format!(r#"date_part('{field}', "TS")"#),
            "the DataFusion dialect must stay unchanged for {field}"
        );
    }
}

#[test]
fn renders_week_as_native_week_in_exasol_dialect() {
    // Exasol's own WEEK is what the DataFusion date_part('week') rendering was
    // chosen to match (both ISO-8601, weeks beginning Monday, week 1 containing
    // the year's first Thursday), so on the Exasol-parsed path re-emitting WEEK
    // is both the form that compiles and the one that is exactly equivalent.
    let expr = json!({
        "type": "function_scalar",
        "name": "WEEK",
        "arguments": [{"type": "column", "name": "d"}]
    });
    assert_eq!(render_expression_exasol(&expr).unwrap(), r#"WEEK("D")"#);
    assert_eq!(
        render_expression(&expr).unwrap(),
        r#"date_part('week', "D")"#
    );
}

#[test]
fn renders_date_trunc_verbatim_in_exasol_dialect() {
    // Exasol's DATE_TRUNC(format, datetime) has the same name and the same
    // argument order as DataFusion's, so the verbatim rendering differs only in
    // case — and the format literal Exasol sent is forwarded untouched by both
    // dialects rather than being re-interpreted by either.
    let expr = json!({
        "type": "function_scalar",
        "name": "DATE_TRUNC",
        "arguments": [
            {"type": "literal_string", "value": "month"},
            {"type": "column", "name": "ts"}
        ]
    });
    assert_eq!(
        render_expression_exasol(&expr).unwrap(),
        r#"DATE_TRUNC('month', "TS")"#
    );
    assert_eq!(
        render_expression(&expr).unwrap(),
        r#"date_trunc('month', "TS")"#
    );
}

#[test]
fn renders_to_date_to_timestamp_verbatim_in_exasol_dialect() {
    // Both names exist in Exasol and both dialects forward the optional format
    // argument unchanged. The format model in the node is Exasol's own
    // ('YYYY-MM-DD'), which is exactly why re-emitting Exasol's call is safe on
    // the Exasol-parsed path: Exasol parses the model it wrote itself.
    for (exasol_name, df_name) in [("TO_DATE", "to_date"), ("TO_TIMESTAMP", "to_timestamp")] {
        let bare = json!({
            "type": "function_scalar",
            "name": exasol_name,
            "arguments": [{"type": "column", "name": "s"}]
        });
        assert_eq!(
            render_expression_exasol(&bare).unwrap(),
            format!(r#"{exasol_name}("S")"#),
            "the Exasol dialect must render {exasol_name} verbatim"
        );
        assert_eq!(
            render_expression(&bare).unwrap(),
            format!(r#"{df_name}("S")"#),
            "the DataFusion dialect must stay unchanged for {exasol_name}"
        );

        let formatted = json!({
            "type": "function_scalar",
            "name": exasol_name,
            "arguments": [
                {"type": "column", "name": "s"},
                {"type": "literal_string", "value": "YYYY-MM-DD"}
            ]
        });
        assert_eq!(
            render_expression_exasol(&formatted).unwrap(),
            format!(r#"{exasol_name}("S", 'YYYY-MM-DD')"#),
            "the Exasol dialect must forward {exasol_name}'s format model verbatim"
        );
        assert_eq!(
            render_expression(&formatted).unwrap(),
            format!(r#"{df_name}("S", 'YYYY-MM-DD')"#),
            "the DataFusion dialect must stay unchanged for {exasol_name} with a format"
        );
    }
}

#[test]
fn renders_days_between_verbatim_in_exasol_dialect() {
    // The DataFusion rendering is a CAST-to-DATE difference — an emulation of a
    // function DataFusion does not have. Exasol has DAYS_BETWEEN, so the Exasol
    // dialect re-emits it and the emulation stays on the DataFusion side only.
    let expr = json!({
        "type": "function_scalar",
        "name": "DAYS_BETWEEN",
        "arguments": [
            {"type": "column", "name": "a"},
            {"type": "column", "name": "b"}
        ]
    });
    assert_eq!(
        render_expression_exasol(&expr).unwrap(),
        r#"DAYS_BETWEEN("A", "B")"#
    );
    assert_eq!(
        render_expression(&expr).unwrap(),
        r#"(CAST("A" AS DATE) - CAST("B" AS DATE))"#
    );
}

#[test]
fn renders_between_family_verbatim_in_exasol_dialect() {
    // The *_BETWEEN pushdown shipped in `add-date-arithmetic-pushdown` was broken
    // on the Exasol-parsed path from day one: its epoch-difference emulation
    // calls DATE_PART, which Exasol does not have (42000). Exasol has all three
    // names natively, so the Exasol dialect re-emits them.
    let df_epoch = r#"(date_part('epoch', "A") - date_part('epoch', "B"))"#;
    let cases = [
        ("HOURS_BETWEEN", format!("({df_epoch} / 3600)")),
        ("MINUTES_BETWEEN", format!("({df_epoch} / 60)")),
        ("SECONDS_BETWEEN", df_epoch.to_string()),
    ];
    for (name, df_expected) in cases {
        let expr = json!({
            "type": "function_scalar",
            "name": name,
            "arguments": [
                {"type": "column", "name": "a"},
                {"type": "column", "name": "b"}
            ]
        });
        assert_eq!(
            render_expression_exasol(&expr).unwrap(),
            format!(r#"{name}("A", "B")"#),
            "the Exasol dialect must render {name} verbatim"
        );
        assert_eq!(
            render_expression(&expr).unwrap(),
            df_expected,
            "the DataFusion dialect must stay unchanged for {name}"
        );
    }
}

// --- Systemic sweep over the whole declared surface (issue #209) ---

/// The verbatim rule is only durable if a forgotten name fails a test rather
/// than a review, so this test iterates `TRANSLATED_SCALAR_FNS` itself rather
/// than a parallel hand-written name list: a name added to the declaration
/// with no fixture fails here BY NAME, and a fixture for a name nobody
/// declared fails here too. A `VerbatimCall` expectation is DERIVED from the
/// node's own uppercased `name` by the same rule the gate applies, so the 66
/// verbatim names cannot be blessed one hand-written string at a time — a
/// rewrite back to a DataFusion name (`signum`, `date_part`, `strpos`) fails
/// the derived comparison. Only the ten `Shaped` names and the five
/// dialect-branching node types outside `function_scalar` declare an expected
/// string, because each of those has a shape of its own.
///
/// Every fixture argument is a dialect-invariant node (a column or a plain
/// literal) on purpose: the derivation renders the arguments through the same
/// entry point under test, so a dialect-sensitive argument would make that
/// half of the expectation self-fulfilling. The dialect-sensitive nodes get
/// their own rows instead.
#[test]
fn exasol_dialect_renders_declared_verbatim_surface() {
    struct ScalarFixture {
        name: &'static str,
        node: Json,
        shaped_exasol: Option<&'static str>,
    }

    fn col(name: &str) -> Json {
        json!({"type": "column", "name": name})
    }
    fn num(value: i64) -> Json {
        json!({"type": "literal_exactnumeric", "value": value})
    }
    fn text(value: &str) -> Json {
        json!({"type": "literal_string", "value": value})
    }
    fn scalar(name: &str, args: Vec<Json>) -> Json {
        json!({"type": "function_scalar", "name": name, "arguments": args})
    }
    fn verbatim(name: &'static str, args: Vec<Json>) -> ScalarFixture {
        ScalarFixture {
            name,
            node: scalar(name, args),
            shaped_exasol: None,
        }
    }
    fn shaped(name: &'static str, node: Json, exasol: &'static str) -> ScalarFixture {
        ScalarFixture {
            name,
            node,
            shaped_exasol: Some(exasol),
        }
    }

    let mut fixtures: Vec<ScalarFixture> = Vec::new();

    // Math family: the one-argument names, then the five taking a second.
    for name in [
        "ABS", "FLOOR", "CEIL", "SQRT", "EXP", "LN", "SIGN", "DEGREES", "RADIANS", "SIN", "COS",
        "TAN", "ASIN", "ACOS", "ATAN", "SINH", "COSH", "TANH", "COT",
    ] {
        fixtures.push(verbatim(name, vec![col("x")]));
    }
    for name in ["ROUND", "TRUNC", "LOG", "POWER", "ATAN2"] {
        fixtures.push(verbatim(name, vec![col("v"), num(2)]));
    }

    // String family.
    for name in [
        "LOWER",
        "UPPER",
        "TRIM",
        "LTRIM",
        "RTRIM",
        "REVERSE",
        "ASCII",
        "INITCAP",
        "LENGTH",
        "OCTET_LENGTH",
        "UNICODE",
    ] {
        fixtures.push(verbatim(name, vec![col("s")]));
    }
    for name in ["CHR", "UNICODECHR"] {
        fixtures.push(verbatim(name, vec![num(65)]));
    }
    for name in ["SUBSTR", "LEFT", "RIGHT", "REPEAT", "LPAD", "RPAD"] {
        fixtures.push(verbatim(name, vec![col("s"), num(3)]));
    }
    fixtures.push(verbatim("REPLACE", vec![col("s"), text("a")]));
    fixtures.push(verbatim(
        "TRANSLATE",
        vec![col("s"), text("ab"), text("xy")],
    ));
    // INSTR(string, substring) against LOCATE(substring, string): opposite
    // argument orders, which is exactly what the verbatim rule preserves and
    // what the DataFusion dialect has to reorder into strpos.
    fixtures.push(verbatim("INSTR", vec![col("s"), text("a")]));
    fixtures.push(verbatim("LOCATE", vec![text("a"), col("s")]));

    // Comparison and null-handling family.
    for name in ["GREATEST", "LEAST", "NULLIF"] {
        fixtures.push(verbatim(name, vec![col("a"), col("b")]));
    }
    for name in ["NULLIFZERO", "ZEROIFNULL"] {
        fixtures.push(verbatim(name, vec![col("v")]));
    }

    // Date field shortcuts, then the conversion and truncation names.
    for name in ["YEAR", "MONTH", "DAY", "HOUR", "MINUTE", "SECOND", "WEEK"] {
        fixtures.push(verbatim(name, vec![col("ts")]));
    }
    fixtures.push(verbatim("DATE_TRUNC", vec![text("month"), col("ts")]));
    fixtures.push(verbatim("TO_DATE", vec![col("s"), text("YYYY-MM-DD")]));
    fixtures.push(verbatim(
        "TO_TIMESTAMP",
        vec![col("s"), text("YYYY-MM-DD HH24:MI:SS")],
    ));

    // Date-difference family.
    for name in [
        "DAYS_BETWEEN",
        "HOURS_BETWEEN",
        "MINUTES_BETWEEN",
        "SECONDS_BETWEEN",
    ] {
        fixtures.push(verbatim(name, vec![col("a"), col("b")]));
    }

    // The ten Shaped names, each declaring the string its own arm renders.
    fixtures.extend([
        shaped(
            "ADD",
            scalar("ADD", vec![col("a"), col("b")]),
            r#"("A" + "B")"#,
        ),
        shaped(
            "SUB",
            scalar("SUB", vec![col("a"), col("b")]),
            r#"("A" - "B")"#,
        ),
        shaped(
            "MULT",
            scalar("MULT", vec![col("a"), col("b")]),
            r#"("A" * "B")"#,
        ),
        shaped(
            "FLOAT_DIV",
            scalar("FLOAT_DIV", vec![col("a"), col("b")]),
            r#"("A" / "B")"#,
        ),
        shaped("NEG", scalar("NEG", vec![col("a")]), r#"(-"A")"#),
        shaped(
            "MOD",
            scalar("MOD", vec![col("a"), col("b")]),
            r#"MOD("A", "B")"#,
        ),
        shaped(
            "CONCAT",
            scalar("CONCAT", vec![col("a"), col("b")]),
            r#"("A" || "B")"#,
        ),
        shaped(
            "CAST",
            json!({
                "type": "function_scalar", "name": "CAST",
                "arguments": [col("v")],
                "dataType": {"type": "VARCHAR", "size": 50}
            }),
            r#"CAST("V" AS VARCHAR(50))"#,
        ),
        shaped(
            "REGEXP_LIKE",
            scalar("REGEXP_LIKE", vec![col("s"), text("^a")]),
            r#"("S" REGEXP_LIKE '^a')"#,
        ),
        shaped(
            "CASE",
            scalar(
                "CASE",
                vec![
                    json!({"type": "predicate_greater", "left": col("x"), "right": num(0)}),
                    num(1),
                    num(0),
                ],
            ),
            r#"CASE WHEN ("X" > 0) THEN 1 ELSE 0 END"#,
        ),
    ]);

    // Completeness in both directions, before anything is rendered: a
    // declared name with no fixture, and a fixture nobody declared, must each
    // fail by name.
    let missing: Vec<&str> = TRANSLATED_SCALAR_FNS
        .iter()
        .map(|(name, _)| *name)
        .filter(|name| !fixtures.iter().any(|f| f.name == *name))
        .collect();
    assert!(
        missing.is_empty(),
        "every name declared in TRANSLATED_SCALAR_FNS needs a sweep fixture; missing: \
         {missing:?}"
    );
    let undeclared: Vec<&str> = fixtures
        .iter()
        .map(|f| f.name)
        .filter(|name| declared_scalar_fn(name).is_none())
        .collect();
    assert!(
        undeclared.is_empty(),
        "every sweep fixture must name a declared function; undeclared: {undeclared:?}"
    );
    assert_eq!(
        fixtures.len(),
        TRANSLATED_SCALAR_FNS.len(),
        "the fixture map and the declaration must line up one to one; a duplicated row on \
         either side is the only way both subset checks above can pass at different sizes"
    );

    let mut swept: Vec<String> = Vec::new();

    for (declared_name, form) in TRANSLATED_SCALAR_FNS {
        let fixture = fixtures
            .iter()
            .find(|f| f.name == *declared_name)
            .expect("fixture completeness is asserted above");
        // One declaration lookup gates BOTH dialects, so a declaration row is
        // a promise about the DataFusion dialect too. A `VerbatimCall` returns
        // AHEAD of the per-name arms in the Exasol dialect, so every Exasol
        // assertion below passes whether or not the arm still exists — this
        // call is the only one that reaches the arms. No expected string is
        // asserted: the per-family paired tests own the frozen DataFusion
        // output, and a second copy here would drift from them.
        if let Err(err) = render_expression(&fixture.node) {
            panic!(
                "{declared_name} is declared in TRANSLATED_SCALAR_FNS, which gates BOTH \
                 dialects from one lookup, so it MUST render in the DataFusion dialect too; \
                 it declined with {err:?}. A declared name that has lost its per-name arm \
                 still renders in the Exasol dialect through the verbatim gate, so this is \
                 the only assertion that catches it."
            );
        }
        let rendered = render_expression_exasol(&fixture.node)
            .unwrap_or_else(|err| panic!("{declared_name} failed to render: {err:?}"));
        match form {
            ExasolForm::VerbatimCall => {
                assert!(
                    fixture.shaped_exasol.is_none(),
                    "{declared_name} is declared VerbatimCall, so its expectation MUST be \
                     derived from the node, never hand-written"
                );
                let node_name = fixture
                    .node
                    .get("name")
                    .and_then(|n| n.as_str())
                    .expect("fixture node carries a name")
                    .to_uppercase();
                let rendered_args: Vec<String> = fixture
                    .node
                    .get("arguments")
                    .and_then(|a| a.as_array())
                    .expect("fixture node carries arguments")
                    .iter()
                    .map(|arg| render_expression_exasol(arg).expect("argument renders"))
                    .collect();
                assert_eq!(
                    rendered,
                    format!("{node_name}({})", rendered_args.join(", ")),
                    "the Exasol dialect must re-emit {declared_name} as the call Exasol sent"
                );
            }
            ExasolForm::Shaped => {
                let expected = fixture.shaped_exasol.unwrap_or_else(|| {
                    panic!(
                        "{declared_name} is declared Shaped, so its fixture MUST declare the \
                         expected Exasol string"
                    )
                });
                assert_eq!(
                    rendered, expected,
                    "{declared_name} is outside the <NAME>(<args>) shape and must render its \
                     own declared form"
                );
            }
        }
        swept.push(rendered);
    }

    // The five dialect-branching node types outside `function_scalar`. They
    // are node types rather than function names, so the declaration does not
    // carry them and each row asserts both dialects itself.
    let node_type_rows = [
        (
            json!({
                "type": "function_scalar_extract", "name": "EXTRACT",
                "toExtract": "YEAR", "arguments": [col("ts")]
            }),
            r#"EXTRACT(YEAR FROM "TS")"#,
            r#"date_part('YEAR', "TS")"#,
        ),
        (
            json!({
                "type": "function_scalar_cast", "name": "CAST",
                "arguments": [col("v")],
                "dataType": {"type": "VARCHAR", "size": 50}
            }),
            r#"CAST("V" AS VARCHAR(50))"#,
            r#"CAST("V" AS VARCHAR)"#,
        ),
        (
            json!({
                "type": "predicate_like_regexp",
                "expression": col("s"), "pattern": text("^a")
            }),
            r#"("S" REGEXP_LIKE '^a')"#,
            r#"regexp_like("S", '^a')"#,
        ),
        (
            json!({"type": "literal_timestamp", "value": "2024-03-01 12:34:56.789"}),
            "TIMESTAMP '2024-03-01 12:34:56.789'",
            "arrow_cast('2024-03-01 12:34:56.789', 'Timestamp(Microsecond, None)')",
        ),
        (
            json!({"type": "literal_timestamp_utc", "value": "2024-03-01 12:34:56.789"}),
            "CAST(CONVERT_TZ(TIMESTAMP '2024-03-01 12:34:56.789', 'UTC', SESSIONTIMEZONE) \
             AS TIMESTAMP WITH LOCAL TIME ZONE)",
            r#"arrow_cast('2024-03-01 12:34:56.789+00:00', 'Timestamp(Microsecond, Some("UTC"))')"#,
        ),
    ];
    for (node, expected_exasol, expected_datafusion) in node_type_rows {
        let node_type = node["type"]
            .as_str()
            .expect("row carries a node type")
            .to_string();
        let rendered = render_expression_exasol(&node)
            .unwrap_or_else(|err| panic!("{node_type} failed to render: {err:?}"));
        assert_eq!(
            rendered, expected_exasol,
            "{node_type} in the Exasol dialect"
        );
        assert_eq!(
            render_expression(&node).unwrap(),
            expected_datafusion,
            "{node_type} in the DataFusion dialect"
        );
        swept.push(rendered);
    }

    // Secondary guard over everything swept above. The comparison is
    // deliberately case-SENSITIVE: `OCTET_LENGTH("S")` and `NULLIF("A", "B")`
    // are correct Exasol renderings, and it is their lowercase DataFusion
    // twins that must never reach an Exasol-parsed wrapper. `current_date()`
    // and `now()` are live guards now that the now-family is undeclared —
    // re-adding a DataFusion-shaped now-family arm trips them.
    for rendered in &swept {
        for token in [
            "signum",
            "date_part",
            "strpos",
            "arrow_cast",
            "character_length",
            "octet_length",
            "regexp_like(",
            "current_date()",
            "now()",
            "nullif(",
            "coalesce(",
        ] {
            assert!(
                !rendered.contains(token),
                "Exasol-dialect output must not contain the DataFusion-only token `{token}`, \
                 but rendered: {rendered}"
            );
        }
    }
}

// --- Dialect-invariant surface (regression freeze) ---
//
// This plan branches `function_scalar_extract`, `predicate_like_regexp`, the
// `REGEXP_LIKE` alternate encoding, and the two timestamp-literal node types
// on `dialect`. Everything else was not meant to move. These three tests
// freeze the surface that MUST stay dialect-invariant, so a future change
// that accidentally starts branching one of these paths on `dialect` fails
// here instead of only showing up as a silent divergence downstream.

#[test]
fn arithmetic_operators_render_identically_in_both_dialects() {
    // The `ADD`/`SUB`/`MULT`/`NEG` wire names never inspect `dialect` in their
    // own arm — `render_expression_inner` renders the same `(<left> <op>
    // <right>)` / `(-<operand>)` shape regardless of which dialect is
    // requested. Pins that invariance directly, on the same node, for both
    // dialects at once.
    let binary = [("ADD", "+"), ("SUB", "-"), ("MULT", "*")];
    for (name, op) in binary {
        let expr = json!({
            "type": "function_scalar",
            "name": name,
            "arguments": [
                {"type": "column", "name": "a"},
                {"type": "literal_exactnumeric", "value": 1}
            ]
        });
        let expected = format!(r#"("A" {op} 1)"#);
        assert_eq!(
            render_expression(&expr).unwrap(),
            expected,
            "{name} DataFusion dialect"
        );
        assert_eq!(
            render_expression_exasol(&expr).unwrap(),
            expected,
            "{name} Exasol dialect"
        );
    }

    let neg = json!({
        "type": "function_scalar",
        "name": "NEG",
        "arguments": [{"type": "column", "name": "a"}]
    });
    let expected_neg = r#"(-"A")"#;
    assert_eq!(
        render_expression(&neg).unwrap(),
        expected_neg,
        "NEG DataFusion dialect"
    );
    assert_eq!(
        render_expression_exasol(&neg).unwrap(),
        expected_neg,
        "NEG Exasol dialect"
    );
}

/// Divergence guard: FLOAT_DIV casts its left operand to DOUBLE only in the
/// DataFusion dialect; the Exasol dialect renders the bare operator.
#[test]
fn float_div_casts_to_double_only_in_the_datafusion_dialect() {
    let float_div = json!({
        "type": "function_scalar",
        "name": "FLOAT_DIV",
        "arguments": [
            {"type": "column", "name": "a"},
            {"type": "literal_exactnumeric", "value": 1}
        ]
    });
    assert_eq!(
        render_expression(&float_div).unwrap(),
        r#"(CAST("A" AS DOUBLE) / 1)"#,
        "FLOAT_DIV DataFusion dialect"
    );
    assert_eq!(
        render_expression_exasol(&float_div).unwrap(),
        r#"("A" / 1)"#,
        "FLOAT_DIV Exasol dialect"
    );
}

#[test]
fn non_timestamp_literals_render_identically_in_both_dialects() {
    // Every literal node type except `literal_timestamp` and
    // `literal_timestamp_utc` (branched on dialect by task 5) renders the
    // same string in both dialects, because none of these arms reads
    // `dialect` at all.
    let cases: [(Json, &str); 7] = [
        (json!({"type": "literal_null"}), "NULL"),
        (json!({"type": "literal_bool", "value": true}), "TRUE"),
        (json!({"type": "literal_bool", "value": false}), "FALSE"),
        (
            json!({"type": "literal_string", "value": "it's"}),
            "'it''s'",
        ),
        (json!({"type": "literal_exactnumeric", "value": 42}), "42"),
        (json!({"type": "literal_double", "value": 0.5}), "0.5"),
        (
            json!({"type": "literal_date", "value": "2024-01-15"}),
            "DATE '2024-01-15'",
        ),
    ];
    for (node, expected) in cases {
        let node_type = node["type"].as_str().unwrap();
        assert_eq!(
            render_expression(&node).unwrap(),
            expected,
            "{node_type} DataFusion dialect"
        );
        assert_eq!(
            render_expression_exasol(&node).unwrap(),
            expected,
            "{node_type} Exasol dialect"
        );
    }
}

#[test]
fn exasol_df_filter_suppresses_trivially_true() {
    // Exasol-dialect twin of `true_filter_returns_none_in_safe_mode` /
    // `null_filter_returns_none_in_safe_mode` above: `render_df_filter_exasol_safe`
    // suppresses a trivially-true (`TRUE` or `NULL`) filter exactly like
    // `render_df_filter_safe` does. A trivially-true filter is a correct
    // no-op to omit from the scan spec — but that is one of two
    // distinguishable causes of a `None` return, regardless of which
    // dialect rendered the fragment. The other cause, a genuine decline,
    // must be self-applied by the caller — a declined predicate omitted
    // here would be silently lost, not backstopped.
    let true_filter = json!({"type": "literal_bool", "value": true});
    assert!(render_df_filter_exasol_safe(&true_filter).is_none());

    let null_filter = json!({"type": "literal_null"});
    assert!(render_df_filter_exasol_safe(&null_filter).is_none());
}
