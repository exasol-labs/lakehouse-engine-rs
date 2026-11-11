use super::*;
use crate::test_support::*;
use crate::{exact_expression_type, render_expression_exasol, render_scan_expression};

fn columns() -> TypedColumns {
    typed_columns(&[
        ("DOC", ExasolType::WIDEST_VARCHAR),
        ("N", ExasolType::decimal(9, 0)),
    ])
}

fn null() -> Json {
    json_behavior("NULL")
}

#[test]
fn paths_of_members_and_indexes_parse() {
    use JsonPathStep::{Index, Member};
    let member = |name: &str| Member(name.to_string());
    let cases = [
        ("$", vec![]),
        ("$._a1.b", vec![member("_a1"), member("b")]),
        ("$.size", vec![member("size")]),
        (r#"$."a b"[0]"#, vec![member("a b"), Index(0)]),
        (r#"$."""#, vec![member("")]),
        ("$[2147483647]", vec![Index(2_147_483_647)]),
        (
            "$.a[1][10].c",
            vec![member("a"), Index(1), Index(10), member("c")],
        ),
    ];
    for (path, steps) in cases {
        assert_eq!(parse_json_path(path), Some(steps), "{path}");
    }
}

/// Exasol parses some of these and fails the statement on others; either way the scan declines.
#[test]
fn paths_outside_the_subset_decline() {
    for path in [
        "",
        "a",
        "$.",
        "$.1a",
        "$.a$",
        "$.a-b",
        "$.é",
        " $.a",
        "$.a ",
        "$. a",
        "lax $.a",
        "strict $.a",
        "$.*",
        "$[*]",
        "$[0,1]",
        "$[0 to 1]",
        "$[last]",
        "$[-1]",
        "$[01]",
        "$[2147483648]",
        "$..a",
        "$.a.size()",
        r#"$."a\"b""#,
        r#"$."a"#,
        "$[0",
    ] {
        assert_eq!(parse_json_path(path), None, "{path:?}");
    }
}

#[test]
fn session_free_calls_render_to_the_scan_functions() {
    let doc = || col("DOC");
    let cases = [
        (
            json_value(doc(), "$.a"),
            "lakehouse_json_value_text(\"DOC\", '$.a', 2000000, NULL, NULL)",
        ),
        (
            json_value_as(
                doc(),
                r#"$."it's""#,
                ExasolType::varchar(4),
                json_default(str_lit("it's")),
                json_default(str_lit("")),
            ),
            "lakehouse_json_value_text(\"DOC\", '$.\"it''s\"', 4, 'it''s', NULL)",
        ),
        (
            json_value_as(
                doc(),
                "$[0]",
                ExasolType::Boolean,
                json_default(bool_lit(true)),
                json_default(null_lit()),
            ),
            "lakehouse_json_value_boolean(\"DOC\", '$[0]', TRUE, NULL)",
        ),
    ];
    for (call, sql) in cases {
        assert_eq!(render_scan_expression(&call, &columns()).unwrap().sql, sql);
    }
}

#[test]
fn calls_the_scan_cannot_compute_decline() {
    let doc = || col("DOC");
    let returning = |ty| json_value_as(doc(), "$.a", ty, null(), null());
    let text_with = |on_empty, on_error| {
        json_value_as(doc(), "$.a", ExasolType::varchar(3), on_empty, on_error)
    };
    let mut path_node = json_value(doc(), "$.a");
    path_node["arguments"][1] = col("DOC");
    let cases = [
        ("RETURNING DECIMAL", returning(ExasolType::decimal(18, 0))),
        ("RETURNING DOUBLE", returning(ExasolType::DoublePrecision)),
        ("RETURNING DATE", returning(ExasolType::Date)),
        ("RETURNING TIMESTAMP", returning(ExasolType::timestamp(3))),
        ("RETURNING ASCII", returning(ascii_varchar(10))),
        (
            "RETURNING CHAR",
            returning(ExasolType::Char {
                size: 3,
                ascii: false,
            }),
        ),
        ("ERROR ON EMPTY", text_with(json_behavior("ERROR"), null())),
        ("ERROR ON ERROR", text_with(null(), json_behavior("ERROR"))),
        (
            "DEFAULT longer than the size",
            text_with(json_default(str_lit("abcd")), null()),
        ),
        (
            "DEFAULT number",
            text_with(json_default(num_lit(4)), null()),
        ),
        (
            "DEFAULT column",
            text_with(null(), json_default(col("DOC"))),
        ),
        (
            "DEFAULT text for BOOLEAN",
            json_value_as(
                doc(),
                "$.a",
                ExasolType::Boolean,
                json_default(str_lit("true")),
                null(),
            ),
        ),
        ("path outside the subset", json_value(doc(), "$.*")),
        ("path not a literal", path_node),
        ("document not text", json_value(col("N"), "$.a")),
    ];
    for (case, call) in cases {
        assert!(
            render_scan_expression(&call, &columns()).is_err(),
            "{case} must decline"
        );
    }
}

#[test]
fn exasol_dialect_reemits_every_clause() {
    let call = json_value_as(
        col("DOC"),
        "$.a",
        ExasolType::decimal(18, 0),
        json_default(num_lit(0)),
        json_behavior("ERROR"),
    );
    assert_eq!(
        render_expression_exasol(&call).unwrap(),
        "JSON_VALUE(\"DOC\", '$.a' RETURNING DECIMAL(18,0) DEFAULT 0 ON EMPTY ERROR ON ERROR)"
    );
}

#[test]
fn the_returning_type_is_the_exact_type() {
    let call = json_value_as(
        col("DOC"),
        "$.a",
        ExasolType::decimal(18, 0),
        null(),
        null(),
    );
    assert_eq!(
        exact_expression_type(&call, &columns()),
        Some(ExasolType::decimal(18, 0))
    );
}
