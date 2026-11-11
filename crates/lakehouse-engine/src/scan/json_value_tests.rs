use super::*;
use crate::scan::scan_session_context;
use crate::scan::test_support::current_thread_runtime;
use crate::scan::test_support::invoke_scalar_udf;
use arrow::array::{BooleanArray, StringArray};
use arrow::util::pretty::pretty_format_batches;
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::prelude::SessionConfig;

/// One batch of `documents`, with `constants` after them.
fn invoke(
    returning: Returning,
    documents: Vec<Option<&str>>,
    constants: Vec<ScalarValue>,
) -> ArrayRef {
    let name = match returning {
        Returning::Text => JSON_VALUE_TEXT_FN,
        Returning::Boolean => JSON_VALUE_BOOLEAN_FN,
    };
    let udf = JsonUdf {
        name,
        returning,
        signature: Signature::any(constants.len() + 1, Volatility::Immutable),
    };
    let rows = documents.len();
    let mut args = vec![ColumnarValue::Array(Arc::new(StringArray::from(documents)))];
    args.extend(constants.into_iter().map(ColumnarValue::Scalar));
    invoke_scalar_udf(&udf, args, rows)
        .and_then(|values| values.to_array(rows))
        .expect("constant arguments")
}

fn text(value: &str) -> ScalarValue {
    ScalarValue::Utf8(Some(value.to_string()))
}

#[test]
fn text_function_applies_the_behaviors() {
    let values = invoke(
        Returning::Text,
        vec![
            None,
            Some(""),
            Some("{"),
            Some(r#"{"b":1}"#),
            Some(r#"{"a":"abcd"}"#),
            Some(r#"{"a":"ab"}"#),
            Some(r#"{"a":null}"#),
            Some(r#"{"a":""}"#),
        ],
        vec![
            text("$.a"),
            ScalarValue::Int64(Some(3)),
            text("e"),
            text("x"),
        ],
    );
    let expected = StringArray::from(vec![
        None,
        None,
        Some("x"),
        Some("e"),
        Some("x"),
        Some("ab"),
        None,
        None,
    ]);
    assert_eq!(values.as_string::<i32>(), &expected);
}

#[test]
fn boolean_function_applies_the_behaviors() {
    let values = invoke(
        Returning::Boolean,
        vec![
            None,
            Some(r#"{"a":"yes"}"#),
            Some(r#"{"a":0}"#),
            Some(r#"{"b":1}"#),
            Some(r#"{"a":"on"}"#),
        ],
        vec![
            text("$.a"),
            ScalarValue::Boolean(Some(true)),
            ScalarValue::Null,
        ],
    );
    let expected = BooleanArray::from(vec![None, Some(true), Some(false), Some(true), None]);
    assert_eq!(values.as_boolean(), &expected);
}

fn selected(doc: &str, path: &str) -> String {
    let root = parse(doc);
    format!(
        "{:?}",
        select(
            root.as_ref(),
            &parse_json_path(path).expect("path in the subset")
        )
    )
}

/// Each expected outcome is what Exasol 2025.1 returns for the same document and path.
#[test]
fn paths_select_what_exasol_selects() {
    let cases = [
        (r#"{"a":"x"}"#, "$.a", r#"Text("x")"#),
        (r#" {"a" : 1.5 } "#, "$.a", r#"Text("1.5")"#),
        (r#"{"a":-12}"#, "$.a", r#"Text("-12")"#),
        (r#"{"a":true}"#, "$.a", r#"Text("true")"#),
        (r#"{"a":null}"#, "$.a", "Null"),
        (r#"{"a":null}"#, "$.a.b", "Empty"),
        (r#"{"a":"é😀\/"}"#, "$.a", r#"Text("é😀/")"#),
        (r#"{"A":1}"#, "$.a", "Empty"),
        (r#"{"a":1}"#, "$.b", "Empty"),
        (r#"{"a":{"b":1}}"#, "$.a", "Error"),
        (r#"{"a":[1]}"#, "$.a", "Error"),
        (r#"{"a":1}"#, "$", "Error"),
        (r#""s""#, "$", r#"Text("s")"#),
        (r#"{"a":1,"b":2,"b":3}"#, "$.a", r#"Text("1")"#),
        (r#"{"a":[{"b":1}]}"#, "$.a.b", r#"Text("1")"#),
        (r#"{"a":[{"b":1},{"b":2}]}"#, "$.a.b", "Error"),
        (r#"{"a":[{"b":1},{"c":2},5]}"#, "$.a.b", r#"Text("1")"#),
        (r#"{"a":[[{"b":1}]]}"#, "$.a.b", "Empty"),
        (r#"{"a":[10,20]}"#, "$.a[1]", r#"Text("20")"#),
        (r#"{"a":[10,20]}"#, "$.a[5]", "Empty"),
        (r#"{"a":7}"#, "$.a[0]", r#"Text("7")"#),
        (r#"{"a":7}"#, "$.a[1]", "Empty"),
        (r#"{"a":{"b":1}}"#, "$.a[0].b", r#"Text("1")"#),
        (r#"{"a":[{"b":[5,6]}]}"#, "$.a.b[1]", r#"Text("6")"#),
        (r#"{"a b":1,"":2}"#, r#"$."""#, r#"Text("2")"#),
        ("{}", "$.a", "Empty"),
        ("[]", "$[0]", "Empty"),
    ];
    for (doc, path, expected) in cases {
        assert_eq!(selected(doc, path), expected, "{doc} {path}");
    }
}

/// Exasol applies `ON ERROR` to each of these.
#[test]
fn documents_that_are_not_strict_json_fail() {
    let tab = "{\"a\":\"x\ty\"}";
    for doc in [
        r#"{"a":1,"#,
        r#"{"a":1} x"#,
        r#"{"a":1}{"a":2}"#,
        r#"{"a":01}"#,
        r#"{"a":+1}"#,
        r#"{"a":.5}"#,
        r#"{"a":1.}"#,
        r#"{"a":1e}"#,
        r#"{"a":-}"#,
        r#"{"a":NaN}"#,
        r#"{"a":TRUE}"#,
        r#"{a:1}"#,
        r#"{'a':1}"#,
        r#"[1,]"#,
        r#"{"a":1,}"#,
        r#"{"a":"\q"}"#,
        r#"{"a":"\udc00","b":1}"#,
        tab,
        "{\"a\":\u{c}1}",
        "\u{feff}{\"a\":1}",
        "   ",
    ] {
        assert_eq!(selected(doc, "$.b"), "Error", "{doc:?}");
    }
}

/// docs/semantic-differences.md lists each of these.
#[test]
fn parsed_documents_differ_from_exasol_as_documented() {
    let deep = |depth| {
        format!(
            r#"{{"a":{}1{},"b":1}}"#,
            "[".repeat(depth),
            "]".repeat(depth)
        )
    };
    let cases = [
        (r#"{"a":1.50}"#.to_string(), "$.a", r#"Text("1.5")"#),
        (r#"{"a":1e2}"#.to_string(), "$.a", r#"Text("100.0")"#),
        (r#"{"a":-1.5E+3}"#.to_string(), "$.a", r#"Text("-1500.0")"#),
        (r#"{"a":-0}"#.to_string(), "$.a", r#"Text("-0.0")"#),
        (
            r#"{"a":12345678901234567890123}"#.to_string(),
            "$.a",
            r#"Text("1.2345678901234568e+22")"#,
        ),
        (r#"{"a":1,"a":2}"#.to_string(), "$.a", r#"Text("2")"#),
        (r#"{"a":{"b":1},"a":{"c":2}}"#.to_string(), "$.a.b", "Empty"),
        (deep(120), "$.b", r#"Text("1")"#),
        (deep(200), "$.b", "Error"),
    ];
    for (doc, path, expected) in cases {
        assert_eq!(selected(&doc, path), expected, "{doc} {path}");
    }
}

#[test]
fn text_to_boolean_follows_exasol_cast() {
    for (text, expected) in [
        ("true", Some(true)),
        ("TRUE", Some(true)),
        ("  yes ", Some(true)),
        ("t", Some(true)),
        ("Y", Some(true)),
        ("1", Some(true)),
        ("001", Some(true)),
        ("false", Some(false)),
        ("No", Some(false)),
        ("0", Some(false)),
        ("000", Some(false)),
        ("10", None),
        ("2", None),
        ("-0", None),
        ("1.0", None),
        ("1e0", None),
        ("on", None),
        ("\ttrue", None),
        ("true\n", None),
        ("", None),
    ] {
        assert_eq!(boolean_from_text(text), expected, "{text:?}");
    }
}

#[test]
fn text_results_count_characters_and_read_empty_as_null() {
    for (text, max_chars, expected) in [
        ("éé€", 3, Ok(Some("éé€"))),
        ("😀x", 1, Err(())),
        ("12345", 3, Err(())),
        ("false", 3, Err(())),
        ("false", 5, Ok(Some("false"))),
        ("", 3, Ok(None)),
    ] {
        assert_eq!(text_value(text, max_chars), expected, "{text:?}");
    }
}

/// The scalar functions the optimized plan of `sql` evaluates, and its rows.
fn evaluated_functions_and_rows(sql: &str) -> (Vec<String>, String) {
    let ctx = scan_session_context(SessionConfig::new(), Arc::default());
    let (plan, rows) = current_thread_runtime()
        .block_on(async {
            let frame = ctx.sql(sql).await?;
            let plan = frame.clone().into_optimized_plan()?;
            let rows = pretty_format_batches(&frame.collect().await?)?.to_string();
            Ok::<_, DataFusionError>((plan, rows))
        })
        .expect("query runs");
    let mut functions = Vec::new();
    plan.apply(|node| {
        node.apply_expressions(|expr| {
            expr.apply(|e| {
                if let Expr::ScalarFunction(call) = e {
                    functions.push(call.name().to_string());
                }
                Ok(TreeNodeRecursion::Continue)
            })
        })
    })
    .expect("plan walk");
    (functions, rows)
}

/// Scenario: Several JSON_VALUE calls on one document parse it once per row
#[test]
fn calls_on_one_document_share_one_parse() {
    let docs = r#"(VALUES ('{"a":"x","b":1.5,"c":true}'), ('{"a":"y","c":false}'), (NULL)) t(doc)"#;
    let text = |path: &str| format!("{JSON_VALUE_TEXT_FN}(doc, '{path}', 10, 'none', NULL)");
    let boolean = |path: &str| format!("{JSON_VALUE_BOOLEAN_FN}(doc, '{path}', NULL, NULL)");
    let (a, b, c) = (text("$.a"), text("$.b"), boolean("$.c"));
    for (sql, expected) in [
        (
            format!("SELECT {a} AS a, {b} AS b, {c} AS c FROM {docs} ORDER BY a"),
            "| x | 1.5  | true  |\n| y | none | false |\n|   |      |       |",
        ),
        (
            format!("SELECT doc IS NULL AS n FROM {docs} WHERE {a} = 'x' OR {c} IS NULL"),
            "| false |\n| true  |",
        ),
        (
            format!("SELECT {a} AS a, COUNT({b}) AS b FROM {docs} GROUP BY {a} ORDER BY a"),
            "| x | 1 |\n| y | 1 |\n|   | 0 |",
        ),
    ] {
        let (functions, rows) = evaluated_functions_and_rows(&sql);
        let count = |name: &str| functions.iter().filter(|f| *f == name).count();
        assert_eq!(count(JSON_VALUES_FN), 1, "{sql}\n{functions:?}");
        assert_eq!(
            count(JSON_VALUE_TEXT_FN) + count(JSON_VALUE_BOOLEAN_FN),
            0,
            "{sql}"
        );
        assert!(rows.contains(expected), "{sql}\n{rows}");
    }
}
