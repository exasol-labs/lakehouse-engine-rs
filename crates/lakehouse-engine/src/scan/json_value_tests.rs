use super::*;
use crate::scan::scan_session_context;
use crate::scan::test_support::{current_thread_runtime, invoke_scalar_udf};
use arrow::array::{BooleanArray, StringArray};
use arrow::util::pretty::pretty_format_batches;
use datafusion::prelude::SessionConfig;

/// One batch of `documents` as `document_type`, with `constants` after them.
fn invoke(
    returning: Returning,
    document_type: &DataType,
    documents: Vec<Option<&str>>,
    constants: Vec<ScalarValue>,
) -> ArrayRef {
    let udf = json_udf(returning, false);
    let rows = documents.len();
    let documents: ArrayRef = Arc::new(StringArray::from(documents));
    let documents = cast(&documents, document_type).expect("a string type");
    let mut args = vec![ColumnarValue::Array(documents)];
    args.extend(constants.into_iter().map(ColumnarValue::Scalar));
    invoke_scalar_udf(udf.inner().as_ref(), args, rows)
        .and_then(|values| values.to_array(rows))
        .expect("constant arguments")
}

fn text(value: &str) -> ScalarValue {
    ScalarValue::Utf8(Some(value.to_string()))
}

#[test]
fn text_function_applies_the_behaviors_to_every_string_type() {
    let documents = vec![
        None,
        Some(""),
        Some("{"),
        Some(r#"{"b":1}"#),
        Some(r#"{"a":"abcd"}"#),
        Some(r#"{"a":"ab"}"#),
        Some(r#"{"a":null}"#),
        Some(r#"{"a":""}"#),
    ];
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
    for document_type in [
        DataType::Utf8,
        DataType::LargeUtf8,
        DataType::Utf8View,
        DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
    ] {
        let values = invoke(
            Returning::Text,
            &document_type,
            documents.clone(),
            vec![
                text("$.a"),
                ScalarValue::Int64(Some(3)),
                text("e"),
                text("x"),
            ],
        );
        assert_eq!(values.as_string::<i32>(), &expected, "{document_type}");
    }
}

#[test]
fn boolean_function_applies_the_behaviors() {
    let values = invoke(
        Returning::Boolean,
        &DataType::Utf8,
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

fn steps(path: &str) -> Vec<JsonPathStep> {
    parse_json_path(path).expect("path in the subset")
}

/// What `paths` select in one read of `doc`.
fn selected_all(doc: &str, paths: &[&str], strict: bool) -> Vec<String> {
    let steps: Vec<Vec<JsonPathStep>> = paths.iter().map(|path| steps(path)).collect();
    let paths: Vec<&[JsonPathStep]> = steps.iter().map(Vec::as_slice).collect();
    let items = select(doc, &paths, strict);
    items.iter().map(|item| format!("{item:?}")).collect()
}

fn selected(doc: &str, path: &str, strict: bool) -> String {
    selected_all(doc, &[path], strict).remove(0)
}

/// Each expected outcome is what Exasol 2025.1 returns for the same document and path.
#[test]
fn paths_select_what_exasol_selects() {
    let cases = [
        (r#"{"a":"x"}"#, "$.a", r#"Text("x")"#),
        (r#" {"a" : 1.50 } "#, "$.a", r#"Text("1.50")"#),
        (r#"{"a":-1.5E+3}"#, "$.a", r#"Text("-1.5E+3")"#),
        (
            r#"{"a":12345678901234567890123}"#,
            "$.a",
            r#"Text("12345678901234567890123")"#,
        ),
        (r#"{"a":1e400}"#, "$.a", r#"Text("1e400")"#),
        (r#"{"a":-0}"#, "$.a", r#"Text("-0")"#),
        (r#"{"a":true}"#, "$.a", r#"Text("true")"#),
        (r#"{"a":null}"#, "$.a", "Null"),
        (r#"{"a":null}"#, "$.a.b", "Empty"),
        (r#"{"a":"é😀\/"}"#, "$.a", r#"Text("é😀/")"#),
        (r#"{"a":"😀"}"#, "$.a", r#"Text("😀")"#),
        (r#"{"a":1}"#, "$.a", r#"Text("1")"#),
        (r#"{"a":1}"#, "$.a", r#"Text("1")"#),
        (r#"{"A":1}"#, "$.a", "Empty"),
        (r#"{"a":1}"#, "$.b", "Empty"),
        (r#"{"a":{"b":1}}"#, "$.a", "Error"),
        (r#"{"a":[1]}"#, "$.a", "Error"),
        (r#"{"a":1}"#, "$", "Error"),
        (r#""s""#, "$", r#"Text("s")"#),
        (r#"{"a":{"b":1},"a":{"c":2}}"#, "$.a.b", r#"Text("1")"#),
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
    for strict in [true, false] {
        for (doc, path, expected) in cases {
            assert_eq!(
                selected(doc, path, strict),
                expected,
                "{doc} {path} {strict}"
            );
        }
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
        r#"{"a":"\ud800"}"#,
        r#"{"b":"\udc00","a":1}"#,
        tab,
        "{\"a\":\u{c}1}",
        "\u{feff}{\"a\":1}",
        "   ",
    ] {
        for path in ["$", "$.a", "$.x"] {
            assert_eq!(selected(doc, path, true), "Error", "{doc:?} {path}");
        }
    }
}

/// docs/semantic-differences.md: a fast read stops once no path can select anything more.
#[test]
fn a_fast_read_ignores_the_document_after_the_selected_values() {
    let cases = [
        (r#"{"a":1} x"#, "$.x", "Empty"),
        (r#"{"a":1,"#, "$.x", "Error"),
        (r#"{"a":1,"b":01}"#, "$.a", r#"Text("1")"#),
        (r#"{"a":1} x"#, "$.a", r#"Text("1")"#),
        (r#"{"a":"x","b":"\ud800"}"#, "$.a", r#"Text("x")"#),
        (r#"{"a":1,"a":2}"#, "$.a", r#"Text("1")"#),
        (r#"{"a":{"c":2},"a":{"b":1}}"#, "$.a.b", "Empty"),
        (r#"{"a":[10,20,x"#, "$.a[1]", r#"Text("20")"#),
        // A member step reads the whole array, and a selected container the whole value.
        (r#"{"a":[{"b":1},{"c":2}]} x"#, "$.a.b", r#"Text("1")"#),
        (r#"{"a":[{"b":1},{"c":2},x]}"#, "$.a.b", "Error"),
        (r#"{"a":{"b":x}}"#, "$.a", "Error"),
        (r#"{"b":01,"a":1}"#, "$.a", "Error"),
    ];
    for (doc, path, expected) in cases {
        assert_eq!(selected(doc, path, false), expected, "{doc} {path}");
    }
    assert_eq!(selected(r#"{"a":1,"a":2}"#, "$.a", true), "Error");
    assert_eq!(selected(r#"{"a":1} x"#, "$.a", true), "Error");
}

/// Scenario: Several JSON_VALUE calls on one document parse it once per row
#[test]
fn one_read_selects_every_path() {
    let doc = r#"{"a":{"b":1,"c":"x"},"d":[true,null],"e":{}} trailing"#;
    let paths = ["$.a.c", "$.d[1]", "$.a", "$.a.b", "$.d[0]", "$.zz"];
    let expected = [
        r#"Text("x")"#,
        "Null",
        "Error",
        r#"Text("1")"#,
        r#"Text("true")"#,
    ];
    let mut all = expected.to_vec();
    all.push("Empty");
    assert_eq!(selected_all(doc, &paths, false), all);
    assert_eq!(selected_all(doc, &paths, true), vec!["Error"; 6]);
    let valid = doc.trim_end_matches(" trailing");
    assert_eq!(selected_all(valid, &paths, true), all);
}

#[test]
fn deep_documents_take_on_error() {
    let nested = |depth: usize| {
        format!(
            r#"{{"a":{}1{},"b":1}}"#,
            "[".repeat(depth),
            "]".repeat(depth)
        )
    };
    for strict in [true, false] {
        assert_eq!(selected(&nested(100), "$.b", strict), r#"Text("1")"#);
        assert_eq!(selected(&nested(100_000), "$.b", strict), "Error");
    }
    let deep_path = format!("${}", ".a".repeat(MAX_DEPTH + 1));
    let deep_doc = format!(
        "{}1{}",
        r#"{"a":"#.repeat(MAX_DEPTH + 1),
        "}".repeat(MAX_DEPTH + 1)
    );
    assert_eq!(selected(&deep_doc, &deep_path, false), "Error");
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
