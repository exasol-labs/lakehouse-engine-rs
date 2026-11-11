use super::*;
use arrow::datatypes::Field;
use datafusion::common::config::ConfigOptions;

/// Drives `invoke_with_args` as DataFusion does per batch, with `constants` after the documents.
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
    let arg_fields = args
        .iter()
        .map(|arg| Arc::new(Field::new("arg", arg.data_type(), true)))
        .collect();
    let return_type = udf.return_type(&[]).expect("fixed return type");
    let args = ScalarFunctionArgs {
        arg_fields,
        args,
        number_rows: rows,
        return_field: Arc::new(Field::new(name, return_type, true)),
        config_options: Arc::new(ConfigOptions::default()),
    };
    udf.invoke_with_args(args)
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
    format!(
        "{:?}",
        select(doc, &parse_json_path(path).expect("path in the subset"))
    )
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
        (r#"{"a":true}"#, "$.a", r#"Text("true")"#),
        (r#"{"a":null}"#, "$.a", "Null"),
        (r#"{"a":null}"#, "$.a.b", "Empty"),
        (r#"{"a":"é😀\/"}"#, "$.a", r#"Text("é😀/")"#),
        (r#"{"a":1}"#, "$.a", r#"Text("1")"#),
        (r#"{"A":1}"#, "$.a", "Empty"),
        (r#"{"a":1}"#, "$.b", "Empty"),
        (r#"{"a":{"b":1}}"#, "$.a", "Error"),
        (r#"{"a":[1]}"#, "$.a", "Error"),
        (r#"{"a":1}"#, "$", "Error"),
        (r#""s""#, "$", r#"Text("s")"#),
        (r#"{"a":1,"a":2}"#, "$.a", "Error"),
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
        tab,
        "{\"a\":\u{c}1}",
        "\u{feff}{\"a\":1}",
        "   ",
    ] {
        assert_eq!(select(doc, &[]), Item::Error, "{doc:?}");
    }
}

/// A lone surrogate escape fails only in the selected item, see docs/semantic-differences.md.
#[test]
fn lone_surrogates_fail_in_the_selected_item() {
    assert_eq!(selected(r#"{"a":"\ud800"}"#, "$.a"), "Error");
    assert_eq!(selected(r#"{"a":"\udc00","b":1}"#, "$.b"), r#"Text("1")"#);
}

#[test]
fn nesting_depth_is_unbounded() {
    let depth = 1_000_000;
    let doc = format!(r#"{{"a":{}1{}}}"#, "[".repeat(depth), "]".repeat(depth));
    assert_eq!(selected(&doc, "$.b"), "Empty");
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
