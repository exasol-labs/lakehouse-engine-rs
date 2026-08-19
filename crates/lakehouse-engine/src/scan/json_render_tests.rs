use super::*;
use arrow::array::{
    ArrayRef, BooleanArray, Date32Array, FixedSizeBinaryArray, FixedSizeListArray, Int32Array,
    LargeListArray, LargeStringArray, ListArray, MapArray, StringArray, StructArray,
};
use arrow::buffer::{NullBuffer, OffsetBuffer};
use arrow::datatypes::{DataType, Field, Fields};
use std::sync::Arc;

/// Assert `text` is a JSON document a real parser accepts, and return it parsed.
fn parsed(text: &str) -> serde_json::Value {
    serde_json::from_str(text).unwrap_or_else(|e| panic!("{text} must parse as JSON: {e}"))
}

/// Render `array` and return every cell as `Option<String>` — `None` for a SQL NULL.
fn rendered(array: ArrayRef) -> Vec<Option<String>> {
    let out = render_nested_column_as_json(&array).expect("a nested column must render");
    (0..out.len())
        .map(|idx| (!out.is_null(idx)).then(|| out.value(idx).to_string()))
        .collect()
}

/// One `list<utf8>` column from rows of optional elements.
fn list_of_strings(rows: Vec<Option<Vec<Option<&str>>>>) -> ArrayRef {
    let mut builder = arrow::array::ListBuilder::new(arrow::array::StringBuilder::new());
    for row in rows {
        match row {
            None => builder.append_null(),
            Some(elements) => {
                for element in elements {
                    builder.values().append_option(element);
                }
                builder.append(true);
            }
        }
    }
    Arc::new(builder.finish())
}

/// One `map<K,V>` column assembled from its key child, value child, entry offsets and
/// cell nulls — the four parts `MapArray` is made of, so a key of any type is expressible.
fn map_column(
    keys: ArrayRef,
    values: ArrayRef,
    offsets: &[i32],
    nulls: Option<NullBuffer>,
) -> ArrayRef {
    let entries = StructArray::try_new(
        Fields::from(vec![
            Field::new("keys", keys.data_type().clone(), false),
            Field::new("values", values.data_type().clone(), true),
        ]),
        vec![keys, values],
        None,
    )
    .expect("the entries struct must build");
    let entries_field = Arc::new(Field::new("entries", entries.data_type().clone(), false));
    Arc::new(
        MapArray::try_new(
            entries_field,
            OffsetBuffer::new(offsets.to_vec().into()),
            entries,
            nulls,
            false,
        )
        .expect("the map column must build"),
    )
}

fn utf8(values: &[&str]) -> ArrayRef {
    Arc::new(StringArray::from(values.to_vec()))
}

/// nested-json-rendering / A list, struct, or map value renders as one valid JSON document.
///
/// Every shape carries POPULATED values, and every rendered cell is handed to a real JSON
/// parser — the Arrow display text this encoder replaces (`[hello, world]`) would fail that.
#[test]

fn populated_nested_values_render_as_valid_json_documents() {
    let assert_every_cell_parses = |cells: &[Option<String>]| {
        for cell in cells.iter().flatten() {
            parsed(cell);
        }
    };

    let lists = rendered(list_of_strings(vec![
        Some(vec![Some("hello"), Some("world")]),
        Some(vec![Some(r#"say "hi"\"#)]),
        Some(vec![]),
    ]));
    assert_every_cell_parses(&lists);
    assert_eq!(
        lists,
        vec![
            Some(r#"["hello","world"]"#.to_string()),
            Some(r#"["say \"hi\"\\"]"#.to_string()),
            Some("[]".to_string()),
        ],
        "a list renders as a JSON array of escaped strings, never as Arrow display text"
    );

    let address: ArrayRef = Arc::new(StructArray::from(vec![
        (
            Arc::new(Field::new("street", DataType::Utf8, true)),
            utf8(&["Main St"]),
        ),
        (
            Arc::new(Field::new("city", DataType::Utf8, true)),
            utf8(&["Berlin"]),
        ),
    ]));
    let rendered_address = rendered(address);
    assert_every_cell_parses(&rendered_address);
    assert_eq!(
        rendered_address,
        vec![Some(r#"{"street":"Main St","city":"Berlin"}"#.to_string())],
        "a struct renders as a JSON object keyed by field name, in declared order"
    );

    let attributes = map_column(utf8(&["a", "b"]), utf8(&["1", "2"]), &[0, 2, 2], None);
    let rendered_attributes = rendered(attributes);
    assert_every_cell_parses(&rendered_attributes);
    assert_eq!(
        rendered_attributes,
        vec![
            Some(r#"{"a":"1","b":"2"}"#.to_string()),
            Some("{}".to_string()),
        ],
        "a map renders as one JSON object, and an empty map as an empty object"
    );

    let element = Arc::new(Field::new(
        "item",
        DataType::Struct(Fields::from(vec![Field::new("a", DataType::Int32, true)])),
        true,
    ));
    let list_of_structs: ArrayRef = Arc::new(
        ListArray::try_new(
            element,
            OffsetBuffer::new(vec![0, 2].into()),
            Arc::new(StructArray::from(vec![(
                Arc::new(Field::new("a", DataType::Int32, true)),
                Arc::new(Int32Array::from(vec![10, 20])) as ArrayRef,
            )])),
            None,
        )
        .expect("the list-of-struct column must build"),
    );
    let rendered_list_of_structs = rendered(list_of_structs);
    assert_every_cell_parses(&rendered_list_of_structs);
    assert_eq!(
        rendered_list_of_structs,
        vec![Some(r#"[{"a":10},{"a":20}]"#.to_string())],
        "nesting recurses into the element type"
    );

    let struct_of_list: ArrayRef = Arc::new(StructArray::from(vec![(
        Arc::new(Field::new(
            "inner",
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
            true,
        )),
        list_of_strings(vec![Some(vec![Some("p"), Some("q")])]),
    )]));
    let rendered_struct_of_list = rendered(struct_of_list);
    assert_every_cell_parses(&rendered_struct_of_list);
    assert_eq!(
        rendered_struct_of_list,
        vec![Some(r#"{"inner":["p","q"]}"#.to_string())],
        "nesting recurses into a struct field's own container"
    );

    let large_list: ArrayRef = Arc::new(
        LargeListArray::try_new(
            Arc::new(Field::new("item", DataType::Utf8, true)),
            OffsetBuffer::new(vec![0i64, 2i64].into()),
            utf8(&["hello", "world"]),
            None,
        )
        .expect("the large-list column must build"),
    );
    let rendered_large_list = rendered(large_list);
    assert_every_cell_parses(&rendered_large_list);
    assert_eq!(
        rendered_large_list,
        vec![Some(r#"["hello","world"]"#.to_string())],
        "a large_list renders the same JSON array shape as a list"
    );

    let fixed_size_list: ArrayRef = Arc::new(
        FixedSizeListArray::try_new(
            Arc::new(Field::new("item", DataType::Utf8, true)),
            2,
            utf8(&["p", "q"]),
            None,
        )
        .expect("the fixed-size-list column must build"),
    );
    let rendered_fixed_size_list = rendered(fixed_size_list);
    assert_every_cell_parses(&rendered_fixed_size_list);
    assert_eq!(
        rendered_fixed_size_list,
        vec![Some(r#"["p","q"]"#.to_string())],
        "a fixed_size_list renders the same JSON array shape as a list"
    );
}

/// nested-json-rendering / A null nested value emits SQL NULL, not the text "null".
///
/// A null CELL is an Exasol NULL; a null MEMBER of a populated cell is an explicit JSON
/// `null` inside the document, so one column renders the same object shape on every row.
#[test]
fn null_cells_emit_sql_null_and_null_members_render_explicitly() {
    let lists = rendered(list_of_strings(vec![
        Some(vec![Some("a"), None]),
        None,
        Some(vec![]),
    ]));
    assert_eq!(
        lists,
        vec![
            Some(r#"["a",null]"#.to_string()),
            None,
            Some("[]".to_string()),
        ],
        "a null element keeps its position while a null cell becomes SQL NULL"
    );
    assert_eq!(
        parsed(lists[0].as_ref().unwrap()).as_array().unwrap().len(),
        2,
        "a null element preserves the element count"
    );

    let address: ArrayRef = Arc::new(
        StructArray::try_new(
            Fields::from(vec![
                Field::new("street", DataType::Utf8, true),
                Field::new("city", DataType::Utf8, true),
            ]),
            vec![
                utf8(&["Second St", "unread"]),
                Arc::new(StringArray::from(vec![None, Some("unread")])) as ArrayRef,
            ],
            Some(NullBuffer::from(vec![true, false])),
        )
        .expect("the struct column must build"),
    );
    assert_eq!(
        rendered(address),
        vec![
            Some(r#"{"street":"Second St","city":null}"#.to_string()),
            None,
        ],
        "a null field renders as an explicit null, never omitted, and never as {{}}"
    );

    let attributes = map_column(
        utf8(&["k", "unread"]),
        Arc::new(StringArray::from(vec![None, Some("unread")])) as ArrayRef,
        &[0, 1, 2],
        Some(NullBuffer::from(vec![true, false])),
    );
    assert_eq!(
        rendered(attributes),
        vec![Some(r#"{"k":null}"#.to_string()), None],
        "a null map value renders as an explicit null, and a null map cell as SQL NULL"
    );
}

/// nested-json-rendering / A non-string map key is stringified into the JSON object name.
///
/// Every key type the Iceberg spec permits reaches a string object name: a nested key
/// through its own JSON rendering, every other through the Arrow-to-`Utf8` cast.
#[test]

fn non_utf8_map_keys_stringify_into_object_names() {
    let integer_keys = map_column(
        Arc::new(Int32Array::from(vec![42, 7])),
        utf8(&["a", "b"]),
        &[0, 2],
        None,
    );
    assert_eq!(
        rendered(integer_keys),
        vec![Some(r#"{"42":"a","7":"b"}"#.to_string())],
        "an integer key becomes its own decimal text, in source entry order"
    );

    let boolean_keys = map_column(
        Arc::new(BooleanArray::from(vec![true])),
        utf8(&["t"]),
        &[0, 1],
        None,
    );
    assert_eq!(
        rendered(boolean_keys),
        vec![Some(r#"{"true":"t"}"#.to_string())]
    );

    let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
    let day = chrono::NaiveDate::from_ymd_opt(2026, 8, 18).unwrap();
    let date_keys = map_column(
        Arc::new(Date32Array::from(vec![(day - epoch).num_days() as i32])),
        utf8(&["d"]),
        &[0, 1],
        None,
    );
    assert_eq!(
        rendered(date_keys),
        vec![Some(r#"{"2026-08-18":"d"}"#.to_string())]
    );

    let struct_keys = map_column(
        Arc::new(StructArray::from(vec![(
            Arc::new(Field::new("a", DataType::Int32, true)),
            Arc::new(Int32Array::from(vec![1])) as ArrayRef,
        )])),
        utf8(&["v"]),
        &[0, 1],
        None,
    );
    let rendered_struct_keys = rendered(struct_keys);
    assert_eq!(
        rendered_struct_keys,
        vec![Some(r#"{"{\"a\":1}":"v"}"#.to_string())],
        "a nested key becomes its own JSON rendering, escaped as an object name"
    );
    assert_eq!(
        parsed(rendered_struct_keys[0].as_ref().unwrap())
            .as_object()
            .unwrap()
            .keys()
            .next()
            .unwrap(),
        r#"{"a":1}"#,
        "the object name reads back as the key's own JSON document"
    );

    let nested_map: ArrayRef = Arc::new(StructArray::from(vec![(
        Arc::new(Field::new(
            "m",
            map_column(
                Arc::new(Int32Array::from(Vec::<i32>::new())),
                utf8(&[]),
                &[0],
                None,
            )
            .data_type()
            .clone(),
            true,
        )),
        map_column(
            Arc::new(Int32Array::from(vec![7])),
            utf8(&["x"]),
            &[0, 1],
            None,
        ),
    )]));
    assert_eq!(
        rendered(nested_map),
        vec![Some(r#"{"m":{"7":"x"}}"#.to_string())],
        "stringification reaches a map at any depth, not only a top-level one"
    );

    let map_valued_element = map_column(
        Arc::new(Int32Array::from(vec![1, 2])),
        utf8(&["a", "b"]),
        &[0, 1, 2],
        None,
    );
    let element_field = Arc::new(Field::new(
        "item",
        map_valued_element.data_type().clone(),
        true,
    ));
    let list_of_maps: ArrayRef = Arc::new(
        ListArray::try_new(
            element_field,
            OffsetBuffer::new(vec![0, 2].into()),
            map_valued_element,
            None,
        )
        .expect("the list-of-map column must build"),
    );
    assert_eq!(
        rendered(list_of_maps),
        vec![Some(r#"[{"1":"a"},{"2":"b"}]"#.to_string())],
        "the list arm's recursion stringifies a map key nested inside its elements"
    );

    let inner_map_value = map_column(
        Arc::new(Int32Array::from(vec![7])),
        utf8(&["x"]),
        &[0, 1],
        None,
    );
    let outer_keys: ArrayRef = Arc::new(LargeStringArray::from(vec!["outer"]));
    let large_utf8_keyed_map = map_column(outer_keys, inner_map_value, &[0, 1], None);
    assert_eq!(
        rendered(large_utf8_keyed_map),
        vec![Some(r#"{"outer":{"7":"x"}}"#.to_string())],
        "a LargeUtf8 map key passes through unchanged while its map value is still stringified"
    );
}

/// A key type no Arrow cast turns into text is refused BY NAME rather than rendered wrong
/// — the one failure mode a silent fallback would turn into a wrong object name.
#[test]
fn a_map_key_type_no_cast_reaches_utf8_is_refused_by_name() {
    let keys = FixedSizeBinaryArray::try_from_iter([[0u8, 1u8]].into_iter())
        .expect("the fixed-size-binary key child must build");
    let column = map_column(Arc::new(keys), utf8(&["v"]), &[0, 1], None);

    let error = render_nested_column_as_json(&column)
        .expect_err("a key type with no cast to text must be refused");

    let message = error.to_string();
    assert!(
        message.contains("FixedSizeBinary"),
        "the refusal must name the key type: {message}"
    );
    assert!(
        message.contains("utf8") || message.contains("Utf8"),
        "the refusal must name the constraint it could not satisfy: {message}"
    );
}

/// The renderer owns the five nested Arrow types and nothing else, so a column reaching it
/// by mistake is refused instead of being silently re-encoded as a bare JSON scalar.
#[test]
fn a_non_nested_column_is_refused_by_the_nested_renderer() {
    let column: ArrayRef = Arc::new(Int32Array::from(vec![1, 2]));

    let error = render_nested_column_as_json(&column)
        .expect_err("a primitive column must not reach the nested renderer");

    assert!(
        error.to_string().contains("Int32"),
        "the refusal must name the offending type: {error}"
    );
}

/// A map with a null key is a data defect, not a missing encoder — the refusal
/// must name the cause instead of pointing at the type system.
#[test]
fn a_map_column_with_a_null_key_is_refused_with_a_clean_error() {
    let keys = StringArray::from(vec![None, Some("b")]);
    let values = utf8(&["a", "b"]);
    let entries = StructArray::try_new(
        Fields::from(vec![
            Field::new("keys", DataType::Utf8, true),
            Field::new("values", DataType::Utf8, true),
        ]),
        vec![Arc::new(keys) as ArrayRef, values],
        None,
    )
    .expect("the entries struct must build even with a null key");
    let entries_field = Arc::new(Field::new("entries", entries.data_type().clone(), false));
    let column: ArrayRef = Arc::new(
        MapArray::try_new(
            entries_field,
            OffsetBuffer::new(vec![0, 2].into()),
            entries,
            None,
            false,
        )
        .expect("a map array with a null key child must still build"),
    );

    let error = render_nested_column_as_json(&column)
        .expect_err("a map with a null key must be refused, not silently rendered");

    let message = error.to_string();
    assert!(
        message.contains("Map"),
        "the refusal must name the column type: {message}"
    );
    assert!(
        message.to_lowercase().contains("null"),
        "the refusal must name the null-key cause: {message}"
    );
}
