use arrow::array::{BinaryArray, BinaryViewArray, LargeBinaryArray};

use super::*;

fn text_of(array: ArrayRef) -> Vec<Option<String>> {
    let text = bytes_as_text(&array, "C").expect("valid UTF-8 reads as text");
    let text = arrow::compute::cast(&text, &DataType::Utf8).expect("text casts to Utf8");
    text.as_string::<i32>()
        .iter()
        .map(|value| value.map(str::to_string))
        .collect()
}

#[test]
fn every_variable_length_binary_encoding_reads_as_its_utf8_text() {
    let values = [Some("héllo".as_bytes()), Some(&[][..]), None];
    let expected = vec![Some("héllo".to_string()), Some(String::new()), None];

    assert_eq!(
        text_of(Arc::new(BinaryArray::from(values.to_vec()))),
        expected
    );
    assert_eq!(
        text_of(Arc::new(LargeBinaryArray::from(values.to_vec()))),
        expected
    );
    assert_eq!(
        text_of(Arc::new(BinaryViewArray::from(values.to_vec()))),
        expected
    );
}

/// Scenario: BINARY_AS_STRING reads a variable-length binary column as UTF-8 text
#[test]
fn a_value_that_is_not_valid_utf8_fails_naming_the_column() {
    let invalid = vec![Some("ok".as_bytes()), Some(&[0xff, 0xfe][..])];
    for array in [
        Arc::new(BinaryArray::from(invalid.clone())) as ArrayRef,
        Arc::new(BinaryViewArray::from(invalid.clone())) as ArrayRef,
    ] {
        let message = bytes_as_text(&array, "PAYLOAD")
            .expect_err("invalid UTF-8 must never be returned altered or as NULL")
            .to_string();
        assert!(
            message.contains("column 'PAYLOAD'") && message.contains("not valid UTF-8"),
            "{message}"
        );
    }
}
