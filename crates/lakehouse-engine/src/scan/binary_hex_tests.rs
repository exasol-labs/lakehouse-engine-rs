use arrow::array::{
    BinaryArray, BinaryViewArray, FixedSizeBinaryArray, LargeBinaryArray, StringArray,
};

use super::*;

fn rendered(array: ArrayRef) -> Vec<Option<String>> {
    let rendered = render_hex(&array, "B").expect("a binary array renders");
    rendered
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("hex renders as Utf8")
        .iter()
        .map(|value| value.map(str::to_string))
        .collect()
}

#[test]
fn every_binary_encoding_renders_lowercase_hex_and_an_empty_value_as_null() {
    let values = [Some(&[0x00, 0xff, 0x0a][..]), Some(&[][..]), None];
    let expected = vec![Some("00ff0a".to_string()), None, None];

    assert_eq!(
        rendered(Arc::new(BinaryArray::from(values.to_vec()))),
        expected
    );
    assert_eq!(
        rendered(Arc::new(LargeBinaryArray::from(values.to_vec()))),
        expected
    );
    assert_eq!(
        rendered(Arc::new(BinaryViewArray::from(values.to_vec()))),
        expected
    );
    let fixed = FixedSizeBinaryArray::try_from_sparse_iter_with_size(
        [Some([0xde, 0xad]), None].into_iter(),
        2,
    )
    .unwrap();
    assert_eq!(
        rendered(Arc::new(fixed)),
        vec![Some("dead".to_string()), None]
    );
}

#[test]
fn a_value_whose_hex_exceeds_varchar_2000000_fails_naming_the_column_and_its_size() {
    let oversized = vec![0u8; MAX_BINARY_BYTES + 1];
    let at_limit = vec![0u8; MAX_BINARY_BYTES];

    let error = render_hex(
        &(Arc::new(BinaryArray::from(vec![Some(oversized.as_slice())])) as ArrayRef),
        "PAYLOAD",
    )
    .expect_err("an oversized value must fail");
    assert!(
        error.to_string().contains("column 'PAYLOAD'")
            && error.to_string().contains("1000001 bytes"),
        "{error}"
    );
    render_hex(
        &(Arc::new(BinaryArray::from(vec![Some(at_limit.as_slice())])) as ArrayRef),
        "PAYLOAD",
    )
    .expect("a value of exactly the limit renders");
}
