//! The one JSON encoder for nested Arrow columns.
//!
//! Exasol has no array, list, struct, or map type, so every such column is surfaced as the
//! `VARCHAR(2000000)` the schema declares for it, carrying one JSON document per value. This
//! module is that rendering, and it is reached from BOTH scan paths — as the expression the
//! column-binding adapter substitutes for a nested physical column, and as the function the
//! generated SQL invokes on the legacy inferred-schema path — so a given value renders
//! identically whichever path produced it.
//!
//! `arrow-cast` is deliberately not involved: it offers no `Struct → Utf8` or `Map → Utf8`
//! cast at all, and its `List → Utf8` cast produces Arrow display text (`[hello, world]`,
//! unquoted and ambiguous for a null element) rather than JSON.

use crate::types::mapping::needs_nested_json_rendering;
use arrow::array::cast::AsArray;
use arrow::array::{
    Array, ArrayRef, FixedSizeListArray, LargeListArray, ListArray, MapArray, StringArray,
    StringBuilder, StructArray,
};
use arrow::compute::kernels::cast::{can_cast_types, cast};
use arrow::datatypes::{DataType, Field, FieldRef, Fields};
use arrow::json::writer::{EncoderOptions, make_encoder};
use datafusion::error::{DataFusionError, Result};
use std::sync::Arc;

/// Render one nested Arrow column as a `Utf8` column of JSON documents.
///
/// A null cell renders as a NULL in the returned column — never as the text `null`, `{}`, or
/// `[]` — because the encoder's own contract leaves a null index unspecified and renders an
/// empty container for one. A null MEMBER of a populated cell renders as an explicit JSON
/// `null`, so every row of one column carries the same object shape and an Exasol
/// `JSON_VALUE` path never disappears between rows.
///
/// Only the five nested types [`needs_nested_json_rendering`] owns are accepted. That
/// predicate is the single authority on which types this encoder serves, so no caller can
/// classify a column into the JSON-rendered half while this module classifies it out.
pub(crate) fn render_nested_column_as_json(array: &ArrayRef) -> Result<StringArray> {
    if !needs_nested_json_rendering(array.data_type()) {
        return Err(DataFusionError::Execution(format!(
            "JSON rendering was attempted for a column of type {}, which is not one of the \
             nested Arrow types this encoder owns (list, large_list, fixed_size_list, struct, map)",
            array.data_type()
        )));
    }
    encode_documents(&with_stringified_map_keys(array)?)
}

fn encode_documents(array: &ArrayRef) -> Result<StringArray> {
    let field: FieldRef = Arc::new(Field::new("value", array.data_type().clone(), true));
    let options = EncoderOptions::default().with_explicit_nulls(true);
    let mut encoder = make_encoder(&field, array.as_ref(), &options).map_err(|e| {
        DataFusionError::Execution(format!(
            "a column of type {} could not be prepared for JSON rendering: {e}",
            array.data_type()
        ))
    })?;

    let mut documents = StringBuilder::new();
    let mut buffer: Vec<u8> = Vec::new();
    for idx in 0..array.len() {
        if array.is_null(idx) {
            documents.append_null();
            continue;
        }
        buffer.clear();
        encoder.encode(idx, &mut buffer);
        let document = std::str::from_utf8(&buffer).map_err(|e| {
            DataFusionError::Execution(format!(
                "the JSON rendering of a {} value is not valid UTF-8: {e}",
                array.data_type()
            ))
        })?;
        documents.append_value(document);
    }
    Ok(documents.finish())
}

/// Rebuild `array` with every map key child replaced by a `Utf8` array of stringified keys,
/// at any depth, leaving the array untouched when no such key exists.
///
/// A JSON object name is a string (RFC 8259) while the Iceberg spec permits ANY type as a
/// map key, and the encoder refuses a non-`Utf8` key outright — so the replacement happens
/// here, before encoding, rather than as a fallback inside the encoder.
fn with_stringified_map_keys(array: &ArrayRef) -> Result<ArrayRef> {
    if !map_keys_need_stringifying(array.data_type()) {
        return Ok(Arc::clone(array));
    }
    match array.data_type() {
        DataType::List(element) => {
            let list = array.as_list::<i32>();
            let values = with_stringified_map_keys(list.values())?;
            let element = retyped(element, values.data_type());
            Ok(Arc::new(ListArray::try_new(
                element,
                list.offsets().clone(),
                values,
                list.nulls().cloned(),
            )?))
        }
        DataType::LargeList(element) => {
            let list = array.as_list::<i64>();
            let values = with_stringified_map_keys(list.values())?;
            let element = retyped(element, values.data_type());
            Ok(Arc::new(LargeListArray::try_new(
                element,
                list.offsets().clone(),
                values,
                list.nulls().cloned(),
            )?))
        }
        DataType::FixedSizeList(element, size) => {
            let list = array.as_fixed_size_list();
            let values = with_stringified_map_keys(list.values())?;
            let element = retyped(element, values.data_type());
            Ok(Arc::new(FixedSizeListArray::try_new(
                element,
                *size,
                values,
                list.nulls().cloned(),
            )?))
        }
        DataType::Struct(_) => {
            let structure = array.as_struct();
            let mut fields = Vec::with_capacity(structure.num_columns());
            let mut children = Vec::with_capacity(structure.num_columns());
            for (field, child) in structure.fields().iter().zip(structure.columns()) {
                let child = with_stringified_map_keys(child)?;
                fields.push(retyped(field, child.data_type()));
                children.push(child);
            }
            Ok(Arc::new(StructArray::try_new(
                Fields::from(fields),
                children,
                structure.nulls().cloned(),
            )?))
        }
        DataType::Map(entries, ordered) => {
            let map = array.as_map();
            let keys = stringified_keys(map.keys())?;
            let values = with_stringified_map_keys(map.values())?;
            let declared = map.entries().fields();
            let rebuilt = StructArray::try_new(
                Fields::from(vec![
                    retyped(&declared[0], keys.data_type()),
                    retyped(&declared[1], values.data_type()),
                ]),
                vec![keys, values],
                map.entries().nulls().cloned(),
            )?;
            let entries = retyped(entries, rebuilt.data_type());
            Ok(Arc::new(MapArray::try_new(
                entries,
                map.offsets().clone(),
                rebuilt,
                map.nulls().cloned(),
                *ordered,
            )?))
        }
        _ => Ok(Arc::clone(array)),
    }
}

fn stringified_keys(keys: &ArrayRef) -> Result<ArrayRef> {
    match keys.data_type() {
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => Ok(Arc::clone(keys)),
        nested if needs_nested_json_rendering(nested) => {
            Ok(Arc::new(render_nested_column_as_json(keys)?))
        }
        castable if can_cast_types(castable, &DataType::Utf8) => {
            cast(keys.as_ref(), &DataType::Utf8).map_err(|e| {
                DataFusionError::Execution(format!(
                    "a map key of type {castable} could not be stringified into a JSON object \
                     name: {e}"
                ))
            })
        }
        refused => Err(DataFusionError::Execution(format!(
            "a map key of type {refused} cannot be rendered as a JSON object name: an object \
             name must be a string and Arrow offers no cast from {refused} to utf8"
        ))),
    }
}

fn map_keys_need_stringifying(data_type: &DataType) -> bool {
    match data_type {
        DataType::Map(entries, _) => match entries.data_type() {
            DataType::Struct(entry) if entry.len() == 2 => {
                !matches!(
                    entry[0].data_type(),
                    DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
                ) || map_keys_need_stringifying(entry[1].data_type())
            }
            _ => false,
        },
        DataType::List(element)
        | DataType::LargeList(element)
        | DataType::FixedSizeList(element, _) => map_keys_need_stringifying(element.data_type()),
        DataType::Struct(fields) => fields
            .iter()
            .any(|field| map_keys_need_stringifying(field.data_type())),
        _ => false,
    }
}

fn retyped(field: &FieldRef, data_type: &DataType) -> FieldRef {
    Arc::new(field.as_ref().clone().with_data_type(data_type.clone()))
}

#[cfg(test)]
#[path = "json_render_tests.rs"]
mod tests;
