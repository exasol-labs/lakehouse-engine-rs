use delta_kernel::schema::{
    ColumnMetadataKey, DataType, MetadataValue, PrimitiveType, StructField, StructType,
};
use delta_kernel::table_features::ColumnMappingMode;
use exasol_udf_sdk::error::UdfError;

use crate::scan::spec::LogicalField;
use crate::types::mapping::exasol_representable_catalog_decimal;

#[cfg(test)]
#[path = "delta_schema_tests.rs"]
mod tests;

/// Resolves a Delta table's logical schema and metadata into the two format-neutral values the
/// scan spec carries for it: the ordered [`LogicalField`] list feeding `ScanSpec::logical_schema`,
/// each field carrying the ONE binding key its column-mapping mode selects, and the table's ordered
/// partition-column names feeding `CommonScanSpec::partition_columns`.
///
/// `column_mapping_mode` is the column-mapping mode already IN FORCE — the protocol-gated mode
/// from [`DeltaSnapshot::column_mapping_mode`](super::delta_replay::DeltaSnapshot::column_mapping_mode),
/// never the raw `delta.columnMapping.mode` property. Passing the ungated property would have
/// this engine expect physical column names the table never wrote, because the Delta protocol
/// requires that property to be ignored unless the protocol supports the `columnMapping` reader
/// feature. The mode itself is carried NO further than this call: its only consumer is the
/// per-field binding-key choice made here, so encoding that choice on each field leaves no second
/// home free to disagree with it.
///
/// `partition_columns` is the table's own partition-column list, threaded through unchanged so a
/// table with zero active files still carries it. Maps only the Delta primitive types that already
/// have an Arrow type tag in this engine's vocabulary; anything else — including a decimal outside
/// Exasol's representable domain — is refused with a [`UdfError`] naming the column and its Delta
/// type rather than emitting a misdescribed tag. Performs no Delta reader-feature gating.
///
/// Under `id`/`name` column mapping a column's binding key comes from its `delta.columnMapping.*`
/// annotations ALONE: a column missing either annotation, or carrying an id no `i32` holds, is
/// refused — its ordinal position and its logical name are values the writer never used.
pub(super) fn build_delta_table_schema(
    schema: &StructType,
    column_mapping_mode: ColumnMappingMode,
    partition_columns: Vec<String>,
) -> Result<(Vec<LogicalField>, Vec<String>), UdfError> {
    let mut logical_fields = Vec::with_capacity(schema.num_fields());

    for field in schema.fields() {
        let arrow_type = delta_type_to_arrow_tag(field.name(), field.data_type())?;
        let (field_id, physical_name) = binding_key(field, column_mapping_mode)?;
        logical_fields.push(LogicalField {
            field_id,
            name: field.name().clone(),
            arrow_type,
            nullable: field.is_nullable(),
            initial_default: None,
            physical_name,
        });
    }

    Ok((logical_fields, partition_columns))
}

/// The ONE binding key `field`'s logical field carries, as the `(field_id, physical_name)` pair
/// [`LogicalField`] holds. At most one member is ever populated: two keys would need a precedence
/// rule the Delta protocol does not define, and the second would never be consulted.
///
/// `Id` mode selects the `delta.columnMapping.id` annotation — the only mode in which Delta writes
/// Parquet field-ids. `Name` mode selects the `delta.columnMapping.physicalName` annotation, which
/// the protocol REQUIRES a `name`-mode reader to match on. `None` mode selects NEITHER, leaving the
/// column to bind by its own logical name: an ordinal position is a value no writer ever wrote into
/// any file, so carrying one invites a false field-id match against a file that does carry ids.
///
/// The dispatch is exhaustive rather than defaulted, so a column-mapping mode added to the Delta
/// protocol is a compile error here rather than a column silently bound by the wrong key.
fn binding_key(
    field: &StructField,
    mode: ColumnMappingMode,
) -> Result<(Option<i32>, Option<String>), UdfError> {
    match mode {
        ColumnMappingMode::None => Ok((None, None)),
        ColumnMappingMode::Id => {
            let (id, _physical_name) = mapped_column_annotations(field, mode)?;
            Ok((Some(id), None))
        }
        ColumnMappingMode::Name => {
            let (_id, physical_name) = mapped_column_annotations(field, mode)?;
            Ok((None, Some(physical_name)))
        }
    }
}

/// BOTH `delta.columnMapping.*` annotations a column must carry under `id`/`name` mode.
///
/// Both are read in EITHER mapped mode even though only one becomes the column's binding key,
/// because the Delta protocol requires both in either mode and nothing on the read path validates
/// either — so a column declaring only the one its current mode happens to select is refused here
/// rather than reaching the scan as a half-annotated column.
fn mapped_column_annotations(
    field: &StructField,
    mode: ColumnMappingMode,
) -> Result<(i32, String), UdfError> {
    Ok((
        column_mapping_id(field, mode)?,
        column_mapping_physical_name(field, mode)?,
    ))
}

/// The `delta.columnMapping.physicalName` annotation `field` carries — the name its Parquet
/// counterpart was written under.
///
/// Absent, or present but non-string, is refused rather than substituted, because nothing on the
/// read path validates the annotation and the logical name is a column the writer never wrote.
fn column_mapping_physical_name(
    field: &StructField,
    mode: ColumnMappingMode,
) -> Result<String, UdfError> {
    let key = ColumnMetadataKey::ColumnMappingPhysicalName.as_ref();
    match field.get_config_value(&ColumnMetadataKey::ColumnMappingPhysicalName) {
        Some(MetadataValue::String(name)) => Ok(name.clone()),
        Some(other) => Err(unusable_column_mapping(
            field,
            mode,
            format!("{key} is '{other}', which is not a string"),
        )),
        None => Err(unusable_column_mapping(
            field,
            mode,
            format!("{key} is absent"),
        )),
    }
}

/// The `delta.columnMapping.id` annotation `field` carries, refused when absent or wider than the
/// `i32` the wire carries.
///
/// Never substituted by the field's ordinal position: an ordinal can collide with a sibling
/// column's assigned id, and no writer ever wrote it into a file.
fn column_mapping_id(field: &StructField, mode: ColumnMappingMode) -> Result<i32, UdfError> {
    let key = ColumnMetadataKey::ColumnMappingId.as_ref();
    let id = field.column_mapping_id().ok_or_else(|| {
        unusable_column_mapping(field, mode, format!("{key} is absent or not a number"))
    })?;
    i32::try_from(id).map_err(|_| {
        unusable_column_mapping(
            field,
            mode,
            format!("{key} is {id}, which does not fit the 32-bit field-id the scan binds by"),
        )
    })
}

fn unusable_column_mapping(
    field: &StructField,
    mode: ColumnMappingMode,
    problem: String,
) -> UdfError {
    UdfError::User(format!(
        "Delta column '{}' carries no usable column-mapping annotation under {mode:?}-mode \
         column mapping: {problem}. The Delta protocol requires every field to carry both {} \
         and {} in that mode, and the read path validates neither, so substituting an ordinal \
         position or the logical name would bind the scan to a column the writer never wrote",
        field.name(),
        ColumnMetadataKey::ColumnMappingId.as_ref(),
        ColumnMetadataKey::ColumnMappingPhysicalName.as_ref(),
    ))
}

fn delta_type_to_arrow_tag(column_name: &str, data_type: &DataType) -> Result<String, UdfError> {
    let DataType::Primitive(primitive) = data_type else {
        return Err(unmapped_delta_type_error(column_name, data_type));
    };
    use PrimitiveType::*;
    let tag = match primitive {
        Boolean => "bool".to_string(),
        Integer => "int32".to_string(),
        Long => "int64".to_string(),
        Float => "float32".to_string(),
        Double => "float64".to_string(),
        String => "utf8".to_string(),
        Date => "date32".to_string(),
        Timestamp => "timestamptz_us".to_string(),
        TimestampNtz => "timestamp_us".to_string(),
        Decimal(decimal) => {
            let (precision, scale) = (u32::from(decimal.precision()), u32::from(decimal.scale()));
            if exasol_representable_catalog_decimal(precision, scale) {
                format!("decimal128({precision},{scale})")
            } else {
                return Err(unmapped_delta_type_error(column_name, data_type));
            }
        }
        _ => return Err(unmapped_delta_type_error(column_name, data_type)),
    };
    Ok(tag)
}

fn unmapped_delta_type_error(column_name: &str, data_type: &DataType) -> UdfError {
    UdfError::User(format!(
        "Delta column '{column_name}' has type '{data_type}', which this engine does not map \
         at plan time; broad Delta type mapping, including the incompatible-type \
         VARCHAR(2000000)-via-JSON convention, is tracked as issue #322"
    ))
}
