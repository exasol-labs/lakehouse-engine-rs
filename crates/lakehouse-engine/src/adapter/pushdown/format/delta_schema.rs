use delta_kernel::schema::{
    ColumnMetadataKey, DataType, MetadataValue, PrimitiveType, StructField, StructType,
};
use delta_kernel::table_features::ColumnMappingMode;
use exasol_udf_sdk::error::UdfError;

use crate::scan::spec::{DeltaColumnMapping, DeltaColumnMappingMode, DeltaTableSpec, LogicalField};
use crate::types::mapping::exasol_representable_catalog_decimal;

#[cfg(test)]
#[path = "delta_schema_tests.rs"]
mod tests;

/// Builds the per-table Delta schema block from a Delta table's logical schema and metadata:
/// the ordered [`LogicalField`] list feeding `ScanSpec::logical_schema`, and the
/// [`DeltaTableSpec`] carrying the column-mapping mode, each column's logical/physical name and
/// id, and the table's partition columns.
///
/// `column_mapping_mode` is the column-mapping mode already IN FORCE — the protocol-gated mode
/// from [`DeltaSnapshot::column_mapping_mode`](super::delta_replay::DeltaSnapshot::column_mapping_mode),
/// never the raw `delta.columnMapping.mode` property. Passing the ungated property would have
/// this engine expect physical column names the table never wrote, because the Delta protocol
/// requires that property to be ignored unless the protocol supports the `columnMapping` reader
/// feature. `partition_columns` is the table's own partition-column list, threaded through
/// unchanged so a table with zero active files still carries it. Maps only the Delta
/// primitive types that already have an Arrow type tag in this engine's vocabulary; anything
/// else — including a decimal outside Exasol's representable domain — is refused with a
/// [`UdfError`] naming the column and its Delta type rather than emitting a misdescribed tag.
/// Performs no Delta reader-feature gating.
///
/// Under `id`/`name` column mapping each column's id and physical name come from its
/// `delta.columnMapping.*` annotations ALONE: a column missing either, or carrying an id no
/// `i32` holds, is refused the same way — its ordinal position and its logical name are
/// values the writer never used.
pub(super) fn build_delta_table_schema(
    schema: &StructType,
    column_mapping_mode: ColumnMappingMode,
    partition_columns: Vec<String>,
) -> Result<(Vec<LogicalField>, DeltaTableSpec), UdfError> {
    let mode = wire_column_mapping_mode(column_mapping_mode);
    let mut logical_fields = Vec::with_capacity(schema.num_fields());
    let mut columns = Vec::with_capacity(schema.num_fields());

    for (ordinal, field) in schema.fields().enumerate() {
        let arrow_type = delta_type_to_arrow_tag(field.name(), field.data_type())?;
        let id = field_id(field, mode, ordinal)?;
        logical_fields.push(LogicalField {
            field_id: id,
            name: field.name().clone(),
            arrow_type,
            nullable: field.is_nullable(),
            initial_default: None,
        });
        columns.push(DeltaColumnMapping {
            logical_name: field.name().clone(),
            physical_name: physical_name(field, mode)?,
            physical_id: id,
        });
    }

    Ok((
        logical_fields,
        DeltaTableSpec {
            column_mapping_mode: mode,
            columns,
            partition_columns,
        },
    ))
}

fn wire_column_mapping_mode(mode: ColumnMappingMode) -> DeltaColumnMappingMode {
    match mode {
        ColumnMappingMode::None => DeltaColumnMappingMode::None,
        ColumnMappingMode::Id => DeltaColumnMappingMode::Id,
        ColumnMappingMode::Name => DeltaColumnMappingMode::Name,
    }
}

/// The name `field`'s Parquet counterpart was written under.
///
/// Under [`DeltaColumnMappingMode::None`] that is always the logical name: the Delta
/// protocol resolves it that way and a residual annotation is inert. Under `Id`/`Name`
/// mode it is the `delta.columnMapping.physicalName` annotation the protocol REQUIRES —
/// absent or non-string is refused rather than substituted, because nothing on the read
/// path validates the annotation and the logical name is a column the writer never wrote.
fn physical_name(field: &StructField, mode: DeltaColumnMappingMode) -> Result<String, UdfError> {
    if mode == DeltaColumnMappingMode::None {
        return Ok(field.name().clone());
    }
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

/// The field-id carried for `field`.
///
/// Under [`DeltaColumnMappingMode::None`] always its 1-based ordinal position, so
/// ordinals never share a namespace with assigned ids — a residual annotation is inert
/// there, and honouring one would let it collide with an unannotated sibling's ordinal.
/// Under `Id`/`Name` mode always the `delta.columnMapping.id` annotation, refused when
/// absent or wider than the `i32` the wire carries.
fn field_id(
    field: &StructField,
    mode: DeltaColumnMappingMode,
    ordinal: usize,
) -> Result<i32, UdfError> {
    if mode == DeltaColumnMappingMode::None {
        return Ok((ordinal as i32) + 1);
    }
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
    mode: DeltaColumnMappingMode,
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
