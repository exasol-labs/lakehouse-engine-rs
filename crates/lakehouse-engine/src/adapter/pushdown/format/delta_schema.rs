use delta_kernel::schema::{
    ColumnMetadataKey, DataType, MetadataValue, PrimitiveType, StructField, StructType,
};
use delta_kernel::table_features::ColumnMappingMode;
use exasol_udf_sdk::error::UdfError;

use crate::scan::spec::LogicalField;
use crate::types::mapping::exasol_representable_catalog_decimal;

use super::RefusedColumn;

#[cfg(test)]
#[path = "delta_schema_tests.rs"]
mod tests;

/// The three values `build_delta_table_schema` answers: the ordered [`LogicalField`] list, the
/// table's ordered partition-column names, and the columns it declined to map.
type DeltaTableSchema = (Vec<LogicalField>, Vec<String>, Vec<RefusedColumn>);

/// Resolves a Delta table's logical schema and metadata into the three format-neutral values the
/// scan spec carries for it: the ordered [`LogicalField`] list feeding `ScanSpec::logical_schema`,
/// each field carrying the ONE binding key its column-mapping mode selects; the table's ordered
/// partition-column names feeding `CommonScanSpec::partition_columns`; and the columns this call
/// declined to map, each named with the reason.
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
/// table with zero active files still carries it. Maps each column's Delta type onto its own Arrow
/// tag, onto the `utf8` tag when Exasol cannot represent the type natively — an out-of-domain
/// decimal, `void`, either interval type, or an `array` whose element type is itself mappable at
/// every nesting depth — or REFUSES the column: no [`LogicalField`] is emitted for it, and it is
/// recorded in the returned refused list, naming the reason its type cannot be rendered faithfully.
/// A refused column never fails this call by itself — refusing the whole table when NO column is
/// mappable is the caller's decision, made once every column has been classified. A [`UdfError`]
/// surfaces from this call only when a MAPPABLE column carries a malformed column-mapping
/// annotation, below. Performs no Delta reader-feature gating.
///
/// A column's TYPE is classified BEFORE its `delta.columnMapping.*` binding key is ever read: a
/// refused column's binding key is never looked up, so a column is refused for its type and never
/// for an annotation on a column this engine will not read.
///
/// Under `id`/`name` column mapping a MAPPABLE column's binding key comes from its
/// `delta.columnMapping.*` annotations ALONE: a column missing either annotation, or carrying an id
/// no `i32` holds, is refused — its ordinal position and its logical name are values the writer
/// never used.
pub(super) fn build_delta_table_schema(
    schema: &StructType,
    column_mapping_mode: ColumnMappingMode,
    partition_columns: Vec<String>,
) -> Result<DeltaTableSchema, UdfError> {
    let mut logical_fields = Vec::with_capacity(schema.num_fields());
    let mut refused_columns = Vec::new();

    for field in schema.fields() {
        let arrow_type = match classify_delta_type(field.name(), field.data_type()) {
            ClassifiedDeltaColumn::Tag(arrow_type) => arrow_type,
            ClassifiedDeltaColumn::Refused(reason) => {
                refused_columns.push(RefusedColumn {
                    column_name: field.name().clone(),
                    reason,
                });
                continue;
            }
        };
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

    Ok((logical_fields, partition_columns, refused_columns))
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

/// What classifying ONE Delta column's type answers: the Arrow tag the scan binds the column
/// by, or the reason this engine will not render it.
///
/// Refusing a column is an expected outcome of reading a Delta schema — never a failure of it —
/// so it is answered as a value rather than signalled as an error and converted back to data one
/// line later. That leaves a [`UdfError`] out of [`build_delta_table_schema`] meaning exactly one
/// thing: a MAPPABLE column carries a malformed column-mapping annotation.
enum ClassifiedDeltaColumn {
    Tag(String),
    Refused(String),
}

/// Classifies `data_type` for the column named `column_name`.
///
/// An `array` classifies as its INNERMOST element type does — mirroring `can_cast_types`' own
/// `(List(inner), Utf8) => can_cast_types(inner, Utf8)` rule, where only whether the element
/// classifies at all decides and its tag is discarded. Its refusal is composed ONCE against the
/// column's own declared type, so nesting adds no message layer and no operator is told the
/// column has the element's type.
fn classify_delta_type(column_name: &str, data_type: &DataType) -> ClassifiedDeltaColumn {
    use ClassifiedDeltaColumn::{Refused, Tag};
    use PrimitiveType::*;
    match data_type {
        DataType::Primitive(primitive) => match primitive {
            Boolean => Tag("bool".to_string()),
            Byte | Short | Integer => Tag("int32".to_string()),
            Long => Tag("int64".to_string()),
            Float => Tag("float32".to_string()),
            Double => Tag("float64".to_string()),
            String => Tag("utf8".to_string()),
            Date => Tag("date32".to_string()),
            Timestamp => Tag("timestamptz_us".to_string()),
            TimestampNtz => Tag("timestamp_us".to_string()),
            Void | IntervalYearMonth | IntervalDayTime => Tag("utf8".to_string()),
            Decimal(decimal) => {
                let (precision, scale) =
                    (u32::from(decimal.precision()), u32::from(decimal.scale()));
                Tag(if exasol_representable_catalog_decimal(precision, scale) {
                    format!("decimal128({precision},{scale})")
                } else {
                    "utf8".to_string()
                })
            }
            Binary => Refused(binary_refusal(column_name)),
        },
        DataType::Struct(_) => Refused(struct_refusal(column_name)),
        DataType::Map(_) => Refused(map_refusal(column_name)),
        DataType::Variant(_) => Refused(variant_refusal(column_name)),
        DataType::Array(array) => {
            let mut element = array.element_type();
            while let DataType::Array(nested) = element {
                element = nested.element_type();
            }
            match classify_delta_type(column_name, element) {
                Tag(_) => Tag("utf8".to_string()),
                Refused(element_reason) => Refused(array_element_refusal(
                    column_name,
                    data_type,
                    &element_reason,
                )),
            }
        }
    }
}

fn array_element_refusal(column_name: &str, data_type: &DataType, element_reason: &str) -> String {
    format!(
        "Delta column '{column_name}' has type '{data_type}', whose element type is refused: \
         {element_reason}"
    )
}

fn binary_refusal(column_name: &str) -> String {
    format!(
        "Delta column '{column_name}' has type 'binary', which this engine refuses rather than \
         casting to text: the cast replaces every byte sequence that is not valid UTF-8 with NULL, \
         silently corrupting the value; real JSON rendering is tracked as issue #350"
    )
}

fn struct_refusal(column_name: &str) -> String {
    format!(
        "Delta column '{column_name}' has type 'struct', which arrow-cast reports no cast to text \
         for; real JSON rendering is tracked as issue #350"
    )
}

fn map_refusal(column_name: &str) -> String {
    format!(
        "Delta column '{column_name}' has type 'map', which arrow-cast reports no cast to text for; \
         real JSON rendering is tracked as issue #350"
    )
}

fn variant_refusal(column_name: &str) -> String {
    format!(
        "Delta column '{column_name}' has type 'variant', whose on-disk form is an opaque \
         (metadata, value) binary pair this engine cannot render as a meaningful value"
    )
}
