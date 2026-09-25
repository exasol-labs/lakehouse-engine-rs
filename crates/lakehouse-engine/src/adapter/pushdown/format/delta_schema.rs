use delta_kernel::schema::{
    ColumnMetadataKey, DataType, DecimalType, MetadataValue, PrimitiveType, StructField, StructType,
};
use delta_kernel::table_features::ColumnMappingMode;
use exasol_udf_sdk::error::UdfError;

use crate::scan::spec::{LogicalField, NestedField, NestedMembers};
use crate::types::mapping::exasol_representable_catalog_decimal;

use super::RefusedColumn;

#[cfg(test)]
#[path = "delta_schema_tests.rs"]
mod tests;

type DeltaTableSchema = (Vec<LogicalField>, Vec<String>, Vec<RefusedColumn>);

/// Maps a Delta schema to the scan spec's logical fields (each with the one binding key its
/// column-mapping mode selects), the partition columns, and the refused columns.
///
/// `column_mapping_mode` must be the protocol-gated mode in force, never the raw
/// `delta.columnMapping.mode` property, which the protocol says to ignore without the
/// `columnMapping` reader feature. The mode goes no further than this call.
///
/// Types Exasol cannot represent natively (out-of-domain decimal, `void`, intervals, and
/// containers whose members all map) are tagged `utf8`; others are refused. A refused column
/// never fails the call; refusing a table with no mappable column is the caller's decision.
/// Errors arise only from a malformed column-mapping annotation on a mappable column or a
/// malformed `delta.typeChanges` annotation. Performs no reader-feature gating.
///
/// Per field, checks run type, then recorded type changes, then binding key, so a column
/// refused for its type or change is never failed for an annotation this engine won't read.
/// Per `PROTOCOL.md` § Reader Requirements for Type Widening, readers must *"validate that
/// they support all type changes … and fail when finding any unsupported type change"*; an
/// unsupported change refuses only its own column.
///
/// Under `id`/`name` mapping the binding key comes from the annotations alone: ordinal
/// position and logical name are values the writer never used.
pub(super) fn build_delta_table_schema(
    schema: &StructType,
    column_mapping_mode: ColumnMappingMode,
    partition_columns: Vec<String>,
) -> Result<DeltaTableSchema, UdfError> {
    let mut logical_fields = Vec::with_capacity(schema.num_fields());
    let mut refused_columns = Vec::new();

    for field in schema.fields() {
        match walk_field(field, &FieldPath::column(field.name()), column_mapping_mode)? {
            Walked::Refused(refusal) => refused_columns.push(RefusedColumn {
                column_name: field.name().clone(),
                reason: refusal.stated_for(field),
            }),
            Walked::Mapped(MappedField {
                arrow_type,
                descriptor,
            }) => logical_fields.push(LogicalField {
                field_id: descriptor.field_id,
                name: descriptor.name,
                arrow_type,
                nullable: field.is_nullable(),
                initial_default: None,
                nested: descriptor.nested,
                physical_name: descriptor.physical_name,
            }),
        }
    }

    Ok((logical_fields, partition_columns, refused_columns))
}

fn unsupported_type_change(
    field: &StructField,
    path: &FieldPath,
) -> Result<Option<RecordedTypeChange>, UdfError> {
    Ok(recorded_type_changes(field, path)?
        .into_iter()
        .find(|change| !is_supported_type_change(change)))
}

/// At most one member is populated: two keys would need a precedence rule the protocol does
/// not define. `None` mode binds by logical name; an ordinal would invite a false field-id
/// match against a file that does carry ids. Exhaustive so a new mode is a compile error.
fn binding_key(
    field: &StructField,
    path: &FieldPath,
    mode: ColumnMappingMode,
) -> Result<(Option<i32>, Option<String>), UdfError> {
    match mode {
        ColumnMappingMode::None => Ok((None, None)),
        ColumnMappingMode::Id => {
            let (id, _physical_name) = mapped_column_annotations(field, path, mode)?;
            Ok((Some(id), None))
        }
        ColumnMappingMode::Name => {
            let (_id, physical_name) = mapped_column_annotations(field, path, mode)?;
            Ok((None, Some(physical_name)))
        }
    }
}

/// Both are required in either mapped mode by the protocol and nothing on the read path
/// validates them, so a half-annotated column is refused here.
fn mapped_column_annotations(
    field: &StructField,
    path: &FieldPath,
    mode: ColumnMappingMode,
) -> Result<(i32, String), UdfError> {
    Ok((
        column_mapping_id(field, path, mode)?,
        column_mapping_physical_name(field, path, mode)?,
    ))
}

fn column_mapping_physical_name(
    field: &StructField,
    path: &FieldPath,
    mode: ColumnMappingMode,
) -> Result<String, UdfError> {
    let key = ColumnMetadataKey::ColumnMappingPhysicalName.as_ref();
    match field.get_config_value(&ColumnMetadataKey::ColumnMappingPhysicalName) {
        Some(MetadataValue::String(name)) => Ok(name.clone()),
        Some(other) => Err(unusable_column_mapping(
            path,
            mode,
            format!("{key} is '{other}', which is not a string"),
        )),
        None => Err(unusable_column_mapping(
            path,
            mode,
            format!("{key} is absent"),
        )),
    }
}

/// Never substituted by the ordinal position, which can collide with a sibling's assigned id.
fn column_mapping_id(
    field: &StructField,
    path: &FieldPath,
    mode: ColumnMappingMode,
) -> Result<i32, UdfError> {
    let key = ColumnMetadataKey::ColumnMappingId.as_ref();
    let id = field.column_mapping_id().ok_or_else(|| {
        unusable_column_mapping(path, mode, format!("{key} is absent or not a number"))
    })?;
    i32::try_from(id).map_err(|_| {
        unusable_column_mapping(
            path,
            mode,
            format!("{key} is {id}, which does not fit the 32-bit field-id the scan binds by"),
        )
    })
}

fn unusable_column_mapping(path: &FieldPath, mode: ColumnMappingMode, problem: String) -> UdfError {
    UdfError::User(format!(
        "Delta column '{}' carries no usable column-mapping annotation under {mode:?}-mode \
         column mapping: {problem}. The Delta protocol requires every field to carry both {} \
         and {} in that mode, and the read path validates neither, so substituting an ordinal \
         position or the logical name would bind the scan to a column the writer never wrote",
        path.rendered(),
        ColumnMetadataKey::ColumnMappingId.as_ref(),
        ColumnMetadataKey::ColumnMappingPhysicalName.as_ref(),
    ))
}

/// A refusal is an expected outcome, not an error, so a [`UdfError`] from
/// [`build_delta_table_schema`] means only a malformed annotation.
enum Walked<T> {
    Mapped(T),
    Refused(Refusal),
}

/// `member_path` is `None` when the cause is the column's own type or type change.
struct Refusal {
    member_path: Option<String>,
    cause: String,
}

impl Refusal {
    fn stated_for(&self, column: &StructField) -> String {
        match &self.member_path {
            Some(member_path) => refused_container_member(column, member_path, &self.cause),
            None => refused_column(column, &self.cause),
        }
    }
}

struct MappedType {
    arrow_type: String,
    members: Option<NestedMembers>,
}

struct MappedField {
    arrow_type: String,
    descriptor: NestedField,
}

/// Segments use the protocol's own `fieldPath` vocabulary: `element`, `key`, `value` for
/// positional container members.
#[derive(Clone)]
struct FieldPath(Vec<String>);

impl FieldPath {
    fn column(column_name: &str) -> Self {
        Self(vec![column_name.to_string()])
    }

    fn child(&self, segment: &str) -> Self {
        let mut segments = self.0.clone();
        segments.push(segment.to_string());
        Self(segments)
    }

    fn rendered(&self) -> String {
        self.0.join(".")
    }

    fn member_path(&self) -> Option<String> {
        (self.0.len() > 1).then(|| self.rendered())
    }
}

const ELEMENT_SEGMENT: &str = "element";

const KEY_SEGMENT: &str = "key";

const VALUE_SEGMENT: &str = "value";

/// Check order sets precedence: type, then recorded type changes, then binding key.
fn walk_field(
    field: &StructField,
    path: &FieldPath,
    mode: ColumnMappingMode,
) -> Result<Walked<MappedField>, UdfError> {
    let mapped = match walk_type(field.data_type(), path, mode)? {
        Walked::Refused(refusal) => return Ok(Walked::Refused(refusal)),
        Walked::Mapped(mapped) => mapped,
    };

    if let Some(change) = unsupported_type_change(field, path)? {
        return Ok(Walked::Refused(Refusal {
            member_path: change.applied_to(path).member_path(),
            cause: type_change_cause(&change.from_type, &change.to_type),
        }));
    }

    let (field_id, physical_name) = binding_key(field, path, mode)?;
    Ok(Walked::Mapped(MappedField {
        arrow_type: mapped.arrow_type,
        descriptor: NestedField {
            field_id,
            name: field.name().clone(),
            physical_name,
            nested: mapped.members,
        },
    }))
}

/// A container maps exactly when every member maps, and is tagged `utf8` because the JSON
/// renderer recurses natively; casting the Arrow form to text is not an option (`struct` and
/// `map` can't be cast, `array` yields display text). A member's refusal outranks a
/// sibling's malformed annotation.
fn walk_type(
    data_type: &DataType,
    path: &FieldPath,
    mode: ColumnMappingMode,
) -> Result<Walked<MappedType>, UdfError> {
    match data_type {
        DataType::Primitive(primitive) => Ok(walk_primitive(primitive, path)),
        DataType::Variant(_) => Ok(refused_type(path, variant_cause())),
        DataType::Array(array) => {
            match walk_type(array.element_type(), &path.child(ELEMENT_SEGMENT), mode)? {
                Walked::Refused(refusal) => Ok(Walked::Refused(refusal)),
                Walked::Mapped(element) => Ok(rendered_container(NestedMembers::List {
                    element: element.members.map(Box::new),
                })),
            }
        }
        DataType::Map(map) => {
            let key = walk_type(map.key_type(), &path.child(KEY_SEGMENT), mode);
            let value = walk_type(map.value_type(), &path.child(VALUE_SEGMENT), mode);
            match (key, value) {
                (Ok(Walked::Refused(refusal)), _) | (_, Ok(Walked::Refused(refusal))) => {
                    Ok(Walked::Refused(refusal))
                }
                (Err(malformed), _) | (_, Err(malformed)) => Err(malformed),
                (Ok(Walked::Mapped(key)), Ok(Walked::Mapped(value))) => {
                    Ok(rendered_container(NestedMembers::Map {
                        key: key.members.map(Box::new),
                        value: value.members.map(Box::new),
                    }))
                }
            }
        }
        DataType::Struct(struct_type) => {
            let mut fields = Vec::with_capacity(struct_type.num_fields());
            let mut malformed = None;
            for field in struct_type.fields() {
                match walk_field(field, &path.child(field.name()), mode) {
                    Ok(Walked::Refused(refusal)) => return Ok(Walked::Refused(refusal)),
                    Ok(Walked::Mapped(mapped)) => fields.push(mapped.descriptor),
                    Err(error) => malformed = malformed.or(Some(error)),
                }
            }
            match malformed {
                Some(error) => Err(error),
                None => Ok(rendered_container(NestedMembers::Struct { fields })),
            }
        }
    }
}

fn walk_primitive(primitive: &PrimitiveType, path: &FieldPath) -> Walked<MappedType> {
    use PrimitiveType::*;
    match primitive {
        Boolean => tagged("bool"),
        Byte | Short | Integer => tagged("int32"),
        Long => tagged("int64"),
        Float => tagged("float32"),
        Double => tagged("float64"),
        String => tagged("utf8"),
        Date => tagged("date32"),
        Timestamp => tagged("timestamptz_us"),
        TimestampNtz => tagged("timestamp_us"),
        Void | IntervalYearMonth | IntervalDayTime => tagged("utf8"),
        Decimal(decimal) => {
            let (precision, scale) = (u32::from(decimal.precision()), u32::from(decimal.scale()));
            if exasol_representable_catalog_decimal(precision, scale) {
                tagged(&format!("decimal128({precision},{scale})"))
            } else {
                tagged("utf8")
            }
        }
        Binary => refused_type(path, binary_cause()),
    }
}

fn tagged(arrow_type: &str) -> Walked<MappedType> {
    Walked::Mapped(MappedType {
        arrow_type: arrow_type.to_string(),
        members: None,
    })
}

fn rendered_container(members: NestedMembers) -> Walked<MappedType> {
    Walked::Mapped(MappedType {
        arrow_type: "utf8".to_string(),
        members: Some(members),
    })
}

fn refused_type(path: &FieldPath, cause: String) -> Walked<MappedType> {
    Walked::Refused(Refusal {
        member_path: path.member_path(),
        cause,
    })
}

fn refused_column(column: &StructField, cause: &str) -> String {
    format!("Delta column '{}' {cause}", column.name())
}

fn refused_container_member(column: &StructField, member_path: &str, cause: &str) -> String {
    format!(
        "Delta column '{}' has type '{}', whose member '{member_path}' {cause}",
        column.name(),
        column.data_type(),
    )
}

fn binary_cause() -> String {
    "has type 'binary', which this engine refuses rather than casting to text: the cast replaces \
     every byte sequence that is not valid UTF-8 with NULL, silently corrupting the value; JSON \
     rendering for binary is tracked as issue #351"
        .to_string()
}

fn variant_cause() -> String {
    "has type 'variant', whose on-disk form is an opaque (metadata, value) binary pair this engine \
     cannot render as a meaningful value"
        .to_string()
}

/// Per the Delta protocol's § Type Change Metadata.
const TYPE_CHANGES_KEY: &str = "delta.typeChanges";

/// `from_type`/`to_type` are the raw strings. `field_path` is present only for a map
/// key/value or array element change; it is kept verbatim for the refusal message and never
/// used for validation.
#[derive(Debug, Clone, PartialEq, Eq)]
struct RecordedTypeChange {
    from_type: String,
    to_type: String,
    field_path: Option<String>,
}

impl RecordedTypeChange {
    /// The protocol writes `fieldPath` only for a change applying below the annotated field.
    fn applied_to(&self, annotated: &FieldPath) -> FieldPath {
        match &self.field_path {
            Some(field_path) => annotated.child(field_path),
            None => annotated.clone(),
        }
    }
}

fn type_change_cause(from_type: &str, to_type: &str) -> String {
    format!(
        "records a 'delta.typeChanges' entry from '{from_type}' to '{to_type}', which the Delta \
         protocol's type-widening feature does not support: readers must fail on any unsupported \
         recorded type change"
    )
}

/// Validates shape only; support is decided by [`is_supported_type_change`]. Unknown keys are
/// ignored, notably `tableVersion`, which Delta 3.2-era writers still emit (as in the vendored
/// `type-widening` fixture) and rejecting it would refuse conformant entries.
fn recorded_type_changes(
    field: &StructField,
    path: &FieldPath,
) -> Result<Vec<RecordedTypeChange>, UdfError> {
    let Some(value) = field.metadata().get(TYPE_CHANGES_KEY) else {
        return Ok(Vec::new());
    };
    let MetadataValue::Other(json) = value else {
        return Err(malformed_type_change(
            path,
            format!("{TYPE_CHANGES_KEY} is '{value}', which is not a JSON list"),
        ));
    };
    let entries = json.as_array().ok_or_else(|| {
        malformed_type_change(
            path,
            format!("{TYPE_CHANGES_KEY} is '{json}', which is not a JSON list"),
        )
    })?;

    entries
        .iter()
        .map(|entry| parse_type_change_entry(entry, path))
        .collect()
}

fn parse_type_change_entry(
    entry: &serde_json::Value,
    path: &FieldPath,
) -> Result<RecordedTypeChange, UdfError> {
    let object = entry.as_object().ok_or_else(|| {
        malformed_type_change(path, format!("entry '{entry}' is not a JSON object"))
    })?;

    let from_type = required_type_change_string(object, "fromType", path)?;
    let to_type = required_type_change_string(object, "toType", path)?;
    let field_path = match object.get("fieldPath") {
        None => None,
        Some(serde_json::Value::String(field_path)) => Some(field_path.clone()),
        Some(other) => {
            return Err(malformed_type_change(
                path,
                format!("fieldPath is '{other}', which is not a string"),
            ));
        }
    };

    Ok(RecordedTypeChange {
        from_type,
        to_type,
        field_path,
    })
}

fn required_type_change_string(
    object: &serde_json::Map<String, serde_json::Value>,
    key: &str,
    path: &FieldPath,
) -> Result<String, UdfError> {
    match object.get(key) {
        Some(serde_json::Value::String(value)) => Ok(value.clone()),
        Some(other) => Err(malformed_type_change(
            path,
            format!("{key} is '{other}', which is not a string"),
        )),
        None => Err(malformed_type_change(path, format!("{key} is absent"))),
    }
}

fn malformed_type_change(path: &FieldPath, problem: String) -> UdfError {
    UdfError::User(format!(
        "Delta column '{}' carries a malformed '{TYPE_CHANGES_KEY}' entry: {problem}",
        path.rendered(),
    ))
}

/// All of `Byte`, `Short`, `Int` are stored as `INT32`, so the protocol's supported decimal
/// target is `Decimal(10 + k1, k2)` for each, not one derived from the source's own range.
const INT32_SOURCE_DECIMAL_PRECISION: u8 = 10;

/// `Long` → `Decimal(20 + k1, k2)`, `INT64` being the physical form.
const INT64_SOURCE_DECIMAL_PRECISION: u8 = 20;

/// The protocol's § Type Widening supported list and nothing else. `long` → `double` is
/// refused: the list omits `Long` (lossy above 2^53) even though arrow-cast would do it.
/// A non-primitive type name answers `false`, not malformed: every protocol change is
/// primitive to primitive.
fn is_supported_type_change(change: &RecordedTypeChange) -> bool {
    match (
        parse_delta_type(&change.from_type),
        parse_delta_type(&change.to_type),
    ) {
        (Some(from), Some(to)) => widens(&from, &to),
        _ => false,
    }
}

fn widens(from: &PrimitiveType, to: &PrimitiveType) -> bool {
    use PrimitiveType::*;
    match (from, to) {
        (Byte, Short | Integer | Long) | (Short, Integer | Long) | (Integer, Long) => true,
        (Float, Double) => true,
        (Byte | Short | Integer, Double) => true,
        (Date, TimestampNtz) => true,
        (Decimal(source), Decimal(target)) => {
            widens_decimal((source.precision(), source.scale()), target)
        }
        (Byte | Short | Integer, Decimal(target)) => {
            widens_decimal((INT32_SOURCE_DECIMAL_PRECISION, 0), target)
        }
        (Long, Decimal(target)) => widens_decimal((INT64_SOURCE_DECIMAL_PRECISION, 0), target),
        _ => false,
    }
}

/// `Decimal(p, s)` → `Decimal(p + k1, s + k2)` with `k1 >= k2 >= 0`: integral digits may not
/// shrink, so `decimal(10,1)` → `decimal(11,3)` is refused.
fn widens_decimal((from_precision, from_scale): (u8, u8), to: &DecimalType) -> bool {
    let precision_growth = i32::from(to.precision()) - i32::from(from_precision);
    let scale_growth = i32::from(to.scale()) - i32::from(from_scale);
    scale_growth >= 0 && precision_growth >= scale_growth
}

/// Uses the same deserializer as `schemaString` so the two never disagree on a spelling.
fn parse_delta_type(raw: &str) -> Option<PrimitiveType> {
    serde_json::from_value(serde_json::Value::String(raw.to_string())).ok()
}
