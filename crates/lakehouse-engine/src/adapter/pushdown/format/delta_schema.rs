use delta_kernel::schema::{
    ColumnMetadataKey, DataType, DecimalType, MetadataValue, PrimitiveType, StructField, StructType,
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
/// surfaces from this call for two reasons only: a MAPPABLE column carries a malformed
/// column-mapping annotation, or any column carries a malformed `delta.typeChanges` annotation.
/// An UNSUPPORTED (as opposed to malformed) recorded type change refuses only its own column and
/// never fails the call. Performs no Delta reader-feature gating.
///
/// A column's TYPE is classified BEFORE its `delta.columnMapping.*` binding key is ever read: a
/// refused column's binding key is never looked up, so a column is refused for its type and never
/// for an annotation on a column this engine will not read.
///
/// A column whose type classifies successfully is then checked against its OWN recorded
/// `delta.typeChanges` history: an entry whose `fromType`/`toType` pair the Delta protocol's
/// type-widening feature does not support refuses the column, naming both types, through the SAME
/// refused-column list — the reader obligation `PROTOCOL.md` § Reader Requirements for Type
/// Widening states as *"validate that they support all type changes … and fail when finding any
/// unsupported type change"*. This check runs BEFORE the binding-key lookup too, for the same
/// reason: a column refused for an unsupported recorded change is never also failed for a missing
/// column-mapping annotation.
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

        if let Some(change) = unsupported_type_change(field)? {
            refused_columns.push(RefusedColumn {
                column_name: field.name().clone(),
                reason: type_change_refusal(field.name(), &change.from_type, &change.to_type),
            });
            continue;
        }

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

fn unsupported_type_change(field: &StructField) -> Result<Option<RecordedTypeChange>, UdfError> {
    Ok(recorded_type_changes(field)?
        .into_iter()
        .find(|change| !is_supported_type_change(change)))
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
/// line later. That leaves a [`UdfError`] out of [`build_delta_table_schema`] meaning a MALFORMED
/// annotation — either column-mapping or `delta.typeChanges` — never anything else; a refused
/// column is still answered as a value, never as an error.
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

/// The `delta.typeChanges` metadata key, quoted from the Delta protocol's § Type Change Metadata.
const TYPE_CHANGES_KEY: &str = "delta.typeChanges";

/// One recorded entry of a Delta field's `delta.typeChanges` metadata: a single type change the
/// table schema declares as applied to this field, per § Type Change Metadata. `from_type` and
/// `to_type` are the RAW `fromType`/`toType` strings the entry carries — `"byte"`, `"long"`,
/// `"decimal(10,2)"`, and so on — left unparsed because interpreting them against the protocol's
/// supported-pair rule is a separate concern from reading the entry's shape. An entry's optional
/// `fieldPath` — present only "When updating the type of a map key/value or array element", per
/// the protocol — is validated for shape alone and not retained: no production code reads it.
#[derive(Debug, Clone, PartialEq, Eq)]
struct RecordedTypeChange {
    from_type: String,
    to_type: String,
}

fn type_change_refusal(column_name: &str, from_type: &str, to_type: &str) -> String {
    format!(
        "Delta column '{column_name}' records a 'delta.typeChanges' entry from '{from_type}' to \
         '{to_type}', which the Delta protocol's type-widening feature does not support: readers \
         must fail on any unsupported recorded type change"
    )
}

/// Parses `field`'s `delta.typeChanges` metadata into its recorded type-change entries.
///
/// Validates only the entry's SHAPE, never whether the recorded change is one this engine
/// supports — that predicate belongs to the Delta protocol validation this parser feeds, not to
/// reading the annotation. Returns an empty list for a field carrying no `delta.typeChanges` key,
/// so an unannotated table (or column) is unaffected. Ignores every entry key besides `fromType`,
/// `toType`, and `fieldPath` — notably `tableVersion`, which the superseded accepted RFC required
/// and which Delta 3.2-era clients still write on every entry, including all thirteen entries of
/// the vendored `type-widening` fixture — because rejecting an unrecognized key would refuse an
/// otherwise valid, protocol-conformant entry for carrying one.
fn recorded_type_changes(field: &StructField) -> Result<Vec<RecordedTypeChange>, UdfError> {
    let Some(value) = field.metadata().get(TYPE_CHANGES_KEY) else {
        return Ok(Vec::new());
    };
    let MetadataValue::Other(json) = value else {
        return Err(malformed_type_change(
            field,
            format!("{TYPE_CHANGES_KEY} is '{value}', which is not a JSON list"),
        ));
    };
    let entries = json.as_array().ok_or_else(|| {
        malformed_type_change(
            field,
            format!("{TYPE_CHANGES_KEY} is '{json}', which is not a JSON list"),
        )
    })?;

    entries
        .iter()
        .map(|entry| parse_type_change_entry(field, entry))
        .collect()
}

fn parse_type_change_entry(
    field: &StructField,
    entry: &serde_json::Value,
) -> Result<RecordedTypeChange, UdfError> {
    let object = entry.as_object().ok_or_else(|| {
        malformed_type_change(field, format!("entry '{entry}' is not a JSON object"))
    })?;

    let from_type = required_type_change_string(field, object, "fromType")?;
    let to_type = required_type_change_string(field, object, "toType")?;
    match object.get("fieldPath") {
        None | Some(serde_json::Value::String(_)) => {}
        Some(other) => {
            return Err(malformed_type_change(
                field,
                format!("fieldPath is '{other}', which is not a string"),
            ));
        }
    }

    Ok(RecordedTypeChange { from_type, to_type })
}

fn required_type_change_string(
    field: &StructField,
    object: &serde_json::Map<String, serde_json::Value>,
    key: &str,
) -> Result<String, UdfError> {
    match object.get(key) {
        Some(serde_json::Value::String(value)) => Ok(value.clone()),
        Some(other) => Err(malformed_type_change(
            field,
            format!("{key} is '{other}', which is not a string"),
        )),
        None => Err(malformed_type_change(field, format!("{key} is absent"))),
    }
}

fn malformed_type_change(field: &StructField, problem: String) -> UdfError {
    UdfError::User(format!(
        "Delta column '{}' carries a malformed '{TYPE_CHANGES_KEY}' entry: {problem}",
        field.name(),
    ))
}

/// The precision the Delta protocol gives a `Byte`, `Short`, or `Int` source when the target is a
/// decimal. All three are stored as `INT32`, so the protocol's supported target is
/// `Decimal(10 + k1, k2)` for every one of them — never a target derived from the declared source
/// type's own narrower range.
const INT32_SOURCE_DECIMAL_PRECISION: u8 = 10;

/// The precision the Delta protocol gives a `Long` source when the target is a decimal:
/// `Decimal(20 + k1, k2)`, `INT64` being the physical form.
const INT64_SOURCE_DECIMAL_PRECISION: u8 = 20;

/// Answers whether the Delta protocol's type-widening feature supports the change `change`
/// records, per § Type Widening's supported list — the check § Reader Requirements for Type
/// Widening makes a reader obligation: *"Readers must validate that they support all type changes
/// in the `delta.typeChanges` field … and fail when finding any unsupported type change."*
///
/// Answers the protocol's list and nothing else. `long` → `double` is REFUSED: the floating-point
/// bullet names `Byte`, `Short` or `Int` and omits `Long`, which is lossy above 2^53, so a cast
/// arrow-cast will happily perform is still not a change any conforming writer records.
///
/// `field_path` is deliberately not read. It names a map key/value or an array element, and this
/// engine refuses `map` outright and text-renders `array<E>`, so no scalar value is at risk and
/// parsing the nested path grammar would buy nothing.
///
/// A `fromType`/`toType` that is not a Delta primitive type name answers `false` — one more pair
/// the protocol's list does not contain. It is not a malformed entry: [`recorded_type_changes`]
/// owns the entry's shape, and every type change the protocol defines is primitive to primitive.
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

/// The protocol's decimal rule: `Decimal(p, s)` → `Decimal(p + k1, s + k2)` where `k1 >= k2 >= 0`.
/// `k1 >= k2` forbids the INTEGRAL digit count shrinking, which makes this strictly stronger than
/// "precision and scale may both grow" — `decimal(10,1)` → `decimal(11,3)` grows both and is
/// still refused.
fn widens_decimal((from_precision, from_scale): (u8, u8), to: &DecimalType) -> bool {
    let precision_growth = i32::from(to.precision()) - i32::from(from_precision);
    let scale_growth = i32::from(to.scale()) - i32::from(from_scale);
    scale_growth >= 0 && precision_growth >= scale_growth
}

/// Parses a raw `fromType`/`toType` name with the SAME deserializer the table's `schemaString` is
/// read by, so the two can never disagree on a spelling — including `decimal(p,s)`, whose grammar
/// would otherwise have a second owner here.
fn parse_delta_type(raw: &str) -> Option<PrimitiveType> {
    serde_json::from_value(serde_json::Value::String(raw.to_string())).ok()
}
