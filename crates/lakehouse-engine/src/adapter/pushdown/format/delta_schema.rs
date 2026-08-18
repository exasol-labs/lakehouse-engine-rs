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
/// decimal, `void`, either interval type, or a CONTAINER (`array`, `struct`, or `map`) every one of
/// whose members is itself mappable at every nesting depth — or REFUSES the column: no
/// [`LogicalField`] is emitted for it, and it is recorded in the returned refused list, naming the
/// reason its type cannot be rendered faithfully. A refused column never fails this call by
/// itself — refusing the whole table when NO column is mappable is the caller's decision, made once
/// every column has been classified. A [`UdfError`] surfaces from this call for two reasons only: a
/// MAPPABLE column carries a malformed column-mapping annotation at any depth, or any column
/// carries a malformed `delta.typeChanges` annotation at any depth. An UNSUPPORTED (as opposed to
/// malformed) recorded type change refuses only its own column and never fails the call. Performs
/// no Delta reader-feature gating.
///
/// Each column is resolved by ONE recursive walk that visits every nested field exactly once, so
/// the three answers a column needs — its Arrow tag, its recorded type changes' validity, and the
/// nested descriptor the JSON renderer keys its value by — can never disagree about which fields
/// the column has.
///
/// A column's TYPE is classified BEFORE its `delta.columnMapping.*` binding key is ever read, at
/// every depth: a refused column's binding key is never looked up, so a column is refused for its
/// type and never for an annotation on a column this engine will not read.
///
/// A column whose type classifies successfully is then checked against the recorded
/// `delta.typeChanges` history of EVERY field it carries, nested ones included: an entry whose
/// `fromType`/`toType` pair the Delta protocol's type-widening feature does not support refuses the
/// column, naming both types and the annotated field's path, through the SAME refused-column list —
/// the reader obligation `PROTOCOL.md` § Reader Requirements for Type Widening states as *"validate
/// that they support all type changes … and fail when finding any unsupported type change"*. This
/// check runs BEFORE the binding-key lookup too, for the same reason: a column refused for an
/// unsupported recorded change is never also failed for a missing column-mapping annotation.
///
/// Under `id`/`name` column mapping a MAPPABLE column's binding key comes from its
/// `delta.columnMapping.*` annotations ALONE, at every depth: a field missing either annotation, or
/// carrying an id no `i32` holds, is refused — its ordinal position and its logical name are values
/// the writer never used.
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

/// The ONE binding key `field` binds by, as the `(field_id, physical_name)` pair both
/// [`LogicalField`] and [`NestedField`] hold — one rule for a top-level column and a nested field
/// alike, `path` locating the field for the message a missing annotation produces. At most one member is ever populated: two keys would need a precedence
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

/// BOTH `delta.columnMapping.*` annotations a column must carry under `id`/`name` mode.
///
/// Both are read in EITHER mapped mode even though only one becomes the column's binding key,
/// because the Delta protocol requires both in either mode and nothing on the read path validates
/// either — so a column declaring only the one its current mode happens to select is refused here
/// rather than reaching the scan as a half-annotated column.
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

/// The `delta.columnMapping.physicalName` annotation `field` carries — the name its Parquet
/// counterpart was written under.
///
/// Absent, or present but non-string, is refused rather than substituted, because nothing on the
/// read path validates the annotation and the logical name is a column the writer never wrote.
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

/// The `delta.columnMapping.id` annotation `field` carries, refused when absent or wider than the
/// `i32` the wire carries.
///
/// Never substituted by the field's ordinal position: an ordinal can collide with a sibling
/// column's assigned id, and no writer ever wrote it into a file.
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

/// What walking one Delta type or one Delta field answers: it maps, or the column carrying it is
/// refused.
///
/// Refusing a column is an expected outcome of reading a Delta schema — never a failure of it — so
/// it is answered as a value rather than signalled as an error and converted back to data one line
/// later. That leaves a [`UdfError`] out of [`build_delta_table_schema`] meaning a MALFORMED
/// annotation — either column-mapping or `delta.typeChanges` — and nothing else.
enum Walked<T> {
    Mapped(T),
    Refused(Refusal),
}

/// Why a column is not mapped: the cause, phrased as the predicate a composer completes with the
/// column's name, and the path of the nested member the cause belongs to — `None` when the cause is
/// the column's own declared type or its own recorded type change.
struct Refusal {
    member_path: Option<String>,
    cause: String,
}

impl Refusal {
    /// This refusal stated for the column that carries it, by whichever of the two composers the
    /// cause's own location selects.
    fn stated_for(&self, column: &StructField) -> String {
        match &self.member_path {
            Some(member_path) => refused_container_member(column, member_path, &self.cause),
            None => refused_column(column, &self.cause),
        }
    }
}

/// What one mapped Delta TYPE answers: the Arrow tag the scan binds it by, and — for a container —
/// the members the JSON renderer keys its value by, `None` for every other type.
struct MappedType {
    arrow_type: String,
    members: Option<NestedMembers>,
}

/// What one mapped Delta FIELD answers: its type's Arrow tag, which only a top-level column
/// declares, and the descriptor entry carrying its logical name, its one binding key, and its own
/// members.
struct MappedField {
    arrow_type: String,
    descriptor: NestedField,
}

/// One field's path within the top-level column that carries it: the column's own name, then one
/// segment per nesting step — a `struct` field's name, or `element`, `key`, or `value` for the
/// positional member of an `array` or a `map`, which is the vocabulary the Delta protocol's own
/// `fieldPath` uses for exactly those positions.
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

    /// This path as the member path a refusal reports, or `None` when it is the top-level column
    /// itself and therefore names no member inside it.
    fn member_path(&self) -> Option<String> {
        (self.0.len() > 1).then(|| self.rendered())
    }
}

/// The path segment naming an `array`'s element.
const ELEMENT_SEGMENT: &str = "element";

/// The path segment naming a `map`'s key.
const KEY_SEGMENT: &str = "key";

/// The path segment naming a `map`'s value.
const VALUE_SEGMENT: &str = "value";

/// Walks `field` — a top-level Delta column, or a `struct` field at any depth — at its own `path`
/// within its column, into the three answers its column needs from it.
///
/// The three checks run in the order their outcomes must outrank one another: the field's TYPE
/// first, so a field refused for its type is never instead failed for an annotation on a column
/// this engine will not read; then the field's own recorded `delta.typeChanges` history; then the
/// binding key its column-mapping mode selects.
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

/// Walks `data_type` at `path`, recursing through every member of a container: an `array`'s
/// element, a `struct`'s every field, and a `map`'s key AND value.
///
/// A container maps exactly when every one of its members maps, and is tagged `utf8` because the
/// JSON renderer recurses natively through every nesting depth. Membership of the rendered set is
/// therefore decided by whether each member is itself RENDERABLE — never by whether the container's
/// Arrow form can be cast to text, which for `struct` and `map` it cannot be and for `array` yields
/// display text rather than JSON.
///
/// A member's REFUSAL outranks a sibling's malformed annotation: a column refused for one member's
/// type must never instead fail the call for an annotation on a column this engine will not read.
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

/// A container this engine renders as one JSON document: tagged `utf8`, carrying the members the
/// renderer keys that document by.
fn rendered_container(members: NestedMembers) -> Walked<MappedType> {
    Walked::Mapped(MappedType {
        arrow_type: "utf8".to_string(),
        members: Some(members),
    })
}

/// A refusal whose cause is the type at `path` itself, which `path` alone decides is the column's
/// own type or one member inside it.
fn refused_type(path: &FieldPath, cause: String) -> Walked<MappedType> {
    Walked::Refused(Refusal {
        member_path: path.member_path(),
        cause,
    })
}

/// The refusal of a column for its OWN declared type or its OWN recorded type change.
fn refused_column(column: &StructField, cause: &str) -> String {
    format!("Delta column '{}' {cause}", column.name())
}

/// The refusal of a column for ONE member inside its container type — an `array`'s element, a
/// `struct`'s field, or a `map`'s key or value alike, at any nesting depth.
///
/// The one composer every container kind shares. It names the column's own declared type, the path
/// of the offending member, and that member's own cause, so nesting adds no message layer per kind
/// and no operator is told the column has a member's type.
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

/// The `delta.typeChanges` metadata key, quoted from the Delta protocol's § Type Change Metadata.
const TYPE_CHANGES_KEY: &str = "delta.typeChanges";

/// One recorded entry of a Delta field's `delta.typeChanges` metadata: a single type change the
/// table schema declares as applied to this field, per § Type Change Metadata. `from_type` and
/// `to_type` are the RAW `fromType`/`toType` strings the entry carries — `"byte"`, `"long"`,
/// `"decimal(10,2)"`, and so on — left unparsed because interpreting them against the protocol's
/// supported-pair rule is a separate concern from reading the entry's shape. `field_path` is the
/// entry's optional `fieldPath`, present only "When updating the type of a map key/value or array
/// element", per the protocol: it is retained verbatim and never interpreted, locating the change
/// for the operator who reads a refusal while the `fromType`/`toType` pair stays the sole validation
/// input.
#[derive(Debug, Clone, PartialEq, Eq)]
struct RecordedTypeChange {
    from_type: String,
    to_type: String,
    field_path: Option<String>,
}

impl RecordedTypeChange {
    /// The path of the field this entry's change applies to: the annotated field's own path,
    /// extended by the entry's own `fieldPath` when it carries one, because the protocol writes that
    /// key only for a change applying to a member BELOW the annotated field.
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
/// The entry's `field_path` is never an input here: the protocol's supported-pair rule does not
/// depend on it, so the path is retained for the refusal to REPORT and its grammar is never parsed.
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
