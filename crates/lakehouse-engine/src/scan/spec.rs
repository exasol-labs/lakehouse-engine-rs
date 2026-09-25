//! Scan spec split across two UDF arguments: the shard-invariant [`CommonScanSpec`] (argument 0,
//! serialized once per fan-out) and the per-shard files array (argument 1). `CommonScanSpec` has
//! no `files` field, so "files is the only per-shard field" is a type-level guarantee.
//!
//! Credentials MUST NEVER appear in any error message.
use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

/// AVG decomposes into (sum, count) and the STDDEV/VARIANCE family into (cnt, sum, sum_sq); the
/// wrapper finishes them. Single-group `COUNT(DISTINCT col)` is not an aggregate partial but a
/// DISTINCT row-scan fan-out (`CommonScanSpec::distinct`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum AggKind {
    Count,
    CountCol,
    Sum,
    Min,
    Max,
    Avg,
    /// VAR_POP / VARIANCE_POP.
    VarPop,
    /// VAR_SAMP / VARIANCE / VARIANCE_SAMP.
    VarSamp,
    StddevPop,
    /// STDDEV / STDDEV_SAMP.
    StddevSamp,
}

/// One partial-aggregate column. The scan renders its DataFusion expression and the adapter its
/// `EMITS` type, so a new variant is a compile error at both. `CountStar` and `CountArg` stay
/// distinct because they render different SQL (`COUNT(*)` vs `COUNT(<arg>)`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PartialAggColumn {
    CountStar,
    CountArg,
    Sum,
    Min,
    Max,
    AvgSum,
    AvgCnt,
    StatCnt,
    StatSum,
    /// `SUM(<arg> * <arg>)`.
    StatSumSq,
}

impl PartialAggColumn {
    /// A counter contributes `0` for an empty shard, a value column NULL. A boolean keeps this
    /// module free of `exasol_udf_sdk`.
    pub fn is_counter(&self) -> bool {
        match self {
            Self::CountStar | Self::CountArg | Self::AvgCnt | Self::StatCnt => true,
            Self::Sum | Self::Min | Self::Max | Self::AvgSum | Self::StatSum | Self::StatSumSq => {
                false
            }
        }
    }
}

/// The sole owner of the `PARTIAL_<role>_<ordinal>` text shared by the scan aliases, the
/// adapter's `EMITS`, and its merge expressions; unquoted, since each site quotes itself.
/// `ordinal` is the aggregate's plan position, shared by all its columns.
pub fn partial_column_name(col: PartialAggColumn, ordinal: usize) -> String {
    let role = match col {
        PartialAggColumn::CountStar | PartialAggColumn::CountArg => "count",
        PartialAggColumn::Sum => "sum",
        PartialAggColumn::Min => "min",
        PartialAggColumn::Max => "max",
        PartialAggColumn::AvgSum => "avg_sum",
        PartialAggColumn::AvgCnt => "avg_cnt",
        PartialAggColumn::StatCnt => "stat_cnt",
        PartialAggColumn::StatSum => "stat_sum",
        PartialAggColumn::StatSumSq => "stat_sumsq",
    };
    format!("PARTIAL_{role}_{ordinal}")
}

impl AggKind {
    /// The single owner of the COLUMN CONTRACT's arity and order. Emit paths address columns
    /// positionally, so any site disagreeing would shift every later aggregate's value.
    pub fn partial_columns(&self) -> &'static [PartialAggColumn] {
        match self {
            AggKind::Count => &[PartialAggColumn::CountStar],
            AggKind::CountCol => &[PartialAggColumn::CountArg],
            AggKind::Sum => &[PartialAggColumn::Sum],
            AggKind::Min => &[PartialAggColumn::Min],
            AggKind::Max => &[PartialAggColumn::Max],
            AggKind::Avg => &[PartialAggColumn::AvgSum, PartialAggColumn::AvgCnt],
            AggKind::VarPop | AggKind::VarSamp | AggKind::StddevPop | AggKind::StddevSamp => &[
                PartialAggColumn::StatCnt,
                PartialAggColumn::StatSum,
                PartialAggColumn::StatSumSq,
            ],
        }
    }
}

/// `column` is `None` for `COUNT(*)`; otherwise the uppercase projected column name. `arg_expr`
/// is a rendered DataFusion fragment when the argument is an expression (e.g.
/// `SUM(LENGTH(L_COMMENT))`); kept separate from `column` so bare-column lookups and the JSON
/// wire shape are unaffected.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AggregatePlan {
    pub kind: AggKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub column: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub arg_expr: Option<String>,
}

/// A [`Column`](ProjectionItem::Column) is quoted as an identifier by the scan; an
/// [`Expr`](ProjectionItem::Expr) is spliced verbatim, being valid DataFusion SQL already.
///
/// Untagged serde: a `Column` is a bare JSON string and an `Expr` is `{"expr": "..."}`, so legacy
/// plain-string projections still load as columns.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ProjectionItem {
    /// E.g. `("SCORE" * 2)`.
    Expr { expr: String },
    /// E.g. `SCORE`.
    Column(String),
}

impl ProjectionItem {
    /// Only names the positional EMITS slot; for an `Expr` it is the fragment itself.
    pub fn emit_name(&self) -> &str {
        match self {
            ProjectionItem::Column(name) => name,
            ProjectionItem::Expr { expr } => expr,
        }
    }
}

impl From<&str> for ProjectionItem {
    fn from(name: &str) -> Self {
        ProjectionItem::Column(name.to_string())
    }
}

impl From<String> for ProjectionItem {
    fn from(name: String) -> Self {
        ProjectionItem::Column(name)
    }
}

impl PartialEq<&str> for ProjectionItem {
    fn eq(&self, other: &&str) -> bool {
        self.emit_name() == *other
    }
}

/// `column` is a bare uppercase identifier: top-N (`TopK`) eligibility is deliberately limited
/// to bare columns, although Exasol may also send expression sort keys (#198), which never
/// construct a `SortKey`.
///
/// `ascending` and `nulls_last` map to Exasol's `orderBy[].isAscending` / `nullsLast` and must be
/// rendered explicitly on both the shard and merge `ORDER BY` so the sorts rank identically.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SortKey {
    pub column: String,
    pub ascending: bool,
    pub nulls_last: bool,
}

impl SortKey {
    /// `"COLUMN" ASC|DESC NULLS FIRST|LAST`, valid in both DataFusion and Exasol SQL.
    pub fn render_order_by_element(&self) -> String {
        self.render_ordered(&format!("\"{}\"", self.column.replace('"', "\"\"")))
    }

    /// `self.column` is deliberately unread: `expr` carries the ordering target.
    pub fn render_ordered(&self, expr: &str) -> String {
        render_ordered(expr, self.ascending, self.nulls_last)
    }
}

/// `<expr> ASC|DESC NULLS FIRST|LAST`. Every ORDER BY the adapter emits goes through this one
/// seam, structurally guaranteeing that all sorts agree on direction and NULL placement.
pub fn render_ordered(expr: &str, ascending: bool, nulls_last: bool) -> String {
    let direction = if ascending { "ASC" } else { "DESC" };
    let nulls = if nulls_last {
        "NULLS LAST"
    } else {
        "NULLS FIRST"
    };
    format!("{expr} {direction} {nulls}")
}

/// E.g. `"L_EXTENDEDPRICE" DESC NULLS LAST, "L_ORDERKEY" ASC NULLS FIRST`, without the
/// `ORDER BY` keyword. Shared by the adapter's merge sort and the scan's per-shard sort. Empty
/// keys yield an empty string, which callers must guard.
pub fn render_order_by_clause(keys: &[SortKey]) -> String {
    keys.iter()
        .map(SortKey::render_order_by_element)
        .collect::<Vec<_>>()
        .join(", ")
}

/// Declared in `lakehouse-catalog` (which produces them) and re-exported at their consumers'
/// path. The wire contract is pinned by `common_blob_wire_is_byte_stable`.
pub use lakehouse_catalog::{AdlsCred, CatalogProps, StorageBackend, StorageProps};

/// # Binding key
///
/// AT MOST ONE of `field_id` and `physical_name` is populated; which one encodes the binding
/// decision, so nothing downstream re-derives it:
///
/// | Producer | `field_id` | `physical_name` | Scan-side binding |
/// |---|---|---|---|
/// | Iceberg (always) | `Some(id)` | `None` | embedded `PARQUET:field_id`, then `name_mapping`, then the physical name |
/// | Delta `id` column mapping | `Some(columnMapping.id)` | `None` | embedded `PARQUET:field_id` |
/// | Delta `name` column mapping | `None` | `Some(physicalName)` | the declared physical name |
/// | Delta `none` column mapping | `None` | `None` | identity — the logical name itself |
///
/// A field with neither key carries no stand-in ordinal, which would invite a false
/// `PARQUET:field_id` match.
///
/// `arrow_type` is a compact tag (`types::mapping::arrow_type_to_tag` / `arrow_type_from_tag`):
/// `"bool"`, `"int32"`, `"int64"`, `"float32"`, `"float64"`, `"utf8"`, `"date32"`,
/// `"timestamp_us"`, `"timestamp_ns"`, `"timestamptz_us"`, `"timestamptz_ns"`,
/// `"decimal128(p,s)"`.
///
/// # `initial_default`
///
/// The Iceberg `initial-default` (column-projection rule 3), encoded as the RAW primitive per tag:
///
/// | `arrow_type` | encoded text of `initial_default` |
/// |---|---|
/// | `"bool"` | `"true"` / `"false"` |
/// | `"int32"` | decimal `i32` |
/// | `"int64"` | decimal `i64` |
/// | `"float32"` | `f32` in Rust `Display` form (round-trippable) |
/// | `"float64"` | `f64` in Rust `Display` form (round-trippable) |
/// | `"utf8"` | the string value verbatim |
/// | `"date32"` | `i32` days since the Unix epoch |
/// | `"timestamp_us"` / `"timestamptz_us"` | `i64` microseconds |
/// | `"timestamp_ns"` / `"timestamptz_ns"` | `i64` nanoseconds |
/// | `"decimal128(p,s)"` | `i128` unscaled mantissa |
///
/// Non-primitive defaults and primitives reaching only the JSON-fallback `"utf8"` path encode
/// `None` and fall through to NULL / required-error.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogicalField {
    /// Declared FIRST so a field-id-bound column serializes `"field_id":N` as its leading key.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub field_id: Option<i32>,
    pub name: String,
    pub arrow_type: String,
    pub nullable: bool,
    /// Absent from JSON when `None`, so older specs deserialize unchanged.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub initial_default: Option<String>,
    /// Lets the JSON renderer key a struct by LOGICAL member names. Not a type: `arrow_type`
    /// stays `"utf8"` for every nested column.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub nested: Option<NestedMembers>,
    /// Delta `name` mapping's `physicalName`. Appended LAST so a field-id-bound column's
    /// encoding gains no key.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub physical_name: Option<String>,
}

/// The format-neutral nested counterpart of [`LogicalField`]'s binding-key choice, recursed.
/// List elements and map keys/values are positional and carry only their own members, present
/// only when they are containers: `list<string>` encodes as `{"list":{}}` and
/// `map<int,struct<a>>` as `{"map":{"value":{"struct":{"fields":[{"name":"a"}]}}}}`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NestedMembers {
    List {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        element: Option<Box<NestedMembers>>,
    },
    /// In the schema's declared order.
    Struct { fields: Vec<NestedField> },
    Map {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        key: Option<Box<NestedMembers>>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        value: Option<Box<NestedMembers>>,
    },
}

/// Carries AT MOST ONE binding key, like [`LogicalField`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NestedField {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub field_id: Option<i32>,
    /// The name the rendered JSON object uses.
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub physical_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub nested: Option<NestedMembers>,
}

/// One flattened top-level entry of Iceberg's `schema.name-mapping.default`, for files written
/// without an embedded `PARQUET:field_id`. Nested entries are never parsed (issue #28).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NameMappingEntry {
    pub name: String,
    pub field_id: i32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ScanStorage {
    Connection {
        name: String,
        allow_http: bool,
    },
    Sealed {
        name: String,
        payload: String,
    },
    /// Host-test only — the adapter never emits this variant.
    Inline(StorageBackend),
}

/// Only `Inner` is ever produced; the adapter declines every other join shape.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum JoinType {
    Inner,
}

/// SHARD-INVARIANT: the dimension side's full file list is resolved once and carried in the
/// [`CommonScanSpec`], so every shard joins it against its own fact-file subset. `condition` is
/// rendered DataFusion SQL, spliced verbatim.
///
/// `storage` is required (no default) because a vended credential is scoped to its own table: a
/// join block without its own storage fails to deserialize rather than borrow the fact side's.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JoinSpec {
    /// Empty means every path is absolute.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub table_root: String,

    /// Each entry's deletes are applied to the dimension registration too.
    pub files: Vec<FileEntry>,

    /// Empty falls back to first-file schema inference.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub logical_schema: Vec<LogicalField>,

    /// Empty means no name-mapping property is present.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub name_mapping: Vec<NameMappingEntry>,

    pub join_type: JoinType,

    pub condition: String,

    /// Applied AFTER the node-local join and its `WHERE`, never to either side's scan, which
    /// would drop rows the join or filter keeps. Not [`CommonScanSpec::limit`], which caps the
    /// scan. Only unordered caps are pushed: each shard may truncate at `n` and the merge again;
    /// an ordered window rides on the adapter's outer wrapper instead.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub post_join_limit: Option<u64>,

    /// Empty on every Iceberg join spec, and absent from JSON when empty so the Iceberg
    /// encoding stays byte-identical.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub partition_columns: Vec<String>,

    pub storage: ScanStorage,
}

/// Closed so an unknown storage kind fails at plan time instead of being silently ignored,
/// leaving deleted rows in the result.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeltaDeletionVectorStorage {
    /// Delta `u`: a UUID-named `.bin` file addressed relative to the table root.
    UuidRelative,
    /// Delta `i`: the vector itself, encoded inline in `path_or_inline_dv`.
    Inline,
    /// Delta `p`: an absolute path.
    AbsolutePath,
}

/// One row-deletion mechanism, naming itself on the wire, so the scan dispatches on content and
/// never on which format produced the spec. Carries only the minimum payload (ADR-085): no
/// serialized Iceberg `Schema` or `BoundPredicate`.
///
/// Only [`IcebergPositionalDelete`](DeleteMechanism::IcebergPositionalDelete) and
/// [`DeltaDeletionVector`](DeleteMechanism::DeltaDeletionVector) are applied. Plan time rejects
/// the others; they exist so the read-time backstop can refuse one cleanly if it slips through.
///
/// Serde routes through [`DeleteMechanismWire`]: an internally-tagged enum would emit its
/// discriminant first and break the pinned `{"path":…,"size":…,"content_type":…}` key order.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(from = "DeleteMechanismWire", into = "DeleteMechanismWire")]
pub enum DeleteMechanism {
    /// An Iceberg Parquet positional-delete file (`file_path`/`pos` columns).
    IcebergPositionalDelete {
        /// Relative to [`CommonScanSpec::table_root`] or absolute, like [`FileEntry::path`].
        path: String,
        /// Builds the delete file's `ObjectMeta` without a HEAD, like [`FileEntry::size`].
        size: u64,
    },
    /// Never applied by this engine.
    IcebergEqualityDelete { path: String, size: u64 },
    /// A Puffin-encoded Iceberg v3 deletion vector. Never applied by this engine.
    IcebergPuffinDeletionVector { path: String, size: u64 },
    /// A byte RANGE inside a possibly-shared `.bin` file, resolved into no path at plan time.
    DeltaDeletionVector {
        storage: DeltaDeletionVectorStorage,
        /// Delta `pathOrInlineDv` exactly as logged; path reconstruction is deferred to file
        /// registration.
        path_or_inline_dv: String,
        /// `None` when the file holds this vector alone; absent from JSON when `None`.
        offset: Option<i32>,
        size_in_bytes: i32,
        cardinality: i64,
    },
}

impl DeleteMechanism {
    /// The axis [`ScanSpec::files_from_json`]'s mutual-exclusion gate turns on. Matched
    /// exhaustively so a new mechanism must state its side.
    fn is_delete_file_reference(&self) -> bool {
        match self {
            Self::IcebergPositionalDelete { .. }
            | Self::IcebergEqualityDelete { .. }
            | Self::IcebergPuffinDeletionVector { .. } => true,
            Self::DeltaDeletionVector { .. } => false,
        }
    }

    /// `None` for a deletion vector, which is resolved at file registration and never
    /// addressed from the delete list, whatever its value looks like. Callers treating a delete
    /// as an addressable file ask here, so that refusal is stated once.
    pub fn object_store_path(&self) -> Option<&str> {
        match self {
            Self::IcebergPositionalDelete { path, .. }
            | Self::IcebergEqualityDelete { path, .. }
            | Self::IcebergPuffinDeletionVector { path, .. } => Some(path.as_str()),
            Self::DeltaDeletionVector { .. } => None,
        }
    }

    /// Mutable counterpart of [`object_store_path`](Self::object_store_path) for plan-time rewriting.
    pub(crate) fn object_store_path_mut(&mut self) -> Option<&mut String> {
        match self {
            Self::IcebergPositionalDelete { path, .. }
            | Self::IcebergEqualityDelete { path, .. }
            | Self::IcebergPuffinDeletionVector { path, .. } => Some(path),
            Self::DeltaDeletionVector { .. } => None,
        }
    }
}

/// Wire-private: reproduces the frozen `content_type` tag.
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum IcebergDeleteContentType {
    PositionDeletes,
    EqualityDeletes,
    PuffinDeletionVector,
}

/// `untagged` over DISJOINT key sets, so neither arm matches the other's encoding and a member
/// matching neither is refused. The Iceberg arm's field order is the frozen key order
/// `{"path":…,"size":…,"content_type":…}`.
#[derive(Serialize, Deserialize)]
#[serde(untagged)]
enum DeleteMechanismWire {
    IcebergDeleteFile {
        path: String,
        size: u64,
        content_type: IcebergDeleteContentType,
    },
    DeltaDeletionVector {
        storage: DeltaDeletionVectorStorage,
        path_or_inline_dv: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        offset: Option<i32>,
        size_in_bytes: i32,
        cardinality: i64,
    },
}

impl From<DeleteMechanismWire> for DeleteMechanism {
    fn from(wire: DeleteMechanismWire) -> Self {
        match wire {
            DeleteMechanismWire::IcebergDeleteFile {
                path,
                size,
                content_type,
            } => match content_type {
                IcebergDeleteContentType::PositionDeletes => {
                    DeleteMechanism::IcebergPositionalDelete { path, size }
                }
                IcebergDeleteContentType::EqualityDeletes => {
                    DeleteMechanism::IcebergEqualityDelete { path, size }
                }
                IcebergDeleteContentType::PuffinDeletionVector => {
                    DeleteMechanism::IcebergPuffinDeletionVector { path, size }
                }
            },
            DeleteMechanismWire::DeltaDeletionVector {
                storage,
                path_or_inline_dv,
                offset,
                size_in_bytes,
                cardinality,
            } => DeleteMechanism::DeltaDeletionVector {
                storage,
                path_or_inline_dv,
                offset,
                size_in_bytes,
                cardinality,
            },
        }
    }
}

impl From<DeleteMechanism> for DeleteMechanismWire {
    fn from(mechanism: DeleteMechanism) -> Self {
        match mechanism {
            DeleteMechanism::IcebergPositionalDelete { path, size } => {
                DeleteMechanismWire::IcebergDeleteFile {
                    path,
                    size,
                    content_type: IcebergDeleteContentType::PositionDeletes,
                }
            }
            DeleteMechanism::IcebergEqualityDelete { path, size } => {
                DeleteMechanismWire::IcebergDeleteFile {
                    path,
                    size,
                    content_type: IcebergDeleteContentType::EqualityDeletes,
                }
            }
            DeleteMechanism::IcebergPuffinDeletionVector { path, size } => {
                DeleteMechanismWire::IcebergDeleteFile {
                    path,
                    size,
                    content_type: IcebergDeleteContentType::PuffinDeletionVector,
                }
            }
            DeleteMechanism::DeltaDeletionVector {
                storage,
                path_or_inline_dv,
                offset,
                size_in_bytes,
                cardinality,
            } => DeleteMechanismWire::DeltaDeletionVector {
                storage,
                path_or_inline_dv,
                offset,
                size_in_bytes,
                cardinality,
            },
        }
    }
}

/// On the wire, [`FileEntryWire`] accepts a legacy `[path, size]` tuple, a
/// `[path, size, deletes]` tuple, or an object carrying `partition_values` (disjoint from both
/// tuples). Serialization picks the SHORTEST form: 2-tuple when bare, 3-tuple with deletes, the
/// object whenever there are partition values, never by table format.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(from = "FileEntryWire", into = "FileEntryWire")]
pub struct FileEntry {
    /// Relative to [`CommonScanSpec::table_root`] when under it, otherwise absolute; stored
    /// exactly as the catalog or log records it. Read as a URL-encoded reference (see
    /// [`encode_file_path`]).
    pub path: String,
    /// Builds the file's `ObjectMeta` without an object-store HEAD.
    pub size: u64,
    /// Every deletion mechanism, whichever format planned the scan. Holds Iceberg delete-file
    /// references OR one deletion vector, never both: applying both returns wrong rows, and
    /// [`ScanSpec::files_from_json`] refuses the mix.
    pub deletes: Vec<DeleteMechanism>,
    /// One entry per [`CommonScanSpec::partition_columns`] column, recorded outside the data
    /// file. A key with no value is a NULL partition value, whereas a missing key is a planning
    /// defect the scan detects, hence the optional VALUE. `BTreeMap` keeps the wire byte-stable.
    pub partition_values: BTreeMap<String, Option<String>>,
}

/// `untagged` resolves in declaration order; an object matches neither tuple variant and an
/// array never matches the struct variant. `WithPartitionValues` also carries `deletes` so the
/// conversion from [`FileEntry`] is total and lossless.
#[derive(Serialize, Deserialize)]
#[serde(untagged)]
enum FileEntryWire {
    Legacy(String, u64),
    WithDeletes(String, u64, Vec<DeleteMechanism>),
    WithPartitionValues {
        path: String,
        size: u64,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        deletes: Vec<DeleteMechanism>,
        partition_values: BTreeMap<String, Option<String>>,
    },
}

impl From<FileEntryWire> for FileEntry {
    fn from(wire: FileEntryWire) -> Self {
        match wire {
            FileEntryWire::Legacy(path, size) => FileEntry {
                path,
                size,
                deletes: Vec::new(),
                partition_values: BTreeMap::new(),
            },
            FileEntryWire::WithDeletes(path, size, deletes) => FileEntry {
                path,
                size,
                deletes,
                partition_values: BTreeMap::new(),
            },
            FileEntryWire::WithPartitionValues {
                path,
                size,
                deletes,
                partition_values,
            } => FileEntry {
                path,
                size,
                deletes,
                partition_values,
            },
        }
    }
}

impl From<FileEntry> for FileEntryWire {
    /// Destructured exhaustively so a new [`FileEntry`] field is a compile error here
    /// rather than silently dropped from the wire.
    fn from(entry: FileEntry) -> Self {
        let FileEntry {
            path,
            size,
            deletes,
            partition_values,
        } = entry;
        if !partition_values.is_empty() {
            return FileEntryWire::WithPartitionValues {
                path,
                size,
                deletes,
                partition_values,
            };
        }
        if deletes.is_empty() {
            FileEntryWire::Legacy(path, size)
        } else {
            FileEntryWire::WithDeletes(path, size, deletes)
        }
    }
}

impl FileEntry {
    pub fn new(path: impl Into<String>, size: u64) -> Self {
        FileEntry {
            path: path.into(),
            size,
            deletes: Vec::new(),
            partition_values: BTreeMap::new(),
        }
    }

    /// Callers must not mix a deletion vector with an Iceberg delete-file reference.
    pub fn with_deletes(path: impl Into<String>, size: u64, deletes: Vec<DeleteMechanism>) -> Self {
        FileEntry {
            path: path.into(),
            size,
            deletes,
            partition_values: BTreeMap::new(),
        }
    }

    pub fn with_partition_values(
        path: impl Into<String>,
        size: u64,
        partition_values: BTreeMap<String, Option<String>>,
    ) -> Self {
        FileEntry {
            path: path.into(),
            size,
            deletes: Vec::new(),
            partition_values,
        }
    }
}

impl From<(String, u64)> for FileEntry {
    fn from((path, size): (String, u64)) -> Self {
        FileEntry::new(path, size)
    }
}

/// Shard-invariant part of a [`ScanSpec`], serialized once as the first UDF argument.
/// It has no `files` field, so the common blob can never carry one.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CommonScanSpec {
    /// Empty means every per-shard file path is already absolute.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub table_root: String,

    /// Empty means all columns on the row-scan and join paths. On the aggregate path
    /// the field is inert, so empty there means "not applicable" (#145).
    pub projection: Vec<ProjectionItem>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub filter: Option<String>,

    /// Not consulted on the join path; see [`JoinSpec::post_join_limit`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<u64>,

    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub order_by: Vec<SortKey>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub aggregates: Option<Vec<AggregatePlan>>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub group_keys: Option<Vec<String>>,

    /// Set only on the single-group `COUNT(DISTINCT col)` fan-out: shards emit
    /// shard-local distinct values and the wrapper counts distinct over the union.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub distinct: bool,

    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub logical_schema: Vec<LogicalField>,

    /// Flattened `schema.name-mapping.default` entries.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub name_mapping: Vec<NameMappingEntry>,

    /// Broadcast side of an inner equi-join, re-scanned by every shard.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub join: Option<JoinSpec>,

    /// Table-level, so a scan with zero active files still knows which logical columns
    /// have no physical counterpart.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub partition_columns: Vec<String>,

    pub storage: ScanStorage,

    #[serde(default = "default_one_usize")]
    pub df_target_partitions: usize,

    #[serde(default = "default_batch_size")]
    pub df_batch_size: usize,

    #[serde(default = "default_one_usize")]
    pub df_threads_per_udf: usize,

    /// Fraction of the net per-instance budget given to the DataFusion memory pool.
    #[serde(default = "default_memory_pool_fraction")]
    pub memory_pool_fraction: f64,

    /// Fixed container/binary RSS overhead (MB) subtracted from the per-instance limit.
    #[serde(default = "default_instance_overhead_mb")]
    pub instance_overhead_mb: u64,

    /// Concurrent connections held warm per host.
    #[serde(default = "default_s3_max_connections")]
    pub s3_max_connections: usize,
}

impl CommonScanSpec {
    pub fn to_json(&self) -> String {
        serde_json::to_string(self).expect("CommonScanSpec serialization is infallible")
    }

    /// Errors never echo the input, which carries credentials.
    pub fn from_json(s: &str) -> Result<Self, String> {
        serde_json::from_str(s).map_err(|e| {
            // serde's Display can quote the input.
            format!(
                "scan common spec deserialization failed ({:?} at line {}, column {})",
                e.classify(),
                e.line(),
                e.column()
            )
        })
    }
}

impl Default for CommonScanSpec {
    /// Test-construction baseline. `s3_max_connections` is the fixture value `8`,
    /// deliberately not serde's field-absent [`DEFAULT_S3_MAX_CONNECTIONS`].
    fn default() -> Self {
        Self {
            table_root: String::new(),
            projection: Vec::new(),
            filter: None,
            limit: None,
            order_by: Vec::new(),
            aggregates: None,
            group_keys: None,
            distinct: false,
            logical_schema: Vec::new(),
            name_mapping: Vec::new(),
            join: None,
            partition_columns: Vec::new(),
            storage: ScanStorage::Inline(StorageBackend::S3(StorageProps::default())),
            df_target_partitions: default_one_usize(),
            df_batch_size: default_batch_size(),
            df_threads_per_udf: default_one_usize(),
            memory_pool_fraction: default_memory_pool_fraction(),
            instance_overhead_mb: default_instance_overhead_mb(),
            s3_max_connections: 8,
        }
    }
}

/// `common` is flattened so a whole-spec JSON and the two-argument wire (common blob
/// plus files array) carry the same keys at the same level.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ScanSpec {
    #[serde(flatten)]
    pub common: CommonScanSpec,

    /// Each `path` is relative to `common.table_root` when it lives under it, else
    /// absolute; `size` lets the scan build `ObjectMeta` without a HEAD request.
    pub files: Vec<FileEntry>,
}

fn default_one_usize() -> usize {
    1
}

fn default_batch_size() -> usize {
    8192
}

fn default_memory_pool_fraction() -> f64 {
    0.6
}

fn default_instance_overhead_mb() -> u64 {
    200
}

/// Defined here, not in `adapter`, so `scan::spec` has no reverse dependency on it.
pub(crate) const DEFAULT_S3_MAX_CONNECTIONS: usize = 16;

fn default_s3_max_connections() -> usize {
    DEFAULT_S3_MAX_CONNECTIONS
}

impl ScanSpec {
    pub fn to_json(&self) -> String {
        serde_json::to_string(self).expect("ScanSpec serialization is infallible")
    }

    /// Production reconstitutes via `from_parts_json`. Errors never echo the input.
    pub fn from_json(s: &str) -> Result<Self, String> {
        serde_json::from_str(s).map_err(|e| {
            // serde's Display can quote the input.
            format!(
                "scan spec deserialization failed ({:?} at line {}, column {})",
                e.classify(),
                e.line(),
                e.column()
            )
        })
    }

    pub fn to_common(&self) -> CommonScanSpec {
        self.common.clone()
    }

    pub fn to_common_json(&self) -> String {
        self.to_common().to_json()
    }

    /// The sole way to reattach `files`, making it the only per-shard field by
    /// construction.
    pub fn from_parts(common: CommonScanSpec, files: Vec<FileEntry>) -> Self {
        Self { common, files }
    }

    /// Errors never echo the inputs (the common blob carries credentials).
    pub fn from_parts_json(common_json: &str, files_json: &str) -> Result<Self, String> {
        let common = CommonScanSpec::from_json(common_json)?;
        let files = Self::files_from_json(files_json)?;
        Ok(Self::from_parts(common, files))
    }

    pub fn files_json(files: &[FileEntry]) -> String {
        serde_json::to_string(files).expect("files list serialization is infallible")
    }

    /// Refuses an entry mixing a deletion vector with an Iceberg delete-file
    /// reference: applying both returns wrong rows, and this is the one gate that
    /// holds for a payload this process did not build.
    pub fn files_from_json(s: &str) -> Result<Vec<FileEntry>, String> {
        let files: Vec<FileEntry> = serde_json::from_str(s).map_err(|e| {
            // serde's Display can quote the input.
            format!(
                "scan files deserialization failed ({:?} at line {}, column {})",
                e.classify(),
                e.line(),
                e.column()
            )
        })?;

        for (index, entry) in files.iter().enumerate() {
            let mut delete_files = false;
            let mut deletion_vectors = false;
            for mechanism in &entry.deletes {
                if mechanism.is_delete_file_reference() {
                    delete_files = true;
                } else {
                    deletion_vectors = true;
                }
            }
            if delete_files && deletion_vectors {
                return Err(format!(
                    "scan files deserialization failed (entry {index} mixes a deletion vector \
                     with an Iceberg delete-file reference; the two are independent delete \
                     mechanisms, so applying both to one data file returns wrong rows)"
                ));
            }
        }

        Ok(files)
    }
}

pub(crate) fn reconstruct_abs_uri(entry_path: &str, table_root: &str) -> String {
    if entry_path.contains("://") {
        return entry_path.to_string();
    }
    let root = table_root.strip_suffix('/').unwrap_or(table_root);
    let rel = entry_path.strip_prefix('/').unwrap_or(entry_path);
    format!("{root}/{rel}")
}

/// The characters `ListingTableUrl::parse` would decode or read as fragment/query.
const URL_PARSE_UNSAFE: &percent_encoding::AsciiSet = &percent_encoding::AsciiSet::EMPTY
    .add(b'%')
    .add(b'#')
    .add(b'?');

/// Escapes what `ListingTableUrl::parse` would otherwise decode.
pub(crate) fn encode_file_path<'p>(
    parts: impl Iterator<Item = object_store::path::PathPart<'p>>,
    len_hint: usize,
) -> String {
    let mut encoded = String::with_capacity(len_hint);
    for (index, part) in parts.enumerate() {
        if index > 0 {
            encoded.push('/');
        }
        encoded.extend(percent_encoding::utf8_percent_encode(
            part.as_ref(),
            URL_PARSE_UNSAFE,
        ));
    }
    encoded
}

#[cfg(test)]
#[path = "spec_tests.rs"]
mod tests;
