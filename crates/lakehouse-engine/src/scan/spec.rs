//! Scan specification types that cross the UDF argument boundary.
//!
//! The adapter splits the spec across TWO VARCHAR UDF arguments: the
//! shard-invariant [`CommonScanSpec`] serialized ONCE per fan-out (argument 0)
//! and the per-shard files JSON array (argument 1). The scan UDF reads both via
//! `ctx.get_string(0)` / `ctx.get_string(1)` and reconstitutes a [`ScanSpec`]
//! through [`ScanSpec::from_parts_json`]. Because [`CommonScanSpec`] has no
//! `files` field, "files is the only per-shard field" is a type-level guarantee.
//!
//! Credentials (`access_key`, `secret_key`) MUST NEVER appear in any error message.
use serde::{Deserialize, Serialize};

/// The kind of aggregate function to compute node-locally as a partial result.
///
/// COUNT(*) maps to `Count` (no column), COUNT(col) maps to `CountCol`.
/// AVG is decomposed into a (partial_sum, partial_count) pair in the scan UDF;
/// the adapter wrapper SQL performs the final division.
///
/// STDDEV/VARIANCE family are decomposed into a (cnt, sum, sum_sq) sufficient-
/// statistics triple; the wrapper reconstructs the population or sample statistic.
///
/// Single-group `COUNT(DISTINCT col)` is NOT an aggregate partial: it is decomposed
/// into a DISTINCT row-scan fan-out (`CommonScanSpec::distinct`) whose per-shard local
/// distinct rows are counted by an outer Exasol-native `COUNT(DISTINCT "V")`, so no
/// `AggKind` variant represents it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum AggKind {
    Count,
    CountCol,
    Sum,
    Min,
    Max,
    Avg,
    /// VAR_POP / VARIANCE_POP — divide final numer by N.
    VarPop,
    /// VAR_SAMP / VARIANCE / VARIANCE_SAMP — divide final numer by N-1.
    VarSamp,
    /// STDDEV_POP — sqrt(VAR_POP).
    StddevPop,
    /// STDDEV / STDDEV_SAMP — sqrt(VAR_SAMP).
    StddevSamp,
}

/// One aggregate function in a pushed-down aggregate plan.
///
/// `column` is `None` for `COUNT(*)` and `Some(col_name)` for all other
/// variants.  The column name matches the projected column name (uppercase).
///
/// `arg_expr` carries a DataFusion SQL fragment (rendered via
/// `vs_expression::render_expression`, the same seam GROUP BY keys use) when the
/// aggregate's argument is a scalar expression rather than a bare column reference —
/// e.g. `SUM(LENGTH(L_COMMENT))`. It is `None` for the bare-column and `COUNT(*)` forms.
/// This is a separate field rather than an overload of `column` so bare-column lookups
/// (e.g. MIN/MAX exact source type) and the pre-existing JSON wire shape are unaffected;
/// when both are absent (`COUNT(*)`) the aggregate has no argument at all. See
/// `specs/_plans/add-count-distinct-and-expression-aggregate-pushdown/vs-adapter/pushdown-planning-expression-aggregate/spec.md`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AggregatePlan {
    pub kind: AggKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub column: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub arg_expr: Option<String>,
}

/// One entry in a scan's projection list.
///
/// Distinguishes a bare source-column reference from an already-rendered scalar
/// expression fragment — mirroring how [`AggregatePlan`] separates a bare
/// `column` from a rendered `arg_expr`, and for the same reason: the two forms
/// look alike as strings but must be spliced into the scan SQL differently. A
/// [`Column`](ProjectionItem::Column) is quoted as an identifier by the scan
/// (applying the VARCHAR cast for JSON-fallback types); an
/// [`Expr`](ProjectionItem::Expr) is spliced into the SELECT list VERBATIM,
/// because it is already valid DataFusion SQL that resolves against the
/// uppercase-aliased inner scan — exactly like `filter` and `arg_expr`.
///
/// # Serde
///
/// Untagged: a [`Column`](ProjectionItem::Column) serializes as a bare JSON
/// string and an [`Expr`](ProjectionItem::Expr) as `{"expr": "..."}`. A bare
/// string therefore deserializes as a `Column`, so payloads and specs that
/// predate this type (whose projection was a plain string array) still load
/// correctly — every legacy entry is a column reference.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ProjectionItem {
    /// A rendered scalar-expression SQL fragment (e.g. `("SCORE" * 2)`) spliced
    /// into the scan SELECT list verbatim.
    Expr { expr: String },
    /// A bare, uppercase source-column identifier (e.g. `SCORE`) quoted as an
    /// identifier by the scan.
    Column(String),
}

impl ProjectionItem {
    /// The identifier this item contributes as its positional EMITS column name.
    ///
    /// Emission is positional, so this string only names the EMITS slot — for an
    /// [`Expr`](ProjectionItem::Expr) it is the rendered fragment itself, which is
    /// an ugly-but-valid quoted EMITS identifier and never a column lookup.
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

/// One ORDER BY sort key in a pushed-down ordered top-N plan.
///
/// `column` is a bare, uppercase source-column identifier. This is a deliberately
/// narrow gate for the per-shard bounded top-N (`TopK`) detection path, not a
/// reflection of what Exasol can send: both `ORDER_BY_COLUMN` and
/// `ORDER_BY_EXPRESSION` are advertised (issue #198), so Exasol may send an
/// expression sort key too — those are parsed separately (`parse_sort_flags` in
/// `adapter/pushdown/topn.rs`) and never construct a `SortKey`, keeping top-N
/// eligibility restricted to bare columns — see
/// `specs/_plans/add-topn-pushdown/decision-log.md` decision [3].
///
/// `ascending` maps directly to Exasol's `orderBy[].isAscending`
/// (`true` = `ASC`, `false` = `DESC`). `nulls_last` maps directly to Exasol's
/// `orderBy[].nullsLast` (`true` = `NULLS LAST`, `false` = `NULLS FIRST`). Both
/// must be rendered explicitly (never left to a side's default NULL ordering) on
/// both the per-shard `ORDER BY` and the outer merge `ORDER BY` so the two sorts
/// induce the same ranking — see decision [7].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SortKey {
    pub column: String,
    pub ascending: bool,
    pub nulls_last: bool,
}

impl SortKey {
    /// Render this key as one SQL `ORDER BY` element:
    /// `"COLUMN" ASC|DESC NULLS FIRST|LAST`.
    ///
    /// Direction and NULL placement are ALWAYS rendered explicitly (never left to
    /// a side's default) so the per-shard bounded sort (scan UDF `build_scan_sql`)
    /// and the outer merge sort (adapter wrapper) induce the same ranking. The
    /// identifier is double-quoted with embedded quotes doubled — the same quoting
    /// the adapter and the scan use for column identifiers, and valid in both
    /// DataFusion SQL (per-shard) and Exasol SQL (merge).
    pub fn render_order_by_element(&self) -> String {
        self.render_ordered(&format!("\"{}\"", self.column.replace('"', "\"\"")))
    }

    /// Render this key's flags onto an already-rendered ordering expression, via the
    /// free [`render_ordered`] seam. `self.column` is deliberately unread: `expr`
    /// carries the ordering target.
    pub fn render_ordered(&self, expr: &str) -> String {
        render_ordered(expr, self.ascending, self.nulls_last)
    }
}

/// Render direction + NULL placement onto an already-rendered ordering expression:
/// `<expr> ASC|DESC NULLS FIRST|LAST`.
///
/// `expr` may be a quoted column reference (the row-scan and per-shard sorts), a
/// positional output ordinal (the grouped-aggregate merge sort, whose output columns
/// are `GK_*`/merged aggregates, not the source names), a table-qualified or merged
/// expression (the join wrapper and the grouped merge), or any other valid ordering
/// expression. Routing every ORDER BY the adapter emits through this ONE
/// direction/NULL seam is what structurally guarantees they agree on direction and
/// NULL placement — the correctness-critical top-N invariant (decision [7]).
///
/// Callers holding a [`SortKey`] reach it through [`SortKey::render_ordered`];
/// callers holding only a parsed flags pair (an expression sort key, which is no
/// bare column and so yields no `SortKey`) call it directly.
pub fn render_ordered(expr: &str, ascending: bool, nulls_last: bool) -> String {
    let direction = if ascending { "ASC" } else { "DESC" };
    let nulls = if nulls_last {
        "NULLS LAST"
    } else {
        "NULLS FIRST"
    };
    format!("{expr} {direction} {nulls}")
}

/// Render a comma-separated `ORDER BY` element list from sort keys, in order —
/// e.g. `"L_EXTENDEDPRICE" DESC NULLS LAST, "L_ORDERKEY" ASC NULLS FIRST`.
///
/// Returns the element list WITHOUT the leading `ORDER BY ` keyword (callers splice
/// it after their own `ORDER BY`). This is the SINGLE rendering seam shared by the
/// adapter's outer merge `ORDER BY` and the scan UDF's per-shard `ORDER BY`: routing
/// both through this function is what structurally guarantees the two sorts agree on
/// direction and NULL placement (the correctness-critical top-N invariant). An empty
/// key slice yields an empty string; callers must guard on that before emitting a
/// bare `ORDER BY`.
pub fn render_order_by_clause(keys: &[SortKey]) -> String {
    keys.iter()
        .map(SortKey::render_order_by_element)
        .collect::<Vec<_>>()
        .join(", ")
}

/// Storage connection properties (S3-compatible / MinIO).
/// Fields are plain Strings so serde handles them uniformly.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageProps {
    pub endpoint: String,
    pub region: String,
    pub access_key: String,
    pub secret_key: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_token: Option<String>,
    /// Enable HTTP (MinIO local dev typically uses HTTP, not HTTPS).
    #[serde(default)]
    pub allow_http: bool,
    /// Use path-style access (required for MinIO).
    #[serde(default = "default_true")]
    pub path_style: bool,
}

fn default_true() -> bool {
    true
}

impl Default for StorageProps {
    /// Mirrors serde's field-absent defaults: empty connection fields, no session
    /// token, HTTPS (`allow_http` false), and path-style access ON (`default_true`).
    /// So `StorageProps::default()` equals deserializing a `StorageProps` whose
    /// optional fields are all absent — the single source of truth is the same
    /// `default_true` seam serde uses. A placeholder for tests, which override the
    /// connection fields that matter to a given scenario.
    fn default() -> Self {
        Self {
            endpoint: String::new(),
            region: String::new(),
            access_key: String::new(),
            secret_key: String::new(),
            session_token: None,
            allow_http: false,
            path_style: default_true(),
        }
    }
}

impl StorageProps {
    /// The non-empty secret values (access key, secret key, session token).
    ///
    /// Used for value-based error redaction: any error string containing one of
    /// these literal values has it stripped before the error is surfaced.
    pub fn secret_values(&self) -> Vec<&str> {
        let mut secrets = Vec::new();
        for candidate in [self.access_key.as_str(), self.secret_key.as_str()] {
            if !candidate.is_empty() {
                secrets.push(candidate);
            }
        }
        if let Some(token) = self.session_token.as_deref()
            && !token.is_empty()
        {
            secrets.push(token);
        }
        secrets
    }
}

/// Iceberg REST catalog connection properties.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CatalogProps {
    pub uri: String,
    pub warehouse: String,
    /// Fully-qualified table identifier: "<namespace>.<table>".
    pub table: String,
}

/// One field in the logical schema carried by `ScanSpec::logical_schema`.
///
/// The `arrow_type` is a compact string tag produced by
/// `types::mapping::arrow_type_to_tag` and parsed back by
/// `types::mapping::arrow_type_from_tag`. Using a string tag rather than a
/// serialized `DataType` keeps the field credential-free and JSON-portable.
///
/// Supported tags:
/// - Primitives: `"bool"`, `"int32"`, `"int64"`, `"float32"`, `"float64"`,
///   `"utf8"`, `"date32"`
/// - Timestamps: `"timestamp_us"`, `"timestamp_ns"`,
///   `"timestamptz_us"`, `"timestamptz_ns"`
/// - Decimal: `"decimal128(p,s)"` (e.g. `"decimal128(18,4)"`)
///
/// # `initial_default`
///
/// Carries the field's Iceberg `initial-default` (the value an absent field
/// reads for rows written before the field existed — Iceberg column-projection
/// rule 3), encoded as the RAW primitive scalar in plain text and reconstructed
/// on the scan side against this same `arrow_type` tag. The encoding is
/// per-tag; the scan-side reconstruction dispatches on `arrow_type`:
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
/// The raw integer (days / micros / nanos) and the `i128` mantissa are stored
/// directly — NOT Iceberg's canonical single-value strings — so the scan side
/// reconstructs a `ScalarValue` against the Arrow tag with no second
/// temporal/decimal parse. A default is present ONLY for a field whose
/// `PrimitiveType` maps to one of the first-class tags above; a non-primitive
/// (struct/list/map) default, and a primitive that only reaches the
/// JSON-fallback `"utf8"` path (`uuid`/`time`/`fixed`/`binary`/oversized
/// `decimal`), both encode nothing (`None`) and fall through to NULL /
/// required-error downstream. The encoded text is a bare scalar value, so it is
/// inherently credential-free.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogicalField {
    /// Iceberg field-id for this column.
    pub field_id: i32,
    /// Current logical name (from the Iceberg schema at query time).
    pub name: String,
    /// Compact Arrow type tag (see struct doc for the tag vocabulary).
    pub arrow_type: String,
    /// Whether the column is nullable (`optional` in Iceberg terms).
    pub nullable: bool,
    /// Encoded Iceberg `initial-default` for this field (see struct doc for the
    /// per-tag encoding). `None` (the default) when the field has no
    /// `initial-default`, the default is non-primitive, or the primitive type
    /// only reaches the JSON-fallback path. Absent from JSON when `None`, so
    /// every spec authored before this field existed deserializes unchanged.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub initial_default: Option<String>,
}

/// One flattened, top-level entry of the Iceberg `schema.name-mapping.default`
/// table property: a physical column name and the Iceberg field-id it maps to
/// for data files written without an embedded `PARQUET:field_id`.
///
/// This is the FLAT representation the scan-side resolver looks up by physical
/// name: the VS planning layer parses the property's nested `NameMapping` JSON
/// once (via the `iceberg` crate's own deserializer) and flattens only the
/// TOP-LEVEL entries into this shape. Nested `fields` entries (struct/map/list
/// children) are deliberately never parsed or represented here — out of scope
/// for this phase (issue #28); see `specs/_plans/change-name-mapping-fallback/plan.md`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NameMappingEntry {
    /// The physical (on-disk Parquet) column name.
    pub name: String,
    /// The Iceberg field-id this physical name maps to.
    pub field_id: i32,
}

/// The kind of join to execute node-locally in the scan UDF.
///
/// This phase supports only `Inner` (inner equi-join); the adapter declines and
/// falls through for every other join shape, so no other variant is ever produced.
/// The lowercase serde tag mirrors [`AggKind`], keeping the wire form compact.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum JoinType {
    /// INNER JOIN — the only broadcast-join shape this phase pushes down.
    Inner,
}

/// The broadcast (small/dimension) side of a pushed-down inner equi-join.
///
/// This block is SHARD-INVARIANT: the dimension side's FULL file list is resolved
/// once in the VS planning layer and carried once in the [`CommonScanSpec`] (the
/// UDF's first argument), so every shard's UDF invocation re-scans the SAME full
/// dimension file list and joins it against that shard's fact-file subset. It is
/// therefore never part of the per-shard files argument, and the fact side's
/// per-shard `files` and this block's `files` never collide.
///
/// The `condition` is a rendered DataFusion SQL expression string (produced by the
/// VS join-condition renderer), spliced into the scan's inner equi-join VERBATIM —
/// the same treatment [`ProjectionItem::Expr`] and `filter` receive.
///
/// Credentials never appear here: the dimension side is referenced by file list,
/// not materialized, and storage credentials live once in [`StorageProps`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JoinSpec {
    /// The dimension Iceberg table's root location, used to reconstruct absolute
    /// file paths from relative `files` entries (empty = every path is absolute).
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub table_root: String,

    /// The dimension side's FULL file list as [`FileEntry`] values. Carried once
    /// (shard-invariant) and re-scanned by every shard's DataFusion session. Each
    /// entry may carry its own positional-delete files, which the scan applies to
    /// the dimension registration exactly as the raw-scan path does — a dimension
    /// table with merge-on-read deletes joins on its post-delete rows.
    pub files: Vec<FileEntry>,

    /// Full logical schema of the dimension Iceberg table at query time. Absent
    /// (empty) falls back to first-file schema inference, as on the raw-scan path.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub logical_schema: Vec<LogicalField>,

    /// Flattened `schema.name-mapping.default` entries for the dimension table,
    /// resolved once in the VS alongside `logical_schema`. Empty (the default)
    /// means no name-mapping property is present, or it was not consulted.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub name_mapping: Vec<NameMappingEntry>,

    /// The join kind. This phase only ever carries [`JoinType::Inner`].
    pub join_type: JoinType,

    /// Rendered DataFusion SQL join condition, spliced into the equi-join verbatim.
    pub condition: String,
}

/// The Iceberg delete mechanism a [`DeleteFileRef`] belongs to.
///
/// Carries just enough to tell an actually-applicable Parquet positional
/// delete apart from every delete mechanism this engine does not apply. Plan
/// time (the adapter) is the authoritative gate that fails loud on anything
/// other than [`PositionDeletes`](DeleteFileContentType::PositionDeletes)
/// BEFORE a file reaches this spec; the other variants exist so the scan
/// reader's read-time backstop can still reject a delete file cleanly (rather
/// than panic or apply it incorrectly) if one ever slips through — see
/// ADR-085 ("Minimal Scan-Spec Surface for Delete Support") in
/// `specs/decision-log.md`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeleteFileContentType {
    /// A Parquet positional-delete file (`file_path`/`pos` columns) — the
    /// only delete mechanism this engine applies.
    PositionDeletes,
    /// An Iceberg equality-delete file. Never applied by this engine.
    EqualityDeletes,
    /// A Puffin-encoded v3 deletion vector. Never applied by this engine.
    PuffinDeletionVector,
}

/// Reference to one Iceberg delete file associated with a [`FileEntry`].
///
/// Deliberately minimal — `path`, `size`, and `content_type` only. Per the
/// "Minimal ScanSpec surface" decision this carries NO serialized Iceberg
/// `Schema` or `BoundPredicate`: the scan reader already has the logical
/// schema (`ScanSpec::logical_schema`) and does its own predicate pushdown, so
/// a delete ref needs nothing beyond what it takes to open the file (`path`,
/// `size`, matching how a [`FileEntry`] itself carries no more than that) and
/// to reject it cleanly if unsupported (`content_type`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeleteFileRef {
    /// Path to the delete file, relative to `ScanSpec::table_root` when
    /// non-empty and the file lives under it, otherwise an absolute URI —
    /// exactly like [`FileEntry::path`].
    pub path: String,
    /// Byte size, used the same way as [`FileEntry::size`]: to build the
    /// delete file's `ObjectMeta` without an object-store HEAD.
    pub size: u64,
    /// The delete mechanism this file encodes.
    pub content_type: DeleteFileContentType,
}

/// One per-shard scanned-file entry: a data file's path and byte size, plus
/// the positional-delete files (if any) that must be applied when reading it.
///
/// # Chosen shape: struct-per-file with an untagged legacy fallback
///
/// The Rust-level API is a plain struct (`path`, `size`, `deletes`) so callers
/// never pattern-match a bare tuple to reach the delete list. On the wire,
/// `#[serde(from/into = "FileEntryWire")]` routes (de)serialization through
/// the private [`FileEntryWire`] enum, mirroring how [`ProjectionItem`]
/// already gives a bare-string legacy payload a typed fallback in this same
/// module:
/// - A legacy `[path, size]` 2-tuple (every entry written before
///   positional-delete support) deserializes with an empty `deletes` list.
/// - `[path, size, deletes]` (a 3-tuple) deserializes with `deletes` intact.
/// - Serialization always picks the SHORTEST form for the value at hand: the
///   compact 2-tuple when `deletes` is empty (keeping the still-common
///   delete-free case exactly as small on the wire as before this field
///   existed) and the 3-tuple only when there is something to carry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(from = "FileEntryWire", into = "FileEntryWire")]
pub struct FileEntry {
    /// Path to the data file, relative to `ScanSpec::table_root` when
    /// non-empty and the file lives under it, otherwise an absolute URI (S3
    /// or s3a).
    pub path: String,
    /// Byte size, used to build the file's `ObjectMeta` without an
    /// object-store HEAD.
    pub size: u64,
    /// Positional-delete files that must be applied when reading this data
    /// file. Empty (the default for legacy and delete-free entries) means the
    /// file is read as-is.
    pub deletes: Vec<DeleteFileRef>,
}

/// Wire form of [`FileEntry`] — see that struct's doc for why this shape
/// exists. Not part of the public API; [`FileEntry`] is the only type callers
/// construct or match on.
#[derive(Serialize, Deserialize)]
#[serde(untagged)]
enum FileEntryWire {
    Legacy(String, u64),
    WithDeletes(String, u64, Vec<DeleteFileRef>),
}

impl From<FileEntryWire> for FileEntry {
    fn from(wire: FileEntryWire) -> Self {
        match wire {
            FileEntryWire::Legacy(path, size) => FileEntry {
                path,
                size,
                deletes: Vec::new(),
            },
            FileEntryWire::WithDeletes(path, size, deletes) => FileEntry {
                path,
                size,
                deletes,
            },
        }
    }
}

impl From<FileEntry> for FileEntryWire {
    fn from(entry: FileEntry) -> Self {
        if entry.deletes.is_empty() {
            FileEntryWire::Legacy(entry.path, entry.size)
        } else {
            FileEntryWire::WithDeletes(entry.path, entry.size, entry.deletes)
        }
    }
}

impl FileEntry {
    /// A data-file entry with no associated delete files — the common case,
    /// and the only shape a legacy (pre-delete-support) entry can take.
    pub fn new(path: impl Into<String>, size: u64) -> Self {
        FileEntry {
            path: path.into(),
            size,
            deletes: Vec::new(),
        }
    }

    /// A data-file entry with its associated positional-delete file refs.
    pub fn with_deletes(path: impl Into<String>, size: u64, deletes: Vec<DeleteFileRef>) -> Self {
        FileEntry {
            path: path.into(),
            size,
            deletes,
        }
    }
}

/// Eases migrating existing `(path, size)` call sites to [`FileEntry`]: every
/// such tuple is a delete-free entry.
impl From<(String, u64)> for FileEntry {
    fn from((path, size): (String, u64)) -> Self {
        FileEntry::new(path, size)
    }
}

/// The shard-INVARIANT portion of a scan specification.
///
/// Holds every field the scan UDF reads that is identical across all shards of a
/// single query fan-out — i.e. everything EXCEPT the per-shard `files` list (and
/// excluding the adapter-side-only `catalog`). The adapter serializes this ONCE
/// as the first UDF argument; only the per-shard files list varies per invocation.
///
/// Because this struct structurally has no `files` field, "files is the only
/// per-shard field" is a type-level guarantee: the common blob can never carry a
/// stray `files` value.
///
/// Credentials (`storage.access_key`, `storage.secret_key`) MUST NEVER appear in
/// any error message produced by `from_json`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CommonScanSpec {
    /// The Iceberg table's root location (`table.metadata().location()`), used
    /// to reconstruct absolute file paths from per-shard relative paths.
    ///
    /// An empty string (the default) means every per-shard file path is already
    /// absolute — either a legacy payload that predates this field, or a file
    /// that does not live under the table root.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub table_root: String,

    /// Projected columns in order, for the row-scan and join paths, where an empty
    /// value means "all columns" (no projection push). Each entry is either a bare
    /// column reference or a rendered scalar expression (see [`ProjectionItem`]).
    ///
    /// NOT consulted on the aggregate-dispatch path (single-group or grouped
    /// aggregate): that scan builds its query from `aggregates`/`group_keys` and
    /// DataFusion's projection pushdown prunes the physical Parquet read, so the
    /// field is inert there and the adapter leaves it empty. An empty value on an
    /// aggregate spec therefore means "not applicable", NOT "all columns" (#145).
    pub projection: Vec<ProjectionItem>,

    /// DataFusion SQL WHERE predicate fragment, already translated.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub filter: Option<String>,

    /// Row limit. None means no LIMIT pushdown.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<u64>,

    /// ORDER BY sort keys for a pushed-down ordered top-N scan, in order. Empty
    /// (the default) means no ordering pushdown; absent from JSON when empty so
    /// specs that predate this field are unaffected (backward-compatible).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub order_by: Vec<SortKey>,

    /// Ordered list of aggregate functions to compute as node-local partial results.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub aggregates: Option<Vec<AggregatePlan>>,

    /// Rendered DataFusion SQL fragments for each GROUP BY key, in order.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub group_keys: Option<Vec<String>>,

    /// Apply DataFusion `.distinct()` to the row-scan projection. Set only on the
    /// single-group `COUNT(DISTINCT col)` fan-out (see
    /// `vs-adapter/pushdown-planning-count-distinct`): each shard streams one row per
    /// shard-local distinct projected value through the `emit_batch` path, and the
    /// outer wrapper runs a native `COUNT(DISTINCT "V")` over the union. Inert (and
    /// absent from JSON) on every other path — `false` (the default) means a plain
    /// row scan; a legacy spec lacking the field deserializes to `false`.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub distinct: bool,

    /// Declared Exasol EMITS type string for each output column.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub emit_exa_types: Vec<String>,

    /// Full logical schema of the Iceberg table at query time.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub logical_schema: Vec<LogicalField>,

    /// Flattened `schema.name-mapping.default` entries, resolved once in the VS
    /// alongside `logical_schema`. Empty (the default) means no name-mapping
    /// property is present on the table, or it was not consulted.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub name_mapping: Vec<NameMappingEntry>,

    /// Broadcast (dimension) side of a pushed-down inner equi-join. `None` (the
    /// default) means a plain single-table scan; absent from JSON when `None` so
    /// every pre-existing non-join spec deserializes unchanged (backward-compatible).
    /// Shard-invariant, hence part of the common blob: the dimension side is
    /// resolved once and re-scanned by every shard — see [`JoinSpec`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub join: Option<JoinSpec>,

    pub storage: StorageProps,

    /// DataFusion `target_partitions` for this scan instance.
    #[serde(default = "default_one_usize")]
    pub df_target_partitions: usize,

    /// DataFusion `batch_size` (rows per Arrow RecordBatch) for this scan instance.
    #[serde(default = "default_batch_size")]
    pub df_batch_size: usize,

    /// Number of Tokio worker threads for the scan runtime.
    #[serde(default = "default_one_usize")]
    pub df_threads_per_udf: usize,

    /// Fraction of the net per-instance budget given to the DataFusion memory pool.
    #[serde(default = "default_memory_pool_fraction")]
    pub memory_pool_fraction: f64,

    /// Fixed container/binary RSS overhead (MB) subtracted from the per-instance limit.
    #[serde(default = "default_instance_overhead_mb")]
    pub instance_overhead_mb: u64,

    /// Connection-concurrency budget for the scan's S3-compatible object store
    /// (number of concurrent connections held warm per host).
    #[serde(default = "default_s3_max_connections")]
    pub s3_max_connections: usize,
}

impl CommonScanSpec {
    /// Serialize the shard-invariant common blob to a JSON string.
    ///
    /// The output never contains a `files` key (structurally impossible) nor a
    /// `catalog` key.
    pub fn to_json(&self) -> String {
        serde_json::to_string(self).expect("CommonScanSpec serialization is infallible")
    }

    /// Deserialize a common blob from a JSON string received from `ctx.get(0)`.
    ///
    /// Returns an error that does NOT include the raw input (which carries
    /// credentials).
    pub fn from_json(s: &str) -> Result<Self, String> {
        serde_json::from_str(s).map_err(|e| {
            // Do not echo `s` — it contains credentials. Build the message from the
            // serde error's structural fields only; its Display can quote the input.
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
    /// The shard-invariant baseline: no pushdown (empty projection/filter/order-by,
    /// no aggregate/group/join), a placeholder [`StorageProps`], and every tuning
    /// knob at its shared test-fixture value. Its purpose is construction ergonomics
    /// for tests, which spread `..Default::default()` and override ONLY the fields a
    /// given scenario exercises, rather than respelling all shard-invariant fields.
    ///
    /// The five `df_*`/`memory_pool_fraction`/`instance_overhead_mb` knobs reuse the
    /// same `default_*` seams serde applies to an absent JSON field, so they cannot
    /// drift from the wire defaults. `s3_max_connections` is the one deliberate
    /// exception: it is the fixture convention (`8`) shared with the golden dispatch
    /// baselines, NOT serde's field-absent fallback [`DEFAULT_S3_MAX_CONNECTIONS`]
    /// (`16`) — `Default` is a test-construction aid, not a wire-compat contract.
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
            emit_exa_types: Vec::new(),
            logical_schema: Vec::new(),
            name_mapping: Vec::new(),
            join: None,
            storage: StorageProps::default(),
            df_target_partitions: default_one_usize(),
            df_batch_size: default_batch_size(),
            df_threads_per_udf: default_one_usize(),
            memory_pool_fraction: default_memory_pool_fraction(),
            instance_overhead_mb: default_instance_overhead_mb(),
            s3_max_connections: 8,
        }
    }
}

/// The scan specification passed from the adapter to the scan SET UDF.
///
/// Holds the shard-invariant [`CommonScanSpec`] (every field identical across all
/// shards of a fan-out) plus the per-shard `files` list. `common` is embedded via
/// `#[serde(flatten)]` so the serialized JSON stays FLAT — a whole-`ScanSpec` JSON
/// and the two-argument wire (common blob + files array) carry the same keys at the
/// same level, and the common-blob and files-list serializations are byte-identical
/// whether produced from a `ScanSpec` or a standalone `CommonScanSpec`. This is the
/// single declaration of the shard-invariant fields (see [`CommonScanSpec`]); reads
/// reach them through `spec.common.<field>`.
///
/// Note: flatten serializes `common`'s fields first and `files` last, so a
/// whole-`ScanSpec` `to_json()` places `files` at the END rather than second. The
/// two-argument wire (`to_common_json` + `files_json`) — the ONLY form production
/// reconstitutes from, via `from_parts_json` — is unaffected, because each part is
/// serialized independently.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ScanSpec {
    /// The shard-invariant portion of the spec — everything except `files`. Embedded
    /// flat on the wire (see the struct doc); the sole declaration of these fields.
    #[serde(flatten)]
    pub common: CommonScanSpec,

    /// Explicit list of assigned Parquet files, each carrying its byte size
    /// and its associated positional-delete file refs (if any). `path` is
    /// relative to `common.table_root` when non-empty and the file lives under it,
    /// otherwise an absolute URI (S3 or s3a). The scan UDF registers ONLY
    /// these files — no catalog discovery — and uses `size` to build each
    /// file's `ObjectMeta` without an object-store HEAD. See [`FileEntry`]
    /// for the wire shape and its backward-compatible legacy fallback.
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

/// Built-in fallback connection-concurrency budget: used both as the serde
/// default for [`CommonScanSpec::s3_max_connections`] / [`ScanSpec::s3_max_connections`]
/// when the field is absent from JSON, and by the adapter's AUTO derivation
/// (`resolve_s3_max_connections`) when `nr_of_cores` is `0` (unknown). Defined
/// here rather than in `adapter` so `scan::spec` — the lower-level module the
/// adapter already depends on — has no reverse dependency on `adapter`.
pub(crate) const DEFAULT_S3_MAX_CONNECTIONS: usize = 16;

/// Conservative built-in default for [`CommonScanSpec::s3_max_connections`] /
/// [`ScanSpec::s3_max_connections`] when the field is absent from JSON.
fn default_s3_max_connections() -> usize {
    DEFAULT_S3_MAX_CONNECTIONS
}

impl ScanSpec {
    /// Serialize to a JSON string suitable for `Value::String`.
    pub fn to_json(&self) -> String {
        serde_json::to_string(self).expect("ScanSpec serialization is infallible")
    }

    /// Deserialize a whole `ScanSpec` from JSON; used by tests and as the
    /// pre-split equivalence baseline (production reconstitutes via `from_parts_json`).
    /// Returns an error that does NOT include any credential values.
    pub fn from_json(s: &str) -> Result<Self, String> {
        serde_json::from_str(s).map_err(|e| {
            // Do not echo `s` — it contains credentials. Build the message from the
            // serde error's structural fields only; its Display can quote the input.
            format!(
                "scan spec deserialization failed ({:?} at line {}, column {})",
                e.classify(),
                e.line(),
                e.column()
            )
        })
    }

    /// Extract the shard-invariant portion of this spec (everything except `files`).
    pub fn to_common(&self) -> CommonScanSpec {
        self.common.clone()
    }

    /// Serialize the shard-invariant common blob once (the UDF's first argument).
    ///
    /// The output never contains a `files` key nor a `catalog` key.
    pub fn to_common_json(&self) -> String {
        self.to_common().to_json()
    }

    /// Reconstitute a full `ScanSpec` from a shard-invariant common spec and a
    /// per-shard files list. This is the SOLE way to reattach `files`, which makes
    /// `files` the only per-shard field by construction.
    pub fn from_parts(common: CommonScanSpec, files: Vec<FileEntry>) -> Self {
        Self { common, files }
    }

    /// Reconstitute a full `ScanSpec` from the two UDF arguments: the common blob
    /// JSON (`ctx.get(0)`) and the per-shard files JSON (`ctx.get(1)`).
    ///
    /// Errors NEVER include the raw inputs (the common blob carries credentials).
    pub fn from_parts_json(common_json: &str, files_json: &str) -> Result<Self, String> {
        let common = CommonScanSpec::from_json(common_json)?;
        let files = Self::files_from_json(files_json)?;
        Ok(Self::from_parts(common, files))
    }

    /// Serialize a per-shard files list to the JSON array carried in the UDF's
    /// second argument. Each delete-free entry is a compact `[path, size]`
    /// 2-tuple; an entry carrying positional-delete refs is a `[path, size,
    /// deletes]` 3-tuple (see [`FileEntry`]). Paired with `files_from_json`.
    pub fn files_json(files: &[FileEntry]) -> String {
        serde_json::to_string(files).expect("files list serialization is infallible")
    }

    /// Deserialize a per-shard files list from the UDF's second argument.
    ///
    /// Accepts both the legacy `[path, size]` wire form (reconstituted with an
    /// empty delete list) and the current `[path, size, deletes]` form — see
    /// [`FileEntry`]. Returns an error that does NOT include the raw input.
    pub fn files_from_json(s: &str) -> Result<Vec<FileEntry>, String> {
        serde_json::from_str(s).map_err(|e| {
            // Do not echo `s`. A data error (e.g. a bare-string entry where a
            // [path, size] tuple is expected) can quote the input in `e`'s Display,
            // so build the message from structural fields only.
            format!(
                "scan files deserialization failed ({:?} at line {}, column {})",
                e.classify(),
                e.line(),
                e.column()
            )
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_spec() -> ScanSpec {
        ScanSpec {
            common: CommonScanSpec {
                table_root: "s3://warehouse/db/table".into(),
                projection: vec!["id".into(), "name".into()],
                filter: Some("(\"ID\" > 10)".into()),
                limit: Some(100),
                storage: StorageProps {
                    endpoint: "http://minio:9000".into(),
                    region: "us-east-1".into(),
                    access_key: "minioadmin".into(),
                    secret_key: "minioadmin".into(),
                    allow_http: true,
                    ..Default::default()
                },
                ..Default::default()
            },
            files: vec![
                FileEntry::new("data/part-00000.parquet", 1024),
                FileEntry::new("data/part-00001.parquet", 2048),
            ],
        }
    }

    /// `CommonScanSpec::default()` — the shared test-construction baseline that
    /// `..Default::default()` spreads across the test suite fill in — must track
    /// serde's field-absent defaults for every tuning knob that reuses a `default_*`
    /// seam, so the two default sources cannot silently drift. `s3_max_connections`
    /// is the one deliberate exception (the fixture convention `8`, not the serde
    /// field-absent fallback `DEFAULT_S3_MAX_CONNECTIONS` = `16`); this pins that
    /// intent so a change to either side is a conscious edit, not an accident.
    #[test]
    fn default_matches_serde_absent_except_s3_max_connections() {
        // A common blob whose every optional/tuning field is absent from JSON
        // (only the two non-defaulted fields, `projection` and `storage`, present).
        let minimal = r#"{"projection":[],"storage":{"endpoint":"","region":"","access_key":"","secret_key":""}}"#;
        let from_absent = CommonScanSpec::from_json(minimal).unwrap();
        let d = CommonScanSpec::default();

        // The knobs Default shares with serde agree field-for-field.
        assert_eq!(d.table_root, from_absent.table_root);
        assert_eq!(d.df_target_partitions, from_absent.df_target_partitions);
        assert_eq!(d.df_batch_size, from_absent.df_batch_size);
        assert_eq!(d.df_threads_per_udf, from_absent.df_threads_per_udf);
        assert_eq!(d.memory_pool_fraction, from_absent.memory_pool_fraction);
        assert_eq!(d.instance_overhead_mb, from_absent.instance_overhead_mb);
        assert_eq!(d.storage.path_style, from_absent.storage.path_style);
        assert!(!d.storage.allow_http && !from_absent.storage.allow_http);

        // The one deliberate divergence: Default is the fixture value, serde's
        // field-absent fallback is the conservative wire default.
        assert_eq!(d.s3_max_connections, 8);
        assert_eq!(from_absent.s3_max_connections, DEFAULT_S3_MAX_CONNECTIONS);
    }

    /// Scenario (D.2): Scan-spec round-trips through Value boundary.
    /// serialize → Value::String → deserialize equals original;
    /// credentials survive round-trip but never appear in error text on malformed input.
    #[test]
    fn scan_spec_round_trips_through_value_boundary() {
        let spec = sample_spec();

        // Serialize to JSON (→ the Value::String payload that crosses the UDF boundary).
        let json = spec.to_json();
        // The JSON must be valid UTF-8 string (Value::String is a Rust String).
        let _value_string: String = json.clone(); // satisfies Value::String ownership model.

        // The wire form is a compact array of `[path, size]` 2-tuples.
        assert!(
            json.contains(
                r#""files":[["data/part-00000.parquet",1024],["data/part-00001.parquet",2048]]"#
            ),
            "files must serialize as compact [path,size] 2-tuples: {json}"
        );

        // Deserialize back: must equal original.
        let back = ScanSpec::from_json(&json).unwrap();
        assert_eq!(back.files.len(), 2);
        assert_eq!(
            back.files,
            vec![
                FileEntry::new("data/part-00000.parquet", 1024),
                FileEntry::new("data/part-00001.parquet", 2048),
            ]
        );
        assert_eq!(back.common.table_root, "s3://warehouse/db/table");
        assert_eq!(back.common.projection, vec!["id", "name"]);
        assert_eq!(back.common.filter.as_deref(), Some("(\"ID\" > 10)"));
        assert_eq!(back.common.limit, Some(100));

        // Credentials survive the round-trip (they must reach the scan UDF).
        assert_eq!(back.common.storage.endpoint, "http://minio:9000");
        assert_eq!(back.common.storage.access_key, "minioadmin");
        assert_eq!(back.common.storage.secret_key, "minioadmin");
        assert!(back.common.storage.path_style);
        assert!(back.common.storage.allow_http);
    }

    #[test]
    fn optional_fields_omitted_when_none() {
        let mut spec = sample_spec();
        spec.common.filter = None;
        spec.common.limit = None;
        spec.common.storage.session_token = None;
        spec.common.aggregates = None;
        spec.common.group_keys = None;
        let json = spec.to_json();
        assert!(!json.contains("filter"));
        assert!(!json.contains("limit"));
        assert!(!json.contains("session_token"));
        assert!(
            !json.contains("aggregates"),
            "aggregates field must be absent when None: {json}"
        );
        assert!(
            !json.contains("group_keys"),
            "group_keys field must be absent when None: {json}"
        );
    }

    /// `emit_exa_types` round-trips through JSON, is omitted when empty, and a
    /// legacy payload lacking it deserializes to an empty Vec (backward-compatible).
    #[test]
    fn emit_exa_types_round_trips_and_defaults_to_empty() {
        // Empty (default): the field is omitted from serialized JSON.
        let row_spec = sample_spec();
        assert!(row_spec.common.emit_exa_types.is_empty());
        let row_json = row_spec.to_json();
        assert!(
            !row_json.contains("emit_exa_types"),
            "empty emit_exa_types must be absent from JSON: {row_json}"
        );

        // Non-empty: the declared EMITS types survive the round-trip in order.
        let mut spec = sample_spec();
        spec.common.emit_exa_types = vec![
            "DECIMAL(20,0)".to_string(),
            "VARCHAR(2000000)".to_string(),
            "DOUBLE PRECISION".to_string(),
        ];
        let json = spec.to_json();
        assert!(
            json.contains("emit_exa_types"),
            "non-empty emit_exa_types must appear in JSON: {json}"
        );
        let back = ScanSpec::from_json(&json).unwrap();
        assert_eq!(
            back.common.emit_exa_types,
            vec![
                "DECIMAL(20,0)".to_string(),
                "VARCHAR(2000000)".to_string(),
                "DOUBLE PRECISION".to_string()
            ]
        );

        // Legacy payload without the field deserializes to an empty Vec.
        let legacy_json = r#"{
            "files": [["s3://w/f0.parquet", 100]],
            "projection": [],
            "storage": {
                "endpoint": "http://minio:9000",
                "region": "us-east-1",
                "access_key": "k",
                "secret_key": "s"
            }
        }"#;
        let legacy = ScanSpec::from_json(legacy_json).unwrap();
        assert!(
            legacy.common.emit_exa_types.is_empty(),
            "missing emit_exa_types must default to empty (backward-compat)"
        );
    }

    /// Task 4.1: Aggregate plan round-trips through JSON and does not appear in row-scan specs.
    #[test]
    fn aggregate_plan_round_trips_and_absent_from_row_scan() {
        // Row scan: aggregates must be absent.
        let row_spec = sample_spec();
        let row_json = row_spec.to_json();
        assert!(
            !row_json.contains("aggregates"),
            "row-scan spec must not carry aggregates field: {row_json}"
        );

        // Aggregate scan: round-trip with all supported kinds.
        let mut agg_spec = sample_spec();
        agg_spec.common.aggregates = Some(vec![
            AggregatePlan {
                kind: AggKind::Count,
                column: None,
                arg_expr: None,
            },
            AggregatePlan {
                kind: AggKind::CountCol,
                column: Some("ID".into()),
                arg_expr: None,
            },
            AggregatePlan {
                kind: AggKind::Sum,
                column: Some("AMOUNT".into()),
                arg_expr: None,
            },
            AggregatePlan {
                kind: AggKind::Min,
                column: Some("TS".into()),
                arg_expr: None,
            },
            AggregatePlan {
                kind: AggKind::Max,
                column: Some("TS".into()),
                arg_expr: None,
            },
            AggregatePlan {
                kind: AggKind::Avg,
                column: Some("AMOUNT".into()),
                arg_expr: None,
            },
        ]);
        let agg_json = agg_spec.to_json();
        assert!(
            agg_json.contains("aggregates"),
            "aggregate spec must carry the aggregates field: {agg_json}"
        );

        let back = ScanSpec::from_json(&agg_json).unwrap();
        let plans = back
            .common
            .aggregates
            .expect("aggregates must survive round-trip");
        assert_eq!(plans.len(), 6);
        assert_eq!(plans[0].kind, AggKind::Count);
        assert_eq!(plans[0].column, None);
        assert_eq!(plans[1].kind, AggKind::CountCol);
        assert_eq!(plans[1].column.as_deref(), Some("ID"));
        assert_eq!(plans[2].kind, AggKind::Sum);
        assert_eq!(plans[3].kind, AggKind::Min);
        assert_eq!(plans[4].kind, AggKind::Max);
        assert_eq!(plans[5].kind, AggKind::Avg);
        assert_eq!(plans[5].column.as_deref(), Some("AMOUNT"));
    }

    /// Task 1.1: `AggregatePlan.arg_expr` round-trips through JSON, is omitted from the
    /// wire form when `None` (backward-compatible with bare-column plans), and a plan
    /// carrying an expression argument survives the round-trip alongside a bare-column plan.
    #[test]
    fn arg_expr_round_trips_and_omitted_when_none() {
        // A bare-column plan (arg_expr: None) must not carry the key at all.
        let mut agg_spec = sample_spec();
        agg_spec.common.aggregates = Some(vec![AggregatePlan {
            kind: AggKind::Sum,
            column: Some("AMOUNT".into()),
            arg_expr: None,
        }]);
        let bare_json = agg_spec.to_json();
        assert!(
            !bare_json.contains("arg_expr"),
            "arg_expr must be absent when None: {bare_json}"
        );
        let back = ScanSpec::from_json(&bare_json).unwrap();
        assert_eq!(back.common.aggregates.unwrap()[0].arg_expr, None);

        // An expression-argument plan carries the rendered SQL fragment and round-trips.
        let mut expr_spec = sample_spec();
        expr_spec.common.aggregates = Some(vec![
            AggregatePlan {
                kind: AggKind::Sum,
                column: None,
                arg_expr: Some("LENGTH(\"L_COMMENT\")".into()),
            },
            AggregatePlan {
                kind: AggKind::CountCol,
                column: Some("L_SHIPMODE".into()),
                arg_expr: None,
            },
        ]);
        let expr_json = expr_spec.to_json();
        assert!(
            expr_json.contains("arg_expr"),
            "non-empty arg_expr must appear in JSON: {expr_json}"
        );

        let back = ScanSpec::from_json(&expr_json).unwrap();
        let plans = back
            .common
            .aggregates
            .expect("aggregates must survive round-trip");
        assert_eq!(plans.len(), 2);
        assert_eq!(plans[0].kind, AggKind::Sum);
        assert_eq!(plans[0].column, None);
        assert_eq!(plans[0].arg_expr.as_deref(), Some("LENGTH(\"L_COMMENT\")"));
        assert_eq!(plans[1].kind, AggKind::CountCol);
        assert_eq!(plans[1].arg_expr, None);

        // A legacy aggregate payload (predating arg_expr) deserializes with it defaulting
        // to None — bare-column plans serialized before this field existed still parse.
        let legacy_json = r#"{
            "files": [["s3://w/f0.parquet", 100]],
            "projection": [],
            "aggregates": [{"kind": "sum", "column": "AMOUNT"}],
            "storage": {
                "endpoint": "http://minio:9000",
                "region": "us-east-1",
                "access_key": "k",
                "secret_key": "s"
            }
        }"#;
        let legacy = ScanSpec::from_json(legacy_json).unwrap();
        let legacy_plans = legacy
            .common
            .aggregates
            .expect("legacy aggregates must parse");
        assert_eq!(
            legacy_plans[0].arg_expr, None,
            "missing arg_expr must default to None (backward-compat)"
        );
    }

    /// The free [`render_ordered`] IS the direction/NULL seam, and
    /// [`SortKey::render_ordered`] is a pure delegator to it: the two agree on every
    /// flag combination, and the bare-column element list still renders exactly as
    /// before. A second copy of this formatting is what the seam exists to prevent.
    #[test]
    fn render_ordered_free_fn_and_method_are_one_implementation() {
        for (ascending, nulls_last, expected_suffix) in [
            (true, true, "ASC NULLS LAST"),
            (true, false, "ASC NULLS FIRST"),
            (false, true, "DESC NULLS LAST"),
            (false, false, "DESC NULLS FIRST"),
        ] {
            let key = SortKey {
                column: "IGNORED".into(),
                ascending,
                nulls_last,
            };
            let expr = r#"ABS("C_PRICE")"#;
            assert_eq!(
                render_ordered(expr, ascending, nulls_last),
                format!("{expr} {expected_suffix}"),
                "free render_ordered must append direction + NULL placement"
            );
            assert_eq!(
                key.render_ordered(expr),
                render_ordered(expr, ascending, nulls_last),
                "the method must delegate to the free function, not re-implement it"
            );
        }

        // Regression: the bare-column element list is byte-identical to before.
        assert_eq!(
            render_order_by_clause(&[
                SortKey {
                    column: "L_EXTENDEDPRICE".into(),
                    ascending: false,
                    nulls_last: true,
                },
                SortKey {
                    column: "L_ORDERKEY".into(),
                    ascending: true,
                    nulls_last: false,
                },
            ]),
            r#""L_EXTENDEDPRICE" DESC NULLS LAST, "L_ORDERKEY" ASC NULLS FIRST"#
        );
    }

    /// Task B1: `order_by` round-trips through JSON, is omitted from the wire form
    /// when empty (backward-compatible with every pre-existing spec shape), and a
    /// legacy JSON payload with no `order_by` key deserializes to an empty list.
    #[test]
    fn order_by_round_trips_and_defaults_to_empty() {
        // Empty (default): the field is omitted from serialized JSON.
        let row_spec = sample_spec();
        assert!(row_spec.common.order_by.is_empty());
        let row_json = row_spec.to_json();
        assert!(
            !row_json.contains("order_by"),
            "empty order_by must be absent from JSON: {row_json}"
        );

        // Non-empty: sort keys survive the round-trip, in order, with direction
        // and NULL placement intact.
        let mut spec = sample_spec();
        spec.common.order_by = vec![
            SortKey {
                column: "L_EXTENDEDPRICE".to_string(),
                ascending: false,
                nulls_last: true,
            },
            SortKey {
                column: "L_ORDERKEY".to_string(),
                ascending: true,
                nulls_last: false,
            },
        ];
        let json = spec.to_json();
        assert!(
            json.contains("order_by"),
            "non-empty order_by must appear in JSON: {json}"
        );

        let back = ScanSpec::from_json(&json).unwrap();
        assert_eq!(back.common.order_by, spec.common.order_by);
        assert_eq!(back.common.order_by.len(), 2);
        assert_eq!(back.common.order_by[0].column, "L_EXTENDEDPRICE");
        assert!(!back.common.order_by[0].ascending);
        assert!(back.common.order_by[0].nulls_last);
        assert_eq!(back.common.order_by[1].column, "L_ORDERKEY");
        assert!(back.common.order_by[1].ascending);
        assert!(!back.common.order_by[1].nulls_last);

        // Full-spec equality also holds (order_by participates in ScanSpec's PartialEq).
        assert_eq!(back, spec);

        // The split (to_common) / merge (from_parts) path threads order_by through.
        let common = spec.to_common();
        assert_eq!(common.order_by, spec.common.order_by);
        let merged = ScanSpec::from_parts(common, spec.files.clone());
        assert_eq!(merged.common.order_by, spec.common.order_by);

        // A legacy payload without the field deserializes to an empty Vec.
        let legacy_json = r#"{
            "files": [["s3://w/f0.parquet", 100]],
            "projection": [],
            "storage": {
                "endpoint": "http://minio:9000",
                "region": "us-east-1",
                "access_key": "k",
                "secret_key": "s"
            }
        }"#;
        let legacy = ScanSpec::from_json(legacy_json).unwrap();
        assert!(
            legacy.common.order_by.is_empty(),
            "missing order_by must default to empty (backward-compat)"
        );

        // Same for the common blob in isolation.
        let legacy_common_json = r#"{
            "projection": [],
            "storage": {
                "endpoint": "http://minio:9000",
                "region": "us-east-1",
                "access_key": "k",
                "secret_key": "s"
            }
        }"#;
        let legacy_common = CommonScanSpec::from_json(legacy_common_json).unwrap();
        assert!(
            legacy_common.order_by.is_empty(),
            "missing order_by must default to empty on the common blob (backward-compat)"
        );
    }

    /// Task 2.1: group_keys round-trips through JSON and is absent from row-scan specs.
    #[test]
    fn group_keys_round_trips_and_absent_from_row_scan() {
        // Row scan: group_keys must be absent from serialized JSON.
        let row_spec = sample_spec();
        let row_json = row_spec.to_json();
        assert!(
            !row_json.contains("group_keys"),
            "row-scan spec must not carry group_keys field: {row_json}"
        );

        // Grouped scan: round-trip with Some group keys.
        let mut grouped_spec = sample_spec();
        grouped_spec.common.group_keys = Some(vec![
            "\"REGION\"".to_string(),
            "YEAR(\"EVENT_DATE\")".to_string(),
        ]);
        let grouped_json = grouped_spec.to_json();
        assert!(
            grouped_json.contains("group_keys"),
            "grouped spec must carry group_keys field: {grouped_json}"
        );

        let back = ScanSpec::from_json(&grouped_json).unwrap();
        let keys = back
            .common
            .group_keys
            .expect("group_keys must survive round-trip");
        assert_eq!(keys.len(), 2);
        assert_eq!(keys[0], "\"REGION\"");
        assert_eq!(keys[1], "YEAR(\"EVENT_DATE\")");
    }

    #[test]
    fn bad_json_error_does_not_leak_credentials() {
        let garbled =
            r#"{"storage": {"access_key": "SECRET", "secret_key": "TOPSECRET"}, incomplete"#;
        let err = ScanSpec::from_json(garbled).unwrap_err();
        // The error must not echo the raw input (which contains credentials).
        assert!(!err.contains("SECRET"));
        assert!(!err.contains("TOPSECRET"));
        // But it should say something useful.
        assert!(err.contains("scan spec deserialization failed"));
    }

    /// Task 2.2: logical_schema round-trips through JSON (spec WITH the field) and
    /// a legacy spec WITHOUT it deserializes correctly (backward-compatible default).
    #[test]
    fn logical_schema_round_trips_and_defaults_to_empty() {
        // A spec with a populated logical_schema.
        let mut spec = sample_spec();
        spec.common.logical_schema = vec![
            LogicalField {
                field_id: 1,
                name: "id".to_string(),
                arrow_type: "int32".to_string(),
                nullable: false,
                initial_default: None,
            },
            LogicalField {
                field_id: 2,
                name: "rating".to_string(),
                arrow_type: "float64".to_string(),
                nullable: true,
                initial_default: None,
            },
            LogicalField {
                field_id: 3,
                name: "label".to_string(),
                arrow_type: "utf8".to_string(),
                nullable: true,
                initial_default: None,
            },
            LogicalField {
                field_id: 4,
                name: "ts".to_string(),
                arrow_type: "timestamp_us".to_string(),
                nullable: true,
                initial_default: None,
            },
            LogicalField {
                field_id: 5,
                name: "amount".to_string(),
                arrow_type: "decimal128(18,4)".to_string(),
                nullable: false,
                initial_default: None,
            },
        ];
        let json = spec.to_json();

        // The field must appear in the serialized JSON when non-empty.
        assert!(
            json.contains("logical_schema"),
            "non-empty logical_schema must appear in JSON: {json}"
        );

        // Round-trip: all fields survive.
        let back = ScanSpec::from_json(&json).unwrap();
        let fields = &back.common.logical_schema;
        assert_eq!(fields.len(), 5);
        assert_eq!(fields[0].field_id, 1);
        assert_eq!(fields[0].name, "id");
        assert_eq!(fields[0].arrow_type, "int32");
        assert!(!fields[0].nullable);
        assert_eq!(fields[1].field_id, 2);
        assert_eq!(fields[1].name, "rating");
        assert_eq!(fields[1].arrow_type, "float64");
        assert!(fields[1].nullable);
        assert_eq!(fields[2].arrow_type, "utf8");
        assert_eq!(fields[3].arrow_type, "timestamp_us");
        assert_eq!(fields[4].arrow_type, "decimal128(18,4)");
        assert!(!fields[4].nullable);

        // A spec without logical_schema must omit the field from JSON.
        let row_spec = sample_spec();
        assert!(row_spec.common.logical_schema.is_empty());
        let row_json = row_spec.to_json();
        assert!(
            !row_json.contains("logical_schema"),
            "empty logical_schema must be absent from JSON: {row_json}"
        );

        // A legacy payload without the field deserializes to an empty Vec.
        let legacy_json = r#"{
            "files": [["s3://w/f0.parquet", 100]],
            "projection": [],
            "storage": {
                "endpoint": "http://minio:9000",
                "region": "us-east-1",
                "access_key": "k",
                "secret_key": "s"
            }
        }"#;
        let legacy = ScanSpec::from_json(legacy_json).unwrap();
        assert!(
            legacy.common.logical_schema.is_empty(),
            "missing logical_schema must default to empty (backward-compat)"
        );
    }

    /// name_mapping round-trips through JSON (spec WITH the field) and
    /// a legacy spec WITHOUT it deserializes correctly (backward-compatible default).
    #[test]
    fn name_mapping_round_trips_and_defaults_to_empty() {
        // A spec with a populated name_mapping.
        let mut spec = sample_spec();
        spec.common.name_mapping = vec![
            NameMappingEntry {
                name: "id".to_string(),
                field_id: 1,
            },
            NameMappingEntry {
                name: "rating".to_string(),
                field_id: 2,
            },
        ];
        let json = spec.to_json();

        // The field must appear in the serialized JSON when non-empty.
        assert!(
            json.contains("name_mapping"),
            "non-empty name_mapping must appear in JSON: {json}"
        );

        // Round-trip: all entries survive.
        let back = ScanSpec::from_json(&json).unwrap();
        let entries = &back.common.name_mapping;
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].name, "id");
        assert_eq!(entries[0].field_id, 1);
        assert_eq!(entries[1].name, "rating");
        assert_eq!(entries[1].field_id, 2);

        // A spec without name_mapping must omit the field from JSON.
        let row_spec = sample_spec();
        assert!(row_spec.common.name_mapping.is_empty());
        let row_json = row_spec.to_json();
        assert!(
            !row_json.contains("name_mapping"),
            "empty name_mapping must be absent from JSON: {row_json}"
        );

        // A legacy payload without the field deserializes to an empty Vec.
        let legacy_json = r#"{
            "files": [["s3://w/f0.parquet", 100]],
            "projection": [],
            "storage": {
                "endpoint": "http://minio:9000",
                "region": "us-east-1",
                "access_key": "k",
                "secret_key": "s"
            }
        }"#;
        let legacy = ScanSpec::from_json(legacy_json).unwrap();
        assert!(
            legacy.common.name_mapping.is_empty(),
            "missing name_mapping must default to empty (backward-compat)"
        );
    }

    /// T8 — ScanSpec threading fields round-trip and default to 1 when absent.
    ///
    /// Verifies that:
    /// 1. Explicit `df_target_partitions` / `df_threads_per_udf` values survive
    ///    serialize → deserialize.
    /// 2. A legacy JSON payload that lacks these fields deserializes with both
    ///    fields defaulting to 1 (backward-compatible with pre-existing specs).
    #[test]
    fn scan_spec_threading_fields_round_trip_and_default_to_one() {
        // 1. Explicit values round-trip.
        let mut spec = sample_spec();
        spec.common.df_target_partitions = 4;
        spec.common.df_threads_per_udf = 2;
        let json = spec.to_json();
        let back = ScanSpec::from_json(&json).unwrap();
        assert_eq!(
            back.common.df_target_partitions, 4,
            "df_target_partitions must survive round-trip"
        );
        assert_eq!(
            back.common.df_threads_per_udf, 2,
            "df_threads_per_udf must survive round-trip"
        );

        // 2. The fields are present in the serialized JSON.
        assert!(
            json.contains("df_target_partitions"),
            "serialized JSON must carry df_target_partitions: {json}"
        );
        assert!(
            json.contains("df_threads_per_udf"),
            "serialized JSON must carry df_threads_per_udf: {json}"
        );

        // 3. A legacy payload without these fields deserializes with both defaulting to 1.
        let legacy_json = r#"{
            "files": [["s3://w/f0.parquet", 100]],
            "projection": [],
            "storage": {
                "endpoint": "http://minio:9000",
                "region": "us-east-1",
                "access_key": "k",
                "secret_key": "s"
            }
        }"#;
        let legacy = ScanSpec::from_json(legacy_json).unwrap();
        assert_eq!(
            legacy.common.df_target_partitions, 1,
            "missing df_target_partitions must default to 1 (backward-compat)"
        );
        assert_eq!(
            legacy.common.df_threads_per_udf, 1,
            "missing df_threads_per_udf must default to 1 (backward-compat)"
        );
    }

    /// Task 4.3: df_batch_size round-trips through JSON and defaults correctly on a legacy spec.
    ///
    /// Verifies that:
    /// 1. An explicit `df_batch_size` value survives serialize → deserialize.
    /// 2. A legacy JSON payload lacking the field deserializes to 8192 (backward-compatible).
    #[test]
    fn df_batch_size_round_trips_and_defaults() {
        // 1. Explicit non-default value round-trips.
        let mut spec = sample_spec();
        spec.common.df_batch_size = 4096;
        let json = spec.to_json();
        let back = ScanSpec::from_json(&json).unwrap();
        assert_eq!(
            back.common.df_batch_size, 4096,
            "df_batch_size must survive round-trip"
        );

        // 2. The field is present in the serialized JSON.
        assert!(
            json.contains("df_batch_size"),
            "serialized JSON must carry df_batch_size: {json}"
        );

        // 3. A legacy payload without df_batch_size deserializes to 8192.
        let legacy_json = r#"{
            "files": [["s3://w/f0.parquet", 100]],
            "projection": [],
            "storage": {
                "endpoint": "http://minio:9000",
                "region": "us-east-1",
                "access_key": "k",
                "secret_key": "s"
            }
        }"#;
        let legacy = ScanSpec::from_json(legacy_json).unwrap();
        assert_eq!(
            legacy.common.df_batch_size, 8192,
            "missing df_batch_size must default to 8192 (backward-compat)"
        );
    }

    /// Task 1.2: memory_pool_fraction and instance_overhead_mb round-trip and default correctly.
    ///
    /// Verifies that:
    /// 1. Explicit values survive serialize → deserialize.
    /// 2. A legacy JSON payload lacking both fields deserializes to 0.6 / 200.
    #[test]
    fn scan_spec_memory_fields_round_trip_and_default() {
        // 1. Explicit non-default values round-trip.
        let mut spec = sample_spec();
        spec.common.memory_pool_fraction = 0.5;
        spec.common.instance_overhead_mb = 256;
        let json = spec.to_json();
        let back = ScanSpec::from_json(&json).unwrap();
        assert_eq!(
            back.common.memory_pool_fraction, 0.5,
            "memory_pool_fraction must survive round-trip"
        );
        assert_eq!(
            back.common.instance_overhead_mb, 256,
            "instance_overhead_mb must survive round-trip"
        );

        // 2. Legacy payload without these fields → defaults 0.6 / 200.
        let legacy_json = r#"{
            "files": [["s3://w/f0.parquet", 100]],
            "projection": [],
            "storage": {
                "endpoint": "http://minio:9000",
                "region": "us-east-1",
                "access_key": "k",
                "secret_key": "s"
            }
        }"#;
        let legacy = ScanSpec::from_json(legacy_json).unwrap();
        assert_eq!(
            legacy.common.memory_pool_fraction, 0.6,
            "missing memory_pool_fraction must default to 0.6 (backward-compat)"
        );
        assert_eq!(
            legacy.common.instance_overhead_mb, 200,
            "missing instance_overhead_mb must default to 200 (backward-compat)"
        );
    }

    /// Task 2.2: s3_max_connections round-trips through JSON and defaults to a
    /// conservative built-in budget (clamped to at least 1) when absent.
    ///
    /// Verifies that:
    /// 1. An explicit value survives serialize → deserialize.
    /// 2. A legacy JSON payload lacking the field deserializes to the built-in
    ///    default (backward-compatible).
    #[test]
    fn s3_max_connections_round_trips_and_defaults() {
        // 1. Explicit non-default value round-trips.
        let mut spec = sample_spec();
        spec.common.s3_max_connections = 32;
        let json = spec.to_json();
        let back = ScanSpec::from_json(&json).unwrap();
        assert_eq!(
            back.common.s3_max_connections, 32,
            "s3_max_connections must survive round-trip"
        );

        // 2. The field is present in the serialized JSON.
        assert!(
            json.contains("s3_max_connections"),
            "serialized JSON must carry s3_max_connections: {json}"
        );

        // 3. A legacy payload without the field deserializes to the built-in default.
        // `files` uses the current compact [path, size] 2-tuple wire form (ADR-053).
        let legacy_json = r#"{
            "files": [["s3://w/f0.parquet", 123]],
            "projection": [],
            "storage": {
                "endpoint": "http://minio:9000",
                "region": "us-east-1",
                "access_key": "k",
                "secret_key": "s"
            }
        }"#;
        let legacy = ScanSpec::from_json(legacy_json).unwrap();
        assert_eq!(
            legacy.common.s3_max_connections,
            default_s3_max_connections(),
            "missing s3_max_connections must default to the built-in budget (backward-compat)"
        );
        assert!(
            legacy.common.s3_max_connections >= 1,
            "default s3_max_connections must be clamped to at least 1"
        );

        // 4. The default also applies to CommonScanSpec (shard-invariant blob).
        let legacy_common_json = r#"{
            "projection": [],
            "storage": {
                "endpoint": "http://minio:9000",
                "region": "us-east-1",
                "access_key": "k",
                "secret_key": "s"
            }
        }"#;
        let legacy_common = CommonScanSpec::from_json(legacy_common_json).unwrap();
        assert_eq!(
            legacy_common.s3_max_connections,
            default_s3_max_connections(),
            "missing s3_max_connections must default on CommonScanSpec too (backward-compat)"
        );

        // 5. The value threads through the split (to_common) / merge (from_parts) impls.
        let split = spec.to_common();
        assert_eq!(
            split.s3_max_connections, 32,
            "to_common must carry s3_max_connections through the split"
        );
        let merged = ScanSpec::from_parts(split, spec.files.clone());
        assert_eq!(
            merged.common.s3_max_connections, 32,
            "from_parts must carry s3_max_connections through the merge"
        );
    }

    /// Task 1.3(a): the common blob serializes WITHOUT `files` but WITH
    /// `table_root` (carried once, shard-invariant); the per-shard files list
    /// serializes as compact `[path, size]` 2-tuples; and reconstituting via
    /// `from_parts` (through JSON) yields a spec equal to the pre-split spec.
    #[test]
    fn from_parts_reconstitutes_files_tuples_and_table_root() {
        let original = sample_spec();

        // Split into the shard-invariant common blob + the per-shard files list.
        let common_json = original.to_common_json();
        let files_json = ScanSpec::files_json(&original.files);

        // The common blob must NOT carry the per-shard files list (type-level guarantee).
        assert!(
            !common_json.contains("\"files\""),
            "common blob must not contain a files key: {common_json}"
        );
        // Nor may any file path value leak into the common blob.
        assert!(
            !common_json.contains("part-00000.parquet"),
            "common blob must not carry any file path: {common_json}"
        );
        // The common blob DOES carry table_root, once.
        assert!(
            common_json.contains(r#""table_root":"s3://warehouse/db/table""#),
            "common blob must carry table_root: {common_json}"
        );

        // The per-shard files list is a compact array of [path, size] 2-tuples.
        assert_eq!(
            files_json,
            r#"[["data/part-00000.parquet",1024],["data/part-00001.parquet",2048]]"#
        );

        // The common blob round-trips on its own.
        let common_back = CommonScanSpec::from_json(&common_json).unwrap();
        assert_eq!(common_back, original.to_common());
        assert_eq!(common_back.table_root, "s3://warehouse/db/table");

        // from_parts_json reconstitutes a spec equal to the pre-split original,
        // with table_root reattached from the common blob and files as tuples.
        let reconstituted = ScanSpec::from_parts_json(&common_json, &files_json).unwrap();
        assert_eq!(reconstituted, original);
        assert_eq!(reconstituted.common.table_root, "s3://warehouse/db/table");
        assert_eq!(
            reconstituted.files,
            vec![
                FileEntry::new("data/part-00000.parquet", 1024),
                FileEntry::new("data/part-00001.parquet", 2048),
            ]
        );

        // The struct-level from_parts is equivalent to the JSON round-trip.
        let via_struct = ScanSpec::from_parts(original.to_common(), original.files.clone());
        assert_eq!(via_struct, original);
    }

    /// Task 1.3(b): malformed common OR files JSON produces errors that never echo
    /// the raw input (which carries credentials).
    #[test]
    fn malformed_common_or_files_json_does_not_leak_credentials() {
        // Malformed common blob carrying credential-shaped values.
        let garbled_common =
            r#"{"storage": {"access_key": "SECRET", "secret_key": "TOPSECRET"}, incomplete"#;
        let err = CommonScanSpec::from_json(garbled_common).unwrap_err();
        assert!(
            !err.contains("SECRET"),
            "common error leaked a secret: {err}"
        );
        assert!(
            !err.contains("TOPSECRET"),
            "common error leaked a secret: {err}"
        );
        assert!(err.contains("scan common spec deserialization failed"));

        // Malformed files argument.
        let garbled_files = r#"["s3://w/SECRETFILE.parquet", incomplete"#;
        let files_err = ScanSpec::files_from_json(garbled_files).unwrap_err();
        assert!(
            !files_err.contains("SECRETFILE"),
            "files error leaked input: {files_err}"
        );
        assert!(files_err.contains("scan files deserialization failed"));

        // from_parts_json surfaces the common-arg error without leaking either input.
        let combined = ScanSpec::from_parts_json(garbled_common, "[]").unwrap_err();
        assert!(!combined.contains("SECRET"));
        assert!(!combined.contains("TOPSECRET"));
    }

    /// Task 1.3(d): `table_root` round-trips through JSON, and a legacy payload
    /// that predates the field (no `table_root` key) deserializes with it
    /// defaulting to the empty string — the documented "treat every path as
    /// absolute" case.
    #[test]
    fn legacy_empty_root_treats_paths_as_absolute() {
        // Explicit table_root survives serialize -> deserialize on both spec kinds.
        let spec = sample_spec();
        assert_eq!(spec.common.table_root, "s3://warehouse/db/table");
        let json = spec.to_json();
        assert!(
            json.contains(r#""table_root":"s3://warehouse/db/table""#),
            "non-empty table_root must appear in JSON: {json}"
        );
        let back = ScanSpec::from_json(&json).unwrap();
        assert_eq!(back.common.table_root, "s3://warehouse/db/table");

        let common = spec.to_common();
        let common_json = common.to_json();
        assert!(
            common_json.contains(r#""table_root":"s3://warehouse/db/table""#),
            "non-empty table_root must appear in the common blob: {common_json}"
        );

        // An empty table_root is omitted from serialized JSON (skip_serializing_if).
        let mut rootless = sample_spec();
        rootless.common.table_root = String::new();
        let rootless_json = rootless.to_json();
        assert!(
            !rootless_json.contains("table_root"),
            "empty table_root must be absent from JSON: {rootless_json}"
        );

        // A legacy full-spec payload without table_root deserializes to empty
        // (all file paths in `files` are then absolute, per field semantics).
        let legacy_json = r#"{
            "files": [["s3://w/f0.parquet", 100]],
            "projection": [],
            "storage": {
                "endpoint": "http://minio:9000",
                "region": "us-east-1",
                "access_key": "k",
                "secret_key": "s"
            }
        }"#;
        let legacy = ScanSpec::from_json(legacy_json).unwrap();
        assert_eq!(
            legacy.common.table_root, "",
            "missing table_root must default to empty (backward-compat; paths are absolute)"
        );
        assert_eq!(legacy.files, vec![FileEntry::new("s3://w/f0.parquet", 100)]);

        // Same for the common blob in isolation.
        let legacy_common_json = r#"{
            "projection": [],
            "storage": {
                "endpoint": "http://minio:9000",
                "region": "us-east-1",
                "access_key": "k",
                "secret_key": "s"
            }
        }"#;
        let legacy_common = CommonScanSpec::from_json(legacy_common_json).unwrap();
        assert_eq!(
            legacy_common.table_root, "",
            "missing table_root must default to empty on the common blob (backward-compat)"
        );

        // from_parts reattaches the empty table_root onto the reconstituted spec.
        let reconstituted = ScanSpec::from_parts(
            legacy_common,
            vec![FileEntry::new("s3://w/f0.parquet", 100)],
        );
        assert_eq!(reconstituted.common.table_root, "");
    }

    /// Task 1.3(c): `catalog` no longer appears in any serialized JSON.
    #[test]
    fn catalog_absent_from_all_serialized_json() {
        let spec = sample_spec();
        assert!(
            !spec.to_json().contains("catalog"),
            "full spec JSON must not contain a catalog key: {}",
            spec.to_json()
        );
        assert!(
            !spec.to_common_json().contains("catalog"),
            "common blob JSON must not contain a catalog key: {}",
            spec.to_common_json()
        );
    }

    /// Task 2.1(a): a spec WITHOUT a join block serializes with no `join` key and a
    /// legacy payload that predates the field deserializes with `join` defaulting to
    /// `None` — existing non-join specs are unchanged (backward-compatible).
    #[test]
    fn absent_join_block_round_trips_unchanged() {
        // A non-join spec (join: None) must omit the field from serialized JSON on
        // both the full spec and the shard-invariant common blob.
        let spec = sample_spec();
        assert!(spec.common.join.is_none());
        let json = spec.to_json();
        assert!(
            !json.contains("\"join\""),
            "non-join spec must not carry a join key: {json}"
        );
        let common_json = spec.to_common_json();
        assert!(
            !common_json.contains("\"join\""),
            "non-join common blob must not carry a join key: {common_json}"
        );

        // A legacy full-spec payload predating the field deserializes with join = None.
        let legacy_json = r#"{
            "files": [["s3://w/f0.parquet", 100]],
            "projection": [],
            "storage": {
                "endpoint": "http://minio:9000",
                "region": "us-east-1",
                "access_key": "k",
                "secret_key": "s"
            }
        }"#;
        let legacy = ScanSpec::from_json(legacy_json).unwrap();
        assert!(
            legacy.common.join.is_none(),
            "missing join must default to None (backward-compat)"
        );

        // Same for the common blob in isolation.
        let legacy_common_json = r#"{
            "projection": [],
            "storage": {
                "endpoint": "http://minio:9000",
                "region": "us-east-1",
                "access_key": "k",
                "secret_key": "s"
            }
        }"#;
        let legacy_common = CommonScanSpec::from_json(legacy_common_json).unwrap();
        assert!(
            legacy_common.join.is_none(),
            "missing join must default to None on the common blob (backward-compat)"
        );
    }

    /// Task 1.1: a legacy `[path, size]` per-shard file entry (every entry ever
    /// written before positional-delete support) still deserializes — as a
    /// [`FileEntry`] whose `deletes` list is empty — inside a full `ScanSpec`
    /// payload, inside the isolated `files_from_json` helper, and as the
    /// compact wire form a delete-free [`FileEntry`] serializes back to.
    #[test]
    fn legacy_file_entry_reconstitutes_empty_deletes() {
        // A whole legacy ScanSpec payload whose `files` array uses the
        // pre-existing bare `[path, size]` 2-tuple wire form.
        let legacy_json = r#"{
            "files": [["s3://w/f0.parquet", 100], ["s3://w/f1.parquet", 200]],
            "projection": [],
            "storage": {
                "endpoint": "http://minio:9000",
                "region": "us-east-1",
                "access_key": "k",
                "secret_key": "s"
            }
        }"#;
        let legacy = ScanSpec::from_json(legacy_json).unwrap();
        assert_eq!(legacy.files.len(), 2);
        for entry in &legacy.files {
            assert!(
                entry.deletes.is_empty(),
                "legacy [path, size] entry must reconstitute with an empty delete list: {entry:?}"
            );
        }
        assert_eq!(legacy.files[0].path, "s3://w/f0.parquet");
        assert_eq!(legacy.files[0].size, 100);
        assert_eq!(legacy.files[1].path, "s3://w/f1.parquet");
        assert_eq!(legacy.files[1].size, 200);
        assert_eq!(
            legacy.files,
            vec![
                FileEntry::new("s3://w/f0.parquet", 100),
                FileEntry::new("s3://w/f1.parquet", 200),
            ]
        );

        // The same legacy 2-tuple form deserializes through the isolated
        // per-shard `files_from_json` helper the UDF boundary actually uses.
        let files_only_json = r#"[["s3://w/f0.parquet", 100], ["s3://w/f1.parquet", 200]]"#;
        let files = ScanSpec::files_from_json(files_only_json).unwrap();
        assert_eq!(
            files,
            vec![
                FileEntry::new("s3://w/f0.parquet", 100),
                FileEntry::new("s3://w/f1.parquet", 200),
            ]
        );
        assert!(files.iter().all(|f| f.deletes.is_empty()));

        // A delete-free FileEntry serializes back to the SAME compact 2-tuple
        // form (not a 3-tuple with a trailing empty array) — the wire stays
        // minimal for the still-common delete-free case.
        let round_tripped = ScanSpec::files_json(&files);
        assert_eq!(
            round_tripped,
            files_only_json.replace(' ', ""),
            "delete-free entries must round-trip to the compact [path,size] form: {round_tripped}"
        );

        // A FileEntry carrying positional-delete refs serializes as a 3-tuple
        // and deserializes back with the delete refs intact.
        let with_deletes = FileEntry::with_deletes(
            "s3://w/f2.parquet",
            300,
            vec![DeleteFileRef {
                path: "s3://w/deletes/d0.parquet".to_string(),
                size: 50,
                content_type: DeleteFileContentType::PositionDeletes,
            }],
        );
        let mixed_json = ScanSpec::files_json(&[
            FileEntry::new("s3://w/f0.parquet", 100),
            with_deletes.clone(),
        ]);
        assert!(
            mixed_json.contains("s3://w/deletes/d0.parquet"),
            "delete-carrying entry must serialize its delete file path: {mixed_json}"
        );
        let mixed_back = ScanSpec::files_from_json(&mixed_json).unwrap();
        assert_eq!(mixed_back[0].deletes, Vec::new());
        assert_eq!(mixed_back[1], with_deletes);
        assert_eq!(
            mixed_back[1].deletes[0].content_type,
            DeleteFileContentType::PositionDeletes
        );
    }

    /// Task 2.1(b): a spec WITH a join block round-trips through JSON and through the
    /// common/per-shard split and merge. The join block (dimension side) is
    /// shard-INVARIANT: it rides in the common blob (UDF argument 0), never in the
    /// per-shard files list (argument 1), so the fact side's per-shard `files` and
    /// the dimension side's `join.files` never collide.
    #[test]
    fn join_block_round_trips_through_split_and_merge() {
        let mut spec = sample_spec();
        spec.common.join = Some(JoinSpec {
            table_root: "s3://warehouse/db/dim".into(),
            files: vec![
                FileEntry::new("data/dim-00000.parquet", 512),
                FileEntry::new("data/dim-00001.parquet", 1024),
            ],
            logical_schema: vec![LogicalField {
                field_id: 1,
                name: "d_key".into(),
                arrow_type: "int64".into(),
                nullable: false,
                initial_default: None,
            }],
            name_mapping: Vec::new(),
            join_type: JoinType::Inner,
            condition: "\"F_KEY\" = \"D_KEY\"".into(),
        });

        // The serialized JSON carries the join block; join_type is a lowercase tag.
        let json = spec.to_json();
        assert!(
            json.contains("\"join\""),
            "join spec must carry the join block: {json}"
        );
        assert!(
            json.contains("\"join_type\":\"inner\""),
            "join_type must serialize as the lowercase tag: {json}"
        );

        // Whole-spec round-trip.
        let back = ScanSpec::from_json(&json).unwrap();
        assert_eq!(back, spec);

        // The join block lives in the shard-invariant common part, so the dimension
        // files ride in the common blob (once), not per shard.
        let common = spec.to_common();
        assert_eq!(common.join, spec.common.join);
        let common_json = spec.to_common_json();
        assert!(
            common_json.contains("dim-00000.parquet"),
            "dimension files must ride in the shard-invariant common blob: {common_json}"
        );

        // The per-shard files list still carries ONLY the fact side's files.
        let files_json = ScanSpec::files_json(&spec.files);
        assert!(
            !files_json.contains("dim-00000.parquet"),
            "per-shard files must not carry dimension files: {files_json}"
        );

        // Reconstitution from the two UDF arguments reattaches the join block.
        let reconstituted = ScanSpec::from_parts_json(&common_json, &files_json).unwrap();
        assert_eq!(reconstituted, spec);
        let jb = reconstituted
            .common
            .join
            .expect("join block must survive reconstitution");
        assert_eq!(jb.table_root, "s3://warehouse/db/dim");
        assert_eq!(
            jb.files,
            vec![
                FileEntry::new("data/dim-00000.parquet", 512),
                FileEntry::new("data/dim-00001.parquet", 1024),
            ]
        );
        assert_eq!(jb.join_type, JoinType::Inner);
        assert_eq!(jb.condition, "\"F_KEY\" = \"D_KEY\"");
        assert_eq!(jb.logical_schema.len(), 1);
        assert_eq!(jb.logical_schema[0].name, "d_key");

        // The struct-level split/merge is equivalent to the JSON round-trip.
        let via_struct = ScanSpec::from_parts(spec.to_common(), spec.files.clone());
        assert_eq!(via_struct, spec);
    }

    /// The two-argument UDF wire (shard-invariant common blob + per-shard files
    /// array) MUST stay byte-for-byte identical after `CommonScanSpec` was embedded
    /// into `ScanSpec` via `#[serde(flatten)]`. Flatten reorders `ScanSpec`'s own
    /// whole-struct serialization (`files` moves to the end), but production never
    /// reconstitutes from a whole-`ScanSpec` JSON — it splits via `to_common_json()`
    /// (which serializes `CommonScanSpec`, untouched) and `files_json()` (untouched).
    /// This pins both against strings captured from the pre-flatten code, so any
    /// future field reorder, dropped `skip_serializing_if`, or default drift in the
    /// common blob or files list is caught as a byte diff.
    #[test]
    fn common_blob_wire_is_byte_stable() {
        let spec = sample_spec();

        let common_wire = r#"{"table_root":"s3://warehouse/db/table","projection":["id","name"],"filter":"(\"ID\" > 10)","limit":100,"storage":{"endpoint":"http://minio:9000","region":"us-east-1","access_key":"minioadmin","secret_key":"minioadmin","allow_http":true,"path_style":true},"df_target_partitions":1,"df_batch_size":8192,"df_threads_per_udf":1,"memory_pool_fraction":0.6,"instance_overhead_mb":200,"s3_max_connections":8}"#;
        assert_eq!(spec.to_common_json(), common_wire);

        let files_wire = r#"[["data/part-00000.parquet",1024],["data/part-00001.parquet",2048]]"#;
        assert_eq!(ScanSpec::files_json(&spec.files), files_wire);

        // The common blob is structurally free of the per-shard `files` key and the
        // adapter-only `catalog` key (the flatten preserves this guarantee).
        assert!(!common_wire.contains("\"files\""));
        assert!(!common_wire.contains("catalog"));
    }
}
