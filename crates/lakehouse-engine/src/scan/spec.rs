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

/// One column of the partial-aggregate COLUMN CONTRACT — the per-shard columns an
/// [`AggKind`] decomposes into for the Exasol outer wrapper to re-aggregate.
///
/// [`AggKind::partial_columns`] owns which of these an aggregate contributes and in
/// what order; [`partial_column_name`] owns what each is called. The scan renders
/// each one's DataFusion aggregate expression and the adapter renders each one's
/// Exasol `EMITS` type, so a variant added here is a compile error at both — the
/// contract is extended by adding a case, never by editing a dispatch that
/// silently defaults.
///
/// `CountStar` and `CountArg` are distinct despite sharing a name and an `EMITS`
/// type: they render different DataFusion SQL (`COUNT(*)` versus `COUNT(<arg>)`),
/// so collapsing them would force the scan back to consulting the [`AggKind`] this
/// descriptor exists to abstract away.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PartialAggColumn {
    /// `COUNT(*)` — the `COUNT(*)` aggregate's only partial column.
    CountStar,
    /// `COUNT(<arg>)` — the `COUNT(col)` aggregate's only partial column.
    CountArg,
    /// `SUM(<arg>)` for a `SUM` aggregate.
    Sum,
    /// `MIN(<arg>)` for a `MIN` aggregate.
    Min,
    /// `MAX(<arg>)` for a `MAX` aggregate.
    Max,
    /// `AVG`'s numerator: `SUM(<arg>)`.
    AvgSum,
    /// `AVG`'s denominator: `COUNT(<arg>)`.
    AvgCnt,
    /// The statistical family's N: `COUNT(<arg>)`.
    StatCnt,
    /// The statistical family's Σx: `SUM(<arg>)`.
    StatSum,
    /// The statistical family's Σx²: `SUM(<arg> * <arg>)`.
    StatSumSq,
}

impl PartialAggColumn {
    /// Whether an empty shard contributes a zero rather than a NULL for this column.
    ///
    /// A counter column counts rows, so a shard that matched none legitimately
    /// contributes `0`; a value column has no value at all and contributes NULL.
    /// The distinction is expressed as a boolean rather than as an SDK `Value` so
    /// this module stays serde-only and never learns about `exasol_udf_sdk` — the
    /// emit site owns the `Value::Int64(0)` / `Value::Null` mapping.
    pub fn is_counter(&self) -> bool {
        match self {
            Self::CountStar | Self::CountArg | Self::AvgCnt | Self::StatCnt => true,
            Self::Sum | Self::Min | Self::Max | Self::AvgSum | Self::StatSum | Self::StatSumSq => {
                false
            }
        }
    }
}

/// The UNQUOTED partial column name for `col` at aggregate ordinal `ordinal`.
///
/// The sole owner of the `PARTIAL_<role>_<ordinal>` text. Three sites must agree on
/// it byte-for-byte — the scan's `AS "PARTIAL_…"` aliases, the adapter's `EMITS`
/// items, and the adapter's merge `SUM("PARTIAL_…")` expressions — and each applies
/// its own quoting, so this returns the bare name and never a quoted identifier.
///
/// `ordinal` is the aggregate's position in the plan list, NOT the partial column's
/// position: a multi-column aggregate's columns all carry its one plan ordinal and
/// are told apart by their role.
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
    /// The ordered partial columns this aggregate decomposes into.
    ///
    /// The single owner of the COLUMN CONTRACT's arity and order. Five sites
    /// depend on it — the scan's DataFusion SELECT list, its empty-shard fallback
    /// row, and its batch-column walk, plus the adapter's `EMITS` declaration and
    /// outer merge SELECT. Before this method each encoded the answer separately,
    /// and a disagreement was silent: the emit paths address columns positionally,
    /// so a site advancing by two where the SELECT list produced three shifts every
    /// later aggregate's value for the rest of the row.
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

/// Storage and catalog connection properties, both declared once in the
/// `lakehouse-catalog` crate and re-exported here at the path their consumers
/// already import.
///
/// [`StorageProps`] belongs to the crate that PRODUCES it — a `loadTable` response
/// vends the S3 credentials, region, endpoint, and path-style — while the scan
/// layer that CONSUMES it as a [`CommonScanSpec`] field keeps this path. One
/// definition therefore backs one serde wire contract, pinned by this module's
/// `common_blob_wire_is_byte_stable` test.
///
/// [`CatalogProps`] is not a scan-spec type at all: no `scan` module names it and
/// no serialized [`ScanSpec`] carries a catalog block. It is re-exported here only
/// so the adapter planning layer and the E2E harness keep the `use` path they were
/// written against.
///
/// [`StorageBackend`] is the backend selector wrapping [`StorageProps`], declared in
/// that same producing crate and re-exported here so the scan, adapter, and catalog
/// layers all name the storage backend at one path.
pub use lakehouse_catalog::{AdlsCred, CatalogProps, StorageBackend, StorageProps};

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
/// The dimension side is read through its OWN [`StorageBackend`], never the fact
/// side's: a vended credential is scoped to the table it was resolved for, so
/// reusing the fact side's `common.storage` for the dimension side's files serves
/// a credential that was never granted access to them. Requiring this field (no
/// default) is what makes that true at every construction site — a join block
/// built without its own storage fails to deserialize rather than silently
/// borrowing the fact side's grant.
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

    /// The dimension side's own resolved [`StorageBackend`], distinct from
    /// `common.storage` (the fact side's). Required — see the struct doc.
    pub storage: StorageBackend,
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

    pub storage: StorageBackend,

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

    /// EVERY secret value that must be stripped from an error this scan surfaces:
    /// the fact side's credentials unioned with the join's dimension-side
    /// credentials.
    ///
    /// The SINGLE owner of that union rule. Each side is read through its own
    /// [`StorageBackend`], but an error can be raised by code holding one side's
    /// store — or a router over both — which structurally cannot assemble a set
    /// covering a side it never sees. So the union lives here, beside the only two
    /// fields that carry a credential, and every redaction site reads it from here
    /// rather than rebuilding it. A second, independently maintained copy is how a
    /// dimension-side credential leaks through a fact-side-only redaction set.
    pub fn all_secret_values(&self) -> Vec<&str> {
        let mut secrets = self.storage.secret_values();
        if let Some(join) = &self.join {
            secrets.extend(join.storage.secret_values());
        }
        secrets
    }
}

impl Default for CommonScanSpec {
    /// The shard-invariant baseline: no pushdown (empty projection/filter/order-by,
    /// no aggregate/group/join), a placeholder S3 [`StorageBackend`], and every tuning
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
            storage: StorageBackend::S3(StorageProps::default()),
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

/// Reconstruct the absolute file URI for a per-shard `(path, _)` entry.
///
/// An entry that already contains a scheme (`"://"`) is absolute and returned
/// unchanged. Otherwise it is relative to `table_root` and joined onto it with
/// exactly one `/` separator (a trailing `/` on the root and a leading `/` on the
/// entry are both trimmed first, so the separator is neither doubled nor dropped).
pub(crate) fn reconstruct_abs_uri(entry_path: &str, table_root: &str) -> String {
    if entry_path.contains("://") {
        return entry_path.to_string();
    }
    let root = table_root.strip_suffix('/').unwrap_or(table_root);
    let rel = entry_path.strip_prefix('/').unwrap_or(entry_path);
    format!("{root}/{rel}")
}

#[cfg(test)]
#[path = "spec_tests.rs"]
mod tests;
