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
use std::collections::BTreeMap;

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
/// # Binding key
///
/// `field_id` and `physical_name` are the two ways a logical field can name its
/// physical counterpart, and AT MOST ONE of them is ever populated: two populated
/// keys would need a precedence rule with no authority behind it, and the second key
/// would never be consulted. Which one the producer populates encodes the binding
/// DECISION rather than the input that drove it, so nothing downstream re-derives it:
///
/// | Producer | `field_id` | `physical_name` | Scan-side binding |
/// |---|---|---|---|
/// | Iceberg (always) | `Some(id)` | `None` | embedded `PARQUET:field_id`, then `name_mapping`, then the physical name |
/// | Delta `id` column mapping | `Some(columnMapping.id)` | `None` | embedded `PARQUET:field_id` |
/// | Delta `name` column mapping | `None` | `Some(physicalName)` | the declared physical name |
/// | Delta `none` column mapping | `None` | `None` | identity — the logical name itself |
///
/// A field with NEITHER key deliberately carries no stand-in ordinal: an ordinal is a
/// value no writer ever wrote into any file, so tagging the logical schema with one
/// invites a false `PARQUET:field_id` match against a file that does carry ids.
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
    /// This column's field-id binding key — an Iceberg field-id, or a Delta
    /// `columnMapping.id` under `id` mapping. `None` when the column binds by its
    /// declared `physical_name` or by identity instead (see the struct doc). Absent
    /// from JSON when `None`, and declared FIRST so a field-id-bound column still
    /// serializes `"field_id":N` as its leading key.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub field_id: Option<i32>,
    /// Current logical name (from the table's schema at query time).
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
    /// The members of this column's nested type — see [`NestedMembers`] — carried so the
    /// JSON renderer keys a struct by its LOGICAL field name rather than by the physical
    /// name the file stores. `None` for every primitive column, and absent from JSON when
    /// `None`, so a spec authored before this field existed deserializes unchanged. It is
    /// NOT a type: `arrow_type` stays the `"utf8"` tag for every nested column, because the
    /// rendered JSON string IS the column's type everywhere the type is read.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub nested: Option<NestedMembers>,
    /// This column's physical-name binding key — its `delta.columnMapping.physicalName`
    /// under Delta `name` mapping, where the protocol requires a reader to match on the
    /// physical name. `None` whenever the column binds by `field_id` or by identity
    /// instead (see the struct doc), which is every Iceberg column. Appended LAST and
    /// absent from JSON when `None`, so a field-id-bound column's encoding gains no key.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub physical_name: Option<String>,
}

/// The members one nested logical field exposes, one variant per container kind.
///
/// This is the format-neutral nested counterpart of [`LogicalField`]'s own binding-key
/// choice, recursed: a struct's fields each carry a logical name plus the ONE binding key
/// the format's column mapping selects, exactly as a top-level column does, and a member
/// that is itself nested carries its own [`NestedMembers`] in turn. Nothing here names a
/// table format — an Iceberg nested field-id and a Delta `columnMapping` physical name are
/// the same two keys the top level already distinguishes.
///
/// A list's element and a map's key and value are POSITIONAL: a `ListArray` has exactly one
/// child and a `MapArray` exactly one key and one value child, so no name or id is needed to
/// find them, and carrying one would invent a binding key for a member the Delta protocol
/// never names. Each therefore carries only its own members, present only when that member
/// is itself a container — so `list<string>` encodes as `{"list":{}}` and
/// `map<int,struct<a>>` as `{"map":{"value":{"struct":{"fields":[{"name":"a"}]}}}}`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NestedMembers {
    /// A `list`, `large_list`, or `fixed_size_list` and its element's own members.
    List {
        /// The element's members, or `None` when the element is not itself a container.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        element: Option<Box<NestedMembers>>,
    },
    /// A `struct` and its fields, in the schema's declared order.
    Struct {
        /// The struct's fields, each with its logical name and single binding key.
        fields: Vec<NestedField>,
    },
    /// A `map` and the members of its key and of its value.
    Map {
        /// The key's members, or `None` when the key is not itself a container.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        key: Option<Box<NestedMembers>>,
        /// The value's members, or `None` when the value is not itself a container.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        value: Option<Box<NestedMembers>>,
    },
}

/// One named field of a nested `struct`, carrying its logical name and AT MOST ONE binding
/// key — the same `field_id` XOR `physical_name` XOR identity choice [`LogicalField`] makes
/// for a top-level column, so the scan side resolves a nested field by the one binding order
/// it already applies and no format-specific nested branch exists. See [`LogicalField`]'s
/// "Binding key" table for which producer populates which key.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NestedField {
    /// This field's field-id binding key — an Iceberg nested field-id, or a Delta
    /// `columnMapping.id` under `id` mapping. Absent from JSON when `None`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub field_id: Option<i32>,
    /// Current logical name, which is the name the rendered JSON object uses.
    pub name: String,
    /// This field's physical-name binding key — its `delta.columnMapping.physicalName`
    /// under Delta `name` mapping. Absent from JSON when `None`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub physical_name: Option<String>,
    /// This field's own members when it is itself a container. Absent from JSON when `None`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub nested: Option<NestedMembers>,
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

    /// Full logical schema of the dimension table at query time. Absent (empty) falls
    /// back to first-file schema inference, as on the raw-scan path.
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

    /// Row cap applied AFTER the node-local join and its `WHERE`, never to either
    /// side's scanned input. This asymmetry is the whole point of the field: a cap
    /// on a side's scan drops rows the join or the filter would have kept, so the
    /// shard answers with fewer rows than the query has. `None` = no cap.
    ///
    /// It lives here rather than on [`CommonScanSpec::limit`] because that field
    /// caps the SCAN — the wrong stage for a join — and because a spec carrying no
    /// join block has no post-join stage at all, so there is no field on which such
    /// a cap could exist. Only an UNORDERED cap is ever pushed: any `n` rows answer
    /// an unordered `LIMIT n`, so each shard may truncate its own joined output at
    /// `n` and the merge truncate again at `n`. An ordered window is global and
    /// rides on the adapter's outer wrapper instead, leaving every shard unbounded.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub post_join_limit: Option<u64>,

    /// The dimension table's partition-column names, in partition order — the same
    /// neutral concept as [`CommonScanSpec::partition_columns`], needed on this side
    /// because the broadcast/dimension side is its own table with its own partition
    /// layout. Empty (the default) on every Iceberg join spec today, and absent from
    /// JSON when empty, which is what keeps an Iceberg join spec byte-identical to
    /// its pre-existing encoding.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub partition_columns: Vec<String>,

    /// The dimension side's own resolved [`StorageBackend`], distinct from
    /// `common.storage` (the fact side's). Required — see the struct doc.
    pub storage: StorageBackend,
}

/// Where a Delta deletion vector's bytes live — the closed set of the Delta
/// protocol's three storage kinds.
///
/// Closed so a descriptor naming a fourth kind fails at plan time rather than
/// reaching the scan as an unread string that would be silently ignored, leaving
/// deleted rows in the result.
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

/// One row-deletion mechanism attached to a [`FileEntry`], naming ITSELF on the wire
/// and carrying only that mechanism's own payload.
///
/// A data file's deletions ride in ONE list of these values whichever table format
/// planned the scan, so the scan side reads that one list and dispatches on CONTENT —
/// it never asks which format produced the spec. A fifth mechanism is a new variant
/// here, not another optional field and another fork at every reader.
///
/// Each variant carries the minimum needed to act on it, per the "Minimal ScanSpec
/// surface" decision (ADR-085 in `specs/decision-log.md`): no serialized Iceberg
/// `Schema` and no `BoundPredicate`, because the reader already has the logical schema
/// ([`CommonScanSpec::logical_schema`]) and does its own predicate pushdown.
///
/// Only [`IcebergPositionalDelete`](DeleteMechanism::IcebergPositionalDelete) is
/// applied today. Plan time is the authoritative gate that fails loud on every other
/// mechanism BEFORE a file reaches this spec; the other variants exist so the scan
/// reader's read-time backstop can reject one cleanly — rather than panic, or stay
/// silent and return rows the table has deleted — if one ever slips through.
///
/// # Serde
///
/// (De)serialization routes through the private [`DeleteMechanismWire`], exactly as
/// [`FileEntry`] routes through [`FileEntryWire`], which keeps the neutral Rust shape
/// and the frozen JSON encoding two independent decisions with one owner each. An
/// internally-tagged enum would emit its discriminant key FIRST and reorder every
/// Iceberg member's keys, breaking the pinned
/// `{"path":…,"size":…,"content_type":…}` encoding for no behavioral gain.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(from = "DeleteMechanismWire", into = "DeleteMechanismWire")]
pub enum DeleteMechanism {
    /// An Iceberg Parquet positional-delete file (`file_path`/`pos` columns) — the
    /// only mechanism this engine applies.
    IcebergPositionalDelete {
        /// Path to the delete file, relative to [`CommonScanSpec::table_root`] when
        /// non-empty and the file lives under it, otherwise an absolute URI —
        /// exactly like [`FileEntry::path`].
        path: String,
        /// Byte size, used the same way as [`FileEntry::size`]: to build the delete
        /// file's `ObjectMeta` without an object-store HEAD.
        size: u64,
    },
    /// An Iceberg equality-delete file. Never applied by this engine.
    IcebergEqualityDelete { path: String, size: u64 },
    /// A Puffin-encoded Iceberg v3 deletion vector. Never applied by this engine.
    IcebergPuffinDeletionVector { path: String, size: u64 },
    /// A Delta deletion vector: a byte RANGE inside a possibly-shared `.bin` file,
    /// carried verbatim and resolved into no path at plan time. Unrelated to the
    /// Iceberg variants above, each of which names a whole delete FILE.
    DeltaDeletionVector {
        storage: DeltaDeletionVectorStorage,
        /// The Delta `pathOrInlineDv` value, stored exactly as logged — a UUID, an
        /// absolute path, or the inline vector — so path reconstruction stays
        /// deferred to file registration.
        path_or_inline_dv: String,
        /// Byte offset of the vector inside its file. `None` when the file holds
        /// this vector alone, and absent from JSON when `None`.
        offset: Option<i32>,
        size_in_bytes: i32,
        /// Number of rows the vector deletes.
        cardinality: i64,
    },
}

impl DeleteMechanism {
    /// Whether this mechanism names a whole delete FILE (every Iceberg variant)
    /// rather than a byte range inside a shared one (the Delta deletion vector).
    ///
    /// The axis [`ScanSpec::files_from_json`]'s mutual-exclusion gate turns on.
    /// Matched exhaustively, so a mechanism added later must state which side it
    /// falls on instead of defaulting into one.
    fn is_delete_file_reference(&self) -> bool {
        match self {
            Self::IcebergPositionalDelete { .. }
            | Self::IcebergEqualityDelete { .. }
            | Self::IcebergPuffinDeletionVector { .. } => true,
            Self::DeltaDeletionVector { .. } => false,
        }
    }

    /// The object-store path whose bytes this mechanism reads, or `None` for a
    /// mechanism that names no path of its own.
    ///
    /// `None` for [`DeltaDeletionVector`](Self::DeltaDeletionVector): its
    /// `path_or_inline_dv` is resolved into a path at file registration and is never
    /// addressed from the delete list itself, so it is never fetched, relativized, or
    /// claimed for a store here — regardless of whether the value happens to look
    /// like a UUID token, an inline payload, or an absolute path
    /// ([`AbsolutePath`](DeltaDeletionVectorStorage::AbsolutePath)).
    /// Every caller that treats a delete member as an addressable file asks here
    /// instead of reaching into a variant, so that refusal is stated once and a
    /// mechanism added later cannot inherit it by accident.
    pub fn object_store_path(&self) -> Option<&str> {
        match self {
            Self::IcebergPositionalDelete { path, .. }
            | Self::IcebergEqualityDelete { path, .. }
            | Self::IcebergPuffinDeletionVector { path, .. } => Some(path.as_str()),
            Self::DeltaDeletionVector { .. } => None,
        }
    }

    /// [`object_store_path`](Self::object_store_path) for the plan-time caller that
    /// rewrites the path in place — same mechanisms carry one, same reason.
    pub(crate) fn object_store_path_mut(&mut self) -> Option<&mut String> {
        match self {
            Self::IcebergPositionalDelete { path, .. }
            | Self::IcebergEqualityDelete { path, .. }
            | Self::IcebergPuffinDeletionVector { path, .. } => Some(path),
            Self::DeltaDeletionVector { .. } => None,
        }
    }
}

/// The frozen `content_type` tag of an Iceberg delete-file member. Wire-private: it
/// exists only to reproduce that tag, and the public [`DeleteMechanism`] carries the
/// same information as a variant instead.
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum IcebergDeleteContentType {
    PositionDeletes,
    EqualityDeletes,
    PuffinDeletionVector,
}

/// Wire form of [`DeleteMechanism`] — see that enum's doc for why this shape exists.
/// Not part of the public API; [`DeleteMechanism`] is the only type callers construct
/// or match on.
///
/// `untagged` resolves variants against structurally DISJOINT key sets: the Iceberg
/// arm requires `path`, `size`, and `content_type`, the deletion-vector arm requires
/// `storage`, `path_or_inline_dv`, `size_in_bytes`, and `cardinality`. So neither can
/// match the other's encoding, and a member matching neither is refused rather than
/// read as the wrong mechanism. The Iceberg arm's FIELD ORDER is the frozen key order
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

/// One per-shard scanned-file entry: a data file's path and byte size, plus the
/// delete mechanisms (if any) that must be applied when reading it and the partition
/// values (if any) the file's own bytes do not record.
///
/// # Chosen shape: struct-per-file with an untagged legacy fallback
///
/// The Rust-level API is a plain struct (`path`, `size`, `deletes`,
/// `partition_values`) so callers never pattern-match a bare tuple to reach the
/// delete list. On the wire, `#[serde(from/into = "FileEntryWire")]` routes
/// (de)serialization through the private [`FileEntryWire`] enum, mirroring how
/// [`ProjectionItem`] already gives a bare-string legacy payload a typed fallback in
/// this same module:
/// - A legacy `[path, size]` 2-tuple (every entry written before
///   positional-delete support) deserializes with an empty `deletes` list.
/// - `[path, size, deletes]` (a 3-tuple) deserializes with `deletes` intact.
/// - A JSON OBJECT carrying a `partition_values` member deserializes with that map
///   intact. An object is disjoint from both tuple forms, so adding it left their
///   encodings and their deserialization precedence untouched — which a fourth tuple
///   slot would not have.
/// - Serialization always picks the SHORTEST form for the value at hand: the compact
///   2-tuple when there is neither a delete mechanism nor a partition value (keeping
///   the still-common bare case exactly as small on the wire as before either field
///   existed), the 3-tuple when there are deletes to carry, and the object whenever
///   there are partition values.
///
/// The object form is therefore selected by PARTITION VALUES, never by table format:
/// an entry whose only extra content is a deletion vector rides in the 3-tuple, which
/// is correct because each delete member names its own mechanism.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(from = "FileEntryWire", into = "FileEntryWire")]
pub struct FileEntry {
    /// Path to the data file, relative to [`CommonScanSpec::table_root`] when
    /// non-empty and the file lives under it, otherwise an absolute URI. Stored
    /// exactly as the source catalog or transaction log records it, resolved by
    /// nothing at plan time.
    pub path: String,
    /// Byte size, used to build the file's `ObjectMeta` without an
    /// object-store HEAD.
    pub size: u64,
    /// EVERY row-deletion mechanism that must be applied when reading this data
    /// file, whichever table format planned the scan — see [`DeleteMechanism`].
    /// Empty (the default for legacy and delete-free entries) means the file is read
    /// as-is. One list rather than one per format is what lets the scan dispatch on
    /// each member's own mechanism instead of on the spec's provenance.
    ///
    /// A list may hold Iceberg delete-file references OR one deletion vector, never
    /// both: the two are independent mechanisms, and applying both to one data file
    /// returns wrong rows — a mix [`ScanSpec::files_from_json`] refuses rather than
    /// reconstitutes.
    pub deletes: Vec<DeleteMechanism>,
    /// This file's partition values, one entry per column in
    /// [`CommonScanSpec::partition_columns`] — the values a scan reading the Parquet
    /// bytes alone cannot recover, because a partitioned writer records them outside
    /// the data file.
    ///
    /// A key present with NO value is a partition value of NULL — a value the scan
    /// materializes — whereas a partition column MISSING from the map is a planning
    /// defect the scan can detect. Collapsing the two onto one encoding would make
    /// that impossible, which is why the VALUE is optional rather than the key.
    /// Ordered by key on the wire, so a golden encoding is byte-stable across runs.
    ///
    /// Empty (the default) on an unpartitioned entry and on every Iceberg entry
    /// today, and absent from JSON when empty — expressed by the wire enum's
    /// shortest-form variant selection rather than by a field attribute, since this
    /// struct's own serde attributes route through that enum.
    pub partition_values: BTreeMap<String, Option<String>>,
}

/// Wire form of [`FileEntry`] — see that struct's doc for why this shape
/// exists. Not part of the public API; [`FileEntry`] is the only type callers
/// construct or match on.
///
/// `untagged` resolves variants in DECLARATION order, and a JSON object matches
/// neither tuple variant while a JSON array matches neither the struct variant. So
/// [`WithPartitionValues`](FileEntryWire::WithPartitionValues) is disjoint from both
/// tuple forms and its addition left their precedence exactly as it was. It carries
/// `deletes` as well, so the conversion from [`FileEntry`] is TOTAL and lossless for
/// every value the struct admits rather than dropping a field in a combination the
/// type permits but production construction never produces.
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
    /// Destructured exhaustively rather than read field by field, so a field added
    /// to [`FileEntry`] is a compile error here — the one place a new field could
    /// otherwise be silently dropped from the wire.
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
    /// A data-file entry with no delete mechanisms and no partition values — the
    /// common unpartitioned Iceberg case, and the only shape a legacy
    /// (pre-delete-support) entry can take.
    pub fn new(path: impl Into<String>, size: u64) -> Self {
        FileEntry {
            path: path.into(),
            size,
            deletes: Vec::new(),
            partition_values: BTreeMap::new(),
        }
    }

    /// A data-file entry with its delete mechanisms. Callers must not mix a deletion
    /// vector with an Iceberg delete-file reference in one list — see
    /// [`FileEntry::deletes`].
    pub fn with_deletes(path: impl Into<String>, size: u64, deletes: Vec<DeleteMechanism>) -> Self {
        FileEntry {
            path: path.into(),
            size,
            deletes,
            partition_values: BTreeMap::new(),
        }
    }

    /// A data-file entry with its partition values and no delete mechanism. A caller
    /// needing both assigns [`FileEntry::deletes`] on the result.
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
    /// The table's root location, used to reconstruct absolute file paths from
    /// per-shard relative paths.
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

    /// Row limit. `None` means no LIMIT pushdown. Not consulted on the join path: a
    /// join spec's fan-out helper always sets this to `None`, and the join path's
    /// row cap is [`JoinSpec::post_join_limit`] instead.
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

    /// Full logical schema of the table at query time, each field carrying the
    /// binding key its producer selected (see [`LogicalField`]).
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

    /// The table's partition-column names, in partition order. Carried ONCE here
    /// rather than per file, so a scan of a table with ZERO active files still knows
    /// which logical columns have no physical counterpart. Shard-invariant: the
    /// partition columns are table-level and identical across every shard.
    ///
    /// Empty (the default) on an unpartitioned table and on every Iceberg scan today,
    /// and absent from JSON when empty, which is what keeps an Iceberg common blob
    /// byte-identical to its pre-consolidation encoding.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub partition_columns: Vec<String>,

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
            partition_columns: Vec::new(),
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
    /// second argument. Each delete-free entry is a compact `[path, size]` 2-tuple;
    /// an entry carrying delete mechanisms is a `[path, size, deletes]` 3-tuple; an
    /// entry carrying partition values is a self-describing JSON object (see
    /// [`FileEntry`]). Paired with `files_from_json`.
    pub fn files_json(files: &[FileEntry]) -> String {
        serde_json::to_string(files).expect("files list serialization is infallible")
    }

    /// Deserialize a per-shard files list from the UDF's second argument.
    ///
    /// Accepts the legacy `[path, size]` wire form (reconstituted with an empty
    /// delete list and no partition values), the `[path, size, deletes]` form, and
    /// the partition-value object form — see [`FileEntry`]. Returns an error that
    /// does NOT include the raw input.
    ///
    /// Refuses an entry whose delete list MIXES a deletion vector with an Iceberg
    /// delete-file reference: the two are independent delete mechanisms, and applying
    /// both to one data file returns wrong rows. This is the one gate that holds for a
    /// payload this process did not build, so the scan never has to decide which
    /// mechanism wins. The [`FileEntry`] wire conversions stay total and lossless
    /// either way, keeping the struct-level round trip a property of the type rather
    /// than of this check.
    pub fn files_from_json(s: &str) -> Result<Vec<FileEntry>, String> {
        let files: Vec<FileEntry> = serde_json::from_str(s).map_err(|e| {
            // Do not echo `s`. A data error (e.g. a bare-string entry where a
            // [path, size] tuple is expected) can quote the input in `e`'s Display,
            // so build the message from structural fields only.
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
