//! Scan specification types that cross the UDF argument boundary.
//!
//! The adapter splits the spec across TWO VARCHAR UDF arguments: the
//! shard-invariant [`CommonScanSpec`] serialized ONCE per fan-out (argument 0)
//! and the per-shard files [`FileSet`] JSON object `{deleteFiles, dataFiles}`
//! (argument 1). The scan UDF reads both via
//! `ctx.get_string(0)` / `ctx.get_string(1)` and reconstitutes a [`ScanSpec`]
//! through [`ScanSpec::from_parts_json`]. Because [`CommonScanSpec`] has no
//! `files` field, "files is the only per-shard field" is a type-level guarantee.
//!
//! Credentials (`access_key`, `secret_key`) MUST NEVER appear in any error message.
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// The kind of aggregate function to compute node-locally as a partial result.
///
/// COUNT(*) maps to `Count` (no column), COUNT(col) maps to `CountCol`.
/// AVG is decomposed into a (partial_sum, partial_count) pair in the scan UDF;
/// the adapter wrapper SQL performs the final division.
///
/// STDDEV/VARIANCE family are decomposed into a (cnt, sum, sum_sq) sufficient-
/// statistics triple; the wrapper reconstructs the population or sample statistic.
///
/// `CountDistinct` is the single-group `COUNT(DISTINCT col)` shape: each shard computes
/// its LOCAL distinct value set (NULLs excluded) and emits it as one VARCHAR partial
/// value carrying a JSON array; a scalar merge UDF unions the per-shard sets and returns
/// the final cardinality. No Arrow type crosses the `.so` boundary — see
/// `specs/_plans/add-count-distinct-and-expression-aggregate-pushdown/vs-adapter/pushdown-planning-count-distinct/spec.md`.
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
    /// Single-group `COUNT(DISTINCT col)` — see the doc above for the shard-local-set /
    /// scalar-merge-UDF decomposition. Never produced by the grouped (GROUP BY) detection
    /// path, which still declines `distinct: true` and falls back to row scanning.
    CountDistinct,
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
/// `column` is a bare, uppercase source-column identifier: only `ORDER_BY_COLUMN`
/// is advertised as a capability (not `ORDER_BY_EXPRESSION`), so Exasol only ever
/// sends bare column sort keys — see
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
    fn render_order_by_element(&self) -> String {
        self.render_ordered(&format!("\"{}\"", self.column.replace('"', "\"\"")))
    }

    /// Render this key's direction + NULL placement onto an already-rendered
    /// ordering expression: `<expr> ASC|DESC NULLS FIRST|LAST`.
    ///
    /// `expr` may be a quoted column reference (the row-scan and per-shard sorts),
    /// a positional output ordinal (the grouped-aggregate merge sort, whose output
    /// columns are `GK_*`/merged aggregates, not the source names), or any other
    /// valid ordering expression. Routing every ORDER BY the adapter emits through
    /// this ONE direction/NULL seam is what structurally guarantees they agree on
    /// direction and NULL placement — the correctness-critical top-N invariant
    /// (decision [7]).
    pub fn render_ordered(&self, expr: &str) -> String {
        let direction = if self.ascending { "ASC" } else { "DESC" };
        let nulls = if self.nulls_last {
            "NULLS LAST"
        } else {
            "NULLS FIRST"
        };
        format!("{expr} {direction} {nulls}")
    }
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
    /// entry may carry its own positional-delete files (and, for free, a deletion
    /// vector), which the scan applies to the dimension registration exactly as
    /// the raw-scan path does — a dimension table with merge-on-read deletes joins
    /// on its post-delete rows. Serialized as the normalized `{deleteFiles,
    /// dataFiles}` [`FileSet`] object (its pool scoped to this shard-invariant join
    /// block), the SAME encoding the per-shard fact side uses.
    #[serde(with = "file_set_serde")]
    pub files: Vec<FileEntry>,

    /// Full logical schema of the dimension Iceberg table at query time. Absent
    /// (empty) falls back to first-file schema inference, as on the raw-scan path.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub logical_schema: Vec<LogicalField>,

    /// The join kind. This phase only ever carries [`JoinType::Inner`].
    pub join_type: JoinType,

    /// Rendered DataFusion SQL join condition, spliced into the equi-join verbatim.
    pub condition: String,
}

/// The Iceberg delete mechanism a pooled delete file/container encodes.
///
/// Serialized SCREAMING_SNAKE_CASE (`POS_DEL`/`EQ_DEL`/`DV`) as the `type` field
/// of a [`PooledDeleteFile`]. Plan time (the adapter) is the authoritative gate
/// that fails loud on any mechanism this engine cannot apply BEFORE it reaches
/// the wire; the scan-side read-time backstop rejects an unapplicable pooled
/// entry cleanly if one ever slips through.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum DeleteType {
    /// A positional-delete file (`file_path`/`pos` columns) — applied on read.
    PosDel,
    /// An Iceberg equality-delete file. Never applied by this engine.
    EqDel,
    /// A v3 deletion vector: a Roaring-bitmap `deletion-vector-v1` blob stored
    /// inside a Puffin container — applied on read.
    Dv,
}

/// The physical container format of a pooled delete file.
///
/// Serialized SCREAMING_SNAKE_CASE (`PARQUET`/`AVRO`/`ORC`/`PUFFIN`) as the
/// `format` field of a [`PooledDeleteFile`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum DeleteFormat {
    Parquet,
    Avro,
    Orc,
    Puffin,
}

/// One interned entry in a shard's `deleteFiles` pool.
///
/// Each physical delete file or container is interned EXACTLY ONCE per shard,
/// regardless of how many data files reference it (e.g. a partition-granularity
/// positional-delete file, or a packed Puffin container many data files' DVs
/// share). Carries only what it takes to open the file and dispatch its
/// mechanism — `path`, `size`, `type`, `format`. It carries NO blob coordinates:
/// a DV blob's `offset`/`length` live on the referencing [`DeleteRef`], because a
/// single Puffin container can hold many blobs.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct PooledDeleteFile {
    /// Path to the delete file/container, relative to the file set's table root
    /// when non-empty and the file lives under it, otherwise an absolute URI.
    pub path: String,
    /// Byte size, used to build the file's `ObjectMeta` / Puffin `InputFile`
    /// without an object-store HEAD.
    pub size: u64,
    /// The delete mechanism this file encodes.
    #[serde(rename = "type")]
    pub delete_type: DeleteType,
    /// The physical container format of the file.
    pub format: DeleteFormat,
}

/// A structural reference from a data file to a pooled delete file.
///
/// `df` indexes the file set's `deleteFiles` pool. `offset`/`length` locate a
/// deletion-vector blob within a Puffin container and are present ONLY for a
/// blob-addressed deletion vector; both are absent for a whole-file positional-
/// or equality-delete file.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeleteRef {
    /// Index into the file set's `deleteFiles` pool.
    pub df: usize,
    /// Byte offset of the deletion-vector blob within its Puffin container.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub offset: Option<u64>,
    /// Byte length of the deletion-vector blob within its Puffin container.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub length: Option<u64>,
}

/// One `dataFiles` entry on the wire: a data file's path and byte size plus its
/// `df`-indexed delete references. `deletes` is omitted when the data file has
/// no deletes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DataFileWire {
    pub path: String,
    pub size: u64,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub deletes: Vec<DeleteRef>,
}

/// The normalized per-shard (and per-join-block) file set: an interned
/// `deleteFiles` pool plus a `dataFiles` list whose entries carry `df`-indexed
/// delete references into that pool.
///
/// This is the wire shape of the per-shard files argument (UDF argument 1) and
/// of the broadcast-join dimension side (in the shard-invariant common blob):
/// there is ONE file-set encoding across both sides. It (de)serializes as the
/// JSON object `{deleteFiles, dataFiles}`. The ergonomic Rust representation is a
/// `Vec<FileEntry>` (each carrying fully-resolved [`ResolvedDelete`]s);
/// [`FileSet::from_entries`] interns on the way out and [`FileSet::into_entries`]
/// resolves the pool on the way in.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct FileSet {
    #[serde(rename = "deleteFiles", default, skip_serializing_if = "Vec::is_empty")]
    pub delete_files: Vec<PooledDeleteFile>,
    #[serde(rename = "dataFiles")]
    pub data_files: Vec<DataFileWire>,
}

impl FileSet {
    /// Intern a resolved data-file list into the normalized wire shape: build the
    /// `deleteFiles` pool (each physical delete file/container appearing exactly
    /// once) and emit `df`-indexed references on each data file, preserving the
    /// blob `offset`/`length` of every deletion-vector reference.
    pub fn from_entries(entries: &[FileEntry]) -> Self {
        let mut delete_files: Vec<PooledDeleteFile> = Vec::new();
        let mut index: HashMap<PooledDeleteFile, usize> = HashMap::new();
        let mut data_files = Vec::with_capacity(entries.len());
        for entry in entries {
            let deletes = entry
                .deletes
                .iter()
                .map(|d| {
                    let pooled = PooledDeleteFile {
                        path: d.path.clone(),
                        size: d.size,
                        delete_type: d.delete_type,
                        format: d.format,
                    };
                    let df = *index.entry(pooled.clone()).or_insert_with(|| {
                        delete_files.push(pooled.clone());
                        delete_files.len() - 1
                    });
                    DeleteRef {
                        df,
                        offset: d.offset,
                        length: d.length,
                    }
                })
                .collect();
            data_files.push(DataFileWire {
                path: entry.path.clone(),
                size: entry.size,
                deletes,
            });
        }
        Self {
            delete_files,
            data_files,
        }
    }

    /// Resolve the normalized wire shape back into ergonomic [`FileEntry`] values,
    /// failing loud on any `df` index that falls outside the `deleteFiles` pool
    /// (a corrupt or mis-produced spec — never silently drop a delete).
    pub fn into_entries(self) -> Result<Vec<FileEntry>, String> {
        let FileSet {
            delete_files,
            data_files,
        } = self;
        let mut out = Vec::with_capacity(data_files.len());
        for data_file in data_files {
            let mut deletes = Vec::with_capacity(data_file.deletes.len());
            for r in data_file.deletes {
                let pooled = delete_files.get(r.df).ok_or_else(|| {
                    format!(
                        "delete reference df index {} is out of range (the deleteFiles pool holds \
                         {} entries)",
                        r.df,
                        delete_files.len()
                    )
                })?;
                deletes.push(ResolvedDelete {
                    path: pooled.path.clone(),
                    size: pooled.size,
                    delete_type: pooled.delete_type,
                    format: pooled.format,
                    offset: r.offset,
                    length: r.length,
                });
            }
            out.push(FileEntry {
                path: data_file.path,
                size: data_file.size,
                deletes,
            });
        }
        Ok(out)
    }
}

/// serde adapter that serializes a `Vec<FileEntry>` as the normalized
/// `{deleteFiles, dataFiles}` [`FileSet`] object and deserializes it back,
/// resolving the interned pool. Used via `#[serde(with = "file_set_serde")]` on
/// the `files` field of [`ScanSpec`] and [`JoinSpec`].
pub(crate) mod file_set_serde {
    use super::{FileEntry, FileSet};
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    pub(crate) fn serialize<S: Serializer>(
        files: &[FileEntry],
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        FileSet::from_entries(files).serialize(serializer)
    }

    pub(crate) fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Vec<FileEntry>, D::Error> {
        FileSet::deserialize(deserializer)?
            .into_entries()
            .map_err(serde::de::Error::custom)
    }
}

/// A fully-resolved delete reference applied to a data file.
///
/// Carries the pooled delete file's identity (`path`, `size`, `delete_type`,
/// `format`) plus, for a deletion vector, the blob's `offset`/`length` within its
/// Puffin container. This is the ergonomic Rust shape both the adapter (producer)
/// and the scan (consumer) work with; the `df`-indexed interning is a pure wire
/// concern handled by [`FileSet`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedDelete {
    pub path: String,
    pub size: u64,
    pub delete_type: DeleteType,
    pub format: DeleteFormat,
    pub offset: Option<u64>,
    pub length: Option<u64>,
}

impl ResolvedDelete {
    /// A Parquet positional-delete file reference (no blob coordinates).
    pub fn position(path: impl Into<String>, size: u64) -> Self {
        ResolvedDelete {
            path: path.into(),
            size,
            delete_type: DeleteType::PosDel,
            format: DeleteFormat::Parquet,
            offset: None,
            length: None,
        }
    }

    /// A v3 deletion-vector blob reference inside a Puffin container.
    pub fn deletion_vector(path: impl Into<String>, size: u64, offset: u64, length: u64) -> Self {
        ResolvedDelete {
            path: path.into(),
            size,
            delete_type: DeleteType::Dv,
            format: DeleteFormat::Puffin,
            offset: Some(offset),
            length: Some(length),
        }
    }
}

/// One per-shard scanned data file: a data file's path and byte size, plus the
/// resolved delete references (positional deletes and/or a deletion vector) that
/// must be applied when reading it.
///
/// The Rust-level API is a plain struct so callers never pattern-match a bare
/// tuple to reach the delete list. Serialization goes through [`FileSet`] (see
/// `file_set_serde`), which interns each physical delete file once per shard.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileEntry {
    /// Path to the data file, relative to the file set's table root when
    /// non-empty and the file lives under it, otherwise an absolute URI (S3
    /// or s3a).
    pub path: String,
    /// Byte size, used to build the file's `ObjectMeta` without an
    /// object-store HEAD.
    pub size: u64,
    /// Delete files that must be applied when reading this data file. Empty (the
    /// default for delete-free entries) means the file is read as-is.
    pub deletes: Vec<ResolvedDelete>,
}

impl FileEntry {
    /// A data-file entry with no associated delete files — the common case.
    pub fn new(path: impl Into<String>, size: u64) -> Self {
        FileEntry {
            path: path.into(),
            size,
            deletes: Vec::new(),
        }
    }

    /// A data-file entry with its associated resolved delete refs.
    pub fn with_deletes(path: impl Into<String>, size: u64, deletes: Vec<ResolvedDelete>) -> Self {
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

    /// Projected columns in order. Empty means "all columns" (no projection push).
    /// Each entry is either a bare column reference or a rendered scalar expression
    /// (see [`ProjectionItem`]).
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

    /// Declared Exasol EMITS type string for each output column.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub emit_exa_types: Vec<String>,

    /// Full logical schema of the Iceberg table at query time.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub logical_schema: Vec<LogicalField>,

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

/// The scan specification passed from the adapter to the scan SET UDF.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ScanSpec {
    /// The Iceberg table's root location (`table.metadata().location()`), used
    /// to reconstruct absolute file paths from per-shard relative paths.
    ///
    /// An empty string (the default) means every entry in `files` is already
    /// absolute — either a legacy payload that predates this field, or a file
    /// that does not live under the table root.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub table_root: String,

    /// Explicit list of assigned Parquet files, each carrying its byte size
    /// and its associated delete refs (positional deletes and/or a deletion
    /// vector, if any). `path` is relative to `table_root` when non-empty and
    /// the file lives under it, otherwise an absolute URI (S3 or s3a). The scan
    /// UDF registers ONLY these files — no catalog discovery — and uses `size`
    /// to build each file's `ObjectMeta` without an object-store HEAD.
    /// Serialized as the normalized `{deleteFiles, dataFiles}` [`FileSet`]
    /// object (an interned delete-file pool plus `df`-indexed references).
    #[serde(with = "file_set_serde")]
    pub files: Vec<FileEntry>,

    /// Projected columns in order. Empty means "all columns" (no projection push).
    /// Each entry is either a bare column reference or a rendered scalar expression
    /// (see [`ProjectionItem`]).
    pub projection: Vec<ProjectionItem>,

    /// DataFusion SQL WHERE predicate fragment, already translated.
    /// None means no filter pushdown (Exasol keeps the predicate for correctness).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub filter: Option<String>,

    /// Row limit. None means no LIMIT pushdown.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<u64>,

    /// ORDER BY sort keys for a pushed-down ordered top-N scan, in order. Empty
    /// (the default) means no ordering pushdown (row scan or aggregate); absent
    /// from JSON when empty so pre-existing scan specs are backward-compatible.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub order_by: Vec<SortKey>,

    /// Ordered list of aggregate functions to compute as node-local partial
    /// results. `None` (the default) means row scanning; absent from JSON when
    /// serialized so pre-existing scan specs are backward-compatible.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub aggregates: Option<Vec<AggregatePlan>>,

    /// Rendered DataFusion SQL fragments for each GROUP BY key, in order.
    /// `None` means no GROUP BY pushdown (single-group or row scan).
    /// Present only for grouped aggregate scans.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub group_keys: Option<Vec<String>>,

    /// Declared Exasol EMITS type string for each output column, positionally
    /// aligned with the row-scan projection. The scan coerces each emitted Arrow
    /// column to the type this ExaType accepts (via `emit_batch`'s strict feed)
    /// before emitting. Populated by the adapter from the SAME types it writes
    /// into the EMITS clause. Empty (the default) for aggregate scans — which use
    /// the freely-coercing Value emit path — and for specs that predate this
    /// field (backward-compatible).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub emit_exa_types: Vec<String>,

    /// Full logical schema of the Iceberg table at query time: every column
    /// (not just the projected subset), each carrying its Iceberg field-id,
    /// current logical name, Arrow type tag, and nullability.
    ///
    /// The VS adapter populates this once at `resolve_file_list` from
    /// `table.metadata().current_schema()`. The scan UDF uses it to build the
    /// logical Arrow schema and install a `FieldIdExprAdapter` so column binding
    /// is field-id-first (name fallback) — correct across Iceberg schema evolution
    /// (renames, drops, nullable additions).
    ///
    /// Absent (empty, the default) for specs that predate this field; the scan
    /// UDF falls back to first-file schema inference (backward-compatible).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub logical_schema: Vec<LogicalField>,

    /// Broadcast (dimension) side of a pushed-down inner equi-join, or `None` (the
    /// default) for a plain single-table scan. Shard-invariant, so it round-trips
    /// through the [`CommonScanSpec`] on the split/merge path. Absent from JSON when
    /// `None`, so every pre-existing non-join spec deserializes unchanged. The fact
    /// side stays in `files`; the dimension side's files live in [`JoinSpec::files`],
    /// so the two file lists never collide. See [`JoinSpec`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub join: Option<JoinSpec>,

    pub storage: StorageProps,

    /// DataFusion `target_partitions` for this scan instance.
    /// Controls the number of logical partitions DataFusion creates internally.
    /// Defaults to 1 (no intra-instance partitioning) so the cluster-level shard
    /// fan-out is the sole source of parallelism and nodes are not oversubscribed.
    /// Old specs that lack this field deserialize to 1 (backward-compatible).
    #[serde(default = "default_one_usize")]
    pub df_target_partitions: usize,

    /// DataFusion `batch_size` (rows per Arrow RecordBatch) for this scan instance.
    /// Controls the granularity of DataFusion's internal execution batches.
    /// Defaults to 8192 (DataFusion's own default).
    /// Old specs that lack this field deserialize to 8192 (backward-compatible).
    #[serde(default = "default_batch_size")]
    pub df_batch_size: usize,

    /// Number of Tokio worker threads for the scan runtime.
    /// When 1 (the default), `new_current_thread()` is used (one OS thread).
    /// When > 1, `new_multi_thread().worker_threads(n)` is used.
    /// Old specs that lack this field deserialize to 1 (backward-compatible).
    #[serde(default = "default_one_usize")]
    pub df_threads_per_udf: usize,

    /// Fraction of the net per-instance budget given to the DataFusion memory pool.
    /// Net budget = per-instance RSS limit − container overhead. Old specs that lack
    /// this field deserialize to 0.6 (backward-compatible).
    #[serde(default = "default_memory_pool_fraction")]
    pub memory_pool_fraction: f64,

    /// Fixed container/binary RSS overhead (MB) subtracted from the per-instance
    /// limit before applying `memory_pool_fraction`. Old specs that lack this field
    /// deserialize to 200 (backward-compatible).
    #[serde(default = "default_instance_overhead_mb")]
    pub instance_overhead_mb: u64,

    /// Connection-concurrency budget for the scan's S3-compatible object store:
    /// the number of concurrent connections held warm per host, independent of
    /// the CPU thread/partition budget (`df_target_partitions`/`df_threads_per_udf`).
    /// Old specs that lack this field deserialize to a conservative built-in
    /// default (backward-compatible), clamped to at least 1.
    #[serde(default = "default_s3_max_connections")]
    pub s3_max_connections: usize,
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
        CommonScanSpec {
            table_root: self.table_root.clone(),
            projection: self.projection.clone(),
            filter: self.filter.clone(),
            limit: self.limit,
            order_by: self.order_by.clone(),
            aggregates: self.aggregates.clone(),
            group_keys: self.group_keys.clone(),
            emit_exa_types: self.emit_exa_types.clone(),
            logical_schema: self.logical_schema.clone(),
            join: self.join.clone(),
            storage: self.storage.clone(),
            df_target_partitions: self.df_target_partitions,
            df_batch_size: self.df_batch_size,
            df_threads_per_udf: self.df_threads_per_udf,
            memory_pool_fraction: self.memory_pool_fraction,
            instance_overhead_mb: self.instance_overhead_mb,
            s3_max_connections: self.s3_max_connections,
        }
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
        Self {
            table_root: common.table_root,
            files,
            projection: common.projection,
            filter: common.filter,
            limit: common.limit,
            order_by: common.order_by,
            aggregates: common.aggregates,
            group_keys: common.group_keys,
            emit_exa_types: common.emit_exa_types,
            logical_schema: common.logical_schema,
            join: common.join,
            storage: common.storage,
            df_target_partitions: common.df_target_partitions,
            df_batch_size: common.df_batch_size,
            df_threads_per_udf: common.df_threads_per_udf,
            memory_pool_fraction: common.memory_pool_fraction,
            instance_overhead_mb: common.instance_overhead_mb,
            s3_max_connections: common.s3_max_connections,
        }
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

    /// Serialize a per-shard files list to the normalized `{deleteFiles,
    /// dataFiles}` JSON object carried in the UDF's second argument: an interned
    /// `deleteFiles` pool plus `dataFiles` entries with `df`-indexed delete
    /// references (see [`FileSet`]). Paired with `files_from_json`.
    pub fn files_json(files: &[FileEntry]) -> String {
        serde_json::to_string(&FileSet::from_entries(files))
            .expect("files list serialization is infallible")
    }

    /// Deserialize a per-shard files list from the UDF's second argument (the
    /// normalized `{deleteFiles, dataFiles}` object), resolving the interned pool.
    ///
    /// Returns an error that does NOT include the raw input.
    pub fn files_from_json(s: &str) -> Result<Vec<FileEntry>, String> {
        let set: FileSet = serde_json::from_str(s).map_err(|e| {
            // Do not echo `s`. A data error can quote the input in `e`'s Display,
            // so build the message from structural fields only.
            format!(
                "scan files deserialization failed ({:?} at line {}, column {})",
                e.classify(),
                e.line(),
                e.column()
            )
        })?;
        set.into_entries()
            .map_err(|e| format!("scan files deserialization failed: {e}"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_spec() -> ScanSpec {
        ScanSpec {
            table_root: "s3://warehouse/db/table".into(),
            files: vec![
                FileEntry::new("data/part-00000.parquet", 1024),
                FileEntry::new("data/part-00001.parquet", 2048),
            ],
            projection: vec!["id".into(), "name".into()],
            filter: Some("(\"ID\" > 10)".into()),
            limit: Some(100),
            order_by: Vec::new(),
            aggregates: None,
            group_keys: None,
            emit_exa_types: Vec::new(),
            logical_schema: Vec::new(),
            join: None,
            storage: StorageProps {
                endpoint: "http://minio:9000".into(),
                region: "us-east-1".into(),
                access_key: "minioadmin".into(),
                secret_key: "minioadmin".into(),
                session_token: None,
                allow_http: true,
                path_style: true,
            },
            df_target_partitions: 1,
            df_batch_size: 8192,
            df_threads_per_udf: 1,
            memory_pool_fraction: 0.6,
            instance_overhead_mb: 200,
            s3_max_connections: 8,
        }
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

        // The wire form is the normalized `{dataFiles: [...]}` object (no deletes
        // ⇒ no deleteFiles pool key, deletes omitted on each data file).
        assert!(
            json.contains(
                r#""files":{"dataFiles":[{"path":"data/part-00000.parquet","size":1024},{"path":"data/part-00001.parquet","size":2048}]}"#
            ),
            "files must serialize as the normalized dataFiles object: {json}"
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
        assert_eq!(back.table_root, "s3://warehouse/db/table");
        assert_eq!(back.projection, vec!["id", "name"]);
        assert_eq!(back.filter.as_deref(), Some("(\"ID\" > 10)"));
        assert_eq!(back.limit, Some(100));

        // Credentials survive the round-trip (they must reach the scan UDF).
        assert_eq!(back.storage.endpoint, "http://minio:9000");
        assert_eq!(back.storage.access_key, "minioadmin");
        assert_eq!(back.storage.secret_key, "minioadmin");
        assert!(back.storage.path_style);
        assert!(back.storage.allow_http);
    }

    #[test]
    fn optional_fields_omitted_when_none() {
        let mut spec = sample_spec();
        spec.filter = None;
        spec.limit = None;
        spec.storage.session_token = None;
        spec.aggregates = None;
        spec.group_keys = None;
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
        assert!(row_spec.emit_exa_types.is_empty());
        let row_json = row_spec.to_json();
        assert!(
            !row_json.contains("emit_exa_types"),
            "empty emit_exa_types must be absent from JSON: {row_json}"
        );

        // Non-empty: the declared EMITS types survive the round-trip in order.
        let mut spec = sample_spec();
        spec.emit_exa_types = vec![
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
            back.emit_exa_types,
            vec![
                "DECIMAL(20,0)".to_string(),
                "VARCHAR(2000000)".to_string(),
                "DOUBLE PRECISION".to_string()
            ]
        );

        // Legacy payload without the field deserializes to an empty Vec.
        let legacy_json = r#"{
            "files": {"dataFiles": [{"path": "s3://w/f0.parquet", "size": 100}]},
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
            legacy.emit_exa_types.is_empty(),
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
        agg_spec.aggregates = Some(vec![
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
            AggregatePlan {
                kind: AggKind::CountDistinct,
                column: Some("L_SHIPMODE".into()),
                arg_expr: None,
            },
        ]);
        let agg_json = agg_spec.to_json();
        assert!(
            agg_json.contains("aggregates"),
            "aggregate spec must carry the aggregates field: {agg_json}"
        );

        let back = ScanSpec::from_json(&agg_json).unwrap();
        let plans = back.aggregates.expect("aggregates must survive round-trip");
        assert_eq!(plans.len(), 7);
        assert_eq!(plans[0].kind, AggKind::Count);
        assert_eq!(plans[0].column, None);
        assert_eq!(plans[1].kind, AggKind::CountCol);
        assert_eq!(plans[1].column.as_deref(), Some("ID"));
        assert_eq!(plans[2].kind, AggKind::Sum);
        assert_eq!(plans[3].kind, AggKind::Min);
        assert_eq!(plans[4].kind, AggKind::Max);
        assert_eq!(plans[5].kind, AggKind::Avg);
        assert_eq!(plans[5].column.as_deref(), Some("AMOUNT"));
        assert_eq!(plans[6].kind, AggKind::CountDistinct);
        assert_eq!(plans[6].column.as_deref(), Some("L_SHIPMODE"));
    }

    /// Task 1.1: `AggregatePlan.arg_expr` round-trips through JSON, is omitted from the
    /// wire form when `None` (backward-compatible with bare-column plans), and a plan
    /// carrying an expression argument survives the round-trip alongside `CountDistinct`.
    #[test]
    fn arg_expr_round_trips_and_omitted_when_none() {
        // A bare-column plan (arg_expr: None) must not carry the key at all.
        let mut agg_spec = sample_spec();
        agg_spec.aggregates = Some(vec![AggregatePlan {
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
        assert_eq!(back.aggregates.unwrap()[0].arg_expr, None);

        // An expression-argument plan carries the rendered SQL fragment and round-trips.
        let mut expr_spec = sample_spec();
        expr_spec.aggregates = Some(vec![
            AggregatePlan {
                kind: AggKind::Sum,
                column: None,
                arg_expr: Some("LENGTH(\"L_COMMENT\")".into()),
            },
            AggregatePlan {
                kind: AggKind::CountDistinct,
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
        let plans = back.aggregates.expect("aggregates must survive round-trip");
        assert_eq!(plans.len(), 2);
        assert_eq!(plans[0].kind, AggKind::Sum);
        assert_eq!(plans[0].column, None);
        assert_eq!(plans[0].arg_expr.as_deref(), Some("LENGTH(\"L_COMMENT\")"));
        assert_eq!(plans[1].kind, AggKind::CountDistinct);
        assert_eq!(plans[1].arg_expr, None);

        // A legacy aggregate payload (predating arg_expr) deserializes with it defaulting
        // to None — bare-column plans serialized before this field existed still parse.
        let legacy_json = r#"{
            "files": {"dataFiles": [{"path": "s3://w/f0.parquet", "size": 100}]},
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
        let legacy_plans = legacy.aggregates.expect("legacy aggregates must parse");
        assert_eq!(
            legacy_plans[0].arg_expr, None,
            "missing arg_expr must default to None (backward-compat)"
        );
    }

    /// Task B1: `order_by` round-trips through JSON, is omitted from the wire form
    /// when empty (backward-compatible with every pre-existing spec shape), and a
    /// legacy JSON payload with no `order_by` key deserializes to an empty list.
    #[test]
    fn order_by_round_trips_and_defaults_to_empty() {
        // Empty (default): the field is omitted from serialized JSON.
        let row_spec = sample_spec();
        assert!(row_spec.order_by.is_empty());
        let row_json = row_spec.to_json();
        assert!(
            !row_json.contains("order_by"),
            "empty order_by must be absent from JSON: {row_json}"
        );

        // Non-empty: sort keys survive the round-trip, in order, with direction
        // and NULL placement intact.
        let mut spec = sample_spec();
        spec.order_by = vec![
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
        assert_eq!(back.order_by, spec.order_by);
        assert_eq!(back.order_by.len(), 2);
        assert_eq!(back.order_by[0].column, "L_EXTENDEDPRICE");
        assert!(!back.order_by[0].ascending);
        assert!(back.order_by[0].nulls_last);
        assert_eq!(back.order_by[1].column, "L_ORDERKEY");
        assert!(back.order_by[1].ascending);
        assert!(!back.order_by[1].nulls_last);

        // Full-spec equality also holds (order_by participates in ScanSpec's PartialEq).
        assert_eq!(back, spec);

        // The split (to_common) / merge (from_parts) path threads order_by through.
        let common = spec.to_common();
        assert_eq!(common.order_by, spec.order_by);
        let merged = ScanSpec::from_parts(common, spec.files.clone());
        assert_eq!(merged.order_by, spec.order_by);

        // A legacy payload without the field deserializes to an empty Vec.
        let legacy_json = r#"{
            "files": {"dataFiles": [{"path": "s3://w/f0.parquet", "size": 100}]},
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
            legacy.order_by.is_empty(),
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
        grouped_spec.group_keys = Some(vec![
            "\"REGION\"".to_string(),
            "YEAR(\"EVENT_DATE\")".to_string(),
        ]);
        let grouped_json = grouped_spec.to_json();
        assert!(
            grouped_json.contains("group_keys"),
            "grouped spec must carry group_keys field: {grouped_json}"
        );

        let back = ScanSpec::from_json(&grouped_json).unwrap();
        let keys = back.group_keys.expect("group_keys must survive round-trip");
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
        spec.logical_schema = vec![
            LogicalField {
                field_id: 1,
                name: "id".to_string(),
                arrow_type: "int32".to_string(),
                nullable: false,
            },
            LogicalField {
                field_id: 2,
                name: "rating".to_string(),
                arrow_type: "float64".to_string(),
                nullable: true,
            },
            LogicalField {
                field_id: 3,
                name: "label".to_string(),
                arrow_type: "utf8".to_string(),
                nullable: true,
            },
            LogicalField {
                field_id: 4,
                name: "ts".to_string(),
                arrow_type: "timestamp_us".to_string(),
                nullable: true,
            },
            LogicalField {
                field_id: 5,
                name: "amount".to_string(),
                arrow_type: "decimal128(18,4)".to_string(),
                nullable: false,
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
        let fields = &back.logical_schema;
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
        assert!(row_spec.logical_schema.is_empty());
        let row_json = row_spec.to_json();
        assert!(
            !row_json.contains("logical_schema"),
            "empty logical_schema must be absent from JSON: {row_json}"
        );

        // A legacy payload without the field deserializes to an empty Vec.
        let legacy_json = r#"{
            "files": {"dataFiles": [{"path": "s3://w/f0.parquet", "size": 100}]},
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
            legacy.logical_schema.is_empty(),
            "missing logical_schema must default to empty (backward-compat)"
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
        spec.df_target_partitions = 4;
        spec.df_threads_per_udf = 2;
        let json = spec.to_json();
        let back = ScanSpec::from_json(&json).unwrap();
        assert_eq!(
            back.df_target_partitions, 4,
            "df_target_partitions must survive round-trip"
        );
        assert_eq!(
            back.df_threads_per_udf, 2,
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
            "files": {"dataFiles": [{"path": "s3://w/f0.parquet", "size": 100}]},
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
            legacy.df_target_partitions, 1,
            "missing df_target_partitions must default to 1 (backward-compat)"
        );
        assert_eq!(
            legacy.df_threads_per_udf, 1,
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
        spec.df_batch_size = 4096;
        let json = spec.to_json();
        let back = ScanSpec::from_json(&json).unwrap();
        assert_eq!(
            back.df_batch_size, 4096,
            "df_batch_size must survive round-trip"
        );

        // 2. The field is present in the serialized JSON.
        assert!(
            json.contains("df_batch_size"),
            "serialized JSON must carry df_batch_size: {json}"
        );

        // 3. A legacy payload without df_batch_size deserializes to 8192.
        let legacy_json = r#"{
            "files": {"dataFiles": [{"path": "s3://w/f0.parquet", "size": 100}]},
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
            legacy.df_batch_size, 8192,
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
        spec.memory_pool_fraction = 0.5;
        spec.instance_overhead_mb = 256;
        let json = spec.to_json();
        let back = ScanSpec::from_json(&json).unwrap();
        assert_eq!(
            back.memory_pool_fraction, 0.5,
            "memory_pool_fraction must survive round-trip"
        );
        assert_eq!(
            back.instance_overhead_mb, 256,
            "instance_overhead_mb must survive round-trip"
        );

        // 2. Legacy payload without these fields → defaults 0.6 / 200.
        let legacy_json = r#"{
            "files": {"dataFiles": [{"path": "s3://w/f0.parquet", "size": 100}]},
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
            legacy.memory_pool_fraction, 0.6,
            "missing memory_pool_fraction must default to 0.6 (backward-compat)"
        );
        assert_eq!(
            legacy.instance_overhead_mb, 200,
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
        spec.s3_max_connections = 32;
        let json = spec.to_json();
        let back = ScanSpec::from_json(&json).unwrap();
        assert_eq!(
            back.s3_max_connections, 32,
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
            "files": {"dataFiles": [{"path": "s3://w/f0.parquet", "size": 123}]},
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
            legacy.s3_max_connections,
            default_s3_max_connections(),
            "missing s3_max_connections must default to the built-in budget (backward-compat)"
        );
        assert!(
            legacy.s3_max_connections >= 1,
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
            merged.s3_max_connections, 32,
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

        // The per-shard files list is the normalized `{dataFiles: [...]}` object.
        assert_eq!(
            files_json,
            r#"{"dataFiles":[{"path":"data/part-00000.parquet","size":1024},{"path":"data/part-00001.parquet","size":2048}]}"#
        );

        // The common blob round-trips on its own.
        let common_back = CommonScanSpec::from_json(&common_json).unwrap();
        assert_eq!(common_back, original.to_common());
        assert_eq!(common_back.table_root, "s3://warehouse/db/table");

        // from_parts_json reconstitutes a spec equal to the pre-split original,
        // with table_root reattached from the common blob and files as tuples.
        let reconstituted = ScanSpec::from_parts_json(&common_json, &files_json).unwrap();
        assert_eq!(reconstituted, original);
        assert_eq!(reconstituted.table_root, "s3://warehouse/db/table");
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
        assert_eq!(spec.table_root, "s3://warehouse/db/table");
        let json = spec.to_json();
        assert!(
            json.contains(r#""table_root":"s3://warehouse/db/table""#),
            "non-empty table_root must appear in JSON: {json}"
        );
        let back = ScanSpec::from_json(&json).unwrap();
        assert_eq!(back.table_root, "s3://warehouse/db/table");

        let common = spec.to_common();
        let common_json = common.to_json();
        assert!(
            common_json.contains(r#""table_root":"s3://warehouse/db/table""#),
            "non-empty table_root must appear in the common blob: {common_json}"
        );

        // An empty table_root is omitted from serialized JSON (skip_serializing_if).
        let mut rootless = sample_spec();
        rootless.table_root = String::new();
        let rootless_json = rootless.to_json();
        assert!(
            !rootless_json.contains("table_root"),
            "empty table_root must be absent from JSON: {rootless_json}"
        );

        // A legacy full-spec payload without table_root deserializes to empty
        // (all file paths in `files` are then absolute, per field semantics).
        let legacy_json = r#"{
            "files": {"dataFiles": [{"path": "s3://w/f0.parquet", "size": 100}]},
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
            legacy.table_root, "",
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
        assert_eq!(reconstituted.table_root, "");
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
        assert!(spec.join.is_none());
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
            "files": {"dataFiles": [{"path": "s3://w/f0.parquet", "size": 100}]},
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
            legacy.join.is_none(),
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

    /// Task 2.A.2: a data file with no deletes reconstitutes in the compact form —
    /// an empty `deleteFiles` pool (omitted from JSON) and each `dataFiles` entry
    /// with its `deletes` array omitted. The round-trip through `files_json` /
    /// `files_from_json` (the UDF-boundary helpers) and through the full ScanSpec
    /// yields delete-free entries.
    #[test]
    fn no_deletes_reconstitutes_compact_form() {
        let files = vec![
            FileEntry::new("s3://w/f0.parquet", 100),
            FileEntry::new("s3://w/f1.parquet", 200),
        ];
        let json = ScanSpec::files_json(&files);
        // The compact form carries no deleteFiles pool and no per-file deletes key.
        assert_eq!(
            json,
            r#"{"dataFiles":[{"path":"s3://w/f0.parquet","size":100},{"path":"s3://w/f1.parquet","size":200}]}"#
        );
        assert!(
            !json.contains("deleteFiles"),
            "empty pool must be omitted: {json}"
        );
        assert!(
            !json.contains("deletes"),
            "empty deletes must be omitted: {json}"
        );

        let back = ScanSpec::files_from_json(&json).unwrap();
        assert_eq!(back, files);
        assert!(back.iter().all(|f| f.deletes.is_empty()));

        // A whole ScanSpec payload with the compact files object also round-trips.
        let spec_json = r#"{
            "files": {"dataFiles": [{"path": "s3://w/f0.parquet", "size": 100}]},
            "projection": [],
            "storage": {
                "endpoint": "http://minio:9000",
                "region": "us-east-1",
                "access_key": "k",
                "secret_key": "s"
            }
        }"#;
        let spec = ScanSpec::from_json(spec_json).unwrap();
        assert_eq!(spec.files, vec![FileEntry::new("s3://w/f0.parquet", 100)]);
        assert!(spec.files[0].deletes.is_empty());
    }

    /// Task 2.A.2: the `deleteFiles` pool round-trips (path/size/type/format for
    /// each mechanism) and a positional-delete reference carries no offset/length.
    #[test]
    fn pool_round_trips() {
        let files = vec![FileEntry::with_deletes(
            "s3://w/f0.parquet",
            100,
            vec![ResolvedDelete::position("s3://w/deletes/d0.parquet", 50)],
        )];
        let json = ScanSpec::files_json(&files);
        assert!(
            json.contains(r#""type":"POS_DEL""#),
            "pool type SCREAMING_SNAKE: {json}"
        );
        assert!(
            json.contains(r#""format":"PARQUET""#),
            "pool format SCREAMING_SNAKE: {json}"
        );
        assert!(
            !json.contains("offset"),
            "POS_DEL ref carries no offset: {json}"
        );
        assert!(
            !json.contains("length"),
            "POS_DEL ref carries no length: {json}"
        );
        assert!(
            json.contains(r#""deletes":[{"df":0}]"#),
            "df-indexed ref: {json}"
        );

        let back = ScanSpec::files_from_json(&json).unwrap();
        assert_eq!(back, files);
    }

    /// Task 2.A.2: a partition-granularity positional-delete file referenced by two
    /// data files appears EXACTLY ONCE in the `deleteFiles` pool, and each
    /// referencing data file's `df` index resolves back to that one pooled entry.
    #[test]
    fn interned_pool_dedups_and_resolves_df() {
        let shared = ResolvedDelete::position("s3://w/deletes/shared.parquet", 80);
        let files = vec![
            FileEntry::with_deletes("s3://w/f0.parquet", 100, vec![shared.clone()]),
            FileEntry::with_deletes("s3://w/f1.parquet", 200, vec![shared.clone()]),
        ];

        // On the wire the shared delete file is interned once and both data files
        // reference df 0.
        let set = FileSet::from_entries(&files);
        assert_eq!(
            set.delete_files.len(),
            1,
            "shared delete file interned once"
        );
        assert_eq!(set.data_files[0].deletes[0].df, 0);
        assert_eq!(set.data_files[1].deletes[0].df, 0);

        let json = ScanSpec::files_json(&files);
        assert_eq!(
            json.matches("shared.parquet").count(),
            1,
            "the shared delete path must appear exactly once on the wire: {json}"
        );

        // Reconstitution resolves the df index back to the same pooled delete on
        // both data files.
        let back = ScanSpec::files_from_json(&json).unwrap();
        assert_eq!(back, files);
        assert_eq!(back[0].deletes[0].path, "s3://w/deletes/shared.parquet");
        assert_eq!(back[1].deletes[0].path, "s3://w/deletes/shared.parquet");
    }

    /// Task 2.A.2: a deletion-vector reference carries `df` + `offset` + `length`
    /// and reconstitutes with all three intact and no `referenced_data_file`.
    #[test]
    fn reconstitutes_dv_refs() {
        let files = vec![FileEntry::with_deletes(
            "s3://w/f0.parquet",
            100,
            vec![ResolvedDelete::deletion_vector(
                "s3://w/deletes/dv.puffin",
                4096,
                4,
                33,
            )],
        )];
        let json = ScanSpec::files_json(&files);
        assert!(json.contains(r#""type":"DV""#), "DV type: {json}");
        assert!(
            json.contains(r#""format":"PUFFIN""#),
            "PUFFIN format: {json}"
        );
        assert!(
            json.contains(r#""offset":4"#),
            "DV ref carries offset: {json}"
        );
        assert!(
            json.contains(r#""length":33"#),
            "DV ref carries length: {json}"
        );
        assert!(
            !json.contains("referenced"),
            "wire must not carry referenced_data_file: {json}"
        );

        let back = ScanSpec::files_from_json(&json).unwrap();
        assert_eq!(back, files);
        let dv = &back[0].deletes[0];
        assert_eq!(dv.delete_type, DeleteType::Dv);
        assert_eq!(dv.offset, Some(4));
        assert_eq!(dv.length, Some(33));
    }

    /// Task 2.A.2: a mixed shard — one POS_DEL-backed data file, one DV-backed data
    /// file, and one data file referencing BOTH a POS_DEL and a DV — round-trips
    /// with every reference resolving to the correct pooled entry and only the DV
    /// references carrying offset/length.
    #[test]
    fn mixed_pos_and_dv_shard_round_trips() {
        let pos = ResolvedDelete::position("s3://w/deletes/d0.parquet", 50);
        let dv = ResolvedDelete::deletion_vector("s3://w/deletes/dv.puffin", 4096, 4, 33);
        let files = vec![
            FileEntry::with_deletes("s3://w/pos_only.parquet", 100, vec![pos.clone()]),
            FileEntry::with_deletes("s3://w/dv_only.parquet", 200, vec![dv.clone()]),
            FileEntry::with_deletes("s3://w/both.parquet", 300, vec![pos.clone(), dv.clone()]),
        ];

        let set = FileSet::from_entries(&files);
        // Two physical delete files interned once each (POS_DEL + Puffin container).
        assert_eq!(set.delete_files.len(), 2);

        let json = ScanSpec::files_json(&files);
        let back = ScanSpec::files_from_json(&json).unwrap();
        assert_eq!(back, files);

        // POS_DEL-only file: one positional ref, no blob coordinates.
        assert_eq!(back[0].deletes.len(), 1);
        assert_eq!(back[0].deletes[0].delete_type, DeleteType::PosDel);
        assert_eq!(back[0].deletes[0].offset, None);
        // DV-only file: one DV ref with coordinates.
        assert_eq!(back[1].deletes[0].delete_type, DeleteType::Dv);
        assert_eq!(back[1].deletes[0].offset, Some(4));
        // Both: a positional ref (no coords) unioned with a DV ref (coords).
        assert_eq!(back[2].deletes.len(), 2);
        assert_eq!(back[2].deletes[0].delete_type, DeleteType::PosDel);
        assert_eq!(back[2].deletes[0].offset, None);
        assert_eq!(back[2].deletes[1].delete_type, DeleteType::Dv);
        assert_eq!(back[2].deletes[1].offset, Some(4));
    }

    /// Task 2.A.2: an out-of-range `df` index fails loud on reconstitution rather
    /// than silently dropping a delete.
    #[test]
    fn out_of_range_df_index_fails_loud() {
        let json = r#"{"deleteFiles":[],"dataFiles":[{"path":"s3://w/f0.parquet","size":100,"deletes":[{"df":3}]}]}"#;
        let err = ScanSpec::files_from_json(json).unwrap_err();
        assert!(
            err.contains("out of range"),
            "must fail loud on bad df: {err}"
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
        // The dimension side carries its OWN positional-delete file — it reuses the
        // same normalized `{deleteFiles, dataFiles}` shape (interned pool +
        // df-indexed refs) as the per-shard fact side.
        spec.join = Some(JoinSpec {
            table_root: "s3://warehouse/db/dim".into(),
            files: vec![
                FileEntry::with_deletes(
                    "data/dim-00000.parquet",
                    512,
                    vec![ResolvedDelete::position("data/dim-deletes/d0.parquet", 64)],
                ),
                FileEntry::new("data/dim-00001.parquet", 1024),
            ],
            logical_schema: vec![LogicalField {
                field_id: 1,
                name: "d_key".into(),
                arrow_type: "int64".into(),
                nullable: false,
            }],
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
        assert_eq!(common.join, spec.join);
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
            .join
            .expect("join block must survive reconstitution");
        assert_eq!(jb.table_root, "s3://warehouse/db/dim");
        assert_eq!(
            jb.files,
            vec![
                FileEntry::with_deletes(
                    "data/dim-00000.parquet",
                    512,
                    vec![ResolvedDelete::position("data/dim-deletes/d0.parquet", 64)],
                ),
                FileEntry::new("data/dim-00001.parquet", 1024),
            ]
        );
        // The dimension side's own positional delete survived reconstitution.
        assert_eq!(jb.files[0].deletes.len(), 1);
        assert_eq!(jb.files[0].deletes[0].delete_type, DeleteType::PosDel);
        assert_eq!(jb.files[0].deletes[0].path, "data/dim-deletes/d0.parquet");
        assert_eq!(jb.join_type, JoinType::Inner);
        assert_eq!(jb.condition, "\"F_KEY\" = \"D_KEY\"");
        assert_eq!(jb.logical_schema.len(), 1);
        assert_eq!(jb.logical_schema[0].name, "d_key");

        // The struct-level split/merge is equivalent to the JSON round-trip.
        let via_struct = ScanSpec::from_parts(spec.to_common(), spec.files.clone());
        assert_eq!(via_struct, spec);
    }
}
