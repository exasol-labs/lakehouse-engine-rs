//! The one operation surface the engine uses to reach any catalog kind, and the
//! catalog-neutral metadata types it returns.

use std::future::Future;
use std::pin::Pin;

use exasol_udf_sdk::error::UdfError;
use iceberg::TableIdent;

use crate::namespace::list_namespace_tables;
use crate::session::{CatalogSession, load_table_any_auth};
use crate::{CatalogProps, ConnectionCreds, StorageBackend};

/// A table's identity, carried as namespace SEGMENTS.
///
/// Never a pre-joined dotted string: a segment may itself contain the separator,
/// which neither catalog forbids, so re-splitting a joined identifier is
/// ambiguous.
#[derive(Debug, Clone, PartialEq)]
pub struct CatalogTableIdent {
    pub namespace: Vec<String>,
    pub name: String,
}

/// What the catalog reports an entry to be.
///
/// `Other` carries the catalog's own value verbatim, so an entry kind this engine
/// does not classify is never silently reported as a base table.
#[derive(Debug, Clone, PartialEq)]
pub enum CatalogTableType {
    Table,
    View,
    Other(String),
}

/// A column's type as its SOURCE catalog declares it.
///
/// Deliberately not pre-mapped to an Exasol type: this crate must not name the
/// Exasol delivery mechanism, and the engine keeps one mapping home that matches
/// this exhaustively — so a third source kind is a build failure there.
#[derive(Debug, Clone, PartialEq)]
pub enum ColumnSourceType {
    Iceberg(iceberg::spec::Type),
    /// A Unity Catalog Spark type, fully parameterized: `precision` and `scale`
    /// carry the `DECIMAL(p, s)` arguments and are `0` for a type taking none.
    Unity {
        type_name: String,
        precision: u32,
        scale: u32,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub struct CatalogColumn {
    pub name: String,
    pub source_type: ColumnSourceType,
}

/// One table's metadata in the shape every catalog kind returns.
#[derive(Debug, Clone, PartialEq)]
pub struct CatalogTable {
    pub ident: CatalogTableIdent,
    pub table_type: CatalogTableType,
    /// Absent for an entry that has none, such as a view.
    pub storage_location: Option<String>,
    pub columns: Vec<CatalogColumn>,
}

/// Why a listed entry is not reported as a virtual table.
///
/// Carries no `CatalogKind` value: the client that decides to skip owns the
/// reason, and consumers render wording by matching the reason alone, so no
/// second `CatalogKind` match site is reintroduced downstream.
#[derive(Debug, Clone, PartialEq)]
pub enum SkipReason {
    /// The Iceberg REST catalog listed the identifier, but `loadTable` reported
    /// it is not a loadable Iceberg table — the entry is skipped rather than
    /// failing the whole enumeration.
    NotLoadableIcebergTable,
    /// `detail` is the disqualifier fragment naming the offending value verbatim,
    /// such as `table_type=VIEW` or `data_source_format=ICEBERG`.
    NotDeltaBaseTable { detail: String },
}

/// One entry the catalog listed that the enumeration excluded, with its reason.
#[derive(Debug, Clone, PartialEq)]
pub struct SkippedTable {
    pub ident: CatalogTableIdent,
    pub reason: SkipReason,
}

/// The outcome of enumerating one namespace: the tables that resolved, plus the
/// entries that were excluded and why, which are skipped rather than failing the
/// whole enumeration.
#[derive(Debug, Clone, PartialEq)]
pub struct CatalogListing {
    pub tables: Vec<CatalogTable>,
    pub skipped: Vec<SkippedTable>,
}

/// Enumerating a namespace and loading one table's metadata, for any catalog
/// kind.
///
/// Each method returns a boxed future rather than being an `async fn`, because a
/// native `async fn` in a trait is not dyn-compatible: the engine holds one
/// `Box<dyn CatalogClient>` chosen at a single construction site, and boxing here
/// is what buys that without an `async-trait` dependency this crate is forbidden
/// to declare.
pub trait CatalogClient: Send + Sync {
    /// Every table in `namespace` and its descendants, each fully populated with
    /// its columns, plus the identifiers that were skipped.
    fn list_tables(
        &self,
        namespace: &[String],
    ) -> Pin<Box<dyn Future<Output = Result<CatalogListing, UdfError>> + Send + '_>>;

    /// One named table's metadata.
    fn load_table(
        &self,
        ident: &CatalogTableIdent,
    ) -> Pin<Box<dyn Future<Output = Result<CatalogTable, UdfError>> + Send + '_>>;
}

/// The Iceberg REST implementation of [`CatalogClient`].
///
/// Composes [`CatalogSession`] rather than being one: listing needs `storage`
/// and `creds`, which the session neither holds nor takes, and the session must
/// be built AFTER enumeration so an empty namespace performs no OAuth2 grant.
/// So the client holds only the inputs a session is resolved from and builds the
/// session lazily, exactly once for a whole enumeration.
pub struct IcebergRestCatalogClient {
    catalog_uri: String,
    storage: StorageBackend,
    creds: ConnectionCreds,
}

impl IcebergRestCatalogClient {
    pub fn new(catalog_uri: String, storage: StorageBackend, creds: ConnectionCreds) -> Self {
        Self {
            catalog_uri,
            storage,
            creds,
        }
    }

    /// Resolve a pre-enumerated identifier batch into a listing, building exactly
    /// ONE [`CatalogSession`] for the whole batch and reusing it across every
    /// identifier through [`Self::load_on_session`].
    ///
    /// An empty batch returns immediately, building NO session and performing NO
    /// OAuth2 grant: a namespace the catalog reports as holding no table costs no
    /// catalog contact beyond the enumeration that produced the empty batch, and
    /// a grant failure on a non-empty batch surfaces once, before the per-table
    /// loop, rather than at whichever table resolved first. An identifier the
    /// catalog reports as not a loadable Iceberg table is routed into
    /// [`CatalogListing::skipped`]; every other failure aborts the batch.
    async fn resolve_listing(&self, idents: &[TableIdent]) -> Result<CatalogListing, UdfError> {
        if idents.is_empty() {
            return Ok(CatalogListing {
                tables: Vec::new(),
                skipped: Vec::new(),
            });
        }

        let session =
            CatalogSession::resolve(&self.catalog_uri, &self.creds.warehouse, &self.creds).await?;

        let mut tables = Vec::with_capacity(idents.len());
        let mut skipped = Vec::new();
        for table_ident in idents {
            let ident = neutral_ident(table_ident);
            match self.load_on_session(&session, &ident).await {
                Ok(table) => tables.push(table),
                Err(err) if is_not_loadable_iceberg_table(&err) => skipped.push(SkippedTable {
                    ident,
                    reason: SkipReason::NotLoadableIcebergTable,
                }),
                Err(err) => return Err(err),
            }
        }

        Ok(CatalogListing { tables, skipped })
    }

    /// Load one table's metadata on an ALREADY-built session — the single home
    /// both `list_tables` (reusing one session across a batch) and the trait
    /// `load_table` (building its own) funnel through, so the one-session
    /// guarantee is expressed structurally rather than by convention.
    ///
    /// Returns the table's root location and its `current_schema()` fields as
    /// ordered columns in their ORIGINAL case, each tagged with its Iceberg
    /// source type and left unmapped: the engine owns the single Exasol-mapping
    /// and case-folding home for both catalog kinds.
    async fn load_on_session(
        &self,
        session: &CatalogSession,
        ident: &CatalogTableIdent,
    ) -> Result<CatalogTable, UdfError> {
        let catalog = CatalogProps {
            warehouse: self.creds.warehouse.clone(),
            table: dotted_identifier(ident),
        };
        let result = load_table_any_auth(session, &catalog, &self.creds).await?;

        let storage_location = result.metadata.location().to_string();
        let columns = result
            .metadata
            .current_schema()
            .as_struct()
            .fields()
            .iter()
            .map(|field| CatalogColumn {
                name: field.name.clone(),
                source_type: ColumnSourceType::Iceberg(field.field_type.as_ref().clone()),
            })
            .collect();

        Ok(CatalogTable {
            ident: ident.clone(),
            table_type: CatalogTableType::Table,
            storage_location: Some(storage_location),
            columns,
        })
    }
}

impl CatalogClient for IcebergRestCatalogClient {
    fn list_tables(
        &self,
        namespace: &[String],
    ) -> Pin<Box<dyn Future<Output = Result<CatalogListing, UdfError>> + Send + '_>> {
        // Own the segments before the future is built: the future is bound to
        // `&self`, not to the caller's slice borrow.
        let namespace = namespace.to_vec();
        Box::pin(async move {
            let idents =
                list_namespace_tables(&self.catalog_uri, &namespace, &self.storage, &self.creds)
                    .await?;
            self.resolve_listing(&idents).await
        })
    }

    fn load_table(
        &self,
        ident: &CatalogTableIdent,
    ) -> Pin<Box<dyn Future<Output = Result<CatalogTable, UdfError>> + Send + '_>> {
        let ident = ident.clone();
        Box::pin(async move {
            let session =
                CatalogSession::resolve(&self.catalog_uri, &self.creds.warehouse, &self.creds)
                    .await?;
            self.load_on_session(&session, &ident).await
        })
    }
}

/// The catalog-neutral identifier for an enumerated Iceberg table, carrying its
/// namespace as segments.
fn neutral_ident(ident: &TableIdent) -> CatalogTableIdent {
    CatalogTableIdent {
        namespace: ident.namespace.as_ref().to_vec(),
        name: ident.name.clone(),
    }
}

/// The dot-joined identifier `load_table_any_auth` parses back into namespace and
/// table. Segments are joined verbatim, matching the catalog's own identifier
/// contract (a segment carrying a dot would not round-trip, which is why the
/// neutral identifier carries segments and this join is the last step).
fn dotted_identifier(ident: &CatalogTableIdent) -> String {
    let mut parts: Vec<&str> = ident.namespace.iter().map(String::as_str).collect();
    parts.push(&ident.name);
    parts.join(".")
}

/// Whether `err` is the catalog's "not a loadable Iceberg table" signal — the
/// HTTP 404 the single catalog error site (`iceberg_io::authed_get_json`) mints
/// as `catalog returned HTTP 404: <body>`.
///
/// Matched against the full pinned prefix, including the `": "` separator, so a
/// non-404 response whose body merely contains `404` cannot false-match; every
/// other outcome — a non-404 status, a transport or parse failure — returns
/// `false` so the enumeration aborts loudly, preserving the unreachable-catalog
/// contract.
fn is_not_loadable_iceberg_table(err: &UdfError) -> bool {
    matches!(err, UdfError::User(msg) if msg.starts_with("catalog returned HTTP 404: "))
}

#[cfg(test)]
#[path = "client_tests.rs"]
mod tests;
