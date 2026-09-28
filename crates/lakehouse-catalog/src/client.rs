//! The one operation surface the engine uses to reach any catalog kind, and the
//! catalog-neutral metadata types it returns.

use std::future::Future;
use std::pin::Pin;

use exasol_udf_sdk::error::UdfError;
use futures::stream::{self, StreamExt, TryStreamExt};
use iceberg::TableIdent;

use crate::namespace::list_namespace_tables;
use crate::session::{CatalogSession, load_table_schema};
use crate::{CatalogProps, ConnectionCreds};

/// Per-table catalog loads in flight at once on one shared session.
pub(crate) const LOAD_CONCURRENCY: usize = 8;

/// Namespace carried as segments, never a joined dotted string: a segment may
/// itself contain the separator.
#[derive(Debug, Clone, PartialEq)]
pub struct CatalogTableIdent {
    pub namespace: Vec<String>,
    pub name: String,
}

/// `Other` carries the catalog's value verbatim so an unclassified kind is never
/// reported as a base table.
#[derive(Debug, Clone, PartialEq)]
pub enum CatalogTableType {
    Table,
    View,
    Other(String),
}

/// Deliberately not pre-mapped to an Exasol type: the engine owns the single
/// exhaustive mapping.
#[derive(Debug, Clone, PartialEq)]
pub enum ColumnSourceType {
    Iceberg(iceberg::spec::Type),
    /// `precision`/`scale` are the `DECIMAL(p, s)` arguments, `0` for a type taking none.
    Unity {
        type_name: String,
        precision: u32,
        scale: u32,
        type_json: Option<String>,
    },
    /// A scan-spec type tag, not an Arrow `DataType`: this crate must not depend on `arrow`.
    Parquet(String),
}

/// Closed on purpose: a catalog value outside the set is refused where it is read.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TableFormat {
    Iceberg,
    Delta,
    Parquet,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CatalogColumn {
    pub name: String,
    pub source_type: ColumnSourceType,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CatalogTable {
    pub ident: CatalogTableIdent,
    pub table_type: CatalogTableType,
    /// Absent for an entry that has none, such as a view.
    pub storage_location: Option<String>,
    pub format: TableFormat,
    /// Opaque per-table credential scope, handed back to the producing client and
    /// never parsed. Absent when the catalog vends without a per-table scope; an
    /// empty or whitespace-only key counts as absent.
    pub vended_credential_key: Option<String>,
    /// Catalog-declared partition columns in catalog order; empty if the kind declares none.
    pub partition_columns: Vec<String>,
    pub columns: Vec<CatalogColumn>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum SkipReason {
    NotLoadableIcebergTable,
    /// A table named by a scoped load that the catalog no longer holds.
    NotFound,
    /// `detail` names the offending value verbatim, e.g. `table_type=VIEW`.
    NotDeltaBaseTable {
        detail: String,
    },
    NoDataFile,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SkippedTable {
    pub ident: CatalogTableIdent,
    pub reason: SkipReason,
}

#[derive(Debug, Clone, PartialEq, Default)]
pub struct CatalogListing {
    pub tables: Vec<CatalogTable>,
    pub skipped: Vec<SkippedTable>,
}

/// Per-table outcomes in load order: a table, or the reason it was skipped.
impl Extend<Result<CatalogTable, SkippedTable>> for CatalogListing {
    fn extend<I: IntoIterator<Item = Result<CatalogTable, SkippedTable>>>(&mut self, iter: I) {
        for outcome in iter {
            match outcome {
                Ok(table) => self.tables.push(table),
                Err(skipped) => self.skipped.push(skipped),
            }
        }
    }
}

impl FromIterator<Result<CatalogTable, SkippedTable>> for CatalogListing {
    fn from_iter<I: IntoIterator<Item = Result<CatalogTable, SkippedTable>>>(iter: I) -> Self {
        let mut listing = Self::default();
        listing.extend(iter);
        listing
    }
}

/// Boxed futures instead of `async fn` keep the trait dyn-compatible without an
/// `async-trait` dependency.
pub trait CatalogClient: Send + Sync {
    /// Includes tables in descendant namespaces.
    fn list_tables(
        &self,
        namespace: &[String],
    ) -> Pin<Box<dyn Future<Output = Result<CatalogListing, UdfError>> + Send + '_>>;

    fn load_table(
        &self,
        ident: &CatalogTableIdent,
    ) -> Pin<Box<dyn Future<Output = Result<CatalogTable, UdfError>> + Send + '_>>;

    /// Only the named tables, under the same skip contract as `list_tables`; one
    /// the catalog no longer holds is skipped, not an error.
    fn load_tables<'a>(
        &'a self,
        idents: &'a [CatalogTableIdent],
    ) -> Pin<Box<dyn Future<Output = Result<CatalogListing, UdfError>> + Send + 'a>>;
}

pub struct IcebergRestCatalogClient {
    catalog_uri: String,
    creds: ConnectionCreds,
}

impl IcebergRestCatalogClient {
    pub fn new(catalog_uri: String, creds: ConnectionCreds) -> Self {
        Self { catalog_uri, creds }
    }

    async fn session(&self) -> Result<CatalogSession, UdfError> {
        CatalogSession::resolve(&self.catalog_uri, &self.creds.warehouse, &self.creds).await
    }

    async fn resolve_listing(
        &self,
        session: &CatalogSession,
        idents: Vec<CatalogTableIdent>,
    ) -> Result<CatalogListing, UdfError> {
        stream::iter(idents)
            .map(|ident| async move {
                match self.load_on_session(session, &ident).await {
                    Ok(table) => Ok(Ok(table)),
                    Err(err) if is_not_loadable_iceberg_table(&err) => Ok(Err(SkippedTable {
                        ident,
                        reason: SkipReason::NotLoadableIcebergTable,
                    })),
                    Err(err) => Err(UdfError::User(format!(
                        "failed to load table '{}': {err}",
                        dotted_identifier(&ident)
                    ))),
                }
            })
            .buffered(LOAD_CONCURRENCY)
            .try_collect()
            .await
    }

    /// Column names keep their original case; the engine owns case folding. No
    /// credential key: this catalog vends credentials inline with table metadata.
    async fn load_on_session(
        &self,
        session: &CatalogSession,
        ident: &CatalogTableIdent,
    ) -> Result<CatalogTable, UdfError> {
        let catalog = CatalogProps {
            warehouse: self.creds.warehouse.clone(),
            table: dotted_identifier(ident),
        };
        let result = load_table_schema(session, &catalog, &self.creds).await?;

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
            format: TableFormat::Iceberg,
            vended_credential_key: None,
            partition_columns: Vec::new(),
            columns,
        })
    }
}

impl CatalogClient for IcebergRestCatalogClient {
    fn list_tables(
        &self,
        namespace: &[String],
    ) -> Pin<Box<dyn Future<Output = Result<CatalogListing, UdfError>> + Send + '_>> {
        let namespace = namespace.to_vec();
        Box::pin(async move {
            let session = self.session().await?;
            let idents = list_namespace_tables(&session, &namespace, &self.creds).await?;
            self.resolve_listing(&session, idents.iter().map(neutral_ident).collect())
                .await
        })
    }

    fn load_table(
        &self,
        ident: &CatalogTableIdent,
    ) -> Pin<Box<dyn Future<Output = Result<CatalogTable, UdfError>> + Send + '_>> {
        let ident = ident.clone();
        Box::pin(async move {
            let session = self.session().await?;
            self.load_on_session(&session, &ident).await
        })
    }

    fn load_tables<'a>(
        &'a self,
        idents: &'a [CatalogTableIdent],
    ) -> Pin<Box<dyn Future<Output = Result<CatalogListing, UdfError>> + Send + 'a>> {
        Box::pin(async move {
            if idents.is_empty() {
                return Ok(CatalogListing::default());
            }
            let session = self.session().await?;
            self.resolve_listing(&session, idents.to_vec()).await
        })
    }
}

fn neutral_ident(ident: &TableIdent) -> CatalogTableIdent {
    CatalogTableIdent {
        namespace: ident.namespace.as_ref().to_vec(),
        name: ident.name.clone(),
    }
}

/// A segment carrying a dot does not round-trip, so this join is the last step.
fn dotted_identifier(ident: &CatalogTableIdent) -> String {
    let mut parts: Vec<&str> = ident.namespace.iter().map(String::as_str).collect();
    parts.push(&ident.name);
    parts.join(".")
}

/// Matches the prefix minted by `iceberg_io::authed_get_json`, including `": "`,
/// so a body merely containing `404` cannot false-match.
fn is_not_loadable_iceberg_table(err: &UdfError) -> bool {
    matches!(err, UdfError::User(msg) if msg.starts_with("catalog returned HTTP 404: "))
}

#[cfg(test)]
#[path = "client_tests.rs"]
mod tests;
