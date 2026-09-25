//! The one operation surface the engine uses to reach any catalog kind, and the
//! catalog-neutral metadata types it returns.

use std::future::Future;
use std::pin::Pin;

use exasol_udf_sdk::error::UdfError;
use iceberg::TableIdent;

use crate::namespace::list_namespace_tables;
use crate::session::{CatalogSession, load_table_any_auth};
use crate::{CatalogProps, ConnectionCreds, StorageBackend};

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
    pub columns: Vec<CatalogColumn>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum SkipReason {
    NotLoadableIcebergTable,
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

#[derive(Debug, Clone, PartialEq)]
pub struct CatalogListing {
    pub tables: Vec<CatalogTable>,
    pub skipped: Vec<SkippedTable>,
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
}

/// Builds its [`CatalogSession`] lazily, after enumeration, so an empty namespace
/// performs no OAuth2 grant.
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

    /// One session for the whole batch; none (and no OAuth2 grant) for an empty one.
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
            format: TableFormat::Iceberg,
            vended_credential_key: None,
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
