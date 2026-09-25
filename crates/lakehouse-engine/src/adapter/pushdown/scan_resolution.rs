use std::sync::Arc;

use exasol_udf_sdk::error::UdfError;
use lakehouse_catalog::{
    CatalogClient, CatalogProps, CatalogSession, CatalogTableIdent, UnityCatalogSession,
    parse_table_ident,
};
use object_store::ObjectStore;
use serde_json::Value as Json;

use super::{ConnectionStorage, ResolvedScan, ScanSource, format_reader};
use crate::adapter::catalog_kind::CatalogKind;
use crate::adapter::direct_storage_properties::{
    join_storage_path, resolve_direct_storage_properties,
};
use crate::adapter::parquet_directory::DirectoryOptions;
use crate::scan::{build_admission_limited_store, store_root_url};

#[cfg(test)]
#[path = "scan_resolution_tests.rs"]
mod tests;

/// Built once per request with the catalog session resolved into it, so a
/// multi-leg join costs no more catalog authentication than a single scan.
pub(super) struct TableScanResolver<'a> {
    session: RequestSession,
    connection: ConnectionStorage<'a>,
}

/// Deliberately not the [`CatalogKind`]: the kind is matched once in
/// [`TableScanResolver::for_request`], so no second match site can disagree.
enum RequestSession {
    Iceberg(CatalogSession),
    Unity(Box<UnityCatalogSession>),
    DirectStorage {
        store: Arc<dyn ObjectStore>,
        base_path: String,
        options: DirectoryOptions,
    },
}

impl<'a> TableScanResolver<'a> {
    /// The pushdown path's one exhaustive [`CatalogKind`] match.
    ///
    /// Every `table_identifiers` entry is validated by its own format's rule before
    /// the session is built: the Iceberg arm contacts the network for `/v1/config`,
    /// so a later check would surface a transport error instead of the parse error.
    pub(super) async fn for_request(
        kind: CatalogKind,
        catalog_uri: &str,
        connection: ConnectionStorage<'a>,
        table_identifiers: &[&str],
        props: &Json,
    ) -> Result<Self, UdfError> {
        let session = match kind {
            CatalogKind::IcebergRest => {
                for identifier in table_identifiers {
                    parse_table_ident(identifier)?;
                }
                RequestSession::Iceberg(
                    CatalogSession::resolve(
                        catalog_uri,
                        &connection.creds.warehouse,
                        connection.creds,
                    )
                    .await?,
                )
            }
            CatalogKind::UnityCatalogNative => {
                for identifier in table_identifiers {
                    unity_table_ident(identifier)?;
                }
                RequestSession::Unity(Box::new(UnityCatalogSession::new(
                    catalog_uri,
                    connection.creds.clone(),
                )))
            }
            CatalogKind::DirectStorage => {
                for identifier in table_identifiers {
                    direct_storage_directory(identifier)?;
                }
                let properties = resolve_direct_storage_properties(props, catalog_uri)?;
                let store = build_admission_limited_store(
                    connection.storage,
                    &store_root_url(&properties.base_path)?,
                    &connection.storage.secret_values(),
                )?;
                let options = properties.directory_options();
                RequestSession::DirectStorage {
                    store,
                    base_path: properties.base_path,
                    options,
                }
            }
        };
        Ok(Self {
            session,
            connection,
        })
    }

    /// `table_identifier` is the original-cased `TABLE_MAP` identifier. `filter_json`
    /// is forwarded unchanged for format-side pruning. `declared_columns` is read only
    /// by a format whose pruning can drop every file carrying a column.
    pub(super) async fn resolve(
        &self,
        table_identifier: &str,
        filter_json: Option<&Json>,
        declared_columns: &[(String, String)],
    ) -> Result<ResolvedScan, UdfError> {
        match &self.session {
            RequestSession::Iceberg(session) => {
                let catalog_props = CatalogProps {
                    warehouse: self.connection.creds.warehouse.clone(),
                    table: table_identifier.to_string(),
                };
                let reader = format_reader(
                    ScanSource::Iceberg {
                        session,
                        catalog_props: &catalog_props,
                    },
                    &self.connection,
                )?;
                reader.resolve_scan(filter_json).await
            }
            RequestSession::Unity(session) => {
                let table = session
                    .load_table(&unity_table_ident(table_identifier)?)
                    .await?;
                let reader = format_reader(
                    ScanSource::UnityDelta {
                        session: session.as_ref(),
                        table: &table,
                    },
                    &self.connection,
                )?;
                reader.resolve_scan(filter_json).await
            }
            RequestSession::DirectStorage {
                store,
                base_path,
                options,
            } => {
                let table_root =
                    join_storage_path(base_path, Some(direct_storage_directory(table_identifier)?));
                let reader = format_reader(
                    ScanSource::DirectParquet {
                        store,
                        table_root: &table_root,
                        options: *options,
                        declared_columns,
                    },
                    &self.connection,
                )?;
                reader.resolve_scan(filter_json).await
            }
        }
    }
}

/// Refuses empty, separator-carrying, or relative-path values, which would compose
/// a table root outside the storage base path.
fn direct_storage_directory(table_identifier: &str) -> Result<&str, UdfError> {
    let refusal = |reason: &str| {
        Err(UdfError::User(format!(
            "pushdown: the recorded catalog identifier '{table_identifier}' names no \
             first-level directory under the storage base path — {reason}; drop and \
             recreate the virtual schema"
        )))
    };
    if table_identifier.trim().is_empty() {
        return refusal("it is empty");
    }
    if table_identifier.contains('/') || table_identifier.contains('\\') {
        return refusal("it carries a path separator");
    }
    if table_identifier == "." || table_identifier == ".." {
        return refusal("it names a relative path rather than a directory");
    }
    Ok(table_identifier)
}

/// An identifier with no separator (empty namespace) or an empty last segment is
/// refused rather than sent to the catalog: falling back to an earlier segment
/// would address a different table.
fn unity_table_ident(table_identifier: &str) -> Result<CatalogTableIdent, UdfError> {
    let Some((namespace, name)) = table_identifier.rsplit_once('.') else {
        return Err(UdfError::User(format!(
            "pushdown: the recorded catalog identifier '{table_identifier}' names no Unity Catalog \
             table — a Unity Catalog table is addressed as 'catalog.schema.table'; drop and \
             recreate the virtual schema"
        )));
    };
    if name.trim().is_empty() {
        return Err(UdfError::User(format!(
            "pushdown: the recorded catalog identifier '{table_identifier}' names no table — \
             its last dot-separated segment is empty; drop and recreate the virtual schema"
        )));
    }
    Ok(CatalogTableIdent {
        namespace: namespace.split('.').map(String::from).collect(),
        name: name.to_string(),
    })
}
