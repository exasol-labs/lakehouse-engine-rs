use std::collections::HashSet;
use std::sync::Arc;

use exasol_udf_sdk::error::UdfError;
use lakehouse_catalog::{
    CatalogClient, CatalogProps, CatalogSession, CatalogTableIdent, GlueCatalogSession,
    UnityCatalogSession, parse_glue_table_ident, parse_table_ident,
};
use object_store::ObjectStore;
use serde_json::Value as Json;

use super::{ConnectionStorage, ResolvedScan, ScanSource, format_reader};
use crate::adapter::catalog_kind::CatalogKind;
use crate::adapter::direct_storage_properties::{
    join_storage_path, resolve_direct_storage_properties,
};
use crate::adapter::parquet_directory::DirectoryOptions;
use crate::adapter::permission::PermissionCheck;
use crate::scan::{build_admission_limited_store, store_root_url};

#[cfg(test)]
#[path = "scan_resolution_tests.rs"]
mod tests;

/// Built once per request with the catalog session resolved into it, so a
/// multi-leg join costs no more catalog authentication than a single scan.
///
/// Every pushdown shape loads its tables through [`Self::resolve`], so the request's permission
/// check runs here, once, before any table is loaded, and a shape added later cannot skip it.
pub(super) struct TableScanResolver<'a> {
    session: RequestSession,
    connection: ConnectionStorage<'a>,
    admission: TableAdmission,
}

/// Which identifiers [`TableScanResolver::resolve`] may load.
enum TableAdmission {
    /// The virtual schema runs no permission check.
    Unchecked,
    /// Only the identifiers the request's permission check authorized. A kind whose resolution
    /// runs no check authorizes none, so a gated request under it reads no table.
    Authorized(HashSet<String>),
}

impl TableAdmission {
    fn admit(&self, table_identifier: &str) -> Result<(), UdfError> {
        match self {
            Self::Unchecked => Ok(()),
            Self::Authorized(tables) if tables.contains(table_identifier) => Ok(()),
            Self::Authorized(_) => Err(UdfError::User(format!(
                "pushdown: the Lakekeeper permission check did not cover table \
                 '{table_identifier}', so the adapter does not read it"
            ))),
        }
    }
}

/// Deliberately not the [`CatalogKind`]: the kind is matched once in
/// [`TableScanResolver::for_request`], so no second match site can disagree.
enum RequestSession {
    Iceberg(CatalogSession),
    Unity(Box<UnityCatalogSession>),
    Glue(Box<GlueCatalogSession>),
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
    ///
    /// With an enforced `permission`, the Iceberg arm sends the request's one batch-check on the
    /// new session and fails before any `loadTable` unless every identifier is allowed; the
    /// resolver then admits only those identifiers.
    pub(super) async fn for_request(
        kind: CatalogKind,
        catalog_uri: &str,
        connection: ConnectionStorage<'a>,
        table_identifiers: &[&str],
        props: &Json,
        permission: &PermissionCheck,
    ) -> Result<Self, UdfError> {
        let mut authorized = HashSet::new();
        let session = match kind {
            CatalogKind::IcebergRest => {
                for identifier in table_identifiers {
                    parse_table_ident(identifier)?;
                }
                let session = CatalogSession::resolve(
                    catalog_uri,
                    &connection.creds.warehouse,
                    connection.creds,
                )
                .await?;
                if let PermissionCheck::Enforced(gate) = permission {
                    gate.authorize(&session, table_identifiers, connection.creds)
                        .await?;
                    authorized.extend(table_identifiers.iter().map(|id| id.to_string()));
                }
                RequestSession::Iceberg(session)
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
            CatalogKind::Glue => {
                for identifier in table_identifiers {
                    parse_glue_table_ident(identifier)?;
                }
                RequestSession::Glue(Box::new(GlueCatalogSession::new(
                    catalog_uri,
                    connection.storage.clone(),
                    connection.creds.clone(),
                )?))
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
        let admission = match permission {
            PermissionCheck::Off => TableAdmission::Unchecked,
            PermissionCheck::Enforced(_) => TableAdmission::Authorized(authorized),
        };
        Ok(Self {
            session,
            connection,
            admission,
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
        self.admission.admit(table_identifier)?;
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
                    ScanSource::Unity {
                        session: session.as_ref(),
                        table: &table,
                    },
                    &self.connection,
                )?;
                reader.resolve_scan(filter_json).await
            }
            RequestSession::Glue(session) => {
                let table = session
                    .load_table(&parse_glue_table_ident(table_identifier)?)
                    .await?;
                let reader = format_reader(
                    ScanSource::Glue {
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
