//! Per-request table resolution: the pushdown path's ONE catalog-kind match, and
//! the ONE thing the pipeline learns about a table.

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
use crate::adapter::parquet_directory::MergeMode;
use crate::scan::{build_admission_limited_store, store_root_url};

#[cfg(test)]
#[path = "scan_resolution_tests.rs"]
mod tests;

/// One pushdown request's table resolver: every table the request touches, in the
/// one shape every table format answers.
///
/// Built ONCE per request, which is what makes a per-table session rebuild
/// inexpressible — the catalog session is resolved INTO the resolver and
/// [`Self::resolve`] takes `&self`, so a two-leg join performs no more catalog
/// authentication round-trips than a single-table scan.
///
/// Deep by design: a [`ResolvedScan`] is the whole of what the pipeline learns
/// about a table, so the single-table path, each join leg, and every aggregate
/// shape resolve identically and none of them names a table format or a catalog
/// kind.
pub(super) struct TableScanResolver<'a> {
    session: RequestSession,
    connection: ConnectionStorage<'a>,
}

/// The request's live catalog session, in the shape its catalog kind resolved
/// into.
///
/// Deliberately NOT the [`CatalogKind`] value: the kind is matched once, in
/// [`TableScanResolver::for_request`], and an already-resolved session is all
/// every later step needs. Carrying the kind alongside it would invite a second
/// match site free to disagree with the first.
enum RequestSession {
    Iceberg(CatalogSession),
    /// Boxed: a Unity Catalog session is several times the size of an Iceberg
    /// one, and a request holds exactly one session either way.
    Unity(Box<UnityCatalogSession>),
    /// No catalog at all: the request's ONE admission-limited object store takes
    /// the session's role, so its limiter bounds the whole request rather than
    /// each leg. `base_path` is the subtree every table root is composed under
    /// and `merge_mode` the footer policy the virtual schema resolved.
    DirectStorage {
        store: Arc<dyn ObjectStore>,
        base_path: String,
        merge_mode: MergeMode,
    },
}

impl<'a> TableScanResolver<'a> {
    /// Resolve this request's catalog session at the pushdown path's ONE
    /// exhaustive [`CatalogKind`] match, so a third catalog kind is a compile
    /// error here rather than a silent fall-through.
    ///
    /// `connection` is the CONNECTION's static storage decision every table this
    /// request resolves is read through.
    ///
    /// `table_identifiers` names every table the request will go on to resolve.
    /// Each is checked against the identifier rule of the kind's OWN table format,
    /// inside that kind's arm and ahead of the session that arm builds — so the
    /// shape decision cannot be taken by another format's rule, and cannot be
    /// skipped by a caller that forgets it. The ordering is load-bearing: the
    /// Iceberg arm resolves its `/v1/config` prefix over the network, so an
    /// identifier checked afterwards would surface a transport error from an
    /// unreachable catalog rather than the parse error it is.
    ///
    /// `props` are the request's merged virtual-schema properties. Only the arm
    /// whose kind declares a property reads one, so no property name is parsed for
    /// a kind that ignores it and the kind-specific configuration stays inside the
    /// one match this seam already owns.
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
                RequestSession::DirectStorage {
                    store,
                    base_path: properties.base_path,
                    merge_mode: MergeMode::for_merge_schema(properties.merge_schema),
                }
            }
        };
        Ok(Self {
            session,
            connection,
        })
    }

    /// `table_identifier`'s table as it stands now: its active files, the storage
    /// they were resolved THROUGH, its logical schema, its table root, its name
    /// mapping, and its partition columns.
    ///
    /// `table_identifier` is the original-cased identifier recorded in `TABLE_MAP`
    /// at create time — the dot-joined catalog identifier under a catalog kind, the
    /// bare directory name under direct storage. `filter_json` is the request's raw
    /// filter, forwarded unchanged so each format prunes by it wherever its own
    /// planning can; `None` prunes nothing.
    pub(super) async fn resolve(
        &self,
        table_identifier: &str,
        filter_json: Option<&Json>,
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
                merge_mode,
            } => {
                let table_root =
                    join_storage_path(base_path, Some(direct_storage_directory(table_identifier)?));
                let reader = format_reader(
                    ScanSource::DirectParquet {
                        store,
                        table_root: &table_root,
                        merge_mode: *merge_mode,
                    },
                    &self.connection,
                )?;
                reader.resolve_scan(filter_json).await
            }
        }
    }
}

/// Recover a direct-storage table's first-level directory name from the identifier
/// recorded in `TABLE_MAP` — the ONE place the direct-storage identifier shape is
/// decided.
///
/// A first-level directory name is the WHOLE identifier: this kind records the bare
/// original-cased directory name, because a directory name may itself contain a dot
/// and a dotted form could not be split back unambiguously.
///
/// Every value that names no first-level directory is refused here rather than
/// composed into a table root: an empty or blank name, a name carrying a path
/// separator, and the two relative-path names. Each would compose a root reaching
/// OUTSIDE the storage base path the operator scoped the virtual schema to.
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

/// Recover a Unity Catalog table's identity from the dot-joined identifier
/// recorded in `TABLE_MAP` — the ONE place the Unity Catalog identifier shape is
/// decided.
///
/// The split is the exact inverse of the join that recorded it, and the Unity
/// Catalog addresses a table by that same dotted full name — the loader re-joins
/// the segments verbatim — so the round trip is lossless and cannot address a
/// different table.
///
/// Both shapes that name no Unity Catalog table are refused here rather than
/// sent to the catalog: an identifier carrying no separator at all recovers an
/// EMPTY namespace, which addresses nothing under `catalog.schema.table`; and an
/// identifier whose last segment is empty names no table, where falling back to
/// the segment before it would resolve a DIFFERENT table.
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
