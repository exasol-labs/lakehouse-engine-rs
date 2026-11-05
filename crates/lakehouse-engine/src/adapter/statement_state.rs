//! The adapter's pushdown state for the statement its VM serves: per virtual schema, the catalog
//! client, the tables the statement's permission checks admitted, and the tables it loaded.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::future::Future;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use exasol_udf_sdk::error::UdfError;
use serde_json::Value as Json;
use tokio::runtime::Runtime;
use tokio::sync::OnceCell;

use crate::adapter::pushdown::{LoadedTable, ScanCatalog};
use crate::adapter::{ResolvedConnectionConfig, construct_catalog_client};

#[cfg(test)]
#[path = "statement_state_tests.rs"]
mod tests;

/// Lives as long as the adapter VM. Exasol runs one adapter VM per statement (#583), so no entry
/// outlives its statement.
pub(crate) static STATEMENT_STATES: StatementStates<VirtualSchemaState> = StatementStates::new();

/// States keyed by virtual schema name, because nothing establishes that one VM never serves two
/// virtual schemas of a statement, and each must use its own CONNECTION.
pub(crate) struct StatementStates<T> {
    states: Mutex<BTreeMap<String, Arc<T>>>,
}

impl<T> StatementStates<T> {
    pub(crate) const fn new() -> Self {
        Self {
            states: Mutex::new(BTreeMap::new()),
        }
    }

    /// The state kept under `name`, built on a miss. A failed build keeps nothing. The build runs
    /// outside the lock, because it may wait on the network.
    pub(crate) fn get_or_build<E>(
        &self,
        name: &str,
        build: impl FnOnce() -> Result<T, E>,
    ) -> Result<Arc<T>, E> {
        if let Some(state) = self.locked().get(name) {
            return Ok(Arc::clone(state));
        }
        let built = Arc::new(build()?);
        Ok(Arc::clone(
            self.locked().entry(name.to_string()).or_insert(built),
        ))
    }

    fn locked(&self) -> MutexGuard<'_, BTreeMap<String, Arc<T>>> {
        self.states.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// One virtual schema's state for its statement, with the runtime it was built on.
pub(crate) struct VirtualSchemaState {
    catalog: StatementCatalog,
    /// Kept, and dropped last, because the client's connection pools and every loaded Iceberg
    /// table hold handles to the runtime they were built on.
    runtime: Runtime,
}

impl VirtualSchemaState {
    pub(crate) fn new(runtime: Runtime, catalog: StatementCatalog) -> Self {
        Self { catalog, runtime }
    }

    /// Runs one request's planning on the runtime that built the state's sessions and tables.
    pub(crate) fn block_on<'s, F: Future>(
        &'s self,
        plan: impl FnOnce(&'s StatementCatalog) -> F,
    ) -> F::Output {
        self.runtime.block_on(plan(&self.catalog))
    }
}

/// A virtual schema's catalog as one statement sees it: the client its CONNECTION built, the
/// tables a permission check of the statement admitted, and each table loaded once for every
/// reference. It holds no runtime; the [`VirtualSchemaState`] that owns it keeps one.
pub(crate) struct StatementCatalog {
    connection: ResolvedConnectionConfig,
    client: Box<dyn ScanCatalog>,
    admitted: Mutex<HashSet<String>>,
    loaded: Mutex<HashMap<String, SharedLoad>>,
}

/// One identifier's load, which every reference to the table awaits.
type SharedLoad = Arc<OnceCell<Arc<dyn LoadedTable>>>;

impl StatementCatalog {
    /// Builds `connection`'s catalog client, which issues no request, with no table admitted or
    /// loaded yet.
    pub(crate) fn open(
        connection: ResolvedConnectionConfig,
        props: &Json,
    ) -> Result<Self, UdfError> {
        let client = construct_catalog_client(&connection, props)?;
        Ok(Self {
            connection,
            client,
            admitted: Mutex::default(),
            loaded: Mutex::default(),
        })
    }

    pub(crate) fn connection(&self) -> &ResolvedConnectionConfig {
        &self.connection
    }

    pub(crate) fn client(&self) -> &dyn ScanCatalog {
        self.client.as_ref()
    }

    pub(crate) fn is_admitted(&self, table_identifier: &str) -> bool {
        self.admitted_tables().contains(table_identifier)
    }

    /// The `identifiers` that no permission check of the statement admitted yet, in their order.
    pub(crate) fn unchecked<'i>(&self, identifiers: &[&'i str]) -> Vec<&'i str> {
        let admitted = self.admitted_tables();
        identifiers
            .iter()
            .copied()
            .filter(|identifier| !admitted.contains(*identifier))
            .collect()
    }

    fn admitted_tables(&self) -> MutexGuard<'_, HashSet<String>> {
        self.admitted.lock().unwrap_or_else(PoisonError::into_inner)
    }

    pub(crate) fn admit(&self, identifiers: Vec<String>) {
        self.admitted_tables().extend(identifiers);
    }

    /// The first caller for an identifier loads the table, and every concurrent or later caller
    /// shares that load. A failed load keeps nothing, so the next caller loads again.
    pub(crate) async fn loaded_table(
        &self,
        table_identifier: &str,
    ) -> Result<Arc<dyn LoadedTable>, UdfError> {
        let cell = Arc::clone(
            self.loaded
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .entry(table_identifier.to_string())
                .or_default(),
        );
        let connection = self.connection.connection_storage();
        cell.get_or_try_init(|| self.client.load_for_scan(table_identifier, connection))
            .await
            .map(Arc::clone)
    }
}
