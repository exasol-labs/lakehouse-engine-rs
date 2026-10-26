//! Engine-typed helpers for the tests that drive the planner directly instead of going through
//! Exasol (#443 category C). Kept out of `common/` so the shared harness links no engine code.
// Each test binary compiles this module separately, so helpers used by only one binary look dead.
#![allow(dead_code)]

use crate::common::e2e_harness::persisted_column_notes;
use crate::common::exasol_ws::ExaConn;
use crate::common::stack::{local_stack_connection_password, seaweedfs_url};

use lakehouse_engine::adapter::column_notes::{DeclaredSchema, TableNotes};
use lakehouse_engine::adapter::connection::{ConnectionCreds, catalog_block, storage_block};
use lakehouse_engine::scan::spec::{CatalogProps, StorageBackend};

/// The local-stack CONNECTION as the test process reaches it: host-mapped SeaweedFS.
pub fn local_stack_creds() -> ConnectionCreds {
    let password = local_stack_connection_password();
    ConnectionCreds {
        warehouse: password.warehouse,
        endpoint: seaweedfs_url(),
        region: password.region,
        access_key: password.access_key,
        secret_key: password.secret_key,
        path_style: Some(password.path_style),
        ..Default::default()
    }
}

pub fn local_stack_storage() -> StorageBackend {
    storage_block(&local_stack_creds(), true)
}

pub fn local_stack_catalog(table: &str) -> CatalogProps {
    catalog_block(&local_stack_creds(), table)
}

/// The schema a pushdown over `table` plans from, for a test that drives a reader directly.
pub fn declared_schema_of(conn: &mut ExaConn, vs_name: &str, table: &str) -> DeclaredSchema {
    let notes = persisted_column_notes(conn, vs_name, table);
    TableNotes {
        table,
        columns: notes
            .iter()
            .map(|(column, note)| (column.as_str(), Some(note.as_str())))
            .collect(),
    }
    .declared_schema()
    .unwrap_or_else(|e| panic!("the notes of {vs_name}.{table} parse: {e}"))
}
