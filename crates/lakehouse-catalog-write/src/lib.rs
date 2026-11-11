//! Iceberg REST catalog writes for the export: namespace and table creation, staged creation, the
//! commit loop that adds a writer's data files to a table exactly once, and the vended storage
//! credentials that renew themselves inside a writer table's FileIO. Every state-changing request
//! goes through `CatalogSession::post`, and each one the session sends only once is retried here by
//! the operation that can check its outcome.

mod iceberg_write;
mod retry;
mod snapshot_capture;
mod vended_refresh;

#[cfg(test)]
#[path = "test_support_tests.rs"]
mod test_support;

pub use iceberg_write::{
    CommitError, CommitTag, StagedTable, TableWriteSession, WriteStorage, WriteTable,
};
