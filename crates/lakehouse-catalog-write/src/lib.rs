//! Iceberg REST catalog writes for the export: namespace and table creation, staged creation, the
//! commit loop that adds a writer's data files to a table exactly once, and the vended storage
//! credentials that renew themselves inside a writer table's FileIO. Every state-changing request
//! goes through `CatalogSession::post`, and each one the session sends only once is retried here by
//! the operation that can check its outcome.

mod adls_storage;
mod iceberg_write;
mod object_store_dns;
mod retry;
mod snapshot_capture;
mod unappendable;
mod vended_refresh;
mod write_storage;

#[cfg(any(test, feature = "test-support"))]
#[path = "exported_test_support_tests.rs"]
pub mod exported_test_support;

#[cfg(test)]
#[path = "test_support_tests.rs"]
mod test_support;

pub use object_store_dns::{adls_store_builder, caching_client_options, s3_store_builder};

pub use iceberg_write::{CommitError, CommitTag, TableWriteSession, WriteTable};
pub use unappendable::{Refusal, refuse_unappendable};
pub use write_storage::WriteStorage;
