//! The native Unity Catalog REST client: a `UnityCatalogSession` implementing the
//! shared `CatalogClient` trait over the standard `/api/2.1/unity-catalog` API,
//! its crate-private authentication strategy and wire types, and the vended-
//! credentials selector that terminates a temporary-table-credentials response in
//! a `StorageBackend`.

mod auth;
mod client;
mod vended;

pub use client::UnityCatalogSession;
pub use vended::{TemporaryTableCredentials, resolve_uc_vended_storage};

#[cfg(test)]
#[path = "mock_server_tests.rs"]
mod mock_server;
