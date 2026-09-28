mod auth;
mod client;
mod vended;

pub use client::UnityCatalogSession;
pub use vended::{TemporaryTableCredentials, resolve_uc_vended_storage};

#[cfg(test)]
#[path = "mock_server_tests.rs"]
mod mock_server;
