//! Test-only fixtures reached by two or more of this crate's test modules —
//! the single home the `vs-adapter/pushdown-module-structure` rule requires
//! for a test helper reachable from multiple submodules. Each fixture's doc
//! comment names its consumers; a fixture reached by only one module lives
//! in that module's own `mod tests` instead.

use crate::{ConnectionCreds, StorageBackend, StorageProps};

/// A baseline `ConnectionCreds` with no catalog auth (all auth fields `None`).
/// Individual tests set only the auth fields under test.
///
/// Consumers: `auth`, `namespace`, `session`.
pub(crate) fn base_creds() -> ConnectionCreds {
    ConnectionCreds {
        warehouse: "warehouse".into(),
        endpoint: "http://minio:9000".into(),
        region: "us-east-1".into(),
        access_key: "minioadmin".into(),
        secret_key: "minioadmin".into(),
        session_token: None,
        path_style: true,
        use_sigv4: false,
        use_vended_credentials: false,
        token: None,
        client_id: None,
        client_secret: None,
        oauth2_server_uri: None,
        scope: None,
        account_name: None,
        account_key: None,
        sas_token: None,
    }
}

/// Static storage with the sentinel keys `STATIC_AK_SENTINEL` / `STATIC_SK_SENTINEL`
/// (matching the credentials-cluster test sentinels below).
///
/// Consumers: `vended`, and [`static_backend`] below.
pub(crate) fn static_storage() -> StorageProps {
    StorageProps {
        endpoint: "https://s3.amazonaws.com".into(),
        region: "us-east-1".into(),
        access_key: "STATIC_AK_SENTINEL".into(),
        secret_key: "STATIC_SK_SENTINEL".into(),
        path_style: false,
        ..Default::default()
    }
}

/// [`static_storage`] wrapped in the `S3` backend variant — the fixture for every
/// call site that now takes `&StorageBackend` (`list_namespace_tables`,
/// `resolve_vended_storage`) rather than `&StorageProps`.
///
/// Consumers: `namespace`, `vended`.
pub(crate) fn static_backend() -> StorageBackend {
    StorageBackend::S3(static_storage())
}

/// Unwrap a `StorageBackend`'s `S3` payload — the test-only inverse of
/// `StorageBackend::S3(..)`, so a test asserting directly on `StorageProps`
/// fields against a `resolve_vended_storage` return value can stay unchanged
/// below the unwrap.
///
/// Consumers: `vended`.
pub(crate) fn s3_payload(backend: StorageBackend) -> StorageProps {
    match backend {
        StorageBackend::S3(props) => props,
        StorageBackend::Adls { .. } => panic!("s3_payload is S3-only"),
    }
}

// --- Shared sentinels ---
/// Consumers: `creds_no_auth` (below) and `vended`.
pub(crate) const STATIC_AK: &str = "STATIC_AK_SENTINEL";
/// Consumers: `creds_no_auth` (below) and `vended`.
pub(crate) const STATIC_SK: &str = "STATIC_SK_SENTINEL";
/// Consumers: `auth`, `iceberg_io`.
pub(crate) const BEARER_TOK: &str = "BEARER_TOKEN_SENTINEL_VALUE";
/// Consumers: `auth`, `iceberg_io`, `vended`.
pub(crate) const CLIENT_SECRET: &str = "CLIENT_SECRET_SENTINEL_VALUE";
/// Consumers: `auth`, `iceberg_io`.
pub(crate) const OAUTH_ACCESS_TOKEN: &str = "OAUTH_OBTAINED_ACCESS_TOKEN";

/// A `ConnectionCreds` with no auth, no vending — the no-op baseline.
///
/// Consumers: `auth`, `iceberg_io`, `session`.
pub(crate) fn creds_no_auth() -> ConnectionCreds {
    ConnectionCreds {
        warehouse: "warehouse".into(),
        endpoint: "https://s3.amazonaws.com".into(),
        region: "us-east-1".into(),
        access_key: STATIC_AK.into(),
        secret_key: STATIC_SK.into(),
        session_token: None,
        path_style: false,
        use_sigv4: false,
        use_vended_credentials: false,
        token: None,
        client_id: None,
        client_secret: None,
        oauth2_server_uri: None,
        scope: None,
        account_name: None,
        account_key: None,
        sas_token: None,
    }
}
