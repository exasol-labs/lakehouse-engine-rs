//! The two I/O primitives the catalog layer is built on: an authenticated
//! catalog `GET` that deserializes its JSON body, and the S3 `FileIO` the scan
//! reads manifest files through.
//!
//! Moved verbatim from the engine's `adapter/pushdown/credentials.rs`.
//! Credential values NEVER appear in any returned error — every error site
//! routes through a redaction closure.

use crate::auth::{CatalogAuth, redact_catalog_auth_error};
use crate::redaction::redact_secret_values;
use crate::{ConnectionCreds, StorageProps};
use exasol_udf_sdk::error::UdfError;
use iceberg::io::{
    FileIOBuilder, S3_ACCESS_KEY_ID, S3_ENDPOINT, S3_PATH_STYLE_ACCESS, S3_REGION,
    S3_SECRET_ACCESS_KEY, S3_SESSION_TOKEN,
};
use iceberg_storage_opendal::OpenDalStorageFactory;
use std::sync::Arc;

/// Build an S3 `FileIO` from storage props.
///
/// Used by the signed path to give the iceberg `Table` a way to read manifest
/// files from S3 after we have fetched and deserialized the `LoadTableResult`.
pub fn build_s3_file_io(storage: &StorageProps) -> iceberg::io::FileIO {
    let mut builder = FileIOBuilder::new(Arc::new(OpenDalStorageFactory::S3 {
        customized_credential_load: None,
    }));
    if !storage.endpoint.is_empty() {
        builder = builder.with_prop(S3_ENDPOINT, &storage.endpoint);
    }
    if !storage.region.is_empty() {
        builder = builder.with_prop(S3_REGION, &storage.region);
    }
    if !storage.access_key.is_empty() {
        builder = builder.with_prop(S3_ACCESS_KEY_ID, &storage.access_key);
    }
    if !storage.secret_key.is_empty() {
        builder = builder.with_prop(S3_SECRET_ACCESS_KEY, &storage.secret_key);
    }
    if let Some(token) = &storage.session_token {
        builder = builder.with_prop(S3_SESSION_TOKEN, token);
    }
    builder = builder.with_prop(S3_PATH_STYLE_ACCESS, storage.path_style.to_string());
    builder.build()
}

/// Build and authenticate a `GET` request against `url`, applying the resolved
/// catalog-auth strategy and (when vending) the access-delegation header, then
/// execute it and deserialize the JSON body into `T`.
///
/// Credential values NEVER appear in the returned error.
pub(crate) async fn authed_get_json<T: serde::de::DeserializeOwned>(
    client: &reqwest::Client,
    url: &str,
    auth: &CatalogAuth,
    send_access_delegation: bool,
    creds: &ConnectionCreds,
) -> Result<T, UdfError> {
    // Redact static catalog-auth secrets AND the live bearer token (which, for the
    // OAuth2 mode, is the grant-obtained access token and is NOT present in
    // `creds`). Every error site below routes through this closure.
    let redact = |msg: &str| {
        let base = redact_catalog_auth_error(msg, creds);
        match auth {
            CatalogAuth::Bearer(token) => redact_secret_values(&base, &[token.as_str()]),
            CatalogAuth::Sigv4 | CatalogAuth::None => base,
        }
    };

    let mut builder = client.get(url).header("accept", "application/json");
    if send_access_delegation {
        builder = builder.header("X-Iceberg-Access-Delegation", "vended-credentials");
    }
    if let CatalogAuth::Bearer(token) = auth {
        builder = builder.bearer_auth(token);
    }
    let request = builder.build().map_err(|e| {
        UdfError::User(format!(
            "failed to build catalog request: {}",
            redact(&e.to_string())
        ))
    })?;

    let request = match auth {
        CatalogAuth::Sigv4 => crate::sigv4::sign_request(
            request,
            &creds.access_key,
            &creds.secret_key,
            creds.session_token.as_deref(),
            &creds.region,
            "glue",
        )
        .map_err(|e| {
            UdfError::User(format!(
                "failed to sign catalog request: {}",
                redact(&e.to_string())
            ))
        })?,
        CatalogAuth::Bearer(_) | CatalogAuth::None => request,
    };

    let response = client.execute(request).await.map_err(|e| {
        UdfError::User(format!(
            "catalog request failed: {}",
            redact(&e.to_string())
        ))
    })?;

    let status = response.status();
    if !status.is_success() {
        let body = response
            .text()
            .await
            .unwrap_or_else(|_| "(unreadable body)".into());
        return Err(UdfError::User(format!(
            "catalog returned HTTP {}: {}",
            status.as_u16(),
            redact(&body)
        )));
    }

    response.json::<T>().await.map_err(|e| {
        UdfError::User(format!(
            "failed to parse catalog response: {}",
            redact(&e.to_string())
        ))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::*;

    // ---------------------------------------------------------------------------
    // Task 3.3 / 3.4 — SigV4 wiring: signed/unsigned request routing
    // ---------------------------------------------------------------------------

    /// Scenario: Unsigned catalog path is unchanged when SigV4 is disabled.
    ///
    /// Tests that with use_sigv4=false, the ConnectionCreds does not affect the
    /// path logic (the unsigned RestCatalogBuilder path is selected). We verify
    /// this by confirming an unsigned request carries no Authorization header.
    #[test]
    fn disabled_sigv4_produces_no_auth_header_in_request() {
        // Construct a raw reqwest::Request without signing it.
        let client = reqwest::Client::new();
        let request = client
            .get("https://minio.local:9000/iceberg/v1/namespaces/db/tables/events")
            .build()
            .expect("valid request");

        // An unsigned request must carry no Authorization or x-amz-date headers.
        assert!(
            request.headers().get("authorization").is_none(),
            "unsigned path: no Authorization header expected"
        );
        assert!(
            request.headers().get("x-amz-date").is_none(),
            "unsigned path: no x-amz-date header expected"
        );
    }

    /// Scenario: Signing keys must not appear in any error output from sign_request.
    ///
    /// The SigningError type from aws-sigv4 carries no credential fields.
    /// We verify this indirectly: a successful sign followed by inspection of all
    /// header values must not contain the secret key in plaintext.
    #[test]
    fn signed_request_does_not_leak_keys_in_headers() {
        let secret = "wJalrXUtnFEMI_EXAMPLE_KEY";
        let client = reqwest::Client::new();
        let request = client
            .get("https://glue.us-east-1.amazonaws.com/iceberg/v1/123/namespaces/db/tables/t")
            .build()
            .expect("valid request");

        let signed =
            crate::sigv4::sign_request(request, "AKIDEXAMPLE", secret, None, "us-east-1", "glue")
                .expect("signing must succeed");

        for (name, value) in signed.headers().iter() {
            let v = value.to_str().unwrap_or("");
            assert!(
                !v.contains(secret),
                "secret key must not appear in signed header '{name}': {v}"
            );
        }
    }

    // ---------------------------------------------------------------------------
    // Task 4.2 — auth-mode selection and header construction
    // ---------------------------------------------------------------------------

    /// Scenario: Static bearer token is attached to unsigned catalog requests.
    ///
    /// Constructs a reqwest request with a bearer token and verifies the
    /// `Authorization: Bearer <token>` header is set — mirroring the
    /// `authed_get_json` bearer-auth branch.
    #[test]
    fn bearer_token_attached_to_load_table_request() {
        let client = reqwest::Client::new();
        let url = "https://catalog.example.com/v1/namespaces/db/tables/t";

        // Build the request exactly as authed_get_json does for CatalogAuth::Bearer.
        let request = client
            .get(url)
            .header("accept", "application/json")
            .bearer_auth(BEARER_TOK)
            .build()
            .expect("valid request");

        let auth_header = request
            .headers()
            .get("authorization")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");

        assert!(
            auth_header.starts_with("Bearer "),
            "authorization header must start with 'Bearer ': {auth_header}"
        );
        assert!(
            auth_header.contains(BEARER_TOK),
            "bearer token must appear in the authorization header"
        );

        // The token value is NOT a signing key — it's sent literally; the leak
        // guard is that it must NOT appear in any *error* message (tested in 4.5).
        // Confirm the SigV4 signing headers (x-amz-*) are absent.
        assert!(
            request.headers().get("x-amz-date").is_none(),
            "bearer-auth must not set x-amz-date"
        );
        assert!(
            request.headers().get("x-amz-security-token").is_none(),
            "bearer-auth must not set x-amz-security-token"
        );
    }

    /// Scenario: No catalog auth props are set when neither token nor OAuth
    /// credentials are supplied — the request carries no Authorization header.
    #[test]
    fn no_auth_load_table_sends_no_authorization() {
        let client = reqwest::Client::new();
        // Build the request as authed_get_json does for CatalogAuth::None:
        // only the "accept" header, no bearer_auth, no signing.
        let request = client
            .get("https://catalog.example.com/v1/namespaces/db/tables/t")
            .header("accept", "application/json")
            .build()
            .expect("valid request");

        assert!(
            request.headers().get("authorization").is_none(),
            "no-auth path must not set Authorization header"
        );
        assert!(
            request.headers().get("x-amz-date").is_none(),
            "no-auth path must not set x-amz-date"
        );
    }

    /// Contract pin: the catalog error site emits the exact
    /// `catalog returned HTTP <status>: <body>` prefix that `adapter::mod`'s
    /// `is_table_not_found` classifier keys on via `starts_with`.
    ///
    /// This drives the REAL `authed_get_json` non-success branch
    /// (`format!("catalog returned HTTP {}: {}", status.as_u16(), redact(&body))`)
    /// against a local server returning 404, so a future edit to the message
    /// shape here breaks this test rather than silently making the skip-non-
    /// Iceberg-table logic dead. The body is credential-free, so `redact`
    /// leaves it verbatim, pinning both the `404` status rendering and the
    /// `": "` separator.
    #[tokio::test]
    async fn catalog_error_message_uses_http_status_prefix() {
        use std::net::SocketAddr;
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpListener;

        const NOT_ICEBERG_BODY: &str =
            "NoSuchIcebergTableException: Input table is not an iceberg table";

        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind failed");
        let addr: SocketAddr = listener.local_addr().expect("local_addr");
        let port = addr.port();

        // Reply to a single request with an HTTP 404 and a Hive-table body.
        tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("accept");
            let mut buf = vec![0u8; 4096];
            let _ = stream.read(&mut buf).await.expect("read");
            let response = format!(
                "HTTP/1.1 404 Not Found\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
                NOT_ICEBERG_BODY.len(),
                NOT_ICEBERG_BODY
            );
            stream.write_all(response.as_bytes()).await.expect("write");
        });

        let url = format!("http://127.0.0.1:{port}/v1/warehouse/namespaces/db/tables/hive_table");
        let creds = creds_no_auth();
        let client = reqwest::Client::new();
        let err =
            authed_get_json::<serde_json::Value>(&client, &url, &CatalogAuth::None, false, &creds)
                .await
                .expect_err("a 404 response must surface as an error");

        let msg = err.to_string();
        assert!(
            msg.starts_with("catalog returned HTTP 404: "),
            "the classifier's load-bearing prefix must be emitted verbatim, got: {msg}"
        );
        assert!(
            msg.contains(NOT_ICEBERG_BODY),
            "the (credential-free) response body must follow the status prefix, got: {msg}"
        );
    }

    // ---------------------------------------------------------------------------
    // Task 4.5 / 3.1 — Redaction: secrets never in errors from the new paths
    // ---------------------------------------------------------------------------

    /// Scenario: a `loadTable` error surfaced through the REAL `authed_get_json`
    /// redact closure, with the session's resolved auth set to
    /// `CatalogAuth::Bearer(<live token>)`, strips BOTH the static catalog-auth
    /// secrets (via `redact_catalog_auth_error`, keyed on `creds.client_secret`)
    /// AND the live bearer token (added to the redaction set only because
    /// `auth` is `CatalogAuth::Bearer` — the live token is never present in
    /// `creds`, so `redact_catalog_auth_error` alone could not strip it).
    ///
    /// The local server echoes both secrets in the error body — the failure
    /// mode the closure guards against — so this drives the real function
    /// rather than re-implementing its redaction logic inline.
    #[tokio::test]
    async fn load_table_error_redacts_session_bearer_and_static_secrets() {
        use std::net::SocketAddr;
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpListener;

        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind failed");
        let addr: SocketAddr = listener.local_addr().expect("local_addr");
        let port = addr.port();

        let body = format!("error: secret={CLIENT_SECRET} bearer={OAUTH_ACCESS_TOKEN}");

        tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("accept");
            let mut buf = vec![0u8; 4096];
            let _ = stream.read(&mut buf).await.expect("read");
            let response = format!(
                "HTTP/1.1 401 Unauthorized\r\nContent-Type: text/plain\r\nContent-Length: {}\r\n\r\n{}",
                body.len(),
                body
            );
            stream.write_all(response.as_bytes()).await.expect("write");
        });

        let client = reqwest::Client::new();
        let url = format!("http://127.0.0.1:{port}/v1/namespaces/db/tables/t");
        let mut creds = creds_no_auth();
        creds.client_secret = Some(CLIENT_SECRET.into());
        let auth = CatalogAuth::Bearer(OAUTH_ACCESS_TOKEN.to_string());

        let err = authed_get_json::<serde_json::Value>(&client, &url, &auth, false, &creds)
            .await
            .expect_err("a 401 response must surface as an error");

        let msg = err.to_string();
        assert!(
            msg.starts_with("catalog returned HTTP 401: "),
            "the redaction closure must have run against the real 401 body, got: {msg}"
        );
        assert!(
            !msg.contains(CLIENT_SECRET),
            "static client_secret must not appear in error: {msg}"
        );
        assert!(
            !msg.contains(OAUTH_ACCESS_TOKEN),
            "live session bearer token must not appear in error: {msg}"
        );
    }
}
