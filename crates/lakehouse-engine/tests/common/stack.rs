//! Stack readiness helpers and environment accessors for lakehouse-engine E2E tests.
#![cfg(feature = "exasol-e2e")]

use std::time::{Duration, Instant};

const DEFAULT_TIMEOUT: Duration = Duration::from_secs(30);
const POLL_INTERVAL: Duration = Duration::from_millis(500);

/// Read a `u16` port from `env_var`, falling back to `default`.
///
/// Port discipline: the suite targets the lakehouse-engine compose stack's
/// dedicated host ports, all overridable so a fresh stack can always pick
/// free ports. Defaults match `docker-compose.yml`.
fn port_from_env(env_var: &str, default: u16) -> u16 {
    std::env::var(env_var)
        .ok()
        .and_then(|s| s.trim().parse::<u16>().ok())
        .unwrap_or(default)
}

/// Hostname/IP of the Exasol container.
pub fn exasol_host() -> String {
    std::env::var("EXASOL_HOST").unwrap_or_else(|_| "localhost".to_string())
}

/// Exasol SQL (WebSocket) host port. `LH_EXASOL_PORT`, default 28563.
pub fn exasol_sql_port() -> u16 {
    port_from_env("LH_EXASOL_PORT", 28563)
}

/// Exasol BucketFS HTTPS host port. `LH_BUCKETFS_PORT`, default 22581.
pub fn bucketfs_port() -> u16 {
    port_from_env("LH_BUCKETFS_PORT", 22581)
}

/// MinIO S3 host port. `LH_MINIO_PORT`, default 19000.
pub fn minio_port() -> u16 {
    port_from_env("LH_MINIO_PORT", 19000)
}

/// Iceberg REST catalog host port. `LH_REST_PORT`, default 18181.
pub fn rest_port() -> u16 {
    port_from_env("LH_REST_PORT", 18181)
}

/// Base URL of the Iceberg REST catalog as seen from the test process (host-side).
pub fn iceberg_catalog_url() -> String {
    std::env::var("ICEBERG_CATALOG_URL")
        .unwrap_or_else(|_| format!("http://localhost:{}", rest_port()))
}

/// Base URL of MinIO as seen from the test process (host-side).
pub fn minio_url() -> String {
    std::env::var("MINIO_URL").unwrap_or_else(|_| format!("http://localhost:{}", minio_port()))
}

/// The Iceberg catalog URL as reached from inside the Exasol UDF (Docker network).
///
/// The UDF runs inside the Exasol container on the `lakehouse` network and
/// reaches the catalog by its in-container address (alias `iceberg-rest`,
/// internal port 8181), NOT the host-published port.
pub fn iceberg_catalog_url_internal() -> String {
    std::env::var("ICEBERG_CATALOG_URL_INTERNAL")
        .unwrap_or_else(|_| "http://iceberg-rest:8181".to_string())
}

/// MinIO endpoint as reached from inside the Exasol UDF (Docker network).
pub fn minio_url_internal() -> String {
    std::env::var("MINIO_URL_INTERNAL").unwrap_or_else(|_| "http://minio:9000".to_string())
}

/// The Exasol container name (for `docker exec` credential extraction).
pub fn exasol_container() -> String {
    std::env::var("EXASOL_CONTAINER").unwrap_or_else(|_| "lakehouse-engine-rs-exasol-1".to_string())
}

/// Extract the BucketFS write password.
///
/// Resolution order:
/// 1. `BUCKETFS_WRITE_PASSWORD` env var
/// 2. `BUCKETFS_WRITE_PASS` env var
/// 3. docker exec into the Exasol container to read `/exa/etc/EXAConf`
pub fn bucketfs_write_password() -> String {
    if let Ok(p) = std::env::var("BUCKETFS_WRITE_PASSWORD") {
        if !p.is_empty() {
            return p;
        }
    }
    if let Ok(p) = std::env::var("BUCKETFS_WRITE_PASS") {
        if !p.is_empty() {
            return p;
        }
    }
    let container = exasol_container();
    let script = "awk '/\\[\\[Bucket : default\\]\\]/{flag=1} flag && /WritePasswd/{print $3; exit}' /exa/etc/EXAConf | base64 -d";
    let out = std::process::Command::new("docker")
        .args(["exec", &container, "bash", "-c", script])
        .output()
        .unwrap_or_else(|e| panic!("docker exec {container} to read BucketFS password: {e}"));
    let pw = String::from_utf8_lossy(&out.stdout).trim().to_string();
    assert!(
        !pw.is_empty(),
        "BucketFS write password unavailable — set BUCKETFS_WRITE_PASSWORD or ensure the Exasol container is up"
    );
    pw
}

/// Polls a URL until it returns 2xx or the timeout expires.
pub fn wait_for_url(url: &str, timeout: Duration) {
    let client = reqwest::blocking::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .expect("build HTTP client");
    let deadline = Instant::now() + timeout;
    loop {
        if client
            .get(url)
            .send()
            .map(|r| r.status().is_success())
            .unwrap_or(false)
        {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "service at {url} did not become healthy within {timeout:?}"
        );
        std::thread::sleep(POLL_INTERVAL);
    }
}

/// Assert Exasol SQL port is reachable; panic if not.
pub fn wait_for_exasol() {
    use std::net::TcpStream;
    let host = exasol_host();
    let addr = format!("{host}:{}", exasol_sql_port());
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        if TcpStream::connect(&addr).is_ok() {
            std::thread::sleep(Duration::from_secs(1));
            return;
        }
        assert!(
            Instant::now() < deadline,
            "Exasol SQL port at {addr} did not become ready within 60s"
        );
        std::thread::sleep(POLL_INTERVAL);
    }
}

/// Assert MinIO is reachable; panic if not.
pub fn wait_for_minio() {
    let url = format!("{}/minio/health/live", minio_url());
    wait_for_url(&url, DEFAULT_TIMEOUT);
}

/// Assert Iceberg REST catalog is reachable; panic if not.
pub fn wait_for_iceberg_catalog() {
    let url = format!("{}/v1/config", iceberg_catalog_url());
    wait_for_url(&url, DEFAULT_TIMEOUT);
}

/// Upload a file to BucketFS via HTTPS PUT.
///
/// The file surfaces inside the DB at `/buckets/bfsdefault/default/<name>`.
pub fn upload_to_bucketfs(local_path: &std::path::Path, bucketfs_path: &str) {
    assert!(
        local_path.exists(),
        "file not found at {local_path:?} — ensure it was built first"
    );
    let bytes = std::fs::read(local_path).unwrap_or_else(|e| panic!("read {local_path:?}: {e}"));
    let password = bucketfs_write_password();
    let url = format!(
        "https://{}:{}{}",
        exasol_host(),
        bucketfs_port(),
        bucketfs_path
    );
    let client = reqwest::blocking::Client::builder()
        .danger_accept_invalid_certs(true)
        .timeout(Duration::from_secs(120))
        .build()
        .expect("build BucketFS HTTPS client");
    let resp = client
        .put(&url)
        .basic_auth("w", Some(&password))
        .body(bytes)
        .send()
        .unwrap_or_else(|e| panic!("BucketFS PUT to {url} failed: {e}"));
    assert!(
        resp.status().is_success(),
        "BucketFS PUT to {url} returned {} (expected 2xx)",
        resp.status()
    );
}

/// Path (host-side) of the compiled lakehouse-engine .so.
pub fn lakehouse_engine_so_path() -> std::path::PathBuf {
    // CARGO_MANIFEST_DIR = lakehouse-engine-rs/crates/lakehouse-engine; go up two levels to workspace root.
    let manifest = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    // crates/lakehouse-engine -> workspace root is ../../
    manifest
        .parent()
        .and_then(|p| p.parent())
        .expect("could not navigate to workspace root from CARGO_MANIFEST_DIR")
        .join("target/release/liblakehouse_engine.so")
}
