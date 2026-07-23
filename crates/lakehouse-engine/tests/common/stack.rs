//! Stack readiness helpers and environment accessors for lakehouse-engine E2E tests.
#![cfg(any(feature = "exasol-e2e", feature = "cloud-e2e"))]

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
///
/// Resolution order:
/// 1. `EXASOL_CONTAINER` env var (CI / manual override).
/// 2. Discover the running container by its Compose service label, narrowed to
///    the one publishing this stack's SQL port — works regardless of the
///    Compose project prefix (`lakehouse-vs-*`, `lakehouse-engine-rs-*`, …) and
///    stays correct when an unrelated Exasol stack is also running.
/// 3. Hardcoded directory-derived default.
pub fn exasol_container() -> String {
    if let Ok(c) = std::env::var("EXASOL_CONTAINER")
        && !c.trim().is_empty()
    {
        return c.trim().to_string();
    }
    // Disambiguate by the published SQL port so we never read credentials from a
    // different Exasol stack that happens to share the `exasol` compose-service
    // label. (A bare label filter assumes a single exasol container on the host.)
    if let Ok(out) = std::process::Command::new("docker")
        .args([
            "ps",
            "--filter",
            "label=com.docker.compose.service=exasol",
            "--filter",
            &format!("publish={}", exasol_sql_port()),
            "--format",
            "{{.Names}}",
        ])
        .output()
        && let Some(name) = String::from_utf8_lossy(&out.stdout).lines().next()
    {
        let name = name.trim();
        if !name.is_empty() {
            return name.to_string();
        }
    }
    "lakehouse-engine-rs-exasol-1".to_string()
}

/// Extract the BucketFS write password.
///
/// Resolution order:
/// 1. `BUCKETFS_WRITE_PASSWORD` env var
/// 2. `BUCKETFS_WRITE_PASS` env var
/// 3. docker exec into the Exasol container to read `/exa/etc/EXAConf`
pub fn bucketfs_write_password() -> String {
    if let Ok(p) = std::env::var("BUCKETFS_WRITE_PASSWORD")
        && !p.is_empty()
    {
        return p;
    }
    if let Ok(p) = std::env::var("BUCKETFS_WRITE_PASS")
        && !p.is_empty()
    {
        return p;
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
    // BucketFS's HTTPS listener can transiently refuse/reset a connection right
    // after the SQL port already accepts connections (`wait_for_exasol` only
    // checks the SQL port). Retry a few times on a connection-level send()
    // error before giving up; a non-2xx HTTP response is a real failure and
    // is NOT retried.
    const MAX_ATTEMPTS: u32 = 5;
    let mut last_err = None;
    let mut resp = None;
    for attempt in 1..=MAX_ATTEMPTS {
        match client
            .put(&url)
            .basic_auth("w", Some(&password))
            .body(bytes.clone())
            .send()
        {
            Ok(r) => {
                resp = Some(r);
                break;
            }
            Err(e) => {
                last_err = Some(e);
                if attempt < MAX_ATTEMPTS {
                    std::thread::sleep(Duration::from_secs(2));
                }
            }
        }
    }
    let resp = resp.unwrap_or_else(|| {
        panic!(
            "BucketFS PUT to {url} failed after {MAX_ATTEMPTS} attempts: {}",
            last_err.expect("at least one attempt recorded an error")
        )
    });
    assert!(
        resp.status().is_success(),
        "BucketFS PUT to {url} returned {} (expected 2xx)",
        resp.status()
    );
}

/// Path (host-side) of the compiled lakehouse-engine .so.
#[cfg(feature = "exasol-e2e")]
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

// ---------------------------------------------------------------------------
// CONNECTION credential helpers (shared by local-E2E and cloud-E2E)
// ---------------------------------------------------------------------------

/// Build the JSON password string for a catalog CONNECTION object.
///
/// The resulting string is suitable for use in:
///   `CREATE OR REPLACE CONNECTION <name> TO '<uri>' USER '' IDENTIFIED BY '<json>'`
///
/// All boolean flags default to `false`; `path_style` defaults to `true`.
pub struct CatalogConnectionPassword {
    pub warehouse: String,
    pub endpoint: String,
    pub region: String,
    pub access_key: String,
    pub secret_key: String,
    pub session_token: Option<String>,
    pub path_style: bool,
    pub use_sigv4: bool,
    pub use_vended_credentials: bool,
}

impl CatalogConnectionPassword {
    /// Serialize to a JSON string suitable for the CONNECTION `IDENTIFIED BY` clause.
    ///
    /// Single quotes within the value are escaped as `''` for SQL embedding.
    pub fn to_sql_password_json(&self) -> String {
        let mut obj = serde_json::json!({
            "warehouse": self.warehouse,
            "endpoint": self.endpoint,
            "region": self.region,
            "access_key": self.access_key,
            "secret_key": self.secret_key,
            "path_style": self.path_style,
            "use_sigv4": self.use_sigv4,
            "use_vended_credentials": self.use_vended_credentials,
        });
        if let Some(token) = &self.session_token {
            obj["session_token"] = serde_json::Value::String(token.clone());
        }
        // Escape single quotes for safe SQL embedding (SQL string literal).
        obj.to_string().replace('\'', "''")
    }
}

/// Build the `CREATE OR REPLACE CONNECTION` SQL statement for a catalog connection.
///
/// `conn_name`: the Exasol CONNECTION object name (bare, no quoting)
/// `catalog_uri`: the Iceberg REST catalog address (goes into CONNECTION address)
/// `password`: credential parameters for the JSON password
pub fn build_create_connection_sql(
    conn_name: &str,
    catalog_uri: &str,
    password: &CatalogConnectionPassword,
) -> String {
    let json_pw = password.to_sql_password_json();
    // catalog_uri goes into TO '...' — escape any single quotes in it too.
    let safe_uri = catalog_uri.replace('\'', "''");
    format!(
        "CREATE OR REPLACE CONNECTION {conn_name} TO '{safe_uri}' USER '' IDENTIFIED BY '{json_pw}'"
    )
}

/// Build and return the `CatalogConnectionPassword` for the local Docker stack
/// (MinIO + Iceberg REST catalog, internal Docker network addresses).
///
/// Uses the same internal URLs that `create_virtual_schema` uses for
/// CATALOG_URI / S3_ENDPOINT — these are the addresses reachable from
/// inside the Exasol UDF container.
#[cfg(feature = "exasol-e2e")]
pub fn local_stack_connection_password() -> CatalogConnectionPassword {
    CatalogConnectionPassword {
        warehouse: "s3://warehouse/".to_string(),
        endpoint: minio_url_internal(),
        region: "us-east-1".to_string(),
        access_key: "minioadmin".to_string(),
        secret_key: "minioadmin".to_string(),
        session_token: None,
        path_style: true,
        use_sigv4: false,
        use_vended_credentials: false,
    }
}
