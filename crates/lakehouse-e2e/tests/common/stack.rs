#![cfg(any(
    feature = "exasol-e2e",
    feature = "cloud-e2e",
    feature = "lakekeeper-e2e",
    feature = "azure-e2e",
    feature = "unity-e2e",
    feature = "glue-e2e"
))]

use std::time::{Duration, Instant};

const DEFAULT_TIMEOUT: Duration = Duration::from_secs(30);
const POLL_INTERVAL: Duration = Duration::from_millis(500);

/// Defaults match `docker-compose.yml`; overridable so a fresh stack can pick free ports.
fn port_from_env(env_var: &str, default: u16) -> u16 {
    std::env::var(env_var)
        .ok()
        .and_then(|s| s.trim().parse::<u16>().ok())
        .unwrap_or(default)
}

pub fn exasol_host() -> String {
    std::env::var("EXASOL_HOST").unwrap_or_else(|_| "localhost".to_string())
}

pub fn exasol_sql_port() -> u16 {
    port_from_env("LH_EXASOL_PORT", 28563)
}

pub fn bucketfs_port() -> u16 {
    port_from_env("LH_BUCKETFS_PORT", 22581)
}

pub fn seaweedfs_port() -> u16 {
    port_from_env("LH_SEAWEEDFS_PORT", 19000)
}

pub fn rest_port() -> u16 {
    port_from_env("LH_REST_PORT", 18181)
}

pub fn iceberg_catalog_url() -> String {
    std::env::var("ICEBERG_CATALOG_URL")
        .unwrap_or_else(|_| format!("http://localhost:{}", rest_port()))
}

pub fn seaweedfs_url() -> String {
    std::env::var("SEAWEEDFS_URL")
        .unwrap_or_else(|_| format!("http://localhost:{}", seaweedfs_port()))
}

/// The UDF runs inside the Exasol container, so it needs the in-network address, not the
/// host-published port.
pub fn iceberg_catalog_url_internal() -> String {
    std::env::var("ICEBERG_CATALOG_URL_INTERNAL")
        .unwrap_or_else(|_| "http://iceberg-rest:8181".to_string())
}

pub fn seaweedfs_url_internal() -> String {
    std::env::var("SEAWEEDFS_URL_INTERNAL").unwrap_or_else(|_| "http://seaweedfs:8333".to_string())
}

// The assume-role identity, mirroring `seaweedfs-iam.json`: a base user with no S3 access
// that the `LakehouseReader` role's trust policy admits. SeaweedFS does not evaluate
// `sts:ExternalId`, so the external id (deliberately holding `+`, `=`, `/`, `:`, `@`) only
// exercises the encoding path; its enforcement is covered by the cloud E2E.
pub const ASSUME_ROLE_BASE_ACCESS_KEY: &str = "lhassumebase";
pub const ASSUME_ROLE_BASE_SECRET_KEY: &str = "lhassumebasesecret123";
pub const ASSUME_ROLE_ARN: &str = "arn:aws:iam::000000000000:role/LakehouseReader";
pub const ASSUME_ROLE_UNKNOWN_ARN: &str = "arn:aws:iam::000000000000:role/NoSuchRole";
pub const ASSUME_ROLE_EXTERNAL_ID: &str = "lh+ext=id/2026:demo@example";

/// Prefers `EXASOL_CONTAINER`, then the compose-labelled container publishing this
/// stack's SQL port, then a directory-derived default.
pub fn exasol_container() -> String {
    if let Ok(c) = std::env::var("EXASOL_CONTAINER")
        && !c.trim().is_empty()
    {
        return c.trim().to_string();
    }
    // Filter by the published SQL port too, so credentials are never read from an
    // unrelated Exasol stack sharing the `exasol` compose-service label.
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

/// Which Exasol image runs the stack. `docker-db` is amd64-only, so arm64 hosts run `nano`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ExasolFlavor {
    DockerDb,
    Nano,
}

/// `EXASOL_FLAVOR` (`docker-db` or `nano`) when set, else read from the container's image.
pub fn exasol_flavor() -> ExasolFlavor {
    static FLAVOR: std::sync::OnceLock<ExasolFlavor> = std::sync::OnceLock::new();
    *FLAVOR.get_or_init(|| {
        if let Ok(declared) = std::env::var("EXASOL_FLAVOR")
            && !declared.trim().is_empty()
        {
            return match declared.trim() {
                "docker-db" => ExasolFlavor::DockerDb,
                "nano" => ExasolFlavor::Nano,
                other => panic!("EXASOL_FLAVOR must be `docker-db` or `nano`, got {other:?}"),
            };
        }
        let image = std::process::Command::new("docker")
            .args([
                "inspect",
                "--format",
                "{{.Config.Image}}",
                &exasol_container(),
            ])
            .output()
            .map(|out| String::from_utf8_lossy(&out.stdout).trim().to_string())
            .unwrap_or_default();
        if image.contains("exasol/nano") {
            ExasolFlavor::Nano
        } else {
            ExasolFlavor::DockerDb
        }
    })
}

/// A command that runs `args` in the Exasol container's network namespace. Nano's image has no
/// shell or `getent`, so there a throwaway container joins that namespace and its name resolution.
fn in_exasol_network(container: &str, args: &[&str]) -> std::process::Command {
    let mut command = std::process::Command::new("docker");
    match exasol_flavor() {
        ExasolFlavor::DockerDb => command.args(["exec", container]),
        ExasolFlavor::Nano => command.args([
            "run",
            "--rm",
            "--network",
            &format!("container:{container}"),
            NETWORK_PROBE_IMAGE,
        ]),
    };
    command.args(args);
    command
}

/// Has bash and `getent`; `scripts/e2e-stack.sh` probes Nano's readiness with the same image.
const NETWORK_PROBE_IMAGE: &str = "debian:trixie-slim";

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

pub fn wait_for_seaweedfs() {
    let url = format!("{}/status", seaweedfs_url());
    wait_for_url(&url, DEFAULT_TIMEOUT);
}

pub fn wait_for_iceberg_catalog() {
    let url = format!("{}/v1/config", iceberg_catalog_url());
    wait_for_url(&url, DEFAULT_TIMEOUT);
}

/// Defaults match `docker-compose.yml`; overridable so a parallel stack can use its own network.
fn env_or(env_var: &str, default: &str) -> String {
    std::env::var(env_var)
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| default.to_string())
}

pub fn network_name() -> String {
    env_or("LH_NETWORK", "lakehouse-engine")
}

pub fn proxy_host_internal() -> String {
    format!("{}.0.12", env_or("LH_SUBNET_PREFIX", "10.84"))
}

pub fn proxy_url_internal() -> String {
    format!("http://{}:8888", proxy_host_internal())
}

pub fn closed_proxy_url_internal() -> String {
    format!("http://{}:1", proxy_host_internal())
}

/// Only the forward proxy resolves a `-via-proxy` host, so a request that bypasses it fails.
pub const ICEBERG_REST_VIA_PROXY: &str = "http://iceberg-rest-via-proxy:8181";
pub const SEAWEEDFS_VIA_PROXY: &str = "http://seaweedfs-via-proxy:8333";
pub const UNITY_CATALOG_VIA_PROXY: &str = "http://unitycatalog-via-proxy:8080";
/// The adapter schema of a suite's proxy-only scripts, so their `%env` lines reach no other
/// virtual schema.
pub const PROXY_ADAPTER_SCHEMA: &str = "LHVS_PROXY";

/// `url` with its host renamed to the host's `-via-proxy` name.
pub fn via_proxy(url: &str) -> String {
    let host_start = url.find("//").map_or(0, |at| at + 2);
    let host_end = url[host_start..]
        .find([':', '/'])
        .map_or(url.len(), |at| host_start + at);
    format!("{}-via-proxy{}", &url[..host_end], &url[host_end..])
}

/// The host of `url`, as `getent` resolves it.
pub fn url_host(url: &str) -> &str {
    let rest = url.split_once("//").map_or(url, |(_, rest)| rest);
    rest.split([':', '/']).next().unwrap_or(rest)
}

/// Probes from the Exasol container, because only the UDFs there reach the proxy.
pub fn wait_for_proxy() {
    let container = exasol_container();
    let probe = format!(
        "timeout 3 bash -c '</dev/tcp/{}/8888'",
        proxy_host_internal()
    );
    let deadline = Instant::now() + DEFAULT_TIMEOUT;
    loop {
        let reachable = in_exasol_network(&container, &["bash", "-c", &probe])
            .status()
            .is_ok_and(|s| s.success());
        if reachable {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "forward proxy at {} unreachable from {container}; start the `proxy` Compose service",
            proxy_url_internal()
        );
        std::thread::sleep(POLL_INTERVAL);
    }
}

/// Fails when `host` resolves inside the Exasol container: a proxy-only name that resolves
/// there cannot prove a request went through the proxy.
pub fn assert_resolves_only_through_proxy(host: &str) {
    let container = exasol_container();
    let out = in_exasol_network(&container, &["getent", "hosts", host])
        .output()
        .unwrap_or_else(|e| panic!("getent hosts {host} in {container}'s network: {e}"));
    assert!(
        !out.status.success(),
        "{host} resolves inside {container}, so a passing query would not prove the proxy was used: {}",
        String::from_utf8_lossy(&out.stdout)
    );
}

/// A `since` for [`proxy_log_since`]: the current Unix second.
pub fn proxy_log_mark() -> String {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock after the epoch")
        .as_secs()
        .to_string()
}

pub fn proxy_log_since(since: &str) -> String {
    let network = network_name();
    let out = std::process::Command::new("docker")
        .args([
            "ps",
            "--filter",
            "label=com.docker.compose.service=proxy",
            "--filter",
            &format!("network={network}"),
            "--format",
            "{{.Names}}",
        ])
        .output()
        .expect("docker ps for the proxy container");
    let container = String::from_utf8_lossy(&out.stdout)
        .lines()
        .next()
        .map(str::trim)
        .filter(|name| !name.is_empty())
        .unwrap_or_else(|| panic!("no running `proxy` Compose service on the {network} network"))
        .to_string();
    let out = std::process::Command::new("docker")
        .args(["logs", "--since", since, &container])
        .output()
        .unwrap_or_else(|e| panic!("docker logs {container}: {e}"));
    format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    )
}

/// Runs `statement` and counts, for each `requests` entry, the proxy-log lines it produced that
/// contain it.
pub fn count_proxy_requests<const N: usize>(
    requests: [&str; N],
    statement: impl FnOnce(),
) -> [usize; N] {
    let log = proxy_log_during(statement);
    requests.map(|request| request_count(&log, request))
}

/// The proxy-log lines that `statement` produced. It first waits past one second, because the
/// log's `since` has second granularity and an earlier statement's lines must fall before the mark.
pub fn proxy_log_during(statement: impl FnOnce()) -> String {
    std::thread::sleep(Duration::from_millis(1100));
    let mark = proxy_log_mark();
    statement();
    proxy_log_since(&mark)
}

/// How many of `log`'s lines contain `request`.
pub fn request_count(log: &str, request: &str) -> usize {
    log.lines().filter(|line| line.contains(request)).count()
}

/// The file surfaces inside the DB at `/buckets/bfsdefault/default/<name>`.
pub fn upload_to_bucketfs(local_path: &std::path::Path, bucketfs_path: &str) {
    assert!(
        local_path.exists(),
        "file not found at {local_path:?} — ensure it was built first"
    );
    let bytes = std::fs::read(local_path).unwrap_or_else(|e| panic!("read {local_path:?}: {e}"));
    put_to_bucketfs(bytes, bucketfs_path);
}

/// The bytes surface inside the DB at `/buckets/bfsdefault/default/<name>`.
pub fn put_to_bucketfs(bytes: Vec<u8>, bucketfs_path: &str) {
    if exasol_flavor() == ExasolFlavor::Nano {
        copy_into_nano_bucketfs(&bytes, bucketfs_path);
        return;
    }
    let password = bucketfs_write_password();
    let url = format!(
        "https://{}:{}{}",
        exasol_host(),
        bucketfs_port(),
        bucketfs_path
    );
    // A healthy PUT answers in about a second. bucketfsd holds the response until it unpacks an
    // archive, and its unpacker can stall on a half-written one until a re-PUT touches the file.
    let client = reqwest::blocking::Client::builder()
        .danger_accept_invalid_certs(true)
        .timeout(Duration::from_secs(30))
        .build()
        .expect("build BucketFS HTTPS client");
    // Send errors, including that timeout and BucketFS refusing connections after the SQL
    // port is up, are retried; a non-2xx response is not.
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
            "BucketFS PUT to {url} failed after {MAX_ATTEMPTS} attempts: {:?}",
            last_err.expect("at least one attempt recorded an error")
        )
    });
    assert!(
        resp.status().is_success(),
        "BucketFS PUT to {url} returned {} (expected 2xx)",
        resp.status()
    );
}

/// Nano serves BucketFS from `/exa/bucketfs/bfsdefault/<bucket>/` and has no HTTP endpoint, so
/// the bytes are copied in, and a `.tar.gz` lands extracted as docker-db's BucketFS unpacks it.
fn copy_into_nano_bucketfs(bytes: &[u8], bucketfs_path: &str) {
    let container = exasol_container();
    let relative = bucketfs_path.trim_start_matches('/');
    let (target, archive) = match relative.strip_suffix(".tar.gz") {
        Some(dir) => (dir, true),
        None => (relative, false),
    };
    let staging = std::env::temp_dir().join(format!(
        "lh-bucketfs-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock after the epoch")
            .as_nanos()
    ));
    let staged = staging.join(target);
    let staged_dir = if archive {
        staged.as_path()
    } else {
        staged.parent().expect("a BucketFS path names a bucket")
    };
    std::fs::create_dir_all(staged_dir).unwrap_or_else(|e| panic!("create {staged_dir:?}: {e}"));
    if !archive {
        std::fs::write(&staged, bytes).unwrap_or_else(|e| panic!("write {staged:?}: {e}"));
    }
    // `docker cp` creates no missing parent, so the staged tree carries every directory.
    let staged_tree = format!("{}/.", staging.display());
    let copied = docker_cp(
        &[&staged_tree, &format!("{container}:{NANO_BUCKETFS_ROOT}/")],
        None,
    );
    let _ = std::fs::remove_dir_all(&staging);
    copied.unwrap_or_else(|e| panic!("{e}"));
    if archive {
        // The daemon gunzips and unpacks a tar streamed to `docker cp -`.
        let dest = format!("{container}:{NANO_BUCKETFS_ROOT}/{target}");
        docker_cp(&["-", &dest], Some(bytes)).unwrap_or_else(|e| panic!("{e}"));
    }
    let bucket = relative
        .split('/')
        .next()
        .expect("a BucketFS path names a bucket");
    wait_for_nano_bucket(&container, bucket);
}

/// Nano registers a new bucket directory only when its poller next lists it in `bucketfs.conf`.
/// Until then a script whose language alias points into the bucket fails "Cannot access bucket".
fn wait_for_nano_bucket(container: &str, bucket: &str) {
    let entry = format!("{NANO_BUCKETFS_ROOT}/{bucket}");
    let local = std::env::temp_dir().join(format!("lh-bucketfs-conf-{}", std::process::id()));
    let local_path = local.display().to_string();
    let deadline = Instant::now() + DEFAULT_TIMEOUT;
    loop {
        docker_cp(
            &[&format!("{container}:{NANO_BUCKETFS_CONF}"), &local_path],
            None,
        )
        .unwrap_or_else(|e| panic!("{e}"));
        let conf =
            std::fs::read_to_string(&local).unwrap_or_else(|e| panic!("read {local:?}: {e}"));
        if conf
            .lines()
            .any(|line| line.split_whitespace().next() == Some(entry.as_str()))
        {
            let _ = std::fs::remove_file(&local);
            return;
        }
        assert!(
            Instant::now() < deadline,
            "Nano did not register bucket `{bucket}` within {DEFAULT_TIMEOUT:?}; \
             {NANO_BUCKETFS_CONF} holds:\n{conf}"
        );
        std::thread::sleep(POLL_INTERVAL);
    }
}

const NANO_BUCKETFS_ROOT: &str = "/exa/bucketfs/bfsdefault";
const NANO_BUCKETFS_CONF: &str = "/exa/bucketfs.conf";

fn docker_cp(args: &[&str], stdin: Option<&[u8]>) -> Result<(), String> {
    use std::io::Write;
    let mut child = std::process::Command::new("docker")
        .arg("cp")
        .args(args)
        .stdin(if stdin.is_some() {
            std::process::Stdio::piped()
        } else {
            std::process::Stdio::null()
        })
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .map_err(|e| format!("docker cp {args:?}: {e}"))?;
    if let Some(bytes) = stdin {
        let mut pipe = child.stdin.take().expect("stdin is piped");
        pipe.write_all(bytes)
            .map_err(|e| format!("docker cp {args:?}: writing stdin: {e}"))?;
    }
    let out = child
        .wait_with_output()
        .map_err(|e| format!("docker cp {args:?}: {e}"))?;
    if out.status.success() {
        Ok(())
    } else {
        Err(format!(
            "docker cp {args:?} failed ({}): {}",
            out.status,
            String::from_utf8_lossy(&out.stderr).trim()
        ))
    }
}

#[cfg(any(
    feature = "exasol-e2e",
    feature = "lakekeeper-e2e",
    feature = "azure-e2e",
    feature = "unity-e2e",
    feature = "glue-e2e"
))]
pub fn lakehouse_engine_so_path() -> std::path::PathBuf {
    let manifest = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    manifest
        .parent()
        .and_then(|p| p.parent())
        .expect("could not navigate to workspace root from CARGO_MANIFEST_DIR")
        .join("target/release/liblakehouse_engine.so")
}

/// `path_style` defaults to `false`; SeaweedFS callers must set it explicitly.
#[derive(Default)]
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
    pub token: Option<String>,
    pub client_id: Option<String>,
    pub client_secret: Option<String>,
    pub oauth2_server_uri: Option<String>,
    pub scope: Option<String>,
    pub account_name: Option<String>,
    pub account_key: Option<String>,
    /// AWS IAM role to assume before signing catalog/storage requests. Absent when not supplied.
    pub aws_assume_role_arn: Option<String>,
    /// STS `ExternalId` for the assumed role. Requires `aws_assume_role_arn`. Absent when not supplied.
    pub aws_external_id: Option<String>,
    /// STS endpoint override (e.g. the local SeaweedFS STS). Requires `aws_assume_role_arn`. Absent when not supplied.
    pub aws_sts_endpoint: Option<String>,
}

impl CatalogConnectionPassword {
    /// The same CONNECTION with its SeaweedFS endpoint and token endpoint renamed to the names
    /// only the forward proxy resolves. An empty endpoint, as under vending, stays empty.
    pub fn via_proxy(self) -> Self {
        Self {
            endpoint: if self.endpoint.is_empty() {
                self.endpoint
            } else {
                SEAWEEDFS_VIA_PROXY.to_string()
            },
            oauth2_server_uri: self.oauth2_server_uri.as_deref().map(via_proxy),
            ..self
        }
    }

    /// Single quotes are escaped as `''` for SQL embedding.
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
        if let Some(token) = &self.token {
            obj["token"] = serde_json::Value::String(token.clone());
        }
        if let Some(client_id) = &self.client_id {
            obj["client_id"] = serde_json::Value::String(client_id.clone());
        }
        if let Some(client_secret) = &self.client_secret {
            obj["client_secret"] = serde_json::Value::String(client_secret.clone());
        }
        if let Some(oauth2_server_uri) = &self.oauth2_server_uri {
            obj["oauth2_server_uri"] = serde_json::Value::String(oauth2_server_uri.clone());
        }
        if let Some(scope) = &self.scope {
            obj["scope"] = serde_json::Value::String(scope.clone());
        }
        if let Some(account_name) = &self.account_name {
            obj["account_name"] = serde_json::Value::String(account_name.clone());
        }
        if let Some(account_key) = &self.account_key {
            obj["account_key"] = serde_json::Value::String(account_key.clone());
        }
        if let Some(role_arn) = &self.aws_assume_role_arn {
            obj["aws_assume_role_arn"] = serde_json::Value::String(role_arn.clone());
        }
        if let Some(external_id) = &self.aws_external_id {
            obj["aws_external_id"] = serde_json::Value::String(external_id.clone());
        }
        if let Some(sts_endpoint) = &self.aws_sts_endpoint {
            obj["aws_sts_endpoint"] = serde_json::Value::String(sts_endpoint.clone());
        }
        obj.to_string().replace('\'', "''")
    }
}

pub fn build_create_connection_sql(
    conn_name: &str,
    catalog_uri: &str,
    password: &CatalogConnectionPassword,
) -> String {
    let json_pw = password.to_sql_password_json();
    let safe_uri = catalog_uri.replace('\'', "''");
    format!(
        "CREATE OR REPLACE CONNECTION {conn_name} TO '{safe_uri}' USER '' IDENTIFIED BY '{json_pw}'"
    )
}

#[cfg(any(
    feature = "exasol-e2e",
    feature = "lakekeeper-e2e",
    feature = "azure-e2e",
    feature = "unity-e2e",
    feature = "glue-e2e"
))]
pub fn local_stack_connection_password() -> CatalogConnectionPassword {
    CatalogConnectionPassword {
        warehouse: "s3://warehouse/".to_string(),
        endpoint: seaweedfs_url_internal(),
        region: "us-east-1".to_string(),
        access_key: "lhadmin".to_string(),
        secret_key: "lhadminsecret123".to_string(),
        session_token: None,
        path_style: true,
        use_sigv4: false,
        use_vended_credentials: false,
        ..Default::default()
    }
}

/// A libtest panic payload is always a `&'static str` or a `String`.
pub fn panic_payload_message(payload: &(dyn std::any::Any + Send)) -> Option<String> {
    payload
        .downcast_ref::<String>()
        .cloned()
        .or_else(|| payload.downcast_ref::<&str>().map(|s| (*s).to_string()))
}

#[cfg(test)]
#[path = "stack_tests.rs"]
mod tests;
