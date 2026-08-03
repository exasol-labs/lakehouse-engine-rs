//! Azure credential accessors and per-run container naming for the `azure-e2e` suite.
//!
//! Two credential paths, never conflated: the account name/key pair is the data
//! path under test (reaches the Exasol CONNECTION), while the tenant/client/secret
//! triple only lets the harness create and delete its own container. Every value
//! is read explicitly — `azure_identity` 1.x has no environment-scanning
//! credential — and an absent one panics immediately rather than surfacing later
//! as an opaque authorization failure.

use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, bail};
use azure_core::credentials::Secret;
use azure_core::http::Url;
use azure_identity::ClientSecretCredential;
use azure_storage_blob::models::StorageErrorCode;
use azure_storage_blob::{BlobContainerClient, BlobServiceClient, StorageError};

/// Longest name Azure accepts for a blob container, and Lakekeeper's limit on an
/// ADLS filesystem name.
const MAX_CONTAINER_NAME_LEN: usize = 63;

/// Shortest name Azure accepts for a blob container, and Lakekeeper's limit on an
/// ADLS filesystem name.
const MIN_CONTAINER_NAME_LEN: usize = 3;

/// Marks a container as this suite's, so an orphan left by a killed run stays
/// attributable.
const CONTAINER_NAME_PREFIX: &str = "lhrs-e2e";

/// Storage account under test. Data path: reaches the Exasol CONNECTION.
pub fn account_name() -> String {
    read_var("AZURE_STORAGE_ACCOUNT_NAME")
}

/// Account key under test — the `AdlsCred::AccountKey` path. Data path: reaches
/// the Exasol CONNECTION, the warehouse storage credential, and the seed `FileIO`.
pub fn account_key() -> String {
    read_var("AZURE_STORAGE_ACCOUNT_KEY")
}

/// Entra ID tenant of the container-lifecycle service principal. Harness only.
fn tenant_id() -> String {
    read_var("AZURE_TENANT_ID")
}

/// Entra ID client id of the container-lifecycle service principal. Harness only.
fn client_id() -> String {
    read_var("AZURE_CLIENT_ID")
}

/// Entra ID client secret of the container-lifecycle service principal. Harness
/// only — it must never reach the CONNECTION, or the suite would pass without
/// exercising the account-key path it exists to verify.
fn client_secret() -> String {
    read_var("AZURE_CLIENT_SECRET")
}

fn read_var(name: &str) -> String {
    require_var(name, std::env::var(name).ok().as_deref())
}

/// Return `value` trimmed, panicking when the variable is unset or blank.
///
/// Takes `value` as a parameter so the panic path is testable without mutating
/// the process environment other tests share. Panics name only the variable,
/// never the value — three of the five are credentials.
fn require_var(name: &str, value: Option<&str>) -> String {
    let Some(value) = value else {
        panic!("the azure-e2e suite requires environment variable {name}, which is not set");
    };
    let value = value.trim();
    assert!(
        !value.is_empty(),
        "the azure-e2e suite requires environment variable {name}, which is set but empty"
    );
    value.to_string()
}

/// Container name for this run: `lhrs-e2e-<sanitized-user>-<millis>`.
///
/// The millisecond suffix makes a create-time name collision a defect rather
/// than tolerable, and keeps an orphan attributable to one run; `$USER` keeps it
/// attributable to a person. An unset `$USER` degrades to no segment instead of
/// failing — it's cosmetic, not a credential.
pub fn per_run_container_name() -> String {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock is before the UNIX epoch")
        .as_millis();
    derive_container_name(&std::env::var("USER").unwrap_or_default(), millis)
}

/// Build the per-run container name from an arbitrary `user` and `millis`.
///
/// Azure and Lakekeeper both require 3-63 characters of `[a-z0-9-]`, no
/// consecutive/leading/trailing hyphens — enforced here since Lakekeeper rejects
/// a violation at warehouse-creation time, not at scan time. `user` is arbitrary
/// (empty, mixed-case, dotted, non-ASCII, over-long), so it's sanitized,
/// truncated to the remaining budget, and dropped entirely rather than leaving
/// the `--` an empty segment would produce.
fn derive_container_name(user: &str, millis: u128) -> String {
    let suffix = millis.to_string();
    let hyphens = 2;
    let budget =
        MAX_CONTAINER_NAME_LEN.saturating_sub(CONTAINER_NAME_PREFIX.len() + hyphens + suffix.len());
    let user_segment = sanitize_segment(user, budget);
    if user_segment.is_empty() {
        format!("{CONTAINER_NAME_PREFIX}-{suffix}")
    } else {
        format!("{CONTAINER_NAME_PREFIX}-{user_segment}-{suffix}")
    }
}

fn sanitize_segment(raw: &str, max_len: usize) -> String {
    let mut segment = String::new();
    for character in raw.chars() {
        let legal = if character.is_ascii_alphanumeric() {
            character.to_ascii_lowercase()
        } else {
            '-'
        };
        if legal == '-' && segment.ends_with('-') {
            continue;
        }
        if segment.len() == max_len {
            break;
        }
        segment.push(legal);
    }
    segment.trim_matches('-').to_string()
}

/// Everything needed to reach the per-run container: account/container names
/// plus the container-lifecycle service principal's three Entra ID values.
///
/// Plain owned data, no client or runtime handle, so a clone can cross into the
/// teardown thread and satisfy `spawn`'s `'static` bound — which `Drop`'s
/// `&mut self` cannot.
#[derive(Clone)]
struct ContainerAccess {
    account_name: String,
    container_name: String,
    tenant_id: String,
    client_id: String,
    client_secret: String,
}

impl ContainerAccess {
    fn from_environment(container_name: &str) -> Self {
        Self {
            account_name: account_name(),
            container_name: container_name.to_string(),
            tenant_id: tenant_id(),
            client_id: client_id(),
            client_secret: client_secret(),
        }
    }

    /// Build a client for this container on the calling thread's current runtime.
    ///
    /// Rebuilt per use rather than cached: `azure_core` gives each client its own
    /// connection pool driven by tasks on whichever runtime created it. Reusing
    /// the construction-time client from `Drop` would dispatch onto the
    /// fixture's runtime, which is blocked in `Drop`'s own `join()` and polling
    /// nothing — deadlocking the delete with no timeout to break it.
    fn blob_container_client(&self) -> Result<BlobContainerClient> {
        let credential = ClientSecretCredential::new(
            &self.tenant_id,
            self.client_id.clone(),
            Secret::new(self.client_secret.clone()),
            None,
        )
        .context("build the container-lifecycle service-principal credential")?;

        let service_url = Url::parse(&format!(
            "https://{}.blob.core.windows.net/",
            self.account_name
        ))
        .context("build the blob service URL from AZURE_STORAGE_ACCOUNT_NAME")?;

        let service_client = BlobServiceClient::new(service_url, Some(credential), None)
            .context("build the blob service client for the test storage account")?;

        Ok(service_client.blob_container_client(&self.container_name))
    }

    /// Delete the container, treating an already-absent one as the desired end state.
    async fn delete(&self) -> Result<()> {
        let client = self.blob_container_client()?;
        match client.delete(None).await {
            Ok(_) => Ok(()),
            Err(error) => {
                let (code, description) = azure_failure(error);
                if delete_reached_desired_state(code.as_ref()) {
                    return Ok(());
                }
                bail!("delete container {}: {description}", self.container_name)
            }
        }
    }
}

/// The Azure error code `error` carried, paired with a printable rendering.
///
/// A failure that never reached the service (DNS/TLS/timeout) carries no Azure
/// code — inventing one here would let [`ContainerAccess::delete`] misread a
/// transport failure as `ContainerNotFound` and call a still-present container
/// cleaned up. An unmapped code arrives as `UnknownValue`; the rendering flags
/// that explicitly, since `StorageError`'s `Display` prints mapped and unmapped
/// codes identically.
fn azure_failure(error: azure_core::Error) -> (Option<StorageErrorCode>, String) {
    match StorageError::try_from(error) {
        Ok(storage_error) => {
            let unmapped = match &storage_error.error_code {
                Some(StorageErrorCode::UnknownValue(code)) => {
                    format!(" — azure_storage_blob 1.0 does not map the code {code}")
                }
                _ => String::new(),
            };
            let description = format!("{}{unmapped}", storage_error.to_string().trim_end());
            (storage_error.error_code, description)
        }
        Err(original) => (None, format!("no Azure error code: {original}")),
    }
}

/// Whether `code` means the container was already absent — delete's desired
/// end state. Only `ContainerNotFound` qualifies; every other code, an unmapped
/// code, or `None` must be reported instead, or teardown would call a
/// surviving container cleaned up.
fn delete_reached_desired_state(code: Option<&StorageErrorCode>) -> bool {
    matches!(code, Some(StorageErrorCode::ContainerNotFound))
}

/// Whether `code` means the container name was already taken — a defect, since
/// [`per_run_container_name`]'s millisecond suffix should make collisions
/// impossible. Only `ContainerAlreadyExists` qualifies; every other code keeps
/// its own description.
fn is_name_collision(code: Option<&StorageErrorCode>) -> bool {
    matches!(code, Some(StorageErrorCode::ContainerAlreadyExists))
}

/// An Azure blob container that lives exactly as long as this value.
///
/// Lakekeeper validates physical access at warehouse-creation time, so the
/// container must exist first and be deleted after, or the shared test account
/// accumulates one per run. Hold this on a test function's stack — unwinding
/// runs `Drop`, so a panicking test still cleans up; a guard parked in a
/// `OnceLock` never would, since statics aren't dropped at process exit.
///
/// Known ceiling: a *killed* process skips `Drop` and orphans the container;
/// the per-run name keeps it attributable.
pub struct AzureContainer {
    access: ContainerAccess,
}

impl AzureContainer {
    /// Create `container_name` in the test storage account and own its deletion.
    ///
    /// Authenticates with the container-lifecycle service principal — the
    /// official blob crate accepts Entra ID only, so the account key never
    /// reaches this call. An already-existing container fails the run rather
    /// than being adopted: a collision is a defect (see
    /// [`per_run_container_name`]), and adopting one would hand `Drop` a
    /// container this run didn't create.
    pub async fn create(container_name: &str) -> Result<Self> {
        let access = ContainerAccess::from_environment(container_name);
        let client = access.blob_container_client()?;

        match client.create(None).await {
            Ok(_) => Ok(Self { access }),
            Err(error) => {
                let (code, description) = azure_failure(error);
                if is_name_collision(code.as_ref()) {
                    bail!(
                        "container {container_name} already exists in storage account {}: the \
                         per-run millisecond suffix makes a name collision a defect, not a state \
                         to adopt",
                        access.account_name
                    );
                }
                bail!("create container {container_name}: {description}")
            }
        }
    }
}

impl Drop for AzureContainer {
    /// Delete the container on a thread of its own, with a runtime of its own.
    ///
    /// `Drop` fires synchronously inside the fixture's `rt.block_on(…)`; driving
    /// the delete on that ambient runtime would panic with "Cannot start a
    /// runtime from within a runtime", aborting the process instead of deleting
    /// anything while unwinding. A separate thread with its own runtime and
    /// client works regardless of whether `Drop` fires inside or outside a
    /// runtime context. Nothing here panics — every failure is reported by
    /// name, so an unwinding test keeps its original failure and any orphan
    /// stays traceable.
    fn drop(&mut self) {
        let container_name = self.access.container_name.clone();
        let access = self.access.clone();

        // `Builder::spawn` over `thread::spawn`: the latter panics when the OS
        // refuses the thread, and a panic here would abort an unwinding test.
        let teardown = std::thread::Builder::new()
            .name("azure-container-teardown".to_string())
            .spawn(move || {
                tokio::runtime::Builder::new_current_thread()
                    // HTTP needs the IO driver, timeouts need the timer. Named
                    // explicitly — `enable_all` silently skips IO when tokio's
                    // `net` feature is off.
                    .enable_io()
                    .enable_time()
                    .build()
                    .context("build the container-teardown runtime")?
                    .block_on(access.delete())
            });

        let joined = match teardown {
            Ok(handle) => handle.join(),
            Err(error) => {
                eprintln!(
                    "LEAKED Azure container {container_name}: its teardown thread could not be \
                     spawned: {error}"
                );
                return;
            }
        };

        match joined {
            Ok(Ok(())) => {}
            Ok(Err(error)) => eprintln!("LEAKED Azure container {container_name}: {error:#}"),
            Err(_) => {
                eprintln!("LEAKED Azure container {container_name}: its teardown thread panicked")
            }
        }
    }
}

/// Whether `container_name` currently exists in the test storage account.
///
/// Reuses [`ContainerAccess`] rather than duplicating it — exists only so a
/// container-guard test can prove a container is gone after its guard's scope
/// ends, without holding a second `AzureContainer`.
pub async fn container_exists(container_name: &str) -> Result<bool> {
    let access = ContainerAccess::from_environment(container_name);
    let client = access.blob_container_client()?;
    Ok(client.exists().await?)
}

#[cfg(test)]
mod azure_credentials_and_naming_tests {
    use super::{
        CONTAINER_NAME_PREFIX, MAX_CONTAINER_NAME_LEN, MIN_CONTAINER_NAME_LEN,
        derive_container_name, require_var,
    };
    use std::panic;

    /// Fixed so naming assertions never depend on the wall clock.
    const FIXED_MILLIS: u128 = 1_762_000_000_000;

    fn panic_message(body: impl FnOnce() + panic::UnwindSafe) -> String {
        let payload = panic::catch_unwind(body).expect_err("expected require_var to panic");
        super::super::stack::panic_payload_message(&*payload)
            .expect("panic payload was neither String nor &str")
    }

    fn assert_legal_container_name(name: &str, user: &str) {
        assert!(
            (MIN_CONTAINER_NAME_LEN..=MAX_CONTAINER_NAME_LEN).contains(&name.len()),
            "user {user:?}: name {name:?} must be {MIN_CONTAINER_NAME_LEN} to \
             {MAX_CONTAINER_NAME_LEN} characters, got {}",
            name.len()
        );
        assert!(
            name.chars()
                .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-'),
            "user {user:?}: name {name:?} must contain only lowercase letters, digits and hyphens"
        );
        assert!(
            !name.contains("--"),
            "user {user:?}: name {name:?} must not contain consecutive hyphens"
        );
        assert!(
            !name.starts_with('-') && !name.ends_with('-'),
            "user {user:?}: name {name:?} must not begin or end with a hyphen"
        );
        assert!(
            name.starts_with(CONTAINER_NAME_PREFIX),
            "user {user:?}: name {name:?} must keep the {CONTAINER_NAME_PREFIX} prefix"
        );
        assert!(
            name.ends_with(&FIXED_MILLIS.to_string()),
            "user {user:?}: name {name:?} must keep the millisecond suffix"
        );
    }

    #[test]
    fn container_name_is_azure_and_lakekeeper_legal() {
        let ninety_chars = "A".repeat(90);
        let truncated_at_a_hyphen = format!("{}.tail", "a".repeat(39));

        for user in [
            "",
            "-",
            "---",
            "Antoni.Reus",
            "a..b",
            "ÜBER_user",
            "9",
            ninety_chars.as_str(),
            truncated_at_a_hyphen.as_str(),
        ] {
            assert_legal_container_name(&derive_container_name(user, FIXED_MILLIS), user);
        }

        assert_eq!(
            derive_container_name("", FIXED_MILLIS),
            format!("lhrs-e2e-{FIXED_MILLIS}"),
            "an empty user leaves no segment rather than a double hyphen"
        );
        assert_eq!(
            derive_container_name("---", FIXED_MILLIS),
            format!("lhrs-e2e-{FIXED_MILLIS}"),
            "a user of only punctuation leaves no segment"
        );
        assert_eq!(
            derive_container_name("Antoni.Reus", FIXED_MILLIS),
            format!("lhrs-e2e-antoni-reus-{FIXED_MILLIS}")
        );
        assert_eq!(
            derive_container_name("a..b", FIXED_MILLIS),
            format!("lhrs-e2e-a-b-{FIXED_MILLIS}"),
            "consecutive illegal characters collapse to one hyphen"
        );
        assert_eq!(
            derive_container_name("ÜBER_user", FIXED_MILLIS),
            format!("lhrs-e2e-ber-user-{FIXED_MILLIS}"),
            "a multi-byte character maps to one hyphen, trimmed at the segment start"
        );
        assert_eq!(
            derive_container_name(&truncated_at_a_hyphen, FIXED_MILLIS),
            format!("lhrs-e2e-{}-{FIXED_MILLIS}", "a".repeat(39)),
            "truncation on a hyphen drops it instead of leaving a trailing one"
        );
        assert_eq!(
            derive_container_name(&ninety_chars, FIXED_MILLIS).len(),
            MAX_CONTAINER_NAME_LEN,
            "an over-long user is truncated to exactly the remaining budget"
        );
    }

    #[test]
    fn missing_credential_variable_fails_loud() {
        for absent in [None, Some(""), Some("   ")] {
            let message = panic_message(|| {
                require_var("AZURE_CLIENT_SECRET", absent);
            });
            assert!(
                message.contains("AZURE_CLIENT_SECRET"),
                "panic for {absent:?} must name the variable, got: {message}"
            );
            assert!(
                !message.contains("   "),
                "panic for {absent:?} must not echo the value, got: {message}"
            );
        }
    }

    #[test]
    fn present_credential_variable_is_read_without_surrounding_whitespace() {
        assert_eq!(
            require_var("AZURE_STORAGE_ACCOUNT_KEY", Some(" a2V5\n")),
            "a2V5",
            "a value sourced from test.env or a CI secret may carry a line ending"
        );
    }
}

#[cfg(test)]
mod azure_error_classification_tests {
    use super::{azure_failure, delete_reached_desired_state, is_name_collision};
    use azure_core::error::ErrorKind;
    use azure_core::http::StatusCode;
    use azure_storage_blob::models::StorageErrorCode;

    /// A code is only ever reported when Azure actually sent one.
    ///
    /// `delete` treats exactly `ContainerNotFound` as "already gone", so
    /// inventing a code for a failure that never reached the service would
    /// misreport a transport error as a cleaned-up container. An HTTP failure
    /// with no raw response is included deliberately: `TryFrom` needs the
    /// response body to build a `StorageError` and hands the error back
    /// untouched without it.
    #[test]
    fn failure_without_a_service_response_carries_no_error_code() {
        let failures = [
            azure_core::Error::with_message(ErrorKind::Connection, "connection refused"),
            azure_core::Error::with_message(ErrorKind::Io, "read timed out"),
            azure_core::Error::with_message(ErrorKind::Credential, "token request rejected"),
            ErrorKind::HttpResponse {
                status: StatusCode::NotFound,
                error_code: Some("ContainerNotFound".to_string()),
                raw_response: None,
            }
            .into_error(),
        ];

        for failure in failures {
            let rendered = failure.to_string();
            let (code, description) = azure_failure(failure);

            assert!(
                code.is_none(),
                "{rendered}: a failure carrying no service response must yield no Azure error \
                 code, got {code:?}"
            );
            assert!(
                !description.is_empty(),
                "{rendered}: the failure must still be described, or teardown reports a leak \
                 with no reason"
            );
        }
    }

    /// Each container-guard clause keys on exactly one Azure code.
    ///
    /// Widening the delete clause (only `ContainerNotFound`) would make
    /// teardown call a surviving container cleaned up; widening the create
    /// clause (only `ContainerAlreadyExists`) would blame a name collision for
    /// an unrelated create failure.
    #[test]
    fn container_guard_keys_each_spec_clause_on_exactly_one_code() {
        let unmapped = StorageErrorCode::UnknownValue("SomethingNew".to_string());

        assert!(
            delete_reached_desired_state(Some(&StorageErrorCode::ContainerNotFound)),
            "a container already absent at delete time SHALL be treated as deleted"
        );
        for other in [
            Some(&StorageErrorCode::ContainerAlreadyExists),
            Some(&unmapped),
            None,
        ] {
            assert!(
                !delete_reached_desired_state(other),
                "{other:?} is not an absent container: treating it as the desired end state \
                 would report a surviving container as cleaned up"
            );
        }

        assert!(
            is_name_collision(Some(&StorageErrorCode::ContainerAlreadyExists)),
            "a name collision at create time SHALL fail the run"
        );
        for other in [
            Some(&StorageErrorCode::ContainerNotFound),
            Some(&unmapped),
            None,
        ] {
            assert!(
                !is_name_collision(other),
                "{other:?} is not a name collision: it must fail with its own description \
                 instead of one blaming the container name"
            );
        }
    }
}
