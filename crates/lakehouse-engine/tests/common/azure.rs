//! Two credential paths, never conflated: the account name/key pair is the data path
//! under test (reaches the Exasol CONNECTION); the tenant/client/secret triple only
//! lets the harness create and delete its own container.

use super::cloud_fixture::{
    derive_run_segment, per_run_segment, require_var, run_teardown_off_runtime,
};

use anyhow::{Context, Result, bail};
use azure_core::credentials::Secret;
use azure_core::http::Url;
use azure_identity::ClientSecretCredential;
use azure_storage_blob::models::StorageErrorCode;
use azure_storage_blob::{BlobContainerClient, BlobServiceClient, StorageError};

/// Azure's blob-container limit and Lakekeeper's ADLS filesystem-name limit.
const MAX_CONTAINER_NAME_LEN: usize = 63;

/// Azure's blob-container limit and Lakekeeper's ADLS filesystem-name limit.
const MIN_CONTAINER_NAME_LEN: usize = 3;

/// Keeps an orphan left by a killed run attributable to this suite.
const CONTAINER_NAME_PREFIX: &str = "lhrs-e2e";

pub fn account_name() -> String {
    read_var("AZURE_STORAGE_ACCOUNT_NAME")
}

pub fn account_key() -> String {
    read_var("AZURE_STORAGE_ACCOUNT_KEY")
}

fn tenant_id() -> String {
    read_var("AZURE_TENANT_ID")
}

fn client_id() -> String {
    read_var("AZURE_CLIENT_ID")
}

/// Must never reach the CONNECTION, or the suite would pass without exercising the
/// account-key path it exists to verify.
fn client_secret() -> String {
    read_var("AZURE_CLIENT_SECRET")
}

fn read_var(name: &str) -> String {
    require_var("azure-e2e", name, std::env::var(name).ok().as_deref())
}

/// Leaves room for `CONTAINER_NAME_PREFIX` and the hyphen after it.
const RUN_SEGMENT_LEN: usize = MAX_CONTAINER_NAME_LEN - CONTAINER_NAME_PREFIX.len() - 1;

/// Lakekeeper rejects a name outside 3-63 chars of `[a-z0-9-]` with no
/// consecutive/leading/trailing hyphens at warehouse creation, so it is enforced here.
pub fn per_run_container_name() -> String {
    format!(
        "{CONTAINER_NAME_PREFIX}-{}",
        per_run_segment('-', RUN_SEGMENT_LEN)
    )
}

fn derive_container_name(user: &str, millis: u128) -> String {
    format!(
        "{CONTAINER_NAME_PREFIX}-{}",
        derive_run_segment(user, millis, '-', RUN_SEGMENT_LEN)
    )
}

/// Plain owned data so a clone can cross into the teardown thread (`spawn` needs `'static`).
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

    /// Rebuilt per use, not cached: an `azure_core` client's connection pool runs on the
    /// runtime that created it, and reusing it from `Drop` would deadlock on the fixture's
    /// runtime, which is blocked in `Drop`'s own `join()`.
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

/// A failure that never reached the service carries no code; inventing one would let
/// `delete` misread a transport failure as `ContainerNotFound`. `StorageError`'s
/// `Display` prints mapped and unmapped codes identically, so unmapped is flagged.
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

fn delete_reached_desired_state(code: Option<&StorageErrorCode>) -> bool {
    matches!(code, Some(StorageErrorCode::ContainerNotFound))
}

fn is_name_collision(code: Option<&StorageErrorCode>) -> bool {
    matches!(code, Some(StorageErrorCode::ContainerAlreadyExists))
}

/// Lakekeeper validates physical access at warehouse creation, so the container must
/// exist first and be deleted after. Hold it on the test's stack, not in a `OnceLock`:
/// statics are never dropped. A killed process still orphans the container.
pub struct AzureContainer {
    access: ContainerAccess,
}

impl AzureContainer {
    /// The blob crate accepts Entra ID only, so the account key never reaches this call. An
    /// existing container fails the run rather than being adopted, since `Drop` would delete it.
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
    fn drop(&mut self) {
        let container_name = self.access.container_name.clone();
        let access = self.access.clone();
        match run_teardown_off_runtime(
            "azure-container-teardown",
            async move { access.delete().await },
        ) {
            Ok(Ok(())) => {}
            Ok(Err(error)) => eprintln!("LEAKED Azure container {container_name}: {error:#}"),
            Err(failure) => eprintln!("LEAKED Azure container {container_name}: {failure}"),
        }
    }
}

pub async fn container_exists(container_name: &str) -> Result<bool> {
    let access = ContainerAccess::from_environment(container_name);
    let client = access.blob_container_client()?;
    Ok(client.exists().await?)
}

mod azure_naming_tests {
    use super::{
        CONTAINER_NAME_PREFIX, MAX_CONTAINER_NAME_LEN, MIN_CONTAINER_NAME_LEN,
        derive_container_name,
    };

    const FIXED_MILLIS: u128 = 1_762_000_000_000;

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
}

mod azure_error_classification_tests {
    use super::{azure_failure, delete_reached_desired_state, is_name_collision};
    use azure_core::error::ErrorKind;
    use azure_core::http::StatusCode;
    use azure_storage_blob::models::StorageErrorCode;

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
