use std::time::Duration;

use aws_sdk_glue::config::retry::RetryConfig;
use aws_sdk_glue::config::timeout::TimeoutConfig;
use aws_sdk_glue::config::{BehaviorVersion, Credentials, Region};
use aws_sdk_glue::error::{ProvideErrorMetadata, SdkError};

use crate::ConnectionCreds;
use crate::aws_error::{SdkFailure, sdk_failure};

const MAX_ATTEMPTS: u32 = 5;
/// Bounds each call, retries included, because it runs inside a pushdown.
const OPERATION_TIMEOUT: Duration = Duration::from_secs(30);
const CREDENTIALS_PROVIDER: &str = "exasol-connection";
const NOT_FOUND_CODE: &str = "EntityNotFoundException";

pub(super) fn glue_client(
    address: &str,
    creds: &ConnectionCreds,
    region: String,
) -> aws_sdk_glue::Client {
    let config = aws_sdk_glue::Config::builder()
        .behavior_version(BehaviorVersion::latest())
        .credentials_provider(Credentials::new(
            creds.access_key.clone(),
            creds.secret_key.clone(),
            creds.session_token.clone(),
            None,
            CREDENTIALS_PROVIDER,
        ))
        .region(Region::new(region))
        .endpoint_url(address)
        .retry_config(RetryConfig::standard().with_max_attempts(MAX_ATTEMPTS))
        .timeout_config(
            TimeoutConfig::builder()
                .operation_timeout(OPERATION_TIMEOUT)
                .build(),
        );
    aws_sdk_glue::Client::from_conf(config.build())
}

/// Glue rejects an empty `CatalogId`, so a blank `warehouse` sends none and Glue uses the
/// caller's account.
pub(super) fn catalog_id(warehouse: &str) -> Option<String> {
    Some(warehouse.to_string()).filter(|id| !id.trim().is_empty())
}

/// Glue can return a non-null token after the last page, so an empty page also ends a listing.
pub(super) fn next_page_token(token: Option<&str>, page_len: usize) -> Option<String> {
    if page_len == 0 {
        return None;
    }
    token.filter(|token| !token.is_empty()).map(str::to_string)
}

/// Classified by the service error code, never the HTTP status.
pub(super) fn error_text<E: ProvideErrorMetadata>(
    error: &SdkError<E>,
    operation: &str,
    subject: &str,
) -> String {
    match sdk_failure(error, "Glue") {
        SdkFailure::Service {
            status,
            code,
            message,
        } => {
            let message = message.unwrap_or("(no message)");
            match code {
                Some(NOT_FOUND_CODE) => format!(
                    "Glue {operation} failed: {subject} does not exist \
                     ({NOT_FOUND_CODE}: {message})"
                ),
                Some(code) if is_signing_time_rejection(code, message) => format!(
                    "Glue {operation} failed for {subject}: the Exasol node's clock differs from \
                     AWS time, so AWS rejected the request's signing time; synchronize the node's \
                     clock ({code}: {message})"
                ),
                Some(code) => format!("Glue {operation} failed for {subject}: {code}: {message}"),
                None => format!("Glue {operation} failed for {subject}: HTTP {status}: {message}"),
            }
        }
        SdkFailure::TimedOut => format!(
            "Glue {operation} for {subject} did not complete within {} seconds, retries included",
            OPERATION_TIMEOUT.as_secs()
        ),
        SdkFailure::Request(detail) => {
            format!("Glue {operation} request for {subject} failed: {detail}")
        }
    }
}

/// AWS's own codes and texts for a signature outside its time window. One that reaches here
/// survived the SDK's skew-corrected retries.
fn is_signing_time_rejection(code: &str, message: &str) -> bool {
    matches!(code, "RequestTimeTooSkewed" | "RequestExpired")
        || (code == "InvalidSignatureException"
            && (message.starts_with("Signature expired")
                || message.starts_with("Signature not yet current")))
}

#[cfg(test)]
#[path = "sdk_tests.rs"]
mod tests;
