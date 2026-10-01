use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;

use iceberg::spec::TableMetadata;

pub(super) type SourceFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

const NOT_FOUND_CODE: &str = "EntityNotFoundException";

/// One Glue table registration, as the session routes and declares it.
#[derive(Debug, Clone, Default, PartialEq)]
pub(super) struct GlueTable {
    pub name: String,
    /// Glue's `TableType`, e.g. `EXTERNAL_TABLE` or `VIRTUAL_VIEW`.
    pub table_type: Option<String>,
    pub parameters: HashMap<String, String>,
    pub input_format: Option<String>,
    pub location: Option<String>,
    pub columns: Vec<GlueColumn>,
    pub partition_keys: Vec<GlueColumn>,
}

#[derive(Debug, Clone, PartialEq)]
pub(super) struct GlueColumn {
    pub name: String,
    pub hive_type: String,
}

#[derive(Debug, Clone, Default, PartialEq)]
pub(super) struct GluePartition {
    /// Positional against the table's partition keys.
    pub values: Vec<String>,
    pub location: Option<String>,
    pub input_format: Option<String>,
}

/// Why a Glue call failed, classified by the service error code, never the HTTP status.
#[derive(Debug, Clone, PartialEq)]
pub(super) enum GlueFailure {
    NotFound { message: String },
    SigningTime { code: String, message: String },
    Service { code: String, message: String },
    Uncoded { status: u16, message: String },
    TimedOut { seconds: u64 },
    Request(String),
}

/// What the session reads from Glue and the Iceberg metadata files Glue points to.
pub(super) trait GlueSource: Send + Sync {
    fn tables<'a>(
        &'a self,
        database: &'a str,
    ) -> SourceFuture<'a, Result<Vec<GlueTable>, GlueFailure>>;

    fn table<'a>(
        &'a self,
        database: &'a str,
        name: &'a str,
    ) -> SourceFuture<'a, Result<Option<GlueTable>, GlueFailure>>;

    fn partitions<'a>(
        &'a self,
        database: &'a str,
        table: &'a str,
    ) -> SourceFuture<'a, Result<Vec<GluePartition>, GlueFailure>>;

    fn iceberg_metadata<'a>(
        &'a self,
        location: &'a str,
    ) -> SourceFuture<'a, Result<TableMetadata, String>>;
}

pub(super) fn classify_service_error(
    code: Option<&str>,
    message: Option<&str>,
    status: u16,
) -> GlueFailure {
    let message = message.unwrap_or("(no message)").to_string();
    match code {
        Some(NOT_FOUND_CODE) => GlueFailure::NotFound { message },
        Some(code) if is_signing_time_rejection(code, &message) => GlueFailure::SigningTime {
            code: code.to_string(),
            message,
        },
        Some(code) => GlueFailure::Service {
            code: code.to_string(),
            message,
        },
        None => GlueFailure::Uncoded { status, message },
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

impl GlueFailure {
    pub(super) fn text(&self, operation: &str, subject: &str) -> String {
        match self {
            Self::NotFound { message } => format!(
                "Glue {operation} failed: {subject} does not exist ({NOT_FOUND_CODE}: {message})"
            ),
            Self::SigningTime { code, message } => format!(
                "Glue {operation} failed for {subject}: the Exasol node's clock differs from AWS \
                 time, so AWS rejected the request's signing time; synchronize the node's clock \
                 ({code}: {message})"
            ),
            Self::Service { code, message } => {
                format!("Glue {operation} failed for {subject}: {code}: {message}")
            }
            Self::Uncoded { status, message } => {
                format!("Glue {operation} failed for {subject}: HTTP {status}: {message}")
            }
            Self::TimedOut { seconds } => format!(
                "Glue {operation} for {subject} did not complete within {seconds} seconds, \
                 retries included"
            ),
            Self::Request(detail) => {
                format!("Glue {operation} request for {subject} failed: {detail}")
            }
        }
    }
}

#[cfg(test)]
#[path = "source_tests.rs"]
mod tests;
