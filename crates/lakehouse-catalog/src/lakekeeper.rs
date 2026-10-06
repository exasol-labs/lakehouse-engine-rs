//! Lakekeeper management calls are free functions, not `CatalogSession` methods, so a
//! Lakekeeper-only concern stays off the session that every Iceberg REST catalog shares.

use crate::iceberg_io::{PostAnswer, authed_post_json, quote_catalog_body};
use crate::namespace::parse_table_ident;
use crate::{CatalogSession, ConnectionCreds};
use exasol_udf_sdk::error::UdfError;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::time::Duration;

const CATALOG_SEGMENT: &str = "/catalog";
const MANAGEMENT_SEGMENT: &str = "/management";
const BATCH_CHECK_PATH: &str = "/v1/action/batch-check";
const READ_CHECK_ID: &str = "read-data";
const CANNOT_INSPECT_ERROR_TYPE: &str = "CannotInspectPermissions";
const NEEDS_LAKEKEEPER: &str = "The catalog must be a Lakekeeper server.";
/// The pushdown waits on the batch-check, so a stalled server fails the query instead of
/// hanging it.
const BATCH_CHECK_DEADLINE: Duration = Duration::from_secs(30);

/// Whether the principal can read the table; a missing table is `allowed: false` too.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableReadDecision {
    pub table: String,
    pub allowed: bool,
}

/// Swaps `/catalog` for `/management` under one base, so a bearer token goes only to the catalog's
/// origin. A path-rewriting gateway is rejected: no config value names the management API.
pub fn lakekeeper_management_url(catalog_uri: &str) -> Result<String, UdfError> {
    let trimmed = catalog_uri.strip_suffix('/').unwrap_or(catalog_uri);
    let path_ends_in_catalog = url::Url::parse(trimmed)
        .is_ok_and(|parsed| parsed.path().ends_with(CATALOG_SEGMENT) && parsed.query().is_none());
    match trimmed.strip_suffix(CATALOG_SEGMENT) {
        Some(base) if path_ends_in_catalog => Ok(format!("{base}{MANAGEMENT_SEGMENT}")),
        _ => Err(UdfError::User(format!(
            "the Lakekeeper permission check needs a catalog URI ending in '{CATALOG_SEGMENT}', \
             got '{catalog_uri}'"
        ))),
    }
}

/// One decision per distinct table, in first-seen order; the warehouse is the session's
/// `/v1/config` prefix. A timeout or unreadable answer is an error naming the URL, never an allow.
pub async fn lakekeeper_batch_check(
    session: &CatalogSession,
    principal: &str,
    tables: &[&str],
    creds: &ConnectionCreds,
) -> Result<Vec<TableReadDecision>, UdfError> {
    batch_check_within(session, principal, tables, creds, BATCH_CHECK_DEADLINE).await
}

async fn batch_check_within(
    session: &CatalogSession,
    principal: &str,
    tables: &[&str],
    creds: &ConnectionCreds,
    deadline: Duration,
) -> Result<Vec<TableReadDecision>, UdfError> {
    let distinct = distinct_in_first_seen_order(tables);
    if distinct.is_empty() {
        return Ok(Vec::new());
    }
    let url = format!(
        "{}{BATCH_CHECK_PATH}",
        lakekeeper_management_url(session.catalog_uri())?
    );
    let request = build_batch_check(principal, &distinct, session.prefix())?;

    let answer = authed_post_json(
        session.client(),
        &url,
        &request,
        session.auth(),
        creds,
        deadline,
    )
    .await
    .map_err(|error| {
        UdfError::User(format!(
            "the Lakekeeper batch-check at {url} could not be completed: {error}. \
             {NEEDS_LAKEKEEPER}"
        ))
    })?;

    match answer {
        PostAnswer::Accepted { status, body } => {
            read_decisions(&distinct, &body).ok_or_else(|| {
                let shown = quote_catalog_body(&body, session.auth(), creds);
                UdfError::User(format!(
                    "the Lakekeeper batch-check at {url} answered HTTP {status} with a body that \
                     is not a batch-check answer for its {} checks: {shown}. {NEEDS_LAKEKEEPER}",
                    distinct.len()
                ))
            })
        }
        PostAnswer::Refused { status, body } => Err(refusal(&url, status, &body)),
    }
}

fn distinct_in_first_seen_order<'a>(tables: &[&'a str]) -> Vec<&'a str> {
    let mut seen = HashSet::new();
    tables
        .iter()
        .copied()
        .filter(|table| seen.insert(*table))
        .collect()
}

fn refusal(url: &str, status: u16, redacted_body: &str) -> UdfError {
    let advice = if status == reqwest::StatusCode::FORBIDDEN.as_u16()
        && names_cannot_inspect(redacted_body)
    {
        "The CONNECTION's identity needs a Lakekeeper grant that includes \
         can_read_assignments on the checked tables, such as manage_grants on the warehouse or \
         the namespace."
    } else {
        NEEDS_LAKEKEEPER
    };
    UdfError::User(format!(
        "the Lakekeeper batch-check at {url} answered HTTP {status}: {redacted_body}. {advice}"
    ))
}

fn names_cannot_inspect(body: &str) -> bool {
    serde_json::from_str::<serde_json::Value>(body)
        .ok()
        .and_then(|json| json["error"]["type"].as_str().map(str::to_owned))
        .is_some_and(|error_type| error_type == CANNOT_INSPECT_ERROR_TYPE)
}

fn check_id(index: usize) -> String {
    if index == 0 {
        READ_CHECK_ID.to_string()
    } else {
        format!("{READ_CHECK_ID}-{index}")
    }
}

#[derive(Serialize)]
struct BatchCheckRequest<'a> {
    checks: Vec<TableCheck<'a>>,
    #[serde(rename = "error-on-not-found")]
    error_on_not_found: bool,
}

#[derive(Serialize)]
struct TableCheck<'a> {
    id: String,
    identity: Identity<'a>,
    operation: Operation<'a>,
}

#[derive(Serialize)]
struct Identity<'a> {
    user: &'a str,
}

#[derive(Serialize)]
struct Operation<'a> {
    table: TableOperation<'a>,
}

#[derive(Serialize)]
struct TableOperation<'a> {
    #[serde(rename = "warehouse-id")]
    warehouse_id: &'a str,
    namespace: Vec<String>,
    table: String,
    action: Action,
}

#[derive(Serialize)]
struct Action {
    action: &'static str,
}

#[derive(Deserialize)]
struct BatchCheckAnswer {
    results: Vec<CheckResult>,
}

#[derive(Deserialize)]
struct CheckResult {
    id: String,
    allowed: bool,
}

fn build_batch_check<'a>(
    principal: &'a str,
    tables: &[&'a str],
    warehouse_id: &'a str,
) -> Result<BatchCheckRequest<'a>, UdfError> {
    let checks = tables
        .iter()
        .enumerate()
        .map(|(index, qualified)| {
            let (namespace, table) = parse_table_ident(qualified)?;
            Ok(TableCheck {
                id: check_id(index),
                identity: Identity { user: principal },
                operation: Operation {
                    table: TableOperation {
                        warehouse_id,
                        namespace: namespace.inner(),
                        table,
                        action: Action {
                            action: "read_data",
                        },
                    },
                },
            })
        })
        .collect::<Result<Vec<_>, UdfError>>()?;
    Ok(BatchCheckRequest {
        checks,
        error_on_not_found: false,
    })
}

/// A body is a batch-check answer only when its `results` hold exactly one boolean `allowed`
/// for each check id. Anything else is `None`, never a partial answer.
fn read_decisions(distinct: &[&str], body: &str) -> Option<Vec<TableReadDecision>> {
    let answer: BatchCheckAnswer = serde_json::from_str(body).ok()?;
    let mut allowed_by_id = HashMap::with_capacity(answer.results.len());
    for result in answer.results {
        if allowed_by_id.insert(result.id, result.allowed).is_some() {
            return None;
        }
    }
    if allowed_by_id.len() != distinct.len() {
        return None;
    }
    distinct
        .iter()
        .enumerate()
        .map(|(index, table)| {
            Some(TableReadDecision {
                table: (*table).to_string(),
                allowed: *allowed_by_id.get(&check_id(index))?,
            })
        })
        .collect()
}

#[cfg(test)]
#[path = "lakekeeper_tests.rs"]
mod tests;
