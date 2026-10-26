use crate::ConnectionCreds;
use crate::auth::CatalogAuth;
use crate::iceberg_io::authed_get_json;
use crate::redaction::redact_credentials;
use crate::session::CatalogSession;
use exasol_udf_sdk::error::UdfError;
use futures::{StreamExt, TryStreamExt};
use iceberg::{NamespaceIdent, TableIdent};
use iceberg_catalog_rest::{ListNamespaceResponse, ListTablesResponse};
use serde::de::DeserializeOwned;

/// Bounds the catalog requests one enumeration keeps in flight.
pub(crate) const CATALOG_REQUEST_CONCURRENCY: usize = 16;

/// The last `.`-delimited segment is the table name, all preceding ones the
/// namespace. Errors when the input contains no `.`.
pub fn parse_table_ident(qualified: &str) -> Result<(NamespaceIdent, String), UdfError> {
    let mut parts: Vec<&str> = qualified.split('.').collect();
    if parts.len() < 2 {
        return Err(UdfError::User(format!(
            "table property must be 'namespace.table', got: '{qualified}'"
        )));
    }
    let table_name = parts.pop().unwrap().to_string();
    let ns_ident = NamespaceIdent::from_vec(parts.iter().map(|s| s.to_string()).collect())
        .map_err(|e| UdfError::User(format!("invalid namespace in '{qualified}': {e}")))?;
    Ok((ns_ident, table_name))
}

/// Includes descendant namespaces, except on Glue, whose namespaces are single-level.
pub(crate) async fn list_namespace_tables(
    session: &CatalogSession,
    configured_ns: &[String],
    creds: &ConnectionCreds,
) -> Result<Vec<TableIdent>, UdfError> {
    let root = NamespaceIdent::from_vec(configured_ns.to_vec()).map_err(|e| {
        UdfError::User(format!(
            "invalid namespace '{}': {}",
            configured_ns.join("."),
            e
        ))
    })?;
    let descend = !matches!(session.auth(), CatalogAuth::Sigv4 { .. });

    let mut tables = Vec::new();
    let mut level = vec![root];
    while !level.is_empty() {
        let listed: Vec<(Vec<TableIdent>, Vec<NamespaceIdent>)> = futures::stream::iter(level)
            .map(|ns| async move {
                if descend {
                    futures::try_join!(
                        list_tables(session, &ns, creds),
                        list_child_namespaces(session, &ns, creds)
                    )
                } else {
                    Ok((list_tables(session, &ns, creds).await?, Vec::new()))
                }
            })
            .buffered(CATALOG_REQUEST_CONCURRENCY)
            .try_collect()
            .await?;
        level = Vec::new();
        for (found, children) in listed {
            tables.extend(found);
            level.extend(children);
        }
    }
    Ok(tables)
}

async fn list_tables(
    session: &CatalogSession,
    ns: &NamespaceIdent,
    creds: &ConnectionCreds,
) -> Result<Vec<TableIdent>, UdfError> {
    let url = namespaces_url(session, Some(&format!("{}/tables", ns.to_url_string())));
    list_pages(session, &url, &[], creds, |page: ListTablesResponse| {
        (page.identifiers, page.next_page_token)
    })
    .await
    .map_err(|e| listing_error("list tables in namespace", ns, e))
}

/// Only strict descendants are kept, so a catalog that ignores `parent` cannot loop the descent.
async fn list_child_namespaces(
    session: &CatalogSession,
    ns: &NamespaceIdent,
    creds: &ConnectionCreds,
) -> Result<Vec<NamespaceIdent>, UdfError> {
    let url = namespaces_url(session, None);
    let parent = ns.to_url_string();
    let children = list_pages(
        session,
        &url,
        &[("parent", parent.as_str())],
        creds,
        |page: ListNamespaceResponse| (page.namespaces, page.next_page_token),
    )
    .await
    .map_err(|e| listing_error("list namespaces under", ns, e))?;
    Ok(children
        .into_iter()
        .filter(|child| child.len() > ns.len() && child.starts_with(ns))
        .collect())
}

async fn list_pages<P: DeserializeOwned, T>(
    session: &CatalogSession,
    url: &str,
    query: &[(&str, &str)],
    creds: &ConnectionCreds,
    split: impl Fn(P) -> (Vec<T>, Option<String>),
) -> Result<Vec<T>, UdfError> {
    let mut first_page = url::Url::parse(url)
        .map_err(|e| UdfError::User(format!("invalid catalog URL '{url}': {e}")))?;
    if !query.is_empty() {
        first_page.query_pairs_mut().extend_pairs(query);
    }
    let mut all = Vec::new();
    let mut page_token: Option<String> = None;
    loop {
        let mut page_url = first_page.clone();
        if let Some(token) = &page_token {
            page_url.query_pairs_mut().append_pair("pageToken", token);
        }
        let page: P = authed_get_json(
            session.client(),
            page_url.as_str(),
            session.auth(),
            false,
            creds,
        )
        .await?;
        let (items, next) = split(page);
        all.extend(items);
        // Glue can answer an empty page that still carries a token, so only the token ends paging.
        match next {
            Some(token) if !token.is_empty() => page_token = Some(token),
            _ => return Ok(all),
        }
    }
}

fn namespaces_url(session: &CatalogSession, below: Option<&str>) -> String {
    let base = session.catalog_uri().trim_end_matches('/');
    let prefix = match session.prefix() {
        "" => String::new(),
        prefix => format!("/{prefix}"),
    };
    match below {
        Some(below) => format!("{base}/v1{prefix}/namespaces/{below}"),
        None => format!("{base}/v1{prefix}/namespaces"),
    }
}

fn listing_error(action: &str, ns: &NamespaceIdent, error: UdfError) -> UdfError {
    UdfError::User(format!(
        "failed to {action} '{}': {}",
        ns.join("."),
        redact_credentials(&error.to_string())
    ))
}

#[cfg(test)]
#[path = "namespace_tests.rs"]
mod tests;
