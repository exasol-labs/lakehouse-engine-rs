use crate::ConnectionCreds;
use crate::session::CatalogSession;
use exasol_udf_sdk::error::UdfError;
use futures::stream::{self, StreamExt, TryStreamExt};
use iceberg::{NamespaceIdent, TableIdent};
use std::future::Future;
use std::pin::Pin;

/// Sibling namespaces enumerated at once.
const NAMESPACE_CONCURRENCY: usize = 8;

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

/// Includes descendant namespaces; a flat catalog (AWS Glue) is never asked for
/// children.
pub(crate) async fn list_namespace_tables(
    session: &CatalogSession,
    configured_ns: &[String],
    creds: &ConnectionCreds,
) -> Result<Vec<TableIdent>, UdfError> {
    let ns_ident = NamespaceIdent::from_vec(configured_ns.to_vec()).map_err(|e| {
        UdfError::User(format!(
            "invalid namespace '{}': {}",
            configured_ns.join("."),
            e
        ))
    })?;
    list_in_namespace(session, ns_ident, creds).await
}

fn list_in_namespace<'a>(
    session: &'a CatalogSession,
    ns: NamespaceIdent,
    creds: &'a ConnectionCreds,
) -> Pin<Box<dyn Future<Output = Result<Vec<TableIdent>, UdfError>> + Send + 'a>> {
    Box::pin(async move {
        let (mut all, children) = if session.is_flat() {
            (session.list_tables(&ns, creds).await?, Vec::new())
        } else {
            futures::try_join!(
                session.list_tables(&ns, creds),
                session.list_namespaces(&ns, creds)
            )?
        };
        let nested: Vec<Vec<TableIdent>> = stream::iter(children)
            .map(|child| list_in_namespace(session, child, creds))
            .buffered(NAMESPACE_CONCURRENCY)
            .try_collect()
            .await?;
        all.extend(nested.into_iter().flatten());
        Ok(all)
    })
}

#[cfg(test)]
#[path = "namespace_tests.rs"]
mod tests;
