use crate::error_chain;
use crate::redaction::redact_credentials;
use crate::session::{build_rest_catalog, glue_catalog_prefix};
use crate::sigv4::required_signing_region;
use crate::{CatalogProps, ConnectionCreds, StorageBackend};
use exasol_udf_sdk::error::UdfError;
use iceberg::{Catalog, NamespaceIdent, TableIdent};
use iceberg_catalog_rest::ListTablesResponse;

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
/// SigV4 targets Glue only: requests sign for the `glue` service under `catalogs/{warehouse}`.
pub(crate) async fn list_namespace_tables(
    catalog_uri: &str,
    configured_ns: &[String],
    storage: &StorageBackend,
    creds: &ConnectionCreds,
) -> Result<Vec<TableIdent>, UdfError> {
    let ns_ident = NamespaceIdent::from_vec(configured_ns.to_vec()).map_err(|e| {
        UdfError::User(format!(
            "invalid namespace '{}': {}",
            configured_ns.join("."),
            e
        ))
    })?;

    if creds.use_sigv4 {
        let region = required_signing_region(creds, catalog_uri)?;
        let prefix = glue_catalog_prefix(&creds.warehouse);
        let enumeration = GlueEnumeration {
            client: reqwest::Client::new(),
            catalog_uri,
            prefix: &prefix,
            creds,
            region: &region,
        };
        enumeration.list_glue_tables(&ns_ident).await
    } else {
        list_namespace_tables_unsigned(catalog_uri, &ns_ident, &creds.warehouse, storage, creds)
            .await
    }
}

async fn list_namespace_tables_unsigned(
    catalog_uri: &str,
    parent: &NamespaceIdent,
    warehouse: &str,
    storage: &StorageBackend,
    creds: &ConnectionCreds,
) -> Result<Vec<TableIdent>, UdfError> {
    let dummy_catalog = CatalogProps {
        warehouse: warehouse.to_string(),
        table: String::new(),
    };
    let catalog = build_rest_catalog(catalog_uri, &dummy_catalog, storage, creds).await?;
    list_in_namespace_unsigned(&catalog, parent).await
}

fn list_in_namespace_unsigned<'a>(
    catalog: &'a iceberg_catalog_rest::RestCatalog,
    ns: &'a NamespaceIdent,
) -> std::pin::Pin<
    Box<dyn std::future::Future<Output = Result<Vec<TableIdent>, UdfError>> + Send + 'a>,
> {
    Box::pin(async move {
        let mut all: Vec<TableIdent> = Vec::new();

        let tables = catalog.list_tables(ns).await.map_err(|e: iceberg::Error| {
            UdfError::User(format!(
                "failed to list tables in namespace '{}': {}",
                ns.join("."),
                redact_credentials(&error_chain(&e))
            ))
        })?;
        all.extend(tables);

        let children = catalog
            .list_namespaces(Some(ns))
            .await
            .map_err(|e: iceberg::Error| {
                UdfError::User(format!(
                    "failed to list namespaces under '{}': {}",
                    ns.join("."),
                    redact_credentials(&error_chain(&e))
                ))
            })?;

        for child in children {
            let child_tables = list_in_namespace_unsigned(catalog, &child).await?;
            all.extend(child_tables);
        }

        Ok(all)
    })
}

fn build_list_tables_url(catalog_uri: &str, warehouse: &str, ns: &NamespaceIdent) -> String {
    let ns_url = ns.to_url_string();
    if warehouse.is_empty() {
        format!("{catalog_uri}/v1/namespaces/{ns_url}/tables")
    } else {
        format!("{catalog_uri}/v1/{warehouse}/namespaces/{ns_url}/tables")
    }
}

struct GlueEnumeration<'a> {
    client: reqwest::Client,
    catalog_uri: &'a str,
    prefix: &'a str,
    creds: &'a ConnectionCreds,
    region: &'a str,
}

impl<'a> GlueEnumeration<'a> {
    async fn signed_get_json(
        &self,
        url: &str,
        page_token: Option<&str>,
    ) -> Result<serde_json::Value, UdfError> {
        let mut request = self.client.get(url).header("accept", "application/json");
        if let Some(token) = page_token {
            request = request.query(&[("pageToken", token)]);
        }
        let request = request
            .build()
            .map_err(|e| UdfError::User(format!("failed to build catalog request: {e}")))?;

        let signed = crate::sigv4::sign_request(
            request,
            &self.creds.access_key,
            &self.creds.secret_key,
            self.creds.session_token.as_deref(),
            self.region,
            "glue",
        )
        .map_err(|e| {
            UdfError::User(format!(
                "failed to sign catalog request: {}",
                redact_credentials(&e.to_string())
            ))
        })?;

        let response = self.client.execute(signed).await.map_err(|e| {
            UdfError::User(format!(
                "catalog request failed: {}",
                redact_credentials(&error_chain(&e))
            ))
        })?;

        let status = response.status();
        if !status.is_success() {
            let body = response
                .text()
                .await
                .unwrap_or_else(|_| "(unreadable body)".into());
            return Err(UdfError::User(format!(
                "catalog returned HTTP {}: {}",
                status.as_u16(),
                redact_credentials(&body)
            )));
        }

        response.json::<serde_json::Value>().await.map_err(|e| {
            UdfError::User(format!(
                "failed to parse catalog response: {}",
                redact_credentials(&e.to_string())
            ))
        })
    }

    async fn list_glue_tables(&self, ns: &NamespaceIdent) -> Result<Vec<TableIdent>, UdfError> {
        let url = build_list_tables_url(self.catalog_uri, self.prefix, ns);
        let mut all = Vec::new();
        let mut page_token: Option<String> = None;
        loop {
            let page = self
                .signed_get_json(&url, page_token.as_deref())
                .await
                .map_err(|e| {
                    UdfError::User(format!(
                        "failed to list tables in namespace '{}': {}",
                        ns.join("."),
                        redact_credentials(&e.to_string())
                    ))
                })?;
            let page: ListTablesResponse = serde_json::from_value(page).map_err(|e| {
                UdfError::User(format!(
                    "failed to parse list-tables response for namespace '{}': {}",
                    ns.join("."),
                    redact_credentials(&e.to_string())
                ))
            })?;
            all.extend(page.identifiers);
            match page.next_page_token {
                Some(token) if !token.is_empty() => page_token = Some(token),
                _ => return Ok(all),
            }
        }
    }
}

#[cfg(test)]
#[path = "namespace_tests.rs"]
mod tests;
