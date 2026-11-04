use std::collections::HashSet;

use exasol_udf_sdk::error::UdfError;
use serde_json::Value as Json;

use super::ResolvedScan;
use super::binary_filter::with_binary_literals;
use super::support::{ColumnTypes, InvolvedColumns};
use crate::adapter::permission::PermissionCheck;
use crate::adapter::properties::resolve_binary_as_string;
use crate::adapter::statement_state::StatementCatalog;

#[cfg(test)]
#[path = "scan_resolution_tests.rs"]
mod tests;

/// One request's tables, resolved through its statement's catalog, so every request of the
/// statement shares one catalog session and every reference to a table shares one load.
///
/// Every shape loads tables through [`Self::resolve`], so the permission check runs once here and
/// a shape added later cannot skip it.
pub(super) struct TableScanResolver<'a> {
    statement: &'a StatementCatalog,
    permission: &'a PermissionCheck,
    binary_as_string: bool,
}

impl<'a> TableScanResolver<'a> {
    /// Every `table_identifiers` entry passes its kind's identifier rule before any catalog
    /// request, so a malformed identifier surfaces its own error instead of a transport error.
    ///
    /// With an enforced `permission`, one batch-check covers the identifiers that no earlier
    /// request of the statement checked, before any of them is loaded.
    pub(super) async fn for_request(
        statement: &'a StatementCatalog,
        permission: &'a PermissionCheck,
        table_identifiers: &[&str],
        props: &Json,
    ) -> Result<Self, UdfError> {
        let client = statement.client();
        for identifier in table_identifiers {
            client.check_identifier(identifier)?;
        }
        if let PermissionCheck::Enforced(gate) = permission {
            let unchecked = unchecked_identifiers(table_identifiers, &statement.admitted());
            if !unchecked.is_empty() {
                let creds = &statement.connection().creds;
                statement.admit(client.authorize(gate, &unchecked, creds).await?);
            }
        }
        Ok(Self {
            statement,
            permission,
            binary_as_string: resolve_binary_as_string(props),
        })
    }

    /// `table_identifier` is the original-cased identifier from the involved table's identifier
    /// note. `involved` is the table's columns as the request declares them; their raw column
    /// notes are parsed here once for every format before any table is loaded.
    ///
    /// Returns the table's per-column type map beside the scan, built here because only this
    /// point has both the request types and the binary layouts.
    pub(super) async fn resolve(
        &self,
        table_identifier: &str,
        filter_json: Option<&Json>,
        involved: &InvolvedColumns<'_>,
    ) -> Result<(ResolvedScan, ColumnTypes), UdfError> {
        self.require_admitted(table_identifier)?;
        let declared = involved.notes.declared_schema()?;
        let column_types =
            ColumnTypes::of(involved, &declared.logical_schema, self.binary_as_string);
        // Retyped so every reader prunes binary comparisons on the raw bytes.
        let filter_json =
            filter_json.map(|filter| with_binary_literals(filter.clone(), &column_types));
        let loaded = self.statement.loaded_table(table_identifier).await?;
        let scan = ResolvedScan::resolve(loaded.as_ref(), declared, filter_json.as_ref()).await?;
        Ok((scan, column_types))
    }

    fn require_admitted(&self, table_identifier: &str) -> Result<(), UdfError> {
        match self.permission {
            PermissionCheck::Off => Ok(()),
            PermissionCheck::Enforced(_)
                if self.statement.admitted().contains(table_identifier) =>
            {
                Ok(())
            }
            PermissionCheck::Enforced(_) => Err(UdfError::User(format!(
                "pushdown: the Lakekeeper permission check did not cover table \
                 '{table_identifier}', so the adapter does not read it"
            ))),
        }
    }
}

/// The identifiers that no permission check of the statement admitted yet, in request order.
fn unchecked_identifiers<'i>(identifiers: &[&'i str], admitted: &HashSet<String>) -> Vec<&'i str> {
    identifiers
        .iter()
        .copied()
        .filter(|identifier| !admitted.contains(*identifier))
        .collect()
}
