//! The local stack's Iceberg tables for the E2E tests: the warehouse objects under a prefix, and a
//! table of the local REST catalog.

use futures::TryStreamExt;
use iceberg::{Catalog, NamespaceIdent, TableIdent};
use object_store::ObjectStore;
use object_store::path::Path as ObjectStorePath;

use crate::common::e2e_harness::{local_stack_s3_store, runtime};
use crate::common::seed::build_seed_catalog;
use crate::common::stack::iceberg_catalog_url;

/// The SeaweedFS bucket of the local stack's warehouse.
pub const WAREHOUSE_BUCKET: &str = "warehouse";

/// The keys under `prefix` of the warehouse bucket.
pub fn objects_under(prefix: &str) -> Vec<String> {
    let store = local_stack_s3_store(WAREHOUSE_BUCKET);
    let prefix = ObjectStorePath::from(prefix);
    runtime()
        .block_on(
            store
                .list(Some(&prefix))
                .map_ok(|object| object.location.to_string())
                .try_collect(),
        )
        .unwrap_or_else(|e| panic!("list {WAREHOUSE_BUCKET}/{prefix}: {e}"))
}

/// `namespace.table` of the local REST catalog.
pub fn load_table(namespace: &str, table: &str) -> iceberg::table::Table {
    runtime().block_on(async {
        let catalog = build_seed_catalog(&iceberg_catalog_url(), "s3://warehouse/", "load")
            .await
            .unwrap_or_else(|e| panic!("connect to the Iceberg REST catalog for {table}: {e:#}"));
        let ident = TableIdent::new(NamespaceIdent::new(namespace.into()), table.into());
        catalog
            .load_table(&ident)
            .await
            .unwrap_or_else(|e| panic!("load {namespace}.{table}: {e}"))
    })
}
