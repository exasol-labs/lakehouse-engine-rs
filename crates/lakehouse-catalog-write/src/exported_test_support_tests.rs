//! The fake Iceberg REST catalog and the recorded answers that the write crate's tests share
//! with its dependents' tests.

use iceberg::spec::{
    NestedField, PartitionSpec, PrimitiveType, Schema, SchemaRef, Transform, Type,
};
use lakehouse_catalog::exported_test_support::spawn_server;
use std::sync::{Arc, Mutex};

/// A freshly created table of the vended warehouse, loaded with `snapshots=all`.
pub const LOAD_TABLE_NO_SNAPSHOTS: &str = include_str!("testdata/load_table_no_snapshots.json");

/// The `/v1/config` answer that gives the catalog the prefix `p`.
pub const PREFIX_CONFIG: &str = r#"{"defaults":{"prefix":"p"}}"#;

/// A fake catalog: `/v1/config` answers `config`, and every other request the answer `route`
/// gives it, by request line.
pub async fn catalog(
    config: &'static str,
    route: impl Fn(&str) -> (u16, String) + Send + 'static,
) -> (String, Arc<Mutex<Vec<String>>>) {
    spawn_server("application/json", move |request| {
        if request.starts_with("GET /v1/config") {
            (200, config.to_string())
        } else {
            route(request.lines().next().unwrap_or_default())
        }
    })
    .await
}

/// `table`, a load-table answer, with commit retry waits of 1 ms, so a test that exhausts a
/// commit's retries runs on the real clock.
pub fn with_short_commit_waits(table: &str) -> String {
    let mut table: serde_json::Value = serde_json::from_str(table).expect("a load-table answer");
    let properties = &mut table["metadata"]["properties"];
    properties["commit.retry.min-wait-ms"] = "1".into();
    properties["commit.retry.max-wait-ms"] = "1".into();
    table.to_string()
}

/// A schema of the `date` columns `c0` to `c<count - 1>`.
pub fn date_columns(count: usize) -> SchemaRef {
    let fields = (0..count).map(|index| {
        let id = i32::try_from(index).expect("a few columns") + 1;
        NestedField::optional(
            id,
            format!("c{index}"),
            Type::Primitive(PrimitiveType::Date),
        )
        .into()
    });
    Arc::new(
        Schema::builder()
            .with_fields(fields)
            .build()
            .expect("a schema"),
    )
}

/// The partition spec `spec_id` over `date_columns(transforms.len())` whose field `p<i>` applies
/// `transforms[i]` to `c<i>`.
pub fn partition_spec(spec_id: i32, transforms: &[Transform]) -> PartitionSpec {
    let builder = PartitionSpec::builder(date_columns(transforms.len())).with_spec_id(spec_id);
    transforms
        .iter()
        .enumerate()
        .fold(builder, |builder, (index, transform)| {
            builder
                .add_partition_field(format!("c{index}"), format!("p{index}"), *transform)
                .expect("a partition field")
        })
        .build()
        .expect("a partition spec")
}
