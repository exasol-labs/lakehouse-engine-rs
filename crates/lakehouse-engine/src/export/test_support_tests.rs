//! Inputs the export's unit tests share: the CONNECTION and its sentinel secrets, an
//! `ExportConnection` over static keys, the JSON object merge, and partition specs.

use super::target::ExportConnection;
use exasol_udf_sdk::connection::ConnectionObject;
use iceberg::spec::{
    NestedField, PartitionSpec, PrimitiveType, Schema, SchemaRef, Transform, Type,
};
use lakehouse_catalog::ConnectionCreds;
use std::sync::Arc;

pub(in crate::export) const CONNECTION: &str = "LAKEHOUSE_CATALOG_CREDS";

/// The address, user, and password values that no generated SQL or error may carry.
pub(in crate::export) const SENTINELS: [&str; 3] = [
    "https://sentinel-address.example",
    "sentinel-user",
    "sentinel-password",
];

/// A CONNECTION object of kind `password`; `password` holds the credentials JSON.
pub(in crate::export) fn connection_object(
    address: &str,
    user: &str,
    password: &str,
) -> ConnectionObject {
    ConnectionObject {
        kind: "password".to_string(),
        address: address.to_string(),
        user: user.to_string(),
        password: password.to_string(),
    }
}

/// `base` with the fields of `extra` added.
pub(in crate::export) fn merged(
    mut base: serde_json::Value,
    extra: serde_json::Value,
) -> serde_json::Value {
    let fields = base.as_object_mut().expect("an object");
    fields.extend(extra.as_object().cloned().unwrap_or_default());
    base
}

/// A static-key CONNECTION whose catalog and storage are both at `address`.
pub(in crate::export) fn static_key_connection(
    address: &str,
    allow_http: bool,
) -> ExportConnection {
    ExportConnection {
        address: address.to_string(),
        creds: ConnectionCreds {
            warehouse: "wh".into(),
            endpoint: address.to_string(),
            region: "us-east-1".into(),
            access_key: "STATIC_ACCESS_KEY".into(),
            secret_key: "STATIC_SECRET_KEY".into(),
            path_style: Some(true),
            ..Default::default()
        },
        allow_http,
    }
}

/// A schema of the `date` columns `c0` to `c<count - 1>`.
pub(in crate::export) fn date_columns(count: usize) -> SchemaRef {
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
pub(in crate::export) fn partition_spec(spec_id: i32, transforms: &[Transform]) -> PartitionSpec {
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
