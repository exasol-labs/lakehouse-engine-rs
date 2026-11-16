//! Inputs the export's unit tests share: the CONNECTION name and its sentinel secrets, a
//! CONNECTION object, an `ExportConnection` over static keys, and the JSON object merge.

use super::target::ExportConnection;
use exasol_udf_sdk::connection::ConnectionObject;
use lakehouse_catalog::ConnectionCreds;

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
