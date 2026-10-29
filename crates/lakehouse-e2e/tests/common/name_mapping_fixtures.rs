//! Ground truth for the Spark name-mapping fixtures (#83): Iceberg tables whose imported file
//! carries no embedded field ids, so only `schema.name-mapping.default` binds its columns.
//! Authored at stack bring-up by the `spark-iceberg-fixtures` job, because only Spark's
//! `add_files` registers a file without field ids. Must stay in lockstep with
//! `scripts/spark-fixtures/create_name_mapping_fixture.sql`.

pub const NAMESPACE: &str = "e2e_lakehouse";

/// Imported without field ids, then its nested members renamed, so only
/// `schema.name-mapping.default` binds the imported row's members.
pub const NESTED_TABLE: &str = "nested_name_map";

pub const NESTED_IMPORT_DIR: &str = "nested_name_map_source/";

/// `addr.street` carries every name it had; `addr.zip_v1` is missing from the mapping.
pub const NESTED_STREET_NAMES: [&str; 3] = ["street_v1", "street_v2", "street"];
pub const NESTED_UNMAPPED_NAME: &str = "zip_v1";

/// `(id, label, addr, items, attrs)`, the nested columns as the JSON a reader renders. Id 1 is
/// imported: `town` and `city` are absent from its file, and its `zip_v1` is unmapped. Id 2 is
/// native from before the renames: its file's `city` embeds `town`'s field id.
pub const NESTED_ROWS: [(i64, &str, &str, &str, &str); 3] = [
    (
        1,
        "one",
        r#"{"street":"Main St","town":null,"zip":null,"geo":{"lat":52.5},"city":null}"#,
        r#"[{"qty":1},{"qty":3}]"#,
        r#"{"a":{"amt":10}}"#,
    ),
    (
        2,
        "two",
        r#"{"street":"Elm St","town":"Oslo","zip":"0150","geo":{"lat":59.9},"city":null}"#,
        r#"[{"qty":2}]"#,
        r#"{"b":{"amt":20}}"#,
    ),
    (
        3,
        "three",
        r#"{"street":"Oak St","town":"Rome","zip":"00184","geo":{"lat":41.9},"city":"Roma"}"#,
        r#"[{"qty":4}]"#,
        r#"{"c":{"amt":30}}"#,
    ),
];

/// Its hand-set mapping never maps `extra` or `info.memo`, and `note` and `info.tag` were dropped
/// and re-added after id 1 was written.
pub const PROJECTION_TABLE: &str = "projection_by_id";

pub const PROJECTION_IMPORT_DIR: &str = "projection_by_id_source/";

pub const PROJECTION_UNMAPPED: [&str; 2] = ["extra", "info.memo"];

/// `(id, note, info, extra)`, `info` as the JSON a reader renders. Id 1 is native and holds the
/// dropped `note` and `info.tag`. Id 2 is imported: the mapping binds its `note` and `tag` to the
/// re-added fields, and nothing binds its `extra` and `memo`.
pub const PROJECTION_ROWS: [(i64, Option<&str>, &str, Option<&str>); 3] = [
    (
        1,
        None,
        r#"{"code":"c1","memo":"m1","tag":null}"#,
        Some("e1"),
    ),
    (
        2,
        Some("imported note"),
        r#"{"code":"c2","memo":null,"tag":"t2"}"#,
        None,
    ),
    (
        3,
        Some("n3"),
        r#"{"code":"c3","memo":"m3","tag":"t3"}"#,
        Some("e3"),
    ),
];
