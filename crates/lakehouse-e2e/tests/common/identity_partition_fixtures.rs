//! Ground truth for the Spark identity-partition fixtures (#99): Iceberg tables whose data files
//! a metadata-only Hive import (`add_files`) registered without the identity-partition source
//! column. Authored at stack bring-up by the `spark-iceberg-fixtures` job, because only Spark's
//! `add_files` registers a file that lacks the column of its own `identity` partition field. Must
//! stay in lockstep with `scripts/spark-fixtures/create_identity_partition_fixture.sql`.

pub const NAMESPACE: &str = "e2e_lakehouse";

pub const ABSENT_TABLE: &str = "ident_part_absent";

/// Every column but `id` carries an `identity` partition field. Row 1 is imported, row 2 is
/// native with the same values, row 3 is imported with every partition value null.
pub const TYPES_TABLE: &str = "ident_part_types";

pub const REGION_COLUMN: &str = "region";

/// `add_files` records the import's own path, and every import directory but the held one ends
/// in this, so it tells a file without its identity columns from one that holds them.
pub const IMPORT_DIR_SUFFIX: &str = "_source/";

/// The import directory of the one `ABSENT_TABLE` file that holds `region`.
pub const HELD_DIR: &str = "ident_part_absent_held/";

/// `(id, region)` as a reader returns it, which the manifest's partition value decides for an
/// imported file. The tables' `amount` column is not read.
pub type RegionRow = (i64, Option<&'static str>);

/// Ids 1 to 4, 7, 9, and 10 are imported under spec 0 (`identity(region)`), 5 and 6 are native,
/// and 8 is imported under spec 1, where the field is dropped. Ids 7 and 8 read NULL: 7 because
/// its partition value is null, 8 because its spec has no `identity` field on `region`. Ids 9 and
/// 10 come from the held file, which stores [`HELD_STORED_REGIONS`] but reads its `eu`.
pub const ABSENT_ROWS: [RegionRow; 10] = [
    (1, Some("eu")),
    (2, Some("eu")),
    (3, Some("us")),
    (4, Some("us")),
    (5, Some("apac")),
    (6, Some("eu")),
    (7, None),
    (8, None),
    (9, Some("eu")),
    (10, Some("eu")),
];

/// What the held file physically stores for `region`, by row id.
pub const HELD_STORED_REGIONS: [(i32, Option<&str>); 2] = [(9, Some("us")), (10, None)];

pub struct TypesColumn {
    pub name: &'static str,
    /// The declared Exasol type, as `declared_types` reads it.
    pub declared: &'static str,
    /// What a reader returns for the imported and the native row, as text.
    pub read_value: &'static str,
}

pub const TYPES_ID_COLUMN: &str = "id";
pub const TYPES_ID_DECLARED: &str = "DECIMAL(10,0)";

pub const TYPES_ROW_IDS_TEXT: [&str; 3] = ["1", "2", "3"];

pub const TYPES_COLUMNS: [TypesColumn; 9] = [
    TypesColumn {
        name: "b",
        declared: "BOOLEAN",
        read_value: "true",
    },
    TypesColumn {
        name: "i",
        declared: "DECIMAL(10,0)",
        read_value: "-2147483648",
    },
    // Not exactly representable as a double, so a lossy text round trip would show.
    TypesColumn {
        name: "l",
        declared: "DECIMAL(20,0)",
        read_value: "9007199254740993",
    },
    // 0.1f32 widened to its exact double expansion, not the double nearest 0.1.
    TypesColumn {
        name: "f",
        declared: "DOUBLE",
        read_value: "0.10000000149011612",
    },
    TypesColumn {
        name: "d",
        declared: "DOUBLE",
        read_value: "1234.5678",
    },
    TypesColumn {
        name: "dec_small",
        declared: "DECIMAL(10,2)",
        read_value: "12345678.9",
    },
    // Outside Exasol's catalog-decimal domain, so it reads as `VARCHAR` text.
    TypesColumn {
        name: "dec_large",
        declared: "VARCHAR(2000000)",
        read_value: "1234567890123456789012345678.123456",
    },
    TypesColumn {
        name: "dt",
        declared: "DATE",
        read_value: "2024-02-29",
    },
    TypesColumn {
        name: "s",
        declared: "VARCHAR(2000000)",
        read_value: "north",
    },
];

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
