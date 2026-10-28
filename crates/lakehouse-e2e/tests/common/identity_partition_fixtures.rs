//! Ground truth for the Spark identity-partition fixtures (#99): Iceberg tables whose data files
//! a metadata-only Hive import (`add_files`) registered without the identity-partition source
//! column. Authored at stack bring-up by the `spark-iceberg-fixtures` job, because only Spark's
//! `add_files` registers a file that lacks the column of its own `identity` partition field. Must
//! stay in lockstep with `scripts/spark-fixtures/create_identity_partition_fixture.sql`.

pub const NAMESPACE: &str = "e2e_lakehouse";

pub const ABSENT_TABLE: &str = "ident_part_absent";

pub const REQUIRED_TABLE: &str = "ident_part_required";

/// Every column but `id` carries an `identity` partition field. Row 1 is imported, row 2 is
/// native with the same values, row 3 is imported with every partition value null.
pub const TYPES_TABLE: &str = "ident_part_types";

pub const REGION_COLUMN: &str = "region";

/// `add_files` records the import's own path, so these directory names tell an imported data file
/// from one the table wrote natively.
pub const ABSENT_IMPORT_DIR: &str = "ident_part_absent_source";

pub const ABSENT_UNPARTITIONED_IMPORT_DIR: &str = "ident_part_absent_unpartitioned_source";

pub const REQUIRED_IMPORT_DIR: &str = "ident_part_required_source";

pub const TYPES_IMPORT_DIR: &str = "ident_part_types_source";

/// The directory name Hive gives a null partition value.
pub const HIVE_DEFAULT_PARTITION: &str = "__HIVE_DEFAULT_PARTITION__";

pub struct RegionRow {
    pub id: i64,
    pub amount: i64,
    /// What a reader returns, which the manifest's partition value decides for an imported file.
    pub region: Option<&'static str>,
}

/// Ids 1 to 4 and 7 are imported under spec 0 (`identity(region)`), 5 and 6 are native, and 8 is
/// imported under spec 1, where the field is dropped. Ids 7 and 8 read NULL: 7 because its
/// partition value is null, 8 because its spec has no `identity` field on `region`.
pub const ABSENT_ROWS: [RegionRow; 8] = [
    RegionRow {
        id: 1,
        amount: 10,
        region: Some("eu"),
    },
    RegionRow {
        id: 2,
        amount: 20,
        region: Some("eu"),
    },
    RegionRow {
        id: 3,
        amount: 30,
        region: Some("us"),
    },
    RegionRow {
        id: 4,
        amount: 40,
        region: Some("us"),
    },
    RegionRow {
        id: 5,
        amount: 50,
        region: Some("apac"),
    },
    RegionRow {
        id: 6,
        amount: 60,
        region: Some("eu"),
    },
    RegionRow {
        id: 7,
        amount: 70,
        region: None,
    },
    RegionRow {
        id: 8,
        amount: 80,
        region: None,
    },
];

/// The table's two partition spec ids: `identity(region)`, then the empty spec.
pub const ABSENT_SPEC_ID_COUNT: usize = 2;

/// `region` is REQUIRED and every data file is imported.
pub const REQUIRED_ROWS: [RegionRow; 3] = [
    RegionRow {
        id: 1,
        amount: 10,
        region: Some("eu"),
    },
    RegionRow {
        id: 2,
        amount: 20,
        region: Some("us"),
    },
    RegionRow {
        id: 3,
        amount: 30,
        region: Some("apac"),
    },
];

pub struct TypesColumn {
    pub name: &'static str,
    /// The declared Exasol type, as `declared_types` reads it.
    pub declared: &'static str,
    /// What a reader returns for the imported and the native row, as text.
    pub read_value: &'static str,
    /// The manifest partition literal of the imported row, as the guard renders it.
    pub manifest_value: &'static str,
}

pub const TYPES_ID_COLUMN: &str = "id";
pub const TYPES_ID_DECLARED: &str = "DECIMAL(10,0)";

pub const TYPES_ROW_IDS_TEXT: [&str; 3] = ["1", "2", "3"];

pub const TYPES_COLUMNS: [TypesColumn; 9] = [
    TypesColumn {
        name: "b",
        declared: "BOOLEAN",
        read_value: "true",
        manifest_value: "true",
    },
    TypesColumn {
        name: "i",
        declared: "DECIMAL(10,0)",
        read_value: "-2147483648",
        manifest_value: "-2147483648",
    },
    // Not exactly representable as a double, so a lossy text round trip would show.
    TypesColumn {
        name: "l",
        declared: "DECIMAL(20,0)",
        read_value: "9007199254740993",
        manifest_value: "9007199254740993",
    },
    // 0.1f32 widened to its exact double expansion, not the double nearest 0.1.
    TypesColumn {
        name: "f",
        declared: "DOUBLE",
        read_value: "0.10000000149011612",
        manifest_value: "0.1",
    },
    TypesColumn {
        name: "d",
        declared: "DOUBLE",
        read_value: "1234.5678",
        manifest_value: "1234.5678",
    },
    // The manifest holds the unscaled value.
    TypesColumn {
        name: "dec_small",
        declared: "DECIMAL(10,2)",
        read_value: "12345678.9",
        manifest_value: "1234567890",
    },
    // Outside Exasol's catalog-decimal domain, so it reads as `VARCHAR` text.
    TypesColumn {
        name: "dec_large",
        declared: "VARCHAR(2000000)",
        read_value: "1234567890123456789012345678.123456",
        manifest_value: "1234567890123456789012345678123456",
    },
    // The manifest holds days since 1970-01-01.
    TypesColumn {
        name: "dt",
        declared: "DATE",
        read_value: "2024-02-29",
        manifest_value: "19782",
    },
    TypesColumn {
        name: "s",
        declared: "VARCHAR(2000000)",
        read_value: "north",
        manifest_value: "north",
    },
];

/// One file imported under `region=eu` that physically holds `region`, without Iceberg field ids,
/// so only `schema.name-mapping.default` binds it.
pub const HELD_TABLE: &str = "ident_part_held";

pub const HELD_IMPORT_DIR: &str = "ident_part_held_source";

/// Every row reads the manifest's `eu`, not the value its file stores.
pub const HELD_ROWS: [RegionRow; 2] = [
    RegionRow {
        id: 1,
        amount: 10,
        region: Some("eu"),
    },
    RegionRow {
        id: 2,
        amount: 20,
        region: Some("eu"),
    },
];

/// What the imported file physically stores for `region`, by row id.
pub const HELD_STORED_REGIONS: [(i32, Option<&str>); 2] = [(1, Some("us")), (2, None)];
