//! Ground truth for the Spark export-append fixture. Must stay in lockstep with
//! `scripts/spark-fixtures/create_export_append_fixture.sql`.

pub const NAMESPACE: &str = "e2e_export_append";

pub const APPENDABLE_V2: &str = "appendable_v2";
pub const APPENDABLE_V1: &str = "appendable_v1";
pub const ODD_TYPES: &str = "odd_types";
pub const PARTITIONED: &str = "partitioned";
pub const V3: &str = "v3";
pub const V1_VOID_COLUMN: &str = "v1_void_column";

/// The name, Iceberg type, and required flag of each column of both appendable tables.
pub const APPENDABLE_COLUMNS: [(&str, &str, bool); 9] = [
    ("id", "long", true),
    ("qty", "int", false),
    ("amount", "decimal(20, 2)", false),
    ("name", "string", true),
    ("day", "date", false),
    ("ts", "timestamp", false),
    ("created", "timestamptz", false),
    ("flag", "boolean", false),
    ("score", "double", false),
];

/// The transforms of the current partition spec of `appendable_v2`, whose `day` field was dropped.
pub const APPENDABLE_V2_SPEC: &[&str] = &[];
/// Dropping `day` added spec 1.
pub const APPENDABLE_V2_SPEC_ID: i64 = 1;
/// Format version 1 replaces a dropped partition field's transform with `void`.
pub const APPENDABLE_V1_SPEC: &[&str] = &["void"];
/// Adding `day_part` added spec 1, dropping it spec 2.
pub const APPENDABLE_V1_SPEC_ID: i64 = 2;

/// The rows of both appendable tables in `id` order, as `APPENDABLE_TEXT` renders them, NULL as
/// `None`. `ts` and `created` are exact to the millisecond.
pub const APPENDABLE_ROWS: [[Option<&str>; 9]; 3] = [
    [
        Some("1"),
        Some("7"),
        Some("12.5"),
        Some("alpha"),
        Some("2024-01-15"),
        Some("2024-01-15 10:30:00.123"),
        Some("2024-01-15 10:30:00.123"),
        Some("TRUE"),
        Some("1.5"),
    ],
    [
        Some("2"),
        Some("-3"),
        Some("-99999.99"),
        Some("beta"),
        Some("2024-02-20"),
        Some("2024-02-20 23:59:59.999"),
        Some("2024-02-20 00:00:00.001"),
        Some("FALSE"),
        Some("-0.25"),
    ],
    [
        Some("3"),
        None,
        None,
        Some("gamma"),
        None,
        None,
        None,
        None,
        None,
    ],
];

/// The select list rendering an appendable table as `APPENDABLE_ROWS` on every Exasol version:
/// timestamps at milliseconds, `created` as its UTC date and time.
pub const APPENDABLE_TEXT: &str = "CAST(ID AS VARCHAR(50)), CAST(QTY AS VARCHAR(50)), \
     CAST(AMOUNT AS VARCHAR(50)), NAME, TO_CHAR(\"DAY\", 'YYYY-MM-DD'), \
     TO_CHAR(TS, 'YYYY-MM-DD HH24:MI:SS.FF3'), TO_CHAR(CREATED, 'YYYY-MM-DD HH24:MI:SS.FF3'), \
     CAST(FLAG AS VARCHAR(5)), CAST(SCORE AS VARCHAR(50))";

/// `odd_types`: no Exasol type fills its `float` column.
pub const ODD_TYPES_COLUMNS: [(&str, &str, bool); 3] = [
    ("id", "long", true),
    ("ratio", "float", false),
    ("note", "string", false),
];

/// `partitioned`: identity on `day`.
pub const PARTITIONED_COLUMNS: [(&str, &str, bool); 2] =
    [("id", "long", false), ("day", "date", false)];
pub const PARTITIONED_SPEC: &[&str] = &["identity"];

/// `v3`: format version 3.
pub const V3_COLUMNS: [(&str, &str, bool); 1] = [("id", "long", false)];

/// `v1_void_column`: format version 1, whose dropped `day` field stays `void` under the name `day`.
pub const V1_VOID_COLUMN_COLUMNS: [(&str, &str, bool); 2] =
    [("id", "long", false), ("day", "date", false)];
pub const V1_VOID_COLUMN_SPEC: &[&str] = &["void"];
