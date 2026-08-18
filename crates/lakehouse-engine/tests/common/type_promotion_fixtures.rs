//! Ground truth for the Apache Spark Iceberg type-promotion E2E fixture
//! (`packaging/iceberg-type-promotion-fixture`), used to prove
//! `vs-adapter/iceberg-type-promotion`'s read path against a table a real
//! Iceberg writer promoted mid-life.
//!
//! Like `int96_fixtures.rs` and `pos_delete_fixtures.rs`, this table is NOT
//! seeded by this Rust test harness: it is authored once, at Docker Compose
//! stack bring-up, by the `spark-iceberg-fixtures` one-shot job running
//! Apache Spark's Iceberg Spark runtime against the SAME shared REST catalog
//! and MinIO every other E2E table uses (`NAMESPACE` below matches
//! `seed::E2E_NAMESPACE`). No API in `iceberg` 0.10.0 can change a column's
//! type, so only a real writer's `ALTER TABLE ... ALTER COLUMN ... TYPE` can
//! produce this shape.
//!
//! The table/column/row ground truth below is NOT discovered at test time —
//! it is the fixed ground truth
//! `scripts/spark-fixtures/create_iceberg_type_promotion_fixture.sql` commits,
//! and MUST stay in lockstep with that script.
//!
//! There is deliberately no `date` -> `timestamp` fixture here: Apache
//! Iceberg Java never implements that promotion at any version this stack
//! can run, so no conforming writer can author it. Its refusal
//! (`refuse_date_promotion`) is covered by unit tests over a synthetic
//! `TableMetadata` alone — see `specs/_decision/074-add-type-relaxation.md`,
//! "Iceberg `date` -> `timestamp` / `timestamp_ns` is refused at plan time from
//! the schema history".
//!
//! Iceberg source/target types committed by the fixture SQL (the authority
//! for these types): `int_long` `int` -> `long`; `float_double` `float` ->
//! `double`; `decimal_decimal` `decimal(10,2)` -> `decimal(20,2)` (precision
//! widens, scale stays 2 — Iceberg's decimal promotion rule keeps scale
//! unchanged).

/// Namespace shared with the other E2E seed tables (`seed::E2E_NAMESPACE`).
pub const NAMESPACE: &str = "e2e_lakehouse";

/// Table name for the Iceberg type-promotion fixture. Full ref:
/// `rest_catalog.e2e_lakehouse.iceberg_type_promotion`.
pub const ICEBERG_TYPE_PROMOTION_TABLE: &str = "iceberg_type_promotion";

/// Never-promoted identity column.
pub const ID_COLUMN: &str = "id";

/// `int` -> `long` promoted column.
pub const INT_LONG_COLUMN: &str = "int_long";

/// `float` -> `double` promoted column.
pub const FLOAT_DOUBLE_COLUMN: &str = "float_double";

/// `decimal(10,2)` -> `decimal(20,2)` promoted column — precision widens,
/// scale stays 2 (Iceberg's decimal promotion rule keeps scale unchanged).
pub const DECIMAL_DECIMAL_COLUMN: &str = "decimal_decimal";

/// Physical Parquet type the pre-promotion data file's `int_long` column MUST
/// still carry — see the fixture-shape test in
/// `packaging/iceberg-type-promotion-fixture`.
pub const INT_LONG_PRE_PROMOTION_PHYSICAL_TYPE: &str = "INT32";

/// Physical Parquet type the pre-promotion data file's `float_double` column
/// MUST still carry.
pub const FLOAT_DOUBLE_PRE_PROMOTION_PHYSICAL_TYPE: &str = "FLOAT";

/// Physical Parquet type the pre-promotion data file's `decimal_decimal`
/// column MUST still carry — Iceberg encodes a decimal of precision <= 18 as
/// a physical `INT64`.
pub const DECIMAL_DECIMAL_PRE_PROMOTION_PHYSICAL_TYPE: &str = "INT64";

/// One row of `iceberg_type_promotion`, at the types the scan returns after
/// the cast to the table's current (promoted) schema.
pub struct TypePromotionRow {
    pub id: i64,
    pub int_long: i64,
    pub float_double: f64,
    /// Exact decimal literal text — `f64` cannot carry
    /// `decimal(20,2)`'s 18 integral digits without rounding.
    pub decimal_decimal: &'static str,
}

/// Rows committed BEFORE the three promotions, in the data file whose
/// physical Parquet encoding is still `int` / `float` / `decimal(10,2)`.
/// `int_long` and `float_double` are exactly representable at their source
/// width; a wrong-width or unsigned read of `int_long` would lose the sign
/// of the second row.
pub const PRE_PROMOTION_ROWS: [TypePromotionRow; 2] = [
    TypePromotionRow {
        id: 1,
        int_long: 2_147_483_647,
        float_double: 3.5,
        decimal_decimal: "12345678.9",
    },
    TypePromotionRow {
        id: 2,
        int_long: -2_147_483_648,
        float_double: -1.25,
        decimal_decimal: "-12345678.9",
    },
];

/// Rows committed AFTER the three promotions, in the data file whose
/// physical Parquet encoding is `long` / `double` / `decimal(20,2)`. Every
/// value is outside what the source type could hold — `int_long` sits one
/// step past each 32-bit boundary, `float_double` needs more than binary32's
/// mantissa, and `decimal_decimal` uses all 18 integral digits of
/// `decimal(20,2)`.
pub const POST_PROMOTION_ROWS: [TypePromotionRow; 2] = [
    TypePromotionRow {
        id: 3,
        int_long: 2_147_483_648,
        float_double: 1.234_567_890_123_457,
        decimal_decimal: "123456789012345678.9",
    },
    TypePromotionRow {
        id: 4,
        int_long: -2_147_483_649,
        float_double: -9.876_543_210_987_654,
        decimal_decimal: "-123456789012345678.9",
    },
];
