//! Ground truth for the Spark Iceberg type-promotion fixture. Authored at stack
//! bring-up by the `spark-iceberg-fixtures` job because no `iceberg` 0.10 API can
//! change a column's type. Must stay in lockstep with
//! `scripts/spark-fixtures/create_iceberg_type_promotion_fixture.sql`.
//!
//! No `date` -> `timestamp` fixture: Iceberg Java never implements that promotion, so
//! no conforming writer can author it; its refusal is unit-tested instead.

pub const NAMESPACE: &str = "e2e_lakehouse";

pub const ICEBERG_TYPE_PROMOTION_TABLE: &str = "iceberg_type_promotion";

pub const ID_COLUMN: &str = "id";

pub const INT_LONG_COLUMN: &str = "int_long";

pub const FLOAT_DOUBLE_COLUMN: &str = "float_double";

/// Precision widens, scale stays 2 (Iceberg's decimal promotion keeps scale).
pub const DECIMAL_DECIMAL_COLUMN: &str = "decimal_decimal";

pub const INT_LONG_PRE_PROMOTION_PHYSICAL_TYPE: &str = "INT32";

pub const FLOAT_DOUBLE_PRE_PROMOTION_PHYSICAL_TYPE: &str = "FLOAT";

/// Iceberg encodes a decimal of precision <= 18 as physical `INT64`.
pub const DECIMAL_DECIMAL_PRE_PROMOTION_PHYSICAL_TYPE: &str = "INT64";

pub struct TypePromotionRow {
    pub id: i64,
    pub int_long: i64,
    pub float_double: f64,
    /// `f64` cannot carry `decimal(20,2)`'s 18 integral digits without rounding.
    pub decimal_decimal: &'static str,
}

/// A wrong-width or unsigned read of `int_long` would lose the second row's sign.
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

/// Every value is outside what the pre-promotion source type could hold.
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
