use super::*;

fn text(value: f64) -> String {
    ExasolDoubleText::of(value)
        .expect("a finite value has Exasol text")
        .to_string()
}

fn assert_texts(cases: &[(f64, &str)]) {
    for &(value, expected) in cases {
        assert_eq!(text(value), expected, "Exasol's text for {value:e}");
    }
}

/// The FNV-1a digest of Exasol 2025.1.16's `power_ten_longdouble_base` entries `-308..=308`
/// (significand, then exponent, little-endian), read from `libElementaryConstants.so`.
const EXASOL_POWER_TABLE_DIGEST: u64 = 0x3e8c_db84_ceb3_d677;

#[test]
fn power_table_matches_exasols_extended_powers_of_ten() {
    let mut digest = 0xcbf2_9ce4_8422_2325_u64;
    for power in -LARGEST_TABLE_POWER..=LARGEST_TABLE_POWER {
        let entry = power_of_ten(power);
        for byte in entry
            .significand
            .to_le_bytes()
            .into_iter()
            .chain(entry.exponent.to_le_bytes())
        {
            digest ^= u64::from(byte);
            digest = digest.wrapping_mul(0x0000_0100_0000_01b3);
        }
    }

    assert_eq!(power_of_ten(-1).significand, 0xcccc_cccc_cccc_cccd);
    assert_eq!(power_of_ten(28).significand, 0x813f_3978_f894_0984);
    assert_eq!(power_of_ten(308).exponent, 960);
    assert_eq!(digest, EXASOL_POWER_TABLE_DIGEST);
}

#[test]
fn zero_prints_without_a_sign() {
    assert_texts(&[(0.0, "0"), (-0.0, "0")]);
}

#[test]
fn non_finite_values_have_no_text() {
    for value in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
        assert!(ExasolDoubleText::of(value).is_none(), "{value}");
    }
}

#[test]
fn values_below_one_keep_fifteen_decimal_places() {
    assert_texts(&[
        (0.028_980_134_693_128_67, "0.028980134693129"),
        (-0.000_396_246_652_945_307_85, "-0.000396246652945"),
        (1.0 / 3.0, "0.333333333333333"),
    ]);
}

#[test]
fn a_tie_rounds_away_from_zero() {
    assert_texts(&[
        (702_268_084_903_108.5, "702268084903109"),
        (-702_268_084_903_108.5, "-702268084903109"),
        (92_754_470_907_676.25, "92754470907676.3"),
        (5_016_950_868_666_405.0, "5.01695086866641e15"),
        (-5_016_950_868_666_405.0, "-5.01695086866641e15"),
    ]);
}

#[test]
fn the_extended_precision_product_decides_a_near_tie() {
    // Exactly 8575990.634034804999828...; the 64-bit product rounds onto the half.
    assert_texts(&[(8_575_990.634_034_805, "8575990.63403481")]);
}

#[test]
fn a_carry_in_fixed_notation_widens_the_integer_part() {
    assert_texts(&[
        (0.999_999_999_999_999_9, "1"),
        (99.999_999_999_999_99, "100"),
        (999_999_999_999_999.9, "1000000000000000"),
    ]);
}

#[test]
fn a_carry_in_scientific_notation_saturates_the_mantissa() {
    assert_texts(&[
        (1e-20, "9.99999999999999e-21"),
        (1e23, "9.99999999999999e22"),
        (0.0001_f64.next_down(), "9.99999999999999e-5"),
        (1e-79, "9.99999999999999e-80"),
    ]);
}

#[test]
fn the_double_nearest_an_exasol_rounded_up_power_prints_that_power() {
    assert_texts(&[
        (1e-14, "1e-14"),
        (-1e-14, "-1e-14"),
        (1e89, "1e89"),
        (1e-14_f64.next_down(), "9.99999999999999e-15"),
    ]);
}

#[test]
fn subnormal_and_extreme_values_scale_through_the_table_limit() {
    assert_texts(&[
        (f64::from_bits(1), "4.94065645841247e-324"),
        (f64::MIN_POSITIVE, "2.2250738585072e-308"),
        (1e-300, "1e-300"),
        (1e300, "1e300"),
        (f64::MAX, "1.79769313486232e308"),
        (f64::MIN, "-1.79769313486232e308"),
    ]);
}
