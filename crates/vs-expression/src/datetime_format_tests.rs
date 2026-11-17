use super::*;

const BOTH: [FormatTarget; 2] = [FormatTarget::Date, FormatTarget::Timestamp];
const TIMESTAMP_ONLY: [FormatTarget; 1] = [FormatTarget::Timestamp];

#[test]
fn translates_supported_formats() {
    let cases: [(&str, &str, &[FormatTarget]); 16] = [
        ("YYYY-MM-DD", "%Y-%m-%d", &BOTH),
        ("yyyy-mm-dd", "%Y-%m-%d", &BOTH),
        ("Yyyy-Mm-Dd", "%Y-%m-%d", &BOTH),
        ("DD.MM.YYYY", "%d.%m.%Y", &BOTH),
        ("YYYYMMDD", "%Y%m%d", &BOTH),
        ("YYYY_MM_DD", "%Y_%m_%d", &BOTH),
        ("YYYY/MM/DD", "%Y/%m/%d", &BOTH),
        ("YYYY,MM;DD", "%Y,%m;%d", &BOTH),
        ("YYYY:MM:DD", "%Y:%m:%d", &BOTH),
        ("YYYY MM DD", "%Y %m %d", &BOTH),
        (
            "YYYY-MM-DD HH24:MI:SS",
            "%Y-%m-%d %H:%M:%S",
            &TIMESTAMP_ONLY,
        ),
        ("YYYY-MM-DD HH24:MI", "%Y-%m-%d %H:%M", &TIMESTAMP_ONLY),
        (
            "YYYY-MM-DD HH24:MI:SS.FF3",
            "%Y-%m-%d %H:%M:%S%.f",
            &TIMESTAMP_ONLY,
        ),
        (
            "YYYY-MM-DD HH24:MI:ss.ff9",
            "%Y-%m-%d %H:%M:%S%.f",
            &TIMESTAMP_ONLY,
        ),
        (
            "HH24:MI:SS YYYY-MM-DD",
            "%H:%M:%S %Y-%m-%d",
            &TIMESTAMP_ONLY,
        ),
        (
            "YYYY-MM-DD HH24:MI:SS.FF6",
            "%Y-%m-%d %H:%M:%S%.f",
            &TIMESTAMP_ONLY,
        ),
    ];
    for (format, expected, targets) in cases {
        for &target in targets {
            assert_eq!(
                strftime_pattern(format, target).as_deref(),
                Some(expected),
                "{format} for {target:?}"
            );
        }
    }
}

#[test]
fn declines_unsupported_formats() {
    let cases: [(&str, FormatTarget); 20] = [
        ("DD-MON-YYYY", FormatTarget::Date),
        ("YYYY-MM-DD HH24:MI", FormatTarget::Date),
        ("YYYY-MM-DD HH24:MI:SS", FormatTarget::Date),
        ("YYYY-MM-DD HH24", FormatTarget::Timestamp),
        ("YYYY-MM-DD HH24:SS", FormatTarget::Timestamp),
        ("YYYY-MM-DD HH24:MI:SS.FF2", FormatTarget::Timestamp),
        ("YYYY-MM-DD HH24:MI.FF3", FormatTarget::Timestamp),
        ("YYYY-MM-DD HH24:MI:SS.FF3.FF6", FormatTarget::Timestamp),
        ("YYYY-MM", FormatTarget::Date),
        ("YYYY-MM-DD-DD", FormatTarget::Date),
        ("YYYY-MM-DD-YYYY", FormatTarget::Date),
        ("YYYY-MM-DD\"T\"", FormatTarget::Date),
        ("", FormatTarget::Date),
        ("YYYY-MM-DD%", FormatTarget::Date),
        ("YYYY-MM-DD HH24:MI:SS.FF", FormatTarget::Timestamp),
        ("YYYY-MM-DD HH24:MI:SSFF", FormatTarget::Timestamp),
        ("YYYY-MM-DD HH24:MI:SSFF6", FormatTarget::Timestamp),
        ("YYYY年MM月DD日", FormatTarget::Date),
        ("YYYY-MM-DDX", FormatTarget::Date),
        ("YYYY-MM-DD HH24:MI:ſſ", FormatTarget::Timestamp),
    ];
    for (format, target) in cases {
        assert_eq!(
            strftime_pattern(format, target),
            None,
            "{format} for {target:?}"
        );
    }
}

#[test]
fn fraction_digits_follow_the_ff_part_of_a_translated_format() {
    let cases = [
        ("YYYY-MM-DD", Some(0)),
        ("YYYY-MM-DD HH24:MI:SS", Some(0)),
        ("YYYY-MM-DD HH24:MI:SS.FF3", Some(3)),
        ("YYYY-MM-DD HH24:MI:ss.ff6", Some(6)),
        ("YYYY-MM-DD HH24:MI:SS.FF9", Some(9)),
        ("YYYY-MM-DD HH24:MI:SSFF9", None),
        ("YYYY-MM-DD HH24:MI:SS.FF", None),
        ("YYYY-MM-DD HH24:MI:SS.FF2", None),
    ];
    for (format, expected) in cases {
        assert_eq!(fraction_digits(format), expected, "{format}");
    }
}
