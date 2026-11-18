use super::*;
use chrono::TimeZone as _;

/// `SELECT TIME_ZONE_NAME FROM EXA_TIME_ZONES` of Exasol 8.29.13 and 2025.1.16, merged.
const EXA_TIME_ZONES: &str = include_str!("testdata/exa_time_zones.txt");

fn instant() -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2024, 1, 15, 12, 0, 0).unwrap()
}

fn wall_clock_text(zone: TimeZone) -> String {
    zone.wall_clock(instant())
        .format("%Y-%m-%d %H:%M")
        .to_string()
}

#[test]
fn tz_reads_zone_names_and_inverted_utc_offsets() {
    let cases = [
        ("CET", "2024-01-15 13:00"),
        ("Europe/Berlin", "2024-01-15 13:00"),
        ("EUROPE/BERLIN", "2024-01-15 13:00"),
        ("UTC-7", "2024-01-15 19:00"),
        ("UTC+5:30", "2024-01-15 06:30"),
        ("UTC+05:30", "2024-01-15 06:30"),
        ("Etc/GMT-7", "2024-01-15 19:00"),
        ("UTC", "2024-01-15 12:00"),
    ];
    for (tz, expected) in cases {
        let zone = TimeZone::of_tz_variable(Some(tz))
            .unwrap_or_else(|e| panic!("TZ '{tz}' must resolve: {e}"));
        assert_eq!(wall_clock_text(zone), expected, "TZ '{tz}'");
    }
}

#[test]
fn unreadable_tz_is_refused() {
    let cases = [
        (Some("NOT/A_ZONE"), "'NOT/A_ZONE'"),
        (Some(""), "''"),
        (Some("UTC+x"), "'UTC+x'"),
        (Some("UTC+25"), "'UTC+25'"),
        (None, "unset"),
    ];
    for (tz, named) in cases {
        let message = match TimeZone::of_tz_variable(tz) {
            Ok(zone) => panic!("TZ {tz:?} must be refused, got {zone:?}"),
            Err(UdfError::User(message)) => message,
            Err(other) => panic!("TZ {tz:?} must fail as a user error, got {other:?}"),
        };
        assert!(message.contains(named), "{message} must name {named}");
    }
}

#[test]
fn time_zone_values_compare_without_regard_to_case() {
    for name in ["ASIA/TOKYO", "Asia/Tokyo", "asia/tokyo"] {
        let zone = TimeZone::exasol_name(name).unwrap_or_else(|e| panic!("{name}: {e}"));
        assert_eq!(wall_clock_text(zone), "2024-01-15 21:00", "{name}");
    }
}

#[test]
fn a_posix_offset_or_unknown_name_is_no_time_zone_value() {
    for raw in ["MARS/OLYMPUS", "UTC-7", "+07:00", ""] {
        assert_eq!(
            TimeZone::exasol_name(raw),
            Err("a time zone name from EXA_TIME_ZONES".to_string()),
            "{raw}"
        );
    }
}

#[test]
fn every_exa_time_zones_name_is_a_time_zone_value() {
    for name in EXA_TIME_ZONES.lines() {
        assert!(
            TimeZone::exasol_name(name).is_ok(),
            "EXA_TIME_ZONES lists {name}"
        );
    }
}

/// Fails when a `chrono-tz` upgrade adds a zone Exasol does not list, which `SESSION_TIME_ZONE`
/// would then accept although Exasol rejects it.
#[test]
fn every_time_zone_value_is_an_exa_time_zones_name() {
    let listed: Vec<&str> = EXA_TIME_ZONES.lines().collect();
    for tz in chrono_tz::TZ_VARIANTS {
        let name = tz.name().to_uppercase();
        assert!(
            listed.contains(&name.as_str()),
            "{name} is no EXA_TIME_ZONES name"
        );
    }
}
