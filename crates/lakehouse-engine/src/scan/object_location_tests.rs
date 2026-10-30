use super::*;

/// Every character a URL parse, `object_store`'s `PathPart::from`, or a percent-decoder changes.
const AWKWARD_SEGMENTS: &[&str] = &[
    "%",
    "%3D",
    "%zz",
    "%25",
    "%2F",
    "%2E%2E",
    "#",
    "?",
    "*",
    "~",
    "[",
    "]",
    "{",
    "}",
    "^",
    "|",
    " ",
    "+",
    "€",
    "&",
    "a\\b",
    "trailing ",
];

const STORE_ROOTS: &[&str] = &[
    "s3://bucket",
    "abfss://fs@account.dfs.core.windows.net",
    "memory://",
    "file://",
];

fn segment_name(segment: &str) -> String {
    format!("x{segment}y")
}

fn round_trip(location: &ObjectLocation) -> ObjectLocation {
    ObjectLocation::parse_wire(&location.to_wire())
        .unwrap_or_else(|e| panic!("'{}' parses back: {e}", location.to_wire()))
}

/// Scenario: Every source's key reaches the scan as the same raw key
#[test]
fn every_entry_point_round_trips_its_raw_key_through_the_wire() {
    for store_root in STORE_ROOTS {
        for segment in AWKWARD_SEGMENTS {
            let raw_key = format!("t/{}/f.parquet", segment_name(segment));
            let raw = format!("{store_root}/{raw_key}");

            let from_raw = ObjectLocation::from_raw(&raw).expect("a raw location parses");
            let listed = ObjectLocation::from_raw(&format!("{store_root}/t"))
                .and_then(|root| root.child(&segment_name(segment)))
                .and_then(|folder| folder.child("f.parquet"))
                .expect("listed segments join literally");
            let encoded: String = utf8_percent_encode(&raw_key, percent_encoding::NON_ALPHANUMERIC)
                .to_string()
                .replace("%2F", "/");
            let from_url = ObjectLocation::from_url(&format!("{store_root}/{encoded}"))
                .expect("a percent-encoded URL parses");

            for (source, location) in [
                ("from_raw", from_raw),
                ("child", listed),
                ("from_url", from_url),
            ] {
                assert_eq!(location.key().as_ref(), raw_key, "{source}: {raw}");
                let read = round_trip(&location);
                assert_eq!(
                    read,
                    location,
                    "{source}: {raw} via '{}'",
                    location.to_wire()
                );
                assert_eq!(read.to_string(), raw, "{source}: the raw form");
            }
        }
    }
}

/// Scenario: A data-file path under a table root holding escaped characters is carried relative
#[test]
fn a_root_holding_awkward_characters_yields_relative_entries_that_rebuild_the_raw_key() {
    let root = ObjectLocation::from_raw("s3://bucket/db/odd%3D#?€/t").expect("raw root");
    let file = ObjectLocation::from_raw("s3://bucket/db/odd%3D#?€/t/data/p=a%3D/f#1?.parquet")
        .expect("raw file");

    let relative = file
        .relative_to(&root)
        .expect("the file lies below the root");
    let rebuilt = ObjectLocation::parse_entry(&relative, &root.to_wire()).expect("entry parses");

    assert_eq!(relative, "data/p=a%253D/f%231%3F.parquet");
    assert_eq!(rebuilt, file);
}

#[test]
fn only_an_object_strictly_below_the_root_in_its_store_is_relative() {
    let root = ObjectLocation::from_raw("s3://bucket/db/t").unwrap();
    for outside in [
        "s3://bucket/db/t",
        "s3://bucket/db/t-archive/f.parquet",
        "s3://bucket/db/other/f.parquet",
        "s3://other/db/t/f.parquet",
        "s3a://bucket/db/t/f.parquet",
    ] {
        let location = ObjectLocation::from_raw(outside).unwrap();
        assert_eq!(location.relative_to(&root), None, "{outside}");
    }
    let bucket_root = ObjectLocation::from_raw("s3://bucket/").unwrap();
    let file = ObjectLocation::from_raw("s3://bucket/db/f.parquet").unwrap();
    assert_eq!(
        file.relative_to(&bucket_root).as_deref(),
        Some("db/f.parquet")
    );
}

#[test]
fn a_raw_location_keeps_its_key_verbatim_and_drops_bare_separators() {
    for (location, store_root, key) in [
        (
            "s3://bucket/tbl/p_str=a b%2Fc/",
            "s3://bucket",
            "tbl/p_str=a b%2Fc",
        ),
        ("S3://bucket/tbl", "s3://bucket", "tbl"),
        ("s3://bucket", "s3://bucket", ""),
        ("s3://bucket/", "s3://bucket", ""),
        ("s3://bucket/a#b/c?d", "s3://bucket", "a#b/c?d"),
    ] {
        let parsed = ObjectLocation::from_raw(location).expect("a raw location parses");
        assert_eq!(
            (parsed.store_root().as_str(), parsed.key().as_ref()),
            (store_root, key),
            "{location}"
        );
    }
}

#[test]
fn a_location_naming_no_scheme_or_no_valid_key_is_refused_naming_it() {
    for (location, cause) in [
        ("bucket/tbl", "no scheme"),
        ("://bucket/tbl", "no scheme"),
        ("s3://bucket/tbl//part", "empty path segment"),
        ("s3://bucket/tbl/../part", ".."),
    ] {
        let error = ObjectLocation::from_raw(location)
            .expect_err("a location naming no raw key is refused")
            .to_string();
        assert!(
            error.contains(location) && error.contains(cause),
            "'{location}' must be refused naming it and '{cause}': {error}"
        );
    }
}

#[test]
fn a_url_decodes_exactly_once() {
    let location = ObjectLocation::from_url("s3://warehouse/direct/my%20events%2525").unwrap();
    assert_eq!(location.key().as_ref(), "direct/my events%25");
}
