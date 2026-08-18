use delta_kernel::table_features::TableFeature;

use super::*;

fn user_message(err: UdfError) -> String {
    match err {
        UdfError::User(message) => message,
        other => panic!("expected UdfError::User, got {other:?}"),
    }
}

/// Scenario: A reader feature outside the allow-list refuses the table before any log replay
#[test]
fn a_reader_feature_outside_the_allow_list_is_refused_with_no_per_feature_special_case() {
    let features = [TableFeature::VariantType];

    let err =
        ensure_readable(3, Some(&features)).expect_err("variantType is not on the allow-list");

    let message = user_message(err);
    assert!(
        message.contains("variantType"),
        "refusal must name the feature by its protocol spelling, got: {message}"
    );
    assert!(
        !message.contains("issue"),
        "refusal must carry no per-feature special-case citation, got: {message}"
    );
}

/// Scenario: A reader protocol version outside the readable range is refused
#[test]
fn a_reader_version_outside_the_kernels_range_is_refused_before_any_feature_check() {
    let err = ensure_readable(4, None).expect_err("reader version 4 exceeds the readable range");

    let message = user_message(err);
    assert!(
        message.contains('4'),
        "refusal must name the declared min_reader_version, got: {message}"
    );
    assert!(
        message.contains('1') && message.contains('3'),
        "refusal must state the readable range, got: {message}"
    );
}

/// Scenario: A reader protocol version outside the readable range is refused
#[test]
fn the_version_check_runs_before_the_per_feature_check() {
    let features = [TableFeature::VariantType];

    let err = ensure_readable(4, Some(&features)).expect_err("reader version 4 is unreadable");

    let message = user_message(err);
    assert!(
        !message.contains("variantType"),
        "version check must be reported instead of the feature check, got: {message}"
    );
}

/// Scenario: A reader feature outside the allow-list refuses the table before any log replay
#[test]
fn refusing_multiple_features_names_all_of_them_sorted_in_one_error() {
    let features = [
        TableFeature::VariantType,
        TableFeature::DomainMetadata,
        TableFeature::AdaptiveMetadataPreview,
    ];

    let err = ensure_readable(3, Some(&features)).expect_err("none of these are allow-listed");

    let message = user_message(err);
    let domain_metadata_pos = message
        .find("domainMetadata")
        .expect("message must name domainMetadata");
    let adaptive_metadata_pos = message
        .find("adaptiveMetadata-preview")
        .expect("message must name adaptiveMetadata-preview");
    let variant_type_pos = message
        .find("variantType")
        .expect("message must name variantType");
    assert!(
        adaptive_metadata_pos < domain_metadata_pos && domain_metadata_pos < variant_type_pos,
        "refused features must be sorted, got: {message}"
    );
}

/// Scenario: Every allow-listed reader feature keeps its table queryable
#[test]
fn both_type_widening_variants_are_allow_listed_and_pass_the_gate() {
    ensure_readable(3, Some(&[TableFeature::TypeWidening]))
        .expect("typeWidening is on the allow-list");
    ensure_readable(3, Some(&[TableFeature::TypeWideningPreview]))
        .expect("typeWidening-preview is on the allow-list");
}

/// Scenario: A legacy protocol with no reader-feature list passes the gate
#[test]
fn a_legacy_protocol_table_with_no_reader_feature_list_passes_the_gate() {
    ensure_readable(1, None).expect("min_reader_version 1 with no reader features is readable");
    ensure_readable(2, None).expect("min_reader_version 2 with no reader features is readable");
}

/// Scenario: A legacy-protocol table carries no explicit reader-feature list
#[test]
fn a_version_3_table_with_no_reader_feature_list_is_refused_as_malformed() {
    let err = ensure_readable(3, None)
        .expect_err("the protocol requires a readerFeatures list at reader version 3");

    let message = user_message(err);
    assert!(
        message.contains("readerFeatures"),
        "refusal must name the missing list, got: {message}"
    );
    assert!(
        message.contains('3'),
        "refusal must name the declared min_reader_version, got: {message}"
    );
}

/// Scenario: A reader protocol version outside the readable range is refused
#[test]
fn the_readable_version_range_is_inclusive_at_both_ends() {
    ensure_readable(0, None).expect_err("reader version 0 is below the readable range");
    ensure_readable(1, None).expect("reader version 1 is the readable range's lower bound");
    // Reader version 3 must carry the array; an empty one is the feature-free version-3 shape.
    ensure_readable(3, Some(&[])).expect("reader version 3 is the readable range's upper bound");
}

/// Scenario: Every allow-listed reader feature together still passes the gate
#[test]
fn all_seven_allow_listed_reader_features_pass_including_both_type_widening_names() {
    let features = [
        TableFeature::ColumnMapping,
        TableFeature::DeletionVectors,
        TableFeature::TimestampWithoutTimezone,
        TableFeature::TypeWidening,
        TableFeature::TypeWideningPreview,
        TableFeature::V2Checkpoint,
        TableFeature::VacuumProtocolCheck,
    ];

    ensure_readable(3, Some(&features)).expect("every allow-listed feature together is readable");
}

/// Scenario: A reader feature outside the allow-list refuses the table before any log replay
#[test]
fn an_unrecognized_reader_feature_is_refused_by_its_raw_protocol_name() {
    let features = [TableFeature::Unknown("someFutureFeature".to_string())];

    let err = ensure_readable(3, Some(&features))
        .expect_err("an unrecognized reader feature is not allow-listed");

    let message = user_message(err);
    assert!(
        message.contains("someFutureFeature"),
        "refusal must name the unrecognized feature by its raw protocol string, got: {message}"
    );
}
