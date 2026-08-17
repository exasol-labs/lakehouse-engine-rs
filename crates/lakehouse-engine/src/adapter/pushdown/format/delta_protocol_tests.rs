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
fn a_reader_feature_outside_the_allow_list_is_refused_by_its_protocol_name() {
    let features = [TableFeature::TypeWideningPreview];

    let err = ensure_readable(3, Some(&features))
        .expect_err("typeWidening-preview is not on the allow-list");

    let message = user_message(err);
    assert!(
        message.contains("typeWidening-preview"),
        "refusal must name the feature by its protocol spelling, got: {message}"
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
    let features = [TableFeature::TypeWideningPreview];

    let err = ensure_readable(4, Some(&features)).expect_err("reader version 4 is unreadable");

    let message = user_message(err);
    assert!(
        !message.contains("typeWidening-preview"),
        "version check must be reported instead of the feature check, got: {message}"
    );
}

/// Scenario: A reader feature outside the allow-list refuses the table before any log replay
#[test]
fn refusing_multiple_features_names_all_of_them_sorted_in_one_error() {
    let features = [
        TableFeature::VariantType,
        TableFeature::TypeWideningPreview,
        TableFeature::AdaptiveMetadataPreview,
    ];

    let err = ensure_readable(3, Some(&features)).expect_err("none of these are allow-listed");

    let message = user_message(err);
    let type_widening_preview_pos = message
        .find("typeWidening-preview")
        .expect("message must name typeWidening-preview");
    let adaptive_metadata_pos = message
        .find("adaptiveMetadata-preview")
        .expect("message must name adaptiveMetadata-preview");
    let variant_type_pos = message
        .find("variantType")
        .expect("message must name variantType");
    assert!(
        adaptive_metadata_pos < type_widening_preview_pos
            && type_widening_preview_pos < variant_type_pos,
        "refused features must be sorted, got: {message}"
    );
}

/// Scenario: A reader feature outside the allow-list refuses the table before any log replay
#[test]
fn typewidening_variants_cite_issue_349_other_refusals_do_not() {
    let type_widening_err = ensure_readable(3, Some(&[TableFeature::TypeWidening]))
        .expect_err("typeWidening is not allow-listed");
    assert!(
        user_message(type_widening_err).contains("#349"),
        "typeWidening refusal must cite issue #349"
    );

    let type_widening_preview_err = ensure_readable(3, Some(&[TableFeature::TypeWideningPreview]))
        .expect_err("typeWidening-preview is not allow-listed");
    assert!(
        user_message(type_widening_preview_err).contains("#349"),
        "typeWidening-preview refusal must cite issue #349"
    );

    let variant_type_err = ensure_readable(3, Some(&[TableFeature::VariantType]))
        .expect_err("variantType is not allow-listed");
    assert!(
        !user_message(variant_type_err).contains("#349"),
        "variantType refusal must not cite issue #349"
    );
}

/// Scenario: A legacy protocol with no reader-feature list passes the gate
#[test]
fn a_legacy_protocol_table_with_no_reader_feature_list_passes_the_gate() {
    ensure_readable(1, None).expect("min_reader_version 1 with no reader features is readable");
    ensure_readable(2, None).expect("min_reader_version 2 with no reader features is readable");
}

/// Scenario: A reader protocol version outside the readable range is refused
#[test]
fn the_readable_version_range_is_inclusive_at_both_ends() {
    ensure_readable(0, None).expect_err("reader version 0 is below the readable range");
    ensure_readable(1, None).expect("reader version 1 is the readable range's lower bound");
    ensure_readable(3, None).expect("reader version 3 is the readable range's upper bound");
}

/// Scenario: Every allow-listed reader feature together still passes the gate
#[test]
fn every_allow_listed_reader_feature_together_passes_the_gate() {
    let features = [
        TableFeature::ColumnMapping,
        TableFeature::DeletionVectors,
        TableFeature::TimestampWithoutTimezone,
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
