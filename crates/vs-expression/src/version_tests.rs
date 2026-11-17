use super::ExasolVersion;

#[test]
fn a_version_string_names_its_release_by_its_major() {
    assert_eq!(ExasolVersion::of("8.29.13"), ExasolVersion::Exasol8);
    assert_eq!(ExasolVersion::of("7.1.0"), ExasolVersion::Exasol8);
    assert_eq!(ExasolVersion::of("2025.1.16"), ExasolVersion::Exasol2025);
    assert_eq!(ExasolVersion::of("2026.1.1"), ExasolVersion::Exasol2025);
    assert_eq!(ExasolVersion::of(""), ExasolVersion::Unknown);
    assert_eq!(ExasolVersion::of("next"), ExasolVersion::Unknown);
}
