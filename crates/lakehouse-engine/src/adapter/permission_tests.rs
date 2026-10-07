use super::*;

fn props(pairs: &[(&str, &str)]) -> Json {
    Json::Object(
        pairs
            .iter()
            .map(|(key, value)| (key.to_string(), Json::from(*value)))
            .collect(),
    )
}

fn user_error<T>(result: Result<T, UdfError>) -> String {
    match result {
        Err(UdfError::User(message)) => message,
        Err(other) => panic!("expected a user error, got {other:?}"),
        Ok(_) => panic!("expected an error"),
    }
}

fn principal(template: &str, user: &str) -> Result<String, UdfError> {
    UserMapping::compile(template)?.principal_for(user)
}

fn checked(mapping: &str) -> PermissionSettings {
    PermissionSettings::parse(&props(&[
        (PERMISSION_CHECK, "LAKEKEEPER"),
        (USER_MAPPING, mapping),
    ]))
    .expect("valid properties")
}

#[test]
fn a_template_maps_the_user_to_a_principal() {
    assert_eq!(
        principal("oidc~{{ user | lower }}@corp.net", "ALICE").unwrap(),
        "oidc~alice@corp.net"
    );
    assert_eq!(
        principal("{{ {'ALICE': 'oidc~a'}[user] }}", "ALICE").unwrap(),
        "oidc~a"
    );
}

#[test]
fn a_user_without_a_valid_principal_is_refused() {
    let lookup = "{{ {'ALICE': 'oidc~a'}[user] }}";
    for (template, user) in [
        (lookup, "BOB"),
        ("{{ '' }}", "ALICE"),
        ("a b{{ user }}", "ALICE"),
    ] {
        let message = user_error(principal(template, user));
        assert!(message.contains(USER_MAPPING), "{message}");
    }
}

#[test]
fn a_user_name_is_data_and_never_template_text() {
    assert_eq!(
        principal("oidc~{{ user }}", "{{7*7}}").unwrap(),
        "oidc~{{7*7}}"
    );
}

#[test]
fn a_template_that_does_not_compile_is_rejected() {
    assert!(user_error(UserMapping::compile("{{ user ").map(drop)).contains("does not compile"));
}

#[test]
fn an_absent_permission_check_is_off_and_needs_no_user() {
    let settings = PermissionSettings::parse(&props(&[])).unwrap();

    let check = settings.check_for(|| panic!("an off check never reads the user"));

    assert!(matches!(check, Ok(PermissionCheck::Off)));
}

#[test]
fn invalid_permission_properties_are_rejected() {
    for properties in [
        props(&[(PERMISSION_CHECK, "OPA")]),
        props(&[(PERMISSION_CHECK, "LAKEKEEPER")]),
        props(&[(PERMISSION_CHECK, "LAKEKEEPER"), (USER_MAPPING, "{{ user ")]),
    ] {
        assert!(PermissionSettings::parse(&properties).is_err());
    }
}

#[test]
fn the_check_needs_the_iceberg_rest_kind() {
    let settings = checked("{{ user }}");

    assert!(
        settings
            .require_supported_kind(CatalogKind::IcebergRest)
            .is_ok()
    );
    assert!(
        settings
            .require_supported_kind(CatalogKind::UnityCatalogNative)
            .is_err()
    );
}

#[test]
fn the_check_needs_a_catalog_uri_ending_in_catalog() {
    let settings = checked("{{ user }}");

    assert!(settings.require_management_url("http://lk/catalog").is_ok());
    assert!(settings.require_management_url("http://lk/api").is_err());
}

#[test]
fn an_absent_current_user_is_refused() {
    let settings = checked("{{ user }}");

    assert!(settings.check_for(|| None).is_err());
    assert!(settings.check_for(|| Some(String::new())).is_err());
}

/// Scenario: A denied table refuses the whole query
#[test]
fn a_denial_names_the_user_the_principal_and_each_denied_table_once() {
    let Ok(PermissionCheck::Enforced(gate)) =
        checked("oidc~{{ user }}").check_for(|| Some("ALICE".into()))
    else {
        panic!("the check must be enforced");
    };
    let decisions = [
        TableReadDecision {
            table: "a.t".into(),
            allowed: true,
        },
        TableReadDecision {
            table: "b.u".into(),
            allowed: false,
        },
    ];

    let message = user_error(gate.verdict(&["a.t", "b.u", "b.u", "c.v"], &decisions));

    assert!(
        message.contains("'ALICE'") && message.contains("'oidc~ALICE'"),
        "{message}"
    );
    assert!(message.contains("b.u, c.v"), "{message}");
}
