use super::*;

fn mapping(template: &str) -> UserMapping {
    UserMapping::compile(template)
        .unwrap_or_else(|e| panic!("the template must compile: {template:?}: {e}"))
}

fn principal(template: &str, user: &str) -> String {
    mapping(template)
        .principal_for(user)
        .unwrap_or_else(|e| panic!("{template:?} must map {user:?}: {e}"))
}

fn refusal(template: &str, user: &str) -> String {
    match mapping(template).principal_for(user) {
        Ok(principal) => panic!("{template:?} must refuse {user:?}, mapped it to {principal:?}"),
        Err(UdfError::User(message)) => message,
        Err(other) => panic!("a refusal is a user error, got {other:?}"),
    }
}

fn compile_error(template: &str) -> String {
    match UserMapping::compile(template) {
        Ok(_) => panic!("{template:?} must not compile"),
        Err(UdfError::User(message)) => message,
        Err(other) => panic!("a compile error is a user error, got {other:?}"),
    }
}

fn props(pairs: &[(&str, &str)]) -> Json {
    Json::Object(
        pairs
            .iter()
            .map(|(key, value)| (key.to_string(), Json::from(*value)))
            .collect(),
    )
}

fn gate(mapping: &str, user: &str) -> PermissionGate {
    PermissionSettings::parse(&props(&[
        (PERMISSION_CHECK, "LAKEKEEPER"),
        (USER_MAPPING, mapping),
    ]))
    .expect("the properties are valid")
    .gate_for(|| Some(user.to_string()))
    .unwrap_or_else(|e| panic!("{user:?} must map: {e}"))
    .expect("the check is on, so a gate is built")
}

fn parse_error(properties: &Json) -> String {
    match PermissionSettings::parse(properties) {
        Ok(_) => panic!("{properties} must be rejected"),
        Err(UdfError::User(message)) => message,
        Err(other) => panic!("a property error is a user error, got {other:?}"),
    }
}

const ONE_LINE_BRANCHES: &str = r#"{% if user == "ETL_SVC" %}oidc~6f1c0d2e-58b4-4c39-9a8f-2b1e7d4c0a91{% elif user is endingwith("_EXT") %}oidc~{{ user[:-4]|lower }}@partner.com{% else %}oidc~{{ user|lower }}@corp.net{% endif %}"#;

const INDENTED_BRANCHES: &str = r#"{% if user == "ETL_SVC" %}
  oidc~6f1c0d2e-58b4-4c39-9a8f-2b1e7d4c0a91
{% elif user is endingwith("_EXT") %}
  oidc~{{ user[:-4]|lower }}@partner.com
{% else %}
  oidc~{{ user|lower }}@corp.net
{% endif %}"#;

const TAGS_ON_INDENTED_LINES: &str = "  {% if user == \"ETL_SVC\" %}\n    \
     oidc~6f1c0d2e-58b4-4c39-9a8f-2b1e7d4c0a91\n  \
     {% elif user is endingwith(\"_EXT\") %}\n    \
     oidc~{{ user[:-4]|lower }}@partner.com\n  \
     {% else %}\n    \
     oidc~{{ user|lower }}@corp.net\n  \
     {% endif %}\n";

const LOOKUP_TABLE: &str =
    r#"{% set ids = {"ALICE": "a.smith", "BOB": "b.jones"} %}oidc~{{ ids[user] }}@corp.net"#;

const E2E_TEMPLATE: &str = "{% if user is startingwith(\"LK_PERM_\") %}\n\
     \toidc~lk.{{ user[8:]|lower }}@lakehouse.test\n\
     {% endif %}";

/// Scenario: USER_MAPPING maps the querying user to a principal
#[test]
fn each_example_template_renders_its_principal() {
    let cases: [(&str, &str, &str); 14] = [
        (
            "oidc~{{ user|lower }}@corp.net",
            "ALICE",
            "oidc~alice@corp.net",
        ),
        (
            "oidc~{{ user[4:]|lower }}@corp.net",
            "EXA_ALICE",
            "oidc~alice@corp.net",
        ),
        (
            r#"oidc~{{ user|lower|replace("_", ".") }}@corp.net"#,
            "ALICE_COOPER",
            "oidc~alice.cooper@corp.net",
        ),
        (
            ONE_LINE_BRANCHES,
            "ETL_SVC",
            "oidc~6f1c0d2e-58b4-4c39-9a8f-2b1e7d4c0a91",
        ),
        (ONE_LINE_BRANCHES, "BOB_EXT", "oidc~bob@partner.com"),
        (
            ONE_LINE_BRANCHES,
            "ALICE_COOPER",
            "oidc~alice_cooper@corp.net",
        ),
        (
            INDENTED_BRANCHES,
            "ETL_SVC",
            "oidc~6f1c0d2e-58b4-4c39-9a8f-2b1e7d4c0a91",
        ),
        (INDENTED_BRANCHES, "BOB_EXT", "oidc~bob@partner.com"),
        (
            INDENTED_BRANCHES,
            "ALICE_COOPER",
            "oidc~alice_cooper@corp.net",
        ),
        (
            TAGS_ON_INDENTED_LINES,
            "ETL_SVC",
            "oidc~6f1c0d2e-58b4-4c39-9a8f-2b1e7d4c0a91",
        ),
        (TAGS_ON_INDENTED_LINES, "BOB_EXT", "oidc~bob@partner.com"),
        (
            TAGS_ON_INDENTED_LINES,
            "ALICE_COOPER",
            "oidc~alice_cooper@corp.net",
        ),
        (LOOKUP_TABLE, "ALICE", "oidc~a.smith@corp.net"),
        (
            E2E_TEMPLATE,
            "LK_PERM_ALLOWED",
            "oidc~lk.allowed@lakehouse.test",
        ),
    ];

    for (template, user, expected) in cases {
        assert_eq!(
            principal(template, user),
            expected,
            "{template:?} for {user}"
        );
    }
}

/// Scenario: A user that USER_MAPPING cannot map is refused before any request
#[test]
fn each_render_failure_and_rejected_principal_is_refused() {
    let deny_list =
        r#"{% if user not in ["SYS", "DBA_ADMIN"] %}oidc~{{ user|lower }}@corp.net{% endif %}"#;
    let strict_case = "{% if user == user|upper %}oidc~{{ user|lower }}@corp.net{% endif %}";
    let unknown_filter = "oidc~{{ user|nosuch }}@corp.net";
    let fuel_burner = "{% for i in range(1000) %}{% for j in range(1000) %}{% endfor %}\
                       {% endfor %}oidc~{{ user|lower }}@corp.net";
    let cases: [(&str, &str, &[&str]); 9] = [
        (LOOKUP_TABLE, "CAROL", &["'CAROL'", "undefined"]),
        (deny_list, "SYS", &["'SYS'", "empty principal"]),
        (E2E_TEMPLATE, "SYS", &["'SYS'", "empty principal"]),
        (strict_case, "alice", &["'alice'", "empty principal"]),
        (
            unknown_filter,
            "ALICE",
            &["'ALICE'", "unknown filter", "nosuch"],
        ),
        (fuel_burner, "ALICE", &["'ALICE'", "fuel"]),
        (
            "oidc~{{ user }}@corp.net",
            "ALICE SMITH",
            &["'ALICE SMITH'", "whitespace"],
        ),
        (
            "oidc~{{ user }}@corp.net",
            "ALICE\u{7}",
            &["control character"],
        ),
        (
            "oidc~{{ user }}@corp.net",
            "ALICE\u{2003}B",
            &["whitespace"],
        ),
    ];

    for (template, user, fragments) in cases {
        let message = refusal(template, user);
        for fragment in fragments {
            assert!(
                message.contains(fragment),
                "{template:?} for {user:?}: the refusal must contain {fragment:?}: {message}"
            );
        }
        assert!(
            message.contains(USER_MAPPING),
            "the refusal must name USER_MAPPING: {message}"
        );
    }
    for user in ["BOB", "SYS", "ANYONE"] {
        refusal(unknown_filter, user);
        refusal(fuel_burner, user);
    }
}

/// Scenario: A user that USER_MAPPING cannot map is refused before any request
#[test]
fn an_absent_or_empty_current_user_is_refused() {
    let settings = PermissionSettings::parse(&props(&[
        (PERMISSION_CHECK, "LAKEKEEPER"),
        (USER_MAPPING, "oidc~{{ user|lower }}@corp.net"),
    ]))
    .expect("the properties are valid");

    for current_user in [None, Some(String::new())] {
        let Err(UdfError::User(message)) = settings.gate_for(|| current_user.clone()) else {
            panic!("{current_user:?} must be refused as a user error");
        };
        assert!(
            message.contains("names no current Exasol user"),
            "the refusal must state that the request names no user: {message}"
        );
    }
}

/// Scenario: A user that USER_MAPPING cannot map is refused before any request
#[test]
fn a_missing_lookup_never_yields_a_partial_principal() {
    for template in [
        r#"{% set ids = {"ALICE": "a.smith"} %}oidc~{{ ids[user] }}@corp.net"#,
        "oidc~{{ undeclared }}@corp.net",
        "oidc~{{ user.no_such_attribute }}@corp.net",
    ] {
        let message = refusal(template, "CAROL");
        assert!(
            !message.contains("oidc~@corp.net"),
            "a partial principal must never surface: {message}"
        );
    }
}

/// Scenario: Invalid permission properties are rejected before the catalog is contacted
#[test]
fn user_mapping_rejects_a_template_that_does_not_compile() {
    let message = compile_error("oidc~{{ user|lower @corp.net");
    assert!(message.contains("does not compile"), "{message}");
    assert!(message.contains("line 1"), "{message}");
    assert!(message.contains(USER_MAPPING), "{message}");

    let message =
        compile_error("{% if user %}\n  oidc~{{ user }}@corp.net\n{% elif %}\n{% endif %}");
    assert!(message.contains("line 3"), "{message}");

    for template in [
        r#"{% include "other" %}"#,
        r#"{% import "other" as other %}"#,
        r#"{% extends "base" %}"#,
        "{% macro principal(u) %}oidc~{{ u }}{% endmacro %}{{ principal(user) }}",
    ] {
        let message = compile_error(template);
        assert!(
            message.contains("does not compile"),
            "{template:?}: {message}"
        );
    }
}

/// Scenario: The adapter trusts the template author and evaluates no user name as template text
#[test]
fn a_user_name_is_never_compiled_as_template_text() {
    let verbatim = "oidc~{{ user }}@corp.net";
    assert_eq!(principal(verbatim, "{{7*7}}"), "oidc~{{7*7}}@corp.net");
    assert_eq!(principal(verbatim, "{%if%}"), "oidc~{%if%}@corp.net");
    assert_eq!(principal(verbatim, r#"A"B\C"#), r#"oidc~A"B\C@corp.net"#);

    let shared = r#"oidc~{% if user is startingwith("BI_") %}svc-reporting{% else %}{{ user|lower }}{% endif %}@corp.net"#;
    for user in ["BI_TABLEAU", "BI_POWERBI"] {
        assert_eq!(principal(shared, user), "oidc~svc-reporting@corp.net");
    }
    assert_eq!(
        principal("oidc~{{ user|lower }}@corp.net", "alice"),
        principal("oidc~{{ user|lower }}@corp.net", "ALICE"),
        "the author's template decides whether two names share a principal"
    );
}

/// Scenario: Absent PERMISSION_CHECK leaves every request unchanged and contacts no management API
#[test]
fn an_absent_or_empty_permission_check_is_off_and_reads_neither_mapping_nor_user() {
    for properties in [
        props(&[]),
        props(&[(USER_MAPPING, "oidc~{{ user|lower @corp.net")]),
        props(&[
            (PERMISSION_CHECK, ""),
            (USER_MAPPING, "{% include \"x\" %}"),
        ]),
    ] {
        let settings = PermissionSettings::parse(&properties)
            .unwrap_or_else(|e| panic!("{properties} turns the check off: {e}"));
        assert!(!settings.is_on(), "{properties} turns the check off");
        let gate = settings
            .gate_for(|| panic!("the check is off, so the current user must not be read"))
            .expect("an off check needs no user");
        assert!(gate.is_none(), "an off check builds no gate");
    }
}

/// Scenario: Invalid permission properties are rejected before the catalog is contacted
#[test]
fn invalid_permission_properties_are_rejected() {
    let message = parse_error(&props(&[(PERMISSION_CHECK, "OPA")]));
    for fragment in ["'OPA'", "'LAKEKEEPER'", "absent", "off"] {
        assert!(message.contains(fragment), "{fragment:?}: {message}");
    }

    for mapping_props in [vec![], vec![(USER_MAPPING, "")]] {
        let mut pairs = vec![(PERMISSION_CHECK, "LAKEKEEPER")];
        pairs.extend(mapping_props);
        let message = parse_error(&props(&pairs));
        assert!(
            message.contains(USER_MAPPING) && message.contains("requires"),
            "{message}"
        );
    }

    let message = parse_error(&props(&[
        (PERMISSION_CHECK, "LAKEKEEPER"),
        (USER_MAPPING, "oidc~{{ user|lower @corp.net"),
    ]));
    assert!(
        message.contains("does not compile") && message.contains("line 1"),
        "{message}"
    );
}

#[test]
fn the_check_is_on_for_lakekeeper_in_any_letter_case() {
    for value in ["LAKEKEEPER", "lakekeeper", "LakeKeeper"] {
        let settings = PermissionSettings::parse(&props(&[
            (PERMISSION_CHECK, value),
            (USER_MAPPING, "oidc~{{ user|lower }}@corp.net"),
        ]))
        .unwrap_or_else(|e| panic!("{value} turns the check on: {e}"));
        assert!(settings.is_on(), "{value} turns the check on");
    }
}

/// Scenario: USER_MAPPING maps the querying user to a principal
#[test]
fn the_gate_carries_the_current_user_and_its_principal() {
    let gate = gate("oidc~{{ user|lower }}@corp.net", "ALICE");

    let Err(UdfError::User(message)) = gate.verdict(&["db.t"], &[]) else {
        panic!("a table without an allowing decision refuses the query with a user error");
    };

    assert!(message.contains("'ALICE'"), "{message}");
    assert!(message.contains("'oidc~alice@corp.net'"), "{message}");
}

#[test]
fn a_denial_names_the_user_the_principal_and_every_denied_table_once() {
    let gate = gate(E2E_TEMPLATE, "LK_PERM_DENIED");
    let decisions = [
        TableReadDecision {
            table: "db.allowed".to_string(),
            allowed: true,
        },
        TableReadDecision {
            table: "db.denied".to_string(),
            allowed: false,
        },
        TableReadDecision {
            table: "db.missing".to_string(),
            allowed: false,
        },
    ];

    let Err(UdfError::User(message)) = gate.verdict(
        &["db.allowed", "db.denied", "db.denied", "db.missing"],
        &decisions,
    ) else {
        panic!("a denied table refuses the query with a user error");
    };

    for fragment in [
        "'LK_PERM_DENIED'",
        "'oidc~lk.denied@lakehouse.test'",
        "db.denied",
        "db.missing",
        "grant",
    ] {
        assert!(message.contains(fragment), "{fragment:?}: {message}");
    }
    assert_eq!(message.matches("db.denied").count(), 1, "{message}");
    assert!(!message.contains("db.allowed"), "{message}");
}

#[test]
fn a_table_without_an_allowing_decision_is_denied() {
    let gate = gate("oidc~{{ user|lower }}@corp.net", "ALICE");
    let allowed = [TableReadDecision {
        table: "db.t".to_string(),
        allowed: true,
    }];

    gate.verdict(&["db.t"], &allowed)
        .expect("every table allowed admits the request");
    let Err(UdfError::User(message)) = gate.verdict(&["db.t", "db.u"], &allowed) else {
        panic!("a table the answer does not cover must never count as allowed");
    };
    assert!(message.contains("db.u"), "{message}");
}
