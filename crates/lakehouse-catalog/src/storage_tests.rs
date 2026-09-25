use super::*;
use crate::test_support::s3_payload;

const ENDPOINT: &str = "http://minio.local:9000";
const REGION: &str = "us-east-1";
const ACCESS_KEY: &str = "AKIDEXAMPLE";
const SECRET_KEY: &str = "wJalrXUtnFEMI_EXAMPLE_KEY";
const SESSION_TOKEN: &str = "FwoGZXIvYXdzEXAMPLE_TOKEN";

/// `allow_http` is ON so exact map comparisons pin that it is not an iceberg config key.
fn populated_backend() -> StorageBackend {
    StorageBackend::S3(StorageProps {
        endpoint: ENDPOINT.into(),
        region: REGION.into(),
        access_key: ACCESS_KEY.into(),
        secret_key: SECRET_KEY.into(),
        session_token: Some(SESSION_TOKEN.into()),
        allow_http: true,
        path_style: true,
    })
}

fn expected_populated_config() -> HashMap<String, String> {
    HashMap::from([
        (S3_ENDPOINT.to_string(), ENDPOINT.to_string()),
        (S3_REGION.to_string(), REGION.to_string()),
        (S3_ACCESS_KEY_ID.to_string(), ACCESS_KEY.to_string()),
        (S3_SECRET_ACCESS_KEY.to_string(), SECRET_KEY.to_string()),
        (S3_SESSION_TOKEN.to_string(), SESSION_TOKEN.to_string()),
        (S3_PATH_STYLE_ACCESS.to_string(), "true".to_string()),
    ])
}

#[test]
fn catalog_storage_props_emits_every_populated_s3_key_and_nothing_else() {
    assert_eq!(
        populated_backend().catalog_storage_props(),
        expected_populated_config()
    );
}

#[test]
fn catalog_storage_props_omits_empty_connection_fields_and_an_absent_token() {
    let backend = StorageBackend::S3(StorageProps::default());

    assert_eq!(
        backend.catalog_storage_props(),
        HashMap::from([(S3_PATH_STYLE_ACCESS.to_string(), "true".to_string())]),
        "only path-style access is unconditional; every empty credential field \
         and the absent session token must be left out"
    );
}

#[test]
fn addresses_scheme_accepts_s3_and_s3a_for_the_s3_backend() {
    let backend = StorageBackend::S3(StorageProps::default());

    assert!(backend.addresses_scheme("s3"));
    assert!(backend.addresses_scheme("s3a"));
}

#[test]
fn addresses_scheme_rejects_abfss_for_the_s3_backend() {
    let backend = StorageBackend::S3(StorageProps::default());

    assert!(!backend.addresses_scheme("abfss"));
}

#[test]
fn addresses_scheme_accepts_abfss_for_the_adls_backend() {
    let backend = StorageBackend::Adls {
        account_name: "acct".into(),
        cred: AdlsCred::AccountKey("key".into()),
    };

    assert!(backend.addresses_scheme("abfss"));
}

#[test]
fn addresses_scheme_rejects_plaintext_abfs_for_the_adls_backend() {
    let backend = StorageBackend::Adls {
        account_name: "acct".into(),
        cred: AdlsCred::AccountKey("key".into()),
    };

    assert!(
        !backend.addresses_scheme("abfs"),
        "the plaintext 'abfs' scheme must be rejected; only 'abfss' is accepted"
    );
}

#[test]
fn addresses_scheme_rejects_an_unrelated_scheme_for_either_backend() {
    let s3 = StorageBackend::S3(StorageProps::default());
    let adls = StorageBackend::Adls {
        account_name: "acct".into(),
        cred: AdlsCred::AccountKey("key".into()),
    };

    assert!(!s3.addresses_scheme("gs"));
    assert!(!adls.addresses_scheme("gs"));
}

/// Scenario: A present-but-empty session token is still emitted, unlike empty connection fields
#[test]
fn catalog_storage_props_emits_a_present_but_empty_session_token() {
    let backend = StorageBackend::S3(StorageProps {
        session_token: Some(String::new()),
        path_style: false,
        ..StorageProps::default()
    });

    assert_eq!(
        backend.catalog_storage_props(),
        HashMap::from([
            (S3_SESSION_TOKEN.to_string(), String::new()),
            (S3_PATH_STYLE_ACCESS.to_string(), "false".to_string()),
        ])
    );
}

#[test]
fn file_io_is_configured_from_exactly_the_catalog_storage_props() {
    assert_eq!(
        populated_backend().file_io().config().props(),
        &expected_populated_config()
    );
}

#[test]
fn secret_values_are_the_wrapped_props_secret_values() {
    let props = StorageProps {
        access_key: ACCESS_KEY.into(),
        secret_key: SECRET_KEY.into(),
        session_token: Some(SESSION_TOKEN.into()),
        ..StorageProps::default()
    };

    assert_eq!(
        StorageBackend::S3(props.clone()).secret_values(),
        props.secret_values()
    );
}

#[test]
fn s3_serializes_under_a_lowercase_externally_tagged_variant_key() {
    assert_eq!(
        serde_json::to_value(populated_backend()).expect("backend serializes"),
        serde_json::json!({
            "s3": {
                "endpoint": ENDPOINT,
                "region": REGION,
                "access_key": ACCESS_KEY,
                "secret_key": SECRET_KEY,
                "session_token": SESSION_TOKEN,
                "allow_http": true,
                "path_style": true,
            }
        })
    );
}

#[test]
fn s3_round_trips_through_its_tagged_encoding() {
    let backend = populated_backend();
    let encoded = serde_json::to_string(&backend).expect("backend serializes");

    assert_eq!(
        serde_json::from_str::<StorageBackend>(&encoded).expect("backend deserializes"),
        backend
    );
}

/// Scenario: Untagged, unknown, wrong-case, or mismatched variant payloads are rejected
#[test]
fn only_matching_lowercase_variant_keys_decode() {
    for payload in [
        r#"{"endpoint":"","region":"","access_key":"","secret_key":""}"#,
        r#"{"S3":{"endpoint":"","region":"","access_key":"","secret_key":""}}"#,
        r#"{"azure":{"endpoint":"","region":"","access_key":"","secret_key":""}}"#,
        r#"{"Adls":{"account_name":"","cred":{"AccountKey":""}}}"#,
        r#"{"azure":{"account_name":"","cred":{"AccountKey":""}}}"#,
        r#"{"adls":{"endpoint":"","region":"","access_key":"","secret_key":""}}"#,
        r#"{"adls":{"account_name":"","cred":{"AccountKey":""}}}"#,
    ] {
        assert!(
            serde_json::from_str::<StorageBackend>(payload).is_err(),
            "payload must not decode to a storage backend: {payload}"
        );
    }
}

#[test]
fn adls_catalog_storage_props_emit_the_account_and_one_credential_key() {
    let account_key_backend = StorageBackend::Adls {
        account_name: "myaccount".into(),
        cred: AdlsCred::AccountKey("azure-static-key-secret".into()),
    };
    assert_eq!(
        account_key_backend.catalog_storage_props(),
        HashMap::from([
            (ADLS_ACCOUNT_NAME.to_string(), "myaccount".to_string()),
            (
                ADLS_ACCOUNT_KEY.to_string(),
                "azure-static-key-secret".to_string()
            ),
        ])
    );

    let sas_backend = StorageBackend::Adls {
        account_name: "myaccount".into(),
        cred: AdlsCred::Sas("sv=2024&sig=azure-sas-secret".into()),
    };
    assert_eq!(
        sas_backend.catalog_storage_props(),
        HashMap::from([
            (ADLS_ACCOUNT_NAME.to_string(), "myaccount".to_string()),
            (
                ADLS_SAS_TOKEN.to_string(),
                "sv=2024&sig=azure-sas-secret".to_string()
            ),
        ])
    );
}

#[test]
fn adls_file_io_is_configured_from_exactly_the_catalog_storage_props() {
    let backend = StorageBackend::Adls {
        account_name: "myaccount".into(),
        cred: AdlsCred::AccountKey("azure-static-key-secret".into()),
    };

    assert_eq!(
        backend.file_io().config().props(),
        &backend.catalog_storage_props()
    );
}

#[test]
fn adls_secret_values_are_the_one_credential_and_omit_an_empty_one() {
    let account_key_backend = StorageBackend::Adls {
        account_name: "myaccount".into(),
        cred: AdlsCred::AccountKey("azure-static-key-secret".into()),
    };
    assert_eq!(
        account_key_backend.secret_values(),
        vec!["azure-static-key-secret"]
    );

    let sas_backend = StorageBackend::Adls {
        account_name: "myaccount".into(),
        cred: AdlsCred::Sas("sv=2024&sig=azure-sas-secret".into()),
    };
    assert_eq!(
        sas_backend.secret_values(),
        vec!["sv=2024&sig=azure-sas-secret"]
    );

    let empty_key_backend = StorageBackend::Adls {
        account_name: "myaccount".into(),
        cred: AdlsCred::AccountKey(String::new()),
    };
    assert!(
        empty_key_backend.secret_values().is_empty(),
        "an empty credential must not surface as a secret to redact against"
    );
}

/// Scenario: AdlsCred never prints its secret via Debug, standalone or inside StorageBackend
#[test]
fn adls_cred_is_redacted_in_debug_output() {
    let account_key = AdlsCred::AccountKey("azure-static-key-secret".into());
    let key_debug = format!("{account_key:?}");
    assert!(
        !key_debug.contains("azure-static-key-secret"),
        "{key_debug}"
    );
    assert!(key_debug.contains("[redacted]"), "{key_debug}");

    let sas = AdlsCred::Sas("sv=2024&sig=azure-sas-secret".into());
    let sas_debug = format!("{sas:?}");
    assert!(
        !sas_debug.contains("sv=2024&sig=azure-sas-secret"),
        "{sas_debug}"
    );
    assert!(sas_debug.contains("[redacted]"), "{sas_debug}");

    let backend = StorageBackend::Adls {
        account_name: "myaccount".into(),
        cred: account_key,
    };
    let backend_debug = format!("{backend:?}");
    assert!(
        !backend_debug.contains("azure-static-key-secret"),
        "{backend_debug}"
    );
}

#[test]
fn adls_serializes_under_a_lowercase_externally_tagged_variant_key() {
    let backend = StorageBackend::Adls {
        account_name: "myaccount".into(),
        cred: AdlsCred::AccountKey("azure-static-key-secret".into()),
    };

    assert_eq!(
        serde_json::to_value(backend).expect("backend serializes"),
        serde_json::json!({
            "adls": {
                "account_name": "myaccount",
                "cred": {"account_key": "azure-static-key-secret"},
            }
        })
    );
}

#[test]
fn adls_round_trips_through_its_tagged_encoding() {
    let backend = StorageBackend::Adls {
        account_name: "myaccount".into(),
        cred: AdlsCred::Sas("sv=2024&sig=azure-sas-secret".into()),
    };
    let encoded = serde_json::to_string(&backend).expect("backend serializes");

    assert_eq!(
        serde_json::from_str::<StorageBackend>(&encoded).expect("backend deserializes"),
        backend
    );
}

#[test]
fn scheme_of_lowercases_a_mixed_case_scheme() {
    assert_eq!(scheme_of("S3A://bucket/key"), "s3a");
}

#[test]
fn scheme_of_is_empty_when_the_location_carries_none() {
    assert_eq!(scheme_of("bucket/key"), "");
}

#[test]
fn location_host_reads_the_authority_when_there_is_no_userinfo() {
    assert_eq!(location_host("s3://bucket/db/t"), "bucket");
}

/// Scenario: An ADLS `<container>@` segment is userinfo, not part of the host
#[test]
fn location_host_reads_the_host_after_the_container_userinfo() {
    let host = "myacct.dfs.core.windows.net";
    assert_eq!(
        location_host(&format!("abfss://mycontainer@{host}/db/t")),
        host
    );
    assert_eq!(location_host(&format!("abfss://{host}/db/t")), host);
}

#[test]
fn adls_account_name_reads_the_hosts_leading_label() {
    let location = "abfss://mycontainer@myacct.dfs.core.windows.net/db/t";
    assert_eq!(
        adls_account_name(location).expect("account name resolves"),
        "myacct"
    );
}

/// Scenario: A storage host with no leading label yields a catalog-neutral refusal
#[test]
fn adls_account_name_errs_when_the_host_has_no_leading_label() {
    for location in [
        "abfss://mycontainer@/db/t",
        "abfss://.dfs.core.windows.net/db/t",
    ] {
        let message = match adls_account_name(location) {
            Ok(name) => panic!("expected a refusal, resolved account name {name}"),
            Err(UdfError::User(message)) => message,
            Err(other) => panic!("expected UdfError::User, got {other:?}"),
        };
        assert!(
            message.contains(location),
            "the refusal must name the table location {location}: {message}"
        );
        assert!(
            message.contains("account name"),
            "the refusal must name the account name it could not derive: {message}"
        );
        assert!(
            !message.contains("Iceberg") && !message.contains("Unity"),
            "the shared refusal text must name neither catalog kind: {message}"
        );
    }
}

const ADLS_HOST: &str = "myacct.dfs.core.windows.net";
const VENDED_SAS: &str = "sv=2024-11-04&sig=VENDED_SAS_SIGNATURE";

fn user_message(error: UdfError) -> String {
    match error {
        UdfError::User(message) => message,
        other => panic!("expected UdfError::User, got {other:?}"),
    }
}

/// Scenario: `abfs://` requires ALLOW_HTTP consent; `abfss://` is never gated
#[test]
fn adls_backend_gates_abfs_on_allow_http_and_never_gates_abfss() {
    let plaintext = format!("abfs://mycontainer@{ADLS_HOST}/db/t");
    let secure = format!("abfss://mycontainer@{ADLS_HOST}/db/t");

    let message = user_message(
        adls_backend(VENDED_SAS.to_string(), &plaintext, false)
            .expect_err("abfs:// without ALLOW_HTTP must be refused"),
    );
    assert!(
        message.contains(&plaintext),
        "the refusal must name the table location {plaintext}: {message}"
    );
    assert!(
        message.contains("ALLOW_HTTP"),
        "the refusal must name the property that withholds consent: {message}"
    );
    assert!(
        message.contains("abfs://"),
        "the refusal must name the plaintext scheme it is gating: {message}"
    );
    assert!(
        !message.contains("Iceberg") && !message.contains("Unity"),
        "the shared refusal text must name neither catalog kind: {message}"
    );
    assert!(
        !message.contains(VENDED_SAS),
        "no vended secret may appear in the refusal: {message}"
    );

    let expected = StorageBackend::Adls {
        account_name: "myacct".to_string(),
        cred: AdlsCred::Sas(VENDED_SAS.to_string()),
    };
    for (location, allow_http) in [(&plaintext, true), (&secure, false), (&secure, true)] {
        let resolved =
            adls_backend(VENDED_SAS.to_string(), location, allow_http).unwrap_or_else(|e| {
                panic!("{location} with allow_http={allow_http} must resolve: {e}")
            });
        assert_eq!(
            resolved, expected,
            "{location} with allow_http={allow_http} must build the ADLS backend"
        );
    }
}

const S3_LOCATION: &str = "s3://bucket/db/t";
const VENDED_AK: &str = "VENDED_ACCESS_KEY";
const VENDED_SK: &str = "VENDED_SECRET_KEY_SENTINEL";

fn address(endpoint: &str, region: &str) -> StaticStoreAddress {
    StaticStoreAddress {
        endpoint: endpoint.to_string(),
        region: region.to_string(),
        path_style: None,
    }
}

fn address_with_path_style(
    endpoint: &str,
    region: &str,
    path_style: Option<bool>,
) -> StaticStoreAddress {
    StaticStoreAddress {
        endpoint: endpoint.to_string(),
        region: region.to_string(),
        path_style,
    }
}

fn vended_s3(endpoint: Option<&str>, region: Option<&str>, path_style: Option<bool>) -> VendedS3 {
    VendedS3 {
        access_key: VENDED_AK.to_string(),
        secret_key: VENDED_SK.to_string(),
        session_token: None,
        region: region.map(str::to_string),
        endpoint: endpoint.map(str::to_string),
        path_style,
    }
}

/// Scenario: The plaintext gate reads the resolved endpoint, case-insensitively, including a CONNECTION-supplied one
#[test]
fn s3_backend_gates_a_plaintext_endpoint_the_connection_supplied() {
    let plaintext = "http://minio:9000";

    for connection_endpoint in [plaintext, "HTTP://minio:9000"] {
        let connection = address(connection_endpoint, "");

        for vended_endpoint in [None, Some("https://s3.eu-central-1.amazonaws.com")] {
            let message = user_message(
                s3_backend(
                    vended_s3(vended_endpoint, None, None),
                    S3_LOCATION,
                    false,
                    &connection,
                )
                .expect_err("a resolved plaintext endpoint without ALLOW_HTTP must be refused"),
            );
            assert!(
                message.contains(connection_endpoint),
                "the refusal must name the plaintext endpoint it resolved: {message}"
            );
            assert!(
                message.contains(S3_LOCATION),
                "the refusal must name the table location: {message}"
            );
            assert!(
                message.contains("ALLOW_HTTP"),
                "the refusal must name the property that withholds consent: {message}"
            );
            assert!(
                !message.contains("Iceberg") && !message.contains("Unity"),
                "the shared refusal text must name neither catalog kind: {message}"
            );
            assert!(
                !message.contains(VENDED_SK),
                "no vended secret may appear in the refusal: {message}"
            );
        }
    }

    let connection = address(plaintext, "");
    let consented = s3_payload(
        s3_backend(
            vended_s3(Some("https://s3.eu-central-1.amazonaws.com"), None, None),
            S3_LOCATION,
            true,
            &connection,
        )
        .expect("the operator's consent admits the resolved plaintext endpoint"),
    );
    assert_eq!(
        consented.endpoint, plaintext,
        "under consent the CONNECTION's endpoint still wins the address rule"
    );
}

/// Scenario: Endpoint and region resolve independently, CONNECTION first, then vended
#[test]
fn store_address_resolves_endpoint_and_region_independently_with_the_connection_winning() {
    let connection_endpoint = "https://connection.endpoint.invalid";
    let connection_region = "eu-central-1";
    let vended_endpoint = "https://vended.endpoint.invalid";
    let vended_region = "us-east-1";

    for (connection, vended, expected_endpoint, expected_region) in [
        (
            address(connection_endpoint, connection_region),
            vended_s3(Some(vended_endpoint), Some(vended_region), None),
            connection_endpoint,
            connection_region,
        ),
        (
            address("", ""),
            vended_s3(Some(vended_endpoint), Some(vended_region), None),
            vended_endpoint,
            vended_region,
        ),
        (
            address(connection_endpoint, ""),
            vended_s3(Some(vended_endpoint), Some(vended_region), None),
            connection_endpoint,
            vended_region,
        ),
        (
            address("", connection_region),
            vended_s3(Some(vended_endpoint), Some(vended_region), None),
            vended_endpoint,
            connection_region,
        ),
        (
            address(connection_endpoint, connection_region),
            vended_s3(None, None, None),
            connection_endpoint,
            connection_region,
        ),
        (address("", ""), vended_s3(None, None, None), "", ""),
    ] {
        let props = s3_payload(
            s3_backend(vended, S3_LOCATION, false, &connection)
                .expect("every address combination resolves a backend"),
        );
        assert_eq!(
            props.endpoint, expected_endpoint,
            "endpoint resolved from the wrong source"
        );
        assert_eq!(
            props.region, expected_region,
            "region resolved from the wrong source"
        );
        assert_eq!(
            props.access_key, VENDED_AK,
            "credentials stay vended-only whatever the address does"
        );
        assert_eq!(props.secret_key, VENDED_SK);
    }
}

/// Scenario: A store address empty on both sides resolves (Databricks AWS vends no address)
#[test]
fn a_both_empty_store_address_resolves_rather_than_refusing() {
    let props = s3_payload(
        s3_backend(
            vended_s3(None, None, None),
            S3_LOCATION,
            false,
            &StaticStoreAddress::default(),
        )
        .expect("an undetermined store address is legal, not a plan-time failure"),
    );

    assert_eq!(props.endpoint, "", "no source placed the store");
    assert_eq!(props.region, "", "no source named a region");
    assert_eq!(props.access_key, VENDED_AK);
    assert_eq!(props.secret_key, VENDED_SK);
    assert!(
        !props.path_style,
        "with no endpoint to reach, path-style addressing is off"
    );
}

/// Scenario: A stated vended path_style wins; otherwise it follows whether an endpoint resolved
#[test]
fn path_style_composes_the_vended_override_with_the_resolved_endpoint() {
    let endpoint = "https://minio.invalid";

    for stated in [Some(true), Some(false), None] {
        for (connection, vended_endpoint, endpoint_resolved) in [
            (address("", ""), None, false),
            (address("", ""), Some(endpoint), true),
            (address(endpoint, ""), None, true),
            (address(endpoint, ""), Some(endpoint), true),
        ] {
            let props = s3_payload(
                s3_backend(
                    vended_s3(vended_endpoint, None, stated),
                    S3_LOCATION,
                    false,
                    &connection,
                )
                .expect("resolves a backend"),
            );
            let expected = stated.unwrap_or(endpoint_resolved);
            assert_eq!(
                props.path_style, expected,
                "stated={stated:?} with endpoint_resolved={endpoint_resolved} must yield \
                 path_style={expected}"
            );
        }
    }
}

#[test]
fn a_stated_connection_path_style_wins_over_the_vended_value() {
    let endpoint = "https://minio.invalid";

    let false_wins = s3_payload(
        s3_backend(
            vended_s3(Some(endpoint), None, Some(true)),
            S3_LOCATION,
            false,
            &address_with_path_style(endpoint, "", Some(false)),
        )
        .expect("resolves"),
    );
    assert!(
        !false_wins.path_style,
        "a CONNECTION-stated false must beat a vended true"
    );

    let true_wins = s3_payload(
        s3_backend(
            vended_s3(Some(endpoint), None, Some(false)),
            S3_LOCATION,
            false,
            &address_with_path_style(endpoint, "", Some(true)),
        )
        .expect("resolves"),
    );
    assert!(
        true_wins.path_style,
        "a CONNECTION-stated true must beat a vended false"
    );
}

#[test]
fn path_style_resolves_the_connection_then_the_vended_value_then_the_endpoint_derivation() {
    let endpoint = "https://minio.invalid";

    let stated_wins = s3_payload(
        s3_backend(
            vended_s3(Some(endpoint), None, Some(true)),
            S3_LOCATION,
            false,
            &address_with_path_style(endpoint, "", Some(false)),
        )
        .expect("resolves"),
    );
    assert!(
        !stated_wins.path_style,
        "step 1: the CONNECTION-stated value wins"
    );

    let vended_wins = s3_payload(
        s3_backend(
            vended_s3(Some(endpoint), None, Some(true)),
            S3_LOCATION,
            false,
            &address_with_path_style("", "", None),
        )
        .expect("resolves"),
    );
    assert!(
        vended_wins.path_style,
        "step 2: when the CONNECTION states nothing, the vended value wins"
    );

    let derivation = s3_payload(
        s3_backend(
            vended_s3(Some(endpoint), None, None),
            S3_LOCATION,
            false,
            &address_with_path_style("", "", None),
        )
        .expect("resolves"),
    );
    assert!(
        derivation.path_style,
        "step 3: when neither states a value, endpoint-presence derivation runs (endpoint \
         resolved, so true)"
    );

    let derivation_no_endpoint = s3_payload(
        s3_backend(
            vended_s3(None, None, None),
            S3_LOCATION,
            false,
            &address_with_path_style("", "", None),
        )
        .expect("resolves"),
    );
    assert!(
        !derivation_no_endpoint.path_style,
        "step 3: when neither states a value and no endpoint resolved, derivation yields false"
    );
}

#[test]
fn a_stated_path_style_resolves_independently_of_the_resolved_endpoint() {
    let stated_true_no_endpoint = s3_payload(
        s3_backend(
            vended_s3(None, None, None),
            S3_LOCATION,
            false,
            &address_with_path_style("", "", Some(true)),
        )
        .expect("resolves"),
    );
    assert!(
        stated_true_no_endpoint.path_style,
        "a stated true must resolve true even when no endpoint resolved"
    );

    let stated_false_no_endpoint = s3_payload(
        s3_backend(
            vended_s3(None, None, None),
            S3_LOCATION,
            false,
            &address_with_path_style("", "", Some(false)),
        )
        .expect("resolves"),
    );
    assert!(
        !stated_false_no_endpoint.path_style,
        "a stated false must resolve false even when no endpoint resolved"
    );
}

#[test]
fn a_stated_false_path_style_beside_a_resolved_endpoint_is_honoured() {
    let endpoint = "https://minio.invalid";

    let props = s3_payload(
        s3_backend(
            vended_s3(Some(endpoint), None, None),
            S3_LOCATION,
            false,
            &address_with_path_style(endpoint, "", Some(false)),
        )
        .expect("resolves"),
    );
    assert!(
        !props.path_style,
        "a CONNECTION-stated false must be honoured even when an endpoint resolved (the \
         derivation would have yielded true)"
    );
}
