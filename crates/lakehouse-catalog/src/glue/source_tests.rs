use super::*;

fn service(code: &str, message: &str) -> GlueFailure {
    GlueFailure::Service {
        code: code.to_string(),
        message: message.to_string(),
    }
}

/// Scenario: Service errors are classified by their error code, not their HTTP status
#[test]
fn a_service_error_is_classified_by_its_code_never_its_status() {
    let not_found = GlueFailure::NotFound {
        message: "Table orders not found.".to_string(),
    };
    let cases = [
        (
            Some("EntityNotFoundException"),
            "Table orders not found.",
            400,
            not_found,
        ),
        (
            Some("AccessDeniedException"),
            "denied",
            404,
            service("AccessDeniedException", "denied"),
        ),
        (
            Some("AccessDeniedException"),
            "denied",
            400,
            service("AccessDeniedException", "denied"),
        ),
        (
            Some("InvalidInputException"),
            "bad name",
            400,
            service("InvalidInputException", "bad name"),
        ),
        (
            None,
            "Service Unavailable",
            503,
            GlueFailure::Uncoded {
                status: 503,
                message: "Service Unavailable".to_string(),
            },
        ),
    ];

    for (code, message, status, expected) in cases {
        assert_eq!(
            classify_service_error(code, Some(message), status),
            expected,
            "{code:?} with HTTP {status}"
        );
    }
}

/// Scenario: A clock-skew signing failure names the clock
#[test]
fn a_signing_time_rejection_names_the_clock_never_the_credential() {
    for (code, message) in [
        ("RequestTimeTooSkewed", ""),
        ("RequestExpired", ""),
        (
            "InvalidSignatureException",
            "Signature expired: 20260101T000000Z is now earlier than 20260101T005500Z",
        ),
        ("InvalidSignatureException", "Signature not yet current: x"),
    ] {
        let failure = classify_service_error(Some(code), Some(message), 400);

        let text = failure.text("GetTables", "database 'sales'");
        assert!(
            matches!(failure, GlueFailure::SigningTime { .. }),
            "{code}: {failure:?}"
        );
        assert!(text.contains("clock differs from AWS time"), "{text}");
        assert!(
            !text.to_lowercase().contains("credential"),
            "a clock error must not read as a credential error: {text}"
        );
    }

    assert_eq!(
        classify_service_error(
            Some("InvalidSignatureException"),
            Some("The request signature we calculated does not match"),
            400,
        ),
        service(
            "InvalidSignatureException",
            "The request signature we calculated does not match"
        ),
        "a mismatched signature is not a clock error"
    );
}
