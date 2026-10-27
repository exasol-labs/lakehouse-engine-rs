use super::*;
use crate::test_support::{Layer, layer};

#[test]
fn the_cause_walk_follows_io_payloads_where_source_stops() {
    let rustls = rustls::Error::InvalidMessage(rustls::InvalidMessage::InvalidContentType);
    let io = std::io::Error::other(std::io::Error::new(std::io::ErrorKind::InvalidData, rustls));
    let error = layer("error sending request", layer("client error (Connect)", io));

    let walked: Vec<String> = causes(&error).map(|cause| cause.to_string()).collect();

    assert_eq!(walked.len(), 5, "{walked:?}");
    assert!(causes(&error).last().unwrap().is::<rustls::Error>());
    assert!(
        std::iter::successors(Some(&error as &(dyn Error + 'static)), |&e| e.source())
            .all(|e| !e.is::<rustls::Error>()),
        "source() alone must not reach the rustls error, or this test proves nothing"
    );
}

#[test]
fn every_cause_is_appended_once_in_order() {
    let error = layer(
        "list failed, source: error sending request",
        layer(
            "error sending request",
            layer("dns error", Layer("Name or service not known", None)),
        ),
    );

    assert_eq!(
        error_chain(&error),
        "list failed, source: error sending request: dns error: Name or service not known"
    );
}

#[test]
fn a_plain_http_answer_to_a_tls_request_names_the_scheme() {
    let rustls = rustls::Error::InvalidMessage(rustls::InvalidMessage::InvalidContentType);
    let io = std::io::Error::other(std::io::Error::new(std::io::ErrorKind::InvalidData, rustls));
    let error = layer("error sending request", io);

    let text = error_chain(&error);

    assert!(
        text.ends_with(
            "; the server did not answer with TLS, so check whether the URL should use http://"
        ),
        "{text}"
    );
}

#[test]
fn a_certificate_failure_gets_no_scheme_hint() {
    let rustls = rustls::Error::InvalidCertificate(rustls::CertificateError::UnknownIssuer);
    let error = layer("error sending request", std::io::Error::other(rustls));

    let text = error_chain(&error);

    assert!(
        text.contains("UnknownIssuer") && !text.contains("http://"),
        "{text}"
    );
}
