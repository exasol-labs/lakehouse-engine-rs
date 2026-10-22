use super::*;
use std::fmt;

#[derive(Debug)]
struct Layer(&'static str, Option<Box<dyn Error + Send + Sync>>);

impl fmt::Display for Layer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.0)
    }
}

impl Error for Layer {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        self.1.as_deref().map(|e| e as &(dyn Error + 'static))
    }
}

fn layer(text: &'static str, cause: impl Error + Send + Sync + 'static) -> Layer {
    Layer(text, Some(Box::new(cause)))
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
