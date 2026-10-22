use std::error::Error;

/// HTTP client errors render only their outermost layer, such as `error sending request`; the DNS,
/// TLS, or connect cause is further down `source()`. A cause the text already shows is skipped.
pub fn error_chain(error: &(dyn Error + 'static)) -> String {
    let mut text = error.to_string();
    let mut tls_answer_missing = false;
    let mut cause = error.source();
    while let Some(source) = cause {
        let part = source.to_string();
        if !text.contains(&part) {
            text.push_str(": ");
            text.push_str(&part);
        }
        tls_answer_missing |= is_non_tls_answer(source);
        cause = source.source();
    }
    if tls_answer_missing {
        text.push_str(
            "; the server did not answer with TLS, so check whether the URL should use http://",
        );
    }
    text
}

/// rustls reports a plain-HTTP answer as `InvalidContentType`, nested in `io::Error`s whose
/// `source()` skips it.
fn is_non_tls_answer(error: &(dyn Error + 'static)) -> bool {
    let mut current = Some(error);
    while let Some(error) = current {
        if let Some(rustls::Error::InvalidMessage(rustls::InvalidMessage::InvalidContentType)) =
            error.downcast_ref::<rustls::Error>()
        {
            return true;
        }
        current = error
            .downcast_ref::<std::io::Error>()
            .and_then(|io| io.get_ref())
            .map(|inner| inner as &(dyn Error + 'static));
    }
    false
}

#[cfg(test)]
#[path = "error_chain_tests.rs"]
mod tests;
