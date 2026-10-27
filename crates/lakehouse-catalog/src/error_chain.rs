use std::error::Error;

/// HTTP client errors render only their outermost layer, such as `error sending request`; the DNS,
/// TLS, or connect cause is further down `source()`. A cause the text already shows is skipped.
pub fn error_chain(error: &(dyn Error + 'static)) -> String {
    let mut text = error.to_string();
    let mut cause = error.source();
    while let Some(source) = cause {
        let part = source.to_string();
        if !text.contains(&part) {
            text.push_str(": ");
            text.push_str(&part);
        }
        cause = source.source();
    }
    if is_non_tls_answer(error) {
        text.push_str(
            "; the server did not answer with TLS, so check whether the URL should use http://",
        );
    }
    text
}

/// rustls reports a plain-HTTP answer as `InvalidContentType`.
fn is_non_tls_answer(error: &(dyn Error + 'static)) -> bool {
    causes(error).any(|cause| {
        matches!(
            cause.downcast_ref::<rustls::Error>(),
            Some(rustls::Error::InvalidMessage(
                rustls::InvalidMessage::InvalidContentType
            ))
        )
    })
}

/// `error` and every cause below it. At an `io::Error` the walk enters its payload, because
/// rustls and the TLS connector nest errors there and `io::Error::source()` skips the payload.
pub(crate) fn causes<'a>(
    error: &'a (dyn Error + 'static),
) -> impl Iterator<Item = &'a (dyn Error + 'static)> {
    std::iter::successors(Some(error), |&current| {
        match current.downcast_ref::<std::io::Error>() {
            Some(io) => io.get_ref().map(|inner| inner as &(dyn Error + 'static)),
            None => current.source(),
        }
    })
}

#[cfg(test)]
#[path = "error_chain_tests.rs"]
mod tests;
