use std::error::Error;

use http::Uri;
use hyper_util::client::proxy::matcher::Matcher;

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
    if let Some(proxy) =
        failed_connect_url(error).and_then(|url| proxy_for(&Matcher::from_env(), &url))
    {
        text.push_str(&format!("; the connection went through the proxy {proxy}"));
    }
    text
}

fn failed_connect_url(error: &(dyn Error + 'static)) -> Option<reqwest::Url> {
    causes(error)
        .filter_map(|cause| cause.downcast_ref::<reqwest::Error>())
        .find(|request| request.is_connect())
        .and_then(|request| request.url().cloned())
}

/// The proxy that `matcher` picks for `url`, without its credentials.
fn proxy_for(matcher: &Matcher, url: &reqwest::Url) -> Option<String> {
    let intercept = matcher.intercept(&url.as_str().parse::<Uri>().ok()?)?;
    let proxy = intercept.uri();
    let scheme = proxy.scheme_str().unwrap_or("http");
    let host = proxy.host()?;
    Some(match proxy.port_u16() {
        Some(port) => format!("{scheme}://{host}:{port}"),
        None => format!("{scheme}://{host}"),
    })
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
