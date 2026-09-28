//! One HTTP client shape for every catalog kind: bounded connect and request
//! timeouts, plus a bounded retry of transient failures.

use std::time::Duration;

const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const REQUEST_TIMEOUT: Duration = Duration::from_secs(120);
const RETRY_ATTEMPTS: u32 = 3;
const RETRY_BACKOFF: Duration = Duration::from_millis(250);
/// Caps a server's `Retry-After` so one throttled call cannot stall the whole request.
const RETRY_AFTER_CAP: Duration = Duration::from_secs(5);
const RETRYABLE_STATUSES: [u16; 4] = [429, 502, 503, 504];

pub(crate) fn build_client() -> reqwest::Client {
    reqwest::Client::builder()
        .connect_timeout(CONNECT_TIMEOUT)
        .timeout(REQUEST_TIMEOUT)
        .build()
        .expect("reqwest client with timeouts")
}

/// Retries a connect failure or a 429/502/503/504 with doubling backoff (or the
/// capped `Retry-After`). A request whose body cannot be re-sent runs once.
pub(crate) async fn execute_with_retry(
    client: &reqwest::Client,
    mut request: reqwest::Request,
) -> reqwest::Result<reqwest::Response> {
    let mut attempt = 1;
    loop {
        let Some(resend) = request.try_clone().filter(|_| attempt < RETRY_ATTEMPTS) else {
            return client.execute(request).await;
        };
        let outcome = client.execute(request).await;
        let delay = match &outcome {
            Err(err) if err.is_connect() => None,
            Ok(response) if RETRYABLE_STATUSES.contains(&response.status().as_u16()) => {
                retry_after(response)
            }
            _ => return outcome,
        };
        tokio::time::sleep(delay.unwrap_or(RETRY_BACKOFF * 2u32.pow(attempt - 1))).await;
        request = resend;
        attempt += 1;
    }
}

fn retry_after(response: &reqwest::Response) -> Option<Duration> {
    let secs: u64 = response
        .headers()
        .get(reqwest::header::RETRY_AFTER)?
        .to_str()
        .ok()?
        .trim()
        .parse()
        .ok()?;
    Some(Duration::from_secs(secs).min(RETRY_AFTER_CAP))
}

#[cfg(test)]
#[path = "http_tests.rs"]
mod tests;
