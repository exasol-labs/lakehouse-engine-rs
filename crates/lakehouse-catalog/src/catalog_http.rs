//! The one HTTP path of every catalog request. It owns the connect timeout, the retries of a
//! transient failure, and the bound on the whole request, so each caller keeps only its own error
//! text and redaction.

use crate::error_chain::{causes, error_chain};
use aws_smithy_types::date_time::{DateTime, Format};
use reqwest::header::{HeaderMap, RETRY_AFTER};
use reqwest::{IntoUrl, Request, RequestBuilder, Response, StatusCode};
use std::error::Error;
use std::hash::{BuildHasher, RandomState};
use std::io::ErrorKind;
use std::time::{Duration, SystemTime};
use tokio::time::{Instant, sleep, timeout_at};

/// How hard one catalog request tries before it fails.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct RetryPolicy {
    /// Attempts in total, the first one included.
    pub(crate) attempts: u32,
    /// The longest one connection attempt may take.
    pub(crate) connect_timeout: Duration,
    /// The longest the whole request may take: every attempt, wait, and answer-body read.
    pub(crate) bound: Duration,
    /// The upper end of the first backoff wait, doubled for each later retry.
    pub(crate) backoff_base: Duration,
}

/// The attempts and the bound match the Glue native client (`glue/sdk.rs`).
const CATALOG_POLICY: RetryPolicy = RetryPolicy {
    attempts: 5,
    connect_timeout: Duration::from_secs(10),
    bound: Duration::from_secs(30),
    backoff_base: Duration::from_millis(250),
};

/// The HTTP client of one catalog session. Callers build a request with [`Self::get`] or
/// [`Self::post`] and send it only through [`Self::send`] or [`Self::send_capped`], so every
/// catalog request gets the same connect timeout, retries, and bound. A retry re-sends a clone of
/// the request as the caller built and signed it: the bound ends long before a SigV4 signature or
/// a cached OAuth token expires, so this type needs no knowledge of auth or signing.
#[derive(Clone)]
pub(crate) struct CatalogHttp {
    client: reqwest::Client,
    policy: RetryPolicy,
}

/// A complete answer of any HTTP status; the caller decides what the status means.
#[derive(Debug)]
pub(crate) struct CatalogAnswer {
    pub(crate) status: StatusCode,
    headers: HeaderMap,
    pub(crate) body: Vec<u8>,
    /// The body exceeded the caller's byte cap, and `body` holds only its first bytes.
    pub(crate) cut: bool,
}

/// A catalog request that ended without a complete answer. [`Self::render`] gives its text.
#[derive(Debug)]
pub(crate) struct CatalogHttpError(Failure);

#[derive(Debug)]
enum Failure {
    BoundSpent(Duration),
    /// `status` is set when the answer's headers arrived and the read of its body broke off.
    Transport {
        error: reqwest::Error,
        status: Option<StatusCode>,
    },
}

impl CatalogHttp {
    /// The client of every catalog session, with 5 attempts, a 10-second connect timeout, a
    /// 30-second bound, and a 250 ms backoff base.
    pub(crate) fn new() -> Self {
        Self::with_policy(CATALOG_POLICY)
    }

    /// Panics where `reqwest::Client::new` panics: when the TLS backend cannot start.
    pub(crate) fn with_policy(policy: RetryPolicy) -> Self {
        let client = reqwest::Client::builder()
            .connect_timeout(policy.connect_timeout)
            .build()
            .expect("the TLS backend of the catalog HTTP client starts");
        Self { client, policy }
    }

    pub(crate) fn get(&self, url: impl IntoUrl) -> RequestBuilder {
        self.client.get(url)
    }

    pub(crate) fn post(&self, url: impl IntoUrl) -> RequestBuilder {
        self.client.post(url)
    }

    /// Sends `request` and reads its whole answer body, retrying a transient failure, all within
    /// the policy's bound. Every HTTP answer is `Ok`, the last one when the retries run out; only a
    /// transport failure or the spent bound is an error. A request whose body cannot be cloned is
    /// sent once.
    pub(crate) async fn send(&self, request: Request) -> Result<CatalogAnswer, CatalogHttpError> {
        self.send_within_bound(request, usize::MAX).await
    }

    /// [`Self::send`], keeping at most `max_bytes` of the answer body; [`CatalogAnswer::cut`]
    /// reports a cut.
    pub(crate) async fn send_capped(
        &self,
        request: Request,
        max_bytes: usize,
    ) -> Result<CatalogAnswer, CatalogHttpError> {
        self.send_within_bound(request, max_bytes).await
    }

    async fn send_within_bound(
        &self,
        mut request: Request,
        max_bytes: usize,
    ) -> Result<CatalogAnswer, CatalogHttpError> {
        let deadline = Instant::now() + self.policy.bound;
        let mut attempts_made = 0;
        loop {
            attempts_made += 1;
            let resend = (attempts_made < self.policy.attempts)
                .then(|| request.try_clone())
                .flatten();
            let outcome = self.attempt(request, max_bytes, deadline).await;
            let Some(resend) = resend else {
                return outcome;
            };
            let Some(wait) = self.wait_before_retry(attempts_made, &outcome, deadline) else {
                return outcome;
            };
            sleep(wait).await;
            request = resend;
        }
    }

    async fn attempt(
        &self,
        request: Request,
        max_bytes: usize,
        deadline: Instant,
    ) -> Result<CatalogAnswer, CatalogHttpError> {
        timeout_at(deadline, self.exchange(request, max_bytes))
            .await
            .unwrap_or_else(|_| Err(CatalogHttpError(Failure::BoundSpent(self.policy.bound))))
    }

    async fn exchange(
        &self,
        request: Request,
        max_bytes: usize,
    ) -> Result<CatalogAnswer, CatalogHttpError> {
        let mut response = self
            .client
            .execute(request)
            .await
            .map_err(|error| CatalogHttpError::transport(error, None))?;
        let status = response.status();
        let headers = std::mem::take(response.headers_mut());
        let (body, cut) = read_body(&mut response, max_bytes)
            .await
            .map_err(|error| CatalogHttpError::transport(error, Some(status)))?;
        Ok(CatalogAnswer {
            status,
            headers,
            body,
            cut,
        })
    }

    fn wait_before_retry(
        &self,
        retry: u32,
        outcome: &Result<CatalogAnswer, CatalogHttpError>,
        deadline: Instant,
    ) -> Option<Duration> {
        let requested = match outcome {
            Ok(answer) if is_transient_status(answer.status) => stated_wait(&answer.headers),
            Err(error) if error.is_transient() => None,
            _ => return None,
        };
        let backoff = backoff_wait(self.policy.backoff_base, retry, random_jitter());
        chosen_wait(
            requested,
            backoff,
            deadline.saturating_duration_since(Instant::now()),
        )
    }
}

impl CatalogHttpError {
    fn transport(error: reqwest::Error, status: Option<StatusCode>) -> Self {
        Self(Failure::Transport { error, status })
    }

    /// The status of an answer whose body read broke off, so a caller can still report it.
    pub(crate) fn status(&self) -> Option<StatusCode> {
        match &self.0 {
            Failure::Transport { status, .. } => *status,
            Failure::BoundSpent(_) => None,
        }
    }

    /// The spent bound's sentence, or else the whole error chain passed through `redact`,
    /// preceded by the answer's HTTP status when its body broke off.
    pub(crate) fn render(&self, redact: impl Fn(&str) -> String) -> String {
        match &self.0 {
            Failure::BoundSpent(bound) => format!(
                "the catalog sent no complete answer within {} seconds, retries included",
                bound.as_secs_f64()
            ),
            Failure::Transport {
                error,
                status: Some(status),
            } => format!(
                "the catalog's HTTP {} answer broke off: {}",
                status.as_u16(),
                redact(&error_chain(error))
            ),
            Failure::Transport {
                error,
                status: None,
            } => redact(&error_chain(error)),
        }
    }

    fn is_transient(&self) -> bool {
        matches!(
            &self.0,
            Failure::Transport { error, status: None } if is_transient_transport_failure(error)
        )
    }
}

async fn read_body(
    response: &mut Response,
    max_bytes: usize,
) -> Result<(Vec<u8>, bool), reqwest::Error> {
    let mut body = Vec::new();
    while let Some(chunk) = response.chunk().await? {
        let room = max_bytes - body.len();
        if chunk.len() > room {
            body.extend_from_slice(&chunk[..room]);
            return Ok((body, true));
        }
        body.extend_from_slice(&chunk);
    }
    Ok((body, false))
}

fn stated_wait(headers: &HeaderMap) -> Option<Duration> {
    let value = headers.get(RETRY_AFTER)?.to_str().ok()?;
    retry_after_wait(value, SystemTime::now())
}

/// `RandomState` keys differ per instance, so hashing nothing still yields fresh bits.
fn random_jitter() -> u32 {
    (RandomState::new().hash_one(()) >> 32) as u32
}

/// 408, 429, 502, 503, and 504: the catalog or a proxy in front of it is briefly unable to answer.
fn is_transient_status(status: StatusCode) -> bool {
    matches!(status.as_u16(), 408 | 429 | 502 | 503 | 504)
}

/// The wait a `Retry-After` value asks for (RFC 9110 § 10.2.3), or `None` when it is neither
/// delay-seconds nor an IMF-fixdate. A past date asks for no wait.
fn retry_after_wait(value: &str, now: SystemTime) -> Option<Duration> {
    let value = value.trim();
    if !value.is_empty() && value.bytes().all(|byte| byte.is_ascii_digit()) {
        return Some(value.parse().map_or(Duration::MAX, Duration::from_secs));
    }
    // aws-smithy-types reads only the IMF-fixdate form, a scoped deviation of the spec delta.
    let date = DateTime::from_str(value, Format::HttpDate).ok()?;
    let stated = SystemTime::try_from(date).ok()?;
    Some(stated.duration_since(now).unwrap_or(Duration::ZERO))
}

/// A wait in [b/2, b] with b = `base` × 2^(`retry` − 1). `jitter` places it in that range: 0 at
/// the lower end, `u32::MAX` at the upper end.
fn backoff_wait(base: Duration, retry: u32, jitter: u32) -> Duration {
    let ceiling = base.saturating_mul(2u32.saturating_pow(retry.saturating_sub(1)));
    let floor = ceiling / 2;
    let spread = (ceiling - floor).as_nanos() * u128::from(jitter) / u128::from(u32::MAX);
    floor.saturating_add(Duration::from_nanos(
        u64::try_from(spread).unwrap_or(u64::MAX),
    ))
}

/// The catalog's own wait when it stated one, else `backoff`; `None` when that wait would leave
/// no time for the next attempt.
fn chosen_wait(
    requested: Option<Duration>,
    backoff: Duration,
    time_left: Duration,
) -> Option<Duration> {
    let wait = requested.unwrap_or(backoff);
    (wait < time_left).then_some(wait)
}

/// A connection that never opened in time or that the peer cut, with no TLS library error: a new
/// connection may succeed. DNS and TLS failures are final. Reads typed causes, never message text.
fn is_transient_transport_failure(error: &reqwest::Error) -> bool {
    (error.is_connect() && error.is_timeout()) || is_transient_cause_chain(error)
}

fn is_transient_cause_chain(error: &(dyn Error + 'static)) -> bool {
    let mut cut_connection = false;
    for cause in causes(error) {
        if cause.is::<rustls::Error>() {
            return false;
        }
        cut_connection |= cause.downcast_ref::<std::io::Error>().is_some_and(|io| {
            matches!(
                io.kind(),
                ErrorKind::ConnectionRefused
                    | ErrorKind::ConnectionReset
                    | ErrorKind::ConnectionAborted
                    | ErrorKind::UnexpectedEof
            )
        });
    }
    cut_connection
}

#[cfg(test)]
#[path = "catalog_http_tests.rs"]
mod tests;
