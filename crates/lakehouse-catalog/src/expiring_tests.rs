use super::*;
use crate::test_support::clock_reading;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

const SKEW: Duration = Duration::from_secs(60);
const EXPIRES_AT_MS: u64 = 120_000;

fn lease(value: &str, expires_at_ms: Option<u64>) -> Lease<String> {
    Lease {
        value: value.to_string(),
        expires_at_ms,
    }
}

async fn renew_to(value: &str, renewals: &AtomicUsize) -> Result<Lease<String>, String> {
    renewals.fetch_add(1, Ordering::SeqCst);
    Ok(lease(value, None))
}

#[tokio::test]
async fn an_empty_holder_renews_at_its_first_get() {
    let holder = ExpiringCredential::new(SKEW, clock_reading(&Arc::new(AtomicU64::new(0))));
    let renewals = AtomicUsize::new(0);

    let value = holder.get(|_| renew_to("first", &renewals)).await;

    assert_eq!(value, Ok("first".to_string()));
    assert_eq!(renewals.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn a_lease_is_renewed_once_it_is_within_the_skew_of_its_expiry() {
    for (now_ms, renewed) in [
        (0, false),
        (EXPIRES_AT_MS - SKEW.as_millis() as u64 - 1, false),
        (EXPIRES_AT_MS - SKEW.as_millis() as u64, true),
        (EXPIRES_AT_MS + 1, true),
    ] {
        let time = Arc::new(AtomicU64::new(now_ms));
        let holder = ExpiringCredential::seeded(
            lease("seed", Some(EXPIRES_AT_MS)),
            SKEW,
            clock_reading(&time),
        );
        let renewals = AtomicUsize::new(0);

        let value = holder.get(|_| renew_to("renewed", &renewals)).await;

        let expected = if renewed { "renewed" } else { "seed" };
        assert_eq!(value, Ok(expected.to_string()), "at {now_ms} ms");
        assert_eq!(
            renewals.load(Ordering::SeqCst),
            usize::from(renewed),
            "at {now_ms} ms"
        );
    }
}

#[tokio::test]
async fn a_lease_without_an_expiry_is_never_renewed() {
    let time = Arc::new(AtomicU64::new(u64::MAX));
    let holder = ExpiringCredential::seeded(lease("forever", None), SKEW, clock_reading(&time));
    let renewals = AtomicUsize::new(0);

    let value = holder.get(|_| renew_to("renewed", &renewals)).await;

    assert_eq!(value, Ok("forever".to_string()));
    assert_eq!(renewals.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn concurrent_callers_at_the_renewal_point_share_one_renewal() {
    let time = Arc::new(AtomicU64::new(EXPIRES_AT_MS));
    let holder = ExpiringCredential::seeded(
        lease("stale", Some(EXPIRES_AT_MS)),
        SKEW,
        clock_reading(&time),
    );
    let renewals = AtomicUsize::new(0);
    let renew = |_| async {
        renewals.fetch_add(1, Ordering::SeqCst);
        tokio::task::yield_now().await;
        Ok::<_, String>(lease("fresh", Some(EXPIRES_AT_MS * 10)))
    };

    let (first, second) = tokio::join!(holder.get(renew), holder.get(renew));

    assert_eq!(renewals.load(Ordering::SeqCst), 1);
    assert_eq!(first, Ok("fresh".to_string()));
    assert_eq!(second, Ok("fresh".to_string()));
}

#[tokio::test]
async fn renew_gets_the_time_the_holder_read() {
    let time = Arc::new(AtomicU64::new(1_234_567));
    let holder = ExpiringCredential::<String>::new(SKEW, clock_reading(&time));

    let value = holder
        .get(|now_ms| async move { Ok::<_, String>(lease(&now_ms.to_string(), None)) })
        .await;

    assert_eq!(value, Ok("1234567".to_string()));
}

#[tokio::test]
async fn a_failed_renewal_keeps_the_held_lease_and_returns_the_error() {
    let time = Arc::new(AtomicU64::new(EXPIRES_AT_MS));
    let holder = ExpiringCredential::seeded(
        lease("seed", Some(EXPIRES_AT_MS)),
        SKEW,
        clock_reading(&time),
    );

    let failed = holder
        .get(|_| async { Err::<Lease<String>, _>("grant refused".to_string()) })
        .await;
    let renewals = AtomicUsize::new(0);
    let retried = holder.get(|_| renew_to("renewed", &renewals)).await;

    assert_eq!(failed, Err("grant refused".to_string()));
    assert_eq!(retried, Ok("renewed".to_string()));
    assert_eq!(holder.held_values(), ["seed", "renewed"]);
}

#[tokio::test]
async fn held_values_lists_every_value_held_oldest_first() {
    let time = Arc::new(AtomicU64::new(0));
    let holder = ExpiringCredential::seeded(lease("seed", Some(0)), SKEW, clock_reading(&time));
    let renewals = AtomicUsize::new(0);

    holder
        .get(|_| async { Ok::<_, String>(lease("second", Some(0))) })
        .await
        .unwrap();
    holder.get(|_| renew_to("third", &renewals)).await.unwrap();
    holder.get(|_| renew_to("unused", &renewals)).await.unwrap();

    assert_eq!(holder.held_values(), ["seed", "second", "third"]);
    assert_eq!(renewals.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn a_due_lease_that_the_decision_keeps_is_returned_without_a_renewal() {
    let time = Arc::new(AtomicU64::new(EXPIRES_AT_MS));
    let holder = ExpiringCredential::seeded(
        lease("seed", Some(EXPIRES_AT_MS)),
        SKEW,
        clock_reading(&time),
    );
    let renewals = AtomicUsize::new(0);

    let value = holder
        .get_or_keep(
            |_| Ok::<_, String>(AtRenewalPoint::Keep),
            |_| renew_to("renewed", &renewals),
        )
        .await;

    assert_eq!(value, Ok("seed".to_string()));
    assert_eq!(renewals.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn a_decision_that_fails_returns_its_error_and_keeps_the_lease() {
    let time = Arc::new(AtomicU64::new(EXPIRES_AT_MS));
    let holder = ExpiringCredential::seeded(
        lease("seed", Some(EXPIRES_AT_MS)),
        SKEW,
        clock_reading(&time),
    );
    let renewals = AtomicUsize::new(0);

    let failed = holder
        .get_or_keep(
            |_| Err::<AtRenewalPoint, _>("expired".to_string()),
            |_| renew_to("renewed", &renewals),
        )
        .await;

    assert_eq!(failed, Err("expired".to_string()));
    assert_eq!(renewals.load(Ordering::SeqCst), 0);
    assert_eq!(holder.held_values(), ["seed"]);
}

#[tokio::test]
async fn the_decision_sees_the_expiry_the_time_and_the_last_renewal() {
    let time = Arc::new(AtomicU64::new(EXPIRES_AT_MS));
    let holder = ExpiringCredential::seeded(
        lease("seed", Some(EXPIRES_AT_MS)),
        SKEW,
        clock_reading(&time),
    );
    let seen = Mutex::new(Vec::new());
    let renew_until =
        |expires_at_ms| async move { Ok::<_, String>(lease("next", Some(expires_at_ms))) };

    for now_ms in [EXPIRES_AT_MS, EXPIRES_AT_MS + 5] {
        time.store(now_ms, Ordering::SeqCst);
        holder
            .get_or_keep(
                |point| {
                    seen.lock().unwrap().push(point);
                    Ok(AtRenewalPoint::Renew)
                },
                |_| renew_until(EXPIRES_AT_MS + 10),
            )
            .await
            .unwrap();
    }

    assert_eq!(
        *seen.lock().unwrap(),
        [
            RenewalPoint {
                expires_at_ms: EXPIRES_AT_MS,
                renewed_at_ms: None,
                now_ms: EXPIRES_AT_MS,
            },
            RenewalPoint {
                expires_at_ms: EXPIRES_AT_MS + 10,
                renewed_at_ms: Some(EXPIRES_AT_MS),
                now_ms: EXPIRES_AT_MS + 5,
            },
        ]
    );
}

#[tokio::test]
async fn the_decision_is_asked_only_about_a_held_lease_at_its_renewal_point() {
    let time = Arc::new(AtomicU64::new(0));
    let empty = ExpiringCredential::<String>::new(SKEW, clock_reading(&time));
    let fresh = ExpiringCredential::seeded(
        lease("fresh", Some(EXPIRES_AT_MS)),
        SKEW,
        clock_reading(&time),
    );
    let renewals = AtomicUsize::new(0);
    let refuse = |_| Err::<AtRenewalPoint, _>("asked".to_string());

    let renewed = empty
        .get_or_keep(refuse, |_| renew_to("first", &renewals))
        .await;
    let kept = fresh
        .get_or_keep(refuse, |_| renew_to("unused", &renewals))
        .await;

    assert_eq!(renewed, Ok("first".to_string()));
    assert_eq!(kept, Ok("fresh".to_string()));
    assert_eq!(renewals.load(Ordering::SeqCst), 1);
}

#[test]
fn the_system_clock_reads_epoch_milliseconds() {
    let before = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis();

    let read = u128::from(system_clock()());

    let after = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis();
    assert!(
        before <= read && read <= after,
        "{before} <= {read} <= {after}"
    );
}
