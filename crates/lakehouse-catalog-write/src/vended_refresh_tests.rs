use super::*;
use crate::test_support::{PREFIX_CONFIG, STAGED_LOCATION, assert_free_of, catalog, session};
use lakehouse_catalog::exported_test_support::clock_reading;
use reqsign_core::Context;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

const LOAD_CREDENTIALS: &str = include_str!("testdata/load_credentials_staged.json");
const REFRESHED_EXPIRES_AT_MS: u64 = 1_794_213_322_000;
const FIXTURE_PLACEHOLDERS: [&str; 3] = [
    "REFRESHED_ACCESS_KEY_ID_PLACEHOLDER",
    "REFRESHED_SECRET_ACCESS_KEY_PLACEHOLDER",
    "REFRESHED_SESSION_TOKEN_PLACEHOLDER",
];
const SEED_SECRETS: [&str; 3] = ["SEED_ACCESS_KEY", "SEED_SECRET_KEY", "SEED_SESSION_TOKEN"];
const SEED_EXPIRES_AT_MS: u64 = 1_700_000_000_000;
const MINUTE_MS: u64 = 60_000;
const CREDENTIALS_REQUEST: &str = "GET /v1/p/namespaces/ns/tables/staged/credentials ";
const OFFERING_CONFIG: &str = r#"{"defaults":{"prefix":"p"},"endpoints":["GET /v1/{prefix}/namespaces/{namespace}/tables/{table}/credentials"]}"#;

fn fixture_answer(_: &str) -> (u16, String) {
    (200, LOAD_CREDENTIALS.to_string())
}

/// [`LOAD_CREDENTIALS`] with its expiry moved to `expires_at_ms`.
fn fixture_expiring_at(expires_at_ms: u64) -> String {
    LOAD_CREDENTIALS.replace(
        &REFRESHED_EXPIRES_AT_MS.to_string(),
        &expires_at_ms.to_string(),
    )
}

fn credential_requests(requests: &Mutex<Vec<String>>) -> usize {
    requests
        .lock()
        .unwrap()
        .iter()
        .filter(|request| request.starts_with(CREDENTIALS_REQUEST))
        .count()
}

fn seed(expires_at_ms: Option<u64>) -> VendedS3Credentials {
    VendedS3Credentials {
        access_key_id: SEED_SECRETS[0].into(),
        secret_access_key: SEED_SECRETS[1].into(),
        session_token: Some(SEED_SECRETS[2].into()),
        endpoint: Some("https://storage.example".into()),
        expires_at_ms,
    }
}

async fn staged_table(uri: &str, location: &str, allow_http: bool) -> VendedTable {
    VendedTable {
        session: Arc::new(session(uri, true).await),
        table: TableIdent::from_strs(["ns", "staged"]).unwrap(),
        location: location.to_string(),
        allow_http,
    }
}

async fn source_at(
    uri: &str,
    seed: VendedS3Credentials,
    time: &Arc<AtomicU64>,
) -> VendedCredentials {
    VendedCredentials::seeded(
        staged_table(uri, STAGED_LOCATION, true).await,
        seed,
        clock_reading(time),
    )
    .expect("a seeded source")
}

async fn provide(source: &VendedCredentials) -> Result<AwsCredential, String> {
    source
        .provide_credential(&Context::new())
        .await
        .map(|credential| credential.expect("a credential"))
        .map_err(|error| error.to_string())
}

/// Scenario: A writer that outlives its vended credentials refreshes them, or fails naming their expiry
#[test]
fn the_decision_keeps_renews_or_fails_by_expiry_endpoint_and_last_renewal() {
    let expires_at = 10 * MINUTE_MS;
    let point = |renewed_at_ms, now_ms| RenewalPoint {
        expires_at_ms: expires_at,
        renewed_at_ms,
        now_ms,
    };
    let cases = [
        (
            point(None, expires_at - 5 * MINUTE_MS),
            true,
            RefreshDecision::Renew,
        ),
        (point(None, expires_at + 1), true, RefreshDecision::Renew),
        (
            point(Some(expires_at - 30_000), expires_at),
            true,
            RefreshDecision::Renew,
        ),
        (
            point(Some(expires_at - 29_999), expires_at),
            true,
            RefreshDecision::Keep,
        ),
        (point(None, expires_at - 1), false, RefreshDecision::Keep),
        (point(None, expires_at), false, RefreshDecision::Expired),
        (
            point(Some(0), expires_at + 1),
            false,
            RefreshDecision::Expired,
        ),
    ];

    for (point, offered, expected) in cases {
        assert_eq!(
            refresh_decision(point, offered),
            expected,
            "{point:?}, endpoint offered {offered}"
        );
    }
}

/// Scenario: A writer that outlives its vended credentials refreshes them, or fails naming their expiry
#[tokio::test]
async fn credentials_within_five_minutes_of_expiry_are_renewed_through_one_load_credentials_request()
 {
    let (uri, requests) = catalog(OFFERING_CONFIG, fixture_answer).await;
    let time = Arc::new(AtomicU64::new(SEED_EXPIRES_AT_MS - 5 * MINUTE_MS - 1));
    let source = source_at(&uri, seed(Some(SEED_EXPIRES_AT_MS)), &time).await;

    let early = provide(&source).await.unwrap();
    time.store(SEED_EXPIRES_AT_MS - 4 * MINUTE_MS, Ordering::SeqCst);
    let renewed = provide(&source).await.unwrap();
    let again = provide(&source).await.unwrap();

    assert_eq!(early.access_key_id, SEED_SECRETS[0]);
    assert_eq!(renewed.access_key_id, FIXTURE_PLACEHOLDERS[0]);
    assert_eq!(again.access_key_id, FIXTURE_PLACEHOLDERS[0]);
    assert_eq!(credential_requests(&requests), 1);
}

#[tokio::test]
async fn a_load_credentials_answer_yields_the_covering_credentials_and_their_expiry() {
    let (uri, _) = catalog(OFFERING_CONFIG, fixture_answer).await;
    let time = Arc::new(AtomicU64::new(SEED_EXPIRES_AT_MS));
    let source = source_at(&uri, seed(Some(SEED_EXPIRES_AT_MS)), &time).await;

    let renewed = provide(&source).await.unwrap();

    assert_eq!(renewed.access_key_id, FIXTURE_PLACEHOLDERS[0]);
    assert_eq!(renewed.secret_access_key, FIXTURE_PLACEHOLDERS[1]);
    assert_eq!(
        renewed.session_token.as_deref(),
        Some(FIXTURE_PLACEHOLDERS[2])
    );
    assert_eq!(
        renewed.expires_in,
        Some(Timestamp::from_millisecond(REFRESHED_EXPIRES_AT_MS as i64).unwrap())
    );
}

#[tokio::test]
async fn the_seed_carries_its_expiry_and_never_expires_without_one() {
    let (uri, requests) = catalog(OFFERING_CONFIG, fixture_answer).await;
    let time = Arc::new(AtomicU64::new(u64::MAX / 2));
    let lasting = source_at(&uri, seed(None), &time).await;
    time.store(0, Ordering::SeqCst);
    let expiring = source_at(&uri, seed(Some(SEED_EXPIRES_AT_MS)), &time).await;

    time.store(u64::MAX / 2, Ordering::SeqCst);
    let kept = provide(&lasting).await.unwrap();
    time.store(0, Ordering::SeqCst);
    let seeded = provide(&expiring).await.unwrap();

    assert_eq!(kept.access_key_id, SEED_SECRETS[0]);
    assert_eq!(kept.expires_in, None);
    assert_eq!(
        seeded.expires_in,
        Some(Timestamp::from_millisecond(SEED_EXPIRES_AT_MS as i64).unwrap())
    );
    assert_eq!(credential_requests(&requests), 0);
}

#[tokio::test]
async fn concurrent_callers_at_the_renewal_point_share_one_renewal() {
    let (uri, requests) = catalog(OFFERING_CONFIG, fixture_answer).await;
    let time = Arc::new(AtomicU64::new(SEED_EXPIRES_AT_MS));
    let source = source_at(&uri, seed(Some(SEED_EXPIRES_AT_MS)), &time).await;

    let (first, second) = tokio::join!(provide(&source), provide(&source));

    assert_eq!(first.unwrap().access_key_id, FIXTURE_PLACEHOLDERS[0]);
    assert_eq!(second.unwrap().access_key_id, FIXTURE_PLACEHOLDERS[0]);
    assert_eq!(credential_requests(&requests), 1);
}

#[tokio::test]
async fn credentials_renewed_close_to_their_expiry_are_renewed_again_only_after_30_seconds() {
    let now = SEED_EXPIRES_AT_MS;
    let (uri, requests) = catalog(OFFERING_CONFIG, move |_| {
        (200, fixture_expiring_at(now + MINUTE_MS))
    })
    .await;
    let time = Arc::new(AtomicU64::new(now));
    let source = source_at(&uri, seed(Some(SEED_EXPIRES_AT_MS)), &time).await;

    provide(&source).await.unwrap();
    time.store(now + 29_999, Ordering::SeqCst);
    provide(&source).await.unwrap();
    let within_pause = credential_requests(&requests);
    time.store(now + 30_000, Ordering::SeqCst);
    provide(&source).await.unwrap();

    assert_eq!(within_pause, 1);
    assert_eq!(credential_requests(&requests), 2);
}

/// Scenario: A writer that outlives its vended credentials refreshes them, or fails naming their expiry
#[tokio::test]
async fn expired_credentials_without_a_credentials_endpoint_fail_naming_the_table_and_the_expiry() {
    let (uri, requests) = catalog(PREFIX_CONFIG, fixture_answer).await;
    let time = Arc::new(AtomicU64::new(SEED_EXPIRES_AT_MS - MINUTE_MS));
    let source = source_at(&uri, seed(Some(SEED_EXPIRES_AT_MS)), &time).await;

    let before_expiry = provide(&source).await;
    time.store(SEED_EXPIRES_AT_MS, Ordering::SeqCst);
    let expired = provide(&source).await.unwrap_err();

    assert_eq!(before_expiry.unwrap().access_key_id, SEED_SECRETS[0]);
    assert_eq!(
        expired,
        format!(
            "the vended storage credentials of table ns.staged expired at 2023-11-14T22:13:20Z, \
             and the catalog {uri} offers no credentials endpoint to refresh them"
        )
    );
    assert_free_of(&expired, &SEED_SECRETS);
    assert_eq!(credential_requests(&requests), 0);
}

#[tokio::test]
async fn a_refused_load_credentials_request_fails_naming_the_table_and_the_catalog() {
    let (uri, _) = catalog(OFFERING_CONFIG, |_| {
        (403, "{\"error\":\"forbidden\"}".into())
    })
    .await;
    let time = Arc::new(AtomicU64::new(SEED_EXPIRES_AT_MS));
    let source = source_at(&uri, seed(Some(SEED_EXPIRES_AT_MS)), &time).await;

    let failed = provide(&source).await.unwrap_err();

    assert!(
        failed.starts_with(&format!(
            "could not refresh the vended storage credentials of table ns.staged through the \
             catalog {uri}: "
        )),
        "{failed}"
    );
    assert!(failed.contains("HTTP 403"), "{failed}");
    assert_free_of(&failed, &SEED_SECRETS);
}

#[tokio::test]
async fn credentials_that_cover_another_location_are_refused_naming_the_location() {
    let (uri, _) = catalog(OFFERING_CONFIG, fixture_answer).await;
    let time = Arc::new(AtomicU64::new(SEED_EXPIRES_AT_MS));
    let elsewhere = "s3://warehouse/another_table";
    let source = VendedCredentials::seeded(
        staged_table(&uri, elsewhere, true).await,
        seed(Some(SEED_EXPIRES_AT_MS)),
        clock_reading(&time),
    )
    .unwrap();

    let failed = provide(&source).await.unwrap_err();

    assert!(failed.contains(elsewhere), "{failed}");
    assert!(failed.starts_with("could not refresh"), "{failed}");
    assert_free_of(&failed, &FIXTURE_PLACEHOLDERS);
    assert_free_of(&failed, &SEED_SECRETS);
}

#[tokio::test]
async fn a_plaintext_vended_endpoint_without_allow_http_is_refused_at_seed_and_at_renewal() {
    let (uri, _) = catalog(OFFERING_CONFIG, fixture_answer).await;
    let time = Arc::new(AtomicU64::new(SEED_EXPIRES_AT_MS));
    let plaintext_seed = VendedS3Credentials {
        endpoint: Some("http://storage.example".into()),
        ..seed(Some(SEED_EXPIRES_AT_MS))
    };

    let at_seed = VendedCredentials::seeded(
        staged_table(&uri, STAGED_LOCATION, false).await,
        plaintext_seed,
        clock_reading(&time),
    )
    .unwrap_err()
    .to_string();
    let source = VendedCredentials::seeded(
        staged_table(&uri, STAGED_LOCATION, false).await,
        seed(Some(SEED_EXPIRES_AT_MS)),
        clock_reading(&time),
    )
    .unwrap();
    let at_renewal = provide(&source).await.unwrap_err();

    assert_eq!(
        at_seed,
        "the catalog vended the plaintext storage endpoint http://storage.example for table \
         ns.staged, but the export option ALLOW_HTTP is not 'true'"
    );
    assert_eq!(
        at_renewal,
        format!(
            "could not refresh the vended storage credentials of table ns.staged through the \
             catalog {uri}: the catalog vended the plaintext storage endpoint \
             http://seaweedfs:8333/ for table ns.staged, but the export option ALLOW_HTTP is not \
             'true'"
        )
    );
    assert_free_of(&at_renewal, &FIXTURE_PLACEHOLDERS);
}

#[tokio::test]
async fn a_renewal_answer_without_a_key_is_refused_naming_the_key() {
    let keyless = LOAD_CREDENTIALS.replace("s3.secret-access-key", "s3.unread-key");
    let (uri, _) = catalog(OFFERING_CONFIG, move |_| (200, keyless.clone())).await;
    let time = Arc::new(AtomicU64::new(SEED_EXPIRES_AT_MS));
    let source = source_at(&uri, seed(Some(SEED_EXPIRES_AT_MS)), &time).await;

    let refused = provide(&source).await.unwrap_err();

    assert!(
        refused.starts_with(&format!(
            "could not refresh the vended storage credentials of table ns.staged through the \
             catalog {uri}: loadCredentials of table 'ns.staged' failed: vended credentials were \
             requested for table location {STAGED_LOCATION}, but the catalog returned none"
        )),
        "{refused}"
    );
    assert!(refused.contains("s3.secret-access-key"), "{refused}");
    assert_free_of(&refused, &FIXTURE_PLACEHOLDERS);
    assert_free_of(&refused, &SEED_SECRETS);
}

#[tokio::test]
async fn debug_shows_no_credential_and_secret_values_lists_every_one_held() {
    let (uri, _) = catalog(OFFERING_CONFIG, fixture_answer).await;
    let time = Arc::new(AtomicU64::new(SEED_EXPIRES_AT_MS));
    let source = source_at(&uri, seed(Some(SEED_EXPIRES_AT_MS)), &time).await;

    provide(&source).await.unwrap();
    let shown = format!("{source:?}");
    let mut held = source.secret_values();
    held.sort();

    assert_free_of(&shown, &FIXTURE_PLACEHOLDERS);
    assert_free_of(&shown, &SEED_SECRETS);
    let mut expected: Vec<String> = SEED_SECRETS
        .iter()
        .chain(&FIXTURE_PLACEHOLDERS)
        .map(|value| value.to_string())
        .collect();
    expected.sort();
    assert_eq!(held, expected);
}
