use super::*;
use crate::test_support::{PREFIX_CONFIG, STAGED_LOCATION, assert_free_of, catalog, session};
use lakehouse_catalog::exported_test_support::clock_reading;
use reqsign_core::Context;
use reqsign_core::time::Timestamp;
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
) -> StorageCredentials {
    StorageCredentials::vended(
        staged_table(uri, STAGED_LOCATION, true).await,
        seed,
        clock_reading(time),
    )
    .expect("a seeded source")
}

async fn provide(source: &StorageCredentials) -> Result<AwsCredential, String> {
    source
        .provide_credential(&Context::new())
        .await
        .map(|credential| credential.expect("a credential"))
        .map_err(|error| error.to_string())
}

/// Scenario: A writer that outlives its vended credentials refreshes them, or fails naming their expiry
#[test]
fn credentials_are_kept_only_while_valid_and_within_30_seconds_of_their_renewal() {
    let expires_at = 10 * MINUTE_MS;
    let point = |renewed_at_ms, now_ms| RenewalPoint {
        expires_at_ms: expires_at,
        renewed_at_ms,
        now_ms,
    };
    let cases = [
        (point(None, expires_at - 5 * MINUTE_MS), false),
        (point(None, expires_at + 1), false),
        (point(Some(expires_at - 30_000), expires_at), false),
        (point(Some(expires_at - 29_999), expires_at - 1), true),
        (point(Some(expires_at - 29_999), expires_at), false),
    ];

    for (point, kept) in cases {
        assert_eq!(keep_at_renewal_point(point), kept, "{point:?}");
    }
}

/// Scenario: A writer that outlives its vended credentials refreshes them, or fails naming their expiry
#[tokio::test]
async fn credentials_within_five_minutes_of_expiry_are_renewed_through_one_load_credentials_request()
 {
    let (uri, _) = catalog(OFFERING_CONFIG, fixture_answer).await;
    let time = Arc::new(AtomicU64::new(SEED_EXPIRES_AT_MS - 60 * MINUTE_MS));
    let source = source_at(&uri, seed(Some(SEED_EXPIRES_AT_MS)), &time).await;
    time.store(SEED_EXPIRES_AT_MS - 5 * MINUTE_MS - 1, Ordering::SeqCst);

    let early = provide(&source).await.unwrap();
    time.store(SEED_EXPIRES_AT_MS - 4 * MINUTE_MS, Ordering::SeqCst);
    let renewed = provide(&source).await.unwrap();
    let again = provide(&source).await.unwrap();

    assert_eq!(early.access_key_id, SEED_SECRETS[0]);
    assert_eq!(renewed.access_key_id, FIXTURE_PLACEHOLDERS[0]);
    assert_eq!(again.access_key_id, FIXTURE_PLACEHOLDERS[0]);
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
async fn credentials_renewed_close_to_their_expiry_are_renewed_again_only_after_30_seconds() {
    let now = SEED_EXPIRES_AT_MS;
    let renewals = AtomicU64::new(0);
    let (uri, _) = catalog(OFFERING_CONFIG, move |_| {
        let renewal = renewals.fetch_add(1, Ordering::SeqCst);
        let answer = fixture_expiring_at(now + MINUTE_MS);
        let answer = answer.replace(FIXTURE_PLACEHOLDERS[0], &format!("RENEWAL_{renewal}"));
        (200, answer)
    })
    .await;
    let time = Arc::new(AtomicU64::new(now));
    let source = source_at(&uri, seed(Some(SEED_EXPIRES_AT_MS)), &time).await;

    let first = provide(&source).await.unwrap();
    time.store(now + 29_999, Ordering::SeqCst);
    let within_pause = provide(&source).await.unwrap();
    time.store(now + 30_000, Ordering::SeqCst);
    let after_pause = provide(&source).await.unwrap();

    assert_eq!(first.access_key_id, "RENEWAL_0");
    assert_eq!(within_pause.access_key_id, "RENEWAL_0");
    assert_eq!(after_pause.access_key_id, "RENEWAL_1");
}

/// Scenario: A writer that outlives its vended credentials refreshes them, or fails naming their expiry
#[tokio::test]
async fn expired_credentials_renew_whether_or_not_the_catalog_lists_the_credentials_endpoint() {
    let (uri, _) = catalog(PREFIX_CONFIG, fixture_answer).await;
    let time = Arc::new(AtomicU64::new(SEED_EXPIRES_AT_MS + MINUTE_MS));
    let source = source_at(&uri, seed(Some(SEED_EXPIRES_AT_MS)), &time).await;

    let renewed = provide(&source).await.unwrap();

    assert_eq!(renewed.access_key_id, FIXTURE_PLACEHOLDERS[0]);
}

#[tokio::test]
async fn connection_keys_never_expire_and_are_never_renewed() {
    let time = Arc::new(AtomicU64::new(0));
    let source = StorageCredentials::keys("KEY", "SECRET", Some("TOKEN"), clock_reading(&time));

    time.store(u64::MAX / 2, Ordering::SeqCst);
    let kept = provide(&source).await.unwrap();

    assert_eq!(kept.access_key_id, "KEY");
    assert_eq!(kept.expires_in, None);
    assert_eq!(source.secret_values(), ["KEY", "SECRET", "TOKEN"]);
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
    let source = StorageCredentials::vended(
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

    let at_seed = StorageCredentials::vended(
        staged_table(&uri, STAGED_LOCATION, false).await,
        plaintext_seed,
        clock_reading(&time),
    )
    .unwrap_err()
    .to_string();
    let source = StorageCredentials::vended(
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
async fn debug_shows_no_credential_and_secret_values_lists_only_the_credential_held_now() {
    let (uri, _) = catalog(OFFERING_CONFIG, fixture_answer).await;
    let time = Arc::new(AtomicU64::new(SEED_EXPIRES_AT_MS));
    let source = source_at(&uri, seed(Some(SEED_EXPIRES_AT_MS)), &time).await;
    let before = source.secret_values();

    provide(&source).await.unwrap();
    let shown = format!("{source:?}");
    let mut held = source.secret_values();
    held.sort();

    assert_eq!(before.len(), SEED_SECRETS.len());
    assert_free_of(&shown, &FIXTURE_PLACEHOLDERS);
    assert_free_of(&shown, &SEED_SECRETS);
    let mut expected: Vec<String> = FIXTURE_PLACEHOLDERS
        .iter()
        .map(ToString::to_string)
        .collect();
    expected.sort();
    assert_eq!(held, expected);
}
