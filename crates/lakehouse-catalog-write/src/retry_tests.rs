use super::*;

#[test]
fn the_wait_doubles_from_100_ms_and_stops_at_60_s() {
    let waits: Vec<Duration> = [1, 2, 3, 10, 11, 40, u32::MAX]
        .into_iter()
        .map(|retry| backoff_wait(retry, 0.0))
        .collect();

    assert_eq!(
        waits,
        [
            Duration::from_millis(100),
            Duration::from_millis(200),
            Duration::from_millis(400),
            Duration::from_millis(51_200),
            Duration::from_secs(60),
            Duration::from_secs(60),
            Duration::from_secs(60),
        ]
    );
}

#[test]
fn jitter_adds_up_to_a_tenth_and_never_passes_60_s() {
    assert_eq!(backoff_wait(1, 0.5), Duration::from_millis(105));
    assert_eq!(backoff_wait(1, 1.0), Duration::from_millis(110));
    assert_eq!(backoff_wait(1, 7.0), Duration::from_millis(110));
    assert_eq!(backoff_wait(1, -1.0), Duration::from_millis(100));
    assert_eq!(backoff_wait(11, 0.9), Duration::from_secs(60));
}

#[test]
fn a_table_sets_its_commit_retries_or_gets_iceberg_s_default() {
    let set = HashMap::from([(NUM_RETRIES_PROPERTY.to_string(), "20".to_string())]);
    let unreadable = HashMap::from([(NUM_RETRIES_PROPERTY.to_string(), "many".to_string())]);

    assert_eq!(commit_retries(&set), 20);
    assert_eq!(commit_retries(&unreadable), DEFAULT_RETRIES);
    assert_eq!(commit_retries(&HashMap::new()), DEFAULT_RETRIES);
}

#[tokio::test(start_paused = true)]
async fn retries_stop_at_their_bound() {
    let mut retries = Retries::new(2);

    let granted = [
        retries.wait_for_next().await,
        retries.wait_for_next().await,
        retries.wait_for_next().await,
    ];

    assert_eq!(granted, [true, true, false]);
    assert_eq!(retries.attempts(), 3);
}

#[tokio::test(start_paused = true)]
async fn no_retry_is_granted_with_a_bound_of_zero() {
    let mut retries = Retries::new(0);

    assert!(!retries.wait_for_next().await);
    assert_eq!(retries.attempts(), 1);
}
