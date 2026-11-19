use super::*;

#[test]
fn the_wait_doubles_from_100_ms_and_stops_at_60_s() {
    let waits: Vec<Duration> = [1, 2, 3, 10, 11, 40, u32::MAX]
        .into_iter()
        .map(|retry| Waits::DEFAULT.backoff(retry, 0.0))
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
    assert_eq!(Waits::DEFAULT.backoff(1, 0.5), Duration::from_millis(105));
    assert_eq!(Waits::DEFAULT.backoff(1, 1.0), Duration::from_millis(110));
    assert_eq!(Waits::DEFAULT.backoff(1, 7.0), Duration::from_millis(110));
    assert_eq!(Waits::DEFAULT.backoff(1, -1.0), Duration::from_millis(100));
    assert_eq!(Waits::DEFAULT.backoff(11, 0.9), Duration::from_secs(60));
}

#[test]
fn a_table_sets_its_commit_retries_or_gets_iceberg_s_default() {
    let set = HashMap::from([(NUM_RETRIES_PROPERTY.to_string(), "20".to_string())]);
    let unreadable = HashMap::from([(NUM_RETRIES_PROPERTY.to_string(), "many".to_string())]);

    assert_eq!(commit_retries(&set), 20);
    assert_eq!(commit_retries(&unreadable), DEFAULT_RETRIES);
    assert_eq!(commit_retries(&HashMap::new()), DEFAULT_RETRIES);
}

#[test]
fn an_append_retries_at_least_as_often_as_an_export_s_own_table() {
    let few = HashMap::from([(NUM_RETRIES_PROPERTY.to_string(), "2".to_string())]);
    let many = HashMap::from([(NUM_RETRIES_PROPERTY.to_string(), "50".to_string())]);

    assert_eq!(append_retries(&HashMap::new()), EXPORT_RETRIES);
    assert_eq!(append_retries(&few), EXPORT_RETRIES);
    assert_eq!(append_retries(&many), 50);
}

#[test]
fn a_table_sets_the_bounds_of_its_waits_or_gets_iceberg_s_defaults() {
    let table = |min: &str, max: &str| {
        Waits::of_table(&HashMap::from([
            (MIN_WAIT_PROPERTY.to_string(), min.to_string()),
            (MAX_WAIT_PROPERTY.to_string(), max.to_string()),
        ]))
    };
    let set = table("10", "50");
    let unreadable = table("soon", "-1");

    assert_eq!(
        [1, 2, 3, 4].map(|retry| set.backoff(retry, 0.0)),
        [10, 20, 40, 50].map(Duration::from_millis)
    );
    assert_eq!(set.backoff(3, 1.0), Duration::from_millis(44));
    assert_eq!(set.backoff(4, 1.0), Duration::from_millis(50));
    assert_eq!(table("0", "0").backoff(5, 0.5), Duration::ZERO);
    assert_eq!(
        [1, 11].map(|retry| unreadable.backoff(retry, 0.0)),
        [Duration::from_millis(100), Duration::from_secs(60)]
    );
    assert_eq!(
        Waits::of_table(&HashMap::new()).backoff(11, 0.0),
        Duration::from_secs(60)
    );
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
