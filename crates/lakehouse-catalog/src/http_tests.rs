use super::*;
use crate::test_support::*;

async fn get(base_uri: &str) -> reqwest::Response {
    let client = build_client();
    let request = client
        .get(format!("{base_uri}/v1/config"))
        .build()
        .expect("request builds");
    execute_with_retry(&client, request)
        .await
        .expect("the request reaches the mock")
}

#[tokio::test]
async fn a_throttled_or_unavailable_response_is_retried() {
    for status in [429u16, 502, 503, 504] {
        let catalog = spawn_routing_catalog(
            vec![(
                "/v1/config",
                vec![(status, String::new()), (200, "{}".into())],
            )],
            Duration::ZERO,
        )
        .await;

        let response = get(&catalog.base_uri).await;

        assert_eq!(
            response.status().as_u16(),
            200,
            "HTTP {status} must be retried"
        );
        assert_eq!(catalog.request_lines().len(), 2);
    }
}

#[tokio::test]
async fn other_failures_are_not_retried() {
    for status in [400u16, 404, 500] {
        let catalog = spawn_routing_catalog(
            vec![(
                "/v1/config",
                vec![(status, String::new()), (200, "{}".into())],
            )],
            Duration::ZERO,
        )
        .await;

        let response = get(&catalog.base_uri).await;

        assert_eq!(
            response.status().as_u16(),
            status,
            "HTTP {status} must not be retried"
        );
        assert_eq!(catalog.request_lines().len(), 1);
    }
}

#[tokio::test]
async fn retries_are_bounded() {
    let catalog = spawn_routing_catalog(
        vec![("/v1/config", vec![(503, String::new())])],
        Duration::ZERO,
    )
    .await;

    let response = get(&catalog.base_uri).await;

    assert_eq!(response.status().as_u16(), 503);
    assert_eq!(
        catalog.request_lines().len(),
        RETRY_ATTEMPTS as usize,
        "a persistently unavailable catalog is given up on after the retry budget"
    );
}
