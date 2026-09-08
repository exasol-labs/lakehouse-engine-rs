use super::*;
use crate::scan::spec::{AdlsCred, StorageProps};
use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;

type SecretFieldSetter = fn(&mut ConnectionCreds, &str);

const SEALING_PASSWORD: &str = r#"{"warehouse":"wh","secret_key":"S3CR3TV4LU3"}"#;

fn creds_without_key_material() -> ConnectionCreds {
    ConnectionCreds { warehouse: "wh".into(), path_style: true, ..Default::default() }
}

fn secret_bearing_fields() -> Vec<(&'static str, SecretFieldSetter)> {
    vec![
        ("token", |c, v| c.token = Some(v.into())),
        ("client_secret", |c, v| c.client_secret = Some(v.into())),
        ("secret_key", |c, v| c.secret_key = v.into()),
        ("session_token", |c, v| c.session_token = Some(v.into())),
        ("account_key", |c, v| c.account_key = Some(v.into())),
        ("sas_token", |c, v| c.sas_token = Some(v.into())),
    ]
}

fn s3_backend() -> StorageBackend {
    StorageBackend::S3(StorageProps {
        endpoint: "http://minio:9000".into(), region: "us-east-1".into(),
        access_key: "VENDEDAK".into(), secret_key: "VENDEDSK".into(),
        session_token: Some("VENDEDTOK".into()), allow_http: true, ..Default::default()
    })
}

fn adls_backend() -> StorageBackend {
    StorageBackend::Adls {
        account_name: "acct".into(),
        cred: AdlsCred::Sas("sv=2021&sig=VENDEDSAS".into()),
    }
}

fn assert_carries_no_sentinel(context: &str, text: &str) {
    for s in ["VENDEDAK", "VENDEDSK", "VENDEDTOK", "VENDEDSAS", "S3CR3TV4LU3", SEALING_PASSWORD] {
        assert!(!text.contains(s), "{context} must not echo {s}: {text}");
    }
}

#[test]
fn sealed_storage_round_trips_and_rejects_a_tampered_payload() {
    let key = derive_sealed_storage_key(SEALING_PASSWORD);

    for backend in [s3_backend(), adls_backend()] {
        let payload = seal_storage(&backend, &key).expect("sealing a backend must succeed");
        assert_carries_no_sentinel("a sealed payload", &payload);
        assert_eq!(
            unseal_storage(&payload, &key).expect("its own envelope must open"),
            backend,
        );

        let mut raw = BASE64.decode(&payload).expect("a fresh seal must decode");
        raw[NONCE_BYTES] ^= 0xFF;
        let tampered = BASE64.encode(&raw);
        let err = unseal_storage(&tampered, &key)
            .expect_err("a tampered envelope must not open")
            .to_string();
        assert!(err.contains("AES-256-GCM authentication"), "{err}");
        assert_carries_no_sentinel("a tampered-envelope error", &err);

        let rotated = derive_sealed_storage_key(r#"{"warehouse":"wh","secret_key":"ROTATED"}"#);
        let err = unseal_storage(&payload, &rotated)
            .expect_err("an envelope must not open under another key")
            .to_string();
        assert!(err.contains("AES-256-GCM authentication"), "{err}");
        assert_carries_no_sentinel("a wrong-key error", &err);
    }
}

#[test]
fn key_material_is_present_only_for_a_non_empty_secret_bearing_field() {
    assert!(!connection_password_carries_key_material(&creds_without_key_material()));
    for (field, set) in secret_bearing_fields() {
        let mut non_empty = creds_without_key_material();
        set(&mut non_empty, "s");
        assert!(connection_password_carries_key_material(&non_empty), "{field} non-empty");
        let mut empty = creds_without_key_material();
        set(&mut empty, "");
        assert!(!connection_password_carries_key_material(&empty), "{field} empty");
    }
}
