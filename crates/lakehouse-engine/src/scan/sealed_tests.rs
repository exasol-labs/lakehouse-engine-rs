use super::*;
use crate::scan::spec::{AdlsCred, StorageProps};
use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;

/// A setter that assigns one secret-bearing field of a [`ConnectionCreds`] a
/// given value — the shape [`secret_bearing_fields`] pairs with each field name.
type SecretFieldSetter = fn(&mut ConnectionCreds, &str);

/// The password whose bytes derive every sealing key in this module.
const SEALING_PASSWORD: &str = r#"{"warehouse":"wh","secret_key":"S3CR3TV4LU3"}"#;

/// A CONNECTION password carrying no secret-bearing field at all — the shape the
/// vending gate refuses.
fn creds_without_key_material() -> ConnectionCreds {
    ConnectionCreds {
        warehouse: "wh".into(),
        endpoint: String::new(),
        region: String::new(),
        access_key: String::new(),
        secret_key: String::new(),
        session_token: None,
        path_style: true,
        use_sigv4: false,
        use_vended_credentials: false,
        token: None,
        client_id: None,
        client_secret: None,
        oauth2_server_uri: None,
        scope: None,
        account_name: None,
        account_key: None,
        sas_token: None,
    }
}

/// Every secret-bearing field the predicate reads, paired with a setter that
/// assigns it the given value. Naming all six here is what makes the truth table
/// below exhaustive by construction rather than by inspection.
fn secret_bearing_fields() -> Vec<(&'static str, SecretFieldSetter)> {
    vec![
        ("token", |c, v| c.token = Some(v.to_string())),
        ("client_secret", |c, v| {
            c.client_secret = Some(v.to_string())
        }),
        ("secret_key", |c, v| c.secret_key = v.to_string()),
        ("session_token", |c, v| {
            c.session_token = Some(v.to_string())
        }),
        ("account_key", |c, v| c.account_key = Some(v.to_string())),
        ("sas_token", |c, v| c.sas_token = Some(v.to_string())),
    ]
}

fn s3_backend() -> StorageBackend {
    StorageBackend::S3(StorageProps {
        endpoint: "http://minio:9000".into(),
        region: "us-east-1".into(),
        access_key: "VENDEDAK".into(),
        secret_key: "VENDEDSK".into(),
        session_token: Some("VENDEDTOK".into()),
        allow_http: true,
        path_style: true,
    })
}

fn adls_backend() -> StorageBackend {
    StorageBackend::Adls {
        account_name: "acct".into(),
        cred: AdlsCred::Sas("sv=2021&sig=VENDEDSAS".into()),
    }
}

/// Every credential value the fixtures above put inside an envelope, plus the
/// sealing password itself: the strings no error text may echo.
fn sentinel_values() -> Vec<&'static str> {
    vec![
        "VENDEDAK",
        "VENDEDSK",
        "VENDEDTOK",
        "VENDEDSAS",
        "S3CR3TV4LU3",
        SEALING_PASSWORD,
    ]
}

fn assert_carries_no_sentinel(context: &str, text: &str) {
    for sentinel in sentinel_values() {
        assert!(
            !text.contains(sentinel),
            "{context} must not echo {sentinel}: {text}"
        );
    }
}

/// A sealed envelope round-trips to the backend that was sealed, for EVERY
/// backend variant — and a payload that has been tampered with, truncated, or
/// opened under the wrong key fails with an error naming the operation that
/// failed and echoing neither the password nor any credential value.
///
/// Both halves live in one test because the negative half is only meaningful
/// against a positive control: an "unseal failed" assertion is satisfied by an
/// envelope that never held anything.
#[test]
fn sealed_storage_round_trips_and_rejects_a_tampered_payload() {
    let key = derive_sealed_storage_key(SEALING_PASSWORD);

    for backend in [s3_backend(), adls_backend()] {
        let payload = seal_storage(&backend, &key).expect("sealing a backend must succeed");
        assert_carries_no_sentinel("a sealed payload", &payload);
        assert_eq!(
            unseal_storage(&payload, &key).expect("its own envelope must open"),
            backend,
            "the envelope must open to exactly the backend that was sealed"
        );

        // Tampered: flip one byte INSIDE the ciphertext body (at or after
        // NONCE_BYTES, so the nonce itself is untouched). AES-GCM authenticates,
        // so this must fail the AEAD tag check specifically — not merely fail to
        // decode or fail some other way.
        let mut raw = BASE64.decode(&payload).expect("a fresh seal must decode");
        raw[NONCE_BYTES] ^= 0xFF;
        let tampered = BASE64.encode(&raw);
        let err = unseal_storage(&tampered, &key)
            .expect_err("a tampered envelope must not open")
            .to_string();
        assert!(
            err.contains("AES-256-GCM authentication"),
            "the error must name AEAD authentication as the failed step: {err}"
        );
        assert_carries_no_sentinel("a tampered-envelope error", &err);

        // Truncated to inside the nonce: shorter than one nonce carries no
        // ciphertext at all.
        let truncated = &payload[..8];
        let err = unseal_storage(truncated, &key)
            .expect_err("a truncated envelope must not open")
            .to_string();
        assert!(
            err.contains("the envelope is not longer than its nonce"),
            "the error must name the length precondition as the failed step: {err}"
        );
        assert_carries_no_sentinel("a truncated-envelope error", &err);

        // Wrong key: the shape a CONNECTION rotated mid-query produces. This also
        // fails AEAD authentication, not decoding or length.
        let rotated = derive_sealed_storage_key(r#"{"warehouse":"wh","secret_key":"ROTATED"}"#);
        let err = unseal_storage(&payload, &rotated)
            .expect_err("an envelope must not open under another key")
            .to_string();
        assert!(
            err.contains("AES-256-GCM authentication"),
            "the error must name AEAD authentication as the failed step: {err}"
        );
        assert_carries_no_sentinel("a wrong-key error", &err);

        // Not valid base64 at all: fails before the length precondition or AEAD
        // are ever reached.
        let err = unseal_storage("not valid base64 !!!", &key)
            .expect_err("a non-base64 payload must not open")
            .to_string();
        assert!(
            err.contains("base64-decoding the envelope"),
            "the error must name base64 decoding as the failed step: {err}"
        );
        assert_carries_no_sentinel("an invalid-base64 error", &err);
    }
}

/// Sealing one backend twice yields two DIFFERENT payloads — a fresh nonce per
/// encryption, never a fixed one — and both still open to the same backend.
///
/// A reused nonce under one key is the classic AES-GCM break, and it would also
/// make the wire leak equality: two queries over the same vended credential
/// would carry byte-identical ciphertext.
#[test]
fn two_seals_of_one_backend_differ_and_both_unseal() {
    let key = derive_sealed_storage_key(SEALING_PASSWORD);
    let backend = s3_backend();

    let first = seal_storage(&backend, &key).expect("the first seal must succeed");
    let second = seal_storage(&backend, &key).expect("the second seal must succeed");

    assert_ne!(
        first, second,
        "two seals of one backend must differ — a fresh nonce per encryption"
    );
    assert_eq!(unseal_storage(&first, &key).expect("first opens"), backend);
    assert_eq!(
        unseal_storage(&second, &key).expect("second opens"),
        backend
    );
}

/// The derived key is a function of the password bytes ALONE: the same password
/// derives the same key (so the adapter and the scan UDF agree without sharing
/// state), and a different password derives a different one.
#[test]
fn the_derived_key_is_a_function_of_the_password_alone() {
    let backend = s3_backend();
    let payload = seal_storage(&backend, &derive_sealed_storage_key(SEALING_PASSWORD))
        .expect("sealing must succeed");

    // A key derived independently from the same password opens the envelope.
    assert_eq!(
        unseal_storage(&payload, &derive_sealed_storage_key(SEALING_PASSWORD))
            .expect("an independently derived key must open the envelope"),
        backend
    );
    // One byte of difference in the password does not.
    assert!(
        unseal_storage(
            &payload,
            &derive_sealed_storage_key(&format!("{SEALING_PASSWORD} "))
        )
        .is_err(),
        "a different password must not derive an opening key"
    );
}

/// [`connection_password_carries_key_material`]'s full truth table: FALSE for a
/// password carrying none of the six secret-bearing fields, TRUE for each of the
/// six carried non-empty in turn, FALSE for each carried but EMPTY, and FALSE for
/// a non-empty `access_key` with an empty `secret_key` — an AWS access key id is
/// an identifier, not a secret.
#[test]
fn key_material_is_present_only_for_a_non_empty_secret_bearing_field() {
    assert!(
        !connection_password_carries_key_material(&creds_without_key_material()),
        "a password carrying none of the six secret fields carries no key material"
    );

    for (field, set) in secret_bearing_fields() {
        let mut non_empty = creds_without_key_material();
        set(&mut non_empty, "s");
        assert!(
            connection_password_carries_key_material(&non_empty),
            "a non-empty {field} alone must satisfy the gate"
        );

        let mut empty = creds_without_key_material();
        set(&mut empty, "");
        assert!(
            !connection_password_carries_key_material(&empty),
            "a present-but-empty {field} must not satisfy the gate"
        );
    }

    // Every one of the six present but empty, all at once.
    let mut all_empty = creds_without_key_material();
    for (_, set) in secret_bearing_fields() {
        set(&mut all_empty, "");
    }
    assert!(
        !connection_password_carries_key_material(&all_empty),
        "all six fields present but empty must not satisfy the gate"
    );

    let mut access_key_only = creds_without_key_material();
    access_key_only.access_key = "AKIAEXAMPLE".into();
    assert!(
        !connection_password_carries_key_material(&access_key_only),
        "an access_key id without a secret_key is an identifier, not key material"
    );
}
