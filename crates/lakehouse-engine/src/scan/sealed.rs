//! AES-256-GCM envelope for vended credentials that have no CONNECTION name to
//! reference. Key is HKDF-SHA256 over the CONNECTION password bytes (no parsing,
//! so no catalog-auth field is ever constructed on the scan side). Defeats
//! plaintext reads of pushdown SQL; does not claim offline cryptanalysis
//! resistance — vended values are short-lived and the key material requires
//! `ACCESS ON CONNECTION`.

use aes_gcm::aead::{Aead, KeyInit};
use aes_gcm::{Aes256Gcm, Key, Nonce};
use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use exasol_udf_sdk::error::UdfError;
use hkdf::Hkdf;
use lakehouse_catalog::ConnectionCreds;
use rand::RngCore;
use sha2::Sha256;

use crate::scan::spec::StorageBackend;

/// Changing the `v1` suffix ensures a key derived for one format cannot open another.
const SEALED_STORAGE_INFO: &[u8] = b"lakehouse-engine scan-storage sealed v1";

const NONCE_BYTES: usize = 12;

/// `Debug` prints a placeholder — never key material.
pub(crate) struct SealedStorageKey([u8; 32]);

impl std::fmt::Debug for SealedStorageKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("SealedStorageKey")
            .field(&"[redacted]")
            .finish()
    }
}

/// No salt — both sides must derive the same key without shared state.
pub(crate) fn derive_sealed_storage_key(password: &str) -> SealedStorageKey {
    let mut key = [0u8; 32];
    Hkdf::<Sha256>::new(None, password.as_bytes())
        .expand(SEALED_STORAGE_INFO, &mut key)
        .expect("32 bytes is far within HKDF-SHA256's 8160-byte output limit");
    SealedStorageKey(key)
}

/// Seal into `base64(nonce || AES-256-GCM ciphertext)`. Fresh 96-bit nonce per call.
pub(crate) fn seal_storage(
    backend: &StorageBackend,
    key: &SealedStorageKey,
) -> Result<String, UdfError> {
    let plaintext = serde_json::to_vec(backend).map_err(|_| {
        UdfError::User(
            "sealing the scan storage failed: serializing the storage backend".to_string(),
        )
    })?;

    let mut nonce = [0u8; NONCE_BYTES];
    rand::rngs::OsRng.fill_bytes(&mut nonce);

    let ciphertext = Aes256Gcm::new(Key::<Aes256Gcm>::from_slice(&key.0))
        .encrypt(Nonce::from_slice(&nonce), plaintext.as_slice())
        .map_err(|_| {
            UdfError::User("sealing the scan storage failed: AES-256-GCM encryption".to_string())
        })?;

    let mut envelope = Vec::with_capacity(NONCE_BYTES + ciphertext.len());
    envelope.extend_from_slice(&nonce);
    envelope.extend_from_slice(&ciphertext);
    Ok(BASE64.encode(envelope))
}

pub(crate) fn unseal_storage(
    payload: &str,
    key: &SealedStorageKey,
) -> Result<StorageBackend, UdfError> {
    let envelope = BASE64.decode(payload).map_err(|_| {
        UdfError::User(
            "unsealing the scan storage failed: base64-decoding the envelope".to_string(),
        )
    })?;

    if envelope.len() <= NONCE_BYTES {
        return Err(UdfError::User(
            "unsealing the scan storage failed: the envelope is not longer than its nonce"
                .to_string(),
        ));
    }
    let (nonce, ciphertext) = envelope.split_at(NONCE_BYTES);

    let plaintext = Aes256Gcm::new(Key::<Aes256Gcm>::from_slice(&key.0))
        .decrypt(Nonce::from_slice(nonce), ciphertext)
        .map_err(|_| {
            UdfError::User(
                "unsealing the scan storage failed: AES-256-GCM authentication".to_string(),
            )
        })?;

    serde_json::from_slice(&plaintext).map_err(|_| {
        UdfError::User(
            "unsealing the scan storage failed: deserializing the storage backend".to_string(),
        )
    })
}

/// True when the password carries at least one secret field (not just an access_key id).
pub(crate) fn connection_password_carries_key_material(creds: &ConnectionCreds) -> bool {
    let optional = [
        creds.token.as_deref(),
        creds.client_secret.as_deref(),
        creds.session_token.as_deref(),
        creds.account_key.as_deref(),
        creds.sas_token.as_deref(),
    ];
    !creds.secret_key.is_empty()
        || optional
            .into_iter()
            .flatten()
            .any(|value| !value.is_empty())
}

#[cfg(test)]
#[path = "sealed_tests.rs"]
mod tests;
