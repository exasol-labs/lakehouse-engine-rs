//! Sealing the one storage credential the wire cannot reference by name.
//!
//! A STATIC storage credential travels as a CONNECTION name the scan UDF
//! resolves for itself ([`crate::scan::spec::ScanStorage::Connection`]). A VENDED
//! one has no name: it comes from the catalog's own `loadTable` / temporary
//! credentials response, the UDF may not re-request it, and nothing identifies
//! it. So it travels inside this module's envelope instead — AES-256-GCM
//! ciphertext under a key both sides derive, independently and identically, from
//! the CONNECTION password each of them reads through `ctx.connection()`.
//!
//! **The guarantee is deliberately bounded.** It defeats a plaintext read or grep
//! of the pushdown SQL — `EXPLAIN VIRTUAL` output and pushdown-path error text.
//! It does NOT claim to withstand offline cryptanalysis of the ciphertext against
//! a low-entropy password. Two facts make that an acceptable trade rather than an
//! oversight: the protected values are short-lived and prefix-scoped (the catalog
//! vends them per query and expires them on its own schedule), and the key
//! material is exactly the secret an attacker would need `ACCESS ON CONNECTION`
//! to read — a grant whose holder can already read the credential outright.
//!
//! Key derivation reads the password BYTES without parsing them, so no
//! catalog-authentication field is ever constructed on the scan side.

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

/// The HKDF `info` string binding a derived key to this purpose and this wire
/// version. A later envelope format changes the `v1` suffix, so a key derived for
/// one format can never open the other.
const SEALED_STORAGE_INFO: &[u8] = b"lakehouse-engine scan-storage sealed v1";

/// AES-256-GCM's nonce width: 96 bits, the size the AEAD construction is
/// specified for.
const NONCE_BYTES: usize = 12;

/// The 32-byte key an envelope is sealed and opened under, derived from a
/// CONNECTION password.
///
/// Opaque and redacting: the bytes are reachable only inside this module, and
/// `Debug` prints a placeholder, so a `{:?}` added at any call site cannot print
/// key material.
pub(crate) struct SealedStorageKey([u8; 32]);

impl std::fmt::Debug for SealedStorageKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("SealedStorageKey")
            .field(&"[redacted]")
            .finish()
    }
}

/// Derive the envelope key from a CONNECTION password: HKDF-SHA256 over the raw
/// password bytes, empty salt, [`SEALED_STORAGE_INFO`] as `info`.
///
/// The password is consumed as BYTES and never parsed, so this derivation
/// constructs no credential field of any kind. Both sides of the wire run it —
/// the adapter at plan time and the scan UDF after its own `ctx.connection()`
/// read — so the two agree on the key without any shared state and without the
/// key itself ever travelling.
///
/// There is no salt because there is no second party to negotiate one with: the
/// same password must derive the same key in two processes that never talk.
pub(crate) fn derive_sealed_storage_key(password: &str) -> SealedStorageKey {
    let mut key = [0u8; 32];
    Hkdf::<Sha256>::new(None, password.as_bytes())
        .expand(SEALED_STORAGE_INFO, &mut key)
        .expect("32 bytes is far within HKDF-SHA256's 8160-byte output limit");
    SealedStorageKey(key)
}

/// Seal a resolved [`StorageBackend`] into the wire form
/// `base64(nonce ‖ AES-256-GCM ciphertext)`.
///
/// A FRESH 96-bit nonce is drawn from the OS entropy source on every call. That
/// is not an optimisation to skip: a nonce reused under one key breaks AES-GCM
/// outright, and a fixed nonce would additionally make the wire leak equality —
/// two queries over the same vended credential would carry byte-identical
/// ciphertext.
///
/// Every failure names the operation that failed and nothing else: the value
/// being sealed IS the credential, so no error may quote it.
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

/// Open an envelope produced by [`seal_storage`] under the key derived from the
/// same CONNECTION password.
///
/// Every failure path returns an error naming the operation that failed —
/// base64 decoding, the length precondition, AEAD authentication, or
/// deserialization — and carries neither the payload, nor the password, nor any
/// plaintext. The expected cause of an authentication failure is a CONNECTION
/// rotated between the adapter's read and this one, so the outcome must be a
/// clear refusal and never a fallback to a partial or stale credential.
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

/// Whether this CONNECTION password carries secret material a sealing key can be
/// derived from — the ONE predicate deciding whether the envelope's guarantee can
/// hold, declared here beside the derivation it gates.
///
/// True iff at least one of `token`, `client_secret`, `secret_key`,
/// `session_token`, `account_key`, or `sas_token` is present AND non-empty. A
/// non-empty `access_key` alone does NOT satisfy it: an AWS access key id is an
/// identifier, not a secret.
///
/// **The test is NON-EMPTINESS, not entropy.** A `token` of `"x"` satisfies it,
/// so the envelope's bound rests on the module-level threat model AND on the
/// operator's own secret strength. The engine cannot measure the entropy of an
/// arbitrary password without rejecting legitimate secrets, so it tests the
/// property it can test and states the residual rather than implying more.
///
/// The criterion is the password's secret CONTENT, not the catalog-auth mode: the
/// installer's own default template creates a CONNECTION with
/// `{"warehouse", "region", "access_key", "secret_key"}` and no catalog
/// authentication, and that high-entropy password must be admitted. What is
/// refused is a password holding no secret at all — `{"warehouse":"…"}` is the
/// canonical shape — because a key derived from it would be a false guarantee.
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
