//! The shape of an S3 credential lease that every renewable source shares.

use crate::expiring::Lease;
use iceberg_storage_opendal::AwsCredential;
use reqsign_core::time::Timestamp;

/// An S3 credential and its expiry. The credential carries the expiry too, because the signer
/// asks its source again only once the credential it holds is near expiry.
pub fn aws_lease(
    access_key_id: String,
    secret_access_key: String,
    session_token: Option<String>,
    expires_at_ms: Option<u64>,
) -> Lease<AwsCredential> {
    Lease {
        value: AwsCredential {
            access_key_id,
            secret_access_key,
            session_token,
            expires_in: expires_at_ms.and_then(timestamp),
        },
        expires_at_ms,
    }
}

/// The instant `epoch_ms` after the Unix epoch, `None` when it is out of range.
pub(crate) fn timestamp(epoch_ms: u64) -> Option<Timestamp> {
    Timestamp::from_millisecond(i64::try_from(epoch_ms).ok()?).ok()
}
