//! The shape of an S3 credential lease that every renewable source shares.

use crate::expiring::Lease;

/// An AWS key triple, whose `Debug` shows no value.
#[derive(Clone, Default, PartialEq, Eq)]
pub struct AwsCredential {
    pub access_key_id: String,
    pub secret_access_key: String,
    pub session_token: Option<String>,
}

impl std::fmt::Debug for AwsCredential {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AwsCredential").finish_non_exhaustive()
    }
}

impl From<AwsCredential> for object_store::aws::AwsCredential {
    fn from(credential: AwsCredential) -> Self {
        Self {
            key_id: credential.access_key_id,
            secret_key: credential.secret_access_key,
            token: credential.session_token,
        }
    }
}

/// An S3 credential and its expiry.
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
        },
        expires_at_ms,
    }
}
