//! SigV4 signing for AWS Glue catalog requests, and the sole owner of the
//! signing-region rule. Key material is never stored; `SigningError` carries no
//! credential fields.
use crate::ConnectionCreds;
use aws_credential_types::Credentials;
use aws_sigv4::http_request::{
    PayloadChecksumKind, SignableBody, SignableRequest, SigningError, SigningSettings, sign,
};
use aws_sigv4::sign::v4;
use aws_smithy_runtime_api::client::identity::Identity;
use exasol_udf_sdk::error::UdfError;
use std::time::SystemTime;

pub(crate) const MISSING_SIGNING_REGION: &str = "SigV4 catalog signing requires a region: neither the \
                                      stated region nor the catalog URI supplies one";

impl ConnectionCreds {
    /// A standard commercial Glue endpoint host's region wins over a stated,
    /// differing `region`; otherwise the non-empty stated `region`. Never written
    /// back into `region`: the catalog and its bucket may sit in different regions.
    pub fn sigv4_signing_region(&self, catalog_uri: &str) -> Option<String> {
        glue_endpoint_region(catalog_uri)
            .or_else(|| (!self.region.is_empty()).then(|| self.region.clone()))
    }
}

/// Refuses up front rather than signing for an empty region, which Glue rejects
/// later with an opaque 403.
pub(crate) fn required_signing_region(
    creds: &ConnectionCreds,
    catalog_uri: &str,
) -> Result<String, UdfError> {
    creds
        .sigv4_signing_region(catalog_uri)
        .ok_or_else(|| UdfError::User(MISSING_SIGNING_REGION.into()))
}

fn glue_endpoint_region(catalog_uri: &str) -> Option<String> {
    let address = url::Url::parse(catalog_uri).ok()?;
    if address.scheme() != "https" {
        return None;
    }
    let region = address
        .host_str()?
        .strip_prefix("glue.")?
        .strip_suffix(".amazonaws.com")?;
    is_commercial_region_code(region).then(|| region.to_string())
}

fn is_commercial_region_code(candidate: &str) -> bool {
    let mut parts = candidate.split('-');
    let (Some(area), Some(subarea), Some(ordinal), None) =
        (parts.next(), parts.next(), parts.next(), parts.next())
    else {
        return false;
    };
    area.len() == 2
        && area.bytes().all(|b| b.is_ascii_lowercase())
        && !subarea.is_empty()
        && subarea.bytes().all(|b| b.is_ascii_lowercase())
        && !ordinal.is_empty()
        && ordinal.bytes().all(|b| b.is_ascii_digit())
}

pub(crate) fn sign_request(
    mut request: reqwest::Request,
    access_key: &str,
    secret_key: &str,
    session_token: Option<&str>,
    region: &str,
    service: &str,
) -> Result<reqwest::Request, SigningError> {
    let creds = Credentials::new(
        access_key,
        secret_key,
        session_token.map(|s| s.to_string()),
        None,
        "lakehouse-engine",
    );
    let identity: Identity = creds.into();

    let mut settings = SigningSettings::default();
    // Without `x-amz-content-sha256`, Glue recomputes a different payload hash and
    // returns 403 SignatureDoesNotMatch.
    settings.payload_checksum_kind = PayloadChecksumKind::XAmzSha256;
    let params: aws_sigv4::http_request::SigningParams<'_> = v4::SigningParams::builder()
        .identity(&identity)
        .region(region)
        .name(service)
        .time(SystemTime::now())
        .settings(settings)
        .build()
        .expect("all SigningParams fields are set")
        .into();

    let url = request.url().to_string();

    // reqwest adds `host` only on the wire, but SigV4 canonicalization needs it now.
    let host_value: String = {
        let h = request.url().host_str().unwrap_or("");
        match request.url().port() {
            Some(p) => format!("{h}:{p}"),
            None => h.to_string(),
        }
    };

    let existing: Vec<(String, String)> = request
        .headers()
        .iter()
        .filter_map(|(name, value)| {
            value
                .to_str()
                .ok()
                .map(|v| (name.as_str().to_string(), v.to_string()))
        })
        .collect();

    let mut header_pairs: Vec<(&str, &str)> = vec![("host", host_value.as_str())];
    for (k, v) in &existing {
        header_pairs.push((k.as_str(), v.as_str()));
    }

    let signable = SignableRequest::new(
        request.method().as_str(),
        &url,
        header_pairs.into_iter(),
        // Every signed request is a bodiless GET.
        SignableBody::Bytes(&[]),
    )?;

    let (instructions, _signature) = sign(signable, &params)?.into_parts();

    for (name, value) in instructions.headers() {
        let header_name = name
            .parse::<reqwest::header::HeaderName>()
            .expect("aws-sigv4 always emits valid header names");
        let header_value = reqwest::header::HeaderValue::from_str(value)
            .expect("aws-sigv4 always emits ASCII-safe header values");
        request.headers_mut().insert(header_name, header_value);
    }

    Ok(request)
}

#[cfg(test)]
#[path = "sigv4_tests.rs"]
mod tests;
