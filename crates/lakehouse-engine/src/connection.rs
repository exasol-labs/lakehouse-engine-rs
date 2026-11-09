//! Reads an Exasol CONNECTION password into `ConnectionCreds` and runs the field checks that do
//! not depend on the catalog kind, for the virtual schema adapter and the export alike.
//! Credential values never appear in an error message produced by this module.

use exasol_udf_sdk::error::UdfError;
use lakehouse_catalog::{ConnectionCreds, StorageCreds};

/// The password of CONNECTION `name` as a JSON object.
pub(crate) fn password_object(name: &str, password: &str) -> Result<serde_json::Value, UdfError> {
    let not_an_object = || {
        UdfError::User(format!(
            "CONNECTION '{name}' password is not a valid JSON object"
        ))
    };
    let json: serde_json::Value = serde_json::from_str(password).map_err(|_| not_an_object())?;
    if !json.is_object() {
        return Err(not_an_object());
    }
    Ok(json)
}

pub(crate) fn parse_creds(json: &serde_json::Value) -> ConnectionCreds {
    let text = |key: &str| {
        json.get(key)
            .and_then(|value| value.as_str())
            .filter(|value| !value.is_empty())
            .map(str::to_string)
    };
    let flag = |key: &str| json.get(key).and_then(|v| v.as_bool()).unwrap_or(false);
    let StorageCreds {
        endpoint,
        region,
        access_key,
        secret_key,
        session_token,
        path_style,
        account_name,
        account_key,
        sas_token,
    } = StorageCreds::from_json(json);
    ConnectionCreds {
        warehouse: text("warehouse").unwrap_or_default(),
        endpoint,
        region,
        access_key,
        secret_key,
        session_token,
        path_style,
        use_sigv4: flag("use_sigv4"),
        use_vended_credentials: flag("use_vended_credentials"),
        token: text("token"),
        client_id: text("client_id"),
        client_secret: text("client_secret"),
        oauth2_server_uri: text("oauth2_server_uri"),
        scope: text("scope"),
        account_name,
        account_key,
        sas_token,
        aws_assume_role_arn: text("aws_assume_role_arn"),
        aws_external_id: text("aws_external_id"),
        aws_sts_endpoint: text("aws_sts_endpoint"),
    }
}

/// The checks of CONNECTION `name` that hold under every catalog kind: Azure storage, role
/// assumption, SigV4 signing, exclusive catalog auth, OAuth2, and the path style of an endpoint.
pub(crate) fn validate_shared_creds(
    name: &str,
    creds: &ConnectionCreds,
    address: &str,
) -> Result<(), UdfError> {
    validate_azure_storage_creds(name, creds)?;
    validate_assume_role_creds(name, creds)?;
    validate_sigv4_creds(name, creds, address)?;
    validate_exclusive_catalog_auth_creds(name, creds)?;
    validate_oauth2_creds(name, creds)?;
    validate_path_style_with_endpoint(name, creds)?;
    Ok(())
}

fn validate_azure_storage_creds(name: &str, creds: &ConnectionCreds) -> Result<(), UdfError> {
    let azure_fields = supplied_azure_fields(creds);
    if azure_fields.is_empty() {
        return Ok(());
    }

    let s3_fields = supplied_s3_fields(creds);
    if !s3_fields.is_empty() {
        return Err(UdfError::User(format!(
            "CONNECTION '{name}' supplies Azure storage credential field(s) {} together \
             with S3 storage credential field(s) {}; Azure and S3 storage credentials \
             cannot both be supplied on one CONNECTION",
            azure_fields.join(", "),
            s3_fields.join(", ")
        )));
    }

    let mut defects: Vec<&str> = Vec::new();
    if creds.account_name.is_none() {
        defects.push("account_name is missing");
    }
    match (creds.account_key.is_some(), creds.sas_token.is_some()) {
        (true, true) => defects.push("account_key and sas_token are both present"),
        (false, false) => defects.push("neither account_key nor sas_token is present"),
        (true, false) | (false, true) => {}
    }
    if !defects.is_empty() {
        return Err(UdfError::User(format!(
            "CONNECTION '{name}' supplies Azure storage credential field(s) {}; an Azure \
             CONNECTION requires account_name and exactly one of account_key and sas_token: {}",
            azure_fields.join(", "),
            defects.join("; ")
        )));
    }
    Ok(())
}

/// Runs before the SigV4 check so a role CONNECTION missing its key pair is told
/// that the role needs it, not only that SigV4 does.
fn validate_assume_role_creds(name: &str, creds: &ConnectionCreds) -> Result<(), UdfError> {
    if creds.assume_role_arn().is_none() {
        let orphaned = flagged_fields(&[
            ("aws_external_id", creds.aws_external_id.is_some()),
            ("aws_sts_endpoint", creds.aws_sts_endpoint.is_some()),
        ]);
        if orphaned.is_empty() {
            return Ok(());
        }
        return Err(UdfError::User(format!(
            "CONNECTION '{name}' supplies field(s) {} without aws_assume_role_arn; they \
             configure AWS IAM role assumption, so each requires aws_assume_role_arn",
            orphaned.join(", ")
        )));
    }
    let missing = flagged_fields(&[
        ("access_key", creds.access_key.is_empty()),
        ("secret_key", creds.secret_key.is_empty()),
    ]);
    if missing.is_empty() {
        return Ok(());
    }
    Err(UdfError::User(format!(
        "CONNECTION '{name}' names aws_assume_role_arn but is missing field(s) {}; the \
         role is assumed with the CONNECTION's own access_key and secret_key, and no \
         ambient AWS credential, such as an environment variable or an instance \
         profile, is read",
        missing.join(", ")
    )))
}

fn flagged_fields<'a>(flags: &[(&'a str, bool)]) -> Vec<&'a str> {
    flags
        .iter()
        .filter_map(|&(field, flag)| flag.then_some(field))
        .collect()
}

fn validate_sigv4_creds(
    name: &str,
    creds: &ConnectionCreds,
    catalog_uri: &str,
) -> Result<(), UdfError> {
    if creds.use_sigv4 && creds.has_catalog_auth() {
        return Err(UdfError::User(format!(
            "CONNECTION '{name}' enables SigV4 signing together with catalog \
             token/OAuth authentication; these cannot both be enabled"
        )));
    }

    if creds.use_sigv4 {
        let mut missing: Vec<&str> = Vec::new();
        if creds.access_key.is_empty() {
            missing.push("access_key");
        }
        if creds.secret_key.is_empty() {
            missing.push("secret_key");
        }
        if creds.sigv4_signing_region(catalog_uri).is_none() {
            missing.push("region");
        }
        if !missing.is_empty() {
            let glue_hint = if missing.contains(&"region") {
                "; a CONNECTION whose address is a standard AWS Glue endpoint of the form \
                 https://glue.<region>.amazonaws.com can omit region"
            } else {
                ""
            };
            return Err(UdfError::User(format!(
                "CONNECTION '{name}' enables SigV4 signing but is missing field(s) \
                 required when SigV4 signing is enabled: {}{glue_hint}",
                missing.join(", ")
            )));
        }
    }
    Ok(())
}

fn validate_exclusive_catalog_auth_creds(
    name: &str,
    creds: &ConnectionCreds,
) -> Result<(), UdfError> {
    if creds.token.is_some() && creds.client_id.is_some() && creds.client_secret.is_some() {
        return Err(UdfError::User(format!(
            "CONNECTION '{name}' supplies a token together with a complete \
             client_id/client_secret pair; these are mutually exclusive, \
             remove one: token, client_id, client_secret"
        )));
    }
    Ok(())
}

fn validate_oauth2_creds(name: &str, creds: &ConnectionCreds) -> Result<(), UdfError> {
    match (creds.client_id.is_some(), creds.client_secret.is_some()) {
        (true, false) => Err(UdfError::User(format!(
            "CONNECTION '{name}' OAuth2 client credentials require both \
             client_id and client_secret; missing field: client_secret"
        ))),
        (false, true) => Err(UdfError::User(format!(
            "CONNECTION '{name}' OAuth2 client credentials require both \
             client_id and client_secret; missing field: client_id"
        ))),
        _ => Ok(()),
    }
}

fn validate_path_style_with_endpoint(name: &str, creds: &ConnectionCreds) -> Result<(), UdfError> {
    if !creds.endpoint.is_empty() && creds.path_style.is_none() && !creds.use_vended_credentials {
        return Err(UdfError::User(format!(
            "CONNECTION '{name}' supplies a storage endpoint but states no path_style; \
             an absent path_style resolves to false, which discards the endpoint and \
             derives an AWS host from region instead — set path_style to true or false"
        )));
    }
    Ok(())
}

pub(crate) fn supplied_azure_fields(creds: &ConnectionCreds) -> Vec<&'static str> {
    [
        ("account_name", creds.account_name.is_some()),
        ("account_key", creds.account_key.is_some()),
        ("sas_token", creds.sas_token.is_some()),
    ]
    .into_iter()
    .filter_map(|(field, supplied)| supplied.then_some(field))
    .collect()
}

pub(crate) fn supplied_s3_fields(creds: &ConnectionCreds) -> Vec<&'static str> {
    [
        ("endpoint", !creds.endpoint.is_empty()),
        ("region", !creds.region.is_empty()),
        ("access_key", !creds.access_key.is_empty()),
        ("secret_key", !creds.secret_key.is_empty()),
        ("session_token", creds.session_token.is_some()),
    ]
    .into_iter()
    .filter_map(|(field, supplied)| supplied.then_some(field))
    .collect()
}

#[cfg(test)]
#[path = "connection_tests.rs"]
mod tests;
