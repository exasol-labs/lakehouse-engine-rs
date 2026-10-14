use aws_sdk_glue::error::ProvideErrorMetadata;
use aws_smithy_runtime_api::client::orchestrator::HttpResponse;
use aws_smithy_runtime_api::client::result::SdkError;

/// Built from the service's own status, code, and message only: the SDK's `Debug` and
/// `DisplayErrorContext` renderings embed the raw response and would echo its body.
pub(crate) enum SdkFailure<'a> {
    Service {
        status: u16,
        code: Option<&'a str>,
        message: Option<&'a str>,
    },
    TimedOut,
    Request(String),
}

/// `service` names the AWS service in the text of a response the SDK could not parse.
pub(crate) fn sdk_failure<'a, E>(
    error: &'a SdkError<E, HttpResponse>,
    service: &str,
) -> SdkFailure<'a>
where
    E: ProvideErrorMetadata,
{
    match error {
        SdkError::ServiceError(context) => SdkFailure::Service {
            status: context.raw().status().as_u16(),
            code: context.err().code(),
            message: context.err().message(),
        },
        SdkError::TimeoutError(_) => SdkFailure::TimedOut,
        SdkError::DispatchFailure(failure) => SdkFailure::Request(
            failure
                .as_connector_error()
                .map_or_else(|| "the request could not be sent".to_string(), |e| chain(e)),
        ),
        SdkError::ConstructionFailure(_) => {
            SdkFailure::Request("the request could not be built".to_string())
        }
        SdkError::ResponseError(context) => SdkFailure::Request(format!(
            "{service} returned HTTP {} with an unreadable response",
            context.raw().status().as_u16()
        )),
        _ => SdkFailure::Request("the request failed".to_string()),
    }
}

fn chain(error: &dyn std::error::Error) -> String {
    let mut description = error.to_string();
    let mut cause = error.source();
    while let Some(source) = cause {
        description.push_str(&format!(": {source}"));
        cause = source.source();
    }
    description
}
