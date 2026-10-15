//! The Parquet row filter flattens a predicate error to text, so a scan function that raises a
//! user error also records it on its session, where the scan dispatcher reads it back.

use datafusion::execution::context::SessionContext;
use std::sync::OnceLock;

/// A session config extension, so it is scoped to one session, i.e. one scan invocation.
#[derive(Debug, Default)]
pub(crate) struct RaisedFailure(OnceLock<String>);

impl RaisedFailure {
    /// Partitions run concurrently but every failure names the same defect, so the first suffices.
    pub(crate) fn record(&self, failure: String) {
        let _ = self.0.set(failure);
    }
}

pub(crate) fn session_raised_failure(session: &SessionContext) -> Option<String> {
    session
        .state()
        .config()
        .get_extension::<RaisedFailure>()?
        .0
        .get()
        .cloned()
}
