//! DataFusion's Parquet row filter flattens any predicate error into
//! `ArrowError::ComputeError(format!("Error evaluating filter predicate: {e:?}"))`
//! (`datafusion-datasource-parquet` 54.1, `row_filter.rs`), so a scan function that raises a user
//! error also records it on its session, where the scan dispatcher reads it back.

use datafusion::execution::context::SessionContext;
use std::sync::{Arc, OnceLock};

/// A session config extension, so it is scoped to one session, i.e. one scan invocation.
#[derive(Debug, Default)]
pub(crate) struct RaisedFailure(OnceLock<String>);

impl RaisedFailure {
    /// Partitions run concurrently but every failure names the same defect, so the first suffices.
    pub(crate) fn record(&self, failure: String) {
        let _ = self.0.set(failure);
    }
}

/// The session's slot, added on first use.
pub(crate) fn raised_failure(session: &SessionContext) -> Arc<RaisedFailure> {
    let state = session.state_ref();
    let mut state = state.write();
    if let Some(slot) = state.config().get_extension::<RaisedFailure>() {
        return slot;
    }
    let slot = Arc::new(RaisedFailure::default());
    state.config_mut().set_extension(slot.clone());
    slot
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
