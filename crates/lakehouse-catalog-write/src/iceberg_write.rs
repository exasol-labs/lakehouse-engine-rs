//! The export's Iceberg REST writes: create the namespace, create or stage the table, and commit a
//! writer's data files exactly once. A commit whose answer was lost is checked by reloading the
//! table and looking for the writer's tag before it is sent again (decision [2]).

use crate::retry::{DEFAULT_RETRIES, Retries, commit_retries};
use crate::snapshot_capture::{CapturedCommit, capture_fast_append, creation_updates};
use crate::vended_refresh::{VendedCredentials, VendedTable};
use exasol_udf_sdk::error::UdfError;
use iceberg::io::FileIO;
use iceberg::spec::{DataFile, Schema, TableMetadata};
use iceberg::table::Table;
use iceberg::{NamespaceIdent, Runtime, TableIdent, TableRequirement};
use iceberg_catalog_rest::{
    CommitTableRequest, CreateNamespaceRequest, CreateTableRequest, LoadTableResult,
};
use lakehouse_catalog::{
    CatalogResponse, CatalogSendError, CatalogSession, ConnectionCreds, EpochClock, RepeatSafety,
    StaticStoreAddress, StorageBackend, StorageCreds, namespace_tables_path,
    resolve_vended_storage, scheme_of, system_clock, table_ident_path, vended_s3_credentials,
};
use serde::Serialize;
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

const EXPORT_ID_PROPERTY: &str = "lakehouse.export-id";
const EXPORT_WRITER_PROPERTY: &str = "lakehouse.export-writer";
/// Concurrent writers of one export all commit to the table it creates, so it allows many more
/// commit retries than Iceberg's default of 4.
const CREATED_TABLE_PROPERTIES: [(&str, &str); 2] =
    [("format-version", "2"), ("commit.retry.num-retries", "20")];

/// Where an export table's storage credentials come from.
pub enum WriteStorage {
    /// The CONNECTION's own storage keys, never refreshed.
    Static(StorageBackend),
    /// Credentials the catalog vends per table, renewed before they expire. The CONNECTION
    /// contributes at most the store's address.
    Vended(StaticStoreAddress),
}

impl WriteStorage {
    /// Vended per table when the CONNECTION sets `use_vended_credentials`, else its own keys.
    pub fn of(creds: &ConnectionCreds, allow_http: bool) -> Self {
        if creds.use_vended_credentials {
            Self::Vended(StaticStoreAddress::from(creds))
        } else {
            Self::Static(StorageCreds::from(creds).backend(allow_http))
        }
    }
}

/// Identifies one writer's commit: the export statement's ID and the writer's own ID. The snapshot
/// a writer commits carries both in its summary, so a writer that lost a commit's answer can tell
/// whether the commit landed, and the export's tables show which statement wrote them.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CommitTag {
    /// The export statement's ID, the `lakehouse.export-id` of every snapshot it commits.
    pub export_id: String,
    /// The writer's own ID, the `lakehouse.export-writer` of its snapshot.
    pub writer_id: String,
}

impl CommitTag {
    fn summary(&self) -> HashMap<String, String> {
        HashMap::from([
            (EXPORT_ID_PROPERTY.to_string(), self.export_id.clone()),
            (EXPORT_WRITER_PROPERTY.to_string(), self.writer_id.clone()),
        ])
    }

    fn is_on(&self, metadata: &TableMetadata) -> bool {
        metadata.snapshots().any(|snapshot| {
            let summary = &snapshot.summary().additional_properties;
            summary.get(EXPORT_ID_PROPERTY) == Some(&self.export_id)
                && summary.get(EXPORT_WRITER_PROPERTY) == Some(&self.writer_id)
        })
    }
}

/// A table the catalog holds, opened for a writer: its FileIO signs with the table's storage
/// credentials, which renew themselves when the catalog vends them.
pub struct WriteTable {
    table: Table,
    credentials: TableCredentials,
}

/// A table staged for `SINGLE_COMMIT`: invisible in the catalog until [`TableWriteSession::
/// commit_staged`] creates it together with its first snapshot.
pub struct StagedTable(WriteTable);

enum TableCredentials {
    Static(Vec<String>),
    Vended(VendedCredentials),
}

impl WriteTable {
    /// The iceberg table, whose FileIO signs with the table's storage credentials.
    pub fn table(&self) -> &Table {
        &self.table
    }

    /// Every storage credential value the table's FileIO has signed with, for value-based error
    /// redaction.
    pub fn secret_values(&self) -> Vec<String> {
        match &self.credentials {
            TableCredentials::Static(secrets) => secrets.clone(),
            TableCredentials::Vended(vended) => vended.secret_values(),
        }
    }
}

impl StagedTable {
    /// The staged table, which the catalog does not list until `commit_staged` creates it.
    pub fn table(&self) -> &Table {
        self.0.table()
    }

    /// See [`WriteTable::secret_values`].
    pub fn secret_values(&self) -> Vec<String> {
        self.0.secret_values()
    }
}

/// A commit that did not end with the writer's snapshot in the table.
#[derive(Debug)]
pub struct CommitError {
    message: String,
    outcome_unknown: bool,
}

impl CommitError {
    fn new(table: &TableIdent, cause: impl fmt::Display, outcome_unknown: bool) -> Self {
        let outcome = if outcome_unknown {
            "the outcome of its last request is unknown, so the table may hold the data files"
        } else {
            "the catalog did not apply it"
        };
        Self {
            message: format!(
                "the commit of the data files to table {} failed: {cause}; {outcome}",
                table
            ),
            outcome_unknown,
        }
    }

    /// Whether the catalog may have applied the commit, in which case the writer must keep its
    /// data files.
    pub fn outcome_unknown(&self) -> bool {
        self.outcome_unknown
    }
}

impl fmt::Display for CommitError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl From<CommitError> for UdfError {
    fn from(error: CommitError) -> Self {
        UdfError::User(error.message)
    }
}

/// What one state-changing request came to. Each cause names the catalog and is redacted.
enum Sent {
    /// 2xx, with the answer body.
    Applied(Vec<u8>),
    /// 409: the catalog holds a state the request conflicts with.
    Conflict(String),
    /// 429 or 503: the catalog did not apply the request and may accept it later.
    Busy(String),
    /// 500: the catalog may have applied the request.
    ServerError(String),
    /// 502, 504, or no complete answer after the request went out: the catalog may have applied it.
    NoAnswer(String),
    /// Any other status: the catalog did not apply the request, and a repeat fails alike.
    Refused(String),
    /// Nothing reached the catalog, such as a refused connection: it did not apply the request,
    /// and a repeat may reach it.
    Unsent(String),
}

/// How one commit request ended, short of a failure that ends the commit.
enum CommitAttempt {
    Committed,
    Retry(Retryable),
}

/// Where a staged table stands after a commit attempt that did not succeed.
enum StagedOutcome {
    Created,
    CreatedElsewhere,
    Absent,
}

/// A failed commit attempt that the loop may repeat once it has checked the table.
struct Retryable {
    cause: String,
    outcome_unknown: bool,
}

/// The Iceberg REST writes of one export writer, through its one catalog session. Namespace
/// creation, table creation, and the commit are not safe to repeat blindly, so the session sends
/// each once and the operation here checks its outcome before it repeats it, under one bounded,
/// jittered backoff (decision [9]).
pub struct TableWriteSession {
    session: Arc<CatalogSession>,
    storage: WriteStorage,
    allow_http: bool,
    clock: EpochClock,
}

impl TableWriteSession {
    /// `allow_http` is the export's consent to plain HTTP, which a vended storage endpoint needs.
    pub fn new(session: CatalogSession, storage: WriteStorage, allow_http: bool) -> Self {
        Self {
            session: Arc::new(session),
            storage,
            allow_http,
            clock: system_clock(),
        }
    }

    /// The catalog address, for an error that names the catalog.
    pub fn catalog_uri(&self) -> &str {
        self.session.catalog_uri()
    }

    /// Refuses `table` when the catalog already holds it, because the export only creates tables.
    pub async fn refuse_existing_table(&self, table: &TableIdent) -> Result<(), UdfError> {
        if self.session.table_exists(table).await? {
            return Err(existing_table_error(table));
        }
        Ok(())
    }

    /// Creates each level of `namespace` that is missing, top-down. A level that exists answers
    /// HTTP 409, which counts as created, so a repeat after an unknown outcome is safe.
    pub async fn ensure_namespace(&self, namespace: &NamespaceIdent) -> Result<(), UdfError> {
        let levels = namespace.as_ref();
        for depth in 1..=levels.len() {
            let level = NamespaceIdent::from_vec(levels[..depth].to_vec())
                .map_err(|error| UdfError::User(error.to_string()))?;
            self.create_namespace_level(&level).await?;
        }
        Ok(())
    }

    /// The table with every snapshot, opened for writing.
    pub async fn load_table(&self, table: &TableIdent) -> Result<WriteTable, UdfError> {
        let answer = self.session.load_table_with_all_snapshots(table).await?;
        self.open(table, answer)
    }

    /// Creates the table as format version 2, tagged with the export's ID. When another writer of
    /// the same export created it first, also after an unknown outcome of this request, the
    /// writer gets that table; a table of anything else is refused.
    pub async fn create_table(
        &self,
        table: &TableIdent,
        schema: Schema,
        export_id: &str,
    ) -> Result<WriteTable, UdfError> {
        let body = encoded(table, &creation_request(table, schema, export_id))?;
        let mut retries = Retries::new(DEFAULT_RETRIES);
        loop {
            let cause = match self
                .send(
                    &namespace_tables_path(&table.namespace),
                    &body,
                    RepeatSafety::Unsafe,
                )
                .await
            {
                Sent::Applied(answer) => return self.open(table, self.read_table(table, &answer)?),
                Sent::Busy(cause) | Sent::Unsent(cause) => cause,
                Sent::Conflict(cause) | Sent::ServerError(cause) | Sent::NoAnswer(cause) => {
                    if let Some(created) = self.created_by_export(table, export_id).await? {
                        return Ok(created);
                    }
                    cause
                }
                Sent::Refused(cause) => return Err(creation_error(table, &cause)),
            };
            if !retries.wait_for_next().await {
                return Err(exhausted_creation_error(table, &retries, &cause));
            }
        }
    }

    /// Stages the table for `SINGLE_COMMIT`: the catalog prepares its metadata and location but
    /// does not create it. A staged table reserves nothing, so the request is safe to repeat. A
    /// catalog that refuses staged creation, or ignores it and creates the table, is refused.
    pub async fn stage_table(
        &self,
        table: &TableIdent,
        schema: Schema,
        export_id: &str,
    ) -> Result<StagedTable, UdfError> {
        let mut request = creation_request(table, schema, export_id);
        request.stage_create = Some(true);
        let body = encoded(table, &request)?;
        let answer = match self
            .send(
                &namespace_tables_path(&table.namespace),
                &body,
                RepeatSafety::Safe,
            )
            .await
        {
            Sent::Applied(answer) => answer,
            Sent::Conflict(cause)
            | Sent::Busy(cause)
            | Sent::ServerError(cause)
            | Sent::NoAnswer(cause)
            | Sent::Refused(cause)
            | Sent::Unsent(cause) => return Err(staged_creation_error(table, cause)),
        };
        let staged = self.read_table(table, &answer)?;
        if self.session.table_exists(table).await? {
            return Err(UdfError::User(format!(
                "the catalog {} ignored staged creation (stage-create): table {} became visible \
                 before its commit, so SINGLE_COMMIT cannot make it appear only with its data",
                self.catalog_uri(),
                table
            )));
        }
        Ok(StagedTable(self.open(table, staged)?))
    }

    /// Adds `files` to `table` in one snapshot tagged with `tag`. A conflict (HTTP 409), a busy
    /// catalog (429, 503), a request that never reached the catalog, or a lost answer (502, 504, a
    /// broken transport) waits, then reloads the table: a snapshot with the tag means the commit
    /// landed, else the snapshot is rebuilt on the reloaded metadata and sent again, up to the
    /// table's `commit.retry.num-retries` times. Every other answer fails at once.
    pub async fn append_files(
        &self,
        table: &WriteTable,
        files: Vec<DataFile>,
        tag: &CommitTag,
    ) -> Result<(), CommitError> {
        let ident = table.table.identifier();
        let mut retries = Retries::new(commit_retries(table.table.metadata().properties()));
        let mut base = table.table.clone();
        let mut last_outcome_unknown = false;
        loop {
            let captured = capture_fast_append(&base, files.clone(), tag.summary())
                .await
                .map_err(|error| CommitError::new(ident, error, last_outcome_unknown))?;
            let body = commit_request(ident, captured.requirements, captured.updates)
                .map_err(|error| CommitError::new(ident, error, last_outcome_unknown))?;
            let failure = match self.send_commit(ident, &body).await? {
                CommitAttempt::Committed => return Ok(()),
                CommitAttempt::Retry(failure) => failure,
            };
            last_outcome_unknown = failure.outcome_unknown;
            // Reloading after the wait keeps a commit that lands during the wait from making the
            // rebuilt snapshot stale, which would grow every writer's wait under contention.
            let may_retry = retries.wait_for_next().await;
            let reloaded = self
                .session
                .load_table_with_all_snapshots(ident)
                .await
                .map_err(|error| CommitError::new(ident, error, failure.outcome_unknown))?;
            if tag.is_on(&reloaded.metadata) {
                return Ok(());
            }
            if !may_retry {
                return Err(exhausted_commit_error(ident, &retries, &failure));
            }
            base = rebuilt(&base, reloaded)
                .map_err(|error| CommitError::new(ident, error, failure.outcome_unknown))?;
        }
    }

    /// Creates the staged table together with one snapshot of `files` tagged with `tag`, in one
    /// commit whose only requirement is that the table does not exist (`assert-create`). After a
    /// conflict, a busy catalog, a request that never reached the catalog, or a lost answer, a
    /// table that holds the tag means the commit landed, a table without it was created by another
    /// statement, and no table means the commit is sent again.
    pub async fn commit_staged(
        &self,
        staged: &StagedTable,
        files: Vec<DataFile>,
        tag: &CommitTag,
    ) -> Result<(), CommitError> {
        let table = staged.table();
        let ident = table.identifier();
        let body = staged_commit(table, files, tag)
            .await
            .map_err(|error| CommitError::new(ident, error, false))?;
        let mut retries = Retries::new(commit_retries(table.metadata().properties()));
        loop {
            let failure = match self.send_commit(ident, &body).await? {
                CommitAttempt::Committed => return Ok(()),
                CommitAttempt::Retry(failure) => failure,
            };
            match self
                .staged_outcome(ident, tag)
                .await
                .map_err(|error| CommitError::new(ident, error, failure.outcome_unknown))?
            {
                StagedOutcome::Created => return Ok(()),
                StagedOutcome::CreatedElsewhere => {
                    let cause = format!("table {} was created by another statement", ident);
                    return Err(CommitError::new(ident, cause, false));
                }
                StagedOutcome::Absent => {}
            }
            if !retries.wait_for_next().await {
                return Err(exhausted_commit_error(ident, &retries, &failure));
            }
        }
    }

    async fn send_commit(
        &self,
        table: &TableIdent,
        body: &serde_json::Value,
    ) -> Result<CommitAttempt, CommitError> {
        let retry = |cause, outcome_unknown| {
            Ok(CommitAttempt::Retry(Retryable {
                cause,
                outcome_unknown,
            }))
        };
        match self
            .send(&table_ident_path(table), body, RepeatSafety::Unsafe)
            .await
        {
            Sent::Applied(_) => Ok(CommitAttempt::Committed),
            Sent::Conflict(cause) | Sent::Busy(cause) | Sent::Unsent(cause) => retry(cause, false),
            Sent::NoAnswer(cause) => retry(cause, true),
            Sent::ServerError(cause) => Err(CommitError::new(table, cause, true)),
            Sent::Refused(cause) => Err(CommitError::new(table, cause, false)),
        }
    }

    async fn create_namespace_level(&self, level: &NamespaceIdent) -> Result<(), UdfError> {
        let request = CreateNamespaceRequest {
            namespace: level.clone(),
            properties: HashMap::new(),
        };
        let body = serde_json::to_value(&request).map_err(|error| {
            UdfError::User(format!(
                "could not encode the creation of namespace {}: {error}",
                level
            ))
        })?;
        let mut retries = Retries::new(DEFAULT_RETRIES);
        loop {
            let cause = match self.send("namespaces", &body, RepeatSafety::Unsafe).await {
                Sent::Applied(_) | Sent::Conflict(_) => return Ok(()),
                Sent::Busy(cause)
                | Sent::Unsent(cause)
                | Sent::ServerError(cause)
                | Sent::NoAnswer(cause) => cause,
                Sent::Refused(cause) => {
                    return Err(UdfError::User(format!(
                        "creating namespace {} failed: {cause}",
                        level
                    )));
                }
            };
            if !retries.wait_for_next().await {
                return Err(UdfError::User(format!(
                    "creating namespace {} failed after {} attempts: {cause}",
                    level,
                    retries.attempts()
                )));
            }
        }
    }

    /// The table another writer of the same export created, `None` when no table exists.
    async fn created_by_export(
        &self,
        table: &TableIdent,
        export_id: &str,
    ) -> Result<Option<WriteTable>, UdfError> {
        if !self.session.table_exists(table).await? {
            return Ok(None);
        }
        let existing = self.session.load_table_with_all_snapshots(table).await?;
        if existing
            .metadata
            .properties()
            .get(EXPORT_ID_PROPERTY)
            .map(String::as_str)
            != Some(export_id)
        {
            return Err(existing_table_error(table));
        }
        self.open(table, existing).map(Some)
    }

    /// What a staged commit whose answer was not success came to. A table without the tag was
    /// created by another statement, so this commit's `assert-create` cannot have applied.
    async fn staged_outcome(
        &self,
        table: &TableIdent,
        tag: &CommitTag,
    ) -> Result<StagedOutcome, UdfError> {
        if !self.session.table_exists(table).await? {
            return Ok(StagedOutcome::Absent);
        }
        let created = self.session.load_table_with_all_snapshots(table).await?;
        Ok(if tag.is_on(&created.metadata) {
            StagedOutcome::Created
        } else {
            StagedOutcome::CreatedElsewhere
        })
    }

    async fn send(&self, path: &str, body: &serde_json::Value, safety: RepeatSafety) -> Sent {
        match self.session.post(path, body, safety).await {
            Ok(answer) => self.classify(answer),
            Err(CatalogSendError::OutcomeUnknown(cause)) => Sent::NoAnswer(cause),
            Err(CatalogSendError::Unsent(cause)) => Sent::Unsent(cause),
        }
    }

    fn classify(&self, answer: CatalogResponse) -> Sent {
        if (200..300).contains(&answer.status) {
            return Sent::Applied(answer.body);
        }
        let cause = format!(
            "the catalog {} answered HTTP {}: {}",
            self.catalog_uri(),
            answer.status,
            self.session.quote(&answer.body)
        );
        match answer.status {
            409 => Sent::Conflict(cause),
            429 | 503 => Sent::Busy(cause),
            500 => Sent::ServerError(cause),
            502 | 504 => Sent::NoAnswer(cause),
            _ => Sent::Refused(cause),
        }
    }

    /// A create or stage answer. The reason for an unreadable one quotes none of it, because it
    /// can carry vended credentials.
    fn read_table(&self, table: &TableIdent, answer: &[u8]) -> Result<LoadTableResult, UdfError> {
        serde_json::from_slice(answer).map_err(|error| {
            UdfError::User(format!(
                "the catalog {} answered the creation of table {} with no table: invalid JSON at \
                 line {}, column {}",
                self.catalog_uri(),
                table,
                error.line(),
                error.column()
            ))
        })
    }

    /// The table of a load, create, or stage answer, with a FileIO over its location. Only an
    /// `s3` or `s3a` location is accepted, before any storage is configured.
    fn open(&self, table: &TableIdent, answer: LoadTableResult) -> Result<WriteTable, UdfError> {
        let location = answer.metadata.location().to_string();
        let scheme = scheme_of(&location);
        if !matches!(scheme.as_str(), "s3" | "s3a") {
            return Err(UdfError::User(format!(
                "table {} has the location {location}, whose scheme {scheme} is no S3-compatible \
                 storage; the export writes only to s3:// or s3a:// locations",
                table
            )));
        }
        let (file_io, credentials) = self.storage_of(table, &answer, location)?;
        let table = iceberg_table(table, answer, file_io)
            .map_err(|error| UdfError::User(format!("could not open table {}: {error}", table)))?;
        Ok(WriteTable { table, credentials })
    }

    /// The FileIO of the table at `location` and the credentials it signs with: the CONNECTION's
    /// own keys, or the credentials `answer` vends, seeded into a source that renews them.
    fn storage_of(
        &self,
        table: &TableIdent,
        answer: &LoadTableResult,
        location: String,
    ) -> Result<(FileIO, TableCredentials), UdfError> {
        let address = match &self.storage {
            WriteStorage::Static(backend) => {
                let secrets = backend.secret_values().into_iter().map(String::from);
                return Ok((
                    backend.file_io(),
                    TableCredentials::Static(secrets.collect()),
                ));
            }
            WriteStorage::Vended(address) => address,
        };
        let seed = vended_s3_credentials(answer, &location)?;
        let vended = VendedTable {
            session: self.session.clone(),
            table: table.clone(),
            location,
            allow_http: self.allow_http,
        };
        let credentials = VendedCredentials::seeded(vended, seed, self.clock.clone())?;
        let location = answer.metadata.location();
        let backend = resolve_vended_storage(answer, location, self.allow_http, address)?;
        let file_io = backend.file_io_with_credential_loader(credentials.loader())?;
        Ok((file_io, TableCredentials::Vended(credentials)))
    }
}

fn creation_request(table: &TableIdent, schema: Schema, export_id: &str) -> CreateTableRequest {
    let mut properties: HashMap<String, String> = CREATED_TABLE_PROPERTIES
        .iter()
        .map(|(key, value)| (key.to_string(), value.to_string()))
        .collect();
    properties.insert(EXPORT_ID_PROPERTY.to_string(), export_id.to_string());
    CreateTableRequest {
        name: table.name.clone(),
        location: None,
        schema,
        partition_spec: None,
        write_order: None,
        stage_create: None,
        properties,
    }
}

fn encoded(table: &TableIdent, request: &impl Serialize) -> Result<serde_json::Value, UdfError> {
    serde_json::to_value(request).map_err(|error| {
        UdfError::User(format!(
            "could not encode the request for table {}: {error}",
            table
        ))
    })
}

fn commit_request(
    table: &TableIdent,
    requirements: Vec<TableRequirement>,
    updates: Vec<iceberg::TableUpdate>,
) -> Result<serde_json::Value, serde_json::Error> {
    serde_json::to_value(CommitTableRequest {
        identifier: Some(table.clone()),
        requirements,
        updates,
    })
}

/// The one commit that creates the staged table with its snapshot: the creation updates, then the
/// snapshot's, under `assert-create` alone.
async fn staged_commit(
    staged: &Table,
    files: Vec<DataFile>,
    tag: &CommitTag,
) -> Result<serde_json::Value, String> {
    let CapturedCommit { updates, .. } = capture_fast_append(staged, files, tag.summary())
        .await
        .map_err(|error| error.to_string())?;
    let mut creation = creation_updates(staged.metadata()).map_err(|error| error.to_string())?;
    creation.extend(updates);
    commit_request(
        staged.identifier(),
        vec![TableRequirement::NotExist],
        creation,
    )
    .map_err(|error| error.to_string())
}

/// `base` on the reloaded metadata, keeping `base`'s FileIO, so the table's storage credentials
/// also sign the rebuilt snapshot's manifests.
fn rebuilt(base: &Table, reloaded: LoadTableResult) -> iceberg::Result<Table> {
    iceberg_table(base.identifier(), reloaded, base.file_io().clone())
}

/// The table `answer` describes, named `table`, reading and writing through `file_io`.
fn iceberg_table(
    table: &TableIdent,
    answer: LoadTableResult,
    file_io: FileIO,
) -> iceberg::Result<Table> {
    let mut builder = Table::builder()
        .identifier(table.clone())
        .metadata(answer.metadata)
        .file_io(file_io)
        .runtime(Runtime::try_current()?);
    if let Some(metadata_location) = answer.metadata_location {
        builder = builder.metadata_location(metadata_location);
    }
    builder.build()
}

fn staged_creation_error(table: &TableIdent, cause: impl fmt::Display) -> UdfError {
    UdfError::User(format!(
        "staged creation of table {} failed: {cause}; SINGLE_COMMIT needs a catalog that accepts \
         staged table creation (stage-create)",
        table
    ))
}

fn existing_table_error(table: &TableIdent) -> UdfError {
    UdfError::User(format!(
        "table {table} already exists; the export only creates new tables"
    ))
}

fn creation_error(table: &TableIdent, cause: &str) -> UdfError {
    UdfError::User(format!("creating table {} failed: {cause}", table))
}

fn exhausted_creation_error(table: &TableIdent, retries: &Retries, cause: &str) -> UdfError {
    UdfError::User(format!(
        "creating table {} failed after {} attempts: {cause}",
        table,
        retries.attempts()
    ))
}

fn exhausted_commit_error(table: &TableIdent, retries: &Retries, last: &Retryable) -> CommitError {
    CommitError::new(
        table,
        format!(
            "{} attempts did not commit, the last because {}",
            retries.attempts(),
            last.cause
        ),
        last.outcome_unknown,
    )
}

#[cfg(test)]
#[path = "iceberg_write_tests.rs"]
mod tests;
