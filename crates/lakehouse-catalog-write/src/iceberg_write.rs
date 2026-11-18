//! The export's Iceberg REST writes: create the namespace, create or stage the table, and commit a
//! writer's data files exactly once. A commit whose answer was lost is checked by reloading the
//! table and looking for the writer's tag before it is sent again (decision [2]).

use crate::adls_storage::adls_file_io;
use crate::retry::{
    DEFAULT_RETRIES, EXPORT_RETRIES, NUM_RETRIES_PROPERTY, Retries, append_retries, commit_retries,
};
use crate::snapshot_capture::{CapturedCommit, capture_fast_append, creation_updates};
use crate::vended_refresh::{AdlsCredentials, StorageCredentials, TableCredentials, VendedTable};
use exasol_udf_sdk::error::UdfError;
use iceberg::io::FileIO;
use iceberg::spec::{DataFile, FormatVersion, Schema, TableMetadata};
use iceberg::table::Table;
use iceberg::{NamespaceIdent, Runtime, TableIdent, TableRequirement};
use iceberg_catalog_rest::{
    CommitTableRequest, CreateNamespaceRequest, CreateTableRequest, LoadTableResult,
};
use lakehouse_catalog::{
    AssumedRole, CatalogResponse, CatalogSendError, CatalogSession, ConnectionCreds, EpochClock,
    RepeatSafety, StaticStoreAddress, StorageBackend, StorageCreds, VendedBackendKind,
    VendedCredentialSet, classify_vended_scheme, namespace_tables_path, resolve_vended_storage,
    scheme_of, system_clock, table_ident_path,
};
use serde::Serialize;
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

const EXPORT_ID_PROPERTY: &str = "lakehouse.export-id";
const EXPORT_WRITER_PROPERTY: &str = "lakehouse.export-writer";

/// Where an export table's storage credentials come from. Every kind reaches the table's FileIO
/// through one credential source, which renews the credentials its own way.
pub enum WriteStorage {
    /// The CONNECTION's own storage credentials, S3 keys or an Azure account key or SAS, which
    /// do not expire.
    Static(StorageBackend),
    /// The credentials of an IAM role the CONNECTION names, assumed again before they expire.
    /// `backend` carries the store's address.
    AssumedRole {
        backend: StorageBackend,
        role: AssumedRole,
    },
    /// Credentials the catalog vends per table, renewed before they expire. The CONNECTION
    /// contributes at most the store's address.
    Vended(StaticStoreAddress),
}

impl WriteStorage {
    /// Vended per table when the CONNECTION sets `use_vended_credentials`, else the credentials of
    /// `role` when the CONNECTION names one, else its own keys.
    pub fn of(creds: &ConnectionCreds, allow_http: bool, role: Option<AssumedRole>) -> Self {
        if creds.use_vended_credentials {
            return Self::Vended(StaticStoreAddress::from(creds));
        }
        let backend = StorageCreds::from(creds).backend(allow_http);
        match role {
            Some(role) => Self::AssumedRole { backend, role },
            None => Self::Static(backend),
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

impl WriteTable {
    /// The iceberg table, whose FileIO signs with the table's storage credentials.
    pub fn table(&self) -> &Table {
        &self.table
    }

    /// The storage credential values the table's FileIO signs with now, for value-based error
    /// redaction.
    pub fn secret_values(&self) -> Vec<String> {
        self.credentials.secret_values()
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
    /// 429: the catalog did not apply the request and may accept it later.
    Busy(String),
    /// 500, 502, 503, 504, or no complete answer after the request went out: the catalog may have
    /// applied the request, as Iceberg's `CommitErrorHandler` reads them.
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

    /// Whether the catalog holds `table`.
    pub async fn table_exists(&self, table: &TableIdent) -> Result<bool, UdfError> {
        self.session.table_exists(table).await
    }

    /// Creates each level of `namespace` that the catalog does not hold, top-down. A level is
    /// looked up first and created only on a 404, so a user who may create tables but not
    /// namespaces can export into an existing one. A level created meanwhile answers HTTP 409,
    /// which counts as created, so a repeat after an unknown outcome is safe.
    pub async fn ensure_namespace(&self, namespace: &NamespaceIdent) -> Result<(), UdfError> {
        let levels = namespace.as_ref();
        for depth in 1..=levels.len() {
            let level = NamespaceIdent::from_vec(levels[..depth].to_vec())
                .map_err(|error| UdfError::User(error.to_string()))?;
            if !self.session.namespace_exists(&level).await? {
                self.create_namespace_level(&level).await?;
            }
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
                Sent::Conflict(cause) | Sent::NoAnswer(cause) => {
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
            | Sent::NoAnswer(cause)
            | Sent::Refused(cause)
            | Sent::Unsent(cause) => return Err(staged_creation_error(table, cause)),
        };
        let staged = self.read_table(table, &answer)?;
        if self.session.table_exists(table).await? {
            return Err(self.ignored_staging(table, export_id).await);
        }
        Ok(StagedTable(self.open(table, staged)?))
    }

    /// The error for a catalog that ignored `stage-create`. A table this export created is dropped,
    /// so the failed export leaves no empty table behind; else the error says to drop it.
    async fn ignored_staging(&self, table: &TableIdent, export_id: &str) -> UdfError {
        let ignored = format!(
            "the catalog {} ignored staged creation (stage-create): table {} became visible \
             before its commit, so SINGLE_COMMIT cannot make it appear only with its data",
            self.catalog_uri(),
            table
        );
        let created_by_export = match self.session.load_table_with_all_snapshots(table).await {
            Ok(created) => {
                created
                    .metadata
                    .properties()
                    .get(EXPORT_ID_PROPERTY)
                    .map(String::as_str)
                    == Some(export_id)
            }
            Err(_) => false,
        };
        let dropped = created_by_export && self.session.drop_table(table).await.is_ok();
        let outcome = if dropped {
            "the table was dropped again"
        } else {
            "the table now exists and must be dropped"
        };
        UdfError::User(format!("{ignored}; {outcome}"))
    }

    /// Adds `files`, written under `written`, to the table in one snapshot tagged with `tag`. The
    /// table is reloaded first, so the snapshot holds every commit of other writers so far. A
    /// conflict (HTTP 409), a busy catalog (429, 503), a request that never reached the catalog,
    /// or a lost answer (502, 504, a broken transport) waits, then reloads the table: a snapshot
    /// with the tag means the commit landed, else the snapshot is rebuilt on the reloaded metadata
    /// and sent again, up to the table's `commit.retry.num-retries` times, and at least 20. Every
    /// other answer fails at once.
    pub async fn append_files(
        &self,
        written: &WriteTable,
        files: Vec<DataFile>,
        tag: &CommitTag,
    ) -> Result<(), CommitError> {
        let ident = written.table.identifier();
        // A fast append asserts neither the schema nor the partition spec the files were written
        // under, so every commit pins both.
        let checked = TableShape::of(written.table.metadata());
        let reloaded = self
            .session
            .load_table_with_all_snapshots(ident)
            .await
            .map_err(|error| CommitError::new(ident, error, false))?;
        refuse_reloaded(checked, &reloaded.metadata)
            .map_err(|change| CommitError::new(ident, change, false))?;
        // The rebuilt table keeps `written`'s FileIO, so the reload's own credentials sign nothing.
        let mut base = rebuilt(&written.table, reloaded)
            .map_err(|error| CommitError::new(ident, error, false))?;
        let mut retries = Retries::new(append_retries(base.metadata().properties()));
        let mut last_outcome_unknown = false;
        loop {
            let captured = capture_fast_append(&base, files.clone(), tag.summary())
                .await
                .map_err(|error| CommitError::new(ident, error, last_outcome_unknown))?;
            let requirements = checked.pinned(captured.requirements);
            let body = commit_request(ident, requirements, captured.updates)
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
            refuse_reloaded(checked, &reloaded.metadata)
                .map_err(|change| CommitError::new(ident, change, false))?;
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
                Sent::Busy(cause) | Sent::Unsent(cause) | Sent::NoAnswer(cause) => cause,
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
            return Err(created_by_another_statement(table));
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
        let outcome = classify(answer.status);
        if outcome == Outcome::Applied {
            return Sent::Applied(answer.body);
        }
        let cause = format!(
            "the catalog {} answered HTTP {}: {}",
            self.catalog_uri(),
            answer.status,
            self.session.quote(&answer)
        );
        match outcome {
            Outcome::Applied | Outcome::Refused => Sent::Refused(cause),
            Outcome::Conflict => Sent::Conflict(cause),
            Outcome::Busy => Sent::Busy(cause),
            Outcome::MayHaveApplied => Sent::NoAnswer(cause),
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

    /// The table of a load, create, or stage answer, with a FileIO over its location. A location
    /// the storage cannot address is refused before any storage is configured.
    fn open(&self, table: &TableIdent, answer: LoadTableResult) -> Result<WriteTable, UdfError> {
        let location = answer.metadata.location().to_string();
        let vended_kind = refuse_unaddressable(&self.storage, self.allow_http, table, &location)?;
        let (file_io, credentials) = self.storage_of(table, &answer, location, vended_kind)?;
        let table = iceberg_table(table, answer, file_io)
            .map_err(|error| UdfError::User(format!("could not open table {}: {error}", table)))?;
        Ok(WriteTable { table, credentials })
    }

    /// The FileIO of the table at `location` and the credential source it signs with: the
    /// CONNECTION's own credentials, the credentials of its role, or the credentials `answer`
    /// vends, of the backend `vended_kind` selects. Each is seeded into a source that renews it its
    /// own way.
    fn storage_of(
        &self,
        table: &TableIdent,
        answer: &LoadTableResult,
        location: String,
        vended_kind: Option<VendedBackendKind>,
    ) -> Result<(FileIO, TableCredentials), UdfError> {
        match &self.storage {
            WriteStorage::Static(StorageBackend::Adls { cred, .. }) => {
                Ok(adls_storage(&location, AdlsCredentials::connection(cred)?))
            }
            WriteStorage::Static(backend @ StorageBackend::S3(props)) => {
                let keys = StorageCredentials::keys(
                    &props.access_key,
                    &props.secret_key,
                    props.session_token.as_deref(),
                );
                s3_storage(backend, keys)
            }
            WriteStorage::AssumedRole { backend, role } => {
                s3_storage(backend, StorageCredentials::role(role.clone()))
            }
            WriteStorage::Vended(address) => {
                let vended = VendedTable {
                    session: self.session.clone(),
                    table: table.clone(),
                    location: location.clone(),
                    allow_http: self.allow_http,
                };
                let set = VendedCredentialSet::of(answer, &location);
                if let Some(VendedBackendKind::Adls) = vended_kind {
                    let seed = set.adls_sas(&location)?;
                    let credentials = AdlsCredentials::vended(vended, seed, self.clock.clone())?;
                    return Ok(adls_storage(&location, credentials));
                }
                let seed = set.s3(&location)?;
                let credentials = StorageCredentials::vended(vended, seed, self.clock.clone())?;
                let backend = resolve_vended_storage(answer, &location, self.allow_http, address)?;
                s3_storage(&backend, credentials)
            }
        }
    }
}

/// Refuses a location the storage cannot address: the CONNECTION's S3 storage, by keys or a role,
/// addresses `s3` and `s3a`, its Azure storage `abfss`, and vended storage all four schemes,
/// `abfs` only under the export's `ALLOW_HTTP`. Returns the backend a vended location selects.
fn refuse_unaddressable(
    storage: &WriteStorage,
    allow_http: bool,
    table: &TableIdent,
    location: &str,
) -> Result<Option<VendedBackendKind>, UdfError> {
    let scheme = scheme_of(location);
    let storage = match storage {
        WriteStorage::Static(backend) | WriteStorage::AssumedRole { backend, .. } => {
            if backend.addresses_scheme(&scheme) {
                return Ok(None);
            }
            match backend {
                StorageBackend::S3(_) => "the CONNECTION's S3 storage",
                StorageBackend::Adls { .. } => "the CONNECTION's Azure storage",
            }
        }
        WriteStorage::Vended(_) => match classify_vended_scheme(&scheme) {
            // object_store writes `abfs` over HTTPS too; ALLOW_HTTP acknowledges the upgrade, as
            // the virtual schema's property does.
            Some(VendedBackendKind::Adls) if scheme == "abfs" && !allow_http => {
                return Err(UdfError::User(format!(
                    "table {table} has the location {location}, whose plaintext scheme abfs needs \
                     the export option ALLOW_HTTP = 'true', which acknowledges that the export \
                     writes the location over HTTPS"
                )));
            }
            Some(kind) => return Ok(Some(kind)),
            None => "vended storage",
        },
    };
    Err(UdfError::User(format!(
        "table {table} has the location {location}, whose scheme {scheme} {storage} cannot address"
    )))
}

fn s3_storage(
    backend: &StorageBackend,
    credentials: StorageCredentials,
) -> Result<(FileIO, TableCredentials), UdfError> {
    let file_io = backend.file_io_with_credential_loader(credentials.loader())?;
    Ok((file_io, TableCredentials::S3(credentials)))
}

fn adls_storage(location: &str, credentials: AdlsCredentials) -> (FileIO, TableCredentials) {
    (
        adls_file_io(location, credentials.clone()),
        TableCredentials::Adls(credentials),
    )
}

/// What a writer's data files depend on: the field IDs of the current schema, and the partition
/// spec their partition values follow.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct TableShape {
    schema_id: i32,
    spec_id: i32,
}

impl TableShape {
    fn of(metadata: &TableMetadata) -> Self {
        Self {
            schema_id: metadata.current_schema_id(),
            spec_id: metadata.default_partition_spec_id(),
        }
    }

    /// `requirements` plus the assertions that the table still has this schema and partition spec.
    fn pinned(self, mut requirements: Vec<TableRequirement>) -> Vec<TableRequirement> {
        requirements.push(TableRequirement::CurrentSchemaIdMatch {
            current_schema_id: self.schema_id,
        });
        requirements.push(TableRequirement::DefaultSpecIdMatch {
            default_spec_id: self.spec_id,
        });
        requirements
    }
}

/// Why the writer's files cannot go onto `reloaded`: a schema or partition spec other than the one
/// it checked, or format version 3 or later, whose row lineage the export does not maintain. No
/// Iceberg REST requirement asserts the format version, so only a reload can see an upgrade.
fn refuse_reloaded(checked: TableShape, reloaded: &TableMetadata) -> Result<(), String> {
    refuse_changed(checked, TableShape::of(reloaded))?;
    let version = reloaded.format_version();
    if version > FormatVersion::V2 {
        return Err(format!(
            "it was upgraded to format version {} after the writer checked the table",
            version as u8
        ));
    }
    Ok(())
}

/// What changed in `reloaded` since the writer checked `checked`, if anything that its data files
/// depend on did.
fn refuse_changed(checked: TableShape, reloaded: TableShape) -> Result<(), String> {
    let mut changes = Vec::new();
    if reloaded.schema_id != checked.schema_id {
        changes.push(format!(
            "its current schema ID changed from {} to {}",
            checked.schema_id, reloaded.schema_id
        ));
    }
    if reloaded.spec_id != checked.spec_id {
        changes.push(format!(
            "its default partition spec ID changed from {} to {}",
            checked.spec_id, reloaded.spec_id
        ));
    }
    if changes.is_empty() {
        return Ok(());
    }
    Err(format!(
        "{} after the writer checked the table",
        changes.join(" and ")
    ))
}

/// What an HTTP status says about a state-changing request.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Outcome {
    Applied,
    Conflict,
    Busy,
    MayHaveApplied,
    Refused,
}

/// Iceberg Java's `CommitErrorHandler` reads 500, 502, 503, and 504 as a commit of unknown state,
/// and the REST spec marks 500, 502, and 504 so.
fn classify(status: u16) -> Outcome {
    match status {
        200..=299 => Outcome::Applied,
        409 => Outcome::Conflict,
        429 => Outcome::Busy,
        500 | 502 | 503 | 504 => Outcome::MayHaveApplied,
        _ => Outcome::Refused,
    }
}

fn creation_request(table: &TableIdent, schema: Schema, export_id: &str) -> CreateTableRequest {
    let properties = HashMap::from([
        ("format-version".to_string(), "2".to_string()),
        (NUM_RETRIES_PROPERTY.to_string(), EXPORT_RETRIES.to_string()),
        (EXPORT_ID_PROPERTY.to_string(), export_id.to_string()),
    ]);
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

fn created_by_another_statement(table: &TableIdent) -> UdfError {
    UdfError::User(format!(
        "table {table} was created by another statement after the export checked that it does \
         not exist"
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
