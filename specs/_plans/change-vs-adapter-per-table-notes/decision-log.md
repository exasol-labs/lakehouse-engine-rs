# Decision Log: change-vs-adapter-per-table-notes

## Interview

- Q: Issue scope → A: #425 as written: per-table adapterNotes, drop TABLE_MAP, no migration, no fallback, no TABLE_MAP-absence tests.
- Q: Schema-level adapterNotes size cap and SKIPPED_TABLES truncation? → A: Keep unchanged.
- Q: Per-table note shape? → A: Same dotted identifier string TABLE_MAP holds today, under key `name`: `{"name": "<catalog identifier>"}`.
- Q: Pushdown on a table with empty notes or no `name`? → A: Hard error naming the table and telling the user to run `ALTER VIRTUAL SCHEMA ... REFRESH`.
- Q: Partial `REFRESH TABLES` coverage? → A: Add a spec scenario and an E2E test.
- Q: Partial refresh semantics? → A: Honor `requestedTables`: return only the requested tables, merge SKIPPED_TABLES (keep unrequested entries, drop a requested table that now loads, add or update a requested table still skipped). The first task verifies live that Exasol keeps unrequested tables and their notes. Supersedes part of ADR 009.
- Q: A requested table that has a skip entry and now loads? → A: Remove its skip entry, so it becomes a virtual table.
- Q: A requested table the catalog no longer has? → A: Omit it from the response and remove its skip entry; the live check confirms what Exasol does with the virtual table.
- Q: Does a scoped `refresh` request carry the existing virtual tables or their table-level notes? → A: No (verified live on Docker 2025.1.16): only `type`, `requestedTables`, `schemaMetadataInfo.name`, `schemaMetadataInfo.adapterNotes`. So a scoped refresh always lists the namespace to resolve requested names.
- Q: Scoped refresh, an unrequested skip entry whose table the source no longer lists? → A: Drop it (decision [3] approved; rationale is that the listing shows it gone, as a full REFRESH would).
- Q: What does a scoped refresh list? → A: Identifiers only, never per-table metadata for the whole namespace (thousands of tables in customer environments). Match requested names against flattened identifiers, run the collision check over them, and `load_table` only the requested matches.
- Q: Which identifiers does the name-collision check cover? → A: Load all name matches (round-2 Option C). CREATE, setProperties, and a full REFRESH keep today's check over loaded tables only (#425: no change). A scoped refresh selects every listed identifier that flattens to a requested name, loads those, and applies the same loaded-only check to them. Unrequested names are never loaded or checked. The previous mapping is never consulted: each requested name resolves exactly as a full REFRESH would.
- Q: Split into two plans (per-table notes, then scoped refresh)? → A: No, keep one plan.

## Design Decisions

### [1] Adapter notes live at the level they describe

- **Decision:** The adapter persists each value in the `adapterNotes` of the narrowest object it describes: a column, a virtual table, or the virtual schema. The schema-level notes hold only values that belong to the whole schema.
- **Rationale:** Exasol creates, refreshes, and drops a table or column together with its notes, so a note cannot drift from its object. A pushdown receives the notes of exactly the tables and columns it touches. The schema-level notes are capped at 2,000,000 characters (sqlCode 04000), so they must not grow with the table count. Criterion: a long-lived persistence constraint that binds the adapter, the pushdown planner, and every catalog kind. ADR search: no accepted ADR states where a note lives. `adapternotes-admits-only-create-time-values-a-pushdown-cannot-recompute` decides whether a value is persisted, and this decision decides where.
- **Alternatives:** A schema-level map keyed by Exasol table name: rejected, because it grows toward the cap, can disagree with the virtual tables, and a `REFRESH TABLES` cannot update one entry of it.
- **Consequences:**
  - Each virtual table stores its catalog identifier as `{"name": "<catalog identifier>"}`. A table without that note fails at pushdown until `ALTER VIRTUAL SCHEMA ... REFRESH` writes it.
  - `SKIPPED_TABLES` and the tuning values stay at schema level, because a skipped table has no virtual table to hold a note.
- **Architecture:** § Components, § Data Flow, § Interfaces, § Constraints
- **Promotes to ADR:** yes

### [2] A partial refresh lists identifiers and loads only the requested tables

- **Decision:** `setProperties` and a `refresh` without `requestedTables` run the full `createVirtualSchema` listing. A `refresh` that carries `requestedTables` lists only the source's table identifiers, loads every identifier whose flattened Exasol name is requested, returns only the tables that load, and merges `SKIPPED_TABLES` with the persisted list. Each requested name resolves exactly as a full `REFRESH` resolves it. Every path keeps the schema-level `adapterNotes` entries the adapter does not write.
- **Rationale:** Exasol scopes such a refresh to the requested tables: it keeps every unrequested virtual table with its notes, creates a returned table that has none, and drops a requested table the response omits (same probe). The request carries no virtual table and no table-level note (verified live), so only a listing maps a requested name to a catalog identifier. An identifier listing costs one paged call per namespace, while loading table metadata costs at least one call per table, and customer namespaces hold thousands of tables. The name-collision check keeps running over loaded tables only, as #425 requires ("No change there"), so the partial refresh applies it to the tables its load admits. The merge reads only the notes Exasol sends in the request, so the adapter stays stateless. ADR digest: no accepted ADR covers refresh scoping. The entry does not promote, because the scenarios of `vs-adapter/refresh-and-set-properties` state the whole rule (`/speq:adr-rules` rule 3), and its rejected alternatives concern the refresh path of one component.
- **Alternatives:** Return every table for a partial refresh, which makes `REFRESH TABLES` act as a full `REFRESH`: rejected in the interview. Run the full listing and filter its result: rejected, because it loads the metadata of every table in the namespace. Resolve each requested name from its virtual table's identifier note: impossible, because the request carries no virtual table. Check every listed identifier for a name collision: rejected, because a collision among unrequested or skipped tables would then fail a `REFRESH TABLES` that a full `REFRESH` accepts.
- **Consequences:**
  - A load failure or a column-note error on an unrequested table no longer fails `REFRESH TABLES`. A failure of the identifier listing still fails it.
  - A requested name can match several listed identifiers, which differ in letter case or in `__` placement. The adapter loads each of them. The statement succeeds when at most one of them loads, and it fails with the name-collision error when two load, as a full `REFRESH` does. A collision among unrequested identifiers never fails `REFRESH TABLES`, because the adapter neither loads nor checks them.
  - The adapter never reads a requested table's previous identifier note. A requested virtual table whose only loadable identifier changed is re-declared from the identifier that loads now.
  - Skip entries of unrequested tables can be stale until a full `REFRESH`.
- **Architecture:** § Components, § Data Flow
- **Promotes to ADR:** no

### [3] A persisted skip entry survives only while the source still lists its table

- **Decision:** The `SKIPPED_TABLES` merge matches persisted entries against the catalog identifiers of the fresh identifier listing. A persisted entry whose table the source no longer lists is dropped, whether or not the request names it.
- **Rationale:** The fresh listing shows that the entry's table is gone. A full `REFRESH` drops the same entry. Listing order, which orders `SKIPPED_TABLES`, is defined only for entries the fresh listing returns.
- **Consequences:** `SKIPPED_TABLES_OMITTED` carries the persisted count forward, because entries a previous run dropped stay unknown. After a partial refresh it is an upper bound: each `REFRESH TABLES` of a requested table that is still skipped and that the cap drops again counts that table again, until the next refresh without `requestedTables`.
- **Promotes to ADR:** no

### [4] One module owns each adapterNotes format and the partial-refresh rule

- **Decision:** A new `adapter/table_identity.rs` owns the identifier note: it encodes the note at listing time and reads the identifier from an involved table at pushdown. A new `adapter/skipped_tables.rs` owns the `SKIPPED_TABLES` and `SKIPPED_TABLES_OMITTED` format: it encodes and caps the list, which moves out of `adapter/mod.rs`, and it reads the persisted list. A new `adapter/scoped_refresh.rs` owns the partial refresh: the one test of whether an Exasol name is requested, the selection of the listed identifiers to load, and the merge rule.
- **Rationale:** The identifier note has one writer (the listing) and two readers (single-table and join pushdown), and the skip list gains a reader (the merge). Each format then has exactly one owner, so a writer and a reader cannot drift (`/speq:design-philosophy` Quick Diagnostic: back-door leakage). The table selection and the merge both ask whether a name is requested, so one module answers that question for both. Each module is summed up in one sentence, takes plain JSON and listing values, performs no I/O, and hides its key names, its error text, and the cap arithmetic behind a few functions.
- **Alternatives:** Put the merge in `skipped_tables.rs`: rejected, because the merge needs the requested-name test, and `scoped_refresh.rs` calls `skipped_tables.rs`, so the two modules would depend on each other.
- **Promotes to ADR:** no

### [5] Each catalog kind classifies a table in one function that both listing paths call

- **Decision:** `CatalogClient` gains an identifier-only listing and a load of listed tables. In each kind, the full listing and the load of listed tables call one function that admits a table or names its skip reason. Unity Catalog and Glue keep their bulk full listing, and the Iceberg REST and direct-storage full listings become the identifier listing followed by the load of listed tables.
- **Rationale:** One function per kind makes the two paths agree on admission and skip reason by construction, so `SKIPPED_TABLES` reads the same after `REFRESH` and `REFRESH TABLES`. Unity Catalog's `GET /tables` and Glue's `GetTables` return every table's metadata in a few pages, so their full listing stays one sweep. The Iceberg REST and direct-storage full listings already list identifiers first and load each table after.
- **Alternatives:** Build every kind's full listing from the identifier listing and the load of listed tables: rejected, because the Unity Catalog and Glue full listings would issue one request per table instead of a few paged calls.
- **Architecture:** § Components, § Constraints
- **Promotes to ADR:** no

## Review Findings

### [plan-review] A partial refresh drops the skip entries of tables the source no longer lists

- **Finding:** `[INTENT_DRIFT]` (HUMAN). Decision [3] dropped an unrequested skip entry whose table the source no longer lists, against the interview answer "keep unrequested entries", and justified it with a parse ambiguity that does not exist.
- **Direction change:** The requester approved decision [3]. Its Rationale now gives the real reasons, and the parse-ambiguity sentence and its alternative are deleted. plan.md § Impact states that `SKIPPED_TABLES` keeps the entries of unrequested tables the source still lists.
- **Promotes to ADR:** no

### [plan-review] REFRESH TABLES lists identifiers and loads only the requested tables

- **Finding:** `[NFR_IGNORED]` (advisory): `REFRESH TABLES` ran the full listing, so its cost grew with the namespace and a failure on any unrequested table aborted it. After the review, the requester required that a partial refresh never load per-table metadata for the whole namespace.
- **Direction change:** Decision [2] replaces the full listing of a partial refresh with an identifier listing and a load of the requested tables. Decision [5] adds the two `CatalogClient` methods and gives each kind one classification function.
- **Promotes to ADR:** no

### [plan-review] The name-collision check covers loaded tables only, on every path

- **Finding:** `[INTENT_DRIFT]` (HUMAN). The plan extended the name-collision check to every listed identifier, loaded or skipped, on `CREATE`, `setProperties`, and a full `REFRESH`. Issue #425 says "No change there", and a namespace with a skipped table that collides would no longer create or refresh, so its upgrade `REFRESH` could not heal it.
- **Direction change:** The requester chose "load all name matches". `CREATE`, `setProperties`, and a full `REFRESH` keep today's check over loaded tables. A partial refresh loads every listed identifier whose flattened name is requested and applies the same check to the tables that load. It never loads or checks an unrequested identifier, and it never reads a previous identifier note. Decision [2] states the rule. The `vs-adapter/refresh-and-set-properties` delta gains the scenario "A partial refresh resolves each requested name as a full refresh would", and the collision clauses of `vs-adapter/create-virtual-schema-declaration-details` and `direct-storage/direct-storage-table-discovery` name loaded tables.
- **Promotes to ADR:** no
