# Decisions: change-vs-adapter-per-table-notes

## ADR: Adapter notes live at the level they describe

**ID:** adapter-notes-live-at-the-level-they-describe
**Plan:** change-vs-adapter-per-table-notes
**Status:** Accepted

### Context

Exasol creates, refreshes, and drops a table or column together with its notes, and a pushdown receives the notes of exactly the tables and columns it touches. The schema-level notes are capped at 2,000,000 characters (sqlCode 04000).

### Decision

The adapter persists each value in the `adapterNotes` of the narrowest object it describes: a column, a virtual table, or the virtual schema. The schema-level notes hold only values that belong to the whole schema.

### Options Considered

| Option | Verdict |
|--------|---------|
| A schema-level map keyed by Exasol table name | Rejected: it grows toward the cap, can disagree with the virtual tables, and a `REFRESH TABLES` cannot update one entry of it |

### Consequences

- Each virtual table stores its catalog identifier as `{"name": "<catalog identifier>"}`. A table without that note fails at pushdown until `ALTER VIRTUAL SCHEMA ... REFRESH` writes it.
- `SKIPPED_TABLES` and the tuning values stay at schema level, because a skipped table has no virtual table to hold a note.
