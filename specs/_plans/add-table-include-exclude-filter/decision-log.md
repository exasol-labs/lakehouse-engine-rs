# Decision Log: add-table-include-exclude-filter

## Interview

- Q: What makes a glob pattern invalid? → A: only an empty entry; every other character, `[ ] \` included, matches itself literally
- Q: `REFRESH ... TABLES X` names a table the filter hides? → A: treated as not served by the source; dropped from the result, statement succeeds
- Q: Scope and rules from issue #544 (precedence, case, SKIPPED_TABLES, SET, all catalog kinds)? → A: taken as written in the issue; no split, one plan

## Design Decisions

### [1] The full catalog listing takes the caller's identifier predicate

- **Decision:** `CatalogClient::list_tables` takes a predicate over the catalog identifier. Its provided body lists the identifiers, keeps those the predicate accepts, and loads only those through `load_listed_tables`. Unity Catalog and Glue override it to keep their single-sweep listings, and apply the predicate before they admit a table. The adapter builds the predicate from the virtual table name and the table filter, so the catalog crate never learns the Exasol naming rule.
- **Rationale:** The Unity Catalog full listing reads every table's columns from one paginated `GET /tables` sweep, and the Glue full listing reads whole table records from `GetTables`. A filtered listing built outside the trait from `list_table_idents` and `load_listed_tables` would add one `GET /tables/{full_name}` or `GetTable` request per exposed table. Iceberg REST and direct storage already list identifiers and then load each table, so the provided body replaces their overrides.
- **Alternatives:** (a) Always list identifiers, filter them, then call `load_listed_tables`: loses the single-sweep listings of Unity Catalog and Glue. (b) Filter the `CatalogListing` that `list_tables` returns: too late, because every listing loads metadata inside `list_tables`, so a hidden table could still fail the statement and land in `SKIPPED_TABLES`.
- **Promotes to ADR:** no

### [2] One module owns the pattern rules, with its own matcher

- **Decision:** `crates/lakehouse-engine/src/adapter/table_filter.rs` owns the pattern syntax (comma split, trim, empty-entry rule), the uppercase fold, the `*` and `?` matcher, and the include and exclude precedence. `properties.rs` uses its parse function as the value rule of both properties, and `list_source` uses its exposure check. The matcher is an iterative loop over `char`s in that module, not a crate.
- **Rationale:** The interview fixed that `[`, `]`, and `\` match literally. The `glob` and `globset` crates read `[` as a character class and `\` as an escape, so each would need every pattern escaped first, which costs as much code as the matcher. One owner keeps the value check and the listing from disagreeing on what a pattern means. Quick diagnostic: the module has a one-sentence summary ("decides from the two property values whether a virtual table name is exposed"); its interface (parse, expose check) is far smaller than its body; no other module learns the syntax; it depends on nothing.
- **Promotes to ADR:** no

## Review Findings

### [plan-review] One provided `list_tables` body

- **Finding:** The rule "apply the predicate before any load" was written in four `list_tables` implementations, and two of them were the same identifier-then-load code.
- **Direction change:** `list_tables` is a provided trait method. Iceberg REST and direct storage use its body, and only Unity Catalog and Glue override it.
- **Promotes to ADR:** no
