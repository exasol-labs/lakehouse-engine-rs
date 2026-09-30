# adapterNotes size limit (task 6.6)

Measured live on the Docker stack (Exasol 2025.1.16, .so from `make cross-udf-build`, `DIRECT_STORAGE` virtual schema over a MinIO prefix).

## Method

- `SYS.EXA_ALL_COLUMNS` / `SYS.EXA_SYS_COLUMNS` declare `ADAPTER_NOTES` as `VARCHAR(2000000) UTF8` (`COLUMN_MAXSIZE` 2000000) on every `EXA_*_VIRTUAL_*` view.
- The fixture was N directories, each holding only an empty `_SUCCESS` object, with 120-character names. Every directory is skipped as `holds no data file`, so `TABLE_MAP` is `{}` and `SKIPPED_TABLES` holds N entries of 163 bytes each. An empty-list notes value is 285 bytes, so the stored length grows by 163 bytes per skipped directory (measured values below).
- Each step dropped and recreated the virtual schema, then read `LENGTH(ADAPTER_NOTES)` and parsed the stored value.

## Results (before the cap)

| N | adapterNotes length | CREATE |
|---|---|---|
| 100 | 16,584 | ok |
| 10,000 | 1,630,284 | ok |
| 12,000 | 1,956,284 | ok |
| 12,268 | 1,999,968 | ok |
| 12,269 | 2,000,131 | error, sqlCode 04000, `Maximum adapterNotes length exceeded (2000000)` |
| 14,000 / 16,000 | over the limit | same error |

- A limit exists: 2,000,000, the declared column size. The boundary lies between 1,999,968 (accepted) and 2,000,131 (rejected).
- CREATE fails; the value never truncates.
- `ALTER VIRTUAL SCHEMA ... REFRESH` fails with the same error, and the previously stored value (1,999,968) stays unchanged.

## Cap

`fit_skipped_tables` in `adapter/mod.rs` keeps the longest listing-order prefix of `SKIPPED_TABLES` whose notes stay within 2,000,000 bytes, and records the dropped count as `SKIPPED_TABLES_OMITTED` (string) only when it is above zero. After the cap, the same fixtures with N = 14,000 and N = 16,000 create successfully: stored length exactly 2,000,000, 12,268 entries kept, `SKIPPED_TABLES_OMITTED` 1732 and 3732. A length of exactly 2,000,000 is accepted.
