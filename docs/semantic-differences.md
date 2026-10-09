[lakehouse-engine](../README.md) › [Docs](index.md) › Semantic differences

---

# Semantic differences from native Exasol

A few pushed-down functions return a different result than the same query on a native Exasol
table. Each difference on this page is known and accepted: the engine does not plan to change
it. The [Capabilities](capabilities.md) page lists which functions are pushed down.

## Case conversion of non-ASCII text

`UPPER`, `LOWER`, and `INITCAP` can return different text for non-ASCII characters. ASCII text
is not affected.

The difference comes from two case-mapping rules:

- Native Exasol maps each character to exactly one character, using case tables from an older
  Unicode version (about 5.0). A character without a single-character mapping stays unchanged.
- The scan runs these functions in DataFusion, which applies current Unicode full case mapping.
  One character can become several, and Greek capital sigma becomes final sigma at the end of a
  word.

| Expression | Virtual schema | Native Exasol |
|---|---|---|
| `UPPER('straße')` | `STRASSE` | `STRAßE` |
| `UPPER('ﬁ')` (ligature) | `FI` | `ﬁ` |
| `LOWER('İ')` (Turkish dotted I) | `i̇` (two characters) | `i` |
| `LOWER('ΟΔΟΣ')` | `οδος` | `οδοσ` |
| `LOWER('ẞ')` (capital sharp s) | `ß` | `ẞ` |
| `UPPER('ა')` (Georgian) | `Ა` | `ა` |
| `INITCAP('ßa')` | `SSa` | `ßa` |

The difference also reaches results built on the converted text:

- `LENGTH(UPPER('ß'))` returns 2 instead of 1.
- A filter such as `WHERE UPPER(name) = 'STRAßE'` can return different rows.

No session parameter changes this. Exasol's `NLS_*` parameters control date and number
formats, not case conversion. These results were checked against Exasol 2025.2.0.
