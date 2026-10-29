# Decisions: fix-nested-name-mapping

## ADR: An Iceberg column binds only by field id or the name mapping

**ID:** an-iceberg-column-binds-only-by-field-id-or-the-name-mapping
**Plan:** `fix-nested-name-mapping`
**Status:** Accepted

### Context

The Iceberg table spec states "Columns in Iceberg data files are selected by field id", and a field id a file does not hold resolves through partition metadata, `schema.name-mapping.default`, the `initial-default`, then "Return `null` in all other cases". The scan fell back to a physical column of the logical name. A dropped and re-added column then read the dropped column's values, and a name the mapping omits read the file's same-named column.

### Decision

A field-id-bound logical field binds only by an embedded field id or, in a file without any field id, by the name mapping. Name binding stays for a field that carries no field id, which a Delta `none` column mapping and a direct-storage table produce. Where the spec is silent, iceberg-java is the reference: one field id at any depth makes the whole file id-bound, as `ParquetSchemaUtil.hasIds` decides.

### Options Considered

| Option | Verdict |
|--------|---------|
| Spec rule "Return `null` in all other cases" for a file with neither field ids nor a mapping | Chosen: the spec covers the case, and the required-absent error guards a required column |
| iceberg-java's positional fallback (`ParquetSchemaUtil.addFallbackIds`) for that file | Rejected: it assumes column order was preserved and no column was dropped, which the spec does not guarantee |
| Physical-name fallback for any unbound field | Rejected: it binds a dropped column's reused name to the re-added field |
