# Decisions: fix-binary-hex-rendering

## ADR: Binary reads as lowercase hex text, and hex-literal comparisons compare the raw bytes

**ID:** binary-reads-as-lowercase-hex-text
**Plan:** fix-binary-hex-rendering
**Status:** Accepted

### Context

Exasol has no binary type and rejects `HASHTYPE` as a UDF output type. Exasol pushes a filter on a binary column as a comparison with a plain string literal, and it applies an advertised filter only in the adapter's scan.

### Decision

The scan renders each binary column as lowercase hex `VARCHAR` in its select list and also exposes the column's raw bytes under a private alias. The adapter rewrites a comparison, `IN`, or `BETWEEN` against a lowercase hex literal of whole bytes, reached from the filter root through `AND` and `OR` only, into a comparison of the raw bytes with a binary literal. Every other filter operates on the hex text.

### Options Considered

| Option | Verdict |
|--------|---------|
| Declare the column `HASHTYPE` | Rejected: Exasol rejects it as a UDF output type |
| Render hex where the scan binds file columns | Rejected: a filter on the rendered column turns off row filtering for the whole scan |
| Compare every filter on the hex text | Rejected: Parquet statistics no longer prune binary columns |

### Consequences

- Results match native Exasol over the hex text, including an empty value reading as NULL.
- The rewrite stays out of `NOT`, because there the empty value's raw-byte result differs from the text's NULL.
- Plan-time pruning also compares the raw bytes, under `NOT` too: pruning only needs every matching row kept, and the raw bytes are TRUE wherever the hex text is.
- `BINARY_AS_STRING` converts the bytes to text in the scan with a validating function, because arrow-rs validates UTF-8 only for an annotated `STRING` column.
