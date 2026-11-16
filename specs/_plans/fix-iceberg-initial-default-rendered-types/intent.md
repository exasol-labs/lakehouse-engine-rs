# Intent: fix-iceberg-initial-default-rendered-types

## Interview

- Q: Encoding for time, decimal p>36, binary, fixed, uuid defaults? → A: Targeted: keep raw encoding for existing types, render only these five
- Q: Default that cannot be rendered at the declared type? → A: Decline pushdown with a clean error, never NULL
- Q: Nested (struct/list/map) defaults in scope? → A: No, out of scope

## Directions

- [D1] An unrenderable default refuses its column through the existing column-note `refused` mechanism, so only a query that reads that column fails and the other columns stay queryable.
- [D2] The VS reuses the identity-partition renderer, and the scan reuses only the hex-decode branch of `partition_scalar` for the `binary` and `fixed_size_binary(L)` tags; every other tag keeps its raw parse, so existing defaults keep their wire form.
- [D3] The shared renderer returns a context-free cause, and each caller names its subject (partition value or `initial-default`), so a refusal reason never mentions a partition.
- [D4] A virtual schema created before this fix reads the new defaults only after its next `CREATE`, `REFRESH`, or `SET PROPERTIES`, because the encoded default lives in the column note.
- [D5] A `binary` default whose hex pushes the column note over the per-column limit fails the statement, as `vs-adapter/column-source-notes` records for any over-long note; only an absurd default reaches it.
