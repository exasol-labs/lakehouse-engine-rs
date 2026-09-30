# .so size (task 8.2)

Both binaries came from `make cross-udf-build` (`rust:1.94-trixie`, release profile). Each size is the byte count after `strip --strip-unneeded` on a copy of `target/release/liblakehouse_engine.so`. Both builds passed `cargo exasol-udf validate` (3 UDFs).

| Build | Commit | Unstripped | Stripped (`--strip-unneeded`) |
|---|---|---|---|
| Merge base (`origin/main`, temporary worktree outside the repo, removed afterwards) | `75cb878` | 174.1M | 144,791,640 bytes |
| Branch `feat/add-glue-catalog-kind` (working tree) | `75cb878` plus the uncommitted plan changes | 190,751,104 bytes | 148,621,152 bytes |

- Stripped growth: 3,829,512 bytes (3.65 MiB), 2.6% over the merge base.
- The plan adds `aws-sdk-glue` and the Glue reader code. No per-crate size breakdown was measured.
