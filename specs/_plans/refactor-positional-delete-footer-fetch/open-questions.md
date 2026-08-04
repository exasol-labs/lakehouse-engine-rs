# Open Questions: refactor-positional-delete-footer-fetch

speq-plan-pr could not complete this plan without human input. What's done so far is committed on this branch. Reply inline on the PR, or resume with `/speq:plan refactor-positional-delete-footer-fetch` locally, or re-run `/speq:plan-pr refactor-positional-delete-footer-fetch` after commenting.

- [ ] Issue #165's proposed change asks to "guard against silent double-fetch if [the metadata cache] evicts." Task 1.7 (`scan_footer_reuse_holds_at_shard_scale`) measures cache reuse over K=64 two-column fixtures, whose footers total a few KB against DataFusion's 50 MiB `DEFAULT_METADATA_CACHE_LIMIT` — the test cannot fail from eviction, the exact case it is meant to guard. Decision-log entry [6] then defers any real fix to "if the measurement fails," so no eviction guard, metric, or log ships. Pick one:
  - (a) Scale task 1.7's fixture until K parsed footers approach the 50 MiB limit, record the measured per-entry cache size, and add task 1.7b: an observable (log line + counter) that fires when a footer cached in Phase B is re-fetched by the opener, so a production eviction is visible instead of silent.
  - (b) Accept a documented, best-effort measurement at today's scale for this PR, and file a follow-up issue for the runtime eviction guard.
