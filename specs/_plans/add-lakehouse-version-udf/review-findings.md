# Review Findings: add-lakehouse-version-udf

## Standard fixes

### S1. `[TACTICAL_SHORTCUT]` — `extract_version_value` header-skip pattern lacks trailing glob

**File**: `deploy/scripts/install.sh`, function `extract_version_value`
**Line**: the `LAKEHOUSE_ENGINE_VERSION)` case branch

The column-header skip pattern is an exact-match case branch (`LAKEHOUSE_ENGINE_VERSION)`), while the established sibling `extract_query_value` uses a glob suffix (`SYSTEM_VALUE*`) to tolerate trailing whitespace in exapump tabular output. A single trailing space from exapump would cause `extract_version_value` to return the header literal as the version value, which `classify_version_smoke` would then classify as `version-mismatch` and fail an otherwise correct install.

**Fix**: Change `LAKEHOUSE_ENGINE_VERSION)` to `LAKEHOUSE_ENGINE_VERSION*)` in the `case` branch, matching the glob-suffix convention `SYSTEM_VALUE*` already used in `extract_query_value`.

## Expert fixes

(none)
