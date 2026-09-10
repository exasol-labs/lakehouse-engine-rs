# Decision Log: add-lakehouse-version-udf

## Interview

**Q:** How should the spec library reflect the new `LAKEHOUSE_VERSION` entry point?
**A:** New sibling feature `packaging/version-udf`. Leave `single-so-two-entry-points` untouched.

**Q:** How strict should the installer's version smoke test be?
**A:** Exact match against `RESOLVED_ENGINE_VERSION`.

## Design Decisions

None beyond the interview choices.

## Review Findings
