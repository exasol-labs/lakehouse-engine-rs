# Reviewer Feedback: add-export-into-script-entry-points (round 4)

Human reviewer feedback on the plan, recorded verbatim.

1. The usual pattern for Export UDF is to have a single function, LAKEHOUSE_EXPORT, that has two entry points: the generate export spec and the run entry point. We don't need two UDFs. See that the `run` for LAKEHOUSE_EXPORT is now defined and just returns an error. Use that `run` instead of creating a second UDF LAKEHOUSE_EXPORT_WRITER.

2. Additionally, drop `--export-connection` from install.sh. This is my mistake in #565. Its acceptance criterion says the installer should grant the scripts access to the CONNECTION. That doesn't make sense, because the installer runs before any CONNECTION exists. Export should work like virtual schemas: install the scripts, then users create CONNECTIONs and grant access to them. One CONNECTION can then serve both a virtual schema and an export. The installer should print the export grants as a template, as it already does for virtual schemas. I'll fix the wording in #565.

## Resolution

- Item 1: `LAKEHOUSE_EXPORT` is one script. Its `export_spec` hook builds the query and its `run` counts the rows. `LAKEHOUSE_EXPORT_WRITER` is removed. The `.so` has four entry points. Decision [1] in `decision-log.md` holds the measured evidence.
- Item 2: `--export-connection` and its grant-running code are removed. The installer prints the export grants as a template. The `[plan-review]` entry in `decision-log.md` quotes this feedback.
