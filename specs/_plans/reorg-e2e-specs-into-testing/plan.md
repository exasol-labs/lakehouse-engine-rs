# Plan: reorg-e2e-specs-into-testing

## Summary

This plan reorganizes the whole spec library. It moves test-harness, unit-test, regression-guard, fixture, and ops rules out of feature specs into one new file, `specs/testing.md`, and keeps product behavior only in feature specs. It also splits every domain above 8 features and every feature above 10 scenarios. It changes specs, comments, and docs only, and changes no code behavior.

## Context

- Before this plan the library holds 11 domains and 152 features.
- Six domains (`e2e-harness`, `azure-e2e`, `unity-e2e`, `lakekeeper-e2e`, `glue-e2e`, `cloud-e2e`) hold 21 features with 137 scenarios that describe E2E suites, fixtures, cleanup workflows, and a benchmark catalog deployment, or restate product behavior that a feature spec already owns.
- Three `packaging` features (`positional-delete-fixtures`, `int96-timestamp-fixture`, `iceberg-type-promotion-fixture`) describe only how Spark writes E2E fixtures.
- Three domains exceed the library limit of 8 features: `vs-adapter` holds 84, `datafusion-scan` 27, and `sql-comprehension` 11.
- Six features exceed the limit of 10 scenarios: `vs-adapter/pushdown-planning-char-type-declaration` (16), `vs-adapter/connection-credentials-assume-role` (13), `vs-adapter/pushdown-planning-cloud-credentials` (13), `vs-adapter/storage-backend-enum` (13), `datafusion-scan/scan-execution-field-id-projection` (13), and `vs-adapter/parquet-directory-seam` (12).
- `/speq:record` stops before archiving when a recorded spec exceeds either limit.
- speq 0.25.0 supports only `specs/<domain>/<feature>/spec.md`. A scratch test showed that a nested path such as `vs-adapter/pushdown/<feature>` is left out of `speq feature list`, and `speq feature get` reads its third segment as a scenario name.
- speq has no move operation. A move is a full REMOVED delta at the old path plus a full new spec at the new path. `speq record` then leaves the old `spec.md` with no scenario, which `speq feature validate` rejects, and does not delete the directory.
- speq 0.25.0 validates and records only feature deltas and `architecture.md`. It ignores any other file in the plan directory, so `testing.md` reaches `specs/` through a copy task.
- Spec paths appear in other specs, in 11 code comment lines, in `AGENTS.md`, in `specs/mission.md`, in 7 ADR fragments, and in five comment or doc lines that name a removed E2E spec. 23 `/// Scenario:` test doc lines quote a removed E2E scenario title, and one quotes a scenario title this plan rewrites.
- The plan changes no component, boundary, interface, data flow, constraint, or external dependency, and `specs/architecture.md` names no spec path, so the plan has no architecture delta (decision-log.md [9]).

## Features

The plan directory holds 106 NEW, 21 CHANGED, and 124 REMOVED delta files. A NEW file at a new path is the full spec of a moved or split feature; its old path holds the matching REMOVED delta. All paths are relative to `specs/_plans/reorg-e2e-specs-into-testing/`. The plan also holds `testing.md`, a non-feature file that task 5.2 copies to `specs/testing.md` (decision-log.md [1]).

| Feature | Status | Spec |
|---------|--------|------|
| azure-e2e/azure-e2e-harness-operations | REMOVED | `azure-e2e/azure-e2e-harness-operations/spec.md` |
| azure-e2e/azure-e2e-harness | REMOVED | `azure-e2e/azure-e2e-harness/spec.md` |
| azure-e2e/azure-orphan-container-sweep | REMOVED | `azure-e2e/azure-orphan-container-sweep/spec.md` |
| catalog/catalog-crate-public-surface-extensions | NEW | `catalog/catalog-crate-public-surface-extensions/spec.md` |
| catalog/catalog-crate-structure | NEW | `catalog/catalog-crate-structure/spec.md` |
| catalog/pushdown-catalog-session | NEW | `catalog/pushdown-catalog-session/spec.md` |
| catalog/rest-catalog-oauth-auth | NEW | `catalog/rest-catalog-oauth-auth/spec.md` |
| cloud-e2e/assume-role-e2e | REMOVED | `cloud-e2e/assume-role-e2e/spec.md` |
| cloud-e2e/cloud-assume-role-e2e | REMOVED | `cloud-e2e/cloud-assume-role-e2e/spec.md` |
| connection/connection-credentials-assume-role-session-use | NEW | `connection/connection-credentials-assume-role-session-use/spec.md` |
| connection/connection-credentials-assume-role | NEW | `connection/connection-credentials-assume-role/spec.md` |
| connection/connection-credentials-azure | NEW | `connection/connection-credentials-azure/spec.md` |
| connection/connection-credentials-catalog-auth | NEW | `connection/connection-credentials-catalog-auth/spec.md` |
| connection/connection-credentials-direct-storage | NEW | `connection/connection-credentials-direct-storage/spec.md` |
| connection/connection-credentials-sigv4 | NEW | `connection/connection-credentials-sigv4/spec.md` |
| connection/connection-credentials-unity-catalog | NEW | `connection/connection-credentials-unity-catalog/spec.md` |
| connection/connection-credentials | NEW | `connection/connection-credentials/spec.md` |
| datafusion-scan/nested-json-rendering | REMOVED | `datafusion-scan/nested-json-rendering/spec.md` |
| datafusion-scan/scan-execution-column-case-fold | CHANGED | `datafusion-scan/scan-execution-column-case-fold/spec.md` |
| datafusion-scan/scan-execution-connection-concurrency | REMOVED | `datafusion-scan/scan-execution-connection-concurrency/spec.md` |
| datafusion-scan/scan-execution-delta-deletion-vectors | REMOVED | `datafusion-scan/scan-execution-delta-deletion-vectors/spec.md` |
| datafusion-scan/scan-execution-expression-pushdown | CHANGED | `datafusion-scan/scan-execution-expression-pushdown/spec.md` |
| datafusion-scan/scan-execution-field-id-projection | REMOVED | `datafusion-scan/scan-execution-field-id-projection/spec.md` |
| datafusion-scan/scan-execution-file-metadata | CHANGED | `datafusion-scan/scan-execution-file-metadata/spec.md` |
| datafusion-scan/scan-execution-grouped-agg | REMOVED | `datafusion-scan/scan-execution-grouped-agg/spec.md` |
| datafusion-scan/scan-execution-join | REMOVED | `datafusion-scan/scan-execution-join/spec.md` |
| datafusion-scan/scan-execution-memory-and-credentials | REMOVED | `datafusion-scan/scan-execution-memory-and-credentials/spec.md` |
| datafusion-scan/scan-execution-partial-agg | REMOVED | `datafusion-scan/scan-execution-partial-agg/spec.md` |
| datafusion-scan/scan-execution-partition-values | REMOVED | `datafusion-scan/scan-execution-partition-values/spec.md` |
| datafusion-scan/scan-execution-positional-deletes-fanout | REMOVED | `datafusion-scan/scan-execution-positional-deletes-fanout/spec.md` |
| datafusion-scan/scan-execution-positional-deletes | REMOVED | `datafusion-scan/scan-execution-positional-deletes/spec.md` |
| datafusion-scan/scan-execution-spec-reconstitution | CHANGED | `datafusion-scan/scan-execution-spec-reconstitution/spec.md` |
| datafusion-scan/scan-execution-telemetry | REMOVED | `datafusion-scan/scan-execution-telemetry/spec.md` |
| datafusion-scan/scan-execution-threading | REMOVED | `datafusion-scan/scan-execution-threading/spec.md` |
| datafusion-scan/scan-execution-value-conversion | CHANGED | `datafusion-scan/scan-execution-value-conversion/spec.md` |
| datafusion-scan/scan-execution | CHANGED | `datafusion-scan/scan-execution/spec.md` |
| datafusion-scan/scan-module-structure | CHANGED | `datafusion-scan/scan-module-structure/spec.md` |
| datafusion-scan/scan-partial-agg-column-contract | REMOVED | `datafusion-scan/scan-partial-agg-column-contract/spec.md` |
| datafusion-scan/type-mapping-live-coverage | REMOVED | `datafusion-scan/type-mapping-live-coverage/spec.md` |
| datafusion-scan/type-mapping-module-structure | REMOVED | `datafusion-scan/type-mapping-module-structure/spec.md` |
| datafusion-scan/type-mapping-timestamp-precision | REMOVED | `datafusion-scan/type-mapping-timestamp-precision/spec.md` |
| datafusion-scan/type-mapping | REMOVED | `datafusion-scan/type-mapping/spec.md` |
| datafusion-scan/type-relaxation | REMOVED | `datafusion-scan/type-relaxation/spec.md` |
| delta/delta-file-pruning | NEW | `delta/delta-file-pruning/spec.md` |
| delta/delta-reader-feature-gating | NEW | `delta/delta-reader-feature-gating/spec.md` |
| delta/delta-table-planning | NEW | `delta/delta-table-planning/spec.md` |
| delta/delta-type-mapping | NEW | `delta/delta-type-mapping/spec.md` |
| direct-storage/direct-storage-hive-partitioning | NEW | `direct-storage/direct-storage-hive-partitioning/spec.md` |
| direct-storage/direct-storage-properties | NEW | `direct-storage/direct-storage-properties/spec.md` |
| direct-storage/direct-storage-table-discovery | NEW | `direct-storage/direct-storage-table-discovery/spec.md` |
| direct-storage/direct-storage-table-planning | NEW | `direct-storage/direct-storage-table-planning/spec.md` |
| direct-storage/parquet-directory-seam-file-listing | NEW | `direct-storage/parquet-directory-seam-file-listing/spec.md` |
| direct-storage/parquet-directory-seam | NEW | `direct-storage/parquet-directory-seam/spec.md` |
| e2e-harness/cloud-e2e-harness | REMOVED | `e2e-harness/cloud-e2e-harness/spec.md` |
| e2e-harness/direct-storage-e2e-properties | REMOVED | `e2e-harness/direct-storage-e2e-properties/spec.md` |
| e2e-harness/direct-storage-e2e | REMOVED | `e2e-harness/direct-storage-e2e/spec.md` |
| e2e-harness/e2e-harness-grouped-agg | REMOVED | `e2e-harness/e2e-harness-grouped-agg/spec.md` |
| e2e-harness/e2e-harness-grouped-order | REMOVED | `e2e-harness/e2e-harness-grouped-order/spec.md` |
| e2e-harness/e2e-harness-positional-deletes | REMOVED | `e2e-harness/e2e-harness-positional-deletes/spec.md` |
| e2e-harness/e2e-harness-scan-correctness | REMOVED | `e2e-harness/e2e-harness-scan-correctness/spec.md` |
| e2e-harness/e2e-harness | REMOVED | `e2e-harness/e2e-harness/spec.md` |
| file-planning/iceberg-type-promotion | NEW | `file-planning/iceberg-type-promotion/spec.md` |
| file-planning/partition-predicate-declared-types | NEW | `file-planning/partition-predicate-declared-types/spec.md` |
| file-planning/pushdown-file-pruning | NEW | `file-planning/pushdown-file-pruning/spec.md` |
| file-planning/pushdown-format-neutral-resolution | NEW | `file-planning/pushdown-format-neutral-resolution/spec.md` |
| file-planning/pushdown-planning-empty-result | NEW | `file-planning/pushdown-planning-empty-result/spec.md` |
| file-planning/pushdown-planning-file-encoding | NEW | `file-planning/pushdown-planning-file-encoding/spec.md` |
| file-planning/pushdown-planning-file-resolution | NEW | `file-planning/pushdown-planning-file-resolution/spec.md` |
| glue-e2e/glue-e2e-harness | REMOVED | `glue-e2e/glue-e2e-harness/spec.md` |
| glue-e2e/glue-orphan-sweep | REMOVED | `glue-e2e/glue-orphan-sweep/spec.md` |
| glue/catalog-crate-public-surface-extensions-glue | NEW | `glue/catalog-crate-public-surface-extensions-glue/spec.md` |
| glue/glue-catalog-client | NEW | `glue/glue-catalog-client/spec.md` |
| glue/glue-hive-type-mapping | NEW | `glue/glue-hive-type-mapping/spec.md` |
| glue/glue-table-planning | NEW | `glue/glue-table-planning/spec.md` |
| lakekeeper-e2e/aws-lakekeeper-perf-catalog | REMOVED | `lakekeeper-e2e/aws-lakekeeper-perf-catalog/spec.md` |
| lakekeeper-e2e/lakekeeper-e2e-harness | REMOVED | `lakekeeper-e2e/lakekeeper-e2e-harness/spec.md` |
| packaging/aarch64-ci-build | CHANGED | `packaging/aarch64-ci-build/spec.md` |
| packaging/iceberg-type-promotion-fixture | REMOVED | `packaging/iceberg-type-promotion-fixture/spec.md` |
| packaging/int96-timestamp-fixture | REMOVED | `packaging/int96-timestamp-fixture/spec.md` |
| packaging/positional-delete-fixtures | REMOVED | `packaging/positional-delete-fixtures/spec.md` |
| parallelism/work-unit-sharding | CHANGED | `parallelism/work-unit-sharding/spec.md` |
| pushdown-aggregates/pushdown-agg-sql-consolidation | NEW | `pushdown-aggregates/pushdown-agg-sql-consolidation/spec.md` |
| pushdown-aggregates/pushdown-planning-aggregate-extensions | NEW | `pushdown-aggregates/pushdown-planning-aggregate-extensions/spec.md` |
| pushdown-aggregates/pushdown-planning-count-distinct | NEW | `pushdown-aggregates/pushdown-planning-count-distinct/spec.md` |
| pushdown-aggregates/pushdown-planning-expression-aggregate | NEW | `pushdown-aggregates/pushdown-planning-expression-aggregate/spec.md` |
| pushdown-aggregates/pushdown-planning-nested-aggregate-fallback | NEW | `pushdown-aggregates/pushdown-planning-nested-aggregate-fallback/spec.md` |
| pushdown-aggregates/pushdown-planning-single-group-agg-scalar-over-aggregate | NEW | `pushdown-aggregates/pushdown-planning-single-group-agg-scalar-over-aggregate/spec.md` |
| pushdown-aggregates/pushdown-planning-single-group-agg | NEW | `pushdown-aggregates/pushdown-planning-single-group-agg/spec.md` |
| pushdown-capabilities/pushdown-planning-capability-extensions | NEW | `pushdown-capabilities/pushdown-planning-capability-extensions/spec.md` |
| pushdown-capabilities/pushdown-planning-order-by-capability | NEW | `pushdown-capabilities/pushdown-planning-order-by-capability/spec.md` |
| pushdown-capabilities/pushdown-planning-string-conversion-capability | NEW | `pushdown-capabilities/pushdown-planning-string-conversion-capability/spec.md` |
| pushdown-capabilities/pushdown-planning-topn | NEW | `pushdown-capabilities/pushdown-planning-topn/spec.md` |
| pushdown-grouped-agg/pushdown-planning-grouped-agg-multikey | NEW | `pushdown-grouped-agg/pushdown-planning-grouped-agg-multikey/spec.md` |
| pushdown-grouped-agg/pushdown-planning-grouped-agg-scalar-over-aggregate | NEW | `pushdown-grouped-agg/pushdown-planning-grouped-agg-scalar-over-aggregate/spec.md` |
| pushdown-grouped-agg/pushdown-planning-grouped-agg-wrapper-fallback | NEW | `pushdown-grouped-agg/pushdown-planning-grouped-agg-wrapper-fallback/spec.md` |
| pushdown-grouped-agg/pushdown-planning-grouped-agg | NEW | `pushdown-grouped-agg/pushdown-planning-grouped-agg/spec.md` |
| pushdown-joins/pushdown-joins-module-structure | NEW | `pushdown-joins/pushdown-joins-module-structure/spec.md` |
| pushdown-joins/pushdown-planning-join-fallback-self-join | NEW | `pushdown-joins/pushdown-planning-join-fallback-self-join/spec.md` |
| pushdown-joins/pushdown-planning-join-fallback | NEW | `pushdown-joins/pushdown-planning-join-fallback/spec.md` |
| pushdown-joins/pushdown-planning-join-filter-type-coercion | NEW | `pushdown-joins/pushdown-planning-join-filter-type-coercion/spec.md` |
| pushdown-joins/pushdown-planning-join | NEW | `pushdown-joins/pushdown-planning-join/spec.md` |
| pushdown-types/pushdown-planning-char-type-declaration-padding | NEW | `pushdown-types/pushdown-planning-char-type-declaration-padding/spec.md` |
| pushdown-types/pushdown-planning-char-type-declaration | NEW | `pushdown-types/pushdown-planning-char-type-declaration/spec.md` |
| pushdown-types/pushdown-planning-decimal-string-format | NEW | `pushdown-types/pushdown-planning-decimal-string-format/spec.md` |
| pushdown-types/pushdown-planning-like-type-coercion | NEW | `pushdown-types/pushdown-planning-like-type-coercion/spec.md` |
| pushdown-types/pushdown-planning-string-fn-type-coercion-composition | NEW | `pushdown-types/pushdown-planning-string-fn-type-coercion-composition/spec.md` |
| pushdown-types/pushdown-planning-string-fn-type-coercion | NEW | `pushdown-types/pushdown-planning-string-fn-type-coercion/spec.md` |
| pushdown/pushdown-col-types-consolidation | NEW | `pushdown/pushdown-col-types-consolidation/spec.md` |
| pushdown/pushdown-declined-filter-self-apply | NEW | `pushdown/pushdown-declined-filter-self-apply/spec.md` |
| pushdown/pushdown-module-dedup-consolidation | NEW | `pushdown/pushdown-module-dedup-consolidation/spec.md` |
| pushdown/pushdown-module-structure | NEW | `pushdown/pushdown-module-structure/spec.md` |
| pushdown/pushdown-planning-alias-stripping | NEW | `pushdown/pushdown-planning-alias-stripping/spec.md` |
| pushdown/pushdown-planning-literal-projection | NEW | `pushdown/pushdown-planning-literal-projection/spec.md` |
| pushdown/pushdown-planning-selectlist-expressions | NEW | `pushdown/pushdown-planning-selectlist-expressions/spec.md` |
| pushdown/pushdown-planning | NEW | `pushdown/pushdown-planning/spec.md` |
| scan-aggregation/scan-execution-grouped-agg | NEW | `scan-aggregation/scan-execution-grouped-agg/spec.md` |
| scan-aggregation/scan-execution-join | NEW | `scan-aggregation/scan-execution-join/spec.md` |
| scan-aggregation/scan-execution-partial-agg | NEW | `scan-aggregation/scan-execution-partial-agg/spec.md` |
| scan-aggregation/scan-partial-agg-column-contract | NEW | `scan-aggregation/scan-partial-agg-column-contract/spec.md` |
| scan-read-path/scan-execution-delta-deletion-vectors | NEW | `scan-read-path/scan-execution-delta-deletion-vectors/spec.md` |
| scan-read-path/scan-execution-field-id-projection-absent-fields | NEW | `scan-read-path/scan-execution-field-id-projection-absent-fields/spec.md` |
| scan-read-path/scan-execution-field-id-projection | NEW | `scan-read-path/scan-execution-field-id-projection/spec.md` |
| scan-read-path/scan-execution-partition-values | NEW | `scan-read-path/scan-execution-partition-values/spec.md` |
| scan-read-path/scan-execution-positional-deletes-fanout | NEW | `scan-read-path/scan-execution-positional-deletes-fanout/spec.md` |
| scan-read-path/scan-execution-positional-deletes | NEW | `scan-read-path/scan-execution-positional-deletes/spec.md` |
| scan-runtime/scan-execution-connection-concurrency | NEW | `scan-runtime/scan-execution-connection-concurrency/spec.md` |
| scan-runtime/scan-execution-memory-and-credentials | NEW | `scan-runtime/scan-execution-memory-and-credentials/spec.md` |
| scan-runtime/scan-execution-telemetry | NEW | `scan-runtime/scan-execution-telemetry/spec.md` |
| scan-runtime/scan-execution-threading | NEW | `scan-runtime/scan-execution-threading/spec.md` |
| scan-types/nested-json-rendering | NEW | `scan-types/nested-json-rendering/spec.md` |
| scan-types/type-mapping-live-coverage | NEW | `scan-types/type-mapping-live-coverage/spec.md` |
| scan-types/type-mapping-module-structure | NEW | `scan-types/type-mapping-module-structure/spec.md` |
| scan-types/type-mapping-timestamp-precision | NEW | `scan-types/type-mapping-timestamp-precision/spec.md` |
| scan-types/type-mapping | NEW | `scan-types/type-mapping/spec.md` |
| scan-types/type-relaxation | NEW | `scan-types/type-relaxation/spec.md` |
| sql-comprehension/vs-expression-translator-cast | CHANGED | `sql-comprehension/vs-expression-translator-cast/spec.md` |
| sql-comprehension/vs-expression-translator-concat | REMOVED | `sql-comprehension/vs-expression-translator-concat/spec.md` |
| sql-comprehension/vs-expression-translator-date-diff-fns | REMOVED | `sql-comprehension/vs-expression-translator-date-diff-fns/spec.md` |
| sql-comprehension/vs-expression-translator-date-fns | REMOVED | `sql-comprehension/vs-expression-translator-date-fns/spec.md` |
| sql-comprehension/vs-expression-translator-float-div | CHANGED | `sql-comprehension/vs-expression-translator-float-div/spec.md` |
| sql-comprehension/vs-expression-translator-greatest-least | REMOVED | `sql-comprehension/vs-expression-translator-greatest-least/spec.md` |
| sql-comprehension/vs-expression-translator-scalar-fns | REMOVED | `sql-comprehension/vs-expression-translator-scalar-fns/spec.md` |
| sql-comprehension/vs-expression-translator-scalar-ops | CHANGED | `sql-comprehension/vs-expression-translator-scalar-ops/spec.md` |
| sql-comprehension/vs-expression-translator | CHANGED | `sql-comprehension/vs-expression-translator/spec.md` |
| sql-functions/vs-expression-translator-concat | NEW | `sql-functions/vs-expression-translator-concat/spec.md` |
| sql-functions/vs-expression-translator-date-diff-fns | NEW | `sql-functions/vs-expression-translator-date-diff-fns/spec.md` |
| sql-functions/vs-expression-translator-date-fns | NEW | `sql-functions/vs-expression-translator-date-fns/spec.md` |
| sql-functions/vs-expression-translator-greatest-least | NEW | `sql-functions/vs-expression-translator-greatest-least/spec.md` |
| sql-functions/vs-expression-translator-scalar-fns | NEW | `sql-functions/vs-expression-translator-scalar-fns/spec.md` |
| storage-access/pushdown-planning-capability-extensions-credential-reference | NEW | `storage-access/pushdown-planning-capability-extensions-credential-reference/spec.md` |
| storage-access/pushdown-planning-cloud-credentials-vended-storage | NEW | `storage-access/pushdown-planning-cloud-credentials-vended-storage/spec.md` |
| storage-access/pushdown-planning-cloud-credentials | NEW | `storage-access/pushdown-planning-cloud-credentials/spec.md` |
| storage-access/pushdown-planning-grouped-agg-credential-reference | NEW | `storage-access/pushdown-planning-grouped-agg-credential-reference/spec.md` |
| storage-access/scan-spec-credential-reference | NEW | `storage-access/scan-spec-credential-reference/spec.md` |
| storage-access/storage-backend-enum-selection | NEW | `storage-access/storage-backend-enum-selection/spec.md` |
| storage-access/storage-backend-enum | NEW | `storage-access/storage-backend-enum/spec.md` |
| unity-catalog/catalog-crate-public-surface-extensions-unity-parquet | NEW | `unity-catalog/catalog-crate-public-surface-extensions-unity-parquet/spec.md` |
| unity-catalog/unity-catalog-auth | NEW | `unity-catalog/unity-catalog-auth/spec.md` |
| unity-catalog/unity-catalog-client-parquet-admission | NEW | `unity-catalog/unity-catalog-client-parquet-admission/spec.md` |
| unity-catalog/unity-catalog-client | NEW | `unity-catalog/unity-catalog-client/spec.md` |
| unity-catalog/unity-catalog-create-virtual-schema | NEW | `unity-catalog/unity-catalog-create-virtual-schema/spec.md` |
| unity-catalog/unity-catalog-vended-credentials | NEW | `unity-catalog/unity-catalog-vended-credentials/spec.md` |
| unity-catalog/unity-parquet-table-planning | NEW | `unity-catalog/unity-parquet-table-planning/spec.md` |
| unity-e2e/unity-catalog-e2e-harness-delta-queries | REMOVED | `unity-e2e/unity-catalog-e2e-harness-delta-queries/spec.md` |
| unity-e2e/unity-catalog-e2e-harness-delta-types | REMOVED | `unity-e2e/unity-catalog-e2e-harness-delta-types/spec.md` |
| unity-e2e/unity-catalog-e2e-harness-parquet-queries | REMOVED | `unity-e2e/unity-catalog-e2e-harness-parquet-queries/spec.md` |
| unity-e2e/unity-catalog-e2e-harness | REMOVED | `unity-e2e/unity-catalog-e2e-harness/spec.md` |
| vs-adapter/adapter-module-structure | CHANGED | `vs-adapter/adapter-module-structure/spec.md` |
| vs-adapter/binary-column-refusal | CHANGED | `vs-adapter/binary-column-refusal/spec.md` |
| vs-adapter/catalog-crate-public-surface-extensions-glue | REMOVED | `vs-adapter/catalog-crate-public-surface-extensions-glue/spec.md` |
| vs-adapter/catalog-crate-public-surface-extensions-unity-parquet | REMOVED | `vs-adapter/catalog-crate-public-surface-extensions-unity-parquet/spec.md` |
| vs-adapter/catalog-crate-public-surface-extensions | REMOVED | `vs-adapter/catalog-crate-public-surface-extensions/spec.md` |
| vs-adapter/catalog-crate-structure | REMOVED | `vs-adapter/catalog-crate-structure/spec.md` |
| vs-adapter/catalog-kind-selection | CHANGED | `vs-adapter/catalog-kind-selection/spec.md` |
| vs-adapter/connection-credentials-assume-role | REMOVED | `vs-adapter/connection-credentials-assume-role/spec.md` |
| vs-adapter/connection-credentials-azure | REMOVED | `vs-adapter/connection-credentials-azure/spec.md` |
| vs-adapter/connection-credentials-catalog-auth | REMOVED | `vs-adapter/connection-credentials-catalog-auth/spec.md` |
| vs-adapter/connection-credentials-direct-storage | REMOVED | `vs-adapter/connection-credentials-direct-storage/spec.md` |
| vs-adapter/connection-credentials-sigv4 | REMOVED | `vs-adapter/connection-credentials-sigv4/spec.md` |
| vs-adapter/connection-credentials-unity-catalog | REMOVED | `vs-adapter/connection-credentials-unity-catalog/spec.md` |
| vs-adapter/connection-credentials | REMOVED | `vs-adapter/connection-credentials/spec.md` |
| vs-adapter/create-virtual-schema-adapter-notes-resources | CHANGED | `vs-adapter/create-virtual-schema-adapter-notes-resources/spec.md` |
| vs-adapter/create-virtual-schema-adapter-notes | CHANGED | `vs-adapter/create-virtual-schema-adapter-notes/spec.md` |
| vs-adapter/create-virtual-schema-declaration-details | CHANGED | `vs-adapter/create-virtual-schema-declaration-details/spec.md` |
| vs-adapter/create-virtual-schema | CHANGED | `vs-adapter/create-virtual-schema/spec.md` |
| vs-adapter/delta-file-pruning | REMOVED | `vs-adapter/delta-file-pruning/spec.md` |
| vs-adapter/delta-reader-feature-gating | REMOVED | `vs-adapter/delta-reader-feature-gating/spec.md` |
| vs-adapter/delta-table-planning | REMOVED | `vs-adapter/delta-table-planning/spec.md` |
| vs-adapter/delta-type-mapping | REMOVED | `vs-adapter/delta-type-mapping/spec.md` |
| vs-adapter/direct-storage-hive-partitioning | REMOVED | `vs-adapter/direct-storage-hive-partitioning/spec.md` |
| vs-adapter/direct-storage-properties | REMOVED | `vs-adapter/direct-storage-properties/spec.md` |
| vs-adapter/direct-storage-table-discovery | REMOVED | `vs-adapter/direct-storage-table-discovery/spec.md` |
| vs-adapter/direct-storage-table-planning | REMOVED | `vs-adapter/direct-storage-table-planning/spec.md` |
| vs-adapter/glue-catalog-client | REMOVED | `vs-adapter/glue-catalog-client/spec.md` |
| vs-adapter/glue-hive-type-mapping | REMOVED | `vs-adapter/glue-hive-type-mapping/spec.md` |
| vs-adapter/glue-table-planning | REMOVED | `vs-adapter/glue-table-planning/spec.md` |
| vs-adapter/iceberg-type-promotion | REMOVED | `vs-adapter/iceberg-type-promotion/spec.md` |
| vs-adapter/parquet-directory-seam | REMOVED | `vs-adapter/parquet-directory-seam/spec.md` |
| vs-adapter/partition-predicate-declared-types | REMOVED | `vs-adapter/partition-predicate-declared-types/spec.md` |
| vs-adapter/pushdown-agg-sql-consolidation | REMOVED | `vs-adapter/pushdown-agg-sql-consolidation/spec.md` |
| vs-adapter/pushdown-catalog-session | REMOVED | `vs-adapter/pushdown-catalog-session/spec.md` |
| vs-adapter/pushdown-col-types-consolidation | REMOVED | `vs-adapter/pushdown-col-types-consolidation/spec.md` |
| vs-adapter/pushdown-declined-filter-self-apply | REMOVED | `vs-adapter/pushdown-declined-filter-self-apply/spec.md` |
| vs-adapter/pushdown-file-pruning | REMOVED | `vs-adapter/pushdown-file-pruning/spec.md` |
| vs-adapter/pushdown-format-neutral-resolution | REMOVED | `vs-adapter/pushdown-format-neutral-resolution/spec.md` |
| vs-adapter/pushdown-joins-module-structure | REMOVED | `vs-adapter/pushdown-joins-module-structure/spec.md` |
| vs-adapter/pushdown-module-dedup-consolidation | REMOVED | `vs-adapter/pushdown-module-dedup-consolidation/spec.md` |
| vs-adapter/pushdown-module-structure | REMOVED | `vs-adapter/pushdown-module-structure/spec.md` |
| vs-adapter/pushdown-planning-aggregate-extensions | REMOVED | `vs-adapter/pushdown-planning-aggregate-extensions/spec.md` |
| vs-adapter/pushdown-planning-alias-stripping | REMOVED | `vs-adapter/pushdown-planning-alias-stripping/spec.md` |
| vs-adapter/pushdown-planning-capability-extensions-credential-reference | REMOVED | `vs-adapter/pushdown-planning-capability-extensions-credential-reference/spec.md` |
| vs-adapter/pushdown-planning-capability-extensions | REMOVED | `vs-adapter/pushdown-planning-capability-extensions/spec.md` |
| vs-adapter/pushdown-planning-char-type-declaration | REMOVED | `vs-adapter/pushdown-planning-char-type-declaration/spec.md` |
| vs-adapter/pushdown-planning-cloud-credentials | REMOVED | `vs-adapter/pushdown-planning-cloud-credentials/spec.md` |
| vs-adapter/pushdown-planning-count-distinct | REMOVED | `vs-adapter/pushdown-planning-count-distinct/spec.md` |
| vs-adapter/pushdown-planning-decimal-string-format | REMOVED | `vs-adapter/pushdown-planning-decimal-string-format/spec.md` |
| vs-adapter/pushdown-planning-empty-result | REMOVED | `vs-adapter/pushdown-planning-empty-result/spec.md` |
| vs-adapter/pushdown-planning-expression-aggregate | REMOVED | `vs-adapter/pushdown-planning-expression-aggregate/spec.md` |
| vs-adapter/pushdown-planning-file-encoding | REMOVED | `vs-adapter/pushdown-planning-file-encoding/spec.md` |
| vs-adapter/pushdown-planning-file-resolution | REMOVED | `vs-adapter/pushdown-planning-file-resolution/spec.md` |
| vs-adapter/pushdown-planning-grouped-agg-credential-reference | REMOVED | `vs-adapter/pushdown-planning-grouped-agg-credential-reference/spec.md` |
| vs-adapter/pushdown-planning-grouped-agg-multikey | REMOVED | `vs-adapter/pushdown-planning-grouped-agg-multikey/spec.md` |
| vs-adapter/pushdown-planning-grouped-agg-scalar-over-aggregate | REMOVED | `vs-adapter/pushdown-planning-grouped-agg-scalar-over-aggregate/spec.md` |
| vs-adapter/pushdown-planning-grouped-agg-wrapper-fallback | REMOVED | `vs-adapter/pushdown-planning-grouped-agg-wrapper-fallback/spec.md` |
| vs-adapter/pushdown-planning-grouped-agg | REMOVED | `vs-adapter/pushdown-planning-grouped-agg/spec.md` |
| vs-adapter/pushdown-planning-join-fallback-self-join | REMOVED | `vs-adapter/pushdown-planning-join-fallback-self-join/spec.md` |
| vs-adapter/pushdown-planning-join-fallback | REMOVED | `vs-adapter/pushdown-planning-join-fallback/spec.md` |
| vs-adapter/pushdown-planning-join-filter-type-coercion | REMOVED | `vs-adapter/pushdown-planning-join-filter-type-coercion/spec.md` |
| vs-adapter/pushdown-planning-join | REMOVED | `vs-adapter/pushdown-planning-join/spec.md` |
| vs-adapter/pushdown-planning-like-type-coercion | REMOVED | `vs-adapter/pushdown-planning-like-type-coercion/spec.md` |
| vs-adapter/pushdown-planning-literal-projection | REMOVED | `vs-adapter/pushdown-planning-literal-projection/spec.md` |
| vs-adapter/pushdown-planning-nested-aggregate-fallback | REMOVED | `vs-adapter/pushdown-planning-nested-aggregate-fallback/spec.md` |
| vs-adapter/pushdown-planning-order-by-capability | REMOVED | `vs-adapter/pushdown-planning-order-by-capability/spec.md` |
| vs-adapter/pushdown-planning-selectlist-expressions | REMOVED | `vs-adapter/pushdown-planning-selectlist-expressions/spec.md` |
| vs-adapter/pushdown-planning-single-group-agg-scalar-over-aggregate | REMOVED | `vs-adapter/pushdown-planning-single-group-agg-scalar-over-aggregate/spec.md` |
| vs-adapter/pushdown-planning-single-group-agg | REMOVED | `vs-adapter/pushdown-planning-single-group-agg/spec.md` |
| vs-adapter/pushdown-planning-string-conversion-capability | REMOVED | `vs-adapter/pushdown-planning-string-conversion-capability/spec.md` |
| vs-adapter/pushdown-planning-string-fn-type-coercion-composition | REMOVED | `vs-adapter/pushdown-planning-string-fn-type-coercion-composition/spec.md` |
| vs-adapter/pushdown-planning-string-fn-type-coercion | REMOVED | `vs-adapter/pushdown-planning-string-fn-type-coercion/spec.md` |
| vs-adapter/pushdown-planning-topn | REMOVED | `vs-adapter/pushdown-planning-topn/spec.md` |
| vs-adapter/pushdown-planning | REMOVED | `vs-adapter/pushdown-planning/spec.md` |
| vs-adapter/refresh-and-set-properties | CHANGED | `vs-adapter/refresh-and-set-properties/spec.md` |
| vs-adapter/rest-catalog-oauth-auth | REMOVED | `vs-adapter/rest-catalog-oauth-auth/spec.md` |
| vs-adapter/scan-spec-credential-reference | REMOVED | `vs-adapter/scan-spec-credential-reference/spec.md` |
| vs-adapter/storage-backend-enum | REMOVED | `vs-adapter/storage-backend-enum/spec.md` |
| vs-adapter/unity-catalog-auth | REMOVED | `vs-adapter/unity-catalog-auth/spec.md` |
| vs-adapter/unity-catalog-client-parquet-admission | REMOVED | `vs-adapter/unity-catalog-client-parquet-admission/spec.md` |
| vs-adapter/unity-catalog-client | REMOVED | `vs-adapter/unity-catalog-client/spec.md` |
| vs-adapter/unity-catalog-create-virtual-schema | REMOVED | `vs-adapter/unity-catalog-create-virtual-schema/spec.md` |
| vs-adapter/unity-catalog-vended-credentials | REMOVED | `vs-adapter/unity-catalog-vended-credentials/spec.md` |
| vs-adapter/unity-parquet-table-planning | REMOVED | `vs-adapter/unity-parquet-table-planning/spec.md` |

## Impact

- Engine users and operators: none. No code, SQL behavior, Make target, CI job, or GitHub workflow changes.
- Contributors: features live in new domains (§ Domain Layout). A feature's slug and every scenario title are unchanged, so a `/// Scenario:` line still matches, and only the domain part of a spec path changes (§ Feature Moves).
- Contributors: the rules for writing and running tests, the Spark fixture rules, and the runbooks for the orphan sweeps and the AWS benchmark catalog live in `specs/testing.md`. speq does not validate, record, or search-index that file, so later edits to it are direct edits, like edits to `specs/mission.md`.
- Spec library: from 11 domains and 152 features to 24 domains and 134 features. No domain holds more than 8 features and no feature more than 10 scenarios.
- Not a breaking change.

## Dependencies

- speq 0.25.0, with the behavior stated in § Context.
- `/speq:record` checks the library limits before it archives. After this plan no recorded spec exceeds them, so the recorder has no threshold question.

## Domain Layout

The library after task 5.3. `vs-adapter`, `datafusion-scan`, and `sql-comprehension` keep their core features under their own names. `packaging` and `parallelism` are unchanged apart from the three fixture specs that leave `packaging`.

| Domain | Features | Feature slugs |
|--------|----------|---------------|
| `catalog` | 4 | `catalog-crate-public-surface-extensions`, `catalog-crate-structure`, `pushdown-catalog-session`, `rest-catalog-oauth-auth` |
| `connection` | 8 | `connection-credentials`, `connection-credentials-assume-role`, `connection-credentials-assume-role-session-use`, `connection-credentials-azure`, `connection-credentials-catalog-auth`, `connection-credentials-direct-storage`, `connection-credentials-sigv4`, `connection-credentials-unity-catalog` |
| `datafusion-scan` | 8 | `scan-execution`, `scan-execution-column-case-fold`, `scan-execution-expression-pushdown`, `scan-execution-file-metadata`, `scan-execution-plan-shape`, `scan-execution-spec-reconstitution`, `scan-execution-value-conversion`, `scan-module-structure` |
| `delta` | 4 | `delta-file-pruning`, `delta-reader-feature-gating`, `delta-table-planning`, `delta-type-mapping` |
| `direct-storage` | 6 | `direct-storage-hive-partitioning`, `direct-storage-properties`, `direct-storage-table-discovery`, `direct-storage-table-planning`, `parquet-directory-seam`, `parquet-directory-seam-file-listing` |
| `file-planning` | 7 | `iceberg-type-promotion`, `partition-predicate-declared-types`, `pushdown-file-pruning`, `pushdown-format-neutral-resolution`, `pushdown-planning-empty-result`, `pushdown-planning-file-encoding`, `pushdown-planning-file-resolution` |
| `glue` | 4 | `catalog-crate-public-surface-extensions-glue`, `glue-catalog-client`, `glue-hive-type-mapping`, `glue-table-planning` |
| `packaging` | 5 | `aarch64-ci-build`, `architecture-aware-install`, `personal-deployment-install`, `single-so-two-entry-points`, `version-udf` |
| `parallelism` | 1 | `work-unit-sharding` |
| `pushdown` | 8 | `pushdown-col-types-consolidation`, `pushdown-declined-filter-self-apply`, `pushdown-module-dedup-consolidation`, `pushdown-module-structure`, `pushdown-planning`, `pushdown-planning-alias-stripping`, `pushdown-planning-literal-projection`, `pushdown-planning-selectlist-expressions` |
| `pushdown-aggregates` | 7 | `pushdown-agg-sql-consolidation`, `pushdown-planning-aggregate-extensions`, `pushdown-planning-count-distinct`, `pushdown-planning-expression-aggregate`, `pushdown-planning-nested-aggregate-fallback`, `pushdown-planning-single-group-agg`, `pushdown-planning-single-group-agg-scalar-over-aggregate` |
| `pushdown-capabilities` | 4 | `pushdown-planning-capability-extensions`, `pushdown-planning-order-by-capability`, `pushdown-planning-string-conversion-capability`, `pushdown-planning-topn` |
| `pushdown-grouped-agg` | 4 | `pushdown-planning-grouped-agg`, `pushdown-planning-grouped-agg-multikey`, `pushdown-planning-grouped-agg-scalar-over-aggregate`, `pushdown-planning-grouped-agg-wrapper-fallback` |
| `pushdown-joins` | 5 | `pushdown-joins-module-structure`, `pushdown-planning-join`, `pushdown-planning-join-fallback`, `pushdown-planning-join-fallback-self-join`, `pushdown-planning-join-filter-type-coercion` |
| `pushdown-types` | 6 | `pushdown-planning-char-type-declaration`, `pushdown-planning-char-type-declaration-padding`, `pushdown-planning-decimal-string-format`, `pushdown-planning-like-type-coercion`, `pushdown-planning-string-fn-type-coercion`, `pushdown-planning-string-fn-type-coercion-composition` |
| `scan-aggregation` | 4 | `scan-execution-grouped-agg`, `scan-execution-join`, `scan-execution-partial-agg`, `scan-partial-agg-column-contract` |
| `scan-read-path` | 6 | `scan-execution-delta-deletion-vectors`, `scan-execution-field-id-projection`, `scan-execution-field-id-projection-absent-fields`, `scan-execution-partition-values`, `scan-execution-positional-deletes`, `scan-execution-positional-deletes-fanout` |
| `scan-runtime` | 4 | `scan-execution-connection-concurrency`, `scan-execution-memory-and-credentials`, `scan-execution-telemetry`, `scan-execution-threading` |
| `scan-types` | 6 | `nested-json-rendering`, `type-mapping`, `type-mapping-live-coverage`, `type-mapping-module-structure`, `type-mapping-timestamp-precision`, `type-relaxation` |
| `sql-comprehension` | 6 | `vs-expression-translator`, `vs-expression-translator-cast`, `vs-expression-translator-float-div`, `vs-expression-translator-literals`, `vs-expression-translator-predicates`, `vs-expression-translator-scalar-ops` |
| `sql-functions` | 5 | `vs-expression-translator-concat`, `vs-expression-translator-date-diff-fns`, `vs-expression-translator-date-fns`, `vs-expression-translator-greatest-least`, `vs-expression-translator-scalar-fns` |
| `storage-access` | 7 | `pushdown-planning-capability-extensions-credential-reference`, `pushdown-planning-cloud-credentials`, `pushdown-planning-cloud-credentials-vended-storage`, `pushdown-planning-grouped-agg-credential-reference`, `scan-spec-credential-reference`, `storage-backend-enum`, `storage-backend-enum-selection` |
| `unity-catalog` | 7 | `catalog-crate-public-surface-extensions-unity-parquet`, `unity-catalog-auth`, `unity-catalog-client`, `unity-catalog-client-parquet-admission`, `unity-catalog-create-virtual-schema`, `unity-catalog-vended-credentials`, `unity-parquet-table-planning` |
| `vs-adapter` | 8 | `adapter-module-structure`, `binary-column-refusal`, `catalog-kind-selection`, `create-virtual-schema`, `create-virtual-schema-adapter-notes`, `create-virtual-schema-adapter-notes-resources`, `create-virtual-schema-declaration-details`, `refresh-and-set-properties` |
| Total | 134 | 24 domains |

## Feature Moves

Every feature that changes path. A plain move keeps the slug and changes the domain. A split feature has two rows: the first half keeps the slug (decision-log.md [11]).

| Old path | New path | Kind |
|----------|----------|------|
| `datafusion-scan/nested-json-rendering` | `scan-types/nested-json-rendering` | moved |
| `datafusion-scan/scan-execution-connection-concurrency` | `scan-runtime/scan-execution-connection-concurrency` | moved |
| `datafusion-scan/scan-execution-delta-deletion-vectors` | `scan-read-path/scan-execution-delta-deletion-vectors` | moved |
| `datafusion-scan/scan-execution-field-id-projection` | `scan-read-path/scan-execution-field-id-projection` | split half |
| `datafusion-scan/scan-execution-field-id-projection` | `scan-read-path/scan-execution-field-id-projection-absent-fields` | split half |
| `datafusion-scan/scan-execution-grouped-agg` | `scan-aggregation/scan-execution-grouped-agg` | moved |
| `datafusion-scan/scan-execution-join` | `scan-aggregation/scan-execution-join` | moved |
| `datafusion-scan/scan-execution-memory-and-credentials` | `scan-runtime/scan-execution-memory-and-credentials` | moved |
| `datafusion-scan/scan-execution-partial-agg` | `scan-aggregation/scan-execution-partial-agg` | moved |
| `datafusion-scan/scan-execution-partition-values` | `scan-read-path/scan-execution-partition-values` | moved |
| `datafusion-scan/scan-execution-positional-deletes` | `scan-read-path/scan-execution-positional-deletes` | moved |
| `datafusion-scan/scan-execution-positional-deletes-fanout` | `scan-read-path/scan-execution-positional-deletes-fanout` | moved |
| `datafusion-scan/scan-execution-telemetry` | `scan-runtime/scan-execution-telemetry` | moved |
| `datafusion-scan/scan-execution-threading` | `scan-runtime/scan-execution-threading` | moved |
| `datafusion-scan/scan-partial-agg-column-contract` | `scan-aggregation/scan-partial-agg-column-contract` | moved |
| `datafusion-scan/type-mapping` | `scan-types/type-mapping` | moved |
| `datafusion-scan/type-mapping-live-coverage` | `scan-types/type-mapping-live-coverage` | moved |
| `datafusion-scan/type-mapping-module-structure` | `scan-types/type-mapping-module-structure` | moved |
| `datafusion-scan/type-mapping-timestamp-precision` | `scan-types/type-mapping-timestamp-precision` | moved |
| `datafusion-scan/type-relaxation` | `scan-types/type-relaxation` | moved |
| `packaging/iceberg-type-promotion-fixture` | `specs/testing.md` | removed, rules to `testing.md` § Fixtures |
| `packaging/int96-timestamp-fixture` | `specs/testing.md` | removed, rules to `testing.md` § Fixtures |
| `packaging/positional-delete-fixtures` | `specs/testing.md` | removed, rules to `testing.md` § Fixtures |
| `sql-comprehension/vs-expression-translator-concat` | `sql-functions/vs-expression-translator-concat` | moved |
| `sql-comprehension/vs-expression-translator-date-diff-fns` | `sql-functions/vs-expression-translator-date-diff-fns` | moved |
| `sql-comprehension/vs-expression-translator-date-fns` | `sql-functions/vs-expression-translator-date-fns` | moved |
| `sql-comprehension/vs-expression-translator-greatest-least` | `sql-functions/vs-expression-translator-greatest-least` | moved |
| `sql-comprehension/vs-expression-translator-scalar-fns` | `sql-functions/vs-expression-translator-scalar-fns` | moved |
| `vs-adapter/catalog-crate-public-surface-extensions` | `catalog/catalog-crate-public-surface-extensions` | moved |
| `vs-adapter/catalog-crate-public-surface-extensions-glue` | `glue/catalog-crate-public-surface-extensions-glue` | moved |
| `vs-adapter/catalog-crate-public-surface-extensions-unity-parquet` | `unity-catalog/catalog-crate-public-surface-extensions-unity-parquet` | moved |
| `vs-adapter/catalog-crate-structure` | `catalog/catalog-crate-structure` | moved |
| `vs-adapter/connection-credentials` | `connection/connection-credentials` | moved |
| `vs-adapter/connection-credentials-assume-role` | `connection/connection-credentials-assume-role` | split half |
| `vs-adapter/connection-credentials-assume-role` | `connection/connection-credentials-assume-role-session-use` | split half |
| `vs-adapter/connection-credentials-azure` | `connection/connection-credentials-azure` | moved |
| `vs-adapter/connection-credentials-catalog-auth` | `connection/connection-credentials-catalog-auth` | moved |
| `vs-adapter/connection-credentials-direct-storage` | `connection/connection-credentials-direct-storage` | moved |
| `vs-adapter/connection-credentials-sigv4` | `connection/connection-credentials-sigv4` | moved |
| `vs-adapter/connection-credentials-unity-catalog` | `connection/connection-credentials-unity-catalog` | moved |
| `vs-adapter/delta-file-pruning` | `delta/delta-file-pruning` | moved |
| `vs-adapter/delta-reader-feature-gating` | `delta/delta-reader-feature-gating` | moved |
| `vs-adapter/delta-table-planning` | `delta/delta-table-planning` | moved |
| `vs-adapter/delta-type-mapping` | `delta/delta-type-mapping` | moved |
| `vs-adapter/direct-storage-hive-partitioning` | `direct-storage/direct-storage-hive-partitioning` | moved |
| `vs-adapter/direct-storage-properties` | `direct-storage/direct-storage-properties` | moved |
| `vs-adapter/direct-storage-table-discovery` | `direct-storage/direct-storage-table-discovery` | moved |
| `vs-adapter/direct-storage-table-planning` | `direct-storage/direct-storage-table-planning` | moved |
| `vs-adapter/glue-catalog-client` | `glue/glue-catalog-client` | moved |
| `vs-adapter/glue-hive-type-mapping` | `glue/glue-hive-type-mapping` | moved |
| `vs-adapter/glue-table-planning` | `glue/glue-table-planning` | moved |
| `vs-adapter/iceberg-type-promotion` | `file-planning/iceberg-type-promotion` | moved |
| `vs-adapter/parquet-directory-seam` | `direct-storage/parquet-directory-seam` | split half |
| `vs-adapter/parquet-directory-seam` | `direct-storage/parquet-directory-seam-file-listing` | split half |
| `vs-adapter/partition-predicate-declared-types` | `file-planning/partition-predicate-declared-types` | moved |
| `vs-adapter/pushdown-agg-sql-consolidation` | `pushdown-aggregates/pushdown-agg-sql-consolidation` | moved |
| `vs-adapter/pushdown-catalog-session` | `catalog/pushdown-catalog-session` | moved |
| `vs-adapter/pushdown-col-types-consolidation` | `pushdown/pushdown-col-types-consolidation` | moved |
| `vs-adapter/pushdown-declined-filter-self-apply` | `pushdown/pushdown-declined-filter-self-apply` | moved |
| `vs-adapter/pushdown-file-pruning` | `file-planning/pushdown-file-pruning` | moved |
| `vs-adapter/pushdown-format-neutral-resolution` | `file-planning/pushdown-format-neutral-resolution` | moved |
| `vs-adapter/pushdown-joins-module-structure` | `pushdown-joins/pushdown-joins-module-structure` | moved |
| `vs-adapter/pushdown-module-dedup-consolidation` | `pushdown/pushdown-module-dedup-consolidation` | moved |
| `vs-adapter/pushdown-module-structure` | `pushdown/pushdown-module-structure` | moved |
| `vs-adapter/pushdown-planning` | `pushdown/pushdown-planning` | moved |
| `vs-adapter/pushdown-planning-aggregate-extensions` | `pushdown-aggregates/pushdown-planning-aggregate-extensions` | moved |
| `vs-adapter/pushdown-planning-alias-stripping` | `pushdown/pushdown-planning-alias-stripping` | moved |
| `vs-adapter/pushdown-planning-capability-extensions` | `pushdown-capabilities/pushdown-planning-capability-extensions` | moved |
| `vs-adapter/pushdown-planning-capability-extensions-credential-reference` | `storage-access/pushdown-planning-capability-extensions-credential-reference` | moved |
| `vs-adapter/pushdown-planning-char-type-declaration` | `pushdown-types/pushdown-planning-char-type-declaration` | split half |
| `vs-adapter/pushdown-planning-char-type-declaration` | `pushdown-types/pushdown-planning-char-type-declaration-padding` | split half |
| `vs-adapter/pushdown-planning-cloud-credentials` | `storage-access/pushdown-planning-cloud-credentials` | split half |
| `vs-adapter/pushdown-planning-cloud-credentials` | `storage-access/pushdown-planning-cloud-credentials-vended-storage` | split half |
| `vs-adapter/pushdown-planning-count-distinct` | `pushdown-aggregates/pushdown-planning-count-distinct` | moved |
| `vs-adapter/pushdown-planning-decimal-string-format` | `pushdown-types/pushdown-planning-decimal-string-format` | moved |
| `vs-adapter/pushdown-planning-empty-result` | `file-planning/pushdown-planning-empty-result` | moved |
| `vs-adapter/pushdown-planning-expression-aggregate` | `pushdown-aggregates/pushdown-planning-expression-aggregate` | moved |
| `vs-adapter/pushdown-planning-file-encoding` | `file-planning/pushdown-planning-file-encoding` | moved |
| `vs-adapter/pushdown-planning-file-resolution` | `file-planning/pushdown-planning-file-resolution` | moved |
| `vs-adapter/pushdown-planning-grouped-agg` | `pushdown-grouped-agg/pushdown-planning-grouped-agg` | moved |
| `vs-adapter/pushdown-planning-grouped-agg-credential-reference` | `storage-access/pushdown-planning-grouped-agg-credential-reference` | moved |
| `vs-adapter/pushdown-planning-grouped-agg-multikey` | `pushdown-grouped-agg/pushdown-planning-grouped-agg-multikey` | moved |
| `vs-adapter/pushdown-planning-grouped-agg-scalar-over-aggregate` | `pushdown-grouped-agg/pushdown-planning-grouped-agg-scalar-over-aggregate` | moved |
| `vs-adapter/pushdown-planning-grouped-agg-wrapper-fallback` | `pushdown-grouped-agg/pushdown-planning-grouped-agg-wrapper-fallback` | moved |
| `vs-adapter/pushdown-planning-join` | `pushdown-joins/pushdown-planning-join` | moved |
| `vs-adapter/pushdown-planning-join-fallback` | `pushdown-joins/pushdown-planning-join-fallback` | moved |
| `vs-adapter/pushdown-planning-join-fallback-self-join` | `pushdown-joins/pushdown-planning-join-fallback-self-join` | moved |
| `vs-adapter/pushdown-planning-join-filter-type-coercion` | `pushdown-joins/pushdown-planning-join-filter-type-coercion` | moved |
| `vs-adapter/pushdown-planning-like-type-coercion` | `pushdown-types/pushdown-planning-like-type-coercion` | moved |
| `vs-adapter/pushdown-planning-literal-projection` | `pushdown/pushdown-planning-literal-projection` | moved |
| `vs-adapter/pushdown-planning-nested-aggregate-fallback` | `pushdown-aggregates/pushdown-planning-nested-aggregate-fallback` | moved |
| `vs-adapter/pushdown-planning-order-by-capability` | `pushdown-capabilities/pushdown-planning-order-by-capability` | moved |
| `vs-adapter/pushdown-planning-selectlist-expressions` | `pushdown/pushdown-planning-selectlist-expressions` | moved |
| `vs-adapter/pushdown-planning-single-group-agg` | `pushdown-aggregates/pushdown-planning-single-group-agg` | moved |
| `vs-adapter/pushdown-planning-single-group-agg-scalar-over-aggregate` | `pushdown-aggregates/pushdown-planning-single-group-agg-scalar-over-aggregate` | moved |
| `vs-adapter/pushdown-planning-string-conversion-capability` | `pushdown-capabilities/pushdown-planning-string-conversion-capability` | moved |
| `vs-adapter/pushdown-planning-string-fn-type-coercion` | `pushdown-types/pushdown-planning-string-fn-type-coercion` | moved |
| `vs-adapter/pushdown-planning-string-fn-type-coercion-composition` | `pushdown-types/pushdown-planning-string-fn-type-coercion-composition` | moved |
| `vs-adapter/pushdown-planning-topn` | `pushdown-capabilities/pushdown-planning-topn` | moved |
| `vs-adapter/rest-catalog-oauth-auth` | `catalog/rest-catalog-oauth-auth` | moved |
| `vs-adapter/scan-spec-credential-reference` | `storage-access/scan-spec-credential-reference` | moved |
| `vs-adapter/storage-backend-enum` | `storage-access/storage-backend-enum` | split half |
| `vs-adapter/storage-backend-enum` | `storage-access/storage-backend-enum-selection` | split half |
| `vs-adapter/unity-catalog-auth` | `unity-catalog/unity-catalog-auth` | moved |
| `vs-adapter/unity-catalog-client` | `unity-catalog/unity-catalog-client` | moved |
| `vs-adapter/unity-catalog-client-parquet-admission` | `unity-catalog/unity-catalog-client-parquet-admission` | moved |
| `vs-adapter/unity-catalog-create-virtual-schema` | `unity-catalog/unity-catalog-create-virtual-schema` | moved |
| `vs-adapter/unity-catalog-vended-credentials` | `unity-catalog/unity-catalog-vended-credentials` | moved |
| `vs-adapter/unity-parquet-table-planning` | `unity-catalog/unity-parquet-table-planning` | moved |

## Migration

| Current | New |
|---------|-----|
| 21 features in the six E2E domains | Removed. Each scenario's destination is in § Scenario Mapping |
| 3 fixture features in `packaging` | Removed. Their rules are bullets in `testing.md` § Testing › Fixtures |
| Harness, suite, fixture, unit-test, regression-guard, and CI rules in feature specs | `specs/testing.md` § Testing (§ Scenario Mapping, § Test-Rule Review) |
| Orphan sweep, remote benchmark harness, and AWS Lakekeeper benchmark catalog rules | `specs/testing.md` § Ops |
| `vs-adapter`, `datafusion-scan`, `sql-comprehension` above 8 features | 24 domains of at most 8 features (§ Domain Layout) |
| Six features above 10 scenarios | Two features each (§ Feature Moves, kind "split half") |
| Spec paths in specs | Rewritten by this plan's deltas |
| Spec paths in code comments, `AGENTS.md`, `specs/mission.md`, workflows, and `deploy/README.md` | Rewritten by tasks 2.1 and 2.3 (§ Reference Edits) |
| Spec paths in ADR fragments under `specs/_decision/` | Unchanged (decision-log.md [15]) |

## Scenario Mapping

Each of the 137 removed E2E scenarios has exactly one primary destination, given at its new path:

- `testing.md` § *section*: the scenario states only harness, test-run, or ops rules, and that section of `specs/testing.md` states them.
- dup of *feature* § "*title*": an existing feature scenario states the same observable product behavior (decision-log.md [3]). Test-run clauses of the removed scenario, such as the oracle or the fail-not-skip rule, go to `specs/testing.md`.
- moved to *feature* § "*title*": the product clause that no feature stated moves into that scenario (decision-log.md [8], [14]).

Two clauses are dropped instead of moved (decision-log.md [7]). The rows that carry them say so.

| Destination | Scenarios |
|-------------|-----------|
| `testing.md` § Testing | 32 |
| `testing.md` § Ops | 26 |
| Duplicate of an existing feature scenario | 67 |
| Moved into a feature scenario of this plan | 12 |
| Total | 137 |

### e2e-harness/cloud-e2e-harness (10)

| # | Scenario | Destination |
|---|----------|-------------|
| 1 | Cloud smoke test queries a real Glue-backed virtual schema | dup of `storage-access/pushdown-planning-cloud-credentials` § "Catalog REST requests to Glue are SigV4-signed when enabled" |
| 2 | Cloud test skips cleanly when AWS credentials are absent | `testing.md` § Testing › Failure contract |
| 3 | Cloud performance smoke records timing and row-count sanity | `testing.md` § Testing › Failure contract |
| 4 | Vended credentials are exercised end to end against Glue | dup of `storage-access/pushdown-planning-cloud-credentials` § "Vended S3 credentials are the sole storage source regardless of catalog auth mode" |
| 5 | A Glue CONNECTION that omits region lists the Glue table through SigV4-signed catalog requests | dup of `connection/connection-credentials-sigv4` § "A standard AWS Glue endpoint supplies the SigV4 signing region when the CONNECTION omits region" |
| 6 | Remote bench wires PARALLELISM_FACTOR into the virtual schema | `testing.md` § Ops › Remote benchmark harness |
| 7 | Cloud suite drives Exasol through the shared redacting WebSocket client | `testing.md` § Testing › Exasol session client |
| 8 | Remote bench selects its catalog backend from the bench environment | `testing.md` § Ops › Remote benchmark harness |
| 9 | The Lakekeeper CONNECTION password carries OAuth2 credentials and never SigV4 | `testing.md` § Ops › Remote benchmark harness |
| 10 | A completed remote run leaves the CONNECTION and virtual schema in place | `testing.md` § Ops › Remote benchmark harness |

### e2e-harness/direct-storage-e2e-properties (4)

| # | Scenario | Destination |
|---|----------|-------------|
| 11 | Only first-level directories holding a data file become virtual tables | dup of `direct-storage/direct-storage-table-discovery` § "A first-level directory under the base path is a table" |
| 12 | NAMESPACE scopes discovery to a subtree of the CONNECTION address | dup of `direct-storage/direct-storage-properties` § "NAMESPACE is optional under direct storage and scopes the table subtree" |
| 13 | A CONNECTION the direct-storage kind cannot accept is rejected at create time | dup of `connection/connection-credentials-direct-storage` § "The address scheme must agree with the credential shape" |
| 14 | Projection, filter, and LIMIT reach the direct-storage scan | dup of `file-planning/pushdown-format-neutral-resolution` § "Every pushdown request shape resolves through the one format-reader seam" |

### e2e-harness/direct-storage-e2e (9)

| # | Scenario | Destination |
|---|----------|-------------|
| 15 | The direct-storage binary is wired into the suite gate | `testing.md` § Testing › E2E suites |
| 16 | A raw-Parquet fixture writer puts data files with no catalog | `testing.md` § Testing › Fixtures |
| 17 | A directory of mixed-type Parquet files is declared and queried end to end | moved to `scan-types/type-mapping-live-coverage` § "Every type a Parquet file can carry declares and returns its mapped value on direct storage" |
| 18 | A column typed narrowly in one file and widely in another returns every row | dup of `scan-types/type-relaxation` § "A narrow physical column binds to the current wider logical type and is cast per file" |
| 19 | A file whose column set is a subset returns NULL for the columns it lacks | dup of `scan-read-path/scan-execution-field-id-projection` § "A logical field carrying no binding key binds by its own name" |
| 20 | A column no widening rule folds fails the refresh naming the column and the files | moved to `direct-storage/direct-storage-table-discovery` § "A first-level directory holding no data file is skipped, not failed" |
| 21 | MERGE_SCHEMA FALSE declares the sampled footer's type on the refresh and the scan path | dup of `direct-storage/direct-storage-properties` § "MERGE_SCHEMA selects one footer or every footer, on both the refresh and the plan path" |
| 22 | MERGE_SCHEMA FALSE narrows the declaration, not the file set | dup of `direct-storage/parquet-directory-seam` § "The merge mode selects every footer or exactly one" |
| 23 | A Delta table directory read as raw Parquet returns its tombstoned rows | dup of `direct-storage/direct-storage-table-planning` § "An Iceberg or Delta table directory read as raw Parquet ignores its table format" |

### e2e-harness/e2e-harness-grouped-agg (6)

| # | Scenario | Destination |
|---|----------|-------------|
| 24 | End-to-end grouped aggregate query returns correct per-group results | dup of `pushdown-grouped-agg/pushdown-planning-grouped-agg` § "Grouped aggregate wrapper SQL re-groups partial results per user group key" |
| 25 | End-to-end multi-key GROUP BY with WHERE filter returns correct results | dup of `pushdown-grouped-agg/pushdown-planning-grouped-agg-multikey` § "Each group key in a multi-key tuple resolves its own declared result type" |
| 26 | End-to-end GROUP BY with expression group key returns correct results | dup of `pushdown-grouped-agg/pushdown-planning-grouped-agg` § "Grouped scan spec carries group-key rendered SQL fragments" |
| 27 | End-to-end grouped AVG is correct across all groups | dup of `scan-aggregation/scan-execution-partial-agg` § "AVG is emitted as a partial sum and partial count pair" |
| 28 | High-cardinality grouped query completes via memory-pool spill | dup of `scan-aggregation/scan-execution-grouped-agg` § "Spill to disk is enabled when /tmp is real disk with free space" |
| 29 | End-to-end nested aggregate over a grouped sub-select returns the correct outer count | moved to `pushdown-aggregates/pushdown-planning-nested-aggregate-fallback` § "Composed pushdown request never renders a scan spec that references a non-source column" |

### e2e-harness/e2e-harness-grouped-order (7)

| # | Scenario | Destination |
|---|----------|-------------|
| 30 | End-to-end grouped aggregate with an aggregate before the group key returns correct results | dup of `pushdown-grouped-agg/pushdown-planning-grouped-agg` § "Outer wrapper SELECT preserves user select-list order for interleaved keys and aggregates" |
| 31 | End-to-end interleaved multi-key GROUP BY with an aggregate between the keys returns correct results | dup of `pushdown-grouped-agg/pushdown-planning-grouped-agg` § "Outer wrapper SELECT preserves user select-list order for interleaved keys and aggregates" |
| 32 | End-to-end expression group key placed after an aggregate returns correct results | dup of `pushdown-grouped-agg/pushdown-planning-grouped-agg` § "Outer wrapper SELECT preserves user select-list order for interleaved keys and aggregates" |
| 33 | End-to-end aggregate-first GROUP BY with HAVING returns correct results | dup of `pushdown-aggregates/pushdown-planning-aggregate-extensions` § "HAVING predicate is pushed into the grouped scan plan" |
| 34 | End-to-end multi-column GROUP BY over plain columns is pushed down (EXPLAIN-verified) | dup of `pushdown-grouped-agg/pushdown-planning-grouped-agg-multikey` § "Multi-column GROUP BY is pushed down as partial aggregation rather than a raw row scan" |
| 35 | End-to-end expression-valued multi-key tuple GROUP BY returns correct results | dup of `pushdown-grouped-agg/pushdown-planning-grouped-agg-multikey` § "Every element of a multi-key tuple may be an expression" |
| 36 | End-to-end multi-key GROUP BY with HAVING and LIMIT returns correct results | dup of `pushdown-grouped-agg/pushdown-planning-grouped-agg` § "LIMIT is NOT pushed into per-shard scan for a grouped query" |

### e2e-harness/e2e-harness-positional-deletes (8)

| # | Scenario | Destination |
|---|----------|-------------|
| 37 | End-to-end query over a file-granularity delete table returns post-delete rows | dup of `scan-read-path/scan-execution-positional-deletes` § "Positional deletes remove flagged rows (file granularity)" |
| 38 | End-to-end query over a partition-granularity delete table returns post-delete rows | dup of `scan-read-path/scan-execution-positional-deletes` § "A partition-granularity delete file is filtered to the data file being read" |
| 39 | End-to-end query over a multi-partition-spanning delete returns the exact post-delete set | dup of `scan-read-path/scan-execution-positional-deletes` § "A partition-granularity delete file is filtered to the data file being read" |
| 40 | Post-delete result is invariant across fan-out placement of affected data files | moved to `scan-read-path/scan-execution-positional-deletes-fanout` § "A shared delete file is read once per shard regardless of referencing data-file count" |
| 41 | End-to-end deletes compose with projection, filter, and LIMIT | dup of `scan-read-path/scan-execution-positional-deletes` § "Positional deletes compose with projection, filter, LIMIT, and pruning" |
| 42 | End-to-end deletes compose with aggregation | dup of `scan-read-path/scan-execution-positional-deletes` § "Positional deletes compose with projection, filter, LIMIT, and pruning" |
| 43 | End-to-end unsupported delete mechanism fails loud | dup of `file-planning/pushdown-file-pruning` § "An unsupported delete mechanism fails loud at plan time" |
| 44 | End-to-end delete-free table shows no regression | dup of `scan-read-path/scan-execution-positional-deletes` § "A delete-free data file scans through the same provider unchanged" |

### e2e-harness/e2e-harness-scan-correctness (7)

| # | Scenario | Destination |
|---|----------|-------------|
| 45 | End-to-end projection + filter + LIMIT query returns correct rows | dup of `datafusion-scan/scan-execution` § "Scan registers only its assigned files and returns matching rows" |
| 46 | Oversubscribed shard fan-out is observable via EXPLAIN VIRTUAL | dup of `parallelism/work-unit-sharding` § "Scan-driving query fans out via a nested distributor over a scalar scan UDF" |
| 47 | End-to-end filtered query over a partitioned table returns correct rows with file pruning | dup of `file-planning/pushdown-file-pruning` § "Equality on a partition column prunes data files" |
| 48 | An Iceberg table's list, struct, and map columns return valid JSON end to end | dup of `scan-types/nested-json-rendering` § "A list, struct, or map value renders as one valid JSON document" |
| 49 | Microsecond-distinct Iceberg timestamps round-trip at the declared precision | dup of `scan-types/type-mapping-timestamp-precision` § "A catalog timestamp column is declared TIMESTAMP(6) on Exasol 2025.x and later" |
| 50 | A VS timestamp compared as a rendered string uses a precision-matched oracle | `testing.md` § Testing › Correctness oracles |
| 51 | The scan returns correct values across the type mix with no spec-carried emit types | dup of `datafusion-scan/scan-execution-value-conversion` § "Output columns are coerced to the Arrow type the declared EMITS ExaType requires before emit_batch" |

### e2e-harness/e2e-harness (9)

| # | Scenario | Destination |
|---|----------|-------------|
| 52 | E2E suite fails when the stack is unavailable | `testing.md` § Testing › Failure contract |
| 53 | Harness provisions the scalar scan and the LUA distributor scripts | `testing.md` § Testing › Shared harness |
| 54 | Every E2E binary provisions the scan path from one shared harness definition | `testing.md` § Testing › Shared harness |
| 55 | A least-privilege user queries the VS and recovers no credential from the plan | moved to `storage-access/scan-spec-credential-reference` § "The scan script requires a script-scoped grant; rotation is observed per shard"; one clause dropped (decision-log.md [7]) |
| 56 | Harness statements carry no row cap the test did not declare | `testing.md` § Testing › Exasol session client |
| 57 | A declared row cap truncates the returned row count | `testing.md` § Testing › Exasol session client |
| 58 | Harness returns every row of a result set larger than one fetch response | `testing.md` § Testing › Exasol session client |
| 59 | The E2E suite gates on both supported Exasol major versions | `testing.md` § Testing › Make targets and CI |
| 60 | A least-privilege reader cannot execute the pushdown plan it can read | moved to `storage-access/scan-spec-credential-reference` § "A virtual schema reader can read the pushdown plan but cannot execute it" |

### azure-e2e/azure-e2e-harness-operations (7)

| # | Scenario | Destination |
|---|----------|-------------|
| 61 | Container name is legal for Azure and Lakekeeper whatever the user name contains | `testing.md` § Testing › Per-run cloud resources |
| 62 | Azure suite fails when a required credential variable is absent | `testing.md` § Testing › Failure contract |
| 63 | Azure suite fails when the local stack is unavailable | `testing.md` § Testing › Failure contract |
| 64 | The Azure Make target rebuilds the .so before running the suite | `testing.md` § Testing › Make targets and CI |
| 65 | Local credential file cannot be committed | `testing.md` § Testing › Credentials in tests |
| 66 | Azure binary provisions the scan path from the shared harness definition | `testing.md` § Testing › Shared harness |
| 67 | No Azure credential value appears in output when credential-bearing DDL fails | `testing.md` § Testing › Exasol session client |

### azure-e2e/azure-e2e-harness (5)

| # | Scenario | Destination |
|---|----------|-------------|
| 68 | Harness provisions a per-run container and one ADLS warehouse per credential mode | `testing.md` § Testing › Per-run cloud resources |
| 69 | End-to-end scan over the static-credential ADLS warehouse returns correct rows | dup of `connection/connection-credentials-azure` § "Azure credentials select the ADLS backend" |
| 70 | End-to-end scan over the vended-credential ADLS warehouse returns correct rows | dup of `storage-access/pushdown-planning-cloud-credentials-vended-storage` § "A vended Azure SAS is selected by host and carries a consistent account name" |
| 71 | Per-run container is deleted when its owning scope ends, including on panic | `testing.md` § Testing › Per-run cloud resources |
| 72 | End-to-end scan over a raw Parquet directory on ADLS returns correct rows | dup of `connection/connection-credentials-direct-storage` § "A direct-storage CONNECTION carries storage credentials and a storage base path" |

### azure-e2e/azure-orphan-container-sweep (8)

| # | Scenario | Destination |
|---|----------|-------------|
| 73 | Scheduled run reclaims stale orphaned containers | `testing.md` § Ops › Orphaned fixture sweeps |
| 74 | A container within the 24-hour retention floor is never swept | `testing.md` § Ops › Orphaned fixture sweeps |
| 75 | Sweep with nothing to reclaim succeeds without deleting | `testing.md` § Ops › Orphaned fixture sweeps |
| 76 | Manual dispatch previews by default | `testing.md` § Ops › Orphaned fixture sweeps |
| 77 | Manual dispatch with dry-run disabled deletes for real | `testing.md` § Ops › Orphaned fixture sweeps |
| 78 | Sweep fails loudly when a required variable is absent | `testing.md` § Ops › Orphaned fixture sweeps |
| 79 | Any Azure CLI failure fails the run | `testing.md` § Ops › Orphaned fixture sweeps |
| 80 | No credential value appears in the run log | `testing.md` § Ops › Orphaned fixture sweeps |

### unity-e2e/unity-catalog-e2e-harness-delta-queries (7)

| # | Scenario | Destination |
|---|----------|-------------|
| 81 | A delete-free Delta table returns its rows end to end | dup of `scan-read-path/scan-execution-delta-deletion-vectors` § "A Delta data file carrying no deletion vector scans unchanged" |
| 82 | A Delta table with deletion vectors returns only its live rows | dup of `scan-read-path/scan-execution-delta-deletion-vectors` § "Deletion vectors compose with projection, filter, LIMIT, and aggregation" |
| 83 | A column-mapped Delta table returns values under its logical column names | dup of `delta/delta-table-planning` § "Each logical field carries the binding key its column-mapping mode selects" |
| 84 | A partitioned Delta table returns its partition column values | dup of `scan-read-path/scan-execution-partition-values` § "A partition column absent from the data file is materialized per file" |
| 85 | Join and aggregate pushdown reach a Delta table by the same route as a scan | dup of `file-planning/pushdown-format-neutral-resolution` § "Every pushdown request shape resolves through the one format-reader seam" |
| 86 | A Delta table using an unsupported reader feature fails the query loud | dup of `delta/delta-reader-feature-gating` § "A reader feature outside the allow-list refuses the table before any log replay" |
| 87 | A query whose files were pruned returns the same rows as before pruning | dup of `delta/delta-file-pruning` § "Pruning reaches every request shape and changes no result end to end" |

### unity-e2e/unity-catalog-e2e-harness-delta-types (4)

| # | Scenario | Destination |
|---|----------|-------------|
| 88 | A type-widened Delta table returns its current wider types across the widening boundary | dup of `scan-types/type-relaxation` § "A narrow physical column binds to the current wider logical type and is cast per file" |
| 89 | A refused column refuses only the queries naming it | dup of `delta/delta-type-mapping` § "A refused column refuses only the requests that read or emit it" |
| 90 | A Delta table's varied types return their expected Exasol types and values | moved to `scan-types/type-mapping-live-coverage` § "Every Delta type declares and returns its mapped value through Unity Catalog" |
| 91 | A Delta timestamp column's declared Exasol type is asserted exactly at the engine's precision | dup of `scan-types/type-mapping-timestamp-precision` § "A catalog timestamp column is declared TIMESTAMP(6) on Exasol 2025.x and later" |

### unity-e2e/unity-catalog-e2e-harness-parquet-queries (3)

| # | Scenario | Destination |
|---|----------|-------------|
| 92 | A Unity Parquet table appears in the createVirtualSchema listing | moved to `scan-types/type-mapping-live-coverage` § "Every Spark type a Unity Parquet table declares returns its mapped value" |
| 93 | A Unity Parquet table returns its rows and partition values end to end | dup of `unity-catalog/unity-parquet-table-planning` § "Partition columns come from the catalog and partition values from the file paths" |
| 94 | A Unity Parquet table's scan resolves identically under vended and static credentials | dup of `unity-catalog/unity-parquet-table-planning` § "Storage is resolved through the table's own catalog exactly as for a Delta table" |

### unity-e2e/unity-catalog-e2e-harness (6)

| # | Scenario | Destination |
|---|----------|-------------|
| 95 | Harness brings up Unity Catalog and seeds the Delta fixtures | `testing.md` § Testing › Fixtures |
| 96 | Create virtual schema over a Unity Catalog namespace lists the fixture tables and columns | dup of `unity-catalog/unity-catalog-create-virtual-schema` § "Create virtual schema enumerates every table in the configured Unity Catalog namespace" |
| 97 | The suite resolves a seeded Delta table's scan spec over SeaweedFS under both credential modes | dup of `delta/delta-table-planning` § "Delta planning resolves its storage credential through the table's own catalog" |
| 98 | The Unity Catalog E2E suite fails when the stack is unavailable | `testing.md` § Testing › Failure contract |
| 99 | The Unity Catalog E2E suite leaks no credential value | dup of `delta/delta-table-planning` § "Delta planning resolves its storage credential through the table's own catalog" |
| 100 | The suite's virtual schema carries the storage credentials a UDF-side scan needs | moved to `unity-catalog/unity-catalog-vended-credentials` § "A catalog that vends no storage endpoint or credential is read through the CONNECTION's storage fields" |

### lakekeeper-e2e/aws-lakekeeper-perf-catalog (9)

| # | Scenario | Destination |
|---|----------|-------------|
| 101 | An ephemeral Lakekeeper stack stands up in the cluster's VPC | `testing.md` § Ops › AWS Lakekeeper benchmark catalog |
| 102 | Keycloak issues tokens both issuers accept | `testing.md` § Ops › AWS Lakekeeper benchmark catalog |
| 103 | The catalog's storage credential is separate from the engine's read-only credential | `testing.md` § Ops › AWS Lakekeeper benchmark catalog |
| 104 | Provisioning runs unchanged from an operator's laptop and from an EC2 box | `testing.md` § Ops › AWS Lakekeeper benchmark catalog |
| 105 | No credential reaches a process listing, standard output, or an error body | `testing.md` § Ops › AWS Lakekeeper benchmark catalog |
| 106 | Provisioning bootstraps Lakekeeper and creates the S3-backed warehouse idempotently | `testing.md` § Ops › AWS Lakekeeper benchmark catalog |
| 107 | Source-cataloged Iceberg tables are registered into the warehouse without a data rewrite | `testing.md` § Ops › AWS Lakekeeper benchmark catalog |
| 108 | Bench secrets carry both catalogs' variables from one environment | `testing.md` § Ops › AWS Lakekeeper benchmark catalog |
| 109 | Teardown removes only the Lakekeeper stack | `testing.md` § Ops › AWS Lakekeeper benchmark catalog |

### lakekeeper-e2e/lakekeeper-e2e-harness (8)

| # | Scenario | Destination |
|---|----------|-------------|
| 110 | Harness bootstraps Lakekeeper and creates the SeaweedFS-backed warehouses | `testing.md` § Testing › Per-run cloud resources |
| 111 | createVirtualSchema enumerates Lakekeeper tables over OAuth2 client-credentials auth | moved to `catalog/rest-catalog-oauth-auth` § "OAuth2 client-credentials path resolves tables from a multi-warehouse catalog served under a base path" |
| 112 | End-to-end scan over a static-credential Lakekeeper warehouse returns correct rows | dup of `storage-access/pushdown-planning-cloud-credentials` § "Static credentials are used for data files when vending is disabled" |
| 113 | End-to-end scan over a vended-credential Lakekeeper warehouse returns correct rows | dup of `storage-access/pushdown-planning-cloud-credentials` § "Vended credentials are extracted on the OAuth2 client-credentials catalog path"; one clause dropped (decision-log.md [7]) |
| 114 | Lakekeeper suite fails when the stack is unavailable | `testing.md` § Testing › Failure contract |
| 115 | Lakekeeper binary provisions the scan path from the shared harness definition | `testing.md` § Testing › Shared harness |
| 116 | A two-table broadcast join over a vended-credential warehouse returns correct rows | dup of `pushdown-joins/pushdown-planning-join` § "Broadcast-eligible inner equi-join is planned as a broadcast fan-out" |
| 117 | The vended credential scope divergence the defect needs is established by observation | `testing.md` § Testing › Correctness oracles |

### glue-e2e/glue-e2e-harness (7)

| # | Scenario | Destination |
|---|----------|-------------|
| 118 | Each run provisions its own Glue database and S3 prefix and removes both, including on panic | `testing.md` § Testing › Per-run cloud resources |
| 119 | The listing includes the routed tables and records every skip | dup of `glue/glue-catalog-client` § "The client routes a table by its declared table type before its storage descriptor" |
| 120 | Queries through pushdown return the expected rows | dup of `glue/glue-table-planning` § "Each kept partition's location is listed and its files carry the partition's Glue values" |
| 121 | An assume-role CONNECTION reads through the role and its base identity alone is denied | dup of `connection/connection-credentials-assume-role-session-use` § "Session credentials sign every native Glue request" |
| 122 | The suite fails, never skips, when a variable or the stack is missing | `testing.md` § Testing › Failure contract |
| 123 | The Make target and the CI job run the suite as a non-release gate | `testing.md` § Testing › Make targets and CI |
| 124 | No credential value appears in output | `testing.md` § Testing › Credentials in tests |

### glue-e2e/glue-orphan-sweep (5)

| # | Scenario | Destination |
|---|----------|-------------|
| 125 | A scheduled run deletes stale orphaned fixtures | `testing.md` § Ops › Orphaned fixture sweeps |
| 126 | A fixture younger than 24 hours is never swept | `testing.md` § Ops › Orphaned fixture sweeps |
| 127 | A manual dispatch previews by default | `testing.md` § Ops › Orphaned fixture sweeps |
| 128 | The sweep fails loudly when a variable is absent or an AWS call fails | `testing.md` § Ops › Orphaned fixture sweeps |
| 129 | No credential value appears in the run log | `testing.md` § Ops › Orphaned fixture sweeps |

### cloud-e2e/assume-role-e2e (5)

| # | Scenario | Destination |
|---|----------|-------------|
| 130 | The base identity alone is denied the warehouse bucket | moved to `file-planning/pushdown-planning-file-resolution` § "A storage read the object store denies fails the pushdown with the store's denial" |
| 131 | Naming the role reads the rows the base identity was denied | dup of `connection/connection-credentials-assume-role-session-use` § "Session credentials are the storage credential when the CONNECTION does not vend" |
| 132 | A wrong base secret fails CREATE VIRTUAL SCHEMA with HTTP 403 | dup of `connection/connection-credentials-assume-role` § "A failed AssumeRole is a credential-safe error" |
| 133 | A direct-storage CONNECTION naming the role lists and reads through the session | dup of `connection/connection-credentials-assume-role-session-use` § "Session credentials are the storage credential when the CONNECTION does not vend" |
| 134 | A Unity Catalog CONNECTION with static keys naming the role reads a Delta table through the session | dup of `connection/connection-credentials-assume-role-session-use` § "Session credentials are the storage credential when the CONNECTION does not vend" |

### cloud-e2e/cloud-assume-role-e2e (3)

| # | Scenario | Destination |
|---|----------|-------------|
| 135 | An assume-role CONNECTION reaches Glue and S3 through the assumed role | dup of `connection/connection-credentials-assume-role-session-use` § "Session credentials sign every SigV4 catalog request" |
| 136 | The assume-role base identity alone is denied by Glue | moved to `connection/connection-credentials-assume-role-session-use` § "A catalog denial of a CONNECTION that names no role is a credential-safe error" |
| 137 | A wrong external id is denied by STS | dup of `connection/connection-credentials-assume-role` § "A failed AssumeRole is a credential-safe error" |

## Test-Rule Review

Every remaining feature spec that mentions E2E or end-to-end was reviewed (decision-log.md [13]). Each passage that states only how a test is built, run, organized, or guarded moved to `testing.md`: the E2E requirements, and the unit-test, golden-fixture, crate-probe, regression, and arm64 unit-test CI rules found in the same specs. Where a moved passage was part of a scenario's steps, the product part stays in the scenario. One scenario, `scan-types/type-relaxation` § "Every supported relaxation pair is proven castable rather than assumed", is rewritten as a product scenario under a new title, and its test doc line follows it (§ Reference Edits). Two scenarios that held no product behavior are removed: `packaging/aarch64-ci-build` § "arm64 CI job runs unit tests without coverage or E2E" and `catalog/catalog-crate-structure` § "Every moved module keeps its own tests".

| Feature (new path) | Passage | Destination |
|--------------------|---------|-------------|
| `scan-types/nested-json-rendering` | clause rewritten: `h` — and prunes a row group that does contain the match. Row loss from pruning is silent, so this | `testing.md` § Correctness oracles (already stated) |
| `scan-types/nested-json-rendering` | clause: * *AND* this MUST be verified against a live Exasol instance rather than inferred from the capability registry… | `testing.md` § Correctness oracles |
| `scan-types/nested-json-rendering` | clause rewritten: * *AND* a predicate over a nested column MUST NOT prune any row group, page, or file on Parquet statistics, an… | `testing.md` § Correctness oracles |
| `scan-types/type-mapping-live-coverage` | clause: * Each technology's suite reads its all-types tables through the virtual schema that suite already creates. Th… | `testing.md` § Fixtures |
| `scan-types/type-mapping-live-coverage` | clause: * An all-types table holds the ids 1, 2, and 3. Rows 1 and 2 carry values, and row 3 is NULL in every column b… | `testing.md` § Fixtures |
| `scan-types/type-mapping-live-coverage` | clause: * Each suite compares a table's declared types, read from `SYS.EXA_ALL_COLUMNS`, with its exact expected list,… | `testing.md` § Correctness oracles |
| `scan-types/type-mapping-live-coverage` | clause rewritten: * A catalog-declared timestamp declares the precision `datafusion-scan/type-mapping-timestamp-precision` gates… | `testing.md` § Fixtures |
| `scan-types/type-relaxation` | scenario "Every supported relaxation pair is proven castable rather than assumed" rewritten as product scenario "Every supported relaxation pair casts without losing a value" | `testing.md` § Unit tests |
| `scan-types/type-relaxation` | clause: * *AND* the existing test list `supported_relaxation_pairs` SHALL STAY the concrete 17-entry pin it is today a… | `testing.md` § Unit tests |
| `scan-types/type-relaxation` | clause: * *AND* every existing assertion of this feature's test suite MUST pass with no change to any expected value, … | `testing.md` § Regression guards and probes |
| `scan-types/type-mapping-timestamp-precision` | clause rewritten: * `[C1]` and `[C2]` hold on 2025.1.16 as well, each confirmed by its own check because they answer | `testing.md` § Correctness oracles |
| `packaging/aarch64-ci-build` | clause rewritten: CI builds the UDF `.so` for both x86_64 and aarch64 on native runners, runs arm64 unit tests, and publishes ar… | `testing.md` § Make targets and CI (already stated) |
| `packaging/aarch64-ci-build` | clause: - E2E tests stay x86_64-only because `exasol/docker-db` publishes amd64-only images | `testing.md` § Make targets and CI |
| `packaging/aarch64-ci-build` | scenario "arm64 CI job runs unit tests without coverage or E2E" | `testing.md` § Make targets and CI |
| `sql-functions/vs-expression-translator-date-diff-fns` | clause rewritten: * `ADD_HOURS`, `ADD_MINUTES` — withdrawn after end-to-end parity testing (the parity gate this | `testing.md` § Correctness oracles (already stated) |
| `sql-functions/vs-expression-translator-date-diff-fns` | clause rewritten: * *AND* `FN_DAYS_BETWEEN` SHALL be advertised only while an end-to-end parity test confirms the DataFusion-dia… | `testing.md` § Correctness oracles |
| `sql-functions/vs-expression-translator-date-diff-fns` | clause rewritten: * *AND* `FN_HOURS_BETWEEN`, `FN_MINUTES_BETWEEN`, and `FN_SECONDS_BETWEEN` SHALL each be advertised only while… | `testing.md` § Correctness oracles (already stated) |
| `sql-functions/vs-expression-translator-concat` | clause: * *AND* the pushed-down SQL that `EXPLAIN VIRTUAL` reports SHALL carry `nullif(concat(` inside the scan spec, … | `testing.md` § Correctness oracles (already stated) |
| `sql-functions/vs-expression-translator-concat` | clause: * *AND* the test SHALL FAIL, not skip, when no Exasol container is reachable | `testing.md` § Failure contract (already stated) |
| `sql-comprehension/vs-expression-translator-float-div` | clause rewritten: rather than claimed away, and it is why an equality assertion against a native oracle must use a | `testing.md` § Correctness oracles |
| `sql-comprehension/vs-expression-translator-float-div` | clause rewritten: * *AND* parity against native Exasol SHALL be asserted as bit-exact for an integer (scale-0) numerator and as … | `testing.md` § Correctness oracles (already stated) |
| `sql-comprehension/vs-expression-translator-float-div` | clause rewritten: * *AND* the two dialects SHALL therefore DIVERGE on the same `FLOAT_DIV` node — `vs_checked_float_div(<l>, <r>… | `testing.md` § Coverage rule (already stated) |
| `sql-comprehension/vs-expression-translator-float-div` | clause rewritten: * *AND* every Exasol-dialect consumer's SQL SHALL stay byte-identical — the qualified single-table wrapper, th… | `testing.md` § Regression guards and probes |
| `sql-comprehension/vs-expression-translator-float-div` | clause: * *AND* the sweep's Exasol-dialect expectation for `FLOAT_DIV` SHALL remain the bare `(<l> / <r>)` — unchanged… | `testing.md` § Coverage rule (already stated) |
| `sql-comprehension/vs-expression-translator-float-div` | clause rewritten: * *AND* the sweep's banned-token list SHALL GAIN the checked-division function name, because Exasol has no suc… | `testing.md` § Unit tests |
| `sql-comprehension/vs-expression-translator-float-div` | clause: * *AND* `CAST` MUST still NOT be added to that list, since `CAST` is valid Exasol SQL that the CAST scenarios … | `testing.md` § Unit tests (already stated) |
| `sql-functions/vs-expression-translator-greatest-least` | clause: * *AND* the pushed-down SQL that `EXPLAIN VIRTUAL` reports SHALL carry the guarded form inside the scan spec, … | `testing.md` § Correctness oracles (already stated) |
| `sql-functions/vs-expression-translator-greatest-least` | clause: * *AND* the test SHALL FAIL, not skip, when no Exasol container is reachable | `testing.md` § Failure contract (already stated) |
| `catalog/catalog-crate-structure` | clause: * The MUST-NOT-name rule and the tests-move-with-their-code rule meet at exactly one place, and the rule wins … | `testing.md` § Regression guards and probes (already stated) |
| `catalog/catalog-crate-structure` | clause: * **The extraction's historical scenarios are left standing.** "Every moved module keeps its own | `testing.md` § Regression guards and probes (already stated) |
| `catalog/catalog-crate-structure` | clause rewritten: * *AND* `StorageBackend` SHALL expose NO accessor returning its `StorageProps` payload, so a caller outside th… | `testing.md` § Regression guards and probes |
| `catalog/catalog-crate-structure` | clause: * *AND* an external-vantage reachability probe at `crates/lakehouse-catalog/tests/catalog_public_surface.rs` S… | `testing.md` § Regression guards and probes |
| `catalog/catalog-crate-structure` | scenario "Every moved module keeps its own tests" | `testing.md` § Regression guards and probes |
| `catalog/catalog-crate-structure` | clause rewritten: * *GIVEN* the pre-extraction unit, integration, and E2E suites | `testing.md` § Regression guards and probes |
| `vs-adapter/create-virtual-schema-declaration-details` | clause: * The non-ASCII round trip below is a LIVE E2E scenario, not a unit one, because the property under | `testing.md` § Coverage rule (already stated) |
| `vs-adapter/create-virtual-schema-declaration-details` | clause: * The non-ASCII fixture MUST live in its OWN Iceberg namespace, not in `e2e_lakehouse`. Every | `testing.md` § Fixtures (already stated) |
| `vs-adapter/create-virtual-schema-declaration-details` | clause rewritten: * *GIVEN* a live Exasol instance, an Iceberg REST catalog, and an Iceberg table whose TABLE name and one of wh… | `testing.md` § Fixtures (already stated) |
| `vs-adapter/create-virtual-schema-declaration-details` | clause rewritten: * *AND* the adapter-GENERATED pushdown SQL for that same `LIKE` query SHALL carry the predicate over `"STRASSE… | `testing.md` § Correctness oracles |
| `vs-adapter/create-virtual-schema-declaration-details` | clause: * *AND* the scenario SHALL FAIL, not skip, when no live Exasol instance is available, per this repo's E2E cont… | `testing.md` § Failure contract (already stated) |
| `delta/delta-file-pruning` | clause rewritten: reached only from inside `format`. The new submodule carries its own sibling `_tests.rs`, per the | `testing.md` § Test tiers (already stated) |
| `delta/delta-file-pruning` | clause rewritten: * *AND* every scenario of `vs-adapter/delta-table-planning`, `vs-adapter/delta-reader-feature-gating`, and `vs… | `testing.md` § Regression guards and probes |
| `delta/delta-file-pruning` | clause: * *AND* the suite MUST fail (not skip) when the Unity Catalog server, SeaweedFS, or Exasol is unreachable | `testing.md` § Failure contract (already stated) |
| `delta/delta-table-planning` | clause rewritten: session with the table it reads.** `ScanSource` is NOT `CatalogKind`: the kind is a parsed | `testing.md` § Coverage rule (already stated) |
| `delta/delta-table-planning` | clause: * *AND* the existing Iceberg unit, integration, and E2E suites MUST pass with no change to any test | `testing.md` § Regression guards and probes |
| `vs-adapter/create-virtual-schema` | clause: * The added scenario is a LIVE E2E scenario, not a unit one, because the property under test is a round-trip t… | `testing.md` § Coverage rule (already stated) |
| `vs-adapter/create-virtual-schema` | clause: * The added scenario's fixture MUST live in its OWN Iceberg namespace, not in `e2e_lakehouse`. Every existing … | `testing.md` § Fixtures (already stated) |
| `pushdown/pushdown-col-types-consolidation` | clause: * That reasoning does NOT run in the opposite direction, which is what decides WHERE each new test is declared… | `testing.md` § Test tiers (already stated) |
| `pushdown/pushdown-col-types-consolidation` | clause rewritten: * The carve-out permits an edit to the `storage` value ALONE. Every other byte of every golden stays unedited,… | `testing.md` § Regression guards and probes |
| `pushdown/pushdown-col-types-consolidation` | clause rewritten: * *AND* a unit test SHALL pin the helper's fold SENSITIVITY at this boundary, over a CONSTRUCTED column name w… | `testing.md` § Unit tests |
| `pushdown/pushdown-col-types-consolidation` | clause: * *AND* that test SHALL construct both `col_types` slices as LITERALS — one carrying the Unicode-folded `STRAS… | `testing.md` § Unit tests |
| `pushdown/pushdown-col-types-consolidation` | clause rewritten: * *AND* the scan-driving SQL generated for every pushdown request MUST be byte-identical to its pre-extraction… | `testing.md` § Regression guards and probes |
| `pushdown/pushdown-col-types-consolidation` | clause: * *AND* the unit test `each_builder_keeps_its_own_case_fold_on_a_constructed_non_ascii_literal` (`joins/planni… | `testing.md` § Regression guards and probes |
| `pushdown/pushdown-col-types-consolidation` | clause: * *AND* NO replacement unified-fold test SHALL be added, because an agreement assertion over two wrappers that… | `testing.md` § Unit tests |
| `pushdown/pushdown-col-types-consolidation` | clause rewritten: * *AND* the scan-driving SQL for every single-table pushdown request and the join SQL for every join shape MUS… | `testing.md` § Regression guards and probes |
| `pushdown/pushdown-col-types-consolidation` | clause rewritten: * *AND* the scan-driving SQL generated for every pushdown request MUST be byte-identical to its pre-substituti… | `testing.md` § Regression guards and probes |
| `catalog/pushdown-catalog-session` | clause rewritten: * *AND* the generated broadcast or N-scan join SQL MUST be byte-identical to the pre-refactor output EXCEPT fo… | `testing.md` § Regression guards and probes |
| `catalog/pushdown-catalog-session` | clause rewritten: * *AND* the external E2E callers (`tests/common/e2e_harness.rs`, `tests/e2e_scan_test.rs`) SHALL construct a `… | `testing.md` § Shared harness |
| `file-planning/pushdown-format-neutral-resolution` | clause: * *AND* every existing test MUST pass with no change to any assertion or expected value, EXCEPT for | `testing.md` § Regression guards and probes |
| `file-planning/pushdown-format-neutral-resolution` | clause: * *AND* the compile-time signature pin asserting that Iceberg file resolution takes a SHARED catalog | `testing.md` § Regression guards and probes |
| `file-planning/pushdown-format-neutral-resolution` | clause: * *AND* the compile-time probe pinning that the capability set is assembled without the catalog kind SHALL sta… | `testing.md` § Regression guards and probes |
| `pushdown-capabilities/pushdown-planning-capability-extensions` | clause rewritten: * *WHEN* the test reads `SESSIONTIMEZONE` before asserting any value | `testing.md` § Correctness oracles |
| `pushdown-capabilities/pushdown-planning-capability-extensions` | clause rewritten: * *AND* a projected TSTZ LITERAL SHALL be asserted by EXACT value, which the two moving now-family values cann… | `testing.md` § Correctness oracles |
| `pushdown-joins/pushdown-planning-join-fallback-self-join` | clause rewritten: (verified live, and the premise `strip_table_alias` already documents) — so it is pinned by a | `testing.md` § Coverage rule |
| `pushdown-joins/pushdown-planning-join-fallback-self-join` | clause rewritten: * *AND* this state SHALL be UNREACHABLE for a well-formed request — a table joined to itself is only legal SQL… | `testing.md` § Coverage rule |
| `pushdown-aggregates/pushdown-planning-count-distinct` | clause rewritten: merge (`vs-adapter/pushdown-planning-single-group-agg`): no offset parameter, no collapse | `testing.md` § Correctness oracles |
| `pushdown-aggregates/pushdown-planning-count-distinct` | clause rewritten: * *AND* NULL values SHALL never be counted, and a zero-match or all-NULL result SHALL yield a distinct count o… | `testing.md` § Failure contract (already stated) |
| `pushdown-aggregates/pushdown-planning-count-distinct` | clause rewritten: * *AND* the scan SHALL NOT abort under any per-shard element or byte cap, because no such cap exists on this p… | `testing.md` § Failure contract (already stated) |
| `pushdown-aggregates/pushdown-planning-count-distinct` | clause rewritten: * *AND* the wrapper SHALL render NO `OFFSET`, because a non-zero request `limit.offset` cannot reach it: Exaso… | `testing.md` § Correctness oracles |
| `pushdown-aggregates/pushdown-planning-single-group-agg` | clause rewritten: collapse arithmetic, and NO failure branch: the unreachability is pinned by an assertion and | `testing.md` § Correctness oracles |
| `pushdown-aggregates/pushdown-planning-single-group-agg` | clause: * *AND* the unreachability SHALL be pinned by an assertion at the merge builder's call site AND by an end-to-e… | `testing.md` § Correctness oracles |
| `pushdown-capabilities/pushdown-planning-order-by-capability` | clause rewritten: * *AND* every render site an offset CANNOT reach SHALL be left without offset rendering, collapse arithmetic, … | `testing.md` § Correctness oracles |
| `pushdown-capabilities/pushdown-planning-order-by-capability` | clause rewritten: * *AND* each such unreachability claim SHALL be pinned by a LIVE end-to-end assertion in addition to any `debu… | `testing.md` § Correctness oracles |
| `pushdown-joins/pushdown-planning-join-filter-type-coercion` | clause: * The "Two N-scan sides sharing a column name" scenario is pinned at the PARTITION level only — it | `testing.md` § Coverage rule (already stated) |
| `pushdown-joins/pushdown-planning-join-filter-type-coercion` | clause: * *AND* no existing golden-SQL fixture covering a broadcast join or an N-scan fallback whose filter carries no… | `testing.md` § Regression guards and probes |

## Implementation Tasks

Tasks 2.1 to 3.5 run under `/speq:implement`. Tasks 5.1 to 5.4 run after it, in order, because `speq record` must apply the deltas before the emptied directories can be deleted. `tasks.md` holds the same list.

- 2.1 Rewrite the spec paths in comments and docs outside `specs/` that § Reference Edits › Path references lists.
- 2.2 Rewrite the `/// Scenario:` test doc lines that § Reference Edits › Scenario doc lines lists. Change no test name, body, or attribute.
- 2.3 Add the pointer lines and path fixes to `AGENTS.md` and `specs/mission.md` that § Reference Edits › Pointer lines lists (decision-log.md [1]).
- 3.1 to 3.5 Run the checks in § Verification › Checklist.
- 5.1 Run `/speq:record reorg-e2e-specs-into-testing`. Expect `speq record` to exit 1 on the 124 emptied specs (decision-log.md [2]).
- 5.2 Copy `specs/_recorded/<NNN>-reorg-e2e-specs-into-testing/testing.md` to `specs/testing.md`. `<NNN>` is the number `speq record` assigned, `047` unless another plan records first.
- 5.3 Confirm that each directory in § Dead Code Removal holds only a `spec.md` with no scenario, then delete those directories and the six E2E domain directories.
- 5.4 Run the post-record checks in § Verification › Checklist.

## Parallelization

| Group | Tasks | Depends on | Knowledge |
|-------|-------|------------|-----------|
| A: Reference edits | 2.1-2.3, 3.1-3.5 | none | plan.md § Feature Moves and § Reference Edits; every file § Reference Edits names |

Group A is the only group, because tasks 2.1 and 2.2 both edit `crates/lakehouse-engine/tests/common/glue.rs`, and every task is a small text edit. Tasks 5.1 to 5.4 belong to no group: the operator runs them after `/speq:implement`.

## Reference Edits

### Path references (task 2.1)

| Location | Old text | New text |
|----------|----------|----------|
| `.github/workflows/azure-orphan-sweep.yml:3` | `# See specs/azure-e2e/azure-orphan-container-sweep/spec.md.` | `# See specs/testing.md § Ops › Orphaned fixture sweeps.` |
| `.github/workflows/glue-orphan-sweep.yml:3` | `# See specs/glue-e2e/glue-orphan-sweep/spec.md.` | `# See specs/testing.md § Ops › Orphaned fixture sweeps.` |
| `crates/lakehouse-engine/tests/common/glue.rs:2` | ``(`glue-e2e/glue-e2e-harness`).`` | ``(`specs/testing.md` § Per-run cloud resources).`` |
| `crates/lakehouse-engine/tests/common/glue.rs:844` | ``/// Registers the whole fixture set of `glue-e2e/glue-e2e-harness` in the run's database.`` | `/// Registers the Glue E2E suite's whole fixture set in the run's database.` |
| `deploy/README.md:385` | ``the CONNECTION contract `e2e-harness/lakekeeper-e2e-harness` proves is`` | ``the CONNECTION contract that the Lakekeeper E2E suite (`specs/testing.md`) proves is`` |
| `crates/lakehouse-engine/src/adapter/pushdown/format/catalog_parquet_format_reader.rs:42` | `vs-adapter/glue-table-planning` | `glue/glue-table-planning` |
| `crates/lakehouse-engine/src/adapter/pushdown/joins/planning.rs:241` | `vs-adapter/pushdown-module-structure` | `pushdown/pushdown-module-structure` |
| `crates/lakehouse-engine/src/adapter/pushdown/joins/rendering.rs:206` | `vs-adapter/pushdown-module-structure` | `pushdown/pushdown-module-structure` |
| `crates/lakehouse-engine/src/adapter/pushdown/joins/rendering.rs:209` | `vs-adapter/pushdown-joins-module-structure` | `pushdown-joins/pushdown-joins-module-structure` |
| `crates/lakehouse-engine/src/adapter/pushdown_surface_probe_tests.rs:4` | `vs-adapter/pushdown-module-structure` | `pushdown/pushdown-module-structure` |
| `crates/lakehouse-engine/src/scan/partition_values.rs:177` | `vs-adapter/partition-predicate-declared-types` | `file-planning/partition-predicate-declared-types` |
| `crates/lakehouse-engine/src/types/hive_type.rs:3` | `vs-adapter/glue-hive-type-mapping` | `glue/glue-hive-type-mapping` |
| `crates/lakehouse-engine/src/types/hive_type_cases_tests.rs:13` | `vs-adapter/glue-hive-type-mapping` | `glue/glue-hive-type-mapping` |
| `crates/lakehouse-engine/src/types/hive_type_cases_tests.rs:36` | `vs-adapter/glue-hive-type-mapping` | `glue/glue-hive-type-mapping` |
| `crates/lakehouse-engine/tests/common/glue.rs:473` | `vs-adapter/glue-hive-type-mapping` | `glue/glue-hive-type-mapping` |
| `crates/lakehouse-engine/tests/pushdown_public_surface.rs:4` | `vs-adapter/pushdown-module-structure` | `pushdown/pushdown-module-structure` |

Each code line changes the domain part of one spec path only. `binary-column-refusal` stays in `vs-adapter`, so the references to it in `parquet_directory.rs:95`, `format/mod.rs:72`, and `hive_type_cases_tests.rs:64` stay. `glue.rs:61`, `:64`, and `:229` name the workflow file `glue-orphan-sweep.yml`, which stays. `glue.rs:67` is the string constant `"glue-e2e-harness"`, a credentials-provider name in code, and stays.

### Scenario doc lines (task 2.2)

A line for a duplicate quotes the covering scenario's title. A line for a moved clause quotes the destination scenario's title. A line for a harness rule becomes `/// specs/testing.md § <section>` (decision-log.md [6]). The last row follows the rewritten type-relaxation scenario (§ Test-Rule Review). Nine of the lines quote the old title with a lowercase first letter. They are rewritten the same way.

| Location | Mapping row | New line |
|----------|-------------|----------|
| `crates/lakehouse-engine/tests/e2e_glue_test.rs:160` | 118 | `/// specs/testing.md § Per-run cloud resources` |
| `crates/lakehouse-engine/tests/e2e_glue_test.rs:263` | 119 | `/// Scenario: The client routes a table by its declared table type before its storage descriptor` |
| `crates/lakehouse-engine/tests/e2e_glue_test.rs:344` | 120 | `/// Scenario: Each kept partition's location is listed and its files carry the partition's Glue values` |
| `crates/lakehouse-engine/tests/e2e_glue_test.rs:432` | 120 | `/// Scenario: A kept partition the reader cannot read faithfully fails the query naming it` |
| `crates/lakehouse-engine/tests/e2e_glue_test.rs:590` | 121 | `/// Scenario: Session credentials sign every native Glue request` |
| `crates/lakehouse-engine/tests/e2e_glue_test.rs:651` | 122 | `/// specs/testing.md § Failure contract` |
| `crates/lakehouse-engine/tests/e2e_glue_test.rs:663` | 124 | `/// specs/testing.md § Credentials in tests` |
| `crates/lakehouse-engine/tests/common/glue.rs:1275` | 118 | `/// specs/testing.md § Per-run cloud resources` |
| `crates/lakehouse-engine/tests/common/glue.rs:1299` | 122 | `/// specs/testing.md § Failure contract` |
| `crates/lakehouse-engine/tests/common/glue.rs:1334` | 124 | `/// specs/testing.md § Credentials in tests` |
| `crates/lakehouse-engine/tests/e2e_complex_type_test.rs:66` | 48 | `/// Scenario: A list, struct, or map value renders as one valid JSON document` |
| `crates/lakehouse-engine/tests/e2e_positional_deletes_test.rs:414` | 41 | `/// Scenario: Positional deletes compose with projection, filter, LIMIT, and pruning` |
| `crates/lakehouse-engine/tests/e2e_emit_declaration_test.rs:144` | 51 | `/// Scenario: Output columns are coerced to the Arrow type the declared EMITS ExaType requires before emit_batch` |
| `crates/lakehouse-engine/tests/e2e_unity_test.rs:815` | 81 | `/// Scenario: A Delta data file carrying no deletion vector scans unchanged` |
| `crates/lakehouse-engine/tests/e2e_unity_test.rs:846` | 134 | `/// Scenario: Session credentials are the storage credential when the CONNECTION does not vend` |
| `crates/lakehouse-engine/tests/e2e_unity_test.rs:872` | 82 | `/// Scenario: Deletion vectors compose with projection, filter, LIMIT, and aggregation` |
| `crates/lakehouse-engine/tests/e2e_unity_test.rs:909` | 83 | `/// Scenario: Each logical field carries the binding key its column-mapping mode selects` |
| `crates/lakehouse-engine/tests/e2e_unity_test.rs:950` | 84 | `/// Scenario: A partition column absent from the data file is materialized per file` |
| `crates/lakehouse-engine/tests/e2e_unity_test.rs:1028` | 85 | `/// Scenario: Every pushdown request shape resolves through the one format-reader seam` |
| `crates/lakehouse-engine/tests/e2e_unity_test.rs:1179` | 86 | `/// Scenario: A reader feature outside the allow-list refuses the table before any log replay` |
| `crates/lakehouse-engine/tests/e2e_unity_test.rs:1222` | 88 | `/// Scenario: A narrow physical column binds to the current wider logical type and is cast per file` |
| `crates/lakehouse-engine/tests/e2e_unity_test.rs:1404` | 90 | `/// Scenario: Every Delta type declares and returns its mapped value through Unity Catalog` |
| `crates/lakehouse-engine/tests/e2e_unity_test.rs:1934` | 94 | `/// Scenario: Storage is resolved through the table's own catalog exactly as for a Delta table` |
| `crates/lakehouse-engine/src/scan/type_relaxation_tests.rs:67` | test-rule review | `/// Scenario: Every supported relaxation pair casts without losing a value` |

### Pointer lines (task 2.3)

| Location | Edit |
|----------|------|
| `AGENTS.md` § Testing, after the `validateservercertificate=0` bullet | Add: `` - Read `specs/testing.md` before adding or changing an E2E suite, fixture, or harness helper. It holds the coverage rule, the suite layout, and the ops rules for the orphan sweeps and the benchmark catalog. `` |
| `AGENTS.md:62` | Replace `specs/datafusion-scan/type-mapping/spec.md` with `specs/scan-types/type-mapping/spec.md` |
| `specs/mission.md:52` | Replace `datafusion-scan/scan-execution-delta-deletion-vectors` with `scan-read-path/scan-execution-delta-deletion-vectors` |
| `specs/mission.md` Core Capability 13, end of the paragraph | Append: `` `specs/testing.md` states how the suites are built, run, and cleaned up. `` |

## Dead Code Removal

No code, test, or module is removed. Task 5.3 deletes the spec directories that `speq record` empties:

The 21 feature directories of the six E2E domains, and the domain directories themselves: `specs/e2e-harness`, `specs/azure-e2e`, `specs/unity-e2e`, `specs/lakekeeper-e2e`, `specs/glue-e2e`, `specs/cloud-e2e`.

The 103 old feature directories of § Feature Moves:

- `specs/datafusion-scan/nested-json-rendering`
- `specs/datafusion-scan/scan-execution-connection-concurrency`
- `specs/datafusion-scan/scan-execution-delta-deletion-vectors`
- `specs/datafusion-scan/scan-execution-field-id-projection`
- `specs/datafusion-scan/scan-execution-grouped-agg`
- `specs/datafusion-scan/scan-execution-join`
- `specs/datafusion-scan/scan-execution-memory-and-credentials`
- `specs/datafusion-scan/scan-execution-partial-agg`
- `specs/datafusion-scan/scan-execution-partition-values`
- `specs/datafusion-scan/scan-execution-positional-deletes`
- `specs/datafusion-scan/scan-execution-positional-deletes-fanout`
- `specs/datafusion-scan/scan-execution-telemetry`
- `specs/datafusion-scan/scan-execution-threading`
- `specs/datafusion-scan/scan-partial-agg-column-contract`
- `specs/datafusion-scan/type-mapping`
- `specs/datafusion-scan/type-mapping-live-coverage`
- `specs/datafusion-scan/type-mapping-module-structure`
- `specs/datafusion-scan/type-mapping-timestamp-precision`
- `specs/datafusion-scan/type-relaxation`
- `specs/packaging/iceberg-type-promotion-fixture`
- `specs/packaging/int96-timestamp-fixture`
- `specs/packaging/positional-delete-fixtures`
- `specs/sql-comprehension/vs-expression-translator-concat`
- `specs/sql-comprehension/vs-expression-translator-date-diff-fns`
- `specs/sql-comprehension/vs-expression-translator-date-fns`
- `specs/sql-comprehension/vs-expression-translator-greatest-least`
- `specs/sql-comprehension/vs-expression-translator-scalar-fns`
- `specs/vs-adapter/catalog-crate-public-surface-extensions`
- `specs/vs-adapter/catalog-crate-public-surface-extensions-glue`
- `specs/vs-adapter/catalog-crate-public-surface-extensions-unity-parquet`
- `specs/vs-adapter/catalog-crate-structure`
- `specs/vs-adapter/connection-credentials`
- `specs/vs-adapter/connection-credentials-assume-role`
- `specs/vs-adapter/connection-credentials-azure`
- `specs/vs-adapter/connection-credentials-catalog-auth`
- `specs/vs-adapter/connection-credentials-direct-storage`
- `specs/vs-adapter/connection-credentials-sigv4`
- `specs/vs-adapter/connection-credentials-unity-catalog`
- `specs/vs-adapter/delta-file-pruning`
- `specs/vs-adapter/delta-reader-feature-gating`
- `specs/vs-adapter/delta-table-planning`
- `specs/vs-adapter/delta-type-mapping`
- `specs/vs-adapter/direct-storage-hive-partitioning`
- `specs/vs-adapter/direct-storage-properties`
- `specs/vs-adapter/direct-storage-table-discovery`
- `specs/vs-adapter/direct-storage-table-planning`
- `specs/vs-adapter/glue-catalog-client`
- `specs/vs-adapter/glue-hive-type-mapping`
- `specs/vs-adapter/glue-table-planning`
- `specs/vs-adapter/iceberg-type-promotion`
- `specs/vs-adapter/parquet-directory-seam`
- `specs/vs-adapter/partition-predicate-declared-types`
- `specs/vs-adapter/pushdown-agg-sql-consolidation`
- `specs/vs-adapter/pushdown-catalog-session`
- `specs/vs-adapter/pushdown-col-types-consolidation`
- `specs/vs-adapter/pushdown-declined-filter-self-apply`
- `specs/vs-adapter/pushdown-file-pruning`
- `specs/vs-adapter/pushdown-format-neutral-resolution`
- `specs/vs-adapter/pushdown-joins-module-structure`
- `specs/vs-adapter/pushdown-module-dedup-consolidation`
- `specs/vs-adapter/pushdown-module-structure`
- `specs/vs-adapter/pushdown-planning`
- `specs/vs-adapter/pushdown-planning-aggregate-extensions`
- `specs/vs-adapter/pushdown-planning-alias-stripping`
- `specs/vs-adapter/pushdown-planning-capability-extensions`
- `specs/vs-adapter/pushdown-planning-capability-extensions-credential-reference`
- `specs/vs-adapter/pushdown-planning-char-type-declaration`
- `specs/vs-adapter/pushdown-planning-cloud-credentials`
- `specs/vs-adapter/pushdown-planning-count-distinct`
- `specs/vs-adapter/pushdown-planning-decimal-string-format`
- `specs/vs-adapter/pushdown-planning-empty-result`
- `specs/vs-adapter/pushdown-planning-expression-aggregate`
- `specs/vs-adapter/pushdown-planning-file-encoding`
- `specs/vs-adapter/pushdown-planning-file-resolution`
- `specs/vs-adapter/pushdown-planning-grouped-agg`
- `specs/vs-adapter/pushdown-planning-grouped-agg-credential-reference`
- `specs/vs-adapter/pushdown-planning-grouped-agg-multikey`
- `specs/vs-adapter/pushdown-planning-grouped-agg-scalar-over-aggregate`
- `specs/vs-adapter/pushdown-planning-grouped-agg-wrapper-fallback`
- `specs/vs-adapter/pushdown-planning-join`
- `specs/vs-adapter/pushdown-planning-join-fallback`
- `specs/vs-adapter/pushdown-planning-join-fallback-self-join`
- `specs/vs-adapter/pushdown-planning-join-filter-type-coercion`
- `specs/vs-adapter/pushdown-planning-like-type-coercion`
- `specs/vs-adapter/pushdown-planning-literal-projection`
- `specs/vs-adapter/pushdown-planning-nested-aggregate-fallback`
- `specs/vs-adapter/pushdown-planning-order-by-capability`
- `specs/vs-adapter/pushdown-planning-selectlist-expressions`
- `specs/vs-adapter/pushdown-planning-single-group-agg`
- `specs/vs-adapter/pushdown-planning-single-group-agg-scalar-over-aggregate`
- `specs/vs-adapter/pushdown-planning-string-conversion-capability`
- `specs/vs-adapter/pushdown-planning-string-fn-type-coercion`
- `specs/vs-adapter/pushdown-planning-string-fn-type-coercion-composition`
- `specs/vs-adapter/pushdown-planning-topn`
- `specs/vs-adapter/rest-catalog-oauth-auth`
- `specs/vs-adapter/scan-spec-credential-reference`
- `specs/vs-adapter/storage-backend-enum`
- `specs/vs-adapter/unity-catalog-auth`
- `specs/vs-adapter/unity-catalog-client`
- `specs/vs-adapter/unity-catalog-client-parquet-admission`
- `specs/vs-adapter/unity-catalog-create-virtual-schema`
- `specs/vs-adapter/unity-catalog-vended-credentials`
- `specs/vs-adapter/unity-parquet-table-planning`

## Verification

### Scenario Coverage

The plan adds no product behavior. Each changed or new scenario is already exercised by an existing test, listed below. A moved or split feature keeps its scenarios and their tests unchanged. The REMOVED scenarios need no test.

| Scenario | Test Type | Test Location | Test Name |
|----------|-----------|---------------|-----------|
| OAuth2 client-credentials path resolves tables from a multi-warehouse catalog served under a base path | E2E | `crates/lakehouse-engine/tests/e2e_lakekeeper_test.rs` | `lakekeeper_oauth_prefix_under_base_path_resolves`, `lakekeeper_create_virtual_schema_lists_tables_over_oidc` |
| The scan script requires a script-scoped grant; rotation is observed per shard | E2E | `crates/lakehouse-engine/tests/e2e_credential_exposure_test.rs` | `a_least_privilege_reader_gets_the_owners_rows_without_a_connection_grant`, `revoking_the_owners_scan_grant_denies_the_reader_without_leaking_the_credential` |
| A virtual schema reader can read the pushdown plan but cannot execute it | E2E | `crates/lakehouse-engine/tests/e2e_credential_exposure_test.rs` | `the_reader_cannot_execute_the_pushdown_plan_it_captured` |
| Composed pushdown request never renders a scan spec that references a non-source column | Unit, E2E | `crates/lakehouse-engine/src/adapter/pushdown/grouped_agg_tests.rs`, `crates/lakehouse-engine/tests/e2e_scan_test.rs` | `composed_nested_aggregate_request_does_not_reference_phantom_column`, `e2e_nested_aggregate_over_grouped_subselect_returns_correct_count` |
| A first-level directory holding no data file is skipped, not failed | E2E | `crates/lakehouse-engine/tests/e2e_direct_storage_test.rs` | `loose_file_and_empty_directory_serve_no_table`, `incompatible_pair_fails_create_and_refresh_naming_column_and_files` |
| A storage read the object store denies fails the pushdown with the store's denial | E2E | `crates/lakehouse-engine/tests/e2e_assume_role_test.rs` | `the_base_identity_alone_is_denied_the_warehouse_bucket` |
| A shared delete file is read once per shard regardless of referencing data-file count | Integration, E2E | `crates/lakehouse-engine/tests/scan_positional_deletes.rs`, `crates/lakehouse-engine/tests/e2e_positional_deletes_test.rs` | `scan_reads_shared_delete_file_once_per_shard`, `e2e_partition_delete_invariant_across_fanout` |
| Every type a Parquet file can carry declares and returns its mapped value on direct storage | E2E | `crates/lakehouse-engine/tests/e2e_direct_storage_test.rs` | `all_types_directories_declare_and_return_their_mapped_values`, `events_directory_declares_and_returns_mixed_types_across_both_files`, `complex_directory_declares_varchar_and_returns_parseable_json` |
| Every Delta type declares and returns its mapped value through Unity Catalog | E2E | `crates/lakehouse-engine/tests/e2e_unity_test.rs` | `unity_delta_extra_types_declare_and_return_their_mapped_values`, `unity_delta_varied_types_return_their_expected_exasol_types_and_values` |
| Every Spark type a Unity Parquet table declares returns its mapped value | E2E | `crates/lakehouse-engine/tests/e2e_unity_test.rs` | `unity_parquet_all_types_declare_and_return_their_mapped_values`, `unity_parquet_table_is_listed_and_returns_its_rows_and_partition_values` |
| Every allow-listed reader feature keeps its table queryable | Unit | `crates/lakehouse-engine/src/adapter/pushdown/format/delta_protocol_tests.rs` | `both_type_widening_variants_are_allow_listed_and_pass_the_gate` |
| A catalog that vends no storage endpoint or credential is read through the CONNECTION's storage fields | E2E | `crates/lakehouse-engine/tests/e2e_unity_test.rs` | `unity_delta_delete_free_table_returns_its_rows`, `unity_create_virtual_schema_lists_fixture_tables_and_columns` |
| A catalog denial of a CONNECTION that names no role is a credential-safe error | E2E (opt-in `cloud-e2e`) | `crates/lakehouse-engine/tests/cloud_e2e_test.rs` | `cloud_assume_role_base_identity_alone_is_denied` |

### Manual Testing

| Feature | Command | Expected Output |
|---------|---------|-----------------|
| Domain layout after task 5.3 | `speq domain list` | The 24 domains of § Domain Layout |
| Library limits | `for d in specs/*/; do ls -d $d*/ 2>/dev/null \| wc -l; done \| sort -n \| tail -1` and `grep -c '^### Scenario' specs/*/*/spec.md \| sort -t: -k2 -n \| tail -1` | At most 8 features per domain, at most 10 scenarios per feature |
| `specs/testing.md` after task 5.2 | `grep -n '^## ' specs/testing.md` | `## Testing` and `## Ops` |
| Search no longer returns harness scenarios | `speq search index && speq search query "suite fails when the stack is unavailable"` | No hit under a removed domain |
| A moved feature resolves at its new path | `speq feature get 'storage-access/scan-spec-credential-reference/A virtual schema reader can read the pushdown plan but cannot execute it'` | Prints the scenario |
| No stale path reference | the dangling-path check of `tasks.md` 5.4: every old-domain path token in a tracked file outside `specs/_recorded` and `specs/_decision` must name an existing `specs/<domain>/<feature>/spec.md` | No output |

### Checklist

| Step | Command | Expected |
|------|---------|----------|
| 3.1 Build and lint every target, including the feature-gated E2E binaries | `cargo clippy --workspace --all-targets --all-features -- -D warnings` | Exit 0, 0 warnings |
| 3.2 Format | `cargo fmt --all -- --check` | No changes |
| 3.3 Test | `cargo test` | 0 failures |
| 3.4 Comment-only diff | `git diff -U0 -- crates .github deploy AGENTS.md specs/mission.md` | Every changed line is a comment, a doc comment, or Markdown prose |
| 3.5 Plan | `speq plan validate reorg-e2e-specs-into-testing` | Validation passed |
| 5.4 Library | `speq feature validate` | Exit 0 |
| 5.4 Domains | `speq domain list` | The 24 domains of § Domain Layout |
| 5.4 References | the dangling-path check of `tasks.md` 5.4 | No output |

No change touches the `.so` sources' behavior, so `make cross-udf-build` and the E2E suites are not required. Step 3.1 compiles every E2E test binary, so a misplaced comment fails there.
