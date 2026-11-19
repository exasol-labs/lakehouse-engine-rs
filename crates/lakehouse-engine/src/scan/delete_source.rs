//! Wraps a Parquet file source so that a delete-carrying data file gets its access plan when
//! DataFusion opens it, not when the scan is planned.

use crate::scan::shared_files::ScanFiles;
use datafusion::common::Result;
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::physical_plan::{FileOpener, FileScanConfig, FileSource};
use datafusion::datasource::table_schema::TableSchema;
use datafusion::physical_expr::projection::ProjectionExprs;
use datafusion::physical_expr::{EquivalenceProperties, LexOrdering, PhysicalExpr};
use datafusion::physical_expr_common::sort_expr::PhysicalSortExpr;
use datafusion::physical_plan::filter_pushdown::FilterPushdownPropagation;
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion::physical_plan::{DisplayFormatType, SortOrderPushdownResult};
use datafusion_datasource::morsel::{MorselPlan, MorselPlanner, Morselizer};
use object_store::ObjectStore;
use std::fmt::Formatter;
use std::sync::Arc;

/// Every method that returns a rewritten source wraps it again. If a rewrite ever drops the
/// wrapper, the reader factory refuses the file (see `SharedReaderFactory`).
pub(super) struct DeleteApplyingSource {
    inner: Arc<dyn FileSource>,
    files: Arc<ScanFiles>,
}

impl DeleteApplyingSource {
    pub(super) fn wrap(inner: Arc<dyn FileSource>, files: &Arc<ScanFiles>) -> Arc<dyn FileSource> {
        Arc::new(Self {
            inner,
            files: Arc::clone(files),
        })
    }
}

impl FileSource for DeleteApplyingSource {
    fn create_file_opener(
        &self,
        object_store: Arc<dyn ObjectStore>,
        base_config: &FileScanConfig,
        partition: usize,
    ) -> Result<Arc<dyn FileOpener>> {
        self.inner
            .create_file_opener(object_store, base_config, partition)
    }

    fn create_morselizer(
        &self,
        object_store: Arc<dyn ObjectStore>,
        base_config: &FileScanConfig,
        partition: usize,
    ) -> Result<Box<dyn Morselizer>> {
        let inner = self
            .inner
            .create_morselizer(object_store, base_config, partition)?;
        Ok(Box::new(DeleteApplyingMorselizer {
            inner: Arc::from(inner),
            files: Arc::clone(&self.files),
        }))
    }

    fn table_schema(&self) -> &TableSchema {
        self.inner.table_schema()
    }

    fn with_batch_size(&self, batch_size: usize) -> Arc<dyn FileSource> {
        Self::wrap(self.inner.with_batch_size(batch_size), &self.files)
    }

    fn filter(&self) -> Option<Arc<dyn PhysicalExpr>> {
        self.inner.filter()
    }

    fn projection(&self) -> Option<&ProjectionExprs> {
        self.inner.projection()
    }

    fn metrics(&self) -> &ExecutionPlanMetricsSet {
        self.inner.metrics()
    }

    fn file_type(&self) -> &str {
        self.inner.file_type()
    }

    fn fmt_extra(&self, t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        self.inner.fmt_extra(t, f)
    }

    fn supports_repartitioning(&self) -> bool {
        self.inner.supports_repartitioning()
    }

    fn repartitioned(
        &self,
        target_partitions: usize,
        repartition_file_min_size: usize,
        output_ordering: Option<LexOrdering>,
        config: &FileScanConfig,
    ) -> Result<Option<FileScanConfig>> {
        self.inner.repartitioned(
            target_partitions,
            repartition_file_min_size,
            output_ordering,
            config,
        )
    }

    fn try_pushdown_filters(
        &self,
        filters: Vec<Arc<dyn PhysicalExpr>>,
        config: &ConfigOptions,
    ) -> Result<FilterPushdownPropagation<Arc<dyn FileSource>>> {
        let pushed = self.inner.try_pushdown_filters(filters, config)?;
        Ok(FilterPushdownPropagation {
            filters: pushed.filters,
            updated_node: pushed
                .updated_node
                .map(|inner| Self::wrap(inner, &self.files)),
        })
    }

    fn try_pushdown_sort(
        &self,
        order: &[PhysicalSortExpr],
        eq_properties: &EquivalenceProperties,
    ) -> Result<SortOrderPushdownResult<Arc<dyn FileSource>>> {
        Ok(self
            .inner
            .try_pushdown_sort(order, eq_properties)?
            .map(|inner| Self::wrap(inner, &self.files)))
    }

    fn reorder_files(&self, files: Vec<PartitionedFile>) -> Vec<PartitionedFile> {
        self.inner.reorder_files(files)
    }

    fn try_pushdown_projection(
        &self,
        projection: &ProjectionExprs,
    ) -> Result<Option<Arc<dyn FileSource>>> {
        Ok(self
            .inner
            .try_pushdown_projection(projection)?
            .map(|inner| Self::wrap(inner, &self.files)))
    }

    fn apply_expressions(
        &self,
        f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        self.inner.apply_expressions(f)
    }
}

#[derive(Debug)]
struct DeleteApplyingMorselizer {
    inner: Arc<dyn Morselizer>,
    files: Arc<ScanFiles>,
}

impl Morselizer for DeleteApplyingMorselizer {
    fn plan_file(&self, file: PartitionedFile) -> Result<Box<dyn MorselPlanner>> {
        if !self.files.carries_deletes(&file.object_meta.location) {
            return self.inner.plan_file(file);
        }
        Ok(Box::new(DeleteApplyingPlanner {
            inner: Arc::clone(&self.inner),
            files: Arc::clone(&self.files),
            file,
        }))
    }
}

#[derive(Debug)]
struct DeleteApplyingPlanner {
    inner: Arc<dyn Morselizer>,
    files: Arc<ScanFiles>,
    file: PartitionedFile,
}

impl MorselPlanner for DeleteApplyingPlanner {
    /// One pending I/O loads the footer and the deletes together, because a stream allows only
    /// one outstanding planner I/O.
    fn plan(self: Box<Self>) -> Result<Option<MorselPlan>> {
        let Self { inner, files, file } = *self;
        Ok(Some(MorselPlan::new().with_pending_planner(async move {
            inner.plan_file(files.with_access_plan(file).await?)
        })))
    }
}
