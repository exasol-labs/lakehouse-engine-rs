use futures::stream::{BoxStream, StreamExt, TryStreamExt};
use object_store::path::Path as StorePath;
use object_store::{ObjectMeta, ObjectStore, PutPayload};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

/// Passes every call through to `inner`, logging each read (`head`/`get <location>`) and each
/// listing (`recursive`/`delimited <prefix>`), and counting the delimiter listings in flight.
#[derive(Debug)]
pub(crate) struct RecordingStore {
    inner: Arc<dyn ObjectStore>,
    /// Hands each listing back reversed, so a test cannot pass by `InMemory`'s sorted order.
    reversed: bool,
    reads: Mutex<Vec<String>>,
    listings: Mutex<Vec<String>>,
    in_flight: AtomicUsize,
    peak: AtomicUsize,
}

impl RecordingStore {
    pub(crate) fn wrapping(inner: Arc<dyn ObjectStore>) -> Arc<Self> {
        Self::listing_in_order(inner, false)
    }

    pub(crate) fn reversing(inner: Arc<dyn ObjectStore>) -> Arc<Self> {
        Self::listing_in_order(inner, true)
    }

    fn listing_in_order(inner: Arc<dyn ObjectStore>, reversed: bool) -> Arc<Self> {
        Arc::new(Self {
            inner,
            reversed,
            reads: Mutex::default(),
            listings: Mutex::default(),
            in_flight: AtomicUsize::new(0),
            peak: AtomicUsize::new(0),
        })
    }

    pub(crate) fn reads(&self) -> Vec<String> {
        self.reads.lock().expect("read log").clone()
    }

    pub(crate) fn listings(&self) -> Vec<String> {
        self.listings.lock().expect("listing log").clone()
    }

    /// Each listing's prefix, in call order, without its kind.
    pub(crate) fn listed_prefixes(&self) -> Vec<String> {
        self.listings()
            .into_iter()
            .map(|listing| listing.split_once(' ').expect("kind prefix").1.to_string())
            .collect()
    }

    /// The distinct locations a `get` reached, sorted: a footer read costs a variable number of
    /// ranged requests, so only the set of files opened is stable.
    pub(crate) fn files_read(&self) -> Vec<String> {
        let mut paths: Vec<String> = self
            .reads()
            .into_iter()
            .filter_map(|read| read.strip_prefix("get ").map(str::to_string))
            .collect();
        paths.sort();
        paths.dedup();
        paths
    }

    pub(crate) fn peak_delimited_listings(&self) -> usize {
        self.peak.load(Ordering::SeqCst)
    }

    fn record_listing(&self, kind: &str, prefix: Option<&StorePath>) {
        self.listings
            .lock()
            .expect("listing log")
            .push(format!("{kind} {}", prefix.map_or("", StorePath::as_ref)));
    }
}

impl std::fmt::Display for RecordingStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "RecordingStore({})", self.inner)
    }
}

#[async_trait::async_trait]
impl ObjectStore for RecordingStore {
    async fn put_opts(
        &self,
        location: &StorePath,
        payload: PutPayload,
        opts: object_store::PutOptions,
    ) -> object_store::Result<object_store::PutResult> {
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &StorePath,
        opts: object_store::PutMultipartOptions,
    ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(
        &self,
        location: &StorePath,
        options: object_store::GetOptions,
    ) -> object_store::Result<object_store::GetResult> {
        let verb = if options.head { "head" } else { "get" };
        self.reads
            .lock()
            .expect("read log")
            .push(format!("{verb} {location}"));
        self.inner.get_opts(location, options).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<StorePath>>,
    ) -> BoxStream<'static, object_store::Result<StorePath>> {
        self.inner.delete_stream(locations)
    }

    fn list(
        &self,
        prefix: Option<&StorePath>,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.record_listing("recursive", prefix);
        if !self.reversed {
            return self.inner.list(prefix);
        }
        let inner = Arc::clone(&self.inner);
        let prefix = prefix.cloned();
        futures::stream::once(async move {
            match inner.list(prefix.as_ref()).try_collect::<Vec<_>>().await {
                Ok(metas) => metas.into_iter().rev().map(Ok).collect::<Vec<_>>(),
                Err(error) => vec![Err(error)],
            }
        })
        .flat_map(futures::stream::iter)
        .boxed()
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&StorePath>,
    ) -> object_store::Result<object_store::ListResult> {
        self.record_listing("delimited", prefix);
        let now = self.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
        self.peak.fetch_max(now, Ordering::SeqCst);
        tokio::task::yield_now().await;
        let listing = self.inner.list_with_delimiter(prefix).await;
        self.in_flight.fetch_sub(1, Ordering::SeqCst);
        let mut listing = listing?;
        if self.reversed {
            listing.objects.reverse();
        }
        Ok(listing)
    }

    async fn copy_opts(
        &self,
        from: &StorePath,
        to: &StorePath,
        options: object_store::CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}
