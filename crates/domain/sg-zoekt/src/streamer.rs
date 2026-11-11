#![forbid(unsafe_code)]

//! The `zoekt.Streamer` abstraction the searcher wrappers build on.
//!
//! Mirrors `github.com/sourcegraph/zoekt.Streamer` (`api.go`): a searcher that
//! can both `StreamSearch` and `List`. The futures are required to be `Send`
//! so [`crate::cached::CachedStreamer`] can refresh entries from a spawned
//! task.

use std::future::Future;

use crate::query::Query;
use crate::types::{ListOptions, ListResponse, SearchChunk, SearchOptions, SearchResult};

/// A zoekt searcher transport (`zoekt.Streamer`).
pub trait Streamer: Send + Sync + 'static {
    /// The transport error type. It must be cloneable because the cached
    /// wrapper hands the same cached error to every caller.
    type Error: std::error::Error + Clone + Send + Sync + 'static;

    /// `zoekt.Streamer.List`.
    fn list(
        &self,
        query: &Query,
        opts: Option<ListOptions>,
    ) -> impl Future<Output = Result<ListResponse, Self::Error>> + Send;

    /// `zoekt.Streamer.StreamSearch`.
    fn stream_search(
        &self,
        query: &Query,
        options: &SearchOptions,
        sender: &mut (dyn FnMut(SearchChunk) + Send),
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;
}

/// Blocking bridge from the async [`Streamer`] to the synchronous result-level
/// streamer used by `search_stream::zoekt_search`.
///
/// This mirrors the baseline's synchronous `zoekt.Streamer`
/// (`internal/search/backend/grpc.go:27-52`): [`Self::stream_search_blocking`]
/// runs the wrapped async transport to completion and hands the caller one
/// [`SearchResult`] per wire [`SearchChunk`] (via
/// [`SearchChunk::to_search_result`], the M9.8ib lossless adapter).
///
/// Runtime strategy (see the M9.8ig overview, §2.3):
///
/// * inside a multi-thread Tokio runtime it uses
///   [`tokio::task::block_in_place`] plus `Handle::block_on`;
/// * with no runtime, or inside a current-thread runtime (where
///   `block_in_place` and a nested `block_on` would panic), it runs the future
///   on a dedicated thread with its own current-thread runtime.
///
/// Results are buffered until the future completes because the synchronous
/// sender has no `Send` bound and therefore cannot be called from the runtime
/// thread while the caller is blocked. The sequence is unchanged.
#[derive(Debug, Clone, Default)]
pub struct BlockingStreamer<S> {
    inner: S,
}

impl<S> BlockingStreamer<S> {
    /// Wrap an async [`Streamer`].
    pub fn new(inner: S) -> Self {
        Self { inner }
    }

    /// The wrapped async transport.
    pub fn inner(&self) -> &S {
        &self.inner
    }

    /// Unwrap the async transport.
    pub fn into_inner(self) -> S {
        self.inner
    }
}

impl<S: Streamer> BlockingStreamer<S> {
    /// `zoekt.Streamer.StreamSearch`, blocking.
    ///
    /// Each received chunk is converted with
    /// [`SearchChunk::to_search_result`] and delivered to `sender` in stream
    /// order. If the transport fails, the results received before the failure
    /// are delivered first, then the error is returned (the baseline sends
    /// every received chunk before returning an error).
    pub fn stream_search_blocking(
        &self,
        query: &Query,
        options: &SearchOptions,
        sender: &mut dyn FnMut(SearchResult),
    ) -> Result<(), S::Error> {
        let (tx, rx) = std::sync::mpsc::channel::<SearchResult>();
        let result = {
            // `Sender<SearchResult>` is `Send`, so the chunk callback satisfies
            // the async trait's `Send` requirement even though `sender` is not
            // `Send`.
            let mut on_chunk = move |chunk: SearchChunk| {
                // The receiver is drained right below; a failed send only
                // means this bridge was already abandoned.
                let _ = tx.send(chunk.to_search_result());
            };
            let future = self.inner.stream_search(query, options, &mut on_chunk);
            run_to_completion(future)
        };

        for result in rx.try_iter() {
            sender(result);
        }
        result
    }
}

/// Drive `future` to completion from a synchronous context.
///
/// `block_in_place` is only legal on the multi-thread runtime; a current-thread
/// runtime would otherwise panic both for `block_in_place` and for a nested
/// `block_on`, so those cases get a dedicated thread with its own runtime.
fn run_to_completion<F>(future: F) -> F::Output
where
    F: Future + Send,
    F::Output: Send,
{
    match tokio::runtime::Handle::try_current() {
        Ok(handle) if handle.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread => {
            tokio::task::block_in_place(|| handle.block_on(future))
        }
        _ => std::thread::scope(|scope| {
            scope
                .spawn(move || {
                    let runtime = tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()
                        .expect("building the zoekt blocking bridge runtime cannot fail");
                    runtime.block_on(future)
                })
                .join()
                .unwrap_or_else(|payload| std::panic::resume_unwind(payload))
        }),
    }
}

#[cfg(test)]
mod tests {
    use std::fmt;

    use super::*;

    #[derive(Debug, Clone, PartialEq, Eq)]
    struct TestError;

    impl fmt::Display for TestError {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("test error")
        }
    }

    impl std::error::Error for TestError {}

    #[derive(Clone, Debug)]
    struct FakeStreamer {
        chunks: Vec<SearchChunk>,
        fail_after: Option<usize>,
    }

    impl FakeStreamer {
        fn with_chunks(chunks: Vec<SearchChunk>) -> Self {
            Self {
                chunks,
                fail_after: None,
            }
        }
    }

    impl Streamer for FakeStreamer {
        type Error = TestError;

        async fn list(
            &self,
            _query: &Query,
            _opts: Option<ListOptions>,
        ) -> Result<ListResponse, TestError> {
            Ok(ListResponse::default())
        }

        async fn stream_search(
            &self,
            _query: &Query,
            _options: &SearchOptions,
            sender: &mut (dyn FnMut(SearchChunk) + Send),
        ) -> Result<(), TestError> {
            for (index, chunk) in self.chunks.iter().enumerate() {
                if Some(index) == self.fail_after {
                    return Err(TestError);
                }
                sender(chunk.clone());
                // Prove the future is actually polled on a Tokio runtime.
                tokio::task::yield_now().await;
            }
            Ok(())
        }
    }

    fn chunk(file_count: i64, match_count: i64) -> SearchChunk {
        SearchChunk {
            stats: crate::types::Stats {
                file_count,
                match_count,
                ..Default::default()
            },
            ..SearchChunk::default()
        }
    }

    #[test]
    fn blocking_streamer_works_without_a_runtime() {
        let streamer =
            BlockingStreamer::new(FakeStreamer::with_chunks(vec![chunk(1, 2), chunk(3, 4)]));
        let mut results = Vec::new();
        streamer
            .stream_search_blocking(
                &Query::Const(true),
                &SearchOptions::default(),
                &mut |result| results.push(result),
            )
            .expect("fake stream succeeds");
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].stats.file_count, 1);
        assert_eq!(results[1].stats.match_count, 4);
    }

    #[test]
    fn blocking_streamer_works_inside_a_multi_thread_runtime() {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .expect("build multi-thread runtime");
        let results = runtime.block_on(async {
            let streamer =
                BlockingStreamer::new(FakeStreamer::with_chunks(vec![chunk(5, 6), chunk(7, 8)]));
            let mut results = Vec::new();
            // Exercises the `block_in_place` branch.
            streamer
                .stream_search_blocking(
                    &Query::Const(true),
                    &SearchOptions::default(),
                    &mut |result| results.push(result),
                )
                .expect("fake stream succeeds");
            results
        });
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].stats.file_count, 5);
        assert_eq!(results[1].stats.file_count, 7);
    }

    #[test]
    fn blocking_streamer_works_inside_a_current_thread_runtime() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("build current-thread runtime");
        let results = runtime.block_on(async {
            let streamer =
                BlockingStreamer::new(FakeStreamer::with_chunks(vec![chunk(9, 10), chunk(11, 12)]));
            let mut results = Vec::new();
            // Exercises the dedicated-thread fallback; `block_in_place` would
            // panic here.
            streamer
                .stream_search_blocking(
                    &Query::Const(true),
                    &SearchOptions::default(),
                    &mut |result| results.push(result),
                )
                .expect("fake stream succeeds");
            results
        });
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].stats.match_count, 10);
        assert_eq!(results[1].stats.match_count, 12);
    }

    #[test]
    fn blocking_streamer_delivers_buffered_results_before_the_error() {
        let streamer = BlockingStreamer::new(FakeStreamer {
            chunks: vec![chunk(1, 1), chunk(2, 2)],
            fail_after: Some(1),
        });
        let mut results = Vec::new();
        let error = streamer
            .stream_search_blocking(
                &Query::Const(true),
                &SearchOptions::default(),
                &mut |result| results.push(result),
            )
            .expect_err("fake stream fails after one chunk");
        assert_eq!(error, TestError);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].stats.file_count, 1);
    }
}
