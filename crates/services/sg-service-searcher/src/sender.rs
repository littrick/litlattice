#![forbid(unsafe_code)]

//! Limit-aware match streaming (`sender.go`).
//!
//! Workers call [`LimitedStream::send`] concurrently; the callback is invoked
//! under the atomics' ordering, so callbacks for different matches may be
//! concurrent. The gRPC plumbing serializes them through a channel.

use std::sync::atomic::{AtomicBool, AtomicI32, Ordering};

use tokio_util::sync::CancellationToken;

use crate::protocol::FileMatch;

type Callback = Box<dyn Fn(FileMatch) + Send + Sync>;

/// Baseline `limitedStream`.
pub struct LimitedStream {
    callback: Callback,
    limit: i32,
    remaining: AtomicI32,
    limit_hit: AtomicBool,
    cancel: CancellationToken,
}

impl LimitedStream {
    /// Baseline `newLimitedStream`: cancels `cancel` (and returns a derived
    /// token) when the limit is reached. The caller cancels the parent token
    /// when the client goes away.
    pub fn new(limit: i32, callback: Callback, cancel: CancellationToken) -> Self {
        Self {
            callback,
            limit,
            remaining: AtomicI32::new(limit),
            limit_hit: AtomicBool::new(false),
            cancel,
        }
    }

    /// Baseline `limitedStream.Send`: pass through while below the limit,
    /// truncate when crossing it, and drop everything after it.
    pub fn send(&self, mut file_match: FileMatch) {
        let count = file_match.match_count();
        let after = self.remaining.fetch_sub(count, Ordering::SeqCst) - count;
        let before = after + count;

        if after > 0 {
            // Remaining was large enough to send the full match.
            (self.callback)(file_match);
        } else if before <= 0 {
            // The limit was already hit; ignore this event.
        } else if after == 0 {
            // We hit the limit exactly.
            self.cancel.cancel();
            self.limit_hit.store(true, Ordering::SeqCst);
            (self.callback)(file_match);
        } else {
            // We crossed the limit threshold; truncate before sending.
            self.cancel.cancel();
            self.limit_hit.store(true, Ordering::SeqCst);
            file_match.limit(before);
            (self.callback)(file_match);
        }
    }

    /// Baseline `limitedStream.SentCount`.
    pub fn sent_count(&self) -> i32 {
        self.limit - self.remaining()
    }

    /// Baseline `limitedStream.Remaining`.
    pub fn remaining(&self) -> i32 {
        self.remaining.load(Ordering::SeqCst).max(0)
    }

    /// Baseline `limitedStream.LimitHit`.
    pub fn limit_hit(&self) -> bool {
        self.limit_hit.load(Ordering::SeqCst)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use super::*;
    use crate::protocol::{ChunkMatch, Location, Range};

    fn file_match(ranges: usize) -> FileMatch {
        FileMatch {
            path: "file".to_owned(),
            chunk_matches: vec![ChunkMatch {
                content: String::new(),
                content_start: Location::default(),
                ranges: vec![Range::default(); ranges],
            }],
            ..FileMatch::default()
        }
    }

    fn collector() -> (Arc<Mutex<Vec<FileMatch>>>, Callback) {
        let collected = Arc::new(Mutex::new(Vec::new()));
        let sink = Arc::clone(&collected);
        let callback: Callback = Box::new(move |file_match| {
            sink.lock().expect("collector").push(file_match);
        });
        (collected, callback)
    }

    #[test]
    fn sends_until_the_limit() {
        let cancel = CancellationToken::new();
        let (collected, callback) = collector();
        let stream = LimitedStream::new(10, callback, cancel.clone());

        stream.send(file_match(4));
        assert_eq!(stream.sent_count(), 4);
        assert_eq!(stream.remaining(), 6);
        assert!(!stream.limit_hit());
        assert!(!cancel.is_cancelled());

        // Crossing the threshold truncates and cancels.
        stream.send(file_match(10));
        assert!(stream.limit_hit());
        assert!(cancel.is_cancelled());
        assert_eq!(stream.sent_count(), 10);
        assert_eq!(stream.remaining(), 0);

        {
            let collected = collected.lock().expect("collector");
            assert_eq!(collected.len(), 2);
            assert_eq!(collected[1].match_count(), 6);
        }

        // Everything after the limit is dropped.
        stream.send(file_match(1));
        assert_eq!(collected.lock().expect("collector").len(), 2);
        assert_eq!(stream.sent_count(), 10);
    }

    #[test]
    fn hitting_the_limit_exactly_still_sends() {
        let (collected, callback) = collector();
        let stream = LimitedStream::new(4, callback, CancellationToken::new());
        stream.send(file_match(4));
        assert!(stream.limit_hit());
        assert_eq!(stream.sent_count(), 4);
        assert_eq!(collected.lock().expect("collector").len(), 1);
    }

    #[test]
    fn ignores_matches_after_the_limit() {
        let (collected, callback) = collector();
        let stream = LimitedStream::new(2, callback, CancellationToken::new());
        stream.send(file_match(3));
        // The truncated match uses "before" (2) ranges.
        assert_eq!(collected.lock().expect("collector")[0].match_count(), 2);
        stream.send(file_match(1));
        assert_eq!(collected.lock().expect("collector").len(), 1);
        assert_eq!(stream.sent_count(), 2);
    }

    #[test]
    fn path_matches_count_as_one() {
        let (collected, callback) = collector();
        let stream = LimitedStream::new(5, callback, CancellationToken::new());
        stream.send(FileMatch {
            path: "file".to_owned(),
            ..FileMatch::default()
        });
        assert_eq!(stream.sent_count(), 1);
        assert_eq!(collected.lock().expect("collector").len(), 1);
    }
}
