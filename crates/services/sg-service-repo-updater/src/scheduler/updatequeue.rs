#![forbid(unsafe_code)]

//! `cmd/repo-updater/internal/scheduler/updatequeue.go`: the priority queue of
//! repositories waiting for a clone/fetch.

use std::cmp::Ordering;
use std::collections::HashMap;
use std::sync::Mutex;

use tokio::sync::Notify;

use super::{ConfiguredRepo, Priority};
use crate::metrics::Metrics;

/// `repoUpdate`.
#[derive(Debug, Clone)]
pub struct RepoUpdate {
    pub repo: ConfiguredRepo,
    pub priority: Priority,
    pub seq: u64,
    pub updating: bool,
    pub(crate) index: Option<usize>,
}

struct Inner {
    heap: Vec<RepoUpdate>,
    index: HashMap<i32, usize>,
    seq: u64,
}

pub struct UpdateQueue {
    inner: Mutex<Inner>,
    /// `notifyEnqueue`.
    pub(crate) notify_enqueue: Notify,
    metrics: std::sync::Arc<Metrics>,
}

impl UpdateQueue {
    pub fn new(metrics: std::sync::Arc<Metrics>) -> Self {
        Self {
            inner: Mutex::new(Inner {
                heap: Vec::new(),
                index: HashMap::new(),
                seq: 0,
            }),
            notify_enqueue: Notify::new(),
            metrics,
        }
    }

    fn refresh_length(&self) {
        let length = self.lock().heap.len();
        self.metrics.sched_update_queue_length.set(length as f64);
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Inner> {
        self.inner.lock().expect("update queue lock poisoned")
    }

    /// `updateQueue.enqueue`: returns whether the repo was already queued.
    pub fn enqueue(&self, repo: ConfiguredRepo, priority: Priority) -> bool {
        assert!(repo.id != 0, "repo.id is zero");
        let mut inner = self.lock();
        let Some(slot) = inner.index.get(&repo.id).copied() else {
            let seq = next_seq(&mut inner);
            heap_push(
                &mut inner,
                RepoUpdate {
                    repo,
                    priority,
                    seq,
                    updating: false,
                    index: None,
                },
            );
            drop(inner);
            self.refresh_length();
            self.notify_enqueue.notify_one();
            return false;
        };

        if inner.heap[slot].updating {
            return false;
        }
        inner.heap[slot].repo = repo;
        if priority <= inner.heap[slot].priority {
            return true;
        }
        inner.heap[slot].priority = priority;
        inner.heap[slot].seq = next_seq(&mut inner);
        heap_fix(&mut inner, slot);
        drop(inner);
        self.refresh_length();
        self.notify_enqueue.notify_one();
        true
    }

    /// `updateQueue.remove`.
    pub fn remove(&self, repo: &ConfiguredRepo, updating: bool) -> bool {
        assert!(repo.id != 0, "repo.id is zero");
        let mut inner = self.lock();
        let Some(slot) = inner.index.get(&repo.id).copied() else {
            return false;
        };
        if inner.heap[slot].updating != updating {
            return false;
        }
        heap_remove(&mut inner, slot);
        drop(inner);
        self.refresh_length();
        true
    }

    /// `updateQueue.acquireNext`.
    pub fn acquire_next(&self) -> Option<ConfiguredRepo> {
        let mut inner = self.lock();
        let top = inner.heap.first()?;
        if top.updating {
            return None;
        }
        let repo = top.repo.clone();
        let slot = inner.index[&repo.id];
        inner.heap[slot].updating = true;
        heap_fix(&mut inner, slot);
        Some(repo)
    }

    pub fn reset(&self) {
        let mut inner = self.lock();
        inner.heap.clear();
        inner.index.clear();
        inner.seq = 0;
        drop(inner);
        self.metrics.sched_update_queue_length.set(0.0);
    }

    pub fn len(&self) -> usize {
        let length = self.lock().heap.len();
        self.metrics.sched_update_queue_length.set(length as f64);
        length
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// `updateQueue.ScheduleInfo`'s queue half.
    pub fn queue_info(&self, id: i32) -> Option<QueueState> {
        let inner = self.lock();
        let slot = inner.index.get(&id).copied()?;
        let update = &inner.heap[slot];
        Some(QueueState {
            index: slot,
            total: inner.heap.len(),
            updating: update.updating,
            priority: update.priority as i32,
        })
    }

    /// Debug snapshot, highest priority first (pop order).
    pub fn snapshot(&self) -> Vec<RepoUpdate> {
        let inner = self.lock();
        let mut heap = inner.heap.clone();
        for (i, item) in heap.iter_mut().enumerate() {
            item.index = Some(i);
        }
        heap.sort_by(|a, b| {
            if queue_less(a, b) {
                Ordering::Less
            } else if queue_less(b, a) {
                Ordering::Greater
            } else {
                Ordering::Equal
            }
        });
        heap
    }
}

/// `protocol.RepoQueueState`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueueState {
    pub index: usize,
    pub total: usize,
    pub updating: bool,
    pub priority: i32,
}

fn next_seq(inner: &mut Inner) -> u64 {
    inner.seq += 1;
    inner.seq
}

// ---------------------------------------------------------------------------
// Priority heap. `Less` mirrors the baseline:
//   1. not-updating before updating
//   2. higher priority first
//   3. lower sequence number first
// ---------------------------------------------------------------------------

fn queue_less(a: &RepoUpdate, b: &RepoUpdate) -> bool {
    if a.updating != b.updating {
        return b.updating;
    }
    if a.priority != b.priority {
        return a.priority > b.priority;
    }
    a.seq < b.seq
}

fn heap_push(inner: &mut Inner, mut item: RepoUpdate) {
    let slot = inner.heap.len();
    item.index = Some(slot);
    inner.index.insert(item.repo.id, slot);
    inner.heap.push(item);
    sift_up(inner, slot);
}

fn sift_up(inner: &mut Inner, mut slot: usize) {
    while slot > 0 {
        let parent = (slot - 1) / 2;
        if !queue_less(&inner.heap[slot], &inner.heap[parent]) {
            break;
        }
        swap(inner, slot, parent);
        slot = parent;
    }
}

fn sift_down(inner: &mut Inner, mut slot: usize) {
    let len = inner.heap.len();
    loop {
        let left = 2 * slot + 1;
        if left >= len {
            break;
        }
        let right = left + 1;
        let mut smallest = left;
        if right < len && queue_less(&inner.heap[right], &inner.heap[left]) {
            smallest = right;
        }
        if !queue_less(&inner.heap[smallest], &inner.heap[slot]) {
            break;
        }
        swap(inner, slot, smallest);
        slot = smallest;
    }
}

fn swap(inner: &mut Inner, a: usize, b: usize) {
    inner.heap.swap(a, b);
    inner.heap[a].index = Some(a);
    inner.heap[b].index = Some(b);
    inner.index.insert(inner.heap[a].repo.id, a);
    inner.index.insert(inner.heap[b].repo.id, b);
}

fn heap_fix(inner: &mut Inner, slot: usize) {
    if slot > 0 && queue_less(&inner.heap[slot], &inner.heap[(slot - 1) / 2]) {
        sift_up(inner, slot);
    } else {
        sift_down(inner, slot);
    }
}

fn heap_remove(inner: &mut Inner, slot: usize) {
    let last = inner.heap.len() - 1;
    if slot != last {
        swap(inner, slot, last);
    }
    let removed = inner.heap.pop().expect("non-empty heap");
    inner.index.remove(&removed.repo.id);
    if slot < inner.heap.len() {
        heap_fix(inner, slot);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;

    fn queue() -> UpdateQueue {
        UpdateQueue::new(Arc::new(Metrics::new()))
    }

    fn repo(id: i32) -> ConfiguredRepo {
        ConfiguredRepo {
            id,
            name: format!("repo-{id}"),
        }
    }

    #[test]
    fn pops_in_fifo_order_within_a_priority() {
        let queue = queue();
        assert!(!queue.enqueue(repo(1), Priority::Low));
        assert!(!queue.enqueue(repo(2), Priority::Low));
        assert_eq!(queue.acquire_next(), Some(repo(1)));
        assert_eq!(queue.acquire_next(), Some(repo(2)));
        assert_eq!(queue.acquire_next(), None);
    }

    #[test]
    fn higher_priority_goes_first() {
        let queue = queue();
        queue.enqueue(repo(1), Priority::Low);
        queue.enqueue(repo(2), Priority::High);
        assert_eq!(queue.acquire_next(), Some(repo(2)));
        assert_eq!(queue.acquire_next(), Some(repo(1)));
    }

    #[test]
    fn promoting_a_repo_keeps_it_behind_other_high_priority_entries() {
        let queue = queue();
        queue.enqueue(repo(1), Priority::Low);
        queue.enqueue(repo(2), Priority::High);
        assert!(queue.enqueue(repo(1), Priority::High));
        assert_eq!(queue.acquire_next(), Some(repo(2)));
        assert_eq!(queue.acquire_next(), Some(repo(1)));
    }

    #[test]
    fn enqueue_while_updating_is_ignored() {
        let queue = queue();
        queue.enqueue(repo(1), Priority::Low);
        assert_eq!(queue.acquire_next(), Some(repo(1)));
        // The entry stays queued, flagged as updating.
        assert!(!queue.enqueue(repo(1), Priority::High));
        assert_eq!(queue.acquire_next(), None);
        let state = queue.queue_info(1).expect("queued");
        assert!(state.updating);

        // `remove(repo, true)` is how the update goroutine clears it.
        assert!(!queue.remove(&repo(1), false));
        assert!(queue.remove(&repo(1), true));
        assert!(queue.is_empty());
    }

    #[test]
    fn queue_info_reports_position_and_total() {
        let queue = queue();
        queue.enqueue(repo(1), Priority::Low);
        queue.enqueue(repo(2), Priority::Low);
        let state = queue.queue_info(1).expect("queued");
        assert_eq!(state.index, 0);
        assert_eq!(state.total, 2);
        assert_eq!(state.priority, 0);
        assert!(!state.updating);
    }

    #[test]
    fn reset_clears_sequence_and_length() {
        let queue = queue();
        queue.enqueue(repo(1), Priority::Low);
        queue.reset();
        assert!(queue.is_empty());
        queue.enqueue(repo(2), Priority::Low);
        let state = queue.queue_info(2).expect("queued");
        assert_eq!(state.index, 0);
    }
}
