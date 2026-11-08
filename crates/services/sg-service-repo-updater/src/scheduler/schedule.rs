#![forbid(unsafe_code)]

//! `cmd/repo-updater/internal/scheduler/schedule.go`: the min-heap of repos
//! ordered by their next due time.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, SystemTime};

use tokio::sync::Notify;

use super::{Clock, ConfiguredRepo, MAX_DELAY, MIN_DELAY};
use crate::metrics::Metrics;

/// `scheduledRepoUpdate`.
#[derive(Debug, Clone)]
pub struct ScheduledRepoUpdate {
    pub repo: ConfiguredRepo,
    pub interval: Duration,
    pub due: SystemTime,
    /// Index in the heap; `None` after removal (`-1` in the baseline).
    pub(crate) index: Option<usize>,
}

/// State shared behind one mutex, like the Go `schedule` struct.
struct Inner {
    heap: Vec<ScheduledRepoUpdate>,
    index: HashMap<i32, usize>,
    /// xorshift state standing in for `rand.Int63n`.
    rand_state: u64,
}

pub struct Schedule {
    inner: Mutex<Inner>,
    /// Replaces the Go timer's `wakeup` channel. Every structural change
    /// notifies the scheduling loop so it recomputes its sleep deadline.
    pub(crate) wakeup: Notify,
    clock: std::sync::Arc<dyn Clock>,
    metrics: std::sync::Arc<Metrics>,
}

impl Schedule {
    pub fn new(clock: std::sync::Arc<dyn Clock>, metrics: std::sync::Arc<Metrics>) -> Self {
        Self {
            inner: Mutex::new(Inner {
                heap: Vec::new(),
                index: HashMap::new(),
                rand_state: seed(),
            }),
            wakeup: Notify::new(),
            clock,
            metrics,
        }
    }

    /// Test hook: fix the jitter generator, mirroring a seeded
    /// `rand.NewSource` in the baseline tests.
    pub fn with_seed(mut self, seed: u64) -> Self {
        self.inner.get_mut().expect("fresh mutex").rand_state = seed | 1;
        self
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Inner> {
        self.inner.lock().expect("schedule lock poisoned")
    }

    /// `schedule.upsert`: returns whether the repo was already scheduled.
    pub fn upsert(&self, repo: ConfiguredRepo) -> bool {
        assert!(repo.id != 0, "repo.id is zero");
        let mut inner = self.lock();
        if inner.index.contains_key(&repo.id) {
            let slot = inner.index[&repo.id];
            inner.heap[slot].repo = repo;
            return true;
        }
        let due = self.clock.now() + MIN_DELAY;
        let update = ScheduledRepoUpdate {
            repo,
            interval: MIN_DELAY,
            due,
            index: None,
        };
        heap_push(&mut inner, update);
        drop(inner);
        self.metrics.sched_known_repos.set(self.len() as f64);
        self.wakeup.notify_one();
        false
    }

    /// `schedule.prioritiseUncloned`.
    pub fn prioritise_uncloned(&self, repos: &[ConfiguredRepo]) {
        let due = self.clock.now() + MIN_DELAY;
        let mut inner = self.lock();
        let mut changed = false;
        for repo in repos {
            match inner.index.get(&repo.id).copied() {
                None => {
                    heap_push(
                        &mut inner,
                        ScheduledRepoUpdate {
                            repo: repo.clone(),
                            interval: MIN_DELAY,
                            due,
                            index: None,
                        },
                    );
                    changed = true;
                }
                Some(slot) => {
                    if inner.heap[slot].due > due {
                        inner.heap[slot].due = due;
                        heap_fix(&mut inner, slot);
                        changed = true;
                    }
                }
            }
        }
        drop(inner);
        if changed {
            self.metrics.sched_known_repos.set(self.len() as f64);
            self.wakeup.notify_one();
        }
    }

    /// `schedule.insertNew`.
    pub fn insert_new(&self, repos: &[ConfiguredRepo]) {
        let due = self.clock.now() + MIN_DELAY;
        let mut inner = self.lock();
        let mut changed = false;
        for repo in repos {
            if inner.index.contains_key(&repo.id) {
                continue;
            }
            heap_push(
                &mut inner,
                ScheduledRepoUpdate {
                    repo: repo.clone(),
                    interval: MIN_DELAY,
                    due,
                    index: None,
                },
            );
            changed = true;
        }
        drop(inner);
        if changed {
            self.metrics.sched_known_repos.set(self.len() as f64);
            self.wakeup.notify_one();
        }
    }

    /// `schedule.updateInterval`.
    pub fn update_interval(&self, repo: &ConfiguredRepo, interval: Duration) {
        assert!(repo.id != 0, "repo.id is zero");
        let mut inner = self.lock();
        let Some(slot) = inner.index.get(&repo.id).copied() else {
            return;
        };
        let clamped = interval.clamp(MIN_DELAY, MAX_DELAY);
        let delta = clamped.as_nanos() as i64 / 20;
        let jitter = if delta > 0 {
            int63n(&mut inner.rand_state, 2 * delta) - delta
        } else {
            0
        };
        let interval = if jitter >= 0 {
            clamped + Duration::from_nanos(jitter as u64)
        } else {
            clamped - Duration::from_nanos((-jitter) as u64)
        };
        inner.heap[slot].interval = interval;
        inner.heap[slot].due = self.clock.now() + interval;
        heap_fix(&mut inner, slot);
        drop(inner);
        self.wakeup.notify_one();
    }

    /// `schedule.getCurrentInterval`.
    pub fn current_interval(&self, repo: &ConfiguredRepo) -> Option<Duration> {
        let inner = self.lock();
        let slot = inner.index.get(&repo.id).copied()?;
        Some(inner.heap[slot].interval)
    }

    /// `schedule.remove`: returns whether the repo was scheduled.
    pub fn remove(&self, repo: &ConfiguredRepo) -> bool {
        assert!(repo.id != 0, "repo.id is zero");
        let mut inner = self.lock();
        let Some(slot) = inner.index.get(&repo.id).copied() else {
            return false;
        };
        heap_remove(&mut inner, slot);
        drop(inner);
        self.metrics.sched_known_repos.set(self.len() as f64);
        self.wakeup.notify_one();
        true
    }

    /// `schedule.reset`.
    pub fn reset(&self) {
        let mut inner = self.lock();
        inner.heap.clear();
        inner.index.clear();
        drop(inner);
        self.metrics.sched_known_repos.set(0.0);
    }

    pub fn len(&self) -> usize {
        self.lock().heap.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn ids(&self) -> Vec<i32> {
        self.lock().heap.iter().map(|u| u.repo.id).collect()
    }

    /// The next due time, if any.
    pub fn next_due(&self) -> Option<SystemTime> {
        self.lock().heap.first().map(|u| u.due)
    }

    /// `schedule.ScheduleInfo`'s schedule half.
    pub fn schedule_info(&self, id: i32) -> Option<ScheduleState> {
        let inner = self.lock();
        let slot = inner.index.get(&id).copied()?;
        let update = &inner.heap[slot];
        Some(ScheduleState {
            index: slot,
            total: inner.heap.len(),
            interval_seconds: update.interval.as_secs() as i64,
            due: update.due,
        })
    }

    /// `runSchedule`: enqueue every due repo and advance its deadline.
    /// Returns the number of repos enqueued.
    pub fn run_due(&self, queue: &super::updatequeue::UpdateQueue) -> usize {
        let now = self.clock.now();
        let mut inner = self.lock();
        let mut enqueued = 0;
        while let Some(top) = inner.heap.first() {
            if top.due > now + Duration::from_millis(1) {
                break;
            }
            let repo = top.repo.clone();
            let interval = top.interval;
            let slot = inner.index[&repo.id];
            inner.heap[slot].due = self.clock.now() + interval;
            heap_fix(&mut inner, slot);
            queue.enqueue(repo, super::Priority::Low);
            self.metrics.sched_auto_fetch.inc();
            enqueued += 1;
        }
        enqueued
    }

    /// A snapshot for the debug endpoint, in due order.
    pub fn snapshot(&self) -> Vec<ScheduledRepoUpdate> {
        let inner = self.lock();
        let mut heap = inner.heap.clone();
        for (i, item) in heap.iter_mut().enumerate() {
            item.index = Some(i);
        }
        heap.sort_by_key(|u| u.due);
        heap
    }
}

/// `protocol.RepoScheduleState`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScheduleState {
    pub index: usize,
    pub total: usize,
    pub interval_seconds: i64,
    pub due: SystemTime,
}

// ---------------------------------------------------------------------------
// Binary min-heap (Go's container/heap over scheduledRepoUpdate)
// ---------------------------------------------------------------------------

fn heap_less(a: &ScheduledRepoUpdate, b: &ScheduledRepoUpdate) -> bool {
    a.due < b.due
}

fn heap_push(inner: &mut Inner, mut item: ScheduledRepoUpdate) {
    let slot = inner.heap.len();
    item.index = Some(slot);
    inner.index.insert(item.repo.id, slot);
    inner.heap.push(item);
    sift_up(inner, slot);
}

fn sift_up(inner: &mut Inner, mut slot: usize) {
    while slot > 0 {
        let parent = (slot - 1) / 2;
        if !heap_less(&inner.heap[slot], &inner.heap[parent]) {
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
        if right < len && heap_less(&inner.heap[right], &inner.heap[left]) {
            smallest = right;
        }
        if !heap_less(&inner.heap[smallest], &inner.heap[slot]) {
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
    // `container/heap.Fix`: restore the invariant starting at slot.
    if slot > 0 && heap_less(&inner.heap[slot], &inner.heap[(slot - 1) / 2]) {
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

/// `rand.Int63n`: xorshift64*, non-negative result in `[0, n)`.
fn int63n(state: &mut u64, n: i64) -> i64 {
    if n <= 0 {
        return 0;
    }
    let mut x = *state;
    x ^= x >> 12;
    x ^= x << 25;
    x ^= x >> 27;
    *state = x;
    let value = x.wrapping_mul(0x2545_F491_4F6C_DD1D) >> 1;
    (value % n as u64) as i64
}

fn seed() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0x9E37_79B9_7F4A_7C15)
        | 1
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use super::*;
    use crate::scheduler::updatequeue::UpdateQueue;
    use crate::scheduler::Priority;

    struct FakeClock(Mutex<SystemTime>);

    impl Clock for FakeClock {
        fn now(&self) -> SystemTime {
            *self.0.lock().expect("clock")
        }
    }

    fn fixture() -> (Arc<FakeClock>, Schedule, Arc<Metrics>, SystemTime) {
        let start = std::time::UNIX_EPOCH + Duration::from_secs(1_700_000_000);
        let clock = Arc::new(FakeClock(Mutex::new(start)));
        let metrics = Arc::new(Metrics::new());
        let schedule = Schedule::new(clock.clone(), Arc::clone(&metrics)).with_seed(42);
        (clock, schedule, metrics, start)
    }

    fn repo(id: i32) -> ConfiguredRepo {
        ConfiguredRepo {
            id,
            name: format!("github.com/example/repo-{id}"),
        }
    }

    #[test]
    fn upsert_reports_whether_the_repo_was_known() {
        let (_clock, schedule, metrics, start) = fixture();
        assert!(!schedule.upsert(repo(1)));
        assert!(schedule.upsert(repo(1)));
        assert_eq!(schedule.len(), 1);
        let info = schedule.schedule_info(1).expect("scheduled");
        assert_eq!(info.interval_seconds, 45);
        assert_eq!(info.due, start + MIN_DELAY);
        assert_eq!(metrics.sched_known_repos.get(), 1.0);
        assert!(schedule.remove(&repo(1)));
        assert_eq!(metrics.sched_known_repos.get(), 0.0);
    }

    #[test]
    fn update_interval_clamps_and_applies_five_percent_jitter() {
        let (_clock, schedule, _metrics, start) = fixture();
        schedule.upsert(repo(1));
        schedule.update_interval(&repo(1), Duration::from_secs(1));
        let interval = schedule.current_interval(&repo(1)).expect("scheduled");
        assert!(interval >= Duration::from_millis(42_750));
        assert!(interval <= Duration::from_millis(47_250));
        let info = schedule.schedule_info(1).expect("scheduled");
        assert_eq!(info.due, start + interval);

        schedule.update_interval(&repo(1), Duration::from_secs(24 * 60 * 60));
        let interval = schedule.current_interval(&repo(1)).expect("scheduled");
        assert!(interval >= MAX_DELAY - MAX_DELAY / 20);
        assert!(interval <= MAX_DELAY + MAX_DELAY / 20);
    }

    #[test]
    fn run_due_enqueues_due_repos_at_low_priority_and_advances_due() {
        let (clock, schedule, metrics, start) = fixture();
        schedule.upsert(repo(1));
        let queue = UpdateQueue::new(Arc::clone(&metrics));

        assert_eq!(schedule.run_due(&queue), 0);
        *clock.0.lock().expect("clock") = start + MIN_DELAY;
        assert_eq!(schedule.run_due(&queue), 1);
        assert_eq!(queue.len(), 1);
        assert_eq!(metrics.sched_auto_fetch.get(), 1.0);
        // Low-priority entry waiting for the update loop.
        let state = queue.queue_info(1).expect("queued");
        assert_eq!(state.priority, Priority::Low as i32);
        assert!(!state.updating);
        // The next deadline moved one interval into the future.
        let info = schedule.schedule_info(1).expect("scheduled");
        assert_eq!(info.due, start + 2 * MIN_DELAY);
    }

    #[test]
    fn prioritise_uncloned_inserts_missing_repos_and_pulls_known_ones_forward() {
        let (_clock, schedule, _metrics, start) = fixture();
        schedule.upsert(repo(1));
        schedule.update_interval(&repo(1), MAX_DELAY);
        assert!(schedule.schedule_info(1).expect("scheduled").due > start + MAX_DELAY / 2);

        schedule.prioritise_uncloned(&[repo(1), repo(2)]);
        assert_eq!(schedule.len(), 2);
        assert_eq!(
            schedule.schedule_info(1).expect("scheduled").due,
            start + MIN_DELAY
        );
        assert_eq!(
            schedule.schedule_info(2).expect("scheduled").due,
            start + MIN_DELAY
        );
    }

    #[test]
    fn insert_new_skips_known_repos() {
        let (_clock, schedule, _metrics, start) = fixture();
        schedule.upsert(repo(1));
        schedule.update_interval(&repo(1), MAX_DELAY);
        schedule.insert_new(&[repo(1), repo(2)]);
        assert_eq!(schedule.len(), 2);
        // The known repo keeps its long interval; only the new one gets the default.
        assert!(schedule.schedule_info(1).expect("scheduled").due > start + MAX_DELAY / 2);
        assert_eq!(
            schedule.schedule_info(2).expect("scheduled").due,
            start + MIN_DELAY
        );
    }

    #[test]
    fn reset_clears_everything() {
        let (_clock, schedule, metrics, _start) = fixture();
        schedule.upsert(repo(1));
        schedule.upsert(repo(2));
        schedule.reset();
        assert!(schedule.is_empty());
        assert_eq!(metrics.sched_known_repos.get(), 0.0);
        assert!(schedule.schedule_info(1).is_none());
    }
}
