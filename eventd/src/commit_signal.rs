//! Process-local commit generations used by live query handlers.

use core::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Condvar, Mutex};
use std::time::Duration;

/// A monotonic generation plus a wake-up edge for committed store changes.
pub struct CommitSignal {
    generation: AtomicU64,
    wait_lock: Mutex<()>,
    changed: Condvar,
}

impl CommitSignal {
    pub const fn new() -> Self {
        Self {
            generation: AtomicU64::new(0),
            wait_lock: Mutex::new(()),
            changed: Condvar::new(),
        }
    }

    pub fn generation(&self) -> u64 {
        self.generation.load(Ordering::Acquire)
    }

    pub fn committed(&self) {
        // Pair the predicate update with the waiter's mutex: otherwise a notification can
        // land after its last predicate check but before it actually sleeps.
        let guard = self
            .wait_lock
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        self.generation.fetch_add(1, Ordering::Release);
        drop(guard);
        self.changed.notify_all();
    }

    pub fn wait_for_change(&self, observed: u64, timeout: Duration) -> u64 {
        let current = self.generation();
        if current != observed {
            return current;
        }
        {
            let guard = self
                .wait_lock
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            drop(
                self.changed
                    .wait_timeout_while(guard, timeout, |()| self.generation() == observed)
                    .unwrap_or_else(std::sync::PoisonError::into_inner),
            );
        }
        self.generation()
    }
}

impl Default for CommitSignal {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::mpsc::{self, RecvTimeoutError};
    use std::sync::{Arc, Condvar, Mutex};
    use std::time::Duration;

    use super::{AtomicU64, CommitSignal};

    #[test]
    fn commit_advances_generation() {
        let signal = CommitSignal::new();
        let before = signal.generation();
        signal.committed();
        assert_ne!(signal.generation(), before);
    }

    #[test]
    fn commit_cannot_notify_between_the_waiters_check_and_sleep() {
        let signal = Arc::new(CommitSignal::new());
        // Hold the same critical section as the waiter's predicate check. A committer
        // must not publish and notify until that check can transition into waiting.
        let guard = signal.wait_lock.lock().unwrap();
        let (started, entering) = mpsc::channel();
        let (finished, completed) = mpsc::channel();
        let committer = {
            let signal = Arc::clone(&signal);
            std::thread::spawn(move || {
                started.send(()).unwrap();
                signal.committed();
                finished.send(()).unwrap();
            })
        };
        entering.recv().unwrap();
        let while_locked = completed.recv_timeout(Duration::from_millis(50));
        drop(guard);
        committer.join().unwrap();
        assert_eq!(while_locked, Err(RecvTimeoutError::Timeout));
        assert_eq!(signal.generation(), 1);
    }

    #[test]
    fn a_commit_that_wraps_the_generation_wakes_a_waiting_handler() {
        let signal = std::sync::Arc::new(CommitSignal {
            generation: AtomicU64::new(u64::MAX),
            wait_lock: Mutex::new(()),
            changed: Condvar::new(),
        });
        let waiter = {
            let signal = std::sync::Arc::clone(&signal);
            std::thread::spawn(move || {
                let started = std::time::Instant::now();
                let seen = signal.wait_for_change(u64::MAX, Duration::from_secs(30));
                (seen, started.elapsed())
            })
        };
        // Let the handler reach its wait before the wrapping commit.
        std::thread::sleep(Duration::from_millis(50));
        signal.committed();
        let (seen, blocked_for) = waiter.join().unwrap();
        assert_eq!(seen, 0, "the increment past u64::MAX wraps to zero");
        assert!(
            blocked_for < Duration::from_secs(10),
            "and wakes the handler rather than leaving it to its timeout"
        );
        // A handler that last observed u64::MAX sees the wrapped value as a
        // change at once.
        let started = std::time::Instant::now();
        assert_eq!(signal.wait_for_change(u64::MAX, Duration::from_secs(30)), 0);
        assert!(started.elapsed() < Duration::from_secs(10));
    }
}
