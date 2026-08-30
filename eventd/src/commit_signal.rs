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
        self.generation.fetch_add(1, Ordering::Release);
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
    use super::*;

    #[test]
    fn commit_advances_generation() {
        let signal = CommitSignal::new();
        let before = signal.generation();
        signal.committed();
        assert_ne!(signal.generation(), before);
    }
}
