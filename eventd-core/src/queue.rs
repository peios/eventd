//! Fixed-allocation, slot-and-byte bounded MPSC handoff.
//!
//! The fast path is lock-free. A mutex and condition variable are touched only
//! after the queue applies backpressure or when an idle consumer sleeps.

use core::cell::UnsafeCell;
use core::fmt;
use core::mem::MaybeUninit;
use core::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};

use crossbeam_utils::CachePadded;

/// A fixed-allocation MPSC queue with independent slot and byte limits.
pub struct BoundedQueue<T> {
    inner: Arc<Inner<T>>,
}

impl<T> Clone for BoundedQueue<T> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<T> BoundedQueue<T> {
    /// Allocate a queue. `slots` must be a power of two and both bounds positive.
    pub fn new(slots: usize, bytes: usize) -> Result<Self, QueueConfigError> {
        if slots == 0 || !slots.is_power_of_two() || bytes == 0 {
            return Err(QueueConfigError);
        }
        let mut ring = Vec::with_capacity(slots);
        for sequence in 0..slots {
            ring.push(Slot::new(sequence));
        }
        Ok(Self {
            inner: Arc::new(Inner {
                ring: ring.into_boxed_slice(),
                mask: slots - 1,
                capacity: slots,
                enqueue: CachePadded::new(AtomicUsize::new(0)),
                dequeue: CachePadded::new(AtomicUsize::new(0)),
                bytes_used: CachePadded::new(AtomicUsize::new(0)),
                byte_capacity: bytes,
                closed: AtomicBool::new(false),
                epoch: AtomicU64::new(0),
                wait_lock: Mutex::new(()),
                wake: Condvar::new(),
            }),
        })
    }

    /// Reserve one slot and `bytes` before copying data from KMES.
    ///
    /// This blocks only while a configured bound is exhausted. Dropping the
    /// returned permit safely cancels it and makes the slot reusable.
    pub fn reserve(&self, bytes: usize) -> Result<Permit<'_, T>, ReserveError> {
        if bytes > self.inner.byte_capacity {
            return Err(ReserveError::TooLarge);
        }
        loop {
            let observed = self.inner.epoch.load(Ordering::Acquire);
            match self.inner.try_reserve(bytes) {
                Ok(permit) => return Ok(permit),
                Err(ReserveError::Full) => self.inner.park_until_change(observed),
                Err(error) => return Err(error),
            }
        }
    }

    /// Try to reserve without sleeping.
    pub fn try_reserve(&self, bytes: usize) -> Result<Permit<'_, T>, ReserveError> {
        if bytes > self.inner.byte_capacity {
            return Err(ReserveError::TooLarge);
        }
        self.inner.try_reserve(bytes)
    }

    /// Pop one item. Exactly one thread may call consumer methods.
    #[must_use]
    pub fn pop(&self) -> Pop<T> {
        self.inner.pop()
    }

    /// Wait until an item arrives or all producers close the queue.
    #[must_use]
    pub fn pop_wait(&self) -> Pop<T> {
        loop {
            let observed = self.inner.epoch.load(Ordering::Acquire);
            match self.pop() {
                Pop::Empty => self.inner.park_until_change(observed),
                result => return result,
            }
        }
    }

    /// Prevent new reservations and wake all sleepers.
    pub fn close(&self) {
        self.inner.closed.store(true, Ordering::Release);
        self.inner.notify_change();
    }

    /// Approximate number of reserved or published slots.
    #[must_use]
    pub fn len(&self) -> usize {
        self.inner
            .enqueue
            .load(Ordering::Relaxed)
            .wrapping_sub(self.inner.dequeue.load(Ordering::Relaxed))
    }

    /// Whether the queue currently has no reserved slots.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Bytes currently reserved or awaiting consumption.
    #[must_use]
    pub fn bytes_used(&self) -> usize {
        self.inner.bytes_used.load(Ordering::Relaxed)
    }
}

struct Inner<T> {
    ring: Box<[Slot<T>]>,
    mask: usize,
    capacity: usize,
    enqueue: CachePadded<AtomicUsize>,
    dequeue: CachePadded<AtomicUsize>,
    bytes_used: CachePadded<AtomicUsize>,
    byte_capacity: usize,
    closed: AtomicBool,
    epoch: AtomicU64,
    wait_lock: Mutex<()>,
    wake: Condvar,
}

impl<T> Inner<T> {
    fn try_reserve(&self, bytes: usize) -> Result<Permit<'_, T>, ReserveError> {
        if self.closed.load(Ordering::Acquire) {
            return Err(ReserveError::Closed);
        }
        self.bytes_used
            .fetch_update(Ordering::AcqRel, Ordering::Relaxed, |used| {
                used.checked_add(bytes)
                    .filter(|next| *next <= self.byte_capacity)
            })
            .map_err(|_| ReserveError::Full)?;

        loop {
            let position = self.enqueue.load(Ordering::Relaxed);
            let slot = &self.ring[position & self.mask];
            let sequence = slot.sequence.load(Ordering::Acquire);
            let difference = sequence.wrapping_sub(position).cast_signed();
            match difference.cmp(&0) {
                core::cmp::Ordering::Equal => {
                    if self
                        .enqueue
                        .compare_exchange_weak(
                            position,
                            position.wrapping_add(1),
                            Ordering::Relaxed,
                            Ordering::Relaxed,
                        )
                        .is_ok()
                    {
                        return Ok(Permit {
                            inner: self,
                            slot,
                            position,
                            bytes,
                            published: false,
                        });
                    }
                }
                core::cmp::Ordering::Less => {
                    self.bytes_used.fetch_sub(bytes, Ordering::Release);
                    return Err(ReserveError::Full);
                }
                core::cmp::Ordering::Greater => core::hint::spin_loop(),
            }
        }
    }

    fn pop(&self) -> Pop<T> {
        loop {
            let position = self.dequeue.load(Ordering::Relaxed);
            let slot = &self.ring[position & self.mask];
            let sequence = slot.sequence.load(Ordering::Acquire);
            let difference = sequence
                .wrapping_sub(position.wrapping_add(1))
                .cast_signed();
            if difference == 0 {
                self.dequeue
                    .store(position.wrapping_add(1), Ordering::Relaxed);
                let cancelled = slot.cancelled.load(Ordering::Relaxed);
                let bytes = slot.bytes.load(Ordering::Relaxed);
                let value = if cancelled {
                    None
                } else {
                    // SAFETY: the producer wrote exactly one T before publishing
                    // this sequence with Release; this is the single consumer.
                    Some(unsafe { (*slot.value.get()).assume_init_read() })
                };
                slot.sequence
                    .store(position.wrapping_add(self.capacity), Ordering::Release);
                if bytes != 0 {
                    self.bytes_used.fetch_sub(bytes, Ordering::Release);
                }
                self.notify_change();
                if let Some(value) = value {
                    return Pop::Item(value);
                }
                continue;
            }
            if difference < 0 {
                return if self.closed.load(Ordering::Acquire) && self.is_drained() {
                    Pop::Closed
                } else {
                    Pop::Empty
                };
            }
            core::hint::spin_loop();
        }
    }

    fn is_drained(&self) -> bool {
        self.dequeue.load(Ordering::Acquire) == self.enqueue.load(Ordering::Acquire)
    }

    fn notify_change(&self) {
        self.epoch.fetch_add(1, Ordering::Release);
        self.wake.notify_all();
    }

    fn park_until_change(&self, observed: u64) {
        let guard = self
            .wait_lock
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if self.epoch.load(Ordering::Acquire) == observed {
            drop(
                self.wake
                    .wait(guard)
                    .unwrap_or_else(std::sync::PoisonError::into_inner),
            );
        }
    }
}

impl<T> Drop for Inner<T> {
    fn drop(&mut self) {
        let mut position = self.dequeue.load(Ordering::Relaxed);
        let end = self.enqueue.load(Ordering::Relaxed);
        while position != end {
            let slot = &mut self.ring[position & self.mask];
            if slot.sequence.load(Ordering::Relaxed) == position.wrapping_add(1)
                && !slot.cancelled.load(Ordering::Relaxed)
            {
                // SAFETY: final Arc ownership proves no concurrent access, and a
                // published, non-cancelled slot contains one initialized T.
                unsafe { (*slot.value.get()).assume_init_drop() };
            }
            position = position.wrapping_add(1);
        }
    }
}

struct Slot<T> {
    sequence: AtomicUsize,
    bytes: AtomicUsize,
    cancelled: AtomicBool,
    value: UnsafeCell<MaybeUninit<T>>,
}

impl<T> Slot<T> {
    const fn new(sequence: usize) -> Self {
        Self {
            sequence: AtomicUsize::new(sequence),
            bytes: AtomicUsize::new(0),
            cancelled: AtomicBool::new(false),
            value: UnsafeCell::new(MaybeUninit::uninit()),
        }
    }
}

// SAFETY: ownership of the UnsafeCell moves from one producer to the sole
// consumer through the slot's release/acquire sequence protocol.
unsafe impl<T: Send> Sync for Slot<T> {}

/// An acquired queue slot. Publishing consumes it; dropping cancels it.
pub struct Permit<'a, T> {
    inner: &'a Inner<T>,
    slot: &'a Slot<T>,
    position: usize,
    bytes: usize,
    published: bool,
}

impl<T> Permit<'_, T> {
    /// Store `value` and make it visible to the consumer.
    pub fn publish(mut self, value: T) {
        // SAFETY: this permit uniquely owns the reserved slot until the Release
        // publication below.
        unsafe { (*self.slot.value.get()).write(value) };
        self.slot.bytes.store(self.bytes, Ordering::Relaxed);
        self.slot.cancelled.store(false, Ordering::Relaxed);
        self.slot
            .sequence
            .store(self.position.wrapping_add(1), Ordering::Release);
        self.published = true;
        self.inner.notify_change();
    }
}

impl<T> Drop for Permit<'_, T> {
    fn drop(&mut self) {
        if self.published {
            return;
        }
        self.slot.bytes.store(0, Ordering::Relaxed);
        self.slot.cancelled.store(true, Ordering::Relaxed);
        self.inner
            .bytes_used
            .fetch_sub(self.bytes, Ordering::Release);
        self.slot
            .sequence
            .store(self.position.wrapping_add(1), Ordering::Release);
        self.inner.notify_change();
    }
}

/// Result of a consumer pop.
#[derive(Debug, PartialEq, Eq)]
pub enum Pop<T> {
    /// One item was consumed.
    Item(T),
    /// No item is currently published.
    Empty,
    /// The queue is closed and drained.
    Closed,
}

/// Invalid queue capacity configuration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QueueConfigError;

impl fmt::Display for QueueConfigError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("queue slots must be a power of two and both bounds must be positive")
    }
}

impl std::error::Error for QueueConfigError {}

/// A queue reservation could not be made.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReserveError {
    /// A configured bound is currently exhausted.
    Full,
    /// The item can never fit within the configured byte bound.
    TooLarge,
    /// The queue is shutting down.
    Closed,
}

impl fmt::Display for ReserveError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Full => formatter.write_str("queue capacity is exhausted"),
            Self::TooLarge => formatter.write_str("item exceeds the queue byte bound"),
            Self::Closed => formatter.write_str("queue is closed"),
        }
    }
}

impl std::error::Error for ReserveError {}

#[cfg(test)]
mod tests {
    use std::thread;

    use super::*;

    #[test]
    fn enforces_slot_and_byte_bounds_before_publish() {
        let queue = BoundedQueue::new(2, 10).unwrap();
        let first = queue.try_reserve(6).unwrap();
        assert!(matches!(queue.try_reserve(5), Err(ReserveError::Full)));
        first.publish(11);
        assert_eq!(queue.pop(), Pop::Item(11));
        assert_eq!(queue.bytes_used(), 0);
    }

    #[test]
    fn cancelled_permit_does_not_block_the_ring() {
        let queue = BoundedQueue::new(2, 32).unwrap();
        drop(queue.try_reserve(8).unwrap());
        let permit = queue.try_reserve(8).unwrap();
        permit.publish(42);
        assert_eq!(queue.pop(), Pop::Item(42));
    }

    #[test]
    fn multiple_producers_deliver_every_item() {
        let queue = BoundedQueue::new(256, 1 << 20).unwrap();
        thread::scope(|scope| {
            for producer in 0..4_u32 {
                let sender = queue.clone();
                scope.spawn(move || {
                    for item in 0..2_000_u32 {
                        sender.reserve(4).unwrap().publish((producer, item));
                    }
                });
            }
            let mut count = 0;
            while count != 8_000 {
                if let Pop::Item(_) = queue.pop_wait() {
                    count += 1;
                }
            }
        });
    }
}
