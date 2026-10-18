//! Fixed-allocation, slot-and-byte bounded MPSC handoff.
//!
//! The fast path is lock-free. A mutex and condition variable are touched only
//! after the queue applies backpressure or when an idle consumer sleeps.

#[cfg(not(loom))]
use core::cell::UnsafeCell;
use core::fmt;
use core::mem::MaybeUninit;
use core::sync::atomic::Ordering;
#[cfg(not(loom))]
use core::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, fence};
#[cfg(not(loom))]
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

use crossbeam_utils::CachePadded;
#[cfg(loom)]
use loom::cell::UnsafeCell;
#[cfg(loom)]
use loom::sync::{
    Arc, Condvar, Mutex,
    atomic::{AtomicBool, AtomicU64, AtomicUsize, fence},
};

/// The clonable producer and control handle for a bounded MPSC queue.
///
/// [`Self::new`] also returns the sole [`QueueReceiver`]. Cloning this handle
/// never grants consumer access.
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
    /// Allocate producer and receiver handles with independent slot and byte limits.
    ///
    /// `slots` must be a power of two of at least two, and `bytes` must be positive.
    /// A one-slot ring cannot distinguish published data from the next free slot.
    pub fn new(slots: usize, bytes: usize) -> Result<(Self, QueueReceiver<T>), QueueConfigError> {
        if slots < 2 || !slots.is_power_of_two() || bytes == 0 {
            return Err(QueueConfigError);
        }
        let mut ring = Vec::with_capacity(slots);
        for sequence in 0..slots {
            ring.push(Slot::new(sequence));
        }
        let sender = Self {
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
                waiters: AtomicUsize::new(0),
                wait_lock: Mutex::new(()),
                wake: Condvar::new(),
            }),
        };
        let receiver = QueueReceiver {
            queue: sender.clone(),
        };
        Ok((sender, receiver))
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

/// The unique consumer of a [`BoundedQueue`].
///
/// Consumer methods require `&mut self`, so safe callers cannot read the same
/// slot concurrently. The receiver can move between threads; it cannot be cloned.
/// Dropping it closes the queue and wakes blocked producers.
///
/// The consumer cannot be duplicated:
///
/// ```compile_fail
/// use eventd_core::BoundedQueue;
/// let (_, receiver) = BoundedQueue::<u32>::new(2, 8).unwrap();
/// let other = receiver.clone();
/// ```
///
/// A shared reference cannot consume a value:
///
/// ```compile_fail
/// use eventd_core::BoundedQueue;
/// let (_, receiver) = BoundedQueue::<u32>::new(2, 8).unwrap();
/// let shared = &receiver;
/// shared.pop();
/// ```
pub struct QueueReceiver<T> {
    queue: BoundedQueue<T>,
}

impl<T> QueueReceiver<T> {
    /// Clone a producer/control handle for this receiver's queue.
    #[must_use]
    pub fn sender(&self) -> BoundedQueue<T> {
        self.queue.clone()
    }

    /// Pop one item, requiring exclusive access to the receiver.
    #[must_use]
    pub fn pop(&mut self) -> Pop<T> {
        self.queue.inner.pop()
    }

    /// Wait until an item arrives or the queue is closed.
    #[must_use]
    pub fn pop_wait(&mut self) -> Pop<T> {
        loop {
            let observed = self.queue.inner.epoch.load(Ordering::Acquire);
            match self.pop() {
                Pop::Empty => self.queue.inner.park_until_change(observed),
                result => return result,
            }
        }
    }

    /// Wait until an item arrives, the queue is closed, or
    /// `timeout` passes; [`Pop::Empty`] means the time ran out. A timeout beyond
    /// the clock's representable range waits until data or closure instead.
    #[must_use]
    pub fn pop_wait_timeout(&mut self, timeout: Duration) -> Pop<T> {
        // An unrepresentable deadline is farther away than this clock can
        // express, so waiting without a timer avoids an arithmetic panic.
        let Some(deadline) = Instant::now().checked_add(timeout) else {
            return self.pop_wait();
        };
        loop {
            let observed = self.queue.inner.epoch.load(Ordering::Acquire);
            match self.pop() {
                Pop::Empty => {
                    let remaining = deadline.saturating_duration_since(Instant::now());
                    if remaining.is_zero() {
                        return Pop::Empty;
                    }
                    self.queue.inner.park_until_change_or(observed, remaining);
                }
                result => return result,
            }
        }
    }

    /// Prevent new reservations and wake all sleepers.
    pub fn close(&self) {
        self.queue.close();
    }

    /// Approximate number of reserved or published slots.
    #[must_use]
    pub fn len(&self) -> usize {
        self.queue.len()
    }

    /// Whether the queue currently has no reserved slots.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }

    /// Bytes currently reserved or awaiting consumption.
    #[must_use]
    pub fn bytes_used(&self) -> usize {
        self.queue.bytes_used()
    }
}

impl<T> Drop for QueueReceiver<T> {
    fn drop(&mut self) {
        self.close();
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
    waiters: AtomicUsize,
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
                        let permit = Permit {
                            inner: self,
                            slot,
                            position,
                            bytes,
                            published: false,
                        };
                        // Closing can race the capacity checks and slot
                        // claim. Do not hand out a new permit after that
                        // race: the consumer may already have drained and
                        // exited. Dropping this permit cancels its slot.
                        // Pair with is_drained's fence after it observes
                        // closed: the claim and close cannot both be missed.
                        fence(Ordering::SeqCst);
                        return if self.closed.load(Ordering::Acquire) {
                            Err(ReserveError::Closed)
                        } else {
                            Ok(permit)
                        };
                    }
                }
                core::cmp::Ordering::Less => {
                    self.bytes_used.fetch_sub(bytes, Ordering::Release);
                    return Err(ReserveError::Full);
                }
                core::cmp::Ordering::Greater => queue_spin_loop(),
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
                    Some(with_slot_value(&slot.value, |value| unsafe {
                        (*value).assume_init_read()
                    }))
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
            queue_spin_loop();
        }
    }

    fn is_drained(&self) -> bool {
        // Called only after observing closed. Pair with the fence between a
        // producer's claim and its closed re-check: either we see its claim,
        // or it sees closed and cancels, so no accepted item is left behind.
        fence(Ordering::SeqCst);
        self.dequeue.load(Ordering::Acquire) == self.enqueue.load(Ordering::Acquire)
    }

    fn notify_change(&self) {
        self.epoch.fetch_add(1, Ordering::Release);
        // Pair with the fence between waiter registration and its epoch
        // check: either we see a waiter, or it sees the change and never parks.
        fence(Ordering::SeqCst);
        // Keep the fast path free of mutex acquisition when nobody is waiting.
        if self.waiters.load(Ordering::Acquire) != 0 {
            let _guard = self
                .wait_lock
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            // The waiter releases this mutex atomically with going to sleep,
            // so notification cannot fall between its check and its wait.
            self.wake.notify_all();
        }
    }

    fn park_until_change(&self, observed: u64) {
        let guard = self
            .wait_lock
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        self.waiters.fetch_add(1, Ordering::Release);
        fence(Ordering::SeqCst);
        let guard = if self.epoch.load(Ordering::Acquire) == observed {
            self.wake
                .wait(guard)
                .unwrap_or_else(std::sync::PoisonError::into_inner)
        } else {
            guard
        };
        self.waiters.fetch_sub(1, Ordering::Release);
        drop(guard);
    }

    fn park_until_change_or(&self, observed: u64, timeout: Duration) {
        let guard = self
            .wait_lock
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        self.waiters.fetch_add(1, Ordering::Release);
        fence(Ordering::SeqCst);
        let guard = if self.epoch.load(Ordering::Acquire) == observed {
            self.wake
                .wait_timeout(guard, timeout)
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .0
        } else {
            guard
        };
        self.waiters.fetch_sub(1, Ordering::Release);
        drop(guard);
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
                with_slot_value(&slot.value, |value| unsafe {
                    (*value).assume_init_drop();
                });
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
    // Loom atomics must be constructed at runtime inside a model.
    #[allow(clippy::missing_const_for_fn)]
    fn new(sequence: usize) -> Self {
        Self {
            sequence: AtomicUsize::new(sequence),
            bytes: AtomicUsize::new(0),
            cancelled: AtomicBool::new(false),
            value: UnsafeCell::new(MaybeUninit::uninit()),
        }
    }
}

// SAFETY: ownership of the UnsafeCell moves from one producer to the sole
// consumer through the slot's release/acquire sequence protocol. Only the
// non-clonable QueueReceiver exposes pop, through an exclusive &mut borrow.
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
        with_slot_value(&self.slot.value, |slot| unsafe {
            (*slot).write(value);
        });
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
        formatter.write_str("queue slots must be a power of two of at least two and the byte bound must be positive")
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

#[cfg(all(test, not(loom)))]
mod tests {
    use std::thread;

    use super::*;

    #[test]
    fn rejects_single_slot_rings() {
        assert!(matches!(
            BoundedQueue::<u32>::new(1, 8),
            Err(QueueConfigError)
        ));
    }

    #[test]
    fn dropping_receiver_wakes_a_blocked_producer() {
        let (sender, receiver) = BoundedQueue::new(2, 1).unwrap();
        sender.reserve(1).unwrap().publish(1);
        let producer = sender.clone();
        let (finished, result) = std::sync::mpsc::channel();
        let thread = thread::spawn(move || {
            let reservation = producer.reserve(1).map(|_| ());
            finished.send(reservation).unwrap();
        });
        let deadline = Instant::now() + Duration::from_secs(5);
        while sender.inner.waiters.load(Ordering::Acquire) == 0 {
            assert!(
                Instant::now() < deadline,
                "producer did not wait for capacity"
            );
            thread::yield_now();
        }
        drop(receiver);
        assert_eq!(
            result.recv_timeout(Duration::from_secs(5)).unwrap(),
            Err(ReserveError::Closed)
        );
        thread.join().unwrap();
    }

    #[test]
    fn an_unrepresentable_timeout_still_receives_published_data() {
        let (sender, mut receiver) = BoundedQueue::new(2, 8).unwrap();
        sender.reserve(1).unwrap().publish(17);
        assert_eq!(receiver.pop_wait_timeout(Duration::MAX), Pop::Item(17));
        sender.close();
        assert_eq!(receiver.pop_wait_timeout(Duration::MAX), Pop::Closed);
    }

    #[test]
    fn no_waiter_fast_path_does_not_take_the_wait_mutex() {
        let (queue, mut receiver) = BoundedQueue::new(2, 2).unwrap();
        let guard = queue.inner.wait_lock.lock().unwrap();
        let (done, result) = std::sync::mpsc::channel();
        thread::scope(|scope| {
            let worker = queue.clone();
            let handle = scope.spawn(move || {
                worker.try_reserve(1).unwrap().publish(7);
                assert_eq!(receiver.pop(), Pop::Item(7));
                worker.close();
                assert_eq!(receiver.pop(), Pop::Closed);
                done.send(()).unwrap();
            });
            let finished = result.recv_timeout(Duration::from_secs(10));
            // Release on failure too, so an unwanted lock cannot hang the test.
            drop(guard);
            handle.join().unwrap();
            finished.unwrap();
        });
    }

    #[test]
    fn enforces_slot_and_byte_bounds_before_publish() {
        let (queue, mut receiver) = BoundedQueue::new(2, 10).unwrap();
        let first = queue.try_reserve(6).unwrap();
        assert!(matches!(queue.try_reserve(5), Err(ReserveError::Full)));
        first.publish(11);
        assert_eq!(receiver.pop(), Pop::Item(11));
        assert_eq!(queue.bytes_used(), 0);
    }

    #[test]
    fn cancelled_permit_does_not_block_the_ring() {
        let (queue, mut receiver) = BoundedQueue::new(2, 32).unwrap();
        drop(queue.try_reserve(8).unwrap());
        let permit = queue.try_reserve(8).unwrap();
        permit.publish(42);
        assert_eq!(receiver.pop(), Pop::Item(42));
    }

    #[test]
    fn a_timed_wait_ends_empty_or_with_what_arrives() {
        let (queue, mut receiver) = BoundedQueue::new(4, 64).unwrap();
        let started = std::time::Instant::now();
        assert_eq!(
            receiver.pop_wait_timeout(Duration::from_millis(50)),
            Pop::<u32>::Empty
        );
        assert!(started.elapsed() >= Duration::from_millis(50));
        thread::scope(|scope| {
            let sender = queue.clone();
            scope.spawn(move || {
                thread::sleep(Duration::from_millis(20));
                sender.reserve(4).unwrap().publish(7);
            });
            assert_eq!(
                receiver.pop_wait_timeout(Duration::from_secs(10)),
                Pop::Item(7)
            );
        });
        queue.close();
        assert_eq!(
            receiver.pop_wait_timeout(Duration::from_secs(10)),
            Pop::Closed
        );
    }

    #[test]
    fn multiple_producers_deliver_every_item() {
        let (queue, mut receiver) = BoundedQueue::new(256, 1 << 20).unwrap();
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
                if let Pop::Item(_) = receiver.pop_wait() {
                    count += 1;
                }
            }
        });
    }

    #[test]
    fn closing_with_active_producers_drains_every_admitted_item() {
        for _ in 0..100 {
            let (queue, mut receiver) = BoundedQueue::new(8, 64).unwrap();
            let published = AtomicUsize::new(0);
            let drained = thread::scope(|scope| {
                for _ in 0..4 {
                    let queue = &queue;
                    let published = &published;
                    scope.spawn(move || {
                        loop {
                            match queue.reserve(1) {
                                Ok(permit) => {
                                    permit.publish(());
                                    published.fetch_add(1, Ordering::Relaxed);
                                }
                                Err(ReserveError::Closed) => break,
                                Err(error) => panic!("unexpected reservation failure: {error}"),
                            }
                        }
                    });
                }
                while published.load(Ordering::Relaxed) < 4 {
                    thread::yield_now();
                }
                queue.close();
                let mut drained = 0;
                while receiver.pop_wait() == Pop::Item(()) {
                    drained += 1;
                }
                // Joining the producers is outside this consumer's loop:
                // none may publish an item behind its final Closed result.
                drained
            });
            assert_eq!(drained, published.load(Ordering::Relaxed));
        }
    }
}

// Loom needs an explicit scheduling point in retry loops.
fn queue_spin_loop() {
    #[cfg(loom)]
    loom::thread::yield_now();
    #[cfg(not(loom))]
    core::hint::spin_loop();
}

#[cfg(all(test, loom))]
mod loom_tests {
    use loom::thread;

    use super::*;

    #[test]
    fn publish_wakes_an_idle_consumer() {
        loom::model(|| {
            let (queue, mut receiver) = BoundedQueue::new(2, 2).unwrap();
            let producer = queue.clone();
            let sender = thread::spawn(move || producer.reserve(1).unwrap().publish(7));
            assert_eq!(receiver.pop_wait(), Pop::Item(7));
            sender.join().unwrap();
        });
    }

    #[test]
    fn freed_capacity_wakes_a_blocked_producer() {
        loom::model(|| {
            let (queue, mut receiver) = BoundedQueue::new(2, 1).unwrap();
            queue.reserve(1).unwrap().publish(1);
            let producer = queue.clone();
            let sender = thread::spawn(move || producer.reserve(1).unwrap().publish(2));
            assert_eq!(receiver.pop_wait(), Pop::Item(1));
            assert_eq!(receiver.pop_wait(), Pop::Item(2));
            sender.join().unwrap();
        });
    }

    #[test]
    fn close_cannot_leave_an_admitted_item_behind() {
        loom::model(|| {
            let (queue, mut receiver) = BoundedQueue::new(2, 2).unwrap();
            let producer = queue.clone();
            let sender = thread::spawn(move || match producer.try_reserve(1) {
                Ok(permit) => {
                    permit.publish(7);
                    true
                }
                Err(ReserveError::Closed) => false,
                Err(error) => panic!("unexpected reservation failure: {error}"),
            });
            queue.close();
            let mut received = 0;
            loop {
                match receiver.pop_wait() {
                    Pop::Item(7) => received += 1,
                    Pop::Closed => break,
                    other => panic!("unexpected queue result: {other:?}"),
                }
            }
            assert_eq!(received, usize::from(sender.join().unwrap()));
        });
    }

    // Shutdown closes the queue from the pipeline thread while the writer
    // drains it. With the closer on the consumer thread, notify_change's
    // fence already orders its closed store before the drained check; only a
    // separate closer depends on is_drained's own fence. Three threads need a
    // preemption bound to keep the model fast.
    #[test]
    fn close_from_another_thread_cannot_leave_an_admitted_item_behind() {
        let mut builder = loom::model::Builder::new();
        builder.preemption_bound = Some(3);
        builder.check(|| {
            let (queue, mut receiver) = BoundedQueue::new(2, 2).unwrap();
            let producer = queue.clone();
            let closer = queue.clone();
            let sender = thread::spawn(move || match producer.try_reserve(1) {
                Ok(permit) => {
                    permit.publish(7);
                    true
                }
                Err(ReserveError::Closed) => false,
                Err(error) => panic!("unexpected reservation failure: {error}"),
            });
            let closing = thread::spawn(move || closer.close());
            let mut received = 0;
            loop {
                match receiver.pop_wait() {
                    Pop::Item(7) => received += 1,
                    Pop::Closed => break,
                    other => panic!("unexpected queue result: {other:?}"),
                }
            }
            closing.join().unwrap();
            assert_eq!(received, usize::from(sender.join().unwrap()));
        });
    }
}

// Keep payload accesses inside Loom's tracking guard while using the same
// release/acquire ownership protocol and payload operations in both builds.
fn with_slot_value<T, R>(
    value: &UnsafeCell<MaybeUninit<T>>,
    access: impl FnOnce(*mut MaybeUninit<T>) -> R,
) -> R {
    #[cfg(loom)]
    {
        value.with_mut(access)
    }
    #[cfg(not(loom))]
    {
        access(value.get())
    }
}
