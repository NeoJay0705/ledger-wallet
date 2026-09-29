//! Fixed-capacity storage for the Tokio request-ring benchmark.
//!
//! `Metadata` is mutated only while the benchmark's single async metadata
//! mutex is held. `push` writes at `tail` and publishes the new tail only after
//! the slot is initialized. `seal` turns the consecutive sealed prefix into a
//! non-Clone range token. `claim` consumes that token and grants the sole
//! worker exclusive mutable access to that range. Producers may append only
//! while `tail - head < capacity`, so every appended slot is disjoint from the
//! claimed range. The worker keeps `head` fixed until every response send has
//! been attempted, then drops the whole range and advances `head` in one
//! synchronous call under the metadata mutex. The mutex unlock/lock pair and
//! per-slot release/acquire flag publish initialized requests to the worker.
//!
//! The ring's `Drop` scans per-slot live flags, so cancellation or task failure
//! drops every still-initialized value once after all ring references are gone.

use std::cell::UnsafeCell;
use std::mem::MaybeUninit;
use std::ops::Range;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::Notify;

struct Slot<T> {
    value: UnsafeCell<MaybeUninit<T>>,
    initialized: AtomicBool,
}

impl<T> Slot<T> {
    fn empty() -> Self {
        Self {
            value: UnsafeCell::new(MaybeUninit::uninit()),
            initialized: AtomicBool::new(false),
        }
    }
}

/// A fixed-size array of slots. The array never reallocates after construction.
pub struct FixedRing<T> {
    slots: Box<[Slot<T>]>,
    identity: Arc<()>,
    metadata_issued: AtomicBool,
}

// SAFETY: Each value is accessed through a unique logical sequence token. The
// metadata mutex serializes producers and publication; a claimed batch is
// disjoint from all producer writes because tail-head is kept below capacity.
// `T: Send` permits the uniquely-owned value to move between producer, worker,
// and final-drop threads. `UnsafeCell` prevents ordinary shared references to
// slot contents.
unsafe impl<T: Send> Sync for FixedRing<T> {}

impl<T> FixedRing<T> {
    pub fn new(capacity: usize) -> Result<Self, String> {
        if capacity == 0 {
            return Err("request ring capacity must be nonzero".to_owned());
        }
        u64::try_from(capacity)
            .map_err(|_| "request ring capacity exceeds the u64 index space".to_owned())?;
        let mut slots = Vec::new();
        slots
            .try_reserve_exact(capacity)
            .map_err(|error| format!("could not allocate request ring: {error}"))?;
        slots.resize_with(capacity, Slot::empty);
        Ok(Self {
            slots: slots.into_boxed_slice(),
            identity: Arc::new(()),
            metadata_issued: AtomicBool::new(false),
        })
    }

    pub fn capacity(&self) -> usize {
        self.slots.len()
    }

    /// Issue the ring's sole mutable metadata token. The non-Clone token is
    /// bound to this exact storage allocation and cannot be issued twice.
    pub fn metadata(&self) -> Result<Metadata, RingError> {
        if self
            .metadata_issued
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return Err(RingError::InvalidState("metadata token was already issued"));
        }
        Ok(Metadata {
            identity: Arc::clone(&self.identity),
            head: 0,
            tail: 0,
            sealed_tail: 0,
            claimed_end: None,
        })
    }

    pub fn occupancy(&self, metadata: &Metadata) -> Result<usize, RingError> {
        self.check_metadata(metadata)?;
        let occupied = metadata
            .tail
            .checked_sub(metadata.head)
            .ok_or(RingError::InvalidState("tail preceded head"))?;
        let occupied = usize::try_from(occupied)
            .map_err(|_| RingError::InvalidState("occupancy exceeded platform size"))?;
        if occupied > self.capacity() {
            return Err(RingError::InvalidState(
                "occupancy exceeded fixed ring capacity",
            ));
        }
        Ok(occupied)
    }

    fn check_metadata(&self, metadata: &Metadata) -> Result<(), RingError> {
        if Arc::ptr_eq(&self.identity, &metadata.identity) {
            Ok(())
        } else {
            Err(RingError::InvalidState(
                "metadata token belongs to a different ring",
            ))
        }
    }

    /// Initialize and publish one slot, or return the value if the ring is full.
    pub fn push(&self, metadata: &mut Metadata, value: T) -> Result<u64, PushError<T>> {
        let occupied = match self.occupancy(metadata) {
            Ok(occupied) => occupied,
            Err(error) => return Err(PushError::InvalidState(value, error)),
        };
        if occupied == self.capacity() {
            return Err(PushError::Full(value));
        }
        let Some(next_tail) = metadata.tail.checked_add(1) else {
            return Err(PushError::SequenceOverflow(value));
        };
        let sequence = metadata.tail;
        let slot_index = (sequence % self.capacity() as u64) as usize;
        let slot = &self.slots[slot_index];
        if slot.initialized.load(Ordering::Acquire) {
            return Err(PushError::InvalidState(
                value,
                RingError::InvalidState("target slot was still initialized"),
            ));
        }

        // SAFETY: `metadata` is exclusively borrowed by this call. Its
        // tail-head occupancy check proves this logical sequence is outside all
        // live slots, and the live flag above confirms this physical slot has
        // already been reclaimed. The value is initialized exactly once here.
        unsafe { (*slot.value.get()).write(value) };
        slot.initialized.store(true, Ordering::Release);
        // No cancellation point or fallible operation occurs between the slot
        // write and publishing tail. Callers publish this metadata by releasing
        // their Tokio mutex guard.
        metadata.tail = next_tail;
        Ok(sequence)
    }

    /// Update the just-published active tail slot while admission still holds
    /// the metadata mutex. This is used to stamp `t_admit` immediately after
    /// append publication, before the active batch can be sealed.
    #[allow(dead_code)]
    pub fn update_unsealed_tail(
        &self,
        metadata: &mut Metadata,
        sequence: u64,
        update: impl FnOnce(&mut T),
    ) -> Result<(), RingError> {
        self.check_metadata(metadata)?;
        let Some(last_sequence) = metadata.tail.checked_sub(1) else {
            return Err(RingError::InvalidState("cannot update an empty ring"));
        };
        if sequence != last_sequence || sequence < metadata.sealed_tail {
            return Err(RingError::InvalidState(
                "only the latest unsealed tail slot can be updated",
            ));
        }
        let slot_index = (sequence % self.capacity() as u64) as usize;
        let slot = &self.slots[slot_index];
        if !slot.initialized.load(Ordering::Acquire) {
            return Err(RingError::InvalidState(
                "unsealed tail slot was not initialized",
            ));
        }
        // SAFETY: the one-shot metadata token is exclusively borrowed. This is
        // the latest unsealed tail slot, so it is disjoint from every worker
        // claim; the update closure runs synchronously before admission can
        // seal the slot and release the metadata mutex.
        let value = unsafe { (&mut *slot.value.get()).assume_init_mut() };
        update(value);
        Ok(())
    }

    /// Seal the consecutive active range since the preceding seal.
    pub fn seal(&self, metadata: &mut Metadata) -> Result<Option<SealedRange>, RingError> {
        self.check_metadata(metadata)?;
        if metadata.sealed_tail > metadata.tail {
            return Err(RingError::InvalidState("sealed tail exceeded tail"));
        }
        if metadata.sealed_tail == metadata.tail {
            return Ok(None);
        }
        let start = metadata.sealed_tail;
        let end = metadata.tail;
        metadata.sealed_tail = end;
        Ok(Some(SealedRange {
            identity: Arc::clone(&self.identity),
            start,
            end,
        }))
    }

    /// Claim exactly the next FIFO sealed range for the single consumer.
    pub fn claim<'ring>(
        &'ring self,
        metadata: &mut Metadata,
        range: SealedRange,
    ) -> Result<Batch<'ring, T>, RingError> {
        self.check_metadata(metadata)?;
        if !Arc::ptr_eq(&self.identity, &range.identity) {
            return Err(RingError::InvalidState(
                "sealed range belongs to a different ring",
            ));
        }
        if metadata.claimed_end.is_some() {
            return Err(RingError::InvalidState("a batch is already claimed"));
        }
        if range.start != metadata.head {
            return Err(RingError::InvalidState(
                "sealed ranges were claimed out of order",
            ));
        }
        if range.end <= range.start || range.end > metadata.sealed_tail {
            return Err(RingError::InvalidState("invalid sealed range bounds"));
        }
        metadata.claimed_end = Some(range.end);
        Ok(Batch {
            ring: self,
            start: range.start,
            end: range.end,
        })
    }
}

impl<T> Drop for FixedRing<T> {
    fn drop(&mut self) {
        for slot in self.slots.iter_mut() {
            if slot.initialized.swap(false, Ordering::AcqRel) {
                // SAFETY: the ring is being dropped after its final reference is
                // gone, so no producer or batch token can access a slot. The
                // live flag is cleared before dropping, preventing a second
                // drop if a value destructor unwinds.
                unsafe { (*slot.value.get()).assume_init_drop() };
            }
        }
    }
}

/// Logical request positions and single-worker claim state.
pub struct Metadata {
    identity: Arc<()>,
    head: u64,
    tail: u64,
    sealed_tail: u64,
    claimed_end: Option<u64>,
}

/// A unique token for one sealed FIFO range. It cannot be copied or forged.
pub struct SealedRange {
    identity: Arc<()>,
    start: u64,
    end: u64,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum RingError {
    InvalidState(&'static str),
}

impl std::fmt::Display for RingError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidState(message) => write!(formatter, "request ring {message}"),
        }
    }
}

pub enum PushError<T> {
    Full(T),
    SequenceOverflow(T),
    InvalidState(T, RingError),
}

/// Tracks tasks sleeping because tail-head reached ring capacity. Reclaim
/// wakes at most one waiter per freed slot, avoiding broadcast wakeups over all
/// configured client coroutines.
pub struct SpaceWaiters {
    count: std::sync::atomic::AtomicU64,
}

impl SpaceWaiters {
    pub fn new() -> Self {
        Self {
            count: std::sync::atomic::AtomicU64::new(0),
        }
    }

    pub fn register(&self) -> Result<SpaceWaitGuard<'_>, RingError> {
        self.count
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |count| {
                count.checked_add(1)
            })
            .map_err(|_| RingError::InvalidState("full-waiter counter overflowed"))?;
        Ok(SpaceWaitGuard { waiters: self })
    }

    pub fn waiting(&self) -> u64 {
        self.count.load(Ordering::Acquire)
    }

    /// Wake no more than the number of slots reclaimed and waiters currently
    /// registered. Notification registration occurs before admission checks,
    /// so a reclaim cannot be lost between the full check and await.
    pub fn notify_reclaimed(&self, notify: &Notify, slots_reclaimed: usize) {
        let waiting = self.waiting();
        let wake_count = usize::try_from(waiting)
            .unwrap_or(usize::MAX)
            .min(slots_reclaimed);
        for _ in 0..wake_count {
            notify.notify_one();
        }
    }
}

impl Default for SpaceWaiters {
    fn default() -> Self {
        Self::new()
    }
}

pub struct SpaceWaitGuard<'a> {
    waiters: &'a SpaceWaiters,
}

impl Drop for SpaceWaitGuard<'_> {
    fn drop(&mut self) {
        let previous = self.waiters.count.fetch_sub(1, Ordering::AcqRel);
        assert!(
            previous != 0,
            "request ring full-waiter counter underflowed"
        );
    }
}

impl<T> std::fmt::Debug for PushError<T> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Full(_) => formatter.write_str("Full(..)"),
            Self::SequenceOverflow(_) => formatter.write_str("SequenceOverflow(..)"),
            Self::InvalidState(_, error) => {
                formatter.debug_tuple("InvalidState").field(error).finish()
            }
        }
    }
}

/// Exclusive mutable access to one claimed batch. It borrows the ring and is
/// consumed by `reclaim` after the caller finishes handler and dispatch work.
pub struct Batch<'ring, T> {
    ring: &'ring FixedRing<T>,
    start: u64,
    end: u64,
}

impl<T> Batch<'_, T> {
    pub fn len(&self) -> usize {
        (self.end - self.start) as usize
    }

    pub fn is_wrapped(&self) -> bool {
        let first_slot = (self.start % self.ring.capacity() as u64) as usize;
        first_slot + self.len() > self.ring.capacity()
    }

    /// Return the at-most-two physical slot slices in FIFO order.
    pub fn physical_segments(&self) -> (Range<usize>, Option<Range<usize>>) {
        let first_slot = (self.start % self.ring.capacity() as u64) as usize;
        let first_len = self.len().min(self.ring.capacity() - first_slot);
        let first = first_slot..first_slot + first_len;
        let second_len = self.len() - first_len;
        let second = (second_len != 0).then_some(0..second_len);
        (first, second)
    }

    /// Visit requests in physical FIFO order without moving or copying them.
    pub fn try_for_each_mut<E>(
        &mut self,
        mut visit: impl FnMut(&mut T) -> Result<(), E>,
    ) -> Result<(), E> {
        let (first, second) = self.physical_segments();
        for slot_index in first.chain(second.into_iter().flatten()) {
            let slot = &self.ring.slots[slot_index];
            assert!(
                slot.initialized.load(Ordering::Acquire),
                "claimed request ring slot was not initialized"
            );
            // SAFETY: the unforgeable batch token grants unique access to the
            // entire claimed range. Producer positions are disjoint by the
            // tail-head capacity invariant; only this one worker can claim a
            // range, and `&mut self` prevents overlapping visits in this batch.
            let value = unsafe { (&mut *slot.value.get()).assume_init_mut() };
            visit(value)?;
        }
        Ok(())
    }

    /// Drop every slot in the batch and publish the new head in one synchronous
    /// operation. Call only after all map updates and response send attempts.
    pub fn reclaim(self, metadata: &mut Metadata) -> Result<(), RingError> {
        self.ring.check_metadata(metadata)?;
        if metadata.head != self.start || metadata.claimed_end != Some(self.end) {
            return Err(RingError::InvalidState(
                "reclaim did not match the claimed range",
            ));
        }
        if self.end > metadata.tail {
            return Err(RingError::InvalidState("reclaim end exceeded tail"));
        }

        let (first, second) = self.physical_segments();
        for slot_index in first.chain(second.into_iter().flatten()) {
            let slot = &self.ring.slots[slot_index];
            if !slot.initialized.swap(false, Ordering::AcqRel) {
                return Err(RingError::InvalidState(
                    "reclaim found an uninitialized slot",
                ));
            }
            // SAFETY: the unique batch token owns this range; no outstanding
            // request references can exist after its synchronous visitors have
            // returned. The live flag is cleared before dropping the value.
            unsafe { (*slot.value.get()).assume_init_drop() };
        }

        // The benchmark's Request fields have non-panicking destructors
        // (oneshot Sender, Instant, integers). For a generic T whose destructor
        // unwinds, the live flag cleared above prevents a double drop; the
        // head remains unchanged, the task fails, and final ring Drop cleans
        // the remaining live slots. No await occurs in this reclaim section.
        metadata.head = self.end;
        metadata.claimed_end = None;
        Ok(())
    }
}

#[cfg(test)]
#[allow(dead_code, unused_imports)]
mod tests {
    use super::{FixedRing, PushError, SpaceWaiters};
    use std::sync::{Arc, Mutex};
    use tokio::sync::{Notify, mpsc};

    #[derive(Debug)]
    struct DropMark(usize, Arc<Mutex<Vec<usize>>>);

    impl Drop for DropMark {
        fn drop(&mut self) {
            self.1.lock().unwrap().push(self.0);
        }
    }

    #[test]
    fn full_ring_rejects_without_publishing_and_reclaims_once() {
        let ring = FixedRing::new(3).unwrap();
        let mut metadata = ring.metadata().unwrap();
        let drops = Arc::new(Mutex::new(Vec::new()));
        for id in 0..3 {
            ring.push(&mut metadata, DropMark(id, Arc::clone(&drops)))
                .unwrap();
        }
        let attempted = ring.push(&mut metadata, DropMark(usize::MAX, Arc::clone(&drops)));
        match attempted {
            Err(PushError::Full(value)) => drop(value),
            _ => panic!("full ring accepted or misclassified another request"),
        }
        let sealed = ring.seal(&mut metadata).unwrap().unwrap();
        let mut batch = ring.claim(&mut metadata, sealed).unwrap();
        let mut observed = Vec::new();
        batch
            .try_for_each_mut::<()>(|value| {
                observed.push(value.0);
                Ok(())
            })
            .unwrap();
        assert_eq!(observed, vec![0, 1, 2]);
        batch.reclaim(&mut metadata).unwrap();
        assert_eq!(ring.occupancy(&metadata).unwrap(), 0);

        let observed_drops = drops.lock().unwrap();
        assert_eq!(observed_drops.len(), 4);
        assert_eq!(
            observed_drops
                .iter()
                .copied()
                .collect::<std::collections::HashSet<_>>()
                .len(),
            4
        );
    }

    #[test]
    fn batches_wrap_through_multiple_cycles_in_slot_order() {
        let ring = FixedRing::new(5).unwrap();
        let mut metadata = ring.metadata().unwrap();
        let drops = Arc::new(Mutex::new(Vec::new()));

        for cycle in 0..4 {
            for offset in 0..3 {
                let id = cycle * 3 + offset;
                ring.push(&mut metadata, DropMark(id, Arc::clone(&drops)))
                    .unwrap();
            }
            let sealed = ring.seal(&mut metadata).unwrap().unwrap();
            let mut batch = ring.claim(&mut metadata, sealed).unwrap();
            assert_eq!(batch.len(), 3);
            let should_wrap = cycle == 1 || cycle == 3;
            assert_eq!(batch.is_wrapped(), should_wrap);
            let (first, second) = batch.physical_segments();
            assert_eq!(second.is_some(), should_wrap);
            if should_wrap {
                assert!(first.end == 5 && second.unwrap().start == 0);
            }
            let mut observed = Vec::new();
            batch
                .try_for_each_mut::<()>(|value| {
                    observed.push(value.0);
                    Ok(())
                })
                .unwrap();
            assert_eq!(observed, (cycle * 3..cycle * 3 + 3).collect::<Vec<_>>());
            batch.reclaim(&mut metadata).unwrap();
            assert_eq!(ring.occupancy(&metadata).unwrap(), 0);
        }

        let observed_drops = drops.lock().unwrap();
        assert_eq!(observed_drops.len(), 12);
        assert_eq!(
            observed_drops
                .iter()
                .copied()
                .collect::<std::collections::HashSet<_>>()
                .len(),
            12
        );
    }

    #[test]
    fn a_sealed_batch_can_be_read_while_later_slots_are_appended() {
        let ring = FixedRing::new(5).unwrap();
        let mut metadata = ring.metadata().unwrap();
        for id in 0..3 {
            ring.push(&mut metadata, id).unwrap();
        }
        let first = ring.seal(&mut metadata).unwrap().unwrap();
        let mut claimed = ring.claim(&mut metadata, first).unwrap();
        for id in 3..5 {
            ring.push(&mut metadata, id).unwrap();
        }

        let mut first_batch = Vec::new();
        claimed
            .try_for_each_mut::<()>(|value| {
                first_batch.push(*value);
                Ok(())
            })
            .unwrap();
        assert_eq!(first_batch, vec![0, 1, 2]);
        claimed.reclaim(&mut metadata).unwrap();

        let second = ring.seal(&mut metadata).unwrap().unwrap();
        let mut claimed = ring.claim(&mut metadata, second).unwrap();
        let mut second_batch = Vec::new();
        claimed
            .try_for_each_mut::<()>(|value| {
                second_batch.push(*value);
                Ok(())
            })
            .unwrap();
        assert_eq!(second_batch, vec![3, 4]);
        claimed.reclaim(&mut metadata).unwrap();
    }

    #[test]
    fn final_drop_cleans_unsealed_and_unreclaimed_slots_once() {
        let drops = Arc::new(Mutex::new(Vec::new()));
        {
            let ring = FixedRing::new(4).unwrap();
            let mut metadata = ring.metadata().unwrap();
            for id in 0..4 {
                ring.push(&mut metadata, DropMark(id, Arc::clone(&drops)))
                    .unwrap();
            }
            let range = ring.seal(&mut metadata).unwrap().unwrap();
            let mut claimed = ring.claim(&mut metadata, range).unwrap();
            claimed
                .try_for_each_mut::<()>(|value| {
                    if value.0 == 0 {
                        return Err(());
                    }
                    Ok(())
                })
                .unwrap_err();
            // Dropping the ring models sibling-task abort after handler error.
            // The live flags keep all four initialized slots eligible for one
            // final cleanup, even though the batch token is abandoned.
            drop(claimed);
        }
        let observed = drops.lock().unwrap();
        assert_eq!(observed.len(), 4);
        assert_eq!(
            observed
                .iter()
                .copied()
                .collect::<std::collections::HashSet<_>>()
                .len(),
            4
        );
    }

    #[test]
    fn metadata_is_one_shot_and_bound_to_its_ring() {
        let ring = FixedRing::<u64>::new(3).unwrap();
        let mut metadata = ring.metadata().unwrap();
        assert!(
            ring.metadata().is_err(),
            "metadata was issued more than once"
        );

        let foreign_ring = FixedRing::<u64>::new(3).unwrap();
        let mut foreign_metadata = foreign_ring.metadata().unwrap();
        assert!(ring.occupancy(&foreign_metadata).is_err());
        let push = ring.push(&mut foreign_metadata, 7);
        assert!(matches!(push, Err(PushError::InvalidState(7, _))));
        assert_eq!(ring.occupancy(&metadata).unwrap(), 0);
        assert_eq!(foreign_ring.occupancy(&foreign_metadata).unwrap(), 0);

        ring.push(&mut metadata, 11).unwrap();
        let foreign_range = ring.seal(&mut metadata).unwrap().unwrap();
        foreign_ring.push(&mut foreign_metadata, 22).unwrap();
        let _local_range = foreign_ring.seal(&mut foreign_metadata).unwrap().unwrap();
        assert!(
            foreign_ring
                .claim(&mut foreign_metadata, foreign_range)
                .is_err()
        );
        assert_eq!(foreign_ring.occupancy(&foreign_metadata).unwrap(), 1);
    }

    #[tokio::test]
    async fn out_of_order_producer_scheduling_preserves_admission_fifo() {
        let ring = Arc::new(FixedRing::new(16).unwrap());
        let metadata = Arc::new(tokio::sync::Mutex::new(ring.metadata().unwrap()));
        let admission_order = Arc::new(Mutex::new(Vec::new()));
        let mut producers = Vec::new();
        let (ack_tx, mut ack_rx) = tokio::sync::mpsc::unbounded_channel();
        let mut release = Vec::new();

        for id in 0_u64..12 {
            let ring = Arc::clone(&ring);
            let metadata = Arc::clone(&metadata);
            let admission_order = Arc::clone(&admission_order);
            let ack_tx = ack_tx.clone();
            let (release_tx, release_rx) = tokio::sync::oneshot::channel();
            release.push((id, release_tx));
            producers.push(tokio::spawn(async move {
                release_rx.await.unwrap();
                let mut state = metadata.lock().await;
                let sequence = ring.push(&mut state, id).unwrap();
                admission_order.lock().unwrap().push((sequence, id));
                ack_tx.send((sequence, id)).unwrap();
            }));
        }
        for (expected_id, release_tx) in release.into_iter().rev() {
            release_tx.send(()).unwrap();
            let (_, admitted_id) = ack_rx.recv().await.unwrap();
            assert_eq!(admitted_id, expected_id);
        }
        for producer in producers {
            producer.await.unwrap();
        }

        let mut state = metadata.lock().await;
        let range = ring.seal(&mut state).unwrap().unwrap();
        let mut batch = ring.claim(&mut state, range).unwrap();
        drop(state);
        let mut observed = Vec::new();
        batch
            .try_for_each_mut::<()>(|id| {
                observed.push(*id);
                Ok(())
            })
            .unwrap();
        let mut state = metadata.lock().await;
        batch.reclaim(&mut state).unwrap();

        let mut expected = admission_order.lock().unwrap().clone();
        expected.sort_unstable_by_key(|(sequence, _)| *sequence);
        assert_eq!(
            observed,
            expected.into_iter().map(|(_, id)| id).collect::<Vec<_>>()
        );
        assert_eq!(observed, (0_u64..12).rev().collect::<Vec<_>>());
    }

    #[tokio::test]
    async fn cancelled_full_waiters_unregister_and_reclaim_wakes_only_freed_slots() {
        async fn park_until_notified(
            waiters: Arc<SpaceWaiters>,
            notify: Arc<Notify>,
            ready: mpsc::UnboundedSender<()>,
            completed: mpsc::UnboundedSender<()>,
        ) {
            let notified = notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            let _registration = waiters.register().unwrap();
            ready.send(()).unwrap();
            notified.await;
            completed.send(()).unwrap();
        }

        let waiters = Arc::new(SpaceWaiters::new());
        let notify = Arc::new(Notify::new());
        let (ready_tx, mut ready_rx) = mpsc::unbounded_channel();
        let (done_tx, mut done_rx) = mpsc::unbounded_channel();

        let cancelled = tokio::spawn(park_until_notified(
            Arc::clone(&waiters),
            Arc::clone(&notify),
            ready_tx.clone(),
            done_tx.clone(),
        ));
        ready_rx.recv().await.unwrap();
        assert_eq!(waiters.waiting(), 1);
        cancelled.abort();
        let _ = cancelled.await;
        assert_eq!(waiters.waiting(), 0);

        let mut parked = Vec::new();
        for _ in 0..3 {
            parked.push(tokio::spawn(park_until_notified(
                Arc::clone(&waiters),
                Arc::clone(&notify),
                ready_tx.clone(),
                done_tx.clone(),
            )));
        }
        for _ in 0..3 {
            ready_rx.recv().await.unwrap();
        }
        assert_eq!(waiters.waiting(), 3);
        waiters.notify_reclaimed(&notify, 2);
        done_rx.recv().await.unwrap();
        done_rx.recv().await.unwrap();
        assert_eq!(waiters.waiting(), 1);
        waiters.notify_reclaimed(&notify, 8);
        done_rx.recv().await.unwrap();
        for task in parked {
            task.await.unwrap();
        }
        assert_eq!(waiters.waiting(), 0);
    }
}
