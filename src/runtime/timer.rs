//! Runtime-native timers backed by a user-space timing wheel.
//!
//! The runtime owns logical timer entries in a hierarchical timing wheel.
//! When the executor becomes idle, it computes the duration until the nearest
//! deadline and passes that as the blocking timeout for the reactor wait. When
//! the wait returns, the runtime expires due timers in user space and schedules
//! their owning tasks.
//!
//! Timer maintenance is budgeted the same way as the other executor phases:
//! cascades from higher levels are resumed incrementally, and pure I/O passes
//! skip clock reads entirely when no timers are pending.
//!
//! # Fast-Path Guidance
//!
//! Preferred when a fast path requires a deadline:
//! - Use the top-level [`sleep`], [`sleep_until`], [`timeout`], and
//!   [`timeout_at`] helpers; they use the executor-owned bounded timer wheel.
//! - Compute one absolute phase deadline and use [`timeout_at`] when several
//!   operations share the same budget.
//! - Account for timer storage explicitly: entries are acquired in fixed
//!   1024-entry slabs, but there is currently no user-configurable total timer
//!   cap.
//!
//! Avoid on the fast path:
//! - Do not wrap every small I/O step in a separate timer when one phase-level
//!   deadline preserves protocol semantics. Each armed timer consumes a pooled
//!   timer entry and adds insertion, cancellation, or expiry work.
//! - Do not remove a required timeout merely for speed. Keep per-operation
//!   timers when the protocol needs independent deadlines.
//!
//! # Example
//! ```no_run
//! use flowio::runtime::executor::Executor;
//! use flowio::runtime::timer::{sleep, timeout};
//! use std::time::Duration;
//!
//! let mut executor = Executor::new()?;
//! executor.run(async {
//!     let _ = sleep(Duration::from_millis(10)).await;
//!     let _ = timeout(Duration::from_millis(10), async {}).await;
//! })?;
//! # Ok::<(), std::io::Error>(())
//! ```

use crate::runtime::executor::{
    ExecutorOwner, PollCtx, next_timer_wake_epoch_unchecked, note_timer_expired,
    note_timer_now_tick_call, note_waiter_wake, poll_ctx_from_waker, schedule_ctx_unchecked,
    schedule_timer_woken_task_unchecked,
};
use crate::runtime::task::{
    TaskHeader, clear_task_ref, release_task, replace_task_ref, take_task_ref,
};
use crate::utils::list::intrusive::dlist::{DList, Link};
use crate::utils::memory::pool::{InPlaceInit, Pool};
use crate::utils::memory::provider::BasicMemoryProvider;
use crate::utils::memory::provider_owned_pool::ProviderOwnedPool;
use std::array;
use std::fmt;
use std::future::Future;
use std::io;
use std::mem::{ManuallyDrop, MaybeUninit};
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

/// Duration of one internal timer tick in nanoseconds.
pub const TIMER_TICK_NS: u64 = 1_000_000;
const LVL0_SLOTS: usize = 256;
const LVLN_SLOTS: usize = 64;
const TIMERS_PER_SLAB: usize = 1024;
const INVALID_BUCKET_LEVEL: u8 = u8::MAX;

#[derive(Clone, Copy, PartialEq, Eq)]
enum TimerState {
    /// Entry is not currently armed in the wheel.
    Idle,
    /// Entry is queued in the timing wheel and waiting to expire.
    Armed,
    /// Entry has expired and its waiter has already been scheduled.
    Fired,
    /// Executor shutdown detached the entry before its deadline.
    Cancelled,
}

#[repr(C)]
pub(crate) struct TimerEntry {
    /// Intrusive bucket link while the timer is queued in the wheel.
    link: Link,
    /// Task to wake when the timer expires. A non-null pointer owns one task
    /// reference.
    waiter: *mut TaskHeader,
    /// Absolute deadline expressed in timer ticks.
    deadline_tick: u64,
    /// Current timer lifecycle state.
    state: TimerState,
    /// Wheel level currently owning this entry, or `INVALID_BUCKET_LEVEL`.
    bucket_level: u8,
    /// Bucket index within the owning level.
    bucket_index: u16,
    /// Stable executor owner that allocated this timer entry.
    owner: Option<Rc<ExecutorOwner>>,
}

impl TimerEntry {
    const LINK_OFFSET: usize = std::mem::offset_of!(TimerEntry, link);

    const fn new() -> Self {
        Self {
            link: Link::new_unlinked(),
            waiter: std::ptr::null_mut(),
            deadline_tick: 0,
            state: TimerState::Idle,
            bucket_level: INVALID_BUCKET_LEVEL,
            bucket_index: 0,
            owner: None,
        }
    }

    /// Replaces this timer's owned waiter reference.
    ///
    /// # Safety
    ///
    /// A non-null `task` must point to a live task on its executor owner thread,
    /// and this entry must be exclusively accessible.
    unsafe fn register_waiter(&mut self, task: *mut TaskHeader) {
        unsafe { replace_task_ref(&mut self.waiter, task) };
    }

    fn take_waiter(&mut self) -> *mut TaskHeader {
        unsafe { take_task_ref(&mut self.waiter) }
    }

    fn clear_waiter(&mut self) {
        unsafe { clear_task_ref(&mut self.waiter) };
    }

    #[inline(always)]
    fn owner_ptr(&self) -> *const ExecutorOwner {
        self.owner.as_ref().map_or(std::ptr::null(), Rc::as_ptr)
    }
}

impl InPlaceInit for TimerEntry {
    type Args = ();

    fn init_at(slot: &mut MaybeUninit<Self>, _: Self::Args) {
        slot.write(TimerEntry::new());
    }
}

#[cfg(target_pointer_width = "64")]
const _: [(); 48] = [(); std::mem::size_of::<TimerEntry>()];

/// Hierarchical timing wheel plus the cached bookkeeping needed to resume
/// timer work incrementally across executor passes.
struct TimerWheel {
    /// Current tick processed by the wheel.
    current_tick: u64,
    /// Cached nearest known deadline across all buckets.
    next_deadline_tick: Option<u64>,
    /// True when `next_deadline_tick` must be recomputed before use.
    next_deadline_dirty: bool,
    /// Non-empty occupancy bits for the 256 level-0 buckets.
    lvl0_bits: [u64; 4],
    /// Non-empty occupancy bits for level-1 buckets.
    lvl1_bits: u64,
    /// Non-empty occupancy bits for level-2 buckets.
    lvl2_bits: u64,
    /// Non-empty occupancy bits for level-3 buckets.
    lvl3_bits: u64,
    /// Upper wheel levels with unfinished cascade work for the current tick.
    cascade_levels: [u8; 3],
    /// Bucket indices paired with `cascade_levels`.
    cascade_indices: [usize; 3],
    /// Number of valid entries in the cascade arrays.
    cascade_count: u8,
    /// Next cascade-array entry to resume.
    cascade_pos: u8,
    /// Near-future wheel buckets covering the lowest tick bits directly.
    lvl0: [DList<TimerEntry>; LVL0_SLOTS],
    /// Next coarser bucket level.
    lvl1: [DList<TimerEntry>; LVLN_SLOTS],
    /// Coarser bucket level for more distant deadlines.
    lvl2: [DList<TimerEntry>; LVLN_SLOTS],
    /// Outermost bucket level for the farthest tracked deadlines.
    lvl3: [DList<TimerEntry>; LVLN_SLOTS],
}

impl TimerWheel {
    fn new_uninit() -> Self {
        Self {
            current_tick: 0,
            next_deadline_tick: None,
            next_deadline_dirty: false,
            lvl0_bits: [0; 4],
            lvl1_bits: 0,
            lvl2_bits: 0,
            lvl3_bits: 0,
            cascade_levels: [0; 3],
            cascade_indices: [0; 3],
            cascade_count: 0,
            cascade_pos: 0,
            lvl0: array::from_fn(|_| DList::new_uninit()),
            lvl1: array::from_fn(|_| DList::new_uninit()),
            lvl2: array::from_fn(|_| DList::new_uninit()),
            lvl3: array::from_fn(|_| DList::new_uninit()),
        }
    }

    fn init(&mut self) -> io::Result<()> {
        self.current_tick = now_tick()?;
        self.next_deadline_tick = None;
        for bucket in &mut self.lvl0 {
            bucket.init();
        }
        for bucket in &mut self.lvl1 {
            bucket.init();
        }
        for bucket in &mut self.lvl2 {
            bucket.init();
        }
        for bucket in &mut self.lvl3 {
            bucket.init();
        }
        Ok(())
    }

    fn unlink_all_for_drop(&mut self) {
        for bucket in &mut self.lvl0 {
            bucket.unlink_all_for_drop();
        }
        for bucket in &mut self.lvl1 {
            bucket.unlink_all_for_drop();
        }
        for bucket in &mut self.lvl2 {
            bucket.unlink_all_for_drop();
        }
        for bucket in &mut self.lvl3 {
            bucket.unlink_all_for_drop();
        }

        self.next_deadline_tick = None;
        self.next_deadline_dirty = false;
        self.lvl0_bits = [0; 4];
        self.lvl1_bits = 0;
        self.lvl2_bits = 0;
        self.lvl3_bits = 0;
        self.cascade_count = 0;
        self.cascade_pos = 0;
    }

    fn insert(&mut self, entry: *mut TimerEntry) {
        let deadline = unsafe { (*entry).deadline_tick };
        let delta = deadline.saturating_sub(self.current_tick);
        let (level, index) = if delta < LVL0_SLOTS as u64 {
            (0u8, (deadline & ((LVL0_SLOTS as u64) - 1)) as usize)
        } else if delta < (1u64 << 14) {
            (1u8, ((deadline >> 8) & ((LVLN_SLOTS as u64) - 1)) as usize)
        } else if delta < (1u64 << 20) {
            (2u8, ((deadline >> 14) & ((LVLN_SLOTS as u64) - 1)) as usize)
        } else {
            (3u8, ((deadline >> 20) & ((LVLN_SLOTS as u64) - 1)) as usize)
        };

        unsafe {
            (*entry).bucket_level = level;
            (*entry).bucket_index = index as u16;
            (*entry).state = TimerState::Armed;
            match level {
                0 => {
                    self.lvl0[index].push_back(std::ptr::addr_of_mut!((*entry).link));
                    self.set_bucket_occupied(0, index);
                }
                1 => {
                    self.lvl1[index].push_back(std::ptr::addr_of_mut!((*entry).link));
                    self.set_bucket_occupied(1, index);
                }
                2 => {
                    self.lvl2[index].push_back(std::ptr::addr_of_mut!((*entry).link));
                    self.set_bucket_occupied(2, index);
                }
                _ => {
                    self.lvl3[index].push_back(std::ptr::addr_of_mut!((*entry).link));
                    self.set_bucket_occupied(3, index);
                }
            }
        }

        if self
            .next_deadline_tick
            .map(|tick| deadline < tick)
            .unwrap_or(true)
        {
            self.next_deadline_tick = Some(deadline);
        }
    }

    fn remove(&mut self, entry: *mut TimerEntry) {
        let level = unsafe { (*entry).bucket_level };
        if level == INVALID_BUCKET_LEVEL || level > 3 {
            return;
        }
        let index = unsafe { (*entry).bucket_index as usize };
        unsafe {
            let link = std::ptr::addr_of_mut!((*entry).link);
            match level {
                0 => self.lvl0[index].remove(link),
                1 => self.lvl1[index].remove(link),
                2 => self.lvl2[index].remove(link),
                3 => self.lvl3[index].remove(link),
                _ => {}
            }
            self.clear_bucket_if_empty(level, index);
            (*entry).bucket_level = INVALID_BUCKET_LEVEL;
            (*entry).bucket_index = 0;
        }
    }

    #[inline(always)]
    fn bits_mut(&mut self, level: u8) -> &mut u64 {
        match level {
            1 => &mut self.lvl1_bits,
            2 => &mut self.lvl2_bits,
            _ => &mut self.lvl3_bits,
        }
    }

    #[inline(always)]
    fn is_bucket_empty(&self, level: u8, index: usize) -> bool {
        unsafe {
            match level {
                0 => self.lvl0[index].front(TimerEntry::LINK_OFFSET).is_none(),
                1 => self.lvl1[index].front(TimerEntry::LINK_OFFSET).is_none(),
                2 => self.lvl2[index].front(TimerEntry::LINK_OFFSET).is_none(),
                _ => self.lvl3[index].front(TimerEntry::LINK_OFFSET).is_none(),
            }
        }
    }

    #[inline(always)]
    fn set_bucket_occupied(&mut self, level: u8, index: usize) {
        match level {
            0 => self.lvl0_bits[index / 64] |= 1u64 << (index % 64),
            _ => *self.bits_mut(level) |= 1u64 << index,
        }
    }

    #[inline(always)]
    fn clear_bucket_if_empty(&mut self, level: u8, index: usize) {
        if !self.is_bucket_empty(level, index) {
            return;
        }

        match level {
            0 => self.lvl0_bits[index / 64] &= !(1u64 << (index % 64)),
            _ => *self.bits_mut(level) &= !(1u64 << index),
        }
    }

    fn next_set_bit(bits: u64, start: usize) -> Option<usize> {
        if start >= 64 {
            return None;
        }
        let masked = bits & (!0u64 << start);
        if masked == 0 {
            None
        } else {
            Some(masked.trailing_zeros() as usize)
        }
    }

    fn next_nonempty_lvl0_bucket(&self) -> Option<usize> {
        let start = (self.current_tick & ((LVL0_SLOTS as u64) - 1)) as usize;
        let start_word = start / 64;
        let start_bit = start % 64;

        for word in start_word..self.lvl0_bits.len() {
            let bit = if word == start_word {
                Self::next_set_bit(self.lvl0_bits[word], start_bit)
            } else {
                Self::next_set_bit(self.lvl0_bits[word], 0)
            };
            if let Some(bit) = bit {
                return Some(word * 64 + bit);
            }
        }

        for word in 0..start_word {
            if let Some(bit) = Self::next_set_bit(self.lvl0_bits[word], 0) {
                return Some(word * 64 + bit);
            }
        }

        None
    }

    #[inline(always)]
    fn lvl0_bucket_occupied(&self, index: usize) -> bool {
        (self.lvl0_bits[index / 64] & (1u64 << (index % 64))) != 0
    }

    #[inline(always)]
    fn current_tick_has_occupied_cascade_bucket(&self) -> bool {
        if (self.current_tick & ((LVL0_SLOTS as u64) - 1)) != 0 {
            return false;
        }

        let idx1 = ((self.current_tick >> 8) & ((LVLN_SLOTS as u64) - 1)) as usize;
        if (self.lvl1_bits & (1u64 << idx1)) != 0 {
            return true;
        }

        if (self.current_tick & ((1u64 << 14) - 1)) != 0 {
            return false;
        }

        let idx2 = ((self.current_tick >> 14) & ((LVLN_SLOTS as u64) - 1)) as usize;
        if (self.lvl2_bits & (1u64 << idx2)) != 0 {
            return true;
        }

        if (self.current_tick & ((1u64 << 20) - 1)) != 0 {
            return false;
        }

        let idx3 = ((self.current_tick >> 20) & ((LVLN_SLOTS as u64) - 1)) as usize;
        (self.lvl3_bits & (1u64 << idx3)) != 0
    }

    #[inline(always)]
    fn round_up_to_tick_unit(tick: u64, shift: u32) -> Option<u64> {
        let mask = (1u64 << shift) - 1;
        if (tick & mask) == 0 {
            Some(tick)
        } else {
            tick.checked_add(mask).map(|value| value & !mask)
        }
    }

    fn next_occupied_cascade_tick(bits: u64, current_tick: u64, shift: u32) -> Option<u64> {
        if bits == 0 {
            return None;
        }

        let first_boundary = Self::round_up_to_tick_unit(current_tick, shift)?;
        let cycle = 1u64 << (shift + 6);
        let cycle_base = first_boundary & !(cycle - 1);
        let start = ((first_boundary >> shift) & ((LVLN_SLOTS as u64) - 1)) as usize;

        if let Some(index) = Self::next_set_bit(bits, start) {
            return cycle_base.checked_add((index as u64) << shift);
        }

        let index = Self::next_set_bit(bits, 0)?;
        cycle_base
            .checked_add(cycle)?
            .checked_add((index as u64) << shift)
    }

    fn next_upper_cascade_tick(&self) -> Option<u64> {
        Self::next_occupied_cascade_tick(self.lvl1_bits, self.current_tick, 8)
            .into_iter()
            .chain(Self::next_occupied_cascade_tick(
                self.lvl2_bits,
                self.current_tick,
                14,
            ))
            .chain(Self::next_occupied_cascade_tick(
                self.lvl3_bits,
                self.current_tick,
                20,
            ))
            .min()
    }

    fn next_collect_work_tick(&self, target_tick: u64) -> Option<u64> {
        self.level0_candidate_deadline()
            .map(|deadline| deadline.max(self.current_tick))
            .into_iter()
            .chain(self.next_upper_cascade_tick())
            .filter(|tick| *tick <= target_tick)
            .min()
    }

    fn skip_empty_ticks_until_next_work(&mut self, target_tick: u64) -> bool {
        if self.current_tick > target_tick
            || self.has_pending_cascade()
            || self.current_tick_has_occupied_cascade_bucket()
        {
            return false;
        }

        let idx = (self.current_tick & ((LVL0_SLOTS as u64) - 1)) as usize;
        if self.lvl0_bucket_occupied(idx) {
            return false;
        }

        let next_tick = self
            .next_collect_work_tick(target_tick)
            .or_else(|| target_tick.checked_add(1))
            .unwrap_or(target_tick);
        if next_tick <= self.current_tick {
            return false;
        }

        self.current_tick = next_tick;
        true
    }

    fn has_pending_cascade(&self) -> bool {
        self.cascade_pos < self.cascade_count
    }

    #[inline(always)]
    fn has_pending_entries(&self) -> bool {
        self.has_pending_cascade()
            || self.lvl0_bits.iter().any(|bits| *bits != 0)
            || self.lvl1_bits != 0
            || self.lvl2_bits != 0
            || self.lvl3_bits != 0
    }

    fn begin_tick_cascade(&mut self) {
        if self.has_pending_cascade() {
            return;
        }

        self.cascade_count = 0;
        self.cascade_pos = 0;

        if (self.current_tick & ((LVL0_SLOTS as u64) - 1)) == 0 {
            let idx1 = ((self.current_tick >> 8) & ((LVLN_SLOTS as u64) - 1)) as usize;
            self.cascade_levels[self.cascade_count as usize] = 1;
            self.cascade_indices[self.cascade_count as usize] = idx1;
            self.cascade_count += 1;

            if (self.current_tick & ((1u64 << 14) - 1)) == 0 {
                let idx2 = ((self.current_tick >> 14) & ((LVLN_SLOTS as u64) - 1)) as usize;
                self.cascade_levels[self.cascade_count as usize] = 2;
                self.cascade_indices[self.cascade_count as usize] = idx2;
                self.cascade_count += 1;

                if (self.current_tick & ((1u64 << 20) - 1)) == 0 {
                    let idx3 = ((self.current_tick >> 20) & ((LVLN_SLOTS as u64) - 1)) as usize;
                    self.cascade_levels[self.cascade_count as usize] = 3;
                    self.cascade_indices[self.cascade_count as usize] = idx3;
                    self.cascade_count += 1;
                }
            }
        }
    }

    fn process_cascade_with_budget(&mut self, budget: usize) -> usize {
        let mut consumed = 0usize;

        while self.has_pending_cascade() && consumed < budget {
            let pos = self.cascade_pos as usize;
            let level = self.cascade_levels[pos];
            let index = self.cascade_indices[pos];
            let entry_ptr = unsafe {
                match level {
                    1 => self.lvl1[index].pop_front(TimerEntry::LINK_OFFSET),
                    2 => self.lvl2[index].pop_front(TimerEntry::LINK_OFFSET),
                    _ => self.lvl3[index].pop_front(TimerEntry::LINK_OFFSET),
                }
            };
            let Some(entry_ptr) = entry_ptr else {
                self.cascade_pos += 1;
                continue;
            };
            self.clear_bucket_if_empty(level, index);
            unsafe {
                (*entry_ptr).bucket_level = INVALID_BUCKET_LEVEL;
                (*entry_ptr).bucket_index = 0;
            }
            self.insert(entry_ptr);
            consumed += 1;
        }

        while self.has_pending_cascade() {
            let pos = self.cascade_pos as usize;
            let level = self.cascade_levels[pos];
            let index = self.cascade_indices[pos];
            let has_more = unsafe {
                match level {
                    1 => self.lvl1[index].front(TimerEntry::LINK_OFFSET).is_some(),
                    2 => self.lvl2[index].front(TimerEntry::LINK_OFFSET).is_some(),
                    _ => self.lvl3[index].front(TimerEntry::LINK_OFFSET).is_some(),
                }
            };
            if has_more {
                break;
            }
            self.cascade_pos += 1;
        }

        if !self.has_pending_cascade() {
            self.cascade_count = 0;
            self.cascade_pos = 0;
        }

        consumed
    }

    // Recompute the global nearest deadline from the exact level-0 candidate
    // plus the next upper-level cascade boundary. Level 1+ buckets are coarse
    // and unordered, so their front entry may be later than another entry in
    // the same bucket. A cascade-boundary wake can be up to one descent early
    // per upper level, but it is bounded and avoids a late timer fire.
    fn recompute_next_deadline(&mut self) {
        self.next_deadline_tick = self
            .level0_candidate_deadline()
            .into_iter()
            .chain(self.next_upper_cascade_tick())
            .min();
    }

    fn level0_candidate_deadline(&self) -> Option<u64> {
        let index = self.next_nonempty_lvl0_bucket()?;
        let entry = unsafe { self.lvl0[index].front(TimerEntry::LINK_OFFSET)? };
        Some(unsafe { (*entry).deadline_tick })
    }
}

macro_rules! define_timer_runtime {
    ($vis:vis) => {
        /// Executor-owned timer subsystem.
        ///
        /// Exposed publicly only by the dev-only `test-support` feature for
        /// benchmark probes. Applications should use the free timer helpers.
        $vis struct TimerRuntime {
            /// Pool of runtime-owned timer entries.
            timer_pool: ManuallyDrop<ProviderOwnedPool<TimerEntry, BasicMemoryProvider>>,
            /// Hierarchical timing wheel used to organize deadlines.
            wheel: TimerWheel,
            /// Per-pass paired clock sample used only for absolute deadlines.
            absolute_arm_base: Option<ArmBase>,
            /// Stable owner containing this timer runtime, or null in unit tests.
            owner: *const ExecutorOwner,
        }
    };
}

#[cfg(feature = "test-support")]
define_timer_runtime!(pub);
#[cfg(not(feature = "test-support"))]
define_timer_runtime!(pub(crate));

#[derive(Clone, Copy)]
struct ArmBase {
    /// `Instant` half of the paired absolute-deadline clock sample.
    instant: Instant,
    /// Timer-wheel tick corresponding to `instant`.
    tick: u64,
}

impl TimerRuntime {
    #[allow(clippy::new_without_default)]
    /// Creates a timer runtime in an uninitialized state.
    ///
    /// Call [`TimerRuntime::init`] after moving it to its final memory
    /// location.
    pub fn new() -> io::Result<Self> {
        Ok(Self {
            timer_pool: ManuallyDrop::new(
                ProviderOwnedPool::new(BasicMemoryProvider::new(), TIMERS_PER_SLAB)
                    .map_err(|_| io::Error::from(io::ErrorKind::InvalidInput))?,
            ),
            wheel: TimerWheel::new_uninit(),
            absolute_arm_base: None,
            owner: std::ptr::null(),
        })
    }

    /// Initializes the timer pool and timing wheel.
    pub fn init(&mut self) -> io::Result<()> {
        self.timer_pool.init();
        self.wheel.init()?;
        self.absolute_arm_base = None;
        Ok(())
    }

    pub(crate) fn bind_owner(&mut self, owner: *const ExecutorOwner) {
        self.owner = owner;
    }

    /// Samples and returns the current monotonic timer tick.
    pub fn now_tick(&mut self) -> io::Result<u64> {
        now_tick()
    }

    #[inline(always)]
    /// Clears the cached absolute-deadline sample for a new executor pass.
    pub(crate) fn begin_executor_pass(&mut self) {
        self.absolute_arm_base = None;
    }

    fn submit_sleep_at_tick(
        &mut self,
        task: *mut TaskHeader,
        deadline_tick: u64,
    ) -> io::Result<*mut TimerEntry> {
        #[cfg(any(debug_assertions, feature = "test-support"))]
        if crate::runtime::test_hooks::take_timer_alloc_failure() {
            return Err(io::Error::from(io::ErrorKind::OutOfMemory));
        }

        let entry = unsafe {
            match self.timer_pool.alloc(()) {
                Some(entry) => entry,
                None => {
                    return Err(io::Error::from(io::ErrorKind::OutOfMemory));
                }
            }
        };

        unsafe {
            (*entry).link = Link::new_unlinked();
            (*entry).clear_waiter();
            (*entry).owner = if self.owner.is_null() {
                None
            } else {
                Some(ExecutorOwner::clone_rc(self.owner))
            };
            (*entry).register_waiter(task);
            (*entry).deadline_tick = deadline_tick;
            (*entry).state = TimerState::Armed;
        }
        self.wheel.insert(entry);
        Ok(entry)
    }

    fn sample_arm_tick(&mut self) -> io::Result<u64> {
        let tick = now_tick()?;

        // When the wheel is empty, its current tick may be arbitrarily stale
        // after a long timer-free idle period. Snap it forward so newly armed
        // sleeps do not start from an old baseline and later timer processing
        // does not need to burn budget catching up empty ticks.
        if !self.has_pending() {
            self.wheel.current_tick = tick;
        }

        Ok(tick)
    }

    fn absolute_arm_base(&mut self) -> io::Result<ArmBase> {
        if let Some(base) = self.absolute_arm_base {
            return Ok(base);
        }

        // Absolute deadlines convert against one paired sample per executor
        // pass. Relative durations sample their own arm tick instead.
        let instant = Instant::now();
        let tick = self.sample_arm_tick()?;

        let base = ArmBase { instant, tick };
        self.absolute_arm_base = Some(base);
        Ok(base)
    }

    fn deadline_tick_for_duration(&mut self, duration: Duration) -> io::Result<u64> {
        let ticks = duration_to_ticks(duration);
        let arm_tick = self.sample_arm_tick()?;
        Ok(arm_tick.saturating_add(ticks))
    }

    fn deadline_tick_for_instant(&mut self, deadline: Instant) -> io::Result<Option<u64>> {
        let base = self.absolute_arm_base()?;
        if deadline <= base.instant {
            return Ok(None);
        }

        let delta = deadline.duration_since(base.instant);
        Ok(Some(base.tick.saturating_add(duration_to_ticks(delta))))
    }

    pub(crate) fn submit_sleep_duration(
        &mut self,
        task: *mut TaskHeader,
        duration: Duration,
    ) -> io::Result<*mut TimerEntry> {
        let deadline_tick = self.deadline_tick_for_duration(duration)?;
        self.submit_sleep_at_tick(task, deadline_tick)
    }

    pub(crate) fn submit_sleep_deadline(
        &mut self,
        task: *mut TaskHeader,
        deadline: Instant,
    ) -> io::Result<Option<*mut TimerEntry>> {
        let Some(deadline_tick) = self.deadline_tick_for_instant(deadline)? else {
            return Ok(None);
        };
        self.submit_sleep_at_tick(task, deadline_tick).map(Some)
    }

    pub(crate) fn cancel_sleep(&mut self, entry: *mut TimerEntry) -> io::Result<()> {
        if entry.is_null() {
            return Ok(());
        }

        if unsafe { (*entry).state } == TimerState::Armed {
            let deadline_tick = unsafe { (*entry).deadline_tick };
            self.wheel.remove(entry);
            // Upper-level cached deadlines are cascade boundaries, not entry
            // deadlines. Missing an exact match after cancellation can leave
            // only an earlier stale boundary, which forces an early recompute
            // rather than delaying another timer.
            if self.wheel.next_deadline_tick == Some(deadline_tick) {
                self.wheel.next_deadline_dirty = true;
            }
        }
        unsafe {
            (*entry).state = TimerState::Idle;
            (*entry).clear_waiter();
            self.timer_pool.free(entry);
        }
        Ok(())
    }

    unsafe fn free_fired_sleep(&mut self, entry: *mut TimerEntry) {
        unsafe {
            (*entry).state = TimerState::Idle;
            (*entry).clear_waiter();
            self.timer_pool.free(entry);
        }
    }

    /// Expires timers up to `now`, respecting the provided per-pass budget.
    ///
    /// Returns `true` when timer work remains pending for a later pass.
    pub fn process_at_with_budget(&mut self, now: u64, budget: usize) -> io::Result<bool> {
        if now >= self.wheel.current_tick {
            return Ok(self.collect_expired(now, budget));
        }
        Ok(false)
    }

    /// Returns `true` if any timer is currently armed.
    pub fn has_pending(&mut self) -> bool {
        self.wheel.has_pending_entries()
    }

    /// Returns the duration until the next timer deadline, if any.
    pub fn next_wait_duration(&mut self, now_tick: u64) -> Option<Duration> {
        if self.wheel.next_deadline_dirty {
            self.wheel.recompute_next_deadline();
            self.wheel.next_deadline_dirty = false;
        }
        let deadline_tick = self.wheel.next_deadline_tick?;
        Some(tick_to_duration(deadline_tick.saturating_sub(now_tick)))
    }

    // Expire due timers directly into the executor's main ready queue. The
    // phase budget covers both cascade reinsertion and final expiry so timer
    // maintenance cannot monopolize the executor loop.
    fn collect_expired(&mut self, target_tick: u64, budget: usize) -> bool {
        let schedule_ctx = unsafe { schedule_ctx_unchecked() };
        let wake_epoch = unsafe { next_timer_wake_epoch_unchecked(schedule_ctx) };
        let mut remaining_budget = budget;
        while self.wheel.current_tick <= target_tick {
            if self.wheel.skip_empty_ticks_until_next_work(target_tick) {
                continue;
            }

            self.wheel.begin_tick_cascade();
            if self.wheel.has_pending_cascade() {
                if remaining_budget == 0 {
                    self.wheel.next_deadline_dirty = true;
                    return true;
                }

                let consumed = self.wheel.process_cascade_with_budget(remaining_budget);
                remaining_budget -= consumed;
                if self.wheel.has_pending_cascade() {
                    self.wheel.next_deadline_dirty = true;
                    return true;
                }
            }

            let idx = (self.wheel.current_tick & ((LVL0_SLOTS as u64) - 1)) as usize;
            while let Some(entry_ptr) =
                unsafe { self.wheel.lvl0[idx].pop_front(TimerEntry::LINK_OFFSET) }
            {
                self.wheel.clear_bucket_if_empty(0, idx);
                if remaining_budget == 0 {
                    unsafe {
                        self.wheel.lvl0[idx]
                            .push_front_unchecked(std::ptr::addr_of_mut!((*entry_ptr).link));
                    }
                    self.wheel.set_bucket_occupied(0, idx);
                    self.wheel.next_deadline_dirty = true;
                    return true;
                }

                unsafe {
                    (*entry_ptr).bucket_level = INVALID_BUCKET_LEVEL;
                    (*entry_ptr).bucket_index = 0;
                    (*entry_ptr).state = TimerState::Fired;
                    let waiter = (*entry_ptr).take_waiter();
                    if !waiter.is_null() {
                        note_timer_expired();
                        note_waiter_wake();
                        schedule_timer_woken_task_unchecked(
                            waiter,
                            schedule_ctx.ready_queue,
                            schedule_ctx.runtime_state,
                            wake_epoch,
                        );
                        release_task(waiter);
                    }
                }
                remaining_budget -= 1;
            }
            if self.wheel.current_tick == u64::MAX {
                break;
            }
            self.wheel.current_tick = self.wheel.current_tick.saturating_add(1);
        }
        self.wheel.next_deadline_dirty = true;
        false
    }

    fn free_wheel_entries_for_drop(&mut self) {
        let timer_pool = &mut *self.timer_pool;
        for index in 0..LVL0_SLOTS {
            if (self.wheel.lvl0_bits[index / 64] & (1u64 << (index % 64))) != 0 {
                free_timer_bucket_entries(&mut self.wheel.lvl0[index], timer_pool);
            }
        }
        for index in 0..LVLN_SLOTS {
            if (self.wheel.lvl1_bits & (1u64 << index)) != 0 {
                free_timer_bucket_entries(&mut self.wheel.lvl1[index], timer_pool);
            }
        }
        for index in 0..LVLN_SLOTS {
            if (self.wheel.lvl2_bits & (1u64 << index)) != 0 {
                free_timer_bucket_entries(&mut self.wheel.lvl2[index], timer_pool);
            }
        }
        for index in 0..LVLN_SLOTS {
            if (self.wheel.lvl3_bits & (1u64 << index)) != 0 {
                free_timer_bucket_entries(&mut self.wheel.lvl3[index], timer_pool);
            }
        }
    }

    pub(crate) fn cancel_all_for_shutdown(&mut self) {
        for bucket in &mut self.wheel.lvl0 {
            cancel_timer_bucket_entries(bucket);
        }
        for bucket in &mut self.wheel.lvl1 {
            cancel_timer_bucket_entries(bucket);
        }
        for bucket in &mut self.wheel.lvl2 {
            cancel_timer_bucket_entries(bucket);
        }
        for bucket in &mut self.wheel.lvl3 {
            cancel_timer_bucket_entries(bucket);
        }
        self.wheel.unlink_all_for_drop();
        self.absolute_arm_base = None;
    }
}

impl Drop for TimerRuntime {
    fn drop(&mut self) {
        unsafe {
            self.free_wheel_entries_for_drop();
            self.wheel.unlink_all_for_drop();
            ManuallyDrop::drop(&mut self.timer_pool);
        }
    }
}

fn free_timer_bucket_entries(
    bucket: &mut DList<TimerEntry>,
    timer_pool: &mut Pool<'static, TimerEntry, BasicMemoryProvider>,
) {
    unsafe {
        bucket.drain_all_for_drop(TimerEntry::LINK_OFFSET, |entry| {
            (*entry).state = TimerState::Idle;
            (*entry).bucket_level = INVALID_BUCKET_LEVEL;
            (*entry).bucket_index = 0;
            (*entry).clear_waiter();
            timer_pool.free(entry);
        });
    }
}

fn cancel_timer_bucket_entries(bucket: &mut DList<TimerEntry>) {
    unsafe {
        bucket.drain_all_for_drop(TimerEntry::LINK_OFFSET, |entry| {
            (*entry).state = TimerState::Cancelled;
            (*entry).bucket_level = INVALID_BUCKET_LEVEL;
            (*entry).bucket_index = 0;
            (*entry).clear_waiter();
        });
    }
}

fn duration_to_ticks(duration: Duration) -> u64 {
    let nanos = duration.as_nanos();
    let tick_ns = TIMER_TICK_NS as u128;
    let ticks = nanos / tick_ns;
    let remainder = nanos % tick_ns;
    let rounded = if remainder == 0 {
        ticks
    } else {
        ticks.saturating_add(1)
    };

    if rounded > u64::MAX as u128 {
        u64::MAX
    } else {
        rounded as u64
    }
}

fn tick_to_duration(ticks: u64) -> Duration {
    if ticks == 0 {
        Duration::ZERO
    } else {
        Duration::from_nanos(ticks.saturating_mul(TIMER_TICK_NS))
    }
}

fn now_tick() -> io::Result<u64> {
    note_timer_now_tick_call();
    let mut ts = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    // SAFETY: `ts` points to writable timespec storage for the duration of the
    // libc call, and CLOCK_MONOTONIC requires no additional ownership.
    let rc = unsafe { libc::clock_gettime(libc::CLOCK_MONOTONIC, &mut ts) };
    if rc < 0 {
        return Err(io::Error::last_os_error());
    }

    Ok(((ts.tv_sec as u64) * 1_000_000_000u64 + (ts.tv_nsec as u64)) / TIMER_TICK_NS)
}

#[inline(always)]
/// Replaces an armed timer's waiter with the task currently being polled.
///
/// # Safety
///
/// `entry` must point to a live timer entry owned by the active executor, and
/// this function must run during the poll of the future that owns that entry.
unsafe fn refresh_sleep_waiter(entry: *mut TimerEntry, pctx: &PollCtx) {
    debug_assert!(
        !entry.is_null(),
        "cannot refresh waiter for a missing timer entry"
    );
    if entry.is_null() {
        return;
    }
    unsafe {
        (*entry).register_waiter(pctx.owner_task());
    }
}

/// Returns the timer runtime recorded by an allocated entry and keeps its owner
/// alive until the caller finishes reclamation.
unsafe fn timer_runtime_for_entry(
    entry: *mut TimerEntry,
) -> (Option<Rc<ExecutorOwner>>, *mut TimerRuntime) {
    let owner = unsafe { (*entry).owner.clone() };
    let timers = owner
        .as_ref()
        .map_or(std::ptr::null_mut(), |owner| owner.timers_ptr());
    (owner, timers)
}

/// One-shot sleep future scheduled by the runtime timer wheel.
///
/// Constructed by [`sleep`] or [`sleep_until`].
///
/// Timer entries come from the executor-owned pool when first polled. Prefer
/// one deadline around a larger protocol phase instead of wrapping every
/// data-path read/write step when the same timeout semantics can be preserved.
///
/// The future must be polled inside [`crate::runtime::executor::Executor::run`]
/// on the thread that owns that executor. Polling outside that run or through
/// another executor's task waker cancels the timer and returns
/// [`io::ErrorKind::NotConnected`].
///
/// # Example
/// ```no_run
/// use flowio::runtime::executor::Executor;
/// use flowio::runtime::timer::{sleep, Sleep};
/// use std::time::Duration;
///
/// let mut executor = Executor::new()?;
/// executor.run(async {
///     let timer: Sleep = sleep(Duration::from_millis(1));
///     timer.await.unwrap();
/// })?;
/// # Ok::<(), std::io::Error>(())
/// ```
pub struct Sleep {
    /// Relative duration to arm on first poll, if this is a duration-based sleep.
    duration: Option<Duration>,
    /// Absolute deadline to arm on first poll, if this is a deadline-based sleep.
    deadline: Option<Instant>,
    /// Timer entry currently owned by this future, if it has been armed.
    entry: *mut TimerEntry,
}

impl Sleep {
    /// Creates a sleep that will arm from a relative duration on first poll.
    pub(crate) fn new_duration(duration: Duration) -> Self {
        Self {
            duration: Some(duration),
            deadline: None,
            entry: std::ptr::null_mut(),
        }
    }

    /// Creates a sleep that will arm from an absolute deadline on first poll.
    pub(crate) fn new_deadline(deadline: Instant) -> Self {
        Self {
            duration: None,
            deadline: Some(deadline),
            entry: std::ptr::null_mut(),
        }
    }
}

/// Error returned when a timeout expires or its runtime timer fails.
///
/// # Example
/// ```
/// use flowio::runtime::timer::TimeoutError;
///
/// let elapsed = TimeoutError::Elapsed;
/// assert_eq!(elapsed.to_string(), "runtime timer elapsed");
/// ```
#[derive(Debug)]
pub enum TimeoutError {
    /// The configured deadline elapsed before the wrapped future completed.
    Elapsed,
    /// The runtime could not arm or drive the deadline timer.
    ///
    /// The contained [`io::Error`] preserves the original runtime failure,
    /// including resource-pressure errors such as
    /// [`io::ErrorKind::OutOfMemory`].
    Runtime(io::Error),
}

impl fmt::Display for TimeoutError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Elapsed => f.write_str("runtime timer elapsed"),
            Self::Runtime(err) => write!(f, "runtime timer failed: {err}"),
        }
    }
}

impl std::error::Error for TimeoutError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Elapsed => None,
            Self::Runtime(err) => Some(err),
        }
    }
}

impl Future for Sleep {
    type Output = io::Result<()>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = unsafe { self.get_unchecked_mut() };

        let pctx = match poll_ctx_from_waker(cx) {
            Ok(pctx) => pctx,
            Err(err) => {
                if !this.entry.is_null() {
                    let (owner, timers) = unsafe { timer_runtime_for_entry(this.entry) };
                    if !timers.is_null() {
                        let _ = unsafe { &mut *timers }.cancel_sleep(this.entry);
                    }
                    this.entry = std::ptr::null_mut();
                    drop(owner);
                }
                return Poll::Ready(Err(err));
            }
        };

        if !this.entry.is_null() && unsafe { (*this.entry).owner_ptr() } != pctx.owner_ptr() {
            let (owner, timers) = unsafe { timer_runtime_for_entry(this.entry) };
            if !timers.is_null() {
                let _ = unsafe { &mut *timers }.cancel_sleep(this.entry);
            }
            this.entry = std::ptr::null_mut();
            drop(owner);
            return Poll::Ready(Err(io::Error::from(io::ErrorKind::NotConnected)));
        }

        if !this.entry.is_null() {
            let state = unsafe { (*this.entry).state };
            if state == TimerState::Fired {
                let (owner, timers) = unsafe { timer_runtime_for_entry(this.entry) };
                debug_assert!(!timers.is_null());
                unsafe {
                    (*timers).free_fired_sleep(this.entry);
                }
                this.entry = std::ptr::null_mut();
                drop(owner);
                return Poll::Ready(Ok(()));
            }
            if state == TimerState::Cancelled {
                let (owner, timers) = unsafe { timer_runtime_for_entry(this.entry) };
                debug_assert!(!timers.is_null());
                unsafe {
                    (*timers).free_fired_sleep(this.entry);
                }
                this.entry = std::ptr::null_mut();
                drop(owner);
                return Poll::Ready(Err(io::Error::from(io::ErrorKind::BrokenPipe)));
            }
            unsafe { refresh_sleep_waiter(this.entry, &pctx) };
            return Poll::Pending;
        }

        let timers = pctx.timers();

        if let Some(duration) = this.duration.take() {
            if duration == Duration::ZERO {
                return Poll::Ready(Ok(()));
            }

            let entry =
                unsafe { &mut *timers }.submit_sleep_duration(pctx.owner_task(), duration)?;
            this.entry = entry;
            return Poll::Pending;
        }

        let Some(deadline) = this.deadline.take() else {
            return Poll::Ready(Ok(()));
        };

        match unsafe { &mut *timers }.submit_sleep_deadline(pctx.owner_task(), deadline)? {
            Some(entry) => {
                this.entry = entry;
                Poll::Pending
            }
            None => Poll::Ready(Ok(())),
        }
    }
}

impl Drop for Sleep {
    fn drop(&mut self) {
        if !self.entry.is_null() {
            let (owner, timers) = unsafe { timer_runtime_for_entry(self.entry) };
            if !timers.is_null() {
                let _ = unsafe { &mut *timers }.cancel_sleep(self.entry);
            }
            self.entry = std::ptr::null_mut();
            drop(owner);
        }
    }
}

/// Sleeps for at least the provided duration.
///
/// A zero duration completes immediately without arming the timer wheel.
///
/// The returned future must be polled inside an active
/// [`crate::runtime::executor::Executor::run`].
///
/// # Example
/// ```no_run
/// use flowio::runtime::executor::Executor;
/// use flowio::runtime::timer::sleep;
/// use std::time::Duration;
///
/// let mut executor = Executor::new()?;
/// executor.run(async {
///     sleep(Duration::from_millis(10)).await.unwrap();
/// })?;
/// # Ok::<(), std::io::Error>(())
/// ```
pub fn sleep(duration: Duration) -> Sleep {
    Sleep::new_duration(duration)
}

/// Sleeps until the provided monotonic deadline.
///
/// The returned future must be polled inside an active
/// [`crate::runtime::executor::Executor::run`].
///
/// # Example
/// ```no_run
/// use flowio::runtime::executor::Executor;
/// use flowio::runtime::timer::sleep_until;
/// use std::time::{Duration, Instant};
///
/// let mut executor = Executor::new()?;
/// executor.run(async {
///     let deadline = Instant::now() + Duration::from_millis(10);
///     sleep_until(deadline).await.unwrap();
/// })?;
/// # Ok::<(), std::io::Error>(())
/// ```
pub fn sleep_until(deadline: Instant) -> Sleep {
    Sleep::new_deadline(deadline)
}

/// Future returned by [`timeout`] and [`timeout_at`].
///
/// This is a runtime deadline wrapper, not a transport I/O primitive. It arms
/// one pooled timer entry if the wrapped future does not complete first.
///
/// The wrapper must be polled inside an active
/// [`crate::runtime::executor::Executor::run`]. [`TimeoutError::Elapsed`]
/// reports deadline expiry, while [`TimeoutError::Runtime`] preserves timer
/// allocation and runtime failures.
///
/// # Example
/// ```no_run
/// use flowio::runtime::executor::Executor;
/// use flowio::runtime::timer::{sleep, timeout, TimeoutError};
/// use std::time::Duration;
///
/// let mut executor = Executor::new()?;
/// executor.run(async {
///     let result: Result<std::io::Result<()>, TimeoutError> =
///         timeout(Duration::from_millis(10), sleep(Duration::from_millis(1))).await;
///     assert!(result.is_ok());
/// })?;
/// # Ok::<(), std::io::Error>(())
/// ```
pub struct Timeout<F> {
    /// User future being raced against the timer.
    future: F,
    /// Sleep future that enforces the deadline.
    sleep: Sleep,
}

impl<F> Timeout<F> {
    fn new_duration(duration: Duration, future: F) -> Self {
        Self {
            future,
            sleep: Sleep::new_duration(duration),
        }
    }

    fn new_deadline(deadline: Instant, future: F) -> Self {
        Self {
            future,
            sleep: Sleep::new_deadline(deadline),
        }
    }
}

impl<F: Future> Future for Timeout<F> {
    type Output = Result<F::Output, TimeoutError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = unsafe { self.get_unchecked_mut() };

        let mut future = unsafe { Pin::new_unchecked(&mut this.future) };
        if let Poll::Ready(output) = future.as_mut().poll(cx) {
            return Poll::Ready(Ok(output));
        }

        match Pin::new(&mut this.sleep).poll(cx) {
            Poll::Ready(Ok(())) => Poll::Ready(Err(TimeoutError::Elapsed)),
            Poll::Ready(Err(err)) => Poll::Ready(Err(TimeoutError::Runtime(err))),
            Poll::Pending => Poll::Pending,
        }
    }
}

/// Runs a future with a relative timeout.
///
/// Prefer this around a larger operation or protocol phase when one relative
/// budget applies. Avoid creating one wrapper per tiny I/O step when a shared
/// phase deadline has the same semantics.
/// Returns [`TimeoutError::Elapsed`] when the deadline wins and
/// [`TimeoutError::Runtime`] when the timer cannot be armed or driven.
/// The returned wrapper must be polled inside an active
/// [`crate::runtime::executor::Executor::run`].
///
/// # Example
/// ```no_run
/// use flowio::runtime::executor::Executor;
/// use flowio::runtime::timer::{sleep, timeout};
/// use std::time::Duration;
///
/// let mut executor = Executor::new()?;
/// executor.run(async {
///     let result = timeout(Duration::from_millis(10), async {
///         sleep(Duration::from_millis(1)).await
///     })
///     .await;
///     let _ = result;
/// })?;
/// # Ok::<(), std::io::Error>(())
/// ```
pub fn timeout<F: Future>(duration: Duration, future: F) -> Timeout<F> {
    Timeout::new_duration(duration, future)
}

/// Runs a future with an absolute monotonic deadline.
///
/// Prefer this when several operations share one precomputed absolute
/// deadline. Avoid creating a separate timer per tiny I/O step when that shared
/// phase deadline has the same semantics.
/// Returns [`TimeoutError::Elapsed`] when the deadline wins and
/// [`TimeoutError::Runtime`] when the timer cannot be armed or driven.
/// The returned wrapper must be polled inside an active
/// [`crate::runtime::executor::Executor::run`].
///
/// # Example
/// ```no_run
/// use flowio::runtime::executor::Executor;
/// use flowio::runtime::timer::{sleep, timeout_at};
/// use std::time::{Duration, Instant};
///
/// let mut executor = Executor::new()?;
/// executor.run(async {
///     let result = timeout_at(Instant::now() + Duration::from_millis(10), async {
///         sleep(Duration::from_millis(1)).await
///     })
///     .await;
///     let _ = result;
/// })?;
/// # Ok::<(), std::io::Error>(())
/// ```
pub fn timeout_at<F: Future>(deadline: Instant, future: F) -> Timeout<F> {
    Timeout::new_deadline(deadline, future)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn timer_waiter_reference_pairing_is_exact() {
        let first = TaskHeader::new();
        let second = TaskHeader::new();
        let first_ptr = &first as *const TaskHeader as *mut TaskHeader;
        let second_ptr = &second as *const TaskHeader as *mut TaskHeader;
        let mut entry = TimerEntry::new();

        unsafe { entry.register_waiter(first_ptr) };
        assert_eq!(first.refs.get(), 2);

        unsafe { entry.register_waiter(first_ptr) };
        assert_eq!(first.refs.get(), 2, "same timer waiter retained twice");

        unsafe { entry.register_waiter(second_ptr) };
        assert_eq!(first.refs.get(), 1, "replaced timer waiter leaked");
        assert_eq!(second.refs.get(), 2);

        let transferred = entry.take_waiter();
        assert_eq!(transferred, second_ptr);
        assert!(entry.waiter.is_null());
        assert_eq!(second.refs.get(), 2, "taking timer waiter released it");
        unsafe { release_task(transferred) };
        assert_eq!(second.refs.get(), 1);

        unsafe { entry.register_waiter(first_ptr) };
        entry.clear_waiter();
        assert_eq!(first.refs.get(), 1, "clearing timer waiter leaked it");
    }

    fn init_wheel_at(wheel: &mut TimerWheel, current_tick: u64) {
        wheel.init().expect("timer wheel init failed");
        wheel.current_tick = current_tick;
        wheel.next_deadline_tick = None;
        wheel.next_deadline_dirty = false;
    }

    fn timer_entry_at(deadline_tick: u64) -> TimerEntry {
        let mut entry = TimerEntry::new();
        entry.deadline_tick = deadline_tick;
        entry
    }

    #[test]
    fn duration_to_ticks_rounds_up_without_wrapping() {
        assert_eq!(duration_to_ticks(Duration::ZERO), 0);
        assert_eq!(duration_to_ticks(Duration::from_nanos(1)), 1);
        assert_eq!(duration_to_ticks(Duration::from_nanos(TIMER_TICK_NS)), 1);
        assert_eq!(
            duration_to_ticks(Duration::from_nanos(TIMER_TICK_NS + 1)),
            2
        );
        assert_eq!(duration_to_ticks(Duration::from_secs(u64::MAX)), u64::MAX);
    }

    #[test]
    fn absolute_deadline_conversion_preserves_paired_base_and_rounding() {
        let mut runtime = TimerRuntime::new().expect("timer runtime construction failed");
        runtime.init().expect("timer runtime init failed");
        let instant = Instant::now();
        runtime.absolute_arm_base = Some(ArmBase { instant, tick: 41 });

        assert_eq!(
            runtime
                .deadline_tick_for_instant(instant)
                .expect("deadline conversion failed"),
            None
        );
        assert_eq!(
            runtime
                .deadline_tick_for_instant(instant + Duration::from_nanos(1))
                .expect("deadline conversion failed"),
            Some(42)
        );
        assert_eq!(
            runtime
                .deadline_tick_for_instant(instant + Duration::from_nanos(TIMER_TICK_NS))
                .expect("deadline conversion failed"),
            Some(42)
        );
        assert_eq!(
            runtime
                .deadline_tick_for_instant(
                    instant + Duration::from_nanos(TIMER_TICK_NS.saturating_add(1)),
                )
                .expect("deadline conversion failed"),
            Some(43)
        );
    }

    #[test]
    fn timer_wheel_remove_clears_bucket_ownership_and_deadline_cache() {
        let mut wheel = TimerWheel::new_uninit();
        init_wheel_at(&mut wheel, 100);
        let mut entry = timer_entry_at(105);
        let entry_ptr = &mut entry as *mut TimerEntry;

        wheel.insert(entry_ptr);
        assert_eq!(wheel.next_deadline_tick, Some(105));
        assert_eq!(wheel.level0_candidate_deadline(), Some(105));
        assert_eq!(entry.bucket_level, 0);

        wheel.remove(entry_ptr);
        wheel.next_deadline_dirty = true;
        wheel.recompute_next_deadline();
        wheel.next_deadline_dirty = false;

        assert!(entry.link.is_unlinked());
        assert_eq!(entry.bucket_level, INVALID_BUCKET_LEVEL);
        assert_eq!(entry.bucket_index, 0);
        assert_eq!(wheel.next_deadline_tick, None);
        assert_eq!(wheel.level0_candidate_deadline(), None);
    }

    #[test]
    fn timer_wheel_remove_is_idempotent_for_unlinked_entries() {
        let mut wheel = TimerWheel::new_uninit();
        init_wheel_at(&mut wheel, 100);
        let mut entry = timer_entry_at(105);
        let entry_ptr = &mut entry as *mut TimerEntry;

        wheel.remove(entry_ptr);
        assert!(entry.link.is_unlinked());
        assert_eq!(entry.bucket_level, INVALID_BUCKET_LEVEL);
        assert_eq!(entry.bucket_index, 0);

        wheel.insert(entry_ptr);
        wheel.remove(entry_ptr);
        wheel.remove(entry_ptr);
        wheel.next_deadline_dirty = true;
        wheel.recompute_next_deadline();
        wheel.next_deadline_dirty = false;

        assert!(entry.link.is_unlinked());
        assert_eq!(entry.bucket_level, INVALID_BUCKET_LEVEL);
        assert_eq!(entry.bucket_index, 0);
        assert_eq!(wheel.next_deadline_tick, None);
        assert_eq!(wheel.level0_candidate_deadline(), None);
    }

    #[test]
    fn timer_wheel_pending_entries_uses_occupancy_not_deadline_cache() {
        let mut wheel = TimerWheel::new_uninit();
        init_wheel_at(&mut wheel, 100);
        let mut removed_entry = timer_entry_at(105);
        let removed_ptr = &mut removed_entry as *mut TimerEntry;
        let mut remaining_entry = timer_entry_at(120);
        let remaining_ptr = &mut remaining_entry as *mut TimerEntry;

        wheel.insert(removed_ptr);
        wheel.insert(remaining_ptr);
        assert!(wheel.has_pending_entries());
        assert_eq!(wheel.next_deadline_tick, Some(105));

        wheel.remove(removed_ptr);
        wheel.next_deadline_dirty = true;
        assert!(wheel.has_pending_entries());
        assert_eq!(wheel.next_deadline_tick, Some(105));
        assert!(wheel.next_deadline_dirty);

        wheel.remove(remaining_ptr);
        assert!(!wheel.has_pending_entries());
        assert_eq!(wheel.next_deadline_tick, Some(105));
        assert!(wheel.next_deadline_dirty);
    }

    #[test]
    fn timer_wheel_level0_candidate_deadline_ignores_upper_cascade_buckets() {
        let mut wheel = TimerWheel::new_uninit();
        init_wheel_at(&mut wheel, 0);
        let deadline = LVL0_SLOTS as u64;
        let mut entry = timer_entry_at(deadline);
        let entry_ptr = &mut entry as *mut TimerEntry;

        wheel.insert(entry_ptr);

        assert_eq!(entry.bucket_level, 1);
        assert_eq!(wheel.level0_candidate_deadline(), None);
        assert_eq!(wheel.next_upper_cascade_tick(), Some(deadline));

        wheel.remove(entry_ptr);
    }

    #[test]
    fn timer_wheel_recompute_uses_upper_cascade_boundary_not_bucket_front() {
        let mut wheel = TimerWheel::new_uninit();
        init_wheel_at(&mut wheel, 0);
        let bucket_base = 2u64 << 14;
        let mut later_entry = timer_entry_at(bucket_base + 10_000);
        let mut earlier_entry = timer_entry_at(bucket_base + 100);
        let later_ptr = &mut later_entry as *mut TimerEntry;
        let earlier_ptr = &mut earlier_entry as *mut TimerEntry;

        wheel.insert(later_ptr);
        wheel.insert(earlier_ptr);
        assert_eq!(later_entry.bucket_level, 2);
        assert_eq!(earlier_entry.bucket_level, 2);
        assert_eq!(later_entry.bucket_index, earlier_entry.bucket_index);

        wheel.next_deadline_dirty = true;
        wheel.recompute_next_deadline();
        wheel.next_deadline_dirty = false;

        assert_eq!(wheel.next_deadline_tick, Some(bucket_base));
        assert!(wheel.next_deadline_tick.unwrap() <= earlier_entry.deadline_tick);

        wheel.remove(later_ptr);
        wheel.remove(earlier_ptr);
    }

    #[test]
    fn timer_wheel_cascade_budget_preserves_pending_work() {
        let mut wheel = TimerWheel::new_uninit();
        init_wheel_at(&mut wheel, 0);
        let deadline = LVL0_SLOTS as u64;
        let mut entry = timer_entry_at(deadline);
        let entry_ptr = &mut entry as *mut TimerEntry;

        wheel.insert(entry_ptr);
        assert_eq!(entry.bucket_level, 1);

        wheel.current_tick = deadline;
        wheel.begin_tick_cascade();
        assert!(wheel.has_pending_cascade());
        assert_eq!(wheel.process_cascade_with_budget(0), 0);
        assert!(wheel.has_pending_cascade());

        assert_eq!(wheel.process_cascade_with_budget(1), 1);
        assert!(!wheel.has_pending_cascade());
        assert_eq!(entry.bucket_level, 0);
        assert_eq!(entry.bucket_index, 0);
        assert!(unsafe { wheel.lvl0[0].front(TimerEntry::LINK_OFFSET).is_some() });

        wheel.remove(entry_ptr);
    }

    #[test]
    fn timer_wheel_level2_cascade_reinserts_due_entry_into_level0() {
        let mut wheel = TimerWheel::new_uninit();
        init_wheel_at(&mut wheel, 0);
        let deadline = 1u64 << 14;
        let mut entry = timer_entry_at(deadline);
        let entry_ptr = &mut entry as *mut TimerEntry;

        wheel.insert(entry_ptr);
        assert_eq!(entry.bucket_level, 2);
        assert_eq!(entry.bucket_index, 1);

        wheel.current_tick = deadline;
        wheel.begin_tick_cascade();
        assert_eq!(wheel.cascade_count, 2);
        assert_eq!(wheel.cascade_levels[1], 2);
        assert_eq!(wheel.cascade_indices[1], 1);

        assert_eq!(wheel.process_cascade_with_budget(1), 1);
        assert!(!wheel.has_pending_cascade());
        assert_eq!(entry.bucket_level, 0);
        assert_eq!(entry.bucket_index, 0);
        assert!(unsafe { wheel.lvl0[0].front(TimerEntry::LINK_OFFSET).is_some() });

        wheel.remove(entry_ptr);
    }

    #[test]
    fn timer_wheel_level3_cascade_reinserts_due_entry_into_level0() {
        let mut wheel = TimerWheel::new_uninit();
        init_wheel_at(&mut wheel, 0);
        let deadline = 1u64 << 20;
        let mut entry = timer_entry_at(deadline);
        let entry_ptr = &mut entry as *mut TimerEntry;

        wheel.insert(entry_ptr);
        assert_eq!(entry.bucket_level, 3);
        assert_eq!(entry.bucket_index, 1);

        wheel.current_tick = deadline;
        wheel.begin_tick_cascade();
        assert_eq!(wheel.cascade_count, 3);
        assert_eq!(wheel.cascade_levels[2], 3);
        assert_eq!(wheel.cascade_indices[2], 1);

        assert_eq!(wheel.process_cascade_with_budget(1), 1);
        assert!(!wheel.has_pending_cascade());
        assert_eq!(entry.bucket_level, 0);
        assert_eq!(entry.bucket_index, 0);
        assert!(unsafe { wheel.lvl0[0].front(TimerEntry::LINK_OFFSET).is_some() });

        wheel.remove(entry_ptr);
    }

    #[test]
    fn timer_wheel_skip_empty_ticks_past_empty_target_range() {
        let mut wheel = TimerWheel::new_uninit();
        init_wheel_at(&mut wheel, 1_000);

        assert!(wheel.skip_empty_ticks_until_next_work(2_000));
        assert_eq!(wheel.current_tick, 2_001);
    }

    #[test]
    fn timer_wheel_skip_empty_ticks_stops_at_due_lvl0_bucket() {
        let mut wheel = TimerWheel::new_uninit();
        init_wheel_at(&mut wheel, 100);
        let mut entry = timer_entry_at(117);
        let entry_ptr = &mut entry as *mut TimerEntry;

        wheel.insert(entry_ptr);

        assert!(wheel.skip_empty_ticks_until_next_work(200));
        assert_eq!(wheel.current_tick, 117);
        assert_eq!(entry.bucket_level, 0);
        assert_eq!(entry.bucket_index, 117);

        wheel.remove(entry_ptr);
    }

    #[test]
    fn timer_wheel_skip_empty_ticks_stops_at_occupied_cascade_boundary() {
        let mut wheel = TimerWheel::new_uninit();
        init_wheel_at(&mut wheel, 0);
        let deadline = LVL0_SLOTS as u64;
        let mut entry = timer_entry_at(deadline);
        let entry_ptr = &mut entry as *mut TimerEntry;

        wheel.insert(entry_ptr);
        assert_eq!(entry.bucket_level, 1);

        assert!(wheel.skip_empty_ticks_until_next_work(deadline));
        assert_eq!(wheel.current_tick, deadline);
        assert!(!wheel.skip_empty_ticks_until_next_work(deadline));

        wheel.begin_tick_cascade();
        assert!(wheel.has_pending_cascade());
        assert_eq!(wheel.process_cascade_with_budget(1), 1);
        assert_eq!(entry.bucket_level, 0);
        assert_eq!(entry.bucket_index, 0);

        wheel.remove(entry_ptr);
    }

    #[test]
    fn timer_runtime_pool_provider_survives_arm_cancel_under_miri() {
        // Proxy Miri coverage for executor/reactor/timer holder ownership:
        // executor and reactor construction require real io_uring/socket
        // resources that Miri cannot run, while timer arm/cancel exercises the
        // same provider-owned pool path that must drop its provider without
        // retagging live entries.
        let mut runtime = TimerRuntime::new().expect("timer runtime construction failed");
        runtime.init().expect("timer runtime init failed");
        let deadline = runtime.wheel.current_tick.saturating_add(10);

        let entry = runtime
            .submit_sleep_at_tick(std::ptr::null_mut(), deadline)
            .expect("arming test timer failed");
        runtime
            .cancel_sleep(entry)
            .expect("canceling test timer failed");
    }

    #[test]
    #[cfg(not(miri))]
    fn timer_runtime_drop_unlinks_armed_timer_entries() {
        let mut runtime = TimerRuntime::new().expect("timer runtime construction failed");
        runtime.init().expect("timer runtime init failed");
        let tick = runtime.now_tick().expect("timer tick failed");

        runtime
            .submit_sleep_at_tick(std::ptr::null_mut(), tick.saturating_add(10))
            .expect("arming test timer failed");

        drop(runtime);
    }
}
