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

#[cfg(any(test, feature = "test-support"))]
use crate::runtime::executor::schedule_ctx_from_active_executor;
use crate::runtime::executor::{
    ExecutorOwner, PollCtx, ScheduleCtx, note_timer_expired, note_timer_now_tick_call,
    note_waiter_wake, notify_task_into_list_unchecked, poll_ctx_from_waker, retain_first_panic,
    schedule_ctx_unchecked,
};
use crate::runtime::task::{
    TaskHeader, clear_task_ref, release_task, replace_task_ref, take_task_ref,
};
use crate::utils::list::intrusive::dlist::{DList, Link};
use crate::utils::memory::pool::InPlaceInit;
use crate::utils::memory::provider::BasicMemoryProvider;
use crate::utils::memory::provider_owned_pool::ProviderOwnedPool;
use std::array;
use std::fmt;
use std::future::Future;
use std::io;
use std::mem::{ManuallyDrop, MaybeUninit};
use std::panic::{AssertUnwindSafe, catch_unwind, resume_unwind};
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

/// Duration of one internal timer tick in nanoseconds.
pub const TIMER_TICK_NS: u64 = 1_000_000;
const LVL0_SLOTS: usize = 256;
const LVLN_SLOTS: usize = 64;
const LVL0_SLOT_BITS: u32 = LVL0_SLOTS.trailing_zeros();
const LVLN_SLOT_BITS: u32 = LVLN_SLOTS.trailing_zeros();
const LVL1_SHIFT: u32 = LVL0_SLOT_BITS;
const LVL2_SHIFT: u32 = LVL1_SHIFT + LVLN_SLOT_BITS;
const LVL3_SHIFT: u32 = LVL2_SHIFT + LVLN_SLOT_BITS;
const LVL0_SLOT_MASK: u64 = (LVL0_SLOTS as u64) - 1;
const LVLN_SLOT_MASK: u64 = (LVLN_SLOTS as u64) - 1;
const LVL1_TICK_MASK: u64 = (1u64 << LVL1_SHIFT) - 1;
const LVL2_TICK_MASK: u64 = (1u64 << LVL2_SHIFT) - 1;
const LVL3_TICK_MASK: u64 = (1u64 << LVL3_SHIFT) - 1;
const TIMERS_PER_SLAB: usize = 1024;
const INVALID_BUCKET_LEVEL: u8 = u8::MAX;

const _: () = {
    assert!(LVL0_SLOTS.is_power_of_two());
    assert!(LVLN_SLOTS.is_power_of_two());
    assert!(LVLN_SLOTS <= u64::BITS as usize);
    assert!(LVL1_SHIFT == 8);
    assert!(LVL2_SHIFT == 14);
    assert!(LVL3_SHIFT == 20);
    assert!(LVL3_SHIFT + LVLN_SLOT_BITS < u64::BITS);
    assert!(LVL1_TICK_MASK == LVL0_SLOT_MASK);
};

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
    /// Tick whose upper-level cascades were most recently selected.
    ///
    /// Kept after cascade work completes so another executor pass on the same
    /// tick cannot recapture deferred outer entries.
    cascade_started_tick: u64,
    /// True once `cascade_started_tick` contains a selected wheel tick.
    cascade_started_tick_valid: bool,
    /// Last original link in the active level-3 bucket cascade.
    ///
    /// Entries reinserted or appended after this link remain for a later outer
    /// rotation. Cancellation adjusts this non-owning pointer before unlinking
    /// and freeing its node.
    outer_cascade_tail: *mut Link,
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
            cascade_started_tick: 0,
            cascade_started_tick_valid: false,
            outer_cascade_tail: std::ptr::null_mut(),
            lvl0: array::from_fn(|_| DList::new_uninit()),
            lvl1: array::from_fn(|_| DList::new_uninit()),
            lvl2: array::from_fn(|_| DList::new_uninit()),
            lvl3: array::from_fn(|_| DList::new_uninit()),
        }
    }

    fn init(&mut self) -> io::Result<()> {
        self.current_tick = now_tick()?;
        self.cascade_started_tick_valid = false;
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
        // Clear the non-owning cascade boundary before any payload link can
        // become invalid during teardown.
        self.outer_cascade_tail = std::ptr::null_mut();

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
        self.cascade_started_tick = 0;
        self.cascade_started_tick_valid = false;
    }

    fn insert(&mut self, entry: *mut TimerEntry) {
        let deadline = unsafe { (*entry).deadline_tick };
        let delta = deadline.saturating_sub(self.current_tick);
        let (level, index) = if delta < (1u64 << LVL1_SHIFT) {
            (0u8, (deadline & LVL0_SLOT_MASK) as usize)
        } else if delta < (1u64 << LVL2_SHIFT) {
            (1u8, ((deadline >> LVL1_SHIFT) & LVLN_SLOT_MASK) as usize)
        } else if delta < (1u64 << LVL3_SHIFT) {
            (2u8, ((deadline >> LVL2_SHIFT) & LVLN_SLOT_MASK) as usize)
        } else {
            (3u8, ((deadline >> LVL3_SHIFT) & LVLN_SLOT_MASK) as usize)
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
            self.adjust_outer_cascade_tail_before_remove(level, index, link);
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

    /// Moves the active outer-cascade stop before its node is unlinked.
    ///
    /// Original nodes remain before the captured tail. Entries already
    /// processed or appended after cascade start are behind it, so removing a
    /// front tail means no original work remains for this cascade.
    fn adjust_outer_cascade_tail_before_remove(
        &mut self,
        level: u8,
        index: usize,
        link: *mut Link,
    ) {
        if level != 3 || self.outer_cascade_tail != link {
            return;
        }

        let outer_pending = (self.cascade_pos as usize..self.cascade_count as usize)
            .any(|pos| self.cascade_levels[pos] == 3 && self.cascade_indices[pos] == index);
        debug_assert!(
            outer_pending,
            "outer cascade tail exists without a matching pending bucket"
        );
        if !outer_pending {
            self.outer_cascade_tail = std::ptr::null_mut();
            return;
        }

        self.outer_cascade_tail = unsafe {
            self.lvl3[index]
                .previous_link(link)
                .unwrap_or(std::ptr::null_mut())
        };
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
        let start = (self.current_tick & LVL0_SLOT_MASK) as usize;
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
        if (self.current_tick & LVL1_TICK_MASK) != 0 {
            return false;
        }

        let idx1 = ((self.current_tick >> LVL1_SHIFT) & LVLN_SLOT_MASK) as usize;
        if (self.lvl1_bits & (1u64 << idx1)) != 0 {
            return true;
        }

        if (self.current_tick & LVL2_TICK_MASK) != 0 {
            return false;
        }

        let idx2 = ((self.current_tick >> LVL2_SHIFT) & LVLN_SLOT_MASK) as usize;
        if (self.lvl2_bits & (1u64 << idx2)) != 0 {
            return true;
        }

        if (self.current_tick & LVL3_TICK_MASK) != 0 {
            return false;
        }

        let idx3 = ((self.current_tick >> LVL3_SHIFT) & LVLN_SLOT_MASK) as usize;
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
        let cycle = 1u64 << (shift + LVLN_SLOT_BITS);
        let cycle_base = first_boundary & !(cycle - 1);
        let start = ((first_boundary >> shift) & LVLN_SLOT_MASK) as usize;

        if let Some(index) = Self::next_set_bit(bits, start) {
            return cycle_base.checked_add((index as u64) << shift);
        }

        let index = Self::next_set_bit(bits, 0)?;
        cycle_base
            .checked_add(cycle)?
            .checked_add((index as u64) << shift)
    }

    fn next_upper_cascade_tick(&self) -> Option<u64> {
        Self::next_occupied_cascade_tick(self.lvl1_bits, self.current_tick, LVL1_SHIFT)
            .into_iter()
            .chain(Self::next_occupied_cascade_tick(
                self.lvl2_bits,
                self.current_tick,
                LVL2_SHIFT,
            ))
            .chain(Self::next_occupied_cascade_tick(
                self.lvl3_bits,
                self.current_tick,
                LVL3_SHIFT,
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

        let idx = (self.current_tick & LVL0_SLOT_MASK) as usize;
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
        if self.has_pending_cascade()
            || (self.cascade_started_tick_valid && self.cascade_started_tick == self.current_tick)
        {
            return;
        }

        self.cascade_started_tick = self.current_tick;
        self.cascade_started_tick_valid = true;
        self.cascade_count = 0;
        self.cascade_pos = 0;
        self.outer_cascade_tail = std::ptr::null_mut();

        if (self.current_tick & LVL1_TICK_MASK) == 0 {
            let idx1 = ((self.current_tick >> LVL1_SHIFT) & LVLN_SLOT_MASK) as usize;
            self.cascade_levels[self.cascade_count as usize] = 1;
            self.cascade_indices[self.cascade_count as usize] = idx1;
            self.cascade_count += 1;

            if (self.current_tick & LVL2_TICK_MASK) == 0 {
                let idx2 = ((self.current_tick >> LVL2_SHIFT) & LVLN_SLOT_MASK) as usize;
                self.cascade_levels[self.cascade_count as usize] = 2;
                self.cascade_indices[self.cascade_count as usize] = idx2;
                self.cascade_count += 1;

                if (self.current_tick & LVL3_TICK_MASK) == 0 {
                    let idx3 = ((self.current_tick >> LVL3_SHIFT) & LVLN_SLOT_MASK) as usize;
                    self.cascade_levels[self.cascade_count as usize] = 3;
                    self.cascade_indices[self.cascade_count as usize] = idx3;
                    self.cascade_count += 1;
                    self.outer_cascade_tail =
                        self.lvl3[idx3].back_link().unwrap_or(std::ptr::null_mut());
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
            if level == 3 && self.outer_cascade_tail.is_null() {
                self.cascade_pos += 1;
                continue;
            }
            let entry_ptr = unsafe {
                match level {
                    1 => self.lvl1[index].pop_front(TimerEntry::LINK_OFFSET),
                    2 => self.lvl2[index].pop_front(TimerEntry::LINK_OFFSET),
                    _ => self.lvl3[index].pop_front(TimerEntry::LINK_OFFSET),
                }
            };
            let Some(entry_ptr) = entry_ptr else {
                if level == 3 {
                    self.outer_cascade_tail = std::ptr::null_mut();
                }
                self.cascade_pos += 1;
                continue;
            };
            let entry_link = unsafe { std::ptr::addr_of_mut!((*entry_ptr).link) };
            let reached_outer_tail = level == 3 && entry_link == self.outer_cascade_tail;
            self.clear_bucket_if_empty(level, index);
            unsafe {
                (*entry_ptr).bucket_level = INVALID_BUCKET_LEVEL;
                (*entry_ptr).bucket_index = 0;
            }
            self.insert(entry_ptr);
            if reached_outer_tail {
                self.outer_cascade_tail = std::ptr::null_mut();
                self.cascade_pos += 1;
            }
            consumed += 1;
        }

        while self.has_pending_cascade() {
            let pos = self.cascade_pos as usize;
            let level = self.cascade_levels[pos];
            let index = self.cascade_indices[pos];
            let has_more = if level == 3 {
                !self.outer_cascade_tail.is_null()
            } else {
                unsafe {
                    match level {
                        1 => self.lvl1[index].front(TimerEntry::LINK_OFFSET).is_some(),
                        _ => self.lvl2[index].front(TimerEntry::LINK_OFFSET).is_some(),
                    }
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
            self.outer_cascade_tail = std::ptr::null_mut();
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
    /// Raw monotonic nanoseconds sampled immediately after `instant`.
    nanos: u64,
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
    pub fn now_tick(&self) -> io::Result<u64> {
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
            debug_assert!((*entry).link.is_unlinked());
            debug_assert!((*entry).waiter.is_null());
            (*entry).owner = if self.owner.is_null() {
                None
            } else {
                Some(ExecutorOwner::clone_rc(self.owner))
            };
            (*entry).register_waiter(task);
            (*entry).deadline_tick = deadline_tick;
        }
        self.wheel.insert(entry);
        Ok(entry)
    }

    fn sample_arm_nanos(&mut self) -> io::Result<u64> {
        let nanos = now_nanos()?;
        let tick = nanos / TIMER_TICK_NS;

        // When the wheel is empty, its current tick may be arbitrarily stale
        // after a long timer-free idle period. Snap it forward so newly armed
        // sleeps do not start from an old baseline and later timer processing
        // does not need to burn budget catching up empty ticks.
        if !self.has_pending() {
            self.wheel.current_tick = tick.max(self.wheel.current_tick);
        }

        Ok(nanos)
    }

    fn absolute_arm_base(&mut self) -> io::Result<ArmBase> {
        if let Some(base) = self.absolute_arm_base {
            return Ok(base);
        }

        // Absolute deadlines convert against one paired sample per executor
        // pass. Relative durations take their own raw clock sample instead.
        let instant = Instant::now();
        let nanos = self.sample_arm_nanos()?;

        let base = ArmBase { instant, nanos };
        self.absolute_arm_base = Some(base);
        Ok(base)
    }

    fn deadline_tick_for_duration(&mut self, duration: Duration) -> io::Result<u64> {
        let arm_nanos = self.sample_arm_nanos()?;
        Ok(deadline_tick_from_nanos(arm_nanos, duration))
    }

    fn deadline_tick_for_instant(&mut self, deadline: Instant) -> io::Result<Option<u64>> {
        let base = self.absolute_arm_base()?;
        if deadline <= base.instant {
            return Ok(None);
        }

        let delta = deadline.duration_since(base.instant);
        Ok(Some(deadline_tick_from_nanos(base.nanos, delta)))
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

    /// Expires timers up to `now`, respecting the provided per-pass budget.
    ///
    /// Returns `true` when timer work remains pending for a later pass.
    /// When advancing the timer wheel, returns
    /// [`io::ErrorKind::NotConnected`] if no
    /// [`crate::runtime::executor::Executor::run`] context is active.
    #[cfg(any(test, feature = "test-support"))]
    pub fn process_at_with_budget(&mut self, now: u64, budget: usize) -> io::Result<bool> {
        if now >= self.wheel.current_tick {
            let schedule_ctx = schedule_ctx_from_active_executor()?;
            return Ok(unsafe { Self::collect_expired_unchecked(self, now, budget, schedule_ctx) });
        }
        Ok(false)
    }

    /// Expires owner-bound timers without retaining a mutable runtime borrow
    /// across waiter scheduling or final reference release.
    ///
    /// # Safety
    ///
    /// `timers` must identify a live, initialized, address-stable timer runtime.
    /// The caller and every non-null waiter must belong to the same owner
    /// thread. No caller may access that runtime concurrently, but waiter
    /// destruction may re-enter it synchronously.
    pub(crate) unsafe fn process_at_with_budget_unchecked(
        timers: *mut Self,
        now: u64,
        budget: usize,
    ) -> io::Result<bool> {
        debug_assert!(!timers.is_null(), "timer processing requires a runtime");
        if now >= unsafe { (*timers).wheel.current_tick } {
            let schedule_ctx = unsafe { schedule_ctx_unchecked() };
            return Ok(unsafe {
                Self::collect_expired_unchecked(timers, now, budget, schedule_ctx)
            });
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
    //
    // This core deliberately operates from a raw pointer. Releasing the final
    // waiter reference can destroy a task output whose timer future re-enters
    // cancellation on this same runtime, including the just-fired entry.
    unsafe fn collect_expired_unchecked(
        timers: *mut Self,
        target_tick: u64,
        budget: usize,
        schedule_ctx: ScheduleCtx,
    ) -> bool {
        let mut remaining_budget = budget;
        while unsafe { (*timers).wheel.current_tick <= target_tick } {
            if unsafe {
                (*std::ptr::addr_of_mut!((*timers).wheel))
                    .skip_empty_ticks_until_next_work(target_tick)
            } {
                continue;
            }

            unsafe {
                (*std::ptr::addr_of_mut!((*timers).wheel)).begin_tick_cascade();
            }
            if unsafe { (*timers).wheel.has_pending_cascade() } {
                if remaining_budget == 0 {
                    unsafe {
                        (*timers).wheel.next_deadline_dirty = true;
                    }
                    return true;
                }

                let consumed = unsafe {
                    (*std::ptr::addr_of_mut!((*timers).wheel))
                        .process_cascade_with_budget(remaining_budget)
                };
                remaining_budget -= consumed;
                if unsafe { (*timers).wheel.has_pending_cascade() } {
                    unsafe {
                        (*timers).wheel.next_deadline_dirty = true;
                    }
                    return true;
                }
            }

            let idx = unsafe { ((*timers).wheel.current_tick & LVL0_SLOT_MASK) as usize };
            while let Some(entry_ptr) =
                unsafe { (*timers).wheel.lvl0[idx].pop_front(TimerEntry::LINK_OFFSET) }
            {
                unsafe {
                    (*std::ptr::addr_of_mut!((*timers).wheel)).clear_bucket_if_empty(0, idx);
                }
                if remaining_budget == 0 {
                    unsafe {
                        (*timers).wheel.lvl0[idx]
                            .push_front_unchecked(std::ptr::addr_of_mut!((*entry_ptr).link));
                        (*std::ptr::addr_of_mut!((*timers).wheel)).set_bucket_occupied(0, idx);
                        (*timers).wheel.next_deadline_dirty = true;
                    }
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
                        notify_task_into_list_unchecked(
                            waiter,
                            schedule_ctx.ready_queue,
                            schedule_ctx.runtime_state,
                        );
                        release_task(waiter);
                    }
                }
                remaining_budget -= 1;
            }
            if unsafe { (*timers).wheel.current_tick == u64::MAX } {
                break;
            }
            unsafe {
                (*timers).wheel.current_tick += 1;
            }
        }
        if unsafe { (*timers).wheel.next_deadline_tick }
            .is_some_and(|deadline_tick| deadline_tick <= target_tick)
        {
            unsafe {
                (*timers).wheel.next_deadline_dirty = true;
            }
        }
        false
    }

    fn free_wheel_entries_for_drop(&mut self) {
        // The boundary does not own the entry. Clear it before draining and
        // returning any linked entry to the pool.
        self.wheel.outer_cascade_tail = std::ptr::null_mut();

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

    /// Cancels every armed timer without retaining a runtime borrow across
    /// waiter destruction.
    ///
    /// # Safety
    ///
    /// `timers` must identify the initialized timer runtime belonging to the
    /// active executor on its owner thread. No caller may access that runtime
    /// concurrently, but waiter destruction may re-enter it synchronously.
    pub(crate) unsafe fn cancel_all_for_shutdown_unchecked(timers: *mut Self) {
        debug_assert!(!timers.is_null(), "timer shutdown requires a runtime");
        // Shutdown invalidates every bucket link, so retire the non-owning
        // boundary before the first drain.
        unsafe {
            (*timers).wheel.outer_cascade_tail = std::ptr::null_mut();
        }
        let mut first_panic = None;

        let lvl0 = unsafe { std::ptr::addr_of_mut!((*timers).wheel.lvl0).cast::<DList<_>>() };
        for index in 0..LVL0_SLOTS {
            unsafe {
                cancel_timer_bucket_entries(lvl0.add(index), &mut first_panic);
            }
        }
        let lvl1 = unsafe { std::ptr::addr_of_mut!((*timers).wheel.lvl1).cast::<DList<_>>() };
        for index in 0..LVLN_SLOTS {
            unsafe {
                cancel_timer_bucket_entries(lvl1.add(index), &mut first_panic);
            }
        }
        let lvl2 = unsafe { std::ptr::addr_of_mut!((*timers).wheel.lvl2).cast::<DList<_>>() };
        for index in 0..LVLN_SLOTS {
            unsafe {
                cancel_timer_bucket_entries(lvl2.add(index), &mut first_panic);
            }
        }
        let lvl3 = unsafe { std::ptr::addr_of_mut!((*timers).wheel.lvl3).cast::<DList<_>>() };
        for index in 0..LVLN_SLOTS {
            unsafe {
                cancel_timer_bucket_entries(lvl3.add(index), &mut first_panic);
            }
        }
        unsafe {
            (*std::ptr::addr_of_mut!((*timers).wheel)).unlink_all_for_drop();
            (*timers).absolute_arm_base = None;
        }

        if let Some(payload) = first_panic {
            resume_unwind(payload);
        }
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
    timer_pool: &mut ProviderOwnedPool<TimerEntry, BasicMemoryProvider>,
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

unsafe fn cancel_timer_bucket_entries(
    bucket: *mut DList<TimerEntry>,
    first_panic: &mut Option<crate::runtime::executor::PanicPayload>,
) {
    while let Some(entry) = unsafe { (*bucket).pop_front(TimerEntry::LINK_OFFSET) } {
        let waiter = unsafe {
            (*entry).state = TimerState::Cancelled;
            (*entry).bucket_level = INVALID_BUCKET_LEVEL;
            (*entry).bucket_index = 0;
            (*entry).take_waiter()
        };
        if !waiter.is_null() {
            retain_first_panic(
                first_panic,
                catch_unwind(AssertUnwindSafe(|| unsafe {
                    release_task(waiter);
                })),
            );
        }
    }
}

fn deadline_tick_from_nanos(arm_nanos: u64, duration: Duration) -> u64 {
    // Expiry compares a floored current tick with this deadline. Round the
    // complete raw target up so a fractional arm tick cannot shorten the
    // caller's requested duration by as much as one timer tick.
    let nanos = (arm_nanos as u128).saturating_add(duration.as_nanos());
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
    now_nanos().map(|nanos| nanos / TIMER_TICK_NS)
}

fn now_nanos() -> io::Result<u64> {
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

    Ok((ts.tv_sec as u64)
        .saturating_mul(1_000_000_000u64)
        .saturating_add(ts.tv_nsec as u64))
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

    /// Returns an armed entry to its recorded origin timer runtime.
    #[inline(always)]
    fn reclaim_entry(&mut self) {
        if self.entry.is_null() {
            return;
        }

        let entry = self.entry;
        self.entry = std::ptr::null_mut();
        let (owner, timers) = unsafe { timer_runtime_for_entry(entry) };
        if !timers.is_null() {
            let _ = unsafe { &mut *timers }.cancel_sleep(entry);
        }
        drop(owner);
    }

    /// Validates one timer poll without arming or advancing the timer.
    #[inline(always)]
    fn validate_poll_context(&mut self, cx: &Context<'_>) -> io::Result<PollCtx> {
        let pctx = match poll_ctx_from_waker(cx) {
            Ok(pctx) => pctx,
            Err(err) => {
                self.reclaim_entry();
                return Err(err);
            }
        };

        if !self.entry.is_null() && unsafe { (*self.entry).owner_ptr() } != pctx.owner_ptr() {
            self.reclaim_entry();
            return Err(io::Error::from(io::ErrorKind::NotConnected));
        }

        Ok(pctx)
    }

    /// Polls a timer after its current and origin contexts have been checked.
    #[inline(always)]
    fn poll_validated(&mut self, pctx: &PollCtx) -> Poll<io::Result<()>> {
        if !self.entry.is_null() {
            let state = unsafe { (*self.entry).state };
            if state == TimerState::Fired {
                self.reclaim_entry();
                return Poll::Ready(Ok(()));
            }
            if state == TimerState::Cancelled {
                self.reclaim_entry();
                return Poll::Ready(Err(io::Error::from(io::ErrorKind::BrokenPipe)));
            }
            unsafe { refresh_sleep_waiter(self.entry, pctx) };
            return Poll::Pending;
        }

        let timers = pctx.timers();

        if let Some(duration) = self.duration.take() {
            if duration == Duration::ZERO {
                return Poll::Ready(Ok(()));
            }

            let entry =
                unsafe { &mut *timers }.submit_sleep_duration(pctx.owner_task(), duration)?;
            self.entry = entry;
            return Poll::Pending;
        }

        let Some(deadline) = self.deadline.take() else {
            return Poll::Ready(Ok(()));
        };

        match unsafe { &mut *timers }.submit_sleep_deadline(pctx.owner_task(), deadline)? {
            Some(entry) => {
                self.entry = entry;
                Poll::Pending
            }
            None => Poll::Ready(Ok(())),
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
    /// including an inactive or foreign executor context and resource-pressure
    /// errors such as [`io::ErrorKind::OutOfMemory`].
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
        let pctx = match this.validate_poll_context(cx) {
            Ok(pctx) => pctx,
            Err(err) => return Poll::Ready(Err(err)),
        };
        this.poll_validated(&pctx)
    }
}

impl Drop for Sleep {
    fn drop(&mut self) {
        self.reclaim_entry();
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
/// allocation and runtime failures. Context validation happens before the
/// wrapped future is polled; an inactive or foreign poll returns
/// `TimeoutError::Runtime` containing [`io::ErrorKind::NotConnected`] without
/// reaching that future. After successful validation, the wrapped future is
/// polled first and wins a same-poll deadline race. An immediately ready
/// wrapped future allocates and arms no timer entry.
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

        let pctx = match this.sleep.validate_poll_context(cx) {
            Ok(pctx) => pctx,
            Err(err) => return Poll::Ready(Err(TimeoutError::Runtime(err))),
        };

        let mut future = unsafe { Pin::new_unchecked(&mut this.future) };
        if let Poll::Ready(output) = future.as_mut().poll(cx) {
            return Poll::Ready(Ok(output));
        }

        match this.sleep.poll_validated(&pctx) {
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
/// [`crate::runtime::executor::Executor::run`]. Invalid context is reported
/// before the wrapped future is polled. In a valid context, an immediately
/// ready wrapped future completes without allocating or arming a timer.
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
/// [`crate::runtime::executor::Executor::run`]. Invalid context is reported
/// before the wrapped future is polled. In a valid context, an immediately
/// ready wrapped future completes without allocating or arming a timer.
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
    #[cfg(not(miri))]
    use crate::runtime::executor::Executor;
    use crate::runtime::executor::RuntimeState;
    use crate::runtime::task::TaskVTable;
    use std::cell::Cell;
    #[cfg(not(miri))]
    use std::cell::RefCell;
    use std::panic::{AssertUnwindSafe, catch_unwind};

    thread_local! {
        static TIMER_DRAIN_DESTROYS: Cell<usize> = const { Cell::new(0) };
        static TIMER_REENTRY_RUNTIME: Cell<*mut TimerRuntime> =
            const { Cell::new(std::ptr::null_mut()) };
        static TIMER_REENTRY_ENTRY: Cell<*mut TimerEntry> =
            const { Cell::new(std::ptr::null_mut()) };
        static TIMER_REENTRY_DESTROYS: Cell<usize> = const { Cell::new(0) };
    }

    #[derive(Debug)]
    struct TimerDrainWaiterPanic;

    #[derive(Debug)]
    struct LaterTimerDrainWaiterPanic;

    #[derive(Debug)]
    struct ReentrantTimerDrainWaiterPanic;

    unsafe fn panic_timer_waiter_destroy(_: *mut TaskHeader) {
        TIMER_DRAIN_DESTROYS.with(|destroys| destroys.set(destroys.get() + 1));
        std::panic::panic_any(TimerDrainWaiterPanic);
    }

    unsafe fn later_panic_timer_waiter_destroy(_: *mut TaskHeader) {
        TIMER_DRAIN_DESTROYS.with(|destroys| destroys.set(destroys.get() + 1));
        std::panic::panic_any(LaterTimerDrainWaiterPanic);
    }

    static PANIC_TIMER_WAITER_VTABLE: TaskVTable = TaskVTable {
        poll: |_| Poll::Ready(()),
        finish: |_| {},
        cancel: |_| {},
        destroy: panic_timer_waiter_destroy,
    };

    static LATER_PANIC_TIMER_WAITER_VTABLE: TaskVTable = TaskVTable {
        poll: |_| Poll::Ready(()),
        finish: |_| {},
        cancel: |_| {},
        destroy: later_panic_timer_waiter_destroy,
    };

    unsafe fn reenter_timer_waiter_destroy(_: *mut TaskHeader) {
        TIMER_REENTRY_DESTROYS.with(|destroys| destroys.set(destroys.get() + 1));
        let runtime = TIMER_REENTRY_RUNTIME.with(Cell::get);
        let entry = TIMER_REENTRY_ENTRY.with(Cell::get);
        assert!(!runtime.is_null(), "timer re-entry runtime is missing");
        assert!(!entry.is_null(), "timer re-entry entry is missing");
        unsafe {
            (*runtime)
                .cancel_sleep(entry)
                .expect("timer re-entry cancellation failed");
        }
    }

    unsafe fn reenter_then_panic_timer_waiter_destroy(task: *mut TaskHeader) {
        unsafe {
            reenter_timer_waiter_destroy(task);
        }
        std::panic::panic_any(ReentrantTimerDrainWaiterPanic);
    }

    static REENTER_TIMER_WAITER_VTABLE: TaskVTable = TaskVTable {
        poll: |_| Poll::Ready(()),
        finish: |_| {},
        cancel: |_| {},
        destroy: reenter_timer_waiter_destroy,
    };

    static REENTER_PANIC_TIMER_WAITER_VTABLE: TaskVTable = TaskVTable {
        poll: |_| Poll::Ready(()),
        finish: |_| {},
        cancel: |_| {},
        destroy: reenter_then_panic_timer_waiter_destroy,
    };

    enum ScriptedMode {
        Ready(usize),
        #[cfg(not(miri))]
        AlwaysPending,
        #[cfg(not(miri))]
        PendingThenReady(usize),
    }

    struct ScriptedFuture {
        mode: ScriptedMode,
        polls: Rc<Cell<usize>>,
        drops: Rc<Cell<usize>>,
    }

    impl Future for ScriptedFuture {
        type Output = usize;

        fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
            let this = self.get_mut();
            let prior_polls = this.polls.get();
            this.polls.set(prior_polls + 1);
            match this.mode {
                ScriptedMode::Ready(value) => Poll::Ready(value),
                #[cfg(not(miri))]
                ScriptedMode::AlwaysPending => Poll::Pending,
                #[cfg(not(miri))]
                ScriptedMode::PendingThenReady(value) => {
                    if prior_polls == 0 {
                        Poll::Pending
                    } else {
                        Poll::Ready(value)
                    }
                }
            }
        }
    }

    impl Drop for ScriptedFuture {
        fn drop(&mut self) {
            self.drops.set(self.drops.get() + 1);
        }
    }

    fn scripted_future(
        mode: ScriptedMode,
        polls: &Rc<Cell<usize>>,
        drops: &Rc<Cell<usize>>,
    ) -> ScriptedFuture {
        ScriptedFuture {
            mode,
            polls: Rc::clone(polls),
            drops: Rc::clone(drops),
        }
    }

    #[cfg(not(miri))]
    struct StageFiredTimeout {
        timeout: Option<Timeout<ScriptedFuture>>,
        staged: Rc<RefCell<Option<Timeout<ScriptedFuture>>>>,
        armed: bool,
    }

    #[cfg(not(miri))]
    impl Future for StageFiredTimeout {
        type Output = ();

        fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
            let this = self.get_mut();
            if this.armed {
                *this.staged.borrow_mut() = this.timeout.take();
                return Poll::Ready(());
            }

            let timeout = this.timeout.as_mut().expect("staged timeout missing");
            assert!(Pin::new(timeout).poll(cx).is_pending());
            this.armed = true;
            Poll::Pending
        }
    }

    #[cfg(not(miri))]
    fn stage_armed_timeout(
        executor: &mut Executor,
        polls: &Rc<Cell<usize>>,
        drops: &Rc<Cell<usize>>,
    ) -> Timeout<ScriptedFuture> {
        let staged = Rc::new(RefCell::new(None));
        let staged_slot = Rc::clone(&staged);
        let future = scripted_future(ScriptedMode::AlwaysPending, polls, drops);

        let err = executor
            .run(async move {
                let mut timed = timeout(Duration::from_secs(60), future);
                std::future::poll_fn(|cx| {
                    assert!(Pin::new(&mut timed).poll(cx).is_pending());
                    Poll::Ready(())
                })
                .await;
                *staged_slot.borrow_mut() = Some(timed);
                crate::runtime::test_hooks::fail_next_ring_wait_errno(libc::EIO);
            })
            .expect_err("injected wait error should leave the timeout armed");
        assert_eq!(err.raw_os_error(), Some(libc::EIO));

        staged
            .borrow_mut()
            .take()
            .expect("armed timeout did not escape")
    }

    #[cfg(not(miri))]
    fn assert_timer_entry_reused(executor: &mut Executor, expected: *mut TimerEntry) {
        executor
            .run(async move {
                let mut probe = sleep(Duration::from_secs(60));
                std::future::poll_fn(|cx| {
                    assert!(Pin::new(&mut probe).poll(cx).is_pending());
                    assert_eq!(
                        probe.entry, expected,
                        "origin timer pool did not reuse the reclaimed entry"
                    );
                    Poll::Ready(())
                })
                .await;
                drop(probe);
            })
            .expect("origin executor failed while proving timer-entry reuse");
    }

    #[test]
    fn timeout_rejects_inactive_context_before_polling_ready_inner() {
        let polls = Rc::new(Cell::new(0));
        let drops = Rc::new(Cell::new(0));
        let future = scripted_future(ScriptedMode::Ready(7), &polls, &drops);
        let mut timed = timeout(Duration::from_secs(1), future);
        let mut cx = Context::from_waker(std::task::Waker::noop());

        assert!(matches!(
            Pin::new(&mut timed).poll(&mut cx),
            Poll::Ready(Err(TimeoutError::Runtime(err)))
                if err.kind() == io::ErrorKind::NotConnected
        ));
        assert_eq!(polls.get(), 0, "inactive poll reached the wrapped future");

        drop(timed);
        assert_eq!(
            drops.get(),
            1,
            "wrapped future was not dropped exactly once"
        );
    }

    #[test]
    #[cfg(not(miri))]
    fn runtime_timeout_ready_inner_does_not_arm_timer() {
        let polls = Rc::new(Cell::new(0));
        let drops = Rc::new(Cell::new(0));
        let polls_flag = Rc::clone(&polls);
        let drops_flag = Rc::clone(&drops);
        let mut executor = Executor::new().expect("failed to construct executor");

        executor
            .run(async move {
                crate::runtime::test_hooks::fail_next_timer_alloc();
                let future = scripted_future(ScriptedMode::Ready(7), &polls_flag, &drops_flag);
                let mut timed = timeout(Duration::from_secs(1), future);
                let result = std::future::poll_fn(|cx| match Pin::new(&mut timed).poll(cx) {
                    Poll::Ready(result) => Poll::Ready(result),
                    Poll::Pending => panic!("ready inner future left timeout pending"),
                })
                .await;

                assert_eq!(result.expect("ready timeout failed"), 7);
                assert_eq!(polls_flag.get(), 1);
                assert!(
                    timed.sleep.entry.is_null(),
                    "ready inner future allocated a timer entry"
                );

                let err = sleep(Duration::from_millis(1))
                    .await
                    .expect_err("ready timeout consumed the timer-allocation failure");
                assert_eq!(err.kind(), io::ErrorKind::OutOfMemory);
                drop(timed);
            })
            .expect("executor failed while checking immediate timeout completion");

        assert_eq!(
            drops.get(),
            1,
            "wrapped future was not dropped exactly once"
        );
    }

    #[test]
    #[cfg(not(miri))]
    fn runtime_timeout_armed_foreign_repoll_reclaims_origin_slot() {
        let polls = Rc::new(Cell::new(0));
        let drops = Rc::new(Cell::new(0));
        let mut origin = Executor::new().expect("failed to construct origin executor");
        let mut timed = stage_armed_timeout(&mut origin, &polls, &drops);
        let origin_entry = timed.sleep.entry;
        assert!(!origin_entry.is_null());
        assert!(unsafe { (*origin_entry).state } == TimerState::Armed);
        assert_eq!(polls.get(), 1);

        let returned = Rc::new(RefCell::new(None));
        let returned_slot = Rc::clone(&returned);
        let mut foreign = Executor::new().expect("failed to construct foreign executor");
        foreign
            .run(async move {
                let result = std::future::poll_fn(|cx| match Pin::new(&mut timed).poll(cx) {
                    Poll::Ready(result) => Poll::Ready(result),
                    Poll::Pending => panic!("foreign timeout repoll remained pending"),
                })
                .await;
                assert!(matches!(
                    result,
                    Err(TimeoutError::Runtime(err))
                        if err.kind() == io::ErrorKind::NotConnected
                ));
                assert!(
                    timed.sleep.entry.is_null(),
                    "foreign rejection retained the origin timer entry"
                );
                *returned_slot.borrow_mut() = Some(timed);
            })
            .expect("foreign executor failed while rejecting timeout");

        assert_eq!(
            polls.get(),
            1,
            "foreign rejection polled the wrapped future"
        );
        let timed = returned
            .borrow_mut()
            .take()
            .expect("rejected timeout was not returned");
        assert_timer_entry_reused(&mut origin, origin_entry);
        drop(timed);
        assert_eq!(
            drops.get(),
            1,
            "wrapped future was not dropped exactly once"
        );
    }

    #[test]
    #[cfg(not(miri))]
    fn runtime_timeout_inner_wins_when_timer_already_fired() {
        let polls = Rc::new(Cell::new(0));
        let drops = Rc::new(Cell::new(0));
        let staged = Rc::new(RefCell::new(None));
        let future = scripted_future(ScriptedMode::PendingThenReady(11), &polls, &drops);
        let mut executor = Executor::new().expect("failed to construct executor");

        executor
            .run(StageFiredTimeout {
                timeout: Some(timeout(Duration::from_millis(1), future)),
                staged: Rc::clone(&staged),
                armed: false,
            })
            .expect("executor failed while staging fired timeout");

        let mut timed = staged
            .borrow_mut()
            .take()
            .expect("fired timeout did not escape");
        let fired_entry = timed.sleep.entry;
        assert!(!fired_entry.is_null());
        assert!(unsafe { (*fired_entry).state } == TimerState::Fired);
        assert_eq!(polls.get(), 1);

        let returned = Rc::new(RefCell::new(None));
        let returned_slot = Rc::clone(&returned);
        executor
            .run(async move {
                let result = std::future::poll_fn(|cx| match Pin::new(&mut timed).poll(cx) {
                    Poll::Ready(result) => Poll::Ready(result),
                    Poll::Pending => panic!("ready inner lost the fired-deadline race"),
                })
                .await;
                assert_eq!(result.expect("inner-first timeout failed"), 11);
                assert_eq!(
                    timed.sleep.entry, fired_entry,
                    "inner-first completion consumed the fired timer early"
                );
                *returned_slot.borrow_mut() = Some(timed);
            })
            .expect("executor failed while resolving fired timeout race");

        assert_eq!(polls.get(), 2);
        let timed = returned
            .borrow_mut()
            .take()
            .expect("completed timeout was not returned");
        drop(timed);
        assert_timer_entry_reused(&mut executor, fired_entry);
        assert_eq!(
            drops.get(),
            1,
            "wrapped future was not dropped exactly once"
        );
    }

    #[test]
    #[cfg(not(miri))]
    fn runtime_timeout_drop_reclaims_armed_slot_and_inner_once() {
        let polls = Rc::new(Cell::new(0));
        let drops = Rc::new(Cell::new(0));
        let polls_flag = Rc::clone(&polls);
        let drops_flag = Rc::clone(&drops);
        let mut executor = Executor::new().expect("failed to construct executor");

        executor
            .run(async move {
                let future = scripted_future(ScriptedMode::AlwaysPending, &polls_flag, &drops_flag);
                let mut timed = timeout(Duration::from_secs(60), future);
                std::future::poll_fn(|cx| {
                    assert!(Pin::new(&mut timed).poll(cx).is_pending());
                    Poll::Ready(())
                })
                .await;
                let entry = timed.sleep.entry;
                assert!(!entry.is_null());
                drop(timed);
                assert_eq!(drops_flag.get(), 1);

                let mut probe = sleep(Duration::from_secs(60));
                std::future::poll_fn(|cx| {
                    assert!(Pin::new(&mut probe).poll(cx).is_pending());
                    assert_eq!(
                        probe.entry, entry,
                        "drop did not return the timer entry to its origin pool"
                    );
                    Poll::Ready(())
                })
                .await;
                drop(probe);
            })
            .expect("executor failed while checking timeout drop reclamation");

        assert_eq!(polls.get(), 1);
        assert_eq!(
            drops.get(),
            1,
            "wrapped future was not dropped exactly once"
        );
    }

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
    fn timer_wheel_derived_level_boundaries_preserve_bucket_selection() {
        let cases = [
            (LVL1_TICK_MASK, 0, LVL0_SLOT_MASK as u16),
            (LVL1_TICK_MASK + 1, 1, 1),
            (LVL2_TICK_MASK, 1, LVLN_SLOT_MASK as u16),
            (LVL2_TICK_MASK + 1, 2, 1),
            (LVL3_TICK_MASK, 2, LVLN_SLOT_MASK as u16),
            (LVL3_TICK_MASK + 1, 3, 1),
        ];

        for (deadline, expected_level, expected_index) in cases {
            let mut wheel = TimerWheel::new_uninit();
            init_wheel_at(&mut wheel, 0);
            let mut entry = timer_entry_at(deadline);
            let entry_ptr = &mut entry as *mut TimerEntry;

            wheel.insert(entry_ptr);
            assert_eq!(entry.bucket_level, expected_level, "deadline {deadline}");
            assert_eq!(entry.bucket_index, expected_index, "deadline {deadline}");
            wheel.remove(entry_ptr);
        }
    }

    #[test]
    fn deadline_tick_from_nanos_rounds_the_complete_target_up() {
        let half_tick = TIMER_TICK_NS / 2;

        assert_eq!(deadline_tick_from_nanos(0, Duration::ZERO), 0);
        assert_eq!(deadline_tick_from_nanos(0, Duration::from_nanos(1)), 1);
        assert_eq!(
            deadline_tick_from_nanos(TIMER_TICK_NS, Duration::from_nanos(TIMER_TICK_NS)),
            2
        );
        assert_eq!(
            deadline_tick_from_nanos(TIMER_TICK_NS - 1, Duration::from_nanos(1)),
            1
        );
        assert_eq!(
            deadline_tick_from_nanos(TIMER_TICK_NS - 1, Duration::from_nanos(2)),
            2
        );
        assert_eq!(
            deadline_tick_from_nanos(
                TIMER_TICK_NS + half_tick,
                Duration::from_nanos(TIMER_TICK_NS),
            ),
            3
        );
        assert_eq!(
            deadline_tick_from_nanos(u64::MAX, Duration::from_secs(u64::MAX)),
            u64::MAX
        );
    }

    #[test]
    fn empty_timer_wheel_arm_never_moves_current_tick_backward() {
        let mut runtime = TimerRuntime::new().expect("timer runtime construction failed");
        runtime.init().expect("timer runtime init failed");
        assert!(!runtime.has_pending());

        runtime.wheel.current_tick = u64::MAX;
        let sampled_tick =
            runtime.sample_arm_nanos().expect("timer arm sample failed") / TIMER_TICK_NS;

        assert!(
            sampled_tick < u64::MAX,
            "test requires a sampled tick below the saturated wheel tick"
        );
        assert_eq!(runtime.wheel.current_tick, u64::MAX);
    }

    #[test]
    fn absolute_deadline_conversion_preserves_paired_base_and_rounding() {
        let mut runtime = TimerRuntime::new().expect("timer runtime construction failed");
        runtime.init().expect("timer runtime init failed");
        let instant = Instant::now();
        let base_nanos = 41 * TIMER_TICK_NS + TIMER_TICK_NS / 2;
        runtime.absolute_arm_base = Some(ArmBase {
            instant,
            nanos: base_nanos,
        });

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
                .deadline_tick_for_instant(instant + Duration::from_nanos(TIMER_TICK_NS / 2))
                .expect("deadline conversion failed"),
            Some(42)
        );
        assert_eq!(
            runtime
                .deadline_tick_for_instant(instant + Duration::from_nanos(TIMER_TICK_NS))
                .expect("deadline conversion failed"),
            Some(43)
        );
    }

    #[test]
    #[cfg(not(miri))]
    fn fractional_duration_deadline_does_not_fire_at_floored_boundary() {
        let mut executor = Executor::new().expect("failed to construct executor");

        executor
            .run(async {
                let mut runtime = TimerRuntime::new().expect("timer runtime construction failed");
                runtime.init().expect("timer runtime init failed");

                let arm_tick = 100;
                let arm_nanos = arm_tick * TIMER_TICK_NS + TIMER_TICK_NS / 2;
                let deadline_tick =
                    deadline_tick_from_nanos(arm_nanos, Duration::from_nanos(TIMER_TICK_NS));
                assert_eq!(deadline_tick, arm_tick + 2);

                runtime.wheel.current_tick = arm_tick;
                let entry = runtime
                    .submit_sleep_at_tick(std::ptr::null_mut(), deadline_tick)
                    .expect("fractional timer arm failed");

                assert!(
                    !runtime
                        .process_at_with_budget(arm_tick + 1, usize::MAX)
                        .expect("pre-deadline processing failed")
                );
                assert!(unsafe { (*entry).state == TimerState::Armed });

                assert!(
                    !runtime
                        .process_at_with_budget(deadline_tick, usize::MAX)
                        .expect("deadline processing failed")
                );
                assert!(unsafe { (*entry).state == TimerState::Fired });

                runtime
                    .cancel_sleep(entry)
                    .expect("fractional timer reclamation failed");
            })
            .expect("executor failed while checking fractional timer rounding");
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
    #[cfg(not(miri))]
    fn timer_runtime_collection_preserves_uncrossed_deadline_cache() {
        let mut executor = Executor::new().expect("failed to construct executor");

        executor
            .run(async {
                let mut runtime = TimerRuntime::new().expect("timer runtime construction failed");
                runtime.init().expect("timer runtime init failed");
                runtime.wheel.current_tick = 100;

                let earliest = runtime
                    .submit_sleep_at_tick(std::ptr::null_mut(), 105)
                    .expect("earliest timer arm failed");
                let later = runtime
                    .submit_sleep_at_tick(std::ptr::null_mut(), 120)
                    .expect("later timer arm failed");

                assert_eq!(runtime.wheel.next_deadline_tick, Some(105));
                assert!(!runtime.wheel.next_deadline_dirty);
                assert!(
                    !runtime
                        .process_at_with_budget(104, usize::MAX)
                        .expect("pre-deadline collection failed")
                );
                assert_eq!(runtime.wheel.next_deadline_tick, Some(105));
                assert!(
                    !runtime.wheel.next_deadline_dirty,
                    "processing before the cached deadline dirtied the cache"
                );
                assert_eq!(
                    runtime.next_wait_duration(104),
                    Some(Duration::from_nanos(TIMER_TICK_NS))
                );

                assert!(
                    !runtime
                        .process_at_with_budget(105, usize::MAX)
                        .expect("deadline collection failed")
                );
                assert!(
                    runtime.wheel.next_deadline_dirty,
                    "crossing the cached deadline left the cache clean"
                );
                assert_eq!(
                    runtime.next_wait_duration(105),
                    Some(Duration::from_nanos(15 * TIMER_TICK_NS))
                );
                assert_eq!(runtime.wheel.next_deadline_tick, Some(120));
                assert!(!runtime.wheel.next_deadline_dirty);

                runtime
                    .cancel_sleep(earliest)
                    .expect("earliest timer reclamation failed");
                runtime
                    .cancel_sleep(later)
                    .expect("later timer cancellation failed");
            })
            .expect("executor failed while checking timer deadline caching");
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
    fn timer_wheel_outer_cascade_stops_after_captured_tail() {
        let mut wheel = TimerWheel::new_uninit();
        init_wheel_at(&mut wheel, 0);
        let mut reinserted = timer_entry_at(u64::MAX);
        let mut captured_tail = timer_entry_at(u64::MAX);
        let mut appended = timer_entry_at(u64::MAX);
        let reinserted_ptr = &mut reinserted as *mut TimerEntry;
        let captured_tail_ptr = &mut captured_tail as *mut TimerEntry;
        let appended_ptr = &mut appended as *mut TimerEntry;
        let boundary_tick = 63u64 << 20;

        wheel.insert(reinserted_ptr);
        wheel.insert(captured_tail_ptr);

        wheel.current_tick = boundary_tick;
        wheel.begin_tick_cascade();
        assert_eq!(wheel.cascade_count, 3);
        assert_eq!(wheel.cascade_levels[2], 3);
        assert_eq!(wheel.cascade_indices[2], 63);

        assert_eq!(wheel.process_cascade_with_budget(1), 1);
        assert!(wheel.has_pending_cascade());
        wheel.insert(appended_ptr);
        assert_eq!(wheel.process_cascade_with_budget(8), 1);
        assert!(
            !wheel.has_pending_cascade(),
            "entries added beyond the captured tail were processed again"
        );
        assert_eq!(wheel.process_cascade_with_budget(1), 0);
        for entry in [&reinserted, &captured_tail, &appended] {
            assert_eq!(entry.deadline_tick, u64::MAX);
            assert_eq!(entry.bucket_level, 3);
            assert_eq!(entry.bucket_index, 63);
            assert!(entry.state == TimerState::Armed);
        }
        assert_eq!(
            unsafe { wheel.lvl3[63].front(TimerEntry::LINK_OFFSET) },
            Some(reinserted_ptr)
        );
        assert_eq!(
            wheel.lvl3[63].back_link(),
            Some(std::ptr::addr_of_mut!(captured_tail.link))
        );
        assert_eq!(reinserted.link.next, std::ptr::addr_of_mut!(appended.link));
        assert_eq!(
            appended.link.next,
            std::ptr::addr_of_mut!(captured_tail.link)
        );

        wheel.remove(reinserted_ptr);
        wheel.remove(captured_tail_ptr);
        wheel.remove(appended_ptr);
    }

    #[test]
    fn timer_wheel_outer_cascade_is_not_recaptured_before_tick_advances() {
        let mut wheel = TimerWheel::new_uninit();
        init_wheel_at(&mut wheel, 0);
        let boundary_tick = 63u64 << 20;
        let mut due = timer_entry_at(boundary_tick);
        let mut multi_rotation = timer_entry_at(u64::MAX);
        let due_ptr = &mut due as *mut TimerEntry;
        let multi_rotation_ptr = &mut multi_rotation as *mut TimerEntry;

        wheel.insert(due_ptr);
        wheel.insert(multi_rotation_ptr);
        assert_eq!(due.bucket_level, 3);
        assert_eq!(due.bucket_index, 63);
        assert_eq!(multi_rotation.bucket_level, 3);
        assert_eq!(multi_rotation.bucket_index, 63);

        wheel.current_tick = boundary_tick;
        wheel.begin_tick_cascade();
        assert_eq!(wheel.process_cascade_with_budget(2), 2);
        assert!(!wheel.has_pending_cascade());
        assert_eq!(due.bucket_level, 0);
        assert_eq!(due.bucket_index, 0);
        assert_eq!(multi_rotation.bucket_level, 3);
        assert_eq!(multi_rotation.bucket_index, 63);

        // Model the next executor pass after the cascade consumed the entire
        // phase budget: the due level-0 entry keeps the wheel on this tick.
        wheel.begin_tick_cascade();
        assert!(
            !wheel.has_pending_cascade(),
            "deferred outer entries were recaptured on the same tick"
        );
        assert_eq!(multi_rotation.bucket_level, 3);
        assert_eq!(multi_rotation.bucket_index, 63);

        wheel.current_tick = 127u64 << 20;
        wheel.begin_tick_cascade();
        assert_eq!(wheel.process_cascade_with_budget(1), 1);
        assert!(!wheel.has_pending_cascade());
        assert_eq!(multi_rotation.bucket_level, 3);
        assert_eq!(multi_rotation.bucket_index, 63);

        wheel.remove(due_ptr);
        wheel.remove(multi_rotation_ptr);
    }

    #[test]
    fn timer_wheel_outer_cascade_history_does_not_suppress_future_bucket() {
        let mut wheel = TimerWheel::new_uninit();
        init_wheel_at(&mut wheel, 0);
        let mut due = timer_entry_at(0);
        let mut multi_rotation = timer_entry_at(1u64 << 26);
        let mut future = timer_entry_at(1u64 << 8);
        let due_ptr = &mut due as *mut TimerEntry;
        let multi_rotation_ptr = &mut multi_rotation as *mut TimerEntry;
        let future_ptr = &mut future as *mut TimerEntry;

        wheel.insert(due_ptr);
        wheel.insert(multi_rotation_ptr);
        wheel.insert(future_ptr);
        assert_eq!(due.bucket_level, 0);
        assert_eq!(multi_rotation.bucket_level, 3);
        assert_eq!(multi_rotation.bucket_index, 0);
        assert_eq!(future.bucket_level, 1);
        assert_eq!(future.bucket_index, 1);

        wheel.begin_tick_cascade();
        assert_eq!(wheel.process_cascade_with_budget(1), 1);
        assert!(!wheel.has_pending_cascade());
        assert_eq!(wheel.cascade_started_tick, 0);
        assert!(wheel.cascade_started_tick_valid);

        // Model cancellation between budgeted passes: only a future upper
        // bucket remains, just beyond this pass's target.
        wheel.remove(due_ptr);
        wheel.remove(multi_rotation_ptr);
        assert!(wheel.skip_empty_ticks_until_next_work(255));
        assert_eq!(wheel.current_tick, 256);

        wheel.begin_tick_cascade();
        assert!(wheel.has_pending_cascade());
        assert_eq!(wheel.cascade_levels[0], 1);
        assert_eq!(wheel.cascade_indices[0], 1);
        assert_eq!(wheel.process_cascade_with_budget(1), 1);
        assert_eq!(future.bucket_level, 0);
        assert_eq!(future.bucket_index, 0);

        wheel.remove(future_ptr);
    }

    #[test]
    fn timer_wheel_outer_cascade_cancelled_tail_moves_boundary_backward() {
        let task = TaskHeader::new();
        let task_ptr = &task as *const TaskHeader as *mut TaskHeader;
        let mut runtime = TimerRuntime::new().expect("timer runtime construction failed");
        runtime.init().expect("timer runtime init failed");
        runtime.wheel.current_tick = 0;
        let reinserted_ptr = runtime
            .submit_sleep_at_tick(task_ptr, u64::MAX)
            .expect("first outer timer arm failed");
        let preceding_ptr = runtime
            .submit_sleep_at_tick(task_ptr, u64::MAX)
            .expect("second outer timer arm failed");
        let captured_tail_ptr = runtime
            .submit_sleep_at_tick(task_ptr, u64::MAX)
            .expect("captured-tail timer arm failed");
        assert_eq!(task.refs.get(), 4);

        runtime.wheel.current_tick = 63u64 << 20;
        runtime.wheel.begin_tick_cascade();
        assert_eq!(runtime.wheel.process_cascade_with_budget(1), 1);
        assert_eq!(task.refs.get(), 4);

        runtime
            .cancel_sleep(captured_tail_ptr)
            .expect("captured-tail cancellation failed");
        assert_eq!(task.refs.get(), 3);
        let replacement_ptr = runtime
            .submit_sleep_at_tick(task_ptr, u64::MAX)
            .expect("replacement outer timer arm failed");
        assert_eq!(
            replacement_ptr, captured_tail_ptr,
            "timer pool did not immediately reuse the cancelled boundary slot"
        );
        assert_eq!(task.refs.get(), 4);

        assert_eq!(runtime.wheel.process_cascade_with_budget(8), 1);
        assert!(
            !runtime.wheel.has_pending_cascade(),
            "cancelled tail left the preceding original entry unbounded"
        );
        for entry_ptr in [reinserted_ptr, preceding_ptr, replacement_ptr] {
            assert_eq!(unsafe { (*entry_ptr).bucket_level }, 3);
            assert_eq!(unsafe { (*entry_ptr).bucket_index }, 63);
            assert!(unsafe { (*entry_ptr).state } == TimerState::Armed);
        }
        assert_eq!(task.refs.get(), 4);

        runtime
            .cancel_sleep(reinserted_ptr)
            .expect("reinserted timer cancellation failed");
        runtime
            .cancel_sleep(preceding_ptr)
            .expect("preceding timer cancellation failed");
        runtime
            .cancel_sleep(replacement_ptr)
            .expect("replacement timer cancellation failed");
        assert_eq!(task.refs.get(), 1);
        assert!(!runtime.wheel.has_pending_entries());
    }

    #[test]
    fn timer_wheel_outer_cascade_cancelled_front_tail_clears_boundary() {
        let mut wheel = TimerWheel::new_uninit();
        init_wheel_at(&mut wheel, 0);
        let mut reinserted = timer_entry_at(u64::MAX);
        let mut captured_tail = timer_entry_at(u64::MAX);
        let reinserted_ptr = &mut reinserted as *mut TimerEntry;
        let captured_tail_ptr = &mut captured_tail as *mut TimerEntry;

        wheel.insert(reinserted_ptr);
        wheel.insert(captured_tail_ptr);
        wheel.current_tick = 63u64 << 20;
        wheel.begin_tick_cascade();

        assert_eq!(wheel.process_cascade_with_budget(1), 1);
        assert!(wheel.has_pending_cascade());
        assert_eq!(
            unsafe { wheel.lvl3[63].front(TimerEntry::LINK_OFFSET) },
            Some(captured_tail_ptr)
        );

        wheel.remove(captured_tail_ptr);
        assert_eq!(
            wheel.process_cascade_with_budget(1),
            0,
            "entry appended beyond the cancelled boundary was reprocessed"
        );
        assert!(!wheel.has_pending_cascade());
        assert_eq!(reinserted.bucket_level, 3);
        assert_eq!(reinserted.bucket_index, 63);

        wheel.remove(reinserted_ptr);
    }

    #[test]
    fn timer_wheel_outer_cascade_preserves_saturating_deadline_rotations() {
        let mut wheel = TimerWheel::new_uninit();
        init_wheel_at(&mut wheel, 0);
        let mut entry = timer_entry_at(u64::MAX);
        let entry_ptr = &mut entry as *mut TimerEntry;

        wheel.insert(entry_ptr);
        for boundary_tick in [63u64 << 20, 127u64 << 20] {
            wheel.current_tick = boundary_tick;
            wheel.begin_tick_cascade();
            assert_eq!(wheel.process_cascade_with_budget(1), 1);
            assert!(!wheel.has_pending_cascade());
            assert_eq!(entry.deadline_tick, u64::MAX);
            assert_eq!(entry.bucket_level, 3);
            assert_eq!(entry.bucket_index, 63);
            assert!(entry.state == TimerState::Armed);
        }

        wheel.current_tick = !((1u64 << 20) - 1);
        wheel.begin_tick_cascade();
        assert_eq!(wheel.process_cascade_with_budget(1), 1);
        assert_eq!(entry.bucket_level, 2);
        assert_eq!(entry.bucket_index, 63);

        wheel.current_tick = !((1u64 << 14) - 1);
        wheel.begin_tick_cascade();
        assert_eq!(wheel.process_cascade_with_budget(1), 1);
        assert_eq!(entry.bucket_level, 1);
        assert_eq!(entry.bucket_index, 63);

        wheel.current_tick = !((1u64 << 8) - 1);
        wheel.begin_tick_cascade();
        assert_eq!(wheel.process_cascade_with_budget(1), 1);
        assert_eq!(entry.deadline_tick, u64::MAX);
        assert_eq!(entry.bucket_level, 0);
        assert_eq!(entry.bucket_index, 255);
        assert!(entry.state == TimerState::Armed);

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
    fn timer_runtime_pool_provider_survives_arm_cancel_reuse_under_miri() {
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
        assert!(
            unsafe { (*entry).state == TimerState::Armed },
            "wheel insertion must publish the armed state"
        );
        runtime
            .cancel_sleep(entry)
            .expect("canceling test timer failed");

        let reused = runtime
            .submit_sleep_at_tick(std::ptr::null_mut(), deadline)
            .expect("rearming test timer failed");
        assert_eq!(reused, entry, "timer pool did not reuse the canceled slot");
        assert!(
            unsafe { (*reused).state == TimerState::Armed },
            "reused timer entry must publish the armed state"
        );
        runtime
            .cancel_sleep(reused)
            .expect("canceling reused test timer failed");
    }

    #[test]
    fn timer_expiry_notified_flag_coalesces_same_waiter() {
        let mut runtime = TimerRuntime::new().expect("timer runtime construction failed");
        runtime.init().expect("timer runtime init failed");
        let deadline = runtime.wheel.current_tick;
        let mut waiter = TaskHeader::new();
        let waiter_ptr = std::ptr::addr_of_mut!(waiter);
        let entries = [
            runtime
                .submit_sleep_at_tick(waiter_ptr, deadline)
                .expect("first timer arm failed"),
            runtime
                .submit_sleep_at_tick(waiter_ptr, deadline)
                .expect("second timer arm failed"),
        ];
        assert_eq!(waiter.refs.get(), 3);

        let mut ready_queue = DList::<TaskHeader>::new_uninit();
        ready_queue.init();
        let mut runtime_state = RuntimeState {
            live_tasks: 0,
            inflight_ops: 0,
            #[cfg(debug_assertions)]
            stats: crate::runtime::executor::RuntimeStats::default(),
        };
        let schedule_ctx = ScheduleCtx {
            ready_queue: std::ptr::addr_of_mut!(ready_queue),
            runtime_state: std::ptr::addr_of_mut!(runtime_state),
        };

        let pending = unsafe {
            TimerRuntime::collect_expired_unchecked(
                std::ptr::addr_of_mut!(runtime),
                deadline,
                usize::MAX,
                schedule_ctx,
            )
        };

        assert!(!pending);
        assert!(!runtime.has_pending());
        assert_eq!(waiter.refs.get(), 1);
        assert!(waiter.has_flag(TaskHeader::FLAG_NOTIFIED));
        assert!(waiter.has_flag(TaskHeader::FLAG_QUEUED));
        #[cfg(debug_assertions)]
        assert_eq!(runtime_state.stats.task_schedules, 1);
        assert_eq!(
            unsafe { ready_queue.pop_front(TaskHeader::READY_LINK_OFFSET) },
            Some(waiter_ptr)
        );
        assert!(ready_queue.is_empty());

        for entry in entries {
            assert!(unsafe { (*entry).state == TimerState::Fired });
            assert!(unsafe { (*entry).waiter.is_null() });
            runtime
                .cancel_sleep(entry)
                .expect("fired timer reclaim failed");
        }
    }

    #[test]
    fn timer_expiry_waiter_destructor_can_cancel_the_just_fired_entry() {
        TIMER_REENTRY_DESTROYS.with(|destroys| destroys.set(0));
        let mut runtime = TimerRuntime::new().expect("timer runtime construction failed");
        runtime.init().expect("timer runtime init failed");
        let deadline = runtime.wheel.current_tick;
        let mut waiter = TaskHeader::new();
        waiter.vtable = &REENTER_TIMER_WAITER_VTABLE;
        waiter.set_flag(TaskHeader::FLAG_COMPLETED);
        let waiter_ptr = &mut waiter as *mut TaskHeader;
        let entry = runtime
            .submit_sleep_at_tick(waiter_ptr, deadline)
            .expect("timer arm failed");
        let runtime_ptr = std::ptr::addr_of_mut!(runtime);
        TIMER_REENTRY_RUNTIME.with(|stored| stored.set(runtime_ptr));
        TIMER_REENTRY_ENTRY.with(|entry_ptr| entry_ptr.set(entry));

        // Leave the armed timer's waiter as the final task reference. Expiry
        // releases it after scheduling observes the terminal task state.
        unsafe {
            release_task(waiter_ptr);
        }
        assert_eq!(waiter.refs.get(), 1);

        let mut ready_queue = DList::<TaskHeader>::new_uninit();
        ready_queue.init();
        let mut runtime_state = RuntimeState {
            live_tasks: 0,
            inflight_ops: 0,
            #[cfg(debug_assertions)]
            stats: crate::runtime::executor::RuntimeStats::default(),
        };
        let schedule_ctx = ScheduleCtx {
            ready_queue: &mut ready_queue,
            runtime_state: &mut runtime_state,
        };
        let pending = unsafe {
            TimerRuntime::collect_expired_unchecked(runtime_ptr, deadline, usize::MAX, schedule_ctx)
        };
        TIMER_REENTRY_ENTRY.with(|entry_ptr| entry_ptr.set(std::ptr::null_mut()));
        TIMER_REENTRY_RUNTIME.with(|runtime_ptr| runtime_ptr.set(std::ptr::null_mut()));

        assert!(!pending);
        assert!(ready_queue.is_empty());
        assert!(!runtime.has_pending());
        TIMER_REENTRY_DESTROYS.with(|destroys| assert_eq!(destroys.get(), 1));

        let replacement_deadline = runtime.wheel.current_tick.saturating_add(1);
        let replacement = runtime
            .submit_sleep_at_tick(std::ptr::null_mut(), replacement_deadline)
            .expect("replacement timer arm failed");
        assert_eq!(
            replacement, entry,
            "reentrant cancellation did not return the fired timer slot"
        );
        runtime
            .cancel_sleep(replacement)
            .expect("replacement timer cancellation failed");
    }

    #[test]
    fn timer_shutdown_drain_completes_all_buckets_before_resuming_first_panic() {
        TIMER_DRAIN_DESTROYS.with(|destroys| destroys.set(0));
        let mut runtime = TimerRuntime::new().expect("timer runtime construction failed");
        runtime.init().expect("timer runtime init failed");
        runtime.wheel.current_tick = 0;
        let deadlines = [
            10,
            10,
            1u64 << LVL1_SHIFT,
            1u64 << LVL2_SHIFT,
            1u64 << LVL3_SHIFT,
        ];
        let mut first_waiter = TaskHeader::new();
        first_waiter.vtable = &PANIC_TIMER_WAITER_VTABLE;
        let first_waiter_ptr = &mut first_waiter as *mut TaskHeader;
        let mut same_bucket_waiter = TaskHeader::new();
        let same_bucket_waiter_ptr = &mut same_bucket_waiter as *mut TaskHeader;
        let mut level_one_waiter = TaskHeader::new();
        let level_one_waiter_ptr = &mut level_one_waiter as *mut TaskHeader;
        let mut later_panic_waiter = TaskHeader::new();
        later_panic_waiter.vtable = &LATER_PANIC_TIMER_WAITER_VTABLE;
        let later_panic_waiter_ptr = &mut later_panic_waiter as *mut TaskHeader;
        let mut level_three_waiter = TaskHeader::new();
        let level_three_waiter_ptr = &mut level_three_waiter as *mut TaskHeader;

        let waiter_ptrs = [
            first_waiter_ptr,
            same_bucket_waiter_ptr,
            level_one_waiter_ptr,
            later_panic_waiter_ptr,
            level_three_waiter_ptr,
        ];
        let entries = std::array::from_fn(|index| {
            runtime
                .submit_sleep_at_tick(waiter_ptrs[index], deadlines[index])
                .expect("shutdown timer arm failed")
        });
        assert_eq!(
            entries.map(|entry| unsafe { (*entry).bucket_level }),
            [0, 0, 1, 2, 3]
        );
        assert_eq!(unsafe { (*entries[0]).bucket_index }, unsafe {
            (*entries[1]).bucket_index
        });
        runtime.wheel.current_tick = 1u64 << LVL3_SHIFT;
        runtime.wheel.begin_tick_cascade();
        assert_eq!(runtime.wheel.cascade_count, 3);
        assert!(!runtime.wheel.outer_cascade_tail.is_null());
        runtime.absolute_arm_base = Some(ArmBase {
            instant: Instant::now(),
            nanos: 1,
        });

        unsafe { release_task(first_waiter_ptr) };
        unsafe { release_task(later_panic_waiter_ptr) };
        assert_eq!(same_bucket_waiter.refs.get(), 2);
        assert_eq!(level_one_waiter.refs.get(), 2);
        assert_eq!(level_three_waiter.refs.get(), 2);

        let runtime_ptr = std::ptr::addr_of_mut!(runtime);
        let unwind = catch_unwind(AssertUnwindSafe(|| unsafe {
            TimerRuntime::cancel_all_for_shutdown_unchecked(runtime_ptr);
        }))
        .expect_err("final waiter release did not panic");
        assert!(
            unwind.downcast_ref::<TimerDrainWaiterPanic>().is_some(),
            "timer shutdown replaced the waiter-destroy panic"
        );
        TIMER_DRAIN_DESTROYS.with(|destroys| assert_eq!(destroys.get(), 2));
        for entry in entries {
            assert!(unsafe { (*entry).link.is_unlinked() });
            assert!(unsafe { (*entry).waiter.is_null() });
            assert!(unsafe { (*entry).state == TimerState::Cancelled });
            assert_eq!(unsafe { (*entry).bucket_level }, INVALID_BUCKET_LEVEL);
        }
        assert!(!runtime.has_pending());
        assert!(runtime.wheel.lvl0.iter().all(DList::is_empty));
        assert!(runtime.wheel.lvl1.iter().all(DList::is_empty));
        assert!(runtime.wheel.lvl2.iter().all(DList::is_empty));
        assert!(runtime.wheel.lvl3.iter().all(DList::is_empty));
        assert_eq!(runtime.wheel.lvl0_bits, [0; 4]);
        assert_eq!(runtime.wheel.lvl1_bits, 0);
        assert_eq!(runtime.wheel.lvl2_bits, 0);
        assert_eq!(runtime.wheel.lvl3_bits, 0);
        assert_eq!(runtime.wheel.next_deadline_tick, None);
        assert!(!runtime.wheel.next_deadline_dirty);
        assert_eq!(runtime.wheel.cascade_count, 0);
        assert_eq!(runtime.wheel.cascade_pos, 0);
        assert_eq!(runtime.wheel.cascade_started_tick, 0);
        assert!(!runtime.wheel.cascade_started_tick_valid);
        assert!(runtime.wheel.outer_cascade_tail.is_null());
        assert!(runtime.absolute_arm_base.is_none());
        assert_eq!(same_bucket_waiter.refs.get(), 1);
        assert_eq!(level_one_waiter.refs.get(), 1);
        assert_eq!(level_three_waiter.refs.get(), 1);

        for entry in entries {
            runtime
                .cancel_sleep(entry)
                .expect("detached timer reclaim failed");
        }
        let replacements = deadlines.map(|deadline| {
            runtime
                .submit_sleep_at_tick(std::ptr::null_mut(), deadline)
                .expect("replacement timer arm failed")
        });
        assert_eq!(
            replacements,
            [entries[4], entries[3], entries[2], entries[1], entries[0]],
            "shutdown recovery did not return every timer slot exactly once"
        );
        for replacement in replacements {
            runtime
                .cancel_sleep(replacement)
                .expect("replacement timer cancel failed");
        }
        unsafe { release_task(same_bucket_waiter_ptr) };
        unsafe { release_task(level_one_waiter_ptr) };
        unsafe { release_task(level_three_waiter_ptr) };
    }

    #[test]
    fn timer_shutdown_drain_allows_waiter_reentry_before_resuming_panic() {
        TIMER_REENTRY_DESTROYS.with(|destroys| destroys.set(0));
        let mut runtime = TimerRuntime::new().expect("timer runtime construction failed");
        runtime.init().expect("timer runtime init failed");
        runtime.wheel.current_tick = 0;

        let mut reenter_waiter = TaskHeader::new();
        reenter_waiter.vtable = &REENTER_PANIC_TIMER_WAITER_VTABLE;
        let reenter_waiter_ptr = std::ptr::addr_of_mut!(reenter_waiter);
        let mut target_waiter = TaskHeader::new();
        let target_waiter_ptr = std::ptr::addr_of_mut!(target_waiter);
        let mut later_waiter = TaskHeader::new();
        let later_waiter_ptr = std::ptr::addr_of_mut!(later_waiter);
        let mut upper_waiter = TaskHeader::new();
        let upper_waiter_ptr = std::ptr::addr_of_mut!(upper_waiter);

        let reenter_entry = runtime
            .submit_sleep_at_tick(reenter_waiter_ptr, 10)
            .expect("reentrant shutdown timer arm failed");
        let target_entry = runtime
            .submit_sleep_at_tick(target_waiter_ptr, 10)
            .expect("reentrant target timer arm failed");
        let later_entry = runtime
            .submit_sleep_at_tick(later_waiter_ptr, 10)
            .expect("later shutdown timer arm failed");
        let upper_entry = runtime
            .submit_sleep_at_tick(upper_waiter_ptr, 1u64 << LVL1_SHIFT)
            .expect("upper shutdown timer arm failed");
        let runtime_ptr = std::ptr::addr_of_mut!(runtime);
        TIMER_REENTRY_RUNTIME.with(|stored| stored.set(runtime_ptr));
        TIMER_REENTRY_ENTRY.with(|stored| stored.set(target_entry));

        unsafe { release_task(reenter_waiter_ptr) };
        assert_eq!(target_waiter.refs.get(), 2);
        assert_eq!(later_waiter.refs.get(), 2);
        assert_eq!(upper_waiter.refs.get(), 2);

        let unwind = catch_unwind(AssertUnwindSafe(|| unsafe {
            TimerRuntime::cancel_all_for_shutdown_unchecked(runtime_ptr);
        }))
        .expect_err("reentrant waiter destruction did not panic");
        TIMER_REENTRY_ENTRY.with(|stored| stored.set(std::ptr::null_mut()));
        TIMER_REENTRY_RUNTIME.with(|stored| stored.set(std::ptr::null_mut()));

        assert!(
            unwind
                .downcast_ref::<ReentrantTimerDrainWaiterPanic>()
                .is_some(),
            "timer shutdown replaced the reentrant waiter panic"
        );
        TIMER_REENTRY_DESTROYS.with(|destroys| assert_eq!(destroys.get(), 1));
        for entry in [reenter_entry, later_entry, upper_entry] {
            assert!(unsafe { (*entry).link.is_unlinked() });
            assert!(unsafe { (*entry).waiter.is_null() });
            assert!(unsafe { (*entry).state == TimerState::Cancelled });
            assert_eq!(unsafe { (*entry).bucket_level }, INVALID_BUCKET_LEVEL);
        }
        assert!(!runtime.has_pending());
        assert_eq!(target_waiter.refs.get(), 1);
        assert_eq!(later_waiter.refs.get(), 1);
        assert_eq!(upper_waiter.refs.get(), 1);

        for entry in [reenter_entry, later_entry, upper_entry] {
            runtime
                .cancel_sleep(entry)
                .expect("detached reentrant timer reclaim failed");
        }
        let replacements = [10, 10, 10, 1u64 << LVL1_SHIFT].map(|deadline| {
            runtime
                .submit_sleep_at_tick(std::ptr::null_mut(), deadline)
                .expect("reentrant replacement timer arm failed")
        });
        assert_eq!(
            replacements,
            [upper_entry, later_entry, reenter_entry, target_entry],
            "reentrant shutdown recovery did not return every timer slot exactly once"
        );
        for replacement in replacements {
            runtime
                .cancel_sleep(replacement)
                .expect("reentrant replacement timer cancel failed");
        }

        unsafe { release_task(target_waiter_ptr) };
        unsafe { release_task(later_waiter_ptr) };
        unsafe { release_task(upper_waiter_ptr) };
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
