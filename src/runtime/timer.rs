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
//! Best fast-path-adjacent choices:
//! - Use timers for control-path sleeps, deadlines, and connect/setup
//!   timeouts.
//! - Most applications should use the top-level [`sleep`], [`sleep_until`],
//!   [`timeout`], and [`timeout_at`] helpers rather than interacting with
//!   [`TimerRuntime`] directly.
//!
//! Prefer not to use on the fast path:
//! - Prefer not to wrap every tiny steady-state I/O step in its own timer
//!   when a surrounding protocol loop can track deadlines more coarsely.
//!   Per-step timers are not the crate's data-plane fast path.
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
    current_poll_owner_task_unchecked, next_timer_wake_epoch_unchecked, note_timer_expired,
    note_timer_now_tick_call, note_waiter_wake, schedule_ctx_unchecked,
    schedule_timer_woken_task_unchecked,
};
use crate::runtime::task::TaskHeader;
use crate::utils::list::intrusive::dlist::{DList, Link};
use crate::utils::memory::pool::{InPlaceInit, Pool};
use crate::utils::memory::provider::BasicMemoryProvider;
use std::array;
use std::fmt;
use std::future::Future;
use std::io;
use std::mem::{ManuallyDrop, MaybeUninit};
use std::pin::Pin;
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
}

#[repr(C)]
pub(crate) struct TimerEntry {
    /// Intrusive bucket link while the timer is queued in the wheel.
    link: Link,
    /// Task to wake when the timer expires.
    waiter: *mut TaskHeader,
    /// Absolute deadline expressed in timer ticks.
    deadline_tick: u64,
    /// Current timer lifecycle state.
    state: TimerState,
    /// Wheel level currently owning this entry, or `INVALID_BUCKET_LEVEL`.
    bucket_level: u8,
    /// Bucket index within the owning level.
    bucket_index: u16,
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
        }
    }

    fn clear_waiter(&mut self) {
        self.waiter = std::ptr::null_mut();
    }
}

impl InPlaceInit for TimerEntry {
    type Args = ();

    fn init_at(slot: &mut MaybeUninit<Self>, _: Self::Args) {
        slot.write(TimerEntry::new());
    }
}

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

    fn next_nonempty_lvln_bucket(bits: u64, start: usize) -> Option<usize> {
        Self::next_set_bit(bits, start).or_else(|| Self::next_set_bit(bits, 0))
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
        self.candidate_deadline(0)
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

    // Recompute the global nearest deadline from the nearest non-empty bucket
    // at each level. The occupancy bitsets keep this bounded even after
    // heavy cancellation invalidates the cached nearest deadline.
    fn recompute_next_deadline(&mut self) {
        self.next_deadline_tick = self
            .candidate_deadline(0)
            .into_iter()
            .chain(self.candidate_deadline(1))
            .chain(self.candidate_deadline(2))
            .chain(self.candidate_deadline(3))
            .min();
    }

    fn candidate_deadline(&self, level: u8) -> Option<u64> {
        let index = match level {
            0 => self.next_nonempty_lvl0_bucket()?,
            1 => Self::next_nonempty_lvln_bucket(
                self.lvl1_bits,
                ((self.current_tick >> 8) & ((LVLN_SLOTS as u64) - 1)) as usize,
            )?,
            2 => Self::next_nonempty_lvln_bucket(
                self.lvl2_bits,
                ((self.current_tick >> 14) & ((LVLN_SLOTS as u64) - 1)) as usize,
            )?,
            _ => Self::next_nonempty_lvln_bucket(
                self.lvl3_bits,
                ((self.current_tick >> 20) & ((LVLN_SLOTS as u64) - 1)) as usize,
            )?,
        };

        let entry = unsafe {
            match level {
                0 => self.lvl0[index].front(TimerEntry::LINK_OFFSET)?,
                1 => self.lvl1[index].front(TimerEntry::LINK_OFFSET)?,
                2 => self.lvl2[index].front(TimerEntry::LINK_OFFSET)?,
                _ => self.lvl3[index].front(TimerEntry::LINK_OFFSET)?,
            }
        };
        Some(unsafe { (*entry).deadline_tick })
    }
}

/// Runtime-owned timer subsystem.
///
/// Logical timers live entirely in user space. The timer wheel computes the
/// next wake deadline and the executor uses that deadline to bound how long it
/// may block in the reactor.
///
/// Most applications should use the free functions in this module. This type
/// is public for explicit runtime integration and testing rather than as the
/// normal application fast-path entry point. Creating and initializing a
/// timer runtime is setup work; timer arming/canceling happens through the
/// executor-owned runtime.
///
/// # Example
/// ```no_run
/// use flowio::runtime::timer::TimerRuntime;
///
/// let mut timers = TimerRuntime::new()?;
/// timers.init()?;
/// let _tick = timers.now_tick()?;
/// # Ok::<(), std::io::Error>(())
/// ```
pub struct TimerRuntime {
    /// Stable provider backing the timer-entry pool.
    _provider: Box<BasicMemoryProvider>,
    /// Pool of runtime-owned timer entries.
    timer_pool: ManuallyDrop<Pool<'static, TimerEntry, BasicMemoryProvider>>,
    /// Hierarchical timing wheel used to organize deadlines.
    wheel: TimerWheel,
    /// Most recent monotonic tick sample seen during executor processing.
    cached_now_tick: u64,
    /// Per-pass base used to convert relative and absolute deadlines
    /// consistently without repeated clock reads.
    arm_base: Option<ArmBase>,
}

#[derive(Clone, Copy)]
struct ArmBase {
    /// Instant sampled at the start of the current arm pass.
    instant: Instant,
    /// Tick value corresponding to `instant`, reused for all sleeps armed in
    /// the same executor pass.
    tick: u64,
}

impl TimerRuntime {
    #[allow(clippy::new_without_default)]
    /// Creates a timer runtime in an uninitialized state.
    ///
    /// Call [`TimerRuntime::init`] after moving it to its final memory
    /// location.
    pub fn new() -> io::Result<Self> {
        let mut provider = Box::new(BasicMemoryProvider::new());
        let provider_ptr = &mut *provider as *mut BasicMemoryProvider;
        Ok(Self {
            _provider: provider,
            timer_pool: ManuallyDrop::new(
                Pool::new_uninit(unsafe { &mut *provider_ptr }, TIMERS_PER_SLAB)
                    .map_err(|_| io::Error::from(io::ErrorKind::InvalidInput))?,
            ),
            wheel: TimerWheel::new_uninit(),
            cached_now_tick: 0,
            arm_base: None,
        })
    }

    /// Initializes the timer pool and timing wheel.
    pub fn init(&mut self) -> io::Result<()> {
        self.timer_pool.init();
        self.wheel.init()?;
        self.cached_now_tick = self.wheel.current_tick;
        self.arm_base = None;
        Ok(())
    }

    /// Samples and returns the current monotonic timer tick.
    pub fn now_tick(&mut self) -> io::Result<u64> {
        let now = now_tick()?;
        self.cached_now_tick = now;
        Ok(now)
    }

    #[inline(always)]
    pub(crate) fn begin_executor_pass(&mut self) {
        self.arm_base = None;
    }

    /// Creates a duration-based sleep future.
    ///
    /// The receiver is not captured; the future binds to the active
    /// executor's timer runtime when it is first polled.
    ///
    /// Most callers will prefer the top-level [`sleep`] helper.
    #[doc(hidden)]
    pub fn sleep(&mut self, duration: Duration) -> Sleep {
        Sleep::new_duration(duration)
    }

    /// Creates a deadline-based sleep future.
    ///
    /// The receiver is not captured; the future binds to the active
    /// executor's timer runtime when it is first polled.
    ///
    /// Most callers will prefer the top-level [`sleep_until`] helper.
    #[doc(hidden)]
    pub fn sleep_until(&mut self, deadline: Instant) -> Sleep {
        Sleep::new_deadline(deadline)
    }

    fn submit_sleep_at_tick(
        &mut self,
        task: *mut TaskHeader,
        deadline_tick: u64,
    ) -> io::Result<*mut TimerEntry> {
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
            (*entry).waiter = task;
            (*entry).deadline_tick = deadline_tick;
            (*entry).state = TimerState::Armed;
        }
        self.wheel.insert(entry);
        Ok(entry)
    }

    fn arm_base(&mut self) -> io::Result<ArmBase> {
        if let Some(base) = self.arm_base {
            return Ok(base);
        }

        // Sample once per executor loop pass. Relative sleeps reuse the same
        // base tick, and absolute deadlines convert against the matching
        // Instant sample so both paths stay consistent without a clock read
        // per armed timer.
        let instant = Instant::now();
        let tick = now_tick()?;
        self.cached_now_tick = tick;

        // When the wheel is empty, its current tick may be arbitrarily stale
        // after a long timer-free idle period. Snap it forward so newly armed
        // sleeps do not start from an old baseline and later timer processing
        // does not need to burn budget catching up empty ticks.
        if !self.has_pending() {
            self.wheel.current_tick = tick;
        }

        let base = ArmBase { instant, tick };
        self.arm_base = Some(base);
        Ok(base)
    }

    fn deadline_tick_for_duration(&mut self, duration: Duration) -> io::Result<u64> {
        let ticks = duration_to_ticks(duration);
        let base = self.arm_base()?;
        Ok(base.tick.saturating_add(ticks))
    }

    fn deadline_tick_for_instant(&mut self, deadline: Instant) -> io::Result<Option<u64>> {
        let base = self.arm_base()?;
        if deadline <= base.instant {
            return Ok(None);
        }

        let delta = deadline.duration_since(base.instant);
        Ok(Some(base.tick.saturating_add(duration_to_ticks(delta))))
    }

    pub(crate) fn submit_current_sleep_duration(
        &mut self,
        duration: Duration,
    ) -> io::Result<*mut TimerEntry> {
        // The owning task is taken from the active poll frame and stored once
        // in the timer entry. Expiry later wakes that task directly.
        let task = unsafe { current_poll_owner_task_unchecked() };
        let deadline_tick = self.deadline_tick_for_duration(duration)?;
        self.submit_sleep_at_tick(task, deadline_tick)
    }

    pub(crate) fn submit_current_sleep_deadline(
        &mut self,
        deadline: Instant,
    ) -> io::Result<Option<*mut TimerEntry>> {
        let task = unsafe { current_poll_owner_task_unchecked() };
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
    pub fn process_at_with_budget(&mut self, now: u64, budget: usize) -> io::Result<bool> {
        self.cached_now_tick = now;
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
                    let waiter = (*entry_ptr).waiter;
                    if !waiter.is_null() {
                        note_timer_expired();
                        note_waiter_wake();
                        schedule_timer_woken_task_unchecked(
                            waiter,
                            schedule_ctx.ready_queue,
                            schedule_ctx.runtime_state,
                            wake_epoch,
                        );
                        (*entry_ptr).waiter = std::ptr::null_mut();
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
    let rc = unsafe { libc::clock_gettime(libc::CLOCK_MONOTONIC, &mut ts) };
    if rc < 0 {
        return Err(io::Error::last_os_error());
    }

    Ok(((ts.tv_sec as u64) * 1_000_000_000u64 + (ts.tv_nsec as u64)) / TIMER_TICK_NS)
}

#[inline(always)]
unsafe fn refresh_sleep_waiter(entry: *mut TimerEntry) {
    debug_assert!(
        !entry.is_null(),
        "cannot refresh waiter for a missing timer entry"
    );
    if entry.is_null() {
        return;
    }
    unsafe {
        (*entry).waiter = current_poll_owner_task_unchecked();
    }
}

/// One-shot sleep future scheduled by the runtime timer wheel.
///
/// Constructed by [`sleep`], [`sleep_until`], or the corresponding
/// [`TimerRuntime`] methods.
///
/// This is a control-path timer primitive. Prefer arming deadlines around
/// larger protocol phases instead of wrapping every data-path read/write step
/// with a separate sleep or timeout.
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
    pub fn new_duration(duration: Duration) -> Self {
        Self {
            duration: Some(duration),
            deadline: None,
            entry: std::ptr::null_mut(),
        }
    }

    /// Creates a sleep that will arm from an absolute deadline on first poll.
    pub fn new_deadline(deadline: Instant) -> Self {
        Self {
            duration: None,
            deadline: Some(deadline),
            entry: std::ptr::null_mut(),
        }
    }
}

/// Error returned when a future exceeds its configured deadline.
///
/// # Example
/// ```
/// use flowio::runtime::timer::Elapsed;
///
/// let elapsed = Elapsed;
/// assert_eq!(elapsed.to_string(), "runtime timer elapsed");
/// ```
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Elapsed;

impl fmt::Display for Elapsed {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("runtime timer elapsed")
    }
}

impl std::error::Error for Elapsed {}

impl Future for Sleep {
    type Output = io::Result<()>;

    fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = unsafe { self.get_unchecked_mut() };
        let timers = unsafe { crate::runtime::executor::timers_unchecked() };

        if !this.entry.is_null() {
            let state = unsafe { (*this.entry).state };
            if state == TimerState::Fired {
                unsafe {
                    (*this.entry).state = TimerState::Idle;
                    (*timers).timer_pool.free(this.entry);
                }
                this.entry = std::ptr::null_mut();
                return Poll::Ready(Ok(()));
            }
            unsafe { refresh_sleep_waiter(this.entry) };
            return Poll::Pending;
        }

        if let Some(duration) = this.duration.take() {
            if duration == Duration::ZERO {
                return Poll::Ready(Ok(()));
            }

            let entry = unsafe { &mut *timers }.submit_current_sleep_duration(duration)?;
            this.entry = entry;
            return Poll::Pending;
        }

        let Some(deadline) = this.deadline.take() else {
            return Poll::Ready(Ok(()));
        };

        match unsafe { &mut *timers }.submit_current_sleep_deadline(deadline)? {
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
            // Pool teardown intentionally does not drop live task futures.
            // If that ever changes, this guard keeps an abandoned armed sleep
            // from dereferencing a cleared executor context during shutdown.
            let timers = unsafe { crate::runtime::executor::timers_or_null() };
            if !timers.is_null() {
                let _ = unsafe { &mut *timers }.cancel_sleep(self.entry);
            }
            self.entry = std::ptr::null_mut();
        }
    }
}

/// Sleeps for at least the provided duration.
///
/// A zero duration completes immediately without arming the timer wheel.
///
/// This is a control-path primitive rather than a transport data-path API.
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
/// This is a control-path primitive rather than a transport data-path API.
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
/// This is a control-path deadline wrapper, not a special transport fast-path
/// primitive.
///
/// # Example
/// ```no_run
/// use flowio::runtime::executor::Executor;
/// use flowio::runtime::timer::{sleep, timeout, Elapsed};
/// use std::time::Duration;
///
/// let mut executor = Executor::new()?;
/// executor.run(async {
///     let result: Result<std::io::Result<()>, Elapsed> =
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
    type Output = Result<F::Output, Elapsed>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = unsafe { self.get_unchecked_mut() };

        let mut future = unsafe { Pin::new_unchecked(&mut this.future) };
        if let Poll::Ready(output) = future.as_mut().poll(cx) {
            return Poll::Ready(Ok(output));
        }

        match Pin::new(&mut this.sleep).poll(cx) {
            Poll::Ready(Ok(())) => Poll::Ready(Err(Elapsed)),
            Poll::Ready(Err(_)) => Poll::Ready(Err(Elapsed)),
            Poll::Pending => Poll::Pending,
        }
    }
}

/// Runs a future with a relative timeout.
///
/// This is a control-path deadline wrapper. It is useful around larger
/// operations or phases, but it is not intended to be the crate's per-message
/// I/O fast path.
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
/// This is a control-path deadline wrapper. It is useful around larger
/// operations or phases, but it is not intended to be the crate's per-message
/// I/O fast path.
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
    fn timer_wheel_remove_clears_bucket_ownership_and_deadline_cache() {
        let mut wheel = TimerWheel::new_uninit();
        init_wheel_at(&mut wheel, 100);
        let mut entry = timer_entry_at(105);
        let entry_ptr = &mut entry as *mut TimerEntry;

        wheel.insert(entry_ptr);
        assert_eq!(wheel.next_deadline_tick, Some(105));
        assert_eq!(wheel.candidate_deadline(0), Some(105));
        assert_eq!(entry.bucket_level, 0);

        wheel.remove(entry_ptr);
        wheel.next_deadline_dirty = true;
        wheel.recompute_next_deadline();
        wheel.next_deadline_dirty = false;

        assert!(entry.link.is_unlinked());
        assert_eq!(entry.bucket_level, INVALID_BUCKET_LEVEL);
        assert_eq!(entry.bucket_index, 0);
        assert_eq!(wheel.next_deadline_tick, None);
        assert_eq!(wheel.candidate_deadline(0), None);
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
        assert_eq!(wheel.candidate_deadline(0), None);
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
