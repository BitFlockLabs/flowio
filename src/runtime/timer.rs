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

pub const TIMER_TICK_NS: u64 = 1_000_000;
const LVL0_SLOTS: usize = 256;
const LVLN_SLOTS: usize = 64;
const TIMERS_PER_SLAB: usize = 1024;
const INVALID_BUCKET_LEVEL: u8 = u8::MAX;

#[derive(Clone, Copy, PartialEq, Eq)]
enum TimerState
{
    Idle,
    Armed,
    Fired,
    Cancelled,
}

#[repr(C)]
pub(crate) struct TimerEntry
{
    link: Link,
    waiter: *mut TaskHeader,
    deadline_tick: u64,
    state: TimerState,
    bucket_level: u8,
    bucket_index: u16,
}

impl TimerEntry
{
    const LINK_OFFSET: usize = std::mem::offset_of!(TimerEntry, link);

    const fn new() -> Self
    {
        Self {
            link: Link::new_unlinked(),
            waiter: std::ptr::null_mut(),
            deadline_tick: 0,
            state: TimerState::Idle,
            bucket_level: INVALID_BUCKET_LEVEL,
            bucket_index: 0,
        }
    }

    fn clear_waiter(&mut self)
    {
        self.waiter = std::ptr::null_mut();
    }
}

impl InPlaceInit for TimerEntry
{
    type Args = ();

    fn init_at(slot: &mut MaybeUninit<Self>, _: Self::Args)
    {
        slot.write(TimerEntry::new());
    }
}

struct TimerWheel
{
    current_tick: u64,
    next_deadline_tick: Option<u64>,
    next_deadline_dirty: bool,
    // Non-empty bucket occupancy, used to recompute the nearest deadline
    // without scanning every bucket front after cancels or partial timer work.
    lvl0_bits: [u64; 4],
    lvl1_bits: u64,
    lvl2_bits: u64,
    lvl3_bits: u64,
    // At most one boundary per upper level can need cascading for a given tick.
    // These arrays remember unfinished cascade work so the executor can resume
    // it on the next timer phase instead of draining a whole bucket at once.
    cascade_levels: [u8; 3],
    cascade_indices: [usize; 3],
    cascade_count: u8,
    cascade_pos: u8,
    lvl0: [DList<TimerEntry>; LVL0_SLOTS],
    lvl1: [DList<TimerEntry>; LVLN_SLOTS],
    lvl2: [DList<TimerEntry>; LVLN_SLOTS],
    lvl3: [DList<TimerEntry>; LVLN_SLOTS],
}

impl TimerWheel
{
    fn new_uninit() -> Self
    {
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

    fn init(&mut self) -> io::Result<()>
    {
        self.current_tick = now_tick()?;
        self.next_deadline_tick = None;
        for bucket in &mut self.lvl0
        {
            bucket.init();
        }
        for bucket in &mut self.lvl1
        {
            bucket.init();
        }
        for bucket in &mut self.lvl2
        {
            bucket.init();
        }
        for bucket in &mut self.lvl3
        {
            bucket.init();
        }
        Ok(())
    }

    fn insert(&mut self, entry: *mut TimerEntry)
    {
        let deadline = unsafe { (*entry).deadline_tick };
        let delta = deadline.saturating_sub(self.current_tick);
        let (level, index) = if delta < LVL0_SLOTS as u64
        {
            (0u8, (deadline & ((LVL0_SLOTS as u64) - 1)) as usize)
        }
        else if delta < (1u64 << 14)
        {
            (1u8, ((deadline >> 8) & ((LVLN_SLOTS as u64) - 1)) as usize)
        }
        else if delta < (1u64 << 20)
        {
            (2u8, ((deadline >> 14) & ((LVLN_SLOTS as u64) - 1)) as usize)
        }
        else
        {
            (3u8, ((deadline >> 20) & ((LVLN_SLOTS as u64) - 1)) as usize)
        };

        unsafe {
            (*entry).bucket_level = level;
            (*entry).bucket_index = index as u16;
            (*entry).state = TimerState::Armed;
            match level
            {
                0 =>
                {
                    self.lvl0[index].push_back(&mut (*entry).link as *mut _);
                    self.set_bucket_occupied(0, index);
                }
                1 =>
                {
                    self.lvl1[index].push_back(&mut (*entry).link as *mut _);
                    self.set_bucket_occupied(1, index);
                }
                2 =>
                {
                    self.lvl2[index].push_back(&mut (*entry).link as *mut _);
                    self.set_bucket_occupied(2, index);
                }
                _ =>
                {
                    self.lvl3[index].push_back(&mut (*entry).link as *mut _);
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

    fn remove(&mut self, entry: *mut TimerEntry)
    {
        let level = unsafe { (*entry).bucket_level };
        let index = unsafe { (*entry).bucket_index as usize };
        unsafe {
            match level
            {
                0 => self.lvl0[index].remove(&mut (*entry).link as *mut _),
                1 => self.lvl1[index].remove(&mut (*entry).link as *mut _),
                2 => self.lvl2[index].remove(&mut (*entry).link as *mut _),
                3 => self.lvl3[index].remove(&mut (*entry).link as *mut _),
                _ =>
                {}
            }
            self.clear_bucket_if_empty(level, index);
            (*entry).bucket_level = INVALID_BUCKET_LEVEL;
            (*entry).bucket_index = 0;
        }
    }

    #[inline(always)]
    fn bits_mut(&mut self, level: u8) -> &mut u64
    {
        match level
        {
            1 => &mut self.lvl1_bits,
            2 => &mut self.lvl2_bits,
            _ => &mut self.lvl3_bits,
        }
    }

    #[inline(always)]
    fn is_bucket_empty(&self, level: u8, index: usize) -> bool
    {
        match level
        {
            0 => self.lvl0[index].front(TimerEntry::LINK_OFFSET).is_none(),
            1 => self.lvl1[index].front(TimerEntry::LINK_OFFSET).is_none(),
            2 => self.lvl2[index].front(TimerEntry::LINK_OFFSET).is_none(),
            _ => self.lvl3[index].front(TimerEntry::LINK_OFFSET).is_none(),
        }
    }

    #[inline(always)]
    fn set_bucket_occupied(&mut self, level: u8, index: usize)
    {
        match level
        {
            0 => self.lvl0_bits[index / 64] |= 1u64 << (index % 64),
            _ => *self.bits_mut(level) |= 1u64 << index,
        }
    }

    #[inline(always)]
    fn clear_bucket_if_empty(&mut self, level: u8, index: usize)
    {
        if !self.is_bucket_empty(level, index)
        {
            return;
        }

        match level
        {
            0 => self.lvl0_bits[index / 64] &= !(1u64 << (index % 64)),
            _ => *self.bits_mut(level) &= !(1u64 << index),
        }
    }

    fn next_set_bit(bits: u64, start: usize) -> Option<usize>
    {
        if start >= 64
        {
            return None;
        }
        let masked = bits & (!0u64 << start);
        if masked == 0
        {
            None
        }
        else
        {
            Some(masked.trailing_zeros() as usize)
        }
    }

    fn next_nonempty_lvl0_bucket(&self) -> Option<usize>
    {
        let start = (self.current_tick & ((LVL0_SLOTS as u64) - 1)) as usize;
        let start_word = start / 64;
        let start_bit = start % 64;

        for word in start_word..self.lvl0_bits.len()
        {
            let bit = if word == start_word
            {
                Self::next_set_bit(self.lvl0_bits[word], start_bit)
            }
            else
            {
                Self::next_set_bit(self.lvl0_bits[word], 0)
            };
            if let Some(bit) = bit
            {
                return Some(word * 64 + bit);
            }
        }

        for word in 0..start_word
        {
            if let Some(bit) = Self::next_set_bit(self.lvl0_bits[word], 0)
            {
                return Some(word * 64 + bit);
            }
        }

        None
    }

    fn next_nonempty_lvln_bucket(bits: u64, start: usize) -> Option<usize>
    {
        Self::next_set_bit(bits, start).or_else(|| Self::next_set_bit(bits, 0))
    }

    fn has_pending_cascade(&self) -> bool
    {
        self.cascade_pos < self.cascade_count
    }

    fn begin_tick_cascade(&mut self)
    {
        if self.has_pending_cascade()
        {
            return;
        }

        self.cascade_count = 0;
        self.cascade_pos = 0;

        if (self.current_tick & ((LVL0_SLOTS as u64) - 1)) == 0
        {
            let idx1 = ((self.current_tick >> 8) & ((LVLN_SLOTS as u64) - 1)) as usize;
            self.cascade_levels[self.cascade_count as usize] = 1;
            self.cascade_indices[self.cascade_count as usize] = idx1;
            self.cascade_count += 1;

            if (self.current_tick & ((1u64 << 14) - 1)) == 0
            {
                let idx2 = ((self.current_tick >> 14) & ((LVLN_SLOTS as u64) - 1)) as usize;
                self.cascade_levels[self.cascade_count as usize] = 2;
                self.cascade_indices[self.cascade_count as usize] = idx2;
                self.cascade_count += 1;

                if (self.current_tick & ((1u64 << 20) - 1)) == 0
                {
                    let idx3 = ((self.current_tick >> 20) & ((LVLN_SLOTS as u64) - 1)) as usize;
                    self.cascade_levels[self.cascade_count as usize] = 3;
                    self.cascade_indices[self.cascade_count as usize] = idx3;
                    self.cascade_count += 1;
                }
            }
        }
    }

    fn process_cascade_with_budget(&mut self, budget: usize) -> usize
    {
        let mut consumed = 0usize;

        while self.has_pending_cascade() && consumed < budget
        {
            let pos = self.cascade_pos as usize;
            let level = self.cascade_levels[pos];
            let index = self.cascade_indices[pos];
            let entry_ptr = unsafe {
                match level
                {
                    1 => self.lvl1[index].pop_front(TimerEntry::LINK_OFFSET),
                    2 => self.lvl2[index].pop_front(TimerEntry::LINK_OFFSET),
                    _ => self.lvl3[index].pop_front(TimerEntry::LINK_OFFSET),
                }
            };
            let Some(entry_ptr) = entry_ptr
            else
            {
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

        while self.has_pending_cascade()
        {
            let pos = self.cascade_pos as usize;
            let level = self.cascade_levels[pos];
            let index = self.cascade_indices[pos];
            let has_more = match level
            {
                1 => self.lvl1[index].front(TimerEntry::LINK_OFFSET).is_some(),
                2 => self.lvl2[index].front(TimerEntry::LINK_OFFSET).is_some(),
                _ => self.lvl3[index].front(TimerEntry::LINK_OFFSET).is_some(),
            };
            if has_more
            {
                break;
            }
            self.cascade_pos += 1;
        }

        if !self.has_pending_cascade()
        {
            self.cascade_count = 0;
            self.cascade_pos = 0;
        }

        consumed
    }

    // Recompute the global nearest deadline from the nearest non-empty bucket
    // at each level. The occupancy bitsets keep this bounded even after
    // heavy cancellation invalidates the cached nearest deadline.
    fn recompute_next_deadline(&mut self)
    {
        self.next_deadline_tick = self
            .candidate_deadline(0)
            .into_iter()
            .chain(self.candidate_deadline(1))
            .chain(self.candidate_deadline(2))
            .chain(self.candidate_deadline(3))
            .min();
    }

    fn candidate_deadline(&self, level: u8) -> Option<u64>
    {
        let index = match level
        {
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

        let entry = match level
        {
            0 => self.lvl0[index].front(TimerEntry::LINK_OFFSET)?,
            1 => self.lvl1[index].front(TimerEntry::LINK_OFFSET)?,
            2 => self.lvl2[index].front(TimerEntry::LINK_OFFSET)?,
            _ => self.lvl3[index].front(TimerEntry::LINK_OFFSET)?,
        };
        Some(unsafe { (*entry).deadline_tick })
    }
}

/// Runtime-owned timer subsystem.
///
/// Logical timers live entirely in user space. The timer wheel computes the
/// next wake deadline and the executor uses that deadline to bound how long it
/// may block in the reactor.
pub struct TimerRuntime
{
    _provider: Box<BasicMemoryProvider>,
    timer_pool: ManuallyDrop<Pool<'static, TimerEntry, BasicMemoryProvider>>,
    wheel: TimerWheel,
    cached_now_tick: u64,
    arm_base: Option<ArmBase>,
}

#[derive(Clone, Copy)]
struct ArmBase
{
    instant: Instant,
    tick: u64,
}

impl TimerRuntime
{
    #[allow(clippy::new_without_default)]
    pub fn new() -> io::Result<Self>
    {
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

    pub fn init(&mut self) -> io::Result<()>
    {
        self.timer_pool.init();
        self.wheel.init()?;
        self.cached_now_tick = self.wheel.current_tick;
        self.arm_base = None;
        Ok(())
    }

    pub fn now_tick(&mut self) -> io::Result<u64>
    {
        let now = now_tick()?;
        self.cached_now_tick = now;
        Ok(now)
    }

    #[inline(always)]
    pub(crate) fn begin_executor_pass(&mut self)
    {
        self.arm_base = None;
    }

    pub fn sleep(&mut self, duration: Duration) -> Sleep
    {
        Sleep::new_duration(duration)
    }

    pub fn sleep_until(&mut self, deadline: Instant) -> Sleep
    {
        Sleep::new_deadline(deadline)
    }

    fn submit_sleep_at_tick(
        &mut self,
        task: *mut TaskHeader,
        deadline_tick: u64,
    ) -> io::Result<*mut TimerEntry>
    {
        let entry = unsafe {
            match self.timer_pool.alloc(())
            {
                Some(entry) => entry,
                None =>
                {
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

    fn arm_base(&mut self) -> io::Result<ArmBase>
    {
        if let Some(base) = self.arm_base
        {
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
        if !self.has_pending()
        {
            self.wheel.current_tick = tick;
        }

        let base = ArmBase { instant, tick };
        self.arm_base = Some(base);
        Ok(base)
    }

    fn deadline_tick_for_duration(&mut self, duration: Duration) -> io::Result<u64>
    {
        let ticks = duration_to_ticks(duration);
        let base = self.arm_base()?;
        Ok(base.tick.saturating_add(ticks))
    }

    fn deadline_tick_for_instant(&mut self, deadline: Instant) -> io::Result<Option<u64>>
    {
        let base = self.arm_base()?;
        if deadline <= base.instant
        {
            return Ok(None);
        }

        let delta = deadline.duration_since(base.instant);
        Ok(Some(base.tick.saturating_add(duration_to_ticks(delta))))
    }

    pub(crate) fn submit_current_sleep_duration(
        &mut self,
        duration: Duration,
    ) -> io::Result<*mut TimerEntry>
    {
        // The owning task is taken from the active poll frame and stored once
        // in the timer entry. Expiry later wakes that task directly.
        let task = unsafe { current_poll_owner_task_unchecked() };
        let deadline_tick = self.deadline_tick_for_duration(duration)?;
        self.submit_sleep_at_tick(task, deadline_tick)
    }

    pub(crate) fn submit_current_sleep_deadline(
        &mut self,
        deadline: Instant,
    ) -> io::Result<Option<*mut TimerEntry>>
    {
        let task = unsafe { current_poll_owner_task_unchecked() };
        let Some(deadline_tick) = self.deadline_tick_for_instant(deadline)?
        else
        {
            return Ok(None);
        };
        self.submit_sleep_at_tick(task, deadline_tick).map(Some)
    }

    pub(crate) fn cancel_sleep(&mut self, entry: *mut TimerEntry) -> io::Result<()>
    {
        if entry.is_null()
        {
            return Ok(());
        }

        if unsafe { (*entry).state } == TimerState::Armed
        {
            let deadline_tick = unsafe { (*entry).deadline_tick };
            self.wheel.remove(entry);
            if self.wheel.next_deadline_tick == Some(deadline_tick)
            {
                self.wheel.next_deadline_dirty = true;
            }
        }
        unsafe {
            (*entry).state = TimerState::Cancelled;
            (*entry).clear_waiter();
            self.timer_pool.free(entry);
        }
        Ok(())
    }

    pub fn process_at_with_budget(&mut self, now: u64, budget: usize) -> io::Result<bool>
    {
        self.cached_now_tick = now;
        if now >= self.wheel.current_tick
        {
            return Ok(self.collect_expired(now, budget));
        }
        Ok(false)
    }

    pub fn has_pending(&mut self) -> bool
    {
        if self.wheel.next_deadline_dirty
        {
            self.wheel.recompute_next_deadline();
            self.wheel.next_deadline_dirty = false;
        }
        self.wheel.next_deadline_tick.is_some()
    }

    pub fn next_wait_duration(&mut self, now_tick: u64) -> Option<Duration>
    {
        if self.wheel.next_deadline_dirty
        {
            self.wheel.recompute_next_deadline();
            self.wheel.next_deadline_dirty = false;
        }
        let deadline_tick = self.wheel.next_deadline_tick?;
        Some(tick_to_duration(deadline_tick.saturating_sub(now_tick)))
    }

    // Expire due timers directly into the executor's main ready queue. The
    // phase budget covers both cascade reinsertion and final expiry so timer
    // maintenance cannot monopolize the executor loop.
    fn collect_expired(&mut self, target_tick: u64, budget: usize) -> bool
    {
        let schedule_ctx = unsafe { schedule_ctx_unchecked() };
        let wake_epoch = unsafe { next_timer_wake_epoch_unchecked(schedule_ctx) };
        let mut remaining_budget = budget;
        while self.wheel.current_tick <= target_tick
        {
            self.wheel.begin_tick_cascade();
            if self.wheel.has_pending_cascade()
            {
                if remaining_budget == 0
                {
                    self.wheel.next_deadline_dirty = true;
                    return true;
                }

                let consumed = self.wheel.process_cascade_with_budget(remaining_budget);
                remaining_budget -= consumed;
                if self.wheel.has_pending_cascade()
                {
                    self.wheel.next_deadline_dirty = true;
                    return true;
                }
            }

            let idx = (self.wheel.current_tick & ((LVL0_SLOTS as u64) - 1)) as usize;
            while let Some(entry_ptr) =
                unsafe { self.wheel.lvl0[idx].pop_front(TimerEntry::LINK_OFFSET) }
            {
                self.wheel.clear_bucket_if_empty(0, idx);
                if remaining_budget == 0
                {
                    unsafe {
                        self.wheel.lvl0[idx].push_front_unchecked(&mut (*entry_ptr).link as *mut _);
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
                    if !waiter.is_null()
                    {
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
            self.wheel.current_tick = self.wheel.current_tick.saturating_add(1);
        }
        self.wheel.next_deadline_dirty = true;
        false
    }
}

impl Drop for TimerRuntime
{
    fn drop(&mut self)
    {
        unsafe {
            ManuallyDrop::drop(&mut self.timer_pool);
        }
    }
}

fn duration_to_ticks(duration: Duration) -> u64
{
    let nanos = duration.as_nanos() as u64;
    let ticks = nanos / TIMER_TICK_NS;
    let remainder = nanos % TIMER_TICK_NS;
    if ticks == 0 || remainder != 0
    {
        ticks + 1
    }
    else
    {
        ticks
    }
}

fn tick_to_duration(ticks: u64) -> Duration
{
    if ticks == 0
    {
        Duration::from_nanos(1)
    }
    else
    {
        Duration::from_nanos(ticks.saturating_mul(TIMER_TICK_NS))
    }
}

fn now_tick() -> io::Result<u64>
{
    note_timer_now_tick_call();
    let mut ts = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    let rc = unsafe { libc::clock_gettime(libc::CLOCK_MONOTONIC, &mut ts) };
    if rc < 0
    {
        return Err(io::Error::last_os_error());
    }

    Ok(((ts.tv_sec as u64) * 1_000_000_000u64 + (ts.tv_nsec as u64)) / TIMER_TICK_NS)
}

/// One-shot sleep future scheduled by the runtime timer wheel.
pub struct Sleep
{
    duration: Option<Duration>,
    deadline: Option<Instant>,
    entry: *mut TimerEntry,
}

impl Sleep
{
    pub fn new_duration(duration: Duration) -> Self
    {
        Self {
            duration: Some(duration),
            deadline: None,
            entry: std::ptr::null_mut(),
        }
    }

    pub fn new_deadline(deadline: Instant) -> Self
    {
        Self {
            duration: None,
            deadline: Some(deadline),
            entry: std::ptr::null_mut(),
        }
    }
}

/// Error returned when a future exceeds its configured deadline.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Elapsed;

impl fmt::Display for Elapsed
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result
    {
        f.write_str("runtime timer elapsed")
    }
}

impl std::error::Error for Elapsed {}

impl Future for Sleep
{
    type Output = io::Result<()>;

    fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output>
    {
        let this = unsafe { self.get_unchecked_mut() };
        let timers = unsafe { crate::runtime::executor::timers_unchecked() };

        if !this.entry.is_null()
        {
            let state = unsafe { (*this.entry).state };
            if state == TimerState::Fired
            {
                unsafe {
                    (*this.entry).state = TimerState::Idle;
                    (*timers).timer_pool.free(this.entry);
                }
                this.entry = std::ptr::null_mut();
                return Poll::Ready(Ok(()));
            }
            if state == TimerState::Cancelled
            {
                this.entry = std::ptr::null_mut();
                return Poll::Ready(Err(io::Error::from(io::ErrorKind::Interrupted)));
            }
            return Poll::Pending;
        }

        if let Some(duration) = this.duration.take()
        {
            let entry = unsafe { &mut *timers }.submit_current_sleep_duration(duration)?;
            this.entry = entry;
            return Poll::Pending;
        }

        let Some(deadline) = this.deadline.take()
        else
        {
            return Poll::Ready(Ok(()));
        };

        match unsafe { &mut *timers }.submit_current_sleep_deadline(deadline)?
        {
            Some(entry) =>
            {
                this.entry = entry;
                Poll::Pending
            }
            None => Poll::Ready(Ok(())),
        }
    }
}

impl Drop for Sleep
{
    fn drop(&mut self)
    {
        if !self.entry.is_null()
        {
            let _ = unsafe { &mut *crate::runtime::executor::timers_unchecked() }
                .cancel_sleep(self.entry);
            self.entry = std::ptr::null_mut();
        }
    }
}

/// Sleeps for at least the provided duration.
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
pub fn sleep(duration: Duration) -> Sleep
{
    Sleep::new_duration(duration)
}

/// Sleeps until the provided monotonic deadline.
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
pub fn sleep_until(deadline: Instant) -> Sleep
{
    Sleep::new_deadline(deadline)
}

/// Future returned by [`timeout`] and [`timeout_at`].
pub struct Timeout<F>
{
    future: F,
    sleep: Sleep,
}

impl<F> Timeout<F>
{
    fn new_duration(duration: Duration, future: F) -> Self
    {
        Self {
            future,
            sleep: Sleep::new_duration(duration),
        }
    }

    fn new_deadline(deadline: Instant, future: F) -> Self
    {
        Self {
            future,
            sleep: Sleep::new_deadline(deadline),
        }
    }
}

impl<F: Future> Future for Timeout<F>
{
    type Output = Result<F::Output, Elapsed>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output>
    {
        let this = unsafe { self.get_unchecked_mut() };

        let mut future = unsafe { Pin::new_unchecked(&mut this.future) };
        if let Poll::Ready(output) = future.as_mut().poll(cx)
        {
            return Poll::Ready(Ok(output));
        }

        match Pin::new(&mut this.sleep).poll(cx)
        {
            Poll::Ready(Ok(())) => Poll::Ready(Err(Elapsed)),
            Poll::Ready(Err(_)) => Poll::Ready(Err(Elapsed)),
            Poll::Pending => Poll::Pending,
        }
    }
}

/// Runs a future with a relative timeout.
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
pub fn timeout<F: Future>(duration: Duration, future: F) -> Timeout<F>
{
    Timeout::new_duration(duration, future)
}

/// Runs a future with an absolute monotonic deadline.
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
pub fn timeout_at<F: Future>(deadline: Instant, future: F) -> Timeout<F>
{
    Timeout::new_deadline(deadline, future)
}
