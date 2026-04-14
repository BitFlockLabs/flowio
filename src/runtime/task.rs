use crate::utils::list::intrusive::dlist;
use crate::utils::memory::pool::InPlaceInit;
use std::cell::Cell;
use std::mem::MaybeUninit;
use std::task::{Poll, RawWaker, RawWakerVTable, Waker};

unsafe fn schedule_task(task_ptr: *mut TaskHeader)
{
    unsafe { crate::runtime::executor::schedule_woken_task(task_ptr) };
}

#[repr(C)]
#[doc(hidden)]
pub struct TaskHeader
{
    /// Intrusive ready-queue link used by the executor scheduler.
    pub ready_link: dlist::Link,
    /// Non-atomic task reference count shared by wakers, join handles, and the
    /// executor.
    pub refs: Cell<usize>,
    // Scheduler state is kept in one word so wake/poll transitions stay cheap.
    pub flags: Cell<u64>,
    // Timer expiry uses a per-pass epoch to collapse repeated wakes for the
    // same task before they hit the general scheduler notify path.
    pub last_wake_epoch: Cell<u64>,
    /// Cached `Waker` built around this task's raw-waker implementation.
    pub cached_waker: MaybeUninit<Waker>,
    /// Type-erased hooks for polling, finishing, and destroying the concrete task.
    pub vtable: &'static TaskVTable,
    /// Pointer to the executor's `ThreadCtx`, set before each poll.
    /// Runtime-internal futures read this through the waker to access the
    /// reactor and scheduler without a TLS lookup.
    pub ctx: Cell<*mut ()>,
}

impl TaskHeader
{
    pub const READY_LINK_OFFSET: usize = std::mem::offset_of!(TaskHeader, ready_link);
    pub const FLAG_NOTIFIED: u64 = 1 << 0;
    pub const FLAG_RUNNING: u64 = 1 << 1;
    pub const FLAG_QUEUED: u64 = 1 << 2;
    pub const FLAG_COMPLETED: u64 = 1 << 3;

    #[allow(clippy::new_without_default)]
    pub const fn new() -> Self
    {
        Self {
            ready_link: dlist::Link::new_unlinked(),
            refs: Cell::new(1),
            flags: Cell::new(0),
            last_wake_epoch: Cell::new(0),
            cached_waker: MaybeUninit::uninit(),
            vtable: &DUMMY_VTABLE,
            ctx: Cell::new(std::ptr::null_mut()),
        }
    }

    #[inline(always)]
    pub fn flags(&self) -> u64
    {
        self.flags.get()
    }

    #[inline(always)]
    pub fn has_flag(&self, flag: u64) -> bool
    {
        (self.flags() & flag) != 0
    }

    #[inline(always)]
    pub fn set_flag(&self, flag: u64)
    {
        self.flags.set(self.flags() | flag);
    }

    #[inline(always)]
    pub fn clear_flag(&self, flag: u64)
    {
        self.flags.set(self.flags() & !flag);
    }
}

#[doc(hidden)]
pub struct TaskVTable
{
    /// Polls the concrete future stored in the task.
    pub poll: unsafe fn(*mut TaskHeader) -> Poll<()>,
    // Typed task teardown hook. The executor performs generic completion state
    // changes itself and uses this hook only to drop the concrete future.
    /// Drops the concrete future after task completion.
    pub finish: unsafe fn(*mut TaskHeader),
    /// Destroys the full task allocation after the final reference is released.
    pub destroy: unsafe fn(*mut TaskHeader),
}

#[repr(C, align(64))]
#[doc(hidden)]
pub struct Task<const SIZE: usize>
{
    /// Fixed header shared by all runtime tasks.
    pub header: TaskHeader,
    /// Inline storage for the concrete task payload.
    pub data: [MaybeUninit<u8>; SIZE],
}

impl<const SIZE: usize> InPlaceInit for Task<SIZE>
{
    type Args = ();

    fn init_at(slot: &mut MaybeUninit<Self>, _: Self::Args)
    {
        let header = TaskHeader::new();
        unsafe {
            let header_ptr = (slot.as_mut_ptr() as *mut u8)
                .add(std::mem::offset_of!(Task<SIZE>, header))
                as *mut TaskHeader;
            std::ptr::write(header_ptr, header);
        }
    }
}

static DUMMY_VTABLE: TaskVTable = TaskVTable {
    poll: |_| Poll::Ready(()),
    finish: |_| {},
    destroy: |_| {},
};

const RAW_WAKER_VTABLE: RawWakerVTable =
    RawWakerVTable::new(clone_waker, wake_waker, wake_by_ref_waker, drop_waker);

unsafe fn clone_waker(ptr: *const ()) -> RawWaker
{
    unsafe {
        retain_task(ptr as *mut TaskHeader);
    }
    RawWaker::new(ptr, &RAW_WAKER_VTABLE)
}

unsafe fn wake_waker(ptr: *const ())
{
    unsafe {
        schedule_task(ptr as *mut TaskHeader);
        release_task(ptr as *mut TaskHeader);
    }
}

unsafe fn wake_by_ref_waker(ptr: *const ())
{
    unsafe {
        schedule_task(ptr as *mut TaskHeader);
    }
}

unsafe fn drop_waker(ptr: *const ())
{
    unsafe {
        release_task(ptr as *mut TaskHeader);
    }
}

#[doc(hidden)]
pub unsafe fn init_cached_waker(ptr: *mut TaskHeader)
{
    unsafe {
        (*ptr).cached_waker.write(Waker::from_raw(RawWaker::new(
            ptr as *const (),
            &RAW_WAKER_VTABLE,
        )));
    }
}

#[doc(hidden)]
pub unsafe fn cached_waker_ref<'a>(ptr: *mut TaskHeader) -> &'a Waker
{
    unsafe { (&*ptr).cached_waker.assume_init_ref() }
}

#[inline(always)]
#[doc(hidden)]
pub unsafe fn retain_task(ptr: *mut TaskHeader)
{
    let header = unsafe { &*ptr };
    header.refs.set(header.refs.get() + 1);
}

#[inline(always)]
#[doc(hidden)]
pub unsafe fn release_task(ptr: *mut TaskHeader)
{
    let header = unsafe { &*ptr };
    let prev = header.refs.get();
    debug_assert!(prev > 0, "runtime task refcount underflow");
    header.refs.set(prev - 1);

    if prev == 1
    {
        unsafe {
            (header.vtable.destroy)(ptr);
        }
    }
}
