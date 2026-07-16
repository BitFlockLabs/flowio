//! Runtime task headers, cached wakers, and inline task storage.
//!
//! FlowIO tasks and scheduler state are executor-thread owned. The runtime is
//! single-threaded: a task waker is only ever cloned, woken, or dropped on the
//! executor thread that owns the task. Moving one to another thread would race
//! the non-atomic task reference count and intrusive ready-queue links.

use crate::runtime::executor::ExecutorOwner;
use crate::runtime::refcount::increment_refcount;
use crate::utils::list::intrusive::dlist;
use crate::utils::memory::pool::InPlaceInit;
use std::cell::Cell;
use std::mem::MaybeUninit;
use std::rc::Rc;
use std::task::{Poll, RawWaker, RawWakerVTable, Waker};

/// Debug-only guard that a task waker is used on its executor owner thread.
///
/// The runtime is single-threaded, so this always holds in correct programs. In
/// debug builds it catches accidental off-thread waker use during development;
/// in release builds the owner method compiles to nothing.
#[inline(always)]
fn debug_assert_owner_thread(header: &TaskHeader) {
    if let Some(owner) = header.owner.as_ref() {
        owner.debug_assert_owner_thread();
    }
}

/// Sends a raw-waker notification to the task's recorded executor owner.
///
/// # Safety
///
/// `task_ptr` must identify a live task and this must run on its owner thread.
unsafe fn schedule_woken_task(task_ptr: *mut TaskHeader) {
    unsafe { crate::runtime::executor::schedule_woken_task(task_ptr) };
}

#[repr(C)]
#[doc(hidden)]
pub struct TaskHeader {
    /// Intrusive ready-queue link used by the executor scheduler.
    pub ready_link: dlist::Link,
    /// Intrusive link in the owner's list of allocated task slots.
    pub(crate) all_link: dlist::Link,
    /// Owner-thread-only task reference count shared by the executor, join
    /// handles, and operation/timer waiter slots.
    pub refs: Cell<usize>,
    /// Packed scheduler state bits such as notified/running/queued/completed.
    /// Kept in one word so wake/poll transitions stay cheap.
    pub flags: Cell<u64>,
    /// Last timer wake epoch observed for this task. Timer expiry uses a
    /// per-pass epoch to collapse repeated wakes before they re-enter the
    /// general scheduler path.
    pub last_wake_epoch: Cell<u64>,
    /// Cached `Waker` built around this task's raw-waker implementation.
    pub cached_waker: MaybeUninit<Waker>,
    /// Type-erased hooks for polling, finishing, and destroying the concrete task.
    pub vtable: &'static TaskVTable,
    /// Stable executor owner that allocated this task slot.
    ///
    /// The task keeps the owner and its pools alive until the final task,
    /// join-handle, or waker reference is released.
    pub(crate) owner: Option<Rc<ExecutorOwner>>,
}

impl TaskHeader {
    pub const READY_LINK_OFFSET: usize = std::mem::offset_of!(TaskHeader, ready_link);
    pub(crate) const ALL_LINK_OFFSET: usize = std::mem::offset_of!(TaskHeader, all_link);
    pub const FLAG_NOTIFIED: u64 = 1 << 0;
    pub const FLAG_RUNNING: u64 = 1 << 1;
    pub const FLAG_QUEUED: u64 = 1 << 2;
    pub const FLAG_COMPLETED: u64 = 1 << 3;

    #[allow(clippy::new_without_default)]
    pub const fn new() -> Self {
        Self {
            ready_link: dlist::Link::new_unlinked(),
            all_link: dlist::Link::new_unlinked(),
            refs: Cell::new(1),
            flags: Cell::new(0),
            last_wake_epoch: Cell::new(0),
            cached_waker: MaybeUninit::uninit(),
            vtable: &DUMMY_VTABLE,
            owner: None,
        }
    }

    #[inline(always)]
    #[cfg(test)]
    pub fn flags(&self) -> u64 {
        self.flags.get()
    }

    #[inline(always)]
    #[cfg(test)]
    pub fn has_flag(&self, flag: u64) -> bool {
        (self.flags() & flag) != 0
    }

    #[inline(always)]
    #[cfg(test)]
    pub fn set_flag(&self, flag: u64) {
        self.flags.set(self.flags() | flag);
    }

    #[inline(always)]
    #[cfg(test)]
    pub fn clear_flag(&self, flag: u64) {
        self.flags.set(self.flags() & !flag);
    }
}

#[doc(hidden)]
pub struct TaskVTable {
    /// Polls the concrete future stored in the task.
    pub poll: unsafe fn(*mut TaskHeader) -> Poll<()>,
    /// Drops the concrete future after task completion.
    /// The executor handles the generic scheduler state transition itself.
    pub finish: unsafe fn(*mut TaskHeader),
    /// Cancels an unfinished concrete future and publishes its join error.
    pub cancel: unsafe fn(*mut TaskHeader),
    /// Destroys the full task allocation after the final reference is released.
    pub destroy: unsafe fn(*mut TaskHeader),
}

#[repr(C, align(64))]
#[doc(hidden)]
pub struct Task<const SIZE: usize> {
    /// Fixed header shared by all runtime tasks.
    pub header: TaskHeader,
    /// Inline storage for the concrete future/join payload associated with the
    /// task header.
    pub data: [MaybeUninit<u8>; SIZE],
}

impl<const SIZE: usize> InPlaceInit for Task<SIZE> {
    type Args = ();

    fn init_at(slot: &mut MaybeUninit<Self>, _: Self::Args) {
        let header = TaskHeader::new();
        // SAFETY: `slot` provides correctly aligned storage for Task<SIZE>;
        // writing only the header leaves the byte payload intentionally
        // uninitialized for the concrete task constructor.
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
    cancel: |_| {},
    destroy: |_| {},
};

static TASK_WAKER_VTABLE: RawWakerVTable = RawWakerVTable::new(
    clone_task_waker,
    wake_task_waker,
    wake_by_ref_task_waker,
    drop_task_waker,
);

/// Clones one raw-waker reference to a live task.
///
/// # Safety
///
/// `ptr` must be the data pointer of a FlowIO task waker and therefore point
/// to a live `TaskHeader` on the owning executor thread.
unsafe fn clone_task_waker(ptr: *const ()) -> RawWaker {
    let task = ptr as *mut TaskHeader;
    debug_assert_owner_thread(unsafe { &*task });
    unsafe {
        retain_task(task);
    }
    RawWaker::new(ptr, &TASK_WAKER_VTABLE)
}

/// Consumes one raw-waker reference after scheduling its task.
///
/// # Safety
///
/// `ptr` must own one reference to a live FlowIO task and be woken on the
/// executor thread that owns that task.
unsafe fn wake_task_waker(ptr: *const ()) {
    let task = ptr as *mut TaskHeader;
    debug_assert_owner_thread(unsafe { &*task });
    unsafe {
        schedule_woken_task(task);
        release_task(task);
    }
}

/// Schedules a task without consuming the caller's raw-waker reference.
///
/// # Safety
///
/// `ptr` must refer to a live FlowIO task on its owning executor thread.
unsafe fn wake_by_ref_task_waker(ptr: *const ()) {
    let task = ptr as *mut TaskHeader;
    debug_assert_owner_thread(unsafe { &*task });
    unsafe {
        schedule_woken_task(task);
    }
}

/// Releases one raw-waker reference without scheduling the task.
///
/// # Safety
///
/// `ptr` must own one reference to a live FlowIO task on its executor thread.
unsafe fn drop_task_waker(ptr: *const ()) {
    let task = ptr as *mut TaskHeader;
    // Assert before releasing: the final release may destroy the header.
    debug_assert_owner_thread(unsafe { &*task });
    unsafe {
        release_task(task);
    }
}

/// Returns the live task pointer encoded by a FlowIO task waker.
///
/// The cached task representation is live for its enclosing poll; its caller
/// validates the executor owner before touching owner-only state.
#[inline(always)]
pub(crate) fn task_ptr_from_waker(waker: &Waker) -> Option<*mut TaskHeader> {
    if std::ptr::eq(waker.vtable(), &TASK_WAKER_VTABLE) {
        let task = waker.data().cast_mut().cast::<TaskHeader>();
        if task.is_null() {
            return None;
        }
        return Some(task);
    }

    None
}

#[doc(hidden)]
/// Builds and stores the stable task-local waker once task storage is live.
///
/// # Safety
///
/// `ptr` must point to initialized, stable task storage owned by the active
/// executor. This function must run exactly once before the cached waker is
/// read or the task is released.
pub unsafe fn init_cached_waker(ptr: *mut TaskHeader) {
    unsafe {
        (*ptr).cached_waker.write(Waker::from_raw(RawWaker::new(
            ptr.cast(),
            &TASK_WAKER_VTABLE,
        )));
    }
}

#[doc(hidden)]
/// Returns the cached waker reference stored inside the task header.
///
/// # Safety
///
/// `ptr` must point to a live task whose cached waker has been initialized.
/// The returned reference must not outlive that task allocation.
pub unsafe fn cached_waker_ref<'a>(ptr: *mut TaskHeader) -> &'a Waker {
    unsafe { (&*ptr).cached_waker.assume_init_ref() }
}

#[inline(always)]
#[doc(hidden)]
/// Increments the runtime task reference count.
///
/// # Safety
///
/// `ptr` must point to a live task owned by the current executor thread.
pub unsafe fn retain_task(ptr: *mut TaskHeader) {
    let header = unsafe { &*ptr };
    increment_refcount(&header.refs);
}

#[inline(always)]
#[doc(hidden)]
/// Decrements the runtime task reference count and destroys the task at zero.
///
/// # Safety
///
/// `ptr` must own one live task reference on the current executor thread. The
/// caller must not access the task after releasing its final reference.
pub unsafe fn release_task(ptr: *mut TaskHeader) {
    let header = unsafe { &*ptr };
    let prev = header.refs.get();
    debug_assert!(prev > 0, "runtime task refcount underflow");
    header.refs.set(prev - 1);

    if prev == 1 {
        unsafe {
            (header.vtable.destroy)(ptr);
        }
    }
}

/// Replaces one owned task reference stored in an internal waiter slot.
///
/// Waiter slots follow one pairing rule: every non-null pointer owns exactly
/// one task reference. Registration retains the replacement before releasing
/// the old pointer so replacing the final reference cannot invalidate the new
/// value. Re-registering the same task leaves the count unchanged.
///
/// # Safety
///
/// Every non-null pointer already in `slot` must own one live task reference.
/// A non-null `task` must point to a live task on its owner thread. The caller
/// must clear or transfer the slot before its containing pool entry is reused.
#[inline(always)]
pub(crate) unsafe fn replace_task_ref(slot: &mut *mut TaskHeader, task: *mut TaskHeader) {
    let previous = *slot;
    if previous == task {
        return;
    }

    if !task.is_null() {
        unsafe { retain_task(task) };
    }
    *slot = task;
    if !previous.is_null() {
        unsafe { release_task(previous) };
    }
}

/// Transfers the owned task reference out of an internal waiter slot.
///
/// # Safety
///
/// A non-null pointer in `slot` must own one live task reference. The caller
/// assumes that reference and must eventually pass it to [`release_task`] or
/// transfer it into another owning slot.
#[inline(always)]
pub(crate) unsafe fn take_task_ref(slot: &mut *mut TaskHeader) -> *mut TaskHeader {
    std::mem::replace(slot, std::ptr::null_mut())
}

/// Clears an internal waiter slot and releases its owned task reference.
///
/// # Safety
///
/// A non-null pointer in `slot` must own one live task reference on its owner
/// thread.
#[inline(always)]
pub(crate) unsafe fn clear_task_ref(slot: &mut *mut TaskHeader) {
    let task = unsafe { take_task_ref(slot) };
    if !task.is_null() {
        unsafe { release_task(task) };
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    thread_local! {
        static DESTROY_COUNT: Cell<usize> = const { Cell::new(0) };
    }

    static COUNTING_VTABLE: TaskVTable = TaskVTable {
        poll: |_| Poll::Ready(()),
        finish: |_| {},
        cancel: |_| {},
        destroy: |_| DESTROY_COUNT.with(|count| count.set(count.get() + 1)),
    };

    pub(crate) fn trigger_task_waker_refcount_overflow() {
        let mut task = TaskHeader::new();
        task.refs.set(usize::MAX);
        let task_ptr = &mut task as *mut TaskHeader;
        unsafe { init_cached_waker(task_ptr) };

        let clone = unsafe { cached_waker_ref(task_ptr) }.clone();
        std::mem::forget(clone);
    }

    #[test]
    fn task_waker_extraction_requires_the_stable_flowio_vtable() {
        assert!(task_ptr_from_waker(Waker::noop()).is_none());

        let mut task = TaskHeader::new();
        let task_ptr = &mut task as *mut TaskHeader;
        unsafe { init_cached_waker(task_ptr) };
        let waker = unsafe { cached_waker_ref(task_ptr) };

        assert_eq!(task_ptr_from_waker(waker), Some(task_ptr));
    }

    #[test]
    fn task_waker_refcount_immediately_below_limit_remains_live() {
        DESTROY_COUNT.with(|count| count.set(0));

        let mut task = TaskHeader::new();
        task.vtable = &COUNTING_VTABLE;
        task.refs.set(usize::MAX - 1);
        let task_ptr = &mut task as *mut TaskHeader;
        unsafe { init_cached_waker(task_ptr) };

        let clone = unsafe { cached_waker_ref(task_ptr) }.clone();
        assert_eq!(unsafe { (*task_ptr).refs.get() }, usize::MAX);
        assert_eq!(task_ptr_from_waker(&clone), Some(task_ptr));
        drop(clone);

        assert_eq!(unsafe { (*task_ptr).refs.get() }, usize::MAX - 1);
        DESTROY_COUNT.with(|count| assert_eq!(count.get(), 0));

        // Restore the synthetic count to its one real owning reference before
        // exercising ordinary final release and exact-once destruction.
        unsafe { (*task_ptr).refs.set(1) };
        unsafe { release_task(task_ptr) };
        DESTROY_COUNT.with(|count| assert_eq!(count.get(), 1));
    }
}
