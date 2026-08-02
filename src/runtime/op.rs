//! Per-submission completion state shared between io_uring CQE handling and
//! the runtime's concrete futures.

use crate::runtime::executor::ExecutorOwner;
use crate::runtime::retained::{RetainedPayload, RetainedPayloadPool, RetainedPayloadVtable};
#[cfg(any(test, feature = "test-support"))]
use crate::runtime::task::clear_task_ref;
use crate::runtime::task::{TaskHeader, replace_task_ref, retain_task, take_task_ref};
use crate::utils::memory::pool::InPlaceInit;
use std::mem::MaybeUninit;
use std::rc::Rc;

/// Per-submission state shared between a pinned future and the io_uring CQE
/// completion path.
///
/// Allocated from the reactor's op pool; futures store only a raw pointer.
/// Each in-flight SQE owns one `CompletionState`. Sequential retry futures
/// may reuse the same slot after they have consumed the previous CQE. Dropping
/// a future with a completed state frees it immediately; dropping a future
/// with an in-flight state marks it orphaned and leaves reclamation to the
/// CQE path.
///
/// # Retained payload invariant
///
/// Any memory referenced by an in-flight SQE must remain retained until the
/// original CQE for that SQE is observed, even if the owning future is dropped.
/// Futures that submit SQEs pointing into caller-owned buffers or scratch
/// arrays attach an erased retained payload here. A live future detaches that
/// payload on normal completion and returns ownership to the caller; the
/// reactor drops it when an orphaned original CQE retires. Cancel CQEs use
/// `user_data == 0` and never release retained payloads.
///
/// If bounded reactor shutdown closes the ring without observing a target CQE,
/// the state remains incomplete and is marked ring-abandoned. Its state and
/// payload storage are deliberately leaked: closing the ring does not prove
/// that the kernel has stopped referencing submitted userspace memory.
///
/// Retained payload storage is backed by the reactor's private retained-payload
/// pool for common payload sizes. Oversized or over-aligned payloads use the
/// documented heap fallback carried by the same erased vtable.
#[doc(hidden)]
#[repr(C, align(64))]
pub struct CompletionState {
    /// CQE result value, stored exactly as returned by the kernel.
    pub result: i32,
    /// Internal state bits such as completed/orphaned.
    pub state_flags: u32,
    /// Index in the owner's bounded live-operation registry.
    pub(crate) registry_index: u32,
    /// Task waiting on this operation, or null when no waiter is registered.
    /// A non-null task pointer owns one task reference. After that reference is
    /// released and `FLAG_CANCEL_PENDING` is set, this word stores the previous
    /// completion state in the reactor's cancel-retry queue instead.
    pub waiter: *mut TaskHeader,
    /// Next completion state in the reactor-owned cancel-retry queue.
    pub(crate) cancel_next: *mut CompletionState,
    /// Erased retained payload whose memory may be referenced by the in-flight
    /// SQE associated with this completion state, or null when no payload is
    /// attached.
    retained_payload: *mut (),
    /// Release vtable for `retained_payload`.
    retained_payload_vtable: Option<RetainedPayloadVtable>,
    /// Stable executor owner that allocated this completion-state slot.
    owner: Option<Rc<ExecutorOwner>>,
}

impl CompletionState {
    /// CQE has been observed and its result fields are valid.
    pub const FLAG_COMPLETED: u32 = 1 << 0;
    /// Owning future was dropped before the kernel retired the submission.
    pub const FLAG_ORPHANED: u32 = 1 << 1;
    /// Operation has no waiting task and is reclaimed when its CQE retires.
    pub const FLAG_DETACHED: u32 = 1 << 2;
    /// `ASYNC_CANCEL` submission failed and the reactor must retry it.
    pub const FLAG_CANCEL_PENDING: u32 = 1 << 3;
    /// Executor shutdown owns cancellation and no task may be woken.
    pub const FLAG_RUNTIME_SHUTDOWN: u32 = 1 << 4;
    /// The future was polled without its originating FlowIO executor context.
    /// Completion still owns any retained payload until the original CQE.
    pub const FLAG_CONTEXT_REJECTED: u32 = 1 << 5;
    /// Reactor teardown abandoned this submission without observing its target
    /// CQE. The state and any retained payload must never be reclaimed.
    pub const FLAG_RING_ABANDONED: u32 = 1 << 6;
    /// User-controlled SQE construction unwound before the operation was
    /// submitted. No target CQE exists, so the future's destructor may reclaim
    /// the state directly instead of issuing cancellation.
    pub const FLAG_BUILD_ABORTED: u32 = 1 << 7;

    #[inline(always)]
    pub(crate) fn empty() -> Self {
        Self {
            result: 0,
            state_flags: 0,
            registry_index: u32::MAX,
            waiter: std::ptr::null_mut(),
            cancel_next: std::ptr::null_mut(),
            retained_payload: std::ptr::null_mut(),
            retained_payload_vtable: None,
            owner: None,
        }
    }

    #[inline(always)]
    pub fn is_completed(&self) -> bool {
        self.state_flags & Self::FLAG_COMPLETED != 0
    }

    #[inline(always)]
    pub fn is_orphaned(&self) -> bool {
        self.state_flags & Self::FLAG_ORPHANED != 0
    }

    #[inline(always)]
    pub fn is_detached(&self) -> bool {
        self.state_flags & Self::FLAG_DETACHED != 0
    }

    #[inline(always)]
    pub(crate) fn is_cancel_pending(&self) -> bool {
        self.state_flags & Self::FLAG_CANCEL_PENDING != 0
    }

    #[inline(always)]
    pub(crate) fn is_runtime_shutdown(&self) -> bool {
        self.state_flags & Self::FLAG_RUNTIME_SHUTDOWN != 0
    }

    #[inline(always)]
    pub(crate) fn is_ring_abandoned(&self) -> bool {
        self.state_flags & Self::FLAG_RING_ABANDONED != 0
    }

    #[inline(always)]
    pub(crate) fn is_build_aborted(&self) -> bool {
        self.state_flags & Self::FLAG_BUILD_ABORTED != 0
    }

    #[inline(always)]
    pub fn set_completed(&mut self) {
        self.state_flags |= Self::FLAG_COMPLETED;
    }

    #[inline(always)]
    pub fn set_orphaned(&mut self) {
        self.state_flags |= Self::FLAG_ORPHANED;
    }

    #[inline(always)]
    pub fn set_detached(&mut self) {
        self.state_flags |= Self::FLAG_DETACHED;
    }

    #[inline(always)]
    pub(crate) fn set_cancel_pending(&mut self) {
        self.state_flags |= Self::FLAG_CANCEL_PENDING;
    }

    #[inline(always)]
    pub(crate) fn clear_cancel_pending(&mut self) {
        self.state_flags &= !Self::FLAG_CANCEL_PENDING;
    }

    /// Initializes this orphaned state's intrusive cancel-retry links.
    ///
    /// The waiter reference must already have been released. While the state
    /// remains queued, `waiter` is interpreted only as `cancel_prev`.
    #[inline(always)]
    pub(crate) fn link_pending_cancel_after(&mut self, previous: *mut CompletionState) {
        debug_assert!(!self.is_cancel_pending());
        debug_assert!(self.waiter.is_null());
        self.set_cancel_pending();
        self.waiter = previous.cast();
        self.cancel_next = std::ptr::null_mut();
    }

    /// Returns the previous cancel-retry queue entry.
    #[inline(always)]
    pub(crate) fn cancel_prev(&self) -> *mut CompletionState {
        debug_assert!(self.is_cancel_pending());
        self.waiter.cast()
    }

    /// Updates the previous cancel-retry queue entry.
    #[inline(always)]
    pub(crate) fn set_cancel_prev(&mut self, previous: *mut CompletionState) {
        debug_assert!(self.is_cancel_pending());
        self.waiter = previous.cast();
    }

    /// Clears both intrusive cancel links and restores the waiter word to null.
    #[inline(always)]
    pub(crate) fn clear_pending_cancel_links(&mut self) {
        debug_assert!(self.is_cancel_pending());
        self.waiter = std::ptr::null_mut();
        self.cancel_next = std::ptr::null_mut();
        self.clear_cancel_pending();
    }

    #[inline(always)]
    pub(crate) fn set_runtime_shutdown(&mut self) {
        self.state_flags |= Self::FLAG_RUNTIME_SHUTDOWN;
    }

    #[inline(always)]
    pub(crate) fn set_ring_abandoned(&mut self) {
        self.state_flags |= Self::FLAG_RING_ABANDONED;
    }

    #[inline(always)]
    pub(crate) fn set_build_aborted(&mut self) {
        self.state_flags |= Self::FLAG_BUILD_ABORTED;
    }

    #[inline(always)]
    pub(crate) fn is_context_rejected(&self) -> bool {
        self.state_flags & Self::FLAG_CONTEXT_REJECTED != 0
    }

    #[inline(always)]
    pub(crate) fn set_context_rejected(&mut self) {
        self.state_flags |= Self::FLAG_CONTEXT_REJECTED;
    }

    #[inline(always)]
    pub(crate) fn bind_owner(&mut self, owner: Option<Rc<ExecutorOwner>>, index: u32) {
        debug_assert!(
            self.owner.is_none(),
            "fresh completion state retained a prior owner"
        );
        debug_assert_eq!(
            self.registry_index,
            u32::MAX,
            "fresh completion state retained a registry index"
        );
        self.owner = owner;
        self.registry_index = index;
    }

    #[inline(always)]
    pub(crate) fn clone_owner(&self) -> Option<Rc<ExecutorOwner>> {
        self.owner.clone()
    }

    #[inline(always)]
    pub(crate) fn owner_ptr(&self) -> *const ExecutorOwner {
        self.owner.as_ref().map_or(std::ptr::null(), Rc::as_ptr)
    }

    /// Asserts that releasing this state's owner cannot destroy the pool whose
    /// mutable borrow returns this slot.
    ///
    /// Production operation reclamation is reached through an active poll,
    /// completion-drain, shutdown, or future-drop owner pin. Ownerless unit
    /// fixtures are intentionally permitted.
    #[inline(always)]
    pub(crate) fn debug_assert_reclaim_owner_pinned(&self) {
        #[cfg(debug_assertions)]
        if let Some(owner) = self.owner.as_ref() {
            debug_assert!(
                Rc::strong_count(owner) > 1,
                "completion-state owner release could re-enter its operation pool"
            );
        }
    }

    /// Registers the initial waiter in a freshly allocated or reset state.
    ///
    /// # Safety
    ///
    /// A non-null `task` must point to a live task on its executor owner thread.
    /// This completion state must be live and exclusively accessible, and its
    /// waiter word must be null.
    #[inline(always)]
    pub(crate) unsafe fn register_waiter(&mut self, task: *mut TaskHeader) {
        debug_assert!(!self.is_cancel_pending());
        debug_assert!(
            self.waiter.is_null(),
            "initial completion waiter was already registered"
        );
        if !task.is_null() {
            unsafe { retain_task(task) };
        }
        self.waiter = task;
    }

    /// Replaces an operation's waiter without retaining a state borrow while
    /// releasing the prior task reference.
    ///
    /// # Safety
    ///
    /// `state` must identify a live, exclusively owned completion state, and a
    /// non-null `task` must identify a live task on its executor owner thread.
    #[inline(always)]
    pub(crate) unsafe fn replace_waiter_unchecked(state: *mut Self, task: *mut TaskHeader) {
        debug_assert!(unsafe { !(*state).is_cancel_pending() });
        unsafe { replace_task_ref(std::ptr::addr_of_mut!((*state).waiter), task) };
    }

    /// Transfers the waiter reference to the caller without releasing it.
    ///
    /// The caller must keep the reference through notification and then call
    /// [`crate::runtime::task::release_task`], or transfer it into another
    /// owning slot.
    ///
    /// # Safety
    ///
    /// `state` must identify a live, exclusively owned completion state.
    #[inline(always)]
    pub(crate) unsafe fn take_waiter_unchecked(state: *mut Self) -> *mut TaskHeader {
        debug_assert!(unsafe { !(*state).is_cancel_pending() });
        unsafe { take_task_ref(std::ptr::addr_of_mut!((*state).waiter)) }
    }

    /// Clears the waiter word without retaining a state borrow while releasing
    /// the task reference.
    ///
    /// # Safety
    ///
    /// `state` must identify a live, exclusively owned completion state on its
    /// executor owner thread.
    #[cfg(any(test, feature = "test-support"))]
    #[inline(always)]
    pub(crate) unsafe fn clear_waiter_unchecked(state: *mut Self) {
        debug_assert!(unsafe { !(*state).is_cancel_pending() });
        unsafe { clear_task_ref(std::ptr::addr_of_mut!((*state).waiter)) };
    }

    /// Attaches a retained payload while preparing this operation.
    ///
    /// After submission, the payload is owned by the completion state until a
    /// live future takes it back or the reactor drops it while retiring an
    /// orphaned original CQE. If SQE construction aborts first, the submission
    /// guard detaches it before the state becomes reclaimable.
    #[inline(always)]
    pub(crate) fn attach_retained_payload<T: 'static>(&mut self, payload: RetainedPayload<T>) {
        debug_assert!(
            self.retained_payload.is_null(),
            "CompletionState already has retained payload"
        );
        let (ptr, vtable) = payload.into_raw_parts();
        self.retained_payload = ptr;
        self.retained_payload_vtable = Some(vtable);
    }

    /// Returns a shared reference to the retained payload.
    ///
    /// # Safety
    ///
    /// The caller must request the exact concrete payload type that was
    /// attached to this completion state.
    #[inline(always)]
    pub(crate) unsafe fn retained_payload<T: 'static>(&self) -> &T {
        debug_assert!(
            !self.retained_payload.is_null(),
            "CompletionState retained payload missing"
        );
        unsafe { &*(self.retained_payload as *const T) }
    }

    /// Returns a mutable reference to the retained payload.
    ///
    /// # Safety
    ///
    /// The caller must request the exact concrete payload type that was
    /// attached to this completion state and must have exclusive logical
    /// access to the operation state.
    #[inline(always)]
    pub(crate) unsafe fn retained_payload_mut<T: 'static>(&mut self) -> &mut T {
        debug_assert!(
            !self.retained_payload.is_null(),
            "CompletionState retained payload missing"
        );
        unsafe { &mut *(self.retained_payload as *mut T) }
    }

    /// Detaches and returns the retained payload.
    ///
    /// # Safety
    ///
    /// The caller must request the exact concrete payload type that was
    /// attached to this completion state and provide the reactor-owned pool
    /// that allocated the retained storage.
    #[inline(always)]
    pub(crate) unsafe fn take_retained_payload<T: 'static>(
        &mut self,
        pool: &mut RetainedPayloadPool,
    ) -> T {
        debug_assert!(
            !self.retained_payload.is_null(),
            "CompletionState retained payload missing"
        );
        let ptr = self.retained_payload as *mut T;
        self.retained_payload = std::ptr::null_mut();
        debug_assert!(
            self.retained_payload_vtable.is_some(),
            "retained payload missing vtable"
        );
        let vtable = unsafe { self.retained_payload_vtable.take().unwrap_unchecked() };
        unsafe { RetainedPayload::from_raw_parts(ptr, vtable).take(pool) }
    }

    /// Detaches the retained payload, extracts selected data from it in place,
    /// and releases only the retained backing storage.
    ///
    /// # Safety
    ///
    /// The caller must request the exact concrete payload type that was
    /// attached to this completion state and provide the reactor-owned pool
    /// that allocated the retained storage. `extract` must move or drop every
    /// initialized field that requires destruction.
    #[inline(always)]
    pub(crate) unsafe fn take_retained_payload_with<T: 'static, R>(
        &mut self,
        pool: &mut RetainedPayloadPool,
        extract: impl FnOnce(*mut T) -> R,
    ) -> R {
        debug_assert!(
            !self.retained_payload.is_null(),
            "CompletionState retained payload missing"
        );
        let ptr = self.retained_payload as *mut T;
        self.retained_payload = std::ptr::null_mut();
        debug_assert!(
            self.retained_payload_vtable.is_some(),
            "retained payload missing vtable"
        );
        let vtable = unsafe { self.retained_payload_vtable.take().unwrap_unchecked() };
        unsafe { RetainedPayload::from_raw_parts(ptr, vtable).take_with(pool, extract) }
    }

    /// Drops any retained payload still attached to this completion state.
    ///
    /// # Safety
    ///
    /// `pool` must be the pool that allocated the attached payload, and the
    /// associated kernel submission must no longer be able to reference it.
    #[inline(always)]
    pub(crate) unsafe fn drop_retained_payload_unchecked(
        state: *mut Self,
        pool: *mut RetainedPayloadPool,
    ) {
        if unsafe { (*state).retained_payload.is_null() } {
            return;
        }
        let ptr = unsafe { (*state).retained_payload };
        debug_assert!(
            unsafe { (*state).retained_payload_vtable.is_some() },
            "retained payload missing vtable"
        );
        let vtable = unsafe { (*state).retained_payload_vtable.take().unwrap_unchecked() };
        unsafe {
            (*state).retained_payload = std::ptr::null_mut();
        }
        unsafe { (vtable.drop_and_free)(ptr, pool) };
    }

    /// Reset a retired completion slot for the next sequential submission.
    ///
    /// This is only valid after the previous CQE has already been observed and
    /// fully consumed by the owning future. It must not be used while the slot
    /// still corresponds to an in-flight submission. Any retained payload is
    /// intentionally preserved so retrying futures can keep one caller-owned
    /// buffer alive across multiple sequential SQEs.
    ///
    /// All state flags are cleared so one-shot lifecycle state does not leak
    /// into retrying read/write submissions that reuse a completion slot.
    #[inline(always)]
    pub fn reset_for_resubmit(&mut self) {
        debug_assert!(
            self.is_completed(),
            "retry completion state was not completed"
        );
        debug_assert!(
            !self.is_cancel_pending(),
            "cannot resubmit a completion state queued for cancel retry"
        );
        debug_assert!(
            self.cancel_next.is_null(),
            "cannot resubmit a linked completion state"
        );
        debug_assert!(
            self.waiter.is_null(),
            "retry completion state retained a waiter"
        );
        self.result = 0;
        self.state_flags = 0;
        self.waiter = std::ptr::null_mut();
        self.cancel_next = std::ptr::null_mut();
    }
}

impl InPlaceInit for CompletionState {
    type Args = ();

    fn init_at(slot: &mut MaybeUninit<Self>, _: Self::Args) {
        // SAFETY: `slot` points to writable storage for one CompletionState;
        // writing `empty()` initializes every field exactly once.
        unsafe {
            slot.as_mut_ptr().write(Self::empty());
        }
    }
}

const _: [(); 64] = [(); std::mem::size_of::<CompletionState>()];

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::task::{TaskVTable, release_task};
    use std::cell::Cell;
    use std::task::Poll;

    thread_local! {
        static REPLACED_WAITER_STATE: Cell<*mut CompletionState> =
            const { Cell::new(std::ptr::null_mut()) };
        static EXPECTED_REPLACEMENT: Cell<*mut TaskHeader> =
            const { Cell::new(std::ptr::null_mut()) };
        static REPLACED_WAITER_DESTROYS: Cell<usize> = const { Cell::new(0) };
    }

    unsafe fn inspect_replaced_waiter(_: *mut TaskHeader) {
        REPLACED_WAITER_DESTROYS.with(|count| count.set(count.get() + 1));
        let state = REPLACED_WAITER_STATE.with(Cell::get);
        let expected = EXPECTED_REPLACEMENT.with(Cell::get);
        assert!(!state.is_null(), "replacement state was not published");
        unsafe {
            assert_eq!(
                (*state).waiter,
                expected,
                "replacement waiter was not published before prior destruction"
            );
            (*state).result = 37;
        }
    }

    static REPLACED_WAITER_VTABLE: TaskVTable = TaskVTable {
        poll: |_| Poll::Ready(()),
        finish: |_| {},
        cancel: |_| {},
        destroy: inspect_replaced_waiter,
    };

    #[test]
    fn completion_waiter_reference_pairing_is_exact() {
        let first = TaskHeader::new();
        let second = TaskHeader::new();
        let first_ptr = &first as *const TaskHeader as *mut TaskHeader;
        let second_ptr = &second as *const TaskHeader as *mut TaskHeader;
        let mut state = CompletionState::empty();

        unsafe { state.register_waiter(first_ptr) };
        assert_eq!(first.refs.get(), 2);

        unsafe {
            CompletionState::replace_waiter_unchecked(std::ptr::addr_of_mut!(state), first_ptr)
        };
        assert_eq!(first.refs.get(), 2, "same waiter retained twice");

        unsafe {
            CompletionState::replace_waiter_unchecked(std::ptr::addr_of_mut!(state), second_ptr)
        };
        assert_eq!(first.refs.get(), 1, "replaced waiter reference leaked");
        assert_eq!(second.refs.get(), 2);

        let transferred =
            unsafe { CompletionState::take_waiter_unchecked(std::ptr::addr_of_mut!(state)) };
        assert_eq!(transferred, second_ptr);
        assert!(state.waiter.is_null());
        assert_eq!(second.refs.get(), 2, "taking waiter released ownership");
        unsafe { release_task(transferred) };
        assert_eq!(second.refs.get(), 1);

        unsafe { state.register_waiter(first_ptr) };
        unsafe {
            CompletionState::clear_waiter_unchecked(std::ptr::addr_of_mut!(state));
        }
        assert_eq!(first.refs.get(), 1, "clearing waiter did not release it");

        unsafe { state.register_waiter(first_ptr) };
        let transferred =
            unsafe { CompletionState::take_waiter_unchecked(std::ptr::addr_of_mut!(state)) };
        unsafe { release_task(transferred) };
        state.set_completed();
        state.reset_for_resubmit();
        assert_eq!(
            first.refs.get(),
            1,
            "resubmit reset changed waiter ownership"
        );
    }

    #[test]
    fn completion_waiter_replacement_publishes_before_reentrant_release() {
        REPLACED_WAITER_DESTROYS.with(|count| count.set(0));
        let mut state = CompletionState::empty();
        let mut prior = TaskHeader::new();
        prior.vtable = &REPLACED_WAITER_VTABLE;
        let prior_ptr = std::ptr::addr_of_mut!(prior);
        let replacement = TaskHeader::new();
        let replacement_ptr = &replacement as *const TaskHeader as *mut TaskHeader;

        unsafe {
            state.register_waiter(prior_ptr);
            release_task(prior_ptr);
        }
        assert_eq!(prior.refs.get(), 1);

        let state_ptr = std::ptr::addr_of_mut!(state);
        REPLACED_WAITER_STATE.with(|stored| stored.set(state_ptr));
        EXPECTED_REPLACEMENT.with(|stored| stored.set(replacement_ptr));
        unsafe {
            CompletionState::replace_waiter_unchecked(state_ptr, replacement_ptr);
        }
        REPLACED_WAITER_STATE.with(|stored| stored.set(std::ptr::null_mut()));
        EXPECTED_REPLACEMENT.with(|stored| stored.set(std::ptr::null_mut()));

        REPLACED_WAITER_DESTROYS.with(|count| assert_eq!(count.get(), 1));
        assert_eq!(state.result, 37);
        assert_eq!(state.waiter, replacement_ptr);
        assert_eq!(replacement.refs.get(), 2);
        unsafe {
            CompletionState::clear_waiter_unchecked(std::ptr::addr_of_mut!(state));
        }
        assert_eq!(replacement.refs.get(), 1);
    }

    #[test]
    fn ring_abandonment_does_not_fabricate_target_completion() {
        let mut state = CompletionState::empty();

        state.set_runtime_shutdown();
        state.set_ring_abandoned();

        assert!(state.is_runtime_shutdown());
        assert!(state.is_ring_abandoned());
        assert!(
            !state.is_completed(),
            "ring abandonment must not expose a payload without a target CQE"
        );
    }
}
