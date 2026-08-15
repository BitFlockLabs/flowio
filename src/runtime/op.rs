//! Per-submission completion state shared between io_uring CQE handling and
//! the runtime's concrete futures.

use crate::runtime::executor::ExecutorOwner;
use crate::runtime::fd::{RuntimeFdCore, RuntimeFdLease};
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
///
/// # Stable lifecycle states
///
/// The flag abbreviations below are `C` completed, `O` orphaned, `D` detached,
/// `P` cancel-pending, `S` runtime-shutdown, `X` context-rejected, `A`
/// ring-abandoned, and `B` build-aborted. A stable state is a handoff boundary
/// at which another lifecycle function may inspect the slot; it does not mean
/// that the state remains live for an extended period.
///
/// | Family | Stable flags | `waiter` / `cancel_next` meaning |
/// | --- | --- | --- |
/// | Fresh, resubmitted, or active | none, optionally `X` after submission | `waiter` is null or owns one task reference; stashed SCTP receives legitimately have no waiter. `cancel_next` is null. |
/// | Completed and future-owned | `C`, `C|S`, `C|X`, or `C|S|X` | Both links are null after target retirement transfers the waiter. |
/// | Completion-to-reclamation | `C|O`, optionally `S` and/or `X`; or `C|D`, optionally `S` | Both links are null and the state is consumed immediately. `O` and `D` are mutually exclusive, and detached operations do not acquire `X`. |
/// | Synthetic completed test setup | `C` | `waiter` may still own one task reference so shutdown re-entry tests can build the pre-existing state directly. `cancel_next` is null. |
/// | Orphan awaiting its target CQE | `O`, optionally `S` and/or `X` | Both links are null unless the state enters the cancel-retry queue. |
/// | Cancel retry | `P` plus at least one of `O` or `S`, optionally `X` | `waiter` is `cancel_prev`, which is null at the queue head; `cancel_next` is the next queue entry. |
/// | Detached close | `D` | Both links are null and no future or task waiter owns the operation. |
/// | Shutdown-owned pending | `S`, optionally `O`, `P`, and/or `X` | Both links are null after shutdown transfers the waiter, except for the `P` queue interpretation above. |
/// | Shutdown stress setup | `S|D`, optionally `P` | Test support exercises detached shutdown states; links use the normal null or cancel-queue interpretation. |
/// | Build-aborted before submission | `B` | `waiter` may still own a task reference or be null; `cancel_next` is null and no target CQE exists. |
/// | Ring-abandoned | `A|S`, `A|O`, or `A|S|O`, optionally `X`; or `A|D` (test stress may add `S`) | Both links are null, the target CQE was not observed, and the slot is never reclaimed. Tests may construct bare `A`. |
///
/// `X` records poll-context rejection without changing ownership. Registry,
/// owner, result, and retained-payload fields are orthogonal to this table.
/// Several mutation sequences deliberately pass through shapes that are not
/// stable handoff states: orphaning sets `O` before transferring the waiter;
/// completion sets `C` before transferring the waiter or reclaiming `C|O` and
/// `C|D`; cancel insertion sets `P` before publishing both links; cancel
/// removal clears links before `P`; reset clears flags before registering the
/// next waiter; shutdown and ring abandonment transfer waiters and cancel links
/// before publishing `S` or `A`; and build-abort publication precedes retained
/// payload destruction. No state may be inspected after task or payload
/// release unless its liveness has been independently re-established.
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
    retained_payload_vtable: Option<&'static RetainedPayloadVtable>,
    /// Non-atomic descriptor lease retained from userspace-SQ publication
    /// through the target CQE. Sequential retries keep this same owner.
    fd_lease: Option<Rc<RuntimeFdCore>>,
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
            fd_lease: None,
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
        self.debug_assert_valid_flags();
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
        self.debug_assert_valid_flags();
    }

    #[inline(always)]
    pub(crate) fn set_ring_abandoned(&mut self) {
        self.state_flags |= Self::FLAG_RING_ABANDONED;
        self.debug_assert_valid_flags();
    }

    #[inline(always)]
    pub(crate) fn set_build_aborted(&mut self) {
        self.state_flags |= Self::FLAG_BUILD_ABORTED;
        self.debug_assert_valid_flags();
    }

    #[inline(always)]
    pub(crate) fn is_context_rejected(&self) -> bool {
        self.state_flags & Self::FLAG_CONTEXT_REJECTED != 0
    }

    #[inline(always)]
    pub(crate) fn set_context_rejected(&mut self) {
        self.state_flags |= Self::FLAG_CONTEXT_REJECTED;
        self.debug_assert_valid_flags();
    }

    /// Asserts the stable flag, waiter, and cancel-link relationships recorded
    /// in this type's lifecycle table.
    ///
    /// Callers must use this only after completing a state-local lifecycle
    /// transaction. Completion/orphan publication and cancel-link mutations
    /// deliberately pass through intermediate shapes that are not valid
    /// handoff boundaries.
    #[inline(always)]
    pub(crate) fn debug_assert_valid_flags(&self) {
        #[cfg(debug_assertions)]
        {
            let known_flags = Self::FLAG_COMPLETED
                | Self::FLAG_ORPHANED
                | Self::FLAG_DETACHED
                | Self::FLAG_CANCEL_PENDING
                | Self::FLAG_RUNTIME_SHUTDOWN
                | Self::FLAG_CONTEXT_REJECTED
                | Self::FLAG_RING_ABANDONED
                | Self::FLAG_BUILD_ABORTED;
            let flags = self.state_flags;
            let completed = flags & Self::FLAG_COMPLETED != 0;
            let orphaned = flags & Self::FLAG_ORPHANED != 0;
            let detached = flags & Self::FLAG_DETACHED != 0;
            let cancel_pending = flags & Self::FLAG_CANCEL_PENDING != 0;
            let runtime_shutdown = flags & Self::FLAG_RUNTIME_SHUTDOWN != 0;
            let context_rejected = flags & Self::FLAG_CONTEXT_REJECTED != 0;
            let ring_abandoned = flags & Self::FLAG_RING_ABANDONED != 0;
            let build_aborted = flags & Self::FLAG_BUILD_ABORTED != 0;

            debug_assert_eq!(
                flags & !known_flags,
                0,
                "completion state contains an unknown lifecycle flag"
            );
            debug_assert!(
                !(orphaned && detached),
                "completion state cannot be both orphaned and detached"
            );
            debug_assert!(
                !cancel_pending || orphaned || runtime_shutdown,
                "cancel-pending state lacks orphan or shutdown ownership"
            );
            debug_assert!(
                !(completed && cancel_pending),
                "completed state remained on the cancel-retry queue"
            );
            debug_assert!(
                !(completed && ring_abandoned),
                "completed state was also classified as ring-abandoned"
            );
            debug_assert!(
                !(ring_abandoned && cancel_pending),
                "ring-abandoned state remained on the cancel-retry queue"
            );
            debug_assert!(
                !build_aborted || flags == Self::FLAG_BUILD_ABORTED,
                "build-aborted state retained a submitted lifecycle flag"
            );
            debug_assert!(
                !(detached && context_rejected),
                "detached state cannot acquire context rejection"
            );
            debug_assert!(
                !ring_abandoned
                    || orphaned
                    || runtime_shutdown
                    || detached
                    || flags == Self::FLAG_RING_ABANDONED,
                "ring-abandoned state lacks abandonment ownership provenance"
            );
            debug_assert!(
                cancel_pending || self.cancel_next.is_null(),
                "completion state retained cancel_next outside the retry queue"
            );
            debug_assert!(
                cancel_pending
                    || !(orphaned || detached || runtime_shutdown || ring_abandoned)
                    || self.waiter.is_null(),
                "completion state retained a task waiter after ownership transfer"
            );
            debug_assert!(
                self.waiter.is_null() || !completed || flags == Self::FLAG_COMPLETED,
                "completed state overlay retained a task waiter"
            );
        }
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

    /// Attaches the descriptor lease immediately before userspace-SQ
    /// publication.
    #[inline(always)]
    pub(crate) fn attach_fd_lease(&mut self, lease: RuntimeFdLease) {
        debug_assert!(
            self.fd_lease.is_none(),
            "completion state already has an fd lease"
        );
        self.fd_lease = Some(lease.into_core());
    }

    /// Returns the raw descriptor from the lease already retained by this
    /// state.
    ///
    /// # Safety
    ///
    /// This completion state must hold the descriptor lease installed by a
    /// published typed fd submission.
    #[inline(always)]
    pub(crate) unsafe fn fd_lease_raw_fd(&self) -> std::os::fd::RawFd {
        debug_assert!(self.fd_lease.is_some(), "completion state has no fd lease");
        // SAFETY: the debug assertion and the typed submission lifecycle
        // establish that resubmission only occurs with the initial lease live.
        let core = unsafe { self.fd_lease.as_ref().unwrap_unchecked() };
        core.raw_fd()
    }

    /// Returns the provenance-preserving core pointer from the attached lease.
    ///
    /// # Safety
    ///
    /// This completion state must hold the descriptor lease installed by a
    /// published typed fd submission. The caller must keep either that lease or
    /// an independently live borrowed parent count alive while using the
    /// pointer; borrowed-state retirement relies on its parent lifetime after
    /// the state lease is reclaimed.
    #[inline(always)]
    pub(crate) unsafe fn fd_lease_core_ptr(&self) -> std::ptr::NonNull<RuntimeFdCore> {
        debug_assert!(self.fd_lease.is_some(), "completion state has no fd lease");
        // SAFETY: the typed publication invariant guarantees the option is
        // populated, and `Rc::as_ptr` preserves the allocation provenance.
        let core = unsafe { self.fd_lease.as_ref().unwrap_unchecked() };
        // SAFETY: an Rc allocation pointer is non-null for its live owner.
        unsafe { std::ptr::NonNull::new_unchecked(Rc::as_ptr(core).cast_mut()) }
    }

    /// Takes the state lease before payload destruction and slot recycling.
    #[inline(always)]
    pub(crate) fn take_fd_lease(&mut self) -> Option<RuntimeFdLease> {
        self.fd_lease.take().map(RuntimeFdLease::from_core)
    }

    #[cfg(test)]
    #[inline(always)]
    pub(crate) fn has_fd_lease(&self) -> bool {
        self.fd_lease.is_some()
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
        self.debug_assert_valid_flags();
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
        unsafe { (*state).debug_assert_valid_flags() };
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
        let waiter = unsafe { take_task_ref(std::ptr::addr_of_mut!((*state).waiter)) };
        unsafe { (*state).debug_assert_valid_flags() };
        waiter
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
        self.debug_assert_valid_flags();
    }

    /// Restores ordinary reclamation after a ringless test has proved the
    /// abandoned-storage shape. A real abandoned state must never be reclaimed.
    #[cfg(test)]
    #[inline(always)]
    pub(crate) fn restore_completed_orphaned_after_ringless_abandonment_for_test(&mut self) {
        debug_assert!(self.is_ring_abandoned());
        debug_assert!(!self.is_cancel_pending());
        debug_assert!(self.waiter.is_null());
        debug_assert!(self.cancel_next.is_null());
        self.state_flags = Self::FLAG_COMPLETED | Self::FLAG_ORPHANED;
        self.debug_assert_valid_flags();
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
const _: [(); 64] = [(); std::mem::align_of::<CompletionState>()];
const _: [(); std::mem::size_of::<usize>()] =
    [(); std::mem::size_of::<Option<&'static RetainedPayloadVtable>>()];
const _: [(); std::mem::size_of::<usize>()] =
    [(); std::mem::size_of::<Option<Rc<RuntimeFdCore>>>()];

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::fd::RuntimeFd;
    use crate::runtime::task::{TaskVTable, release_task};
    use std::cell::Cell;
    use std::task::Poll;

    #[test]
    fn completion_state_layout_matches_fd_lease_declaration() {
        assert_eq!(std::mem::size_of::<CompletionState>(), 64);
        assert_eq!(std::mem::align_of::<CompletionState>(), 64);
        assert_eq!(std::mem::offset_of!(CompletionState, result), 0);
        assert_eq!(std::mem::offset_of!(CompletionState, state_flags), 4);
        assert_eq!(std::mem::offset_of!(CompletionState, registry_index), 8);
        assert_eq!(std::mem::offset_of!(CompletionState, waiter), 16);
        assert_eq!(std::mem::offset_of!(CompletionState, cancel_next), 24);
        assert_eq!(std::mem::offset_of!(CompletionState, retained_payload), 32);
        assert_eq!(
            std::mem::offset_of!(CompletionState, retained_payload_vtable),
            40
        );
        assert_eq!(std::mem::offset_of!(CompletionState, fd_lease), 48);
        assert_eq!(std::mem::offset_of!(CompletionState, owner), 56);
        assert_eq!(
            std::mem::size_of::<Option<&'static RetainedPayloadVtable>>(),
            std::mem::size_of::<usize>()
        );
        assert_eq!(
            std::mem::size_of::<Option<Rc<RuntimeFdCore>>>(),
            std::mem::size_of::<usize>()
        );
    }

    #[test]
    fn sequential_reset_preserves_exact_initial_fd_lease() {
        let fd = RuntimeFd::from_fresh_raw_fd(-1);
        let mut fd_state = fd.op_state();
        let mut state = CompletionState::empty();
        state.attach_fd_lease(unsafe { fd_state.take_initial_lease() });
        assert!(state.has_fd_lease());
        assert_eq!(unsafe { state.fd_lease_raw_fd() }, -1);

        state.set_completed();
        state.reset_for_resubmit();
        assert!(state.has_fd_lease());
        assert_eq!(unsafe { state.fd_lease_raw_fd() }, -1);

        let lease = state.take_fd_lease().expect("state lease missing");
        assert!(!state.has_fd_lease());
        assert_eq!(lease.raw_fd(), -1);
        drop(lease);
    }

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

    #[derive(Clone, Copy)]
    enum StableMutation {
        Completed,
        Orphaned,
        Detached,
        RuntimeShutdown,
        ContextRejected,
        RingAbandoned,
        BuildAborted,
    }

    #[derive(Clone, Copy)]
    enum StableWaiterRole {
        Null,
        Task,
        CancelHead,
        CancelAfter,
    }

    struct StableStateCase {
        name: &'static str,
        mutations: &'static [StableMutation],
        waiter_role: StableWaiterRole,
        expected_flags: u32,
    }

    fn apply_stable_mutation(state: &mut CompletionState, mutation: StableMutation) {
        match mutation {
            StableMutation::Completed => state.set_completed(),
            StableMutation::Orphaned => state.set_orphaned(),
            StableMutation::Detached => state.set_detached(),
            StableMutation::RuntimeShutdown => state.set_runtime_shutdown(),
            StableMutation::ContextRejected => state.set_context_rejected(),
            StableMutation::RingAbandoned => state.set_ring_abandoned(),
            StableMutation::BuildAborted => state.set_build_aborted(),
        }
    }

    fn assert_documented_stable_shape(
        case: &StableStateCase,
        state: &CompletionState,
        task_waiter: *mut TaskHeader,
        cancel_previous: *mut CompletionState,
    ) {
        let flags = state.state_flags;
        let completed = flags & CompletionState::FLAG_COMPLETED != 0;
        let orphaned = flags & CompletionState::FLAG_ORPHANED != 0;
        let detached = flags & CompletionState::FLAG_DETACHED != 0;
        let cancel_pending = flags & CompletionState::FLAG_CANCEL_PENDING != 0;
        let runtime_shutdown = flags & CompletionState::FLAG_RUNTIME_SHUTDOWN != 0;
        let ring_abandoned = flags & CompletionState::FLAG_RING_ABANDONED != 0;
        let build_aborted = flags & CompletionState::FLAG_BUILD_ABORTED != 0;

        assert_eq!(flags, case.expected_flags, "{} flags", case.name);
        if cancel_pending {
            assert!(
                orphaned || runtime_shutdown,
                "{} queued without orphan or shutdown ownership",
                case.name
            );
            assert!(
                !completed && !ring_abandoned && !build_aborted,
                "{} queued after terminal classification",
                case.name
            );
        } else {
            assert!(
                state.cancel_next.is_null(),
                "{} retained a next link outside the cancel queue",
                case.name
            );
        }
        if completed {
            assert!(
                !cancel_pending && !ring_abandoned && !build_aborted,
                "{} completed with an incompatible terminal flag",
                case.name
            );
        }
        if ring_abandoned {
            assert!(
                !completed && !cancel_pending && !build_aborted,
                "{} abandoned after completion or before-submission abort",
                case.name
            );
            assert!(
                state.waiter.is_null(),
                "{} abandoned with a waiter",
                case.name
            );
            assert!(
                state.cancel_next.is_null(),
                "{} abandoned with a cancel link",
                case.name
            );
        }
        if build_aborted {
            assert_eq!(
                flags
                    & (CompletionState::FLAG_COMPLETED
                        | CompletionState::FLAG_ORPHANED
                        | CompletionState::FLAG_DETACHED
                        | CompletionState::FLAG_CANCEL_PENDING
                        | CompletionState::FLAG_RUNTIME_SHUTDOWN
                        | CompletionState::FLAG_RING_ABANDONED),
                0,
                "{} build-aborted after submission ownership",
                case.name
            );
        }
        if (orphaned || detached || runtime_shutdown || ring_abandoned) && !cancel_pending {
            assert!(
                state.waiter.is_null(),
                "{} retained a task waiter after ownership transfer",
                case.name
            );
        }

        match case.waiter_role {
            StableWaiterRole::Null => {
                assert!(state.waiter.is_null(), "{} waiter", case.name);
                assert!(!cancel_pending, "{} null role hid cancel_prev", case.name);
            }
            StableWaiterRole::Task => {
                assert_eq!(state.waiter, task_waiter, "{} task waiter", case.name);
                assert!(!cancel_pending, "{} task waiter was repurposed", case.name);
            }
            StableWaiterRole::CancelHead => {
                assert!(cancel_pending, "{} missing cancel-pending", case.name);
                assert!(
                    state.cancel_prev().is_null(),
                    "{} queue-head prev",
                    case.name
                );
            }
            StableWaiterRole::CancelAfter => {
                assert!(cancel_pending, "{} missing cancel-pending", case.name);
                assert_eq!(
                    state.cancel_prev(),
                    cancel_previous,
                    "{} queued previous link",
                    case.name
                );
            }
        }
    }

    #[test]
    fn completion_state_flag_and_accessor_inventory_is_complete() {
        let source = include_str!("op.rs");
        let flag_names: Vec<_> = source
            .lines()
            .filter_map(|line| {
                let (_, tail) = line.split_once("const FLAG_")?;
                tail.split_once(':').map(|(name, _)| name)
            })
            .collect();
        assert_eq!(
            flag_names,
            [
                "COMPLETED",
                "ORPHANED",
                "DETACHED",
                "CANCEL_PENDING",
                "RUNTIME_SHUTDOWN",
                "CONTEXT_REJECTED",
                "RING_ABANDONED",
                "BUILD_ABORTED",
            ]
        );

        let flags = [
            CompletionState::FLAG_COMPLETED,
            CompletionState::FLAG_ORPHANED,
            CompletionState::FLAG_DETACHED,
            CompletionState::FLAG_CANCEL_PENDING,
            CompletionState::FLAG_RUNTIME_SHUTDOWN,
            CompletionState::FLAG_CONTEXT_REJECTED,
            CompletionState::FLAG_RING_ABANDONED,
            CompletionState::FLAG_BUILD_ABORTED,
        ];
        assert!(flags.into_iter().all(u32::is_power_of_two));
        assert_eq!(flags.into_iter().fold(0, |mask, flag| mask | flag), 0xff);

        let implementation = source
            .split_once("impl CompletionState {")
            .expect("CompletionState implementation missing")
            .1
            .split_once("\n}\n\nimpl InPlaceInit for CompletionState")
            .expect("CompletionState implementation boundary missing")
            .0;
        let mut lifecycle_accessors: Vec<_> = implementation
            .split("fn ")
            .skip(1)
            .filter_map(|tail| {
                let name = tail
                    .trim_start()
                    .split(|ch: char| !(ch.is_ascii_alphanumeric() || ch == '_'))
                    .next()?;
                (name == "empty"
                    || name.starts_with("is_")
                    || name.starts_with("set_")
                    || name.starts_with("clear_")
                    || matches!(
                        name,
                        "debug_assert_valid_flags"
                            | "link_pending_cancel_after"
                            | "cancel_prev"
                            | "register_waiter"
                            | "replace_waiter_unchecked"
                            | "restore_completed_orphaned_after_ringless_abandonment_for_test"
                            | "take_waiter_unchecked"
                            | "reset_for_resubmit"
                    ))
                .then_some(name)
            })
            .collect();
        lifecycle_accessors.sort_unstable();
        assert_eq!(
            lifecycle_accessors,
            [
                "cancel_prev",
                "clear_cancel_pending",
                "clear_pending_cancel_links",
                "clear_waiter_unchecked",
                "debug_assert_valid_flags",
                "empty",
                "is_build_aborted",
                "is_cancel_pending",
                "is_completed",
                "is_context_rejected",
                "is_detached",
                "is_orphaned",
                "is_ring_abandoned",
                "is_runtime_shutdown",
                "link_pending_cancel_after",
                "register_waiter",
                "replace_waiter_unchecked",
                "reset_for_resubmit",
                "restore_completed_orphaned_after_ringless_abandonment_for_test",
                "set_build_aborted",
                "set_cancel_pending",
                "set_cancel_prev",
                "set_completed",
                "set_context_rejected",
                "set_detached",
                "set_orphaned",
                "set_ring_abandoned",
                "set_runtime_shutdown",
                "take_waiter_unchecked",
            ]
        );
    }

    #[test]
    fn completion_state_stable_family_matrix_matches_documented_waiter_roles() {
        use StableMutation as M;
        use StableWaiterRole as W;

        let cases = [
            StableStateCase {
                name: "fresh",
                mutations: &[],
                waiter_role: W::Null,
                expected_flags: 0,
            },
            StableStateCase {
                name: "active waiting",
                mutations: &[],
                waiter_role: W::Task,
                expected_flags: 0,
            },
            StableStateCase {
                name: "context-rejected active without waiter",
                mutations: &[M::ContextRejected],
                waiter_role: W::Null,
                expected_flags: CompletionState::FLAG_CONTEXT_REJECTED,
            },
            StableStateCase {
                name: "context-rejected active waiting",
                mutations: &[M::ContextRejected],
                waiter_role: W::Task,
                expected_flags: CompletionState::FLAG_CONTEXT_REJECTED,
            },
            StableStateCase {
                name: "completed future-owned",
                mutations: &[M::Completed],
                waiter_role: W::Null,
                expected_flags: CompletionState::FLAG_COMPLETED,
            },
            StableStateCase {
                name: "completed test setup retaining waiter",
                mutations: &[M::Completed],
                waiter_role: W::Task,
                expected_flags: CompletionState::FLAG_COMPLETED,
            },
            StableStateCase {
                name: "completed shutdown-owned future",
                mutations: &[M::Completed, M::RuntimeShutdown],
                waiter_role: W::Null,
                expected_flags: CompletionState::FLAG_COMPLETED
                    | CompletionState::FLAG_RUNTIME_SHUTDOWN,
            },
            StableStateCase {
                name: "completed context-rejected future",
                mutations: &[M::Completed, M::ContextRejected],
                waiter_role: W::Null,
                expected_flags: CompletionState::FLAG_COMPLETED
                    | CompletionState::FLAG_CONTEXT_REJECTED,
            },
            StableStateCase {
                name: "completed shutdown context-rejected future",
                mutations: &[M::Completed, M::RuntimeShutdown, M::ContextRejected],
                waiter_role: W::Null,
                expected_flags: CompletionState::FLAG_COMPLETED
                    | CompletionState::FLAG_RUNTIME_SHUTDOWN
                    | CompletionState::FLAG_CONTEXT_REJECTED,
            },
            StableStateCase {
                name: "completed orphan reclamation",
                mutations: &[M::Completed, M::Orphaned],
                waiter_role: W::Null,
                expected_flags: CompletionState::FLAG_COMPLETED | CompletionState::FLAG_ORPHANED,
            },
            StableStateCase {
                name: "completed detached reclamation",
                mutations: &[M::Completed, M::Detached],
                waiter_role: W::Null,
                expected_flags: CompletionState::FLAG_COMPLETED | CompletionState::FLAG_DETACHED,
            },
            StableStateCase {
                name: "completed shutdown orphan reclamation",
                mutations: &[M::Completed, M::RuntimeShutdown, M::Orphaned],
                waiter_role: W::Null,
                expected_flags: CompletionState::FLAG_COMPLETED
                    | CompletionState::FLAG_RUNTIME_SHUTDOWN
                    | CompletionState::FLAG_ORPHANED,
            },
            StableStateCase {
                name: "completed context-rejected orphan reclamation",
                mutations: &[M::Completed, M::Orphaned, M::ContextRejected],
                waiter_role: W::Null,
                expected_flags: CompletionState::FLAG_COMPLETED
                    | CompletionState::FLAG_ORPHANED
                    | CompletionState::FLAG_CONTEXT_REJECTED,
            },
            StableStateCase {
                name: "completed shutdown context-rejected orphan reclamation",
                mutations: &[
                    M::Completed,
                    M::RuntimeShutdown,
                    M::Orphaned,
                    M::ContextRejected,
                ],
                waiter_role: W::Null,
                expected_flags: CompletionState::FLAG_COMPLETED
                    | CompletionState::FLAG_RUNTIME_SHUTDOWN
                    | CompletionState::FLAG_ORPHANED
                    | CompletionState::FLAG_CONTEXT_REJECTED,
            },
            StableStateCase {
                name: "orphan awaiting target",
                mutations: &[M::Orphaned],
                waiter_role: W::Null,
                expected_flags: CompletionState::FLAG_ORPHANED,
            },
            StableStateCase {
                name: "context-rejected orphan",
                mutations: &[M::Orphaned, M::ContextRejected],
                waiter_role: W::Null,
                expected_flags: CompletionState::FLAG_ORPHANED
                    | CompletionState::FLAG_CONTEXT_REJECTED,
            },
            StableStateCase {
                name: "orphan cancel head",
                mutations: &[M::Orphaned],
                waiter_role: W::CancelHead,
                expected_flags: CompletionState::FLAG_ORPHANED
                    | CompletionState::FLAG_CANCEL_PENDING,
            },
            StableStateCase {
                name: "orphan cancel after",
                mutations: &[M::Orphaned],
                waiter_role: W::CancelAfter,
                expected_flags: CompletionState::FLAG_ORPHANED
                    | CompletionState::FLAG_CANCEL_PENDING,
            },
            StableStateCase {
                name: "context-rejected orphan cancel after",
                mutations: &[M::Orphaned, M::ContextRejected],
                waiter_role: W::CancelAfter,
                expected_flags: CompletionState::FLAG_ORPHANED
                    | CompletionState::FLAG_CONTEXT_REJECTED
                    | CompletionState::FLAG_CANCEL_PENDING,
            },
            StableStateCase {
                name: "detached close",
                mutations: &[M::Detached],
                waiter_role: W::Null,
                expected_flags: CompletionState::FLAG_DETACHED,
            },
            StableStateCase {
                name: "shutdown-owned pending",
                mutations: &[M::RuntimeShutdown],
                waiter_role: W::Null,
                expected_flags: CompletionState::FLAG_RUNTIME_SHUTDOWN,
            },
            StableStateCase {
                name: "context-rejected shutdown-owned pending",
                mutations: &[M::RuntimeShutdown, M::ContextRejected],
                waiter_role: W::Null,
                expected_flags: CompletionState::FLAG_RUNTIME_SHUTDOWN
                    | CompletionState::FLAG_CONTEXT_REJECTED,
            },
            StableStateCase {
                name: "shutdown orphan awaiting target",
                mutations: &[M::RuntimeShutdown, M::Orphaned],
                waiter_role: W::Null,
                expected_flags: CompletionState::FLAG_RUNTIME_SHUTDOWN
                    | CompletionState::FLAG_ORPHANED,
            },
            StableStateCase {
                name: "context-rejected shutdown orphan awaiting target",
                mutations: &[M::RuntimeShutdown, M::Orphaned, M::ContextRejected],
                waiter_role: W::Null,
                expected_flags: CompletionState::FLAG_RUNTIME_SHUTDOWN
                    | CompletionState::FLAG_ORPHANED
                    | CompletionState::FLAG_CONTEXT_REJECTED,
            },
            StableStateCase {
                name: "shutdown cancel head",
                mutations: &[M::RuntimeShutdown],
                waiter_role: W::CancelHead,
                expected_flags: CompletionState::FLAG_RUNTIME_SHUTDOWN
                    | CompletionState::FLAG_CANCEL_PENDING,
            },
            StableStateCase {
                name: "context-rejected shutdown cancel head",
                mutations: &[M::RuntimeShutdown, M::ContextRejected],
                waiter_role: W::CancelHead,
                expected_flags: CompletionState::FLAG_RUNTIME_SHUTDOWN
                    | CompletionState::FLAG_CONTEXT_REJECTED
                    | CompletionState::FLAG_CANCEL_PENDING,
            },
            StableStateCase {
                name: "shutdown orphan cancel after",
                mutations: &[M::RuntimeShutdown, M::Orphaned],
                waiter_role: W::CancelAfter,
                expected_flags: CompletionState::FLAG_RUNTIME_SHUTDOWN
                    | CompletionState::FLAG_ORPHANED
                    | CompletionState::FLAG_CANCEL_PENDING,
            },
            StableStateCase {
                name: "context-rejected shutdown orphan cancel after",
                mutations: &[M::RuntimeShutdown, M::Orphaned, M::ContextRejected],
                waiter_role: W::CancelAfter,
                expected_flags: CompletionState::FLAG_RUNTIME_SHUTDOWN
                    | CompletionState::FLAG_ORPHANED
                    | CompletionState::FLAG_CONTEXT_REJECTED
                    | CompletionState::FLAG_CANCEL_PENDING,
            },
            StableStateCase {
                name: "shutdown detached cancel stress",
                mutations: &[M::RuntimeShutdown, M::Detached],
                waiter_role: W::CancelAfter,
                expected_flags: CompletionState::FLAG_RUNTIME_SHUTDOWN
                    | CompletionState::FLAG_DETACHED
                    | CompletionState::FLAG_CANCEL_PENDING,
            },
            StableStateCase {
                name: "build-aborted without waiter",
                mutations: &[M::BuildAborted],
                waiter_role: W::Null,
                expected_flags: CompletionState::FLAG_BUILD_ABORTED,
            },
            StableStateCase {
                name: "build-aborted retaining waiter",
                mutations: &[M::BuildAborted],
                waiter_role: W::Task,
                expected_flags: CompletionState::FLAG_BUILD_ABORTED,
            },
            StableStateCase {
                name: "synthetic bare abandonment",
                mutations: &[M::RingAbandoned],
                waiter_role: W::Null,
                expected_flags: CompletionState::FLAG_RING_ABANDONED,
            },
            StableStateCase {
                name: "shutdown abandonment",
                mutations: &[M::RuntimeShutdown, M::RingAbandoned],
                waiter_role: W::Null,
                expected_flags: CompletionState::FLAG_RUNTIME_SHUTDOWN
                    | CompletionState::FLAG_RING_ABANDONED,
            },
            StableStateCase {
                name: "orphan abandonment",
                mutations: &[M::Orphaned, M::RingAbandoned],
                waiter_role: W::Null,
                expected_flags: CompletionState::FLAG_ORPHANED
                    | CompletionState::FLAG_RING_ABANDONED,
            },
            StableStateCase {
                name: "shutdown orphan abandonment",
                mutations: &[M::RuntimeShutdown, M::Orphaned, M::RingAbandoned],
                waiter_role: W::Null,
                expected_flags: CompletionState::FLAG_RUNTIME_SHUTDOWN
                    | CompletionState::FLAG_ORPHANED
                    | CompletionState::FLAG_RING_ABANDONED,
            },
            StableStateCase {
                name: "detached abandonment",
                mutations: &[M::Detached, M::RingAbandoned],
                waiter_role: W::Null,
                expected_flags: CompletionState::FLAG_DETACHED
                    | CompletionState::FLAG_RING_ABANDONED,
            },
            StableStateCase {
                name: "shutdown detached abandonment stress",
                mutations: &[M::RuntimeShutdown, M::Detached, M::RingAbandoned],
                waiter_role: W::Null,
                expected_flags: CompletionState::FLAG_RUNTIME_SHUTDOWN
                    | CompletionState::FLAG_DETACHED
                    | CompletionState::FLAG_RING_ABANDONED,
            },
            StableStateCase {
                name: "context-rejected shutdown abandonment",
                mutations: &[M::RuntimeShutdown, M::ContextRejected, M::RingAbandoned],
                waiter_role: W::Null,
                expected_flags: CompletionState::FLAG_RUNTIME_SHUTDOWN
                    | CompletionState::FLAG_CONTEXT_REJECTED
                    | CompletionState::FLAG_RING_ABANDONED,
            },
            StableStateCase {
                name: "context-rejected orphan abandonment",
                mutations: &[M::Orphaned, M::ContextRejected, M::RingAbandoned],
                waiter_role: W::Null,
                expected_flags: CompletionState::FLAG_ORPHANED
                    | CompletionState::FLAG_CONTEXT_REJECTED
                    | CompletionState::FLAG_RING_ABANDONED,
            },
            StableStateCase {
                name: "context-rejected shutdown orphan abandonment",
                mutations: &[
                    M::RuntimeShutdown,
                    M::Orphaned,
                    M::ContextRejected,
                    M::RingAbandoned,
                ],
                waiter_role: W::Null,
                expected_flags: CompletionState::FLAG_RUNTIME_SHUTDOWN
                    | CompletionState::FLAG_ORPHANED
                    | CompletionState::FLAG_CONTEXT_REJECTED
                    | CompletionState::FLAG_RING_ABANDONED,
            },
        ];

        let task = TaskHeader::new();
        let task_ptr = &task as *const TaskHeader as *mut TaskHeader;
        let mut observed_flags = 0u32;
        for case in &cases {
            let mut state = CompletionState::empty();
            let mut previous = CompletionState::empty();
            for &mutation in case.mutations {
                apply_stable_mutation(&mut state, mutation);
            }
            match case.waiter_role {
                W::Null => {}
                W::Task => unsafe { state.register_waiter(task_ptr) },
                W::CancelHead => state.link_pending_cancel_after(std::ptr::null_mut()),
                W::CancelAfter => {
                    state.link_pending_cancel_after(std::ptr::addr_of_mut!(previous));
                }
            }

            assert_documented_stable_shape(
                case,
                &state,
                task_ptr,
                std::ptr::addr_of_mut!(previous),
            );
            state.debug_assert_valid_flags();
            observed_flags |= state.state_flags;

            match case.waiter_role {
                W::Task => unsafe {
                    CompletionState::clear_waiter_unchecked(std::ptr::addr_of_mut!(state));
                },
                W::CancelHead | W::CancelAfter => state.clear_pending_cancel_links(),
                W::Null => {}
            }
            assert!(state.waiter.is_null(), "{} cleanup waiter", case.name);
            assert!(state.cancel_next.is_null(), "{} cleanup next", case.name);
            assert_eq!(task.refs.get(), 1, "{} task reference pairing", case.name);
        }

        assert_eq!(observed_flags, 0xff, "stable matrix omitted a flag");
    }

    #[cfg(debug_assertions)]
    #[test]
    fn completion_state_invalid_stable_shapes_fail_at_their_exact_rule() {
        struct InvalidCase {
            name: &'static str,
            flags: u32,
            task_waiter: bool,
            cancel_next: bool,
            expected: &'static str,
        }

        let cases = [
            InvalidCase {
                name: "unknown flag",
                flags: 1 << 8,
                task_waiter: false,
                cancel_next: false,
                expected: "unknown lifecycle flag",
            },
            InvalidCase {
                name: "orphaned detached",
                flags: CompletionState::FLAG_ORPHANED | CompletionState::FLAG_DETACHED,
                task_waiter: false,
                cancel_next: false,
                expected: "both orphaned and detached",
            },
            InvalidCase {
                name: "unowned cancel retry",
                flags: CompletionState::FLAG_CANCEL_PENDING,
                task_waiter: false,
                cancel_next: false,
                expected: "lacks orphan or shutdown ownership",
            },
            InvalidCase {
                name: "completed cancel retry",
                flags: CompletionState::FLAG_COMPLETED
                    | CompletionState::FLAG_ORPHANED
                    | CompletionState::FLAG_CANCEL_PENDING,
                task_waiter: false,
                cancel_next: false,
                expected: "completed state remained on the cancel-retry queue",
            },
            InvalidCase {
                name: "completed abandonment",
                flags: CompletionState::FLAG_COMPLETED | CompletionState::FLAG_RING_ABANDONED,
                task_waiter: false,
                cancel_next: false,
                expected: "completed state was also classified as ring-abandoned",
            },
            InvalidCase {
                name: "abandoned cancel retry",
                flags: CompletionState::FLAG_ORPHANED
                    | CompletionState::FLAG_CANCEL_PENDING
                    | CompletionState::FLAG_RING_ABANDONED,
                task_waiter: false,
                cancel_next: false,
                expected: "ring-abandoned state remained on the cancel-retry queue",
            },
            InvalidCase {
                name: "build-aborted shutdown",
                flags: CompletionState::FLAG_BUILD_ABORTED | CompletionState::FLAG_RUNTIME_SHUTDOWN,
                task_waiter: false,
                cancel_next: false,
                expected: "build-aborted state retained a submitted lifecycle flag",
            },
            InvalidCase {
                name: "build-aborted context rejection",
                flags: CompletionState::FLAG_BUILD_ABORTED | CompletionState::FLAG_CONTEXT_REJECTED,
                task_waiter: false,
                cancel_next: false,
                expected: "build-aborted state retained a submitted lifecycle flag",
            },
            InvalidCase {
                name: "detached context rejection",
                flags: CompletionState::FLAG_DETACHED | CompletionState::FLAG_CONTEXT_REJECTED,
                task_waiter: false,
                cancel_next: false,
                expected: "detached state cannot acquire context rejection",
            },
            InvalidCase {
                name: "context-rejected bare abandonment",
                flags: CompletionState::FLAG_CONTEXT_REJECTED
                    | CompletionState::FLAG_RING_ABANDONED,
                task_waiter: false,
                cancel_next: false,
                expected: "lacks abandonment ownership provenance",
            },
            InvalidCase {
                name: "cancel link outside queue",
                flags: 0,
                task_waiter: false,
                cancel_next: true,
                expected: "retained cancel_next outside the retry queue",
            },
            InvalidCase {
                name: "orphaned task waiter",
                flags: CompletionState::FLAG_ORPHANED,
                task_waiter: true,
                cancel_next: false,
                expected: "task waiter after ownership transfer",
            },
            InvalidCase {
                name: "detached task waiter",
                flags: CompletionState::FLAG_DETACHED,
                task_waiter: true,
                cancel_next: false,
                expected: "task waiter after ownership transfer",
            },
            InvalidCase {
                name: "shutdown task waiter",
                flags: CompletionState::FLAG_RUNTIME_SHUTDOWN,
                task_waiter: true,
                cancel_next: false,
                expected: "task waiter after ownership transfer",
            },
            InvalidCase {
                name: "abandoned task waiter",
                flags: CompletionState::FLAG_RING_ABANDONED,
                task_waiter: true,
                cancel_next: false,
                expected: "task waiter after ownership transfer",
            },
            InvalidCase {
                name: "completed context-rejected task waiter",
                flags: CompletionState::FLAG_COMPLETED | CompletionState::FLAG_CONTEXT_REJECTED,
                task_waiter: true,
                cancel_next: false,
                expected: "completed state overlay retained a task waiter",
            },
        ];

        let task = TaskHeader::new();
        let task_ptr = &task as *const TaskHeader as *mut TaskHeader;
        for case in cases {
            let mut state = CompletionState::empty();
            let mut next = CompletionState::empty();
            if case.task_waiter {
                unsafe { state.register_waiter(task_ptr) };
            }
            state.state_flags = case.flags;
            if case.cancel_next {
                state.cancel_next = std::ptr::addr_of_mut!(next);
            }

            let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                state.debug_assert_valid_flags();
            }))
            .expect_err(case.name);
            let message = panic
                .downcast_ref::<&str>()
                .copied()
                .or_else(|| panic.downcast_ref::<String>().map(String::as_str))
                .expect("validator panic did not carry text");
            assert!(
                message.contains(case.expected),
                "{} reached the wrong rule: {message}",
                case.name
            );

            state.state_flags = 0;
            state.cancel_next = std::ptr::null_mut();
            if case.task_waiter {
                unsafe {
                    CompletionState::clear_waiter_unchecked(std::ptr::addr_of_mut!(state));
                }
            }
            assert_eq!(task.refs.get(), 1, "{} leaked a task ref", case.name);
        }
    }

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
