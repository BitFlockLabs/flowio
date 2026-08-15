//! `io_uring` reactor: SQE submission, CQE completion, and operation lifecycle.

use crate::runtime::executor::{
    CompletionDrainGuard, ExecutorOwner, PanicPayload, RuntimeState, completion_drain_active,
    retain_first_panic, run_cleanup_preserving_panic,
};
use crate::runtime::fd::RuntimeFdLease;
use crate::runtime::op::CompletionState;
#[cfg(all(test, not(miri)))]
use crate::runtime::retained::RetainedIovecScratch;
#[cfg(any(debug_assertions, test))]
use crate::runtime::retained::RetainedPayloadPoolStats;
use crate::runtime::retained::{RetainedPayload, RetainedPayloadPool};
use crate::runtime::task::{TaskHeader, release_task};
use crate::utils::disarm_unwind_guard;
use crate::utils::memory::provider::BasicMemoryProvider;
use crate::utils::memory::provider_owned_pool::ProviderOwnedPool;
use io_uring::{IoUring, opcode, types};
use std::collections::VecDeque;
use std::io;
use std::mem::ManuallyDrop;
use std::os::fd::{AsRawFd, IntoRawFd, OwnedFd};
use std::panic::{AssertUnwindSafe, catch_unwind, resume_unwind};
use std::ptr::NonNull;
use std::time::{Duration, Instant};

/// Default number of submission and completion ring entries requested from
/// `io_uring`.
pub const DEFAULT_RING_ENTRIES: u32 = 256;

/// Completion-state records allocated per slab in the internal op pool.
const OP_POOL_OBJS_PER_SLAB: usize = 256;

/// Largest duration representable by Linux's signed kernel timespec.
const MAX_KERNEL_TIMESPEC_DURATION: Duration = Duration::new(i64::MAX as u64, 999_999_999);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ReactorSubmitStatus {
    Ready,
    Busy,
}

#[inline(always)]
fn is_raw_os_error(err: &io::Error, code: libc::c_int) -> bool {
    err.raw_os_error() == Some(code)
}

/// Clamps a Rust duration before `io-uring` casts its seconds to signed
/// kernel storage.
#[inline(always)]
fn bounded_kernel_timespec_duration(duration: Duration) -> Duration {
    duration.min(MAX_KERNEL_TIMESPEC_DURATION)
}

#[cfg(test)]
mod kernel_timespec_tests {
    use super::*;

    #[test]
    fn bounded_kernel_timespec_duration_preserves_and_saturates_exactly() {
        let max_seconds = i64::MAX as u64;
        let cases = [
            (
                Duration::new(max_seconds - 1, 123_456_789),
                Duration::new(max_seconds - 1, 123_456_789),
            ),
            (
                Duration::new(max_seconds, 456_789_123),
                Duration::new(max_seconds, 456_789_123),
            ),
            (MAX_KERNEL_TIMESPEC_DURATION, MAX_KERNEL_TIMESPEC_DURATION),
            (
                Duration::new(max_seconds + 1, 0),
                MAX_KERNEL_TIMESPEC_DURATION,
            ),
            (
                Duration::new(max_seconds + 1, 123_456_789),
                MAX_KERNEL_TIMESPEC_DURATION,
            ),
            (Duration::MAX, MAX_KERNEL_TIMESPEC_DURATION),
        ];

        for (input, expected) in cases {
            assert_eq!(bounded_kernel_timespec_duration(input), expected);
        }
    }
}

#[inline]
fn missing_ext_arg_error() -> io::Error {
    io::Error::new(
        io::ErrorKind::Unsupported,
        "FlowIO requires Linux kernel 5.11 or newer with IORING_ENTER_EXT_ARG support",
    )
}

#[inline]
fn validate_required_ring_features(ring: &IoUring) -> io::Result<()> {
    #[cfg(any(debug_assertions, feature = "test-support"))]
    if crate::runtime::test_hooks::take_reactor_ext_arg_probe_failure() {
        return Err(missing_ext_arg_error());
    }

    if !ring.params().is_feature_ext_arg() {
        return Err(missing_ext_arg_error());
    }
    Ok(())
}

#[inline(always)]
fn cqe_consumes_poll_budget(user_data: u64) -> bool {
    user_data != 0
}

#[inline(always)]
fn retire_tracked_completion(runtime_state: &mut RuntimeState) -> io::Result<()> {
    if runtime_state.inflight_ops == 0 {
        return Err(io::Error::other(
            "CQE observed with no tracked in-flight operation",
        ));
    }

    runtime_state.inflight_ops -= 1;
    #[cfg(debug_assertions)]
    {
        runtime_state.stats.cqe_completions = runtime_state.stats.cqe_completions.saturating_add(1);
    }
    Ok(())
}

/// Owner-thread FIFO of operations awaiting another `ASYNC_CANCEL` attempt.
///
/// The queue is bounded by the reactor's live-operation cap and allocates no
/// nodes. An orphaned state's released waiter word stores the previous link;
/// `CompletionState::cancel_next` stores the next link.
struct PendingCancelQueue {
    head: *mut CompletionState,
    tail: *mut CompletionState,
    len: usize,
}

impl PendingCancelQueue {
    const fn new() -> Self {
        Self {
            head: std::ptr::null_mut(),
            tail: std::ptr::null_mut(),
            len: 0,
        }
    }

    #[inline(always)]
    fn is_empty(&self) -> bool {
        self.head.is_null()
    }

    #[inline(always)]
    fn len(&self) -> usize {
        self.len
    }

    /// Appends one orphaned state unless it is already queued or completed.
    ///
    /// # Safety
    ///
    /// `ptr` must identify a live completion state owned by this reactor. Its
    /// waiter reference must have been released before this call.
    unsafe fn push_back(&mut self, ptr: *mut CompletionState) {
        debug_assert!(
            !ptr.is_null(),
            "pending-cancel push called with null pointer"
        );
        unsafe {
            if (*ptr).is_cancel_pending() || (*ptr).is_completed() {
                (*ptr).debug_assert_valid_flags();
                return;
            }
            debug_assert!(
                (*ptr).is_orphaned() || (*ptr).is_runtime_shutdown(),
                "only orphaned or shutdown-owned ops can queue cancel retry"
            );
            (*ptr).link_pending_cancel_after(self.tail);

            if self.tail.is_null() {
                debug_assert!(self.head.is_null());
                self.head = ptr;
            } else {
                (*self.tail).cancel_next = ptr;
            }
            self.tail = ptr;
            self.len += 1;
            (*ptr).debug_assert_valid_flags();
        }
    }

    /// Removes a known queued state in constant time.
    ///
    /// # Safety
    ///
    /// `ptr` must identify a live completion state owned by this reactor.
    unsafe fn unlink(&mut self, ptr: *mut CompletionState) -> bool {
        debug_assert!(
            !ptr.is_null(),
            "pending-cancel unlink called with null pointer"
        );
        unsafe {
            if !(*ptr).is_cancel_pending() {
                return false;
            }

            let previous = (*ptr).cancel_prev();
            let next = (*ptr).cancel_next;
            if previous.is_null() {
                debug_assert_eq!(self.head, ptr);
                self.head = next;
            } else {
                debug_assert_eq!((*previous).cancel_next, ptr);
                (*previous).cancel_next = next;
            }

            if next.is_null() {
                debug_assert_eq!(self.tail, ptr);
                self.tail = previous;
            } else {
                debug_assert_eq!((*next).cancel_prev(), ptr);
                (*next).set_cancel_prev(previous);
            }

            debug_assert!(self.len > 0);
            self.len -= 1;
            (*ptr).clear_pending_cancel_links();

            if self.head.is_null() {
                debug_assert!(self.tail.is_null());
                debug_assert_eq!(self.len, 0);
                self.tail = std::ptr::null_mut();
            }
            (*ptr).debug_assert_valid_flags();
            true
        }
    }

    /// Pops the oldest retry candidate.
    ///
    /// # Safety
    ///
    /// Every linked state must remain live and owned by this reactor.
    unsafe fn pop_front(&mut self) -> Option<*mut CompletionState> {
        let ptr = self.head;
        if ptr.is_null() {
            return None;
        }
        let removed = unsafe { self.unlink(ptr) };
        debug_assert!(removed);
        Some(ptr)
    }
}

#[inline(always)]
/// Releases every resource owned by one live completion-state slot.
///
/// # Safety
///
/// `ptr` must be checked out from `op_pool`; all pool/list/accounting pointers
/// must describe this reactor. Before an attached payload is released, either
/// the original target CQE must have retired or SQE construction must have
/// aborted before submission.
unsafe fn free_op_fields_with_removal_report<F>(
    pending_cancels: *mut PendingCancelQueue,
    retained_pool: *mut RetainedPayloadPool,
    op_pool: *mut ProviderOwnedPool<CompletionState, BasicMemoryProvider>,
    live_registry: *mut Vec<*mut CompletionState>,
    ptr: *mut CompletionState,
    report_removal: F,
) -> io::Result<()>
where
    F: FnOnce(),
{
    if unsafe { (*live_registry).is_empty() } {
        return Err(io::Error::other(
            "reactor freed more operations than it allocated",
        ));
    }

    let registry_index = unsafe { (*ptr).registry_index as usize };
    if registry_index >= unsafe { (*live_registry).len() }
        || unsafe { (&*live_registry)[registry_index] != ptr }
    {
        return Err(io::Error::other(
            "completion state missing from reactor live registry",
        ));
    }
    let removed = unsafe { (*live_registry).swap_remove(registry_index) };
    debug_assert_eq!(removed, ptr);
    if registry_index < unsafe { (*live_registry).len() } {
        let moved = unsafe { (&*live_registry)[registry_index] };
        unsafe {
            (*moved).registry_index = registry_index as u32;
        }
    }
    unsafe {
        (*ptr).registry_index = u32::MAX;
    }
    // Publish the structural removal before waiter or payload destruction can
    // unwind. Callers that must repair a swap-remove cursor can then use the
    // event itself instead of re-deriving identity from a recyclable address.
    report_removal();

    unsafe { (*pending_cancels).unlink(ptr) };
    let waiter = unsafe { CompletionState::take_waiter_unchecked(ptr) };
    let reclaim = unsafe { OpReclaimGuard::new(retained_pool, op_pool, ptr) };
    if !waiter.is_null() {
        unsafe { release_task(waiter) };
    }
    unsafe { reclaim.finish() };
    Ok(())
}

#[inline(always)]
#[cfg(test)]
unsafe fn free_op_fields(
    pending_cancels: *mut PendingCancelQueue,
    retained_pool: *mut RetainedPayloadPool,
    op_pool: *mut ProviderOwnedPool<CompletionState, BasicMemoryProvider>,
    live_registry: *mut Vec<*mut CompletionState>,
    ptr: *mut CompletionState,
) -> io::Result<()> {
    unsafe {
        free_op_fields_with_removal_report(
            pending_cancels,
            retained_pool,
            op_pool,
            live_registry,
            ptr,
            || {},
        )
    }
}

/// Drops one operation payload and returns its completion-state slot.
///
/// The slot guard is armed before payload destruction so both pooled retained
/// backing and the operation slot return exactly once if user drop glue
/// unwinds.
#[inline(always)]
unsafe fn drop_payload_and_return_op_slot(
    retained_pool: *mut RetainedPayloadPool,
    op_pool: *mut ProviderOwnedPool<CompletionState, BasicMemoryProvider>,
    ptr: *mut CompletionState,
    fd_lease: Option<RuntimeFdLease>,
) {
    let slot = unsafe { OpSlotReturnGuard::new(op_pool, ptr) };
    unsafe { CompletionState::drop_retained_payload_unchecked(ptr, retained_pool) };
    unsafe { slot.finish() };
    // The operation-pool borrow ended when `slot.finish()` returned. A final
    // descriptor release may now route a close through this reactor without
    // re-entering a borrowed pool. If payload destruction unwinds, the slot
    // guard runs before this function parameter is dropped.
    drop(fd_lease);
}

/// Owns complete operation reclamation while waiter destruction can unwind.
///
/// Registry and cancel-queue ownership have already been removed, and the
/// waiter has already been transferred out of the state. The checked-out
/// payload backing and operation slot remain unavailable to reentrant work
/// until this guard finishes or unwinds.
struct OpReclaimGuard {
    retained_pool: *mut RetainedPayloadPool,
    op_pool: *mut ProviderOwnedPool<CompletionState, BasicMemoryProvider>,
    ptr: *mut CompletionState,
    /// Taken before waiter/payload destruction so state Drop cannot release a
    /// final descriptor while the operation pool remains borrowed.
    fd_lease: Option<RuntimeFdLease>,
}

impl OpReclaimGuard {
    #[inline(always)]
    unsafe fn new(
        retained_pool: *mut RetainedPayloadPool,
        op_pool: *mut ProviderOwnedPool<CompletionState, BasicMemoryProvider>,
        ptr: *mut CompletionState,
    ) -> Self {
        let fd_lease = unsafe { (*ptr).take_fd_lease() };
        Self {
            retained_pool,
            op_pool,
            ptr,
            fd_lease,
        }
    }

    #[inline(always)]
    unsafe fn finish(mut self) {
        let fd_lease = self.fd_lease.take();
        let this = disarm_unwind_guard(self);
        unsafe {
            drop_payload_and_return_op_slot(this.retained_pool, this.op_pool, this.ptr, fd_lease);
        }
    }
}

impl Drop for OpReclaimGuard {
    #[cold]
    #[inline(never)]
    fn drop(&mut self) {
        let fd_lease = self.fd_lease.take();
        run_cleanup_preserving_panic(|| unsafe {
            drop_payload_and_return_op_slot(self.retained_pool, self.op_pool, self.ptr, fd_lease);
        });
    }
}

/// Returns one completion-state slot if retained-payload destruction unwinds.
///
/// Registry, cancel-queue, and waiter ownership are removed before this guard
/// is installed. The checked-out slot itself remains unavailable to reentrant
/// owner-thread work until payload destruction finishes or unwinds.
struct OpSlotReturnGuard {
    op_pool: *mut ProviderOwnedPool<CompletionState, BasicMemoryProvider>,
    ptr: *mut CompletionState,
}

impl OpSlotReturnGuard {
    #[inline(always)]
    unsafe fn new(
        op_pool: *mut ProviderOwnedPool<CompletionState, BasicMemoryProvider>,
        ptr: *mut CompletionState,
    ) -> Self {
        Self { op_pool, ptr }
    }

    #[inline(always)]
    unsafe fn finish(self) {
        let mut this = disarm_unwind_guard(self);
        unsafe {
            (*this.ptr).debug_assert_reclaim_owner_pinned();
        }
        unsafe { (*this.op_pool).free(this.ptr) };
    }
}

impl Drop for OpSlotReturnGuard {
    #[cold]
    #[inline(never)]
    fn drop(&mut self) {
        unsafe {
            (*self.ptr).debug_assert_reclaim_owner_pinned();
            (*self.op_pool).free(self.ptr);
        }
    }
}

/// Ensures an orphaned operation remains reactor-owned if waiter destruction
/// unwinds before cancellation can be submitted.
struct PendingCancelEnqueueGuard {
    pending_cancels: *mut PendingCancelQueue,
    ptr: *mut CompletionState,
    armed: bool,
}

impl PendingCancelEnqueueGuard {
    #[inline(always)]
    unsafe fn new(pending_cancels: *mut PendingCancelQueue, ptr: *mut CompletionState) -> Self {
        Self {
            pending_cancels,
            ptr,
            armed: true,
        }
    }

    #[inline(always)]
    fn disarm(mut self) {
        self.armed = false;
    }
}

impl Drop for PendingCancelEnqueueGuard {
    #[inline(always)]
    fn drop(&mut self) {
        if self.armed {
            unsafe {
                (*self.pending_cancels).push_back(self.ptr);
            }
        }
    }
}

#[inline(always)]
unsafe fn orphan_and_clear_waiter_with_cancel_fallback(
    reactor: *mut Reactor,
    ptr: *mut CompletionState,
) -> PendingCancelEnqueueGuard {
    unsafe {
        (*ptr).set_orphaned();
        let pending_cancels = std::ptr::addr_of_mut!((*reactor).pending_cancels);
        let enqueue = PendingCancelEnqueueGuard::new(pending_cancels, ptr);
        let waiter = CompletionState::take_waiter_unchecked(ptr);
        if !waiter.is_null() {
            release_task(waiter);
        }
        enqueue
    }
}

/// Releases one shutdown-owned waiter without letting user drop glue interrupt
/// the remaining reactor safety work.
///
/// # Safety
///
/// `waiter` must be null or own exactly one live task reference on the current
/// executor owner thread. The caller must have transferred that reference out
/// of its completion state before calling this function.
#[inline]
unsafe fn release_shutdown_waiter(waiter: *mut TaskHeader, first_panic: &mut Option<PanicPayload>) {
    if waiter.is_null() {
        return;
    }
    retain_first_panic(
        first_panic,
        catch_unwind(AssertUnwindSafe(|| unsafe {
            release_task(waiter);
        })),
    );
}

/// User-facing `io_uring` setup configuration embedded inside
/// [`crate::runtime::executor::ExecutorConfig`].
///
/// This is construction-time configuration, not a per-operation data fast-path
/// type. Choose the ring size before creating the executor and keep the
/// executor alive for steady-state work.
///
/// FlowIO's timed reactor waits require Linux kernel 5.11 or newer with
/// `IORING_ENTER_EXT_ARG` support. Executor construction reports
/// [`std::io::ErrorKind::Unsupported`] when the running kernel lacks that
/// feature.
///
/// # Example
/// ```no_run
/// use flowio::runtime::reactor::ReactorConfig;
///
/// let config = ReactorConfig {
///     ring_entries: 512,
/// };
/// # let _ = config;
/// ```
#[derive(Clone, Copy)]
pub struct ReactorConfig {
    /// Number of entries requested for both the io_uring submission ring and
    /// completion ring.
    ///
    /// This also caps the number of live FlowIO completion-state records.
    /// Async operations that cannot reserve a completion-state slot return
    /// [`std::io::ErrorKind::WouldBlock`] with their caller-owned payload.
    pub ring_entries: u32,
}

impl Default for ReactorConfig {
    fn default() -> Self {
        Self {
            ring_entries: DEFAULT_RING_ENTRIES,
        }
    }
}

/// Sole userspace owner for a plain socket-close SQE that has not yet crossed
/// the `io_uring_enter` admission boundary.
struct PendingClose {
    /// Wrapping sequence assigned to the corresponding userspace SQ entry.
    sequence: u64,
    /// Descriptor ownership retained until the kernel consumes that entry.
    fd: OwnedFd,
}

#[doc(hidden)]
pub(crate) struct Reactor {
    /// Owned io_uring instance used for submission and completion handling.
    ring: Option<IoUring>,
    /// Stable owner containing this reactor, or null in standalone unit tests.
    owner: *const ExecutorOwner,
    /// Wrapping sequence of the first userspace SQE not consumed by the kernel.
    queued_head: u64,
    /// Wrapping sequence assigned to the next successfully queued SQE.
    next_sequence: u64,
    /// Bounded owners for close SQEs that remain in the userspace SQ.
    pending_closes: VecDeque<PendingClose>,
    /// Explicit close-marker bound, independent of allocator capacity details.
    max_pending_closes: usize,
    /// Bounded owners released while this reactor's completion view is live.
    ///
    /// The post-view guard drains this FIFO immediately after the view and its
    /// thread-local exclusion guard are released.
    deferred_closes: VecDeque<OwnedFd>,
    /// Explicit deferred-owner bound, independent of allocator capacity.
    max_deferred_closes: usize,
    /// Bounded FIFO of orphaned operations whose `ASYNC_CANCEL` SQE could not
    /// be submitted.
    pending_cancels: PendingCancelQueue,
    /// Pool of reusable completion-state records for in-flight operations.
    op_pool: ManuallyDrop<ProviderOwnedPool<CompletionState, BasicMemoryProvider>>,
    /// Maximum number of live completion-state records.
    max_live_ops: usize,
    /// Bounded registry of checked-out completion states used during teardown.
    live_registry: Vec<*mut CompletionState>,
    /// Pool of pointer-stable payload blocks referenced by in-flight
    /// operations, including operations whose owning futures were dropped.
    retained_pool: ManuallyDrop<RetainedPayloadPool>,
    /// True when shutdown closed the ring without observing every target CQE.
    /// The operation and retained-payload pools must then be leaked because the
    /// kernel may still hold pointers into their checked-out storage.
    storage_abandoned: bool,
}

/// Drains descriptor owners only after this reactor's completion view and
/// thread-local exclusion guard have both been released.
///
/// A nested drain may run while a different reactor's completion view remains
/// live. The cleanup submits only to this guard's exact reactor, whose own view
/// is already gone.
struct DeferredCloseDrainGuard {
    reactor: *mut Reactor,
    runtime_state: *mut RuntimeState,
}

impl DeferredCloseDrainGuard {
    #[inline(always)]
    unsafe fn new(reactor: *mut Reactor, runtime_state: *mut RuntimeState) -> Self {
        Self {
            reactor,
            runtime_state,
        }
    }
}

impl Drop for DeferredCloseDrainGuard {
    #[inline(always)]
    fn drop(&mut self) {
        let deferred = unsafe { std::ptr::addr_of!((*self.reactor).deferred_closes) };
        if unsafe { (*deferred).is_empty() } {
            return;
        }
        run_cleanup_preserving_panic(|| unsafe {
            Reactor::drain_deferred_closes_unchecked(self.reactor, self.runtime_state);
        });
    }
}

impl Reactor {
    pub fn new_with_config(config: ReactorConfig) -> io::Result<Self> {
        let ring = IoUring::new(config.ring_entries)?;
        validate_required_ring_features(&ring)?;

        let op_pool = ProviderOwnedPool::new(BasicMemoryProvider::new(), OP_POOL_OBJS_PER_SLAB)
            .map_err(|_| io::Error::from(io::ErrorKind::InvalidInput))?;
        let retained_pool = RetainedPayloadPool::new()?;
        let mut live_registry = Vec::new();
        live_registry
            .try_reserve_exact(config.ring_entries as usize)
            .map_err(|_| io::Error::from(io::ErrorKind::OutOfMemory))?;
        let mut pending_closes = VecDeque::new();
        pending_closes
            .try_reserve_exact(config.ring_entries as usize)
            .map_err(|_| io::Error::from(io::ErrorKind::OutOfMemory))?;
        let mut deferred_closes = VecDeque::new();
        deferred_closes
            .try_reserve_exact(config.ring_entries as usize)
            .map_err(|_| io::Error::from(io::ErrorKind::OutOfMemory))?;

        Ok(Self {
            ring: Some(ring),
            owner: std::ptr::null(),
            queued_head: 0,
            next_sequence: 0,
            pending_closes,
            max_pending_closes: config.ring_entries as usize,
            deferred_closes,
            max_deferred_closes: config.ring_entries as usize,
            pending_cancels: PendingCancelQueue::new(),
            op_pool: ManuallyDrop::new(op_pool),
            max_live_ops: config.ring_entries as usize,
            live_registry,
            retained_pool: ManuallyDrop::new(retained_pool),
            storage_abandoned: false,
        })
    }

    #[cfg(test)]
    pub(crate) fn new_ringless_for_test(max_live_ops: usize) -> io::Result<Self> {
        let op_pool = ProviderOwnedPool::new(BasicMemoryProvider::new(), OP_POOL_OBJS_PER_SLAB)
            .map_err(|_| io::Error::from(io::ErrorKind::InvalidInput))?;
        let retained_pool = RetainedPayloadPool::new()?;
        let mut live_registry = Vec::new();
        live_registry
            .try_reserve_exact(max_live_ops)
            .map_err(|_| io::Error::from(io::ErrorKind::OutOfMemory))?;
        let mut pending_closes = VecDeque::new();
        pending_closes
            .try_reserve_exact(max_live_ops)
            .map_err(|_| io::Error::from(io::ErrorKind::OutOfMemory))?;
        let mut deferred_closes = VecDeque::new();
        deferred_closes
            .try_reserve_exact(max_live_ops)
            .map_err(|_| io::Error::from(io::ErrorKind::OutOfMemory))?;

        let mut reactor = Self {
            ring: None,
            owner: std::ptr::null(),
            queued_head: 0,
            next_sequence: 0,
            pending_closes,
            max_pending_closes: max_live_ops,
            deferred_closes,
            max_deferred_closes: max_live_ops,
            pending_cancels: PendingCancelQueue::new(),
            op_pool: ManuallyDrop::new(op_pool),
            max_live_ops,
            live_registry,
            retained_pool: ManuallyDrop::new(retained_pool),
            storage_abandoned: false,
        };
        reactor.init();
        Ok(reactor)
    }

    pub fn init(&mut self) {
        self.op_pool.init();
    }

    pub(crate) fn bind_owner(&mut self, owner: *const ExecutorOwner) {
        self.owner = owner;
    }

    #[cfg(all(test, not(miri)))]
    pub(crate) fn test_storage_abandoned(&self) -> bool {
        self.storage_abandoned
    }

    #[inline(always)]
    fn ring_mut(&mut self) -> io::Result<&mut IoUring> {
        self.ring
            .as_mut()
            .ok_or_else(|| io::Error::from(io::ErrorKind::BrokenPipe))
    }

    /// Allocate a fresh `CompletionState` for one SQE submission.
    #[inline(always)]
    pub fn alloc_op(&mut self) -> *mut CompletionState {
        #[cfg(any(debug_assertions, feature = "test-support"))]
        if crate::runtime::test_hooks::take_op_alloc_failure() {
            return std::ptr::null_mut();
        }

        if self.live_registry.len() >= self.max_live_ops {
            return std::ptr::null_mut();
        }

        let ptr = unsafe { self.op_pool.alloc(()).unwrap_or(std::ptr::null_mut()) };
        if !ptr.is_null() {
            let owner = if self.owner.is_null() {
                None
            } else {
                Some(unsafe { ExecutorOwner::clone_rc(self.owner) })
            };
            let registry_index = self.live_registry.len() as u32;
            unsafe {
                (*ptr).bind_owner(owner, registry_index);
            }
            self.live_registry.push(ptr);
            unsafe {
                (*ptr).debug_assert_valid_flags();
            }
        }
        ptr
    }

    /// Allocate pointer-stable retained payload storage for an in-flight op.
    #[inline(always)]
    pub(crate) fn alloc_retained_payload<T: 'static>(&mut self, value: T) -> RetainedPayload<T> {
        self.retained_pool.alloc(value)
    }

    /// Returns the active reactor's retained-pool address without creating a
    /// Rust borrow that could overlap owner-thread callback re-entry.
    ///
    /// # Safety
    ///
    /// `reactor` must identify the live reactor for the currently polling
    /// FlowIO task. The returned pointer may be used only synchronously on that
    /// owner thread and must not outlive the active poll.
    #[inline(always)]
    pub(crate) unsafe fn retained_payload_pool_ptr(
        reactor: *mut Self,
    ) -> NonNull<RetainedPayloadPool> {
        debug_assert!(!reactor.is_null(), "retained-pool reactor must be non-null");
        let pool = unsafe {
            std::ptr::addr_of_mut!((*reactor).retained_pool).cast::<RetainedPayloadPool>()
        };
        unsafe { NonNull::new_unchecked(pool) }
    }

    /// Allocate retained kernel-facing `iovec` scratch for a vectored I/O op.
    #[cfg(all(test, not(miri)))]
    #[inline(always)]
    pub(crate) fn alloc_iovec_scratch(
        &mut self,
        iov_count: usize,
    ) -> io::Result<RetainedIovecScratch> {
        self.retained_pool.alloc_iovec_scratch(iov_count)
    }

    /// Detaches a retained payload without creating a whole-reactor mutable
    /// reference.
    ///
    /// # Safety
    ///
    /// `reactor` must own the live completed `ptr`, whose retained payload must
    /// have exactly type `T`. The owner thread must have exclusive logical
    /// access to the completion state and retained pool.
    #[inline(always)]
    pub(crate) unsafe fn take_retained_payload_unchecked<T: 'static>(
        reactor: *mut Self,
        ptr: *mut CompletionState,
    ) -> T {
        let pool = unsafe {
            std::ptr::addr_of_mut!((*reactor).retained_pool).cast::<RetainedPayloadPool>()
        };
        unsafe { (*ptr).take_retained_payload::<T>(&mut *pool) }
    }

    /// Extracts selected retained data without creating a whole-reactor mutable
    /// reference.
    ///
    /// # Safety
    ///
    /// The requirements of [`Self::take_retained_payload_unchecked`] apply,
    /// and `extract` must move or drop every initialized field requiring
    /// destruction.
    #[inline(always)]
    pub(crate) unsafe fn take_retained_payload_with_unchecked<T: 'static, R>(
        reactor: *mut Self,
        ptr: *mut CompletionState,
        extract: impl FnOnce(*mut T) -> R,
    ) -> R {
        let pool = unsafe {
            std::ptr::addr_of_mut!((*reactor).retained_pool).cast::<RetainedPayloadPool>()
        };
        unsafe { (*ptr).take_retained_payload_with::<T, R>(&mut *pool, extract) }
    }

    #[cfg(any(debug_assertions, test))]
    pub(crate) fn retained_payload_stats(&self) -> RetainedPayloadPoolStats {
        self.retained_pool.stats()
    }

    #[cfg(test)]
    #[cfg_attr(miri, allow(dead_code))]
    pub(crate) fn live_op_count(&self) -> usize {
        self.live_registry.len()
    }

    #[inline(always)]
    fn submit_cancel_sqe(&mut self, ptr: *mut CompletionState) -> io::Result<()> {
        let sqe = opcode::AsyncCancel::new(ptr as u64).build().user_data(0);
        self.submit_sqe(sqe)
    }

    #[inline(always)]
    fn has_pending_cancels(&self) -> bool {
        !self.pending_cancels.is_empty()
    }

    #[inline(always)]
    fn queued_sqe_count(&self) -> u64 {
        self.next_sequence.wrapping_sub(self.queued_head)
    }

    #[inline(always)]
    fn has_queued_sqes(&self) -> bool {
        self.queued_head != self.next_sequence
    }

    /// Relinquishes userspace ownership for the prefix consumed by one
    /// successful `io_uring_enter` call.
    ///
    /// A valid plain socket-close SQE removes its fd-table entry while the
    /// kernel consumes the SQE. Until this function suppresses the matching
    /// `OwnedFd`, that numeric fd may already be reusable. Keep this loop free
    /// of allocation, callbacks, logging, and panic-capable invariant checks.
    #[inline(always)]
    fn retire_submitted(&mut self, submitted: usize) -> io::Result<()> {
        let old_head = self.queued_head;
        let submitted = submitted as u64;
        let queued = self.queued_sqe_count();
        let consumed_prefix = submitted.min(queued);

        loop {
            let consumed = match self.pending_closes.front() {
                Some(marker) => marker.sequence.wrapping_sub(old_head) < consumed_prefix,
                None => false,
            };
            if !consumed {
                break;
            }
            let Some(marker) = self.pending_closes.pop_front() else {
                break;
            };
            let _ = marker.fd.into_raw_fd();
        }

        self.queued_head = old_head.wrapping_add(consumed_prefix);
        if submitted > queued {
            return Err(io::Error::other(
                "io_uring reported more submissions than FlowIO queued",
            ));
        }
        if self.queued_head == self.next_sequence {
            debug_assert!(self.pending_closes.is_empty());
            self.queued_head = 0;
            self.next_sequence = 0;
        }
        Ok(())
    }

    /// Releases close owners whose SQEs never crossed into the kernel, plus
    /// any post-view owners left before close-SQE construction.
    ///
    /// The ring must already be gone (or SQPOLL must remain disabled, as it is
    /// for every FlowIO reactor), so no queued SQE can consume these fds later.
    fn drop_unsubmitted_close_owners(&mut self) {
        self.pending_closes.clear();
        self.deferred_closes.clear();
        self.queued_head = 0;
        self.next_sequence = 0;
    }

    /// Retains one sole descriptor owner without touching the completion
    /// drain's borrowed ring field.
    ///
    /// # Safety
    ///
    /// `reactor` must be the exact reactor published for the active completion
    /// drain on its owner thread. The deferred queue must have its
    /// construction-time reservation and no other code may access it
    /// concurrently.
    #[inline(always)]
    pub(crate) unsafe fn defer_close_during_completion_drain(
        reactor: *mut Self,
        fd: OwnedFd,
    ) -> Result<(), OwnedFd> {
        debug_assert!(
            completion_drain_active(),
            "descriptor deferral requires an active completion drain"
        );
        debug_assert!(!reactor.is_null(), "descriptor deferral requires a reactor");
        let deferred = unsafe { std::ptr::addr_of_mut!((*reactor).deferred_closes) };
        let max_deferred = unsafe { *std::ptr::addr_of!((*reactor).max_deferred_closes) };
        debug_assert!(
            unsafe { (*deferred).capacity() } >= max_deferred,
            "deferred-close queue lost its construction-time reservation"
        );
        if unsafe { (*deferred).len() } >= max_deferred {
            return Err(fd);
        }
        unsafe {
            (*deferred).push_back(fd);
        }
        Ok(())
    }

    /// Submits one known-nonblocking close to a specific reactor.
    ///
    /// # Safety
    ///
    /// `reactor` and `runtime_state` must be the matching initialized
    /// owner-thread fields. This reactor's completion view must no longer be
    /// live, though a nested view for a different reactor may remain active.
    pub(crate) unsafe fn try_submit_close_on_reactor(
        reactor: *mut Self,
        runtime_state: *mut RuntimeState,
        fd: OwnedFd,
    ) -> Result<(), OwnedFd> {
        let op = unsafe { (&mut *reactor).alloc_op() };
        if op.is_null() {
            #[cfg(debug_assertions)]
            unsafe {
                let stats = &mut (*runtime_state).stats;
                stats.close_ring_fallbacks = stats.close_ring_fallbacks.saturating_add(1);
            }
            return Err(fd);
        }

        unsafe {
            (*op).set_detached();
        }
        match unsafe { (&mut *reactor).submit_close_sqe(fd, op as u64) } {
            Ok(()) => {
                unsafe {
                    (*runtime_state).inflight_ops += 1;
                }
                #[cfg(debug_assertions)]
                unsafe {
                    let stats = &mut (*runtime_state).stats;
                    stats.sqe_submits = stats.sqe_submits.saturating_add(1);
                    stats.close_ring_submissions = stats.close_ring_submissions.saturating_add(1);
                }
                Ok(())
            }
            Err((_err, fd)) => {
                unsafe {
                    Self::free_op_unchecked(reactor, op);
                }
                #[cfg(debug_assertions)]
                unsafe {
                    let stats = &mut (*runtime_state).stats;
                    stats.close_ring_fallbacks = stats.close_ring_fallbacks.saturating_add(1);
                }
                Err(fd)
            }
        }
    }

    /// Empties the post-completion descriptor FIFO without retaining a queue
    /// borrow across close submission or direct fallback.
    ///
    /// # Safety
    ///
    /// The requirements of [`Self::try_submit_close_on_reactor`] apply, and
    /// this reactor's completion view and exclusion guard must already be
    /// released.
    unsafe fn drain_deferred_closes_unchecked(
        reactor: *mut Self,
        runtime_state: *mut RuntimeState,
    ) {
        loop {
            let deferred = unsafe { std::ptr::addr_of_mut!((*reactor).deferred_closes) };
            let Some(fd) = (unsafe { (*deferred).pop_front() }) else {
                break;
            };
            if let Err(fd) =
                unsafe { Self::try_submit_close_on_reactor(reactor, runtime_state, fd) }
            {
                #[cfg(debug_assertions)]
                unsafe {
                    let stats = &mut (*runtime_state).stats;
                    stats.close_direct_closes = stats.close_direct_closes.saturating_add(1);
                }
                drop(fd);
            }
        }
    }

    #[inline(always)]
    fn submit_ring(&mut self) -> io::Result<usize> {
        #[cfg(any(debug_assertions, feature = "test-support"))]
        if let Some(err) = crate::runtime::test_hooks::take_ring_submit_failure() {
            return Err(err);
        }

        let submitted = self.ring_mut()?.submit()?;
        self.retire_submitted(submitted)?;
        Ok(submitted)
    }

    #[inline(always)]
    fn refresh_completion_queue_view_after_submit_busy(&mut self) {
        if let Some(ring) = self.ring.as_mut() {
            let mut cq = ring.completion();
            cq.sync();
        }
    }

    #[inline(always)]
    fn submit_ring_for_sqe_capacity(&mut self) -> io::Result<()> {
        let mut retried_after_busy = false;
        loop {
            match self.submit_ring() {
                Ok(submitted) => {
                    let full = self.ring_mut()?.submission().is_full();
                    if !full {
                        return Ok(());
                    }
                    if submitted == 0 {
                        return Err(io::Error::from(io::ErrorKind::WouldBlock));
                    }
                }
                Err(err) if is_raw_os_error(&err, libc::EINTR) => continue,
                Err(err) if is_raw_os_error(&err, libc::EBUSY) => {
                    if retried_after_busy {
                        return Err(io::Error::from(io::ErrorKind::WouldBlock));
                    }
                    retried_after_busy = true;
                    self.refresh_completion_queue_view_after_submit_busy();
                }
                Err(err) => return Err(err),
            }
        }
    }

    fn queue_pending_cancel(&mut self, ptr: *mut CompletionState) {
        unsafe { self.pending_cancels.push_back(ptr) };
    }

    fn retry_pending_cancels(&mut self) {
        // Capture one pass's work budget before retrying. Failed submissions
        // append at the tail and are not attempted again until the next pass.
        let retry_budget = self.pending_cancels.len();
        for _ in 0..retry_budget {
            let Some(current) = (unsafe { self.pending_cancels.pop_front() }) else {
                break;
            };

            let should_retry = unsafe {
                ((*current).is_orphaned() || (*current).is_runtime_shutdown())
                    && !(*current).is_completed()
            };
            if should_retry && self.submit_cancel_sqe(current).is_err() {
                self.queue_pending_cancel(current);
            }
        }
    }

    #[inline(always)]
    fn submit_with_args(
        &mut self,
        min_complete: usize,
        args: &types::SubmitArgs<'_, '_>,
    ) -> io::Result<usize> {
        #[cfg(any(debug_assertions, feature = "test-support"))]
        if let Some(err) = crate::runtime::test_hooks::take_ring_wait_failure() {
            return Err(err);
        }

        let submitted = self
            .ring_mut()?
            .submitter()
            .submit_with_args(min_complete, args)?;
        self.retire_submitted(submitted)?;
        Ok(submitted)
    }

    #[inline(always)]
    fn submit_and_wait(&mut self, min_complete: usize) -> io::Result<usize> {
        #[cfg(any(debug_assertions, feature = "test-support"))]
        if let Some(err) = crate::runtime::test_hooks::take_ring_wait_failure() {
            return Err(err);
        }

        let submitted = self.ring_mut()?.submit_and_wait(min_complete)?;
        self.retire_submitted(submitted)?;
        Ok(submitted)
    }

    /// Return a retired `CompletionState` to the pool.
    ///
    /// This releases any retained payload before recycling the state slot. The
    /// retained payload, if present, owns memory referenced by the original
    /// SQE and must only be released after that original CQE has been observed.
    #[cfg(test)]
    #[inline(always)]
    fn try_free_op(&mut self, ptr: *mut CompletionState) -> io::Result<()> {
        unsafe { Self::try_free_op_unchecked(self, ptr) }
    }

    /// Reclaims a retired operation through disjoint raw field pointers.
    ///
    /// # Safety
    ///
    /// `reactor` must identify the reactor that owns the live retired `ptr`.
    /// The ring may remain mutably borrowed by a completion view, but none of
    /// the bookkeeping fields addressed here may be accessed concurrently.
    #[inline(always)]
    unsafe fn try_free_op_unchecked(
        reactor: *mut Self,
        ptr: *mut CompletionState,
    ) -> io::Result<()> {
        unsafe { Self::try_free_op_unchecked_with_removal_report(reactor, ptr, || {}) }
    }

    /// Reporting variant of [`Self::try_free_op_unchecked`].
    ///
    /// `report_removal` runs after the live-registry entry and any moved index
    /// are repaired, but before waiter or retained-payload destruction.
    #[inline(always)]
    unsafe fn try_free_op_unchecked_with_removal_report<F>(
        reactor: *mut Self,
        ptr: *mut CompletionState,
        report_removal: F,
    ) -> io::Result<()>
    where
        F: FnOnce(),
    {
        debug_assert!(!ptr.is_null(), "reactor free_op called with null pointer");
        unsafe {
            free_op_fields_with_removal_report(
                std::ptr::addr_of_mut!((*reactor).pending_cancels),
                std::ptr::addr_of_mut!((*reactor).retained_pool).cast::<RetainedPayloadPool>(),
                std::ptr::addr_of_mut!((*reactor).op_pool)
                    .cast::<ProviderOwnedPool<CompletionState, BasicMemoryProvider>>(),
                std::ptr::addr_of_mut!((*reactor).live_registry),
                ptr,
                report_removal,
            )
        }
    }

    #[cfg(test)]
    #[inline(always)]
    pub fn free_op(&mut self, ptr: *mut CompletionState) {
        if let Err(err) = self.try_free_op(ptr) {
            debug_assert!(false, "reactor free_op failed: {err}");
        }
    }

    /// Reclaims a completed operation through a raw reactor pointer without
    /// creating a whole-reactor mutable reference.
    ///
    /// # Safety
    ///
    /// `reactor` and `ptr` must satisfy [`Self::try_free_op_unchecked`], and the
    /// caller must have exclusive logical access to the bookkeeping fields.
    pub(crate) unsafe fn free_op_unchecked(reactor: *mut Self, ptr: *mut CompletionState) {
        if let Err(err) = unsafe { Self::try_free_op_unchecked(reactor, ptr) } {
            debug_assert!(false, "reactor raw free_op failed: {err}");
        }
    }

    /// Defers cancellation discovered from a destructor while the completion
    /// view has the ring mutably borrowed.
    ///
    /// # Safety
    ///
    /// `reactor` must own the live submitted `ptr`; this must run on its owner
    /// thread while the thread-wide completion-drain flag is active.
    pub(crate) unsafe fn defer_cancel_during_completion_drain(
        reactor: *mut Self,
        ptr: *mut CompletionState,
    ) {
        unsafe {
            let enqueue = orphan_and_clear_waiter_with_cancel_fallback(reactor, ptr);
            drop(enqueue);
        }
    }

    /// Mark an in-flight operation as orphaned and submit `ASYNC_CANCEL`.
    /// The `CompletionState` remains owned by the reactor until the CQE path
    /// reclaims it.
    #[cfg(test)]
    #[cfg_attr(miri, allow(dead_code))]
    pub fn cancel_op(&mut self, ptr: *mut CompletionState) {
        unsafe { Self::cancel_op_unchecked(self, ptr) };
    }

    /// Raw-pointer form of [`Self::cancel_op`] that remains sound when waiter
    /// destruction synchronously re-enters reactor bookkeeping.
    ///
    /// # Safety
    ///
    /// `reactor` must own the live submitted `ptr` on its owner thread, with
    /// exclusive logical access to its cancellation fields.
    pub(crate) unsafe fn cancel_op_unchecked(reactor: *mut Self, ptr: *mut CompletionState) {
        unsafe {
            let enqueue = orphan_and_clear_waiter_with_cancel_fallback(reactor, ptr);
            enqueue.disarm();
            (&mut *reactor).request_cancel(ptr);
        }
    }

    fn request_cancel(&mut self, ptr: *mut CompletionState) {
        // Submit ASYNC_CANCEL targeting the original user_data (the state ptr).
        // The cancel SQE's own user_data is 0 so poll_io silently skips its CQE.
        // If submission fails, keep the orphaned op on a reactor-owned retry
        // list so a never-completing op is not permanently stranded.
        if self.submit_cancel_sqe(ptr).is_err() {
            self.queue_pending_cancel(ptr);
        }
    }

    /// Push an SQE into the submission queue without flushing to the kernel.
    /// The executor calls [`Self::flush_sqes`] after each task-poll batch.
    #[inline(always)]
    pub fn submit_sqe(&mut self, sqe: io_uring::squeue::Entry) -> io::Result<()> {
        #[cfg(any(debug_assertions, feature = "test-support"))]
        if let Some(err) = crate::runtime::test_hooks::take_raw_sqe_submit_failure() {
            return Err(err);
        }

        let ring = self
            .ring
            .as_mut()
            .ok_or_else(|| io::Error::from(io::ErrorKind::BrokenPipe))?;
        let mut sq = ring.submission();
        if sq.is_full() {
            drop(sq);
            self.submit_ring_for_sqe_capacity()?;
            // SAFETY: capacity handling may submit or synchronize the ring,
            // but cannot remove or replace it.
            sq = unsafe { self.ring.as_mut().unwrap_unchecked() }.submission();
        }

        let next_sequence = &mut self.next_sequence;
        unsafe {
            if sq.push(&sqe).is_err() {
                return Err(io::Error::from(io::ErrorKind::WouldBlock));
            }
        }
        *next_sequence = next_sequence.wrapping_add(1);
        drop(sq);
        Ok(())
    }

    /// Queues one private plain socket-close SQE while retaining ownership
    /// until a successful kernel submission consumes its exact SQ position.
    ///
    /// This method constructs the only accepted entry shape itself:
    /// `IORING_OP_CLOSE` over this owner's valid socket fd, without ASYNC,
    /// DRAIN, LINK, FIXED, IOPOLL, or SQPOLL semantics.
    pub(crate) fn submit_close_sqe(
        &mut self,
        fd: OwnedFd,
        user_data: u64,
    ) -> Result<(), (io::Error, OwnedFd)> {
        #[cfg(any(debug_assertions, feature = "test-support"))]
        if let Some(err) = crate::runtime::test_hooks::take_raw_sqe_submit_failure() {
            return Err((err, fd));
        }

        let sqe = opcode::Close::new(types::Fd(fd.as_raw_fd()))
            .build()
            .user_data(user_data);
        let ring = match self.ring.as_mut() {
            Some(ring) => ring,
            None => return Err((io::Error::from(io::ErrorKind::BrokenPipe), fd)),
        };
        let mut sq = ring.submission();
        if sq.is_full() {
            drop(sq);
            if let Err(err) = self.submit_ring_for_sqe_capacity() {
                return Err((err, fd));
            }
            // SAFETY: capacity handling may submit or synchronize the ring,
            // but cannot remove or replace it.
            sq = unsafe { self.ring.as_mut().unwrap_unchecked() }.submission();
        }

        if self.pending_closes.len() >= self.max_pending_closes {
            return Err((io::Error::from(io::ErrorKind::WouldBlock), fd));
        }

        let pending_closes = &mut self.pending_closes;
        let next_sequence = &mut self.next_sequence;
        let sequence = *next_sequence;
        pending_closes.push_back(PendingClose { sequence, fd });
        if unsafe { sq.push(&sqe) }.is_err() {
            // SAFETY: this function appended exactly one marker immediately
            // before the failed push, and no code can mutate the deque between
            // those two operations.
            let marker = unsafe { pending_closes.pop_back().unwrap_unchecked() };
            return Err((io::Error::from(io::ErrorKind::WouldBlock), marker.fd));
        }
        *next_sequence = next_sequence.wrapping_add(1);
        drop(sq);
        Ok(())
    }

    #[inline(always)]
    /// Flushes any queued SQEs to the kernel submission queue.
    pub fn flush_sqes(&mut self) -> io::Result<ReactorSubmitStatus> {
        self.retry_pending_cancels();
        while self.has_queued_sqes() {
            match self.submit_ring() {
                Ok(0) => return Ok(ReactorSubmitStatus::Busy),
                Ok(_) => {}
                Err(err) if is_raw_os_error(&err, libc::EINTR) => continue,
                Err(err) if is_raw_os_error(&err, libc::EBUSY) => {
                    return Ok(ReactorSubmitStatus::Busy);
                }
                Err(err) => return Err(err),
            }
        }
        if self.has_pending_cancels() {
            return Ok(ReactorSubmitStatus::Busy);
        }
        Ok(ReactorSubmitStatus::Ready)
    }

    /// Waits until at least one completion is available or the optional
    /// timeout expires.
    /// Durations beyond Linux's signed kernel-timespec range saturate to its
    /// maximum representable value.
    ///
    /// `EINTR` is retried here because it is not a useful control signal for
    /// callers of the async runtime. Applications that need signal-driven
    /// shutdown should wake the runtime through a registered fd such as
    /// `signalfd` or `eventfd`; interrupted `io_uring_enter` waits are not
    /// surfaced as an executor error.
    pub fn wait_for_events(
        &mut self,
        timeout: Option<Duration>,
    ) -> io::Result<ReactorSubmitStatus> {
        // Timed `io_uring_enter` errors do not carry a submitted-prefix count.
        // Flush ownership-bearing closes first with the count-returning submit
        // path, then wait with an empty userspace SQ.
        if self.flush_sqes()? == ReactorSubmitStatus::Busy {
            return Ok(ReactorSubmitStatus::Busy);
        }
        if let Some(timeout) = timeout {
            let initial_timeout = if timeout.is_zero() {
                Duration::from_nanos(1)
            } else {
                timeout
            };
            let started = Instant::now();
            let mut elapsed = Duration::ZERO;
            loop {
                if elapsed >= initial_timeout {
                    return Ok(ReactorSubmitStatus::Ready);
                }
                let remaining = initial_timeout.saturating_sub(elapsed);
                let timeout = if remaining.is_zero() {
                    Duration::from_nanos(1)
                } else {
                    remaining
                };
                let timespec = types::Timespec::from(bounded_kernel_timespec_duration(timeout));
                let args = types::SubmitArgs::new().timespec(&timespec);
                match self.submit_with_args(1, &args) {
                    // `flush_sqes` emptied the userspace SQ before this wait.
                    // Keep this cheap check as defense if a future submit
                    // wrapper can leave an unconsumed suffix.
                    Ok(_) if self.has_queued_sqes() => {
                        return Ok(ReactorSubmitStatus::Busy);
                    }
                    Ok(_) => return Ok(ReactorSubmitStatus::Ready),
                    Err(err) if is_raw_os_error(&err, libc::ETIME) => {
                        return Ok(if self.has_queued_sqes() {
                            ReactorSubmitStatus::Busy
                        } else {
                            ReactorSubmitStatus::Ready
                        });
                    }
                    Err(err) if is_raw_os_error(&err, libc::EINTR) => {
                        elapsed = started.elapsed();
                        continue;
                    }
                    Err(err) if is_raw_os_error(&err, libc::EBUSY) => {
                        return Ok(ReactorSubmitStatus::Busy);
                    }
                    Err(err) => {
                        return Err(err);
                    }
                }
            }
        } else {
            loop {
                match self.submit_and_wait(1) {
                    // As above, this is defensive after the preceding flush;
                    // the ordinary wait path has no queued userspace SQEs.
                    Ok(_) if self.has_queued_sqes() => {
                        return Ok(ReactorSubmitStatus::Busy);
                    }
                    Ok(_) => return Ok(ReactorSubmitStatus::Ready),
                    Err(err) if is_raw_os_error(&err, libc::EINTR) => continue,
                    Err(err) if is_raw_os_error(&err, libc::EBUSY) => {
                        return Ok(ReactorSubmitStatus::Busy);
                    }
                    Err(err) => return Err(err),
                }
            }
        }
    }

    pub(crate) unsafe fn prepare_shutdown_unchecked(
        reactor: *mut Self,
        first_panic: &mut Option<PanicPayload>,
    ) {
        let mut index = 0usize;
        while index < unsafe { (*std::ptr::addr_of!((*reactor).live_registry)).len() } {
            let state = unsafe { (&*std::ptr::addr_of!((*reactor).live_registry))[index] };
            let released_waiter = unsafe {
                if (*state).is_orphaned() || (*state).is_detached() {
                    index += 1;
                    continue;
                }
                let owns_waiter = !(*state).is_cancel_pending() && !(*state).waiter.is_null();
                if ((*state).is_runtime_shutdown() || (*state).is_build_aborted()) && !owns_waiter {
                    index += 1;
                    continue;
                }

                debug_assert!(!(*state).is_cancel_pending());
                let waiter = CompletionState::take_waiter_unchecked(state);
                if (*state).is_build_aborted() {
                    release_shutdown_waiter(waiter, first_panic);
                } else {
                    if !(*state).is_runtime_shutdown() {
                        (*state).set_runtime_shutdown();
                        if (*state).is_completed() {
                            (*state).result = -libc::ECANCELED;
                        } else {
                            (&mut *reactor).request_cancel(state);
                        }
                    }
                    release_shutdown_waiter(waiter, first_panic);
                }
                !waiter.is_null()
            };
            if released_waiter {
                // A waiter destructor may remove one state and append another,
                // preserving registry length while swap-removing an unvisited
                // tail behind this cursor. Each waiter is transferred at most
                // once, so restarting remains bounded.
                index = 0;
            } else {
                index += 1;
            }
        }
    }

    /// Closes the ring and classifies every state whose target CQE was not
    /// observed, while retaining the first user panic raised by cleanup.
    ///
    /// # Safety
    ///
    /// `reactor` and `runtime_state` must satisfy [`Self::shutdown_unchecked`].
    /// The caller must have decided to abandon the bounded retirement attempt.
    unsafe fn abandon_shutdown_storage_unchecked(
        reactor: *mut Self,
        runtime_state: *mut RuntimeState,
        first_panic: &mut Option<PanicPayload>,
    ) {
        // Ring close bounds shutdown latency but does not synchronously prove
        // that the kernel has stopped referencing submitted userspace memory.
        // Publish abandonment before any remaining user drop glue can run.
        unsafe {
            (*reactor).storage_abandoned = true;
            drop((&mut *std::ptr::addr_of_mut!((*reactor).ring)).take());
            (&mut *reactor).drop_unsubmitted_close_owners();
            (*runtime_state).inflight_ops = 0;
        }

        let mut index = 0usize;
        while index < unsafe { (&*std::ptr::addr_of!((*reactor).live_registry)).len() } {
            let state = unsafe { (&*std::ptr::addr_of!((*reactor).live_registry))[index] };
            let mut released_waiter = false;
            let mut reclaimed_current = false;
            unsafe {
                if (*state).is_build_aborted() {
                    (*std::ptr::addr_of_mut!((*reactor).pending_cancels)).unlink(state);
                    let waiter = CompletionState::take_waiter_unchecked(state);
                    released_waiter = !waiter.is_null();
                    release_shutdown_waiter(waiter, first_panic);
                } else if !(*state).is_completed() {
                    (*std::ptr::addr_of_mut!((*reactor).pending_cancels)).unlink(state);
                    let waiter = CompletionState::take_waiter_unchecked(state);
                    (*state).set_ring_abandoned();
                    released_waiter = !waiter.is_null();
                    release_shutdown_waiter(waiter, first_panic);
                } else if (*state).is_orphaned() || (*state).is_detached() {
                    retain_first_panic(
                        first_panic,
                        catch_unwind(AssertUnwindSafe(|| {
                            if let Err(err) = Self::try_free_op_unchecked_with_removal_report(
                                reactor,
                                state,
                                || {
                                    reclaimed_current = true;
                                },
                            ) {
                                debug_assert!(false, "reactor raw free_op failed: {err}");
                            }
                        })),
                    );
                } else {
                    (*std::ptr::addr_of_mut!((*reactor).pending_cancels)).unlink(state);
                    index += 1;
                    continue;
                }
            }

            if released_waiter || reclaimed_current {
                // Waiter and retained-payload destruction may mutate the
                // registry without changing its length. Revisit from the
                // front after either callback-capable path.
                index = 0;
            } else {
                index += 1;
            }
        }
        debug_assert!(unsafe { (*reactor).pending_cancels.is_empty() });
        unsafe {
            (*reactor).pending_cancels = PendingCancelQueue::new();
        }
    }

    /// Attempts to retire kernel-visible submissions before closing the ring.
    ///
    /// Completed state still owned by escaped futures remains available. If
    /// the bounded drain cannot observe every target CQE, unresolved state and
    /// both backing pools are deliberately abandoned because ring close alone
    /// does not prove that kernel-visible userspace memory is quiescent.
    ///
    /// # Safety
    ///
    /// `reactor` must identify one initialized, heap-stable owner-thread
    /// reactor. `runtime_state` and `ready_queue` must be its matching live
    /// executor fields. The caller must provide exclusive logical access and
    /// must not retain a whole-reactor mutable reference across this call;
    /// synchronous task and payload destruction may re-enter disjoint fields.
    pub(crate) unsafe fn shutdown_unchecked(
        reactor: *mut Self,
        runtime_state: *mut RuntimeState,
        ready_queue: *mut crate::utils::list::intrusive::dlist::DList<
            crate::runtime::task::TaskHeader,
        >,
    ) {
        debug_assert!(!reactor.is_null(), "shutdown requires a reactor");
        if unsafe { (&*std::ptr::addr_of!((*reactor).ring)).is_none() } {
            return;
        }

        let mut first_panic = None;
        unsafe { Self::prepare_shutdown_unchecked(reactor, &mut first_panic) };
        #[cfg(any(debug_assertions, feature = "test-support"))]
        let force_fallback = crate::runtime::test_hooks::take_reactor_shutdown_fallback();
        #[cfg(not(any(debug_assertions, feature = "test-support")))]
        let force_fallback = false;
        let shutdown_started = Instant::now();
        while !force_fallback
            && unsafe { (*runtime_state).inflight_ops > 0 }
            && shutdown_started.elapsed() < Duration::from_secs(1)
        {
            if unsafe { (&mut *reactor).flush_sqes() }.is_err() {
                break;
            }
            match catch_unwind(AssertUnwindSafe(|| unsafe {
                Self::poll_io_unchecked(reactor, usize::MAX, runtime_state, ready_queue)
            })) {
                Ok(Ok(_)) => {}
                Ok(Err(_)) => break,
                Err(payload) => {
                    retain_first_panic(&mut first_panic, Err(payload));
                    break;
                }
            }
            if unsafe { (*runtime_state).inflight_ops == 0 } {
                break;
            }
            if unsafe { (&mut *reactor).wait_for_events(Some(Duration::from_millis(10))) }.is_err()
            {
                break;
            }
        }

        if unsafe { (*runtime_state).inflight_ops > 0 } {
            unsafe {
                Self::abandon_shutdown_storage_unchecked(reactor, runtime_state, &mut first_panic);
            }
        } else {
            unsafe {
                drop((&mut *std::ptr::addr_of_mut!((*reactor).ring)).take());
                (&mut *reactor).drop_unsubmitted_close_owners();
            }
        }

        if let Some(payload) = first_panic {
            resume_unwind(payload);
        }
    }

    /// Retires one target completion through raw, disjoint reactor fields.
    ///
    /// # Safety
    ///
    /// `reactor` must own the live submitted `state`; `owner`,
    /// `runtime_state`, and `ready_queue` must describe its active executor.
    /// The target CQE must have been consumed exactly once. The caller must not
    /// retain a whole-reactor mutable reference while task or payload
    /// destruction may re-enter this function's bookkeeping fields.
    #[inline(always)]
    pub(crate) unsafe fn retire_completion_unchecked(
        reactor: *mut Self,
        owner: *const ExecutorOwner,
        state: *mut CompletionState,
        result: i32,
        runtime_state: *mut RuntimeState,
        ready_queue: *mut crate::utils::list::intrusive::dlist::DList<TaskHeader>,
    ) -> io::Result<()> {
        unsafe {
            (*state).result = result;
            (*state).set_completed();
            retire_tracked_completion(&mut *runtime_state)?;

            if (*state).is_runtime_shutdown() {
                (*state).result = -libc::ECANCELED;
                if (*state).is_orphaned() || (*state).is_detached() {
                    Self::try_free_op_unchecked(reactor, state)?;
                } else {
                    (*std::ptr::addr_of_mut!((*reactor).pending_cancels)).unlink(state);
                    (*state).debug_assert_valid_flags();
                }
            } else if (*state).is_orphaned() || (*state).is_detached() {
                Self::try_free_op_unchecked(reactor, state)?;
            } else {
                let waiter = CompletionState::take_waiter_unchecked(state);
                if !waiter.is_null() {
                    #[cfg(debug_assertions)]
                    {
                        let stats = &mut (*runtime_state).stats;
                        stats.waiter_wakes = stats.waiter_wakes.saturating_add(1);
                    }
                    crate::runtime::executor::notify_reactor_waiter_unchecked(
                        waiter,
                        owner,
                        ready_queue,
                        runtime_state,
                    );
                    release_task(waiter);
                }
            }
        }
        Ok(())
    }

    /// Drains completed CQEs, updates `CompletionState`, and wakes waiting
    /// tasks as needed.
    ///
    /// `max_completions` limits budget-consuming target CQEs. Cancel CQEs
    /// (`user_data == 0`) do not count against that budget while the loop is
    /// still draining. Once the budget is exhausted, no further CQEs are popped;
    /// a cancel CQE queued behind the boundary is left for the next pass rather
    /// than risking loss of a target CQE.
    ///
    /// `runtime_state` and `ready_queue` belong to this reactor's executor and
    /// must remain valid for the whole call. A waiter owned by another executor
    /// on the same thread is routed through that task's stable owner instead of
    /// these origin pointers.
    ///
    /// # Safety
    ///
    /// `reactor` must identify the initialized, heap-stable reactor matching
    /// the live `runtime_state` and `ready_queue` fields on its owner thread.
    /// The caller must provide exclusive logical access without retaining a
    /// whole-reactor mutable reference. This call must own the only active
    /// completion drain for this ring; nested drains may target other rings.
    pub(crate) unsafe fn poll_io_unchecked(
        reactor: *mut Self,
        max_completions: usize,
        runtime_state: *mut crate::runtime::executor::RuntimeState,
        ready_queue: *mut crate::utils::list::intrusive::dlist::DList<
            crate::runtime::task::TaskHeader,
        >,
    ) -> io::Result<usize> {
        debug_assert!(!reactor.is_null(), "completion drain requires a reactor");
        let ring = unsafe { &mut *std::ptr::addr_of_mut!((*reactor).ring) }
            .as_mut()
            .ok_or_else(|| io::Error::from(io::ErrorKind::BrokenPipe))?
            as *mut IoUring;
        let owner = unsafe { (*reactor).owner };
        // Reverse local-drop order is deliberate: release `cq`, restore the
        // prior thread-local drain target, then submit owners accumulated in
        // this exact reactor's bounded deferred list. The final guard also runs
        // on error or unwind and preserves an already-active user panic.
        let _deferred_close_drain = unsafe { DeferredCloseDrainGuard::new(reactor, runtime_state) };
        let _completion_drain = CompletionDrainGuard::enter_for_reactor(reactor);
        // SAFETY: the completion view exclusively borrows the ring field.
        // Completion handling mutates only disjoint bookkeeping fields, and
        // the guard above prevents destructor-driven polling, cancellation,
        // reclamation, or descriptor submission from borrowing the ring until
        // `cq` is dropped.
        let mut cq = unsafe { (&mut *ring).completion() };

        let mut seen = 0usize;
        while seen < max_completions {
            let Some(cqe) = cq.next() else {
                break;
            };
            let user_data = cqe.user_data();
            if !cqe_consumes_poll_budget(user_data) {
                // Cancel SQE completion — silently skip. Retained payloads are
                // released only when the original target CQE is observed.
                continue;
            }

            let state = user_data as *mut CompletionState;
            unsafe {
                Self::retire_completion_unchecked(
                    reactor,
                    owner,
                    state,
                    cqe.result(),
                    runtime_state,
                    ready_queue,
                )?;
            }
            seen += 1;
        }

        Ok(seen)
    }
}

impl Drop for Reactor {
    fn drop(&mut self) {
        if self.ring.is_some() {
            drop(self.ring.take());
        }
        self.drop_unsubmitted_close_owners();
        debug_assert!(
            self.storage_abandoned || self.live_registry.is_empty(),
            "reactor dropped with live completion states"
        );
        if !self.storage_abandoned {
            unsafe {
                ManuallyDrop::drop(&mut self.op_pool);
                ManuallyDrop::drop(&mut self.retained_pool);
            }
        }
    }
}

/// Candidate-only codegen probe for target-completion state reclamation.
///
/// # Safety
///
/// `reactor` must point to the owner-thread reactor that exclusively owns the
/// live completed `state`. The state must be ready for exactly one ordinary
/// target-CQE reclamation and may hold the final descriptor lease.
#[cfg(feature = "test-support")]
#[unsafe(no_mangle)]
#[inline(never)]
pub unsafe extern "C" fn flowio_slice305_probe_reclaim_target(reactor: *mut (), state: *mut ()) {
    unsafe {
        Reactor::free_op_unchecked(reactor.cast::<Reactor>(), state.cast::<CompletionState>())
    };
}

#[cfg(any(feature = "test-support", all(test, not(miri))))]
fn submit_and_wait_retry_eintr(reactor: &mut Reactor, min_complete: usize) -> io::Result<usize> {
    loop {
        match reactor.submit_and_wait(min_complete) {
            Err(err) if is_raw_os_error(&err, libc::EINTR) => continue,
            result => return result,
        }
    }
}

#[cfg(any(test, all(feature = "test-support", not(miri))))]
mod completion_drain_probe {
    use super::*;
    use crate::runtime::executor::{CloseSubmission, completion_drain_active, try_submit_close};
    #[cfg(not(miri))]
    use crate::runtime::executor::{Executor, ExecutorConfig, UnsubmittedOpGuard};
    #[cfg(not(miri))]
    use crate::runtime::fd::{
        RetainedListenerFd, RuntimeFd, distinctive_closeable_test_fd, raw_fd_is_closed,
    };
    #[cfg(not(miri))]
    use crate::runtime::io::Nop;
    use std::cell::Cell;
    #[cfg(not(miri))]
    use std::future::Future;
    #[cfg(not(miri))]
    use std::io::Write;
    #[cfg(not(miri))]
    use std::os::fd::{AsRawFd, FromRawFd};
    #[cfg(not(miri))]
    use std::os::unix::net::UnixStream;
    #[cfg(not(miri))]
    use std::pin::Pin;
    use std::rc::Rc;
    #[cfg(not(miri))]
    use std::task::{Context, Poll};

    unsafe fn reactor_owns_state(reactor: *mut Reactor, state: *mut CompletionState) -> bool {
        !state.is_null()
            && unsafe { (&*std::ptr::addr_of!((*reactor).live_registry)).contains(&state) }
    }

    pub(super) struct CompletionDrainDescriptor {
        pub(super) fd: Option<OwnedFd>,
        pub(super) armed: Rc<Cell<bool>>,
        pub(super) rejected: Rc<Cell<bool>>,
        pub(super) saw_active_drain: Rc<Cell<bool>>,
    }

    impl Drop for CompletionDrainDescriptor {
        fn drop(&mut self) {
            let Some(fd) = self.fd.take() else {
                return;
            };
            if !self.armed.get() {
                drop(fd);
                return;
            }
            self.saw_active_drain.set(completion_drain_active());
            match try_submit_close(fd) {
                CloseSubmission::Rejected(fd) => {
                    self.rejected.set(true);
                    drop(fd);
                }
                CloseSubmission::OutsideExecutor(fd) => {
                    drop(fd);
                    panic!("completion-drain descriptor reached the outside-executor route");
                }
                CloseSubmission::Submitted => {
                    panic!("completion-drain descriptor re-entered ring submission");
                }
                CloseSubmission::Deferred => {
                    panic!("standalone completion probe unexpectedly deferred a descriptor");
                }
            }
        }
    }

    #[cfg(not(miri))]
    pub(super) struct CompletionDrainCloseOwners {
        nop: ManuallyDrop<Nop>,
        runtime_fd: Option<RuntimeFd>,
        retained_listener: Option<RetainedListenerFd>,
        runtime_raw: i32,
        listener_raw: i32,
        drops: Rc<Cell<usize>>,
        saw_active_drain: Rc<Cell<bool>>,
        owners_open_after_drop: Rc<Cell<bool>>,
        fail_close_submit: bool,
        panic_after_drop: bool,
    }

    #[cfg(not(miri))]
    #[derive(Debug)]
    pub(super) struct CompletionDrainClosePanic;

    #[cfg(not(miri))]
    impl Drop for CompletionDrainCloseOwners {
        fn drop(&mut self) {
            self.drops.set(self.drops.get() + 1);
            self.saw_active_drain.set(completion_drain_active());
            // Release the completed operation before the descriptors so its
            // slot is available to the post-view close submissions.
            unsafe {
                ManuallyDrop::drop(&mut self.nop);
            }
            drop(self.runtime_fd.take());
            drop(self.retained_listener.take());
            self.owners_open_after_drop.set(
                (self.runtime_raw < 0 || !raw_fd_is_closed(self.runtime_raw))
                    && (self.listener_raw < 0 || !raw_fd_is_closed(self.listener_raw)),
            );
            if self.fail_close_submit {
                crate::runtime::test_hooks::fail_next_raw_sqe_submit();
            }
            if self.panic_after_drop {
                std::panic::panic_any(CompletionDrainClosePanic);
            }
        }
    }

    #[cfg(not(miri))]
    pub(super) struct ReturnPendingNopWithCloseOwners {
        nop: Option<Nop>,
        runtime_fd: Option<RuntimeFd>,
        retained_listener: Option<RetainedListenerFd>,
        runtime_raw: i32,
        listener_raw: i32,
        drops: Rc<Cell<usize>>,
        saw_active_drain: Rc<Cell<bool>>,
        owners_open_after_drop: Rc<Cell<bool>>,
        fail_close_submit: bool,
        panic_after_drop: bool,
    }

    #[cfg(not(miri))]
    impl ReturnPendingNopWithCloseOwners {
        #[allow(clippy::too_many_arguments)]
        pub(super) fn new(
            runtime_fd: Option<RuntimeFd>,
            retained_listener: Option<RetainedListenerFd>,
            runtime_raw: i32,
            listener_raw: i32,
            drops: Rc<Cell<usize>>,
            saw_active_drain: Rc<Cell<bool>>,
            owners_open_after_drop: Rc<Cell<bool>>,
            fail_close_submit: bool,
            panic_after_drop: bool,
        ) -> Self {
            Self {
                nop: Some(Nop::new()),
                runtime_fd,
                retained_listener,
                runtime_raw,
                listener_raw,
                drops,
                saw_active_drain,
                owners_open_after_drop,
                fail_close_submit,
                panic_after_drop,
            }
        }
    }

    #[cfg(not(miri))]
    impl Future for ReturnPendingNopWithCloseOwners {
        type Output = CompletionDrainCloseOwners;

        fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
            let this = self.get_mut();
            let nop = this.nop.as_mut().expect("completion probe NOP missing");
            assert!(
                Pin::new(nop).poll(cx).is_pending(),
                "completion probe NOP completed before submission"
            );
            Poll::Ready(CompletionDrainCloseOwners {
                nop: ManuallyDrop::new(this.nop.take().expect("completion probe NOP disappeared")),
                runtime_fd: this.runtime_fd.take(),
                retained_listener: this.retained_listener.take(),
                runtime_raw: this.runtime_raw,
                listener_raw: this.listener_raw,
                drops: Rc::clone(&this.drops),
                saw_active_drain: Rc::clone(&this.saw_active_drain),
                owners_open_after_drop: Rc::clone(&this.owners_open_after_drop),
                fail_close_submit: this.fail_close_submit,
                panic_after_drop: this.panic_after_drop,
            })
        }
    }

    pub(super) struct ReentrantOperationPayload {
        pub(super) reactor: *mut Reactor,
        pub(super) completed: *mut CompletionState,
        pub(super) pending: *mut CompletionState,
        pub(super) armed: Rc<Cell<bool>>,
        pub(super) drops: Rc<Cell<usize>>,
        pub(super) saw_active_drain: Rc<Cell<bool>>,
    }

    impl Drop for ReentrantOperationPayload {
        fn drop(&mut self) {
            if !self.armed.get() {
                return;
            }
            self.drops.set(self.drops.get() + 1);
            let drain_active = completion_drain_active();
            self.saw_active_drain.set(drain_active);
            assert!(
                drain_active,
                "operation payload dropped outside completion drain"
            );
            let completed_live = unsafe { reactor_owns_state(self.reactor, self.completed) };
            assert!(
                completed_live,
                "nested completed operation left the live registry before payload destruction"
            );
            let pending_live = unsafe { reactor_owns_state(self.reactor, self.pending) };
            assert!(
                pending_live,
                "pending operation left the live registry before payload destruction"
            );
            unsafe {
                assert!(
                    (*self.completed).is_completed(),
                    "nested completion was not retired before payload destruction"
                );
                assert!(
                    !(*self.pending).is_completed(),
                    "pending operation completed before payload destruction"
                );
                Reactor::free_op_unchecked(self.reactor, self.completed);
                Reactor::defer_cancel_during_completion_drain(self.reactor, self.pending);
            }
        }
    }

    #[cfg(not(miri))]
    struct PendingReadPayload {
        fd: OwnedFd,
        byte: [u8; 1],
        drops: Rc<Cell<usize>>,
    }

    #[cfg(not(miri))]
    impl Drop for PendingReadPayload {
        fn drop(&mut self) {
            self.drops.set(self.drops.get() + 1);
        }
    }

    #[cfg(not(miri))]
    #[derive(Clone, Copy)]
    enum ProbeOperationOwner {
        Detached,
        Pending,
    }

    #[cfg(not(miri))]
    struct TrackedProbeOperation {
        ptr: Cell<*mut CompletionState>,
        submitted: Cell<bool>,
        owner: ProbeOperationOwner,
    }

    #[cfg(not(miri))]
    impl TrackedProbeOperation {
        const fn new(owner: ProbeOperationOwner) -> Self {
            Self {
                ptr: Cell::new(std::ptr::null_mut()),
                submitted: Cell::new(false),
                owner,
            }
        }
    }

    /// Makes every exceptional probe exit safe before the reactor is dropped.
    ///
    /// The probes intentionally retain completed and kernel-visible operation
    /// owners in unusual states. This guard neutralizes their order-sensitive
    /// test destructors, reclaims only operations known not to be
    /// kernel-visible, and delegates submitted retirement to the production
    /// bounded shutdown path.
    #[cfg(not(miri))]
    struct CompletionProbeCleanup {
        reactor: *mut Reactor,
        runtime_state: *mut RuntimeState,
        ready_queue: *mut crate::utils::list::intrusive::dlist::DList<TaskHeader>,
        completed: TrackedProbeOperation,
        pending: TrackedProbeOperation,
        outer: TrackedProbeOperation,
        descriptor_armed: Rc<Cell<bool>>,
        reentrant_armed: Rc<Cell<bool>>,
        armed: bool,
    }

    #[cfg(not(miri))]
    impl CompletionProbeCleanup {
        unsafe fn new(
            reactor: *mut Reactor,
            runtime_state: *mut RuntimeState,
            ready_queue: *mut crate::utils::list::intrusive::dlist::DList<TaskHeader>,
            descriptor_armed: Rc<Cell<bool>>,
            reentrant_armed: Rc<Cell<bool>>,
        ) -> Self {
            Self {
                reactor,
                runtime_state,
                ready_queue,
                completed: TrackedProbeOperation::new(ProbeOperationOwner::Detached),
                pending: TrackedProbeOperation::new(ProbeOperationOwner::Pending),
                outer: TrackedProbeOperation::new(ProbeOperationOwner::Detached),
                descriptor_armed,
                reentrant_armed,
                armed: true,
            }
        }

        fn tracks(&self) -> [&TrackedProbeOperation; 3] {
            [&self.completed, &self.pending, &self.outer]
        }

        unsafe fn state_is_live(&self, ptr: *mut CompletionState) -> bool {
            unsafe { reactor_owns_state(self.reactor, ptr) }
        }

        fn clear_reclaimed(&self) {
            for tracked in self.tracks() {
                let ptr = tracked.ptr.get();
                if !ptr.is_null() && !unsafe { self.state_is_live(ptr) } {
                    tracked.ptr.set(std::ptr::null_mut());
                    tracked.submitted.set(false);
                }
            }
        }

        unsafe fn clean_up(&self) {
            self.descriptor_armed.set(false);
            self.reentrant_armed.set(false);

            let completion_drain = CompletionDrainGuard::enter();
            for tracked in self.tracks() {
                let ptr = tracked.ptr.get();
                if !unsafe { self.state_is_live(ptr) } {
                    continue;
                }

                if !tracked.submitted.get() || unsafe { (*ptr).is_completed() } {
                    unsafe { Reactor::free_op_unchecked(self.reactor, ptr) };
                    continue;
                }

                match tracked.owner {
                    ProbeOperationOwner::Detached => unsafe {
                        (*ptr).set_detached();
                    },
                    ProbeOperationOwner::Pending => unsafe {
                        if !(*ptr).is_orphaned() && !(*ptr).is_cancel_pending() {
                            Reactor::defer_cancel_during_completion_drain(self.reactor, ptr);
                        }
                    },
                }
            }

            let inflight = self
                .tracks()
                .into_iter()
                .filter(|tracked| {
                    let ptr = tracked.ptr.get();
                    tracked.submitted.get()
                        && unsafe { self.state_is_live(ptr) && !(*ptr).is_completed() }
                })
                .count();
            unsafe {
                (*self.runtime_state).inflight_ops = inflight;
            }
            drop(completion_drain);

            unsafe {
                Reactor::shutdown_unchecked(self.reactor, self.runtime_state, self.ready_queue);
            }
        }

        unsafe fn force_storage_abandonment(&self) {
            unsafe {
                (*self.reactor).storage_abandoned = true;
                drop((&mut *std::ptr::addr_of_mut!((*self.reactor).ring)).take());
                (*self.runtime_state).inflight_ops = 0;
            }
        }

        unsafe fn abandon(mut self) {
            unsafe {
                self.force_storage_abandonment();
            }
            self.armed = false;
        }

        fn finish_if_clean(mut self) {
            let clean = unsafe {
                (*self.runtime_state).inflight_ops == 0
                    && (&*std::ptr::addr_of!((*self.reactor).live_registry)).is_empty()
                    && (&*std::ptr::addr_of!((*self.reactor).pending_cancels)).is_empty()
            };
            if clean {
                self.armed = false;
            }
        }
    }

    #[cfg(not(miri))]
    impl Drop for CompletionProbeCleanup {
        fn drop(&mut self) {
            if !self.armed {
                return;
            }

            let already_panicking = std::thread::panicking();
            let cleanup = catch_unwind(AssertUnwindSafe(|| unsafe {
                self.clean_up();
            }));
            if cleanup.is_ok() {
                let reactor_safe = unsafe {
                    (&*std::ptr::addr_of!((*self.reactor).ring)).is_none()
                        && ((*self.reactor).storage_abandoned
                            || (&*std::ptr::addr_of!((*self.reactor).live_registry)).is_empty())
                };
                if reactor_safe {
                    return;
                }
            }

            unsafe {
                self.force_storage_abandonment();
            }
            if let Err(payload) = cleanup {
                if already_panicking {
                    std::mem::forget(payload);
                } else {
                    resume_unwind(payload);
                }
            }
        }
    }

    #[cfg(not(miri))]
    /// Exact observations from the real-operation completion-drain probe.
    #[doc(hidden)]
    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    pub struct CompletionDrainReentrancyReport {
        initial_inflight: usize,
        completed_setup_completions: usize,
        completed_payload_retained: bool,
        first_drain_completions: usize,
        payload_drops: usize,
        payload_saw_active_drain: bool,
        descriptor_rejected_ring: bool,
        descriptor_saw_active_drain: bool,
        descriptor_closed: bool,
        first_drain_inflight: usize,
        first_drain_live_ops: usize,
        first_drain_pending_cancels: usize,
        first_drain_registry_exact: bool,
        first_drain_cancel_links_exact: bool,
        first_drain_pending_payload_drops: usize,
        first_drain_waiter_refs: usize,
        first_drain_ready_queue_empty: bool,
        first_drain_guard_released: bool,
        second_drain_completions: usize,
        final_inflight: usize,
        final_live_ops: usize,
        final_pending_cancels: usize,
        final_cancel_links_clear: bool,
        final_pending_payload_drops: usize,
        final_waiter_refs: usize,
        final_ready_queue_empty: bool,
        final_guard_released: bool,
        all_operation_slots_reused: bool,
    }

    #[cfg(not(miri))]
    impl CompletionDrainReentrancyReport {
        /// Expected result when reentrant reclamation preserves every owner.
        pub const EXPECTED: Self = Self {
            initial_inflight: 0,
            completed_setup_completions: 1,
            completed_payload_retained: true,
            first_drain_completions: 1,
            payload_drops: 1,
            payload_saw_active_drain: true,
            descriptor_rejected_ring: true,
            descriptor_saw_active_drain: true,
            descriptor_closed: true,
            first_drain_inflight: 1,
            first_drain_live_ops: 1,
            first_drain_pending_cancels: 1,
            first_drain_registry_exact: true,
            first_drain_cancel_links_exact: true,
            first_drain_pending_payload_drops: 0,
            first_drain_waiter_refs: 1,
            first_drain_ready_queue_empty: true,
            first_drain_guard_released: true,
            second_drain_completions: 1,
            final_inflight: 0,
            final_live_ops: 0,
            final_pending_cancels: 0,
            final_cancel_links_clear: true,
            final_pending_payload_drops: 1,
            final_waiter_refs: 1,
            final_ready_queue_empty: true,
            final_guard_released: true,
            all_operation_slots_reused: true,
        };
    }

    #[cfg(not(miri))]
    /// Exact observations from the real-CQ descriptor-drop probe.
    #[doc(hidden)]
    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    pub struct CompletionDrainDescriptorReport {
        output_drops: usize,
        destructor_saw_active_drain: bool,
        owners_open_during_destructor: bool,
        runtime_fd_closed: bool,
        retained_listener_closed: bool,
        #[cfg(debug_assertions)]
        close_ring_submissions: usize,
        #[cfg(debug_assertions)]
        close_ring_fallbacks: usize,
        #[cfg(debug_assertions)]
        close_direct_closes: usize,
        #[cfg(debug_assertions)]
        close_worker_admissions: usize,
        #[cfg(debug_assertions)]
        sqe_submits: usize,
        #[cfg(debug_assertions)]
        cqe_completions: usize,
        guard_released: bool,
    }

    #[cfg(not(miri))]
    impl CompletionDrainDescriptorReport {
        /// Expected result when both production descriptor owners defer to the
        /// post-view ring-close drain.
        pub const EXPECTED: Self = Self {
            output_drops: 1,
            destructor_saw_active_drain: true,
            owners_open_during_destructor: true,
            runtime_fd_closed: true,
            retained_listener_closed: true,
            #[cfg(debug_assertions)]
            close_ring_submissions: 2,
            #[cfg(debug_assertions)]
            close_ring_fallbacks: 0,
            #[cfg(debug_assertions)]
            close_direct_closes: 0,
            #[cfg(debug_assertions)]
            close_worker_admissions: 0,
            #[cfg(debug_assertions)]
            sqe_submits: 3,
            #[cfg(debug_assertions)]
            cqe_completions: 3,
            guard_released: true,
        };
    }

    /// Runs completed, pending, and reentrant operations through real CQ views.
    #[cfg(not(miri))]
    #[doc(hidden)]
    pub fn test_completion_drain_reentrancy() -> io::Result<CompletionDrainReentrancyReport> {
        let drops = Rc::new(Cell::new(0));
        let saw_active_drain = Rc::new(Cell::new(false));
        let reentrant_armed = Rc::new(Cell::new(false));
        let descriptor_armed = Rc::new(Cell::new(false));
        let descriptor_rejected = Rc::new(Cell::new(false));
        let descriptor_saw_active_drain = Rc::new(Cell::new(false));
        let pending_payload_drops = Rc::new(Cell::new(0));
        let descriptor_fd = unsafe {
            // SAFETY: ownership is captured immediately after the helper
            // returns its one live descriptor.
            OwnedFd::from_raw_fd(distinctive_closeable_test_fd()?)
        };
        let descriptor_raw = descriptor_fd.as_raw_fd();
        let mut reactor = Box::new(Reactor::new_with_config(ReactorConfig { ring_entries: 8 })?);
        reactor.init();
        let reactor_ptr = std::ptr::from_mut(reactor.as_mut());
        let mut runtime_state = RuntimeState {
            live_tasks: 0,
            inflight_ops: 0,
            #[cfg(debug_assertions)]
            stats: crate::runtime::executor::RuntimeStats::default(),
        };
        let initial_inflight = runtime_state.inflight_ops;
        let mut ready_queue =
            crate::utils::list::intrusive::dlist::DList::<TaskHeader>::new_uninit();
        ready_queue.init();
        let mut pending_waiter = TaskHeader::new();
        let pending_waiter_ptr = std::ptr::addr_of_mut!(pending_waiter);
        let (read_end, mut write_end) = UnixStream::pair()?;
        let cleanup = unsafe {
            CompletionProbeCleanup::new(
                reactor_ptr,
                std::ptr::addr_of_mut!(runtime_state),
                std::ptr::addr_of_mut!(ready_queue),
                Rc::clone(&descriptor_armed),
                Rc::clone(&reentrant_armed),
            )
        };

        let completed = reactor.alloc_op();
        if completed.is_null() {
            return Err(io::Error::from(io::ErrorKind::OutOfMemory));
        }
        cleanup.completed.ptr.set(completed);
        let completed_guard = unsafe { UnsubmittedOpGuard::new(reactor_ptr, completed) };
        let completed_payload = reactor.alloc_retained_payload(CompletionDrainDescriptor {
            fd: Some(descriptor_fd),
            armed: Rc::clone(&descriptor_armed),
            rejected: Rc::clone(&descriptor_rejected),
            saw_active_drain: Rc::clone(&descriptor_saw_active_drain),
        });
        unsafe {
            (*completed).attach_retained_payload(completed_payload);
        }
        reactor.submit_sqe(opcode::Nop::new().build().user_data(completed as u64))?;
        runtime_state.inflight_ops += 1;
        cleanup.completed.submitted.set(true);
        let completed = completed_guard.into_state_ptr();
        descriptor_armed.set(true);
        submit_and_wait_retry_eintr(&mut reactor, 1)?;
        let completed_setup_completions = unsafe {
            Reactor::poll_io_unchecked(reactor_ptr, 1, &mut runtime_state, &mut ready_queue)
        }?;
        let completed_payload_retained = unsafe {
            reactor_owns_state(reactor_ptr, completed)
                && (*completed).is_completed()
                && !descriptor_rejected.get()
                && !raw_fd_is_closed(descriptor_raw)
        };

        let read_fd = OwnedFd::from(read_end);
        let pending = reactor.alloc_op();
        if pending.is_null() {
            return Err(io::Error::from(io::ErrorKind::OutOfMemory));
        }
        cleanup.pending.ptr.set(pending);
        let pending_guard = unsafe { UnsubmittedOpGuard::new(reactor_ptr, pending) };
        let pending_payload = reactor.alloc_retained_payload(PendingReadPayload {
            fd: read_fd,
            byte: [0],
            drops: Rc::clone(&pending_payload_drops),
        });
        let pending_payload_ptr = pending_payload.as_ptr();
        let pending_sqe = unsafe {
            opcode::Read::new(
                types::Fd((*pending_payload_ptr).fd.as_raw_fd()),
                (*pending_payload_ptr).byte.as_mut_ptr(),
                1,
            )
            .build()
            .user_data(pending as u64)
        };
        unsafe {
            (*pending).attach_retained_payload(pending_payload);
            (*pending).register_waiter(pending_waiter_ptr);
        }
        reactor.submit_sqe(pending_sqe)?;
        runtime_state.inflight_ops += 1;
        cleanup.pending.submitted.set(true);
        let pending = pending_guard.into_state_ptr();

        let outer = reactor.alloc_op();
        if outer.is_null() {
            return Err(io::Error::from(io::ErrorKind::OutOfMemory));
        }
        cleanup.outer.ptr.set(outer);
        let outer_guard = unsafe { UnsubmittedOpGuard::new(reactor_ptr, outer) };
        let outer_payload = reactor.alloc_retained_payload(ReentrantOperationPayload {
            reactor: reactor_ptr,
            completed,
            pending,
            armed: Rc::clone(&reentrant_armed),
            drops: Rc::clone(&drops),
            saw_active_drain: Rc::clone(&saw_active_drain),
        });
        unsafe {
            (*outer).attach_retained_payload(outer_payload);
            (*outer).set_detached();
        }
        reactor.submit_sqe(opcode::Nop::new().build().user_data(outer as u64))?;
        runtime_state.inflight_ops += 1;
        cleanup.outer.submitted.set(true);
        let outer = outer_guard.into_state_ptr();
        reentrant_armed.set(true);
        submit_and_wait_retry_eintr(&mut reactor, 1)?;
        let first_drain_completions = unsafe {
            Reactor::poll_io_unchecked(reactor_ptr, 1, &mut runtime_state, &mut ready_queue)
        }?;
        cleanup.clear_reclaimed();

        let first_drain_registry_exact = reactor.live_registry.as_slice() == [pending]
            && unsafe { (*pending).registry_index == 0 };
        let first_drain_cancel_links_exact = first_drain_registry_exact
            && unsafe {
                (*pending).is_orphaned()
                    && (*pending).is_cancel_pending()
                    && (*pending).waiter.is_null()
                    && (*pending).cancel_prev().is_null()
                    && (*pending).cancel_next.is_null()
                    && reactor.pending_cancels.head == pending
                    && reactor.pending_cancels.tail == pending
            };
        let first_drain_inflight = runtime_state.inflight_ops;
        let first_drain_live_ops = reactor.live_registry.len();
        let first_drain_pending_cancels = reactor.pending_cancels.len();
        let first_drain_pending_payload_drops = pending_payload_drops.get();
        let first_drain_waiter_refs = pending_waiter.refs.get();
        let first_drain_ready_queue_empty = ready_queue.is_empty();
        let first_drain_guard_released = !completion_drain_active();

        write_end.write_all(&[0x5a])?;
        submit_and_wait_retry_eintr(&mut reactor, 1)?;
        let second_drain_completions = unsafe {
            Reactor::poll_io_unchecked(reactor_ptr, 1, &mut runtime_state, &mut ready_queue)
        }?;
        let final_inflight = runtime_state.inflight_ops;
        let final_live_ops = reactor.live_registry.len();
        let final_pending_cancels = reactor.pending_cancels.len();
        let final_cancel_links_clear =
            reactor.pending_cancels.head.is_null() && reactor.pending_cancels.tail.is_null();
        let final_pending_payload_drops = pending_payload_drops.get();
        let final_waiter_refs = pending_waiter.refs.get();
        let final_ready_queue_empty = ready_queue.is_empty();
        let final_guard_released = !completion_drain_active();

        cleanup.clear_reclaimed();
        let mut reused = [std::ptr::null_mut(); 3];
        let mut reuse_guards: [Option<UnsubmittedOpGuard>; 3] = [None, None, None];
        let mut reuse_ambiguous = false;
        for index in 0..reused.len() {
            let ptr = reactor.alloc_op();
            reused[index] = ptr;
            if !ptr.is_null() && reused[..index].contains(&ptr) {
                reuse_ambiguous = true;
                break;
            }
            if !ptr.is_null() {
                reuse_guards[index] = Some(unsafe { UnsubmittedOpGuard::new(reactor_ptr, ptr) });
            }
        }
        let [first_reuse, second_reuse, third_reuse] = reused;
        let all_operation_slots_reused = !first_reuse.is_null()
            && !second_reuse.is_null()
            && !third_reuse.is_null()
            && first_reuse != second_reuse
            && first_reuse != third_reuse
            && second_reuse != third_reuse
            && [outer, completed, pending].contains(&first_reuse)
            && [outer, completed, pending].contains(&second_reuse)
            && [outer, completed, pending].contains(&third_reuse);
        if reuse_ambiguous {
            for guard in reuse_guards.into_iter().flatten() {
                let _ = guard.into_state_ptr();
            }
        } else {
            drop(reuse_guards);
        }

        let report = CompletionDrainReentrancyReport {
            initial_inflight,
            completed_setup_completions,
            completed_payload_retained,
            first_drain_completions,
            payload_drops: drops.get(),
            payload_saw_active_drain: saw_active_drain.get(),
            descriptor_rejected_ring: descriptor_rejected.get(),
            descriptor_saw_active_drain: descriptor_saw_active_drain.get(),
            descriptor_closed: raw_fd_is_closed(descriptor_raw),
            first_drain_inflight,
            first_drain_live_ops,
            first_drain_pending_cancels,
            first_drain_registry_exact,
            first_drain_cancel_links_exact,
            first_drain_pending_payload_drops,
            first_drain_waiter_refs,
            first_drain_ready_queue_empty,
            first_drain_guard_released,
            second_drain_completions,
            final_inflight,
            final_live_ops,
            final_pending_cancels,
            final_cancel_links_clear,
            final_pending_payload_drops,
            final_waiter_refs,
            final_ready_queue_empty,
            final_guard_released,
            all_operation_slots_reused,
        };
        if reuse_ambiguous {
            unsafe {
                cleanup.abandon();
            }
        } else {
            cleanup.finish_if_clean();
        }
        Ok(report)
    }

    /// Runs descriptor destruction through the production completion view.
    #[cfg(not(miri))]
    #[doc(hidden)]
    pub fn test_completion_drain_descriptor_close() -> io::Result<CompletionDrainDescriptorReport> {
        let runtime_raw = distinctive_closeable_test_fd()?;
        let listener_raw = distinctive_closeable_test_fd()?;
        let runtime_fd = RuntimeFd::from_fresh_raw_fd(runtime_raw);
        let listener = RuntimeFd::from_fresh_raw_fd(listener_raw);
        let retained_listener = RetainedListenerFd::new(&listener);
        drop(listener);

        let output_drops = Rc::new(Cell::new(0));
        let saw_active_drain = Rc::new(Cell::new(false));
        let owners_open_during_destructor = Rc::new(Cell::new(false));

        let mut executor = Executor::new_with_config(ExecutorConfig {
            reactor: ReactorConfig { ring_entries: 8 },
            ..ExecutorConfig::default()
        })?;
        executor.run({
            let output_drops = Rc::clone(&output_drops);
            let saw_active_drain = Rc::clone(&saw_active_drain);
            let owners_open_during_destructor = Rc::clone(&owners_open_during_destructor);
            async move {
                let handle = Executor::spawn(ReturnPendingNopWithCloseOwners::new(
                    Some(runtime_fd),
                    Some(retained_listener),
                    runtime_raw,
                    listener_raw,
                    output_drops,
                    saw_active_drain,
                    owners_open_during_destructor,
                    false,
                    false,
                ))
                .expect("completion-drain descriptor task did not fit");
                drop(handle);
            }
        })?;

        #[cfg(debug_assertions)]
        let stats = executor.last_stats();

        let report = CompletionDrainDescriptorReport {
            output_drops: output_drops.get(),
            destructor_saw_active_drain: saw_active_drain.get(),
            owners_open_during_destructor: owners_open_during_destructor.get(),
            runtime_fd_closed: raw_fd_is_closed(runtime_raw),
            retained_listener_closed: raw_fd_is_closed(listener_raw),
            #[cfg(debug_assertions)]
            close_ring_submissions: stats.close_ring_submissions,
            #[cfg(debug_assertions)]
            close_ring_fallbacks: stats.close_ring_fallbacks,
            #[cfg(debug_assertions)]
            close_direct_closes: stats.close_direct_closes,
            #[cfg(debug_assertions)]
            close_worker_admissions: stats.close_worker_admissions,
            #[cfg(debug_assertions)]
            sqe_submits: stats.sqe_submits,
            #[cfg(debug_assertions)]
            cqe_completions: stats.cqe_completions,
            guard_released: !completion_drain_active(),
        };
        Ok(report)
    }
}

#[cfg(all(feature = "test-support", not(miri)))]
pub use completion_drain_probe::{
    CompletionDrainDescriptorReport, CompletionDrainReentrancyReport,
    test_completion_drain_descriptor_close, test_completion_drain_reentrancy,
};

#[cfg(feature = "test-support")]
struct CompletionDrainCloseBenchmarkOutput {
    state: *mut CompletionState,
    retained_listener: Option<crate::runtime::fd::RetainedListenerFd>,
}

#[cfg(feature = "test-support")]
impl CompletionDrainCloseBenchmarkOutput {
    fn new(retained_listener: crate::runtime::fd::RetainedListenerFd) -> Self {
        Self {
            state: std::ptr::null_mut(),
            retained_listener: Some(retained_listener),
        }
    }
}

#[cfg(feature = "test-support")]
impl Drop for CompletionDrainCloseBenchmarkOutput {
    fn drop(&mut self) {
        unsafe {
            crate::runtime::executor::drop_op_ptr_unchecked(&mut self.state);
        }
        drop(self.retained_listener.take());
    }
}

/// Measures final listener-owner destruction during completion retirement.
///
/// Repository benchmark setup creates and completes one NOP per listener
/// outside timing. The timed interval contains only production completion
/// retirement, final task-owned listener destruction, and the post-view close
/// submission pass. Close-CQE retirement and validation remain outside timing.
#[cfg(feature = "test-support")]
#[doc(hidden)]
pub fn benchmark_completion_drain_close(
    total_rounds: usize,
    ops_per_round: usize,
    queue_depth: usize,
) -> io::Result<Vec<Duration>> {
    if total_rounds == 0 || ops_per_round == 0 || queue_depth == 0 {
        return Err(io::Error::from(io::ErrorKind::InvalidInput));
    }
    let ring_entries =
        u32::try_from(queue_depth).map_err(|_| io::Error::from(io::ErrorKind::InvalidInput))?;

    crate::runtime::executor::with_executor_context_for_benchmark(
        ring_entries,
        |reactor, runtime_state, ready_queue, all_tasks| {
            let mut raw_fds = Vec::new();
            raw_fds
                .try_reserve_exact(queue_depth)
                .map_err(|_| io::Error::from(io::ErrorKind::OutOfMemory))?;
            let mut durations = Vec::new();
            durations
                .try_reserve_exact(total_rounds)
                .map_err(|_| io::Error::from(io::ErrorKind::OutOfMemory))?;

            for _ in 0..total_rounds {
                let mut remaining = ops_per_round;
                let mut elapsed = Duration::ZERO;

                while remaining > 0 {
                    let batch = remaining.min(queue_depth);
                    raw_fds.clear();

                    for _ in 0..batch {
                        let raw = crate::runtime::fd::distinctive_closeable_test_fd()?;
                        let listener = crate::runtime::fd::RuntimeFd::from_fresh_raw_fd(raw);
                        let retained = crate::runtime::fd::RetainedListenerFd::new(&listener);
                        drop(listener);

                        let staged =
                            crate::runtime::executor::stage_completed_task_output_for_benchmark(
                                CompletionDrainCloseBenchmarkOutput::new(retained),
                            )?;
                        let output = staged.output_ptr();
                        let state = unsafe { (&mut *reactor).alloc_op() };
                        if state.is_null() {
                            return Err(io::Error::from(io::ErrorKind::OutOfMemory));
                        }
                        unsafe {
                            (*output).state = state;
                        }
                        let output = unsafe { staged.transfer_to_waiter(state) };

                        let sqe = opcode::Nop::new().build().user_data(state as u64);
                        if let Err(err) = unsafe { (&mut *reactor).submit_sqe(sqe) } {
                            // Submission never made the state kernel-visible.
                            // Clear the task's back-pointer before operation
                            // reclamation releases its final waiter reference.
                            unsafe {
                                (*output).state = std::ptr::null_mut();
                                Reactor::free_op_unchecked(reactor, state);
                            }
                            return Err(err);
                        }
                        unsafe {
                            (*runtime_state).inflight_ops += 1;
                        }
                        raw_fds.push(raw);
                    }

                    submit_and_wait_retry_eintr(unsafe { &mut *reactor }, batch)?;
                    let started = Instant::now();
                    let completions = unsafe {
                        Reactor::poll_io_unchecked(reactor, batch, runtime_state, ready_queue)
                    }?;
                    elapsed += started.elapsed();
                    if completions != batch {
                        return Err(io::Error::other(
                            "completion-close benchmark drained a short NOP batch",
                        ));
                    }

                    let close_ops = unsafe { (*runtime_state).inflight_ops };
                    if close_ops != 0 && close_ops != batch {
                        return Err(io::Error::other(
                            "completion-close benchmark produced a partial close batch",
                        ));
                    }
                    if unsafe { !(&*ready_queue).is_empty() } {
                        return Err(io::Error::other(
                            "completion-close benchmark unexpectedly queued a task",
                        ));
                    }
                    if unsafe { !(&*all_tasks).is_empty() } {
                        return Err(io::Error::other(
                            "completion-close benchmark retained a staged task",
                        ));
                    }

                    let live_ops =
                        unsafe { (&*std::ptr::addr_of!((*reactor).live_registry)).len() };
                    let pending_closes =
                        unsafe { (&*std::ptr::addr_of!((*reactor).pending_closes)).len() };
                    let queued_sqes = unsafe { (&mut *reactor).ring_mut()?.submission().len() };
                    let any_closed = raw_fds
                        .iter()
                        .any(|&raw| crate::runtime::fd::raw_fd_is_closed(raw));
                    let all_closed = raw_fds
                        .iter()
                        .all(|&raw| crate::runtime::fd::raw_fd_is_closed(raw));
                    if close_ops == batch {
                        if live_ops != batch
                            || pending_closes != batch
                            || queued_sqes != batch
                            || any_closed
                        {
                            return Err(io::Error::other(
                                "completion-close benchmark deferred an inexact close batch",
                            ));
                        }
                    } else if live_ops != 0
                        || pending_closes != 0
                        || queued_sqes != 0
                        || !all_closed
                    {
                        return Err(io::Error::other(
                            "completion-close benchmark completed an inexact direct-close batch",
                        ));
                    }

                    if close_ops > 0 {
                        submit_and_wait_retry_eintr(unsafe { &mut *reactor }, close_ops)?;
                        let close_completions = unsafe {
                            Reactor::poll_io_unchecked(
                                reactor,
                                close_ops,
                                runtime_state,
                                ready_queue,
                            )
                        }?;
                        if close_completions != close_ops {
                            return Err(io::Error::other(
                                "completion-close benchmark drained a short close batch",
                            ));
                        }
                    }

                    if unsafe { (*runtime_state).inflight_ops != 0 }
                        || unsafe { !(&*std::ptr::addr_of!((*reactor).live_registry)).is_empty() }
                        || unsafe { !(&*std::ptr::addr_of!((*reactor).pending_closes)).is_empty() }
                        || unsafe { (&*reactor).has_queued_sqes() }
                    {
                        return Err(io::Error::other(
                            "completion-close benchmark left reactor ownership live",
                        ));
                    }
                    if raw_fds
                        .iter()
                        .any(|&raw| !crate::runtime::fd::raw_fd_is_closed(raw))
                    {
                        return Err(io::Error::other(
                            "completion-close benchmark left a descriptor open",
                        ));
                    }

                    remaining -= batch;
                }

                durations.push(elapsed);
            }

            Ok(durations)
        },
    )
}

/// Measures retirement of targets queued after cancel-submit pressure.
///
/// This repository-only benchmark seam excludes queue construction from the
/// timed interval. Each measured operation retires one completion state in
/// reverse queue order, exercising the pending-cancel unlink path directly.
#[cfg(feature = "test-support")]
#[doc(hidden)]
pub fn benchmark_cancel_submit_pressure(
    total_rounds: usize,
    ops_per_round: usize,
    queue_depth: usize,
) -> io::Result<Vec<Duration>> {
    if total_rounds == 0 || ops_per_round == 0 || queue_depth == 0 {
        return Err(io::Error::from(io::ErrorKind::InvalidInput));
    }

    let ring_entries =
        u32::try_from(queue_depth).map_err(|_| io::Error::from(io::ErrorKind::InvalidInput))?;
    let mut reactor = Reactor::new_with_config(ReactorConfig { ring_entries })?;
    reactor.init();

    let mut queued = Vec::new();
    queued
        .try_reserve_exact(queue_depth)
        .map_err(|_| io::Error::from(io::ErrorKind::OutOfMemory))?;
    let mut durations = Vec::new();
    durations
        .try_reserve_exact(total_rounds)
        .map_err(|_| io::Error::from(io::ErrorKind::OutOfMemory))?;

    for _ in 0..total_rounds {
        let mut remaining = ops_per_round;
        let mut elapsed = Duration::ZERO;

        while remaining > 0 {
            let batch = remaining.min(queue_depth);
            for _ in 0..batch {
                let state = reactor.alloc_op();
                if state.is_null() {
                    return Err(io::Error::from(io::ErrorKind::OutOfMemory));
                }

                // Model the state immediately after an ASYNC_CANCEL submit
                // failure. Release builds intentionally compile fault hooks
                // out of the submission fast path, so this repository-only
                // seam constructs the equivalent queued state directly.
                unsafe {
                    (*state).set_orphaned();
                    CompletionState::clear_waiter_unchecked(state);
                }
                reactor.queue_pending_cancel(state);
                queued.push(state);
            }

            if reactor.pending_cancels.len() != batch {
                return Err(io::Error::other(
                    "cancel-pressure benchmark did not populate the full queue",
                ));
            }

            let started = Instant::now();
            while let Some(state) = queued.pop() {
                // The benchmark constructs an ownerless state with a null
                // waiter and no retained payload, so reclamation cannot invoke
                // callback-capable drop glue.
                unsafe {
                    Reactor::free_op_unchecked(std::ptr::addr_of_mut!(reactor), state);
                }
            }
            elapsed += started.elapsed();

            if !reactor.pending_cancels.is_empty() || !reactor.live_registry.is_empty() {
                return Err(io::Error::other(
                    "cancel-pressure benchmark left reactor state queued",
                ));
            }
            remaining -= batch;
        }

        durations.push(elapsed);
    }

    Ok(durations)
}

#[cfg(test)]
mod pending_cancel_tests {
    #[cfg(not(miri))]
    use super::completion_drain_probe::{
        CompletionDrainClosePanic, CompletionDrainDescriptorReport,
        ReturnPendingNopWithCloseOwners, test_completion_drain_descriptor_close,
    };
    use super::completion_drain_probe::{CompletionDrainDescriptor, ReentrantOperationPayload};
    use super::*;
    use crate::runtime::executor::{
        CloseSubmission, completion_drain_active, poll_ctx_from_waker, try_submit_close,
        with_ringless_poll_context_for_test,
    };
    #[cfg(not(miri))]
    use crate::runtime::executor::{Executor, ExecutorConfig};
    use crate::runtime::fd::{
        RuntimeFd, distinctive_closeable_test_fd, raw_fd_is_closed,
        set_final_core_drop_hook_for_test,
    };
    use crate::runtime::task::{TaskHeader, TaskVTable};
    use std::cell::Cell;
    use std::os::fd::AsRawFd;
    use std::os::fd::FromRawFd;
    #[cfg(not(miri))]
    use std::os::unix::net::UnixStream;
    use std::panic::{AssertUnwindSafe, catch_unwind, resume_unwind};
    use std::rc::Rc;
    use std::task::Poll;

    thread_local! {
        static CANCEL_WAITER_DESTROYS: Cell<usize> = const { Cell::new(0) };
        static SHUTDOWN_REENTRY_REACTOR: Cell<*mut Reactor> =
            const { Cell::new(std::ptr::null_mut()) };
        static SHUTDOWN_REENTRY_STATE: Cell<*mut CompletionState> =
            const { Cell::new(std::ptr::null_mut()) };
        static SHUTDOWN_REENTRY_DESTROYS: Cell<usize> = const { Cell::new(0) };
        static SHUTDOWN_PANIC_REACTOR: Cell<*mut Reactor> =
            const { Cell::new(std::ptr::null_mut()) };
        static FINAL_LEASE_REENTRY_POOL:
            Cell<*mut ProviderOwnedPool<CompletionState, BasicMemoryProvider>> =
                const { Cell::new(std::ptr::null_mut()) };
        static FINAL_LEASE_REENTRY_EXPECTED: Cell<*mut CompletionState> =
            const { Cell::new(std::ptr::null_mut()) };
        static FINAL_LEASE_REENTRY_REUSED: Cell<bool> = const { Cell::new(false) };
        static SHUTDOWN_PANIC_COMPLETED_STATE: Cell<*mut CompletionState> =
            const { Cell::new(std::ptr::null_mut()) };
        static SHUTDOWN_PANIC_APPENDED_STATE: Cell<*mut CompletionState> =
            const { Cell::new(std::ptr::null_mut()) };
        static SHUTDOWN_FIRST_WAITER_DESTROYS: Cell<usize> = const { Cell::new(0) };
        static SHUTDOWN_LATER_WAITER_DESTROYS: Cell<usize> = const { Cell::new(0) };
        static SHUTDOWN_LATER_PANIC_PAYLOAD_DROPS: Cell<usize> = const { Cell::new(0) };
        static SHUTDOWN_RETIRED_PANIC_PAYLOAD_DROPS: Cell<usize> = const { Cell::new(0) };
    }

    fn probe_final_lease_reentry_after_slot_return(_: i32) {
        let pool = FINAL_LEASE_REENTRY_POOL.with(Cell::get);
        let expected = FINAL_LEASE_REENTRY_EXPECTED.with(Cell::get);
        if pool.is_null() || expected.is_null() {
            return;
        }
        let replacement = unsafe { (*pool).alloc(()) };
        let reused = replacement == Some(expected);
        FINAL_LEASE_REENTRY_REUSED.with(|observed| observed.set(reused));
        if let Some(replacement) = replacement {
            unsafe { (*pool).free(replacement) };
        }
    }

    unsafe fn panic_cancel_waiter_destroy(_: *mut TaskHeader) {
        CANCEL_WAITER_DESTROYS.with(|count| count.set(count.get() + 1));
        panic!("cancel waiter destroy panic");
    }

    static PANIC_CANCEL_WAITER_VTABLE: TaskVTable = TaskVTable {
        poll: |_| Poll::Ready(()),
        finish: |_| {},
        cancel: |_| {},
        destroy: panic_cancel_waiter_destroy,
    };

    unsafe fn shutdown_reentry_waiter_destroy(_: *mut TaskHeader) {
        SHUTDOWN_REENTRY_DESTROYS.with(|count| count.set(count.get() + 1));
        let reactor = SHUTDOWN_REENTRY_REACTOR.with(Cell::get);
        let state = SHUTDOWN_REENTRY_STATE.with(Cell::get);
        assert!(!reactor.is_null(), "shutdown re-entry reactor is missing");
        assert!(!state.is_null(), "shutdown re-entry state is missing");
        unsafe {
            Reactor::free_op_unchecked(reactor, state);
        }
    }

    static SHUTDOWN_REENTRY_WAITER_VTABLE: TaskVTable = TaskVTable {
        poll: |_| Poll::Ready(()),
        finish: |_| {},
        cancel: |_| {},
        destroy: shutdown_reentry_waiter_destroy,
    };

    #[derive(Debug)]
    struct FirstShutdownWaiterPanic;

    #[derive(Debug)]
    struct LaterShutdownWaiterPanic;

    impl Drop for LaterShutdownWaiterPanic {
        fn drop(&mut self) {
            SHUTDOWN_LATER_PANIC_PAYLOAD_DROPS.with(|drops| drops.set(drops.get() + 1));
        }
    }

    unsafe fn first_shutdown_waiter_destroy(_: *mut TaskHeader) {
        SHUTDOWN_FIRST_WAITER_DESTROYS.with(|count| count.set(count.get() + 1));
        let reactor = SHUTDOWN_PANIC_REACTOR.with(Cell::get);
        let completed = SHUTDOWN_PANIC_COMPLETED_STATE.with(Cell::get);
        if !completed.is_null() {
            assert!(
                !reactor.is_null(),
                "shutdown panic re-entry reactor is missing"
            );
            unsafe {
                Reactor::free_op_unchecked(reactor, completed);
                let appended = (&mut *reactor).alloc_op();
                assert!(
                    !appended.is_null(),
                    "shutdown panic re-entry operation allocation failed"
                );
                (*appended).set_detached();
                SHUTDOWN_PANIC_APPENDED_STATE.with(|stored| stored.set(appended));
            }
        }
        std::panic::panic_any(FirstShutdownWaiterPanic);
    }

    unsafe fn later_shutdown_waiter_destroy(_: *mut TaskHeader) {
        SHUTDOWN_LATER_WAITER_DESTROYS.with(|count| count.set(count.get() + 1));
        std::panic::panic_any(LaterShutdownWaiterPanic);
    }

    static FIRST_SHUTDOWN_WAITER_VTABLE: TaskVTable = TaskVTable {
        poll: |_| Poll::Ready(()),
        finish: |_| {},
        cancel: |_| {},
        destroy: first_shutdown_waiter_destroy,
    };

    static LATER_SHUTDOWN_WAITER_VTABLE: TaskVTable = TaskVTable {
        poll: |_| Poll::Ready(()),
        finish: |_| {},
        cancel: |_| {},
        destroy: later_shutdown_waiter_destroy,
    };

    struct ShutdownRetainedPayload {
        drops: Rc<Cell<usize>>,
        _bytes: [u8; 8],
    }

    impl Drop for ShutdownRetainedPayload {
        fn drop(&mut self) {
            self.drops.set(self.drops.get() + 1);
        }
    }

    #[cfg(not(miri))]
    #[derive(Debug)]
    struct ShutdownRetainedPayloadPanic;

    #[cfg(not(miri))]
    impl Drop for ShutdownRetainedPayloadPanic {
        fn drop(&mut self) {
            SHUTDOWN_RETIRED_PANIC_PAYLOAD_DROPS.with(|drops| drops.set(drops.get() + 1));
        }
    }

    #[cfg(not(miri))]
    struct ShutdownRetainedPayloadDropBomb {
        drops: Rc<Cell<usize>>,
    }

    #[cfg(not(miri))]
    impl Drop for ShutdownRetainedPayloadDropBomb {
        fn drop(&mut self) {
            self.drops.set(self.drops.get() + 1);
            std::panic::panic_any(ShutdownRetainedPayloadPanic);
        }
    }

    #[derive(Debug)]
    struct OpPayloadDropPanic(&'static str);

    struct OpPayloadDropBomb {
        drops: Rc<Cell<usize>>,
        panic_tag: Option<&'static str>,
    }

    impl Drop for OpPayloadDropBomb {
        fn drop(&mut self) {
            self.drops.set(self.drops.get() + 1);
            if let Some(tag) = self.panic_tag {
                std::panic::panic_any(OpPayloadDropPanic(tag));
            }
        }
    }

    #[repr(align(128))]
    struct HeapOpPayloadDropBomb {
        _bomb: OpPayloadDropBomb,
    }

    fn ringless_reactor() -> Reactor {
        Reactor::new_ringless_for_test(8).expect("ringless reactor construction failed")
    }

    #[test]
    fn ringless_nop_submission_returns_broken_pipe_without_sequence_change() {
        let mut reactor = ringless_reactor();
        let ringless = reactor
            .submit_sqe(opcode::Nop::new().build().user_data(17))
            .expect_err("ringless submission unexpectedly succeeded");
        assert_eq!(ringless.kind(), io::ErrorKind::BrokenPipe);
        assert_eq!(reactor.queued_head, 0);
        assert_eq!(reactor.next_sequence, 0);
        assert!(reactor.pending_closes.is_empty());
    }

    #[cfg(any(debug_assertions, feature = "test-support"))]
    #[test]
    fn ringless_nop_submission_preserves_test_hook_precedence() {
        let mut reactor = ringless_reactor();
        crate::runtime::test_hooks::fail_next_raw_sqe_submit();

        let injected = reactor
            .submit_sqe(opcode::Nop::new().build().user_data(17))
            .expect_err("injected submission failure lost precedence");

        assert_eq!(injected.kind(), io::ErrorKind::WouldBlock);
        assert_eq!(reactor.queued_head, 0);
        assert_eq!(reactor.next_sequence, 0);
    }

    #[test]
    fn deferred_close_queue_is_preallocated_bounded_and_fifo() {
        let mut reactor =
            Reactor::new_ringless_for_test(2).expect("ringless reactor construction failed");
        let reactor_ptr = std::ptr::addr_of_mut!(reactor);
        let initial_capacity = reactor.deferred_closes.capacity();
        assert!(initial_capacity >= 2);
        assert_eq!(reactor.max_deferred_closes, 2);

        let mut raw = [-1; 3];
        let mut owners = Vec::new();
        for slot in &mut raw {
            *slot = distinctive_closeable_test_fd().expect("distinctive descriptor failed");
            // SAFETY: the helper returned one live descriptor whose sole owner
            // moves into this vector.
            owners.push(unsafe { OwnedFd::from_raw_fd(*slot) });
        }

        let drain = CompletionDrainGuard::enter_for_reactor(reactor_ptr);
        for owner in owners.drain(..2) {
            unsafe {
                Reactor::defer_close_during_completion_drain(reactor_ptr, owner)
                    .expect("in-bound descriptor was rejected");
            }
        }
        let rejected = unsafe {
            Reactor::defer_close_during_completion_drain(
                reactor_ptr,
                owners.pop().expect("overflow owner missing"),
            )
        }
        .expect_err("capacity-plus-one descriptor was admitted");
        assert_eq!(rejected.as_raw_fd(), raw[2]);
        assert!(!raw_fd_is_closed(raw[2]));
        assert_eq!(reactor.deferred_closes.capacity(), initial_capacity);
        assert_eq!(reactor.deferred_closes.len(), 2);

        for expected in &raw[..2] {
            let owner = reactor
                .deferred_closes
                .pop_front()
                .expect("deferred owner missing");
            assert_eq!(owner.as_raw_fd(), *expected);
            drop(owner);
            assert!(raw_fd_is_closed(*expected));
        }
        assert!(reactor.deferred_closes.is_empty());
        drop(rejected);
        assert!(raw_fd_is_closed(raw[2]));
        drop(drain);
    }

    #[test]
    fn nested_completion_drains_restore_the_exact_deferred_close_target() {
        with_ringless_poll_context_for_test(4, |owner, cx| {
            let outer = owner.reactor_ptr();
            let outer_runtime_state = poll_ctx_from_waker(cx)
                .expect("outer poll context was rejected")
                .runtime_state();
            let mut inner =
                Reactor::new_ringless_for_test(4).expect("inner ringless reactor failed");
            let inner = std::ptr::addr_of_mut!(inner);
            let mut inner_runtime_state = runtime_state_with_shutdown_inflight(0);
            let raw = [
                distinctive_closeable_test_fd().expect("outer descriptor creation failed"),
                distinctive_closeable_test_fd().expect("inner descriptor creation failed"),
                distinctive_closeable_test_fd().expect("restored descriptor creation failed"),
            ];
            let owners = raw.map(|fd| {
                // SAFETY: each helper result is one distinct live descriptor
                // whose sole ownership transfers into this array.
                unsafe { OwnedFd::from_raw_fd(fd) }
            });
            let [outer_owner, inner_owner, restored_owner] = owners;

            let outer_drain = unsafe { DeferredCloseDrainGuard::new(outer, outer_runtime_state) };
            let outer_guard = CompletionDrainGuard::enter_for_reactor(outer);
            assert!(matches!(
                try_submit_close(outer_owner),
                CloseSubmission::Deferred
            ));
            let inner_drain = unsafe {
                DeferredCloseDrainGuard::new(inner, std::ptr::addr_of_mut!(inner_runtime_state))
            };
            let inner_guard = CompletionDrainGuard::enter_for_reactor(inner);
            assert!(matches!(
                try_submit_close(inner_owner),
                CloseSubmission::Deferred
            ));
            drop(inner_guard);
            drop(inner_drain);
            assert!(raw_fd_is_closed(raw[1]));
            assert!(!raw_fd_is_closed(raw[0]));
            assert!(completion_drain_active());
            assert!(matches!(
                try_submit_close(restored_owner),
                CloseSubmission::Deferred
            ));

            let outer_queue = unsafe { &mut *std::ptr::addr_of_mut!((*outer).deferred_closes) };
            assert_eq!(
                outer_queue
                    .iter()
                    .map(AsRawFd::as_raw_fd)
                    .collect::<Vec<_>>(),
                [raw[0], raw[2]]
            );
            drop(outer_guard);
            drop(outer_drain);
            assert!(!completion_drain_active());
            for fd in raw {
                assert!(raw_fd_is_closed(fd));
            }
        });
    }

    #[test]
    fn shutdown_cleanup_releases_deferred_close_owners() {
        let mut reactor =
            Reactor::new_ringless_for_test(1).expect("ringless reactor construction failed");
        let reactor_ptr = std::ptr::addr_of_mut!(reactor);
        let raw = distinctive_closeable_test_fd().expect("descriptor creation failed");
        // SAFETY: the helper returned one live descriptor whose sole ownership
        // transfers into the deferred queue.
        let owner = unsafe { OwnedFd::from_raw_fd(raw) };

        let drain = CompletionDrainGuard::enter_for_reactor(reactor_ptr);
        unsafe {
            Reactor::defer_close_during_completion_drain(reactor_ptr, owner)
                .expect("deferred owner was rejected");
        }
        drop(drain);
        assert!(!raw_fd_is_closed(raw));
        reactor.drop_unsubmitted_close_owners();
        assert!(reactor.deferred_closes.is_empty());
        assert!(raw_fd_is_closed(raw));
    }

    #[cfg(not(miri))]
    #[test]
    fn deferred_close_submission_failure_drains_during_output_unwind() {
        let raw = distinctive_closeable_test_fd().expect("descriptor creation failed");
        let output_drops = Rc::new(Cell::new(0));
        let saw_active_drain = Rc::new(Cell::new(false));
        let owner_open_during_destructor = Rc::new(Cell::new(false));
        let mut executor = Executor::new_with_config(ExecutorConfig {
            reactor: ReactorConfig { ring_entries: 4 },
            ..ExecutorConfig::default()
        })
        .expect("executor construction failed");

        let unwind = catch_unwind(AssertUnwindSafe(|| {
            executor
                .run({
                    let output_drops = Rc::clone(&output_drops);
                    let saw_active_drain = Rc::clone(&saw_active_drain);
                    let owner_open_during_destructor = Rc::clone(&owner_open_during_destructor);
                    async move {
                        let handle = Executor::spawn(ReturnPendingNopWithCloseOwners::new(
                            Some(RuntimeFd::from_fresh_raw_fd(raw)),
                            None,
                            raw,
                            -1,
                            output_drops,
                            saw_active_drain,
                            owner_open_during_destructor,
                            true,
                            true,
                        ))
                        .expect("completion-drain unwind task did not fit");
                        drop(handle);
                    }
                })
                .expect("executor returned an error before the output panic");
        }))
        .expect_err("task output destructor did not unwind");

        assert!(unwind.is::<CompletionDrainClosePanic>());
        assert_eq!(output_drops.get(), 1);
        assert!(saw_active_drain.get());
        assert!(owner_open_during_destructor.get());
        assert_eq!(
            crate::runtime::test_hooks::raw_sqe_submit_failures_remaining(),
            0,
            "post-view close submission did not consume the injected failure"
        );
        assert!(
            raw_fd_is_closed(raw),
            "failed post-view close submission did not directly close its owner"
        );
        assert!(!completion_drain_active());

        executor
            .run(async {})
            .expect("executor was not reusable after output unwind cleanup");
    }

    fn runtime_state_with_shutdown_inflight(inflight_ops: usize) -> RuntimeState {
        RuntimeState {
            live_tasks: 0,
            inflight_ops,
            #[cfg(debug_assertions)]
            stats: crate::runtime::executor::RuntimeStats::default(),
        }
    }

    fn reset_shutdown_panic_test_state() {
        SHUTDOWN_PANIC_REACTOR.with(|stored| stored.set(std::ptr::null_mut()));
        SHUTDOWN_PANIC_COMPLETED_STATE.with(|stored| stored.set(std::ptr::null_mut()));
        SHUTDOWN_PANIC_APPENDED_STATE.with(|stored| stored.set(std::ptr::null_mut()));
        SHUTDOWN_FIRST_WAITER_DESTROYS.with(|count| count.set(0));
        SHUTDOWN_LATER_WAITER_DESTROYS.with(|count| count.set(0));
        SHUTDOWN_LATER_PANIC_PAYLOAD_DROPS.with(|count| count.set(0));
        SHUTDOWN_RETIRED_PANIC_PAYLOAD_DROPS.with(|count| count.set(0));
    }

    fn queue_states(
        count: usize,
        runtime_shutdown: bool,
    ) -> (Vec<CompletionState>, Vec<*mut CompletionState>) {
        let mut states: Vec<_> = (0..count).map(|_| CompletionState::empty()).collect();
        for state in &mut states {
            if runtime_shutdown {
                state.set_runtime_shutdown();
            } else {
                state.set_orphaned();
            }
        }
        let pointers = states
            .iter_mut()
            .map(|state| state as *mut CompletionState)
            .collect();
        (states, pointers)
    }

    unsafe fn assert_queue_links(queue: &PendingCancelQueue, expected: &[*mut CompletionState]) {
        assert_eq!(queue.len(), expected.len());
        assert_eq!(queue.head, expected.first().copied().unwrap_or_default());
        assert_eq!(queue.tail, expected.last().copied().unwrap_or_default());

        let mut previous = std::ptr::null_mut();
        let mut current = queue.head;
        for &expected_ptr in expected {
            assert_eq!(current, expected_ptr);
            assert!(unsafe { (*current).is_cancel_pending() });
            assert!(unsafe { (*current).is_orphaned() || (*current).is_runtime_shutdown() });
            assert!(unsafe { !(*current).is_completed() });
            assert!(unsafe { !(*current).is_build_aborted() });
            assert!(unsafe { !(*current).is_ring_abandoned() });
            assert_eq!(unsafe { (*current).cancel_prev() }, previous);
            previous = current;
            current = unsafe { (*current).cancel_next };
        }
        assert!(current.is_null());
    }

    #[test]
    fn pending_cancel_queue_unlinks_head_middle_and_tail_in_constant_link_updates() {
        let (_states, pointers) = queue_states(4, false);
        let mut queue = PendingCancelQueue::new();
        for &ptr in &pointers {
            unsafe { queue.push_back(ptr) };
        }
        unsafe { assert_queue_links(&queue, &pointers) };

        assert!(unsafe { queue.unlink(pointers[1]) });
        unsafe { assert_queue_links(&queue, &[pointers[0], pointers[2], pointers[3]]) };

        assert!(unsafe { queue.unlink(pointers[0]) });
        unsafe { assert_queue_links(&queue, &[pointers[2], pointers[3]]) };

        assert!(unsafe { queue.unlink(pointers[3]) });
        unsafe { assert_queue_links(&queue, &[pointers[2]]) };

        assert!(unsafe { queue.unlink(pointers[2]) });
        unsafe { assert_queue_links(&queue, &[]) };

        for &ptr in &pointers {
            unsafe {
                assert!(!(*ptr).is_cancel_pending());
                assert!((*ptr).waiter.is_null());
                assert!((*ptr).cancel_next.is_null());
            }
        }
    }

    #[test]
    fn pending_cancel_retry_snapshot_attempts_each_entry_once_in_fifo_order() {
        let (_states, pointers) = queue_states(3, false);
        let mut queue = PendingCancelQueue::new();
        for &ptr in &pointers {
            unsafe { queue.push_back(ptr) };
        }

        let retry_budget = queue.len();
        let mut attempted = Vec::new();
        for _ in 0..retry_budget {
            let ptr = unsafe { queue.pop_front() }.expect("retry queue ended early");
            attempted.push(ptr);
            unsafe { queue.push_back(ptr) };
        }

        assert_eq!(attempted, pointers);
        unsafe { assert_queue_links(&queue, &pointers) };
        while unsafe { queue.pop_front() }.is_some() {}
    }

    #[test]
    fn pending_cancel_prev_reuses_waiter_only_after_reference_release() {
        let task = TaskHeader::new();
        let task_ptr = &task as *const TaskHeader as *mut TaskHeader;
        let mut states: Vec<_> = (0..2).map(|_| CompletionState::empty()).collect();

        unsafe { states[1].register_waiter(task_ptr) };
        assert_eq!(task.refs.get(), 2);
        for state in &mut states {
            state.set_orphaned();
        }
        let pointers: Vec<_> = states
            .iter_mut()
            .map(|state| state as *mut CompletionState)
            .collect();
        let second = pointers[1];
        unsafe { CompletionState::clear_waiter_unchecked(second) };
        unsafe { (*second).debug_assert_valid_flags() };
        assert_eq!(task.refs.get(), 1);

        let mut queue = PendingCancelQueue::new();
        unsafe {
            queue.push_back(pointers[0]);
            queue.push_back(pointers[1]);
            assert_eq!((*pointers[1]).cancel_prev(), pointers[0]);
            queue.unlink(pointers[1]);
            queue.unlink(pointers[0]);
        }

        assert_eq!(task.refs.get(), 1);
        assert!(states[1].waiter.is_null());
    }

    #[test]
    fn pending_cancel_shutdown_drain_clears_every_link_and_count() {
        let (_states, pointers) = queue_states(4, true);
        let mut queue = PendingCancelQueue::new();
        for &ptr in &pointers {
            unsafe { queue.push_back(ptr) };
        }

        let mut drained = Vec::new();
        while let Some(ptr) = unsafe { queue.pop_front() } {
            drained.push(ptr);
        }

        assert_eq!(drained, pointers);
        unsafe { assert_queue_links(&queue, &[]) };
        for ptr in drained {
            unsafe {
                assert!(!(*ptr).is_cancel_pending());
                assert!((*ptr).waiter.is_null());
                assert!((*ptr).cancel_next.is_null());
            }
        }
    }

    #[test]
    fn queued_shutdown_head_middle_tail_reclaim_preserves_exact_accounting() {
        let mut reactor = ringless_reactor();
        let reactor_ptr = std::ptr::addr_of_mut!(reactor);
        let head = reactor.alloc_op();
        let middle = reactor.alloc_op();
        let tail = reactor.alloc_op();
        assert!(!head.is_null() && !middle.is_null() && !tail.is_null());

        let payload_drops = Rc::new(Cell::new(0));
        for &state in &[head, middle, tail] {
            let payload = reactor.alloc_retained_payload(ShutdownRetainedPayload {
                drops: Rc::clone(&payload_drops),
                _bytes: [0; 8],
            });
            unsafe {
                (*state).attach_retained_payload(payload);
                (*state).set_runtime_shutdown();
            }
        }
        unsafe {
            (*middle).set_orphaned();
            (*tail).set_detached();
        }
        for &state in &[head, middle, tail] {
            reactor.queue_pending_cancel(state);
        }
        unsafe {
            assert_queue_links(&reactor.pending_cancels, &[head, middle, tail]);
        }
        assert_eq!(reactor.live_registry, vec![head, middle, tail]);

        let mut runtime_state = runtime_state_with_shutdown_inflight(3);
        let mut ready_queue =
            crate::utils::list::intrusive::dlist::DList::<TaskHeader>::new_uninit();
        ready_queue.init();
        let owner = reactor.owner;

        unsafe {
            Reactor::retire_completion_unchecked(
                reactor_ptr,
                owner,
                middle,
                -libc::EIO,
                &mut runtime_state,
                &mut ready_queue,
            )
            .expect("queued shutdown middle retirement failed");
            assert_queue_links(&reactor.pending_cancels, &[head, tail]);
            assert_eq!((*head).registry_index, 0);
            assert_eq!((*tail).registry_index, 1);
        }
        assert_eq!(runtime_state.inflight_ops, 2);
        assert_eq!(reactor.live_registry, vec![head, tail]);
        assert_eq!(payload_drops.get(), 1);

        unsafe {
            Reactor::retire_completion_unchecked(
                reactor_ptr,
                owner,
                head,
                -libc::EIO,
                &mut runtime_state,
                &mut ready_queue,
            )
            .expect("queued shutdown head retirement failed");
            assert_queue_links(&reactor.pending_cancels, &[tail]);
            assert!((*head).is_completed());
            assert_eq!((*head).result, -libc::ECANCELED);
            assert_eq!((*head).registry_index, 0);
            assert_eq!((*tail).registry_index, 1);
        }
        assert_eq!(runtime_state.inflight_ops, 1);
        assert_eq!(reactor.live_registry, vec![head, tail]);
        assert_eq!(payload_drops.get(), 1);

        unsafe {
            (*tail).result = -libc::ECANCELED;
            (*tail).set_completed();
        }
        let mut first_panic = None;
        unsafe {
            Reactor::abandon_shutdown_storage_unchecked(
                reactor_ptr,
                std::ptr::addr_of_mut!(runtime_state),
                &mut first_panic,
            );
            assert_queue_links(&reactor.pending_cancels, &[]);
            assert_eq!((*head).registry_index, 0);
        }
        assert!(first_panic.is_none());
        assert_eq!(runtime_state.inflight_ops, 0);
        assert_eq!(reactor.live_registry, vec![head]);
        assert_eq!(payload_drops.get(), 2);
        assert!(ready_queue.is_empty());

        // This ringless model has no kernel-visible storage. Re-enable normal
        // pool destruction after proving the abandonment reclaim shape.
        reactor.storage_abandoned = false;
        reactor.free_op(head);
        assert!(reactor.live_registry.is_empty());
        assert_eq!(payload_drops.get(), 3);

        let reused = [reactor.alloc_op(), reactor.alloc_op(), reactor.alloc_op()];
        assert!(reused.iter().all(|state| !state.is_null()));
        let mut expected = [head as usize, middle as usize, tail as usize];
        let mut actual = reused.map(|state| state as usize);
        expected.sort_unstable();
        actual.sort_unstable();
        assert_eq!(actual, expected, "shutdown reclaim stranded an op slot");
        for state in reused.into_iter().rev() {
            reactor.free_op(state);
        }
        assert!(reactor.live_registry.is_empty());
    }

    #[test]
    fn completion_state_remains_one_cache_line() {
        assert_eq!(std::mem::size_of::<CompletionState>(), 64);
        assert_eq!(std::mem::align_of::<CompletionState>(), 64);
    }

    #[test]
    fn completion_drain_rejects_descriptor_submission_from_payload_destructor() {
        let raw = distinctive_closeable_test_fd().expect("descriptor creation failed");
        let armed = Rc::new(Cell::new(true));
        let rejected = Rc::new(Cell::new(false));
        let saw_active_drain = Rc::new(Cell::new(false));
        let mut pending_cancels = PendingCancelQueue::new();
        let mut retained_pool =
            RetainedPayloadPool::new().expect("retained payload pool construction failed");
        let mut op_pool: ProviderOwnedPool<CompletionState, BasicMemoryProvider> =
            ProviderOwnedPool::new(BasicMemoryProvider::new(), OP_POOL_OBJS_PER_SLAB)
                .expect("operation pool construction failed");
        op_pool.init();
        let mut live_registry = Vec::new();

        let state = unsafe { op_pool.alloc(()) }.expect("operation allocation failed");
        unsafe { (*state).bind_owner(None, 0) };
        live_registry.push(state);
        // SAFETY: the test helper returned one live descriptor whose sole
        // ownership transfers into this payload.
        let fd = unsafe { OwnedFd::from_raw_fd(raw) };
        let payload = retained_pool.alloc(CompletionDrainDescriptor {
            fd: Some(fd),
            armed,
            rejected: Rc::clone(&rejected),
            saw_active_drain: Rc::clone(&saw_active_drain),
        });
        unsafe {
            (*state).attach_retained_payload(payload);
        }

        let drain = CompletionDrainGuard::enter();
        assert!(completion_drain_active());
        unsafe {
            free_op_fields(
                &mut pending_cancels,
                &mut retained_pool,
                &mut op_pool,
                &mut live_registry,
                state,
            )
            .expect("completion-drain operation retirement failed");
        }
        assert!(
            rejected.get(),
            "payload descriptor did not take the no-ring close path"
        );
        assert!(
            saw_active_drain.get(),
            "payload descriptor destructor did not observe the completion drain"
        );
        assert!(
            raw_fd_is_closed(raw),
            "payload descriptor remained open after direct fallback"
        );
        assert!(
            completion_drain_active(),
            "completion drain ended before the view lifetime"
        );
        drop(drain);
        assert!(!completion_drain_active());
        assert!(live_registry.is_empty());
    }

    #[test]
    fn completion_drain_allows_nested_operation_free_and_defers_cancel() {
        let drops = Rc::new(Cell::new(0));
        let saw_active_drain = Rc::new(Cell::new(false));
        let armed = Rc::new(Cell::new(true));
        let mut reactor = ringless_reactor();
        let reactor_ptr = std::ptr::addr_of_mut!(reactor);
        let outer = reactor.alloc_op();
        let completed = reactor.alloc_op();
        let pending = reactor.alloc_op();
        assert!(!outer.is_null() && !completed.is_null() && !pending.is_null());
        unsafe {
            (*completed).set_completed();
        }
        let payload = reactor.alloc_retained_payload(ReentrantOperationPayload {
            reactor: reactor_ptr,
            completed,
            pending,
            armed,
            drops: Rc::clone(&drops),
            saw_active_drain: Rc::clone(&saw_active_drain),
        });
        unsafe {
            (*outer).attach_retained_payload(payload);
            (*outer).set_detached();
        }

        let mut runtime_state = runtime_state_with_shutdown_inflight(1);
        let mut ready_queue =
            crate::utils::list::intrusive::dlist::DList::<TaskHeader>::new_uninit();
        ready_queue.init();
        let owner = reactor.owner;
        // Keep a mutable ring-field borrow live across retirement. Miri then
        // proves completion handling touches only disjoint bookkeeping fields.
        let ring = unsafe { &mut *std::ptr::addr_of_mut!((*reactor_ptr).ring) };
        let drain = CompletionDrainGuard::enter();
        unsafe {
            Reactor::retire_completion_unchecked(
                reactor_ptr,
                owner,
                outer,
                0,
                &mut runtime_state,
                &mut ready_queue,
            )
            .expect("ringless completion retirement failed");
        }
        assert!(
            ring.is_none(),
            "ringless probe unexpectedly acquired a ring"
        );
        assert_eq!(runtime_state.inflight_ops, 0);
        assert_eq!(drops.get(), 1);
        assert!(saw_active_drain.get());
        assert_eq!(reactor.live_registry, vec![pending]);
        assert_eq!(reactor.pending_cancels.len(), 1);
        assert_eq!(reactor.pending_cancels.head, pending);
        assert_eq!(reactor.pending_cancels.tail, pending);
        unsafe {
            assert!((*pending).is_orphaned());
            assert!((*pending).waiter.is_null());
            (*pending).set_completed();
            Reactor::free_op_unchecked(reactor_ptr, pending);
        }
        assert!(reactor.live_registry.is_empty());
        assert!(reactor.pending_cancels.is_empty());
        assert_eq!(reactor.pending_cancels.len(), 0);
        assert!(reactor.pending_cancels.head.is_null());
        assert!(reactor.pending_cancels.tail.is_null());
        drop(drain);
        assert!(!completion_drain_active());

        let first_reuse = reactor.alloc_op();
        let second_reuse = reactor.alloc_op();
        let third_reuse = reactor.alloc_op();
        assert_ne!(first_reuse, second_reuse);
        assert_ne!(first_reuse, third_reuse);
        assert_ne!(second_reuse, third_reuse);
        assert!(
            [outer, completed, pending].contains(&first_reuse)
                && [outer, completed, pending].contains(&second_reuse)
                && [outer, completed, pending].contains(&third_reuse),
            "reentrant retirement stranded an operation slot"
        );
        reactor.free_op(third_reuse);
        reactor.free_op(second_reuse);
        reactor.free_op(first_reuse);
    }

    #[test]
    fn completion_drain_queues_cancel_when_waiter_destructor_panics() {
        CANCEL_WAITER_DESTROYS.with(|count| count.set(0));
        let mut reactor = ringless_reactor();
        let reactor_ptr = std::ptr::addr_of_mut!(reactor);
        let pending = reactor.alloc_op();
        assert!(!pending.is_null(), "operation allocation failed");
        let mut waiter = TaskHeader::new();
        waiter.vtable = &PANIC_CANCEL_WAITER_VTABLE;
        let waiter_ptr = std::ptr::addr_of_mut!(waiter);
        unsafe {
            (*pending).register_waiter(waiter_ptr);
            release_task(waiter_ptr);
        }
        assert_eq!(waiter.refs.get(), 1);

        let drain = CompletionDrainGuard::enter();
        let unwind = catch_unwind(AssertUnwindSafe(|| unsafe {
            Reactor::defer_cancel_during_completion_drain(reactor_ptr, pending);
        }));
        assert!(unwind.is_err(), "waiter destructor did not unwind");
        CANCEL_WAITER_DESTROYS.with(|count| assert_eq!(count.get(), 1));
        unsafe {
            assert!((*pending).is_orphaned());
            assert!((*pending).waiter.is_null());
            assert_queue_links(&reactor.pending_cancels, &[pending]);
            (*pending).set_completed();
            Reactor::free_op_unchecked(reactor_ptr, pending);
        }
        assert!(reactor.live_registry.is_empty());
        assert!(reactor.pending_cancels.is_empty());
        assert_eq!(reactor.pending_cancels.len(), 0);
        assert!(reactor.pending_cancels.head.is_null());
        assert!(reactor.pending_cancels.tail.is_null());
        drop(drain);
        assert!(!completion_drain_active());
    }

    #[test]
    fn shutdown_waiter_destructor_can_free_current_completed_state() {
        SHUTDOWN_REENTRY_DESTROYS.with(|count| count.set(0));
        let mut reactor = ringless_reactor();
        let reactor_ptr = std::ptr::addr_of_mut!(reactor);
        let current = reactor.alloc_op();
        let moved_tail = reactor.alloc_op();
        assert!(!current.is_null() && !moved_tail.is_null());
        unsafe {
            (*current).set_completed();
            (*moved_tail).set_completed();
        }

        let mut waiter = TaskHeader::new();
        waiter.vtable = &SHUTDOWN_REENTRY_WAITER_VTABLE;
        let waiter_ptr = std::ptr::addr_of_mut!(waiter);
        unsafe {
            (*current).register_waiter(waiter_ptr);
            release_task(waiter_ptr);
        }
        SHUTDOWN_REENTRY_REACTOR.with(|stored| stored.set(reactor_ptr));
        SHUTDOWN_REENTRY_STATE.with(|stored| stored.set(current));

        let mut first_panic = None;
        unsafe {
            Reactor::prepare_shutdown_unchecked(reactor_ptr, &mut first_panic);
        }
        assert!(first_panic.is_none());
        SHUTDOWN_REENTRY_STATE.with(|stored| stored.set(std::ptr::null_mut()));
        SHUTDOWN_REENTRY_REACTOR.with(|stored| stored.set(std::ptr::null_mut()));

        SHUTDOWN_REENTRY_DESTROYS.with(|count| assert_eq!(count.get(), 1));
        assert_eq!(reactor.live_registry, vec![moved_tail]);
        unsafe {
            assert_eq!((*moved_tail).registry_index, 0);
            assert!((*moved_tail).is_runtime_shutdown());
            assert_eq!((*moved_tail).result, -libc::ECANCELED);
        }

        let replacement = reactor.alloc_op();
        assert_eq!(
            replacement, current,
            "shutdown re-entry did not return the completed state slot"
        );
        reactor.free_op(replacement);
        reactor.free_op(moved_tail);
        assert!(reactor.live_registry.is_empty());
    }

    #[test]
    fn shutdown_waiter_panics_preserve_live_storage_until_abandonment() {
        reset_shutdown_panic_test_state();
        let mut reactor = ringless_reactor();
        let reactor_ptr = std::ptr::addr_of_mut!(reactor);
        let completed = reactor.alloc_op();
        let first = reactor.alloc_op();
        let later = reactor.alloc_op();
        assert!(!completed.is_null() && !first.is_null() && !later.is_null());
        unsafe {
            (*completed).set_completed();
        }

        let payload_drops = Rc::new(Cell::new(0));
        let first_payload = reactor.alloc_retained_payload(ShutdownRetainedPayload {
            drops: Rc::clone(&payload_drops),
            _bytes: [0; 8],
        });
        let later_payload = reactor.alloc_retained_payload(ShutdownRetainedPayload {
            drops: Rc::clone(&payload_drops),
            _bytes: [0; 8],
        });
        unsafe {
            (*first).attach_retained_payload(first_payload);
            (*later).attach_retained_payload(later_payload);
        }

        let mut first_waiter = TaskHeader::new();
        first_waiter.vtable = &FIRST_SHUTDOWN_WAITER_VTABLE;
        let first_waiter_ptr = std::ptr::addr_of_mut!(first_waiter);
        let mut later_waiter = TaskHeader::new();
        later_waiter.vtable = &LATER_SHUTDOWN_WAITER_VTABLE;
        let later_waiter_ptr = std::ptr::addr_of_mut!(later_waiter);
        unsafe {
            (*first).register_waiter(first_waiter_ptr);
            (*later).register_waiter(later_waiter_ptr);
            release_task(first_waiter_ptr);
            release_task(later_waiter_ptr);
        }
        assert_eq!(first_waiter.refs.get(), 1);
        assert_eq!(later_waiter.refs.get(), 1);

        SHUTDOWN_PANIC_REACTOR.with(|stored| stored.set(reactor_ptr));
        SHUTDOWN_PANIC_COMPLETED_STATE.with(|stored| stored.set(completed));
        let mut first_panic = None;
        unsafe {
            Reactor::prepare_shutdown_unchecked(reactor_ptr, &mut first_panic);
        }
        SHUTDOWN_PANIC_COMPLETED_STATE.with(|stored| stored.set(std::ptr::null_mut()));
        SHUTDOWN_PANIC_REACTOR.with(|stored| stored.set(std::ptr::null_mut()));
        let appended = SHUTDOWN_PANIC_APPENDED_STATE.with(Cell::get);
        assert!(
            !appended.is_null(),
            "shutdown re-entry did not append a replacement state"
        );

        assert!(
            first_panic
                .as_ref()
                .is_some_and(|payload| payload.is::<FirstShutdownWaiterPanic>()),
            "shutdown did not retain the first waiter panic"
        );
        SHUTDOWN_FIRST_WAITER_DESTROYS.with(|count| assert_eq!(count.get(), 1));
        SHUTDOWN_LATER_WAITER_DESTROYS.with(|count| assert_eq!(count.get(), 1));
        SHUTDOWN_LATER_PANIC_PAYLOAD_DROPS.with(|count| assert_eq!(count.get(), 0));
        assert_eq!(first_waiter.refs.get(), 0);
        assert_eq!(later_waiter.refs.get(), 0);
        assert_eq!(reactor.live_registry, vec![later, first, appended]);
        for (index, state) in [later, first].into_iter().enumerate() {
            unsafe {
                assert_eq!((*state).registry_index, index as u32);
                assert!((*state).is_runtime_shutdown());
                assert!(!(*state).is_completed());
                assert!(!(*state).is_ring_abandoned());
            }
        }
        unsafe {
            assert_eq!((*appended).registry_index, 2);
            assert!((*appended).is_detached());
            assert!(!(*appended).is_runtime_shutdown());
        }
        unsafe {
            assert_queue_links(&reactor.pending_cancels, &[first, later]);
        }
        assert_eq!(payload_drops.get(), 0);
        assert_eq!(reactor.retained_payload_stats().pooled_frees, 0);

        let mut runtime_state = runtime_state_with_shutdown_inflight(3);
        unsafe {
            Reactor::abandon_shutdown_storage_unchecked(
                reactor_ptr,
                std::ptr::addr_of_mut!(runtime_state),
                &mut first_panic,
            );
        }

        assert!(reactor.storage_abandoned);
        assert!(reactor.ring.is_none());
        assert_eq!(runtime_state.inflight_ops, 0);
        unsafe {
            assert_queue_links(&reactor.pending_cancels, &[]);
        }
        assert_eq!(reactor.live_registry, vec![later, first, appended]);
        for (index, state) in [later, first].into_iter().enumerate() {
            unsafe {
                assert_eq!((*state).registry_index, index as u32);
                assert!((*state).is_runtime_shutdown());
                assert!((*state).is_ring_abandoned());
                assert!(!(*state).is_completed());
                assert!(!(*state).is_cancel_pending());
                assert!((*state).waiter.is_null());
                assert!((*state).cancel_next.is_null());
            }
        }
        unsafe {
            assert_eq!((*appended).registry_index, 2);
            assert!((*appended).is_detached());
            assert!(!(*appended).is_runtime_shutdown());
            assert!((*appended).is_ring_abandoned());
            assert!(!(*appended).is_cancel_pending());
            assert!((*appended).waiter.is_null());
            assert!((*appended).cancel_next.is_null());
        }
        assert_eq!(payload_drops.get(), 0);
        assert_eq!(reactor.retained_payload_stats().pooled_frees, 0);
        SHUTDOWN_LATER_PANIC_PAYLOAD_DROPS.with(|count| assert_eq!(count.get(), 0));

        let payload = first_panic
            .take()
            .expect("first shutdown panic disappeared before resumption");
        let resumed = catch_unwind(AssertUnwindSafe(|| resume_unwind(payload)))
            .expect_err("shutdown waiter panic was not resumed");
        assert!(
            resumed.downcast_ref::<FirstShutdownWaiterPanic>().is_some(),
            "shutdown resumed the wrong waiter panic"
        );
        drop(resumed);

        // The ringless Miri model has no kernel reference. Re-enable ordinary
        // reclamation only after all abandonment assertions have been made.
        unsafe {
            (*first).restore_completed_orphaned_after_ringless_abandonment_for_test();
            (*later).restore_completed_orphaned_after_ringless_abandonment_for_test();
            (*appended).restore_completed_orphaned_after_ringless_abandonment_for_test();
        }
        reactor.storage_abandoned = false;
        reactor.free_op(first);
        reactor.free_op(later);
        reactor.free_op(appended);
        assert!(reactor.live_registry.is_empty());
        assert_eq!(payload_drops.get(), 2);
        assert_eq!(reactor.retained_payload_stats().pooled_frees, 2);
        SHUTDOWN_LATER_PANIC_PAYLOAD_DROPS.with(|count| assert_eq!(count.get(), 0));
    }

    #[cfg(all(not(miri), any(debug_assertions, feature = "test-support")))]
    #[test]
    fn submitted_reads_are_abandoned_before_shutdown_resumes_waiter_panic() {
        reset_shutdown_panic_test_state();
        let (first_reader, _first_writer) = UnixStream::pair().expect("first socketpair failed");
        let (later_reader, _later_writer) = UnixStream::pair().expect("later socketpair failed");
        let payload_drops = Rc::new(Cell::new(0));
        let mut reactor =
            Reactor::new_with_config(ReactorConfig { ring_entries: 8 }).expect("reactor failed");
        reactor.init();

        let first = reactor.alloc_op();
        let later = reactor.alloc_op();
        assert!(!first.is_null() && !later.is_null());
        let first_payload = reactor.alloc_retained_payload(ShutdownRetainedPayload {
            drops: Rc::clone(&payload_drops),
            _bytes: [0; 8],
        });
        let later_payload = reactor.alloc_retained_payload(ShutdownRetainedPayload {
            drops: Rc::clone(&payload_drops),
            _bytes: [0; 8],
        });
        let first_buffer =
            unsafe { std::ptr::addr_of_mut!((*first_payload.as_ptr())._bytes).cast::<u8>() };
        let later_buffer =
            unsafe { std::ptr::addr_of_mut!((*later_payload.as_ptr())._bytes).cast::<u8>() };
        unsafe {
            (*first).attach_retained_payload(first_payload);
            (*later).attach_retained_payload(later_payload);
        }
        reactor
            .submit_sqe(
                opcode::Read::new(types::Fd(first_reader.as_raw_fd()), first_buffer, 1)
                    .build()
                    .user_data(first as u64),
            )
            .expect("first read submission failed");
        reactor
            .submit_sqe(
                opcode::Read::new(types::Fd(later_reader.as_raw_fd()), later_buffer, 1)
                    .build()
                    .user_data(later as u64),
            )
            .expect("later read submission failed");
        assert_eq!(
            reactor.flush_sqes().expect("read submission flush failed"),
            ReactorSubmitStatus::Ready
        );
        assert!(
            !reactor.has_queued_sqes(),
            "read SQEs were not consumed by the kernel"
        );

        let mut first_waiter = TaskHeader::new();
        first_waiter.vtable = &FIRST_SHUTDOWN_WAITER_VTABLE;
        let first_waiter_ptr = std::ptr::addr_of_mut!(first_waiter);
        let mut later_waiter = TaskHeader::new();
        later_waiter.vtable = &LATER_SHUTDOWN_WAITER_VTABLE;
        let later_waiter_ptr = std::ptr::addr_of_mut!(later_waiter);
        unsafe {
            (*first).register_waiter(first_waiter_ptr);
            (*later).register_waiter(later_waiter_ptr);
            release_task(first_waiter_ptr);
            release_task(later_waiter_ptr);
        }

        let mut runtime_state = runtime_state_with_shutdown_inflight(2);
        let mut ready_queue =
            crate::utils::list::intrusive::dlist::DList::<TaskHeader>::new_uninit();
        ready_queue.init();
        crate::runtime::test_hooks::force_next_reactor_shutdown_fallback();
        let unwind = catch_unwind(AssertUnwindSafe(|| unsafe {
            Reactor::shutdown_unchecked(
                std::ptr::addr_of_mut!(reactor),
                std::ptr::addr_of_mut!(runtime_state),
                std::ptr::addr_of_mut!(ready_queue),
            );
        }))
        .expect_err("shutdown waiter panic was not resumed");

        assert!(
            unwind.downcast_ref::<FirstShutdownWaiterPanic>().is_some(),
            "shutdown resumed the wrong waiter panic"
        );
        drop(unwind);
        SHUTDOWN_FIRST_WAITER_DESTROYS.with(|count| assert_eq!(count.get(), 1));
        SHUTDOWN_LATER_WAITER_DESTROYS.with(|count| assert_eq!(count.get(), 1));
        SHUTDOWN_LATER_PANIC_PAYLOAD_DROPS.with(|count| assert_eq!(count.get(), 0));
        assert_eq!(first_waiter.refs.get(), 0);
        assert_eq!(later_waiter.refs.get(), 0);
        assert_eq!(
            crate::runtime::test_hooks::reactor_shutdown_fallbacks_remaining(),
            0,
            "forced reactor fallback was not consumed"
        );
        assert!(reactor.storage_abandoned);
        assert!(reactor.ring.is_none());
        assert_eq!(runtime_state.inflight_ops, 0);
        assert_eq!(reactor.live_registry, vec![first, later]);
        unsafe {
            assert_queue_links(&reactor.pending_cancels, &[]);
        }
        for (index, state) in reactor.live_registry.iter().copied().enumerate() {
            unsafe {
                assert_eq!((*state).registry_index, index as u32);
                assert!((*state).is_runtime_shutdown());
                assert!((*state).is_ring_abandoned());
                assert!(!(*state).is_completed());
                assert!(!(*state).is_cancel_pending());
                assert!((*state).waiter.is_null());
                assert!((*state).cancel_next.is_null());
            }
        }
        assert_eq!(payload_drops.get(), 0);
        assert_eq!(reactor.retained_payload_stats().pooled_frees, 0);

        drop(reactor);
        assert_eq!(
            payload_drops.get(),
            0,
            "reactor drop released kernel-visible abandoned payloads"
        );
        SHUTDOWN_LATER_PANIC_PAYLOAD_DROPS.with(|count| assert_eq!(count.get(), 0));
    }

    #[cfg(not(miri))]
    #[test]
    fn shutdown_preserves_waiter_panic_when_retired_payload_panics_later() {
        reset_shutdown_panic_test_state();
        let (read_socket, _read_peer) = UnixStream::pair().expect("socketpair failed");
        let retired_payload_drops = Rc::new(Cell::new(0));
        let abandoned_payload_drops = Rc::new(Cell::new(0));
        let mut reactor =
            Reactor::new_with_config(ReactorConfig { ring_entries: 8 }).expect("reactor failed");
        reactor.init();

        let retired = reactor.alloc_op();
        let abandoned = reactor.alloc_op();
        let panic_trigger = reactor.alloc_op();
        assert!(!retired.is_null() && !abandoned.is_null() && !panic_trigger.is_null());
        let retired_payload = reactor.alloc_retained_payload(ShutdownRetainedPayloadDropBomb {
            drops: Rc::clone(&retired_payload_drops),
        });
        let abandoned_payload = reactor.alloc_retained_payload(ShutdownRetainedPayload {
            drops: Rc::clone(&abandoned_payload_drops),
            _bytes: [0; 8],
        });
        let abandoned_buffer =
            unsafe { std::ptr::addr_of_mut!((*abandoned_payload.as_ptr())._bytes).cast::<u8>() };
        unsafe {
            (*retired).attach_retained_payload(retired_payload);
            (*retired).set_orphaned();
            (*abandoned).attach_retained_payload(abandoned_payload);
            (*abandoned).set_orphaned();
            (*panic_trigger).set_build_aborted();
        }

        let mut first_waiter = TaskHeader::new();
        first_waiter.vtable = &FIRST_SHUTDOWN_WAITER_VTABLE;
        let first_waiter_ptr = std::ptr::addr_of_mut!(first_waiter);
        unsafe {
            (*panic_trigger).register_waiter(first_waiter_ptr);
            release_task(first_waiter_ptr);
        }
        assert_eq!(first_waiter.refs.get(), 1);

        reactor
            .submit_sqe(opcode::Nop::new().build().user_data(retired as u64))
            .expect("NOP submission failed");
        reactor
            .submit_sqe(
                opcode::Read::new(types::Fd(read_socket.as_raw_fd()), abandoned_buffer, 1)
                    .build()
                    .user_data(abandoned as u64),
            )
            .expect("read submission failed");
        assert_eq!(
            reactor.flush_sqes().expect("submission flush failed"),
            ReactorSubmitStatus::Ready
        );
        assert!(!reactor.has_queued_sqes());

        let mut runtime_state = runtime_state_with_shutdown_inflight(2);
        let mut ready_queue =
            crate::utils::list::intrusive::dlist::DList::<TaskHeader>::new_uninit();
        ready_queue.init();
        let unwind = catch_unwind(AssertUnwindSafe(|| unsafe {
            Reactor::shutdown_unchecked(
                std::ptr::addr_of_mut!(reactor),
                std::ptr::addr_of_mut!(runtime_state),
                std::ptr::addr_of_mut!(ready_queue),
            );
        }))
        .expect_err("shutdown waiter panic was not resumed");

        assert!(
            unwind.downcast_ref::<FirstShutdownWaiterPanic>().is_some(),
            "later retained-payload panic replaced the first waiter panic"
        );
        drop(unwind);
        SHUTDOWN_FIRST_WAITER_DESTROYS.with(|count| assert_eq!(count.get(), 1));
        SHUTDOWN_RETIRED_PANIC_PAYLOAD_DROPS.with(|count| assert_eq!(count.get(), 0));
        assert_eq!(first_waiter.refs.get(), 0);
        assert_eq!(retired_payload_drops.get(), 1);
        assert_eq!(abandoned_payload_drops.get(), 0);
        assert!(reactor.storage_abandoned);
        assert!(reactor.ring.is_none());
        assert_eq!(runtime_state.inflight_ops, 0);
        assert_eq!(reactor.live_registry, vec![panic_trigger, abandoned]);
        unsafe {
            assert_queue_links(&reactor.pending_cancels, &[]);
            assert_eq!((*panic_trigger).registry_index, 0);
            assert!((*panic_trigger).is_build_aborted());
            assert!(!(*panic_trigger).is_runtime_shutdown());
            assert!(!(*panic_trigger).is_ring_abandoned());
            assert_eq!((*abandoned).registry_index, 1);
            assert!((*abandoned).is_orphaned());
            assert!((*abandoned).is_ring_abandoned());
            assert!(!(*abandoned).is_completed());
            assert!(!(*abandoned).is_cancel_pending());
            assert!((*abandoned).waiter.is_null());
            assert!((*abandoned).cancel_next.is_null());
        }
        assert_eq!(reactor.retained_payload_stats().pooled_frees, 1);

        let replacement = reactor.alloc_op();
        assert_eq!(
            replacement, retired,
            "payload unwind did not return the retired operation slot"
        );
        assert_ne!(replacement, abandoned);
        reactor.free_op(replacement);
        reactor.free_op(panic_trigger);
        assert_eq!(reactor.live_registry, vec![abandoned]);
        unsafe {
            assert_eq!((*abandoned).registry_index, 0);
        }

        drop(reactor);
        assert_eq!(
            abandoned_payload_drops.get(),
            0,
            "reactor drop released the still kernel-visible read payload"
        );
        SHUTDOWN_RETIRED_PANIC_PAYLOAD_DROPS.with(|count| assert_eq!(count.get(), 0));
    }

    #[test]
    #[cfg(not(miri))]
    fn poll_io_keeps_payload_descriptor_off_the_live_completion_ring() {
        assert_eq!(
            test_completion_drain_descriptor_close()
                .expect("real completion-drain descriptor probe failed"),
            CompletionDrainDescriptorReport::EXPECTED
        );
    }

    #[test]
    fn free_op_fields_waiter_panic_reclaims_payload_and_operation_once() {
        CANCEL_WAITER_DESTROYS.with(|count| count.set(0));
        let payload_drops = Rc::new(Cell::new(0));
        let mut pending_cancels = PendingCancelQueue::new();
        let mut retained_pool =
            RetainedPayloadPool::new().expect("retained payload pool construction failed");
        let mut op_pool: ProviderOwnedPool<CompletionState, BasicMemoryProvider> =
            ProviderOwnedPool::new(BasicMemoryProvider::new(), OP_POOL_OBJS_PER_SLAB)
                .expect("operation pool construction failed");
        op_pool.init();
        let mut live_registry = Vec::new();

        let state = unsafe { op_pool.alloc(()) }.expect("operation allocation failed");
        unsafe { (*state).bind_owner(None, 0) };
        live_registry.push(state);
        // Stable execution also proves that a secondary payload panic cannot
        // replace the waiter panic. That branch intentionally forgets the
        // secondary panic payload, so Miri uses a nonpanicking payload while
        // still checking the raw reclamation and exact reuse.
        #[cfg(not(miri))]
        let panic_tag = Some("secondary payload panic");
        #[cfg(miri)]
        let panic_tag = None;
        let payload = retained_pool.alloc(OpPayloadDropBomb {
            drops: Rc::clone(&payload_drops),
            panic_tag,
        });
        let payload_ptr = payload.as_ptr();
        unsafe {
            (*state).attach_retained_payload(payload);
        }

        let mut waiter = TaskHeader::new();
        waiter.vtable = &PANIC_CANCEL_WAITER_VTABLE;
        let waiter_ptr = std::ptr::addr_of_mut!(waiter);
        unsafe {
            (*state).register_waiter(waiter_ptr);
            release_task(waiter_ptr);
        }
        assert_eq!(waiter.refs.get(), 1);

        let unwind = catch_unwind(AssertUnwindSafe(|| unsafe {
            free_op_fields(
                &mut pending_cancels,
                &mut retained_pool,
                &mut op_pool,
                &mut live_registry,
                state,
            )
            .expect("operation retirement failed before waiter destruction");
        }))
        .expect_err("waiter destructor did not panic");
        assert_eq!(
            unwind.downcast_ref::<&str>().copied(),
            Some("cancel waiter destroy panic"),
            "operation cleanup replaced the waiter panic"
        );
        CANCEL_WAITER_DESTROYS.with(|count| assert_eq!(count.get(), 1));
        assert_eq!(payload_drops.get(), 1);
        assert!(live_registry.is_empty());
        assert!(pending_cancels.is_empty());
        assert_eq!(retained_pool.stats().pooled_frees, 1);

        let replacement =
            unsafe { op_pool.alloc(()) }.expect("replacement operation allocation failed");
        assert_eq!(
            replacement, state,
            "waiter panic stranded the completion-state slot"
        );
        unsafe { (*replacement).bind_owner(None, 0) };
        live_registry.push(replacement);
        let replacement_payload = retained_pool.alloc(OpPayloadDropBomb {
            drops: Rc::clone(&payload_drops),
            panic_tag: None,
        });
        assert_eq!(
            replacement_payload.as_ptr(),
            payload_ptr,
            "waiter panic stranded retained payload backing"
        );
        unsafe {
            (*replacement).attach_retained_payload(replacement_payload);
            free_op_fields(
                &mut pending_cancels,
                &mut retained_pool,
                &mut op_pool,
                &mut live_registry,
                replacement,
            )
            .expect("replacement operation retirement failed");
        }
        assert_eq!(payload_drops.get(), 2);
        assert_eq!(retained_pool.stats().pooled_reuses, 1);
        assert_eq!(retained_pool.stats().pooled_frees, 2);
        assert!(live_registry.is_empty());
        assert!(pending_cancels.is_empty());
    }

    #[test]
    fn payload_panic_returns_slot_before_dropping_final_fd_lease() {
        let payload_drops = Rc::new(Cell::new(0));
        let mut pending_cancels = PendingCancelQueue::new();
        let mut retained_pool =
            RetainedPayloadPool::new().expect("retained payload pool construction failed");
        let mut op_pool: ProviderOwnedPool<CompletionState, BasicMemoryProvider> =
            ProviderOwnedPool::new(BasicMemoryProvider::new(), OP_POOL_OBJS_PER_SLAB)
                .expect("operation pool construction failed");
        op_pool.init();
        let op_pool_ptr = std::ptr::addr_of_mut!(op_pool);
        let mut live_registry = Vec::new();

        let state = unsafe { op_pool.alloc(()) }.expect("operation allocation failed");
        unsafe { (*state).bind_owner(None, 0) };
        live_registry.push(state);

        // `-2` is a deliberately non-closeable sentinel distinct from the
        // core's post-take `-1`, so the final-drop hook runs exactly once.
        let runtime = RuntimeFd::from_fresh_raw_fd(-2);
        unsafe { (*state).attach_fd_lease(runtime.lease()) };
        drop(runtime);
        let payload = retained_pool.alloc(OpPayloadDropBomb {
            drops: Rc::clone(&payload_drops),
            panic_tag: Some("payload before final fd lease"),
        });
        unsafe { (*state).attach_retained_payload(payload) };

        // Keep the re-entry probe and reclamation on the same raw provenance;
        // creating a fresh `&mut op_pool` while the hook is armed would itself
        // invalidate the raw capability the test is meant to exercise.
        FINAL_LEASE_REENTRY_POOL.with(|slot| slot.set(op_pool_ptr));
        FINAL_LEASE_REENTRY_EXPECTED.with(|slot| slot.set(state));
        FINAL_LEASE_REENTRY_REUSED.with(|slot| slot.set(false));
        set_final_core_drop_hook_for_test(Some(probe_final_lease_reentry_after_slot_return));

        let unwind = catch_unwind(AssertUnwindSafe(|| unsafe {
            free_op_fields(
                &mut pending_cancels,
                &mut retained_pool,
                op_pool_ptr,
                &mut live_registry,
                state,
            )
            .expect("operation retirement failed before payload destruction");
        }))
        .expect_err("retained payload destructor did not panic");

        set_final_core_drop_hook_for_test(None);
        FINAL_LEASE_REENTRY_POOL.with(|slot| slot.set(std::ptr::null_mut()));
        FINAL_LEASE_REENTRY_EXPECTED.with(|slot| slot.set(std::ptr::null_mut()));

        let panic = unwind
            .downcast_ref::<OpPayloadDropPanic>()
            .expect("operation cleanup replaced the payload panic");
        assert_eq!(panic.0, "payload before final fd lease");
        assert_eq!(payload_drops.get(), 1);
        assert!(live_registry.is_empty());
        assert!(pending_cancels.is_empty());
        FINAL_LEASE_REENTRY_REUSED.with(|reused| {
            assert!(
                reused.get(),
                "final fd lease dropped before the operation slot returned"
            );
        });

        let replacement =
            unsafe { op_pool.alloc(()) }.expect("post-reentry operation allocation failed");
        assert_eq!(replacement, state);
        unsafe { op_pool.free(replacement) };
    }

    #[test]
    fn free_op_reports_registry_removal_before_panicking_payload_and_address_reuse() {
        let payload_drops = Rc::new(Cell::new(0));
        let mut pending_cancels = PendingCancelQueue::new();
        let mut retained_pool =
            RetainedPayloadPool::new().expect("retained payload pool construction failed");
        let mut op_pool: ProviderOwnedPool<CompletionState, BasicMemoryProvider> =
            ProviderOwnedPool::new(BasicMemoryProvider::new(), OP_POOL_OBJS_PER_SLAB)
                .expect("operation pool construction failed");
        op_pool.init();
        let mut live_registry = Vec::new();

        let state = unsafe { op_pool.alloc(()) }.expect("operation allocation failed");
        unsafe { (*state).bind_owner(None, 0) };
        live_registry.push(state);
        let payload = retained_pool.alloc(OpPayloadDropBomb {
            drops: Rc::clone(&payload_drops),
            panic_tag: Some("reported removal"),
        });
        let payload_ptr = payload.as_ptr();
        unsafe {
            (*state).attach_retained_payload(payload);
        }

        let mut removal_reported = false;
        let unwind = catch_unwind(AssertUnwindSafe(|| unsafe {
            free_op_fields_with_removal_report(
                &mut pending_cancels,
                &mut retained_pool,
                &mut op_pool,
                &mut live_registry,
                state,
                || {
                    removal_reported = true;
                },
            )
            .expect("operation retirement failed before payload destruction");
        }))
        .expect_err("retained payload destructor did not panic");
        let panic = unwind
            .downcast_ref::<OpPayloadDropPanic>()
            .expect("operation cleanup replaced the payload panic");
        assert_eq!(panic.0, "reported removal");
        assert!(
            removal_reported,
            "registry removal was not reported before payload destruction"
        );
        assert!(live_registry.is_empty());
        assert!(pending_cancels.is_empty());
        assert_eq!(payload_drops.get(), 1);

        let replacement =
            unsafe { op_pool.alloc(()) }.expect("replacement operation allocation failed");
        assert_eq!(
            replacement, state,
            "panicking payload did not return the operation slot for exact address reuse"
        );
        unsafe { (*replacement).bind_owner(None, 0) };
        live_registry.push(replacement);
        assert!(
            live_registry.contains(&state),
            "replacement did not reproduce the ambiguous numerical pointer identity"
        );
        assert!(
            removal_reported,
            "address reuse changed the already-published removal event"
        );

        let replacement_payload = retained_pool.alloc(OpPayloadDropBomb {
            drops: Rc::clone(&payload_drops),
            panic_tag: None,
        });
        assert_eq!(
            replacement_payload.as_ptr(),
            payload_ptr,
            "panicking payload stranded its retained backing"
        );
        unsafe {
            (*replacement).attach_retained_payload(replacement_payload);
            free_op_fields(
                &mut pending_cancels,
                &mut retained_pool,
                &mut op_pool,
                &mut live_registry,
                replacement,
            )
            .expect("replacement operation retirement failed");
        }
        assert_eq!(payload_drops.get(), 2);
        assert_eq!(retained_pool.stats().pooled_reuses, 1);
        assert_eq!(retained_pool.stats().pooled_frees, 2);
        assert!(live_registry.is_empty());
        assert!(pending_cancels.is_empty());
    }

    #[test]
    fn free_op_fields_retained_drop_panic_recycles_operation_slot_once() {
        let pooled_drops = Rc::new(Cell::new(0));
        let heap_drops = Rc::new(Cell::new(0));
        let mut pending_cancels = PendingCancelQueue::new();
        let mut retained_pool =
            RetainedPayloadPool::new().expect("retained payload pool construction failed");
        let mut op_pool: ProviderOwnedPool<CompletionState, BasicMemoryProvider> =
            ProviderOwnedPool::new(BasicMemoryProvider::new(), OP_POOL_OBJS_PER_SLAB)
                .expect("operation pool construction failed");
        op_pool.init();
        let mut live_registry = Vec::new();

        let state = unsafe { op_pool.alloc(()) }.expect("operation allocation failed");
        unsafe { (*state).bind_owner(None, 0) };
        live_registry.push(state);
        let pooled = retained_pool.alloc(OpPayloadDropBomb {
            drops: Rc::clone(&pooled_drops),
            panic_tag: Some("pooled"),
        });
        let pooled_ptr = pooled.as_ptr();
        unsafe { (*state).attach_retained_payload(pooled) };

        let pooled_unwind = catch_unwind(AssertUnwindSafe(|| unsafe {
            free_op_fields(
                &mut pending_cancels,
                &mut retained_pool,
                &mut op_pool,
                &mut live_registry,
                state,
            )
            .expect("pooled operation retirement failed before payload drop");
        }))
        .expect_err("pooled retained payload destructor did not panic");
        let pooled_panic = pooled_unwind
            .downcast_ref::<OpPayloadDropPanic>()
            .expect("pooled cleanup replaced the original panic");
        assert_eq!(pooled_panic.0, "pooled");
        assert_eq!(pooled_drops.get(), 1);
        assert!(live_registry.is_empty());
        assert!(pending_cancels.is_empty());
        assert_eq!(retained_pool.stats().pooled_frees, 1);

        let pooled_replacement =
            unsafe { op_pool.alloc(()) }.expect("pooled replacement operation allocation failed");
        assert_eq!(
            pooled_replacement, state,
            "panicking payload stranded the operation slot"
        );
        unsafe { (*pooled_replacement).bind_owner(None, 0) };
        live_registry.push(pooled_replacement);
        let pooled_payload_replacement = retained_pool.alloc(OpPayloadDropBomb {
            drops: Rc::clone(&pooled_drops),
            panic_tag: None,
        });
        assert_eq!(
            pooled_payload_replacement.as_ptr(),
            pooled_ptr,
            "panicking payload stranded its pooled backing"
        );
        unsafe {
            (*pooled_replacement).attach_retained_payload(pooled_payload_replacement);
            free_op_fields(
                &mut pending_cancels,
                &mut retained_pool,
                &mut op_pool,
                &mut live_registry,
                pooled_replacement,
            )
            .expect("pooled replacement operation retirement failed");
        }
        assert_eq!(pooled_drops.get(), 2);
        assert_eq!(retained_pool.stats().pooled_reuses, 1);

        let heap_state = unsafe { op_pool.alloc(()) }.expect("heap operation allocation failed");
        assert_eq!(heap_state, state);
        unsafe { (*heap_state).bind_owner(None, 0) };
        live_registry.push(heap_state);
        let heap = retained_pool.alloc(HeapOpPayloadDropBomb {
            _bomb: OpPayloadDropBomb {
                drops: Rc::clone(&heap_drops),
                panic_tag: Some("heap"),
            },
        });
        unsafe { (*heap_state).attach_retained_payload(heap) };

        let heap_unwind = catch_unwind(AssertUnwindSafe(|| unsafe {
            free_op_fields(
                &mut pending_cancels,
                &mut retained_pool,
                &mut op_pool,
                &mut live_registry,
                heap_state,
            )
            .expect("heap operation retirement failed before payload drop");
        }))
        .expect_err("heap retained payload destructor did not panic");
        let heap_panic = heap_unwind
            .downcast_ref::<OpPayloadDropPanic>()
            .expect("heap cleanup replaced the original panic");
        assert_eq!(heap_panic.0, "heap");
        assert_eq!(heap_drops.get(), 1);
        assert!(live_registry.is_empty());
        assert!(pending_cancels.is_empty());
        assert_eq!(retained_pool.stats().heap_frees, 1);

        let heap_replacement =
            unsafe { op_pool.alloc(()) }.expect("heap replacement operation allocation failed");
        assert_eq!(heap_replacement, state);
        unsafe { (*heap_replacement).bind_owner(None, 0) };
        live_registry.push(heap_replacement);
        let heap_payload_replacement = retained_pool.alloc(HeapOpPayloadDropBomb {
            _bomb: OpPayloadDropBomb {
                drops: Rc::clone(&heap_drops),
                panic_tag: None,
            },
        });
        unsafe {
            (*heap_replacement).attach_retained_payload(heap_payload_replacement);
            free_op_fields(
                &mut pending_cancels,
                &mut retained_pool,
                &mut op_pool,
                &mut live_registry,
                heap_replacement,
            )
            .expect("heap replacement operation retirement failed");
        }
        assert_eq!(heap_drops.get(), 2);
        assert_eq!(retained_pool.stats().heap_frees, 2);
        assert!(live_registry.is_empty());
    }
}

#[cfg(all(test, not(miri)))]
mod tests {
    use crate::runtime::fd::{distinctive_closeable_test_fd, raw_fd_is_closed};

    use super::*;
    use std::cell::Cell;
    use std::os::fd::{AsRawFd, FromRawFd, RawFd};
    use std::rc::Rc;

    const KERNEL_TIMESPEC_CHILD_ENV: &str = "FLOWIO_KERNEL_TIMESPEC_WAIT_CHILD";
    const KERNEL_TIMESPEC_CHILD_TEST: &str =
        "runtime::reactor::tests::wait_for_events_duration_max_uses_bounded_kernel_timespec";

    fn runtime_state_with_inflight(inflight_ops: usize) -> RuntimeState {
        RuntimeState {
            live_tasks: 0,
            inflight_ops,
            #[cfg(debug_assertions)]
            stats: crate::runtime::executor::RuntimeStats::default(),
        }
    }

    fn close_fd_if_open(fd: RawFd) {
        if !raw_fd_is_closed(fd) {
            unsafe {
                libc::close(fd);
            }
        }
    }

    fn distinctive_owner() -> (RawFd, OwnedFd) {
        let raw = distinctive_closeable_test_fd().expect("distinctive fd failed");
        // SAFETY: the helper returns one open descriptor whose sole ownership
        // is transferred into this OwnedFd.
        (raw, unsafe { OwnedFd::from_raw_fd(raw) })
    }

    #[cfg(any(debug_assertions, feature = "test-support"))]
    fn nop_sqe(user_data: u64) -> io_uring::squeue::Entry {
        opcode::Nop::new().build().user_data(user_data)
    }

    #[test]
    fn ringless_close_submission_returns_broken_pipe_with_identical_owner() {
        let mut reactor =
            Reactor::new_ringless_for_test(8).expect("ringless reactor construction failed");
        let (raw, owner) = distinctive_owner();

        let (ringless, owner) = reactor
            .submit_close_sqe(owner, 41)
            .expect_err("ringless close submission unexpectedly succeeded");
        assert_eq!(ringless.kind(), io::ErrorKind::BrokenPipe);
        assert_eq!(owner.as_raw_fd(), raw);
        assert!(!raw_fd_is_closed(raw));
        assert_eq!(reactor.queued_head, 0);
        assert_eq!(reactor.next_sequence, 0);
        assert!(reactor.pending_closes.is_empty());

        drop(owner);
        assert!(raw_fd_is_closed(raw));
    }

    #[cfg(any(debug_assertions, feature = "test-support"))]
    #[test]
    fn ringless_close_submission_preserves_test_hook_precedence_and_owner() {
        let mut reactor =
            Reactor::new_ringless_for_test(8).expect("ringless reactor construction failed");
        let (raw, owner) = distinctive_owner();
        crate::runtime::test_hooks::fail_next_raw_sqe_submit();

        let (injected, owner) = reactor
            .submit_close_sqe(owner, 41)
            .expect_err("injected close submission failure lost precedence");

        assert_eq!(injected.kind(), io::ErrorKind::WouldBlock);
        assert_eq!(owner.as_raw_fd(), raw);
        assert!(!raw_fd_is_closed(raw));
        assert_eq!(reactor.queued_head, 0);
        assert_eq!(reactor.next_sequence, 0);
        assert!(reactor.pending_closes.is_empty());

        drop(owner);
        assert!(raw_fd_is_closed(raw));
    }

    #[test]
    fn cancel_cqes_do_not_consume_poll_budget() {
        assert!(!cqe_consumes_poll_budget(0));
        assert!(cqe_consumes_poll_budget(1));
        assert!(cqe_consumes_poll_budget(u64::MAX));
    }

    #[cfg(not(miri))]
    #[test]
    fn cancel_cqe_before_target_retains_payload_until_target_retirement() {
        use std::io::Write;

        let mut reactor =
            Reactor::new_with_config(ReactorConfig { ring_entries: 8 }).expect("reactor failed");
        reactor.init();
        let reactor_ptr = std::ptr::from_mut(&mut reactor);
        let state = reactor.alloc_op();
        assert!(!state.is_null(), "operation allocation failed");

        let (read_end, mut write_end) =
            std::os::unix::net::UnixStream::pair().expect("socketpair failed");
        let retained_fd = OwnedFd::from(read_end);
        let raw_fd = retained_fd.as_raw_fd();
        let payload = reactor.alloc_retained_payload(retained_fd);
        unsafe {
            (*state).attach_retained_payload(payload);
            (*state).set_detached();
        }

        // The unmatched cancel completes immediately with user_data zero;
        // the target poll remains blocked until its peer becomes readable.
        // This deterministically presents the cancel CQE first without
        // inventing a synthetic completion path.
        reactor
            .submit_sqe(opcode::AsyncCancel::new(u64::MAX).build().user_data(0))
            .expect("cancel submission failed");
        reactor
            .submit_sqe(
                opcode::PollAdd::new(types::Fd(raw_fd), libc::POLLIN as u32)
                    .build()
                    .user_data(state as u64),
            )
            .expect("target submission failed");

        let mut runtime_state = runtime_state_with_inflight(1);
        let mut ready_queue =
            crate::utils::list::intrusive::dlist::DList::<TaskHeader>::new_uninit();
        ready_queue.init();

        submit_and_wait_retry_eintr(&mut reactor, 1).expect("cancel completion wait failed");
        let first = unsafe {
            Reactor::poll_io_unchecked(reactor_ptr, 1, &mut runtime_state, &mut ready_queue)
        }
        .expect("cancel completion drain failed");
        assert_eq!(first, 0, "cancel CQE consumed target-completion budget");
        assert_eq!(runtime_state.inflight_ops, 1);
        assert_eq!(reactor.live_op_count(), 1);
        assert!(!unsafe { (*state).is_completed() });
        assert!(!raw_fd_is_closed(raw_fd));

        write_end
            .write_all(&[0x5a])
            .expect("target readiness write failed");
        submit_and_wait_retry_eintr(&mut reactor, 1).expect("target completion wait failed");
        let second = unsafe {
            Reactor::poll_io_unchecked(reactor_ptr, 1, &mut runtime_state, &mut ready_queue)
        }
        .expect("target completion drain failed");
        assert_eq!(second, 1);
        assert_eq!(runtime_state.inflight_ops, 0);
        assert_eq!(reactor.live_op_count(), 0);
        assert!(raw_fd_is_closed(raw_fd));
    }

    #[test]
    fn completion_state_pool_slots_are_cache_line_aligned() {
        const CACHE_LINE: usize = 64;

        assert_eq!(std::mem::align_of::<CompletionState>(), CACHE_LINE);
        assert_eq!(std::mem::size_of::<CompletionState>() % CACHE_LINE, 0);

        let mut reactor =
            Reactor::new_with_config(ReactorConfig { ring_entries: 8 }).expect("reactor failed");
        reactor.init();

        let first = reactor.alloc_op();
        let second = reactor.alloc_op();
        assert!(!first.is_null(), "first op allocation failed");
        assert!(!second.is_null(), "second op allocation failed");

        let first_addr = first as usize;
        let second_addr = second as usize;
        assert_eq!(first_addr % CACHE_LINE, 0);
        assert_eq!(second_addr % CACHE_LINE, 0);
        assert_eq!(
            first_addr.abs_diff(second_addr),
            std::mem::size_of::<CompletionState>()
        );

        reactor.free_op(second);
        reactor.free_op(first);
    }

    #[test]
    fn close_ledger_retires_only_markers_inside_a_partial_prefix() {
        let mut reactor =
            Reactor::new_with_config(ReactorConfig { ring_entries: 8 }).expect("reactor failed");
        reactor.init();
        let (first_raw, first) = distinctive_owner();
        let (boundary_raw, boundary) = distinctive_owner();
        let (suffix_raw, suffix) = distinctive_owner();

        reactor.queued_head = 100;
        reactor.next_sequence = 104;
        reactor.pending_closes.push_back(PendingClose {
            sequence: 100,
            fd: first,
        });
        reactor.pending_closes.push_back(PendingClose {
            sequence: 102,
            fd: boundary,
        });
        reactor.pending_closes.push_back(PendingClose {
            sequence: 103,
            fd: suffix,
        });

        reactor
            .retire_submitted(2)
            .expect("partial ledger retirement failed");

        assert_eq!(reactor.queued_head, 102);
        assert_eq!(reactor.next_sequence, 104);
        assert_eq!(reactor.pending_closes.len(), 2);
        assert_eq!(reactor.pending_closes[0].sequence, 102);
        assert_eq!(reactor.pending_closes[1].sequence, 103);
        assert!(
            !raw_fd_is_closed(first_raw),
            "synthetic retirement suppresses the consumed userspace owner"
        );
        assert!(!raw_fd_is_closed(boundary_raw));
        assert!(!raw_fd_is_closed(suffix_raw));

        close_fd_if_open(first_raw);
        drop(reactor.ring.take());
        reactor.drop_unsubmitted_close_owners();
        assert!(raw_fd_is_closed(boundary_raw));
        assert!(raw_fd_is_closed(suffix_raw));
    }

    #[test]
    fn close_ledger_offsets_remain_correct_across_sequence_wrap() {
        let mut reactor =
            Reactor::new_with_config(ReactorConfig { ring_entries: 8 }).expect("reactor failed");
        reactor.init();
        let (before_raw, before) = distinctive_owner();
        let (inside_raw, inside) = distinctive_owner();
        let (after_raw, after) = distinctive_owner();

        reactor.queued_head = u64::MAX - 1;
        reactor.next_sequence = 2;
        reactor.pending_closes.push_back(PendingClose {
            sequence: u64::MAX - 1,
            fd: before,
        });
        reactor.pending_closes.push_back(PendingClose {
            sequence: 0,
            fd: inside,
        });
        reactor.pending_closes.push_back(PendingClose {
            sequence: 1,
            fd: after,
        });

        reactor
            .retire_submitted(3)
            .expect("wrapped ledger retirement failed");

        assert_eq!(reactor.queued_head, 1);
        assert_eq!(reactor.next_sequence, 2);
        assert_eq!(reactor.pending_closes.len(), 1);
        assert_eq!(reactor.pending_closes[0].sequence, 1);
        close_fd_if_open(before_raw);
        close_fd_if_open(inside_raw);
        drop(reactor.ring.take());
        reactor.drop_unsubmitted_close_owners();
        assert!(raw_fd_is_closed(after_raw));
    }

    #[test]
    fn close_ledger_reconciles_multiple_partial_submissions() {
        let mut reactor =
            Reactor::new_with_config(ReactorConfig { ring_entries: 8 }).expect("reactor failed");
        reactor.init();
        let (first_raw, first) = distinctive_owner();
        let (second_raw, second) = distinctive_owner();
        let (third_raw, third) = distinctive_owner();

        reactor.queued_head = 10;
        reactor.next_sequence = 15;
        reactor.pending_closes.push_back(PendingClose {
            sequence: 11,
            fd: first,
        });
        reactor.pending_closes.push_back(PendingClose {
            sequence: 13,
            fd: second,
        });
        reactor.pending_closes.push_back(PendingClose {
            sequence: 14,
            fd: third,
        });

        reactor
            .retire_submitted(2)
            .expect("first partial retirement failed");
        assert_eq!(reactor.queued_head, 12);
        assert_eq!(reactor.pending_closes.len(), 2);
        reactor
            .retire_submitted(2)
            .expect("second partial retirement failed");
        assert_eq!(reactor.queued_head, 14);
        assert_eq!(reactor.pending_closes.len(), 1);
        reactor
            .retire_submitted(1)
            .expect("final partial retirement failed");
        assert_eq!(reactor.queued_head, 0);
        assert_eq!(reactor.next_sequence, 0);
        assert!(reactor.pending_closes.is_empty());

        close_fd_if_open(first_raw);
        close_fd_if_open(second_raw);
        close_fd_if_open(third_raw);
    }

    #[cfg(any(debug_assertions, feature = "test-support"))]
    #[test]
    fn failed_close_push_returns_the_identical_owner_without_a_marker() {
        let mut reactor =
            Reactor::new_with_config(ReactorConfig { ring_entries: 8 }).expect("reactor failed");
        reactor.init();
        let (raw, owner) = distinctive_owner();
        crate::runtime::test_hooks::fail_next_raw_sqe_submit();

        let (_err, returned) = reactor
            .submit_close_sqe(owner, 0)
            .expect_err("injected close push failure should return ownership");

        assert_eq!(returned.as_raw_fd(), raw);
        assert!(!raw_fd_is_closed(raw));
        assert!(!reactor.has_queued_sqes());
        assert!(reactor.pending_closes.is_empty());
        drop(returned);
        assert!(raw_fd_is_closed(raw));
    }

    #[cfg(any(debug_assertions, feature = "test-support"))]
    #[test]
    fn close_submit_error_preserves_the_queued_owner_and_sequence() {
        let mut reactor =
            Reactor::new_with_config(ReactorConfig { ring_entries: 8 }).expect("reactor failed");
        reactor.init();
        let (raw, owner) = distinctive_owner();
        reactor
            .submit_close_sqe(owner, 0)
            .expect("queue close failed");
        crate::runtime::test_hooks::fail_next_ring_submit_errno(libc::EBUSY);

        assert_eq!(
            reactor.flush_sqes().expect("flush status failed"),
            ReactorSubmitStatus::Busy
        );
        assert_eq!(reactor.queued_head, 0);
        assert_eq!(reactor.next_sequence, 1);
        assert_eq!(reactor.pending_closes.len(), 1);
        assert!(!raw_fd_is_closed(raw));

        drop(reactor.ring.take());
        reactor.drop_unsubmitted_close_owners();
        assert!(raw_fd_is_closed(raw));
    }

    #[cfg(any(debug_assertions, feature = "test-support"))]
    #[test]
    fn initially_full_close_submission_reborrows_and_preserves_marker_order() {
        let mut reactor =
            Reactor::new_with_config(ReactorConfig { ring_entries: 2 }).expect("reactor failed");
        reactor.init();
        reactor
            .submit_sqe(nop_sqe(1))
            .expect("first nop submit failed");
        reactor
            .submit_sqe(nop_sqe(2))
            .expect("second nop submit failed");
        let (raw, owner) = distinctive_owner();

        reactor
            .submit_close_sqe(owner, 41)
            .expect("full-queue close submission failed");

        assert_eq!(reactor.queued_head, 0);
        assert_eq!(reactor.next_sequence, 1);
        assert_eq!(reactor.pending_closes.len(), 1);
        assert_eq!(reactor.pending_closes[0].sequence, 0);
        assert_eq!(reactor.pending_closes[0].fd.as_raw_fd(), raw);
        assert!(!raw_fd_is_closed(raw));

        assert_eq!(
            reactor.flush_sqes().expect("close flush failed"),
            ReactorSubmitStatus::Ready
        );
        assert!(reactor.pending_closes.is_empty());
        assert!(raw_fd_is_closed(raw));
    }

    #[cfg(any(debug_assertions, feature = "test-support"))]
    #[test]
    fn initially_full_close_persistent_ebusy_returns_owner_without_marker() {
        let mut reactor =
            Reactor::new_with_config(ReactorConfig { ring_entries: 2 }).expect("reactor failed");
        reactor.init();
        reactor
            .submit_sqe(nop_sqe(1))
            .expect("first nop submit failed");
        reactor
            .submit_sqe(nop_sqe(2))
            .expect("second nop submit failed");
        let before_head = reactor.queued_head;
        let before_sequence = reactor.next_sequence;
        let (raw, owner) = distinctive_owner();
        crate::runtime::test_hooks::fail_next_ring_submit_errno(libc::EBUSY);
        crate::runtime::test_hooks::fail_next_ring_submit_errno(libc::EBUSY);

        let (err, returned) = reactor
            .submit_close_sqe(owner, 41)
            .expect_err("persistent full-queue pressure should return ownership");

        assert_eq!(err.kind(), io::ErrorKind::WouldBlock);
        assert_eq!(returned.as_raw_fd(), raw);
        assert!(!raw_fd_is_closed(raw));
        assert_eq!(reactor.queued_head, before_head);
        assert_eq!(reactor.next_sequence, before_sequence);
        assert!(reactor.pending_closes.is_empty());
        drop(returned);
        assert!(raw_fd_is_closed(raw));
    }

    #[cfg(any(debug_assertions, feature = "test-support"))]
    #[test]
    fn full_close_submission_error_precedes_marker_capacity() {
        let mut reactor =
            Reactor::new_with_config(ReactorConfig { ring_entries: 2 }).expect("reactor failed");
        reactor.init();
        reactor
            .submit_sqe(nop_sqe(1))
            .expect("first nop submit failed");
        reactor
            .submit_sqe(nop_sqe(2))
            .expect("second nop submit failed");
        reactor.max_pending_closes = 0;
        let before_head = reactor.queued_head;
        let before_sequence = reactor.next_sequence;
        let (raw, owner) = distinctive_owner();
        crate::runtime::test_hooks::fail_next_ring_submit_errno(libc::EIO);

        let (err, returned) = reactor
            .submit_close_sqe(owner, 41)
            .expect_err("full-queue submission error lost precedence");

        assert_eq!(err.raw_os_error(), Some(libc::EIO));
        assert_eq!(returned.as_raw_fd(), raw);
        assert!(!raw_fd_is_closed(raw));
        assert_eq!(reactor.queued_head, before_head);
        assert_eq!(reactor.next_sequence, before_sequence);
        assert!(reactor.pending_closes.is_empty());
        drop(returned);
        assert!(raw_fd_is_closed(raw));
    }

    #[test]
    fn retired_close_owner_cannot_close_a_reused_numeric_fd_at_teardown() {
        let mut reactor =
            Reactor::new_with_config(ReactorConfig { ring_entries: 8 }).expect("reactor failed");
        reactor.init();
        let (raw, owner) = distinctive_owner();
        reactor
            .submit_close_sqe(owner, 0)
            .expect("queue close failed");
        assert_eq!(
            reactor.flush_sqes().expect("flush close failed"),
            ReactorSubmitStatus::Ready
        );
        assert!(reactor.pending_closes.is_empty());
        assert!(raw_fd_is_closed(raw), "kernel did not consume close SQE");

        let (source, peer) =
            std::os::unix::net::UnixStream::pair().expect("replacement socketpair failed");
        // SAFETY: dup2 atomically creates a new descriptor owner at `raw`.
        let replacement = unsafe { libc::dup2(source.as_raw_fd(), raw) };
        assert_eq!(replacement, raw, "numeric fd reuse failed");
        assert!(!raw_fd_is_closed(raw));

        drop(reactor);
        assert!(
            !raw_fd_is_closed(raw),
            "reactor teardown closed a number whose ledger owner had retired"
        );
        close_fd_if_open(raw);
        drop(source);
        drop(peer);
    }

    #[test]
    fn free_op_retirement_updates_live_accounting() {
        let mut reactor =
            Reactor::new_with_config(ReactorConfig { ring_entries: 8 }).expect("reactor failed");
        reactor.init();

        let state = reactor.alloc_op();
        assert!(!state.is_null(), "op allocation failed");
        assert_eq!(reactor.live_registry.len(), 1);

        reactor
            .try_free_op(state)
            .expect("live op should retire cleanly");

        assert!(reactor.live_registry.is_empty());
    }

    #[test]
    fn free_op_swap_removal_updates_moved_registry_index() {
        let mut reactor =
            Reactor::new_with_config(ReactorConfig { ring_entries: 8 }).expect("reactor failed");
        reactor.init();

        let first = reactor.alloc_op();
        let second = reactor.alloc_op();
        assert!(!first.is_null(), "first op allocation failed");
        assert!(!second.is_null(), "second op allocation failed");
        assert_eq!(unsafe { (*first).registry_index }, 0);
        assert_eq!(unsafe { (*second).registry_index }, 1);

        reactor.free_op(first);

        assert_eq!(reactor.live_registry, vec![second]);
        assert_eq!(unsafe { (*second).registry_index }, 0);
        reactor.free_op(second);
        assert!(reactor.live_registry.is_empty());
    }

    #[test]
    fn free_op_without_live_op_is_an_error() {
        let mut reactor =
            Reactor::new_with_config(ReactorConfig { ring_entries: 8 }).expect("reactor failed");
        reactor.init();

        let state = reactor.alloc_op();
        assert!(!state.is_null(), "op allocation failed");
        reactor.live_registry.clear();

        let err = reactor
            .try_free_op(state)
            .expect_err("freeing without a live op should fail");

        assert_eq!(err.kind(), io::ErrorKind::Other);
        assert!(reactor.live_registry.is_empty());

        unsafe {
            (*state).registry_index = 0;
        }
        reactor.live_registry.push(state);
        reactor.free_op(state);
    }

    #[test]
    fn tracked_completion_retirement_updates_inflight_accounting() {
        let mut runtime_state = runtime_state_with_inflight(1);

        retire_tracked_completion(&mut runtime_state).expect("tracked completion should retire");

        assert_eq!(runtime_state.inflight_ops, 0);
        #[cfg(debug_assertions)]
        assert_eq!(runtime_state.stats.cqe_completions, 1);
    }

    #[test]
    fn tracked_completion_without_inflight_op_is_an_error() {
        let mut runtime_state = runtime_state_with_inflight(0);

        let err = retire_tracked_completion(&mut runtime_state)
            .expect_err("untracked completion should fail");

        assert_eq!(err.kind(), io::ErrorKind::Other);
        assert_eq!(runtime_state.inflight_ops, 0);
        #[cfg(debug_assertions)]
        assert_eq!(runtime_state.stats.cqe_completions, 0);
    }

    #[cfg(any(debug_assertions, feature = "test-support"))]
    #[test]
    fn submit_sqe_full_queue_absorbs_eintr_before_push() {
        let mut reactor =
            Reactor::new_with_config(ReactorConfig { ring_entries: 2 }).expect("reactor failed");
        reactor.init();

        reactor
            .submit_sqe(nop_sqe(1))
            .expect("first nop submit failed");
        reactor
            .submit_sqe(nop_sqe(2))
            .expect("second nop submit failed");

        crate::runtime::test_hooks::fail_next_ring_submit_errno(libc::EINTR);
        reactor
            .submit_sqe(nop_sqe(3))
            .expect("EINTR while making SQ space should be absorbed");
        assert_eq!(
            crate::runtime::test_hooks::ring_submit_failures_remaining(),
            0,
            "full-queue EINTR hook was not consumed"
        );
        assert!(
            reactor.has_queued_sqes(),
            "third NOP should remain pending after push"
        );
    }

    #[cfg(any(debug_assertions, feature = "test-support"))]
    #[test]
    fn submit_sqe_success_wraps_sequence_after_push() {
        let mut reactor =
            Reactor::new_with_config(ReactorConfig { ring_entries: 8 }).expect("reactor failed");
        reactor.init();
        reactor.queued_head = u64::MAX;
        reactor.next_sequence = u64::MAX;

        reactor
            .submit_sqe(nop_sqe(41))
            .expect("wrapped NOP submission failed");

        assert_eq!(reactor.queued_head, u64::MAX);
        assert_eq!(reactor.next_sequence, 0);
        assert_eq!(reactor.queued_sqe_count(), 1);
        assert!(reactor.has_queued_sqes());
    }

    #[cfg(any(debug_assertions, feature = "test-support"))]
    #[test]
    fn submit_sqe_full_queue_transient_ebusy_retries_and_pushes() {
        let mut reactor =
            Reactor::new_with_config(ReactorConfig { ring_entries: 2 }).expect("reactor failed");
        reactor.init();

        reactor
            .submit_sqe(nop_sqe(1))
            .expect("first nop submit failed");
        reactor
            .submit_sqe(nop_sqe(2))
            .expect("second nop submit failed");

        crate::runtime::test_hooks::fail_next_ring_submit_errno(libc::EBUSY);
        reactor
            .submit_sqe(nop_sqe(3))
            .expect("transient EBUSY should be retried");
        assert_eq!(
            crate::runtime::test_hooks::ring_submit_failures_remaining(),
            0,
            "full-queue EBUSY hook was not consumed"
        );
        assert!(
            reactor.has_queued_sqes(),
            "third NOP should remain pending after transient submit pressure"
        );
    }

    #[cfg(any(debug_assertions, feature = "test-support"))]
    #[test]
    fn submit_sqe_full_queue_persistent_ebusy_returns_would_block() {
        let mut reactor =
            Reactor::new_with_config(ReactorConfig { ring_entries: 2 }).expect("reactor failed");
        reactor.init();

        reactor
            .submit_sqe(nop_sqe(1))
            .expect("first nop submit failed");
        reactor
            .submit_sqe(nop_sqe(2))
            .expect("second nop submit failed");

        crate::runtime::test_hooks::fail_next_ring_submit_errno(libc::EBUSY);
        crate::runtime::test_hooks::fail_next_ring_submit_errno(libc::EBUSY);
        let err = reactor
            .submit_sqe(nop_sqe(3))
            .expect_err("persistent EBUSY should surface as pressure");
        assert_eq!(err.kind(), io::ErrorKind::WouldBlock);
        assert!(
            reactor.has_queued_sqes(),
            "existing queued SQEs should remain pending after pressure"
        );
    }

    #[cfg(any(debug_assertions, feature = "test-support"))]
    #[test]
    fn flush_sqes_reports_busy_on_submit_ebusy() {
        let mut reactor =
            Reactor::new_with_config(ReactorConfig { ring_entries: 2 }).expect("reactor failed");
        reactor.init();

        reactor.submit_sqe(nop_sqe(1)).expect("nop submit failed");

        crate::runtime::test_hooks::fail_next_ring_submit_errno(libc::EBUSY);
        let status = reactor
            .flush_sqes()
            .expect("flush should not fail on EBUSY");
        assert_eq!(status, ReactorSubmitStatus::Busy);
    }

    #[cfg(any(debug_assertions, feature = "test-support"))]
    #[test]
    fn wait_for_events_reports_busy_with_pending_cancel_retry() {
        let mut reactor =
            Reactor::new_with_config(ReactorConfig { ring_entries: 8 }).expect("reactor failed");
        reactor.init();
        let state = reactor.alloc_op();
        assert!(!state.is_null(), "op allocation failed");

        unsafe { (*state).set_orphaned() };
        reactor.queue_pending_cancel(state);

        crate::runtime::test_hooks::fail_next_raw_sqe_submit();
        let status = reactor
            .wait_for_events(Some(Duration::ZERO))
            .expect("wait_for_events should report pending cancel pressure");
        assert_eq!(status, ReactorSubmitStatus::Busy);

        reactor.free_op(state);
    }

    #[cfg(any(debug_assertions, feature = "test-support"))]
    #[test]
    fn wait_for_events_busy_flushes_cancel_before_the_countless_wait_error() {
        let mut reactor =
            Reactor::new_with_config(ReactorConfig { ring_entries: 8 }).expect("reactor failed");
        reactor.init();
        let state = reactor.alloc_op();
        assert!(!state.is_null(), "op allocation failed");

        unsafe { (*state).set_orphaned() };
        reactor.queue_pending_cancel(state);

        crate::runtime::test_hooks::fail_next_ring_wait_errno(libc::EBUSY);
        let status = reactor
            .wait_for_events(Some(Duration::from_millis(1)))
            .expect("wait_for_events should report busy wait pressure");
        assert_eq!(status, ReactorSubmitStatus::Busy);
        assert!(
            !reactor.has_queued_sqes(),
            "count-bearing submit must flush queued SQEs before a timed wait"
        );

        reactor.free_op(state);
    }

    #[cfg(any(debug_assertions, feature = "test-support"))]
    #[test]
    fn wait_for_events_accepts_duration_max_without_instant_overflow() {
        let mut reactor =
            Reactor::new_with_config(ReactorConfig { ring_entries: 2 }).expect("reactor failed");
        reactor.init();

        crate::runtime::test_hooks::fail_next_ring_wait_errno(libc::EBUSY);
        let status = reactor
            .wait_for_events(Some(Duration::MAX))
            .expect("injected busy wait should remain recoverable");
        assert_eq!(status, ReactorSubmitStatus::Busy);
        assert_eq!(
            crate::runtime::test_hooks::ring_wait_failures_remaining(),
            0
        );
    }

    #[test]
    fn wait_for_events_duration_max_uses_bounded_kernel_timespec() {
        if std::env::var_os(KERNEL_TIMESPEC_CHILD_ENV).is_none() {
            use std::process::{Command, Stdio};

            let current_exe = std::env::current_exe().expect("current unit-test executable");
            let mut child = Command::new(current_exe)
                .args(["--exact", KERNEL_TIMESPEC_CHILD_TEST, "--nocapture"])
                .env(KERNEL_TIMESPEC_CHILD_ENV, "1")
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .spawn()
                .expect("spawn bounded-timespec child");
            let deadline = Instant::now() + Duration::from_secs(8);

            loop {
                if child
                    .try_wait()
                    .expect("poll bounded-timespec child")
                    .is_some()
                {
                    let output = child
                        .wait_with_output()
                        .expect("collect bounded-timespec child output");
                    let stdout = String::from_utf8_lossy(&output.stdout);
                    let stderr = String::from_utf8_lossy(&output.stderr);
                    assert!(
                        output.status.success(),
                        "bounded-timespec child failed: status={:?}, stdout={}, stderr={}",
                        output.status,
                        stdout,
                        stderr
                    );
                    assert!(
                        stdout.contains("1 passed;"),
                        "bounded-timespec child did not run exactly one test: stdout={}, stderr={}",
                        stdout,
                        stderr
                    );
                    return;
                }
                if Instant::now() >= deadline {
                    let _ = child.kill();
                    let output = child
                        .wait_with_output()
                        .expect("reap timed-out bounded-timespec child");
                    panic!(
                        "bounded-timespec child exceeded watchdog; stdout={}, stderr={}",
                        String::from_utf8_lossy(&output.stdout),
                        String::from_utf8_lossy(&output.stderr)
                    );
                }
                std::thread::sleep(Duration::from_millis(10));
            }
        }

        const TIMEOUT_USER_DATA: u64 = u64::MAX - 200;
        let short_duration = bounded_kernel_timespec_duration(Duration::from_millis(250));
        let short_timespec = types::Timespec::from(short_duration);
        let mut reactor =
            Reactor::new_with_config(ReactorConfig { ring_entries: 2 }).expect("reactor failed");
        reactor.init();
        reactor
            .submit_sqe(
                opcode::Timeout::new(&short_timespec)
                    .count(1)
                    .build()
                    .user_data(TIMEOUT_USER_DATA),
            )
            .expect("short timeout submission failed");

        assert_eq!(
            reactor
                .wait_for_events(Some(Duration::MAX))
                .expect("saturated kernel wait failed"),
            ReactorSubmitStatus::Ready
        );

        let mut completions = reactor
            .ring
            .as_mut()
            .expect("initialized reactor missing ring")
            .completion();
        completions.sync();
        let completion = completions.next().expect("short timeout CQE missing");
        assert_eq!(completion.user_data(), TIMEOUT_USER_DATA);
        assert_eq!(completion.result(), -libc::ETIME);
        assert!(completions.next().is_none(), "unexpected extra completion");
    }

    #[cfg(any(debug_assertions, feature = "test-support"))]
    #[test]
    fn timed_wait_flushes_close_owner_before_the_countless_wait_error() {
        let mut reactor =
            Reactor::new_with_config(ReactorConfig { ring_entries: 8 }).expect("reactor failed");
        reactor.init();
        let raw = distinctive_closeable_test_fd().expect("close test fd failed");
        // SAFETY: the helper returned one sole-owned live descriptor.
        let owned = unsafe { OwnedFd::from_raw_fd(raw) };
        reactor
            .submit_close_sqe(owned, 0)
            .expect("queue close owner failed");
        assert_eq!(reactor.pending_closes.len(), 1);

        crate::runtime::test_hooks::fail_next_ring_wait_errno(libc::EBUSY);
        let status = reactor
            .wait_for_events(Some(Duration::from_millis(1)))
            .expect("wait_for_events should report busy wait pressure");
        assert_eq!(status, ReactorSubmitStatus::Busy);
        assert!(
            reactor.pending_closes.is_empty(),
            "count-bearing preflush must retire the accepted close owner"
        );
        assert!(
            raw_fd_is_closed(raw),
            "kernel-accepted close must retire the descriptor before timed wait"
        );
    }

    #[cfg(any(debug_assertions, feature = "test-support"))]
    #[test]
    fn failed_cancel_submission_is_queued_and_retried() {
        let mut reactor =
            Reactor::new_with_config(ReactorConfig { ring_entries: 8 }).expect("reactor failed");
        reactor.init();
        let state = reactor.alloc_op();
        assert!(!state.is_null(), "op allocation failed");

        crate::runtime::test_hooks::fail_next_raw_sqe_submit();
        reactor.cancel_op(state);

        unsafe {
            assert!((*state).is_orphaned());
            assert!((*state).is_cancel_pending());
        }
        assert_eq!(reactor.pending_cancels.len(), 1);

        reactor.retry_pending_cancels();

        assert!(
            reactor.has_queued_sqes(),
            "cancel retry should queue an SQE for the next reactor flush"
        );
        assert_eq!(reactor.pending_cancels.len(), 0);
        unsafe {
            assert!(!(*state).is_cancel_pending());
        }

        reactor.free_op(state);
    }

    #[cfg(any(debug_assertions, feature = "test-support"))]
    #[test]
    fn failed_cancel_retry_uses_one_fifo_snapshot_per_pass() {
        let mut reactor =
            Reactor::new_with_config(ReactorConfig { ring_entries: 8 }).expect("reactor failed");
        reactor.init();
        let states = [reactor.alloc_op(), reactor.alloc_op(), reactor.alloc_op()];
        assert!(states.iter().all(|state| !state.is_null()));

        for &state in &states {
            crate::runtime::test_hooks::fail_next_raw_sqe_submit();
            reactor.cancel_op(state);
        }
        for _ in &states {
            crate::runtime::test_hooks::fail_next_raw_sqe_submit();
        }

        reactor.retry_pending_cancels();

        assert_eq!(reactor.pending_cancels.len(), states.len());
        assert_eq!(reactor.pending_cancels.head, states[0]);
        assert_eq!(reactor.pending_cancels.tail, states[2]);
        unsafe {
            assert_eq!((*states[0]).cancel_next, states[1]);
            assert_eq!((*states[1]).cancel_prev(), states[0]);
            assert_eq!((*states[1]).cancel_next, states[2]);
            assert_eq!((*states[2]).cancel_prev(), states[1]);
        }
        assert!(
            !reactor.has_queued_sqes(),
            "failed retry pass should not have queued an SQE"
        );

        reactor.retry_pending_cancels();
        assert!(reactor.pending_cancels.is_empty());
        assert!(
            reactor.has_queued_sqes(),
            "successful retries should queue cancel SQEs"
        );

        for state in states.into_iter().rev() {
            reactor.free_op(state);
        }
    }

    #[cfg(any(debug_assertions, feature = "test-support"))]
    #[test]
    fn shutdown_prepare_preserves_links_for_already_queued_orphans() {
        let mut reactor =
            Reactor::new_with_config(ReactorConfig { ring_entries: 8 }).expect("reactor failed");
        reactor.init();
        let states = [reactor.alloc_op(), reactor.alloc_op()];
        assert!(states.iter().all(|state| !state.is_null()));

        for &state in &states {
            crate::runtime::test_hooks::fail_next_raw_sqe_submit();
            reactor.cancel_op(state);
        }

        let mut first_panic = None;
        unsafe {
            Reactor::prepare_shutdown_unchecked(std::ptr::addr_of_mut!(reactor), &mut first_panic);
        }
        assert!(first_panic.is_none());

        assert_eq!(reactor.pending_cancels.len(), states.len());
        assert_eq!(reactor.pending_cancels.head, states[0]);
        assert_eq!(reactor.pending_cancels.tail, states[1]);
        unsafe {
            assert_eq!((*states[0]).cancel_next, states[1]);
            assert_eq!((*states[1]).cancel_prev(), states[0]);
        }

        reactor.free_op(states[1]);
        reactor.free_op(states[0]);
    }

    #[cfg(any(debug_assertions, feature = "test-support"))]
    #[test]
    fn shutdown_fallback_abandons_unretired_states_and_payload_storage() {
        struct DropTrackedInlinePayload {
            drops: Rc<Cell<usize>>,
            _bytes: [u8; 8],
        }

        impl Drop for DropTrackedInlinePayload {
            fn drop(&mut self) {
                self.drops.set(self.drops.get() + 1);
            }
        }

        #[repr(align(128))]
        struct DropTrackedHeapPayload {
            drops: Rc<Cell<usize>>,
            _bytes: [u8; 8],
        }

        impl Drop for DropTrackedHeapPayload {
            fn drop(&mut self) {
                self.drops.set(self.drops.get() + 1);
            }
        }

        struct DropTrackedScratchPayload {
            drops: Rc<Cell<usize>>,
            _scratch: RetainedIovecScratch,
        }

        impl Drop for DropTrackedScratchPayload {
            fn drop(&mut self) {
                self.drops.set(self.drops.get() + 1);
            }
        }

        let mut reactor =
            Reactor::new_with_config(ReactorConfig { ring_entries: 8 }).expect("reactor failed");
        reactor.init();

        let abandoned_drops = Rc::new(Cell::new(0));
        let heap_drops = Rc::new(Cell::new(0));
        let scratch_drops = Rc::new(Cell::new(0));
        let orphaned = reactor.alloc_op();
        let attached = reactor.alloc_op();
        let heap_fallback = reactor.alloc_op();
        let pooled_scratch = reactor.alloc_op();
        let build_aborted = reactor.alloc_op();
        assert!(!orphaned.is_null(), "orphaned op allocation failed");
        assert!(!attached.is_null(), "attached op allocation failed");
        assert!(!heap_fallback.is_null(), "heap op allocation failed");
        assert!(!pooled_scratch.is_null(), "scratch op allocation failed");
        assert!(!build_aborted.is_null(), "aborted op allocation failed");

        let orphaned_payload = reactor.alloc_retained_payload(DropTrackedInlinePayload {
            drops: Rc::clone(&abandoned_drops),
            _bytes: [0; 8],
        });
        let orphaned_payload_ptr = orphaned_payload.as_ptr();
        let attached_payload = reactor.alloc_retained_payload(DropTrackedInlinePayload {
            drops: Rc::clone(&abandoned_drops),
            _bytes: [0; 8],
        });
        let attached_payload_ptr = attached_payload.as_ptr();
        let heap_payload = reactor.alloc_retained_payload(DropTrackedHeapPayload {
            drops: Rc::clone(&heap_drops),
            _bytes: [0; 8],
        });
        let scratch = reactor
            .alloc_iovec_scratch(64)
            .expect("pooled iovec scratch allocation failed");
        let scratch_payload = reactor.alloc_retained_payload(DropTrackedScratchPayload {
            drops: Rc::clone(&scratch_drops),
            _scratch: scratch,
        });
        unsafe {
            (*orphaned).attach_retained_payload(orphaned_payload);
            (*orphaned).set_orphaned();
            (*attached).attach_retained_payload(attached_payload);
            (*heap_fallback).attach_retained_payload(heap_payload);
            (*heap_fallback).set_orphaned();
            (*pooled_scratch).attach_retained_payload(scratch_payload);
            (*pooled_scratch).set_orphaned();
            (*build_aborted).set_build_aborted();
        }

        let mut runtime_state = runtime_state_with_inflight(4);
        let mut ready_queue = crate::utils::list::intrusive::dlist::DList::<
            crate::runtime::task::TaskHeader,
        >::new_uninit();
        ready_queue.init();
        crate::runtime::test_hooks::force_next_reactor_shutdown_fallback();

        unsafe {
            Reactor::shutdown_unchecked(
                std::ptr::addr_of_mut!(reactor),
                &mut runtime_state,
                &mut ready_queue,
            );
        }

        assert_eq!(
            crate::runtime::test_hooks::reactor_shutdown_fallbacks_remaining(),
            0,
            "forced reactor fallback was not consumed"
        );
        assert!(reactor.storage_abandoned);
        assert!(reactor.ring.is_none());
        assert_eq!(runtime_state.inflight_ops, 0);
        assert_eq!(reactor.live_registry.len(), 5);
        assert!(reactor.pending_cancels.is_empty());
        unsafe {
            assert!((*orphaned).is_ring_abandoned());
            assert!(!(*orphaned).is_completed());
            assert!((*attached).is_ring_abandoned());
            assert!(!(*attached).is_completed());
            assert!((*attached).is_runtime_shutdown());
            assert!((*attached).waiter.is_null());
            assert!((*heap_fallback).is_ring_abandoned());
            assert!(!(*heap_fallback).is_completed());
            assert!((*pooled_scratch).is_ring_abandoned());
            assert!(!(*pooled_scratch).is_completed());
            assert!((*build_aborted).is_build_aborted());
            assert!(!(*build_aborted).is_ring_abandoned());
            assert!(!(*build_aborted).is_runtime_shutdown());
        }
        assert_eq!(
            abandoned_drops.get(),
            0,
            "ring-abandoned payload value was dropped"
        );

        #[cfg(any(debug_assertions, feature = "test-support"))]
        {
            let stats = reactor.retained_payload_stats();
            assert_eq!(stats.pooled_frees, 0);
            assert_eq!(stats.pooled_reuses, 0);
            assert_eq!(stats.heap_fallbacks, 1);
            assert_eq!(stats.heap_frees, 0);
            assert_eq!(stats.writev_scratch_pooled_allocs, 1);
            assert_eq!(stats.writev_scratch_pooled_frees, 0);
        }
        assert_eq!(heap_drops.get(), 0);
        assert_eq!(scratch_drops.get(), 0);

        let replacement_drops = Rc::new(Cell::new(0));
        let replacement_payload = reactor.alloc_retained_payload(DropTrackedInlinePayload {
            drops: Rc::clone(&replacement_drops),
            _bytes: [0; 8],
        });
        assert_ne!(replacement_payload.as_ptr(), orphaned_payload_ptr);
        assert_ne!(replacement_payload.as_ptr(), attached_payload_ptr);
        #[cfg(any(debug_assertions, feature = "test-support"))]
        assert_eq!(reactor.retained_payload_stats().pooled_reuses, 0);
        unsafe {
            replacement_payload.drop_and_free(&mut reactor.retained_pool);
        }
        assert_eq!(replacement_drops.get(), 1);

        reactor.free_op(build_aborted);
        assert_eq!(reactor.live_registry.len(), 4);
        let replacement_state = reactor.alloc_op();
        assert!(
            !replacement_state.is_null(),
            "replacement op allocation failed"
        );
        assert_eq!(
            replacement_state, build_aborted,
            "never-submitted state was not reusable after fallback"
        );
        assert_ne!(replacement_state, orphaned);
        assert_ne!(replacement_state, attached);
        assert_ne!(replacement_state, heap_fallback);
        assert_ne!(replacement_state, pooled_scratch);
        reactor.free_op(replacement_state);

        drop(reactor);
        assert_eq!(
            abandoned_drops.get(),
            0,
            "reactor drop released ring-abandoned payload storage"
        );
        assert_eq!(heap_drops.get(), 0);
        assert_eq!(scratch_drops.get(), 0);
        assert_eq!(replacement_drops.get(), 1);
    }

    #[cfg(any(debug_assertions, feature = "test-support"))]
    #[test]
    fn target_retirement_unlinks_pending_cancel_head_middle_and_tail() {
        let mut reactor =
            Reactor::new_with_config(ReactorConfig { ring_entries: 8 }).expect("reactor failed");
        reactor.init();
        let states = [reactor.alloc_op(), reactor.alloc_op(), reactor.alloc_op()];
        assert!(states.iter().all(|state| !state.is_null()));

        for &state in &states {
            crate::runtime::test_hooks::fail_next_raw_sqe_submit();
            reactor.cancel_op(state);
        }
        assert_eq!(reactor.pending_cancels.len(), 3);

        reactor.free_op(states[1]);
        assert_eq!(reactor.pending_cancels.len(), 2);
        assert_eq!(reactor.pending_cancels.head, states[0]);
        assert_eq!(reactor.pending_cancels.tail, states[2]);
        unsafe {
            assert_eq!((*states[0]).cancel_next, states[2]);
            assert_eq!((*states[2]).cancel_prev(), states[0]);
        }

        reactor.free_op(states[2]);
        assert_eq!(reactor.pending_cancels.len(), 1);
        assert_eq!(reactor.pending_cancels.head, states[0]);
        assert_eq!(reactor.pending_cancels.tail, states[0]);

        reactor.free_op(states[0]);
        assert!(reactor.pending_cancels.is_empty());
        assert!(reactor.pending_cancels.head.is_null());
        assert!(reactor.pending_cancels.tail.is_null());
    }

    #[cfg(any(debug_assertions, feature = "test-support"))]
    #[test]
    fn failed_cancel_retry_keeps_reactor_busy() {
        let mut reactor =
            Reactor::new_with_config(ReactorConfig { ring_entries: 8 }).expect("reactor failed");
        reactor.init();
        let state = reactor.alloc_op();
        assert!(!state.is_null(), "op allocation failed");

        crate::runtime::test_hooks::fail_next_raw_sqe_submit();
        reactor.cancel_op(state);
        assert_eq!(reactor.pending_cancels.len(), 1);

        crate::runtime::test_hooks::fail_next_raw_sqe_submit();
        let status = reactor.flush_sqes().expect("cancel retry flush failed");

        assert_eq!(status, ReactorSubmitStatus::Busy);
        assert_eq!(reactor.pending_cancels.len(), 1);
        unsafe {
            assert!((*state).is_cancel_pending());
        }

        reactor.free_op(state);
    }

    #[cfg(any(debug_assertions, feature = "test-support"))]
    #[test]
    fn reactor_setup_rejects_missing_ext_arg_support() {
        crate::runtime::test_hooks::fail_next_reactor_ext_arg_probe();
        let err = match Reactor::new_with_config(ReactorConfig { ring_entries: 8 }) {
            Ok(_) => panic!("reactor setup should fail without EXT_ARG support"),
            Err(err) => err,
        };

        assert_eq!(err.kind(), io::ErrorKind::Unsupported);
        assert!(
            err.to_string().contains("Linux kernel 5.11"),
            "error should name the minimum kernel requirement: {err}"
        );
    }
}
