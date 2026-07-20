//! `io_uring` reactor: SQE submission, CQE completion, and operation lifecycle.

use crate::runtime::executor::{ExecutorOwner, RuntimeState};
use crate::runtime::op::CompletionState;
#[cfg(debug_assertions)]
use crate::runtime::retained::RetainedPayloadPoolStats;
use crate::runtime::retained::{RetainedIovecScratch, RetainedPayload, RetainedPayloadPool};
use crate::runtime::task::release_task;
use crate::utils::memory::provider::BasicMemoryProvider;
use crate::utils::memory::provider_owned_pool::ProviderOwnedPool;
use io_uring::{IoUring, opcode, types};
use std::collections::VecDeque;
use std::io;
use std::mem::ManuallyDrop;
use std::os::fd::{AsRawFd, IntoRawFd, OwnedFd};
use std::time::{Duration, Instant};

/// Default number of submission and completion ring entries requested from
/// `io_uring`.
pub const DEFAULT_RING_ENTRIES: u32 = 256;

/// Completion-state records allocated per slab in the internal op pool.
const OP_POOL_OBJS_PER_SLAB: usize = 256;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ReactorSubmitStatus {
    Ready,
    Busy,
}

#[inline(always)]
fn is_raw_os_error(err: &io::Error, code: libc::c_int) -> bool {
    err.raw_os_error() == Some(code)
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
    #[cfg(debug_assertions)]
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
        runtime_state.stats.cqe_completions += 1;
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
/// must describe this reactor, and the original target CQE must have retired
/// before an attached payload is released.
unsafe fn free_op_fields(
    pending_cancels: &mut PendingCancelQueue,
    retained_pool: &mut RetainedPayloadPool,
    op_pool: &mut ProviderOwnedPool<CompletionState, BasicMemoryProvider>,
    live_registry: &mut Vec<*mut CompletionState>,
    ptr: *mut CompletionState,
) -> io::Result<()> {
    if live_registry.is_empty() {
        return Err(io::Error::other(
            "reactor freed more operations than it allocated",
        ));
    }

    let registry_index = unsafe { (*ptr).registry_index as usize };
    if registry_index >= live_registry.len() || live_registry[registry_index] != ptr {
        return Err(io::Error::other(
            "completion state missing from reactor live registry",
        ));
    }
    let removed = live_registry.swap_remove(registry_index);
    debug_assert_eq!(removed, ptr);
    if registry_index < live_registry.len() {
        let moved = live_registry[registry_index];
        unsafe {
            (*moved).registry_index = registry_index as u32;
        }
    }
    unsafe {
        (*ptr).registry_index = u32::MAX;
    }

    unsafe { pending_cancels.unlink(ptr) };
    unsafe { (*ptr).clear_waiter() };
    unsafe { (*ptr).drop_retained_payload(retained_pool) };
    unsafe { op_pool.free(ptr) };
    Ok(())
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
    retained_pool: RetainedPayloadPool,
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

        Ok(Self {
            ring: Some(ring),
            owner: std::ptr::null(),
            queued_head: 0,
            next_sequence: 0,
            pending_closes,
            max_pending_closes: config.ring_entries as usize,
            pending_cancels: PendingCancelQueue::new(),
            op_pool: ManuallyDrop::new(op_pool),
            max_live_ops: config.ring_entries as usize,
            live_registry,
            retained_pool,
        })
    }

    pub fn init(&mut self) {
        self.op_pool.init();
    }

    pub(crate) fn bind_owner(&mut self, owner: *const ExecutorOwner) {
        self.owner = owner;
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
        #[cfg(debug_assertions)]
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
        }
        ptr
    }

    /// Allocate pointer-stable retained payload storage for an in-flight op.
    #[inline(always)]
    pub(crate) fn alloc_retained_payload<T: 'static>(&mut self, value: T) -> RetainedPayload<T> {
        self.retained_pool.alloc(value)
    }

    /// Allocate retained kernel-facing `iovec` scratch for a vectored I/O op.
    #[inline(always)]
    pub(crate) fn alloc_iovec_scratch(
        &mut self,
        iov_count: usize,
    ) -> io::Result<RetainedIovecScratch> {
        self.retained_pool.alloc_iovec_scratch(iov_count)
    }

    /// Detach a retained payload from a completed operation and return it.
    ///
    /// # Safety
    ///
    /// `ptr` must point to a live completion state that has a retained payload
    /// of exactly type `T`.
    #[inline(always)]
    pub(crate) unsafe fn take_retained_payload<T: 'static>(
        &mut self,
        ptr: *mut CompletionState,
    ) -> T {
        unsafe { (*ptr).take_retained_payload::<T>(&mut self.retained_pool) }
    }

    /// Detach a retained payload from a completed operation, extract selected
    /// data in place, and release only the backing storage.
    ///
    /// # Safety
    ///
    /// `ptr` must point to a live completion state that has a retained payload
    /// of exactly type `T`. `extract` must move or drop every initialized field
    /// in the payload that requires destruction.
    #[inline(always)]
    pub(crate) unsafe fn take_retained_payload_with<T: 'static, R>(
        &mut self,
        ptr: *mut CompletionState,
        extract: impl FnOnce(*mut T) -> R,
    ) -> R {
        unsafe { (*ptr).take_retained_payload_with::<T, R>(&mut self.retained_pool, extract) }
    }

    #[cfg(debug_assertions)]
    pub(crate) fn retained_payload_stats(&self) -> RetainedPayloadPoolStats {
        self.retained_pool.stats()
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

    /// Releases close owners whose SQEs never crossed into the kernel.
    ///
    /// The ring must already be gone (or SQPOLL must remain disabled, as it is
    /// for every FlowIO reactor), so no queued SQE can consume these fds later.
    fn drop_unsubmitted_close_owners(&mut self) {
        self.pending_closes.clear();
        self.queued_head = 0;
        self.next_sequence = 0;
    }

    #[inline(always)]
    fn submit_ring(&mut self) -> io::Result<usize> {
        #[cfg(debug_assertions)]
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
        #[cfg(debug_assertions)]
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
        #[cfg(debug_assertions)]
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
    #[inline(always)]
    fn try_free_op(&mut self, ptr: *mut CompletionState) -> io::Result<()> {
        debug_assert!(!ptr.is_null(), "reactor free_op called with null pointer");
        unsafe {
            free_op_fields(
                &mut self.pending_cancels,
                &mut self.retained_pool,
                &mut self.op_pool,
                &mut self.live_registry,
                ptr,
            )
        }
    }

    #[inline(always)]
    pub fn free_op(&mut self, ptr: *mut CompletionState) {
        if let Err(err) = self.try_free_op(ptr) {
            debug_assert!(false, "reactor free_op failed: {err}");
        }
    }

    /// Mark an in-flight operation as orphaned and submit `ASYNC_CANCEL`.
    /// The `CompletionState` remains owned by the reactor until the CQE path
    /// reclaims it.
    pub fn cancel_op(&mut self, ptr: *mut CompletionState) {
        unsafe { (*ptr).set_orphaned() };
        unsafe { (*ptr).clear_waiter() };

        self.request_cancel(ptr);
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
        #[cfg(debug_assertions)]
        if let Some(err) = crate::runtime::test_hooks::take_raw_sqe_submit_failure() {
            return Err(err);
        }

        let is_full = self.ring_mut()?.submission().is_full();
        if is_full {
            self.submit_ring_for_sqe_capacity()?;
        }

        let Some(ring) = self.ring.as_mut() else {
            return Err(io::Error::new(
                io::ErrorKind::NotConnected,
                "io_uring reactor is shut down",
            ));
        };
        let next_sequence = &mut self.next_sequence;
        let mut sq = ring.submission();
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
        #[cfg(debug_assertions)]
        if let Some(err) = crate::runtime::test_hooks::take_raw_sqe_submit_failure() {
            return Err((err, fd));
        }

        let sqe = opcode::Close::new(types::Fd(fd.as_raw_fd()))
            .build()
            .user_data(user_data);
        let is_full = match self.ring_mut() {
            Ok(ring) => ring.submission().is_full(),
            Err(err) => return Err((err, fd)),
        };
        if is_full && let Err(err) = self.submit_ring_for_sqe_capacity() {
            return Err((err, fd));
        }

        if self.pending_closes.len() >= self.max_pending_closes {
            return Err((io::Error::from(io::ErrorKind::WouldBlock), fd));
        }

        let Some(ring) = self.ring.as_mut() else {
            return Err((
                io::Error::new(io::ErrorKind::NotConnected, "io_uring reactor is shut down"),
                fd,
            ));
        };
        let pending_closes = &mut self.pending_closes;
        let next_sequence = &mut self.next_sequence;
        let sequence = *next_sequence;
        pending_closes.push_back(PendingClose { sequence, fd });
        let mut sq = ring.submission();
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
            let deadline = Instant::now() + initial_timeout;
            loop {
                let now = Instant::now();
                if now >= deadline {
                    return Ok(ReactorSubmitStatus::Ready);
                }
                let remaining = deadline.saturating_duration_since(now);
                let timeout = if remaining.is_zero() {
                    Duration::from_nanos(1)
                } else {
                    remaining
                };
                let timespec = types::Timespec::from(timeout);
                let args = types::SubmitArgs::new().timespec(&timespec);
                match self.submit_with_args(1, &args) {
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
                    Err(err) if is_raw_os_error(&err, libc::EINTR) => continue,
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

    fn prepare_shutdown(&mut self) {
        let mut index = 0usize;
        while index < self.live_registry.len() {
            let state = self.live_registry[index];
            unsafe {
                if (*state).is_orphaned() || (*state).is_detached() {
                    index += 1;
                    continue;
                }

                debug_assert!(!(*state).is_cancel_pending());
                (*state).clear_waiter();
                (*state).set_runtime_shutdown();
                if (*state).is_completed() {
                    (*state).result = -libc::ECANCELED;
                } else {
                    self.request_cancel(state);
                }
            }
            index += 1;
        }
    }

    /// Retires kernel-visible submissions and closes the ring while preserving
    /// completed state still owned by escaped futures.
    pub(crate) fn shutdown(
        &mut self,
        runtime_state: *mut RuntimeState,
        ready_queue: *mut crate::utils::list::intrusive::dlist::DList<
            crate::runtime::task::TaskHeader,
        >,
    ) {
        if self.ring.is_none() {
            return;
        }

        self.prepare_shutdown();
        #[cfg(any(test, feature = "test-support"))]
        let force_fallback = crate::runtime::test_hooks::take_reactor_shutdown_fallback();
        #[cfg(not(any(test, feature = "test-support")))]
        let force_fallback = false;
        let deadline = Instant::now() + Duration::from_secs(1);
        while !force_fallback
            && unsafe { (*runtime_state).inflight_ops > 0 }
            && Instant::now() < deadline
        {
            if self.flush_sqes().is_err() {
                break;
            }
            if self
                .poll_io(usize::MAX, runtime_state, ready_queue)
                .is_err()
            {
                break;
            }
            if unsafe { (*runtime_state).inflight_ops == 0 } {
                break;
            }
            if self
                .wait_for_events(Some(Duration::from_millis(10)))
                .is_err()
            {
                break;
            }
        }

        if unsafe { (*runtime_state).inflight_ops > 0 } {
            // Closing the ring is the bounded fallback that ends all remaining
            // kernel access before retained payloads or state slots are
            // touched. Readiness operations retain their source descriptor,
            // and their CQEs never create a process resource.
            drop(self.ring.take());
            self.drop_unsubmitted_close_owners();
            unsafe {
                (*runtime_state).inflight_ops = 0;
            }

            let mut index = 0usize;
            while index < self.live_registry.len() {
                let state = self.live_registry[index];
                unsafe {
                    self.pending_cancels.unlink(state);
                    if !(*state).is_completed() {
                        (*state).result = -libc::ECANCELED;
                        (*state).cqe_flags = 0;
                        (*state).set_completed();
                    }
                }

                if unsafe { (*state).is_orphaned() || (*state).is_detached() } {
                    self.free_op(state);
                } else {
                    index += 1;
                }
            }
            debug_assert!(self.pending_cancels.is_empty());
            self.pending_cancels = PendingCancelQueue::new();
            return;
        }

        drop(self.ring.take());
        self.drop_unsubmitted_close_owners();
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
    pub fn poll_io(
        &mut self,
        max_completions: usize,
        runtime_state: *mut crate::runtime::executor::RuntimeState,
        ready_queue: *mut crate::utils::list::intrusive::dlist::DList<
            crate::runtime::task::TaskHeader,
        >,
    ) -> io::Result<usize> {
        let ring = self
            .ring
            .as_mut()
            .ok_or_else(|| io::Error::from(io::ErrorKind::BrokenPipe))?
            as *mut IoUring;
        // SAFETY: the completion view borrows only the ring field. This method
        // mutates disjoint reactor bookkeeping fields and never replaces or
        // otherwise accesses the ring until `cq` is dropped.
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
                (*state).result = cqe.result();
                (*state).cqe_flags = cqe.flags();
                (*state).set_completed();

                retire_tracked_completion(&mut *runtime_state)?;

                if (*state).is_runtime_shutdown() {
                    (*state).result = -libc::ECANCELED;
                    self.pending_cancels.unlink(state);
                } else if (*state).is_orphaned() || (*state).is_detached() {
                    // Cancelled or abandoned op — free the pool slot, with no
                    // task wake.
                    free_op_fields(
                        &mut self.pending_cancels,
                        &mut self.retained_pool,
                        &mut self.op_pool,
                        &mut self.live_registry,
                        state,
                    )?;
                } else {
                    let waiter = (*state).take_waiter();
                    if !waiter.is_null() {
                        #[cfg(debug_assertions)]
                        {
                            (*runtime_state).stats.waiter_wakes += 1;
                        }
                        crate::runtime::executor::notify_reactor_waiter_unchecked(
                            waiter,
                            self.owner,
                            ready_queue,
                            runtime_state,
                        );
                        // `take_waiter` transfers one owning reference. Keep it
                        // alive through scheduling, then release it after the
                        // executor has either queued the live task or observed
                        // that it already completed.
                        release_task(waiter);
                    }
                }
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
            self.live_registry.is_empty(),
            "reactor dropped with live completion states"
        );
        unsafe { ManuallyDrop::drop(&mut self.op_pool) };
    }
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
                    (*state).clear_waiter();
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
                reactor.free_op(state);
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
    use super::*;
    use crate::runtime::task::TaskHeader;

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
        let (states, pointers) = queue_states(2, false);
        let second = pointers[1];

        unsafe { (*second).register_waiter(task_ptr) };
        assert_eq!(task.refs.get(), 2);
        unsafe { (*second).clear_waiter() };
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
    fn completion_state_remains_one_cache_line() {
        assert_eq!(std::mem::size_of::<CompletionState>(), 64);
        assert_eq!(std::mem::align_of::<CompletionState>(), 64);
    }
}

#[cfg(all(test, not(miri)))]
mod tests {
    use crate::runtime::fd::{distinctive_closeable_test_fd, raw_fd_is_closed};

    use super::*;
    use std::os::fd::{AsRawFd, FromRawFd, RawFd};

    fn runtime_state_with_inflight(inflight_ops: usize) -> RuntimeState {
        RuntimeState {
            live_tasks: 0,
            inflight_ops,
            wake_epoch: 1,
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

    #[cfg(debug_assertions)]
    fn nop_sqe(user_data: u64) -> io_uring::squeue::Entry {
        opcode::Nop::new().build().user_data(user_data)
    }

    #[test]
    fn cancel_cqes_do_not_consume_poll_budget() {
        assert!(!cqe_consumes_poll_budget(0));
        assert!(cqe_consumes_poll_budget(1));
        assert!(cqe_consumes_poll_budget(u64::MAX));
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

    #[cfg(debug_assertions)]
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

    #[cfg(debug_assertions)]
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

    #[cfg(debug_assertions)]
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
        assert!(
            reactor.has_queued_sqes(),
            "third NOP should remain pending after push"
        );
    }

    #[cfg(debug_assertions)]
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
        assert!(
            reactor.has_queued_sqes(),
            "third NOP should remain pending after transient submit pressure"
        );
    }

    #[cfg(debug_assertions)]
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

    #[cfg(debug_assertions)]
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

    #[cfg(debug_assertions)]
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

    #[cfg(debug_assertions)]
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

    #[cfg(debug_assertions)]
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

    #[cfg(debug_assertions)]
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

    #[cfg(debug_assertions)]
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

    #[cfg(debug_assertions)]
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

        reactor.prepare_shutdown();

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

    #[cfg(debug_assertions)]
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

    #[cfg(debug_assertions)]
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

    #[cfg(debug_assertions)]
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
