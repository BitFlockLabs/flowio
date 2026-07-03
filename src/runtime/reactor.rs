//! `io_uring` reactor: SQE submission, CQE completion, and operation lifecycle.

use crate::runtime::executor::RuntimeState;
use crate::runtime::op::CompletionState;
#[cfg(debug_assertions)]
use crate::runtime::retained::RetainedPayloadPoolStats;
use crate::runtime::retained::{RetainedIovecScratch, RetainedPayload, RetainedPayloadPool};
use crate::utils::memory::pool::Pool;
use crate::utils::memory::provider::BasicMemoryProvider;
use io_uring::{IoUring, opcode, types};
use std::io;
use std::mem::ManuallyDrop;
use std::os::fd::RawFd;
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

#[inline(always)]
fn close_result_fd(fd: RawFd) {
    unsafe {
        libc::close(fd);
    }
}

#[inline(always)]
fn close_orphan_result_fd_if_needed(state: &CompletionState) {
    if state.is_orphaned() && state.closes_result_fd_on_orphan() && state.result >= 0 {
        close_result_fd(state.result as RawFd);
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

fn unlink_pending_cancel(
    pending_cancel_head: &mut *mut CompletionState,
    pending_cancel_tail: &mut *mut CompletionState,
    pending_cancel_len: &mut usize,
    ptr: *mut CompletionState,
) {
    debug_assert!(
        !ptr.is_null(),
        "reactor unlink_pending_cancel called with null pointer"
    );
    unsafe {
        if !(*ptr).is_cancel_pending() {
            return;
        }
    }

    let mut prev: *mut CompletionState = std::ptr::null_mut();
    let mut current = *pending_cancel_head;
    while !current.is_null() {
        let next = unsafe { (*current).cancel_next };
        if current == ptr {
            if prev.is_null() {
                *pending_cancel_head = next;
            } else {
                unsafe { (*prev).cancel_next = next };
            }
            if *pending_cancel_tail == current {
                *pending_cancel_tail = prev;
            }
            debug_assert!(*pending_cancel_len > 0);
            *pending_cancel_len -= 1;
            unsafe {
                (*current).cancel_next = std::ptr::null_mut();
                (*current).clear_cancel_pending();
            }
            if (*pending_cancel_head).is_null() {
                debug_assert!((*pending_cancel_tail).is_null());
                *pending_cancel_tail = std::ptr::null_mut();
            }
            return;
        }
        prev = current;
        current = next;
    }

    debug_assert_eq!(
        current, ptr,
        "cancel-pending flag set but completion state is not queued"
    );
    unsafe {
        (*ptr).cancel_next = std::ptr::null_mut();
        (*ptr).clear_cancel_pending();
    }
}

#[inline(always)]
unsafe fn free_op_fields(
    pending_cancel_head: &mut *mut CompletionState,
    pending_cancel_tail: &mut *mut CompletionState,
    pending_cancel_len: &mut usize,
    retained_pool: &mut RetainedPayloadPool,
    op_pool: &mut Pool<'static, CompletionState, BasicMemoryProvider>,
    live_ops: &mut usize,
    ptr: *mut CompletionState,
) {
    unlink_pending_cancel(
        pending_cancel_head,
        pending_cancel_tail,
        pending_cancel_len,
        ptr,
    );
    unsafe { (*ptr).drop_retained_payload(retained_pool) };
    debug_assert!(
        *live_ops > 0,
        "reactor freed more operations than it allocated"
    );
    *live_ops = live_ops.saturating_sub(1);
    unsafe { op_pool.free(ptr) };
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

#[doc(hidden)]
pub(crate) struct Reactor {
    /// Owned io_uring instance used for submission and completion handling.
    pub(crate) ring: IoUring,
    /// True when SQEs have been queued in userspace but not flushed to the
    /// kernel yet.
    pending: bool,
    /// Orphaned operations whose `ASYNC_CANCEL` SQE could not be submitted.
    pending_cancel_head: *mut CompletionState,
    pending_cancel_tail: *mut CompletionState,
    pending_cancel_len: usize,
    /// Pool of reusable completion-state records for in-flight operations.
    op_pool: ManuallyDrop<Pool<'static, CompletionState, BasicMemoryProvider>>,
    /// Maximum number of live completion-state records.
    max_live_ops: usize,
    /// Number of completion-state records currently checked out.
    live_ops: usize,
    /// Pool of pointer-stable retained payload blocks referenced by in-flight
    /// operations after their owning futures are dropped.
    retained_pool: RetainedPayloadPool,
    /// Stable memory provider backing `op_pool`.
    _op_pool_provider: Box<BasicMemoryProvider>,
    /// Set after `op_pool.init()` so drop knows whether the pool is live.
    initialized: bool,
}

impl Reactor {
    pub fn new_with_config(config: ReactorConfig) -> io::Result<Self> {
        let ring = IoUring::new(config.ring_entries)?;
        validate_required_ring_features(&ring)?;

        let mut provider = Box::new(BasicMemoryProvider::new());
        let provider_ptr = &mut *provider as *mut BasicMemoryProvider;
        let op_pool = ManuallyDrop::new(
            Pool::new_uninit(unsafe { &mut *provider_ptr }, OP_POOL_OBJS_PER_SLAB)
                .map_err(|_| io::Error::from(io::ErrorKind::InvalidInput))?,
        );

        Ok(Self {
            ring,
            pending: false,
            pending_cancel_head: std::ptr::null_mut(),
            pending_cancel_tail: std::ptr::null_mut(),
            pending_cancel_len: 0,
            op_pool,
            max_live_ops: config.ring_entries as usize,
            live_ops: 0,
            retained_pool: RetainedPayloadPool::new()?,
            _op_pool_provider: provider,
            initialized: false,
        })
    }

    pub fn init(&mut self) {
        self.op_pool.init();
        self.initialized = true;
    }

    /// Allocate a fresh `CompletionState` for one SQE submission.
    #[inline(always)]
    pub fn alloc_op(&mut self) -> *mut CompletionState {
        #[cfg(debug_assertions)]
        if crate::runtime::test_hooks::take_op_alloc_failure() {
            return std::ptr::null_mut();
        }

        if self.live_ops >= self.max_live_ops {
            return std::ptr::null_mut();
        }

        let ptr = unsafe { self.op_pool.alloc(()).unwrap_or(std::ptr::null_mut()) };
        if !ptr.is_null() {
            self.live_ops += 1;
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
        !self.pending_cancel_head.is_null()
    }

    fn queue_pending_cancel(&mut self, ptr: *mut CompletionState) {
        debug_assert!(
            !ptr.is_null(),
            "reactor queue_pending_cancel called with null pointer"
        );
        unsafe {
            if (*ptr).is_cancel_pending() || (*ptr).is_completed() {
                return;
            }
            debug_assert!(
                (*ptr).is_orphaned(),
                "only orphaned ops can be queued for cancel retry"
            );
            (*ptr).set_cancel_pending();
            (*ptr).cancel_next = std::ptr::null_mut();

            if self.pending_cancel_tail.is_null() {
                debug_assert!(self.pending_cancel_head.is_null());
                self.pending_cancel_head = ptr;
            } else {
                (*self.pending_cancel_tail).cancel_next = ptr;
            }
            self.pending_cancel_tail = ptr;
            self.pending_cancel_len += 1;
        }
    }

    fn retry_pending_cancels(&mut self) {
        let mut current = self.pending_cancel_head;
        self.pending_cancel_head = std::ptr::null_mut();
        self.pending_cancel_tail = std::ptr::null_mut();
        self.pending_cancel_len = 0;

        while !current.is_null() {
            let next = unsafe { (*current).cancel_next };
            unsafe {
                (*current).cancel_next = std::ptr::null_mut();
                (*current).clear_cancel_pending();
            }

            let should_retry = unsafe { (*current).is_orphaned() && !(*current).is_completed() };
            if should_retry && self.submit_cancel_sqe(current).is_err() {
                self.queue_pending_cancel(current);
            }
            current = next;
        }
    }

    /// Return a retired `CompletionState` to the pool.
    ///
    /// This releases any retained payload before recycling the state slot. The
    /// retained payload, if present, owns memory referenced by the original
    /// SQE and must only be released after that original CQE has been observed.
    #[inline(always)]
    pub fn free_op(&mut self, ptr: *mut CompletionState) {
        debug_assert!(!ptr.is_null(), "reactor free_op called with null pointer");
        unsafe {
            free_op_fields(
                &mut self.pending_cancel_head,
                &mut self.pending_cancel_tail,
                &mut self.pending_cancel_len,
                &mut self.retained_pool,
                &mut self.op_pool,
                &mut self.live_ops,
                ptr,
            )
        };
    }

    /// Mark an in-flight operation as orphaned and submit `ASYNC_CANCEL`.
    /// The `CompletionState` remains owned by the reactor until the CQE path
    /// reclaims it.
    pub fn cancel_op(&mut self, ptr: *mut CompletionState) {
        unsafe { (*ptr).set_orphaned() };
        unsafe { (*ptr).clear_waiter() };

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

        let mut sq = self.ring.submission();
        if sq.is_full() {
            drop(sq);
            self.ring.submit()?;
            self.pending = false;
            sq = self.ring.submission();
        }
        unsafe {
            if sq.push(&sqe).is_err() {
                return Err(io::Error::from(io::ErrorKind::WouldBlock));
            }
        }
        drop(sq);
        self.pending = true;
        Ok(())
    }

    #[inline(always)]
    /// Flushes any queued SQEs to the kernel submission queue.
    pub fn flush_sqes(&mut self) -> io::Result<ReactorSubmitStatus> {
        self.retry_pending_cancels();
        if self.pending {
            loop {
                match self.ring.submit() {
                    Ok(_) => break,
                    Err(err) if is_raw_os_error(&err, libc::EINTR) => continue,
                    Err(err) if is_raw_os_error(&err, libc::EBUSY) => {
                        return Ok(ReactorSubmitStatus::Busy);
                    }
                    Err(err) => return Err(err),
                }
            }
            self.pending = false;
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
        self.retry_pending_cancels();
        if self.has_pending_cancels() {
            return Ok(ReactorSubmitStatus::Busy);
        }
        self.pending = false;
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
                match self.ring.submitter().submit_with_args(1, &args) {
                    Ok(_) => return Ok(ReactorSubmitStatus::Ready),
                    Err(err) if is_raw_os_error(&err, libc::ETIME) => {
                        return Ok(ReactorSubmitStatus::Ready);
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
                match self.ring.submit_and_wait(1) {
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

    /// Drains completed CQEs, updates `CompletionState`, and wakes waiting
    /// tasks as needed.
    pub fn poll_io(
        &mut self,
        max_completions: usize,
        runtime_state: *mut crate::runtime::executor::RuntimeState,
        ready_queue: *mut crate::utils::list::intrusive::dlist::DList<
            crate::runtime::task::TaskHeader,
        >,
    ) -> io::Result<usize> {
        let mut cq = self.ring.completion();

        let mut seen = 0usize;
        for cqe in &mut cq {
            let user_data = cqe.user_data();
            if !cqe_consumes_poll_budget(user_data) {
                // Cancel SQE completion — silently skip. Retained payloads are
                // released only when the original target CQE is observed.
                continue;
            }
            if seen >= max_completions {
                break;
            }

            let state = user_data as *mut CompletionState;
            unsafe {
                (*state).result = cqe.result();
                (*state).cqe_flags = cqe.flags();
                (*state).set_completed();

                retire_tracked_completion(&mut *runtime_state)?;

                if (*state).is_orphaned() || (*state).is_detached() {
                    // Cancelled/abandoned or detached op — free the pool slot,
                    // no task wake.
                    close_orphan_result_fd_if_needed(&*state);
                    free_op_fields(
                        &mut self.pending_cancel_head,
                        &mut self.pending_cancel_tail,
                        &mut self.pending_cancel_len,
                        &mut self.retained_pool,
                        &mut self.op_pool,
                        &mut self.live_ops,
                        state,
                    );
                } else {
                    let waiter = (*state).take_waiter();
                    if !waiter.is_null() {
                        #[cfg(debug_assertions)]
                        {
                            (*runtime_state).stats.waiter_wakes += 1;
                        }
                        crate::runtime::executor::notify_task_into_list_unchecked(
                            waiter,
                            ready_queue,
                            runtime_state,
                        );
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
        if self.initialized {
            unsafe { ManuallyDrop::drop(&mut self.op_pool) };
        }
    }
}

#[cfg(all(test, not(miri)))]
mod tests {
    use crate::runtime::fd::{distinctive_closeable_test_fd, raw_fd_is_closed};

    use super::*;

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

    #[test]
    fn orphan_result_fd_helper_closes_positive_accept_result() {
        let fd = distinctive_closeable_test_fd().expect("socketpair fd failed");
        let mut state = CompletionState::empty();
        state.result = fd;
        state.set_orphaned();
        state.set_close_result_fd_on_orphan();

        close_orphan_result_fd_if_needed(&state);

        assert!(
            raw_fd_is_closed(fd),
            "orphaned accept result fd stayed open"
        );
    }

    #[test]
    fn orphan_result_fd_helper_ignores_negative_result() {
        let fd = distinctive_closeable_test_fd().expect("socketpair fd failed");
        let mut state = CompletionState::empty();
        state.result = -libc::ECANCELED;
        state.set_orphaned();
        state.set_close_result_fd_on_orphan();

        close_orphan_result_fd_if_needed(&state);

        assert!(
            !raw_fd_is_closed(fd),
            "negative CQE result should not close unrelated fd"
        );
        close_fd_if_open(fd);
    }

    #[test]
    fn orphan_result_fd_helper_requires_orphan_and_close_flag() {
        let fd_without_orphan = distinctive_closeable_test_fd().expect("socketpair fd failed");
        let mut without_orphan = CompletionState::empty();
        without_orphan.result = fd_without_orphan;
        without_orphan.set_close_result_fd_on_orphan();
        close_orphan_result_fd_if_needed(&without_orphan);
        assert!(!raw_fd_is_closed(fd_without_orphan));
        close_fd_if_open(fd_without_orphan);

        let fd_without_flag = distinctive_closeable_test_fd().expect("socketpair fd failed");
        let mut without_flag = CompletionState::empty();
        without_flag.result = fd_without_flag;
        without_flag.set_orphaned();
        close_orphan_result_fd_if_needed(&without_flag);
        assert!(!raw_fd_is_closed(fd_without_flag));
        close_fd_if_open(fd_without_flag);
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
        assert_eq!(reactor.pending_cancel_len, 1);

        reactor.retry_pending_cancels();

        assert!(
            reactor.pending,
            "cancel retry should queue an SQE for the next reactor flush"
        );
        assert_eq!(reactor.pending_cancel_len, 0);
        unsafe {
            assert!(!(*state).is_cancel_pending());
        }

        reactor.free_op(state);
    }

    #[cfg(debug_assertions)]
    #[test]
    fn freeing_original_cqe_unlinks_pending_cancel_retry() {
        let mut reactor =
            Reactor::new_with_config(ReactorConfig { ring_entries: 8 }).expect("reactor failed");
        reactor.init();
        let state = reactor.alloc_op();
        assert!(!state.is_null(), "op allocation failed");

        crate::runtime::test_hooks::fail_next_raw_sqe_submit();
        reactor.cancel_op(state);
        assert_eq!(reactor.pending_cancel_len, 1);

        reactor.free_op(state);

        assert_eq!(reactor.pending_cancel_len, 0);
        assert!(reactor.pending_cancel_head.is_null());
        assert!(reactor.pending_cancel_tail.is_null());
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
        assert_eq!(reactor.pending_cancel_len, 1);

        crate::runtime::test_hooks::fail_next_raw_sqe_submit();
        let status = reactor.flush_sqes().expect("cancel retry flush failed");

        assert_eq!(status, ReactorSubmitStatus::Busy);
        assert_eq!(reactor.pending_cancel_len, 1);
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
