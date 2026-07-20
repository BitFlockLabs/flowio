//! Executor and scheduler entry points for the runtime.
//!
//! The executor owns the reactor, task pool, ready queue, and timer runtime
//! for one thread. It is intended to be long-lived: construct it once, then
//! run application tasks inside it.
//!
//! The runtime is single-threaded: a task's [`Waker`] is only cloned, woken, or
//! dropped on the executor thread that owns the task. The future and all runtime
//! state stay on that thread.
//!
//! # Fast-Path Guidance
//!
//! Preferred on the fast path:
//! - Use a single executor instance to drive many tasks and I/O completions
//!   over time.
//! - Use [`Executor::spawn`] from inside [`Executor::run`] to add concurrent
//!   work without rebuilding runtime state.
//! - Use [`Executor::try_spawn`] when the caller must keep
//!   ownership of the submitted future if the scheduler cannot accept it, such
//!   as work carrying a response or cleanup obligation.
//! - Account for task storage explicitly: each task must fit a fixed slot, but
//!   the task pool acquires 1024-slot slabs on demand and currently has no
//!   user-configurable total slot cap.
//!
//! Avoid on the fast path:
//! - Do not construct a fresh [`Executor`] or enter a new [`Executor::run`]
//!   boundary around each operation. Spawn work inside the existing run.
//! - Do not use [`Executor::spawn`] when admission failure must preserve the
//!   submitted future: its `io::Error` conversion drops that future. Use
//!   [`Executor::try_spawn`] and handle [`TrySpawnError`] instead.
//!
//! # Example
//! ```no_run
//! use flowio::runtime::executor::Executor;
//!
//! let mut executor = Executor::new()?;
//! executor.run(async {})?;
//! # Ok::<(), std::io::Error>(())
//! ```

use crate::runtime::op::CompletionState;
use crate::runtime::reactor::{Reactor, ReactorConfig, ReactorSubmitStatus};
use crate::runtime::task::{
    Task, TaskHeader, TaskVTable, cached_waker_ref, init_cached_waker, release_task,
    task_ptr_from_waker,
};
use crate::runtime::timer::TimerRuntime;
use crate::utils::list::intrusive::dlist::DList;
use crate::utils::memory::provider::MemoryProvider;
use crate::utils::memory::provider_owned_pool::{ProviderOwnedPool, ProviderOwnedPoolControl};
use io_uring::squeue;
use std::alloc::{Layout, alloc};
use std::cell::{Cell, UnsafeCell};
use std::future::Future;
use std::io;
use std::io::ErrorKind;
use std::mem::{align_of, size_of};
use std::os::fd::OwnedFd;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::mpsc::{Receiver, SyncSender, TrySendError, sync_channel};
use std::task::{Context, Poll, Waker};
use std::thread::JoinHandle as ThreadJoinHandle;

/// Default per-phase cap for one executor loop pass.
///
/// The executor applies this limit separately to ready-task polling, CQE
/// draining, and timer processing so no single queue type monopolizes a pass.
pub const DEFAULT_PROCESS_QUOTA: usize = 128;
/// Bytes reserved for each fixed executor task slot.
const TASK_POOL_SIZE: usize = 4096;
/// Maximum alignment currently guaranteed for payloads stored in `Task::data`.
const TASK_DATA_ALIGN: usize = align_of::<TaskHeader>();
/// Number of task slots allocated per task-pool slab page.
const TASKS_PER_SLAB: usize = 1024;
/// The owner pointer and all-task link consume prior padding at the fixed
/// payload boundary, so 64-bit task slots retain their pre-slice geometry.
#[cfg(target_pointer_width = "64")]
const _: [(); 4224] = [(); size_of::<Task<TASK_POOL_SIZE>>()];

#[allow(unused_macros)]
macro_rules! define_runtime_stats {
    ($vis:vis) => {
        /// Development-only counters for scheduler and allocation regression
        /// tests and benchmark probes.
        ///
        /// The type is exposed outside the crate only by `test-support`, and
        /// its counters exist only when debug assertions are enabled. It is not
        /// a supported production observability API.
        ///
        /// # Example
        /// ```
        /// # #[cfg(feature = "test-support")]
        /// # {
        /// use flowio::runtime::executor::RuntimeStats;
        ///
        /// let stats = RuntimeStats::default();
        /// assert_eq!(stats.task_polls, 0);
        /// # }
        /// ```
        #[derive(Clone, Copy, Default)]
        $vis struct RuntimeStats {
            /// Number of task slab pages requested from the memory provider.
            #[cfg(debug_assertions)]
            pub task_slab_allocs: usize,
            /// Number of task slab pages returned to the memory provider.
            /// Runtime snapshots normally stay at zero; task slabs are freed
            /// during executor teardown.
            #[cfg(debug_assertions)]
            pub task_slab_frees: usize,
            /// Number of task slots allocated from the task pool.
            #[cfg(debug_assertions)]
            pub task_allocs: usize,
            /// Number of task slots freed back to the task pool.
            #[cfg(debug_assertions)]
            pub task_frees: usize,
            /// Total number of times tasks were polled by the executor.
            #[cfg(debug_assertions)]
            pub task_polls: usize,
            /// Number of task ready-queue enqueues from wake reschedules.
            /// Initial spawn enqueues are not counted.
            #[cfg(debug_assertions)]
            pub task_schedules: usize,
            /// Number of SQEs pushed to the io_uring submission queue.
            #[cfg(debug_assertions)]
            pub sqe_submits: usize,
            /// Number of CQEs drained from the io_uring completion queue.
            #[cfg(debug_assertions)]
            pub cqe_completions: usize,
            /// Number of times a waiting task was woken by a retired CQE or an
            /// expired timer.
            #[cfg(debug_assertions)]
            pub waiter_wakes: usize,
            /// Readiness completions whose owner-thread `accept4` found no
            /// queued connection or association and rearmed the one-shot poll.
            #[cfg(debug_assertions)]
            pub accept_readiness_rearms: usize,
            /// Number of `clock_gettime` calls for timer tick computation.
            #[cfg(debug_assertions)]
            pub timer_now_tick_calls: usize,
            /// Number of timer entries that expired and fired.
            #[cfg(debug_assertions)]
            pub timer_expired: usize,
            /// Retained operation payload allocations served by the private
            /// pool.
            #[cfg(debug_assertions)]
            pub retained_pooled_allocs: usize,
            /// Retained operation payload allocations served from a returned
            /// block.
            #[cfg(debug_assertions)]
            pub retained_pooled_reuses: usize,
            /// Retained operation payload blocks returned to size-class free
            /// lists.
            #[cfg(debug_assertions)]
            pub retained_pooled_frees: usize,
            /// Retained operation payload slab pages requested by the private
            /// pool.
            #[cfg(debug_assertions)]
            pub retained_slab_allocs: usize,
            /// Retained operation payloads that used the documented heap
            /// fallback.
            #[cfg(debug_assertions)]
            pub retained_heap_fallbacks: usize,
            /// Retained operation payload heap fallback blocks released.
            #[cfg(debug_assertions)]
            pub retained_heap_frees: usize,
            /// Retained vectored I/O scratch requests served by inline
            /// storage.
            #[cfg(debug_assertions)]
            pub writev_scratch_inline_allocs: usize,
            /// Retained vectored I/O scratch requests served by pooled sidecar
            /// storage.
            #[cfg(debug_assertions)]
            pub writev_scratch_pooled_allocs: usize,
            /// Retained vectored I/O scratch requests served from a returned
            /// block.
            #[cfg(debug_assertions)]
            pub writev_scratch_pooled_reuses: usize,
            /// Retained vectored I/O scratch sidecar blocks returned to
            /// size-class free lists.
            #[cfg(debug_assertions)]
            pub writev_scratch_pooled_frees: usize,
            /// Retained vectored I/O scratch slab pages requested by the
            /// sidecar pool.
            #[cfg(debug_assertions)]
            pub writev_scratch_slab_allocs: usize,
            /// Vectored I/O requests rejected for exceeding the iovec limit.
            #[cfg(debug_assertions)]
            pub writev_scratch_oversize_rejections: usize,
            /// Vectored I/O scratch sidecar allocation failures.
            #[cfg(debug_assertions)]
            pub writev_scratch_alloc_failures: usize,
            /// Partial vectored-write completions that advanced retained iovec
            /// metadata before resubmitting the remaining write window.
            #[cfg(debug_assertions)]
            pub writev_partial_continuations: usize,
            /// Descriptor owners transferred to the executor's bounded close
            /// worker.
            #[cfg(debug_assertions)]
            pub close_worker_admissions: usize,
            /// Plain socket closes queued into the reactor with ownership
            /// retained until kernel submission consumes their exact prefix.
            #[cfg(debug_assertions)]
            pub close_ring_submissions: usize,
            /// Nonpositive-linger closes performed directly because the
            /// reactor could not accept a close SQE.
            #[cfg(debug_assertions)]
            pub close_ring_fallbacks: usize,
            /// Terminal descriptors whose uncertain provenance required one
            /// `SO_LINGER` query.
            #[cfg(debug_assertions)]
            pub close_linger_queries: usize,
            /// Descriptor owners closed directly on unsupported/non-socket,
            /// ring-rejection, or worker-admission fallback paths.
            #[cfg(debug_assertions)]
            pub close_direct_closes: usize,
            /// Descriptor linger states that could not be classified before
            /// conservative worker admission.
            #[cfg(debug_assertions)]
            pub close_linger_classification_failures: usize,
            /// Descriptor owners rejected because the bounded close-worker
            /// queue was full.
            #[cfg(debug_assertions)]
            pub close_worker_full_fallbacks: usize,
            /// Descriptor owners rejected because the close worker was
            /// disconnected during teardown or after worker failure.
            #[cfg(debug_assertions)]
            pub close_worker_disconnected_fallbacks: usize,
            /// Positive `SO_LINGER` waits waived before an overload fallback
            /// close.
            #[cfg(debug_assertions)]
            pub close_linger_waivers: usize,
            /// Failed attempts to disable positive `SO_LINGER` before an
            /// overload fallback close.
            #[cfg(debug_assertions)]
            pub close_linger_waiver_failures: usize,
        }
    };
}

#[cfg(any(test, feature = "test-support"))]
define_runtime_stats!(pub);

#[cfg(all(debug_assertions, not(any(test, feature = "test-support"))))]
define_runtime_stats!(pub(crate));

struct ExecutorTaskMemProvider {
    /// Minimum alignment guaranteed for task slab allocations.
    alignment: usize,
    #[cfg(any(debug_assertions, test))]
    /// Number of task slab allocations requested since the last debug reset.
    request_count: usize,
    #[cfg(debug_assertions)]
    /// Number of task slab frees issued since the last debug reset.
    free_count: usize,
    #[cfg(all(test, not(miri)))]
    /// Optional task slab request cap used by executor allocation-failure tests.
    max_request_count: Option<usize>,
}

impl ExecutorTaskMemProvider {
    fn new() -> Self {
        Self {
            alignment: std::mem::align_of::<usize>(),
            #[cfg(any(debug_assertions, test))]
            request_count: 0,
            #[cfg(debug_assertions)]
            free_count: 0,
            #[cfg(all(test, not(miri)))]
            max_request_count: None,
        }
    }

    #[inline(always)]
    fn note_request(&mut self) {
        #[cfg(any(debug_assertions, test))]
        {
            self.request_count += 1;
        }
    }

    #[inline(always)]
    fn note_free(&mut self) {
        #[cfg(debug_assertions)]
        {
            self.free_count += 1;
        }
    }

    #[inline(always)]
    fn reset_debug_counts(&mut self) {
        #[cfg(any(debug_assertions, test))]
        {
            self.request_count = 0;
        }
        #[cfg(debug_assertions)]
        {
            self.free_count = 0;
        }
    }
}

// SAFETY: these controls only reset observability counters or, in tests, cap
// future slab requests. They do not move/replace the provider, change its
// alignment, or invalidate any live task-slab allocation.
unsafe impl ProviderOwnedPoolControl for ExecutorTaskMemProvider {
    #[inline(always)]
    fn reset_debug_counts(&mut self) {
        ExecutorTaskMemProvider::reset_debug_counts(self);
    }

    #[cfg(all(test, not(miri)))]
    #[inline(always)]
    fn set_max_request_count(&mut self, max_request_count: Option<usize>) {
        self.max_request_count = max_request_count;
    }
}

// SAFETY: this private provider is owned by exactly one task
// ProviderOwnedPool. Its SlabAllocator initializes the alignment once before
// the first request and never changes it while task slabs are live. Each
// successful request is a distinct global-allocator allocation using that
// stable Layout, and pool teardown returns the exact pointer and slab size.
unsafe impl MemoryProvider for ExecutorTaskMemProvider {
    fn init(&mut self, required_align: usize) {
        self.alignment = std::cmp::max(self.alignment, required_align);
    }

    fn alignment_guarantee(&self) -> usize {
        self.alignment
    }

    fn request_memory(&mut self, size: usize) -> Option<*mut u8> {
        if size == 0 {
            return None;
        }

        #[cfg(all(test, not(miri)))]
        {
            if self
                .max_request_count
                .is_some_and(|max_requests| self.request_count >= max_requests)
            {
                return None;
            }
        }

        let layout = Layout::from_size_align(size, self.alignment).ok()?;
        // SAFETY: `layout` is validated above and any non-null result is
        // returned with this provider's matching deallocation contract.
        let ptr = unsafe { alloc(layout) };
        if ptr.is_null() {
            None
        } else {
            self.note_request();
            Some(ptr)
        }
    }

    unsafe fn free_memory(&mut self, ptr: *mut u8, size: usize) {
        if let Ok(layout) = Layout::from_size_align(size, self.alignment) {
            // SAFETY: MemoryProvider callers return the exact pointer and size
            // produced by `request_memory`; `self.alignment` is the allocation
            // alignment used for every task slab.
            unsafe {
                std::alloc::dealloc(ptr, layout);
            }
            self.note_free();
        }
    }
}

/// User-facing runtime configuration.
///
/// The configuration is typically chosen once when the executor is built.
/// Mutating these knobs per request is not a fast-path pattern.
///
/// # Example
/// ```no_run
/// use flowio::runtime::executor::ExecutorConfig;
/// use flowio::runtime::reactor::ReactorConfig;
///
/// let config = ExecutorConfig {
///     reactor: ReactorConfig {
///         ring_entries: 512,
///     },
///     process_quota: 64,
///     cpu_affinity: None,
/// };
/// # let _ = config;
/// ```
#[derive(Clone, Copy)]
pub struct ExecutorConfig {
    /// io_uring reactor configuration used by the executor.
    pub reactor: ReactorConfig,
    /// Per-phase cap used to keep ready-task polling, CQE draining, and timer
    /// processing fair within one loop pass. `0` selects
    /// [`DEFAULT_PROCESS_QUOTA`].
    pub process_quota: usize,
    /// Optional zero-based CPU id to pin the loop thread to on Linux.
    ///
    /// On Linux, values must fit the platform `cpu_set_t` bitset passed to
    /// `sched_setaffinity`; larger values are rejected as `InvalidInput`
    /// before calling libc. CPUs that fit the bitset but are not valid for the
    /// process are reported through the kernel error from `sched_setaffinity`.
    ///
    /// On non-Linux targets, `Some(_)` is rejected as unsupported.
    pub cpu_affinity: Option<usize>,
}

impl Default for ExecutorConfig {
    fn default() -> Self {
        Self {
            reactor: ReactorConfig::default(),
            process_quota: DEFAULT_PROCESS_QUOTA,
            cpu_affinity: None,
        }
    }
}

pub(crate) struct RuntimeState {
    /// Number of unfinished tasks currently owned by the executor.
    pub(crate) live_tasks: usize,
    /// Number of submitted operations that have not retired yet.
    pub(crate) inflight_ops: usize,
    /// Monotonic wake epoch used to coalesce repeated timer wakes in one pass.
    pub(crate) wake_epoch: u64,
    #[cfg(debug_assertions)]
    /// Debug-only scheduler and allocation counters.
    pub(crate) stats: RuntimeStats,
}

impl RuntimeState {
    fn new() -> Self {
        Self {
            live_tasks: 0,
            inflight_ops: 0,
            wake_epoch: 1,
            #[cfg(debug_assertions)]
            stats: RuntimeStats::default(),
        }
    }
}

/// Bounded, executor-owned descriptor-close worker.
///
/// The executor owner thread is the sole producer and uses `try_send`, so
/// admission never waits for queue capacity or for a worker that is honoring
/// positive `SO_LINGER`. The worker is the sole consumer and owns every
/// admitted descriptor until `close(2)` completes. The channel holds at most
/// `ring_entries` queued owners and the worker may hold one additional owner
/// while closing it. Closing the sender drains that finite set before the
/// worker exits; joining it makes executor shutdown semantics explicit.
struct CloseWorker {
    sender: Option<SyncSender<OwnedFd>>,
    worker: Option<ThreadJoinHandle<()>>,
}

impl CloseWorker {
    fn new(capacity: usize) -> io::Result<Self> {
        if capacity == 0 {
            return Err(io::Error::new(
                ErrorKind::InvalidInput,
                "close worker capacity must be positive",
            ));
        }
        let (sender, receiver) = sync_channel(capacity);
        let worker = std::thread::Builder::new()
            .name("flowio-close".to_owned())
            .spawn(move || close_worker_loop(receiver))?;
        Ok(Self {
            sender: Some(sender),
            worker: Some(worker),
        })
    }

    /// Uses the channel's non-waiting admission API, or returns the unchanged
    /// sole owner.
    #[inline(always)]
    fn try_admit(&self, fd: OwnedFd) -> Result<(), CloseWorkerRejection> {
        let Some(sender) = self.sender.as_ref() else {
            return Err(CloseWorkerRejection::Disconnected(fd));
        };
        match sender.try_send(fd) {
            Ok(()) => Ok(()),
            Err(TrySendError::Full(fd)) => Err(CloseWorkerRejection::Full(fd)),
            Err(TrySendError::Disconnected(fd)) => Err(CloseWorkerRejection::Disconnected(fd)),
        }
    }

    /// Stops admission, drains all admitted descriptor owners, and joins the sole
    /// consumer. An admitted positive-linger close may delay this setup-path
    /// shutdown, matching the socket's requested close semantics.
    fn shutdown(&mut self) {
        self.sender.take();
        if let Some(worker) = self.worker.take() {
            // Dropping an OwnedFd cannot unwind; a panic would therefore come
            // from outside the close loop's ownership protocol. Do not panic
            // from Executor::drop while still ensuring the handle is joined.
            let _ = worker.join();
        }
    }
}

impl Drop for CloseWorker {
    fn drop(&mut self) {
        self.shutdown();
    }
}

fn close_worker_loop(receiver: Receiver<OwnedFd>) {
    while let Ok(fd) = receiver.recv() {
        drop(fd);
    }
}

/// Ensures a worker cannot remain reachable through unfinished task cycles if
/// a user future's destructor unwinds during executor shutdown.
struct CloseWorkerShutdownGuard {
    worker: *mut CloseWorker,
}

impl CloseWorkerShutdownGuard {
    fn new(worker: *mut CloseWorker) -> Self {
        Self { worker }
    }
}

impl Drop for CloseWorkerShutdownGuard {
    fn drop(&mut self) {
        // SAFETY: shutdown_owner creates this guard from its heap-stable
        // ExecutorState and keeps that state alive until after guard drop.
        unsafe {
            (*self.worker).shutdown();
        }
    }
}

enum CloseWorkerRejection {
    Full(OwnedFd),
    Disconnected(OwnedFd),
}

#[derive(Clone, Copy)]
pub(crate) struct ScheduleCtx {
    /// Ready queue the woken task should be pushed onto.
    pub(crate) ready_queue: *mut DList<TaskHeader>,
    /// Shared runtime state updated during wake/schedule transitions.
    pub(crate) runtime_state: *mut RuntimeState,
}

/// Heap-stable state shared by one executor and every task, operation, or timer
/// slot that can outlive its public [`Executor`] handle.
struct ExecutorState {
    /// Reactor driving kernel-visible I/O for this owner.
    reactor: Reactor,
    /// Bounded descriptor-close worker used by runtime-owned transports.
    close_worker: CloseWorker,
    /// Pool storing pointer-stable task allocations.
    task_pool: ProviderOwnedPool<Task<TASK_POOL_SIZE>, ExecutorTaskMemProvider>,
    /// Main queue of runnable tasks.
    ready_queue: DList<TaskHeader>,
    /// Intrusive registry of every allocated task slot.
    all_tasks: DList<TaskHeader>,
    /// Runtime timer subsystem shared by all sleeps and deadlines.
    timers: TimerRuntime,
    /// Persistent counters and run lifecycle state.
    runtime_state: RuntimeState,
    /// Task currently being polled, or null outside a task poll.
    owner_task: *mut TaskHeader,
    /// Set after one-time intrusive/runtime initialization is complete.
    initialized: bool,
    /// Prevents teardown wakeups from re-entering the ready queue.
    shutting_down: bool,
}

/// Stable origin identity retained by runtime-owned slots.
///
/// The runtime is single-threaded, so `Rc` strong-count operations and every
/// task-waker action stay confined to the owner thread.
pub(crate) struct ExecutorOwner {
    /// Interior-mutability boundary for the owner-thread-only runtime state.
    state: UnsafeCell<ExecutorState>,
    /// Thread that constructed and owns this executor. Used only by the
    /// debug-only task-waker owner-thread guard.
    #[cfg(debug_assertions)]
    owner_thread: std::thread::ThreadId,
}

impl ExecutorOwner {
    #[inline(always)]
    fn state_ptr(&self) -> *mut ExecutorState {
        self.state.get()
    }

    /// Debug-only check that a task waker is used on this executor's owner
    /// thread. The runtime is single-threaded, so this always holds; the guard
    /// exists to catch accidental off-thread waker use during development.
    #[cfg(debug_assertions)]
    #[inline(always)]
    pub(crate) fn debug_assert_owner_thread(&self) {
        debug_assert_eq!(
            std::thread::current().id(),
            self.owner_thread,
            "FlowIO task waker used off its executor owner thread; the runtime is single-threaded"
        );
    }

    #[cfg(not(debug_assertions))]
    #[inline(always)]
    pub(crate) fn debug_assert_owner_thread(&self) {}

    #[inline(always)]
    pub(crate) fn reactor_ptr(&self) -> *mut Reactor {
        unsafe { std::ptr::addr_of_mut!((*self.state_ptr()).reactor) }
    }

    #[inline(always)]
    pub(crate) fn timers_ptr(&self) -> *mut TimerRuntime {
        unsafe { std::ptr::addr_of_mut!((*self.state_ptr()).timers) }
    }

    /// Clones the `Rc` represented by a live owner pointer.
    ///
    /// # Safety
    ///
    /// `owner` must come from `Rc::as_ptr` and retain at least one strong count
    /// for the duration of this call. This operation must run on the owner
    /// thread; escaped standard wakers never clone this `Rc`.
    #[inline(always)]
    pub(crate) unsafe fn clone_rc(owner: *const Self) -> Rc<Self> {
        unsafe {
            Rc::increment_strong_count(owner);
            Rc::from_raw(owner)
        }
    }
}

thread_local! {
    static EXECUTOR_CTX: Cell<*const ExecutorOwner> = const { Cell::new(std::ptr::null()) };
}

struct ExecutorCtxGuard {
    owner: *const ExecutorOwner,
    previous: *const ExecutorOwner,
}

impl ExecutorCtxGuard {
    #[inline(always)]
    fn reject_if_active() -> io::Result<()> {
        EXECUTOR_CTX.with(|ctx_cell| {
            if ctx_cell.get().is_null() {
                Ok(())
            } else {
                Err(io::Error::new(
                    ErrorKind::InvalidInput,
                    "nested or reentrant Executor::run is not supported",
                ))
            }
        })
    }

    #[inline(always)]
    fn install(owner: *const ExecutorOwner) -> io::Result<Self> {
        Self::reject_if_active()?;
        Ok(Self::install_for_shutdown(owner))
    }

    /// Temporarily installs `owner` while executor teardown drops runtime
    /// futures. Teardown can occur while another executor is active on the
    /// same thread, so the previous context is restored on scope exit.
    #[inline(always)]
    fn install_for_shutdown(owner: *const ExecutorOwner) -> Self {
        let previous = EXECUTOR_CTX.with(|ctx_cell| ctx_cell.replace(owner));
        Self { owner, previous }
    }
}

impl Drop for ExecutorCtxGuard {
    #[inline(always)]
    fn drop(&mut self) {
        EXECUTOR_CTX.with(|ctx_cell| {
            if ctx_cell.get() == self.owner {
                ctx_cell.set(self.previous);
            }
        });
    }
}

/// Thin handle to a validated active executor poll context.
#[derive(Clone, Copy)]
pub(crate) struct PollCtx {
    /// Stable owner encoded in the task currently being polled.
    owner: *const ExecutorOwner,
    /// Task encoded in the validated FlowIO waker, or null for owner-only
    /// internal contexts that never register a waiter.
    task: *mut TaskHeader,
}

impl PollCtx {
    #[inline(always)]
    pub fn reactor(&self) -> *mut Reactor {
        unsafe { (*self.owner).reactor_ptr() }
    }

    #[inline(always)]
    pub fn runtime_state(&self) -> *mut RuntimeState {
        unsafe { std::ptr::addr_of_mut!((*(*self.owner).state_ptr()).runtime_state) }
    }

    #[inline(always)]
    pub fn timers(&self) -> *mut TimerRuntime {
        unsafe { (*self.owner).timers_ptr() }
    }

    #[inline(always)]
    pub fn owner_task(&self) -> *mut TaskHeader {
        debug_assert!(!self.task.is_null(), "poll context has no waiter task");
        self.task
    }

    #[inline(always)]
    pub(crate) fn owner_ptr(&self) -> *const ExecutorOwner {
        self.owner
    }
}

#[inline(always)]
fn inactive_poll_context_error() -> io::Error {
    io::Error::from(ErrorKind::NotConnected)
}

/// Validates and extracts the FlowIO task and active executor represented by
/// one future poll.
#[inline(always)]
pub(crate) fn poll_ctx_from_waker(cx: &std::task::Context<'_>) -> io::Result<PollCtx> {
    let task = task_ptr_from_waker(cx.waker()).ok_or_else(inactive_poll_context_error)?;
    let owner = unsafe { (*task).owner.as_ref().map_or(std::ptr::null(), Rc::as_ptr) };
    let active_owner = EXECUTOR_CTX.with(Cell::get);
    if owner.is_null() || active_owner != owner {
        return Err(inactive_poll_context_error());
    }

    Ok(PollCtx { owner, task })
}

/// Validates the current FlowIO poll context before an I/O future completes
/// locally without allocating or submitting an operation.
#[inline(always)]
pub(crate) fn validate_local_io_result<T>(
    cx: &std::task::Context<'_>,
    result: io::Result<T>,
) -> io::Result<T> {
    poll_ctx_from_waker(cx)?;
    result
}

/// Origin-bound context used to retire a completed operation safely even when
/// its current poll context is invalid.
pub(crate) struct CompletedOpCtx {
    origin: PollCtx,
    current: Option<PollCtx>,
    context_rejected: bool,
}

impl CompletedOpCtx {
    #[inline(always)]
    pub(crate) fn reactor(&self) -> *mut Reactor {
        self.origin.reactor()
    }

    #[inline(always)]
    pub(crate) fn origin_poll_ctx(&self) -> &PollCtx {
        &self.origin
    }

    #[inline(always)]
    pub(crate) fn context_rejected(&self) -> bool {
        self.context_rejected
    }

    #[inline(always)]
    pub(crate) fn matching_poll_ctx(&self) -> Option<PollCtx> {
        if self.context_rejected {
            None
        } else {
            self.current
        }
    }
}

/// Records current-poll misuse and returns the operation's origin reactor for
/// completion cleanup.
///
/// # Safety
///
/// `state_ptr` must identify a live completed state allocated by a FlowIO
/// reactor. Such states always retain a non-null executor owner.
#[inline(always)]
pub(crate) unsafe fn completed_op_ctx_from_waker(
    cx: &std::task::Context<'_>,
    state_ptr: *mut CompletionState,
) -> CompletedOpCtx {
    debug_assert!(!state_ptr.is_null(), "completed operation state is missing");
    let state = unsafe { &mut *state_ptr };
    debug_assert!(state.is_completed(), "operation has not completed");
    let owner = state.owner_ptr();
    debug_assert!(!owner.is_null(), "completed operation has no origin owner");

    let current = poll_ctx_from_waker(cx).ok();
    let current_matches = current.is_some_and(|current| current.owner_ptr() == owner);
    if !current_matches {
        state.set_context_rejected();
    }

    CompletedOpCtx {
        origin: PollCtx {
            owner,
            task: std::ptr::null_mut(),
        },
        current,
        context_rejected: state.is_context_rejected(),
    }
}

#[inline(always)]
/// Replaces an in-flight operation's waiter with a validated FlowIO task.
/// Invalid or foreign polls are remembered so completion returns
/// `NotConnected`; a valid foreign FlowIO task may still be registered so the
/// original reactor can notify it through its stable executor owner after the
/// CQE.
///
/// # Safety
///
/// `state_ptr` must point to a live, incomplete completion state exclusively
/// owned by the currently polled future.
pub(crate) unsafe fn refresh_op_waiter_from_waker(
    cx: &std::task::Context<'_>,
    state_ptr: *mut CompletionState,
) {
    debug_assert!(
        !state_ptr.is_null(),
        "cannot refresh waiter for a missing completion state"
    );
    if state_ptr.is_null() {
        return;
    }

    let state = unsafe { &mut *state_ptr };
    debug_assert!(
        !state.is_completed(),
        "cannot refresh a completed operation"
    );
    match poll_ctx_from_waker(cx) {
        Ok(pctx) => {
            if state.owner_ptr() != pctx.owner_ptr() {
                state.set_context_rejected();
            }
            unsafe { state.register_waiter(pctx.owner_task()) };
        }
        Err(_) => state.set_context_rejected(),
    }
}

// ---------------------------------------------------------------------------
// JoinHandle
// ---------------------------------------------------------------------------

/// Internal wrapper stored in the task data area.  Holds the user's future,
/// the result slot, and an optional waker for the JoinHandle.
#[repr(C)]
struct JoinTask<F: Future> {
    /// Spawned future until it completes and is dropped.
    future: Option<F>,
    /// Completed output or shutdown cancellation, taken by the join handle.
    result: Option<Result<F::Output, JoinError>>,
    /// Last join-handle waker registered while waiting for the result. Woken
    /// once when the spawned future stores its output.
    join_waker: Option<Waker>,
}

/// Error returned when a spawned task cannot produce its output.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum JoinError {
    /// The owning executor was dropped before the task completed.
    Cancelled,
}

impl std::fmt::Display for JoinError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Cancelled => f.write_str("spawned task was cancelled"),
        }
    }
}

impl std::error::Error for JoinError {}

/// Error returned by [`Executor::try_spawn`].
///
/// Each variant contains the original future so callers that own external
/// state, such as an active RPC answer, can retry, reject, or clean up without
/// losing task ownership when the executor cannot accept new work.
///
/// # Example
/// ```
/// use flowio::runtime::executor::{Executor, TrySpawnError};
///
/// let result = Executor::try_spawn(async { 1 });
/// match result {
///     Err(TrySpawnError::NoExecutor { future }) => drop(future),
///     _ => panic!("try_spawn outside Executor::run should fail with NoExecutor"),
/// }
/// ```
pub enum TrySpawnError<F> {
    /// No executor is currently active on this thread.
    NoExecutor {
        /// The original future passed to `try_spawn`.
        future: F,
    },
    /// The concrete `JoinTask<F>` does not fit in the executor's fixed task
    /// slot size or alignment.
    TaskTooLarge {
        /// The original future passed to `try_spawn`.
        future: F,
    },
    /// The task pool could not allocate a task slot.
    AtCapacity {
        /// The original future passed to `try_spawn`.
        future: F,
    },
}

impl<F> TrySpawnError<F> {
    /// Returns the original future that could not be spawned.
    pub fn into_future(self) -> F {
        match self {
            Self::NoExecutor { future }
            | Self::TaskTooLarge { future }
            | Self::AtCapacity { future } => future,
        }
    }

    fn into_io_error(self) -> io::Error {
        match self {
            Self::NoExecutor { .. } | Self::TaskTooLarge { .. } => {
                io::Error::from(ErrorKind::InvalidInput)
            }
            Self::AtCapacity { .. } => io::Error::from(ErrorKind::OutOfMemory),
        }
    }

    fn kind_name(&self) -> &'static str {
        match self {
            Self::NoExecutor { .. } => "NoExecutor",
            Self::TaskTooLarge { .. } => "TaskTooLarge",
            Self::AtCapacity { .. } => "AtCapacity",
        }
    }

    fn description(&self) -> &'static str {
        match self {
            Self::NoExecutor { .. } => "no executor is currently active on this thread",
            Self::TaskTooLarge { .. } => {
                "spawned task does not fit in the executor's fixed task slot"
            }
            Self::AtCapacity { .. } => "executor task pool could not allocate a task slot",
        }
    }
}

impl<F> std::fmt::Debug for TrySpawnError<F> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TrySpawnError")
            .field("kind", &self.kind_name())
            .field("future", &"<returned>")
            .finish()
    }
}

impl<F> std::fmt::Display for TrySpawnError<F> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.description())
    }
}

impl<F> std::error::Error for TrySpawnError<F> {}

/// Handle returned by [`Executor::spawn`] that resolves to the spawned task's
/// return value or an explicit cancellation error.
///
/// Awaiting or dropping a handle is part of the steady-state task path, as the
/// await side of [`Executor::spawn`] / [`Executor::try_spawn`]. It does not
/// allocate.
///
/// Dropping the handle without awaiting detaches the task while its executor is
/// alive. Dropping the executor cancels any unfinished detached task.
///
/// # Example
/// ```no_run
/// use flowio::runtime::executor::Executor;
///
/// let mut executor = Executor::new()?;
/// executor.run(async {
///     let handle = Executor::spawn(async { 42 }).unwrap();
///     assert_eq!(handle.await.unwrap(), 42);
/// })?;
/// # Ok::<(), std::io::Error>(())
/// ```
pub struct JoinHandle<T: 'static> {
    /// Owning task header kept alive while the handle exists.
    task_ptr: *mut TaskHeader,
    /// Pointer to the join result slot inside the task's `JoinTask`.
    result_ptr: *mut Option<Result<T, JoinError>>,
    /// Pointer to the `Option<Waker>` join_waker slot inside the task's JoinTask.
    waker_ptr: *mut Option<Waker>,
}

impl<T: 'static> JoinHandle<T> {
    /// Returns `true` if the spawned task has completed and its result is
    /// available.  This is a non-blocking, non-consuming check.
    pub fn is_finished(&self) -> bool {
        unsafe { (*self.result_ptr).is_some() }
    }
}

impl<T: 'static> Future for JoinHandle<T> {
    type Output = Result<T, JoinError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = unsafe { self.get_unchecked_mut() };
        let result_slot = unsafe { &mut *this.result_ptr };

        if let Some(value) = result_slot.take() {
            return Poll::Ready(value);
        }

        let waker_slot = unsafe { &mut *this.waker_ptr };
        if !waker_slot
            .as_ref()
            .is_some_and(|stored| stored.will_wake(cx.waker()))
        {
            *waker_slot = Some(cx.waker().clone());
        }
        Poll::Pending
    }
}

impl<T: 'static> Drop for JoinHandle<T> {
    fn drop(&mut self) {
        unsafe {
            release_task(self.task_ptr);
        }
    }
}

/// Single-threaded executor that drives tasks and `io_uring` completions.
///
/// The intended fast-path shape is one long-lived executor per runtime thread.
/// Constructing an executor initializes the reactor, task pool, ready queue,
/// and timer runtime, so it is setup work rather than per-request work.
///
/// # Example
/// ```no_run
/// use flowio::runtime::executor::Executor;
///
/// let mut executor = Executor::new()?;
/// executor.run(async {})?;
/// # Ok::<(), std::io::Error>(())
/// ```
pub struct Executor {
    /// Heap-stable owner retained by task, operation, and timer slots.
    owner: Rc<ExecutorOwner>,
    /// Maximum number of items processed per phase (ready tasks, CQEs,
    /// timer expiries) in each executor loop iteration.
    process_quota: usize,
    /// Logical CPU id to pin the executor thread to via `sched_setaffinity`.
    /// `None` means no pinning. On non-Linux targets, `Some(_)` is rejected
    /// as unsupported.
    cpu_affinity: Option<usize>,
    #[cfg(debug_assertions)]
    /// Debug counters captured when the most recent run drained or reported a
    /// stalled `WouldBlock` state.
    last_stats: RuntimeStats,
}

impl Executor {
    /// Returns the configured process quota for repository tests.
    #[cfg(any(test, feature = "test-support"))]
    #[doc(hidden)]
    pub fn test_process_quota(&self) -> usize {
        self.process_quota
    }

    /// Returns the configured CPU affinity for repository tests.
    #[cfg(any(test, feature = "test-support"))]
    #[doc(hidden)]
    pub fn test_cpu_affinity(&self) -> Option<usize> {
        self.cpu_affinity
    }

    /// Constructs an executor with default configuration.
    ///
    /// This is a setup/control-plane API. Typical applications construct one
    /// executor per runtime thread and keep it alive rather than recreating it
    /// in the steady-state fast path.
    ///
    /// # Errors
    /// Returns `Unsupported` when the running Linux kernel does not provide
    /// the `IORING_ENTER_EXT_ARG` feature required for timed `io_uring` waits.
    /// Returns the operating-system error if the bounded close-worker thread
    /// cannot be created.
    ///
    /// # Example
    /// ```no_run
    /// use flowio::runtime::executor::Executor;
    ///
    /// let executor = Executor::new()?;
    /// # let _ = executor;
    /// # Ok::<(), std::io::Error>(())
    /// ```
    pub fn new() -> io::Result<Self> {
        Self::new_with_config(ExecutorConfig::default())
    }

    /// Constructs an executor with explicit reactor and scheduling settings.
    ///
    /// This is also a setup/control-plane API rather than a per-operation
    /// fast-path primitive.
    ///
    /// # Errors
    /// Returns `Unsupported` when the running Linux kernel does not provide
    /// the `IORING_ENTER_EXT_ARG` feature required for timed `io_uring` waits.
    /// Returns `InvalidInput` for a zero close-worker capacity and the
    /// operating-system error if its thread cannot be created.
    ///
    /// # Example
    /// ```no_run
    /// use flowio::runtime::executor::{Executor, ExecutorConfig};
    ///
    /// let executor = Executor::new_with_config(ExecutorConfig::default())?;
    /// # let _ = executor;
    /// # Ok::<(), std::io::Error>(())
    /// ```
    pub fn new_with_config(config: ExecutorConfig) -> io::Result<Self> {
        let task_pool = ProviderOwnedPool::new(ExecutorTaskMemProvider::new(), TASKS_PER_SLAB)
            .map_err(|_| io::Error::from(ErrorKind::InvalidInput))?;
        let ready_queue = DList::new_uninit();
        let all_tasks = DList::new_uninit();
        let reactor = Reactor::new_with_config(config.reactor)?;
        let timers = TimerRuntime::new()?;
        let close_worker = CloseWorker::new(config.reactor.ring_entries as usize)?;

        let owner = Rc::new(ExecutorOwner {
            state: UnsafeCell::new(ExecutorState {
                reactor,
                close_worker,
                task_pool,
                ready_queue,
                all_tasks,
                timers,
                runtime_state: RuntimeState::new(),
                owner_task: std::ptr::null_mut(),
                initialized: false,
                shutting_down: false,
            }),
            // The owner is constructed on the executor's owner thread.
            #[cfg(debug_assertions)]
            owner_thread: std::thread::current().id(),
        });

        Ok(Self {
            owner,
            process_quota: if config.process_quota == 0 {
                DEFAULT_PROCESS_QUOTA
            } else {
                config.process_quota
            },
            cpu_affinity: config.cpu_affinity,
            #[cfg(debug_assertions)]
            last_stats: RuntimeStats::default(),
        })
    }

    /// Performs one-time initialization for the executor's intrusive
    /// structures and runtime-owned subsystems.
    fn init(&mut self) -> io::Result<()> {
        let owner_ptr = Rc::as_ptr(&self.owner);
        let state = unsafe { &mut *self.owner.state_ptr() };
        if state.initialized {
            return Ok(());
        }

        state.task_pool.init();
        state.ready_queue.init();
        state.all_tasks.init();
        state.timers.init()?;
        state.timers.bind_owner(owner_ptr);
        state.reactor.init();
        state.reactor.bind_owner(owner_ptr);
        state.initialized = true;
        Ok(())
    }

    /// Spawns a task onto the currently-running executor, returning a
    /// [`JoinHandle`] that resolves to the task output or [`JoinError`].
    ///
    /// Dropping the handle without awaiting detaches the task while the owning
    /// executor remains alive. Dropping that executor cancels unfinished work.
    ///
    /// This must be called from within [`Executor::run`]. For steady-state
    /// concurrency, this is the fast-path way to add work without rebuilding
    /// the executor.
    ///
    /// On failure, this converts [`TrySpawnError`] to [`io::Error`] and drops
    /// the submitted future. Use [`Executor::try_spawn`] when the caller must
    /// recover the future to retry, reject, or release owned state.
    ///
    /// # Errors
    ///
    /// Returns `InvalidInput` when no executor is active or the task does not
    /// fit a fixed task slot. Returns `OutOfMemory` when the task pool's memory
    /// provider cannot allocate another slot.
    ///
    /// # Example
    /// ```no_run
    /// use flowio::runtime::executor::Executor;
    ///
    /// let mut executor = Executor::new()?;
    /// executor.run(async {
    ///     let handle = Executor::spawn(async { 42 }).unwrap();
    ///     assert_eq!(handle.await.unwrap(), 42);
    /// })?;
    /// # Ok::<(), std::io::Error>(())
    /// ```
    pub fn spawn<F>(future: F) -> io::Result<JoinHandle<F::Output>>
    where
        F: Future + 'static,
        F::Output: 'static,
    {
        Self::try_spawn(future).map_err(TrySpawnError::into_io_error)
    }

    /// Attempts to spawn a task and returns the original future on every
    /// failure path.
    ///
    /// This is for callers that cannot lose ownership of the task body on
    /// scheduler pressure. A future may own a response, lease, or cleanup
    /// obligation that the caller must recover so it can retry, reject, or
    /// release explicitly.
    ///
    /// On success, ownership transfers to the executor exactly as with
    /// [`Executor::spawn`], and the returned [`JoinHandle`] yields
    /// `Ok(future_output)` or [`JoinError::Cancelled`] if executor shutdown wins.
    /// On failure, the future has not been polled, pinned, stored in a task slot,
    /// or dropped by the executor path.
    ///
    /// This is the preferred admission API on overload-sensitive fast paths
    /// because pressure is explicit and ownership is preserved.
    ///
    /// # Example
    /// ```no_run
    /// use flowio::runtime::executor::Executor;
    ///
    /// let mut executor = Executor::new()?;
    /// executor.run(async {
    ///     match Executor::try_spawn(async { 42 }) {
    ///         Ok(handle) => assert_eq!(handle.await.unwrap(), 42),
    ///         Err(error) => drop(error.into_future()),
    ///     }
    /// })?;
    /// # Ok::<(), std::io::Error>(())
    /// ```
    pub fn try_spawn<F>(future: F) -> Result<JoinHandle<F::Output>, TrySpawnError<F>>
    where
        F: Future + 'static,
        F::Output: 'static,
    {
        EXECUTOR_CTX.with(|ctx_cell| {
            let owner_ptr = ctx_cell.get();
            if owner_ptr.is_null() {
                return Err(TrySpawnError::NoExecutor { future });
            }

            if size_of::<JoinTask<F>>() > TASK_POOL_SIZE
                || align_of::<JoinTask<F>>() > TASK_DATA_ALIGN
            {
                return Err(TrySpawnError::TaskTooLarge { future });
            }

            let state_ptr = unsafe { (*owner_ptr).state_ptr() };
            if unsafe { (*state_ptr).shutting_down } {
                return Err(TrySpawnError::NoExecutor { future });
            }

            unsafe {
                let slot_ptr = match (*state_ptr).task_pool.alloc(()) {
                    Some(ptr) => ptr,
                    None => {
                        return Err(TrySpawnError::AtCapacity { future });
                    }
                };

                let join_task = JoinTask {
                    future: Some(future),
                    result: None,
                    join_waker: None,
                };
                let data_ptr = (*slot_ptr).data.as_mut_ptr() as *mut JoinTask<F>;
                std::ptr::write(data_ptr, join_task);

                let result_ptr = std::ptr::addr_of_mut!((*data_ptr).result);
                let waker_ptr = std::ptr::addr_of_mut!((*data_ptr).join_waker);

                (*slot_ptr).header.ready_link =
                    crate::utils::list::intrusive::dlist::Link::new_unlinked();
                (*slot_ptr).header.all_link =
                    crate::utils::list::intrusive::dlist::Link::new_unlinked();
                (*slot_ptr).header.owner = Some(ExecutorOwner::clone_rc(owner_ptr));
                // Start with refcount 2: one for the executor, one for the JoinHandle.
                (*slot_ptr).header.refs.set(2);
                (*slot_ptr)
                    .header
                    .flags
                    .set(TaskHeader::FLAG_NOTIFIED | TaskHeader::FLAG_QUEUED);
                (*slot_ptr).header.last_wake_epoch.set(0);
                init_cached_waker(&mut (*slot_ptr).header as *mut _);
                (*slot_ptr).header.vtable = join_task_vtable_for::<F>();

                (*state_ptr).runtime_state.live_tasks += 1;
                #[cfg(debug_assertions)]
                {
                    (*state_ptr).runtime_state.stats.task_allocs += 1;
                }
                (*state_ptr)
                    .all_tasks
                    .push_back_unchecked(&mut (*slot_ptr).header.all_link as *mut _);
                (*state_ptr)
                    .ready_queue
                    .push_back_unchecked(&mut (*slot_ptr).header.ready_link as *mut _);

                Ok(JoinHandle {
                    task_ptr: &mut (*slot_ptr).header as *mut TaskHeader,
                    result_ptr,
                    waker_ptr,
                })
            }
        })
    }

    #[inline(always)]
    fn poll_io_and_process_timers(&self) -> io::Result<()> {
        let state_ptr = self.owner.state_ptr();
        let runtime_state = unsafe { std::ptr::addr_of_mut!((*state_ptr).runtime_state) };
        let ready_queue = unsafe { std::ptr::addr_of_mut!((*state_ptr).ready_queue) };
        let _ = unsafe { &mut (*state_ptr).reactor }.poll_io(
            self.process_quota,
            runtime_state,
            ready_queue,
        )?;
        let timers = unsafe { &mut (*state_ptr).timers };
        if timers.has_pending() {
            let now_tick = timers.now_tick()?;
            let _ = timers.process_at_with_budget(now_tick, self.process_quota)?;
        }
        Ok(())
    }

    /// Runs the root future and continues until the executor drains all work.
    ///
    /// Call this once around a top-level task tree. The intended usage is a
    /// long-lived `run` boundary, not repeatedly entering and exiting the
    /// executor for tiny units of work.
    ///
    /// # Errors
    ///
    /// Returns [`io::ErrorKind::InvalidInput`] if another executor run is
    /// already active on this thread. Nested and reentrant runs are not
    /// supported. Futures submitted or armed by one executor must remain on
    /// that executor; polling them outside an active run or through another
    /// executor's task waker returns [`io::ErrorKind::NotConnected`].
    ///
    /// Returns [`io::ErrorKind::WouldBlock`] if live runtime work remains but
    /// there are no ready tasks, in-flight I/O operations, or timers that can
    /// make progress. Those tasks remain owned by this executor; a later call
    /// resumes them together with its new root future. Dropping the executor
    /// instead cancels and drops every unfinished future exactly once. Reactor
    /// and timer I/O errors are propagated. Signal
    /// interruptions of the `io_uring` wait are retried internally; use a
    /// runtime-visible fd such as `signalfd` or `eventfd` for signal-driven
    /// shutdown instead of relying on `EINTR`.
    ///
    /// # Example
    /// ```no_run
    /// use flowio::runtime::executor::Executor;
    ///
    /// let mut executor = Executor::new()?;
    /// executor.run(async {
    ///     let handle = Executor::spawn(async { 1 + 1 }).unwrap();
    ///     assert_eq!(handle.await.unwrap(), 2);
    /// })?;
    /// # Ok::<(), std::io::Error>(())
    /// ```
    pub fn run<F: Future<Output = ()> + 'static>(&mut self, initial_task: F) -> io::Result<()> {
        ExecutorCtxGuard::reject_if_active()?;
        self.init()?;
        apply_cpu_affinity(self.cpu_affinity)?;

        let owner_ptr = Rc::as_ptr(&self.owner);
        let state_ptr = self.owner.state_ptr();
        let starts_clean = unsafe {
            (*state_ptr).runtime_state.live_tasks == 0
                && (*state_ptr).runtime_state.inflight_ops == 0
                && (*state_ptr).ready_queue.is_empty()
                && !(*state_ptr).timers.has_pending()
        };
        if starts_clean {
            unsafe {
                (*state_ptr).runtime_state = RuntimeState::new();
                (*state_ptr).task_pool.reset_provider_debug_counts();
            }
        }

        let _ctx_guard = ExecutorCtxGuard::install(owner_ptr)?;

        match Self::spawn(initial_task) {
            Ok(_handle) => { /* drop JoinHandle — root task is detached */ }
            Err(err) => {
                return Err(err);
            }
        }

        'run_loop: loop {
            unsafe { (*state_ptr).timers.begin_executor_pass() };
            let mut polled = 0usize;
            while polled < self.process_quota {
                let header_ptr = unsafe {
                    (*state_ptr)
                        .ready_queue
                        .pop_front(TaskHeader::READY_LINK_OFFSET)
                };
                let Some(header_ptr) = header_ptr else {
                    break;
                };

                let header = unsafe { &*header_ptr };
                // Batch flag update: clear QUEUED+NOTIFIED, set RUNNING — one read + one write.
                header.flags.set(
                    (header.flags.get() & !(TaskHeader::FLAG_QUEUED | TaskHeader::FLAG_NOTIFIED))
                        | TaskHeader::FLAG_RUNNING,
                );
                unsafe { std::ptr::addr_of_mut!((*state_ptr).owner_task).write(header_ptr) };
                #[cfg(debug_assertions)]
                unsafe {
                    (*state_ptr).runtime_state.stats.task_polls += 1;
                }
                let poll_res = unsafe { (header.vtable.poll)(header_ptr) };
                unsafe {
                    std::ptr::addr_of_mut!((*state_ptr).owner_task).write(std::ptr::null_mut())
                };
                if let Poll::Ready(()) = poll_res {
                    // Batch: clear RUNNING+NOTIFIED+QUEUED, set COMPLETED.
                    header.flags.set(
                        (header.flags.get()
                            & !(TaskHeader::FLAG_RUNNING
                                | TaskHeader::FLAG_NOTIFIED
                                | TaskHeader::FLAG_QUEUED))
                            | TaskHeader::FLAG_COMPLETED,
                    );
                    unsafe {
                        debug_assert!((*state_ptr).runtime_state.live_tasks > 0);
                        (*state_ptr).runtime_state.live_tasks -= 1;
                        (header.vtable.finish)(header_ptr);
                        release_task(header_ptr);
                    }
                } else {
                    let flags = header.flags.get();
                    header.flags.set(flags & !TaskHeader::FLAG_RUNNING);
                    if (flags & TaskHeader::FLAG_NOTIFIED) != 0 {
                        unsafe {
                            enqueue_notified_task_unchecked(
                                header_ptr,
                                std::ptr::addr_of_mut!((*state_ptr).ready_queue),
                                std::ptr::addr_of_mut!((*state_ptr).runtime_state),
                            );
                        }
                    }
                }

                polled += 1;
            }

            if unsafe { &mut (*state_ptr).reactor }.flush_sqes()? == ReactorSubmitStatus::Busy {
                self.poll_io_and_process_timers()?;
                continue;
            }
            let completed = unsafe { &mut (*state_ptr).reactor }.poll_io(
                self.process_quota,
                unsafe { std::ptr::addr_of_mut!((*state_ptr).runtime_state) },
                unsafe { std::ptr::addr_of_mut!((*state_ptr).ready_queue) },
            )?;
            let timers_pending = unsafe { (*state_ptr).timers.has_pending() };
            let mut now_tick = None;
            let timer_budget_exhausted = if timers_pending {
                let tick = unsafe { (*state_ptr).timers.now_tick()? };
                now_tick = Some(tick);
                unsafe {
                    (*state_ptr)
                        .timers
                        .process_at_with_budget(tick, self.process_quota)?
                }
            } else {
                false
            };
            let queue_empty = unsafe { (*state_ptr).ready_queue.is_empty() };
            let timers_pending_after = unsafe { (*state_ptr).timers.has_pending() };
            let drained = unsafe { (*state_ptr).runtime_state.live_tasks == 0 }
                && unsafe { (*state_ptr).runtime_state.inflight_ops == 0 }
                && !timers_pending_after
                && queue_empty;

            if drained {
                #[cfg(debug_assertions)]
                {
                    self.snapshot_stats();
                }
                return Ok(());
            }

            if completed > 0 || !queue_empty || timer_budget_exhausted {
                continue;
            }

            let timer_wait = match now_tick {
                Some(tick) => unsafe { (*state_ptr).timers.next_wait_duration(tick) },
                None => None,
            };

            if unsafe { (*state_ptr).runtime_state.inflight_ops == 0 } && timer_wait.is_none() {
                #[cfg(debug_assertions)]
                {
                    self.snapshot_stats();
                }
                return Err(io::Error::from(ErrorKind::WouldBlock));
            }

            if matches!(timer_wait, Some(duration) if duration.is_zero()) {
                let _ = unsafe { &mut (*state_ptr).timers }
                    // SAFETY: now_tick is Some when timer_wait is Some (set in the
                    // has_pending() branch above).
                    .process_at_with_budget(
                        unsafe { now_tick.unwrap_unchecked() },
                        self.process_quota,
                    )?;
                continue;
            }

            if unsafe { &mut (*state_ptr).reactor }.wait_for_events(timer_wait)?
                == ReactorSubmitStatus::Busy
            {
                self.poll_io_and_process_timers()?;
                continue 'run_loop;
            }
            self.poll_io_and_process_timers()?;
        }
    }

    #[cfg(debug_assertions)]
    fn snapshot_stats(&mut self) {
        let state = unsafe { &mut *self.owner.state_ptr() };
        let runtime_state = &mut state.runtime_state;
        let provider = state.task_pool.provider_ref();
        runtime_state.stats.task_slab_allocs = provider.request_count;
        runtime_state.stats.task_slab_frees = provider.free_count;
        let retained = state.reactor.retained_payload_stats();
        runtime_state.stats.retained_pooled_allocs = retained.pooled_allocs;
        runtime_state.stats.retained_pooled_reuses = retained.pooled_reuses;
        runtime_state.stats.retained_pooled_frees = retained.pooled_frees;
        runtime_state.stats.retained_slab_allocs = retained.slab_allocs;
        runtime_state.stats.retained_heap_fallbacks = retained.heap_fallbacks;
        runtime_state.stats.retained_heap_frees = retained.heap_frees;
        runtime_state.stats.writev_scratch_inline_allocs = retained.writev_scratch_inline_allocs;
        runtime_state.stats.writev_scratch_pooled_allocs = retained.writev_scratch_pooled_allocs;
        runtime_state.stats.writev_scratch_pooled_reuses = retained.writev_scratch_pooled_reuses;
        runtime_state.stats.writev_scratch_pooled_frees = retained.writev_scratch_pooled_frees;
        runtime_state.stats.writev_scratch_slab_allocs = retained.writev_scratch_slab_allocs;
        runtime_state.stats.writev_scratch_oversize_rejections =
            retained.writev_scratch_oversize_rejections;
        runtime_state.stats.writev_scratch_alloc_failures = retained.writev_scratch_alloc_failures;
        self.last_stats = runtime_state.stats;
    }

    /// Returns debug counters from the latest run that drained or reached the
    /// stalled-work `WouldBlock` check.
    ///
    /// In release builds this dev-only accessor returns an empty snapshot
    /// because the counters are not compiled in.
    #[cfg(any(test, feature = "test-support"))]
    pub fn last_stats(&self) -> RuntimeStats {
        #[cfg(debug_assertions)]
        {
            self.last_stats
        }
        #[cfg(not(debug_assertions))]
        {
            RuntimeStats::default()
        }
    }

    /// Cancels every unfinished task while leaving completed slots available
    /// to escaped join handles.
    fn shutdown_tasks(&mut self) {
        let state_ptr = self.owner.state_ptr();
        unsafe {
            (*state_ptr).shutting_down = true;
        }

        loop {
            let task_ptr = unsafe {
                (*state_ptr)
                    .all_tasks
                    .pop_front(TaskHeader::ALL_LINK_OFFSET)
            };
            let Some(task_ptr) = task_ptr else {
                break;
            };

            let header = unsafe { &*task_ptr };
            if !header.ready_link.is_unlinked() {
                unsafe {
                    (*state_ptr)
                        .ready_queue
                        .remove(std::ptr::addr_of_mut!((*task_ptr).ready_link));
                }
            }

            let flags = header.flags.get();
            header.flags.set(
                (flags
                    & !(TaskHeader::FLAG_RUNNING
                        | TaskHeader::FLAG_NOTIFIED
                        | TaskHeader::FLAG_QUEUED))
                    | TaskHeader::FLAG_COMPLETED,
            );
            if task_is_completed(flags) {
                continue;
            }

            unsafe {
                debug_assert!((*state_ptr).runtime_state.live_tasks > 0);
                (*state_ptr).runtime_state.live_tasks -= 1;
                (header.vtable.cancel)(task_ptr);
                release_task(task_ptr);
            }
        }

        unsafe {
            (*state_ptr).owner_task = std::ptr::null_mut();
            (*state_ptr).ready_queue.unlink_all_for_drop();
        }
    }

    fn shutdown_owner(&mut self) {
        let state_ptr = self.owner.state_ptr();
        if unsafe { !(*state_ptr).initialized || (*state_ptr).shutting_down } {
            return;
        }

        let owner_ptr = Rc::as_ptr(&self.owner);
        let _ctx_guard = ExecutorCtxGuard::install_for_shutdown(owner_ptr);
        let _close_worker_guard = CloseWorkerShutdownGuard::new(unsafe {
            std::ptr::addr_of_mut!((*state_ptr).close_worker)
        });
        self.shutdown_tasks();
        unsafe {
            (*state_ptr).timers.cancel_all_for_shutdown();
            let runtime_state = std::ptr::addr_of_mut!((*state_ptr).runtime_state);
            let ready_queue = std::ptr::addr_of_mut!((*state_ptr).ready_queue);
            (*state_ptr).reactor.shutdown(runtime_state, ready_queue);
        }
    }
}

#[cfg(target_os = "linux")]
fn apply_cpu_affinity(cpu_affinity: Option<usize>) -> io::Result<()> {
    let Some(cpu) = cpu_affinity else {
        return Ok(());
    };

    let max_cpu = 8 * size_of::<libc::cpu_set_t>();
    if cpu >= max_cpu {
        return Err(io::Error::new(
            ErrorKind::InvalidInput,
            "cpu_affinity exceeds platform cpu_set_t capacity",
        ));
    }

    // SAFETY: all-zero bytes are a valid empty cpu_set_t value for the libc
    // CPU-set helpers used below.
    let mut set = unsafe { std::mem::zeroed::<libc::cpu_set_t>() };
    // SAFETY: `cpu` was bounded to the bit capacity of `set`, which remains
    // exclusively borrowed for both libc operations.
    unsafe {
        libc::CPU_ZERO(&mut set);
        libc::CPU_SET(cpu, &mut set);
    }

    // SAFETY: `set` is initialized and the supplied byte count is its exact
    // in-memory size; pid 0 selects the calling thread.
    let rc = unsafe { libc::sched_setaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &set) };
    if rc < 0 {
        return Err(io::Error::last_os_error());
    }

    Ok(())
}

#[cfg(not(target_os = "linux"))]
fn apply_cpu_affinity(cpu_affinity: Option<usize>) -> io::Result<()> {
    if cpu_affinity.is_some() {
        return Err(io::Error::from(ErrorKind::Unsupported));
    }

    Ok(())
}

impl Drop for Executor {
    fn drop(&mut self) {
        self.shutdown_owner();
    }
}

/// Cancel an in-flight operation from a future's `Drop` impl.
/// Marks the `CompletionState` as orphaned and submits `ASYNC_CANCEL`.
/// Reclaims through the completion state's recorded origin owner.
///
/// # Safety
///
/// `ptr` must point to a live, submitted completion state.
unsafe fn cancel_op_unchecked(ptr: *mut crate::runtime::op::CompletionState) {
    let Some(owner) = (unsafe { (*ptr).clone_owner() }) else {
        return;
    };
    unsafe { (*owner.reactor_ptr()).cancel_op(ptr) };
}

/// Free a completed `CompletionState` from a future's `Drop` impl when the
/// CQE has already been consumed but the future is dropped before polling the
/// result. Reclamation uses the state owner and therefore remains valid after a
/// run boundary or public `Executor` teardown.
///
/// # Safety
///
/// `ptr` must point to a completed state, and its result must already have been
/// consumed or otherwise made safe to drop.
unsafe fn free_op_unchecked(ptr: *mut crate::runtime::op::CompletionState) {
    let Some(owner) = (unsafe { (*ptr).clone_owner() }) else {
        return;
    };
    unsafe { (*owner.reactor_ptr()).free_op(ptr) };
}

/// Release a future-owned `CompletionState` pointer from `Drop`.
/// Completed ops are freed immediately; pending ops are orphaned and cancelled.
/// The caller's pointer is always cleared.
///
/// # Safety
///
/// A non-null `*ptr` must identify the completion state owned by this future
/// in its recorded origin reactor. The caller must not retain another owner
/// that may free the same state.
#[inline(always)]
pub(crate) unsafe fn drop_op_ptr_unchecked(ptr: &mut *mut crate::runtime::op::CompletionState) {
    let state_ptr = *ptr;
    if state_ptr.is_null() {
        return;
    }

    unsafe {
        if (*state_ptr).is_completed() {
            free_op_unchecked(state_ptr);
        } else {
            cancel_op_unchecked(state_ptr);
        }
    }

    *ptr = std::ptr::null_mut();
}

/// Submit an SQE and account for one tracked in-flight operation.
/// Consolidates the normal submission bookkeeping shared by I/O futures.
///
/// # Safety
///
/// `pctx` must refer to the active executor, and every pointer referenced by
/// `sqe` (including its completion-state `user_data`) must remain valid until
/// the target CQE retires.
#[inline(always)]
pub(crate) unsafe fn submit_tracked_sqe(
    pctx: &PollCtx,
    sqe: io_uring::squeue::Entry,
) -> io::Result<()> {
    unsafe { (*pctx.reactor()).submit_sqe(sqe)? };
    unsafe {
        (*pctx.runtime_state()).inflight_ops += 1;
        #[cfg(debug_assertions)]
        {
            (*pctx.runtime_state()).stats.sqe_submits += 1;
        }
    }
    Ok(())
}

/// Retain a kernel-visible payload, build the SQE from that stable storage,
/// and submit it with normal in-flight accounting.
///
/// On error the retained payload is detached and returned to the caller so
/// the future can preserve buffer ownership and retire the completion state.
///
/// # Safety
///
/// `pctx` and `state_ptr` must belong to the same active reactor;
/// `state_ptr` must be a live, exclusively owned state with no attached
/// payload. `build` may expose pointers into its payload only through the SQE
/// returned for immediate submission.
#[inline(always)]
pub(crate) unsafe fn submit_retained_sqe<T: 'static, F>(
    pctx: &PollCtx,
    state_ptr: *mut crate::runtime::op::CompletionState,
    payload_value: T,
    build: F,
) -> Result<(), (io::Error, T)>
where
    F: FnOnce(&mut T) -> io::Result<squeue::Entry>,
{
    let reactor = pctx.reactor();
    let payload = unsafe { (*reactor).alloc_retained_payload(payload_value) };
    unsafe { (*state_ptr).attach_retained_payload(payload) };

    let sqe = match build(unsafe { (*state_ptr).retained_payload_mut::<T>() }) {
        Ok(sqe) => sqe,
        Err(err) => {
            let payload = unsafe { (*reactor).take_retained_payload::<T>(state_ptr) };
            return Err((err, payload));
        }
    };

    if let Err(err) = unsafe { submit_tracked_sqe(pctx, sqe) } {
        let payload = unsafe { (*reactor).take_retained_payload::<T>(state_ptr) };
        return Err((err, payload));
    }

    Ok(())
}

/// Result of trying to transfer a descriptor to the active executor's bounded
/// close worker.
pub(crate) enum CloseAdmission {
    /// The worker accepted sole ownership.
    Admitted,
    /// No executor is active; preserve ordinary caller-thread close behavior.
    OutsideExecutor(OwnedFd),
    /// The active worker queue was full; apply overload fallback.
    Full(OwnedFd),
    /// The active worker was disconnected; apply lifecycle-failure fallback.
    Disconnected(OwnedFd),
}

/// Result of trying to transfer a descriptor into the active reactor's
/// close-only submission ledger.
pub(crate) enum CloseSubmission {
    /// The reactor queued a plain close SQE and retained the owner until kernel
    /// admission.
    Submitted,
    /// No executor is active, so the caller retains ordinary drop semantics.
    OutsideExecutor(OwnedFd),
    /// The active reactor could not queue the close; the unchanged owner is
    /// returned for a nonblocking direct-close fallback.
    Rejected(OwnedFd),
}

/// Returns whether descriptor teardown currently runs inside an executor.
///
/// The caller performs no user code between this check and close routing, so
/// the owner-thread TLS context cannot change between the two operations.
#[inline(always)]
pub(crate) fn has_active_close_context() -> bool {
    EXECUTOR_CTX.with(|ctx_cell| {
        let owner = ctx_cell.get();
        if owner.is_null() {
            return false;
        }
        unsafe {
            (*owner).debug_assert_owner_thread();
        }
        true
    })
}

#[inline(always)]
pub(crate) fn try_admit_close(fd: OwnedFd) -> CloseAdmission {
    EXECUTOR_CTX.with(|ctx_cell| {
        let owner = ctx_cell.get();
        if owner.is_null() {
            return CloseAdmission::OutsideExecutor(fd);
        }

        unsafe {
            (*owner).debug_assert_owner_thread();
            let state_ptr = (*owner).state_ptr();
            let close_worker = std::ptr::addr_of_mut!((*state_ptr).close_worker);
            let _runtime_state = std::ptr::addr_of_mut!((*state_ptr).runtime_state);
            match (*close_worker).try_admit(fd) {
                Ok(()) => {
                    #[cfg(debug_assertions)]
                    {
                        (*_runtime_state).stats.close_worker_admissions += 1;
                    }
                    CloseAdmission::Admitted
                }
                Err(CloseWorkerRejection::Full(fd)) => {
                    #[cfg(debug_assertions)]
                    {
                        (*_runtime_state).stats.close_worker_full_fallbacks += 1;
                    }
                    CloseAdmission::Full(fd)
                }
                Err(CloseWorkerRejection::Disconnected(fd)) => {
                    #[cfg(debug_assertions)]
                    {
                        (*_runtime_state).stats.close_worker_disconnected_fallbacks += 1;
                    }
                    CloseAdmission::Disconnected(fd)
                }
            }
        }
    })
}

/// Tries to queue one plain socket-close SQE while retaining its sole owner
/// until `io_uring_enter` reports that the matching SQ prefix was consumed.
///
/// This is only used for sockets whose positive-linger state has already been
/// ruled out. The SQE deliberately has no `ASYNC`, `DRAIN`, `LINK`, or fixed
/// file flags.
#[inline(always)]
pub(crate) fn try_submit_close(fd: OwnedFd) -> CloseSubmission {
    EXECUTOR_CTX.with(|ctx_cell| {
        let owner = ctx_cell.get();
        if owner.is_null() {
            return CloseSubmission::OutsideExecutor(fd);
        }

        unsafe {
            (*owner).debug_assert_owner_thread();
            let state_ptr = (*owner).state_ptr();
            let reactor = std::ptr::addr_of_mut!((*state_ptr).reactor);
            let runtime_state = std::ptr::addr_of_mut!((*state_ptr).runtime_state);
            let op = (*reactor).alloc_op();
            if op.is_null() {
                #[cfg(debug_assertions)]
                {
                    (*runtime_state).stats.close_ring_fallbacks += 1;
                }
                return CloseSubmission::Rejected(fd);
            }

            (*op).set_detached();
            match (*reactor).submit_close_sqe(fd, op as u64) {
                Ok(()) => {
                    (*runtime_state).inflight_ops += 1;
                    #[cfg(debug_assertions)]
                    {
                        (*runtime_state).stats.sqe_submits += 1;
                        (*runtime_state).stats.close_ring_submissions += 1;
                    }
                    CloseSubmission::Submitted
                }
                Err((_err, fd)) => {
                    (*reactor).free_op(op);
                    #[cfg(debug_assertions)]
                    {
                        (*runtime_state).stats.close_ring_fallbacks += 1;
                    }
                    CloseSubmission::Rejected(fd)
                }
            }
        }
    })
}

#[inline(always)]
pub(crate) fn note_close_direct() {
    #[cfg(debug_assertions)]
    record_runtime_stat(|stats| stats.close_direct_closes += 1);
}

#[inline(always)]
pub(crate) fn note_accept_readiness_rearm() {
    #[cfg(debug_assertions)]
    record_runtime_stat(|stats| stats.accept_readiness_rearms += 1);
}

#[inline(always)]
pub(crate) fn note_close_linger_query() {
    #[cfg(debug_assertions)]
    record_runtime_stat(|stats| stats.close_linger_queries += 1);
}

#[inline(always)]
pub(crate) fn note_close_linger_classification_failure() {
    #[cfg(debug_assertions)]
    record_runtime_stat(|stats| stats.close_linger_classification_failures += 1);
}

#[inline(always)]
pub(crate) fn note_close_linger_waiver(waived: bool, failed: bool) {
    #[cfg(debug_assertions)]
    record_runtime_stat(|stats| {
        stats.close_linger_waivers += usize::from(waived);
        stats.close_linger_waiver_failures += usize::from(failed);
    });
    #[cfg(not(debug_assertions))]
    let _ = (waived, failed);
}

#[inline(always)]
pub(crate) fn note_waiter_wake() {
    #[cfg(debug_assertions)]
    record_runtime_stat(|stats| stats.waiter_wakes += 1);
}

#[inline(always)]
pub(crate) fn note_timer_now_tick_call() {
    #[cfg(debug_assertions)]
    record_runtime_stat(|stats| stats.timer_now_tick_calls += 1);
}

#[inline(always)]
pub(crate) fn note_timer_expired() {
    #[cfg(debug_assertions)]
    record_runtime_stat(|stats| stats.timer_expired += 1);
}

#[cfg(debug_assertions)]
#[inline(always)]
fn record_runtime_stat(update: impl FnOnce(&mut RuntimeStats)) {
    EXECUTOR_CTX.with(|ctx_cell| {
        let owner = ctx_cell.get();
        if owner.is_null() {
            return;
        }
        unsafe {
            update(&mut (*(*owner).state_ptr()).runtime_state.stats);
        }
    });
}

/// # Safety
///
/// Must be called from within `Executor::run` on the executor thread. The
/// returned pointers are only valid for that run; in release builds a missing
/// context is UB rather than a panic.
#[inline(always)]
pub(crate) unsafe fn schedule_ctx_unchecked() -> ScheduleCtx {
    EXECUTOR_CTX.with(|ctx_cell| {
        let owner = ctx_cell.get();
        debug_assert!(
            !owner.is_null(),
            "runtime schedule_ctx_unchecked requested outside executor context"
        );
        let state = unsafe { (*owner).state_ptr() };
        ScheduleCtx {
            ready_queue: unsafe { std::ptr::addr_of_mut!((*state).ready_queue) },
            runtime_state: unsafe { std::ptr::addr_of_mut!((*state).runtime_state) },
        }
    })
}

#[inline(always)]
/// Schedules a timer-expired task at most once for `wake_epoch`.
///
/// # Safety
///
/// All three pointers must be live and owned by the same active executor.
/// `task_ptr` must remain allocated while its intrusive link may be queued.
pub(crate) unsafe fn schedule_timer_woken_task_unchecked(
    task_ptr: *mut TaskHeader,
    ready_list: *mut DList<TaskHeader>,
    runtime_state: *mut RuntimeState,
    wake_epoch: u64,
) {
    let header = unsafe { &mut *task_ptr };
    if header.last_wake_epoch.get() == wake_epoch {
        return;
    }
    header.last_wake_epoch.set(wake_epoch);
    unsafe {
        notify_task_into_list_unchecked(task_ptr, ready_list, runtime_state);
    }
}

/// # Safety
///
/// `schedule_ctx` must be a context obtained from `schedule_ctx_unchecked()`
/// during the currently active `Executor::run`; its `runtime_state` pointer is
/// dereferenced for read and write.
#[inline(always)]
pub(crate) unsafe fn next_timer_wake_epoch_unchecked(schedule_ctx: ScheduleCtx) -> u64 {
    let current = unsafe { (*schedule_ctx.runtime_state).wake_epoch };
    let mut next = current.wrapping_add(1);
    if next == 0 {
        next = 1;
    }
    unsafe {
        (*schedule_ctx.runtime_state).wake_epoch = next;
    }
    next
}

fn join_task_vtable_for<F>() -> &'static TaskVTable
where
    F: Future + 'static,
    F::Output: 'static,
{
    struct VTableGen<F>(#[doc = "Carries `F` without storage."] std::marker::PhantomData<F>);

    impl<F> VTableGen<F>
    where
        F: Future + 'static,
        F::Output: 'static,
    {
        const VTABLE: TaskVTable = TaskVTable {
            poll: |ptr| {
                let slot = unsafe { &mut *(ptr as *mut Task<TASK_POOL_SIZE>) };
                let jt = unsafe { &mut *(slot.data.as_mut_ptr() as *mut JoinTask<F>) };
                // SAFETY: the scheduler never polls a completed task — the
                // COMPLETED flag prevents re-queuing after finish().
                debug_assert!(jt.future.is_some(), "join task polled after completion");
                let fut = unsafe { jt.future.as_mut().unwrap_unchecked() };
                let mut fut_pin = unsafe { Pin::new_unchecked(fut) };
                let waker = unsafe { cached_waker_ref(ptr) };
                let mut cx = Context::from_waker(waker);
                match fut_pin.as_mut().poll(&mut cx) {
                    Poll::Ready(value) => {
                        jt.future = None;
                        jt.result = Some(Ok(value));
                        if let Some(join_waker) = jt.join_waker.take() {
                            join_waker.wake();
                        }
                        Poll::Ready(())
                    }
                    Poll::Pending => Poll::Pending,
                }
            },
            finish: |ptr| unsafe {
                let slot = &mut *(ptr as *mut Task<TASK_POOL_SIZE>);
                let jt = &mut *(slot.data.as_mut_ptr() as *mut JoinTask<F>);
                // Drop only the future. The result and join_waker must survive
                // for the JoinHandle to consume. They are cleaned up in destroy
                // when the last reference (executor or JoinHandle) is released.
                jt.future = None;
            },
            cancel: |ptr| unsafe {
                let slot = &mut *(ptr as *mut Task<TASK_POOL_SIZE>);
                let jt = &mut *(slot.data.as_mut_ptr() as *mut JoinTask<F>);
                jt.future = None;
                jt.result = Some(Err(JoinError::Cancelled));
                if let Some(join_waker) = jt.join_waker.take() {
                    join_waker.wake();
                }
            },
            destroy: |ptr| unsafe {
                let owner = (*ptr).owner.clone();
                // Drop any remaining JoinTask fields (unclaimed result, waker).
                let slot = &mut *(ptr as *mut Task<TASK_POOL_SIZE>);
                let jt = &mut *(slot.data.as_mut_ptr() as *mut JoinTask<F>);
                std::ptr::drop_in_place(jt);

                if let Some(owner) = owner {
                    let state = owner.state_ptr();
                    let all_link = std::ptr::addr_of_mut!((*ptr).all_link);
                    if !(*all_link).is_unlinked() {
                        (*state).all_tasks.remove(all_link);
                    }
                    #[cfg(debug_assertions)]
                    {
                        (*state).runtime_state.stats.task_frees += 1;
                    }
                    (*state).task_pool.free(ptr as *mut Task<TASK_POOL_SIZE>);
                }
            },
        };
    }

    &VTableGen::<F>::VTABLE
}

/// Routes one task notification through the task's stable executor owner.
///
/// # Safety
///
/// `task_ptr` must point to a live task and run on its owner thread.
unsafe fn schedule_task(task_ptr: *mut TaskHeader) {
    let Some(owner) = (unsafe { (*task_ptr).owner.as_ref() }) else {
        return;
    };
    let state = owner.state_ptr();
    if unsafe { (*state).shutting_down } {
        return;
    }

    unsafe {
        notify_task_into_list_unchecked(
            task_ptr,
            std::ptr::addr_of_mut!((*state).ready_queue),
            std::ptr::addr_of_mut!((*state).runtime_state),
        );
    }
}

/// Routes a reactor waiter notification to the task's stable executor owner.
///
/// The common case keeps the reactor's direct ready-queue path. A waiter from
/// another executor on the same owner thread instead uses that task's recorded
/// owner, so it cannot be linked into or accounted by the reactor's executor.
///
/// # Safety
///
/// `task_ptr` must point to a live task and this must run on its owner thread.
/// `reactor_owner`, `ready_list`, and `runtime_state` must describe the same
/// executor. A null task owner and null reactor owner are permitted together
/// only for standalone unit tests using an explicitly supplied ready list.
#[inline(always)]
pub(crate) unsafe fn notify_reactor_waiter_unchecked(
    task_ptr: *mut TaskHeader,
    reactor_owner: *const ExecutorOwner,
    ready_list: *mut DList<TaskHeader>,
    runtime_state: *mut RuntimeState,
) {
    let task_owner = unsafe {
        (*task_ptr)
            .owner
            .as_ref()
            .map_or(std::ptr::null(), Rc::as_ptr)
    };
    if task_owner == reactor_owner {
        unsafe {
            notify_task_into_list_unchecked(task_ptr, ready_list, runtime_state);
        }
    } else {
        unsafe {
            schedule_task(task_ptr);
        }
    }
}

const TASK_READY_BLOCKING_FLAGS: u64 =
    TaskHeader::FLAG_COMPLETED | TaskHeader::FLAG_RUNNING | TaskHeader::FLAG_QUEUED;

#[inline(always)]
fn task_is_notified(flags: u64) -> bool {
    (flags & TaskHeader::FLAG_NOTIFIED) != 0
}

#[inline(always)]
fn task_is_completed(flags: u64) -> bool {
    (flags & TaskHeader::FLAG_COMPLETED) != 0
}

#[inline(always)]
fn task_can_enter_ready_queue(flags: u64) -> bool {
    (flags & TASK_READY_BLOCKING_FLAGS) == 0
}

#[inline(always)]
/// Reads packed scheduler flags from a live task header.
///
/// # Safety
///
/// `task_ptr` must be non-null, aligned, and live on the owning executor thread.
unsafe fn task_flags_unchecked(task_ptr: *mut TaskHeader) -> u64 {
    unsafe { (*std::ptr::addr_of!((*task_ptr).flags)).get() }
}

#[inline(always)]
/// Adds one scheduler flag to a live task header.
///
/// # Safety
///
/// `task_ptr` must be non-null, aligned, and exclusively scheduler-accessible
/// on the owning executor thread.
unsafe fn set_task_flag_unchecked(task_ptr: *mut TaskHeader, flag: u64) {
    let flags = unsafe { &*std::ptr::addr_of!((*task_ptr).flags) };
    flags.set(flags.get() | flag);
}

#[inline(always)]
/// Records a task notification and queues it when its state permits.
///
/// # Safety
///
/// `task_ptr`, `ready_list`, and `runtime_state` must be live objects owned by
/// the same executor. The task's ready link must be unlinked unless its flags
/// already show it as queued.
pub(crate) unsafe fn notify_task_into_list_unchecked(
    task_ptr: *mut TaskHeader,
    ready_list: *mut DList<TaskHeader>,
    runtime_state: *mut RuntimeState,
) -> bool {
    let flags = unsafe { task_flags_unchecked(task_ptr) };
    if task_is_completed(flags) {
        return false;
    }

    if task_is_notified(flags) {
        return false;
    }
    unsafe { set_task_flag_unchecked(task_ptr, TaskHeader::FLAG_NOTIFIED) };

    if task_can_enter_ready_queue(flags) {
        return unsafe { enqueue_ready_task_unchecked(task_ptr, ready_list, runtime_state) };
    }

    false
}

#[inline(always)]
/// Queues a previously notified task if it is idle and unqueued.
///
/// # Safety
///
/// The task, list, and runtime-state pointers must be live and owned by one
/// executor; the task's intrusive ready link must be unlinked.
unsafe fn enqueue_notified_task_unchecked(
    task_ptr: *mut TaskHeader,
    ready_list: *mut DList<TaskHeader>,
    runtime_state: *mut RuntimeState,
) -> bool {
    let flags = unsafe { task_flags_unchecked(task_ptr) };
    if !task_is_notified(flags) {
        return false;
    }
    if !task_can_enter_ready_queue(flags) {
        return false;
    }

    unsafe { enqueue_ready_task_unchecked(task_ptr, ready_list, runtime_state) }
}

#[inline(always)]
/// Links an eligible task at the tail of the executor ready queue.
///
/// # Safety
///
/// `task_ptr` and `ready_list` must be live and owned by the same executor.
/// The task must not be completed, running, queued, or linked elsewhere.
unsafe fn enqueue_ready_task_unchecked(
    task_ptr: *mut TaskHeader,
    ready_list: *mut DList<TaskHeader>,
    _runtime_state: *mut RuntimeState,
) -> bool {
    let ready_link = unsafe { std::ptr::addr_of_mut!((*task_ptr).ready_link) };
    debug_assert!(
        unsafe { (*ready_link).is_unlinked() },
        "enqueue_ready_task attempted to enqueue an already-linked task"
    );
    debug_assert!(
        task_can_enter_ready_queue(unsafe { task_flags_unchecked(task_ptr) }),
        "enqueue_ready_task attempted to enqueue a completed, running, or already queued task"
    );
    unsafe { set_task_flag_unchecked(task_ptr, TaskHeader::FLAG_QUEUED) };
    #[cfg(debug_assertions)]
    {
        if !_runtime_state.is_null() {
            unsafe {
                (*_runtime_state).stats.task_schedules += 1;
            }
        }
    }
    unsafe {
        (*ready_list).push_back_unchecked(ready_link);
    }
    true
}

#[inline(always)]
/// # Safety
/// - `task_ptr` must point to a live, non-freed `TaskHeader` within the
///   executor's task slab.
/// - Must be called from the executor thread (single-threaded contract).
pub(crate) unsafe fn schedule_woken_task(task_ptr: *mut TaskHeader) {
    unsafe {
        schedule_task(task_ptr);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[cfg(not(miri))]
    use crate::runtime::fd::{RuntimeFd, distinctive_closeable_test_fd, raw_fd_is_closed};
    use std::cell::Cell;
    use std::mem::ManuallyDrop;
    #[cfg(not(miri))]
    use std::os::fd::{AsRawFd, FromRawFd};
    use std::rc::Rc;
    use std::task::{RawWaker, RawWakerVTable};

    #[derive(Default)]
    struct CountedWakerStats {
        clones: Cell<usize>,
        drops: Cell<usize>,
        wakes: Cell<usize>,
    }

    unsafe fn counted_waker_clone(data: *const ()) -> RawWaker {
        let stats = unsafe { Rc::<CountedWakerStats>::from_raw(data.cast()) };
        stats.clones.set(stats.clones.get() + 1);
        let cloned = Rc::clone(&stats);
        let _ = Rc::into_raw(stats);
        RawWaker::new(Rc::into_raw(cloned).cast(), &COUNTED_WAKER_VTABLE)
    }

    unsafe fn counted_waker_wake(data: *const ()) {
        let stats = unsafe { Rc::<CountedWakerStats>::from_raw(data.cast()) };
        stats.wakes.set(stats.wakes.get() + 1);
    }

    unsafe fn counted_waker_wake_by_ref(data: *const ()) {
        let stats = unsafe { Rc::<CountedWakerStats>::from_raw(data.cast()) };
        stats.wakes.set(stats.wakes.get() + 1);
        let _ = Rc::into_raw(stats);
    }

    unsafe fn counted_waker_drop(data: *const ()) {
        let stats = unsafe { Rc::<CountedWakerStats>::from_raw(data.cast()) };
        stats.drops.set(stats.drops.get() + 1);
    }

    static COUNTED_WAKER_VTABLE: RawWakerVTable = RawWakerVTable::new(
        counted_waker_clone,
        counted_waker_wake,
        counted_waker_wake_by_ref,
        counted_waker_drop,
    );

    fn counted_waker(stats: &Rc<CountedWakerStats>) -> Waker {
        let data = Rc::into_raw(Rc::clone(stats)).cast();
        unsafe { Waker::from_raw(RawWaker::new(data, &COUNTED_WAKER_VTABLE)) }
    }

    #[cfg(not(miri))]
    #[test]
    fn close_worker_full_returns_the_unchanged_descriptor_owner() {
        let (sender, receiver) = sync_channel(1);
        let worker = CloseWorker {
            sender: Some(sender),
            worker: None,
        };
        let admitted_raw =
            distinctive_closeable_test_fd().expect("create admitted close-test descriptor");
        let rejected_raw =
            distinctive_closeable_test_fd().expect("create rejected close-test descriptor");
        // SAFETY: each helper result is a distinct, open descriptor whose sole
        // ownership is transferred into one OwnedFd.
        let admitted = unsafe { OwnedFd::from_raw_fd(admitted_raw) };
        let rejected = unsafe { OwnedFd::from_raw_fd(rejected_raw) };

        assert!(
            worker.try_admit(admitted).is_ok(),
            "first descriptor should occupy the bounded queue"
        );
        let returned = match worker.try_admit(rejected) {
            Err(CloseWorkerRejection::Full(fd)) => fd,
            Err(CloseWorkerRejection::Disconnected(_)) => {
                panic!("live undrained receiver should report Full")
            }
            Ok(()) => panic!("second descriptor should exceed capacity"),
        };
        assert_eq!(returned.as_raw_fd(), rejected_raw);
        assert!(
            !raw_fd_is_closed(rejected_raw),
            "full admission must return a still-open sole owner"
        );
        drop(returned);
        assert!(raw_fd_is_closed(rejected_raw));

        let admitted = receiver
            .try_recv()
            .expect("queued descriptor should remain receiver-owned");
        assert_eq!(admitted.as_raw_fd(), admitted_raw);
        drop(admitted);
        assert!(raw_fd_is_closed(admitted_raw));
        drop(worker);
    }

    #[cfg(not(miri))]
    #[test]
    fn close_worker_disconnect_returns_the_unchanged_descriptor_owner() {
        let (sender, receiver) = sync_channel(1);
        drop(receiver);
        let worker = CloseWorker {
            sender: Some(sender),
            worker: None,
        };
        let raw =
            distinctive_closeable_test_fd().expect("create disconnected close-test descriptor");
        // SAFETY: the helper returned one open descriptor with sole ownership.
        let owned = unsafe { OwnedFd::from_raw_fd(raw) };

        let returned = match worker.try_admit(owned) {
            Err(CloseWorkerRejection::Disconnected(fd)) => fd,
            Err(CloseWorkerRejection::Full(_)) => {
                panic!("dropped receiver should report Disconnected")
            }
            Ok(()) => panic!("disconnected worker must reject admission"),
        };
        assert_eq!(returned.as_raw_fd(), raw);
        assert!(
            !raw_fd_is_closed(raw),
            "disconnect must return a still-open sole owner"
        );
        drop(returned);
        assert!(raw_fd_is_closed(raw));
        drop(worker);
    }

    #[cfg(not(miri))]
    #[test]
    fn close_worker_rejects_zero_capacity() {
        match CloseWorker::new(0) {
            Err(err) => assert_eq!(err.kind(), ErrorKind::InvalidInput),
            Ok(_) => panic!("zero-capacity close worker should be rejected"),
        }
    }

    #[cfg(all(debug_assertions, not(miri)))]
    #[test]
    fn rejected_ring_close_returns_owner_for_direct_fallback() {
        let mut executor = Executor::new().expect("executor construction failed");
        let raw = distinctive_closeable_test_fd().expect("distinctive fd failed");

        executor
            .run(async move {
                crate::runtime::test_hooks::fail_next_raw_sqe_submit();
                drop(RuntimeFd::from_fresh_raw_fd(raw));
            })
            .expect("ring-close fallback run failed");

        let stats = executor.last_stats();
        assert_eq!(stats.close_linger_queries, 0);
        assert_eq!(stats.close_ring_submissions, 0);
        assert_eq!(stats.close_ring_fallbacks, 1);
        assert_eq!(stats.close_direct_closes, 1);
        assert!(raw_fd_is_closed(raw));
    }

    #[cfg(not(miri))]
    #[test]
    fn executor_shutdown_routes_pending_task_fd_to_its_worker_and_joins() {
        let mut executor = Executor::new().expect("executor construction failed");
        let raw = distinctive_closeable_test_fd().expect("distinctive fd failed");
        set_positive_linger(raw, 1);

        let err = executor
            .run(async move {
                // SAFETY: the test transfers its sole open descriptor owner.
                let owned = unsafe { OwnedFd::from_raw_fd(raw) };
                let _fd = RuntimeFd::from_external_owned(owned);
                std::future::pending::<()>().await;
            })
            .expect_err("pending task should leave the executor stalled");
        assert_eq!(err.kind(), ErrorKind::WouldBlock);
        assert!(
            !raw_fd_is_closed(raw),
            "pending task must retain its descriptor before shutdown"
        );

        executor.shutdown_owner();
        let state = unsafe { &*executor.owner.state_ptr() };
        assert!(state.close_worker.sender.is_none());
        assert!(state.close_worker.worker.is_none());
        #[cfg(debug_assertions)]
        assert_eq!(state.runtime_state.stats.close_worker_admissions, 1);
        assert!(
            raw_fd_is_closed(raw),
            "joined close worker must retire the pending task descriptor"
        );
    }

    #[cfg(not(miri))]
    #[test]
    fn close_worker_shutdown_guard_joins_after_a_task_destructor_panics() {
        struct PanicAfterClose {
            fd: Option<RuntimeFd>,
        }

        impl Future for PanicAfterClose {
            type Output = ();

            fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
                Poll::Pending
            }
        }

        impl Drop for PanicAfterClose {
            fn drop(&mut self) {
                drop(self.fd.take());
                panic!("intentional task-destructor panic");
            }
        }

        let mut executor = Executor::new().expect("executor construction failed");
        let raw = distinctive_closeable_test_fd().expect("distinctive fd failed");
        set_positive_linger(raw, 1);
        let err = executor
            .run(PanicAfterClose {
                // SAFETY: the test transfers its sole open descriptor owner.
                fd: Some(RuntimeFd::from_external_owned(unsafe {
                    OwnedFd::from_raw_fd(raw)
                })),
            })
            .expect_err("pending task should leave the executor stalled");
        assert_eq!(err.kind(), ErrorKind::WouldBlock);

        let unwind = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            executor.shutdown_owner();
        }));
        assert!(unwind.is_err(), "task destructor should unwind shutdown");

        let state = unsafe { &*executor.owner.state_ptr() };
        assert!(state.close_worker.sender.is_none());
        assert!(state.close_worker.worker.is_none());
        assert!(
            raw_fd_is_closed(raw),
            "unwind guard must join after the descriptor reaches the worker"
        );
    }

    #[cfg(not(miri))]
    #[test]
    fn nested_executor_shutdown_restores_the_active_close_owner() {
        let mut outer = Executor::new().expect("outer executor construction failed");
        let mut inner = Executor::new().expect("inner executor construction failed");
        let inner_raw = distinctive_closeable_test_fd().expect("inner fd failed");
        let outer_raw = distinctive_closeable_test_fd().expect("outer fd failed");
        set_positive_linger(inner_raw, 1);
        set_positive_linger(outer_raw, 1);

        let err = inner
            .run(async move {
                // SAFETY: the test transfers its sole open descriptor owner.
                let owned = unsafe { OwnedFd::from_raw_fd(inner_raw) };
                let _fd = RuntimeFd::from_external_owned(owned);
                std::future::pending::<()>().await;
            })
            .expect_err("inner pending task should stall");
        assert_eq!(err.kind(), ErrorKind::WouldBlock);

        outer
            .run(async move {
                drop(inner);
                // SAFETY: the test transfers its sole open descriptor owner.
                let owned = unsafe { OwnedFd::from_raw_fd(outer_raw) };
                drop(RuntimeFd::from_external_owned(owned));
            })
            .expect("outer executor run failed");

        #[cfg(debug_assertions)]
        let outer_state = unsafe { &*outer.owner.state_ptr() };
        #[cfg(debug_assertions)]
        assert_eq!(
            outer_state.runtime_state.stats.close_worker_admissions, 1,
            "post-inner-drop descriptor must route back to the outer worker"
        );
        assert!(raw_fd_is_closed(inner_raw));
        outer.shutdown_owner();
        assert!(raw_fd_is_closed(outer_raw));
    }

    #[cfg(not(miri))]
    const CLOSE_LINGER_CHILD_ENV: &str = "FLOWIO_CLOSE_LINGER_CHILD";
    #[cfg(not(miri))]
    const CLOSE_LINGER_CHILD_TEST: &str =
        "runtime::executor::tests::close_worker_full_fallback_waives_positive_linger_child";

    #[cfg(not(miri))]
    fn replace_with_inert_close_worker(
        executor: &mut Executor,
        capacity: usize,
    ) -> Receiver<OwnedFd> {
        let state = unsafe { &mut *executor.owner.state_ptr() };
        state.close_worker.shutdown();
        let (sender, receiver) = sync_channel(capacity);
        state.close_worker = CloseWorker {
            sender: Some(sender),
            worker: None,
        };
        receiver
    }

    #[cfg(not(miri))]
    fn replace_with_disconnected_close_worker(executor: &mut Executor) {
        let state = unsafe { &mut *executor.owner.state_ptr() };
        state.close_worker.shutdown();
        let (sender, receiver) = sync_channel(1);
        drop(receiver);
        state.close_worker = CloseWorker {
            sender: Some(sender),
            worker: None,
        };
    }

    #[cfg(not(miri))]
    fn set_positive_linger(fd: std::os::fd::RawFd, seconds: libc::c_int) {
        let linger = libc::linger {
            l_onoff: 1,
            l_linger: seconds,
        };
        // SAFETY: linger is initialized and borrowed for the exact option
        // byte count during this call.
        let rc = unsafe {
            libc::setsockopt(
                fd,
                libc::SOL_SOCKET,
                libc::SO_LINGER,
                std::ptr::addr_of!(linger).cast(),
                std::mem::size_of::<libc::linger>() as libc::socklen_t,
            )
        };
        assert_eq!(rc, 0, "set positive SO_LINGER failed");
    }

    #[cfg(not(miri))]
    fn tcp_owner_with_pending_data_and_linger() -> (OwnedFd, std::net::TcpStream) {
        let listener = std::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0))
            .expect("bind linger-test listener");
        let addr = listener.local_addr().expect("linger-test local address");
        let client = std::net::TcpStream::connect(addr).expect("connect linger-test client");
        let (peer, _) = listener.accept().expect("accept linger-test peer");
        client
            .set_nonblocking(true)
            .expect("set linger-test client nonblocking");

        for fd in [client.as_raw_fd(), peer.as_raw_fd()] {
            let small_buffer: libc::c_int = 4096;
            // SAFETY: small_buffer is initialized and borrowed for the exact
            // integer socket-option size during each call.
            let rc = unsafe {
                libc::setsockopt(
                    fd,
                    libc::SOL_SOCKET,
                    if fd == client.as_raw_fd() {
                        libc::SO_SNDBUF
                    } else {
                        libc::SO_RCVBUF
                    },
                    std::ptr::addr_of!(small_buffer).cast(),
                    std::mem::size_of::<libc::c_int>() as libc::socklen_t,
                )
            };
            assert_eq!(rc, 0, "set linger-test socket buffer failed");
        }

        let bytes = [0x5au8; 64 * 1024];
        let mut sent_total = 0usize;
        let mut reached_backpressure = false;
        for _ in 0..4096 {
            // SAFETY: bytes remains readable for the duration of this
            // nonblocking send and client is a live stream descriptor.
            let sent = unsafe {
                libc::send(
                    client.as_raw_fd(),
                    bytes.as_ptr().cast(),
                    bytes.len(),
                    libc::MSG_DONTWAIT | libc::MSG_NOSIGNAL,
                )
            };
            if sent > 0 {
                sent_total += sent as usize;
                continue;
            }
            if sent == -1 {
                let err = io::Error::last_os_error();
                if err.kind() == ErrorKind::WouldBlock {
                    reached_backpressure = true;
                    break;
                }
                if err.kind() == ErrorKind::Interrupted {
                    continue;
                }
                panic!("fill linger-test send queue failed: {err}");
            }
            panic!("linger-test send unexpectedly returned zero");
        }
        assert!(sent_total > 0, "linger-test client sent no data");
        assert!(
            reached_backpressure,
            "linger-test client never reached send backpressure"
        );

        let mut queued: libc::c_int = 0;
        // SAFETY: queued is writable for the integer result and client is a
        // live socket descriptor.
        let rc = unsafe { libc::ioctl(client.as_raw_fd(), libc::TIOCOUTQ, &mut queued) };
        assert_eq!(rc, 0, "TIOCOUTQ failed");
        assert!(queued > 0, "linger-test socket had no unsent bytes");

        set_positive_linger(client.as_raw_fd(), 3);
        let mut observed = libc::linger {
            l_onoff: 0,
            l_linger: 0,
        };
        let mut observed_len = std::mem::size_of::<libc::linger>() as libc::socklen_t;
        // SAFETY: observed and observed_len provide exact writable option
        // storage for this query.
        let rc = unsafe {
            libc::getsockopt(
                client.as_raw_fd(),
                libc::SOL_SOCKET,
                libc::SO_LINGER,
                std::ptr::addr_of_mut!(observed).cast(),
                std::ptr::addr_of_mut!(observed_len),
            )
        };
        assert_eq!(rc, 0, "read positive SO_LINGER failed");
        assert_ne!(observed.l_onoff, 0);
        assert_eq!(observed.l_linger, 3);

        (client.into(), peer)
    }

    #[cfg(not(miri))]
    #[test]
    fn disconnected_close_worker_waives_positive_linger_before_fallback() {
        let mut executor = Executor::new().expect("executor construction failed");
        replace_with_disconnected_close_worker(&mut executor);

        let raw = distinctive_closeable_test_fd().expect("distinctive fd failed");
        set_positive_linger(raw, 2);
        // SAFETY: the helper returned one open descriptor whose sole ownership
        // moves into this test owner.
        let owner = unsafe { OwnedFd::from_raw_fd(raw) };

        executor
            .run(async move {
                drop(RuntimeFd::from_external_owned(owner));
            })
            .expect("disconnected close fallback run failed");
        #[cfg(debug_assertions)]
        {
            let stats = executor.last_stats();
            assert_eq!(stats.close_worker_admissions, 0);
            assert_eq!(stats.close_worker_full_fallbacks, 0);
            assert_eq!(stats.close_worker_disconnected_fallbacks, 1);
            assert_eq!(stats.close_linger_waivers, 1);
            assert_eq!(stats.close_linger_waiver_failures, 0);
            assert_eq!(stats.close_direct_closes, 1);
        }
        assert!(raw_fd_is_closed(raw));
    }

    #[cfg(not(miri))]
    #[test]
    fn close_worker_full_fallback_waives_positive_linger_child() {
        if std::env::var_os(CLOSE_LINGER_CHILD_ENV).is_none() {
            return;
        }

        let mut executor = Executor::new_with_config(ExecutorConfig {
            reactor: ReactorConfig { ring_entries: 1 },
            ..ExecutorConfig::default()
        })
        .expect("construct close-linger executor");
        let receiver = replace_with_inert_close_worker(&mut executor, 1);
        let filler_raw = distinctive_closeable_test_fd().expect("create close-queue filler");
        set_positive_linger(filler_raw, 1);
        let (linger_owner, _peer) = tcp_owner_with_pending_data_and_linger();
        let linger_raw = linger_owner.as_raw_fd();

        let started = std::time::Instant::now();
        executor
            .run(async move {
                // SAFETY: the test transfers its sole open descriptor owner.
                let filler = unsafe { OwnedFd::from_raw_fd(filler_raw) };
                drop(RuntimeFd::from_external_owned(filler));
                drop(RuntimeFd::from_external_owned(linger_owner));
            })
            .expect("close-linger executor run failed");
        let elapsed = started.elapsed();
        assert!(
            elapsed < std::time::Duration::from_millis(750),
            "full fallback honored positive linger on the owner thread: {elapsed:?}"
        );
        assert!(raw_fd_is_closed(linger_raw));

        #[cfg(debug_assertions)]
        {
            let stats = executor.last_stats();
            assert_eq!(stats.close_worker_admissions, 1);
            assert_eq!(stats.close_worker_full_fallbacks, 1);
            assert_eq!(stats.close_worker_disconnected_fallbacks, 0);
            assert_eq!(stats.close_linger_waivers, 1);
            assert_eq!(stats.close_linger_waiver_failures, 0);
            assert_eq!(stats.close_direct_closes, 1);
        }

        let filler = receiver
            .try_recv()
            .expect("admitted filler should remain queued");
        assert_eq!(filler.as_raw_fd(), filler_raw);
        drop(filler);
        assert!(raw_fd_is_closed(filler_raw));
        drop(receiver);
        executor.shutdown_owner();
    }

    #[cfg(not(miri))]
    #[test]
    fn close_worker_full_fallback_positive_linger_has_process_watchdog() {
        use std::process::{Command, Stdio};

        let current_exe = std::env::current_exe().expect("current unit-test executable");
        let mut child = Command::new(current_exe)
            .args(["--exact", CLOSE_LINGER_CHILD_TEST, "--nocapture"])
            .env(CLOSE_LINGER_CHILD_ENV, "1")
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("spawn positive-linger child");
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(8);

        loop {
            if let Some(_status) = child.try_wait().expect("poll positive-linger child") {
                let output = child
                    .wait_with_output()
                    .expect("collect positive-linger child output");
                assert!(
                    output.status.success(),
                    "positive-linger child failed: status={:?}, stdout={}, stderr={}",
                    output.status,
                    String::from_utf8_lossy(&output.stdout),
                    String::from_utf8_lossy(&output.stderr)
                );
                break;
            }
            if std::time::Instant::now() >= deadline {
                let _ = child.kill();
                let output = child
                    .wait_with_output()
                    .expect("reap timed-out positive-linger child");
                panic!(
                    "positive-linger child exceeded watchdog; stdout={}, stderr={}",
                    String::from_utf8_lossy(&output.stdout),
                    String::from_utf8_lossy(&output.stderr)
                );
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
    }

    #[repr(align(16))]
    struct Align16Payload;

    struct OveralignedSpawnFuture {
        id: usize,
        drops: Rc<Cell<usize>>,
        polls: Rc<Cell<usize>>,
        _payload: Align16Payload,
    }

    impl OveralignedSpawnFuture {
        fn new(id: usize, drops: &Rc<Cell<usize>>, polls: &Rc<Cell<usize>>) -> Self {
            Self {
                id,
                drops: Rc::clone(drops),
                polls: Rc::clone(polls),
                _payload: Align16Payload,
            }
        }
    }

    impl Future for OveralignedSpawnFuture {
        type Output = usize;

        fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
            let this = self.get_mut();
            this.polls.set(this.polls.get() + 1);
            Poll::Ready(this.id)
        }
    }

    impl Drop for OveralignedSpawnFuture {
        fn drop(&mut self) {
            self.drops.set(self.drops.get() + 1);
        }
    }

    #[cfg(not(miri))]
    struct GateFuture {
        id: usize,
        release: Rc<Cell<bool>>,
        drops: Rc<Cell<usize>>,
        polls: Rc<Cell<usize>>,
    }

    #[cfg(not(miri))]
    impl GateFuture {
        fn new(
            id: usize,
            release: &Rc<Cell<bool>>,
            drops: &Rc<Cell<usize>>,
            polls: &Rc<Cell<usize>>,
        ) -> Self {
            Self {
                id,
                release: Rc::clone(release),
                drops: Rc::clone(drops),
                polls: Rc::clone(polls),
            }
        }
    }

    #[cfg(not(miri))]
    impl Future for GateFuture {
        type Output = usize;

        fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
            let this = self.get_mut();
            this.polls.set(this.polls.get() + 1);
            if this.release.get() {
                Poll::Ready(this.id)
            } else {
                Poll::Pending
            }
        }
    }

    #[cfg(not(miri))]
    impl Drop for GateFuture {
        fn drop(&mut self) {
            self.drops.set(self.drops.get() + 1);
        }
    }

    #[test]
    fn try_spawn_rejects_overaligned_future_before_task_slot_write() {
        assert!(size_of::<JoinTask<OveralignedSpawnFuture>>() <= TASK_POOL_SIZE);
        assert!(align_of::<JoinTask<OveralignedSpawnFuture>>() > TASK_DATA_ALIGN);

        let drops = Rc::new(Cell::new(0usize));
        let polls = Rc::new(Cell::new(0usize));
        // The over-aligned branch returns before dereferencing the non-null
        // owner pointer; this keeps the layout regression runnable under Miri
        // without constructing an io_uring-backed executor.
        let owner = std::ptr::NonNull::<ExecutorOwner>::dangling().as_ptr();
        let _guard = ExecutorCtxGuard::install(owner)
            .expect("test executor context should install on an idle thread");

        let future = OveralignedSpawnFuture::new(313, &drops, &polls);
        let returned = match Executor::try_spawn(future) {
            Ok(_) => panic!("over-aligned task should not spawn"),
            Err(TrySpawnError::TaskTooLarge { future }) => future,
            Err(_) => panic!("over-aligned task returned the wrong failure class"),
        };

        assert_eq!(returned.id, 313);
        assert_eq!(polls.get(), 0, "rejected future must not be polled");
        assert_eq!(
            drops.get(),
            0,
            "rejected future must be returned before being dropped"
        );
        drop(returned);
        assert_eq!(drops.get(), 1);
    }

    #[test]
    fn join_handle_pending_poll_reuses_same_waker() {
        let mut result = None::<Result<usize, JoinError>>;
        let mut waker_slot = None::<Waker>;
        let mut handle = ManuallyDrop::new(JoinHandle {
            task_ptr: std::ptr::null_mut(),
            result_ptr: &mut result,
            waker_ptr: &mut waker_slot,
        });

        let first_stats = Rc::new(CountedWakerStats::default());
        let first_waker = counted_waker(&first_stats);
        let mut first_cx = Context::from_waker(&first_waker);

        assert!(
            unsafe { Pin::new_unchecked(&mut *handle) }
                .poll(&mut first_cx)
                .is_pending()
        );
        assert_eq!(first_stats.clones.get(), 1);
        assert_eq!(first_stats.drops.get(), 0);

        assert!(
            unsafe { Pin::new_unchecked(&mut *handle) }
                .poll(&mut first_cx)
                .is_pending()
        );
        assert_eq!(
            first_stats.clones.get(),
            1,
            "same waiter should not be cloned again"
        );
        assert_eq!(
            first_stats.drops.get(),
            0,
            "same waiter should not replace the stored waker"
        );

        let second_stats = Rc::new(CountedWakerStats::default());
        let second_waker = counted_waker(&second_stats);
        let mut second_cx = Context::from_waker(&second_waker);

        assert!(
            unsafe { Pin::new_unchecked(&mut *handle) }
                .poll(&mut second_cx)
                .is_pending()
        );
        assert_eq!(
            first_stats.drops.get(),
            1,
            "different waiter should replace the previous stored waker"
        );
        assert_eq!(second_stats.clones.get(), 1);
        assert!(
            waker_slot
                .as_ref()
                .expect("join waker should be stored")
                .will_wake(&second_waker)
        );

        drop(waker_slot.take());
    }

    #[cfg(not(miri))]
    fn executor_with_one_task_slab() -> Executor {
        let executor = Executor::new().expect("failed to construct executor");
        unsafe {
            (*executor.owner.state_ptr())
                .task_pool
                .set_provider_max_request_count(Some(1));
        }
        executor
    }

    #[test]
    fn task_ready_gate_matrix_blocks_only_terminal_or_active_queue_states() {
        assert!(task_can_enter_ready_queue(0));
        assert!(task_can_enter_ready_queue(TaskHeader::FLAG_NOTIFIED));
        assert!(!task_can_enter_ready_queue(TaskHeader::FLAG_COMPLETED));
        assert!(!task_can_enter_ready_queue(TaskHeader::FLAG_RUNNING));
        assert!(!task_can_enter_ready_queue(TaskHeader::FLAG_QUEUED));
        assert!(!task_can_enter_ready_queue(
            TaskHeader::FLAG_NOTIFIED | TaskHeader::FLAG_RUNNING
        ));
        assert!(!task_can_enter_ready_queue(
            TaskHeader::FLAG_NOTIFIED | TaskHeader::FLAG_QUEUED
        ));
    }

    #[test]
    fn notify_task_gate_enqueues_idle_task_once() {
        let mut ready_queue = DList::new_uninit();
        ready_queue.init();
        let mut runtime_state = RuntimeState::new();
        let mut header = TaskHeader::new();
        let task_ptr = &mut header as *mut TaskHeader;

        assert!(unsafe {
            notify_task_into_list_unchecked(task_ptr, &mut ready_queue, &mut runtime_state)
        });
        assert!(unsafe { (*task_ptr).has_flag(TaskHeader::FLAG_NOTIFIED) });
        assert!(unsafe { (*task_ptr).has_flag(TaskHeader::FLAG_QUEUED) });
        #[cfg(debug_assertions)]
        assert_eq!(runtime_state.stats.task_schedules, 1);

        assert!(!unsafe {
            notify_task_into_list_unchecked(task_ptr, &mut ready_queue, &mut runtime_state)
        });
        #[cfg(debug_assertions)]
        assert_eq!(runtime_state.stats.task_schedules, 1);

        let popped = unsafe { ready_queue.pop_front(TaskHeader::READY_LINK_OFFSET) };
        assert_eq!(popped, Some(task_ptr));
        assert!(ready_queue.is_empty());
    }

    #[test]
    fn reactor_waiter_notification_keeps_matching_owner_direct_path() {
        let mut ready_queue = DList::new_uninit();
        ready_queue.init();
        let mut runtime_state = RuntimeState::new();
        let mut header = TaskHeader::new();
        let task_ptr = &mut header as *mut TaskHeader;

        unsafe {
            notify_reactor_waiter_unchecked(
                task_ptr,
                std::ptr::null(),
                &mut ready_queue,
                &mut runtime_state,
            );
        }

        assert!(unsafe { (*task_ptr).has_flag(TaskHeader::FLAG_NOTIFIED) });
        assert!(unsafe { (*task_ptr).has_flag(TaskHeader::FLAG_QUEUED) });
        #[cfg(debug_assertions)]
        assert_eq!(runtime_state.stats.task_schedules, 1);
        assert_eq!(
            unsafe { ready_queue.pop_front(TaskHeader::READY_LINK_OFFSET) },
            Some(task_ptr)
        );
        assert!(ready_queue.is_empty());
    }

    #[test]
    fn notify_task_gate_defers_running_task_until_poll_finishes() {
        let mut ready_queue = DList::new_uninit();
        ready_queue.init();
        let mut runtime_state = RuntimeState::new();
        let mut header = TaskHeader::new();
        let task_ptr = &mut header as *mut TaskHeader;
        unsafe { (*task_ptr).set_flag(TaskHeader::FLAG_RUNNING) };

        assert!(!unsafe {
            notify_task_into_list_unchecked(task_ptr, &mut ready_queue, &mut runtime_state)
        });
        assert!(unsafe { (*task_ptr).has_flag(TaskHeader::FLAG_NOTIFIED) });
        assert!(!unsafe { (*task_ptr).has_flag(TaskHeader::FLAG_QUEUED) });
        assert!(ready_queue.is_empty());
        #[cfg(debug_assertions)]
        assert_eq!(runtime_state.stats.task_schedules, 0);

        unsafe { (*task_ptr).clear_flag(TaskHeader::FLAG_RUNNING) };
        assert!(unsafe {
            enqueue_notified_task_unchecked(task_ptr, &mut ready_queue, &mut runtime_state)
        });
        assert!(unsafe { (*task_ptr).has_flag(TaskHeader::FLAG_QUEUED) });
        #[cfg(debug_assertions)]
        assert_eq!(runtime_state.stats.task_schedules, 1);

        let popped = unsafe { ready_queue.pop_front(TaskHeader::READY_LINK_OFFSET) };
        assert_eq!(popped, Some(task_ptr));
        assert!(ready_queue.is_empty());
    }

    #[test]
    fn notify_task_gate_ignores_completed_tasks() {
        let mut ready_queue = DList::new_uninit();
        ready_queue.init();
        let mut runtime_state = RuntimeState::new();
        let mut header = TaskHeader::new();
        let task_ptr = &mut header as *mut TaskHeader;
        unsafe { (*task_ptr).set_flag(TaskHeader::FLAG_COMPLETED) };

        assert!(!unsafe {
            notify_task_into_list_unchecked(task_ptr, &mut ready_queue, &mut runtime_state)
        });
        assert!(!unsafe { (*task_ptr).has_flag(TaskHeader::FLAG_NOTIFIED) });
        assert!(!unsafe { (*task_ptr).has_flag(TaskHeader::FLAG_QUEUED) });
        assert!(ready_queue.is_empty());
        #[cfg(debug_assertions)]
        assert_eq!(runtime_state.stats.task_schedules, 0);
    }

    #[test]
    fn enqueue_notified_gate_requires_notified_idle_task() {
        let mut ready_queue = DList::new_uninit();
        ready_queue.init();
        let mut runtime_state = RuntimeState::new();
        let mut header = TaskHeader::new();
        let task_ptr = &mut header as *mut TaskHeader;

        assert!(!unsafe {
            enqueue_notified_task_unchecked(task_ptr, &mut ready_queue, &mut runtime_state)
        });
        assert!(ready_queue.is_empty());

        unsafe { (*task_ptr).set_flag(TaskHeader::FLAG_NOTIFIED | TaskHeader::FLAG_QUEUED) };
        assert!(!unsafe {
            enqueue_notified_task_unchecked(task_ptr, &mut ready_queue, &mut runtime_state)
        });
        assert!(ready_queue.is_empty());

        unsafe { (*task_ptr).clear_flag(TaskHeader::FLAG_QUEUED) };
        assert!(unsafe {
            enqueue_notified_task_unchecked(task_ptr, &mut ready_queue, &mut runtime_state)
        });
        assert!(unsafe { (*task_ptr).has_flag(TaskHeader::FLAG_QUEUED) });

        let popped = unsafe { ready_queue.pop_front(TaskHeader::READY_LINK_OFFSET) };
        assert_eq!(popped, Some(task_ptr));
        assert!(ready_queue.is_empty());
    }

    #[test]
    #[cfg(not(miri))]
    fn executor_drop_unlinks_residual_ready_queue_entries() {
        let mut executor = Executor::new().expect("failed to construct executor");
        executor.init().expect("executor init failed");

        let mut header = TaskHeader::new();
        unsafe {
            (*executor.owner.state_ptr())
                .ready_queue
                .push_back(&mut header.ready_link as *mut _);
        }

        drop(executor);

        assert!(
            header.ready_link.is_unlinked(),
            "executor drop should unlink abandoned ready-queue entries"
        );
    }

    #[test]
    #[cfg(not(miri))]
    fn try_spawn_at_capacity_returns_future_without_drop() {
        let mut executor = executor_with_one_task_slab();
        let release = Rc::new(Cell::new(false));
        let filler_drops = Rc::new(Cell::new(0usize));
        let filler_polls = Rc::new(Cell::new(0usize));
        let rejected_drops = Rc::new(Cell::new(0usize));
        let rejected_polls = Rc::new(Cell::new(0usize));
        let returned_id = Rc::new(Cell::new(0usize));
        let release_flag = release.clone();
        let filler_drops_flag = filler_drops.clone();
        let filler_polls_flag = filler_polls.clone();
        let rejected_drops_flag = rejected_drops.clone();
        let rejected_polls_flag = rejected_polls.clone();
        let returned_id_flag = returned_id.clone();

        executor
            .run(async move {
                for id in 0..(TASKS_PER_SLAB - 1) {
                    let handle = match Executor::try_spawn(GateFuture::new(
                        id,
                        &release_flag,
                        &filler_drops_flag,
                        &filler_polls_flag,
                    )) {
                        Ok(handle) => handle,
                        Err(_) => panic!("filler task should fit in the first task slab"),
                    };
                    drop(handle);
                }

                let rejected = GateFuture::new(
                    777,
                    &release_flag,
                    &rejected_drops_flag,
                    &rejected_polls_flag,
                );
                let returned = match Executor::try_spawn(rejected) {
                    Ok(_) => panic!("task pool should be at capacity"),
                    Err(TrySpawnError::AtCapacity { future }) => future,
                    Err(_) => panic!("task pool exhaustion returned the wrong failure class"),
                };

                returned_id_flag.set(returned.id);
                assert_eq!(
                    rejected_polls_flag.get(),
                    0,
                    "AtCapacity future must not be polled"
                );
                assert_eq!(
                    rejected_drops_flag.get(),
                    0,
                    "AtCapacity future must be returned before being dropped"
                );
                drop(returned);

                release_flag.set(true);
            })
            .expect("executor run failed");

        assert_eq!(returned_id.get(), 777);
        assert_eq!(rejected_polls.get(), 0);
        assert_eq!(rejected_drops.get(), 1);
        assert_eq!(filler_drops.get(), TASKS_PER_SLAB - 1);
        assert!(filler_polls.get() >= TASKS_PER_SLAB - 1);
    }

    #[test]
    #[cfg(not(miri))]
    fn spawn_at_capacity_returns_io_error() {
        let mut executor = executor_with_one_task_slab();
        let release = Rc::new(Cell::new(false));
        let filler_drops = Rc::new(Cell::new(0usize));
        let filler_polls = Rc::new(Cell::new(0usize));
        let rejected_drops = Rc::new(Cell::new(0usize));
        let rejected_polls = Rc::new(Cell::new(0usize));
        let release_flag = release.clone();
        let filler_drops_flag = filler_drops.clone();
        let filler_polls_flag = filler_polls.clone();
        let rejected_drops_flag = rejected_drops.clone();
        let rejected_polls_flag = rejected_polls.clone();

        executor
            .run(async move {
                for id in 0..(TASKS_PER_SLAB - 1) {
                    let handle = match Executor::spawn(GateFuture::new(
                        id,
                        &release_flag,
                        &filler_drops_flag,
                        &filler_polls_flag,
                    )) {
                        Ok(handle) => handle,
                        Err(err) => panic!("filler task should fit in the first task slab: {err}"),
                    };
                    drop(handle);
                }

                let err = match Executor::spawn(GateFuture::new(
                    888,
                    &release_flag,
                    &rejected_drops_flag,
                    &rejected_polls_flag,
                )) {
                    Ok(_) => panic!("task pool should be at capacity"),
                    Err(err) => err,
                };
                assert_eq!(err.kind(), ErrorKind::OutOfMemory);
                assert_eq!(
                    rejected_polls_flag.get(),
                    0,
                    "legacy spawn AtCapacity future must not be polled"
                );
                assert_eq!(
                    rejected_drops_flag.get(),
                    1,
                    "legacy spawn consumes the rejected future"
                );

                release_flag.set(true);
            })
            .expect("executor run failed");

        assert_eq!(rejected_polls.get(), 0);
        assert_eq!(rejected_drops.get(), 1);
        assert_eq!(filler_drops.get(), TASKS_PER_SLAB - 1);
        assert!(filler_polls.get() >= TASKS_PER_SLAB - 1);
    }
}
