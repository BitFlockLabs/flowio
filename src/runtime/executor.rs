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
use crate::runtime::retained::RetainedPayload;
use crate::runtime::task::{
    Task, TaskHeader, TaskVTable, cached_waker_ref, init_cached_waker, release_task,
    task_ptr_from_waker,
};
use crate::runtime::timer::TimerRuntime;
use crate::utils::disarm_unwind_guard;
use crate::utils::list::intrusive::dlist::DList;
use crate::utils::memory::provider::MemoryProvider;
use crate::utils::memory::provider_owned_pool::{ProviderOwnedPool, ProviderOwnedPoolControl};
use io_uring::squeue;
use std::alloc::{Layout, alloc};
use std::any::Any;
use std::cell::{Cell, UnsafeCell};
use std::future::Future;
use std::io;
use std::io::ErrorKind;
use std::mem::{align_of, size_of};
use std::os::fd::OwnedFd;
use std::panic::{AssertUnwindSafe, catch_unwind, resume_unwind};
use std::pin::Pin;
use std::ptr::NonNull;
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
const _: () = assert!(std::mem::offset_of!(Task<TASK_POOL_SIZE>, data) % TASK_DATA_ALIGN == 0);

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
        /// let _stats = RuntimeStats::default();
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
            /// Successful FlowIO task poll-context extractions.
            #[cfg(debug_assertions)]
            pub poll_context_extractions: usize,
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
    #[cfg(debug_assertions)]
    /// Debug-only scheduler and allocation counters.
    pub(crate) stats: RuntimeStats,
}

impl RuntimeState {
    fn new() -> Self {
        Self {
            live_tasks: 0,
            inflight_ops: 0,
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
/// any earlier executor shutdown phase unwinds.
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

pub(super) type PanicPayload = Box<dyn Any + Send + 'static>;

/// Owns the executor's reference while a task completes or is cancelled.
///
/// Taking the pointer before release prevents a panic from the final task
/// destructor from causing a second release while this guard unwinds.
struct ExecutorTaskRefGuard {
    task: *mut TaskHeader,
}

impl ExecutorTaskRefGuard {
    #[inline(always)]
    fn new(task: *mut TaskHeader) -> Self {
        Self { task }
    }

    #[inline(always)]
    fn release(mut self) {
        let task = std::mem::replace(&mut self.task, std::ptr::null_mut());
        if !task.is_null() {
            // SAFETY: the task lifecycle transition transfers exactly the
            // executor-owned reference for this live task into the guard.
            unsafe {
                release_task(task);
            }
        }
    }
}

impl Drop for ExecutorTaskRefGuard {
    #[inline(always)]
    fn drop(&mut self) {
        if !self.task.is_null() {
            // SAFETY: a non-null pointer still represents the one
            // executor-owned reference transferred into this guard.
            unsafe {
                release_task(self.task);
            }
            self.task = std::ptr::null_mut();
        }
    }
}

/// Terminalizes one task if its type-erased poll hook unwinds.
///
/// Normal polls consume this guard with [`TaskPollPanicGuard::disarm`], which
/// compiles to no state test. Only the unwind landing pad invokes `Drop`.
struct TaskPollPanicGuard {
    task: *mut TaskHeader,
    runtime_state: *mut RuntimeState,
}

impl TaskPollPanicGuard {
    #[inline(always)]
    fn new(task: *mut TaskHeader, runtime_state: *mut RuntimeState) -> Self {
        Self {
            task,
            runtime_state,
        }
    }

    /// Suppresses exceptional cleanup after the poll hook returns normally.
    #[inline(always)]
    fn disarm(self) {
        std::mem::forget(self);
    }
}

impl Drop for TaskPollPanicGuard {
    #[cold]
    #[inline(never)]
    fn drop(&mut self) {
        let task = self.task;
        let runtime_state = self.runtime_state;
        // SAFETY: the scheduler installs this guard only after popping one
        // live task and marking it RUNNING. Executor::run keeps RuntimeState
        // and the executor-owned task reference alive through guard cleanup.
        let cleanup_panic = unsafe { cancel_task_and_release_executor_ref(task, runtime_state) };
        if let Some(payload) = cleanup_panic {
            // A cleanup panic is secondary to the active poll unwind. Its
            // payload may itself have a panicking destructor, so neither
            // resuming nor dropping it is safe here.
            std::mem::forget(payload);
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
    /// Set after one-time intrusive/runtime initialization is complete.
    initialized: bool,
    /// Prevents teardown wakeups from re-entering the ready queue.
    shutting_down: bool,
    /// Set only after tasks, timers, reactor, and close worker all shut down.
    shutdown_complete: bool,
}

/// Cancels timers after task shutdown.
struct TimerShutdownPhase {
    state: *mut ExecutorState,
}

impl Drop for TimerShutdownPhase {
    fn drop(&mut self) {
        let state = self.state;
        run_cleanup_preserving_panic(|| {
            // SAFETY: shutdown_owner creates this phase from its heap-stable
            // ExecutorState and keeps that state alive until after phase drop.
            unsafe {
                TimerRuntime::cancel_all_for_shutdown_unchecked(std::ptr::addr_of_mut!(
                    (*state).timers
                ));
            }
        });
    }
}

/// Retires reactor work after timer cancellation.
struct ReactorShutdownPhase {
    state: *mut ExecutorState,
}

impl Drop for ReactorShutdownPhase {
    fn drop(&mut self) {
        let state = self.state;
        run_cleanup_preserving_panic(|| {
            // SAFETY: shutdown_owner creates this phase from its heap-stable
            // ExecutorState and keeps that state alive until after phase drop.
            unsafe {
                let runtime_state = std::ptr::addr_of_mut!((*state).runtime_state);
                let ready_queue = std::ptr::addr_of_mut!((*state).ready_queue);
                Reactor::shutdown_unchecked(
                    std::ptr::addr_of_mut!((*state).reactor),
                    runtime_state,
                    ready_queue,
                );
            }
        });
    }
}

/// Completes timer cancellation and reactor retirement after task shutdown.
///
/// Rust drops struct fields in declaration order. Keeping the timer phase
/// before the reactor phase ensures reactor retirement still runs if timer
/// waiter destruction unwinds.
struct RuntimeShutdownGuard {
    _timer: TimerShutdownPhase,
    _reactor: ReactorShutdownPhase,
}

impl RuntimeShutdownGuard {
    fn new(state: *mut ExecutorState) -> Self {
        Self {
            _timer: TimerShutdownPhase { state },
            _reactor: ReactorShutdownPhase { state },
        }
    }
}

/// Latches one completed shutdown even when an earlier teardown phase unwinds.
struct ShutdownCompleteGuard {
    state: *mut ExecutorState,
}

impl ShutdownCompleteGuard {
    fn new(state: *mut ExecutorState) -> Self {
        Self { state }
    }
}

impl Drop for ShutdownCompleteGuard {
    fn drop(&mut self) {
        // SAFETY: shutdown_owner creates this guard from its heap-stable state
        // and keeps that state alive until after guard drop.
        unsafe {
            (*self.state).shutdown_complete = true;
        }
    }
}

/// Runs one cleanup action without allowing a secondary panic to abort an
/// already-unwinding thread.
fn run_cleanup_preserving_panic(cleanup: impl FnOnce()) {
    let already_panicking = std::thread::panicking();
    if let Err(payload) = catch_unwind(AssertUnwindSafe(cleanup)) {
        if already_panicking {
            // Dropping this payload may itself panic, so intentionally retain
            // it for process lifetime while the original unwind continues.
            std::mem::forget(payload);
        } else {
            resume_unwind(payload);
        }
    }
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

    #[cfg(test)]
    pub(crate) fn inflight_op_count_for_test(&self) -> usize {
        unsafe { (*self.state_ptr()).runtime_state.inflight_ops }
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

#[cfg(test)]
fn ringless_owner_for_test(max_live_ops: usize) -> Rc<ExecutorOwner> {
    let task_pool = ProviderOwnedPool::new(ExecutorTaskMemProvider::new(), TASKS_PER_SLAB)
        .expect("task pool construction failed");
    let reactor =
        Reactor::new_ringless_for_test(max_live_ops).expect("ringless reactor construction failed");
    let timers = TimerRuntime::new().expect("timer runtime construction failed");
    let owner = Rc::new(ExecutorOwner {
        state: UnsafeCell::new(ExecutorState {
            reactor,
            close_worker: CloseWorker {
                sender: None,
                worker: None,
            },
            task_pool,
            ready_queue: DList::new_uninit(),
            all_tasks: DList::new_uninit(),
            timers,
            runtime_state: RuntimeState::new(),
            initialized: false,
            shutting_down: false,
            shutdown_complete: false,
        }),
        #[cfg(debug_assertions)]
        owner_thread: std::thread::current().id(),
    });

    let owner_ptr = Rc::as_ptr(&owner);
    let state = owner.state_ptr();
    unsafe {
        (*state).task_pool.init();
        (*state).ready_queue.init();
        (*state).all_tasks.init();
        (*state)
            .timers
            .init()
            .expect("timer runtime initialization failed");
        (*state).timers.bind_owner(owner_ptr);
        (*state).reactor.bind_owner(owner_ptr);
        (*state).initialized = true;
    }
    owner
}

/// Runs a test closure with a genuine FlowIO task waker over a ringless owner.
///
/// The closure must retire every operation waiter it registers before
/// returning so the stack-resident task header can be released safely.
#[cfg(test)]
pub(crate) fn with_ringless_poll_context_for_test<R>(
    max_live_ops: usize,
    test: impl FnOnce(&Rc<ExecutorOwner>, &mut Context<'_>) -> R,
) -> R {
    let owner = ringless_owner_for_test(max_live_ops);
    let _active = ExecutorCtxGuard::install(Rc::as_ptr(&owner))
        .expect("ringless test owner context installation failed");
    let mut task = TaskHeader::new();
    task.owner = Some(Rc::clone(&owner));
    let task_ptr = std::ptr::addr_of_mut!(task);
    unsafe { init_cached_waker(task_ptr) };

    let result = {
        let waker = unsafe { cached_waker_ref(task_ptr) };
        let mut cx = Context::from_waker(waker);
        test(&owner, &mut cx)
    };

    assert_eq!(
        task.refs.get(),
        1,
        "ringless test closure leaked a task waiter reference"
    );
    // The cached Waker owns the task's base reference, but its MaybeUninit
    // storage has no automatic destructor. Release the same reference directly
    // after the final borrow, matching its raw-waker Drop action without
    // creating a field-wide mutable borrow that aliases the callback's header
    // access under Miri.
    unsafe { release_task(task_ptr) };
    assert_eq!(
        task.refs.get(),
        0,
        "ringless test task did not release its base reference"
    );
    result
}

thread_local! {
    static EXECUTOR_CTX: Cell<*const ExecutorOwner> = const { Cell::new(std::ptr::null()) };
    static COMPLETION_DRAIN_ACTIVE: Cell<bool> = const { Cell::new(false) };
}

/// Marks the thread while any reactor completion view has its ring mutably
/// borrowed.
///
/// Descriptor destruction can run from retained-payload or task destructors
/// during completion reclamation. The nestable flag conservatively keeps all
/// destructor-driven owner-thread work off every ring until all active
/// completion views have been released.
pub(crate) struct CompletionDrainGuard {
    previous: bool,
}

impl CompletionDrainGuard {
    #[inline(always)]
    pub(crate) fn enter() -> Self {
        let previous = COMPLETION_DRAIN_ACTIVE.with(|active| active.replace(true));
        Self { previous }
    }
}

impl Drop for CompletionDrainGuard {
    #[inline(always)]
    fn drop(&mut self) {
        COMPLETION_DRAIN_ACTIVE.with(|active| active.set(self.previous));
    }
}

#[inline(always)]
pub(crate) fn completion_drain_active() -> bool {
    COMPLETION_DRAIN_ACTIVE.with(Cell::get)
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
    let shutting_down = !owner.is_null() && unsafe { (*(*owner).state_ptr()).shutting_down };
    if owner.is_null() || active_owner != owner || shutting_down || completion_drain_active() {
        return Err(inactive_poll_context_error());
    }

    let pctx = PollCtx { owner, task };
    #[cfg(debug_assertions)]
    unsafe {
        (*pctx.runtime_state()).stats.poll_context_extractions += 1;
    }
    Ok(pctx)
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
    _origin_keepalive: Option<Rc<ExecutorOwner>>,
    current: Option<PollCtx>,
    context_rejected: bool,
}

impl CompletedOpCtx {
    #[inline(always)]
    pub(crate) fn reactor(&self) -> *mut Reactor {
        self.origin.reactor()
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

    /// Retires a completed state without creating a whole-reactor mutable
    /// reference, including while its completion view is live.
    ///
    /// # Safety
    ///
    /// `ptr` must be the live completed state represented by this origin
    /// context and must no longer be kernel-visible.
    #[inline(always)]
    pub(crate) unsafe fn free_op_unchecked(&self, ptr: *mut CompletionState) {
        unsafe { Reactor::free_op_unchecked(self.reactor(), ptr) };
    }

    /// Takes the retained payload from a completed state without creating a
    /// whole-reactor mutable reference.
    ///
    /// # Safety
    ///
    /// `ptr` must satisfy [`Self::free_op_unchecked`] and retain exactly `T`.
    #[inline(always)]
    pub(crate) unsafe fn take_retained_payload_unchecked<T: 'static>(
        &self,
        ptr: *mut CompletionState,
    ) -> T {
        unsafe { Reactor::take_retained_payload_unchecked(self.reactor(), ptr) }
    }

    /// Extracts the retained payload from a completed state without creating
    /// a whole-reactor mutable reference.
    ///
    /// # Safety
    ///
    /// `ptr` must satisfy [`Self::take_retained_payload_unchecked`], and
    /// `extract` must fully handle every initialized field requiring drop.
    #[inline(always)]
    pub(crate) unsafe fn take_retained_payload_with_unchecked<T: 'static, R>(
        &self,
        ptr: *mut CompletionState,
        extract: impl FnOnce(*mut T) -> R,
    ) -> R {
        unsafe { Reactor::take_retained_payload_with_unchecked(self.reactor(), ptr, extract) }
    }
}

/// Records current-poll misuse and returns the operation's origin reactor for
/// completion cleanup.
///
/// # Safety
///
/// `state_ptr` must identify a live completed state allocated by a FlowIO
/// reactor. Such states always retain a non-null executor owner. `current`
/// must be the result of validating the waker for this poll, or `None` when
/// that validation failed.
#[inline(always)]
pub(crate) unsafe fn completed_op_ctx(
    current: Option<PollCtx>,
    state_ptr: *mut CompletionState,
) -> CompletedOpCtx {
    debug_assert!(!state_ptr.is_null(), "completed operation state is missing");
    let state = unsafe { &mut *state_ptr };
    debug_assert!(state.is_completed(), "operation has not completed");
    let owner = state.owner_ptr();
    debug_assert!(!owner.is_null(), "completed operation has no origin owner");

    let current_matches = current.is_some_and(|current| current.owner_ptr() == owner);
    if !current_matches {
        state.set_context_rejected();
    }
    let origin_keepalive = if current_matches {
        None
    } else {
        let owner = state.clone_owner();
        debug_assert!(owner.is_some(), "completed operation lost its origin owner");
        owner
    };

    CompletedOpCtx {
        origin: PollCtx {
            owner,
            task: std::ptr::null_mut(),
        },
        _origin_keepalive: origin_keepalive,
        current,
        context_rejected: state.is_context_rejected(),
    }
}

#[inline(always)]
/// Replaces an in-flight operation's waiter with a validated FlowIO task.
/// Invalid or foreign polls are remembered so completion returns
/// `NotConnected`; a valid foreign FlowIO task may still be registered so the
/// original reactor can notify it through its stable executor owner after the
/// CQE. Returns `true` when reactor teardown abandoned the operation without
/// observing its target CQE; callers may report that terminal condition only
/// when doing so cannot expose a retained kernel-visible caller payload.
///
/// # Safety
///
/// `state_ptr` must point to a live, incomplete completion state exclusively
/// owned by the currently polled future.
pub(crate) unsafe fn refresh_op_waiter_from_waker(
    cx: &std::task::Context<'_>,
    state_ptr: *mut CompletionState,
) -> bool {
    debug_assert!(
        !state_ptr.is_null(),
        "cannot refresh waiter for a missing completion state"
    );
    if state_ptr.is_null() {
        return false;
    }

    let state = unsafe { &mut *state_ptr };
    debug_assert!(
        !state.is_completed(),
        "cannot refresh a completed operation"
    );
    if state.is_ring_abandoned() {
        return true;
    }
    match poll_ctx_from_waker(cx) {
        Ok(pctx) => {
            if state.owner_ptr() != pctx.owner_ptr() {
                state.set_context_rejected();
            }
            unsafe { state.register_waiter(pctx.owner_task()) };
        }
        Err(_) => state.set_context_rejected(),
    }
    false
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

/// Initializes one join payload directly in its final fixed task slot.
///
/// # Safety
///
/// `dst` must be aligned, writable storage for one uninitialized
/// `JoinTask<F>`. It must be initialized exactly once and must not become
/// visible to polling or destruction until this function returns.
#[inline(always)]
unsafe fn init_join_task_at<F: Future>(dst: *mut JoinTask<F>, future: F) {
    // Publish non-owning empty state first and move the user future into its
    // final address last. None of these writes allocates or invokes user code.
    unsafe {
        std::ptr::addr_of_mut!((*dst).result).write(None);
        std::ptr::addr_of_mut!((*dst).join_waker).write(None);
        std::ptr::addr_of_mut!((*dst).future).write(Some(future));
    }
}

/// Reclaims one zero-reference task after its join payload is destroyed.
trait TaskDestroyCleanup {
    /// # Safety
    ///
    /// `task` must identify one live zero-reference task allocation whose join
    /// payload has completed destruction and has not already been reclaimed.
    unsafe fn reclaim_destroyed_task(&self, task: *mut TaskHeader);
}

impl TaskDestroyCleanup for ExecutorOwner {
    #[inline(always)]
    unsafe fn reclaim_destroyed_task(&self, task: *mut TaskHeader) {
        let state = self.state_ptr();
        let all_link = unsafe { std::ptr::addr_of_mut!((*task).all_link) };
        if unsafe { !(*all_link).is_unlinked() } {
            unsafe {
                (*state).all_tasks.remove(all_link);
            }
        }
        #[cfg(debug_assertions)]
        unsafe {
            (*state).runtime_state.stats.task_frees += 1;
        }
        unsafe {
            (*state).task_pool.free(task as *mut Task<TASK_POOL_SIZE>);
        }
    }
}

/// Runs task-allocation cleanup after the remaining join fields are destroyed,
/// including when one of their destructors unwinds.
struct TaskDestroyGuard<'cleanup, C: TaskDestroyCleanup + ?Sized> {
    cleanup: &'cleanup C,
    task: *mut TaskHeader,
}

impl<'cleanup, C: TaskDestroyCleanup + ?Sized> TaskDestroyGuard<'cleanup, C> {
    #[inline(always)]
    fn new(cleanup: &'cleanup C, task: *mut TaskHeader) -> Self {
        Self { cleanup, task }
    }

    #[inline(always)]
    fn cleanup(&self) {
        unsafe {
            // SAFETY: guard construction transfers exactly one live,
            // zero-reference task allocation into this cleanup obligation.
            self.cleanup.reclaim_destroyed_task(self.task);
        }
    }

    /// Runs ordinary task cleanup inline without entering the cold drop shim.
    #[inline(always)]
    fn finish(self) {
        let this = disarm_unwind_guard(self);
        this.cleanup();
    }
}

impl<C: TaskDestroyCleanup + ?Sized> Drop for TaskDestroyGuard<'_, C> {
    #[cold]
    #[inline(never)]
    fn drop(&mut self) {
        self.cleanup();
    }
}

/// Destroys one join payload with exactly-once allocation cleanup.
///
/// # Safety
///
/// `join_task` must point to the live join payload for `task`, which must be a
/// zero-reference task accepted by `cleanup`. No input may be accessed after
/// cleanup reclaims the task.
#[inline(always)]
unsafe fn drop_join_task_with_cleanup<F: Future, C: TaskDestroyCleanup + ?Sized>(
    join_task: *mut JoinTask<F>,
    task: *mut TaskHeader,
    cleanup: &C,
) {
    let guard = TaskDestroyGuard::new(cleanup, task);
    unsafe {
        std::ptr::drop_in_place(join_task);
    }
    guard.finish();
}

/// Clears a pinned future slot after destroying its value at the pinned
/// address, including when that destructor unwinds.
struct PinnedFutureSlotClearGuard<F> {
    slot: *mut Option<F>,
}

impl<F> Drop for PinnedFutureSlotClearGuard<F> {
    #[inline(always)]
    fn drop(&mut self) {
        // SAFETY: drop_join_future_in_place creates this guard for its live,
        // exclusively borrowed slot after selecting the contained value. That
        // value has completed drop (normally or by unwind), so overwrite the
        // stale representation without attempting a second drop.
        unsafe {
            self.slot.write(None);
        }
    }
}

/// Destroys a previously pinned join-task future without moving it.
///
/// # Safety
///
/// `slot` must point to a live, exclusively borrowed `Option<F>`. Its `Some`
/// value, if present, must no longer be polled or otherwise accessed after
/// this call begins.
#[inline(always)]
unsafe fn drop_join_future_in_place<F>(slot: *mut Option<F>) {
    let future_ptr = match unsafe { &mut *slot } {
        Some(future) => future as *mut F,
        None => return,
    };
    let clear_slot = PinnedFutureSlotClearGuard { slot };
    unsafe {
        std::ptr::drop_in_place(future_ptr);
    }
    drop(clear_slot);
}

/// Error returned when a spawned task cannot produce its output.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum JoinError {
    /// The task was cancelled before publishing an output.
    ///
    /// This occurs when the owning executor shuts down or when the task's
    /// [`Future::poll`] implementation panics. The original poll panic is
    /// re-raised from [`Executor::run`].
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
/// state, such as an active request, can retry, reject, or clean up without
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

#[inline(always)]
fn timers_pending_after_processing(timers_pending: bool, recheck: impl FnOnce() -> bool) -> bool {
    timers_pending && recheck()
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
                initialized: false,
                shutting_down: false,
                shutdown_complete: false,
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
    /// `Ok(future_output)` or [`JoinError::Cancelled`] if executor shutdown or
    /// a panic from the task's [`Future::poll`] wins before output publication.
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

                let data_ptr = (*slot_ptr).data.as_mut_ptr() as *mut JoinTask<F>;
                // Initialize the join payload directly in its fixed task slot.
                // Building a by-value JoinTask first would move a future as
                // large as the slot through a second stack temporary.
                init_join_task_at(data_ptr, future);
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
        let _ = unsafe {
            Reactor::poll_io_unchecked(
                std::ptr::addr_of_mut!((*state_ptr).reactor),
                self.process_quota,
                runtime_state,
                ready_queue,
            )
        }?;
        let timers = unsafe { std::ptr::addr_of_mut!((*state_ptr).timers) };
        if unsafe { (*timers).has_pending() } {
            let now_tick = unsafe { (*timers).now_tick()? };
            let _ = unsafe {
                TimerRuntime::process_at_with_budget_unchecked(
                    timers,
                    now_tick,
                    self.process_quota,
                )?
            };
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
    /// # Panics
    ///
    /// Re-raises a panic from a spawned task's [`Future::poll`] after
    /// terminalizing that task. A surviving [`JoinHandle`] observes
    /// [`JoinError::Cancelled`] unless the task had already published its
    /// output before a join-waker panic. Other queued tasks remain owned by
    /// this executor and can run on a later call.
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

                // Batch flag update: clear QUEUED+NOTIFIED, set RUNNING — one read + one write.
                let flags = unsafe { (*header_ptr).flags.get() };
                unsafe {
                    (*header_ptr).flags.set(
                        (flags & !(TaskHeader::FLAG_QUEUED | TaskHeader::FLAG_NOTIFIED))
                            | TaskHeader::FLAG_RUNNING,
                    );
                }
                let runtime_state = unsafe { std::ptr::addr_of_mut!((*state_ptr).runtime_state) };
                let poll_guard = TaskPollPanicGuard::new(header_ptr, runtime_state);
                #[cfg(debug_assertions)]
                unsafe {
                    (*state_ptr).runtime_state.stats.task_polls += 1;
                }
                let poll = unsafe { (*header_ptr).vtable.poll };
                let poll_res = unsafe { poll(header_ptr) };
                poll_guard.disarm();
                // A poll unwind may release the final task reference, so no
                // reference into the task allocation may span the poll call.
                let header = unsafe { &*header_ptr };
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
                    }
                    let task_ref = ExecutorTaskRefGuard::new(header_ptr);
                    let finish = header.vtable.finish;
                    let mut first_panic = None;
                    retain_first_panic(
                        &mut first_panic,
                        catch_unwind(AssertUnwindSafe(|| unsafe {
                            finish(header_ptr);
                        })),
                    );
                    retain_first_panic(
                        &mut first_panic,
                        catch_unwind(AssertUnwindSafe(|| {
                            task_ref.release();
                        })),
                    );
                    if let Some(payload) = first_panic {
                        resume_unwind(payload);
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
            let completed = unsafe {
                Reactor::poll_io_unchecked(
                    std::ptr::addr_of_mut!((*state_ptr).reactor),
                    self.process_quota,
                    std::ptr::addr_of_mut!((*state_ptr).runtime_state),
                    std::ptr::addr_of_mut!((*state_ptr).ready_queue),
                )
            }?;
            let timers_pending = unsafe { (*state_ptr).timers.has_pending() };
            let mut now_tick = None;
            let timer_budget_exhausted = if timers_pending {
                let tick = unsafe { (*state_ptr).timers.now_tick()? };
                now_tick = Some(tick);
                unsafe {
                    TimerRuntime::process_at_with_budget_unchecked(
                        std::ptr::addr_of_mut!((*state_ptr).timers),
                        tick,
                        self.process_quota,
                    )?
                }
            } else {
                false
            };
            let queue_empty = unsafe { (*state_ptr).ready_queue.is_empty() };
            let timers_pending_after = timers_pending_after_processing(timers_pending, || unsafe {
                (*state_ptr).timers.has_pending()
            });
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
                // A due timer should normally have been consumed by the pass
                // above. Keep this cheap defensive branch so a clock/tick
                // boundary is processed locally instead of entering a
                // nominally timed kernel wait.
                // SAFETY: now_tick is Some when timer_wait is Some (set in the
                // has_pending() branch above), and the raw timer pointer remains
                // owner-thread confined for this call.
                let _ = unsafe {
                    TimerRuntime::process_at_with_budget_unchecked(
                        std::ptr::addr_of_mut!((*state_ptr).timers),
                        now_tick.unwrap_unchecked(),
                        self.process_quota,
                    )?
                };
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
    /// to escaped join handles. The first cancellation panic is retained while
    /// the remaining tasks are drained.
    fn shutdown_tasks(&mut self) -> Option<PanicPayload> {
        let state_ptr = self.owner.state_ptr();
        unsafe {
            (*state_ptr).shutting_down = true;
        }
        let mut first_panic = None;

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
            if task_is_completed(flags) {
                header.flags.set(
                    (flags
                        & !(TaskHeader::FLAG_RUNNING
                            | TaskHeader::FLAG_NOTIFIED
                            | TaskHeader::FLAG_QUEUED))
                        | TaskHeader::FLAG_COMPLETED,
                );
                continue;
            }

            let task_panic = unsafe {
                cancel_task_and_release_executor_ref(
                    task_ptr,
                    std::ptr::addr_of_mut!((*state_ptr).runtime_state),
                )
            };
            if first_panic.is_none() {
                first_panic = task_panic;
            } else if let Some(payload) = task_panic {
                std::mem::forget(payload);
            }
        }

        unsafe {
            (*state_ptr).ready_queue.unlink_all_for_drop();
        }
        first_panic
    }

    fn shutdown_owner(&mut self) {
        let state_ptr = self.owner.state_ptr();
        if unsafe { !(*state_ptr).initialized || (*state_ptr).shutdown_complete } {
            return;
        }

        let owner_ptr = Rc::as_ptr(&self.owner);
        let mut first_panic = None;
        let teardown_result = catch_unwind(AssertUnwindSafe(|| {
            let _ctx_guard = ExecutorCtxGuard::install_for_shutdown(owner_ptr);
            let _shutdown_complete_guard = ShutdownCompleteGuard::new(state_ptr);
            let _close_worker_guard = CloseWorkerShutdownGuard::new(unsafe {
                std::ptr::addr_of_mut!((*state_ptr).close_worker)
            });
            let _runtime_shutdown_guard = RuntimeShutdownGuard::new(state_ptr);
            first_panic = self.shutdown_tasks();
        }));
        retain_first_panic(&mut first_panic, teardown_result);

        if let Some(payload) = first_panic {
            if std::thread::panicking() {
                // Executor::drop may run while another panic is already
                // unwinding this thread. Resuming, or merely dropping, a user
                // panic payload here could start a second panic and abort the
                // process after teardown has otherwise completed.
                std::mem::forget(payload);
            } else {
                resume_unwind(payload);
            }
        }
    }
}

#[inline(always)]
pub(super) fn retain_first_panic(
    first_panic: &mut Option<PanicPayload>,
    result: Result<(), PanicPayload>,
) {
    let Err(payload) = result else {
        return;
    };
    if first_panic.is_none() {
        *first_panic = Some(payload);
    } else {
        // Panic payloads may themselves have a panicking destructor. Once the
        // first user panic is retained, intentionally leak later payloads so
        // they cannot replace it or interrupt the remaining shutdown phases.
        std::mem::forget(payload);
    }
}

/// Marks one unfinished task cancelled, publishes its join outcome, and
/// releases the executor-owned task reference.
///
/// The state transition precedes user drop glue so any wake raised by future
/// destruction sees a terminal task. Both cleanup phases run even if either
/// unwinds; the first panic is returned and any later payload is forgotten.
///
/// # Safety
///
/// `task` must identify one live, unfinished task whose ready link is already
/// unlinked. `runtime_state` must be the live state for that task's executor,
/// and its `live_tasks` count must include this task.
unsafe fn cancel_task_and_release_executor_ref(
    task: *mut TaskHeader,
    runtime_state: *mut RuntimeState,
) -> Option<PanicPayload> {
    let flags = unsafe { (*task).flags.get() };
    let live_tasks = unsafe { (*runtime_state).live_tasks };
    #[cfg(debug_assertions)]
    if !std::thread::panicking() {
        debug_assert!(
            !task_is_completed(flags),
            "completed task cannot consume the executor reference twice"
        );
        debug_assert!(live_tasks > 0, "live task accounting underflow");
    }
    unsafe {
        (*task).flags.set(
            (flags
                & !(TaskHeader::FLAG_RUNNING
                    | TaskHeader::FLAG_NOTIFIED
                    | TaskHeader::FLAG_QUEUED))
                | TaskHeader::FLAG_COMPLETED,
        );
        // Saturation keeps exceptional cleanup non-panicking if a separate
        // internal accounting defect is encountered during an active unwind.
        (*runtime_state).live_tasks = live_tasks.saturating_sub(1);
    }

    let cancel = unsafe { (*task).vtable.cancel };
    let task_ref = ExecutorTaskRefGuard::new(task);
    let mut first_panic = None;
    retain_first_panic(
        &mut first_panic,
        catch_unwind(AssertUnwindSafe(|| unsafe {
            cancel(task);
        })),
    );
    retain_first_panic(
        &mut first_panic,
        catch_unwind(AssertUnwindSafe(|| {
            task_ref.release();
        })),
    );
    first_panic
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
    let reactor = owner.reactor_ptr();
    if completion_drain_active() {
        unsafe { Reactor::defer_cancel_during_completion_drain(reactor, ptr) };
    } else {
        unsafe { Reactor::cancel_op_unchecked(reactor, ptr) };
    }
}

/// Free a reclaimable `CompletionState` from a future's `Drop` impl.
///
/// This covers completed operations whose CQE has already been consumed and
/// operations whose SQE construction aborted before submission. Reclamation
/// uses the state owner and therefore remains valid after a run boundary or
/// public `Executor` teardown.
///
/// # Safety
///
/// `ptr` must point to a completed state whose result has been consumed or
/// otherwise made safe to drop, or to a build-aborted state that was never
/// submitted.
unsafe fn free_op_unchecked(ptr: *mut crate::runtime::op::CompletionState) {
    let Some(owner) = (unsafe { (*ptr).clone_owner() }) else {
        return;
    };
    let reactor = owner.reactor_ptr();
    if completion_drain_active() {
        unsafe { Reactor::free_op_unchecked(reactor, ptr) };
    } else {
        unsafe { (*reactor).free_op(ptr) };
    }
}

/// Release a future-owned `CompletionState` pointer from `Drop`.
/// Completed and pre-submission-aborted ops are freed immediately; pending
/// submitted ops are orphaned and cancelled.
/// Ring-abandoned ops remain leaked because no target CQE proved that their
/// kernel-visible storage may be reclaimed. The caller's pointer is always
/// cleared.
///
/// # Safety
///
/// A non-null `*ptr` must identify the completion state owned by this future
/// in its recorded origin reactor. The caller must not retain another owner
/// that may free the same state.
#[inline(always)]
pub(crate) unsafe fn drop_op_ptr_unchecked(ptr: &mut *mut crate::runtime::op::CompletionState) {
    let state_ptr = std::mem::replace(ptr, std::ptr::null_mut());
    if state_ptr.is_null() {
        return;
    }

    unsafe {
        if (*state_ptr).is_ring_abandoned() {
            // Ring-abandoned state and payload storage are deliberately leaked:
            // no target CQE proved that the kernel released its references.
        } else if (*state_ptr).is_completed() || (*state_ptr).is_build_aborted() {
            free_op_unchecked(state_ptr);
        } else if (*state_ptr).is_runtime_shutdown() {
            // Shutdown already owns cancellation and final retirement. A task
            // destructor may reach this path after the reactor has detached
            // its waiter; submitting or queueing a second cancel would corrupt
            // the pending-cancel ownership links. Mark the now-unowned state so
            // its target CQE retires the slot instead of preserving it for an
            // escaped future.
            (*state_ptr).set_orphaned();
        } else {
            cancel_op_unchecked(state_ptr);
        }
    }
}

/// Owns an allocated completion-state slot until its target SQE is submitted.
///
/// The guard is shared by I/O families that must keep the state local while
/// fallible or user-controlled preparation runs. Dropping it returns the
/// unsubmitted slot; successful submission consumes it without a conditional
/// drop branch and publishes the state pointer to the future.
pub(crate) struct UnsubmittedOpGuard {
    /// Reactor that owns the allocated completion-state slot.
    reactor: NonNull<Reactor>,
    /// Allocated slot that must be returned unless submission succeeds.
    state: NonNull<CompletionState>,
}

impl UnsubmittedOpGuard {
    /// # Safety
    ///
    /// `reactor` and `state` must be non-null; `state` must be a live,
    /// unsubmitted completion state allocated by `reactor` and uniquely owned
    /// by the returned guard.
    #[inline(always)]
    pub(crate) unsafe fn new(reactor: *mut Reactor, state: *mut CompletionState) -> Self {
        debug_assert!(
            !reactor.is_null(),
            "unsubmitted op reactor must be non-null"
        );
        debug_assert!(!state.is_null(), "unsubmitted op state must be non-null");
        Self {
            reactor: unsafe { NonNull::new_unchecked(reactor) },
            state: unsafe { NonNull::new_unchecked(state) },
        }
    }

    #[inline(always)]
    pub(crate) fn state_ptr(&self) -> *mut CompletionState {
        self.state.as_ptr()
    }

    /// Transfers the successfully submitted state to its future owner.
    #[inline(always)]
    pub(crate) fn into_state_ptr(self) -> *mut CompletionState {
        let this = std::mem::ManuallyDrop::new(self);
        this.state.as_ptr()
    }
}

impl Drop for UnsubmittedOpGuard {
    #[inline(always)]
    fn drop(&mut self) {
        unsafe { self.reactor.as_mut().free_op(self.state.as_ptr()) };
    }
}

/// Owns an attached retained payload while user-controlled SQE construction
/// runs.
///
/// Successful construction consumes this guard before the SQE can enter the
/// submission queue. If construction unwinds, the guard marks the operation as
/// never submitted, detaches and destroys the payload in place, and leaves a
/// published state safe for its future's destructor to reclaim directly.
struct AttachedRetainedPayloadGuard<T: 'static> {
    /// Attached completion state.
    state: NonNull<CompletionState>,
    /// Origin pool that owns the retained backing.
    retained_pool: NonNull<crate::runtime::retained::RetainedPayloadPool>,
    /// Exact attached payload type used by the returned-error path.
    _payload: std::marker::PhantomData<T>,
}

impl<T: 'static> AttachedRetainedPayloadGuard<T> {
    /// # Safety
    ///
    /// `state` must be a live, unsubmitted completion state with one attached
    /// retained payload of exactly type `T`, and `retained_pool` must own that
    /// payload's backing.
    #[inline(always)]
    unsafe fn new(
        state: *mut CompletionState,
        retained_pool: NonNull<crate::runtime::retained::RetainedPayloadPool>,
    ) -> Self {
        debug_assert!(!state.is_null(), "attached payload state must be non-null");
        Self {
            state: unsafe { NonNull::new_unchecked(state) },
            retained_pool,
            _payload: std::marker::PhantomData,
        }
    }

    /// Detaches the typed payload for an ordinary returned construction error.
    #[inline(always)]
    unsafe fn take(self) -> T {
        let this = std::mem::ManuallyDrop::new(self);
        unsafe {
            (&mut *this.state.as_ptr())
                .take_retained_payload::<T>(&mut *this.retained_pool.as_ptr())
        }
    }

    /// Leaves the payload attached after successful SQE construction.
    #[inline(always)]
    fn disarm(self) {
        std::mem::forget(self);
    }
}

impl<T: 'static> Drop for AttachedRetainedPayloadGuard<T> {
    #[cold]
    #[inline(never)]
    fn drop(&mut self) {
        unsafe {
            self.state.as_mut().set_build_aborted();
        }
        let state = self.state.as_ptr();
        let retained_pool = self.retained_pool.as_ptr();
        run_cleanup_preserving_panic(|| unsafe {
            CompletionState::drop_retained_payload_unchecked(state, retained_pool);
        });
    }
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
    unsafe { submit_initialized_retained_sqe(pctx, state_ptr, payload, build) }
}

/// Attach an already initialized retained payload, build its SQE, and submit
/// it with normal in-flight accounting.
///
/// On error the retained payload is detached and returned to the caller so
/// ownership and completion-state cleanup remain identical to the safe
/// by-value submission path.
///
/// # Safety
///
/// `pctx` and `state_ptr` must belong to the same active reactor;
/// `state_ptr` must be a live, exclusively owned state with no attached
/// payload. `payload` must have been allocated by that reactor's retained
/// pool. `build` may expose pointers into its payload only through the SQE
/// returned for immediate submission.
#[inline(always)]
pub(crate) unsafe fn submit_initialized_retained_sqe<T: 'static, F>(
    pctx: &PollCtx,
    state_ptr: *mut crate::runtime::op::CompletionState,
    payload: RetainedPayload<T>,
    build: F,
) -> Result<(), (io::Error, T)>
where
    F: FnOnce(&mut T) -> io::Result<squeue::Entry>,
{
    let reactor = pctx.reactor();
    unsafe { (*state_ptr).attach_retained_payload(payload) };
    let retained_pool = unsafe { Reactor::retained_payload_pool_ptr(reactor) };
    let payload_guard = unsafe { AttachedRetainedPayloadGuard::<T>::new(state_ptr, retained_pool) };

    let sqe = match build(unsafe { (*state_ptr).retained_payload_mut::<T>() }) {
        Ok(sqe) => {
            payload_guard.disarm();
            sqe
        }
        Err(err) => {
            let payload = unsafe { payload_guard.take() };
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
        if completion_drain_active() {
            #[cfg(debug_assertions)]
            if !owner.is_null() {
                unsafe {
                    (*owner).debug_assert_owner_thread();
                    let state_ptr = (*owner).state_ptr();
                    (*state_ptr).runtime_state.stats.close_ring_fallbacks += 1;
                }
            }
            return CloseSubmission::Rejected(fd);
        }
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

/// Returns the scheduling state for the currently active executor.
///
/// This checked form exists for safe dev-only entry points that may be called
/// outside [`Executor::run`]. Internal executor-driven paths use
/// [`schedule_ctx_unchecked`] after establishing the stronger context
/// invariant themselves.
#[cfg(any(test, feature = "test-support"))]
#[inline(always)]
pub(crate) fn schedule_ctx_from_active_executor() -> io::Result<ScheduleCtx> {
    EXECUTOR_CTX.with(|ctx_cell| {
        let owner = ctx_cell.get();
        if owner.is_null() {
            return Err(inactive_poll_context_error());
        }
        Ok(unsafe { schedule_ctx_from_owner_unchecked(owner) })
    })
}

/// Builds scheduling state from one validated executor owner.
///
/// # Safety
///
/// `owner` must identify a live executor owner on the current thread.
#[inline(always)]
unsafe fn schedule_ctx_from_owner_unchecked(owner: *const ExecutorOwner) -> ScheduleCtx {
    let state = unsafe { (*owner).state_ptr() };
    ScheduleCtx {
        ready_queue: unsafe { std::ptr::addr_of_mut!((*state).ready_queue) },
        runtime_state: unsafe { std::ptr::addr_of_mut!((*state).runtime_state) },
    }
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
        unsafe { schedule_ctx_from_owner_unchecked(owner) }
    })
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
                drop_join_future_in_place(std::ptr::addr_of_mut!(jt.future));
            },
            cancel: |ptr| unsafe {
                let slot = &mut *(ptr as *mut Task<TASK_POOL_SIZE>);
                let jt = &mut *(slot.data.as_mut_ptr() as *mut JoinTask<F>);
                // A user poll panic leaves no result and is reported as
                // cancellation. Preserve a result already published before a
                // join-waker panic so cleanup cannot overwrite successful
                // completion.
                if jt.result.is_none() {
                    jt.result = Some(Err(JoinError::Cancelled));
                }
                let mut first_panic = None;
                retain_first_panic(
                    &mut first_panic,
                    catch_unwind(AssertUnwindSafe(|| {
                        drop_join_future_in_place(std::ptr::addr_of_mut!(jt.future));
                    })),
                );
                if let Some(join_waker) = jt.join_waker.take() {
                    retain_first_panic(
                        &mut first_panic,
                        catch_unwind(AssertUnwindSafe(|| join_waker.wake())),
                    );
                }
                if let Some(payload) = first_panic {
                    resume_unwind(payload);
                }
            },
            destroy: |ptr| unsafe {
                let owner = (*ptr).owner.clone();
                // Drop any remaining JoinTask fields (unclaimed result, waker).
                let slot = &mut *(ptr as *mut Task<TASK_POOL_SIZE>);
                let jt = slot.data.as_mut_ptr() as *mut JoinTask<F>;
                if let Some(owner) = owner.as_deref() {
                    drop_join_task_with_cleanup(jt, ptr, owner);
                } else {
                    std::ptr::drop_in_place(jt);
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
    // `try_spawn`'s `shutting_down` check is the load-bearing gate that rejects
    // new tasks during teardown. This wake-side check remains defense in depth
    // against notifications raised while cancellation drains existing tasks.
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
    #[cfg(all(any(debug_assertions, feature = "test-support"), not(miri)))]
    use crate::net::unix::UnixStream;
    #[cfg(all(any(debug_assertions, feature = "test-support"), not(miri)))]
    use crate::runtime::buffer::IoBuffReadWrite;
    #[cfg(not(miri))]
    use crate::runtime::fd::{RuntimeFd, distinctive_closeable_test_fd, raw_fd_is_closed};
    #[cfg(not(miri))]
    use crate::runtime::io::Nop;
    #[cfg(all(any(debug_assertions, feature = "test-support"), not(miri)))]
    use crate::runtime::test_hooks;
    #[cfg(all(any(debug_assertions, feature = "test-support"), not(miri)))]
    use crate::runtime::timer::sleep;
    use std::cell::Cell;
    #[cfg(not(miri))]
    use std::cell::RefCell;
    use std::mem::ManuallyDrop;
    #[cfg(not(miri))]
    use std::os::fd::{AsRawFd, FromRawFd};
    use std::rc::Rc;
    use std::task::{RawWaker, RawWakerVTable};
    #[cfg(all(any(debug_assertions, feature = "test-support"), not(miri)))]
    use std::time::Duration;

    #[cfg(not(miri))]
    struct ReturnPendingNop {
        nop: Option<Nop>,
    }

    thread_local! {
        static RUNTIME_SHUTDOWN_DROP_STATE: Cell<*mut CompletionState> =
            const { Cell::new(std::ptr::null_mut()) };
        static RUNTIME_SHUTDOWN_DROP_COUNT: Cell<usize> = const { Cell::new(0) };
    }

    unsafe fn runtime_shutdown_output_destroy(_: *mut TaskHeader) {
        RUNTIME_SHUTDOWN_DROP_COUNT.with(|count| count.set(count.get() + 1));
        let mut state = RUNTIME_SHUTDOWN_DROP_STATE.with(Cell::get);
        assert!(!state.is_null(), "runtime-shutdown state is missing");
        unsafe {
            drop_op_ptr_unchecked(&mut state);
        }
        assert!(
            state.is_null(),
            "future Drop did not clear its state pointer"
        );
    }

    static RUNTIME_SHUTDOWN_OUTPUT_VTABLE: TaskVTable = TaskVTable {
        poll: |_| Poll::Ready(()),
        finish: |_| {},
        cancel: |_| {},
        destroy: runtime_shutdown_output_destroy,
    };

    #[cfg(not(miri))]
    impl Future for ReturnPendingNop {
        type Output = Nop;

        fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
            let this = self.get_mut();
            let nop = this.nop.as_mut().expect("pending NOP was already returned");
            assert!(
                Pin::new(nop).poll(cx).is_pending(),
                "first NOP poll did not submit an operation"
            );
            Poll::Ready(this.nop.take().expect("pending NOP disappeared"))
        }
    }

    #[test]
    fn rejected_completed_cleanup_keeps_origin_alive_through_pool_return() {
        let owner = ringless_owner_for_test(4);
        let reactor = owner.reactor_ptr();
        let state = unsafe { (&mut *reactor).alloc_op() };
        assert!(!state.is_null(), "operation allocation failed");
        unsafe {
            (*state).set_completed();
        }
        assert_eq!(Rc::strong_count(&owner), 2);
        drop(owner);

        let op_ctx = unsafe { completed_op_ctx(None, state) };
        assert!(op_ctx.context_rejected());
        unsafe {
            op_ctx.free_op_unchecked(state);
        }
        drop(op_ctx);
    }

    #[test]
    fn runtime_shutdown_task_output_drop_retires_target_state() {
        RUNTIME_SHUTDOWN_DROP_COUNT.with(|count| count.set(0));
        let owner = ringless_owner_for_test(4);
        let owner_ptr = Rc::as_ptr(&owner);
        let state_ptr = owner.state_ptr();
        let reactor = owner.reactor_ptr();
        let state = unsafe { (&mut *reactor).alloc_op() };
        assert!(!state.is_null(), "operation allocation failed");

        let mut waiter = TaskHeader::new();
        waiter.vtable = &RUNTIME_SHUTDOWN_OUTPUT_VTABLE;
        let waiter_ptr = std::ptr::addr_of_mut!(waiter);
        unsafe {
            (*state).register_waiter(waiter_ptr);
            release_task(waiter_ptr);
            (*state_ptr).runtime_state.inflight_ops = 1;
        }
        RUNTIME_SHUTDOWN_DROP_STATE.with(|stored| stored.set(state));

        let mut first_panic = None;
        unsafe {
            Reactor::prepare_shutdown_unchecked(reactor, &mut first_panic);
        }
        assert!(first_panic.is_none());
        RUNTIME_SHUTDOWN_DROP_STATE.with(|stored| stored.set(std::ptr::null_mut()));
        RUNTIME_SHUTDOWN_DROP_COUNT.with(|count| assert_eq!(count.get(), 1));
        unsafe {
            assert!((*state).is_runtime_shutdown());
            assert!((*state).is_orphaned());
            assert!(!(*state).is_completed());
            Reactor::retire_completion_unchecked(
                reactor,
                owner_ptr,
                state,
                0,
                std::ptr::addr_of_mut!((*state_ptr).runtime_state),
                std::ptr::addr_of_mut!((*state_ptr).ready_queue),
            )
            .expect("runtime-shutdown completion retirement failed");
        }

        assert_eq!(unsafe { (*state_ptr).runtime_state.inflight_ops }, 0);
        assert_eq!(unsafe { (*state_ptr).reactor.live_op_count() }, 0);
        drop(owner);
    }

    #[cfg(not(miri))]
    #[test]
    fn completion_drain_rejects_nested_same_thread_future_poll() {
        let mut executor = Executor::new().expect("executor construction failed");
        executor
            .run(async {
                let mut nop = Nop::new();
                std::future::poll_fn(|cx| {
                    let outer = CompletionDrainGuard::enter();
                    assert!(completion_drain_active());
                    let inner = CompletionDrainGuard::enter();
                    let result = match Pin::new(&mut nop).poll(cx) {
                        Poll::Ready(result) => {
                            result.expect_err("completion drain admitted a fresh NOP")
                        }
                        Poll::Pending => panic!("completion drain submitted a fresh NOP"),
                    };
                    assert_eq!(result.kind(), ErrorKind::NotConnected);
                    drop(inner);
                    assert!(
                        completion_drain_active(),
                        "nested guard ended the outer completion drain"
                    );
                    drop(outer);
                    assert!(!completion_drain_active());
                    Poll::Ready(())
                })
                .await;
            })
            .expect("executor run failed");

        let state = executor.owner.state_ptr();
        assert_eq!(unsafe { (*state).runtime_state.inflight_ops }, 0);
    }

    #[cfg(not(miri))]
    #[test]
    fn completion_drain_reclaims_completed_operation_from_task_output_drop() {
        let mut executor = Executor::new().expect("executor construction failed");
        executor
            .run(async {
                let handle = Executor::spawn(ReturnPendingNop {
                    nop: Some(Nop::new()),
                })
                .expect("pending-NOP task spawn failed");
                drop(handle);
            })
            .expect("executor failed to drain detached pending-NOP task");

        let state = executor.owner.state_ptr();
        assert_eq!(unsafe { (*state).runtime_state.inflight_ops }, 0);
        assert_eq!(
            unsafe { (*state).reactor.live_op_count() },
            0,
            "task-output NOP drop stranded its completed operation"
        );

        executor
            .run(async {
                Nop::new().await.expect("replacement NOP failed");
            })
            .expect("executor failed after reentrant operation reclamation");
        assert_eq!(unsafe { (*state).runtime_state.inflight_ops }, 0);
        assert_eq!(unsafe { (*state).reactor.live_op_count() }, 0);
    }

    #[test]
    fn ring_abandoned_waiter_refresh_reports_without_registering_waiter() {
        let mut state = CompletionState::empty();
        state.set_ring_abandoned();
        let cx = std::task::Context::from_waker(std::task::Waker::noop());

        assert!(unsafe { refresh_op_waiter_from_waker(&cx, &mut state) });
        assert!(state.is_ring_abandoned());
        assert!(!state.is_completed());
        assert!(state.waiter.is_null());
    }

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

    #[derive(Debug)]
    struct TaskPollPanic;

    #[cfg(not(miri))]
    #[derive(Debug)]
    struct TaskJoinWakePanic;

    struct TaskPollCleanupPanic;

    thread_local! {
        static TASK_POLL_CLEANUP_PAYLOAD_DROPS: Cell<usize> = const { Cell::new(0) };
        static SYNTHETIC_POLL_GUARD_CANCELS: Cell<usize> = const { Cell::new(0) };
        static SYNTHETIC_POLL_GUARD_DESTROYS: Cell<usize> = const { Cell::new(0) };
    }

    impl Drop for TaskPollCleanupPanic {
        fn drop(&mut self) {
            TASK_POLL_CLEANUP_PAYLOAD_DROPS.with(|drops| drops.set(drops.get() + 1));
            panic!("secondary poll-cleanup payload must not be dropped");
        }
    }

    #[cfg(not(miri))]
    struct PollAndDropPanic {
        polls: Rc<Cell<usize>>,
        drops: Rc<Cell<usize>>,
    }

    #[cfg(not(miri))]
    impl Future for PollAndDropPanic {
        type Output = ();

        fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
            self.polls.set(self.polls.get() + 1);
            // Exercise the wake-during-poll transition before unwinding.
            cx.waker().wake_by_ref();
            std::panic::panic_any(TaskPollPanic);
        }
    }

    #[cfg(not(miri))]
    impl Drop for PollAndDropPanic {
        fn drop(&mut self) {
            self.drops.set(self.drops.get() + 1);
            std::panic::panic_any(TaskPollCleanupPanic);
        }
    }

    #[cfg(not(miri))]
    unsafe fn panicking_wake_waker_clone(data: *const ()) -> RawWaker {
        let stats = unsafe { Rc::<CountedWakerStats>::from_raw(data.cast()) };
        stats.clones.set(stats.clones.get() + 1);
        let cloned = Rc::clone(&stats);
        let _ = Rc::into_raw(stats);
        RawWaker::new(Rc::into_raw(cloned).cast(), &PANICKING_WAKE_WAKER_VTABLE)
    }

    #[cfg(not(miri))]
    unsafe fn panicking_wake_waker_wake(data: *const ()) {
        let stats = unsafe { Rc::<CountedWakerStats>::from_raw(data.cast()) };
        stats.wakes.set(stats.wakes.get() + 1);
        std::panic::panic_any(TaskJoinWakePanic);
    }

    #[cfg(not(miri))]
    unsafe fn panicking_wake_waker_wake_by_ref(data: *const ()) {
        let stats = unsafe { Rc::<CountedWakerStats>::from_raw(data.cast()) };
        stats.wakes.set(stats.wakes.get() + 1);
        let _ = Rc::into_raw(stats);
        std::panic::panic_any(TaskJoinWakePanic);
    }

    #[cfg(not(miri))]
    unsafe fn panicking_wake_waker_drop(data: *const ()) {
        let stats = unsafe { Rc::<CountedWakerStats>::from_raw(data.cast()) };
        stats.drops.set(stats.drops.get() + 1);
    }

    #[cfg(not(miri))]
    static PANICKING_WAKE_WAKER_VTABLE: RawWakerVTable = RawWakerVTable::new(
        panicking_wake_waker_clone,
        panicking_wake_waker_wake,
        panicking_wake_waker_wake_by_ref,
        panicking_wake_waker_drop,
    );

    #[cfg(not(miri))]
    fn panicking_wake_waker(stats: &Rc<CountedWakerStats>) -> Waker {
        let data = Rc::into_raw(Rc::clone(stats)).cast();
        unsafe { Waker::from_raw(RawWaker::new(data, &PANICKING_WAKE_WAKER_VTABLE)) }
    }

    unsafe fn synthetic_poll_guard_cancel(_: *mut TaskHeader) {
        SYNTHETIC_POLL_GUARD_CANCELS.with(|cancels| cancels.set(cancels.get() + 1));
        std::panic::panic_any(TaskPollCleanupPanic);
    }

    unsafe fn synthetic_poll_guard_destroy(_: *mut TaskHeader) {
        SYNTHETIC_POLL_GUARD_DESTROYS.with(|destroys| destroys.set(destroys.get() + 1));
    }

    static SYNTHETIC_POLL_GUARD_VTABLE: TaskVTable = TaskVTable {
        poll: |_| Poll::Pending,
        finish: |_| {},
        cancel: synthetic_poll_guard_cancel,
        destroy: synthetic_poll_guard_destroy,
    };

    struct TaskOutputDropPanic;

    struct TaskWakerDropPanic;

    struct PanickingTaskOutput {
        drops: Rc<Cell<usize>>,
    }

    impl Drop for PanickingTaskOutput {
        fn drop(&mut self) {
            self.drops.set(self.drops.get() + 1);
            std::panic::panic_any(TaskOutputDropPanic);
        }
    }

    unsafe fn panicking_drop_waker_clone(data: *const ()) -> RawWaker {
        let stats = unsafe { Rc::<CountedWakerStats>::from_raw(data.cast()) };
        stats.clones.set(stats.clones.get() + 1);
        let cloned = Rc::clone(&stats);
        let _ = Rc::into_raw(stats);
        RawWaker::new(Rc::into_raw(cloned).cast(), &PANICKING_DROP_WAKER_VTABLE)
    }

    unsafe fn panicking_drop_waker_wake(data: *const ()) {
        let stats = unsafe { Rc::<CountedWakerStats>::from_raw(data.cast()) };
        stats.wakes.set(stats.wakes.get() + 1);
    }

    unsafe fn panicking_drop_waker_wake_by_ref(data: *const ()) {
        let stats = unsafe { Rc::<CountedWakerStats>::from_raw(data.cast()) };
        stats.wakes.set(stats.wakes.get() + 1);
        let _ = Rc::into_raw(stats);
    }

    unsafe fn panicking_drop_waker_drop(data: *const ()) {
        let stats = unsafe { Rc::<CountedWakerStats>::from_raw(data.cast()) };
        stats.drops.set(stats.drops.get() + 1);
        drop(stats);
        std::panic::panic_any(TaskWakerDropPanic);
    }

    static PANICKING_DROP_WAKER_VTABLE: RawWakerVTable = RawWakerVTable::new(
        panicking_drop_waker_clone,
        panicking_drop_waker_wake,
        panicking_drop_waker_wake_by_ref,
        panicking_drop_waker_drop,
    );

    fn panicking_drop_waker(stats: &Rc<CountedWakerStats>) -> Waker {
        let data = Rc::into_raw(Rc::clone(stats)).cast();
        unsafe { Waker::from_raw(RawWaker::new(data, &PANICKING_DROP_WAKER_VTABLE)) }
    }

    struct CountingTaskDestroyCleanup {
        cleanups: Cell<usize>,
    }

    impl TaskDestroyCleanup for CountingTaskDestroyCleanup {
        unsafe fn reclaim_destroyed_task(&self, _task: *mut TaskHeader) {
            self.cleanups.set(self.cleanups.get() + 1);
        }
    }

    #[cfg(not(miri))]
    struct RecordCurrentTask {
        task: Rc<Cell<*mut TaskHeader>>,
    }

    #[cfg(not(miri))]
    impl Future for RecordCurrentTask {
        type Output = ();

        fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
            let task = task_ptr_from_waker(cx.waker())
                .expect("recording future must receive its FlowIO task waker");
            self.task.set(task);
            Poll::Ready(())
        }
    }

    #[cfg(not(miri))]
    fn assert_destroyed_task_slot_is_reused(
        executor: &mut Executor,
        expected_task: *mut TaskHeader,
    ) {
        let state = executor.owner.state_ptr();
        unsafe {
            assert_eq!((*state).runtime_state.live_tasks, 0);
            assert!((*state).ready_queue.is_empty());
            assert!((*state).all_tasks.is_empty());
            #[cfg(debug_assertions)]
            assert_eq!(
                (*state).runtime_state.stats.task_frees,
                (*state).runtime_state.stats.task_allocs,
                "task destroy must balance slot accounting"
            );
        }

        let reused_task = Rc::new(Cell::new(std::ptr::null_mut()));
        executor
            .run(RecordCurrentTask {
                task: Rc::clone(&reused_task),
            })
            .expect("executor must remain reusable after task-destroy unwind");
        assert_eq!(
            reused_task.get(),
            expected_task,
            "the next root task must reuse the destroyed task's exact slot"
        );
        unsafe {
            #[cfg(debug_assertions)]
            assert_eq!(
                (*state).task_pool.provider_ref().request_count,
                0,
                "exact slot reuse must not request another task slab"
            );
            assert!((*state).all_tasks.is_empty());
            #[cfg(debug_assertions)]
            assert_eq!(
                (*state).runtime_state.stats.task_frees,
                (*state).runtime_state.stats.task_allocs,
                "reuse run must also balance task accounting"
            );
        }
    }

    #[test]
    fn task_destroy_guard_cleans_up_after_output_drop_panics() {
        let drops = Rc::new(Cell::new(0));
        let cleanup = CountingTaskDestroyCleanup {
            cleanups: Cell::new(0),
        };
        let mut task = TaskHeader::new();
        task.refs.set(0);
        let mut join_task =
            std::mem::MaybeUninit::new(JoinTask::<std::future::Ready<PanickingTaskOutput>> {
                future: None,
                result: Some(Ok(PanickingTaskOutput {
                    drops: Rc::clone(&drops),
                })),
                join_waker: None,
            });

        let panic = catch_unwind(AssertUnwindSafe(|| unsafe {
            // SAFETY: join_task is initialized, exclusively owned, and its
            // storage is never read or dropped after destruction begins.
            drop_join_task_with_cleanup(join_task.as_mut_ptr(), &mut task, &cleanup);
        }))
        .expect_err("stored output destructor must unwind");

        assert!(panic.is::<TaskOutputDropPanic>());
        assert_eq!(drops.get(), 1, "stored output must drop exactly once");
        assert_eq!(
            cleanup.cleanups.get(),
            1,
            "task cleanup must run exactly once"
        );
    }

    #[test]
    fn task_destroy_guard_cleans_up_after_waker_drop_panics() {
        let stats = Rc::new(CountedWakerStats::default());
        let cleanup = CountingTaskDestroyCleanup {
            cleanups: Cell::new(0),
        };
        let mut task = TaskHeader::new();
        task.refs.set(0);
        let mut join_task = std::mem::MaybeUninit::new(JoinTask::<std::future::Ready<()>> {
            future: None,
            result: Some(Ok(())),
            join_waker: Some(panicking_drop_waker(&stats)),
        });

        let panic = catch_unwind(AssertUnwindSafe(|| unsafe {
            // SAFETY: join_task is initialized, exclusively owned, and its
            // storage is never read or dropped after destruction begins.
            drop_join_task_with_cleanup(join_task.as_mut_ptr(), &mut task, &cleanup);
        }))
        .expect_err("stored waker destructor must unwind");

        assert!(panic.is::<TaskWakerDropPanic>());
        assert_eq!(stats.drops.get(), 1, "stored waker must drop exactly once");
        assert_eq!(
            cleanup.cleanups.get(),
            1,
            "task cleanup must run exactly once"
        );
    }

    #[test]
    fn panicking_pinned_future_drop_stays_in_place_and_clears_its_slot() {
        struct AddressSensitiveFuture {
            pinned_at: Rc<Cell<*const Self>>,
            drops: Rc<Cell<usize>>,
            _pin: std::marker::PhantomPinned,
        }

        impl Future for AddressSensitiveFuture {
            type Output = ();

            fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
                let this = self.as_ref().get_ref();
                this.pinned_at.set(std::ptr::from_ref(this));
                Poll::Ready(())
            }
        }

        impl Drop for AddressSensitiveFuture {
            fn drop(&mut self) {
                assert_eq!(
                    self.pinned_at.get(),
                    std::ptr::from_ref(self),
                    "pinned future moved before destruction"
                );
                self.drops.set(self.drops.get() + 1);
                panic!("intentional pinned-future destructor panic");
            }
        }

        let pinned_at = Rc::new(Cell::new(std::ptr::null()));
        let drops = Rc::new(Cell::new(0));
        let mut slot = Some(AddressSensitiveFuture {
            pinned_at: Rc::clone(&pinned_at),
            drops: Rc::clone(&drops),
            _pin: std::marker::PhantomPinned,
        });
        let slot_ptr = std::ptr::addr_of_mut!(slot);
        {
            let future = unsafe {
                // SAFETY: slot remains at this stack address until the helper
                // destroys its contained future, and no other reference exists.
                (*slot_ptr).as_mut().unwrap_unchecked()
            };
            let mut future = unsafe {
                // SAFETY: the value remains in slot until it is dropped in place.
                Pin::new_unchecked(future)
            };
            let waker = Waker::noop();
            let mut cx = Context::from_waker(waker);
            assert!(future.as_mut().poll(&mut cx).is_ready());
        }

        let unwind = catch_unwind(AssertUnwindSafe(|| unsafe {
            // SAFETY: slot is live and exclusively owned, and its completed
            // future must never be polled again.
            drop_join_future_in_place(slot_ptr);
        }))
        .expect_err("future destructor should unwind");
        let message = unwind
            .downcast_ref::<&str>()
            .copied()
            .or_else(|| unwind.downcast_ref::<String>().map(String::as_str));

        assert_eq!(message, Some("intentional pinned-future destructor panic"));
        assert_eq!(drops.get(), 1);
        assert!(slot.is_none(), "panicking future remained in its task slot");
    }

    #[test]
    fn absent_initial_timer_skips_post_processing_probe() {
        let probes = Cell::new(0);
        let pending = timers_pending_after_processing(false, || {
            probes.set(probes.get() + 1);
            true
        });

        assert!(!pending);
        assert_eq!(probes.get(), 0);
    }

    #[test]
    fn present_initial_timer_rechecks_after_processing() {
        let probes = Cell::new(0);
        let pending = timers_pending_after_processing(true, || {
            probes.set(probes.get() + 1);
            false
        });

        assert!(!pending);
        assert_eq!(probes.get(), 1);
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

    #[cfg(all(any(debug_assertions, feature = "test-support"), not(miri)))]
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

        assert_eq!(
            test_hooks::raw_sqe_submit_failures_remaining(),
            0,
            "ring-close rejection hook was not consumed"
        );
        #[cfg(debug_assertions)]
        {
            let stats = executor.last_stats();
            assert_eq!(stats.close_linger_queries, 0);
            assert_eq!(stats.close_ring_submissions, 0);
            assert_eq!(stats.close_ring_fallbacks, 1);
            assert_eq!(stats.close_direct_closes, 1);
        }
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
    fn normal_completion_destructor_panic_releases_task_and_owner() {
        struct ReadyThenPanic {
            polls: Rc<Cell<usize>>,
            drops: Rc<Cell<usize>>,
        }

        impl Future for ReadyThenPanic {
            type Output = usize;

            fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
                self.polls.set(self.polls.get() + 1);
                Poll::Ready(77)
            }
        }

        impl Drop for ReadyThenPanic {
            fn drop(&mut self) {
                self.drops.set(self.drops.get() + 1);
                panic!("intentional normal-completion destructor panic");
            }
        }

        let polls = Rc::new(Cell::new(0));
        let drops = Rc::new(Cell::new(0));
        let handle_slot = Rc::new(RefCell::new(None::<JoinHandle<usize>>));
        let mut executor =
            ManuallyDrop::new(Executor::new().expect("executor construction failed"));
        let weak_owner = Rc::downgrade(&executor.owner);
        let initial_owner_refs = Rc::strong_count(&executor.owner);
        assert_eq!(initial_owner_refs, 1);

        let unwind = catch_unwind(AssertUnwindSafe(|| {
            executor.run({
                let polls = Rc::clone(&polls);
                let drops = Rc::clone(&drops);
                let handle_slot = Rc::clone(&handle_slot);
                async move {
                    let handle = Executor::spawn(ReadyThenPanic { polls, drops })
                        .expect("ready task spawn failed");
                    *handle_slot.borrow_mut() = Some(handle);
                }
            })
        }))
        .expect_err("normal-completion destructor should unwind the run");
        let message = unwind
            .downcast_ref::<&str>()
            .copied()
            .or_else(|| unwind.downcast_ref::<String>().map(String::as_str));
        assert_eq!(
            message,
            Some("intentional normal-completion destructor panic"),
            "executor must preserve the user destructor panic"
        );
        assert_eq!(polls.get(), 1, "ready future poll count");
        assert_eq!(drops.get(), 1, "ready future destructor count");

        let state_ptr = executor.owner.state_ptr();
        unsafe {
            assert_eq!((*state_ptr).runtime_state.live_tasks, 0);
            assert!((*state_ptr).ready_queue.is_empty());
            assert!(
                !(*state_ptr).all_tasks.is_empty(),
                "surviving join handle should retain the completed task slot"
            );
            #[cfg(debug_assertions)]
            {
                assert_eq!((*state_ptr).runtime_state.stats.task_allocs, 2);
                assert_eq!((*state_ptr).runtime_state.stats.task_frees, 1);
            }
        }
        assert_eq!(
            Rc::strong_count(&executor.owner),
            initial_owner_refs + 1,
            "only the surviving completed task may retain the executor owner"
        );

        let mut handle = handle_slot
            .borrow_mut()
            .take()
            .expect("completed task handle disappeared");
        assert!(handle.is_finished(), "completed output was not published");
        let waker = Waker::noop();
        let mut cx = Context::from_waker(waker);
        assert!(matches!(
            Pin::new(&mut handle).poll(&mut cx),
            Poll::Ready(Ok(77))
        ));
        drop(handle);

        unsafe {
            assert!((*state_ptr).all_tasks.is_empty());
            #[cfg(debug_assertions)]
            assert_eq!(
                (*state_ptr).runtime_state.stats.task_frees,
                (*state_ptr).runtime_state.stats.task_allocs,
                "normal-completion panic leaked a task slot"
            );
        }
        assert_eq!(
            Rc::strong_count(&executor.owner),
            initial_owner_refs,
            "completed task retained the executor owner after handle drop"
        );

        executor
            .run(async {})
            .expect("executor should remain reusable after completion unwind");
        let drop_result = catch_unwind(AssertUnwindSafe(|| unsafe {
            ManuallyDrop::drop(&mut executor);
        }));
        assert!(drop_result.is_ok(), "executor shutdown panicked again");
        assert_eq!(drops.get(), 1, "future destructor ran more than once");
        assert!(
            weak_owner.upgrade().is_none(),
            "completed task retained an executor owner pin"
        );
    }

    #[cfg(not(miri))]
    #[test]
    fn poll_panic_cancels_task_preserves_original_and_leaves_executor_reusable() {
        TASK_POLL_CLEANUP_PAYLOAD_DROPS.with(|drops| drops.set(0));
        let polls = Rc::new(Cell::new(0));
        let future_drops = Rc::new(Cell::new(0));
        let sibling_polls = Rc::new(Cell::new(0));
        let join_waker_stats = Rc::new(CountedWakerStats::default());
        let handle_slot = Rc::new(RefCell::new(None::<JoinHandle<()>>));
        let mut executor = Executor::new().expect("executor construction failed");
        let initial_owner_refs = Rc::strong_count(&executor.owner);

        let unwind = catch_unwind(AssertUnwindSafe(|| {
            executor.run({
                let polls = Rc::clone(&polls);
                let future_drops = Rc::clone(&future_drops);
                let sibling_polls = Rc::clone(&sibling_polls);
                let join_waker_stats = Rc::clone(&join_waker_stats);
                let handle_slot = Rc::clone(&handle_slot);
                async move {
                    let mut handle = Executor::spawn(PollAndDropPanic {
                        polls,
                        drops: future_drops,
                    })
                    .expect("poll-panic task spawn failed");
                    let waker = panicking_wake_waker(&join_waker_stats);
                    let mut cx = Context::from_waker(&waker);
                    assert!(
                        Pin::new(&mut handle).poll(&mut cx).is_pending(),
                        "unpolled task handle must start pending"
                    );
                    *handle_slot.borrow_mut() = Some(handle);

                    Executor::spawn(async move {
                        sibling_polls.set(sibling_polls.get() + 1);
                    })
                    .expect("sibling task spawn failed");
                }
            })
        }))
        .expect_err("user poll panic must unwind Executor::run");
        assert!(
            unwind.downcast_ref::<TaskPollPanic>().is_some(),
            "cleanup replaced the original user poll panic"
        );
        assert_eq!(polls.get(), 1, "panicking future poll count");
        assert_eq!(future_drops.get(), 1, "panicking future drop count");
        TASK_POLL_CLEANUP_PAYLOAD_DROPS.with(|drops| {
            assert_eq!(
                drops.get(),
                0,
                "secondary cleanup panic payload was dropped during unwind"
            );
        });
        assert_eq!(
            join_waker_stats.wakes.get(),
            1,
            "cancelled task did not wake its registered join handle"
        );
        assert_eq!(
            sibling_polls.get(),
            0,
            "later task ran after the first run had already unwound"
        );

        let state_ptr = executor.owner.state_ptr();
        let mut handle = handle_slot
            .borrow_mut()
            .take()
            .expect("poll-panic join handle disappeared");
        let panicked_task = handle.task_ptr;
        unsafe {
            assert_eq!(
                (*panicked_task).flags.get(),
                TaskHeader::FLAG_COMPLETED,
                "wake-during-poll notification survived terminalization"
            );
            assert!((*panicked_task).ready_link.is_unlinked());
            assert_eq!((*state_ptr).runtime_state.live_tasks, 1);
            assert!(
                !(*state_ptr).ready_queue.is_empty(),
                "queued sibling was discarded with the panicking task"
            );
        }
        assert!(
            handle.is_finished(),
            "cancellation result was not published"
        );
        let waker = Waker::noop();
        let mut cx = Context::from_waker(waker);
        assert!(matches!(
            Pin::new(&mut handle).poll(&mut cx),
            Poll::Ready(Err(JoinError::Cancelled))
        ));

        executor
            .run(async {})
            .expect("later run must resume sibling work after poll panic");
        assert_eq!(sibling_polls.get(), 1, "queued sibling did not resume");
        unsafe {
            assert_eq!((*state_ptr).runtime_state.live_tasks, 0);
            assert!((*state_ptr).ready_queue.is_empty());
        }

        drop(handle);
        assert_destroyed_task_slot_is_reused(&mut executor, panicked_task);
        assert_eq!(future_drops.get(), 1, "panicking future was dropped twice");
        assert_eq!(
            Rc::strong_count(&executor.owner),
            initial_owner_refs,
            "poll-panic cleanup retained a task owner reference"
        );
    }

    #[cfg(not(miri))]
    #[test]
    fn join_wake_panic_after_ready_preserves_published_output() {
        let waker_stats = Rc::new(CountedWakerStats::default());
        let handle_slot = Rc::new(RefCell::new(None::<JoinHandle<usize>>));
        let mut executor = Executor::new().expect("executor construction failed");

        let unwind = catch_unwind(AssertUnwindSafe(|| {
            executor.run({
                let waker_stats = Rc::clone(&waker_stats);
                let handle_slot = Rc::clone(&handle_slot);
                async move {
                    let mut handle = Executor::spawn(std::future::ready(91usize))
                        .expect("ready task spawn failed");
                    let waker = panicking_wake_waker(&waker_stats);
                    let mut cx = Context::from_waker(&waker);
                    assert!(Pin::new(&mut handle).poll(&mut cx).is_pending());
                    *handle_slot.borrow_mut() = Some(handle);
                }
            })
        }))
        .expect_err("registered join wake must unwind");
        assert!(
            unwind.downcast_ref::<TaskJoinWakePanic>().is_some(),
            "terminal cleanup replaced the join-waker panic"
        );
        assert_eq!(waker_stats.clones.get(), 1);
        assert_eq!(waker_stats.wakes.get(), 1);

        let mut handle = handle_slot
            .borrow_mut()
            .take()
            .expect("ready task handle disappeared");
        assert!(
            handle.is_finished(),
            "ready output was lost during wake unwind"
        );
        let waker = Waker::noop();
        let mut cx = Context::from_waker(waker);
        assert!(matches!(
            Pin::new(&mut handle).poll(&mut cx),
            Poll::Ready(Ok(91))
        ));
        drop(handle);

        let state = executor.owner.state_ptr();
        unsafe {
            assert_eq!((*state).runtime_state.live_tasks, 0);
            assert!((*state).ready_queue.is_empty());
            assert!((*state).all_tasks.is_empty());
        }
        executor
            .run(async {})
            .expect("executor must remain reusable after join-wake panic");
    }

    #[test]
    fn poll_panic_guard_terminalizes_and_releases_one_reference_under_miri() {
        TASK_POLL_CLEANUP_PAYLOAD_DROPS.with(|drops| drops.set(0));
        SYNTHETIC_POLL_GUARD_CANCELS.with(|cancels| cancels.set(0));
        SYNTHETIC_POLL_GUARD_DESTROYS.with(|destroys| destroys.set(0));

        let mut task = TaskHeader::new();
        task.vtable = &SYNTHETIC_POLL_GUARD_VTABLE;
        task.refs.set(2);
        task.flags
            .set(TaskHeader::FLAG_RUNNING | TaskHeader::FLAG_NOTIFIED);
        let task_ptr = &mut task as *mut TaskHeader;
        let mut runtime_state = RuntimeState::new();
        runtime_state.live_tasks = 1;

        let unwind = catch_unwind(AssertUnwindSafe(|| {
            let _guard = TaskPollPanicGuard::new(task_ptr, &mut runtime_state);
            std::panic::panic_any(TaskPollPanic);
        }))
        .expect_err("synthetic poll did not unwind");
        assert!(
            unwind.downcast_ref::<TaskPollPanic>().is_some(),
            "synthetic cleanup replaced the poll panic"
        );
        SYNTHETIC_POLL_GUARD_CANCELS.with(|cancels| assert_eq!(cancels.get(), 1));
        SYNTHETIC_POLL_GUARD_DESTROYS.with(|destroys| assert_eq!(destroys.get(), 0));
        TASK_POLL_CLEANUP_PAYLOAD_DROPS.with(|drops| assert_eq!(drops.get(), 0));
        assert_eq!(runtime_state.live_tasks, 0);
        assert_eq!(task.flags.get(), TaskHeader::FLAG_COMPLETED);
        assert!(task.ready_link.is_unlinked());
        assert_eq!(task.refs.get(), 1);

        unsafe { release_task(task_ptr) };
        SYNTHETIC_POLL_GUARD_DESTROYS.with(|destroys| assert_eq!(destroys.get(), 1));
    }

    #[cfg(not(miri))]
    #[test]
    fn task_destroy_detached_output_panic_recycles_slot_and_owner() {
        let output_drops = Rc::new(Cell::new(0));
        let child_task = Rc::new(Cell::new(std::ptr::null_mut()));
        let mut executor =
            ManuallyDrop::new(Executor::new().expect("executor construction failed"));
        let weak_owner = Rc::downgrade(&executor.owner);
        let initial_owner_refs = Rc::strong_count(&executor.owner);

        let panic = catch_unwind(AssertUnwindSafe(|| {
            executor.run({
                let output_drops = Rc::clone(&output_drops);
                let child_task = Rc::clone(&child_task);
                async move {
                    let handle = Executor::spawn(async move {
                        PanickingTaskOutput {
                            drops: output_drops,
                        }
                    })
                    .expect("detached drop-bomb task spawn failed");
                    child_task.set(handle.task_ptr);
                    drop(handle);
                }
            })
        }))
        .expect_err("detached unclaimed output destructor must unwind");

        assert!(panic.is::<TaskOutputDropPanic>());
        assert_eq!(
            output_drops.get(),
            1,
            "detached output must drop exactly once"
        );
        assert!(!child_task.get().is_null());
        assert_eq!(
            Rc::strong_count(&executor.owner),
            initial_owner_refs,
            "destroyed detached task retained its executor owner"
        );
        assert_destroyed_task_slot_is_reused(&mut executor, child_task.get());

        let drop_result = catch_unwind(AssertUnwindSafe(|| unsafe {
            ManuallyDrop::drop(&mut executor);
        }));
        assert!(
            drop_result.is_ok(),
            "clean executor teardown must not panic"
        );
        assert!(
            weak_owner.upgrade().is_none(),
            "detached task retained the executor graph"
        );
    }

    #[cfg(not(miri))]
    #[test]
    fn task_destroy_final_handle_output_panic_recycles_slot_and_owner() {
        let output_drops = Rc::new(Cell::new(0));
        let handle_slot = Rc::new(RefCell::new(None::<JoinHandle<PanickingTaskOutput>>));
        let mut executor =
            ManuallyDrop::new(Executor::new().expect("executor construction failed"));
        let weak_owner = Rc::downgrade(&executor.owner);
        let initial_owner_refs = Rc::strong_count(&executor.owner);

        executor
            .run({
                let output_drops = Rc::clone(&output_drops);
                let handle_slot = Rc::clone(&handle_slot);
                async move {
                    let handle = Executor::spawn(async move {
                        PanickingTaskOutput {
                            drops: output_drops,
                        }
                    })
                    .expect("retained drop-bomb task spawn failed");
                    *handle_slot.borrow_mut() = Some(handle);
                }
            })
            .expect("drop-bomb task should complete before handle destruction");

        let handle = handle_slot
            .borrow_mut()
            .take()
            .expect("completed drop-bomb handle disappeared");
        assert!(handle.is_finished());
        let task = handle.task_ptr;
        assert_eq!(
            Rc::strong_count(&executor.owner),
            initial_owner_refs + 1,
            "completed task should retain one owner reference"
        );

        let panic = catch_unwind(AssertUnwindSafe(|| drop(handle)))
            .expect_err("final handle must expose the output destructor panic");
        assert!(panic.is::<TaskOutputDropPanic>());
        assert_eq!(
            output_drops.get(),
            1,
            "unclaimed handle output must drop exactly once"
        );
        assert_eq!(
            Rc::strong_count(&executor.owner),
            initial_owner_refs,
            "final handle destruction retained its executor owner"
        );
        assert_destroyed_task_slot_is_reused(&mut executor, task);

        let drop_result = catch_unwind(AssertUnwindSafe(|| unsafe {
            ManuallyDrop::drop(&mut executor);
        }));
        assert!(
            drop_result.is_ok(),
            "clean executor teardown must not panic"
        );
        assert!(
            weak_owner.upgrade().is_none(),
            "final output panic retained the executor graph"
        );
    }

    #[cfg(not(miri))]
    #[test]
    fn task_destroy_escaped_handle_waker_panic_releases_final_owner() {
        let waker_stats = Rc::new(CountedWakerStats::default());
        let handle_slot = Rc::new(RefCell::new(None::<JoinHandle<()>>));
        let mut executor =
            ManuallyDrop::new(Executor::new().expect("executor construction failed"));
        let weak_owner = Rc::downgrade(&executor.owner);
        let initial_owner_refs = Rc::strong_count(&executor.owner);

        executor
            .run({
                let handle_slot = Rc::clone(&handle_slot);
                async move {
                    let handle =
                        Executor::spawn(async {}).expect("retained waker task spawn failed");
                    *handle_slot.borrow_mut() = Some(handle);
                }
            })
            .expect("stored-waker task should complete before handle destruction");

        let handle = handle_slot
            .borrow_mut()
            .take()
            .expect("completed stored-waker handle disappeared");
        assert!(handle.is_finished());
        let task = handle.task_ptr;
        unsafe {
            let waker_slot = &mut *handle.waker_ptr;
            assert!(
                waker_slot.is_none(),
                "completion should consume any ordinary stored join waker"
            );
            // A public completion normally takes and wakes this slot. Private
            // injection directly covers destroy's obligation if a remaining
            // Waker destructor unwinds on an exceptional internal path.
            *waker_slot = Some(panicking_drop_waker(&waker_stats));
        }
        assert_eq!(
            Rc::strong_count(&executor.owner),
            initial_owner_refs + 1,
            "completed task should retain one owner reference"
        );

        let drop_result = catch_unwind(AssertUnwindSafe(|| unsafe {
            ManuallyDrop::drop(&mut executor);
        }));
        assert!(
            drop_result.is_ok(),
            "completed escaped handle must permit clean executor shutdown"
        );
        assert_eq!(
            weak_owner.strong_count(),
            1,
            "the escaped task header must be the sole remaining owner"
        );
        unsafe {
            assert!(
                (*task).all_link.is_unlinked(),
                "executor shutdown must unlink the completed escaped task"
            );
        }

        let panic = catch_unwind(AssertUnwindSafe(|| drop(handle)))
            .expect_err("final handle must expose the waker destructor panic");
        assert!(panic.is::<TaskWakerDropPanic>());
        assert_eq!(
            waker_stats.drops.get(),
            1,
            "stored waker must drop exactly once"
        );
        assert!(
            weak_owner.upgrade().is_none(),
            "stored-waker panic retained the final executor graph owner"
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

    #[cfg(all(any(debug_assertions, feature = "test-support"), not(miri)))]
    #[test]
    fn timer_waiter_panic_still_abandons_reactor_and_completes_shutdown() {
        struct FirstShutdownPanic;

        struct PendingDropPanic {
            drops: Rc<Cell<usize>>,
        }

        impl Future for PendingDropPanic {
            type Output = ();

            fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
                Poll::Pending
            }
        }

        impl Drop for PendingDropPanic {
            fn drop(&mut self) {
                self.drops.set(self.drops.get() + 1);
                std::panic::panic_any(FirstShutdownPanic);
            }
        }

        struct StageArmedSleepOutput {
            sleep: Option<crate::runtime::timer::Sleep>,
            staged: Rc<RefCell<Option<crate::runtime::timer::Sleep>>>,
            output_drops: Rc<Cell<usize>>,
        }

        impl Future for StageArmedSleepOutput {
            type Output = PanickingTaskOutput;

            fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
                let this = self.get_mut();
                let sleep = this.sleep.as_mut().expect("shutdown sleep missing");
                assert!(Pin::new(sleep).poll(cx).is_pending());
                *this.staged.borrow_mut() = this.sleep.take();
                Poll::Ready(PanickingTaskOutput {
                    drops: Rc::clone(&this.output_drops),
                })
            }
        }

        let mut executor = Executor::new().expect("executor construction failed");
        let (mut reader, writer) = UnixStream::pair().expect("socketpair failed");
        let first_panic_drops = Rc::new(Cell::new(0));
        let timer_output_drops = Rc::new(Cell::new(0));
        let staged_sleep = Rc::new(RefCell::new(None));

        let err = executor
            .run({
                let first_panic_drops = Rc::clone(&first_panic_drops);
                let timer_output_drops = Rc::clone(&timer_output_drops);
                let staged_sleep = Rc::clone(&staged_sleep);
                async move {
                    let first_panic = Executor::spawn(PendingDropPanic {
                        drops: first_panic_drops,
                    })
                    .expect("first panic task spawn failed");
                    drop(first_panic);

                    let read = Executor::spawn(async move {
                        let _ = reader.read(vec![0u8; 1], 1).await;
                    })
                    .expect("pending read task spawn failed");
                    drop(read);

                    let timer_output = Executor::spawn(StageArmedSleepOutput {
                        sleep: Some(sleep(Duration::from_secs(3_600))),
                        staged: staged_sleep,
                        output_drops: timer_output_drops,
                    })
                    .expect("timer output task spawn failed");
                    drop(timer_output);

                    test_hooks::fail_next_ring_wait_errno(libc::EIO);
                }
            })
            .expect_err("injected ring wait error should stop the run");
        assert_eq!(err.raw_os_error(), Some(libc::EIO));
        assert_eq!(test_hooks::ring_wait_failures_remaining(), 0);
        assert_eq!(first_panic_drops.get(), 0);
        assert_eq!(timer_output_drops.get(), 0);
        assert!(staged_sleep.borrow().is_some(), "sleep was not staged");

        let state_ptr = executor.owner.state_ptr();
        unsafe {
            assert_eq!((*state_ptr).runtime_state.live_tasks, 2);
            assert_eq!((*state_ptr).runtime_state.inflight_ops, 1);
            assert!((*state_ptr).timers.has_pending());
            assert!(!(*state_ptr).shutdown_complete);
        }

        test_hooks::force_next_reactor_shutdown_fallback();
        let unwind = catch_unwind(AssertUnwindSafe(|| executor.shutdown_owner()))
            .expect_err("first task destructor should unwind shutdown");
        assert!(
            unwind.is::<FirstShutdownPanic>(),
            "timer waiter panic replaced the first task panic"
        );

        assert_eq!(first_panic_drops.get(), 1);
        assert_eq!(timer_output_drops.get(), 1);
        assert_eq!(
            test_hooks::reactor_shutdown_fallbacks_remaining(),
            0,
            "timer unwind skipped the forced reactor fallback"
        );
        unsafe {
            assert_eq!((*state_ptr).runtime_state.live_tasks, 0);
            assert_eq!((*state_ptr).runtime_state.inflight_ops, 0);
            assert!((*state_ptr).reactor.test_storage_abandoned());
            assert!((*state_ptr).all_tasks.is_empty());
            assert!((*state_ptr).ready_queue.is_empty());
            assert!((*state_ptr).close_worker.sender.is_none());
            assert!((*state_ptr).close_worker.worker.is_none());
            assert!((*state_ptr).shutdown_complete);
        }

        drop(
            staged_sleep
                .borrow_mut()
                .take()
                .expect("staged sleep disappeared"),
        );
        executor.shutdown_owner();
        assert_eq!(first_panic_drops.get(), 1, "shutdown ran twice");
        assert_eq!(timer_output_drops.get(), 1, "timer output dropped twice");

        drop(writer);
        drop(executor);
        assert_eq!(first_panic_drops.get(), 1, "executor drop reran shutdown");
        assert_eq!(
            timer_output_drops.get(),
            1,
            "executor drop reran timer output destruction"
        );
    }

    #[cfg(all(any(debug_assertions, feature = "test-support"), not(miri)))]
    #[test]
    fn shutdown_drains_io_timer_and_worker_before_resuming_task_destructor_panic() {
        struct DropTrackedBuffer {
            bytes: Vec<u8>,
            drops: Rc<Cell<usize>>,
        }

        impl Drop for DropTrackedBuffer {
            fn drop(&mut self) {
                self.drops.set(self.drops.get() + 1);
            }
        }

        // SAFETY: bytes owns pointer-stable initialized storage and these
        // methods never grow its allocation. The runtime publishes no more
        // than writable_len() bytes.
        unsafe impl IoBuffReadWrite for DropTrackedBuffer {
            fn as_mut_ptr(&mut self) -> *mut u8 {
                self.bytes.as_mut_ptr()
            }

            fn writable_len(&self) -> usize {
                self.bytes.capacity()
            }

            unsafe fn set_written_len(&mut self, len: usize) {
                unsafe {
                    self.bytes.set_len(len);
                }
            }
        }

        struct PanicAfterFuture<F, P: Any + Send> {
            future: ManuallyDrop<F>,
            payload: Option<P>,
            panics: Rc<Cell<usize>>,
        }

        impl<F: Future, P: Any + Send> Future for PanicAfterFuture<F, P> {
            type Output = F::Output;

            fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
                let this = unsafe { self.get_unchecked_mut() };
                // SAFETY: the wrapper never moves the inner future after it is
                // pinned, and Drop destroys it exactly once.
                unsafe { Pin::new_unchecked(&mut *this.future) }.poll(cx)
            }
        }

        impl<F, P: Any + Send> Drop for PanicAfterFuture<F, P> {
            fn drop(&mut self) {
                // Drop the submitted operation first so shutdown must retire
                // its retained payload after catching the intentional panic.
                unsafe {
                    ManuallyDrop::drop(&mut self.future);
                }
                self.panics.set(self.panics.get() + 1);
                std::panic::panic_any(
                    self.payload
                        .take()
                        .expect("panicking future payload already taken"),
                );
            }
        }

        struct SecondaryPanicPayload;

        impl Drop for SecondaryPanicPayload {
            fn drop(&mut self) {
                panic!("secondary panic payload must not be dropped");
            }
        }

        struct DropCounter(Rc<Cell<usize>>);

        impl Drop for DropCounter {
            fn drop(&mut self) {
                self.0.set(self.0.get() + 1);
            }
        }

        let mut executor = Executor::new().expect("executor construction failed");
        let (mut reader, writer) = UnixStream::pair().expect("socketpair failed");
        let buffer_drops = Rc::new(Cell::new(0));
        let panic_count = Rc::new(Cell::new(0));
        let secondary_panic_count = Rc::new(Cell::new(0));
        let timer_task_drops = Rc::new(Cell::new(0));
        let tail_task_drops = Rc::new(Cell::new(0));
        let read_handle = Rc::new(RefCell::new(None::<JoinHandle<()>>));

        let err = executor
            .run({
                let buffer_drops = Rc::clone(&buffer_drops);
                let panic_count = Rc::clone(&panic_count);
                let secondary_panic_count = Rc::clone(&secondary_panic_count);
                let timer_task_drops = Rc::clone(&timer_task_drops);
                let tail_task_drops = Rc::clone(&tail_task_drops);
                let read_handle_slot = Rc::clone(&read_handle);
                async move {
                    let read = async move {
                        let buffer = DropTrackedBuffer {
                            bytes: vec![0; 64],
                            drops: buffer_drops,
                        };
                        let _ = reader.read(buffer, 64).await;
                    };
                    let handle = Executor::spawn(PanicAfterFuture {
                        future: ManuallyDrop::new(read),
                        payload: Some("intentional submitted-task destructor panic"),
                        panics: panic_count,
                    })
                    .expect("submitted read task spawn failed");
                    *read_handle_slot.borrow_mut() = Some(handle);

                    let timer = async move {
                        let _drop_counter = DropCounter(timer_task_drops);
                        let _ = sleep(Duration::from_secs(3_600)).await;
                    };
                    let timer_handle = Executor::spawn(PanicAfterFuture {
                        future: ManuallyDrop::new(timer),
                        payload: Some(SecondaryPanicPayload),
                        panics: secondary_panic_count,
                    })
                    .expect("timer task spawn failed");
                    drop(timer_handle);

                    let tail_handle = Executor::spawn(async move {
                        let _drop_counter = DropCounter(tail_task_drops);
                        std::future::pending::<()>().await;
                    })
                    .expect("tail task spawn failed");
                    drop(tail_handle);

                    test_hooks::fail_next_ring_wait_errno(libc::EIO);
                }
            })
            .expect_err("injected post-submit wait error should stop the run");
        assert_eq!(err.raw_os_error(), Some(libc::EIO));
        assert_eq!(test_hooks::ring_wait_failures_remaining(), 0);
        assert_eq!(buffer_drops.get(), 0, "retained buffer dropped early");
        assert_eq!(panic_count.get(), 0, "task dropped before shutdown");
        assert_eq!(secondary_panic_count.get(), 0, "timer task dropped early");
        assert_eq!(timer_task_drops.get(), 0, "timer task dropped early");
        assert_eq!(tail_task_drops.get(), 0, "tail task dropped early");

        let state_ptr = executor.owner.state_ptr();
        unsafe {
            assert_eq!((*state_ptr).runtime_state.live_tasks, 3);
            assert_eq!((*state_ptr).runtime_state.inflight_ops, 1);
            assert!((*state_ptr).timers.has_pending(), "timer was not armed");
            assert!(!(*state_ptr).shutdown_complete);
        }
        assert!(
            !read_handle
                .borrow()
                .as_ref()
                .expect("read handle missing")
                .is_finished()
        );
        let join_waker_stats = Rc::new(CountedWakerStats::default());
        {
            let mut handle_slot = read_handle.borrow_mut();
            let handle = handle_slot.as_mut().expect("read handle missing");
            let waker = counted_waker(&join_waker_stats);
            let mut cx = Context::from_waker(&waker);
            assert!(Pin::new(handle).poll(&mut cx).is_pending());
        }
        assert_eq!(join_waker_stats.wakes.get(), 0);
        drop(writer);

        let unwind = catch_unwind(AssertUnwindSafe(|| executor.shutdown_owner()))
            .expect_err("submitted task destructor should unwind shutdown");
        let message = unwind
            .downcast_ref::<&str>()
            .copied()
            .or_else(|| unwind.downcast_ref::<String>().map(String::as_str));
        assert_eq!(
            message,
            Some("intentional submitted-task destructor panic"),
            "shutdown must preserve the first user panic"
        );

        assert_eq!(panic_count.get(), 1, "user destructor panic count");
        assert_eq!(
            secondary_panic_count.get(),
            1,
            "secondary destructor panic count"
        );
        assert_eq!(timer_task_drops.get(), 1, "later timer task drop count");
        assert_eq!(tail_task_drops.get(), 1, "tail task drop count");
        assert_eq!(buffer_drops.get(), 1, "retained buffer drop count");
        assert_eq!(
            join_waker_stats.wakes.get(),
            1,
            "cancelled join handle was not woken"
        );
        unsafe {
            assert_eq!((*state_ptr).runtime_state.live_tasks, 0);
            assert_eq!((*state_ptr).runtime_state.inflight_ops, 0);
            assert!(!(*state_ptr).timers.has_pending());
            assert!((*state_ptr).all_tasks.is_empty());
            assert!((*state_ptr).ready_queue.is_empty());
            assert!((*state_ptr).close_worker.sender.is_none());
            assert!((*state_ptr).close_worker.worker.is_none());
            assert!((*state_ptr).shutdown_complete);
        }

        let mut handle = read_handle
            .borrow_mut()
            .take()
            .expect("read handle disappeared");
        assert!(
            handle.is_finished(),
            "cancellation was not published before the destructor panic"
        );
        let waker = Waker::noop();
        let mut cx = Context::from_waker(waker);
        assert!(matches!(
            Pin::new(&mut handle).poll(&mut cx),
            Poll::Ready(Err(JoinError::Cancelled))
        ));
        drop(handle);
        #[cfg(debug_assertions)]
        unsafe {
            assert_eq!(
                (*state_ptr).runtime_state.stats.task_frees,
                (*state_ptr).runtime_state.stats.task_allocs,
                "shutdown leaked a task allocation"
            );
        }

        executor.shutdown_owner();
        assert_eq!(panic_count.get(), 1, "completed shutdown ran twice");
        assert_eq!(
            secondary_panic_count.get(),
            1,
            "secondary destructor ran twice"
        );
        assert_eq!(timer_task_drops.get(), 1, "timer task dropped twice");
        assert_eq!(tail_task_drops.get(), 1, "tail task dropped twice");
        assert_eq!(buffer_drops.get(), 1, "retained buffer dropped twice");
        drop(executor);
        assert_eq!(panic_count.get(), 1, "executor drop reran shutdown");
        assert_eq!(
            secondary_panic_count.get(),
            1,
            "executor drop reran secondary destructor"
        );
        assert_eq!(timer_task_drops.get(), 1, "executor drop reran timer task");
        assert_eq!(tail_task_drops.get(), 1, "executor drop reran tail task");
        assert_eq!(buffer_drops.get(), 1, "executor drop reran retained drop");
    }

    #[cfg(not(miri))]
    #[test]
    fn shutdown_rejects_task_destructor_spawn_without_queueing_child() {
        #[derive(Clone, Copy, Debug, PartialEq, Eq)]
        enum SpawnOutcome {
            NotAttempted,
            NoExecutor,
            OtherError,
            Admitted,
        }

        struct TrackedChild {
            polls: Rc<Cell<usize>>,
            drops: Rc<Cell<usize>>,
        }

        impl Future for TrackedChild {
            type Output = ();

            fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
                self.polls.set(self.polls.get() + 1);
                Poll::Pending
            }
        }

        impl Drop for TrackedChild {
            fn drop(&mut self) {
                self.drops.set(self.drops.get() + 1);
            }
        }

        struct SpawnChildOnDrop {
            polls: Rc<Cell<usize>>,
            drops: Rc<Cell<usize>>,
            child_polls: Rc<Cell<usize>>,
            child_drops: Rc<Cell<usize>>,
            outcome: Rc<Cell<SpawnOutcome>>,
            expected_owner: *const ExecutorOwner,
            saw_active_shutdown_context: Rc<Cell<bool>>,
        }

        impl Future for SpawnChildOnDrop {
            type Output = ();

            fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
                self.polls.set(self.polls.get() + 1);
                Poll::Pending
            }
        }

        impl Drop for SpawnChildOnDrop {
            fn drop(&mut self) {
                self.drops.set(self.drops.get() + 1);
                let active_owner = EXECUTOR_CTX.with(Cell::get);
                let active_owner_is_expected = active_owner == self.expected_owner;
                let active_owner_is_shutting_down = active_owner_is_expected
                    && unsafe { (*(*active_owner).state_ptr()).shutting_down };
                self.saw_active_shutdown_context
                    .set(active_owner_is_shutting_down);
                let child = TrackedChild {
                    polls: Rc::clone(&self.child_polls),
                    drops: Rc::clone(&self.child_drops),
                };
                match Executor::try_spawn(child) {
                    Err(TrySpawnError::NoExecutor { future }) => {
                        self.outcome.set(SpawnOutcome::NoExecutor);
                        drop(future);
                    }
                    Err(error) => {
                        self.outcome.set(SpawnOutcome::OtherError);
                        drop(error.into_future());
                    }
                    Ok(handle) => {
                        self.outcome.set(SpawnOutcome::Admitted);
                        drop(handle);
                    }
                }
            }
        }

        let parent_polls = Rc::new(Cell::new(0));
        let parent_drops = Rc::new(Cell::new(0));
        let child_polls = Rc::new(Cell::new(0));
        let child_drops = Rc::new(Cell::new(0));
        let outcome = Rc::new(Cell::new(SpawnOutcome::NotAttempted));
        let saw_active_shutdown_context = Rc::new(Cell::new(false));
        let mut executor = Executor::new().expect("executor construction failed");
        let expected_owner = Rc::as_ptr(&executor.owner);
        assert!(size_of::<JoinTask<TrackedChild>>() <= TASK_POOL_SIZE);
        assert!(align_of::<JoinTask<TrackedChild>>() <= TASK_DATA_ALIGN);

        let err = executor
            .run(SpawnChildOnDrop {
                polls: Rc::clone(&parent_polls),
                drops: Rc::clone(&parent_drops),
                child_polls: Rc::clone(&child_polls),
                child_drops: Rc::clone(&child_drops),
                outcome: Rc::clone(&outcome),
                expected_owner,
                saw_active_shutdown_context: Rc::clone(&saw_active_shutdown_context),
            })
            .expect_err("pending task should leave the executor stalled");
        assert_eq!(err.kind(), ErrorKind::WouldBlock);
        assert_eq!(parent_polls.get(), 1);
        assert_eq!(parent_drops.get(), 0);

        let state_ptr = executor.owner.state_ptr();
        unsafe {
            assert_eq!((*state_ptr).runtime_state.live_tasks, 1);
            assert!(!(*state_ptr).all_tasks.is_empty());
            assert!((*state_ptr).ready_queue.is_empty());
            assert!(!(*state_ptr).shutdown_complete);
            #[cfg(debug_assertions)]
            {
                assert_eq!((*state_ptr).runtime_state.stats.task_allocs, 1);
                assert_eq!((*state_ptr).runtime_state.stats.task_frees, 0);
            }
        }

        executor.shutdown_owner();

        assert!(
            saw_active_shutdown_context.get(),
            "task destructor did not observe its active owner in shutdown"
        );
        assert_eq!(outcome.get(), SpawnOutcome::NoExecutor);
        assert_eq!(parent_drops.get(), 1);
        assert_eq!(child_polls.get(), 0, "rejected child future was polled");
        assert_eq!(child_drops.get(), 1, "returned child future drop count");
        unsafe {
            assert_eq!((*state_ptr).runtime_state.live_tasks, 0);
            assert!((*state_ptr).all_tasks.is_empty());
            assert!((*state_ptr).ready_queue.is_empty());
            assert!((*state_ptr).shutdown_complete);
            #[cfg(debug_assertions)]
            {
                assert_eq!((*state_ptr).runtime_state.stats.task_allocs, 1);
                assert_eq!((*state_ptr).runtime_state.stats.task_frees, 1);
            }
        }

        executor.shutdown_owner();
        drop(executor);
        assert_eq!(parent_drops.get(), 1, "parent destructor ran twice");
        assert_eq!(child_drops.get(), 1, "returned child was dropped twice");
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

    #[cfg(not(miri))]
    #[repr(align(64))]
    struct Align64Payload;

    #[repr(align(128))]
    struct Align128Payload;

    struct NearSlotSpawnFuture {
        bytes: [u8; 4000],
        drops: Rc<Cell<usize>>,
    }

    impl Future for NearSlotSpawnFuture {
        type Output = ();

        fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
            Poll::Ready(())
        }
    }

    impl Drop for NearSlotSpawnFuture {
        fn drop(&mut self) {
            self.drops.set(self.drops.get() + 1);
        }
    }

    struct AlignedSpawnFuture<A> {
        id: usize,
        drops: Rc<Cell<usize>>,
        polls: Rc<Cell<usize>>,
        _payload: A,
    }

    impl<A> AlignedSpawnFuture<A> {
        fn new(id: usize, drops: &Rc<Cell<usize>>, polls: &Rc<Cell<usize>>, payload: A) -> Self {
            Self {
                id,
                drops: Rc::clone(drops),
                polls: Rc::clone(polls),
                _payload: payload,
            }
        }
    }

    impl<A: Unpin> Future for AlignedSpawnFuture<A> {
        type Output = usize;

        fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
            let this = self.get_mut();
            this.polls.set(this.polls.get() + 1);
            Poll::Ready(this.id)
        }
    }

    impl<A> Drop for AlignedSpawnFuture<A> {
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
    fn task_payload_storage_has_exact_64_byte_alignment() {
        assert_eq!(TASK_DATA_ALIGN, 64);
        assert_eq!(
            std::mem::offset_of!(Task<TASK_POOL_SIZE>, data),
            128,
            "task payload offset changed"
        );
        assert_eq!(
            size_of::<Task<TASK_POOL_SIZE>>(),
            4224,
            "task slot geometry grew"
        );

        let mut slot = std::mem::MaybeUninit::<Task<TASK_POOL_SIZE>>::uninit();
        let data_ptr = unsafe { std::ptr::addr_of_mut!((*slot.as_mut_ptr()).data) };
        assert_eq!(
            data_ptr as usize % TASK_DATA_ALIGN,
            0,
            "placed task payload does not honor the advertised alignment"
        );
    }

    #[test]
    fn join_task_initializer_writes_near_capacity_future_in_final_slot() {
        let drops = Rc::new(Cell::new(0));
        assert!(size_of::<JoinTask<NearSlotSpawnFuture>>() <= TASK_POOL_SIZE);
        assert!(
            size_of::<JoinTask<NearSlotSpawnFuture>>() > TASK_POOL_SIZE - 128,
            "regression future no longer exercises a near-capacity join payload"
        );

        let mut slot = std::mem::MaybeUninit::<Task<TASK_POOL_SIZE>>::uninit();
        let data_ptr = unsafe { std::ptr::addr_of_mut!((*slot.as_mut_ptr()).data) };
        let join_ptr = data_ptr.cast::<JoinTask<NearSlotSpawnFuture>>();
        let future = NearSlotSpawnFuture {
            bytes: [0xA5; 4000],
            drops: Rc::clone(&drops),
        };

        unsafe { init_join_task_at(join_ptr, future) };
        let join_task = unsafe { &*join_ptr };
        assert_eq!(
            join_task
                .future
                .as_ref()
                .expect("future was not initialized")
                .bytes[3999],
            0xA5
        );
        assert!(join_task.result.is_none());
        assert!(join_task.join_waker.is_none());
        assert_eq!(drops.get(), 0);

        unsafe { std::ptr::drop_in_place(join_ptr) };
        assert_eq!(drops.get(), 1);
    }

    #[cfg(not(miri))]
    #[test]
    fn try_spawn_accepts_64_byte_aligned_future() {
        let drops = Rc::new(Cell::new(0usize));
        let polls = Rc::new(Cell::new(0usize));
        let completed = Rc::new(Cell::new(0usize));
        let completed_task = Rc::clone(&completed);
        let future = AlignedSpawnFuture::new(313, &drops, &polls, Align64Payload);
        assert_eq!(
            align_of::<JoinTask<AlignedSpawnFuture<Align64Payload>>>(),
            TASK_DATA_ALIGN
        );

        let mut executor = Executor::new().expect("executor construction failed");
        executor
            .run(async move {
                let output = Executor::spawn(future)
                    .expect("64-byte-aligned task should spawn")
                    .await
                    .expect("64-byte-aligned task should complete");
                completed_task.set(output);
            })
            .expect("executor failed while running aligned task");

        assert_eq!(completed.get(), 313);
        assert_eq!(polls.get(), 1);
        assert_eq!(drops.get(), 1);
    }

    #[test]
    fn try_spawn_rejects_overaligned_future_before_task_slot_write() {
        type OveralignedSpawnFuture = AlignedSpawnFuture<Align128Payload>;

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

        let future = OveralignedSpawnFuture::new(313, &drops, &polls, Align128Payload);
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
