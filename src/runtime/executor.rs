//! Executor and scheduler entry points for the runtime.
//!
//! The executor owns the reactor, task pool, ready queue, and timer runtime
//! for one thread. It is intended to be long-lived: construct it once, then
//! run application tasks inside it.
//!
//! # Fast-Path Guidance
//!
//! Best fast-path choices:
//! - Use a single executor instance to drive many tasks and I/O completions
//!   over time.
//! - Use [`Executor::spawn`] from inside [`Executor::run`] to add concurrent
//!   work without rebuilding runtime state.
//! - Use [`Executor::try_spawn`] when the caller must keep
//!   ownership of the submitted future if the scheduler cannot accept it, such
//!   as RPC generated-method dispatch.
//!
//! Prefer not to use on the fast path:
//! - Prefer not to construct a fresh [`Executor`] around each operation or
//!   request. That is setup/teardown work, not steady-state execution.
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
};
use crate::runtime::timer::TimerRuntime;
use crate::utils::list::intrusive::dlist::DList;
use crate::utils::memory::pool::Pool;
use crate::utils::memory::provider::MemoryProvider;
use io_uring::{opcode, squeue, types};
use std::alloc::{Layout, alloc};
use std::cell::Cell;
use std::future::Future;
use std::io;
use std::io::ErrorKind;
use std::mem::ManuallyDrop;
use std::mem::size_of;
use std::os::fd::RawFd;
use std::pin::Pin;
use std::task::{Context, Poll, Waker};

/// Default per-phase cap for one executor loop pass.
///
/// The executor applies this limit separately to ready-task polling, CQE
/// draining, and timer processing so no single queue type monopolizes a pass.
pub const DEFAULT_PROCESS_QUOTA: usize = 128;
/// Bytes reserved for each fixed executor task slot.
const TASK_POOL_SIZE: usize = 4096;
/// Number of task slots allocated per task-pool slab page.
const TASKS_PER_SLAB: usize = 1024;

/// Lightweight counters for benchmarking and scheduler inspection.
///
/// This is a debug-build observability snapshot, not a fast-path type. Read it
/// out of band, for example after a run, rather than on a per-operation path.
///
/// # Example
/// ```
/// use flowio::runtime::executor::RuntimeStats;
///
/// let stats = RuntimeStats::default();
/// assert_eq!(stats.task_polls, 0);
/// ```
#[cfg(debug_assertions)]
#[derive(Clone, Copy, Default)]
pub struct RuntimeStats {
    /// Number of task slab pages requested from the memory provider.
    pub task_slab_allocs: usize,
    /// Number of task slab pages returned to the memory provider. Runtime
    /// snapshots normally stay at zero; task slabs are freed during executor
    /// teardown.
    pub task_slab_frees: usize,
    /// Number of task slots allocated from the task pool.
    pub task_allocs: usize,
    /// Number of task slots freed back to the task pool.
    pub task_frees: usize,
    /// Total number of times tasks were polled by the executor.
    pub task_polls: usize,
    /// Number of times a task was scheduled into the ready queue.
    pub task_schedules: usize,
    /// Number of SQEs pushed to the io_uring submission queue.
    pub sqe_submits: usize,
    /// Number of CQEs drained from the io_uring completion queue.
    pub cqe_completions: usize,
    /// Number of times a waiting task was woken by a retired CQE or an
    /// expired timer.
    pub waiter_wakes: usize,
    /// Number of `clock_gettime` calls for timer tick computation.
    pub timer_now_tick_calls: usize,
    /// Number of timer entries that expired and fired.
    pub timer_expired: usize,
    /// Retained operation payload allocations served by the private pool.
    pub retained_pooled_allocs: usize,
    /// Retained operation payload allocations served from a returned block.
    pub retained_pooled_reuses: usize,
    /// Retained operation payload blocks returned to the private pool.
    pub retained_pooled_frees: usize,
    /// Retained operation payload slab pages requested by the private pool.
    pub retained_slab_allocs: usize,
    /// Retained operation payloads that used the documented heap fallback.
    pub retained_heap_fallbacks: usize,
    /// Retained operation payload heap fallback blocks released.
    pub retained_heap_frees: usize,
    /// Retained vectored I/O scratch requests served by inline storage.
    pub writev_scratch_inline_allocs: usize,
    /// Retained vectored I/O scratch requests served by pooled sidecar storage.
    pub writev_scratch_pooled_allocs: usize,
    /// Retained vectored I/O scratch requests served from a returned block.
    pub writev_scratch_pooled_reuses: usize,
    /// Retained vectored I/O scratch sidecar blocks returned to the pool.
    pub writev_scratch_pooled_frees: usize,
    /// Retained vectored I/O scratch slab pages requested by the sidecar pool.
    pub writev_scratch_slab_allocs: usize,
    /// Vectored I/O requests rejected for exceeding the iovec limit.
    pub writev_scratch_oversize_rejections: usize,
    /// Vectored I/O scratch sidecar allocation failures.
    pub writev_scratch_alloc_failures: usize,
}

struct ExecutorTaskMemProvider {
    /// Minimum alignment guaranteed for task slab allocations.
    alignment: usize,
    #[cfg(any(debug_assertions, test))]
    /// Number of task slab allocations requested since the last debug reset.
    request_count: usize,
    #[cfg(debug_assertions)]
    /// Number of task slab frees issued since the last debug reset.
    free_count: usize,
    #[cfg(test)]
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
            #[cfg(test)]
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

impl MemoryProvider for ExecutorTaskMemProvider {
    fn init(&mut self, required_align: usize) {
        self.alignment = std::cmp::max(self.alignment, required_align);
    }

    fn alignment_guarantee(&self) -> usize {
        self.alignment
    }

    fn request_memory(&mut self, size: usize) -> Option<*mut u8> {
        #[cfg(test)]
        {
            if self
                .max_request_count
                .is_some_and(|max_requests| self.request_count >= max_requests)
            {
                return None;
            }
        }

        let layout = Layout::from_size_align(size, self.alignment).ok()?;
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
    /// processing fair within one loop pass.
    pub process_quota: usize,
    /// Optional zero-based CPU id to pin the loop thread to on Linux.
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
    /// Number of live tasks currently owned by the executor.
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

#[derive(Clone, Copy)]
#[doc(hidden)]
pub struct ScheduleCtx {
    /// Ready queue the woken task should be pushed onto.
    pub(crate) ready_queue: *mut DList<TaskHeader>,
    /// Shared runtime state updated during wake/schedule transitions.
    pub(crate) runtime_state: *mut RuntimeState,
}

/// Per-thread executor context stored as a raw pointer in thread-local storage.
///
/// The struct lives on the stack of [`Executor::run`] and a `*mut ThreadCtx` is
/// placed in the TLS cell.  All runtime-internal functions read through this
/// pointer — a single 8-byte TLS load — instead of copying the full struct.
/// `owner_task` is set/cleared via raw pointer write around each task poll so
/// that I/O futures can capture their owning task without an additional TLS
/// lookup.
#[derive(Clone, Copy)]
struct ThreadCtx {
    /// Main ready queue for runnable tasks.
    ready_queue: *mut DList<TaskHeader>,
    /// Reactor used for SQE submission and CQE polling.
    reactor: *mut Reactor,
    /// Pool from which runtime task storage is allocated.
    task_pool: *mut Pool<'static, Task<TASK_POOL_SIZE>, ExecutorTaskMemProvider>,
    /// Shared executor counters and lifecycle state.
    runtime_state: *mut RuntimeState,
    /// Runtime-owned timer subsystem for sleeps and deadlines.
    timers: *mut TimerRuntime,
    /// The task currently being polled. Set before `vtable.poll()`, cleared
    /// after. I/O futures access this through `TaskHeader.ctx` -> `ThreadCtx`
    /// to register themselves as the waiter for the submitted operation.
    owner_task: *mut TaskHeader,
}

thread_local! {
    static EXECUTOR_CTX: Cell<*mut ThreadCtx> = const { Cell::new(std::ptr::null_mut()) };
}

struct ExecutorCtxGuard;

impl ExecutorCtxGuard {
    #[inline(always)]
    fn set(ctx_ptr: *mut ThreadCtx) -> Self {
        EXECUTOR_CTX.with(|ctx_cell| ctx_cell.set(ctx_ptr));
        Self
    }
}

impl Drop for ExecutorCtxGuard {
    #[inline(always)]
    fn drop(&mut self) {
        EXECUTOR_CTX.with(|ctx_cell| ctx_cell.set(std::ptr::null_mut()));
    }
}

/// Thin handle to the executor's thread-local context, extracted from the
/// waker without any TLS reads.  Stores a single pointer (8 bytes) instead
/// of copying individual fields.
#[doc(hidden)]
pub(crate) struct PollCtx {
    /// Pointer to the executor thread context active for the current poll.
    ctx: *const ThreadCtx,
}

impl PollCtx {
    #[inline(always)]
    pub fn reactor(&self) -> *mut Reactor {
        unsafe { (*self.ctx).reactor }
    }

    #[inline(always)]
    pub fn runtime_state(&self) -> *mut RuntimeState {
        unsafe { (*self.ctx).runtime_state }
    }

    #[inline(always)]
    pub fn owner_task(&self) -> *mut TaskHeader {
        unsafe { (*self.ctx).owner_task }
    }
}

/// Extract reactor and task pointers for use in I/O future poll paths.
/// Zero TLS reads — extracts the TaskHeader pointer from the waker's internal
/// layout, then reads the `ctx` field set by the executor before each poll.
///
/// # Safety
///
/// Must only be called inside a `poll` invoked by our executor.  Relies on
/// `Waker` being laid out as `(vtable_ptr, data_ptr)` — verified at compile
/// time by the static assert below.
#[inline(always)]
#[doc(hidden)]
pub(crate) unsafe fn poll_ctx_from_waker(cx: &std::task::Context) -> PollCtx {
    let waker_ptr = cx.waker() as *const std::task::Waker as *const *const ();
    let task_ptr = unsafe { *waker_ptr.add(1) } as *mut TaskHeader;
    let raw_ctx = unsafe { (*task_ptr).ctx.get() };
    PollCtx {
        ctx: raw_ctx as *const ThreadCtx,
    }
}

#[inline(always)]
#[doc(hidden)]
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

    let pctx = unsafe { poll_ctx_from_waker(cx) };
    unsafe { (*state_ptr).register_waiter(pctx.owner_task()) };
}

// Compile-time check: `Waker` must stay as two pointers (vtable + data) for
// `poll_ctx_from_waker` to remain valid.
const _: [(); std::mem::size_of::<std::task::Waker>()] = [(); 2 * std::mem::size_of::<*const ()>()];

// ---------------------------------------------------------------------------
// JoinHandle
// ---------------------------------------------------------------------------

/// Internal wrapper stored in the task data area.  Holds the user's future,
/// the result slot, and an optional waker for the JoinHandle.
#[repr(C)]
struct JoinTask<F: Future> {
    /// Spawned future until it completes and is dropped.
    future: Option<F>,
    /// Completed task output, taken by the join handle.
    result: Option<F::Output>,
    /// Last join-handle waker registered while waiting for the result. Woken
    /// once when the spawned future stores its output.
    join_waker: Option<Waker>,
}

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
    /// slot.
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

/// Handle returned by [`Executor::spawn`] that can be `.await`ed to obtain
/// the spawned task's return value.
///
/// Awaiting or dropping a handle is part of the steady-state task path, as the
/// await side of [`Executor::spawn`] / [`Executor::try_spawn`]. It does not
/// allocate.
///
/// Dropping the handle without awaiting detaches the task — it continues
/// running but its result is discarded.
///
/// # Example
/// ```no_run
/// use flowio::runtime::executor::Executor;
///
/// let mut executor = Executor::new()?;
/// executor.run(async {
///     let handle = Executor::spawn(async { 42 }).unwrap();
///     assert_eq!(handle.await, 42);
/// })?;
/// # Ok::<(), std::io::Error>(())
/// ```
pub struct JoinHandle<T: 'static> {
    /// Owning task header kept alive while the handle exists.
    task_ptr: *mut TaskHeader,
    /// Pointer to the `Option<T>` result slot inside the task's JoinTask.
    result_ptr: *mut Option<T>,
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
    type Output = T;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<T> {
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
    /// Reactor driving all kernel-visible I/O submissions and completions.
    pub(crate) reactor: Reactor,
    /// Maximum number of items processed per phase (ready tasks, CQEs,
    /// timer expiries) in each executor loop iteration.
    pub process_quota: usize,
    /// CPU core to pin the executor thread to via `sched_setaffinity`.
    /// `None` means no pinning. On non-Linux targets, `Some(_)` is rejected
    /// as unsupported.
    pub cpu_affinity: Option<usize>,
    #[cfg(debug_assertions)]
    /// Scheduler counters captured after the most recent completed run.
    last_stats: RuntimeStats,
    /// Pool storing task allocations with stable addresses.
    task_pool: ManuallyDrop<Pool<'static, Task<TASK_POOL_SIZE>, ExecutorTaskMemProvider>>,
    /// Main queue of runnable tasks.
    ready_queue: ManuallyDrop<DList<TaskHeader>>,
    /// Runtime timer subsystem shared by all sleeps and deadlines.
    timers: ManuallyDrop<TimerRuntime>,
    /// Backing memory provider for the task pool.
    provider: Box<ExecutorTaskMemProvider>,
    /// Set after one-time intrusive/runtime initialization is complete.
    initialized: bool,
}

impl Executor {
    /// Constructs an executor with default configuration.
    ///
    /// This is a setup/control-plane API. Typical applications construct one
    /// executor per runtime thread and keep it alive rather than recreating it
    /// in the steady-state fast path.
    ///
    /// # Errors
    /// Returns `Unsupported` when the running Linux kernel does not provide
    /// the `IORING_ENTER_EXT_ARG` feature required for timed `io_uring` waits.
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
        let mut provider = Box::new(ExecutorTaskMemProvider::new());
        let provider_ptr = &mut *provider as *mut ExecutorTaskMemProvider;

        let task_pool = ManuallyDrop::new(
            Pool::new_uninit(unsafe { &mut *provider_ptr }, TASKS_PER_SLAB)
                .map_err(|_| io::Error::from(ErrorKind::InvalidInput))?,
        );
        let ready_queue = ManuallyDrop::new(DList::new_uninit());

        Ok(Self {
            reactor: Reactor::new_with_config(config.reactor)?,
            process_quota: if config.process_quota == 0 {
                DEFAULT_PROCESS_QUOTA
            } else {
                config.process_quota
            },
            cpu_affinity: config.cpu_affinity,
            #[cfg(debug_assertions)]
            last_stats: RuntimeStats::default(),
            task_pool,
            ready_queue,
            timers: ManuallyDrop::new(TimerRuntime::new()?),
            provider,
            initialized: false,
        })
    }

    /// Performs one-time initialization for the executor's intrusive
    /// structures and runtime-owned subsystems.
    fn init(&mut self) -> io::Result<()> {
        if self.initialized {
            return Ok(());
        }

        self.task_pool.init();
        self.ready_queue.init();
        self.timers.init()?;
        self.reactor.init();
        self.initialized = true;
        Ok(())
    }

    /// Spawns a task onto the currently-running executor, returning a
    /// [`JoinHandle`] that can be `.await`ed to obtain the task's result.
    ///
    /// Dropping the handle without awaiting detaches the task — it continues
    /// running but its return value is discarded.
    ///
    /// This must be called from within [`Executor::run`]. For steady-state
    /// concurrency, this is the fast-path way to add work without rebuilding
    /// the executor.
    ///
    /// # Example
    /// ```no_run
    /// use flowio::runtime::executor::Executor;
    ///
    /// let mut executor = Executor::new()?;
    /// executor.run(async {
    ///     let handle = Executor::spawn(async { 42 }).unwrap();
    ///     assert_eq!(handle.await, 42);
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
    /// scheduler pressure. For example, generated RPC method dispatch may own
    /// an active answer inside the future; if the executor cannot accept the
    /// work, the caller needs the future back so it can retry, reject, or clean
    /// up explicitly.
    ///
    /// On success, ownership transfers to the executor exactly as with
    /// [`Executor::spawn`], and the returned [`JoinHandle`] yields the future's
    /// output. On failure, the future has not been polled, pinned, stored in a
    /// task slot, or dropped by the executor path.
    pub fn try_spawn<F>(future: F) -> Result<JoinHandle<F::Output>, TrySpawnError<F>>
    where
        F: Future + 'static,
        F::Output: 'static,
    {
        EXECUTOR_CTX.with(|ctx_cell| {
            let ctx_ptr = ctx_cell.get();
            if ctx_ptr.is_null() {
                return Err(TrySpawnError::NoExecutor { future });
            }
            let ctx = unsafe { &*ctx_ptr };

            if size_of::<JoinTask<F>>() > TASK_POOL_SIZE {
                return Err(TrySpawnError::TaskTooLarge { future });
            }

            unsafe {
                let slot_ptr = match (*ctx.task_pool).alloc(()) {
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

                // Compute pointers to the result and waker slots for the JoinHandle.
                let result_ptr = std::ptr::addr_of_mut!((*data_ptr).result);
                let waker_ptr = std::ptr::addr_of_mut!((*data_ptr).join_waker);

                (*slot_ptr).header.ready_link =
                    crate::utils::list::intrusive::dlist::Link::new_unlinked();
                // Start with refcount 2: one for the executor, one for the JoinHandle.
                (*slot_ptr).header.refs.set(2);
                (*slot_ptr)
                    .header
                    .flags
                    .set(TaskHeader::FLAG_NOTIFIED | TaskHeader::FLAG_QUEUED);
                (*slot_ptr).header.last_wake_epoch.set(0);
                init_cached_waker(&mut (*slot_ptr).header as *mut _);
                (*slot_ptr).header.vtable = join_task_vtable_for::<F>();

                (*ctx.runtime_state).live_tasks += 1;
                #[cfg(debug_assertions)]
                {
                    (*ctx.runtime_state).stats.task_allocs += 1;
                }
                (*ctx.ready_queue)
                    .push_back_unchecked(&mut (*slot_ptr).header.ready_link as *mut _);

                Ok(JoinHandle {
                    task_ptr: &mut (*slot_ptr).header as *mut TaskHeader,
                    result_ptr,
                    waker_ptr,
                })
            }
        })
    }

    /// Runs the root future and continues until the executor drains all work.
    ///
    /// Call this once around a top-level task tree. The intended usage is a
    /// long-lived `run` boundary, not repeatedly entering and exiting the
    /// executor for tiny units of work.
    ///
    /// # Errors
    ///
    /// Returns [`io::ErrorKind::WouldBlock`] if live runtime work remains but
    /// there are no ready tasks, in-flight I/O operations, or timers that can
    /// make progress. Reactor and timer I/O errors are propagated. Signal
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
    ///     assert_eq!(handle.await, 2);
    /// })?;
    /// # Ok::<(), std::io::Error>(())
    /// ```
    pub fn run<F: Future<Output = ()> + 'static>(&mut self, initial_task: F) -> io::Result<()> {
        self.init()?;
        self.provider.reset_debug_counts();
        apply_cpu_affinity(self.cpu_affinity)?;

        let mut runtime_state = RuntimeState::new();
        let mut thread_ctx = ThreadCtx {
            ready_queue: &mut *self.ready_queue as *mut _,
            reactor: &mut self.reactor as *mut _,
            task_pool: &mut *self.task_pool as *mut _,
            runtime_state: &mut runtime_state as *mut _,
            timers: &mut *self.timers as *mut _,
            owner_task: std::ptr::null_mut(),
        };
        let ctx_ptr = &mut thread_ctx as *mut ThreadCtx;
        let _ctx_guard = ExecutorCtxGuard::set(ctx_ptr);

        match Self::spawn(initial_task) {
            Ok(_handle) => { /* drop JoinHandle — root task is detached */ }
            Err(err) => {
                return Err(err);
            }
        }

        'run_loop: loop {
            self.timers.begin_executor_pass();
            let mut polled = 0usize;
            while polled < self.process_quota {
                let header_ptr =
                    unsafe { self.ready_queue.pop_front(TaskHeader::READY_LINK_OFFSET) };
                let Some(header_ptr) = header_ptr else {
                    break;
                };

                let header = unsafe { &*header_ptr };
                // Batch flag update: clear QUEUED+NOTIFIED, set RUNNING — one read + one write.
                header.flags.set(
                    (header.flags.get() & !(TaskHeader::FLAG_QUEUED | TaskHeader::FLAG_NOTIFIED))
                        | TaskHeader::FLAG_RUNNING,
                );
                unsafe { std::ptr::addr_of_mut!((*ctx_ptr).owner_task).write(header_ptr) };
                header.ctx.set(ctx_ptr as *mut ());
                #[cfg(debug_assertions)]
                {
                    runtime_state.stats.task_polls += 1;
                }
                let poll_res = unsafe { (header.vtable.poll)(header_ptr) };
                unsafe {
                    std::ptr::addr_of_mut!((*ctx_ptr).owner_task).write(std::ptr::null_mut())
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
                        (header.vtable.finish)(header_ptr);
                        release_task(header_ptr);
                    }
                } else {
                    let flags = header.flags.get();
                    // Clear RUNNING. If NOTIFIED was set during poll, re-enqueue.
                    header.flags.set(flags & !TaskHeader::FLAG_RUNNING);
                    if (flags & TaskHeader::FLAG_NOTIFIED) != 0 {
                        unsafe {
                            enqueue_notified_task_unchecked(
                                header_ptr,
                                &mut *self.ready_queue,
                                &mut runtime_state,
                            );
                        }
                    }
                }

                polled += 1;
            }

            if self.reactor.flush_sqes()? == ReactorSubmitStatus::Busy {
                let _ = self.reactor.poll_io(
                    self.process_quota,
                    &mut runtime_state as *mut RuntimeState,
                    &mut *self.ready_queue as *mut DList<TaskHeader>,
                )?;
                if self.timers.has_pending() {
                    let now_tick = self.timers.now_tick()?;
                    let _ = self
                        .timers
                        .process_at_with_budget(now_tick, self.process_quota)?;
                }
                continue;
            }
            let completed = self.reactor.poll_io(
                self.process_quota,
                &mut runtime_state as *mut RuntimeState,
                &mut *self.ready_queue as *mut DList<TaskHeader>,
            )?;
            let timers_pending = self.timers.has_pending();
            let mut now_tick = None;
            let timer_budget_exhausted = if timers_pending {
                let tick = self.timers.now_tick()?;
                now_tick = Some(tick);
                self.timers
                    .process_at_with_budget(tick, self.process_quota)?
            } else {
                false
            };
            let queue_empty = self.ready_queue.is_empty();
            let drained = runtime_state.live_tasks == 0
                && runtime_state.inflight_ops == 0
                && !timers_pending
                && queue_empty;

            if drained {
                #[cfg(debug_assertions)]
                {
                    self.snapshot_stats(&mut runtime_state);
                }
                return Ok(());
            }

            if completed > 0 || !queue_empty || timer_budget_exhausted {
                continue;
            }

            let timer_wait = match now_tick {
                Some(tick) => self.timers.next_wait_duration(tick),
                None => None,
            };

            if runtime_state.inflight_ops == 0 && timer_wait.is_none() {
                #[cfg(debug_assertions)]
                {
                    self.snapshot_stats(&mut runtime_state);
                }
                return Err(io::Error::from(ErrorKind::WouldBlock));
            }

            if matches!(timer_wait, Some(duration) if duration.is_zero()) {
                let _ = self
                    .timers
                    // SAFETY: now_tick is Some when timer_wait is Some (set in the
                    // has_pending() branch above).
                    .process_at_with_budget(
                        unsafe { now_tick.unwrap_unchecked() },
                        self.process_quota,
                    )?;
                continue;
            }

            if self.reactor.wait_for_events(timer_wait)? == ReactorSubmitStatus::Busy {
                let _ = self.reactor.poll_io(
                    self.process_quota,
                    &mut runtime_state as *mut RuntimeState,
                    &mut *self.ready_queue as *mut DList<TaskHeader>,
                )?;
                if self.timers.has_pending() {
                    let now_tick = self.timers.now_tick()?;
                    let _ = self
                        .timers
                        .process_at_with_budget(now_tick, self.process_quota)?;
                }
                continue 'run_loop;
            }
            let _ = self.reactor.poll_io(
                self.process_quota,
                &mut runtime_state as *mut RuntimeState,
                &mut *self.ready_queue as *mut DList<TaskHeader>,
            )?;
            if self.timers.has_pending() {
                let now_tick = self.timers.now_tick()?;
                let _ = self
                    .timers
                    .process_at_with_budget(now_tick, self.process_quota)?;
            }
        }
    }

    #[cfg(debug_assertions)]
    fn snapshot_stats(&mut self, runtime_state: &mut RuntimeState) {
        runtime_state.stats.task_slab_allocs = self.provider.request_count;
        runtime_state.stats.task_slab_frees = self.provider.free_count;
        let retained = self.reactor.retained_payload_stats();
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

    /// Returns scheduler counters captured for the most recently completed run.
    #[cfg(debug_assertions)]
    pub fn last_stats(&self) -> RuntimeStats {
        self.last_stats
    }
}

#[cfg(target_os = "linux")]
fn apply_cpu_affinity(cpu_affinity: Option<usize>) -> io::Result<()> {
    let Some(cpu) = cpu_affinity else {
        return Ok(());
    };

    let mut set = unsafe { std::mem::zeroed::<libc::cpu_set_t>() };
    unsafe {
        libc::CPU_ZERO(&mut set);
        libc::CPU_SET(cpu, &mut set);
    }

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
        // `ready_queue` and `task_pool` are `new_uninit` until `init()` runs,
        // so they may only be unlinked/dropped once `initialized` is set.
        if self.initialized {
            unsafe {
                self.ready_queue.unlink_all_for_drop();
                ManuallyDrop::drop(&mut self.ready_queue);
                #[cfg(debug_assertions)]
                self.task_pool.abandon_live_slots_for_drop();
                ManuallyDrop::drop(&mut self.task_pool);
            }
        }
        // `timers` is always fully constructed by `new_with_config`; its own
        // Drop handles its internal uninitialized pool. Drop it on every path
        // so constructing an executor and never running it does not leak timer
        // state.
        unsafe {
            ManuallyDrop::drop(&mut self.timers);
        }
    }
}

/// Cancel an in-flight operation from a future's `Drop` impl.
/// Marks the `CompletionState` as orphaned and submits `ASYNC_CANCEL`.
/// Uses one TLS read and only runs on the cancellation path.
unsafe fn cancel_op_unchecked(ptr: *mut crate::runtime::op::CompletionState) {
    EXECUTOR_CTX.with(|ctx_cell| {
        let ctx_ptr = ctx_cell.get();
        if ctx_ptr.is_null() {
            return;
        }
        let ctx = unsafe { &*ctx_ptr };
        unsafe { (*ctx.reactor).cancel_op(ptr) };
    });
}

/// Free a completed `CompletionState` from a future's `Drop` impl when the
/// CQE has already been consumed but the future is dropped before polling the
/// result. Uses one TLS read and only runs on that drop-after-complete path.
///
/// Pool-slot and retained-payload reclamation require an active executor TLS
/// context. If this is called after the executor context has been cleared, it
/// cannot reach the reactor and therefore leaves operation-pool reclamation to
/// process teardown. Callers that own external resources, such as accepted
/// fds in a cached accept state, must release those resources themselves before
/// delegating here.
unsafe fn free_op_unchecked(ptr: *mut crate::runtime::op::CompletionState) {
    EXECUTOR_CTX.with(|ctx_cell| {
        let ctx_ptr = ctx_cell.get();
        if ctx_ptr.is_null() {
            return;
        }
        let ctx = unsafe { &*ctx_ptr };
        unsafe { (*ctx.reactor).free_op(ptr) };
    });
}

/// Release a future-owned `CompletionState` pointer from `Drop`.
/// Completed ops are freed immediately; pending ops are orphaned and cancelled.
/// Reclamation requires an active executor TLS context; without one this only
/// clears the caller's pointer after the attempted free/cancel path.
/// The caller's pointer is always cleared.
#[inline(always)]
#[doc(hidden)]
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
#[inline(always)]
#[doc(hidden)]
pub(crate) unsafe fn submit_tracked_sqe(
    pctx: &PollCtx,
    sqe: io_uring::squeue::Entry,
) -> io::Result<()> {
    #[cfg(debug_assertions)]
    if let Some(err) = crate::runtime::test_hooks::take_sqe_submit_failure() {
        return Err(err);
    }

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
#[inline(always)]
#[doc(hidden)]
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

#[inline(always)]
#[doc(hidden)]
pub(crate) fn submit_detached_close(pctx: &PollCtx, fd: RawFd) -> io::Result<()> {
    let reactor = pctx.reactor();
    let state_ptr = unsafe { (*reactor).alloc_op() };
    if state_ptr.is_null() {
        return Err(io::Error::from(io::ErrorKind::WouldBlock));
    }

    unsafe { (*state_ptr).set_detached() };

    let sqe = opcode::Close::new(types::Fd(fd))
        .build()
        .user_data(state_ptr as u64);

    unsafe {
        if let Err(err) = submit_tracked_sqe(pctx, sqe) {
            (*reactor).free_op(state_ptr);
            return Err(err);
        }
    }

    Ok(())
}

#[inline(always)]
#[doc(hidden)]
pub(crate) fn try_submit_detached_close(fd: RawFd) -> bool {
    EXECUTOR_CTX.with(|ctx_cell| {
        let ctx_ptr = ctx_cell.get();
        if ctx_ptr.is_null() {
            return false;
        }

        let pctx = PollCtx {
            ctx: ctx_ptr as *const ThreadCtx,
        };
        submit_detached_close(&pctx, fd).is_ok()
    })
}

#[inline(always)]
#[doc(hidden)]
pub fn note_waiter_wake() {
    #[cfg(debug_assertions)]
    EXECUTOR_CTX.with(|ctx_cell| {
        let ctx_ptr = ctx_cell.get();
        if ctx_ptr.is_null() {
            return;
        }
        unsafe {
            (*(*ctx_ptr).runtime_state).stats.waiter_wakes += 1;
        }
    });
}

#[inline(always)]
#[doc(hidden)]
pub fn note_timer_now_tick_call() {
    #[cfg(debug_assertions)]
    EXECUTOR_CTX.with(|ctx_cell| {
        let ctx_ptr = ctx_cell.get();
        if ctx_ptr.is_null() {
            return;
        }
        unsafe {
            (*(*ctx_ptr).runtime_state).stats.timer_now_tick_calls += 1;
        }
    });
}

#[inline(always)]
#[doc(hidden)]
pub fn note_timer_expired() {
    #[cfg(debug_assertions)]
    EXECUTOR_CTX.with(|ctx_cell| {
        let ctx_ptr = ctx_cell.get();
        if ctx_ptr.is_null() {
            return;
        }
        unsafe {
            (*(*ctx_ptr).runtime_state).stats.timer_expired += 1;
        }
    });
}

#[inline(always)]
#[doc(hidden)]
pub(crate) unsafe fn current_poll_owner_task_unchecked() -> *mut TaskHeader {
    EXECUTOR_CTX.with(|ctx_cell| {
        let ctx_ptr = ctx_cell.get();
        debug_assert!(
            !ctx_ptr.is_null(),
            "runtime poll owner requested outside task poll context"
        );
        unsafe { (*ctx_ptr).owner_task }
    })
}

/// # Safety
///
/// Must be called from within `Executor::run` on the executor thread. The
/// returned pointer is only valid while that run's TLS context is active; in
/// release builds a missing context is UB rather than a panic.
#[doc(hidden)]
pub unsafe fn timers_unchecked() -> *mut TimerRuntime {
    EXECUTOR_CTX.with(|ctx_cell| {
        let ctx_ptr = ctx_cell.get();
        debug_assert!(
            !ctx_ptr.is_null(),
            "runtime timers_unchecked requested outside executor context"
        );
        unsafe { (*ctx_ptr).timers }
    })
}

#[doc(hidden)]
pub(crate) unsafe fn timers_or_null() -> *mut TimerRuntime {
    EXECUTOR_CTX.with(|ctx_cell| {
        let ctx_ptr = ctx_cell.get();
        if ctx_ptr.is_null() {
            return std::ptr::null_mut();
        }
        unsafe { (*ctx_ptr).timers }
    })
}

/// # Safety
///
/// Must be called from within `Executor::run` on the executor thread. The
/// returned pointers are only valid for that run; in release builds a missing
/// context is UB rather than a panic.
#[inline(always)]
#[doc(hidden)]
pub unsafe fn schedule_ctx_unchecked() -> ScheduleCtx {
    EXECUTOR_CTX.with(|ctx_cell| {
        let ctx_ptr = ctx_cell.get();
        debug_assert!(
            !ctx_ptr.is_null(),
            "runtime schedule_ctx_unchecked requested outside executor context"
        );
        let ctx = unsafe { &*ctx_ptr };
        ScheduleCtx {
            ready_queue: ctx.ready_queue,
            runtime_state: ctx.runtime_state,
        }
    })
}

#[inline(always)]
#[doc(hidden)]
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
#[doc(hidden)]
pub unsafe fn next_timer_wake_epoch_unchecked(schedule_ctx: ScheduleCtx) -> u64 {
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
    struct VTableGen<F>(std::marker::PhantomData<F>);

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
                        jt.result = Some(value);
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
            destroy: |ptr| unsafe {
                // Drop any remaining JoinTask fields (unclaimed result, waker).
                let slot = &mut *(ptr as *mut Task<TASK_POOL_SIZE>);
                let jt = &mut *(slot.data.as_mut_ptr() as *mut JoinTask<F>);
                std::ptr::drop_in_place(jt);

                EXECUTOR_CTX.with(|ctx_cell| {
                    let ctx_ptr = ctx_cell.get();
                    if ctx_ptr.is_null() {
                        return;
                    }
                    let ctx = &*ctx_ptr;
                    (*ctx.task_pool).free(ptr as *mut Task<TASK_POOL_SIZE>);
                    (*ctx.runtime_state).live_tasks -= 1;
                    #[cfg(debug_assertions)]
                    {
                        (*ctx.runtime_state).stats.task_frees += 1;
                    }
                });
            },
        };
    }

    &VTableGen::<F>::VTABLE
}

unsafe fn schedule_task(task_ptr: *mut TaskHeader) {
    EXECUTOR_CTX.with(|ctx_cell| {
        let ctx_ptr = ctx_cell.get();
        if ctx_ptr.is_null() {
            return;
        }
        let ctx = unsafe { &*ctx_ptr };

        unsafe { notify_task_into_list_unchecked(task_ptr, ctx.ready_queue, ctx.runtime_state) };
    });
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
pub(crate) unsafe fn notify_task_into_list_unchecked(
    task_ptr: *mut TaskHeader,
    ready_list: *mut DList<TaskHeader>,
    runtime_state: *mut RuntimeState,
) -> bool {
    let header = unsafe { &mut *task_ptr };
    let flags = header.flags();
    if task_is_completed(flags) {
        return false;
    }

    if task_is_notified(flags) {
        return false;
    }
    header.set_flag(TaskHeader::FLAG_NOTIFIED);

    if task_can_enter_ready_queue(flags) {
        return unsafe { enqueue_ready_task_unchecked(task_ptr, ready_list, runtime_state) };
    }

    false
}

#[inline(always)]
unsafe fn enqueue_notified_task_unchecked(
    task_ptr: *mut TaskHeader,
    ready_list: *mut DList<TaskHeader>,
    runtime_state: *mut RuntimeState,
) -> bool {
    let header = unsafe { &mut *task_ptr };
    let flags = header.flags();
    if !task_is_notified(flags) {
        return false;
    }
    if !task_can_enter_ready_queue(flags) {
        return false;
    }

    unsafe { enqueue_ready_task_unchecked(task_ptr, ready_list, runtime_state) }
}

#[inline(always)]
unsafe fn enqueue_ready_task_unchecked(
    task_ptr: *mut TaskHeader,
    ready_list: *mut DList<TaskHeader>,
    _runtime_state: *mut RuntimeState,
) -> bool {
    let header = unsafe { &mut *task_ptr };
    debug_assert!(
        header.ready_link.is_unlinked(),
        "enqueue_ready_task attempted to enqueue an already-linked task"
    );
    debug_assert!(
        task_can_enter_ready_queue(header.flags()),
        "enqueue_ready_task attempted to enqueue a completed, running, or already queued task"
    );
    header.set_flag(TaskHeader::FLAG_QUEUED);
    #[cfg(debug_assertions)]
    {
        if !_runtime_state.is_null() {
            unsafe {
                (*_runtime_state).stats.task_schedules += 1;
            }
        }
    }
    unsafe {
        (*ready_list).push_back_unchecked(&mut header.ready_link as *mut _);
    }
    true
}

#[inline(always)]
#[doc(hidden)]
/// # Safety
/// - `task_ptr` must point to a live, non-freed `TaskHeader` within the
///   executor's task slab.
/// - The executor TLS context (`EXECUTOR_CTX`) must be active (i.e. this must
///   be called from within `Executor::run`).
/// - Must be called from the executor thread (single-threaded contract).
pub unsafe fn schedule_woken_task(task_ptr: *mut TaskHeader) {
    unsafe {
        schedule_task(task_ptr);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::Cell;
    use std::mem::ManuallyDrop;
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

    struct GateFuture {
        id: usize,
        release: Rc<Cell<bool>>,
        drops: Rc<Cell<usize>>,
        polls: Rc<Cell<usize>>,
    }

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

    impl Drop for GateFuture {
        fn drop(&mut self) {
            self.drops.set(self.drops.get() + 1);
        }
    }

    #[test]
    fn join_handle_pending_poll_reuses_same_waker() {
        let mut result = None::<usize>;
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
        let mut executor = Executor::new().expect("failed to construct executor");
        executor.provider.max_request_count = Some(1);
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
        assert!(header.has_flag(TaskHeader::FLAG_NOTIFIED));
        assert!(header.has_flag(TaskHeader::FLAG_QUEUED));
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
    fn notify_task_gate_defers_running_task_until_poll_finishes() {
        let mut ready_queue = DList::new_uninit();
        ready_queue.init();
        let mut runtime_state = RuntimeState::new();
        let mut header = TaskHeader::new();
        header.set_flag(TaskHeader::FLAG_RUNNING);
        let task_ptr = &mut header as *mut TaskHeader;

        assert!(!unsafe {
            notify_task_into_list_unchecked(task_ptr, &mut ready_queue, &mut runtime_state)
        });
        assert!(header.has_flag(TaskHeader::FLAG_NOTIFIED));
        assert!(!header.has_flag(TaskHeader::FLAG_QUEUED));
        assert!(ready_queue.is_empty());
        #[cfg(debug_assertions)]
        assert_eq!(runtime_state.stats.task_schedules, 0);

        header.clear_flag(TaskHeader::FLAG_RUNNING);
        assert!(unsafe {
            enqueue_notified_task_unchecked(task_ptr, &mut ready_queue, &mut runtime_state)
        });
        assert!(header.has_flag(TaskHeader::FLAG_QUEUED));
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
        header.set_flag(TaskHeader::FLAG_COMPLETED);
        let task_ptr = &mut header as *mut TaskHeader;

        assert!(!unsafe {
            notify_task_into_list_unchecked(task_ptr, &mut ready_queue, &mut runtime_state)
        });
        assert!(!header.has_flag(TaskHeader::FLAG_NOTIFIED));
        assert!(!header.has_flag(TaskHeader::FLAG_QUEUED));
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

        header.set_flag(TaskHeader::FLAG_NOTIFIED | TaskHeader::FLAG_QUEUED);
        assert!(!unsafe {
            enqueue_notified_task_unchecked(task_ptr, &mut ready_queue, &mut runtime_state)
        });
        assert!(ready_queue.is_empty());

        header.clear_flag(TaskHeader::FLAG_QUEUED);
        assert!(unsafe {
            enqueue_notified_task_unchecked(task_ptr, &mut ready_queue, &mut runtime_state)
        });
        assert!(header.has_flag(TaskHeader::FLAG_QUEUED));

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
            executor
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
