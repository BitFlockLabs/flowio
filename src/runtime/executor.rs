//! Executor and scheduler entry points for the runtime.
//!
//! # Example
//! ```no_run
//! use flowio::runtime::executor::Executor;
//!
//! let mut executor = Executor::new()?;
//! executor.run(async {})?;
//! # Ok::<(), std::io::Error>(())
//! ```

use crate::runtime::reactor::{Reactor, ReactorConfig};
use crate::runtime::task::{
    Task, TaskHeader, TaskVTable, cached_waker_ref, init_cached_waker, release_task,
};
use crate::runtime::timer::TimerRuntime;
use crate::utils::list::intrusive::dlist::DList;
use crate::utils::memory::pool::Pool;
use crate::utils::memory::provider::MemoryProvider;
use io_uring::{opcode, types};
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
const TASK_POOL_SIZE: usize = 4096;
const TASKS_PER_SLAB: usize = 1024;

/// Lightweight counters for benchmarking and scheduler inspection.
#[cfg(debug_assertions)]
#[derive(Clone, Copy, Default)]
pub struct RuntimeStats
{
    /// Number of task slab pages requested from the memory provider.
    pub task_slab_allocs: usize,
    /// Number of task slab pages returned to the memory provider.
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
    /// Number of times a completed CQE woke a waiting task.
    pub waiter_wakes: usize,
    /// Number of `clock_gettime` calls for timer tick computation.
    pub timer_now_tick_calls: usize,
    /// Number of timer entries that expired and fired.
    pub timer_expired: usize,
}

struct ExecutorTaskMemProvider
{
    alignment: usize,
    #[cfg(debug_assertions)]
    request_count: usize,
    #[cfg(debug_assertions)]
    free_count: usize,
}

impl ExecutorTaskMemProvider
{
    fn new() -> Self
    {
        Self {
            alignment: std::mem::align_of::<usize>(),
            #[cfg(debug_assertions)]
            request_count: 0,
            #[cfg(debug_assertions)]
            free_count: 0,
        }
    }

    #[inline(always)]
    fn note_request(&mut self)
    {
        #[cfg(debug_assertions)]
        {
            self.request_count += 1;
        }
    }

    #[inline(always)]
    fn note_free(&mut self)
    {
        #[cfg(debug_assertions)]
        {
            self.free_count += 1;
        }
    }

    #[inline(always)]
    fn reset_debug_counts(&mut self)
    {
        #[cfg(debug_assertions)]
        {
            self.request_count = 0;
            self.free_count = 0;
        }
    }
}

impl MemoryProvider for ExecutorTaskMemProvider
{
    fn init(&mut self, required_align: usize)
    {
        self.alignment = std::cmp::max(self.alignment, required_align);
    }

    fn alignment_guarantee(&self) -> usize
    {
        self.alignment
    }

    fn request_memory(&mut self, size: usize) -> Option<*mut u8>
    {
        let layout = Layout::from_size_align(size, self.alignment).ok()?;
        let ptr = unsafe { alloc(layout) };
        if ptr.is_null()
        {
            None
        }
        else
        {
            self.note_request();
            Some(ptr)
        }
    }

    unsafe fn free_memory(&mut self, ptr: *mut u8, size: usize)
    {
        if let Ok(layout) = Layout::from_size_align(size, self.alignment)
        {
            unsafe {
                std::alloc::dealloc(ptr, layout);
            }
            self.note_free();
        }
    }
}

/// User-facing runtime configuration.
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
pub struct ExecutorConfig
{
    /// io_uring reactor configuration used by the executor.
    pub reactor: ReactorConfig,
    /// Per-phase cap used to keep ready-task polling, CQE draining, and timer
    /// processing fair within one loop pass.
    pub process_quota: usize,
    /// Optional zero-based CPU id to pin the loop thread to on Linux.
    pub cpu_affinity: Option<usize>,
}

impl Default for ExecutorConfig
{
    fn default() -> Self
    {
        Self {
            reactor: ReactorConfig::default(),
            process_quota: DEFAULT_PROCESS_QUOTA,
            cpu_affinity: None,
        }
    }
}

pub(crate) struct RuntimeState
{
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

impl RuntimeState
{
    fn new() -> Self
    {
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
pub struct ScheduleCtx
{
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
struct ThreadCtx
{
    ready_queue: *mut DList<TaskHeader>,
    reactor: *mut Reactor,
    task_pool: *mut Pool<'static, Task<TASK_POOL_SIZE>, ExecutorTaskMemProvider>,
    runtime_state: *mut RuntimeState,
    timers: *mut TimerRuntime,
    /// The task currently being polled. Set before `vtable.poll()`, cleared
    /// after. I/O futures access this through `TaskHeader.ctx` -> `ThreadCtx`
    /// to register themselves as the waiter for the submitted operation.
    owner_task: *mut TaskHeader,
}

thread_local! {
    static EXECUTOR_CTX: Cell<*mut ThreadCtx> = const { Cell::new(std::ptr::null_mut()) };
}

/// Thin handle to the executor's thread-local context, extracted from the
/// waker without any TLS reads.  Stores a single pointer (8 bytes) instead
/// of copying individual fields.
#[doc(hidden)]
pub(crate) struct PollCtx
{
    /// Pointer to the executor thread context active for the current poll.
    ctx: *const ThreadCtx,
}

impl PollCtx
{
    #[inline(always)]
    pub fn reactor(&self) -> *mut Reactor
    {
        unsafe { (*self.ctx).reactor }
    }

    #[inline(always)]
    pub fn runtime_state(&self) -> *mut RuntimeState
    {
        unsafe { (*self.ctx).runtime_state }
    }

    #[inline(always)]
    pub fn owner_task(&self) -> *mut TaskHeader
    {
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
pub(crate) unsafe fn poll_ctx_from_waker(cx: &std::task::Context) -> PollCtx
{
    let waker_ptr = cx.waker() as *const std::task::Waker as *const *const ();
    let task_ptr = unsafe { *waker_ptr.add(1) } as *mut TaskHeader;
    let raw_ctx = unsafe { (*task_ptr).ctx.get() };
    PollCtx {
        ctx: raw_ctx as *const ThreadCtx,
    }
}

// Compile-time check: Waker must be exactly 2 pointers (vtable + data).
const _: [(); std::mem::size_of::<std::task::Waker>()] = [(); 2 * std::mem::size_of::<*const ()>()];

// ---------------------------------------------------------------------------
// JoinHandle
// ---------------------------------------------------------------------------

/// Internal wrapper stored in the task data area.  Holds the user's future,
/// the result slot, and an optional waker for the JoinHandle.
#[repr(C)]
struct JoinTask<F: Future>
{
    future: Option<F>,
    result: Option<F::Output>,
    join_waker: Option<Waker>,
}

/// Handle returned by [`Executor::spawn`] that can be `.await`ed to obtain
/// the spawned task's return value.
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
pub struct JoinHandle<T: 'static>
{
    task_ptr: *mut TaskHeader,
    /// Pointer to the `Option<T>` result slot inside the task's JoinTask.
    result_ptr: *mut Option<T>,
    /// Pointer to the `Option<Waker>` join_waker slot inside the task's JoinTask.
    waker_ptr: *mut Option<Waker>,
}

impl<T: 'static> JoinHandle<T>
{
    /// Returns `true` if the spawned task has completed and its result is
    /// available.  This is a non-blocking, non-consuming check.
    pub fn is_finished(&self) -> bool
    {
        unsafe { (*self.result_ptr).is_some() }
    }
}

impl<T: 'static> Future for JoinHandle<T>
{
    type Output = T;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<T>
    {
        let this = unsafe { self.get_unchecked_mut() };
        let result_slot = unsafe { &mut *this.result_ptr };

        if let Some(value) = result_slot.take()
        {
            return Poll::Ready(value);
        }

        let waker_slot = unsafe { &mut *this.waker_ptr };
        *waker_slot = Some(cx.waker().clone());
        Poll::Pending
    }
}

impl<T: 'static> Drop for JoinHandle<T>
{
    fn drop(&mut self)
    {
        unsafe {
            release_task(self.task_ptr);
        }
    }
}

/// Single-threaded executor that drives tasks and `io_uring` completions.
///
/// # Example
/// ```no_run
/// use flowio::runtime::executor::Executor;
///
/// let mut executor = Executor::new()?;
/// executor.run(async {})?;
/// # Ok::<(), std::io::Error>(())
/// ```
pub struct Executor
{
    pub(crate) reactor: Reactor,
    /// Maximum number of items processed per phase (ready tasks, CQEs,
    /// timer expiries) in each executor loop iteration.
    pub process_quota: usize,
    /// CPU core to pin the executor thread to via `sched_setaffinity`.
    /// `None` means no pinning.
    pub cpu_affinity: Option<usize>,
    #[cfg(debug_assertions)]
    last_stats: RuntimeStats,
    task_pool: ManuallyDrop<Pool<'static, Task<TASK_POOL_SIZE>, ExecutorTaskMemProvider>>,
    ready_queue: ManuallyDrop<DList<TaskHeader>>,
    timers: ManuallyDrop<TimerRuntime>,
    provider: Box<ExecutorTaskMemProvider>,
    initialized: bool,
}

impl Executor
{
    /// Constructs an executor with default configuration.
    ///
    /// # Example
    /// ```no_run
    /// use flowio::runtime::executor::Executor;
    ///
    /// let executor = Executor::new()?;
    /// # let _ = executor;
    /// # Ok::<(), std::io::Error>(())
    /// ```
    pub fn new() -> io::Result<Self>
    {
        Self::new_with_config(ExecutorConfig::default())
    }

    /// Constructs an executor with explicit reactor and scheduling settings.
    ///
    /// # Example
    /// ```no_run
    /// use flowio::runtime::executor::{Executor, ExecutorConfig};
    ///
    /// let executor = Executor::new_with_config(ExecutorConfig::default())?;
    /// # let _ = executor;
    /// # Ok::<(), std::io::Error>(())
    /// ```
    pub fn new_with_config(config: ExecutorConfig) -> io::Result<Self>
    {
        let mut provider = Box::new(ExecutorTaskMemProvider::new());
        let provider_ptr = &mut *provider as *mut ExecutorTaskMemProvider;

        let task_pool = ManuallyDrop::new(
            Pool::new_uninit(unsafe { &mut *provider_ptr }, TASKS_PER_SLAB)
                .map_err(|_| io::Error::from(ErrorKind::InvalidInput))?,
        );
        let ready_queue = ManuallyDrop::new(DList::new_uninit());

        Ok(Self {
            reactor: Reactor::new_with_config(config.reactor)?,
            process_quota: if config.process_quota == 0
            {
                DEFAULT_PROCESS_QUOTA
            }
            else
            {
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

    fn init(&mut self) -> io::Result<()>
    {
        if self.initialized
        {
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
    /// This must be called from within [`Executor::run`].
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
        EXECUTOR_CTX.with(|ctx_cell| {
            let ctx_ptr = ctx_cell.get();
            if ctx_ptr.is_null()
            {
                return Err(io::Error::from(ErrorKind::InvalidInput));
            }
            let ctx = unsafe { &*ctx_ptr };

            if size_of::<JoinTask<F>>() > TASK_POOL_SIZE
            {
                return Err(io::Error::from(ErrorKind::InvalidInput));
            }

            unsafe {
                let slot_ptr = match (*ctx.task_pool).alloc(())
                {
                    Some(ptr) => ptr,
                    None =>
                    {
                        return Err(io::Error::from(ErrorKind::OutOfMemory));
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
    pub fn run<F: Future<Output = ()> + 'static>(&mut self, initial_task: F) -> io::Result<()>
    {
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
        EXECUTOR_CTX.with(|ctx_cell| ctx_cell.set(ctx_ptr));

        match Self::spawn(initial_task)
        {
            Ok(_handle) =>
            { /* drop JoinHandle — root task is detached */ }
            Err(err) =>
            {
                EXECUTOR_CTX.with(|ctx_cell| ctx_cell.set(std::ptr::null_mut()));
                return Err(err);
            }
        }

        loop
        {
            self.timers.begin_executor_pass();
            let mut polled = 0usize;
            while polled < self.process_quota
            {
                let header_ptr =
                    unsafe { self.ready_queue.pop_front(TaskHeader::READY_LINK_OFFSET) };
                let Some(header_ptr) = header_ptr
                else
                {
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
                if let Poll::Ready(()) = poll_res
                {
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
                }
                else
                {
                    let flags = header.flags.get();
                    // Clear RUNNING. If NOTIFIED was set during poll, re-enqueue.
                    header.flags.set(flags & !TaskHeader::FLAG_RUNNING);
                    if (flags & TaskHeader::FLAG_NOTIFIED) != 0
                    {
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

            self.reactor.flush_sqes()?;
            let completed = self.reactor.poll_io(
                self.process_quota,
                &mut runtime_state as *mut RuntimeState,
                &mut *self.ready_queue as *mut DList<TaskHeader>,
            )?;
            let timers_pending = self.timers.has_pending();
            let mut now_tick = None;
            let timer_budget_exhausted = if timers_pending
            {
                let tick = self.timers.now_tick()?;
                now_tick = Some(tick);
                self.timers
                    .process_at_with_budget(tick, self.process_quota)?
            }
            else
            {
                false
            };
            let queue_empty = self.ready_queue.is_empty();
            let drained = runtime_state.live_tasks == 0
                && runtime_state.inflight_ops == 0
                && !timers_pending
                && queue_empty;

            if drained
            {
                #[cfg(debug_assertions)]
                {
                    runtime_state.stats.task_slab_allocs = self.provider.request_count;
                    runtime_state.stats.task_slab_frees = self.provider.free_count;
                    self.last_stats = runtime_state.stats;
                }
                EXECUTOR_CTX.with(|ctx_cell| ctx_cell.set(std::ptr::null_mut()));
                return Ok(());
            }

            if completed > 0 || !queue_empty || timer_budget_exhausted
            {
                continue;
            }

            let timer_wait = match now_tick
            {
                Some(tick) => self.timers.next_wait_duration(tick),
                None => None,
            };

            if runtime_state.inflight_ops == 0 && timer_wait.is_none()
            {
                #[cfg(debug_assertions)]
                {
                    runtime_state.stats.task_slab_allocs = self.provider.request_count;
                    runtime_state.stats.task_slab_frees = self.provider.free_count;
                    self.last_stats = runtime_state.stats;
                }
                EXECUTOR_CTX.with(|ctx_cell| ctx_cell.set(std::ptr::null_mut()));
                return Err(io::Error::from(ErrorKind::WouldBlock));
            }

            if matches!(timer_wait, Some(duration) if duration.is_zero())
            {
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

            self.reactor.wait_for_events(timer_wait)?;
            let _ = self.reactor.poll_io(
                self.process_quota,
                &mut runtime_state as *mut RuntimeState,
                &mut *self.ready_queue as *mut DList<TaskHeader>,
            )?;
            if self.timers.has_pending()
            {
                let now_tick = self.timers.now_tick()?;
                let _ = self
                    .timers
                    .process_at_with_budget(now_tick, self.process_quota)?;
            }
        }
    }

    /// Returns scheduler counters captured for the most recently completed run.
    #[cfg(debug_assertions)]
    pub fn last_stats(&self) -> RuntimeStats
    {
        self.last_stats
    }
}

#[cfg(target_os = "linux")]
fn apply_cpu_affinity(cpu_affinity: Option<usize>) -> io::Result<()>
{
    let Some(cpu) = cpu_affinity
    else
    {
        return Ok(());
    };

    let mut set = unsafe { std::mem::zeroed::<libc::cpu_set_t>() };
    unsafe {
        libc::CPU_ZERO(&mut set);
        libc::CPU_SET(cpu, &mut set);
    }

    let rc = unsafe { libc::sched_setaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &set) };
    if rc < 0
    {
        return Err(io::Error::last_os_error());
    }

    Ok(())
}

#[cfg(not(target_os = "linux"))]
fn apply_cpu_affinity(cpu_affinity: Option<usize>) -> io::Result<()>
{
    if cpu_affinity.is_some()
    {
        return Err(io::Error::from(ErrorKind::Unsupported));
    }

    Ok(())
}

impl Drop for Executor
{
    fn drop(&mut self)
    {
        if self.initialized
        {
            unsafe {
                ManuallyDrop::drop(&mut self.ready_queue);
                ManuallyDrop::drop(&mut self.task_pool);
                ManuallyDrop::drop(&mut self.timers);
            }
        }
    }
}

/// Cancel an in-flight operation from a future's `Drop` impl.
/// Marks the `CompletionState` as orphaned and submits `ASYNC_CANCEL`.
/// Uses one TLS read and only runs on the cancellation path.
unsafe fn cancel_op_unchecked(ptr: *mut crate::runtime::op::CompletionState)
{
    EXECUTOR_CTX.with(|ctx_cell| {
        let ctx_ptr = ctx_cell.get();
        if ctx_ptr.is_null()
        {
            return;
        }
        let ctx = unsafe { &*ctx_ptr };
        unsafe { (*ctx.reactor).cancel_op(ptr) };
    });
}

/// Free a completed `CompletionState` from a future's `Drop` impl when the
/// CQE has already been consumed but the future is dropped before polling the
/// result. Uses one TLS read and only runs on that drop-after-complete path.
unsafe fn free_op_unchecked(ptr: *mut crate::runtime::op::CompletionState)
{
    EXECUTOR_CTX.with(|ctx_cell| {
        let ctx_ptr = ctx_cell.get();
        if ctx_ptr.is_null()
        {
            return;
        }
        let ctx = unsafe { &*ctx_ptr };
        unsafe { (*ctx.reactor).free_op(ptr) };
    });
}

/// Release a future-owned `CompletionState` pointer from `Drop`.
/// Completed ops are freed immediately; pending ops are orphaned and cancelled.
/// The caller's pointer is always cleared.
#[inline(always)]
#[doc(hidden)]
pub(crate) unsafe fn drop_op_ptr_unchecked(ptr: &mut *mut crate::runtime::op::CompletionState)
{
    let state_ptr = *ptr;
    if state_ptr.is_null()
    {
        return;
    }

    unsafe {
        if (*state_ptr).is_completed()
        {
            free_op_unchecked(state_ptr);
        }
        else
        {
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
) -> io::Result<()>
{
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

#[inline(always)]
#[doc(hidden)]
pub(crate) fn submit_detached_close(pctx: &PollCtx, fd: RawFd) -> io::Result<()>
{
    let reactor = pctx.reactor();
    let state_ptr = unsafe { (*reactor).alloc_op() };
    if state_ptr.is_null()
    {
        return Err(io::Error::from(io::ErrorKind::WouldBlock));
    }

    unsafe { (*state_ptr).set_detached() };

    let sqe = opcode::Close::new(types::Fd(fd))
        .build()
        .user_data(state_ptr as u64);

    unsafe {
        if let Err(err) = submit_tracked_sqe(pctx, sqe)
        {
            (*reactor).free_op(state_ptr);
            return Err(err);
        }
    }

    Ok(())
}

#[inline(always)]
#[doc(hidden)]
pub(crate) fn try_submit_detached_close(fd: RawFd) -> bool
{
    EXECUTOR_CTX.with(|ctx_cell| {
        let ctx_ptr = ctx_cell.get();
        if ctx_ptr.is_null()
        {
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
pub fn note_waiter_wake()
{
    #[cfg(debug_assertions)]
    EXECUTOR_CTX.with(|ctx_cell| {
        let ctx_ptr = ctx_cell.get();
        if ctx_ptr.is_null()
        {
            return;
        }
        unsafe {
            (*(*ctx_ptr).runtime_state).stats.waiter_wakes += 1;
        }
    });
}

#[inline(always)]
#[doc(hidden)]
pub fn note_timer_now_tick_call()
{
    #[cfg(debug_assertions)]
    EXECUTOR_CTX.with(|ctx_cell| {
        let ctx_ptr = ctx_cell.get();
        if ctx_ptr.is_null()
        {
            return;
        }
        unsafe {
            (*(*ctx_ptr).runtime_state).stats.timer_now_tick_calls += 1;
        }
    });
}

#[inline(always)]
#[doc(hidden)]
pub fn note_timer_expired()
{
    #[cfg(debug_assertions)]
    EXECUTOR_CTX.with(|ctx_cell| {
        let ctx_ptr = ctx_cell.get();
        if ctx_ptr.is_null()
        {
            return;
        }
        unsafe {
            (*(*ctx_ptr).runtime_state).stats.timer_expired += 1;
        }
    });
}

#[inline(always)]
#[doc(hidden)]
pub(crate) unsafe fn current_poll_owner_task_unchecked() -> *mut TaskHeader
{
    EXECUTOR_CTX.with(|ctx_cell| {
        let ctx_ptr = ctx_cell.get();
        debug_assert!(
            !ctx_ptr.is_null(),
            "runtime poll owner requested outside task poll context"
        );
        unsafe { (*ctx_ptr).owner_task }
    })
}

#[doc(hidden)]
pub unsafe fn timers_unchecked() -> *mut TimerRuntime
{
    EXECUTOR_CTX.with(|ctx_cell| {
        let ctx_ptr = ctx_cell.get();
        debug_assert!(
            !ctx_ptr.is_null(),
            "runtime timers_unchecked requested outside executor context"
        );
        unsafe { (*ctx_ptr).timers }
    })
}

#[inline(always)]
#[doc(hidden)]
pub unsafe fn schedule_ctx_unchecked() -> ScheduleCtx
{
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
)
{
    let header = unsafe { &mut *task_ptr };
    if header.last_wake_epoch.get() == wake_epoch
    {
        return;
    }
    header.last_wake_epoch.set(wake_epoch);
    unsafe {
        notify_task_into_list_unchecked(task_ptr, ready_list, runtime_state);
    }
}

#[inline(always)]
#[doc(hidden)]
pub unsafe fn next_timer_wake_epoch_unchecked(schedule_ctx: ScheduleCtx) -> u64
{
    let current = unsafe { (*schedule_ctx.runtime_state).wake_epoch };
    let mut next = current.wrapping_add(1);
    if next == 0
    {
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
                match fut_pin.as_mut().poll(&mut cx)
                {
                    Poll::Ready(value) =>
                    {
                        jt.future = None;
                        jt.result = Some(value);
                        if let Some(join_waker) = jt.join_waker.take()
                        {
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
                    if ctx_ptr.is_null()
                    {
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

unsafe fn schedule_task(task_ptr: *mut TaskHeader)
{
    EXECUTOR_CTX.with(|ctx_cell| {
        let ctx_ptr = ctx_cell.get();
        if ctx_ptr.is_null()
        {
            return;
        }
        let ctx = unsafe { &*ctx_ptr };

        unsafe { notify_task_into_list_unchecked(task_ptr, ctx.ready_queue, ctx.runtime_state) };
    });
}

#[inline(always)]
pub(crate) unsafe fn notify_task_into_list_unchecked(
    task_ptr: *mut TaskHeader,
    ready_list: *mut DList<TaskHeader>,
    runtime_state: *mut RuntimeState,
) -> bool
{
    let header = unsafe { &mut *task_ptr };
    let flags = header.flags();
    if (flags & TaskHeader::FLAG_COMPLETED) != 0
    {
        return false;
    }

    if (flags & TaskHeader::FLAG_NOTIFIED) != 0
    {
        return false;
    }

    header.set_flag(TaskHeader::FLAG_NOTIFIED);

    if (flags & (TaskHeader::FLAG_RUNNING | TaskHeader::FLAG_QUEUED)) == 0
    {
        return unsafe { enqueue_ready_task_unchecked(task_ptr, ready_list, runtime_state) };
    }

    false
}

#[inline(always)]
unsafe fn enqueue_notified_task_unchecked(
    task_ptr: *mut TaskHeader,
    ready_list: *mut DList<TaskHeader>,
    runtime_state: *mut RuntimeState,
) -> bool
{
    let header = unsafe { &mut *task_ptr };
    let flags = header.flags();
    if (flags & TaskHeader::FLAG_COMPLETED) != 0
    {
        return false;
    }
    if (flags & TaskHeader::FLAG_NOTIFIED) == 0
    {
        return false;
    }
    if (flags & (TaskHeader::FLAG_RUNNING | TaskHeader::FLAG_QUEUED)) != 0
    {
        return false;
    }

    unsafe { enqueue_ready_task_unchecked(task_ptr, ready_list, runtime_state) }
}

#[inline(always)]
unsafe fn enqueue_ready_task_unchecked(
    task_ptr: *mut TaskHeader,
    ready_list: *mut DList<TaskHeader>,
    _runtime_state: *mut RuntimeState,
) -> bool
{
    let header = unsafe { &mut *task_ptr };
    debug_assert!(
        header.ready_link.is_unlinked(),
        "enqueue_ready_task attempted to enqueue an already-linked task"
    );
    header.set_flag(TaskHeader::FLAG_QUEUED);
    #[cfg(debug_assertions)]
    {
        if !_runtime_state.is_null()
        {
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
pub unsafe fn schedule_woken_task(task_ptr: *mut TaskHeader)
{
    unsafe {
        schedule_task(task_ptr);
    }
}
