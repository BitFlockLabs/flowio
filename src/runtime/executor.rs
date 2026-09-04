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

use crate::runtime::fd::RuntimeFdOpState;
use crate::runtime::op::CompletionState;
use crate::runtime::reactor::{Reactor, ReactorConfig, ReactorSubmitStatus};
use crate::runtime::retained::RetainedPayload;
#[cfg(debug_assertions)]
use crate::runtime::retained::RetainedPayloadPoolStats;
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
/// payload boundary, so adding them does not change the 64-bit task slot size.
#[cfg(target_pointer_width = "64")]
const _: () = {
    assert!(TASK_DATA_ALIGN == 64);
    assert!(std::mem::offset_of!(Task<TASK_POOL_SIZE>, data) == 128);
    assert!(size_of::<Task<TASK_POOL_SIZE>>() == 4224);
    assert!(align_of::<Task<TASK_POOL_SIZE>>() == 64);
};

#[cfg(any(test, feature = "test-support", debug_assertions))]
macro_rules! define_runtime_stats {
    ($vis:vis) => {
        /// Development-only counters for scheduler and allocation regression
        /// tests and benchmark probes.
        ///
        /// The type is exposed outside the crate only by `test-support`, and
        /// its counters exist only when debug assertions are enabled. It is not
        /// a supported production observability API.
        ///
        /// Counter scopes intentionally differ. Scheduler, task/provider,
        /// poll-context, SQE/CQE, timer, accept, close, and partial-write
        /// counters accumulate across one uninterrupted execution generation.
        /// A generation begins when [`Executor::run`] enters with no live task,
        /// in-flight operation, ready task, or pending timer; a stalled
        /// `WouldBlock` return followed by a resumed run stays in the same
        /// generation. Only the `retained_*` and `writev_scratch_*` fields are
        /// saturating deltas from the latest `run()` entry. In particular,
        /// `writev_partial_continuations` is generation-cumulative despite its
        /// prefix.
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
            /// Owner-thread accept attempts that returned `EMFILE` or `ENFILE`
            /// while preserving observed readiness for the caller's retry.
            #[cfg(debug_assertions)]
            pub accept_descriptor_exhaustions: usize,
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
            /// Stream requests rejected for exceeding the active iovec limit.
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

#[cfg(all(not(debug_assertions), any(test, feature = "test-support")))]
const _: [(); 0] = [(); size_of::<RuntimeStats>()];

/// Opt-in executor-local counters for bounded diagnostic runs.
///
/// These counters are available only with the dev-only
/// `diagnostic-counters` feature and are not a supported production metrics
/// API. They use plain owner-thread-local integers: no atomics, locks, queues,
/// allocation, or background work is introduced. Use the test-support facade
/// to snapshot and reset them only after [`Executor::run`] returns, so the
/// observation work stays outside the timed benchmark interval.
#[cfg(feature = "diagnostic-counters")]
#[doc(hidden)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RuntimeDiagnosticCounters {
    /// Calls that reached an `io_uring_enter`-issuing ring method.
    pub ring_enter_attempts: usize,
    /// Ring-enter calls that returned an `Ok` result.
    pub ring_enter_successes: usize,
    /// SQEs reported submitted by successful ring-enter calls.
    pub ring_enter_submitted_sqes: usize,
    /// Ring-enter calls that returned `EINTR`.
    pub ring_enter_eintr: usize,
    /// Ring-enter calls that returned `EBUSY`.
    pub ring_enter_ebusy: usize,
    /// Timed ring-enter calls that returned `ETIME`.
    pub ring_enter_etime: usize,
    /// Ring-enter calls that returned any other error.
    pub ring_enter_other_errors: usize,
    /// SQEs successfully appended to this executor's userspace submission
    /// queue, including target, cancel, and close entries.
    pub sqes_queued: usize,
    /// Retained payload allocations by the size-class index described by
    /// [`Self::RETAINED_PAYLOAD_CLASS_BYTES`].
    pub retained_payload_class_allocs: [usize; 11],
    /// Retained payload class allocations served from returned blocks.
    pub retained_payload_reuses: usize,
    /// Retained payload class allocations that requested a new slab page.
    pub retained_payload_slab_allocs: usize,
    /// Retained payload allocations that used the heap fallback.
    pub retained_heap_fallbacks: usize,
    /// Retained vectored-I/O scratch requests served from inline storage.
    pub writev_scratch_inline_allocs: usize,
    /// Retained vectored-I/O scratch allocations by the class index described
    /// by [`Self::WRITEV_SCRATCH_CLASS_IOVECS`].
    pub writev_scratch_class_allocs: [usize; 4],
    /// Retained vectored-I/O scratch class allocations served from returned
    /// blocks.
    pub writev_scratch_reuses: usize,
    /// Retained vectored-I/O scratch class allocations that requested a new
    /// slab page.
    pub writev_scratch_slab_allocs: usize,
    /// Partial vectored writes that advanced retained iovec metadata before
    /// submitting the remaining write window.
    pub writev_partial_continuations: usize,
}

#[cfg(feature = "diagnostic-counters")]
impl RuntimeDiagnosticCounters {
    /// Byte capacity represented by each retained-payload counter index.
    pub const RETAINED_PAYLOAD_CLASS_BYTES: [usize; 11] =
        crate::runtime::retained::RETAINED_SIZE_CLASSES;

    /// Iovec capacity represented by each retained-scratch counter index.
    pub const WRITEV_SCRATCH_CLASS_IOVECS: [usize; 4] =
        crate::runtime::retained::RETAINED_IOVEC_SIZE_CLASSES;
}

/// Test-support-only ownership snapshot taken between executor runs.
///
/// This type exists only for crate tests and the dev-only `test-support`
/// feature. It is absent from ordinary production builds and is not a
/// supported observability API.
#[cfg(any(test, feature = "test-support"))]
#[doc(hidden)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RuntimeQuiescence {
    /// Unfinished executor-owned tasks.
    pub live_tasks: usize,
    /// Submitted operations whose target CQE has not retired.
    pub inflight_ops: usize,
    /// Whether the runnable-task queue is empty.
    pub ready_queue_empty: bool,
    /// Whether every allocated task slot has released its references.
    pub task_registry_empty: bool,
    /// Whether any timer entry remains armed.
    pub timers_pending: bool,
    /// Completion states still checked out from the operation pool.
    pub live_ops: usize,
    /// Orphaned operations waiting for another cancel submission attempt.
    pub pending_cancels: usize,
    /// Userspace SQEs not yet consumed by the kernel.
    pub queued_sqes: u64,
    /// Descriptor owners retained for queued reactor close SQEs.
    pub pending_reactor_closes: usize,
    /// Descriptor owners deferred until a completion view is released.
    pub deferred_reactor_closes: usize,
    /// Strong references to the executor's heap-stable owner.
    pub executor_owner_refs: usize,
    /// Strong references to the retained-iovec sidecar owner.
    pub scratch_owner_refs: usize,
    /// Slab pages retained by the task pool.
    pub task_slab_pages: usize,
    /// Slab pages retained by the completion-state pool.
    pub operation_slab_pages: usize,
    /// Slab pages retained by the timer pool.
    pub timer_slab_pages: usize,
    /// Slab pages retained by payload size classes.
    pub retained_slab_pages: usize,
    /// Slab pages retained by vectored-I/O scratch size classes.
    pub scratch_slab_pages: usize,
    /// Retained payload allocations served by slab classes.
    pub retained_pooled_allocs: usize,
    /// Retained payload blocks returned to slab classes.
    pub retained_pooled_frees: usize,
    /// Retained payload allocations served by the heap fallback.
    pub retained_heap_allocs: usize,
    /// Retained heap-fallback blocks released.
    pub retained_heap_frees: usize,
    /// Vectored-I/O scratch allocations served by sidecar slabs.
    pub scratch_pooled_allocs: usize,
    /// Vectored-I/O scratch blocks returned to sidecar slabs.
    pub scratch_pooled_frees: usize,
    /// Whether reactor shutdown abandoned kernel-visible storage.
    pub storage_abandoned: bool,
}

#[cfg(debug_assertions)]
macro_rules! define_apply_retained_payload_stats {
    ($($field:ident => $runtime_field:ident: $doc:literal;)*) => {
        #[inline(always)]
        fn apply_retained_payload_stats(
            stats: &mut RuntimeStats,
            retained: RetainedPayloadPoolStats,
        ) {
            $(stats.$runtime_field = retained.$field;)*
        }
    };
}

#[cfg(debug_assertions)]
crate::runtime::retained::retained_payload_stat_fields!(define_apply_retained_payload_stats);

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
            self.request_count = self.request_count.saturating_add(1);
        }
    }

    #[inline(always)]
    fn note_free(&mut self) {
        #[cfg(debug_assertions)]
        {
            self.free_count = self.free_count.saturating_add(1);
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

#[cfg(all(
    target_arch = "x86_64",
    target_os = "linux",
    not(debug_assertions),
    not(feature = "test-support"),
    not(feature = "diagnostic-counters")
))]
const _: [(); 16] = [(); size_of::<RuntimeState>()];

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
    /// Intrusive link used while an active iterative task-destruction drain
    /// defers this state's remaining shutdown phases.
    deferred_shutdown_next: *mut ExecutorState,
    /// Temporary self-pin keeping this state alive until deferred teardown has
    /// shut down the runtime and joined the existing close worker.
    deferred_shutdown_owner: Option<Rc<ExecutorOwner>>,
}

#[cfg(all(
    target_arch = "x86_64",
    target_os = "linux",
    not(debug_assertions),
    not(feature = "test-support"),
    not(feature = "diagnostic-counters")
))]
const _: [(); 9040] = [(); size_of::<ExecutorState>()];

/// Cancels timers after task shutdown.
struct TimerShutdownPhase {
    state: *mut ExecutorState,
}

impl Drop for TimerShutdownPhase {
    fn drop(&mut self) {
        let state = self.state;
        run_cleanup_preserving_panic(|| {
            // SAFETY: direct shutdown owns this phase inside shutdown_owner
            // while its ExecutorOwner keeps the heap-stable state alive;
            // deferred iterative drain pins that state in
            // deferred_shutdown_owner through phase drop.
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
            // SAFETY: direct shutdown owns this phase inside shutdown_owner
            // while its ExecutorOwner keeps the heap-stable state alive;
            // deferred iterative drain pins that state in
            // deferred_shutdown_owner through phase drop.
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
pub(super) fn run_cleanup_preserving_panic(cleanup: impl FnOnce()) {
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
            deferred_shutdown_next: std::ptr::null_mut(),
            deferred_shutdown_owner: None,
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

/// Runs one benchmark closure with an initialized executor context and raw
/// access to its heap-stable owner-thread fields.
#[cfg(feature = "test-support")]
pub(crate) fn with_executor_context_for_benchmark<R>(
    ring_entries: u32,
    benchmark: impl FnOnce(
        *mut Reactor,
        *mut RuntimeState,
        *mut DList<TaskHeader>,
        *mut DList<TaskHeader>,
    ) -> io::Result<R>,
) -> io::Result<R> {
    let mut executor = Executor::new_with_config(ExecutorConfig {
        reactor: ReactorConfig { ring_entries },
        ..ExecutorConfig::default()
    })?;
    executor.init()?;

    let owner = Rc::as_ptr(&executor.owner);
    let state = executor.owner.state_ptr();
    {
        let _context = ExecutorCtxGuard::install(owner)?;
        benchmark(
            unsafe { std::ptr::addr_of_mut!((*state).reactor) },
            unsafe { std::ptr::addr_of_mut!((*state).runtime_state) },
            unsafe { std::ptr::addr_of_mut!((*state).ready_queue) },
            unsafe { std::ptr::addr_of_mut!((*state).all_tasks) },
        )
    }
}

#[derive(Clone, Copy)]
struct ExecutorThreadContext {
    active_owner: *const ExecutorOwner,
    completion_drain_active: bool,
    completion_drain_reactor: *mut Reactor,
}

impl ExecutorThreadContext {
    const INACTIVE: Self = Self {
        active_owner: std::ptr::null(),
        completion_drain_active: false,
        completion_drain_reactor: std::ptr::null_mut(),
    };
}

thread_local! {
    static EXECUTOR_CTX: Cell<ExecutorThreadContext> =
        const { Cell::new(ExecutorThreadContext::INACTIVE) };
}

/// Marks the thread while any reactor completion view has its ring mutably
/// borrowed.
///
/// Descriptor destruction can run from retained-payload or task destructors
/// during completion reclamation. The nestable flag keeps ordinary ring work
/// excluded while any completion view is live. A production guard also
/// publishes the exact current reactor so known-nonblocking descriptor owners
/// can move into its bounded FIFO; that reactor submits them only after its own
/// view is released, even when an outer view for another reactor remains live.
pub(crate) struct CompletionDrainGuard {
    previous_active: bool,
    previous_reactor: *mut Reactor,
}

impl CompletionDrainGuard {
    #[cfg(any(test, all(feature = "test-support", not(miri))))]
    #[inline(always)]
    pub(crate) fn enter() -> Self {
        EXECUTOR_CTX.with(|context| {
            let previous = context.get();
            context.set(ExecutorThreadContext {
                completion_drain_active: true,
                completion_drain_reactor: if previous.completion_drain_active {
                    previous.completion_drain_reactor
                } else {
                    std::ptr::null_mut()
                },
                ..previous
            });
            Self {
                previous_active: previous.completion_drain_active,
                previous_reactor: previous.completion_drain_reactor,
            }
        })
    }

    /// Marks a production completion drain and publishes its exact reactor for
    /// bounded descriptor deferral.
    #[inline(always)]
    pub(crate) fn enter_for_reactor(reactor: *mut Reactor) -> Self {
        debug_assert!(
            !reactor.is_null(),
            "completion-drain reactor must be non-null"
        );
        EXECUTOR_CTX.with(|context| {
            let previous = context.get();
            context.set(ExecutorThreadContext {
                completion_drain_active: true,
                completion_drain_reactor: reactor,
                ..previous
            });
            Self {
                previous_active: previous.completion_drain_active,
                previous_reactor: previous.completion_drain_reactor,
            }
        })
    }
}

impl Drop for CompletionDrainGuard {
    #[inline(always)]
    fn drop(&mut self) {
        EXECUTOR_CTX.with(|context| {
            let current = context.get();
            context.set(ExecutorThreadContext {
                completion_drain_active: self.previous_active,
                completion_drain_reactor: self.previous_reactor,
                ..current
            });
        });
    }
}

#[inline(always)]
pub(crate) fn completion_drain_active() -> bool {
    EXECUTOR_CTX.with(|context| context.get().completion_drain_active)
}

struct ExecutorCtxGuard {
    owner: *const ExecutorOwner,
    previous_owner: *const ExecutorOwner,
}

impl ExecutorCtxGuard {
    #[inline(always)]
    fn reject_if_active() -> io::Result<()> {
        EXECUTOR_CTX.with(|ctx_cell| {
            if ctx_cell.get().active_owner.is_null() {
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
        EXECUTOR_CTX.with(|context| {
            let previous = context.get();
            context.set(ExecutorThreadContext {
                active_owner: owner,
                ..previous
            });
            Self {
                owner,
                previous_owner: previous.active_owner,
            }
        })
    }
}

impl Drop for ExecutorCtxGuard {
    #[inline(always)]
    fn drop(&mut self) {
        EXECUTOR_CTX.with(|ctx_cell| {
            let current = ctx_cell.get();
            if current.active_owner == self.owner {
                ctx_cell.set(ExecutorThreadContext {
                    active_owner: self.previous_owner,
                    ..current
                });
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

#[derive(Clone, Copy)]
enum PollCtxRejection {
    Permanent,
    CompletionDrain(PollCtx),
}

/// Validates and extracts the FlowIO task and active executor represented by
/// one future poll.
#[inline(always)]
fn classify_poll_ctx_from_waker(cx: &std::task::Context<'_>) -> Result<PollCtx, PollCtxRejection> {
    let task = task_ptr_from_waker(cx.waker()).ok_or(PollCtxRejection::Permanent)?;
    let owner = unsafe { (*task).owner.as_ref().map_or(std::ptr::null(), Rc::as_ptr) };
    let thread_context = EXECUTOR_CTX.with(Cell::get);
    let shutting_down = !owner.is_null() && unsafe { (*(*owner).state_ptr()).shutting_down };
    if owner.is_null() || thread_context.active_owner != owner || shutting_down {
        return Err(PollCtxRejection::Permanent);
    }
    let pctx = PollCtx { owner, task };
    if thread_context.completion_drain_active {
        return Err(PollCtxRejection::CompletionDrain(pctx));
    }

    #[cfg(debug_assertions)]
    unsafe {
        let stats = &mut (*pctx.runtime_state()).stats;
        stats.poll_context_extractions = stats.poll_context_extractions.saturating_add(1);
    }
    Ok(pctx)
}

/// Validates and extracts the FlowIO task and active executor represented by
/// one future poll.
#[inline(always)]
pub(crate) fn poll_ctx_from_waker(cx: &std::task::Context<'_>) -> io::Result<PollCtx> {
    classify_poll_ctx_from_waker(cx).map_err(|_| inactive_poll_context_error())
}

/// Validates a leading operation-poll boundary while allowing an existing
/// matching-owner operation to remain pending during a completion drain.
///
/// `Ok(None)` means the caller must return `Poll::Pending` without changing
/// the operation pointer, flags, waiter, or payload. Fresh, completed,
/// ring-abandoned, and foreign-owner operations retain ordinary context
/// rejection.
///
/// # Safety
///
/// A non-null `state_ptr` must identify the live completion state exclusively
/// owned by the currently polled future.
#[cfg(any(test, feature = "test-support"))]
#[inline(always)]
pub(crate) unsafe fn poll_ctx_or_transient_pending_op(
    cx: &std::task::Context<'_>,
    state_ptr: *mut CompletionState,
) -> io::Result<Option<PollCtx>> {
    match classify_poll_ctx_from_waker(cx) {
        Ok(pctx) => Ok(Some(pctx)),
        Err(PollCtxRejection::CompletionDrain(pctx))
            if !state_ptr.is_null()
                && unsafe { !(*state_ptr).is_completed() }
                && unsafe { !(*state_ptr).is_ring_abandoned() }
                && unsafe { (*state_ptr).owner_ptr() == pctx.owner_ptr() } =>
        {
            Ok(None)
        }
        Err(_) => Err(inactive_poll_context_error()),
    }
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

/// Records one preflighted stream iovec-capacity rejection after its owner
/// poll context and all higher-precedence validation have succeeded.
#[cfg(debug_assertions)]
#[inline(always)]
pub(crate) fn record_retained_iovec_oversize_rejection(pctx: &PollCtx) {
    let pool = unsafe { Reactor::retained_payload_pool_ptr(pctx.reactor()) };
    unsafe { (*pool.as_ptr()).record_iovec_oversize_rejection() };
}

/// Validates a locally rejected stream request before recording its intrinsic
/// iovec-capacity event.
#[cfg(debug_assertions)]
#[inline(always)]
pub(crate) fn validate_local_iovec_oversize_rejection<T>(
    cx: &std::task::Context<'_>,
    result: io::Result<T>,
) -> io::Result<T> {
    let pctx = poll_ctx_from_waker(cx)?;
    record_retained_iovec_oversize_rejection(&pctx);
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
    state.debug_assert_valid_flags();
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
/// CQE. A transient re-poll while a completion view owns the ring leaves both
/// the registered waiter and rejection flag unchanged. Returns `true` when
/// reactor teardown abandoned the operation without observing its target CQE;
/// callers may report that terminal condition only when doing so cannot expose
/// a retained kernel-visible caller payload.
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

    debug_assert!(
        unsafe { !(*state_ptr).is_completed() },
        "cannot refresh a completed operation"
    );
    unsafe {
        (*state_ptr).debug_assert_valid_flags();
    }
    if unsafe { (*state_ptr).is_ring_abandoned() } {
        return true;
    }
    match classify_poll_ctx_from_waker(cx) {
        Ok(pctx) => {
            if unsafe { (*state_ptr).owner_ptr() } != pctx.owner_ptr() {
                unsafe {
                    (*state_ptr).set_context_rejected();
                }
            }
            unsafe {
                CompletionState::replace_waiter_unchecked(state_ptr, pctx.owner_task());
            }
        }
        Err(PollCtxRejection::Permanent) => unsafe {
            (*state_ptr).set_context_rejected();
        },
        Err(PollCtxRejection::CompletionDrain(pctx)) => {
            if unsafe { (*state_ptr).owner_ptr() } != pctx.owner_ptr() {
                unsafe {
                    (*state_ptr).set_context_rejected();
                }
            }
        }
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

#[derive(Clone, Copy)]
struct JoinTaskVTables {
    direct: &'static TaskVTable,
    iterative: &'static TaskVTable,
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

/// Initializes the shared header of one freshly allocated task slot.
///
/// # Safety
///
/// `task` must point to stable, writable `TaskHeader` storage in a checked-out
/// task slot whose prior task lifetime, if any, has been fully destroyed.
/// `owner` must be a live executor owner on the current owner thread. `refs`
/// and `flags` must describe every initial owner and scheduler state, and
/// `vtable` must match the payload already initialized in the slot. This
/// function must be called exactly once for the new task lifetime before the
/// task is published, read, or released.
#[inline(always)]
unsafe fn init_task_slot_header(
    task: *mut TaskHeader,
    owner: *const ExecutorOwner,
    refs: usize,
    flags: u64,
    vtable: &'static TaskVTable,
    iterative_vtable: &'static TaskVTable,
) {
    unsafe {
        (*task).ready_link = crate::utils::list::intrusive::dlist::Link::new_unlinked();
        (*task).all_link = crate::utils::list::intrusive::dlist::Link::new_unlinked();
        (*task).owner = Some(ExecutorOwner::clone_rc(owner));
        (*task).refs.set(refs);
        (*task).flags.set(flags);
        init_cached_waker(task);
        (*task).iterative_vtable = iterative_vtable;
        (*task).vtable = vtable;
    }
}

/// Owns the setup reference for one already-completed benchmark task.
#[cfg(any(test, feature = "test-support"))]
pub(crate) struct StagedCompletedTaskOutput<T: 'static> {
    task: *mut TaskHeader,
    output: *mut T,
    owns_reference: bool,
}

#[cfg(any(test, feature = "test-support"))]
impl<T: 'static> StagedCompletedTaskOutput<T> {
    /// Returns the stable output address inside the task's completed result.
    pub(crate) fn output_ptr(&self) -> *mut T {
        self.output
    }

    /// Transfers the staging reference to one completion-state waiter.
    /// The returned output pointer remains valid only until that waiter
    /// releases the task's final reference.
    ///
    /// # Safety
    ///
    /// `state` must be a live unsubmitted state owned by the active executor,
    /// and it must not already own a waiter.
    pub(crate) unsafe fn transfer_to_waiter(mut self, state: *mut CompletionState) -> *mut T {
        debug_assert!(!state.is_null(), "benchmark waiter state is null");
        unsafe {
            (*state).register_waiter(self.task);
        }
        self.owns_reference = false;
        unsafe {
            release_task(self.task);
        }
        self.output
    }
}

#[cfg(any(test, feature = "test-support"))]
impl<T: 'static> Drop for StagedCompletedTaskOutput<T> {
    fn drop(&mut self) {
        if self.owns_reference {
            unsafe {
                release_task(self.task);
            }
            self.owns_reference = false;
        }
    }
}

/// Stages one completed detached join result in the real executor task pool.
///
/// The returned setup owner can transfer its sole task reference into a
/// completion-state waiter. Final waiter release then runs the ordinary
/// `JoinTask` result destructor, all-task unlink, and pooled slot return.
#[cfg(any(test, feature = "test-support"))]
pub(crate) fn stage_completed_task_output_for_benchmark<T: 'static>(
    output: T,
) -> io::Result<StagedCompletedTaskOutput<T>> {
    type CompletedFuture<T> = std::future::Ready<T>;

    EXECUTOR_CTX.with(|context| {
        let owner = context.get().active_owner;
        if owner.is_null() {
            return Err(io::Error::from(ErrorKind::InvalidInput));
        }
        if size_of::<JoinTask<CompletedFuture<T>>>() > TASK_POOL_SIZE
            || align_of::<JoinTask<CompletedFuture<T>>>() > TASK_DATA_ALIGN
        {
            return Err(io::Error::from(ErrorKind::InvalidInput));
        }

        let state = unsafe { (*owner).state_ptr() };
        if unsafe { (*state).shutting_down } {
            return Err(io::Error::from(ErrorKind::InvalidInput));
        }
        let slot = unsafe { (*state).task_pool.alloc(()) }
            .ok_or_else(|| io::Error::from(ErrorKind::OutOfMemory))?;
        let join = unsafe { (&mut *slot).data.as_mut_ptr() as *mut JoinTask<CompletedFuture<T>> };
        unsafe {
            std::ptr::addr_of_mut!((*join).future).write(None);
            std::ptr::addr_of_mut!((*join).result).write(Some(Ok(output)));
            std::ptr::addr_of_mut!((*join).join_waker).write(None);
        }
        let output = match unsafe { (&mut *std::ptr::addr_of_mut!((*join).result)).as_mut() } {
            Some(Ok(output)) => std::ptr::from_mut(output),
            _ => unreachable!("completed benchmark output was not published"),
        };

        let vtables = join_task_vtable_for::<CompletedFuture<T>>();

        unsafe {
            init_task_slot_header(
                std::ptr::addr_of_mut!((*slot).header),
                owner,
                1,
                TaskHeader::FLAG_COMPLETED,
                vtables.iterative,
                vtables.iterative,
            );
            #[cfg(debug_assertions)]
            {
                let stats = &mut (*state).runtime_state.stats;
                stats.task_allocs = stats.task_allocs.saturating_add(1);
            }
            (*state)
                .all_tasks
                .push_back_unchecked(std::ptr::addr_of_mut!((*slot).header.all_link));
        }

        Ok(StagedCompletedTaskOutput {
            task: unsafe { std::ptr::addr_of_mut!((*slot).header) },
            output,
            owns_reference: true,
        })
    })
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
            let stats = &mut (*state).runtime_state.stats;
            stats.task_frees = stats.task_frees.saturating_add(1);
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

/// Stack-resident owner-thread FIFO for zero-reference tasks whose join
/// payload destruction can release another task's final reference.
///
/// Items are completed, zero-reference [`TaskHeader`] allocations. Nested raw
/// destructors on the owner thread produce entries; the outermost iterative
/// destructor consumes them in FIFO order. The queue has no independent
/// allocation, configured capacity, or full condition: it can contain at most
/// the allocator-limited live task slots that nested destruction releases.
/// Null head/tail terminates an empty drain. Shutdown and cancellation reach
/// this queue only through the same unique final-reference path, and the outer
/// drain keeps the first panic while clearing its TLS registration. Reentrant
/// executor shutdown also uses one embedded node per distinct live executor:
/// task cancellation runs immediately, but timer/reactor teardown and the
/// existing terminal-descriptor worker join wait until callback-capable task
/// destruction has drained under each task's exact owner context. A temporary
/// self-pin keeps each deferred state alive; it adds no allocation, queue
/// storage, thread, or worker responsibility. No metric is emitted.
///
/// `ready_link` is the queue node and is detached from `all_tasks` before
/// publication. A singleton node has null link fields even while head/tail own
/// it, so the link itself does not encode membership. Valid task ownership
/// makes duplicate publication unreachable: `release_task` dispatches the
/// iterative destructor only on the unique 1-to-0 transition, which installs
/// the RAW vtable before any nested publication. This unsafe contract is not
/// repaired or coalesced on the fast path.
struct IterativeTaskDestroyQueue {
    head: *mut crate::utils::list::intrusive::dlist::Link,
    tail: *mut crate::utils::list::intrusive::dlist::Link,
    /// Exact owner for callback-capable destruction currently in progress.
    callback_owner: *const ExecutorOwner,
    /// Thread context restored after all deferred teardown has completed.
    initial_active_owner: *const ExecutorOwner,
    /// Executor states awaiting timer and reactor shutdown.
    deferred_runtime_head: *mut ExecutorState,
    /// Executor states whose runtime is down and whose existing close worker
    /// must be joined after every callback-capable destruction has drained.
    deferred_worker_head: *mut ExecutorState,
}

impl IterativeTaskDestroyQueue {
    const fn new(initial_active_owner: *const ExecutorOwner) -> Self {
        Self {
            head: std::ptr::null_mut(),
            tail: std::ptr::null_mut(),
            callback_owner: std::ptr::null(),
            initial_active_owner,
            deferred_runtime_head: std::ptr::null_mut(),
            deferred_worker_head: std::ptr::null_mut(),
        }
    }

    #[inline(always)]
    fn has_deferred_shutdown(&self) -> bool {
        !self.deferred_runtime_head.is_null() || !self.deferred_worker_head.is_null()
    }

    /// Records the exact owner whose callback-capable destruction is running.
    /// Once any shutdown is deferred, it also publishes that owner as the
    /// active teardown context without disturbing completion-drain state.
    unsafe fn set_callback_owner(&mut self, owner: *const ExecutorOwner) {
        debug_assert!(!owner.is_null());
        self.callback_owner = owner;
        if self.has_deferred_shutdown() {
            set_active_owner_for_iterative_destroy(owner);
        }
    }

    /// Registers one executor exactly once for deferred shutdown. The state is
    /// its own bounded intrusive node; the temporary `Rc` is only a lifetime
    /// pin and allocates no additional storage.
    unsafe fn defer_shutdown(&mut self, owner: &Rc<ExecutorOwner>) {
        let state = owner.state_ptr();
        debug_assert!(unsafe { (*state).deferred_shutdown_owner.is_none() });
        debug_assert!(unsafe { (*state).deferred_shutdown_next.is_null() });
        unsafe {
            (*state).deferred_shutdown_owner = Some(Rc::clone(owner));
            (*state).deferred_shutdown_next = self.deferred_runtime_head;
        }
        self.deferred_runtime_head = state;
    }

    unsafe fn pop_deferred_runtime(&mut self) -> Option<*mut ExecutorState> {
        let state = self.deferred_runtime_head;
        if state.is_null() {
            return None;
        }
        self.deferred_runtime_head = unsafe { (*state).deferred_shutdown_next };
        unsafe {
            (*state).deferred_shutdown_next = std::ptr::null_mut();
        }
        Some(state)
    }

    unsafe fn push_deferred_worker(&mut self, state: *mut ExecutorState) {
        debug_assert!(unsafe { (*state).deferred_shutdown_next.is_null() });
        unsafe {
            (*state).deferred_shutdown_next = self.deferred_worker_head;
        }
        self.deferred_worker_head = state;
    }

    unsafe fn pop_deferred_worker(&mut self) -> Option<*mut ExecutorState> {
        let state = self.deferred_worker_head;
        if state.is_null() {
            return None;
        }
        self.deferred_worker_head = unsafe { (*state).deferred_shutdown_next };
        unsafe {
            (*state).deferred_shutdown_next = std::ptr::null_mut();
        }
        Some(state)
    }

    unsafe fn push_back(&mut self, task: *mut TaskHeader) {
        let link = unsafe { std::ptr::addr_of_mut!((*task).ready_link) };
        debug_assert!(unsafe { (*link).is_unlinked() });
        unsafe {
            (*link).next = std::ptr::null_mut();
            (*link).prev = self.tail;
            if self.tail.is_null() {
                self.head = link;
            } else {
                (*self.tail).next = link;
            }
        }
        self.tail = link;
    }

    unsafe fn pop_front(&mut self) -> Option<*mut TaskHeader> {
        let link = self.head;
        if link.is_null() {
            return None;
        }
        self.head = unsafe { (*link).next };
        if self.head.is_null() {
            self.tail = std::ptr::null_mut();
        } else {
            unsafe {
                (*self.head).prev = std::ptr::null_mut();
            }
        }
        unsafe {
            (*link).next = std::ptr::null_mut();
            (*link).prev = std::ptr::null_mut();
        }
        Some(unsafe {
            link.cast::<u8>()
                .sub(TaskHeader::READY_LINK_OFFSET)
                .cast::<TaskHeader>()
        })
    }
}

thread_local! {
    static ITERATIVE_TASK_DESTROY_QUEUE: Cell<*mut IterativeTaskDestroyQueue> =
        const { Cell::new(std::ptr::null_mut()) };
    #[cfg(test)]
    static ITERATIVE_TASK_DESTROY_ENTRIES: Cell<usize> = const { Cell::new(0) };
}

struct IterativeTaskDestroyRegistration<'cell> {
    active: &'cell Cell<*mut IterativeTaskDestroyQueue>,
    initial_active_owner: *const ExecutorOwner,
}

impl Drop for IterativeTaskDestroyRegistration<'_> {
    fn drop(&mut self) {
        self.active.set(std::ptr::null_mut());
        set_active_owner_for_iterative_destroy(self.initial_active_owner);
    }
}

/// Replaces only the active executor identity while retaining any nested
/// completion-drain state. The iterative-destruction registration restores the
/// original identity on every exit path.
#[inline(always)]
fn set_active_owner_for_iterative_destroy(owner: *const ExecutorOwner) {
    EXECUTOR_CTX.with(|context| {
        let current = context.get();
        context.set(ExecutorThreadContext {
            active_owner: owner,
            ..current
        });
    });
}

/// Detaches one nested RAW task from registry ownership and appends it to the
/// active destruction FIFO before returning to the outer raw destructor.
unsafe fn enqueue_nested_task_destroy(
    queue: *mut IterativeTaskDestroyQueue,
    task: *mut TaskHeader,
    raw_vtable: &'static TaskVTable,
) {
    debug_assert_eq!(unsafe { (*task).refs.get() }, 0);
    debug_assert!(std::ptr::eq(unsafe { (*task).vtable }, raw_vtable));

    debug_assert!(unsafe { (*task).owner.is_some() });
    // SAFETY: ENTRY is installed only by the two real join-task constructors,
    // both of which publish their stable executor owner before either vtable.
    let owner = unsafe { (*task).owner.as_ref().unwrap_unchecked() };
    owner.debug_assert_owner_thread();
    let state = owner.state_ptr();
    let all_link = unsafe { std::ptr::addr_of_mut!((*task).all_link) };
    if unsafe { !(*all_link).is_unlinked() } {
        unsafe {
            (*state).all_tasks.remove(all_link);
        }
    }
    unsafe {
        (*queue).push_back(task);
    }
}

/// Drains callback-capable task destruction without recursive final releases.
///
/// # Safety
///
/// `task` must identify a real initialized owner-thread task at zero
/// references. `raw_vtable` must be the matching generic cleanup vtable.
#[cold]
#[inline(never)]
unsafe fn destroy_task_iteratively(task: *mut TaskHeader, raw_vtable: &'static TaskVTable) {
    unsafe {
        (*task).vtable = raw_vtable;
    }
    #[cfg(test)]
    ITERATIVE_TASK_DESTROY_ENTRIES.with(|entries| entries.set(entries.get() + 1));
    ITERATIVE_TASK_DESTROY_QUEUE.with(|active| {
        let active_queue = active.get();
        if !active_queue.is_null() {
            unsafe {
                enqueue_nested_task_destroy(active_queue, task, raw_vtable);
            }
            return;
        }

        let initial_active_owner = EXECUTOR_CTX.with(|context| context.get().active_owner);
        let mut queue = IterativeTaskDestroyQueue::new(initial_active_owner);
        let queue_ptr = std::ptr::from_mut(&mut queue);
        active.set(queue_ptr);
        let registration = IterativeTaskDestroyRegistration {
            active,
            initial_active_owner: unsafe { (*queue_ptr).initial_active_owner },
        };

        let already_panicking = std::thread::panicking();
        let mut first_panic = None;
        let owner = unsafe { (*task).owner.as_ref().unwrap_unchecked() };
        unsafe {
            (*queue_ptr).set_callback_owner(Rc::as_ptr(owner));
        }
        let raw_destroy = unsafe { (*task).vtable.destroy };
        let result = catch_unwind(AssertUnwindSafe(|| unsafe {
            raw_destroy(task);
        }));
        if already_panicking {
            if let Err(payload) = result {
                std::mem::forget(payload);
            }
        } else {
            retain_first_panic(&mut first_panic, result);
        }

        loop {
            while let Some(next) = unsafe { (*queue_ptr).pop_front() } {
                let owner = unsafe { (*next).owner.as_ref().unwrap_unchecked() };
                unsafe {
                    (*queue_ptr).set_callback_owner(Rc::as_ptr(owner));
                }
                let raw_destroy = unsafe { (*next).vtable.destroy };
                let result = catch_unwind(AssertUnwindSafe(|| unsafe {
                    raw_destroy(next);
                }));
                if already_panicking {
                    if let Err(payload) = result {
                        std::mem::forget(payload);
                    }
                } else {
                    retain_first_panic(&mut first_panic, result);
                }
            }

            let Some(state) = (unsafe { (*queue_ptr).pop_deferred_runtime() }) else {
                break;
            };
            // Keep the state visibly deferred while timer/reactor cleanup can
            // itself release tasks or request another executor shutdown.
            unsafe {
                (*queue_ptr).push_deferred_worker(state);
            }
            let owner = unsafe { (*state).deferred_shutdown_owner.as_ref().unwrap_unchecked() };
            unsafe {
                (*queue_ptr).set_callback_owner(Rc::as_ptr(owner));
            }
            let result = catch_unwind(AssertUnwindSafe(|| {
                let _runtime_shutdown_guard = RuntimeShutdownGuard::new(state);
            }));
            if already_panicking {
                if let Err(payload) = result {
                    std::mem::forget(payload);
                }
            } else {
                retain_first_panic(&mut first_panic, result);
            }
        }

        while let Some(state) = unsafe { (*queue_ptr).pop_deferred_worker() } {
            let owner_ptr =
                unsafe { Rc::as_ptr((*state).deferred_shutdown_owner.as_ref().unwrap_unchecked()) };
            unsafe {
                (*queue_ptr).set_callback_owner(owner_ptr);
                set_active_owner_for_iterative_destroy(owner_ptr);
                (*state).close_worker.shutdown();
                (*state).shutdown_complete = true;
            }
            // Taking the self-pin leaves a local strong reference, so state
            // remains alive through the last mutation and worker join. Do not
            // access `state` after this owner is dropped.
            let owner = unsafe { (*state).deferred_shutdown_owner.take() };
            drop(owner);
        }
        drop(registration);

        if let Some(payload) = first_panic {
            resume_unwind(payload);
        }
    });
}

/// Arms callback-capable destruction for one live owner-thread task.
///
/// # Safety
///
/// `task` must be a real initialized task with at least one reference, owned
/// by the current executor thread.
#[inline(always)]
unsafe fn arm_task_destruction(task: *mut TaskHeader) {
    debug_assert!(!task.is_null());
    let header = unsafe { &*task };
    debug_assert!(header.refs.get() > 0);
    if let Some(owner) = header.owner.as_ref() {
        owner.debug_assert_owner_thread();
    }
    unsafe {
        (*task).vtable = (*task).iterative_vtable;
    }
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

        let same_waker = unsafe { &*this.waker_ptr }
            .as_ref()
            .is_some_and(|stored| stored.will_wake(cx.waker()));
        if !same_waker {
            unsafe {
                arm_task_destruction(this.task_ptr);
            }
            let replacement = cx.waker().clone();
            let previous = unsafe { (&mut *this.waker_ptr).replace(replacement) };
            drop(previous);
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

#[cfg(all(
    target_arch = "x86_64",
    target_os = "linux",
    not(debug_assertions),
    not(feature = "test-support"),
    not(feature = "diagnostic-counters")
))]
const _: [(); 32] = [(); size_of::<Executor>()];

#[inline(always)]
fn timers_pending_after_processing(timers_pending: bool, recheck: impl FnOnce() -> bool) -> bool {
    timers_pending && recheck()
}

impl Executor {
    /// Returns the configured process quota for tests.
    #[cfg(all(not(miri), any(test, feature = "test-support")))]
    #[doc(hidden)]
    pub fn test_process_quota(&self) -> usize {
        self.process_quota
    }

    /// Returns the configured CPU affinity for tests.
    #[cfg(all(not(miri), any(test, feature = "test-support")))]
    #[doc(hidden)]
    pub fn test_cpu_affinity(&self) -> Option<usize> {
        self.cpu_affinity
    }

    /// Takes and resets opt-in diagnostic counters between executor runs.
    ///
    /// The test-support facade is the external entry point. Keeping this
    /// operation between `run` calls prevents snapshot/reset work from
    /// entering the timed benchmark interval.
    #[cfg(feature = "diagnostic-counters")]
    pub(crate) fn take_diagnostic_counters(&mut self) -> RuntimeDiagnosticCounters {
        let state = unsafe { &mut *self.owner.state_ptr() };
        state.reactor.take_diagnostic_counters()
    }

    /// Samples test-support-only ownership state between executor runs.
    ///
    /// Callers must use this only after `run` has returned. The snapshot walks
    /// already-owned slab metadata and reads existing registries/counters; it
    /// allocates nothing and adds no bookkeeping to ordinary builds.
    #[cfg(any(test, feature = "test-support"))]
    #[doc(hidden)]
    pub fn test_quiescence(&self) -> RuntimeQuiescence {
        let state = unsafe { &*self.owner.state_ptr() };
        let reactor = state.reactor.quiescence();
        let retained = reactor.retained;
        RuntimeQuiescence {
            live_tasks: state.runtime_state.live_tasks,
            inflight_ops: state.runtime_state.inflight_ops,
            ready_queue_empty: state.ready_queue.is_empty(),
            task_registry_empty: state.all_tasks.is_empty(),
            timers_pending: state.timers.has_pending(),
            live_ops: reactor.live_ops,
            pending_cancels: reactor.pending_cancels,
            queued_sqes: reactor.queued_sqes,
            pending_reactor_closes: reactor.pending_closes,
            deferred_reactor_closes: reactor.deferred_closes,
            executor_owner_refs: Rc::strong_count(&self.owner),
            scratch_owner_refs: retained.scratch_owner_refs,
            task_slab_pages: state.task_pool.slab_page_count(),
            operation_slab_pages: reactor.operation_slab_pages,
            timer_slab_pages: state.timers.slab_page_count(),
            retained_slab_pages: retained.payload_slab_pages,
            scratch_slab_pages: retained.scratch_slab_pages,
            retained_pooled_allocs: retained.stats.pooled_allocs,
            retained_pooled_frees: retained.stats.pooled_frees,
            retained_heap_allocs: retained.stats.heap_fallbacks,
            retained_heap_frees: retained.stats.heap_frees,
            scratch_pooled_allocs: retained.stats.writev_scratch_pooled_allocs,
            scratch_pooled_frees: retained.stats.writev_scratch_pooled_frees,
            storage_abandoned: reactor.storage_abandoned,
        }
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
                deferred_shutdown_next: std::ptr::null_mut(),
                deferred_shutdown_owner: None,
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
            let owner_ptr = ctx_cell.get().active_owner;
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

                // Keep the long-lived join-field pointers rooted in the whole
                // task allocation. `data.as_mut_ptr()` would first create a
                // field-local mutable borrow that a later whole-task vtable
                // reborrow can invalidate.
                let data_ptr = std::ptr::addr_of_mut!((*slot_ptr).data).cast::<JoinTask<F>>();
                // Initialize the join payload directly in its fixed task slot.
                // Building a by-value JoinTask first would move a future as
                // large as the slot through a second stack temporary.
                init_join_task_at(data_ptr, future);
                let result_ptr = std::ptr::addr_of_mut!((*data_ptr).result);
                let waker_ptr = std::ptr::addr_of_mut!((*data_ptr).join_waker);
                let vtables = join_task_vtable_for::<F>();
                let current_vtable = if std::mem::needs_drop::<F::Output>() {
                    vtables.iterative
                } else {
                    vtables.direct
                };

                // Start with refcount 2: one for the executor, one for the JoinHandle.
                init_task_slot_header(
                    std::ptr::addr_of_mut!((*slot_ptr).header),
                    owner_ptr,
                    2,
                    TaskHeader::FLAG_NOTIFIED | TaskHeader::FLAG_QUEUED,
                    current_vtable,
                    vtables.iterative,
                );

                (*state_ptr).runtime_state.live_tasks += 1;
                #[cfg(debug_assertions)]
                {
                    let stats = &mut (*state_ptr).runtime_state.stats;
                    stats.task_allocs = stats.task_allocs.saturating_add(1);
                }
                (*state_ptr)
                    .all_tasks
                    .push_back_unchecked(std::ptr::addr_of_mut!((*slot_ptr).header.all_link));
                (*state_ptr)
                    .ready_queue
                    .push_back_unchecked(std::ptr::addr_of_mut!((*slot_ptr).header.ready_link));

                Ok(JoinHandle {
                    task_ptr: std::ptr::addr_of_mut!((*slot_ptr).header),
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
    /// supported. An out-of-range configured CPU affinity or a root future too
    /// large for one task slot also returns [`io::ErrorKind::InvalidInput`].
    /// Root-task allocation pressure returns [`io::ErrorKind::OutOfMemory`].
    /// Returns [`io::ErrorKind::NotConnected`] if this executor has begun
    /// shutdown; an executor cannot be restarted after shutdown.
    /// Futures submitted or armed by one executor must remain on that executor;
    /// polling them outside an active run or through another executor's task
    /// waker also returns [`io::ErrorKind::NotConnected`].
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
        let state_ptr = self.owner.state_ptr();
        if unsafe { (*state_ptr).shutting_down } {
            return Err(io::Error::from(ErrorKind::NotConnected));
        }
        self.init()?;
        apply_cpu_affinity(self.cpu_affinity)?;

        let owner_ptr = Rc::as_ptr(&self.owner);
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

        #[cfg(debug_assertions)]
        let retained_stats_baseline = unsafe { (*state_ptr).reactor.retained_payload_stats() };

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
                    let stats = &mut (*state_ptr).runtime_state.stats;
                    stats.task_polls = stats.task_polls.saturating_add(1);
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
                    self.snapshot_stats(retained_stats_baseline);
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
                    self.snapshot_stats(retained_stats_baseline);
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
    fn snapshot_stats(&mut self, retained_stats_baseline: RetainedPayloadPoolStats) {
        let state = unsafe { &mut *self.owner.state_ptr() };
        let runtime_state = &mut state.runtime_state;
        let provider = state.task_pool.provider_ref();
        runtime_state.stats.task_slab_allocs = provider.request_count;
        runtime_state.stats.task_slab_frees = provider.free_count;
        let retained = state
            .reactor
            .retained_payload_stats()
            .saturating_delta_since(retained_stats_baseline);
        apply_retained_payload_stats(&mut runtime_state.stats, retained);
        self.last_stats = runtime_state.stats;
    }

    /// Returns the debug snapshot captured by the latest run that drained or
    /// reached the stalled-work `WouldBlock` check.
    ///
    /// All fields except `retained_*` and `writev_scratch_*` are cumulative
    /// across the current uninterrupted execution generation. A
    /// `WouldBlock` return with unfinished work and the later run that resumes
    /// it therefore share those totals. Retained-payload and vectored-scratch
    /// fields are instead saturating deltas from the latest run's entry, so
    /// pool activity between two calls is part of the later baseline rather
    /// than that invocation's result. Direct retained-pool test-support
    /// snapshots keep their lifetime-total semantics.
    ///
    /// In release builds this dev-only accessor returns an empty snapshot
    /// because the counters are not compiled in.
    #[cfg(all(not(miri), any(test, feature = "test-support")))]
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

            if unsafe { (*task_ptr).refs.get() } == 0 {
                debug_assert!(!std::ptr::eq(unsafe { (*task_ptr).vtable }, unsafe {
                    (*task_ptr).iterative_vtable
                },));
                debug_assert!(task_is_completed(unsafe { (*task_ptr).flags.get() }));
                debug_assert!(unsafe { (*task_ptr).ready_link.is_unlinked() });
                continue;
            }
            unsafe {
                arm_task_destruction(task_ptr);
            }

            let ready_link = unsafe { std::ptr::addr_of_mut!((*task_ptr).ready_link) };
            if unsafe { !(*ready_link).is_unlinked() } {
                unsafe {
                    (*state_ptr).ready_queue.remove(ready_link);
                }
            }

            let flags = unsafe { task_flags_unchecked(task_ptr) };
            if task_is_completed(flags) {
                unsafe {
                    replace_task_flags_unchecked(
                        task_ptr,
                        (flags
                            & !(TaskHeader::FLAG_RUNNING
                                | TaskHeader::FLAG_NOTIFIED
                                | TaskHeader::FLAG_QUEUED))
                            | TaskHeader::FLAG_COMPLETED,
                    );
                }
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
        if unsafe {
            !(*state_ptr).initialized
                || (*state_ptr).shutdown_complete
                || (*state_ptr).deferred_shutdown_owner.is_some()
        } {
            return;
        }

        let owner_ptr = Rc::as_ptr(&self.owner);
        let mut first_panic = None;
        let destroy_queue = ITERATIVE_TASK_DESTROY_QUEUE.with(Cell::get);
        let teardown_result = if destroy_queue.is_null() {
            catch_unwind(AssertUnwindSafe(|| {
                let _ctx_guard = ExecutorCtxGuard::install_for_shutdown(owner_ptr);
                let _shutdown_complete_guard = ShutdownCompleteGuard::new(state_ptr);
                let _close_worker_guard = CloseWorkerShutdownGuard::new(unsafe {
                    std::ptr::addr_of_mut!((*state_ptr).close_worker)
                });
                let _runtime_shutdown_guard = RuntimeShutdownGuard::new(state_ptr);
                first_panic = self.shutdown_tasks();
            }))
        } else {
            let previous_callback_owner = unsafe { (*destroy_queue).callback_owner };
            debug_assert!(!previous_callback_owner.is_null());
            // Register before cancellation so any shutdown re-entry observes
            // the in-progress state and cannot run a teardown phase twice.
            unsafe {
                (*destroy_queue).defer_shutdown(&self.owner);
                (*destroy_queue).set_callback_owner(owner_ptr);
            }
            let result = catch_unwind(AssertUnwindSafe(|| {
                let _ctx_guard = ExecutorCtxGuard::install_for_shutdown(owner_ptr);
                first_panic = self.shutdown_tasks();
            }));
            // Cancellation callbacks belong to the shutdown target; execution
            // now returns to the task destructor that invoked shutdown.
            unsafe {
                (*destroy_queue).set_callback_owner(previous_callback_owner);
            }
            result
        };
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
    unsafe { Reactor::free_op_unchecked(reactor, ptr) };
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
        (*state_ptr).debug_assert_valid_flags();
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
            (*state_ptr).debug_assert_valid_flags();
        } else {
            cancel_op_unchecked(state_ptr);
        }
    }
}

/// Releases the submitted completion owned by an fd-operation state.
///
/// Borrowed initial state needs no cleanup, and an unsubmitted staged state
/// releases its owned lease through [`RuntimeFdOpState`]'s destructor. This
/// bridge handles only the published completion-pointer representation.
///
/// # Safety
///
/// A published pointer in `fd_state` must satisfy the ownership requirements
/// of [`drop_op_ptr_unchecked`].
#[inline(always)]
pub(crate) unsafe fn drop_fd_op_state_unchecked(fd_state: &mut RuntimeFdOpState<'_>) {
    let mut state_ptr = fd_state.take_state_ptr();
    unsafe { drop_op_ptr_unchecked(&mut state_ptr) };
}

/// Owns an allocated completion-state slot until its target SQE is submitted.
///
/// The guard is shared by I/O families that must keep the state local while
/// fallible or user-controlled preparation runs. Dropping it returns the
/// unsubmitted slot; successful submission consumes it without a conditional
/// drop branch. Typed fd submission publishes through `RuntimeFdOpState`, while
/// older lease-free routes can still take the raw pointer directly.
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
    #[cfg(all(not(miri), any(test, feature = "test-support")))]
    #[inline(always)]
    pub(crate) fn into_state_ptr(self) -> *mut CompletionState {
        let this = std::mem::ManuallyDrop::new(self);
        this.state.as_ptr()
    }

    /// Disarms the guard after typed fd submission published the state through
    /// its [`RuntimeFdOpState`].
    #[inline(always)]
    pub(crate) fn disarm(self) {
        let _this = std::mem::ManuallyDrop::new(self);
    }
}

impl Drop for UnsubmittedOpGuard {
    #[inline(always)]
    fn drop(&mut self) {
        unsafe { Reactor::free_op_unchecked(self.reactor.as_ptr(), self.state.as_ptr()) };
    }
}

/// Allocates an operation state, gives it an owning pre-submission guard, and
/// registers the active task as its waiter.
///
/// # Safety
///
/// `pctx` must identify a live FlowIO executor poll context with a non-null
/// owner task. The returned guard must remain the state's unique owner until
/// submission succeeds or preparation is abandoned.
#[inline(always)]
pub(crate) unsafe fn prepare_unsubmitted_op(pctx: &PollCtx) -> Option<UnsubmittedOpGuard> {
    let state_ptr = unsafe { (*pctx.reactor()).alloc_op() };
    if state_ptr.is_null() {
        return None;
    }

    let guard = unsafe { UnsubmittedOpGuard::new(pctx.reactor(), state_ptr) };
    unsafe { (*state_ptr).register_waiter(pctx.owner_task()) };
    Some(guard)
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
    unsafe { submit_tracked_sqe_with_publication(pctx, sqe, || {}) }
}

/// Pushes one SQE, performs its infallible userspace publication callback at
/// the exact successful-push boundary, then records ordinary accounting.
#[inline(always)]
unsafe fn submit_tracked_sqe_with_publication<F>(
    pctx: &PollCtx,
    sqe: io_uring::squeue::Entry,
    publish: F,
) -> io::Result<()>
where
    F: FnOnce(),
{
    // Compute the only fallible bookkeeping result before the irreversible
    // userspace-SQ push. Saturated in-flight ownership cannot be represented.
    let next_inflight = unsafe { (*pctx.runtime_state()).inflight_ops }
        .checked_add(1)
        .ok_or_else(|| io::Error::from(io::ErrorKind::OutOfMemory))?;
    unsafe { (*pctx.reactor()).submit_sqe(sqe)? };
    publish();
    unsafe {
        (*pctx.runtime_state()).inflight_ops = next_inflight;
        #[cfg(debug_assertions)]
        {
            let stats = &mut (*pctx.runtime_state()).stats;
            stats.sqe_submits = stats.sqe_submits.saturating_add(1);
        }
    }
    Ok(())
}

/// Build and publish the next SQE for a sequential operation using the exact
/// lease retained by its original completion state.
///
/// # Safety
///
/// `fd_state` must contain the completed/reset state owned by `pctx`, with its
/// original fd lease still attached. `build` must use the supplied fd.
#[inline(always)]
pub(crate) unsafe fn submit_resubmitted_fd_sqe<F>(
    pctx: &PollCtx,
    fd_state: &RuntimeFdOpState<'_>,
    build: F,
) -> io::Result<()>
where
    F: FnOnce(std::os::fd::RawFd) -> io::Result<squeue::Entry>,
{
    let state_ptr = fd_state.state_ptr();
    debug_assert!(
        !state_ptr.is_null(),
        "fd resubmission requires a published completion state"
    );
    let fd = fd_state.raw_fd();
    let sqe = build(fd)?;
    // On any pre-push error, leave the lease attached. The caller retires the
    // payload and state through ordinary reclamation, which takes the lease
    // local before pool cleanup and drops it only after the pool borrow ends.
    unsafe { submit_tracked_sqe(pctx, sqe) }
}

/// Test-support codegen probe for same-state typed fd resubmission.
///
/// # Safety
///
/// `fd_state` must point to a live published [`RuntimeFdOpState`] whose
/// completion state retains its original descriptor lease. `poll_ctx` is a
/// reserved opaque argument and may be null; the probe isolates only the
/// ownership fragment used before the separately inspected ring submission.
#[cfg(feature = "test-support")]
#[doc(hidden)]
#[unsafe(no_mangle)]
#[inline(never)]
pub unsafe extern "C" fn flowio_probe_resubmit_same_state(
    _poll_ctx: *const (),
    fd_state: *const (),
) -> i32 {
    let fd_state = unsafe { &*fd_state.cast::<RuntimeFdOpState<'static>>() };
    fd_state.raw_fd()
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

/// Retain a payload and publish an initial fd-backed SQE using the operation
/// state's borrowed-or-staged ownership policy.
///
/// # Safety
///
/// The requirements of [`submit_retained_sqe`] apply. `fd_state` must still be
/// in its initial borrowed or staged-owned representation, and `build` must
/// derive its entry descriptor from the supplied typed raw fd.
#[inline(always)]
pub(crate) unsafe fn submit_retained_fd_sqe<T: 'static, F>(
    pctx: &PollCtx,
    state_ptr: *mut CompletionState,
    fd_state: &mut RuntimeFdOpState<'_>,
    payload_value: T,
    build: F,
) -> Result<(), (io::Error, T)>
where
    F: FnOnce(std::os::fd::RawFd, &mut T) -> io::Result<squeue::Entry>,
{
    let reactor = pctx.reactor();
    let payload = unsafe { (*reactor).alloc_retained_payload(payload_value) };
    unsafe { submit_initialized_retained_fd_sqe(pctx, state_ptr, fd_state, payload, build) }
}

/// Attach an initialized payload and publish an initial fd-backed SQE using
/// the operation state's borrowed-or-staged ownership policy.
///
/// # Safety
///
/// The requirements of [`submit_initialized_retained_sqe`] apply. `fd_state`
/// must still be in its initial borrowed or staged-owned representation, and
/// `build` must derive its entry descriptor from the supplied typed raw fd.
#[inline(always)]
pub(crate) unsafe fn submit_initialized_retained_fd_sqe<T: 'static, F>(
    pctx: &PollCtx,
    state_ptr: *mut CompletionState,
    fd_state: &mut RuntimeFdOpState<'_>,
    payload: RetainedPayload<T>,
    build: F,
) -> Result<(), (io::Error, T)>
where
    F: FnOnce(std::os::fd::RawFd, &mut T) -> io::Result<squeue::Entry>,
{
    unsafe {
        submit_initialized_retained_sqe_inner(
            pctx,
            state_ptr,
            payload,
            Some(fd_state),
            |raw_fd, payload| {
                // SAFETY: the typed fd branch always supplies its capability's
                // raw descriptor to the builder.
                build(raw_fd.unwrap_unchecked(), payload)
            },
        )
    }
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
    unsafe {
        submit_initialized_retained_sqe_inner(pctx, state_ptr, payload, None, |_, payload| {
            build(payload)
        })
    }
}

#[inline(always)]
unsafe fn submit_initialized_retained_sqe_inner<T: 'static, F>(
    pctx: &PollCtx,
    state_ptr: *mut CompletionState,
    payload: RetainedPayload<T>,
    mut fd_state: Option<&mut RuntimeFdOpState<'_>>,
    build: F,
) -> Result<(), (io::Error, T)>
where
    F: FnOnce(Option<std::os::fd::RawFd>, &mut T) -> io::Result<squeue::Entry>,
{
    let reactor = pctx.reactor();
    unsafe { (*state_ptr).attach_retained_payload(payload) };
    let retained_pool = unsafe { Reactor::retained_payload_pool_ptr(reactor) };
    let payload_guard = unsafe { AttachedRetainedPayloadGuard::<T>::new(state_ptr, retained_pool) };

    let sqe = match build(fd_state.as_ref().map(|state| state.raw_fd()), unsafe {
        (*state_ptr).retained_payload_mut::<T>()
    }) {
        Ok(sqe) => {
            payload_guard.disarm();
            sqe
        }
        Err(err) => {
            let payload = unsafe { payload_guard.take() };
            return Err((err, payload));
        }
    };

    if let Some(fd_state) = fd_state.as_deref_mut() {
        let lease = unsafe { fd_state.take_initial_lease() };
        unsafe { (*state_ptr).attach_fd_lease(lease) };
    }

    if let Err(err) = unsafe {
        submit_tracked_sqe_with_publication(pctx, sqe, || {
            if let Some(fd_state) = fd_state {
                // SAFETY: the successful push is the publication boundary.
                // The state already owns the matching lease, and this pointer
                // assignment is infallible and cannot unwind.
                fd_state.publish_submitted_state(state_ptr);
            }
        })
    } {
        let payload = unsafe { Reactor::take_retained_payload_unchecked::<T>(reactor, state_ptr) };
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
    /// A live completion view prevented ring access, so the exact reactor
    /// retained the owner in its bounded post-view FIFO.
    Deferred,
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
        let owner = ctx_cell.get().active_owner;
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
        let owner = ctx_cell.get().active_owner;
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
                        let stats = &mut (*_runtime_state).stats;
                        stats.close_worker_admissions =
                            stats.close_worker_admissions.saturating_add(1);
                    }
                    CloseAdmission::Admitted
                }
                Err(CloseWorkerRejection::Full(fd)) => {
                    #[cfg(debug_assertions)]
                    {
                        let stats = &mut (*_runtime_state).stats;
                        stats.close_worker_full_fallbacks =
                            stats.close_worker_full_fallbacks.saturating_add(1);
                    }
                    CloseAdmission::Full(fd)
                }
                Err(CloseWorkerRejection::Disconnected(fd)) => {
                    #[cfg(debug_assertions)]
                    {
                        let stats = &mut (*_runtime_state).stats;
                        stats.close_worker_disconnected_fallbacks =
                            stats.close_worker_disconnected_fallbacks.saturating_add(1);
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
        let context = ctx_cell.get();
        let owner = context.active_owner;
        if context.completion_drain_active {
            #[cfg(debug_assertions)]
            if !owner.is_null() {
                unsafe {
                    (*owner).debug_assert_owner_thread();
                }
            }
            let drain_reactor = context.completion_drain_reactor;
            if !owner.is_null() && !drain_reactor.is_null() {
                match unsafe { Reactor::defer_close_during_completion_drain(drain_reactor, fd) } {
                    Ok(()) => return CloseSubmission::Deferred,
                    Err(fd) => {
                        #[cfg(debug_assertions)]
                        unsafe {
                            let stats = &mut (*(*owner).state_ptr()).runtime_state.stats;
                            stats.close_ring_fallbacks =
                                stats.close_ring_fallbacks.saturating_add(1);
                        }
                        return CloseSubmission::Rejected(fd);
                    }
                }
            }
            #[cfg(debug_assertions)]
            if !owner.is_null() {
                unsafe {
                    let stats = &mut (*(*owner).state_ptr()).runtime_state.stats;
                    stats.close_ring_fallbacks = stats.close_ring_fallbacks.saturating_add(1);
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
            match Reactor::try_submit_close_on_reactor(reactor, runtime_state, fd) {
                Ok(()) => CloseSubmission::Submitted,
                Err(fd) => CloseSubmission::Rejected(fd),
            }
        }
    })
}

#[inline(always)]
pub(crate) fn note_close_direct() {
    #[cfg(debug_assertions)]
    record_runtime_stat(|stats| {
        stats.close_direct_closes = stats.close_direct_closes.saturating_add(1);
    });
}

#[inline(always)]
pub(crate) fn note_accept_readiness_rearm() {
    #[cfg(debug_assertions)]
    record_runtime_stat(|stats| {
        stats.accept_readiness_rearms = stats.accept_readiness_rearms.saturating_add(1);
    });
}

#[inline(always)]
pub(crate) fn note_accept_descriptor_exhaustion() {
    #[cfg(debug_assertions)]
    record_runtime_stat(|stats| {
        stats.accept_descriptor_exhaustions = stats.accept_descriptor_exhaustions.saturating_add(1);
    });
}

#[inline(always)]
pub(crate) fn note_close_linger_query() {
    #[cfg(debug_assertions)]
    record_runtime_stat(|stats| {
        stats.close_linger_queries = stats.close_linger_queries.saturating_add(1);
    });
}

#[inline(always)]
pub(crate) fn note_close_linger_classification_failure() {
    #[cfg(debug_assertions)]
    record_runtime_stat(|stats| {
        stats.close_linger_classification_failures =
            stats.close_linger_classification_failures.saturating_add(1);
    });
}

#[inline(always)]
pub(crate) fn note_close_linger_waiver(waived: bool, failed: bool) {
    #[cfg(debug_assertions)]
    record_runtime_stat(|stats| {
        stats.close_linger_waivers = stats
            .close_linger_waivers
            .saturating_add(usize::from(waived));
        stats.close_linger_waiver_failures = stats
            .close_linger_waiver_failures
            .saturating_add(usize::from(failed));
    });
    #[cfg(not(debug_assertions))]
    let _ = (waived, failed);
}

#[inline(always)]
pub(crate) fn note_waiter_wake() {
    #[cfg(debug_assertions)]
    record_runtime_stat(|stats| {
        stats.waiter_wakes = stats.waiter_wakes.saturating_add(1);
    });
}

#[inline(always)]
pub(crate) fn note_timer_now_tick_call() {
    #[cfg(debug_assertions)]
    record_runtime_stat(|stats| {
        stats.timer_now_tick_calls = stats.timer_now_tick_calls.saturating_add(1);
    });
}

#[inline(always)]
pub(crate) fn note_timer_expired() {
    #[cfg(debug_assertions)]
    record_runtime_stat(|stats| {
        stats.timer_expired = stats.timer_expired.saturating_add(1);
    });
}

#[cfg(debug_assertions)]
#[inline(always)]
fn record_runtime_stat(update: impl FnOnce(&mut RuntimeStats)) {
    EXECUTOR_CTX.with(|ctx_cell| {
        let owner = ctx_cell.get().active_owner;
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
        let owner = ctx_cell.get().active_owner;
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
        let owner = ctx_cell.get().active_owner;
        debug_assert!(
            !owner.is_null(),
            "runtime schedule_ctx_unchecked requested outside executor context"
        );
        unsafe { schedule_ctx_from_owner_unchecked(owner) }
    })
}

fn join_task_vtable_for<F>() -> JoinTaskVTables
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

        const ITERATIVE_VTABLE: TaskVTable = TaskVTable {
            poll: Self::VTABLE.poll,
            finish: Self::VTABLE.finish,
            cancel: Self::VTABLE.cancel,
            destroy: |ptr| unsafe {
                destroy_task_iteratively(ptr, &Self::VTABLE);
            },
        };
    }

    JoinTaskVTables {
        direct: &VTableGen::<F>::VTABLE,
        iterative: &VTableGen::<F>::ITERATIVE_VTABLE,
    }
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
/// Replaces the complete packed scheduler flag word in a live task header.
///
/// # Safety
///
/// `task_ptr` must be non-null, aligned, and exclusively scheduler-accessible
/// on the owning executor thread. `flags` must contain the complete next state.
unsafe fn replace_task_flags_unchecked(task_ptr: *mut TaskHeader, flags: u64) {
    unsafe {
        (*std::ptr::addr_of!((*task_ptr).flags)).set(flags);
    }
}

#[inline(always)]
/// Adds one scheduler flag to a live task header.
///
/// # Safety
///
/// `task_ptr` must be non-null, aligned, and exclusively scheduler-accessible
/// on the owning executor thread.
unsafe fn set_task_flag_unchecked(task_ptr: *mut TaskHeader, flag: u64) {
    let flags = unsafe { task_flags_unchecked(task_ptr) };
    unsafe {
        replace_task_flags_unchecked(task_ptr, flags | flag);
    }
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
                let stats = &mut (*_runtime_state).stats;
                stats.task_schedules = stats.task_schedules.saturating_add(1);
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
    use std::cell::{Cell, RefCell};
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

    struct StagedTaskDropProbe {
        drops: Rc<Cell<usize>>,
        panic_on_drop: bool,
    }

    impl StagedTaskDropProbe {
        fn new(drops: Rc<Cell<usize>>, panic_on_drop: bool) -> Self {
            Self {
                drops,
                panic_on_drop,
            }
        }
    }

    impl Drop for StagedTaskDropProbe {
        fn drop(&mut self) {
            self.drops.set(self.drops.get() + 1);
            if self.panic_on_drop {
                panic!("staged task output drop panic");
            }
        }
    }

    #[derive(Clone, Copy)]
    struct DeferredShutdownDropExpectation {
        owner: *const ExecutorOwner,
        state: *mut ExecutorState,
    }

    fn assert_deferred_shutdown_drop_context(expected: DeferredShutdownDropExpectation) {
        EXECUTOR_CTX.with(|context| {
            assert_eq!(context.get().active_owner, expected.owner);
        });
        unsafe {
            assert!((*expected.state).shutting_down);
            assert!(!(*expected.state).shutdown_complete);
            assert_eq!(
                (*expected.state)
                    .deferred_shutdown_owner
                    .as_ref()
                    .map(Rc::as_ptr),
                Some(expected.owner),
            );
        }
        ITERATIVE_TASK_DESTROY_QUEUE.with(|active| {
            let queue = active.get();
            assert!(!queue.is_null(), "deferred shutdown lost its destroy FIFO");
            unsafe {
                assert_eq!((*queue).callback_owner, expected.owner);
                assert!((*queue).has_deferred_shutdown());
            }
        });
    }

    struct DestroyLinkDropProbe {
        task: Rc<Cell<*mut TaskHeader>>,
        id: usize,
        order: Rc<RefCell<Vec<usize>>>,
        remaining_task: Rc<Cell<*mut TaskHeader>>,
        shutdown_context: Option<DeferredShutdownDropExpectation>,
    }

    impl Drop for DestroyLinkDropProbe {
        fn drop(&mut self) {
            if let Some(expected) = self.shutdown_context {
                assert_deferred_shutdown_drop_context(expected);
            }
            let task = self.task.get();
            assert!(!task.is_null(), "destroy-link probe lost its task");
            let remaining_task = self.remaining_task.get();
            ITERATIVE_TASK_DESTROY_QUEUE.with(|active| {
                let queue = active.get();
                assert!(
                    !queue.is_null(),
                    "destroy FIFO was not registered at raw drop"
                );
                let remaining_link = if remaining_task.is_null() {
                    std::ptr::null_mut()
                } else {
                    unsafe { std::ptr::addr_of_mut!((*remaining_task).ready_link) }
                };
                assert_eq!(unsafe { (*queue).head }, remaining_link);
                assert_eq!(unsafe { (*queue).tail }, remaining_link);
            });
            assert!(unsafe { (*task).ready_link.is_unlinked() });
            assert!(unsafe { (*task).all_link.is_unlinked() });
            let mut order = self.order.borrow_mut();
            assert!(
                !order.contains(&self.id),
                "destroy-link probe dropped twice"
            );
            order.push(self.id);
        }
    }

    #[derive(Debug, Eq, PartialEq)]
    struct SyntheticChainPanic(usize);

    #[derive(Debug, Eq, PartialEq)]
    struct SyntheticOuterPanic;

    #[derive(Default)]
    struct SyntheticChainStats {
        depth: Cell<usize>,
        max_depth: Cell<usize>,
        order: RefCell<Vec<usize>>,
    }

    struct SyntheticTaskRef {
        task: *mut TaskHeader,
    }

    impl Drop for SyntheticTaskRef {
        fn drop(&mut self) {
            unsafe {
                release_task(self.task);
            }
        }
    }

    struct DeferredShutdownTrigger {
        executor: *mut Executor,
        nested: Option<SyntheticTaskRef>,
        calls: Rc<Cell<usize>>,
    }

    impl Drop for DeferredShutdownTrigger {
        fn drop(&mut self) {
            self.calls.set(self.calls.get() + 1);
            drop(self.nested.take());
            unsafe {
                (&mut *self.executor).shutdown_owner();
                let owner = Rc::as_ptr(&(*self.executor).owner);
                let state = (*self.executor).owner.state_ptr();
                assert_deferred_shutdown_drop_context(DeferredShutdownDropExpectation {
                    owner,
                    state,
                });
            }
        }
    }

    struct ReentrantCloneWakerState {
        armed_task: *mut TaskHeader,
        iterative_vtable: &'static TaskVTable,
        release_during_clone: RefCell<Option<SyntheticTaskRef>>,
        clones: Cell<usize>,
    }

    unsafe fn reentrant_clone_waker_clone(data: *const ()) -> RawWaker {
        let state = unsafe { Rc::<ReentrantCloneWakerState>::from_raw(data.cast()) };
        assert!(std::ptr::eq(
            unsafe { (*state.armed_task).vtable },
            state.iterative_vtable,
        ));
        state.clones.set(state.clones.get() + 1);
        let nested_release = state.release_during_clone.borrow_mut().take();
        drop(nested_release);
        let cloned = Rc::clone(&state);
        let _ = Rc::into_raw(state);
        RawWaker::new(Rc::into_raw(cloned).cast(), &REENTRANT_CLONE_WAKER_VTABLE)
    }

    unsafe fn reentrant_clone_waker_wake(data: *const ()) {
        drop(unsafe { Rc::<ReentrantCloneWakerState>::from_raw(data.cast()) });
    }

    unsafe fn reentrant_clone_waker_wake_by_ref(data: *const ()) {
        let state = unsafe { Rc::<ReentrantCloneWakerState>::from_raw(data.cast()) };
        let _ = Rc::into_raw(state);
    }

    unsafe fn reentrant_clone_waker_drop(data: *const ()) {
        drop(unsafe { Rc::<ReentrantCloneWakerState>::from_raw(data.cast()) });
    }

    static REENTRANT_CLONE_WAKER_VTABLE: RawWakerVTable = RawWakerVTable::new(
        reentrant_clone_waker_clone,
        reentrant_clone_waker_wake,
        reentrant_clone_waker_wake_by_ref,
        reentrant_clone_waker_drop,
    );

    fn reentrant_clone_waker(state: &Rc<ReentrantCloneWakerState>) -> Waker {
        let data = Rc::into_raw(Rc::clone(state)).cast();
        unsafe { Waker::from_raw(RawWaker::new(data, &REENTRANT_CLONE_WAKER_VTABLE)) }
    }

    struct SyntheticChainNode {
        id: usize,
        next: Option<SyntheticTaskRef>,
        stats: Rc<SyntheticChainStats>,
        panic_mask: u64,
    }

    impl Drop for SyntheticChainNode {
        fn drop(&mut self) {
            let depth = self.stats.depth.get() + 1;
            self.stats.depth.set(depth);
            self.stats
                .max_depth
                .set(self.stats.max_depth.get().max(depth));
            self.stats.order.borrow_mut().push(self.id);
            drop(self.next.take());
            self.stats.depth.set(depth - 1);
            if self.id < u64::BITS as usize && (self.panic_mask & (1_u64 << self.id)) != 0 {
                std::panic::panic_any(SyntheticChainPanic(self.id));
            }
        }
    }

    struct SyntheticBranchNode {
        id: usize,
        children: [Option<SyntheticTaskRef>; 2],
        stats: Rc<SyntheticChainStats>,
    }

    impl Drop for SyntheticBranchNode {
        fn drop(&mut self) {
            let depth = self.stats.depth.get() + 1;
            self.stats.depth.set(depth);
            self.stats
                .max_depth
                .set(self.stats.max_depth.get().max(depth));
            self.stats.order.borrow_mut().push(self.id);
            drop(self.children[0].take());
            drop(self.children[1].take());
            self.stats.depth.set(depth - 1);
        }
    }

    fn stage_synthetic_branch(
        id: usize,
        children: [Option<SyntheticTaskRef>; 2],
        stats: &Rc<SyntheticChainStats>,
    ) -> SyntheticTaskRef {
        let mut staged = stage_completed_task_output_for_benchmark(SyntheticBranchNode {
            id,
            children,
            stats: Rc::clone(stats),
        })
        .expect("synthetic branch task staging failed");
        let task = staged.task;
        staged.owns_reference = false;
        drop(staged);
        SyntheticTaskRef { task }
    }

    fn staged_synthetic_chain(
        depth: usize,
        stats: &Rc<SyntheticChainStats>,
        panic_mask: u64,
    ) -> (SyntheticTaskRef, *mut TaskHeader) {
        assert!(depth > 0);
        let mut next = None;
        let mut leaf = std::ptr::null_mut();
        for id in 0..depth {
            let mut staged = stage_completed_task_output_for_benchmark(SyntheticChainNode {
                id,
                next,
                stats: Rc::clone(stats),
                panic_mask,
            })
            .expect("synthetic chain task staging failed");
            if id == 0 {
                leaf = staged.task;
            }
            let task = staged.task;
            staged.owns_reference = false;
            drop(staged);
            next = Some(SyntheticTaskRef { task });
        }
        (next.expect("synthetic chain head is missing"), leaf)
    }

    #[cfg(not(miri))]
    struct RealChainNode {
        id: usize,
        next: Option<Box<JoinHandle<RealChainNode>>>,
        stats: Rc<SyntheticChainStats>,
    }

    #[cfg(not(miri))]
    impl Drop for RealChainNode {
        fn drop(&mut self) {
            let depth = self.stats.depth.get() + 1;
            self.stats.depth.set(depth);
            self.stats
                .max_depth
                .set(self.stats.max_depth.get().max(depth));
            self.stats.order.borrow_mut().push(self.id);
            drop(self.next.take());
            self.stats.depth.set(depth - 1);
        }
    }

    unsafe fn assert_staged_tasks_reclaimed(
        owner: &ExecutorOwner,
        expected_allocs: usize,
        expected_frees: usize,
    ) {
        let state = owner.state_ptr();
        assert!(
            unsafe { (*state).all_tasks.is_empty() },
            "staged task remained linked after final release"
        );
        #[cfg(debug_assertions)]
        unsafe {
            assert_eq!(
                (*state).runtime_state.stats.task_allocs,
                expected_allocs,
                "staged task allocation count changed"
            );
            assert_eq!(
                (*state).runtime_state.stats.task_frees,
                expected_frees,
                "staged task free count changed"
            );
        }
        #[cfg(not(debug_assertions))]
        {
            let _ = (expected_allocs, expected_frees);
        }
    }

    #[test]
    fn staged_completed_task_owner_drop_unlinks_and_reuses_exact_slot() {
        with_ringless_poll_context_for_test(1, |owner, _cx| {
            let drops = Rc::new(Cell::new(0));
            let staged = stage_completed_task_output_for_benchmark(StagedTaskDropProbe::new(
                Rc::clone(&drops),
                false,
            ))
            .expect("completed task staging failed");
            let first_task = staged.task;
            assert!(
                unsafe { !(*owner.state_ptr()).all_tasks.is_empty() },
                "staged task was not linked"
            );

            drop(staged);
            assert_eq!(drops.get(), 1);
            unsafe {
                assert_staged_tasks_reclaimed(owner, 1, 1);
            }

            let replacement = stage_completed_task_output_for_benchmark(StagedTaskDropProbe::new(
                Rc::clone(&drops),
                false,
            ))
            .expect("replacement completed task staging failed");
            assert_eq!(
                replacement.task, first_task,
                "staged task slot was not exactly reusable"
            );
            drop(replacement);
            assert_eq!(drops.get(), 2);
            unsafe {
                assert_staged_tasks_reclaimed(owner, 2, 2);
            }
        });
    }

    #[test]
    fn join_task_initial_destruction_policy_and_shutdown_arm_are_exact() {
        struct DropOutput;

        impl Drop for DropOutput {
            fn drop(&mut self) {}
        }

        assert!(!std::mem::needs_drop::<JoinError>());

        let mut executor = Executor {
            owner: ringless_owner_for_test(1),
            process_quota: DEFAULT_PROCESS_QUOTA,
            cpu_affinity: None,
            #[cfg(debug_assertions)]
            last_stats: RuntimeStats::default(),
        };
        let owner = Rc::as_ptr(&executor.owner);
        let (plain, dropping) = {
            let _active = ExecutorCtxGuard::install(owner)
                .expect("ringless policy test context installation failed");
            (
                Executor::try_spawn(std::future::pending::<()>())
                    .expect("plain task admission failed"),
                Executor::try_spawn(std::future::pending::<DropOutput>())
                    .expect("drop-output task admission failed"),
            )
        };

        unsafe {
            assert!(!std::ptr::eq(
                (*plain.task_ptr).vtable,
                (*plain.task_ptr).iterative_vtable,
            ));
            assert!(std::ptr::eq(
                (*dropping.task_ptr).vtable,
                (*dropping.task_ptr).iterative_vtable,
            ));
        }

        executor.shutdown_owner();
        unsafe {
            assert!(std::ptr::eq(
                (*plain.task_ptr).vtable,
                (*plain.task_ptr).iterative_vtable,
            ));
            assert!(std::ptr::eq(
                (*dropping.task_ptr).vtable,
                (*dropping.task_ptr).iterative_vtable,
            ));
        }
        drop(plain);
        drop(dropping);
        unsafe {
            assert!((*executor.owner.state_ptr()).all_tasks.is_empty());
        }
    }

    #[test]
    fn shutdown_skips_reentrant_zero_ref_raw_task_without_rearming() {
        type Completed = std::future::Ready<usize>;

        let mut executor = Executor {
            owner: ringless_owner_for_test(1),
            process_quota: DEFAULT_PROCESS_QUOTA,
            cpu_affinity: None,
            #[cfg(debug_assertions)]
            last_stats: RuntimeStats::default(),
        };
        let owner = Rc::as_ptr(&executor.owner);
        let task = {
            let _active = ExecutorCtxGuard::install(owner)
                .expect("ringless zero-ref shutdown context installation failed");
            let mut staged = stage_completed_task_output_for_benchmark(29usize)
                .expect("zero-ref shutdown task staging failed");
            let task = staged.task;
            let raw = join_task_vtable_for::<Completed>().direct;
            unsafe {
                (*task).vtable = raw;
                (*task).refs.set(0);
            }
            staged.owns_reference = false;
            drop(staged);
            task
        };
        let raw_destroy = unsafe { (*task).vtable.destroy };
        ITERATIVE_TASK_DESTROY_ENTRIES.with(|entries| entries.set(0));

        executor.shutdown_owner();

        ITERATIVE_TASK_DESTROY_ENTRIES.with(|entries| assert_eq!(entries.get(), 0));
        unsafe {
            assert!((*task).all_link.is_unlinked());
            assert!((*task).ready_link.is_unlinked());
            raw_destroy(task);
            assert_staged_tasks_reclaimed(&executor.owner, 1, 1);
        }
    }

    #[test]
    fn iterative_task_destructor_can_reenter_its_executor_shutdown() {
        struct ReentrantShutdownOutput {
            executor: *mut Executor,
            calls: Rc<Cell<usize>>,
        }

        impl Drop for ReentrantShutdownOutput {
            fn drop(&mut self) {
                self.calls.set(self.calls.get() + 1);
                unsafe {
                    (&mut *self.executor).shutdown_owner();
                    let owner = Rc::as_ptr(&(*self.executor).owner);
                    let state = (*self.executor).owner.state_ptr();
                    assert_deferred_shutdown_drop_context(DeferredShutdownDropExpectation {
                        owner,
                        state,
                    });
                }
            }
        }

        let mut executor = Executor {
            owner: ringless_owner_for_test(1),
            process_quota: DEFAULT_PROCESS_QUOTA,
            cpu_affinity: None,
            #[cfg(debug_assertions)]
            last_stats: RuntimeStats::default(),
        };
        let executor_ptr = std::ptr::from_mut(&mut executor);
        let owner = Rc::as_ptr(&executor.owner);
        let calls = Rc::new(Cell::new(0));
        let staged = {
            let _active = ExecutorCtxGuard::install(owner)
                .expect("ringless reentrant-shutdown context installation failed");
            stage_completed_task_output_for_benchmark(ReentrantShutdownOutput {
                executor: executor_ptr,
                calls: Rc::clone(&calls),
            })
            .expect("reentrant-shutdown task staging failed")
        };

        drop(staged);

        assert_eq!(calls.get(), 1);
        let state = executor.owner.state_ptr();
        unsafe {
            assert!((*state).shutdown_complete);
            assert!((*state).deferred_shutdown_owner.is_none());
            assert!((*state).deferred_shutdown_next.is_null());
            assert!((*state).all_tasks.is_empty());
            assert!((*state).ready_queue.is_empty());
            #[cfg(debug_assertions)]
            assert_eq!((*state).runtime_state.stats.task_allocs, 1);
            #[cfg(debug_assertions)]
            assert_eq!((*state).runtime_state.stats.task_frees, 1);
        }
        ITERATIVE_TASK_DESTROY_QUEUE.with(|active| assert!(active.get().is_null()));
    }

    #[test]
    fn iterative_task_destroy_nonempty_fifo_survives_reentrant_shutdown() {
        struct ReentrantShutdownWithQueuedTask {
            executor: *mut Executor,
            outer_task: Rc<Cell<*mut TaskHeader>>,
            nested: [Option<SyntheticTaskRef>; 2],
            nested_tasks: [*mut TaskHeader; 2],
            calls: Rc<Cell<usize>>,
            nested_order: Rc<RefCell<Vec<usize>>>,
        }

        impl Drop for ReentrantShutdownWithQueuedTask {
            fn drop(&mut self) {
                self.calls.set(self.calls.get() + 1);
                let outer_task = self.outer_task.get();
                assert!(
                    !outer_task.is_null(),
                    "outer destroy task was not published"
                );
                drop(self.nested[0].take());
                drop(self.nested[1].take());

                let mut assert_two_member_fifo = || {
                    ITERATIVE_TASK_DESTROY_QUEUE.with(|active| {
                        let queue = active.get();
                        assert!(!queue.is_null(), "outer destroy FIFO was not registered");
                        let outer_link =
                            unsafe { std::ptr::addr_of_mut!((*outer_task).ready_link) };
                        let first_link =
                            unsafe { std::ptr::addr_of_mut!((*self.nested_tasks[0]).ready_link) };
                        let second_link =
                            unsafe { std::ptr::addr_of_mut!((*self.nested_tasks[1]).ready_link) };
                        assert_eq!(unsafe { (*queue).head }, first_link);
                        assert_eq!(unsafe { (*queue).tail }, second_link);
                        assert_ne!(unsafe { (*queue).head }, outer_link);
                        assert_ne!(unsafe { (*queue).tail }, outer_link);
                        assert!(unsafe { (*first_link).prev.is_null() });
                        assert_eq!(unsafe { (*first_link).next }, second_link);
                        assert_eq!(unsafe { (*second_link).prev }, first_link);
                        assert!(unsafe { (*second_link).next.is_null() });
                        assert!(unsafe { (*self.nested_tasks[0]).all_link.is_unlinked() });
                        assert!(unsafe { (*self.nested_tasks[1]).all_link.is_unlinked() });
                    });
                };

                assert_two_member_fifo();
                assert!(self.nested_order.borrow().is_empty());
                unsafe {
                    (&mut *self.executor).shutdown_owner();
                    let owner = Rc::as_ptr(&(*self.executor).owner);
                    let state = (*self.executor).owner.state_ptr();
                    assert_deferred_shutdown_drop_context(DeferredShutdownDropExpectation {
                        owner,
                        state,
                    });
                }
                assert!(
                    self.nested_order.borrow().is_empty(),
                    "reentrant shutdown destroyed active FIFO nodes"
                );
                assert_two_member_fifo();
            }
        }

        let mut executor = Executor {
            owner: ringless_owner_for_test(1),
            process_quota: DEFAULT_PROCESS_QUOTA,
            cpu_affinity: None,
            #[cfg(debug_assertions)]
            last_stats: RuntimeStats::default(),
        };
        let executor_ptr = std::ptr::from_mut(&mut executor);
        let owner = Rc::as_ptr(&executor.owner);
        let state = executor.owner.state_ptr();
        let owner_refs = Rc::strong_count(&executor.owner);
        let calls = Rc::new(Cell::new(0));
        let nested_order = Rc::new(RefCell::new(Vec::new()));
        let outer_task_slot = Rc::new(Cell::new(std::ptr::null_mut()));
        let nested_task_slots: [Rc<Cell<*mut TaskHeader>>; 2] =
            std::array::from_fn(|_| Rc::new(Cell::new(std::ptr::null_mut())));
        let empty_task_slot = Rc::new(Cell::new(std::ptr::null_mut()));
        let staged = {
            let _active = ExecutorCtxGuard::install(owner)
                .expect("nonempty reentrant-shutdown context installation failed");
            let mut first = stage_completed_task_output_for_benchmark(DestroyLinkDropProbe {
                task: Rc::clone(&nested_task_slots[0]),
                id: 0,
                order: Rc::clone(&nested_order),
                remaining_task: Rc::clone(&nested_task_slots[1]),
                shutdown_context: Some(DeferredShutdownDropExpectation { owner, state }),
            })
            .expect("first reentrant-shutdown nested task staging failed");
            nested_task_slots[0].set(first.task);
            let first_ref = SyntheticTaskRef { task: first.task };
            first.owns_reference = false;
            drop(first);

            let mut second = stage_completed_task_output_for_benchmark(DestroyLinkDropProbe {
                task: Rc::clone(&nested_task_slots[1]),
                id: 1,
                order: Rc::clone(&nested_order),
                remaining_task: Rc::clone(&empty_task_slot),
                shutdown_context: Some(DeferredShutdownDropExpectation { owner, state }),
            })
            .expect("second reentrant-shutdown nested task staging failed");
            nested_task_slots[1].set(second.task);
            let second_ref = SyntheticTaskRef { task: second.task };
            second.owns_reference = false;
            drop(second);

            stage_completed_task_output_for_benchmark(ReentrantShutdownWithQueuedTask {
                executor: executor_ptr,
                outer_task: Rc::clone(&outer_task_slot),
                nested: [Some(first_ref), Some(second_ref)],
                nested_tasks: [nested_task_slots[0].get(), nested_task_slots[1].get()],
                calls: Rc::clone(&calls),
                nested_order: Rc::clone(&nested_order),
            })
            .expect("nonempty reentrant-shutdown outer task staging failed")
        };
        outer_task_slot.set(staged.task);

        drop(staged);

        assert_eq!(calls.get(), 1);
        assert_eq!(nested_order.borrow().len(), 2);
        assert_eq!(*nested_order.borrow(), vec![0, 1]);
        assert_eq!(Rc::strong_count(&executor.owner), owner_refs);
        unsafe {
            assert!((*state).shutdown_complete);
            assert!((*state).deferred_shutdown_owner.is_none());
            assert!((*state).deferred_shutdown_next.is_null());
            assert!((*state).all_tasks.is_empty());
            assert!((*state).ready_queue.is_empty());
            #[cfg(debug_assertions)]
            assert_eq!((*state).runtime_state.stats.task_allocs, 3);
            #[cfg(debug_assertions)]
            assert_eq!((*state).runtime_state.stats.task_frees, 3);
        }
        ITERATIVE_TASK_DESTROY_QUEUE.with(|active| assert!(active.get().is_null()));
    }

    #[test]
    fn reentrant_shutdown_cancellation_drains_completed_task_under_owner_context() {
        struct CancelReleasesCompletedTask {
            completed: Option<SyntheticTaskRef>,
            drops: Rc<Cell<usize>>,
        }

        impl Future for CancelReleasesCompletedTask {
            type Output = ();

            fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
                Poll::Pending
            }
        }

        impl Drop for CancelReleasesCompletedTask {
            fn drop(&mut self) {
                self.drops.set(self.drops.get() + 1);
                drop(self.completed.take());
            }
        }

        struct ShutdownDuringOuterDestroy {
            executor: *mut Executor,
            completed_task: *mut TaskHeader,
            cancelled_task: *mut TaskHeader,
            output_order: Rc<RefCell<Vec<usize>>>,
        }

        impl Drop for ShutdownDuringOuterDestroy {
            fn drop(&mut self) {
                ITERATIVE_TASK_DESTROY_QUEUE.with(|active| {
                    let queue = active.get();
                    assert!(!queue.is_null(), "outer destroy FIFO was not registered");
                    unsafe {
                        assert!((*queue).head.is_null());
                        assert!((*queue).tail.is_null());
                    }
                });

                unsafe {
                    (&mut *self.executor).shutdown_owner();
                    let owner = Rc::as_ptr(&(*self.executor).owner);
                    let state = (*self.executor).owner.state_ptr();
                    assert_deferred_shutdown_drop_context(DeferredShutdownDropExpectation {
                        owner,
                        state,
                    });
                }
                assert!(
                    self.output_order.borrow().is_empty(),
                    "shutdown consumed a task from the active outer FIFO"
                );

                ITERATIVE_TASK_DESTROY_QUEUE.with(|active| {
                    let queue = active.get();
                    let completed_link =
                        unsafe { std::ptr::addr_of_mut!((*self.completed_task).ready_link) };
                    let cancelled_link =
                        unsafe { std::ptr::addr_of_mut!((*self.cancelled_task).ready_link) };
                    unsafe {
                        assert_eq!((*queue).head, completed_link);
                        assert_eq!((*queue).tail, cancelled_link);
                        assert!((*completed_link).prev.is_null());
                        assert_eq!((*completed_link).next, cancelled_link);
                        assert_eq!((*cancelled_link).prev, completed_link);
                        assert!((*cancelled_link).next.is_null());
                        assert!((*self.completed_task).all_link.is_unlinked());
                        assert!((*self.cancelled_task).all_link.is_unlinked());
                    }
                });
            }
        }

        let mut executor = Executor {
            owner: ringless_owner_for_test(1),
            process_quota: DEFAULT_PROCESS_QUOTA,
            cpu_affinity: None,
            #[cfg(debug_assertions)]
            last_stats: RuntimeStats::default(),
        };
        let executor_ptr = std::ptr::from_mut(&mut executor);
        let owner = Rc::as_ptr(&executor.owner);
        let state = executor.owner.state_ptr();
        let owner_refs = Rc::strong_count(&executor.owner);
        let cancellation_drops = Rc::new(Cell::new(0));
        let output_order = Rc::new(RefCell::new(Vec::new()));
        let completed_task_slot = Rc::new(Cell::new(std::ptr::null_mut()));
        let cancelled_task_slot = Rc::new(Cell::new(std::ptr::null_mut()));

        let outer = {
            let _active = ExecutorCtxGuard::install(owner)
                .expect("cancellation FIFO test context installation failed");
            let mut completed = stage_completed_task_output_for_benchmark(DestroyLinkDropProbe {
                task: Rc::clone(&completed_task_slot),
                id: 0,
                order: Rc::clone(&output_order),
                remaining_task: Rc::clone(&cancelled_task_slot),
                shutdown_context: Some(DeferredShutdownDropExpectation { owner, state }),
            })
            .expect("shutdown-owned completed task staging failed");
            completed_task_slot.set(completed.task);
            let completed_ref = SyntheticTaskRef {
                task: completed.task,
            };
            completed.owns_reference = false;
            drop(completed);

            let cancelled = Executor::try_spawn(CancelReleasesCompletedTask {
                completed: Some(completed_ref),
                drops: Rc::clone(&cancellation_drops),
            })
            .expect("shutdown-owned pending task admission failed");
            cancelled_task_slot.set(cancelled.task_ptr);
            drop(cancelled);

            stage_completed_task_output_for_benchmark(ShutdownDuringOuterDestroy {
                executor: executor_ptr,
                completed_task: completed_task_slot.get(),
                cancelled_task: cancelled_task_slot.get(),
                output_order: Rc::clone(&output_order),
            })
            .expect("outer shutdown trigger staging failed")
        };

        drop(outer);

        assert_eq!(cancellation_drops.get(), 1);
        assert_eq!(*output_order.borrow(), vec![0]);
        assert_eq!(Rc::strong_count(&executor.owner), owner_refs);
        unsafe {
            assert!((*state).shutdown_complete);
            assert!((*state).deferred_shutdown_owner.is_none());
            assert!((*state).deferred_shutdown_next.is_null());
            assert!((*state).all_tasks.is_empty());
            assert!((*state).ready_queue.is_empty());
            #[cfg(debug_assertions)]
            assert_eq!((*state).runtime_state.stats.task_allocs, 3);
            #[cfg(debug_assertions)]
            assert_eq!((*state).runtime_state.stats.task_frees, 3);
        }
        EXECUTOR_CTX.with(|context| assert!(context.get().active_owner.is_null()));
        ITERATIVE_TASK_DESTROY_QUEUE.with(|active| assert!(active.get().is_null()));
    }

    #[test]
    fn deferred_shutdown_switches_exact_owner_and_restores_prior_context() {
        struct CrossExecutorTail {
            expected: DeferredShutdownDropExpectation,
            drops: Rc<Cell<usize>>,
        }

        impl Drop for CrossExecutorTail {
            fn drop(&mut self) {
                assert_deferred_shutdown_drop_context(self.expected);
                self.drops.set(self.drops.get() + 1);
            }
        }

        struct CrossExecutorShutdownTrigger {
            owner_executor: *mut Executor,
            foreign_executor: *mut Executor,
            tail: Option<SyntheticTaskRef>,
            calls: Rc<Cell<usize>>,
        }

        impl Drop for CrossExecutorShutdownTrigger {
            fn drop(&mut self) {
                self.calls.set(self.calls.get() + 1);
                drop(self.tail.take());
                unsafe {
                    (&mut *self.foreign_executor).shutdown_owner();
                    let callback_owner = Rc::as_ptr(&(*self.owner_executor).owner);
                    EXECUTOR_CTX.with(|context| {
                        assert_eq!(context.get().active_owner, callback_owner);
                    });
                    let foreign_owner = Rc::as_ptr(&(*self.foreign_executor).owner);
                    let foreign_state = (*self.foreign_executor).owner.state_ptr();
                    assert!((*foreign_state).shutting_down);
                    assert!(!(*foreign_state).shutdown_complete);
                    assert_eq!(
                        (*foreign_state)
                            .deferred_shutdown_owner
                            .as_ref()
                            .map(Rc::as_ptr),
                        Some(foreign_owner),
                    );

                    (&mut *self.owner_executor).shutdown_owner();
                    let owner_state = (*self.owner_executor).owner.state_ptr();
                    assert_deferred_shutdown_drop_context(DeferredShutdownDropExpectation {
                        owner: callback_owner,
                        state: owner_state,
                    });
                    assert!(!(*foreign_state).shutdown_complete);
                }
            }
        }

        let mut owner_executor = Executor {
            owner: ringless_owner_for_test(1),
            process_quota: DEFAULT_PROCESS_QUOTA,
            cpu_affinity: None,
            #[cfg(debug_assertions)]
            last_stats: RuntimeStats::default(),
        };
        let mut foreign_executor = Executor {
            owner: ringless_owner_for_test(1),
            process_quota: DEFAULT_PROCESS_QUOTA,
            cpu_affinity: None,
            #[cfg(debug_assertions)]
            last_stats: RuntimeStats::default(),
        };
        let prior_owner = ringless_owner_for_test(1);
        let owner_ptr = Rc::as_ptr(&owner_executor.owner);
        let owner_state = owner_executor.owner.state_ptr();
        let foreign_state = foreign_executor.owner.state_ptr();
        let owner_refs = Rc::strong_count(&owner_executor.owner);
        let foreign_refs = Rc::strong_count(&foreign_executor.owner);
        let tail_drops = Rc::new(Cell::new(0));
        let trigger_calls = Rc::new(Cell::new(0));

        let outer = {
            let _active = ExecutorCtxGuard::install(owner_ptr)
                .expect("cross-executor staging context installation failed");
            let mut tail = stage_completed_task_output_for_benchmark(CrossExecutorTail {
                expected: DeferredShutdownDropExpectation {
                    owner: owner_ptr,
                    state: owner_state,
                },
                drops: Rc::clone(&tail_drops),
            })
            .expect("cross-executor tail staging failed");
            let tail_ref = SyntheticTaskRef { task: tail.task };
            tail.owns_reference = false;
            drop(tail);

            stage_completed_task_output_for_benchmark(CrossExecutorShutdownTrigger {
                owner_executor: std::ptr::from_mut(&mut owner_executor),
                foreign_executor: std::ptr::from_mut(&mut foreign_executor),
                tail: Some(tail_ref),
                calls: Rc::clone(&trigger_calls),
            })
            .expect("cross-executor shutdown trigger staging failed")
        };

        {
            let prior_ptr = Rc::as_ptr(&prior_owner);
            let _prior = ExecutorCtxGuard::install(prior_ptr)
                .expect("prior executor context installation failed");
            drop(outer);
            EXECUTOR_CTX.with(|context| {
                assert_eq!(context.get().active_owner, prior_ptr);
            });
        }

        assert_eq!(trigger_calls.get(), 1);
        assert_eq!(tail_drops.get(), 1);
        assert_eq!(Rc::strong_count(&owner_executor.owner), owner_refs);
        assert_eq!(Rc::strong_count(&foreign_executor.owner), foreign_refs);
        unsafe {
            for state in [owner_state, foreign_state] {
                assert!((*state).shutdown_complete);
                assert!((*state).deferred_shutdown_owner.is_none());
                assert!((*state).deferred_shutdown_next.is_null());
                assert!((*state).all_tasks.is_empty());
                assert!((*state).ready_queue.is_empty());
            }
            #[cfg(debug_assertions)]
            assert_eq!((*owner_state).runtime_state.stats.task_allocs, 2);
            #[cfg(debug_assertions)]
            assert_eq!((*owner_state).runtime_state.stats.task_frees, 2);
        }
        EXECUTOR_CTX.with(|context| assert!(context.get().active_owner.is_null()));
        ITERATIVE_TASK_DESTROY_QUEUE.with(|active| assert!(active.get().is_null()));
    }

    #[test]
    fn deferred_shutdown_finishes_and_restores_context_before_resuming_output_panic() {
        #[derive(Debug)]
        struct DeferredOutputPanic;

        struct PanickingDeferredOutput {
            expected: DeferredShutdownDropExpectation,
            drops: Rc<Cell<usize>>,
        }

        impl Drop for PanickingDeferredOutput {
            fn drop(&mut self) {
                assert_deferred_shutdown_drop_context(self.expected);
                self.drops.set(self.drops.get() + 1);
                std::panic::panic_any(DeferredOutputPanic);
            }
        }

        let mut executor = Executor {
            owner: ringless_owner_for_test(1),
            process_quota: DEFAULT_PROCESS_QUOTA,
            cpu_affinity: None,
            #[cfg(debug_assertions)]
            last_stats: RuntimeStats::default(),
        };
        let prior_owner = ringless_owner_for_test(1);
        let owner = Rc::as_ptr(&executor.owner);
        let state = executor.owner.state_ptr();
        let owner_refs = Rc::strong_count(&executor.owner);
        let output_drops = Rc::new(Cell::new(0));
        let trigger_calls = Rc::new(Cell::new(0));

        let outer = {
            let _active = ExecutorCtxGuard::install(owner)
                .expect("deferred panic staging context installation failed");
            let mut nested = stage_completed_task_output_for_benchmark(PanickingDeferredOutput {
                expected: DeferredShutdownDropExpectation { owner, state },
                drops: Rc::clone(&output_drops),
            })
            .expect("panicking deferred output staging failed");
            let nested_ref = SyntheticTaskRef { task: nested.task };
            nested.owns_reference = false;
            drop(nested);

            stage_completed_task_output_for_benchmark(DeferredShutdownTrigger {
                executor: std::ptr::from_mut(&mut executor),
                nested: Some(nested_ref),
                calls: Rc::clone(&trigger_calls),
            })
            .expect("deferred panic trigger staging failed")
        };

        let unwind = {
            let prior_ptr = Rc::as_ptr(&prior_owner);
            let _prior = ExecutorCtxGuard::install(prior_ptr)
                .expect("deferred panic prior context installation failed");
            let result = catch_unwind(AssertUnwindSafe(|| drop(outer)));
            EXECUTOR_CTX.with(|context| {
                assert_eq!(context.get().active_owner, prior_ptr);
            });
            result.expect_err("queued output destructor did not panic")
        };

        assert!(unwind.is::<DeferredOutputPanic>());
        assert_eq!(trigger_calls.get(), 1);
        assert_eq!(output_drops.get(), 1);
        assert_eq!(Rc::strong_count(&executor.owner), owner_refs);
        unsafe {
            assert!((*state).shutdown_complete);
            assert!((*state).deferred_shutdown_owner.is_none());
            assert!((*state).deferred_shutdown_next.is_null());
            assert!((*state).all_tasks.is_empty());
            assert!((*state).ready_queue.is_empty());
            #[cfg(debug_assertions)]
            assert_eq!((*state).runtime_state.stats.task_allocs, 2);
            #[cfg(debug_assertions)]
            assert_eq!((*state).runtime_state.stats.task_frees, 2);
        }
        EXECUTOR_CTX.with(|context| assert!(context.get().active_owner.is_null()));
        ITERATIVE_TASK_DESTROY_QUEUE.with(|active| assert!(active.get().is_null()));
    }

    #[test]
    fn staged_no_drop_output_starts_entry_entry() {
        with_ringless_poll_context_for_test(1, |owner, _cx| {
            let staged = stage_completed_task_output_for_benchmark(17usize)
                .expect("completed task staging failed");
            unsafe {
                assert!(std::ptr::eq(
                    (*staged.task).vtable,
                    (*staged.task).iterative_vtable,
                ));
            }
            drop(staged);
            unsafe {
                assert_staged_tasks_reclaimed(owner, 1, 1);
            }
        });
    }

    #[cfg(not(miri))]
    #[test]
    fn completed_unpolled_unit_handle_uses_direct_destroy_and_reuses_slot() {
        let handle_slot = Rc::new(RefCell::new(None::<JoinHandle<()>>));
        let mut executor = Executor::new().expect("executor construction failed");
        executor
            .run({
                let handle_slot = Rc::clone(&handle_slot);
                async move {
                    let handle =
                        Executor::spawn(async {}).expect("callback-free unit task spawn failed");
                    *handle_slot.borrow_mut() = Some(handle);
                }
            })
            .expect("callback-free unit task did not complete");

        let handle = handle_slot
            .borrow_mut()
            .take()
            .expect("callback-free unit handle disappeared");
        let task = handle.task_ptr;
        unsafe {
            assert_eq!((*task).refs.get(), 1);
            assert!(!std::ptr::eq((*task).vtable, (*task).iterative_vtable,));
            assert_eq!((*task).flags.get(), TaskHeader::FLAG_COMPLETED);
            assert!((*task).ready_link.is_unlinked());
        }
        ITERATIVE_TASK_DESTROY_ENTRIES.with(|entries| entries.set(0));

        drop(handle);

        ITERATIVE_TASK_DESTROY_ENTRIES.with(|entries| assert_eq!(entries.get(), 0));
        assert_destroyed_task_slot_is_reused(&mut executor, task);
    }

    #[test]
    fn iterative_task_destroy_synthetic_chain_is_fifo_and_depth_one() {
        with_ringless_poll_context_for_test(1, |owner, _cx| {
            let depth = if cfg!(miri) { 8 } else { 64 };
            let stats = Rc::new(SyntheticChainStats::default());
            let owner_refs = Rc::strong_count(owner);
            let (head, leaf) = staged_synthetic_chain(depth, &stats, 0);
            assert_eq!(unsafe { (*head.task).refs.get() }, 1);
            assert_eq!(Rc::strong_count(owner), owner_refs + depth);

            drop(head);

            assert_eq!(stats.max_depth.get(), 1);
            assert_eq!(*stats.order.borrow(), (0..depth).rev().collect::<Vec<_>>());
            assert_eq!(Rc::strong_count(owner), owner_refs);
            unsafe {
                assert_staged_tasks_reclaimed(owner, depth, depth);
            }

            let replacement = stage_completed_task_output_for_benchmark(23usize)
                .expect("replacement task staging failed");
            assert_eq!(
                replacement.task, leaf,
                "iterative drain did not return the tail slot for immediate reuse"
            );
            drop(replacement);
            unsafe {
                assert_staged_tasks_reclaimed(owner, depth + 1, depth + 1);
            }
        });
    }

    #[test]
    fn iterative_destroy_queue_singleton_uses_head_tail_ownership() {
        let mut queue = IterativeTaskDestroyQueue::new(std::ptr::null());
        let mut task = TaskHeader::new();
        task.refs.set(0);
        task.flags.set(TaskHeader::FLAG_COMPLETED);
        let task = std::ptr::from_mut(&mut task);

        unsafe { queue.push_back(task) };

        let link = unsafe { std::ptr::addr_of_mut!((*task).ready_link) };
        assert_eq!(queue.head, link);
        assert_eq!(queue.tail, link);
        assert!(unsafe { (*link).is_unlinked() });
        assert_eq!(unsafe { queue.pop_front() }, Some(task));
        assert!(queue.head.is_null());
        assert!(queue.tail.is_null());
        assert!(unsafe { (*task).ready_link.is_unlinked() });
        assert_eq!(unsafe { queue.pop_front() }, None);
    }

    #[test]
    fn iterative_destroy_queue_three_nodes_preserve_fifo_and_empty_state() {
        let mut queue = IterativeTaskDestroyQueue::new(std::ptr::null());
        let mut tasks: [TaskHeader; 3] = std::array::from_fn(|_| {
            let task = TaskHeader::new();
            task.refs.set(0);
            task.flags.set(TaskHeader::FLAG_COMPLETED);
            task
        });
        let task_ptrs = tasks.each_mut().map(std::ptr::from_mut);

        for task in task_ptrs {
            unsafe { queue.push_back(task) };
            assert!(unsafe { (*task).all_link.is_unlinked() });
        }

        assert!(unsafe { (*task_ptrs[0]).ready_link.prev.is_null() });
        assert_eq!(unsafe { (*task_ptrs[0]).ready_link.next }, unsafe {
            std::ptr::addr_of_mut!((*task_ptrs[1]).ready_link)
        });
        assert_eq!(unsafe { (*task_ptrs[1]).ready_link.prev }, unsafe {
            std::ptr::addr_of_mut!((*task_ptrs[0]).ready_link)
        });
        assert_eq!(unsafe { (*task_ptrs[1]).ready_link.next }, unsafe {
            std::ptr::addr_of_mut!((*task_ptrs[2]).ready_link)
        });
        assert_eq!(unsafe { (*task_ptrs[2]).ready_link.prev }, unsafe {
            std::ptr::addr_of_mut!((*task_ptrs[1]).ready_link)
        });
        assert_eq!(
            unsafe { (*task_ptrs[2]).ready_link.next },
            std::ptr::null_mut()
        );

        for (index, task) in task_ptrs.into_iter().enumerate() {
            let link = unsafe { std::ptr::addr_of_mut!((*task).ready_link) };
            assert_eq!(queue.head, link);
            assert!(unsafe { (*link).prev.is_null() });
            assert_eq!(unsafe { queue.pop_front() }, Some(task));
            assert!(unsafe { (*task).ready_link.is_unlinked() });
            if let Some(next) = task_ptrs.get(index + 1) {
                let next_link = unsafe { std::ptr::addr_of_mut!((**next).ready_link) };
                assert_eq!(queue.head, next_link);
                assert!(unsafe { (*next_link).prev.is_null() });
            }
        }
        assert!(queue.head.is_null());
        assert!(queue.tail.is_null());
        assert_eq!(unsafe { queue.pop_front() }, None);
    }

    #[test]
    fn iterative_destroy_detaches_registry_and_clears_links_before_raw_destroy() {
        type Completed = std::future::Ready<DestroyLinkDropProbe>;

        with_ringless_poll_context_for_test(1, |owner, _cx| {
            let task_slot = Rc::new(Cell::new(std::ptr::null_mut()));
            let order = Rc::new(RefCell::new(Vec::new()));
            let empty_task_slot = Rc::new(Cell::new(std::ptr::null_mut()));
            let mut staged = stage_completed_task_output_for_benchmark(DestroyLinkDropProbe {
                task: Rc::clone(&task_slot),
                id: 0,
                order: Rc::clone(&order),
                remaining_task: Rc::clone(&empty_task_slot),
                shutdown_context: None,
            })
            .expect("destroy-link task staging failed");
            let task = staged.task;
            task_slot.set(task);
            let raw_vtable = join_task_vtable_for::<Completed>().direct;
            unsafe {
                (*task).vtable = raw_vtable;
                (*task).refs.set(0);
            }
            staged.owns_reference = false;
            drop(staged);

            let state = owner.state_ptr();
            let initial_active_owner = EXECUTOR_CTX.with(|context| context.get().active_owner);
            let mut queue = IterativeTaskDestroyQueue::new(initial_active_owner);
            let queue_ptr = std::ptr::from_mut(&mut queue);
            ITERATIVE_TASK_DESTROY_QUEUE.with(|active| {
                assert!(active.get().is_null());
                active.set(queue_ptr);
                let registration = IterativeTaskDestroyRegistration {
                    active,
                    initial_active_owner: unsafe { (*queue_ptr).initial_active_owner },
                };

                unsafe {
                    enqueue_nested_task_destroy(queue_ptr, task, raw_vtable);
                    assert!((*state).all_tasks.is_empty());
                    assert!((*state).ready_queue.is_empty());
                    assert!((*task).all_link.is_unlinked());
                    let ready_link = std::ptr::addr_of_mut!((*task).ready_link);
                    assert_eq!((*queue_ptr).head, ready_link);
                    assert_eq!((*queue_ptr).tail, ready_link);
                    // The singleton's null/null link is shape evidence only;
                    // active head/tail identity above proves membership.
                    assert!((*ready_link).is_unlinked());
                    assert!(!task_can_enter_ready_queue((*task).flags.get()));
                    set_task_flag_unchecked(task, TaskHeader::FLAG_NOTIFIED);
                    assert!(!enqueue_notified_task_unchecked(
                        task,
                        std::ptr::addr_of_mut!((*state).ready_queue),
                        std::ptr::addr_of_mut!((*state).runtime_state),
                    ));
                    assert!((*state).ready_queue.is_empty());
                    assert_eq!((*queue_ptr).pop_front(), Some(task));
                    assert!((*queue_ptr).head.is_null());
                    assert!((*queue_ptr).tail.is_null());
                    assert!((*task).ready_link.is_unlinked());
                    let raw_destroy = raw_vtable.destroy;
                    raw_destroy(task);
                }

                drop(registration);
                assert!(active.get().is_null());
            });
            assert_eq!(order.borrow().len(), 1);
            assert_eq!(*order.borrow(), vec![0]);
            ITERATIVE_TASK_DESTROY_QUEUE.with(|active| assert!(active.get().is_null()));
            unsafe {
                assert_staged_tasks_reclaimed(owner, 1, 1);
            }

            let replacement = stage_completed_task_output_for_benchmark(31usize)
                .expect("destroy-link replacement staging failed");
            assert_eq!(replacement.task, task, "destroyed slot was not reused");
            drop(replacement);
            unsafe {
                assert_staged_tasks_reclaimed(owner, 2, 2);
            }
        });
    }

    #[test]
    fn iterative_task_destroy_branch_is_true_fifo() {
        with_ringless_poll_context_for_test(1, |owner, _cx| {
            let stats = Rc::new(SyntheticChainStats::default());
            let grandchild = stage_synthetic_branch(3, [None, None], &stats);
            let first = stage_synthetic_branch(1, [Some(grandchild), None], &stats);
            let second = stage_synthetic_branch(2, [None, None], &stats);
            let root = stage_synthetic_branch(0, [Some(first), Some(second)], &stats);

            drop(root);

            assert_eq!(stats.max_depth.get(), 1);
            assert_eq!(*stats.order.borrow(), vec![0, 1, 2, 3]);
            unsafe {
                assert_staged_tasks_reclaimed(owner, 4, 4);
            }
        });
    }

    #[test]
    fn iterative_task_destroy_drains_every_panic_position_and_clears_tls() {
        const DEPTH: usize = 8;
        let cases = [
            1_u64 << (DEPTH - 1),
            1_u64 << (DEPTH / 2),
            1,
            (1_u64 << (DEPTH - 1)) | (1_u64 << (DEPTH / 2)) | 1,
        ];

        with_ringless_poll_context_for_test(1, |owner, _cx| {
            let mut expected_total = 0;
            for panic_mask in cases {
                let stats = Rc::new(SyntheticChainStats::default());
                let (head, _) = staged_synthetic_chain(DEPTH, &stats, panic_mask);
                expected_total += DEPTH;
                let panic = catch_unwind(AssertUnwindSafe(|| drop(head)))
                    .expect_err("synthetic chain panic was not propagated");
                let expected_first = (0..DEPTH)
                    .rev()
                    .find(|id| (panic_mask & (1_u64 << id)) != 0)
                    .expect("panic case has no selected node");
                assert_eq!(
                    panic.downcast_ref::<SyntheticChainPanic>(),
                    Some(&SyntheticChainPanic(expected_first))
                );
                assert_eq!(stats.max_depth.get(), 1);
                assert_eq!(*stats.order.borrow(), (0..DEPTH).rev().collect::<Vec<_>>());
                unsafe {
                    assert_staged_tasks_reclaimed(owner, expected_total, expected_total);
                }
            }

            let clean_stats = Rc::new(SyntheticChainStats::default());
            let (clean_head, _) = staged_synthetic_chain(4, &clean_stats, 0);
            expected_total += 4;
            drop(clean_head);
            assert_eq!(clean_stats.max_depth.get(), 1);
            assert_eq!(*clean_stats.order.borrow(), vec![3, 2, 1, 0]);
            ITERATIVE_TASK_DESTROY_QUEUE.with(|active| assert!(active.get().is_null()));
            unsafe {
                assert_staged_tasks_reclaimed(owner, expected_total, expected_total);
            }
        });
    }

    #[cfg(not(miri))]
    #[test]
    fn iterative_task_destroy_preserves_an_active_outer_unwind() {
        struct DropSyntheticHead(Option<SyntheticTaskRef>);

        impl Drop for DropSyntheticHead {
            fn drop(&mut self) {
                drop(self.0.take());
            }
        }

        with_ringless_poll_context_for_test(1, |owner, _cx| {
            let stats = Rc::new(SyntheticChainStats::default());
            let (head, _) = staged_synthetic_chain(4, &stats, u64::MAX);
            let panic = catch_unwind(AssertUnwindSafe(|| {
                let _head = DropSyntheticHead(Some(head));
                std::panic::panic_any(SyntheticOuterPanic);
            }))
            .expect_err("outer synthetic unwind did not propagate");
            assert!(panic.is::<SyntheticOuterPanic>());
            assert_eq!(stats.max_depth.get(), 1);
            assert_eq!(*stats.order.borrow(), vec![3, 2, 1, 0]);
            ITERATIVE_TASK_DESTROY_QUEUE.with(|active| assert!(active.get().is_null()));
            unsafe {
                assert_staged_tasks_reclaimed(owner, 4, 4);
            }
        });
    }

    #[cfg(not(miri))]
    #[test]
    fn iterative_task_destroy_real_64_chain_survives_escaped_final_owner() {
        const DEPTH: usize = 64;
        let stats = Rc::new(SyntheticChainStats::default());
        let head = Rc::new(RefCell::new(None::<Box<JoinHandle<RealChainNode>>>));
        let leaf_task = Rc::new(Cell::new(std::ptr::null_mut()));
        let mut executor =
            ManuallyDrop::new(Executor::new().expect("executor construction failed"));
        let weak_owner = Rc::downgrade(&executor.owner);

        executor
            .run({
                let stats = Rc::clone(&stats);
                let head = Rc::clone(&head);
                let leaf_task = Rc::clone(&leaf_task);
                async move {
                    let mut next = None;
                    for id in 0..DEPTH {
                        let handle = Executor::spawn(std::future::ready(RealChainNode {
                            id,
                            next,
                            stats: Rc::clone(&stats),
                        }))
                        .expect("real chain task spawn failed");
                        if id == 0 {
                            leaf_task.set(handle.task_ptr);
                        }
                        next = Some(Box::new(handle));
                    }
                    *head.borrow_mut() = next;
                }
            })
            .expect("real chain tasks did not complete");

        let state = executor.owner.state_ptr();
        unsafe {
            assert_eq!((*state).runtime_state.live_tasks, 0);
            assert!((*state).ready_queue.is_empty());
            assert!(!(*state).all_tasks.is_empty());
        }

        let mut head = head.borrow_mut().take().expect("real chain head missing");
        let waker = Waker::noop();
        let mut cx = Context::from_waker(waker);
        const CLAIMED: usize = 4;
        for expected_id in ((DEPTH - CLAIMED)..DEPTH).rev() {
            let mut handle = *head;
            let mut node = match Pin::new(&mut handle).poll(&mut cx) {
                Poll::Ready(Ok(node)) => node,
                _ => panic!("claimed chain result was not ready"),
            };
            assert_eq!(node.id, expected_id);
            head = node.next.take().expect("claimed chain boundary missing");
            drop(node);
            drop(handle);
        }
        unsafe {
            ManuallyDrop::drop(&mut executor);
        }
        assert_eq!(
            weak_owner.strong_count(),
            DEPTH - CLAIMED,
            "each escaped completed task must retain one owner pin"
        );

        drop(head);

        assert_eq!(stats.max_depth.get(), 1);
        assert_eq!(*stats.order.borrow(), (0..DEPTH).rev().collect::<Vec<_>>());
        assert!(weak_owner.upgrade().is_none());
        assert!(!leaf_task.get().is_null());
        ITERATIVE_TASK_DESTROY_QUEUE.with(|active| assert!(active.get().is_null()));
    }

    #[test]
    fn iterative_task_destroy_drains_pending_cancellation_cross_executor_chain() {
        let mut inner = ManuallyDrop::new(Executor {
            owner: ringless_owner_for_test(1),
            process_quota: DEFAULT_PROCESS_QUOTA,
            cpu_affinity: None,
            #[cfg(debug_assertions)]
            last_stats: RuntimeStats::default(),
        });
        let inner_weak = Rc::downgrade(&inner.owner);
        let inner_owner = Rc::as_ptr(&inner.owner);
        let inner_handle = {
            let _active = ExecutorCtxGuard::install(inner_owner)
                .expect("inner cross-executor context installation failed");
            Executor::try_spawn(std::future::pending::<()>())
                .expect("pending inner task spawn failed")
        };
        let inner_task = inner_handle.task_ptr;
        let inner_result = inner_handle.result_ptr;

        let mut outer = ManuallyDrop::new(Executor {
            owner: ringless_owner_for_test(1),
            process_quota: DEFAULT_PROCESS_QUOTA,
            cpu_affinity: None,
            #[cfg(debug_assertions)]
            last_stats: RuntimeStats::default(),
        });
        let outer_weak = Rc::downgrade(&outer.owner);
        let owner = Rc::as_ptr(&outer.owner);
        let staged = {
            let _active = ExecutorCtxGuard::install(owner)
                .expect("outer cross-executor context installation failed");
            stage_completed_task_output_for_benchmark(inner_handle)
                .expect("outer cross-executor task staging failed")
        };

        inner.shutdown_owner();
        unsafe {
            assert_eq!((*inner_task).flags.get(), TaskHeader::FLAG_COMPLETED);
            assert_eq!((*inner_task).refs.get(), 1);
            assert!(std::ptr::eq(
                (*inner_task).vtable,
                (*inner_task).iterative_vtable,
            ));
            assert!(matches!(&*inner_result, Some(Err(JoinError::Cancelled))));
        }
        unsafe { ManuallyDrop::drop(&mut inner) };
        unsafe { ManuallyDrop::drop(&mut outer) };
        assert_eq!(inner_weak.strong_count(), 1);
        assert_eq!(outer_weak.strong_count(), 1);

        drop(staged);

        assert!(inner_weak.upgrade().is_none());
        assert!(outer_weak.upgrade().is_none());
        ITERATIVE_TASK_DESTROY_QUEUE.with(|active| assert!(active.get().is_null()));
    }

    #[test]
    fn staged_completed_task_waiter_transfer_releases_on_operation_free() {
        with_ringless_poll_context_for_test(1, |owner, _cx| {
            let drops = Rc::new(Cell::new(0));
            let reactor = owner.reactor_ptr();
            let state = unsafe { (&mut *reactor).alloc_op() };
            assert!(!state.is_null(), "operation allocation failed");
            let staged = stage_completed_task_output_for_benchmark(StagedTaskDropProbe::new(
                Rc::clone(&drops),
                false,
            ))
            .expect("completed task staging failed");
            let task = staged.task;

            let _output = unsafe { staged.transfer_to_waiter(state) };
            assert_eq!(unsafe { (*task).refs.get() }, 1);
            assert_eq!(drops.get(), 0);
            unsafe {
                Reactor::free_op_unchecked(reactor, state);
            }
            assert_eq!(drops.get(), 1);
            assert_eq!(unsafe { (&*reactor).live_op_count() }, 0);
            unsafe {
                assert_staged_tasks_reclaimed(owner, 1, 1);
            }

            let replacement = stage_completed_task_output_for_benchmark(StagedTaskDropProbe::new(
                Rc::clone(&drops),
                false,
            ))
            .expect("replacement completed task staging failed");
            assert_eq!(
                replacement.task, task,
                "waiter-released task slot was not exactly reusable"
            );
            drop(replacement);
            assert_eq!(drops.get(), 2);
            unsafe {
                assert_staged_tasks_reclaimed(owner, 2, 2);
            }
        });
    }

    #[test]
    fn staged_completed_task_panicking_output_still_returns_task_slot() {
        with_ringless_poll_context_for_test(1, |owner, _cx| {
            let drops = Rc::new(Cell::new(0));
            let staged = stage_completed_task_output_for_benchmark(StagedTaskDropProbe::new(
                Rc::clone(&drops),
                true,
            ))
            .expect("completed task staging failed");
            let task = staged.task;

            let unwind = catch_unwind(AssertUnwindSafe(|| drop(staged)))
                .expect_err("staged output destructor did not panic");
            assert!(
                unwind
                    .downcast_ref::<&'static str>()
                    .is_some_and(|message| *message == "staged task output drop panic")
            );
            assert_eq!(drops.get(), 1);
            unsafe {
                assert_staged_tasks_reclaimed(owner, 1, 1);
            }

            let replacement = stage_completed_task_output_for_benchmark(StagedTaskDropProbe::new(
                Rc::clone(&drops),
                false,
            ))
            .expect("replacement completed task staging failed");
            assert_eq!(
                replacement.task, task,
                "panic cleanup did not return the exact task slot"
            );
            drop(replacement);
            assert_eq!(drops.get(), 2);
            unsafe {
                assert_staged_tasks_reclaimed(owner, 2, 2);
            }
        });
    }

    #[test]
    fn shutdown_completed_ready_linked_task_drains_without_header_aliasing() {
        let mut executor = Executor {
            owner: ringless_owner_for_test(1),
            process_quota: DEFAULT_PROCESS_QUOTA,
            cpu_affinity: None,
            #[cfg(debug_assertions)]
            last_stats: RuntimeStats::default(),
        };
        let owner_ptr = Rc::as_ptr(&executor.owner);
        let drops = Rc::new(Cell::new(0));
        let staged = {
            let _active = ExecutorCtxGuard::install(owner_ptr)
                .expect("ringless shutdown test context installation failed");
            stage_completed_task_output_for_benchmark(StagedTaskDropProbe::new(
                Rc::clone(&drops),
                false,
            ))
            .expect("completed task staging failed")
        };
        let task = staged.task;
        let state = executor.owner.state_ptr();
        unsafe {
            (*task).flags.set(
                TaskHeader::FLAG_COMPLETED
                    | TaskHeader::FLAG_RUNNING
                    | TaskHeader::FLAG_NOTIFIED
                    | TaskHeader::FLAG_QUEUED,
            );
            (*state)
                .ready_queue
                .push_back_unchecked(std::ptr::addr_of_mut!((*task).ready_link));
        }

        executor.shutdown_owner();

        unsafe {
            assert!((*state).shutdown_complete);
            assert!((*state).ready_queue.is_empty());
            assert!((*state).all_tasks.is_empty());
            assert!((*task).ready_link.is_unlinked());
            assert_eq!((*task).flags.get(), TaskHeader::FLAG_COMPLETED);
        }
        assert_eq!(
            drops.get(),
            0,
            "escaped task output dropped during shutdown"
        );

        drop(staged);
        assert_eq!(drops.get(), 1);
        unsafe {
            assert_staged_tasks_reclaimed(&executor.owner, 1, 1);
        }
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

    #[test]
    fn executor_and_completion_drain_guards_restore_only_their_own_fields() {
        let active_owner = ringless_owner_for_test(2);
        let shutdown_owner = ringless_owner_for_test(2);
        let active_owner_ptr = Rc::as_ptr(&active_owner);
        let shutdown_owner_ptr = Rc::as_ptr(&shutdown_owner);
        let active_reactor = active_owner.reactor_ptr();
        let shutdown_reactor = shutdown_owner.reactor_ptr();
        let assert_context = |expected_owner: *const ExecutorOwner,
                              expected_drain_active: bool,
                              expected_drain_reactor: *mut Reactor| {
            EXECUTOR_CTX.with(|context| {
                let actual = context.get();
                assert_eq!(actual.active_owner, expected_owner);
                assert_eq!(
                    actual.completion_drain_active, expected_drain_active,
                    "completion-drain activity changed"
                );
                assert_eq!(
                    actual.completion_drain_reactor, expected_drain_reactor,
                    "completion-drain reactor changed"
                );
            });
        };

        assert_context(std::ptr::null(), false, std::ptr::null_mut());
        let active_guard = ExecutorCtxGuard::install(active_owner_ptr)
            .expect("active owner context installation failed");
        assert_context(active_owner_ptr, false, std::ptr::null_mut());

        // Ordinary LIFO nesting restores each exact reactor in turn.
        let outer_drain = CompletionDrainGuard::enter_for_reactor(active_reactor);
        assert_context(active_owner_ptr, true, active_reactor);
        let inner_drain = CompletionDrainGuard::enter_for_reactor(shutdown_reactor);
        assert_context(active_owner_ptr, true, shutdown_reactor);
        drop(inner_drain);
        assert_context(active_owner_ptr, true, active_reactor);
        drop(outer_drain);
        assert_context(active_owner_ptr, false, std::ptr::null_mut());

        // Interleave the independent guards deliberately: dropping either kind
        // must leave the other kind's current fields untouched.
        let drain_during_shutdown = CompletionDrainGuard::enter_for_reactor(active_reactor);
        let shutdown_guard = ExecutorCtxGuard::install_for_shutdown(shutdown_owner_ptr);
        assert_context(shutdown_owner_ptr, true, active_reactor);
        drop(drain_during_shutdown);
        assert_context(shutdown_owner_ptr, false, std::ptr::null_mut());

        let shutdown_drain = CompletionDrainGuard::enter_for_reactor(shutdown_reactor);
        drop(shutdown_guard);
        assert_context(active_owner_ptr, true, shutdown_reactor);
        drop(shutdown_drain);
        assert_context(active_owner_ptr, false, std::ptr::null_mut());

        drop(active_guard);
        assert_context(std::ptr::null(), false, std::ptr::null_mut());
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
    fn completion_drain_pending_nop_repoll_returns_real_completion() {
        let mut executor = Executor::new().expect("executor construction failed");
        executor
            .run(async {
                let mut nop = Nop::new();
                let mut submitted = false;
                let result = std::future::poll_fn(|cx| {
                    if !submitted {
                        assert!(
                            Pin::new(&mut nop).poll(cx).is_pending(),
                            "first NOP poll did not submit"
                        );
                        let drain = CompletionDrainGuard::enter();
                        assert!(
                            Pin::new(&mut nop).poll(cx).is_pending(),
                            "pending NOP did not park during completion drain"
                        );
                        drop(drain);
                        submitted = true;
                        return Poll::Pending;
                    }
                    Pin::new(&mut nop).poll(cx)
                })
                .await;
                assert_eq!(result.expect("NOP completion was rejected"), 0);
            })
            .expect("executor failed after transient NOP repoll");
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
    fn completion_drain_waiter_refresh_preserves_later_completion() {
        with_ringless_poll_context_for_test(1, |owner, cx| {
            let pctx = poll_ctx_from_waker(cx).expect("valid poll context was rejected");
            let reactor = owner.reactor_ptr();
            let state = unsafe { (&mut *reactor).alloc_op() };
            assert!(!state.is_null(), "operation allocation failed");

            unsafe {
                (*state).register_waiter(pctx.owner_task());
            }
            let flags = unsafe { (*state).state_flags };
            let waiter = unsafe { (*state).waiter };
            let waiter_refs = unsafe { (*waiter).refs.get() };

            let drain = CompletionDrainGuard::enter();
            assert!(
                unsafe { poll_ctx_or_transient_pending_op(cx, state) }
                    .expect("matching drain context was rejected")
                    .is_none(),
                "leading operation validation did not park during the drain"
            );
            assert!(
                !unsafe { refresh_op_waiter_from_waker(cx, state) },
                "transient completion drain reported ring abandonment"
            );
            unsafe {
                assert_eq!(
                    (*state).state_flags,
                    flags,
                    "completion drain changed operation flags"
                );
                assert_eq!(
                    (*state).waiter,
                    waiter,
                    "completion drain replaced the registered waiter"
                );
                assert_eq!(
                    (*waiter).refs.get(),
                    waiter_refs,
                    "completion drain changed waiter ownership"
                );
            }
            drop(drain);

            unsafe {
                (*state).result = 73;
                (*state).set_completed();
            }
            let completed = unsafe { completed_op_ctx(Some(pctx), state) };
            assert!(
                !completed.context_rejected(),
                "transient drain permanently rejected the operation"
            );
            assert_eq!(
                unsafe { (*state).result },
                73,
                "later completion result was not preserved"
            );
            unsafe {
                completed.free_op_unchecked(state);
            }
            drop(completed);

            assert_eq!(unsafe { (&*reactor).live_op_count() }, 0);
            let reused = unsafe { (&mut *reactor).alloc_op() };
            assert_eq!(reused, state, "completion state was not exactly reusable");
            unsafe {
                Reactor::free_op_unchecked(reactor, reused);
            }
        });
    }

    #[test]
    fn completion_drain_does_not_mask_permanent_context_rejection() {
        let mut state = CompletionState::empty();
        let cx = std::task::Context::from_waker(std::task::Waker::noop());
        let drain = CompletionDrainGuard::enter();

        assert!(!unsafe { refresh_op_waiter_from_waker(&cx, &mut state) });
        assert!(
            state.is_context_rejected(),
            "completion drain masked a non-FlowIO poll context"
        );
        drop(drain);
    }

    #[test]
    fn completion_drain_does_not_mask_foreign_operation_owner() {
        let origin = ringless_owner_for_test(1);
        let reactor = origin.reactor_ptr();
        let state = unsafe { (&mut *reactor).alloc_op() };
        assert!(!state.is_null(), "origin operation allocation failed");

        let mut origin_waiter = TaskHeader::new();
        origin_waiter.owner = Some(Rc::clone(&origin));
        let origin_waiter_ptr = std::ptr::addr_of_mut!(origin_waiter);
        unsafe {
            (*state).register_waiter(origin_waiter_ptr);
        }
        let waiter_refs = origin_waiter.refs.get();

        with_ringless_poll_context_for_test(1, |_foreign, foreign_cx| {
            let drain = CompletionDrainGuard::enter();
            let err = match unsafe { poll_ctx_or_transient_pending_op(foreign_cx, state) } {
                Err(err) => err,
                Ok(_) => panic!("foreign operation owner was treated as transient"),
            };
            assert_eq!(err.kind(), ErrorKind::NotConnected);
            assert!(!unsafe { refresh_op_waiter_from_waker(foreign_cx, state) });
            unsafe {
                assert!(
                    (*state).is_context_rejected(),
                    "completion drain masked the foreign operation owner"
                );
                assert_eq!(
                    (*state).waiter,
                    origin_waiter_ptr,
                    "foreign drain poll replaced the origin waiter"
                );
                assert_eq!(
                    origin_waiter.refs.get(),
                    waiter_refs,
                    "foreign drain poll changed origin waiter ownership"
                );
            }
            drop(drain);
        });

        unsafe {
            (*state).set_completed();
            Reactor::free_op_unchecked(reactor, state);
        }
        assert_eq!(
            origin_waiter.refs.get(),
            1,
            "origin waiter reference was not released"
        );
        assert_eq!(unsafe { (&*reactor).live_op_count() }, 0);
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
    struct TaskWakerClonePanic;

    unsafe fn panicking_clone_waker_clone(data: *const ()) -> RawWaker {
        let stats = unsafe { Rc::<CountedWakerStats>::from_raw(data.cast()) };
        stats.clones.set(stats.clones.get() + 1);
        let _ = Rc::into_raw(stats);
        std::panic::panic_any(TaskWakerClonePanic);
    }

    static PANICKING_CLONE_WAKER_VTABLE: RawWakerVTable = RawWakerVTable::new(
        panicking_clone_waker_clone,
        counted_waker_wake,
        counted_waker_wake_by_ref,
        counted_waker_drop,
    );

    fn panicking_clone_waker(stats: &Rc<CountedWakerStats>) -> Waker {
        let data = Rc::into_raw(Rc::clone(stats)).cast();
        unsafe { Waker::from_raw(RawWaker::new(data, &PANICKING_CLONE_WAKER_VTABLE)) }
    }

    unsafe fn replacement_drop_source_clone(data: *const ()) -> RawWaker {
        let stats = unsafe { Rc::<CountedWakerStats>::from_raw(data.cast()) };
        stats.clones.set(stats.clones.get() + 1);
        let cloned = Rc::clone(&stats);
        let _ = Rc::into_raw(stats);
        RawWaker::new(Rc::into_raw(cloned).cast(), &PANICKING_DROP_WAKER_VTABLE)
    }

    static REPLACEMENT_DROP_SOURCE_VTABLE: RawWakerVTable = RawWakerVTable::new(
        replacement_drop_source_clone,
        counted_waker_wake,
        counted_waker_wake_by_ref,
        counted_waker_drop,
    );

    fn replacement_drop_source_waker(stats: &Rc<CountedWakerStats>) -> Waker {
        let data = Rc::into_raw(Rc::clone(stats)).cast();
        unsafe { Waker::from_raw(RawWaker::new(data, &REPLACEMENT_DROP_SOURCE_VTABLE)) }
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

    #[test]
    fn run_after_completed_shutdown_returns_not_connected_without_polling_root() {
        struct TrackedRoot {
            polls: Rc<Cell<usize>>,
            drops: Rc<Cell<usize>>,
        }

        impl Future for TrackedRoot {
            type Output = ();

            fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
                self.polls.set(self.polls.get() + 1);
                Poll::Ready(())
            }
        }

        impl Drop for TrackedRoot {
            fn drop(&mut self) {
                self.drops.set(self.drops.get() + 1);
            }
        }

        let polls = Rc::new(Cell::new(0));
        let drops = Rc::new(Cell::new(0));
        let mut executor = Executor {
            owner: ringless_owner_for_test(1),
            process_quota: DEFAULT_PROCESS_QUOTA,
            cpu_affinity: None,
            #[cfg(debug_assertions)]
            last_stats: RuntimeStats::default(),
        };
        executor.shutdown_owner();

        let err = executor
            .run(TrackedRoot {
                polls: Rc::clone(&polls),
                drops: Rc::clone(&drops),
            })
            .expect_err("a completed executor shutdown must reject a later run");
        assert_eq!(err.kind(), ErrorKind::NotConnected);
        assert_eq!(polls.get(), 0, "rejected post-shutdown root was polled");
        assert_eq!(drops.get(), 1, "rejected post-shutdown root drop count");
    }

    #[cfg(all(debug_assertions, not(miri)))]
    #[test]
    fn last_stats_separates_generation_totals_from_latest_run_pool_deltas() {
        let mut executor = Executor::new().expect("executor construction failed");
        let release = Rc::new(Cell::new(false));
        let completed = Rc::new(Cell::new(false));
        let stalled_waker = Rc::new(RefCell::new(None::<Waker>));
        let scratch = Rc::new(RefCell::new(None));

        let first_result = executor.run({
            let release = Rc::clone(&release);
            let completed = Rc::clone(&completed);
            let stalled_waker = Rc::clone(&stalled_waker);
            let scratch = Rc::clone(&scratch);
            std::future::poll_fn(move |cx| {
                if release.get() {
                    completed.set(true);
                    return Poll::Ready(());
                }
                if scratch.borrow().is_none() {
                    let pctx = poll_ctx_from_waker(cx)
                        .expect("scratch stats probe lost its FlowIO context");
                    // SAFETY: the validated poll context identifies this
                    // currently polling task's live owner reactor; the borrow
                    // ends when the allocation call returns, and the returned
                    // scratch owns its pool handle independently.
                    let allocated = unsafe { (&mut *pctx.reactor()).alloc_iovec_scratch(17) }
                        .expect("scratch stats allocation failed");
                    *scratch.borrow_mut() = Some(allocated);
                }
                *stalled_waker.borrow_mut() = Some(cx.waker().clone());
                Poll::Pending
            })
        });
        assert_eq!(
            first_result
                .expect_err("parked stats task should stall the first run")
                .kind(),
            ErrorKind::WouldBlock
        );

        let first = executor.last_stats();
        assert_eq!(first.task_slab_allocs, 1);
        assert_eq!(first.task_allocs, 1);
        assert_eq!(first.task_frees, 0);
        assert_eq!(first.task_polls, 1);
        assert_eq!(first.task_schedules, 0);
        assert_eq!(first.poll_context_extractions, 1);
        assert_eq!(first.writev_scratch_pooled_allocs, 1);
        assert_eq!(first.writev_scratch_pooled_frees, 0);
        assert_eq!(first.writev_scratch_slab_allocs, 1);

        drop(
            scratch
                .borrow_mut()
                .take()
                .expect("first run did not retain its scratch allocation"),
        );
        // SAFETY: the executor and its heap-stable owner reactor remain live,
        // no run or reactor borrow is active, and this test stays on the owner
        // thread.
        let lifetime_pool_stats =
            unsafe { (&*executor.owner.reactor_ptr()).retained_payload_stats() };
        assert_eq!(lifetime_pool_stats.writev_scratch_pooled_allocs, 1);
        assert_eq!(lifetime_pool_stats.writev_scratch_pooled_frees, 1);

        release.set(true);
        stalled_waker
            .borrow_mut()
            .take()
            .expect("stalled task did not publish its waker")
            .wake();

        executor
            .run(async {})
            .expect("resumed stats generation did not drain");
        assert!(completed.get(), "stalled task did not resume");

        let resumed = executor.last_stats();
        assert_eq!(resumed.task_slab_allocs, 1);
        assert_eq!(resumed.task_allocs, 2);
        assert_eq!(resumed.task_frees, 2);
        assert_eq!(resumed.task_polls, 3);
        assert_eq!(resumed.task_schedules, 1);
        assert_eq!(resumed.poll_context_extractions, 1);
        assert_eq!(resumed.retained_pooled_allocs, 0);
        assert_eq!(resumed.retained_pooled_reuses, 0);
        assert_eq!(resumed.retained_pooled_frees, 0);
        assert_eq!(resumed.retained_slab_allocs, 0);
        assert_eq!(resumed.retained_heap_fallbacks, 0);
        assert_eq!(resumed.retained_heap_frees, 0);
        assert_eq!(resumed.writev_scratch_inline_allocs, 0);
        assert_eq!(resumed.writev_scratch_pooled_allocs, 0);
        assert_eq!(resumed.writev_scratch_pooled_reuses, 0);
        assert_eq!(resumed.writev_scratch_pooled_frees, 0);
        assert_eq!(resumed.writev_scratch_slab_allocs, 0);
        assert_eq!(resumed.writev_scratch_oversize_rejections, 0);
        assert_eq!(resumed.writev_scratch_alloc_failures, 0);
    }

    #[cfg(debug_assertions)]
    #[test]
    fn retained_pool_stats_map_every_field_to_runtime_stats() {
        let retained = RetainedPayloadPoolStats {
            pooled_allocs: 1,
            pooled_reuses: 2,
            pooled_frees: 3,
            slab_allocs: 4,
            heap_fallbacks: 5,
            heap_frees: 6,
            writev_scratch_inline_allocs: 7,
            writev_scratch_pooled_allocs: 8,
            writev_scratch_pooled_reuses: 9,
            writev_scratch_pooled_frees: 10,
            writev_scratch_slab_allocs: 11,
            writev_scratch_oversize_rejections: 12,
            writev_scratch_alloc_failures: 13,
        };
        let mut runtime = RuntimeStats::default();

        apply_retained_payload_stats(&mut runtime, retained);

        assert_eq!(
            [
                runtime.retained_pooled_allocs,
                runtime.retained_pooled_reuses,
                runtime.retained_pooled_frees,
                runtime.retained_slab_allocs,
                runtime.retained_heap_fallbacks,
                runtime.retained_heap_frees,
                runtime.writev_scratch_inline_allocs,
                runtime.writev_scratch_pooled_allocs,
                runtime.writev_scratch_pooled_reuses,
                runtime.writev_scratch_pooled_frees,
                runtime.writev_scratch_slab_allocs,
                runtime.writev_scratch_oversize_rejections,
                runtime.writev_scratch_alloc_failures,
            ],
            [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13]
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
            arm_task_destruction(task);
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
    fn deferred_shutdown_keeps_close_worker_until_queued_output_is_destroyed() {
        #[derive(Debug)]
        struct DeferredWorkerOutputPanic;

        struct DeferredWorkerOutput {
            fd: Option<RuntimeFd>,
            expected: DeferredShutdownDropExpectation,
            observed: Rc<Cell<bool>>,
        }

        impl Drop for DeferredWorkerOutput {
            fn drop(&mut self) {
                assert_deferred_shutdown_drop_context(self.expected);
                unsafe {
                    assert!((*self.expected.state).close_worker.sender.is_some());
                    assert!((*self.expected.state).close_worker.worker.is_some());
                }
                self.observed.set(true);
                drop(self.fd.take());
                std::panic::panic_any(DeferredWorkerOutputPanic);
            }
        }

        let mut executor = Executor::new().expect("executor construction failed");
        executor.init().expect("executor initialization failed");
        let owner = Rc::as_ptr(&executor.owner);
        let state = executor.owner.state_ptr();
        let owner_refs = Rc::strong_count(&executor.owner);
        let raw = distinctive_closeable_test_fd().expect("distinctive fd failed");
        set_positive_linger(raw, 1);
        let observed = Rc::new(Cell::new(false));
        let trigger_calls = Rc::new(Cell::new(0));

        let outer = {
            let _active = ExecutorCtxGuard::install(owner)
                .expect("deferred worker staging context installation failed");
            let mut nested = stage_completed_task_output_for_benchmark(DeferredWorkerOutput {
                // SAFETY: the test transfers its sole open descriptor owner.
                fd: Some(RuntimeFd::from_external_owned(unsafe {
                    OwnedFd::from_raw_fd(raw)
                })),
                expected: DeferredShutdownDropExpectation { owner, state },
                observed: Rc::clone(&observed),
            })
            .expect("deferred worker output staging failed");
            let nested_ref = SyntheticTaskRef { task: nested.task };
            nested.owns_reference = false;
            drop(nested);

            stage_completed_task_output_for_benchmark(DeferredShutdownTrigger {
                executor: std::ptr::from_mut(&mut executor),
                nested: Some(nested_ref),
                calls: Rc::clone(&trigger_calls),
            })
            .expect("deferred worker trigger staging failed")
        };

        let unwind = catch_unwind(AssertUnwindSafe(|| drop(outer)))
            .expect_err("queued worker output did not panic");

        assert!(unwind.is::<DeferredWorkerOutputPanic>());
        assert!(observed.get(), "queued worker output did not run");
        assert_eq!(trigger_calls.get(), 1);
        assert_eq!(Rc::strong_count(&executor.owner), owner_refs);
        unsafe {
            assert!((*state).close_worker.sender.is_none());
            assert!((*state).close_worker.worker.is_none());
            assert!((*state).shutdown_complete);
            assert!((*state).deferred_shutdown_owner.is_none());
            assert!((*state).deferred_shutdown_next.is_null());
            assert!((*state).all_tasks.is_empty());
            assert!((*state).ready_queue.is_empty());
            #[cfg(debug_assertions)]
            assert_eq!((*state).runtime_state.stats.close_linger_queries, 1);
            #[cfg(debug_assertions)]
            assert_eq!((*state).runtime_state.stats.close_worker_admissions, 1);
            #[cfg(debug_assertions)]
            assert_eq!((*state).runtime_state.stats.task_allocs, 2);
            #[cfg(debug_assertions)]
            assert_eq!((*state).runtime_state.stats.task_frees, 2);
        }
        assert!(
            raw_fd_is_closed(raw),
            "deferred shutdown returned before the worker joined"
        );
        EXECUTOR_CTX.with(|context| assert!(context.get().active_owner.is_null()));
        ITERATIVE_TASK_DESTROY_QUEUE.with(|active| assert!(active.get().is_null()));
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
                let active_owner = EXECUTOR_CTX.with(|context| context.get().active_owner);
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
    const DATA_FD_REUSE_CHILD_ENV: &str = "FLOWIO_DATA_FD_REUSE_CHILD";
    #[cfg(not(miri))]
    const DATA_FD_REUSE_CHILD_TEST: &str = "runtime::executor::tests::unflushed_data_read_same_poll_drop_blocks_fd_reuse_until_target_cqe";

    #[cfg(not(miri))]
    fn run_exact_unit_test_child_with_watchdog(test_name: &str, child_env: &str, label: &str) {
        use std::process::{Command, Stdio};

        let current_exe = std::env::current_exe().expect("current unit-test executable");
        let child = Command::new(current_exe)
            .args(["--exact", test_name, "--nocapture"])
            .env(child_env, "1")
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap_or_else(|err| panic!("spawn {label} child: {err}"));
        let output = crate::test_child::capture_child_with_watchdog(
            child,
            std::time::Duration::from_secs(8),
        )
        .unwrap_or_else(|err| panic!("{label} child capture failed: {err}"));
        assert!(
            output.status.success(),
            "{label} child failed: status={:?}, stdout={}, stderr={}",
            output.status,
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }

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
    fn descriptor_identity_for_test(fd: std::os::fd::RawFd) -> (libc::dev_t, libc::ino_t) {
        let mut stat = std::mem::MaybeUninit::<libc::stat>::uninit();
        // SAFETY: stat is writable for the exact fstat result and fd is only
        // observed, never consumed.
        let rc = unsafe { libc::fstat(fd, stat.as_mut_ptr()) };
        assert_eq!(
            rc,
            0,
            "fstat descriptor {fd} failed: {}",
            io::Error::last_os_error()
        );
        // SAFETY: successful fstat initialized the complete result.
        let stat = unsafe { stat.assume_init() };
        (stat.st_dev, stat.st_ino)
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

    #[cfg(all(any(debug_assertions, feature = "test-support"), not(miri)))]
    #[test]
    fn unflushed_data_read_same_poll_drop_blocks_fd_reuse_until_target_cqe() {
        if std::env::var_os(DATA_FD_REUSE_CHILD_ENV).is_none() {
            run_exact_unit_test_child_with_watchdog(
                DATA_FD_REUSE_CHILD_TEST,
                DATA_FD_REUSE_CHILD_ENV,
                "unflushed data-fd reuse",
            );
            return;
        }

        struct TargetCqeBuffer {
            bytes: Vec<u8>,
            fd: std::os::fd::RawFd,
            identity: (libc::dev_t, libc::ino_t),
            drops: Rc<Cell<usize>>,
            saw_original_fd: Rc<Cell<bool>>,
        }

        impl Drop for TargetCqeBuffer {
            fn drop(&mut self) {
                assert_eq!(
                    self.drops.replace(self.drops.get() + 1),
                    0,
                    "target-CQE payload dropped more than once"
                );
                assert_eq!(
                    descriptor_identity_for_test(self.fd),
                    self.identity,
                    "target-CQE payload drop did not retain the original descriptor"
                );
                self.saw_original_fd.set(true);
            }
        }

        // SAFETY: bytes owns pointer-stable writable capacity across moves.
        // The runtime publishes no more than writable_len() initialized bytes.
        unsafe impl IoBuffReadWrite for TargetCqeBuffer {
            fn as_mut_ptr(&mut self) -> *mut u8 {
                self.bytes.as_mut_ptr()
            }

            fn writable_len(&self) -> usize {
                self.bytes.capacity()
            }

            unsafe fn set_written_len(&mut self, len: usize) {
                assert!(len <= self.bytes.capacity());
                unsafe { self.bytes.set_len(len) };
            }
        }

        let mut executor = Executor::new().expect("executor construction failed");
        replace_with_disconnected_close_worker(&mut executor);

        // Isolated child execution makes Linux's lowest-free-fd allocation a
        // deterministic oracle without racing sibling tests.
        let predictor = std::fs::File::open("/dev/null").expect("open fd predictor");
        let predicted_fd = predictor.as_raw_fd();
        drop(predictor);

        let (mut reader, peer) = UnixStream::pair().expect("data witness socketpair failed");
        let reader_fd = reader.as_raw_fd();
        assert_eq!(
            reader_fd, predicted_fd,
            "data witness did not acquire the predicted lowest descriptor"
        );
        let reader_identity = descriptor_identity_for_test(reader_fd);
        set_positive_linger(reader_fd, 1);

        let payload_drops = Rc::new(Cell::new(0));
        let saw_original_fd = Rc::new(Cell::new(false));
        let run_drops = Rc::clone(&payload_drops);
        let run_saw_original_fd = Rc::clone(&saw_original_fd);

        executor
            .run(async move {
                let buffer = TargetCqeBuffer {
                    bytes: Vec::with_capacity(64),
                    fd: reader_fd,
                    identity: reader_identity,
                    drops: Rc::clone(&run_drops),
                    saw_original_fd: Rc::clone(&run_saw_original_fd),
                };
                let mut read = Box::pin(reader.read(buffer, 64));
                std::future::poll_fn(|cx| {
                    assert!(
                        read.as_mut().poll(cx).is_pending(),
                        "first data read poll did not queue an SQE"
                    );
                    Poll::Ready(())
                })
                .await;

                // Both drops occur before this task yields, so neither the
                // read nor its cancellation SQE has reached a batch flush.
                drop(read);
                drop(reader);
                assert_eq!(run_drops.get(), 0, "unflushed payload retired early");
                assert_eq!(
                    descriptor_identity_for_test(reader_fd),
                    reader_identity,
                    "same-poll stream drop released the queued read descriptor"
                );

                let held_probe = std::fs::File::open("/dev/null").expect("open held reuse probe");
                assert_ne!(
                    held_probe.as_raw_fd(),
                    reader_fd,
                    "queued data SQE allowed premature numeric-fd reuse"
                );
                assert_eq!(
                    descriptor_identity_for_test(reader_fd),
                    reader_identity,
                    "reuse probe replaced the queued read descriptor"
                );

                // Closing the peer makes the read target retire promptly even
                // if its same-batch cancellation loses the race.
                drop(peer);
                for _ in 0..100 {
                    if run_drops.get() == 1 {
                        break;
                    }
                    sleep(Duration::from_millis(5))
                        .await
                        .expect("target-CQE wait sleep failed");
                }
                assert_eq!(run_drops.get(), 1, "read target CQE did not retire");
                assert!(
                    run_saw_original_fd.get(),
                    "target reclamation did not observe the original descriptor"
                );
                assert!(
                    raw_fd_is_closed(reader_fd),
                    "final data-operation lease did not close after target retirement"
                );

                let reused = std::fs::File::open("/dev/null").expect("force fd reuse");
                assert_eq!(
                    reused.as_raw_fd(),
                    reader_fd,
                    "retired data descriptor did not become the lowest reusable fd"
                );
                let reused_identity = descriptor_identity_for_test(reader_fd);
                assert_ne!(reused_identity, reader_identity);

                Nop::new().await.expect("post-reuse NOP failed");
                assert_eq!(
                    descriptor_identity_for_test(reader_fd),
                    reused_identity,
                    "a delayed second release closed the reused numeric descriptor"
                );
                assert_eq!(run_drops.get(), 1, "target payload dropped twice");

                drop(reused);
                assert!(raw_fd_is_closed(reader_fd));
                drop(held_probe);
            })
            .expect("unflushed data-fd witness run failed");

        assert_eq!(payload_drops.get(), 1);
        assert!(saw_original_fd.get());
        #[cfg(debug_assertions)]
        {
            let stats = executor.last_stats();
            assert_eq!(stats.close_worker_admissions, 0);
            assert_eq!(stats.close_worker_disconnected_fallbacks, 1);
            assert_eq!(stats.close_linger_queries, 1);
            assert_eq!(stats.close_linger_waivers, 1);
            assert_eq!(stats.close_linger_waiver_failures, 0);
            assert_eq!(stats.close_direct_closes, 1);
        }
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
        run_exact_unit_test_child_with_watchdog(
            CLOSE_LINGER_CHILD_TEST,
            CLOSE_LINGER_CHILD_ENV,
            "positive-linger",
        );
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
    fn task_provider_diagnostic_counters_saturate() {
        let mut provider = ExecutorTaskMemProvider::new();
        provider.request_count = usize::MAX;
        provider.note_request();
        assert_eq!(provider.request_count, usize::MAX);

        #[cfg(debug_assertions)]
        {
            provider.free_count = usize::MAX;
            provider.note_free();
            assert_eq!(provider.free_count, usize::MAX);
        }
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

    #[test]
    fn try_spawn_ringless_lifecycle_preserves_whole_task_provenance() {
        struct ReadyTask {
            captured_waker: Rc<Cell<Option<Waker>>>,
            polls: Rc<Cell<usize>>,
            drops: Rc<Cell<usize>>,
        }

        impl Future for ReadyTask {
            type Output = usize;

            fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
                let this = self.get_mut();
                this.polls.set(this.polls.get() + 1);
                this.captured_waker.set(Some(cx.waker().clone()));
                Poll::Ready(41)
            }
        }

        impl Drop for ReadyTask {
            fn drop(&mut self) {
                self.drops.set(self.drops.get() + 1);
            }
        }

        struct PendingTask {
            polls: Rc<Cell<usize>>,
            drops: Rc<Cell<usize>>,
        }

        impl Future for PendingTask {
            type Output = ();

            fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
                self.polls.set(self.polls.get() + 1);
                Poll::Pending
            }
        }

        impl Drop for PendingTask {
            fn drop(&mut self) {
                self.drops.set(self.drops.get() + 1);
            }
        }

        unsafe fn dequeue_for_poll(owner: &ExecutorOwner) -> *mut TaskHeader {
            let state = owner.state_ptr();
            let task = unsafe {
                (*state)
                    .ready_queue
                    .pop_front(TaskHeader::READY_LINK_OFFSET)
            }
            .expect("spawned task was not queued");
            let flags = unsafe { (*task).flags.get() };
            unsafe {
                (*task).flags.set(
                    (flags & !(TaskHeader::FLAG_QUEUED | TaskHeader::FLAG_NOTIFIED))
                        | TaskHeader::FLAG_RUNNING,
                );
            }
            task
        }

        with_ringless_poll_context_for_test(1, |owner, _cx| {
            let state = owner.state_ptr();
            let mut join_cx = Context::from_waker(Waker::noop());

            let ready_polls = Rc::new(Cell::new(0));
            let ready_drops = Rc::new(Cell::new(0));
            let captured_waker = Rc::new(Cell::new(None));
            let mut ready_handle = Executor::try_spawn(ReadyTask {
                captured_waker: Rc::clone(&captured_waker),
                polls: Rc::clone(&ready_polls),
                drops: Rc::clone(&ready_drops),
            })
            .expect("ringless ready task admission failed");
            assert!(!ready_handle.is_finished());
            assert_eq!(
                Pin::new(&mut ready_handle).poll(&mut join_cx),
                Poll::Pending
            );

            let queued_task = unsafe { dequeue_for_poll(owner) };
            let ready_task = unsafe {
                let cached = cached_waker_ref(queued_task);
                task_ptr_from_waker(cached).expect("cached task waker lost its task pointer")
            };
            assert_eq!(ready_task, queued_task);
            let poll = unsafe { (*ready_task).vtable.poll };
            assert_eq!(unsafe { poll(ready_task) }, Poll::Ready(()));

            let flags = unsafe { (*ready_task).flags.get() };
            unsafe {
                (*ready_task).flags.set(
                    (flags
                        & !(TaskHeader::FLAG_RUNNING
                            | TaskHeader::FLAG_NOTIFIED
                            | TaskHeader::FLAG_QUEUED))
                        | TaskHeader::FLAG_COMPLETED,
                );
                assert!((*state).runtime_state.live_tasks > 0);
                (*state).runtime_state.live_tasks -= 1;
            }
            let task_ref = ExecutorTaskRefGuard::new(ready_task);
            let finish = unsafe { (*ready_task).vtable.finish };
            unsafe { finish(ready_task) };
            task_ref.release();

            assert_eq!(ready_polls.get(), 1);
            assert_eq!(ready_drops.get(), 1);
            assert!(ready_handle.is_finished());
            assert_eq!(
                Pin::new(&mut ready_handle).poll(&mut join_cx),
                Poll::Ready(Ok(41))
            );
            drop(ready_handle);
            assert!(
                unsafe { !(*state).all_tasks.is_empty() },
                "captured task waker must retain the completed task"
            );
            drop(captured_waker.take());
            assert!(
                unsafe { (*state).all_tasks.is_empty() },
                "final task-waker release did not destroy the ready task"
            );

            let pending_polls = Rc::new(Cell::new(0));
            let pending_drops = Rc::new(Cell::new(0));
            let mut pending_handle = Executor::try_spawn(PendingTask {
                polls: Rc::clone(&pending_polls),
                drops: Rc::clone(&pending_drops),
            })
            .expect("ringless pending task admission failed");
            assert!(!pending_handle.is_finished());
            assert_eq!(
                Pin::new(&mut pending_handle).poll(&mut join_cx),
                Poll::Pending
            );
            let queued_task = unsafe { dequeue_for_poll(owner) };
            let pending_task = pending_handle.task_ptr;
            assert_eq!(pending_task, queued_task);
            let poll = unsafe { (*pending_task).vtable.poll };
            assert_eq!(unsafe { poll(pending_task) }, Poll::Pending);
            let flags = unsafe { (*pending_task).flags.get() };
            unsafe {
                (*pending_task).flags.set(flags & !TaskHeader::FLAG_RUNNING);
            }
            assert_eq!(
                Pin::new(&mut pending_handle).poll(&mut join_cx),
                Poll::Pending
            );

            let cancel_panic = unsafe {
                cancel_task_and_release_executor_ref(
                    pending_task,
                    std::ptr::addr_of_mut!((*state).runtime_state),
                )
            };
            assert!(cancel_panic.is_none());
            assert_eq!(pending_polls.get(), 1);
            assert_eq!(pending_drops.get(), 1);
            assert!(pending_handle.is_finished());
            assert_eq!(
                Pin::new(&mut pending_handle).poll(&mut join_cx),
                Poll::Ready(Err(JoinError::Cancelled))
            );
            assert!(unsafe { !(*state).all_tasks.is_empty() });
            drop(pending_handle);

            unsafe {
                assert_eq!((*state).runtime_state.live_tasks, 0);
                assert!((*state).ready_queue.is_empty());
                assert!((*state).all_tasks.is_empty());
                #[cfg(debug_assertions)]
                {
                    assert_eq!((*state).runtime_state.stats.task_allocs, 2);
                    assert_eq!((*state).runtime_state.stats.task_frees, 2);
                }
            }
        });
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
        let mut executor = Executor {
            owner: ringless_owner_for_test(1),
            process_quota: DEFAULT_PROCESS_QUOTA,
            cpu_affinity: None,
            #[cfg(debug_assertions)]
            last_stats: RuntimeStats::default(),
        };
        let owner = Rc::as_ptr(&executor.owner);
        let mut handle = {
            let _active = ExecutorCtxGuard::install(owner)
                .expect("ringless join-waker context installation failed");
            Executor::try_spawn(std::future::pending::<usize>())
                .expect("pending join task admission failed")
        };
        let task = handle.task_ptr;
        let state = executor.owner.state_ptr();
        let direct = unsafe { (*task).vtable };
        let iterative = unsafe { (*task).iterative_vtable };
        assert!(!std::ptr::eq(direct, iterative));

        let first_stats = Rc::new(CountedWakerStats::default());
        let first_waker = counted_waker(&first_stats);
        let mut first_cx = Context::from_waker(&first_waker);

        assert!(Pin::new(&mut handle).poll(&mut first_cx).is_pending());
        assert!(std::ptr::eq(unsafe { (*task).vtable }, iterative));
        assert_eq!(first_stats.clones.get(), 1);
        assert_eq!(first_stats.drops.get(), 0);

        assert!(Pin::new(&mut handle).poll(&mut first_cx).is_pending());
        assert!(std::ptr::eq(unsafe { (*task).vtable }, iterative));
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

        assert!(Pin::new(&mut handle).poll(&mut second_cx).is_pending());
        assert_eq!(
            first_stats.drops.get(),
            1,
            "different waiter should replace the previous stored waker"
        );
        assert_eq!(second_stats.clones.get(), 1);
        assert!(
            unsafe { &*handle.waker_ptr }
                .as_ref()
                .expect("join waker should be stored")
                .will_wake(&second_waker)
        );

        unsafe {
            (*state)
                .ready_queue
                .remove(std::ptr::addr_of_mut!((*task).ready_link));
        }
        let panic = unsafe {
            cancel_task_and_release_executor_ref(
                task,
                std::ptr::addr_of_mut!((*state).runtime_state),
            )
        };
        assert!(panic.is_none());
        assert_eq!(second_stats.wakes.get(), 1);
        assert!(matches!(
            Pin::new(&mut handle).poll(&mut second_cx),
            Poll::Ready(Err(JoinError::Cancelled))
        ));
        drop(handle);
        unsafe {
            assert!((*state).all_tasks.is_empty());
        }
        executor.shutdown_owner();
    }

    #[test]
    fn join_handle_arms_before_clone_and_publishes_before_old_waker_drop() {
        let mut executor = Executor {
            owner: ringless_owner_for_test(1),
            process_quota: DEFAULT_PROCESS_QUOTA,
            cpu_affinity: None,
            #[cfg(debug_assertions)]
            last_stats: RuntimeStats::default(),
        };
        let owner = Rc::as_ptr(&executor.owner);
        let mut handle = {
            let _active = ExecutorCtxGuard::install(owner)
                .expect("ringless join panic context installation failed");
            Executor::try_spawn(std::future::pending::<usize>())
                .expect("pending join task admission failed")
        };
        let task = handle.task_ptr;
        let state = executor.owner.state_ptr();
        let iterative = unsafe { (*task).iterative_vtable };

        let clone_stats = Rc::new(CountedWakerStats::default());
        let clone_waker = panicking_clone_waker(&clone_stats);
        let mut clone_cx = Context::from_waker(&clone_waker);
        let first_clone_panic = catch_unwind(AssertUnwindSafe(|| {
            Pin::new(&mut handle).poll(&mut clone_cx)
        }))
        .expect_err("first join-waker clone did not panic");
        assert!(first_clone_panic.is::<TaskWakerClonePanic>());
        assert!(std::ptr::eq(unsafe { (*task).vtable }, iterative));
        assert!(unsafe { (&*handle.waker_ptr).is_none() });

        let first_stats = Rc::new(CountedWakerStats::default());
        let first_waker = counted_waker(&first_stats);
        let mut first_cx = Context::from_waker(&first_waker);
        assert!(Pin::new(&mut handle).poll(&mut first_cx).is_pending());

        let replacement_clone_panic = catch_unwind(AssertUnwindSafe(|| {
            Pin::new(&mut handle).poll(&mut clone_cx)
        }))
        .expect_err("replacement join-waker clone did not panic");
        assert!(replacement_clone_panic.is::<TaskWakerClonePanic>());
        assert!(
            unsafe { &*handle.waker_ptr }
                .as_ref()
                .is_some_and(|stored| stored.will_wake(&first_waker))
        );

        let panicking_drop_stats = Rc::new(CountedWakerStats::default());
        let panicking_drop_source = replacement_drop_source_waker(&panicking_drop_stats);
        let mut panicking_drop_cx = Context::from_waker(&panicking_drop_source);
        assert!(
            Pin::new(&mut handle)
                .poll(&mut panicking_drop_cx)
                .is_pending()
        );
        assert_eq!(first_stats.drops.get(), 1);

        let final_stats = Rc::new(CountedWakerStats::default());
        let final_waker = counted_waker(&final_stats);
        let mut final_cx = Context::from_waker(&final_waker);
        let old_drop_panic = catch_unwind(AssertUnwindSafe(|| {
            Pin::new(&mut handle).poll(&mut final_cx)
        }))
        .expect_err("old stored join-waker drop did not panic");
        assert!(old_drop_panic.is::<TaskWakerDropPanic>());
        assert!(
            unsafe { &*handle.waker_ptr }
                .as_ref()
                .is_some_and(|stored| stored.will_wake(&final_waker))
        );
        assert_eq!(panicking_drop_stats.drops.get(), 1);

        unsafe {
            (*state)
                .ready_queue
                .remove(std::ptr::addr_of_mut!((*task).ready_link));
        }
        let cleanup_panic = unsafe {
            cancel_task_and_release_executor_ref(
                task,
                std::ptr::addr_of_mut!((*state).runtime_state),
            )
        };
        assert!(cleanup_panic.is_none());
        assert!(matches!(
            Pin::new(&mut handle).poll(&mut clone_cx),
            Poll::Ready(Err(JoinError::Cancelled))
        ));
        assert_eq!(clone_stats.clones.get(), 2);
        drop(handle);
        unsafe {
            assert!((*state).all_tasks.is_empty());
        }
        executor.shutdown_owner();
    }

    #[test]
    fn join_waker_clone_reentrancy_observes_arm_and_reclaims_nested_task() {
        let mut executor = Executor {
            owner: ringless_owner_for_test(1),
            process_quota: DEFAULT_PROCESS_QUOTA,
            cpu_affinity: None,
            #[cfg(debug_assertions)]
            last_stats: RuntimeStats::default(),
        };
        let owner = Rc::as_ptr(&executor.owner);
        let drops = Rc::new(Cell::new(0));
        let (mut handle, nested) = {
            let _active = ExecutorCtxGuard::install(owner)
                .expect("ringless reentrant-waker context installation failed");
            let handle = Executor::try_spawn(std::future::pending::<()>())
                .expect("reentrant-waker task admission failed");
            let mut staged = stage_completed_task_output_for_benchmark(StagedTaskDropProbe::new(
                Rc::clone(&drops),
                false,
            ))
            .expect("nested callback task staging failed");
            let nested = SyntheticTaskRef { task: staged.task };
            staged.owns_reference = false;
            drop(staged);
            (handle, nested)
        };
        let task = handle.task_ptr;
        let state = executor.owner.state_ptr();
        let reentrant_state = Rc::new(ReentrantCloneWakerState {
            armed_task: task,
            iterative_vtable: unsafe { (*task).iterative_vtable },
            release_during_clone: RefCell::new(Some(nested)),
            clones: Cell::new(0),
        });
        let waker = reentrant_clone_waker(&reentrant_state);
        let mut cx = Context::from_waker(&waker);

        assert!(Pin::new(&mut handle).poll(&mut cx).is_pending());
        assert_eq!(reentrant_state.clones.get(), 1);
        assert!(reentrant_state.release_during_clone.borrow().is_none());
        assert_eq!(drops.get(), 1);
        assert!(std::ptr::eq(
            unsafe { (*task).vtable },
            reentrant_state.iterative_vtable,
        ));

        unsafe {
            (*state)
                .ready_queue
                .remove(std::ptr::addr_of_mut!((*task).ready_link));
        }
        let cleanup_panic = unsafe {
            cancel_task_and_release_executor_ref(
                task,
                std::ptr::addr_of_mut!((*state).runtime_state),
            )
        };
        assert!(cleanup_panic.is_none());
        assert!(matches!(
            Pin::new(&mut handle).poll(&mut cx),
            Poll::Ready(Err(JoinError::Cancelled))
        ));
        drop(handle);
        unsafe {
            assert!((*state).all_tasks.is_empty());
            #[cfg(debug_assertions)]
            assert_eq!((*state).runtime_state.stats.task_allocs, 2);
            #[cfg(debug_assertions)]
            assert_eq!((*state).runtime_state.stats.task_frees, 2);
        }
        executor.shutdown_owner();
    }

    #[cfg(not(miri))]
    #[test]
    fn join_handle_post_ready_repoll_arms_before_waker_clone() {
        let handle_slot = Rc::new(RefCell::new(None::<JoinHandle<usize>>));
        let mut executor = Executor::new().expect("executor construction failed");
        executor
            .run({
                let handle_slot = Rc::clone(&handle_slot);
                async move {
                    *handle_slot.borrow_mut() = Some(
                        Executor::spawn(async { 41usize }).expect("post-ready task spawn failed"),
                    );
                }
            })
            .expect("post-ready task did not complete");

        let mut handle = handle_slot
            .borrow_mut()
            .take()
            .expect("post-ready handle disappeared");
        let task = handle.task_ptr;
        let direct = unsafe { (*task).vtable };
        let iterative = unsafe { (*task).iterative_vtable };
        assert!(!std::ptr::eq(direct, iterative));

        let ready_waker = Waker::noop();
        let mut ready_cx = Context::from_waker(ready_waker);
        assert_eq!(
            Pin::new(&mut handle).poll(&mut ready_cx),
            Poll::Ready(Ok(41))
        );
        assert!(std::ptr::eq(unsafe { (*task).vtable }, direct));
        assert!(unsafe { (&*handle.waker_ptr).is_none() });

        let clone_stats = Rc::new(CountedWakerStats::default());
        let clone_waker = panicking_clone_waker(&clone_stats);
        let mut clone_cx = Context::from_waker(&clone_waker);
        let panic = catch_unwind(AssertUnwindSafe(|| {
            Pin::new(&mut handle).poll(&mut clone_cx)
        }))
        .expect_err("post-ready repoll clone did not panic");
        assert!(panic.is::<TaskWakerClonePanic>());
        assert!(std::ptr::eq(unsafe { (*task).vtable }, iterative));
        assert!(unsafe { (&*handle.waker_ptr).is_none() });

        drop(handle);
        unsafe {
            assert!((*executor.owner.state_ptr()).all_tasks.is_empty());
        }
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

    #[cfg(debug_assertions)]
    #[test]
    fn task_schedule_counter_saturates_without_changing_queue_ownership() {
        let mut ready_queue = DList::new_uninit();
        ready_queue.init();
        let mut runtime_state = RuntimeState::new();
        runtime_state.stats.task_schedules = usize::MAX;
        let mut header = TaskHeader::new();
        let task_ptr = &mut header as *mut TaskHeader;

        assert!(unsafe {
            notify_task_into_list_unchecked(task_ptr, &mut ready_queue, &mut runtime_state)
        });
        assert_eq!(runtime_state.stats.task_schedules, usize::MAX);
        assert_eq!(
            unsafe { ready_queue.pop_front(TaskHeader::READY_LINK_OFFSET) },
            Some(task_ptr)
        );
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
        let header_ptr = &mut header as *mut TaskHeader;
        unsafe {
            (*executor.owner.state_ptr())
                .ready_queue
                .push_back(std::ptr::addr_of_mut!((*header_ptr).ready_link));
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
