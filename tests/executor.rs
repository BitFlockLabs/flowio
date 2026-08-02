mod common;

use common::{
    DropTrackedReadOnly, DropTrackedReadWrite, poll_once_pending,
    run_exact_test_child_with_watchdog, wait_for_drop_count, wait_for_live_slots,
};
use flowio::net::unix::UnixStream;
use flowio::net::{WritevPieces, WritevProjection};
use flowio::runtime::buffer::iobuffvec::{IoBuffReadOnlyVec, IoBuffVecMut};
use flowio::runtime::buffer::pool::{IoBuffPool, IoBuffPoolConfig};
use flowio::runtime::buffer::{IoBuffMut, IoBuffReadOnly};
#[cfg(debug_assertions)]
use flowio::runtime::executor::RuntimeStats;
use flowio::runtime::executor::{Executor, ExecutorConfig, JoinError, JoinHandle, TrySpawnError};
use flowio::runtime::reactor::ReactorConfig;
use flowio::runtime::timer::{Sleep, TimeoutError, sleep, sleep_until, timeout, timeout_at};
use flowio::test_support::runtime::io::{Nop, NopSlot};
use flowio::test_support::runtime::op::CompletionState;
#[cfg(not(miri))]
use flowio::test_support::runtime::reactor::{
    CompletionDrainDescriptorReport, CompletionDrainReentrancyReport,
    test_completion_drain_descriptor_close, test_completion_drain_reentrancy,
};
use flowio::test_support::runtime::task::TaskHeader;
use flowio::test_support::runtime::test_hooks;
use std::cell::{Cell, RefCell};
use std::future::{Future, poll_fn};
use std::io;
use std::net::Shutdown;
use std::os::fd::AsRawFd;
#[cfg(any(debug_assertions, feature = "test-support"))]
use std::os::fd::RawFd;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::Arc;
#[cfg(target_os = "linux")]
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::task::{Context, Poll, Waker};
use std::time::{Duration, Instant};

const COMPLETION_DRAIN_REENTRANCY_CHILD_ENV: &str = "FLOWIO_COMPLETION_DRAIN_REENTRANCY_CHILD";
const COMPLETION_DRAIN_REENTRANCY_CHILD_TEST: &str =
    "runtime_real_completion_drain_preserves_reentrant_operation_ownership";
const COMPLETION_DRAIN_DESCRIPTOR_CHILD_ENV: &str = "FLOWIO_COMPLETION_DRAIN_DESCRIPTOR_CHILD";
const COMPLETION_DRAIN_DESCRIPTOR_CHILD_TEST: &str =
    "runtime_real_completion_drain_closes_payload_descriptor_without_ring_reentry";

fn new_executor() -> Executor {
    Executor::new().expect("failed to construct runtime executor")
}

fn new_executor_with(process_quota: usize, cpu_affinity: Option<usize>) -> Executor {
    Executor::new_with_config(ExecutorConfig {
        reactor: ReactorConfig { ring_entries: 64 },
        process_quota,
        cpu_affinity,
    })
    .expect("failed to construct runtime executor")
}

fn new_one_slot_executor() -> Executor {
    Executor::new_with_config(ExecutorConfig {
        reactor: ReactorConfig { ring_entries: 1 },
        ..ExecutorConfig::default()
    })
    .expect("failed to construct one-slot executor")
}

#[cfg(debug_assertions)]
fn assert_retained_stats_zero(stats: RuntimeStats, run: &str) {
    assert_eq!(stats.retained_pooled_allocs, 0, "{run}: pooled allocs");
    assert_eq!(stats.retained_pooled_reuses, 0, "{run}: pooled reuses");
    assert_eq!(stats.retained_pooled_frees, 0, "{run}: pooled frees");
    assert_eq!(stats.retained_slab_allocs, 0, "{run}: slab allocs");
    assert_eq!(stats.retained_heap_fallbacks, 0, "{run}: heap fallbacks");
    assert_eq!(stats.retained_heap_frees, 0, "{run}: heap frees");
    assert_eq!(
        stats.writev_scratch_inline_allocs, 0,
        "{run}: inline scratch allocs"
    );
    assert_eq!(
        stats.writev_scratch_pooled_allocs, 0,
        "{run}: pooled scratch allocs"
    );
    assert_eq!(
        stats.writev_scratch_pooled_reuses, 0,
        "{run}: pooled scratch reuses"
    );
    assert_eq!(
        stats.writev_scratch_pooled_frees, 0,
        "{run}: pooled scratch frees"
    );
    assert_eq!(
        stats.writev_scratch_slab_allocs, 0,
        "{run}: scratch slab allocs"
    );
    assert_eq!(
        stats.writev_scratch_oversize_rejections, 0,
        "{run}: scratch oversize rejections"
    );
    assert_eq!(
        stats.writev_scratch_alloc_failures, 0,
        "{run}: scratch allocation failures"
    );
}

#[cfg(any(debug_assertions, feature = "test-support"))]
fn fd_identity(fd: RawFd) -> io::Result<(libc::dev_t, libc::ino_t)> {
    let mut stat = std::mem::MaybeUninit::<libc::stat>::uninit();
    // SAFETY: `stat` points to writable storage and `fstat` accepts any integer
    // descriptor, reporting `EBADF` when it is no longer open.
    let rc = unsafe { libc::fstat(fd, stat.as_mut_ptr()) };
    if rc == -1 {
        return Err(io::Error::last_os_error());
    }

    // SAFETY: successful `fstat` initialized the complete `libc::stat` value.
    let stat = unsafe { stat.assume_init() };
    Ok((stat.st_dev, stat.st_ino))
}

struct ExternalWakeFuture {
    release: Rc<Cell<bool>>,
    completed: Rc<Cell<bool>>,
    waker: Rc<RefCell<Option<Waker>>>,
}

impl Future for ExternalWakeFuture {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if self.release.get() {
            self.completed.set(true);
            return Poll::Ready(());
        }
        *self.waker.borrow_mut() = Some(cx.waker().clone());
        Poll::Pending
    }
}

struct StageCompletedNop {
    nop: Option<Nop>,
    staged: Rc<RefCell<Option<Nop>>>,
    submitted: bool,
}

struct StageFiredSleep {
    sleep: Option<Sleep>,
    staged: Rc<RefCell<Option<Sleep>>>,
    armed: bool,
}

impl Future for StageFiredSleep {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        if this.armed {
            *this.staged.borrow_mut() = this.sleep.take();
            return Poll::Ready(());
        }

        let sleep = this.sleep.as_mut().expect("staged sleep missing");
        assert!(Pin::new(sleep).poll(cx).is_pending());
        this.armed = true;
        Poll::Pending
    }
}

impl Future for StageCompletedNop {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        if this.submitted {
            *this.staged.borrow_mut() = this.nop.take();
            return Poll::Ready(());
        }

        let nop = this.nop.as_mut().expect("staged NOP missing");
        assert!(Pin::new(nop).poll(cx).is_pending());
        this.submitted = true;
        Poll::Pending
    }
}

struct DropCounter(Rc<Cell<usize>>);

impl Drop for DropCounter {
    fn drop(&mut self) {
        self.0.set(self.0.get() + 1);
    }
}

fn poll_join_handle<T: 'static>(handle: &mut JoinHandle<T>) -> Poll<Result<T, JoinError>> {
    let waker = Waker::noop();
    let mut cx = Context::from_waker(waker);
    Pin::new(handle).poll(&mut cx)
}

fn capture_flowio_waker(executor: &mut Executor) -> Waker {
    let captured = Rc::new(RefCell::new(None));
    let captured_slot = Rc::clone(&captured);
    executor
        .run(poll_fn(move |cx| {
            *captured_slot.borrow_mut() = Some(cx.waker().clone());
            Poll::Ready(())
        }))
        .expect("capture-waker run failed");
    captured
        .borrow_mut()
        .take()
        .expect("executor task waker was not captured")
}

struct CrossTaskRepollShared<F> {
    future: RefCell<F>,
    first_poll_pending: Cell<bool>,
    completed_by_second: Cell<bool>,
}

struct FirstPollSharedFuture<F> {
    shared: Rc<CrossTaskRepollShared<F>>,
}

struct SecondPollSharedFuture<F> {
    shared: Rc<CrossTaskRepollShared<F>>,
}

struct PendingFutureHandoff<F> {
    future: RefCell<Option<F>>,
    receiver_waker: RefCell<Option<Waker>>,
}

struct PollPendingThenHandoff<F> {
    future: Option<F>,
    shared: Rc<PendingFutureHandoff<F>>,
    hold_after_poll: Duration,
}

impl<F: Future + Unpin> Future for FirstPollSharedFuture<F> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let mut future = this.shared.future.borrow_mut();
        match Pin::new(&mut *future).poll(cx) {
            Poll::Pending => {
                this.shared.first_poll_pending.set(true);
                Poll::Ready(())
            }
            Poll::Ready(_) => panic!("shared future completed during first poll"),
        }
    }
}

impl<F: Future + Unpin> Future for SecondPollSharedFuture<F> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        if !this.shared.first_poll_pending.get() {
            cx.waker().wake_by_ref();
            return Poll::Pending;
        }

        let mut future = this.shared.future.borrow_mut();
        match Pin::new(&mut *future).poll(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(_) => {
                this.shared.completed_by_second.set(true);
                Poll::Ready(())
            }
        }
    }
}

impl<F: Future + Unpin> Future for PollPendingThenHandoff<F> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let future = this.future.as_mut().expect("handoff future missing");
        assert!(
            Pin::new(future).poll(cx).is_pending(),
            "handoff future completed on its first poll"
        );

        if !this.hold_after_poll.is_zero() {
            // Test-only ordering control: make the timer due before this task
            // completes so process_quota=1 expires it before the receiver runs.
            std::thread::sleep(this.hold_after_poll);
        }

        *this.shared.future.borrow_mut() = this.future.take();
        if let Some(waker) = this.shared.receiver_waker.borrow_mut().take() {
            waker.wake();
        }
        Poll::Ready(())
    }
}

struct BatchedNops {
    nops: Vec<Nop>,
    completed: Vec<bool>,
}

impl BatchedNops {
    fn new(count: usize) -> Self {
        Self {
            nops: (0..count).map(|_| Nop::new()).collect(),
            completed: vec![false; count],
        }
    }
}

impl Future for BatchedNops {
    type Output = io::Result<usize>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let mut completed_count = 0usize;

        for index in 0..this.nops.len() {
            if this.completed[index] {
                completed_count += 1;
                continue;
            }

            match Pin::new(&mut this.nops[index]).poll(cx) {
                Poll::Ready(Ok(0)) => {
                    this.completed[index] = true;
                    completed_count += 1;
                }
                Poll::Ready(Ok(_)) => {
                    return Poll::Ready(Err(io::Error::other("unexpected NOP result")));
                }
                Poll::Ready(Err(err)) => return Poll::Ready(Err(err)),
                Poll::Pending => {}
            }
        }

        if completed_count == this.nops.len() {
            Poll::Ready(Ok(completed_count))
        } else {
            Poll::Pending
        }
    }
}

fn run_cross_task_repoll<F>(future: F)
where
    F: Future + Unpin + 'static,
    F::Output: 'static,
{
    let shared = Rc::new(CrossTaskRepollShared {
        future: RefCell::new(future),
        first_poll_pending: Cell::new(false),
        completed_by_second: Cell::new(false),
    });
    let observed = Rc::clone(&shared);

    let mut executor = new_executor();
    executor
        .run(async move {
            let first = Executor::spawn(FirstPollSharedFuture {
                shared: Rc::clone(&shared),
            })
            .expect("spawn first shared-poll task");
            let second = Executor::spawn(SecondPollSharedFuture { shared })
                .expect("spawn second shared-poll task");

            first.await.expect("first shared task cancelled");
            second.await.expect("second shared task cancelled");
        })
        .expect("cross-task repoll should complete");

    assert!(observed.first_poll_pending.get());
    assert!(observed.completed_by_second.get());
}

fn run_parent_completion_before_waiter_migration<F>(
    future: F,
    hold_after_poll: Duration,
) -> F::Output
where
    F: Future + Unpin + 'static,
    F::Output: 'static,
{
    let shared = Rc::new(PendingFutureHandoff {
        future: RefCell::new(None),
        receiver_waker: RefCell::new(None),
    });
    let output = Rc::new(RefCell::new(None));
    let output_slot = Rc::clone(&output);
    let mut executor = new_executor_with(1, None);

    executor
        .run({
            let shared = Rc::clone(&shared);
            async move {
                let first_owner = Executor::spawn(PollPendingThenHandoff {
                    future: Some(future),
                    shared: Rc::clone(&shared),
                    hold_after_poll,
                })
                .expect("spawn first waiter owner");
                drop(first_owner);

                let mut migrated = poll_fn(|cx| {
                    if let Some(future) = shared.future.borrow_mut().take() {
                        return Poll::Ready(future);
                    }

                    let mut receiver_waker = shared.receiver_waker.borrow_mut();
                    if !receiver_waker
                        .as_ref()
                        .is_some_and(|stored| stored.will_wake(cx.waker()))
                    {
                        *receiver_waker = Some(cx.waker().clone());
                    }
                    Poll::Pending
                })
                .await;

                *output_slot.borrow_mut() = Some(Pin::new(&mut migrated).await);
            }
        })
        .expect("waiter migration run failed");

    #[cfg(debug_assertions)]
    {
        let stats = executor.last_stats();
        assert_eq!(stats.task_allocs, 2);
        assert_eq!(stats.task_frees, 2, "waiter owner task reference leaked");
    }

    output
        .borrow_mut()
        .take()
        .expect("migrated future did not complete")
}

fn run_parent_completion_without_pending_future_drop<F>(future: F, hold_after_poll: Duration)
where
    F: Future + Unpin + 'static,
    F::Output: 'static,
{
    let shared = Rc::new(PendingFutureHandoff {
        future: RefCell::new(None),
        receiver_waker: RefCell::new(None),
    });
    let observed = Rc::clone(&shared);
    let mut executor = new_executor_with(1, None);

    executor
        .run(async move {
            let first_owner = Executor::spawn(PollPendingThenHandoff {
                future: Some(future),
                shared,
                hold_after_poll,
            })
            .expect("spawn retained pending-future owner");
            drop(first_owner);
        })
        .expect("pending future should remain valid through waiter completion");

    #[cfg(debug_assertions)]
    {
        let stats = executor.last_stats();
        assert_eq!(stats.task_allocs, 2);
        assert_eq!(stats.task_frees, 2, "forgotten waiter owner task leaked");
    }

    // Keep the future alive without repolling or dropping it until after the
    // CQE/timer has consumed its waiter, then reclaim its completed state.
    let pending = observed
        .future
        .borrow_mut()
        .take()
        .expect("pending future was not retained outside its first owner");
    drop(pending);
}

#[cfg(target_os = "linux")]
extern "C" fn handle_executor_signal(_signal: libc::c_int) {}

#[cfg(target_os = "linux")]
struct SignalHandlerGuard {
    signal: libc::c_int,
    old_action: libc::sigaction,
}

#[cfg(target_os = "linux")]
impl SignalHandlerGuard {
    fn install(signal: libc::c_int) -> Self {
        let mut action = unsafe { std::mem::zeroed::<libc::sigaction>() };
        action.sa_sigaction = handle_executor_signal as *const () as usize;
        action.sa_flags = 0;
        let rc = unsafe { libc::sigemptyset(&mut action.sa_mask) };
        assert_eq!(rc, 0, "sigemptyset failed");

        let mut old_action = unsafe { std::mem::zeroed::<libc::sigaction>() };
        let rc = unsafe { libc::sigaction(signal, &action, &mut old_action) };
        assert_eq!(rc, 0, "sigaction install failed");

        Self { signal, old_action }
    }
}

#[cfg(target_os = "linux")]
impl Drop for SignalHandlerGuard {
    fn drop(&mut self) {
        let rc = unsafe { libc::sigaction(self.signal, &self.old_action, std::ptr::null_mut()) };
        assert_eq!(rc, 0, "sigaction restore failed");
    }
}

#[cfg(any(debug_assertions, feature = "test-support"))]
fn write_all_raw_fd(fd: RawFd, mut bytes: &[u8]) {
    while !bytes.is_empty() {
        let rc = unsafe { libc::write(fd, bytes.as_ptr().cast(), bytes.len()) };
        if rc < 0 {
            let err = io::Error::last_os_error();
            if err.raw_os_error() == Some(libc::EINTR) {
                continue;
            }
            panic!("raw fd write failed: {err}");
        }
        assert!(rc > 0, "raw fd write made no progress");
        bytes = &bytes[rc as usize..];
    }
}

#[derive(Clone, Copy)]
struct StaticReadOnly;

unsafe impl IoBuffReadOnly for StaticReadOnly {
    fn as_ptr(&self) -> *const u8 {
        b"x".as_ptr()
    }

    fn len(&self) -> usize {
        1
    }
}

// Static per-test drop counters for large SmallTrackedReadOnly chains. The
// segments move into kernel-retained operations, so they cannot borrow an Rc.
static SMALL_TRACKED_DROPS_0: AtomicUsize = AtomicUsize::new(0);
static SMALL_TRACKED_DROPS_1: AtomicUsize = AtomicUsize::new(0);
static SMALL_TRACKED_DROPS_2: AtomicUsize = AtomicUsize::new(0);
static SMALL_TRACKED_DROPS_3: AtomicUsize = AtomicUsize::new(0);
// Shared static payload pages used by SmallTrackedReadOnly segments.
static SMALL_TRACKED_BLOCKS: [[u8; 4096]; 4] =
    [[0x11; 4096], [0x22; 4096], [0x33; 4096], [0x44; 4096]];

fn small_tracked_drops(counter: u8) -> &'static AtomicUsize {
    match counter {
        0 => &SMALL_TRACKED_DROPS_0,
        1 => &SMALL_TRACKED_DROPS_1,
        2 => &SMALL_TRACKED_DROPS_2,
        3 => &SMALL_TRACKED_DROPS_3,
        _ => &SMALL_TRACKED_DROPS_0,
    }
}

/// Static read-only segment that increments its selected drop counter.
struct SmallTrackedReadOnly<const LEN: usize> {
    /// Index into SMALL_TRACKED_BLOCKS for the segment bytes.
    index: u8,
    /// Which static drop counter to increment.
    counter: u8,
}

impl<const LEN: usize> Drop for SmallTrackedReadOnly<LEN> {
    fn drop(&mut self) {
        small_tracked_drops(self.counter).fetch_add(1, Ordering::Relaxed);
    }
}

unsafe impl<const LEN: usize> IoBuffReadOnly for SmallTrackedReadOnly<LEN> {
    fn as_ptr(&self) -> *const u8 {
        SMALL_TRACKED_BLOCKS[self.index as usize % SMALL_TRACKED_BLOCKS.len()].as_ptr()
    }

    fn len(&self) -> usize {
        LEN
    }
}

/// Test future whose size is controlled by PAD.
///
/// PAD=0 fits the task slot; large PAD values force try_spawn/spawn to reject
/// the future as TaskTooLarge while preserving ownership.
struct RecoverableSpawnFuture<const PAD: usize> {
    id: usize,
    drops: Rc<Cell<usize>>,
    polls: Rc<Cell<usize>>,
    _pad: [u8; PAD],
}

impl<const PAD: usize> RecoverableSpawnFuture<PAD> {
    fn new(id: usize, drops: &Rc<Cell<usize>>, polls: &Rc<Cell<usize>>) -> Self {
        Self {
            id,
            drops: Rc::clone(drops),
            polls: Rc::clone(polls),
            _pad: [0; PAD],
        }
    }
}

impl<const PAD: usize> Future for RecoverableSpawnFuture<PAD> {
    type Output = usize;

    fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        this.polls.set(this.polls.get() + 1);
        Poll::Ready(this.id)
    }
}

impl<const PAD: usize> Drop for RecoverableSpawnFuture<PAD> {
    fn drop(&mut self) {
        self.drops.set(self.drops.get() + 1);
    }
}

static PROJECTED_SOURCE_DROPS: AtomicUsize = AtomicUsize::new(0);

#[cfg(target_os = "linux")]
static SIGNAL_TEST_LOCK: Mutex<()> = Mutex::new(());

/// Test WritevProjection source with independently controlled reported and
/// projected lengths so rejection tests can fabricate mismatches.
struct ProjectedBytes<const N: usize> {
    /// Backing bytes projected one byte per piece.
    bytes: [u8; N],
    /// Piece count reported by writev_count_and_len.
    counted_pieces: usize,
    /// Total bytes reported by writev_count_and_len.
    counted_total: usize,
    /// Actual number of bytes pushed by project_writev.
    projected_len: usize,
    /// Whether Drop should increment PROJECTED_SOURCE_DROPS.
    track_drop: bool,
}

impl<const N: usize> ProjectedBytes<N> {
    fn new(track_drop: bool) -> Self {
        Self {
            bytes: std::array::from_fn(|i| (i % 251) as u8),
            counted_pieces: N,
            counted_total: N,
            projected_len: N,
            track_drop,
        }
    }

    fn with_projection(counted_pieces: usize, counted_total: usize, projected_len: usize) -> Self {
        Self {
            bytes: std::array::from_fn(|i| (i % 251) as u8),
            counted_pieces,
            counted_total,
            projected_len,
            track_drop: false,
        }
    }

    fn expected(&self) -> Vec<u8> {
        self.bytes[..self.projected_len].to_vec()
    }
}

impl<const N: usize> Drop for ProjectedBytes<N> {
    fn drop(&mut self) {
        if self.track_drop {
            PROJECTED_SOURCE_DROPS.fetch_add(1, Ordering::Relaxed);
        }
    }
}

impl<const N: usize> WritevProjection for ProjectedBytes<N> {
    fn writev_count_and_len(&self) -> (usize, usize) {
        (self.counted_pieces, self.counted_total)
    }

    fn project_writev<'a>(&'a self, pieces: &mut WritevPieces<'a>) -> io::Result<()> {
        for byte in &self.bytes[..self.projected_len] {
            pieces.push(std::slice::from_ref(byte))?;
        }
        Ok(())
    }
}

/// Projected source with a task-local exact-drop counter for failure tests.
struct DropTrackedProjected<const N: usize> {
    bytes: [u8; N],
    counted_pieces: usize,
    counted_total: usize,
    projected_len: usize,
    drops: Rc<Cell<usize>>,
}

impl<const N: usize> DropTrackedProjected<N> {
    #[cfg(any(debug_assertions, feature = "test-support"))]
    fn matching(drops: &Rc<Cell<usize>>) -> Self {
        Self::with_projection(N, N, N, drops)
    }

    fn with_projection(
        counted_pieces: usize,
        counted_total: usize,
        projected_len: usize,
        drops: &Rc<Cell<usize>>,
    ) -> Self {
        Self {
            bytes: std::array::from_fn(|i| (i % 251) as u8),
            counted_pieces,
            counted_total,
            projected_len,
            drops: Rc::clone(drops),
        }
    }

    fn expected(&self) -> &[u8] {
        &self.bytes[..self.projected_len]
    }
}

impl<const N: usize> Drop for DropTrackedProjected<N> {
    fn drop(&mut self) {
        self.drops.set(self.drops.get() + 1);
    }
}

impl<const N: usize> WritevProjection for DropTrackedProjected<N> {
    fn writev_count_and_len(&self) -> (usize, usize) {
        (self.counted_pieces, self.counted_total)
    }

    fn project_writev<'a>(&'a self, pieces: &mut WritevPieces<'a>) -> io::Result<()> {
        for byte in &self.bytes[..self.projected_len] {
            pieces.push(std::slice::from_ref(byte))?;
        }
        Ok(())
    }
}

/// Read-only segment whose pointer callback can be counted or forced to panic.
#[cfg(any(debug_assertions, feature = "test-support"))]
struct CallbackTrackedReadOnly {
    bytes: &'static [u8],
    as_ptr_calls: Rc<Cell<usize>>,
    drops: Rc<Cell<usize>>,
    panic_on_as_ptr: bool,
}

#[cfg(any(debug_assertions, feature = "test-support"))]
impl CallbackTrackedReadOnly {
    fn new(
        bytes: &'static [u8],
        as_ptr_calls: &Rc<Cell<usize>>,
        drops: &Rc<Cell<usize>>,
        panic_on_as_ptr: bool,
    ) -> Self {
        Self {
            bytes,
            as_ptr_calls: Rc::clone(as_ptr_calls),
            drops: Rc::clone(drops),
            panic_on_as_ptr,
        }
    }
}

#[cfg(any(debug_assertions, feature = "test-support"))]
impl Drop for CallbackTrackedReadOnly {
    fn drop(&mut self) {
        self.drops.set(self.drops.get() + 1);
    }
}

#[cfg(any(debug_assertions, feature = "test-support"))]
unsafe impl IoBuffReadOnly for CallbackTrackedReadOnly {
    fn as_ptr(&self) -> *const u8 {
        self.as_ptr_calls.set(self.as_ptr_calls.get() + 1);
        assert!(
            !self.panic_on_as_ptr,
            "forced writev pointer callback panic"
        );
        self.bytes.as_ptr()
    }

    fn len(&self) -> usize {
        self.bytes.len()
    }
}

#[cfg(any(debug_assertions, feature = "test-support"))]
fn callback_tracked_chain(
    bytes: &'static [u8],
    as_ptr_calls: &Rc<Cell<usize>>,
    drops: &Rc<Cell<usize>>,
    panic_on_as_ptr: bool,
) -> IoBuffReadOnlyVec<CallbackTrackedReadOnly, 1> {
    let mut chain = IoBuffReadOnlyVec::new();
    chain
        .push(CallbackTrackedReadOnly::new(
            bytes,
            as_ptr_calls,
            drops,
            panic_on_as_ptr,
        ))
        .expect("single callback-tracked segment should fit");
    chain
}

/// Test `WritevProjection` source backed by indices into `SMALL_TRACKED_BLOCKS`,
/// projecting one static page per index at length `LEN`.
struct ProjectedStaticSegments<const N: usize, const LEN: usize> {
    /// Index into `SMALL_TRACKED_BLOCKS` for each projected segment.
    indices: [u8; N],
    /// Whether Drop should increment `PROJECTED_SOURCE_DROPS`.
    track_drop: bool,
}

impl<const N: usize, const LEN: usize> ProjectedStaticSegments<N, LEN> {
    fn new(track_drop: bool) -> Self {
        Self {
            indices: std::array::from_fn(|i| (i % SMALL_TRACKED_BLOCKS.len()) as u8),
            track_drop,
        }
    }

    fn expected(&self) -> Vec<u8> {
        let mut expected = Vec::with_capacity(N * LEN);
        for index in self.indices {
            expected.extend_from_slice(&SMALL_TRACKED_BLOCKS[index as usize][..LEN]);
        }
        expected
    }
}

impl<const N: usize, const LEN: usize> Drop for ProjectedStaticSegments<N, LEN> {
    fn drop(&mut self) {
        if self.track_drop {
            PROJECTED_SOURCE_DROPS.fetch_add(1, Ordering::Relaxed);
        }
    }
}

impl<const N: usize, const LEN: usize> WritevProjection for ProjectedStaticSegments<N, LEN> {
    fn writev_count_and_len(&self) -> (usize, usize) {
        (N, N * LEN)
    }

    fn project_writev<'a>(&'a self, pieces: &mut WritevPieces<'a>) -> io::Result<()> {
        for index in self.indices {
            pieces.push(&SMALL_TRACKED_BLOCKS[index as usize][..LEN])?;
        }
        Ok(())
    }
}

async fn fill_unix_send_buffer(writer: &mut UnixStream) {
    // Timeout is the success signal: it establishes socket backpressure so the
    // next write/writev blocks and can be cancelled.
    loop {
        let buf = vec![0xAAu8; 65536];
        let result = timeout(Duration::from_millis(5), async {
            let (res, _buf) = writer.write(buf).await;
            res
        })
        .await;

        match result {
            Ok(Ok(_)) => {}
            Ok(Err(err)) => panic!("fill write failed: {err}"),
            Err(TimeoutError::Elapsed) => break,
            Err(TimeoutError::Runtime(err)) => {
                panic!("fill timeout runtime failed: {err}");
            }
        }
    }
}

fn tracked_chain<const N: usize>(
    segments: [Vec<u8>; N],
    drops: &Rc<Cell<usize>>,
) -> IoBuffReadOnlyVec<DropTrackedReadOnly, N> {
    let mut chain = IoBuffReadOnlyVec::new();
    for bytes in segments {
        chain
            .push(DropTrackedReadOnly::new(bytes, drops))
            .expect("tracked chain has enough capacity");
    }
    chain
}

fn read_chain<const N: usize>(segments: [usize; N]) -> IoBuffVecMut<N> {
    let mut chain = IoBuffVecMut::new();
    for len in segments {
        chain
            .push(IoBuffMut::new(0, len, 0).expect("read segment allocation failed"))
            .unwrap_or_else(|_| panic!("read chain has enough capacity"));
    }
    chain
}

fn spawn_stalling_writer(mut writer: UnixStream, chunk_len: usize) -> Rc<Cell<bool>> {
    // Sends optional priming bytes, then parks until released so read tests
    // control when a cancelled operation's original CQE can retire.
    let release = Rc::new(Cell::new(false));
    let release_flag = release.clone();
    Executor::spawn(async move {
        if chunk_len != 0 {
            let chunk = vec![0xDDu8; chunk_len];
            let (res, _chunk) = writer.write_all(chunk).await;
            let _ = res;
        }
        while !release_flag.get() {
            sleep(Duration::from_millis(5))
                .await
                .expect("stalling writer sleep failed");
        }
    })
    .expect("spawn stalling writer failed");
    release
}

fn small_tracked_chain<const N: usize, const LEN: usize>(
    counter: u8,
    start_index: usize,
) -> (IoBuffReadOnlyVec<SmallTrackedReadOnly<LEN>, N>, Vec<u8>) {
    // Build N static segments plus the expected flattened payload. `counter`
    // selects which static drop counter the segments increment.
    let mut chain = IoBuffReadOnlyVec::new();
    let mut expected = Vec::with_capacity(N * LEN);
    for i in 0..N {
        let index = start_index + i;
        let block = &SMALL_TRACKED_BLOCKS[index % SMALL_TRACKED_BLOCKS.len()];
        expected.extend_from_slice(&block[..LEN]);
        chain
            .push(SmallTrackedReadOnly {
                index: (index % SMALL_TRACKED_BLOCKS.len()) as u8,
                counter,
            })
            .expect("small tracked chain has enough capacity");
    }
    (chain, expected)
}

fn static_read_only_chain<const N: usize>() -> IoBuffReadOnlyVec<StaticReadOnly, N> {
    let mut chain = IoBuffReadOnlyVec::new();
    for _ in 0..N {
        chain
            .push(StaticReadOnly)
            .expect("static chain has enough capacity");
    }
    chain
}

fn assert_kernel_write_error(err: &io::Error) {
    assert!(
        matches!(
            err.raw_os_error(),
            Some(libc::EPIPE | libc::ECONNRESET | libc::ESHUTDOWN | libc::ENOTCONN)
        ) || matches!(
            err.kind(),
            io::ErrorKind::BrokenPipe
                | io::ErrorKind::ConnectionReset
                | io::ErrorKind::NotConnected
        ),
        "unexpected kernel write error: {err}"
    );
}

fn assert_nested_run_error(err: &io::Error) {
    assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
    assert_eq!(
        err.to_string(),
        "nested or reentrant Executor::run is not supported"
    );
}

#[test]
fn runtime_executor_constructs_with_custom_config() {
    let executor = new_executor_with(16, None);
    assert_eq!(executor.test_process_quota(), 16);
    assert_eq!(executor.test_cpu_affinity(), None);
}

#[cfg(target_os = "linux")]
#[test]
fn runtime_executor_preserves_cpu_affinity_config() {
    let current_cpu = unsafe { libc::sched_getcpu() };
    assert!(current_cpu >= 0, "sched_getcpu failed");

    let executor = new_executor_with(16, Some(current_cpu as usize));
    assert_eq!(executor.test_cpu_affinity(), Some(current_cpu as usize));
}

#[cfg(target_os = "linux")]
#[test]
fn runtime_executor_runs_with_cpu_affinity() {
    let current_cpu = unsafe { libc::sched_getcpu() };
    assert!(current_cpu >= 0, "sched_getcpu failed");

    let mut executor = new_executor_with(16, Some(current_cpu as usize));
    let observed_cpu = Rc::new(Cell::new(-1));
    let observed_cpu_flag = observed_cpu.clone();

    executor
        .run(async move {
            observed_cpu_flag.set(unsafe { libc::sched_getcpu() });
        })
        .expect("executor run failed");

    assert_eq!(observed_cpu.get(), current_cpu);
}

#[cfg(target_os = "linux")]
#[test]
fn runtime_executor_rejects_out_of_range_cpu_affinity_without_panic() {
    let max_cpu = 8 * std::mem::size_of::<libc::cpu_set_t>();
    let mut executor = new_executor_with(16, Some(max_cpu));

    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| executor.run(async {})));
    let run_result = result.expect("out-of-range CPU affinity should not panic");
    let err = run_result.expect_err("out-of-range CPU affinity should be rejected");
    assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
}

#[cfg(target_os = "linux")]
#[test]
fn runtime_executor_keeps_cpu_affinity_across_spawned_work() {
    let current_cpu = unsafe { libc::sched_getcpu() };
    assert!(current_cpu >= 0, "sched_getcpu failed");

    let mut executor = new_executor_with(16, Some(current_cpu as usize));
    let root_cpu = Rc::new(Cell::new(-1));
    let spawned_cpu = Rc::new(Cell::new(-1));
    let root_cpu_flag = root_cpu.clone();
    let spawned_cpu_flag = spawned_cpu.clone();

    executor
        .run(async move {
            root_cpu_flag.set(unsafe { libc::sched_getcpu() });
            Executor::spawn(async move {
                spawned_cpu_flag.set(unsafe { libc::sched_getcpu() });
            })
            .expect("spawn failed");
        })
        .expect("executor run failed");

    assert_eq!(root_cpu.get(), current_cpu);
    assert_eq!(spawned_cpu.get(), current_cpu);
}

#[test]
fn runtime_layout_probe() {
    println!(
        "layout CompletionState size={} align={}",
        std::mem::size_of::<CompletionState>(),
        std::mem::align_of::<CompletionState>()
    );
    println!(
        "layout TaskHeader size={} align={}",
        std::mem::size_of::<TaskHeader>(),
        std::mem::align_of::<TaskHeader>()
    );
    println!(
        "layout UnixStream size={}",
        std::mem::size_of::<flowio::net::unix::UnixStream>()
    );
}

#[cfg(not(miri))]
#[test]
fn runtime_real_completion_drain_preserves_reentrant_operation_ownership() {
    if std::env::var_os(COMPLETION_DRAIN_REENTRANCY_CHILD_ENV).is_none() {
        run_exact_test_child_with_watchdog(
            COMPLETION_DRAIN_REENTRANCY_CHILD_TEST,
            COMPLETION_DRAIN_REENTRANCY_CHILD_ENV,
            Duration::from_secs(8),
        );
        return;
    }

    assert_eq!(
        test_completion_drain_reentrancy().expect("real completion-drain re-entrancy probe failed"),
        CompletionDrainReentrancyReport::EXPECTED
    );
}

#[cfg(not(miri))]
#[test]
fn runtime_real_completion_drain_closes_payload_descriptor_without_ring_reentry() {
    if std::env::var_os(COMPLETION_DRAIN_DESCRIPTOR_CHILD_ENV).is_none() {
        run_exact_test_child_with_watchdog(
            COMPLETION_DRAIN_DESCRIPTOR_CHILD_TEST,
            COMPLETION_DRAIN_DESCRIPTOR_CHILD_ENV,
            Duration::from_secs(8),
        );
        return;
    }

    assert_eq!(
        test_completion_drain_descriptor_close()
            .expect("real completion-drain descriptor probe failed"),
        CompletionDrainDescriptorReport::EXPECTED
    );
}

#[test]
fn runtime_executor_runs_immediate_task() {
    let mut executor = new_executor();
    let completed = Rc::new(Cell::new(false));
    let completed_flag = completed.clone();

    executor
        .run(async move {
            completed_flag.set(true);
        })
        .expect("executor run failed");

    assert!(completed.get(), "immediate task did not complete");
}

#[test]
fn runtime_executor_rejects_nested_runs_and_preserves_outer_context() {
    let mut outer = new_executor();
    let mut normal_inner = new_executor();
    let mut panic_inner = new_executor();
    #[cfg(target_os = "linux")]
    let mut invalid_affinity_inner = {
        let max_cpu = 8 * std::mem::size_of::<libc::cpu_set_t>();
        new_executor_with(16, Some(max_cpu))
    };

    let nested_polled = Rc::new(Cell::new(false));
    let nested_drops = Rc::new(Cell::new(0));
    let panic_polled = Rc::new(Cell::new(false));
    let spawned_completed = Rc::new(Cell::new(false));
    let outer_completed = Rc::new(Cell::new(false));

    let nested_polled_probe = Rc::clone(&nested_polled);
    let nested_drops_probe = Rc::clone(&nested_drops);
    let panic_polled_probe = Rc::clone(&panic_polled);
    let spawned_completed_probe = Rc::clone(&spawned_completed);
    let outer_completed_probe = Rc::clone(&outer_completed);

    outer
        .run(async move {
            let nested_drop_guard = DropCounter(nested_drops_probe);
            let err = normal_inner
                .run(async move {
                    let _nested_drop_guard = nested_drop_guard;
                    nested_polled_probe.set(true);
                })
                .expect_err("nested run should be rejected");
            assert_nested_run_error(&err);

            #[cfg(target_os = "linux")]
            {
                let err = invalid_affinity_inner
                    .run(async {})
                    .expect_err("nested run should be rejected before affinity setup");
                assert_nested_run_error(&err);
            }

            let nested_panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                panic_inner.run(async move {
                    panic_polled_probe.set(true);
                    panic!("nested future must not be polled");
                })
            }));
            let err = nested_panic
                .expect("rejecting a nested run must not unwind")
                .expect_err("panic-capable nested run should be rejected");
            assert_nested_run_error(&err);

            let handle = Executor::spawn(async move {
                spawned_completed_probe.set(true);
            })
            .expect("outer spawn failed after nested rejection");
            sleep(Duration::ZERO)
                .await
                .expect("outer timer failed after nested rejection");
            handle
                .await
                .expect("outer spawned task was cancelled after nested rejection");
            outer_completed_probe.set(true);
        })
        .expect("outer executor failed after nested rejection");

    assert!(!nested_polled.get(), "rejected nested future was polled");
    assert_eq!(nested_drops.get(), 1, "rejected nested future leaked");
    assert!(
        !panic_polled.get(),
        "panic-capable nested future was polled"
    );
    assert!(
        spawned_completed.get(),
        "outer spawned task did not complete after nested rejection"
    );
    assert!(
        outer_completed.get(),
        "outer task did not complete after nested rejection"
    );
}

#[test]
fn runtime_executor_context_is_cleared_when_run_unwinds() {
    let mut panicking_executor = new_executor();
    let unwind = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        panicking_executor.run(async {
            panic!("executor context unwind probe");
        })
    }));
    assert!(unwind.is_err(), "executor task should have panicked");

    match Executor::try_spawn(async {}) {
        Err(TrySpawnError::NoExecutor { future }) => drop(future),
        Ok(_) => panic!("executor context remained installed after unwind"),
        Err(_) => panic!("unexpected try_spawn error after unwind"),
    }
    drop(panicking_executor);

    let mut next_executor = new_executor();
    let completed = Rc::new(Cell::new(false));
    let completed_probe = Rc::clone(&completed);
    next_executor
        .run(async move {
            sleep(Duration::ZERO)
                .await
                .expect("timer failed after prior executor unwind");
            let handle = Executor::spawn(async move {
                completed_probe.set(true);
            })
            .expect("spawn failed after prior executor unwind");
            handle
                .await
                .expect("spawned task was cancelled after prior executor unwind");
        })
        .expect("executor failed after prior executor unwind");
    assert!(
        completed.get(),
        "executor context was not restored after unwind"
    );
}

const ACTIVE_UNWIND_DROP_CHILD_ENV: &str = "FLOWIO_ACTIVE_UNWIND_DROP_CHILD";
const ACTIVE_UNWIND_DROP_CHILD_TEST: &str =
    "runtime_executor_drop_during_active_unwind_forgets_caught_task_panic_payload";

#[test]
fn runtime_executor_drop_during_active_unwind_forgets_caught_task_panic_payload() {
    if std::env::var_os(ACTIVE_UNWIND_DROP_CHILD_ENV).is_none() {
        run_exact_test_child_with_watchdog(
            ACTIVE_UNWIND_DROP_CHILD_TEST,
            ACTIVE_UNWIND_DROP_CHILD_ENV,
            Duration::from_secs(8),
        );
        return;
    }

    struct ShutdownPanicPayload(Arc<AtomicUsize>);

    impl Drop for ShutdownPanicPayload {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
            panic!("shutdown panic payload must not be dropped during unwind");
        }
    }

    struct PendingDropPanic {
        task_drops: Rc<Cell<usize>>,
        payload_drops: Arc<AtomicUsize>,
    }

    impl Future for PendingDropPanic {
        type Output = ();

        fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
            Poll::Pending
        }
    }

    impl Drop for PendingDropPanic {
        fn drop(&mut self) {
            self.task_drops.set(self.task_drops.get() + 1);
            std::panic::panic_any(ShutdownPanicPayload(Arc::clone(&self.payload_drops)));
        }
    }

    let task_drops = Rc::new(Cell::new(0));
    let payload_drops = Arc::new(AtomicUsize::new(0));
    let unwind = std::panic::catch_unwind(std::panic::AssertUnwindSafe({
        let task_drops = Rc::clone(&task_drops);
        let payload_drops = Arc::clone(&payload_drops);
        move || {
            let mut executor = new_executor();
            let err = executor
                .run(PendingDropPanic {
                    task_drops,
                    payload_drops,
                })
                .expect_err("pending task should leave the executor stalled");
            assert_eq!(err.kind(), io::ErrorKind::WouldBlock);
            panic!("outer unwind sentinel");
        }
    }))
    .expect_err("outer panic should survive executor destruction");
    let message = unwind
        .downcast_ref::<&str>()
        .copied()
        .or_else(|| unwind.downcast_ref::<String>().map(String::as_str));

    assert_eq!(message, Some("outer unwind sentinel"));
    assert_eq!(task_drops.get(), 1, "pending task destructor count");
    assert_eq!(
        payload_drops.load(Ordering::SeqCst),
        0,
        "retained shutdown panic payload was dropped"
    );
}

#[test]
fn runtime_nop_initial_submission_extracts_poll_context_once() {
    let mut executor = new_executor();

    executor
        .run(async move {
            poll_once_pending(Nop::new()).await;
        })
        .expect("executor run failed");

    #[cfg(debug_assertions)]
    assert_eq!(
        executor.last_stats().poll_context_extractions,
        1,
        "initial NOP submission should derive the validated context once"
    );
}

#[test]
fn runtime_executor_runs_nop_future() {
    let mut executor = new_executor();

    executor
        .run(async move {
            let value = Nop::new().await.expect("nop failed");
            assert_eq!(value, 0);
        })
        .expect("executor run failed");

    #[cfg(debug_assertions)]
    {
        let stats = executor.last_stats();
        assert!(stats.task_polls > 0, "task_polls not recorded");
        assert!(stats.sqe_submits > 0, "sqe_submits not recorded");
        assert!(stats.cqe_completions > 0, "cqe_completions not recorded");
        assert!(stats.waiter_wakes > 0, "waiter_wakes not recorded");
        assert_eq!(
            stats.poll_context_extractions, 2,
            "NOP submission and completion should each validate the poll context once"
        );
    }
    #[cfg(not(debug_assertions))]
    {
        let _ = executor;
    }
}

#[test]
fn runtime_io_wake_can_arm_timer_on_following_executor_pass() {
    let completed = Rc::new(Cell::new(false));
    let completed_probe = Rc::clone(&completed);
    let mut executor = new_executor();

    executor
        .run(async move {
            Nop::new().await.expect("nop failed");
            sleep(Duration::from_millis(1))
                .await
                .expect("post-I/O timer failed");
            completed_probe.set(true);
        })
        .expect("executor failed to process timer armed after I/O wake");

    assert!(completed.get(), "post-I/O timer task did not complete");
    #[cfg(debug_assertions)]
    assert_eq!(executor.last_stats().timer_expired, 1);
}

#[test]
fn runtime_nop_rejects_noop_waker_outside_run() {
    let mut nop = Nop::new();
    let mut cx = Context::from_waker(Waker::noop());

    assert!(matches!(
        Pin::new(&mut nop).poll(&mut cx),
        Poll::Ready(Err(err)) if err.kind() == io::ErrorKind::NotConnected
    ));
}

#[test]
fn runtime_nop_rejects_non_flowio_waker_inside_run() {
    let mut executor = new_executor();
    executor
        .run(async {
            let mut nop = Nop::new();
            let mut cx = Context::from_waker(Waker::noop());
            assert!(matches!(
                Pin::new(&mut nop).poll(&mut cx),
                Poll::Ready(Err(err)) if err.kind() == io::ErrorKind::NotConnected
            ));
        })
        .expect("executor run failed after non-FlowIO Waker rejection");
}

#[test]
fn runtime_read_rejects_initial_poll_outside_run_and_returns_buffer() {
    let (mut reader, _writer) = UnixStream::pair().expect("socketpair failed");
    let mut read = Box::pin(reader.read(vec![0u8; 1], 1));
    let mut cx = Context::from_waker(Waker::noop());

    match read.as_mut().poll(&mut cx) {
        Poll::Ready((Err(err), buffer)) => {
            assert_eq!(err.kind(), io::ErrorKind::NotConnected);
            assert_eq!(buffer.len(), 1);
        }
        Poll::Ready((Ok(_), _)) => panic!("read unexpectedly succeeded outside Executor::run"),
        Poll::Pending => panic!("read remained pending outside Executor::run"),
    }
}

#[test]
fn runtime_unsubmitted_validation_error_outside_run_returns_context_error_and_buffer() {
    let (mut reader, _writer) = UnixStream::pair().expect("socketpair failed");
    let mut read = Box::pin(reader.read(Vec::<u8>::new(), 1));
    let mut cx = Context::from_waker(Waker::noop());

    match read.as_mut().poll(&mut cx) {
        Poll::Ready((Err(err), buffer)) => {
            assert_eq!(err.kind(), io::ErrorKind::NotConnected);
            assert!(buffer.is_empty());
        }
        Poll::Ready((Ok(_), _)) => panic!("invalid read unexpectedly succeeded outside run"),
        Poll::Pending => panic!("invalid read remained pending outside run"),
    }
}

#[test]
fn runtime_unsubmitted_zero_length_stream_io_outside_run_returns_context_error_and_buffers() {
    let (mut writer, _reader) = UnixStream::pair().expect("socketpair failed");

    let mut read_buffer = Vec::with_capacity(8);
    read_buffer.extend_from_slice(b"HEAD");
    let read_ptr = read_buffer.as_ptr();
    let mut read = Box::pin(writer.read(read_buffer, 0));
    let mut cx = Context::from_waker(Waker::noop());

    match read.as_mut().poll(&mut cx) {
        Poll::Ready((Err(err), buffer)) => {
            assert_eq!(err.kind(), io::ErrorKind::NotConnected);
            assert_eq!(buffer, b"HEAD");
            assert_eq!(buffer.as_ptr(), read_ptr);
        }
        Poll::Ready((Ok(_), _)) => panic!("zero read unexpectedly succeeded outside run"),
        Poll::Pending => panic!("zero read remained pending outside run"),
    }
    drop(read);

    let write_buffer = Vec::with_capacity(1);
    let write_ptr = write_buffer.as_ptr();
    let mut write = Box::pin(writer.write(write_buffer));
    let mut cx = Context::from_waker(Waker::noop());

    match write.as_mut().poll(&mut cx) {
        Poll::Ready((Err(err), buffer)) => {
            assert_eq!(err.kind(), io::ErrorKind::NotConnected);
            assert!(buffer.is_empty());
            assert_eq!(buffer.as_ptr(), write_ptr);
        }
        Poll::Ready((Ok(_), _)) => panic!("zero write unexpectedly succeeded outside run"),
        Poll::Pending => panic!("zero write remained pending outside run"),
    }
    drop(write);

    let mut write = Box::pin(writer.write_all(Vec::<u8>::new()));
    let mut cx = Context::from_waker(Waker::noop());

    match write.as_mut().poll(&mut cx) {
        Poll::Ready((Err(err), buffer)) => {
            assert_eq!(err.kind(), io::ErrorKind::NotConnected);
            assert!(buffer.is_empty());
        }
        Poll::Ready((Ok(_), _)) => panic!("zero write unexpectedly succeeded outside run"),
        Poll::Pending => panic!("zero write remained pending outside run"),
    }
}

#[cfg(any(debug_assertions, feature = "test-support"))]
#[test]
fn runtime_untimed_wait_eintr_does_not_abort_blocked_read() {
    let mut executor = new_executor();
    let (writer, mut reader) = UnixStream::pair().expect("socketpair failed");
    let writer_fd = unsafe { libc::dup(writer.as_raw_fd()) };
    assert!(
        writer_fd >= 0,
        "dup writer fd failed: {}",
        io::Error::last_os_error()
    );
    let release_writer = Arc::new(AtomicBool::new(false));
    let writer_release = Arc::clone(&release_writer);

    let writer_thread = std::thread::spawn(move || {
        while !writer_release.load(Ordering::Acquire) {
            std::thread::yield_now();
        }
        std::thread::sleep(Duration::from_millis(20));
        write_all_raw_fd(writer_fd, b"wake");
        let rc = unsafe { libc::close(writer_fd) };
        assert_eq!(rc, 0, "close duplicated writer fd failed");
    });

    executor
        .run(async move {
            let _writer = writer;
            test_hooks::fail_next_ring_wait_errno(libc::EINTR);
            release_writer.store(true, Ordering::Release);
            let (result, buffer) = reader.read_exact(vec![0u8; 4], 4).await;
            assert_eq!(result.expect("read failed after EINTR"), 4);
            assert_eq!(&buffer, b"wake");
        })
        .expect("executor run should absorb untimed wait EINTR");

    writer_thread
        .join()
        .expect("delayed writer thread panicked");
    assert_eq!(
        test_hooks::ring_wait_failures_remaining(),
        0,
        "untimed wait EINTR hook was not consumed"
    );
}

#[test]
fn runtime_executor_drains_multiple_nop_completions() {
    const NOPS: usize = 8;

    let mut executor = new_executor_with(16, None);

    executor
        .run(async move {
            let mut handles = Vec::with_capacity(NOPS);
            for _ in 0..NOPS {
                handles.push(
                    Executor::spawn(async { Nop::new().await.expect("nop failed") })
                        .expect("spawn nop failed"),
                );
            }

            for handle in handles {
                assert_eq!(handle.await.expect("NOP task cancelled"), 0);
            }
        })
        .expect("executor run failed");

    #[cfg(debug_assertions)]
    {
        let stats = executor.last_stats();
        assert!(
            stats.cqe_completions >= NOPS,
            "not all NOP completions were observed"
        );
    }
    #[cfg(not(debug_assertions))]
    {
        let _ = executor;
    }
}

#[test]
#[cfg(not(miri))]
fn runtime_poll_io_budget_boundary_preserves_nop_completions() {
    const PROCESS_QUOTA: usize = 1;
    const NOPS: usize = 8;

    let (done_tx, done_rx) = std::sync::mpsc::channel();
    let worker = std::thread::spawn(move || {
        let mut executor = new_executor_with(PROCESS_QUOTA, None);

        executor
            .run(async move {
                let completed = BatchedNops::new(NOPS)
                    .await
                    .expect("batched NOPs should complete");
                assert_eq!(completed, NOPS);
            })
            .expect("executor run failed");

        #[cfg(debug_assertions)]
        {
            let stats = executor.last_stats();
            assert_eq!(stats.sqe_submits, NOPS);
            assert_eq!(stats.cqe_completions, NOPS);
        }

        done_tx.send(()).expect("done receiver dropped");
    });

    match done_rx.recv_timeout(Duration::from_secs(2)) {
        Ok(()) => worker.join().expect("boundary regression worker panicked"),
        Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
            panic!("budget-boundary NOP batch did not complete before timeout");
        }
        Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
            worker.join().expect("boundary regression worker panicked");
            panic!("boundary regression worker exited without reporting completion");
        }
    }
}

#[test]
fn runtime_nop_repoll_registers_latest_waiter() {
    run_cross_task_repoll(Nop::new());
}

#[test]
fn runtime_nop_waiter_owns_completed_parent_until_migration() {
    let result = run_parent_completion_before_waiter_migration(Nop::new(), Duration::ZERO)
        .expect("migrated NOP failed");
    assert_eq!(result, 0);
}

#[test]
fn runtime_nop_waiter_owns_parent_while_pending_future_is_not_dropped() {
    run_parent_completion_without_pending_future_drop(Nop::new(), Duration::ZERO);
}

#[test]
fn runtime_submitted_read_delays_foreign_context_error_until_cqe() {
    let mut foreign_executor = new_executor();
    let foreign_waker = capture_flowio_waker(&mut foreign_executor);
    let (mut reader, writer) = UnixStream::pair().expect("socketpair failed");
    let mut executor = new_executor();

    executor
        .run(async move {
            let mut read = Box::pin(reader.read(vec![0u8; 1], 1));
            let mut injected_foreign_poll = false;
            let (result, buffer) = poll_fn(|cx| {
                if !injected_foreign_poll {
                    assert!(read.as_mut().poll(cx).is_pending());

                    let mut foreign_cx = Context::from_waker(&foreign_waker);
                    assert!(
                        read.as_mut().poll(&mut foreign_cx).is_pending(),
                        "submitted rental operation returned its buffer before the CQE"
                    );

                    let byte = b"x";
                    let rc = unsafe {
                        libc::send(
                            writer.as_raw_fd(),
                            byte.as_ptr().cast(),
                            byte.len(),
                            libc::MSG_NOSIGNAL,
                        )
                    };
                    assert_eq!(rc, 1, "raw send failed: {}", io::Error::last_os_error());
                    injected_foreign_poll = true;
                    return Poll::Pending;
                }

                read.as_mut().poll(cx)
            })
            .await;

            assert_eq!(
                result
                    .expect_err("foreign-context read unexpectedly succeeded")
                    .kind(),
                io::ErrorKind::NotConnected
            );
            assert_eq!(buffer.len(), 1, "rental buffer was not returned");
        })
        .expect("origin executor failed after foreign-context read poll");
}

#[cfg(any(debug_assertions, feature = "test-support"))]
#[test]
fn runtime_submitted_read_foreign_waiter_routes_to_foreign_executor() {
    let drops = Rc::new(Cell::new(0));
    let read_drops = Rc::clone(&drops);
    let (mut reader, writer) = UnixStream::pair().expect("socketpair failed");
    let staged_read = Rc::new(RefCell::new(Some(Box::pin(async move {
        reader
            .read(DropTrackedReadWrite::zeroed(1, &read_drops), 1)
            .await
    }))));

    let mut origin = Executor::new_with_config(ExecutorConfig {
        reactor: ReactorConfig { ring_entries: 1 },
        ..ExecutorConfig::default()
    })
    .expect("failed to construct one-slot origin executor");

    let origin_read = Rc::clone(&staged_read);
    let err = origin
        .run(poll_fn(move |cx| {
            let mut slot = origin_read.borrow_mut();
            let read = slot.as_mut().expect("staged read missing");
            assert!(
                read.as_mut().poll(cx).is_pending(),
                "one-byte read completed before it was staged"
            );
            test_hooks::fail_next_ring_wait_errno(libc::EIO);
            Poll::Ready(())
        }))
        .expect_err("injected wait failure should stop the origin executor");
    assert_eq!(err.raw_os_error(), Some(libc::EIO));
    assert_eq!(test_hooks::ring_wait_failures_remaining(), 0);
    assert_eq!(drops.get(), 0, "submitted read buffer dropped before CQE");

    let mut foreign_read = staged_read
        .borrow_mut()
        .take()
        .expect("staged read was not retained");
    let foreign_polls = Rc::new(Cell::new(0));
    let observed_polls = Rc::clone(&foreign_polls);
    let output = Rc::new(RefCell::new(None));
    let output_slot = Rc::clone(&output);
    let mut foreign = new_executor();

    let err = foreign
        .run(poll_fn(move |cx| {
            observed_polls.set(observed_polls.get() + 1);
            match foreign_read.as_mut().poll(cx) {
                Poll::Pending => Poll::Pending,
                Poll::Ready(value) => {
                    *output_slot.borrow_mut() = Some(value);
                    Poll::Ready(())
                }
            }
        }))
        .expect_err("foreign waiter should remain live until the origin CQE");
    assert_eq!(err.kind(), io::ErrorKind::WouldBlock);
    assert_eq!(foreign_polls.get(), 1);
    assert!(output.borrow().is_none());
    assert_eq!(
        drops.get(),
        0,
        "foreign pending poll dropped the read buffer"
    );

    let byte = b"x";
    let sent = unsafe {
        libc::send(
            writer.as_raw_fd(),
            byte.as_ptr().cast(),
            byte.len(),
            libc::MSG_NOSIGNAL,
        )
    };
    assert_eq!(sent, 1, "raw send failed: {}", io::Error::last_os_error());

    origin
        .run(async {})
        .expect("origin executor failed while retiring the read CQE");
    assert_eq!(
        foreign_polls.get(),
        1,
        "origin executor polled the foreign waiter"
    );
    assert!(
        output.borrow().is_none(),
        "origin executor completed the foreign task"
    );
    assert_eq!(
        drops.get(),
        0,
        "completed read buffer released before foreign consumption"
    );

    #[cfg(debug_assertions)]
    let origin_completion_stats = origin.last_stats();
    #[cfg(debug_assertions)]
    assert_retained_stats_zero(
        origin_completion_stats,
        "origin completion run with foreign waiter",
    );

    // `starts_clean` is true here even though the completed operation still
    // owns its retained payload for the foreign waiter. A clean run must use a
    // fresh baseline rather than replaying the operation's earlier allocation.
    origin
        .run(async {})
        .expect("origin executor failed with completed foreign-waiter payload");
    assert_eq!(
        drops.get(),
        0,
        "clean origin run released the foreign waiter's completed buffer"
    );
    #[cfg(debug_assertions)]
    assert_retained_stats_zero(
        origin.last_stats(),
        "clean origin run with completed foreign-waiter payload",
    );

    foreign
        .run(async {})
        .expect("foreign executor failed while resuming its waiter");
    assert_eq!(
        foreign_polls.get(),
        2,
        "foreign waiter was not polled exactly once after notification"
    );
    let (result, buffer) = output
        .borrow_mut()
        .take()
        .expect("foreign waiter did not publish the read result");
    assert_eq!(
        result
            .expect_err("foreign-context read unexpectedly succeeded")
            .kind(),
        io::ErrorKind::NotConnected
    );
    drop(buffer);
    assert_eq!(drops.get(), 1, "read buffer was not returned exactly once");

    #[cfg(debug_assertions)]
    {
        assert_eq!(origin_completion_stats.waiter_wakes, 1);
        assert_eq!(
            origin_completion_stats.task_schedules, 0,
            "origin runtime was charged for the foreign waiter schedule"
        );
        assert_eq!(origin_completion_stats.task_allocs, 2);
        assert_eq!(
            origin_completion_stats.task_frees, origin_completion_stats.task_allocs,
            "origin task reference leaked"
        );

        let foreign_stats = foreign.last_stats();
        assert_eq!(foreign_stats.waiter_wakes, 0);
        assert_eq!(
            foreign_stats.task_schedules, 1,
            "foreign runtime did not own its waiter schedule"
        );
        assert_eq!(foreign_stats.task_allocs, 2);
        assert_eq!(
            foreign_stats.task_frees, foreign_stats.task_allocs,
            "foreign task reference leaked"
        );
    }

    origin
        .run(async {
            assert_eq!(Nop::new().await.expect("origin reuse NOP failed"), 0);
        })
        .expect("one-slot origin reactor was not reclaimed");
    #[cfg(debug_assertions)]
    assert_retained_stats_zero(
        origin.last_stats(),
        "origin reuse after foreign payload release",
    );
    foreign
        .run(async {
            assert_eq!(Nop::new().await.expect("foreign reuse NOP failed"), 0);
        })
        .expect("foreign executor was not reusable");
}

#[test]
fn runtime_nop_slot_op_pool_pressure_releases_slot() {
    let mut executor = Executor::new_with_config(ExecutorConfig {
        reactor: ReactorConfig { ring_entries: 1 },
        ..ExecutorConfig::default()
    })
    .expect("failed to construct one-slot executor");

    executor
        .run(async move {
            let (mut reader, writer) = UnixStream::pair().expect("socketpair failed");
            let mut pending_read = Box::pin(reader.read(vec![0u8; 1], 1));

            poll_fn(|cx| match pending_read.as_mut().poll(cx) {
                Poll::Pending => Poll::Ready(()),
                Poll::Ready(other) => panic!("one-byte read unexpectedly completed: {other:?}"),
            })
            .await;

            let mut slot = NopSlot::new();
            let err = slot
                .nop()
                .expect("slot future should be created")
                .await
                .expect_err("op-pool pressure should reject NOP");
            assert_eq!(err.kind(), io::ErrorKind::WouldBlock);

            let retry = slot
                .nop()
                .expect("slot should be reusable after op-pool pressure");
            drop(retry);

            let byte = b"n";
            let rc = unsafe {
                libc::send(
                    writer.as_raw_fd(),
                    byte.as_ptr() as *const libc::c_void,
                    byte.len(),
                    libc::MSG_NOSIGNAL,
                )
            };
            assert_eq!(rc, 1, "raw send failed: {}", io::Error::last_os_error());

            let (read_res, recv) = pending_read.await;
            assert_eq!(read_res.expect("held read failed"), 1);
            assert_eq!(&recv[..1], byte);

            let value = slot
                .nop()
                .expect("slot should be reusable after held read completes")
                .await
                .expect("retry nop failed");
            assert_eq!(value, 0);
        })
        .expect("executor run failed");
}

#[cfg(any(debug_assertions, feature = "test-support"))]
#[test]
fn runtime_nop_slot_submit_failure_releases_slot() {
    let mut executor = new_executor();

    executor
        .run(async move {
            let mut slot = NopSlot::new();

            test_hooks::fail_next_sqe_submit();
            let err = slot
                .nop()
                .expect("slot future should be created")
                .await
                .expect_err("forced submit failure should reject NOP");
            assert_eq!(err.kind(), io::ErrorKind::WouldBlock);

            let value = slot
                .nop()
                .expect("slot should be reusable after submit failure")
                .await
                .expect("retry nop failed");
            assert_eq!(value, 0);
        })
        .expect("executor run failed");
}

#[test]
fn runtime_executor_runs_spawned_task_and_drains() {
    let mut executor = new_executor();
    let completed = Rc::new(Cell::new(0usize));
    let spawned_completed = completed.clone();
    let initial_completed = completed.clone();

    executor
        .run(async move {
            Executor::spawn(async move {
                Nop::new().await.expect("spawned nop failed");
                spawned_completed.set(spawned_completed.get() + 1);
            })
            .expect("spawn failed");
            initial_completed.set(initial_completed.get() + 1);
        })
        .expect("executor run failed");

    assert_eq!(completed.get(), 2, "did not drain both tasks");
}

#[test]
fn runtime_run_stalled_task_returns_would_block_and_executor_drops() {
    let mut executor = new_executor();

    let err = executor
        .run(async move {
            Executor::spawn(std::future::pending::<()>()).expect("spawn pending task failed");
        })
        .expect_err("stalled live task should make run return WouldBlock");

    assert_eq!(err.kind(), io::ErrorKind::WouldBlock);
    drop(executor);
}

#[test]
fn runtime_stalled_task_resumes_on_next_run_after_external_wake() {
    let mut executor = new_executor();
    let release = Rc::new(Cell::new(false));
    let completed = Rc::new(Cell::new(false));
    let stored_waker = Rc::new(RefCell::new(None::<Waker>));

    let err = executor
        .run({
            let release = Rc::clone(&release);
            let completed = Rc::clone(&completed);
            let stored_waker = Rc::clone(&stored_waker);
            async move {
                Executor::spawn(ExternalWakeFuture {
                    release,
                    completed,
                    waker: stored_waker,
                })
                .expect("external-wake task spawn failed");
            }
        })
        .expect_err("parked task should stall the first run");
    assert_eq!(err.kind(), io::ErrorKind::WouldBlock);

    release.set(true);
    stored_waker
        .borrow()
        .as_ref()
        .expect("stalled task did not publish its waker")
        .wake_by_ref();

    let new_root_completed = Rc::new(Cell::new(false));
    let new_root_flag = Rc::clone(&new_root_completed);
    executor
        .run(async move {
            new_root_flag.set(true);
        })
        .expect("second run should drain old and new work");

    assert!(completed.get(), "stalled task did not resume");
    assert!(new_root_completed.get(), "new root did not run");
    drop(stored_waker.borrow_mut().take());
}

#[test]
fn runtime_executor_drop_cancels_escaped_pending_join_handle_once() {
    let mut executor = new_executor();
    let escaped = Rc::new(RefCell::new(None::<JoinHandle<()>>));
    let future_drops = Rc::new(Cell::new(0usize));

    let err = executor
        .run({
            let escaped = Rc::clone(&escaped);
            let future_drops = Rc::clone(&future_drops);
            async move {
                let handle = Executor::spawn(async move {
                    let _drop_counter = DropCounter(future_drops);
                    std::future::pending::<()>().await;
                })
                .expect("pending task spawn failed");
                *escaped.borrow_mut() = Some(handle);
            }
        })
        .expect_err("pending task should stall the run");
    assert_eq!(err.kind(), io::ErrorKind::WouldBlock);

    let mut handle = escaped
        .borrow_mut()
        .take()
        .expect("pending join handle did not escape");
    assert!(!handle.is_finished());
    drop(executor);

    assert_eq!(future_drops.get(), 1, "pending future dropped incorrectly");
    assert!(handle.is_finished());
    assert_eq!(
        poll_join_handle(&mut handle),
        Poll::Ready(Err(JoinError::Cancelled))
    );
}

#[test]
fn runtime_completed_join_handle_survives_executor_drop() {
    let mut executor = new_executor();
    let escaped = Rc::new(RefCell::new(None::<JoinHandle<usize>>));

    executor
        .run({
            let escaped = Rc::clone(&escaped);
            async move {
                *escaped.borrow_mut() =
                    Some(Executor::spawn(async { 73usize }).expect("value task spawn failed"));
            }
        })
        .expect("completed escaped handle should not keep run live");

    let mut handle = escaped
        .borrow_mut()
        .take()
        .expect("completed join handle did not escape");
    assert!(handle.is_finished());
    drop(executor);
    assert_eq!(poll_join_handle(&mut handle), Poll::Ready(Ok(73)));
}

#[test]
fn runtime_handle_drop_inside_foreign_executor_reclaims_origin_slot() {
    let mut first_executor = new_executor();
    let escaped = Rc::new(RefCell::new(None::<JoinHandle<usize>>));
    first_executor
        .run({
            let escaped = Rc::clone(&escaped);
            async move {
                *escaped.borrow_mut() =
                    Some(Executor::spawn(async { 5usize }).expect("value task spawn failed"));
            }
        })
        .expect("first executor run failed");
    let handle = escaped
        .borrow_mut()
        .take()
        .expect("join handle did not escape first executor");

    let mut second_executor = new_executor();
    second_executor
        .run(async move {
            drop(handle);
        })
        .expect("foreign executor handle drop failed");

    first_executor
        .run(async {})
        .expect("origin executor was corrupted by foreign-context drop");
    second_executor
        .run(async {})
        .expect("second executor was corrupted by foreign handle drop");
}

#[test]
fn runtime_completed_nop_dropped_after_run_releases_origin_slot() {
    let mut executor = Executor::new_with_config(ExecutorConfig {
        reactor: ReactorConfig { ring_entries: 1 },
        ..ExecutorConfig::default()
    })
    .expect("failed to construct one-slot executor");
    let staged = Rc::new(RefCell::new(None::<Nop>));

    executor
        .run(StageCompletedNop {
            nop: Some(Nop::new()),
            staged: Rc::clone(&staged),
            submitted: false,
        })
        .expect("staged NOP run failed");

    drop(
        staged
            .borrow_mut()
            .take()
            .expect("completed NOP did not escape"),
    );
    executor
        .run(async {
            assert_eq!(Nop::new().await.expect("reused NOP failed"), 0);
        })
        .expect("one-slot reactor was not reusable after outside-run drop");
}

#[test]
fn runtime_completed_nop_foreign_repoll_reclaims_origin_slot() {
    let mut origin = Executor::new_with_config(ExecutorConfig {
        reactor: ReactorConfig { ring_entries: 1 },
        ..ExecutorConfig::default()
    })
    .expect("failed to construct one-slot executor");
    let staged = Rc::new(RefCell::new(None::<Nop>));

    origin
        .run(StageCompletedNop {
            nop: Some(Nop::new()),
            staged: Rc::clone(&staged),
            submitted: false,
        })
        .expect("staged NOP run failed");
    let mut nop = staged
        .borrow_mut()
        .take()
        .expect("completed NOP did not escape");

    let mut foreign = new_executor();
    foreign
        .run(async move {
            let err = poll_fn(|cx| match Pin::new(&mut nop).poll(cx) {
                Poll::Ready(result) => Poll::Ready(result),
                Poll::Pending => panic!("completed foreign NOP remained pending"),
            })
            .await
            .expect_err("completed foreign NOP unexpectedly succeeded");
            assert_eq!(err.kind(), io::ErrorKind::NotConnected);
        })
        .expect("foreign executor failed while rejecting completed NOP");

    origin
        .run(async {
            assert_eq!(Nop::new().await.expect("reused NOP failed"), 0);
        })
        .expect("origin reactor slot was not reclaimed by foreign rejection");
}

#[test]
fn runtime_fired_sleep_rejects_outside_run_after_executor_drop() {
    let mut executor = new_executor();
    let staged = Rc::new(RefCell::new(None::<Sleep>));
    executor
        .run(StageFiredSleep {
            sleep: Some(sleep(Duration::from_millis(1))),
            staged: Rc::clone(&staged),
            armed: false,
        })
        .expect("staged sleep run failed");

    let mut fired = staged
        .borrow_mut()
        .take()
        .expect("fired sleep did not escape");
    drop(executor);

    let waker = Waker::noop();
    let mut cx = Context::from_waker(waker);
    assert!(matches!(
        Pin::new(&mut fired).poll(&mut cx),
        Poll::Ready(Err(err)) if err.kind() == io::ErrorKind::NotConnected
    ));
}

#[test]
fn runtime_fired_sleep_rejects_foreign_executor_and_reclaims_origin_entry() {
    let mut origin = new_executor();
    let staged = Rc::new(RefCell::new(None::<Sleep>));
    origin
        .run(StageFiredSleep {
            sleep: Some(sleep(Duration::from_millis(1))),
            staged: Rc::clone(&staged),
            armed: false,
        })
        .expect("staged sleep run failed");
    let mut fired = staged
        .borrow_mut()
        .take()
        .expect("fired sleep did not escape");

    let mut foreign = new_executor();
    foreign
        .run(async move {
            let err = poll_fn(|cx| match Pin::new(&mut fired).poll(cx) {
                Poll::Ready(result) => Poll::Ready(result),
                Poll::Pending => panic!("fired foreign timer remained pending"),
            })
            .await
            .expect_err("fired foreign timer unexpectedly succeeded");
            assert_eq!(err.kind(), io::ErrorKind::NotConnected);
        })
        .expect("foreign executor failed while rejecting fired timer");

    origin
        .run(async {})
        .expect("origin executor was corrupted by foreign timer cleanup");
}

#[test]
fn runtime_armed_sleep_rejects_foreign_flowio_waker() {
    let mut foreign = new_executor();
    let foreign_waker = capture_flowio_waker(&mut foreign);
    let mut origin = new_executor();

    origin
        .run(async move {
            let mut timer = sleep(Duration::from_secs(60));
            poll_fn(|cx| {
                assert!(Pin::new(&mut timer).poll(cx).is_pending());

                let mut foreign_cx = Context::from_waker(&foreign_waker);
                assert!(matches!(
                    Pin::new(&mut timer).poll(&mut foreign_cx),
                    Poll::Ready(Err(err)) if err.kind() == io::ErrorKind::NotConnected
                ));
                Poll::Ready(())
            })
            .await;
        })
        .expect("origin executor failed after armed timer rejection");
}

#[cfg(any(debug_assertions, feature = "test-support"))]
#[test]
fn runtime_cancelled_sleep_rejects_outside_run_after_executor_drop() {
    let mut executor = new_executor();
    let staged = Rc::new(RefCell::new(None::<Sleep>));
    let err = executor
        .run({
            let staged = Rc::clone(&staged);
            async move {
                let mut timer = sleep(Duration::from_secs(60));
                poll_fn(|cx| {
                    assert!(Pin::new(&mut timer).poll(cx).is_pending());
                    Poll::Ready(())
                })
                .await;
                *staged.borrow_mut() = Some(timer);
                test_hooks::fail_next_ring_wait_errno(libc::EIO);
            }
        })
        .expect_err("injected wait error should leave the timer armed");
    assert_eq!(err.raw_os_error(), Some(libc::EIO));

    let mut timer = staged
        .borrow_mut()
        .take()
        .expect("armed sleep did not escape");
    drop(executor);

    let waker = Waker::noop();
    let mut cx = Context::from_waker(waker);
    assert!(matches!(
        Pin::new(&mut timer).poll(&mut cx),
        Poll::Ready(Err(err)) if err.kind() == io::ErrorKind::NotConnected
    ));
}

#[cfg(any(debug_assertions, feature = "test-support"))]
#[test]
fn runtime_executor_drop_fallback_abandons_inflight_read_payload() {
    let mut executor = new_executor();
    let (mut reader, writer) = UnixStream::pair().expect("socketpair failed");
    let reader_fd = reader.as_raw_fd();
    let reader_identity = fd_identity(reader_fd).expect("reader fstat failed");
    let buffer_drops = Rc::new(Cell::new(0usize));
    let drops_flag = Rc::clone(&buffer_drops);

    let err = executor
        .run(async move {
            test_hooks::fail_next_ring_wait_errno(libc::EIO);
            let buffer = DropTrackedReadWrite::zeroed(64, &drops_flag);
            let (_result, _buffer) = reader.read(buffer, 64).await;
        })
        .expect_err("injected reactor error should end the run");
    assert_eq!(err.raw_os_error(), Some(libc::EIO));
    assert_eq!(buffer_drops.get(), 0, "in-flight buffer dropped early");

    test_hooks::force_next_reactor_shutdown_fallback();
    drop(executor);

    assert_eq!(
        test_hooks::reactor_shutdown_fallbacks_remaining(),
        0,
        "forced reactor fallback was not consumed"
    );
    assert_eq!(
        buffer_drops.get(),
        0,
        "ring-abandoned in-flight buffer was released without a target CQE"
    );
    match fd_identity(reader_fd) {
        Err(err) => assert_eq!(err.raw_os_error(), Some(libc::EBADF)),
        Ok(current_identity) => assert_ne!(
            current_identity, reader_identity,
            "reader descriptor stayed open after executor drop"
        ),
    }
    drop(writer);
}

#[cfg(any(debug_assertions, feature = "test-support"))]
#[test]
fn runtime_executor_drop_fallback_keeps_escaped_read_pending() {
    let mut executor = new_executor();
    let (mut reader, writer) = UnixStream::pair().expect("socketpair failed");
    let reader_fd = reader.as_raw_fd();
    let reader_identity = fd_identity(reader_fd).expect("reader fstat failed");
    let buffer_drops = Rc::new(Cell::new(0usize));
    let read_drops = Rc::clone(&buffer_drops);
    let staged = Rc::new(RefCell::new(Some(Box::pin(async move {
        reader
            .read(DropTrackedReadWrite::zeroed(64, &read_drops), 64)
            .await
    }))));

    let staged_for_run = Rc::clone(&staged);
    let err = executor
        .run(async move {
            poll_fn(|cx| {
                let mut slot = staged_for_run.borrow_mut();
                let read = slot.as_mut().expect("staged read missing");
                assert!(
                    read.as_mut().poll(cx).is_pending(),
                    "staged read completed before fallback"
                );
                Poll::Ready(())
            })
            .await;
            test_hooks::fail_next_ring_wait_errno(libc::EIO);
        })
        .expect_err("injected reactor error should end the run");
    assert_eq!(err.raw_os_error(), Some(libc::EIO));
    assert_eq!(buffer_drops.get(), 0, "in-flight buffer dropped early");

    test_hooks::force_next_reactor_shutdown_fallback();
    drop(executor);
    assert_eq!(
        test_hooks::reactor_shutdown_fallbacks_remaining(),
        0,
        "forced reactor fallback was not consumed"
    );

    let mut read = staged
        .borrow_mut()
        .take()
        .expect("escaped read disappeared");
    let waker = Waker::noop();
    let mut cx = Context::from_waker(waker);
    assert!(
        read.as_mut().poll(&mut cx).is_pending(),
        "ring-abandoned read fabricated completion and exposed its buffer"
    );
    drop(read);
    assert_eq!(
        buffer_drops.get(),
        0,
        "dropping an escaped ring-abandoned read released its buffer"
    );
    match fd_identity(reader_fd) {
        Err(err) => assert_eq!(err.raw_os_error(), Some(libc::EBADF)),
        Ok(current_identity) => assert_ne!(
            current_identity, reader_identity,
            "escaped reader descriptor stayed open after future drop"
        ),
    }
    drop(writer);
}

#[cfg(debug_assertions)]
#[test]
fn runtime_clean_runs_reset_generation_task_counters() {
    let mut executor = new_executor();
    executor.run(async {}).expect("first clean run failed");
    let first = executor.last_stats();
    assert_eq!(first.task_allocs, 1);
    assert_eq!(first.task_frees, 1);
    assert_eq!(first.task_slab_allocs, 1);
    assert_eq!(first.task_slab_frees, 0);

    executor.run(async {}).expect("second clean run failed");
    let second = executor.last_stats();
    assert_eq!(second.task_allocs, 1);
    assert_eq!(second.task_frees, 1);
    assert_eq!(second.task_slab_allocs, 0);
    assert_eq!(second.task_slab_frees, 0);
}

#[cfg(debug_assertions)]
#[test]
fn runtime_clean_runs_report_retained_payload_counters_per_run() {
    let mut executor = new_executor();
    executor
        .run(async {
            let (mut writer, _reader) = UnixStream::pair().expect("socketpair failed");
            let (result, payload) = writer.write(vec![0xA5; 8]).await;
            assert_eq!(result.expect("retained write failed"), payload.len());
        })
        .expect("first retained run failed");
    let first = executor.last_stats();
    assert_eq!(first.retained_pooled_allocs, 1);
    assert_eq!(first.retained_pooled_frees, 1);
    assert_eq!(first.retained_slab_allocs, 1);

    executor.run(async {}).expect("second clean run failed");
    assert_retained_stats_zero(executor.last_stats(), "second retained run");
}

#[cfg(debug_assertions)]
#[test]
fn runtime_clean_runs_report_vectored_scratch_counters_per_run() {
    let mut executor = new_executor();
    executor
        .run(async {
            let (mut writer, _reader) = UnixStream::pair().expect("socketpair failed");
            let chain = static_read_only_chain::<17>();
            let (result, _chain) = writer.writev(chain).await;
            assert_eq!(result.expect("17-segment writev failed"), 17);
        })
        .expect("first vectored run failed");
    let first = executor.last_stats();
    assert_eq!(first.writev_scratch_inline_allocs, 0);
    assert_eq!(first.writev_scratch_pooled_allocs, 1);
    assert_eq!(first.writev_scratch_pooled_frees, 1);
    assert_eq!(first.writev_scratch_slab_allocs, 1);

    executor.run(async {}).expect("second clean run failed");
    assert_retained_stats_zero(executor.last_stats(), "second vectored run");
}

#[cfg(any(debug_assertions, feature = "test-support"))]
#[test]
fn runtime_write_alloc_op_failure_returns_payload() {
    let mut executor = new_executor();

    executor
        .run(async move {
            let (mut writer, _reader) = UnixStream::pair().expect("socketpair failed");
            let drops = Rc::new(Cell::new(0));
            let tracked = DropTrackedReadOnly::new(b"alloc-op".to_vec(), &drops);

            test_hooks::fail_next_op_alloc();
            let (res, returned) = writer.write(tracked).await;
            let err = res.expect_err("forced alloc_op failure should return WouldBlock");
            assert_eq!(err.kind(), io::ErrorKind::WouldBlock);
            assert_eq!(drops.get(), 0, "payload dropped before caller return");
            drop(returned);
            assert_eq!(drops.get(), 1, "returned payload dropped exactly once");
        })
        .expect("executor run failed");
}

#[cfg(any(debug_assertions, feature = "test-support"))]
#[test]
fn runtime_write_submit_failure_returns_retained_payload() {
    let mut executor = new_executor();

    executor
        .run(async move {
            let (mut writer, _reader) = UnixStream::pair().expect("socketpair failed");
            let drops = Rc::new(Cell::new(0));
            let tracked = DropTrackedReadOnly::new(b"submit".to_vec(), &drops);

            test_hooks::fail_next_sqe_submit();
            let (res, returned) = writer.write(tracked).await;
            let err = res.expect_err("forced SQE submit failure should return WouldBlock");
            assert_eq!(err.kind(), io::ErrorKind::WouldBlock);
            assert_eq!(drops.get(), 0, "payload dropped before caller return");
            drop(returned);
            assert_eq!(drops.get(), 1, "returned payload dropped exactly once");
        })
        .expect("executor run failed");
}

#[cfg(any(debug_assertions, feature = "test-support"))]
#[test]
fn runtime_vectored_submit_failures_return_owners_and_reuse_one_op_slot() {
    let mut executor = new_one_slot_executor();

    executor
        .run(async move {
            let (mut writer, mut reader) = UnixStream::pair().expect("socketpair failed");

            let projected_drops = Rc::new(Cell::new(0));
            let projected = DropTrackedProjected::<2>::matching(&projected_drops);
            test_hooks::fail_next_sqe_submit();
            let (res, projected) = writer.writev_projected(projected).await;
            assert_eq!(
                res.expect_err("projected writev forced submit should fail")
                    .kind(),
                io::ErrorKind::WouldBlock
            );
            assert_eq!(projected.expected(), &[0, 1]);
            assert_eq!(projected_drops.get(), 0, "projected source dropped early");
            drop(projected);
            assert_eq!(projected_drops.get(), 1, "projected source dropped once");

            let mut pool = IoBuffPool::new(IoBuffPoolConfig {
                headroom: 0,
                payload: 8,
                tailroom: 0,
                objs_per_slab: 1,
            })
            .expect("pool config invalid");
            pool.init();

            let recv = IoBuffVecMut::<1>::from_array([pool
                .alloc()
                .expect("readv pool allocation failed")]);
            test_hooks::fail_next_sqe_submit();
            let (res, recv) = reader.readv(recv).await;
            assert_eq!(
                res.expect_err("readv forced submit should fail").kind(),
                io::ErrorKind::WouldBlock
            );
            assert_eq!(pool.live_slots_for_test(), 1, "readv lost its buffer");
            drop(recv);
            assert_eq!(pool.live_slots_for_test(), 0, "readv buffer not returned");

            let writev_drops = Rc::new(Cell::new(0));
            let chain = tracked_chain([b"writev".to_vec()], &writev_drops);
            test_hooks::fail_next_sqe_submit();
            let (res, chain) = writer.writev(chain).await;
            assert_eq!(
                res.expect_err("writev forced submit should fail").kind(),
                io::ErrorKind::WouldBlock
            );
            assert_eq!(writev_drops.get(), 0, "writev source dropped early");
            drop(chain);
            assert_eq!(writev_drops.get(), 1, "writev source dropped once");

            let writev_all_drops = Rc::new(Cell::new(0));
            let chain = tracked_chain([b"writev-all".to_vec()], &writev_all_drops);
            test_hooks::fail_next_sqe_submit();
            let (res, chain) = writer.writev_all(chain).await;
            assert_eq!(
                res.expect_err("writev_all forced submit should fail")
                    .kind(),
                io::ErrorKind::WouldBlock
            );
            assert_eq!(writev_all_drops.get(), 0, "writev_all source dropped early");
            drop(chain);
            assert_eq!(writev_all_drops.get(), 1, "writev_all source dropped once");

            let recv = IoBuffVecMut::<1>::from_array([pool
                .alloc()
                .expect("readv_exact pool allocation failed")]);
            test_hooks::fail_next_sqe_submit();
            let (res, recv) = reader.readv_exact(recv, 1).await;
            assert_eq!(
                res.expect_err("readv_exact forced submit should fail")
                    .kind(),
                io::ErrorKind::WouldBlock
            );
            assert_eq!(pool.live_slots_for_test(), 1, "readv_exact lost its buffer");
            drop(recv);
            assert_eq!(
                pool.live_slots_for_test(),
                0,
                "readv_exact buffer not returned"
            );

            assert_eq!(Nop::new().await.expect("one op slot was not reusable"), 0);
        })
        .expect("executor run failed");
}

#[cfg(debug_assertions)]
#[test]
fn runtime_direct_writev_callback_panics_preserve_sources_and_reuse_one_op_slot() {
    let mut executor = new_one_slot_executor();

    executor
        .run(async move {
            let (mut writer, _reader) = UnixStream::pair().expect("socketpair failed");

            let writev_calls = Rc::new(Cell::new(0));
            let writev_drops = Rc::new(Cell::new(0));
            let chain = callback_tracked_chain(b"writev-panic", &writev_calls, &writev_drops, true);
            let mut writev = Box::pin(writer.writev(chain));
            let unwind = poll_fn(|cx| {
                Poll::Ready(std::panic::catch_unwind(std::panic::AssertUnwindSafe(
                    || writev.as_mut().poll(cx),
                )))
            })
            .await;
            assert!(unwind.is_err(), "writev callback should unwind");
            assert_eq!(writev_calls.get(), 1, "writev callback count changed");
            assert_eq!(writev_drops.get(), 0, "writev source moved before panic");
            drop(writev);
            assert_eq!(writev_drops.get(), 1, "writev source dropped once");
            assert_eq!(
                Nop::new()
                    .await
                    .expect("writev panic left its op slot unavailable"),
                0
            );

            let writev_all_calls = Rc::new(Cell::new(0));
            let writev_all_drops = Rc::new(Cell::new(0));
            let chain = callback_tracked_chain(
                b"writev-all-panic",
                &writev_all_calls,
                &writev_all_drops,
                true,
            );
            let mut writev_all = Box::pin(writer.writev_all(chain));
            let unwind = poll_fn(|cx| {
                Poll::Ready(std::panic::catch_unwind(std::panic::AssertUnwindSafe(
                    || writev_all.as_mut().poll(cx),
                )))
            })
            .await;
            assert!(unwind.is_err(), "writev_all callback should unwind");
            assert_eq!(
                writev_all_calls.get(),
                1,
                "writev_all callback count changed"
            );
            assert_eq!(
                writev_all_drops.get(),
                0,
                "writev_all source moved before panic"
            );
            drop(writev_all);
            assert_eq!(writev_all_drops.get(), 1, "writev_all source dropped once");
            assert_eq!(
                Nop::new()
                    .await
                    .expect("writev_all panic left published or cancellable state"),
                0
            );
        })
        .expect("executor run failed");
}

#[cfg(any(debug_assertions, feature = "test-support"))]
#[test]
fn runtime_direct_writev_op_alloc_failure_preserves_callback_order() {
    let mut executor = new_one_slot_executor();

    executor
        .run(async move {
            let (mut writer, _reader) = UnixStream::pair().expect("socketpair failed");

            let writev_calls = Rc::new(Cell::new(0));
            let writev_drops = Rc::new(Cell::new(0));
            let chain = callback_tracked_chain(b"writev", &writev_calls, &writev_drops, false);
            test_hooks::fail_next_op_alloc();
            let (res, chain) = writer.writev(chain).await;
            assert_eq!(
                res.expect_err("writev forced op allocation should fail")
                    .kind(),
                io::ErrorKind::WouldBlock
            );
            assert_eq!(
                writev_calls.get(),
                1,
                "one-shot writev must invoke its callback before op allocation"
            );
            assert_eq!(writev_drops.get(), 0, "writev source dropped early");
            drop(chain);
            assert_eq!(writev_drops.get(), 1, "writev source dropped once");

            let writev_all_calls = Rc::new(Cell::new(0));
            let writev_all_drops = Rc::new(Cell::new(0));
            let chain =
                callback_tracked_chain(b"writev-all", &writev_all_calls, &writev_all_drops, false);
            test_hooks::fail_next_op_alloc();
            let (res, chain) = writer.writev_all(chain).await;
            assert_eq!(
                res.expect_err("writev_all forced op allocation should fail")
                    .kind(),
                io::ErrorKind::WouldBlock
            );
            assert_eq!(
                writev_all_calls.get(),
                0,
                "writev_all must allocate its op before invoking callbacks"
            );
            assert_eq!(writev_all_drops.get(), 0, "writev_all source dropped early");
            drop(chain);
            assert_eq!(writev_all_drops.get(), 1, "writev_all source dropped once");

            assert_eq!(
                Nop::new()
                    .await
                    .expect("forced failures leaked the op slot"),
                0
            );
        })
        .expect("executor run failed");
}

#[cfg(any(debug_assertions, feature = "test-support"))]
#[test]
fn runtime_readv_exact_op_alloc_failure_precedes_scratch_materialization() {
    let mut executor = new_one_slot_executor();

    executor
        .run(async move {
            let (mut reader, _writer) = UnixStream::pair().expect("socketpair failed");
            let recv = read_chain([8usize]);

            test_hooks::fail_next_op_alloc();
            let (res, recv) = reader.readv_exact(recv, 1).await;
            assert_eq!(
                res.expect_err("readv_exact forced op allocation should fail")
                    .kind(),
                io::ErrorKind::WouldBlock
            );
            assert_eq!(recv.segments(), 1, "readv_exact did not return its chain");
            drop(recv);

            assert_eq!(Nop::new().await.expect("readv_exact leaked the op slot"), 0);
        })
        .expect("executor run failed");

    #[cfg(debug_assertions)]
    {
        let stats = executor.last_stats();
        assert_eq!(
            stats.writev_scratch_inline_allocs, 0,
            "readv_exact allocated scratch before its forced op failure"
        );
        assert_eq!(
            stats.writev_scratch_pooled_allocs, 0,
            "readv_exact allocated sidecar scratch before its forced op failure"
        );
    }
}

#[cfg(any(debug_assertions, feature = "test-support"))]
#[test]
fn runtime_write_ring_submit_eintr_is_absorbed() {
    let mut executor = new_executor();

    executor
        .run(async move {
            let (mut writer, _reader) = UnixStream::pair().expect("socketpair failed");
            let drops = Rc::new(Cell::new(0));
            let tracked = DropTrackedReadOnly::new(b"eintr".to_vec(), &drops);

            test_hooks::fail_next_ring_submit_errno(libc::EINTR);
            let (res, returned) = writer.write(tracked).await;
            assert_eq!(res.expect("EINTR should be absorbed"), 5);
            assert_eq!(drops.get(), 0, "payload dropped before caller return");
            drop(returned);
            assert_eq!(drops.get(), 1, "returned payload dropped exactly once");
        })
        .expect("executor run failed");
    assert_eq!(
        test_hooks::ring_submit_failures_remaining(),
        0,
        "ring-submit EINTR hook was not consumed"
    );
}

#[cfg(any(debug_assertions, feature = "test-support"))]
#[test]
fn runtime_write_ring_submit_ebusy_is_absorbed() {
    let mut executor = new_executor();

    executor
        .run(async move {
            let (mut writer, _reader) = UnixStream::pair().expect("socketpair failed");
            let drops = Rc::new(Cell::new(0));
            let tracked = DropTrackedReadOnly::new(b"ebusy".to_vec(), &drops);

            test_hooks::fail_next_ring_submit_errno(libc::EBUSY);
            let (res, returned) = writer.write(tracked).await;
            assert_eq!(res.expect("EBUSY should be absorbed"), 5);
            assert_eq!(drops.get(), 0, "payload dropped before caller return");
            drop(returned);
            assert_eq!(drops.get(), 1, "returned payload dropped exactly once");
        })
        .expect("executor run failed");
    assert_eq!(
        test_hooks::ring_submit_failures_remaining(),
        0,
        "ring-submit EBUSY hook was not consumed"
    );
}

#[test]
fn runtime_op_pool_capacity_returns_would_block_and_payload() {
    let mut executor = Executor::new_with_config(ExecutorConfig {
        reactor: ReactorConfig { ring_entries: 1 },
        ..ExecutorConfig::default()
    })
    .expect("failed to construct one-slot executor");

    executor
        .run(async move {
            let (mut reader, mut writer) = UnixStream::pair().expect("socketpair failed");
            let mut pending_read = Box::pin(reader.read(vec![0u8; 1], 1));

            poll_fn(|cx| match pending_read.as_mut().poll(cx) {
                Poll::Pending => Poll::Ready(()),
                Poll::Ready(other) => panic!("one-byte read unexpectedly completed: {other:?}"),
            })
            .await;

            let drops = Rc::new(Cell::new(0));
            let tracked = DropTrackedReadOnly::new(b"cap".to_vec(), &drops);
            let (res, returned) = writer.write(tracked).await;
            let err = res.expect_err("op-pool capacity should reject second operation");
            assert_eq!(err.kind(), io::ErrorKind::WouldBlock);
            assert_eq!(drops.get(), 0, "payload dropped before caller return");
            drop(returned);
            assert_eq!(drops.get(), 1, "returned payload dropped exactly once");

            let byte = b"x";
            let rc = unsafe {
                libc::send(
                    writer.as_raw_fd(),
                    byte.as_ptr() as *const libc::c_void,
                    byte.len(),
                    libc::MSG_NOSIGNAL,
                )
            };
            assert_eq!(rc, 1, "raw send failed: {}", io::Error::last_os_error());

            let (read_res, recv) = pending_read.await;
            assert_eq!(read_res.expect("held read failed"), 1);
            assert_eq!(&recv[..1], byte);
        })
        .expect("executor run failed");
}

#[test]
fn runtime_kernel_error_write_completions_return_payloads_once() {
    let mut executor = new_executor();

    executor
        .run(async move {
            let (mut writer, reader) = UnixStream::pair().expect("socketpair failed");
            reader
                .shutdown(Shutdown::Read)
                .expect("reader shutdown read failed");

            let drops = Rc::new(Cell::new(0));
            let tracked = DropTrackedReadOnly::new(vec![0xA1; 64], &drops);
            let (res, returned) = writer.write(tracked).await;
            let err = res.expect_err("write to closed peer should fail");
            assert_kernel_write_error(&err);
            assert_eq!(drops.get(), 0, "write payload dropped before return");
            drop(returned);
            assert_eq!(drops.get(), 1, "write payload dropped exactly once");

            let (mut writer, reader) = UnixStream::pair().expect("socketpair failed");
            reader
                .shutdown(Shutdown::Read)
                .expect("reader shutdown read failed");

            let drops = Rc::new(Cell::new(0));
            let tracked = DropTrackedReadOnly::new(vec![0xA2; 64], &drops);
            let (res, returned) = writer.write_all(tracked).await;
            let err = res.expect_err("write_all to closed peer should fail");
            assert_kernel_write_error(&err);
            assert_eq!(drops.get(), 0, "write_all payload dropped before return");
            drop(returned);
            assert_eq!(drops.get(), 1, "write_all payload dropped exactly once");

            let (mut writer, reader) = UnixStream::pair().expect("socketpair failed");
            reader
                .shutdown(Shutdown::Read)
                .expect("reader shutdown read failed");

            let drops = Rc::new(Cell::new(0));
            let chain = tracked_chain([vec![0xA3; 32], vec![0xA4; 32]], &drops);
            let (res, returned) = writer.writev(chain).await;
            let err = res.expect_err("writev to closed peer should fail");
            assert_kernel_write_error(&err);
            assert_eq!(drops.get(), 0, "writev chain dropped before return");
            drop(returned);
            assert_eq!(drops.get(), 2, "writev chain dropped exactly once");
        })
        .expect("executor run failed");
}

#[test]
fn runtime_try_spawn_returns_join_handle() {
    let mut executor = new_executor();
    let observed = Rc::new(Cell::new(0usize));
    let observed_flag = observed.clone();

    executor
        .run(async move {
            let handle = match Executor::try_spawn(async { 42usize }) {
                Ok(handle) => handle,
                Err(_) => panic!("try_spawn failed inside Executor::run"),
            };
            observed_flag.set(handle.await.expect("spawned task cancelled"));
        })
        .expect("executor run failed");

    assert_eq!(observed.get(), 42);
}

#[test]
fn runtime_try_spawn_outside_run_returns_future() {
    fn assert_std_error<E: std::error::Error>() {}

    let drops = Rc::new(Cell::new(0usize));
    let polls = Rc::new(Cell::new(0usize));

    let future = RecoverableSpawnFuture::<0>::new(7, &drops, &polls);
    let err = match Executor::try_spawn(future) {
        Ok(_) => panic!("try_spawn should fail outside Executor::run"),
        Err(err @ TrySpawnError::NoExecutor { .. }) => err,
        Err(_) => panic!("try_spawn returned the wrong failure class"),
    };

    assert_std_error::<TrySpawnError<RecoverableSpawnFuture<0>>>();
    assert_eq!(
        format!("{err:?}"),
        r#"TrySpawnError { kind: "NoExecutor", future: "<returned>" }"#
    );
    assert_eq!(
        err.to_string(),
        "no executor is currently active on this thread"
    );

    let returned = err.into_future();
    assert_eq!(returned.id, 7);
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
fn runtime_try_spawn_task_too_large_returns_future() {
    let mut executor = new_executor();
    let drops = Rc::new(Cell::new(0usize));
    let polls = Rc::new(Cell::new(0usize));
    let returned_id = Rc::new(Cell::new(0usize));
    let drops_flag = drops.clone();
    let polls_flag = polls.clone();
    let returned_id_flag = returned_id.clone();

    executor
        .run(async move {
            let future = RecoverableSpawnFuture::<8192>::new(99, &drops_flag, &polls_flag);
            let returned = match Executor::try_spawn(future) {
                Ok(_) => panic!("oversized task should not spawn"),
                Err(TrySpawnError::TaskTooLarge { future }) => future,
                Err(_) => panic!("oversized task returned the wrong failure class"),
            };

            returned_id_flag.set(returned.id);
            assert_eq!(
                polls_flag.get(),
                0,
                "oversized rejected future must not be polled"
            );
            assert_eq!(
                drops_flag.get(),
                0,
                "oversized rejected future must be returned before being dropped"
            );
            drop(returned);
        })
        .expect("executor run failed");

    assert_eq!(returned_id.get(), 99);
    assert_eq!(polls.get(), 0);
    assert_eq!(drops.get(), 1);
}

#[test]
fn runtime_spawn_preserves_existing_error_mapping() {
    let drops = Rc::new(Cell::new(0usize));
    let polls = Rc::new(Cell::new(0usize));

    let err = match Executor::spawn(RecoverableSpawnFuture::<0>::new(1, &drops, &polls)) {
        Ok(_) => panic!("spawn should fail outside Executor::run"),
        Err(err) => err,
    };
    assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
    assert_eq!(polls.get(), 0);
    assert_eq!(
        drops.get(),
        1,
        "legacy spawn still consumes rejected futures"
    );

    let mut executor = new_executor();
    let oversized_drops = Rc::new(Cell::new(0usize));
    let oversized_polls = Rc::new(Cell::new(0usize));
    let oversized_drops_flag = oversized_drops.clone();
    let oversized_polls_flag = oversized_polls.clone();

    executor
        .run(async move {
            let err = match Executor::spawn(RecoverableSpawnFuture::<8192>::new(
                2,
                &oversized_drops_flag,
                &oversized_polls_flag,
            )) {
                Ok(_) => panic!("oversized spawn should fail"),
                Err(err) => err,
            };
            assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
            assert_eq!(oversized_polls_flag.get(), 0);
            assert_eq!(
                oversized_drops_flag.get(),
                1,
                "legacy spawn still consumes oversized rejected futures"
            );
        })
        .expect("executor run failed");

    assert_eq!(oversized_drops.get(), 1);
    assert_eq!(oversized_polls.get(), 0);
}

#[test]
fn runtime_sleep_completes() {
    let mut executor = new_executor();
    let completed = Rc::new(Cell::new(false));
    let completed_flag = completed.clone();

    executor
        .run(async move {
            sleep(Duration::from_millis(5)).await.expect("sleep failed");
            completed_flag.set(true);
        })
        .expect("executor run failed");

    assert!(completed.get(), "sleep did not complete");
}

#[test]
fn runtime_sleep_rejects_zero_and_nonzero_polls_outside_run() {
    for duration in [Duration::ZERO, Duration::from_millis(1)] {
        let mut timer = sleep(duration);
        let mut cx = Context::from_waker(Waker::noop());
        assert!(matches!(
            Pin::new(&mut timer).poll(&mut cx),
            Poll::Ready(Err(err)) if err.kind() == io::ErrorKind::NotConnected
        ));
    }
}

#[test]
fn runtime_sleep_repoll_registers_latest_waiter() {
    run_cross_task_repoll(sleep(Duration::from_millis(5)));
}

#[test]
fn runtime_sleep_waiter_owns_completed_parent_until_migration() {
    run_parent_completion_before_waiter_migration(
        sleep(Duration::from_millis(1)),
        Duration::from_millis(5),
    )
    .expect("migrated sleep failed");
}

#[test]
fn runtime_sleep_waiter_owns_parent_while_pending_future_is_not_dropped() {
    run_parent_completion_without_pending_future_drop(
        sleep(Duration::from_millis(1)),
        Duration::from_millis(5),
    );
}

#[test]
fn runtime_sleep_zero_completes_without_timer_wake() {
    let mut executor = new_executor();
    let completed = Rc::new(Cell::new(false));
    let completed_flag = completed.clone();

    executor
        .run(async move {
            sleep(Duration::ZERO).await.expect("zero sleep failed");
            completed_flag.set(true);
        })
        .expect("executor run failed");

    assert!(completed.get(), "zero sleep did not complete");

    #[cfg(debug_assertions)]
    {
        let stats = executor.last_stats();
        assert_eq!(stats.timer_expired, 0, "zero sleep armed the timer wheel");
        assert_eq!(stats.waiter_wakes, 0, "zero sleep needed a timer wake");
    }
}

#[cfg(all(target_os = "linux", not(miri)))]
#[test]
fn runtime_signal_interrupt_does_not_abort_wait() {
    const SLEEP_TARGET: Duration = Duration::from_millis(80);
    const MIN_ELAPSED: Duration = Duration::from_millis(60);
    const MAX_ELAPSED: Duration = SLEEP_TARGET.saturating_mul(2);
    const MAX_SIGNALS: usize = 64;
    const SIGNAL_INTERVAL: Duration = Duration::from_millis(2);

    let _signal_lock = SIGNAL_TEST_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let _signal_guard = SignalHandlerGuard::install(libc::SIGUSR1);

    let target_thread = unsafe { libc::pthread_self() };
    let armed = Arc::new(AtomicBool::new(false));
    let done = Arc::new(AtomicBool::new(false));
    let sent = Arc::new(AtomicUsize::new(0));
    let sender_armed = Arc::clone(&armed);
    let sender_done = Arc::clone(&done);
    let sender_sent = Arc::clone(&sent);

    let sender = std::thread::spawn(move || {
        while !sender_armed.load(Ordering::Acquire) {
            std::thread::yield_now();
        }

        std::thread::sleep(SIGNAL_INTERVAL);
        while !sender_done.load(Ordering::Acquire)
            && sender_sent.load(Ordering::Relaxed) < MAX_SIGNALS
        {
            let rc = unsafe { libc::pthread_kill(target_thread, libc::SIGUSR1) };
            assert_eq!(rc, 0, "pthread_kill failed");
            sender_sent.fetch_add(1, Ordering::Relaxed);
            std::thread::sleep(SIGNAL_INTERVAL);
        }
    });

    let mut executor = new_executor();
    let observed = Rc::new(RefCell::new(None));
    let observed_flag = Rc::clone(&observed);
    let run_result = executor.run({
        let armed = Arc::clone(&armed);
        async move {
            let start = Instant::now();
            armed.store(true, Ordering::Release);
            sleep(SLEEP_TARGET)
                .await
                .expect("sleep interrupted by signal");
            *observed_flag.borrow_mut() = Some(start.elapsed());
        }
    });

    done.store(true, Ordering::Release);
    sender.join().expect("signal sender panicked");
    run_result.expect("executor run should absorb signal interruptions");

    assert!(
        sent.load(Ordering::Relaxed) > 0,
        "test did not deliver a signal"
    );
    let elapsed = observed
        .borrow()
        .expect("sleep did not record completion duration");
    assert!(
        elapsed >= MIN_ELAPSED,
        "sleep completed implausibly early after signal: {elapsed:?}"
    );
    assert!(
        elapsed < MAX_ELAPSED,
        "signal interruptions likely restarted the full wait timeout: {elapsed:?}"
    );
}

#[test]
fn runtime_sleep_ordering() {
    let mut executor = new_executor();
    let order = Rc::new(RefCell::new(Vec::new()));
    let order_first = order.clone();
    let order_second = order.clone();

    executor
        .run(async move {
            Executor::spawn(async move {
                sleep(Duration::from_millis(5)).await.expect("sleep failed");
                order_first.borrow_mut().push(1usize);
            })
            .expect("spawn failed");

            Executor::spawn(async move {
                sleep(Duration::from_millis(20))
                    .await
                    .expect("sleep failed");
                order_second.borrow_mut().push(2usize);
            })
            .expect("spawn failed");
        })
        .expect("executor run failed");

    assert_eq!(&*order.borrow(), &[1usize, 2usize]);
}

#[test]
fn runtime_sleep_ordering_across_cascade_boundary() {
    let mut executor = new_executor_with(1, None);
    let order = Rc::new(RefCell::new(Vec::new()));
    let order_a = order.clone();
    let order_b = order.clone();
    let order_c = order.clone();

    executor
        .run(async move {
            Executor::spawn(async move {
                sleep(Duration::from_millis(260))
                    .await
                    .expect("sleep failed");
                order_a.borrow_mut().push(1usize);
            })
            .expect("spawn failed");

            Executor::spawn(async move {
                sleep(Duration::from_millis(261))
                    .await
                    .expect("sleep failed");
                order_b.borrow_mut().push(2usize);
            })
            .expect("spawn failed");

            Executor::spawn(async move {
                sleep(Duration::from_millis(262))
                    .await
                    .expect("sleep failed");
                order_c.borrow_mut().push(3usize);
            })
            .expect("spawn failed");
        })
        .expect("executor run failed");

    assert_eq!(&*order.borrow(), &[1usize, 2usize, 3usize]);
}

#[test]
fn runtime_sleep_uses_fresh_tick_after_idle_gap() {
    const SLEEP_TARGET: Duration = Duration::from_millis(5);

    let mut executor = new_executor();
    let observed = Rc::new(Cell::new(Duration::ZERO));
    let observed_flag = observed.clone();

    executor
        .run(async move {
            std::thread::sleep(Duration::from_millis(20));
            let start = Instant::now();
            sleep(SLEEP_TARGET).await.expect("sleep failed");
            observed_flag.set(start.elapsed());
        })
        .expect("executor run failed");

    assert!(
        observed.get() >= SLEEP_TARGET,
        "sleep completed too early after idle gap: {:?}",
        observed.get()
    );
}

#[test]
fn runtime_relative_sleep_samples_after_same_pass_cpu_delay() {
    const CPU_DELAY: Duration = Duration::from_millis(40);
    const SLEEP_TARGET: Duration = Duration::from_millis(20);
    const MIN_ELAPSED: Duration = Duration::from_millis(15);

    let mut executor = new_executor();
    let observed = Rc::new(Cell::new(Duration::ZERO));
    let observed_flag = observed.clone();

    executor
        .run(async move {
            let mut anchor = Box::pin(sleep(Duration::from_millis(200)));
            let mut target = Box::pin(sleep(SLEEP_TARGET));
            let mut target_start = None;

            let (result, elapsed) = poll_fn(|cx| {
                if target_start.is_none() {
                    assert!(
                        anchor.as_mut().poll(cx).is_pending(),
                        "anchor sleep should arm before the CPU delay"
                    );
                    std::thread::sleep(CPU_DELAY);
                    target_start = Some(Instant::now());
                }

                match target.as_mut().poll(cx) {
                    Poll::Pending => Poll::Pending,
                    Poll::Ready(result) => {
                        let elapsed = target_start
                            .expect("target start should be recorded")
                            .elapsed();
                        Poll::Ready((result, elapsed))
                    }
                }
            })
            .await;
            result.expect("relative sleep failed");
            drop(anchor);
            observed_flag.set(elapsed);
        })
        .expect("executor run failed");

    assert!(
        observed.get() >= MIN_ELAPSED,
        "relative sleep completed too early after same-pass CPU delay: {:?}",
        observed.get()
    );
}

#[test]
fn runtime_sleep_until_uses_fresh_tick_after_idle_gap() {
    const SLEEP_TARGET: Duration = Duration::from_millis(5);

    let mut executor = new_executor();
    let observed = Rc::new(Cell::new(Duration::ZERO));
    let observed_flag = observed.clone();

    executor
        .run(async move {
            std::thread::sleep(Duration::from_millis(20));
            let start = Instant::now();
            let deadline = start + SLEEP_TARGET;
            sleep_until(deadline).await.expect("sleep_until failed");
            observed_flag.set(start.elapsed());
        })
        .expect("executor run failed");

    assert!(
        observed.get() >= SLEEP_TARGET,
        "sleep_until completed too early after idle gap: {:?}",
        observed.get()
    );
}

#[test]
fn runtime_sleep_can_be_cancelled_by_drop() {
    let mut executor = new_executor();
    let completed = Rc::new(Cell::new(false));
    let completed_flag = completed.clone();

    executor
        .run(async move {
            let sleeper = sleep(Duration::from_millis(25));
            drop(sleeper);
            sleep(Duration::from_millis(5))
                .await
                .expect("follow-up sleep failed");
            completed_flag.set(true);
        })
        .expect("executor run failed");

    assert!(completed.get(), "follow-up sleep did not complete");
}

#[test]
fn runtime_sleep_drop_after_fire_before_poll_reclaims_timer() {
    let mut executor = new_executor();
    let completed = Rc::new(Cell::new(false));
    let completed_flag = completed.clone();

    executor
        .run(async move {
            let mut sleeper = Box::pin(sleep(Duration::from_millis(1)));
            let first_poll =
                std::future::poll_fn(|cx| Poll::Ready(Future::poll(sleeper.as_mut(), cx))).await;
            assert!(
                matches!(first_poll, Poll::Pending),
                "sleep should arm before completing"
            );

            sleep(Duration::from_millis(20))
                .await
                .expect("driver sleep failed");
            drop(sleeper);

            sleep(Duration::from_millis(1))
                .await
                .expect("follow-up sleep failed");
            completed_flag.set(true);
        })
        .expect("executor run failed");

    assert!(
        completed.get(),
        "follow-up sleep did not complete after dropping fired sleep"
    );

    #[cfg(debug_assertions)]
    assert!(
        executor.last_stats().timer_expired >= 3,
        "fired sleep was not observed by the timer wheel before drop"
    );
}

#[test]
fn runtime_timeout_completes_before_deadline() {
    let mut executor = new_executor();
    let completed = Rc::new(Cell::new(false));
    let completed_flag = completed.clone();

    executor
        .run(async move {
            let result = timeout(Duration::from_millis(20), async {
                sleep(Duration::from_millis(5))
                    .await
                    .expect("nested sleep failed");
                7usize
            })
            .await;
            assert_eq!(result.ok(), Some(7usize));
            completed_flag.set(true);
        })
        .expect("executor run failed");

    assert!(completed.get(), "timeout success path did not complete");
}

#[test]
fn timeout_error_formats_and_exposes_runtime_source() {
    let elapsed = TimeoutError::Elapsed;
    assert_eq!(elapsed.to_string(), "runtime timer elapsed");
    assert!(std::error::Error::source(&elapsed).is_none());

    let runtime = TimeoutError::Runtime(io::Error::new(
        io::ErrorKind::OutOfMemory,
        "timer pool exhausted",
    ));
    assert_eq!(
        runtime.to_string(),
        "runtime timer failed: timer pool exhausted"
    );
    let source = std::error::Error::source(&runtime).expect("runtime error source missing");
    let source = source
        .downcast_ref::<io::Error>()
        .expect("runtime error source should remain io::Error");
    assert_eq!(source.kind(), io::ErrorKind::OutOfMemory);
}

#[test]
fn runtime_timeout_expires() {
    let mut executor = new_executor();
    let timed_out = Rc::new(Cell::new(false));
    let timed_out_flag = timed_out.clone();

    executor
        .run(async move {
            let result = timeout(Duration::from_millis(5), async {
                let _ = sleep(Duration::from_millis(20)).await;
                11usize
            })
            .await;
            assert!(
                matches!(result, Err(TimeoutError::Elapsed)),
                "timeout should report deadline expiry"
            );
            timed_out_flag.set(true);
        })
        .expect("executor run failed");

    assert!(timed_out.get(), "timeout expiry path did not run");
}

#[test]
fn runtime_timeout_preserves_timer_allocation_failure() {
    let mut executor = new_executor();

    executor
        .run(async {
            test_hooks::fail_next_timer_alloc();
            let sleep_err = sleep(Duration::from_secs(1))
                .await
                .expect_err("injected sleep allocation failure should surface");
            assert_eq!(sleep_err.kind(), io::ErrorKind::OutOfMemory);

            test_hooks::fail_next_timer_alloc();
            let timeout_err = timeout(Duration::from_secs(1), std::future::pending::<()>())
                .await
                .expect_err("timeout should preserve timer allocation failure");
            match timeout_err {
                TimeoutError::Runtime(err) => {
                    assert_eq!(err.kind(), io::ErrorKind::OutOfMemory);
                }
                TimeoutError::Elapsed => {
                    panic!("timer allocation failure was misreported as deadline expiry");
                }
            }
        })
        .expect("executor run failed");
}

#[test]
fn runtime_timeout_at_completes() {
    let mut executor = new_executor();
    let completed = Rc::new(Cell::new(false));
    let completed_flag = completed.clone();

    executor
        .run(async move {
            let deadline = Instant::now() + Duration::from_millis(20);
            let result = timeout_at(deadline, async {
                sleep(Duration::from_millis(5))
                    .await
                    .expect("nested sleep failed");
                13usize
            })
            .await;
            assert_eq!(result.ok(), Some(13usize));
            completed_flag.set(true);
        })
        .expect("executor run failed");

    assert!(completed.get(), "timeout_at path did not complete");
}

#[test]
fn runtime_nop_slot_can_be_reused() {
    let mut executor = new_executor();

    executor
        .run(async move {
            let mut slot = NopSlot::new();

            let nop1 = slot.nop().expect("first slot nop failed");
            nop1.await.expect("first nop failed");

            let nop2 = slot.nop().expect("second slot nop failed");
            nop2.await.expect("second nop failed");
        })
        .expect("executor run failed");
}

/// Multiple concurrent spawned tasks performing I/O simultaneously.
/// Validates that the pointer-based TLS context derives the correct task
/// identity from each waker across interleaved task polls.
#[test]
fn runtime_concurrent_io_tasks() {
    let mut executor = new_executor();
    let num_pairs = 4;
    let rounds = 50;
    let msg_size = 64;
    let completed = Rc::new(Cell::new(0usize));
    let completed_flag = completed.clone();

    executor
        .run(async move {
            for _ in 0..num_pairs {
                let done = completed_flag.clone();
                let (mut pinger, mut ponger) = UnixStream::pair().expect("socketpair failed");

                Executor::spawn(async move {
                    for _ in 0..rounds {
                        let buf = vec![0u8; msg_size];
                        let (res, buf) = ponger.read_exact(buf, msg_size).await;
                        res.expect("ponger read failed");
                        let (res, _) = ponger.write_all(buf).await;
                        res.expect("ponger write failed");
                    }
                })
                .expect("spawn ponger failed");

                Executor::spawn(async move {
                    let mut data = vec![0xAAu8; msg_size];
                    for _ in 0..rounds {
                        let (res, buf) = pinger.write_all(data).await;
                        res.expect("pinger write failed");
                        data = buf;
                        let recv = vec![0u8; msg_size];
                        let (res, buf) = pinger.read_exact(recv, msg_size).await;
                        res.expect("pinger read failed");
                        assert_eq!(buf[0], 0xAA);
                    }
                    done.set(done.get() + 1);
                })
                .expect("spawn pinger failed");
            }
        })
        .expect("executor run failed");

    assert_eq!(
        completed.get(),
        num_pairs,
        "not all concurrent pairs completed"
    );
}

/// JoinHandle returns the spawned task's result when awaited.
#[test]
fn runtime_join_handle_returns_value() {
    let mut executor = new_executor();

    executor
        .run(async move {
            let handle = Executor::spawn(async { 42usize }).expect("spawn failed");
            let value = handle.await.expect("spawned task cancelled");
            assert_eq!(value, 42);
        })
        .expect("executor run failed");
}

/// JoinHandle works with non-trivial return types.
#[test]
fn runtime_join_handle_returns_string() {
    let mut executor = new_executor();

    executor
        .run(async move {
            let handle = Executor::spawn(async { String::from("hello from spawned task") })
                .expect("spawn failed");
            let value = handle.await.expect("spawned task cancelled");
            assert_eq!(value, "hello from spawned task");
        })
        .expect("executor run failed");
}

/// Dropping a JoinHandle without awaiting detaches the task — it still runs.
#[test]
fn runtime_join_handle_detach_on_drop() {
    let mut executor = new_executor();
    let completed = Rc::new(Cell::new(false));
    let completed_flag = completed.clone();

    executor
        .run(async move {
            let _handle = Executor::spawn(async move {
                completed_flag.set(true);
            })
            .expect("spawn failed");
            // handle dropped here — task should still complete
        })
        .expect("executor run failed");

    assert!(completed.get(), "detached task should still complete");
}

/// Multiple JoinHandles can be awaited concurrently.
#[test]
fn runtime_join_handle_multiple_concurrent() {
    let mut executor = new_executor();

    executor
        .run(async move {
            let h1 = Executor::spawn(async { 10usize }).expect("spawn 1 failed");
            let h2 = Executor::spawn(async { 20usize }).expect("spawn 2 failed");
            let h3 = Executor::spawn(async { 30usize }).expect("spawn 3 failed");

            let v3 = h3.await.expect("spawn 3 cancelled");
            let v1 = h1.await.expect("spawn 1 cancelled");
            let v2 = h2.await.expect("spawn 2 cancelled");

            assert_eq!(v1 + v2 + v3, 60);
        })
        .expect("executor run failed");
}

/// JoinHandle works with async tasks that perform I/O before returning.
#[test]
fn runtime_join_handle_with_io() {
    let mut executor = new_executor();

    executor
        .run(async move {
            let (mut left, mut right) = UnixStream::pair().expect("socketpair failed");

            let writer = Executor::spawn(async move {
                let (res, _buf) = left.write_all(b"join-test".to_vec()).await;
                res.expect("write failed");
                42usize
            })
            .expect("spawn writer failed");

            let reader = Executor::spawn(async move {
                let buf = vec![0u8; 9];
                let (res, buf) = right.read_exact(buf, 9).await;
                res.expect("read failed");
                buf
            })
            .expect("spawn reader failed");

            let write_result = writer.await.expect("writer task cancelled");
            let read_result = reader.await.expect("reader task cancelled");

            assert_eq!(write_result, 42);
            assert_eq!(&read_result[..], b"join-test");
        })
        .expect("executor run failed");
}

/// JoinHandle::is_finished() reports completion status without consuming.
#[test]
fn runtime_join_handle_is_finished() {
    let mut executor = new_executor();

    executor
        .run(async move {
            let handle = Executor::spawn(async {
                sleep(Duration::from_millis(5)).await.expect("sleep failed");
                99usize
            })
            .expect("spawn failed");

            assert!(!handle.is_finished(), "should not be finished immediately");

            let value = handle.await.expect("spawned task cancelled");
            assert_eq!(value, 99);
        })
        .expect("executor run failed");
}

/// Dropping an I/O future while an SQE is in-flight triggers ASYNC_CANCEL
/// and safely frees the CompletionState when the CQE arrives.
#[test]
fn runtime_cancel_in_flight_read_on_drop() {
    let mut executor = new_executor();

    executor
        .run(async move {
            let (mut left, _right) = UnixStream::pair().expect("socketpair failed");

            // Start a read that will never complete (no writer).
            // Wrap in timeout so it gets cancelled after 10ms.
            let result = timeout(Duration::from_millis(10), async {
                let buf = vec![0u8; 64];
                let (res, _buf) = left.read(buf, 64).await;
                res
            })
            .await;

            // Timeout should fire — the inner read future is dropped while in-flight.
            assert!(
                matches!(result, Err(TimeoutError::Elapsed)),
                "should have timed out: {result:?}"
            );

            // Executor continues to work after the cancel.
            sleep(Duration::from_millis(5))
                .await
                .expect("post-cancel sleep failed");
        })
        .expect("executor run failed");
}

/// A read cancelled by timeout must retain its payload until the original CQE
/// retires; the cancel CQE does not free kernel-visible memory.
#[test]
fn runtime_cancelled_read_retains_payload_until_original_cqe() {
    let mut executor = new_executor();

    executor
        .run(async move {
            let (mut reader, writer) = UnixStream::pair().expect("socketpair failed");

            let drops = Rc::new(Cell::new(0));
            let tracked = DropTrackedReadWrite::new(vec![0; 65536], &drops);
            let result = timeout(Duration::from_millis(10), async {
                let (res, _tracked) = reader.read(tracked, 65536).await;
                res
            })
            .await;

            assert!(
                matches!(result, Err(TimeoutError::Elapsed)),
                "read should time out with no writer: {result:?}"
            );
            assert_eq!(
                drops.get(),
                0,
                "read payload dropped while original SQE was live"
            );

            drop(writer);
            wait_for_drop_count(&drops, 1).await;
        })
        .expect("executor run failed");
}

/// Same retain invariant as the timeout variant, but the read is dropped
/// directly after one manual poll with its SQE already in flight.
#[test]
fn runtime_drop_polled_read_retains_payload_until_original_cqe() {
    let mut executor = new_executor();

    executor
        .run(async move {
            let (mut reader, writer) = UnixStream::pair().expect("socketpair failed");

            let drops = Rc::new(Cell::new(0));
            let tracked = DropTrackedReadWrite::new(vec![0; 65536], &drops);
            let mut read = Box::pin(reader.read(tracked, 65536));
            std::future::poll_fn(|cx| match Future::poll(read.as_mut(), cx) {
                Poll::Pending => Poll::Ready(()),
                Poll::Ready(_) => panic!("read completed before cancellation point"),
            })
            .await;

            drop(read);
            assert_eq!(
                drops.get(),
                0,
                "read payload dropped while original SQE was live"
            );

            drop(writer);
            wait_for_drop_count(&drops, 1).await;
        })
        .expect("executor run failed");
}

/// Fills the send buffer until a timed write blocks, then verifies the
/// executor stays healthy after that in-flight write is cancelled.
#[test]
fn runtime_cancel_in_flight_write_on_drop() {
    let mut executor = new_executor();

    executor
        .run(async move {
            let (mut left, _right) = UnixStream::pair().expect("socketpair failed");

            // Fill the socket buffer so the next single write blocks.
            // Write in a loop until we get backpressure, then timeout on the blocking write.
            loop {
                let buf = vec![0xAAu8; 65536];
                let result = timeout(Duration::from_millis(5), async {
                    let (res, _buf) = left.write(buf).await;
                    res
                })
                .await;

                match result {
                    Err(TimeoutError::Elapsed) => {
                        // Timed out — the write future was dropped while in-flight.
                        break;
                    }
                    Err(TimeoutError::Runtime(err)) => {
                        panic!("write timeout runtime failed: {err}");
                    }
                    Ok(_) => {}
                }
            }

            // Executor is still healthy after the cancel.
            sleep(Duration::from_millis(5))
                .await
                .expect("post-cancel sleep failed");
        })
        .expect("executor run failed");
}

/// Multiple concurrent futures can be cancelled independently.
#[test]
fn runtime_cancel_multiple_concurrent() {
    let mut executor = new_executor();

    executor
        .run(async move {
            let (mut s1, _r1) = UnixStream::pair().expect("pair 1");
            let (mut s2, _r2) = UnixStream::pair().expect("pair 2");
            let (mut s3, _r3) = UnixStream::pair().expect("pair 3");

            // Start 3 reads that will never complete, cancel all via timeout.
            let t1 = timeout(Duration::from_millis(5), async {
                let buf = vec![0u8; 8];
                s1.read(buf, 8).await
            });
            let t2 = timeout(Duration::from_millis(5), async {
                let buf = vec![0u8; 8];
                s2.read(buf, 8).await
            });
            let t3 = timeout(Duration::from_millis(5), async {
                let buf = vec![0u8; 8];
                s3.read(buf, 8).await
            });

            let result1 = t1.await;
            assert!(
                matches!(result1, Err(TimeoutError::Elapsed)),
                "first concurrent read should time out: {result1:?}"
            );
            let result2 = t2.await;
            assert!(
                matches!(result2, Err(TimeoutError::Elapsed)),
                "second concurrent read should time out: {result2:?}"
            );
            let result3 = t3.await;
            assert!(
                matches!(result3, Err(TimeoutError::Elapsed)),
                "third concurrent read should time out: {result3:?}"
            );

            // Executor is still healthy.
            sleep(Duration::from_millis(5))
                .await
                .expect("post-cancel sleep failed");
        })
        .expect("executor run failed");
}

/// Dropping a write_all future mid-flight (the partial write resubmission
/// path has been entered) cancels the SQE and frees the pool slot cleanly.
/// Uses a spawned reader that drains slowly so write_all makes partial
/// progress then blocks.
#[test]
fn runtime_cancel_write_all_mid_flight() {
    let mut executor = new_executor();

    executor
        .run(async move {
            let (mut writer, mut reader) = UnixStream::pair().expect("socketpair failed");

            // Spawn a slow reader that drains a little then stops.
            Executor::spawn(async move {
                // Read 64KB then stop — this gives write_all room for one
                // partial write but not enough for the full 1MB.
                let buf = vec![0u8; 65536];
                let (res, _buf) = reader.read(buf, 65536).await;
                let _ = res;
                // Reader hangs here — never reads again.  The write side
                // will block once the socket buffer refills.
                sleep(Duration::from_secs(10)).await.unwrap();
            })
            .expect("spawn reader failed");

            // Give the reader a moment to start.
            sleep(Duration::from_millis(1)).await.unwrap();

            // write_all with 1MB — will make partial progress (socket buffer
            // + 64KB drained by reader), then block on resubmission.
            let big = vec![0xCCu8; 1024 * 1024];
            let result = timeout(Duration::from_millis(50), async {
                let (res, _buf) = writer.write_all(big).await;
                res
            })
            .await;

            assert!(
                matches!(result, Err(TimeoutError::Elapsed)),
                "write_all should have timed out: {result:?}"
            );

            // Executor continues to function after the mid-flight cancel.
            sleep(Duration::from_millis(5))
                .await
                .expect("post-cancel sleep failed");
        })
        .expect("executor run failed");
}

#[test]
fn runtime_write_returns_retained_payload_on_success() {
    let mut executor = new_executor();

    executor
        .run(async move {
            let (mut writer, mut reader) = UnixStream::pair().expect("socketpair failed");
            let drops = Rc::new(Cell::new(0));
            let tracked = DropTrackedReadOnly::new(b"ok".to_vec(), &drops);

            let (res, tracked) = writer.write(tracked).await;
            assert_eq!(res.expect("write failed"), 2);
            assert_eq!(drops.get(), 0, "payload dropped before being returned");

            let (res, recv) = reader.read_exact(vec![0u8; 2], 2).await;
            res.expect("read failed");
            assert_eq!(&recv[..], b"ok");

            drop(tracked);
            assert_eq!(drops.get(), 1, "returned payload should drop once");
        })
        .expect("executor run failed");

    #[cfg(debug_assertions)]
    {
        let stats = executor.last_stats();
        assert!(
            stats.retained_pooled_allocs >= 1,
            "write payload should use retained pool"
        );
        assert!(
            stats.retained_pooled_frees >= 1,
            "write payload storage should return to retained pool"
        );
        assert_eq!(
            stats.retained_heap_fallbacks, 0,
            "small write payload should not use heap fallback"
        );
    }
}

#[test]
fn runtime_write_all_returns_retained_payload_on_success() {
    let mut executor = new_executor();

    executor
        .run(async move {
            let (mut writer, mut reader) = UnixStream::pair().expect("socketpair failed");
            let drops = Rc::new(Cell::new(0));
            let tracked = DropTrackedReadOnly::new(b"all".to_vec(), &drops);

            let (res, tracked) = writer.write_all(tracked).await;
            assert_eq!(res.expect("write_all failed"), 3);
            assert_eq!(drops.get(), 0, "payload dropped before being returned");

            let (res, recv) = reader.read_exact(vec![0u8; 3], 3).await;
            res.expect("read failed");
            assert_eq!(&recv[..], b"all");

            drop(tracked);
            assert_eq!(drops.get(), 1, "returned payload should drop once");
        })
        .expect("executor run failed");
}

#[test]
fn runtime_writev_readonly_chain_returns_retained_payload_on_success() {
    let mut executor = new_executor();

    executor
        .run(async move {
            let (mut writer, mut reader) = UnixStream::pair().expect("socketpair failed");
            let drops = Rc::new(Cell::new(0));
            let chain = tracked_chain([b"vec".to_vec(), b"tor".to_vec()], &drops);

            let (res, chain) = writer.writev(chain).await;
            assert_eq!(res.expect("read-only chain writev failed"), 6);
            assert_eq!(drops.get(), 0, "chain dropped before being returned");

            let (res, recv) = reader.read_exact(vec![0u8; 6], 6).await;
            res.expect("read failed");
            assert_eq!(&recv[..], b"vector");

            drop(chain);
            assert_eq!(drops.get(), 2, "returned chain should drop segments once");
        })
        .expect("executor run failed");

    #[cfg(debug_assertions)]
    {
        let stats = executor.last_stats();
        assert!(
            stats.writev_scratch_inline_allocs >= 1,
            "small writev should use inline retained scratch"
        );
        assert_eq!(
            stats.writev_scratch_pooled_allocs, 0,
            "small writev should not allocate sidecar scratch"
        );
        assert_eq!(
            stats.retained_heap_fallbacks, 0,
            "small writev payload should not use heap fallback"
        );
    }
}

#[test]
fn runtime_writev_all_readonly_chain_returns_retained_payload_on_success() {
    let mut executor = new_executor();

    executor
        .run(async move {
            let (mut writer, mut reader) = UnixStream::pair().expect("socketpair failed");
            let drops = Rc::new(Cell::new(0));
            let chain = tracked_chain([b"write".to_vec(), b"v_all".to_vec()], &drops);

            let (res, chain) = writer.writev_all(chain).await;
            assert_eq!(res.expect("read-only chain writev_all failed"), 10);
            assert_eq!(drops.get(), 0, "chain dropped before being returned");

            let (res, recv) = reader.read_exact(vec![0u8; 10], 10).await;
            res.expect("read failed");
            assert_eq!(&recv[..], b"writev_all");

            drop(chain);
            assert_eq!(drops.get(), 2, "returned chain should drop segments once");
        })
        .expect("executor run failed");

    #[cfg(debug_assertions)]
    {
        let stats = executor.last_stats();
        assert!(
            stats.retained_pooled_allocs >= 1,
            "writev_all payload should use retained pool"
        );
        assert_eq!(
            stats.retained_heap_fallbacks, 0,
            "small writev_all payload should not use heap fallback"
        );
    }
}

#[test]
fn runtime_writev_readonly_chain_512_uses_sidecar_scratch_without_heap_fallback() {
    let mut executor = new_executor();

    executor
        .run(async move {
            let (mut writer, mut reader) = UnixStream::pair().expect("socketpair failed");
            let drops = small_tracked_drops(0);
            drops.store(0, Ordering::Relaxed);
            let (chain, expected) = small_tracked_chain::<512, 1>(0, 0);
            println!(
                "created 512-segment writev chain: segments={}, bytes={}",
                chain.segments(),
                expected.len()
            );

            let (res, chain) = writer.writev(chain).await;
            assert_eq!(
                res.expect("512-segment read-only chain writev failed"),
                expected.len()
            );
            assert_eq!(
                drops.load(Ordering::Relaxed),
                0,
                "512 chain dropped before being returned"
            );

            let (res, recv) = reader
                .read_exact(vec![0u8; expected.len()], expected.len())
                .await;
            res.expect("512 writev readback failed");
            assert_eq!(&recv[..], &expected[..]);

            drop(chain);
            assert_eq!(
                drops.load(Ordering::Relaxed),
                512,
                "returned 512 chain should drop once"
            );
        })
        .expect("executor run failed");

    #[cfg(debug_assertions)]
    {
        let stats = executor.last_stats();
        println!(
            "512 writev retained stats: payload_pooled={}, payload_heap={}, scratch_pooled={}, scratch_slab={}, scratch_frees={}",
            stats.retained_pooled_allocs,
            stats.retained_heap_fallbacks,
            stats.writev_scratch_pooled_allocs,
            stats.writev_scratch_slab_allocs,
            stats.writev_scratch_pooled_frees
        );
        assert!(
            stats.writev_scratch_pooled_allocs >= 1,
            "512 writev should allocate sidecar scratch"
        );
        assert_eq!(
            stats.writev_scratch_oversize_rejections, 0,
            "512 writev should fit supported scratch classes"
        );
        assert_eq!(
            stats.retained_heap_fallbacks, 0,
            "512 writev should not heap-fallback after sidecar scratch split"
        );
    }
}

#[test]
fn runtime_readv_64_uses_sidecar_scratch_without_heap_fallback() {
    let mut executor = new_executor();

    executor
        .run(async move {
            let (mut writer, mut reader) = UnixStream::pair().expect("socketpair failed");
            let expected: Vec<u8> = (0..64).map(|i| (i % 251) as u8).collect();

            let (res, _buf) = writer.write_all(expected.clone()).await;
            assert_eq!(res.expect("64 readv writer failed"), expected.len());

            let (res, chain) = reader.readv(read_chain([1usize; 64])).await;
            assert_eq!(res.expect("64 readv failed"), expected.len());
            assert_eq!(chain.segments(), 64);
            for (index, expected_byte) in expected.iter().copied().enumerate() {
                let segment = chain.get(index).expect("readv segment should exist");
                assert_eq!(segment.payload_bytes(), &[expected_byte]);
            }
        })
        .expect("executor run failed");

    #[cfg(debug_assertions)]
    {
        let stats = executor.last_stats();
        println!(
            "64 readv retained stats: payload_heap={}, scratch_pooled={}, scratch_slab={}, scratch_frees={}",
            stats.retained_heap_fallbacks,
            stats.writev_scratch_pooled_allocs,
            stats.writev_scratch_slab_allocs,
            stats.writev_scratch_pooled_frees
        );
        assert!(
            stats.writev_scratch_pooled_allocs >= 1,
            "64 readv should allocate sidecar scratch"
        );
        assert_eq!(
            stats.writev_scratch_oversize_rejections, 0,
            "64 readv should fit supported scratch classes"
        );
        assert_eq!(
            stats.retained_heap_fallbacks, 0,
            "64 readv should not heap-fallback after sidecar scratch split"
        );
    }
}

#[test]
fn runtime_readv_exact_64_uses_sidecar_scratch_without_heap_fallback() {
    let mut executor = new_executor();

    executor
        .run(async move {
            let (mut writer, mut reader) = UnixStream::pair().expect("socketpair failed");
            let expected: Vec<u8> = (0..64).map(|i| (i % 251) as u8).collect();

            let (res, _buf) = writer.write_all(expected.clone()).await;
            assert_eq!(res.expect("64 readv_exact writer failed"), expected.len());

            let (res, chain) = reader
                .readv_exact(read_chain([1usize; 64]), expected.len())
                .await;
            assert_eq!(res.expect("64 readv_exact failed"), expected.len());
            assert_eq!(chain.segments(), 64);
            for (index, expected_byte) in expected.iter().copied().enumerate() {
                let segment = chain.get(index).expect("readv_exact segment should exist");
                assert_eq!(segment.payload_bytes(), &[expected_byte]);
            }
        })
        .expect("executor run failed");

    #[cfg(debug_assertions)]
    {
        let stats = executor.last_stats();
        assert!(
            stats.writev_scratch_pooled_allocs >= 1,
            "64 readv_exact should allocate sidecar scratch"
        );
        assert_eq!(
            stats.retained_heap_fallbacks, 0,
            "64 readv_exact should not heap-fallback after sidecar scratch split"
        );
    }
}

#[test]
fn runtime_writev_all_readonly_chain_512_returns_chain_on_success() {
    let mut executor = new_executor();

    executor
        .run(async move {
            let (mut writer, mut reader) = UnixStream::pair().expect("socketpair failed");
            let drops = small_tracked_drops(1);
            drops.store(0, Ordering::Relaxed);
            let (chain, expected) = small_tracked_chain::<512, 1>(1, 0);
            println!(
                "created 512-segment writev_all chain: segments={}, bytes={}",
                chain.segments(),
                expected.len()
            );

            let (res, chain) = writer.writev_all(chain).await;
            assert_eq!(
                res.expect("512-segment read-only chain writev_all failed"),
                expected.len()
            );
            assert_eq!(
                drops.load(Ordering::Relaxed),
                0,
                "512 chain dropped before being returned"
            );

            let (res, recv) = reader
                .read_exact(vec![0u8; expected.len()], expected.len())
                .await;
            res.expect("512 writev_all readback failed");
            assert_eq!(&recv[..], &expected[..]);

            drop(chain);
            assert_eq!(
                drops.load(Ordering::Relaxed),
                512,
                "returned 512 chain should drop once"
            );
        })
        .expect("executor run failed");

    #[cfg(debug_assertions)]
    {
        let stats = executor.last_stats();
        assert!(
            stats.writev_scratch_pooled_allocs >= 1,
            "512 writev_all should allocate sidecar scratch"
        );
        assert_eq!(
            stats.retained_heap_fallbacks, 0,
            "512 writev_all should not heap-fallback after sidecar scratch split"
        );
    }
}

#[test]
fn runtime_writev_all_readonly_chain_large_512_advances_across_iovec_boundaries() {
    let mut executor = new_executor();

    executor
        .run(async move {
            let (mut writer, mut reader) = UnixStream::pair().expect("socketpair failed");
            let drops = small_tracked_drops(2);
            drops.store(0, Ordering::Relaxed);
            let (chain, expected) = small_tracked_chain::<512, 4096>(2, 0);
            let total = expected.len();
            println!("created large 512-segment writev_all chain: segments=512, bytes={total}");

            let reader_handle = Executor::spawn(async move {
                let mut out = Vec::with_capacity(total);
                while out.len() < total {
                    let want = std::cmp::min(8192, total - out.len());
                    let (res, buf) = reader.read(vec![0u8; want], want).await;
                    let n = res.expect("large 512 read failed");
                    assert!(n > 0, "reader made no progress before EOF");
                    out.extend_from_slice(&buf[..n]);
                }
                out
            })
            .expect("spawn large 512 reader failed");

            let (res, chain) = writer.writev_all(chain).await;
            assert_eq!(res.expect("large 512 writev_all failed"), total);
            assert_eq!(
                drops.load(Ordering::Relaxed),
                0,
                "large 512 chain dropped early"
            );

            let received = reader_handle.await.expect("reader task cancelled");
            assert_eq!(received, expected);

            drop(chain);
            assert_eq!(
                drops.load(Ordering::Relaxed),
                512,
                "large 512 chain should drop once"
            );
        })
        .expect("executor run failed");
}

#[test]
fn runtime_writev_readonly_chain_oversized_iovec_count_returns_invalid_input() {
    let mut executor = new_executor();

    executor
        .run(async move {
            let (mut writer, _reader) = UnixStream::pair().expect("socketpair failed");
            let chain = static_read_only_chain::<1025>();
            println!(
                "created oversized writev chain: segments={}, bytes={}",
                chain.segments(),
                chain.len()
            );

            let (res, chain) = writer.writev(chain).await;
            let err = res.expect_err("oversized writev should fail");
            assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
            assert_eq!(chain.segments(), 1025);
        })
        .expect("executor run failed");

    #[cfg(debug_assertions)]
    {
        let stats = executor.last_stats();
        assert_eq!(
            stats.writev_scratch_oversize_rejections, 1,
            "oversized writev should be counted separately from payload fallback"
        );
        assert_eq!(
            stats.retained_heap_fallbacks, 0,
            "oversized writev should fail before retaining a payload"
        );
    }
}

#[test]
fn runtime_writev_all_projected_512_writes_in_order_and_fits_task_slot() {
    let mut executor = new_executor();

    executor
        .run(async move {
            let (writer, mut reader) = UnixStream::pair().expect("socketpair failed");
            let source = ProjectedBytes::<512>::new(false);
            let expected = source.expected();
            println!(
                "created compact projected source: pieces=512, bytes={}",
                expected.len()
            );

            let writer_handle = Executor::spawn(async move {
                let mut writer = writer;
                writer.writev_all_projected(source).await
            })
            .expect("compact projected 512 writer should fit task slot");

            let (res, recv) = reader
                .read_exact(vec![0u8; expected.len()], expected.len())
                .await;
            res.expect("projected 512 readback failed");
            assert_eq!(&recv[..], &expected[..]);

            let (res, source) = writer_handle.await.expect("writer task cancelled");
            assert_eq!(
                res.expect("projected 512 writev_all failed"),
                expected.len()
            );
            assert_eq!(source.expected(), expected);
        })
        .expect("executor run failed");

    #[cfg(debug_assertions)]
    {
        let stats = executor.last_stats();
        println!(
            "projected 512 stats: payload_heap={}, scratch_pooled={}, scratch_slab={}, scratch_frees={}",
            stats.retained_heap_fallbacks,
            stats.writev_scratch_pooled_allocs,
            stats.writev_scratch_slab_allocs,
            stats.writev_scratch_pooled_frees
        );
        assert!(
            stats.writev_scratch_pooled_allocs >= 1,
            "projected 512 should allocate sidecar scratch"
        );
        assert_eq!(
            stats.retained_heap_fallbacks, 0,
            "compact projected 512 carrier should not heap-fallback"
        );
    }
}

#[test]
fn runtime_writev_projected_empty_returns_source() {
    let mut executor = new_executor();

    executor
        .run(async move {
            let (mut writer, _reader) = UnixStream::pair().expect("socketpair failed");
            let source = ProjectedBytes::<0>::new(false);

            let (res, source) = writer.writev_projected(source).await;
            assert_eq!(res.expect("empty projected writev failed"), 0);
            assert!(source.expected().is_empty());

            let source = ProjectedBytes::<0>::new(false);
            let (res, source) = writer.writev_all_projected(source).await;
            assert_eq!(res.expect("empty projected writev_all failed"), 0);
            assert!(source.expected().is_empty());
        })
        .expect("executor run failed");
}

#[test]
fn runtime_writev_projected_rejects_oversized_iovec_count() {
    let mut executor = new_executor();

    executor
        .run(async move {
            let (mut writer, _reader) = UnixStream::pair().expect("socketpair failed");
            let source = ProjectedBytes::<1025>::new(false);

            let (res, source) = writer.writev_projected(source).await;
            let err = res.expect_err("oversized projected writev should fail");
            assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
            assert_eq!(source.expected().len(), 1025);
        })
        .expect("executor run failed");

    #[cfg(debug_assertions)]
    {
        let stats = executor.last_stats();
        assert_eq!(
            stats.writev_scratch_oversize_rejections, 1,
            "oversized projected writev should be counted as scratch oversize"
        );
        assert_eq!(
            stats.retained_heap_fallbacks, 0,
            "oversized projected writev should fail before retaining payload"
        );
    }
}

#[test]
fn runtime_writev_projected_rejects_projection_mismatches() {
    let mut executor = new_one_slot_executor();

    executor
        .run(async move {
            let (mut writer, _reader) = UnixStream::pair().expect("socketpair failed");

            let too_many = ProjectedBytes::<2>::with_projection(1, 2, 2);
            let (res, source) = writer.writev_projected(too_many).await;
            assert_eq!(
                res.expect_err("too many projected pieces should fail")
                    .kind(),
                std::io::ErrorKind::InvalidInput
            );
            assert_eq!(source.expected(), vec![0, 1]);

            let too_few = ProjectedBytes::<2>::with_projection(3, 2, 2);
            let (res, source) = writer.writev_projected(too_few).await;
            assert_eq!(
                res.expect_err("too few projected pieces should fail")
                    .kind(),
                std::io::ErrorKind::InvalidInput
            );
            assert_eq!(source.expected(), vec![0, 1]);

            let wrong_total = ProjectedBytes::<2>::with_projection(2, 3, 2);
            let (res, source) = writer.writev_projected(wrong_total).await;
            assert_eq!(
                res.expect_err("wrong projected byte total should fail")
                    .kind(),
                std::io::ErrorKind::InvalidInput
            );
            assert_eq!(source.expected(), vec![0, 1]);

            let all_wrong_total = ProjectedBytes::<2>::with_projection(2, 3, 2);
            let (res, source) = writer.writev_all_projected(all_wrong_total).await;
            assert_eq!(
                res.expect_err("wrong projected byte total should fail for writev_all")
                    .kind(),
                std::io::ErrorKind::InvalidInput
            );
            assert_eq!(source.expected(), vec![0, 1]);

            let drops = Rc::new(Cell::new(0));
            let pooled_wrong_total =
                DropTrackedProjected::<17>::with_projection(17, 18, 17, &drops);
            let (res, source) = writer.writev_projected(pooled_wrong_total).await;
            assert_eq!(
                res.expect_err("pooled projected byte-total mismatch should fail")
                    .kind(),
                std::io::ErrorKind::InvalidInput
            );
            assert_eq!(source.expected().len(), 17);
            assert_eq!(drops.get(), 0, "mismatched projected source dropped early");
            drop(source);
            assert_eq!(drops.get(), 1, "mismatched projected source dropped once");

            assert_eq!(
                Nop::new()
                    .await
                    .expect("projected mismatch leaked its one op slot"),
                0
            );
        })
        .expect("executor run failed");

    #[cfg(debug_assertions)]
    {
        let stats = executor.last_stats();
        assert_eq!(
            stats.writev_scratch_pooled_allocs, 1,
            "pooled projected mismatch should allocate one sidecar"
        );
        assert_eq!(
            stats.writev_scratch_pooled_frees, stats.writev_scratch_pooled_allocs,
            "projected mismatch did not return pooled scratch exactly once"
        );
        assert!(
            stats.retained_pooled_allocs > 0,
            "projected mismatches did not reserve retained payload slots"
        );
        assert_eq!(
            stats.retained_pooled_frees, stats.retained_pooled_allocs,
            "projected mismatches did not recycle retained payload slots"
        );
    }
}

#[test]
fn runtime_writev_all_projected_large_512_advances_across_iovec_boundaries() {
    let mut executor = new_executor();

    executor
        .run(async move {
            let (mut writer, mut reader) = UnixStream::pair().expect("socketpair failed");
            writer
                .set_send_buffer_size(4096)
                .expect("set projected writer send buffer failed");
            let source = ProjectedStaticSegments::<512, 4096>::new(false);
            let expected = source.expected();
            let total = expected.len();
            println!("created large projected source: pieces=512, bytes={total}");

            let reader_handle = Executor::spawn(async move {
                let mut out = Vec::with_capacity(total);
                while out.len() < total {
                    let want = std::cmp::min(8192, total - out.len());
                    let (res, buf) = reader.read(vec![0u8; want], want).await;
                    let n = res.expect("large projected read failed");
                    assert!(n > 0, "reader made no progress before EOF");
                    out.extend_from_slice(&buf[..n]);
                }
                out
            })
            .expect("spawn large projected reader failed");

            let (res, source) = writer.writev_all_projected(source).await;
            assert_eq!(res.expect("large projected writev_all failed"), total);
            assert_eq!(source.expected(), expected);

            let received = reader_handle.await.expect("reader task cancelled");
            assert_eq!(received, expected);
        })
        .expect("executor run failed");

    #[cfg(debug_assertions)]
    assert!(
        executor.last_stats().writev_partial_continuations > 0,
        "large projected write must exercise retry resubmission"
    );
}

/// A write cancelled under backpressure must retain its payload until the
/// original CQE retires; dropping the peer lets that CQE complete.
#[test]
fn runtime_cancelled_write_retains_payload_until_original_cqe() {
    let mut executor = new_executor();

    executor
        .run(async move {
            let (mut writer, reader) = UnixStream::pair().expect("socketpair failed");
            fill_unix_send_buffer(&mut writer).await;

            let drops = Rc::new(Cell::new(0));
            let tracked = DropTrackedReadOnly::new(vec![0x11; 65536], &drops);
            let result = timeout(Duration::from_millis(10), async {
                let (res, _tracked) = writer.write(tracked).await;
                res
            })
            .await;

            assert!(
                matches!(result, Err(TimeoutError::Elapsed)),
                "write should time out under backpressure: {result:?}"
            );
            assert_eq!(
                drops.get(),
                0,
                "payload dropped while original SQE was live"
            );

            drop(reader);
            wait_for_drop_count(&drops, 1).await;
        })
        .expect("executor run failed");
}

/// A backpressured write dropped directly after one manual poll must not free
/// its payload until the original CQE retires.
#[test]
fn runtime_drop_polled_write_retains_payload_until_original_cqe() {
    let mut executor = new_executor();

    executor
        .run(async move {
            let (mut writer, reader) = UnixStream::pair().expect("socketpair failed");
            fill_unix_send_buffer(&mut writer).await;

            let drops = Rc::new(Cell::new(0));
            let tracked = DropTrackedReadOnly::new(vec![0x11; 65536], &drops);
            let mut write = Box::pin(writer.write(tracked));
            std::future::poll_fn(|cx| match Future::poll(write.as_mut(), cx) {
                Poll::Pending => Poll::Ready(()),
                Poll::Ready(_) => panic!("write completed before cancellation point"),
            })
            .await;

            drop(write);
            assert_eq!(
                drops.get(),
                0,
                "write payload dropped while original SQE was live"
            );

            drop(reader);
            wait_for_drop_count(&drops, 1).await;
        })
        .expect("executor run failed");
}

/// write_all cancellation must also retain the large payload across any
/// partial-write bookkeeping until the original CQE retires.
#[test]
fn runtime_cancelled_write_all_retains_payload_until_original_cqe() {
    let mut executor = new_executor();

    executor
        .run(async move {
            let (mut writer, reader) = UnixStream::pair().expect("socketpair failed");
            fill_unix_send_buffer(&mut writer).await;

            let drops = Rc::new(Cell::new(0));
            let tracked = DropTrackedReadOnly::new(vec![0x22; 1024 * 1024], &drops);
            let result = timeout(Duration::from_millis(10), async {
                let (res, _tracked) = writer.write_all(tracked).await;
                res
            })
            .await;

            assert!(
                matches!(result, Err(TimeoutError::Elapsed)),
                "write_all should time out under backpressure: {result:?}"
            );
            assert_eq!(
                drops.get(),
                0,
                "payload dropped while original SQE was live"
            );

            drop(reader);
            wait_for_drop_count(&drops, 1).await;
        })
        .expect("executor run failed");
}

/// writev cancellation under backpressure retains the full read-only segment
/// chain until the original CQE retires.
#[test]
fn runtime_cancelled_writev_readonly_chain_retains_payload_until_original_cqe() {
    let mut executor = new_executor();

    executor
        .run(async move {
            let (mut writer, reader) = UnixStream::pair().expect("socketpair failed");
            fill_unix_send_buffer(&mut writer).await;

            let drops = Rc::new(Cell::new(0));
            let chain = tracked_chain([vec![0x33; 32768], vec![0x44; 32768]], &drops);
            let result = timeout(Duration::from_millis(10), async {
                let (res, _chain) = writer.writev(chain).await;
                res
            })
            .await;

            assert!(
                matches!(result, Err(TimeoutError::Elapsed)),
                "read-only chain writev should time out under backpressure: {result:?}"
            );
            assert_eq!(drops.get(), 0, "chain dropped while original SQE was live");

            drop(reader);
            wait_for_drop_count(&drops, 2).await;
        })
        .expect("executor run failed");
}

/// A manually polled writev dropped in flight keeps both read-only segments
/// alive until the original CQE retires.
#[test]
fn runtime_drop_polled_writev_readonly_chain_retains_payload_until_original_cqe() {
    let mut executor = new_executor();

    executor
        .run(async move {
            let (mut writer, reader) = UnixStream::pair().expect("socketpair failed");
            fill_unix_send_buffer(&mut writer).await;

            let drops = Rc::new(Cell::new(0));
            let chain = tracked_chain([vec![0x33; 32768], vec![0x44; 32768]], &drops);
            let mut writev = Box::pin(writer.writev(chain));
            std::future::poll_fn(|cx| match Future::poll(writev.as_mut(), cx) {
                Poll::Pending => Poll::Ready(()),
                Poll::Ready(_) => {
                    panic!("read-only chain writev completed before cancellation point")
                }
            })
            .await;

            drop(writev);
            assert_eq!(drops.get(), 0, "chain dropped while original SQE was live");

            drop(reader);
            wait_for_drop_count(&drops, 2).await;
        })
        .expect("executor run failed");
}

/// writev_all cancellation retains all segments across retry bookkeeping until
/// the original CQE retires.
#[test]
fn runtime_cancelled_writev_all_readonly_chain_retains_payload_until_original_cqe() {
    let mut executor = new_executor();

    executor
        .run(async move {
            let (mut writer, reader) = UnixStream::pair().expect("socketpair failed");
            fill_unix_send_buffer(&mut writer).await;

            let drops = Rc::new(Cell::new(0));
            let chain = tracked_chain([vec![0x55; 32768], vec![0x66; 32768]], &drops);
            let result = timeout(Duration::from_millis(10), async {
                let (res, _chain) = writer.writev_all(chain).await;
                res
            })
            .await;

            assert!(
                matches!(result, Err(TimeoutError::Elapsed)),
                "read-only chain writev_all should time out under backpressure: {result:?}"
            );
            assert_eq!(drops.get(), 0, "chain dropped while original SQE was live");

            drop(reader);
            wait_for_drop_count(&drops, 2).await;
        })
        .expect("executor run failed");
}

/// 512-segment writev cancellation retains both payload and sidecar iovec
/// scratch until the original CQE retires; the CQE may retire during timeout.
#[test]
fn runtime_cancelled_writev_readonly_chain_512_retains_payload_and_scratch_until_original_cqe() {
    let mut executor = new_executor();

    executor
        .run(async move {
            let (mut writer, reader) = UnixStream::pair().expect("socketpair failed");
            fill_unix_send_buffer(&mut writer).await;

            let drops = small_tracked_drops(3);
            drops.store(0, Ordering::Relaxed);
            let (chain, _expected) = small_tracked_chain::<512, 1024>(3, 0);
            println!(
                "created cancellable 512-segment writev chain: segments={}, bytes={}",
                chain.segments(),
                chain.len()
            );

            let result = timeout(Duration::from_millis(10), async {
                let (res, _chain) = writer.writev(chain).await;
                res
            })
            .await;

            assert!(
                matches!(result, Err(TimeoutError::Elapsed)),
                "512-segment read-only chain writev should time out under backpressure: {result:?}"
            );
            let drops_after_timeout = drops.load(Ordering::Relaxed);
            assert!(
                drops_after_timeout == 0 || drops_after_timeout == 512,
                "512 retained payload should be either still retained or fully retired, got {drops_after_timeout}"
            );

            drop(reader);
            if drops_after_timeout == 0 {
                for _ in 0..100 {
                    if drops.load(Ordering::Relaxed) == 512 {
                        return;
                    }
                    sleep(Duration::from_millis(5))
                        .await
                        .expect("512 drop wait sleep failed");
                }
            }
            assert_eq!(
                drops.load(Ordering::Relaxed),
                512,
                "512 retained payload was not dropped exactly once after CQE retirement"
            );
        })
        .expect("executor run failed");

    #[cfg(debug_assertions)]
    {
        let stats = executor.last_stats();
        println!(
            "cancelled 512 writev stats: payload_heap={}, scratch_pooled={}, scratch_frees={}",
            stats.retained_heap_fallbacks,
            stats.writev_scratch_pooled_allocs,
            stats.writev_scratch_pooled_frees
        );
        assert!(
            stats.writev_scratch_pooled_allocs >= 1,
            "cancelled 512 writev should allocate sidecar scratch"
        );
        assert!(
            stats.writev_scratch_pooled_frees >= 1,
            "cancelled 512 writev should free sidecar scratch after target CQE"
        );
    }
}

/// Projected writev cancellation retains the compact projection source and
/// sidecar scratch until the original CQE retires.
#[test]
fn runtime_cancelled_writev_projected_512_retains_source_and_scratch_until_original_cqe() {
    let mut executor = new_executor();

    executor
        .run(async move {
            let (mut writer, reader) = UnixStream::pair().expect("socketpair failed");
            fill_unix_send_buffer(&mut writer).await;

            PROJECTED_SOURCE_DROPS.store(0, Ordering::Relaxed);
            let source = ProjectedStaticSegments::<512, 1024>::new(true);
            println!(
                "created cancellable projected source: pieces=512, bytes={}",
                source.writev_count_and_len().1
            );

            let result = timeout(Duration::from_millis(10), async {
                let (res, _source) = writer.writev_projected(source).await;
                res
            })
            .await;

            assert!(
                matches!(result, Err(TimeoutError::Elapsed)),
                "512 projected writev should time out under backpressure: {result:?}"
            );
            let drops_after_timeout = PROJECTED_SOURCE_DROPS.load(Ordering::Relaxed);
            assert!(
                drops_after_timeout == 0 || drops_after_timeout == 1,
                "projected source should be either still retained or fully retired, got {drops_after_timeout}"
            );

            drop(reader);
            if drops_after_timeout == 0 {
                for _ in 0..100 {
                    if PROJECTED_SOURCE_DROPS.load(Ordering::Relaxed) == 1 {
                        return;
                    }
                    sleep(Duration::from_millis(5))
                        .await
                        .expect("projected drop wait sleep failed");
                }
            }
            assert_eq!(
                PROJECTED_SOURCE_DROPS.load(Ordering::Relaxed),
                1,
                "projected retained source was not dropped exactly once after CQE retirement"
            );
        })
        .expect("executor run failed");

    #[cfg(debug_assertions)]
    {
        let stats = executor.last_stats();
        println!(
            "cancelled projected 512 stats: payload_heap={}, scratch_pooled={}, scratch_frees={}",
            stats.retained_heap_fallbacks,
            stats.writev_scratch_pooled_allocs,
            stats.writev_scratch_pooled_frees
        );
        assert!(
            stats.writev_scratch_pooled_allocs >= 1,
            "cancelled projected 512 writev should allocate sidecar scratch"
        );
        assert!(
            stats.writev_scratch_pooled_frees >= 1,
            "cancelled projected 512 writev should free sidecar scratch after target CQE"
        );
    }
}

/// Dropping a read_exact future mid-flight after partial progress cancels the
/// outstanding SQE and reclaims the CompletionState exactly once.
#[test]
fn runtime_cancel_read_exact_mid_flight() {
    let mut executor = new_executor();

    executor
        .run(async move {
            let (mut reader, writer) = UnixStream::pair().expect("socketpair failed");

            // Spawn a writer that sends some data, then stalls until the test
            // releases it so the cancelled read's original CQE can retire.
            let release_writer = spawn_stalling_writer(writer, 4096);

            sleep(Duration::from_millis(1)).await.unwrap();

            let drops = Rc::new(Cell::new(0));
            let big = DropTrackedReadWrite::new(vec![0u8; 1024 * 1024], &drops);
            let result = timeout(Duration::from_millis(50), async {
                let (res, _buf) = reader.read_exact(big, 1024 * 1024).await;
                res
            })
            .await;

            assert!(
                matches!(result, Err(TimeoutError::Elapsed)),
                "read_exact should have timed out: {result:?}"
            );
            assert_eq!(
                drops.get(),
                0,
                "read_exact destination buffer dropped while original SQE was live"
            );

            release_writer.set(true);
            wait_for_drop_count(&drops, 1).await;
        })
        .expect("executor run failed");
}

/// read_exact_append cancelled with no priming bytes must cancel the
/// outstanding SQE cleanly and keep the executor usable.
#[test]
fn runtime_cancel_read_exact_append_mid_flight() {
    let mut executor = new_executor();

    executor
        .run(async move {
            let (mut reader, writer) = UnixStream::pair().expect("socketpair failed");
            let release_writer = spawn_stalling_writer(writer, 0);

            let recv = IoBuffMut::new(0, 1024 * 1024, 0).expect("recv buffer allocation failed");
            let result = timeout(Duration::from_millis(50), async {
                let (res, _buf) = reader.read_exact_append(recv, 1024 * 1024).await;
                res
            })
            .await;

            assert!(
                matches!(result, Err(TimeoutError::Elapsed)),
                "read_exact_append should have timed out: {result:?}"
            );

            release_writer.set(true);
            sleep(Duration::from_millis(10))
                .await
                .expect("post-cancel append sleep failed");
        })
        .expect("executor run failed");
}

/// read_exact_append cancelled after partial progress must keep its
/// pool-backed destination checked out until the cancelled read CQE retires.
#[test]
fn runtime_cancel_read_exact_append_retains_pool_buffer_until_cqe() {
    let mut executor = new_executor();

    executor
        .run(async move {
            let (mut reader, writer) = UnixStream::pair().expect("socketpair failed");
            let release_writer = spawn_stalling_writer(writer, 4096);

            let mut pool = IoBuffPool::new(IoBuffPoolConfig {
                headroom: 0,
                payload: 1024 * 1024,
                tailroom: 0,
                objs_per_slab: 1,
            })
            .expect("pool config invalid");
            pool.init();

            sleep(Duration::from_millis(1)).await.unwrap();

            let recv = pool.alloc().expect("recv pool alloc failed");
            assert_eq!(pool.live_slots_for_test(), 1);

            let result = timeout(Duration::from_millis(50), async {
                let (res, _buf) = reader.read_exact_append(recv, 1024 * 1024).await;
                res
            })
            .await;

            assert!(
                matches!(result, Err(TimeoutError::Elapsed)),
                "read_exact_append should have timed out: {result:?}"
            );
            assert_eq!(
                pool.live_slots_for_test(),
                1,
                "read_exact_append buffer was released before the original CQE retired"
            );

            release_writer.set(true);
            wait_for_live_slots(&pool, 0).await;
        })
        .expect("executor run failed");
}

/// readv cancellation must cancel the outstanding SQE cleanly; releasing the
/// stalled writer retires the original CQE.
#[test]
fn runtime_cancel_readv_mid_flight() {
    let mut executor = new_executor();

    executor
        .run(async move {
            let (mut reader, writer) = UnixStream::pair().expect("socketpair failed");
            let release_writer = spawn_stalling_writer(writer, 0);

            let recv = read_chain([512 * 1024, 512 * 1024]);
            let result = timeout(Duration::from_millis(50), async {
                let (res, _chain) = reader.readv(recv).await;
                res
            })
            .await;

            assert!(
                matches!(result, Err(TimeoutError::Elapsed)),
                "readv should have timed out: {result:?}"
            );

            release_writer.set(true);
            sleep(Duration::from_millis(10))
                .await
                .expect("post-cancel readv sleep failed");
        })
        .expect("executor run failed");
}

/// Dropping an in-flight readv must keep its pool-backed destination buffers
/// checked out until the original CQE retires.
#[test]
fn runtime_drop_polled_readv_cleans_up_after_original_cqe() {
    let mut executor = new_executor();

    executor
        .run(async move {
            let (mut reader, writer) = UnixStream::pair().expect("socketpair failed");
            let mut pool = IoBuffPool::new(IoBuffPoolConfig {
                headroom: 0,
                payload: 4096,
                tailroom: 0,
                objs_per_slab: 2,
            })
            .expect("pool config invalid");
            pool.init();
            let recv = IoBuffVecMut::<2>::from_array([
                pool.alloc().expect("first pool alloc failed"),
                pool.alloc().expect("second pool alloc failed"),
            ]);
            assert_eq!(pool.live_slots_for_test(), 2);

            let mut readv = Box::pin(reader.readv(recv));
            std::future::poll_fn(|cx| match Future::poll(readv.as_mut(), cx) {
                Poll::Pending => Poll::Ready(()),
                Poll::Ready(_) => panic!("readv completed before cancellation point"),
            })
            .await;

            drop(readv);
            assert_eq!(
                pool.live_slots_for_test(),
                2,
                "readv buffers were released before the original CQE retired"
            );

            drop(writer);
            for _ in 0..100 {
                if pool.live_slots_for_test() == 0 {
                    return;
                }
                sleep(Duration::from_millis(5))
                    .await
                    .expect("post-drop readv sleep failed");
            }
            panic!("readv buffers were not released after the original CQE retired");
        })
        .expect("executor run failed");
}

/// readv_exact cancelled after partial progress must cancel the outstanding
/// SQE cleanly and keep pool-backed segments checked out until the original
/// CQE retires after peer release.
#[test]
fn runtime_cancel_readv_exact_mid_flight() {
    let mut executor = new_executor();

    executor
        .run(async move {
            let (mut reader, writer) = UnixStream::pair().expect("socketpair failed");
            let release_writer = spawn_stalling_writer(writer, 4096);
            let mut pool = IoBuffPool::new(IoBuffPoolConfig {
                headroom: 0,
                payload: 512 * 1024,
                tailroom: 0,
                objs_per_slab: 2,
            })
            .expect("pool config invalid");
            pool.init();

            sleep(Duration::from_millis(1)).await.unwrap();

            let recv = IoBuffVecMut::<2>::from_array([
                pool.alloc().expect("first pool alloc failed"),
                pool.alloc().expect("second pool alloc failed"),
            ]);
            assert_eq!(pool.live_slots_for_test(), 2);

            let result = timeout(Duration::from_millis(50), async {
                let (res, _chain) = reader.readv_exact(recv, 1024 * 1024).await;
                res
            })
            .await;

            assert!(
                matches!(result, Err(TimeoutError::Elapsed)),
                "readv_exact should have timed out: {result:?}"
            );
            assert_eq!(
                pool.live_slots_for_test(),
                2,
                "readv_exact buffers were released before the original CQE retired"
            );

            release_writer.set(true);
            wait_for_live_slots(&pool, 0).await;
        })
        .expect("executor run failed");
}

/// `Nop::default()` produces a working future identical to `Nop::new()`.
#[test]
fn runtime_nop_default_trait() {
    let mut executor = new_executor();

    executor
        .run(async move {
            let value = Nop::default().await.expect("nop default failed");
            assert_eq!(value, 0);
        })
        .expect("executor run failed");
}

/// `NopSlot::default()` produces a reusable slot identical to `NopSlot::new()`.
#[test]
fn runtime_nop_slot_default_trait() {
    let mut executor = new_executor();

    executor
        .run(async move {
            let mut slot = NopSlot::default();
            let nop = slot.nop().expect("slot nop failed");
            nop.await.expect("nop failed");
        })
        .expect("executor run failed");
}
