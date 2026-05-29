use flowio::net::unix::UnixStream;
use flowio::net::{WritevPieces, WritevProjection};
use flowio::runtime::buffer::iobuffvec::IoBuffReadOnlyVec;
use flowio::runtime::buffer::{IoBuffReadOnly, IoBuffReadWrite};
use flowio::runtime::executor::{Executor, ExecutorConfig};
use flowio::runtime::io::{Nop, NopSlot};
use flowio::runtime::op::CompletionState;
use flowio::runtime::reactor::ReactorConfig;
use flowio::runtime::task::TaskHeader;
use flowio::runtime::timer::{sleep, sleep_until, timeout, timeout_at};
use std::cell::{Cell, RefCell};
use std::io;
use std::rc::Rc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

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

struct DropTrackedReadOnly {
    bytes: Vec<u8>,
    drops: Rc<Cell<usize>>,
}

impl DropTrackedReadOnly {
    fn new(bytes: Vec<u8>, drops: &Rc<Cell<usize>>) -> Self {
        Self {
            bytes,
            drops: Rc::clone(drops),
        }
    }
}

impl Drop for DropTrackedReadOnly {
    fn drop(&mut self) {
        self.drops.set(self.drops.get() + 1);
    }
}

unsafe impl IoBuffReadOnly for DropTrackedReadOnly {
    fn as_ptr(&self) -> *const u8 {
        self.bytes.as_ptr()
    }

    fn len(&self) -> usize {
        self.bytes.len()
    }
}

struct DropTrackedReadWrite {
    bytes: Vec<u8>,
    written: usize,
    drops: Rc<Cell<usize>>,
}

impl DropTrackedReadWrite {
    fn new(bytes: Vec<u8>, drops: &Rc<Cell<usize>>) -> Self {
        Self {
            bytes,
            written: 0,
            drops: Rc::clone(drops),
        }
    }
}

impl Drop for DropTrackedReadWrite {
    fn drop(&mut self) {
        self.drops.set(self.drops.get() + 1);
    }
}

unsafe impl IoBuffReadWrite for DropTrackedReadWrite {
    fn as_mut_ptr(&mut self) -> *mut u8 {
        self.bytes.as_mut_ptr()
    }

    fn writable_len(&self) -> usize {
        self.bytes.len()
    }

    unsafe fn set_written_len(&mut self, len: usize) {
        self.written = len;
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

static SMALL_TRACKED_DROPS_0: AtomicUsize = AtomicUsize::new(0);
static SMALL_TRACKED_DROPS_1: AtomicUsize = AtomicUsize::new(0);
static SMALL_TRACKED_DROPS_2: AtomicUsize = AtomicUsize::new(0);
static SMALL_TRACKED_DROPS_3: AtomicUsize = AtomicUsize::new(0);
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

struct SmallTrackedReadOnly<const LEN: usize> {
    index: u8,
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

static PROJECTED_SOURCE_DROPS: AtomicUsize = AtomicUsize::new(0);

struct ProjectedBytes<const N: usize> {
    bytes: [u8; N],
    counted_pieces: usize,
    counted_total: usize,
    projected_len: usize,
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

struct ProjectedStaticSegments<const N: usize, const LEN: usize> {
    indices: [u8; N],
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
            Err(_) => break,
        }
    }
}

async fn wait_for_drop_count(drops: &Rc<Cell<usize>>, expected: usize) {
    for _ in 0..100 {
        if drops.get() == expected {
            return;
        }
        sleep(Duration::from_millis(5))
            .await
            .expect("drop wait sleep failed");
    }

    assert_eq!(
        drops.get(),
        expected,
        "retained payload was not dropped exactly once after CQE retirement"
    );
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

fn small_tracked_chain<const N: usize, const LEN: usize>(
    counter: u8,
    start_index: usize,
) -> (IoBuffReadOnlyVec<SmallTrackedReadOnly<LEN>, N>, Vec<u8>) {
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

#[test]
fn runtime_executor_constructs_with_custom_config() {
    let executor = new_executor_with(16, None);
    assert_eq!(executor.process_quota, 16);
    assert_eq!(executor.cpu_affinity, None);
}

#[cfg(target_os = "linux")]
#[test]
fn runtime_executor_preserves_cpu_affinity_config() {
    let current_cpu = unsafe { libc::sched_getcpu() };
    assert!(current_cpu >= 0, "sched_getcpu failed");

    let executor = new_executor_with(16, Some(current_cpu as usize));
    assert_eq!(executor.cpu_affinity, Some(current_cpu as usize));
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
    }
    #[cfg(not(debug_assertions))]
    {
        let _ = executor;
    }
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
    let mut executor = new_executor();
    let observed = Rc::new(Cell::new(Duration::ZERO));
    let observed_flag = observed.clone();

    executor
        .run(async move {
            std::thread::sleep(Duration::from_millis(20));
            let start = Instant::now();
            sleep(Duration::from_millis(5)).await.expect("sleep failed");
            observed_flag.set(start.elapsed());
        })
        .expect("executor run failed");

    assert!(
        observed.get() >= Duration::from_millis(4),
        "sleep completed too early after idle gap: {:?}",
        observed.get()
    );
}

#[test]
fn runtime_sleep_until_uses_fresh_tick_after_idle_gap() {
    let mut executor = new_executor();
    let observed = Rc::new(Cell::new(Duration::ZERO));
    let observed_flag = observed.clone();

    executor
        .run(async move {
            std::thread::sleep(Duration::from_millis(20));
            let start = Instant::now();
            let deadline = start + Duration::from_millis(5);
            sleep_until(deadline).await.expect("sleep_until failed");
            observed_flag.set(start.elapsed());
        })
        .expect("executor run failed");

    assert!(
        observed.get() >= Duration::from_millis(4),
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
            assert!(result.is_err(), "timeout should have elapsed");
            timed_out_flag.set(true);
        })
        .expect("executor run failed");

    assert!(timed_out.get(), "timeout expiry path did not run");
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
/// Validates that the pointer-based TLS context correctly cycles owner_task
/// across interleaved task polls.
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

                // Spawn ponger.
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

                // Spawn pinger.
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
            let value = handle.await;
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
            let value = handle.await;
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

            let v3 = h3.await;
            let v1 = h1.await;
            let v2 = h2.await;

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

            let write_result = writer.await;
            let read_result = reader.await;

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

            let value = handle.await;
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
            assert!(result.is_err(), "should have timed out");

            // Executor continues to work after the cancel.
            sleep(Duration::from_millis(5))
                .await
                .expect("post-cancel sleep failed");
        })
        .expect("executor run failed");
}

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

            assert!(result.is_err(), "read should time out with no writer");
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

/// Dropping a single write future mid-flight cancels the operation cleanly.
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

                if result.is_err() {
                    // Timed out — the write future was dropped while in-flight.
                    break;
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

            assert!(t1.await.is_err());
            assert!(t2.await.is_err());
            assert!(t3.await.is_err());

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

            assert!(result.is_err(), "write_all should have timed out");

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
fn runtime_writev_read_only_returns_retained_payload_on_success() {
    let mut executor = new_executor();

    executor
        .run(async move {
            let (mut writer, mut reader) = UnixStream::pair().expect("socketpair failed");
            let drops = Rc::new(Cell::new(0));
            let chain = tracked_chain([b"vec".to_vec(), b"tor".to_vec()], &drops);

            let (res, chain) = writer.writev_read_only(chain).await;
            assert_eq!(res.expect("writev_read_only failed"), 6);
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
fn runtime_writev_all_read_only_returns_retained_payload_on_success() {
    let mut executor = new_executor();

    executor
        .run(async move {
            let (mut writer, mut reader) = UnixStream::pair().expect("socketpair failed");
            let drops = Rc::new(Cell::new(0));
            let chain = tracked_chain([b"write".to_vec(), b"v_all".to_vec()], &drops);

            let (res, chain) = writer.writev_all_read_only(chain).await;
            assert_eq!(res.expect("writev_all_read_only failed"), 10);
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
fn runtime_writev_read_only_512_uses_sidecar_scratch_without_heap_fallback() {
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

            let (res, chain) = writer.writev_read_only(chain).await;
            assert_eq!(res.expect("512 writev_read_only failed"), expected.len());
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
fn runtime_writev_all_read_only_512_returns_chain_on_success() {
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

            let (res, chain) = writer.writev_all_read_only(chain).await;
            assert_eq!(
                res.expect("512 writev_all_read_only failed"),
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
fn runtime_writev_all_read_only_large_512_advances_across_iovec_boundaries() {
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

            let (res, chain) = writer.writev_all_read_only(chain).await;
            assert_eq!(res.expect("large 512 writev_all failed"), total);
            assert_eq!(
                drops.load(Ordering::Relaxed),
                0,
                "large 512 chain dropped early"
            );

            let received = reader_handle.await;
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
fn runtime_writev_read_only_oversized_iovec_count_returns_invalid_input() {
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

            let (res, chain) = writer.writev_read_only(chain).await;
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

            let (res, source) = writer_handle.await;
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
    let mut executor = new_executor();

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
        })
        .expect("executor run failed");
}

#[test]
fn runtime_writev_all_projected_large_512_advances_across_iovec_boundaries() {
    let mut executor = new_executor();

    executor
        .run(async move {
            let (mut writer, mut reader) = UnixStream::pair().expect("socketpair failed");
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

            let received = reader_handle.await;
            assert_eq!(received, expected);
        })
        .expect("executor run failed");
}

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

            assert!(result.is_err(), "write should time out under backpressure");
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
                result.is_err(),
                "write_all should time out under backpressure"
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

#[test]
fn runtime_cancelled_writev_read_only_retains_payload_until_original_cqe() {
    let mut executor = new_executor();

    executor
        .run(async move {
            let (mut writer, reader) = UnixStream::pair().expect("socketpair failed");
            fill_unix_send_buffer(&mut writer).await;

            let drops = Rc::new(Cell::new(0));
            let chain = tracked_chain([vec![0x33; 32768], vec![0x44; 32768]], &drops);
            let result = timeout(Duration::from_millis(10), async {
                let (res, _chain) = writer.writev_read_only(chain).await;
                res
            })
            .await;

            assert!(
                result.is_err(),
                "writev_read_only should time out under backpressure"
            );
            assert_eq!(drops.get(), 0, "chain dropped while original SQE was live");

            drop(reader);
            wait_for_drop_count(&drops, 2).await;
        })
        .expect("executor run failed");
}

#[test]
fn runtime_cancelled_writev_all_read_only_retains_payload_until_original_cqe() {
    let mut executor = new_executor();

    executor
        .run(async move {
            let (mut writer, reader) = UnixStream::pair().expect("socketpair failed");
            fill_unix_send_buffer(&mut writer).await;

            let drops = Rc::new(Cell::new(0));
            let chain = tracked_chain([vec![0x55; 32768], vec![0x66; 32768]], &drops);
            let result = timeout(Duration::from_millis(10), async {
                let (res, _chain) = writer.writev_all_read_only(chain).await;
                res
            })
            .await;

            assert!(
                result.is_err(),
                "writev_all_read_only should time out under backpressure"
            );
            assert_eq!(drops.get(), 0, "chain dropped while original SQE was live");

            drop(reader);
            wait_for_drop_count(&drops, 2).await;
        })
        .expect("executor run failed");
}

#[test]
fn runtime_cancelled_writev_read_only_512_retains_payload_and_scratch_until_original_cqe() {
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
                let (res, _chain) = writer.writev_read_only(chain).await;
                res
            })
            .await;

            assert!(
                result.is_err(),
                "512 writev_read_only should time out under backpressure"
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
                result.is_err(),
                "512 projected writev should time out under backpressure"
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
            let (mut reader, mut writer) = UnixStream::pair().expect("socketpair failed");

            // Spawn a writer that sends some data, then stalls forever.
            Executor::spawn(async move {
                let chunk = vec![0xDDu8; 65536];
                let (res, _chunk) = writer.write_all(chunk).await;
                let _ = res;
                sleep(Duration::from_secs(10)).await.unwrap();
            })
            .expect("spawn writer failed");

            sleep(Duration::from_millis(1)).await.unwrap();

            let big = vec![0u8; 1024 * 1024];
            let result = timeout(Duration::from_millis(50), async {
                let (res, _buf) = reader.read_exact(big, 1024 * 1024).await;
                res
            })
            .await;

            assert!(result.is_err(), "read_exact should have timed out");

            sleep(Duration::from_millis(5))
                .await
                .expect("post-cancel sleep failed");
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
