use flowio::net::unix::UnixStream;
use flowio::runtime::executor::{Executor, ExecutorConfig};
use flowio::runtime::io::{Nop, NopSlot};
use flowio::runtime::op::CompletionState;
use flowio::runtime::reactor::ReactorConfig;
use flowio::runtime::task::TaskHeader;
use flowio::runtime::timer::{sleep, sleep_until, timeout, timeout_at};
use std::cell::{Cell, RefCell};
use std::rc::Rc;
use std::time::{Duration, Instant};

fn new_executor() -> Executor
{
    Executor::new().expect("failed to construct runtime executor")
}

fn new_executor_with(process_quota: usize, cpu_affinity: Option<usize>) -> Executor
{
    Executor::new_with_config(ExecutorConfig {
        reactor: ReactorConfig { ring_entries: 64 },
        process_quota,
        cpu_affinity,
    })
    .expect("failed to construct runtime executor")
}

#[test]
fn runtime_executor_constructs_with_custom_config()
{
    let executor = new_executor_with(16, None);
    assert_eq!(executor.process_quota, 16);
    assert_eq!(executor.cpu_affinity, None);
}

#[cfg(target_os = "linux")]
#[test]
fn runtime_executor_preserves_cpu_affinity_config()
{
    let current_cpu = unsafe { libc::sched_getcpu() };
    assert!(current_cpu >= 0, "sched_getcpu failed");

    let executor = new_executor_with(16, Some(current_cpu as usize));
    assert_eq!(executor.cpu_affinity, Some(current_cpu as usize));
}

#[cfg(target_os = "linux")]
#[test]
fn runtime_executor_runs_with_cpu_affinity()
{
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
fn runtime_executor_keeps_cpu_affinity_across_spawned_work()
{
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
fn runtime_layout_probe()
{
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
fn runtime_executor_runs_immediate_task()
{
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
fn runtime_executor_runs_nop_future()
{
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
fn runtime_executor_runs_spawned_task_and_drains()
{
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
fn runtime_sleep_completes()
{
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
fn runtime_sleep_ordering()
{
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
fn runtime_sleep_ordering_across_cascade_boundary()
{
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
fn runtime_sleep_uses_fresh_tick_after_idle_gap()
{
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
fn runtime_sleep_until_uses_fresh_tick_after_idle_gap()
{
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
fn runtime_sleep_can_be_cancelled_by_drop()
{
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
fn runtime_timeout_completes_before_deadline()
{
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
fn runtime_timeout_expires()
{
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
fn runtime_timeout_at_completes()
{
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
fn runtime_nop_slot_can_be_reused()
{
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
fn runtime_concurrent_io_tasks()
{
    let mut executor = new_executor();
    let num_pairs = 4;
    let rounds = 50;
    let msg_size = 64;
    let completed = Rc::new(Cell::new(0usize));
    let completed_flag = completed.clone();

    executor
        .run(async move {
            for _ in 0..num_pairs
            {
                let done = completed_flag.clone();
                let (mut pinger, mut ponger) = UnixStream::pair().expect("socketpair failed");

                // Spawn ponger.
                Executor::spawn(async move {
                    for _ in 0..rounds
                    {
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
                    for _ in 0..rounds
                    {
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
fn runtime_join_handle_returns_value()
{
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
fn runtime_join_handle_returns_string()
{
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
fn runtime_join_handle_detach_on_drop()
{
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
fn runtime_join_handle_multiple_concurrent()
{
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
fn runtime_join_handle_with_io()
{
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
fn runtime_join_handle_is_finished()
{
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
fn runtime_cancel_in_flight_read_on_drop()
{
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

/// Dropping a single write future mid-flight cancels the operation cleanly.
#[test]
fn runtime_cancel_in_flight_write_on_drop()
{
    let mut executor = new_executor();

    executor
        .run(async move {
            let (mut left, _right) = UnixStream::pair().expect("socketpair failed");

            // Fill the socket buffer so the next single write blocks.
            // Write in a loop until we get backpressure, then timeout on the blocking write.
            loop
            {
                let buf = vec![0xAAu8; 65536];
                let result = timeout(Duration::from_millis(5), async {
                    let (res, _buf) = left.write(buf).await;
                    res
                })
                .await;

                if result.is_err()
                {
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
fn runtime_cancel_multiple_concurrent()
{
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
fn runtime_cancel_write_all_mid_flight()
{
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

/// Dropping a read_exact future mid-flight after partial progress cancels the
/// outstanding SQE and reclaims the CompletionState exactly once.
#[test]
fn runtime_cancel_read_exact_mid_flight()
{
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
fn runtime_nop_default_trait()
{
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
fn runtime_nop_slot_default_trait()
{
    let mut executor = new_executor();

    executor
        .run(async move {
            let mut slot = NopSlot::default();
            let nop = slot.nop().expect("slot nop failed");
            nop.await.expect("nop failed");
        })
        .expect("executor run failed");
}
