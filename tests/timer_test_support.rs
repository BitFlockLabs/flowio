use flowio::test_support::runtime::timer::TimerRuntime;
use std::io;

#[cfg(not(miri))]
use flowio::runtime::executor::Executor;

#[test]
fn timer_runtime_driver_rejects_missing_executor_context() {
    let mut timers = TimerRuntime::new().expect("timer runtime construction failed");
    timers.init().expect("timer runtime initialization failed");
    let now = timers.now_tick().expect("monotonic timer sample failed");

    let error = timers
        .process_at_with_budget(now, usize::MAX)
        .expect_err("timer driver accepted a missing executor context");

    assert_eq!(error.kind(), io::ErrorKind::NotConnected);
    assert!(!timers.has_pending());
}

#[cfg(not(miri))]
#[test]
fn timer_runtime_driver_accepts_active_executor_context() {
    let mut executor = Executor::new().expect("executor construction failed");

    executor
        .run(async {
            let mut timers = TimerRuntime::new().expect("timer runtime construction failed");
            timers.init().expect("timer runtime initialization failed");
            let now = timers.now_tick().expect("monotonic timer sample failed");

            assert!(
                !timers
                    .process_at_with_budget(now, usize::MAX)
                    .expect("timer driver rejected the active executor")
            );
        })
        .expect("executor failed while exercising the timer driver");
}
