//! Shared owner-thread intrusive reference-count mechanics.

use std::cell::Cell;

/// Increments a live non-atomic reference count or aborts before it can wrap.
///
/// Safe clone surfaces cannot report reference-count exhaustion. Aborting at
/// the unreachable `usize::MAX` boundary preserves memory safety without
/// adding allocation, synchronization, or an unwind path to ordinary retains.
#[inline(always)]
pub(crate) fn increment_refcount(counter: &Cell<usize>) {
    let Some(next) = counter.get().checked_add(1) else {
        abort_refcount_overflow();
    };
    counter.set(next);
}

/// Decrements a live non-atomic reference count and reports its final release.
///
/// Stores zero before returning true so the caller can then destroy the owner.
#[inline(always)]
pub(crate) fn decrement_refcount(counter: &Cell<usize>) -> bool {
    let prev = counter.get();
    debug_assert!(prev > 0, "runtime refcount underflow");
    counter.set(prev - 1);
    prev == 1
}

#[cold]
#[inline(never)]
fn abort_refcount_overflow() -> ! {
    std::process::abort()
}

#[cfg(test)]
pub(crate) mod tests {
    #[cfg(debug_assertions)]
    #[test]
    fn decrement_refcount_underflow_preserves_zero_and_static_diagnostic() {
        let counter = std::cell::Cell::new(0usize);
        let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            super::decrement_refcount(&counter)
        }))
        .expect_err("zero reference count decrement unexpectedly succeeded");

        assert_eq!(
            panic.downcast_ref::<&'static str>(),
            Some(&"runtime refcount underflow")
        );
        assert_eq!(counter.get(), 0);
    }

    const CHILD_CASE_ENV: &str = "FLOWIO_REFCOUNT_OVERFLOW_CHILD_CASE";
    #[cfg(not(miri))]
    const CHILD_TEST_NAME: &str = "runtime::refcount::tests::refcount_overflow_child";

    #[test]
    fn refcount_overflow_child() {
        let Some(case) = std::env::var_os(CHILD_CASE_ENV) else {
            return;
        };

        match case.to_str() {
            Some("task-waker") => {
                crate::runtime::task::tests::trigger_task_waker_refcount_overflow()
            }
            Some("iobuff") => crate::runtime::buffer::trigger_iobuff_refcount_overflow(),
            _ => panic!("unknown refcount-overflow child case: {case:?}"),
        }

        panic!("refcount-overflow child returned instead of aborting");
    }

    #[cfg(not(miri))]
    #[test]
    fn safe_refcount_overflow_aborts_for_task_waker_and_iobuff() {
        use std::os::unix::process::ExitStatusExt;
        use std::process::Command;

        let current_exe = std::env::current_exe().expect("current unit-test executable");
        for case in ["task-waker", "iobuff"] {
            let output = Command::new(&current_exe)
                .args(["--exact", CHILD_TEST_NAME, "--nocapture"])
                .env(CHILD_CASE_ENV, case)
                .current_dir(std::env::temp_dir())
                .output()
                .expect("run refcount-overflow child process");

            assert_eq!(
                output.status.signal(),
                Some(libc::SIGABRT),
                "{case} overflow did not abort with SIGABRT; status={:?}, stdout={}, stderr={}",
                output.status,
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr),
            );
        }
    }
}
