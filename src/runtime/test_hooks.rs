//! Development-only fault injection hooks for repository tests.
//!
//! The module is compiled in debug builds or when `test-support` is enabled;
//! normal release builds exclude it. The thread-local counters make otherwise
//! unreachable pressure and kernel-error paths deterministic without changing
//! production behavior when no hook is armed.

#![allow(dead_code)]

use std::cell::Cell;
use std::io;
thread_local! {
    static FAIL_OP_ALLOCS: Cell<usize> = const { Cell::new(0) };
    static FAIL_TIMER_ALLOCS: Cell<usize> = const { Cell::new(0) };
    static FAIL_RAW_SQE_SUBMITS: Cell<usize> = const { Cell::new(0) };
    static FAIL_RING_SUBMITS: Cell<usize> = const { Cell::new(0) };
    static FAIL_RING_SUBMIT_ERRNO: Cell<i32> = const { Cell::new(0) };
    static FAIL_RING_WAITS: Cell<usize> = const { Cell::new(0) };
    static FAIL_RING_WAIT_ERRNO: Cell<i32> = const { Cell::new(0) };
    static FAIL_IOBUFF_POOL_SLAB_ALLOCS: Cell<usize> = const { Cell::new(0) };
    static FAIL_REACTOR_EXT_ARG_PROBES: Cell<usize> = const { Cell::new(0) };
    static FORCE_REACTOR_SHUTDOWN_FALLBACKS: Cell<usize> = const { Cell::new(0) };
}

/// Makes the next completion-state allocation on this thread fail.
#[doc(hidden)]
pub fn fail_next_op_alloc() {
    FAIL_OP_ALLOCS.with(|fails| fails.set(fails.get().saturating_add(1)));
}

/// Makes the next timer-entry allocation on this thread fail.
#[doc(hidden)]
pub fn fail_next_timer_alloc() {
    FAIL_TIMER_ALLOCS.with(|fails| fails.set(fails.get().saturating_add(1)));
}

/// Makes the next raw reactor SQE submission on this thread fail with
/// `WouldBlock`.
#[doc(hidden)]
pub fn fail_next_sqe_submit() {
    fail_next_raw_sqe_submit();
}

/// Makes the next raw reactor SQE submission on this thread fail with
/// `WouldBlock`.
#[doc(hidden)]
pub(crate) fn fail_next_raw_sqe_submit() {
    FAIL_RAW_SQE_SUBMITS.with(|fails| fails.set(fails.get().saturating_add(1)));
}

/// Makes the next raw `io_uring_enter` submit call on this thread fail with a
/// specific OS errno.
#[doc(hidden)]
pub fn fail_next_ring_submit_errno(errno: i32) {
    FAIL_RING_SUBMITS.with(|fails| fails.set(fails.get().saturating_add(1)));
    FAIL_RING_SUBMIT_ERRNO.with(|stored| stored.set(errno));
}

/// Makes the next `io_uring_enter` wait call on this thread fail with a
/// specific OS errno.
#[doc(hidden)]
pub fn fail_next_ring_wait_errno(errno: i32) {
    FAIL_RING_WAITS.with(|fails| fails.set(fails.get().saturating_add(1)));
    FAIL_RING_WAIT_ERRNO.with(|stored| stored.set(errno));
}

/// Makes the next `IoBuffPool` slab allocation on this thread fail.
#[doc(hidden)]
pub(crate) fn fail_next_iobuff_pool_slab_alloc() {
    FAIL_IOBUFF_POOL_SLAB_ALLOCS.with(|fails| fails.set(fails.get().saturating_add(1)));
}

/// Makes the next reactor feature validation report missing
/// `IORING_ENTER_EXT_ARG` support.
#[doc(hidden)]
pub(crate) fn fail_next_reactor_ext_arg_probe() {
    FAIL_REACTOR_EXT_ARG_PROBES.with(|fails| fails.set(fails.get().saturating_add(1)));
}

/// Makes the next reactor shutdown skip its ordinary bounded drain and enter
/// the fallback path immediately.
#[doc(hidden)]
pub fn force_next_reactor_shutdown_fallback() {
    FORCE_REACTOR_SHUTDOWN_FALLBACKS.with(|forces| {
        forces.set(forces.get().saturating_add(1));
    });
}

#[inline(always)]
pub(crate) fn take_op_alloc_failure() -> bool {
    FAIL_OP_ALLOCS.with(|fails| {
        let remaining = fails.get();
        if remaining == 0 {
            false
        } else {
            fails.set(remaining - 1);
            true
        }
    })
}

#[inline(always)]
pub(crate) fn take_timer_alloc_failure() -> bool {
    FAIL_TIMER_ALLOCS.with(|fails| {
        let remaining = fails.get();
        if remaining == 0 {
            false
        } else {
            fails.set(remaining - 1);
            true
        }
    })
}

#[inline(always)]
pub(crate) fn take_raw_sqe_submit_failure() -> Option<io::Error> {
    FAIL_RAW_SQE_SUBMITS.with(|fails| {
        let remaining = fails.get();
        if remaining == 0 {
            None
        } else {
            fails.set(remaining - 1);
            Some(io::Error::from(io::ErrorKind::WouldBlock))
        }
    })
}

#[inline(always)]
pub(crate) fn raw_sqe_submit_failures_remaining() -> usize {
    FAIL_RAW_SQE_SUBMITS.with(Cell::get)
}

#[inline(always)]
pub(crate) fn take_ring_submit_failure() -> Option<io::Error> {
    FAIL_RING_SUBMITS.with(|fails| {
        let remaining = fails.get();
        if remaining == 0 {
            None
        } else {
            fails.set(remaining - 1);
            let errno = FAIL_RING_SUBMIT_ERRNO.with(|stored| stored.get());
            if errno == 0 {
                Some(io::Error::from(io::ErrorKind::WouldBlock))
            } else {
                Some(io::Error::from_raw_os_error(errno))
            }
        }
    })
}

#[inline(always)]
pub(crate) fn take_ring_wait_failure() -> Option<io::Error> {
    FAIL_RING_WAITS.with(|fails| {
        let remaining = fails.get();
        if remaining == 0 {
            None
        } else {
            fails.set(remaining - 1);
            let errno = FAIL_RING_WAIT_ERRNO.with(|stored| stored.get());
            Some(io::Error::from_raw_os_error(errno))
        }
    })
}

#[doc(hidden)]
pub fn ring_wait_failures_remaining() -> usize {
    FAIL_RING_WAITS.with(Cell::get)
}

/// Returns the number of injected ring-submit failures not yet consumed on
/// this thread.
#[doc(hidden)]
pub fn ring_submit_failures_remaining() -> usize {
    FAIL_RING_SUBMITS.with(Cell::get)
}

#[inline(always)]
pub(crate) fn take_iobuff_pool_slab_alloc_failure() -> bool {
    FAIL_IOBUFF_POOL_SLAB_ALLOCS.with(|fails| {
        let remaining = fails.get();
        if remaining == 0 {
            false
        } else {
            fails.set(remaining - 1);
            true
        }
    })
}

#[inline(always)]
pub(crate) fn take_reactor_ext_arg_probe_failure() -> bool {
    FAIL_REACTOR_EXT_ARG_PROBES.with(|fails| {
        let remaining = fails.get();
        if remaining == 0 {
            false
        } else {
            fails.set(remaining - 1);
            true
        }
    })
}

#[inline(always)]
pub(crate) fn take_reactor_shutdown_fallback() -> bool {
    FORCE_REACTOR_SHUTDOWN_FALLBACKS.with(|forces| {
        let remaining = forces.get();
        if remaining == 0 {
            false
        } else {
            forces.set(remaining - 1);
            true
        }
    })
}

/// Returns the number of forced reactor-shutdown fallbacks not yet consumed on
/// this thread.
#[doc(hidden)]
pub fn reactor_shutdown_fallbacks_remaining() -> usize {
    FORCE_REACTOR_SHUTDOWN_FALLBACKS.with(Cell::get)
}
