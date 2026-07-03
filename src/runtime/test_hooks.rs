//! Debug-only fault injection hooks for integration tests.
//!
//! These hooks are intentionally excluded from release builds. They make
//! otherwise unreachable pressure paths deterministic without changing
//! production behavior.

use std::cell::Cell;
use std::io;

thread_local! {
    static FAIL_OP_ALLOCS: Cell<usize> = const { Cell::new(0) };
    static FAIL_SQE_SUBMITS: Cell<usize> = const { Cell::new(0) };
    static FAIL_RAW_SQE_SUBMITS: Cell<usize> = const { Cell::new(0) };
    static FAIL_IOBUFF_POOL_SLAB_ALLOCS: Cell<usize> = const { Cell::new(0) };
    static FAIL_REACTOR_EXT_ARG_PROBES: Cell<usize> = const { Cell::new(0) };
}

/// Makes the next completion-state allocation on this thread fail.
#[doc(hidden)]
pub fn fail_next_op_alloc() {
    FAIL_OP_ALLOCS.with(|fails| fails.set(fails.get().saturating_add(1)));
}

/// Makes the next tracked SQE submission on this thread fail with
/// `WouldBlock`.
#[doc(hidden)]
pub fn fail_next_sqe_submit() {
    FAIL_SQE_SUBMITS.with(|fails| fails.set(fails.get().saturating_add(1)));
}

/// Makes the next raw reactor SQE submission on this thread fail with
/// `WouldBlock`.
#[doc(hidden)]
#[allow(dead_code)]
pub(crate) fn fail_next_raw_sqe_submit() {
    FAIL_RAW_SQE_SUBMITS.with(|fails| fails.set(fails.get().saturating_add(1)));
}

/// Makes the next `IoBuffPool` slab allocation on this thread fail.
#[doc(hidden)]
#[allow(dead_code)]
pub(crate) fn fail_next_iobuff_pool_slab_alloc() {
    FAIL_IOBUFF_POOL_SLAB_ALLOCS.with(|fails| fails.set(fails.get().saturating_add(1)));
}

/// Makes the next reactor feature validation report missing
/// `IORING_ENTER_EXT_ARG` support.
#[doc(hidden)]
#[allow(dead_code)]
pub(crate) fn fail_next_reactor_ext_arg_probe() {
    FAIL_REACTOR_EXT_ARG_PROBES.with(|fails| fails.set(fails.get().saturating_add(1)));
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
pub(crate) fn take_sqe_submit_failure() -> Option<io::Error> {
    FAIL_SQE_SUBMITS.with(|fails| {
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
