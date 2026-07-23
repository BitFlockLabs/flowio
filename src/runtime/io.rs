//! Development-only `io_uring` `NOP` operations.
//!
//! These APIs submit real `IORING_OP_NOP` entries so tests and benchmarks can
//! exercise executor/reactor completion behavior without transport setup.
//! They are compiled only for crate tests or the `test-support` feature and are
//! not application I/O primitives.
//!
//! # Example
//! ```no_run
//! # #[cfg(feature = "test-support")]
//! # {
//! use flowio::runtime::executor::Executor;
//! use flowio::test_support::runtime::io::Nop;
//!
//! let mut executor = Executor::new()?;
//! executor.run(async {
//!     let _ = Nop::new().await;
//! })?;
//! # }
//! # Ok::<(), std::io::Error>(())
//! ```

use crate::runtime::executor::{
    completed_op_ctx_from_waker, drop_op_ptr_unchecked, poll_ctx_from_waker,
    refresh_op_waiter_from_waker, submit_tracked_sqe,
};
use crate::runtime::op::CompletionState;
use io_uring::opcode;
use std::future::Future;
use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};

/// Completes and frees a finished `NOP` submission if one is ready.
#[inline(always)]
fn complete_nop_op(
    cx: &mut Context<'_>,
    state_ptr: &mut *mut CompletionState,
) -> Option<io::Result<i32>> {
    if state_ptr.is_null() {
        return None;
    }

    let state = unsafe { &**state_ptr };
    if !state.is_completed() {
        return None;
    }

    let result = state.result;
    let op_ctx = unsafe { completed_op_ctx_from_waker(cx, *state_ptr) };
    unsafe { (*op_ctx.reactor()).free_op(*state_ptr) };
    *state_ptr = std::ptr::null_mut();

    Some(if op_ctx.context_rejected() {
        Err(io::Error::from(io::ErrorKind::NotConnected))
    } else if result < 0 {
        Err(io::Error::from_raw_os_error(-result))
    } else {
        Ok(result)
    })
}

/// Polls one `NOP` operation stored in `state_ptr`.
#[inline(always)]
fn poll_nop_op(
    cx: &mut Context<'_>,
    state_ptr: &mut *mut CompletionState,
) -> Poll<io::Result<i32>> {
    let pctx = match poll_ctx_from_waker(cx) {
        Ok(pctx) => pctx,
        Err(err) => {
            unsafe { drop_op_ptr_unchecked(state_ptr) };
            return Poll::Ready(Err(err));
        }
    };

    if !state_ptr.is_null() && unsafe { (**state_ptr).owner_ptr() } != pctx.owner_ptr() {
        unsafe { drop_op_ptr_unchecked(state_ptr) };
        return Poll::Ready(Err(io::Error::from(io::ErrorKind::NotConnected)));
    }

    if let Some(result) = complete_nop_op(cx, state_ptr) {
        return Poll::Ready(result);
    }

    if state_ptr.is_null() {
        let new_state_ptr = unsafe { (*pctx.reactor()).alloc_op() };
        if new_state_ptr.is_null() {
            return Poll::Ready(Err(io::Error::from(io::ErrorKind::WouldBlock)));
        }
        *state_ptr = new_state_ptr;

        unsafe { (*new_state_ptr).register_waiter(pctx.owner_task()) };

        let sqe = opcode::Nop::new().build().user_data(new_state_ptr as u64);

        unsafe {
            if let Err(e) = submit_tracked_sqe(&pctx, sqe) {
                (*pctx.reactor()).free_op(new_state_ptr);
                *state_ptr = std::ptr::null_mut();
                return Poll::Ready(Err(e));
            }
        }
        return Poll::Pending;
    }

    if unsafe { refresh_op_waiter_from_waker(cx, *state_ptr) } {
        unsafe { drop_op_ptr_unchecked(state_ptr) };
        return Poll::Ready(Err(io::Error::from(io::ErrorKind::NotConnected)));
    }
    Poll::Pending
}

/// Reusable slot metadata for a `NOP` operation.
///
/// Each call borrows the slot until its [`NopFuture`] completes or is dropped,
/// preventing overlapping submissions through the same slot. Every submitted
/// `NOP` still receives a fresh reactor `CompletionState`; the slot does not
/// cache operation-pool storage. Use [`Nop`] when no reusable owner is needed.
///
/// # Example
/// ```no_run
/// # #[cfg(feature = "test-support")]
/// # {
/// use flowio::runtime::executor::Executor;
/// use flowio::test_support::runtime::io::NopSlot;
///
/// let mut executor = Executor::new()?;
/// executor.run(async {
///     let mut slot = NopSlot::new();
///     if let Ok(fut) = slot.nop() {
///         let _ = fut.await;
///     }
///     if let Ok(fut) = slot.nop() {
///         let _ = fut.await;
///     }
/// })?;
/// # }
/// # Ok::<(), std::io::Error>(())
/// ```
#[doc(hidden)]
pub struct NopSlot {
    /// Completion slot for the currently armed `NOP`, if any.
    state_ptr: *mut CompletionState,
    /// True while a borrowed [`NopFuture`] exists for this slot.
    in_use: bool,
}

/// Equivalent to [`NopSlot::new()`].
impl Default for NopSlot {
    fn default() -> Self {
        Self {
            state_ptr: std::ptr::null_mut(),
            in_use: false,
        }
    }
}

impl NopSlot {
    /// Creates an empty `NOP` slot.
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns a future that submits one `IORING_OP_NOP` through this slot.
    pub fn nop(&mut self) -> io::Result<NopFuture<'_>> {
        if self.in_use {
            return Err(io::Error::from(io::ErrorKind::WouldBlock));
        }

        debug_assert!(
            self.state_ptr.is_null() || unsafe { (*self.state_ptr).is_completed() },
            "nop slot still in flight"
        );
        debug_assert!(
            self.state_ptr.is_null(),
            "completed nop slot should have been reclaimed before reuse"
        );
        self.in_use = true;
        Ok(NopFuture { slot: self })
    }
}

/// One-shot `IORING_OP_NOP` future with its own submitted operation state.
///
/// This is the owning alternative to the borrowed [`NopFuture`] returned by
/// [`NopSlot::nop`].
///
/// # Example
/// ```no_run
/// # #[cfg(feature = "test-support")]
/// # {
/// use flowio::runtime::executor::Executor;
/// use flowio::test_support::runtime::io::Nop;
///
/// let mut executor = Executor::new()?;
/// executor.run(async {
///     let _ = Nop::new().await;
/// })?;
/// # }
/// # Ok::<(), std::io::Error>(())
/// ```
pub struct Nop {
    /// Completion slot for this one-shot `NOP` submission.
    state_ptr: *mut CompletionState,
}

/// Equivalent to [`Nop::new()`].
impl Default for Nop {
    fn default() -> Self {
        Self {
            state_ptr: std::ptr::null_mut(),
        }
    }
}

impl Nop {
    /// Creates a new one-shot `NOP` future.
    pub fn new() -> Self {
        Self::default()
    }
}

impl Future for Nop {
    type Output = io::Result<i32>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = unsafe { self.get_unchecked_mut() };
        poll_nop_op(cx, &mut this.state_ptr)
    }
}

/// Borrowed `NOP` future backed by a reusable [`NopSlot`].
#[doc(hidden)]
pub struct NopFuture<'a> {
    /// Reusable slot borrowed for the lifetime of this future.
    slot: &'a mut NopSlot,
}

impl Future for NopFuture<'_> {
    type Output = io::Result<i32>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = unsafe { self.get_unchecked_mut() };

        let poll = poll_nop_op(cx, &mut this.slot.state_ptr);
        if poll.is_ready() {
            this.slot.in_use = false;
        }
        poll
    }
}

impl Drop for Nop {
    fn drop(&mut self) {
        unsafe { drop_op_ptr_unchecked(&mut self.state_ptr) };
    }
}

impl Drop for NopFuture<'_> {
    fn drop(&mut self) {
        unsafe { drop_op_ptr_unchecked(&mut self.slot.state_ptr) };
        self.slot.in_use = false;
    }
}

#[cfg(all(test, not(miri)))]
mod tests {
    use super::*;
    use crate::runtime::executor::Executor;

    #[test]
    fn ring_abandoned_nop_reports_not_connected_and_clears_its_pointer() {
        let mut executor = Executor::new().expect("executor construction failed");

        executor
            .run(async {
                std::future::poll_fn(|cx| {
                    let pctx = poll_ctx_from_waker(cx).expect("FlowIO poll context missing");
                    let state_ptr = unsafe { (*pctx.reactor()).alloc_op() };
                    assert!(!state_ptr.is_null(), "NOP test state allocation failed");
                    unsafe { (*state_ptr).set_ring_abandoned() };
                    let mut nop = Nop { state_ptr };

                    assert!(matches!(
                        Pin::new(&mut nop).poll(cx),
                        Poll::Ready(Err(err)) if err.kind() == io::ErrorKind::NotConnected
                    ));
                    assert!(nop.state_ptr.is_null());
                    assert!(unsafe { (*state_ptr).is_ring_abandoned() });
                    assert!(!unsafe { (*state_ptr).is_completed() });

                    // This state was fabricated without an SQE or retained
                    // payload, so the test may return it after observing the
                    // production branch. Real abandoned states stay leaked.
                    unsafe { (*pctx.reactor()).free_op(state_ptr) };
                    Poll::Ready(())
                })
                .await;
            })
            .expect("ring-abandoned NOP test run failed");
    }
}
