//! Minimal runtime-owned operations used to bootstrap the executor.
//!
//! # Example
//! ```no_run
//! use flowio::runtime::executor::Executor;
//! use flowio::runtime::io::Nop;
//!
//! let mut executor = Executor::new()?;
//! executor.run(async {
//!     let _ = Nop::new().await;
//! })?;
//! # Ok::<(), std::io::Error>(())
//! ```

use crate::runtime::executor::{drop_op_ptr_unchecked, poll_ctx_from_waker, submit_tracked_sqe};
use crate::runtime::op::CompletionState;
use io_uring::opcode;
use std::future::Future;
use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};

#[inline(always)]
fn complete_nop_op(
    cx: &mut Context<'_>,
    state_ptr: &mut *mut CompletionState,
) -> Option<io::Result<i32>>
{
    if state_ptr.is_null()
    {
        return None;
    }

    let state = unsafe { &**state_ptr };
    if !state.is_completed()
    {
        return None;
    }

    let result = state.result;
    let pctx = unsafe { poll_ctx_from_waker(cx) };
    unsafe { (*pctx.reactor()).free_op(*state_ptr) };
    *state_ptr = std::ptr::null_mut();

    Some(
        if result < 0
        {
            Err(io::Error::from_raw_os_error(-result))
        }
        else
        {
            Ok(result)
        },
    )
}

/// Reusable slot metadata for a `NOP` operation.
///
/// The slot itself is reused across calls, while each submitted `NOP` still
/// gets a fresh `CompletionState` from the reactor pool.
///
/// # Example
/// ```no_run
/// use flowio::runtime::executor::Executor;
/// use flowio::runtime::io::NopSlot;
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
/// # Ok::<(), std::io::Error>(())
/// ```
pub struct NopSlot
{
    state_ptr: *mut CompletionState,
    in_use: bool,
}

/// Equivalent to [`NopSlot::new()`].
impl Default for NopSlot
{
    fn default() -> Self
    {
        Self {
            state_ptr: std::ptr::null_mut(),
            in_use: false,
        }
    }
}

impl NopSlot
{
    /// Creates an empty `NOP` slot.
    pub fn new() -> Self
    {
        Self::default()
    }

    /// Returns a future that submits one `IORING_OP_NOP` through this slot.
    pub fn nop(&mut self) -> io::Result<NopFuture<'_>>
    {
        if self.in_use
        {
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
/// # Example
/// ```no_run
/// use flowio::runtime::executor::Executor;
/// use flowio::runtime::io::Nop;
///
/// let mut executor = Executor::new()?;
/// executor.run(async {
///     let _ = Nop::new().await;
/// })?;
/// # Ok::<(), std::io::Error>(())
/// ```
pub struct Nop
{
    state_ptr: *mut CompletionState,
}

/// Equivalent to [`Nop::new()`].
impl Default for Nop
{
    fn default() -> Self
    {
        Self {
            state_ptr: std::ptr::null_mut(),
        }
    }
}

impl Nop
{
    /// Creates a new one-shot `NOP` future.
    pub fn new() -> Self
    {
        Self::default()
    }
}

impl Future for Nop
{
    type Output = io::Result<i32>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output>
    {
        let this = unsafe { self.get_unchecked_mut() };

        if let Some(result) = complete_nop_op(cx, &mut this.state_ptr)
        {
            return Poll::Ready(result);
        }

        if this.state_ptr.is_null()
        {
            let pctx = unsafe { poll_ctx_from_waker(cx) };
            let state_ptr = unsafe { (*pctx.reactor()).alloc_op() };
            if state_ptr.is_null()
            {
                return Poll::Ready(Err(io::Error::from(io::ErrorKind::WouldBlock)));
            }
            this.state_ptr = state_ptr;

            unsafe { (*state_ptr).register_waiter(pctx.owner_task()) };

            let sqe = opcode::Nop::new().build().user_data(state_ptr as u64);

            unsafe {
                if let Err(e) = submit_tracked_sqe(&pctx, sqe)
                {
                    (*pctx.reactor()).free_op(state_ptr);
                    this.state_ptr = std::ptr::null_mut();
                    return Poll::Ready(Err(e));
                }
            }
        }

        Poll::Pending
    }
}

/// Borrowed `NOP` future backed by a reusable [`NopSlot`].
#[doc(hidden)]
pub struct NopFuture<'a>
{
    slot: &'a mut NopSlot,
}

impl Future for NopFuture<'_>
{
    type Output = io::Result<i32>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output>
    {
        let this = unsafe { self.get_unchecked_mut() };

        if let Some(result) = complete_nop_op(cx, &mut this.slot.state_ptr)
        {
            this.slot.in_use = false;
            return Poll::Ready(result);
        }

        if this.slot.state_ptr.is_null()
        {
            let pctx = unsafe { poll_ctx_from_waker(cx) };
            let state_ptr = unsafe { (*pctx.reactor()).alloc_op() };
            if state_ptr.is_null()
            {
                this.slot.in_use = false;
                return Poll::Ready(Err(io::Error::from(io::ErrorKind::WouldBlock)));
            }
            this.slot.state_ptr = state_ptr;

            unsafe { (*state_ptr).register_waiter(pctx.owner_task()) };

            let sqe = opcode::Nop::new().build().user_data(state_ptr as u64);

            unsafe {
                if let Err(e) = submit_tracked_sqe(&pctx, sqe)
                {
                    (*pctx.reactor()).free_op(state_ptr);
                    this.slot.state_ptr = std::ptr::null_mut();
                    this.slot.in_use = false;
                    return Poll::Ready(Err(e));
                }
            }
        }

        Poll::Pending
    }
}

impl Drop for Nop
{
    fn drop(&mut self)
    {
        unsafe { drop_op_ptr_unchecked(&mut self.state_ptr) };
    }
}

impl Drop for NopFuture<'_>
{
    fn drop(&mut self)
    {
        unsafe { drop_op_ptr_unchecked(&mut self.slot.state_ptr) };
        self.slot.in_use = false;
    }
}
