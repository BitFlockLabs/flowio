//! Shared io_uring futures for byte-stream transports and FlowIO's TLS wrapper.
//!
//! These futures use `IORING_OP_READ` / `IORING_OP_WRITE` and handle partial
//! completion internally for the `_all` / `_exact` variants.  The stream type
//! parameter `S` is carried only in `PhantomData` to borrow the parent stream
//! for the duration of the operation.
//!
//! Vectored operations materialize `iovec` arrays into future-owned scratch
//! storage. Partial progress advances that scratch in place, and the retry
//! path can downgrade to `IORING_OP_READ` / `IORING_OP_WRITE` when only one
//! segment remains.
//!
//! `CompletionState` is allocated from the reactor's pool per submitted SQE
//! and freed once that submission is retired.
//!
//! If a future is dropped while its SQE is still in flight, the state is
//! marked orphaned and an `ASYNC_CANCEL` SQE is submitted; the CQE path then
//! reclaims the pool slot. If a future is dropped after completion but before
//! polling its result, the completed state is freed immediately from `Drop`.

use crate::runtime::buffer::iobuffvec::{IoBuffReadOnlyVec, IoBuffVec, IoBuffVecMut};
use crate::runtime::buffer::{IoBuffReadOnly, IoBuffReadWrite};
use crate::runtime::executor::{
    PollCtx, drop_op_ptr_unchecked, poll_ctx_from_waker, submit_tracked_sqe,
};
use crate::runtime::op::CompletionState;
use io_uring::{opcode, squeue, types};
use std::future::Future;
use std::io;
use std::marker::PhantomData;
use std::mem::MaybeUninit;
use std::os::fd::RawFd;
use std::pin::Pin;
use std::slice;
use std::task::{Context, Poll};

#[inline(always)]
/// Returns the result stored in a completed submission and retires the
/// corresponding completion-state slot.
fn take_completed_result(
    cx: &mut Context<'_>,
    state_ptr: &mut *mut CompletionState,
) -> Option<i32> {
    if state_ptr.is_null() {
        return None;
    }

    let state = unsafe { &**state_ptr };
    if !state.is_completed() {
        return None;
    }

    let result = state.result;
    let pctx = unsafe { poll_ctx_from_waker(cx) };
    unsafe { (*pctx.reactor()).free_op(*state_ptr) };
    *state_ptr = std::ptr::null_mut();
    Some(result)
}

#[inline(always)]
/// Frees a completed retry slot before the next sequential submission.
unsafe fn free_retry_state(pctx: &PollCtx, state_ptr: &mut *mut CompletionState) {
    unsafe { (*pctx.reactor()).free_op(*state_ptr) };
    *state_ptr = std::ptr::null_mut();
}

#[inline(always)]
/// Allocates or resets the retry slot and re-registers the current waiter.
unsafe fn prepare_retry_state(
    pctx: &PollCtx,
    state_ptr: &mut *mut CompletionState,
) -> io::Result<()> {
    if state_ptr.is_null() {
        let new_state_ptr = unsafe { (*pctx.reactor()).alloc_op() };
        if new_state_ptr.is_null() {
            return Err(io::Error::from(io::ErrorKind::WouldBlock));
        }
        *state_ptr = new_state_ptr;
    } else {
        unsafe { (&mut **state_ptr).reset_for_resubmit() };
    }

    unsafe { (&mut **state_ptr).register_waiter(pctx.owner_task()) };
    Ok(())
}

use super::{opt_mut, opt_ref, opt_take};

#[inline(always)]
fn uninit_iovecs<const N: usize>() -> [MaybeUninit<libc::iovec>; N] {
    unsafe { MaybeUninit::uninit().assume_init() }
}

#[inline(always)]
unsafe fn iovec_slice_mut<const N: usize>(
    iovecs: &mut [MaybeUninit<libc::iovec>; N],
    len: usize,
) -> &mut [libc::iovec] {
    unsafe { slice::from_raw_parts_mut(iovecs.as_mut_ptr() as *mut libc::iovec, len) }
}

#[inline(always)]
unsafe fn iovec_ptr<const N: usize>(
    iovecs: &[MaybeUninit<libc::iovec>; N],
    skip: usize,
) -> *const libc::iovec {
    unsafe { iovecs.as_ptr().add(skip) as *const libc::iovec }
}

#[inline(always)]
unsafe fn iovec_ref<const N: usize>(
    iovecs: &[MaybeUninit<libc::iovec>; N],
    index: usize,
) -> &libc::iovec {
    unsafe { &*(iovecs.as_ptr().add(index) as *const libc::iovec) }
}

trait WriteBufferChain<const N: usize>: Sized {
    fn fill_write_iovecs_and_len(&self, dst: &mut [MaybeUninit<libc::iovec>; N]) -> (usize, usize);
}

impl<const N: usize> WriteBufferChain<N> for IoBuffVec<N> {
    #[inline(always)]
    fn fill_write_iovecs_and_len(&self, dst: &mut [MaybeUninit<libc::iovec>; N]) -> (usize, usize) {
        IoBuffVec::fill_write_iovecs_and_len(self, dst)
    }
}

impl<B: IoBuffReadOnly, const N: usize> WriteBufferChain<N> for IoBuffReadOnlyVec<B, N> {
    #[inline(always)]
    fn fill_write_iovecs_and_len(&self, dst: &mut [MaybeUninit<libc::iovec>; N]) -> (usize, usize) {
        IoBuffReadOnlyVec::fill_write_iovecs_and_len(self, dst)
    }
}

#[inline(always)]
/// Builds the next read-style SQE from the current vectored scratch window.
///
/// When only one segment remains and its length fits in `u32`, this downgrades
/// to `IORING_OP_READ` to avoid an unnecessary `readv`.
fn build_read_vectored_entry<const N: usize>(
    fd: RawFd,
    iovecs: &[MaybeUninit<libc::iovec>; N],
    skip: usize,
    count: usize,
    user_data: u64,
) -> squeue::Entry {
    debug_assert!(count > 0, "readv submission requires at least one iovec");

    if count == 1 {
        let iov = unsafe { iovec_ref(iovecs, skip) };
        if let Ok(len) = u32::try_from(iov.iov_len) {
            return opcode::Read::new(types::Fd(fd), iov.iov_base as *mut u8, len)
                .build()
                .user_data(user_data);
        }
    }

    opcode::Readv::new(
        types::Fd(fd),
        unsafe { iovec_ptr(iovecs, skip) } as *const _,
        count as u32,
    )
    .build()
    .user_data(user_data)
}

#[inline(always)]
/// Builds the next write-style SQE from the current vectored scratch window.
///
/// When only one segment remains and its length fits in `u32`, this downgrades
/// to `IORING_OP_WRITE` to avoid an unnecessary `writev`.
fn build_write_vectored_entry<const N: usize>(
    fd: RawFd,
    iovecs: &[MaybeUninit<libc::iovec>; N],
    skip: usize,
    count: usize,
    user_data: u64,
) -> squeue::Entry {
    debug_assert!(count > 0, "writev submission requires at least one iovec");

    if count == 1 {
        let iov = unsafe { iovec_ref(iovecs, skip) };
        if let Ok(len) = u32::try_from(iov.iov_len) {
            return opcode::Write::new(types::Fd(fd), iov.iov_base as *const u8, len)
                .build()
                .user_data(user_data);
        }
    }

    opcode::Writev::new(
        types::Fd(fd),
        unsafe { iovec_ptr(iovecs, skip) },
        count as u32,
    )
    .build()
    .user_data(user_data)
}

/// Advance past `bytes` consumed/filled bytes in an iovec array by mutating
/// the scratch entries in place. `skip` is updated to the first remaining
/// non-empty entry.
#[inline]
fn advance_iovecs_in_place(iovecs: &mut [libc::iovec], skip: &mut usize, bytes: usize) {
    let mut remaining = bytes;
    while remaining > 0 && *skip < iovecs.len() {
        let iov = &mut iovecs[*skip];
        if remaining >= iov.iov_len {
            remaining -= iov.iov_len;
            *skip += 1;
        } else {
            iov.iov_base = unsafe { (iov.iov_base as *mut u8).add(remaining) } as *mut libc::c_void;
            iov.iov_len -= remaining;
            remaining = 0;
        }
    }

    debug_assert!(
        remaining == 0,
        "advance_iovecs_in_place: {} bytes left over after consuming scratch iovecs",
        remaining
    );
}

// ---------------------------------------------------------------------------
// ReadFuture
// ---------------------------------------------------------------------------

/// Single read into a caller-provided buffer (rental pattern).
#[doc(hidden)]
pub struct ReadFuture<'a, B: IoBuffReadWrite, S> {
    /// Completion state for the submitted read SQE, if any.
    state_ptr: *mut CompletionState,
    /// Caller-owned destination buffer returned on completion.
    buffer: Option<B>,
    /// Stream descriptor read from by this future.
    fd: RawFd,
    /// Maximum bytes requested from the kernel.
    len: u32,
    /// Deferred validation error returned before any SQE submission.
    input_error: Option<io::Error>,
    /// Borrows the parent stream for the future lifetime.
    _marker: PhantomData<&'a mut S>,
}

impl<'a, B: IoBuffReadWrite, S> ReadFuture<'a, B, S> {
    pub(crate) fn new(fd: RawFd, buffer: B, len: usize) -> Self {
        let mut input_error = None;
        let len = match super::checked_read_len("read", len, buffer.writable_len()) {
            Ok(len) => len,
            Err(err) => {
                input_error = Some(err);
                0
            }
        };
        Self {
            state_ptr: std::ptr::null_mut(),
            buffer: Some(buffer),
            fd,
            len,
            input_error,
            _marker: PhantomData,
        }
    }
}

impl<B: IoBuffReadWrite, S> Future for ReadFuture<'_, B, S> {
    type Output = (io::Result<usize>, B);

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = unsafe { self.get_unchecked_mut() };

        if this.state_ptr.is_null()
            && let Some(err) = this.input_error.take()
        {
            let buffer = unsafe { opt_take(&mut this.buffer) };
            return Poll::Ready((Err(err), buffer));
        }

        if let Some(result) = take_completed_result(cx, &mut this.state_ptr) {
            let mut buffer = unsafe { opt_take(&mut this.buffer) };
            if result < 0 {
                return Poll::Ready((Err(io::Error::from_raw_os_error(-result)), buffer));
            }
            let actual = result as usize;
            unsafe { buffer.set_written_len(actual) };
            return Poll::Ready((Ok(actual), buffer));
        }

        if this.state_ptr.is_null() {
            let pctx = unsafe { poll_ctx_from_waker(cx) };
            let state_ptr = unsafe { (*pctx.reactor()).alloc_op() };
            if state_ptr.is_null() {
                let buffer = unsafe { opt_take(&mut this.buffer) };
                return Poll::Ready((Err(io::Error::from(io::ErrorKind::WouldBlock)), buffer));
            }
            this.state_ptr = state_ptr;

            unsafe { (*state_ptr).register_waiter(pctx.owner_task()) };

            let buf = unsafe { opt_mut(&mut this.buffer) };
            let ptr = buf.as_mut_ptr();
            let sqe = opcode::Read::new(types::Fd(this.fd), ptr, this.len)
                .build()
                .user_data(state_ptr as u64);

            unsafe {
                if let Err(e) = submit_tracked_sqe(&pctx, sqe) {
                    (*pctx.reactor()).free_op(state_ptr);
                    this.state_ptr = std::ptr::null_mut();
                    let buffer = opt_take(&mut this.buffer);
                    return Poll::Ready((Err(e), buffer));
                }
            }
        }

        Poll::Pending
    }
}

impl<B: IoBuffReadWrite, S> Drop for ReadFuture<'_, B, S> {
    fn drop(&mut self) {
        unsafe { drop_op_ptr_unchecked(&mut self.state_ptr) };
    }
}

// ---------------------------------------------------------------------------
// WriteFuture
// ---------------------------------------------------------------------------

/// Single write from a caller-provided buffer (rental pattern).
#[doc(hidden)]
pub struct WriteFuture<'a, B: IoBuffReadOnly, S> {
    /// Completion state for the submitted write SQE, if any.
    state_ptr: *mut CompletionState,
    /// Caller-owned source buffer returned on completion.
    buffer: Option<B>,
    /// Stream descriptor written by this future.
    fd: RawFd,
    /// Borrows the parent stream for the future lifetime.
    _marker: PhantomData<&'a mut S>,
}

impl<'a, B: IoBuffReadOnly, S> WriteFuture<'a, B, S> {
    pub(crate) fn new(fd: RawFd, buffer: B) -> Self {
        Self {
            state_ptr: std::ptr::null_mut(),
            buffer: Some(buffer),
            fd,
            _marker: PhantomData,
        }
    }
}

impl<B: IoBuffReadOnly, S> Future for WriteFuture<'_, B, S> {
    type Output = (io::Result<usize>, B);

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = unsafe { self.get_unchecked_mut() };

        if let Some(result) = take_completed_result(cx, &mut this.state_ptr) {
            let buffer = unsafe { opt_take(&mut this.buffer) };
            if result < 0 {
                return Poll::Ready((Err(io::Error::from_raw_os_error(-result)), buffer));
            }
            return Poll::Ready((Ok(result as usize), buffer));
        }

        if this.state_ptr.is_null() {
            let pctx = unsafe { poll_ctx_from_waker(cx) };
            let state_ptr = unsafe { (*pctx.reactor()).alloc_op() };
            if state_ptr.is_null() {
                let buffer = unsafe { opt_take(&mut this.buffer) };
                return Poll::Ready((Err(io::Error::from(io::ErrorKind::WouldBlock)), buffer));
            }
            this.state_ptr = state_ptr;

            unsafe { (*state_ptr).register_waiter(pctx.owner_task()) };

            let buf = unsafe { opt_ref(&this.buffer) };
            let ptr = buf.as_ptr();
            let len = buf.len() as u32;
            let sqe = opcode::Write::new(types::Fd(this.fd), ptr, len)
                .build()
                .user_data(state_ptr as u64);

            unsafe {
                if let Err(e) = submit_tracked_sqe(&pctx, sqe) {
                    (*pctx.reactor()).free_op(state_ptr);
                    this.state_ptr = std::ptr::null_mut();
                    let buffer = opt_take(&mut this.buffer);
                    return Poll::Ready((Err(e), buffer));
                }
            }
        }

        Poll::Pending
    }
}

impl<B: IoBuffReadOnly, S> Drop for WriteFuture<'_, B, S> {
    fn drop(&mut self) {
        unsafe { drop_op_ptr_unchecked(&mut self.state_ptr) };
    }
}

// ---------------------------------------------------------------------------
// WriteAllFuture
// ---------------------------------------------------------------------------

/// Writes the entire buffer, re-submitting on partial writes.
///
/// The base buffer pointer is captured once at construction and reused for
/// retries, avoiding repeated `as_ptr()` trait calls.  A single
/// `poll_ctx_from_waker` extraction covers free + alloc + submit per poll.
#[doc(hidden)]
pub struct WriteAllFuture<'a, B: IoBuffReadOnly, S> {
    /// Completion state reused across sequential retry submissions.
    state_ptr: *mut CompletionState,
    /// Caller-owned source buffer returned when the operation finishes.
    buffer: Option<B>,
    /// Stable base pointer into `buffer`, captured once at construction.
    base_ptr: *const u8,
    /// Stream descriptor written by this future.
    fd: RawFd,
    /// Bytes already confirmed written by completed submissions.
    offset: u32,
    /// Total number of bytes that must be written before completion.
    total: u32,
    /// Borrows the parent stream for the future lifetime.
    _marker: PhantomData<&'a mut S>,
}

impl<'a, B: IoBuffReadOnly, S> WriteAllFuture<'a, B, S> {
    pub(crate) fn new(fd: RawFd, buffer: B) -> Self {
        let total = buffer.len() as u32;
        let base_ptr = buffer.as_ptr();
        Self {
            state_ptr: std::ptr::null_mut(),
            buffer: Some(buffer),
            base_ptr,
            fd,
            offset: 0,
            total,
            _marker: PhantomData,
        }
    }
}

impl<B: IoBuffReadOnly, S> Future for WriteAllFuture<'_, B, S> {
    type Output = (io::Result<usize>, B);

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = unsafe { self.get_unchecked_mut() };

        // Fast path: still in flight — return without any context extraction.
        if !this.state_ptr.is_null() {
            let state = unsafe { &*this.state_ptr };
            if !state.is_completed() {
                return Poll::Pending;
            }
        }

        // Zero-length write completes immediately.
        if this.state_ptr.is_null() && this.total == 0 {
            let buffer = unsafe { opt_take(&mut this.buffer) };
            return Poll::Ready((Ok(0), buffer));
        }

        // One context extraction covers free + alloc + submit.
        let pctx = unsafe { poll_ctx_from_waker(cx) };

        // Process completed state if any. Sequential retries reuse the same
        // completion slot once the previous CQE has been fully consumed.
        if !this.state_ptr.is_null() {
            let result = unsafe { (*this.state_ptr).result };

            if result < 0 {
                unsafe { free_retry_state(&pctx, &mut this.state_ptr) };
                let buffer = unsafe { opt_take(&mut this.buffer) };
                return Poll::Ready((Err(io::Error::from_raw_os_error(-result)), buffer));
            }

            let n = result as u32;
            if n == 0 {
                unsafe { free_retry_state(&pctx, &mut this.state_ptr) };
                let buffer = unsafe { opt_take(&mut this.buffer) };
                return Poll::Ready((Err(io::Error::from(io::ErrorKind::WriteZero)), buffer));
            }

            this.offset += n;
            if this.offset >= this.total {
                unsafe { free_retry_state(&pctx, &mut this.state_ptr) };
                let buffer = unsafe { opt_take(&mut this.buffer) };
                return Poll::Ready((Ok(this.offset as usize), buffer));
            }
        }
        if let Err(err) = unsafe { prepare_retry_state(&pctx, &mut this.state_ptr) } {
            let buffer = unsafe { opt_take(&mut this.buffer) };
            return Poll::Ready((Err(err), buffer));
        }

        let ptr = unsafe { this.base_ptr.add(this.offset as usize) };
        let remaining = this.total - this.offset;

        let sqe = opcode::Write::new(types::Fd(this.fd), ptr, remaining)
            .build()
            .user_data(this.state_ptr as u64);

        unsafe {
            if let Err(e) = submit_tracked_sqe(&pctx, sqe) {
                (*pctx.reactor()).free_op(this.state_ptr);
                this.state_ptr = std::ptr::null_mut();
                let buffer = opt_take(&mut this.buffer);
                return Poll::Ready((Err(e), buffer));
            }
        }

        Poll::Pending
    }
}

impl<B: IoBuffReadOnly, S> Drop for WriteAllFuture<'_, B, S> {
    fn drop(&mut self) {
        unsafe { drop_op_ptr_unchecked(&mut self.state_ptr) };
    }
}

// ---------------------------------------------------------------------------
// ReadExactFuture
// ---------------------------------------------------------------------------

/// Reads exactly `target` bytes, re-submitting on partial reads.
///
/// Returns `UnexpectedEof` if the peer closes before the target is reached.
/// On error the buffer reflects the bytes received so far.  Like
/// [`WriteAllFuture`], the base pointer is captured once and a single
/// context extraction covers free + alloc + submit per poll.
#[doc(hidden)]
pub struct ReadExactFuture<'a, B: IoBuffReadWrite, S> {
    /// Completion state reused across sequential retry submissions.
    state_ptr: *mut CompletionState,
    /// Caller-owned destination buffer returned when the operation finishes.
    buffer: Option<B>,
    /// Stable base pointer into the writable region of `buffer`.
    base_ptr: *mut u8,
    /// Stream descriptor read from by this future.
    fd: RawFd,
    /// Exact byte count required before the future can succeed.
    target: u32,
    /// Bytes already read into the destination buffer.
    filled: u32,
    /// Deferred validation error returned before any SQE submission.
    input_error: Option<io::Error>,
    /// Borrows the parent stream for the future lifetime.
    _marker: PhantomData<&'a mut S>,
}

impl<'a, B: IoBuffReadWrite, S> ReadExactFuture<'a, B, S> {
    pub(crate) fn new(fd: RawFd, mut buffer: B, len: usize) -> Self {
        let mut input_error = None;
        let target = match super::checked_read_len("read_exact", len, buffer.writable_len()) {
            Ok(target) => target,
            Err(err) => {
                input_error = Some(err);
                0
            }
        };
        let base_ptr = buffer.as_mut_ptr();
        Self {
            state_ptr: std::ptr::null_mut(),
            buffer: Some(buffer),
            base_ptr,
            fd,
            target,
            filled: 0,
            input_error,
            _marker: PhantomData,
        }
    }
}

impl<B: IoBuffReadWrite, S> Future for ReadExactFuture<'_, B, S> {
    type Output = (io::Result<usize>, B);

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = unsafe { self.get_unchecked_mut() };

        if this.state_ptr.is_null()
            && let Some(err) = this.input_error.take()
        {
            let buffer = unsafe { opt_take(&mut this.buffer) };
            return Poll::Ready((Err(err), buffer));
        }

        // Fast path: still in flight — return without any context extraction.
        if !this.state_ptr.is_null() {
            let state = unsafe { &*this.state_ptr };
            if !state.is_completed() {
                return Poll::Pending;
            }
        }

        // Zero-length read completes immediately.
        if this.state_ptr.is_null() && this.target == 0 {
            let mut buffer = unsafe { opt_take(&mut this.buffer) };
            unsafe { buffer.set_written_len(0) };
            return Poll::Ready((Ok(0), buffer));
        }

        // One context extraction covers free + alloc + submit.
        let pctx = unsafe { poll_ctx_from_waker(cx) };

        // Process completed state if any. Sequential retries reuse the same
        // completion slot once the previous CQE has been fully consumed.
        if !this.state_ptr.is_null() {
            let result = unsafe { (*this.state_ptr).result };

            if result < 0 {
                unsafe { free_retry_state(&pctx, &mut this.state_ptr) };
                let mut buffer = unsafe { opt_take(&mut this.buffer) };
                unsafe { buffer.set_written_len(this.filled as usize) };
                return Poll::Ready((Err(io::Error::from_raw_os_error(-result)), buffer));
            }

            let n = result as u32;
            if n == 0 {
                unsafe { free_retry_state(&pctx, &mut this.state_ptr) };
                let mut buffer = unsafe { opt_take(&mut this.buffer) };
                unsafe { buffer.set_written_len(this.filled as usize) };
                return Poll::Ready((Err(io::Error::from(io::ErrorKind::UnexpectedEof)), buffer));
            }

            this.filled += n;
            if this.filled >= this.target {
                unsafe { free_retry_state(&pctx, &mut this.state_ptr) };
                let mut buffer = unsafe { opt_take(&mut this.buffer) };
                unsafe { buffer.set_written_len(this.target as usize) };
                return Poll::Ready((Ok(this.target as usize), buffer));
            }
        }
        if let Err(err) = unsafe { prepare_retry_state(&pctx, &mut this.state_ptr) } {
            let mut buffer = unsafe { opt_take(&mut this.buffer) };
            unsafe { buffer.set_written_len(this.filled as usize) };
            return Poll::Ready((Err(err), buffer));
        }

        let ptr = unsafe { this.base_ptr.add(this.filled as usize) };
        let remaining = this.target - this.filled;

        let sqe = opcode::Read::new(types::Fd(this.fd), ptr, remaining)
            .build()
            .user_data(this.state_ptr as u64);

        unsafe {
            if let Err(e) = submit_tracked_sqe(&pctx, sqe) {
                (*pctx.reactor()).free_op(this.state_ptr);
                this.state_ptr = std::ptr::null_mut();
                let mut buffer = opt_take(&mut this.buffer);
                buffer.set_written_len(this.filled as usize);
                return Poll::Ready((Err(e), buffer));
            }
        }

        Poll::Pending
    }
}

impl<B: IoBuffReadWrite, S> Drop for ReadExactFuture<'_, B, S> {
    fn drop(&mut self) {
        unsafe { drop_op_ptr_unchecked(&mut self.state_ptr) };
    }
}

// ---------------------------------------------------------------------------
// ReadvFuture
// ---------------------------------------------------------------------------

/// Scatter-read into a vectored buffer chain (rental pattern).
#[doc(hidden)]
pub struct ReadvFuture<'a, const N: usize, S> {
    /// Completion state for the submitted readv/read SQE, if any.
    state_ptr: *mut CompletionState,
    /// Caller-owned mutable segment chain returned on completion.
    buffer: Option<IoBuffVecMut<N>>,
    /// Future-owned `iovec` scratch describing the writable segments.
    iovecs: [MaybeUninit<libc::iovec>; N],
    /// Number of valid entries currently present in `iovecs`.
    iov_count: usize,
    /// Total writable capacity across all segments.
    writable: usize,
    /// Stream descriptor read from by this future.
    fd: RawFd,
    /// Borrows the parent stream for the future lifetime.
    _marker: PhantomData<&'a mut S>,
}

impl<'a, const N: usize, S> ReadvFuture<'a, N, S> {
    pub(crate) fn new(fd: RawFd, buffer: IoBuffVecMut<N>) -> Self {
        let mut buffer = buffer;
        let mut iovecs = uninit_iovecs();
        let (iov_count, writable) = buffer.fill_read_iovecs_and_writable_len(&mut iovecs);
        Self {
            state_ptr: std::ptr::null_mut(),
            buffer: Some(buffer),
            iovecs,
            iov_count,
            writable,
            fd,
            _marker: PhantomData,
        }
    }
}

impl<const N: usize, S> Future for ReadvFuture<'_, N, S> {
    type Output = (io::Result<usize>, IoBuffVecMut<N>);

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = unsafe { self.get_unchecked_mut() };

        if let Some(result) = take_completed_result(cx, &mut this.state_ptr) {
            let mut buffer = unsafe { opt_take(&mut this.buffer) };
            if result < 0 {
                return Poll::Ready((Err(io::Error::from_raw_os_error(-result)), buffer));
            }
            let actual = result as usize;
            unsafe { buffer.distribute_written(actual) };
            return Poll::Ready((Ok(actual), buffer));
        }

        if this.writable == 0 {
            let mut buffer = unsafe { opt_take(&mut this.buffer) };
            unsafe { buffer.distribute_written(0) };
            return Poll::Ready((Ok(0), buffer));
        }

        if this.state_ptr.is_null() {
            let pctx = unsafe { poll_ctx_from_waker(cx) };
            let state_ptr = unsafe { (*pctx.reactor()).alloc_op() };
            if state_ptr.is_null() {
                let buffer = unsafe { opt_take(&mut this.buffer) };
                return Poll::Ready((Err(io::Error::from(io::ErrorKind::WouldBlock)), buffer));
            }
            this.state_ptr = state_ptr;

            unsafe { (*state_ptr).register_waiter(pctx.owner_task()) };

            let sqe = build_read_vectored_entry(
                this.fd,
                &this.iovecs,
                0,
                this.iov_count,
                state_ptr as u64,
            );

            unsafe {
                if let Err(e) = submit_tracked_sqe(&pctx, sqe) {
                    (*pctx.reactor()).free_op(state_ptr);
                    this.state_ptr = std::ptr::null_mut();
                    let buffer = opt_take(&mut this.buffer);
                    return Poll::Ready((Err(e), buffer));
                }
            }
        }

        Poll::Pending
    }
}

impl<const N: usize, S> Drop for ReadvFuture<'_, N, S> {
    fn drop(&mut self) {
        unsafe { drop_op_ptr_unchecked(&mut self.state_ptr) };
    }
}

// ---------------------------------------------------------------------------
// WritevFuture
// ---------------------------------------------------------------------------

/// Shared gather-write future core for owned read-only vectored chains.
struct WritevFutureCore<'a, C: WriteBufferChain<N>, const N: usize, S> {
    /// Completion state for the submitted writev/write SQE, if any.
    state_ptr: *mut CompletionState,
    /// Caller-owned read-only segment chain returned on completion.
    buffer: Option<C>,
    /// Future-owned `iovec` scratch describing non-empty source segments.
    iovecs: [MaybeUninit<libc::iovec>; N],
    /// Number of valid entries currently present in `iovecs`.
    iov_count: usize,
    /// Total initialized bytes available across all segments.
    total: usize,
    /// Stream descriptor written by this future.
    fd: RawFd,
    /// Borrows the parent stream for the future lifetime.
    _marker: PhantomData<&'a mut S>,
}

impl<'a, C: WriteBufferChain<N>, const N: usize, S> WritevFutureCore<'a, C, N, S> {
    fn new(fd: RawFd, buffer: C) -> Self {
        let mut iovecs = uninit_iovecs();
        let (iov_count, total) = buffer.fill_write_iovecs_and_len(&mut iovecs);
        debug_assert!(
            total == 0 || iov_count > 0,
            "non-empty write chain produced no iovecs"
        );
        Self {
            state_ptr: std::ptr::null_mut(),
            buffer: Some(buffer),
            iovecs,
            iov_count,
            total,
            fd,
            _marker: PhantomData,
        }
    }
}

impl<C: WriteBufferChain<N>, const N: usize, S> Future for WritevFutureCore<'_, C, N, S> {
    type Output = (io::Result<usize>, C);

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = unsafe { self.get_unchecked_mut() };

        if let Some(result) = take_completed_result(cx, &mut this.state_ptr) {
            let buffer = unsafe { opt_take(&mut this.buffer) };
            if result < 0 {
                return Poll::Ready((Err(io::Error::from_raw_os_error(-result)), buffer));
            }
            return Poll::Ready((Ok(result as usize), buffer));
        }

        if this.total == 0 {
            let buffer = unsafe { opt_take(&mut this.buffer) };
            return Poll::Ready((Ok(0), buffer));
        }

        if this.state_ptr.is_null() {
            let pctx = unsafe { poll_ctx_from_waker(cx) };
            let state_ptr = unsafe { (*pctx.reactor()).alloc_op() };
            if state_ptr.is_null() {
                let buffer = unsafe { opt_take(&mut this.buffer) };
                return Poll::Ready((Err(io::Error::from(io::ErrorKind::WouldBlock)), buffer));
            }
            this.state_ptr = state_ptr;

            unsafe { (*state_ptr).register_waiter(pctx.owner_task()) };

            let sqe = build_write_vectored_entry(
                this.fd,
                &this.iovecs,
                0,
                this.iov_count,
                state_ptr as u64,
            );

            unsafe {
                if let Err(e) = submit_tracked_sqe(&pctx, sqe) {
                    (*pctx.reactor()).free_op(state_ptr);
                    this.state_ptr = std::ptr::null_mut();
                    let buffer = opt_take(&mut this.buffer);
                    return Poll::Ready((Err(e), buffer));
                }
            }
        }

        Poll::Pending
    }
}

impl<C: WriteBufferChain<N>, const N: usize, S> Drop for WritevFutureCore<'_, C, N, S> {
    fn drop(&mut self) {
        unsafe { drop_op_ptr_unchecked(&mut self.state_ptr) };
    }
}

/// Gather-write from a FlowIO frozen vectored buffer chain (rental pattern).
#[doc(hidden)]
pub struct WritevFuture<'a, const N: usize, S> {
    inner: WritevFutureCore<'a, IoBuffVec<N>, N, S>,
}

impl<'a, const N: usize, S> WritevFuture<'a, N, S> {
    pub(crate) fn new(fd: RawFd, buffer: IoBuffVec<N>) -> Self {
        Self {
            inner: WritevFutureCore::new(fd, buffer),
        }
    }
}

impl<const N: usize, S> Future for WritevFuture<'_, N, S> {
    type Output = (io::Result<usize>, IoBuffVec<N>);

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = unsafe { self.get_unchecked_mut() };
        unsafe { Pin::new_unchecked(&mut this.inner) }.poll(cx)
    }
}

/// Gather-write from a generic read-only vectored buffer chain.
#[doc(hidden)]
pub struct WritevReadOnlyFuture<'a, B: IoBuffReadOnly, const N: usize, S> {
    inner: WritevFutureCore<'a, IoBuffReadOnlyVec<B, N>, N, S>,
}

impl<'a, B: IoBuffReadOnly, const N: usize, S> WritevReadOnlyFuture<'a, B, N, S> {
    pub(crate) fn new(fd: RawFd, buffer: IoBuffReadOnlyVec<B, N>) -> Self {
        Self {
            inner: WritevFutureCore::new(fd, buffer),
        }
    }
}

impl<B: IoBuffReadOnly, const N: usize, S> Future for WritevReadOnlyFuture<'_, B, N, S> {
    type Output = (io::Result<usize>, IoBuffReadOnlyVec<B, N>);

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = unsafe { self.get_unchecked_mut() };
        unsafe { Pin::new_unchecked(&mut this.inner) }.poll(cx)
    }
}

// ---------------------------------------------------------------------------
// WritevAllFuture
// ---------------------------------------------------------------------------

/// Shared gather-write-all future core for owned read-only vectored chains.
struct WritevAllFutureCore<'a, C: WriteBufferChain<N>, const N: usize, S> {
    /// Completion state reused across sequential retry submissions.
    state_ptr: *mut CompletionState,
    /// Caller-owned read-only segment chain returned when the operation finishes.
    buffer: Option<C>,
    /// Future-owned `iovec` scratch advanced in place after partial writes.
    iovecs: [MaybeUninit<libc::iovec>; N],
    /// Number of valid entries currently present in `iovecs`.
    iov_count: usize,
    /// Stream descriptor written by this future.
    fd: RawFd,
    /// Total bytes that must be written before completion.
    total: usize,
    /// Bytes already confirmed written by completed submissions.
    written: usize,
    /// Index of the first still-active `iovec` entry after partial progress.
    skip: usize,
    /// Borrows the parent stream for the future lifetime.
    _marker: PhantomData<&'a mut S>,
}

impl<'a, C: WriteBufferChain<N>, const N: usize, S> WritevAllFutureCore<'a, C, N, S> {
    fn new(fd: RawFd, buffer: C) -> Self {
        let mut iovecs = uninit_iovecs();
        let (iov_count, total) = buffer.fill_write_iovecs_and_len(&mut iovecs);
        debug_assert!(
            total == 0 || iov_count > 0,
            "non-empty write-all chain produced no iovecs"
        );
        Self {
            state_ptr: std::ptr::null_mut(),
            buffer: Some(buffer),
            iovecs,
            iov_count,
            fd,
            total,
            written: 0,
            skip: 0,
            _marker: PhantomData,
        }
    }
}

impl<C: WriteBufferChain<N>, const N: usize, S> Future for WritevAllFutureCore<'_, C, N, S> {
    type Output = (io::Result<usize>, C);

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = unsafe { self.get_unchecked_mut() };

        if !this.state_ptr.is_null() {
            let state = unsafe { &*this.state_ptr };
            if !state.is_completed() {
                return Poll::Pending;
            }
        }

        if this.state_ptr.is_null() && this.total == 0 {
            let buffer = unsafe { opt_take(&mut this.buffer) };
            return Poll::Ready((Ok(0), buffer));
        }

        let pctx = unsafe { poll_ctx_from_waker(cx) };

        if !this.state_ptr.is_null() {
            let result = unsafe { (*this.state_ptr).result };

            if result < 0 {
                unsafe { free_retry_state(&pctx, &mut this.state_ptr) };
                let buffer = unsafe { opt_take(&mut this.buffer) };
                return Poll::Ready((Err(io::Error::from_raw_os_error(-result)), buffer));
            }

            let n = result as usize;
            if n == 0 {
                unsafe { free_retry_state(&pctx, &mut this.state_ptr) };
                let buffer = unsafe { opt_take(&mut this.buffer) };
                return Poll::Ready((Err(io::Error::from(io::ErrorKind::WriteZero)), buffer));
            }

            this.written += n;
            if this.written >= this.total {
                unsafe { free_retry_state(&pctx, &mut this.state_ptr) };
                let buffer = unsafe { opt_take(&mut this.buffer) };
                return Poll::Ready((Ok(this.written), buffer));
            }

            unsafe {
                advance_iovecs_in_place(
                    iovec_slice_mut(&mut this.iovecs, this.iov_count),
                    &mut this.skip,
                    n,
                );
            }
        }
        if let Err(err) = unsafe { prepare_retry_state(&pctx, &mut this.state_ptr) } {
            let buffer = unsafe { opt_take(&mut this.buffer) };
            return Poll::Ready((Err(err), buffer));
        }

        let remaining_iovs = this.iov_count - this.skip;
        let sqe = build_write_vectored_entry(
            this.fd,
            &this.iovecs,
            this.skip,
            remaining_iovs,
            this.state_ptr as u64,
        );

        unsafe {
            if let Err(e) = submit_tracked_sqe(&pctx, sqe) {
                (*pctx.reactor()).free_op(this.state_ptr);
                this.state_ptr = std::ptr::null_mut();
                let buffer = opt_take(&mut this.buffer);
                return Poll::Ready((Err(e), buffer));
            }
        }

        Poll::Pending
    }
}

impl<C: WriteBufferChain<N>, const N: usize, S> Drop for WritevAllFutureCore<'_, C, N, S> {
    fn drop(&mut self) {
        unsafe { drop_op_ptr_unchecked(&mut self.state_ptr) };
    }
}

/// Gather-write the entire FlowIO frozen vectored chain, handling partial writes.
#[doc(hidden)]
pub struct WritevAllFuture<'a, const N: usize, S> {
    inner: WritevAllFutureCore<'a, IoBuffVec<N>, N, S>,
}

impl<'a, const N: usize, S> WritevAllFuture<'a, N, S> {
    pub(crate) fn new(fd: RawFd, buffer: IoBuffVec<N>) -> Self {
        Self {
            inner: WritevAllFutureCore::new(fd, buffer),
        }
    }
}

impl<const N: usize, S> Future for WritevAllFuture<'_, N, S> {
    type Output = (io::Result<usize>, IoBuffVec<N>);

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = unsafe { self.get_unchecked_mut() };
        unsafe { Pin::new_unchecked(&mut this.inner) }.poll(cx)
    }
}

/// Gather-write an entire generic read-only vectored chain.
#[doc(hidden)]
pub struct WritevAllReadOnlyFuture<'a, B: IoBuffReadOnly, const N: usize, S> {
    inner: WritevAllFutureCore<'a, IoBuffReadOnlyVec<B, N>, N, S>,
}

impl<'a, B: IoBuffReadOnly, const N: usize, S> WritevAllReadOnlyFuture<'a, B, N, S> {
    pub(crate) fn new(fd: RawFd, buffer: IoBuffReadOnlyVec<B, N>) -> Self {
        Self {
            inner: WritevAllFutureCore::new(fd, buffer),
        }
    }
}

impl<B: IoBuffReadOnly, const N: usize, S> Future for WritevAllReadOnlyFuture<'_, B, N, S> {
    type Output = (io::Result<usize>, IoBuffReadOnlyVec<B, N>);

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = unsafe { self.get_unchecked_mut() };
        unsafe { Pin::new_unchecked(&mut this.inner) }.poll(cx)
    }
}

// ---------------------------------------------------------------------------
// ReadvExactFuture
// ---------------------------------------------------------------------------

/// Scatter-read exactly `target` bytes into a vectored buffer chain,
/// re-submitting on partial reads with future-owned `iovec` scratch.
/// Returns `UnexpectedEof` if the peer closes before the target is reached.
#[doc(hidden)]
pub struct ReadvExactFuture<'a, const N: usize, S> {
    /// Completion state reused across sequential retry submissions.
    state_ptr: *mut CompletionState,
    /// Caller-owned mutable segment chain returned when the operation finishes.
    buffer: Option<IoBuffVecMut<N>>,
    /// Future-owned `iovec` scratch advanced in place after partial reads.
    iovecs: [MaybeUninit<libc::iovec>; N],
    /// Number of valid entries currently present in `iovecs`.
    iov_count: usize,
    /// Stream descriptor read from by this future.
    fd: RawFd,
    /// Exact byte count required before the future can succeed.
    target: usize,
    /// Bytes already read into the destination chain.
    filled: usize,
    /// Index of the first still-active `iovec` entry after partial progress.
    skip: usize,
    /// Deferred validation error returned before any SQE submission.
    input_error: Option<io::Error>,
    /// Borrows the parent stream for the future lifetime.
    _marker: PhantomData<&'a mut S>,
}

impl<'a, const N: usize, S> ReadvExactFuture<'a, N, S> {
    pub(crate) fn new(fd: RawFd, buffer: IoBuffVecMut<N>, target: usize) -> Self {
        let mut buffer = buffer;
        let mut iovecs = uninit_iovecs();
        let (iov_count, writable) = buffer.fill_read_iovecs_and_writable_len(&mut iovecs);
        let mut input_error = None;
        let target = match super::checked_read_len("readv_exact", target, writable) {
            Ok(target) => target as usize,
            Err(err) => {
                input_error = Some(err);
                0
            }
        };

        Self {
            state_ptr: std::ptr::null_mut(),
            buffer: Some(buffer),
            iovecs,
            iov_count,
            fd,
            target,
            filled: 0,
            skip: 0,
            input_error,
            _marker: PhantomData,
        }
    }
}

impl<const N: usize, S> Future for ReadvExactFuture<'_, N, S> {
    type Output = (io::Result<usize>, IoBuffVecMut<N>);

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = unsafe { self.get_unchecked_mut() };

        if this.state_ptr.is_null()
            && let Some(err) = this.input_error.take()
        {
            let buffer = unsafe { opt_take(&mut this.buffer) };
            return Poll::Ready((Err(err), buffer));
        }

        if !this.state_ptr.is_null() {
            let state = unsafe { &*this.state_ptr };
            if !state.is_completed() {
                return Poll::Pending;
            }
        }

        if this.state_ptr.is_null() && this.target == 0 {
            let mut buffer = unsafe { opt_take(&mut this.buffer) };
            unsafe { buffer.distribute_written(0) };
            return Poll::Ready((Ok(0), buffer));
        }

        let pctx = unsafe { poll_ctx_from_waker(cx) };

        if !this.state_ptr.is_null() {
            let result = unsafe { (*this.state_ptr).result };

            if result < 0 {
                unsafe { free_retry_state(&pctx, &mut this.state_ptr) };
                let mut buffer = unsafe { opt_take(&mut this.buffer) };
                unsafe { buffer.distribute_written(this.filled) };
                return Poll::Ready((Err(io::Error::from_raw_os_error(-result)), buffer));
            }

            let n = result as usize;
            if n == 0 {
                unsafe { free_retry_state(&pctx, &mut this.state_ptr) };
                let mut buffer = unsafe { opt_take(&mut this.buffer) };
                unsafe { buffer.distribute_written(this.filled) };
                return Poll::Ready((Err(io::Error::from(io::ErrorKind::UnexpectedEof)), buffer));
            }

            this.filled += n;
            if this.filled >= this.target {
                unsafe { free_retry_state(&pctx, &mut this.state_ptr) };
                let mut buffer = unsafe { opt_take(&mut this.buffer) };
                unsafe { buffer.distribute_written(this.target) };
                return Poll::Ready((Ok(this.target), buffer));
            }

            unsafe {
                advance_iovecs_in_place(
                    iovec_slice_mut(&mut this.iovecs, this.iov_count),
                    &mut this.skip,
                    n,
                );
            }
        }
        if let Err(err) = unsafe { prepare_retry_state(&pctx, &mut this.state_ptr) } {
            let mut buffer = unsafe { opt_take(&mut this.buffer) };
            unsafe { buffer.distribute_written(this.filled) };
            return Poll::Ready((Err(err), buffer));
        }

        let remaining_iovs = this.iov_count - this.skip;
        let sqe = build_read_vectored_entry(
            this.fd,
            &this.iovecs,
            this.skip,
            remaining_iovs,
            this.state_ptr as u64,
        );

        unsafe {
            if let Err(e) = submit_tracked_sqe(&pctx, sqe) {
                (*pctx.reactor()).free_op(this.state_ptr);
                this.state_ptr = std::ptr::null_mut();
                let mut buffer = opt_take(&mut this.buffer);
                buffer.distribute_written(this.filled);
                return Poll::Ready((Err(e), buffer));
            }
        }

        Poll::Pending
    }
}

impl<const N: usize, S> Drop for ReadvExactFuture<'_, N, S> {
    fn drop(&mut self) {
        unsafe { drop_op_ptr_unchecked(&mut self.state_ptr) };
    }
}
