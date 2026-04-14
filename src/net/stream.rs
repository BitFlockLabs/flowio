//! Shared io_uring futures for byte-stream transports (TCP, Unix).
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

use crate::runtime::buffer::iobuffvec::{IoBuffVec, IoBuffVecMut};
use crate::runtime::buffer::{IoBuffReadOnly, IoBuffReadWrite};
use crate::runtime::executor::{drop_op_ptr_unchecked, poll_ctx_from_waker, submit_tracked_sqe};
use crate::runtime::op::CompletionState;
use io_uring::{opcode, types};
use std::future::Future;
use std::io;
use std::marker::PhantomData;
use std::mem::MaybeUninit;
use std::os::fd::RawFd;
use std::pin::Pin;
use std::slice;
use std::task::{Context, Poll};

#[inline(always)]
fn take_completed_result(cx: &mut Context<'_>, state_ptr: &mut *mut CompletionState)
-> Option<i32>
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
    Some(result)
}

use super::{opt_mut, opt_ref, opt_take};

#[inline(always)]
fn uninit_iovecs<const N: usize>() -> [MaybeUninit<libc::iovec>; N]
{
    unsafe { MaybeUninit::uninit().assume_init() }
}

#[inline(always)]
unsafe fn iovec_slice_mut<const N: usize>(
    iovecs: &mut [MaybeUninit<libc::iovec>; N],
    len: usize,
) -> &mut [libc::iovec]
{
    unsafe { slice::from_raw_parts_mut(iovecs.as_mut_ptr() as *mut libc::iovec, len) }
}

#[inline(always)]
unsafe fn iovec_ptr<const N: usize>(
    iovecs: &[MaybeUninit<libc::iovec>; N],
    skip: usize,
) -> *const libc::iovec
{
    unsafe { iovecs.as_ptr().add(skip) as *const libc::iovec }
}

#[inline(always)]
unsafe fn iovec_ref<const N: usize>(
    iovecs: &[MaybeUninit<libc::iovec>; N],
    index: usize,
) -> &libc::iovec
{
    unsafe { &*(iovecs.as_ptr().add(index) as *const libc::iovec) }
}

/// Advance past `bytes` consumed/filled bytes in an iovec array by mutating
/// the scratch entries in place. `skip` is updated to the first remaining
/// non-empty entry.
#[inline]
fn advance_iovecs_in_place(iovecs: &mut [libc::iovec], skip: &mut usize, bytes: usize)
{
    let mut remaining = bytes;
    while remaining > 0 && *skip < iovecs.len()
    {
        let iov = &mut iovecs[*skip];
        if remaining >= iov.iov_len
        {
            remaining -= iov.iov_len;
            *skip += 1;
        }
        else
        {
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
pub struct ReadFuture<'a, B: IoBuffReadWrite, S>
{
    state_ptr: *mut CompletionState,
    buffer: Option<B>,
    fd: RawFd,
    len: u32,
    input_error: Option<io::Error>,
    _marker: PhantomData<&'a mut S>,
}

impl<'a, B: IoBuffReadWrite, S> ReadFuture<'a, B, S>
{
    pub(crate) fn new(fd: RawFd, buffer: B, len: usize) -> Self
    {
        let mut input_error = None;
        let len = match super::checked_read_len("read", len, buffer.writable_len())
        {
            Ok(len) => len,
            Err(err) =>
            {
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

impl<B: IoBuffReadWrite, S> Future for ReadFuture<'_, B, S>
{
    type Output = (io::Result<usize>, B);

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output>
    {
        let this = unsafe { self.get_unchecked_mut() };

        if this.state_ptr.is_null()
            && let Some(err) = this.input_error.take()
        {
            let buffer = unsafe { opt_take(&mut this.buffer) };
            return Poll::Ready((Err(err), buffer));
        }

        if let Some(result) = take_completed_result(cx, &mut this.state_ptr)
        {
            let mut buffer = unsafe { opt_take(&mut this.buffer) };
            if result < 0
            {
                return Poll::Ready((Err(io::Error::from_raw_os_error(-result)), buffer));
            }
            let actual = result as usize;
            unsafe { buffer.set_written_len(actual) };
            return Poll::Ready((Ok(actual), buffer));
        }

        if this.state_ptr.is_null()
        {
            let pctx = unsafe { poll_ctx_from_waker(cx) };
            let state_ptr = unsafe { (*pctx.reactor()).alloc_op() };
            if state_ptr.is_null()
            {
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
                if let Err(e) = submit_tracked_sqe(&pctx, sqe)
                {
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

impl<B: IoBuffReadWrite, S> Drop for ReadFuture<'_, B, S>
{
    fn drop(&mut self)
    {
        unsafe { drop_op_ptr_unchecked(&mut self.state_ptr) };
    }
}

// ---------------------------------------------------------------------------
// WriteFuture
// ---------------------------------------------------------------------------

/// Single write from a caller-provided buffer (rental pattern).
#[doc(hidden)]
pub struct WriteFuture<'a, B: IoBuffReadOnly, S>
{
    state_ptr: *mut CompletionState,
    buffer: Option<B>,
    fd: RawFd,
    _marker: PhantomData<&'a mut S>,
}

impl<'a, B: IoBuffReadOnly, S> WriteFuture<'a, B, S>
{
    pub(crate) fn new(fd: RawFd, buffer: B) -> Self
    {
        Self {
            state_ptr: std::ptr::null_mut(),
            buffer: Some(buffer),
            fd,
            _marker: PhantomData,
        }
    }
}

impl<B: IoBuffReadOnly, S> Future for WriteFuture<'_, B, S>
{
    type Output = (io::Result<usize>, B);

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output>
    {
        let this = unsafe { self.get_unchecked_mut() };

        if let Some(result) = take_completed_result(cx, &mut this.state_ptr)
        {
            let buffer = unsafe { opt_take(&mut this.buffer) };
            if result < 0
            {
                return Poll::Ready((Err(io::Error::from_raw_os_error(-result)), buffer));
            }
            return Poll::Ready((Ok(result as usize), buffer));
        }

        if this.state_ptr.is_null()
        {
            let pctx = unsafe { poll_ctx_from_waker(cx) };
            let state_ptr = unsafe { (*pctx.reactor()).alloc_op() };
            if state_ptr.is_null()
            {
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
                if let Err(e) = submit_tracked_sqe(&pctx, sqe)
                {
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

impl<B: IoBuffReadOnly, S> Drop for WriteFuture<'_, B, S>
{
    fn drop(&mut self)
    {
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
pub struct WriteAllFuture<'a, B: IoBuffReadOnly, S>
{
    state_ptr: *mut CompletionState,
    buffer: Option<B>,
    base_ptr: *const u8,
    fd: RawFd,
    offset: u32,
    total: u32,
    _marker: PhantomData<&'a mut S>,
}

impl<'a, B: IoBuffReadOnly, S> WriteAllFuture<'a, B, S>
{
    pub(crate) fn new(fd: RawFd, buffer: B) -> Self
    {
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

impl<B: IoBuffReadOnly, S> Future for WriteAllFuture<'_, B, S>
{
    type Output = (io::Result<usize>, B);

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output>
    {
        let this = unsafe { self.get_unchecked_mut() };

        // Fast path: still in flight — return without any context extraction.
        if !this.state_ptr.is_null()
        {
            let state = unsafe { &*this.state_ptr };
            if !state.is_completed()
            {
                return Poll::Pending;
            }
        }

        // Zero-length write completes immediately.
        if this.state_ptr.is_null() && this.total == 0
        {
            let buffer = unsafe { opt_take(&mut this.buffer) };
            return Poll::Ready((Ok(0), buffer));
        }

        // One context extraction covers free + alloc + submit.
        let pctx = unsafe { poll_ctx_from_waker(cx) };

        // Process completed state if any. Sequential retries reuse the same
        // completion slot once the previous CQE has been fully consumed.
        if !this.state_ptr.is_null()
        {
            let result = unsafe { (*this.state_ptr).result };

            if result < 0
            {
                unsafe { (*pctx.reactor()).free_op(this.state_ptr) };
                this.state_ptr = std::ptr::null_mut();
                let buffer = unsafe { opt_take(&mut this.buffer) };
                return Poll::Ready((Err(io::Error::from_raw_os_error(-result)), buffer));
            }

            let n = result as u32;
            if n == 0
            {
                unsafe { (*pctx.reactor()).free_op(this.state_ptr) };
                this.state_ptr = std::ptr::null_mut();
                let buffer = unsafe { opt_take(&mut this.buffer) };
                return Poll::Ready((Err(io::Error::from(io::ErrorKind::WriteZero)), buffer));
            }

            this.offset += n;
            if this.offset >= this.total
            {
                unsafe { (*pctx.reactor()).free_op(this.state_ptr) };
                this.state_ptr = std::ptr::null_mut();
                let buffer = unsafe { opt_take(&mut this.buffer) };
                return Poll::Ready((Ok(this.offset as usize), buffer));
            }

            unsafe {
                let state = &mut *this.state_ptr;
                state.reset_for_resubmit();
                state.register_waiter(pctx.owner_task());
            }
        }
        else
        {
            let state_ptr = unsafe { (*pctx.reactor()).alloc_op() };
            if state_ptr.is_null()
            {
                let buffer = unsafe { opt_take(&mut this.buffer) };
                return Poll::Ready((Err(io::Error::from(io::ErrorKind::WouldBlock)), buffer));
            }
            this.state_ptr = state_ptr;

            unsafe { (*state_ptr).register_waiter(pctx.owner_task()) };
        }

        let ptr = unsafe { this.base_ptr.add(this.offset as usize) };
        let remaining = this.total - this.offset;

        let sqe = opcode::Write::new(types::Fd(this.fd), ptr, remaining)
            .build()
            .user_data(this.state_ptr as u64);

        unsafe {
            if let Err(e) = submit_tracked_sqe(&pctx, sqe)
            {
                (*pctx.reactor()).free_op(this.state_ptr);
                this.state_ptr = std::ptr::null_mut();
                let buffer = opt_take(&mut this.buffer);
                return Poll::Ready((Err(e), buffer));
            }
        }

        Poll::Pending
    }
}

impl<B: IoBuffReadOnly, S> Drop for WriteAllFuture<'_, B, S>
{
    fn drop(&mut self)
    {
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
pub struct ReadExactFuture<'a, B: IoBuffReadWrite, S>
{
    state_ptr: *mut CompletionState,
    buffer: Option<B>,
    base_ptr: *mut u8,
    fd: RawFd,
    target: u32,
    filled: u32,
    input_error: Option<io::Error>,
    _marker: PhantomData<&'a mut S>,
}

impl<'a, B: IoBuffReadWrite, S> ReadExactFuture<'a, B, S>
{
    pub(crate) fn new(fd: RawFd, mut buffer: B, len: usize) -> Self
    {
        let mut input_error = None;
        let target = match super::checked_read_len("read_exact", len, buffer.writable_len())
        {
            Ok(target) => target,
            Err(err) =>
            {
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

impl<B: IoBuffReadWrite, S> Future for ReadExactFuture<'_, B, S>
{
    type Output = (io::Result<usize>, B);

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output>
    {
        let this = unsafe { self.get_unchecked_mut() };

        if this.state_ptr.is_null()
            && let Some(err) = this.input_error.take()
        {
            let buffer = unsafe { opt_take(&mut this.buffer) };
            return Poll::Ready((Err(err), buffer));
        }

        // Fast path: still in flight — return without any context extraction.
        if !this.state_ptr.is_null()
        {
            let state = unsafe { &*this.state_ptr };
            if !state.is_completed()
            {
                return Poll::Pending;
            }
        }

        // Zero-length read completes immediately.
        if this.state_ptr.is_null() && this.target == 0
        {
            let mut buffer = unsafe { opt_take(&mut this.buffer) };
            unsafe { buffer.set_written_len(0) };
            return Poll::Ready((Ok(0), buffer));
        }

        // One context extraction covers free + alloc + submit.
        let pctx = unsafe { poll_ctx_from_waker(cx) };

        // Process completed state if any. Sequential retries reuse the same
        // completion slot once the previous CQE has been fully consumed.
        if !this.state_ptr.is_null()
        {
            let result = unsafe { (*this.state_ptr).result };

            if result < 0
            {
                unsafe { (*pctx.reactor()).free_op(this.state_ptr) };
                this.state_ptr = std::ptr::null_mut();
                let mut buffer = unsafe { opt_take(&mut this.buffer) };
                unsafe { buffer.set_written_len(this.filled as usize) };
                return Poll::Ready((Err(io::Error::from_raw_os_error(-result)), buffer));
            }

            let n = result as u32;
            if n == 0
            {
                unsafe { (*pctx.reactor()).free_op(this.state_ptr) };
                this.state_ptr = std::ptr::null_mut();
                let mut buffer = unsafe { opt_take(&mut this.buffer) };
                unsafe { buffer.set_written_len(this.filled as usize) };
                return Poll::Ready((Err(io::Error::from(io::ErrorKind::UnexpectedEof)), buffer));
            }

            this.filled += n;
            if this.filled >= this.target
            {
                unsafe { (*pctx.reactor()).free_op(this.state_ptr) };
                this.state_ptr = std::ptr::null_mut();
                let mut buffer = unsafe { opt_take(&mut this.buffer) };
                unsafe { buffer.set_written_len(this.target as usize) };
                return Poll::Ready((Ok(this.target as usize), buffer));
            }

            unsafe {
                let state = &mut *this.state_ptr;
                state.reset_for_resubmit();
                state.register_waiter(pctx.owner_task());
            }
        }
        else
        {
            let state_ptr = unsafe { (*pctx.reactor()).alloc_op() };
            if state_ptr.is_null()
            {
                let mut buffer = unsafe { opt_take(&mut this.buffer) };
                unsafe { buffer.set_written_len(this.filled as usize) };
                return Poll::Ready((Err(io::Error::from(io::ErrorKind::WouldBlock)), buffer));
            }
            this.state_ptr = state_ptr;

            unsafe { (*state_ptr).register_waiter(pctx.owner_task()) };
        }

        let ptr = unsafe { this.base_ptr.add(this.filled as usize) };
        let remaining = this.target - this.filled;

        let sqe = opcode::Read::new(types::Fd(this.fd), ptr, remaining)
            .build()
            .user_data(this.state_ptr as u64);

        unsafe {
            if let Err(e) = submit_tracked_sqe(&pctx, sqe)
            {
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

impl<B: IoBuffReadWrite, S> Drop for ReadExactFuture<'_, B, S>
{
    fn drop(&mut self)
    {
        unsafe { drop_op_ptr_unchecked(&mut self.state_ptr) };
    }
}

// ---------------------------------------------------------------------------
// ReadvFuture
// ---------------------------------------------------------------------------

/// Scatter-read into a vectored buffer chain (rental pattern).
#[doc(hidden)]
pub struct ReadvFuture<'a, const N: usize, S>
{
    state_ptr: *mut CompletionState,
    buffer: Option<IoBuffVecMut<N>>,
    iovecs: [MaybeUninit<libc::iovec>; N],
    iov_count: usize,
    writable: usize,
    fd: RawFd,
    _marker: PhantomData<&'a mut S>,
}

impl<'a, const N: usize, S> ReadvFuture<'a, N, S>
{
    pub(crate) fn new(fd: RawFd, buffer: IoBuffVecMut<N>) -> Self
    {
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

impl<const N: usize, S> Future for ReadvFuture<'_, N, S>
{
    type Output = (io::Result<usize>, IoBuffVecMut<N>);

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output>
    {
        let this = unsafe { self.get_unchecked_mut() };

        if let Some(result) = take_completed_result(cx, &mut this.state_ptr)
        {
            let mut buffer = unsafe { opt_take(&mut this.buffer) };
            if result < 0
            {
                return Poll::Ready((Err(io::Error::from_raw_os_error(-result)), buffer));
            }
            let actual = result as usize;
            unsafe { buffer.distribute_written(actual) };
            return Poll::Ready((Ok(actual), buffer));
        }

        if this.writable == 0
        {
            let mut buffer = unsafe { opt_take(&mut this.buffer) };
            unsafe { buffer.distribute_written(0) };
            return Poll::Ready((Ok(0), buffer));
        }

        if this.state_ptr.is_null()
        {
            let pctx = unsafe { poll_ctx_from_waker(cx) };
            let state_ptr = unsafe { (*pctx.reactor()).alloc_op() };
            if state_ptr.is_null()
            {
                let buffer = unsafe { opt_take(&mut this.buffer) };
                return Poll::Ready((Err(io::Error::from(io::ErrorKind::WouldBlock)), buffer));
            }
            this.state_ptr = state_ptr;

            unsafe { (*state_ptr).register_waiter(pctx.owner_task()) };

            let sqe = if this.iov_count == 1
            {
                let iov = unsafe { iovec_ref(&this.iovecs, 0) };
                if let Ok(len) = u32::try_from(iov.iov_len)
                {
                    opcode::Read::new(types::Fd(this.fd), iov.iov_base as *mut u8, len)
                        .build()
                        .user_data(state_ptr as u64)
                }
                else
                {
                    opcode::Readv::new(
                        types::Fd(this.fd),
                        unsafe { iovec_ptr(&this.iovecs, 0) } as *const _,
                        this.iov_count as u32,
                    )
                    .build()
                    .user_data(state_ptr as u64)
                }
            }
            else
            {
                opcode::Readv::new(
                    types::Fd(this.fd),
                    unsafe { iovec_ptr(&this.iovecs, 0) } as *const _,
                    this.iov_count as u32,
                )
                .build()
                .user_data(state_ptr as u64)
            };

            unsafe {
                if let Err(e) = submit_tracked_sqe(&pctx, sqe)
                {
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

impl<const N: usize, S> Drop for ReadvFuture<'_, N, S>
{
    fn drop(&mut self)
    {
        unsafe { drop_op_ptr_unchecked(&mut self.state_ptr) };
    }
}

// ---------------------------------------------------------------------------
// WritevFuture
// ---------------------------------------------------------------------------

/// Gather-write from a vectored buffer chain (rental pattern).
#[doc(hidden)]
pub struct WritevFuture<'a, const N: usize, S>
{
    state_ptr: *mut CompletionState,
    buffer: Option<IoBuffVec<N>>,
    iovecs: [MaybeUninit<libc::iovec>; N],
    iov_count: usize,
    total: usize,
    fd: RawFd,
    _marker: PhantomData<&'a mut S>,
}

impl<'a, const N: usize, S> WritevFuture<'a, N, S>
{
    pub(crate) fn new(fd: RawFd, buffer: IoBuffVec<N>) -> Self
    {
        let mut iovecs = uninit_iovecs();
        let (iov_count, total) = buffer.fill_write_iovecs_and_len(&mut iovecs);
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

impl<const N: usize, S> Future for WritevFuture<'_, N, S>
{
    type Output = (io::Result<usize>, IoBuffVec<N>);

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output>
    {
        let this = unsafe { self.get_unchecked_mut() };

        if let Some(result) = take_completed_result(cx, &mut this.state_ptr)
        {
            let buffer = unsafe { opt_take(&mut this.buffer) };
            if result < 0
            {
                return Poll::Ready((Err(io::Error::from_raw_os_error(-result)), buffer));
            }
            return Poll::Ready((Ok(result as usize), buffer));
        }

        if this.total == 0
        {
            let buffer = unsafe { opt_take(&mut this.buffer) };
            return Poll::Ready((Ok(0), buffer));
        }

        if this.state_ptr.is_null()
        {
            let pctx = unsafe { poll_ctx_from_waker(cx) };
            let state_ptr = unsafe { (*pctx.reactor()).alloc_op() };
            if state_ptr.is_null()
            {
                let buffer = unsafe { opt_take(&mut this.buffer) };
                return Poll::Ready((Err(io::Error::from(io::ErrorKind::WouldBlock)), buffer));
            }
            this.state_ptr = state_ptr;

            unsafe { (*state_ptr).register_waiter(pctx.owner_task()) };

            let sqe = if this.iov_count == 1
            {
                let iov = unsafe { iovec_ref(&this.iovecs, 0) };
                if let Ok(len) = u32::try_from(iov.iov_len)
                {
                    opcode::Write::new(types::Fd(this.fd), iov.iov_base as *const u8, len)
                        .build()
                        .user_data(state_ptr as u64)
                }
                else
                {
                    opcode::Writev::new(
                        types::Fd(this.fd),
                        unsafe { iovec_ptr(&this.iovecs, 0) },
                        this.iov_count as u32,
                    )
                    .build()
                    .user_data(state_ptr as u64)
                }
            }
            else
            {
                opcode::Writev::new(
                    types::Fd(this.fd),
                    unsafe { iovec_ptr(&this.iovecs, 0) },
                    this.iov_count as u32,
                )
                .build()
                .user_data(state_ptr as u64)
            };

            unsafe {
                if let Err(e) = submit_tracked_sqe(&pctx, sqe)
                {
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

impl<const N: usize, S> Drop for WritevFuture<'_, N, S>
{
    fn drop(&mut self)
    {
        unsafe { drop_op_ptr_unchecked(&mut self.state_ptr) };
    }
}

// ---------------------------------------------------------------------------
// WritevAllFuture
// ---------------------------------------------------------------------------

/// Gather-write the entire vectored buffer chain, re-submitting on partial
/// writes and reusing future-owned `iovec` scratch across retries.
#[doc(hidden)]
pub struct WritevAllFuture<'a, const N: usize, S>
{
    state_ptr: *mut CompletionState,
    buffer: Option<IoBuffVec<N>>,
    iovecs: [MaybeUninit<libc::iovec>; N],
    iov_count: usize,
    fd: RawFd,
    total: usize,
    written: usize,
    skip: usize,
    _marker: PhantomData<&'a mut S>,
}

impl<'a, const N: usize, S> WritevAllFuture<'a, N, S>
{
    pub(crate) fn new(fd: RawFd, buffer: IoBuffVec<N>) -> Self
    {
        let mut iovecs = uninit_iovecs();
        let (iov_count, total) = buffer.fill_write_iovecs_and_len(&mut iovecs);
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

impl<const N: usize, S> Future for WritevAllFuture<'_, N, S>
{
    type Output = (io::Result<usize>, IoBuffVec<N>);

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output>
    {
        let this = unsafe { self.get_unchecked_mut() };

        if !this.state_ptr.is_null()
        {
            let state = unsafe { &*this.state_ptr };
            if !state.is_completed()
            {
                return Poll::Pending;
            }
        }

        if this.state_ptr.is_null() && this.total == 0
        {
            let buffer = unsafe { opt_take(&mut this.buffer) };
            return Poll::Ready((Ok(0), buffer));
        }

        let pctx = unsafe { poll_ctx_from_waker(cx) };

        if !this.state_ptr.is_null()
        {
            let result = unsafe { (*this.state_ptr).result };

            if result < 0
            {
                unsafe { (*pctx.reactor()).free_op(this.state_ptr) };
                this.state_ptr = std::ptr::null_mut();
                let buffer = unsafe { opt_take(&mut this.buffer) };
                return Poll::Ready((Err(io::Error::from_raw_os_error(-result)), buffer));
            }

            let n = result as usize;
            if n == 0
            {
                unsafe { (*pctx.reactor()).free_op(this.state_ptr) };
                this.state_ptr = std::ptr::null_mut();
                let buffer = unsafe { opt_take(&mut this.buffer) };
                return Poll::Ready((Err(io::Error::from(io::ErrorKind::WriteZero)), buffer));
            }

            this.written += n;
            if this.written >= this.total
            {
                unsafe { (*pctx.reactor()).free_op(this.state_ptr) };
                this.state_ptr = std::ptr::null_mut();
                let buffer = unsafe { opt_take(&mut this.buffer) };
                return Poll::Ready((Ok(this.written), buffer));
            }

            unsafe {
                advance_iovecs_in_place(
                    iovec_slice_mut(&mut this.iovecs, this.iov_count),
                    &mut this.skip,
                    n,
                );
                let state = &mut *this.state_ptr;
                state.reset_for_resubmit();
                state.register_waiter(pctx.owner_task());
            }
        }
        else
        {
            let state_ptr = unsafe { (*pctx.reactor()).alloc_op() };
            if state_ptr.is_null()
            {
                let buffer = unsafe { opt_take(&mut this.buffer) };
                return Poll::Ready((Err(io::Error::from(io::ErrorKind::WouldBlock)), buffer));
            }
            this.state_ptr = state_ptr;

            unsafe { (*state_ptr).register_waiter(pctx.owner_task()) };
        }

        let remaining_iovs = this.iov_count - this.skip;
        let sqe = if remaining_iovs == 1
        {
            let iov = unsafe { iovec_ref(&this.iovecs, this.skip) };
            if let Ok(len) = u32::try_from(iov.iov_len)
            {
                opcode::Write::new(types::Fd(this.fd), iov.iov_base as *const u8, len)
                    .build()
                    .user_data(this.state_ptr as u64)
            }
            else
            {
                opcode::Writev::new(
                    types::Fd(this.fd),
                    unsafe { iovec_ptr(&this.iovecs, this.skip) },
                    remaining_iovs as u32,
                )
                .build()
                .user_data(this.state_ptr as u64)
            }
        }
        else
        {
            opcode::Writev::new(
                types::Fd(this.fd),
                unsafe { iovec_ptr(&this.iovecs, this.skip) },
                remaining_iovs as u32,
            )
            .build()
            .user_data(this.state_ptr as u64)
        };

        unsafe {
            if let Err(e) = submit_tracked_sqe(&pctx, sqe)
            {
                (*pctx.reactor()).free_op(this.state_ptr);
                this.state_ptr = std::ptr::null_mut();
                let buffer = opt_take(&mut this.buffer);
                return Poll::Ready((Err(e), buffer));
            }
        }

        Poll::Pending
    }
}

impl<const N: usize, S> Drop for WritevAllFuture<'_, N, S>
{
    fn drop(&mut self)
    {
        unsafe { drop_op_ptr_unchecked(&mut self.state_ptr) };
    }
}

// ---------------------------------------------------------------------------
// ReadvExactFuture
// ---------------------------------------------------------------------------

/// Scatter-read exactly `target` bytes into a vectored buffer chain,
/// re-submitting on partial reads with future-owned `iovec` scratch.
/// Returns `UnexpectedEof` if the peer closes before the target is reached.
#[doc(hidden)]
pub struct ReadvExactFuture<'a, const N: usize, S>
{
    state_ptr: *mut CompletionState,
    buffer: Option<IoBuffVecMut<N>>,
    iovecs: [MaybeUninit<libc::iovec>; N],
    iov_count: usize,
    fd: RawFd,
    target: usize,
    filled: usize,
    skip: usize,
    input_error: Option<io::Error>,
    _marker: PhantomData<&'a mut S>,
}

impl<'a, const N: usize, S> ReadvExactFuture<'a, N, S>
{
    pub(crate) fn new(fd: RawFd, buffer: IoBuffVecMut<N>, target: usize) -> Self
    {
        let mut buffer = buffer;
        let mut iovecs = uninit_iovecs();
        let (iov_count, writable) = buffer.fill_read_iovecs_and_writable_len(&mut iovecs);
        let mut input_error = None;
        let target = match super::checked_read_len("readv_exact", target, writable)
        {
            Ok(target) => target as usize,
            Err(err) =>
            {
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

impl<const N: usize, S> Future for ReadvExactFuture<'_, N, S>
{
    type Output = (io::Result<usize>, IoBuffVecMut<N>);

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output>
    {
        let this = unsafe { self.get_unchecked_mut() };

        if this.state_ptr.is_null()
            && let Some(err) = this.input_error.take()
        {
            let buffer = unsafe { opt_take(&mut this.buffer) };
            return Poll::Ready((Err(err), buffer));
        }

        if !this.state_ptr.is_null()
        {
            let state = unsafe { &*this.state_ptr };
            if !state.is_completed()
            {
                return Poll::Pending;
            }
        }

        if this.state_ptr.is_null() && this.target == 0
        {
            let mut buffer = unsafe { opt_take(&mut this.buffer) };
            unsafe { buffer.distribute_written(0) };
            return Poll::Ready((Ok(0), buffer));
        }

        let pctx = unsafe { poll_ctx_from_waker(cx) };

        if !this.state_ptr.is_null()
        {
            let result = unsafe { (*this.state_ptr).result };

            if result < 0
            {
                unsafe { (*pctx.reactor()).free_op(this.state_ptr) };
                this.state_ptr = std::ptr::null_mut();
                let mut buffer = unsafe { opt_take(&mut this.buffer) };
                unsafe { buffer.distribute_written(this.filled) };
                return Poll::Ready((Err(io::Error::from_raw_os_error(-result)), buffer));
            }

            let n = result as usize;
            if n == 0
            {
                unsafe { (*pctx.reactor()).free_op(this.state_ptr) };
                this.state_ptr = std::ptr::null_mut();
                let mut buffer = unsafe { opt_take(&mut this.buffer) };
                unsafe { buffer.distribute_written(this.filled) };
                return Poll::Ready((Err(io::Error::from(io::ErrorKind::UnexpectedEof)), buffer));
            }

            this.filled += n;
            if this.filled >= this.target
            {
                unsafe { (*pctx.reactor()).free_op(this.state_ptr) };
                this.state_ptr = std::ptr::null_mut();
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
                let state = &mut *this.state_ptr;
                state.reset_for_resubmit();
                state.register_waiter(pctx.owner_task());
            }
        }
        else
        {
            let state_ptr = unsafe { (*pctx.reactor()).alloc_op() };
            if state_ptr.is_null()
            {
                let mut buffer = unsafe { opt_take(&mut this.buffer) };
                unsafe { buffer.distribute_written(this.filled) };
                return Poll::Ready((Err(io::Error::from(io::ErrorKind::WouldBlock)), buffer));
            }
            this.state_ptr = state_ptr;

            unsafe { (*state_ptr).register_waiter(pctx.owner_task()) };
        }

        let remaining_iovs = this.iov_count - this.skip;
        let sqe = if remaining_iovs == 1
        {
            let iov = unsafe { iovec_ref(&this.iovecs, this.skip) };
            if let Ok(len) = u32::try_from(iov.iov_len)
            {
                opcode::Read::new(types::Fd(this.fd), iov.iov_base as *mut u8, len)
                    .build()
                    .user_data(this.state_ptr as u64)
            }
            else
            {
                opcode::Readv::new(
                    types::Fd(this.fd),
                    unsafe { iovec_ptr(&this.iovecs, this.skip) } as *const _,
                    remaining_iovs as u32,
                )
                .build()
                .user_data(this.state_ptr as u64)
            }
        }
        else
        {
            opcode::Readv::new(
                types::Fd(this.fd),
                unsafe { iovec_ptr(&this.iovecs, this.skip) } as *const _,
                remaining_iovs as u32,
            )
            .build()
            .user_data(this.state_ptr as u64)
        };

        unsafe {
            if let Err(e) = submit_tracked_sqe(&pctx, sqe)
            {
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

impl<const N: usize, S> Drop for ReadvExactFuture<'_, N, S>
{
    fn drop(&mut self)
    {
        unsafe { drop_op_ptr_unchecked(&mut self.state_ptr) };
    }
}
