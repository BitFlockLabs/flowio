//! Shared io_uring futures for byte-stream transports and FlowIO's TLS wrapper.
//!
//! These futures use `IORING_OP_READ` / `IORING_OP_WRITE` and handle partial
//! completion internally for the `_all` / `_exact` variants.  The stream type
//! parameter `S` is carried only in `PhantomData` to borrow the parent stream
//! for the duration of the operation.
//!
//! Vectored operations materialize `iovec` arrays into scratch storage.
//! Partial progress advances that scratch in place, and the retry path can
//! downgrade to `IORING_OP_READ` / `IORING_OP_WRITE` when only one segment
//! remains.
//!
//! `CompletionState` is allocated from the reactor's pool for each active
//! operation. Simple futures free it when their one submission retires; retry
//! futures reuse the same slot across sequential resubmissions after each CQE
//! has been consumed.
//!
//! If a future is dropped while its SQE is still in flight, the state is
//! marked orphaned and an `ASYNC_CANCEL` SQE is submitted; the CQE path then
//! reclaims the pool slot. Futures attach caller buffers and vectored scratch
//! to the `CompletionState` before submission so kernel-referenced memory stays
//! alive until the original CQE retires. If a read future is dropped before
//! completion, any bytes consumed by a racing completion are discarded with the
//! retained buffer when the original CQE retires.
//! If a future is dropped after completion but before polling its result, the
//! completed state is freed immediately from `Drop`.

use crate::runtime::buffer::iobuffvec::{IoBuffReadOnlyVec, IoBuffVec, IoBuffVecMut};
use crate::runtime::buffer::{IoBuffMut, IoBuffReadOnly, IoBuffReadWrite};
use crate::runtime::executor::{
    PollCtx, drop_op_ptr_unchecked, poll_ctx_from_waker, submit_retained_sqe, submit_tracked_sqe,
};
use crate::runtime::op::CompletionState;
use crate::runtime::reactor::Reactor;
use crate::runtime::retained::RetainedIovecScratch;
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
/// Returns a completed result plus the retained payload, then retires the
/// completion-state slot.
unsafe fn take_completed_result_and_payload<T: 'static>(
    cx: &mut Context<'_>,
    state_ptr: &mut *mut CompletionState,
) -> Option<(i32, T)> {
    if state_ptr.is_null() {
        return None;
    }

    let state = unsafe { &mut **state_ptr };
    if !state.is_completed() {
        return None;
    }

    let result = state.result;
    let pctx = unsafe { poll_ctx_from_waker(cx) };
    let payload = unsafe { (*pctx.reactor()).take_retained_payload::<T>(*state_ptr) };
    unsafe { (*pctx.reactor()).free_op(*state_ptr) };
    *state_ptr = std::ptr::null_mut();
    Some((result, payload))
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

use super::{WritevPieces, WritevProjection, opt_ref, opt_take};

#[inline(always)]
fn uninit_iovecs<const N: usize>() -> [MaybeUninit<libc::iovec>; N] {
    unsafe { MaybeUninit::uninit().assume_init() }
}

#[inline(always)]
unsafe fn iovec_slice_mut_from_uninit(
    iovecs: &mut [MaybeUninit<libc::iovec>],
) -> &mut [libc::iovec] {
    unsafe { slice::from_raw_parts_mut(iovecs.as_mut_ptr() as *mut libc::iovec, iovecs.len()) }
}

#[inline(always)]
unsafe fn iovec_slice_ptr(iovecs: &[MaybeUninit<libc::iovec>], skip: usize) -> *const libc::iovec {
    unsafe { iovecs.as_ptr().add(skip) as *const libc::iovec }
}

#[inline(always)]
unsafe fn iovec_slice_ref(iovecs: &[MaybeUninit<libc::iovec>], index: usize) -> &libc::iovec {
    unsafe { &*(iovecs.as_ptr().add(index) as *const libc::iovec) }
}

trait WriteBufferChain<const N: usize>: Sized {
    fn write_iovec_count_and_len(&self) -> (usize, usize);
    fn fill_write_iovecs(&self, dst: &mut [MaybeUninit<libc::iovec>]);
}

impl<const N: usize> WriteBufferChain<N> for IoBuffVec<N> {
    #[inline(always)]
    fn write_iovec_count_and_len(&self) -> (usize, usize) {
        let mut iov_count = 0;
        let mut total = 0;
        for buf in self.iter() {
            let len = buf.len();
            total += len;
            if len != 0 {
                iov_count += 1;
            }
        }
        (iov_count, total)
    }

    #[inline(always)]
    fn fill_write_iovecs(&self, dst: &mut [MaybeUninit<libc::iovec>]) {
        let mut iov_count = 0;
        for buf in self.iter() {
            let len = buf.len();
            if len == 0 {
                continue;
            }
            debug_assert!(iov_count < dst.len(), "writev scratch too small");
            dst[iov_count].write(libc::iovec {
                iov_base: buf.as_ptr() as *mut libc::c_void,
                iov_len: len,
            });
            iov_count += 1;
        }
    }
}

impl<B: IoBuffReadOnly, const N: usize> WriteBufferChain<N> for IoBuffReadOnlyVec<B, N> {
    #[inline(always)]
    fn write_iovec_count_and_len(&self) -> (usize, usize) {
        let mut iov_count = 0;
        let mut total = 0;
        for buf in self.iter() {
            let len = buf.len();
            total += len;
            if len != 0 {
                iov_count += 1;
            }
        }
        (iov_count, total)
    }

    #[inline(always)]
    fn fill_write_iovecs(&self, dst: &mut [MaybeUninit<libc::iovec>]) {
        let mut iov_count = 0;
        for buf in self.iter() {
            let len = buf.len();
            if len == 0 {
                continue;
            }
            debug_assert!(iov_count < dst.len(), "writev scratch too small");
            dst[iov_count].write(libc::iovec {
                iov_base: buf.as_ptr() as *mut libc::c_void,
                iov_len: len,
            });
            iov_count += 1;
        }
    }
}

struct RetainedWritePayload<B: IoBuffReadOnly> {
    /// Caller-owned source buffer retained while the write SQE is in flight.
    buffer: B,
}

struct RetainedReadPayload<B: IoBuffReadWrite> {
    /// Caller-owned destination buffer retained while the read SQE is in flight.
    buffer: B,
}

struct RetainedReadvPayload<const N: usize> {
    /// Caller-owned destination chain retained until the original CQE retires.
    buffer: IoBuffVecMut<N>,
    /// Kernel-facing `iovec` array pointing into `buffer` segments.
    scratch: RetainedIovecScratch,
}

struct RetainedWritevPayload<C> {
    /// Caller-owned source chain retained until the original CQE retires.
    buffer: C,
    /// Kernel-facing `iovec` array pointing into `buffer` segments.
    scratch: RetainedIovecScratch,
    /// Bytes confirmed by completed SQEs for `_all` retry futures.
    written: usize,
    /// First active `iovec` entry after partial write progress.
    skip: usize,
}

struct RetainedProjectedWritevPayload<T> {
    /// Compact caller-owned source retained until the original CQE retires.
    source: T,
    /// Kernel-facing `iovec` array pointing into `source` projections.
    scratch: RetainedIovecScratch,
    /// Bytes confirmed by completed SQEs for projected `_all` retries.
    written: usize,
    /// First active projected `iovec` entry after partial write progress.
    skip: usize,
}

struct UnsubmittedOpGuard {
    /// Reactor that owns the allocated completion-state slot.
    reactor: *mut Reactor,
    /// Allocated slot that must be returned unless submission succeeds.
    state_ptr: *mut CompletionState,
}

impl UnsubmittedOpGuard {
    #[inline(always)]
    fn new(reactor: *mut Reactor, state_ptr: *mut CompletionState) -> Self {
        Self { reactor, state_ptr }
    }

    #[inline(always)]
    fn free(mut self) {
        if !self.state_ptr.is_null() {
            unsafe { (*self.reactor).free_op(self.state_ptr) };
            self.state_ptr = std::ptr::null_mut();
        }
    }

    #[inline(always)]
    fn disarm(mut self) -> *mut CompletionState {
        let state_ptr = self.state_ptr;
        self.state_ptr = std::ptr::null_mut();
        state_ptr
    }
}

impl Drop for UnsubmittedOpGuard {
    fn drop(&mut self) {
        if !self.state_ptr.is_null() {
            unsafe { (*self.reactor).free_op(self.state_ptr) };
            self.state_ptr = std::ptr::null_mut();
        }
    }
}

#[inline(always)]
/// Builds the next read-style SQE from the current vectored scratch window.
///
/// When only one segment remains and its length fits in `u32`, this downgrades
/// to `IORING_OP_READ` to avoid an unnecessary `readv`.
fn build_read_vectored_entry(
    fd: RawFd,
    iovecs: &[MaybeUninit<libc::iovec>],
    skip: usize,
    count: usize,
    user_data: u64,
) -> squeue::Entry {
    debug_assert!(count > 0, "readv submission requires at least one iovec");

    if count == 1 {
        let iov = unsafe { iovec_slice_ref(iovecs, skip) };
        if let Ok(len) = u32::try_from(iov.iov_len) {
            return opcode::Read::new(types::Fd(fd), iov.iov_base as *mut u8, len)
                .build()
                .user_data(user_data);
        }
    }

    opcode::Readv::new(
        types::Fd(fd),
        unsafe { iovec_slice_ptr(iovecs, skip) },
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
fn build_write_vectored_entry(
    fd: RawFd,
    iovecs: &[MaybeUninit<libc::iovec>],
    skip: usize,
    count: usize,
    user_data: u64,
) -> squeue::Entry {
    debug_assert!(count > 0, "writev submission requires at least one iovec");

    if count == 1 {
        let iov = unsafe { iovec_slice_ref(iovecs, skip) };
        if let Ok(len) = u32::try_from(iov.iov_len) {
            return opcode::Write::new(types::Fd(fd), iov.iov_base as *const u8, len)
                .build()
                .user_data(user_data);
        }
    }

    opcode::Writev::new(
        types::Fd(fd),
        unsafe { iovec_slice_ptr(iovecs, skip) },
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

#[inline(always)]
fn retained_iovecs(scratch: &RetainedIovecScratch) -> &[MaybeUninit<libc::iovec>] {
    scratch.as_uninit_slice()
}

#[inline(always)]
fn retained_iovecs_mut(scratch: &mut RetainedIovecScratch) -> &mut [libc::iovec] {
    unsafe { iovec_slice_mut_from_uninit(scratch.as_uninit_slice_mut()) }
}

#[inline(always)]
fn remaining_iovec_count(scratch: &RetainedIovecScratch, skip: usize) -> usize {
    scratch.len() - skip
}

#[inline(always)]
fn invalid_input(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message)
}

#[inline(always)]
fn invalid_input_kind() -> io::Error {
    io::Error::from(io::ErrorKind::InvalidInput)
}

#[inline(always)]
fn validate_projected_count_and_len(iov_count: usize, total: usize) -> io::Result<()> {
    if iov_count == 0 && total == 0 {
        return Ok(());
    }
    if iov_count == 0 {
        return Err(invalid_input(
            "projected writev reported bytes but no active pieces",
        ));
    }
    if total == 0 {
        return Err(invalid_input(
            "projected writev reported active pieces but no bytes",
        ));
    }
    Ok(())
}

#[inline(always)]
fn validate_try_projected_count_and_len(iov_count: usize, total: usize) -> io::Result<()> {
    if iov_count == 0 && total == 0 {
        return Ok(());
    }
    if iov_count == 0 || total == 0 {
        return Err(invalid_input_kind());
    }
    Ok(())
}

#[inline]
fn project_retained_writev_payload<T: WritevProjection>(
    payload: &mut RetainedProjectedWritevPayload<T>,
    expected_count: usize,
    expected_total: usize,
) -> io::Result<()> {
    let (projected_count, projected_total) = {
        let source = &payload.source;
        let scratch = payload.scratch.as_uninit_slice_mut();
        let mut pieces = WritevPieces::new(scratch);
        source.project_writev(&mut pieces)?;
        (pieces.count(), pieces.total())
    };

    if projected_count != expected_count {
        return Err(invalid_input(
            "projected writev piece count did not match counted pieces",
        ));
    }
    if projected_total != expected_total {
        return Err(invalid_input(
            "projected writev byte length did not match counted length",
        ));
    }

    Ok(())
}

#[inline(always)]
fn one_shot_syscall_result(result: libc::ssize_t) -> io::Result<usize> {
    if result < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(result as usize)
}

#[inline(always)]
fn checked_try_write_len(len: usize) -> io::Result<usize> {
    if len > u32::MAX as usize {
        return Err(io::Error::from(io::ErrorKind::InvalidInput));
    }
    Ok(len)
}

/// Attempts one nonblocking read syscall into the caller-owned buffer.
#[inline]
pub(crate) fn try_read_once<B: IoBuffReadWrite>(
    fd: RawFd,
    mut buffer: B,
    len: usize,
) -> (io::Result<usize>, B) {
    let len = match super::checked_read_len("try_read", len, buffer.writable_len()) {
        Ok(len) => len as usize,
        Err(err) => return (Err(err), buffer),
    };

    if len == 0 {
        unsafe { buffer.set_written_len(0) };
        return (Ok(0), buffer);
    }

    let result = unsafe { libc::recv(fd, buffer.as_mut_ptr() as *mut libc::c_void, len, 0) };
    match one_shot_syscall_result(result) {
        Ok(actual) => {
            unsafe { buffer.set_written_len(actual) };
            (Ok(actual), buffer)
        }
        Err(err) => (Err(err), buffer),
    }
}

/// Attempts one nonblocking read syscall into the current payload tail.
#[inline]
pub(crate) fn try_read_append_once(
    fd: RawFd,
    mut buffer: IoBuffMut,
    len: usize,
) -> (io::Result<usize>, IoBuffMut) {
    let start_len = buffer.payload_len();
    let len = match super::checked_read_len("try_read_append", len, buffer.payload_remaining()) {
        Ok(len) => len as usize,
        Err(err) => return (Err(err), buffer),
    };

    if len == 0 {
        unsafe { buffer.set_written_len(start_len) };
        return (Ok(0), buffer);
    }

    let result = unsafe { libc::recv(fd, buffer.as_mut_ptr() as *mut libc::c_void, len, 0) };
    match one_shot_syscall_result(result) {
        Ok(actual) => {
            unsafe { buffer.set_written_len(start_len + actual) };
            (Ok(actual), buffer)
        }
        Err(err) => (Err(err), buffer),
    }
}

/// Attempts one nonblocking write syscall from the caller-owned buffer.
#[inline]
pub(crate) fn try_write_once<B: IoBuffReadOnly>(fd: RawFd, buffer: B) -> (io::Result<usize>, B) {
    let len = match checked_try_write_len(buffer.len()) {
        Ok(len) => len,
        Err(err) => return (Err(err), buffer),
    };

    if len == 0 {
        return (Ok(0), buffer);
    }

    let result = unsafe {
        libc::send(
            fd,
            buffer.as_ptr() as *const libc::c_void,
            len,
            libc::MSG_NOSIGNAL,
        )
    };
    (one_shot_syscall_result(result), buffer)
}

const TRY_WRITEV_INLINE_IOVECS: usize = 16;
const TRY_WRITEV_MAX_IOVECS: usize = 1024;

#[inline]
fn try_writev_projected_with_scratch<T: WritevProjection>(
    fd: RawFd,
    source: T,
    expected_count: usize,
    expected_total: usize,
    scratch: &mut [MaybeUninit<libc::iovec>],
) -> (io::Result<usize>, T) {
    let projection = {
        let mut pieces = WritevPieces::new(&mut scratch[..expected_count]);
        source
            .project_writev(&mut pieces)
            .map(|()| (pieces.count(), pieces.total()))
    };

    let (projected_count, projected_total) = match projection {
        Ok(count_and_total) => count_and_total,
        Err(err) => return (Err(err), source),
    };

    if projected_count != expected_count || projected_total != expected_total {
        return (Err(invalid_input_kind()), source);
    }

    let iovecs = unsafe { iovec_slice_mut_from_uninit(&mut scratch[..expected_count]) };
    let msg = libc::msghdr {
        msg_name: std::ptr::null_mut(),
        msg_namelen: 0,
        msg_iov: iovecs.as_mut_ptr(),
        msg_iovlen: iovecs.len(),
        msg_control: std::ptr::null_mut(),
        msg_controllen: 0,
        msg_flags: 0,
    };

    let result = unsafe { libc::sendmsg(fd, &msg, libc::MSG_NOSIGNAL) };
    (one_shot_syscall_result(result), source)
}

/// Attempts one nonblocking gather-write syscall from a projected source.
#[inline]
pub(crate) fn try_writev_projected_once<T: WritevProjection>(
    fd: RawFd,
    source: T,
) -> (io::Result<usize>, T) {
    let (iov_count, total) = source.writev_count_and_len();
    if let Err(err) = validate_try_projected_count_and_len(iov_count, total) {
        return (Err(err), source);
    }
    if total == 0 {
        return (Ok(0), source);
    }
    if iov_count > TRY_WRITEV_MAX_IOVECS {
        return (Err(invalid_input_kind()), source);
    }

    if iov_count <= TRY_WRITEV_INLINE_IOVECS {
        let mut scratch = uninit_iovecs::<TRY_WRITEV_INLINE_IOVECS>();
        return try_writev_projected_with_scratch(fd, source, iov_count, total, &mut scratch);
    }

    let mut scratch = uninit_iovecs::<TRY_WRITEV_MAX_IOVECS>();
    try_writev_projected_with_scratch(fd, source, iov_count, total, &mut scratch)
}

#[inline]
fn submit_initial_projected_writev<T: WritevProjection>(
    pctx: &PollCtx,
    fd: RawFd,
    source: &mut Option<T>,
    iov_count: usize,
    total: usize,
) -> Result<*mut CompletionState, (io::Error, T)> {
    let scratch = match unsafe { (*pctx.reactor()).alloc_iovec_scratch(iov_count) } {
        Ok(scratch) => scratch,
        Err(err) => {
            let source = unsafe { opt_take(source) };
            return Err((err, source));
        }
    };

    let state_ptr = unsafe { (*pctx.reactor()).alloc_op() };
    if state_ptr.is_null() {
        let source = unsafe { opt_take(source) };
        return Err((io::Error::from(io::ErrorKind::WouldBlock), source));
    }

    let guard = UnsubmittedOpGuard::new(pctx.reactor(), state_ptr);
    unsafe { (*state_ptr).register_waiter(pctx.owner_task()) };

    let payload = RetainedProjectedWritevPayload {
        source: unsafe { opt_take(source) },
        scratch,
        written: 0,
        skip: 0,
    };

    unsafe {
        if let Err((err, payload)) = submit_retained_sqe(pctx, state_ptr, payload, |payload| {
            project_retained_writev_payload(payload, iov_count, total)?;
            Ok(build_write_vectored_entry(
                fd,
                retained_iovecs(&payload.scratch),
                0,
                payload.scratch.len(),
                state_ptr as u64,
            ))
        }) {
            guard.free();
            return Err((err, payload.source));
        }
    }

    Ok(guard.disarm())
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

        if let Some((result, payload)) = unsafe {
            take_completed_result_and_payload::<RetainedReadPayload<B>>(cx, &mut this.state_ptr)
        } {
            let mut buffer = payload.buffer;
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

            let payload = RetainedReadPayload {
                buffer: unsafe { opt_take(&mut this.buffer) },
            };
            unsafe {
                if let Err((e, payload)) =
                    submit_retained_sqe(&pctx, state_ptr, payload, |payload| {
                        let ptr = payload.buffer.as_mut_ptr();
                        Ok(opcode::Read::new(types::Fd(this.fd), ptr, this.len)
                            .build()
                            .user_data(state_ptr as u64))
                    })
                {
                    (*pctx.reactor()).free_op(state_ptr);
                    this.state_ptr = std::ptr::null_mut();
                    return Poll::Ready((Err(e), payload.buffer));
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

impl<B: IoBuffReadOnly + 'static, S> Future for WriteFuture<'_, B, S> {
    type Output = (io::Result<usize>, B);

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = unsafe { self.get_unchecked_mut() };

        if let Some((result, payload)) = unsafe {
            take_completed_result_and_payload::<RetainedWritePayload<B>>(cx, &mut this.state_ptr)
        } {
            let buffer = payload.buffer;
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

            let payload = RetainedWritePayload {
                buffer: unsafe { opt_take(&mut this.buffer) },
            };
            unsafe {
                if let Err((e, payload)) =
                    submit_retained_sqe(&pctx, state_ptr, payload, |payload| {
                        let ptr = payload.buffer.as_ptr();
                        let len = payload.buffer.len() as u32;
                        Ok(opcode::Write::new(types::Fd(this.fd), ptr, len)
                            .build()
                            .user_data(state_ptr as u64))
                    })
                {
                    (*pctx.reactor()).free_op(state_ptr);
                    this.state_ptr = std::ptr::null_mut();
                    return Poll::Ready((Err(e), payload.buffer));
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
    /// Stable base pointer into the retained buffer, captured once after the
    /// buffer has been moved into operation state.
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
        Self {
            state_ptr: std::ptr::null_mut(),
            buffer: Some(buffer),
            base_ptr: std::ptr::null(),
            fd,
            offset: 0,
            total,
            _marker: PhantomData,
        }
    }
}

impl<B: IoBuffReadOnly + 'static, S> Future for WriteAllFuture<'_, B, S> {
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
                let payload = unsafe {
                    (*pctx.reactor())
                        .take_retained_payload::<RetainedWritePayload<B>>(this.state_ptr)
                };
                unsafe { free_retry_state(&pctx, &mut this.state_ptr) };
                return Poll::Ready((Err(io::Error::from_raw_os_error(-result)), payload.buffer));
            }

            let n = result as u32;
            if n == 0 {
                let payload = unsafe {
                    (*pctx.reactor())
                        .take_retained_payload::<RetainedWritePayload<B>>(this.state_ptr)
                };
                unsafe { free_retry_state(&pctx, &mut this.state_ptr) };
                return Poll::Ready((
                    Err(io::Error::from(io::ErrorKind::WriteZero)),
                    payload.buffer,
                ));
            }

            debug_assert!(n <= this.total - this.offset);
            this.offset += n;
            if this.offset >= this.total {
                let payload = unsafe {
                    (*pctx.reactor())
                        .take_retained_payload::<RetainedWritePayload<B>>(this.state_ptr)
                };
                unsafe { free_retry_state(&pctx, &mut this.state_ptr) };
                return Poll::Ready((Ok(this.offset as usize), payload.buffer));
            }
        }
        if let Err(err) = unsafe { prepare_retry_state(&pctx, &mut this.state_ptr) } {
            let buffer = unsafe { opt_take(&mut this.buffer) };
            return Poll::Ready((Err(err), buffer));
        }

        if this.buffer.is_some() {
            let payload = RetainedWritePayload {
                buffer: unsafe { opt_take(&mut this.buffer) },
            };
            unsafe {
                if let Err((e, payload)) =
                    submit_retained_sqe(&pctx, this.state_ptr, payload, |payload| {
                        this.base_ptr = payload.buffer.as_ptr();
                        let ptr = this.base_ptr;
                        let remaining = this.total - this.offset;
                        Ok(opcode::Write::new(types::Fd(this.fd), ptr, remaining)
                            .build()
                            .user_data(this.state_ptr as u64))
                    })
                {
                    (*pctx.reactor()).free_op(this.state_ptr);
                    this.state_ptr = std::ptr::null_mut();
                    return Poll::Ready((Err(e), payload.buffer));
                }
            }
            return Poll::Pending;
        }

        let ptr = unsafe { this.base_ptr.add(this.offset as usize) };
        let remaining = this.total - this.offset;

        let sqe = opcode::Write::new(types::Fd(this.fd), ptr, remaining)
            .build()
            .user_data(this.state_ptr as u64);

        unsafe {
            if let Err(e) = submit_tracked_sqe(&pctx, sqe) {
                let payload = (*pctx.reactor())
                    .take_retained_payload::<RetainedWritePayload<B>>(this.state_ptr);
                (*pctx.reactor()).free_op(this.state_ptr);
                this.state_ptr = std::ptr::null_mut();
                return Poll::Ready((Err(e), payload.buffer));
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
    /// Stable base pointer into the retained buffer's writable region.
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
    pub(crate) fn new(fd: RawFd, buffer: B, len: usize) -> Self {
        let mut input_error = None;
        let target = match super::checked_read_len("read_exact", len, buffer.writable_len()) {
            Ok(target) => target,
            Err(err) => {
                input_error = Some(err);
                0
            }
        };
        Self {
            state_ptr: std::ptr::null_mut(),
            buffer: Some(buffer),
            base_ptr: std::ptr::null_mut(),
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
                let payload = unsafe {
                    (*pctx.reactor())
                        .take_retained_payload::<RetainedReadPayload<B>>(this.state_ptr)
                };
                unsafe { free_retry_state(&pctx, &mut this.state_ptr) };
                let mut buffer = payload.buffer;
                unsafe { buffer.set_written_len(this.filled as usize) };
                return Poll::Ready((Err(io::Error::from_raw_os_error(-result)), buffer));
            }

            let n = result as u32;
            if n == 0 {
                let payload = unsafe {
                    (*pctx.reactor())
                        .take_retained_payload::<RetainedReadPayload<B>>(this.state_ptr)
                };
                unsafe { free_retry_state(&pctx, &mut this.state_ptr) };
                let mut buffer = payload.buffer;
                unsafe { buffer.set_written_len(this.filled as usize) };
                return Poll::Ready((Err(io::Error::from(io::ErrorKind::UnexpectedEof)), buffer));
            }

            debug_assert!(n <= this.target - this.filled);
            this.filled += n;
            if this.filled >= this.target {
                let payload = unsafe {
                    (*pctx.reactor())
                        .take_retained_payload::<RetainedReadPayload<B>>(this.state_ptr)
                };
                unsafe { free_retry_state(&pctx, &mut this.state_ptr) };
                let mut buffer = payload.buffer;
                unsafe { buffer.set_written_len(this.target as usize) };
                return Poll::Ready((Ok(this.target as usize), buffer));
            }
        }
        if let Err(err) = unsafe { prepare_retry_state(&pctx, &mut this.state_ptr) } {
            let mut buffer = unsafe { opt_take(&mut this.buffer) };
            unsafe { buffer.set_written_len(this.filled as usize) };
            return Poll::Ready((Err(err), buffer));
        }

        if this.buffer.is_some() {
            let payload = RetainedReadPayload {
                buffer: unsafe { opt_take(&mut this.buffer) },
            };
            unsafe {
                if let Err((e, payload)) =
                    submit_retained_sqe(&pctx, this.state_ptr, payload, |payload| {
                        this.base_ptr = payload.buffer.as_mut_ptr();
                        let ptr = this.base_ptr.add(this.filled as usize);
                        let remaining = this.target - this.filled;
                        Ok(opcode::Read::new(types::Fd(this.fd), ptr, remaining)
                            .build()
                            .user_data(this.state_ptr as u64))
                    })
                {
                    (*pctx.reactor()).free_op(this.state_ptr);
                    this.state_ptr = std::ptr::null_mut();
                    let mut buffer = payload.buffer;
                    buffer.set_written_len(this.filled as usize);
                    return Poll::Ready((Err(e), buffer));
                }
            }
            return Poll::Pending;
        }

        let ptr = unsafe { this.base_ptr.add(this.filled as usize) };
        let remaining = this.target - this.filled;

        let sqe = opcode::Read::new(types::Fd(this.fd), ptr, remaining)
            .build()
            .user_data(this.state_ptr as u64);

        unsafe {
            if let Err(e) = submit_tracked_sqe(&pctx, sqe) {
                let payload = (*pctx.reactor())
                    .take_retained_payload::<RetainedReadPayload<B>>(this.state_ptr);
                (*pctx.reactor()).free_op(this.state_ptr);
                this.state_ptr = std::ptr::null_mut();
                let mut buffer = payload.buffer;
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
// ReadExactAppendFuture
// ---------------------------------------------------------------------------

/// Reads exactly `target` bytes into the current writable tail of an
/// [`IoBuffMut`], preserving any existing payload bytes.
#[doc(hidden)]
pub struct ReadExactAppendFuture<'a, S> {
    /// Completion state reused across sequential retry submissions.
    state_ptr: *mut CompletionState,
    /// Caller-owned destination buffer returned when the operation finishes.
    buffer: Option<IoBuffMut>,
    /// Stable base pointer into the retained buffer's append tail.
    base_ptr: *mut u8,
    /// Stream descriptor read from by this future.
    fd: RawFd,
    /// Payload length present before this append operation started.
    start_len: usize,
    /// Exact append byte count required before the future can succeed.
    target: u32,
    /// Bytes already appended into the destination buffer.
    filled: u32,
    /// Deferred validation error returned before any SQE submission.
    input_error: Option<io::Error>,
    /// Borrows the parent stream for the future lifetime.
    _marker: PhantomData<&'a mut S>,
}

impl<'a, S> ReadExactAppendFuture<'a, S> {
    pub(crate) fn new(fd: RawFd, buffer: IoBuffMut, len: usize) -> Self {
        let start_len = buffer.payload_len();
        let mut input_error = None;
        let target =
            match super::checked_read_len("read_exact_append", len, buffer.payload_remaining()) {
                Ok(target) => target,
                Err(err) => {
                    input_error = Some(err);
                    0
                }
            };
        Self {
            state_ptr: std::ptr::null_mut(),
            buffer: Some(buffer),
            base_ptr: std::ptr::null_mut(),
            fd,
            start_len,
            target,
            filled: 0,
            input_error,
            _marker: PhantomData,
        }
    }

    #[inline(always)]
    unsafe fn set_appended_len(
        mut buffer: IoBuffMut,
        start_len: usize,
        appended: u32,
    ) -> IoBuffMut {
        unsafe { buffer.set_written_len(start_len + appended as usize) };
        buffer
    }
}

impl<S> Future for ReadExactAppendFuture<'_, S> {
    type Output = (io::Result<usize>, IoBuffMut);

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

        // Zero-length append completes immediately and preserves the payload.
        if this.state_ptr.is_null() && this.target == 0 {
            let buffer =
                unsafe { Self::set_appended_len(opt_take(&mut this.buffer), this.start_len, 0) };
            return Poll::Ready((Ok(0), buffer));
        }

        // One context extraction covers free + alloc + submit.
        let pctx = unsafe { poll_ctx_from_waker(cx) };

        // Process completed state if any. Sequential retries reuse the same
        // completion slot once the previous CQE has been fully consumed.
        if !this.state_ptr.is_null() {
            let result = unsafe { (*this.state_ptr).result };

            if result < 0 {
                let payload = unsafe {
                    (*pctx.reactor())
                        .take_retained_payload::<RetainedReadPayload<IoBuffMut>>(this.state_ptr)
                };
                unsafe { free_retry_state(&pctx, &mut this.state_ptr) };
                let buffer =
                    unsafe { Self::set_appended_len(payload.buffer, this.start_len, this.filled) };
                return Poll::Ready((Err(io::Error::from_raw_os_error(-result)), buffer));
            }

            let n = result as u32;
            if n == 0 {
                let payload = unsafe {
                    (*pctx.reactor())
                        .take_retained_payload::<RetainedReadPayload<IoBuffMut>>(this.state_ptr)
                };
                unsafe { free_retry_state(&pctx, &mut this.state_ptr) };
                let buffer =
                    unsafe { Self::set_appended_len(payload.buffer, this.start_len, this.filled) };
                return Poll::Ready((Err(io::Error::from(io::ErrorKind::UnexpectedEof)), buffer));
            }

            debug_assert!(n <= this.target - this.filled);
            this.filled += n;
            if this.filled >= this.target {
                let payload = unsafe {
                    (*pctx.reactor())
                        .take_retained_payload::<RetainedReadPayload<IoBuffMut>>(this.state_ptr)
                };
                unsafe { free_retry_state(&pctx, &mut this.state_ptr) };
                let buffer =
                    unsafe { Self::set_appended_len(payload.buffer, this.start_len, this.target) };
                return Poll::Ready((Ok(this.target as usize), buffer));
            }
        }
        if let Err(err) = unsafe { prepare_retry_state(&pctx, &mut this.state_ptr) } {
            let buffer = unsafe {
                Self::set_appended_len(opt_take(&mut this.buffer), this.start_len, this.filled)
            };
            return Poll::Ready((Err(err), buffer));
        }

        if this.buffer.is_some() {
            let payload = RetainedReadPayload {
                buffer: unsafe { opt_take(&mut this.buffer) },
            };
            unsafe {
                if let Err((e, payload)) =
                    submit_retained_sqe(&pctx, this.state_ptr, payload, |payload| {
                        this.base_ptr = payload.buffer.as_mut_ptr();
                        let ptr = this.base_ptr.add(this.filled as usize);
                        let remaining = this.target - this.filled;
                        Ok(opcode::Read::new(types::Fd(this.fd), ptr, remaining)
                            .build()
                            .user_data(this.state_ptr as u64))
                    })
                {
                    (*pctx.reactor()).free_op(this.state_ptr);
                    this.state_ptr = std::ptr::null_mut();
                    let buffer =
                        Self::set_appended_len(payload.buffer, this.start_len, this.filled);
                    return Poll::Ready((Err(e), buffer));
                }
            }
            return Poll::Pending;
        }

        let ptr = unsafe { this.base_ptr.add(this.filled as usize) };
        let remaining = this.target - this.filled;

        let sqe = opcode::Read::new(types::Fd(this.fd), ptr, remaining)
            .build()
            .user_data(this.state_ptr as u64);

        unsafe {
            if let Err(e) = submit_tracked_sqe(&pctx, sqe) {
                let payload = (*pctx.reactor())
                    .take_retained_payload::<RetainedReadPayload<IoBuffMut>>(this.state_ptr);
                (*pctx.reactor()).free_op(this.state_ptr);
                this.state_ptr = std::ptr::null_mut();
                let buffer = Self::set_appended_len(payload.buffer, this.start_len, this.filled);
                return Poll::Ready((Err(e), buffer));
            }
        }

        Poll::Pending
    }
}

impl<S> Drop for ReadExactAppendFuture<'_, S> {
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
    /// Number of active segments materialized into retained scratch.
    iov_count: usize,
    /// Total writable capacity across all segments, cached so zero-capacity
    /// reads complete before submission and debug checks do not re-walk the
    /// caller-owned chain.
    writable: usize,
    /// Stream descriptor read from by this future.
    fd: RawFd,
    /// Borrows the parent stream for the future lifetime.
    _marker: PhantomData<&'a mut S>,
}

impl<'a, const N: usize, S> ReadvFuture<'a, N, S> {
    pub(crate) fn new(fd: RawFd, buffer: IoBuffVecMut<N>) -> Self {
        let iov_count = buffer.segments();
        let writable = buffer.writable_len();
        Self {
            state_ptr: std::ptr::null_mut(),
            buffer: Some(buffer),
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

        if let Some((result, payload)) = unsafe {
            take_completed_result_and_payload::<RetainedReadvPayload<N>>(cx, &mut this.state_ptr)
        } {
            let mut buffer = payload.buffer;
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

            let scratch = match unsafe { (*pctx.reactor()).alloc_iovec_scratch(this.iov_count) } {
                Ok(scratch) => scratch,
                Err(err) => {
                    let buffer = unsafe { opt_take(&mut this.buffer) };
                    unsafe { (*pctx.reactor()).free_op(state_ptr) };
                    this.state_ptr = std::ptr::null_mut();
                    return Poll::Ready((Err(err), buffer));
                }
            };

            let payload = RetainedReadvPayload {
                buffer: unsafe { opt_take(&mut this.buffer) },
                scratch,
            };
            unsafe {
                if let Err((e, payload)) =
                    submit_retained_sqe(&pctx, state_ptr, payload, |payload| {
                        let (iov_count, writable) =
                            payload.buffer.fill_read_iovecs_and_writable_len(
                                payload.scratch.as_uninit_slice_mut(),
                            );
                        debug_assert_eq!(iov_count, this.iov_count);
                        debug_assert_eq!(writable, this.writable);
                        Ok(build_read_vectored_entry(
                            this.fd,
                            retained_iovecs(&payload.scratch),
                            0,
                            iov_count,
                            state_ptr as u64,
                        ))
                    })
                {
                    (*pctx.reactor()).free_op(state_ptr);
                    this.state_ptr = std::ptr::null_mut();
                    return Poll::Ready((Err(e), payload.buffer));
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
struct WritevFutureCore<'a, C: WriteBufferChain<N> + 'static, const N: usize, S> {
    /// Completion state for the submitted writev/write SQE, if any.
    state_ptr: *mut CompletionState,
    /// Caller-owned read-only segment chain returned on completion.
    buffer: Option<C>,
    /// Number of non-empty source segments to materialize into iovec scratch.
    iov_count: usize,
    /// Total initialized bytes available across all segments.
    total: usize,
    /// Stream descriptor written by this future.
    fd: RawFd,
    /// Borrows the parent stream for the future lifetime.
    _marker: PhantomData<&'a mut S>,
}

impl<'a, C: WriteBufferChain<N> + 'static, const N: usize, S> WritevFutureCore<'a, C, N, S> {
    fn new(fd: RawFd, buffer: C) -> Self {
        let (iov_count, total) = buffer.write_iovec_count_and_len();
        debug_assert!(
            total == 0 || iov_count > 0,
            "non-empty write chain produced no iovecs"
        );
        Self {
            state_ptr: std::ptr::null_mut(),
            buffer: Some(buffer),
            iov_count,
            total,
            fd,
            _marker: PhantomData,
        }
    }
}

impl<C: WriteBufferChain<N> + 'static, const N: usize, S> Future for WritevFutureCore<'_, C, N, S> {
    type Output = (io::Result<usize>, C);

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = unsafe { self.get_unchecked_mut() };

        if let Some((result, payload)) = unsafe {
            take_completed_result_and_payload::<RetainedWritevPayload<C>>(cx, &mut this.state_ptr)
        } {
            let buffer = payload.buffer;
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
            let mut scratch = match unsafe { (*pctx.reactor()).alloc_iovec_scratch(this.iov_count) }
            {
                Ok(scratch) => scratch,
                Err(err) => {
                    let buffer = unsafe { opt_take(&mut this.buffer) };
                    return Poll::Ready((Err(err), buffer));
                }
            };

            unsafe { opt_ref(&this.buffer) }.fill_write_iovecs(scratch.as_uninit_slice_mut());

            let state_ptr = unsafe { (*pctx.reactor()).alloc_op() };
            if state_ptr.is_null() {
                let buffer = unsafe { opt_take(&mut this.buffer) };
                return Poll::Ready((Err(io::Error::from(io::ErrorKind::WouldBlock)), buffer));
            }
            this.state_ptr = state_ptr;

            unsafe { (*state_ptr).register_waiter(pctx.owner_task()) };

            let payload = RetainedWritevPayload {
                buffer: unsafe { opt_take(&mut this.buffer) },
                scratch,
                written: 0,
                skip: 0,
            };
            unsafe {
                if let Err((e, payload)) =
                    submit_retained_sqe(&pctx, state_ptr, payload, |payload| {
                        Ok(build_write_vectored_entry(
                            this.fd,
                            retained_iovecs(&payload.scratch),
                            0,
                            payload.scratch.len(),
                            state_ptr as u64,
                        ))
                    })
                {
                    (*pctx.reactor()).free_op(state_ptr);
                    this.state_ptr = std::ptr::null_mut();
                    return Poll::Ready((Err(e), payload.buffer));
                }
            }
        }

        Poll::Pending
    }
}

impl<C: WriteBufferChain<N> + 'static, const N: usize, S> Drop for WritevFutureCore<'_, C, N, S> {
    fn drop(&mut self) {
        unsafe { drop_op_ptr_unchecked(&mut self.state_ptr) };
    }
}

/// Gather-write from a FlowIO frozen vectored buffer chain (rental pattern).
#[doc(hidden)]
pub struct WritevFuture<'a, const N: usize, S> {
    /// Shared gather-write core specialized to a frozen FlowIO buffer chain.
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
pub struct WritevReadOnlyFuture<'a, B: IoBuffReadOnly + 'static, const N: usize, S> {
    /// Shared gather-write core specialized to a generic read-only chain.
    inner: WritevFutureCore<'a, IoBuffReadOnlyVec<B, N>, N, S>,
}

impl<'a, B: IoBuffReadOnly + 'static, const N: usize, S> WritevReadOnlyFuture<'a, B, N, S> {
    pub(crate) fn new(fd: RawFd, buffer: IoBuffReadOnlyVec<B, N>) -> Self {
        Self {
            inner: WritevFutureCore::new(fd, buffer),
        }
    }
}

impl<B: IoBuffReadOnly + 'static, const N: usize, S> Future for WritevReadOnlyFuture<'_, B, N, S> {
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
struct WritevAllFutureCore<'a, C: WriteBufferChain<N> + 'static, const N: usize, S> {
    /// Completion state reused across sequential retry submissions.
    state_ptr: *mut CompletionState,
    /// Caller-owned read-only segment chain returned when the operation finishes.
    buffer: Option<C>,
    /// Number of non-empty source segments to materialize into iovec scratch.
    iov_count: usize,
    /// Stream descriptor written by this future.
    fd: RawFd,
    /// Total bytes that must be written before completion.
    total: usize,
    /// Borrows the parent stream for the future lifetime.
    _marker: PhantomData<&'a mut S>,
}

impl<'a, C: WriteBufferChain<N> + 'static, const N: usize, S> WritevAllFutureCore<'a, C, N, S> {
    fn new(fd: RawFd, buffer: C) -> Self {
        let (iov_count, total) = buffer.write_iovec_count_and_len();
        debug_assert!(
            total == 0 || iov_count > 0,
            "non-empty write-all chain produced no iovecs"
        );
        Self {
            state_ptr: std::ptr::null_mut(),
            buffer: Some(buffer),
            iov_count,
            fd,
            total,
            _marker: PhantomData,
        }
    }
}

impl<C: WriteBufferChain<N> + 'static, const N: usize, S> Future
    for WritevAllFutureCore<'_, C, N, S>
{
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
                let payload = unsafe {
                    (*pctx.reactor())
                        .take_retained_payload::<RetainedWritevPayload<C>>(this.state_ptr)
                };
                unsafe { free_retry_state(&pctx, &mut this.state_ptr) };
                return Poll::Ready((Err(io::Error::from_raw_os_error(-result)), payload.buffer));
            }

            let n = result as usize;
            if n == 0 {
                let payload = unsafe {
                    (*pctx.reactor())
                        .take_retained_payload::<RetainedWritevPayload<C>>(this.state_ptr)
                };
                unsafe { free_retry_state(&pctx, &mut this.state_ptr) };
                return Poll::Ready((
                    Err(io::Error::from(io::ErrorKind::WriteZero)),
                    payload.buffer,
                ));
            }

            let completed = unsafe {
                let payload = (*this.state_ptr).retained_payload_mut::<RetainedWritevPayload<C>>();
                debug_assert!(n <= this.total - payload.written);
                payload.written += n;
                payload.written >= this.total
            };
            if completed {
                let payload = unsafe {
                    (*pctx.reactor())
                        .take_retained_payload::<RetainedWritevPayload<C>>(this.state_ptr)
                };
                let written = payload.written;
                unsafe { free_retry_state(&pctx, &mut this.state_ptr) };
                return Poll::Ready((Ok(written), payload.buffer));
            }

            unsafe {
                let payload = (*this.state_ptr).retained_payload_mut::<RetainedWritevPayload<C>>();
                let mut skip = payload.skip;
                advance_iovecs_in_place(retained_iovecs_mut(&mut payload.scratch), &mut skip, n);
                payload.skip = skip;
            }
        }
        if let Err(err) = unsafe { prepare_retry_state(&pctx, &mut this.state_ptr) } {
            let buffer = unsafe { opt_take(&mut this.buffer) };
            return Poll::Ready((Err(err), buffer));
        }

        if this.buffer.is_some() {
            let mut scratch = match unsafe { (*pctx.reactor()).alloc_iovec_scratch(this.iov_count) }
            {
                Ok(scratch) => scratch,
                Err(err) => {
                    unsafe { free_retry_state(&pctx, &mut this.state_ptr) };
                    let buffer = unsafe { opt_take(&mut this.buffer) };
                    return Poll::Ready((Err(err), buffer));
                }
            };
            unsafe { opt_ref(&this.buffer) }.fill_write_iovecs(scratch.as_uninit_slice_mut());

            let payload = RetainedWritevPayload {
                buffer: unsafe { opt_take(&mut this.buffer) },
                scratch,
                written: 0,
                skip: 0,
            };

            unsafe {
                if let Err((e, payload)) =
                    submit_retained_sqe(&pctx, this.state_ptr, payload, |payload| {
                        let remaining_iovs = remaining_iovec_count(&payload.scratch, payload.skip);
                        Ok(build_write_vectored_entry(
                            this.fd,
                            retained_iovecs(&payload.scratch),
                            payload.skip,
                            remaining_iovs,
                            this.state_ptr as u64,
                        ))
                    })
                {
                    (*pctx.reactor()).free_op(this.state_ptr);
                    this.state_ptr = std::ptr::null_mut();
                    return Poll::Ready((Err(e), payload.buffer));
                }
            }

            return Poll::Pending;
        }

        let payload = unsafe { (*this.state_ptr).retained_payload::<RetainedWritevPayload<C>>() };
        let remaining_iovs = remaining_iovec_count(&payload.scratch, payload.skip);
        let sqe = build_write_vectored_entry(
            this.fd,
            retained_iovecs(&payload.scratch),
            payload.skip,
            remaining_iovs,
            this.state_ptr as u64,
        );

        unsafe {
            if let Err(e) = submit_tracked_sqe(&pctx, sqe) {
                let payload = (*pctx.reactor())
                    .take_retained_payload::<RetainedWritevPayload<C>>(this.state_ptr);
                (*pctx.reactor()).free_op(this.state_ptr);
                this.state_ptr = std::ptr::null_mut();
                return Poll::Ready((Err(e), payload.buffer));
            }
        }

        Poll::Pending
    }
}

impl<C: WriteBufferChain<N> + 'static, const N: usize, S> Drop
    for WritevAllFutureCore<'_, C, N, S>
{
    fn drop(&mut self) {
        unsafe { drop_op_ptr_unchecked(&mut self.state_ptr) };
    }
}

/// Gather-write the entire FlowIO frozen vectored chain, handling partial writes.
#[doc(hidden)]
pub struct WritevAllFuture<'a, const N: usize, S> {
    /// Shared gather-write-all core specialized to a frozen FlowIO buffer chain.
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
pub struct WritevAllReadOnlyFuture<'a, B: IoBuffReadOnly + 'static, const N: usize, S> {
    /// Shared gather-write-all core specialized to a generic read-only chain.
    inner: WritevAllFutureCore<'a, IoBuffReadOnlyVec<B, N>, N, S>,
}

impl<'a, B: IoBuffReadOnly + 'static, const N: usize, S> WritevAllReadOnlyFuture<'a, B, N, S> {
    pub(crate) fn new(fd: RawFd, buffer: IoBuffReadOnlyVec<B, N>) -> Self {
        Self {
            inner: WritevAllFutureCore::new(fd, buffer),
        }
    }
}

impl<B: IoBuffReadOnly + 'static, const N: usize, S> Future
    for WritevAllReadOnlyFuture<'_, B, N, S>
{
    type Output = (io::Result<usize>, IoBuffReadOnlyVec<B, N>);

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = unsafe { self.get_unchecked_mut() };
        unsafe { Pin::new_unchecked(&mut this.inner) }.poll(cx)
    }
}

// ---------------------------------------------------------------------------
// Projected WritevFuture
// ---------------------------------------------------------------------------

/// Gather-write from one compact retained source projected into write pieces.
#[doc(hidden)]
pub struct WritevProjectedFuture<'a, T: WritevProjection, S> {
    /// Completion state for the submitted projected writev/write SQE, if any.
    state_ptr: *mut CompletionState,
    /// Caller-owned projection source before it is moved into retained state.
    source: Option<T>,
    /// Number of non-empty projected pieces reported by the source.
    iov_count: usize,
    /// Total projected byte length reported by the source.
    total: usize,
    /// Stream descriptor written by this future.
    fd: RawFd,
    /// Borrows the parent stream for the future lifetime.
    _marker: PhantomData<&'a mut S>,
}

impl<'a, T: WritevProjection, S> WritevProjectedFuture<'a, T, S> {
    pub(crate) fn new(fd: RawFd, source: T) -> Self {
        let (iov_count, total) = source.writev_count_and_len();
        Self {
            state_ptr: std::ptr::null_mut(),
            source: Some(source),
            iov_count,
            total,
            fd,
            _marker: PhantomData,
        }
    }
}

impl<T: WritevProjection, S> Future for WritevProjectedFuture<'_, T, S> {
    type Output = (io::Result<usize>, T);

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = unsafe { self.get_unchecked_mut() };

        if let Some((result, payload)) = unsafe {
            take_completed_result_and_payload::<RetainedProjectedWritevPayload<T>>(
                cx,
                &mut this.state_ptr,
            )
        } {
            let source = payload.source;
            if result < 0 {
                return Poll::Ready((Err(io::Error::from_raw_os_error(-result)), source));
            }
            return Poll::Ready((Ok(result as usize), source));
        }

        if let Err(err) = validate_projected_count_and_len(this.iov_count, this.total) {
            let source = unsafe { opt_take(&mut this.source) };
            return Poll::Ready((Err(err), source));
        }

        if this.total == 0 {
            let source = unsafe { opt_take(&mut this.source) };
            return Poll::Ready((Ok(0), source));
        }

        if this.state_ptr.is_null() {
            let pctx = unsafe { poll_ctx_from_waker(cx) };
            match submit_initial_projected_writev(
                &pctx,
                this.fd,
                &mut this.source,
                this.iov_count,
                this.total,
            ) {
                Ok(state_ptr) => {
                    this.state_ptr = state_ptr;
                }
                Err((err, source)) => return Poll::Ready((Err(err), source)),
            }
        }

        Poll::Pending
    }
}

impl<T: WritevProjection, S> Drop for WritevProjectedFuture<'_, T, S> {
    fn drop(&mut self) {
        unsafe { drop_op_ptr_unchecked(&mut self.state_ptr) };
    }
}

// ---------------------------------------------------------------------------
// Projected WritevAllFuture
// ---------------------------------------------------------------------------

/// Gather-write all projected pieces from one compact retained source.
#[doc(hidden)]
pub struct WritevAllProjectedFuture<'a, T: WritevProjection, S> {
    /// Completion state reused across projected retry submissions.
    state_ptr: *mut CompletionState,
    /// Caller-owned projection source before it is moved into retained state.
    source: Option<T>,
    /// Number of non-empty projected pieces reported by the source.
    iov_count: usize,
    /// Stream descriptor written by this future.
    fd: RawFd,
    /// Total projected byte length that must be written before completion.
    total: usize,
    /// Borrows the parent stream for the future lifetime.
    _marker: PhantomData<&'a mut S>,
}

impl<'a, T: WritevProjection, S> WritevAllProjectedFuture<'a, T, S> {
    pub(crate) fn new(fd: RawFd, source: T) -> Self {
        let (iov_count, total) = source.writev_count_and_len();
        Self {
            state_ptr: std::ptr::null_mut(),
            source: Some(source),
            iov_count,
            fd,
            total,
            _marker: PhantomData,
        }
    }
}

impl<T: WritevProjection, S> Future for WritevAllProjectedFuture<'_, T, S> {
    type Output = (io::Result<usize>, T);

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = unsafe { self.get_unchecked_mut() };

        if !this.state_ptr.is_null() {
            let state = unsafe { &*this.state_ptr };
            if !state.is_completed() {
                return Poll::Pending;
            }
        }

        if this.source.is_some() {
            if let Err(err) = validate_projected_count_and_len(this.iov_count, this.total) {
                let source = unsafe { opt_take(&mut this.source) };
                return Poll::Ready((Err(err), source));
            }
            if this.total == 0 {
                let source = unsafe { opt_take(&mut this.source) };
                return Poll::Ready((Ok(0), source));
            }
        }

        let pctx = unsafe { poll_ctx_from_waker(cx) };

        if !this.state_ptr.is_null() {
            let result = unsafe { (*this.state_ptr).result };

            if result < 0 {
                let payload = unsafe {
                    (*pctx.reactor())
                        .take_retained_payload::<RetainedProjectedWritevPayload<T>>(this.state_ptr)
                };
                unsafe { free_retry_state(&pctx, &mut this.state_ptr) };
                return Poll::Ready((Err(io::Error::from_raw_os_error(-result)), payload.source));
            }

            let n = result as usize;
            if n == 0 {
                let payload = unsafe {
                    (*pctx.reactor())
                        .take_retained_payload::<RetainedProjectedWritevPayload<T>>(this.state_ptr)
                };
                unsafe { free_retry_state(&pctx, &mut this.state_ptr) };
                return Poll::Ready((
                    Err(io::Error::from(io::ErrorKind::WriteZero)),
                    payload.source,
                ));
            }

            let completed = unsafe {
                let payload =
                    (*this.state_ptr).retained_payload_mut::<RetainedProjectedWritevPayload<T>>();
                debug_assert!(n <= this.total - payload.written);
                payload.written += n;
                payload.written >= this.total
            };
            if completed {
                let payload = unsafe {
                    (*pctx.reactor())
                        .take_retained_payload::<RetainedProjectedWritevPayload<T>>(this.state_ptr)
                };
                let written = payload.written;
                unsafe { free_retry_state(&pctx, &mut this.state_ptr) };
                return Poll::Ready((Ok(written), payload.source));
            }

            unsafe {
                let payload =
                    (*this.state_ptr).retained_payload_mut::<RetainedProjectedWritevPayload<T>>();
                let mut skip = payload.skip;
                advance_iovecs_in_place(retained_iovecs_mut(&mut payload.scratch), &mut skip, n);
                payload.skip = skip;
            }
        }

        if this.source.is_some() {
            match submit_initial_projected_writev(
                &pctx,
                this.fd,
                &mut this.source,
                this.iov_count,
                this.total,
            ) {
                Ok(state_ptr) => {
                    this.state_ptr = state_ptr;
                    return Poll::Pending;
                }
                Err((err, source)) => return Poll::Ready((Err(err), source)),
            }
        }

        debug_assert!(
            !this.state_ptr.is_null(),
            "projected writev retry state unexpectedly missing"
        );
        if this.state_ptr.is_null() {
            return Poll::Pending;
        }
        unsafe {
            (*this.state_ptr).reset_for_resubmit();
            (*this.state_ptr).register_waiter(pctx.owner_task());
        }

        let payload =
            unsafe { (*this.state_ptr).retained_payload::<RetainedProjectedWritevPayload<T>>() };
        let remaining_iovs = remaining_iovec_count(&payload.scratch, payload.skip);
        let sqe = build_write_vectored_entry(
            this.fd,
            retained_iovecs(&payload.scratch),
            payload.skip,
            remaining_iovs,
            this.state_ptr as u64,
        );

        unsafe {
            if let Err(e) = submit_tracked_sqe(&pctx, sqe) {
                let payload = (*pctx.reactor())
                    .take_retained_payload::<RetainedProjectedWritevPayload<T>>(this.state_ptr);
                (*pctx.reactor()).free_op(this.state_ptr);
                this.state_ptr = std::ptr::null_mut();
                return Poll::Ready((Err(e), payload.source));
            }
        }

        Poll::Pending
    }
}

impl<T: WritevProjection, S> Drop for WritevAllProjectedFuture<'_, T, S> {
    fn drop(&mut self) {
        unsafe { drop_op_ptr_unchecked(&mut self.state_ptr) };
    }
}

// ---------------------------------------------------------------------------
// ReadvExactFuture
// ---------------------------------------------------------------------------

/// Scatter-read exactly `target` bytes into a vectored buffer chain,
/// re-submitting on partial reads with retained `iovec` scratch.
/// Returns `UnexpectedEof` if the peer closes before the target is reached.
#[doc(hidden)]
pub struct ReadvExactFuture<'a, const N: usize, S> {
    /// Completion state reused across sequential retry submissions.
    state_ptr: *mut CompletionState,
    /// Caller-owned mutable segment chain returned when the operation finishes.
    buffer: Option<IoBuffVecMut<N>>,
    /// Number of active segments materialized into retained scratch.
    iov_count: usize,
    /// Total writable capacity across all segments, cached for pre-submit
    /// target validation and debug checks without re-walking the caller-owned
    /// chain.
    writable: usize,
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
        let iov_count = buffer.segments();
        let writable = buffer.writable_len();
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
            iov_count,
            writable,
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
                let payload = unsafe {
                    (*pctx.reactor())
                        .take_retained_payload::<RetainedReadvPayload<N>>(this.state_ptr)
                };
                unsafe { free_retry_state(&pctx, &mut this.state_ptr) };
                let mut buffer = payload.buffer;
                unsafe { buffer.distribute_written(this.filled) };
                return Poll::Ready((Err(io::Error::from_raw_os_error(-result)), buffer));
            }

            let n = result as usize;
            if n == 0 {
                let payload = unsafe {
                    (*pctx.reactor())
                        .take_retained_payload::<RetainedReadvPayload<N>>(this.state_ptr)
                };
                unsafe { free_retry_state(&pctx, &mut this.state_ptr) };
                let mut buffer = payload.buffer;
                unsafe { buffer.distribute_written(this.filled) };
                return Poll::Ready((Err(io::Error::from(io::ErrorKind::UnexpectedEof)), buffer));
            }

            debug_assert!(n <= this.target - this.filled);
            this.filled += n;
            if this.filled >= this.target {
                let payload = unsafe {
                    (*pctx.reactor())
                        .take_retained_payload::<RetainedReadvPayload<N>>(this.state_ptr)
                };
                unsafe { free_retry_state(&pctx, &mut this.state_ptr) };
                let mut buffer = payload.buffer;
                unsafe { buffer.distribute_written(this.target) };
                return Poll::Ready((Ok(this.target), buffer));
            }

            unsafe {
                let payload = (*this.state_ptr).retained_payload_mut::<RetainedReadvPayload<N>>();
                advance_iovecs_in_place(
                    retained_iovecs_mut(&mut payload.scratch),
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

        if this.buffer.is_some() {
            let scratch = match unsafe { (*pctx.reactor()).alloc_iovec_scratch(this.iov_count) } {
                Ok(scratch) => scratch,
                Err(err) => {
                    let mut buffer = unsafe { opt_take(&mut this.buffer) };
                    unsafe { buffer.distribute_written(this.filled) };
                    unsafe { free_retry_state(&pctx, &mut this.state_ptr) };
                    return Poll::Ready((Err(err), buffer));
                }
            };

            let payload = RetainedReadvPayload {
                buffer: unsafe { opt_take(&mut this.buffer) },
                scratch,
            };
            unsafe {
                if let Err((e, payload)) =
                    submit_retained_sqe(&pctx, this.state_ptr, payload, |payload| {
                        let (iov_count, writable) =
                            payload.buffer.fill_read_iovecs_and_writable_len(
                                payload.scratch.as_uninit_slice_mut(),
                            );
                        debug_assert_eq!(iov_count, this.iov_count);
                        debug_assert_eq!(writable, this.writable);
                        let remaining_iovs = remaining_iovec_count(&payload.scratch, this.skip);
                        Ok(build_read_vectored_entry(
                            this.fd,
                            retained_iovecs(&payload.scratch),
                            this.skip,
                            remaining_iovs,
                            this.state_ptr as u64,
                        ))
                    })
                {
                    (*pctx.reactor()).free_op(this.state_ptr);
                    this.state_ptr = std::ptr::null_mut();
                    let mut buffer = payload.buffer;
                    buffer.distribute_written(this.filled);
                    return Poll::Ready((Err(e), buffer));
                }
            }

            return Poll::Pending;
        }

        let payload = unsafe { (*this.state_ptr).retained_payload::<RetainedReadvPayload<N>>() };
        let remaining_iovs = remaining_iovec_count(&payload.scratch, this.skip);
        let sqe = build_read_vectored_entry(
            this.fd,
            retained_iovecs(&payload.scratch),
            this.skip,
            remaining_iovs,
            this.state_ptr as u64,
        );

        unsafe {
            if let Err(e) = submit_tracked_sqe(&pctx, sqe) {
                let payload = (*pctx.reactor())
                    .take_retained_payload::<RetainedReadvPayload<N>>(this.state_ptr);
                (*pctx.reactor()).free_op(this.state_ptr);
                this.state_ptr = std::ptr::null_mut();
                let mut buffer = payload.buffer;
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
