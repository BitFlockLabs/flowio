//! Shared io_uring futures for byte-stream transports and FlowIO's TLS wrapper.
//!
//! Scalar reads use `IORING_OP_RECV` without flags; writes use `IORING_OP_SEND` /
//! `IORING_OP_SENDMSG` with `MSG_NOSIGNAL`. The `_all` / `_exact` variants
//! handle partial completions internally. The stream type
//! parameter `S` is carried only in `PhantomData` to borrow the parent stream
//! for the duration of the operation.
//!
//! Vectored operations materialize `iovec` arrays into scratch storage.
//! Partial progress advances that scratch in place. One remaining segment
//! uses `IORING_OP_RECV` for reads up to `i32::MAX` bytes or `IORING_OP_SEND`
//! for write windows up to `u32::MAX`, submitting at most `i32::MAX` bytes.
//! Other vectored reads use `IORING_OP_RECVMSG`.
//! Complete vectored writes switch to single-buffer SEND for the remaining
//! call after a completed SENDMSG EAGAIN; a scalar SEND EAGAIN stays terminal.
//! Socket `SO_SNDTIMEO` does not bound FlowIO writes; use cancellation or a
//! FlowIO timeout to bound the call.
//!
//! `CompletionState` is allocated from the reactor's pool for each active
//! operation. A stream receive resubmits a no-progress EAGAIN completion with
//! the same slot and payload. Exact/all futures also reuse their slot for
//! partial progress; every resubmission follows the previous target CQE.
//!
//! If a future is dropped while its SQE is still in flight, the state is
//! marked orphaned and an `ASYNC_CANCEL` SQE is submitted; the CQE path then
//! reclaims the pool slot. Futures attach caller buffers and vectored scratch
//! to the `CompletionState` before submission so kernel-referenced memory stays
//! alive through every referring CQE. Retry futures retain that payload across
//! sequential submissions until the overall operation finishes. If a read
//! future is dropped before completion, any bytes consumed by a racing
//! completion are discarded with the retained buffer when its final referring
//! CQE retires.
//! If a future is dropped after completion but before polling its result, the
//! completed state is freed immediately from `Drop`.

use crate::net::send_sqe::{
    build_recv_entry, build_recvmsg_entry, build_send_entry, build_sendmsg_entry,
};
use crate::net::{
    CompletionTake, MsgHdrInit, complete_read_with_progress, completion_cqe_result,
    finish_completed_payload_take, finish_completed_take, write_msghdr,
};
use crate::runtime::buffer::iobuffvec::{
    IoBuffReadOnlyVec, IoBuffVec, IoBuffVecMut, checked_iovec_count_and_length_sum,
    invalid_read_iovec_shape,
};
use crate::runtime::buffer::{IoBuffReadOnly, IoBuffReadWrite};
use crate::runtime::executor::{
    PollCtx, completed_op_ctx, drop_fd_op_state_unchecked, poll_ctx_from_waker,
    prepare_unsubmitted_op, refresh_op_waiter_from_waker, submit_initialized_retained_fd_sqe,
    submit_resubmitted_fd_sqe, submit_retained_fd_sqe, validate_local_io_result,
};
#[cfg(debug_assertions)]
use crate::runtime::executor::{
    record_retained_iovec_oversize_rejection, validate_local_iovec_oversize_rejection,
};
use crate::runtime::fd::{FdStateDiagnostic, RuntimeFdOpState};
use crate::runtime::op::CompletionState;
use crate::runtime::reactor::Reactor;
use crate::runtime::retained::{
    RETAINED_IOVEC_INLINE_COUNT, RETAINED_IOVEC_MAX_COUNT, RETAINED_IOVEC_OVERSIZE_MESSAGE,
    RetainedIovecScratch, RetainedIovecScratchInit, RetainedPayload, RetainedPayloadPool,
    with_raw_retained_slot,
};
use crate::runtime::task::TaskHeader;
use io_uring::squeue;
use std::cell::RefCell;
use std::future::Future;
use std::io;
use std::marker::PhantomData;
use std::mem::MaybeUninit;
use std::os::fd::RawFd;
use std::pin::Pin;
use std::ptr::NonNull;
use std::slice;
use std::task::{Context, Poll};

// Return send errors without delivering SIGPIPE.
const SEND_FLAGS: i32 = libc::MSG_NOSIGNAL;

macro_rules! impl_stream_rw {
    ($stream:ident, $stream_path:literal) => {
        /// Attempts one nonblocking read syscall and returns immediately.
        ///
        /// This is a deadline-edge primitive for callers whose phase
        /// timeout has already reached `Duration::ZERO`. It does not
        /// submit an `io_uring` operation, register a waiter, park, retry,
        /// or allocate. If no data is immediately available on the
        /// existing nonblocking socket, it returns
        /// [`io::ErrorKind::WouldBlock`] and returns `buffer` unchanged.
        /// Positive progress appends to an
        /// [`IoBuffMut`](crate::runtime::buffer::IoBuffMut) payload; buffers
        /// that retain the provided zero write base publish from their beginning.
        /// The returned byte count is always relative to this call.
        ///
        /// Prefer [`Self::read`] for normal FlowIO async I/O.
        /// Avoid polling this method as a readiness loop: `WouldBlock` does not
        /// register a wakeup.
        #[doc = concat!(
            "# Example\n",
            "```no_run\n",
            "use ", $stream_path, ";\n\n",
            "# fn deadline_edge_read(mut stream: ", stringify!($stream), ") {\n",
            "let (result, buffer) = stream.try_read(vec![0u8; 1024], 1024);\n",
            "if let Ok(n) = result {\n",
            "    let _bytes = &buffer[..n];\n",
            "}\n",
            "# }\n",
            "```"
        )]
        pub fn try_read<B: IoBuffReadWrite>(
            &mut self,
            buffer: B,
            len: usize,
        ) -> (io::Result<usize>, B) {
            stream::try_read_once(self.fd.raw_fd(), buffer, len)
        }

        /// Attempts one nonblocking write syscall and returns immediately.
        ///
        /// This sends from the initialized bytes in `buffer` with no
        /// reactor registration and no retry. If the socket cannot accept
        /// bytes now, it returns [`io::ErrorKind::WouldBlock`] and returns
        /// `buffer` unchanged.
        ///
        /// Prefer [`Self::write`] for normal FlowIO async I/O.
        /// Avoid polling this method as a readiness loop: `WouldBlock` does not
        /// register a wakeup.
        #[doc = concat!(
            "# Example\n",
            "```no_run\n",
            "use ", $stream_path, ";\n\n",
            "# fn deadline_edge_write(mut stream: ", stringify!($stream), ") {\n",
            "let (result, buffer) = stream.try_write(b\"ping\".to_vec());\n",
            "if result.is_err() {\n",
            "    let _retry_later = buffer;\n",
            "}\n",
            "# }\n",
            "```"
        )]
        pub fn try_write<B: IoBuffReadOnly>(
            &mut self,
            buffer: B,
        ) -> (io::Result<usize>, B) {
            stream::try_write_once(self.fd.raw_fd(), buffer)
        }

        /// Attempts one nonblocking projected gather-write syscall.
        ///
        /// FlowIO projects borrowed byte pieces from the owned `source`,
        /// performs one `sendmsg`, and returns the source immediately. Up to 16
        /// pieces use inline stack scratch; larger projections normally use
        /// bounded reusable thread-local `Vec` scratch and may allocate when
        /// capacity must grow. Re-entry or thread-local teardown uses one
        /// bounded local vector instead; if its reservation fails, this returns
        /// [`io::ErrorKind::WouldBlock`] with the exact source. Message bytes
        /// are not copied, and no operation slot is used.
        /// Projections above 1024 non-empty pieces are rejected with
        /// [`io::ErrorKind::InvalidInput`]. A declared-empty projection is
        /// still invoked once for contract validation; a valid empty projection
        /// completes with `Ok(0)` and no syscall.
        ///
        /// This is a deadline-edge primitive. Prefer
        /// [`Self::writev_projected`] / [`Self::writev_all_projected`] for
        /// normal FlowIO async I/O. Avoid this on an allocation-sensitive
        /// deadline edge with more than 16 pieces unless execution is
        /// non-reentrant and its thread-local scratch has already grown to the
        /// required capacity.
        #[doc = concat!(
            "# Example\n",
            "```no_run\n",
            "use ", $stream_path, ";\n",
            "use flowio::net::{WritevPieces, WritevProjection};\n",
            "use std::io;\n\n",
            "struct Frame {\n",
            "    header: [u8; 2],\n",
            "    body: Vec<u8>,\n",
            "}\n\n",
            "impl WritevProjection for Frame {\n",
            "    fn writev_count_and_len(&self) -> (usize, usize) {\n",
            "        (2, self.header.len() + self.body.len())\n",
            "    }\n\n",
            "    fn project_writev<'a>(&'a self, pieces: &mut WritevPieces<'a>) -> io::Result<()> {\n",
            "        pieces.push(&self.header)?;\n",
            "        pieces.push(&self.body)?;\n",
            "        Ok(())\n",
            "    }\n",
            "}\n\n",
            "# fn deadline_edge_projected(mut stream: ", stringify!($stream), ") {\n",
            "let frame = Frame {\n",
            "    header: *b\"H:\",\n",
            "    body: b\"ping\".to_vec(),\n",
            "};\n",
            "let (result, frame) = stream.try_writev_projected(frame);\n",
            "if result.is_err() {\n",
            "    let _retry_later = frame;\n",
            "}\n",
            "# }\n",
            "```"
        )]
        pub fn try_writev_projected<T: WritevProjection>(
            &mut self,
            source: T,
        ) -> (io::Result<usize>, T) {
            stream::try_writev_projected_once(self.fd.raw_fd(), source)
        }

        /// Reads up to `len` bytes into `buffer`.
        ///
        /// The buffer is consumed and returned alongside the result on
        /// completion (rental pattern); the actual byte count is returned
        /// in the `Ok` variant.
        ///
        /// Positive progress appends to an
        /// [`IoBuffMut`](crate::runtime::buffer::IoBuffMut) payload. Buffers
        /// that retain the provided zero write base publish from their beginning.
        /// A zero-byte completion or an error before progress leaves the
        /// buffer's existing logical contents unchanged; the returned count is
        /// relative to this operation rather than the resulting total length.
        /// A request with `len == 0` completes locally with `Ok(0)` and no
        /// kernel submission; that request-scoped result does not report peer
        /// EOF.
        ///
        /// Preferred on the stream fast path when the caller tracks framing:
        /// this returns the first positive or terminal completion without
        /// looping on partial progress. A no-progress EAGAIN completion is
        /// resubmitted internally.
        pub fn read<B: IoBuffReadWrite>(
            &mut self,
            buffer: B,
            len: usize,
        ) -> stream::ReadFuture<'_, B, Self> {
            stream::ReadFuture::new(self.fd.op_state(), buffer, len)
        }

        /// Writes the initialized portion of `buffer`.
        ///
        /// The buffer is consumed and returned alongside the result on
        /// completion (rental pattern); the actual byte count is returned
        /// in the `Ok` variant.
        /// An empty readable window completes locally with `Ok(0)` and no
        /// kernel submission.
        ///
        /// Preferred on the stream fast path when the caller tracks progress:
        /// this performs one contiguous submission and returns a short write
        /// directly. Each submission requests at most `i32::MAX` bytes;
        /// [`write_all`](Self::write_all) advances by the actual completion count.
        pub fn write<B: IoBuffReadOnly>(
            &mut self,
            buffer: B,
        ) -> stream::WriteFuture<'_, B, Self> {
            stream::WriteFuture::new(self.fd.op_state(), buffer)
        }

        /// Writes the entire buffer, handling partial writes internally.
        ///
        /// Returns `(Ok(n), buffer)` where `n` equals `buffer.len()` on
        /// success. On error the buffer is returned with an unspecified
        /// amount already written.
        ///
        /// This complete-buffer API may resubmit after partial writes. Avoid
        /// that retry bookkeeping when complete-buffer semantics are not
        /// required; use [`Self::write`] and track progress in the caller.
        pub fn write_all<B: IoBuffReadOnly>(
            &mut self,
            buffer: B,
        ) -> stream::WriteAllFuture<'_, B, Self> {
            stream::WriteAllFuture::new(self.fd.op_state(), buffer)
        }

        /// Reads exactly `len` bytes, handling partial reads internally.
        ///
        /// Returns `(Ok(len), buffer)` on success. Returns `UnexpectedEof`
        /// if the peer closes before `len` bytes arrive. Positive progress
        /// appends to an [`IoBuffMut`](crate::runtime::buffer::IoBuffMut)
        /// payload; buffers that retain the provided zero write base publish
        /// from their beginning. Any prefix read before EOF or another terminal
        /// error remains published, while an error before progress preserves
        /// the existing logical contents. The result count remains relative
        /// to this operation.
        ///
        /// This complete-buffer API may resubmit after partial reads. Avoid
        /// that retry bookkeeping when exact-length semantics are not
        /// required; use [`Self::read`] and track framing in the caller.
        pub fn read_exact<B: IoBuffReadWrite>(
            &mut self,
            buffer: B,
            len: usize,
        ) -> stream::ReadExactFuture<'_, B, Self> {
            stream::ReadExactFuture::new(self.fd.op_state(), buffer, len)
        }

        /// Scatter-read into a vectored buffer chain.
        ///
        /// The chain is consumed and returned alongside the result (rental
        /// pattern). The total number of bytes read is returned in `Ok`.
        /// A no-progress EAGAIN completion is resubmitted internally;
        /// partial progress is returned without another receive.
        ///
        /// Use this when the receive path is already naturally segmented. For a
        /// single contiguous destination buffer, prefer [`Self::read`] to avoid
        /// building an `iovec` array.
        ///
        /// # Errors
        ///
        /// Returns [`io::ErrorKind::InvalidInput`] if the chain has no writable
        /// segments, has more than 1024 non-empty writable segments, has a
        /// total writable length that overflows `usize`, or its writable
        /// segment count or byte total changes before submission. The
        /// segment-count, overflow, and shape failures return the chain without
        /// submitting kernel I/O.
        pub fn readv<const N: usize>(
            &mut self,
            buffer: IoBuffVecMut<N>,
        ) -> stream::ReadvFuture<'_, N, Self> {
            stream::ReadvFuture::new(self.fd.op_state(), buffer)
        }

        /// Gather-write from an owned vectored buffer chain.
        ///
        /// The chain is consumed and returned alongside the result (rental
        /// pattern). The total number of bytes written is returned in
        /// `Ok`. Empty chains complete with `Ok(0)` without submitting
        /// kernel I/O. Both FlowIO frozen chains and generic read-only
        /// chains are accepted.
        ///
        /// Use this when the send path is already naturally segmented. For one
        /// contiguous payload, prefer [`Self::write`] to avoid building an
        /// `iovec` array.
        ///
        /// # Errors
        ///
        /// Returns [`io::ErrorKind::InvalidInput`] when the aggregate readable
        /// byte count cannot be represented by `usize`.
        /// A write of more than one segment can also return the kernel's `WouldBlock`;
        /// see [error semantics](crate::net#error-semantics).
        pub fn writev<C: WriteBufferChain<N>, const N: usize>(
            &mut self,
            buffer: C,
        ) -> stream::WritevFuture<'_, C, N, Self> {
            stream::WritevFuture::new(self.fd.op_state(), buffer)
        }

        /// Gather-write projected pieces from one compact owned source.
        ///
        /// FlowIO takes ownership of `source`. For a non-empty write, it moves
        /// `source` into storage owned by the operation, then projects borrowed
        /// byte slices into an `iovec` array that FlowIO owns. Both stay alive
        /// until FlowIO observes the write's own completion, even if the future
        /// is dropped. Projection copies only pointer/length metadata, not
        /// message bytes. After runtime-context validation succeeds, declared-empty
        /// projections are still invoked once for contract validation; valid
        /// empty projections complete with
        /// `Ok(0)` without submitting kernel I/O.
        ///
        /// Use this when the send path is already naturally segmented inside
        /// `source`. For one contiguous payload, prefer [`Self::write`] to
        /// avoid projection and building an `iovec` array.
        /// A write of more than one segment can also return the kernel's `WouldBlock`;
        /// see [error semantics](crate::net#error-semantics).
        pub fn writev_projected<T: WritevProjection>(
            &mut self,
            source: T,
        ) -> stream::WritevProjectedFuture<'_, T, Self> {
            stream::WritevProjectedFuture::new(self.fd.op_state(), source)
        }

        /// Gather-write an entire owned vectored chain, handling partial
        /// writes.
        ///
        /// Returns `(Ok(n), chain)` where `n` equals the total byte count on
        /// success. On error the chain is returned with an unspecified
        /// amount already written. Empty chains complete with `Ok(0)`
        /// without submitting kernel I/O. Both FlowIO frozen chains and
        /// generic read-only chains are accepted.
        ///
        /// If the kernel reports `WouldBlock` for a multi-segment send, or for
        /// one segment of 4 GiB or more, the rest is sent one segment at a time
        /// instead of returning that error.
        ///
        /// This complete-buffer vectored API may resubmit after partial writes.
        /// Avoid that retry bookkeeping when the caller handles partial
        /// progress; use [`Self::writev`] instead.
        ///
        /// # Errors
        ///
        /// Returns [`io::ErrorKind::InvalidInput`] when the aggregate readable
        /// byte count cannot be represented by `usize`.
        pub fn writev_all<C: WriteBufferChain<N>, const N: usize>(
            &mut self,
            buffer: C,
        ) -> stream::WritevAllFuture<'_, C, N, Self> {
            stream::WritevAllFuture::new(self.fd.op_state(), buffer)
        }

        /// Gather-write all projected pieces from one compact owned
        /// source.
        ///
        /// Returns `(Ok(n), source)` where `n` equals the projected total
        /// byte count on success. On error the source is returned with an
        /// unspecified amount already written. After runtime-context
        /// validation succeeds, declared-empty projections are still invoked
        /// once for contract validation; valid empty projections complete with
        /// `Ok(0)` without submitting kernel I/O.
        ///
        /// This complete-buffer API may resubmit after partial writes. Avoid
        /// that retry bookkeeping when the caller can handle partial progress;
        /// use [`Self::writev_projected`] instead.
        ///
        /// If the kernel reports `WouldBlock` for a multi-segment send, or for
        /// one segment of 4 GiB or more, the rest is sent one segment at a time
        /// instead of returning that error.
        pub fn writev_all_projected<T: WritevProjection>(
            &mut self,
            source: T,
        ) -> stream::WritevAllProjectedFuture<'_, T, Self> {
            stream::WritevAllProjectedFuture::new(self.fd.op_state(), source)
        }

        /// Scatter-read exactly `len` total bytes into a vectored chain.
        ///
        /// Returns `(Ok(len), chain)` on success. Returns `UnexpectedEof`
        /// if the peer closes before `len` bytes arrive. A zero `len`
        /// completes with `Ok(0)` without submitting kernel I/O.
        ///
        /// This complete-buffer vectored API may resubmit after partial reads.
        /// Avoid that retry bookkeeping when the caller handles partial
        /// progress; use [`Self::readv`] instead.
        ///
        /// # Errors
        ///
        /// Returns [`io::ErrorKind::InvalidInput`] if `len` exceeds the chain's
        /// writable capacity or `u32::MAX`, the chain's total writable length
        /// overflows `usize`, a nonzero `len` targets a chain with more than
        /// 1024 non-empty writable segments, or the chain's writable segment
        /// count or byte total changes before submission. The overflow,
        /// segment-count, and shape failures return the chain without submitting
        /// kernel I/O. A zero `len` is valid even when the chain has no writable
        /// segments.
        pub fn readv_exact<const N: usize>(
            &mut self,
            buffer: IoBuffVecMut<N>,
            len: usize,
        ) -> stream::ReadvExactFuture<'_, N, Self> {
            stream::ReadvExactFuture::new(self.fd.op_state(), buffer, len)
        }
    };
}

pub(crate) use impl_stream_rw;

#[inline(always)]
/// Returns a completed result plus the retained payload, then retires the
/// completion-state slot.
///
/// # Safety
///
/// A non-null `*state_ptr` must identify a completed FlowIO operation, and its
/// retained payload type must be `T`. Cleanup uses the recorded origin reactor.
unsafe fn take_completed_result_and_payload<T: 'static>(
    cx: &mut Context<'_>,
    fd_state: &mut RuntimeFdOpState<'_>,
) -> Option<CompletionTake<i32, T>> {
    let state_ptr = fd_state.state_ptr();
    if state_ptr.is_null() {
        return None;
    }

    let state = unsafe { &mut *state_ptr };
    if !state.is_completed() {
        return None;
    }

    let result = state.result;
    unsafe {
        finish_completed_payload_take::<T>(cx, state_ptr, result, || {
            let retired = fd_state.take_state_ptr();
            debug_assert_eq!(retired, state_ptr);
        })
    }
}

#[inline(always)]
/// Returns a completed result plus data extracted from the retained payload,
/// then retires the completion-state slot.
///
/// # Safety
///
/// A non-null `*state_ptr` must identify a completed FlowIO operation with
/// retained payload type `T`. Cleanup uses its recorded origin reactor, and
/// `extract` must leave no live resource requiring destruction.
unsafe fn take_completed_result_and_payload_with<T: 'static, R>(
    cx: &mut Context<'_>,
    fd_state: &mut RuntimeFdOpState<'_>,
    extract: impl FnOnce(*mut T) -> R,
) -> Option<CompletionTake<i32, R>> {
    let state_ptr = fd_state.state_ptr();
    if state_ptr.is_null() {
        return None;
    }

    let state = unsafe { &mut *state_ptr };
    if !state.is_completed() {
        return None;
    }

    let result = state.result;
    unsafe {
        finish_completed_take(
            cx,
            state_ptr,
            result,
            |op_ctx, state_ptr| {
                op_ctx.take_retained_payload_with_unchecked::<T, R>(state_ptr, extract)
            },
            || {
                let retired = fd_state.take_state_ptr();
                debug_assert_eq!(retired, state_ptr);
            },
        )
    }
}

#[inline(always)]
/// Returns the matching current poll context for a completed retry, or takes
/// its retained payload through the origin reactor after context rejection.
///
/// # Safety
///
/// `*state_ptr` must identify a completed operation retaining payload type `T`.
unsafe fn retry_poll_ctx_or_rejected_payload<T: 'static>(
    cx: &mut Context<'_>,
    fd_state: &mut RuntimeFdOpState<'_>,
) -> Result<PollCtx, T> {
    let state_ptr = fd_state.state_ptr();
    let op_ctx = unsafe { completed_op_ctx(poll_ctx_from_waker(cx).ok(), state_ptr) };
    if let Some(pctx) = op_ctx.matching_poll_ctx() {
        return Ok(pctx);
    }

    let payload = unsafe { op_ctx.take_retained_payload_unchecked::<T>(state_ptr) };
    let retired = fd_state.take_state_ptr();
    debug_assert_eq!(retired, state_ptr);
    unsafe { op_ctx.free_op_unchecked(state_ptr) };
    Err(payload)
}

#[inline(always)]
/// Extracting variant of [`retry_poll_ctx_or_rejected_payload`].
///
/// # Safety
///
/// The state and `extract` requirements match
/// [`take_retained_payload_with_and_free_state`].
unsafe fn retry_poll_ctx_or_rejected_payload_with<T: 'static, R>(
    cx: &mut Context<'_>,
    fd_state: &mut RuntimeFdOpState<'_>,
    extract: impl FnOnce(*mut T) -> R,
) -> Result<PollCtx, R> {
    let state_ptr = fd_state.state_ptr();
    let op_ctx = unsafe { completed_op_ctx(poll_ctx_from_waker(cx).ok(), state_ptr) };
    if let Some(pctx) = op_ctx.matching_poll_ctx() {
        return Ok(pctx);
    }

    let value = unsafe { op_ctx.take_retained_payload_with_unchecked::<T, R>(state_ptr, extract) };
    let retired = fd_state.take_state_ptr();
    debug_assert_eq!(retired, state_ptr);
    unsafe { op_ctx.free_op_unchecked(state_ptr) };
    Err(value)
}

#[inline(always)]
/// Frees a completed retry slot before the next sequential submission.
///
/// # Safety
///
/// `*state_ptr` must be a non-null operation owned by `pctx`'s reactor, and no
/// kernel submission may still reference that state or its retained payload.
unsafe fn free_retry_state(pctx: &PollCtx, fd_state: &mut RuntimeFdOpState<'_>) {
    let state_ptr = fd_state.take_state_ptr();
    unsafe { Reactor::free_op_unchecked(pctx.reactor(), state_ptr) };
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RetryCqeResult {
    KernelError(#[doc = "Positive errno decoded from a negative CQE result."] i32),
    /// A zero-byte completion, interpreted by the caller as EOF or WriteZero.
    Zero,
    Bytes(#[doc = "Positive byte count reported by the completed SQE."] usize),
}

#[inline(always)]
fn classify_retry_cqe_result(result: i32) -> RetryCqeResult {
    if result < 0 {
        return RetryCqeResult::KernelError(-result);
    }
    if result == 0 {
        return RetryCqeResult::Zero;
    }
    RetryCqeResult::Bytes(result as usize)
}

/// Completes a validated empty stream operation and returns its exact owner.
#[inline(always)]
fn complete_empty_stream_io<B>(cx: &Context<'_>, owner: B) -> Poll<(io::Result<usize>, B)> {
    Poll::Ready((validate_local_io_result(cx, Ok(0)), owner))
}

#[inline(always)]
/// Reports whether a retry operation is still pending and refreshes its waiter.
///
/// # Safety
///
/// A non-null `state_ptr` must identify a live operation owned by this future.
unsafe fn retry_state_is_in_flight(cx: &mut Context<'_>, state_ptr: *mut CompletionState) -> bool {
    if state_ptr.is_null() {
        return false;
    }

    let state = unsafe { &*state_ptr };
    if state.is_completed() {
        return false;
    }

    unsafe { refresh_op_waiter_from_waker(cx, state_ptr) };
    true
}

#[inline(always)]
/// Takes a retained payload and releases its completed operation state.
///
/// # Safety
///
/// `*state_ptr` must be a non-null completed operation owned by `pctx`'s
/// reactor, and its retained payload type must be `T`.
unsafe fn take_retained_payload_and_free_state<T: 'static>(
    pctx: &PollCtx,
    fd_state: &mut RuntimeFdOpState<'_>,
) -> T {
    let op = fd_state.state_ptr();
    debug_assert!(!op.is_null(), "missing retained operation state");
    let payload = unsafe { Reactor::take_retained_payload_unchecked::<T>(pctx.reactor(), op) };
    unsafe { free_retry_state(pctx, fd_state) };
    payload
}

#[inline(always)]
/// Extracts from a retained payload and releases its completed operation state.
///
/// # Safety
///
/// `*state_ptr` must be a non-null completed operation owned by `pctx`'s
/// reactor with payload type `T`. `extract` must account for every payload
/// field that requires destruction before the retained allocation is released.
unsafe fn take_retained_payload_with_and_free_state<T: 'static, R>(
    pctx: &PollCtx,
    fd_state: &mut RuntimeFdOpState<'_>,
    extract: impl FnOnce(*mut T) -> R,
) -> R {
    let op = fd_state.state_ptr();
    debug_assert!(!op.is_null(), "missing retained operation state");
    let value = unsafe {
        Reactor::take_retained_payload_with_unchecked::<T, R>(pctx.reactor(), op, extract)
    };
    unsafe { free_retry_state(pctx, fd_state) };
    value
}

#[inline(always)]
/// Resets a completed retry slot and registers the current waiter.
///
/// # Safety
///
/// `state` must be an exclusively accessible completed operation whose prior
/// CQE and waiter have been consumed. Any retained payload must remain attached
/// and valid for the next submission. `waiter` must point to a live task on its
/// executor owner thread.
unsafe fn reset_existing_retry_state(state: &mut CompletionState, waiter: *mut TaskHeader) {
    debug_assert!(!waiter.is_null(), "retry waiter was missing");
    state.reset_for_resubmit();
    unsafe { state.register_waiter(waiter) };
}

/// Resubmits only a completed `-EAGAIN` receive, retaining its owner and lease.
/// Other states and completion results are left to the caller. Context
/// rejection returns `NotConnected`; a failed submission returns its error.
/// Both failures return the retained owner without another receive.
///
/// # Safety
///
/// `fd_state` must own the operation with retained payload type `T`. `build`
/// must use the supplied descriptor and only retained kernel-visible storage.
/// `extract` must move or drop every owned payload field before reclamation.
#[inline(always)]
unsafe fn retry_completed_stream_read<T: 'static, R>(
    cx: &mut Context<'_>,
    fd_state: &mut RuntimeFdOpState<'_>,
    build: impl FnOnce(RawFd, &mut T, u64) -> squeue::Entry,
    extract: impl Fn(*mut T) -> R + Copy,
) -> Option<Poll<(io::Result<usize>, R)>> {
    let state_ptr = fd_state.state_ptr();
    if state_ptr.is_null()
        || !unsafe { (*state_ptr).is_completed() }
        || unsafe { (*state_ptr).result } != -libc::EAGAIN
    {
        return None;
    }

    let pctx =
        match unsafe { retry_poll_ctx_or_rejected_payload_with::<T, R>(cx, fd_state, extract) } {
            Ok(pctx) => pctx,
            Err(owner) => {
                return Some(Poll::Ready((
                    Err(io::Error::from(io::ErrorKind::NotConnected)),
                    owner,
                )));
            }
        };

    if let Err(error) = unsafe {
        submit_resubmitted_fd_sqe(&pctx, fd_state, |fd| {
            let payload = (*state_ptr).retained_payload_mut::<T>();
            let entry = build(fd, payload, state_ptr as u64);
            // The completed state stays reclaimable if a buffer callback
            // unwinds during SQE construction or header initialization.
            reset_existing_retry_state(&mut *state_ptr, pctx.owner_task());
            Ok(entry)
        })
    } {
        let owner =
            unsafe { take_retained_payload_with_and_free_state::<T, R>(&pctx, fd_state, extract) };
        return Some(Poll::Ready((Err(error), owner)));
    }
    Some(Poll::Pending)
}

use super::{WritevPieces, WritevProjection, invalid_input, invalid_input_kind, opt_ref, opt_take};

/// Returns an uninitialized inline `iovec` scratch array.
///
/// `assume_init` is sound here because an array of `MaybeUninit` is itself
/// initialized; each element remains uninitialized until written.
#[inline(always)]
fn uninit_iovecs<const N: usize>() -> [MaybeUninit<libc::iovec>; N] {
    unsafe { MaybeUninit::uninit().assume_init() }
}

#[cold]
#[inline(never)]
fn retained_iovec_oversize_error() -> io::Error {
    invalid_input(RETAINED_IOVEC_OVERSIZE_MESSAGE)
}

/// Reinterprets an `iovec` scratch slice as initialized `libc::iovec`s.
///
/// # Safety
///
/// Every entry in `iovecs` must already be initialized before it is read as a
/// `libc::iovec`.
#[inline(always)]
unsafe fn iovec_slice_mut_from_uninit(
    iovecs: &mut [MaybeUninit<libc::iovec>],
) -> &mut [libc::iovec] {
    unsafe { slice::from_raw_parts_mut(iovecs.as_mut_ptr() as *mut libc::iovec, iovecs.len()) }
}

/// Returns a pointer to the `iovec` scratch entry at `skip`.
///
/// The returned pointer is used as the base of a `readv`/`writev` submission
/// window.
///
/// # Safety
///
/// `skip` must be `<= iovecs.len()`, and entries `[skip, skip + count)` for the
/// later opcode count must be initialized `libc::iovec`s.
#[inline(always)]
unsafe fn iovec_slice_ptr(iovecs: &[MaybeUninit<libc::iovec>], skip: usize) -> *const libc::iovec {
    unsafe { iovecs.as_ptr().add(skip) as *const libc::iovec }
}

/// Borrows the initialized `iovec` scratch entry at `index`.
///
/// # Safety
///
/// `index` must be `< iovecs.len()` and that entry must be an initialized
/// `libc::iovec`.
#[inline(always)]
unsafe fn iovec_slice_ref(iovecs: &[MaybeUninit<libc::iovec>], index: usize) -> &libc::iovec {
    unsafe { &*(iovecs.as_ptr().add(index) as *const libc::iovec) }
}

mod write_buffer_chain_sealed {
    use super::*;

    pub trait Sealed<const N: usize>: Sized {
        /// Returns at most `N` active entries. For an unchanged chain,
        /// `fill_write_iovecs` must report the same bounded active count.
        fn write_iovec_count_and_len(&self) -> Option<(usize, usize)>;
        fn fill_write_iovecs(
            &self,
            dst: &mut [MaybeUninit<libc::iovec>],
        ) -> io::Result<(usize, usize)>;
    }
}

/// Sealed marker for owned buffer-chain types accepted by stream `writev`
/// operations.
///
/// FlowIO implements this for [`IoBuffVec`] and [`IoBuffReadOnlyVec`]. It is
/// public only so those types can satisfy the bound on public stream methods;
/// downstream crates cannot implement it.
#[doc(hidden)]
#[allow(private_bounds)]
pub trait WriteBufferChain<const N: usize>: write_buffer_chain_sealed::Sealed<N> + 'static {}

impl<T, const N: usize> WriteBufferChain<N> for T where
    T: write_buffer_chain_sealed::Sealed<N> + 'static
{
}

trait WriteBufferItem {
    fn write_ptr(&self) -> *const u8;

    fn write_len(&self) -> usize;
}

impl<T: IoBuffReadOnly> WriteBufferItem for T {
    #[inline(always)]
    fn write_ptr(&self) -> *const u8 {
        self.as_ptr()
    }

    #[inline(always)]
    fn write_len(&self) -> usize {
        self.len()
    }
}

#[inline(always)]
fn checked_write_iovec_count_and_len<'a, I, T>(iter: I) -> Option<(usize, usize)>
where
    I: IntoIterator<Item = &'a T>,
    T: WriteBufferItem + 'a,
{
    checked_iovec_count_and_length_sum(iter.into_iter().map(WriteBufferItem::write_len))
}

#[inline(always)]
fn fill_write_iovecs<'a, I, T>(
    iter: I,
    dst: &mut [MaybeUninit<libc::iovec>],
) -> io::Result<(usize, usize)>
where
    I: IntoIterator<Item = &'a T>,
    T: WriteBufferItem + 'a,
{
    let mut iov_count = 0;
    let mut total = 0usize;
    let mut active = iter.into_iter().filter_map(|buf| {
        let len = buf.write_len();
        (len != 0).then_some((buf, len))
    });

    for (iovec, (buf, len)) in dst.iter_mut().zip(active.by_ref()) {
        total = total.checked_add(len).ok_or_else(invalid_writev_shape)?;
        iovec.write(libc::iovec {
            iov_base: buf.write_ptr() as *mut libc::c_void,
            iov_len: len,
        });
        iov_count += 1;
    }

    for (_, len) in active {
        total = total.checked_add(len).ok_or_else(invalid_writev_shape)?;
        iov_count += 1;
    }

    Ok((iov_count, total))
}

#[inline(always)]
pub(super) fn fill_iobuffvec_write_iovecs<const N: usize>(
    buffer: &IoBuffVec<N>,
    dst: &mut [MaybeUninit<libc::iovec>],
) -> io::Result<(usize, usize)> {
    fill_write_iovecs(buffer.iter(), dst)
}

#[inline(always)]
pub(super) fn checked_iobuffvec_write_iovec_count_and_len<const N: usize>(
    buffer: &IoBuffVec<N>,
) -> Option<(usize, usize)> {
    checked_write_iovec_count_and_len(buffer.iter())
}

impl<const N: usize> write_buffer_chain_sealed::Sealed<N> for IoBuffVec<N> {
    #[inline(always)]
    fn write_iovec_count_and_len(&self) -> Option<(usize, usize)> {
        checked_iobuffvec_write_iovec_count_and_len(self)
    }

    #[inline(always)]
    fn fill_write_iovecs(
        &self,
        dst: &mut [MaybeUninit<libc::iovec>],
    ) -> io::Result<(usize, usize)> {
        fill_iobuffvec_write_iovecs(self, dst)
    }
}

impl<B: IoBuffReadOnly, const N: usize> write_buffer_chain_sealed::Sealed<N>
    for IoBuffReadOnlyVec<B, N>
{
    #[inline(always)]
    fn write_iovec_count_and_len(&self) -> Option<(usize, usize)> {
        checked_write_iovec_count_and_len(self.iter())
    }

    #[inline(always)]
    fn fill_write_iovecs(
        &self,
        dst: &mut [MaybeUninit<libc::iovec>],
    ) -> io::Result<(usize, usize)> {
        fill_write_iovecs(self.iter(), dst)
    }
}

const WRITEV_SHAPE_CHANGED: &str = "write buffer chain shape changed before submission";
const WRITEV_AGGREGATE_OVERFLOW: &str = "write buffer chain byte length exceeds usize::MAX";
const READV_AGGREGATE_OVERFLOW: &str = "read buffer chain writable length exceeds usize::MAX";

#[inline(always)]
pub(super) fn invalid_writev_shape() -> io::Error {
    invalid_input(WRITEV_SHAPE_CHANGED)
}

#[inline(always)]
fn invalid_writev_aggregate() -> io::Error {
    invalid_input(WRITEV_AGGREGATE_OVERFLOW)
}

#[inline(always)]
pub(super) fn invalid_readv_aggregate() -> io::Error {
    invalid_input(READV_AGGREGATE_OVERFLOW)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
enum DeferredReadvError {
    None = 0,
    AggregateOverflow = 1,
    ActiveIovecOverflow = 2,
}

#[cold]
#[inline(never)]
fn materialize_deferred_readv_error(error: &DeferredReadvError) -> io::Error {
    match error {
        DeferredReadvError::None | DeferredReadvError::AggregateOverflow => {
            invalid_readv_aggregate()
        }
        DeferredReadvError::ActiveIovecOverflow => retained_iovec_oversize_error(),
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
    /// Caller-owned destination chain retained across every active submission.
    buffer: IoBuffVecMut<N>,
    /// Kernel-facing `iovec` array pointing into `buffer` segments.
    scratch: RetainedIovecScratch,
    /// Kernel-facing header, initialized in retained storage for each RECVMSG.
    /// Single-iovec RECV submissions never read this field.
    msg: MaybeUninit<libc::msghdr>,
}

struct RetainedWritevPayload<C> {
    /// Caller-owned source chain retained across every active submission.
    buffer: C,
    /// Kernel-facing `iovec` array pointing into `buffer` segments.
    scratch: RetainedIovecScratch,
    /// Kernel-facing sendmsg header, initialized only when an SQE uses SENDMSG.
    /// Every SEND submission leaves this field unread.
    msg: MaybeUninit<libc::msghdr>,
    /// Bytes confirmed by completed SQEs for `_all` retry futures.
    written: usize,
    /// First active `iovec` entry after partial write progress.
    skip: usize,
}

struct RetainedWritevCompletion<C> {
    /// Caller-owned source chain returned to the completed future.
    buffer: C,
    /// Bytes confirmed by completed SQEs for `_all` retry futures.
    written: usize,
}

struct RetainedProjectedWritevPayload<T> {
    /// Compact caller-owned source retained across every active submission.
    source: T,
    /// Kernel-facing `iovec` array pointing into `source` projections.
    scratch: RetainedIovecScratch,
    /// Kernel-facing sendmsg header, initialized only when an SQE uses SENDMSG.
    /// Every SEND submission leaves this field unread.
    msg: MaybeUninit<libc::msghdr>,
    /// Bytes confirmed by completed SQEs for projected `_all` retries.
    written: usize,
    /// First active projected `iovec` entry after partial write progress.
    skip: usize,
}

/// Constructs a readv payload directly in its final retained allocation.
///
/// The concrete internal buffer chain is transferred only after every
/// potentially unwinding preparation step has completed. Its iovec entries
/// are materialized later, after the initialized payload is attached.
///
/// # Safety
///
/// `pool` must identify the live retained pool for the active owner-thread
/// reactor. `buffer` must contain one value, `scratch_init` must have the
/// matching active iovec count, and the returned payload must be attached to a
/// state owned by that same reactor or consumed through `pool`.
#[inline(always)]
unsafe fn emplace_retained_readv_payload<const N: usize>(
    pool: NonNull<RetainedPayloadPool>,
    buffer: &mut Option<IoBuffVecMut<N>>,
    scratch_init: RetainedIovecScratchInit,
) -> RetainedPayload<RetainedReadvPayload<N>> {
    unsafe {
        with_raw_retained_slot::<RetainedReadvPayload<N>, _>(pool, |slot| {
            let source = buffer.as_mut().unwrap_unchecked() as *mut IoBuffVecMut<N>;
            let mut writing = slot.begin_writing();
            let dst = writing.as_mut_ptr();
            scratch_init.initialize_at(std::ptr::addr_of_mut!((*dst).scratch));
            std::ptr::addr_of_mut!((*dst).msg).write(MaybeUninit::uninit());
            std::ptr::copy_nonoverlapping(source, std::ptr::addr_of_mut!((*dst).buffer), 1);
            std::ptr::write(buffer, None);
            writing.finish()
        })
    }
}

/// Constructs either direct-writev payload in final retained storage.
///
/// Iovec materialization targets the final inline array or the token's pooled
/// sidecar while the retained slot is still ownership-free. `after_fill`
/// then preserves each call site's required completion-state ordering: the
/// one-shot path allocates after materialization, while write-all supplies a
/// guard allocated before this constructor is entered.
///
/// # Safety
///
/// `pool` must identify the live retained pool for the active owner-thread
/// reactor. `buffer` must contain one `C`; `scratch_init.len()` and
/// `expected_total` must be the paired first-pass shape snapshot. A later
/// materialization mismatch is handled as `InvalidInput`. Any ownership
/// returned by `after_fill` must remain valid until the caller consumes it.
/// `fill_write_iovecs` records each nonempty source item's base pointer before
/// `C` moves into retained storage. Those pointers remain valid only because
/// every supported source item satisfies [`IoBuffReadOnly`]'s requirement that
/// its backing range is not invalidated by moving the item or its containing
/// chain; every in-crate sealed chain implementation must preserve that
/// invariant. The returned payload must be attached to the matching operation
/// state or consumed through `pool`.
#[inline(always)]
unsafe fn emplace_retained_writev_payload<C: WriteBufferChain<N>, const N: usize, R>(
    pool: NonNull<RetainedPayloadPool>,
    buffer: &mut Option<C>,
    mut scratch_init: RetainedIovecScratchInit,
    expected_total: usize,
    after_fill: impl FnOnce() -> io::Result<R>,
) -> io::Result<(RetainedPayload<RetainedWritevPayload<C>>, R)> {
    unsafe {
        with_raw_retained_slot::<RetainedWritevPayload<C>, _>(pool, |mut slot| {
            let dst = slot.as_mut_ptr();
            std::ptr::addr_of_mut!((*dst).written).write(0);
            std::ptr::addr_of_mut!((*dst).skip).write(0);

            {
                let scratch = scratch_init.destination_mut(std::ptr::addr_of_mut!((*dst).scratch));
                let expected_shape = (scratch.len(), expected_total);
                let actual_shape = opt_ref(buffer).fill_write_iovecs(scratch)?;
                if actual_shape != expected_shape {
                    return Err(invalid_writev_shape());
                }
            }
            let after_fill = after_fill()?;
            let source = buffer.as_mut().unwrap_unchecked() as *mut C;

            let mut writing = slot.begin_writing();
            let dst = writing.as_mut_ptr();
            scratch_init.initialize_at(std::ptr::addr_of_mut!((*dst).scratch));
            std::ptr::copy_nonoverlapping(source, std::ptr::addr_of_mut!((*dst).buffer), 1);
            std::ptr::write(buffer, None);
            Ok((writing.finish(), after_fill))
        })
    }
}

/// Constructs a projected-writev payload directly in retained storage.
///
/// Projection runs after payload attachment so its fallible callback observes
/// the final stable source and scratch addresses.
///
/// # Safety
///
/// `pool` must identify the live retained pool for the active owner-thread
/// reactor. `source` must contain one value, `scratch_init` must have the
/// declared projection count, and the returned payload must be attached to a
/// state owned by that same reactor or consumed through `pool`.
#[inline(always)]
unsafe fn emplace_retained_projected_writev_payload<T: 'static>(
    pool: NonNull<RetainedPayloadPool>,
    source: &mut Option<T>,
    scratch_init: RetainedIovecScratchInit,
) -> RetainedPayload<RetainedProjectedWritevPayload<T>> {
    unsafe {
        with_raw_retained_slot::<RetainedProjectedWritevPayload<T>, _>(pool, |mut slot| {
            let dst = slot.as_mut_ptr();
            std::ptr::addr_of_mut!((*dst).written).write(0);
            std::ptr::addr_of_mut!((*dst).skip).write(0);

            let source_ptr = source.as_mut().unwrap_unchecked() as *mut T;
            let mut writing = slot.begin_writing();
            let dst = writing.as_mut_ptr();
            scratch_init.initialize_at(std::ptr::addr_of_mut!((*dst).scratch));
            std::ptr::copy_nonoverlapping(source_ptr, std::ptr::addr_of_mut!((*dst).source), 1);
            std::ptr::write(source, None);
            writing.finish()
        })
    }
}

#[inline(always)]
/// Moves a readv buffer out of retained payload storage and drops its scratch.
///
/// # Safety
///
/// `payload` must point to a live, uniquely owned `RetainedReadvPayload<N>`.
/// The caller must ensure the retained allocation will be released without
/// subsequently dropping the moved `buffer` or the already-dropped `scratch`.
unsafe fn take_readv_buffer_from_retained<const N: usize>(
    payload: *mut RetainedReadvPayload<N>,
) -> IoBuffVecMut<N> {
    let buffer = unsafe { std::ptr::read(std::ptr::addr_of!((*payload).buffer)) };
    unsafe { std::ptr::drop_in_place(std::ptr::addr_of_mut!((*payload).scratch)) };
    buffer
}

#[inline(always)]
/// Moves writev completion data out of retained storage and drops its scratch.
///
/// # Safety
///
/// `payload` must point to a live, uniquely owned `RetainedWritevPayload<C>`.
/// The caller must ensure the retained allocation will not later drop the
/// moved `buffer` or the already-dropped `scratch`.
unsafe fn take_writev_completion_from_retained<C>(
    payload: *mut RetainedWritevPayload<C>,
) -> RetainedWritevCompletion<C> {
    let buffer = unsafe { std::ptr::read(std::ptr::addr_of!((*payload).buffer)) };
    let written = unsafe { std::ptr::read(std::ptr::addr_of!((*payload).written)) };
    unsafe { std::ptr::drop_in_place(std::ptr::addr_of_mut!((*payload).scratch)) };
    RetainedWritevCompletion { buffer, written }
}

#[inline(always)]
/// Builds the next read-style SQE from the current vectored scratch window.
///
/// One active segment within the signed byte-count limit uses `IORING_OP_RECV`.
/// Larger single segments and multiple segments use `IORING_OP_RECVMSG`.
fn build_read_vectored_entry(
    fd: RawFd,
    iovecs: &[MaybeUninit<libc::iovec>],
    skip: usize,
    count: usize,
    msg: &mut MaybeUninit<libc::msghdr>,
    user_data: u64,
) -> squeue::Entry {
    debug_assert!(count > 0, "readv submission requires at least one iovec");

    if count == 1 {
        let iov = unsafe { iovec_slice_ref(iovecs, skip) };
        if iov.iov_len <= i32::MAX as usize {
            return build_recv_entry(fd, iov.iov_base as *mut u8, iov.iov_len as u32, user_data);
        }
    }

    let msg = prepare_message_header(msg, iovecs, skip, count);
    build_recvmsg_entry(fd, msg, user_data)
}

#[inline(always)]
fn prepare_message_header(
    msg: &mut MaybeUninit<libc::msghdr>,
    iovecs: &[MaybeUninit<libc::iovec>],
    skip: usize,
    count: usize,
) -> *mut libc::msghdr {
    write_msghdr(
        msg,
        MsgHdrInit {
            name: std::ptr::null_mut(),
            namelen: 0,
            iov: unsafe { iovec_slice_ptr(iovecs, skip) as *mut libc::iovec },
            iovlen: count,
            control: std::ptr::null_mut(),
            controllen: 0,
        },
    );
    msg.as_mut_ptr()
}

#[inline(always)]
/// Builds the next write-style SQE from the current vectored scratch window.
///
/// One remaining segment whose length fits in `u32` uses `IORING_OP_SEND`,
/// submitting at most `i32::MAX` bytes. Larger or multiple segments use SENDMSG.
fn build_write_vectored_entry(
    fd: RawFd,
    iovecs: &[MaybeUninit<libc::iovec>],
    skip: usize,
    count: usize,
    msg: &mut MaybeUninit<libc::msghdr>,
    user_data: u64,
) -> squeue::Entry {
    debug_assert!(count > 0, "writev submission requires at least one iovec");

    if !write_uses_message(iovecs, skip, count) {
        return build_write_scalar_entry(fd, iovecs, skip, user_data);
    }

    let msg = prepare_message_header(msg, iovecs, skip, count);
    build_sendmsg_entry(fd, msg, SEND_FLAGS as u32, user_data)
}

#[derive(Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
enum WritevSendMode {
    Vectored,
    Scalar,
}

/// Selects SENDMSG for multiple remaining segments or a segment larger than u32::MAX.
#[inline(always)]
fn write_uses_message(iovecs: &[MaybeUninit<libc::iovec>], skip: usize, count: usize) -> bool {
    count > 1 || unsafe { iovec_slice_ref(iovecs, skip) }.iov_len > u32::MAX as usize
}

/// Switches once to scalar sends after a completed SENDMSG returns EAGAIN.
#[inline(always)]
fn switch_writev_to_scalar(
    mode: &mut WritevSendMode,
    result: i32,
    iovecs: &[MaybeUninit<libc::iovec>],
    skip: usize,
    count: usize,
) -> bool {
    if result == -libc::EAGAIN
        && *mode == WritevSendMode::Vectored
        && write_uses_message(iovecs, skip, count)
    {
        *mode = WritevSendMode::Scalar;
        return true;
    }
    false
}

/// Builds a SEND for the current segment without accessing the message header.
#[inline(always)]
fn build_write_scalar_entry(
    fd: RawFd,
    iovecs: &[MaybeUninit<libc::iovec>],
    skip: usize,
    user_data: u64,
) -> squeue::Entry {
    debug_assert!(skip < iovecs.len());
    let iov = unsafe { iovec_slice_ref(iovecs, skip) };
    build_send_entry(
        fd,
        iov.iov_base as *const u8,
        iov.iov_len.min(u32::MAX as usize) as u32,
        SEND_FLAGS,
        user_data,
    )
}

/// Builds a retry SQE using the call's current vectored or scalar send mode.
#[inline(always)]
fn build_write_retry_entry(
    mode: WritevSendMode,
    fd: RawFd,
    scratch: &RetainedIovecScratch,
    skip: usize,
    msg: &mut MaybeUninit<libc::msghdr>,
    user_data: u64,
) -> squeue::Entry {
    let iovecs = retained_iovecs(scratch);
    match mode {
        WritevSendMode::Scalar => build_write_scalar_entry(fd, iovecs, skip, user_data),
        WritevSendMode::Vectored => {
            let count = remaining_iovec_count(scratch, skip);
            build_write_vectored_entry(fd, iovecs, skip, count, msg, user_data)
        }
    }
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

/// Truncates the submitted readv window so it exposes at most `max_bytes`
/// bytes starting at `skip`. Returns the number of iovecs to submit from
/// `skip`.
#[inline]
fn clamp_iovecs_to_read_limit(iovecs: &mut [libc::iovec], skip: usize, max_bytes: usize) -> usize {
    debug_assert!(max_bytes > 0, "readv_exact should not submit zero bytes");
    debug_assert!(skip <= iovecs.len(), "readv_exact skip beyond scratch");

    let mut remaining = max_bytes;
    for (index, iov) in iovecs.iter_mut().enumerate().skip(skip) {
        let len = iov.iov_len;
        if len >= remaining {
            iov.iov_len = remaining;
            return index + 1 - skip;
        }
        remaining -= len;
    }

    debug_assert!(
        remaining == 0,
        "readv_exact clamp exceeded materialized writable iovecs"
    );

    iovecs.len() - skip
}

#[inline(always)]
fn retained_iovecs(scratch: &RetainedIovecScratch) -> &[MaybeUninit<libc::iovec>] {
    scratch.as_uninit_slice()
}

/// Borrows the proven-initialized prefix of retained iovec scratch.
///
/// # Safety
///
/// `initialized_count` must not exceed `scratch.len()`, and every entry before
/// that count must contain an initialized `libc::iovec`.
#[inline(always)]
unsafe fn retained_iovecs_mut(
    scratch: &mut RetainedIovecScratch,
    initialized_count: usize,
) -> &mut [libc::iovec] {
    debug_assert!(
        initialized_count <= scratch.len(),
        "initialized iovec count exceeds retained scratch"
    );
    let initialized = unsafe {
        slice::from_raw_parts_mut(
            scratch.as_uninit_slice_mut().as_mut_ptr(),
            initialized_count,
        )
    };
    unsafe { iovec_slice_mut_from_uninit(initialized) }
}

#[inline(always)]
fn remaining_iovec_count(scratch: &RetainedIovecScratch, skip: usize) -> usize {
    scratch.len() - skip
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ProjectedWritevShapeError {
    ReportedBytesWithoutPieces,
    ReportedPiecesWithoutBytes,
    PieceCountMismatch,
    ByteLengthMismatch,
}

impl ProjectedWritevShapeError {
    #[inline(always)]
    fn message(self) -> &'static str {
        match self {
            Self::ReportedBytesWithoutPieces => {
                "projected writev reported bytes but no active pieces"
            }
            Self::ReportedPiecesWithoutBytes => {
                "projected writev reported active pieces but no bytes"
            }
            Self::PieceCountMismatch => "projected writev piece count did not match counted pieces",
            Self::ByteLengthMismatch => "projected writev byte length did not match counted length",
        }
    }
}

#[inline(always)]
fn projected_report_shape_error(
    iov_count: usize,
    total: usize,
) -> Option<ProjectedWritevShapeError> {
    if iov_count == 0 && total != 0 {
        return Some(ProjectedWritevShapeError::ReportedBytesWithoutPieces);
    }
    if iov_count != 0 && total == 0 {
        return Some(ProjectedWritevShapeError::ReportedPiecesWithoutBytes);
    }
    None
}

#[inline(always)]
fn projected_materialized_shape_error(
    projected_count: usize,
    projected_total: usize,
    expected_count: usize,
    expected_total: usize,
) -> Option<ProjectedWritevShapeError> {
    if projected_count != expected_count {
        return Some(ProjectedWritevShapeError::PieceCountMismatch);
    }
    if projected_total != expected_total {
        return Some(ProjectedWritevShapeError::ByteLengthMismatch);
    }
    None
}

#[inline(always)]
fn validate_projected_count_and_len(iov_count: usize, total: usize) -> io::Result<()> {
    if let Some(error) = projected_report_shape_error(iov_count, total) {
        return Err(invalid_input(error.message()));
    }
    Ok(())
}

#[inline(always)]
fn validate_try_projected_count_and_len(iov_count: usize, total: usize) -> io::Result<()> {
    if projected_report_shape_error(iov_count, total).is_some() {
        return Err(invalid_input_kind());
    }
    Ok(())
}

#[inline(always)]
fn validate_projected_materialized_shape(
    projected_count: usize,
    projected_total: usize,
    expected_count: usize,
    expected_total: usize,
) -> io::Result<()> {
    if let Some(error) = projected_materialized_shape_error(
        projected_count,
        projected_total,
        expected_count,
        expected_total,
    ) {
        return Err(invalid_input(error.message()));
    }
    Ok(())
}

#[inline(always)]
fn validate_try_projected_materialized_shape(
    projected_count: usize,
    projected_total: usize,
    expected_count: usize,
    expected_total: usize,
) -> io::Result<()> {
    if projected_materialized_shape_error(
        projected_count,
        projected_total,
        expected_count,
        expected_total,
    )
    .is_some()
    {
        return Err(invalid_input_kind());
    }
    Ok(())
}

/// Validates a projection that declared no active pieces and no bytes.
#[inline]
fn validate_empty_projected_writev<T: WritevProjection>(source: &T) -> io::Result<()> {
    let mut scratch: [MaybeUninit<libc::iovec>; 0] = [];
    let mut pieces = WritevPieces::new(&mut scratch);
    source.project_writev(&mut pieces)?;
    if pieces.count() != 0 || pieces.total() != 0 {
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

    validate_projected_materialized_shape(
        projected_count,
        projected_total,
        expected_count,
        expected_total,
    )
}

#[inline(always)]
fn one_shot_syscall_result(result: libc::ssize_t) -> io::Result<usize> {
    if result < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(result as usize)
}

/// Attempts one nonblocking read syscall into the caller-owned buffer.
#[inline]
pub(crate) fn try_read_once<B: IoBuffReadWrite>(
    fd: RawFd,
    mut buffer: B,
    len: usize,
) -> (io::Result<usize>, B) {
    let len = match super::checked_read_len(len, buffer.writable_len()) {
        Ok(len) => len as usize,
        Err(err) => return (Err(err), buffer),
    };
    let write_base_len = buffer.write_base_len();

    if len == 0 {
        return (Ok(0), buffer);
    }

    let result = unsafe { libc::recv(fd, buffer.as_mut_ptr() as *mut libc::c_void, len, 0) };
    match one_shot_syscall_result(result) {
        Ok(actual) => unsafe {
            complete_read_with_progress(buffer, write_base_len, actual, Ok(actual))
        },
        Err(err) => (Err(err), buffer),
    }
}

/// Attempts one nonblocking write syscall from the caller-owned buffer.
#[inline]
pub(crate) fn try_write_once<B: IoBuffReadOnly>(fd: RawFd, buffer: B) -> (io::Result<usize>, B) {
    let len = match super::send_len_u32(buffer.len()) {
        Some(len) => len as usize,
        None => return (Err(invalid_input_kind()), buffer),
    };

    if len == 0 {
        return (Ok(0), buffer);
    }

    let result = unsafe { libc::send(fd, buffer.as_ptr() as *const libc::c_void, len, SEND_FLAGS) };
    (one_shot_syscall_result(result), buffer)
}

const TRY_WRITEV_INLINE_IOVECS: usize = RETAINED_IOVEC_INLINE_COUNT;
const TRY_WRITEV_MAX_IOVECS: usize = RETAINED_IOVEC_MAX_COUNT;

thread_local! {
    static TRY_WRITEV_PROJECTED_SCRATCH: RefCell<Vec<MaybeUninit<libc::iovec>>> =
        const { RefCell::new(Vec::new()) };
}

#[inline]
fn try_writev_projected_with_scratch<T: WritevProjection>(
    fd: RawFd,
    source: &T,
    expected_count: usize,
    expected_total: usize,
    scratch: &mut [MaybeUninit<libc::iovec>],
) -> io::Result<usize> {
    let projection = {
        let mut pieces = WritevPieces::new(&mut scratch[..expected_count]);
        source
            .project_writev(&mut pieces)
            .map(|()| (pieces.count(), pieces.total()))
    };

    let (projected_count, projected_total) = projection?;

    validate_try_projected_materialized_shape(
        projected_count,
        projected_total,
        expected_count,
        expected_total,
    )?;

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

    let result = unsafe { libc::sendmsg(fd, &msg, SEND_FLAGS) };
    one_shot_syscall_result(result)
}

#[inline]
fn try_writev_projected_with_dynamic_scratch<T: WritevProjection>(
    fd: RawFd,
    source: &T,
    expected_count: usize,
    expected_total: usize,
) -> io::Result<usize> {
    let tls_result = TRY_WRITEV_PROJECTED_SCRATCH.try_with(|cell| {
        cell.try_borrow_mut().ok().map(|mut scratch| {
            try_writev_projected_with_vec_scratch(
                fd,
                source,
                expected_count,
                expected_total,
                &mut scratch,
            )
        })
    });

    match tls_result {
        Ok(Some(result)) => result,
        Ok(None) | Err(_) => {
            try_writev_projected_with_fresh_scratch(fd, source, expected_count, expected_total)
        }
    }
}

#[inline]
fn try_writev_projected_with_fresh_scratch<T: WritevProjection>(
    fd: RawFd,
    source: &T,
    expected_count: usize,
    expected_total: usize,
) -> io::Result<usize> {
    let mut scratch = Vec::new();
    try_writev_projected_with_vec_scratch(fd, source, expected_count, expected_total, &mut scratch)
}

#[inline]
fn reserve_projected_scratch_capacity(
    scratch: &mut Vec<MaybeUninit<libc::iovec>>,
    expected_count: usize,
) -> io::Result<()> {
    if scratch.capacity() < expected_count {
        let additional = expected_count - scratch.len();
        scratch
            .try_reserve_exact(additional)
            .map_err(|_| io::Error::from(io::ErrorKind::WouldBlock))?;
    }
    Ok(())
}

#[inline]
fn try_writev_projected_with_vec_scratch<T: WritevProjection>(
    fd: RawFd,
    source: &T,
    expected_count: usize,
    expected_total: usize,
    scratch: &mut Vec<MaybeUninit<libc::iovec>>,
) -> io::Result<usize> {
    reserve_projected_scratch_capacity(scratch, expected_count)?;

    let capacity = scratch.capacity();
    if scratch.len() < capacity {
        // Length represents reusable `MaybeUninit` slots; each projection
        // still receives only the prefix it reported it will initialize.
        scratch.resize_with(capacity, MaybeUninit::uninit);
    }

    try_writev_projected_with_scratch(
        fd,
        source,
        expected_count,
        expected_total,
        &mut scratch[..expected_count],
    )
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
        let result = validate_empty_projected_writev(&source).map(|()| 0);
        return (result, source);
    }
    if iov_count > TRY_WRITEV_MAX_IOVECS {
        return (Err(invalid_input_kind()), source);
    }

    if iov_count <= TRY_WRITEV_INLINE_IOVECS {
        let mut scratch = uninit_iovecs::<TRY_WRITEV_INLINE_IOVECS>();
        let result = try_writev_projected_with_scratch(fd, &source, iov_count, total, &mut scratch);
        return (result, source);
    }

    let result = try_writev_projected_with_dynamic_scratch(fd, &source, iov_count, total);
    (result, source)
}

#[inline]
fn submit_initial_projected_writev<T: WritevProjection>(
    pctx: &PollCtx,
    fd_state: &mut RuntimeFdOpState<'_>,
    source: &mut Option<T>,
    iov_count: usize,
    total: usize,
) -> Result<(), (io::Error, T)> {
    let retained_pool = unsafe { Reactor::retained_payload_pool_ptr(pctx.reactor()) };
    let scratch_init =
        match unsafe { (*retained_pool.as_ptr()).alloc_iovec_scratch_init(iov_count) } {
            Ok(scratch_init) => scratch_init,
            Err(err) => {
                let source = unsafe { opt_take(source) };
                return Err((err, source));
            }
        };

    let guard = match unsafe { prepare_unsubmitted_op(pctx) } {
        Some(guard) => guard,
        None => {
            let source = unsafe { opt_take(source) };
            return Err((io::Error::from(io::ErrorKind::WouldBlock), source));
        }
    };
    let state_ptr = guard.state_ptr();

    let payload =
        unsafe { emplace_retained_projected_writev_payload(retained_pool, source, scratch_init) };

    unsafe {
        if let Err((err, payload)) =
            submit_initialized_retained_fd_sqe(pctx, state_ptr, fd_state, payload, |fd, payload| {
                project_retained_writev_payload(payload, iov_count, total)?;
                Ok(build_write_vectored_entry(
                    fd,
                    retained_iovecs(&payload.scratch),
                    0,
                    payload.scratch.len(),
                    &mut payload.msg,
                    state_ptr as u64,
                ))
            })
        {
            return Err((err, payload.source));
        }
    }

    guard.disarm();
    Ok(())
}

// ---------------------------------------------------------------------------
// ReadFuture
// ---------------------------------------------------------------------------

/// Reads into a caller-provided buffer and returns the first positive or
/// terminal completion (rental pattern). A no-progress EAGAIN completion is
/// resubmitted internally.
///
/// A zero-length request completes locally with `Ok(0)` and returns the
/// buffer unchanged. That request-scoped result is not an EOF observation.
pub struct ReadFuture<'a, B: IoBuffReadWrite, S> {
    /// Completion state for the submitted read SQE, if any.
    state_ptr: RuntimeFdOpState<'a>,
    /// Caller-owned destination buffer returned on completion.
    buffer: Option<B>,
    /// Descriptor captured for debug-build checks against `state_ptr`.
    /// Release builds store `RuntimeFd::INVALID`; SQE construction derives the
    /// live descriptor from `state_ptr`.
    fd: RawFd,
    /// Validated caller request, clamped to i32::MAX at submission.
    len: u32,
    /// Logical length immediately before the submitted writable region.
    write_base_len: usize,
    /// Deferred validation error returned before any SQE submission.
    input_error: Option<io::Error>,
    /// Borrows the parent stream for the future lifetime.
    _marker: PhantomData<&'a mut S>,
}

impl<'a, B: IoBuffReadWrite, S> ReadFuture<'a, B, S> {
    pub(crate) fn new(fd_state: RuntimeFdOpState<'a>, buffer: B, len: usize) -> Self {
        let fd = fd_state.debug_raw_fd();
        let write_base_len = buffer.write_base_len();
        let mut input_error = None;
        let len = match super::checked_read_len(len, buffer.writable_len()) {
            Ok(len) => len,
            Err(err) => {
                input_error = Some(err);
                0
            }
        };
        Self {
            state_ptr: fd_state,
            buffer: Some(buffer),
            fd,
            len,
            write_base_len,
            input_error,
            _marker: PhantomData,
        }
    }
}

impl<B: IoBuffReadWrite, S> Future for ReadFuture<'_, B, S> {
    type Output = (io::Result<usize>, B);

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = unsafe { self.get_unchecked_mut() };
        this.state_ptr
            .debug_assert_matches_raw_fd(this.fd, FdStateDiagnostic::Stream);

        if this.state_ptr.state_ptr().is_null()
            && let Some(err) = this.input_error.take()
        {
            let result = validate_local_io_result(cx, Err(err));
            let buffer = unsafe { opt_take(&mut this.buffer) };
            return Poll::Ready((result, buffer));
        }
        if this.state_ptr.state_ptr().is_null() && this.buffer.is_none() {
            return Poll::Pending;
        }
        if this.state_ptr.state_ptr().is_null() && this.len == 0 {
            let buffer = unsafe { opt_take(&mut this.buffer) };
            return complete_empty_stream_io(cx, buffer);
        }

        if let Some(retry) = unsafe {
            retry_completed_stream_read::<RetainedReadPayload<B>, _>(
                cx,
                &mut this.state_ptr,
                |fd, payload, user_data| {
                    build_recv_entry(fd, payload.buffer.as_mut_ptr(), this.len, user_data)
                },
                |payload| std::ptr::read(std::ptr::addr_of!((*payload).buffer)),
            )
        } {
            return retry;
        }

        if let Some(completion) = unsafe {
            take_completed_result_and_payload::<RetainedReadPayload<B>>(cx, &mut this.state_ptr)
        } {
            let (result, payload) = completion.into_io_result(completion_cqe_result);
            let buffer = payload.buffer;
            let actual = match result {
                Ok(actual) => actual,
                Err(err) => return Poll::Ready((Err(err), buffer)),
            };
            return Poll::Ready(unsafe {
                complete_read_with_progress(buffer, this.write_base_len, actual, Ok(actual))
            });
        }

        if this.state_ptr.state_ptr().is_null() {
            let pctx = match poll_ctx_from_waker(cx) {
                Ok(pctx) => pctx,
                Err(err) => {
                    let buffer = unsafe { opt_take(&mut this.buffer) };
                    return Poll::Ready((Err(err), buffer));
                }
            };
            let guard = match unsafe { prepare_unsubmitted_op(&pctx) } {
                Some(guard) => guard,
                None => {
                    let buffer = unsafe { opt_take(&mut this.buffer) };
                    return Poll::Ready((Err(io::Error::from(io::ErrorKind::WouldBlock)), buffer));
                }
            };
            let state_ptr = guard.state_ptr();

            let payload = RetainedReadPayload {
                buffer: unsafe { opt_take(&mut this.buffer) },
            };
            unsafe {
                if let Err((e, payload)) = submit_retained_fd_sqe(
                    &pctx,
                    state_ptr,
                    &mut this.state_ptr,
                    payload,
                    |fd, payload| {
                        let ptr = payload.buffer.as_mut_ptr();
                        Ok(build_recv_entry(fd, ptr, this.len, state_ptr as u64))
                    },
                ) {
                    return Poll::Ready((Err(e), payload.buffer));
                }
            }
            guard.disarm();
            return Poll::Pending;
        }

        unsafe { refresh_op_waiter_from_waker(cx, this.state_ptr.state_ptr()) };
        Poll::Pending
    }
}

impl<B: IoBuffReadWrite, S> Drop for ReadFuture<'_, B, S> {
    fn drop(&mut self) {
        unsafe { drop_fd_op_state_unchecked(&mut self.state_ptr) };
    }
}

// ---------------------------------------------------------------------------
// WriteFuture
// ---------------------------------------------------------------------------

/// Single write from a caller-provided buffer (rental pattern).
///
/// An empty readable window completes locally with `Ok(0)` and returns the
/// buffer unchanged.
pub struct WriteFuture<'a, B: IoBuffReadOnly, S> {
    /// Completion state for the submitted write SQE, if any.
    state_ptr: RuntimeFdOpState<'a>,
    /// Caller-owned source buffer returned on completion.
    buffer: Option<B>,
    /// Descriptor captured for debug-build checks against `state_ptr`.
    /// Release builds store `RuntimeFd::INVALID`; SQE construction derives the
    /// live descriptor from `state_ptr`.
    fd: RawFd,
    /// Validated request length, clamped to `i32::MAX` on submission.
    len: u32,
    /// Deferred validation error returned before any SQE submission.
    input_error: Option<io::Error>,
    /// Borrows the parent stream for the future lifetime.
    _marker: PhantomData<&'a mut S>,
}

impl<'a, B: IoBuffReadOnly, S> WriteFuture<'a, B, S> {
    pub(crate) fn new(fd_state: RuntimeFdOpState<'a>, buffer: B) -> Self {
        let fd = fd_state.debug_raw_fd();
        let mut input_error = None;
        let len = match super::checked_send_len(buffer.len()) {
            Ok(len) => len,
            Err(err) => {
                input_error = Some(err);
                0
            }
        };
        Self {
            state_ptr: fd_state,
            buffer: Some(buffer),
            fd,
            len,
            input_error,
            _marker: PhantomData,
        }
    }
}

impl<B: IoBuffReadOnly, S> Future for WriteFuture<'_, B, S> {
    type Output = (io::Result<usize>, B);

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = unsafe { self.get_unchecked_mut() };
        this.state_ptr
            .debug_assert_matches_raw_fd(this.fd, FdStateDiagnostic::Stream);

        if this.state_ptr.state_ptr().is_null()
            && let Some(err) = this.input_error.take()
        {
            let result = validate_local_io_result(cx, Err(err));
            let buffer = unsafe { opt_take(&mut this.buffer) };
            return Poll::Ready((result, buffer));
        }
        if this.state_ptr.state_ptr().is_null() && this.buffer.is_none() {
            return Poll::Pending;
        }
        if this.state_ptr.state_ptr().is_null() && this.len == 0 {
            let buffer = unsafe { opt_take(&mut this.buffer) };
            return complete_empty_stream_io(cx, buffer);
        }

        if let Some(completion) = unsafe {
            take_completed_result_and_payload::<RetainedWritePayload<B>>(cx, &mut this.state_ptr)
        } {
            let (result, payload) = completion.into_io_result(completion_cqe_result);
            return Poll::Ready((result, payload.buffer));
        }

        if this.state_ptr.state_ptr().is_null() {
            let pctx = match poll_ctx_from_waker(cx) {
                Ok(pctx) => pctx,
                Err(err) => {
                    let buffer = unsafe { opt_take(&mut this.buffer) };
                    return Poll::Ready((Err(err), buffer));
                }
            };
            let guard = match unsafe { prepare_unsubmitted_op(&pctx) } {
                Some(guard) => guard,
                None => {
                    let buffer = unsafe { opt_take(&mut this.buffer) };
                    return Poll::Ready((Err(io::Error::from(io::ErrorKind::WouldBlock)), buffer));
                }
            };
            let state_ptr = guard.state_ptr();

            let payload = RetainedWritePayload {
                buffer: unsafe { opt_take(&mut this.buffer) },
            };
            unsafe {
                if let Err((e, payload)) = submit_retained_fd_sqe(
                    &pctx,
                    state_ptr,
                    &mut this.state_ptr,
                    payload,
                    |fd, payload| {
                        let ptr = payload.buffer.as_ptr();
                        Ok(build_send_entry(
                            fd,
                            ptr,
                            this.len,
                            SEND_FLAGS,
                            state_ptr as u64,
                        ))
                    },
                ) {
                    return Poll::Ready((Err(e), payload.buffer));
                }
            }
            guard.disarm();
            return Poll::Pending;
        }

        unsafe { refresh_op_waiter_from_waker(cx, this.state_ptr.state_ptr()) };
        Poll::Pending
    }
}

impl<B: IoBuffReadOnly, S> Drop for WriteFuture<'_, B, S> {
    fn drop(&mut self) {
        unsafe { drop_fd_op_state_unchecked(&mut self.state_ptr) };
    }
}

// ---------------------------------------------------------------------------
// WriteAllFuture
// ---------------------------------------------------------------------------

/// Writes the entire buffer, re-submitting on partial writes.
///
/// The buffer's base pointer is captured when the first write is submitted and
/// reused for resubmissions, avoiding repeated `as_ptr()` calls. A poll that
/// handles a completion and resubmits validates the runtime context once for
/// both steps.
pub struct WriteAllFuture<'a, B: IoBuffReadOnly, S> {
    /// Completion state reused across sequential retry submissions.
    state_ptr: RuntimeFdOpState<'a>,
    /// Caller-owned source buffer returned when the operation finishes.
    buffer: Option<B>,
    /// Stable base pointer into the retained buffer, captured once after the
    /// buffer has been moved into operation state.
    base_ptr: *const u8,
    /// Descriptor captured for debug-build checks against `state_ptr`.
    /// Release builds store `RuntimeFd::INVALID`; SQE construction derives the
    /// live descriptor from `state_ptr`.
    fd: RawFd,
    /// Bytes already confirmed written by completed submissions.
    offset: u32,
    /// Total number of bytes that must be written before completion.
    total: u32,
    /// Deferred validation error returned before any SQE submission.
    input_error: Option<io::Error>,
    /// Borrows the parent stream for the future lifetime.
    _marker: PhantomData<&'a mut S>,
}

impl<'a, B: IoBuffReadOnly, S> WriteAllFuture<'a, B, S> {
    pub(crate) fn new(fd_state: RuntimeFdOpState<'a>, buffer: B) -> Self {
        let fd = fd_state.debug_raw_fd();
        let mut input_error = None;
        let total = match super::checked_send_len(buffer.len()) {
            Ok(total) => total,
            Err(err) => {
                input_error = Some(err);
                0
            }
        };
        Self {
            state_ptr: fd_state,
            buffer: Some(buffer),
            base_ptr: std::ptr::null(),
            fd,
            offset: 0,
            total,
            input_error,
            _marker: PhantomData,
        }
    }
}

impl<B: IoBuffReadOnly, S> Future for WriteAllFuture<'_, B, S> {
    type Output = (io::Result<usize>, B);

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = unsafe { self.get_unchecked_mut() };
        this.state_ptr
            .debug_assert_matches_raw_fd(this.fd, FdStateDiagnostic::Stream);

        if this.state_ptr.state_ptr().is_null()
            && let Some(err) = this.input_error.take()
        {
            let result = validate_local_io_result(cx, Err(err));
            let buffer = unsafe { opt_take(&mut this.buffer) };
            return Poll::Ready((result, buffer));
        }
        if this.state_ptr.state_ptr().is_null() && this.buffer.is_none() {
            return Poll::Pending;
        }

        // Fast path: validate/register the current waiter, then remain pending.
        if unsafe { retry_state_is_in_flight(cx, this.state_ptr.state_ptr()) } {
            return Poll::Pending;
        }

        // Zero-length write completes immediately.
        if this.state_ptr.state_ptr().is_null() && this.total == 0 {
            let buffer = unsafe { opt_take(&mut this.buffer) };
            return complete_empty_stream_io(cx, buffer);
        }

        let pctx = if this.state_ptr.state_ptr().is_null() {
            match poll_ctx_from_waker(cx) {
                Ok(pctx) => pctx,
                Err(err) => {
                    let buffer = unsafe { opt_take(&mut this.buffer) };
                    return Poll::Ready((Err(err), buffer));
                }
            }
        } else {
            match unsafe {
                retry_poll_ctx_or_rejected_payload::<RetainedWritePayload<B>>(
                    cx,
                    &mut this.state_ptr,
                )
            } {
                Ok(pctx) => pctx,
                Err(payload) => {
                    return Poll::Ready((
                        Err(io::Error::from(io::ErrorKind::NotConnected)),
                        payload.buffer,
                    ));
                }
            }
        };

        // Process completed state if any. Sequential retries reuse the same
        // completion slot once the previous CQE has been fully consumed.
        if !this.state_ptr.state_ptr().is_null() {
            match classify_retry_cqe_result(unsafe { (*this.state_ptr.state_ptr()).result }) {
                RetryCqeResult::KernelError(errno) => {
                    let payload = unsafe {
                        take_retained_payload_and_free_state::<RetainedWritePayload<B>>(
                            &pctx,
                            &mut this.state_ptr,
                        )
                    };
                    return Poll::Ready((Err(io::Error::from_raw_os_error(errno)), payload.buffer));
                }
                RetryCqeResult::Zero => {
                    let payload = unsafe {
                        take_retained_payload_and_free_state::<RetainedWritePayload<B>>(
                            &pctx,
                            &mut this.state_ptr,
                        )
                    };
                    return Poll::Ready((
                        Err(io::Error::from(io::ErrorKind::WriteZero)),
                        payload.buffer,
                    ));
                }
                RetryCqeResult::Bytes(n) => {
                    let n = n as u32;
                    debug_assert!(n <= this.total - this.offset);
                    this.offset += n;
                    if this.offset >= this.total {
                        let payload = unsafe {
                            take_retained_payload_and_free_state::<RetainedWritePayload<B>>(
                                &pctx,
                                &mut this.state_ptr,
                            )
                        };
                        return Poll::Ready((Ok(this.offset as usize), payload.buffer));
                    }
                }
            }
        }
        if this.buffer.is_some() {
            let guard = match unsafe { prepare_unsubmitted_op(&pctx) } {
                Some(guard) => guard,
                None => {
                    let buffer = unsafe { opt_take(&mut this.buffer) };
                    return Poll::Ready((Err(io::Error::from(io::ErrorKind::WouldBlock)), buffer));
                }
            };
            let state_ptr = guard.state_ptr();
            let payload = RetainedWritePayload {
                buffer: unsafe { opt_take(&mut this.buffer) },
            };
            unsafe {
                if let Err((e, payload)) = submit_retained_fd_sqe(
                    &pctx,
                    state_ptr,
                    &mut this.state_ptr,
                    payload,
                    |fd, payload| {
                        this.base_ptr = payload.buffer.as_ptr();
                        let ptr = this.base_ptr;
                        let remaining = this.total - this.offset;
                        Ok(build_send_entry(
                            fd,
                            ptr,
                            remaining,
                            SEND_FLAGS,
                            state_ptr as u64,
                        ))
                    },
                ) {
                    return Poll::Ready((Err(e), payload.buffer));
                }
            }
            guard.disarm();
            return Poll::Pending;
        }

        let state_ptr = this.state_ptr.state_ptr();
        unsafe { reset_existing_retry_state(&mut *state_ptr, pctx.owner_task()) };
        let ptr = unsafe { this.base_ptr.add(this.offset as usize) };
        let remaining = this.total - this.offset;

        unsafe {
            if let Err(e) = submit_resubmitted_fd_sqe(&pctx, &this.state_ptr, |fd| {
                Ok(build_send_entry(
                    fd,
                    ptr,
                    remaining,
                    SEND_FLAGS,
                    state_ptr as u64,
                ))
            }) {
                let payload = take_retained_payload_and_free_state::<RetainedWritePayload<B>>(
                    &pctx,
                    &mut this.state_ptr,
                );
                return Poll::Ready((Err(e), payload.buffer));
            }
        }

        Poll::Pending
    }
}

impl<B: IoBuffReadOnly, S> Drop for WriteAllFuture<'_, B, S> {
    fn drop(&mut self) {
        unsafe { drop_fd_op_state_unchecked(&mut self.state_ptr) };
    }
}

// ---------------------------------------------------------------------------
// ReadExactFuture
// ---------------------------------------------------------------------------

/// Reads exactly the requested number of bytes, re-submitting on partial reads
/// and no-progress EAGAIN completions.
///
/// Returns `UnexpectedEof` if the peer closes before that many bytes arrive. On
/// error the buffer reflects the bytes received so far. Like
/// [`WriteAllFuture`], it captures the buffer's base pointer when the first
/// read is submitted and reuses it for resubmissions. A poll that handles a
/// completion and resubmits validates the runtime context once for both steps.
pub struct ReadExactFuture<'a, B: IoBuffReadWrite, S> {
    /// Completion state reused across sequential retry submissions.
    state_ptr: RuntimeFdOpState<'a>,
    /// Caller-owned destination buffer returned when the operation finishes.
    buffer: Option<B>,
    /// Stable base pointer into the retained buffer's writable region.
    base_ptr: *mut u8,
    /// Descriptor captured for debug-build checks against `state_ptr`.
    /// Release builds store `RuntimeFd::INVALID`; SQE construction derives the
    /// live descriptor from `state_ptr`.
    fd: RawFd,
    /// Exact byte count required before the future can succeed.
    target: u32,
    /// Bytes already read into the destination buffer.
    filled: u32,
    /// Logical length immediately before the submitted writable region.
    write_base_len: usize,
    /// Deferred validation error returned before any SQE submission.
    input_error: Option<io::Error>,
    /// Borrows the parent stream for the future lifetime.
    _marker: PhantomData<&'a mut S>,
}

impl<'a, B: IoBuffReadWrite, S> ReadExactFuture<'a, B, S> {
    pub(crate) fn new(fd_state: RuntimeFdOpState<'a>, buffer: B, len: usize) -> Self {
        let fd = fd_state.debug_raw_fd();
        let write_base_len = buffer.write_base_len();
        let mut input_error = None;
        let target = match super::checked_read_len(len, buffer.writable_len()) {
            Ok(target) => target,
            Err(err) => {
                input_error = Some(err);
                0
            }
        };
        Self {
            state_ptr: fd_state,
            buffer: Some(buffer),
            base_ptr: std::ptr::null_mut(),
            fd,
            target,
            filled: 0,
            write_base_len,
            input_error,
            _marker: PhantomData,
        }
    }
}

impl<B: IoBuffReadWrite, S> Future for ReadExactFuture<'_, B, S> {
    type Output = (io::Result<usize>, B);

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = unsafe { self.get_unchecked_mut() };
        this.state_ptr
            .debug_assert_matches_raw_fd(this.fd, FdStateDiagnostic::Stream);

        if this.state_ptr.state_ptr().is_null()
            && let Some(err) = this.input_error.take()
        {
            let result = validate_local_io_result(cx, Err(err));
            let buffer = unsafe { opt_take(&mut this.buffer) };
            return Poll::Ready((result, buffer));
        }
        if this.state_ptr.state_ptr().is_null() && this.buffer.is_none() {
            return Poll::Pending;
        }

        // Fast path: validate/register the current waiter, then remain pending.
        if unsafe { retry_state_is_in_flight(cx, this.state_ptr.state_ptr()) } {
            return Poll::Pending;
        }

        // Zero-length read completes immediately.
        if this.state_ptr.state_ptr().is_null() && this.target == 0 {
            let buffer = unsafe { opt_take(&mut this.buffer) };
            return complete_empty_stream_io(cx, buffer);
        }

        let pctx = if this.state_ptr.state_ptr().is_null() {
            match poll_ctx_from_waker(cx) {
                Ok(pctx) => pctx,
                Err(err) => {
                    let buffer = unsafe { opt_take(&mut this.buffer) };
                    return Poll::Ready(unsafe {
                        complete_read_with_progress(
                            buffer,
                            this.write_base_len,
                            this.filled as usize,
                            Err(err),
                        )
                    });
                }
            }
        } else {
            match unsafe {
                retry_poll_ctx_or_rejected_payload::<RetainedReadPayload<B>>(
                    cx,
                    &mut this.state_ptr,
                )
            } {
                Ok(pctx) => pctx,
                Err(payload) => {
                    return Poll::Ready(unsafe {
                        complete_read_with_progress(
                            payload.buffer,
                            this.write_base_len,
                            this.filled as usize,
                            Err(io::Error::from(io::ErrorKind::NotConnected)),
                        )
                    });
                }
            }
        };

        // Process completed state if any. Sequential retries reuse the same
        // completion slot once the previous CQE has been fully consumed.
        if !this.state_ptr.state_ptr().is_null() {
            match classify_retry_cqe_result(unsafe { (*this.state_ptr.state_ptr()).result }) {
                RetryCqeResult::KernelError(libc::EAGAIN) => {}
                RetryCqeResult::KernelError(errno) => {
                    let payload = unsafe {
                        take_retained_payload_and_free_state::<RetainedReadPayload<B>>(
                            &pctx,
                            &mut this.state_ptr,
                        )
                    };
                    return Poll::Ready(unsafe {
                        complete_read_with_progress(
                            payload.buffer,
                            this.write_base_len,
                            this.filled as usize,
                            Err(io::Error::from_raw_os_error(errno)),
                        )
                    });
                }
                RetryCqeResult::Zero => {
                    let payload = unsafe {
                        take_retained_payload_and_free_state::<RetainedReadPayload<B>>(
                            &pctx,
                            &mut this.state_ptr,
                        )
                    };
                    return Poll::Ready(unsafe {
                        complete_read_with_progress(
                            payload.buffer,
                            this.write_base_len,
                            this.filled as usize,
                            Err(io::Error::from(io::ErrorKind::UnexpectedEof)),
                        )
                    });
                }
                RetryCqeResult::Bytes(n) => {
                    let n = n as u32;
                    debug_assert!(n <= this.target - this.filled);
                    this.filled += n;
                    if this.filled >= this.target {
                        let payload = unsafe {
                            take_retained_payload_and_free_state::<RetainedReadPayload<B>>(
                                &pctx,
                                &mut this.state_ptr,
                            )
                        };
                        return Poll::Ready(unsafe {
                            complete_read_with_progress(
                                payload.buffer,
                                this.write_base_len,
                                this.target as usize,
                                Ok(this.target as usize),
                            )
                        });
                    }
                }
            }
        }
        if this.buffer.is_some() {
            let guard = match unsafe { prepare_unsubmitted_op(&pctx) } {
                Some(guard) => guard,
                None => {
                    let buffer = unsafe { opt_take(&mut this.buffer) };
                    return Poll::Ready(unsafe {
                        complete_read_with_progress(
                            buffer,
                            this.write_base_len,
                            this.filled as usize,
                            Err(io::Error::from(io::ErrorKind::WouldBlock)),
                        )
                    });
                }
            };
            let state_ptr = guard.state_ptr();
            let payload = RetainedReadPayload {
                buffer: unsafe { opt_take(&mut this.buffer) },
            };
            unsafe {
                if let Err((e, payload)) = submit_retained_fd_sqe(
                    &pctx,
                    state_ptr,
                    &mut this.state_ptr,
                    payload,
                    |fd, payload| {
                        this.base_ptr = payload.buffer.as_mut_ptr();
                        let ptr = this.base_ptr.add(this.filled as usize);
                        let remaining = this.target - this.filled;
                        Ok(build_recv_entry(fd, ptr, remaining, state_ptr as u64))
                    },
                ) {
                    return Poll::Ready(complete_read_with_progress(
                        payload.buffer,
                        this.write_base_len,
                        this.filled as usize,
                        Err(e),
                    ));
                }
            }
            guard.disarm();
            return Poll::Pending;
        }

        let state_ptr = this.state_ptr.state_ptr();
        unsafe { reset_existing_retry_state(&mut *state_ptr, pctx.owner_task()) };
        let ptr = unsafe { this.base_ptr.add(this.filled as usize) };
        let remaining = this.target - this.filled;

        unsafe {
            if let Err(e) = submit_resubmitted_fd_sqe(&pctx, &this.state_ptr, |fd| {
                Ok(build_recv_entry(fd, ptr, remaining, state_ptr as u64))
            }) {
                let payload = take_retained_payload_and_free_state::<RetainedReadPayload<B>>(
                    &pctx,
                    &mut this.state_ptr,
                );
                return Poll::Ready(complete_read_with_progress(
                    payload.buffer,
                    this.write_base_len,
                    this.filled as usize,
                    Err(e),
                ));
            }
        }

        Poll::Pending
    }
}

impl<B: IoBuffReadWrite, S> Drop for ReadExactFuture<'_, B, S> {
    fn drop(&mut self) {
        unsafe { drop_fd_op_state_unchecked(&mut self.state_ptr) };
    }
}

// ---------------------------------------------------------------------------
// ReadvFuture
// ---------------------------------------------------------------------------

/// Scatter-read into a vectored buffer chain (rental pattern).
///
/// Returns the first positive or terminal completion without looping on partial
/// progress. A no-progress EAGAIN completion is resubmitted internally.
pub struct ReadvFuture<'a, const N: usize, S> {
    /// Completion state for the submitted RECV or RECVMSG SQE, if any.
    state_ptr: RuntimeFdOpState<'a>,
    /// Caller-owned mutable segment chain returned on completion.
    buffer: Option<IoBuffVecMut<N>>,
    /// Number of non-empty segment entries materialized into retained scratch.
    iov_count: usize,
    /// Total writable capacity across all segments, cached so zero-capacity
    /// reads complete before submission and debug checks do not re-walk the
    /// caller-owned chain.
    writable: usize,
    /// Descriptor captured for debug-build checks against `state_ptr`.
    /// Release builds store `RuntimeFd::INVALID`; SQE construction derives the
    /// live descriptor from `state_ptr`.
    fd: RawFd,
    /// Exact deferred error classified by the single sizing pass, if any.
    deferred_error: DeferredReadvError,
    /// Borrows the parent stream for the future lifetime.
    _marker: PhantomData<&'a mut S>,
}

impl<'a, const N: usize, S> ReadvFuture<'a, N, S> {
    pub(crate) fn new(fd_state: RuntimeFdOpState<'a>, buffer: IoBuffVecMut<N>) -> Self {
        let fd = fd_state.debug_raw_fd();
        let (iov_count, writable, deferred_error) =
            match buffer.checked_read_iovec_count_and_writable_len() {
                Some((iov_count, writable)) => (
                    iov_count,
                    writable,
                    if N > RETAINED_IOVEC_MAX_COUNT && iov_count > RETAINED_IOVEC_MAX_COUNT {
                        DeferredReadvError::ActiveIovecOverflow
                    } else {
                        DeferredReadvError::None
                    },
                ),
                None => (0, 0, DeferredReadvError::AggregateOverflow),
            };
        Self {
            state_ptr: fd_state,
            buffer: Some(buffer),
            iov_count,
            writable,
            fd,
            deferred_error,
            _marker: PhantomData,
        }
    }
}

impl<const N: usize, S> Future for ReadvFuture<'_, N, S> {
    type Output = (io::Result<usize>, IoBuffVecMut<N>);

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = unsafe { self.get_unchecked_mut() };
        this.state_ptr
            .debug_assert_matches_raw_fd(this.fd, FdStateDiagnostic::Stream);

        if let Some(retry) = unsafe {
            retry_completed_stream_read::<RetainedReadvPayload<N>, _>(
                cx,
                &mut this.state_ptr,
                |fd, payload, user_data| {
                    build_read_vectored_entry(
                        fd,
                        retained_iovecs(&payload.scratch),
                        0,
                        this.iov_count,
                        &mut payload.msg,
                        user_data,
                    )
                },
                |payload| take_readv_buffer_from_retained(payload),
            )
        } {
            return retry;
        }

        if let Some(completion) = unsafe {
            take_completed_result_and_payload_with::<RetainedReadvPayload<N>, _>(
                cx,
                &mut this.state_ptr,
                |payload| take_readv_buffer_from_retained(payload),
            )
        } {
            let (result, mut buffer) = completion.into_io_result(completion_cqe_result);
            let actual = match result {
                Ok(actual) => actual,
                Err(err) => return Poll::Ready((Err(err), buffer)),
            };
            unsafe { buffer.distribute_written(actual) };
            return Poll::Ready((Ok(actual), buffer));
        }
        if this.state_ptr.state_ptr().is_null() && this.buffer.is_none() {
            return Poll::Pending;
        }

        if this.state_ptr.state_ptr().is_null() && this.deferred_error != DeferredReadvError::None {
            #[cfg(debug_assertions)]
            let result = if this.deferred_error == DeferredReadvError::ActiveIovecOverflow {
                validate_local_iovec_oversize_rejection(
                    cx,
                    Err(materialize_deferred_readv_error(&this.deferred_error)),
                )
            } else {
                validate_local_io_result(
                    cx,
                    Err(materialize_deferred_readv_error(&this.deferred_error)),
                )
            };
            #[cfg(not(debug_assertions))]
            let result = validate_local_io_result(
                cx,
                Err(materialize_deferred_readv_error(&this.deferred_error)),
            );
            let buffer = unsafe { opt_take(&mut this.buffer) };
            return Poll::Ready((result, buffer));
        }

        if this.writable == 0 {
            let buffer = unsafe { opt_take(&mut this.buffer) };
            let result = validate_local_io_result(
                cx,
                Err(super::invalid_input("empty vectored receive chain")),
            );
            return Poll::Ready((result, buffer));
        }

        if this.state_ptr.state_ptr().is_null() {
            let pctx = match poll_ctx_from_waker(cx) {
                Ok(pctx) => pctx,
                Err(err) => {
                    let buffer = unsafe { opt_take(&mut this.buffer) };
                    return Poll::Ready((Err(err), buffer));
                }
            };
            let guard = match unsafe { prepare_unsubmitted_op(&pctx) } {
                Some(guard) => guard,
                None => {
                    let buffer = unsafe { opt_take(&mut this.buffer) };
                    return Poll::Ready((Err(io::Error::from(io::ErrorKind::WouldBlock)), buffer));
                }
            };
            let state_ptr = guard.state_ptr();

            let retained_pool = unsafe { Reactor::retained_payload_pool_ptr(pctx.reactor()) };
            let scratch_init =
                match unsafe { (*retained_pool.as_ptr()).alloc_iovec_scratch_init(this.iov_count) }
                {
                    Ok(scratch_init) => scratch_init,
                    Err(err) => {
                        let buffer = unsafe { opt_take(&mut this.buffer) };
                        return Poll::Ready((Err(err), buffer));
                    }
                };

            let payload = unsafe {
                emplace_retained_readv_payload(retained_pool, &mut this.buffer, scratch_init)
            };
            unsafe {
                if let Err((e, payload)) = submit_initialized_retained_fd_sqe(
                    &pctx,
                    state_ptr,
                    &mut this.state_ptr,
                    payload,
                    |fd, payload| {
                        let actual_shape = payload.buffer.fill_read_iovecs_and_writable_len(
                            payload.scratch.as_uninit_slice_mut(),
                        )?;
                        if actual_shape != (this.iov_count, this.writable) {
                            return Err(invalid_read_iovec_shape());
                        }
                        Ok(build_read_vectored_entry(
                            fd,
                            retained_iovecs(&payload.scratch),
                            0,
                            actual_shape.0,
                            &mut payload.msg,
                            state_ptr as u64,
                        ))
                    },
                ) {
                    return Poll::Ready((Err(e), payload.buffer));
                }
            }
            guard.disarm();
            return Poll::Pending;
        }

        unsafe { refresh_op_waiter_from_waker(cx, this.state_ptr.state_ptr()) };
        Poll::Pending
    }
}

impl<const N: usize, S> Drop for ReadvFuture<'_, N, S> {
    fn drop(&mut self) {
        unsafe { drop_fd_op_state_unchecked(&mut self.state_ptr) };
    }
}

// ---------------------------------------------------------------------------
// WritevFuture
// ---------------------------------------------------------------------------

/// Gather-write from an owned vectored buffer chain (rental pattern).
pub struct WritevFuture<'a, C: WriteBufferChain<N>, const N: usize, S> {
    /// Completion state for the submitted writev/write SQE, if any.
    state_ptr: RuntimeFdOpState<'a>,
    /// Caller-owned read-only segment chain returned on completion.
    buffer: Option<C>,
    /// Number of non-empty source segments to materialize into iovec scratch.
    iov_count: usize,
    /// Total initialized bytes available across all segments.
    total: usize,
    /// Descriptor captured for debug-build checks against `state_ptr`.
    /// Release builds store `RuntimeFd::INVALID`; SQE construction derives the
    /// live descriptor from `state_ptr`.
    fd: RawFd,
    /// Borrows the parent stream for the future lifetime.
    _marker: PhantomData<&'a mut S>,
}

impl<'a, C: WriteBufferChain<N>, const N: usize, S> WritevFuture<'a, C, N, S> {
    pub(crate) fn new(fd_state: RuntimeFdOpState<'a>, buffer: C) -> Self {
        let fd = fd_state.debug_raw_fd();
        Self {
            state_ptr: fd_state,
            buffer: Some(buffer),
            iov_count: 0,
            total: 0,
            fd,
            _marker: PhantomData,
        }
    }
}

impl<C: WriteBufferChain<N>, const N: usize, S> Future for WritevFuture<'_, C, N, S> {
    type Output = (io::Result<usize>, C);

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = unsafe { self.get_unchecked_mut() };
        this.state_ptr
            .debug_assert_matches_raw_fd(this.fd, FdStateDiagnostic::Stream);

        if let Some(completion) = unsafe {
            take_completed_result_and_payload_with::<RetainedWritevPayload<C>, _>(
                cx,
                &mut this.state_ptr,
                |payload| take_writev_completion_from_retained(payload),
            )
        } {
            let (result, completion) = completion.into_io_result(completion_cqe_result);
            return Poll::Ready((result, completion.buffer));
        }
        if this.state_ptr.state_ptr().is_null() && this.buffer.is_none() {
            return Poll::Pending;
        }

        if this.state_ptr.state_ptr().is_null() {
            let pctx = match poll_ctx_from_waker(cx) {
                Ok(pctx) => pctx,
                Err(err) => {
                    let buffer = unsafe { opt_take(&mut this.buffer) };
                    return Poll::Ready((Err(err), buffer));
                }
            };
            let Some((iov_count, total)) =
                unsafe { opt_ref(&this.buffer) }.write_iovec_count_and_len()
            else {
                let buffer = unsafe { opt_take(&mut this.buffer) };
                return Poll::Ready((Err(invalid_writev_aggregate()), buffer));
            };
            debug_assert!(
                total == 0 || iov_count > 0,
                "non-empty write chain produced no iovecs"
            );
            this.iov_count = iov_count;
            this.total = total;
            if total == 0 {
                let buffer = unsafe { opt_take(&mut this.buffer) };
                return Poll::Ready((Ok(0), buffer));
            }

            let retained_pool = unsafe { Reactor::retained_payload_pool_ptr(pctx.reactor()) };
            let scratch_init =
                match unsafe { (*retained_pool.as_ptr()).alloc_iovec_scratch_init(this.iov_count) }
                {
                    Ok(scratch_init) => scratch_init,
                    Err(err) => {
                        let buffer = unsafe { opt_take(&mut this.buffer) };
                        return Poll::Ready((Err(err), buffer));
                    }
                };

            let (payload, guard) = match unsafe {
                emplace_retained_writev_payload::<C, N, _>(
                    retained_pool,
                    &mut this.buffer,
                    scratch_init,
                    this.total,
                    || {
                        prepare_unsubmitted_op(&pctx)
                            .ok_or_else(|| io::Error::from(io::ErrorKind::WouldBlock))
                    },
                )
            } {
                Ok(result) => result,
                Err(err) => {
                    let buffer = unsafe { opt_take(&mut this.buffer) };
                    return Poll::Ready((Err(err), buffer));
                }
            };
            let state_ptr = guard.state_ptr();

            unsafe {
                if let Err((e, payload)) = submit_initialized_retained_fd_sqe(
                    &pctx,
                    state_ptr,
                    &mut this.state_ptr,
                    payload,
                    |fd, payload| {
                        Ok(build_write_vectored_entry(
                            fd,
                            retained_iovecs(&payload.scratch),
                            0,
                            payload.scratch.len(),
                            &mut payload.msg,
                            state_ptr as u64,
                        ))
                    },
                ) {
                    return Poll::Ready((Err(e), payload.buffer));
                }
            }
            guard.disarm();
            return Poll::Pending;
        }

        unsafe { refresh_op_waiter_from_waker(cx, this.state_ptr.state_ptr()) };
        Poll::Pending
    }
}

impl<C: WriteBufferChain<N>, const N: usize, S> Drop for WritevFuture<'_, C, N, S> {
    fn drop(&mut self) {
        unsafe { drop_fd_op_state_unchecked(&mut self.state_ptr) };
    }
}

// ---------------------------------------------------------------------------
// WritevAllFuture
// ---------------------------------------------------------------------------

/// Gather-write an entire owned vectored chain, handling partial writes.
pub struct WritevAllFuture<'a, C: WriteBufferChain<N>, const N: usize, S> {
    /// Completion state reused across sequential retry submissions.
    state_ptr: RuntimeFdOpState<'a>,
    /// Caller-owned read-only segment chain returned when the operation finishes.
    buffer: Option<C>,
    /// Number of non-empty source segments to materialize into iovec scratch.
    iov_count: usize,
    /// Descriptor captured for debug-build checks against `state_ptr`.
    /// Release builds store `RuntimeFd::INVALID`; SQE construction derives the
    /// live descriptor from `state_ptr`.
    fd: RawFd,
    /// Submission mode for the remaining segments of this call.
    send_mode: WritevSendMode,
    /// Total bytes that must be written before completion.
    total: usize,
    /// Borrows the parent stream for the future lifetime.
    _marker: PhantomData<&'a mut S>,
}

impl<'a, C: WriteBufferChain<N>, const N: usize, S> WritevAllFuture<'a, C, N, S> {
    pub(crate) fn new(fd_state: RuntimeFdOpState<'a>, buffer: C) -> Self {
        let fd = fd_state.debug_raw_fd();
        Self {
            state_ptr: fd_state,
            buffer: Some(buffer),
            iov_count: 0,
            fd,
            send_mode: WritevSendMode::Vectored,
            total: 0,
            _marker: PhantomData,
        }
    }
}

impl<C: WriteBufferChain<N>, const N: usize, S> Future for WritevAllFuture<'_, C, N, S> {
    type Output = (io::Result<usize>, C);

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = unsafe { self.get_unchecked_mut() };
        this.state_ptr
            .debug_assert_matches_raw_fd(this.fd, FdStateDiagnostic::Stream);

        if this.state_ptr.state_ptr().is_null() && this.buffer.is_none() {
            return Poll::Pending;
        }

        if unsafe { retry_state_is_in_flight(cx, this.state_ptr.state_ptr()) } {
            return Poll::Pending;
        }

        let pctx = if this.state_ptr.state_ptr().is_null() {
            match poll_ctx_from_waker(cx) {
                Ok(pctx) => pctx,
                Err(err) => {
                    let buffer = unsafe { opt_take(&mut this.buffer) };
                    return Poll::Ready((Err(err), buffer));
                }
            }
        } else {
            match unsafe {
                retry_poll_ctx_or_rejected_payload_with::<RetainedWritevPayload<C>, _>(
                    cx,
                    &mut this.state_ptr,
                    |payload| take_writev_completion_from_retained(payload),
                )
            } {
                Ok(pctx) => pctx,
                Err(completion) => {
                    return Poll::Ready((
                        Err(io::Error::from(io::ErrorKind::NotConnected)),
                        completion.buffer,
                    ));
                }
            }
        };

        if this.buffer.is_some() {
            let Some((iov_count, total)) =
                unsafe { opt_ref(&this.buffer) }.write_iovec_count_and_len()
            else {
                let buffer = unsafe { opt_take(&mut this.buffer) };
                return Poll::Ready((Err(invalid_writev_aggregate()), buffer));
            };
            debug_assert!(
                total == 0 || iov_count > 0,
                "non-empty write-all chain produced no iovecs"
            );
            this.iov_count = iov_count;
            this.total = total;
            if total == 0 {
                let buffer = unsafe { opt_take(&mut this.buffer) };
                return Poll::Ready((Ok(0), buffer));
            }
            if N > RETAINED_IOVEC_MAX_COUNT && iov_count > RETAINED_IOVEC_MAX_COUNT {
                #[cfg(debug_assertions)]
                record_retained_iovec_oversize_rejection(&pctx);
                let buffer = unsafe { opt_take(&mut this.buffer) };
                return Poll::Ready((Err(retained_iovec_oversize_error()), buffer));
            }
        }

        let switched = !this.state_ptr.state_ptr().is_null()
            && unsafe { (*this.state_ptr.state_ptr()).result == -libc::EAGAIN }
            && unsafe {
                let state = &mut *this.state_ptr.state_ptr();
                let result = state.result;
                let payload = state.retained_payload_mut::<RetainedWritevPayload<C>>();
                switch_writev_to_scalar(
                    &mut this.send_mode,
                    result,
                    retained_iovecs(&payload.scratch),
                    payload.skip,
                    remaining_iovec_count(&payload.scratch, payload.skip),
                )
            };
        if !this.state_ptr.state_ptr().is_null() && !switched {
            let state_ptr = this.state_ptr.state_ptr();
            match classify_retry_cqe_result(unsafe { (*state_ptr).result }) {
                RetryCqeResult::KernelError(errno) => {
                    let completion = unsafe {
                        take_retained_payload_with_and_free_state::<RetainedWritevPayload<C>, _>(
                            &pctx,
                            &mut this.state_ptr,
                            |payload| take_writev_completion_from_retained(payload),
                        )
                    };
                    return Poll::Ready((
                        Err(io::Error::from_raw_os_error(errno)),
                        completion.buffer,
                    ));
                }
                RetryCqeResult::Zero => {
                    let completion = unsafe {
                        take_retained_payload_with_and_free_state::<RetainedWritevPayload<C>, _>(
                            &pctx,
                            &mut this.state_ptr,
                            |payload| take_writev_completion_from_retained(payload),
                        )
                    };
                    return Poll::Ready((
                        Err(io::Error::from(io::ErrorKind::WriteZero)),
                        completion.buffer,
                    ));
                }
                RetryCqeResult::Bytes(n) => {
                    let completed = unsafe {
                        let payload =
                            (*state_ptr).retained_payload_mut::<RetainedWritevPayload<C>>();
                        debug_assert!(n <= this.total - payload.written);
                        payload.written += n;
                        payload.written >= this.total
                    };
                    if completed {
                        let completion = unsafe {
                            take_retained_payload_with_and_free_state::<RetainedWritevPayload<C>, _>(
                                &pctx,
                                &mut this.state_ptr,
                                |payload| take_writev_completion_from_retained(payload),
                            )
                        };
                        return Poll::Ready((Ok(completion.written), completion.buffer));
                    }

                    unsafe {
                        let payload =
                            (*state_ptr).retained_payload_mut::<RetainedWritevPayload<C>>();
                        let mut skip = payload.skip;
                        advance_iovecs_in_place(
                            retained_iovecs_mut(&mut payload.scratch, this.iov_count),
                            &mut skip,
                            n,
                        );
                        payload.skip = skip;
                    }
                    #[cfg(debug_assertions)]
                    unsafe {
                        let stats = &mut (*pctx.runtime_state()).stats;
                        stats.writev_partial_continuations =
                            stats.writev_partial_continuations.saturating_add(1);
                    }
                    #[cfg(feature = "diagnostic-counters")]
                    unsafe {
                        Reactor::note_writev_partial_continuation(pctx.reactor());
                    }
                }
            }
        }
        if this.buffer.is_some() {
            debug_assert!(
                this.state_ptr.state_ptr().is_null(),
                "initial writev_all state was published"
            );
            let guard = match unsafe { prepare_unsubmitted_op(&pctx) } {
                Some(guard) => guard,
                None => {
                    let buffer = unsafe { opt_take(&mut this.buffer) };
                    return Poll::Ready((Err(io::Error::from(io::ErrorKind::WouldBlock)), buffer));
                }
            };
            let state_ptr = guard.state_ptr();

            let retained_pool = unsafe { Reactor::retained_payload_pool_ptr(pctx.reactor()) };
            let scratch_init =
                match unsafe { (*retained_pool.as_ptr()).alloc_iovec_scratch_init(this.iov_count) }
                {
                    Ok(scratch_init) => scratch_init,
                    Err(err) => {
                        let buffer = unsafe { opt_take(&mut this.buffer) };
                        return Poll::Ready((Err(err), buffer));
                    }
                };
            let (payload, guard) = match unsafe {
                emplace_retained_writev_payload::<C, N, _>(
                    retained_pool,
                    &mut this.buffer,
                    scratch_init,
                    this.total,
                    move || Ok(guard),
                )
            } {
                Ok(result) => result,
                Err(err) => {
                    let buffer = unsafe { opt_take(&mut this.buffer) };
                    return Poll::Ready((Err(err), buffer));
                }
            };

            unsafe {
                if let Err((e, payload)) = submit_initialized_retained_fd_sqe(
                    &pctx,
                    state_ptr,
                    &mut this.state_ptr,
                    payload,
                    |fd, payload| {
                        let remaining_iovs = remaining_iovec_count(&payload.scratch, payload.skip);
                        Ok(build_write_vectored_entry(
                            fd,
                            retained_iovecs(&payload.scratch),
                            payload.skip,
                            remaining_iovs,
                            &mut payload.msg,
                            state_ptr as u64,
                        ))
                    },
                ) {
                    return Poll::Ready((Err(e), payload.buffer));
                }
            }
            guard.disarm();

            return Poll::Pending;
        }

        let state_ptr = this.state_ptr.state_ptr();
        unsafe { reset_existing_retry_state(&mut *state_ptr, pctx.owner_task()) };

        unsafe {
            if let Err(e) = submit_resubmitted_fd_sqe(&pctx, &this.state_ptr, |fd| {
                let payload = (*state_ptr).retained_payload_mut::<RetainedWritevPayload<C>>();
                Ok(build_write_retry_entry(
                    this.send_mode,
                    fd,
                    &payload.scratch,
                    payload.skip,
                    &mut payload.msg,
                    state_ptr as u64,
                ))
            }) {
                let payload = take_retained_payload_and_free_state::<RetainedWritevPayload<C>>(
                    &pctx,
                    &mut this.state_ptr,
                );
                return Poll::Ready((Err(e), payload.buffer));
            }
        }

        Poll::Pending
    }
}

impl<C: WriteBufferChain<N>, const N: usize, S> Drop for WritevAllFuture<'_, C, N, S> {
    fn drop(&mut self) {
        unsafe { drop_fd_op_state_unchecked(&mut self.state_ptr) };
    }
}

// ---------------------------------------------------------------------------
// Projected WritevFuture
// ---------------------------------------------------------------------------

/// Gather-write from one compact owned source projected into write pieces.
pub struct WritevProjectedFuture<'a, T: WritevProjection, S> {
    /// Completion state for the submitted projected writev/write SQE, if any.
    state_ptr: RuntimeFdOpState<'a>,
    /// Caller-owned projection source before it is moved into retained state.
    source: Option<T>,
    /// Number of non-empty projected pieces reported by the source.
    iov_count: usize,
    /// Total projected byte length reported by the source.
    total: usize,
    /// Input-shape validation result computed once at construction.
    input_error: Option<io::Error>,
    /// Descriptor captured for debug-build checks against `state_ptr`.
    /// Release builds store `RuntimeFd::INVALID`; SQE construction derives the
    /// live descriptor from `state_ptr`.
    fd: RawFd,
    /// Borrows the parent stream for the future lifetime.
    _marker: PhantomData<&'a mut S>,
}

impl<'a, T: WritevProjection, S> WritevProjectedFuture<'a, T, S> {
    pub(crate) fn new(fd_state: RuntimeFdOpState<'a>, source: T) -> Self {
        let fd = fd_state.debug_raw_fd();
        let (iov_count, total) = source.writev_count_and_len();
        let input_error = validate_projected_count_and_len(iov_count, total).err();
        Self {
            state_ptr: fd_state,
            source: Some(source),
            iov_count,
            total,
            input_error,
            fd,
            _marker: PhantomData,
        }
    }
}

impl<T: WritevProjection, S> Future for WritevProjectedFuture<'_, T, S> {
    type Output = (io::Result<usize>, T);

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = unsafe { self.get_unchecked_mut() };
        this.state_ptr
            .debug_assert_matches_raw_fd(this.fd, FdStateDiagnostic::Stream);

        if let Some(completion) = unsafe {
            take_completed_result_and_payload::<RetainedProjectedWritevPayload<T>>(
                cx,
                &mut this.state_ptr,
            )
        } {
            let (result, payload) = completion.into_io_result(completion_cqe_result);
            return Poll::Ready((result, payload.source));
        }
        if this.state_ptr.state_ptr().is_null() && this.source.is_none() {
            return Poll::Pending;
        }

        if this.state_ptr.state_ptr().is_null()
            && let Some(err) = this.input_error.take()
        {
            let result = validate_local_io_result(cx, Err(err));
            let source = unsafe { opt_take(&mut this.source) };
            return Poll::Ready((result, source));
        }

        if this.total == 0 {
            let result = validate_local_io_result(cx, Ok(())).and_then(|()| {
                validate_empty_projected_writev(unsafe { opt_ref(&this.source) }).map(|()| 0)
            });
            let source = unsafe { opt_take(&mut this.source) };
            return Poll::Ready((result, source));
        }

        if this.state_ptr.state_ptr().is_null() {
            let pctx = match poll_ctx_from_waker(cx) {
                Ok(pctx) => pctx,
                Err(err) => {
                    let source = unsafe { opt_take(&mut this.source) };
                    return Poll::Ready((Err(err), source));
                }
            };
            match submit_initial_projected_writev(
                &pctx,
                &mut this.state_ptr,
                &mut this.source,
                this.iov_count,
                this.total,
            ) {
                Ok(()) => return Poll::Pending,
                Err((err, source)) => return Poll::Ready((Err(err), source)),
            }
        }

        unsafe { refresh_op_waiter_from_waker(cx, this.state_ptr.state_ptr()) };
        Poll::Pending
    }
}

impl<T: WritevProjection, S> Drop for WritevProjectedFuture<'_, T, S> {
    fn drop(&mut self) {
        unsafe { drop_fd_op_state_unchecked(&mut self.state_ptr) };
    }
}

// ---------------------------------------------------------------------------
// Projected WritevAllFuture
// ---------------------------------------------------------------------------

/// Gather-write all projected pieces from one compact owned source.
pub struct WritevAllProjectedFuture<'a, T: WritevProjection, S> {
    /// Completion state reused across projected retry submissions.
    state_ptr: RuntimeFdOpState<'a>,
    /// Caller-owned projection source before it is moved into retained state.
    source: Option<T>,
    /// Number of non-empty projected pieces reported by the source.
    iov_count: usize,
    /// Descriptor captured for debug-build checks against `state_ptr`.
    /// Release builds store `RuntimeFd::INVALID`; SQE construction derives the
    /// live descriptor from `state_ptr`.
    fd: RawFd,
    /// Submission mode for the remaining segments of this call.
    send_mode: WritevSendMode,
    /// Total projected byte length that must be written before completion.
    total: usize,
    /// Borrows the parent stream for the future lifetime.
    _marker: PhantomData<&'a mut S>,
}

impl<'a, T: WritevProjection, S> WritevAllProjectedFuture<'a, T, S> {
    pub(crate) fn new(fd_state: RuntimeFdOpState<'a>, source: T) -> Self {
        let fd = fd_state.debug_raw_fd();
        let (iov_count, total) = source.writev_count_and_len();
        Self {
            state_ptr: fd_state,
            source: Some(source),
            iov_count,
            fd,
            send_mode: WritevSendMode::Vectored,
            total,
            _marker: PhantomData,
        }
    }
}

impl<T: WritevProjection, S> Future for WritevAllProjectedFuture<'_, T, S> {
    type Output = (io::Result<usize>, T);

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = unsafe { self.get_unchecked_mut() };
        this.state_ptr
            .debug_assert_matches_raw_fd(this.fd, FdStateDiagnostic::Stream);

        if this.state_ptr.state_ptr().is_null() && this.source.is_none() {
            return Poll::Pending;
        }

        if unsafe { retry_state_is_in_flight(cx, this.state_ptr.state_ptr()) } {
            return Poll::Pending;
        }

        if this.source.is_some() {
            if let Err(err) = validate_projected_count_and_len(this.iov_count, this.total) {
                let result = validate_local_io_result(cx, Err(err));
                let source = unsafe { opt_take(&mut this.source) };
                return Poll::Ready((result, source));
            }
            if this.total == 0 {
                let result = validate_local_io_result(cx, Ok(())).and_then(|()| {
                    validate_empty_projected_writev(unsafe { opt_ref(&this.source) }).map(|()| 0)
                });
                let source = unsafe { opt_take(&mut this.source) };
                return Poll::Ready((result, source));
            }
        }

        let pctx = if this.state_ptr.state_ptr().is_null() {
            match poll_ctx_from_waker(cx) {
                Ok(pctx) => pctx,
                Err(err) => {
                    let source = unsafe { opt_take(&mut this.source) };
                    return Poll::Ready((Err(err), source));
                }
            }
        } else {
            match unsafe {
                retry_poll_ctx_or_rejected_payload::<RetainedProjectedWritevPayload<T>>(
                    cx,
                    &mut this.state_ptr,
                )
            } {
                Ok(pctx) => pctx,
                Err(payload) => {
                    return Poll::Ready((
                        Err(io::Error::from(io::ErrorKind::NotConnected)),
                        payload.source,
                    ));
                }
            }
        };

        let switched = !this.state_ptr.state_ptr().is_null()
            && unsafe { (*this.state_ptr.state_ptr()).result == -libc::EAGAIN }
            && unsafe {
                let state = &mut *this.state_ptr.state_ptr();
                let result = state.result;
                let payload = state.retained_payload_mut::<RetainedProjectedWritevPayload<T>>();
                switch_writev_to_scalar(
                    &mut this.send_mode,
                    result,
                    retained_iovecs(&payload.scratch),
                    payload.skip,
                    remaining_iovec_count(&payload.scratch, payload.skip),
                )
            };
        if !this.state_ptr.state_ptr().is_null() && !switched {
            let state_ptr = this.state_ptr.state_ptr();
            match classify_retry_cqe_result(unsafe { (*state_ptr).result }) {
                RetryCqeResult::KernelError(errno) => {
                    let payload = unsafe {
                        take_retained_payload_and_free_state::<RetainedProjectedWritevPayload<T>>(
                            &pctx,
                            &mut this.state_ptr,
                        )
                    };
                    return Poll::Ready((Err(io::Error::from_raw_os_error(errno)), payload.source));
                }
                RetryCqeResult::Zero => {
                    let payload = unsafe {
                        take_retained_payload_and_free_state::<RetainedProjectedWritevPayload<T>>(
                            &pctx,
                            &mut this.state_ptr,
                        )
                    };
                    return Poll::Ready((
                        Err(io::Error::from(io::ErrorKind::WriteZero)),
                        payload.source,
                    ));
                }
                RetryCqeResult::Bytes(n) => {
                    let completed = unsafe {
                        let payload = (*state_ptr)
                            .retained_payload_mut::<RetainedProjectedWritevPayload<T>>();
                        debug_assert!(n <= this.total - payload.written);
                        payload.written += n;
                        payload.written >= this.total
                    };
                    if completed {
                        let payload = unsafe {
                            take_retained_payload_and_free_state::<RetainedProjectedWritevPayload<T>>(
                                &pctx,
                                &mut this.state_ptr,
                            )
                        };
                        let written = payload.written;
                        return Poll::Ready((Ok(written), payload.source));
                    }

                    unsafe {
                        let payload = (*state_ptr)
                            .retained_payload_mut::<RetainedProjectedWritevPayload<T>>();
                        let mut skip = payload.skip;
                        advance_iovecs_in_place(
                            retained_iovecs_mut(&mut payload.scratch, this.iov_count),
                            &mut skip,
                            n,
                        );
                        payload.skip = skip;
                    }
                    #[cfg(debug_assertions)]
                    unsafe {
                        let stats = &mut (*pctx.runtime_state()).stats;
                        stats.writev_partial_continuations =
                            stats.writev_partial_continuations.saturating_add(1);
                    }
                    #[cfg(feature = "diagnostic-counters")]
                    unsafe {
                        Reactor::note_writev_partial_continuation(pctx.reactor());
                    }
                }
            }
        }

        if this.source.is_some() {
            match submit_initial_projected_writev(
                &pctx,
                &mut this.state_ptr,
                &mut this.source,
                this.iov_count,
                this.total,
            ) {
                Ok(()) => return Poll::Pending,
                Err((err, source)) => return Poll::Ready((Err(err), source)),
            }
        }

        debug_assert!(
            !this.state_ptr.state_ptr().is_null(),
            "transferred projected writev source must retain its retry state"
        );
        let state_ptr = this.state_ptr.state_ptr();
        unsafe { reset_existing_retry_state(&mut *state_ptr, pctx.owner_task()) };

        unsafe {
            if let Err(e) = submit_resubmitted_fd_sqe(&pctx, &this.state_ptr, |fd| {
                let payload =
                    (*state_ptr).retained_payload_mut::<RetainedProjectedWritevPayload<T>>();
                Ok(build_write_retry_entry(
                    this.send_mode,
                    fd,
                    &payload.scratch,
                    payload.skip,
                    &mut payload.msg,
                    state_ptr as u64,
                ))
            }) {
                let payload = take_retained_payload_and_free_state::<
                    RetainedProjectedWritevPayload<T>,
                >(&pctx, &mut this.state_ptr);
                return Poll::Ready((Err(e), payload.source));
            }
        }

        Poll::Pending
    }
}

impl<T: WritevProjection, S> Drop for WritevAllProjectedFuture<'_, T, S> {
    fn drop(&mut self) {
        unsafe { drop_fd_op_state_unchecked(&mut self.state_ptr) };
    }
}

// ---------------------------------------------------------------------------
// ReadvExactFuture
// ---------------------------------------------------------------------------

/// Scatter-read exactly the requested number of bytes into a vectored buffer
/// chain, re-submitting on partial reads and no-progress EAGAIN completions
/// with the same `iovec` array. Returns `UnexpectedEof` if the peer closes
/// before that many bytes arrive.
pub struct ReadvExactFuture<'a, const N: usize, S> {
    /// Completion state reused across sequential retry submissions.
    state_ptr: RuntimeFdOpState<'a>,
    /// Caller-owned mutable segment chain returned when the operation finishes.
    buffer: Option<IoBuffVecMut<N>>,
    /// Number of non-empty segment entries materialized into retained scratch.
    iov_count: usize,
    /// Total writable capacity across all segments, cached for pre-submit
    /// target validation and debug checks without re-walking the caller-owned
    /// chain.
    writable: usize,
    /// Descriptor captured for debug-build checks against `state_ptr`.
    /// Release builds store `RuntimeFd::INVALID`; SQE construction derives the
    /// live descriptor from `state_ptr`.
    fd: RawFd,
    /// Exact byte count required before the future can succeed.
    target: usize,
    /// Bytes already read into the destination chain.
    filled: usize,
    /// Index of the first still-active `iovec` entry after partial progress.
    skip: usize,
    /// Size of the initial clamped `iovec` window; `skip` advances within this
    /// fixed prefix after partial reads.
    window_iov_count: usize,
    /// Deferred validation error returned before any SQE submission.
    input_error: Option<io::Error>,
    /// Whether the deferred error is the intrinsic retained-iovec capacity
    /// rejection counted after poll-context validation.
    #[cfg(debug_assertions)]
    record_iovec_oversize_rejection: bool,
    /// Borrows the parent stream for the future lifetime.
    _marker: PhantomData<&'a mut S>,
}

impl<'a, const N: usize, S> ReadvExactFuture<'a, N, S> {
    pub(crate) fn new(
        fd_state: RuntimeFdOpState<'a>,
        buffer: IoBuffVecMut<N>,
        target: usize,
    ) -> Self {
        let fd = fd_state.debug_raw_fd();
        let (iov_count, writable, mut input_error) =
            match buffer.checked_read_iovec_count_and_writable_len() {
                Some((iov_count, writable)) => (iov_count, writable, None),
                None => (0, 0, Some(invalid_readv_aggregate())),
            };
        let target = if input_error.is_some() {
            0
        } else {
            match super::checked_read_len(target, writable) {
                Ok(target) => target as usize,
                Err(err) => {
                    input_error = Some(err);
                    0
                }
            }
        };
        let iovec_oversize = N > RETAINED_IOVEC_MAX_COUNT
            && input_error.is_none()
            && target != 0
            && iov_count > RETAINED_IOVEC_MAX_COUNT;
        if iovec_oversize {
            input_error = Some(retained_iovec_oversize_error());
        }

        Self {
            state_ptr: fd_state,
            buffer: Some(buffer),
            iov_count,
            writable,
            fd,
            target,
            filled: 0,
            skip: 0,
            window_iov_count: 0,
            input_error,
            #[cfg(debug_assertions)]
            record_iovec_oversize_rejection: iovec_oversize,
            _marker: PhantomData,
        }
    }
}

impl<const N: usize, S> Future for ReadvExactFuture<'_, N, S> {
    type Output = (io::Result<usize>, IoBuffVecMut<N>);

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = unsafe { self.get_unchecked_mut() };
        this.state_ptr
            .debug_assert_matches_raw_fd(this.fd, FdStateDiagnostic::Stream);

        if this.state_ptr.state_ptr().is_null()
            && let Some(err) = this.input_error.take()
        {
            #[cfg(debug_assertions)]
            let result = if this.record_iovec_oversize_rejection {
                validate_local_iovec_oversize_rejection(cx, Err(err))
            } else {
                validate_local_io_result(cx, Err(err))
            };
            #[cfg(not(debug_assertions))]
            let result = validate_local_io_result(cx, Err(err));
            let buffer = unsafe { opt_take(&mut this.buffer) };
            return Poll::Ready((result, buffer));
        }
        if this.state_ptr.state_ptr().is_null() && this.buffer.is_none() {
            return Poll::Pending;
        }

        if unsafe { retry_state_is_in_flight(cx, this.state_ptr.state_ptr()) } {
            return Poll::Pending;
        }

        if this.state_ptr.state_ptr().is_null() && this.target == 0 {
            let mut buffer = unsafe { opt_take(&mut this.buffer) };
            unsafe { buffer.distribute_written(0) };
            return Poll::Ready((validate_local_io_result(cx, Ok(0)), buffer));
        }

        let pctx = if this.state_ptr.state_ptr().is_null() {
            match poll_ctx_from_waker(cx) {
                Ok(pctx) => pctx,
                Err(err) => {
                    let mut buffer = unsafe { opt_take(&mut this.buffer) };
                    unsafe { buffer.distribute_written(this.filled) };
                    return Poll::Ready((Err(err), buffer));
                }
            }
        } else {
            match unsafe {
                retry_poll_ctx_or_rejected_payload_with::<RetainedReadvPayload<N>, _>(
                    cx,
                    &mut this.state_ptr,
                    |payload| take_readv_buffer_from_retained(payload),
                )
            } {
                Ok(pctx) => pctx,
                Err(mut buffer) => {
                    unsafe { buffer.distribute_written(this.filled) };
                    return Poll::Ready((
                        Err(io::Error::from(io::ErrorKind::NotConnected)),
                        buffer,
                    ));
                }
            }
        };

        if !this.state_ptr.state_ptr().is_null() {
            let state_ptr = this.state_ptr.state_ptr();
            match classify_retry_cqe_result(unsafe { (*state_ptr).result }) {
                RetryCqeResult::KernelError(libc::EAGAIN) => {}
                RetryCqeResult::KernelError(errno) => {
                    let mut buffer = unsafe {
                        take_retained_payload_with_and_free_state::<RetainedReadvPayload<N>, _>(
                            &pctx,
                            &mut this.state_ptr,
                            |payload| take_readv_buffer_from_retained(payload),
                        )
                    };
                    unsafe { buffer.distribute_written(this.filled) };
                    return Poll::Ready((Err(io::Error::from_raw_os_error(errno)), buffer));
                }
                RetryCqeResult::Zero => {
                    let mut buffer = unsafe {
                        take_retained_payload_with_and_free_state::<RetainedReadvPayload<N>, _>(
                            &pctx,
                            &mut this.state_ptr,
                            |payload| take_readv_buffer_from_retained(payload),
                        )
                    };
                    unsafe { buffer.distribute_written(this.filled) };
                    return Poll::Ready((
                        Err(io::Error::from(io::ErrorKind::UnexpectedEof)),
                        buffer,
                    ));
                }
                RetryCqeResult::Bytes(n) => {
                    debug_assert!(n <= this.target - this.filled);
                    this.filled += n;
                    if this.filled >= this.target {
                        let mut buffer = unsafe {
                            take_retained_payload_with_and_free_state::<RetainedReadvPayload<N>, _>(
                                &pctx,
                                &mut this.state_ptr,
                                |payload| take_readv_buffer_from_retained(payload),
                            )
                        };
                        unsafe { buffer.distribute_written(this.target) };
                        return Poll::Ready((Ok(this.target), buffer));
                    }

                    unsafe {
                        let payload =
                            (*state_ptr).retained_payload_mut::<RetainedReadvPayload<N>>();
                        advance_iovecs_in_place(
                            retained_iovecs_mut(&mut payload.scratch, this.iov_count),
                            &mut this.skip,
                            n,
                        );
                    }
                }
            }
        }
        if this.buffer.is_some() {
            debug_assert!(
                this.state_ptr.state_ptr().is_null(),
                "initial readv_exact state was published"
            );
            let guard = match unsafe { prepare_unsubmitted_op(&pctx) } {
                Some(guard) => guard,
                None => {
                    let mut buffer = unsafe { opt_take(&mut this.buffer) };
                    unsafe { buffer.distribute_written(this.filled) };
                    return Poll::Ready((Err(io::Error::from(io::ErrorKind::WouldBlock)), buffer));
                }
            };
            let state_ptr = guard.state_ptr();

            let retained_pool = unsafe { Reactor::retained_payload_pool_ptr(pctx.reactor()) };
            let scratch_init =
                match unsafe { (*retained_pool.as_ptr()).alloc_iovec_scratch_init(this.iov_count) }
                {
                    Ok(scratch_init) => scratch_init,
                    Err(err) => {
                        let mut buffer = unsafe { opt_take(&mut this.buffer) };
                        unsafe { buffer.distribute_written(this.filled) };
                        return Poll::Ready((Err(err), buffer));
                    }
                };

            let payload = unsafe {
                emplace_retained_readv_payload(retained_pool, &mut this.buffer, scratch_init)
            };
            unsafe {
                if let Err((e, payload)) = submit_initialized_retained_fd_sqe(
                    &pctx,
                    state_ptr,
                    &mut this.state_ptr,
                    payload,
                    |fd, payload| {
                        let actual_shape = payload.buffer.fill_read_iovecs_and_writable_len(
                            payload.scratch.as_uninit_slice_mut(),
                        )?;
                        if actual_shape != (this.iov_count, this.writable) {
                            return Err(invalid_read_iovec_shape());
                        }
                        let remaining = this.target - this.filled;
                        this.window_iov_count = clamp_iovecs_to_read_limit(
                            retained_iovecs_mut(&mut payload.scratch, this.iov_count),
                            this.skip,
                            remaining,
                        );
                        Ok(build_read_vectored_entry(
                            fd,
                            retained_iovecs(&payload.scratch),
                            this.skip,
                            this.window_iov_count,
                            &mut payload.msg,
                            state_ptr as u64,
                        ))
                    },
                ) {
                    let mut buffer = payload.buffer;
                    buffer.distribute_written(this.filled);
                    return Poll::Ready((Err(e), buffer));
                }
            }
            guard.disarm();

            return Poll::Pending;
        }

        let state_ptr = this.state_ptr.state_ptr();
        unsafe { reset_existing_retry_state(&mut *state_ptr, pctx.owner_task()) };

        unsafe {
            if let Err(e) = submit_resubmitted_fd_sqe(&pctx, &this.state_ptr, |fd| {
                let payload = (*state_ptr).retained_payload_mut::<RetainedReadvPayload<N>>();
                let remaining_iovs = this.window_iov_count - this.skip;
                Ok(build_read_vectored_entry(
                    fd,
                    retained_iovecs(&payload.scratch),
                    this.skip,
                    remaining_iovs,
                    &mut payload.msg,
                    state_ptr as u64,
                ))
            }) {
                let payload = take_retained_payload_and_free_state::<RetainedReadvPayload<N>>(
                    &pctx,
                    &mut this.state_ptr,
                );
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
        unsafe { drop_fd_op_state_unchecked(&mut self.state_ptr) };
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::net::send_sqe::test_support::sqe_prefix;
    use crate::runtime::buffer::IoBuffMut;
    use crate::runtime::executor::{CompletionDrainGuard, with_ringless_poll_context_for_test};
    #[cfg(not(miri))]
    use crate::runtime::executor::{Executor, ExecutorConfig};
    use crate::runtime::fd::{RuntimeFd, with_final_core_drop_hook_for_test};
    #[cfg(not(miri))]
    use crate::runtime::reactor::ReactorConfig;
    use crate::runtime::task::release_task;
    use io_uring::opcode;
    use std::cell::Cell;
    use std::rc::Rc;

    fn invalid_fd_capability() -> RuntimeFdOpState<'static> {
        RuntimeFd::from_fresh_raw_fd(RuntimeFd::INVALID)
            .lease()
            .into_op_state()
    }

    fn panic_on_final_fd_core_drop(_: RawFd) {
        panic!("final fd core drop panic");
    }

    fn initialized_iovec(base: *const u8, len: usize) -> MaybeUninit<libc::iovec> {
        MaybeUninit::new(libc::iovec {
            iov_base: base as *mut libc::c_void,
            iov_len: len,
        })
    }

    fn sentinel_message_header() -> MaybeUninit<libc::msghdr> {
        MaybeUninit::new(libc::msghdr {
            msg_name: NonNull::<u8>::dangling().as_ptr().cast(),
            msg_namelen: 17,
            msg_iov: NonNull::<libc::iovec>::dangling().as_ptr(),
            msg_iovlen: 19,
            msg_control: NonNull::<u8>::dangling().as_ptr().cast(),
            msg_controllen: 23,
            msg_flags: libc::MSG_TRUNC,
        })
    }

    fn assert_message_free_invalid_input(result: io::Result<()>) {
        let err = result.expect_err("shape should be rejected");
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
        assert!(
            err.get_ref().is_none(),
            "try-path shape errors must remain message-free"
        );
    }

    #[cfg(any(debug_assertions, feature = "test-support"))]
    mod writev_fallback_tests {
        use super::*;
        #[cfg(not(miri))]
        use crate::net::send_sqe::test_support::assert_entry_fields;
        use crate::net::send_sqe::test_support::{
            ExpectedScalarEntry, SocketEntryCapture, assert_rejected_scalar_owner,
        };
        use crate::runtime::executor::ExecutorOwner;
        use crate::runtime::op::completion_retirement_test_support::stage_payload;

        struct Source {
            bytes: Box<[u8; 12]>,
            drops: Rc<Cell<usize>>,
            calls: Rc<Cell<usize>>,
        }
        impl Drop for Source {
            fn drop(&mut self) {
                self.drops.set(self.drops.get() + 1);
            }
        }
        impl WritevProjection for Source {
            fn writev_count_and_len(&self) -> (usize, usize) {
                (3, 12)
            }
            fn project_writev<'a>(&'a self, pieces: &mut WritevPieces<'a>) -> io::Result<()> {
                self.calls.set(self.calls.get() + 1);
                for bytes in self.bytes.chunks_exact(4) {
                    pieces.push(bytes)?;
                }
                Ok(())
            }
        }
        impl write_buffer_chain_sealed::Sealed<3> for Source {
            fn write_iovec_count_and_len(&self) -> Option<(usize, usize)> {
                Some((3, 12))
            }
            fn fill_write_iovecs(
                &self,
                dst: &mut [MaybeUninit<libc::iovec>],
            ) -> io::Result<(usize, usize)> {
                for (slot, bytes) in dst.iter_mut().zip(self.bytes.chunks_exact(4)) {
                    *slot = initialized_iovec(bytes.as_ptr(), bytes.len());
                }
                Ok((3, 12))
            }
        }
        enum Subject {
            Direct(WritevAllFuture<'static, Source, 3, ()>),
            Projected(WritevAllProjectedFuture<'static, Source, ()>),
        }
        type Fields<'a> = (
            &'a mut RetainedIovecScratch,
            &'a mut MaybeUninit<libc::msghdr>,
            &'a mut usize,
            &'a mut usize,
        );
        impl Future for Subject {
            type Output = (io::Result<usize>, Source);
            fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
                match self.get_mut() {
                    Self::Direct(future) => Pin::new(future).poll(cx),
                    Self::Projected(future) => Pin::new(future).poll(cx),
                }
            }
        }
        impl Subject {
            fn state(&mut self) -> &mut RuntimeFdOpState<'static> {
                match self {
                    Self::Direct(f) => &mut f.state_ptr,
                    Self::Projected(f) => &mut f.state_ptr,
                }
            }
            // The fixture owns this retained payload; no kernel target accesses it.
            unsafe fn fields(&mut self) -> Fields<'_> {
                let state = self.state().state_ptr();
                match self {
                    Self::Direct(_) => {
                        let p = unsafe {
                            (*state).retained_payload_mut::<RetainedWritevPayload<Source>>()
                        };
                        (&mut p.scratch, &mut p.msg, &mut p.written, &mut p.skip)
                    }
                    Self::Projected(_) => {
                        let p = unsafe {
                            (*state)
                                .retained_payload_mut::<RetainedProjectedWritevPayload<Source>>()
                        };
                        (&mut p.scratch, &mut p.msg, &mut p.written, &mut p.skip)
                    }
                }
            }
            #[cfg(not(miri))]
            fn snapshot(&mut self) -> (usize, usize, [u8; 56]) {
                let (_, msg, written, skip) = unsafe { self.fields() };
                // Every byte was initialized after the payload reached final storage.
                let bytes = unsafe { msg.as_ptr().cast::<[u8; 56]>().read() };
                (*written, *skip, bytes)
            }
        }
        struct Witness {
            addr: *const u8,
            drops: Rc<Cell<usize>>,
            calls: Rc<Cell<usize>>,
        }
        impl Witness {
            fn returned(&self, source: Source, projected: bool) {
                assert_eq!(source.bytes.as_ptr(), self.addr);
                assert_eq!(&*source.bytes, b"abcdefghijkl");
                assert_eq!(self.calls.get(), usize::from(projected));
                assert_eq!(self.drops.get(), 0);
                drop(source);
                assert_eq!(self.drops.get(), 1);
            }
        }
        fn staged(
            projected: bool,
            owner: &Rc<ExecutorOwner>,
            fd: &RuntimeFd,
            prior: usize,
        ) -> (Subject, Witness) {
            let source = Source {
                bytes: Box::new(*b"abcdefghijkl"),
                drops: Rc::new(Cell::new(0)),
                calls: Rc::new(Cell::new(0)),
            };
            let witness = Witness {
                addr: source.bytes.as_ptr(),
                drops: Rc::clone(&source.drops),
                calls: Rc::clone(&source.calls),
            };
            let pool = unsafe { Reactor::retained_payload_pool_ptr(owner.reactor_ptr()) };
            let scratch = unsafe { (*pool.as_ptr()).alloc_iovec_scratch(3) }
                .expect("scratch allocation failed");
            let mut subject = if projected {
                let mut f =
                    WritevAllProjectedFuture::<_, ()>::new(fd.lease().into_op_state(), source);
                let payload = RetainedProjectedWritevPayload {
                    source: f.source.take().expect("source missing"),
                    scratch,
                    msg: MaybeUninit::uninit(),
                    written: prior,
                    skip: 0,
                };
                let saved =
                    unsafe { stage_payload(owner, &mut f.state_ptr, payload, -libc::EAGAIN, true) };
                project_retained_writev_payload(unsafe { &mut *saved.payload }, 3, 12)
                    .expect("projection failed");
                Subject::Projected(f)
            } else {
                let mut f = WritevAllFuture::<_, 3, ()>::new(fd.lease().into_op_state(), source);
                f.iov_count = 3;
                f.total = 12;
                let mut payload = RetainedWritevPayload {
                    buffer: f.buffer.take().expect("buffer missing"),
                    scratch,
                    msg: MaybeUninit::uninit(),
                    written: prior,
                    skip: 0,
                };
                write_buffer_chain_sealed::Sealed::fill_write_iovecs(
                    &payload.buffer,
                    payload.scratch.as_uninit_slice_mut(),
                )
                .expect("materialization failed");
                unsafe { stage_payload(owner, &mut f.state_ptr, payload, -libc::EAGAIN, true) };
                Subject::Direct(f)
            };
            let (scratch, msg, _, skip) = unsafe { subject.fields() };
            advance_iovecs_in_place(unsafe { retained_iovecs_mut(scratch, 3) }, skip, prior);
            // Initialize padding too; the scalar path must preserve all header bytes.
            unsafe { std::ptr::write_bytes(msg.as_mut_ptr().cast::<u8>(), 0xa5, 56) };
            assert_eq!(fd.strong_count_for_test(), 2);
            (subject, witness)
        }
        fn expected(
            fd: &RuntimeFd,
            subject: &mut Subject,
            witness: &Witness,
            prior: usize,
            len: u32,
        ) -> ExpectedScalarEntry {
            ExpectedScalarEntry {
                opcode: opcode::Send::CODE,
                fd: fd.raw_fd(),
                addr: unsafe { witness.addr.add(prior) },
                owner_addr: witness.addr,
                len,
                msg_flags: libc::MSG_NOSIGNAL as u32,
                user_data: subject.state().state_ptr() as u64,
            }
        }
        fn terminal(cx: &mut Context<'_>, subject: &mut Subject, kind: io::ErrorKind) -> Source {
            let capture = SocketEntryCapture::new();
            let Poll::Ready((result, source)) = Pin::new(&mut *subject).poll(cx) else {
                panic!("terminal completion remained pending")
            };
            assert_eq!(
                result.expect_err("terminal completion succeeded").kind(),
                kind
            );
            assert!(subject.state().state_ptr().is_null());
            assert!(Pin::new(subject).poll(cx).is_pending());
            capture.assert_empty();
            source
        }
        fn clean(owner: &Rc<ExecutorOwner>, fd: &RuntimeFd) {
            assert_eq!(fd.strong_count_for_test(), 1);
            assert_eq!(unsafe { (&*owner.reactor_ptr()).live_op_count() }, 0);
            assert_eq!(owner.inflight_op_count_for_test(), 0);
        }
        #[test]
        fn writev_all_fallback_clamps_segments_above_u32_max() {
            for projected in [false, true] {
                for length in [1usize << 32, (1usize << 32) + 4] {
                    with_ringless_poll_context_for_test(1, |owner, cx| {
                        let fd = RuntimeFd::from_fresh_raw_fd(-7);
                        let (mut subject, witness) = staged(projected, owner, &fd, 8);
                        let (scratch, _, _, _) = unsafe { subject.fields() };
                        unsafe { retained_iovecs_mut(scratch, 3)[2].iov_len = length };
                        // Only the encoded window is synthetic; rejection precedes submission.
                        let entry = expected(&fd, &mut subject, &witness, 8, i32::MAX as u32);
                        let source = assert_rejected_scalar_owner(cx, &mut subject, &entry);
                        witness.returned(source, projected);
                        clean(owner, &fd);
                    });
                }
            }
        }
        #[test]
        fn writev_all_single_segment_send_eagain_stays_terminal() {
            for projected in [false, true] {
                with_ringless_poll_context_for_test(1, |owner, cx| {
                    let fd = RuntimeFd::from_fresh_raw_fd(-7);
                    let (mut subject, witness) = staged(projected, owner, &fd, 8);
                    witness.returned(
                        terminal(cx, &mut subject, io::ErrorKind::WouldBlock),
                        projected,
                    );
                    clean(owner, &fd);
                });
            }
        }
        #[test]
        fn writev_all_fallback_submission_failure_returns_owner() {
            for projected in [false, true] {
                with_ringless_poll_context_for_test(1, |owner, cx| {
                    let fd = RuntimeFd::from_fresh_raw_fd(-7);
                    let (mut subject, witness) = staged(projected, owner, &fd, 1);
                    let entry = expected(&fd, &mut subject, &witness, 1, 3);
                    let source = assert_rejected_scalar_owner(cx, &mut subject, &entry);
                    witness.returned(source, projected);
                    clean(owner, &fd);
                });
            }
        }
        #[test]
        fn writev_all_fallback_rejected_context_returns_owner() {
            for projected in [false, true] {
                let (origin, fd, mut subject, witness) =
                    with_ringless_poll_context_for_test(1, |owner, _| {
                        let fd = RuntimeFd::from_fresh_raw_fd(-7);
                        let (subject, witness) = staged(projected, owner, &fd, 1);
                        (Rc::clone(owner), fd, subject, witness)
                    });
                with_ringless_poll_context_for_test(1, |_, cx| {
                    witness.returned(
                        terminal(cx, &mut subject, io::ErrorKind::NotConnected),
                        projected,
                    );
                });
                clean(&origin, &fd);
            }
        }
        #[cfg(not(miri))]
        fn pending(
            cx: &mut Context<'_>,
            subject: &mut Subject,
            expected: Option<ExpectedScalarEntry>,
        ) {
            let capture = SocketEntryCapture::new();
            #[cfg(debug_assertions)]
            let state = poll_ctx_from_waker(cx)
                .expect("context missing")
                .runtime_state();
            #[cfg(debug_assertions)]
            let before = unsafe { (*state).stats.sqe_submits };
            if let Poll::Ready((result, _)) = Pin::new(subject).poll(cx) {
                panic!("fallback returned instead of waiting: {result:?}");
            }
            #[cfg(debug_assertions)]
            assert_eq!(
                unsafe { (*state).stats.sqe_submits },
                before + usize::from(expected.is_some())
            );
            if let Some(e) = expected {
                assert_entry_fields(
                    &capture.take(),
                    e.opcode,
                    e.fd,
                    e.addr as u64,
                    e.len,
                    e.msg_flags,
                    e.user_data,
                );
            } else {
                capture.assert_empty();
            }
        }
        #[cfg(not(miri))]
        async fn completed(subject: &mut Subject) -> *mut CompletionState {
            std::future::poll_fn(|_| {
                let state = subject.state().state_ptr();
                if unsafe { (*state).is_completed() } {
                    assert!(unsafe { (*state).waiter.is_null() });
                    Poll::Ready(state)
                } else {
                    Poll::Pending
                }
            })
            .await
        }
        #[cfg(not(miri))]
        async fn successful_switch(projected: bool, prior: usize, scalar_error: bool) {
            let (socket, _peer) = std::os::unix::net::UnixStream::pair().expect("pair failed");
            socket.set_nonblocking(true).expect("nonblocking failed");
            let fd = RuntimeFd::from_external_owned(socket.into());
            let (owner, mut subject, witness) = std::future::poll_fn(|cx| {
                let pctx = poll_ctx_from_waker(cx).expect("context missing");
                let owner = unsafe { ExecutorOwner::clone_rc(pctx.owner_ptr()) };
                let (mut subject, witness) = staged(projected, &owner, &fd, prior);
                let snapshot = subject.snapshot();
                let entry = expected(&fd, &mut subject, &witness, prior, (4 - prior % 4) as u32);
                pending(cx, &mut subject, Some(entry));
                assert_eq!(subject.snapshot(), snapshot);
                assert_eq!(snapshot, (prior, prior / 4, [0xa5; 56]));
                assert_eq!(fd.strong_count_for_test(), 2);
                assert_eq!(witness.drops.get(), 0);
                pending(cx, &mut subject, None);
                Poll::Ready((owner, subject, witness))
            })
            .await;
            let source = if scalar_error {
                let state = completed(&mut subject).await;
                assert_eq!(unsafe { (*state).result }, 4);
                // The real target has retired; plant partial progress without a live kernel owner.
                unsafe { (*state).result = 2 };
                std::future::poll_fn(|cx| {
                    let entry = expected(&fd, &mut subject, &witness, 2, 2);
                    pending(cx, &mut subject, Some(entry));
                    assert_eq!(subject.snapshot(), (2, 0, [0xa5; 56]));
                    pending(cx, &mut subject, None);
                    Poll::Ready(())
                })
                .await;
                let state = completed(&mut subject).await;
                assert_eq!(unsafe { (*state).result }, 2);
                unsafe { (*state).result = -libc::EAGAIN };
                std::future::poll_fn(|cx| {
                    Poll::Ready(terminal(cx, &mut subject, io::ErrorKind::WouldBlock))
                })
                .await
            } else {
                let (result, source) = subject.await;
                assert_eq!(result.expect("fallback write failed"), 12);
                source
            };
            witness.returned(source, projected);
            clean(&owner, &fd);
        }
        #[cfg(not(miri))]
        fn run(future: impl Future<Output = ()> + 'static) {
            Executor::new()
                .expect("executor failed")
                .run(future)
                .expect("fallback test failed");
        }
        #[cfg(not(miri))]
        #[test]
        fn writev_all_message_eagain_switches_to_scalar_send() {
            run(async {
                for projected in [false, true] {
                    for prior in [0, 1, 4] {
                        successful_switch(projected, prior, false).await;
                    }
                }
            });
        }
        #[cfg(not(miri))]
        #[test]
        fn writev_all_fallback_is_one_way_and_scalar_eagain_is_terminal() {
            run(async {
                for projected in [false, true] {
                    successful_switch(projected, 0, true).await;
                }
            });
        }
        #[cfg(not(miri))]
        #[test]
        fn writev_all_projected_fallback_projects_once() {
            run(successful_switch(true, 1, false));
        }
    }

    #[cfg(not(miri))]
    mod socket_receive_submission_tests {
        use super::*;
        use crate::net::unix::UnixStream;
        use std::io::Write;
        use std::os::fd::{FromRawFd, OwnedFd};

        fn fixture() -> (UnixStream, IoBuffMut, *const u8) {
            let mut fds = [-1; 2];
            // SAFETY: the array has room for both descriptors returned by pipe2.
            assert_eq!(
                unsafe { libc::pipe2(fds.as_mut_ptr(), libc::O_CLOEXEC | libc::O_NONBLOCK) },
                0,
                "pipe creation failed: {}",
                io::Error::last_os_error()
            );
            // SAFETY: successful pipe2 transfers two distinct fresh descriptors.
            let reader = unsafe { OwnedFd::from_raw_fd(fds[0]) };
            let writer = unsafe { OwnedFd::from_raw_fd(fds[1]) };
            std::fs::File::from(writer)
                .write_all(b"pipe")
                .expect("pipe data could not be queued");
            let mut buffer = IoBuffMut::new(2, 16, 0).expect("buffer allocation failed");
            buffer.payload_append(b"pre").expect("prefix did not fit");
            let original = buffer.as_ptr();
            (UnixStream::from_owned_fd(reader), buffer, original)
        }

        fn run(future: impl Future<Output = ()> + 'static) {
            Executor::new()
                .expect("executor creation failed")
                .run(future)
                .expect("receive test executor failed");
        }

        fn assert_pipe_rejection(
            result: io::Result<usize>,
            buffer: &IoBuffMut,
            original: *const u8,
            prefix: &[u8],
        ) {
            assert_eq!(
                result
                    .expect_err("socket receive accepted a pipe")
                    .raw_os_error(),
                Some(libc::ENOTSOCK)
            );
            assert_eq!(
                buffer.as_ptr(),
                original,
                "receive replaced the buffer owner"
            );
            assert_eq!(
                buffer.payload_bytes(),
                prefix,
                "receive lost initialized bytes"
            );
        }

        struct ResetReadBuffer {
            bytes: Vec<u8>,
            drops: Rc<Cell<usize>>,
        }

        // SAFETY: the Vec owns stable writable capacity across moves; the
        // receive completion initializes every byte before publishing it.
        unsafe impl IoBuffReadWrite for ResetReadBuffer {
            fn as_mut_ptr(&mut self) -> *mut u8 {
                self.bytes.as_mut_ptr()
            }

            fn writable_len(&self) -> usize {
                self.bytes.capacity()
            }

            unsafe fn set_written_len(&mut self, len: usize) {
                unsafe { IoBuffReadWrite::set_written_len(&mut self.bytes, len) };
            }
        }

        impl Drop for ResetReadBuffer {
            fn drop(&mut self) {
                self.drops.set(self.drops.get() + 1);
            }
        }

        async fn assert_exact_reset_after_progress<S, P>(
            mut future: ReadExactFuture<'_, ResetReadBuffer, S>,
            peer: P,
        ) {
            let buffer = future.buffer.as_ref().expect("read buffer missing");
            let original = buffer.bytes.as_ptr();
            let drops = Rc::clone(&buffer.drops);
            std::future::poll_fn(|cx| {
                assert!(Pin::new(&mut future).poll(cx).is_pending());
                match future.filled {
                    0 | 1 => Poll::Pending,
                    2 => Poll::Ready(()),
                    count => panic!("unexpected exact-read prefix length: {count}"),
                }
            })
            .await;
            assert_eq!(drops.get(), 0);
            drop(peer);

            let (result, buffer) = future.await;
            assert_eq!(
                result
                    .expect_err("peer reset was not reported")
                    .raw_os_error(),
                Some(libc::ECONNRESET)
            );
            assert_eq!(buffer.bytes, b"hi");
            assert_eq!(buffer.bytes.as_ptr(), original);
            assert_eq!(drops.get(), 0);
            drop(buffer);
            assert_eq!(drops.get(), 1);
        }

        #[test]
        fn unix_exact_read_reset_preserves_consumed_prefix_and_owner() {
            run(async {
                let (socket, mut peer) =
                    std::os::unix::net::UnixStream::pair().expect("Unix socket pair failed");
                socket
                    .set_nonblocking(true)
                    .expect("set receive nonblocking failed");
                peer.set_nonblocking(true)
                    .expect("set peer nonblocking failed");
                let mut stream = UnixStream::from_owned_fd(socket.into());
                let (result, _) = stream.try_write(b"unread".to_vec());
                assert_eq!(result.expect("queue unread peer data failed"), 6);
                peer.write_all(b"hi").expect("peer prefix write failed");
                let buffer = ResetReadBuffer {
                    bytes: b"seed".to_vec(),
                    drops: Rc::new(Cell::new(0)),
                };
                assert_exact_reset_after_progress(stream.read_exact(buffer, 4), peer).await;
            });
        }

        #[test]
        #[cfg(feature = "test-support")]
        fn tcp_exact_read_reset_preserves_consumed_prefix_and_owner() {
            run(async {
                let listener = std::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0))
                    .expect("TCP bind failed");
                let mut peer = std::net::TcpStream::connect(
                    listener.local_addr().expect("TCP address missing"),
                )
                .expect("TCP peer connect failed");
                let (socket, _) = listener.accept().expect("TCP accept failed");
                socket
                    .set_nonblocking(true)
                    .expect("set receive nonblocking failed");
                peer.set_nonblocking(true)
                    .expect("set peer nonblocking failed");
                crate::net::tls_test_peer::force_reset_on_drop(&peer);
                let mut stream = crate::net::tcp::TcpStream::from_owned_fd(socket.into());
                peer.write_all(b"hi").expect("peer prefix write failed");
                let buffer = ResetReadBuffer {
                    bytes: b"seed".to_vec(),
                    drops: Rc::new(Cell::new(0)),
                };
                assert_exact_reset_after_progress(stream.read_exact(buffer, 4), peer).await;
            });
        }

        // SAFETY: the payload owns its buffer and any positive result counts only
        // initialized bytes. The state is unsubmitted on this executor's reactor.
        unsafe fn publish_completed_result<T: 'static>(
            pctx: &PollCtx,
            fd_state: &mut RuntimeFdOpState<'_>,
            payload: RetainedPayload<T>,
            result: i32,
        ) {
            let state = unsafe { (&mut *pctx.reactor()).alloc_op() };
            assert!(!state.is_null(), "operation allocation failed");
            unsafe {
                (*state).attach_retained_payload(payload);
                (*state).attach_fd_lease(fd_state.take_initial_lease());
                (*state).result = result;
                (*state).set_completed();
                // No SQE refers to this completed state; the next poll submits
                // the retry with the same retained owner and descriptor lease.
                fd_state.publish_submitted_state(state);
            }
        }

        fn assert_pending_receive<F: Future + Unpin>(
            _pctx: &PollCtx,
            cx: &mut Context<'_>,
            future: &mut F,
            _submissions: usize,
        ) {
            #[cfg(debug_assertions)]
            let before = unsafe { (*_pctx.runtime_state()).stats.sqe_submits };
            assert!(Pin::new(future).poll(cx).is_pending());
            #[cfg(debug_assertions)]
            assert_eq!(
                unsafe { (*_pctx.runtime_state()).stats.sqe_submits },
                before + _submissions,
                "receive poll submitted an unexpected number of SQEs"
            );
        }

        #[test]
        fn read_initial_rejects_pipe_and_returns_owner() {
            run(async {
                let (mut stream, buffer, original) = fixture();
                let (result, buffer) = stream.read(buffer, 4).await;
                assert_pipe_rejection(result, &buffer, original, b"pre");
            });
        }

        #[test]
        fn read_eagain_resubmission_rejects_pipe_and_returns_owner() {
            run(async {
                let (mut stream, buffer, original) = fixture();
                let mut future = stream.read(buffer, 4);
                std::future::poll_fn(|cx| {
                    let pctx = poll_ctx_from_waker(cx).expect("poll context missing");
                    let payload = RetainedReadPayload {
                        buffer: future.buffer.take().expect("read buffer missing"),
                    };
                    // SAFETY: no SQE refers to this completed state; the next
                    // poll submits the receive against the adopted pipe.
                    unsafe {
                        let retained = (&mut *pctx.reactor()).alloc_retained_payload(payload);
                        publish_completed_result(
                            &pctx,
                            &mut future.state_ptr,
                            retained,
                            -libc::EAGAIN,
                        );
                    }
                    assert_pending_receive(&pctx, cx, &mut future, 1);
                    Poll::Ready(())
                })
                .await;
                let (result, buffer) = future.await;
                assert_pipe_rejection(result, &buffer, original, b"pre");
            });
        }

        #[test]
        fn read_exact_initial_rejects_pipe_and_returns_owner() {
            run(async {
                let (mut stream, buffer, original) = fixture();
                let (result, buffer) = stream.read_exact(buffer, 4).await;
                assert_pipe_rejection(result, &buffer, original, b"pre");
            });
        }

        #[test]
        fn read_exact_retry_rejects_pipe_and_preserves_progress() {
            run(async {
                let (mut stream, buffer, original) = fixture();
                let mut future = stream.read_exact(buffer, 4);
                std::future::poll_fn(|cx| {
                    let pctx = poll_ctx_from_waker(cx).expect("poll context missing");
                    let mut payload = RetainedReadPayload {
                        buffer: future.buffer.take().expect("read buffer missing"),
                    };
                    future.base_ptr = payload.buffer.as_mut_ptr();
                    // SAFETY: the writable region has at least four bytes. This
                    // byte models one consumed CQE before the real retry SQE.
                    unsafe {
                        future.base_ptr.write(b'x');
                        let retained = (&mut *pctx.reactor()).alloc_retained_payload(payload);
                        publish_completed_result(&pctx, &mut future.state_ptr, retained, 1);
                    }
                    assert!(Pin::new(&mut future).poll(cx).is_pending());
                    assert_eq!(future.filled, 1);
                    Poll::Ready(())
                })
                .await;
                let (result, buffer) = future.await;
                assert_pipe_rejection(result, &buffer, original, b"prex");
            });
        }

        #[test]
        fn readv_single_initial_rejects_pipe_and_returns_owner() {
            run(async {
                let (mut stream, buffer, original) = fixture();
                let (result, chain) = stream.readv(IoBuffVecMut::from_array([buffer])).await;
                assert_pipe_rejection(
                    result,
                    chain.get(0).expect("segment missing"),
                    original,
                    b"pre",
                );
            });
        }

        #[test]
        fn readv_exact_single_initial_rejects_pipe_and_returns_owner() {
            run(async {
                let (mut stream, buffer, original) = fixture();
                let (result, chain) = stream
                    .readv_exact(IoBuffVecMut::from_array([buffer]), 4)
                    .await;
                assert_pipe_rejection(
                    result,
                    chain.get(0).expect("segment missing"),
                    original,
                    b"pre",
                );
            });
        }

        #[test]
        fn readv_exact_single_retry_rejects_pipe_and_preserves_progress() {
            run(async {
                let (mut stream, buffer, original) = fixture();
                let mut future = stream.readv_exact(IoBuffVecMut::from_array([buffer]), 4);
                std::future::poll_fn(|cx| {
                    let pctx = poll_ctx_from_waker(cx).expect("poll context missing");
                    let pool = unsafe { Reactor::retained_payload_pool_ptr(pctx.reactor()) };
                    let scratch = unsafe { (*pool.as_ptr()).alloc_iovec_scratch(1) }
                        .expect("iovec scratch allocation failed");
                    let mut payload = RetainedReadvPayload {
                        buffer: future.buffer.take().expect("readv buffer missing"),
                        scratch,
                        msg: sentinel_message_header(),
                    };
                    assert_eq!(
                        payload
                            .buffer
                            .fill_read_iovecs_and_writable_len(
                                payload.scratch.as_uninit_slice_mut()
                            )
                            .expect("readv shape invalid"),
                        (future.iov_count, future.writable)
                    );
                    // SAFETY: the initialized iovec describes the buffer's
                    // writable region. No operation has been submitted yet.
                    unsafe {
                        future.window_iov_count = clamp_iovecs_to_read_limit(
                            retained_iovecs_mut(&mut payload.scratch, future.iov_count),
                            0,
                            4,
                        );
                        let iov = iovec_slice_ref(retained_iovecs(&payload.scratch), 0);
                        (iov.iov_base as *mut u8).write(b'x');
                        let retained = (&mut *pctx.reactor()).alloc_retained_payload(payload);
                        publish_completed_result(&pctx, &mut future.state_ptr, retained, 1);
                    }
                    assert!(Pin::new(&mut future).poll(cx).is_pending());
                    assert_eq!(future.filled, 1);
                    assert_eq!(future.skip, 0);
                    Poll::Ready(())
                })
                .await;
                let (result, chain) = future.await;
                assert_pipe_rejection(
                    result,
                    chain.get(0).expect("segment missing"),
                    original,
                    b"prex",
                );
            });
        }

        fn socket_fixture() -> (RuntimeFd, std::os::unix::net::UnixStream) {
            let (socket, peer) =
                std::os::unix::net::UnixStream::pair().expect("Unix socket pair failed");
            socket
                .set_nonblocking(true)
                .expect("set nonblocking failed");
            (RuntimeFd::from_external_owned(socket.into()), peer)
        }

        // SAFETY: the state owns a payload of T and no submitted operation has
        // access to its completion metadata during this synchronous poll.
        unsafe fn retry_identity<T: 'static>(
            fd_state: &RuntimeFdOpState<'_>,
        ) -> (*mut CompletionState, *const T, *const ()) {
            let state = fd_state.state_ptr();
            assert!(!state.is_null(), "receive lost its completion state");
            unsafe {
                assert!((*state).has_fd_lease(), "receive lost its descriptor lease");
                (
                    state,
                    (*state).retained_payload::<T>() as *const T,
                    (*state).fd_lease_core_ptr().as_ptr().cast(),
                )
            }
        }

        fn eagain_scalar_buffer() -> ResetReadBuffer {
            ResetReadBuffer {
                bytes: b"seed".to_vec(),
                drops: Rc::new(Cell::new(0)),
            }
        }

        #[test]
        fn read_eagain_retains_owner_and_lease_until_data_or_eof() {
            run(async {
                for data in [false, true] {
                    let (fd, mut peer) = socket_fixture();
                    let weak = fd.weak_for_test();
                    let buffer = eagain_scalar_buffer();
                    let original = buffer.bytes.as_ptr();
                    let drops = Rc::clone(&buffer.drops);
                    let mut future =
                        ReadFuture::<_, ()>::new(fd.lease().into_op_state(), buffer, 4);
                    drop(fd);
                    std::future::poll_fn(|cx| {
                        let pctx = poll_ctx_from_waker(cx).expect("poll context missing");
                        let payload = RetainedReadPayload {
                            buffer: future.buffer.take().expect("read buffer missing"),
                        };
                        // SAFETY: this is an unsubmitted state on the current reactor.
                        unsafe {
                            let retained = (&mut *pctx.reactor()).alloc_retained_payload(payload);
                            publish_completed_result(
                                &pctx,
                                &mut future.state_ptr,
                                retained,
                                -libc::EAGAIN,
                            );
                            let before = retry_identity::<RetainedReadPayload<ResetReadBuffer>>(
                                &future.state_ptr,
                            );
                            for submissions in [1, 0] {
                                assert_pending_receive(&pctx, cx, &mut future, submissions);
                                assert_eq!(
                                    retry_identity::<RetainedReadPayload<ResetReadBuffer>>(
                                        &future.state_ptr
                                    ),
                                    before
                                );
                                assert!(!(*before.0).is_completed());
                                assert_eq!(
                                    (*before.0)
                                        .retained_payload::<RetainedReadPayload<ResetReadBuffer>>()
                                        .buffer
                                        .bytes,
                                    b"seed"
                                );
                                assert_eq!(weak.strong_count(), 1);
                                assert_eq!(drops.get(), 0);
                            }
                        }
                        Poll::Ready(())
                    })
                    .await;
                    if data {
                        peer.write_all(b"x").expect("peer write failed");
                    }
                    peer.shutdown(std::net::Shutdown::Write)
                        .expect("peer shutdown failed");
                    let (result, buffer) = future.await;
                    assert_eq!(
                        result.expect("receive after EAGAIN failed"),
                        usize::from(data)
                    );
                    assert_eq!(
                        buffer.bytes.as_slice(),
                        if data {
                            b"x".as_slice()
                        } else {
                            b"seed".as_slice()
                        }
                    );
                    assert_eq!(buffer.bytes.as_ptr(), original);
                    assert_eq!(weak.strong_count(), 0);
                    assert_eq!(drops.get(), 0);
                    drop(buffer);
                    assert_eq!(drops.get(), 1);
                }
            });
        }

        #[test]
        fn read_exact_eagain_preserves_prior_prefix_until_data_or_eof() {
            run(async {
                for prior in [0, 1] {
                    for data in [false, true] {
                        let (fd, mut peer) = socket_fixture();
                        let weak = fd.weak_for_test();
                        let buffer = eagain_scalar_buffer();
                        let original = buffer.bytes.as_ptr();
                        let drops = Rc::clone(&buffer.drops);
                        let mut future =
                            ReadExactFuture::<_, ()>::new(fd.lease().into_op_state(), buffer, 4);
                        drop(fd);
                        std::future::poll_fn(|cx| {
                            let pctx = poll_ctx_from_waker(cx).expect("poll context missing");
                            let mut payload = RetainedReadPayload {
                                buffer: future.buffer.take().expect("read buffer missing"),
                            };
                            future.base_ptr = payload.buffer.as_mut_ptr();
                            future.filled = prior;
                            // SAFETY: the writable window contains four bytes and no
                            // kernel operation uses it while prior progress is planted.
                            unsafe {
                                std::ptr::copy_nonoverlapping(
                                    b"abcd".as_ptr(),
                                    future.base_ptr,
                                    prior as usize,
                                );
                                let retained =
                                    (&mut *pctx.reactor()).alloc_retained_payload(payload);
                                publish_completed_result(
                                    &pctx,
                                    &mut future.state_ptr,
                                    retained,
                                    -libc::EAGAIN,
                                );
                                let before = retry_identity::<RetainedReadPayload<ResetReadBuffer>>(
                                    &future.state_ptr,
                                );
                                for submissions in [1, 0] {
                                    assert_pending_receive(&pctx, cx, &mut future, submissions);
                                    assert_eq!(future.filled, prior);
                                    assert_eq!(
                                        retry_identity::<RetainedReadPayload<ResetReadBuffer>>(
                                            &future.state_ptr
                                        ),
                                        before
                                    );
                                    assert!(!(*before.0).is_completed());
                                    assert_eq!(weak.strong_count(), 1);
                                    assert_eq!(drops.get(), 0);
                                }
                            }
                            Poll::Ready(())
                        })
                        .await;
                        if data {
                            peer.write_all(&b"abcd"[prior as usize..])
                                .expect("peer write failed");
                        }
                        peer.shutdown(std::net::Shutdown::Write)
                            .expect("peer shutdown failed");
                        let (result, buffer) = future.await;
                        if data {
                            assert_eq!(result.expect("exact receive after EAGAIN failed"), 4);
                        } else {
                            assert_eq!(
                                result.expect_err("partial EOF accepted").kind(),
                                io::ErrorKind::UnexpectedEof
                            );
                        }
                        assert_eq!(
                            buffer.bytes.as_slice(),
                            if !data && prior == 0 {
                                b"seed".as_slice()
                            } else {
                                &b"abcd"[..if data { 4 } else { prior as usize }]
                            }
                        );
                        assert_eq!(buffer.bytes.as_ptr(), original);
                        assert_eq!(weak.strong_count(), 0);
                        assert_eq!(drops.get(), 0);
                        drop(buffer);
                        assert_eq!(drops.get(), 1);
                    }
                }
            });
        }

        fn eagain_chain(active: usize) -> IoBuffVecMut<2> {
            assert!((1..=2).contains(&active));
            let mut first = IoBuffMut::new(2, if active == 1 { 7 } else { 5 }, 0)
                .expect("first segment allocation failed");
            first.payload_append(b"pre").expect("first prefix failed");
            let mut second = IoBuffMut::new(1, if active == 1 { 1 } else { 3 }, 0)
                .expect("second segment allocation failed");
            second.payload_append(b"q").expect("second prefix failed");
            IoBuffVecMut::from_array([first, second])
        }

        fn chain_pointers(chain: &IoBuffVecMut<2>) -> [*const u8; 2] {
            std::array::from_fn(|index| chain.get(index).expect("segment missing").as_ptr())
        }

        fn assert_chain_prefix(
            chain: &IoBuffVecMut<2>,
            original: [*const u8; 2],
            active: usize,
            data: &[u8],
        ) {
            assert_eq!(chain_pointers(chain), original);
            let first_count = data.len().min(if active == 1 { 4 } else { 2 });
            assert_eq!(
                chain.get(0).expect("first segment missing").payload_bytes(),
                [b"pre".as_slice(), &data[..first_count]].concat()
            );
            assert_eq!(
                chain
                    .get(1)
                    .expect("second segment missing")
                    .payload_bytes(),
                [b"q".as_slice(), &data[first_count..]].concat()
            );
        }

        fn readv_payload(pctx: &PollCtx, mut buffer: IoBuffVecMut<2>) -> RetainedReadvPayload<2> {
            let (count, writable) = buffer
                .checked_read_iovec_count_and_writable_len()
                .expect("readv shape invalid");
            let pool = unsafe { Reactor::retained_payload_pool_ptr(pctx.reactor()) };
            let mut scratch = unsafe { (*pool.as_ptr()).alloc_iovec_scratch(count) }
                .expect("iovec scratch allocation failed");
            assert_eq!(
                buffer
                    .fill_read_iovecs_and_writable_len(scratch.as_uninit_slice_mut())
                    .expect("readv materialization failed"),
                (count, writable)
            );
            RetainedReadvPayload {
                buffer,
                scratch,
                msg: sentinel_message_header(),
            }
        }

        // SAFETY: every scratch entry is initialized and points to its retained
        // buffer's writable window; no submitted operation accesses it.
        unsafe fn write_vector_prefix(payload: &mut RetainedReadvPayload<2>, count: usize) {
            let mut copied = 0;
            for iov in unsafe { retained_iovecs_mut(&mut payload.scratch, 2) } {
                let take = (count - copied).min(iov.iov_len);
                unsafe {
                    std::ptr::copy_nonoverlapping(
                        b"abcd".as_ptr().add(copied),
                        iov.iov_base.cast(),
                        take,
                    )
                };
                copied += take;
                if copied == count {
                    break;
                }
            }
            assert_eq!(copied, count);
        }

        #[test]
        fn readv_eagain_retains_owner_and_lease_for_both_opcodes() {
            run(async {
                for active in [1, 2] {
                    for data in [false, true] {
                        let (fd, mut peer) = socket_fixture();
                        let weak = fd.weak_for_test();
                        let chain = eagain_chain(active);
                        let original = chain_pointers(&chain);
                        let mut future =
                            ReadvFuture::<2, ()>::new(fd.lease().into_op_state(), chain);
                        drop(fd);
                        std::future::poll_fn(|cx| {
                            let pctx = poll_ctx_from_waker(cx).expect("poll context missing");
                            let payload = readv_payload(
                                &pctx,
                                future.buffer.take().expect("readv buffer missing"),
                            );
                            // SAFETY: this is an unsubmitted state on the current reactor.
                            unsafe {
                                let retained =
                                    (&mut *pctx.reactor()).alloc_retained_payload(payload);
                                publish_completed_result(
                                    &pctx,
                                    &mut future.state_ptr,
                                    retained,
                                    -libc::EAGAIN,
                                );
                                let before =
                                    retry_identity::<RetainedReadvPayload<2>>(&future.state_ptr);
                                for submissions in [1, 0] {
                                    assert_pending_receive(&pctx, cx, &mut future, submissions);
                                    assert_eq!(
                                        retry_identity::<RetainedReadvPayload<2>>(
                                            &future.state_ptr
                                        ),
                                        before
                                    );
                                    assert!(!(*before.0).is_completed());
                                    assert_chain_prefix(
                                        &(*before.0)
                                            .retained_payload::<RetainedReadvPayload<2>>()
                                            .buffer,
                                        original,
                                        active,
                                        b"",
                                    );
                                    assert_eq!(weak.strong_count(), 1);
                                }
                            }
                            Poll::Ready(())
                        })
                        .await;
                        if data {
                            peer.write_all(b"a").expect("peer write failed");
                        }
                        peer.shutdown(std::net::Shutdown::Write)
                            .expect("peer shutdown failed");
                        let (result, chain) = future.await;
                        assert_eq!(
                            result.expect("vectored receive after EAGAIN failed"),
                            usize::from(data)
                        );
                        assert_chain_prefix(
                            &chain,
                            original,
                            active,
                            if data {
                                b"a".as_slice()
                            } else {
                                b"".as_slice()
                            },
                        );
                        assert_eq!(weak.strong_count(), 0);
                    }
                }
            });
        }

        #[test]
        fn readv_exact_eagain_preserves_progress_and_remaining_window() {
            run(async {
                for active in [1, 2] {
                    for prior in [0, 1, 2] {
                        for data in [false, true] {
                            let (fd, mut peer) = socket_fixture();
                            let weak = fd.weak_for_test();
                            let chain = eagain_chain(active);
                            let original = chain_pointers(&chain);
                            let mut future = ReadvExactFuture::<2, ()>::new(
                                fd.lease().into_op_state(),
                                chain,
                                4,
                            );
                            drop(fd);
                            std::future::poll_fn(|cx| {
                                let pctx = poll_ctx_from_waker(cx).expect("poll context missing");
                                let mut payload = readv_payload(
                                    &pctx,
                                    future.buffer.take().expect("readv buffer missing"),
                                );
                                // SAFETY: the active iovecs cover exactly four writable
                                // bytes. Prior progress is initialized before publication.
                                unsafe {
                                    future.window_iov_count = clamp_iovecs_to_read_limit(
                                        retained_iovecs_mut(&mut payload.scratch, active),
                                        0,
                                        4,
                                    );
                                    if prior != 0 {
                                        // A single active entry has enough capacity for
                                        // the entire prefix; the second entry is unused.
                                        let first =
                                            iovec_slice_ref(retained_iovecs(&payload.scratch), 0);
                                        std::ptr::copy_nonoverlapping(
                                            b"abcd".as_ptr(),
                                            first.iov_base.cast(),
                                            prior,
                                        );
                                    }
                                    advance_iovecs_in_place(
                                        retained_iovecs_mut(&mut payload.scratch, active),
                                        &mut future.skip,
                                        prior,
                                    );
                                    future.filled = prior;
                                    let retained =
                                        (&mut *pctx.reactor()).alloc_retained_payload(payload);
                                    publish_completed_result(
                                        &pctx,
                                        &mut future.state_ptr,
                                        retained,
                                        -libc::EAGAIN,
                                    );
                                    let before = retry_identity::<RetainedReadvPayload<2>>(
                                        &future.state_ptr,
                                    );
                                    let skip = future.skip;
                                    for submissions in [1, 0] {
                                        assert_pending_receive(&pctx, cx, &mut future, submissions);
                                        assert_eq!((future.filled, future.skip), (prior, skip));
                                        assert_eq!(
                                            retry_identity::<RetainedReadvPayload<2>>(
                                                &future.state_ptr
                                            ),
                                            before
                                        );
                                        assert!(!(*before.0).is_completed());
                                        assert_eq!(weak.strong_count(), 1);
                                    }
                                }
                                Poll::Ready(())
                            })
                            .await;
                            if data {
                                peer.write_all(&b"abcd"[prior..])
                                    .expect("peer write failed");
                            }
                            peer.shutdown(std::net::Shutdown::Write)
                                .expect("peer shutdown failed");
                            let (result, chain) = future.await;
                            if data {
                                assert_eq!(
                                    result.expect("vectored exact receive after EAGAIN failed"),
                                    4
                                );
                            } else {
                                assert_eq!(
                                    result.expect_err("partial EOF accepted").kind(),
                                    io::ErrorKind::UnexpectedEof
                                );
                            }
                            assert_chain_prefix(
                                &chain,
                                original,
                                active,
                                &b"abcd"[..if data { 4 } else { prior }],
                            );
                            assert_eq!(weak.strong_count(), 0);
                        }
                    }
                }
            });
        }

        #[test]
        fn readv_multi_initial_rejects_pipe_and_returns_owners() {
            run(async {
                let (mut stream, _, _) = fixture();
                let chain = eagain_chain(2);
                let original = chain_pointers(&chain);
                let (result, chain) = stream.readv(chain).await;
                assert_eq!(
                    result.expect_err("RECVMSG accepted a pipe").raw_os_error(),
                    Some(libc::ENOTSOCK)
                );
                assert_chain_prefix(&chain, original, 2, b"");
            });
        }

        #[test]
        fn readv_eagain_resubmission_rejects_pipe_for_both_opcodes() {
            run(async {
                for active in [1, 2] {
                    let (mut stream, _, _) = fixture();
                    let chain = eagain_chain(active);
                    let original = chain_pointers(&chain);
                    let mut future = stream.readv(chain);
                    std::future::poll_fn(|cx| {
                        let pctx = poll_ctx_from_waker(cx).expect("poll context missing");
                        let payload = readv_payload(
                            &pctx,
                            future.buffer.take().expect("readv buffer missing"),
                        );
                        // SAFETY: no SQE refers to this completed state; the
                        // real resubmission must use RECV or RECVMSG on the pipe.
                        unsafe {
                            let retained = (&mut *pctx.reactor()).alloc_retained_payload(payload);
                            publish_completed_result(
                                &pctx,
                                &mut future.state_ptr,
                                retained,
                                -libc::EAGAIN,
                            );
                        }
                        assert_pending_receive(&pctx, cx, &mut future, 1);
                        Poll::Ready(())
                    })
                    .await;
                    let (result, chain) = future.await;
                    assert_eq!(
                        result
                            .expect_err("socket receive retry accepted a pipe")
                            .raw_os_error(),
                        Some(libc::ENOTSOCK)
                    );
                    assert_chain_prefix(&chain, original, active, b"");
                }
            });
        }

        #[test]
        fn readv_exact_multi_initial_rejects_pipe_and_returns_owners() {
            run(async {
                let (mut stream, _, _) = fixture();
                let chain = eagain_chain(2);
                let original = chain_pointers(&chain);
                let (result, chain) = stream.readv_exact(chain, 4).await;
                assert_eq!(
                    result.expect_err("RECVMSG accepted a pipe").raw_os_error(),
                    Some(libc::ENOTSOCK)
                );
                assert_chain_prefix(&chain, original, 2, b"");
            });
        }

        #[test]
        fn readv_exact_retry_rejects_pipe_before_and_after_single_window_transition() {
            run(async {
                for prior in [1, 2] {
                    let (mut stream, _, _) = fixture();
                    let chain = eagain_chain(2);
                    let original = chain_pointers(&chain);
                    let mut future = stream.readv_exact(chain, 4);
                    std::future::poll_fn(|cx| {
                        let pctx = poll_ctx_from_waker(cx).expect("poll context missing");
                        let mut payload = readv_payload(
                            &pctx,
                            future.buffer.take().expect("readv buffer missing"),
                        );
                        // SAFETY: the planted positive completion describes exactly
                        // the initialized prefix in the unsubmitted writable window.
                        unsafe {
                            future.window_iov_count = clamp_iovecs_to_read_limit(
                                retained_iovecs_mut(&mut payload.scratch, 2),
                                0,
                                4,
                            );
                            write_vector_prefix(&mut payload, prior);
                            let retained = (&mut *pctx.reactor()).alloc_retained_payload(payload);
                            publish_completed_result(
                                &pctx,
                                &mut future.state_ptr,
                                retained,
                                prior as i32,
                            );
                        }
                        assert!(Pin::new(&mut future).poll(cx).is_pending());
                        assert_eq!(future.filled, prior);
                        assert_eq!(future.skip, usize::from(prior == 2));
                        Poll::Ready(())
                    })
                    .await;
                    let (result, chain) = future.await;
                    assert_eq!(
                        result
                            .expect_err("socket receive retry accepted a pipe")
                            .raw_os_error(),
                        Some(libc::ENOTSOCK)
                    );
                    assert_chain_prefix(&chain, original, 2, &b"abcd"[..prior]);
                }
            });
        }

        struct PanicReadBuffer {
            inner: ResetReadBuffer,
            pointer_calls: usize,
        }

        // SAFETY: the inner Vec owns stable writable capacity; the second
        // pointer request unwinds before returning a pointer or changing it.
        unsafe impl IoBuffReadWrite for PanicReadBuffer {
            fn as_mut_ptr(&mut self) -> *mut u8 {
                self.pointer_calls += 1;
                assert_eq!(self.pointer_calls, 1, "receive retry pointer panic");
                self.inner.as_mut_ptr()
            }

            fn writable_len(&self) -> usize {
                self.inner.writable_len()
            }

            unsafe fn set_written_len(&mut self, len: usize) {
                unsafe { self.inner.set_written_len(len) };
            }
        }

        #[test]
        fn read_eagain_pointer_panic_reclaims_completed_owner_and_lease() {
            run(async {
                let (fd, _peer) = socket_fixture();
                let weak = fd.weak_for_test();
                let buffer = PanicReadBuffer {
                    inner: eagain_scalar_buffer(),
                    pointer_calls: 0,
                };
                let drops = Rc::clone(&buffer.inner.drops);
                let mut future = ReadFuture::<_, ()>::new(fd.lease().into_op_state(), buffer, 4);
                let (reactor, before_ops) = std::future::poll_fn(|cx| {
                    let pctx = poll_ctx_from_waker(cx).expect("poll context missing");
                    let before_ops = unsafe { (&*pctx.reactor()).live_op_count() };
                    let mut payload = RetainedReadPayload {
                        buffer: future.buffer.take().expect("read buffer missing"),
                    };
                    let _ = payload.buffer.as_mut_ptr();
                    // SAFETY: the planted state is completed and no SQE refers
                    // to it. The second pointer callback unwinds before reset.
                    unsafe {
                        let retained = (&mut *pctx.reactor()).alloc_retained_payload(payload);
                        publish_completed_result(
                            &pctx,
                            &mut future.state_ptr,
                            retained,
                            -libc::EAGAIN,
                        );
                        let state = future.state_ptr.state_ptr();
                        let unwind = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                            let _ = Pin::new(&mut future).poll(cx);
                        }));
                        let panic = unwind.expect_err("retry pointer callback did not unwind");
                        let message = panic
                            .downcast_ref::<String>()
                            .expect("assertion panic message missing");
                        assert!(message.contains("receive retry pointer panic"));
                        assert_eq!(future.state_ptr.state_ptr(), state);
                        assert!((*state).is_completed());
                        assert_eq!(weak.strong_count(), 2);
                        assert_eq!(drops.get(), 0);
                    }
                    Poll::Ready((pctx.reactor(), before_ops))
                })
                .await;
                drop(future);
                assert_eq!(drops.get(), 1);
                assert_eq!(unsafe { (&*reactor).live_op_count() }, before_ops);
                assert_eq!(weak.strong_count(), 1);
                drop(fd);
                assert_eq!(weak.strong_count(), 0);
                assert_eq!(drops.get(), 1);
            });
        }

        #[test]
        fn cancelled_read_eagain_keeps_owner_until_original_completion_retires() {
            let drops = Rc::new(Cell::new(0));
            let after = Rc::clone(&drops);
            run(async move {
                let (fd, peer) = socket_fixture();
                let weak = fd.weak_for_test();
                let buffer = ResetReadBuffer {
                    bytes: b"seed".to_vec(),
                    drops: Rc::clone(&drops),
                };
                let mut future = ReadFuture::<_, ()>::new(fd.lease().into_op_state(), buffer, 4);
                drop(fd);
                std::future::poll_fn(|cx| {
                    let pctx = poll_ctx_from_waker(cx).expect("poll context missing");
                    let payload = RetainedReadPayload {
                        buffer: future.buffer.take().expect("read buffer missing"),
                    };
                    // SAFETY: the completed state is unsubmitted; the next poll
                    // starts the real operation whose target owns the buffer.
                    unsafe {
                        let retained = (&mut *pctx.reactor()).alloc_retained_payload(payload);
                        publish_completed_result(
                            &pctx,
                            &mut future.state_ptr,
                            retained,
                            -libc::EAGAIN,
                        );
                    }
                    assert_pending_receive(&pctx, cx, &mut future, 1);
                    Poll::Ready(())
                })
                .await;
                drop(future);
                assert_eq!(
                    drops.get(),
                    0,
                    "cancel freed a buffer before its target completion"
                );
                assert_eq!(weak.strong_count(), 1);
                drop(peer);
            });
            assert_eq!(
                after.get(),
                1,
                "executor shutdown did not retire the cancelled receive owner"
            );
        }

        fn poll_rejected_context<F: Future + Unpin>(
            foreign: bool,
            future: &mut F,
        ) -> Poll<F::Output> {
            if foreign {
                with_ringless_poll_context_for_test(1, |_owner, cx| Pin::new(future).poll(cx))
            } else {
                let mut cx = Context::from_waker(std::task::Waker::noop());
                Pin::new(future).poll(&mut cx)
            }
        }

        #[test]
        fn read_eagain_rejected_context_reclaims_origin_and_returns_owner() {
            for foreign in [false, true] {
                let (origin, mut future, original, drops, weak) =
                    with_ringless_poll_context_for_test(1, |owner, cx| {
                        let pctx = poll_ctx_from_waker(cx).expect("origin context missing");
                        let fd = RuntimeFd::from_fresh_raw_fd(RuntimeFd::INVALID);
                        let weak = fd.weak_for_test();
                        let buffer = eagain_scalar_buffer();
                        let original = buffer.bytes.as_ptr();
                        let drops = Rc::clone(&buffer.drops);
                        let mut future =
                            ReadFuture::<_, ()>::new(fd.lease().into_op_state(), buffer, 4);
                        drop(fd);
                        let payload = RetainedReadPayload {
                            buffer: future.buffer.take().expect("read owner missing"),
                        };
                        // SAFETY: no operation is submitted; the completed target
                        // retains its origin and buffer until the rejecting poll.
                        unsafe {
                            let retained = (&mut *pctx.reactor()).alloc_retained_payload(payload);
                            publish_completed_result(
                                &pctx,
                                &mut future.state_ptr,
                                retained,
                                -libc::EAGAIN,
                            );
                        }
                        (Rc::clone(owner), future, original, drops, weak)
                    });
                let Poll::Ready((result, buffer)) = poll_rejected_context(foreign, &mut future)
                else {
                    panic!("rejected read context attempted resubmission");
                };
                assert_eq!(
                    result.expect_err("invalid context accepted").kind(),
                    io::ErrorKind::NotConnected
                );
                assert_eq!(buffer.bytes.as_ptr(), original);
                assert_eq!(buffer.bytes, b"seed");
                assert!(future.state_ptr.state_ptr().is_null());
                assert_eq!(weak.strong_count(), 0);
                assert_eq!(drops.get(), 0);
                assert_eq!(unsafe { (&*origin.reactor_ptr()).live_op_count() }, 0);
                drop(future);
                assert_eq!(drops.get(), 0);
                drop(buffer);
                assert_eq!(drops.get(), 1);
            }
        }

        #[test]
        fn readv_eagain_rejected_context_reclaims_origin_and_returns_owners() {
            for foreign in [false, true] {
                let (origin, mut future, original, weak) =
                    with_ringless_poll_context_for_test(1, |owner, cx| {
                        let pctx = poll_ctx_from_waker(cx).expect("origin context missing");
                        let fd = RuntimeFd::from_fresh_raw_fd(RuntimeFd::INVALID);
                        let weak = fd.weak_for_test();
                        let chain = eagain_chain(2);
                        let original = chain_pointers(&chain);
                        let mut future =
                            ReadvFuture::<2, ()>::new(fd.lease().into_op_state(), chain);
                        drop(fd);
                        let payload = readv_payload(
                            &pctx,
                            future.buffer.take().expect("readv owners missing"),
                        );
                        // SAFETY: the retained chain belongs to this origin and
                        // no SQE refers to this planted completed target.
                        unsafe {
                            let retained = (&mut *pctx.reactor()).alloc_retained_payload(payload);
                            publish_completed_result(
                                &pctx,
                                &mut future.state_ptr,
                                retained,
                                -libc::EAGAIN,
                            );
                        }
                        (Rc::clone(owner), future, original, weak)
                    });
                let Poll::Ready((result, chain)) = poll_rejected_context(foreign, &mut future)
                else {
                    panic!("rejected readv context attempted resubmission");
                };
                assert_eq!(
                    result.expect_err("invalid context accepted").kind(),
                    io::ErrorKind::NotConnected
                );
                assert_chain_prefix(&chain, original, 2, b"");
                assert!(future.state_ptr.state_ptr().is_null());
                assert_eq!(weak.strong_count(), 0);
                assert_eq!(unsafe { (&*origin.reactor_ptr()).live_op_count() }, 0);
                let stats = unsafe { (&*origin.reactor_ptr()).retained_payload_stats() };
                assert_eq!(stats.pooled_allocs, 1);
                assert_eq!(stats.pooled_frees, 1);
            }
        }
    }

    #[cfg(all(target_arch = "x86_64", target_os = "linux"))]
    fn assert_retained_readv_class<const N: usize, const CLASS: usize>() {
        assert_eq!(std::mem::size_of::<RetainedReadvPayload<N>>(), 40 * N + 352);
        assert_eq!(std::mem::align_of::<RetainedReadvPayload<N>>(), 8);
        let mut pool = RetainedPayloadPool::new().expect("retained pool creation failed");
        if CLASS != 0 {
            let warm = pool.alloc([0u8; CLASS]);
            // SAFETY: this payload belongs to pool and has no external references.
            unsafe { warm.drop_and_free(&mut pool) };
        }
        let before = pool.stats();
        for extract in [false, true] {
            let mut chain = IoBuffVecMut::<N>::new();
            let original = if N == 0 {
                std::ptr::null()
            } else {
                let segment = IoBuffMut::new(0, 4, 0).expect("segment allocation failed");
                let original = segment.as_ptr();
                chain.push(segment).expect("segment did not fit");
                original
            };
            let mut buffer = Some(chain);
            let scratch = pool
                .alloc_iovec_scratch_init(usize::from(N != 0))
                .expect("scratch allocation failed");
            // SAFETY: the scratch count equals the chain's active count; the
            // handle is consumed below through the same live pool.
            let payload = unsafe {
                emplace_retained_readv_payload(NonNull::from(&mut pool), &mut buffer, scratch)
            };
            assert!(buffer.is_none());
            // The unsubmitted payload's message header remains uninitialized.
            if extract {
                let returned = unsafe {
                    payload.take_with(&mut pool, |ptr| take_readv_buffer_from_retained(ptr))
                };
                assert_eq!(returned.segments(), usize::from(N != 0));
                if N != 0 {
                    assert_eq!(returned.get(0).expect("segment missing").as_ptr(), original);
                }
                drop(returned);
            } else {
                unsafe { payload.drop_and_free(&mut pool) };
            }
        }
        let delta = pool.stats().saturating_delta_since(before);
        if CLASS == 0 {
            assert_eq!(delta.heap_fallbacks, 2, "N={N}");
            assert_eq!(delta.heap_frees, 2, "N={N}");
            assert_eq!(delta.pooled_allocs, 0, "N={N}");
            assert_eq!(delta.pooled_frees, 0, "N={N}");
        } else {
            assert_eq!(delta.pooled_allocs, 2, "N={N}");
            assert_eq!(delta.pooled_frees, 2, "N={N}");
            assert_eq!(delta.pooled_reuses, 2, "N={N}, expected class {CLASS}");
            assert_eq!(delta.heap_fallbacks, 0, "N={N}");
            assert_eq!(delta.heap_frees, 0, "N={N}");
        }
    }

    #[cfg(all(target_arch = "x86_64", target_os = "linux"))]
    #[test]
    fn retained_readv_payload_class_boundaries_and_releases() {
        macro_rules! check {
            ($($capacity:literal => $class:literal),+ $(,)?) => {
                $(assert_retained_readv_class::<$capacity, $class>();)+
            };
        }
        check!(
            0 => 512, 1 => 512, 2 => 512, 3 => 512, 4 => 512,
            5 => 1024, 6 => 1024, 8 => 1024, 16 => 1024,
            17 => 2048, 18 => 2048, 19 => 2048, 42 => 2048,
            43 => 4096, 44 => 4096, 93 => 4096,
            94 => 8192, 95 => 8192, 96 => 8192, 196 => 8192,
            197 => 16384, 198 => 16384, 400 => 16384,
            401 => 32768, 402 => 32768, 403 => 32768, 810 => 32768,
            811 => 65536, 812 => 65536, 1629 => 65536,
            1630 => 0, 1631 => 0, 1632 => 0,
        );
    }

    #[test]
    fn stream_read_vectored_reinitializes_every_message_header_field() {
        let bytes = [0u8; 4];
        let mut iovecs = [
            initialized_iovec(bytes.as_ptr(), 1),
            initialized_iovec(bytes.as_ptr(), 2),
            initialized_iovec(bytes.as_ptr(), 4),
        ];
        let mut msg = sentinel_message_header();
        for (skip, count, last_len) in [(0, 3, 4), (1, 2, 4), (2, 1, i32::MAX as usize + 1)] {
            // No operation is submitted; only the encoded window is inspected.
            iovecs[2] = initialized_iovec(bytes.as_ptr(), last_len);
            let entry = build_read_vectored_entry(7, &iovecs, skip, count, &mut msg, 99);
            let sqe = sqe_prefix(&entry);
            // SAFETY: each RECVMSG branch initializes every header field.
            let header = unsafe { msg.assume_init_mut() };
            assert_eq!(sqe.opcode, opcode::RecvMsg::CODE);
            assert_eq!(sqe.addr, (header as *mut libc::msghdr) as u64);
            assert_eq!(
                header.msg_iov,
                std::ptr::addr_of!(iovecs[skip]) as *mut libc::iovec
            );
            assert_eq!(header.msg_iovlen, count);
            assert!(header.msg_name.is_null());
            assert_eq!(header.msg_namelen, 0);
            assert!(header.msg_control.is_null());
            assert_eq!(header.msg_controllen, 0);
            assert_eq!(header.msg_flags, 0);
            // Completed receives may modify output fields. No operation uses
            // these sentinel values before the next builder resets the header.
            msg = sentinel_message_header();
        }
    }

    #[test]
    fn retained_readv_message_header_stays_at_final_address_after_future_moves() {
        use crate::runtime::buffer::pool::{IoBuffPool, IoBuffPoolConfig};

        with_ringless_poll_context_for_test(1, |owner, cx| {
            let mut buffers = IoBuffPool::new(IoBuffPoolConfig {
                headroom: 0,
                payload: 1,
                tailroom: 0,
                objs_per_slab: 2,
            })
            .expect("buffer pool creation failed");
            buffers.init();
            let chain = IoBuffVecMut::from_array(std::array::from_fn::<_, 2, _>(|_| {
                buffers.alloc().expect("buffer allocation failed")
            }));
            let originals = [
                chain.get(0).expect("first segment missing").as_ptr(),
                chain.get(1).expect("second segment missing").as_ptr(),
            ];
            let mut future = ReadvFuture::<2, ()>::new(invalid_fd_capability(), chain);
            let reactor = owner.reactor_ptr();
            let pool = unsafe { Reactor::retained_payload_pool_ptr(reactor) };
            let scratch = unsafe { (&mut *pool.as_ptr()).alloc_iovec_scratch_init(2) }
                .expect("scratch allocation failed");
            let mut retained =
                unsafe { emplace_retained_readv_payload(pool, &mut future.buffer, scratch) };
            let payload_address = retained.as_ptr();
            let entry = unsafe {
                let payload = retained.as_mut();
                assert_eq!(
                    payload
                        .buffer
                        .fill_read_iovecs_and_writable_len(payload.scratch.as_uninit_slice_mut())
                        .expect("iovec construction failed"),
                    (2, 2)
                );
                build_read_vectored_entry(
                    future.fd,
                    retained_iovecs(&payload.scratch),
                    0,
                    2,
                    &mut payload.msg,
                    99,
                )
            };
            let header_address = unsafe { std::ptr::addr_of!((*payload_address).msg) };
            assert_eq!(sqe_prefix(&entry).addr, header_address as u64);
            let state = unsafe { (&mut *reactor).alloc_op() };
            assert!(!state.is_null());
            unsafe {
                (*state).attach_retained_payload(retained);
                (*state).attach_fd_lease(future.state_ptr.take_initial_lease());
                future.state_ptr.publish_submitted_state(state);
            }
            let mut moved = Box::new(future);
            assert_eq!(moved.state_ptr.state_ptr(), state);
            assert_eq!(buffers.live_slots_for_test(), 2);
            unsafe {
                let payload = (*state).retained_payload::<RetainedReadvPayload<2>>();
                assert_eq!(payload as *const _, payload_address.cast_const());
                assert_eq!(std::ptr::addr_of!(payload.msg), header_address);
                let header = payload.msg.assume_init_ref();
                assert_eq!(
                    header.msg_iov,
                    payload.scratch.as_uninit_slice().as_ptr() as *mut _
                );
                assert_eq!(header.msg_iovlen, 2);
                for (index, original) in originals.iter().enumerate() {
                    let iovec = &*header.msg_iov.add(index);
                    assert_eq!(iovec.iov_base.cast_const(), (*original).cast());
                    iovec.iov_base.cast::<u8>().write(b'a' + index as u8);
                }
                (*state).result = 2;
                (*state).set_completed();
            }
            let Poll::Ready((result, returned)) = Pin::new(moved.as_mut()).poll(cx) else {
                panic!("completed moved receive remained pending");
            };
            assert_eq!(result.expect("completed receive failed"), 2);
            for (index, original) in originals.iter().enumerate() {
                let segment = returned.get(index).expect("returned segment missing");
                assert_eq!(segment.as_ptr(), *original);
                assert_eq!(segment.payload_bytes(), &[b'a' + index as u8]);
            }
            assert_eq!(buffers.live_slots_for_test(), 2);
            drop(moved);
            assert_eq!(buffers.live_slots_for_test(), 2);
            drop(returned);
            assert_eq!(buffers.live_slots_for_test(), 0);
            assert_eq!(unsafe { (&*reactor).live_op_count() }, 0);
            let stats = unsafe { (&*reactor).retained_payload_stats() };
            assert_eq!(stats.pooled_allocs, 1);
            assert_eq!(stats.pooled_frees, 1);
        });
    }

    #[test]
    fn pending_write_repoll_during_completion_drain_returns_real_result() {
        with_ringless_poll_context_for_test(1, |owner, cx| {
            let pctx = poll_ctx_from_waker(cx).expect("valid poll context was rejected");
            let reactor = owner.reactor_ptr();
            let state = unsafe { (&mut *reactor).alloc_op() };
            assert!(!state.is_null(), "operation allocation failed");

            let mut future =
                WriteFuture::<Vec<u8>, ()>::new(invalid_fd_capability(), b"done".to_vec());
            let payload = RetainedWritePayload {
                buffer: future.buffer.take().expect("write buffer was missing"),
            };
            let retained = unsafe { (&mut *reactor).alloc_retained_payload(payload) };
            unsafe {
                (*state).attach_retained_payload(retained);
                (*state).register_waiter(pctx.owner_task());
                (*state).attach_fd_lease(future.state_ptr.take_initial_lease());
                // Ringless test setup emulates the post-push representation;
                // the live state now owns the descriptor lease it publishes.
                future.state_ptr.publish_submitted_state(state);
            }

            let flags = unsafe { (*state).state_flags };
            let waiter = unsafe { (*state).waiter };
            let waiter_refs = unsafe { (*waiter).refs.get() };
            let drain = CompletionDrainGuard::enter();
            assert!(
                Pin::new(&mut future).poll(cx).is_pending(),
                "transient completion drain completed the pending write"
            );
            unsafe {
                assert_eq!(
                    (*state).state_flags,
                    flags,
                    "transient drain changed write-operation flags"
                );
                assert_eq!(
                    (*state).waiter,
                    waiter,
                    "transient drain replaced the write waiter"
                );
                assert_eq!(
                    (*waiter).refs.get(),
                    waiter_refs,
                    "transient drain changed write-waiter ownership"
                );
            }
            drop(drain);

            unsafe {
                (*state).result = 4;
                (*state).set_completed();
                let completed_waiter = CompletionState::take_waiter_unchecked(state);
                assert_eq!(completed_waiter, waiter);
                release_task(completed_waiter);
            }

            let Poll::Ready((result, buffer)) = Pin::new(&mut future).poll(cx) else {
                panic!("completed write remained pending after the drain");
            };
            assert_eq!(result.expect("completed write was rejected"), 4);
            assert_eq!(buffer, b"done");
            assert!(future.state_ptr.is_null());
            assert_eq!(unsafe { (&*reactor).live_op_count() }, 0);

            let stats = unsafe { (&*reactor).retained_payload_stats() };
            assert_eq!(stats.pooled_allocs, 1);
            assert_eq!(stats.pooled_frees, 1);
        });
    }

    #[test]
    fn completion_free_panic_clears_fd_state_before_future_drop() {
        with_ringless_poll_context_for_test(1, |owner, cx| {
            let reactor = owner.reactor_ptr();
            let runtime = RuntimeFd::from_fresh_raw_fd(-2);
            let staged = runtime.lease().into_op_state();
            drop(runtime);

            let mut future = WriteFuture::<Vec<u8>, ()>::new(staged, b"done".to_vec());
            let state = unsafe { (&mut *reactor).alloc_op() };
            assert!(!state.is_null(), "operation allocation failed");
            let payload = RetainedWritePayload {
                buffer: future.buffer.take().expect("write buffer was missing"),
            };
            let retained = unsafe { (&mut *reactor).alloc_retained_payload(payload) };
            unsafe {
                (*state).attach_retained_payload(retained);
                (*state).attach_fd_lease(future.state_ptr.take_initial_lease());
                (*state).result = 4;
                (*state).set_completed();
                future.state_ptr.publish_submitted_state(state);
            }

            let unwind = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                with_final_core_drop_hook_for_test(panic_on_final_fd_core_drop, || {
                    let _ = Pin::new(&mut future).poll(cx);
                });
            }));

            assert!(unwind.is_err(), "final descriptor release did not panic");
            assert!(
                future.state_ptr.is_null(),
                "completion free left the future owning a reclaimed state"
            );
            assert_eq!(unsafe { (&*reactor).live_op_count() }, 0);

            drop(future);
            let replacement = unsafe { (&mut *reactor).alloc_op() };
            assert_eq!(replacement, state, "completion slot was not returned once");
            unsafe { (&mut *reactor).free_op(replacement) };
        });
    }

    #[test]
    fn projected_shape_validators_preserve_async_and_try_error_contracts() {
        for shape in [(0, 0), (1, 1), (usize::MAX, usize::MAX)] {
            validate_projected_count_and_len(shape.0, shape.1)
                .expect("valid async reported shape should pass");
            validate_try_projected_count_and_len(shape.0, shape.1)
                .expect("valid try reported shape should pass");
        }

        let reported_bytes =
            validate_projected_count_and_len(0, 1).expect_err("bytes without pieces should fail");
        assert_eq!(reported_bytes.kind(), io::ErrorKind::InvalidInput);
        assert_eq!(
            reported_bytes.to_string(),
            "projected writev reported bytes but no active pieces"
        );
        assert_message_free_invalid_input(validate_try_projected_count_and_len(0, 1));

        let reported_pieces =
            validate_projected_count_and_len(1, 0).expect_err("pieces without bytes should fail");
        assert_eq!(reported_pieces.kind(), io::ErrorKind::InvalidInput);
        assert_eq!(
            reported_pieces.to_string(),
            "projected writev reported active pieces but no bytes"
        );
        assert_message_free_invalid_input(validate_try_projected_count_and_len(1, 0));

        validate_projected_materialized_shape(2, 3, 2, 3)
            .expect("matching materialized shape should pass");
        validate_try_projected_materialized_shape(2, 3, 2, 3)
            .expect("matching try materialized shape should pass");

        let count_mismatch = validate_projected_materialized_shape(1, 3, 2, 3)
            .expect_err("piece-count mismatch should fail");
        assert_eq!(
            count_mismatch.to_string(),
            "projected writev piece count did not match counted pieces"
        );
        assert_message_free_invalid_input(validate_try_projected_materialized_shape(1, 3, 2, 3));

        let length_mismatch = validate_projected_materialized_shape(2, 2, 2, 3)
            .expect_err("byte-length mismatch should fail");
        assert_eq!(
            length_mismatch.to_string(),
            "projected writev byte length did not match counted length"
        );
        assert_message_free_invalid_input(validate_try_projected_materialized_shape(2, 2, 2, 3));

        let both_mismatch = validate_projected_materialized_shape(1, 2, 2, 3)
            .expect_err("combined shape mismatch should fail");
        assert_eq!(
            both_mismatch.to_string(),
            "projected writev piece count did not match counted pieces",
            "piece-count mismatch must retain precedence"
        );
        assert_message_free_invalid_input(validate_try_projected_materialized_shape(1, 2, 2, 3));
    }

    fn compacted_readv_test_chain() -> IoBuffVecMut<4> {
        let mut full = IoBuffMut::new(0, 4, 0).expect("full segment allocation failed");
        full.payload_append(b"full")
            .expect("full segment initialization failed");
        let writable = IoBuffMut::new(0, 8, 0).expect("writable segment allocation failed");
        let zero = IoBuffMut::new(0, 0, 0).expect("zero segment allocation failed");
        let mut partial = IoBuffMut::new(0, 6, 0).expect("partial segment allocation failed");
        partial
            .payload_append(b"ok")
            .expect("partial segment initialization failed");
        IoBuffVecMut::from_array([full, writable, zero, partial])
    }

    #[derive(Clone, Copy)]
    struct WritevShape {
        initial: [usize; 2],
        materialized: [usize; 2],
    }

    const WRITEV_SHAPE_SHRINK: WritevShape = WritevShape {
        initial: [1, 1],
        materialized: [1, 0],
    };
    const WRITEV_SHAPE_GROWTH: WritevShape = WritevShape {
        initial: [1, 0],
        materialized: [1, 1],
    };
    const WRITEV_SHAPE_TOTAL_DRIFT: WritevShape = WritevShape {
        initial: [1, 1],
        materialized: [2, 1],
    };

    const OVERSIZED_RETAINED_IOVEC_COUNT: usize = RETAINED_IOVEC_MAX_COUNT + 1;

    const fn expected_preflight_oversize_rejections(debug_count: usize) -> usize {
        if cfg!(debug_assertions) {
            debug_count
        } else {
            0
        }
    }

    #[derive(Clone, Copy)]
    struct OversizedWriteItem(u8);

    impl WriteBufferItem for OversizedWriteItem {
        fn write_ptr(&self) -> *const u8 {
            std::ptr::addr_of!(self.0)
        }

        fn write_len(&self) -> usize {
            1
        }
    }

    struct OversizedWriteChain {
        token: Box<u8>,
        sizing_calls: Rc<Cell<usize>>,
        fill_calls: Rc<Cell<usize>>,
        items: [OversizedWriteItem; OVERSIZED_RETAINED_IOVEC_COUNT],
    }

    impl OversizedWriteChain {
        fn new(sizing_calls: &Rc<Cell<usize>>, fill_calls: &Rc<Cell<usize>>) -> Self {
            Self {
                token: Box::new(0xA5),
                sizing_calls: Rc::clone(sizing_calls),
                fill_calls: Rc::clone(fill_calls),
                items: [OversizedWriteItem(0x5A); OVERSIZED_RETAINED_IOVEC_COUNT],
            }
        }

        fn token_ptr(&self) -> *const u8 {
            self.token.as_ref()
        }
    }

    impl write_buffer_chain_sealed::Sealed<OVERSIZED_RETAINED_IOVEC_COUNT> for OversizedWriteChain {
        fn write_iovec_count_and_len(&self) -> Option<(usize, usize)> {
            self.sizing_calls.set(self.sizing_calls.get() + 1);
            checked_write_iovec_count_and_len(self.items.iter())
        }

        fn fill_write_iovecs(
            &self,
            _dst: &mut [MaybeUninit<libc::iovec>],
        ) -> io::Result<(usize, usize)> {
            self.fill_calls.set(self.fill_calls.get() + 1);
            panic!("oversized write chain reached iovec materialization");
        }
    }

    fn oversized_read_chain() -> IoBuffVecMut<OVERSIZED_RETAINED_IOVEC_COUNT> {
        let mut chain = IoBuffVecMut::new();
        for _ in 0..OVERSIZED_RETAINED_IOVEC_COUNT {
            let segment =
                IoBuffMut::new(0, 1, 0).expect("oversized read segment allocation failed");
            chain
                .push(segment)
                .expect("oversized read chain exceeded its const capacity");
        }
        chain
    }

    fn oversized_read_chain_endpoint_ptrs(
        chain: &mut IoBuffVecMut<OVERSIZED_RETAINED_IOVEC_COUNT>,
    ) -> (usize, usize) {
        let first = chain
            .get_mut(0)
            .expect("oversized read chain first segment missing")
            .as_mut_ptr() as usize;
        let last = chain
            .get_mut(OVERSIZED_RETAINED_IOVEC_COUNT - 1)
            .expect("oversized read chain last segment missing")
            .as_mut_ptr() as usize;
        (first, last)
    }

    fn observed_error(result: io::Result<usize>) -> Option<(io::ErrorKind, String)> {
        result.err().map(|err| (err.kind(), err.to_string()))
    }

    struct OverlimitObservation {
        error: Option<(io::ErrorKind, String)>,
        owner_ok: bool,
        fault_remaining: bool,
    }

    fn poll_overlimit_readv(cx: &mut Context<'_>) -> OverlimitObservation {
        let mut future = ReadvFuture::<OVERSIZED_RETAINED_IOVEC_COUNT, ()>::new(
            invalid_fd_capability(),
            oversized_read_chain(),
        );
        let expected = oversized_read_chain_endpoint_ptrs(
            future.buffer.as_mut().expect("readv chain missing"),
        );
        assert_eq!(
            future.deferred_error,
            DeferredReadvError::ActiveIovecOverflow,
            "readv constructor did not retain its exact deferred diagnostic"
        );
        future.iov_count = 0;
        crate::runtime::test_hooks::fail_next_op_alloc();
        let output = match Pin::new(&mut future).poll(cx) {
            Poll::Ready(output) => Some(output),
            Poll::Pending => None,
        };
        let fault_remaining = crate::runtime::test_hooks::take_op_alloc_failure();
        let (error, owner_ok) = match output {
            Some((result, mut chain)) => (
                observed_error(result),
                chain.segments() == OVERSIZED_RETAINED_IOVEC_COUNT
                    && oversized_read_chain_endpoint_ptrs(&mut chain) == expected,
            ),
            None => (None, false),
        };
        OverlimitObservation {
            error,
            owner_ok,
            fault_remaining,
        }
    }

    fn poll_overlimit_readv_exact(cx: &mut Context<'_>) -> OverlimitObservation {
        let mut future = ReadvExactFuture::<OVERSIZED_RETAINED_IOVEC_COUNT, ()>::new(
            invalid_fd_capability(),
            oversized_read_chain(),
            1,
        );
        let expected = oversized_read_chain_endpoint_ptrs(
            future.buffer.as_mut().expect("readv_exact chain missing"),
        );
        crate::runtime::test_hooks::fail_next_op_alloc();
        let output = match Pin::new(&mut future).poll(cx) {
            Poll::Ready(output) => Some(output),
            Poll::Pending => None,
        };
        let fault_remaining = crate::runtime::test_hooks::take_op_alloc_failure();
        let (error, owner_ok) = match output {
            Some((result, mut chain)) => (
                observed_error(result),
                chain.segments() == OVERSIZED_RETAINED_IOVEC_COUNT
                    && oversized_read_chain_endpoint_ptrs(&mut chain) == expected,
            ),
            None => (None, false),
        };
        OverlimitObservation {
            error,
            owner_ok,
            fault_remaining,
        }
    }

    struct WriteOverlimitObservation {
        base: OverlimitObservation,
        sizing_calls: usize,
        fill_calls: usize,
    }

    fn poll_overlimit_writev_all(cx: &mut Context<'_>) -> WriteOverlimitObservation {
        let sizing_calls = Rc::new(Cell::new(0));
        let fill_calls = Rc::new(Cell::new(0));
        let chain = OversizedWriteChain::new(&sizing_calls, &fill_calls);
        let expected = chain.token_ptr();
        let mut future = WritevAllFuture::<_, OVERSIZED_RETAINED_IOVEC_COUNT, ()>::new(
            invalid_fd_capability(),
            chain,
        );
        crate::runtime::test_hooks::fail_next_op_alloc();
        let output = match Pin::new(&mut future).poll(cx) {
            Poll::Ready(output) => Some(output),
            Poll::Pending => None,
        };
        let fault_remaining = crate::runtime::test_hooks::take_op_alloc_failure();
        let (error, owner_ok) = match output {
            Some((result, chain)) => (observed_error(result), chain.token_ptr() == expected),
            None => (None, false),
        };
        WriteOverlimitObservation {
            base: OverlimitObservation {
                error,
                owner_ok,
                fault_remaining,
            },
            sizing_calls: sizing_calls.get(),
            fill_calls: fill_calls.get(),
        }
    }

    fn poll_outside_overlimit_readv(cx: &mut Context<'_>) -> (Option<io::ErrorKind>, bool) {
        let mut future = ReadvFuture::<OVERSIZED_RETAINED_IOVEC_COUNT, ()>::new(
            invalid_fd_capability(),
            oversized_read_chain(),
        );
        let expected = oversized_read_chain_endpoint_ptrs(
            future.buffer.as_mut().expect("readv chain missing"),
        );
        match Pin::new(&mut future).poll(cx) {
            Poll::Ready((result, mut chain)) => (
                result.err().map(|err| err.kind()),
                chain.segments() == OVERSIZED_RETAINED_IOVEC_COUNT
                    && oversized_read_chain_endpoint_ptrs(&mut chain) == expected,
            ),
            Poll::Pending => (None, false),
        }
    }

    fn poll_outside_overlimit_readv_exact(cx: &mut Context<'_>) -> (Option<io::ErrorKind>, bool) {
        let mut future = ReadvExactFuture::<OVERSIZED_RETAINED_IOVEC_COUNT, ()>::new(
            invalid_fd_capability(),
            oversized_read_chain(),
            1,
        );
        let expected = oversized_read_chain_endpoint_ptrs(
            future.buffer.as_mut().expect("readv_exact chain missing"),
        );
        match Pin::new(&mut future).poll(cx) {
            Poll::Ready((result, mut chain)) => (
                result.err().map(|err| err.kind()),
                chain.segments() == OVERSIZED_RETAINED_IOVEC_COUNT
                    && oversized_read_chain_endpoint_ptrs(&mut chain) == expected,
            ),
            Poll::Pending => (None, false),
        }
    }

    fn poll_outside_overlimit_writev_all(
        cx: &mut Context<'_>,
    ) -> (Option<io::ErrorKind>, bool, usize, usize) {
        let sizing_calls = Rc::new(Cell::new(0));
        let fill_calls = Rc::new(Cell::new(0));
        let chain = OversizedWriteChain::new(&sizing_calls, &fill_calls);
        let expected = chain.token_ptr();
        let mut future = WritevAllFuture::<_, OVERSIZED_RETAINED_IOVEC_COUNT, ()>::new(
            invalid_fd_capability(),
            chain,
        );
        let (error, owner_ok) = match Pin::new(&mut future).poll(cx) {
            Poll::Ready((result, chain)) => (
                result.err().map(|err| err.kind()),
                chain.token_ptr() == expected,
            ),
            Poll::Pending => (None, false),
        };
        (error, owner_ok, sizing_calls.get(), fill_calls.get())
    }

    fn poll_overlimit_zero_target(cx: &mut Context<'_>) -> (Option<io::Result<usize>>, bool, bool) {
        let mut future = ReadvExactFuture::<OVERSIZED_RETAINED_IOVEC_COUNT, ()>::new(
            invalid_fd_capability(),
            oversized_read_chain(),
            0,
        );
        let expected = oversized_read_chain_endpoint_ptrs(
            future.buffer.as_mut().expect("zero-target chain missing"),
        );
        crate::runtime::test_hooks::fail_next_op_alloc();
        let output = match Pin::new(&mut future).poll(cx) {
            Poll::Ready(output) => Some(output),
            Poll::Pending => None,
        };
        let fault_remaining = crate::runtime::test_hooks::take_op_alloc_failure();
        let (result, owner_ok) = match output {
            Some((result, mut chain)) => (
                Some(result),
                chain.segments() == OVERSIZED_RETAINED_IOVEC_COUNT
                    && oversized_read_chain_endpoint_ptrs(&mut chain) == expected,
            ),
            None => (None, false),
        };
        (result, owner_ok, fault_remaining)
    }

    struct AggregateLengthItem(usize);

    impl WriteBufferItem for AggregateLengthItem {
        fn write_ptr(&self) -> *const u8 {
            NonNull::<u8>::dangling().as_ptr()
        }

        fn write_len(&self) -> usize {
            self.0
        }
    }

    struct AggregateOverflowWriteChain {
        id: usize,
        token: Box<u8>,
        sizing_calls: Rc<Cell<usize>>,
        fill_calls: Rc<Cell<usize>>,
        drops: Rc<Cell<usize>>,
        items: [AggregateLengthItem; 3],
    }

    impl AggregateOverflowWriteChain {
        fn new(
            id: usize,
            sizing_calls: &Rc<Cell<usize>>,
            fill_calls: &Rc<Cell<usize>>,
            drops: &Rc<Cell<usize>>,
        ) -> Self {
            let half = isize::MAX as usize;
            Self {
                id,
                token: Box::new(id as u8),
                sizing_calls: Rc::clone(sizing_calls),
                fill_calls: Rc::clone(fill_calls),
                drops: Rc::clone(drops),
                items: [
                    AggregateLengthItem(half),
                    AggregateLengthItem(half),
                    AggregateLengthItem(2),
                ],
            }
        }

        fn token_ptr(&self) -> *const u8 {
            self.token.as_ref()
        }
    }

    impl write_buffer_chain_sealed::Sealed<3> for AggregateOverflowWriteChain {
        fn write_iovec_count_and_len(&self) -> Option<(usize, usize)> {
            self.sizing_calls.set(self.sizing_calls.get() + 1);
            checked_write_iovec_count_and_len(self.items.iter())
        }

        fn fill_write_iovecs(
            &self,
            _dst: &mut [MaybeUninit<libc::iovec>],
        ) -> io::Result<(usize, usize)> {
            self.fill_calls.set(self.fill_calls.get() + 1);
            panic!("overflowed write chain reached iovec materialization");
        }
    }

    impl Drop for AggregateOverflowWriteChain {
        fn drop(&mut self) {
            self.drops.set(self.drops.get() + 1);
        }
    }

    #[test]
    fn writev_sizing_accepts_exact_usize_max_and_rejects_overflow() {
        let half = isize::MAX as usize;
        let exact = [
            AggregateLengthItem(half),
            AggregateLengthItem(half),
            AggregateLengthItem(1),
        ];
        assert_eq!(
            checked_write_iovec_count_and_len(exact.iter()),
            Some((3, usize::MAX))
        );

        let overflow = [
            AggregateLengthItem(half),
            AggregateLengthItem(half),
            AggregateLengthItem(2),
        ];
        assert_eq!(checked_write_iovec_count_and_len(overflow.iter()), None);

        let compacted = [
            AggregateLengthItem(0),
            AggregateLengthItem(7),
            AggregateLengthItem(0),
        ];
        assert_eq!(
            checked_write_iovec_count_and_len(compacted.iter()),
            Some((1, 7))
        );
    }

    #[test]
    fn write_iovec_fill_reports_complete_shape_overflow_and_compaction() {
        let half = isize::MAX as usize;
        let exact = [
            AggregateLengthItem(half),
            AggregateLengthItem(half),
            AggregateLengthItem(1),
        ];
        let mut exact_scratch: [MaybeUninit<libc::iovec>; 3] =
            std::array::from_fn(|_| MaybeUninit::uninit());
        assert_eq!(
            fill_write_iovecs(exact.iter(), &mut exact_scratch)
                .expect("exact usize maximum should materialize"),
            (3, usize::MAX)
        );

        let overflow = [
            AggregateLengthItem(half),
            AggregateLengthItem(half),
            AggregateLengthItem(2),
        ];
        let mut overflow_scratch: [MaybeUninit<libc::iovec>; 3] =
            std::array::from_fn(|_| MaybeUninit::uninit());
        let overflow_err = fill_write_iovecs(overflow.iter(), &mut overflow_scratch)
            .expect_err("overflowing aggregate should fail");
        assert_eq!(overflow_err.kind(), io::ErrorKind::InvalidInput);
        assert_eq!(overflow_err.to_string(), WRITEV_SHAPE_CHANGED);

        let short = [AggregateLengthItem(1), AggregateLengthItem(1)];
        let mut short_scratch = [MaybeUninit::uninit()];
        assert_eq!(
            fill_write_iovecs(short.iter(), &mut short_scratch)
                .expect("short scratch should still report the complete source shape"),
            (2, 2)
        );
        let short_prefix = unsafe { short_scratch[0].assume_init_ref() };
        assert_eq!(short_prefix.iov_len, 1);

        let mut short_overflow_scratch = [MaybeUninit::uninit()];
        let short_overflow_err = fill_write_iovecs(overflow.iter(), &mut short_overflow_scratch)
            .expect_err("overflow in the unmaterialized tail should fail");
        assert_eq!(short_overflow_err.kind(), io::ErrorKind::InvalidInput);
        assert_eq!(short_overflow_err.to_string(), WRITEV_SHAPE_CHANGED);

        let mut first = IoBuffMut::new(0, 3, 0).expect("first segment allocation failed");
        first
            .payload_append(b"abc")
            .expect("first segment initialization failed");
        let empty = IoBuffMut::new(0, 5, 0).expect("empty segment allocation failed");
        let mut second = IoBuffMut::new(0, 5, 0).expect("second segment allocation failed");
        second
            .payload_append(b"defgh")
            .expect("second segment initialization failed");
        let chain = IoBuffVec::from_array([first.freeze(), empty.freeze(), second.freeze()]);
        let first_ptr = chain.get(0).expect("first segment missing").as_ptr();
        let second_ptr = chain.get(2).expect("second segment missing").as_ptr();
        let mut compacted_scratch: [MaybeUninit<libc::iovec>; 3] = std::array::from_fn(|_| {
            MaybeUninit::new(libc::iovec {
                iov_base: std::ptr::null_mut(),
                iov_len: 77,
            })
        });

        assert_eq!(
            fill_iobuffvec_write_iovecs(&chain, &mut compacted_scratch)
                .expect("compacted chain should materialize"),
            (2, 8)
        );
        let first_iovec = unsafe { compacted_scratch[0].assume_init_ref() };
        let second_iovec = unsafe { compacted_scratch[1].assume_init_ref() };
        let untouched_tail = unsafe { compacted_scratch[2].assume_init_ref() };
        assert_eq!(first_iovec.iov_base, first_ptr.cast_mut().cast());
        assert_eq!(first_iovec.iov_len, 3);
        assert_eq!(second_iovec.iov_base, second_ptr.cast_mut().cast());
        assert_eq!(second_iovec.iov_len, 5);
        assert!(untouched_tail.iov_base.is_null());
        assert_eq!(untouched_tail.iov_len, 77);
    }

    fn assert_overflow_context_rejection<F>(
        future: F,
        expected_id: usize,
        expected_token: *const u8,
        sizing_calls: &Rc<Cell<usize>>,
        fill_calls: &Rc<Cell<usize>>,
        drops: &Rc<Cell<usize>>,
    ) where
        F: Future<Output = (io::Result<usize>, AggregateOverflowWriteChain)>,
    {
        let mut future = Box::pin(future);
        let mut cx = Context::from_waker(std::task::Waker::noop());
        let (result, chain) = match future.as_mut().poll(&mut cx) {
            Poll::Ready(output) => output,
            Poll::Pending => panic!("invalid aggregate parked outside FlowIO"),
        };
        assert_eq!(
            result
                .expect_err("FlowIO context rejection must precede aggregate validation")
                .kind(),
            io::ErrorKind::NotConnected
        );
        assert_eq!(chain.id, expected_id);
        assert_eq!(chain.token_ptr(), expected_token);
        assert_eq!(sizing_calls.get(), 0);
        assert_eq!(fill_calls.get(), 0);
        assert_eq!(drops.get(), 0);
        assert!(future.as_mut().poll(&mut cx).is_pending());
        drop(chain);
        assert_eq!(drops.get(), 1);
    }

    #[test]
    fn writev_aggregate_overflow_preserves_context_precedence_and_post_ready_state() {
        let sizing_calls = Rc::new(Cell::new(0));
        let fill_calls = Rc::new(Cell::new(0));
        let drops = Rc::new(Cell::new(0));
        let chain = AggregateOverflowWriteChain::new(118, &sizing_calls, &fill_calls, &drops);
        let token = chain.token_ptr();
        assert_overflow_context_rejection(
            WritevFuture::<_, 3, ()>::new(invalid_fd_capability(), chain),
            118,
            token,
            &sizing_calls,
            &fill_calls,
            &drops,
        );

        let sizing_calls = Rc::new(Cell::new(0));
        let fill_calls = Rc::new(Cell::new(0));
        let drops = Rc::new(Cell::new(0));
        let chain = AggregateOverflowWriteChain::new(119, &sizing_calls, &fill_calls, &drops);
        let token = chain.token_ptr();
        assert_overflow_context_rejection(
            WritevAllFuture::<_, 3, ()>::new(invalid_fd_capability(), chain),
            119,
            token,
            &sizing_calls,
            &fill_calls,
            &drops,
        );
    }

    #[cfg(not(miri))]
    async fn assert_overflow_inside_flowio<F>(
        future: F,
        expected_id: usize,
        expected_token: *const u8,
        sizing_calls: &Rc<Cell<usize>>,
        fill_calls: &Rc<Cell<usize>>,
        drops: &Rc<Cell<usize>>,
    ) where
        F: Future<Output = (io::Result<usize>, AggregateOverflowWriteChain)>,
    {
        #[cfg(debug_assertions)]
        crate::runtime::test_hooks::fail_next_op_alloc();

        let mut future = Box::pin(future);
        let (result, chain) = future.as_mut().await;
        let err = result.expect_err("overflowed aggregate must fail locally");
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
        assert_eq!(err.to_string(), WRITEV_AGGREGATE_OVERFLOW);
        assert_eq!(chain.id, expected_id);
        assert_eq!(chain.token_ptr(), expected_token);
        assert_eq!(sizing_calls.get(), 1);
        assert_eq!(fill_calls.get(), 0);
        assert_eq!(drops.get(), 0);

        std::future::poll_fn(|cx| {
            assert!(future.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;

        #[cfg(debug_assertions)]
        assert!(
            crate::runtime::test_hooks::take_op_alloc_failure(),
            "invalid aggregate consumed an operation slot"
        );

        drop(chain);
        assert_eq!(drops.get(), 1);
    }

    #[cfg(not(miri))]
    #[test]
    fn writev_aggregate_overflow_returns_exact_owner_without_runtime_allocation() {
        let mut executor = Executor::new().expect("executor creation failed");
        executor
            .run(async move {
                let sizing_calls = Rc::new(Cell::new(0));
                let fill_calls = Rc::new(Cell::new(0));
                let drops = Rc::new(Cell::new(0));
                let chain =
                    AggregateOverflowWriteChain::new(120, &sizing_calls, &fill_calls, &drops);
                let token = chain.token_ptr();
                assert_overflow_inside_flowio(
                    WritevFuture::<_, 3, ()>::new(invalid_fd_capability(), chain),
                    120,
                    token,
                    &sizing_calls,
                    &fill_calls,
                    &drops,
                )
                .await;

                let sizing_calls = Rc::new(Cell::new(0));
                let fill_calls = Rc::new(Cell::new(0));
                let drops = Rc::new(Cell::new(0));
                let chain =
                    AggregateOverflowWriteChain::new(121, &sizing_calls, &fill_calls, &drops);
                let token = chain.token_ptr();
                assert_overflow_inside_flowio(
                    WritevAllFuture::<_, 3, ()>::new(invalid_fd_capability(), chain),
                    121,
                    token,
                    &sizing_calls,
                    &fill_calls,
                    &drops,
                )
                .await;
            })
            .expect("overflow validation executor run failed");

        #[cfg(debug_assertions)]
        {
            let stats = executor.last_stats();
            assert_eq!(stats.sqe_submits, 0);
            assert_eq!(stats.cqe_completions, 0);
            assert_eq!(stats.retained_pooled_allocs, 0);
            assert_eq!(stats.retained_heap_fallbacks, 0);
            assert_eq!(stats.writev_scratch_inline_allocs, 0);
            assert_eq!(stats.writev_scratch_pooled_allocs, 0);
            assert_eq!(
                stats.poll_context_extractions, 2,
                "each invalid aggregate must validate its FlowIO context once"
            );
        }
    }

    // Exercise the defensive materialization seam without forging a public
    // `IoBuffReadOnly` implementation that violates its tightened unsafe
    // shape-stability contract.
    struct ShapeDriftingWriteItem {
        id: usize,
        bytes: Box<[u8; 2]>,
        initial_len: usize,
        materialized_len: usize,
        len_calls: Rc<Cell<usize>>,
        drops: Rc<Cell<usize>>,
    }

    impl WriteBufferItem for ShapeDriftingWriteItem {
        fn write_ptr(&self) -> *const u8 {
            self.bytes.as_ptr()
        }

        fn write_len(&self) -> usize {
            let call = self.len_calls.get();
            self.len_calls.set(call + 1);
            if call == 0 {
                self.initial_len
            } else {
                self.materialized_len
            }
        }
    }

    impl Drop for ShapeDriftingWriteItem {
        fn drop(&mut self) {
            self.drops.set(self.drops.get() + 1);
        }
    }

    struct ShapeDriftingWriteChain {
        items: [ShapeDriftingWriteItem; 2],
    }

    struct MaterializationCountingWriteChain {
        items: [MaterializationCountingWriteItem; 2],
        fill_calls: Rc<Cell<usize>>,
    }

    struct MaterializationCountingWriteItem(Box<[u8; 2]>);

    impl WriteBufferItem for MaterializationCountingWriteItem {
        fn write_ptr(&self) -> *const u8 {
            self.0.as_ptr()
        }

        fn write_len(&self) -> usize {
            self.0.len()
        }
    }

    impl MaterializationCountingWriteChain {
        fn new(fill_calls: &Rc<Cell<usize>>) -> Self {
            Self {
                items: [
                    MaterializationCountingWriteItem(Box::new(*b"ab")),
                    MaterializationCountingWriteItem(Box::new(*b"cd")),
                ],
                fill_calls: Rc::clone(fill_calls),
            }
        }
    }

    impl write_buffer_chain_sealed::Sealed<2> for MaterializationCountingWriteChain {
        fn write_iovec_count_and_len(&self) -> Option<(usize, usize)> {
            checked_write_iovec_count_and_len(self.items.iter())
        }

        fn fill_write_iovecs(
            &self,
            dst: &mut [MaybeUninit<libc::iovec>],
        ) -> io::Result<(usize, usize)> {
            self.fill_calls.set(self.fill_calls.get() + 1);
            fill_write_iovecs(self.items.iter(), dst)
        }
    }

    impl ShapeDriftingWriteChain {
        fn new(shape: WritevShape, drops: &Rc<Cell<usize>>) -> Self {
            Self {
                items: std::array::from_fn(|id| ShapeDriftingWriteItem {
                    id,
                    bytes: Box::new([id as u8, id as u8]),
                    initial_len: shape.initial[id],
                    materialized_len: shape.materialized[id],
                    len_calls: Rc::new(Cell::new(0)),
                    drops: Rc::clone(drops),
                }),
            }
        }

        fn ids(&self) -> [usize; 2] {
            self.items.each_ref().map(|item| item.id)
        }

        fn pointers(&self) -> [*const u8; 2] {
            self.items.each_ref().map(|item| item.bytes.as_ptr())
        }

        fn len_calls(&self) -> [usize; 2] {
            self.items.each_ref().map(|item| item.len_calls.get())
        }

        #[cfg(not(miri))]
        fn len_call_handles(&self) -> [Rc<Cell<usize>>; 2] {
            self.items.each_ref().map(|item| Rc::clone(&item.len_calls))
        }
    }

    impl write_buffer_chain_sealed::Sealed<2> for ShapeDriftingWriteChain {
        fn write_iovec_count_and_len(&self) -> Option<(usize, usize)> {
            checked_write_iovec_count_and_len(self.items.iter())
        }

        fn fill_write_iovecs(
            &self,
            dst: &mut [MaybeUninit<libc::iovec>],
        ) -> io::Result<(usize, usize)> {
            fill_write_iovecs(self.items.iter(), dst)
        }
    }

    #[test]
    fn sealed_writev_counter_observes_actual_iovec_materialization() {
        let fill_calls = Rc::new(Cell::new(0));
        let chain = MaterializationCountingWriteChain::new(&fill_calls);
        let expected_ptrs = chain.items.each_ref().map(WriteBufferItem::write_ptr);
        let mut scratch: [MaybeUninit<libc::iovec>; 2] =
            std::array::from_fn(|_| MaybeUninit::uninit());

        for expected_calls in 1..=2 {
            assert_eq!(
                write_buffer_chain_sealed::Sealed::fill_write_iovecs(&chain, &mut scratch)
                    .expect("counting chain materialization failed"),
                (2, 4)
            );
            assert_eq!(fill_calls.get(), expected_calls);
            for (index, expected_ptr) in expected_ptrs.iter().enumerate() {
                let iovec = unsafe { scratch[index].assume_init_ref() };
                assert_eq!(iovec.iov_base, expected_ptr.cast_mut().cast());
                assert_eq!(iovec.iov_len, 2);
            }
        }
    }

    #[test]
    fn direct_writev_construction_does_not_size_unpolled_chains() {
        let stable_shape = WritevShape {
            initial: [2, 2],
            materialized: [2, 2],
        };

        let drops = Rc::new(Cell::new(0));
        let future = WritevFuture::<_, 2, ()>::new(
            invalid_fd_capability(),
            ShapeDriftingWriteChain::new(stable_shape, &drops),
        );
        assert_eq!(
            future
                .buffer
                .as_ref()
                .expect("unpolled writev chain missing")
                .len_calls(),
            [0, 0]
        );
        assert_eq!((future.iov_count, future.total), (0, 0));
        drop(future);
        assert_eq!(drops.get(), 2);

        let drops = Rc::new(Cell::new(0));
        let future = WritevAllFuture::<_, 2, ()>::new(
            invalid_fd_capability(),
            ShapeDriftingWriteChain::new(stable_shape, &drops),
        );
        assert_eq!(
            future
                .buffer
                .as_ref()
                .expect("unpolled writev_all chain missing")
                .len_calls(),
            [0, 0]
        );
        assert_eq!((future.iov_count, future.total), (0, 0));
        drop(future);
        assert_eq!(drops.get(), 2);
    }

    #[cfg(not(miri))]
    #[test]
    fn direct_writev_sizing_runs_once_on_first_valid_poll() {
        let mut executor = Executor::new().expect("executor creation failed");
        executor
            .run(async move {
                for write_all in [false, true] {
                    let drops = Rc::new(Cell::new(0));
                    let chain = ShapeDriftingWriteChain::new(
                        WritevShape {
                            initial: [2, 2],
                            materialized: [2, 2],
                        },
                        &drops,
                    );
                    let calls = chain.len_call_handles();
                    let future: Pin<
                        Box<dyn Future<Output = (io::Result<usize>, ShapeDriftingWriteChain)>>,
                    > = if write_all {
                        Box::pin(WritevAllFuture::<_, 2, ()>::new(
                            invalid_fd_capability(),
                            chain,
                        ))
                    } else {
                        Box::pin(WritevFuture::<_, 2, ()>::new(
                            invalid_fd_capability(),
                            chain,
                        ))
                    };
                    let mut future = future;

                    std::future::poll_fn(|cx| {
                        assert!(future.as_mut().poll(cx).is_pending());
                        assert_eq!(calls.each_ref().map(|calls| calls.get()), [2, 2]);
                        assert!(future.as_mut().poll(cx).is_pending());
                        assert_eq!(calls.each_ref().map(|calls| calls.get()), [2, 2]);
                        Poll::Ready(())
                    })
                    .await;

                    let (result, chain) = future.await;
                    assert_eq!(
                        result
                            .expect_err("invalid descriptor should fail the write")
                            .raw_os_error(),
                        Some(libc::EBADF)
                    );
                    assert_eq!(chain.len_calls(), [2, 2]);
                    assert_eq!(drops.get(), 0);
                    drop(chain);
                    assert_eq!(drops.get(), 2);
                }
            })
            .expect("first-poll sizing executor run failed");
    }

    #[cfg(not(miri))]
    #[test]
    fn repeated_one_shot_writev_submissions_rematerialize_iovecs() {
        let mut executor = Executor::new().expect("executor creation failed");
        executor
            .run(async move {
                let fill_calls = Rc::new(Cell::new(0));
                let mut chain = MaterializationCountingWriteChain::new(&fill_calls);

                for expected_calls in 1..=3 {
                    let (result, returned) =
                        WritevFuture::<_, 2, ()>::new(invalid_fd_capability(), chain).await;
                    assert_eq!(
                        result
                            .expect_err("invalid descriptor should fail the write submission")
                            .raw_os_error(),
                        Some(libc::EBADF)
                    );
                    assert_eq!(
                        fill_calls.get(),
                        expected_calls,
                        "each one-shot writev submission must rebuild its actual iovec view"
                    );
                    chain = returned;
                }
            })
            .expect("repeated writev materialization executor run failed");
    }

    #[test]
    fn writev_shape_drift_recycles_raw_slot_before_ownership_transfer() {
        let mut pool = RetainedPayloadPool::new().expect("retained pool creation failed");

        for shape in [
            WRITEV_SHAPE_SHRINK,
            WRITEV_SHAPE_GROWTH,
            WRITEV_SHAPE_TOTAL_DRIFT,
        ] {
            let drops = Rc::new(Cell::new(0));
            let chain = ShapeDriftingWriteChain::new(shape, &drops);
            let expected_pointers = chain.pointers();
            let expected_shape =
                write_buffer_chain_sealed::Sealed::write_iovec_count_and_len(&chain)
                    .expect("initial shape should fit usize");
            let mut buffer = Some(chain);
            let scratch_init = pool
                .alloc_iovec_scratch_init(expected_shape.0)
                .expect("shape-drift scratch allocation failed");
            let after_fill_calls = Cell::new(0);
            let pool_ptr = NonNull::from(&mut pool);

            let result = unsafe {
                emplace_retained_writev_payload::<_, 2, _>(
                    pool_ptr,
                    &mut buffer,
                    scratch_init,
                    expected_shape.1,
                    || {
                        after_fill_calls.set(after_fill_calls.get() + 1);
                        Ok(())
                    },
                )
            };
            let err = match result {
                Ok(_) => panic!("shape drift must fail before ownership transfer"),
                Err(err) => err,
            };

            assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
            assert_eq!(err.to_string(), WRITEV_SHAPE_CHANGED);
            assert_eq!(after_fill_calls.get(), 0);
            let returned = buffer.as_ref().expect("shape drift lost caller chain");
            assert_eq!(returned.ids(), [0, 1]);
            assert_eq!(returned.pointers(), expected_pointers);
            assert_eq!(returned.len_calls(), [2, 2]);
            assert_eq!(drops.get(), 0, "shape drift dropped caller ownership");
            drop(buffer.take());
            assert_eq!(drops.get(), 2, "returned chain did not drop exactly once");
        }

        let stats = pool.stats();
        assert_eq!(stats.pooled_allocs, 3);
        assert_eq!(stats.pooled_frees, 3);
    }

    #[cfg(not(miri))]
    #[test]
    fn writev_shape_drift_futures_reject_before_submission_and_return_chain() {
        let mut executor = Executor::new_with_config(ExecutorConfig {
            reactor: ReactorConfig { ring_entries: 1 },
            ..ExecutorConfig::default()
        })
        .expect("one-slot executor creation failed");

        executor
            .run(async move {
                for write_all in [false, true] {
                    for shape in [
                        WRITEV_SHAPE_SHRINK,
                        WRITEV_SHAPE_GROWTH,
                        WRITEV_SHAPE_TOTAL_DRIFT,
                    ] {
                        let drops = Rc::new(Cell::new(0));
                        let chain = ShapeDriftingWriteChain::new(shape, &drops);
                        let expected_pointers = chain.pointers();

                        let (result, chain) = if write_all {
                            WritevAllFuture::<_, 2, ()>::new(invalid_fd_capability(), chain).await
                        } else {
                            WritevFuture::<_, 2, ()>::new(invalid_fd_capability(), chain).await
                        };

                        let err = result.expect_err("shape-drift write must fail locally");
                        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
                        assert_eq!(err.to_string(), WRITEV_SHAPE_CHANGED);
                        assert_eq!(chain.ids(), [0, 1]);
                        assert_eq!(chain.pointers(), expected_pointers);
                        assert_eq!(chain.len_calls(), [2, 2]);
                        assert_eq!(drops.get(), 0, "future dropped caller chain");
                        drop(chain);
                        assert_eq!(drops.get(), 2, "returned chain did not drop exactly once");
                    }
                }
            })
            .expect("shape-drift executor run failed");

        #[cfg(debug_assertions)]
        {
            let stats = executor.last_stats();
            assert_eq!(stats.sqe_submits, 0);
            assert_eq!(stats.cqe_completions, 0);
            assert_eq!(stats.retained_pooled_allocs, 6);
            assert_eq!(stats.retained_pooled_frees, 6);
        }
    }

    #[test]
    fn retry_state_reset_preserves_payload_and_refreshes_waiter() {
        struct DropTracker {
            id: usize,
            drops: Rc<Cell<usize>>,
        }

        impl Drop for DropTracker {
            fn drop(&mut self) {
                self.drops.set(self.drops.get() + 1);
            }
        }

        let drops = Rc::new(Cell::new(0));
        let mut pool = RetainedPayloadPool::new().expect("retained pool creation failed");
        let payload = pool.alloc(DropTracker {
            id: 79,
            drops: Rc::clone(&drops),
        });
        let payload_ptr = payload.as_ptr();
        let old_waiter = TaskHeader::new();
        let new_waiter = TaskHeader::new();
        let old_waiter_ptr = &old_waiter as *const TaskHeader as *mut TaskHeader;
        let new_waiter_ptr = &new_waiter as *const TaskHeader as *mut TaskHeader;
        let mut state = CompletionState::empty();
        state.result = 17;
        state.set_completed();
        state.attach_retained_payload(payload);
        unsafe { state.register_waiter(old_waiter_ptr) };
        let completed_waiter =
            unsafe { CompletionState::take_waiter_unchecked(std::ptr::addr_of_mut!(state)) };
        unsafe { crate::runtime::task::release_task(completed_waiter) };

        unsafe { reset_existing_retry_state(&mut state, new_waiter_ptr) };

        assert!(!state.is_completed());
        assert_eq!(state.result, 0);
        assert_eq!(old_waiter.refs.get(), 1, "old waiter reference leaked");
        assert_eq!(new_waiter.refs.get(), 2, "new waiter was not retained");
        let retained = unsafe { state.retained_payload::<DropTracker>() };
        assert_eq!(std::ptr::from_ref(retained).cast_mut(), payload_ptr);
        assert_eq!(retained.id, 79);
        assert_eq!(drops.get(), 0, "retained payload was dropped during reset");

        unsafe {
            CompletionState::clear_waiter_unchecked(std::ptr::addr_of_mut!(state));
        }
        assert_eq!(new_waiter.refs.get(), 1, "new waiter reference leaked");
        let payload = unsafe { state.take_retained_payload::<DropTracker>(&mut pool) };
        drop(payload);
        assert_eq!(
            drops.get(),
            1,
            "retained payload was not dropped exactly once"
        );
    }

    #[test]
    fn readv_futures_cache_compacted_iovec_count() {
        let readv =
            ReadvFuture::<4, ()>::new(invalid_fd_capability(), compacted_readv_test_chain());
        assert_eq!(readv.iov_count, 2);
        assert_eq!(readv.writable, 12);
        assert_eq!(readv.deferred_error, DeferredReadvError::None);
        assert_eq!(size_of::<DeferredReadvError>(), 1);
        assert_eq!(
            readv
                .buffer
                .as_ref()
                .expect("readv buffer missing")
                .segments(),
            4
        );

        let readv_exact = ReadvExactFuture::<4, ()>::new(
            invalid_fd_capability(),
            compacted_readv_test_chain(),
            12,
        );
        assert_eq!(readv_exact.iov_count, 2);
        assert_eq!(readv_exact.writable, 12);
        assert_eq!(readv_exact.target, 12);
        assert!(readv_exact.input_error.is_none());
        assert_eq!(
            readv_exact
                .buffer
                .as_ref()
                .expect("readv_exact buffer missing")
                .segments(),
            4
        );
    }

    #[test]
    fn vectored_overlimit_precedes_op_allocation_and_preserves_owners() {
        with_ringless_poll_context_for_test(0, |owner, cx| {
            let reactor = owner.reactor_ptr();
            assert_eq!(
                unsafe { (&*reactor).retained_payload_stats() }.writev_scratch_oversize_rejections,
                0
            );
            let read = poll_overlimit_readv(cx);
            assert_eq!(
                unsafe { (&*reactor).retained_payload_stats() }.writev_scratch_oversize_rejections,
                expected_preflight_oversize_rejections(1),
                "readv preflight rejection was not counted exactly once"
            );
            let exact = poll_overlimit_readv_exact(cx);
            assert_eq!(
                unsafe { (&*reactor).retained_payload_stats() }.writev_scratch_oversize_rejections,
                expected_preflight_oversize_rejections(2),
                "readv_exact preflight rejection was not counted exactly once"
            );
            let write = poll_overlimit_writev_all(cx);
            assert_eq!(
                unsafe { (&*reactor).retained_payload_stats() }.writev_scratch_oversize_rejections,
                expected_preflight_oversize_rejections(3),
                "writev_all preflight rejection was not counted exactly once"
            );

            let expected = Some((io::ErrorKind::InvalidInput, RETAINED_IOVEC_OVERSIZE_MESSAGE));
            assert_eq!(
                read.error
                    .as_ref()
                    .map(|(kind, message)| (*kind, message.as_str())),
                expected,
                "readv over-limit error changed"
            );
            assert_eq!(
                write
                    .base
                    .error
                    .as_ref()
                    .map(|(kind, message)| (*kind, message.as_str())),
                expected,
                "writev_all over-limit error changed"
            );
            assert_eq!(
                exact
                    .error
                    .as_ref()
                    .map(|(kind, message)| (*kind, message.as_str())),
                expected,
                "readv_exact over-limit error changed"
            );
            assert!(read.owner_ok, "readv did not return its exact owner");
            assert!(
                write.base.owner_ok,
                "writev_all did not return its exact owner"
            );
            assert!(exact.owner_ok, "readv_exact did not return its exact owner");
            assert!(read.fault_remaining, "readv attempted op allocation");
            assert!(
                write.base.fault_remaining,
                "writev_all attempted op allocation"
            );
            assert!(exact.fault_remaining, "readv_exact attempted op allocation");
            assert_eq!(write.sizing_calls, 1, "writev_all repeated its sizing pass");
            assert_eq!(write.fill_calls, 0, "writev_all materialized scratch");

            assert_eq!(unsafe { (&*reactor).live_op_count() }, 0);
            assert_eq!(owner.inflight_op_count_for_test(), 0);
            let stats = unsafe { (&*reactor).retained_payload_stats() };
            assert_eq!(stats.pooled_allocs, 0);
            assert_eq!(stats.heap_fallbacks, 0);
            assert_eq!(stats.writev_scratch_inline_allocs, 0);
            assert_eq!(stats.writev_scratch_pooled_allocs, 0);
            assert_eq!(
                stats.writev_scratch_oversize_rejections,
                expected_preflight_oversize_rejections(3)
            );
        });
    }

    #[test]
    fn vectored_overlimit_preserves_context_and_zero_target_precedence() {
        let mut outside_cx = Context::from_waker(std::task::Waker::noop());
        let (read_error, read_owner_ok) = poll_outside_overlimit_readv(&mut outside_cx);
        assert_eq!(
            read_error,
            Some(io::ErrorKind::NotConnected),
            "context rejection must precede readv limit"
        );
        assert!(read_owner_ok, "readv context rejection changed its owner");

        let (write_error, write_owner_ok, sizing_calls, fill_calls) =
            poll_outside_overlimit_writev_all(&mut outside_cx);
        assert_eq!(
            write_error,
            Some(io::ErrorKind::NotConnected),
            "context rejection must precede writev_all limit"
        );
        assert!(
            write_owner_ok,
            "writev_all context rejection changed its owner"
        );
        assert_eq!(sizing_calls, 0, "context rejection ran sizing");
        assert_eq!(fill_calls, 0, "context rejection materialized scratch");

        let (exact_error, exact_owner_ok) = poll_outside_overlimit_readv_exact(&mut outside_cx);
        assert_eq!(
            exact_error,
            Some(io::ErrorKind::NotConnected),
            "context rejection must precede readv_exact limit"
        );
        assert!(
            exact_owner_ok,
            "readv_exact context rejection changed its owner"
        );

        with_ringless_poll_context_for_test(0, |owner, cx| {
            let reactor = owner.reactor_ptr();
            let (result, owner_ok, fault_remaining) = poll_overlimit_zero_target(cx);
            assert_eq!(
                result
                    .expect("zero-target readv_exact remained pending")
                    .expect("zero-target readv_exact failed"),
                0
            );
            assert!(owner_ok, "zero-target readv_exact changed its owner");
            assert!(
                fault_remaining,
                "zero-target readv_exact attempted op allocation"
            );

            let mut empty_read =
                ReadvFuture::<1, ()>::new(invalid_fd_capability(), IoBuffVecMut::<1>::new());
            let (empty_read_result, empty_read_chain) = match Pin::new(&mut empty_read).poll(cx) {
                Poll::Ready(output) => output,
                Poll::Pending => panic!("empty readv remained pending"),
            };
            assert_eq!(
                empty_read_result
                    .expect_err("empty readv unexpectedly succeeded")
                    .kind(),
                io::ErrorKind::InvalidInput
            );
            assert_eq!(empty_read_chain.segments(), 0);

            let mut empty_write =
                WritevAllFuture::<_, 1, ()>::new(invalid_fd_capability(), IoBuffVec::<1>::new());
            let (empty_write_result, empty_write_chain) = match Pin::new(&mut empty_write).poll(cx)
            {
                Poll::Ready(output) => output,
                Poll::Pending => panic!("empty writev_all remained pending"),
            };
            assert_eq!(empty_write_result.expect("empty writev_all failed"), 0);
            assert_eq!(empty_write_chain.segments(), 0);

            assert_eq!(
                unsafe { (&*reactor).retained_payload_stats() }.writev_scratch_oversize_rejections,
                0,
                "target-zero or empty-shape paths counted an oversize rejection"
            );
        });
    }

    fn aggregate_rejection_test_chain() -> IoBuffVecMut<1> {
        IoBuffVecMut::from_array([
            IoBuffMut::new(0, 8, 0).expect("aggregate-rejection segment allocation failed")
        ])
    }

    fn aggregate_rejection_chain_ptr(chain: &mut IoBuffVecMut<1>) -> usize {
        chain
            .get_mut(0)
            .expect("aggregate-rejection segment missing")
            .as_mut_ptr() as usize
    }

    fn assert_readv_aggregate_rejection<F>(
        future: F,
        expected_ptr: usize,
        cx: &mut Context<'_>,
        expected_kind: io::ErrorKind,
    ) where
        F: Future<Output = (io::Result<usize>, IoBuffVecMut<1>)>,
    {
        let mut future = Box::pin(future);
        let Poll::Ready((result, mut returned)) = future.as_mut().poll(cx) else {
            panic!("invalid read aggregate remained pending");
        };
        let err = result.expect_err("invalid read aggregate unexpectedly succeeded");
        assert_eq!(err.kind(), expected_kind);
        if expected_kind == io::ErrorKind::InvalidInput {
            assert_eq!(err.to_string(), READV_AGGREGATE_OVERFLOW);
        }
        assert_eq!(aggregate_rejection_chain_ptr(&mut returned), expected_ptr);
        assert!(
            future.as_mut().poll(cx).is_pending(),
            "completed invalid-read-aggregate future did not fuse"
        );
    }

    #[test]
    fn readv_aggregate_overflow_preserves_context_precedence_and_returns_exact_chain() {
        let mut readv =
            ReadvFuture::<1, ()>::new(invalid_fd_capability(), aggregate_rejection_test_chain());
        readv.deferred_error = DeferredReadvError::AggregateOverflow;
        readv.iov_count = 0;
        readv.writable = 0;
        let expected_ptr = aggregate_rejection_chain_ptr(
            readv
                .buffer
                .as_mut()
                .expect("readv rejection chain missing"),
        );
        let mut outside_cx = Context::from_waker(std::task::Waker::noop());
        assert_readv_aggregate_rejection(
            readv,
            expected_ptr,
            &mut outside_cx,
            io::ErrorKind::NotConnected,
        );

        let mut readv_exact = ReadvExactFuture::<1, ()>::new(
            invalid_fd_capability(),
            aggregate_rejection_test_chain(),
            1,
        );
        readv_exact.input_error = Some(invalid_readv_aggregate());
        let expected_ptr = aggregate_rejection_chain_ptr(
            readv_exact
                .buffer
                .as_mut()
                .expect("readv_exact rejection chain missing"),
        );
        assert_readv_aggregate_rejection(
            readv_exact,
            expected_ptr,
            &mut outside_cx,
            io::ErrorKind::NotConnected,
        );

        with_ringless_poll_context_for_test(1, |owner, cx| {
            let reactor = owner.reactor_ptr();

            let mut readv = ReadvFuture::<1, ()>::new(
                invalid_fd_capability(),
                aggregate_rejection_test_chain(),
            );
            readv.deferred_error = DeferredReadvError::AggregateOverflow;
            readv.iov_count = 0;
            readv.writable = 0;
            let expected_ptr = aggregate_rejection_chain_ptr(
                readv
                    .buffer
                    .as_mut()
                    .expect("readv rejection chain missing"),
            );
            assert_readv_aggregate_rejection(readv, expected_ptr, cx, io::ErrorKind::InvalidInput);

            let mut readv_exact = ReadvExactFuture::<1, ()>::new(
                invalid_fd_capability(),
                aggregate_rejection_test_chain(),
                1,
            );
            readv_exact.input_error = Some(invalid_readv_aggregate());
            let expected_ptr = aggregate_rejection_chain_ptr(
                readv_exact
                    .buffer
                    .as_mut()
                    .expect("readv_exact rejection chain missing"),
            );
            assert_readv_aggregate_rejection(
                readv_exact,
                expected_ptr,
                cx,
                io::ErrorKind::InvalidInput,
            );

            assert_eq!(unsafe { (&*reactor).live_op_count() }, 0);
            assert_eq!(owner.inflight_op_count_for_test(), 0);
            let stats = unsafe { (&*reactor).retained_payload_stats() };
            assert_eq!(stats.pooled_allocs, 0);
            assert_eq!(stats.heap_fallbacks, 0);
            assert_eq!(stats.writev_scratch_inline_allocs, 0);
            assert_eq!(stats.writev_scratch_pooled_allocs, 0);
            assert_eq!(stats.writev_scratch_oversize_rejections, 0);
        });
    }

    fn assert_retained_iovec_initialized_prefix<const N: usize>() {
        let mut pool = RetainedPayloadPool::new().expect("retained pool creation failed");
        let segments: [IoBuffMut; N] = std::array::from_fn(|_| {
            IoBuffMut::new(0, 1, 0).expect("readv prefix segment allocation failed")
        });
        let mut buffer = Some(IoBuffVecMut::from_array(segments));
        let scratch_init = pool
            .alloc_iovec_scratch_init(N)
            .expect("readv prefix scratch allocation failed");
        let pool_ptr = NonNull::from(&mut pool);
        let mut payload =
            unsafe { emplace_retained_readv_payload(pool_ptr, &mut buffer, scratch_init) };
        assert!(buffer.is_none(), "readv prefix constructor lost ownership");

        unsafe {
            let retained = payload.as_mut();
            let first_ptr = retained
                .buffer
                .get_mut(0)
                .expect("first readv prefix segment missing")
                .as_mut_ptr();
            retained.scratch.as_uninit_slice_mut()[0].write(libc::iovec {
                iov_base: first_ptr.cast(),
                iov_len: 1,
            });
            let initialized = retained_iovecs_mut(&mut retained.scratch, 1);
            assert_eq!(initialized.len(), 1);
            assert_eq!(initialized[0].iov_base, first_ptr.cast());
            assert_eq!(initialized[0].iov_len, 1);
        }

        let returned = unsafe { payload.take(&mut pool) };
        assert_eq!(returned.buffer.segments(), N);
        drop(returned);
        let stats = pool.stats();
        assert_eq!(stats.pooled_allocs, 1);
        assert_eq!(stats.pooled_frees, 1);
        if N > RETAINED_IOVEC_INLINE_COUNT {
            assert_eq!(stats.writev_scratch_pooled_allocs, 1);
            assert_eq!(stats.writev_scratch_pooled_frees, 1);
        }
    }

    #[test]
    fn retained_iovecs_mut_exposes_only_initialized_prefix() {
        assert_retained_iovec_initialized_prefix::<2>();
        assert_retained_iovec_initialized_prefix::<{ RETAINED_IOVEC_INLINE_COUNT + 1 }>();
    }

    #[derive(Clone, Copy, Debug)]
    enum ReadvShapeDrift {
        Growth,
        Shrink,
        Total,
    }

    fn readv_shape_drift_chain(drift: ReadvShapeDrift) -> IoBuffVecMut<2> {
        match drift {
            ReadvShapeDrift::Growth => {
                let mut full =
                    IoBuffMut::new(0, 4, 0).expect("full readv segment allocation failed");
                full.payload_append(b"full")
                    .expect("full readv segment initialization failed");
                let writable =
                    IoBuffMut::new(0, 4, 0).expect("writable readv segment allocation failed");
                IoBuffVecMut::from_array([full, writable])
            }
            ReadvShapeDrift::Shrink | ReadvShapeDrift::Total => {
                let first = IoBuffMut::new(0, 4, 0).expect("first readv segment allocation failed");
                let second =
                    IoBuffMut::new(0, 4, 0).expect("second readv segment allocation failed");
                IoBuffVecMut::from_array([first, second])
            }
        }
    }

    fn mutate_readv_shape(chain: &mut IoBuffVecMut<2>, drift: ReadvShapeDrift) {
        let first = chain
            .get_mut(0)
            .expect("first readv shape-drift segment missing");
        match drift {
            ReadvShapeDrift::Growth => first.reset(),
            ReadvShapeDrift::Shrink => first
                .payload_append(b"full")
                .expect("readv shrink mutation failed"),
            ReadvShapeDrift::Total => first
                .payload_append(b"x")
                .expect("readv total mutation failed"),
        }
    }

    fn readv_chain_ptrs(chain: &mut IoBuffVecMut<2>) -> [usize; 2] {
        std::array::from_fn(|index| {
            chain
                .get_mut(index)
                .expect("readv shape-drift segment missing")
                .as_mut_ptr() as usize
        })
    }

    fn assert_readv_shape_rejection<F>(
        future: F,
        expected_ptrs: [usize; 2],
        cx: &mut Context<'_>,
        reactor: *mut Reactor,
        owner: &crate::runtime::executor::ExecutorOwner,
    ) where
        F: Future<Output = (io::Result<usize>, IoBuffVecMut<2>)>,
    {
        let mut future = Box::pin(future);
        let Poll::Ready((result, mut returned)) = future.as_mut().poll(cx) else {
            panic!("readv shape drift remained pending");
        };
        let err = result.expect_err("readv shape drift unexpectedly succeeded");
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
        assert_eq!(
            err.to_string(),
            crate::runtime::buffer::iobuffvec::READ_IOVEC_SHAPE_CHANGED
        );
        assert_eq!(readv_chain_ptrs(&mut returned), expected_ptrs);
        assert!(
            future.as_mut().poll(cx).is_pending(),
            "completed readv shape-drift future did not fuse"
        );
        assert_eq!(unsafe { (&*reactor).live_op_count() }, 0);
        assert_eq!(owner.inflight_op_count_for_test(), 0);
        drop(returned);
    }

    #[test]
    fn readv_shape_drift_futures_reject_before_submission_and_return_chain() {
        with_ringless_poll_context_for_test(1, |owner, cx| {
            let reactor = owner.reactor_ptr();

            for drift in [
                ReadvShapeDrift::Growth,
                ReadvShapeDrift::Shrink,
                ReadvShapeDrift::Total,
            ] {
                let mut future = ReadvFuture::<2, ()>::new(
                    invalid_fd_capability(),
                    readv_shape_drift_chain(drift),
                );
                let chain = future.buffer.as_mut().expect("readv chain missing");
                mutate_readv_shape(chain, drift);
                let expected_ptrs = readv_chain_ptrs(chain);
                assert_readv_shape_rejection(future, expected_ptrs, cx, reactor, owner);

                let mut future = ReadvExactFuture::<2, ()>::new(
                    invalid_fd_capability(),
                    readv_shape_drift_chain(drift),
                    1,
                );
                let chain = future.buffer.as_mut().expect("readv_exact chain missing");
                mutate_readv_shape(chain, drift);
                let expected_ptrs = readv_chain_ptrs(chain);
                assert_readv_shape_rejection(future, expected_ptrs, cx, reactor, owner);
            }

            let stats = unsafe { (&*reactor).retained_payload_stats() };
            assert_eq!(stats.pooled_allocs, 6);
            assert_eq!(stats.pooled_frees, 6);
            assert_eq!(stats.writev_scratch_inline_allocs, 6);
        });
    }

    struct RetainedWritevConstructorChain {
        bytes: Box<[u8; 32]>,
        reenter_pool: Option<NonNull<RetainedPayloadPool>>,
        fill_calls: Rc<Cell<usize>>,
        drops: Rc<Cell<usize>>,
        panic_on_fill: bool,
    }

    impl Drop for RetainedWritevConstructorChain {
        fn drop(&mut self) {
            self.drops.set(self.drops.get() + 1);
        }
    }

    impl super::write_buffer_chain_sealed::Sealed<1> for RetainedWritevConstructorChain {
        fn write_iovec_count_and_len(&self) -> Option<(usize, usize)> {
            Some((1, self.bytes.len()))
        }

        fn fill_write_iovecs(
            &self,
            dst: &mut [MaybeUninit<libc::iovec>],
        ) -> io::Result<(usize, usize)> {
            self.fill_calls.set(self.fill_calls.get() + 1);
            if let Some(mut pool) = self.reenter_pool {
                // SAFETY: the constructor callback runs synchronously on the
                // owner thread. Its outer raw slot retains no Rust borrow of
                // this same retained pool across callback-capable work.
                let nested = unsafe { pool.as_mut().alloc(0x68_u64) };
                let value = unsafe { nested.take(pool.as_mut()) };
                assert_eq!(value, 0x68);
            }
            assert!(!self.panic_on_fill, "intentional writev fill panic");
            assert_eq!(dst.len(), 1);
            dst[0].write(libc::iovec {
                iov_base: self.bytes.as_ptr() as *mut libc::c_void,
                iov_len: self.bytes.len(),
            });
            Ok((1, self.bytes.len()))
        }
    }

    fn retained_writev_constructor_chain(
        pool: Option<NonNull<RetainedPayloadPool>>,
        fill_calls: &Rc<Cell<usize>>,
        drops: &Rc<Cell<usize>>,
        panic_on_fill: bool,
    ) -> RetainedWritevConstructorChain {
        RetainedWritevConstructorChain {
            bytes: Box::new([0x5a; 32]),
            reenter_pool: pool,
            fill_calls: Rc::clone(fill_calls),
            drops: Rc::clone(drops),
            panic_on_fill,
        }
    }

    #[test]
    fn retained_writev_constructor_recycles_raw_slot_after_callback_panics() {
        let mut pool = RetainedPayloadPool::new().expect("retained pool creation failed");
        let fill_calls = Rc::new(Cell::new(0));
        let after_fill_calls = Rc::new(Cell::new(0));
        let drops = Rc::new(Cell::new(0));
        let mut buffer = Some(retained_writev_constructor_chain(
            None,
            &fill_calls,
            &drops,
            true,
        ));
        let scratch_init = pool
            .alloc_iovec_scratch_init(1)
            .expect("inline scratch token allocation failed");
        let pool_ptr = NonNull::from(&mut pool);

        let unwind = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| unsafe {
            emplace_retained_writev_payload::<_, 1, _>(
                pool_ptr,
                &mut buffer,
                scratch_init,
                32,
                || {
                    after_fill_calls.set(after_fill_calls.get() + 1);
                    Ok(())
                },
            )
        }));
        assert!(unwind.is_err(), "writev callback should unwind");
        assert!(buffer.is_some(), "callback panic moved caller source");
        assert_eq!(fill_calls.get(), 1);
        assert_eq!(after_fill_calls.get(), 0);
        assert_eq!(drops.get(), 0);
        let after_unwind = pool.stats();
        assert_eq!(after_unwind.pooled_allocs, 1);
        assert_eq!(after_unwind.pooled_frees, 1);
        assert_eq!(after_unwind.pooled_reuses, 0);

        buffer.as_mut().unwrap().panic_on_fill = false;
        let scratch_init = pool
            .alloc_iovec_scratch_init(1)
            .expect("retry scratch token allocation failed");
        let pool_ptr = NonNull::from(&mut pool);
        let (payload, ()) = unsafe {
            emplace_retained_writev_payload::<_, 1, _>(
                pool_ptr,
                &mut buffer,
                scratch_init,
                32,
                || {
                    after_fill_calls.set(after_fill_calls.get() + 1);
                    Ok(())
                },
            )
        }
        .expect("retry emplacement failed");
        assert!(buffer.is_none());
        assert_eq!(fill_calls.get(), 2);
        assert_eq!(after_fill_calls.get(), 1);

        let retained = unsafe { payload.as_ref() };
        let iovec = unsafe { retained.scratch.as_uninit_slice()[0].assume_init_ref() };
        assert_eq!(iovec.iov_base, retained.buffer.bytes.as_ptr() as *mut _);
        assert_eq!(iovec.iov_len, retained.buffer.bytes.len());
        let scratch_start = std::ptr::addr_of!(retained.scratch) as usize;
        let scratch_end = scratch_start + std::mem::size_of::<RetainedIovecScratch>();
        let iovec_start = retained.scratch.as_uninit_slice().as_ptr() as usize;
        assert!(
            (scratch_start..scratch_end).contains(&iovec_start),
            "inline iovec does not reside in final retained scratch"
        );

        let after_retry = pool.stats();
        assert_eq!(after_retry.pooled_allocs, 2);
        assert_eq!(after_retry.pooled_frees, 1);
        assert_eq!(after_retry.pooled_reuses, 1);
        let returned = unsafe { payload.take(&mut pool) };
        drop(returned);
        assert_eq!(drops.get(), 1);
        assert_eq!(pool.stats().pooled_frees, 2);
    }

    #[test]
    fn retained_writev_constructor_allows_same_pool_reentry() {
        let mut pool = RetainedPayloadPool::new().expect("retained pool creation failed");
        let fill_calls = Rc::new(Cell::new(0));
        let drops = Rc::new(Cell::new(0));
        let scratch_init = pool
            .alloc_iovec_scratch_init(1)
            .expect("inline scratch token allocation failed");
        let pool_ptr = NonNull::from(&mut pool);
        let mut buffer = Some(retained_writev_constructor_chain(
            Some(pool_ptr),
            &fill_calls,
            &drops,
            false,
        ));

        let (payload, ()) = unsafe {
            emplace_retained_writev_payload::<_, 1, _>(
                pool_ptr,
                &mut buffer,
                scratch_init,
                32,
                || Ok(()),
            )
        }
        .expect("reentrant emplacement failed");
        assert!(buffer.is_none());
        assert_eq!(fill_calls.get(), 1);
        assert_eq!(drops.get(), 0);

        let retained = unsafe { payload.as_ref() };
        let iovec = unsafe { retained.scratch.as_uninit_slice()[0].assume_init_ref() };
        assert_eq!(iovec.iov_base, retained.buffer.bytes.as_ptr() as *mut _);
        assert_eq!(iovec.iov_len, retained.buffer.bytes.len());

        let returned = unsafe { payload.take(&mut pool) };
        drop(returned);
        assert_eq!(drops.get(), 1);
        let stats = pool.stats();
        assert_eq!(stats.pooled_allocs, 2, "outer plus reentrant allocation");
        assert_eq!(stats.pooled_frees, 2, "outer plus reentrant release");
    }

    #[test]
    fn retained_projected_writev_unsubmitted_drop_does_not_read_message_header() {
        struct DeferredProjection([u8; 4]);

        impl WritevProjection for DeferredProjection {
            fn writev_count_and_len(&self) -> (usize, usize) {
                (1, self.0.len())
            }

            fn project_writev<'a>(&'a self, pieces: &mut WritevPieces<'a>) -> io::Result<()> {
                pieces.push(&self.0)
            }
        }

        let mut pool = RetainedPayloadPool::new().expect("retained pool creation failed");
        let scratch_init = pool
            .alloc_iovec_scratch_init(1)
            .expect("inline scratch token allocation failed");
        let pool_ptr = NonNull::from(&mut pool);
        let mut source = Some(DeferredProjection([1, 2, 3, 4]));
        let payload = unsafe {
            emplace_retained_projected_writev_payload(pool_ptr, &mut source, scratch_init)
        };
        assert!(source.is_none(), "projected source was not transferred");

        // This is the projection-failure/cancellation shape: retained storage
        // retires before any SENDMSG builder initializes its message header.
        let returned = unsafe { payload.take(&mut pool) };
        assert_eq!(returned.source.0, [1, 2, 3, 4]);
        drop(returned);
        assert_eq!(pool.stats().pooled_frees, 1);
    }

    #[test]
    fn projected_try_write_scratch_reserves_to_target_capacity() {
        let mut scratch = Vec::<MaybeUninit<libc::iovec>>::with_capacity(300);
        assert_eq!(scratch.len(), 0);
        assert!(
            scratch.capacity() < 512,
            "test requires a below-target starting capacity"
        );

        reserve_projected_scratch_capacity(&mut scratch, 512)
            .expect("projected scratch reserve should succeed");
        assert!(
            scratch.capacity() >= 512,
            "projected scratch reserve must grow to the target capacity"
        );
        assert_eq!(scratch.len(), 0);
    }

    struct ProjectedTryScratchFixture {
        previous: Option<Vec<MaybeUninit<libc::iovec>>>,
        allocation: *const MaybeUninit<libc::iovec>,
        capacity: usize,
        len: usize,
    }

    impl ProjectedTryScratchFixture {
        fn install(capacity: usize) -> Self {
            TRY_WRITEV_PROJECTED_SCRATCH
                .try_with(|cell| {
                    let mut scratch = Vec::with_capacity(capacity);
                    let actual_capacity = scratch.capacity();
                    scratch.resize_with(actual_capacity, MaybeUninit::uninit);
                    let allocation = scratch.as_ptr();
                    let len = scratch.len();
                    let previous = cell.replace(scratch);
                    Self {
                        previous: Some(previous),
                        allocation,
                        capacity: actual_capacity,
                        len,
                    }
                })
                .expect("projected-write scratch TLS should be available in a unit test")
        }

        fn assert_unchanged(&self) {
            TRY_WRITEV_PROJECTED_SCRATCH
                .try_with(|cell| {
                    let scratch = cell.borrow();
                    assert_eq!(scratch.as_ptr(), self.allocation);
                    assert_eq!(scratch.capacity(), self.capacity);
                    assert_eq!(scratch.len(), self.len);
                })
                .expect("projected-write scratch TLS disappeared during a unit test");
        }
    }

    impl Drop for ProjectedTryScratchFixture {
        fn drop(&mut self) {
            let previous = self
                .previous
                .take()
                .expect("projected-write scratch fixture restored twice");
            TRY_WRITEV_PROJECTED_SCRATCH
                .try_with(|cell| drop(cell.replace(previous)))
                .expect("projected-write scratch TLS disappeared before fixture restoration");
        }
    }

    struct TrackedProjectedTryCarrier {
        id: usize,
        token: Box<u8>,
        bytes: [u8; 17],
        projection_calls: Rc<Cell<usize>>,
        drops: Rc<Cell<usize>>,
        saw_borrowed_tls_scratch: Rc<Cell<bool>>,
        projection_error: Option<i32>,
    }

    impl TrackedProjectedTryCarrier {
        fn new(
            id: usize,
            projection_calls: &Rc<Cell<usize>>,
            drops: &Rc<Cell<usize>>,
            saw_borrowed_tls_scratch: &Rc<Cell<bool>>,
            projection_error: Option<i32>,
        ) -> Self {
            Self {
                id,
                token: Box::new(id as u8),
                bytes: [0x5a; 17],
                projection_calls: Rc::clone(projection_calls),
                drops: Rc::clone(drops),
                saw_borrowed_tls_scratch: Rc::clone(saw_borrowed_tls_scratch),
                projection_error,
            }
        }

        fn token_ptr(&self) -> *const u8 {
            self.token.as_ref()
        }
    }

    impl WritevProjection for TrackedProjectedTryCarrier {
        fn writev_count_and_len(&self) -> (usize, usize) {
            (self.bytes.len(), self.bytes.len())
        }

        fn project_writev<'a>(&'a self, pieces: &mut WritevPieces<'a>) -> io::Result<()> {
            self.projection_calls.set(self.projection_calls.get() + 1);
            let tls_scratch_is_borrowed = TRY_WRITEV_PROJECTED_SCRATCH
                .try_with(|cell| cell.try_borrow_mut().is_err())
                .expect("projected-write scratch TLS should remain addressable");
            self.saw_borrowed_tls_scratch.set(tls_scratch_is_borrowed);
            for byte in &self.bytes {
                pieces.push(std::slice::from_ref(byte))?;
            }
            match self.projection_error {
                Some(raw_error) => Err(io::Error::from_raw_os_error(raw_error)),
                None => Ok(()),
            }
        }
    }

    impl Drop for TrackedProjectedTryCarrier {
        fn drop(&mut self) {
            self.drops.set(self.drops.get() + 1);
        }
    }

    struct ReentrantProjectedTryCarrier {
        id: usize,
        token: Box<u8>,
        bytes: [u8; 17],
        projection_calls: Rc<Cell<usize>>,
        drops: Rc<Cell<usize>>,
        inner: RefCell<Option<TrackedProjectedTryCarrier>>,
        inner_error: Cell<Option<i32>>,
    }

    impl ReentrantProjectedTryCarrier {
        fn token_ptr(&self) -> *const u8 {
            self.token.as_ref()
        }

        fn inner_identity(&self) -> (usize, *const u8) {
            let inner = self.inner.borrow();
            let inner = inner
                .as_ref()
                .expect("reentrant projection did not restore its inner carrier");
            (inner.id, inner.token_ptr())
        }
    }

    impl WritevProjection for ReentrantProjectedTryCarrier {
        fn writev_count_and_len(&self) -> (usize, usize) {
            (self.bytes.len(), self.bytes.len())
        }

        fn project_writev<'a>(&'a self, pieces: &mut WritevPieces<'a>) -> io::Result<()> {
            self.projection_calls.set(self.projection_calls.get() + 1);

            let inner = self
                .inner
                .borrow_mut()
                .take()
                .expect("reentrant projection lost its inner carrier");
            let (inner_result, inner) = try_writev_projected_once(-1, inner);
            let inner_error = inner_result
                .expect_err("inner projected write should fail")
                .raw_os_error();
            self.inner_error.set(inner_error);
            assert!(
                self.inner.replace(Some(inner)).is_none(),
                "reentrant projection replaced a live inner carrier"
            );

            for byte in &self.bytes {
                pieces.push(std::slice::from_ref(byte))?;
            }
            Err(io::Error::from_raw_os_error(libc::ECANCELED))
        }
    }

    impl Drop for ReentrantProjectedTryCarrier {
        fn drop(&mut self) {
            self.drops.set(self.drops.get() + 1);
        }
    }

    #[test]
    fn projected_try_write_available_tls_propagates_projection_error_once() {
        let scratch = ProjectedTryScratchFixture::install(32);
        let projection_calls = Rc::new(Cell::new(0));
        let drops = Rc::new(Cell::new(0));
        let saw_borrowed_tls_scratch = Rc::new(Cell::new(false));
        let source = TrackedProjectedTryCarrier::new(
            0x71,
            &projection_calls,
            &drops,
            &saw_borrowed_tls_scratch,
            Some(libc::EPROTO),
        );
        let token = source.token_ptr();

        let (result, source) = try_writev_projected_once(-1, source);

        assert_eq!(
            result
                .expect_err("projection error should bypass the socket syscall")
                .raw_os_error(),
            Some(libc::EPROTO)
        );
        assert_eq!(projection_calls.get(), 1, "projection was retried");
        assert!(
            saw_borrowed_tls_scratch.get(),
            "available TLS scratch was not borrowed around projection"
        );
        assert_eq!(source.id, 0x71);
        assert_eq!(source.token_ptr(), token);
        assert_eq!(drops.get(), 0, "projection failure dropped the source");
        scratch.assert_unchanged();

        drop(source);
        assert_eq!(drops.get(), 1, "returned source did not drop exactly once");
    }

    #[test]
    fn projected_try_write_reentry_uses_fresh_scratch_and_returns_both_carriers() {
        let scratch = ProjectedTryScratchFixture::install(32);
        let outer_projection_calls = Rc::new(Cell::new(0));
        let outer_drops = Rc::new(Cell::new(0));
        let inner_projection_calls = Rc::new(Cell::new(0));
        let inner_drops = Rc::new(Cell::new(0));
        let inner_saw_borrowed_tls_scratch = Rc::new(Cell::new(false));
        let inner = TrackedProjectedTryCarrier::new(
            0x72,
            &inner_projection_calls,
            &inner_drops,
            &inner_saw_borrowed_tls_scratch,
            Some(libc::EPROTO),
        );
        let inner_token = inner.token_ptr();
        let source = ReentrantProjectedTryCarrier {
            id: 0x73,
            token: Box::new(0x73),
            bytes: [0x6b; 17],
            projection_calls: Rc::clone(&outer_projection_calls),
            drops: Rc::clone(&outer_drops),
            inner: RefCell::new(Some(inner)),
            inner_error: Cell::new(None),
        };
        let outer_token = source.token_ptr();

        let (result, source) = try_writev_projected_once(-1, source);

        assert_eq!(
            result
                .expect_err("outer projection should fail before its socket syscall")
                .raw_os_error(),
            Some(libc::ECANCELED)
        );
        assert_eq!(outer_projection_calls.get(), 1);
        assert_eq!(
            inner_projection_calls.get(),
            1,
            "inner projection was retried"
        );
        assert_eq!(source.inner_error.get(), Some(libc::EPROTO));
        assert!(
            inner_saw_borrowed_tls_scratch.get(),
            "inner projection did not run while the outer TLS scratch was borrowed"
        );
        assert_eq!(source.id, 0x73);
        assert_eq!(source.token_ptr(), outer_token);
        assert_eq!(source.inner_identity(), (0x72, inner_token));
        assert_eq!(outer_drops.get(), 0, "outer source was dropped early");
        assert_eq!(inner_drops.get(), 0, "inner source was dropped early");
        scratch.assert_unchanged();

        drop(source);
        assert_eq!(
            outer_drops.get(),
            1,
            "outer source did not drop exactly once"
        );
        assert_eq!(
            inner_drops.get(),
            1,
            "inner source did not drop exactly once"
        );
    }

    #[cfg(not(miri))]
    #[test]
    fn projected_try_write_reentry_propagates_inner_syscall_error_once() {
        let scratch = ProjectedTryScratchFixture::install(32);
        let outer_projection_calls = Rc::new(Cell::new(0));
        let outer_drops = Rc::new(Cell::new(0));
        let inner_projection_calls = Rc::new(Cell::new(0));
        let inner_drops = Rc::new(Cell::new(0));
        let inner_saw_borrowed_tls_scratch = Rc::new(Cell::new(false));
        let inner = TrackedProjectedTryCarrier::new(
            0x74,
            &inner_projection_calls,
            &inner_drops,
            &inner_saw_borrowed_tls_scratch,
            None,
        );
        let inner_token = inner.token_ptr();
        let source = ReentrantProjectedTryCarrier {
            id: 0x75,
            token: Box::new(0x75),
            bytes: [0x6b; 17],
            projection_calls: Rc::clone(&outer_projection_calls),
            drops: Rc::clone(&outer_drops),
            inner: RefCell::new(Some(inner)),
            inner_error: Cell::new(None),
        };
        let outer_token = source.token_ptr();

        let (result, source) = try_writev_projected_once(-1, source);

        assert_eq!(
            result
                .expect_err("outer projection should fail before its socket syscall")
                .raw_os_error(),
            Some(libc::ECANCELED)
        );
        assert_eq!(outer_projection_calls.get(), 1);
        assert_eq!(inner_projection_calls.get(), 1, "inner write was retried");
        assert_eq!(source.inner_error.get(), Some(libc::EBADF));
        assert!(
            inner_saw_borrowed_tls_scratch.get(),
            "inner syscall path did not use fresh scratch during re-entry"
        );
        assert_eq!(source.id, 0x75);
        assert_eq!(source.token_ptr(), outer_token);
        assert_eq!(source.inner_identity(), (0x74, inner_token));
        assert_eq!(outer_drops.get(), 0, "outer source was dropped early");
        assert_eq!(inner_drops.get(), 0, "inner source was dropped early");
        scratch.assert_unchanged();

        drop(source);
        assert_eq!(outer_drops.get(), 1);
        assert_eq!(inner_drops.get(), 1);
    }

    #[test]
    fn projected_try_write_limits_follow_retained_scratch_limits() {
        struct OversizedProjection {
            count: usize,
        }

        impl WritevProjection for OversizedProjection {
            fn writev_count_and_len(&self) -> (usize, usize) {
                (self.count, self.count)
            }

            fn project_writev<'a>(&'a self, _pieces: &mut WritevPieces<'a>) -> io::Result<()> {
                panic!("oversized projections must be rejected before projection");
            }
        }

        assert_eq!(TRY_WRITEV_INLINE_IOVECS, RETAINED_IOVEC_INLINE_COUNT);
        assert_eq!(TRY_WRITEV_MAX_IOVECS, RETAINED_IOVEC_MAX_COUNT);

        let source = OversizedProjection {
            count: TRY_WRITEV_MAX_IOVECS + 1,
        };
        let (result, source) = try_writev_projected_once(-1, source);
        assert_eq!(
            result
                .expect_err("oversized projection should be rejected")
                .kind(),
            io::ErrorKind::InvalidInput
        );
        assert_eq!(source.count, TRY_WRITEV_MAX_IOVECS + 1);
    }

    #[test]
    fn read_exact_iobuff_future_preserves_layout() {
        assert_eq!(
            std::mem::size_of::<ReadExactFuture<'static, IoBuffMut, ()>>(),
            88
        );
        assert_eq!(
            std::mem::align_of::<ReadExactFuture<'static, IoBuffMut, ()>>(),
            8
        );
    }

    #[test]
    fn flattened_writev_futures_keep_raw_state_layout_and_drop() {
        type Chain = IoBuffVec<2>;
        type OneShot = WritevFuture<'static, Chain, 2, ()>;
        type All = WritevAllFuture<'static, Chain, 2, ()>;

        #[cfg(target_pointer_width = "64")]
        {
            assert_eq!(std::mem::size_of::<OneShot>(), 112);
            assert_eq!(std::mem::size_of::<All>(), 112);
        }
        assert_eq!(std::mem::size_of::<OneShot>(), std::mem::size_of::<All>());
        assert_eq!(std::mem::align_of::<OneShot>(), std::mem::align_of::<All>());
        assert!(std::mem::needs_drop::<OneShot>());
        assert!(std::mem::needs_drop::<All>());
    }

    #[test]
    fn projected_try_write_scratch_keeps_capacity_len_across_reuse() {
        struct UnderProjected([u8; 16]);

        impl WritevProjection for UnderProjected {
            fn writev_count_and_len(&self) -> (usize, usize) {
                (17, 17)
            }

            fn project_writev<'a>(&'a self, pieces: &mut WritevPieces<'a>) -> io::Result<()> {
                for byte in &self.0 {
                    pieces.push(std::slice::from_ref(byte))?;
                }
                Ok(())
            }
        }

        let mut scratch = Vec::<MaybeUninit<libc::iovec>>::with_capacity(32);
        let allocation = scratch.as_ptr();
        let capacity = scratch.capacity();
        let source = UnderProjected([0x5a; 16]);

        for _ in 0..2 {
            let result = try_writev_projected_with_vec_scratch(-1, &source, 17, 17, &mut scratch);
            assert_eq!(
                result.expect_err("under-projection should fail").kind(),
                io::ErrorKind::InvalidInput
            );
            assert_eq!(scratch.as_ptr(), allocation);
            assert_eq!(scratch.capacity(), capacity);
            assert_eq!(scratch.len(), capacity);
        }
    }

    #[test]
    fn stream_retry_cqe_result_classifies_error_zero_and_progress() {
        assert_eq!(
            classify_retry_cqe_result(-libc::EPIPE),
            RetryCqeResult::KernelError(libc::EPIPE)
        );
        assert_eq!(classify_retry_cqe_result(0), RetryCqeResult::Zero);
        assert_eq!(classify_retry_cqe_result(7), RetryCqeResult::Bytes(7));
    }

    #[test]
    fn advance_iovecs_in_place_handles_partial_and_full_entries() {
        let mut bytes = [0u8; 16];
        let base = bytes.as_mut_ptr();
        let mut iovecs = [
            libc::iovec {
                iov_base: base.cast(),
                iov_len: 3,
            },
            libc::iovec {
                iov_base: unsafe { base.add(3) }.cast(),
                iov_len: 5,
            },
            libc::iovec {
                iov_base: unsafe { base.add(8) }.cast(),
                iov_len: 8,
            },
        ];
        let mut skip = 0usize;

        advance_iovecs_in_place(&mut iovecs, &mut skip, 4);
        assert_eq!(skip, 1);
        assert_eq!(iovecs[1].iov_base, unsafe { base.add(4) }.cast());
        assert_eq!(iovecs[1].iov_len, 4);

        advance_iovecs_in_place(&mut iovecs, &mut skip, 4);
        assert_eq!(skip, 2);
        assert_eq!(iovecs[2].iov_base, unsafe { base.add(8) }.cast());
        assert_eq!(iovecs[2].iov_len, 8);
    }

    #[test]
    fn advance_iovecs_in_place_zero_bytes_is_noop() {
        let mut bytes = [0u8; 8];
        let base = bytes.as_mut_ptr();
        let mut iovecs = [
            libc::iovec {
                iov_base: base.cast(),
                iov_len: 3,
            },
            libc::iovec {
                iov_base: unsafe { base.add(3) }.cast(),
                iov_len: 5,
            },
        ];
        let mut skip = 1usize;

        advance_iovecs_in_place(&mut iovecs, &mut skip, 0);

        assert_eq!(skip, 1);
        assert_eq!(iovecs[0].iov_base, base.cast());
        assert_eq!(iovecs[0].iov_len, 3);
        assert_eq!(iovecs[1].iov_base, unsafe { base.add(3) }.cast());
        assert_eq!(iovecs[1].iov_len, 5);
    }

    #[test]
    fn clamp_iovecs_to_read_limit_clamps_from_skip_index() {
        let mut bytes = [0u8; 16];
        let base = bytes.as_mut_ptr();
        let mut iovecs = [
            libc::iovec {
                iov_base: base.cast(),
                iov_len: 3,
            },
            libc::iovec {
                iov_base: unsafe { base.add(3) }.cast(),
                iov_len: 5,
            },
            libc::iovec {
                iov_base: unsafe { base.add(8) }.cast(),
                iov_len: 8,
            },
        ];

        let count = clamp_iovecs_to_read_limit(&mut iovecs, 1, 6);

        assert_eq!(count, 2);
        assert_eq!(iovecs[0].iov_len, 3);
        assert_eq!(iovecs[1].iov_len, 5);
        assert_eq!(iovecs[2].iov_len, 1);
    }

    #[test]
    fn clamp_iovecs_to_read_limit_keeps_exact_full_remaining_window() {
        let mut bytes = [0u8; 16];
        let base = bytes.as_mut_ptr();
        let mut iovecs = [
            libc::iovec {
                iov_base: base.cast(),
                iov_len: 3,
            },
            libc::iovec {
                iov_base: unsafe { base.add(3) }.cast(),
                iov_len: 5,
            },
            libc::iovec {
                iov_base: unsafe { base.add(8) }.cast(),
                iov_len: 8,
            },
        ];
        let count = clamp_iovecs_to_read_limit(&mut iovecs, 1, 13);

        assert_eq!(count, 2);
        assert_eq!(iovecs[1].iov_len, 5);
        assert_eq!(iovecs[2].iov_len, 8);
    }

    #[test]
    fn scalar_write_callers_clamp_initial_submissions_without_replacing_owners() {
        use crate::net::send_sqe::test_support::{
            ExpectedScalarEntry, ScalarTestFuture, assert_rejected_scalar_submission,
            next_operation_user_data,
        };

        for all in [false, true] {
            for requested in [i32::MAX as u32, i32::MAX as u32 + 1, u32::MAX] {
                with_ringless_poll_context_for_test(1, |owner, cx| {
                    let reactor = owner.reactor_ptr();
                    let expected_user_data = next_operation_user_data(owner);
                    let buffer = b"abcdefgh".to_vec();
                    let original = buffer.as_ptr();
                    // Only encoded lengths are synthetic. Submission fails before
                    // any kernel can access beyond this real buffer.
                    let mut future: ScalarTestFuture<'_> = if all {
                        let mut future =
                            WriteAllFuture::<_, ()>::new(invalid_fd_capability(), buffer);
                        future.total = requested;
                        Box::pin(future)
                    } else {
                        let mut future = WriteFuture::<_, ()>::new(invalid_fd_capability(), buffer);
                        future.len = requested;
                        Box::pin(future)
                    };
                    let returned = assert_rejected_scalar_submission(
                        cx,
                        &mut future,
                        ExpectedScalarEntry {
                            opcode: opcode::Send::CODE,
                            fd: RuntimeFd::INVALID,
                            addr: original,
                            owner_addr: original,
                            len: requested.min(i32::MAX as u32),
                            msg_flags: libc::MSG_NOSIGNAL as u32,
                            user_data: expected_user_data,
                        },
                        b"abcdefgh",
                    );
                    drop(returned);
                    assert_eq!(unsafe { (&*reactor).live_op_count() }, 0);
                    assert_eq!(owner.inflight_op_count_for_test(), 0);
                });
            }
        }
    }

    #[cfg(any(debug_assertions, feature = "test-support"))]
    #[test]
    fn scalar_write_all_retry_clamps_remaining_request_after_progress() {
        use crate::net::send_sqe::test_support::{
            ExpectedScalarEntry, assert_rejected_scalar_submission,
        };
        use crate::runtime::op::completion_retirement_test_support::stage_payload;

        for total in [i32::MAX as u32 + 1, i32::MAX as u32 + 2, u32::MAX] {
            with_ringless_poll_context_for_test(1, |owner, cx| {
                let buffer = b"abcdefgh".to_vec();
                let original = buffer.as_ptr();
                let mut future = WriteAllFuture::<_, ()>::new(invalid_fd_capability(), buffer);
                future.total = total;
                future.base_ptr = original;
                let payload = RetainedWritePayload {
                    buffer: future.buffer.take().expect("write owner missing"),
                };
                // The completed request consumed one real byte. Its next SQE
                // fails before the synthetic remaining length reaches a kernel.
                let saved =
                    unsafe { stage_payload(owner, &mut future.state_ptr, payload, 1, true) };
                let returned = assert_rejected_scalar_submission(
                    cx,
                    &mut future,
                    ExpectedScalarEntry {
                        opcode: opcode::Send::CODE,
                        fd: RuntimeFd::INVALID,
                        addr: unsafe { original.add(1) },
                        owner_addr: original,
                        len: (total - 1).min(i32::MAX as u32),
                        msg_flags: libc::MSG_NOSIGNAL as u32,
                        user_data: saved.state as u64,
                    },
                    b"abcdefgh",
                );
                assert_eq!(future.offset, 1);
                assert_eq!(future.total, total);
                drop(returned);
                assert_eq!(unsafe { (&*owner.reactor_ptr()).live_op_count() }, 0);
                assert_eq!(owner.inflight_op_count_for_test(), 0);
            });
        }
    }

    #[test]
    fn stream_write_vectored_single_window_clamps_send_and_keeps_larger_sendmsg() {
        let bytes = [1u8, 2, 3, 4];
        for len in [
            i32::MAX as usize,
            i32::MAX as usize + 1,
            u32::MAX as usize,
            u32::MAX as usize + 1,
        ] {
            // No operation is submitted; the artificial length is inspected only.
            let iovecs = [
                initialized_iovec(bytes.as_ptr(), 1),
                initialized_iovec(bytes.as_ptr(), len),
            ];
            let mut msg = sentinel_message_header();
            let entry = build_write_vectored_entry(7, &iovecs, 1, 1, &mut msg, 99);
            let sqe = sqe_prefix(&entry);
            assert_eq!(sqe.fd, 7);
            assert_eq!(sqe.flags, 0);
            assert_eq!(sqe.ioprio, 0);
            assert_eq!(sqe.off_or_addr2, 0);
            assert_eq!(sqe.msg_flags, libc::MSG_NOSIGNAL as u32);
            assert_eq!(sqe.user_data, 99);
            let header = unsafe { msg.assume_init_ref() };
            if len <= u32::MAX as usize {
                assert_eq!(sqe.opcode, opcode::Send::CODE);
                assert_eq!(sqe.addr, bytes.as_ptr() as u64);
                assert_eq!(sqe.len, len.min(i32::MAX as usize) as u32);
                let sentinel = unsafe { sentinel_message_header().assume_init() };
                assert_eq!(header.msg_name, sentinel.msg_name);
                assert_eq!(header.msg_namelen, sentinel.msg_namelen);
                assert_eq!(header.msg_iov, sentinel.msg_iov);
                assert_eq!(header.msg_iovlen, sentinel.msg_iovlen);
                assert_eq!(header.msg_control, sentinel.msg_control);
                assert_eq!(header.msg_controllen, sentinel.msg_controllen);
                assert_eq!(header.msg_flags, sentinel.msg_flags);
            } else {
                assert_eq!(sqe.opcode, opcode::SendMsg::CODE);
                assert_eq!(sqe.addr, msg.as_ptr() as u64);
                assert_eq!(sqe.len, 1);
                assert_eq!(
                    header.msg_iov,
                    std::ptr::addr_of!(iovecs[1]).cast_mut().cast()
                );
                assert_eq!(header.msg_iovlen, 1);
                assert_eq!(unsafe { (*header.msg_iov).iov_len }, len);
                assert!(header.msg_name.is_null());
                assert_eq!(header.msg_namelen, 0);
                assert!(header.msg_control.is_null());
                assert_eq!(header.msg_controllen, 0);
                assert_eq!(header.msg_flags, 0);
            }
        }
    }

    #[test]
    fn stream_write_caller_submits_send_with_nosignal() {
        use crate::net::send_sqe::test_support::{
            ExpectedScalarEntry, assert_rejected_scalar_submission, next_operation_user_data,
        };

        with_ringless_poll_context_for_test(1, |owner, cx| {
            let buffer = vec![1u8, 2, 3, 4];
            let original = buffer.as_ptr();
            let mut future = WriteFuture::<_, ()>::new(invalid_fd_capability(), buffer);
            let expected = ExpectedScalarEntry {
                opcode: opcode::Send::CODE,
                fd: RuntimeFd::INVALID,
                addr: original,
                owner_addr: original,
                len: 4,
                msg_flags: libc::MSG_NOSIGNAL as u32,
                user_data: next_operation_user_data(owner),
            };
            let returned =
                assert_rejected_scalar_submission(cx, &mut future, expected, &[1, 2, 3, 4]);
            assert_eq!(returned.as_ptr(), original);
        });
    }

    #[test]
    fn stream_read_vectored_single_entry_uses_recv_with_zero_flags() {
        let mut bytes = [0u8; 4];
        let iovecs = [
            initialized_iovec(bytes.as_ptr(), 1),
            initialized_iovec(bytes.as_mut_ptr(), bytes.len()),
        ];
        let mut msg = MaybeUninit::uninit();
        let entry = build_read_vectored_entry(7, &iovecs, 1, 1, &mut msg, 99);
        let sqe = sqe_prefix(&entry);

        assert_eq!(sqe.opcode, opcode::Recv::CODE);
        assert_eq!(sqe.flags, 0);
        assert_eq!(sqe.ioprio, 0);
        assert_eq!(sqe.fd, 7);
        assert_eq!(sqe.off_or_addr2, 0);
        assert_eq!(sqe.addr, bytes.as_ptr() as u64);
        assert_eq!(sqe.len, bytes.len() as u32);
        assert_eq!(sqe.msg_flags, 0);
        assert_eq!(sqe.user_data, 99);
    }

    #[test]
    fn stream_read_vectored_single_entry_respects_signed_length_boundary() {
        let byte = 0u8;
        for len in [i32::MAX as usize, i32::MAX as usize + 1, u32::MAX as usize] {
            // Only SQE fields are inspected; no operation accesses this range.
            let iovecs = [initialized_iovec(&byte, len)];
            let mut msg = MaybeUninit::uninit();
            let entry = build_read_vectored_entry(7, &iovecs, 0, 1, &mut msg, 99);
            let sqe = sqe_prefix(&entry);
            if len <= i32::MAX as usize {
                assert_eq!(sqe.opcode, opcode::Recv::CODE);
                assert_eq!(sqe.addr, std::ptr::addr_of!(byte) as u64);
                assert_eq!(sqe.len, len as u32);
            } else {
                assert_eq!(sqe.opcode, opcode::RecvMsg::CODE);
                assert_eq!(sqe.addr, msg.as_ptr() as u64);
                assert_eq!(sqe.len, 1);
                let header = unsafe { msg.assume_init_ref() };
                assert_eq!(header.msg_iov, iovecs.as_ptr().cast_mut().cast());
                assert_eq!(header.msg_iovlen, 1);
                assert_eq!(unsafe { (*header.msg_iov).iov_len }, len);
            }
            assert_eq!(sqe.fd, 7);
            assert_eq!(sqe.flags, 0);
            assert_eq!(sqe.ioprio, 0);
            assert_eq!(sqe.off_or_addr2, 0);
            assert_eq!(sqe.msg_flags, 0);
            assert_eq!(sqe.user_data, 99);
        }
    }

    #[test]
    fn stream_read_vectored_multi_entry_uses_recvmsg_window() {
        let bytes = [0u8; 4];
        let iovecs = [
            initialized_iovec(bytes.as_ptr(), 1),
            initialized_iovec(bytes.as_ptr(), 2),
            initialized_iovec(bytes.as_ptr(), 4),
        ];
        let mut msg = MaybeUninit::uninit();
        let entry = build_read_vectored_entry(7, &iovecs, 1, 2, &mut msg, 99);
        let sqe = sqe_prefix(&entry);

        assert_eq!(sqe.opcode, opcode::RecvMsg::CODE);
        assert_eq!(sqe.flags, 0);
        assert_eq!(sqe.ioprio, 0);
        assert_eq!(sqe.off_or_addr2, 0);
        assert_eq!(sqe.fd, 7);
        assert_eq!(sqe.addr, msg.as_ptr() as u64);
        assert_eq!(sqe.len, 1);
        let header = unsafe { msg.assume_init_ref() };
        assert_eq!(
            header.msg_iov,
            std::ptr::addr_of!(iovecs[1]).cast_mut().cast()
        );
        assert_eq!(header.msg_iovlen, 2);
        assert_eq!(sqe.msg_flags, 0);
        assert_eq!(sqe.user_data, 99);
    }

    #[test]
    fn stream_write_vectored_single_entry_uses_send_with_nosignal() {
        let bytes = [1u8, 2, 3, 4];
        let iovecs = [initialized_iovec(bytes.as_ptr(), bytes.len())];
        let mut msg = MaybeUninit::uninit();

        let entry = build_write_vectored_entry(7, &iovecs, 0, 1, &mut msg, 99);
        let sqe = sqe_prefix(&entry);

        assert_eq!(sqe.opcode, opcode::Send::CODE);
        assert_eq!(sqe.msg_flags, libc::MSG_NOSIGNAL as u32);
        assert_eq!(sqe.user_data, 99);
    }

    #[test]
    fn stream_write_vectored_single_entry_leaves_message_header_untouched() {
        let bytes = [1u8, 2, 3, 4];
        let iovecs = [initialized_iovec(bytes.as_ptr(), bytes.len())];
        let mut name = 0u8;
        let mut control = 0u8;
        let mut sentinel_iovec = libc::iovec {
            iov_base: bytes.as_ptr() as *mut _,
            iov_len: bytes.len(),
        };
        let mut msg = MaybeUninit::new(libc::msghdr {
            msg_name: std::ptr::addr_of_mut!(name).cast(),
            msg_namelen: 7,
            msg_iov: std::ptr::addr_of_mut!(sentinel_iovec),
            msg_iovlen: 9,
            msg_control: std::ptr::addr_of_mut!(control).cast(),
            msg_controllen: 11,
            msg_flags: 13,
        });

        let entry = build_write_vectored_entry(7, &iovecs, 0, 1, &mut msg, 99);
        let sqe = sqe_prefix(&entry);
        // SAFETY: this test initialized every field above; the single-iovec
        // branch must leave those initialized bytes untouched.
        let msg = unsafe { msg.assume_init_ref() };

        assert_eq!(sqe.opcode, opcode::Send::CODE);
        assert_eq!(msg.msg_name, std::ptr::addr_of_mut!(name).cast());
        assert_eq!(msg.msg_namelen, 7);
        assert_eq!(msg.msg_iov, std::ptr::addr_of_mut!(sentinel_iovec));
        assert_eq!(msg.msg_iovlen, 9);
        assert_eq!(msg.msg_control, std::ptr::addr_of_mut!(control).cast());
        assert_eq!(msg.msg_controllen, 11);
        assert_eq!(msg.msg_flags, 13);
    }

    #[cfg(all(target_arch = "x86_64", target_os = "linux"))]
    #[test]
    fn retained_writev_message_header_layout_is_exact() {
        assert_eq!(std::mem::size_of::<libc::msghdr>(), 56);
        assert_eq!(std::mem::align_of::<libc::msghdr>(), 8);
        assert_eq!(std::mem::size_of::<MaybeUninit<libc::msghdr>>(), 56);
        assert_eq!(std::mem::align_of::<MaybeUninit<libc::msghdr>>(), 8);

        type Direct = RetainedWritevPayload<()>;
        type Projected = RetainedProjectedWritevPayload<()>;
        assert_eq!(std::mem::size_of::<Direct>(), 360);
        assert_eq!(std::mem::align_of::<Direct>(), 8);
        assert_eq!(std::mem::offset_of!(Direct, msg), 288);
        assert_eq!(std::mem::offset_of!(Direct, written), 344);
        assert_eq!(std::mem::offset_of!(Direct, skip), 352);
        assert_eq!(std::mem::size_of::<Projected>(), 360);
        assert_eq!(std::mem::align_of::<Projected>(), 8);
        assert_eq!(std::mem::offset_of!(Projected, msg), 288);
        assert_eq!(std::mem::offset_of!(Projected, written), 344);
        assert_eq!(std::mem::offset_of!(Projected, skip), 352);
    }

    #[test]
    fn stream_write_vectored_multi_entry_uses_sendmsg_with_nosignal() {
        let first = [1u8, 2];
        let second = [3u8, 4, 5];
        let iovecs = [
            initialized_iovec(first.as_ptr(), first.len()),
            initialized_iovec(second.as_ptr(), second.len()),
        ];
        let mut msg = MaybeUninit::uninit();

        let entry = build_write_vectored_entry(7, &iovecs, 0, 2, &mut msg, 99);
        let sqe = sqe_prefix(&entry);
        // SAFETY: the multi-iovec branch initializes the complete header
        // before building an SQE that publishes its pointer.
        let msg = unsafe { msg.assume_init_ref() };

        assert_eq!(sqe.opcode, opcode::SendMsg::CODE);
        assert_eq!(sqe.msg_flags, libc::MSG_NOSIGNAL as u32);
        assert_eq!(sqe.addr, (msg as *const libc::msghdr) as u64);
        assert_eq!(sqe.user_data, 99);
        assert_eq!(msg.msg_iov, iovecs.as_ptr() as *mut libc::iovec);
        assert_eq!(msg.msg_iovlen, 2);
        assert!(msg.msg_name.is_null());
        assert_eq!(msg.msg_namelen, 0);
        assert!(msg.msg_control.is_null());
        assert_eq!(msg.msg_controllen, 0);
        assert_eq!(msg.msg_flags, 0);
    }
    #[cfg(any(debug_assertions, feature = "test-support"))]
    mod completion_retirement_tests {
        use super::*;
        use crate::runtime::op::completion_retirement_test_support::*;
        use crate::runtime::retained::RetainedPayload;
        use std::panic::{AssertUnwindSafe, catch_unwind};
        use std::task::Waker;

        #[test]
        fn completed_stream_extractor_panic_preserves_state_for_drop_recovery() {
            for staged in [false, true] {
                for rejected in [false, true] {
                    with_ringless_poll_context_for_test(2, |owner, cx| {
                        let runtime = RuntimeFd::from_fresh_raw_fd(-7);
                        let fd_state = if staged {
                            runtime.lease().into_op_state()
                        } else {
                            runtime.op_state()
                        };
                        let owner_count = Rc::strong_count(owner);
                        let mut future =
                            WriteFuture::<Vec<u8>, ()>::new(fd_state, b"extractor-owner".to_vec());
                        let buffer = future.buffer.take().expect("future lost its initial owner");
                        let buffer_ptr = buffer.as_ptr();
                        let capacity = buffer.capacity();
                        let saved = unsafe {
                            stage_payload(
                                owner,
                                &mut future.state_ptr,
                                RetainedWritePayload { buffer },
                                5,
                                true,
                            )
                        };
                        let before_state = unsafe { state_snapshot(saved.state) };
                        let before_owner = owner_snapshot(owner, cx);
                        assert_eq!(runtime.strong_count_for_test(), 2);
                        assert_idle_queues(owner, 1);
                        assert_payload_stats(owner, (1, 0, 0));
                        let mut noop = Context::from_waker(Waker::noop());
                        let selected = if rejected { &mut noop } else { &mut *cx };
                        let unwind = catch_unwind(AssertUnwindSafe(|| {
                            let _ = unsafe {
                                take_completed_result_and_payload_with::<
                                    RetainedWritePayload<Vec<u8>>,
                                    (),
                                >(
                                    selected,
                                    &mut future.state_ptr,
                                    |payload| {
                                        assert_eq!(payload, saved.payload);
                                        panic!("stream extractor retirement probe");
                                    },
                                )
                            };
                        }));
                        let panic = unwind.expect_err("extractor did not unwind");
                        assert_eq!(
                            panic.downcast_ref::<&'static str>(),
                            Some(&"stream extractor retirement probe")
                        );
                        drop(panic);
                        assert_eq!(future.state_ptr.state_ptr(), saved.state);
                        assert_eq!(future.state_ptr.raw_fd(), -7);
                        unsafe { assert_detached(&saved, before_state, rejected) };
                        assert_eq!(owner_snapshot(owner, cx), before_owner);
                        assert_eq!(runtime.strong_count_for_test(), 2);
                        assert_payload_stats(owner, (1, 0, 0));
                        {
                            // The exact callback above neither read nor moved a
                            // payload field. This detached T remains initialized.
                            let payload = unsafe { &*saved.payload };
                            assert_eq!(payload.buffer.as_ptr(), buffer_ptr);
                            assert_eq!(payload.buffer.capacity(), capacity);
                            assert_eq!(payload.buffer, b"extractor-owner");
                        }

                        drop(future);
                        assert_idle_queues(owner, 0);
                        assert_eq!(runtime.strong_count_for_test(), 1);
                        assert_eq!(Rc::strong_count(owner), owner_count);
                        assert_payload_stats(owner, (1, 0, 0));
                        unsafe { assert_slot_recovery(owner, saved.state) };

                        let reactor = owner.reactor_ptr();
                        let companion = unsafe {
                            (&mut *reactor).alloc_retained_payload(RetainedWritePayload {
                                buffer: b"companion".to_vec(),
                            })
                        };
                        assert_ne!(
                            companion.as_ptr(),
                            saved.payload,
                            "detached initialized payload was recycled early"
                        );
                        assert_payload_stats(owner, (2, 0, 0));
                        let pool = unsafe { Reactor::retained_payload_pool_ptr(reactor) };
                        unsafe { companion.drop_and_free(&mut *pool.as_ptr()) };
                        assert_payload_stats(owner, (2, 1, 0));
                        // Unlike whole-take storage unwind, this extractor
                        // touched no field. Destroy the original initialized T
                        // exactly once, then release its exact original backing.
                        unsafe {
                            RetainedPayload::<RetainedWritePayload<Vec<u8>>>::from_raw_parts(
                                saved.payload,
                                saved.original_vtable,
                            )
                            .drop_and_free(&mut *pool.as_ptr());
                        }
                        assert_payload_stats(owner, (2, 2, 0));
                        let reused = unsafe {
                            (&mut *reactor).alloc_retained_payload(RetainedWritePayload {
                                buffer: b"reused".to_vec(),
                            })
                        };
                        assert_eq!(reused.as_ptr(), saved.payload);
                        assert_eq!(unsafe { &reused.as_ref().buffer[..] }, b"reused");
                        let pool = unsafe { Reactor::retained_payload_pool_ptr(reactor) };
                        unsafe { reused.drop_and_free(&mut *pool.as_ptr()) };
                        assert_payload_stats(owner, (3, 3, 1));
                        assert_idle_queues(owner, 0);
                        assert_eq!(runtime.strong_count_for_test(), 1);
                        assert_eq!(Rc::strong_count(owner), owner_count);
                    });
                }
            }
        }
    }
}
