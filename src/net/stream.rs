//! Shared io_uring futures for byte-stream transports and FlowIO's TLS wrapper.
//!
//! These futures use `IORING_OP_READ` for reads and `IORING_OP_SEND` /
//! `IORING_OP_SENDMSG` with `MSG_NOSIGNAL` for writes, then handle partial
//! completion internally for the `_all` / `_exact` variants.  The stream type
//! parameter `S` is carried only in `PhantomData` to borrow the parent stream
//! for the duration of the operation.
//!
//! Vectored operations materialize `iovec` arrays into scratch storage.
//! Partial progress advances that scratch in place, and the retry path can
//! downgrade to `IORING_OP_READ` / `IORING_OP_SEND` when only one segment
//! remains.
//!
//! `CompletionState` is allocated from the reactor's pool for each active
//! operation. Simple futures free it when their one submission retires; retry
//! futures reset the same slot for sequential resubmissions after each CQE has
//! been consumed.
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

use crate::net::send_sqe::{build_send_entry, build_sendmsg_entry};
use crate::net::{CompletionTake, complete_read_with_progress, completion_cqe_result};
use crate::runtime::buffer::iobuffvec::{
    IoBuffReadOnlyVec, IoBuffVec, IoBuffVecMut, checked_iovec_count_and_length_sum,
    invalid_read_iovec_shape,
};
use crate::runtime::buffer::{IoBuffMut, IoBuffReadOnly, IoBuffReadWrite};
use crate::runtime::executor::{
    PollCtx, UnsubmittedOpGuard, completed_op_ctx, drop_fd_op_state_unchecked, poll_ctx_from_waker,
    refresh_op_waiter_from_waker, submit_initialized_retained_fd_sqe, submit_resubmitted_fd_sqe,
    submit_retained_fd_sqe, validate_local_io_result,
};
use crate::runtime::fd::RuntimeFdOpState;
use crate::runtime::op::CompletionState;
use crate::runtime::reactor::Reactor;
use crate::runtime::retained::{
    RETAINED_IOVEC_INLINE_COUNT, RETAINED_IOVEC_MAX_COUNT, RetainedIovecScratch,
    RetainedIovecScratchInit, RetainedPayload, RetainedPayloadPool, with_raw_retained_slot,
};
use crate::runtime::task::TaskHeader;
use io_uring::{opcode, squeue, types};
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
        /// Positive progress appends to an [`IoBuffMut`] payload; buffers that
        /// retain the provided zero write base publish from their beginning.
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

        /// Attempts one nonblocking read syscall into the current payload
        /// tail.
        ///
        /// On success, only the bytes actually read are appended to
        /// `buffer`. Existing payload bytes are preserved. If no data is
        /// immediately available, this returns
        /// [`io::ErrorKind::WouldBlock`] and leaves the payload length
        /// unchanged.
        ///
        /// This is a deadline-edge primitive, not a replacement for
        /// [`Self::read_exact_append`] in normal async protocol flow.
        /// `WouldBlock` does not register a wakeup; return to the async path
        /// unless the caller is deliberately handling an expired deadline.
        #[doc = concat!(
            "# Example\n",
            "```no_run\n",
            "use ", $stream_path, ";\n",
            "use flowio::runtime::buffer::IoBuffMut;\n\n",
            "# fn deadline_edge_append(mut stream: ", stringify!($stream),
            ", buffer: IoBuffMut) {\n",
            "let (result, buffer) = stream.try_read_append(buffer, 128);\n",
            "if result.is_err() {\n",
            "    let _retry_later = buffer;\n",
            "}\n",
            "# }\n",
            "```"
        )]
        pub fn try_read_append(
            &mut self,
            buffer: IoBuffMut,
            len: usize,
        ) -> (io::Result<usize>, IoBuffMut) {
            stream::try_read_append_once(self.fd.raw_fd(), buffer, len)
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
        /// performs one `sendmsg`, and returns the source immediately. Up to
        /// 16 pieces use inline stack scratch; larger projections normally use
        /// bounded reusable thread-local `Vec` scratch and may allocate when
        /// capacity must grow. Re-entry or thread-local teardown uses one
        /// bounded local vector instead; if its reservation fails, this returns
        /// [`io::ErrorKind::WouldBlock`] with the exact source. Message bytes
        /// are not copied, and no retained operation state is created.
        /// Projections above 1024 non-empty pieces are rejected with
        /// [`io::ErrorKind::InvalidInput`].
        /// A declared-empty projection is still invoked once for contract
        /// validation; a valid empty projection completes with `Ok(0)` and no
        /// syscall.
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
        /// Positive progress appends to an [`IoBuffMut`] payload. Buffers that
        /// retain the provided zero write base publish from their beginning.
        /// A zero-byte completion or an error before progress leaves the
        /// buffer's existing logical contents unchanged; the returned count is
        /// relative to this operation rather than the resulting total length.
        /// A request with `len == 0` completes locally with `Ok(0)` and no
        /// kernel submission; that request-scoped result does not report peer
        /// EOF.
        ///
        /// Preferred on the stream fast path when the caller tracks framing:
        /// this performs one contiguous submission and returns short reads
        /// directly.
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
        /// directly.
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
        /// appends to an [`IoBuffMut`] payload; buffers that retain the
        /// provided zero write base publish from their beginning. Any prefix
        /// read before EOF or another terminal error remains published, while
        /// an error before progress preserves the existing logical contents.
        /// The result count remains relative to this operation.
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

        /// Appends exactly `len` bytes to the current payload end of
        /// `buffer`.
        ///
        /// Returns `UnexpectedEof` if the peer closes before `len` bytes
        /// arrive. On success the returned buffer payload length is the
        /// original payload length plus `len`; on EOF or error it includes
        /// any bytes appended before completion.
        ///
        /// This preserves [`Self::read_exact`] semantics while supporting
        /// staged protocol reads into one [`IoBuffMut`].
        ///
        /// This complete-buffer API may resubmit after partial reads. Avoid
        /// that retry bookkeeping when the caller already manages staged
        /// framing; use [`Self::read`] in that case.
        pub fn read_exact_append(
            &mut self,
            buffer: IoBuffMut,
            len: usize,
        ) -> stream::ReadExactAppendFuture<'_, Self> {
            stream::ReadExactAppendFuture::new(self.fd.op_state(), buffer, len)
        }

        /// Scatter-read into a vectored buffer chain.
        ///
        /// The chain is consumed and returned alongside the result (rental
        /// pattern). The total number of bytes read is returned in `Ok`.
        ///
        /// Use this when the receive path is already naturally segmented.
        /// For a single contiguous destination buffer, prefer
        /// [`Self::read`] to avoid iovec materialization.
        ///
        /// # Errors
        ///
        /// Returns [`io::ErrorKind::InvalidInput`] if the chain has no
        /// writable segments, iovec materialization overflows, or its writable
        /// segment count or byte total changes before submission.
        /// Materialization and shape failures return the exact chain without
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
        /// Use this when the send path is already naturally segmented. For
        /// one contiguous payload, prefer [`Self::write`] to avoid iovec
        /// materialization.
        ///
        /// # Errors
        ///
        /// Returns [`io::ErrorKind::InvalidInput`] when the aggregate readable
        /// byte count cannot be represented by `usize`.
        pub fn writev<C: WriteBufferChain<N>, const N: usize>(
            &mut self,
            buffer: C,
        ) -> stream::WritevFuture<'_, C, N, Self> {
            stream::WritevFuture::new(self.fd.op_state(), buffer)
        }

        /// Gather-write projected pieces from one compact owned source.
        ///
        /// FlowIO retains `source`, then projects borrowed byte slices from
        /// that retained source into retained kernel-facing `iovec`
        /// scratch. Projection copies only pointer/length metadata, not message
        /// bytes. After runtime-context validation succeeds, declared-empty
        /// projections are still invoked once for contract validation; valid
        /// empty projections complete with `Ok(0)` without submitting kernel
        /// I/O.
        ///
        /// Use this when the send path is already naturally segmented
        /// inside the retained carrier. For one contiguous payload, prefer
        /// [`Self::write`] to avoid projection and iovec materialization.
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
        /// Returns [`io::ErrorKind::InvalidInput`] if `len` exceeds the
        /// chain's writable capacity or the `io_uring` 32-bit byte-count
        /// limit, iovec materialization overflows, or the chain's writable
        /// segment count or byte total changes before submission.
        /// Materialization and shape failures return the exact chain without
        /// submitting kernel I/O. A zero `len` remains valid even when the
        /// chain has no writable segments.
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
fn debug_assert_stream_fd_state(fd: RawFd, fd_state: &RuntimeFdOpState<'_>) {
    let state_fd = fd_state.raw_fd();
    debug_assert!(
        state_fd < 0 || fd == state_fd,
        "stream future raw descriptor and typed operation state diverged"
    );
}

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
    let op_ctx = unsafe { completed_op_ctx(poll_ctx_from_waker(cx).ok(), state_ptr) };
    let payload = unsafe { op_ctx.take_retained_payload_unchecked::<T>(state_ptr) };
    let retired = fd_state.take_state_ptr();
    debug_assert_eq!(retired, state_ptr);
    unsafe { op_ctx.free_op_unchecked(state_ptr) };
    Some(CompletionTake::from_context(
        result,
        payload,
        op_ctx.context_rejected(),
    ))
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
    let op_ctx = unsafe { completed_op_ctx(poll_ctx_from_waker(cx).ok(), state_ptr) };
    let value = unsafe { op_ctx.take_retained_payload_with_unchecked::<T, R>(state_ptr, extract) };
    let retired = fd_state.take_state_ptr();
    debug_assert_eq!(retired, state_ptr);
    unsafe { op_ctx.free_op_unchecked(state_ptr) };
    Some(CompletionTake::from_context(
        result,
        value,
        op_ctx.context_rejected(),
    ))
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

use super::{WritevPieces, WritevProjection, invalid_input, invalid_input_kind, opt_ref, opt_take};

/// Returns an uninitialized inline `iovec` scratch array.
///
/// `assume_init` is sound here because an array of `MaybeUninit` is itself
/// initialized; each element remains uninitialized until written.
#[inline(always)]
fn uninit_iovecs<const N: usize>() -> [MaybeUninit<libc::iovec>; N] {
    unsafe { MaybeUninit::uninit().assume_init() }
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
}

struct RetainedWritevPayload<C> {
    /// Caller-owned source chain retained across every active submission.
    buffer: C,
    /// Kernel-facing `iovec` array pointing into `buffer` segments.
    scratch: RetainedIovecScratch,
    /// Kernel-facing sendmsg header pointing at the active scratch window.
    msg: libc::msghdr,
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
    /// Kernel-facing sendmsg header pointing at the active scratch window.
    msg: libc::msghdr,
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
    let msg = empty_sendmsg_header();
    unsafe {
        with_raw_retained_slot::<RetainedWritevPayload<C>, _>(pool, |mut slot| {
            let dst = slot.as_mut_ptr();
            std::ptr::addr_of_mut!((*dst).msg).write(msg);
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
/// Projection remains post-attachment so its fallible callback observes the
/// same stable source and scratch addresses as before.
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
    let msg = empty_sendmsg_header();
    unsafe {
        with_raw_retained_slot::<RetainedProjectedWritevPayload<T>, _>(pool, |mut slot| {
            let dst = slot.as_mut_ptr();
            std::ptr::addr_of_mut!((*dst).msg).write(msg);
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
fn empty_sendmsg_header() -> libc::msghdr {
    libc::msghdr {
        msg_name: std::ptr::null_mut(),
        msg_namelen: 0,
        msg_iov: std::ptr::null_mut(),
        msg_iovlen: 0,
        msg_control: std::ptr::null_mut(),
        msg_controllen: 0,
        msg_flags: 0,
    }
}

#[inline(always)]
fn build_write_entry(fd: RawFd, ptr: *const u8, len: u32, user_data: u64) -> squeue::Entry {
    build_send_entry(fd, ptr, len, user_data)
}

#[inline(always)]
fn prepare_sendmsg_header(
    msg: &mut libc::msghdr,
    iovecs: &[MaybeUninit<libc::iovec>],
    skip: usize,
    count: usize,
) -> *const libc::msghdr {
    *msg = libc::msghdr {
        msg_name: std::ptr::null_mut(),
        msg_namelen: 0,
        msg_iov: unsafe { iovec_slice_ptr(iovecs, skip) as *mut libc::iovec },
        msg_iovlen: count,
        msg_control: std::ptr::null_mut(),
        msg_controllen: 0,
        msg_flags: 0,
    };
    msg as *const libc::msghdr
}

#[inline(always)]
/// Builds the next write-style SQE from the current vectored scratch window.
///
/// When only one segment remains and its length fits in `u32`, this downgrades
/// to `IORING_OP_SEND` to avoid an unnecessary `sendmsg`.
fn build_write_vectored_entry(
    fd: RawFd,
    iovecs: &[MaybeUninit<libc::iovec>],
    skip: usize,
    count: usize,
    msg: &mut libc::msghdr,
    user_data: u64,
) -> squeue::Entry {
    debug_assert!(count > 0, "writev submission requires at least one iovec");

    if count == 1 {
        let iov = unsafe { iovec_slice_ref(iovecs, skip) };
        if let Ok(len) = u32::try_from(iov.iov_len) {
            return build_write_entry(fd, iov.iov_base as *const u8, len, user_data);
        }
    }

    let msg = prepare_sendmsg_header(msg, iovecs, skip, count);
    build_sendmsg_entry(fd, msg, user_data)
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

/// Attempts one nonblocking read syscall into the current payload tail.
#[inline]
pub(crate) fn try_read_append_once(
    fd: RawFd,
    buffer: IoBuffMut,
    len: usize,
) -> (io::Result<usize>, IoBuffMut) {
    try_read_once(fd, buffer, len)
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

    let result = unsafe { libc::sendmsg(fd, &msg, libc::MSG_NOSIGNAL) };
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

    let state_ptr = unsafe { (*pctx.reactor()).alloc_op() };
    if state_ptr.is_null() {
        let source = unsafe { opt_take(source) };
        return Err((io::Error::from(io::ErrorKind::WouldBlock), source));
    }

    let guard = unsafe { UnsubmittedOpGuard::new(pctx.reactor(), state_ptr) };
    unsafe { (*state_ptr).register_waiter(pctx.owner_task()) };

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

/// Single read into a caller-provided buffer (rental pattern).
///
/// A zero-length request completes locally with `Ok(0)` and returns the
/// buffer unchanged. That request-scoped result is not an EOF observation.
pub struct ReadFuture<'a, B: IoBuffReadWrite, S> {
    /// Completion state for the submitted read SQE, if any.
    state_ptr: RuntimeFdOpState<'a>,
    /// Caller-owned destination buffer returned on completion.
    buffer: Option<B>,
    /// Stream descriptor read from by this future.
    fd: RawFd,
    /// Maximum bytes requested from the kernel.
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
        let fd = fd_state.raw_fd();
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
        debug_assert_stream_fd_state(this.fd, &this.state_ptr);

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
            let state_ptr = unsafe { (*pctx.reactor()).alloc_op() };
            if state_ptr.is_null() {
                let buffer = unsafe { opt_take(&mut this.buffer) };
                return Poll::Ready((Err(io::Error::from(io::ErrorKind::WouldBlock)), buffer));
            }
            let guard = unsafe { UnsubmittedOpGuard::new(pctx.reactor(), state_ptr) };
            unsafe { (*state_ptr).register_waiter(pctx.owner_task()) };

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
                        Ok(opcode::Read::new(types::Fd(fd), ptr, this.len)
                            .build()
                            .user_data(state_ptr as u64))
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
    /// Stream descriptor written by this future.
    fd: RawFd,
    /// Validated byte count submitted to the kernel.
    len: u32,
    /// Deferred validation error returned before any SQE submission.
    input_error: Option<io::Error>,
    /// Borrows the parent stream for the future lifetime.
    _marker: PhantomData<&'a mut S>,
}

impl<'a, B: IoBuffReadOnly, S> WriteFuture<'a, B, S> {
    pub(crate) fn new(fd_state: RuntimeFdOpState<'a>, buffer: B) -> Self {
        let fd = fd_state.raw_fd();
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
        debug_assert_stream_fd_state(this.fd, &this.state_ptr);

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
            let state_ptr = unsafe { (*pctx.reactor()).alloc_op() };
            if state_ptr.is_null() {
                let buffer = unsafe { opt_take(&mut this.buffer) };
                return Poll::Ready((Err(io::Error::from(io::ErrorKind::WouldBlock)), buffer));
            }
            let guard = unsafe { UnsubmittedOpGuard::new(pctx.reactor(), state_ptr) };
            unsafe { (*state_ptr).register_waiter(pctx.owner_task()) };

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
                        Ok(build_write_entry(fd, ptr, this.len, state_ptr as u64))
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
/// The base buffer pointer is captured during the initial retained submission
/// and reused for retries, avoiding repeated `as_ptr()` trait calls. Context is
/// validated once for each completion/resubmission pass.
pub struct WriteAllFuture<'a, B: IoBuffReadOnly, S> {
    /// Completion state reused across sequential retry submissions.
    state_ptr: RuntimeFdOpState<'a>,
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
    /// Deferred validation error returned before any SQE submission.
    input_error: Option<io::Error>,
    /// Borrows the parent stream for the future lifetime.
    _marker: PhantomData<&'a mut S>,
}

impl<'a, B: IoBuffReadOnly, S> WriteAllFuture<'a, B, S> {
    pub(crate) fn new(fd_state: RuntimeFdOpState<'a>, buffer: B) -> Self {
        let fd = fd_state.raw_fd();
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
        debug_assert_stream_fd_state(this.fd, &this.state_ptr);

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
            let state_ptr = unsafe { (*pctx.reactor()).alloc_op() };
            if state_ptr.is_null() {
                let buffer = unsafe { opt_take(&mut this.buffer) };
                return Poll::Ready((Err(io::Error::from(io::ErrorKind::WouldBlock)), buffer));
            }
            let guard = unsafe { UnsubmittedOpGuard::new(pctx.reactor(), state_ptr) };
            unsafe { (*state_ptr).register_waiter(pctx.owner_task()) };
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
                        Ok(build_write_entry(fd, ptr, remaining, state_ptr as u64))
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
                Ok(build_write_entry(fd, ptr, remaining, state_ptr as u64))
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

/// Reads exactly `target` bytes, re-submitting on partial reads.
///
/// Returns `UnexpectedEof` if the peer closes before the target is reached.
/// On error the buffer reflects the bytes received so far.  Like
/// [`WriteAllFuture`], the base pointer is captured during the initial
/// retained submission and one context extraction covers state handling and
/// submission per poll.
pub struct ReadExactFuture<'a, B: IoBuffReadWrite, S> {
    /// Completion state reused across sequential retry submissions.
    state_ptr: RuntimeFdOpState<'a>,
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
    /// Logical length immediately before the submitted writable region.
    write_base_len: usize,
    /// Deferred validation error returned before any SQE submission.
    input_error: Option<io::Error>,
    /// Borrows the parent stream for the future lifetime.
    _marker: PhantomData<&'a mut S>,
}

impl<'a, B: IoBuffReadWrite, S> ReadExactFuture<'a, B, S> {
    pub(crate) fn new(fd_state: RuntimeFdOpState<'a>, buffer: B, len: usize) -> Self {
        let fd = fd_state.raw_fd();
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
        debug_assert_stream_fd_state(this.fd, &this.state_ptr);

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
            let state_ptr = unsafe { (*pctx.reactor()).alloc_op() };
            if state_ptr.is_null() {
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
            let guard = unsafe { UnsubmittedOpGuard::new(pctx.reactor(), state_ptr) };
            unsafe { (*state_ptr).register_waiter(pctx.owner_task()) };
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
                        Ok(opcode::Read::new(types::Fd(fd), ptr, remaining)
                            .build()
                            .user_data(state_ptr as u64))
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
                Ok(opcode::Read::new(types::Fd(fd), ptr, remaining)
                    .build()
                    .user_data(state_ptr as u64))
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
// ReadExactAppendFuture
// ---------------------------------------------------------------------------

/// Reads exactly `target` bytes into the current writable tail of an
/// [`IoBuffMut`], preserving any existing payload bytes.
pub struct ReadExactAppendFuture<'a, S> {
    /// Shared exact-read implementation using `IoBuffMut`'s append-aware
    /// writable base, capacity, and publication hooks.
    inner: ReadExactFuture<'a, IoBuffMut, S>,
}

impl<'a, S> ReadExactAppendFuture<'a, S> {
    pub(crate) fn new(fd: RuntimeFdOpState<'a>, buffer: IoBuffMut, len: usize) -> Self {
        Self {
            inner: ReadExactFuture::new(fd, buffer, len),
        }
    }
}

impl<S> Future for ReadExactAppendFuture<'_, S> {
    type Output = (io::Result<usize>, IoBuffMut);

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.get_mut().inner).poll(cx)
    }
}

// ---------------------------------------------------------------------------
// ReadvFuture
// ---------------------------------------------------------------------------

/// Scatter-read into a vectored buffer chain (rental pattern).
pub struct ReadvFuture<'a, const N: usize, S> {
    /// Completion state for the submitted readv/read SQE, if any.
    state_ptr: RuntimeFdOpState<'a>,
    /// Caller-owned mutable segment chain returned on completion.
    buffer: Option<IoBuffVecMut<N>>,
    /// Number of non-empty segment entries materialized into retained scratch.
    iov_count: usize,
    /// Total writable capacity across all segments, cached so zero-capacity
    /// reads complete before submission and debug checks do not re-walk the
    /// caller-owned chain.
    writable: usize,
    /// Stream descriptor read from by this future.
    fd: RawFd,
    /// Whether the sizing pass found an unrepresentable writable aggregate.
    invalid_aggregate: bool,
    /// Borrows the parent stream for the future lifetime.
    _marker: PhantomData<&'a mut S>,
}

impl<'a, const N: usize, S> ReadvFuture<'a, N, S> {
    pub(crate) fn new(fd_state: RuntimeFdOpState<'a>, buffer: IoBuffVecMut<N>) -> Self {
        let fd = fd_state.raw_fd();
        let (iov_count, writable, invalid_aggregate) =
            match buffer.checked_read_iovec_count_and_writable_len() {
                Some((iov_count, writable)) => (iov_count, writable, false),
                None => (0, 0, true),
            };
        Self {
            state_ptr: fd_state,
            buffer: Some(buffer),
            iov_count,
            writable,
            fd,
            invalid_aggregate,
            _marker: PhantomData,
        }
    }
}

impl<const N: usize, S> Future for ReadvFuture<'_, N, S> {
    type Output = (io::Result<usize>, IoBuffVecMut<N>);

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = unsafe { self.get_unchecked_mut() };
        debug_assert_stream_fd_state(this.fd, &this.state_ptr);

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

        if this.state_ptr.state_ptr().is_null() && this.invalid_aggregate {
            let result = validate_local_io_result(cx, Err(invalid_readv_aggregate()));
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
            let state_ptr = unsafe { (*pctx.reactor()).alloc_op() };
            if state_ptr.is_null() {
                let buffer = unsafe { opt_take(&mut this.buffer) };
                return Poll::Ready((Err(io::Error::from(io::ErrorKind::WouldBlock)), buffer));
            }
            let guard = unsafe { UnsubmittedOpGuard::new(pctx.reactor(), state_ptr) };
            unsafe { (*state_ptr).register_waiter(pctx.owner_task()) };

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
    /// Stream descriptor written by this future.
    fd: RawFd,
    /// Borrows the parent stream for the future lifetime.
    _marker: PhantomData<&'a mut S>,
}

impl<'a, C: WriteBufferChain<N>, const N: usize, S> WritevFuture<'a, C, N, S> {
    pub(crate) fn new(fd_state: RuntimeFdOpState<'a>, buffer: C) -> Self {
        let fd = fd_state.raw_fd();
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
        debug_assert_stream_fd_state(this.fd, &this.state_ptr);

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
                        let state_ptr = (*pctx.reactor()).alloc_op();
                        if state_ptr.is_null() {
                            return Err(io::Error::from(io::ErrorKind::WouldBlock));
                        }
                        let guard = UnsubmittedOpGuard::new(pctx.reactor(), state_ptr);
                        (*state_ptr).register_waiter(pctx.owner_task());
                        Ok(guard)
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
    /// Stream descriptor written by this future.
    fd: RawFd,
    /// Total bytes that must be written before completion.
    total: usize,
    /// Borrows the parent stream for the future lifetime.
    _marker: PhantomData<&'a mut S>,
}

impl<'a, C: WriteBufferChain<N>, const N: usize, S> WritevAllFuture<'a, C, N, S> {
    pub(crate) fn new(fd_state: RuntimeFdOpState<'a>, buffer: C) -> Self {
        let fd = fd_state.raw_fd();
        Self {
            state_ptr: fd_state,
            buffer: Some(buffer),
            iov_count: 0,
            fd,
            total: 0,
            _marker: PhantomData,
        }
    }
}

impl<C: WriteBufferChain<N>, const N: usize, S> Future for WritevAllFuture<'_, C, N, S> {
    type Output = (io::Result<usize>, C);

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = unsafe { self.get_unchecked_mut() };
        debug_assert_stream_fd_state(this.fd, &this.state_ptr);

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
        }

        if !this.state_ptr.state_ptr().is_null() {
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
                }
            }
        }
        if this.buffer.is_some() {
            debug_assert!(
                this.state_ptr.state_ptr().is_null(),
                "initial writev_all state was published"
            );
            let state_ptr = unsafe { (*pctx.reactor()).alloc_op() };
            if state_ptr.is_null() {
                let buffer = unsafe { opt_take(&mut this.buffer) };
                return Poll::Ready((Err(io::Error::from(io::ErrorKind::WouldBlock)), buffer));
            }
            let guard = unsafe { UnsubmittedOpGuard::new(pctx.reactor(), state_ptr) };
            unsafe { (*state_ptr).register_waiter(pctx.owner_task()) };

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
                let remaining_iovs = remaining_iovec_count(&payload.scratch, payload.skip);
                Ok(build_write_vectored_entry(
                    fd,
                    retained_iovecs(&payload.scratch),
                    payload.skip,
                    remaining_iovs,
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

/// Gather-write from one compact retained source projected into write pieces.
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
    /// Stream descriptor written by this future.
    fd: RawFd,
    /// Borrows the parent stream for the future lifetime.
    _marker: PhantomData<&'a mut S>,
}

impl<'a, T: WritevProjection, S> WritevProjectedFuture<'a, T, S> {
    pub(crate) fn new(fd_state: RuntimeFdOpState<'a>, source: T) -> Self {
        let fd = fd_state.raw_fd();
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
        debug_assert_stream_fd_state(this.fd, &this.state_ptr);

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

/// Gather-write all projected pieces from one compact retained source.
pub struct WritevAllProjectedFuture<'a, T: WritevProjection, S> {
    /// Completion state reused across projected retry submissions.
    state_ptr: RuntimeFdOpState<'a>,
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
    pub(crate) fn new(fd_state: RuntimeFdOpState<'a>, source: T) -> Self {
        let fd = fd_state.raw_fd();
        let (iov_count, total) = source.writev_count_and_len();
        Self {
            state_ptr: fd_state,
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
        debug_assert_stream_fd_state(this.fd, &this.state_ptr);

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

        if !this.state_ptr.state_ptr().is_null() {
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
                let remaining_iovs = remaining_iovec_count(&payload.scratch, payload.skip);
                Ok(build_write_vectored_entry(
                    fd,
                    retained_iovecs(&payload.scratch),
                    payload.skip,
                    remaining_iovs,
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

/// Scatter-read exactly `target` bytes into a vectored buffer chain,
/// re-submitting on partial reads with retained `iovec` scratch.
/// Returns `UnexpectedEof` if the peer closes before the target is reached.
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
    /// Stream descriptor read from by this future.
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
    /// Borrows the parent stream for the future lifetime.
    _marker: PhantomData<&'a mut S>,
}

impl<'a, const N: usize, S> ReadvExactFuture<'a, N, S> {
    pub(crate) fn new(
        fd_state: RuntimeFdOpState<'a>,
        buffer: IoBuffVecMut<N>,
        target: usize,
    ) -> Self {
        let fd = fd_state.raw_fd();
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
            _marker: PhantomData,
        }
    }
}

impl<const N: usize, S> Future for ReadvExactFuture<'_, N, S> {
    type Output = (io::Result<usize>, IoBuffVecMut<N>);

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = unsafe { self.get_unchecked_mut() };
        debug_assert_stream_fd_state(this.fd, &this.state_ptr);

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
            let state_ptr = unsafe { (*pctx.reactor()).alloc_op() };
            if state_ptr.is_null() {
                let mut buffer = unsafe { opt_take(&mut this.buffer) };
                unsafe { buffer.distribute_written(this.filled) };
                return Poll::Ready((Err(io::Error::from(io::ErrorKind::WouldBlock)), buffer));
            }
            let guard = unsafe { UnsubmittedOpGuard::new(pctx.reactor(), state_ptr) };
            unsafe { (*state_ptr).register_waiter(pctx.owner_task()) };

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
                let payload = (*state_ptr).retained_payload::<RetainedReadvPayload<N>>();
                let remaining_iovs = this.window_iov_count - this.skip;
                Ok(build_read_vectored_entry(
                    fd,
                    retained_iovecs(&payload.scratch),
                    this.skip,
                    remaining_iovs,
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
    use crate::runtime::executor::{CompletionDrainGuard, with_ringless_poll_context_for_test};
    #[cfg(not(miri))]
    use crate::runtime::executor::{Executor, ExecutorConfig};
    use crate::runtime::fd::{RuntimeFd, with_final_core_drop_hook_for_test};
    #[cfg(not(miri))]
    use crate::runtime::reactor::ReactorConfig;
    use crate::runtime::task::release_task;
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

    fn assert_message_free_invalid_input(result: io::Result<()>) {
        let err = result.expect_err("shape should be rejected");
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
        assert!(
            err.get_ref().is_none(),
            "try-path shape errors must remain message-free"
        );
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
        assert!(!readv.invalid_aggregate);
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
        readv.invalid_aggregate = true;
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
            readv.invalid_aggregate = true;
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
    fn read_exact_append_future_remains_a_thin_nominal_wrapper() {
        assert_eq!(
            std::mem::size_of::<ReadExactAppendFuture<'static, ()>>(),
            std::mem::size_of::<ReadExactFuture<'static, IoBuffMut, ()>>()
        );
        assert_eq!(
            std::mem::align_of::<ReadExactAppendFuture<'static, ()>>(),
            std::mem::align_of::<ReadExactFuture<'static, IoBuffMut, ()>>()
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
    fn stream_write_entry_uses_send_with_nosignal() {
        let bytes = [1u8, 2, 3, 4];
        let entry = build_write_entry(7, bytes.as_ptr(), bytes.len() as u32, 99);
        let sqe = sqe_prefix(&entry);

        assert_eq!(sqe.opcode, opcode::Send::CODE);
        assert_eq!(sqe.msg_flags, libc::MSG_NOSIGNAL as u32);
        assert_eq!(sqe.user_data, 99);
    }

    #[test]
    fn stream_write_vectored_single_entry_uses_send_with_nosignal() {
        let bytes = [1u8, 2, 3, 4];
        let iovecs = [initialized_iovec(bytes.as_ptr(), bytes.len())];
        let mut msg = empty_sendmsg_header();

        let entry = build_write_vectored_entry(7, &iovecs, 0, 1, &mut msg, 99);
        let sqe = sqe_prefix(&entry);

        assert_eq!(sqe.opcode, opcode::Send::CODE);
        assert_eq!(sqe.msg_flags, libc::MSG_NOSIGNAL as u32);
        assert_eq!(sqe.user_data, 99);
    }

    #[test]
    fn stream_write_vectored_multi_entry_uses_sendmsg_with_nosignal() {
        let first = [1u8, 2];
        let second = [3u8, 4, 5];
        let iovecs = [
            initialized_iovec(first.as_ptr(), first.len()),
            initialized_iovec(second.as_ptr(), second.len()),
        ];
        let mut msg = empty_sendmsg_header();

        let entry = build_write_vectored_entry(7, &iovecs, 0, 2, &mut msg, 99);
        let sqe = sqe_prefix(&entry);

        assert_eq!(sqe.opcode, opcode::SendMsg::CODE);
        assert_eq!(sqe.msg_flags, libc::MSG_NOSIGNAL as u32);
        assert_eq!(sqe.addr, (&msg as *const libc::msghdr) as u64);
        assert_eq!(sqe.user_data, 99);
        assert_eq!(msg.msg_iov, iovecs.as_ptr() as *mut libc::iovec);
        assert_eq!(msg.msg_iovlen, 2);
        assert!(msg.msg_name.is_null());
        assert!(msg.msg_control.is_null());
    }
}
