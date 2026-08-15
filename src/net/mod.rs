//! Transport implementations built on top of the runtime core.
//!
//! Each transport exposes generic buffer I/O through the [`IoBuffReadOnly`] /
//! [`IoBuffReadWrite`] traits. Types such as `Vec<u8>` and `Box<[u8]>` can be
//! used directly because they implement the relevant stable-pointer trait.
//!
//! Stream transports also expose concrete vectored APIs built on
//! [`IoBuffVec`](crate::runtime::buffer::iobuffvec::IoBuffVec) and
//! [`IoBuffVecMut`](crate::runtime::buffer::iobuffvec::IoBuffVecMut):
//! - TCP / Unix: `readv`, `writev`, `writev_all`, `readv_exact`
//! - SCTP: `recv_msg_vectored`, `send_msg_vectored`
//!
//! TCP and Unix streams also support [`WritevProjection`], which lets a caller
//! pass one compact owned carrier and project borrowed byte pieces from that
//! retained carrier into FlowIO-owned kernel-facing `iovec` scratch.
//!
//! Client-side TLS is provided separately by [`tls`], which wraps an existing
//! connected [`tcp::TcpStream`] with an explicit rustls-driven handshake and
//! encrypted I/O API.
//!
//! Hostname resolution is provided by [`resolver`], which offers a small
//! FlowIO-native DNS helper for turning host names into `SocketAddr` values
//! before connecting transports such as TCP or SCTP.
//!
//! UDP remains single-datagram and therefore uses single-buffer sends and
//! receives only.
//!
//! # Nameable TCP and Unix operation futures
//!
//! TCP and Unix stream methods return the concrete futures re-exported from
//! this module, including [`ReadFuture`], [`ReadExactFuture`],
//! [`WriteFuture`], and their vectored and projected variants. Directly
//! awaiting these methods remains allocation-free without naming the return
//! type. The public names additionally let downstream protocol libraries use
//! the operations in concrete associated types or inline state machines
//! without erasing them behind `Box<dyn Future>`.
//!
//! Each operation mutably borrows its parent [`tcp::TcpStream`] or
//! [`unix::UnixStream`] and owns the submitted rental buffer, chain, or
//! projection source until completion. It must be polled on the executor owner
//! thread and, after submission, in its originating executor. Dropping an
//! in-flight operation is nonblocking: FlowIO retains any kernel-visible
//! payload until the target completion retires. Racing read bytes can be
//! discarded after cancellation, so framed protocols should treat read
//! cancellation as terminal unless they provide stronger recovery semantics.
//!
//! The operation fields and constructors remain private, their layouts are not
//! stable, and exposing their names does not make them cross-thread values. The
//! private implementation module is intentionally inaccessible:
//!
//! ```compile_fail
//! use flowio::net::stream::ReadFuture;
//! ```
//!
//! # Error Semantics
//!
//! Async transport operations return `io::Error` through their futures. For
//! rental I/O methods, the caller-owned buffer or chain is returned alongside
//! the result, including on recoverable errors.
//!
//! [`io::ErrorKind::WouldBlock`] can report internal FlowIO pressure rather
//! than socket readiness: completion-state capacity exhaustion capped by the
//! executor's `ReactorConfig::ring_entries`, io_uring submission-queue
//! pressure, retained `iovec` scratch allocation pressure, or a reusable
//! listener/connector slot that is still occupied by an active or intentionally
//! forgotten future. The first three cases may become available after the
//! executor makes progress; a busy reusable slot becomes available only when
//! the previous future completes or is dropped, or when the owning
//! listener/connector is dropped. An accept future that reports this busy-slot
//! error never owns the slot: later polls park, and dropping it does not cancel
//! or replace the earlier owner's readiness waiter. TCP/SCTP accept latches
//! [`io::ErrorKind::ConnectionAborted`] when `POLLHUP` or `POLLNVAL` remains
//! after owner-thread `accept4` finds no queued connection. That listener is no
//! longer retryable; later accepts fail without another readiness submission.
//! A bare `POLLERR` receives one internal rearm per accept future, then exact
//! `EAGAIN` propagates without latching. A positive `POLLNVAL` confirmed by
//! `accept4` as `EBADF` preserves that raw errno for the current future while
//! latching the same later fail-fast state. The listener terminal-state
//! accessors expose only this sticky FlowIO latch; `false` is not a general
//! socket-health result. `EMFILE` and `ENFILE` propagate with their exact errno
//! without latching or rearming. The slot preserves the observed
//! readiness, so the next accept polled in the owner context makes one direct
//! nonblocking `accept4` attempt without another readiness submission. FlowIO
//! performs no hidden retry, timer, or backoff; callers should relieve
//! descriptor pressure and apply bounded backoff before retrying. If that
//! direct attempt returns `WouldBlock`, the retained mask is classified by the
//! same rules above: HUP/NVAL latches, bare `POLLERR` uses its bounded budget,
//! and plain stale readiness takes the ordinary one-shot rearm. Other
//! `accept4` errors propagate unchanged.
//!
//! [`io::ErrorKind::NotConnected`] also reports an invalid runtime poll context:
//! transport futures must be polled inside the FlowIO executor that submitted
//! them. An unsubmitted rental operation returns the buffer immediately. Once
//! submitted, it keeps the buffer retained until the original CQE and then
//! returns the buffer with `NotConnected`. Bytes reported by a completion first
//! observed from a rejected context are not published into the returned
//! buffer; only progress published by earlier valid exact-read iterations
//! remains visible. The exceptional bounded shutdown fallback cannot return
//! ownership if it abandons a ring without observing that target CQE; the
//! operation remains pending and its kernel-visible state and buffer are
//! intentionally retained until process exit.
//!
//! # Fast-Path Guidance
//!
//! Preferred on the per-message fast path:
//! - On stream transports, `read` / `write` and `readv` / `writev` perform one
//!   submission and expose partial progress to the caller.
//! - For fixed-peer UDP, prefer [`udp::UdpSocket::connect`] plus `send` /
//!   `recv` because that avoids per-datagram destination handling.
//! - Use vectored APIs only when payloads are already segmented. For one
//!   contiguous payload, the contiguous APIs are the simpler fast-path
//!   alternative.
//!
//! - Use projected writes when a compact owned carrier already contains
//!   segmented fields; use ordinary owned chains when the segments are
//!   independent buffer values.
//!
//! Avoid on the per-message fast path:
//! - Avoid `_exact` / `_all` variants unless complete-buffer
//!   semantics are required. Use partial-I/O APIs instead when the caller can
//!   track progress explicitly.
//! - Avoid `send_to` / `recv_from` when the peer is stable. Use
//!   connected UDP `send` / `recv` instead.
//! - Avoid resolving names in the steady-state data path. [`resolver`]
//!   is a setup/control-plane helper; resolve once and reuse the resulting
//!   `SocketAddr` values.
//!
//! On the connection path, reuse [`tcp::TcpConnector`] or
//! [`sctp::SctpConnector`] across repeated attempts. Reuse preserves the
//! connector-owned slot wrapper, but each attempt still creates and configures
//! a fresh socket; connection establishment is not the message data path.
//!
//! The examples below often use `_all` / `_exact` variants because they make
//! protocol framing obvious in docs. On the hot path, prefer partial-I/O APIs
//! when the caller can manage progress explicitly.
//!
//! [`IoBuffReadOnly`]: crate::runtime::buffer::IoBuffReadOnly
//! [`IoBuffReadWrite`]: crate::runtime::buffer::IoBuffReadWrite
//!
//! # Example
//! ```no_run
//! use flowio::net::unix::UnixStream;
//! use flowio::runtime::executor::Executor;
//!
//! let mut executor = Executor::new()?;
//! executor.run(async {
//!     let (mut left, mut right) = UnixStream::pair().unwrap();
//!
//!     let (res, _buf) = left.write_all(b"hello".to_vec()).await;
//!     res.unwrap();
//!
//!     let (res, buf) = right.read_exact(vec![0u8; 5], 5).await;
//!     res.unwrap();
//!     assert_eq!(&buf[..], b"hello");
//! })?;
//! # Ok::<(), std::io::Error>(())
//! ```
//!
//! The same transport methods also work with [`crate::runtime::buffer::IoBuffMut`]:
//! ```no_run
//! use flowio::net::unix::UnixStream;
//! use flowio::runtime::buffer::pool::{IoBuffPool, IoBuffPoolConfig};
//! use flowio::runtime::executor::Executor;
//!
//! let mut pool = IoBuffPool::new(IoBuffPoolConfig {
//!     headroom: 0,
//!     payload: 64,
//!     tailroom: 0,
//!     objs_per_slab: 8,
//! }).unwrap();
//! pool.init();
//!
//! let mut executor = Executor::new()?;
//! executor.run(async move {
//!     let (mut left, mut right) = UnixStream::pair().unwrap();
//!
//!     let mut send = pool.alloc().unwrap();
//!     send.payload_append(b"hello").unwrap();
//!     let (res, _send) = left.write_all(send).await;
//!     res.unwrap();
//!
//!     let recv = pool.alloc().unwrap();
//!     let (res, recv) = right.read_exact(recv, 5).await;
//!     res.unwrap();
//!     assert_eq!(recv.payload_bytes(), b"hello");
//! })?;
//! # Ok::<(), std::io::Error>(())
//! ```

use std::cell::Cell;
use std::io;
use std::marker::PhantomData;
use std::mem::MaybeUninit;
use std::net::{SocketAddr, SocketAddrV4, SocketAddrV6};
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};
use std::task::{Context, Poll};

use crate::runtime::buffer::IoBuffReadWrite;
use crate::runtime::executor::{
    completed_op_ctx, drop_op_ptr_unchecked, note_accept_descriptor_exhaustion,
    note_accept_readiness_rearm, poll_ctx_from_waker, refresh_op_waiter_from_waker,
    submit_retained_sqe,
};
use crate::runtime::fd::{LingerProvenance, RetainedListenerFd, RuntimeFd};
use crate::runtime::op::CompletionState;
use crate::runtime::reactor::Reactor;
use crate::runtime::timer::TimeoutError;

pub mod resolver;
pub mod sctp;
pub(crate) mod send_sqe;
pub(crate) mod stream;
#[doc(hidden)]
pub use stream::WriteBufferChain;
#[doc(inline)]
pub use stream::{
    ReadExactAppendFuture, ReadExactFuture, ReadFuture, ReadvExactFuture, ReadvFuture,
    WriteAllFuture, WriteFuture, WritevAllFuture, WritevAllProjectedFuture, WritevFuture,
    WritevProjectedFuture,
};
pub mod tcp;
pub mod tls;
#[cfg(feature = "test-support")]
pub(crate) mod tls_test_peer;
pub mod udp;
pub mod unix;

/// Safe projection interface for retained owned vectored writes.
///
/// Implement this for compact owned message carriers that can expose their
/// already-encoded byte pieces as borrowed slices. For a non-empty operation,
/// FlowIO moves the carrier into retained operation state before calling
/// [`WritevProjection::project_writev`], so slices may safely point into inline
/// fields or owned allocations inside the carrier. The retained carrier and
/// FlowIO-owned `iovec` scratch remain alive until the original write CQE
/// retires, even if the future is dropped. Declared-empty projections are
/// validated locally before retained state is allocated.
///
/// `writev_count_and_len` must report the number of active non-empty pieces
/// and the total byte length that `project_writev` will push. Empty pieces are
/// ignored by [`WritevPieces::push`]. Mismatches are rejected with
/// [`io::ErrorKind::InvalidInput`]. For an otherwise valid call, FlowIO still
/// invokes `project_writev` once when the reported shape is `(0, 0)`: a valid
/// empty projection pushes no non-empty piece and returns `Ok(())`;
/// implementation errors propagate.
///
/// This trait does not expose a borrowed-SQE API. Callers pass ownership of
/// the carrier to the stream method and receive it back with the I/O result.
///
/// This is a preferred fast-path API when a protocol already owns a compact
/// message carrier with segmented byte fields: it copies pointer/length
/// metadata, not message bytes. Use the contiguous stream `write` APIs for one
/// contiguous byte range, and use non-`_all` projected writes when the caller
/// can track partial progress explicitly.
///
/// # Example
/// ```no_run
/// use flowio::net::tcp::TcpStream;
/// use flowio::net::{WritevPieces, WritevProjection};
/// use std::io;
///
/// struct Message {
///     header: [u8; 4],
///     body: Vec<u8>,
/// }
///
/// impl WritevProjection for Message {
///     fn writev_count_and_len(&self) -> (usize, usize) {
///         (2, self.header.len() + self.body.len())
///     }
///
///     fn project_writev<'a>(&'a self, pieces: &mut WritevPieces<'a>) -> io::Result<()> {
///         pieces.push(&self.header)?;
///         pieces.push(&self.body)?;
///         Ok(())
///     }
/// }
///
/// # async fn send(mut stream: TcpStream, msg: Message) -> io::Result<Message> {
/// let (result, msg) = stream.writev_projected(msg).await;
/// result?;
/// Ok(msg)
/// # }
/// ```
pub trait WritevProjection: 'static {
    /// Returns `(active_non_empty_piece_count, total_byte_len)`.
    fn writev_count_and_len(&self) -> (usize, usize);

    /// Projects borrowed byte pieces from this retained carrier.
    ///
    /// The lifetime on `pieces` ties every pushed slice to the borrow of
    /// `self`, preventing safe implementations from pushing temporary slices
    /// that die when this method returns.
    fn project_writev<'a>(&'a self, pieces: &mut WritevPieces<'a>) -> io::Result<()>;
}

/// Sink used by [`WritevProjection`] implementations to expose write pieces.
///
/// Values of this type are constructed only by FlowIO. Implementations push
/// slices borrowed from the retained carrier; FlowIO stores only pointer/length
/// metadata in retained scratch and never copies the slice bytes.
///
/// This type belongs to the projected vectored-write fast path and copies only
/// slice metadata. Use it only inside [`WritevProjection::project_writev`];
/// callers do not construct it directly.
///
/// # Example
/// ```
/// use flowio::net::{WritevPieces, WritevProjection};
/// use std::io;
///
/// struct Pair {
///     first: [u8; 2],
///     second: [u8; 2],
/// }
///
/// impl WritevProjection for Pair {
///     fn writev_count_and_len(&self) -> (usize, usize) {
///         (2, self.first.len() + self.second.len())
///     }
///
///     fn project_writev<'a>(&'a self, pieces: &mut WritevPieces<'a>) -> io::Result<()> {
///         pieces.push(&self.first)?;
///         pieces.push(&self.second)?;
///         Ok(())
///     }
/// }
/// ```
pub struct WritevPieces<'a> {
    /// FlowIO-owned retained scratch where projected slice metadata is written.
    iovecs: &'a mut [MaybeUninit<libc::iovec>],
    /// Number of initialized non-empty `iovec` entries.
    count: usize,
    /// Sum of bytes represented by initialized entries.
    total: usize,
}

impl<'a> WritevPieces<'a> {
    #[inline(always)]
    pub(crate) fn new(iovecs: &'a mut [MaybeUninit<libc::iovec>]) -> Self {
        Self {
            iovecs,
            count: 0,
            total: 0,
        }
    }

    /// Adds a non-empty byte piece to the projected write.
    ///
    /// Empty slices are ignored, matching FlowIO's existing owned-chain
    /// `writev` behavior. If the projection pushes more non-empty pieces than
    /// were reported by `writev_count_and_len`, this returns
    /// [`io::ErrorKind::InvalidInput`].
    #[inline(always)]
    pub fn push(&mut self, bytes: &'a [u8]) -> io::Result<()> {
        if bytes.is_empty() {
            return Ok(());
        }

        if self.count >= self.iovecs.len() {
            return Err(io::Error::from(io::ErrorKind::InvalidInput));
        }

        let total = self
            .total
            .checked_add(bytes.len())
            .ok_or_else(|| io::Error::from(io::ErrorKind::InvalidInput))?;

        self.iovecs[self.count].write(libc::iovec {
            iov_base: bytes.as_ptr() as *mut libc::c_void,
            iov_len: bytes.len(),
        });
        self.count += 1;
        self.total = total;
        Ok(())
    }

    #[inline(always)]
    pub(crate) fn count(&self) -> usize {
        self.count
    }

    #[inline(always)]
    pub(crate) fn total(&self) -> usize {
        self.total
    }
}

// ---------------------------------------------------------------------------
// Shared option helpers for rental-pattern futures.
// Rental futures store caller values in `Option<T>` so they can move each value
// out exactly once on completion or error. These helpers preserve that
// invariant without carrying `expect()` branches in the hot path.
// ---------------------------------------------------------------------------

/// # Safety
/// The caller must guarantee the option is `Some`.
#[inline(always)]
pub(crate) unsafe fn opt_take<T>(opt: &mut Option<T>) -> T {
    debug_assert!(opt.is_some(), "buffer option was None (internal invariant)");
    unsafe { opt.take().unwrap_unchecked() }
}

/// # Safety
/// The caller must guarantee the option is `Some`.
#[inline(always)]
pub(crate) unsafe fn opt_ref<T>(opt: &Option<T>) -> &T {
    debug_assert!(opt.is_some(), "buffer option was None (internal invariant)");
    unsafe { opt.as_ref().unwrap_unchecked() }
}

/// # Safety
/// The caller must guarantee the option is `Some`.
#[inline(always)]
pub(crate) unsafe fn opt_mut<T>(opt: &mut Option<T>) -> &mut T {
    debug_assert!(opt.is_some(), "buffer option was None (internal invariant)");
    unsafe { opt.as_mut().unwrap_unchecked() }
}

const READ_LEN_EXCEEDS_WRITABLE: &str = "length exceeds writable buffer capacity";
const LEN_EXCEEDS_U32: &str = "length exceeds io_uring u32 byte-count limit";
const READ_PROGRESS_EXCEEDS_WRITABLE: &str = "read completion exceeds writable buffer capacity";
const READ_PUBLICATION_OVERFLOW: &str = "read publication length overflow";

#[inline(always)]
pub(crate) fn invalid_input(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message)
}

#[inline(always)]
pub(crate) fn invalid_input_kind() -> io::Error {
    io::Error::from(io::ErrorKind::InvalidInput)
}

/// Builds either FlowIO's production static-message `InvalidData` diagnostic
/// or a message-free comparator selected at monomorphization time.
///
/// `BARE` is used only by feature-gated diagnostic observers. Shipping paths
/// instantiate `false`, preserving their exact cause text. Keeping the choice
/// const-generic lets an observer reach the same validation branch without a
/// runtime mode branch in that path.
#[inline(always)]
pub(crate) fn invalid_data<const BARE: bool>(message: &'static str) -> io::Error {
    if BARE {
        io::Error::from(io::ErrorKind::InvalidData)
    } else {
        io::Error::new(io::ErrorKind::InvalidData, message)
    }
}

/// Result of retiring one completed payload-owning operation.
///
/// Rejection remains a distinct variant so ordinary completion sites cannot
/// accidentally interpret or publish a CQE observed from an invalid context.
/// The variant itself encodes the fixed `NotConnected` response; retaining the
/// CQE result lets the rich SCTP receive paths update record-recovery state
/// before returning that response.
#[repr(u8)]
enum CompletionTake<R, V> {
    Accepted { result: R, value: V },
    ContextRejected { result: R, value: V },
}

impl<R, V> CompletionTake<R, V> {
    #[inline(always)]
    fn from_context(result: R, value: V, context_rejected: bool) -> Self {
        if context_rejected {
            Self::ContextRejected { result, value }
        } else {
            Self::Accepted { result, value }
        }
    }

    /// Resolves the ordinary completion shape while returning the exact owner.
    ///
    /// `accepted` is never called for a rejected context, so callers can keep
    /// accepted-only CQE interpretation and publication inside that closure.
    #[inline(always)]
    fn into_io_result<T>(self, accepted: impl FnOnce(R) -> io::Result<T>) -> (io::Result<T>, V) {
        match self {
            Self::Accepted { result, value } => (accepted(result), value),
            Self::ContextRejected { result: _, value } => {
                (Err(io::Error::from(io::ErrorKind::NotConnected)), value)
            }
        }
    }
}

/// Maps the ordinary byte-count CQE shape used by transport data operations.
#[inline(always)]
fn completion_cqe_result(result: i32) -> io::Result<usize> {
    if result < 0 {
        Err(io::Error::from_raw_os_error(-result))
    } else {
        Ok(result as usize)
    }
}

/// Validates a caller-supplied read length against the writable capacity that
/// the buffer actually exposes to the kernel.
pub(crate) fn checked_read_len(requested: usize, writable: usize) -> io::Result<u32> {
    if requested > writable {
        return Err(invalid_input(READ_LEN_EXCEEDS_WRITABLE));
    }
    if requested > u32::MAX as usize {
        return Err(invalid_input(LEN_EXCEEDS_U32));
    }

    Ok(requested as u32)
}

/// Completes a contiguous read while publishing positive progress relative to
/// a snapshotted writable-buffer base.
///
/// Zero progress deliberately leaves the logical buffer length unchanged.
/// This preserves pre-existing payload on EOF, empty datagrams, and internal
/// no-progress receive transitions.
///
/// # Safety
///
/// `write_base_len` must have been obtained from `buffer.write_base_len()` for
/// the same writable window used by the read. The first `written` bytes at
/// `buffer.as_mut_ptr()` must have been initialized by the producer. `result`
/// may be an error only when those bytes are still intentional caller-visible
/// progress, such as a truncated message; pass zero for errors that produced
/// no caller-visible bytes.
#[inline(always)]
pub(crate) unsafe fn complete_read_with_progress<B: IoBuffReadWrite, T>(
    mut buffer: B,
    write_base_len: usize,
    written: usize,
    result: io::Result<T>,
) -> (io::Result<T>, B) {
    if written == 0 {
        return (result, buffer);
    }

    let publication = if written > buffer.writable_len() {
        Err(invalid_input(READ_PROGRESS_EXCEEDS_WRITABLE))
    } else if let Some(published_len) = write_base_len.checked_add(written) {
        debug_assert_eq!(
            write_base_len,
            buffer.write_base_len(),
            "writable publication base changed while a read was active"
        );
        unsafe { buffer.set_written_len(published_len) };
        Ok(())
    } else {
        Err(invalid_input(READ_PUBLICATION_OVERFLOW))
    };

    match publication {
        Ok(()) => (result, buffer),
        Err(err) => (Err(err), buffer),
    }
}

/// Validates a contiguous send length against io_uring opcodes that accept a
/// 32-bit byte count.
pub(crate) fn checked_send_len(requested: usize) -> io::Result<u32> {
    match send_len_u32(requested) {
        Some(len) => Ok(len),
        None => Err(invalid_input(LEN_EXCEEDS_U32)),
    }
}

/// Classifies a send length without constructing an error.
#[inline(always)]
fn send_len_u32(requested: usize) -> Option<u32> {
    if requested > u32::MAX as usize {
        return None;
    }
    Some(requested as u32)
}

/// Maps the shared runtime timeout result onto a transport connect result.
#[inline(always)]
fn map_connect_timeout<T>(result: Result<io::Result<T>, TimeoutError>) -> io::Result<T> {
    match result {
        Ok(result) => result,
        Err(TimeoutError::Elapsed) => Err(io::Error::from(io::ErrorKind::TimedOut)),
        Err(TimeoutError::Runtime(err)) => Err(err),
    }
}

/// Interpret a connect CQE result code. `io_uring` may re-issue an async
/// `connect(2)` once the socket is writable; if the connection already
/// completed, that retry returns `EISCONN`, which means the connection is
/// established, not an error. A real connect failure returns its own errno
/// (e.g. `ECONNREFUSED`), never `EISCONN`.
fn connect_cqe_result(result: i32) -> io::Result<()> {
    if result >= 0 || result == -libc::EISCONN {
        return Ok(());
    }
    Err(io::Error::from_raw_os_error(-result))
}

/// Builds the one-shot readiness notification used by TCP and SCTP accept.
///
/// The CQE reports only readiness; it never owns an accepted descriptor.
#[inline(always)]
fn accept_readiness_sqe(fd: RawFd, user_data: u64) -> io_uring::squeue::Entry {
    io_uring::opcode::PollAdd::new(io_uring::types::Fd(fd), libc::POLLIN as u32)
        .build()
        .user_data(user_data)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum AcceptFailureDisposition {
    Rearm,
    RearmBarePollError,
    PreserveReadiness,
    LatchTerminal,
    LatchAndPropagate,
    Propagate,
}

/// Classifies a failed owner-thread accept attempt after a positive readiness
/// CQE.
///
/// `POLLHUP` and `POLLNVAL` are unrecoverable poll conditions that can remain
/// continuously ready. A bare `POLLERR` can be transient, so it gets one
/// bounded rearm before exact `EAGAIN` propagates without latching. The caller
/// still attempts `accept4` before consulting this predicate so a queued
/// connection wins when `POLLIN` and an error condition arrive together.
/// Descriptor exhaustion instead preserves the observed mask for one direct
/// attempt by the next caller-owned accept.
#[inline(always)]
fn accept_failure_disposition(
    readiness: i32,
    accept_error: &io::Error,
    bare_poll_error_rearms: u8,
) -> AcceptFailureDisposition {
    debug_assert!(readiness >= 0, "accept readiness mask must be nonnegative");

    if accept_error.kind() == io::ErrorKind::WouldBlock {
        if accept_readiness_is_unrecoverable(readiness) {
            return AcceptFailureDisposition::LatchTerminal;
        }
        if readiness & libc::POLLERR as i32 != 0 {
            return if bare_poll_error_rearms >= BARE_POLLERR_REARM_LIMIT {
                AcceptFailureDisposition::Propagate
            } else {
                AcceptFailureDisposition::RearmBarePollError
            };
        }
        return AcceptFailureDisposition::Rearm;
    }
    if matches!(
        accept_error.raw_os_error(),
        Some(libc::EMFILE) | Some(libc::ENFILE)
    ) {
        return AcceptFailureDisposition::PreserveReadiness;
    }
    if readiness & libc::POLLNVAL as i32 != 0 && accept_error.raw_os_error() == Some(libc::EBADF) {
        return AcceptFailureDisposition::LatchAndPropagate;
    }
    AcceptFailureDisposition::Propagate
}

#[inline(always)]
fn accept_readiness_is_unrecoverable(readiness: i32) -> bool {
    debug_assert!(readiness >= 0, "accept readiness mask must be nonnegative");
    const TERMINAL_EVENTS: i32 = libc::POLLHUP as i32 | libc::POLLNVAL as i32;

    readiness & TERMINAL_EVENTS != 0
}

#[inline(always)]
fn terminal_accept_error() -> io::Error {
    io::Error::from(io::ErrorKind::ConnectionAborted)
}

const BARE_POLLERR_REARM_LIMIT: u8 = 1;
const NO_UNCONSUMED_ACCEPT_READINESS: libc::c_short = -1;

#[repr(u8)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum AcceptReadinessState {
    Ready,
    BarePollErrorRearmed,
    Terminal,
}

/// Reusable one-shot readiness state shared by TCP and SCTP listeners.
///
/// The readiness CQE owns only a retained listener reference. A successful
/// owner-thread `accept4` transfers the accepted descriptor to
/// `finish_accepted`; all other completion, cancellation, and submission
/// behavior is transport-independent.
struct AcceptReadinessSlot {
    /// Listener retained by every readiness submission until its CQE or
    /// cancellation retires.
    listener_fd: RuntimeFd,
    /// Completion state for the current or last readiness submission.
    state_ptr: *mut CompletionState,
    /// Nonnegative readiness mask retained after `EMFILE` or `ENFILE`.
    ///
    /// The next caller-owned accept attempts `accept4` directly instead of
    /// submitting another readiness operation. The `POLLIN` bit requested by
    /// this accept poll and the kernel's always-reported low-bit conditions
    /// are `libc::c_short`-representable; retaining that width preserves the
    /// slot's three-word geometry on both 32-bit and 64-bit targets. `-1`
    /// means no mask is retained.
    unconsumed_readiness: libc::c_short,
    /// True while a transport accept future is borrowing this slot.
    in_use: bool,
    /// Normal, one bare-`POLLERR` rearm consumed, or permanently terminal.
    readiness_state: AcceptReadinessState,
    /// Preserves the listener handles' pre-core `!UnwindSafe` auto-trait
    /// boundary without adding storage.
    _unwind_boundary: PhantomData<&'static Cell<()>>,
}

impl AcceptReadinessSlot {
    fn new(listener_fd: &RuntimeFd) -> Self {
        Self {
            listener_fd: listener_fd.clone_handle(),
            state_ptr: std::ptr::null_mut(),
            unconsumed_readiness: NO_UNCONSUMED_ACCEPT_READINESS,
            in_use: false,
            readiness_state: AcceptReadinessState::Ready,
            _unwind_boundary: PhantomData,
        }
    }

    fn prepare(&mut self) -> io::Result<()> {
        debug_assert!(
            self.unconsumed_readiness < 0 || self.state_ptr.is_null(),
            "retained accept readiness must not coexist with an operation"
        );
        if self.is_terminal() {
            return Err(terminal_accept_error());
        }
        if self.in_use || !self.state_ptr.is_null() {
            return Err(io::Error::from(io::ErrorKind::WouldBlock));
        }
        self.in_use = true;
        Ok(())
    }

    fn drop_future(&mut self) {
        debug_assert!(
            self.unconsumed_readiness < 0 || self.state_ptr.is_null(),
            "retained accept readiness must not coexist with an operation"
        );
        unsafe { drop_op_ptr_unchecked(&mut self.state_ptr) };

        self.finish_nonterminal_attempt();
    }

    fn drop_cached_state(&mut self) {
        // Normal safe use drops the accept future before its listener. This
        // also handles safe `mem::forget` teardown, where the slot can still
        // hold an in-flight or completed readiness state. A readiness CQE
        // never owns an accepted descriptor.
        self.drop_future();
        self.unconsumed_readiness = NO_UNCONSUMED_ACCEPT_READINESS;
        self.readiness_state = AcceptReadinessState::Ready;
    }

    #[inline(always)]
    fn is_terminal(&self) -> bool {
        self.readiness_state == AcceptReadinessState::Terminal
    }

    #[inline(always)]
    fn bare_poll_error_rearms(&self) -> u8 {
        if self.readiness_state == AcceptReadinessState::BarePollErrorRearmed {
            1
        } else {
            0
        }
    }

    #[inline(always)]
    fn record_rearm(&mut self, consumes_bare_poll_error_budget: bool) {
        if consumes_bare_poll_error_budget {
            self.readiness_state = AcceptReadinessState::BarePollErrorRearmed;
        }
        note_accept_readiness_rearm();
    }

    #[inline(always)]
    fn finish_nonterminal_attempt(&mut self) {
        self.in_use = false;
        if !self.is_terminal() {
            self.readiness_state = AcceptReadinessState::Ready;
        }
    }

    fn poll_accept<T, F>(
        &mut self,
        slot_owned: bool,
        cx: &mut Context<'_>,
        finish_accepted: F,
    ) -> Poll<io::Result<T>>
    where
        F: FnOnce(
            OwnedFd,
            LingerProvenance,
            &libc::sockaddr_storage,
            libc::socklen_t,
        ) -> io::Result<T>,
    {
        self.poll_accept_with(slot_owned, cx, accept_nonblocking, finish_accepted)
    }

    fn poll_accept_with<T, A, F>(
        &mut self,
        slot_owned: bool,
        cx: &mut Context<'_>,
        mut accept: A,
        finish_accepted: F,
    ) -> Poll<io::Result<T>>
    where
        A: FnMut(RawFd, bool) -> io::Result<(OwnedFd, libc::sockaddr_storage, libc::socklen_t)>,
        F: FnOnce(
            OwnedFd,
            LingerProvenance,
            &libc::sockaddr_storage,
            libc::socklen_t,
        ) -> io::Result<T>,
    {
        if !slot_owned {
            return Poll::Pending;
        }
        if self.is_terminal() {
            return Poll::Ready(Err(terminal_accept_error()));
        }

        let mut cached_pctx = None;
        let mut retired_listener = None;
        let mut pending_rearm = None;
        let readiness = if self.unconsumed_readiness >= 0 {
            debug_assert!(
                self.state_ptr.is_null(),
                "retained accept readiness must not own an operation"
            );
            let pctx = match poll_ctx_from_waker(cx) {
                Ok(pctx) => pctx,
                Err(err) => {
                    self.finish_nonterminal_attempt();
                    return Poll::Ready(Err(err));
                }
            };
            cached_pctx = Some(pctx);
            Some(i32::from(std::mem::replace(
                &mut self.unconsumed_readiness,
                NO_UNCONSUMED_ACCEPT_READINESS,
            )))
        } else if !self.state_ptr.is_null() {
            let state = unsafe { &*self.state_ptr };
            if state.is_completed() {
                let result = state.result;
                let op_ctx =
                    unsafe { completed_op_ctx(poll_ctx_from_waker(cx).ok(), self.state_ptr) };
                let poll_fd = unsafe {
                    op_ctx.take_retained_payload_unchecked::<RetainedListenerFd>(self.state_ptr)
                };
                unsafe { op_ctx.free_op_unchecked(self.state_ptr) };
                self.state_ptr = std::ptr::null_mut();
                retired_listener = Some(poll_fd);

                if op_ctx.context_rejected() {
                    self.finish_nonterminal_attempt();
                    return Poll::Ready(Err(io::Error::from(io::ErrorKind::NotConnected)));
                }
                if result < 0 {
                    self.finish_nonterminal_attempt();
                    return Poll::Ready(Err(io::Error::from_raw_os_error(-result)));
                }
                Some(result)
            } else {
                None
            }
        } else {
            None
        };

        if let Some(readiness) = readiness {
            let accepted_linger_provenance = self.listener_fd.linger_provenance();
            let listener_fd = retired_listener
                .as_ref()
                .map_or_else(|| self.listener_fd.raw_fd(), RetainedListenerFd::raw_fd);
            match accept(
                listener_fd,
                accepted_linger_provenance == LingerProvenance::Uncertain,
            ) {
                Ok((accepted_fd, addr, addrlen)) => {
                    self.finish_nonterminal_attempt();
                    return Poll::Ready(finish_accepted(
                        accepted_fd,
                        accepted_linger_provenance,
                        &addr,
                        addrlen,
                    ));
                }
                Err(err) => {
                    match accept_failure_disposition(readiness, &err, self.bare_poll_error_rearms())
                    {
                        AcceptFailureDisposition::Rearm => {
                            // Readiness is only a hint and can be stale. Rearm the
                            // one-shot poll without consuming slot ownership or
                            // replenishing an already-used bare-POLLERR budget.
                            pending_rearm = Some(false);
                        }
                        AcceptFailureDisposition::RearmBarePollError => {
                            // A lone `POLLERR` can be transient. Give it exactly
                            // one additional readiness cycle before surfacing the
                            // unchanged `WouldBlock` result.
                            pending_rearm = Some(true);
                        }
                        AcceptFailureDisposition::PreserveReadiness => {
                            note_accept_descriptor_exhaustion();
                            let Ok(stored_readiness) = libc::c_short::try_from(readiness) else {
                                // The accept poll requests only `POLLIN`; its
                                // expected result plus always-reported low-bit
                                // conditions fit `pollfd.revents`. Preserve the
                                // exact accept errno rather than caching a
                                // truncated unexpected result.
                                self.finish_nonterminal_attempt();
                                return Poll::Ready(Err(err));
                            };
                            self.unconsumed_readiness = stored_readiness;
                            self.finish_nonterminal_attempt();
                            return Poll::Ready(Err(err));
                        }
                        AcceptFailureDisposition::LatchTerminal => {
                            self.in_use = false;
                            self.readiness_state = AcceptReadinessState::Terminal;
                            return Poll::Ready(Err(terminal_accept_error()));
                        }
                        AcceptFailureDisposition::LatchAndPropagate => {
                            self.in_use = false;
                            self.readiness_state = AcceptReadinessState::Terminal;
                            return Poll::Ready(Err(err));
                        }
                        AcceptFailureDisposition::Propagate => {
                            self.finish_nonterminal_attempt();
                            return Poll::Ready(Err(err));
                        }
                    }
                }
            }
        }
        drop(retired_listener);

        if self.state_ptr.is_null() {
            let pctx = match cached_pctx {
                Some(pctx) => pctx,
                None => match poll_ctx_from_waker(cx) {
                    Ok(pctx) => pctx,
                    Err(err) => {
                        self.finish_nonterminal_attempt();
                        return Poll::Ready(Err(err));
                    }
                },
            };
            let state_ptr = unsafe { (*pctx.reactor()).alloc_op() };
            if state_ptr.is_null() {
                self.finish_nonterminal_attempt();
                return Poll::Ready(Err(io::Error::from(io::ErrorKind::WouldBlock)));
            }
            self.state_ptr = state_ptr;

            unsafe { (*state_ptr).register_waiter(pctx.owner_task()) };

            let poll_fd = RetainedListenerFd::new(&self.listener_fd);
            unsafe {
                if let Err((err, _poll_fd)) =
                    submit_retained_sqe(&pctx, state_ptr, poll_fd, |poll_fd| {
                        Ok(accept_readiness_sqe(poll_fd.raw_fd(), state_ptr as u64))
                    })
                {
                    Reactor::free_op_unchecked(pctx.reactor(), state_ptr);
                    self.state_ptr = std::ptr::null_mut();
                    self.finish_nonterminal_attempt();
                    return Poll::Ready(Err(err));
                }
            }
            if let Some(consumes_bare_poll_error_budget) = pending_rearm {
                self.record_rearm(consumes_bare_poll_error_budget);
            }
            return Poll::Pending;
        }

        if unsafe { refresh_op_waiter_from_waker(cx, self.state_ptr) } {
            self.drop_future();
            return Poll::Ready(Err(io::Error::from(io::ErrorKind::NotConnected)));
        }
        Poll::Pending
    }
}

/// Runs the failed-preparation regression through one transport's real accept
/// future without requiring a live io_uring or transport-specific kernel
/// support.
#[cfg(test)]
fn test_unprepared_accept_future_parks<T, P>(transport: &'static str, mut poll_accept: P)
where
    P: for<'cx> FnMut(
        &mut AcceptReadinessSlot,
        &mut std::task::Context<'cx>,
        io::Error,
    ) -> (
        std::task::Poll<io::Result<T>>,
        std::task::Poll<io::Result<T>>,
    ),
{
    fn assert_outcomes<T>(
        transport: &str,
        first: std::task::Poll<io::Result<T>>,
        second: std::task::Poll<io::Result<T>>,
    ) {
        match first {
            std::task::Poll::Ready(Err(err)) => {
                assert_eq!(
                    err.kind(),
                    io::ErrorKind::WouldBlock,
                    "{transport} failed preparation lost its pressure error"
                );
            }
            std::task::Poll::Ready(Ok(_)) => {
                panic!("{transport} failed preparation unexpectedly accepted a peer");
            }
            std::task::Poll::Pending => {
                panic!("{transport} failed preparation did not return its error");
            }
        }
        assert!(
            matches!(second, std::task::Poll::Pending),
            "{transport} unprepared accept future did not park after completion"
        );
    }

    crate::runtime::executor::with_ringless_poll_context_for_test(1, |owner, cx| {
        let reactor = owner.reactor_ptr();
        let listener_fd = crate::runtime::fd::RuntimeFd::from_fresh_raw_fd(-1);
        let mut slot = AcceptReadinessSlot::new(&listener_fd);
        slot.in_use = true;
        let input_error = slot
            .prepare()
            .expect_err("occupied accept slot should reject preparation");

        let (first, second) = poll_accept(&mut slot, cx, input_error);
        assert_outcomes(transport, first, second);
        assert!(
            slot.state_ptr.is_null(),
            "{transport} unprepared accept future created a readiness operation"
        );
        assert!(
            slot.in_use,
            "{transport} unprepared accept future cleared the prior owner's marker"
        );
        assert_eq!(
            unsafe { (&*reactor).live_op_count() },
            0,
            "{transport} unprepared accept future retained a reactor operation"
        );

        slot.drop_cached_state();
        assert!(slot.state_ptr.is_null());
        assert!(
            !slot.in_use,
            "{transport} listener teardown retained its accept marker"
        );

        let listener_fd = crate::runtime::fd::RuntimeFd::from_fresh_raw_fd(-1);
        let mut slot = AcceptReadinessSlot::new(&listener_fd);
        let state_ptr = unsafe { (&mut *reactor).alloc_op() };
        assert!(
            !state_ptr.is_null(),
            "{transport} prior accept state allocation failed"
        );
        let mut prior_waiter = crate::runtime::task::TaskHeader::new();
        let prior_waiter_ptr = std::ptr::addr_of_mut!(prior_waiter);
        unsafe { (*state_ptr).register_waiter(prior_waiter_ptr) };
        let prior_flags = unsafe { (*state_ptr).state_flags };
        slot.state_ptr = state_ptr;
        slot.in_use = true;
        let input_error = slot
            .prepare()
            .expect_err("state-bearing accept slot should reject preparation");

        let (first, second) = poll_accept(&mut slot, cx, input_error);
        assert_outcomes(transport, first, second);
        assert_eq!(
            slot.state_ptr, state_ptr,
            "{transport} unprepared accept future replaced the prior state"
        );
        assert_eq!(
            unsafe { (*state_ptr).state_flags },
            prior_flags,
            "{transport} unprepared accept future changed the prior state flags"
        );
        assert_eq!(
            unsafe { (*state_ptr).waiter },
            prior_waiter_ptr,
            "{transport} unprepared accept future replaced the prior waiter"
        );
        assert_eq!(
            prior_waiter.refs.get(),
            2,
            "{transport} unprepared accept future changed the prior waiter refcount"
        );
        assert!(
            slot.in_use,
            "{transport} unprepared accept future cleared the state owner's marker"
        );
        assert_eq!(
            unsafe { (&*reactor).live_op_count() },
            1,
            "{transport} unprepared accept future changed live operation ownership"
        );

        slot.state_ptr = std::ptr::null_mut();
        unsafe { Reactor::free_op_unchecked(reactor, state_ptr) };
        assert_eq!(prior_waiter.refs.get(), 1);
        assert_eq!(unsafe { (&*reactor).live_op_count() }, 0);
        slot.drop_cached_state();
        assert!(!slot.in_use);
    });
}

/// Installs a completed readiness state without submitting an SQE.
///
/// This deterministic test seam lets each transport exercise its real
/// completion branch against an empty nonblocking listener. Because no SQE is
/// submitted, retiring the synthetic state cannot race a later kernel CQE.
#[cfg(all(test, not(miri)))]
fn completed_accept_readiness_for_test(
    cx: &std::task::Context<'_>,
    listener_fd: &crate::runtime::fd::RuntimeFd,
    result: i32,
) -> *mut crate::runtime::op::CompletionState {
    let pctx = crate::runtime::executor::poll_ctx_from_waker(cx)
        .expect("accept readiness test requires a FlowIO task context");
    let reactor = pctx.reactor();
    // SAFETY: `pctx` was extracted from the task context currently polling on
    // this owner thread, so its reactor remains live for this synchronous call.
    let state_ptr = unsafe { (*reactor).alloc_op() };
    assert!(
        !state_ptr.is_null(),
        "accept readiness test completion allocation failed"
    );

    // SAFETY: the same live origin reactor owns both the completion state and
    // the retained-payload allocation; no SQE is built or submitted here.
    let payload = unsafe {
        (*reactor).alloc_retained_payload(crate::runtime::fd::RetainedListenerFd::new(listener_fd))
    };
    // SAFETY: `state_ptr` is a fresh exclusive slot allocated above. The
    // payload came from its reactor, and the synthetic completed state is
    // consumed synchronously by the transport under test.
    unsafe {
        (*state_ptr).attach_retained_payload(payload);
        (*state_ptr).result = result;
        (*state_ptr).set_completed();
    }
    state_ptr
}

/// Runs the shared terminal-readiness regression through one transport's real
/// accept future.
#[cfg(all(test, not(miri)))]
fn test_terminal_accept_readiness<T, P, N>(
    transport: &'static str,
    mut poll_accept: P,
    mut poll_new_accept: N,
) where
    T: 'static,
    P: for<'cx> FnMut(
            &mut AcceptReadinessSlot,
            &mut std::task::Context<'cx>,
            *mut crate::runtime::op::CompletionState,
        ) -> std::task::Poll<io::Result<T>>
        + 'static,
    N: for<'cx> FnMut(
            &mut AcceptReadinessSlot,
            &mut std::task::Context<'cx>,
        ) -> std::task::Poll<io::Result<T>>
        + 'static,
{
    // The terminal-mask branch runs before transport-specific accepted-fd
    // setup, so an empty TCP listener deterministically supplies EAGAIN for
    // both TCP and SCTP without making the unit test kernel-SCTP-dependent.
    let listener = std::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0))
        .unwrap_or_else(|err| panic!("empty {transport}-path listener bind failed: {err}"));
    listener.set_nonblocking(true).unwrap_or_else(|err| {
        panic!("empty {transport}-path listener nonblocking setup failed: {err}")
    });
    let listener_fd =
        crate::runtime::fd::RuntimeFd::from_fresh_owned(std::os::fd::OwnedFd::from(listener));
    // Keep one owner outside `Executor::run` so this readiness-only test does
    // not count the listener's ordinary final close SQE as a readiness submit.
    let listener_keepalive = listener_fd.clone_handle();
    let terminal_masks = [
        libc::POLLHUP as i32,
        libc::POLLNVAL as i32,
        (libc::POLLERR | libc::POLLHUP) as i32,
        (libc::POLLERR | libc::POLLNVAL) as i32,
        (libc::POLLIN | libc::POLLHUP) as i32,
        (libc::POLLIN | libc::POLLNVAL) as i32,
        (libc::POLLERR | libc::POLLHUP | libc::POLLNVAL) as i32,
    ];
    let mut executor = crate::runtime::executor::Executor::new()
        .unwrap_or_else(|err| panic!("{transport} accept readiness executor failed: {err}"));

    executor
        .run(async move {
            for readiness in terminal_masks {
                let mut adapter = AcceptReadinessSlot::new(&listener_fd);
                let state_ptr = std::future::poll_fn(|cx| {
                    std::task::Poll::Ready(completed_accept_readiness_for_test(
                        cx,
                        &listener_fd,
                        readiness,
                    ))
                })
                .await;
                let outcome = std::future::poll_fn(|cx| {
                    std::task::Poll::Ready(poll_accept(&mut adapter, cx, state_ptr))
                })
                .await;

                let err = match outcome {
                    std::task::Poll::Ready(Err(err)) => err,
                    std::task::Poll::Ready(Ok(_)) => {
                        panic!("terminal {transport} readiness unexpectedly accepted a peer")
                    }
                    std::task::Poll::Pending => {
                        panic!("terminal {transport} readiness was rearmed")
                    }
                };
                assert_eq!(err.kind(), io::ErrorKind::ConnectionAborted);
                assert_eq!(err.raw_os_error(), None);
                assert!(
                    adapter.state_ptr.is_null() && !adapter.in_use && adapter.is_terminal(),
                    "terminal {transport} readiness did not latch a reusable empty slot"
                );

                for _ in 0..8 {
                    let outcome = std::future::poll_fn(|cx| {
                        std::task::Poll::Ready(poll_new_accept(&mut adapter, cx))
                    })
                    .await;
                    assert!(
                        matches!(
                            outcome,
                            std::task::Poll::Ready(Err(ref err))
                                if err.kind() == io::ErrorKind::ConnectionAborted
                                    && err.raw_os_error().is_none()
                        ),
                        "latched {transport} listener did not fail a later accept"
                    );
                    assert!(
                        adapter.state_ptr.is_null() && !adapter.in_use && adapter.is_terminal(),
                        "latched {transport} listener changed state on a later accept"
                    );
                }
            }
        })
        .unwrap_or_else(|err| panic!("{transport} accept readiness run failed: {err}"));

    #[cfg(debug_assertions)]
    {
        let stats = executor.last_stats();
        assert_eq!(stats.accept_readiness_rearms, 0);
        assert_eq!(stats.sqe_submits, 0);
    }
    drop(listener_keepalive);
}

/// Runs the exhausted bare-`POLLERR` rearm budget through one transport's real
/// accept future.
#[cfg(all(test, not(miri)))]
fn test_bare_poll_error_budget_exhaustion<T, P>(transport: &'static str, mut poll_accept: P)
where
    T: 'static,
    P: for<'cx> FnMut(
            &mut AcceptReadinessSlot,
            &mut std::task::Context<'cx>,
            *mut crate::runtime::op::CompletionState,
        ) -> std::task::Poll<io::Result<T>>
        + 'static,
{
    // The empty TCP listener deterministically supplies EAGAIN before either
    // transport reaches accepted-descriptor setup.
    let listener = std::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0))
        .unwrap_or_else(|err| panic!("empty {transport}-path listener bind failed: {err}"));
    listener.set_nonblocking(true).unwrap_or_else(|err| {
        panic!("empty {transport}-path listener nonblocking setup failed: {err}")
    });
    let listener_fd =
        crate::runtime::fd::RuntimeFd::from_fresh_owned(std::os::fd::OwnedFd::from(listener));
    // Keep one owner outside `Executor::run` so this readiness-only test does
    // not count the listener's ordinary final close SQE as a readiness submit.
    let listener_keepalive = listener_fd.clone_handle();
    let mut executor = crate::runtime::executor::Executor::new()
        .unwrap_or_else(|err| panic!("{transport} accept readiness executor failed: {err}"));

    executor
        .run(async move {
            for readiness in [libc::POLLERR as i32, (libc::POLLIN | libc::POLLERR) as i32] {
                let mut adapter = AcceptReadinessSlot::new(&listener_fd);
                adapter.readiness_state = AcceptReadinessState::BarePollErrorRearmed;
                let state_ptr = std::future::poll_fn(|cx| {
                    std::task::Poll::Ready(completed_accept_readiness_for_test(
                        cx,
                        &listener_fd,
                        readiness,
                    ))
                })
                .await;
                let outcome = std::future::poll_fn(|cx| {
                    std::task::Poll::Ready(poll_accept(&mut adapter, cx, state_ptr))
                })
                .await;

                let err = match outcome {
                    std::task::Poll::Ready(Err(err)) => err,
                    std::task::Poll::Ready(Ok(_)) => {
                        panic!("empty {transport} listener unexpectedly accepted a peer")
                    }
                    std::task::Poll::Pending => {
                        panic!("{transport} exceeded the bare POLLERR rearm budget")
                    }
                };
                assert_eq!(err.kind(), io::ErrorKind::WouldBlock);
                assert_eq!(err.raw_os_error(), Some(libc::EAGAIN));
                assert!(
                    adapter.state_ptr.is_null()
                        && !adapter.in_use
                        && !adapter.is_terminal()
                        && adapter.readiness_state == AcceptReadinessState::Ready,
                    "{transport} bare POLLERR exhaustion left the slot latched or occupied"
                );

                adapter
                    .prepare()
                    .unwrap_or_else(|err| panic!("{transport} slot was not reusable: {err}"));
                adapter.drop_future();
            }
        })
        .unwrap_or_else(|err| panic!("{transport} accept readiness run failed: {err}"));

    #[cfg(debug_assertions)]
    {
        let stats = executor.last_stats();
        assert_eq!(stats.accept_readiness_rearms, 0);
        assert_eq!(stats.sqe_submits, 0);
    }
    drop(listener_keepalive);
}

/// Accepts one ready connection without allowing the owner thread to block.
///
/// Performing `accept4` after a readiness CQE keeps descriptor creation on the
/// owner thread. An unread or cancelled readiness CQE therefore owns no file
/// descriptor during bounded reactor teardown. Exposed listeners reassert
/// `O_NONBLOCK` before the call; fresh internal listeners keep their creation
/// invariant and avoid the extra status query.
fn accept_nonblocking(
    fd: RawFd,
    reassert_listener_nonblocking: bool,
) -> io::Result<(OwnedFd, libc::sockaddr_storage, libc::socklen_t)> {
    if reassert_listener_nonblocking {
        let flags = unsafe { libc::fcntl(fd, libc::F_GETFL) };
        if flags < 0 {
            return Err(io::Error::last_os_error());
        }
        if flags & libc::O_NONBLOCK == 0 {
            let rc = unsafe { libc::fcntl(fd, libc::F_SETFL, flags | libc::O_NONBLOCK) };
            if rc < 0 {
                return Err(io::Error::last_os_error());
            }
        }
    }

    let mut addr: libc::sockaddr_storage = unsafe { std::mem::zeroed() };
    let mut addrlen = std::mem::size_of::<libc::sockaddr_storage>() as libc::socklen_t;
    let accepted = unsafe {
        libc::accept4(
            fd,
            &mut addr as *mut libc::sockaddr_storage as *mut libc::sockaddr,
            &mut addrlen,
            libc::SOCK_NONBLOCK | libc::SOCK_CLOEXEC,
        )
    };
    if accepted < 0 {
        return Err(io::Error::last_os_error());
    }

    // SAFETY: a successful accept4 returns one newly installed descriptor and
    // transfers its sole userspace ownership to this caller.
    let accepted = unsafe { OwnedFd::from_raw_fd(accepted) };
    Ok((accepted, addr, addrlen))
}

fn socket_domain(addr: SocketAddr) -> libc::c_int {
    match addr {
        SocketAddr::V4(_) => libc::AF_INET,
        SocketAddr::V6(_) => libc::AF_INET6,
    }
}

fn new_nonblocking_socket(domain: libc::c_int, kind: libc::c_int) -> io::Result<RawFd> {
    let fd = unsafe { libc::socket(domain, kind | libc::SOCK_NONBLOCK | libc::SOCK_CLOEXEC, 0) };
    if fd < 0 {
        return Err(io::Error::last_os_error());
    }

    Ok(fd)
}

/// Applies the standard stream-shutdown mapping to one socket descriptor.
#[inline(always)]
fn shutdown_socket(fd: RawFd, how: std::net::Shutdown) -> io::Result<()> {
    let how = match how {
        std::net::Shutdown::Read => libc::SHUT_RD,
        std::net::Shutdown::Write => libc::SHUT_WR,
        std::net::Shutdown::Both => libc::SHUT_RDWR,
    };
    let rc = unsafe { libc::shutdown(fd, how) };
    if rc < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

#[inline(always)]
fn close_fd(fd: RawFd) {
    unsafe {
        libc::close(fd);
    }
}

fn set_reuse_addr(fd: RawFd) -> io::Result<()> {
    set_sock_opt(fd, libc::SOL_SOCKET, libc::SO_REUSEADDR, &1i32)
}

fn socket_addr_to_c(addr: SocketAddr) -> (libc::sockaddr_storage, libc::socklen_t) {
    let mut storage: libc::sockaddr_storage = unsafe { std::mem::zeroed() };
    let len;

    match addr {
        SocketAddr::V4(v4) => {
            let sockaddr_in = unsafe { &mut *(&mut storage as *mut _ as *mut libc::sockaddr_in) };
            sockaddr_in.sin_family = libc::AF_INET as libc::sa_family_t;
            sockaddr_in.sin_port = v4.port().to_be();
            sockaddr_in.sin_addr.s_addr = u32::from_ne_bytes(v4.ip().octets());
            len = std::mem::size_of::<libc::sockaddr_in>() as libc::socklen_t;
        }
        SocketAddr::V6(v6) => {
            let sockaddr_in6 = unsafe { &mut *(&mut storage as *mut _ as *mut libc::sockaddr_in6) };
            sockaddr_in6.sin6_family = libc::AF_INET6 as libc::sa_family_t;
            sockaddr_in6.sin6_port = v6.port().to_be();
            sockaddr_in6.sin6_addr.s6_addr = v6.ip().octets();
            sockaddr_in6.sin6_flowinfo = v6.flowinfo();
            sockaddr_in6.sin6_scope_id = v6.scope_id();
            len = std::mem::size_of::<libc::sockaddr_in6>() as libc::socklen_t;
        }
    }

    (storage, len)
}

/// Prepared connect address stored in the slot until submission.
#[derive(Clone, Copy)]
struct RetainedConnectAddr {
    /// Prepared peer address retained until connect completion.
    addr: libc::sockaddr_storage,
    /// Length of the prepared peer address.
    addrlen: libc::socklen_t,
}

impl RetainedConnectAddr {
    fn from_socket_addr(addr: SocketAddr) -> Self {
        let (addr, addrlen) = socket_addr_to_c(addr);
        Self { addr, addrlen }
    }

    fn addr_ptr(&self) -> *const libc::sockaddr {
        &self.addr as *const libc::sockaddr_storage as *const libc::sockaddr
    }
}

/// Sole socket owner and kernel-visible address retained until the target
/// connect CQE retires.
struct RetainedConnectPayload {
    /// Socket whose numeric descriptor is referenced by the connect SQE.
    fd: OwnedFd,
    /// Prepared peer address referenced by the connect SQE.
    addr: RetainedConnectAddr,
}

impl RetainedConnectPayload {
    fn new(fd: OwnedFd, addr: RetainedConnectAddr) -> Self {
        Self { fd, addr }
    }

    fn raw_fd(&self) -> RawFd {
        self.fd.as_raw_fd()
    }

    fn addr_ptr(&self) -> *const libc::sockaddr {
        self.addr.addr_ptr()
    }

    fn addrlen(&self) -> libc::socklen_t {
        self.addr.addrlen
    }

    fn into_fd(self) -> OwnedFd {
        self.fd
    }
}

/// Common owner-thread state for one asynchronous connect submission.
///
/// The socket stays outside [`RuntimeFd`] until connection establishment and
/// any transport-specific completion work succeeds. Before submission this
/// slot owns it directly; after submission the retained completion payload owns
/// it until the target CQE proves that the kernel no longer references its
/// numeric descriptor.
struct ConnectSubmissionSlot<C> {
    /// Completion state for the current or last connect submission.
    state_ptr: *mut CompletionState,
    /// True while a reusable transport future is borrowing this slot.
    in_use: bool,
    /// Socket being prepared for the current attempt. Submission moves this
    /// owner into [`RetainedConnectPayload`].
    fd: Option<OwnedFd>,
    /// Prepared remote address retained until submission.
    addr: Option<RetainedConnectAddr>,
    /// Transport data needed after the connect CQE succeeds.
    completion_data: C,
}

impl<C> ConnectSubmissionSlot<C> {
    fn new(completion_data: C) -> Self {
        Self {
            state_ptr: std::ptr::null_mut(),
            in_use: false,
            fd: None,
            addr: None,
            completion_data,
        }
    }

    fn cleanup_fd(&mut self) {
        self.fd = None;
    }

    fn drop_future(&mut self) {
        unsafe { drop_op_ptr_unchecked(&mut self.state_ptr) };
        self.addr = None;
        self.cleanup_fd();
        self.in_use = false;
    }

    fn retire_cached_state(&mut self) {
        unsafe { drop_op_ptr_unchecked(&mut self.state_ptr) };
        self.in_use = false;
    }

    fn drop_cached_state(&mut self) {
        self.drop_future();
    }

    #[inline(always)]
    fn poll_connect<T, F>(
        &mut self,
        cx: &mut Context<'_>,
        finish_connected: F,
    ) -> Poll<io::Result<T>>
    where
        F: FnOnce(OwnedFd, &C) -> io::Result<T>,
    {
        if !self.state_ptr.is_null() {
            let state = unsafe { &*self.state_ptr };
            if state.is_completed() {
                let result = state.result;
                let op_ctx =
                    unsafe { completed_op_ctx(poll_ctx_from_waker(cx).ok(), self.state_ptr) };
                let payload = unsafe {
                    op_ctx.take_retained_payload_unchecked::<RetainedConnectPayload>(self.state_ptr)
                };
                let completion =
                    CompletionTake::from_context(result, payload, op_ctx.context_rejected());
                unsafe { op_ctx.free_op_unchecked(self.state_ptr) };
                self.state_ptr = std::ptr::null_mut();
                self.in_use = false;

                let (result, payload) = completion.into_io_result(connect_cqe_result);
                return Poll::Ready(match result {
                    Ok(()) => finish_connected(payload.into_fd(), &self.completion_data),
                    Err(err) => Err(err),
                });
            }
        }

        if self.state_ptr.is_null() {
            let pctx = match poll_ctx_from_waker(cx) {
                Ok(pctx) => pctx,
                Err(err) => {
                    self.in_use = false;
                    self.cleanup_fd();
                    return Poll::Ready(Err(err));
                }
            };
            let state_ptr = unsafe { (*pctx.reactor()).alloc_op() };
            if state_ptr.is_null() {
                self.in_use = false;
                self.cleanup_fd();
                return Poll::Ready(Err(io::Error::from(io::ErrorKind::WouldBlock)));
            }
            self.state_ptr = state_ptr;

            unsafe { (*state_ptr).register_waiter(pctx.owner_task()) };

            let payload = match self.addr.take() {
                Some(addr) => match self.fd.take() {
                    Some(fd) => RetainedConnectPayload::new(fd, addr),
                    None => {
                        unsafe { Reactor::free_op_unchecked(pctx.reactor(), state_ptr) };
                        self.state_ptr = std::ptr::null_mut();
                        self.in_use = false;
                        return Poll::Ready(Err(io::Error::from(io::ErrorKind::InvalidInput)));
                    }
                },
                None => {
                    unsafe { Reactor::free_op_unchecked(pctx.reactor(), state_ptr) };
                    self.state_ptr = std::ptr::null_mut();
                    self.in_use = false;
                    self.cleanup_fd();
                    return Poll::Ready(Err(io::Error::from(io::ErrorKind::InvalidInput)));
                }
            };

            unsafe {
                if let Err((err, _payload)) =
                    submit_retained_sqe(&pctx, state_ptr, payload, |payload| {
                        let sqe = io_uring::opcode::Connect::new(
                            io_uring::types::Fd(payload.raw_fd()),
                            payload.addr_ptr(),
                            payload.addrlen(),
                        )
                        .build()
                        .user_data(state_ptr as u64);
                        Ok(sqe)
                    })
                {
                    Reactor::free_op_unchecked(pctx.reactor(), state_ptr);
                    self.state_ptr = std::ptr::null_mut();
                    self.in_use = false;
                    self.cleanup_fd();
                    return Poll::Ready(Err(err));
                }
            }
            return Poll::Pending;
        }

        if unsafe { refresh_op_waiter_from_waker(cx, self.state_ptr) } {
            self.drop_future();
            return Poll::Ready(Err(io::Error::from(io::ErrorKind::NotConnected)));
        }
        Poll::Pending
    }
}

fn socket_addr_from_c(
    storage: &libc::sockaddr_storage,
    len: libc::socklen_t,
) -> io::Result<SocketAddr> {
    let family = storage.ss_family as libc::c_int;

    if family == libc::AF_INET && len >= std::mem::size_of::<libc::sockaddr_in>() as libc::socklen_t
    {
        let sockaddr_in = unsafe { &*(storage as *const _ as *const libc::sockaddr_in) };
        let ip = std::net::Ipv4Addr::from(sockaddr_in.sin_addr.s_addr.to_ne_bytes());
        let port = u16::from_be(sockaddr_in.sin_port);
        return Ok(SocketAddr::V4(SocketAddrV4::new(ip, port)));
    }

    if family == libc::AF_INET6
        && len >= std::mem::size_of::<libc::sockaddr_in6>() as libc::socklen_t
    {
        let sockaddr_in6 = unsafe { &*(storage as *const _ as *const libc::sockaddr_in6) };
        let ip = std::net::Ipv6Addr::from(sockaddr_in6.sin6_addr.s6_addr);
        let port = u16::from_be(sockaddr_in6.sin6_port);
        return Ok(SocketAddr::V6(SocketAddrV6::new(
            ip,
            port,
            sockaddr_in6.sin6_flowinfo,
            sockaddr_in6.sin6_scope_id,
        )));
    }

    Err(io::Error::from(io::ErrorKind::InvalidData))
}

fn current_local_addr(fd: RawFd) -> io::Result<SocketAddr> {
    let mut storage: libc::sockaddr_storage = unsafe { std::mem::zeroed() };
    let mut len = std::mem::size_of::<libc::sockaddr_storage>() as libc::socklen_t;

    let rc =
        unsafe { libc::getsockname(fd, &mut storage as *mut _ as *mut libc::sockaddr, &mut len) };
    if rc < 0 {
        return Err(io::Error::last_os_error());
    }

    socket_addr_from_c(&storage, len)
}

fn current_peer_addr(fd: RawFd) -> io::Result<SocketAddr> {
    let mut storage: libc::sockaddr_storage = unsafe { std::mem::zeroed() };
    let mut len = std::mem::size_of::<libc::sockaddr_storage>() as libc::socklen_t;

    let rc =
        unsafe { libc::getpeername(fd, &mut storage as *mut _ as *mut libc::sockaddr, &mut len) };
    if rc < 0 {
        return Err(io::Error::last_os_error());
    }

    socket_addr_from_c(&storage, len)
}

/// Pointer-bearing fields copied into a retained `libc::msghdr`.
///
/// The referenced address, iovec, and control storage must remain live and at
/// stable addresses for every kernel operation that uses the resulting header.
pub(super) struct MsgHdrInit {
    /// Optional socket-address storage, or null when no address is supplied.
    pub(super) name: *mut libc::c_void,
    /// Input capacity or prepared length of `name`, depending on the opcode.
    pub(super) namelen: libc::socklen_t,
    /// First entry in the kernel-facing iovec array.
    pub(super) iov: *mut libc::iovec,
    /// Number of initialized entries starting at `iov`.
    pub(super) iovlen: usize,
    /// Optional ancillary-data buffer, or null when no control data is used.
    pub(super) control: *mut libc::c_void,
    /// Writable capacity or initialized length of `control` for the operation.
    pub(super) controllen: usize,
}

#[inline(always)]
pub(super) fn write_msghdr(dst: &mut MaybeUninit<libc::msghdr>, init: MsgHdrInit) {
    dst.write(libc::msghdr {
        msg_name: init.name,
        msg_namelen: init.namelen,
        msg_iov: init.iov,
        msg_iovlen: init.iovlen,
        msg_control: init.control,
        msg_controllen: init.controllen,
        msg_flags: 0,
    });
}

fn set_reuse_port(fd: RawFd) -> io::Result<()> {
    set_sock_opt(fd, libc::SOL_SOCKET, libc::SO_REUSEPORT, &1i32)
}

// Shared socket-buffer option helpers used by TCP, Unix, and UDP sockets.

fn sock_send_buffer_size(fd: RawFd) -> io::Result<usize> {
    let val: libc::c_int = get_sock_opt(fd, libc::SOL_SOCKET, libc::SO_SNDBUF)?;
    Ok(val as usize)
}

fn set_sock_send_buffer_size(fd: RawFd, size: usize) -> io::Result<()> {
    let size = socket_buffer_size_to_c_int(size)?;
    set_sock_opt(fd, libc::SOL_SOCKET, libc::SO_SNDBUF, &size)
}

fn sock_recv_buffer_size(fd: RawFd) -> io::Result<usize> {
    let val: libc::c_int = get_sock_opt(fd, libc::SOL_SOCKET, libc::SO_RCVBUF)?;
    Ok(val as usize)
}

fn set_sock_recv_buffer_size(fd: RawFd, size: usize) -> io::Result<()> {
    let size = socket_buffer_size_to_c_int(size)?;
    set_sock_opt(fd, libc::SOL_SOCKET, libc::SO_RCVBUF, &size)
}

fn socket_buffer_size_to_c_int(size: usize) -> io::Result<libc::c_int> {
    if size > libc::c_int::MAX as usize {
        return Err(invalid_input("socket buffer size exceeds c_int::MAX"));
    }

    Ok(size as libc::c_int)
}

fn set_sock_opt<T>(fd: RawFd, level: libc::c_int, name: libc::c_int, value: &T) -> io::Result<()> {
    let rc = unsafe {
        libc::setsockopt(
            fd,
            level,
            name,
            value as *const T as *const libc::c_void,
            std::mem::size_of::<T>() as libc::socklen_t,
        )
    };
    if rc < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

fn get_sock_opt<T: Default>(fd: RawFd, level: libc::c_int, name: libc::c_int) -> io::Result<T> {
    let mut value = T::default();
    let mut len = std::mem::size_of::<T>() as libc::socklen_t;
    let rc = unsafe {
        libc::getsockopt(
            fd,
            level,
            name,
            &mut value as *mut T as *mut libc::c_void,
            &mut len,
        )
    };
    if rc < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::buffer::IoBuffMut;
    use static_assertions::assert_not_impl_any;
    use std::cell::Cell;

    assert_not_impl_any!(AcceptReadinessSlot: Send, Sync, std::panic::UnwindSafe, std::panic::RefUnwindSafe);

    #[test]
    fn accept_readiness_slot_preserves_listener_layout_and_auto_traits() {
        assert_eq!(std::mem::size_of::<AcceptReadinessSlot>(), 24);
        assert_eq!(std::mem::align_of::<AcceptReadinessSlot>(), 8);
    }

    #[test]
    fn completion_take_resolves_rejection_without_running_accepted_mapping() {
        let accepted = CompletionTake::from_context(7_i32, 11_usize, false);
        let (result, value) = accepted.into_io_result(|result| Ok(result as usize + 1));
        assert_eq!(result.expect("accepted completion mapping failed"), 8);
        assert_eq!(value, 11);

        let rejected = CompletionTake::from_context(-libc::EPIPE, 13_usize, true);
        let (result, value) =
            rejected.into_io_result::<usize>(|_| panic!("rejected completion ran accepted mapper"));
        assert_eq!(
            result
                .expect_err("rejected completion unexpectedly succeeded")
                .kind(),
            io::ErrorKind::NotConnected
        );
        assert_eq!(value, 13);

        let rejected = CompletionTake::from_context(17_usize, 19_usize, true);
        assert!(matches!(
            rejected,
            CompletionTake::ContextRejected {
                result: 17,
                value: 19
            }
        ));
    }

    #[test]
    fn completion_take_preserves_previous_return_carrier_layout() {
        fn assert_layout<R, V>() {
            assert_eq!(
                std::mem::size_of::<CompletionTake<R, V>>(),
                std::mem::size_of::<(R, V, bool)>()
            );
            assert_eq!(
                std::mem::align_of::<CompletionTake<R, V>>(),
                std::mem::align_of::<(R, V, bool)>()
            );
            assert_eq!(
                std::mem::size_of::<Option<CompletionTake<R, V>>>(),
                std::mem::size_of::<Option<(R, V, bool)>>()
            );
            assert_eq!(
                std::mem::align_of::<Option<CompletionTake<R, V>>>(),
                std::mem::align_of::<Option<(R, V, bool)>>()
            );
        }

        assert_layout::<i32, usize>();
        assert_layout::<i32, Vec<u8>>();
        assert_layout::<io::Result<usize>, [usize; 12]>();
    }

    #[test]
    fn contiguous_read_progress_appends_to_prefilled_iobuff() {
        let mut buffer = IoBuffMut::new(0, 8, 0).expect("buffer allocation failed");
        buffer
            .payload_append(b"HEAD")
            .expect("prefix append failed");
        let write_base_len = buffer.write_base_len();
        unsafe { std::ptr::copy_nonoverlapping(b"ok".as_ptr(), buffer.as_mut_ptr(), 2) };

        let (result, buffer) = unsafe {
            complete_read_with_progress(buffer, write_base_len, 2, Ok::<_, io::Error>(2))
        };

        assert_eq!(result.expect("publication failed"), 2);
        assert_eq!(buffer.payload_bytes(), b"HEADok");
    }

    #[test]
    fn contiguous_read_zero_preserves_flat_and_sealed_buffers() {
        let flat = b"HEAD".to_vec();
        let flat_base = flat.write_base_len();
        let (result, flat) =
            unsafe { complete_read_with_progress(flat, flat_base, 0, Ok::<_, io::Error>(0)) };
        assert_eq!(result.expect("zero publication failed"), 0);
        assert_eq!(flat, b"HEAD");

        let mut sealed = IoBuffMut::new(0, 8, 2).expect("buffer allocation failed");
        sealed
            .payload_append(b"HEAD")
            .expect("payload append failed");
        sealed.tailroom_append(b":T").expect("tail append failed");
        assert_eq!(sealed.writable_len(), 0);
        let sealed_base = sealed.write_base_len();
        let (result, sealed) =
            unsafe { complete_read_with_progress(sealed, sealed_base, 0, Ok::<_, io::Error>(0)) };
        assert_eq!(result.expect("sealed zero publication failed"), 0);
        assert_eq!(sealed.bytes(), b"HEAD:T");
        assert_eq!(sealed.payload_bytes(), b"HEAD");
    }

    #[test]
    fn contiguous_read_base_counts_payload_not_active_headroom() {
        let mut buffer = IoBuffMut::new(4, 4, 0).expect("buffer allocation failed");
        buffer
            .headroom_prepend(b"H:")
            .expect("headroom prepend failed");
        assert_eq!(buffer.write_base_len(), 0);
        let write_base_len = buffer.write_base_len();
        unsafe { std::ptr::copy_nonoverlapping(b"ok".as_ptr(), buffer.as_mut_ptr(), 2) };

        let (result, buffer) = unsafe {
            complete_read_with_progress(buffer, write_base_len, 2, Ok::<_, io::Error>(2))
        };

        assert_eq!(result.expect("publication failed"), 2);
        assert_eq!(buffer.bytes(), b"H:ok");
        assert_eq!(buffer.payload_bytes(), b"ok");
    }

    #[test]
    fn contiguous_read_default_base_overwrites_after_positive_progress() {
        let mut buffer = b"HEAD".to_vec();
        buffer.reserve(4);
        assert_eq!(buffer.write_base_len(), 0);
        let write_base_len = buffer.write_base_len();
        unsafe { std::ptr::copy_nonoverlapping(b"ok".as_ptr(), buffer.as_mut_ptr(), 2) };

        let (result, buffer) = unsafe {
            complete_read_with_progress(buffer, write_base_len, 2, Ok::<_, io::Error>(2))
        };

        assert_eq!(result.expect("publication failed"), 2);
        assert_eq!(buffer, b"ok");
    }

    #[test]
    fn socket_buffer_size_conversion_accepts_c_int_max() {
        assert_eq!(
            socket_buffer_size_to_c_int(libc::c_int::MAX as usize).expect("conversion failed"),
            libc::c_int::MAX
        );
    }

    #[test]
    fn socket_buffer_size_conversion_rejects_overflow() {
        let err = socket_buffer_size_to_c_int(libc::c_int::MAX as usize + 1)
            .expect_err("oversize socket buffer should fail");
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
    }

    #[test]
    fn connect_cqe_result_treats_eisconn_as_established() {
        connect_cqe_result(0).expect("zero connect CQE should succeed");
        connect_cqe_result(-libc::EISCONN).expect("EISCONN connect CQE should mean established");

        let err = connect_cqe_result(-libc::ECONNREFUSED)
            .expect_err("real connect failure should retain its errno");
        assert_eq!(err.raw_os_error(), Some(libc::ECONNREFUSED));
    }

    #[cfg(not(miri))]
    #[test]
    fn ring_abandoned_connect_submission_retains_socket_with_kernel_visible_state() {
        crate::runtime::executor::with_ringless_poll_context_for_test(1, |owner, cx| {
            let reactor = owner.reactor_ptr();
            let state_ptr = unsafe { (&mut *reactor).alloc_op() };
            assert!(!state_ptr.is_null(), "connect state allocation failed");
            let fd = crate::runtime::fd::distinctive_closeable_test_fd()
                .expect("connect fd creation failed");
            // SAFETY: the test-created descriptor has no other owner.
            let owned_fd = unsafe { OwnedFd::from_raw_fd(fd) };
            let retained = unsafe {
                (&mut *reactor).alloc_retained_payload(RetainedConnectPayload::new(
                    owned_fd,
                    RetainedConnectAddr::from_socket_addr(SocketAddr::from(([127, 0, 0, 1], 9))),
                ))
            };
            unsafe {
                (*state_ptr).attach_retained_payload(retained);
                (*state_ptr).set_ring_abandoned();
            }

            let mut slot = ConnectSubmissionSlot::new(());
            slot.state_ptr = state_ptr;
            slot.in_use = true;
            let outcome: Poll<io::Result<()>> = slot.poll_connect(cx, |_, _| {
                panic!("ring-abandoned connect reached its success finalizer")
            });
            assert!(matches!(
                outcome,
                Poll::Ready(Err(err)) if err.kind() == io::ErrorKind::NotConnected
            ));
            assert!(slot.state_ptr.is_null());
            assert!(!slot.in_use);
            assert!(slot.fd.is_none());
            assert!(slot.addr.is_none());
            assert!(unsafe { (*state_ptr).is_ring_abandoned() });
            assert!(!crate::runtime::fd::raw_fd_is_closed(fd));

            unsafe {
                (*state_ptr).restore_completed_orphaned_after_ringless_abandonment_for_test();
                Reactor::free_op_unchecked(reactor, state_ptr);
            }
            assert!(crate::runtime::fd::raw_fd_is_closed(fd));
            assert_eq!(unsafe { (&*reactor).live_op_count() }, 0);
            assert_eq!(owner.inflight_op_count_for_test(), 0);
        });
    }

    #[cfg(not(miri))]
    #[test]
    fn completed_connect_payload_transitions_cover_success_error_and_context_rejection() {
        #[derive(Clone, Copy)]
        enum Expected {
            Success,
            OsError(libc::c_int),
            ContextRejected,
        }

        crate::runtime::executor::with_ringless_poll_context_for_test(1, |owner, cx| {
            let reactor = owner.reactor_ptr();
            for (result, context_rejected, expected) in [
                (0, false, Expected::Success),
                (
                    -libc::ECONNREFUSED,
                    false,
                    Expected::OsError(libc::ECONNREFUSED),
                ),
                (0, true, Expected::ContextRejected),
            ] {
                let state_ptr = unsafe { (&mut *reactor).alloc_op() };
                assert!(!state_ptr.is_null(), "connect state allocation failed");
                let raw_fd = crate::runtime::fd::distinctive_closeable_test_fd()
                    .expect("connect fd creation failed");
                // SAFETY: the test-created descriptor has no other owner.
                let fd = unsafe { OwnedFd::from_raw_fd(raw_fd) };
                let retained = unsafe {
                    (&mut *reactor).alloc_retained_payload(RetainedConnectPayload::new(
                        fd,
                        RetainedConnectAddr::from_socket_addr(SocketAddr::from((
                            [127, 0, 0, 1],
                            9,
                        ))),
                    ))
                };
                unsafe {
                    (*state_ptr).attach_retained_payload(retained);
                    (*state_ptr).result = result;
                    if context_rejected {
                        (*state_ptr).set_context_rejected();
                    }
                    (*state_ptr).set_completed();
                }

                let mut slot = ConnectSubmissionSlot::new(());
                slot.state_ptr = state_ptr;
                slot.in_use = true;
                let outcome = slot.poll_connect(cx, |fd, _| Ok(fd));
                assert!(slot.state_ptr.is_null());
                assert!(!slot.in_use);
                assert!(slot.fd.is_none());
                assert!(slot.addr.is_none());

                match expected {
                    Expected::Success => {
                        let Poll::Ready(Ok(fd)) = outcome else {
                            panic!("successful connect did not transfer its exact fd owner");
                        };
                        assert_eq!(fd.as_raw_fd(), raw_fd);
                        assert!(!crate::runtime::fd::raw_fd_is_closed(raw_fd));
                        drop(fd);
                    }
                    Expected::OsError(errno) => {
                        let Poll::Ready(Err(err)) = outcome else {
                            panic!("failed connect did not return its CQE error");
                        };
                        assert_eq!(err.raw_os_error(), Some(errno));
                    }
                    Expected::ContextRejected => {
                        let Poll::Ready(Err(err)) = outcome else {
                            panic!("context-rejected connect unexpectedly succeeded");
                        };
                        assert_eq!(err.kind(), io::ErrorKind::NotConnected);
                    }
                }
                assert!(crate::runtime::fd::raw_fd_is_closed(raw_fd));
                assert_eq!(unsafe { (&*reactor).live_op_count() }, 0);
                assert_eq!(owner.inflight_op_count_for_test(), 0);
            }
        });
    }

    #[test]
    fn connect_timeout_mapping_preserves_each_result_class() {
        assert_eq!(
            map_connect_timeout::<u8>(Ok(Ok(7))).expect("successful connect should pass through"),
            7
        );

        let connect_error =
            map_connect_timeout::<u8>(Ok(Err(io::Error::from_raw_os_error(libc::ECONNREFUSED))))
                .expect_err("connect error should pass through");
        assert_eq!(connect_error.raw_os_error(), Some(libc::ECONNREFUSED));

        let elapsed = map_connect_timeout::<u8>(Err(TimeoutError::Elapsed))
            .expect_err("elapsed timeout should fail");
        assert_eq!(elapsed.kind(), io::ErrorKind::TimedOut);
        assert!(
            elapsed.get_ref().is_none(),
            "elapsed timeout should retain the message-free error contract"
        );

        let runtime_error = map_connect_timeout::<u8>(Err(TimeoutError::Runtime(
            io::Error::from_raw_os_error(libc::ENOMEM),
        )))
        .expect_err("timer runtime error should pass through");
        assert_eq!(runtime_error.raw_os_error(), Some(libc::ENOMEM));
    }

    #[test]
    fn accept_readiness_classifier_bounds_bare_error_and_latches_unrecoverable_bits() {
        assert_eq!(
            std::mem::size_of::<AcceptReadinessSlot>(),
            3 * std::mem::size_of::<usize>(),
            "carried readiness and terminal state must retain three-word slot geometry"
        );
        assert_eq!(
            std::mem::size_of::<AcceptReadinessState>(),
            1,
            "accept readiness policy must remain one byte"
        );

        let would_block = io::Error::from(io::ErrorKind::WouldBlock);
        assert_eq!(
            accept_failure_disposition(libc::POLLIN as i32, &would_block, 0),
            AcceptFailureDisposition::Rearm
        );

        for readiness in [libc::POLLERR as i32, (libc::POLLIN | libc::POLLERR) as i32] {
            assert!(!accept_readiness_is_unrecoverable(readiness));
            assert_eq!(
                accept_failure_disposition(readiness, &would_block, 0),
                AcceptFailureDisposition::RearmBarePollError
            );
            assert_eq!(
                accept_failure_disposition(readiness, &would_block, BARE_POLLERR_REARM_LIMIT),
                AcceptFailureDisposition::Propagate
            );
        }

        for readiness in [
            libc::POLLHUP as i32,
            libc::POLLNVAL as i32,
            (libc::POLLERR | libc::POLLHUP) as i32,
            (libc::POLLIN | libc::POLLHUP) as i32,
            (libc::POLLIN | libc::POLLNVAL) as i32,
            (libc::POLLERR | libc::POLLHUP | libc::POLLNVAL) as i32,
        ] {
            assert!(accept_readiness_is_unrecoverable(readiness));
            for bare_poll_error_rearms in [0, BARE_POLLERR_REARM_LIMIT] {
                assert_eq!(
                    accept_failure_disposition(readiness, &would_block, bare_poll_error_rearms),
                    AcceptFailureDisposition::LatchTerminal
                );
            }
        }

        let bad_fd = io::Error::from_raw_os_error(libc::EBADF);
        assert_eq!(
            accept_failure_disposition(libc::POLLNVAL as i32, &bad_fd, 0),
            AcceptFailureDisposition::LatchAndPropagate
        );
        assert_eq!(bad_fd.raw_os_error(), Some(libc::EBADF));

        for raw_error in [libc::ECONNABORTED, libc::ENETDOWN] {
            let transient = io::Error::from_raw_os_error(raw_error);
            assert_eq!(
                accept_failure_disposition(libc::POLLERR as i32, &transient, 0),
                AcceptFailureDisposition::Propagate
            );
            assert_eq!(transient.raw_os_error(), Some(raw_error));
        }

        for raw_error in [libc::EMFILE, libc::ENFILE] {
            let exhausted = io::Error::from_raw_os_error(raw_error);
            for readiness in [libc::POLLIN as i32, libc::POLLERR as i32] {
                assert_eq!(
                    accept_failure_disposition(readiness, &exhausted, 0),
                    AcceptFailureDisposition::PreserveReadiness
                );
            }
            assert_eq!(exhausted.raw_os_error(), Some(raw_error));
        }

        let refused = io::Error::from_raw_os_error(libc::ECONNREFUSED);
        assert_eq!(
            accept_failure_disposition(libc::POLLIN as i32, &refused, 0),
            AcceptFailureDisposition::Propagate
        );
        assert_eq!(refused.raw_os_error(), Some(libc::ECONNREFUSED));
        let terminal = terminal_accept_error();
        assert_eq!(terminal.kind(), io::ErrorKind::ConnectionAborted);
        assert_eq!(terminal.raw_os_error(), None);
    }

    #[test]
    fn accept_slot_bare_poll_error_budget_survives_ordinary_rearm() {
        let listener_fd = RuntimeFd::from_fresh_raw_fd(-1);
        let mut slot = AcceptReadinessSlot::new(&listener_fd);
        let would_block = io::Error::from_raw_os_error(libc::EAGAIN);

        slot.record_rearm(true);
        assert_eq!(
            slot.readiness_state,
            AcceptReadinessState::BarePollErrorRearmed
        );
        assert_eq!(
            accept_failure_disposition(
                libc::POLLIN as i32,
                &would_block,
                slot.bare_poll_error_rearms()
            ),
            AcceptFailureDisposition::Rearm
        );

        slot.record_rearm(false);
        assert_eq!(
            slot.readiness_state,
            AcceptReadinessState::BarePollErrorRearmed,
            "ordinary stale readiness replenished the bare POLLERR budget"
        );
        assert_eq!(
            accept_failure_disposition(
                libc::POLLERR as i32,
                &would_block,
                slot.bare_poll_error_rearms()
            ),
            AcceptFailureDisposition::Propagate,
            "POLLERR/POLLIN/POLLERR exceeded the per-future rearm budget"
        );
    }

    #[test]
    fn failed_accept_rearms_publish_no_state_or_submission_accounting() {
        for readiness in [
            libc::POLLIN as libc::c_short,
            libc::POLLERR as libc::c_short,
        ] {
            for fail_submission in [false, true] {
                crate::runtime::executor::with_ringless_poll_context_for_test(1, |owner, cx| {
                    let reactor = owner.reactor_ptr();
                    let listener_keepalive = RuntimeFd::from_fresh_raw_fd(-1);
                    let mut slot = AcceptReadinessSlot::new(&listener_keepalive);
                    slot.prepare()
                        .expect("fresh accept slot should prepare for fault injection");
                    slot.unconsumed_readiness = readiness;

                    if fail_submission {
                        crate::runtime::test_hooks::fail_next_sqe_submit();
                    } else {
                        crate::runtime::test_hooks::fail_next_op_alloc();
                    }

                    let accept_calls = Cell::new(0_usize);
                    let outcome: Poll<io::Result<()>> = slot.poll_accept_with(
                        true,
                        cx,
                        |_fd, _reassert_nonblocking| {
                            accept_calls.set(accept_calls.get() + 1);
                            Err(io::Error::from_raw_os_error(libc::EAGAIN))
                        },
                        |_accepted, _provenance, _addr, _addrlen| {
                            panic!("faulted empty-listener accept unexpectedly succeeded")
                        },
                    );

                    assert!(
                        matches!(
                            outcome,
                            Poll::Ready(Err(ref err))
                                if err.kind() == io::ErrorKind::WouldBlock
                        ),
                        "failed rearm did not return its pressure error"
                    );
                    assert_eq!(accept_calls.get(), 1);
                    assert!(slot.state_ptr.is_null());
                    assert!(!slot.in_use);
                    assert_eq!(
                        slot.unconsumed_readiness, NO_UNCONSUMED_ACCEPT_READINESS,
                        "stale readiness was incorrectly restored after rearm failure"
                    );
                    assert_eq!(slot.readiness_state, AcceptReadinessState::Ready);
                    assert_eq!(slot.bare_poll_error_rearms(), 0);
                    assert!(!slot.is_terminal());
                    assert_eq!(unsafe { (&*reactor).live_op_count() }, 0);
                    assert_eq!(owner.inflight_op_count_for_test(), 0);
                    assert_eq!(
                        listener_keepalive.strong_count_for_test(),
                        2,
                        "failed rearm leaked or released retained listener ownership"
                    );
                    assert_eq!(
                        crate::runtime::test_hooks::raw_sqe_submit_failures_remaining(),
                        0,
                        "failed rearm did not consume the armed submission fault"
                    );

                    #[cfg(debug_assertions)]
                    {
                        let pctx = poll_ctx_from_waker(cx)
                            .expect("fault regression lost its FlowIO poll context");
                        let stats = unsafe { (*pctx.runtime_state()).stats };
                        assert_eq!(stats.accept_readiness_rearms, 0);
                        assert_eq!(stats.sqe_submits, 0);
                    }

                    let replacement = unsafe { (&mut *reactor).alloc_op() };
                    assert!(
                        !replacement.is_null(),
                        "failed rearm did not restore operation-slot usability"
                    );
                    unsafe { Reactor::free_op_unchecked(reactor, replacement) };
                    assert_eq!(unsafe { (&*reactor).live_op_count() }, 0);

                    slot.prepare()
                        .expect("failed rearm did not restore accept-slot usability");
                    slot.drop_future();
                    assert!(slot.state_ptr.is_null());
                    assert!(!slot.in_use);
                    assert_eq!(slot.readiness_state, AcceptReadinessState::Ready);

                    drop(slot);
                    assert_eq!(listener_keepalive.strong_count_for_test(), 1);
                });
            }
        }
    }

    #[cfg(not(miri))]
    #[test]
    fn accept_slot_rearms_bare_poll_error_once_and_resets_on_cancel() {
        let listener = std::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0))
            .expect("listener bind failed");
        listener
            .set_nonblocking(true)
            .expect("listener nonblocking setup failed");
        let listener_keepalive = RuntimeFd::from_fresh_owned(OwnedFd::from(listener));
        let mut slot = AcceptReadinessSlot::new(&listener_keepalive);
        let mut executor =
            crate::runtime::executor::Executor::new().expect("executor construction failed");

        executor
            .run(async move {
                slot.prepare().expect("accept slot preparation failed");
                slot.unconsumed_readiness = libc::POLLERR as libc::c_short;

                let outcome = std::future::poll_fn(|cx| {
                    Poll::Ready(slot.poll_accept_with(
                        true,
                        cx,
                        |_fd, _uncertain_linger| Err(io::Error::from_raw_os_error(libc::EAGAIN)),
                        |_accepted, _provenance, _addr, _addrlen| Ok(()),
                    ))
                })
                .await;
                assert!(
                    matches!(outcome, Poll::Pending),
                    "first bare POLLERR did not rearm"
                );
                assert!(!slot.state_ptr.is_null());
                assert!(slot.in_use);
                assert_eq!(
                    slot.readiness_state,
                    AcceptReadinessState::BarePollErrorRearmed
                );
                assert!(!slot.is_terminal());

                slot.drop_future();
                assert!(slot.state_ptr.is_null());
                assert!(!slot.in_use);
                assert_eq!(slot.readiness_state, AcceptReadinessState::Ready);
                assert!(!slot.is_terminal());
            })
            .expect("bare POLLERR rearm run failed");

        #[cfg(debug_assertions)]
        assert_eq!(executor.last_stats().accept_readiness_rearms, 1);
        drop(listener_keepalive);
    }

    #[cfg(not(miri))]
    #[test]
    fn accept_descriptor_exhaustion_preserves_readiness_for_direct_caller_retries() {
        let listener = std::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0))
            .expect("listener bind failed");
        listener
            .set_nonblocking(true)
            .expect("listener nonblocking setup failed");
        let listener_addr = listener.local_addr().expect("listener address missing");
        let peer =
            std::net::TcpStream::connect_timeout(&listener_addr, std::time::Duration::from_secs(1))
                .expect("peer connect failed");
        let expected_peer = peer.local_addr().expect("peer local address missing");
        let listener_raw = std::os::fd::AsRawFd::as_raw_fd(&listener);

        let listener_fd = RuntimeFd::from_fresh_owned(OwnedFd::from(listener));
        let listener_weak = listener_fd.weak_for_test();
        let listener_keepalive = listener_fd.clone_handle();
        let mut slot = AcceptReadinessSlot::new(&listener_fd);
        drop(listener_fd);
        let mut executor =
            crate::runtime::executor::Executor::new().expect("executor construction failed");
        let exhausted_errnos = [
            libc::EMFILE,
            libc::ENFILE,
            libc::EMFILE,
            libc::ENFILE,
            libc::EMFILE,
            libc::ENFILE,
        ];

        executor
            .run(async move {
                let reactor = std::future::poll_fn(|cx| {
                    let pctx = poll_ctx_from_waker(cx)
                        .expect("descriptor-exhaustion test requires a FlowIO context");
                    Poll::Ready(pctx.reactor())
                })
                .await;
                slot.prepare().expect("initial accept preparation failed");
                slot.state_ptr = std::future::poll_fn(|cx| {
                    Poll::Ready(completed_accept_readiness_for_test(
                        cx,
                        &slot.listener_fd,
                        libc::POLLIN as i32,
                    ))
                })
                .await;

                let accept_calls = std::cell::Cell::new(0_usize);
                for (attempt, raw_error) in exhausted_errnos.into_iter().enumerate() {
                    if attempt != 0 {
                        slot.prepare()
                            .expect("cached readiness should admit the next caller");
                    }

                    let calls_before = accept_calls.get();
                    let outcome: Poll<io::Result<()>> = std::future::poll_fn(|cx| {
                        Poll::Ready(slot.poll_accept_with(
                            true,
                            cx,
                            |fd, reassert_nonblocking| {
                                assert_eq!(fd, listener_raw);
                                assert!(
                                    !reassert_nonblocking,
                                    "fresh listener unexpectedly reasserted O_NONBLOCK"
                                );
                                assert_eq!(
                                    listener_weak.strong_count(),
                                    if attempt == 0 { 3 } else { 2 },
                                    "completed readiness must retain its listener through accept4"
                                );
                                accept_calls.set(accept_calls.get() + 1);
                                Err(io::Error::from_raw_os_error(raw_error))
                            },
                            |_accepted, _provenance, _addr, _addrlen| {
                                panic!("descriptor-exhausted accept unexpectedly succeeded")
                            },
                        ))
                    })
                    .await;

                    let Poll::Ready(Err(err)) = outcome else {
                        panic!("descriptor-exhausted accept did not return its exact error");
                    };
                    assert_eq!(err.raw_os_error(), Some(raw_error));
                    assert_eq!(
                        accept_calls.get(),
                        calls_before + 1,
                        "one caller retry must perform exactly one accept4 attempt"
                    );
                    assert!(slot.state_ptr.is_null());
                    assert_eq!(slot.unconsumed_readiness, libc::POLLIN as libc::c_short);
                    assert!(!slot.in_use);
                    assert!(!slot.is_terminal());
                    assert_eq!(unsafe { (&*reactor).live_op_count() }, 0);
                }

                slot.prepare()
                    .expect("cached readiness should admit a context check");
                let calls_before_rejection = accept_calls.get();
                let mut rejected_cx = Context::from_waker(std::task::Waker::noop());
                let rejected: Poll<io::Result<()>> = slot.poll_accept_with(
                    true,
                    &mut rejected_cx,
                    |_fd, _reassert_nonblocking| {
                        accept_calls.set(accept_calls.get() + 1);
                        panic!("invalid context reached accept4")
                    },
                    |_accepted, _provenance, _addr, _addrlen| {
                        panic!("invalid context completed accept")
                    },
                );
                let Poll::Ready(Err(err)) = rejected else {
                    panic!("invalid context did not reject cached readiness");
                };
                assert_eq!(err.kind(), io::ErrorKind::NotConnected);
                assert_eq!(accept_calls.get(), calls_before_rejection);
                assert_eq!(slot.unconsumed_readiness, libc::POLLIN as libc::c_short);
                assert!(slot.state_ptr.is_null());
                assert!(!slot.in_use);

                slot.prepare()
                    .expect("cached readiness should remain independently cancellable");
                slot.drop_future();
                assert_eq!(
                    slot.unconsumed_readiness,
                    libc::POLLIN as libc::c_short,
                    "dropping an unpolled retry must preserve unconsumed readiness"
                );
                assert!(slot.state_ptr.is_null());
                assert!(!slot.in_use);

                slot.prepare()
                    .expect("cached readiness should admit the successful retry");
                let calls_before_success = accept_calls.get();
                let outcome: Poll<io::Result<(OwnedFd, SocketAddr)>> = std::future::poll_fn(|cx| {
                    Poll::Ready(slot.poll_accept(
                        true,
                        cx,
                        |accepted, _provenance, addr, addrlen| {
                            assert_eq!(
                                listener_weak.strong_count(),
                                2,
                                "cached accept must not retain an operation payload"
                            );
                            accept_calls.set(accept_calls.get() + 1);
                            Ok((accepted, socket_addr_from_c(addr, addrlen)?))
                        },
                    ))
                })
                .await;
                let Poll::Ready(Ok((accepted, accepted_peer))) = outcome else {
                    panic!("cached readiness did not accept the queued peer directly");
                };
                assert_eq!(accepted_peer, expected_peer);
                assert_eq!(
                    accept_calls.get(),
                    calls_before_success + 1,
                    "successful caller retry must perform exactly one accept4 attempt"
                );
                drop(accepted);
                assert!(slot.state_ptr.is_null());
                assert_eq!(slot.unconsumed_readiness, NO_UNCONSUMED_ACCEPT_READINESS);
                assert!(!slot.in_use);
                assert!(!slot.is_terminal());
                assert_eq!(unsafe { (&*reactor).live_op_count() }, 0);
            })
            .expect("descriptor-exhaustion accept run failed");

        #[cfg(debug_assertions)]
        {
            let stats = executor.last_stats();
            assert_eq!(stats.accept_descriptor_exhaustions, exhausted_errnos.len());
            assert_eq!(stats.accept_readiness_rearms, 0);
            assert_eq!(stats.sqe_submits, 0);
        }
        drop(peer);

        let stale_peer =
            std::net::TcpStream::connect_timeout(&listener_addr, std::time::Duration::from_secs(1))
                .expect("stale-readiness peer connect failed");
        let expected_stale_peer = stale_peer
            .local_addr()
            .expect("stale-readiness peer local address missing");
        let stale_listener = listener_keepalive.clone_handle();
        let mut stale_executor =
            crate::runtime::executor::Executor::new().expect("stale executor construction failed");

        stale_executor
            .run(async move {
                let mut slot = AcceptReadinessSlot::new(&stale_listener);
                drop(stale_listener);
                slot.unconsumed_readiness = libc::POLLIN as libc::c_short;
                slot.prepare()
                    .expect("cached stale-readiness accept preparation failed");

                let accept_calls = std::cell::Cell::new(0_usize);
                let (accepted, accepted_peer) = std::future::poll_fn(|cx| {
                    slot.poll_accept_with(
                        true,
                        cx,
                        |fd, reassert_nonblocking| {
                            let attempt = accept_calls.get();
                            accept_calls.set(attempt + 1);
                            if attempt == 0 {
                                return Err(io::Error::from(io::ErrorKind::WouldBlock));
                            }
                            accept_nonblocking(fd, reassert_nonblocking)
                        },
                        |accepted, _provenance, addr, addrlen| {
                            Ok((accepted, socket_addr_from_c(addr, addrlen)?))
                        },
                    )
                })
                .await
                .expect("cached stale readiness did not recover after rearm");

                assert_eq!(accepted_peer, expected_stale_peer);
                assert_eq!(
                    accept_calls.get(),
                    2,
                    "cached stale readiness must attempt directly, then once after rearm"
                );
                assert!(slot.state_ptr.is_null());
                assert_eq!(
                    slot.unconsumed_readiness, NO_UNCONSUMED_ACCEPT_READINESS,
                    "successful post-rearm accept must consume cached readiness"
                );
                assert!(!slot.in_use);
                assert!(!slot.is_terminal());
                drop(accepted);
            })
            .expect("cached stale-readiness run failed");

        #[cfg(debug_assertions)]
        {
            let stats = stale_executor.last_stats();
            assert_eq!(stats.accept_descriptor_exhaustions, 0);
            assert_eq!(stats.accept_readiness_rearms, 1);
            assert_eq!(stats.sqe_submits, 1);
        }
        drop(stale_peer);
        drop(listener_keepalive);
    }

    #[cfg(not(miri))]
    #[test]
    fn pollnval_ebadf_accept_readiness_latches_after_preserving_the_first_errno() {
        crate::runtime::executor::with_ringless_poll_context_for_test(1, |_owner, cx| {
            let listener_fd = RuntimeFd::from_fresh_raw_fd(-1);
            let mut slot = AcceptReadinessSlot::new(&listener_fd);
            slot.prepare().expect("fresh accept slot should prepare");
            slot.state_ptr =
                completed_accept_readiness_for_test(cx, &slot.listener_fd, libc::POLLNVAL as i32);

            let outcome: Poll<io::Result<()>> =
                slot.poll_accept(true, cx, |_accepted, _provenance, _addr, _addrlen| {
                    panic!("invalid listener unexpectedly accepted a peer")
                });
            let Poll::Ready(Err(err)) = outcome else {
                panic!("POLLNVAL did not complete the invalid listener accept");
            };
            assert_eq!(err.raw_os_error(), Some(libc::EBADF));
            assert!(slot.state_ptr.is_null());
            assert!(!slot.in_use);
            assert!(slot.is_terminal());

            let later = slot
                .prepare()
                .expect_err("invalid listener should remain terminal");
            assert_eq!(later.kind(), io::ErrorKind::ConnectionAborted);
            assert_eq!(later.raw_os_error(), None);
            assert!(slot.state_ptr.is_null());
            assert!(!slot.in_use);
            assert!(slot.is_terminal());
        });
    }

    #[cfg(not(miri))]
    #[test]
    fn shutdown_socket_maps_all_modes_and_preserves_errno() {
        for how in [
            std::net::Shutdown::Read,
            std::net::Shutdown::Write,
            std::net::Shutdown::Both,
        ] {
            let (stream, _peer) =
                std::os::unix::net::UnixStream::pair().expect("socketpair failed");
            shutdown_socket(std::os::fd::AsRawFd::as_raw_fd(&stream), how)
                .expect("valid stream shutdown failed");
        }

        let err = shutdown_socket(-1, std::net::Shutdown::Both)
            .expect_err("invalid descriptor shutdown should fail");
        assert_eq!(err.raw_os_error(), Some(libc::EBADF));
    }

    #[test]
    fn accept_readiness_slot_prepare_drop_and_reuse_match_transport_contracts() {
        let raw = crate::runtime::fd::distinctive_closeable_test_fd()
            .expect("distinctive listener fd failed");
        let listener = RuntimeFd::from_fresh_raw_fd(raw);
        let mut slot = AcceptReadinessSlot::new(&listener);

        slot.prepare().expect("first prepare should claim the slot");
        let err = slot
            .prepare()
            .expect_err("a borrowed accept slot should report pressure");
        assert_eq!(err.kind(), io::ErrorKind::WouldBlock);
        slot.drop_future();
        assert!(slot.state_ptr.is_null());
        assert!(!slot.in_use);

        slot.prepare().expect("dropped slot should be reusable");
        let mut abandoned = CompletionState::empty();
        abandoned.set_ring_abandoned();
        slot.state_ptr = &mut abandoned;
        slot.drop_cached_state();
        assert!(slot.state_ptr.is_null());
        assert!(!slot.in_use);
        assert!(abandoned.is_ring_abandoned());

        slot.unconsumed_readiness = libc::POLLIN as libc::c_short;
        slot.in_use = true;
        slot.drop_cached_state();
        assert_eq!(
            slot.unconsumed_readiness, NO_UNCONSUMED_ACCEPT_READINESS,
            "listener teardown must discard userspace-only cached readiness"
        );
        assert!(!slot.in_use);

        slot.prepare()
            .expect("cached-state teardown should leave the slot reusable");
        slot.drop_future();
        drop(slot);
        assert!(
            !crate::runtime::fd::raw_fd_is_closed(raw),
            "slot drop must preserve the listener's owning reference"
        );
        drop(listener);
        assert!(
            crate::runtime::fd::raw_fd_is_closed(raw),
            "last listener owner must close the descriptor"
        );
    }

    #[cfg(not(miri))]
    #[test]
    fn queued_connection_wins_mixed_terminal_accept_readiness() {
        for readiness in [
            (libc::POLLIN | libc::POLLERR) as i32,
            (libc::POLLIN | libc::POLLHUP) as i32,
        ] {
            let listener = std::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0))
                .expect("listener bind failed");
            listener
                .set_nonblocking(true)
                .expect("listener nonblocking setup failed");
            let listener_addr = listener.local_addr().expect("listener address missing");
            let peer = std::net::TcpStream::connect_timeout(
                &listener_addr,
                std::time::Duration::from_secs(1),
            )
            .expect("peer connect failed");
            let expected_peer = peer.local_addr().expect("peer local address missing");

            let listener_keepalive = RuntimeFd::from_fresh_owned(OwnedFd::from(listener));
            let mut slot = AcceptReadinessSlot::new(&listener_keepalive);
            let mut executor =
                crate::runtime::executor::Executor::new().expect("executor construction failed");

            executor
                .run(async move {
                    slot.prepare().expect("accept slot preparation failed");
                    let state_ptr = std::future::poll_fn(|cx| {
                        Poll::Ready(completed_accept_readiness_for_test(
                            cx,
                            &slot.listener_fd,
                            readiness,
                        ))
                    })
                    .await;
                    slot.state_ptr = state_ptr;

                    let outcome = std::future::poll_fn(|cx| {
                        Poll::Ready(slot.poll_accept(
                            true,
                            cx,
                            |accepted, _provenance, addr, addrlen| {
                                Ok((accepted, socket_addr_from_c(addr, addrlen)?))
                            },
                        ))
                    })
                    .await;
                    let Poll::Ready(Ok((accepted, accepted_peer))) = outcome else {
                        panic!("queued connection did not win mixed readiness");
                    };
                    assert_eq!(accepted_peer, expected_peer);
                    drop(accepted);
                    assert!(slot.state_ptr.is_null());
                    assert!(!slot.in_use);
                    assert!(
                        !slot.is_terminal(),
                        "successful accept incorrectly latched terminal readiness"
                    );
                })
                .expect("queued-peer accept run failed");

            drop(listener_keepalive);
            drop(peer);
        }
    }

    #[test]
    fn socket_addr_v6_c_layout_and_round_trip_preserve_all_fields() {
        let ip = std::net::Ipv6Addr::new(
            0x2001, 0x0db8, 0x1234, 0x5678, 0x90ab, 0xcdef, 0x1020, 0x3040,
        );
        let addr = SocketAddr::V6(SocketAddrV6::new(ip, 0x1234, 0x0102_0304, 0x0506_0708));

        let (storage, len) = socket_addr_to_c(addr);
        assert_eq!(
            len,
            std::mem::size_of::<libc::sockaddr_in6>() as libc::socklen_t
        );

        let raw = unsafe {
            std::ptr::read_unaligned(
                (&storage as *const libc::sockaddr_storage).cast::<libc::sockaddr_in6>(),
            )
        };
        assert_eq!(raw.sin6_family, libc::AF_INET6 as libc::sa_family_t);
        assert_eq!(raw.sin6_port, 0x1234u16.to_be());
        assert_eq!(raw.sin6_addr.s6_addr, ip.octets());
        assert_eq!(raw.sin6_flowinfo, 0x0102_0304);
        assert_eq!(raw.sin6_scope_id, 0x0506_0708);
        assert_eq!(
            socket_addr_from_c(&storage, len).expect("IPv6 sockaddr should decode"),
            addr
        );
    }

    #[test]
    fn retained_connect_address_owns_exact_ipv4_and_ipv6_storage() {
        let address_size = std::mem::size_of::<RetainedConnectAddr>();
        let payload_size = std::mem::size_of::<RetainedConnectPayload>();
        assert!(address_size > 128 && address_size <= 256);
        assert!(payload_size > 128 && payload_size <= 256);
        assert!(std::mem::align_of::<RetainedConnectPayload>() <= 64);

        let addresses = [
            SocketAddr::from((std::net::Ipv4Addr::new(192, 0, 2, 17), 0x1234)),
            SocketAddr::V6(SocketAddrV6::new(
                std::net::Ipv6Addr::new(
                    0x2001, 0x0db8, 0x1234, 0x5678, 0x90ab, 0xcdef, 0x1020, 0x3040,
                ),
                0x5678,
                0x0102_0304,
                0x0506_0708,
            )),
        ];

        for address in addresses {
            let retained = RetainedConnectAddr::from_socket_addr(address);
            let expected_len = match address {
                SocketAddr::V4(_) => std::mem::size_of::<libc::sockaddr_in>(),
                SocketAddr::V6(_) => std::mem::size_of::<libc::sockaddr_in6>(),
            } as libc::socklen_t;

            assert_eq!(retained.addrlen, expected_len);
            assert_eq!(
                retained.addr_ptr().cast::<libc::sockaddr_storage>(),
                std::ptr::addr_of!(retained.addr)
            );
            assert_eq!(
                socket_addr_from_c(&retained.addr, retained.addrlen)
                    .expect("retained connect address should decode"),
                address
            );
        }
    }

    #[test]
    fn socket_addr_from_c_rejects_malformed_ipv6_lengths_and_families() {
        let addr = SocketAddr::V6(SocketAddrV6::new(
            std::net::Ipv6Addr::LOCALHOST,
            0x1234,
            0x0102_0304,
            0x0506_0708,
        ));
        let (storage, _) = socket_addr_to_c(addr);
        let family_len = std::mem::size_of::<libc::sa_family_t>() as libc::socklen_t;
        let sockaddr_in6_len = std::mem::size_of::<libc::sockaddr_in6>() as libc::socklen_t;

        for (case, len) in [
            ("zero length", 0),
            ("short family", family_len - 1),
            ("short IPv6 address", sockaddr_in6_len - 1),
        ] {
            let err = socket_addr_from_c(&storage, len).expect_err(case);
            assert_eq!(err.kind(), io::ErrorKind::InvalidData, "{case}");
        }

        let (mut zero_family, _) = socket_addr_to_c(addr);
        zero_family.ss_family = 0;
        let err = socket_addr_from_c(&zero_family, sockaddr_in6_len)
            .expect_err("zero family should fail");
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);

        let (mut unknown_family, _) = socket_addr_to_c(addr);
        unknown_family.ss_family = libc::sa_family_t::MAX;
        let err = socket_addr_from_c(&unknown_family, sockaddr_in6_len)
            .expect_err("unknown family should fail");
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }

    #[cfg(not(miri))]
    #[test]
    fn nonblocking_accept_reasserts_listener_mode_then_returns_cloexec_socket() {
        use std::net::{Ipv4Addr, TcpListener, TcpStream};
        use std::os::fd::AsRawFd;

        let listener =
            TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).expect("std listener bind should succeed");

        let err = accept_nonblocking(listener.as_raw_fd(), true)
            .expect_err("empty listener should not accept");
        assert_eq!(err.kind(), io::ErrorKind::WouldBlock);

        let peer = TcpStream::connect(listener.local_addr().expect("listener address missing"))
            .expect("std client connect should succeed");
        let peer_addr = peer.local_addr().expect("client local address missing");
        let (accepted, addr, addrlen) =
            accept_nonblocking(listener.as_raw_fd(), true).expect("ready listener should accept");

        let status = unsafe { libc::fcntl(accepted.as_raw_fd(), libc::F_GETFL) };
        assert!(
            status >= 0,
            "F_GETFL failed: {}",
            io::Error::last_os_error()
        );
        assert_ne!(status & libc::O_NONBLOCK, 0);

        let fd_flags = unsafe { libc::fcntl(accepted.as_raw_fd(), libc::F_GETFD) };
        assert!(
            fd_flags >= 0,
            "F_GETFD failed: {}",
            io::Error::last_os_error()
        );
        assert_ne!(fd_flags & libc::FD_CLOEXEC, 0);
        assert_eq!(
            socket_addr_from_c(&addr, addrlen).expect("accepted address should decode"),
            peer_addr
        );
    }

    #[test]
    fn checked_read_len_rejects_over_writable_with_static_message() {
        let err = checked_read_len(2, 1).expect_err("oversize read should fail");
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
        assert_eq!(err.to_string(), READ_LEN_EXCEEDS_WRITABLE);
    }

    #[test]
    fn checked_lengths_reject_u32_overflow_with_static_message() {
        let oversized = u32::MAX as usize + 1;

        assert_eq!(
            send_len_u32(u32::MAX as usize),
            Some(u32::MAX),
            "the exact io_uring byte-count limit should remain valid"
        );
        assert_eq!(
            send_len_u32(oversized),
            None,
            "the first oversized byte count should be rejected"
        );

        let read_err =
            checked_read_len(oversized, usize::MAX).expect_err("oversize read should fail");
        assert_eq!(read_err.kind(), io::ErrorKind::InvalidInput);
        assert_eq!(read_err.to_string(), LEN_EXCEEDS_U32);

        let send_err = checked_send_len(oversized).expect_err("oversize send should fail");
        assert_eq!(send_err.kind(), io::ErrorKind::InvalidInput);
        assert_eq!(send_err.to_string(), LEN_EXCEEDS_U32);
    }

    #[test]
    fn msghdr_init_writes_all_kernel_fields() {
        let mut hdr = MaybeUninit::uninit();
        let mut name = 0u8;
        let mut byte = 0u8;
        let mut iovec = libc::iovec {
            iov_base: &mut byte as *mut u8 as *mut libc::c_void,
            iov_len: 1,
        };
        let mut control = [0u8; 8];
        let name_ptr = &mut name as *mut u8 as *mut libc::c_void;
        let iov_ptr = &mut iovec as *mut libc::iovec;
        let control_ptr = control.as_mut_ptr() as *mut libc::c_void;

        write_msghdr(
            &mut hdr,
            MsgHdrInit {
                name: name_ptr,
                namelen: 7,
                iov: iov_ptr,
                iovlen: 1,
                control: control_ptr,
                controllen: control.len(),
            },
        );

        let hdr = unsafe { hdr.assume_init() };
        assert_eq!(hdr.msg_name, name_ptr);
        assert_eq!(hdr.msg_namelen, 7);
        assert_eq!(hdr.msg_iov, iov_ptr);
        assert_eq!(hdr.msg_iovlen, 1);
        assert_eq!(hdr.msg_control, control_ptr);
        assert_eq!(hdr.msg_controllen, control.len());
        assert_eq!(hdr.msg_flags, 0);
    }
}
