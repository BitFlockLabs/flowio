//! UDP transport for connected and unconnected datagram flows.
//!
//! All send/recv operations are single-datagram and use the rental pattern —
//! the caller-provided buffer is consumed and returned alongside the result on
//! completion.  Any type implementing [`IoBuffReadOnly`] / [`IoBuffReadWrite`] can be used
//! (`Vec<u8>`, `Box<[u8]>`, etc.).
//!
//! # Fast-Path Guidance
//!
//! Preferred on the per-datagram fast path:
//! - If the peer is stable, call [`UdpSocket::connect`] once and then use
//!   [`UdpSocket::send`] / [`UdpSocket::recv`]. That is the fixed-peer UDP
//!   fast path in this crate. Connected [`UdpSocket::recv`] uses the kernel
//!   `recv` operation, which does not report datagram truncation flags; callers
//!   that must reject oversized connected datagrams should use
//!   [`UdpSocket::recv_msg`].
//! - For fixed-shape datagram buffers on the hot path, prefer
//!   [`crate::runtime::buffer::pool::IoBuffPool`] plus
//!   [`crate::runtime::buffer::IoBuffMut`].
//!
//! Avoid on the per-datagram fast path:
//! - Avoid [`UdpSocket::send_to`] / [`UdpSocket::recv_from`] when
//!   the peer is stable. Use connected UDP `send` / `recv` instead.
//! - Avoid [`UdpSocket::recv`] when a too-large datagram must be detected;
//!   use connected [`UdpSocket::recv_msg`] and handle `InvalidData` instead.
//!
//! [`IoBuffReadOnly`]: crate::runtime::buffer::IoBuffReadOnly
//! [`IoBuffReadWrite`]: crate::runtime::buffer::IoBuffReadWrite
//!
//! # Example
//! ```no_run
//! use flowio::net::udp::UdpSocket;
//! use flowio::runtime::executor::Executor;
//! use std::net::{Ipv4Addr, SocketAddr};
//!
//! let mut executor = Executor::new()?;
//! executor.run(async {
//!     let mut socket = UdpSocket::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0))).unwrap();
//!     let peer = socket.local_addr().unwrap();
//!     socket.connect(peer).unwrap();
//!
//!     let (res, _buf) = socket.send(b"ping".to_vec()).await;
//!     res.unwrap();
//!
//!     let (res, buf) = socket.recv(vec![0u8; 4], 4).await;
//!     let len = res.unwrap();
//!     assert_eq!(&buf[..len], b"ping");
//! })?;
//! # Ok::<(), std::io::Error>(())
//! ```
//!
//! The same single-datagram API works with [`crate::runtime::buffer::IoBuffMut`]:
//! ```no_run
//! use flowio::net::udp::UdpSocket;
//! use flowio::runtime::buffer::pool::{IoBuffPool, IoBuffPoolConfig};
//! use flowio::runtime::executor::Executor;
//! use std::net::{Ipv4Addr, SocketAddr};
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
//!     let mut socket = UdpSocket::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0))).unwrap();
//!     let peer = socket.local_addr().unwrap();
//!     socket.connect(peer).unwrap();
//!
//!     let mut send = pool.alloc().unwrap();
//!     send.payload_append(b"ping").unwrap();
//!     let (res, _send) = socket.send(send).await;
//!     res.unwrap();
//!
//!     let recv = pool.alloc().unwrap();
//!     let (res, recv) = socket.recv(recv, 4).await;
//!     let len = res.unwrap();
//!     assert_eq!(len, 4);
//!     assert_eq!(recv.payload_bytes(), b"ping");
//! })?;
//! # Ok::<(), std::io::Error>(())
//! ```

use super::{
    CompletionTake, MsgHdrInit, checked_read_len, checked_send_len, close_fd,
    completion_cqe_result, current_local_addr, get_sock_opt, invalid_data, new_nonblocking_socket,
    set_reuse_addr, set_sock_opt, socket_addr_from_c, socket_addr_to_c, socket_domain,
    write_msghdr,
};
use crate::net::complete_read_with_progress;
use crate::runtime::buffer::{IoBuffReadOnly, IoBuffReadWrite};
use crate::runtime::executor::{
    completed_op_ctx, drop_op_ptr_unchecked, poll_ctx_from_waker, refresh_op_waiter_from_waker,
    submit_retained_sqe, validate_local_io_result,
};
use crate::runtime::fd::RuntimeFd;
use crate::runtime::op::CompletionState;
use crate::runtime::reactor::Reactor;
use io_uring::{opcode, types};
use std::future::Future;
use std::io;
use std::marker::PhantomData;
use std::mem::MaybeUninit;
use std::net::SocketAddr;
use std::os::fd::{AsRawFd, RawFd};
use std::pin::Pin;
use std::task::{Context, Poll};

/// Datagram socket with generic buffer support.
///
/// On the fast path, keep the socket open and connected to a default peer if
/// that peer is stable. Avoid rebuilding sockets or using `send_to` /
/// `recv_from` in that case; connected `send` / `recv` is the intended
/// fixed-peer fast path. Use `recv_msg` on connected sockets when the caller
/// must reject truncated datagrams.
///
/// # Example
/// ```no_run
/// use flowio::net::udp::UdpSocket;
/// use flowio::runtime::executor::Executor;
/// use std::net::{Ipv4Addr, SocketAddr};
///
/// let mut socket = UdpSocket::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))?;
/// socket.connect(SocketAddr::from((Ipv4Addr::LOCALHOST, 9000)))?;
/// let mut executor = Executor::new()?;
/// executor.run(async move {
///     let (res, _) = socket.send(b"hello".to_vec()).await;
///     res.unwrap();
/// })?;
/// # Ok::<(), std::io::Error>(())
/// ```
pub struct UdpSocket {
    /// Owned datagram socket descriptor.
    fd: RuntimeFd,
    /// Connected default peer used by `send`/`recv`, if any.
    peer_addr: Option<SocketAddr>,
}

impl UdpSocket {
    /// Binds a UDP socket to the requested local address.
    ///
    /// This enables `SO_REUSEADDR` before binding. A socket-option failure is
    /// returned before `bind(2)` is attempted.
    ///
    /// This is socket setup work. Keep the bound socket alive for steady-state
    /// datagram I/O rather than rebinding per message.
    pub fn bind(addr: SocketAddr) -> io::Result<Self> {
        let fd = new_nonblocking_socket(socket_domain(addr), libc::SOCK_DGRAM)?;

        if let Err(err) = set_reuse_addr(fd) {
            close_fd(fd);
            return Err(err);
        }

        let (sockaddr, sockaddr_len) = socket_addr_to_c(addr);
        let bind_res = unsafe {
            libc::bind(
                fd,
                &sockaddr as *const _ as *const libc::sockaddr,
                sockaddr_len,
            )
        };
        if bind_res < 0 {
            let err = io::Error::last_os_error();
            close_fd(fd);
            return Err(err);
        }

        Ok(Self {
            fd: RuntimeFd::from_fresh_raw_fd(fd),
            peer_addr: None,
        })
    }

    /// Returns the local address currently assigned to the socket.
    ///
    /// Each call queries the live descriptor with `getsockname(2)`; no local
    /// address is cached.
    ///
    /// This is socket status/control-plane lookup, not the per-datagram data
    /// fast path.
    ///
    /// # Errors
    ///
    /// Returns the operating-system error from `getsockname(2)`, or
    /// [`io::ErrorKind::InvalidData`] if the kernel returns an unsupported or
    /// malformed socket address.
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        current_local_addr(self.fd.raw_fd())
    }

    /// Returns the peer from the last successful FlowIO [`Self::connect`].
    ///
    /// This is an infallible cache lookup, not a live `getpeername(2)` query.
    /// A failed connect leaves the previous value unchanged, and raw-descriptor
    /// changes can make live kernel state disagree with this cache. This is
    /// socket status/control-plane lookup, not the per-datagram data fast path.
    /// Returns `None` until the first successful connect.
    pub fn peer_addr(&self) -> Option<SocketAddr> {
        self.peer_addr
    }

    /// Connects the socket to a default peer for `send` and `recv`.
    ///
    /// For fixed-peer UDP traffic, this enables connected send/recv operations
    /// without a per-datagram destination address.
    pub fn connect(&mut self, addr: SocketAddr) -> io::Result<()> {
        let (sockaddr, sockaddr_len) = socket_addr_to_c(addr);
        let rc = unsafe {
            libc::connect(
                self.fd.raw_fd(),
                &sockaddr as *const _ as *const libc::sockaddr,
                sockaddr_len,
            )
        };
        if rc < 0 {
            return Err(io::Error::last_os_error());
        }
        self.peer_addr = Some(addr);

        Ok(())
    }

    /// Sets the `SO_SNDBUF` socket send buffer size.
    ///
    /// This is socket configuration/control-plane work. Apply it during
    /// socket setup instead of changing it per datagram.
    pub fn set_send_buffer_size(&self, size: usize) -> io::Result<()> {
        super::set_sock_send_buffer_size(self.fd.raw_fd(), size)
    }

    /// Returns the current `SO_SNDBUF` socket send buffer size.
    ///
    /// This is socket status/control-plane lookup, not the per-datagram data
    /// fast path.
    pub fn send_buffer_size(&self) -> io::Result<usize> {
        super::sock_send_buffer_size(self.fd.raw_fd())
    }

    /// Sets the `SO_RCVBUF` socket receive buffer size.
    ///
    /// This is socket configuration/control-plane work. Apply it during
    /// socket setup instead of changing it per datagram.
    pub fn set_recv_buffer_size(&self, size: usize) -> io::Result<()> {
        super::set_sock_recv_buffer_size(self.fd.raw_fd(), size)
    }

    /// Returns the current `SO_RCVBUF` socket receive buffer size.
    ///
    /// This is socket status/control-plane lookup, not the per-datagram data
    /// fast path.
    pub fn recv_buffer_size(&self) -> io::Result<usize> {
        super::sock_recv_buffer_size(self.fd.raw_fd())
    }

    /// Enables or disables `SO_BROADCAST`.
    ///
    /// This is socket configuration/control-plane work. Apply it during
    /// socket setup instead of toggling it per datagram.
    pub fn set_broadcast(&self, broadcast: bool) -> io::Result<()> {
        set_sock_opt(
            self.fd.raw_fd(),
            libc::SOL_SOCKET,
            libc::SO_BROADCAST,
            &(broadcast as libc::c_int),
        )
    }

    /// Returns the current `SO_BROADCAST` setting.
    ///
    /// This is socket status/control-plane lookup, not the per-datagram data
    /// fast path.
    pub fn broadcast(&self) -> io::Result<bool> {
        let val: libc::c_int =
            get_sock_opt(self.fd.raw_fd(), libc::SOL_SOCKET, libc::SO_BROADCAST)?;
        Ok(val != 0)
    }

    /// Starts one connected receive into the provided buffer.
    ///
    /// This omits per-datagram peer-address handling on a connected socket.
    /// Positive progress appends to an `IoBuffMut` payload; buffers that keep
    /// the provided zero write base publish from their beginning. A zero-byte
    /// datagram preserves the existing logical contents. The returned byte
    /// count is relative to this receive.
    pub fn recv<B: IoBuffReadWrite>(&mut self, buffer: B, len: usize) -> RecvFuture<'_, B> {
        let write_base_len = buffer.write_base_len();
        let mut input_error = None;
        let len = match checked_read_len(len, buffer.writable_len()) {
            Ok(len) => len,
            Err(err) => {
                input_error = Some(err);
                0
            }
        };
        RecvFuture {
            fd: self.fd.raw_fd(),
            state_ptr: std::ptr::null_mut(),
            buffer: Some(buffer),
            len,
            write_base_len,
            input_error,
            _marker: PhantomData,
        }
    }

    /// Starts one connected `recvmsg` receive into the provided buffer.
    ///
    /// This uses the connected socket peer like [`UdpSocket::recv`], but asks
    /// the kernel for message flags and rejects payload-truncated datagrams
    /// with [`io::ErrorKind::InvalidData`]. This metadata-free API requests no
    /// ancillary control data, so discarded ancillary metadata is not part of
    /// its result and does not make a complete payload fail. Use it when a
    /// fixed-peer caller needs payload-truncation detection; use
    /// [`UdpSocket::recv`] when protocol sizing guarantees the buffer is large
    /// enough and the extra `msghdr`/`iovec` metadata is unnecessary.
    /// Positive progress follows the same relative-publication contract as
    /// [`UdpSocket::recv`]. If truncation is reported, the copied prefix is
    /// still published before `InvalidData` is returned.
    pub fn recv_msg<B: IoBuffReadWrite>(&mut self, buffer: B, len: usize) -> RecvMsgFuture<'_, B> {
        let write_base_len = buffer.write_base_len();
        let mut input_error = None;
        let len = match checked_read_len(len, buffer.writable_len()) {
            Ok(len) => len,
            Err(err) => {
                input_error = Some(err);
                0
            }
        };
        RecvMsgFuture {
            fd: self.fd.raw_fd(),
            state_ptr: std::ptr::null_mut(),
            buffer: Some(buffer),
            len,
            write_base_len,
            input_error,
            _marker: PhantomData,
        }
    }

    /// Starts one connected send from the provided buffer.
    ///
    /// This omits per-datagram destination handling on a connected socket.
    pub fn send<B: IoBuffReadOnly>(&mut self, buffer: B) -> SendFuture<'_, B> {
        let mut input_error = None;
        let len = match checked_send_len(buffer.len()) {
            Ok(len) => len,
            Err(err) => {
                input_error = Some(err);
                0
            }
        };
        SendFuture {
            fd: self.fd.raw_fd(),
            state_ptr: std::ptr::null_mut(),
            buffer: Some(buffer),
            len,
            input_error,
            _marker: PhantomData,
        }
    }

    /// Starts one unconnected receive and also returns the sending peer.
    ///
    /// Use this instead of [`UdpSocket::recv`] when peer addresses vary per
    /// datagram or when the sender address is needed by the caller.
    /// Positive progress follows the same relative-publication contract as
    /// [`UdpSocket::recv`]. Payload truncation or address-decoding errors still
    /// publish the bytes the kernel copied before the error is returned. This
    /// metadata-free API requests no ancillary control data, so discarded
    /// ancillary metadata does not make a complete payload fail.
    pub fn recv_from<B: IoBuffReadWrite>(
        &mut self,
        buffer: B,
        len: usize,
    ) -> RecvFromFuture<'_, B> {
        let write_base_len = buffer.write_base_len();
        let mut input_error = None;
        let len = match checked_read_len(len, buffer.writable_len()) {
            Ok(len) => len,
            Err(err) => {
                input_error = Some(err);
                0
            }
        };
        RecvFromFuture {
            fd: self.fd.raw_fd(),
            state_ptr: std::ptr::null_mut(),
            buffer: Some(buffer),
            len,
            write_base_len,
            input_error,
            _marker: PhantomData,
        }
    }

    /// Starts one unconnected send to the provided destination.
    ///
    /// Use this instead of [`UdpSocket::send`] when the destination varies per
    /// datagram. A source longer than the io_uring 32-bit byte-count limit is
    /// rejected before submission; an empty source remains a legal datagram
    /// and is submitted to the kernel.
    pub fn send_to<B: IoBuffReadOnly>(
        &mut self,
        buffer: B,
        addr: SocketAddr,
    ) -> SendToFuture<'_, B> {
        let mut input_error = None;
        let len = match checked_send_len(buffer.len()) {
            Ok(len) => len,
            Err(err) => {
                input_error = Some(err);
                0
            }
        };
        SendToFuture {
            fd: self.fd.raw_fd(),
            state_ptr: std::ptr::null_mut(),
            buffer: Some(buffer),
            len,
            addr,
            input_error,
            _marker: PhantomData,
        }
    }
}

impl AsRawFd for UdpSocket {
    fn as_raw_fd(&self) -> RawFd {
        self.fd.expose_raw_fd()
    }
}

// ---------------------------------------------------------------------------
// Option helpers — avoid expect()/unwrap() in fast-path code.
use super::opt_take;

struct RetainedRecvPayload<B: IoBuffReadWrite> {
    /// Caller-owned destination buffer retained while connected recv is live.
    buffer: B,
}

// The retained recvmsg/sendmsg payloads become self-referential after their
// msghdr points at embedded iovec fields and, where needed, embedded sockaddr
// storage. Initialize those pointers only after submit_retained_sqe has moved
// the payload to its stable retained address.
struct RetainedRecvMsgPayload<B: IoBuffReadWrite> {
    /// Caller-owned destination buffer retained while connected recvmsg is live.
    buffer: B,
    /// Single kernel-facing iovec pointing into `buffer`.
    iovec: MaybeUninit<libc::iovec>,
    /// Message header initialized in retained storage before submission.
    msghdr: MaybeUninit<libc::msghdr>,
}

struct RetainedSendPayload<B: IoBuffReadOnly> {
    /// Caller-owned source buffer retained while connected send is live.
    buffer: B,
}

struct RetainedRecvFromPayload<B: IoBuffReadWrite> {
    /// Caller-owned destination buffer retained while recvmsg is live.
    buffer: B,
    /// Kernel-written source address storage for the received datagram.
    addr: MaybeUninit<libc::sockaddr_storage>,
    /// Capacity passed to the kernel for `addr`; read back through `msghdr`.
    addrlen: libc::socklen_t,
    /// Single kernel-facing iovec pointing into `buffer`.
    iovec: MaybeUninit<libc::iovec>,
    /// Message header whose pointers target fields in this retained payload.
    msghdr: MaybeUninit<libc::msghdr>,
}

struct RetainedSendToPayload<B: IoBuffReadOnly> {
    /// Caller-owned source buffer retained while sendmsg is live.
    buffer: B,
    /// Prepared destination address retained for the kernel.
    addr: MaybeUninit<libc::sockaddr_storage>,
    /// Single kernel-facing iovec pointing into `buffer`.
    iovec: MaybeUninit<libc::iovec>,
    /// Message header whose pointers target fields in this retained payload.
    msghdr: MaybeUninit<libc::msghdr>,
}

/// Initializes the pointer-bearing fields shared by retained UDP receives.
///
/// # Safety
///
/// `buffer`, `iovec`, `msghdr`, and non-null `name` storage must already
/// occupy their final addresses and must not move until the target recvmsg CQE
/// retires. `len` must be a checked snapshot no greater than
/// `buffer.writable_len()`.
#[inline(always)]
unsafe fn initialize_retained_recv_fields<B: IoBuffReadWrite>(
    buffer: &mut B,
    iovec: &mut MaybeUninit<libc::iovec>,
    msghdr: &mut MaybeUninit<libc::msghdr>,
    name: *mut libc::c_void,
    namelen: libc::socklen_t,
    len: u32,
) {
    iovec.write(libc::iovec {
        iov_base: buffer.as_mut_ptr() as *mut libc::c_void,
        iov_len: len as usize,
    });
    write_msghdr(
        msghdr,
        MsgHdrInit {
            name,
            namelen,
            iov: iovec.as_mut_ptr(),
            iovlen: 1,
            control: std::ptr::null_mut(),
            controllen: 0,
        },
    );
}

/// Initializes a connected recvmsg payload at its retained address.
///
/// # Safety
///
/// `payload` must already occupy its final address and must not move until the
/// target recvmsg CQE retires. `len` must be the checked receive length for
/// `payload.buffer`.
#[inline(always)]
unsafe fn initialize_retained_recv_msg_payload<B: IoBuffReadWrite>(
    payload: &mut RetainedRecvMsgPayload<B>,
    len: u32,
) {
    unsafe {
        initialize_retained_recv_fields(
            &mut payload.buffer,
            &mut payload.iovec,
            &mut payload.msghdr,
            std::ptr::null_mut(),
            0,
            len,
        );
    }
}

/// Initializes an explicit-source recvmsg payload at its retained address.
///
/// # Safety
///
/// `payload` must already occupy its final address and must not move until the
/// target recvmsg CQE retires. `len` must be the checked receive length for
/// `payload.buffer`.
#[inline(always)]
unsafe fn initialize_retained_recv_from_payload<B: IoBuffReadWrite>(
    payload: &mut RetainedRecvFromPayload<B>,
    len: u32,
) {
    let RetainedRecvFromPayload {
        buffer,
        addr,
        addrlen,
        iovec,
        msghdr,
    } = payload;
    unsafe {
        initialize_retained_recv_fields(
            buffer,
            iovec,
            msghdr,
            addr.as_mut_ptr() as *mut libc::c_void,
            *addrlen,
            len,
        );
    }
}

/// Initializes an explicit-destination send payload at its retained address.
///
/// # Safety
///
/// `payload` must already occupy its final address and must not move until the
/// target sendmsg CQE retires because its message header points into itself.
/// `len` must be the checked snapshot of `payload.buffer.len()`.
#[inline(always)]
unsafe fn initialize_retained_send_to_payload<B: IoBuffReadOnly>(
    payload: &mut RetainedSendToPayload<B>,
    destination: SocketAddr,
    len: u32,
) {
    let (addr, addrlen) = socket_addr_to_c(destination);
    payload.addr.write(addr);
    payload.iovec.write(libc::iovec {
        iov_base: payload.buffer.as_ptr() as *mut libc::c_void,
        iov_len: len as usize,
    });
    write_msghdr(
        &mut payload.msghdr,
        MsgHdrInit {
            name: payload.addr.as_mut_ptr() as *mut libc::c_void,
            namelen: addrlen,
            iov: payload.iovec.as_mut_ptr(),
            iovlen: 1,
            control: std::ptr::null_mut(),
            controllen: 0,
        },
    );
}

fn zeroed_sockaddr_storage() -> MaybeUninit<libc::sockaddr_storage> {
    MaybeUninit::zeroed()
}

#[inline(always)]
fn udp_payload_is_truncated(msg_flags: libc::c_int) -> bool {
    // These metadata-free receive APIs provide no control buffer. MSG_CTRUNC
    // therefore reports discarded ancillary metadata, not payload loss.
    (msg_flags & libc::MSG_TRUNC) != 0
}

#[inline(always)]
fn udp_receive_result<const BARE_INVALID_DATA: bool>(
    actual: usize,
    msg_flags: libc::c_int,
    truncated_message: &'static str,
) -> io::Result<usize> {
    if udp_payload_is_truncated(msg_flags) {
        Err(invalid_data::<BARE_INVALID_DATA>(truncated_message))
    } else {
        Ok(actual)
    }
}

#[inline(always)]
fn udp_recv_msg_result<const BARE_INVALID_DATA: bool>(
    actual: usize,
    msg_flags: libc::c_int,
) -> io::Result<usize> {
    udp_receive_result::<BARE_INVALID_DATA>(actual, msg_flags, "UDP recv_msg message was truncated")
}

#[inline(always)]
fn udp_recv_from_result<const BARE_INVALID_DATA: bool>(
    actual: usize,
    msg_flags: libc::c_int,
) -> io::Result<usize> {
    udp_receive_result::<BARE_INVALID_DATA>(
        actual,
        msg_flags,
        "UDP recv_from message was truncated",
    )
}

#[inline(always)]
/// Takes a completed datagram payload through its origin reactor.
///
/// # Safety
///
/// A non-null `*state_ptr` must identify a completed operation retaining a
/// payload of type `T`.
unsafe fn take_completed_udp_payload<T: 'static>(
    cx: &mut Context<'_>,
    state_ptr: &mut *mut CompletionState,
) -> Option<CompletionTake<i32, T>> {
    if (*state_ptr).is_null() || unsafe { !(**state_ptr).is_completed() } {
        return None;
    }

    let result = unsafe { (**state_ptr).result };
    let op_ctx = unsafe { completed_op_ctx(poll_ctx_from_waker(cx).ok(), *state_ptr) };
    let payload = unsafe { op_ctx.take_retained_payload_unchecked::<T>(*state_ptr) };
    unsafe { op_ctx.free_op_unchecked(*state_ptr) };
    *state_ptr = std::ptr::null_mut();
    Some(CompletionTake::from_context(
        result,
        payload,
        op_ctx.context_rejected(),
    ))
}

// ---------------------------------------------------------------------------
// RecvFuture (connected recv via IORING_OP_RECV)
// ---------------------------------------------------------------------------

#[doc(hidden)]
pub struct RecvFuture<'a, B: IoBuffReadWrite> {
    /// Connected socket descriptor used for this receive.
    fd: RawFd,
    /// Completion state for the submitted receive operation.
    state_ptr: *mut CompletionState,
    /// Caller-owned destination buffer returned on completion.
    buffer: Option<B>,
    /// Maximum datagram bytes requested from the kernel.
    len: u32,
    /// Logical payload length captured for relative publication.
    write_base_len: usize,
    /// Deferred validation error returned before any SQE submission.
    input_error: Option<io::Error>,
    /// Borrows the parent socket for the future lifetime.
    _marker: PhantomData<&'a mut UdpSocket>,
}

impl<B: IoBuffReadWrite> Future for RecvFuture<'_, B> {
    type Output = (io::Result<usize>, B);

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = unsafe { self.get_unchecked_mut() };

        if this.state_ptr.is_null()
            && let Some(err) = this.input_error.take()
        {
            let result = validate_local_io_result(cx, Err(err));
            let buffer = unsafe { opt_take(&mut this.buffer) };
            return Poll::Ready((result, buffer));
        }
        if this.state_ptr.is_null() && this.buffer.is_none() {
            return Poll::Pending;
        }

        if let Some(completion) =
            unsafe { take_completed_udp_payload::<RetainedRecvPayload<B>>(cx, &mut this.state_ptr) }
        {
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

        if this.state_ptr.is_null() {
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
            this.state_ptr = state_ptr;

            unsafe { (*state_ptr).register_waiter(pctx.owner_task()) };

            let payload = RetainedRecvPayload {
                buffer: unsafe { opt_take(&mut this.buffer) },
            };
            unsafe {
                if let Err((e, payload)) =
                    submit_retained_sqe(&pctx, state_ptr, payload, |payload| {
                        let ptr = payload.buffer.as_mut_ptr();
                        Ok(opcode::Recv::new(types::Fd(this.fd), ptr, this.len)
                            .build()
                            .user_data(state_ptr as u64))
                    })
                {
                    Reactor::free_op_unchecked(pctx.reactor(), state_ptr);
                    this.state_ptr = std::ptr::null_mut();
                    return Poll::Ready((Err(e), payload.buffer));
                }
            }
            return Poll::Pending;
        }

        unsafe { refresh_op_waiter_from_waker(cx, this.state_ptr) };
        Poll::Pending
    }
}

impl<B: IoBuffReadWrite> Drop for RecvFuture<'_, B> {
    fn drop(&mut self) {
        unsafe { drop_op_ptr_unchecked(&mut self.state_ptr) };
    }
}

// ---------------------------------------------------------------------------
// RecvMsgFuture (connected recv via IORING_OP_RECVMSG)
// ---------------------------------------------------------------------------

#[doc(hidden)]
pub struct RecvMsgFuture<'a, B: IoBuffReadWrite> {
    /// Connected socket descriptor used for this receive.
    fd: RawFd,
    /// Completion state for the submitted recvmsg operation.
    state_ptr: *mut CompletionState,
    /// Caller-owned destination buffer returned on completion.
    buffer: Option<B>,
    /// Maximum datagram bytes requested from the kernel.
    len: u32,
    /// Logical payload length captured for relative publication.
    write_base_len: usize,
    /// Deferred validation error returned before any SQE submission.
    input_error: Option<io::Error>,
    /// Borrows the parent socket for the future lifetime.
    _marker: PhantomData<&'a mut UdpSocket>,
}

impl<B: IoBuffReadWrite> Future for RecvMsgFuture<'_, B> {
    type Output = (io::Result<usize>, B);

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = unsafe { self.get_unchecked_mut() };

        if this.state_ptr.is_null()
            && let Some(err) = this.input_error.take()
        {
            let result = validate_local_io_result(cx, Err(err));
            let buffer = unsafe { opt_take(&mut this.buffer) };
            return Poll::Ready((result, buffer));
        }
        if this.state_ptr.is_null() && this.buffer.is_none() {
            return Poll::Pending;
        }

        if let Some(completion) = unsafe {
            take_completed_udp_payload::<RetainedRecvMsgPayload<B>>(cx, &mut this.state_ptr)
        } {
            let (result, payload) = completion.into_io_result(completion_cqe_result);
            let buffer = payload.buffer;
            let actual = match result {
                Ok(actual) => actual,
                Err(err) => return Poll::Ready((Err(err), buffer)),
            };

            let msg = unsafe { payload.msghdr.assume_init_ref() };
            let result = udp_recv_msg_result::<false>(actual, msg.msg_flags);

            return Poll::Ready(unsafe {
                complete_read_with_progress(buffer, this.write_base_len, actual, result)
            });
        }

        if this.state_ptr.is_null() {
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
            this.state_ptr = state_ptr;

            unsafe { (*state_ptr).register_waiter(pctx.owner_task()) };

            let payload = RetainedRecvMsgPayload {
                buffer: unsafe { opt_take(&mut this.buffer) },
                iovec: MaybeUninit::uninit(),
                msghdr: MaybeUninit::uninit(),
            };

            unsafe {
                if let Err((e, payload)) =
                    submit_retained_sqe(&pctx, state_ptr, payload, |payload| {
                        initialize_retained_recv_msg_payload(payload, this.len);

                        Ok(
                            opcode::RecvMsg::new(types::Fd(this.fd), payload.msghdr.as_mut_ptr())
                                .build()
                                .user_data(state_ptr as u64),
                        )
                    })
                {
                    Reactor::free_op_unchecked(pctx.reactor(), state_ptr);
                    this.state_ptr = std::ptr::null_mut();
                    return Poll::Ready((Err(e), payload.buffer));
                }
            }
            return Poll::Pending;
        }

        unsafe { refresh_op_waiter_from_waker(cx, this.state_ptr) };
        Poll::Pending
    }
}

impl<B: IoBuffReadWrite> Drop for RecvMsgFuture<'_, B> {
    fn drop(&mut self) {
        unsafe { drop_op_ptr_unchecked(&mut self.state_ptr) };
    }
}

// ---------------------------------------------------------------------------
// SendFuture (connected send via IORING_OP_SEND)
// ---------------------------------------------------------------------------

#[doc(hidden)]
pub struct SendFuture<'a, B: IoBuffReadOnly> {
    /// Connected socket descriptor used for this send.
    fd: RawFd,
    /// Completion state for the submitted send operation.
    state_ptr: *mut CompletionState,
    /// Caller-owned send buffer returned on completion.
    buffer: Option<B>,
    /// Validated datagram byte count submitted to the kernel.
    len: u32,
    /// Deferred validation error returned before any SQE submission.
    input_error: Option<io::Error>,
    /// Borrows the parent socket for the future lifetime.
    _marker: PhantomData<&'a mut UdpSocket>,
}

impl<B: IoBuffReadOnly> Future for SendFuture<'_, B> {
    type Output = (io::Result<usize>, B);

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = unsafe { self.get_unchecked_mut() };

        if this.state_ptr.is_null()
            && let Some(err) = this.input_error.take()
        {
            let result = validate_local_io_result(cx, Err(err));
            let buffer = unsafe { opt_take(&mut this.buffer) };
            return Poll::Ready((result, buffer));
        }
        if this.state_ptr.is_null() && this.buffer.is_none() {
            return Poll::Pending;
        }

        if let Some(completion) =
            unsafe { take_completed_udp_payload::<RetainedSendPayload<B>>(cx, &mut this.state_ptr) }
        {
            let (result, payload) = completion.into_io_result(completion_cqe_result);
            return Poll::Ready((result, payload.buffer));
        }

        if this.state_ptr.is_null() {
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
            this.state_ptr = state_ptr;

            unsafe { (*state_ptr).register_waiter(pctx.owner_task()) };

            let payload = RetainedSendPayload {
                buffer: unsafe { opt_take(&mut this.buffer) },
            };
            unsafe {
                if let Err((e, payload)) =
                    submit_retained_sqe(&pctx, state_ptr, payload, |payload| {
                        let ptr = payload.buffer.as_ptr();
                        Ok(opcode::Send::new(types::Fd(this.fd), ptr, this.len)
                            .build()
                            .user_data(state_ptr as u64))
                    })
                {
                    Reactor::free_op_unchecked(pctx.reactor(), state_ptr);
                    this.state_ptr = std::ptr::null_mut();
                    return Poll::Ready((Err(e), payload.buffer));
                }
            }
            return Poll::Pending;
        }

        unsafe { refresh_op_waiter_from_waker(cx, this.state_ptr) };
        Poll::Pending
    }
}

impl<B: IoBuffReadOnly> Drop for SendFuture<'_, B> {
    fn drop(&mut self) {
        unsafe { drop_op_ptr_unchecked(&mut self.state_ptr) };
    }
}

// ---------------------------------------------------------------------------
// RecvFromFuture (unconnected recv via IORING_OP_RECVMSG)
// ---------------------------------------------------------------------------

#[doc(hidden)]
pub struct RecvFromFuture<'a, B: IoBuffReadWrite> {
    /// Socket descriptor used for the explicit-peer `recvmsg` receive path.
    /// The socket may also be connected; this API still asks the kernel for
    /// the source address.
    fd: RawFd,
    /// Completion state for the submitted recvmsg operation.
    state_ptr: *mut CompletionState,
    /// Caller-owned destination buffer returned on completion.
    buffer: Option<B>,
    /// Maximum datagram bytes requested from the kernel.
    len: u32,
    /// Logical payload length captured for relative publication.
    write_base_len: usize,
    /// Deferred validation error returned before any SQE submission.
    input_error: Option<io::Error>,
    /// Borrows the parent socket for the future lifetime.
    _marker: PhantomData<&'a mut UdpSocket>,
}

impl<B: IoBuffReadWrite> Future for RecvFromFuture<'_, B> {
    type Output = (io::Result<(usize, SocketAddr)>, B);

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = unsafe { self.get_unchecked_mut() };

        if this.state_ptr.is_null()
            && let Some(err) = this.input_error.take()
        {
            let result = validate_local_io_result(cx, Err(err));
            let buffer = unsafe { opt_take(&mut this.buffer) };
            return Poll::Ready((result, buffer));
        }
        if this.state_ptr.is_null() && this.buffer.is_none() {
            return Poll::Pending;
        }

        if let Some(completion) = unsafe {
            take_completed_udp_payload::<RetainedRecvFromPayload<B>>(cx, &mut this.state_ptr)
        } {
            let (result, payload) = completion.into_io_result(completion_cqe_result);
            let buffer = payload.buffer;
            let actual = match result {
                Ok(actual) => actual,
                Err(err) => return Poll::Ready((Err(err), buffer)),
            };

            let msg = unsafe { payload.msghdr.assume_init_ref() };
            let result = match udp_recv_from_result::<false>(actual, msg.msg_flags) {
                Ok(actual) => {
                    unsafe { socket_addr_from_c(payload.addr.assume_init_ref(), msg.msg_namelen) }
                        .map(|addr| (actual, addr))
                }
                Err(err) => Err(err),
            };
            return Poll::Ready(unsafe {
                complete_read_with_progress(buffer, this.write_base_len, actual, result)
            });
        }

        if this.state_ptr.is_null() {
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
            this.state_ptr = state_ptr;

            unsafe { (*state_ptr).register_waiter(pctx.owner_task()) };

            let payload = RetainedRecvFromPayload {
                buffer: unsafe { opt_take(&mut this.buffer) },
                addr: zeroed_sockaddr_storage(),
                addrlen: std::mem::size_of::<libc::sockaddr_storage>() as libc::socklen_t,
                iovec: MaybeUninit::uninit(),
                msghdr: MaybeUninit::uninit(),
            };
            unsafe {
                if let Err((e, payload)) =
                    submit_retained_sqe(&pctx, state_ptr, payload, |payload| {
                        initialize_retained_recv_from_payload(payload, this.len);

                        Ok(
                            opcode::RecvMsg::new(types::Fd(this.fd), payload.msghdr.as_mut_ptr())
                                .build()
                                .user_data(state_ptr as u64),
                        )
                    })
                {
                    Reactor::free_op_unchecked(pctx.reactor(), state_ptr);
                    this.state_ptr = std::ptr::null_mut();
                    return Poll::Ready((Err(e), payload.buffer));
                }
            }
            return Poll::Pending;
        }

        unsafe { refresh_op_waiter_from_waker(cx, this.state_ptr) };
        Poll::Pending
    }
}

impl<B: IoBuffReadWrite> Drop for RecvFromFuture<'_, B> {
    fn drop(&mut self) {
        unsafe { drop_op_ptr_unchecked(&mut self.state_ptr) };
    }
}

// ---------------------------------------------------------------------------
// SendToFuture (unconnected send via IORING_OP_SENDMSG)
// ---------------------------------------------------------------------------

#[doc(hidden)]
pub struct SendToFuture<'a, B: IoBuffReadOnly> {
    /// Socket descriptor used for the explicit-destination `sendmsg` path.
    /// The socket may also be connected; this API still sends to the provided
    /// destination address.
    fd: RawFd,
    /// Completion state for the submitted sendmsg operation.
    state_ptr: *mut CompletionState,
    /// Caller-owned send buffer returned on completion.
    buffer: Option<B>,
    /// Validated datagram byte count submitted through the retained iovec.
    len: u32,
    /// Destination address prepared after the payload reaches retained storage.
    addr: SocketAddr,
    /// Deferred validation error returned before any SQE submission.
    input_error: Option<io::Error>,
    /// Borrows the parent socket for the future lifetime.
    _marker: PhantomData<&'a mut UdpSocket>,
}

impl<B: IoBuffReadOnly> Future for SendToFuture<'_, B> {
    type Output = (io::Result<usize>, B);

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = unsafe { self.get_unchecked_mut() };

        if this.state_ptr.is_null()
            && let Some(err) = this.input_error.take()
        {
            let result = validate_local_io_result(cx, Err(err));
            let buffer = unsafe { opt_take(&mut this.buffer) };
            return Poll::Ready((result, buffer));
        }
        if this.state_ptr.is_null() && this.buffer.is_none() {
            return Poll::Pending;
        }

        if let Some(completion) = unsafe {
            take_completed_udp_payload::<RetainedSendToPayload<B>>(cx, &mut this.state_ptr)
        } {
            let (result, payload) = completion.into_io_result(completion_cqe_result);
            return Poll::Ready((result, payload.buffer));
        }

        if this.state_ptr.is_null() {
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
            this.state_ptr = state_ptr;

            unsafe { (*state_ptr).register_waiter(pctx.owner_task()) };

            let destination = this.addr;
            let fd = this.fd;
            let len = this.len;
            let payload = RetainedSendToPayload {
                buffer: unsafe { opt_take(&mut this.buffer) },
                addr: MaybeUninit::uninit(),
                iovec: MaybeUninit::uninit(),
                msghdr: MaybeUninit::uninit(),
            };
            unsafe {
                if let Err((e, payload)) =
                    submit_retained_sqe(&pctx, state_ptr, payload, |payload| {
                        initialize_retained_send_to_payload(payload, destination, len);

                        Ok(opcode::SendMsg::new(types::Fd(fd), payload.msghdr.as_ptr())
                            .build()
                            .user_data(state_ptr as u64))
                    })
                {
                    Reactor::free_op_unchecked(pctx.reactor(), state_ptr);
                    this.state_ptr = std::ptr::null_mut();
                    return Poll::Ready((Err(e), payload.buffer));
                }
            }
            return Poll::Pending;
        }

        unsafe { refresh_op_waiter_from_waker(cx, this.state_ptr) };
        Poll::Pending
    }
}

impl<B: IoBuffReadOnly> Drop for SendToFuture<'_, B> {
    fn drop(&mut self) {
        unsafe { drop_op_ptr_unchecked(&mut self.state_ptr) };
    }
}

#[cfg(feature = "test-support")]
pub(crate) mod test_support {
    use super::*;

    /// Classifies a connected UDP `recv_msg` completion through the production
    /// static-message error representation.
    pub fn test_classify_recv_msg(actual: usize, msg_flags: libc::c_int) -> io::Result<usize> {
        udp_recv_msg_result::<false>(actual, msg_flags)
    }

    /// Classifies a connected UDP `recv_msg` completion through the
    /// diagnostic-only bare-`InvalidData` comparator.
    pub fn test_classify_recv_msg_bare(actual: usize, msg_flags: libc::c_int) -> io::Result<usize> {
        udp_recv_msg_result::<true>(actual, msg_flags)
    }

    /// Classifies an explicit-source UDP `recv_from` completion through the
    /// production static-message error representation.
    pub fn test_classify_recv_from(actual: usize, msg_flags: libc::c_int) -> io::Result<usize> {
        udp_recv_from_result::<false>(actual, msg_flags)
    }

    /// Classifies an explicit-source UDP `recv_from` completion through the
    /// diagnostic-only bare-`InvalidData` comparator.
    pub fn test_classify_recv_from_bare(
        actual: usize,
        msg_flags: libc::c_int,
    ) -> io::Result<usize> {
        udp_recv_from_result::<true>(actual, msg_flags)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn udp_payload_truncation_flag_predicate_covers_supported_combinations() {
        for (flags, expected) in [
            (0, false),
            (libc::MSG_TRUNC, true),
            (libc::MSG_CTRUNC, false),
            (libc::MSG_TRUNC | libc::MSG_CTRUNC, true),
            (libc::MSG_PEEK, false),
            (libc::MSG_PEEK | libc::MSG_CTRUNC, false),
            (libc::MSG_PEEK | libc::MSG_TRUNC, true),
        ] {
            assert_eq!(udp_payload_is_truncated(flags), expected, "flags={flags}");
        }
    }

    #[test]
    fn udp_error_modes_share_each_payload_classification_branch() {
        type Classifier = fn(usize, libc::c_int) -> io::Result<usize>;

        for (production, bare, expected_message) in [
            (
                udp_recv_msg_result::<false> as Classifier,
                udp_recv_msg_result::<true> as Classifier,
                "UDP recv_msg message was truncated",
            ),
            (
                udp_recv_from_result::<false> as Classifier,
                udp_recv_from_result::<true> as Classifier,
                "UDP recv_from message was truncated",
            ),
        ] {
            for flags in [0, libc::MSG_CTRUNC, libc::MSG_PEEK] {
                assert_eq!(production(17, flags).expect("production success"), 17);
                assert_eq!(bare(17, flags).expect("bare success"), 17);
            }

            let production_error =
                production(17, libc::MSG_TRUNC).expect_err("production truncation should fail");
            assert_eq!(production_error.kind(), io::ErrorKind::InvalidData);
            assert_eq!(production_error.to_string(), expected_message);
            assert!(production_error.get_ref().is_some());

            let bare_error = bare(17, libc::MSG_TRUNC).expect_err("bare truncation should fail");
            assert_eq!(bare_error.kind(), io::ErrorKind::InvalidData);
            assert!(bare_error.get_ref().is_none());
        }
    }

    #[test]
    fn recv_from_addr_storage_starts_zeroed() {
        let addr = unsafe { zeroed_sockaddr_storage().assume_init() };
        assert_eq!(addr.ss_family, 0);
    }

    #[test]
    fn retained_recv_msg_payload_uses_final_storage_pointers() {
        let mut payload = RetainedRecvMsgPayload {
            buffer: vec![0u8; 32],
            iovec: MaybeUninit::uninit(),
            msghdr: MaybeUninit::uninit(),
        };
        let len = checked_read_len(17, payload.buffer.writable_len())
            .expect("fixture receive length should be valid");

        unsafe { initialize_retained_recv_msg_payload(&mut payload, len) };

        let buffer_ptr = payload.buffer.as_mut_ptr();
        let iovec_ptr = payload.iovec.as_mut_ptr();
        let iovec = unsafe { payload.iovec.assume_init_ref() };
        let msghdr = unsafe { payload.msghdr.assume_init_ref() };
        assert!(msghdr.msg_name.is_null());
        assert_eq!(msghdr.msg_namelen, 0);
        assert_eq!(msghdr.msg_iov, iovec_ptr);
        assert_eq!(msghdr.msg_iovlen, 1);
        assert_eq!(iovec.iov_base, buffer_ptr as *mut libc::c_void);
        assert_eq!(iovec.iov_len, len as usize);
        assert!(msghdr.msg_control.is_null());
        assert_eq!(msghdr.msg_controllen, 0);
        assert_eq!(msghdr.msg_flags, 0);
    }

    #[test]
    fn retained_recv_from_payload_uses_final_storage_pointers() {
        let mut payload = RetainedRecvFromPayload {
            buffer: vec![0u8; 64],
            addr: zeroed_sockaddr_storage(),
            addrlen: std::mem::size_of::<libc::sockaddr_storage>() as libc::socklen_t,
            iovec: MaybeUninit::uninit(),
            msghdr: MaybeUninit::uninit(),
        };
        let len = checked_read_len(29, payload.buffer.writable_len())
            .expect("fixture receive length should be valid");

        unsafe { initialize_retained_recv_from_payload(&mut payload, len) };

        let buffer_ptr = payload.buffer.as_mut_ptr();
        let addr_ptr = payload.addr.as_mut_ptr();
        let iovec_ptr = payload.iovec.as_mut_ptr();
        let addr = unsafe { payload.addr.assume_init_ref() };
        let iovec = unsafe { payload.iovec.assume_init_ref() };
        let msghdr = unsafe { payload.msghdr.assume_init_ref() };
        assert_eq!(
            msghdr.msg_name,
            addr_ptr as *mut libc::sockaddr_storage as *mut libc::c_void
        );
        assert_eq!(msghdr.msg_namelen, payload.addrlen);
        assert_eq!(msghdr.msg_iov, iovec_ptr);
        assert_eq!(msghdr.msg_iovlen, 1);
        assert_eq!(iovec.iov_base, buffer_ptr as *mut libc::c_void);
        assert_eq!(iovec.iov_len, len as usize);
        assert_eq!(addr.ss_family, 0);
        assert!(msghdr.msg_control.is_null());
        assert_eq!(msghdr.msg_controllen, 0);
        assert_eq!(msghdr.msg_flags, 0);
    }

    #[test]
    fn retained_send_to_payload_uses_final_storage_pointers() {
        let destination = SocketAddr::V6(std::net::SocketAddrV6::new(
            std::net::Ipv6Addr::LOCALHOST,
            5432,
            17,
            9,
        ));
        let mut payload = RetainedSendToPayload {
            buffer: b"payload".to_vec(),
            addr: MaybeUninit::uninit(),
            iovec: MaybeUninit::uninit(),
            msghdr: MaybeUninit::uninit(),
        };
        let len =
            checked_send_len(payload.buffer.len()).expect("fixture send length should be valid");

        unsafe { initialize_retained_send_to_payload(&mut payload, destination, len) };

        let addr = unsafe { payload.addr.assume_init_ref() };
        let iovec = unsafe { payload.iovec.assume_init_ref() };
        let msghdr = unsafe { payload.msghdr.assume_init_ref() };
        assert_eq!(
            msghdr.msg_name,
            addr as *const libc::sockaddr_storage as *mut libc::c_void
        );
        assert_eq!(msghdr.msg_iov, iovec as *const libc::iovec as *mut _);
        assert_eq!(iovec.iov_base, payload.buffer.as_ptr() as *mut libc::c_void);
        assert_eq!(iovec.iov_len, payload.buffer.len());
        assert_eq!(
            socket_addr_from_c(addr, msghdr.msg_namelen).expect("destination should decode"),
            destination
        );
        assert!(msghdr.msg_control.is_null());
        assert_eq!(msghdr.msg_controllen, 0);
    }
}
